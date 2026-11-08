//! One outbound delivery attempt: forwarded headers, QStash stamps, the JWT signature,
//! the dial-time SSRF gate, the HTTP/1.1 exchange through the shared keep-alive pool,
//! and QStash's redirect policy.
//!
//! Boundary: `deliver` owns sockets and framing, `signer` owns the JWT, `tenant` and
//! `model` supply identity and rows. This module performs no database access and writes
//! no message state — the engine decides what a response means (retry, DLQ, success).
//! The SSRF policy is passed in pre-built so the CIDR lists are parsed once per process,
//! not once per delivery (nor once per redirect hop).

use serde_json::Value;
use url::Url;

use crate::config::Config;
use crate::deliver::Pool as DeliveryPool;
use crate::headers::is_workflow_message;
use crate::model::{DeliveryResponse, MessageRow};
use crate::signer::sign_delivery;
use crate::ssrf::{check_resolved_allowed, SsrfPolicy};
use crate::tenant::Tenant;

/// QStash documents "up to 10 in a row".
const MAX_REDIRECT_HOPS: u32 = 10;

/// How long this delivery may run: the message's own `Upstash-Timeout`, capped by
/// `MAX_HTTP_TIMEOUT_MS`. Public so the engine can decide whether a lease renewer is
/// needed without duplicating the cap rule.
pub fn timeout_ms(config: &Config, row: &MessageRow) -> i64 {
    row.timeout_ms
        .unwrap_or(config.max_http_timeout_ms)
        .min(config.max_http_timeout_ms)
}

/// One delivery attempt: build headers (forwarded + stamps + JWT signature), honor
/// GET/HEAD bodyless + LazyFetch empty bodies, SSRF dial-time check, timeout cap.
pub async fn attempt(
    pool: &DeliveryPool,
    policy: &SsrfPolicy,
    config: &Config,
    tenant: &Tenant,
    row: &MessageRow,
    now: i64,
) -> Result<DeliveryResponse, String> {
    let parsed =
        Url::parse(&row.url).map_err(|_| format!("invalid destination URL \"{}\"", row.url))?;
    check_resolved_allowed(policy, &parsed)?;

    // QStash LazyFetch: workflow invocations always deliver an EMPTY body.
    let deliver_body: Option<Vec<u8>> = if is_workflow_message(&row.headers) {
        None
    } else {
        row.body.clone()
    };

    let mut headers: Vec<(String, String)> = Vec::new();
    if let Some(map) = row.headers.as_object() {
        for (name, values) in map {
            if let Some(arr) = values.as_array() {
                for v in arr {
                    headers.push((name.clone(), v.as_str().unwrap_or_default().to_string()));
                }
            } else if let Some(v) = values.as_str() {
                headers.push((name.clone(), v.to_string()));
            }
        }
    }
    let signature = sign_delivery(
        &tenant.signing_key_current,
        &row.url,
        deliver_body.as_deref(),
        now,
    );
    headers.push(("Upstash-Signature".into(), signature));
    headers.push(("Upstash-Message-Id".into(), row.id.clone()));
    headers.push(("Upstash-Retried".into(), row.retried.to_string()));
    headers.push(("Upstash-Region".into(), config.region.clone()));
    if let Some(sid) = &row.schedule_id {
        headers.push(("Upstash-Schedule-Id".into(), sid.clone()));
    }
    if let Some(ip) = &row.caller_ip {
        headers.push(("Upstash-Caller-Ip".into(), ip.clone()));
    }
    if let Some(topic) = &row.topic_name {
        headers.push(("Upstash-Topic-Name".into(), topic.clone()));
    }
    if !headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("user-agent"))
    {
        headers.push(("User-Agent".into(), "Upstash-QStash".into()));
    }

    let method = row.method.to_ascii_uppercase();
    let mut method_owned = method.clone();
    let mut bodyless = method == "GET" || method == "HEAD";
    let timeout = std::time::Duration::from_millis(timeout_ms(config, row).max(1) as u64);

    // Wire client: host/connection first, caller headers with their stored name casing
    // verbatim, accept*/content-length framing last.
    let body_bytes = deliver_body.unwrap_or_default();
    let mut raw = crate::deliver::send(
        pool,
        &parsed,
        &method,
        &headers,
        if bodyless { None } else { Some(&body_bytes) },
        timeout,
        config.max_response_body_bytes,
    )
    .await?;

    // QStash follows redirects from the endpoint, up to 10 in a row, and each hop's
    // target goes through the same destination checks as the original. 307/308 keep
    // method and body; 301/302/303 turn the request into a bodiless GET. Only the
    // final response decides the outcome. `Upstash-Signature` rides along unchanged —
    // its `sub` claim still names the ORIGINAL destination, exactly as documented.
    let mut current = parsed.clone();
    let mut hops = 0u32;
    while matches!(raw.status, 301 | 302 | 303 | 307 | 308) && hops < MAX_REDIRECT_HOPS {
        let Some(location) = raw
            .headers
            .get("location")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        else {
            break; // a 3XX without Location is the final answer (and a retryable failure)
        };
        let Ok(next) = current.join(&location) else {
            return Err(format!("invalid redirect location \"{}\"", location));
        };
        if next.scheme() != "http" && next.scheme() != "https" {
            return Err(format!(
                "redirect to unsupported scheme \"{}\"",
                next.scheme()
            ));
        }
        check_resolved_allowed(policy, &next)?;
        let keep_method = matches!(raw.status, 307 | 308);
        if !keep_method {
            method_owned = "GET".to_string();
            bodyless = true;
        }
        hops += 1;
        let next_host = next
            .host_str()
            .ok_or_else(|| "redirect target has no host".to_string())?
            .to_string();
        let deliver_body_now: Option<&[u8]> = if bodyless { None } else { Some(&body_bytes) };
        raw = crate::deliver::send(
            pool,
            &next,
            &method_owned,
            &headers,
            deliver_body_now,
            timeout,
            config.max_response_body_bytes,
        )
        .await?;
        let _ = next_host;
        current = next;
    }

    Ok(DeliveryResponse {
        status: Some(raw.status as i64),
        // names lowercased, first-seen arrival order
        headers: if raw.headers.is_empty() {
            None
        } else {
            Some(Value::Object(raw.headers))
        },
        body: Some(raw.body),
    })
}
