//! `batch` and its `batchPublish` alias: one publish per array item, where an item's
//! `headers` plus its `delay`/`notBefore`/`retries` fields form one specification.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

use crate::headers::HeaderEntry;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::ingest::{caller_ip, is_url, spec_from_entries};
use super::publish::{publish_json, publish_to_destination};
use super::queues::ensure_queue;
use super::AppState;

pub(super) async fn batch_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let parsed: Value = serde_json::from_slice(&body).map_err(|_| AppError::internal())?;
    let items = parsed
        .as_array()
        .cloned()
        .ok_or_else(|| AppError::new(400, "batch body must be an array"))?;
    let caller = caller_ip(&headers);
    let mut out: Vec<Value> = Vec::new();
    for item in items {
        let mut entry_pairs: Vec<HeaderEntry> = Vec::new();
        if let Some(hs) = item.get("headers").and_then(|v| v.as_object()) {
            for (name, value) in hs {
                let lower = name.to_ascii_lowercase();
                match value {
                    Value::Array(vals) => {
                        for v in vals {
                            entry_pairs
                                .push((lower.clone(), v.as_str().unwrap_or_default().to_string()));
                        }
                    }
                    other => entry_pairs.push((
                        lower.clone(),
                        other
                            .as_str()
                            .map(String::from)
                            .unwrap_or_else(|| other.to_string()),
                    )),
                }
            }
        }
        // item-level delay / notBefore / retries map to headers UNLESS the item already
        // sets that header (headers win — `Headers.set` replaces existing values).
        if let Some(d) = item.get("delay") {
            if !pair_has(&entry_pairs, "upstash-delay") {
                pair_set(&mut entry_pairs, "upstash-delay", js_number_string(d));
            }
        }
        if let Some(nb) = item.get("notBefore") {
            if !pair_has(&entry_pairs, "upstash-not-before") {
                pair_set(&mut entry_pairs, "upstash-not-before", js_number_string(nb));
            }
        }
        if let Some(r) = item.get("retries") {
            if !pair_has(&entry_pairs, "upstash-retries") {
                pair_set(&mut entry_pairs, "upstash-retries", js_number_string(r));
            }
        }
        let spec = spec_from_entries(entry_pairs, &state.config)?;
        let queue_name = item
            .get("queue")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or(spec.queue_name.clone());
        if let Some(q) = &queue_name {
            let conn = state.db.conn.lock().unwrap();
            ensure_queue(&conn, &tenant.id, q);
        }
        let body_vec = item
            .get("body")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.as_bytes().to_vec());
        let destination = item
            .get("destination")
            .and_then(|v| v.as_str())
            .unwrap_or("undefined")
            .to_string();
        let results = publish_to_destination(
            &state,
            &tenant,
            &destination,
            body_vec,
            &spec,
            queue_name,
            caller.clone(),
        )
        .await?;
        out.push(publish_json(&results, is_url(&destination)));
    }
    Ok((StatusCode::CREATED, Json(Value::Array(out))).into_response())
}

/// JS `String(number)` for JSON numbers (integer form when integral).
fn pair_has(pairs: &[HeaderEntry], n: &str) -> bool {
    pairs.iter().any(|(k, _)| k == n)
}

fn pair_set(pairs: &mut Vec<HeaderEntry>, n: &str, v: String) {
    pairs.retain(|(k, _)| k != n);
    pairs.push((n.to_string(), v));
}

fn js_number_string(v: &Value) -> String {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                if f == f.round() && f.abs() < 1e15 {
                    (f as i64).to_string()
                } else {
                    format!("{}", f)
                }
            } else {
                n.to_string()
            }
        }
        other => other
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| other.to_string()),
    }
}
