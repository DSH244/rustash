//! Publish-header parsing, ported from the fork `core/headers.js`:
//!  - `Upstash-Forward-<name>` → `<name>` (multi-value preserved, deduped),
//!  - `Upstash-Workflow-*` pass through as-is (lowercased like fetch Headers),
//!  - top-level `Content-Type` under the canonical key (overrides forwarded casing),
//!  - NON_FORWARDABLE reserved names are dropped,
//!  - every other `Upstash-*` header is consumed, non-Upstash headers dropped.

use serde_json::{json, Map, Value};

use crate::backoff::first_header;
use crate::duration::{parse_duration_ms, parse_duration_secs, VResult, ValidationError};
use crate::retry_expr::validate_retry_delay_expr;

const METHODS: [&str; 7] = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];

/// Delivery stamps + hop-by-hop framing are never client-forwardable (fork list incl.
/// lowercase `content-type`).
const NON_FORWARDABLE: [&str; 16] = [
    "upstash-signature",
    "upstash-message-id",
    "upstash-retried",
    "upstash-region",
    "upstash-schedule-id",
    "upstash-caller-ip",
    "upstash-topic-name",
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "te",
    "upgrade",
    "expect",
    "content-type",
];

/// Never replicated to the destination under `Upstash-Header-Forward`, on top of
/// `NON_FORWARDABLE`: the publisher's own credentials and this service's control headers.
const NEVER_FORWARDED_ON_HEADER_FORWARD: [&str; 8] = [
    "authorization",
    "cookie",
    "proxy-authorization",
    "upstash-header-forward",
    "upstash-destination",
    "upstash-token",
    "idempotency-key",
    "upstash-deduplication-id",
];

/// One inbound header pair, with the (fetch-normalized) lowercase name.
pub type HeaderEntry = (String, String);

/// Build fetch-`Headers`-ordered entries: sorted by lowercase name, values in
/// insertion order within a name.
pub fn entries_sorted(mut pairs: Vec<HeaderEntry>) -> Vec<HeaderEntry> {
    // Stable sort by name — JS `Headers.forEach` iterates the sorted header list.
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

pub fn get_first(entries: &[HeaderEntry], name: &str) -> Option<String> {
    let wanted = name.to_ascii_lowercase();
    entries
        .iter()
        .find(|(n, _)| *n == wanted)
        .map(|(_, v)| v.clone())
}

fn parse_int_header(value: &str, name: &str) -> VResult<i64> {
    let trimmed = value.trim();
    let ok = !trimmed.is_empty()
        && trimmed
            .strip_prefix('-')
            .unwrap_or(trimmed)
            .bytes()
            .all(|b| b.is_ascii_digit())
        && trimmed != "-";
    if !ok {
        return Err(ValidationError(format!("invalid {}: \"{}\"", name, value)));
    }
    trimmed
        .parse::<i64>()
        .map_err(|_| ValidationError(format!("invalid {}: \"{}\"", name, value)))
}

#[derive(Debug, Clone)]
pub struct FlowControlSpec {
    pub key: String,
    pub parallelism: Option<i64>,
    pub rate: Option<i64>,
    pub period_secs: i64,
}

#[derive(Debug, Clone)]
pub struct MessageSpec {
    pub method: String,
    pub forward_headers: Value,
    /// `Upstash-Header-Forward: true` (or `?upstash-header-forward=true`): forward every
    /// inbound header verbatim instead of only the `Upstash-Forward-*` set.
    pub header_forward: bool,
    pub content_type: Option<String>,
    pub timeout_ms: Option<i64>,
    pub retries: i64,
    pub retry_delay_expr: Option<String>,
    pub delay_secs: Option<i64>,
    pub not_before_secs: Option<i64>,
    pub labels: Vec<String>,
    pub dedupe_id: Option<String>,
    pub content_based_dedup: bool,
    pub callback_url: Option<String>,
    pub callback_opts: Option<Value>,
    pub failure_callback_url: Option<String>,
    pub failure_callback_opts: Option<Value>,
    pub flow_control: Option<FlowControlSpec>,
    pub redact_fields: Option<Value>,
    pub cron: Option<String>,
    pub schedule_id: Option<String>,
    pub queue_name: Option<String>,
    pub workflow_run_id: Option<String>,
}

impl Default for MessageSpec {
    fn default() -> Self {
        MessageSpec {
            method: "POST".into(),
            forward_headers: Value::Object(Map::new()),
            header_forward: false,
            content_type: None,
            timeout_ms: None,
            retries: 0,
            retry_delay_expr: None,
            delay_secs: None,
            not_before_secs: None,
            labels: vec![],
            dedupe_id: None,
            content_based_dedup: false,
            callback_url: None,
            callback_opts: None,
            failure_callback_url: None,
            failure_callback_opts: None,
            flow_control: None,
            redact_fields: None,
            cron: None,
            schedule_id: None,
            queue_name: None,
            workflow_run_id: None,
        }
    }
}

pub struct ParseOpts {
    pub default_max_retries: i64,
    pub max_retries_cap: i64,
    pub max_timeout_ms: i64,
}

pub struct CallbackFamily {
    pub url: Option<String>,
    pub opts: Option<Value>,
}

fn parse_callback_family(entries: &[HeaderEntry], prefix: &str) -> VResult<CallbackFamily> {
    let url = get_first(entries, prefix);
    let mut opts = Map::new();
    let lower_prefix = prefix.to_ascii_lowercase();
    if let Some(method) = get_first(entries, &format!("{}-Method", prefix)) {
        let upper = method.to_ascii_uppercase();
        if !METHODS.contains(&upper.as_str()) {
            return Err(ValidationError(format!(
                "invalid {}-Method: \"{}\"",
                prefix, method
            )));
        }
        opts.insert("method".into(), json!(upper));
    }
    if let Some(timeout) = get_first(entries, &format!("{}-Timeout", prefix)) {
        opts.insert(
            "timeoutMs".into(),
            json!(parse_duration_ms(&timeout, &format!("{}-Timeout", prefix))?),
        );
    }
    if let Some(retries) = get_first(entries, &format!("{}-Retries", prefix)) {
        let n = parse_int_header(&retries, &format!("{}-Retries", prefix))?;
        if n < 0 {
            return Err(ValidationError(format!("{}-Retries must be >= 0", prefix)));
        }
        opts.insert("retries".into(), json!(n));
    }
    if let Some(retry_delay) = get_first(entries, &format!("{}-Retry-Delay", prefix)) {
        validate_retry_delay_expr(&retry_delay)?;
        opts.insert("retryDelayExpr".into(), json!(retry_delay));
    }
    // `Upstash-Callback-Forward-X: my-value` → `X: my-value` on the callback delivery.
    let forward_prefix = format!("{}-forward-", lower_prefix);
    let mut forward: Map<String, Value> = Map::new();
    for (name, value) in entries {
        if name.starts_with(&forward_prefix) {
            let target = &name[forward_prefix.len()..];
            if !target.is_empty() {
                forward
                    .entry(target.to_string())
                    .or_insert_with(|| Value::Array(vec![]))
                    .as_array_mut()
                    .unwrap()
                    .push(json!(value));
            }
        }
    }
    if !forward.is_empty() {
        opts.insert("forwardHeaders".into(), Value::Object(forward));
    }
    let has_opts = !opts.is_empty();
    if url.is_none() && has_opts {
        // Options without a URL are tolerated (QStash ignores them).
        return Ok(CallbackFamily {
            url: None,
            opts: None,
        });
    }
    Ok(CallbackFamily {
        url,
        opts: if has_opts {
            Some(Value::Object(opts))
        } else {
            None
        },
    })
}

pub fn parse_flow_control_value(key: &str, value: &str) -> VResult<FlowControlSpec> {
    let mut spec = FlowControlSpec {
        key: key.to_string(),
        parallelism: None,
        rate: None,
        period_secs: 1,
    };
    for part in value.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()) {
        let Some(eq) = part.find('=') else {
            return Err(ValidationError(format!(
                "invalid Upstash-Flow-Control-Value segment \"{}\"",
                part
            )));
        };
        let k = part[..eq].trim().to_ascii_lowercase();
        let v = part[eq + 1..].trim();
        match k.as_str() {
            "parallelism" => {
                let n = parse_int_header(v, "parallelism")?;
                if n < 1 {
                    return Err(ValidationError(
                        "flow-control parallelism must be >= 1".into(),
                    ));
                }
                spec.parallelism = Some(n);
            }
            "rate" => {
                let n = parse_int_header(v, "rate")?;
                if n < 1 {
                    return Err(ValidationError("flow-control rate must be >= 1".into()));
                }
                spec.rate = Some(n);
            }
            "period" => {
                spec.period_secs = parse_duration_secs(v, "flow-control period")?;
                if spec.period_secs < 1 {
                    return Err(ValidationError("flow-control period must be >= 1s".into()));
                }
            }
            other => {
                return Err(ValidationError(format!(
                    "unknown Upstash-Flow-Control-Value field \"{}\"",
                    other
                )))
            }
        }
    }
    if spec.parallelism.is_none() && spec.rate.is_none() {
        return Err(ValidationError(
            "Upstash-Flow-Control-Value must set parallelism and/or rate".into(),
        ));
    }
    Ok(spec)
}

fn parse_redact_fields(input: &str) -> VResult<Value> {
    let mut result = Map::new();
    for part in input.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()) {
        if part == "body" {
            result.insert("body".into(), json!(true));
            continue;
        }
        if part == "headers" {
            result.insert("headers".into(), json!(true));
            continue;
        }
        if part.starts_with("header[") && part.ends_with(']') && part.len() > 8 {
            let name = part[7..part.len() - 1].to_ascii_lowercase();
            let entry = result
                .entry("headerNames".to_string())
                .or_insert_with(|| Value::Array(vec![]));
            entry.as_array_mut().unwrap().push(json!(name));
            continue;
        }
        return Err(ValidationError(format!(
            "invalid Upstash-Redact-Fields directive \"{}\"",
            part
        )));
    }
    Ok(Value::Object(result))
}

/// The main parser — `parsePublishHeaders`. `entries` must be fetch-order (sorted,
/// lowercased names). Accepts `Idempotency-Key` as an alias for the dedup id (task
/// scope); `Upstash-Deduplication-Id` wins when both are present.
pub fn parse_publish_headers(entries: &[HeaderEntry], opts: &ParseOpts) -> VResult<MessageSpec> {
    let mut forward: Map<String, Value> = Map::new();
    // `Upstash-Header-Forward: true` (query form `?upstash-header-forward=true` is merged
    // into `entries` by the router) switches from "only Upstash-Forward-*" to "every inbound
    // header". Documented behaviour, and webhook receivers rely on it because most of them
    // cannot be configured to require a prefix.
    let header_forward = get_first(entries, "upstash-header-forward")
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    for (name, value) in entries {
        if let Some(target_full) = name.strip_prefix("upstash-forward-") {
            let target = target_full;
            if !target.is_empty()
                && !NON_FORWARDABLE.contains(&target.to_ascii_lowercase().as_str())
            {
                forward
                    .entry(target.to_string())
                    .or_insert_with(|| Value::Array(vec![]))
                    .as_array_mut()
                    .unwrap()
                    .push(json!(value.clone()));
            }
        } else if name.starts_with("upstash-workflow-") {
            // Workflow headers reach the destination; dedupe identical (name,value)
            // pairs so SDK direct + Forward forms don't double up.
            let list = forward
                .entry(name.clone())
                .or_insert_with(|| Value::Array(vec![]));
            let arr = list.as_array_mut().unwrap();
            if !arr.iter().any(|v| v.as_str() == Some(value.as_str())) {
                arr.push(json!(value.clone()));
            }
        }
    }
    if header_forward {
        // `Upstash-Header-Forward` asks for the inbound headers verbatim. Everything that
        // authenticates against Rustash itself and the hop-by-hop/control set is withheld:
        // forwarding the caller's bearer token would hand the queue credentials to every
        // destination, and the framing headers belong to the outbound connection. Every
        // `upstash-*` control header is skipped too (they describe *this* publish, and the
        // `upstash-workflow-*` set is already added above).
        for (name, value) in entries {
            let lower = name.to_ascii_lowercase();
            if lower.starts_with("upstash-")
                || NON_FORWARDABLE.contains(&lower.as_str())
                || NEVER_FORWARDED_ON_HEADER_FORWARD.contains(&lower.as_str())
            {
                continue;
            }
            let list = forward
                .entry(name.clone())
                .or_insert_with(|| Value::Array(vec![]));
            let arr = list.as_array_mut().unwrap();
            if !arr.iter().any(|v| v.as_str() == Some(value.as_str())) {
                arr.push(json!(value.clone()));
            }
        }
    }
    // A header supplied both directly and via Upstash-Forward- must not double up.
    for (_k, v) in forward.iter_mut() {
        let arr = v.as_array_mut().unwrap();
        let mut seen: Vec<String> = Vec::new();
        arr.retain(|item| {
            let s = item.as_str().unwrap_or_default().to_string();
            if seen.contains(&s) {
                false
            } else {
                seen.push(s);
                true
            }
        });
    }
    let content_type = get_first(entries, "content-type");
    if let Some(ct) = &content_type {
        forward.insert("Content-Type".into(), json!([ct.clone()]));
    }

    let workflow_run_id = get_first(entries, "upstash-workflow-runid");

    let mut method = "POST".to_string();
    if let Some(raw) = get_first(entries, "upstash-method") {
        let upper = raw.to_ascii_uppercase();
        if !METHODS.contains(&upper.as_str()) {
            return Err(ValidationError(format!(
                "invalid Upstash-Method: \"{}\"",
                raw
            )));
        }
        method = upper;
    }

    let mut timeout_ms: Option<i64> = None;
    if let Some(timeout) = get_first(entries, "upstash-timeout") {
        let parsed = parse_duration_ms(&timeout, "Upstash-Timeout")?;
        let capped = parsed.min(opts.max_timeout_ms);
        if capped <= 0 {
            return Err(ValidationError("Upstash-Timeout must be positive".into()));
        }
        timeout_ms = Some(capped);
    }

    let mut retries = opts.default_max_retries;
    if let Some(raw) = get_first(entries, "upstash-retries") {
        let n = parse_int_header(&raw, "Upstash-Retries")?;
        if n < 0 {
            return Err(ValidationError("Upstash-Retries must be >= 0".into()));
        }
        retries = n.min(opts.max_retries_cap);
    }

    let mut retry_delay_expr = None;
    if let Some(expr) = get_first(entries, "upstash-retry-delay") {
        validate_retry_delay_expr(&expr)?;
        retry_delay_expr = Some(expr);
    }

    let mut delay_secs = None;
    if let Some(delay) = get_first(entries, "upstash-delay") {
        let n = parse_duration_secs(&delay, "Upstash-Delay")?;
        if n < 0 {
            return Err(ValidationError("Upstash-Delay must be >= 0".into()));
        }
        delay_secs = Some(n);
    }

    let mut not_before_secs = None;
    if let Some(nb) = get_first(entries, "upstash-not-before") {
        not_before_secs = Some(parse_int_header(&nb, "Upstash-Not-Before")?);
    }

    let labels = match get_first(entries, "upstash-label") {
        Some(raw) => raw
            .split(',')
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(|l| l.to_string())
            .collect(),
        None => vec![],
    };

    let dedupe_id = get_first(entries, "upstash-deduplication-id")
        .or_else(|| get_first(entries, "idempotency-key"));
    let content_based_dedup =
        get_first(entries, "upstash-content-based-deduplication").as_deref() == Some("true");

    let callback = parse_callback_family(entries, "Upstash-Callback")?;
    let failure_callback = parse_callback_family(entries, "Upstash-Failure-Callback")?;

    let fc_key = get_first(entries, "upstash-flow-control-key");
    let fc_value = get_first(entries, "upstash-flow-control-value");
    let flow_control =
        match (&fc_key, &fc_value) {
            (Some(k), Some(v)) => Some(parse_flow_control_value(k, v)?),
            (Some(_), None) | (None, Some(_)) => return Err(ValidationError(
                "Upstash-Flow-Control-Key and Upstash-Flow-Control-Value must be provided together"
                    .into(),
            )),
            (None, None) => None,
        };

    let redact_fields = match get_first(entries, "upstash-redact-fields") {
        Some(raw) => Some(parse_redact_fields(&raw)?),
        None => None,
    };

    Ok(MessageSpec {
        method,
        forward_headers: Value::Object(forward),
        header_forward,
        content_type,
        timeout_ms,
        retries,
        retry_delay_expr,
        delay_secs,
        not_before_secs,
        labels,
        dedupe_id,
        content_based_dedup,
        callback_url: callback.url,
        callback_opts: callback.opts,
        failure_callback_url: failure_callback.url,
        failure_callback_opts: failure_callback.opts,
        flow_control,
        redact_fields,
        cron: get_first(entries, "upstash-cron"),
        schedule_id: get_first(entries, "upstash-schedule-id"),
        queue_name: get_first(entries, "upstash-queue-name"),
        workflow_run_id,
    })
}

/// Earliest-delivery time: Not-Before wins over Delay; null = immediately (insert now).
pub fn spec_not_before(spec: &MessageSpec, now_ms: i64) -> Option<i64> {
    if let Some(n) = spec.not_before_secs {
        return Some(n * 1000);
    }
    if let Some(d) = spec.delay_secs {
        return Some(now_ms + d * 1000);
    }
    None
}

/// Workflow LazyFetch detection: a forwarded `Upstash-Workflow-RunId` marks a workflow
/// message, delivered with an EMPTY body (signer signs over the empty body).
pub fn is_workflow_message(headers: &Value) -> bool {
    first_header(headers, "upstash-workflow-runid").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(pairs: &[(&str, &str)]) -> Vec<HeaderEntry> {
        entries_sorted(
            pairs
                .iter()
                .map(|(n, v)| (n.to_ascii_lowercase(), v.to_string()))
                .collect(),
        )
    }

    fn opts() -> ParseOpts {
        ParseOpts {
            default_max_retries: 3,
            max_retries_cap: 100,
            max_timeout_ms: 120_000,
        }
    }

    #[test]
    fn forward_headers_and_content_type_canonicalization() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-forward-x-foo", "bar"),
                ("upstash-forward-x-foo", "baz"),
                ("upstash-forward-x-dup", "1"),
                ("content-type", "application/json"),
                ("upstash-forward-content-type", "text/plain"),
                ("upstash-forward-host", "evil.example"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(
            spec.forward_headers,
            json!({
                "x-dup": ["1"],
                "x-foo": ["bar", "baz"],
                "Content-Type": ["application/json"]
            })
        );
        // sorted-by-name insertion order: x-dup, x-foo, then canonical Content-Type
        let keys: Vec<&String> = spec.forward_headers.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["x-dup", "x-foo", "Content-Type"]);
    }

    #[test]
    fn workflow_headers_pass_through_and_dedupe() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-workflow-runid", "run_1"),
                ("upstash-forward-upstash-workflow-sdk-version", "1.0"),
                ("upstash-workflow-sdk-version", "1.0"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(spec.workflow_run_id.as_deref(), Some("run_1"));
        assert_eq!(
            spec.forward_headers["upstash-workflow-sdk-version"],
            json!(["1.0"])
        );
    }

    #[test]
    fn method_retries_timeout_delay_not_before() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-method", "patch"),
                ("upstash-retries", "5"),
                ("upstash-timeout", "30s"),
                ("upstash-delay", "1m"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(spec.method, "PATCH");
        assert_eq!(spec.retries, 5);
        assert_eq!(spec.timeout_ms, Some(30_000));
        assert_eq!(spec.delay_secs, Some(60));

        let err = parse_publish_headers(&e(&[("upstash-method", "TRACE")]), &opts()).unwrap_err();
        assert_eq!(err.0, "invalid Upstash-Method: \"TRACE\"");

        let err = parse_publish_headers(&e(&[("upstash-retries", "200")]), &opts()).unwrap();
        assert_eq!(err.retries, 100); // clamped to MAX_RETRIES

        let err = parse_publish_headers(&e(&[("upstash-retries", "x")]), &opts()).unwrap_err();
        assert_eq!(err.0, "invalid Upstash-Retries: \"x\"");

        let err = parse_publish_headers(&e(&[("upstash-timeout", "0s")]), &opts()).unwrap_err();
        assert_eq!(err.0, "Upstash-Timeout must be positive");
    }

    #[test]
    fn not_before_wins_over_delay() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-delay", "60"),
                ("upstash-not-before", "1730592700"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(
            spec_not_before(&spec, 1_730_591_700_000),
            Some(1_730_592_700_000)
        );
        let spec2 = parse_publish_headers(&e(&[("upstash-delay", "60")]), &opts()).unwrap();
        assert_eq!(
            spec_not_before(&spec2, 1_730_591_700_000),
            Some(1_730_591_760_000)
        );
        let spec3 = parse_publish_headers(&e(&[]), &opts()).unwrap();
        assert_eq!(spec_not_before(&spec3, 0), None);
    }

    #[test]
    fn dedupe_headers_both_names() {
        let s1 = parse_publish_headers(&e(&[("upstash-deduplication-id", "X")]), &opts()).unwrap();
        assert_eq!(s1.dedupe_id.as_deref(), Some("X"));
        let s2 = parse_publish_headers(&e(&[("idempotency-key", "K")]), &opts()).unwrap();
        assert_eq!(s2.dedupe_id.as_deref(), Some("K"));
        let s3 = parse_publish_headers(
            &e(&[("idempotency-key", "K"), ("upstash-deduplication-id", "X")]),
            &opts(),
        )
        .unwrap();
        assert_eq!(s3.dedupe_id.as_deref(), Some("X"));
    }

    #[test]
    fn flow_control_pairing_and_grammar() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-flow-control-key", "orders"),
                (
                    "upstash-flow-control-value",
                    "parallelism=3,rate=10,period=2m",
                ),
            ]),
            &opts(),
        )
        .unwrap();
        let fc = spec.flow_control.unwrap();
        assert_eq!(fc.key, "orders");
        assert_eq!(fc.parallelism, Some(3));
        assert_eq!(fc.rate, Some(10));
        assert_eq!(fc.period_secs, 120);

        let err = parse_publish_headers(&e(&[("upstash-flow-control-key", "orders")]), &opts())
            .unwrap_err();
        assert_eq!(
            err.0,
            "Upstash-Flow-Control-Key and Upstash-Flow-Control-Value must be provided together"
        );
        let err = parse_flow_control_value("k", "bogus").unwrap_err();
        assert_eq!(
            err.0,
            "invalid Upstash-Flow-Control-Value segment \"bogus\""
        );
        let err = parse_flow_control_value("k", "").unwrap_err();
        assert_eq!(
            err.0,
            "Upstash-Flow-Control-Value must set parallelism and/or rate"
        );
    }

    #[test]
    fn callback_family() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-callback", "https://cb.example/"),
                ("upstash-callback-method", "PUT"),
                ("upstash-callback-forward-x-note", "hi"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(spec.callback_url.as_deref(), Some("https://cb.example/"));
        let opts = spec.callback_opts.unwrap();
        assert_eq!(opts["method"], "PUT");
        assert_eq!(opts["forwardHeaders"]["x-note"], json!(["hi"]));
    }

    #[test]
    fn labels_and_redact_and_content_dedupe() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-label", "a, b ,,"),
                ("upstash-content-based-deduplication", "true"),
                ("upstash-redact-fields", "body,headers,header[secret]"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(spec.labels, vec!["a", "b"]);
        assert!(spec.content_based_dedup);
        assert_eq!(
            spec.redact_fields.unwrap(),
            json!({"body": true, "headers": true, "headerNames": ["secret"]})
        );
        let err =
            parse_publish_headers(&e(&[("upstash-redact-fields", "bogus")]), &opts()).unwrap_err();
        assert_eq!(err.0, "invalid Upstash-Redact-Fields directive \"bogus\"");
    }

    /// `Upstash-Header-Forward: true` forwards every inbound header verbatim instead of only
    /// the `Upstash-Forward-*` set (documented: "since most webhook services do not allow
    /// header prefixing, we introduced a configuration option to enable forwarding all
    /// incoming request headers"). The publisher's own credentials and this service's control
    /// headers must never travel with the delivery.
    #[test]
    fn header_forward_replicates_inbound_headers_but_never_credentials() {
        let spec = parse_publish_headers(
            &e(&[
                ("upstash-header-forward", "true"),
                ("authorization", "Bearer queue-token"),
                ("cookie", "session=1"),
                ("x-webhook-signature", "abc123"),
                ("x-region", "eu"),
                ("content-type", "application/json"),
                ("upstash-retries", "5"),
                ("upstash-forward-x-extra", "direct"),
            ]),
            &opts(),
        )
        .unwrap();
        assert!(spec.header_forward);
        let fwd = &spec.forward_headers;
        assert_eq!(fwd["x-webhook-signature"], json!(["abc123"]));
        assert_eq!(fwd["x-region"], json!(["eu"]));
        // Prefix form keeps working alongside the flag.
        assert_eq!(fwd["x-extra"], json!(["direct"]));
        // Content-Type is forwarded once, from the canonical key.
        assert_eq!(fwd["Content-Type"], json!(["application/json"]));
        // Credentials and control headers stay behind.
        for forbidden in [
            "authorization",
            "cookie",
            "upstash-retries",
            "upstash-header-forward",
        ] {
            assert!(
                fwd.get(forbidden).is_none(),
                "{forbidden} must not be forwarded: {fwd}"
            );
        }
    }

    /// Without the flag, an inbound custom header is not forwarded — the pre-existing
    /// contract for callers that never opt in.
    #[test]
    fn header_forward_is_opt_in() {
        let spec = parse_publish_headers(&e(&[("x-region", "eu")]), &opts()).unwrap();
        assert!(!spec.header_forward);
        assert!(spec.forward_headers.get("x-region").is_none());
    }
}
