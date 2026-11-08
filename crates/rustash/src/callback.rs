//! Callback payload and row construction (pure: no DB, no network, no clock injection).
//!
//! QStash callbacks are ordinary messages. Keeping their JSON shape and the redaction
//! rules here means the engine only decides *when* a callback is due, not how its body
//! is serialized. Header keys in the callback `header` field are byte-sorted because the
//! payload is built in memory (unlike a persisted response-header row, which reads back
//! in JSON collation order).

use serde_json::{json, Map, Value};

use crate::ids;
use crate::model::{DeliveryResponse, MessageRow};
use crate::time::now_ms;

/// buildCallbackBody — the documented QStash callback JSON (numbers as STRING ms,
/// bodies base64, headers map->string[]).
pub fn build_callback_body(
    row: &MessageRow,
    response: &Option<DeliveryResponse>,
    dlq_id: Option<&str>,
) -> Value {
    let redact = row.redact_fields.clone().unwrap_or(Value::Null);
    let redact_body = redact
        .get("body")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let source_body = match &row.body {
        Some(b) if redact_body => Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("REDACTED:{}", ids::sha256_hex(b)),
        )),
        Some(b) => Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        None => json!(""),
    };
    let source_header = redact_headers(&row.headers, &redact);

    let mut body = Map::new();
    body.insert(
        "status".into(),
        json!(response.as_ref().and_then(|r| r.status).unwrap_or(0)),
    );
    body.insert(
        "header".into(),
        match response.as_ref().and_then(|r| r.headers.clone()) {
            Some(h) => sort_object_keys_byte_order(h),
            None => Value::Object(Map::new()),
        },
    );
    body.insert(
        "body".into(),
        match response.as_ref().and_then(|r| r.body.as_ref()) {
            Some(b) => Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b,
            )),
            None => json!(""),
        },
    );
    body.insert("retried".into(), json!(row.retried));
    body.insert("maxRetries".into(), json!(row.max_retries));
    body.insert("sourceMessageId".into(), json!(&row.id));
    if let Some(t) = &row.topic_name {
        body.insert("topicName".into(), json!(t));
    }
    if let Some(e) = &row.endpoint_name {
        body.insert("endpointName".into(), json!(e));
    }
    body.insert("url".into(), json!(&row.url));
    body.insert("method".into(), json!(&row.method));
    body.insert("sourceHeader".into(), source_header);
    body.insert("sourceBody".into(), source_body);
    body.insert("notBefore".into(), json!(row.not_before.to_string()));
    body.insert("createdAt".into(), json!(row.created_at.to_string()));
    if let Some(s) = &row.schedule_id {
        body.insert("scheduleId".into(), json!(s));
    }
    if let Some(ip) = &row.caller_ip {
        body.insert("callerIP".into(), json!(ip));
    }
    if let Some(d) = dlq_id {
        body.insert("dlqId".into(), json!(d));
    }
    Value::Object(body)
}

fn sort_object_keys_byte_order(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            let mut out = Map::new();
            for (k, val) in entries {
                out.insert(k, val);
            }
            Value::Object(out)
        }
        other => other,
    }
}

fn redact_headers(headers: &Value, redact: &Value) -> Value {
    let red_all = redact
        .get("headers")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let names: Vec<String> = redact
        .get("headerNames")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if !red_all && names.is_empty() {
        return headers.clone();
    }
    let Some(map) = headers.as_object() else {
        return headers.clone();
    };
    let mut out = Map::new();
    for (name, values) in map {
        let hit = red_all || names.iter().any(|n| n.eq_ignore_ascii_case(name));
        if hit {
            let arr = values.as_array().cloned().unwrap_or_default();
            out.insert(
                name.clone(),
                Value::Array(
                    arr.iter()
                        .map(|v| {
                            let s = v.as_str().unwrap_or_default();
                            json!(format!("REDACTED:{}", ids::sha256_hex(s.as_bytes())))
                        })
                        .collect(),
                ),
            );
        } else {
            out.insert(name.clone(), values.clone());
        }
    }
    Value::Object(out)
}

/// buildCallbackRow: materialize a (failure) callback as a regular message row.
/// Callbacks carry no callbacks of their own (no-recursion guard) and deliver
/// immediately. Exactly one Content-Type: application/json.
pub fn build_callback_row(
    row: &MessageRow,
    kind: &str,
    url: &str,
    opts: Option<&Value>,
    response: &Option<DeliveryResponse>,
    dlq_id: Option<&str>,
) -> MessageRow {
    let now = now_ms();
    let payload = build_callback_body(row, response, dlq_id).to_string();
    let mut headers = opts
        .and_then(|o| o.get("forwardHeaders"))
        .and_then(|f| f.as_object())
        .cloned()
        .unwrap_or_default();
    for name in headers.keys().cloned().collect::<Vec<_>>() {
        if name.eq_ignore_ascii_case("content-type") {
            headers.remove(&name);
        }
    }
    headers.insert("Content-Type".into(), json!(["application/json"]));
    MessageRow {
        id: ids::new_message_id(),
        tenant_id: row.tenant_id.clone(),
        kind: kind.into(),
        state: "pending".into(),
        url: url.to_string(),
        topic_name: row.topic_name.clone(),
        endpoint_name: row.endpoint_name.clone(),
        queue_name: None,
        schedule_id: row.schedule_id.clone(),
        method: opts
            .and_then(|o| o.get("method"))
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_string(),
        headers: Value::Object(headers),
        body: Some(payload.into_bytes()),
        callback_url: None,
        callback_opts: None,
        failure_callback_url: None,
        failure_callback_opts: None,
        max_retries: opts
            .and_then(|o| o.get("retries"))
            .and_then(|v| v.as_i64())
            .unwrap_or(row.max_retries),
        retry_delay_expr: opts
            .and_then(|o| o.get("retryDelayExpr"))
            .and_then(|v| v.as_str())
            .map(String::from),
        timeout_ms: opts
            .and_then(|o| o.get("timeoutMs"))
            .and_then(|v| v.as_i64()),
        not_before: now,
        flow_control_key: None,
        fc_parallelism: None,
        fc_rate: None,
        fc_period_secs: None,
        labels: row.labels.clone(),
        redact_fields: None,
        caller_ip: row.caller_ip.clone(),
        retried: 0,
        leased_until: None,
        last_error: None,
        created_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::sha256_hex;

    fn row(redact: Value) -> MessageRow {
        MessageRow {
            id: "msg_source".into(),
            tenant_id: "t1".into(),
            kind: "message".into(),
            state: "leased".into(),
            url: "http://example.invalid/".into(),
            topic_name: None,
            endpoint_name: None,
            queue_name: None,
            schedule_id: None,
            method: "POST".into(),
            headers: json!({"X-Secret": ["hunter2"], "X-Open": ["visible"]}),
            body: Some(b"payload".to_vec()),
            callback_url: None,
            callback_opts: None,
            failure_callback_url: None,
            failure_callback_opts: None,
            max_retries: 3,
            retry_delay_expr: None,
            timeout_ms: None,
            not_before: 1_000,
            flow_control_key: None,
            fc_parallelism: None,
            fc_rate: None,
            fc_period_secs: None,
            labels: vec![],
            redact_fields: Some(redact),
            caller_ip: None,
            retried: 1,
            leased_until: None,
            last_error: None,
            created_at: 900,
        }
    }

    fn response() -> Option<DeliveryResponse> {
        Some(DeliveryResponse {
            status: Some(200),
            headers: Some(json!({"z-last": ["1"], "A-First": ["2"], "a-lower": ["3"]})),
            body: Some(b"ok".to_vec()),
        })
    }

    #[test]
    fn response_header_keys_are_byte_sorted_not_case_folded() {
        let body = build_callback_body(&row(json!({})), &response(), Some("dlq_1"));
        let keys: Vec<&str> = body["header"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, vec!["A-First", "a-lower", "z-last"]);
        assert_eq!(body["dlqId"], json!("dlq_1"));
        assert_eq!(body["notBefore"], json!("1000"));
        assert_eq!(body["createdAt"], json!("900"));
    }

    #[test]
    fn redaction_replaces_body_and_named_headers() {
        let body = build_callback_body(
            &row(json!({"body": true, "headerNames": ["x-secret"]})),
            &response(),
            None,
        );
        let want_body = format!("REDACTED:{}", sha256_hex(b"payload"));
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, want_body);
        assert_eq!(body["sourceBody"], json!(encoded));
        assert_eq!(
            body["sourceHeader"]["X-Secret"],
            json!([format!("REDACTED:{}", sha256_hex(b"hunter2"))])
        );
        assert_eq!(body["sourceHeader"]["X-Open"], json!(["visible"]));
    }

    #[test]
    fn callback_row_has_no_recursion_and_one_content_type() {
        let opts = json!({
            "forwardHeaders": {"content-type": ["text/plain"], "X-Keep": ["1"]},
            "method": "PUT",
            "retries": 7
        });
        let cb = build_callback_row(
            &row(json!({})),
            "callback",
            "http://callback.invalid/",
            Some(&opts),
            &response(),
            None,
        );
        assert_eq!(cb.kind, "callback");
        assert_eq!(cb.method, "PUT");
        assert_eq!(cb.max_retries, 7);
        assert!(cb.callback_url.is_none());
        assert!(cb.failure_callback_url.is_none());
        let ct: Vec<&String> = cb
            .headers
            .as_object()
            .unwrap()
            .keys()
            .filter(|k| k.eq_ignore_ascii_case("content-type"))
            .collect();
        assert_eq!(ct.len(), 1, "exactly one content-type survives");
        assert_eq!(cb.headers["Content-Type"], json!(["application/json"]));
        // The body is the documented JSON, reparsable and tied to the source id.
        let payload: Value = serde_json::from_slice(cb.body.as_ref().unwrap()).unwrap();
        assert_eq!(payload["sourceMessageId"], json!("msg_source"));
        assert_eq!(payload["status"], json!(200));
    }
}
