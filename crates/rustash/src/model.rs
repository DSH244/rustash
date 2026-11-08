//! Row structs and the JSON response shapes. Key order follows the QStash API reference
//! response examples so a consumer can diff a response against the documentation.

use serde_json::{json, Map, Value};

/// UTF-8 text when cleanly decodable, base64 otherwise (core/serialize.js bodyToText).
pub fn body_to_text(body: Option<&[u8]>) -> Option<String> {
    let body = body?;
    let decoded = String::from_utf8_lossy(body);
    if decoded.contains('\u{FFFD}') {
        use base64::Engine;
        Some(base64::engine::general_purpose::STANDARD.encode(body))
    } else {
        Some(decoded.into_owned())
    }
}

pub fn body_to_base64(body: Option<&[u8]>) -> Option<String> {
    let body = body?;
    use base64::Engine;
    Some(base64::engine::general_purpose::STANDARD.encode(body))
}

/// Insertion-ordered JSON object builder.
pub fn jmap<const N: usize>(pairs: [(&str, Option<Value>); N]) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        if let Some(v) = v {
            m.insert(k.to_string(), v);
        }
    }
    Value::Object(m)
}

/// `publishItemJson`: messageId (+url for group items) and deduplicated only when true.
pub fn publish_item_json(message_id: &str, url: Option<&str>, deduplicated: bool) -> Value {
    jmap([
        ("messageId", Some(json!(message_id))),
        ("url", url.map(|u| json!(u))),
        (
            "deduplicated",
            if deduplicated {
                Some(json!(true))
            } else {
                None
            },
        ),
    ])
}

#[derive(Debug, Clone)]
pub struct MessageRow {
    pub id: String,
    pub tenant_id: String,
    pub kind: String,
    pub state: String,
    pub url: String,
    pub topic_name: Option<String>,
    pub endpoint_name: Option<String>,
    pub queue_name: Option<String>,
    pub schedule_id: Option<String>,
    pub method: String,
    /// Stored forwarded-header map: { name: [values...] } (insertion-ordered JSON).
    pub headers: Value,
    pub body: Option<Vec<u8>>,
    pub callback_url: Option<String>,
    pub callback_opts: Option<Value>,
    pub failure_callback_url: Option<String>,
    pub failure_callback_opts: Option<Value>,
    pub max_retries: i64,
    pub retry_delay_expr: Option<String>,
    pub timeout_ms: Option<i64>,
    pub not_before: i64,
    pub flow_control_key: Option<String>,
    pub fc_parallelism: Option<i64>,
    pub fc_rate: Option<i64>,
    pub fc_period_secs: Option<i64>,
    pub labels: Vec<String>,
    pub redact_fields: Option<Value>,
    pub caller_ip: Option<String>,
    pub retried: i64,
    pub leased_until: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
}

impl MessageRow {
    /// GET /v2/messages/{id} response body.
    pub fn to_message_json(&self) -> Value {
        jmap([
            ("messageId", Some(json!(self.id))),
            ("topicName", self.topic_name.as_ref().map(|t| json!(t))),
            ("url", Some(json!(&self.url))),
            ("method", Some(json!(&self.method))),
            ("header", Some(self.headers.clone())),
            (
                "body",
                Some(
                    body_to_text(self.body.as_deref())
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
            ),
            ("createdAt", Some(json!(self.created_at))),
            (
                "state",
                Some(json!(if self.state == "leased" {
                    "ACTIVE"
                } else {
                    "CREATED"
                })),
            ),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct DlqRow {
    pub dlq_id: String,
    pub tenant_id: String,
    pub message_id: String,
    pub kind: String,
    pub url: String,
    pub topic_name: Option<String>,
    pub endpoint_name: Option<String>,
    pub queue_name: Option<String>,
    pub schedule_id: Option<String>,
    pub method: String,
    pub headers: Value,
    pub body: Option<Vec<u8>>,
    pub callback_url: Option<String>,
    pub callback_opts: Option<Value>,
    pub failure_callback_url: Option<String>,
    pub failure_callback_opts: Option<Value>,
    pub max_retries: i64,
    pub retry_delay_expr: Option<String>,
    pub timeout_ms: Option<i64>,
    pub flow_control_key: Option<String>,
    pub fc_parallelism: Option<i64>,
    pub fc_rate: Option<i64>,
    pub fc_period_secs: Option<i64>,
    pub labels: Vec<String>,
    pub caller_ip: Option<String>,
    pub retried: i64,
    pub last_error: Option<String>,
    pub response_status: Option<i64>,
    pub response_headers: Option<Value>,
    pub response_body: Option<Vec<u8>>,
    pub created_at: i64,
    pub dlq_created_at: i64,
}

impl DlqRow {
    /// DLQ list entry — key order matches the QStash API reference, absent fields omitted.
    pub fn to_dlq_json(&self) -> Value {
        jmap([
            ("dlqId", Some(json!(&self.dlq_id))),
            ("messageId", Some(json!(&self.message_id))),
            ("url", Some(json!(&self.url))),
            ("topicName", self.topic_name.as_ref().map(|t| json!(t))),
            (
                "endpointName",
                self.endpoint_name.as_ref().map(|t| json!(t)),
            ),
            ("scheduleId", self.schedule_id.as_ref().map(|t| json!(t))),
            ("queueName", self.queue_name.as_ref().map(|t| json!(t))),
            ("method", Some(json!(&self.method))),
            ("header", Some(self.headers.clone())),
            (
                "body",
                body_to_text(self.body.as_deref()).map(Value::String),
            ),
            ("maxRetries", Some(json!(self.max_retries))),
            ("createdAt", Some(json!(self.created_at))),
            ("state", Some(json!("FAILED"))),
            ("responseStatus", self.response_status.map(|s| json!(s))),
            (
                "responseBody",
                body_to_text(self.response_body.as_deref()).map(Value::String),
            ),
            ("responseHeader", self.response_headers.clone()),
            ("callerIP", self.caller_ip.as_ref().map(|t| json!(t))),
            ("labels", Some(json!(&self.labels))),
        ])
    }
}

/// The response kept from one delivery attempt. It is a row-level value shared by
/// `store` (DLQ snapshots), `callback` (callback bodies) and `delivery` (the wire
/// response), so it belongs to the model rather than to any one of those layers.
#[derive(Debug, Clone)]
pub struct DeliveryResponse {
    pub status: Option<i64>,
    pub headers: Option<Value>,
    pub body: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct EventExtras {
    pub error: Option<String>,
    pub next_delivery_time: Option<i64>,
    pub response_status: Option<i64>,
    pub response_body: Option<Vec<u8>>,
    pub response_headers: Option<Value>,
}

/// A stored `message_events` row (GET /v2/logs & /v2/events).
#[derive(Debug, Clone)]
pub struct EventRow {
    pub id: i64,
    pub time: i64,
    pub message_id: String,
    pub state: String,
    pub error: Option<String>,
    pub next_delivery_time: Option<i64>,
    pub url: String,
    pub topic_name: Option<String>,
    pub endpoint_name: Option<String>,
    pub schedule_id: Option<String>,
    pub queue_name: Option<String>,
    pub method: String,
    pub headers: Value,
    pub body_base64: Option<String>,
    pub response_status: Option<i64>,
    pub response_body_base64: Option<String>,
    pub response_headers: Option<Value>,
    pub timeout_ms: Option<i64>,
    pub callback_url: Option<String>,
    pub failure_callback_url: Option<String>,
    pub max_retries: i64,
    pub retried: i64,
    pub retry_delay_expr: Option<String>,
    pub labels: Vec<String>,
}

impl EventRow {
    /// Log entry key order per the QStash `GET /v2/logs` response example.
    pub fn to_log_json(&self) -> Value {
        jmap([
            ("time", Some(json!(self.time))),
            ("messageId", Some(json!(&self.message_id))),
            ("state", Some(json!(&self.state))),
            ("error", self.error.as_ref().map(|e| json!(e))),
            (
                "nextDeliveryTime",
                self.next_delivery_time.map(|t| json!(t)),
            ),
            ("url", Some(json!(&self.url))),
            ("topicName", self.topic_name.as_ref().map(|t| json!(t))),
            (
                "endpointName",
                self.endpoint_name.as_ref().map(|t| json!(t)),
            ),
            ("scheduleId", self.schedule_id.as_ref().map(|t| json!(t))),
            ("queueName", self.queue_name.as_ref().map(|t| json!(t))),
            ("method", Some(json!(&self.method))),
            ("header", Some(self.headers.clone())),
            ("body", self.body_base64.as_ref().map(|b| json!(b))),
            ("responseStatus", self.response_status.map(|s| json!(s))),
            (
                "responseBody",
                self.response_body_base64.as_ref().map(|b| json!(b)),
            ),
            ("responseHeader", self.response_headers.clone()),
            ("timeout", self.timeout_ms.map(|t| json!(t))),
            ("callback", self.callback_url.as_ref().map(|c| json!(c))),
            (
                "failureCallback",
                self.failure_callback_url.as_ref().map(|c| json!(c)),
            ),
            ("maxRetries", Some(json!(self.max_retries))),
            ("retried", Some(json!(self.retried))),
            ("labels", Some(json!(&self.labels))),
        ])
    }
}
