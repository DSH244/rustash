//! Byte-level callback JSON, retry-policy and fenced-completion contract tests.
//!
//! These tests exercise stable wire and transactional behavior through the public API,
//! so internal changes cannot silently change what LobeHub observes.

use rusqlite::Connection;
use serde_json::{json, Value};

use rustash::backoff::retry_delay_ms_for;
use rustash::callback::build_callback_body;
use rustash::db::apply_migrations;
use rustash::ids;
use rustash::model::{DeliveryResponse, MessageRow};
use rustash::store::{
    delete_leased_with_callback, insert_message, is_cancel_requested, select_message_by_id,
};

fn anchor_row(redact: Option<Value>) -> MessageRow {
    MessageRow {
        id: "msg_anchor".into(),
        tenant_id: "t1".into(),
        kind: "message".into(),
        state: "leased".into(),
        url: "https://sink.invalid/ok".into(),
        topic_name: None,
        endpoint_name: None,
        queue_name: None,
        schedule_id: None,
        method: "POST".into(),
        headers: json!({"x-secret": ["token"], "content-type": ["application/json"]}),
        body: Some(b"hi".to_vec()),
        callback_url: None,
        callback_opts: None,
        failure_callback_url: None,
        failure_callback_opts: None,
        max_retries: 3,
        retry_delay_expr: None,
        timeout_ms: None,
        not_before: 1000,
        flow_control_key: None,
        fc_parallelism: None,
        fc_rate: None,
        fc_period_secs: None,
        labels: vec!["label-a".into()],
        redact_fields: redact,
        caller_ip: None,
        retried: 1,
        leased_until: None,
        last_error: None,
        created_at: 2000,
    }
}

fn response() -> Option<DeliveryResponse> {
    Some(DeliveryResponse {
        status: Some(200),
        headers: Some(json!({"z-last": ["1"], "a-first": ["2"]})),
        body: Some(b"ok".to_vec()),
    })
}

// Byte-exact contract values. Do not regenerate these literals as part of an
// implementation change; update them only after an intentional protocol change is reviewed.
const PLAIN_ANCHOR: &str = r#"{"status":200,"header":{"a-first":["2"],"z-last":["1"]},"body":"b2s=","retried":1,"maxRetries":3,"sourceMessageId":"msg_anchor","url":"https://sink.invalid/ok","method":"POST","sourceHeader":{"x-secret":["token"],"content-type":["application/json"]},"sourceBody":"aGk=","notBefore":"1000","createdAt":"2000","dlqId":"dlq_anchor"}"#;
const REDACTED_ANCHOR: &str = r#"{"status":200,"header":{"a-first":["2"],"z-last":["1"]},"body":"b2s=","retried":1,"maxRetries":3,"sourceMessageId":"msg_anchor","url":"https://sink.invalid/ok","method":"POST","sourceHeader":{"x-secret":["REDACTED:3c469e9d6c5875d37a43f353d4f88e61fcf812c66eee3457465a40b0da4153e0"],"content-type":["REDACTED:bacb769b46f6d169fb227ea026550f411d46cbe66a9c2a6ba36449c8cf8e4dea"]},"sourceBody":"UkVEQUNURUQ6OGY0MzQzNDY2NDhmNmI5NmRmODlkZGE5MDFjNTE3NmIxMGE2ZDgzOTYxZGQzYzFhYzg4YjU5YjJkYzMyN2FhNA==","notBefore":"1000","createdAt":"2000","dlqId":"dlq_anchor"}"#;

#[test]
fn callback_and_retry_anchors_are_byte_identical_to_baseline() {
    let plain = build_callback_body(&anchor_row(None), &response(), Some("dlq_anchor"));
    assert_eq!(plain.to_string(), PLAIN_ANCHOR);
    let redacted = build_callback_body(
        &anchor_row(Some(json!({"body": true, "headers": true}))),
        &response(),
        Some("dlq_anchor"),
    );
    assert_eq!(redacted.to_string(), REDACTED_ANCHOR);

    let mut custom = anchor_row(None);
    custom.retry_delay_expr = Some("retried * 7".into());
    assert_eq!(retry_delay_ms_for(&custom), 7);
    assert_eq!(retry_delay_ms_for(&anchor_row(None)), 12_182);
    let mut r1 = anchor_row(None);
    r1.retried = 1;
    assert_eq!(retry_delay_ms_for(&r1), 12_182);
    // A misbehaving expression falls back to the default curve instead of failing retry.
    let mut broken = custom.clone();
    broken.retry_delay_expr = Some("1 / (retried - retried)".into());
    assert_eq!(retry_delay_ms_for(&broken), 12_182);
}

fn fresh_db() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    apply_migrations(&mut conn).unwrap();
    conn
}

fn sample_row() -> MessageRow {
    MessageRow {
        id: ids::new_message_id(),
        tenant_id: "t1".into(),
        kind: "message".into(),
        state: "pending".into(),
        url: "https://x.example/".into(),
        topic_name: None,
        endpoint_name: None,
        queue_name: None,
        schedule_id: None,
        method: "POST".into(),
        headers: json!({"x-a": ["1"]}),
        body: Some(b"hi".to_vec()),
        callback_url: None,
        callback_opts: None,
        failure_callback_url: None,
        failure_callback_opts: None,
        max_retries: 3,
        retry_delay_expr: None,
        timeout_ms: None,
        not_before: rustash::time::now_ms(),
        flow_control_key: None,
        fc_parallelism: None,
        fc_rate: None,
        fc_period_secs: None,
        labels: vec![],
        redact_fields: None,
        caller_ip: None,
        retried: 0,
        leased_until: None,
        last_error: None,
        created_at: rustash::time::now_ms(),
    }
}

/// The success callback insert rides the SAME transaction as the fenced delete: a
/// reclaimed twin (retried bumped) can neither delete the row nor enqueue a duplicate
/// callback for an attempt it no longer owns.
#[test]
fn success_callback_is_fenced_with_the_delete() {
    let mut conn = fresh_db();
    let row = sample_row();
    insert_message(&conn, &row).unwrap();
    conn.execute(
        "UPDATE messages SET state='leased', leased_until=1 WHERE id=?1",
        rusqlite::params![row.id],
    )
    .unwrap();
    let mut cb = sample_row();
    cb.id = ids::new_message_id();
    cb.url = "https://sink.invalid/callback".into();
    cb.state = "pending".into();

    assert!(delete_leased_with_callback(&mut conn, &row.id, 0, Some(&cb)).unwrap());
    assert!(select_message_by_id(&conn, "t1", &row.id)
        .unwrap()
        .is_none());
    assert!(select_message_by_id(&conn, "t1", &cb.id).unwrap().is_some());

    // A stale twin: leased and live, but the attempt counter moved on. Passing the old
    // `retried` must lose the fence entirely.
    let other = sample_row();
    insert_message(&conn, &other).unwrap();
    conn.execute(
        "UPDATE messages SET state='leased', retried=7 WHERE id=?1",
        rusqlite::params![other.id],
    )
    .unwrap();
    assert!(!delete_leased_with_callback(&mut conn, &other.id, 0, Some(&cb)).unwrap());
    assert!(select_message_by_id(&conn, "t1", &other.id)
        .unwrap()
        .is_some());
    let callback_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE id=?1",
            rusqlite::params![cb.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        callback_count, 1,
        "only the winning attempt inserts a callback"
    );
}

#[test]
fn cancel_requested_probe_is_scoped_to_the_message_state() {
    let conn = fresh_db();
    let row = sample_row();
    insert_message(&conn, &row).unwrap();
    assert!(!is_cancel_requested(&conn, &row.id));
    conn.execute(
        "UPDATE messages SET state='cancel_requested' WHERE id=?1",
        rusqlite::params![row.id],
    )
    .unwrap();
    assert!(is_cancel_requested(&conn, &row.id));
}
