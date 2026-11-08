//! Persistence core (`engine/store.js` + `engine/lease.js` + `engine/events.js` ported to
//! rusqlite): createMessage with the dedupe-window transaction, fenced delete/retry/DLQ
//! moves, event appends, the lease claim statement, cancel, and DLQ requeue.

use rusqlite::{params, Connection, Row, Transaction};
use serde_json::Value;

use crate::headers::{spec_not_before, MessageSpec};
use crate::ids;
pub use crate::model::DeliveryResponse;
use crate::model::{body_to_base64, DlqRow, EventExtras, EventRow, MessageRow};
pub use crate::outcome::{delete_leased_with_callback, is_cancel_requested};
use crate::time::now_ms;

pub const MESSAGE_INSERT_COLS: &str =
    "id, tenant_id, kind, state, url, topic_name, endpoint_name, queue_name, schedule_id, method, headers, body, callback_url, callback_opts, failure_callback_url, failure_callback_opts, max_retries, retry_delay_expr, timeout_ms, not_before, flow_control_key, fc_parallelism, fc_rate, fc_period_secs, labels, redact_fields, caller_ip, retried, leased_until, last_error, created_at";

/// Stable SQL strings: `prepare_cached` keys on the exact text, so `format!` on every
/// call would defeat the statement cache. The column list is a compile-time constant.
const MESSAGE_INSERT_SQL: &str = concat!(
    "INSERT INTO messages (id, tenant_id, kind, state, url, topic_name, endpoint_name, queue_name, schedule_id, method, headers, body, callback_url, callback_opts, failure_callback_url, failure_callback_opts, max_retries, retry_delay_expr, timeout_ms, not_before, flow_control_key, fc_parallelism, fc_rate, fc_period_secs, labels, redact_fields, caller_ip, retried, leased_until, last_error, created_at) ",
    "VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31)"
);
const MESSAGE_SELECT_BY_ID_SQL: &str = concat!(
    "SELECT id, tenant_id, kind, state, url, topic_name, endpoint_name, queue_name, schedule_id, method, headers, body, callback_url, callback_opts, failure_callback_url, failure_callback_opts, max_retries, retry_delay_expr, timeout_ms, not_before, flow_control_key, fc_parallelism, fc_rate, fc_period_secs, labels, redact_fields, caller_ip, retried, leased_until, last_error, created_at ",
    "FROM messages WHERE id = ?1 AND tenant_id = ?2"
);
const DELETE_LEASED_SQL: &str = concat!(
    "DELETE FROM messages WHERE id = ?1 AND retried = ?2 AND state IN ('leased','cancel_requested') ",
    "RETURNING id, tenant_id, kind, state, url, topic_name, endpoint_name, queue_name, schedule_id, method, headers, body, callback_url, callback_opts, failure_callback_url, failure_callback_opts, max_retries, retry_delay_expr, timeout_ms, not_before, flow_control_key, fc_parallelism, fc_rate, fc_period_secs, labels, redact_fields, caller_ip, retried, leased_until, last_error, created_at"
);

pub fn row_to_message(row: &Row<'_>) -> rusqlite::Result<MessageRow> {
    let headers_json: String = row.get::<_, String>(10)?;
    let labels_json: String = row.get::<_, String>(24)?;
    Ok(MessageRow {
        id: row.get(0)?,
        tenant_id: row.get(1)?,
        kind: row.get(2)?,
        state: row.get(3)?,
        url: row.get(4)?,
        topic_name: row.get(5)?,
        endpoint_name: row.get(6)?,
        queue_name: row.get(7)?,
        schedule_id: row.get(8)?,
        method: row.get(9)?,
        headers: serde_json::from_str(&headers_json)
            .unwrap_or_else(|_| Value::Object(Default::default())),
        body: row.get::<_, Option<Vec<u8>>>(11)?,
        callback_url: row.get(12)?,
        callback_opts: json_col(row, 13)?,
        failure_callback_url: row.get(14)?,
        failure_callback_opts: json_col(row, 15)?,
        max_retries: row.get(16)?,
        retry_delay_expr: row.get(17)?,
        timeout_ms: row.get(18)?,
        not_before: row.get(19)?,
        flow_control_key: row.get(20)?,
        fc_parallelism: row.get(21)?,
        fc_rate: row.get(22)?,
        fc_period_secs: row.get(23)?,
        labels: serde_json::from_str(&labels_json).unwrap_or_default(),
        redact_fields: json_col(row, 25)?,
        caller_ip: row.get(26)?,
        retried: row.get(27)?,
        leased_until: row.get(28)?,
        last_error: row.get(29)?,
        created_at: row.get(30)?,
    })
}

fn json_col(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<Value>> {
    let s: Option<String> = row.get(idx)?;
    match s {
        None => Ok(None),
        Some(s) => Ok(serde_json::from_str(&s).ok()),
    }
}

/// specToRow: build a messages row from a parsed spec (publish/enqueue/batch/fire).
pub fn spec_to_row(
    tenant_id: &str,
    url: &str,
    body: Option<Vec<u8>>,
    spec: &MessageSpec,
    now: i64,
) -> MessageRow {
    MessageRow {
        id: ids::new_message_id(),
        tenant_id: tenant_id.to_string(),
        kind: "message".into(),
        state: "pending".into(),
        url: url.to_string(),
        topic_name: None,
        endpoint_name: None,
        queue_name: spec.queue_name.clone(),
        schedule_id: None,
        method: spec.method.clone(),
        headers: spec.forward_headers.clone(),
        body,
        callback_url: spec.callback_url.clone(),
        callback_opts: spec.callback_opts.clone(),
        failure_callback_url: spec.failure_callback_url.clone(),
        failure_callback_opts: spec.failure_callback_opts.clone(),
        max_retries: spec.retries,
        retry_delay_expr: spec.retry_delay_expr.clone(),
        timeout_ms: spec.timeout_ms,
        not_before: spec_not_before(spec, now).unwrap_or(now),
        flow_control_key: spec.flow_control.as_ref().map(|f| f.key.clone()),
        fc_parallelism: spec.flow_control.as_ref().and_then(|f| f.parallelism),
        fc_rate: spec.flow_control.as_ref().and_then(|f| f.rate),
        fc_period_secs: spec.flow_control.as_ref().map(|f| f.period_secs),
        labels: spec.labels.clone(),
        redact_fields: spec.redact_fields.clone(),
        caller_ip: None,
        retried: 0,
        leased_until: None,
        last_error: None,
        created_at: now,
    }
}

pub fn insert_message(conn: &Connection, m: &MessageRow) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare_cached(MESSAGE_INSERT_SQL)?;
    stmt.execute(params![
        m.id,
        m.tenant_id,
        m.kind,
        m.state,
        m.url,
        m.topic_name,
        m.endpoint_name,
        m.queue_name,
        m.schedule_id,
        m.method,
        m.headers.to_string(),
        m.body,
        m.callback_url,
        m.callback_opts.as_ref().map(|v| v.to_string()),
        m.failure_callback_url,
        m.failure_callback_opts.as_ref().map(|v| v.to_string()),
        m.max_retries,
        m.retry_delay_expr,
        m.timeout_ms,
        m.not_before,
        m.flow_control_key,
        m.fc_parallelism,
        m.fc_rate,
        m.fc_period_secs,
        serde_json::to_string(&m.labels).unwrap(),
        m.redact_fields.as_ref().map(|v| v.to_string()),
        m.caller_ip,
        m.retried,
        m.leased_until,
        m.last_error,
        m.created_at,
    ])?;
    Ok(())
}

pub fn select_message_by_id(
    conn: &Connection,
    tenant_id: &str,
    id: &str,
) -> rusqlite::Result<Option<MessageRow>> {
    let mut stmt = conn.prepare_cached(MESSAGE_SELECT_BY_ID_SQL)?;
    let mut rows = stmt.query(params![id, tenant_id])?;
    match rows.next()? {
        Some(r) => Ok(Some(row_to_message(r)?)),
        None => Ok(None),
    }
}

/// Content-based dedupe key (core/dedupe.js): sha256 over
/// destination + " " + body + " " + content-type + " " + sorted `name=values,joined` + " ".
pub fn content_dedupe_key(
    destination: &str,
    body: Option<&[u8]>,
    content_type: Option<&str>,
    forward_headers: &Value,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(destination.as_bytes());
    h.update(b" ");
    if let Some(b) = body {
        h.update(b);
    }
    h.update(b" ");
    h.update(content_type.unwrap_or(""));
    h.update(b" ");
    if let Some(map) = forward_headers.as_object() {
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        for name in keys {
            h.update(name.to_ascii_lowercase().as_bytes());
            h.update(b"=");
            let joined: Vec<String> = map[name]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            h.update(joined.join(",").as_bytes());
            h.update(b" ");
        }
    }
    format!("content:{}", hex::encode(h.finalize()))
}

pub fn dedupe_key_for(spec: &MessageSpec, url: &str, body: Option<&[u8]>) -> Option<String> {
    if let Some(id) = &spec.dedupe_id {
        return Some(format!("id:{}", id));
    }
    if spec.content_based_dedup {
        return Some(content_dedupe_key(
            url,
            body,
            spec.content_type.as_deref(),
            &spec.forward_headers,
        ));
    }
    None
}

/// createMessage: insert honoring the dedupe TTL window. Duplicates return the ORIGINAL
/// messageId with deduplicated=true and create NO message row. Single transaction.
pub fn create_message(
    conn: &mut Connection,
    input_row: &MessageRow,
    dedupe_key: Option<&str>,
    dedupe_window_secs: i64,
) -> rusqlite::Result<(String, bool)> {
    let Some(key) = dedupe_key else {
        insert_message(conn, input_row)?;
        return Ok((input_row.id.clone(), false));
    };
    let now = input_row.created_at;
    let expires_at = now + dedupe_window_secs * 1000;
    let tx = conn.transaction()?;
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO dedupe_entries (tenant_id, dedupe_key, message_id, expires_at) VALUES (?1,?2,?3,?4)",
        params![input_row.tenant_id, key, input_row.id, expires_at],
    )?;
    if inserted == 0 {
        // Key exists: live entry → duplicate; expired entry → CAS takeover.
        let takeover = tx.execute(
            "UPDATE dedupe_entries SET message_id = ?3, expires_at = ?4 WHERE tenant_id = ?1 AND dedupe_key = ?2 AND expires_at <= ?5",
            params![input_row.tenant_id, key, input_row.id, expires_at, now],
        )?;
        if takeover == 0 {
            let existing: Option<String> = tx
                .query_row(
                    "SELECT message_id FROM dedupe_entries WHERE tenant_id = ?1 AND dedupe_key = ?2",
                    params![input_row.tenant_id, key],
                    |r| r.get(0),
                )
                .ok();
            tx.commit()?;
            return Ok((existing.unwrap_or_else(|| input_row.id.clone()), true));
        }
    }
    insert_message(&tx, input_row)?;
    tx.commit()?;
    Ok((input_row.id.clone(), false))
}

/// Fenced delete after successful delivery (or cancellation while leased).
pub fn delete_leased(
    conn: &Connection,
    id: &str,
    leased_retried: i64,
) -> rusqlite::Result<Option<MessageRow>> {
    let mut stmt = conn.prepare_cached(DELETE_LEASED_SQL)?;
    let mut rows = stmt.query(params![id, leased_retried])?;
    match rows.next()? {
        Some(r) => Ok(Some(row_to_message(r)?)),
        None => Ok(None),
    }
}

/// Push a live delivery's lease forward. Fenced on `(retried, state='leased')`, so a row
/// that was reclaimed by another claim (retried bumped) or cancelled stops matching and the
/// caller learns to stop renewing. Returns false when the fence no longer holds.
pub fn renew_lease(
    conn: &Connection,
    id: &str,
    leased_retried: i64,
    leased_until: i64,
) -> rusqlite::Result<bool> {
    let n = conn.execute(
        "UPDATE messages SET leased_until=?3 WHERE id=?1 AND retried=?2 AND state='leased'",
        params![id, leased_retried, leased_until],
    )?;
    Ok(n > 0)
}

/// Fenced retry: back to pending with the retry counter incremented and a new due time.
pub fn retry_at(
    conn: &Connection,
    id: &str,
    leased_retried: i64,
    error: &str,
    run_at: i64,
) -> rusqlite::Result<bool> {
    let n = conn.execute(
        "UPDATE messages SET state='pending', retried=?3, not_before=?4, leased_until=NULL, last_error=?5 WHERE id=?1 AND retried=?2 AND state='leased'",
        params![id, leased_retried, leased_retried + 1, run_at, error],
    )?;
    Ok(n > 0)
}

/// Exhausted: snapshot into the DLQ and remove from the hot table, atomically, fenced.
pub fn move_to_dlq(
    conn: &mut Connection,
    id: &str,
    leased_retried: i64,
    error: &str,
    response: &Option<DeliveryResponse>,
) -> rusqlite::Result<Option<(String, MessageRow)>> {
    let tx = conn.transaction()?;
    let row = delete_leased_conn(&tx, id, leased_retried)?;
    let Some(row) = row else {
        tx.commit()?;
        return Ok(None);
    };
    let dlq_id = ids::new_dlq_id();
    tx.execute(
        r#"INSERT INTO dlq_messages (dlq_id, tenant_id, message_id, kind, url, topic_name, endpoint_name, queue_name, schedule_id, method, headers, body, callback_url, callback_opts, failure_callback_url, failure_callback_opts, max_retries, retry_delay_expr, timeout_ms, flow_control_key, fc_parallelism, fc_rate, fc_period_secs, labels, caller_ip, retried, last_error, response_status, response_headers, response_body, created_at, dlq_created_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32)"#,
        params![
            dlq_id,
            row.tenant_id,
            row.id,
            row.kind,
            row.url,
            row.topic_name,
            row.endpoint_name,
            row.queue_name,
            row.schedule_id,
            row.method,
            row.headers.to_string(),
            row.body,
            row.callback_url,
            row.callback_opts.as_ref().map(|v| v.to_string()),
            row.failure_callback_url,
            row.failure_callback_opts.as_ref().map(|v| v.to_string()),
            row.max_retries,
            row.retry_delay_expr,
            row.timeout_ms,
            row.flow_control_key,
            row.fc_parallelism,
            row.fc_rate,
            row.fc_period_secs,
            serde_json::to_string(&row.labels).unwrap(),
            row.caller_ip,
            row.retried,
            error,
            response.as_ref().and_then(|r| r.status),
            response
                .as_ref()
                .and_then(|r| r.headers.as_ref())
                .map(|v| v.to_string()),
            response.as_ref().and_then(|r| r.body.clone()),
            row.created_at,
            now_ms(),
        ],
    )?;
    tx.commit()?;
    Ok(Some((dlq_id, row)))
}

fn delete_leased_conn(
    tx: &Transaction,
    id: &str,
    leased_retried: i64,
) -> rusqlite::Result<Option<MessageRow>> {
    let mut stmt = tx.prepare_cached(DELETE_LEASED_SQL)?;
    let mut rows = stmt.query(params![id, leased_retried])?;
    match rows.next()? {
        Some(r) => Ok(Some(row_to_message(r)?)),
        None => Ok(None),
    }
}

const EVENT_INSERT_SQL: &str = r#"INSERT INTO message_events (tenant_id, time, message_id, state, error, next_delivery_time, url, topic_name, endpoint_name, schedule_id, queue_name, method, headers, body_base64, response_status, response_body_base64, response_headers, timeout_ms, callback_url, failure_callback_url, max_retries, retried, retry_delay_expr, labels)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)"#;

/// appendEvent — one lifecycle row; best-effort outside state transactions.
///
/// `body_max` caps the message body copied into the event. QStash documents the log entry's
/// `body` as the message body, so small messages keep it on every lifecycle row. Large ones
/// do not, because the copy is not free: base64 inflates 1.33x and a message produces several
/// rows, so a 200 KiB payload wrote ~536 KiB of `message_events` per delivery and a 1 MiB
/// payload wrote ~2.7 MiB (both measured) — the amplification, not the delivery, was what
/// pushed publish throughput down as bodies grew (1 KiB → 442 req/s, 1 MiB → 29 req/s).
/// The full body is never lost: `/v2/messages/{id}` returns it while the message exists and
/// the DLQ snapshot keeps it after a failure.
pub fn append_event(
    conn: &Connection,
    m: &MessageRow,
    state: &str,
    extras: &EventExtras,
    body_max: usize,
) -> rusqlite::Result<()> {
    let stored_body = m.body.as_deref().filter(|b| b.len() <= body_max);
    let mut stmt = conn.prepare_cached(EVENT_INSERT_SQL)?;
    stmt.execute(params![
        m.tenant_id,
        now_ms(),
        m.id,
        state,
        extras.error,
        extras.next_delivery_time,
        m.url,
        m.topic_name,
        m.endpoint_name,
        m.schedule_id,
        m.queue_name,
        m.method,
        m.headers.to_string(),
        body_to_base64(stored_body),
        extras.response_status,
        body_to_base64(extras.response_body.as_deref()),
        extras.response_headers.as_ref().map(|v| v.to_string()),
        m.timeout_ms,
        m.callback_url,
        m.failure_callback_url,
        m.max_retries,
        m.retried,
        m.retry_delay_expr,
        serde_json::to_string(&m.labels).unwrap(),
    ])?;
    Ok(())
}

pub fn row_to_event(row: &Row<'_>) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        id: row.get(0)?,
        time: row.get(1)?,
        message_id: row.get(2)?,
        state: row.get(3)?,
        error: row.get(4)?,
        next_delivery_time: row.get(5)?,
        url: row.get(6)?,
        topic_name: row.get(7)?,
        endpoint_name: row.get(8)?,
        schedule_id: row.get(9)?,
        queue_name: row.get(10)?,
        method: row.get(11)?,
        headers: serde_json::from_str(&row.get::<_, String>(12)?)
            .unwrap_or_else(|_| Value::Object(Default::default())),
        body_base64: row.get(13)?,
        response_status: row.get(14)?,
        response_body_base64: row.get(15)?,
        response_headers: json_col(row, 16)?,
        timeout_ms: row.get(17)?,
        callback_url: row.get(18)?,
        failure_callback_url: row.get(19)?,
        max_retries: row.get(20)?,
        retried: row.get(21)?,
        retry_delay_expr: row.get(22)?,
        labels: serde_json::from_str(&row.get::<_, String>(23)?).unwrap_or_default(),
    })
}

pub const EVENT_SELECT_COLS: &str =
    "id, time, message_id, state, error, next_delivery_time, url, topic_name, endpoint_name, schedule_id, queue_name, method, headers, body_base64, response_status, response_body_base64, response_headers, timeout_ms, callback_url, failure_callback_url, max_retries, retried, retry_delay_expr, labels";

/// cancelMessageById: pending → delete + CANCELLED; leased → cancel_requested +
/// CANCEL_REQUESTED. None when the message is gone.
pub fn cancel_message_by_id(
    conn: &Connection,
    tenant_id: &str,
    message_id: &str,
) -> rusqlite::Result<Option<String>> {
    let Some(row) = select_message_by_id(conn, tenant_id, message_id)? else {
        return Ok(None);
    };
    if row.state == "leased" {
        conn.execute(
            "UPDATE messages SET state='cancel_requested' WHERE id=?1 AND tenant_id=?2",
            params![message_id, tenant_id],
        )?;
        append_event(
            conn,
            &row,
            "CANCEL_REQUESTED",
            &EventExtras::default(),
            usize::MAX,
        )?;
        return Ok(Some("cancel_requested".into()));
    }
    conn.execute(
        "DELETE FROM messages WHERE id=?1 AND tenant_id=?2",
        params![message_id, tenant_id],
    )?;
    append_event(conn, &row, "CANCELLED", &EventExtras::default(), usize::MAX)?;
    Ok(Some("cancelled".into()))
}

pub fn dlq_cols() -> &'static str {
    "dlq_id, tenant_id, message_id, kind, url, topic_name, endpoint_name, queue_name, schedule_id, method, headers, body, callback_url, callback_opts, failure_callback_url, failure_callback_opts, max_retries, retry_delay_expr, timeout_ms, flow_control_key, fc_parallelism, fc_rate, fc_period_secs, labels, caller_ip, retried, last_error, response_status, response_headers, response_body, created_at, dlq_created_at"
}

pub fn row_to_dlq(row: &Row<'_>) -> rusqlite::Result<DlqRow> {
    Ok(DlqRow {
        dlq_id: row.get(0)?,
        tenant_id: row.get(1)?,
        message_id: row.get(2)?,
        kind: row.get(3)?,
        url: row.get(4)?,
        topic_name: row.get(5)?,
        endpoint_name: row.get(6)?,
        queue_name: row.get(7)?,
        schedule_id: row.get(8)?,
        method: row.get(9)?,
        headers: serde_json::from_str(&row.get::<_, String>(10)?)
            .unwrap_or_else(|_| Value::Object(Default::default())),
        body: row.get::<_, Option<Vec<u8>>>(11)?,
        callback_url: row.get(12)?,
        callback_opts: json_col(row, 13)?,
        failure_callback_url: row.get(14)?,
        failure_callback_opts: json_col(row, 15)?,
        max_retries: row.get(16)?,
        retry_delay_expr: row.get(17)?,
        timeout_ms: row.get(18)?,
        flow_control_key: row.get(19)?,
        fc_parallelism: row.get(20)?,
        fc_rate: row.get(21)?,
        fc_period_secs: row.get(22)?,
        labels: serde_json::from_str(&row.get::<_, String>(23)?).unwrap_or_default(),
        caller_ip: row.get(24)?,
        retried: row.get(25)?,
        last_error: row.get(26)?,
        response_status: row.get(27)?,
        response_headers: json_col(row, 28)?,
        response_body: row.get::<_, Option<Vec<u8>>>(29)?,
        created_at: row.get(30)?,
        dlq_created_at: row.get(31)?,
    })
}

/// requeueDlqRow: re-inject as a fresh pending message (retried=0, no dedupe) and delete
/// the DLQ row atomically. Returns the new messageId.
pub fn requeue_dlq_row(conn: &mut Connection, row: &DlqRow) -> rusqlite::Result<String> {
    let now = now_ms();
    let fresh = MessageRow {
        id: ids::new_message_id(),
        tenant_id: row.tenant_id.clone(),
        kind: row.kind.clone(),
        state: "pending".into(),
        url: row.url.clone(),
        topic_name: row.topic_name.clone(),
        endpoint_name: row.endpoint_name.clone(),
        queue_name: row.queue_name.clone(),
        schedule_id: row.schedule_id.clone(),
        method: row.method.clone(),
        headers: row.headers.clone(),
        body: row.body.clone(),
        callback_url: row.callback_url.clone(),
        callback_opts: row.callback_opts.clone(),
        failure_callback_url: row.failure_callback_url.clone(),
        failure_callback_opts: row.failure_callback_opts.clone(),
        max_retries: row.max_retries,
        retry_delay_expr: row.retry_delay_expr.clone(),
        timeout_ms: row.timeout_ms,
        not_before: now,
        flow_control_key: row.flow_control_key.clone(),
        fc_parallelism: row.fc_parallelism,
        fc_rate: row.fc_rate,
        fc_period_secs: row.fc_period_secs,
        labels: row.labels.clone(),
        redact_fields: None,
        caller_ip: row.caller_ip.clone(),
        retried: 0,
        leased_until: None,
        last_error: None,
        created_at: now,
    };
    let tx = conn.transaction()?;
    insert_message(&tx, &fresh)?;
    tx.execute(
        "DELETE FROM dlq_messages WHERE dlq_id = ?1",
        params![row.dlq_id],
    )?;
    tx.commit()?;
    Ok(fresh.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{apply_migrations, open_db};
    use rusqlite::Connection;

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
            headers: serde_json::json!({"x-a": ["1"]}),
            body: Some(b"hi".to_vec()),
            callback_url: None,
            callback_opts: None,
            failure_callback_url: None,
            failure_callback_opts: None,
            max_retries: 3,
            retry_delay_expr: None,
            timeout_ms: None,
            not_before: now_ms(),
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
            created_at: now_ms(),
        }
    }

    #[test]
    fn insert_and_select_roundtrip() {
        let conn = fresh_db();
        let mut row = sample_row();
        row.created_at = 123;
        row.not_before = 123;
        insert_message(&conn, &row).unwrap();
        let got = select_message_by_id(&conn, "t1", &row.id).unwrap().unwrap();
        assert_eq!(got.headers, serde_json::json!({"x-a": ["1"]}));
        assert_eq!(got.body.as_deref(), Some(b"hi".as_slice()));
        assert_eq!(got.created_at, 123);
        assert!(select_message_by_id(&conn, "other", &row.id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn dedupe_window_drops_duplicates_and_takes_over_on_expiry() {
        let mut conn = fresh_db();
        let row = sample_row();
        let (id, dup) = create_message(&mut conn, &row, Some("id:k"), 600).unwrap();
        assert_eq!(id, row.id);
        assert!(!dup);

        // duplicate inside the window: original id, no new row
        let row2 = sample_row();
        let (id2, dup2) = create_message(&mut conn, &row2, Some("id:k"), 600).unwrap();
        assert_eq!(id2, row.id);
        assert!(dup2);
        assert!(select_message_by_id(&conn, "t1", &row2.id)
            .unwrap()
            .is_none());

        // expired entry: CAS takeover, fresh message
        conn.execute(
            "UPDATE dedupe_entries SET expires_at = 1 WHERE dedupe_key='id:k'",
            [],
        )
        .unwrap();
        let row3 = sample_row();
        let (id3, dup3) = create_message(&mut conn, &row3, Some("id:k"), 600).unwrap();
        assert_eq!(id3, row3.id);
        assert!(!dup3);
        assert!(select_message_by_id(&conn, "t1", &row3.id)
            .unwrap()
            .is_some());
    }

    #[test]
    fn renew_lease_extends_only_the_lease_this_worker_owns() {
        let conn = fresh_db();
        let row = sample_row();
        insert_message(&conn, &row).unwrap();
        conn.execute(
            "UPDATE messages SET state='leased', leased_until=1000 WHERE id=?1",
            params![row.id],
        )
        .unwrap();

        // Owner (retried=0, leased) pushes the deadline forward.
        assert!(renew_lease(&conn, &row.id, 0, 9_999).unwrap());
        let got = select_message_by_id(&conn, "t1", &row.id).unwrap().unwrap();
        assert_eq!(got.leased_until, Some(9_999));

        // A reclaimed twin (retried bumped by the claim) no longer owns it.
        conn.execute(
            "UPDATE messages SET retried = 1 WHERE id=?1",
            params![row.id],
        )
        .unwrap();
        assert!(!renew_lease(&conn, &row.id, 0, 20_000).unwrap());
        let got = select_message_by_id(&conn, "t1", &row.id).unwrap().unwrap();
        assert_eq!(
            got.leased_until,
            Some(9_999),
            "a stale worker must not extend the lease"
        );

        // Same for a row that left the leased state.
        conn.execute(
            "UPDATE messages SET state='pending' WHERE id=?1",
            params![row.id],
        )
        .unwrap();
        assert!(!renew_lease(&conn, &row.id, 1, 30_000).unwrap());
    }

    #[test]
    fn fenced_retry_and_leased_delete() {
        let conn = fresh_db();
        let row = sample_row();
        insert_message(&conn, &row).unwrap();
        conn.execute(
            "UPDATE messages SET state='leased', leased_until=99999999999999 WHERE id=?1",
            params![row.id],
        )
        .unwrap();
        // fence hit at retried=0
        assert!(retry_at(&conn, &row.id, 0, "boom", 42).unwrap());
        let got = select_message_by_id(&conn, "t1", &row.id).unwrap().unwrap();
        assert_eq!(got.state, "pending");
        assert_eq!(got.retried, 1);
        assert_eq!(got.not_before, 42);
        assert_eq!(got.last_error.as_deref(), Some("boom"));
        // stale fence (wrong retried) no-ops
        assert!(!retry_at(&conn, &row.id, 0, "stale", 1).unwrap());
        // fenced delete requires leased/cancel_requested
        assert!(delete_leased(&conn, &row.id, 1).unwrap().is_none());
        conn.execute(
            "UPDATE messages SET state='leased', leased_until=1 WHERE id=?1",
            params![row.id],
        )
        .unwrap();
        let deleted = delete_leased(&conn, &row.id, 1).unwrap();
        assert!(deleted.is_some());
        assert!(select_message_by_id(&conn, "t1", &row.id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn move_to_dlq_snapshots_response_and_fences() {
        let mut conn = fresh_db();
        let row = sample_row();
        insert_message(&conn, &row).unwrap();
        conn.execute(
            "UPDATE messages SET state='leased' WHERE id=?1",
            params![row.id],
        )
        .unwrap();
        let resp = Some(DeliveryResponse {
            status: Some(500),
            headers: Some(serde_json::json!({"retry-after": ["12"]})),
            body: Some(b"nope".to_vec()),
        });
        let (dlq_id, moved) = move_to_dlq(&mut conn, &row.id, 0, "endpoint returned 500", &resp)
            .unwrap()
            .unwrap();
        assert!(dlq_id.starts_with("dlq_"));
        assert_eq!(moved.id, row.id);
        assert!(delete_leased(&conn, &row.id, 0).unwrap().is_none()); // gone
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {} FROM dlq_messages WHERE dlq_id=?1",
                dlq_cols()
            ))
            .unwrap();
        let d = stmt.query_row(params![dlq_id], row_to_dlq).unwrap();
        assert_eq!(d.response_status, Some(500));
        assert_eq!(d.last_error.as_deref(), Some("endpoint returned 500"));
        assert_eq!(d.body.as_deref(), Some(b"hi".as_slice()));
    }

    #[test]
    fn requeue_resets_retried_and_removes_dlq_row() {
        let mut conn = fresh_db();
        let row = sample_row();
        insert_message(&conn, &row).unwrap();
        conn.execute(
            "UPDATE messages SET state='leased', retried=3 WHERE id=?1",
            params![row.id],
        )
        .unwrap();
        let resp = None;
        let (dlq_id, _) = move_to_dlq(&mut conn, &row.id, 3, "exhausted", &resp)
            .unwrap()
            .unwrap();
        let d = {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {} FROM dlq_messages WHERE dlq_id=?1",
                    dlq_cols()
                ))
                .unwrap();
            stmt.query_row(params![dlq_id], row_to_dlq).unwrap()
        };
        let new_id = requeue_dlq_row(&mut conn, &d).unwrap();
        assert!(new_id.starts_with("msg_"));
        assert_ne!(new_id, row.id);
        let fresh = select_message_by_id(&conn, "t1", &new_id).unwrap().unwrap();
        assert_eq!(fresh.retried, 0);
        assert_eq!(fresh.state, "pending");
        assert_eq!(fresh.headers, row.headers);
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM dlq_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn content_dedupe_key_matches_js_shape() {
        // destination "https://e/", body "hi", CT "text/plain", one forwarded header.
        let k = content_dedupe_key(
            "https://e/",
            Some(b"hi"),
            Some("text/plain"),
            &serde_json::json!({"x-a": ["1", "2"]}),
        );
        // sha256("https://e/ hi text/plain x-a=1,2 ")  (space separators)
        use sha2::{Digest, Sha256};
        let want = format!(
            "content:{}",
            hex::encode(Sha256::digest(b"https://e/ hi text/plain x-a=1,2 "))
        );
        assert_eq!(k, want);
    }

    #[test]
    fn open_db_plus_apply_migrations_builds_ledger() {
        let db = open_db(":memory:");
        {
            let mut conn = db.conn.lock().unwrap();
            crate::db::apply_migrations(&mut conn).unwrap();
        }
        let mut conn = db.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM __migrations", [], |r| r.get(0))
            .unwrap();
        // One ledger row per declared migration — derived from the list itself so adding
        // a migration does not require editing this assertion.
        let want = crate::db::MIGRATIONS.len() as i64;
        assert_eq!(n, want);
        // Idempotent: re-applying is a no-op (ledger guards each version).
        crate::db::apply_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM __migrations", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, want);
    }
}
