//! Workflow durable execution support (engine/workflow.js + workflow-timeout.js):
//! step/run history, waiter registration/resolution, run cancellation and the waiter
//! timeout sweep.

use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};

use crate::ids;
use crate::model::body_to_base64;
use crate::time::now_ms;

/// Fetch a forwarded header by case-insensitive name (first value).
pub fn forwarded_header(headers: &Value, name: &str) -> Option<String> {
    let wanted = name.to_ascii_lowercase();
    let map = headers.as_object()?;
    for (key, value) in map {
        if key.to_ascii_lowercase() == wanted {
            if let Some(arr) = value.as_array() {
                return arr.first().and_then(|v| v.as_str()).map(String::from);
            }
            return value.as_str().map(String::from);
        }
    }
    None
}

/// The run id carrying a workflow message, if any.
pub fn workflow_run_id_of(headers: &Value) -> Option<String> {
    forwarded_header(headers, "upstash-workflow-runid")
}

/// `recordWorkflowRunStart`: RUN_STARTED on first sight; never overwrites (a later
/// step must not resurrect a run that already reached a terminal state).
pub fn record_run_start(
    conn: &Connection,
    tenant_id: &str,
    run_id: &str,
    workflow_url: &str,
    flow_control_key: Option<&str>,
) -> rusqlite::Result<()> {
    let now = now_ms();
    conn.execute(
        r#"INSERT INTO workflow_runs
             (tenant_id, workflow_run_id, workflow_url, state, dlq_id, flow_control_key, created_at, completed_at, updated_at)
           VALUES (?1, ?2, ?3, 'RUN_STARTED', NULL, ?4, ?5, NULL, ?5)
           ON CONFLICT(tenant_id, workflow_run_id) DO NOTHING"#,
        params![tenant_id, run_id, workflow_url, flow_control_key, now],
    )?;
    Ok(())
}

/// `recordWorkflowStep`: a normal message step, deduplicated per message id by the
/// partial unique index (wait/resolution markers carry an empty id and are exempt).
pub fn record_step(
    conn: &Connection,
    tenant_id: &str,
    run_id: &str,
    message_id: &str,
    body: Option<&[u8]>,
    headers: &Value,
) -> rusqlite::Result<()> {
    conn.execute(
        r#"INSERT OR IGNORE INTO workflow_steps
             (tenant_id, workflow_run_id, message_id, body, call_type, headers, created_at)
           VALUES (?1, ?2, ?3, ?4, 'step', ?5, ?6)"#,
        params![
            tenant_id,
            run_id,
            message_id,
            body,
            headers.to_string(),
            now_ms()
        ],
    )?;
    Ok(())
}

/// `appendWorkflowStep` (wait markers and notify/timeout resolutions): always inserted.
pub fn append_step(
    conn: &Connection,
    tenant_id: &str,
    run_id: &str,
    message_id: &str,
    body: Option<&[u8]>,
    call_type: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        r#"INSERT INTO workflow_steps
             (tenant_id, workflow_run_id, message_id, body, call_type, headers, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, '{}', ?6)"#,
        params![tenant_id, run_id, message_id, body, call_type, now_ms()],
    )?;
    Ok(())
}

/// `getRunSteps`: publish order; an empty/absent body serializes as `""` (never omitted).
pub fn get_run_steps_json(
    conn: &Connection,
    tenant_id: &str,
    run_id: &str,
) -> rusqlite::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT message_id, body, call_type FROM workflow_steps WHERE tenant_id = ?1 AND workflow_run_id = ?2 ORDER BY id",
    )?;
    let rows = stmt
        .query_map(params![tenant_id, run_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .filter_map(|x| x.ok())
        .map(|(message_id, body, call_type)| {
            json!({
                "messageId": message_id,
                "body": body_to_base64(body.as_deref()).unwrap_or_default(),
                "callType": call_type,
            })
        })
        .collect();
    Ok(rows)
}

/// `setWorkflowRunState`: terminal archive update; no-op for an unknown run.
/// `dlq_id` is always written, NULL when absent.
pub fn set_run_state(
    conn: &Connection,
    tenant_id: &str,
    run_id: &str,
    state: &str,
    dlq_id: Option<&str>,
) -> rusqlite::Result<()> {
    let now = now_ms();
    conn.execute(
        "UPDATE workflow_runs SET state = ?3, dlq_id = ?4, completed_at = ?5, updated_at = ?5 WHERE tenant_id = ?1 AND workflow_run_id = ?2",
        params![tenant_id, run_id, state, dlq_id, now],
    )?;
    Ok(())
}

/// Delete a run's still-queued deliveries (pending/leased rows whose forwarded headers
/// carry the matching `Upstash-Workflow-RunId`) so a canceled run stops waking up.
pub fn delete_run_messages(
    conn: &Connection,
    tenant_id: &str,
    run_id: &str,
) -> rusqlite::Result<usize> {
    conn.execute(
        r#"DELETE FROM messages
            WHERE tenant_id = ?1
              AND state IN ('pending', 'leased')
              AND EXISTS (
                    SELECT 1 FROM json_each(messages.headers) e
                     WHERE lower(e.key) = 'upstash-workflow-runid'
                       AND EXISTS (SELECT 1 FROM json_each(e.value) v WHERE v.value = ?2)
                  )"#,
        params![tenant_id, run_id],
    )
}

/// `cancelWorkflowRun`: fenced RUN_STARTED → RUN_CANCELED transition; on success drop
/// the run's waiters, steps and queued messages. Returns false if unknown/terminal.
pub fn cancel_run(conn: &mut Connection, tenant_id: &str, run_id: &str) -> rusqlite::Result<bool> {
    let tx = conn.transaction()?;
    let now = now_ms();
    let updated = tx.execute(
        "UPDATE workflow_runs SET state = 'RUN_CANCELED', completed_at = ?3, updated_at = ?3 WHERE tenant_id = ?1 AND workflow_run_id = ?2 AND state = 'RUN_STARTED'",
        params![tenant_id, run_id, now],
    )?;
    if updated == 0 {
        tx.commit()?;
        return Ok(false);
    }
    tx.execute(
        "DELETE FROM workflow_waiters WHERE tenant_id = ?1 AND workflow_run_id = ?2",
        params![tenant_id, run_id],
    )?;
    tx.execute(
        "DELETE FROM workflow_steps WHERE tenant_id = ?1 AND workflow_run_id = ?2",
        params![tenant_id, run_id],
    )?;
    tx.commit()?;
    delete_run_messages(conn, tenant_id, run_id)?;
    Ok(true)
}

#[derive(Debug, Clone)]
pub struct WaiterRow {
    pub id: String,
    pub tenant_id: String,
    pub event_id: String,
    pub workflow_run_id: Option<String>,
    pub waiter_id: String,
    pub resume_url: String,
    pub resume_headers: Value,
    pub step_meta: Value,
    pub timeout_url: Option<String>,
    pub timeout_headers: Option<Value>,
    pub timeout_at: i64,
    pub created_at: i64,
}

pub const WAITER_COLS: &str =
    "id, tenant_id, event_id, workflow_run_id, waiter_id, resume_url, resume_headers, step_meta, timeout_url, timeout_headers, timeout_at, created_at";

pub fn row_to_waiter(r: &rusqlite::Row<'_>) -> rusqlite::Result<WaiterRow> {
    let resume_headers: String = r.get(6)?;
    let step_meta: String = r.get(7)?;
    let timeout_headers: Option<String> = r.get(9)?;
    Ok(WaiterRow {
        id: r.get(0)?,
        tenant_id: r.get(1)?,
        event_id: r.get(2)?,
        workflow_run_id: r.get(3)?,
        waiter_id: r.get(4)?,
        resume_url: r.get(5)?,
        resume_headers: serde_json::from_str(&resume_headers).unwrap_or_else(|_| json!({})),
        step_meta: serde_json::from_str(&step_meta).unwrap_or_else(|_| json!({})),
        timeout_url: r.get(8)?,
        timeout_headers: timeout_headers.and_then(|s| serde_json::from_str(&s).ok()),
        timeout_at: r.get(10)?,
        created_at: r.get(11)?,
    })
}

/// The resolution-step payload shared by notify and timeout.
pub fn resolution_body(waiter: &WaiterRow, out_base64: &str, wait_timeout: bool) -> Value {
    let meta = &waiter.step_meta;
    let step = meta.get("step").cloned().unwrap_or_else(|| json!({}));
    let mut body = Map::new();
    body.insert(
        "stepId".into(),
        json!(step.get("stepId").and_then(|v| v.as_i64()).unwrap_or(0)),
    );
    body.insert(
        "stepName".into(),
        json!(step
            .get("stepName")
            .and_then(|v| v.as_str())
            .unwrap_or("wait")),
    );
    body.insert("stepType".into(), json!("Wait"));
    body.insert("out".into(), json!(out_base64));
    body.insert(
        "concurrent".into(),
        json!(step.get("concurrent").and_then(|v| v.as_i64()).unwrap_or(1)),
    );
    body.insert(
        "targetStep".into(),
        json!(step.get("targetStep").and_then(|v| v.as_i64()).unwrap_or(0)),
    );
    body.insert("waitEventId".into(), json!(&waiter.event_id));
    body.insert(
        "waitTimeoutDuration".into(),
        json!(meta
            .get("timeoutDuration")
            .and_then(|v| v.as_str())
            .unwrap_or("")),
    );
    body.insert(
        "waitTimeoutDeadline".into(),
        json!(meta
            .get("timeoutDeadline")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)),
    );
    body.insert("waitTimeout".into(), json!(wait_timeout));
    Value::Object(body)
}

/// A wake message row (workflow invocation — delivered empty-bodied by LazyFetch).
pub fn wake_message_row(
    tenant_id: &str,
    message_id: &str,
    url: &str,
    headers: Value,
    max_retries: i64,
) -> crate::model::MessageRow {
    let now = now_ms();
    crate::model::MessageRow {
        id: message_id.to_string(),
        tenant_id: tenant_id.to_string(),
        kind: "message".into(),
        state: "pending".into(),
        url: url.to_string(),
        topic_name: None,
        endpoint_name: None,
        queue_name: None,
        schedule_id: None,
        method: "POST".into(),
        headers,
        body: None,
        callback_url: None,
        callback_opts: None,
        failure_callback_url: None,
        failure_callback_opts: None,
        max_retries,
        retry_delay_expr: None,
        timeout_ms: None,
        not_before: now,
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
        created_at: now,
    }
}

/// `scanWaiterTimeouts`: claim + resolve each due waiter in one transaction; a failed
/// waiter rolls back to be retried by the next sweep. Returns the number claimed.
/// `tenant_id` of `None` scans every tenant in one statement (see `idx_waiter_timeout_tenant`).
pub fn scan_waiter_timeouts(
    conn: &mut Connection,
    tenant_id: Option<&str>,
    default_max_retries: i64,
    now: i64,
) -> rusqlite::Result<usize> {
    let ids: Vec<(String, String)> = {
        let mut stmt = match tenant_id {
            Some(_) => {
                conn.prepare("SELECT tenant_id, id FROM workflow_waiters WHERE tenant_id = ?1 AND timeout_at <= ?2 ORDER BY timeout_at, rowid")?
            }
            None => conn.prepare(
                "SELECT tenant_id, id FROM workflow_waiters WHERE timeout_at <= ?1 ORDER BY timeout_at, rowid",
            )?,
        };
        let rows: Vec<(String, String)> = match tenant_id {
            Some(t) => stmt
                .query_map(params![t, now], |r| Ok((r.get(0)?, r.get(1)?)))?
                .filter_map(|x| x.ok())
                .collect(),
            None => stmt
                .query_map(params![now], |r| Ok((r.get(0)?, r.get(1)?)))?
                .filter_map(|x| x.ok())
                .collect(),
        };
        rows
    };
    let mut fired = 0usize;
    for (waiter_tenant, id) in ids {
        let tenant_id = waiter_tenant.as_str();
        let tx = conn.transaction()?;
        let waiter = {
            let mut stmt = tx.prepare(&format!(
                "DELETE FROM workflow_waiters WHERE tenant_id = ?1 AND id = ?2 RETURNING {}",
                WAITER_COLS
            ))?;
            let mut rows = stmt.query(params![tenant_id, id])?;
            match rows.next()? {
                Some(r) => Some(row_to_waiter(r)?),
                None => None,
            }
        };
        let Some(waiter) = waiter else {
            tx.commit()?;
            continue;
        };
        if let Some(run_id) = &waiter.workflow_run_id {
            let message_id = ids::new_message_id();
            let resolution = resolution_body(&waiter, "", true);
            append_step(
                &tx,
                &waiter.tenant_id,
                run_id,
                &message_id,
                Some(resolution.to_string().as_bytes()),
                "step",
            )?;
            let headers = waiter
                .timeout_headers
                .clone()
                .unwrap_or_else(|| waiter.resume_headers.clone());
            let url = waiter
                .timeout_url
                .clone()
                .unwrap_or_else(|| waiter.resume_url.clone());
            let wake = wake_message_row(
                &waiter.tenant_id,
                &message_id,
                &url,
                headers,
                default_max_retries,
            );
            crate::store::insert_message(&tx, &wake)?;
        }
        tx.commit()?;
        fired += 1;
    }
    Ok(fired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::apply_migrations;

    fn fresh() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_migrations(&mut conn).unwrap();
        conn
    }

    fn headers(run_id: &str) -> Value {
        json!({ "Upstash-Workflow-RunId": [run_id] })
    }

    #[test]
    fn run_start_is_idempotent_and_never_overwrites_terminal() {
        let conn = fresh();
        record_run_start(&conn, "t1", "run_1", "https://wf/", None).unwrap();
        record_run_start(&conn, "t1", "run_1", "https://other/", None).unwrap();
        let url: String = conn
            .query_row(
                "SELECT workflow_url FROM workflow_runs WHERE tenant_id='t1' AND workflow_run_id='run_1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(url, "https://wf/");
        set_run_state(&conn, "t1", "run_1", "RUN_SUCCESS", None).unwrap();
        record_run_start(&conn, "t1", "run_1", "https://wf/", None).unwrap();
        let state: String = conn
            .query_row(
                "SELECT state FROM workflow_runs WHERE tenant_id='t1' AND workflow_run_id='run_1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(state, "RUN_SUCCESS");
    }

    #[test]
    fn steps_are_deduped_per_message_but_wait_markers_are_not() {
        let conn = fresh();
        let h = json!({});
        record_step(&conn, "t1", "run_1", "msg_a", Some(b"one"), &h).unwrap();
        record_step(&conn, "t1", "run_1", "msg_a", Some(b"one"), &h).unwrap();
        append_step(&conn, "t1", "run_1", "", Some(b"w1"), "wait").unwrap();
        append_step(&conn, "t1", "run_1", "", Some(b"w2"), "wait").unwrap();
        let steps = get_run_steps_json(&conn, "t1", "run_1").unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0]["messageId"], "msg_a");
        assert_eq!(steps[0]["body"], "b25l");
        assert_eq!(steps[1]["callType"], "wait");
    }

    #[test]
    fn cancel_is_fenced_to_started_and_drops_queued_messages() {
        let mut conn = fresh();
        record_run_start(&conn, "t1", "run_1", "https://wf/", None).unwrap();
        crate::store::insert_message(
            &conn,
            &wake_message_row("t1", "msg_queued", "https://wf/", headers("run_1"), 3),
        )
        .unwrap();
        assert!(cancel_run(&mut conn, "t1", "run_1").unwrap());
        assert!(!cancel_run(&mut conn, "t1", "run_1").unwrap());
        let left: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE tenant_id='t1' AND id='msg_queued'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
        let state: String = conn
            .query_row(
                "SELECT state FROM workflow_runs WHERE tenant_id='t1' AND workflow_run_id='run_1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(state, "RUN_CANCELED");
    }

    #[test]
    fn timeout_sweep_resolves_and_wakes_workflow() {
        let mut conn = fresh();
        // A waiter whose deadline has passed.
        conn.execute(
            r#"INSERT INTO workflow_waiters
                 (id, tenant_id, event_id, workflow_run_id, waiter_id, resume_url, resume_headers, step_meta, timeout_url, timeout_headers, timeout_at, created_at)
               VALUES ('w1','t1','evt','run_1','wtr_x','https://wf/','{}','{}','https://wf/','{}',?1,0)"#,
            params![now_ms() - 1],
        )
        .unwrap();
        let fired = scan_waiter_timeouts(&mut conn, Some("t1"), 3, now_ms()).unwrap();
        assert_eq!(fired, 1);
        let steps = get_run_steps_json(&conn, "t1", "run_1").unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0]["callType"], "step");
        let body = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            steps[0]["body"].as_str().unwrap(),
        )
        .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["waitTimeout"], true);
        assert_eq!(parsed["out"], "");
        let msg: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE tenant_id='t1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(msg, 1);
    }

    #[test]
    fn detects_run_id_case_insensitively() {
        let h = json!({ "upstash-workflow-runid": ["run_x"] });
        assert_eq!(workflow_run_id_of(&h).as_deref(), Some("run_x"));
        let h = json!({ "Upstash-Workflow-RunId": ["run_y"] });
        assert_eq!(workflow_run_id_of(&h).as_deref(), Some("run_y"));
    }
}
