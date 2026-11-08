//! Workflow wait/notify: register a waiter, resolve it with an event payload, hand the
//! resolution back as a wake message, and list what is still waiting.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::ids;
use crate::time::now_ms;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::AppState;

// ---------------------------------------------------------------------------
// Workflow protocol
// ---------------------------------------------------------------------------

fn parse_timeout_secs(value: Option<&Value>) -> i64 {
    let Some(value) = value else { return 604_800 };
    if let Some(n) = value.as_i64() {
        return n;
    }
    if let Some(n) = value.as_f64() {
        return n as i64;
    }
    let Some(raw) = value.as_str() else {
        return 604_800;
    };
    let trimmed = raw.trim();
    let (digits, unit) = match trimmed.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((idx, c)) => (&trimmed[..idx], Some(c)),
        None => (trimmed, None),
    };
    if digits.is_empty() {
        return 604_800;
    }
    let Ok(n) = digits.parse::<i64>() else {
        return 604_800;
    };
    let mult = match unit {
        None => 1,
        Some('s') => 1,
        Some('m') => 60,
        Some('h') => 3_600,
        Some('d') => 86_400,
        _ => return 604_800,
    };
    n * mult
}

fn normalize_headers(value: Option<&Value>) -> Value {
    let mut out = Map::new();
    if let Some(obj) = value.and_then(|v| v.as_object()) {
        for (name, v) in obj {
            if let Some(arr) = v.as_array() {
                out.insert(name.clone(), Value::Array(arr.clone()));
            } else {
                out.insert(name.clone(), Value::Array(vec![v.clone()]));
            }
        }
    }
    Value::Object(out)
}

fn workflow_resume_headers(headers: &HeaderMap) -> (Value, Option<String>) {
    let mut out = Map::new();
    let mut run_id = None;
    for name in headers.keys() {
        let lower = name.as_str().to_ascii_lowercase();
        if !lower.starts_with("upstash-workflow-") {
            continue;
        }
        if let Some(value) = headers.get(name) {
            if let Ok(text) = value.to_str() {
                if lower == "upstash-workflow-runid" {
                    run_id = Some(text.to_string());
                }
                out.insert(lower, json!([text]));
            }
        }
    }
    (Value::Object(out), run_id)
}

pub(super) async fn wait_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(event_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let input: Value = serde_json::from_slice(&body).map_err(|_| AppError::internal())?;
    let resume_url = input
        .get("url")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(AppError::internal)?; // missing url → 500 (url is NOT NULL)
    let timeout_secs = parse_timeout_secs(input.get("timeout"));
    let timeout_at = now_ms() + timeout_secs * 1000;
    let (resume_headers, run_id) = workflow_resume_headers(&headers);
    let waiter_id = ids::new_waiter_id();
    let step_meta = json!({
        "step": input.get("step").cloned().unwrap_or_else(|| json!({})),
        "timeoutDuration": match input.get("timeout") {
            Some(Value::String(s)) => s.clone(),
            _ => format!("{}s", timeout_secs),
        },
        "timeoutDeadline": timeout_at,
    });
    let timeout_url = input
        .get("timeoutUrl")
        .and_then(|v| v.as_str())
        .unwrap_or(&resume_url)
        .to_string();
    let timeout_headers = normalize_headers(input.get("timeoutHeaders"));
    let mut conn = state.db.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|_| AppError::internal())?;
    tx.execute(
        r#"INSERT INTO workflow_waiters
             (id, tenant_id, event_id, workflow_run_id, waiter_id, resume_url, resume_headers,
              step_meta, timeout_url, timeout_headers, timeout_at, created_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)"#,
        rusqlite::params![
            ids::new_uuid(),
            tenant.id,
            event_id,
            run_id,
            waiter_id,
            resume_url,
            resume_headers.to_string(),
            step_meta.to_string(),
            timeout_url,
            timeout_headers.to_string(),
            timeout_at,
            now_ms(),
        ],
    )?;
    if let Some(run) = &run_id {
        let marker = json!({
            "waiterId": waiter_id,
            "eventId": event_id,
            "userId": tenant.id,
        });
        let _ = crate::workflow::append_step(
            &tx,
            &tenant.id,
            run,
            "",
            Some(marker.to_string().as_bytes()),
            "wait",
        );
    }
    tx.commit().map_err(|_| AppError::internal())?;
    Ok(Json(json!({ "waiterId": waiter_id })).into_response())
}

async fn notify_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    event_id: String,
    run_filter: Option<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let event_text = String::from_utf8_lossy(&body).into_owned();
    let event_b64 = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(event_text.as_bytes())
    };
    let waiters: Vec<crate::workflow::WaiterRow> = {
        let conn = state.db.conn.lock().unwrap();
        let mut sql = format!(
            "SELECT {} FROM workflow_waiters WHERE tenant_id = ?1 AND event_id = ?2",
            crate::workflow::WAITER_COLS
        );
        if run_filter.is_some() {
            sql.push_str(" AND workflow_run_id = ?3");
        }
        sql.push_str(" ORDER BY rowid");
        let mut stmt = conn.prepare(&sql).map_err(|_| AppError::internal())?;
        let rows = match &run_filter {
            Some(run) => stmt.query_map(
                rusqlite::params![tenant.id, event_id, run],
                crate::workflow::row_to_waiter,
            ),
            None => stmt.query_map(
                rusqlite::params![tenant.id, event_id],
                crate::workflow::row_to_waiter,
            ),
        }
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .collect();
        rows
    };
    let mut notified = Vec::new();
    for waiter in waiters {
        let message_id = ids::new_message_id();
        let mut headers = waiter
            .resume_headers
            .as_object()
            .cloned()
            .unwrap_or_default();
        headers.insert("Content-Type".into(), json!(["application/json"]));
        let wake_headers = Value::Object(headers);
        {
            let mut conn = state.db.conn.lock().unwrap();
            let tx = conn.transaction().map_err(|_| AppError::internal())?;
            if let Some(run) = &waiter.workflow_run_id {
                let resolution = crate::workflow::resolution_body(&waiter, &event_b64, false);
                crate::workflow::append_step(
                    &tx,
                    &waiter.tenant_id,
                    run,
                    &message_id,
                    Some(resolution.to_string().as_bytes()),
                    "step",
                )?;
            }
            let wake = crate::workflow::wake_message_row(
                &waiter.tenant_id,
                &message_id,
                &waiter.resume_url,
                wake_headers,
                state.config.default_max_retries,
            );
            crate::store::insert_message(&tx, &wake)?;
            tx.execute("DELETE FROM workflow_waiters WHERE id = ?1", [&waiter.id])?;
            tx.commit().map_err(|_| AppError::internal())?;
        }
        notified.push(json!({
            "waiter": {
                "url": waiter.resume_url,
                "headers": waiter.resume_headers,
                "deadline": waiter.timeout_at,
            },
            "messageId": message_id,
            "workflowRunId": waiter.workflow_run_id,
            "workflowCreatedAt": waiter.created_at,
        }));
    }
    state.engine.notify_dirty();
    Ok((StatusCode::CREATED, Json(Value::Array(notified))).into_response())
}

pub(super) async fn notify_route_event(
    state: State<AppState>,
    a: Authed,
    Path(event_id): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    notify_route(state, a, event_id, None, body).await
}

pub(super) async fn notify_route_run(
    state: State<AppState>,
    a: Authed,
    Path((run_id, event_id)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Response> {
    notify_route(state, a, event_id, Some(run_id), body).await
}

pub(super) async fn get_waiters(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(event_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {} FROM workflow_waiters WHERE tenant_id = ?1 AND event_id = ?2 ORDER BY rowid",
            crate::workflow::WAITER_COLS
        ))
        .map_err(|_| AppError::internal())?;
    let rows: Vec<Value> = stmt
        .query_map(
            rusqlite::params![tenant.id, event_id],
            crate::workflow::row_to_waiter,
        )
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .map(|w| {
            json!({
                "waiterId": w.waiter_id,
                "url": w.resume_url,
                "headers": w.resume_headers,
                "deadline": w.timeout_at.div_euclid(1000),
                "timeoutUrl": w.timeout_url.unwrap_or_else(|| w.resume_url.clone()),
            })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}
