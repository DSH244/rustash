//! `schedules`: create/upsert by destination, list, read, pause, resume, delete — plus
//! the method-plus-tail dispatch that reproduces the reference server's single
//! `/schedules/*` route.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::ids;
use crate::ssrf::{create_policy, validate_destination_url};
use crate::tenant::Tenant;
use crate::time::now_ms;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::ingest::{check_payload_limit, is_url, optional_body, spec_from_headers, HEADERS_ONLY};
use super::prefix::{self, Tail, SCHEDULES};
use super::wire::empty_chunked;
use super::AppState;

/// POST /v2/schedules/{destination} — create or upsert (by Upstash-Schedule-Id).
async fn create_schedule(
    state: &AppState,
    tenant: &Tenant,
    destination: &str,
    headers: &HeaderMap,
    body: &Bytes,
) -> ApiResult<Response> {
    if destination.is_empty() {
        return Err(AppError::new(400, "missing destination"));
    }
    let spec = spec_from_headers(headers, None, &state.config, HEADERS_ONLY)?;
    let cron_raw = spec
        .cron
        .clone()
        .ok_or_else(|| AppError::new(400, "missing Upstash-Cron header"))?;
    let cron = crate::cron::parse_cron(&cron_raw)?;
    // A URL destination is SSRF-checked now; a URL-group name is resolved at fire time.
    if is_url(destination) {
        let policy = create_policy(
            state.config.allow_private_destinations,
            &state.config.destination_allow_cidrs,
            &state.config.destination_block_cidrs,
        );
        validate_destination_url(&policy, destination)?;
    }
    check_payload_limit(body.len(), state.config.max_body_bytes)?;
    let body_opt = optional_body(body);
    let id = spec
        .schedule_id
        .clone()
        .unwrap_or_else(ids::new_schedule_id);
    let now = now_ms();
    let next = crate::schedule::initial_next_run(&cron.expr, cron.tz.as_deref(), now)
        .map_err(AppError::validation)?;
    let fc_key = spec.flow_control.as_ref().map(|f| f.key.clone());
    let fc_parallelism = spec.flow_control.as_ref().and_then(|f| f.parallelism);
    let fc_rate = spec.flow_control.as_ref().and_then(|f| f.rate);
    let fc_period = spec.flow_control.as_ref().map(|f| f.period_secs);
    let conn = state.db.conn.lock().unwrap();
    conn.execute(
        r#"INSERT INTO schedules
             (id, tenant_id, destination, cron_expr, tz, method, headers, body, retries,
              retry_delay_expr, delay_secs, timeout_ms, callback_url, callback_opts,
              failure_callback_url, failure_callback_opts, queue_name, flow_control_key,
              fc_parallelism, fc_rate, fc_period_secs, labels, redact_fields, next_run_at,
              created_at, updated_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?25)
           ON CONFLICT(id) DO UPDATE SET
             tenant_id=excluded.tenant_id,
             destination=excluded.destination,
             cron_expr=excluded.cron_expr,
             tz=excluded.tz,
             method=excluded.method,
             headers=excluded.headers,
             body=excluded.body,
             retries=excluded.retries,
             retry_delay_expr=excluded.retry_delay_expr,
             delay_secs=excluded.delay_secs,
             timeout_ms=excluded.timeout_ms,
             callback_url=excluded.callback_url,
             callback_opts=excluded.callback_opts,
             failure_callback_url=excluded.failure_callback_url,
             failure_callback_opts=excluded.failure_callback_opts,
             queue_name=excluded.queue_name,
             flow_control_key=excluded.flow_control_key,
             fc_parallelism=excluded.fc_parallelism,
             fc_rate=excluded.fc_rate,
             fc_period_secs=excluded.fc_period_secs,
             labels=excluded.labels,
             redact_fields=excluded.redact_fields,
             next_run_at=excluded.next_run_at,
             updated_at=excluded.updated_at"#,
        rusqlite::params![
            id,
            tenant.id,
            destination,
            cron.expr,
            cron.tz,
            spec.method,
            spec.forward_headers.to_string(),
            body_opt,
            spec.retries,
            spec.retry_delay_expr,
            spec.delay_secs,
            spec.timeout_ms,
            spec.callback_url,
            spec.callback_opts.as_ref().map(|v| v.to_string()),
            spec.failure_callback_url,
            spec.failure_callback_opts.as_ref().map(|v| v.to_string()),
            spec.queue_name,
            fc_key,
            fc_parallelism,
            fc_rate,
            fc_period,
            serde_json::to_string(&spec.labels).unwrap_or_else(|_| "[]".into()),
            spec.redact_fields.as_ref().map(|v| v.to_string()),
            next,
            now,
        ],
    )?;
    Ok((StatusCode::CREATED, Json(json!({ "scheduleId": id }))).into_response())
}

pub(super) async fn create_schedule_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let destination = prefix::resource_tail(&uri, &[SCHEDULES, SCHEDULES], Tail::WithQuery);
    create_schedule(&state, &tenant, &destination, &headers, &body).await
}

fn owned_schedule(
    conn: &rusqlite::Connection,
    tenant_id: &str,
    id: &str,
) -> ApiResult<crate::schedule::ScheduleRow> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {} FROM schedules WHERE tenant_id = ?1 AND id = ?2",
            crate::schedule::SCHEDULE_COLS
        ))
        .map_err(|_| AppError::internal())?;
    let mut rows = stmt
        .query(rusqlite::params![tenant_id, id])
        .map_err(|_| AppError::internal())?;
    match rows.next().map_err(|_| AppError::internal())? {
        Some(r) => crate::schedule::row_to_schedule(r).map_err(|_| AppError::internal()),
        None => Err(AppError::new(404, "schedule not found")),
    }
}

pub(super) async fn list_schedules(
    State(state): State<AppState>,
    Authed(tenant): Authed,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {} FROM schedules WHERE tenant_id = ?1 ORDER BY created_at DESC, rowid DESC",
            crate::schedule::SCHEDULE_COLS
        ))
        .map_err(|_| AppError::internal())?;
    let rows: Vec<Value> = stmt
        .query_map([&tenant.id], crate::schedule::row_to_schedule)
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .map(|r| crate::schedule::to_schedule_json(&r))
        .collect();
    Ok(Json(Value::Array(rows)))
}

async fn set_schedule_paused(
    state: &AppState,
    tenant: &Tenant,
    id: &str,
    paused: bool,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let row = owned_schedule(&conn, &tenant.id, id)?;
    if paused {
        conn.execute(
            "UPDATE schedules SET is_paused = 1 WHERE tenant_id = ?1 AND id = ?2",
            rusqlite::params![tenant.id, id],
        )?;
    } else {
        // Recompute the next firing so a long-paused schedule skips missed windows.
        let next = crate::schedule::initial_next_run(&row.cron_expr, row.tz.as_deref(), now_ms())
            .map_err(AppError::validation)?;
        conn.execute(
            "UPDATE schedules SET is_paused = 0, next_run_at = ?3 WHERE tenant_id = ?1 AND id = ?2",
            rusqlite::params![tenant.id, id, next],
        )?;
    }
    state.engine.notify_dirty();
    Ok(empty_chunked(StatusCode::OK))
}

/// Hono serves `GET/DELETE /schedules/:id` and `POST|PATCH /schedules/:id/pause|resume`
/// plus `POST /schedules/*` create from the same prefix. Axum's catch-all dispatches
/// by method + tail, which keeps identical matching semantics without path conflicts.
pub(super) async fn schedules_dispatch(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    if method == Method::GET {
        let id = prefix::resource_tail(&uri, &[SCHEDULES, SCHEDULES], Tail::PathOnly);
        let conn = state.db.conn.lock().unwrap();
        return Ok(Json(crate::schedule::to_schedule_json(&owned_schedule(
            &conn, &tenant.id, &id,
        )?))
        .into_response());
    }
    if method == Method::DELETE {
        let id = prefix::resource_tail(&uri, &[SCHEDULES, SCHEDULES], Tail::PathOnly);
        let conn = state.db.conn.lock().unwrap();
        let _ = owned_schedule(&conn, &tenant.id, &id)?;
        conn.execute(
            "DELETE FROM schedules WHERE tenant_id = ?1 AND id = ?2",
            rusqlite::params![tenant.id, id],
        )?;
        return Ok(empty_chunked(StatusCode::OK));
    }
    let tail = prefix::resource_tail(&uri, &[SCHEDULES, SCHEDULES], Tail::PathOnly);
    if let Some((id, action)) = tail.rsplit_once('/') {
        if !id.is_empty() && !id.contains('/') {
            if action == "pause" {
                return set_schedule_paused(&state, &tenant, id, true).await;
            }
            if action == "resume" {
                return set_schedule_paused(&state, &tenant, id, false).await;
            }
        }
    }
    let destination = prefix::resource_tail(&uri, &[SCHEDULES, SCHEDULES], Tail::WithQuery);
    create_schedule(&state, &tenant, &destination, &headers, &body).await
}
