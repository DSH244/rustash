//! `flowControl` and `globalParallelism`: read a key, pause/resume it, pin and unpin its
//! dimensions, and reset the rate window.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Map, Value};

use crate::tenant::Tenant;
use crate::time::now_ms;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::wire::empty_chunked;
use super::AppState;

#[derive(serde::Deserialize, Default)]
pub(super) struct FcQuery {
    parallelism: Option<String>,
    rate: Option<String>,
    period: Option<String>,
}

struct FcRow {
    key: String,
    parallelism: Option<i64>,
    rate: Option<i64>,
    period_secs: i64,
    paused: bool,
    rate_count: i64,
    rate_period_start: i64,
    pinned_parallelism: bool,
    pinned_rate: bool,
}

fn owned_fc(conn: &rusqlite::Connection, tenant_id: &str, key: &str) -> ApiResult<FcRow> {
    conn.query_row(
        "SELECT key, parallelism, rate, period_secs, paused, rate_count, rate_period_start, pinned_parallelism, pinned_rate FROM flow_control_keys WHERE tenant_id = ?1 AND key = ?2",
        rusqlite::params![tenant_id, key],
        |r| {
            Ok(FcRow {
                key: r.get(0)?,
                parallelism: r.get(1)?,
                rate: r.get(2)?,
                period_secs: r.get(3)?,
                paused: r.get::<_, i64>(4)? != 0,
                rate_count: r.get(5)?,
                rate_period_start: r.get(6)?,
                pinned_parallelism: r.get::<_, i64>(7)? != 0,
                pinned_rate: r.get::<_, i64>(8)? != 0,
            })
        },
    )
    .map_err(|_| AppError::new(404, "flow control key not found"))
}

pub(super) async fn global_parallelism(
    State(state): State<AppState>,
    Authed(tenant): Authed,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE tenant_id = ?1 AND state = 'leased' AND leased_until > ?2",
            rusqlite::params![tenant.id, now_ms()],
            |r| r.get(0),
        )
        .unwrap_or(0);
    Ok(Json(json!({
        "parallelismMax": state.config.tenant_parallelism_max,
        "parallelismCount": count,
    })))
}

pub(super) async fn get_flow_control(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(key): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let row = owned_fc(&conn, &tenant.id, &key)?;
    let now = now_ms();
    let leased: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE tenant_id = ?1 AND flow_control_key = ?2 AND state = 'leased' AND leased_until > ?3",
            rusqlite::params![tenant.id, key, now],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let waiting: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE tenant_id = ?1 AND flow_control_key = ?2 AND state = 'pending' AND not_before <= ?3",
            rusqlite::params![tenant.id, key, now],
            |r| r.get(0),
        )
        .unwrap_or(0);
    // Field names/meanings per the Flow Control doc's `flowControl.get()` response:
    // parallel/Rate maxima, the live counters, the rate window, and the pin flags.
    let mut m = Map::new();
    m.insert("flowControlKey".into(), json!(&row.key));
    m.insert("waitListSize".into(), json!(waiting));
    if let Some(p) = row.parallelism {
        m.insert("parallelismMax".into(), json!(p));
    }
    m.insert("parallelismCount".into(), json!(leased));
    if let Some(r) = row.rate {
        m.insert("rateMax".into(), json!(r));
    }
    m.insert("rateCount".into(), json!(row.rate_count));
    m.insert("ratePeriod".into(), json!(row.period_secs));
    m.insert(
        "ratePeriodStart".into(),
        json!(row.rate_period_start / 1000),
    );
    m.insert("isPaused".into(), json!(row.paused));
    m.insert("isPinnedParallelism".into(), json!(row.pinned_parallelism));
    m.insert("isPinnedRate".into(), json!(row.pinned_rate));
    // The pre-existing shapes stay alongside: rustash's own tooling reads them, and
    // dropping them would be a silent breaking change for anything already deployed.
    if let Some(p) = row.parallelism {
        m.insert("parallelism".into(), json!(p));
    }
    if let Some(r) = row.rate {
        m.insert("rate".into(), json!(r));
    }
    m.insert("period".into(), json!(row.period_secs));
    m.insert("paused".into(), json!(row.paused));
    Ok(Json(Value::Object(m)))
}

async fn set_flow_control_paused(
    state: &AppState,
    tenant: &Tenant,
    key: &str,
    paused: bool,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let _ = owned_fc(&conn, &tenant.id, key)?;
    conn.execute(
        "UPDATE flow_control_keys SET paused = ?3, updated_at = ?4 WHERE tenant_id = ?1 AND key = ?2",
        rusqlite::params![tenant.id, key, paused as i64, now_ms()],
    )?;
    state.engine.notify_dirty();
    Ok(empty_chunked(StatusCode::OK))
}

pub(super) async fn pin_flow_control(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(key): Path<String>,
    Query(q): Query<FcQuery>,
) -> ApiResult<Response> {
    let p_present = q.parallelism.is_some();
    let r_present = q.rate.is_some();
    let parallelism = q
        .parallelism
        .as_deref()
        .and_then(|s| s.trim().parse::<i64>().ok());
    let rate = q.rate.as_deref().and_then(|s| s.trim().parse::<i64>().ok());
    let period_secs = match q.period.as_deref() {
        Some(raw) => Some(crate::duration::parse_duration_secs(raw, "period")?),
        None => None,
    };
    let now = now_ms();
    let conn = state.db.conn.lock().unwrap();
    conn.execute(
        r#"INSERT INTO flow_control_keys
             (tenant_id, key, parallelism, rate, period_secs, pinned_parallelism, pinned_rate, paused, rate_count, rate_period_start, updated_at)
           VALUES (?1, ?2, ?3, ?4, COALESCE(?5, 1), ?6, ?7, 0, 0, ?8, ?8)
           ON CONFLICT(tenant_id, key) DO UPDATE SET
             parallelism = CASE WHEN ?6 THEN excluded.parallelism ELSE parallelism END,
             pinned_parallelism = CASE WHEN ?6 THEN 1 ELSE pinned_parallelism END,
             rate = CASE WHEN ?7 THEN excluded.rate ELSE rate END,
             pinned_rate = CASE WHEN ?7 THEN 1 ELSE pinned_rate END,
             period_secs = CASE WHEN ?5 IS NOT NULL THEN excluded.period_secs ELSE period_secs END,
             rate_count = 0,
             rate_period_start = excluded.rate_period_start,
             updated_at = excluded.updated_at"#,
        rusqlite::params![
            tenant.id,
            key,
            parallelism,
            rate,
            period_secs,
            p_present as i64,
            r_present as i64,
            now
        ],
    )?;
    Ok(empty_chunked(StatusCode::OK))
}

pub(super) async fn unpin_flow_control(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(key): Path<String>,
    Query(q): Query<FcQuery>,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let _ = owned_fc(&conn, &tenant.id, &key)?;
    let (unpin_p, unpin_r) = if q.parallelism.is_none() && q.rate.is_none() {
        (true, true)
    } else {
        (q.parallelism.is_some(), q.rate.is_some())
    };
    conn.execute(
        r#"UPDATE flow_control_keys
              SET pinned_parallelism = CASE WHEN ?3 THEN 0 ELSE pinned_parallelism END,
                  pinned_rate = CASE WHEN ?4 THEN 0 ELSE pinned_rate END,
                  updated_at = ?5
            WHERE tenant_id = ?1 AND key = ?2"#,
        rusqlite::params![tenant.id, key, unpin_p as i64, unpin_r as i64, now_ms()],
    )?;
    Ok(empty_chunked(StatusCode::OK))
}

pub(super) async fn reset_flow_control_rate(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(key): Path<String>,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let _ = owned_fc(&conn, &tenant.id, &key)?;
    let now = now_ms();
    conn.execute(
        "UPDATE flow_control_keys SET rate_count = 0, rate_period_start = ?3, updated_at = ?3 WHERE tenant_id = ?1 AND key = ?2",
        rusqlite::params![tenant.id, key, now],
    )?;
    Ok(empty_chunked(StatusCode::OK))
}

/// Pause/resume answer any method, so they are named handlers rather than closures.
pub(super) async fn pause_flow_control(
    state: State<AppState>,
    authed: Authed,
    key: Path<String>,
) -> ApiResult<Response> {
    set_flow_control_paused(&state, &authed.0, &key, true).await
}

pub(super) async fn resume_flow_control(
    state: State<AppState>,
    authed: Authed,
    key: Path<String>,
) -> ApiResult<Response> {
    set_flow_control_paused(&state, &authed.0, &key, false).await
}
