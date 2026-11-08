//! `queues`: create/upsert, list, read (with lag), delete, pause, resume.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Value};

use crate::ids;
use crate::time::now_ms;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::wire::empty_chunked;
use super::AppState;

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct QueueUpsert {
    #[serde(rename = "queueName")]
    queue_name: Option<String>,
    parallelism: Option<i64>,
    paused: Option<bool>,
}

fn queue_name_valid(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Auto-create the queue row a publish names, if the tenant does not have it yet
/// (idempotent, parallelism 1, not paused). `publish`, `enqueue` and `batch` items naming a
/// queue all rely on it: the reference server treats a publish into an unknown queue as
/// auto-creating that queue, so the write is shared instead of repeated per entry point.
pub(super) fn ensure_queue(conn: &rusqlite::Connection, tenant_id: &str, name: &str) {
    let _ = conn.execute(
        "INSERT OR IGNORE INTO queues (id, tenant_id, name, parallelism, paused, created_at, updated_at) VALUES (?1,?2,?3,1,0,?4,?4)",
        rusqlite::params![ids::new_uuid(), tenant_id, name, now_ms()],
    );
}

pub(super) async fn upsert_queue(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    body: Bytes,
) -> ApiResult<Response> {
    let input: QueueUpsert = serde_json::from_slice(&body).map_err(|_| AppError::internal())?;
    let name = input
        .queue_name
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::new(400, "queueName is required"))?;
    if !queue_name_valid(&name) {
        return Err(AppError::validation(
            "queue name may only contain letters, numbers, hyphens, underscores",
        ));
    }
    let parallelism = input.parallelism.unwrap_or(1);
    if parallelism < 1 {
        return Err(AppError::validation("parallelism must be >= 1"));
    }
    let paused = input.paused.unwrap_or(false);
    let conn = state.db.conn.lock().unwrap();
    conn.execute(
        r#"INSERT INTO queues (id, tenant_id, name, parallelism, paused, created_at, updated_at)
           VALUES (?1,?2,?3,?4,?5,?6,?6)
           ON CONFLICT(tenant_id, name) DO UPDATE SET parallelism = excluded.parallelism, paused = excluded.paused, updated_at = excluded.updated_at"#,
        rusqlite::params![ids::new_uuid(), tenant.id, name, parallelism, paused as i64, now_ms()],
    )?;
    Ok(empty_chunked(StatusCode::OK))
}

fn queue_json(
    conn: &rusqlite::Connection,
    tenant_id: &str,
    name: &str,
    parallelism: i64,
    paused: bool,
    created_at: i64,
    updated_at: i64,
) -> Value {
    let lag: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE tenant_id = ?1 AND queue_name = ?2",
            rusqlite::params![tenant_id, name],
            |r| r.get(0),
        )
        .unwrap_or(0);
    json!({
        "name": name,
        "parallelism": parallelism,
        "paused": paused,
        "lag": lag,
        "createdAt": created_at,
        "updatedAt": updated_at,
    })
}

pub(super) async fn list_queues(
    State(state): State<AppState>,
    Authed(tenant): Authed,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT name, parallelism, paused, created_at, updated_at FROM queues WHERE tenant_id = ?1 ORDER BY rowid")
        .map_err(|_| AppError::internal())?;
    let out: Vec<Value> = stmt
        .query_map([&tenant.id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)? != 0,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .map(|(n, p, paused, c, u)| queue_json(&conn, &tenant.id, &n, p, paused, c, u))
        .collect();
    Ok(Json(Value::Array(out)))
}

pub(super) async fn get_queue(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let row = conn
        .query_row(
            "SELECT parallelism, paused, created_at, updated_at FROM queues WHERE tenant_id = ?1 AND name = ?2",
            rusqlite::params![tenant.id, name],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)? != 0, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?)),
        )
        .map_err(|_| AppError::new(404, "queue not found"))?;
    Ok(Json(queue_json(
        &conn, &tenant.id, &name, row.0, row.1, row.2, row.3,
    )))
}

pub(super) async fn delete_queue(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
) -> ApiResult<Response> {
    let mut conn = state.db.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|_| AppError::internal())?;
    let deleted = tx.execute(
        "DELETE FROM queues WHERE tenant_id = ?1 AND name = ?2",
        rusqlite::params![tenant.id, name],
    )?;
    if deleted == 0 {
        return Err(AppError::new(404, "queue not found"));
    }
    tx.execute(
        "DELETE FROM messages WHERE tenant_id = ?1 AND queue_name = ?2",
        rusqlite::params![tenant.id, name],
    )?;
    tx.commit().map_err(|_| AppError::internal())?;
    Ok(empty_chunked(StatusCode::OK))
}

async fn set_queue_paused(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
    paused: bool,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let n = conn.execute(
        "UPDATE queues SET paused = ?3, updated_at = ?4 WHERE tenant_id = ?1 AND name = ?2",
        rusqlite::params![tenant.id, name, paused as i64, now_ms()],
    )?;
    if n == 0 {
        return Err(AppError::new(404, "queue not found"));
    }
    state.engine.notify_dirty();
    Ok(empty_chunked(StatusCode::OK))
}

/// The pause/resume pair answers any method (the reference server does not restrict it),
/// so each is a named handler rather than an inline closure in the route table.
pub(super) async fn pause_queue(
    state: State<AppState>,
    authed: Authed,
    name: Path<String>,
) -> ApiResult<Response> {
    set_queue_paused(state, authed, name, true).await
}

pub(super) async fn resume_queue(
    state: State<AppState>,
    authed: Authed,
    name: Path<String>,
) -> ApiResult<Response> {
    set_queue_paused(state, authed, name, false).await
}
