//! `topics` (URL groups): list, read, delete, add endpoints, remove endpoints.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Map, Value};

use crate::ids;
use crate::ssrf::{create_policy, validate_destination_url};
use crate::time::now_ms;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::wire::empty_chunked;
use super::AppState;

// ---------------------------------------------------------------------------
// Topics (URL groups)
// ---------------------------------------------------------------------------

fn group_json(
    conn: &rusqlite::Connection,
    group_id: &str,
    name: &str,
    created_at: i64,
    updated_at: i64,
) -> Value {
    let mut stmt = conn
        .prepare("SELECT name, url FROM url_group_endpoints WHERE group_id = ?1 ORDER BY rowid")
        .unwrap();
    let endpoints: Vec<Value> = stmt
        .query_map([group_id], |r| {
            Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?))
        })
        .unwrap()
        .filter_map(|x| x.ok())
        .map(|(name, url)| {
            let mut m = Map::new();
            if let Some(n) = name {
                m.insert("name".into(), json!(n));
            }
            m.insert("url".into(), json!(url));
            Value::Object(m)
        })
        .collect();
    json!({
        "name": name,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "endpoints": endpoints,
    })
}

pub(super) async fn list_topics(
    State(state): State<AppState>,
    Authed(tenant): Authed,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT id, name, created_at, updated_at FROM url_groups WHERE tenant_id = ?1 ORDER BY rowid")
        .map_err(|_| AppError::internal())?;
    let groups: Vec<(String, String, i64, i64)> = stmt
        .query_map([&tenant.id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .collect();
    Ok(Json(Value::Array(
        groups
            .into_iter()
            .map(|(id, n, c, u)| group_json(&conn, &id, &n, c, u))
            .collect(),
    )))
}

pub(super) async fn get_topic(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let row = conn
        .query_row(
            "SELECT id, created_at, updated_at FROM url_groups WHERE tenant_id = ?1 AND name = ?2",
            rusqlite::params![tenant.id, name],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(|_| AppError::new(404, "URL group not found"))?;
    Ok(Json(group_json(&conn, &row.0, &name, row.1, row.2)))
}

pub(super) async fn delete_topic(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let n = conn.execute(
        "DELETE FROM url_groups WHERE tenant_id = ?1 AND name = ?2",
        rusqlite::params![tenant.id, name],
    )?;
    if n == 0 {
        return Err(AppError::new(404, "URL group not found"));
    }
    Ok(empty_chunked(StatusCode::OK))
}

#[derive(serde::Deserialize)]
struct EndpointsBody {
    endpoints: Option<Vec<Value>>,
}

pub(super) async fn add_endpoints(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let input: EndpointsBody =
        serde_json::from_slice(&body).unwrap_or(EndpointsBody { endpoints: None });
    let endpoints = input
        .endpoints
        .filter(|v| !v.is_empty())
        .ok_or_else(|| AppError::new(400, "endpoints are required"))?;
    let policy = create_policy(
        state.config.allow_private_destinations,
        &state.config.destination_allow_cidrs,
        &state.config.destination_block_cidrs,
    );
    let mut parsed = Vec::new();
    for e in &endpoints {
        let url = e
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("undefined")
            .to_string();
        validate_destination_url(&policy, &url)?;
        parsed.push((
            e.get("name").and_then(|v| v.as_str()).map(String::from),
            url,
        ));
    }
    let mut conn = state.db.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|_| AppError::internal())?;
    tx.execute(
        "INSERT OR IGNORE INTO url_groups (id, tenant_id, name, created_at, updated_at) VALUES (?1,?2,?3,?4,?4)",
        rusqlite::params![ids::new_uuid(), tenant.id, name, now_ms()],
    )?;
    let group_id: String = tx
        .query_row(
            "SELECT id FROM url_groups WHERE tenant_id = ?1 AND name = ?2",
            rusqlite::params![tenant.id.clone(), name.clone()],
            |r| r.get(0),
        )
        .map_err(|_| AppError::internal())?;
    for (ename, url) in parsed {
        tx.execute(
            "INSERT OR IGNORE INTO url_group_endpoints (id, group_id, name, url, created_at) VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![ids::new_uuid(), group_id.clone(), ename, url, now_ms()],
        )?;
    }
    tx.execute(
        "UPDATE url_groups SET updated_at = ?2 WHERE id = ?1",
        rusqlite::params![group_id, now_ms()],
    )?;
    tx.commit().map_err(|_| AppError::internal())?;
    Ok(empty_chunked(StatusCode::OK))
}

pub(super) async fn remove_endpoints(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let input: EndpointsBody =
        serde_json::from_slice(&body).unwrap_or(EndpointsBody { endpoints: None });
    let urls: Vec<String> = input
        .endpoints
        .unwrap_or_default()
        .into_iter()
        .map(|e| {
            e.get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("undefined")
                .to_string()
        })
        .collect();
    let mut conn = state.db.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|_| AppError::internal())?;
    let group_id: Option<String> = tx
        .query_row(
            "SELECT id FROM url_groups WHERE tenant_id = ?1 AND name = ?2",
            rusqlite::params![tenant.id.clone(), name.clone()],
            |r| r.get(0),
        )
        .ok();
    let Some(group_id) = group_id else {
        return Err(AppError::new(404, "URL group not found"));
    };
    for url in urls {
        tx.execute(
            "DELETE FROM url_group_endpoints WHERE group_id = ?1 AND url = ?2",
            rusqlite::params![group_id.clone(), url],
        )?;
    }
    let remaining: i64 = tx.query_row(
        "SELECT COUNT(*) FROM url_group_endpoints WHERE group_id = ?1",
        [group_id.clone()],
        |r| r.get(0),
    )?;
    if remaining == 0 {
        tx.execute("DELETE FROM url_groups WHERE id = ?1", [group_id])?;
    }
    tx.commit().map_err(|_| AppError::internal())?;
    Ok(empty_chunked(StatusCode::OK))
}
