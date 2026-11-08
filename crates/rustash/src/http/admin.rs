//! The tenant registry (`/admin/*`), mounted only when `ADMIN_TOKEN` is set: create (with
//! `externalId` idempotency), list, read with usage counts, delete, rotate token/keys.
//!
//! The route list is returned as data; `router::build_router` is the only place that
//! registers it, and only when the token is configured.

use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, MethodRouter};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::ids;
use crate::time::now_ms;

use super::error::{ApiResult, AppError};
use super::wire::empty_chunked;
use super::AppState;

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub(super) async fn admin_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let expected = state
        .config
        .admin_token
        .as_deref()
        .ok_or_else(|| AppError::new(401, "invalid admin token"))?;
    let provided = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v.trim())
        .unwrap_or("");
    if provided.is_empty() || !constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
        return Err(AppError::new(401, "invalid admin token"));
    }
    Ok(next.run(req).await)
}

fn tenant_meta(
    tenant_id: &str,
    name: &str,
    external_id: Option<&str>,
    created_at: i64,
) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("tenantId".into(), json!(tenant_id));
    m.insert("name".into(), json!(name));
    if let Some(e) = external_id {
        m.insert("externalId".into(), json!(e));
    }
    m.insert("createdAt".into(), json!(created_at));
    m
}

struct TenantRecord {
    id: String,
    name: String,
    external_id: Option<String>,
    signing_key_current: String,
    signing_key_next: String,
    created_at: i64,
}

fn get_tenant_record(conn: &rusqlite::Connection, id: &str) -> Option<TenantRecord> {
    conn.query_row(
        "SELECT id, name, external_id, signing_key_current, signing_key_next, created_at FROM tenants WHERE id = ?1",
        [id],
        |r| {
            Ok(TenantRecord {
                id: r.get(0)?,
                name: r.get(1)?,
                external_id: r.get(2)?,
                signing_key_current: r.get(3)?,
                signing_key_next: r.get(4)?,
                created_at: r.get(5)?,
            })
        },
    )
    .ok()
}

async fn admin_create_tenant(State(state): State<AppState>, body: Bytes) -> ApiResult<Response> {
    let input: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
    let name = input
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("tenant");
    let external_id = input
        .get("externalId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let conn = state.db.conn.lock().unwrap();
    if let Some(ext) = external_id {
        if let Ok(existing) = conn
            .query_row(
                "SELECT id, name, external_id, signing_key_current, signing_key_next, created_at FROM tenants WHERE external_id = ?1",
                [ext],
                |r| {
                    Ok(TenantRecord {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        external_id: r.get(2)?,
                        signing_key_current: r.get(3)?,
                        signing_key_next: r.get(4)?,
                        created_at: r.get(5)?,
                    })
                },
            )
        {
            let mut m = tenant_meta(
                &existing.id,
                &existing.name,
                existing.external_id.as_deref(),
                existing.created_at,
            );
            m.insert("currentSigningKey".into(), json!(existing.signing_key_current));
            m.insert("nextSigningKey".into(), json!(existing.signing_key_next));
            m.insert("existed".into(), json!(true));
            return Ok((StatusCode::OK, Json(Value::Object(m))).into_response());
        }
    }
    let token = ids::new_tenant_token();
    let current = ids::new_signing_key();
    let next = ids::new_signing_key();
    let now = now_ms();
    let inserted = conn
        .execute(
            "INSERT OR IGNORE INTO tenants (id, name, token_hash, external_id, signing_key_current, signing_key_next, created_at, updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?7)",
            rusqlite::params![
                ids::new_uuid(),
                name,
                ids::hash_token(&token),
                external_id,
                current,
                next,
                now
            ],
        )
        .map_err(|_| AppError::internal())?;
    if inserted == 0 {
        // Lost an externalId race — answer with the winner's row.
        if let Some(ext) = external_id {
            if let Ok(existing) = conn
                .query_row(
                    "SELECT id, name, external_id, signing_key_current, signing_key_next, created_at FROM tenants WHERE external_id = ?1",
                    [ext],
                    |r| {
                        Ok(TenantRecord {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            external_id: r.get(2)?,
                            signing_key_current: r.get(3)?,
                            signing_key_next: r.get(4)?,
                            created_at: r.get(5)?,
                        })
                    },
                )
            {
                let mut m = tenant_meta(
                    &existing.id,
                    &existing.name,
                    existing.external_id.as_deref(),
                    existing.created_at,
                );
                m.insert("currentSigningKey".into(), json!(existing.signing_key_current));
                m.insert("nextSigningKey".into(), json!(existing.signing_key_next));
                m.insert("existed".into(), json!(true));
                return Ok((StatusCode::OK, Json(Value::Object(m))).into_response());
            }
        }
        return Err(AppError::internal());
    }
    let tenant_id: String = conn
        .query_row(
            "SELECT id FROM tenants WHERE token_hash = ?1",
            [ids::hash_token(&token)],
            |r| r.get(0),
        )
        .map_err(|_| AppError::internal())?;
    let mut m = tenant_meta(&tenant_id, name, external_id, now);
    m.insert("token".into(), json!(token));
    m.insert("currentSigningKey".into(), json!(current));
    m.insert("nextSigningKey".into(), json!(next));
    m.insert("existed".into(), json!(false));
    Ok((StatusCode::CREATED, Json(Value::Object(m))).into_response())
}

async fn admin_list_tenants(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT id, name, external_id, created_at FROM tenants ORDER BY created_at DESC, rowid DESC")
        .map_err(|_| AppError::internal())?;
    let rows: Vec<Value> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .map(|(id, name, ext, created)| {
            Value::Object(tenant_meta(&id, &name, ext.as_deref(), created))
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}

async fn admin_get_tenant(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let t = get_tenant_record(&conn, &id).ok_or_else(|| AppError::new(404, "tenant not found"))?;
    let messages: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE tenant_id = ?1 AND state IN ('pending','leased','cancel_requested')",
            [&id],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let schedules: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM schedules WHERE tenant_id = ?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let dlq: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dlq_messages WHERE tenant_id = ?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let mut m = tenant_meta(&t.id, &t.name, t.external_id.as_deref(), t.created_at);
    m.insert(
        "usage".into(),
        json!({ "messages": messages, "schedules": schedules, "dlq": dlq }),
    );
    Ok(Json(Value::Object(m)))
}

async fn admin_delete_tenant(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let mut conn = state.db.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|_| AppError::internal())?;
    let deleted = tx
        .execute("DELETE FROM tenants WHERE id = ?1", [&id])
        .map_err(|_| AppError::internal())?;
    if deleted == 0 {
        return Err(AppError::new(404, "tenant not found"));
    }
    let group_ids: Vec<String> = {
        let mut stmt = tx
            .prepare("SELECT id FROM url_groups WHERE tenant_id = ?1")
            .map_err(|_| AppError::internal())?;
        let rows = stmt
            .query_map([&id], |r| r.get::<_, String>(0))
            .map_err(|_| AppError::internal())?
            .filter_map(|x| x.ok())
            .collect();
        rows
    };
    for gid in group_ids {
        tx.execute("DELETE FROM url_group_endpoints WHERE group_id = ?1", [gid])
            .map_err(|_| AppError::internal())?;
    }
    for table in [
        "url_groups",
        "messages",
        "dedupe_entries",
        "queues",
        "schedules",
        "dlq_messages",
        "message_events",
        "flow_control_keys",
        "workflow_waiters",
        "workflow_steps",
        "workflow_runs",
    ] {
        tx.execute(
            &format!("DELETE FROM {} WHERE tenant_id = ?1", table),
            [&id],
        )
        .map_err(|_| AppError::internal())?;
    }
    tx.commit().map_err(|_| AppError::internal())?;
    Ok(empty_chunked(StatusCode::OK))
}

async fn admin_rotate_token(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let token = ids::new_tenant_token();
    let conn = state.db.conn.lock().unwrap();
    let n = conn
        .execute(
            "UPDATE tenants SET token_hash = ?2, updated_at = ?3 WHERE id = ?1",
            rusqlite::params![id, ids::hash_token(&token), now_ms()],
        )
        .map_err(|_| AppError::internal())?;
    if n == 0 {
        return Err(AppError::new(404, "tenant not found"));
    }
    Ok(Json(json!({ "token": token })))
}

async fn admin_rotate_keys(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let t = get_tenant_record(&conn, &id).ok_or_else(|| AppError::new(404, "tenant not found"))?;
    let fresh = ids::new_signing_key();
    conn.execute(
        "UPDATE tenants SET signing_key_current = ?2, signing_key_next = ?3, updated_at = ?4 WHERE id = ?1",
        rusqlite::params![id, t.signing_key_next, fresh, now_ms()],
    )?;
    Ok(Json(
        json!({ "current": t.signing_key_next, "next": fresh }),
    ))
}

/// The registry's route table, as data.
pub(super) fn routes() -> Vec<(&'static str, MethodRouter<AppState>)> {
    vec![
        (
            "/admin/tenants",
            post(admin_create_tenant).get(admin_list_tenants),
        ),
        (
            "/admin/tenants/:id",
            get(admin_get_tenant).delete(admin_delete_tenant),
        ),
        ("/admin/tenants/:id/rotate-token", post(admin_rotate_token)),
        ("/admin/tenants/:id/rotate-keys", post(admin_rotate_keys)),
    ]
}
