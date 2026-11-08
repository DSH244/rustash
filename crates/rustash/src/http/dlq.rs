//! `dlq`: list (cursor-paged), read, delete, retry, and the two bulk variants.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Map, Value};

use crate::model::DlqRow;
use crate::store::{dlq_cols, requeue_dlq_row, row_to_dlq};

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::paging::{decode_cursor, encode_cursor};
use super::wire::empty_chunked;
use super::AppState;

// ---------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub(super) struct DlqQuery {
    count: Option<String>,
    cursor: Option<String>,
    url: Option<String>,
    #[serde(rename = "scheduleId")]
    schedule_id: Option<String>,
    #[serde(rename = "queueName")]
    queue_name: Option<String>,
}

fn load_dlq(conn: &rusqlite::Connection, tenant_id: &str, dlq_id: &str) -> ApiResult<DlqRow> {
    conn.prepare(&format!(
        "SELECT {} FROM dlq_messages WHERE dlq_id = ?1 AND tenant_id = ?2",
        dlq_cols()
    ))
    .and_then(|mut s| s.query_row(rusqlite::params![dlq_id, tenant_id], row_to_dlq))
    .map_err(|_| AppError::new(404, "DLQ message not found"))
}

pub(super) async fn list_dlq(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Query(q): Query<DlqQuery>,
) -> ApiResult<Json<Value>> {
    let limit = q
        .count
        .as_deref()
        .and_then(|c| c.parse::<i64>().ok())
        .unwrap_or(100)
        .min(100);
    let mut sql = String::from(&format!(
        "SELECT {} FROM dlq_messages WHERE tenant_id = ?1",
        dlq_cols()
    ));
    let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(tenant.id.clone())];
    let mut idx = 2usize;
    for (col, val) in [
        ("url", q.url),
        ("schedule_id", q.schedule_id),
        ("queue_name", q.queue_name),
    ] {
        if let Some(v) = val {
            sql.push_str(&format!(" AND {} = ?{}", col, idx));
            args.push(Box::new(v));
            idx += 1;
        }
    }
    if let Some(cursor) = &q.cursor {
        if let Some((ms, id)) = decode_cursor(cursor) {
            sql.push_str(&format!(
                " AND (dlq_created_at < ?{} OR (dlq_created_at = ?{} AND dlq_id < ?{}))",
                idx,
                idx + 1,
                idx + 2
            ));
            args.push(Box::new(ms));
            args.push(Box::new(ms));
            args.push(Box::new(id));
            idx += 3;
        }
    }
    let _ = idx;
    sql.push_str(" ORDER BY dlq_created_at DESC, dlq_id DESC LIMIT ?NEXT");
    args.push(Box::new(limit + 1));
    let sql = sql.replace("?NEXT", &format!("?{}", args.len()));
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn.prepare(&sql).map_err(|_| AppError::internal())?;
    let refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
    let rows: Vec<DlqRow> = stmt
        .query_map(refs.as_slice(), row_to_dlq)
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .collect();
    let page: Vec<&DlqRow> = rows.iter().take(limit as usize).collect();
    let next_cursor = if rows.len() as i64 > limit {
        page.last()
            .map(|d| encode_cursor(d.dlq_created_at, &d.dlq_id))
    } else {
        None
    };
    let messages: Vec<Value> = page.iter().map(|d| d.to_dlq_json()).collect();
    let mut out = Map::new();
    out.insert("messages".into(), Value::Array(messages));
    if let Some(c) = next_cursor {
        out.insert("cursor".into(), json!(c));
    }
    Ok(Json(Value::Object(out)))
}

pub(super) async fn get_dlq(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    Ok(Json(load_dlq(&conn, &tenant.id, &id)?.to_dlq_json()))
}

pub(super) async fn delete_dlq(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    {
        let conn = state.db.conn.lock().unwrap();
        load_dlq(&conn, &tenant.id, &id)?;
        conn.execute("DELETE FROM dlq_messages WHERE dlq_id = ?1", [&id])?;
    }
    Ok(empty_chunked(StatusCode::OK))
}

#[derive(serde::Deserialize)]
struct DlqIdsFilter {
    #[serde(rename = "dlqIds")]
    dlq_ids: Option<Vec<String>>,
}

pub(super) async fn bulk_delete_dlq(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let filter: DlqIdsFilter =
        serde_json::from_slice(&body).unwrap_or(DlqIdsFilter { dlq_ids: None });
    let conn = state.db.conn.lock().unwrap();
    let deleted = match filter.dlq_ids.filter(|v| !v.is_empty()) {
        Some(ids) => {
            let marks = (0..ids.len())
                .map(|i| format!("?{}", i + 2))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "DELETE FROM dlq_messages WHERE tenant_id = ?1 AND dlq_id IN ({})",
                marks
            );
            let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(tenant.id.clone())];
            for id in ids {
                args.push(Box::new(id));
            }
            let refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
            conn.execute(sql.as_str(), refs.as_slice())?
        }
        None => conn.execute(
            "DELETE FROM dlq_messages WHERE tenant_id = ?1",
            [&tenant.id],
        )?,
    };
    Ok(Json(json!({ "deleted": deleted })))
}

pub(super) async fn retry_dlq(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(id): Path<String>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let message_id = {
        let mut conn = state.db.conn.lock().unwrap();
        let row = load_dlq(&conn, &tenant.id, &id)?;
        requeue_dlq_row(&mut conn, &row)?
    };
    state.engine.notify_dirty();
    Ok((
        StatusCode::CREATED,
        Json(json!({ "messageId": message_id })),
    ))
}

pub(super) async fn bulk_retry_dlq(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let filter: DlqIdsFilter =
        serde_json::from_slice(&body).unwrap_or(DlqIdsFilter { dlq_ids: None });
    let mut conn = state.db.conn.lock().unwrap();
    let rows: Vec<DlqRow> = {
        let sql = format!(
            "SELECT {} FROM dlq_messages WHERE tenant_id = ?1 ORDER BY rowid",
            dlq_cols()
        );
        let mut stmt = conn.prepare(&sql).map_err(|_| AppError::internal())?;
        let rows = stmt
            .query_map([&tenant.id], row_to_dlq)
            .map_err(|_| AppError::internal())?
            .filter_map(|x| x.ok())
            .collect();
        rows
    };
    let selected: Vec<DlqRow> = match filter.dlq_ids.filter(|v| !v.is_empty()) {
        Some(ids) => rows
            .into_iter()
            .filter(|r| ids.contains(&r.dlq_id))
            .collect(),
        None => rows,
    };
    let mut ids_out = Vec::new();
    for row in selected {
        ids_out.push(requeue_dlq_row(&mut conn, &row)?);
    }
    drop(conn);
    state.engine.notify_dirty();
    Ok(Json(json!({ "messageIds": ids_out })))
}
