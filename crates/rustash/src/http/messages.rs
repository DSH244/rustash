//! `messages`: read one message, cancel one, and the filtered bulk cancel.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Value};

use crate::store::{cancel_message_by_id, select_message_by_id};

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::wire::empty_chunked;
use super::AppState;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

pub(super) async fn get_message(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let row = select_message_by_id(&conn, &tenant.id, &id)?
        .ok_or_else(|| AppError::new(404, "message not found"))?;
    Ok(Json(row.to_message_json()))
}

pub(super) async fn cancel_message(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let conn = state.db.conn.lock().unwrap();
    let outcome = cancel_message_by_id(&conn, &tenant.id, &id)?;
    if outcome.is_none() {
        return Err(AppError::new(404, "message not found"));
    }
    state.engine.notify_dirty();
    Ok(empty_chunked(StatusCode::ACCEPTED))
}

#[derive(serde::Deserialize)]
struct BulkCancelFilter {
    #[serde(rename = "messageIds")]
    message_ids: Option<Vec<String>>,
    url: Option<String>,
    #[serde(rename = "scheduleId")]
    schedule_id: Option<String>,
    #[serde(rename = "queueName")]
    queue_name: Option<String>,
    #[serde(rename = "fromDate")]
    from_date: Option<i64>,
    #[serde(rename = "toDate")]
    to_date: Option<i64>,
}

pub(super) async fn bulk_cancel(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Value>)> {
    // A malformed/empty body is tolerated and treated as `{}` (no filters).
    let filter: BulkCancelFilter = serde_json::from_slice(&body).unwrap_or(BulkCancelFilter {
        message_ids: None,
        url: None,
        schedule_id: None,
        queue_name: None,
        from_date: None,
        to_date: None,
    });
    let mut sql = String::from("DELETE FROM messages WHERE tenant_id = ?1 AND state = 'pending'");
    let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(tenant.id.clone())];
    let mut idx = 2usize;
    if let Some(ids_list) = filter.message_ids.filter(|v| !v.is_empty()) {
        let marks = ids_list
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", idx + i))
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" AND id IN ({})", marks));
        for id in ids_list {
            args.push(Box::new(id));
            idx += 1;
        }
    }
    for (col, val) in [
        ("url", filter.url),
        ("schedule_id", filter.schedule_id),
        ("queue_name", filter.queue_name),
    ] {
        if let Some(v) = val {
            sql.push_str(&format!(" AND {} = ?{}", col, idx));
            args.push(Box::new(v));
            idx += 1;
        }
    }
    if let Some(v) = filter.from_date {
        sql.push_str(&format!(" AND created_at >= ?{}", idx));
        args.push(Box::new(v));
        idx += 1;
    }
    if let Some(v) = filter.to_date {
        sql.push_str(&format!(" AND created_at <= ?{}", idx));
        args.push(Box::new(v));
        idx += 1;
    }
    let _ = idx;
    let conn = state.db.conn.lock().unwrap();
    let refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
    let deleted = conn.execute(sql.as_str(), refs.as_slice())?;
    Ok((StatusCode::ACCEPTED, Json(json!({ "cancelled": deleted }))))
}
