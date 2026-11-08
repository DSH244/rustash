//! `logs` and `events`: one query over `message_events`, two response field names.

use axum::extract::{Query, State};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::model::EventRow;
use crate::store::{row_to_event, EVENT_SELECT_COLS};

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::paging::{decode_cursor, encode_cursor};
use super::AppState;

#[derive(serde::Deserialize)]
pub(super) struct LogsQuery {
    count: Option<String>,
    cursor: Option<String>,
    #[serde(rename = "messageId")]
    message_id: Option<String>,
    state: Option<String>,
}

async fn logs_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Query(q): Query<LogsQuery>,
    field: &'static str,
) -> ApiResult<Json<Value>> {
    let limit = q
        .count
        .as_deref()
        .and_then(|c| c.parse::<i64>().ok())
        .unwrap_or(100)
        .min(1000);
    let mut sql = format!(
        "SELECT {} FROM message_events WHERE tenant_id = ?1",
        EVENT_SELECT_COLS
    );
    let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(tenant.id.clone())];
    let mut idx = 2usize;
    if let Some(mid) = q.message_id {
        sql.push_str(&format!(" AND message_id = ?{}", idx));
        args.push(Box::new(mid));
        idx += 1;
    }
    if let Some(st) = q.state {
        if st == "IN_PROGRESS" {
            sql.push_str(&format!(
                " AND state IN (?{},?{},?{})",
                idx,
                idx + 1,
                idx + 2
            ));
            args.push(Box::new("ACTIVE".to_string()));
            args.push(Box::new("RETRY".to_string()));
            args.push(Box::new("ERROR".to_string()));
            idx += 3;
        } else {
            sql.push_str(&format!(" AND state = ?{}", idx));
            args.push(Box::new(st));
            idx += 1;
        }
    }
    if let Some(cursor) = &q.cursor {
        if let Some((ms, id)) = decode_cursor(cursor) {
            sql.push_str(&format!(
                " AND (time < ?{} OR (time = ?{} AND id < ?{}))",
                idx,
                idx + 1,
                idx + 2
            ));
            args.push(Box::new(ms));
            args.push(Box::new(ms));
            args.push(Box::new(id.parse::<i64>().unwrap_or(0)));
            idx += 3;
        }
    }
    let _ = idx;
    sql.push_str(" ORDER BY time DESC, id DESC LIMIT ?NEXT");
    args.push(Box::new(limit + 1));
    let sql = sql.replace("?NEXT", &format!("?{}", args.len()));
    let conn = state.db.conn.lock().unwrap();
    let mut stmt = conn.prepare(&sql).map_err(|_| AppError::internal())?;
    let refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
    let rows: Vec<EventRow> = stmt
        .query_map(refs.as_slice(), row_to_event)
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .collect();
    let page: Vec<&EventRow> = rows.iter().take(limit as usize).collect();
    let next_cursor = if rows.len() as i64 > limit {
        page.last()
            .map(|e| encode_cursor(e.time, &e.id.to_string()))
    } else {
        None
    };
    let entries: Vec<Value> = page.iter().map(|e| e.to_log_json()).collect();
    let mut out = Map::new();
    out.insert(field.to_string(), Value::Array(entries));
    if let Some(c) = next_cursor {
        out.insert("cursor".into(), json!(c));
    }
    Ok(Json(Value::Object(out)))
}

/// `GET /logs` and `GET /events` differ only in the response array's field name, so the
/// route table points at two one-line wrappers instead of two inline closures.
pub(super) async fn logs_handler(
    state: State<AppState>,
    authed: Authed,
    query: Query<LogsQuery>,
) -> ApiResult<Json<Value>> {
    logs_route(state, authed, query, "logs").await
}

pub(super) async fn events_handler(
    state: State<AppState>,
    authed: Authed,
    query: Query<LogsQuery>,
) -> ApiResult<Json<Value>> {
    logs_route(state, authed, query, "events").await
}
