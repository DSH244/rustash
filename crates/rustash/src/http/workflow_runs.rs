//! Workflow runs: read a run's steps, page its log rows, cancel one run, cancel a batch.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Map, Value};

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::wire::empty_chunked;
use super::AppState;

pub(super) async fn get_workflow_run(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let steps = crate::workflow::get_run_steps_json(&conn, &tenant.id, &run_id)
        .map_err(|_| AppError::internal())?;
    if steps.is_empty() {
        return Err(AppError::new(404, "workflow run not found"));
    }
    Ok(Json(Value::Array(steps)))
}

#[derive(serde::Deserialize, Default)]
pub(super) struct WorkflowLogsQuery {
    #[serde(rename = "workflowRunId")]
    workflow_run_id: Option<String>,
    state: Option<String>,
    #[serde(rename = "fromDate")]
    from_date: Option<String>,
    #[serde(rename = "toDate")]
    to_date: Option<String>,
    count: Option<String>,
    cursor: Option<String>,
}

pub(super) async fn workflow_logs(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Query(q): Query<WorkflowLogsQuery>,
) -> ApiResult<Json<Value>> {
    let conn = state.db.conn.lock().unwrap();
    let mut sql = String::from(
        "SELECT tenant_id, workflow_run_id, workflow_url, state, dlq_id, flow_control_key, created_at, completed_at, updated_at FROM workflow_runs WHERE tenant_id = ?1",
    );
    let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(tenant.id.clone())];
    let mut idx = 2usize;
    if let Some(run) = &q.workflow_run_id {
        if !run.is_empty() {
            sql.push_str(&format!(" AND workflow_run_id = ?{}", idx));
            args.push(Box::new(run.clone()));
            idx += 1;
        }
    }
    if let Some(s) = &q.state {
        if matches!(
            s.as_str(),
            "RUN_STARTED" | "RUN_SUCCESS" | "RUN_FAILED" | "RUN_CANCELED"
        ) {
            sql.push_str(&format!(" AND state = ?{}", idx));
            args.push(Box::new(s.clone()));
            idx += 1;
        }
    }
    if let Some(raw) = &q.from_date {
        if let Ok(n) = raw.parse::<i64>() {
            sql.push_str(&format!(" AND created_at >= ?{}", idx));
            args.push(Box::new(n));
            idx += 1;
        }
    }
    if let Some(raw) = &q.to_date {
        if let Ok(n) = raw.parse::<i64>() {
            sql.push_str(&format!(" AND created_at <= ?{}", idx));
            args.push(Box::new(n));
            idx += 1;
        }
    }
    let count = q
        .count
        .as_deref()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|n| *n != 0.0)
        .unwrap_or(1000.0)
        .clamp(1.0, 1000.0) as i64;
    let offset = q
        .cursor
        .as_deref()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|n| n.is_finite())
        .unwrap_or(0.0)
        .max(0.0) as i64;
    sql.push_str(&format!(
        " ORDER BY created_at DESC, workflow_run_id DESC LIMIT ?{} OFFSET ?{}",
        idx,
        idx + 1
    ));
    args.push(Box::new(count + 1));
    args.push(Box::new(offset));
    let refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
    let mut stmt = conn.prepare(&sql).map_err(|_| AppError::internal())?;
    struct RunRow {
        tenant_id: String,
        run_id: String,
        url: String,
        state: String,
        dlq_id: Option<String>,
        fc_key: Option<String>,
        created_at: i64,
        completed_at: Option<i64>,
    }
    let mut rows: Vec<RunRow> = stmt
        .query_map(refs.as_slice(), |r| {
            Ok(RunRow {
                tenant_id: r.get(0)?,
                run_id: r.get(1)?,
                url: r.get(2)?,
                state: r.get(3)?,
                dlq_id: r.get(4)?,
                fc_key: r.get(5)?,
                created_at: r.get(6)?,
                completed_at: r.get(7)?,
            })
        })
        .map_err(|_| AppError::internal())?
        .filter_map(|x| x.ok())
        .collect();
    let has_more = rows.len() as i64 > count;
    rows.truncate(count as usize);
    let next_cursor = if has_more {
        (offset + count).to_string()
    } else {
        String::new()
    };
    let hydrate = q
        .workflow_run_id
        .as_deref()
        .map(|s| !s.is_empty())
        .unwrap_or(false)
        && rows.len() == 1;
    let runs: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            let steps = if hydrate {
                crate::workflow::get_run_steps_json(&conn, &r.tenant_id, &r.run_id)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            let mut m = Map::new();
            m.insert("workflowRunId".into(), json!(r.run_id));
            m.insert("workflowUrl".into(), json!(r.url));
            m.insert("workflowState".into(), json!(r.state));
            m.insert("workflowRunCreatedAt".into(), json!(r.created_at));
            m.insert(
                "workflowRunCompletedAt".into(),
                json!(r.completed_at.unwrap_or(0)),
            );
            if let Some(d) = r.dlq_id {
                m.insert("dlqId".into(), json!(d));
            }
            if let Some(f) = r.fc_key {
                m.insert("flowControlKey".into(), json!(f));
            }
            m.insert("steps".into(), Value::Array(steps));
            Value::Object(m)
        })
        .collect();
    Ok(Json(json!({ "cursor": next_cursor, "runs": runs })))
}

pub(super) async fn cancel_workflow_run(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    Path(run_id): Path<String>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> ApiResult<Response> {
    let cancelled = q.get("cancel").map(|v| v != "false").unwrap_or(true);
    let mut conn = state.db.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|_| AppError::internal())?;
    crate::workflow::set_run_state(
        &tx,
        &tenant.id,
        &run_id,
        if cancelled {
            "RUN_CANCELED"
        } else {
            "RUN_SUCCESS"
        },
        None,
    )?;
    tx.execute(
        "DELETE FROM workflow_waiters WHERE tenant_id = ?1 AND workflow_run_id = ?2",
        rusqlite::params![tenant.id, run_id],
    )?;
    tx.execute(
        "DELETE FROM workflow_steps WHERE tenant_id = ?1 AND workflow_run_id = ?2",
        rusqlite::params![tenant.id, run_id],
    )?;
    tx.commit().map_err(|_| AppError::internal())?;
    let _ = crate::workflow::delete_run_messages(&conn, &tenant.id, &run_id);
    Ok(empty_chunked(StatusCode::OK))
}

pub(super) async fn bulk_cancel_workflow_runs(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
    let mut prefixes: Vec<String> = Vec::new();
    match parsed.get("workflowUrl") {
        Some(Value::Array(a)) => {
            for v in a {
                if let Some(s) = v.as_str() {
                    if !s.is_empty() {
                        prefixes.push(s.to_string());
                    }
                }
            }
        }
        Some(Value::String(s)) if !s.is_empty() => prefixes.push(s.clone()),
        _ => {}
    }
    let ids: Vec<String> = parsed
        .get("workflowRunIds")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let mut cancelled = 0i64;
    if !prefixes.is_empty() {
        let matches: Vec<String> = {
            let conn = state.db.conn.lock().unwrap();
            let ors = prefixes
                .iter()
                .enumerate()
                .map(|(i, _)| format!("workflow_url LIKE ?{}", i + 3))
                .collect::<Vec<_>>()
                .join(" OR ");
            let sql = format!(
                "SELECT workflow_run_id FROM workflow_runs WHERE tenant_id = ?1 AND state = 'RUN_STARTED' AND ({})",
                ors
            );
            let mut stmt = conn.prepare(&sql).map_err(|_| AppError::internal())?;
            let mut args: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
                Box::new(tenant.id.clone()),
                Box::new("RUN_STARTED".to_string()),
            ];
            for p in &prefixes {
                args.push(Box::new(format!("{}%", p)));
            }
            let refs: Vec<&dyn rusqlite::types::ToSql> = args.iter().map(|a| a.as_ref()).collect();
            let rows = stmt
                .query_map(refs.as_slice(), |r| r.get::<_, String>(0))
                .map_err(|_| AppError::internal())?
                .filter_map(|x| x.ok())
                .collect();
            rows
        };
        for run_id in matches {
            let mut conn = state.db.conn.lock().unwrap();
            if crate::workflow::cancel_run(&mut conn, &tenant.id, &run_id)
                .map_err(|_| AppError::internal())?
            {
                cancelled += 1;
            }
        }
    }
    for run_id in ids {
        let mut conn = state.db.conn.lock().unwrap();
        if crate::workflow::cancel_run(&mut conn, &tenant.id, &run_id)
            .map_err(|_| AppError::internal())?
        {
            cancelled += 1;
        }
    }
    Ok(Json(json!({ "cancelled": cancelled })))
}
