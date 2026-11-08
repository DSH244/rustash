//! Publish semantics and the two entry points that share them.
//!
//! [`publish_to_destination`] is the whole of "accept a message": the payload gate,
//! destination resolution (a URL, or a URL group whose endpoints fan out), the SSRF check
//! per endpoint, flow-control key refresh, workflow INIT idempotency, and the dedupe-aware
//! insert. `publish_route` and `enqueue_route` only differ in where the destination and the
//! queue name come from, and in the order their body gate runs.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

use crate::flow;
use crate::headers::MessageSpec;
use crate::model::publish_item_json;
use crate::ssrf::{create_policy, validate_destination_url};
use crate::store::{create_message, dedupe_key_for, spec_to_row};
use crate::tenant::Tenant;
use crate::time::now_ms;
use crate::workflow;

use super::auth::Authed;
use super::error::{ApiResult, AppError};
use super::ingest::{
    caller_ip, check_payload_limit, envelope, is_url, legacy_destination, GateOrder, HEADERS_ONLY,
    QUERY_CONFIG,
};
use super::prefix::{self, Tail, ENQUEUE, PUBLISH, QUEUE};
use super::queues::ensure_queue;
use super::AppState;

pub(super) struct PublishResult {
    message_id: String,
    url: Option<String>,
    deduplicated: bool,
}

/// publishToDestination: MAX_BODY_BYTES gate → resolve destination (URL or group
/// fan-out) → SSRF check each → dedupe-aware insert per endpoint.
pub(super) async fn publish_to_destination(
    state: &AppState,
    tenant: &Tenant,
    destination: &str,
    body: Option<Vec<u8>>,
    spec: &MessageSpec,
    queue_name_override: Option<String>,
    caller: Option<String>,
) -> ApiResult<Vec<PublishResult>> {
    if let Some(b) = &body {
        if !b.is_empty() {
            check_payload_limit(b.len(), state.config.max_body_bytes)?;
        }
    }
    // resolveDestination
    let endpoints: Vec<(String, Option<String>, Option<String>)> = if is_url(destination) {
        vec![(destination.to_string(), None, None)]
    } else {
        let group_id: Option<String> = {
            let conn = state.db.conn.lock().unwrap();
            conn.query_row(
                "SELECT id FROM url_groups WHERE tenant_id = ?1 AND name = ?2",
                rusqlite::params![tenant.id.clone(), destination.to_string()],
                |r| r.get::<_, String>(0),
            )
            .ok()
        };
        let group_id = match group_id {
            Some(g) => g,
            None => {
                return Err(AppError::new(
                    404,
                    format!("URL group \"{}\" not found", destination),
                ))
            }
        };
        let eps: Vec<(String, Option<String>)> = {
            let conn = state.db.conn.lock().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT url, name FROM url_group_endpoints WHERE group_id = ?1 ORDER BY rowid",
                )
                .map_err(|_| AppError::internal())?;
            let rows = stmt
                .query_map([group_id.clone()], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(|_| AppError::internal())?
                .filter_map(|x| x.ok())
                .collect();
            rows
        };
        if eps.is_empty() {
            return Err(AppError::new(
                400,
                format!("URL group \"{}\" has no endpoints", destination),
            ));
        }
        eps.into_iter()
            .map(|(u, name)| (u, Some(destination.to_string()), name))
            .collect()
    };

    let policy = create_policy(
        state.config.allow_private_destinations,
        &state.config.destination_allow_cidrs,
        &state.config.destination_block_cidrs,
    );

    // Every publish carrying an fc spec refreshes its key config (pinned dimensions keep
    // the stored values; a changed rate restarts the bucket).
    if let Some(fc) = &spec.flow_control {
        let conn = state.db.conn.lock().unwrap();
        let _ = flow::upsert_key_config(&conn, &tenant.id, fc);
    }

    // Workflow INIT idempotency: the run id is the idempotency key of the initial
    // invocation, so a repeat marked `Upstash-Workflow-Init: true` creates nothing and
    // answers with the run's first recorded step messageId.
    if let Some(run_id) = &spec.workflow_run_id {
        let is_init = workflow::forwarded_header(&spec.forward_headers, "upstash-workflow-init")
            .as_deref()
            == Some("true");
        if is_init {
            let existing: Option<String> = {
                let conn = state.db.conn.lock().unwrap();
                conn.query_row(
                    "SELECT workflow_run_id FROM workflow_runs WHERE tenant_id = ?1 AND workflow_run_id = ?2",
                    rusqlite::params![tenant.id, run_id],
                    |r| r.get(0),
                )
                .ok()
            };
            if existing.is_some() {
                let first: Option<String> = {
                    let conn = state.db.conn.lock().unwrap();
                    conn.query_row(
                        "SELECT message_id FROM workflow_steps WHERE tenant_id = ?1 AND workflow_run_id = ?2 ORDER BY id LIMIT 1",
                        rusqlite::params![tenant.id, run_id],
                        |r| r.get(0),
                    )
                    .ok()
                };
                let message_id = first.unwrap_or_else(|| run_id.clone());
                let results: Vec<PublishResult> = endpoints
                    .iter()
                    .map(|(url, topic_name, _)| PublishResult {
                        message_id: message_id.clone(),
                        url: topic_name.as_ref().map(|_| url.clone()),
                        deduplicated: true,
                    })
                    .collect();
                return Ok(results);
            }
        }
    }

    let mut results = Vec::new();
    for (url, topic_name, endpoint_name) in endpoints {
        // ingest SSRF structural check
        validate_destination_url(&policy, &url)?;
        let mut row = spec_to_row(&tenant.id, &url, body.clone(), spec, now_ms());
        row.topic_name = topic_name.clone();
        row.endpoint_name = endpoint_name.clone();
        row.queue_name = queue_name_override
            .clone()
            .or_else(|| spec.queue_name.clone());
        row.caller_ip = caller.clone();
        let key = dedupe_key_for(spec, &url, row.body.as_deref());
        let mut conn = state.db.conn.lock().unwrap();
        let (message_id, deduplicated) = create_message(
            &mut conn,
            &row,
            key.as_deref(),
            state.config.dedupe_window_secs,
        )?;
        // Workflow steps persist beyond delivery so run history can be replayed; the run
        // itself is archived RUN_STARTED on first sight. Recording on a deduplicated
        // publish is a no-op for a genuine duplicate and restores a lost init-step row.
        if let Some(run_id) = &spec.workflow_run_id {
            let _ = workflow::record_run_start(
                &conn,
                &tenant.id,
                run_id,
                &url,
                spec.flow_control.as_ref().map(|f| f.key.as_str()),
            );
            let _ = workflow::record_step(
                &conn,
                &tenant.id,
                run_id,
                &message_id,
                body.as_deref(),
                &spec.forward_headers,
            );
        }
        drop(conn);
        results.push(PublishResult {
            message_id,
            url: topic_name.as_ref().map(|_| url.clone()),
            deduplicated,
        });
    }
    state.engine.notify_dirty();
    Ok(results)
}

pub(super) fn publish_json(results: &[PublishResult], single: bool) -> Value {
    let items: Vec<Value> = results
        .iter()
        .map(|r| publish_item_json(&r.message_id, r.url.as_deref(), r.deduplicated))
        .collect();
    if single {
        items.into_iter().next().unwrap_or(Value::Null)
    } else {
        Value::Array(items)
    }
}

pub(super) async fn publish_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let mut destination = prefix::resource_tail(&uri, &[PUBLISH, PUBLISH], Tail::WithQuery);
    if destination.is_empty() && prefix::is_v1(&uri) {
        // Legacy QStash v1 shape: POST /v1/publish with the Upstash-Destination header.
        destination = legacy_destination(&headers).unwrap_or_default();
    }
    if destination.is_empty() {
        return Err(AppError::new(400, "missing destination"));
    }
    let (spec, body_vec) = envelope(
        &headers,
        &body,
        uri.query(),
        &state.config,
        QUERY_CONFIG,
        GateOrder::HeadersFirst,
    )?;
    let caller = caller_ip(&headers);
    let results =
        publish_to_destination(&state, &tenant, &destination, body_vec, &spec, None, caller)
            .await?;
    Ok((
        StatusCode::CREATED,
        Json(publish_json(&results, is_url(&destination))),
    )
        .into_response())
}

pub(super) async fn enqueue_route(
    State(state): State<AppState>,
    Authed(tenant): Authed,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let rest = prefix::resource_tail(&uri, &[ENQUEUE, QUEUE], Tail::WithQuery);
    let (queue_name, destination) = match rest.split_once('/') {
        Some((q, d)) if !q.is_empty() && !d.is_empty() => (q.to_string(), d.to_string()),
        // Legacy QStash v1 shape: POST /v1/queue/{queueName} with Upstash-Destination header.
        _ if prefix::is_v1(&uri) => match legacy_destination(&headers) {
            Some(destination) if !rest.is_empty() && !rest.contains('/') => {
                (rest.to_string(), destination)
            }
            _ => return Err(AppError::new(400, "missing queue name or destination")),
        },
        _ => return Err(AppError::new(400, "missing queue name or destination")),
    };
    // auto-create the queue if absent (idempotent, parallelism 1)
    {
        let conn = state.db.conn.lock().unwrap();
        ensure_queue(&conn, &tenant.id, &queue_name);
    }
    // fork order: readLimitedBody runs BEFORE header parsing
    let (spec, body_vec) = envelope(
        &headers,
        &body,
        None,
        &state.config,
        HEADERS_ONLY,
        GateOrder::BodyFirst,
    )?;
    let caller = caller_ip(&headers);
    let results = publish_to_destination(
        &state,
        &tenant,
        &destination,
        body_vec,
        &spec,
        Some(queue_name),
        caller,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(publish_json(&results, is_url(&destination))),
    )
        .into_response())
}
