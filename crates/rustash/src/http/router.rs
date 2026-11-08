//! The single route registration point.
//!
//! Three tables describe the whole surface — unversioned, versioned-but-public, and the
//! authed API — and [`build_router`] is the only place that instantiates them. `/v2` and
//! `/v1` are expanded from [`API_PREFIXES`], so the two surfaces cannot answer different
//! route sets; `tools/check-structure.sh` re-expands these same tables and diffs the result
//! against the pre-split route list.

use axum::extract::DefaultBodyLimit;
use axum::middleware;
use axum::routing::{any, delete, get, post, MethodRouter};
use axum::Router;

use super::auth::auth_middleware;
use super::prefix::API_PREFIXES;
use super::wire::stamp_node_framing;
use super::AppState;
use super::{
    admin, batch, dlq, flow_control, health, keys, logs, messages, publish, queues, schedules,
    topics, workflow_runs, workflow_wait,
};

/// A path and the methods it answers.
type Route = (&'static str, MethodRouter<AppState>);

/// Routes with no version prefix at all.
fn unversioned_routes() -> Vec<Route> {
    vec![
        ("/health", get(health::health)),
        ("/healthz", get(health::health)),
        ("/readyz", get(health::readiness)),
    ]
}

/// Versioned routes that stay OUTSIDE the tenant middleware: liveness must answer without a
/// token, which is why the health routes are registered separately from `api_routes`.
fn versioned_public_routes() -> Vec<Route> {
    vec![("/health", get(health::health))]
}

/// Every authed API route, as `(suffix after the version prefix, methods)`.
///
/// `any(..)` reproduces the reference server, which does not restrict the pause/resume and
/// `schedules/*` routes to a single method.
fn api_routes() -> Vec<Route> {
    vec![
        ("/publish", post(publish::publish_route)),
        ("/publish/*tail", post(publish::publish_route)),
        (
            "/messages/:messageId",
            get(messages::get_message).delete(messages::cancel_message),
        ),
        ("/messages", delete(messages::bulk_cancel)),
        ("/keys", get(keys::get_keys)),
        ("/keys/rotate", post(keys::rotate_keys)),
        ("/dlq", get(dlq::list_dlq).delete(dlq::bulk_delete_dlq)),
        ("/dlq/retry", post(dlq::bulk_retry_dlq)),
        ("/dlq/retry/:dlqId", post(dlq::retry_dlq)),
        ("/dlq/:dlqId", get(dlq::get_dlq).delete(dlq::delete_dlq)),
        ("/logs", get(logs::logs_handler)),
        ("/events", get(logs::events_handler)),
        ("/enqueue/*tail", post(publish::enqueue_route)),
        ("/queue/*tail", post(publish::enqueue_route)),
        ("/batch", post(batch::batch_route)),
        ("/batchPublish", post(batch::batch_route)),
        (
            "/queues",
            post(queues::upsert_queue).get(queues::list_queues),
        ),
        (
            "/queues/:queueName",
            get(queues::get_queue).delete(queues::delete_queue),
        ),
        ("/queues/:queueName/pause", any(queues::pause_queue)),
        ("/queues/:queueName/resume", any(queues::resume_queue)),
        ("/topics", get(topics::list_topics)),
        (
            "/topics/:urlGroupName",
            get(topics::get_topic).delete(topics::delete_topic),
        ),
        (
            "/topics/:urlGroupName/endpoints",
            post(topics::add_endpoints).delete(topics::remove_endpoints),
        ),
        (
            "/schedules",
            get(schedules::list_schedules).post(schedules::create_schedule_route),
        ),
        ("/schedules/*tail", any(schedules::schedules_dispatch)),
        ("/globalParallelism", get(flow_control::global_parallelism)),
        ("/flowControl/:key", get(flow_control::get_flow_control)),
        (
            "/flowControl/:key/pause",
            any(flow_control::pause_flow_control),
        ),
        (
            "/flowControl/:key/resume",
            any(flow_control::resume_flow_control),
        ),
        (
            "/flowControl/:key/pin",
            post(flow_control::pin_flow_control),
        ),
        (
            "/flowControl/:key/unpin",
            post(flow_control::unpin_flow_control),
        ),
        (
            "/flowControl/:key/resetRate",
            post(flow_control::reset_flow_control_rate),
        ),
        ("/wait/:eventId", post(workflow_wait::wait_route)),
        ("/waiters/:eventId", get(workflow_wait::get_waiters)),
        ("/notify/:eventId", post(workflow_wait::notify_route_event)),
        (
            "/notify/:workflowRunId/:eventId",
            post(workflow_wait::notify_route_run),
        ),
        (
            "/workflows/runs",
            delete(workflow_runs::bulk_cancel_workflow_runs),
        ),
        (
            "/workflows/runs/:runId",
            get(workflow_runs::get_workflow_run).delete(workflow_runs::cancel_workflow_run),
        ),
        ("/workflows/logs", get(workflow_runs::workflow_logs)),
    ]
}

/// Build the router: public health/readiness, then the authed API surface under every
/// version prefix. Routes carry their full version prefix — axum's `nest` would strip it
/// from `Uri` before handlers extract the destination tail.
pub fn build_router(state: AppState) -> Router {
    let mut public = Router::new();
    for (path, methods) in unversioned_routes() {
        public = public.route(path, methods);
    }
    for prefix in API_PREFIXES {
        for (suffix, methods) in versioned_public_routes() {
            public = public.route(&format!("{prefix}{suffix}"), methods);
        }
    }

    let mut api = Router::new();
    for prefix in API_PREFIXES {
        for (suffix, methods) in api_routes() {
            api = api.route(&format!("{prefix}{suffix}"), methods);
        }
    }
    let api = api.route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth_middleware,
    ));

    let mut router = Router::new().merge(public).merge(api);
    // The registry is only served when an admin token is configured.
    if state.config.admin_token.is_some() {
        let mut registry = Router::new();
        for (path, methods) in admin::routes() {
            registry = registry.route(path, methods);
        }
        let registry = registry.route_layer(middleware::from_fn_with_state(
            state.clone(),
            admin::admin_auth,
        ));
        router = router.merge(registry);
    }
    router
        .fallback(health::not_found)
        // MAX_BODY_BYTES (10 MiB by default) is enforced by `check_body_limit` with the
        // exact QStash 412; axum's own 2 MiB `Bytes` default would answer 413 first.
        .layer(DefaultBodyLimit::disable())
        .layer(middleware::from_fn(stamp_node_framing))
        .with_state(state)
}

/// Serve everything (called by the binary and by tests).
pub async fn serve(state: AppState, listener: tokio::net::TcpListener) -> std::io::Result<()> {
    let router = build_router(state);
    axum::serve(listener, router).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pre-split contract generated mechanically from commit de59b1c.
    const BASELINE: &str = r#"
GET /health
GET /healthz
GET /readyz
GET /v2/health
GET /v1/health
POST /v2/publish
POST /v2/publish/*tail
GET /v2/messages/:messageId
DELETE /v2/messages/:messageId
DELETE /v2/messages
GET /v2/keys
POST /v2/keys/rotate
GET /v2/dlq
DELETE /v2/dlq
POST /v2/dlq/retry
POST /v2/dlq/retry/:dlqId
GET /v2/dlq/:dlqId
DELETE /v2/dlq/:dlqId
GET /v2/logs
GET /v2/events
POST /v2/enqueue/*tail
POST /v2/queue/*tail
POST /v2/batch
POST /v2/batchPublish
POST /v2/queues
GET /v2/queues
GET /v2/queues/:queueName
DELETE /v2/queues/:queueName
ANY /v2/queues/:queueName/pause
ANY /v2/queues/:queueName/resume
GET /v2/topics
GET /v2/topics/:urlGroupName
DELETE /v2/topics/:urlGroupName
POST /v2/topics/:urlGroupName/endpoints
DELETE /v2/topics/:urlGroupName/endpoints
GET /v2/schedules
POST /v2/schedules
ANY /v2/schedules/*tail
GET /v2/globalParallelism
GET /v2/flowControl/:key
ANY /v2/flowControl/:key/pause
ANY /v2/flowControl/:key/resume
POST /v2/flowControl/:key/pin
POST /v2/flowControl/:key/unpin
POST /v2/flowControl/:key/resetRate
POST /v2/wait/:eventId
GET /v2/waiters/:eventId
POST /v2/notify/:eventId
POST /v2/notify/:workflowRunId/:eventId
DELETE /v2/workflows/runs
GET /v2/workflows/runs/:runId
DELETE /v2/workflows/runs/:runId
GET /v2/workflows/logs
POST /v1/publish
POST /v1/publish/*tail
GET /v1/messages/:messageId
DELETE /v1/messages/:messageId
DELETE /v1/messages
GET /v1/keys
POST /v1/keys/rotate
GET /v1/dlq
DELETE /v1/dlq
POST /v1/dlq/retry
POST /v1/dlq/retry/:dlqId
GET /v1/dlq/:dlqId
DELETE /v1/dlq/:dlqId
GET /v1/logs
GET /v1/events
POST /v1/enqueue/*tail
POST /v1/queue/*tail
POST /v1/batch
POST /v1/batchPublish
POST /v1/queues
GET /v1/queues
GET /v1/queues/:queueName
DELETE /v1/queues/:queueName
ANY /v1/queues/:queueName/pause
ANY /v1/queues/:queueName/resume
GET /v1/topics
GET /v1/topics/:urlGroupName
DELETE /v1/topics/:urlGroupName
POST /v1/topics/:urlGroupName/endpoints
DELETE /v1/topics/:urlGroupName/endpoints
GET /v1/schedules
POST /v1/schedules
ANY /v1/schedules/*tail
GET /v1/globalParallelism
GET /v1/flowControl/:key
ANY /v1/flowControl/:key/pause
ANY /v1/flowControl/:key/resume
POST /v1/flowControl/:key/pin
POST /v1/flowControl/:key/unpin
POST /v1/flowControl/:key/resetRate
POST /v1/wait/:eventId
GET /v1/waiters/:eventId
POST /v1/notify/:eventId
POST /v1/notify/:workflowRunId/:eventId
DELETE /v1/workflows/runs
GET /v1/workflows/runs/:runId
DELETE /v1/workflows/runs/:runId
GET /v1/workflows/logs
POST /admin/tenants
GET /admin/tenants
GET /admin/tenants/:id
DELETE /admin/tenants/:id
POST /admin/tenants/:id/rotate-token
POST /admin/tenants/:id/rotate-keys
"#;

    fn baseline_lines() -> Vec<String> {
        BASELINE
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(String::from)
            .collect()
    }

    fn baseline_paths() -> Vec<String> {
        let mut paths: Vec<String> = baseline_lines()
            .iter()
            .map(|l| l.split_once(' ').expect("METHOD PATH").1.to_string())
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }

    /// Expanded path set of the tables `build_router` installs, including the admin surface
    /// that is merged only when `ADMIN_TOKEN` is configured.
    fn current_paths() -> Vec<String> {
        let mut paths: Vec<String> = Vec::new();
        for (path, _) in unversioned_routes() {
            paths.push(path.to_string());
        }
        for prefix in API_PREFIXES {
            for (suffix, _) in versioned_public_routes() {
                paths.push(format!("{prefix}{suffix}"));
            }
            for (suffix, _) in api_routes() {
                paths.push(format!("{prefix}{suffix}"));
            }
        }
        for (path, _) in admin::routes() {
            paths.push(path.to_string());
        }
        paths.sort();
        paths.dedup();
        paths
    }

    #[test]
    fn expanded_route_paths_match_the_pre_split_contract() {
        let expected = baseline_paths();
        let actual = current_paths();
        let missing: Vec<&String> = expected.iter().filter(|p| !actual.contains(p)).collect();
        let extra: Vec<&String> = actual.iter().filter(|p| !expected.contains(p)).collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "route path sets differ\nmissing={missing:?}\nextra={extra:?}"
        );
        assert_eq!(actual.len(), expected.len());
    }

    #[test]
    fn versioned_api_tables_are_identical_under_every_prefix() {
        // The whole point of the prefix table: the two surfaces must not drift apart. Every
        // suffix the expansion consumes is registered under every prefix, so compare the
        // suffix sets and then require the fully expanded paths in the baseline contract.
        let suffixes = {
            let mut s: Vec<String> = api_routes()
                .into_iter()
                .map(|(suffix, _)| suffix.to_string())
                .collect();
            s.extend(
                versioned_public_routes()
                    .into_iter()
                    .map(|(suffix, _)| suffix.to_string()),
            );
            s.sort();
            s
        };
        assert_eq!(API_PREFIXES.len(), 2, "the alias table is v2 + v1");
        let baseline = baseline_paths();
        for prefix in API_PREFIXES {
            for suffix in &suffixes {
                let full = format!("{prefix}{suffix}");
                assert!(
                    baseline.contains(&full),
                    "{full} is not in the baseline contract"
                );
            }
        }
    }

    #[test]
    fn baseline_contract_covers_the_admin_surface() {
        let baseline = baseline_paths();
        for (path, _) in admin::routes() {
            assert!(
                baseline.contains(&path.to_string()),
                "admin route {path} missing from the baseline contract"
            );
        }
    }
}
