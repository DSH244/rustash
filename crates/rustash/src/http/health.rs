//! Liveness, readiness, and the fallback that answers every unmatched path.

use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use super::error::AppError;
use super::prefix;
use super::AppState;

// ---------------------------------------------------------------------------
// Health / router
// ---------------------------------------------------------------------------

pub(super) async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

pub(super) async fn readiness(State(state): State<AppState>) -> Response {
    let ok = state
        .db
        .conn
        .lock()
        .unwrap()
        .query_row("SELECT 1", [], |r| r.get::<_, i64>(0))
        .is_ok();
    if ok {
        (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable" })),
        )
            .into_response()
    }
}

pub(super) async fn not_found(uri: Uri) -> Response {
    // An empty publish tail does not match the `*tail` route but IS the
    // missing-destination request — the handler still serves it.
    if prefix::is_missing_publish(&uri) {
        return AppError::new(400, "missing destination").into_response();
    }
    (StatusCode::NOT_FOUND, "404 Not Found").into_response()
}
