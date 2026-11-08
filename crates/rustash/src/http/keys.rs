//! `keys`: read the tenant's current/next signing key, and rotate them.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::ids;
use crate::time::now_ms;

use super::auth::Authed;
use super::error::ApiResult;
use super::AppState;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

pub(super) async fn get_keys(
    State(_state): State<AppState>,
    Authed(tenant): Authed,
) -> Json<Value> {
    Json(json!({ "current": tenant.signing_key_current, "next": tenant.signing_key_next }))
}

pub(super) async fn rotate_keys(
    State(state): State<AppState>,
    Authed(tenant): Authed,
) -> ApiResult<Json<Value>> {
    let fresh = ids::new_signing_key();
    let conn = state.db.conn.lock().unwrap();
    conn.execute(
        "UPDATE tenants SET signing_key_current = signing_key_next, signing_key_next = ?2, updated_at = ?3 WHERE id = ?1",
        rusqlite::params![tenant.id, fresh, now_ms()],
    )?;
    Ok(Json(
        json!({ "current": tenant.signing_key_next, "next": fresh }),
    ))
}
