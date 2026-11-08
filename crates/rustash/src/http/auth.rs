//! Request authentication: both QStash security schemes (the bearer header and
//! `?qstash_token=`), the tenant lookup behind them, and the middleware that parks the
//! resolved tenant in request extensions for [`Authed`] to pick up.

use axum::extract::{Request, State};
use axum::http::{HeaderMap, Uri};
use axum::middleware::Next;
use axum::response::Response;

use crate::db::Db;
use crate::ids;
use crate::tenant::{find_by_token_hash, Tenant};

use super::error::AppError;
use super::AppState;

/// Tenant pulled from request extensions by the auth middleware.
pub(super) struct Authed(pub(super) Tenant);

#[axum::async_trait]
impl<S: Send + Sync> axum::extract::FromRequestParts<S> for Authed {
    type Rejection = AppError;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _s: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Tenant>()
            .cloned()
            .map(Authed)
            .ok_or_else(|| AppError::new(401, "invalid authorization token"))
    }
}

/// Bearer token OR `?qstash_token=` (both QStash security schemes).
fn extract_token(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(rest) = auth.strip_prefix("Bearer ") {
            let t = rest.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    uri.query().and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(k, _)| k == "qstash_token")
            .map(|(_, v)| v.into_owned())
    })
}

fn resolve_tenant(db: &Db, token: &str) -> Option<Tenant> {
    find_by_token_hash(db, &ids::hash_token(token))
}

pub(super) async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let token = extract_token(req.headers(), req.uri());
    let Some(token) = token else {
        return Err(AppError::new(401, "missing authorization token"));
    };
    let tenant = resolve_tenant(&state.db, &token)
        .ok_or_else(|| AppError::new(401, "invalid authorization token"))?;
    req.extensions_mut().insert(tenant);
    Ok(next.run(req).await)
}
