//! The axum HTTP surface, served under both `/v2` (the real protocol paths) and `/v1`
//! aliases for the endpoints named in the task scope.
//!
//! The surface is split by responsibility so each file has one reason to change:
//!
//! * cross-cutting: `wire` (framing), `error` (the error body), `auth` (tenant lookup),
//!   `ingest` (request envelope), `prefix` (version surface + path tails), `paging`;
//! * one file per protocol resource family: `publish`, `batch`, `messages`, `keys`, `dlq`,
//!   `logs`, `queues`, `topics`, `schedules`, `flow_control`, `workflow_wait`,
//!   `workflow_runs`, `admin`, `health`;
//! * `router` — the only place a route is registered.
//!
//! Storage semantics stay in `store`/`lease`/`engine`: these files translate HTTP into those
//! calls and shape the JSON. `tools/check-structure.sh` enforces the module boundaries.

use std::sync::Arc;

use crate::config::Config;
use crate::db::Db;
use crate::engine::Engine;

mod admin;
mod auth;
mod batch;
mod dlq;
mod error;
mod flow_control;
mod health;
mod ingest;
mod keys;
mod logs;
mod messages;
mod paging;
mod prefix;
mod publish;
mod queues;
mod router;
mod schedules;
mod topics;
mod wire;
mod workflow_runs;
mod workflow_wait;

pub use error::AppError;
pub use router::{build_router, serve};

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub config: Arc<Config>,
    pub engine: Arc<Engine>,
}
