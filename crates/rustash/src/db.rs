//! SQLite plumbing: the connection wrapper, the startup DDL migrations, and the
//! bootstrap tenant. Ids are TEXT, timestamps are INTEGER epoch-ms, JSON is TEXT and
//! arrays are TEXT JSON, which is the whole mapping SQLite needs.
//!
//! The single `Connection` is wrapped in one `Mutex`: every caller takes it for the
//! duration of a statement or transaction, never across an await. `prepare_cached`
//! keeps the hot statements alive on that one connection, so increasing the cache
//! capacity here (rather than opening more connections) preserves the single-writer
//! contract while avoiding a sqlite3_prepare_v2 per publish/claim/event append.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::config::Config;
use crate::ids;

/// Bounded capacity for the single connection's prepared-statement cache. The hot path
/// touches well over rusqlite's default (16) distinct statements; 64 holds the working set
/// without growing with request count. One named constant keeps the policy auditable.
pub const PREPARED_STATEMENT_CACHE_CAPACITY: usize = 64;

/// Single-writer SQLite connection. rusqlite calls are synchronous and short-lived
/// (microseconds); handlers take the lock only for the duration of a statement or
/// transaction — never across an await. The Mutex is what serializes concurrent claims.
#[derive(Clone)]
pub struct Db {
    pub conn: Arc<Mutex<Connection>>,
}

/// Ordered migration list — the startup DDL script. Each entry is (version, name, sql);
/// applied once and recorded in `__migrations`.
pub const MIGRATIONS: &[(i64, &str, &str)] = &[
    (
        1,
        "0000_sqlite_init",
        r#"
CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  external_id TEXT UNIQUE,
  signing_key_current TEXT NOT NULL,
  signing_key_next TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);

CREATE TABLE IF NOT EXISTS dedupe_entries (
  tenant_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  message_id TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, dedupe_key)
);
CREATE INDEX IF NOT EXISTS idx_dedupe_expiry ON dedupe_entries (expires_at);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'message',
  state TEXT NOT NULL DEFAULT 'pending',
  url TEXT NOT NULL,
  topic_name TEXT,
  endpoint_name TEXT,
  queue_name TEXT,
  schedule_id TEXT,
  method TEXT NOT NULL DEFAULT 'POST',
  headers TEXT NOT NULL DEFAULT '{}',
  body BLOB,
  callback_url TEXT,
  callback_opts TEXT,
  failure_callback_url TEXT,
  failure_callback_opts TEXT,
  max_retries INTEGER NOT NULL,
  retry_delay_expr TEXT,
  timeout_ms INTEGER,
  not_before INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  flow_control_key TEXT,
  fc_parallelism INTEGER,
  fc_rate INTEGER,
  fc_period_secs INTEGER,
  labels TEXT NOT NULL DEFAULT '[]',
  redact_fields TEXT,
  caller_ip TEXT,
  retried INTEGER NOT NULL DEFAULT 0,
  leased_until INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE INDEX IF NOT EXISTS idx_messages_due ON messages (tenant_id, state, not_before) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_messages_queue ON messages (tenant_id, queue_name, created_at) WHERE queue_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_fc ON messages (tenant_id, flow_control_key) WHERE flow_control_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_lease ON messages (leased_until) WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS idx_messages_schedule ON messages (tenant_id, schedule_id);
CREATE INDEX IF NOT EXISTS idx_messages_url ON messages (tenant_id, url);
-- Order-preserving pending scan: the dispatcher only ever needs the FIFO head of the
-- due set, and this index serves `ORDER BY not_before, created_at, id` directly so the
-- claim stops at its LIMIT instead of materialising a TEMP B-TREE over every due row.
-- Partial: only pending rows participate, so the write cost is bounded by the backlog.
CREATE INDEX IF NOT EXISTS idx_messages_fifo ON messages (tenant_id, state, not_before, created_at, id) WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS dlq_messages (
  dlq_id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'message',
  url TEXT NOT NULL,
  topic_name TEXT,
  endpoint_name TEXT,
  queue_name TEXT,
  schedule_id TEXT,
  method TEXT NOT NULL,
  headers TEXT NOT NULL DEFAULT '{}',
  body BLOB,
  callback_url TEXT,
  callback_opts TEXT,
  failure_callback_url TEXT,
  failure_callback_opts TEXT,
  max_retries INTEGER NOT NULL,
  retry_delay_expr TEXT,
  timeout_ms INTEGER,
  flow_control_key TEXT,
  fc_parallelism INTEGER,
  fc_rate INTEGER,
  fc_period_secs INTEGER,
  labels TEXT NOT NULL DEFAULT '[]',
  caller_ip TEXT,
  retried INTEGER NOT NULL,
  last_error TEXT,
  response_status INTEGER,
  response_headers TEXT,
  response_body BLOB,
  created_at INTEGER NOT NULL,
  dlq_created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE INDEX IF NOT EXISTS idx_dlq_cursor ON dlq_messages (tenant_id, dlq_created_at, dlq_id);
CREATE INDEX IF NOT EXISTS idx_dlq_url ON dlq_messages (tenant_id, url);
CREATE INDEX IF NOT EXISTS idx_dlq_schedule ON dlq_messages (tenant_id, schedule_id);
CREATE INDEX IF NOT EXISTS idx_dlq_queue ON dlq_messages (tenant_id, queue_name);
CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_messages (tenant_id, response_status);

CREATE TABLE IF NOT EXISTS message_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  tenant_id TEXT NOT NULL,
  time INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  message_id TEXT NOT NULL,
  state TEXT NOT NULL,
  error TEXT,
  next_delivery_time INTEGER,
  url TEXT NOT NULL,
  topic_name TEXT,
  endpoint_name TEXT,
  schedule_id TEXT,
  queue_name TEXT,
  method TEXT NOT NULL,
  headers TEXT NOT NULL DEFAULT '{}',
  body_base64 TEXT,
  response_status INTEGER,
  response_body_base64 TEXT,
  response_headers TEXT,
  timeout_ms INTEGER,
  callback_url TEXT,
  failure_callback_url TEXT,
  max_retries INTEGER NOT NULL,
  retried INTEGER NOT NULL DEFAULT 0,
  retry_delay_expr TEXT,
  labels TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_events_cursor ON message_events (tenant_id, time, id);
CREATE INDEX IF NOT EXISTS idx_events_message ON message_events (tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_events_state ON message_events (tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_events_time ON message_events (time);

CREATE TABLE IF NOT EXISTS queues (
  id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  parallelism INTEGER NOT NULL DEFAULT 1,
  paused INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_queues_tenant_name ON queues (tenant_id, name);

CREATE TABLE IF NOT EXISTS schedules (
  id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT NOT NULL,
  destination TEXT NOT NULL,
  cron_expr TEXT NOT NULL,
  tz TEXT,
  method TEXT NOT NULL DEFAULT 'POST',
  headers TEXT NOT NULL DEFAULT '{}',
  body BLOB,
  retries INTEGER NOT NULL,
  retry_delay_expr TEXT,
  delay_secs INTEGER,
  timeout_ms INTEGER,
  callback_url TEXT,
  callback_opts TEXT,
  failure_callback_url TEXT,
  failure_callback_opts TEXT,
  queue_name TEXT,
  flow_control_key TEXT,
  fc_parallelism INTEGER,
  fc_rate INTEGER,
  fc_period_secs INTEGER,
  labels TEXT NOT NULL DEFAULT '[]',
  redact_fields TEXT,
  caller_ip TEXT,
  is_paused INTEGER NOT NULL DEFAULT 0,
  next_run_at INTEGER NOT NULL,
  last_run_at INTEGER,
  last_planned_run_at INTEGER,
  last_message_ids TEXT NOT NULL DEFAULT '[]',
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (is_paused, next_run_at);
CREATE INDEX IF NOT EXISTS idx_schedules_tenant ON schedules (tenant_id);

CREATE TABLE IF NOT EXISTS flow_control_keys (
  tenant_id TEXT NOT NULL,
  key TEXT NOT NULL,
  parallelism INTEGER,
  rate INTEGER,
  period_secs INTEGER NOT NULL DEFAULT 1,
  pinned_parallelism INTEGER NOT NULL DEFAULT 0,
  pinned_rate INTEGER NOT NULL DEFAULT 0,
  paused INTEGER NOT NULL DEFAULT 0,
  rate_count INTEGER NOT NULL DEFAULT 0,
  rate_period_start INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  PRIMARY KEY (tenant_id, key)
);

CREATE TABLE IF NOT EXISTS url_groups (
  id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_url_groups_tenant_name ON url_groups (tenant_id, name);

CREATE TABLE IF NOT EXISTS url_group_endpoints (
  id TEXT PRIMARY KEY NOT NULL,
  group_id TEXT NOT NULL REFERENCES url_groups (id) ON DELETE CASCADE,
  name TEXT,
  url TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_url_group_endpoints_url ON url_group_endpoints (group_id, url);

CREATE TABLE IF NOT EXISTS workflow_steps (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  tenant_id TEXT NOT NULL,
  workflow_run_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  body BLOB,
  call_type TEXT NOT NULL DEFAULT 'step',
  headers TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE INDEX IF NOT EXISTS idx_wf_steps_run ON workflow_steps (tenant_id, workflow_run_id, id);

CREATE TABLE IF NOT EXISTS workflow_waiters (
  id TEXT PRIMARY KEY NOT NULL,
  tenant_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  workflow_run_id TEXT,
  waiter_id TEXT NOT NULL,
  resume_url TEXT NOT NULL,
  resume_headers TEXT NOT NULL DEFAULT '{}',
  step_meta TEXT NOT NULL DEFAULT '{}',
  timeout_url TEXT,
  timeout_headers TEXT,
  timeout_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000)
);
CREATE INDEX IF NOT EXISTS idx_waiters_event ON workflow_waiters (tenant_id, event_id);
CREATE INDEX IF NOT EXISTS idx_waiters_run ON workflow_waiters (tenant_id, workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_waiter_timeout_tenant ON workflow_waiters (tenant_id, timeout_at);

CREATE TABLE IF NOT EXISTS workflow_runs (
  tenant_id TEXT NOT NULL,
  workflow_run_id TEXT NOT NULL,
  workflow_url TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RUN_STARTED',
  dlq_id TEXT,
  flow_control_key TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  completed_at INTEGER,
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
  PRIMARY KEY (tenant_id, workflow_run_id)
);
CREATE INDEX IF NOT EXISTS idx_wf_runs_list ON workflow_runs (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_wf_runs_state ON workflow_runs (tenant_id, state);
"#,
    ),
    (
        2,
        "0001_registry_init",
        r#"
-- The tenants table is part of MIGRATIONS[0] already; this keeps the split of
-- data vs registry migrations and remains a no-op bookkeeping entry.
SELECT 1;
"#,
    ),
    (
        3,
        "0002_workflow_step_dedupe",
        r#"
-- Workflow step republishes are idempotent per message id; wait/resolution markers
-- carry message_id = '' and are exempt (partial unique index below).
DELETE FROM workflow_steps
 WHERE message_id <> ''
   AND id NOT IN (
     SELECT MIN(id) FROM workflow_steps
      WHERE message_id <> ''
      GROUP BY tenant_id, workflow_run_id, message_id
   );
CREATE UNIQUE INDEX IF NOT EXISTS idx_wf_steps_msg
  ON workflow_steps (tenant_id, workflow_run_id, message_id)
  WHERE message_id <> '';
"#,
    ),
    (
        4,
        "0003_messages_fifo_index",
        r#"
-- See db::MIGRATIONS[0]: the claim's ORDER BY needs an index that already carries
-- (not_before, created_at, id) after the equality columns, otherwise SQLite sorts every
-- due row. Existing databases get the same index here.
CREATE INDEX IF NOT EXISTS idx_messages_fifo
  ON messages (tenant_id, state, not_before, created_at, id)
  WHERE state = 'pending';
"#,
    ),
];

pub fn apply_migrations(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL)",
        [],
    )?;
    for (version, name, sql) in MIGRATIONS {
        let already: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM __migrations WHERE version = ?1",
                [*version],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if already > 0 {
            continue;
        }
        let tx_guard = conn.transaction()?;
        tx_guard.execute_batch(sql)?;
        tx_guard.execute(
            "INSERT INTO __migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![version, name, crate::time::now_ms()],
        )?;
        tx_guard.commit()?;
    }
    // Schema may have changed: drop cached plans so the next prepare_cached sees the new
    // schema instead of a statement compiled against the old one.
    conn.flush_prepared_statement_cache();
    Ok(())
}

pub fn open_db(path: &str) -> Db {
    let conn = if path == ":memory:" {
        Connection::open_in_memory().expect("open in-memory sqlite")
    } else {
        Connection::open(path).expect("open sqlite file")
    };
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;").unwrap();
    // 16 is rusqlite's default. The hot path alone touches well over that many distinct
    // statements (claim scans, inserts, fencing updates, events, retention, HTTP reads),
    // so a default LRU would thrash on every drain. 64 is a bounded, measured-for-purpose
    // capacity: it holds the whole working set without growing with request count.
    conn.set_prepared_statement_cache_capacity(PREPARED_STATEMENT_CACHE_CAPACITY);
    Db {
        conn: Arc::new(Mutex::new(conn)),
    }
}

/// Boot the database per config: optional migrations + the single-tenant bootstrap
/// (`ensureBootstrapTenant`): upsert the default tenant from QSTASH_TOKEN; signing keys
/// come from env only on FIRST creation.
pub fn bootstrap(db: &Db, config: &Config) -> Result<(), String> {
    {
        let mut conn = db.conn.lock().unwrap();
        if config.run_migrations_on_start {
            apply_migrations(&mut conn).map_err(|e| format!("migration failed: {}", e))?;
        } else {
            // `lease_messages` pins `idx_messages_fifo` with INDEXED BY, so a database that
            // predates that index would fail every claim instead of merely running slow.
            // RUN_MIGRATIONS_ON_START=false is a supported setting, so the index is made
            // idempotently here too rather than assumed to exist.
            let has_messages: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='messages'",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if has_messages > 0 {
                conn.execute_batch(
                    r#"CREATE INDEX IF NOT EXISTS idx_messages_fifo
                         ON messages (tenant_id, state, not_before, created_at, id)
                        WHERE state = 'pending';"#,
                )
                .map_err(|e| format!("ensure claim index failed: {}", e))?;
            }
        }
    }
    let Some(token) = &config.qstash_token else {
        tracing::warn!("QSTASH_TOKEN unset: no bootstrap tenant (all /v2 calls will 401)");
        return Ok(());
    };
    let token_hash = ids::hash_token(token);
    let conn = db.conn.lock().unwrap();
    let existing: Option<(String, String, String, String)> = conn
        .query_row(
            "SELECT id, name, signing_key_current, signing_key_next FROM tenants WHERE token_hash = ?1",
            [&token_hash],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .ok();
    if let Some((id, name, _, _)) = existing {
        tracing::info!(tenant = %id, name = %name, "bootstrap tenant exists (signing keys unchanged)");
        return Ok(());
    }
    let current = config
        .qstash_current_signing_key
        .clone()
        .unwrap_or_else(ids::new_signing_key);
    let next = config
        .qstash_next_signing_key
        .clone()
        .unwrap_or_else(ids::new_signing_key);
    let now = crate::time::now_ms();
    conn.execute(
        "INSERT INTO tenants (id, name, token_hash, external_id, signing_key_current, signing_key_next, created_at, updated_at) VALUES (?1,'default',?2,NULL,?3,?4,?5,?5)",
        rusqlite::params![ids::new_uuid(), token_hash, current, next, now],
    )
    .map_err(|e| format!("bootstrap tenant insert failed: {}", e))?;
    tracing::info!("bootstrap tenant created from QSTASH_TOKEN");
    Ok(())
}
