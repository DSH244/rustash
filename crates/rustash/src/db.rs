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
/// come from env only on FIRST creation. Afterwards, apply schedule seeding if configured.
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
        if config.seed_preset.is_some() || config.seed_schedules_json.is_some() {
            tracing::warn!("schedule seed skipped: no tenant (QSTASH_TOKEN unset)");
        }
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
    let tenant_id = match existing {
        Some((id, name, _, _)) => {
            tracing::info!(tenant = %id, name = %name, "bootstrap tenant exists (signing keys unchanged)");
            id
        }
        None => {
            let current = config
                .qstash_current_signing_key
                .clone()
                .unwrap_or_else(ids::new_signing_key);
            let next = config
                .qstash_next_signing_key
                .clone()
                .unwrap_or_else(ids::new_signing_key);
            let now = crate::time::now_ms();
            let id = ids::new_uuid();
            conn.execute(
                "INSERT INTO tenants (id, name, token_hash, external_id, signing_key_current, signing_key_next, created_at, updated_at) VALUES (?1,'default',?2,NULL,?3,?4,?5,?5)",
                rusqlite::params![id, token_hash, current, next, now],
            )
            .map_err(|e| format!("bootstrap tenant insert failed: {}", e))?;
            tracing::info!("bootstrap tenant created from QSTASH_TOKEN");
            id
        }
    };
    seed_schedules(&conn, config, &tenant_id)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Schedule seeding
// ---------------------------------------------------------------------------

/// The `lobehub-extras` preset: the workflow schedules LobeHub's own
/// `startServer.js` deliberately does NOT register (upstash-quota reasons) but whose
/// handler docs say they expect a QStash Schedule. The 3 schedules LobeHub registers
/// itself on every boot (`lobe-inbound-webhook-sweep`, `lobe-task-schedule-dispatch`,
/// `lobe-goal-sweep`) are intentionally absent — seeding them would only compete with
/// LobeHub's own upsert. Cadences mirror hardy-one/lobehub HELLOSTASH_EXTRA_SCHEDULES:
/// safety-net defaults, hourly granularity is enough for a personal instance.
const LOBEHUB_EXTRAS_PRESET: &[(&str, &str, &str)] = &[
    (
        "lobe-task-scheduled-topic-dispatch",
        "*/10 * * * *",
        "/api/workflows/task/scheduled-topic-dispatch",
    ),
    (
        "lobe-task-watchdog",
        "15 * * * *",
        "/api/workflows/task/watchdog",
    ),
    (
        "lobe-verify-sweep",
        "30 * * * *",
        "/api/workflows/verify/sweep",
    ),
    (
        "lobe-trash-purge",
        "45 * * * *",
        "/api/workflows/trash/purge",
    ),
    (
        "lobe-agent-signal-nightly-self-review",
        "0 * * * *",
        "/api/workflows/agent-signal/cron-hourly-nightly-self-review",
    ),
    (
        "lobe-memory-hourly-analysis",
        "5 * * * *",
        "/api/workflows/memory-user-memory/call-cron-hourly-analysis",
    ),
    (
        "lobe-widget-tick",
        "* * * * *",
        "/api/workflows/widget/tick",
    ),
];

/// One schedule to seed. JSON entries carry a full destination; preset entries get one
/// built from `RUSTASH_SEED_BASE_URL`.
struct SeedSpec {
    id: String,
    cron: String,
    destination: String,
    method: String,
    headers: Option<String>,
    body: Option<Vec<u8>>,
    paused: bool,
}

/// Seed schedules at startup. Semantics, deliberately narrow:
///
/// - INSERT-only (`ON CONFLICT DO NOTHING`): a seed NEVER overwrites an existing
///   schedule. LobeHub upserts its own schedules by the same `Upstash-Schedule-Id` on
///   every boot, so both writers converge on one row and LobeHub always wins for the
///   schedules it declares. Seeding is only the safety net for the ones LobeHub does
///   not register.
/// - Whole-config errors (`RUSTASH_SEED_SCHEDULES` not JSON, unknown preset name, an
///   entry missing a required field) fail startup: a misconfigured seed must not look
///   like it took effect.
/// - Value-level errors in a well-formed entry (bad cron, non-http destination,
///   disallowed method) warn and skip that entry.
///
/// Known drift: if LobeHub later drops a workflow that a preset entry still targets,
/// a fresh database grows a schedule that delivers into a 404. That is visible in the
/// delivery log and DLQ, and removable via the schedules API; presets are updated with
/// rustash releases.
pub fn seed_schedules(conn: &Connection, config: &Config, tenant_id: &str) -> Result<(), String> {
    let mut specs: Vec<SeedSpec> = Vec::new();

    if let Some(preset) = &config.seed_preset {
        match preset.as_str() {
            "lobehub-extras" => {
                let Some(base) = &config.seed_base_url else {
                    tracing::warn!(
                        "seed preset {preset} skipped: RUSTASH_SEED_BASE_URL not set (e.g. http://lobe:3210)"
                    );
                    return Ok(());
                };
                let base = base.trim_end_matches('/');
                for (id, cron, path) in LOBEHUB_EXTRAS_PRESET {
                    specs.push(SeedSpec {
                        id: (*id).to_string(),
                        cron: (*cron).to_string(),
                        destination: format!("{base}{path}"),
                        method: "POST".into(),
                        headers: None,
                        body: None,
                        paused: false,
                    });
                }
            }
            other => return Err(format!("unknown RUSTASH_SEED_PRESET \"{other}\"")),
        }
    }

    if let Some(json) = &config.seed_schedules_json {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(json)
            .map_err(|e| format!("RUSTASH_SEED_SCHEDULES is not a JSON array: {e}"))?;
        for (i, v) in parsed.iter().enumerate() {
            let id = v
                .get("id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| format!("RUSTASH_SEED_SCHEDULES[{i}].id missing"))?
                .to_string();
            let cron = v
                .get("cron")
                .and_then(|x| x.as_str())
                .ok_or_else(|| format!("RUSTASH_SEED_SCHEDULES[{i}].cron missing"))?
                .to_string();
            let destination = v
                .get("destination")
                .and_then(|x| x.as_str())
                .ok_or_else(|| format!("RUSTASH_SEED_SCHEDULES[{i}].destination missing"))?
                .to_string();
            let method = v
                .get("method")
                .and_then(|x| x.as_str())
                .unwrap_or("POST")
                .to_ascii_uppercase();
            let headers = v
                .get("headers")
                .and_then(|x| x.as_object())
                .map(|m| serde_json::to_string(&m).expect("serialize a just-parsed Map"));
            let body = v
                .get("body")
                .and_then(|x| x.as_str())
                .map(|s| s.as_bytes().to_vec());
            let paused = v.get("paused").and_then(|x| x.as_bool()).unwrap_or(false);
            specs.push(SeedSpec {
                id,
                cron,
                destination,
                method,
                headers,
                body,
                paused,
            });
        }
    }

    if specs.is_empty() {
        return Ok(());
    }

    for spec in specs {
        // Per-entry validation: a bad entry skips itself instead of failing startup.
        let result = validate_seed_spec(&spec)
            .and_then(|()| insert_seed_spec(conn, tenant_id, &spec, config.default_max_retries));
        match result {
            Ok(inserted) if inserted => tracing::info!(
                id = %spec.id,
                cron = %spec.cron,
                dest = %spec.destination,
                "seeded schedule"
            ),
            Ok(_) => {} // already exists; seed semantics are insert-only, stay silent
            Err(e) => tracing::warn!(id = %spec.id, error = %e, "seed entry skipped"),
        }
    }
    Ok(())
}

fn validate_seed_spec(spec: &SeedSpec) -> Result<(), String> {
    if spec.id.is_empty() {
        return Err("id is empty".into());
    }
    crate::cron::parse_cron(&spec.cron)
        .map_err(|e| format!("bad cron \"{}\": {}", spec.cron, e))?;
    let lower = spec.destination.to_ascii_lowercase();
    if !lower.starts_with("http://") && !lower.starts_with("https://") {
        return Err(format!(
            "destination must be an http(s) URL: {}",
            spec.destination
        ));
    }
    if !matches!(
        spec.method.as_str(),
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
    ) {
        return Err(format!("method {} not allowed", spec.method));
    }
    Ok(())
}

/// INSERT-only: `ON CONFLICT DO NOTHING`. Never UPDATEs, never deletes.
fn insert_seed_spec(
    conn: &Connection,
    tenant_id: &str,
    spec: &SeedSpec,
    default_retries: i64,
) -> Result<bool, String> {
    // Parse ONCE and store the PARSED parts, exactly like the API path
    // (http/schedules.rs writes cron.expr + cron.tz separately). Storing the raw
    // string instead would make `CRON_TZ=... 0 3 * * *` fire once correctly (next_run_at
    // is computed from the parsed value here) and then stall forever: fire_schedule
    // re-parses the STORED cron_expr, which rejects the raw prefix.
    let cron = crate::cron::parse_cron(&spec.cron).map_err(|e| format!("bad cron: {e}"))?;
    let now = crate::time::now_ms();
    let next = crate::schedule::initial_next_run(&cron.expr, cron.tz.as_deref(), now)
        .map_err(|e| format!("bad next run: {e}"))?;
    let headers = spec.headers.clone().unwrap_or_else(|| "{}".into());
    let n = conn
        .execute(
            r#"INSERT INTO schedules
                 (id, tenant_id, destination, cron_expr, tz, method, headers, body, retries,
                  labels, next_run_at, created_at, updated_at, is_paused)
               VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'[]',?10,?11,?11,?12)
               ON CONFLICT(id) DO NOTHING"#,
            rusqlite::params![
                spec.id,
                tenant_id,
                spec.destination,
                cron.expr,
                cron.tz,
                spec.method,
                headers,
                spec.body,
                default_retries,
                next,
                now,
                spec.paused as i64,
            ],
        )
        .map_err(|e| e.to_string())?;
    Ok(n > 0)
}

#[cfg(test)]
mod seed_tests {
    #[test]
    fn insert_into_fresh_row_returns_true() {
        let config = config_with(None, None, None);
        let (db, tenant) = bootstrapped_db(&config);
        let spec = SeedSpec {
            id: "probe".into(),
            cron: "0 3 * * *".into(),
            destination: "http://app:3000/x".into(),
            method: "POST".into(),
            headers: None,
            body: None,
            paused: false,
        };
        let inserted =
            insert_seed_spec(&db.conn.lock().unwrap(), &tenant, &spec, 3).expect("insert");
        assert!(inserted);
    }

    /// Regression for the High finding: a `CRON_TZ=` entry must store the PARSED cron
    /// expression and its timezone in separate columns — the raw string in cron_expr
    /// would make fire_schedule's re-parse fail after the first fire, stalling the
    /// schedule forever with a misleading "seeded" info log.
    /// Seed must write a REAL next_run_at: it is the field fire_schedule advances from.
    /// 'a' is `0 3 * * *` (03:00 daily); the stored value must equal the next occurrence
    /// computed by the same routine the API path uses, and must lie in the future.
    #[test]
    fn seeded_next_run_at_matches_the_api_path_computation() {
        let json = r#"[{"id":"a","cron":"0 3 * * *","destination":"http://app:3000/x"}]"#;
        let config = config_with(None, None, Some(json));
        let (db, _) = bootstrapped_db(&config);
        let (stored, created_at): (i64, i64) = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT next_run_at, created_at FROM schedules WHERE id='a'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        // The seed computes next from the row's created_at instant; recompute the same
        // way and require exact agreement (not just "in the future").
        let expected = crate::schedule::initial_next_run("0 3 * * *", None, created_at)
            .expect("API-path computation must succeed");
        assert_eq!(stored, expected);
        assert!(stored > created_at, "next run must be after creation");
    }

    /// TZ-qualified crons compute the next occurrence in that timezone: 03:00 Shanghai
    /// is a specific UTC instant, and the stored next_run_at must be exactly that.
    #[test]
    fn seeded_next_run_at_respects_cron_tz() {
        let json = r#"[{"id":"tz","cron":"CRON_TZ=Asia/Shanghai 0 3 * * *","destination":"http://app:3000/x"}]"#;
        let config = config_with(None, None, Some(json));
        let (db, _) = bootstrapped_db(&config);
        let (stored, created_at): (i64, i64) = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT next_run_at, created_at FROM schedules WHERE id='tz'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let expected =
            crate::schedule::initial_next_run("0 3 * * *", Some("Asia/Shanghai"), created_at)
                .expect("tz-qualified computation must succeed");
        assert_eq!(stored, expected);
        // Sanity: it must differ from the naive (UTC) computation, proving the tz took
        // effect rather than being dropped (Shanghai is UTC+8, so the instants differ).
        let naive = crate::schedule::initial_next_run("0 3 * * *", None, created_at)
            .expect("naive computation must succeed");
        assert_ne!(stored, naive, "tz must shift the computed instant");
    }

    /// QSTASH_TOKEN unset + seed env configured: bootstrap must skip seeding entirely
    /// (no tenant to own the rows) and still succeed — the process runs but /v2 is 401.
    #[test]
    fn seed_without_token_skips_and_leaves_no_schedules() {
        let mut config = config_with(Some("lobehub-extras"), Some("http://lobe:3210"), None);
        config.qstash_token = None;
        let db = open_db(":memory:");
        bootstrap(&db, &config).expect("bootstrap must not fail without a token");
        assert_eq!(schedule_count(&db), 0);
        let tenants: i64 = db
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM tenants", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tenants, 0);
    }

    #[test]
    fn cron_tz_entry_stores_parsed_expr_and_tz() {
        let json = r#"[{"id":"tz-job","cron":"CRON_TZ=Asia/Shanghai 0 3 * * *","destination":"http://app:3000/x"}]"#;
        let config = config_with(None, None, Some(json));
        let (db, _) = bootstrapped_db(&config);
        let (expr, tz): (String, Option<String>) = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT cron_expr, tz FROM schedules WHERE id='tz-job'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(expr, "0 3 * * *");
        assert_eq!(tz.as_deref(), Some("Asia/Shanghai"));
        // The stored parts must re-parse and advance, i.e. fire_schedule can progress.
        let now = crate::time::now_ms();
        let next = crate::schedule::initial_next_run(&expr, tz.as_deref(), now);
        assert!(next.is_ok(), "stored cron must re-parse: {next:?}");
    }

    use super::*;

    fn config_with(preset: Option<&str>, base: Option<&str>, json: Option<&str>) -> Config {
        let mut c = Config::for_test();
        c.qstash_token = Some("st_test".into());
        c.seed_preset = preset.map(String::from);
        c.seed_base_url = base.map(String::from);
        c.seed_schedules_json = json.map(String::from);
        c
    }

    fn bootstrapped_db(config: &Config) -> (Db, String) {
        let db = open_db(":memory:");
        bootstrap(&db, config).expect("bootstrap");
        let tenant_id: String = db
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT id FROM tenants LIMIT 1", [], |r| r.get(0))
            .unwrap();
        (db, tenant_id)
    }

    fn schedule_count(db: &Db) -> i64 {
        db.conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM schedules", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn preset_seeds_all_seven_entries() {
        let config = config_with(Some("lobehub-extras"), Some("http://lobe:3210"), None);
        let (db, _) = bootstrapped_db(&config);
        assert_eq!(schedule_count(&db), 7);
        let dest: String = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT destination FROM schedules WHERE id='lobe-widget-tick'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dest, "http://lobe:3210/api/workflows/widget/tick");
    }

    #[test]
    fn preset_is_idempotent_across_reboots() {
        let config = config_with(Some("lobehub-extras"), Some("http://lobe:3210"), None);
        let (db, tenant) = bootstrapped_db(&config);
        assert_eq!(schedule_count(&db), 7);
        // Second boot on the SAME database: insert-only semantics must be a no-op.
        seed_schedules(&db.conn.lock().unwrap(), &config, &tenant).expect("re-seed");
        assert_eq!(schedule_count(&db), 7);
    }

    #[test]
    fn seed_never_overwrites_an_existing_schedule() {
        let config = config_with(Some("lobehub-extras"), Some("http://lobe:3210"), None);
        let (db, tenant) = bootstrapped_db(&config);
        // Simulate LobeHub having registered a DIFFERENT destination under the same id.
        db.conn.lock().unwrap().execute(
            "UPDATE schedules SET destination='http://real-lobe:9999/api/workflows/task/watchdog' WHERE id='lobe-task-watchdog'",
            [],
        )
        .unwrap();
        seed_schedules(&db.conn.lock().unwrap(), &config, &tenant).expect("re-seed");
        let dest: String = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT destination FROM schedules WHERE id='lobe-task-watchdog'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dest, "http://real-lobe:9999/api/workflows/task/watchdog");
    }

    /// End-to-end convergence: LobeHub's upsert semantics (`ON CONFLICT DO UPDATE`
    /// — the same conflict target as create_schedule's statement, with an update set
    /// covering the asserted columns; executed here directly because create_schedule
    /// lives behind HTTP) must win over a seed row, and a later re-seed must NOT
    /// revert it.
    #[test]
    fn lobe_hub_upsert_wins_and_reseed_does_not_revert() {
        let config = config_with(Some("lobehub-extras"), Some("http://lobe:3210"), None);
        let (db, tenant) = bootstrapped_db(&config);
        // 1. LobeHub upserts one of the preset ids through its own ON CONFLICT DO UPDATE.
        db.conn
            .lock()
            .unwrap()
            .execute(
                r#"INSERT INTO schedules
                     (id, tenant_id, destination, cron_expr, method, headers, body, retries,
                      labels, next_run_at, created_at, updated_at, is_paused)
                   VALUES ('lobe-task-watchdog',?1,'http://real-lobe:9999/api/workflows/task/watchdog',
                           '15 * * * *','POST','{}',NULL,3,'[]',999999,111,111,0)
                   ON CONFLICT(id) DO UPDATE SET
                     destination=excluded.destination, cron_expr=excluded.cron_expr,
                     method=excluded.method, headers=excluded.headers, body=excluded.body,
                     retries=excluded.retries, next_run_at=excluded.next_run_at,
                     updated_at=excluded.updated_at"#,
                rusqlite::params![tenant],
            )
            .unwrap();
        // 2. Rustash re-seeds (e.g. a reboot with seed env still set).
        seed_schedules(&db.conn.lock().unwrap(), &config, &tenant).expect("re-seed");
        // 3. LobeHub's values survive; the seed must not revert them.
        let (dest, cron, next): (String, String, i64) = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT destination, cron_expr, next_run_at FROM schedules WHERE id='lobe-task-watchdog'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(dest, "http://real-lobe:9999/api/workflows/task/watchdog");
        assert_eq!(cron, "15 * * * *");
        assert_eq!(next, 999999, "upserted next_run_at must not be touched");
    }

    #[test]
    fn json_entries_seed_with_defaults() {
        let json = r#"[
            {"id":"a","cron":"0 3 * * *","destination":"http://app:3000/x"},
            {"id":"b","cron":"*/5 * * * *","destination":"http://app:3000/y","method":"PUT","paused":true},
            {"id":"c","cron":"0 0 * * *","destination":"http://app:3000/z","headers":{"X-T":"1"},"body":"go"}
        ]"#;
        let config = config_with(None, None, Some(json));
        let (db, _) = bootstrapped_db(&config);
        assert_eq!(schedule_count(&db), 3);
        // 'b' sets method/paused and takes the header default.
        let (method, paused, headers): (String, i64, String) = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT method, is_paused, headers FROM schedules WHERE id='b'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(method, "PUT");
        assert_eq!(paused, 1);
        assert_eq!(headers, "{}");
        // 'c' carries headers and body.
        let (headers_c, body_c): (String, Option<Vec<u8>>) = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT headers, body FROM schedules WHERE id='c'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(
            headers_c.contains("X-T"),
            "headers should carry X-T: {headers_c}"
        );
        assert_eq!(body_c.as_deref(), Some(b"go".as_slice()));
        // Seeded rows use the deployment's default retry budget.
        let retries: i64 = db
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT retries FROM schedules WHERE id='a'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(retries, 3);
    }

    #[test]
    fn bad_json_fails_startup_not_silently_ignored() {
        let config = config_with(None, None, Some("[{bad"));
        let db = open_db(":memory:");
        assert!(bootstrap(&db, &config).is_err());
    }

    #[test]
    fn unknown_preset_fails_startup() {
        let config = config_with(Some("nope"), None, None);
        let db = open_db(":memory:");
        assert!(bootstrap(&db, &config).is_err());
    }

    #[test]
    fn preset_without_base_url_skips_with_no_entries() {
        let config = config_with(Some("lobehub-extras"), None, None);
        let (db, _) = bootstrapped_db(&config);
        assert_eq!(schedule_count(&db), 0);
    }

    #[test]
    fn bad_entry_skips_but_good_ones_land() {
        let json = r#"[
            {"id":"good","cron":"0 3 * * *","destination":"http://app:3000/x"},
            {"id":"bad-cron","cron":"25 hours","destination":"http://app:3000/y"},
            {"id":"bad-url","cron":"0 4 * * *","destination":"ftp://app:3000/z"}
        ]"#;
        let config = config_with(None, None, Some(json));
        let (db, _) = bootstrapped_db(&config);
        let ids: i64 = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM schedules WHERE id IN ('good','bad-cron','bad-url')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ids, 1);
    }
}
