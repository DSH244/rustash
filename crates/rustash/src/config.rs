//! Engine and standalone configuration. Names follow the QStash/Upstash environment
//! variables so an existing deployment keeps working unchanged.

use std::env;

/// Default for `MAX_HTTP_TIMEOUT_MS`: QStash documents a 2-hour Max HTTP Response Duration
/// on pay-as-you-go (15 minutes on Free, 6 hours on Fixed 1M, 12 hours on Fixed 10M).
/// Shipping the pay-as-you-go figure means a step that QStash would have let finish is not
/// aborted and retried mid-flight here.
pub const DEFAULT_MAX_HTTP_TIMEOUT_MS: i64 = 2 * 60 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct Config {
    pub default_max_retries: i64,
    pub max_retries: i64,
    pub max_http_timeout_ms: i64,
    pub lease_ms: i64,
    /// How often an in-flight delivery's lease is pushed forward. Only used when
    /// `LEASE_MS < MAX_HTTP_TIMEOUT_MS`, i.e. when a delivery is allowed to outlive its
    /// initial lease. 0 disables renewal, which then requires `LEASE_MS > MAX_HTTP_TIMEOUT_MS`.
    pub lease_renew_interval_ms: i64,
    pub dispatch_batch_limit: i64,
    pub dispatch_tick_ms: i64,
    pub scheduler_tick_ms: i64,
    pub retention_tick_ms: i64,
    pub tenant_parallelism_max: i64,
    pub delivery_slots: i64,
    /// Keep-alive sockets kept per destination authority; 0 disables reuse.
    pub delivery_pool_max_idle: i64,
    /// How long an idle socket may sit in the pool before it is dropped.
    pub delivery_pool_idle_ms: i64,
    pub max_body_bytes: usize,
    /// Cap for a response body kept from an endpoint. QStash documents that an
    /// over-limit response body is trimmed for logging/DLQ; this is that bound.
    pub max_response_body_bytes: usize,
    /// Largest message body copied into a `message_events` row. Larger bodies are omitted
    /// from the event row; the message row and the DLQ snapshot still carry the full body.
    pub event_body_max_bytes: usize,
    pub log_retention_days: i64,
    pub dlq_retention_days: i64,
    pub dedupe_window_secs: i64,
    pub workflow_run_stuck_hours: i64,
    pub workflow_run_retention_days: i64,
    pub allow_private_destinations: bool,
    pub destination_allow_cidrs: String,
    pub destination_block_cidrs: String,
    pub region: String,
    pub database_url: String,
    pub port: u16,
    pub qstash_token: Option<String>,
    pub qstash_current_signing_key: Option<String>,
    pub qstash_next_signing_key: Option<String>,
    pub run_migrations_on_start: bool,
    pub admin_token: Option<String>,
    pub log_level: String,
}

impl Config {
    /// SQLite file path derived from `DATABASE_URL`. Accepts a bare path,
    /// `sqlite:<path>`, `sqlite://<path>` or `file:<path>` forms. For personal use a
    /// server URL is meaningless; the port keeps the env-var NAME for parity.
    pub fn sqlite_path(&self) -> String {
        let u = self.database_url.trim();
        let mut p = u;
        for prefix in ["sqlite://", "sqlite:", "file://", "file:"] {
            if let Some(rest) = p.strip_prefix(prefix) {
                p = rest;
                break;
            }
        }
        if p.is_empty() {
            ":memory:".to_string()
        } else {
            p.to_string()
        }
    }
}

fn env_or_default(key: &str, default: i64) -> i64 {
    match env::var(key) {
        Ok(v) => v.trim().parse::<i64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn env_int(keys: &[&str], default: i64) -> i64 {
    for k in keys {
        if let Ok(v) = env::var(k) {
            if !v.trim().is_empty() {
                return v.trim().parse::<i64>().unwrap_or(default);
            }
        }
    }
    default
}

fn env_str(keys: &[&str], default: &str) -> String {
    for k in keys {
        if let Ok(v) = env::var(k) {
            return v;
        }
    }
    default.to_string()
}

fn env_opt_str(keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Ok(v) = env::var(k) {
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn env_bool(keys: &[&str], default: bool) -> bool {
    for k in keys {
        if let Ok(v) = env::var(k) {
            let l = v.trim().to_ascii_lowercase();
            if l == "true" {
                return true;
            }
            if l == "false" {
                return false;
            }
        }
    }
    default
}

/// Load the standalone config from the environment. `DEFAULT_RETRY` is accepted as an
/// accepted alias of `DEFAULT_MAX_RETRIES`.
pub fn load_from_env() -> Config {
    let cfg = Config {
        default_max_retries: env_int(&["DEFAULT_MAX_RETRIES", "DEFAULT_RETRY"], 3),
        max_retries: env_int(&["MAX_RETRIES"], 100),
        max_http_timeout_ms: env_int(&["MAX_HTTP_TIMEOUT_MS"], DEFAULT_MAX_HTTP_TIMEOUT_MS),
        lease_ms: env_int(&["LEASE_MS"], 960_000),
        // -1 = derive from LEASE_MS / MAX_HTTP_TIMEOUT_MS (see `resolve_lease_renewal`).
        lease_renew_interval_ms: env_int(&["LEASE_RENEW_INTERVAL_MS"], -1),
        dispatch_batch_limit: env_int(&["DISPATCH_BATCH_LIMIT"], 32),
        dispatch_tick_ms: env_int(&["DISPATCH_TICK_MS"], 500),
        scheduler_tick_ms: env_int(&["SCHEDULER_TICK_MS"], 1_000),
        retention_tick_ms: env_int(&["RETENTION_TICK_MS"], 60_000),
        tenant_parallelism_max: env_int(&["TENANT_PARALLELISM_MAX"], 1024),
        delivery_slots: env_int(&["DELIVERY_SLOTS"], 256),
        delivery_pool_max_idle: env_int(&["DELIVERY_POOL_MAX_IDLE"], 64),
        delivery_pool_idle_ms: env_int(&["DELIVERY_POOL_IDLE_MS"], 4_000),
        max_body_bytes: env_or_default("MAX_BODY_BYTES", 10 * 1024 * 1024).max(1) as usize,
        max_response_body_bytes: env_or_default("MAX_RESPONSE_BODY_BYTES", 1024 * 1024).max(0)
            as usize,
        event_body_max_bytes: env_or_default("EVENT_BODY_MAX_BYTES", 32 * 1024).max(0) as usize,
        log_retention_days: env_int(&["LOG_RETENTION_DAYS"], 14),
        dlq_retention_days: env_int(&["DLQ_RETENTION_DAYS"], 7),
        dedupe_window_secs: env_int(&["DEDUPE_WINDOW_SECS"], 600),
        workflow_run_stuck_hours: env_int(&["WORKFLOW_RUN_STUCK_HOURS"], 24),
        workflow_run_retention_days: env_int(&["WORKFLOW_RUN_RETENTION_DAYS"], 7),
        allow_private_destinations: env_bool(&["ALLOW_PRIVATE_DESTINATIONS"], false),
        destination_allow_cidrs: env_str(&["DESTINATION_ALLOW_CIDRS"], ""),
        destination_block_cidrs: env_str(&["DESTINATION_BLOCK_CIDRS"], ""),
        region: env_str(&["REGION"], "us-east-1"),
        database_url: env_str(&["DATABASE_URL"], ""),
        port: env_int(&["PORT"], 8080) as u16,
        qstash_token: env_opt_str(&["QSTASH_TOKEN"]),
        qstash_current_signing_key: env_opt_str(&["QSTASH_CURRENT_SIGNING_KEY"]),
        qstash_next_signing_key: env_opt_str(&["QSTASH_NEXT_SIGNING_KEY"]),
        run_migrations_on_start: env_bool(&["RUN_MIGRATIONS_ON_START"], true),
        admin_token: env_opt_str(&["ADMIN_TOKEN"]),
        log_level: env_str(&["LOG_LEVEL"], "info"),
    };
    cfg.validate();
    cfg
}

impl Config {
    /// A delivery longer than `MAX_HTTP_TIMEOUT_MS` must not lose its lease while it is
    /// still in flight, or at-least-once degrades to at-least-twice: the reclaim bumps
    /// `retried`, the live worker's fenced writes stop matching, and the endpoint runs the
    /// same step again.
    ///
    /// `LEASE_MS` is therefore either (a) longer than any possible delivery — renewal off,
    /// which is what a deployment sized for short steps wants — or (b) shorter, in which
    /// case the worker pushes its own lease forward every `LEASE_RENEW_INTERVAL_MS` while
    /// the request is running. Only (b) scales to QStash's documented 2-hour (PAYG) or
    /// 6-hour (Fixed 1M) response budget without a 6–7 hour lease on every message.
    ///
    /// Returns `None` when renewal is not needed (`lease_ms > max_http_timeout_ms`) or is
    /// explicitly disabled (`LEASE_RENEW_INTERVAL_MS=0`).
    pub fn resolve_lease_renewal(&self) -> Result<Option<i64>, String> {
        if self.lease_ms > self.max_http_timeout_ms {
            return Ok(None); // every delivery ends before its lease could lapse
        }
        if self.lease_renew_interval_ms < 0 {
            // Derive: renew at a third of the lease, so two consecutive renewals may fail
            // before the lease is actually lost.
            let derived = (self.lease_ms / 3).max(100);
            return Ok(Some(derived.min(self.max_http_timeout_ms.max(100))));
        }
        if self.lease_renew_interval_ms == 0 {
            return Err(format!(
                "LEASE_MS ({}) must exceed MAX_HTTP_TIMEOUT_MS ({}) unless lease renewal is \
                 enabled, otherwise a delivery outliving its lease is retried while still in \
                 flight — set LEASE_RENEW_INTERVAL_MS>0 or raise LEASE_MS",
                self.lease_ms, self.max_http_timeout_ms
            ));
        }
        Ok(Some(self.lease_renew_interval_ms))
    }

    /// The one startup invariant, in its full form: either the lease outlives every
    /// delivery, or renewal is enabled to keep it alive.
    pub fn validate(&self) {
        if let Err(e) = self.resolve_lease_renewal() {
            panic!("{e}");
        }
    }
}

#[cfg(test)]
impl Config {
    /// Test/default construction, no env involvement.
    pub fn for_test() -> Config {
        Config {
            default_max_retries: 3,
            max_retries: 100,
            max_http_timeout_ms: 900_000,
            lease_ms: 960_000,
            lease_renew_interval_ms: -1,
            dispatch_batch_limit: 32,
            dispatch_tick_ms: 500,
            scheduler_tick_ms: 1_000,
            retention_tick_ms: 60_000,
            tenant_parallelism_max: 1024,
            delivery_slots: 256,
            delivery_pool_max_idle: 64,
            delivery_pool_idle_ms: 4_000,
            max_body_bytes: 10 * 1024 * 1024,
            max_response_body_bytes: 1024 * 1024,
            event_body_max_bytes: 32 * 1024,
            log_retention_days: 14,
            dlq_retention_days: 7,
            dedupe_window_secs: 600,
            workflow_run_stuck_hours: 24,
            workflow_run_retention_days: 7,
            allow_private_destinations: false,
            destination_allow_cidrs: String::new(),
            destination_block_cidrs: String::new(),
            region: "us-east-1".into(),
            database_url: ":memory:".into(),
            port: 8080,
            qstash_token: None,
            qstash_current_signing_key: None,
            qstash_next_signing_key: None,
            run_migrations_on_start: true,
            admin_token: None,
            log_level: "info".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_panics_when_lease_is_short_and_renewal_disabled() {
        let cfg = Config {
            max_http_timeout_ms: 200,
            lease_ms: 100,
            lease_renew_interval_ms: 0,
            ..base()
        };
        let r = std::panic::catch_unwind(|| cfg.validate());
        assert!(
            r.is_err(),
            "a short lease with renewal off must refuse to start"
        );
    }

    #[test]
    fn long_lease_needs_no_renewal() {
        let cfg = Config {
            max_http_timeout_ms: 900_000,
            lease_ms: 960_000,
            lease_renew_interval_ms: -1,
            ..base()
        };
        assert_eq!(cfg.resolve_lease_renewal().unwrap(), None);
    }

    #[test]
    fn short_lease_derives_a_renewal_interval() {
        // A 2-hour QStash response budget under a 16-minute lease: renewal must be on.
        let cfg = Config {
            max_http_timeout_ms: 7_200_000,
            lease_ms: 960_000,
            lease_renew_interval_ms: -1,
            ..base()
        };
        let derived = cfg.resolve_lease_renewal().unwrap().unwrap();
        assert_eq!(
            derived, 320_000,
            "a third of LEASE_MS, capped by the timeout"
        );
        assert!(
            derived < cfg.lease_ms,
            "must renew before the lease can lapse"
        );
    }

    #[test]
    fn explicit_renewal_interval_is_honoured() {
        let cfg = Config {
            max_http_timeout_ms: 7_200_000,
            lease_ms: 960_000,
            lease_renew_interval_ms: 60_000,
            ..base()
        };
        assert_eq!(cfg.resolve_lease_renewal().unwrap(), Some(60_000));
    }

    #[test]
    fn default_timeout_matches_qstash_pay_as_you_go() {
        // QStash documents a 2-hour Max HTTP Response Duration on pay-as-you-go; the
        // shipped default must not be shorter, or a long step is retried mid-flight.
        assert_eq!(DEFAULT_MAX_HTTP_TIMEOUT_MS, 2 * 60 * 60 * 1000);
        let cfg = load_from_env();
        assert_eq!(cfg.max_http_timeout_ms, DEFAULT_MAX_HTTP_TIMEOUT_MS);
        // The shipped lease is shorter than that, so renewal is what keeps it valid.
        assert!(cfg.lease_ms < cfg.max_http_timeout_ms);
        assert!(cfg.resolve_lease_renewal().unwrap().is_some());
    }

    fn base() -> Config {
        Config {
            default_max_retries: 3,
            max_retries: 100,
            max_http_timeout_ms: 900_000,
            lease_ms: 960_000,
            lease_renew_interval_ms: -1,
            dispatch_batch_limit: 32,
            dispatch_tick_ms: 500,
            scheduler_tick_ms: 1_000,
            retention_tick_ms: 60_000,
            tenant_parallelism_max: 1024,
            delivery_slots: 256,
            delivery_pool_max_idle: 64,
            delivery_pool_idle_ms: 4_000,
            max_body_bytes: 10 * 1024 * 1024,
            max_response_body_bytes: 1024 * 1024,
            event_body_max_bytes: 32 * 1024,
            log_retention_days: 14,
            dlq_retention_days: 7,
            dedupe_window_secs: 600,
            workflow_run_stuck_hours: 24,
            workflow_run_retention_days: 7,
            allow_private_destinations: false,
            destination_allow_cidrs: String::new(),
            destination_block_cidrs: String::new(),
            region: "us-east-1".into(),
            database_url: ":memory:".into(),
            port: 8080,
            qstash_token: None,
            qstash_current_signing_key: None,
            qstash_next_signing_key: None,
            run_migrations_on_start: true,
            admin_token: None,
            log_level: "info".into(),
        }
    }
}
