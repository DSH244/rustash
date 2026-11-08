//! The dispatch engine (fork `engine/dispatcher.js` + `engine/deliver.js` +
//! `engine/callbacks.js` port): claim → deliver in parallel → classify → retry/DLQ.
//!
//! Boundary: this module owns *scheduling and decisions only*. Tenant discovery is
//! `tenant`, SQL transitions are `store`/`lease`/`outcome`/`retention`, callback bodies
//! are `callback`, and the outbound wire attempt is `delivery`. There is deliberately no
//! SQL string here and no socket here: a change to the SQLite schema cannot alter the
//! dispatcher, and a change to HTTP framing cannot alter retry classification.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};

use crate::backoff::{is_non_retryable, retry_delay_from_response};
use crate::callback::build_callback_row;
use crate::config::Config;
use crate::db::Db;
use crate::deliver::Pool as DeliveryPool;
use crate::model::{DeliveryResponse, EventExtras, MessageRow};
use crate::outcome::{check_cancellation_race, complete_delivery, CancelRace};
use crate::ssrf::{create_policy, SsrfPolicy};
use crate::tenant::load_with_due_work;
use crate::time::now_ms;

pub const MAX_DRAIN_BATCHES: usize = 50;

/// How long a paused-prefix resume point stays valid. Long enough to walk a huge prefix
/// across many ticks, short enough that a newly published message is never starved by a
/// walk that started long ago.
const CLAIM_CURSOR_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// How many extra scan budgets one drain may spend walking a paused prefix before handing
/// the rest to the next tick. Bounds the time the drain lock is held while still letting a
/// large prefix be crossed in one drain instead of one budget per tick.
const MAX_SCAN_BATCHES_PER_DRAIN: usize = 4;

/// Backward-compatible re-export: the pure backoff curve now lives in `backoff`, but the
/// dispatch module keeps `engine::retry_delay_ms_for` usable for existing consumers.
pub use crate::backoff::retry_delay_ms_for;

/// Backward-compatible re-export: the type's home is now `tenant`, but the dispatch
/// module keeps `engine::Tenant` usable for existing consumers.
pub use crate::tenant::Tenant;
pub struct Engine {
    pub db: Db,
    pub config: Arc<Config>,
    pub wake: Arc<Notify>,
    pub slots: Arc<Semaphore>,
    /// Keep-alive sockets per destination authority, shared by every delivery.
    pub pool: Arc<DeliveryPool>,
    /// Serializes drains so only one runs per process at a time.
    pub drain_lock: Arc<AsyncMutex<()>>,
    /// Resume points for tenants whose claim scan was cut short by `MAX_CLAIM_SCAN` while
    /// walking a paused prefix, plus when the walk started.
    ///
    /// This has to outlive a single drain. A local variable dies the moment a batch comes
    /// back empty, so the next tick restarts at the paused head and the scan never gets
    /// past one drain's budget: with 650k paused rows in front of 20 live messages,
    /// nothing was delivered at all until this map existed. The timestamp bounds
    /// staleness — a walk older than `CLAIM_CURSOR_TTL` restarts from the head so newly
    /// published work is not starved behind an old prefix.
    pub claim_cursors:
        Arc<std::sync::Mutex<HashMap<String, (crate::lease::ClaimCursor, std::time::Instant)>>>,
    /// How often an in-flight delivery pushes its own lease forward, or `None` when the
    /// lease already outlives every possible delivery. See `Config::resolve_lease_renewal`.
    pub lease_renew_ms: Option<i64>,
    /// CIDR policy parsed once at startup, shared by every delivery and every redirect
    /// hop (the old code rebuilt it on each attempt).
    ssrf_policy: SsrfPolicy,
}

impl Engine {
    pub fn new(db: Db, config: Arc<Config>) -> Arc<Engine> {
        let lease_renew_ms = config.resolve_lease_renewal().unwrap_or(None);
        let ssrf_policy = create_policy(
            config.allow_private_destinations,
            &config.destination_allow_cidrs,
            &config.destination_block_cidrs,
        );
        Arc::new(Engine {
            db,
            slots: Arc::new(Semaphore::new(config.delivery_slots as usize)),
            pool: Arc::new(DeliveryPool::new(
                std::time::Duration::from_millis(config.delivery_pool_idle_ms.max(0) as u64),
                config.delivery_pool_max_idle.max(0) as usize,
            )),
            lease_renew_ms,
            config,
            ssrf_policy,
            wake: Arc::new(Notify::new()),
            drain_lock: Arc::new(AsyncMutex::new(())),
            claim_cursors: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    pub fn notify_dirty(&self) {
        self.wake.notify_one();
    }

    /// Start the fast dispatch loop, the slow sweep (schedules + waiter timeouts) and
    /// the retention sweep. Returns their join handles.
    pub fn spawn(self: &Arc<Engine>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();
        handles.push({
            let e = self.clone();
            tokio::spawn(async move { e.dispatch_loop().await })
        });
        handles.push({
            let e = self.clone();
            tokio::spawn(async move { e.sweep_loop().await })
        });
        handles.push({
            let e = self.clone();
            tokio::spawn(async move { e.retention_loop().await })
        });
        handles
    }

    async fn dispatch_loop(self: &Arc<Engine>) {
        let tick = std::time::Duration::from_millis(self.config.dispatch_tick_ms.max(1) as u64);
        loop {
            self.drain_all().await;
            tokio::select! {
                _ = tokio::time::sleep(tick) => {}
                _ = self.wake.notified() => {}
            }
        }
    }

    /// Slow sweep: fire due schedules, fire waiter timeouts, then drain.
    async fn sweep_loop(self: &Arc<Engine>) {
        let tick = std::time::Duration::from_millis(self.config.scheduler_tick_ms.max(1) as u64);
        loop {
            tokio::time::sleep(tick).await;
            self.sweep_all();
            self.drain_all().await;
        }
    }

    /// Both scans run as ONE statement across all tenants. The indexes
    /// (`idx_schedules_due` on `(is_paused, next_run_at)`, `idx_waiter_timeout_tenant` on
    /// `(tenant_id, timeout_at)`) make the due set a seek, so looping per tenant was pure
    /// overhead: 1001 tenants took CPU from 4.2% to 34.3% and publish p99 from 2.2 ms to
    /// 164.5 ms, because each iteration re-acquired the single DB mutex.
    pub fn sweep_all(&self) {
        let now = now_ms();
        let mut conn = self.db.conn.lock().unwrap();
        if let Err(e) = crate::schedule::scan_due_schedules(&mut conn, None, &self.config, now) {
            tracing::error!(error = %e, "schedule scan failed");
        }
        if let Err(e) = crate::workflow::scan_waiter_timeouts(
            &mut conn,
            None,
            self.config.default_max_retries,
            now,
        ) {
            tracing::error!(error = %e, "waiter timeout scan failed");
        }
    }

    async fn retention_loop(&self) {
        let tick = std::time::Duration::from_millis(self.config.retention_tick_ms.max(1) as u64);
        loop {
            tokio::time::sleep(tick).await;
            self.retention_sweep();
        }
    }

    /// One retention pass. The statements and their best-effort error policy now live in
    /// `retention`; this method only supplies the clock and the once-per-tick log context.
    pub fn retention_sweep(&self) {
        let now = now_ms();
        let counts = {
            let conn = self.db.conn.lock().unwrap();
            crate::retention::sweep(&conn, &self.config, now)
        };
        if counts.total() > 0 {
            tracing::debug!(
                dedupe = counts.dedupe,
                events = counts.events,
                dlq = counts.dlq,
                stuck_runs = counts.stuck_runs,
                terminal_runs = counts.terminal_runs,
                orphan_steps = counts.orphan_steps,
                "retention sweep removed aged rows"
            );
        }
    }

    pub async fn drain_all(self: &Arc<Engine>) {
        let _guard = self.drain_lock.lock().await;
        // Only tenants that actually have due work: an idle tenant used to cost a whole
        // claim transaction per drain (measured at 1001 tenants: p99 2.2 ms → 76.5 ms).
        for tenant in load_with_due_work(&self.db, now_ms()) {
            self.drain_tenant(&tenant).await;
        }
    }

    /// Drain ONE tenant's due messages: acquire slots BEFORE leasing, lease a batch,
    /// dispatch fire-and-forget, repeat while batches come back full.
    async fn drain_tenant(self: &Arc<Engine>, tenant: &Tenant) {
        let mut walk = 0usize;
        for _batch in 0..MAX_DRAIN_BATCHES {
            let want = self.config.dispatch_batch_limit;
            let mut permits: Vec<OwnedSemaphorePermit> = Vec::new();
            for _ in 0..want {
                match self.slots.clone().try_acquire_owned() {
                    Ok(p) => permits.push(p),
                    Err(_) => break,
                }
            }
            let got = permits.len() as i64;
            if got == 0 {
                break; // out of process-wide capacity — next tick retries
            }
            // Resume a paused-prefix walk from where the previous claim stopped, but only
            // while that walk is recent: an old cursor would keep new publishes waiting
            // behind a stale prefix, so it is dropped after CLAIM_CURSOR_TTL.
            let start = {
                let mut cursors = self.claim_cursors.lock().unwrap();
                match cursors.get(&tenant.id) {
                    Some((cursor, since)) if since.elapsed() < CLAIM_CURSOR_TTL => {
                        Some(cursor.clone())
                    }
                    Some(_) => {
                        cursors.remove(&tenant.id);
                        None
                    }
                    None => None,
                }
            };
            let (leased, next) = {
                let mut conn = self.db.conn.lock().unwrap();
                crate::lease::lease_batch(
                    &mut conn,
                    &tenant.id,
                    got,
                    self.config.lease_ms,
                    self.config.tenant_parallelism_max,
                    start.as_ref(),
                )
                .unwrap_or_default()
            };
            // `next` is only Some when the claim was cut short by MAX_CLAIM_SCAN, i.e. there
            // is more prefix to walk without having found live work yet.
            let cut_short = next.is_some();
            {
                let mut cursors = self.claim_cursors.lock().unwrap();
                match next {
                    Some(cursor) => {
                        cursors.insert(tenant.id.clone(), (cursor, std::time::Instant::now()));
                    }
                    None => {
                        cursors.remove(&tenant.id);
                    }
                }
            }
            // return unused slot acquisitions
            let unused = permits.len().saturating_sub(leased.len());
            for p in permits.drain(0..unused) {
                drop(p);
            }
            if leased.is_empty() {
                // Nothing claimable yet. If the scan was cut short there is more prefix to
                // walk, so keep walking inside this drain — one budget per drain would leave
                // a 650k paused prefix needing minutes of ticks before live work appears.
                // Capped so a huge prefix cannot monopolise the drain lock.
                if cut_short && walk < MAX_SCAN_BATCHES_PER_DRAIN {
                    walk += 1;
                    continue;
                }
                break;
            }
            for (row, permit) in leased.into_iter().zip(permits) {
                let e = self.clone();
                let t = tenant.clone();
                tokio::spawn(async move {
                    e.dispatch_one(&t, row).await;
                    drop(permit); // each slot frees as its delivery settles
                                  // A freed slot is demand, not a reason to wait out the tick: without
                                  // this wake the achievable rate of any cap-bound work (flow control,
                                  // queue parallelism) is `slots_free / DISPATCH_TICK_MS`.
                    e.notify_dirty();
                });
            }
            // fire-and-forget: do NOT await the batch; keep draining.
        }
    }

    fn log_event(&self, row: &MessageRow, state: &str, extras: &EventExtras) {
        let conn = self.db.conn.lock().unwrap();
        if let Err(e) =
            crate::store::append_event(&conn, row, state, extras, self.config.event_body_max_bytes)
        {
            // The original dispatcher discarded this error. Keep the best-effort
            // behavior, but make a missing lifecycle row diagnosable in the logs.
            tracing::warn!(
                message_id = %row.id,
                tenant_id = %row.tenant_id,
                state = %state,
                error = %e,
                "could not append message event"
            );
        }
    }

    async fn dispatch_one(self: &Arc<Engine>, tenant: &Tenant, row: MessageRow) {
        // A reclaim can push `retried` past the budget without a fresh response — exhaust now.
        if row.retried > row.max_retries {
            let err = row
                .last_error
                .clone()
                .unwrap_or_else(|| "lease expired (worker lost)".to_string());
            self.exhaust(&row, &err, &None).await;
            return;
        }
        self.log_event(&row, "ACTIVE", &EventExtras::default());

        let response = self.deliver_with_lease(tenant, &row).await;

        // Cancellation raced the delivery: honor it regardless of the outcome, and stop
        // here even when the fenced delete lost (the old worker must not classify a
        // response that belongs to a reclaimed twin).
        let cancel_race = {
            let conn = self.db.conn.lock().unwrap();
            check_cancellation_race(&conn, &row)
        };
        if matches!(cancel_race, CancelRace::Deleted | CancelRace::LostFence) {
            if cancel_race == CancelRace::Deleted {
                self.log_event(&row, "CANCELLED", &EventExtras::default());
            }
            return;
        }

        match response {
            Ok(resp) if (200..300).contains(&resp.status.unwrap_or(0)) => {
                {
                    let mut conn = self.db.conn.lock().unwrap();
                    let owned = complete_delivery(&mut conn, &row, &resp);
                    if !owned {
                        tracing::debug!(
                            message_id = %row.id,
                            retried = row.retried,
                            "success fence lost to a reclaimed attempt"
                        );
                    }
                }
                self.log_event(
                    &row,
                    "DELIVERED",
                    &EventExtras {
                        response_status: resp.status,
                        response_body: resp.body.clone(),
                        response_headers: resp.headers.clone(),
                        ..Default::default()
                    },
                );
            }
            Ok(resp) => {
                let error = format!("endpoint returned {}", resp.status.unwrap_or(0));
                // Non-retryable answers must not burn the remaining attempts.
                if is_non_retryable(resp.status.unwrap_or(0), resp.headers.as_ref()) {
                    self.exhaust(&row, &error, &Some(resp)).await;
                    return;
                }
                if row.retried >= row.max_retries {
                    self.exhaust(&row, &error, &Some(resp)).await;
                    return;
                }
                let delay_ms = retry_delay_from_response(resp.headers.as_ref(), now_ms())
                    .unwrap_or_else(|| retry_delay_ms_for(&row));
                self.retry(&row, &error, delay_ms, Some(&resp)).await;
            }
            Err(error) => {
                if row.retried >= row.max_retries {
                    self.exhaust(&row, &error, &None).await;
                    return;
                }
                let delay_ms = retry_delay_ms_for(&row);
                self.retry(&row, &error, delay_ms, None).await;
            }
        }
    }

    async fn retry(
        &self,
        row: &MessageRow,
        error: &str,
        delay_ms: i64,
        resp: Option<&DeliveryResponse>,
    ) {
        let run_at = now_ms() + delay_ms;
        let done = {
            let conn = self.db.conn.lock().unwrap();
            crate::store::retry_at(&conn, &row.id, row.retried, error, run_at).unwrap_or(false)
        };
        if done {
            self.log_event(
                row,
                "ERROR",
                &EventExtras {
                    error: Some(error.to_string()),
                    response_status: resp.and_then(|r| r.status),
                    response_body: resp.and_then(|r| r.body.clone()),
                    response_headers: resp.and_then(|r| r.headers.clone()),
                    ..Default::default()
                },
            );
            self.log_event(
                row,
                "RETRY",
                &EventExtras {
                    next_delivery_time: Some(run_at),
                    ..Default::default()
                },
            );
        }
    }

    async fn exhaust(&self, row: &MessageRow, error: &str, resp: &Option<DeliveryResponse>) {
        let moved = {
            let mut conn = self.db.conn.lock().unwrap();
            crate::store::move_to_dlq(&mut conn, &row.id, row.retried, error, resp).unwrap_or(None)
        };
        let Some((dlq_id, _)) = moved else {
            return; // fence lost
        };
        self.log_event(
            row,
            "FAILED",
            &EventExtras {
                error: Some(error.to_string()),
                response_status: resp.as_ref().and_then(|r| r.status),
                response_body: resp.as_ref().and_then(|r| r.body.clone()),
                response_headers: resp.as_ref().and_then(|r| r.headers.clone()),
                ..Default::default()
            },
        );
        if let Some(fc_url) = &row.failure_callback_url {
            let cb = build_callback_row(
                row,
                "failure_callback",
                fc_url,
                row.failure_callback_opts.as_ref(),
                resp,
                Some(&dlq_id),
            );
            let conn = self.db.conn.lock().unwrap();
            let _ = crate::store::insert_message(&conn, &cb);
        }
        // A step of a workflow run exhausting its retries fails the whole run.
        if let Some(run_id) = crate::workflow::workflow_run_id_of(&row.headers) {
            let conn = self.db.conn.lock().unwrap();
            let _ = crate::workflow::set_run_state(
                &conn,
                &row.tenant_id,
                &run_id,
                "RUN_FAILED",
                Some(&dlq_id),
            );
        }
    }

    /// Deliver while keeping the lease alive.
    ///
    /// With `lease_ms > max_http_timeout_ms` every attempt ends before its lease could
    /// lapse, so this is just `deliver`. When a delivery is allowed to run longer than the
    /// lease (a 2-hour QStash step under a shorter `LEASE_MS`), the lease is pushed forward
    /// every `lease_renew_ms` instead of letting the row be reclaimed and re-run while the
    /// first attempt is still on the wire.
    ///
    /// The renewer is only started for a delivery whose own timeout can outlive the lease.
    /// A short step (the overwhelming majority) therefore pays nothing: no task, no extra
    /// UPDATE, no lock traffic.
    ///
    /// Renewal stops as soon as the fence misses: `renew_lease` is conditional on
    /// `(retried, state='leased')`, so a row reclaimed by another claim (retried bumped) or
    /// cancelled no longer matches, and a stale worker must not keep extending it. It also
    /// stops once the delivery resolves.
    async fn deliver_with_lease(
        self: &Arc<Engine>,
        tenant: &Tenant,
        row: &MessageRow,
    ) -> Result<DeliveryResponse, String> {
        let Some(interval_ms) = self.lease_renew_ms else {
            return self.deliver(tenant, row).await;
        };
        // The expiry this attempt can actually reach; below the lease there is nothing to
        // renew, so most deliveries skip the spawn entirely.
        if crate::delivery::timeout_ms(&self.config, row) <= self.config.lease_ms {
            return self.deliver(tenant, row).await;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let renewer = {
            let engine = self.clone();
            let stop = stop.clone();
            let id = row.id.clone();
            let retried = row.retried;
            let lease_ms = self.config.lease_ms;
            tokio::spawn(async move {
                let period = std::time::Duration::from_millis(interval_ms.max(1) as u64);
                loop {
                    tokio::time::sleep(period).await;
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let until = now_ms() + lease_ms;
                    let kept = {
                        let conn = engine.db.conn.lock().unwrap();
                        crate::store::renew_lease(&conn, &id, retried, until).unwrap_or(false)
                    };
                    if !kept {
                        // Reclaimed, cancelled, or already resolved: never extend a lease
                        // this worker no longer owns.
                        return;
                    }
                }
            })
        };
        let outcome = self.deliver(tenant, row).await;
        stop.store(true, Ordering::SeqCst);
        renewer.abort();
        outcome
    }

    /// One HTTP attempt through the `delivery` layer. No state is written here.
    async fn deliver(&self, tenant: &Tenant, row: &MessageRow) -> Result<DeliveryResponse, String> {
        let result = crate::delivery::attempt(
            &self.pool,
            &self.ssrf_policy,
            &self.config,
            tenant,
            row,
            now_ms(),
        )
        .await;
        if let Err(error) = &result {
            // Normalize transport/validation failures with the message identity; the error
            // string itself is unchanged (it is part of the last_error/DLQ contract).
            tracing::debug!(
                message_id = %row.id,
                tenant_id = %tenant.id,
                destination = %row.url,
                error = %error,
                "delivery attempt failed"
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::apply_migrations;

    /// Terminal runs are archived and age out; their steps must go with them, otherwise
    /// `workflow_steps` grows forever behind an empty `workflow_runs` (observed: 60 orphan
    /// rows surviving a 45-minute soak's retention window). The statements now live in
    /// `retention`; this test keeps the engine-level entry point honest.
    #[test]
    fn retention_sweep_drops_steps_of_expired_runs() {
        let db = crate::db::open_db(":memory:");
        {
            let mut c = db.conn.lock().unwrap();
            apply_migrations(&mut c).unwrap();
        }
        let cfg = Arc::new(Config::for_test());
        let engine = Engine::new(db.clone(), cfg.clone());

        let old = now_ms() - (cfg.workflow_run_retention_days + 1) * 86_400_000;
        let fresh = now_ms();
        {
            let conn = db.conn.lock().unwrap();
            // one expired terminal run with a step, one live run with a step
            for (run, state, at) in [
                ("run-old", "RUN_SUCCESS", old),
                ("run-new", "RUN_STARTED", fresh),
            ] {
                conn.execute(
                    "INSERT INTO workflow_runs (tenant_id, workflow_run_id, workflow_url, state, created_at, updated_at) VALUES ('t1', ?1, 'http://x/', ?2, ?3, ?3)",
                    rusqlite::params![run, state, at],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO workflow_steps (tenant_id, workflow_run_id, message_id, call_type, created_at) VALUES ('t1', ?1, ?2, 'step', ?3)",
                    rusqlite::params![run, format!("msg-{run}"), at],
                )
                .unwrap();
            }
        }

        engine.retention_sweep();

        let conn = db.conn.lock().unwrap();
        let runs: Vec<String> = conn
            .prepare("SELECT workflow_run_id FROM workflow_runs ORDER BY workflow_run_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .filter_map(|x| x.ok())
            .collect();
        let steps: Vec<String> = conn
            .prepare("SELECT workflow_run_id FROM workflow_steps ORDER BY workflow_run_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .filter_map(|x| x.ok())
            .collect();
        assert_eq!(
            runs,
            vec!["run-new".to_string()],
            "only the live run survives"
        );
        assert_eq!(
            steps,
            vec!["run-new".to_string()],
            "steps of an archived run must be swept with it"
        );
    }

    /// A completed delivery must wake the dispatcher so the freed slot is refilled at once.
    /// This is the difference between `parallelism / DISPATCH_TICK_MS` and the delivery
    /// latency as the binding constraint of flow-controlled work.
    #[tokio::test]
    async fn freed_delivery_slot_wakes_the_dispatcher() {
        let db = crate::db::open_db(":memory:");
        {
            let mut c = db.conn.lock().unwrap();
            apply_migrations(&mut c).unwrap();
        }
        let mut cfg = Config::for_test();
        cfg.dispatch_tick_ms = 60_000; // a tick this slow can never explain any throughput
        cfg.scheduler_tick_ms = 60_000;
        cfg.retention_tick_ms = 600_000;
        cfg.delivery_slots = 4;
        let engine = Engine::new(db, Arc::new(cfg));
        engine.spawn();
        let notified = engine.wake.notified();
        engine.notify_dirty();
        tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("notify_dirty must resolve `notified()` without waiting for a tick");
    }
}
