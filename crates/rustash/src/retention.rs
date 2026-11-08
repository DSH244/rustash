//! Retention sweeps: every statement that ages data out lives here, so the engine owns
//! *when* a sweep runs but not the SQL shape.
//!
//! Each statement is independent and best-effort, exactly as before: a failure in one
//! table is logged and does not prevent the other five from running. They are all
//! idempotent DELETE/UPDATE statements, so the next tick retries safely.

use rusqlite::{params, Connection};

use crate::config::Config;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepCounts {
    pub dedupe: usize,
    pub events: usize,
    pub dlq: usize,
    pub stuck_runs: usize,
    pub terminal_runs: usize,
    pub orphan_steps: usize,
}

impl SweepCounts {
    pub fn total(&self) -> usize {
        self.dedupe
            + self.events
            + self.dlq
            + self.stuck_runs
            + self.terminal_runs
            + self.orphan_steps
    }
}

/// One sweep. `now` is passed in so a caller with a controlled clock can reproduce it.
pub fn sweep(conn: &Connection, config: &Config, now: i64) -> SweepCounts {
    let mut out = SweepCounts::default();

    macro_rules! run {
        ($field:ident, $sql:expr, $params:expr) => {
            match conn.execute($sql, $params) {
                Ok(n) => out.$field = n,
                Err(e) => tracing::warn!(
                    step = stringify!($field),
                    error = %e,
                    "retention statement failed"
                ),
            }
        };
    }

    run!(
        dedupe,
        "DELETE FROM dedupe_entries WHERE expires_at < ?1",
        [now]
    );

    let log_cutoff = now - config.log_retention_days * 86_400_000;
    run!(
        events,
        "DELETE FROM message_events WHERE time < ?1",
        [log_cutoff]
    );

    let dlq_cutoff = now - config.dlq_retention_days * 86_400_000;
    run!(
        dlq,
        "DELETE FROM dlq_messages WHERE dlq_created_at < ?1",
        [dlq_cutoff]
    );

    // A run stuck in RUN_STARTED past the window means the app died mid-run.
    let stuck_cutoff = now - config.workflow_run_stuck_hours * 3_600_000;
    run!(
        stuck_runs,
        "UPDATE workflow_runs SET state = 'RUN_FAILED', completed_at = ?2, updated_at = ?2 WHERE state = 'RUN_STARTED' AND created_at < ?1",
        params![stuck_cutoff, now]
    );

    // Age out terminal run rows (the archive is time-boxed, like logs/DLQ). Steps are
    // dropped in the same statement pair: they exist only to replay a run, so once the
    // run row is gone they are unreachable rows that would otherwise grow forever
    // (a 45-minute soak left 60 orphaned steps behind an empty `workflow_runs`).
    let run_cutoff = now - config.workflow_run_retention_days * 86_400_000;
    run!(
        terminal_runs,
        "DELETE FROM workflow_runs WHERE state <> 'RUN_STARTED' AND updated_at < ?1",
        [run_cutoff]
    );
    run!(
        orphan_steps,
        r#"DELETE FROM workflow_steps
            WHERE created_at < ?1
              AND NOT EXISTS (
                    SELECT 1 FROM workflow_runs r
                     WHERE r.tenant_id = workflow_steps.tenant_id
                       AND r.workflow_run_id = workflow_steps.workflow_run_id
                   )"#,
        [run_cutoff]
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{apply_migrations, open_db};

    #[test]
    fn sweep_counts_every_aged_row_and_keeps_live_runs() {
        let db = open_db(":memory:");
        {
            let mut c = db.conn.lock().unwrap();
            apply_migrations(&mut c).unwrap();
        }
        let cfg = Config::for_test();
        let now = crate::time::now_ms();
        let old = now - (cfg.workflow_run_retention_days + 1) * 86_400_000;

        {
            let c = db.conn.lock().unwrap();
            c.execute(
                "INSERT INTO dedupe_entries (tenant_id, dedupe_key, message_id, expires_at) VALUES ('t1','k','m',1)",
                [],
            ).unwrap();
            c.execute(
                "INSERT INTO message_events (tenant_id, time, message_id, state, url, method, headers, max_retries, retried, labels) VALUES ('t1',1,'m','ACTIVE','http://x/','POST','{}',3,0,'[]')",
                [],
            ).unwrap();
            c.execute(
                "INSERT INTO dlq_messages (dlq_id, tenant_id, message_id, url, method, headers, max_retries, retried, created_at, dlq_created_at) VALUES ('d1','t1','m','http://x/','POST','{}',3,0,1,1)",
                [],
            ).unwrap();
            for (run, state, at) in [
                ("expired", "RUN_SUCCESS", old),
                ("stuck", "RUN_STARTED", old),
                ("live", "RUN_STARTED", now),
            ] {
                c.execute(
                    "INSERT INTO workflow_runs (tenant_id, workflow_run_id, workflow_url, state, created_at, updated_at) VALUES ('t1', ?1, 'http://x/', ?2, ?3, ?3)",
                    params![run, state, at],
                ).unwrap();
                c.execute(
                    "INSERT INTO workflow_steps (tenant_id, workflow_run_id, message_id, call_type, created_at) VALUES ('t1', ?1, ?2, 'step', ?3)",
                    params![run, format!("msg-{run}"), at],
                ).unwrap();
            }
        }

        let counts = {
            let c = db.conn.lock().unwrap();
            sweep(&c, &cfg, now)
        };
        assert_eq!(counts.dedupe, 1);
        assert_eq!(counts.events, 1);
        assert_eq!(counts.dlq, 1);
        assert_eq!(counts.stuck_runs, 1, "stuck run transitions to RUN_FAILED");
        assert_eq!(
            counts.terminal_runs, 1,
            "expired terminal run is archived away"
        );
        assert_eq!(
            counts.orphan_steps, 1,
            "only the expired terminal run's step is orphaned; the still-live RUN_FAILED row keeps its step"
        );
        assert_eq!(counts.total(), 6);

        let c = db.conn.lock().unwrap();
        let runs: Vec<(String, String)> = c
            .prepare("SELECT workflow_run_id, state FROM workflow_runs ORDER BY workflow_run_id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(
            runs,
            vec![
                ("live".to_string(), "RUN_STARTED".to_string()),
                ("stuck".to_string(), "RUN_FAILED".to_string()),
            ]
        );
        let steps: Vec<String> = c
            .prepare("SELECT workflow_run_id FROM workflow_steps ORDER BY workflow_run_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(
            steps,
            vec!["live".to_string(), "stuck".to_string()],
            "a RUN_FAILED row has not aged out yet, so its step remains reachable"
        );
    }
}
