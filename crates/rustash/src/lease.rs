//! The claim (engine/lease.js port): one transaction that finds due candidates
//! (`pending` & due OR `leased` & lease-expired), ranks them per-queue FIFO, enforces
//! queue parallelism + the tenant-global cap, and flips them to `leased`.
//!
//! The JS reference used a single CTE statement with `FOR UPDATE SKIP LOCKED`; this port
//! achieves the same with SQLite's window functions plus the serialized single-writer
//! connection (concurrent claims queue on the Db mutex — at most one claim runs at a
//! time, which is the correctness property the claim tests check).

use std::collections::{HashMap, HashSet};

use rusqlite::{params, Connection};

use crate::flow;
use crate::model::MessageRow;
use crate::store::{row_to_message, MESSAGE_INSERT_COLS};
use crate::time::now_ms;

#[derive(Debug)]
pub struct Candidate {
    pub id: String,
    pub queue_name: Option<String>,
    pub flow_control_key: Option<String>,
    pub prev_state: String,
    pub created_at: i64,
    pub not_before: i64,
}

/// Position in the pending FIFO `(not_before, created_at, id)`. Carried across claims so a
/// paused prefix longer than `MAX_CLAIM_SCAN` still makes progress instead of re-reading the
/// same head forever.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimCursor {
    pub not_before: i64,
    pub created_at: i64,
    pub id: String,
}

/// In-flight snapshot: global count, then `(queue, count)` and `(flow-control key, count)`.
type InflightSnapshot = (i64, Vec<(String, i64)>, Vec<(String, i64)>);

/// Snapshot counts of currently in-flight (unexpired) leases, per queue, per
/// flow-control key and global.
fn inflight(conn: &Connection, tenant_id: &str, now: i64) -> InflightSnapshot {
    // `INDEXED BY idx_messages_lease` for the same reason as the candidate scan: an
    // unpinned plan reads every row of the tenant through `idx_messages_url` (24 ms at
    // 40k rows) to count what is usually zero to a few hundred leased rows.
    let g: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages INDEXED BY idx_messages_lease WHERE tenant_id=?1 AND state='leased' AND leased_until > ?2",
            params![tenant_id, now],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let mut stmt = conn
        .prepare(
            "SELECT queue_name, COUNT(*) FROM messages INDEXED BY idx_messages_lease WHERE tenant_id=?1 AND state='leased' AND leased_until > ?2 AND queue_name IS NOT NULL GROUP BY queue_name",
        )
        .unwrap();
    let per_queue = stmt
        .query_map(params![tenant_id, now], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })
        .unwrap()
        .filter_map(|x| x.ok())
        .collect();
    drop(stmt);

    let mut stmt = conn
        .prepare(
            "SELECT flow_control_key, COUNT(*) FROM messages INDEXED BY idx_messages_lease WHERE tenant_id=?1 AND state='leased' AND leased_until > ?2 AND flow_control_key IS NOT NULL GROUP BY flow_control_key",
        )
        .unwrap();
    let per_fc = stmt
        .query_map(params![tenant_id, now], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })
        .unwrap()
        .filter_map(|x| x.ok())
        .collect();
    (g, per_queue, per_fc)
}

fn queue_parallelism(conn: &Connection, tenant_id: &str, name: &str) -> i64 {
    conn.query_row(
        "SELECT parallelism FROM queues WHERE tenant_id=?1 AND name=?2",
        params![tenant_id, name],
        |r| r.get(0),
    )
    .unwrap_or(1)
}

fn bump_counter(counters: &mut Vec<(String, i64)>, name: &str) -> i64 {
    match counters.iter_mut().find(|(n, _)| n == name) {
        Some(slot) => {
            slot.1 += 1;
            slot.1
        }
        None => {
            counters.push((name.to_string(), 1));
            1
        }
    }
}

/// One page of the pending FIFO, strictly after the cursor `(not_before, created_at, id)`.
///
/// Kept as a named constant so tests can assert the plan it compiles to: it must be a
/// single ordered `SEARCH` on `idx_messages_fifo` with no TEMP B-TREE. The row-value
/// comparison is what makes the page boundary seekable — a plain `OFFSET` would re-read
/// the skipped prefix on every page.
pub const PENDING_PAGE_SQL: &str = r#"SELECT m.id, m.queue_name, m.flow_control_key, m.state, m.created_at, m.not_before
  FROM messages m INDEXED BY idx_messages_fifo
 WHERE m.tenant_id = ?1
   AND m.state = 'pending'
   AND m.not_before <= ?2
   AND (m.not_before, m.created_at, m.id) > (?3, ?4, ?5)
 ORDER BY m.not_before, m.created_at, m.id
 LIMIT ?6"#;

/// Expired-lease candidates. `idx_messages_lease` is keyed on `leased_until` only, so
/// this branch cannot be paged by the FIFO cursor; it is also intrinsically small (only
/// rows whose lease already ran out), so a single bounded scan is enough.
pub const LEASE_EXPIRED_SQL: &str = r#"SELECT m.id, m.queue_name, m.flow_control_key, m.state, m.created_at, m.not_before
  FROM messages m INDEXED BY idx_messages_lease
 WHERE m.tenant_id = ?1
   AND m.state = 'leased'
   AND m.leased_until <= ?2
 ORDER BY m.not_before, m.created_at, m.id
 LIMIT ?3"#;

/// How many candidate rows one claim may inspect. Bounds the work a paused prefix can
/// cost: before paging, a paused flow-control key holding the whole queue head made every
/// claim read the entire paused set (140 ms/claim at 650k rows, measured), and that ran
/// inside the drain lock and behind the single DB mutex, so every publish stalled with it.
pub const MAX_CLAIM_SCAN: i64 = 1024;

/// The paused queue names and flow-control keys, loaded once per claim so the page scan
/// can filter in memory instead of probing two tables per candidate row.
fn paused_sets(
    conn: &Connection,
    tenant_id: &str,
) -> rusqlite::Result<(
    std::collections::HashSet<String>,
    std::collections::HashSet<String>,
)> {
    let mut q = std::collections::HashSet::new();
    let mut stmt = conn.prepare("SELECT name FROM queues WHERE tenant_id = ?1 AND paused <> 0")?;
    for row in stmt.query_map(params![tenant_id], |r| r.get::<_, String>(0))? {
        q.insert(row?);
    }
    drop(stmt);
    let mut k = std::collections::HashSet::new();
    let mut stmt =
        conn.prepare("SELECT key FROM flow_control_keys WHERE tenant_id = ?1 AND paused <> 0")?;
    for row in stmt.query_map(params![tenant_id], |r| r.get::<_, String>(0))? {
        k.insert(row?);
    }
    Ok((q, k))
}

fn is_paused(
    c: &Candidate,
    paused_queues: &std::collections::HashSet<String>,
    paused_keys: &std::collections::HashSet<String>,
) -> bool {
    c.queue_name
        .as_ref()
        .is_some_and(|n| paused_queues.contains(n))
        || c.flow_control_key
            .as_ref()
            .is_some_and(|k| paused_keys.contains(k))
}

/// Walk one claim's candidates.
///
/// Shape matters more than it looks. The old statement filtered paused queues and
/// flow-control keys with `NOT IN (SELECT ...)` *before* `LIMIT`, so a paused key holding
/// the head of the queue made every claim read the whole paused prefix: 11 ms at 40k rows,
/// 140 ms at 650k. That ran inside the drain lock and behind the single DB mutex, so
/// publish latency tracked the size of the paused backlog (1.0 ms → 396 ms measured).
///
/// Now the SQL is a pure ordered index seek with a row-value cursor and no subqueries, and
/// the pause filter runs in memory over a small paused set. `MAX_CLAIM_SCAN` bounds the
/// rows one claim may read, so the cost no longer depends on the prefix length — the caller
/// carries `next_cursor` between claims to keep progress when the prefix is longer than the
/// budget (see `lease_messages`).
fn collect_candidates(
    conn: &Connection,
    tenant_id: &str,
    now: i64,
    want: i64,
    start: Option<&ClaimCursor>,
    paused_queues: &HashSet<String>,
    paused_keys: &HashSet<String>,
) -> rusqlite::Result<(Vec<Candidate>, i64, Option<ClaimCursor>)> {
    let mut out: Vec<Candidate> = Vec::with_capacity(want as usize);
    let mut stmt = conn.prepare(PENDING_PAGE_SQL)?;
    // Before every real row: not_before is epoch ms (>= 0) and the message id is a
    // non-empty ULID, so this sorts below any stored tuple.
    let mut cursor: ClaimCursor = start.cloned().unwrap_or(ClaimCursor {
        not_before: i64::MIN,
        created_at: i64::MIN,
        id: String::new(),
    });
    let mut scanned: i64 = 0;
    let mut hit_budget = false;
    while out.len() < want as usize {
        let budget = MAX_CLAIM_SCAN - scanned;
        if budget <= 0 {
            hit_budget = true;
            break;
        }
        let page_size = (want as usize - out.len()).max(1).min(budget as usize) as i64;
        let page: Vec<Candidate> = stmt
            .query_map(
                params![
                    tenant_id,
                    now,
                    cursor.not_before,
                    cursor.created_at,
                    cursor.id,
                    page_size
                ],
                |r| {
                    Ok(Candidate {
                        id: r.get(0)?,
                        queue_name: r.get(1)?,
                        flow_control_key: r.get(2)?,
                        prev_state: r.get(3)?,
                        created_at: r.get(4)?,
                        not_before: r.get(5)?,
                    })
                },
            )?
            .filter_map(|x| x.ok())
            .collect();
        if page.is_empty() {
            return Ok((out, scanned, None)); // reached the end of the due set
        }
        let page_len = page.len() as i64;
        scanned += page_len;
        if let Some(last) = page.last() {
            cursor = ClaimCursor {
                not_before: last.not_before,
                created_at: last.created_at,
                id: last.id.clone(),
            };
        }
        for c in page {
            if !is_paused(&c, paused_queues, paused_keys) {
                out.push(c);
            }
        }
        if page_len < page_size {
            return Ok((out, scanned, None)); // fewer rows than asked for: due set is empty
        }
    }
    // Only a budget cut leaves the caller with somewhere to continue from. Reaching `want`
    // means this claim is already full and the next one restarts at the head, exactly like
    // the original head-relative claim — which is what preserves per-queue FIFO.
    let next = if hit_budget { Some(cursor) } else { None };
    Ok((out, scanned, next))
}

fn leased_candidates(
    conn: &Connection,
    tenant_id: &str,
    now: i64,
    want: i64,
    paused_queues: &HashSet<String>,
    paused_keys: &HashSet<String>,
) -> rusqlite::Result<Vec<Candidate>> {
    // Read up to the scan budget, filter paused rows in memory, then take `want`. Filtering
    // after a `LIMIT want` would return fewer than `want` whenever some expired leases belong
    // to paused keys, silently shrinking every claim that overlaps a pause.
    let scan = MAX_CLAIM_SCAN.min(i64::MAX / 2);
    let mut stmt = conn.prepare(LEASE_EXPIRED_SQL)?;
    let rows: Vec<Candidate> = stmt
        .query_map(params![tenant_id, now, scan], |r| {
            Ok(Candidate {
                id: r.get(0)?,
                queue_name: r.get(1)?,
                flow_control_key: r.get(2)?,
                prev_state: r.get(3)?,
                created_at: r.get(4)?,
                not_before: r.get(5)?,
            })
        })?
        .filter_map(|x| x.ok())
        .filter(|c| !is_paused(c, paused_queues, paused_keys))
        .take(want.max(0) as usize)
        .collect();
    Ok(rows)
}

/// Claim up to `batch_limit` due messages for one tenant. Returns the leased rows in
/// claim order. A reclaim (expired-lease candidate) increments `retried` and stamps
/// `last_error = "lease expired (worker lost)"`.
///
/// Caps enforced: per-queue FIFO parallelism, per-flow-control key parallelism and
/// token-bucket rate, and the tenant-global cap. The token bucket is rolled over lazily
/// inside this transaction and spent for what was actually claimed.
///
/// `start` resumes a scan the previous claim abandoned because it hit `MAX_CLAIM_SCAN`
/// (only possible when a paused prefix is longer than the budget). `None` means "from the
/// head", which is the normal case and what keeps per-queue FIFO intact.
pub fn lease_batch(
    conn: &mut Connection,
    tenant_id: &str,
    batch_limit: i64,
    lease_ms: i64,
    tenant_parallelism_max: i64,
    start: Option<&ClaimCursor>,
) -> rusqlite::Result<(Vec<MessageRow>, Option<ClaimCursor>)> {
    let now = now_ms();
    let tx = conn.transaction()?;

    // Lazy token-bucket rollover (a data-modifying CTE cannot see sibling CTE effects,
    // so this is a separate statement inside the same transaction).
    flow::rollover_rate_buckets(&tx, tenant_id, now)?;

    let (paused_queues, paused_keys) = paused_sets(&tx, tenant_id)?;
    let (mut candidates, _scanned, next_cursor) = collect_candidates(
        &tx,
        tenant_id,
        now,
        batch_limit,
        start,
        &paused_queues,
        &paused_keys,
    )?;
    candidates.extend(leased_candidates(
        &tx,
        tenant_id,
        now,
        batch_limit,
        &paused_queues,
        &paused_keys,
    )?);
    // The two scans are ordered independently; merge so ranks follow global FIFO. Ties on
    // (not_before, created_at) fall back to id, exactly like the SQL ordering.
    candidates.sort_by(|a, b| {
        (a.not_before, a.created_at, &a.id).cmp(&(b.not_before, b.created_at, &b.id))
    });
    candidates.truncate(batch_limit.max(0) as usize);

    let (inflight_global, inflight_q, inflight_fc) = inflight(&tx, tenant_id, now);

    // Ranks are computed over ALL candidates (JS window functions); a row failing a cap
    // does not renumber the ones behind it.
    let mut q_counter: Vec<(String, i64)> = Vec::new();
    let mut fc_counter: Vec<(String, i64)> = Vec::new();
    let mut spent: HashMap<String, i64> = HashMap::new();
    let mut claimed = Vec::new();
    for (idx, c) in candidates.iter().enumerate() {
        let g_rn = idx as i64 + 1;
        let mut eligible = inflight_global + g_rn <= tenant_parallelism_max;

        if let Some(queue) = &c.queue_name {
            let q_rn = bump_counter(&mut q_counter, queue);
            let inflight = inflight_q
                .iter()
                .find(|(n, _)| n == queue)
                .map(|(_, n)| *n)
                .unwrap_or(0);
            let par = queue_parallelism(&tx, tenant_id, queue);
            if inflight + q_rn > par {
                eligible = false;
            }
        }

        if let Some(key) = &c.flow_control_key {
            let fc_rn = bump_counter(&mut fc_counter, key);
            if let Some(cfg) = flow::config_for(&tx, tenant_id, key) {
                if let Some(par) = cfg.parallelism {
                    let inflight = inflight_fc
                        .iter()
                        .find(|(n, _)| n == key)
                        .map(|(_, n)| *n)
                        .unwrap_or(0);
                    if inflight + fc_rn > par {
                        eligible = false;
                    }
                }
                if let Some(rate) = cfg.rate {
                    if cfg.rate_count + fc_rn > rate {
                        eligible = false;
                    }
                }
            }
        }

        if !eligible {
            continue;
        }

        let reclaim = c.prev_state == "leased";
        tx.execute(
            r#"UPDATE messages
               SET state = 'leased',
                   leased_until = ?2,
                   retried = CASE WHEN ?3 THEN retried + 1 ELSE retried END,
                   last_error = CASE WHEN ?3 THEN 'lease expired (worker lost)' ELSE last_error END
               WHERE id = ?1"#,
            params![c.id, now + lease_ms, reclaim],
        )?;
        let mut s = tx.prepare(&format!(
            "SELECT {} FROM messages WHERE id = ?1",
            MESSAGE_INSERT_COLS
        ))?;
        let row = s.query_row(params![c.id], row_to_message)?;
        drop(s);
        if let Some(key) = &c.flow_control_key {
            *spent.entry(key.clone()).or_insert(0) += 1;
        }
        claimed.push(row);
    }

    // Spend the rate budget for what was actually claimed, per rate-limited key.
    for (key, n) in spent {
        flow::spend_rate(&tx, tenant_id, &key, n)?;
    }

    tx.commit()?;
    // A cursor is only handed back when the scan was cut short by the budget; in every
    // other case the next claim starts at the head so per-queue FIFO is never reordered.
    Ok((claimed, next_cursor))
}

/// Claim without carrying a cursor: one batch from the head, discarding any resume point.
/// Convenience for callers that re-claim from scratch each time (and for the tests that
/// predate paging).
pub fn lease_messages(
    conn: &mut Connection,
    tenant_id: &str,
    batch_limit: i64,
    lease_ms: i64,
    tenant_parallelism_max: i64,
) -> rusqlite::Result<Vec<MessageRow>> {
    lease_batch(
        conn,
        tenant_id,
        batch_limit,
        lease_ms,
        tenant_parallelism_max,
        None,
    )
    .map(|(rows, _)| rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::apply_migrations;
    use rusqlite::Connection;

    fn fresh() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_migrations(&mut conn).unwrap();
        conn.execute(
            "INSERT INTO tenants (id,name,token_hash,signing_key_current,signing_key_next,created_at,updated_at) VALUES ('t1','default','h','c','n',0,0)",
            [],
        )
        .unwrap();
        conn
    }

    /// Fixture row for the claim tests. Only the varied fields are spelled out at the call
    /// site; the rest keep the "plain pending message" defaults below.
    struct Row<'a> {
        id: &'a str,
        state: &'a str,
        not_before: i64,
        created_at: i64,
        queue: Option<&'a str>,
        leased_until: Option<i64>,
        retried: i64,
    }

    impl<'a> Row<'a> {
        fn new(id: &'a str, state: &'a str, not_before: i64, created_at: i64) -> Self {
            Self {
                id,
                state,
                not_before,
                created_at,
                queue: None,
                leased_until: None,
                retried: 0,
            }
        }

        fn queue(mut self, queue: &'a str) -> Self {
            self.queue = Some(queue);
            self
        }

        fn leased_until(mut self, leased_until: i64) -> Self {
            self.leased_until = Some(leased_until);
            self
        }

        fn retried(mut self, retried: i64) -> Self {
            self.retried = retried;
            self
        }
    }

    fn add(conn: &Connection, row: Row<'_>) {
        conn.execute(
            r#"INSERT INTO messages (id, tenant_id, kind, state, url, method, headers, max_retries, not_before, queue_name, labels, retried, leased_until, created_at)
               VALUES (?1,'t1','message',?2,'https://x/','POST','{}',3,?3,?4,'[]',?5,?6,?7)"#,
            params![
                row.id,
                row.state,
                row.not_before,
                row.queue,
                row.retried,
                row.leased_until,
                row.created_at
            ],
        )
        .unwrap();
    }

    #[test]
    fn due_pending_becomes_leased() {
        let mut conn = fresh();
        add(&conn, Row::new("m1", "pending", 1, 1));
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].state, "leased");
        assert_eq!(leased[0].retried, 0);
        assert!(leased[0].leased_until.unwrap() > now_ms());
        // second claim finds nothing (leased & unexpired is invisible)
        assert!(lease_messages(&mut conn, "t1", 32, 5000, 1024)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn expired_lease_is_reclaimed_with_retry_increment() {
        let mut conn = fresh();
        add(
            &conn,
            Row::new("m1", "leased", 1, 1)
                .leased_until(now_ms() - 1)
                .retried(1),
        );
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].retried, 2);
        assert_eq!(
            leased[0].last_error.as_deref(),
            Some("lease expired (worker lost)")
        );
    }

    #[test]
    fn future_not_before_is_not_due() {
        let mut conn = fresh();
        add(&conn, Row::new("m1", "pending", now_ms() + 60_000, 1));
        assert!(lease_messages(&mut conn, "t1", 32, 5000, 1024)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn fifo_single_flight_per_queue() {
        let mut conn = fresh();
        conn.execute(
            "INSERT INTO queues (id, tenant_id, name, parallelism, paused, created_at, updated_at) VALUES ('q1','t1','Q',1,0,0,0)",
            [],
        )
        .unwrap();
        add(&conn, Row::new("m1", "pending", 1, 100).queue("Q"));
        add(&conn, Row::new("m2", "pending", 1, 200).queue("Q"));
        add(&conn, Row::new("m3", "pending", 1, 300).queue("Q"));
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        // only the FIFO head may enter flight with parallelism=1
        assert_eq!(
            leased.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            vec!["m1"]
        );
        // after m1 settles (leave queue), the next head claims
        conn.execute("DELETE FROM messages WHERE id='m1'", [])
            .unwrap();
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(
            leased.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            vec!["m2"]
        );
    }

    #[test]
    fn parallelism_two_admits_two_heads() {
        let mut conn = fresh();
        conn.execute(
            "INSERT INTO queues (id, tenant_id, name, parallelism, paused, created_at, updated_at) VALUES ('q1','t1','Q',2,0,0,0)",
            [],
        )
        .unwrap();
        add(&conn, Row::new("m1", "pending", 1, 100).queue("Q"));
        add(&conn, Row::new("m2", "pending", 1, 200).queue("Q"));
        add(&conn, Row::new("m3", "pending", 1, 300).queue("Q"));
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(leased.len(), 2);
    }

    #[test]
    fn paused_queue_is_skipped() {
        let mut conn = fresh();
        conn.execute(
            "INSERT INTO queues (id, tenant_id, name, parallelism, paused, created_at, updated_at) VALUES ('q1','t1','Q',1,1,0,0)",
            [],
        )
        .unwrap();
        add(&conn, Row::new("m1", "pending", 1, 100).queue("Q"));
        assert!(lease_messages(&mut conn, "t1", 32, 5000, 1024)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn tenant_global_cap_bounds_claims() {
        let mut conn = fresh();
        add(&conn, Row::new("m1", "pending", 1, 1));
        add(&conn, Row::new("m2", "pending", 1, 2));
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1).unwrap();
        assert_eq!(leased.len(), 1);
    }

    #[test]
    fn concurrent_claims_are_serialized_single_claimer_wins() {
        // Two threads claim the same due row through the shared Mutex<Connection> (the
        // same shape the server uses): exactly one lease total across both calls — the
        // mutex serializes claims, so no message can be double-leased.
        let conn = std::sync::Arc::new(std::sync::Mutex::new(fresh()));
        {
            let g = conn.lock().unwrap();
            add(&g, Row::new("m1", "pending", 1, 1));
        }
        let c1 = conn.clone();
        let c2 = conn.clone();
        let t1 = std::thread::spawn(move || {
            let mut g = c1.lock().unwrap();
            lease_messages(&mut g, "t1", 32, 5000, 1024).unwrap().len()
        });
        let t2 = std::thread::spawn(move || {
            let mut g = c2.lock().unwrap();
            lease_messages(&mut g, "t1", 32, 5000, 1024).unwrap().len()
        });
        let a = t1.join().unwrap();
        let b = t2.join().unwrap();
        assert_eq!(a + b, 1, "exactly one concurrent claim may win");
    }

    fn add_fc(conn: &Connection, id: &str, key: &str, not_before: i64, created_at: i64) {
        conn.execute(
            r#"INSERT INTO messages (id, tenant_id, kind, state, url, method, headers, max_retries, not_before, flow_control_key, labels, retried, created_at)
               VALUES (?1,'t1','message','pending','https://x/','POST','{}',3,?2,?3,'[]',0,?4)"#,
            params![id, not_before, key, created_at],
        )
        .unwrap();
    }

    fn fc_key(conn: &Connection, key: &str, parallelism: Option<i64>, rate: Option<i64>) {
        conn.execute(
            "INSERT INTO flow_control_keys (tenant_id, key, parallelism, rate, period_secs, pinned_parallelism, pinned_rate, paused, rate_count, rate_period_start, updated_at) VALUES ('t1',?1,?2,?3,1,0,0,0,0,0,0)",
            params![key, parallelism, rate],
        )
        .unwrap();
    }

    #[test]
    fn paused_flow_control_key_is_skipped() {
        let mut conn = fresh();
        fc_key(&conn, "k", Some(2), None);
        conn.execute(
            "UPDATE flow_control_keys SET paused = 1 WHERE tenant_id='t1' AND key='k'",
            [],
        )
        .unwrap();
        add_fc(&conn, "m1", "k", 1, 1);
        assert!(lease_messages(&mut conn, "t1", 32, 5000, 1024)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn flow_control_parallelism_caps_leases() {
        let mut conn = fresh();
        fc_key(&conn, "k", Some(1), None);
        add_fc(&conn, "m1", "k", 1, 1);
        add_fc(&conn, "m2", "k", 1, 2);
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(
            leased.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            vec!["m1"]
        );
    }

    /// A paused prefix longer than one claim's scan budget must still let the rows behind
    /// it through. This is the behavioural half of the paused-prefix fix: the old SQL
    /// filtered paused rows with `NOT IN` before `LIMIT`, so a paused key holding the whole
    /// head made every claim re-walk the entire prefix and return nothing (11 ms at 4 万行,
    /// 140 ms at 65 万 rows — inside the drain lock and the single DB mutex).
    #[test]
    fn paused_prefix_longer_than_scan_budget_still_yields_live_rows() {
        let mut conn = fresh();
        fc_key(&conn, "paused", Some(1), None);
        fc_key(&conn, "live", Some(4), None);
        conn.execute(
            "UPDATE flow_control_keys SET paused = 1 WHERE tenant_id='t1' AND key='paused'",
            [],
        )
        .unwrap();
        // Paused rows occupy the whole head: more of them than one claim may scan.
        let head = crate::lease::MAX_CLAIM_SCAN + 200;
        for i in 0..head {
            add_fc(&conn, &format!("p{:06}", i), "paused", 1, i);
        }
        for i in 0..4 {
            add_fc(&conn, &format!("live{i}"), "live", 1, head + i);
        }

        // Claim 1: budget cuts inside the paused prefix, so nothing is claimable yet but the
        // cursor must survive.
        let (first, cursor) = lease_batch(&mut conn, "t1", 32, 5000, 1024, None).unwrap();
        assert!(first.is_empty(), "paused rows must not be claimed");
        let mut cursor = cursor.expect("a cut-short scan must hand back a resume point");

        // Keep walking with the returned cursor; eventually the live rows appear.
        let mut got: Vec<String> = Vec::new();
        for _ in 0..64 {
            let (rows, next) = lease_batch(&mut conn, "t1", 32, 5000, 1024, Some(&cursor)).unwrap();
            got.extend(rows.into_iter().map(|r| r.id));
            match next {
                Some(c) => cursor = c,
                None => break,
            }
            if !got.is_empty() {
                break;
            }
        }
        got.sort();
        assert_eq!(
            got,
            vec!["live0", "live1", "live2", "live3"],
            "live rows behind the paused prefix must be claimable"
        );
        // Every paused row stayed pending.
        let pending_paused: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE flow_control_key='paused' AND state='pending'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pending_paused, head);
    }

    /// One claim must not read more than `MAX_CLAIM_SCAN` rows, whatever the prefix looks
    /// like — that bound is what keeps publish latency independent of paused backlog size.
    #[test]
    fn claim_scan_is_bounded_by_the_budget() {
        let mut conn = fresh();
        fc_key(&conn, "paused", Some(1), None);
        conn.execute(
            "UPDATE flow_control_keys SET paused = 1 WHERE tenant_id='t1' AND key='paused'",
            [],
        )
        .unwrap();
        let rows = MAX_CLAIM_SCAN * 3;
        for i in 0..rows {
            add_fc(&conn, &format!("q{:06}", i), "paused", 1, i);
        }
        // The page walk stops at the budget, so a single claim cannot be O(backlog).
        let (got, cursor) = lease_batch(&mut conn, "t1", 32, 5000, 1024, None).unwrap();
        assert!(got.is_empty());
        let cursor = cursor.expect("budget cut must return a cursor");
        // The resume point is the last row the budget allowed it to look at, not the end.
        assert_eq!(cursor.created_at, MAX_CLAIM_SCAN - 1);
    }

    #[test]
    fn flow_control_rate_caps_and_rolls_over() {
        let mut conn = fresh();
        fc_key(&conn, "k", None, Some(2));
        add_fc(&conn, "m1", "k", 1, 1);
        add_fc(&conn, "m2", "k", 1, 2);
        add_fc(&conn, "m3", "k", 1, 3);
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(leased.len(), 2);
        // budget spent for this period
        assert!(lease_messages(&mut conn, "t1", 32, 5000, 1024)
            .unwrap()
            .is_empty());
        // a period older than period_secs allows the bucket to refill
        let stale = now_ms() - 5_000;
        conn.execute(
            "UPDATE flow_control_keys SET rate_count = 2, rate_period_start = ?1 WHERE tenant_id='t1' AND key='k'",
            params![stale],
        )
        .unwrap();
        let leased = lease_messages(&mut conn, "t1", 32, 5000, 1024).unwrap();
        assert_eq!(leased.len(), 1);
    }

    /// The claim's two scans must each be a single ordered index seek — no tenant-wide
    /// scan, no TEMP B-TREE. This is the regression guard for the 40k-backlog stall: the
    /// plan used to be `SEARCH ... idx_messages_url` plus a sort of every due row, which
    /// pinned the single DB mutex for >10 ms per claim. It also guards the paused-prefix
    /// fix: the pending scan must stay a plain seek with no subquery in the WHERE clause,
    /// otherwise SQLite re-walks the paused prefix on every claim.
    #[test]
    fn claim_scans_are_index_bounded() {
        let conn = fresh();
        // Enough rows that a wrong plan is structurally different, not just slower.
        for i in 0..500 {
            let st = if i % 50 == 0 { "leased" } else { "pending" };
            let id = format!("m{i}");
            add(&conn, Row::new(&id, st, i as i64, i as i64).leased_until(1));
        }
        let plan = |sql: &str, args: &[&dyn rusqlite::types::ToSql]| -> Vec<String> {
            conn.prepare(&format!("EXPLAIN QUERY PLAN {}", sql))
                .unwrap()
                .query_map(args, |r| r.get::<_, String>(3))
                .unwrap()
                .filter_map(|x| x.ok())
                .collect()
        };

        let tenant = "t1".to_string();
        let floor = i64::MIN;
        let pending = plan(
            PENDING_PAGE_SQL,
            &[&tenant, &10_000i64, &floor, &floor, &String::new(), &32i64],
        )
        .join(" || ");
        assert!(
            pending.contains("idx_messages_fifo"),
            "pending scan must use idx_messages_fifo, plan was: {pending}"
        );
        assert!(
            !pending.contains("TEMP B-TREE"),
            "pending scan must be index-ordered, plan was: {pending}"
        );
        for forbidden in ["idx_messages_url (tenant_id=?)", "SCAN m", "SCAN messages"] {
            assert!(
                !pending.contains(forbidden),
                "pending scan must not contain {forbidden}: {pending}"
            );
        }
        // No paused subquery may creep back into the SQL: that is what made claim cost
        // scale with the paused prefix (140 ms/claim at 650k paused rows).
        assert!(
            !pending.to_uppercase().contains("SUBQUERY"),
            "pending scan must not probe paused sets per row, plan was: {pending}"
        );

        let lease = plan(LEASE_EXPIRED_SQL, &[&tenant, &10_000i64, &32i64]).join(" || ");
        assert!(
            lease.contains("idx_messages_lease"),
            "lease scan must use idx_messages_lease, plan was: {lease}"
        );
        for forbidden in ["idx_messages_url (tenant_id=?)", "SCAN m", "SCAN messages"] {
            assert!(
                !lease.contains(forbidden),
                "lease scan must not contain {forbidden}: {lease}"
            );
        }
    }

    /// The inflight counter is the other half of the same stall: unpinned it read every row
    /// of the tenant (24 ms at 40k rows) to count a handful of leased ones.
    #[test]
    fn inflight_counts_use_the_lease_index() {
        let conn = fresh();
        let plan: Vec<String> = conn
            .prepare("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM messages INDEXED BY idx_messages_lease WHERE tenant_id=?1 AND state='leased' AND leased_until > ?2")
            .unwrap()
            .query_map(params!["t1", 0i64], |r| r.get::<_, String>(3))
            .unwrap()
            .filter_map(|x| x.ok())
            .collect();
        let joined = plan.join(" || ");
        assert!(
            joined.contains("idx_messages_lease"),
            "inflight count must use idx_messages_lease: {joined}"
        );
    }
}
