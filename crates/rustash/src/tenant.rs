//! Tenant identity and the only SQL that reads the `tenants` table.
//!
//! Both the HTTP auth layer and the dispatch engine need the same four columns. Keeping
//! the row mapping here means a schema change has one place to land: neither the
//! adapter nor the orchestrator owns a hand-written tenant `SELECT`.
//!
//! `delivery` signs with `signing_key_current`; `http::auth` resolves a bearer token
//! through [`find_by_token_hash`]. The token-to-hash rule itself stays in `ids`.

use rusqlite::Row;

use crate::db::Db;

#[derive(Debug, Clone)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub signing_key_current: String,
    pub signing_key_next: String,
}

fn from_row(r: &Row<'_>) -> rusqlite::Result<Tenant> {
    Ok(Tenant {
        id: r.get(0)?,
        name: r.get(1)?,
        signing_key_current: r.get(2)?,
        signing_key_next: r.get(3)?,
    })
}

const SELECT_ALL: &str = "SELECT id, name, signing_key_current, signing_key_next FROM tenants";

const SELECT_BY_TOKEN_HASH: &str =
    "SELECT id, name, signing_key_current, signing_key_next FROM tenants WHERE token_hash = ?1";

/// Tenants with at least one due row, found in ONE statement.
///
/// Iterating the plain tenant list makes every drain run a full claim transaction
/// (token bucket rollover, paused-set loads, candidate scan, inflight counts) for each
/// tenant, including the idle ones. At 1001 tenants that was measured at ~8k statements
/// per drain: CPU 4.2% -> 24.2% and publish p99 2.2 ms -> 76.5 ms purely from tenants
/// with nothing to do. Both `EXISTS` probes are index seeks, so an idle tenant costs
/// microseconds instead of a transaction.
const SELECT_WITH_DUE_WORK: &str = r#"SELECT t.id, t.name, t.signing_key_current, t.signing_key_next
         FROM tenants t
        WHERE EXISTS (SELECT 1 FROM messages m INDEXED BY idx_messages_fifo
                       WHERE m.tenant_id = t.id AND m.state = 'pending' AND m.not_before <= ?1)
           OR EXISTS (SELECT 1 FROM messages m INDEXED BY idx_messages_lease
                       WHERE m.tenant_id = t.id AND m.state = 'leased' AND m.leased_until <= ?1)"#;

/// Every tenant (admin / key-rotation surfaces).
pub fn load_all(db: &Db) -> Vec<Tenant> {
    let conn = db.conn.lock().unwrap();
    let mut stmt = match conn.prepare_cached(SELECT_ALL) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stmt.query_map([], from_row)
        .map(|rows| rows.filter_map(Result::ok).collect())
        .unwrap_or_default()
}

/// The authed-request lookup. `hash_token` belongs to the caller.
pub fn find_by_token_hash(db: &Db, token_hash: &str) -> Option<Tenant> {
    let conn = db.conn.lock().unwrap();
    let mut stmt = conn.prepare_cached(SELECT_BY_TOKEN_HASH).ok()?;
    stmt.query_row([token_hash], from_row).ok()
}

/// The drain probe: only tenants that can actually produce work this tick.
pub fn load_with_due_work(db: &Db, now: i64) -> Vec<Tenant> {
    let conn = db.conn.lock().unwrap();
    let mut stmt = match conn.prepare_cached(SELECT_WITH_DUE_WORK) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    stmt.query_map([now], from_row)
        .map(|rows| rows.filter_map(Result::ok).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{apply_migrations, open_db};
    use crate::ids::hash_token;

    fn seeded() -> Db {
        let db = open_db(":memory:");
        {
            let mut c = db.conn.lock().unwrap();
            apply_migrations(&mut c).unwrap();
            let now = crate::time::now_ms();
            for (id, name, token, key) in
                [("t1", "one", "tok-1", "k1"), ("t2", "two", "tok-2", "k2")]
            {
                c.execute(
                    "INSERT INTO tenants (id, name, token_hash, signing_key_current, signing_key_next, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                    rusqlite::params![id, name, hash_token(token), key, "next", now],
                )
                .unwrap();
            }
        }
        db
    }

    #[test]
    fn token_lookup_and_full_list_share_one_row_mapping() {
        let db = seeded();
        let mut all = load_all(&db);
        all.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, "t1");
        assert_eq!(all[0].name, "one");

        let t = find_by_token_hash(&db, &hash_token("tok-1")).expect("known token resolves");
        assert_eq!(t.id, "t1");
        assert_eq!(t.signing_key_current, "k1");
        assert_eq!(t.signing_key_next, "next");
        assert!(find_by_token_hash(&db, &hash_token("tok-missing")).is_none());
    }

    #[test]
    fn due_work_probe_excludes_idle_tenants() {
        let db = seeded();
        let now = crate::time::now_ms();
        assert!(
            load_with_due_work(&db, now).is_empty(),
            "no messages means no claim transaction"
        );
        {
            let c = db.conn.lock().unwrap();
            c.execute(
                "INSERT INTO messages (id, tenant_id, url, max_retries, not_before, created_at)
                 VALUES ('m1', 't2', 'http://example.invalid/', 3, ?1, ?1)",
                rusqlite::params![now],
            )
            .unwrap();
        }
        let due = load_with_due_work(&db, now);
        assert_eq!(due.len(), 1, "only the tenant with a due row");
        assert_eq!(due[0].id, "t2");

        // A future-due row is not work yet.
        {
            let c = db.conn.lock().unwrap();
            c.execute(
                "UPDATE messages SET not_before = ?1 WHERE id = 'm1'",
                rusqlite::params![now + 60_000],
            )
            .unwrap();
        }
        assert!(load_with_due_work(&db, now).is_empty());
    }

    #[test]
    fn expired_lease_counts_as_due_work() {
        let db = seeded();
        let now = crate::time::now_ms();
        {
            let c = db.conn.lock().unwrap();
            c.execute(
                "INSERT INTO messages (id, tenant_id, state, url, max_retries, not_before, leased_until, created_at)
                 VALUES ('m2', 't1', 'leased', 'http://example.invalid/', 3, ?1, ?2, ?1)",
                rusqlite::params![now, now - 1],
            )
            .unwrap();
        }
        let due = load_with_due_work(&db, now);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "t1");
    }
}
