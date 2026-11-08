//! Flow-control key configuration (engine/flow-control.js): every publish carrying an
//! fc spec refreshes the key row, except pinned dimensions, and a changed rate resets
//! the token bucket.

use rusqlite::{params, Connection};

use crate::headers::FlowControlSpec;
use crate::time::now_ms;

/// `upsertKeyConfig`. Unpinned dimensions adopt the message's values; pinned ones keep
/// the stored values (`period` rides the rate pin). An actually changed rate restarts
/// the bucket; an unchanged rate preserves it.
pub fn upsert_key_config(
    conn: &Connection,
    tenant_id: &str,
    spec: &FlowControlSpec,
) -> rusqlite::Result<()> {
    let now = now_ms();
    conn.execute(
        r#"INSERT INTO flow_control_keys
             (tenant_id, key, parallelism, rate, period_secs, pinned_parallelism, pinned_rate, paused, rate_count, rate_period_start, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, 0, 0, ?6, ?6)
           ON CONFLICT(tenant_id, key) DO UPDATE SET
             parallelism = CASE WHEN pinned_parallelism <> 0 THEN parallelism ELSE excluded.parallelism END,
             rate = CASE WHEN pinned_rate <> 0 THEN rate ELSE excluded.rate END,
             period_secs = CASE WHEN pinned_rate <> 0 THEN period_secs ELSE excluded.period_secs END,
             rate_count = CASE WHEN pinned_rate <> 0 OR rate IS excluded.rate THEN rate_count ELSE 0 END,
             rate_period_start = CASE WHEN pinned_rate <> 0 OR rate IS excluded.rate THEN rate_period_start ELSE excluded.rate_period_start END,
             updated_at = excluded.updated_at"#,
        params![
            tenant_id,
            spec.key,
            spec.parallelism,
            spec.rate,
            spec.period_secs,
            now
        ],
    )?;
    Ok(())
}

/// Lazy token-bucket rollover. Called inside the lease transaction before ranking.
pub fn rollover_rate_buckets(conn: &Connection, tenant_id: &str, now: i64) -> rusqlite::Result<()> {
    conn.execute(
        r#"UPDATE flow_control_keys
              SET rate_count = 0, rate_period_start = ?2
            WHERE tenant_id = ?1
              AND rate IS NOT NULL
              AND rate_period_start + period_secs * 1000 <= ?2"#,
        params![tenant_id, now],
    )?;
    Ok(())
}

/// Spend `claimed` tokens from a key's bucket (no-op for a non-rate-limited key).
pub fn spend_rate(
    conn: &Connection,
    tenant_id: &str,
    key: &str,
    claimed: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        r#"UPDATE flow_control_keys
              SET rate_count = rate_count + ?3
            WHERE tenant_id = ?1 AND key = ?2 AND rate IS NOT NULL"#,
        params![tenant_id, key, claimed],
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct FcConfig {
    pub parallelism: Option<i64>,
    pub rate: Option<i64>,
    pub rate_count: i64,
}

pub fn config_for(conn: &Connection, tenant_id: &str, key: &str) -> Option<FcConfig> {
    conn.query_row(
        "SELECT parallelism, rate, rate_count FROM flow_control_keys WHERE tenant_id = ?1 AND key = ?2",
        params![tenant_id, key],
        |r| {
            Ok(FcConfig {
                parallelism: r.get(0)?,
                rate: r.get(1)?,
                rate_count: r.get(2)?,
            })
        },
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::apply_migrations;

    fn fresh() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_migrations(&mut conn).unwrap();
        conn
    }

    fn spec(
        key: &str,
        parallelism: Option<i64>,
        rate: Option<i64>,
        period: i64,
    ) -> FlowControlSpec {
        FlowControlSpec {
            key: key.into(),
            parallelism,
            rate,
            period_secs: period,
        }
    }

    #[test]
    fn insert_then_refresh_adopts_unpinned_dimensions() {
        let conn = fresh();
        upsert_key_config(&conn, "t1", &spec("orders", Some(1), Some(10), 1)).unwrap();
        let cfg = config_for(&conn, "t1", "orders").unwrap();
        assert_eq!(cfg.parallelism, Some(1));
        assert_eq!(cfg.rate, Some(10));

        upsert_key_config(&conn, "t1", &spec("orders", Some(4), Some(20), 2)).unwrap();
        let cfg = config_for(&conn, "t1", "orders").unwrap();
        assert_eq!(cfg.parallelism, Some(4));
        assert_eq!(cfg.rate, Some(20));
    }

    #[test]
    fn pinned_dimensions_are_not_overwritten() {
        let conn = fresh();
        upsert_key_config(&conn, "t1", &spec("k", Some(2), Some(5), 1)).unwrap();
        conn.execute(
            "UPDATE flow_control_keys SET pinned_parallelism=1, pinned_rate=1 WHERE tenant_id='t1' AND key='k'",
            [],
        )
        .unwrap();
        upsert_key_config(&conn, "t1", &spec("k", Some(9), Some(99), 3)).unwrap();
        let cfg = config_for(&conn, "t1", "k").unwrap();
        assert_eq!(cfg.parallelism, Some(2));
        assert_eq!(cfg.rate, Some(5));
    }

    #[test]
    fn changed_rate_resets_bucket_and_unchanged_preserves_it() {
        let conn = fresh();
        upsert_key_config(&conn, "t1", &spec("k", None, Some(5), 1)).unwrap();
        conn.execute(
            "UPDATE flow_control_keys SET rate_count=3 WHERE tenant_id='t1' AND key='k'",
            [],
        )
        .unwrap();
        // unchanged rate → bucket preserved
        upsert_key_config(&conn, "t1", &spec("k", None, Some(5), 1)).unwrap();
        assert_eq!(config_for(&conn, "t1", "k").unwrap().rate_count, 3);
        // changed rate → reset
        upsert_key_config(&conn, "t1", &spec("k", None, Some(6), 1)).unwrap();
        assert_eq!(config_for(&conn, "t1", "k").unwrap().rate_count, 0);
    }
}
