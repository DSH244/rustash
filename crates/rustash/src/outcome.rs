//! Post-delivery state transitions. This is the boundary between the orchestrator
//! (`engine`, which decides *what* a response means) and the persistence primitives
//! (`store`, which owns the `messages` table). The engine never writes SQL; this module
//! composes fenced primitives into the two compound transitions a delivery can reach:
//! cancellation winning the race, and success deleting the row plus enqueueing its
//! callback atomically.

use rusqlite::Connection;

use crate::callback::build_callback_row;
use crate::model::{DeliveryResponse, MessageRow};
use crate::store::{delete_leased, insert_message};

/// Outcome of the cancellation probe that runs after the HTTP response arrives.
///
/// `LostFence` also means "stop classifying": the original dispatcher returned early for
/// ANY cancel_requested row, whether or not its fenced delete succeeded. A stale worker
/// that already lost the fence must not retry, DLQ or delete a newer attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelRace {
    /// No cancel request exists; classify the response normally.
    NotRequested,
    /// The leased row was deleted by us; a CANCELLED event is due.
    Deleted,
    /// A cancel request existed but this worker lost the fence. Still stop here.
    LostFence,
}

const CANCEL_REQUESTED_SQL: &str =
    "SELECT 1 FROM messages WHERE id = ?1 AND state = 'cancel_requested'";

/// A cancel that arrived while the request was in flight wins regardless of the HTTP
/// outcome. This keeps the original `SELECT id` probe shape (no tenant scoping): the
/// dispatch row and the cancel request come from the same single-writer database, and a
/// tenant-scoped lookup would change the exact set of rows the probe can see.
///
/// The probe is a stable SQL string so `prepare_cached` can reuse it on every delivery.
pub fn check_cancellation_race(conn: &Connection, row: &MessageRow) -> CancelRace {
    let requested = match conn.prepare_cached(CANCEL_REQUESTED_SQL) {
        Ok(mut stmt) => stmt.exists([row.id.as_str()]).unwrap_or(false),
        Err(_) => return CancelRace::NotRequested,
    };
    if !requested {
        return CancelRace::NotRequested;
    }
    match delete_leased(conn, &row.id, row.retried) {
        Ok(Some(_)) => CancelRace::Deleted,
        _ => CancelRace::LostFence,
    }
}

/// Fenced success: delete the leased row and insert the success callback in ONE
/// transaction. Returns whether this worker still owned the row.
///
/// `delete_leased` is fenced on `(id, retried, state IN leased/cancel_requested)`, so a
/// reclaimed twin cannot insert a second callback. The callback row is built ONLY after
/// the fence hit, exactly as before — no id is minted for a lost race. A callback insert
/// error is logged but does not turn a successful delivery into a retry: the original
/// path committed regardless and still recorded the DELIVERED event.
pub fn complete_delivery(
    conn: &mut Connection,
    row: &MessageRow,
    response: &DeliveryResponse,
) -> bool {
    let Ok(tx) = conn.transaction() else {
        return false;
    };
    let deleted = match delete_leased(&tx, &row.id, row.retried) {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(
                message_id = %row.id,
                error = %e,
                "fenced delete failed after successful delivery"
            );
            false
        }
    };
    if deleted {
        if let Some(cb_url) = &row.callback_url {
            let cb = build_callback_row(
                row,
                "callback",
                cb_url,
                row.callback_opts.as_ref(),
                &Some(response.clone()),
                None,
            );
            if let Err(e) = insert_message(&tx, &cb) {
                tracing::warn!(
                    message_id = %row.id,
                    error = %e,
                    "success callback could not be persisted"
                );
            }
        }
    }
    let _ = tx.commit();
    deleted
}

/// Byte-level contract seam: read-only cancellation probe (id + state only, no tenant
/// scope). It shares `CANCEL_REQUESTED_SQL` with `check_cancellation_race`, so the dispatch
/// race and the external assertion can never disagree on the predicate.
pub fn is_cancel_requested(conn: &Connection, id: &str) -> bool {
    match conn.prepare_cached(CANCEL_REQUESTED_SQL) {
        Ok(mut stmt) => stmt.exists([id]).unwrap_or(false),
        Err(_) => false,
    }
}

/// Byte-level contract seam: fenced success with a caller-supplied callback row, in ONE
/// transaction. `complete_delivery` is the engine-facing wrapper that builds the callback
/// only after the fence hits (no id minted on a lost race); this lower-level form lets the
/// contract assert the fence without depending on callback construction. The callback
/// insert remains best-effort: a failure is logged, the fenced delete still commits, and a
/// completed send is not re-run after lease expiry. Returns whether the fence was won.
pub fn delete_leased_with_callback(
    conn: &mut Connection,
    id: &str,
    leased_retried: i64,
    callback: Option<&MessageRow>,
) -> rusqlite::Result<bool> {
    let tx = conn.transaction()?;
    let deleted = match delete_leased(&tx, id, leased_retried) {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(message_id = %id, error = %e, "fenced delete failed");
            false
        }
    };
    if deleted {
        if let Some(cb) = callback {
            if let Err(e) = insert_message(&tx, cb) {
                tracing::warn!(
                    message_id = %id,
                    error = %e,
                    "success callback could not be persisted"
                );
            }
        }
    }
    tx.commit()?;
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{apply_migrations, open_db};
    use crate::store::{insert_message as insert, select_message_by_id};

    fn row(id: &str) -> MessageRow {
        MessageRow {
            id: id.into(),
            tenant_id: "t1".into(),
            kind: "message".into(),
            state: "leased".into(),
            url: "http://example.invalid/".into(),
            topic_name: None,
            endpoint_name: None,
            queue_name: None,
            schedule_id: None,
            method: "POST".into(),
            headers: serde_json::json!({}),
            body: Some(b"hi".to_vec()),
            callback_url: None,
            callback_opts: None,
            failure_callback_url: None,
            failure_callback_opts: None,
            max_retries: 3,
            retry_delay_expr: None,
            timeout_ms: None,
            not_before: 1,
            flow_control_key: None,
            fc_parallelism: None,
            fc_rate: None,
            fc_period_secs: None,
            labels: vec![],
            redact_fields: None,
            caller_ip: None,
            retried: 0,
            leased_until: None,
            last_error: None,
            created_at: 1,
        }
    }

    fn response() -> DeliveryResponse {
        DeliveryResponse {
            status: Some(200),
            headers: None,
            body: Some(b"ok".to_vec()),
        }
    }

    fn db_with_rows(ids: &[&str], with_callback: bool) -> crate::db::Db {
        let db = open_db(":memory:");
        {
            let mut c = db.conn.lock().unwrap();
            apply_migrations(&mut c).unwrap();
            for id in ids {
                let mut r = row(id);
                if with_callback {
                    r.callback_url = Some("http://callback.invalid/".into());
                }
                insert(&c, &r).unwrap();
                c.execute("UPDATE messages SET state='leased' WHERE id=?1", [id])
                    .unwrap();
            }
        }
        db
    }

    #[test]
    fn no_cancel_request_leaves_the_row_for_classification() {
        let db = db_with_rows(&["m1"], false);
        let conn = db.conn.lock().unwrap();
        assert_eq!(
            check_cancellation_race(&conn, &row("m1")),
            CancelRace::NotRequested
        );
        assert!(select_message_by_id(&conn, "t1", "m1").unwrap().is_some());
    }

    #[test]
    fn cancel_request_wins_and_is_fenced_by_retried() {
        let db = db_with_rows(&["m1"], false);
        {
            let c = db.conn.lock().unwrap();
            c.execute(
                "UPDATE messages SET state='cancel_requested' WHERE id='m1'",
                [],
            )
            .unwrap();
        }
        let conn = db.conn.lock().unwrap();
        assert_eq!(
            check_cancellation_race(&conn, &row("m1")),
            CancelRace::Deleted
        );
        assert!(select_message_by_id(&conn, "t1", "m1").unwrap().is_none());
    }

    #[test]
    fn stale_retried_turns_a_cancel_request_into_a_lost_fence() {
        let db = db_with_rows(&["m1"], false);
        {
            let c = db.conn.lock().unwrap();
            c.execute(
                "UPDATE messages SET state='cancel_requested', retried=1 WHERE id='m1'",
                [],
            )
            .unwrap();
        }
        let conn = db.conn.lock().unwrap();
        assert_eq!(
            check_cancellation_race(&conn, &row("m1")),
            CancelRace::LostFence,
            "the engine must stop classifying a response owned by a reclaimed twin"
        );
        assert!(select_message_by_id(&conn, "t1", "m1").unwrap().is_some());
    }

    #[test]
    fn cancelled_row_from_another_tenant_is_still_honored() {
        // Preserves the exact original probe: it keyed on id + state only, not tenant.
        let db = open_db(":memory:");
        {
            let mut c = db.conn.lock().unwrap();
            apply_migrations(&mut c).unwrap();
            let mut cross = row("m1");
            cross.tenant_id = "t2".into();
            cross.state = "cancel_requested".into();
            insert(&c, &cross).unwrap();
        }
        let conn = db.conn.lock().unwrap();
        assert_eq!(
            check_cancellation_race(&conn, &row("m1")),
            CancelRace::Deleted
        );
    }

    #[test]
    fn success_deletes_row_and_persists_callback_atomically() {
        let db = db_with_rows(&["m1"], true);
        // `complete_delivery` builds the callback from the leased row (as the original
        // dispatcher did), not from the deleted RETURNING row — so the passed row must
        // carry the callback_url that was present when the message was leased.
        let mut src = row("m1");
        src.callback_url = Some("http://callback.invalid/".into());
        {
            let mut conn = db.conn.lock().unwrap();
            assert!(complete_delivery(&mut conn, &src, &response()));
        }
        let conn = db.conn.lock().unwrap();
        assert!(select_message_by_id(&conn, "t1", "m1").unwrap().is_none());
        let callbacks: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE kind='callback'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(callbacks, 1);
    }

    #[test]
    fn stale_fence_inserts_no_callback_and_reports_false() {
        let db = db_with_rows(&["m1"], true);
        {
            let c = db.conn.lock().unwrap();
            c.execute("UPDATE messages SET retried=1 WHERE id='m1'", [])
                .unwrap();
        }
        let mut src = row("m1");
        src.callback_url = Some("http://callback.invalid/".into());
        {
            let mut conn = db.conn.lock().unwrap();
            assert!(!complete_delivery(&mut conn, &src, &response()));
        }
        let conn = db.conn.lock().unwrap();
        assert!(select_message_by_id(&conn, "t1", "m1").unwrap().is_some());
        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 1, "no callback row was minted for a lost fence");
    }
}
