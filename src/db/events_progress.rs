use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::error::AhError;
use rusqlite::{TransactionBehavior, params};
use serde_json::Value;

pub(crate) fn record_send_progress_sync(
    db: &Db,
    seq_id: i64,
    final_payload: &Value,
    agent_id: &str,
    write_succeeded: bool,
) -> Result<(), AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin send.update", err))?;
    tx.execute(
        "UPDATE events SET payload = ? WHERE seq_id = ?",
        params![final_payload.to_string(), seq_id],
    )
    .map_err(|err| map_db_error("update send event", err))?;
    let _ = (agent_id, write_succeeded);
    tx.commit()
        .map_err(|err| map_db_error("commit send.update", err))
}

pub async fn record_send_progress(
    db: Db,
    seq_id: i64,
    final_payload: Value,
    agent_id: String,
    write_succeeded: bool,
) -> Result<(), AhError> {
    spawn_db("events_progress::record_send_progress", move || {
        record_send_progress_sync(&db, seq_id, &final_payload, &agent_id, write_succeeded)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::record_send_progress_sync;
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::STATE_WAITING_FOR_ACK;

    fn seed_agent(conn: &rusqlite::Connection) {
        insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
    }

    #[test]
    fn test_record_send_progress_updates_event_without_forcing_busy() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        let (sent_seq, failed_seq, crashed_seq) = {
            let conn = db.conn();
            seed_agent(&conn);
            insert_agent_sync(&conn, "a2", "s1", "bash", "IDLE", Some(2)).unwrap();
            insert_agent_sync(&conn, "a3", "s1", "bash", "CRASHED", Some(3)).unwrap();
            (
                insert_event_sync(&conn, "a1", Some("r1"), "command_received", "{}").unwrap(),
                insert_event_sync(&conn, "a2", Some("r2"), "command_received", "{}").unwrap(),
                insert_event_sync(&conn, "a3", Some("r3"), "command_received", "{}").unwrap(),
            )
        };
        record_send_progress_sync(
            &db,
            sent_seq,
            &serde_json::json!({"status":"SENT"}),
            "a1",
            true,
        )
        .unwrap();
        record_send_progress_sync(
            &db,
            failed_seq,
            &serde_json::json!({"status":"FAILED"}),
            "a2",
            false,
        )
        .unwrap();
        record_send_progress_sync(
            &db,
            crashed_seq,
            &serde_json::json!({"status":"SENT"}),
            "a3",
            true,
        )
        .unwrap();
        let states: Vec<String> = ["a1", "a2", "a3"]
            .into_iter()
            .map(|id| {
                db.conn()
                    .query_row("SELECT state FROM agents WHERE id=?", [id], |row| {
                        row.get(0)
                    })
                    .unwrap()
            })
            .collect();
        assert_eq!(states, ["IDLE", "IDLE", "CRASHED"]);

        let sent_payload: String = db
            .conn()
            .query_row(
                "SELECT payload FROM events WHERE seq_id = ?",
                [sent_seq],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            sent_payload,
            serde_json::json!({"status":"SENT"}).to_string()
        );
    }

    #[test]
    fn test_record_send_progress_keeps_waiting_for_ack_on_send_success() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        let seq_id = {
            let conn = db.conn();
            insert_session_sync(&conn, "s_ack", "p_ack", "/tmp/ack").unwrap();
            insert_agent_sync(
                &conn,
                "a_ack",
                "s_ack",
                "bash",
                STATE_WAITING_FOR_ACK,
                Some(1),
            )
            .unwrap();
            insert_event_sync(&conn, "a_ack", Some("r_ack"), "command_received", "{}").unwrap()
        };

        record_send_progress_sync(
            &db,
            seq_id,
            &serde_json::json!({"status":"SENT"}),
            "a_ack",
            true,
        )
        .unwrap();

        let (state, payload): (String, String) = db
            .conn()
            .query_row(
                "SELECT agents.state, events.payload \
                 FROM agents JOIN events ON events.agent_id = agents.id \
                 WHERE agents.id = 'a_ack'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, STATE_WAITING_FOR_ACK);
        assert_eq!(payload, serde_json::json!({"status":"SENT"}).to_string());
    }
}
