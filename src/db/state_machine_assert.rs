use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::db::state_machine::{STATE_UNKNOWN, STATE_WAITING_FOR_ACK};
use crate::error::AhError;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::json;

#[derive(Debug)]
pub struct AssertStateOutcome {
    pub state_change_seq_id: i64,
}

pub(crate) fn assert_state_to_idle_sync(
    db: &Db,
    agent_id: &str,
    evidence_id: &str,
) -> Result<AssertStateOutcome, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin assert_state", err))?;

    let evidence_agent_id = tx
        .query_row(
            "SELECT agent_id FROM evidence WHERE id = ?",
            params![evidence_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query assert evidence", err))?
        .ok_or_else(|| AhError::DbEvidenceNotFound {
            details: format!("evidence_id={evidence_id}"),
        })?;
    if evidence_agent_id != agent_id {
        return Err(AhError::DbEvidenceNotFound {
            details: "agent_id mismatch".into(),
        });
    }

    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query assert agent", err))?;
    let Some((current_state, state_version)) = current else {
        return Err(AhError::AgentNotFound(agent_id.to_string()));
    };
    if current_state != STATE_UNKNOWN && current_state != STATE_WAITING_FOR_ACK {
        return Err(AhError::AgentWrongState { current_state });
    }

    let changes = tx
        .execute(
            "UPDATE agents SET state='IDLE', sub_state='Asserted', state_version=state_version+1, updated_at=unixepoch() WHERE id=? AND state IN ('UNKNOWN', 'WAITING_FOR_ACK') AND state_version=?",
            params![agent_id, state_version],
        )
        .map_err(|err| map_db_error("assert agent state", err))?;
    if changes == 0 {
        let current_state = tx
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("requery assert agent", err))?
            .unwrap_or_else(|| "MISSING".to_string());
        return Err(AhError::AgentWrongState { current_state });
    }

    tx.execute(
        "UPDATE evidence SET status = 'REVIEWED', l3_asserted_state = 'IDLE' WHERE id = ?",
        params![evidence_id],
    )
    .map_err(|err| map_db_error("update assert evidence", err))?;
    let payload = json!({
        "from": current_state,
        "to": "IDLE",
        "sub_state": "Asserted",
        "reason": "L3_ASSERTED",
        "evidence_id": evidence_id,
    })
    .to_string();
    tx.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
        params![agent_id, payload],
    )
    .map_err(|err| map_db_error("insert assert state_change", err))?;
    let state_change_seq_id = tx.last_insert_rowid();
    tx.commit()
        .map_err(|err| map_db_error("commit assert_state", err))?;

    Ok(AssertStateOutcome {
        state_change_seq_id,
    })
}

pub async fn assert_state_to_idle(
    db: Db,
    agent_id: String,
    evidence_id: String,
) -> Result<AssertStateOutcome, AhError> {
    let result = spawn_db("state_machine_assert::assert_state_to_idle", move || {
        assert_state_to_idle_sync(&db, &agent_id, &evidence_id)
    })
    .await;
    if result.is_ok() {
        crate::orchestrator::wake_up();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::assert_state_to_idle_sync;
    use crate::db::agents::insert_agent_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::{STATE_IDLE, STATE_WAITING_FOR_ACK, mark_agent_unknown_sync};
    use crate::db::{Db, init};

    fn with_test_db_handle<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_unknown_with_evidence(db: &Db, agent_id: &str) -> String {
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_assert", "p_assert", "/tmp/assert").unwrap();
            insert_agent_sync(&conn, agent_id, "s_assert", "bash", "BUSY", Some(1)).unwrap();
        }
        mark_agent_unknown_sync(
            db,
            agent_id,
            "PTY_MARKER_TIMEOUT",
            b"pane".to_vec(),
            serde_json::json!(["rule"]),
        )
        .unwrap();
        db.conn()
            .query_row(
                "SELECT id FROM evidence WHERE agent_id = ?",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn test_assert_state_to_idle_tx_cases() {
        with_test_db_handle(|db| {
            let evidence_id = seed_unknown_with_evidence(db, "a_assert");
            assert!(matches!(
                assert_state_to_idle_sync(db, "a_assert", "missing").unwrap_err(),
                crate::error::AhError::DbEvidenceNotFound { .. }
            ));
            {
                let conn = db.conn();
                insert_agent_sync(&conn, "a_other", "s_assert", "bash", "UNKNOWN", Some(2))
                    .unwrap();
            }
            assert!(matches!(
                assert_state_to_idle_sync(db, "a_other", &evidence_id).unwrap_err(),
                crate::error::AhError::DbEvidenceNotFound { .. }
            ));
            db.conn()
                .execute("UPDATE agents SET state='BUSY' WHERE id='a_assert'", [])
                .unwrap();
            assert!(matches!(
                assert_state_to_idle_sync(db, "a_assert", &evidence_id).unwrap_err(),
                crate::error::AhError::AgentWrongState { .. }
            ));
            db.conn()
                .execute("UPDATE agents SET state='UNKNOWN' WHERE id='a_assert'", [])
                .unwrap();
            let outcome = assert_state_to_idle_sync(db, "a_assert", &evidence_id).unwrap();
            assert!(outcome.state_change_seq_id > 0);
            let (state, sub_state, evidence_status): (String, Option<String>, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.sub_state, evidence.status FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id='a_assert' AND evidence.id=?",
                    [evidence_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(state, "IDLE");
            assert_eq!(sub_state.as_deref(), Some("Asserted"));
            assert_eq!(evidence_status, "REVIEWED");
        });
    }

    #[test]
    fn test_assert_state_to_idle_accepts_waiting_for_ack() {
        with_test_db_handle(|db| {
            let evidence_id = seed_unknown_with_evidence(db, "a_ack_assert");
            db.conn()
                .execute(
                    "UPDATE agents SET state = ? WHERE id = 'a_ack_assert'",
                    [STATE_WAITING_FOR_ACK],
                )
                .unwrap();

            let outcome = assert_state_to_idle_sync(db, "a_ack_assert", &evidence_id).unwrap();
            let (state, sub_state, evidence_status, payload): (
                String,
                Option<String>,
                String,
                String,
            ) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.sub_state, evidence.status, events.payload \
                     FROM agents \
                     JOIN evidence ON evidence.agent_id = agents.id \
                     JOIN events ON events.seq_id = ? \
                     WHERE agents.id='a_ack_assert' AND evidence.id=?",
                    rusqlite::params![outcome.state_change_seq_id, evidence_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(state, STATE_IDLE);
            assert_eq!(sub_state.as_deref(), Some("Asserted"));
            assert_eq!(evidence_status, "REVIEWED");
            assert_eq!(payload["from"], STATE_WAITING_FOR_ACK);
            assert_eq!(payload["to"], STATE_IDLE);
        });
    }
}
