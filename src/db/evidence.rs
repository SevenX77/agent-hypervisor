use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::db::schema::Evidence;
use crate::error::AhError;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

pub(crate) fn query_evidence_by_id_sync(
    conn: &Connection,
    evidence_id: &str,
) -> Result<Option<Evidence>, AhError> {
    conn.query_row(
        "SELECT id, agent_id, event_seq_id, status, job_id, evidence_type, subject_path, payload, created_at FROM evidence WHERE id = ?",
        params![evidence_id],
        |row| {
            Ok(Evidence {
                id: row.get(0)?,
                agent_id: row.get(1)?,
                event_seq_id: row.get(2)?,
                status: row.get(3)?,
                job_id: row.get(4)?,
                evidence_type: row.get(5)?,
                subject_path: row.get(6)?,
                payload: row.get(7)?,
                created_at: row.get(8)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query evidence by id", err))
}

pub(crate) fn insert_evidence_record_sync(
    conn: &Connection,
    agent_id: &str,
    job_id: Option<&str>,
    evidence_type: &str,
    subject_path: Option<&str>,
    payload: &Value,
) -> Result<String, AhError> {
    let evidence_id = format!("evi_{}", uuid::Uuid::new_v4().simple());
    conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'evidence_recorded', ?)",
        params![agent_id, payload.to_string()],
    )
    .map_err(|err| map_db_error("insert evidence record event", err))?;
    let event_seq_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO evidence (
            id, agent_id, event_seq_id, pane_bytes, failed_rules, status,
            job_id, evidence_type, subject_path, payload
        ) VALUES (?, ?, ?, X'', '[]', 'PENDING', ?, ?, ?, ?)",
        params![
            evidence_id,
            agent_id,
            event_seq_id,
            job_id,
            evidence_type,
            subject_path,
            payload.to_string()
        ],
    )
    .map_err(|err| map_db_error("insert evidence record", err))?;
    Ok(evidence_id)
}

pub(crate) fn has_job_evidence_sync(
    conn: &Connection,
    agent_id: &str,
    job_id: &str,
    evidence_types: &[&str],
) -> Result<bool, AhError> {
    for evidence_type in evidence_types {
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM evidence
                    WHERE agent_id = ? AND job_id = ? AND evidence_type = ?
                )",
                params![agent_id, job_id, evidence_type],
                |row| row.get(0),
            )
            .map_err(|err| map_db_error("query job evidence", err))?;
        if exists {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn has_job_evidence_for_path_sync(
    conn: &Connection,
    job_id: &str,
    evidence_type: &str,
    subject_path: &str,
) -> Result<bool, AhError> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM evidence
            WHERE job_id = ? AND evidence_type = ? AND subject_path = ?
        )",
        params![job_id, evidence_type, subject_path],
        |row| row.get(0),
    )
    .map_err(|err| map_db_error("query job evidence by path", err))
}

pub(crate) fn update_evidence_status_sync(
    conn: &Connection,
    evidence_id: &str,
    status: &str,
    l3_asserted_state: Option<&str>,
) -> Result<usize, AhError> {
    conn.execute(
        "UPDATE evidence SET status = ?, l3_asserted_state = ? WHERE id = ?",
        params![status, l3_asserted_state, evidence_id],
    )
    .map_err(|err| map_db_error("update evidence status", err))
}

pub(crate) fn discard_evidence_sync(db: &Db, evidence_id: &str) -> Result<(), AhError> {
    let conn = db.conn();
    if query_evidence_by_id_sync(&conn, evidence_id)?.is_none() {
        return Err(AhError::DbEvidenceNotFound {
            details: format!("evidence_id={evidence_id}"),
        });
    }
    update_evidence_status_sync(&conn, evidence_id, "DISCARDED", None)?;
    Ok(())
}

pub async fn query_evidence_by_id(
    db: Db,
    evidence_id: String,
) -> Result<Option<Evidence>, AhError> {
    spawn_db("evidence::query_evidence_by_id", move || {
        let conn = db.conn();
        query_evidence_by_id_sync(&conn, &evidence_id)
    })
    .await
}

pub async fn insert_evidence_record(
    db: Db,
    agent_id: String,
    job_id: Option<String>,
    evidence_type: String,
    subject_path: Option<String>,
    payload: Value,
) -> Result<String, AhError> {
    spawn_db("evidence::insert_evidence_record", move || {
        let conn = db.conn();
        insert_evidence_record_sync(
            &conn,
            &agent_id,
            job_id.as_deref(),
            &evidence_type,
            subject_path.as_deref(),
            &payload,
        )
    })
    .await
}

pub async fn has_job_evidence_for_path(
    db: Db,
    job_id: String,
    evidence_type: String,
    subject_path: String,
) -> Result<bool, AhError> {
    spawn_db("evidence::has_job_evidence_for_path", move || {
        let conn = db.fresh_conn()?;
        has_job_evidence_for_path_sync(&conn, &job_id, &evidence_type, &subject_path)
    })
    .await
}

pub async fn update_evidence_status(
    db: Db,
    evidence_id: String,
    status: String,
    l3_asserted_state: Option<String>,
) -> Result<usize, AhError> {
    spawn_db("evidence::update_evidence_status", move || {
        let conn = db.conn();
        update_evidence_status_sync(&conn, &evidence_id, &status, l3_asserted_state.as_deref())
    })
    .await
}

pub async fn discard_evidence(db: Db, evidence_id: String) -> Result<(), AhError> {
    spawn_db("evidence::discard_evidence", move || {
        discard_evidence_sync(&db, &evidence_id)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        discard_evidence_sync, has_job_evidence_sync, insert_evidence_record_sync,
        query_evidence_by_id_sync, update_evidence_status_sync,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::mark_agent_unknown_sync;
    use crate::db::{Db, init};

    fn with_test_db_handle<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    #[test]
    fn test_evidence_helpers() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", "BUSY", Some(1)).unwrap();
            }
            mark_agent_unknown_sync(
                db,
                "a1",
                "PTY_MARKER_TIMEOUT",
                b"pane".to_vec(),
                serde_json::json!(["rule"]),
            )
            .unwrap();
            let evidence_id: String = db
                .conn()
                .query_row("SELECT id FROM evidence WHERE agent_id = 'a1'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            let evidence = query_evidence_by_id_sync(&db.conn(), &evidence_id)
                .unwrap()
                .unwrap();
            assert_eq!(evidence.agent_id, "a1");
            assert_eq!(evidence.status, "PENDING");
            assert_eq!(evidence.job_id, None);
            let changes =
                update_evidence_status_sync(&db.conn(), &evidence_id, "REVIEWED", Some("IDLE"))
                    .unwrap();
            assert_eq!(changes, 1);
            discard_evidence_sync(db, &evidence_id).unwrap();
        });
    }

    #[test]
    fn test_insert_evidence_record_nullable_job_id_and_lookup() {
        with_test_db_handle(|db| {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "bash", "BUSY", Some(1)).unwrap();
            conn.execute(
                "INSERT INTO jobs (id, agent_id, prompt_text, status) VALUES ('job1', 'a1', 'p', 'DISPATCHED')",
                [],
            )
            .unwrap();
            let null_job_evidence = insert_evidence_record_sync(
                &conn,
                "a1",
                None,
                "diff_generated",
                Some("src/main.rs"),
                &serde_json::json!({"source": "read-first"}),
            )
            .unwrap();
            let job_evidence = insert_evidence_record_sync(
                &conn,
                "a1",
                Some("job1"),
                "test_passed",
                None,
                &serde_json::json!({"cmd": "cargo test"}),
            )
            .unwrap();

            let null_record = query_evidence_by_id_sync(&conn, &null_job_evidence)
                .unwrap()
                .unwrap();
            assert_eq!(null_record.job_id, None);
            assert_eq!(null_record.evidence_type.as_deref(), Some("diff_generated"));
            let job_record = query_evidence_by_id_sync(&conn, &job_evidence)
                .unwrap()
                .unwrap();
            assert_eq!(job_record.job_id.as_deref(), Some("job1"));
            assert!(has_job_evidence_sync(&conn, "a1", "job1", &["test_passed"]).unwrap());
            assert!(!has_job_evidence_sync(&conn, "a1", "job1", &["diff_generated"]).unwrap());
        });
    }
}
