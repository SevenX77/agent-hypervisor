use ah::db;
use rusqlite::{Connection, params};

fn setup_db_antigravity() -> (tempfile::NamedTempFile, db::Db) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let db = db::init(file.path()).unwrap();
    {
        let conn = db.conn();
        conn.execute(
            "INSERT INTO projects (id, absolute_path) VALUES ('p1', '/tmp/pr_antigravity')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1', 'p1', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agents (id, session_id, provider, state, pid) VALUES ('a1', 's1', 'antigravity', 'BUSY', 123)",
            [],
        )
        .unwrap();
    }
    (file, db)
}

fn insert_dispatched_job(conn: &Connection, job_id: &str) {
    conn.execute(
        "INSERT INTO jobs (
            id, agent_id, request_id, prompt_text, status, dispatched_at, dispatched_at_seq_id,
            requires_physical_evidence, requires_test_evidence
        ) VALUES (?, 'a1', NULL, 'test cargo', 'DISPATCHED', unixepoch(), 0, 0, 0)",
        params![job_id],
    )
    .unwrap();
}

fn get_agent_state_and_job_status(db: &db::Db, job_id: &str) -> (String, String) {
    db.conn()
        .query_row(
            "SELECT agents.state, jobs.status FROM agents JOIN jobs ON jobs.agent_id = agents.id WHERE agents.id = 'a1' AND jobs.id = ?",
            params![job_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

#[tokio::test]
async fn antigravity_deferred_background_cargo_reply_does_not_complete_from_log() {
    let (_file, db) = setup_db_antigravity();
    {
        let conn = db.conn();
        insert_dispatched_job(&conn, "job1");
    }

    // Call mark_agent_idle_log_event_outcome with deferred cargo/waiting reply
    let reply = "等后台 cargo test 跑完，我再看最终结果。";
    let outcome = db::state_machine::mark_agent_idle_log_event_outcome(
        db.clone(),
        "a1".to_string(),
        "antigravity".to_string(),
        Some(reply.to_string()),
        "some_path.jsonl".to_string(),
        100,
        None,
        "".to_string(),
        None,
    )
    .await
    .unwrap();

    // The first log candidate must not complete the job.
    assert_eq!(outcome.changes, 0);
    assert_eq!(outcome.affected_job, None);

    // Check agent state and job status: job stays DISPATCHED
    let (agent_state, job_status) = get_agent_state_and_job_status(&db, "job1");
    assert_eq!(agent_state, "BUSY");
    assert_eq!(job_status, "DISPATCHED");

    // A deferral event must be recorded.
    let count: i64 = db.conn()
        .query_row(
            "SELECT count(*) FROM events WHERE agent_id = 'a1' AND event_type = 'completion_deferred'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);

    // Nudge is directly returned in the outcome
    let nudge = outcome
        .deferred_nudge
        .expect("Should have returned a deferred nudge");
    assert!(nudge.contains("The job is still open. Wait for the background command to finish"));
}

#[tokio::test]
async fn antigravity_final_test_result_completes_after_deferred_candidate() {
    let (_file, db) = setup_db_antigravity();
    {
        let conn = db.conn();
        insert_dispatched_job(&conn, "job3");
    }

    // 1. Send deferred reply
    let reply1 = "等后台 cargo test 跑完，我再看最终结果。";
    let outcome1 = db::state_machine::mark_agent_idle_log_event_outcome(
        db.clone(),
        "a1".to_string(),
        "antigravity".to_string(),
        Some(reply1.to_string()),
        "some_path.jsonl".to_string(),
        100,
        None,
        "".to_string(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(outcome1.changes, 0);
    assert_eq!(outcome1.affected_job, None);
    assert!(outcome1.deferred_nudge.is_some());

    // 2. Send final reply containing "5 passed"
    let reply2 = "test result: ok. 5 passed\n物化形状核对全绿\n";
    let outcome2 = db::state_machine::mark_agent_idle_log_event_outcome(
        db.clone(),
        "a1".to_string(),
        "antigravity".to_string(),
        Some(reply2.to_string()),
        "some_path.jsonl".to_string(),
        200,
        None,
        "".to_string(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(outcome2.changes, 1);
    assert_eq!(outcome2.affected_job, Some("job3".to_string()));
    assert!(outcome2.deferred_nudge.is_none());

    let (agent_state, job_status) = get_agent_state_and_job_status(&db, "job3");
    assert_eq!(agent_state, "IDLE");
    assert_eq!(job_status, "COMPLETED");
}
