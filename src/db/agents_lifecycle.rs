use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::db::jobs::mark_dispatched_jobs_failed_for_agent_conn_sync;
use crate::db::recovery::{
    AgentRecoveryIntent, CapturedInterruptedJob, RecoveryIntentAction,
    persist_agent_recovery_intent_sync,
};
use crate::db::state_machine::{
    STATE_BUSY, STATE_IDLE, STATE_PROMPT_PENDING, STATE_SPAWNING, STATE_WAITING_FOR_ACK,
};
use crate::error::AhError;
use rusqlite::{OptionalExtension, params};

/// Mark one active agent as KILLED and emit a state_change event in the same transaction.
pub(crate) fn mark_agent_killed_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, AhError> {
    mark_agent_killed_sync_inner(db, agent_id, reason, false)
}

pub(crate) fn mark_agent_killed_for_master_death_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, AhError> {
    mark_agent_killed_sync_inner(db, agent_id, reason, true)
}

fn mark_agent_killed_sync_inner(
    db: &Db,
    agent_id: &str,
    reason: &str,
    capture_revive_intent: bool,
) -> Result<usize, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent killed", err))?;
    let previous_state = tx
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent state for killed", err))?;
    let cleanup_session_id = tx
        .query_row(
            "SELECT session_id FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent session for killed cleanup", err))?;
    if capture_revive_intent
        && matches!(
            previous_state.as_deref(),
            Some(state) if state != "CRASHED" && state != "KILLED"
        )
    {
        tracing::info!(
            agent_id,
            previous_state = ?previous_state,
            reason,
            "capturing recovery intent before master-death worker kill"
        );
        if let Err(err) =
            capture_recovery_intent_for_crash(&tx, agent_id, previous_state.as_deref(), reason)
        {
            tracing::warn!(
                agent_id,
                reason,
                error = %err,
                "failed to capture recovery intent before master-death worker kill"
            );
            return Err(err);
        }
    }
    let changes = tx
        .execute(
            "UPDATE agents SET state = 'KILLED', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state NOT IN ('CRASHED', 'KILLED')",
            params![agent_id],
        )
        .map_err(|err| map_db_error("mark agent killed", err))?;

    if changes == 1 {
        mark_dispatched_jobs_failed_for_agent_conn_sync(&tx, agent_id, reason)?;
        let payload = serde_json::json!({
            "from": previous_state.as_deref().unwrap_or("UNKNOWN"),
            "to": "KILLED",
            "reason": reason,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert killed state_change", err))?;
    }

    let cleanup_policy = if changes > 0 && capture_revive_intent {
        let provider = tx
            .query_row(
                "SELECT provider FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query master-death cleanup policy", err))?;
        provider
            .as_deref()
            .map(crashed_cleanup_policy)
            .unwrap_or(crate::agent_io::registry::RuntimeCleanupPolicy::Default)
    } else {
        crate::agent_io::registry::RuntimeCleanupPolicy::Default
    };

    if changes > 0 && capture_revive_intent {
        crate::agent_io::registry::cleanup_agent_runtime_resources_with_policy(
            agent_id,
            cleanup_session_id.as_deref(),
            cleanup_policy,
        );
    }
    tx.commit()
        .map_err(|err| map_db_error("commit mark agent killed", err))?;
    if changes > 0 && !capture_revive_intent {
        crate::agent_io::registry::cleanup_agent_runtime_resources_with_policy(
            agent_id,
            cleanup_session_id.as_deref(),
            cleanup_policy,
        );
    }
    if changes > 0 {
        notify_runtime_agent_changed();
    }
    Ok(changes)
}

/// Mark one active agent as CRASHED with exit metadata and emit state_change atomically.
pub(crate) fn mark_agent_crashed_with_exit_sync(
    db: &Db,
    agent_id: &str,
    exit_code: Option<i32>,
) -> Result<usize, AhError> {
    mark_agent_crashed_sync(db, agent_id, exit_code, "AGENT_UNEXPECTED_EXIT")
}

fn mark_agent_crashed_sync(
    db: &Db,
    agent_id: &str,
    exit_code: Option<i32>,
    error_code: &str,
) -> Result<usize, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent crashed", err))?;
    let previous_state = tx
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent state for crashed", err))?;
    let cleanup_session_id = tx
        .query_row(
            "SELECT session_id FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent session for crashed cleanup", err))?;
    let changes = tx
        .execute(
            "UPDATE agents SET state = 'CRASHED', exit_code = ?, error_code = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state NOT IN ('CRASHED', 'KILLED', 'PROMPT_PENDING')",
            params![exit_code, error_code, agent_id],
        )
        .map_err(|err| map_db_error("mark agent crashed", err))?;
    if changes == 0 && previous_state.as_deref() == Some(STATE_PROMPT_PENDING) {
        tracing::info!(
            agent_id,
            state = STATE_PROMPT_PENDING,
            error_code,
            impact = "prompt resolution remains pending; broad crash path did not override it",
            "mark agent crashed skipped prompt pending"
        );
    }

    if changes == 1 {
        let reason = if error_code == "AGENT_UNEXPECTED_EXIT" && exit_code.is_none() {
            "EXIT_CODE_UNAVAILABLE_NON_CHILD"
        } else {
            error_code
        };
        capture_recovery_intent_for_crash(&tx, agent_id, previous_state.as_deref(), reason)?;
        mark_dispatched_jobs_failed_for_agent_conn_sync(&tx, agent_id, reason)?;
        let payload = serde_json::json!({
            "from": previous_state.as_deref().unwrap_or("UNKNOWN"),
            "to": "CRASHED",
            "reason": reason,
            "exit_code": exit_code,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
            .map_err(|err| map_db_error("insert crashed state_change", err))?;
    }

    let cleanup_policy = if changes > 0 {
        let provider = tx
            .query_row(
                "SELECT provider FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query crashed cleanup policy", err))?;
        provider
            .as_deref()
            .map(crashed_cleanup_policy)
            .unwrap_or(crate::agent_io::registry::RuntimeCleanupPolicy::Default)
    } else {
        crate::agent_io::registry::RuntimeCleanupPolicy::Default
    };

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent crashed", err))?;
    if changes > 0 {
        // Recoverable CRASHED home preservation depends on this cleanup popping the registry
        // before agent_watch's later Default cleanup; do not add an earlier Default cleanup.
        crate::agent_io::registry::cleanup_agent_runtime_resources_with_policy(
            agent_id,
            cleanup_session_id.as_deref(),
            cleanup_policy,
        );
        notify_runtime_agent_changed();
    }
    Ok(changes)
}

fn notify_runtime_agent_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::AgentChanged,
    );
}

fn capture_recovery_intent_for_crash(
    conn: &rusqlite::Connection,
    agent_id: &str,
    previous_state: Option<&str>,
    reason: &str,
) -> Result<(), AhError> {
    let Some((session_id, provider, crashed_state_version)) = conn
        .query_row(
            "SELECT session_id, provider, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query crashed agent for recovery intent", err))?
    else {
        tracing::warn!(
            agent_id,
            "skipped recovery intent capture because crashed agent row was missing"
        );
        return Ok(());
    };

    let interrupted = conn
        .query_row(
            "SELECT id, status, request_id, prompt_text, cancel_requested, \
                    requires_physical_evidence, requires_test_evidence, governance_binding_json \
             FROM jobs \
             WHERE agent_id = ? AND status = 'DISPATCHED' \
             ORDER BY dispatched_at ASC, id ASC LIMIT 1",
            params![agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query interrupted job for recovery intent", err))?;

    let previous_state = previous_state.unwrap_or("UNKNOWN").to_string();
    let is_unexpected_exit_reason = matches!(
        reason,
        "AGENT_UNEXPECTED_EXIT" | "EXIT_CODE_UNAVAILABLE_NON_CHILD"
    );
    let action = if matches!(previous_state.as_str(), STATE_WAITING_FOR_ACK | STATE_BUSY)
        || interrupted.is_some()
    {
        RecoveryIntentAction::Revive
    } else if previous_state == STATE_IDLE && is_unexpected_exit_reason {
        RecoveryIntentAction::ReviveIdle
    } else if matches!(previous_state.as_str(), STATE_IDLE | STATE_SPAWNING) {
        RecoveryIntentAction::ReapOnly
    } else {
        RecoveryIntentAction::ReapOnly
    };
    let (interrupted_job_id, interrupted_job_status, interrupted_job) = interrupted
        .map(
            |(
                id,
                status,
                request_id,
                prompt_text,
                cancel_requested,
                requires_physical_evidence,
                requires_test_evidence,
                governance_binding_json,
            )| {
                (
                    Some(id.clone()),
                    Some(status),
                    Some(CapturedInterruptedJob {
                        id,
                        request_id,
                        prompt_text,
                        cancel_requested: cancel_requested != 0,
                        requires_physical_evidence: requires_physical_evidence != 0,
                        requires_test_evidence: requires_test_evidence != 0,
                        governance_binding_json,
                    }),
                )
            },
        )
        .unwrap_or((None, None, None));

    let intent = AgentRecoveryIntent {
        agent_id: agent_id.to_string(),
        session_id,
        provider,
        previous_state,
        crashed_state_version,
        interrupted_job_id,
        interrupted_job_status,
        interrupted_job,
        action,
        reason: reason.to_string(),
    };
    tracing::info!(
        agent_id,
        previous_state = %intent.previous_state,
        action = ?intent.action,
        interrupted_job_id = ?intent.interrupted_job_id,
        "captured recovery intent for worker"
    );
    persist_agent_recovery_intent_sync(conn, &intent)
}

fn crashed_cleanup_policy(provider: &str) -> crate::agent_io::registry::RuntimeCleanupPolicy {
    if crate::provider::manifest::is_recovery_eligible_provider(provider) {
        crate::agent_io::registry::RuntimeCleanupPolicy::PreserveRecoverableCrashedHome
    } else {
        crate::agent_io::registry::RuntimeCleanupPolicy::Default
    }
}

pub async fn mark_agent_killed(db: Db, agent_id: String, reason: String) -> Result<usize, AhError> {
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let result = spawn_db("agents_lifecycle::mark_agent_killed", move || {
        mark_agent_killed_sync(&db, &agent_id, &reason)
    })
    .await;
    if matches!(result, Ok(changes) if changes > 0) {
        if let Some(job_id) = affected_job {
            crate::orchestrator::pubsub::notify_job_update(&job_id);
        }
        crate::orchestrator::wake_up();
    }
    result
}

pub async fn mark_agent_crashed_with_exit(
    db: Db,
    agent_id: String,
    exit_code: Option<i32>,
) -> Result<usize, AhError> {
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let result = spawn_db(
        "agents_lifecycle::mark_agent_crashed_with_exit",
        move || mark_agent_crashed_with_exit_sync(&db, &agent_id, exit_code),
    )
    .await;
    if matches!(result, Ok(changes) if changes > 0) {
        if let Some(job_id) = affected_job {
            crate::orchestrator::pubsub::notify_job_update(&job_id);
        }
        crate::orchestrator::wake_up();
    }
    result
}

pub async fn mark_agent_crashed_with_reason(
    db: Db,
    agent_id: String,
    exit_code: Option<i32>,
    reason: String,
) -> Result<usize, AhError> {
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let result = spawn_db(
        "agents_lifecycle::mark_agent_crashed_with_reason",
        move || mark_agent_crashed_sync(&db, &agent_id, exit_code, &reason),
    )
    .await;
    if matches!(result, Ok(changes) if changes > 0) {
        if let Some(job_id) = affected_job {
            crate::orchestrator::pubsub::notify_job_update(&job_id);
        }
        crate::orchestrator::wake_up();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{
        mark_agent_crashed_with_exit_sync, mark_agent_killed_for_master_death_sync,
        mark_agent_killed_sync,
    };
    use crate::agent_io::{AgentIoEntry, register};
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::recovery::{RecoveryIntentAction, query_agent_recovery_intent_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::{
        STATE_BUSY, STATE_CRASHED, STATE_IDLE, STATE_KILLED, STATE_PROMPT_PENDING,
        STATE_WAITING_FOR_ACK,
    };
    use crate::db::{Db, init};
    use crate::home_materialization::sandbox_home_for_sandbox_dir;
    use crate::tmux::TmuxPaneId;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    fn with_test_db_handle<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_agent(conn: &rusqlite::Connection) {
        insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
    }

    fn seed_agent_with_state(conn: &rusqlite::Connection, state: &str) {
        seed_agent_with_state_and_provider_for_agent(conn, "a1", state, "codex");
    }

    fn seed_agent_with_state_and_provider_for_agent(
        conn: &rusqlite::Connection,
        agent_id: &str,
        state: &str,
        provider: &str,
    ) {
        insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(conn, agent_id, "s1", provider, state, Some(123)).unwrap();
    }

    fn seed_dispatched_job(conn: &rusqlite::Connection, job_id: &str) {
        seed_dispatched_job_for_agent(conn, "a1", job_id);
    }

    fn seed_dispatched_job_for_agent(conn: &rusqlite::Connection, agent_id: &str, job_id: &str) {
        insert_job_sync(conn, job_id, agent_id, None, "work\n").unwrap();
        conn.execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch(), dispatched_at_seq_id = 7 WHERE id = ?",
            [job_id],
        )
        .unwrap();
    }

    #[test]
    fn test_mark_agent_killed_is_terminal_and_idempotent() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent(&conn);
            }
            let first = mark_agent_killed_sync(db, "a1", "SIGKILL_BY_DAEMON").unwrap();
            let second = mark_agent_killed_sync(db, "a1", "SIGKILL_BY_DAEMON").unwrap();
            let (state, state_version, payload): (String, i64, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.state_version, events.payload FROM agents JOIN events ON events.agent_id = agents.id WHERE agents.id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(first, 1);
            assert_eq!(second, 0);
            assert_eq!(state, "KILLED");
            assert_eq!(state_version, 2);
            assert_eq!(payload["to"], "KILLED");
        });
    }

    #[test]
    fn test_mark_agent_crashed_with_exit_sets_metadata() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent(&conn);
            }
            let changes = mark_agent_crashed_with_exit_sync(db, "a1", Some(137)).unwrap();
            let (state, exit_code, error_code): (String, Option<i64>, Option<String>) = db
                .conn()
                .query_row(
                    "SELECT state, exit_code, error_code FROM agents WHERE id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(changes, 1);
            assert_eq!(state, "CRASHED");
            assert_eq!(exit_code, Some(137));
            assert_eq!(error_code.as_deref(), Some("AGENT_UNEXPECTED_EXIT"));
        });
    }

    #[test]
    fn test_mark_agent_crashed_with_unknown_exit_sets_null_exit_code() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent(&conn);
            }
            let changes = mark_agent_crashed_with_exit_sync(db, "a1", None).unwrap();
            let (state, exit_code, payload): (String, Option<i64>, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.exit_code, events.payload FROM agents JOIN events ON events.agent_id = agents.id WHERE agents.id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(changes, 1);
            assert_eq!(state, "CRASHED");
            assert_eq!(exit_code, None);
            assert_eq!(payload["reason"], "EXIT_CODE_UNAVAILABLE_NON_CHILD");
            assert!(payload["exit_code"].is_null());
        });
    }

    #[test]
    fn test_mark_agent_crashed_accepts_waiting_for_ack() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", STATE_WAITING_FOR_ACK, Some(123))
                    .unwrap();
            }
            let changes = mark_agent_crashed_with_exit_sync(db, "a1", None).unwrap();
            let (state, payload): (String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, events.payload \
                     FROM agents JOIN events ON events.agent_id = agents.id \
                     WHERE agents.id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(changes, 1);
            assert_eq!(state, STATE_CRASHED);
            assert_eq!(payload["from"], STATE_WAITING_FOR_ACK);
            assert_eq!(payload["to"], STATE_CRASHED);
        });
    }

    #[test]
    fn rr_worker_crash_busy_dispatched_records_revive_intent_and_fails_job() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent_with_state(&conn, STATE_BUSY);
                seed_dispatched_job(&conn, "job_busy");
                conn.execute(
                    "UPDATE jobs
                     SET governance_binding_json = '{\"binding_version\":1}'
                     WHERE id = 'job_busy'",
                    [],
                )
                .unwrap();
            }

            let changes = mark_agent_crashed_with_exit_sync(db, "a1", Some(137)).unwrap();
            let conn = db.conn();
            let intent = query_agent_recovery_intent_sync(&conn, "a1")
                .unwrap()
                .expect("BUSY crash with DISPATCHED job should capture REVIVE intent");
            let job = query_job_sync(&conn, "job_busy").unwrap().unwrap();

            assert_eq!(changes, 1);
            assert_eq!(intent.previous_state, STATE_BUSY);
            assert_eq!(intent.interrupted_job_id.as_deref(), Some("job_busy"));
            assert_eq!(intent.interrupted_job_status.as_deref(), Some("DISPATCHED"));
            let captured = intent.interrupted_job.as_ref().unwrap();
            assert_eq!(captured.id, "job_busy");
            assert_eq!(captured.prompt_text, "work\n");
            assert_eq!(
                captured.governance_binding_json.as_deref(),
                Some("{\"binding_version\":1}")
            );
            assert_eq!(intent.action, RecoveryIntentAction::Revive);
            assert_eq!(job.status, "FAILED");
        });
    }

    #[test]
    fn rr_worker_crash_waiting_for_ack_dispatched_records_revive_intent() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent_with_state(&conn, STATE_WAITING_FOR_ACK);
                seed_dispatched_job(&conn, "job_ack");
            }

            let changes = mark_agent_crashed_with_exit_sync(db, "a1", None).unwrap();
            let intent = query_agent_recovery_intent_sync(&db.conn(), "a1")
                .unwrap()
                .expect("WAITING_FOR_ACK crash should capture REVIVE intent");

            assert_eq!(changes, 1);
            assert_eq!(intent.previous_state, STATE_WAITING_FOR_ACK);
            assert_eq!(intent.interrupted_job_id.as_deref(), Some("job_ack"));
            assert_eq!(
                intent.interrupted_job.as_ref().unwrap().prompt_text,
                "work\n"
            );
            assert_eq!(intent.action, RecoveryIntentAction::Revive);
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_master_death_preserves_recoverable_worker_home() {
        let agent_id = "a_master_death_preserve";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let pipes_dir = state_dir.path().join("pipes");
        std::fs::create_dir_all(&pipes_dir).unwrap();
        let sandbox_dir = state_dir.path().join("sandboxes").join("s1").join(agent_id);
        std::fs::create_dir_all(&sandbox_dir).unwrap();
        let home_root = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let marker = home_root.join(".codex/sessions/marker");
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, b"resume-session").unwrap();
        let fifo_path = pipes_dir.join(format!("{agent_id}.fifo"));
        std::fs::write(&fifo_path, b"").unwrap();
        register(
            agent_id.to_string(),
            AgentIoEntry {
                session_id: "s1".to_string(),
                pane_id: TmuxPaneId("%master-death-preserve".to_string()),
                expected_pid: None,
                reader_handle: tokio::spawn(async { std::future::pending::<()>().await }),
                fifo_path,
                socket_name: "missing-socket".to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );
        {
            let conn = db.conn();
            seed_agent_with_state_and_provider_for_agent(&conn, agent_id, STATE_BUSY, "codex");
            seed_dispatched_job_for_agent(&conn, agent_id, "job_master_death_preserve");
        }

        let changes =
            mark_agent_killed_for_master_death_sync(&db, agent_id, "MASTER_EXIT").unwrap();

        assert_eq!(changes, 1);
        assert!(
            marker.exists(),
            "master-death recovery-eligible worker cleanup must preserve provider session home"
        );
        assert!(
            !sandbox_dir.exists(),
            "runtime sandbox shell should still be removed while preserving home"
        );
        let _ = std::fs::remove_dir_all(&home_root);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_master_death_deletes_non_recoverable_worker_home() {
        let agent_id = "a_master_death_delete";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let pipes_dir = state_dir.path().join("pipes");
        std::fs::create_dir_all(&pipes_dir).unwrap();
        let sandbox_dir = state_dir.path().join("sandboxes").join("s1").join(agent_id);
        std::fs::create_dir_all(&sandbox_dir).unwrap();
        let home_root = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let marker = home_root.join(".bash/sessions/marker");
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, b"non-recoverable-session").unwrap();
        let fifo_path = pipes_dir.join(format!("{agent_id}.fifo"));
        std::fs::write(&fifo_path, b"").unwrap();
        register(
            agent_id.to_string(),
            AgentIoEntry {
                session_id: "s1".to_string(),
                pane_id: TmuxPaneId("%master-death-delete".to_string()),
                expected_pid: None,
                reader_handle: tokio::spawn(async { std::future::pending::<()>().await }),
                fifo_path,
                socket_name: "missing-socket".to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );
        {
            let conn = db.conn();
            seed_agent_with_state_and_provider_for_agent(&conn, agent_id, STATE_BUSY, "bash");
            seed_dispatched_job_for_agent(&conn, agent_id, "job_master_death_delete");
        }

        let changes =
            mark_agent_killed_for_master_death_sync(&db, agent_id, "MASTER_EXIT").unwrap();

        assert_eq!(changes, 1);
        assert!(
            !marker.exists(),
            "master-death non-recoverable worker cleanup must delete provider session home"
        );
        assert!(
            !sandbox_dir.exists(),
            "runtime sandbox shell should be removed for non-recoverable worker"
        );
    }

    #[test]
    fn rr_worker_crash_idle_without_dispatched_records_revive_idle_intent() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent_with_state(&conn, STATE_IDLE);
            }

            let changes = mark_agent_crashed_with_exit_sync(db, "a1", Some(1)).unwrap();
            let intent = query_agent_recovery_intent_sync(&db.conn(), "a1")
                .unwrap()
                .expect("IDLE crash without interrupted job should capture REAP_ONLY intent");

            assert_eq!(changes, 1);
            assert_eq!(intent.previous_state, STATE_IDLE);
            assert_eq!(intent.interrupted_job_id, None);
            assert_eq!(intent.action, RecoveryIntentAction::ReviveIdle);
        });
    }

    #[test]
    fn rr_worker_crash_spawning_without_dispatched_stays_reap_only() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent_with_state(&conn, crate::db::state_machine::STATE_SPAWNING);
            }

            let changes = mark_agent_crashed_with_exit_sync(db, "a1", Some(1)).unwrap();
            let intent = query_agent_recovery_intent_sync(&db.conn(), "a1")
                .unwrap()
                .expect("SPAWNING crash should still capture cleanup-only intent");

            assert_eq!(changes, 1);
            assert_eq!(
                intent.previous_state,
                crate::db::state_machine::STATE_SPAWNING
            );
            assert_eq!(intent.interrupted_job_id, None);
            assert_eq!(intent.action, RecoveryIntentAction::ReapOnly);
        });
    }

    #[test]
    fn rr_master_death_idle_without_dispatched_stays_reap_only() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent_with_state(&conn, STATE_IDLE);
            }

            let changes = mark_agent_killed_for_master_death_sync(db, "a1", "MASTER_EXIT")
                .expect("master-death cleanup should still mark idle worker killed");
            let intent = query_agent_recovery_intent_sync(&db.conn(), "a1")
                .unwrap()
                .expect("master-death cleanup captures cleanup-only intent");

            assert_eq!(changes, 1);
            assert_eq!(intent.previous_state, STATE_IDLE);
            assert_eq!(intent.interrupted_job_id, None);
            assert_eq!(intent.action, RecoveryIntentAction::ReapOnly);
        });
    }

    #[test]
    fn rr_worker_crash_prompt_pending_does_not_record_intent_or_change_state() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent_with_state(&conn, STATE_PROMPT_PENDING);
            }

            let changes = mark_agent_crashed_with_exit_sync(db, "a1", Some(1)).unwrap();
            let state: String = db
                .conn()
                .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            let event_count: i64 = db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE agent_id = 'a1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(changes, 0);
            assert_eq!(state, STATE_PROMPT_PENDING);
            assert_eq!(event_count, 0);
        });
    }

    #[test]
    fn test_mark_agent_killed_accepts_prompt_pending() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", STATE_PROMPT_PENDING, Some(123))
                    .unwrap();
            }

            let killed = mark_agent_killed_sync(db, "a1", "SIGKILL_BY_DAEMON").unwrap();
            let crashed = mark_agent_crashed_with_exit_sync(db, "a1", Some(1)).unwrap();
            let (state, state_version, event_count, payload): (String, i64, i64, String) = db
                .conn()
                .query_row(
                    "SELECT state, state_version, \
                            (SELECT COUNT(*) FROM events WHERE agent_id = 'a1'), \
                            (SELECT payload FROM events WHERE agent_id = 'a1') \
                     FROM agents WHERE id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(killed, 1);
            assert_eq!(crashed, 0);
            assert_eq!(state, STATE_KILLED);
            assert_eq!(state_version, 2);
            assert_eq!(event_count, 1);
            assert_eq!(payload["from"], STATE_PROMPT_PENDING);
            assert_eq!(payload["to"], STATE_KILLED);
        });
    }
}
