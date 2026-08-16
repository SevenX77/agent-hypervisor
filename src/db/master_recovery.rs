use crate::error::AhError;
use rusqlite::{Connection, OptionalExtension, params};

pub const MASTER_RECOVERY_CASCADE_DEFER_SECS: i64 = 90;
pub const MASTER_RECOVERY_WINDOW_MAX_TOTAL_SECS: i64 = 300;
pub const MASTER_REVIVE_READINESS_TIMEOUT_SECS: i64 = 30;
pub const MASTER_REVIVE_READINESS_CLEANUP_MARGIN_SECS: i64 = 5;

pub const MASTER_RECOVERY_WINDOWS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS master_recovery_windows (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    expected_pid INTEGER NOT NULL,
    expected_generation INTEGER NOT NULL,
    claimed_generation INTEGER,
    phase TEXT NOT NULL CHECK(phase IN ('DETECTED','WORKERS_REAPED','MASTER_SPAWNING','MASTER_RUNNING','MASTER_VERIFYING','WORKERS_REPROVISIONING','COMPLETED','FAILED','FUSED')),
    active_work INTEGER NOT NULL,
    defer_until INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    completed_at INTEGER,
    readiness_mode TEXT,
    ready_at INTEGER,
    ready_reason TEXT,
    readiness_token TEXT
) STRICT;
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnchorCascadeDecision {
    Defer { until: i64, phase: String },
    CascadeNow { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterRecoveryWindow {
    pub session_id: String,
    pub expected_pid: i64,
    pub expected_generation: i64,
    pub claimed_generation: Option<i64>,
    pub phase: String,
    pub active_work: bool,
    pub defer_until: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

pub fn resolve_master_recovery_cascade_defer_secs() -> i64 {
    std::env::var("AH_MASTER_RECOVERY_CASCADE_DEFER_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(MASTER_RECOVERY_CASCADE_DEFER_SECS)
        .clamp(10, MASTER_RECOVERY_WINDOW_MAX_TOTAL_SECS)
}

pub fn resolve_master_revive_readiness_timeout_secs() -> i64 {
    std::env::var("AH_MASTER_REVIVE_READINESS_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(MASTER_REVIVE_READINESS_TIMEOUT_SECS)
        .clamp(10, 60)
}

pub fn effective_readiness_timeout(
    now: i64,
    defer_until: i64,
    created_at: i64,
    configured_secs: i64,
    cleanup_margin: i64,
) -> i64 {
    let hard_deadline = defer_until.min(created_at + MASTER_RECOVERY_WINDOW_MAX_TOTAL_SECS);
    let remaining = hard_deadline - now;
    let timeout = configured_secs.clamp(10, 60);
    timeout.min(remaining - cleanup_margin)
}

pub fn begin_master_recovery_window_sync(
    conn: &Connection,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    active_work: bool,
    now: i64,
    defer_secs: i64,
) -> Result<MasterRecoveryWindow, AhError> {
    let tx = conn
        .unchecked_transaction()
        .map_err(|err| map_master_recovery_error("begin master recovery window tx", err))?;
    let defer_until = capped_defer_until(now, defer_secs, now);
    let existing = query_window_in_tx(&tx, session_id)?;
    match existing {
        None => {
            tx.execute(
                "INSERT INTO master_recovery_windows (
                    session_id,
                    expected_pid,
                    expected_generation,
                    claimed_generation,
                    phase,
                    active_work,
                    defer_until,
                    created_at,
                    updated_at
                 ) VALUES (?1, ?2, ?3, NULL, 'DETECTED', ?4, ?5, ?6, ?6)",
                params![
                    session_id,
                    expected_pid,
                    expected_generation,
                    bool_to_i64(active_work),
                    defer_until,
                    now
                ],
            )
            .map_err(|err| map_master_recovery_error("insert master recovery window", err))?;
        }
        Some(existing) if existing.expected_generation > expected_generation => {}
        Some(existing)
            if existing.expected_generation < expected_generation
                || is_terminal_phase(&existing.phase) =>
        {
            tx.execute(
                "UPDATE master_recovery_windows
                 SET expected_pid = ?2,
                     expected_generation = ?3,
                     claimed_generation = NULL,
                     phase = 'DETECTED',
                     active_work = ?4,
                     defer_until = ?5,
                     created_at = ?6,
                     updated_at = ?6,
                     completed_at = NULL,
                     readiness_mode = NULL,
                     ready_at = NULL,
                     ready_reason = NULL,
                     readiness_token = NULL
                 WHERE session_id = ?1",
                params![
                    session_id,
                    expected_pid,
                    expected_generation,
                    bool_to_i64(active_work),
                    defer_until,
                    now
                ],
            )
            .map_err(|err| map_master_recovery_error("replace master recovery window", err))?;
        }
        Some(existing) => {
            let active_work = existing.active_work || active_work;
            let defer_until =
                existing
                    .defer_until
                    .max(capped_defer_until(now, defer_secs, existing.created_at));
            tx.execute(
                "UPDATE master_recovery_windows
                 SET expected_pid = ?2,
                     active_work = ?3,
                     defer_until = ?4,
                     updated_at = ?5
                 WHERE session_id = ?1
                   AND expected_generation = ?6",
                params![
                    session_id,
                    expected_pid,
                    bool_to_i64(active_work),
                    defer_until,
                    now,
                    expected_generation
                ],
            )
            .map_err(|err| map_master_recovery_error("upsert master recovery window", err))?;
        }
    }
    let window = query_window_in_tx(&tx, session_id)?.ok_or_else(|| {
        AhError::DbConstraintViolation(format!(
            "master recovery window missing after begin for {session_id}"
        ))
    })?;
    tx.commit()
        .map_err(|err| map_master_recovery_error("commit master recovery window begin", err))?;
    Ok(window)
}

pub fn update_master_recovery_phase_sync(
    conn: &Connection,
    session_id: &str,
    expected_generation: i64,
    new_phase: &str,
    now: i64,
) -> Result<bool, AhError> {
    let tx = conn
        .unchecked_transaction()
        .map_err(|err| map_master_recovery_error("begin master recovery phase tx", err))?;
    let Some(existing) = query_window_in_tx(&tx, session_id)? else {
        tx.commit()
            .map_err(|err| map_master_recovery_error("commit missing phase update", err))?;
        return Ok(false);
    };
    if existing.expected_generation != expected_generation {
        tx.commit()
            .map_err(|err| map_master_recovery_error("commit stale phase update", err))?;
        return Ok(false);
    }
    let defer_until = existing.defer_until.max(capped_defer_until(
        now,
        resolve_master_recovery_cascade_defer_secs(),
        existing.created_at,
    ));
    let changes = tx
        .execute(
            "UPDATE master_recovery_windows
             SET phase = ?3,
                 defer_until = ?4,
                 updated_at = ?5
             WHERE session_id = ?1
               AND expected_generation = ?2",
            params![session_id, expected_generation, new_phase, defer_until, now],
        )
        .map_err(|err| map_master_recovery_error("update master recovery phase", err))?;
    tx.commit()
        .map_err(|err| map_master_recovery_error("commit master recovery phase", err))?;
    Ok(changes == 1)
}

pub fn complete_master_recovery_window_sync(
    conn: &Connection,
    session_id: &str,
    expected_generation: i64,
    now: i64,
) -> Result<bool, AhError> {
    let changes = conn
        .execute(
            "UPDATE master_recovery_windows
             SET phase = 'COMPLETED',
                 completed_at = ?3,
                 updated_at = ?3
             WHERE session_id = ?1
               AND expected_generation = ?2",
            params![session_id, expected_generation, now],
        )
        .map_err(|err| map_master_recovery_error("complete master recovery window", err))?;
    Ok(changes == 1)
}

pub fn begin_master_recovery_readiness_wait_sync(
    conn: &Connection,
    session_id: &str,
    expected_generation: i64,
    readiness_mode: &str,
    readiness_token: &str,
    now: i64,
) -> Result<bool, AhError> {
    let changes = conn
        .execute(
            "UPDATE master_recovery_windows
             SET phase = 'MASTER_VERIFYING',
                 readiness_mode = ?3,
                 readiness_token = ?4,
                 ready_at = NULL,
                 ready_reason = NULL,
                 updated_at = ?5
             WHERE session_id = ?1
               AND expected_generation = ?2
               AND phase NOT IN ('COMPLETED', 'FAILED', 'FUSED')",
            params![
                session_id,
                expected_generation,
                readiness_mode,
                readiness_token,
                now
            ],
        )
        .map_err(|err| map_master_recovery_error("begin master recovery readiness wait", err))?;
    Ok(changes == 1)
}

pub fn mark_master_recovery_ready_sync(
    conn: &Connection,
    session_id: &str,
    expected_generation: i64,
    ready_reason: &str,
    now: i64,
) -> Result<bool, AhError> {
    let changes = conn
        .execute(
            "UPDATE master_recovery_windows
             SET ready_at = ?4,
                 ready_reason = ?3,
                 updated_at = ?4
             WHERE session_id = ?1
               AND expected_generation = ?2
               AND phase = 'MASTER_VERIFYING'",
            params![session_id, expected_generation, ready_reason, now],
        )
        .map_err(|err| map_master_recovery_error("mark master recovery ready", err))?;
    Ok(changes == 1)
}

pub fn fail_master_recovery_readiness_sync(
    conn: &Connection,
    session_id: &str,
    expected_generation: i64,
    reason: &str,
    now: i64,
) -> Result<bool, AhError> {
    let changes = conn
        .execute(
            "UPDATE master_recovery_windows
             SET phase = 'FAILED',
                 ready_reason = ?3,
                 updated_at = ?4
             WHERE session_id = ?1
               AND expected_generation = ?2
               AND phase = 'MASTER_VERIFYING'",
            params![session_id, expected_generation, reason, now],
        )
        .map_err(|err| map_master_recovery_error("fail master recovery readiness", err))?;
    Ok(changes == 1)
}

pub fn expire_master_recovery_window_sync(
    conn: &Connection,
    session_id: &str,
    now: i64,
) -> Result<bool, AhError> {
    let tx = conn
        .unchecked_transaction()
        .map_err(|err| map_master_recovery_error("begin expire master recovery tx", err))?;
    let expired = expire_master_recovery_window_in_tx(&tx, session_id, now)?;
    tx.commit()
        .map_err(|err| map_master_recovery_error("commit expire master recovery", err))?;
    Ok(expired)
}

pub fn decide_anchor_cascade_sync(
    conn: &Connection,
    session_id: &str,
    now: i64,
) -> Result<AnchorCascadeDecision, AhError> {
    let tx = conn
        .unchecked_transaction()
        .map_err(|err| map_master_recovery_error("begin anchor cascade decision tx", err))?;
    let decision = decide_anchor_cascade_in_tx(&tx, session_id, now)?;
    tx.commit()
        .map_err(|err| map_master_recovery_error("commit anchor cascade decision", err))?;
    Ok(decision)
}

fn decide_anchor_cascade_in_tx(
    conn: &Connection,
    session_id: &str,
    now: i64,
) -> Result<AnchorCascadeDecision, AhError> {
    let Some(session) = query_session_runtime(conn, session_id)? else {
        return Ok(AnchorCascadeDecision::CascadeNow {
            reason: "SESSION_NOT_ACTIVE",
        });
    };
    if session.status != "ACTIVE" {
        return Ok(AnchorCascadeDecision::CascadeNow {
            reason: "SESSION_NOT_ACTIVE",
        });
    }

    if let Some(window) = query_window_in_tx(conn, session_id)? {
        return decision_for_existing_window(conn, &window, now);
    }

    if master_cutover_inflight_in_tx(conn, session_id)? {
        return Ok(AnchorCascadeDecision::CascadeNow {
            reason: "MASTER_CUTOVER_IN_FLIGHT",
        });
    }

    if session_has_active_work(conn, session_id)? {
        let window = begin_master_recovery_window_in_tx(
            conn,
            session_id,
            session.master_pid,
            session.master_generation,
            true,
            now,
            resolve_master_recovery_cascade_defer_secs(),
        )?;
        return Ok(AnchorCascadeDecision::Defer {
            until: window.defer_until,
            phase: window.phase,
        });
    }

    Ok(AnchorCascadeDecision::CascadeNow {
        reason: "NO_MASTER_RECOVERY_WINDOW",
    })
}

fn decision_for_existing_window(
    conn: &Connection,
    window: &MasterRecoveryWindow,
    now: i64,
) -> Result<AnchorCascadeDecision, AhError> {
    match window.phase.as_str() {
        "COMPLETED" => Ok(AnchorCascadeDecision::CascadeNow {
            reason: "MASTER_RECOVERY_COMPLETED",
        }),
        "FAILED" | "FUSED" => Ok(AnchorCascadeDecision::CascadeNow {
            reason: "MASTER_RECOVERY_TERMINAL",
        }),
        _ if now > window.defer_until => {
            expire_master_recovery_window_in_tx(conn, &window.session_id, now)?;
            Ok(AnchorCascadeDecision::CascadeNow {
                reason: "MASTER_REVIVE_WINDOW_EXPIRED",
            })
        }
        _ if window.active_work => Ok(AnchorCascadeDecision::Defer {
            until: window.defer_until,
            phase: window.phase.clone(),
        }),
        _ => Ok(AnchorCascadeDecision::CascadeNow {
            reason: "NO_ACTIVE_WORK_WINDOW",
        }),
    }
}

fn begin_master_recovery_window_in_tx(
    conn: &Connection,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    active_work: bool,
    now: i64,
    defer_secs: i64,
) -> Result<MasterRecoveryWindow, AhError> {
    let defer_until = capped_defer_until(now, defer_secs, now);
    conn.execute(
        "INSERT INTO master_recovery_windows (
            session_id,
            expected_pid,
            expected_generation,
            phase,
            active_work,
            defer_until,
            created_at,
            updated_at
         ) VALUES (?1, ?2, ?3, 'DETECTED', ?4, ?5, ?6, ?6)",
        params![
            session_id,
            expected_pid,
            expected_generation,
            bool_to_i64(active_work),
            defer_until,
            now
        ],
    )
    .map_err(|err| map_master_recovery_error("insert detected master recovery window", err))?;
    query_window_in_tx(conn, session_id)?.ok_or_else(|| {
        AhError::DbConstraintViolation(format!(
            "master recovery window missing after detected insert for {session_id}"
        ))
    })
}

fn expire_master_recovery_window_in_tx(
    conn: &Connection,
    session_id: &str,
    now: i64,
) -> Result<bool, AhError> {
    let Some(window) = query_window_in_tx(conn, session_id)? else {
        return Ok(false);
    };
    if is_terminal_phase(&window.phase) || now <= window.defer_until {
        return Ok(false);
    }
    let changes = conn
        .execute(
            "UPDATE master_recovery_windows
             SET phase = 'FAILED',
                 updated_at = ?2
             WHERE session_id = ?1
               AND phase NOT IN ('COMPLETED', 'FAILED', 'FUSED')
               AND ?2 > defer_until",
            params![session_id, now],
        )
        .map_err(|err| map_master_recovery_error("expire master recovery window", err))?;
    if changes == 1 {
        conn.execute(
            "UPDATE sessions
             SET status = 'FAILED',
                 master_last_exit_reason = 'MASTER_REVIVE_WINDOW_EXPIRED'
             WHERE id = ?1",
            params![session_id],
        )
        .map_err(|err| map_master_recovery_error("mark session master recovery expired", err))?;
        crate::orchestrator::pubsub::notify_runtime_changed(
            crate::runtime_events::RuntimeSnapshotReason::InventoryChanged,
        );
    }
    Ok(changes == 1)
}

#[derive(Debug)]
struct SessionRuntime {
    status: String,
    master_pid: i64,
    master_generation: i64,
}

fn query_session_runtime(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<SessionRuntime>, AhError> {
    conn.query_row(
        "SELECT status, master_pid, master_generation FROM sessions WHERE id = ?1",
        params![session_id],
        |row| {
            Ok(SessionRuntime {
                status: row.get(0)?,
                master_pid: row.get(1)?,
                master_generation: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_master_recovery_error("query session runtime for master recovery", err))
}

fn query_window_in_tx(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<MasterRecoveryWindow>, AhError> {
    conn.query_row(
        "SELECT session_id,
                expected_pid,
                expected_generation,
                claimed_generation,
                phase,
                active_work,
                defer_until,
                created_at,
                updated_at,
                completed_at
         FROM master_recovery_windows
         WHERE session_id = ?1",
        params![session_id],
        |row| {
            Ok(MasterRecoveryWindow {
                session_id: row.get(0)?,
                expected_pid: row.get(1)?,
                expected_generation: row.get(2)?,
                claimed_generation: row.get(3)?,
                phase: row.get(4)?,
                active_work: row.get::<_, i64>(5)? != 0,
                defer_until: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
                completed_at: row.get(9)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_master_recovery_error("query master recovery window", err))
}

fn session_has_active_work(conn: &Connection, session_id: &str) -> Result<bool, AhError> {
    let has_active_worker = conn
        .query_row(
            "SELECT 1
             FROM agents
             WHERE session_id = ?1
               AND state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY', 'PROMPT_PENDING')
             LIMIT 1",
            params![session_id],
            |_| Ok(true),
        )
        .optional()
        .map(|value| value.unwrap_or(false))
        .map_err(|err| map_master_recovery_error("query active worker for master recovery", err))?;
    if has_active_worker {
        return Ok(true);
    }
    conn.query_row(
        "SELECT 1
         FROM jobs
         JOIN agents ON agents.id = jobs.agent_id
         WHERE agents.session_id = ?1
           AND jobs.status IN ('QUEUED', 'DISPATCHED')
         LIMIT 1",
        params![session_id],
        |_| Ok(true),
    )
    .optional()
    .map(|value| value.unwrap_or(false))
    .map_err(|err| map_master_recovery_error("query active job for master recovery", err))
}

fn master_cutover_inflight_in_tx(conn: &Connection, session_id: &str) -> Result<bool, AhError> {
    conn.query_row(
        "SELECT 1
         FROM master_cutovers
         WHERE session_id = ?1
           AND state IN ('PREPARING', 'SPAWNING', 'VERIFYING')
         LIMIT 1",
        params![session_id],
        |_| Ok(true),
    )
    .optional()
    .map(|value| value.unwrap_or(false))
    .map_err(|err| map_master_recovery_error("query master cutover for recovery window", err))
}

fn capped_defer_until(now: i64, defer_secs: i64, created_at: i64) -> i64 {
    let defer_secs = defer_secs.clamp(10, MASTER_RECOVERY_WINDOW_MAX_TOTAL_SECS);
    (now + defer_secs).min(created_at + MASTER_RECOVERY_WINDOW_MAX_TOTAL_SECS)
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn is_terminal_phase(phase: &str) -> bool {
    matches!(phase, "COMPLETED" | "FAILED" | "FUSED")
}

fn map_master_recovery_error(context: &str, err: rusqlite::Error) -> AhError {
    AhError::DbConstraintViolation(format!("{context}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::insert_job_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};

    fn with_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_session(db: &Db, session_id: &str, master_pid: i64, master_generation: i64) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            session_id,
            &format!("p_{session_id}"),
            "/tmp/project",
        )
        .unwrap();
        conn.execute(
            "UPDATE sessions SET master_pid = ?2, master_generation = ?3 WHERE id = ?1",
            rusqlite::params![session_id, master_pid, master_generation],
        )
        .unwrap();
    }

    fn window_phase(db: &Db, session_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT phase FROM master_recovery_windows WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn window_defer_until(db: &Db, session_id: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT defer_until FROM master_recovery_windows WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn window_readiness(
        db: &Db,
        session_id: &str,
    ) -> (String, Option<i64>, Option<String>, String) {
        db.conn()
            .query_row(
                "SELECT readiness_mode, ready_at, ready_reason, readiness_token
                 FROM master_recovery_windows
                 WHERE session_id = ?1",
                [session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap()
    }

    #[test]
    fn master_recovery_schema_new_db_has_windows_table() {
        with_db(|db| {
            let conn = db.conn();
            let table_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'master_recovery_windows')",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(table_exists);
            for column in [
                "session_id",
                "expected_pid",
                "expected_generation",
                "claimed_generation",
                "phase",
                "active_work",
                "defer_until",
                "created_at",
                "updated_at",
                "completed_at",
                "readiness_mode",
                "ready_at",
                "ready_reason",
                "readiness_token",
            ] {
                let exists: bool = conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('master_recovery_windows') WHERE name = ?1)",
                        [column],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert!(exists, "missing column {column}");
            }
        });
    }

    #[test]
    fn master_recovery_schema_accepts_master_verifying_and_readiness_columns() {
        with_db(|db| {
            seed_session(db, "s_schema_verify", 111, 8);
            let conn = db.conn();
            conn.execute(
                "INSERT INTO master_recovery_windows (
                    session_id,
                    expected_pid,
                    expected_generation,
                    phase,
                    active_work,
                    defer_until,
                    created_at,
                    updated_at,
                    readiness_mode,
                    ready_at,
                    ready_reason,
                    readiness_token
                 ) VALUES (?1, 111, 8, 'MASTER_VERIFYING', 1, 1090, 1000, 1001, 'probe', 1002, 'probe-ready', 'tok')",
                ["s_schema_verify"],
            )
            .unwrap();

            drop(conn);
            assert_eq!(window_phase(db, "s_schema_verify"), "MASTER_VERIFYING");
            assert_eq!(
                window_readiness(db, "s_schema_verify"),
                (
                    "probe".to_string(),
                    Some(1002),
                    Some("probe-ready".to_string()),
                    "tok".to_string()
                )
            );
        });
    }

    #[test]
    fn master_recovery_readiness_wait_marks_master_verifying_and_is_generation_fenced() {
        with_db(|db| {
            seed_session(db, "s_verify_wait", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_verify_wait", 111, 8, true, 1000, 90)
                .unwrap();
            update_master_recovery_phase_sync(&conn, "s_verify_wait", 8, "MASTER_RUNNING", 1001)
                .unwrap();

            assert!(
                !begin_master_recovery_readiness_wait_sync(
                    &conn,
                    "s_verify_wait",
                    7,
                    "probe",
                    "stale-token",
                    1002,
                )
                .unwrap()
            );
            assert!(
                begin_master_recovery_readiness_wait_sync(
                    &conn,
                    "s_verify_wait",
                    8,
                    "probe",
                    "tok-8",
                    1003,
                )
                .unwrap()
            );

            drop(conn);
            assert_eq!(window_phase(db, "s_verify_wait"), "MASTER_VERIFYING");
            assert_eq!(
                window_readiness(db, "s_verify_wait"),
                ("probe".to_string(), None, None, "tok-8".to_string())
            );
        });
    }

    #[test]
    fn master_recovery_mark_ready_only_updates_master_verifying_window() {
        with_db(|db| {
            seed_session(db, "s_mark_ready", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_mark_ready", 111, 8, true, 1000, 90)
                .unwrap();
            assert!(
                !mark_master_recovery_ready_sync(&conn, "s_mark_ready", 8, "too-early", 1001)
                    .unwrap()
            );
            begin_master_recovery_readiness_wait_sync(
                &conn,
                "s_mark_ready",
                8,
                "ack",
                "ready-token",
                1002,
            )
            .unwrap();
            assert!(
                !mark_master_recovery_ready_sync(&conn, "s_mark_ready", 7, "stale", 1003).unwrap()
            );
            assert!(
                mark_master_recovery_ready_sync(&conn, "s_mark_ready", 8, "ack-ready", 1004)
                    .unwrap()
            );

            drop(conn);
            assert_eq!(
                window_readiness(db, "s_mark_ready"),
                (
                    "ack".to_string(),
                    Some(1004),
                    Some("ack-ready".to_string()),
                    "ready-token".to_string()
                )
            );
        });
    }

    #[test]
    fn master_recovery_fail_readiness_marks_failed_and_is_generation_fenced() {
        with_db(|db| {
            seed_session(db, "s_fail_ready", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_fail_ready", 111, 8, true, 1000, 90)
                .unwrap();
            begin_master_recovery_readiness_wait_sync(
                &conn,
                "s_fail_ready",
                8,
                "probe",
                "tok-fail",
                1001,
            )
            .unwrap();

            assert!(
                !fail_master_recovery_readiness_sync(&conn, "s_fail_ready", 7, "stale", 1002)
                    .unwrap()
            );
            assert!(
                fail_master_recovery_readiness_sync(
                    &conn,
                    "s_fail_ready",
                    8,
                    "probe-timeout",
                    1003,
                )
                .unwrap()
            );

            drop(conn);
            assert_eq!(window_phase(db, "s_fail_ready"), "FAILED");
            assert_eq!(
                window_readiness(db, "s_fail_ready").2.as_deref(),
                Some("probe-timeout")
            );
        });
    }

    #[test]
    fn master_recovery_effective_readiness_timeout_is_double_capped() {
        assert_eq!(effective_readiness_timeout(1000, 1090, 1000, 1, 5), 10);
        assert_eq!(effective_readiness_timeout(1000, 1090, 1000, 120, 5), 60);
        assert_eq!(effective_readiness_timeout(1000, 1020, 1000, 60, 5), 15);
        assert!(effective_readiness_timeout(1000, 1004, 1000, 30, 5) <= 0);
        assert_eq!(effective_readiness_timeout(1280, 1400, 1000, 60, 5), 15);
    }

    #[test]
    fn master_recovery_decide_treats_master_verifying_as_active_nonterminal_phase() {
        with_db(|db| {
            seed_session(db, "s_decide_verify", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_decide_verify", 111, 8, true, 1000, 90)
                .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_decide_verify",
                8,
                "MASTER_VERIFYING",
                1001,
            )
            .unwrap();

            assert_eq!(
                decide_anchor_cascade_sync(&conn, "s_decide_verify", 1050).unwrap(),
                AnchorCascadeDecision::Defer {
                    until: 1091,
                    phase: "MASTER_VERIFYING".to_string()
                }
            );
            assert_eq!(
                decide_anchor_cascade_sync(&conn, "s_decide_verify", 1092).unwrap(),
                AnchorCascadeDecision::CascadeNow {
                    reason: "MASTER_REVIVE_WINDOW_EXPIRED"
                }
            );
            drop(conn);
            assert_eq!(window_phase(db, "s_decide_verify"), "FAILED");
        });
    }

    #[test]
    fn master_recovery_anchor_decision_defers_unexpired_activework_window() {
        with_db(|db| {
            seed_session(db, "s_defer", 111, 7);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_defer", 111, 7, true, 1000, 90).unwrap();

            let decision = decide_anchor_cascade_sync(&conn, "s_defer", 1010).unwrap();

            assert_eq!(
                decision,
                AnchorCascadeDecision::Defer {
                    until: 1090,
                    phase: "DETECTED".to_string()
                }
            );
        });
    }

    #[test]
    fn master_recovery_anchor_decision_expires_window_and_cascades() {
        with_db(|db| {
            seed_session(db, "s_expire", 111, 7);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_expire", 111, 7, true, 1000, 90).unwrap();

            let decision = decide_anchor_cascade_sync(&conn, "s_expire", 1091).unwrap();

            assert_eq!(
                decision,
                AnchorCascadeDecision::CascadeNow {
                    reason: "MASTER_REVIVE_WINDOW_EXPIRED"
                }
            );
            let reason: String = conn
                .query_row(
                    "SELECT master_last_exit_reason FROM sessions WHERE id = 's_expire'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(reason, "MASTER_REVIVE_WINDOW_EXPIRED");
            drop(conn);
            assert_eq!(window_phase(db, "s_expire"), "FAILED");
        });
    }

    #[test]
    fn master_recovery_anchor_decision_creates_detected_window_for_toctou() {
        with_db(|db| {
            seed_session(db, "s_toctou", 222, 3);
            {
                let conn = db.conn();
                insert_agent_sync(&conn, "a_busy", "s_toctou", "codex", "BUSY", Some(10)).unwrap();
            }
            let conn = db.conn();

            let decision = decide_anchor_cascade_sync(&conn, "s_toctou", 2000).unwrap();

            assert_eq!(
                decision,
                AnchorCascadeDecision::Defer {
                    until: 2090,
                    phase: "DETECTED".to_string()
                }
            );
            drop(conn);
            assert_eq!(window_phase(db, "s_toctou"), "DETECTED");
        });
    }

    #[test]
    fn master_recovery_anchor_decision_does_not_defer_idle_no_work() {
        with_db(|db| {
            seed_session(db, "s_idle", 222, 3);
            {
                let conn = db.conn();
                insert_agent_sync(&conn, "a_idle", "s_idle", "codex", "IDLE", Some(10)).unwrap();
            }
            let conn = db.conn();

            let decision = decide_anchor_cascade_sync(&conn, "s_idle", 2000).unwrap();

            assert_eq!(
                decision,
                AnchorCascadeDecision::CascadeNow {
                    reason: "NO_MASTER_RECOVERY_WINDOW"
                }
            );
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM master_recovery_windows WHERE session_id = 's_idle'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        });
    }

    #[test]
    fn master_recovery_generation_fence_stale_update_and_complete_are_noops() {
        with_db(|db| {
            seed_session(db, "s_fence", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_fence", 111, 8, true, 1000, 90).unwrap();

            assert!(
                !update_master_recovery_phase_sync(&conn, "s_fence", 7, "MASTER_RUNNING", 1010)
                    .unwrap()
            );
            assert!(!complete_master_recovery_window_sync(&conn, "s_fence", 7, 1020).unwrap());

            drop(conn);
            assert_eq!(window_phase(db, "s_fence"), "DETECTED");
        });
    }

    #[test]
    fn master_recovery_begin_upsert_is_idempotent_and_preserves_existing_fields() {
        with_db(|db| {
            seed_session(db, "s_upsert", 111, 8);
            let conn = db.conn();
            let first =
                begin_master_recovery_window_sync(&conn, "s_upsert", 111, 8, true, 1000, 90)
                    .unwrap();
            update_master_recovery_phase_sync(&conn, "s_upsert", 8, "WORKERS_REAPED", 1010)
                .unwrap();
            let second =
                begin_master_recovery_window_sync(&conn, "s_upsert", 111, 8, false, 1020, 90)
                    .unwrap();

            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM master_recovery_windows WHERE session_id = 's_upsert'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
            assert_eq!(first.created_at, second.created_at);
            assert!(second.active_work);
            assert_eq!(second.phase, "WORKERS_REAPED");
        });
    }

    #[test]
    fn master_recovery_defer_extension_is_capped_by_total_window_limit() {
        with_db(|db| {
            seed_session(db, "s_cap", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_cap", 111, 8, true, 1000, 300).unwrap();

            update_master_recovery_phase_sync(&conn, "s_cap", 8, "MASTER_SPAWNING", 1290).unwrap();

            drop(conn);
            assert_eq!(window_defer_until(db, "s_cap"), 1300);
        });
    }

    #[test]
    fn master_recovery_expire_helper_only_fails_expired_active_windows() {
        with_db(|db| {
            seed_session(db, "s_expire_helper", 111, 8);
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_expire_helper", 111, 8, true, 1000, 90)
                .unwrap();

            assert!(!expire_master_recovery_window_sync(&conn, "s_expire_helper", 1080).unwrap());
            drop(conn);
            assert_eq!(window_phase(db, "s_expire_helper"), "DETECTED");
            let conn = db.conn();
            assert!(expire_master_recovery_window_sync(&conn, "s_expire_helper", 1091).unwrap());
            drop(conn);
            assert_eq!(window_phase(db, "s_expire_helper"), "FAILED");
        });
    }

    #[test]
    fn master_recovery_activework_snapshot_uses_queued_jobs() {
        with_db(|db| {
            seed_session(db, "s_queued", 111, 8);
            {
                let conn = db.conn();
                insert_agent_sync(&conn, "a_idle_queued", "s_queued", "codex", "IDLE", Some(1))
                    .unwrap();
                insert_job_sync(&conn, "j_queued", "a_idle_queued", None, "work").unwrap();
            }
            let conn = db.conn();

            let decision = decide_anchor_cascade_sync(&conn, "s_queued", 3000).unwrap();

            assert!(matches!(decision, AnchorCascadeDecision::Defer { .. }));
        });
    }
}
