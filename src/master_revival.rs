use crate::db::Db;
use crate::db::common::map_db_error;
use crate::db::master_recovery::{
    MASTER_REVIVE_READINESS_CLEANUP_MARGIN_SECS, begin_master_recovery_readiness_wait_sync,
    begin_master_recovery_window_sync, complete_master_recovery_window_sync,
    effective_readiness_timeout, fail_master_recovery_readiness_sync,
    mark_master_recovery_ready_sync, resolve_master_recovery_cascade_defer_secs,
    update_master_recovery_phase_sync,
};
use crate::db::system::MasterDeathSessionActivity;
use crate::error::AhError;
use rusqlite::{OptionalExtension, params};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::Mutex as AsyncMutex;

const MASTER_REVIVE_FUSE_THRESHOLD: i64 = 5;

static MASTER_SPAWN_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterDeathDecision {
    Revive,
    IntentionalExit,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterTransitionOutcome {
    Claimed,
    Stale,
    NoChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterReviveAttemptDecision {
    Spawn {
        retry_count: i64,
        next_retry_at: i64,
    },
    Fused,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviveSessionMasterRequest {
    pub session_id: String,
    pub expected_pid: i64,
    pub expected_generation: i64,
    pub new_pid: i64,
    pub new_pane_id: String,
    pub master_sandbox_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviveSessionMasterOutcome {
    pub generation: i64,
    pub monitor_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MasterRuntime {
    pub pid: i64,
    pub generation: i64,
}

pub fn classify_master_death(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<MasterDeathDecision, AhError> {
    let row = {
        let conn = db.conn();
        conn.query_row(
            "SELECT status, master_pid, master_generation FROM sessions WHERE id = ?1",
            params![session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("classify master death", err))?
    };
    let Some((status, master_pid, master_generation)) = row else {
        return Ok(MasterDeathDecision::Stale);
    };
    if status != "ACTIVE" {
        return Ok(MasterDeathDecision::IntentionalExit);
    }
    if crate::db::master_cutovers::master_cutover_has_inflight_state(db, session_id)? {
        return Ok(MasterDeathDecision::IntentionalExit);
    }
    if master_pid == expected_pid && master_generation == expected_generation {
        Ok(MasterDeathDecision::Revive)
    } else {
        Ok(MasterDeathDecision::Stale)
    }
}

pub fn try_claim_master_transition(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<MasterTransitionOutcome, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE sessions
             SET master_generation = master_generation + 1,
                 master_state = 'IDLE',
                 master_pending_tell_request = NULL
             WHERE id = ?1
               AND status = 'ACTIVE'
               AND master_pid = ?2
               AND master_generation = ?3",
            params![session_id, expected_pid, expected_generation],
        )
        .map_err(|err| map_db_error("claim master transition", err))?;
    if changes == 1 {
        Ok(MasterTransitionOutcome::Claimed)
    } else {
        Ok(MasterTransitionOutcome::Stale)
    }
}

pub fn query_master_runtime(db: &Db, session_id: &str) -> Result<Option<MasterRuntime>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT master_pid, master_generation FROM sessions
         WHERE id = ?1 AND status = 'ACTIVE'",
        params![session_id],
        |row| {
            Ok(MasterRuntime {
                pid: row.get(0)?,
                generation: row.get(1)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query master runtime", err))
}

pub(crate) fn master_runtime_matches(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT 1 FROM sessions
         WHERE id = ?1
           AND status = 'ACTIVE'
           AND master_pid = ?2
           AND master_generation = ?3",
        params![session_id, expected_pid, expected_generation],
        |_| Ok(true),
    )
    .optional()
    .map(|value| value.unwrap_or(false))
    .map_err(|err| {
        AhError::DbConstraintViolation(format!("query master revive readiness runtime: {err}"))
    })
}

pub(crate) fn master_runtime_generation_matches(
    db: &Db,
    session_id: &str,
    master_pid: i64,
    generation: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT 1 FROM sessions
         WHERE id = ?1
           AND master_pid = ?2
           AND master_generation = ?3",
        params![session_id, master_pid, generation],
        |_| Ok(true),
    )
    .optional()
    .map(|value| value.unwrap_or(false))
    .map_err(|err| {
        AhError::DbConstraintViolation(format!("query failed revive master runtime fence: {err}"))
    })
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MasterRecoveryWindowTiming {
    pub defer_until: i64,
    pub created_at: i64,
}

pub(crate) fn master_recovery_window_timing(
    db: &Db,
    session_id: &str,
) -> Result<Option<MasterRecoveryWindowTiming>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT defer_until, created_at FROM master_recovery_windows WHERE session_id = ?1",
        params![session_id],
        |row| {
            Ok(MasterRecoveryWindowTiming {
                defer_until: row.get(0)?,
                created_at: row.get(1)?,
            })
        },
    )
    .optional()
    .map_err(|err| {
        AhError::DbConstraintViolation(format!("query master recovery window timing: {err}"))
    })
}

pub(crate) fn master_recovery_verifying_window_expected_generation(
    db: &Db,
    session_id: &str,
) -> Result<Option<i64>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT expected_generation FROM master_recovery_windows
         WHERE session_id = ?1 AND phase = 'MASTER_VERIFYING'",
        params![session_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(|err| {
        AhError::DbConstraintViolation(format!(
            "query master recovery verifying window generation: {err}"
        ))
    })
}

pub(crate) fn master_recovery_effective_readiness_timeout(
    db: &Db,
    session_id: &str,
    now: i64,
    configured_timeout_secs: i64,
) -> Result<Option<i64>, AhError> {
    let Some(timing) = master_recovery_window_timing(db, session_id)? else {
        return Ok(None);
    };
    Ok(Some(effective_readiness_timeout(
        now,
        timing.defer_until,
        timing.created_at,
        configured_timeout_secs,
        MASTER_REVIVE_READINESS_CLEANUP_MARGIN_SECS,
    )))
}

pub(crate) fn begin_master_recovery_readiness_wait_for_master_watch(
    db: &Db,
    session_id: &str,
    expected_generation: i64,
    readiness_mode: &str,
    readiness_token: &str,
    now: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    begin_master_recovery_readiness_wait_sync(
        &conn,
        session_id,
        expected_generation,
        readiness_mode,
        readiness_token,
        now,
    )
}

pub(crate) fn mark_master_recovery_ready_for_master_watch(
    db: &Db,
    session_id: &str,
    expected_generation: i64,
    ready_reason: &str,
    now: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    mark_master_recovery_ready_sync(&conn, session_id, expected_generation, ready_reason, now)
}

pub(crate) fn fail_master_recovery_readiness_for_master_watch(
    db: &Db,
    session_id: &str,
    expected_generation: i64,
    reason: &str,
    now: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    fail_master_recovery_readiness_sync(&conn, session_id, expected_generation, reason, now)
}

pub(crate) fn recovered_workers_ready_sync(db: &Db, agent_ids: &[String]) -> Result<bool, AhError> {
    let conn = db.conn();
    for agent_id in agent_ids {
        let state = conn
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| {
                AhError::DbConstraintViolation(format!(
                    "query recovered worker readiness state: {err}"
                ))
            })?;
        if !matches!(
            state.as_deref(),
            Some("IDLE" | "WAITING_FOR_ACK" | "BUSY" | "PROMPT_PENDING")
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn begin_master_recovery_window_for_snapshot(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    classification: MasterDeathSessionActivity,
    now: i64,
) -> Result<(), AhError> {
    let conn = db.conn();
    begin_master_recovery_window_sync(
        &conn,
        session_id,
        expected_pid,
        expected_generation,
        classification == MasterDeathSessionActivity::ActiveWork,
        now,
        resolve_master_recovery_cascade_defer_secs(),
    )?;
    Ok(())
}

pub(crate) fn mark_master_recovery_phase(
    db: &Db,
    session_id: &str,
    expected_generation: i64,
    phase: &str,
    now: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    update_master_recovery_phase_sync(&conn, session_id, expected_generation, phase, now)
}

pub(crate) fn complete_master_recovery_window_for_master_watch(
    db: &Db,
    session_id: &str,
    expected_generation: i64,
    now: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    complete_master_recovery_window_sync(&conn, session_id, expected_generation, now)
}

pub(crate) fn mark_master_recovery_non_revive_terminal(
    db: &Db,
    session_id: &str,
    expected_generation: i64,
    now: i64,
) -> Result<bool, AhError> {
    mark_master_recovery_phase(db, session_id, expected_generation, "FAILED", now)
}

pub(crate) fn mark_session_closed_after_idle_master_death(
    db: &Db,
    session_id: &str,
) -> Result<(), AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE sessions
         SET status = 'CLOSED',
             master_state = 'IDLE',
             master_pending_tell_request = NULL,
             master_last_exit_reason = 'IDLE_MASTER_EXIT'
         WHERE id = ?1
           AND status = 'ACTIVE'",
            params![session_id],
        )
        .map_err(|err| {
            AhError::DbConstraintViolation(format!("mark idle master death failed: {err}"))
        })?;
    if changes > 0 {
        notify_runtime_inventory_changed();
    }
    Ok(())
}

pub(crate) fn persist_revived_master_cmd(
    db: &Db,
    session_id: &str,
    master_cmd: &str,
) -> Result<(), AhError> {
    db.conn()
        .execute(
            "UPDATE sessions SET master_cmd = ?2 WHERE id = ?1",
            params![session_id, master_cmd],
        )
        .map_err(|err| {
            AhError::DbConstraintViolation(format!("persist revived master_cmd: {err}"))
        })?;
    Ok(())
}

fn warn_if_master_auth_missing(session_id: &str, master_sandbox_home: &std::path::Path) {
    if !master_sandbox_home.join(".claude").exists() {
        tracing::warn!(
            session_id = %session_id,
            home = %master_sandbox_home.display(),
            "reviving master with missing claude config dir"
        );
    }
}

pub fn complete_claimed_master_transition(
    db: &Db,
    session_id: &str,
    claimed_generation: i64,
    new_pid: i64,
    new_pane_id: &str,
    master_sandbox_home: &std::path::Path,
) -> Result<Option<ReviveSessionMasterOutcome>, AhError> {
    warn_if_master_auth_missing(session_id, master_sandbox_home);
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE sessions
             SET master_pid = ?3,
                 master_pane_id = ?4,
                 master_state = 'IDLE',
                 master_pending_tell_request = NULL,
                 master_last_exit_reason = 'OOM_OR_CRASH'
             WHERE id = ?1
               AND status = 'ACTIVE'
               AND master_generation = ?2",
            params![session_id, claimed_generation, new_pid, new_pane_id],
        )
        .map_err(|err| map_db_error("complete claimed master transition", err))?;
    if changes != 1 {
        return Ok(None);
    }
    Ok(Some(ReviveSessionMasterOutcome {
        generation: claimed_generation,
        monitor_key: master_monitor_key(session_id, claimed_generation),
    }))
}

pub fn revive_session_master(
    db: &Db,
    request: ReviveSessionMasterRequest,
) -> Result<ReviveSessionMasterOutcome, AhError> {
    if try_claim_master_transition(
        db,
        &request.session_id,
        request.expected_pid,
        request.expected_generation,
    )? != MasterTransitionOutcome::Claimed
    {
        return Ok(ReviveSessionMasterOutcome {
            generation: request.expected_generation,
            monitor_key: master_monitor_key(&request.session_id, request.expected_generation),
        });
    }
    let generation = request.expected_generation + 1;
    complete_claimed_master_transition(
        db,
        &request.session_id,
        generation,
        request.new_pid,
        &request.new_pane_id,
        &request.master_sandbox_home,
    )?
    .ok_or_else(|| AhError::IpcInvalidRequest("claimed master transition went stale".into()))
}

pub fn query_master_revive_next_retry_at(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<Option<i64>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT master_next_retry_at FROM sessions
         WHERE id = ?1
           AND status = 'ACTIVE'
           AND master_pid = ?2
           AND master_generation = ?3",
        params![session_id, expected_pid, expected_generation],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(|err| map_db_error("query master revive next retry", err))
}

pub fn record_master_revive_attempt(
    db: &Db,
    session_id: &str,
    current_pid: i64,
    claimed_generation: i64,
    now: i64,
) -> Result<MasterReviveAttemptDecision, AhError> {
    let (new_count, next_retry_at) = {
        let conn = db.conn();
        let retry_count = conn
            .query_row(
                "SELECT master_retry_count FROM sessions
                 WHERE id = ?1
                   AND status = 'ACTIVE'
                   AND master_pid = ?2
                   AND master_generation = ?3",
                params![session_id, current_pid, claimed_generation],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query master revive attempt", err))?;
        let Some(retry_count) = retry_count else {
            return Ok(MasterReviveAttemptDecision::Stale);
        };
        let new_count = retry_count + 1;
        let backoff = 1_i64 << (new_count.saturating_sub(1).min(4) as u32);
        let next_retry_at = now + backoff;
        conn.execute(
            "UPDATE sessions
             SET master_retry_count = ?4,
                 master_next_retry_at = ?5,
                 master_last_exit_reason = 'REVIVE_FAILED'
             WHERE id = ?1
               AND status = 'ACTIVE'
               AND master_pid = ?2
               AND master_generation = ?3",
            params![
                session_id,
                current_pid,
                claimed_generation,
                new_count,
                next_retry_at
            ],
        )
        .map_err(|err| map_db_error("record master revive attempt", err))?;
        (new_count, next_retry_at)
    };
    if new_count >= MASTER_REVIVE_FUSE_THRESHOLD {
        let _ = fuse_session_after_master_revive_exhausted(db, session_id)?;
        return Ok(MasterReviveAttemptDecision::Fused);
    }
    Ok(MasterReviveAttemptDecision::Spawn {
        retry_count: new_count,
        next_retry_at,
    })
}

pub fn confirm_master_stable(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<MasterTransitionOutcome, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE sessions
             SET master_retry_count = 0,
                 master_next_retry_at = 0
             WHERE id = ?1
               AND status = 'ACTIVE'
               AND master_pid = ?2
               AND master_generation = ?3",
            params![session_id, expected_pid, expected_generation],
        )
        .map_err(|err| map_db_error("confirm master stable", err))?;
    if changes == 1 {
        Ok(MasterTransitionOutcome::Claimed)
    } else {
        Ok(MasterTransitionOutcome::Stale)
    }
}

pub fn mark_all_sessions_intentional_shutdown(db: &Db) -> Result<usize, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark sessions intentional shutdown", err))?;
    let mut stmt = tx
        .prepare("SELECT id FROM sessions WHERE status = 'ACTIVE'")
        .map_err(|err| map_db_error("prepare active sessions for intentional shutdown", err))?;
    let active_session_ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|err| map_db_error("query active sessions for intentional shutdown", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect active sessions for intentional shutdown", err))?;
    drop(stmt);

    let sessions = tx
        .execute(
            "UPDATE sessions
             SET status = 'KILLED',
                 master_state = 'IDLE',
                 master_pending_tell_request = NULL,
                 master_last_exit_reason = 'INTENTIONAL_KILL'
             WHERE status = 'ACTIVE'",
            [],
        )
        .map_err(|err| map_db_error("mark sessions intentional shutdown", err))?;
    for session_id in active_session_ids {
        tx.execute(
            "UPDATE agents
             SET state = 'KILLED',
                 state_version = state_version + 1,
                 updated_at = unixepoch()
             WHERE session_id = ?
               AND state NOT IN ('CRASHED', 'KILLED')",
            rusqlite::params![session_id],
        )
        .map_err(|err| map_db_error("mark agents intentional shutdown", err))?;
    }
    tx.commit()
        .map_err(|err| map_db_error("commit mark sessions intentional shutdown", err))?;
    if sessions > 0 {
        notify_runtime_inventory_changed();
        notify_runtime_agent_changed();
    }
    Ok(sessions)
}

pub fn fuse_session_after_master_revive_exhausted(
    db: &Db,
    session_id: &str,
) -> Result<usize, AhError> {
    let retry_count = {
        let conn = db.conn();
        conn.query_row(
            "SELECT master_retry_count FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query master revive fuse retry count", err))?
        .unwrap_or_default()
    };
    tracing::error!(
        session_id = %session_id,
        retry_count,
        last_error = "master revive attempts exhausted",
        "fusing session after repeated master revive failures"
    );
    let changes = {
        let conn = db.conn();
        conn.execute(
            "UPDATE sessions
             SET status = 'FAILED',
                 master_state = 'IDLE',
                 master_pending_tell_request = NULL,
                 master_last_exit_reason = 'FUSED'
             WHERE id = ?1",
            params![session_id],
        )
        .map_err(|err| map_db_error("fuse session after master revive exhausted", err))?
    };
    if changes > 0 {
        notify_runtime_inventory_changed();
    }
    crate::db::system::stop_session_anchor_for_session_sync(session_id);
    Ok(0)
}

pub fn master_monitor_key(session_id: &str, generation: i64) -> String {
    format!("master:{session_id}:{generation}")
}

pub fn remove_master_monitor_key_if_generation_matches(
    session_id: &str,
    expected_generation: i64,
) -> bool {
    crate::monitor::remove(&master_monitor_key(session_id, expected_generation)).is_some()
}

pub fn master_spawn_lock(session_id: &str) -> Arc<AsyncMutex<()>> {
    let mut locks = MASTER_SPAWN_LOCKS
        .lock()
        .expect("master spawn locks mutex poisoned");
    locks
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

pub fn master_revive_in_flight(session_id: &str) -> bool {
    let lock = {
        let locks = MASTER_SPAWN_LOCKS
            .lock()
            .expect("master spawn locks mutex poisoned");
        locks.get(session_id).cloned()
    };
    let Some(lock) = lock else {
        return false;
    };
    lock.try_lock().is_err()
}

pub fn record_spawned_master_runtime(
    db: &Db,
    session_id: &str,
    pane_id: &str,
    pid: i64,
) -> Result<i64, AhError> {
    let conn = db.conn();
    conn.execute(
        "UPDATE sessions
         SET master_pid = ?2,
             master_pane_id = ?3,
             master_generation = master_generation + 1,
             master_state = 'IDLE',
             master_pending_tell_request = NULL
         WHERE id = ?1",
        params![session_id, pid, pane_id],
    )
    .map_err(|err| map_db_error("record spawned master runtime", err))?;
    let generation = conn
        .query_row(
            "SELECT master_generation FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|err| map_db_error("query spawned master generation", err))?;
    notify_runtime_tmux_changed();
    Ok(generation)
}

pub fn record_spawned_master_runtime_after_claim(
    db: &Db,
    session_id: &str,
    pane_id: &str,
    pid: i64,
    claimed_generation: i64,
) -> Result<Option<i64>, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE sessions
             SET master_pid = ?3,
                 master_pane_id = ?4,
                 master_state = 'IDLE',
                 master_pending_tell_request = NULL
             WHERE id = ?1
               AND status = 'ACTIVE'
               AND master_generation = ?2",
            params![session_id, claimed_generation, pid, pane_id],
        )
        .map_err(|err| map_db_error("record claimed spawned master runtime", err))?;
    if changes == 1 {
        notify_runtime_tmux_changed();
        Ok(Some(claimed_generation))
    } else {
        Ok(None)
    }
}

pub fn mark_session_intentional_killed(db: &Db, session_id: &str) -> Result<usize, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE sessions
         SET status = 'KILLED',
             master_state = 'IDLE',
             master_pending_tell_request = NULL,
             master_last_exit_reason = 'INTENTIONAL_KILL'
         WHERE id = ?1 AND status = 'ACTIVE'",
            params![session_id],
        )
        .map_err(|err| map_db_error("mark session intentional killed", err))?;
    if changes > 0 {
        notify_runtime_inventory_changed();
    }
    Ok(changes)
}

fn notify_runtime_inventory_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::InventoryChanged,
    );
}

fn notify_runtime_tmux_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::TmuxChanged,
    );
}

fn notify_runtime_agent_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::AgentChanged,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::sessions::insert_session_sync;

    fn with_test_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        test(&db)
    }

    fn seed_session(db: &Db, session_id: &str, status: &str, pid: i64, generation: i64) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            session_id,
            &format!("project_{session_id}"),
            &format!("/tmp/{session_id}"),
        )
        .unwrap();
        conn.execute(
            "UPDATE sessions
             SET status = ?2, master_pid = ?3, master_generation = ?4, master_pane_id = ?5
             WHERE id = ?1",
            rusqlite::params![
                session_id,
                status,
                pid,
                generation,
                format!("%{session_id}:1.0")
            ],
        )
        .unwrap();
    }

    fn session_column_i64(db: &Db, session_id: &str, column: &str) -> i64 {
        db.conn()
            .query_row(
                &format!("SELECT {column} FROM sessions WHERE id = ?1"),
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn session_column_string(db: &Db, session_id: &str, column: &str) -> String {
        db.conn()
            .query_row(
                &format!("SELECT {column} FROM sessions WHERE id = ?1"),
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn master_revive_schema_migration_is_idempotent() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let _db = crate::db::init(file.path()).unwrap();
        }
        let db = crate::db::init(file.path()).unwrap();
        let columns = db
            .conn()
            .prepare("PRAGMA table_info(sessions)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for expected in [
            "master_retry_count",
            "master_next_retry_at",
            "master_generation",
            "master_last_exit_reason",
        ] {
            assert!(columns.iter().any(|column| column == expected));
        }
    }

    #[test]
    fn master_revive_status_active_revives_killed_ignores() {
        with_test_db(|db| {
            seed_session(db, "s_active", "ACTIVE", 111, 7);
            seed_session(db, "s_killed", "KILLED", 222, 3);

            assert_eq!(
                classify_master_death(db, "s_active", 111, 7).unwrap(),
                MasterDeathDecision::Revive
            );
            assert_eq!(
                classify_master_death(db, "s_killed", 222, 3).unwrap(),
                MasterDeathDecision::IntentionalExit
            );
        });
    }

    #[test]
    fn master_runtime_fences_are_generation_scoped() {
        with_test_db(|db| {
            seed_session(db, "s_runtime_fence", "ACTIVE", 111, 7);

            assert!(master_runtime_matches(db, "s_runtime_fence", 111, 7).unwrap());
            assert!(master_runtime_generation_matches(db, "s_runtime_fence", 111, 7).unwrap());
            assert!(!master_runtime_matches(db, "s_runtime_fence", 111, 8).unwrap());
            assert!(!master_runtime_generation_matches(db, "s_runtime_fence", 222, 7).unwrap());
        });
    }

    #[test]
    fn idle_master_death_close_is_a_revival_decision_transition() {
        with_test_db(|db| {
            seed_session(db, "s_idle_close", "ACTIVE", 111, 7);

            mark_session_closed_after_idle_master_death(db, "s_idle_close").unwrap();

            assert_eq!(
                session_column_string(db, "s_idle_close", "status"),
                "CLOSED"
            );
            assert_eq!(
                session_column_string(db, "s_idle_close", "master_last_exit_reason"),
                "IDLE_MASTER_EXIT"
            );
        });
    }

    #[test]
    fn master_revive_ignores_active_session_with_inflight_cutover() {
        with_test_db(|db| {
            seed_session(db, "s_cutover_revive", "ACTIVE", 333, 4);
            assert_eq!(
                crate::db::master_cutovers::claim_master_cutover(
                    db,
                    "cutover_revive",
                    "s_cutover_revive",
                    Some(222),
                    "/tmp/ah-state",
                    "/tmp/ah-state/ahd.sock",
                    "/tmp/ah-state/cutovers/cutover_revive/handoff.md",
                )
                .unwrap(),
                crate::db::master_cutovers::MasterCutoverClaim::Claimed
            );

            assert_eq!(
                classify_master_death(db, "s_cutover_revive", 333, 4).unwrap(),
                MasterDeathDecision::IntentionalExit
            );
        });
    }

    #[test]
    fn master_revive_updates_pid_generation_and_uses_generation_key() {
        with_test_db(|db| {
            seed_session(db, "s_revive", "ACTIVE", 111, 7);
            let outcome = revive_session_master(
                db,
                ReviveSessionMasterRequest {
                    session_id: "s_revive".to_string(),
                    expected_pid: 111,
                    expected_generation: 7,
                    new_pid: 333,
                    new_pane_id: "%revived:1.0".to_string(),
                    master_sandbox_home: tempfile::tempdir().unwrap().path().join("home"),
                },
            )
            .unwrap();

            assert_eq!(outcome.generation, 8);
            assert_eq!(outcome.monitor_key, "master:s_revive:8");
            assert_eq!(session_column_i64(db, "s_revive", "master_pid"), 333);
            assert_eq!(session_column_i64(db, "s_revive", "master_generation"), 8);
            assert_eq!(
                session_column_string(db, "s_revive", "master_pane_id"),
                "%revived:1.0"
            );
            assert!(!remove_master_monitor_key_if_generation_matches(
                "s_revive", 7
            ));
            db.conn()
                .execute(
                    "UPDATE sessions SET master_retry_count = 3, master_next_retry_at = 99 WHERE id = 's_revive'",
                    [],
                )
                .unwrap();
            assert_eq!(
                confirm_master_stable(db, "s_revive", 333, 999).unwrap(),
                MasterTransitionOutcome::Stale
            );
            assert_eq!(session_column_i64(db, "s_revive", "master_retry_count"), 3);
            assert_eq!(
                confirm_master_stable(db, "s_revive", 333, 8).unwrap(),
                MasterTransitionOutcome::Claimed
            );
            assert_eq!(session_column_i64(db, "s_revive", "master_retry_count"), 0);
        });
    }

    #[test]
    fn master_revive_transition_cas_rejects_stale_and_allows_one_winner() {
        with_test_db(|db| {
            seed_session(db, "s_cas", "ACTIVE", 111, 4);

            assert_eq!(
                try_claim_master_transition(db, "s_cas", 999, 4).unwrap(),
                MasterTransitionOutcome::Stale
            );
            assert_eq!(
                try_claim_master_transition(db, "s_cas", 111, 99).unwrap(),
                MasterTransitionOutcome::Stale
            );
            assert_eq!(
                try_claim_master_transition(db, "s_cas", 111, 4).unwrap(),
                MasterTransitionOutcome::Claimed
            );
            assert_eq!(
                try_claim_master_transition(db, "s_cas", 111, 4).unwrap(),
                MasterTransitionOutcome::Stale
            );
        });
    }

    #[test]
    fn master_revive_startup_crash_loop_counts_attempts_and_fuses_without_worker_cleanup() {
        with_test_db(|db| {
            seed_session(db, "s_loop", "ACTIVE", 100, 0);
            {
                let conn = db.conn();
                insert_agent_sync(&conn, "a_loop", "s_loop", "bash", "IDLE", Some(10)).unwrap();
            }

            let mut completed_spawns = 0;
            for attempt in 1..=5 {
                let expected_pid = session_column_i64(db, "s_loop", "master_pid");
                let expected_generation = session_column_i64(db, "s_loop", "master_generation");
                assert_eq!(
                    try_claim_master_transition(db, "s_loop", expected_pid, expected_generation)
                        .unwrap(),
                    MasterTransitionOutcome::Claimed
                );
                let claimed_generation = expected_generation + 1;
                let decision = record_master_revive_attempt(
                    db,
                    "s_loop",
                    expected_pid,
                    claimed_generation,
                    1_000 + attempt,
                )
                .unwrap();

                if attempt < 5 {
                    let MasterReviveAttemptDecision::Spawn {
                        retry_count,
                        next_retry_at,
                    } = decision
                    else {
                        panic!("attempt {attempt} should still be allowed to spawn");
                    };
                    assert_eq!(retry_count, attempt);
                    assert!(next_retry_at > 1_000 + attempt);
                    assert_eq!(
                        query_master_revive_next_retry_at(
                            db,
                            "s_loop",
                            expected_pid,
                            claimed_generation
                        )
                        .unwrap(),
                        Some(next_retry_at)
                    );
                    let new_pid = expected_pid + 1;
                    assert!(
                        complete_claimed_master_transition(
                            db,
                            "s_loop",
                            claimed_generation,
                            new_pid,
                            &format!("%loop:{attempt}.0"),
                            &tempfile::tempdir().unwrap().path().join("home"),
                        )
                        .unwrap()
                        .is_some()
                    );
                    completed_spawns += 1;
                    assert_eq!(session_column_string(db, "s_loop", "status"), "ACTIVE");
                } else {
                    assert_eq!(decision, MasterReviveAttemptDecision::Fused);
                    assert_eq!(session_column_string(db, "s_loop", "status"), "FAILED");
                    assert_eq!(
                        session_column_string(db, "s_loop", "master_last_exit_reason"),
                        "FUSED"
                    );
                }
            }

            assert_eq!(completed_spawns, 4);
            assert_eq!(session_column_i64(db, "s_loop", "master_retry_count"), 5);
            let killed_workers: i64 = db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM agents WHERE session_id = 's_loop' AND state = 'KILLED'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(killed_workers, 0);
        });
    }

    #[test]
    fn master_revive_intentional_shutdown_marks_killed_before_signal() {
        with_test_db(|db| {
            seed_session(db, "s_shutdown_a", "ACTIVE", 111, 1);
            seed_session(db, "s_shutdown_b", "ACTIVE", 222, 1);

            assert_eq!(mark_all_sessions_intentional_shutdown(db).unwrap(), 2);
            for session_id in ["s_shutdown_a", "s_shutdown_b"] {
                assert_eq!(session_column_string(db, session_id, "status"), "KILLED");
                assert_eq!(
                    session_column_string(db, session_id, "master_last_exit_reason"),
                    "INTENTIONAL_KILL"
                );
            }
        });
    }

    #[test]
    fn bug_b_intentional_shutdown_marks_active_agents_killed_for_later_reuse() {
        with_test_db(|db| {
            seed_session(db, "s_shutdown_agents", "ACTIVE", 111, 1);
            {
                let conn = db.conn();
                crate::db::agents::insert_agent_sync(
                    &conn,
                    "a_shutdown_reuse",
                    "s_shutdown_agents",
                    "bash",
                    "IDLE",
                    Some(123),
                )
                .unwrap();
            }

            assert_eq!(mark_all_sessions_intentional_shutdown(db).unwrap(), 1);

            let state: String = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = 'a_shutdown_reuse'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(state, "KILLED");
        });
    }

    #[test]
    fn master_revive_preserves_claude_transcript_sentinel() {
        with_test_db(|db| {
            seed_session(db, "s_transcript", "ACTIVE", 111, 1);
            let sandbox = tempfile::tempdir().unwrap();
            let sentinel = sandbox
                .path()
                .join("home/.claude/projects/project/transcript.jsonl");
            std::fs::create_dir_all(sentinel.parent().unwrap()).unwrap();
            std::fs::write(&sentinel, "keep me").unwrap();

            let _ = revive_session_master(
                db,
                ReviveSessionMasterRequest {
                    session_id: "s_transcript".to_string(),
                    expected_pid: 111,
                    expected_generation: 1,
                    new_pid: 222,
                    new_pane_id: "%transcript:1.0".to_string(),
                    master_sandbox_home: sandbox.path().join("home"),
                },
            )
            .unwrap();

            assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "keep me");
        });
    }

    #[test]
    fn master_revive_fuses_after_fifth_failure_without_cascading_workers() {
        with_test_db(|db| {
            seed_session(db, "s_fuse", "ACTIVE", 111, 1);
            {
                let conn = db.conn();
                insert_agent_sync(&conn, "a_fuse_1", "s_fuse", "bash", "IDLE", Some(10)).unwrap();
                insert_agent_sync(&conn, "a_fuse_2", "s_fuse", "bash", "BUSY", Some(11)).unwrap();
                conn.execute(
                    "UPDATE sessions SET master_retry_count = 5 WHERE id = 's_fuse'",
                    [],
                )
                .unwrap();
            }

            assert_eq!(
                fuse_session_after_master_revive_exhausted(db, "s_fuse").unwrap(),
                0
            );
            assert_eq!(session_column_string(db, "s_fuse", "status"), "FAILED");
            assert_eq!(
                session_column_string(db, "s_fuse", "master_last_exit_reason"),
                "FUSED"
            );
            let killed_workers: i64 = db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM agents WHERE session_id = 's_fuse' AND state = 'KILLED'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(killed_workers, 0);
        });
    }
}
