use crate::db::Db;
use crate::db::agents_lifecycle::{
    mark_agent_killed_for_master_death_sync, mark_agent_killed_sync,
};
use crate::db::common::{map_db_error, spawn_db};
use crate::db::jobs::mark_dispatched_jobs_failed_for_agent_conn_sync;
use crate::db::master_recovery::expire_master_recovery_window_sync;
use crate::db::state_machine::{
    STATE_BUSY, STATE_PROMPT_PENDING, STATE_SPAWNING, STATE_WAITING_FOR_ACK,
};
use crate::error::CcbdError;
use crate::monitor::session_watch::unit_name_for_session;
use crate::platform::sys::scope::RealSystemctlRunner;
pub(crate) use crate::platform::sys::scope::{ScopeUnit, SystemctlRunner};
use crate::tmux::TmuxPaneId;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DaemonMarkerProvenance {
    Explicit,
    Ambient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DaemonMarker {
    value: String,
    provenance: DaemonMarkerProvenance,
}

impl DaemonMarker {
    fn explicit(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            provenance: DaemonMarkerProvenance::Explicit,
        }
    }

    fn ambient(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            provenance: DaemonMarkerProvenance::Ambient,
        }
    }

    fn may_stop_scopes(&self) -> bool {
        self.provenance == DaemonMarkerProvenance::Explicit
    }
}

#[derive(Debug, Clone)]
struct StartupAgentCandidate {
    id: String,
    session_id: String,
    pid: Option<i64>,
    state: String,
    provider: String,
}

#[derive(Debug, Clone)]
struct StartupCrashCandidate {
    agent: StartupAgentCandidate,
    reason: &'static str,
}

struct StartupAliveIoCandidate {
    agent: StartupAgentCandidate,
    fifo_path: PathBuf,
    fifo_file: File,
    socket_name: String,
}

#[derive(Debug, Clone)]
struct StartupClaudeWorkerSeat {
    id: String,
    session_id: String,
}

#[derive(Debug, Clone, Default)]
struct StartupClaudeSeats {
    workers: Vec<StartupClaudeWorkerSeat>,
    sessions: Vec<String>,
}

pub fn recovery_eligible_orphan_scope_should_be_preserved(
    provider: &str,
    agent_state: &str,
) -> bool {
    agent_state == "CRASHED" && crate::provider::manifest::is_recovery_eligible_provider(provider)
}

pub(crate) fn system_dump_sync(db: &Db) -> Result<Value, CcbdError> {
    let conn = db.conn();
    let projects = {
        let mut stmt = conn
            .prepare("SELECT id, absolute_path, created_at FROM projects ORDER BY created_at ASC")
            .map_err(|err| map_db_error("prepare dump projects", err))?;
        stmt.query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "absolute_path": row.get::<_, String>(1)?,
                "created_at": row.get::<_, i64>(2)?,
            }))
        })
        .map_err(|err| map_db_error("query dump projects", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect dump projects", err))?
    };
    let sessions = {
        let mut stmt = conn
            .prepare(
                "SELECT id, project_id, status, created_at FROM sessions ORDER BY created_at ASC",
            )
            .map_err(|err| map_db_error("prepare dump sessions", err))?;
        stmt.query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "project_id": row.get::<_, String>(1)?,
                "status": row.get::<_, String>(2)?,
                "created_at": row.get::<_, i64>(3)?,
            }))
        })
        .map_err(|err| map_db_error("query dump sessions", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect dump sessions", err))?
    };
    let agents = {
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, provider, state, sub_state, state_version, pid, exit_code, error_code, created_at, updated_at FROM agents ORDER BY created_at ASC",
            )
            .map_err(|err| map_db_error("prepare dump agents", err))?;
        stmt.query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "session_id": row.get::<_, String>(1)?,
                "provider": row.get::<_, String>(2)?,
                "state": row.get::<_, String>(3)?,
                "sub_state": row.get::<_, Option<String>>(4)?,
                "state_version": row.get::<_, i64>(5)?,
                "pid": row.get::<_, Option<i64>>(6)?,
                "exit_code": row.get::<_, Option<i64>>(7)?,
                "error_code": row.get::<_, Option<String>>(8)?,
                "created_at": row.get::<_, i64>(9)?,
                "updated_at": row.get::<_, i64>(10)?,
            }))
        })
        .map_err(|err| map_db_error("query dump agents", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect dump agents", err))?
    };
    let evidence_pending = {
        let mut stmt = conn
            .prepare(
                "SELECT id, agent_id, status, created_at FROM evidence WHERE status = 'PENDING' ORDER BY created_at DESC LIMIT 100",
            )
            .map_err(|err| map_db_error("prepare dump evidence", err))?;
        stmt.query_map([], |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "agent_id": row.get::<_, String>(1)?,
                "status": row.get::<_, String>(2)?,
                "created_at": row.get::<_, i64>(3)?,
            }))
        })
        .map_err(|err| map_db_error("query dump evidence", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect dump evidence", err))?
    };

    Ok(json!({
        "projects": projects,
        "sessions": sessions,
        "agents": agents,
        "evidence_pending": evidence_pending,
        "monitors": crate::monitor::list_keys(),
    }))
}

pub(crate) fn cascade_kill_session_agents_sync(
    db: &Db,
    session_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    cascade_kill_session_agents_with_runner_sync(db, session_id, reason, None, &RealSystemctlRunner)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MasterDeathSessionActivity {
    ActiveWork,
    IdleNoWork,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MasterDeathSessionSnapshot {
    pub classification: MasterDeathSessionActivity,
    pub worker_ids_to_reap: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WorkerRuntimeCleanupOutcome {
    pub db_killed_count: usize,
    pub scope_stop_failures: usize,
    pub pidfd_kill_failures: usize,
    pub registry_cleanup_count: usize,
}

pub(crate) fn snapshot_master_death_session_activity(
    db: &Db,
    session_id: &str,
) -> Result<MasterDeathSessionSnapshot, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin master-death session activity snapshot", err))?;
    let worker_rows = {
        let mut stmt = tx
            .prepare(
                "SELECT id, state FROM agents WHERE session_id = ?1 ORDER BY created_at ASC, id ASC",
            )
            .map_err(|err| map_db_error("prepare master-death worker snapshot", err))?;
        let rows = stmt
            .query_map(params![session_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|err| map_db_error("query master-death worker snapshot", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect master-death worker snapshot", err))?
    };
    let has_active_worker = worker_rows.iter().any(|(_, state)| {
        matches!(
            state.as_str(),
            STATE_SPAWNING | STATE_WAITING_FOR_ACK | STATE_BUSY | STATE_PROMPT_PENDING
        )
    });
    let has_queued_or_dispatched_job: bool = tx
        .query_row(
            "SELECT 1 \
             FROM jobs \
             JOIN agents ON agents.id = jobs.agent_id \
             WHERE agents.session_id = ?1 \
               AND jobs.status IN ('QUEUED', 'DISPATCHED') \
             LIMIT 1",
            params![session_id],
            |_| Ok(true),
        )
        .optional()
        .map(|value| value.unwrap_or(false))
        .map_err(|err| map_db_error("query master-death queued/dispatched jobs", err))?;
    let worker_ids_to_reap = worker_rows
        .into_iter()
        .map(|(agent_id, _)| agent_id)
        .collect::<Vec<_>>();
    tx.commit()
        .map_err(|err| map_db_error("commit master-death session activity snapshot", err))?;
    Ok(MasterDeathSessionSnapshot {
        classification: if has_active_worker || has_queued_or_dispatched_job {
            MasterDeathSessionActivity::ActiveWork
        } else {
            MasterDeathSessionActivity::IdleNoWork
        },
        worker_ids_to_reap,
    })
}

pub(crate) fn clean_worker_runtime_resources_with_runner_sync(
    db: &Db,
    session_id: &str,
    worker_ids: &[String],
    reason: &str,
    daemon_marker: Option<&str>,
    preserve_session_anchor: bool,
    runner: &dyn SystemctlRunner,
) -> Result<WorkerRuntimeCleanupOutcome, CcbdError> {
    let mut outcome = WorkerRuntimeCleanupOutcome::default();
    let worker_set = worker_ids.iter().cloned().collect::<HashSet<_>>();

    for agent_id in worker_ids {
        let had_agent_io_entry = crate::agent_io::contains(agent_id);
        if had_agent_io_entry {
            outcome.registry_cleanup_count += 1;
        }
        if let Some(handle) = crate::marker::registry::take(agent_id) {
            let _ = handle.cancel_tx.send(());
            outcome.registry_cleanup_count += 1;
        }
        if crate::marker::parser_registry::remove(agent_id).is_some() {
            outcome.registry_cleanup_count += 1;
        }
        if crate::completion::registry::contains(agent_id) {
            crate::completion::registry::cancel(agent_id);
            outcome.registry_cleanup_count += 1;
        }
    }

    if let Some(daemon_marker) = daemon_marker {
        let scopes = match runner.list_scope_units() {
            Ok(scopes) => scopes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => {
                tracing::warn!(
                    session_id,
                    daemon_marker,
                    error = %err,
                    "failed to list systemd scope units during master-death worker cleanup"
                );
                Vec::new()
            }
        };
        for scope in scopes {
            let Some(agent_id) = worker_ids.iter().find(|agent_id| {
                scope
                    .description
                    .contains(&format!("ccbd-agent-{agent_id}@{daemon_marker}"))
            }) else {
                continue;
            };
            if let Err(err) = runner.stop_unit(&scope.unit) {
                outcome.scope_stop_failures += 1;
                tracing::warn!(
                    session_id,
                    agent_id = %agent_id,
                    unit = %scope.unit,
                    error = %err,
                    "failed to stop agent systemd scope during master-death worker cleanup"
                );
            }
        }
        if !preserve_session_anchor
            && let Err(err) = runner.stop_unit(&unit_name_for_session(session_id))
        {
            tracing::warn!(
                session_id,
                error = %err,
                "failed to stop session anchor during master-death worker cleanup"
            );
        }
    }

    for agent_id in worker_ids {
        let pidfd_ok =
            match crate::monitor::with_borrowed(agent_id, crate::monitor::pidfd_send_sigkill) {
                Some(Ok(())) => true,
                Some(Err(err)) => {
                    outcome.pidfd_kill_failures += 1;
                    tracing::warn!(
                        session_id,
                        agent_id = %agent_id,
                        error = %err,
                        "failed to SIGKILL worker pidfd during master-death cleanup"
                    );
                    false
                }
                None => {
                    outcome.pidfd_kill_failures += 1;
                    tracing::debug!(
                        session_id,
                        agent_id = %agent_id,
                        "no pidfd registered during master-death worker cleanup"
                    );
                    false
                }
            };
        let scoped_failed = daemon_marker.is_some() && outcome.scope_stop_failures > 0;
        if scoped_failed && !pidfd_ok {
            tracing::error!(
                session_id,
                agent_id = %agent_id,
                impact = "worker may remain until startup reconcile or OS cleanup",
                "scope stop and pidfd SIGKILL both failed during master-death worker cleanup"
            );
        }
        outcome.db_killed_count += mark_agent_killed_for_master_death_sync(db, agent_id, reason)?;
    }

    for agent_id in worker_set {
        let _ = crate::marker::parser_registry::remove(&agent_id);
        crate::completion::registry::cancel(&agent_id);
        let _ = crate::monitor::remove(&agent_id);
    }

    Ok(outcome)
}

pub(crate) fn clean_worker_runtime_resources_sync(
    db: &Db,
    session_id: &str,
    worker_ids: &[String],
    reason: &str,
    daemon_marker: Option<&str>,
    preserve_session_anchor: bool,
) -> Result<WorkerRuntimeCleanupOutcome, CcbdError> {
    clean_worker_runtime_resources_with_runner_sync(
        db,
        session_id,
        worker_ids,
        reason,
        daemon_marker,
        preserve_session_anchor,
        &RealSystemctlRunner,
    )
}

pub(crate) fn cascade_kill_session_agents_with_runner_sync(
    db: &Db,
    session_id: &str,
    reason: &str,
    daemon_marker: Option<&str>,
    runner: &dyn SystemctlRunner,
) -> Result<usize, CcbdError> {
    let status_changed = {
        let conn = db.conn();
        conn.execute(
            "UPDATE sessions SET status = 'KILLED' WHERE id = ?1 AND status = 'ACTIVE'",
            params![session_id],
        )
        .map_err(|err| map_db_error("mark session killed for cascade", err))?
    };
    if status_changed > 0 {
        crate::orchestrator::pubsub::notify_runtime_changed(
            crate::runtime_events::RuntimeSnapshotReason::InventoryChanged,
        );
    }
    if status_changed == 0 {
        let status = {
            let conn = db.conn();
            conn.query_row(
                "SELECT status FROM sessions WHERE id = ?1",
                params![session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query session status for cascade", err))?
        };
        if !matches!(status.as_deref(), Some("KILLED" | "FAILED" | "CLOSED")) {
            return Ok(0);
        }
    }

    let agent_ids = {
        let conn = db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT id FROM agents WHERE session_id = ? AND state NOT IN ('CRASHED', 'KILLED')",
            )
            .map_err(|err| map_db_error("prepare cascade agent query", err))?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))
            .map_err(|err| map_db_error("query cascade agents", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect cascade agents", err))?
    };

    if let Some(daemon_marker) = daemon_marker {
        stop_agent_scopes_with_runner(&agent_ids, daemon_marker, runner);
        stop_session_anchor_with_runner(session_id, runner);
    }

    let mut changed = 0;
    for agent_id in agent_ids {
        // The systemd scope stop above is the authoritative subtree reap path.
        // Keep pidfd SIGKILL as a fallback for unsafe/no-systemd sessions and
        // pre-scope agents so the main provider process still gets killed.
        match crate::monitor::with_borrowed(&agent_id, crate::monitor::pidfd_send_sigkill) {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                tracing::warn!(agent_id = %agent_id, error = %err, "failed to SIGKILL agent during cascade")
            }
            None => tracing::debug!(agent_id = %agent_id, "no pidfd registered during cascade"),
        }
        changed += mark_agent_killed_sync(db, &agent_id, reason)?;
        if let Some(handle) = crate::marker::registry::take(&agent_id) {
            let _ = handle.cancel_tx.send(());
        }
        let _ = crate::marker::parser_registry::remove(&agent_id);
    }
    Ok(changed)
}

fn stop_agent_scopes_with_runner(
    agent_ids: &[String],
    daemon_marker: &str,
    runner: &dyn SystemctlRunner,
) {
    let scopes = match runner.list_scope_units() {
        Ok(scopes) => scopes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return,
        Err(err) => {
            tracing::warn!(
                daemon_marker,
                error = %err,
                "failed to list systemd scope units during cascade scope teardown"
            );
            return;
        }
    };
    let agent_needles = agent_ids
        .iter()
        .map(|agent_id| (agent_id, format!("ccbd-agent-{agent_id}@{daemon_marker}")))
        .collect::<Vec<_>>();

    for scope in scopes {
        let Some((agent_id, _)) = agent_needles
            .iter()
            .find(|(_, needle)| scope.description.contains(needle.as_str()))
        else {
            continue;
        };
        match runner.stop_unit(&scope.unit) {
            Ok(()) => tracing::info!(
                agent_id = %agent_id,
                unit = %scope.unit,
                "stopped agent systemd scope during cascade"
            ),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                agent_id = %agent_id,
                unit = %scope.unit,
                error = %err,
                "failed to stop agent systemd scope during cascade; falling back to pidfd/tmux cleanup"
            ),
        }
    }
}

fn stop_session_anchor_with_runner(session_id: &str, runner: &dyn SystemctlRunner) {
    let unit_name = unit_name_for_session(session_id);
    match runner.stop_unit(&unit_name) {
        Ok(()) => tracing::info!(
            session_id = %session_id,
            unit = %unit_name,
            "stopped session anchor during cascade"
        ),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(
            session_id = %session_id,
            unit = %unit_name,
            error = %err,
            "failed to stop session anchor during cascade"
        ),
    }
}

pub(crate) fn stop_session_anchor_for_session_sync(session_id: &str) {
    stop_session_anchor_with_runner(session_id, &RealSystemctlRunner);
}

pub(crate) fn session_agent_ids_sync(db: &Db, session_id: &str) -> Result<Vec<String>, CcbdError> {
    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT id FROM agents WHERE session_id = ? ORDER BY created_at ASC, id ASC")
        .map_err(|err| map_db_error("prepare session agent ids", err))?;
    let rows = stmt
        .query_map(params![session_id], |row| row.get::<_, String>(0))
        .map_err(|err| map_db_error("query session agent ids", err))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect session agent ids", err))
}

/// Reconcile active agents during daemon startup.
#[cfg(test)]
pub(crate) fn reconcile_startup_sync(db: &Db) -> Result<usize, CcbdError> {
    reconcile_active_agents_to_crashed_sync(db, None, None)
}

pub(crate) fn reconcile_startup_sync_with_state_dir(
    db: &Db,
    state_dir: Option<&Path>,
) -> Result<usize, CcbdError> {
    reconcile_startup_sync_with_state_dir_and_runner(
        db,
        state_dir,
        &RealSystemctlRunner,
        unixepoch(),
        reconcile_orphan_scopes_dry_run_enabled(),
    )
}

fn reconcile_startup_sync_with_state_dir_and_runner(
    db: &Db,
    state_dir: Option<&Path>,
    runner: &dyn SystemctlRunner,
    now: i64,
    orphan_dry_run: bool,
) -> Result<usize, CcbdError> {
    let socket_name = state_dir.map(crate::tmux::compute_socket_name);
    let agents_count =
        reconcile_active_agents_to_crashed_sync(db, state_dir, socket_name.as_deref())?;
    let daemon_marker = state_dir
        .map(crate::tmux::compute_socket_name)
        .map(DaemonMarker::explicit)
        .unwrap_or_else(|| DaemonMarker::ambient("ccbd-unknown"));
    // Recovery windows are authoritative after ahd restart. Reconcile them before
    // ordinary orphan-scope cleanup so live recovery windows keep their scopes.
    let recovery_count =
        reconcile_master_recovery_windows_with_marker_sync(db, now, &daemon_marker, runner)?;
    let scopes_count =
        reconcile_orphan_scopes_with_marker_sync(db, runner, &daemon_marker, orphan_dry_run)?;
    Ok(agents_count + recovery_count + scopes_count)
}

pub(crate) fn reconcile_orphan_scopes_sync(
    db: &Db,
    daemon_marker: &str,
) -> Result<usize, CcbdError> {
    let dry_run = reconcile_orphan_scopes_dry_run_enabled();
    reconcile_orphan_scopes_with_runner_sync(db, &RealSystemctlRunner, daemon_marker, dry_run)
}

fn reconcile_orphan_scopes_dry_run_enabled() -> bool {
    std::env::var("CCBD_RECONCILE_DRY_RUN").as_deref() == Ok("1")
}

pub(crate) fn reconcile_orphan_scopes_with_runner_sync(
    db: &Db,
    runner: &dyn SystemctlRunner,
    daemon_marker: &str,
    dry_run: bool,
) -> Result<usize, CcbdError> {
    reconcile_orphan_scopes_with_marker_sync(
        db,
        runner,
        &DaemonMarker::explicit(daemon_marker),
        dry_run,
    )
}

fn reconcile_orphan_scopes_with_marker_sync(
    db: &Db,
    runner: &dyn SystemctlRunner,
    daemon_marker: &DaemonMarker,
    dry_run: bool,
) -> Result<usize, CcbdError> {
    if !daemon_marker.may_stop_scopes() {
        tracing::warn!(
            daemon_marker = %daemon_marker.value,
            provenance = ?daemon_marker.provenance,
            "startup reconcile skipped orphan scope stops because daemon marker is not explicit"
        );
        return Ok(0);
    }
    let live_refs = active_session_and_agent_refs_sync(db)?;
    let scopes = match runner.list_scope_units() {
        Ok(scopes) => scopes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => {
            tracing::warn!(error = %err, "failed to list systemd scope units during startup reconcile");
            return Ok(0);
        }
    };

    let mut stopped = 0;
    for scope in scopes {
        if !is_own_ccbd_scope(&scope, &daemon_marker.value) || !is_orphan_scope(&scope, &live_refs)
        {
            continue;
        }
        if dry_run {
            tracing::info!(unit = %scope.unit, "dry-run: would stop orphan systemd scope");
            stopped += 1;
            continue;
        }
        match runner.stop_unit(&scope.unit) {
            Ok(()) => stopped += 1,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(unit = %scope.unit, error = %err, "failed to stop orphan systemd scope")
            }
        }
    }
    Ok(stopped)
}

#[derive(Debug, Clone)]
struct StartupMasterRecoveryWindow {
    session_id: String,
    phase: String,
    defer_until: i64,
}

pub(crate) fn reconcile_master_recovery_windows_sync(
    db: &Db,
    now: i64,
) -> Result<usize, CcbdError> {
    reconcile_master_recovery_windows_with_marker_sync(
        db,
        now,
        &DaemonMarker::ambient("ccbd-unknown"),
        &RealSystemctlRunner,
    )
}

fn reconcile_master_recovery_windows_with_runner_sync(
    db: &Db,
    now: i64,
    daemon_marker: &str,
    runner: &dyn SystemctlRunner,
) -> Result<usize, CcbdError> {
    reconcile_master_recovery_windows_with_marker_sync(
        db,
        now,
        &DaemonMarker::explicit(daemon_marker),
        runner,
    )
}

fn reconcile_master_recovery_windows_with_marker_sync(
    db: &Db,
    now: i64,
    daemon_marker: &DaemonMarker,
    runner: &dyn SystemctlRunner,
) -> Result<usize, CcbdError> {
    let windows = {
        let conn = db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT master_recovery_windows.session_id,
                        master_recovery_windows.phase,
                        master_recovery_windows.defer_until
                 FROM master_recovery_windows
                 ORDER BY master_recovery_windows.created_at ASC,
                          master_recovery_windows.session_id ASC",
            )
            .map_err(|err| map_db_error("prepare master recovery startup reconcile", err))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(StartupMasterRecoveryWindow {
                    session_id: row.get(0)?,
                    phase: row.get(1)?,
                    defer_until: row.get(2)?,
                })
            })
            .map_err(|err| map_db_error("query master recovery startup reconcile", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect master recovery startup reconcile", err))?
    };

    let mut reconciled_count = 0;
    for window in windows {
        if matches!(window.phase.as_str(), "COMPLETED" | "FAILED" | "FUSED") {
            continue;
        }

        if now <= window.defer_until {
            tracing::info!(
                session_id = %window.session_id,
                phase = %window.phase,
                defer_until = window.defer_until,
                now,
                "master recovery window still active during startup reconcile"
            );
            continue;
        }

        let expired = {
            let conn = db.conn();
            expire_master_recovery_window_sync(&conn, &window.session_id, now)?
        };
        if expired {
            reconciled_count += 1;
            tracing::warn!(
                session_id = %window.session_id,
                phase = %window.phase,
                defer_until = window.defer_until,
                now,
                "master recovery window expired during startup reconcile; cascading session"
            );
            cascade_kill_session_agents_with_runner_sync(
                db,
                &window.session_id,
                "MASTER_REVIVE_WINDOW_EXPIRED",
                daemon_marker
                    .may_stop_scopes()
                    .then_some(daemon_marker.value.as_str()),
                runner,
            )?;
        }
    }
    Ok(reconciled_count)
}

fn unixepoch() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

fn active_session_and_agent_refs_sync(db: &Db) -> Result<HashSet<String>, CcbdError> {
    let conn = db.conn();
    let mut refs = HashSet::new();
    {
        let mut stmt = conn
            .prepare(
                // Recovery-eligible providers mirror provider::manifest::is_recovery_eligible_provider;
                // keep this SQL in sync if that single source of truth changes.
                "SELECT DISTINCT sessions.id \
                 FROM sessions \
                 JOIN agents ON agents.session_id = sessions.id \
                 WHERE sessions.status = 'ACTIVE' \
                   AND (agents.state NOT IN ('CRASHED', 'KILLED') \
                        OR (agents.state = 'CRASHED' AND agents.provider IN ('codex', 'claude', 'antigravity')))",
            )
            .map_err(|err| map_db_error("prepare active session refs", err))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|err| map_db_error("query active session refs", err))?;
        for session_id in rows {
            refs.insert(session_id.map_err(|err| map_db_error("collect active session ref", err))?);
        }
    }
    {
        let mut stmt = conn
            .prepare(
                // Recovery-eligible providers mirror provider::manifest::is_recovery_eligible_provider;
                // keep this SQL in sync if that single source of truth changes.
                "SELECT agents.id \
                 FROM agents \
                 JOIN sessions ON sessions.id = agents.session_id \
                 WHERE sessions.status = 'ACTIVE' \
                   AND (agents.state NOT IN ('CRASHED', 'KILLED') \
                        OR (agents.state = 'CRASHED' AND agents.provider IN ('codex', 'claude', 'antigravity'))) \
                 ORDER BY agents.id ASC",
            )
            .map_err(|err| map_db_error("prepare active agent refs", err))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|err| map_db_error("query active agent refs", err))?;
        for agent_id in rows {
            refs.insert(agent_id.map_err(|err| map_db_error("collect active agent ref", err))?);
        }
    }
    Ok(refs)
}

fn is_own_ccbd_scope(scope: &ScopeUnit, daemon_marker: &str) -> bool {
    crate::platform::sys::scope::is_own_ccbd_scope(scope, daemon_marker)
}

fn is_orphan_scope(scope: &ScopeUnit, live_refs: &HashSet<String>) -> bool {
    crate::platform::sys::scope::is_orphan_scope(scope, live_refs)
}

pub(crate) fn reconcile_active_agents_to_crashed_sync(
    db: &Db,
    state_dir: Option<&Path>,
    socket_name: Option<&str>,
) -> Result<usize, CcbdError> {
    startup_reconcile_phase_prompt_pending_preserve(db)?;
    let candidates = startup_reconcile_phase_a_select_candidates(db)?;
    let (dead, alive) = startup_reconcile_phase_b_probe_pids(candidates);
    let (reattach_dead, reattachable_alive) =
        startup_reconcile_phase_b2_prepare_alive_io(state_dir, socket_name, alive);
    let dead = dead.into_iter().chain(reattach_dead).collect::<Vec<_>>();
    let changed = startup_reconcile_phase_c_crash_dead(db, &dead)?;
    if let Some(state_dir) = state_dir {
        for candidate in &dead {
            if crate::provider::manifest::is_recovery_eligible_provider(&candidate.agent.provider) {
                remove_agent_sandbox_dir_preserving_home_sync(
                    state_dir,
                    &candidate.agent.session_id,
                    &candidate.agent.id,
                );
            } else {
                remove_agent_sandbox_dir_sync(
                    state_dir,
                    &candidate.agent.session_id,
                    &candidate.agent.id,
                );
            }
        }
    }
    startup_reconcile_phase_d_reregister_alive(db, reattachable_alive)?;
    Ok(changed)
}

fn startup_reconcile_phase_prompt_pending_preserve(db: &Db) -> Result<(), CcbdError> {
    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT id FROM agents WHERE state = ? ORDER BY id ASC")
        .map_err(|err| map_db_error("prepare prompt pending reconcile query", err))?;
    let rows = stmt
        .query_map([STATE_PROMPT_PENDING], |row| row.get::<_, String>(0))
        .map_err(|err| map_db_error("query prompt pending reconcile agents", err))?;
    let agent_ids = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect prompt pending reconcile agents", err))?;
    for agent_id in agent_ids {
        tracing::info!(
            agent_id = %agent_id,
            state = STATE_PROMPT_PENDING,
            reason = "startup reconcile preserves pending prompt",
            impact = "agent is not restarted, reset, dispatched, or crashed until resolve_prompt",
            "startup reconcile preserved prompt pending agent"
        );
    }
    Ok(())
}

fn startup_reconcile_phase_a_select_candidates(
    db: &Db,
) -> Result<Vec<StartupAgentCandidate>, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin startup reconcile candidate scan", err))?;
    let candidates = {
        let mut stmt = tx
            .prepare(
                "SELECT id, session_id, pid, state, provider FROM agents WHERE state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY', 'IDLE')",
            )
            .map_err(|err| map_db_error("prepare active agents query", err))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(StartupAgentCandidate {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    pid: row.get(2)?,
                    state: row.get(3)?,
                    provider: row.get(4)?,
                })
            })
            .map_err(|err| map_db_error("query active agents", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect active agents", err))?
    };
    tx.commit()
        .map_err(|err| map_db_error("commit startup reconcile candidate scan", err))?;
    Ok(candidates)
}

/// Destroys an agent's sandbox after handing its session records to the
/// project.
///
/// The sandbox home holds the provider's transcripts, which belong to the
/// project rather than to ah (decision 0004). Archiving happens first and a
/// failed archive stops the deletion: a sandbox left on disk is recoverable,
/// a deleted session is not (#27).
pub fn remove_agent_sandbox_dir_sync(state_dir: &Path, session_id: &str, agent_id: &str) {
    let sandbox_dir = state_dir.join("sandboxes").join(session_id).join(agent_id);
    match crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox_dir) {
        Ok(home_root) => {
            if !archive_before_destroying_home(&sandbox_dir, &home_root, session_id, agent_id) {
                return;
            }
            if let Err(err) = std::fs::remove_dir_all(&home_root)
                && err.kind() != io::ErrorKind::NotFound
            {
                tracing::warn!(?home_root, error = %err, "failed to remove agent sandbox home");
            }
        }
        Err(err) => {
            tracing::warn!(?sandbox_dir, error = %err, "failed to resolve agent sandbox home");
        }
    }
    if let Err(err) = std::fs::remove_dir_all(&sandbox_dir)
        && err.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(?sandbox_dir, error = %err, "failed to remove agent sandbox dir");
    }
}

/// Returns whether destruction may proceed.
fn archive_before_destroying_home(
    sandbox_dir: &Path,
    home_root: &Path,
    session_id: &str,
    agent_id: &str,
) -> bool {
    use crate::sandbox::session_archive::{ArchiveOutcome, archive_session_records};

    match archive_session_records(sandbox_dir, home_root, session_id, agent_id) {
        ArchiveOutcome::Archived { destination, files } => {
            tracing::info!(
                session_id,
                agent_id,
                files,
                destination = %destination.display(),
                "archived provider session records to the project before destroying the sandbox"
            );
            true
        }
        ArchiveOutcome::NothingToArchive => true,
        ArchiveOutcome::NoProjectRoot => {
            tracing::warn!(
                session_id,
                agent_id,
                sandbox_dir = %sandbox_dir.display(),
                "sandbox has no recorded project root; destroying without archiving session records"
            );
            true
        }
        ArchiveOutcome::Failed(details) => {
            tracing::error!(
                session_id,
                agent_id,
                home_root = %home_root.display(),
                details,
                "refusing to destroy a sandbox whose session records could not be archived"
            );
            false
        }
    }
}

/// Clears an agent's sandbox dir while leaving its home for recovery.
///
/// The project marker stays: the home it points at is still on disk, and
/// whoever destroys that home later needs to know which project owns its
/// session records (#27).
pub fn remove_agent_sandbox_dir_preserving_home_sync(
    state_dir: &Path,
    session_id: &str,
    agent_id: &str,
) {
    let sandbox_dir = state_dir.join("sandboxes").join(session_id).join(agent_id);
    let entries = match std::fs::read_dir(&sandbox_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return,
        Err(err) => {
            tracing::warn!(
                ?sandbox_dir,
                error = %err,
                "failed to read preserved-home agent sandbox dir"
            );
            return;
        }
    };
    for entry in entries.flatten() {
        if crate::sandbox::session_archive::is_project_root_marker(&entry.file_name()) {
            continue;
        }
        let path = entry.path();
        let removed = if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        if let Err(err) = removed
            && err.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(
                ?path,
                error = %err,
                "failed to remove entry from preserved-home agent sandbox dir"
            );
        }
    }
}

fn startup_reconcile_phase_b_probe_pids(
    candidates: Vec<StartupAgentCandidate>,
) -> (Vec<StartupCrashCandidate>, Vec<StartupAgentCandidate>) {
    let mut dead = Vec::new();
    let mut alive = Vec::new();
    for candidate in candidates {
        let Some(pid) = candidate.pid else {
            dead.push(StartupCrashCandidate {
                agent: candidate,
                reason: "STARTUP_RECONCILE_DEAD_PID",
            });
            continue;
        };
        match probe_pid_alive(pid as i32) {
            Ok(true) => alive.push(candidate),
            Ok(false) => dead.push(StartupCrashCandidate {
                agent: candidate,
                reason: "STARTUP_RECONCILE_DEAD_PID",
            }),
            Err(err) => {
                tracing::warn!(agent_id = %candidate.id, pid, error = %err, "startup reconcile pid probe failed");
                dead.push(StartupCrashCandidate {
                    agent: candidate,
                    reason: "STARTUP_RECONCILE_DEAD_PID",
                });
            }
        }
    }
    (dead, alive)
}

fn startup_reconcile_phase_b2_prepare_alive_io(
    state_dir: Option<&Path>,
    socket_name: Option<&str>,
    alive: Vec<StartupAgentCandidate>,
) -> (Vec<StartupCrashCandidate>, Vec<StartupAliveIoCandidate>) {
    let Some(state_dir) = state_dir else {
        return (Vec::new(), Vec::new());
    };
    let socket_name = socket_name
        .map(ToString::to_string)
        .unwrap_or_else(|| crate::tmux::compute_socket_name(state_dir));

    let mut dead = Vec::new();
    let mut reattachable = Vec::new();
    for agent in alive {
        let fifo_path = state_dir.join("pipes").join(format!("{}.fifo", agent.id));
        if !fifo_path.exists() {
            tracing::warn!(agent_id = %agent.id, ?fifo_path, "startup reconcile cannot reattach missing fifo");
            dead.push(StartupCrashCandidate {
                agent,
                reason: "STARTUP_RECONCILE_FIFO_REATTACH_FAILED",
            });
            continue;
        }
        match open_fifo_for_reattach(&fifo_path) {
            Ok(fifo_file) => reattachable.push(StartupAliveIoCandidate {
                agent,
                fifo_path,
                fifo_file,
                socket_name: socket_name.clone(),
            }),
            Err(err) => {
                tracing::warn!(agent_id = %agent.id, ?fifo_path, error = %err, "startup reconcile cannot open fifo");
                dead.push(StartupCrashCandidate {
                    agent,
                    reason: "STARTUP_RECONCILE_FIFO_REATTACH_FAILED",
                });
            }
        }
    }
    (dead, reattachable)
}

#[cfg(unix)]
fn open_fifo_for_reattach(fifo_path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo_path)
}

#[cfg(windows)]
fn open_fifo_for_reattach(_fifo_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Windows startup FIFO reattach is not implemented until M2 stream reattach",
    ))
}

fn probe_pid_alive(pid: i32) -> Result<bool, std::io::Error> {
    match crate::platform::sys::proc_info::kill_zero_check(pid) {
        crate::platform::sys::proc_info::ProcessLiveness::Alive => Ok(true),
        crate::platform::sys::proc_info::ProcessLiveness::Dead => Ok(false),
        crate::platform::sys::proc_info::ProcessLiveness::Unknown => Err(io::Error::other(
            format!("process liveness is unknown for pid {pid}"),
        )),
    }
}

fn startup_reconcile_phase_c_crash_dead(
    db: &Db,
    dead: &[StartupCrashCandidate],
) -> Result<usize, CcbdError> {
    if dead.is_empty() {
        return Ok(0);
    }
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin startup reconcile crash dead pids", err))?;
    let mut changed = 0;
    for crash in dead {
        let candidate = &crash.agent;
        let changes = tx
            .execute(
                "UPDATE agents SET state = 'CRASHED', error_code = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY', 'IDLE')",
                params![crash.reason, candidate.id],
            )
            .map_err(|err| map_db_error("mark dead-pid agent crashed", err))?;
        if changes == 1 {
            changed += 1;
            let payload = serde_json::json!({
                "from": candidate.state,
                "to": "CRASHED",
                "reason": crash.reason,
            })
            .to_string();
            tx.execute(
                "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
                params![candidate.id, payload],
            )
            .map_err(|err| map_db_error("insert startup reconcile dead-pid state_change", err))?;
            mark_dispatched_jobs_failed_for_agent_conn_sync(&tx, &candidate.id, crash.reason)?;
        }
    }
    tx.commit()
        .map_err(|err| map_db_error("commit startup reconcile crash dead pids", err))?;
    for crash in dead {
        let policy =
            if recovery_eligible_orphan_scope_should_be_preserved(&crash.agent.provider, "CRASHED")
            {
                crate::agent_io::registry::RuntimeCleanupPolicy::PreserveRecoverableCrashedHome
            } else {
                crate::agent_io::registry::RuntimeCleanupPolicy::Default
            };
        crate::agent_io::registry::cleanup_agent_runtime_resources_with_policy(
            &crash.agent.id,
            Some(&crash.agent.session_id),
            policy,
        );
    }
    Ok(changed)
}

fn startup_reconcile_phase_d_reregister_alive(
    db: &Db,
    alive: Vec<StartupAliveIoCandidate>,
) -> Result<(), CcbdError> {
    for alive_agent in alive {
        let candidate = alive_agent.agent;
        let Some(pid) = candidate.pid else {
            continue;
        };
        let pidfd = match crate::monitor::pidfd_open(pid as i32) {
            Ok(pidfd) => pidfd,
            Err(CcbdError::AgentUnexpectedExit { .. }) => continue,
            Err(err) => return Err(err),
        };
        let task_fd = pidfd
            .try_clone()
            .map_err(|err| CcbdError::EnvironmentNotSupported {
                details: format!("clone agent pidfd for {}: {err}", candidate.id),
            })?;
        crate::monitor::register(candidate.id.clone(), pidfd);
        let can_spawn_tasks = tokio::runtime::Handle::try_current().is_ok();
        if can_spawn_tasks {
            crate::monitor::agent_watch::spawn_agent_pidfd_watch_task(
                candidate.id.clone(),
                candidate.session_id.clone(),
                pid as i32,
                task_fd,
                Arc::new(db.clone()),
                None,
            );
        } else {
            drop(task_fd);
        }

        if can_spawn_tasks {
            let manifest = crate::provider::manifest::get_manifest(&candidate.provider);
            let parser_handle = Arc::new(std::sync::Mutex::new(vt100::Parser::new(200, 200, 0)));
            crate::marker::parser_registry::register(candidate.id.clone(), parser_handle.clone());
            let matcher = Arc::new(crate::marker::MarkerMatcher::from_manifest(&manifest));
            let idle_scan_enabled = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let (output_tx, output_rx) = tokio::sync::mpsc::channel(128);
            let reader_handle = crate::agent_io::spawn_agent_io_reader_task(
                candidate.id.clone(),
                alive_agent.fifo_file,
                output_tx,
            );
            crate::marker::spawn_perception_stream_processor_task(
                candidate.id.clone(),
                db.clone(),
                parser_handle.clone(),
                output_rx,
                crate::marker::PerceptionStreamConfig {
                    matcher: matcher.clone(),
                    stability_ms: manifest.stability_ms,
                    idle_scan_enabled: idle_scan_enabled.clone(),
                },
            );
            crate::agent_io::registry::register(
                candidate.id.clone(),
                crate::agent_io::registry::AgentIoEntry {
                    session_id: candidate.session_id.clone(),
                    pane_id: TmuxPaneId(format!("reattached:{}", candidate.id)),
                    expected_pid: Some(pid),
                    reader_handle,
                    fifo_path: alive_agent.fifo_path,
                    socket_name: alive_agent.socket_name,
                    idle_scan_enabled,
                },
            );
            let timer_kind = if candidate.state == "SPAWNING" {
                crate::marker::TimerKind::Startup
            } else {
                crate::marker::TimerKind::Busy
            };
            let marker_handle = crate::marker::spawn_marker_timer_task(
                candidate.id.clone(),
                timer_kind,
                Arc::new(db.clone()),
                parser_handle,
            );
            crate::marker::registry::register(candidate.id.clone(), marker_handle);
        }
    }
    Ok(())
}

pub async fn system_dump(db: Db) -> Result<Value, CcbdError> {
    spawn_db("system::system_dump", move || system_dump_sync(&db)).await
}

pub async fn cascade_kill_session_agents(
    db: Db,
    session_id: String,
    reason: String,
) -> Result<usize, CcbdError> {
    spawn_db("system::cascade_kill_session_agents", move || {
        cascade_kill_session_agents_sync(&db, &session_id, &reason)
    })
    .await
}

pub async fn cascade_kill_session_agents_for_daemon(
    db: Db,
    session_id: String,
    reason: String,
    daemon_marker: String,
) -> Result<usize, CcbdError> {
    spawn_db(
        "system::cascade_kill_session_agents_for_daemon",
        move || {
            cascade_kill_session_agents_with_runner_sync(
                &db,
                &session_id,
                &reason,
                Some(&daemon_marker),
                &RealSystemctlRunner,
            )
        },
    )
    .await
}

pub async fn session_agent_ids(db: Db, session_id: String) -> Result<Vec<String>, CcbdError> {
    spawn_db("system::session_agent_ids", move || {
        session_agent_ids_sync(&db, &session_id)
    })
    .await
}

pub async fn reconcile_startup(db: Db) -> Result<usize, CcbdError> {
    let state_dir = crate::env::resolve_state_dir();
    reconcile_startup_with_tmux_socket_and_provenance(
        db,
        state_dir,
        None,
        DaemonMarkerProvenance::Ambient,
    )
    .await
}

pub async fn reconcile_startup_with_state_dir(
    db: Db,
    state_dir: PathBuf,
) -> Result<usize, CcbdError> {
    reconcile_startup_with_tmux_socket(db, state_dir, None).await
}

pub async fn reconcile_startup_with_tmux_socket(
    db: Db,
    state_dir: PathBuf,
    current_socket_name: Option<String>,
) -> Result<usize, CcbdError> {
    reconcile_startup_with_tmux_socket_and_gateway(db, state_dir, current_socket_name, None).await
}

pub async fn reconcile_startup_with_tmux_socket_and_gateway(
    db: Db,
    state_dir: PathBuf,
    current_socket_name: Option<String>,
    claude_gateway: Option<Arc<crate::claude_gateway::ClaudeGatewayService>>,
) -> Result<usize, CcbdError> {
    let reconciled = reconcile_startup_with_tmux_socket_and_provenance(
        db.clone(),
        state_dir.clone(),
        current_socket_name,
        DaemonMarkerProvenance::Explicit,
    )
    .await?;

    if claude_gateway.is_some() {
        tracing::debug!("startup reconcile complete; rebuilding claude gateway seats");
    }

    if let Some(claude_gateway) = claude_gateway {
        reconcile_claude_gateway_seats(db, state_dir, claude_gateway).await?;
    }
    Ok(reconciled)
}

async fn reconcile_startup_with_tmux_socket_and_provenance(
    db: Db,
    state_dir: PathBuf,
    current_socket_name: Option<String>,
    marker_provenance: DaemonMarkerProvenance,
) -> Result<usize, CcbdError> {
    spawn_db("system::reconcile_startup", move || {
        reconcile_startup_with_tmux_socket_sync_and_runner(
            &db,
            &state_dir,
            current_socket_name.as_deref(),
            marker_provenance,
            &RealSystemctlRunner,
            unixepoch(),
            reconcile_orphan_scopes_dry_run_enabled(),
        )
    })
    .await
}

fn reconcile_startup_with_tmux_socket_sync_and_runner(
    db: &Db,
    state_dir: &Path,
    current_socket_name: Option<&str>,
    marker_provenance: DaemonMarkerProvenance,
    runner: &dyn SystemctlRunner,
    now: i64,
    orphan_dry_run: bool,
) -> Result<usize, CcbdError> {
    let socket_name = current_socket_name
        .map(str::to_string)
        .unwrap_or_else(|| crate::tmux::compute_socket_name(state_dir));
    let daemon_marker = DaemonMarker {
        value: socket_name.clone(),
        provenance: marker_provenance,
    };
    let agents_count =
        reconcile_active_agents_to_crashed_sync(db, Some(state_dir), Some(&socket_name))?;
    let recovery_count =
        reconcile_master_recovery_windows_with_marker_sync(db, now, &daemon_marker, runner)?;
    let scopes_count =
        reconcile_orphan_scopes_with_marker_sync(db, runner, &daemon_marker, orphan_dry_run)?;
    sweep_stale_tmux_sockets_sync(current_socket_name)?;
    Ok(agents_count + recovery_count + scopes_count)
}

fn select_startup_claude_gateway_seats_sync(db: &Db) -> Result<StartupClaudeSeats, CcbdError> {
    let conn = db.conn();
    let workers = {
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id
                 FROM agents
                 WHERE provider = 'claude'
                   AND state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY', 'IDLE', 'PROMPT_PENDING')
                 ORDER BY created_at ASC, id ASC",
            )
            .map_err(|err| map_db_error("prepare startup claude gateway workers", err))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(StartupClaudeWorkerSeat {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                })
            })
            .map_err(|err| map_db_error("query startup claude gateway workers", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect startup claude gateway workers", err))?
    };
    let sessions = {
        let mut stmt = conn
            .prepare(
                "SELECT id FROM sessions WHERE status = 'ACTIVE' ORDER BY created_at ASC, id ASC",
            )
            .map_err(|err| map_db_error("prepare startup claude gateway sessions", err))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|err| map_db_error("query startup claude gateway sessions", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect startup claude gateway sessions", err))?
    };
    Ok(StartupClaudeSeats { workers, sessions })
}

async fn reconcile_claude_gateway_seats(
    db: Db,
    state_dir: PathBuf,
    claude_gateway: Arc<crate::claude_gateway::ClaudeGatewayService>,
) -> Result<(), CcbdError> {
    let seats = spawn_db("system::select_startup_claude_gateway_seats", move || {
        select_startup_claude_gateway_seats_sync(&db)
    })
    .await?;

    for session_id in seats.sessions {
        let sandbox_dir = state_dir.join("sandboxes").join(&session_id).join("master");
        match claude_gateway
            .register_master(&session_id, &sandbox_dir)
            .await
        {
            Ok(topology) => tracing::info!(
                session_id = %session_id,
                host_uds = %topology.host_uds_path.display(),
                "startup reconcile restored claude gateway master listener"
            ),
            Err(err) => tracing::warn!(
                session_id = %session_id,
                error = %err,
                "startup reconcile could not restore claude gateway master listener"
            ),
        }
    }

    for worker in seats.workers {
        let sandbox_dir = state_dir
            .join("sandboxes")
            .join(&worker.session_id)
            .join(&worker.id);
        match claude_gateway
            .register_worker(&worker.id, &sandbox_dir)
            .await
        {
            Ok(topology) => tracing::info!(
                agent_id = %worker.id,
                session_id = %worker.session_id,
                host_uds = %topology.host_uds_path.display(),
                "startup reconcile restored claude gateway worker listener"
            ),
            Err(err) => tracing::warn!(
                agent_id = %worker.id,
                session_id = %worker.session_id,
                error = %err,
                "startup reconcile could not restore claude gateway worker listener"
            ),
        }
    }

    Ok(())
}

#[cfg(test)]
async fn reconcile_startup_with_tmux_socket_and_runner_for_test(
    db: Db,
    state_dir: PathBuf,
    current_socket_name: Option<String>,
    runner: Arc<dyn SystemctlRunner + Send + Sync>,
    now: i64,
    orphan_dry_run: bool,
) -> Result<usize, CcbdError> {
    spawn_db("system::reconcile_startup_test", move || {
        reconcile_startup_with_tmux_socket_sync_and_runner(
            &db,
            &state_dir,
            current_socket_name.as_deref(),
            DaemonMarkerProvenance::Explicit,
            runner.as_ref(),
            now,
            orphan_dry_run,
        )
    })
    .await
}

#[cfg(test)]
async fn reconcile_startup_with_ambient_marker_and_runner_for_test(
    db: Db,
    state_dir: PathBuf,
    current_socket_name: Option<String>,
    runner: Arc<dyn SystemctlRunner + Send + Sync>,
    now: i64,
    orphan_dry_run: bool,
) -> Result<usize, CcbdError> {
    spawn_db("system::reconcile_startup_ambient_test", move || {
        reconcile_startup_with_tmux_socket_sync_and_runner(
            &db,
            &state_dir,
            current_socket_name.as_deref(),
            DaemonMarkerProvenance::Ambient,
            runner.as_ref(),
            now,
            orphan_dry_run,
        )
    })
    .await
}

#[cfg(test)]
async fn reconcile_startup_from_ambient_env_with_runner_for_test(
    db: Db,
    runner: Arc<dyn SystemctlRunner + Send + Sync>,
    now: i64,
    orphan_dry_run: bool,
) -> Result<usize, CcbdError> {
    let state_dir = crate::env::resolve_state_dir();
    spawn_db("system::reconcile_startup_ambient_env_test", move || {
        reconcile_startup_with_tmux_socket_sync_and_runner(
            &db,
            &state_dir,
            None,
            DaemonMarkerProvenance::Ambient,
            runner.as_ref(),
            now,
            orphan_dry_run,
        )
    })
    .await
}

pub fn sweep_stale_tmux_sockets_sync(
    current_socket_name: Option<&str>,
) -> Result<usize, CcbdError> {
    #[cfg(windows)]
    {
        let _ = current_socket_name;
        return Ok(0);
    }

    #[cfg(unix)]
    {
        let socket_dir = format!("/tmp/tmux-{}", unsafe { libc::geteuid() });
        let entries = match std::fs::read_dir(&socket_dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(err) => {
                return Err(CcbdError::EnvironmentNotSupported {
                    details: format!("read tmux socket dir {socket_dir}: {err}"),
                });
            }
        };

        let mut removed = 0;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to read tmux socket entry");
                    continue;
                }
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if !(name.starts_with("ahd-") || name.starts_with("ccbd-"))
                || current_socket_name == Some(name.as_str())
            {
                continue;
            }
            let alive = tmux_sessions_alive(&name);
            if alive {
                continue;
            }
            match std::fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    tracing::warn!(socket = %name, error = %err, "failed to remove stale tmux socket")
                }
            }
        }
        Ok(removed)
    }
}

fn tmux_sessions_alive(socket_name: &str) -> bool {
    match Command::new("tmux")
        .args(["-L", socket_name, "ls-sessions"])
        .output()
    {
        Ok(output) if output.status.success() => true,
        Ok(output) if String::from_utf8_lossy(&output.stderr).contains("unknown command") => {
            Command::new("tmux")
                .args(["-L", socket_name, "list-sessions"])
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }
        _ => false,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{
        MasterDeathSessionActivity, ScopeUnit, SystemctlRunner, cascade_kill_session_agents_sync,
        cascade_kill_session_agents_with_runner_sync,
        clean_worker_runtime_resources_with_runner_sync, reconcile_active_agents_to_crashed_sync,
        reconcile_master_recovery_windows_with_runner_sync,
        reconcile_orphan_scopes_dry_run_enabled, reconcile_orphan_scopes_with_runner_sync,
        reconcile_startup_sync, reconcile_startup_sync_with_state_dir_and_runner,
        remove_agent_sandbox_dir_preserving_home_sync, remove_agent_sandbox_dir_sync,
        snapshot_master_death_session_activity,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::master_recovery::{
        begin_master_recovery_window_sync, complete_master_recovery_window_sync,
        update_master_recovery_phase_sync,
    };
    use crate::db::recovery::query_agent_recovery_intent_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::STATE_PROMPT_PENDING;
    use crate::db::{Db, init};
    use crate::monitor::session_watch::unit_name_for_session;
    use crate::provider::home_layout::sandbox_home_for_sandbox_dir;
    use std::cell::RefCell;
    use std::ffi::OsString;
    use std::fs;
    use std::path::Path;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    struct EnvGuard {
        key: &'static str,
        old: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &Path) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.old {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn with_test_db_handle<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_master_death_session(db: &Db, session_id: &str, agents: &[(&str, &str)]) {
        let conn = db.conn();
        insert_session_sync(&conn, session_id, "p_master_death", "/tmp/master-death").unwrap();
        for (agent_id, state) in agents {
            insert_agent_sync(&conn, agent_id, session_id, "bash", state, Some(123)).unwrap();
        }
    }

    fn register_fake_pidfd(agent_id: &str) {
        let fake_fd: std::os::fd::OwnedFd = tempfile::tempfile().unwrap().into();
        crate::monitor::register(agent_id.to_string(), fake_fd);
    }

    fn seed_active_recovery_session(db: &Db, session_id: &str, agent_id: &str, agent_state: &str) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            session_id,
            "p_master_recovery",
            "/tmp/master-recovery",
        )
        .unwrap();
        conn.execute(
            "UPDATE sessions
             SET status = 'ACTIVE', master_pid = ?2, master_generation = 5
             WHERE id = ?1",
            rusqlite::params![session_id, i64::from(i32::MAX)],
        )
        .unwrap();
        insert_agent_sync(&conn, agent_id, session_id, "bash", agent_state, Some(123)).unwrap();
    }

    fn query_window_phase(db: &Db, session_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT phase FROM master_recovery_windows WHERE session_id = ?1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    }

    fn query_session_status(db: &Db, session_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT status FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    }

    fn query_session_exit_reason(db: &Db, session_id: &str) -> Option<String> {
        db.conn()
            .query_row(
                "SELECT master_last_exit_reason FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap()
    }

    fn query_agent_state(db: &Db, agent_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    }

    #[test]
    fn master_death_snapshot_active_states_are_active_work() {
        for (state, agent_id) in [
            ("SPAWNING", "a_spawning"),
            ("WAITING_FOR_ACK", "a_waiting"),
            ("BUSY", "a_busy"),
        ] {
            with_test_db_handle(|db| {
                seed_master_death_session(db, "s_master_death_active", &[(agent_id, state)]);

                let snapshot =
                    snapshot_master_death_session_activity(db, "s_master_death_active").unwrap();

                assert_eq!(
                    snapshot.classification,
                    MasterDeathSessionActivity::ActiveWork
                );
                assert_eq!(snapshot.worker_ids_to_reap, [agent_id.to_string()]);
            });
        }
    }

    #[test]
    fn master_death_snapshot_prompt_pending_is_active_work() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_prompt",
                &[("a_prompt", STATE_PROMPT_PENDING)],
            );

            let snapshot =
                snapshot_master_death_session_activity(db, "s_master_death_prompt").unwrap();

            assert_eq!(
                snapshot.classification,
                MasterDeathSessionActivity::ActiveWork
            );
            assert_eq!(snapshot.worker_ids_to_reap, ["a_prompt".to_string()]);
        });
    }

    #[test]
    fn master_death_snapshot_queued_only_idle_worker_is_active_work() {
        with_test_db_handle(|db| {
            seed_master_death_session(db, "s_master_death_queued", &[("a_idle", "IDLE")]);
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_queued", "a_idle", None, "do work").unwrap();
            }

            let snapshot =
                snapshot_master_death_session_activity(db, "s_master_death_queued").unwrap();

            assert_eq!(
                snapshot.classification,
                MasterDeathSessionActivity::ActiveWork
            );
            assert_eq!(snapshot.worker_ids_to_reap, ["a_idle".to_string()]);
        });
    }

    #[test]
    fn master_death_snapshot_dispatched_job_is_active_work() {
        with_test_db_handle(|db| {
            seed_master_death_session(db, "s_master_death_dispatched", &[("a_idle", "IDLE")]);
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_dispatched", "a_idle", None, "do work").unwrap();
                conn.execute(
                    "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_dispatched'",
                    [],
                )
                .unwrap();
            }

            let snapshot =
                snapshot_master_death_session_activity(db, "s_master_death_dispatched").unwrap();

            assert_eq!(
                snapshot.classification,
                MasterDeathSessionActivity::ActiveWork
            );
            assert_eq!(snapshot.worker_ids_to_reap, ["a_idle".to_string()]);
        });
    }

    #[test]
    fn master_death_snapshot_all_idle_or_dead_without_jobs_is_idle_no_work() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_idle",
                &[
                    ("a_idle", "IDLE"),
                    ("a_crashed", "CRASHED"),
                    ("a_killed", "KILLED"),
                ],
            );

            let snapshot =
                snapshot_master_death_session_activity(db, "s_master_death_idle").unwrap();

            assert_eq!(
                snapshot.classification,
                MasterDeathSessionActivity::IdleNoWork
            );
            assert_eq!(
                snapshot.worker_ids_to_reap,
                [
                    "a_crashed".to_string(),
                    "a_idle".to_string(),
                    "a_killed".to_string()
                ]
            );
        });
    }

    #[test]
    fn master_revive_deliberate_cascade_kill_does_not_capture_revive_intent() {
        with_test_db_handle(|db| {
            let session_id = "s_cascade_no_intent";
            let agent_id = "a_cascade_no_intent";
            seed_master_death_session(db, session_id, &[(agent_id, "BUSY")]);
            {
                let conn = db.conn();
                insert_job_sync(
                    &conn,
                    "job_cascade_no_intent",
                    agent_id,
                    None,
                    "do not revive deliberate kill",
                )
                .unwrap();
                conn.execute(
                    "UPDATE jobs
                     SET status = 'DISPATCHED', dispatched_at = unixepoch()
                     WHERE id = 'job_cascade_no_intent'",
                    [],
                )
                .unwrap();
            }

            let changed = cascade_kill_session_agents_sync(db, session_id, "SESSION_KILL").unwrap();

            assert_eq!(changed, 1);
            assert!(
                query_agent_recovery_intent_sync(&db.conn(), agent_id)
                    .unwrap()
                    .is_none(),
                "deliberate cascade kill must not capture revive intent"
            );
        });
    }

    #[test]
    fn clean_worker_runtime_resources_keeps_session_active_and_marks_worker_killed() {
        with_test_db_handle(|db| {
            seed_master_death_session(db, "s_master_death_cleanup", &[("a_cleanup", "BUSY")]);
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_cleanup", "a_cleanup", None, "do work").unwrap();
                conn.execute(
                    "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_cleanup'",
                    [],
                )
                .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_cleanup",
                &["a_cleanup".to_string()],
                "MASTER_EXIT",
                None,
                false,
                &runner,
            )
            .unwrap();

            let (session_status, agent_state, state_change_count): (String, String, i64) = db
                .conn()
                .query_row(
                    "SELECT sessions.status, agents.state, COUNT(events.seq_id) \
                     FROM sessions \
                     JOIN agents ON agents.session_id = sessions.id \
                     LEFT JOIN events ON events.agent_id = agents.id AND events.event_type = 'state_change' \
                     WHERE sessions.id = 's_master_death_cleanup' AND agents.id = 'a_cleanup'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let job = query_job_sync(&db.conn(), "job_cleanup").unwrap().unwrap();

            assert_eq!(outcome.db_killed_count, 1);
            assert_eq!(session_status, "ACTIVE");
            assert_eq!(agent_state, "KILLED");
            assert_eq!(job.status, "FAILED");
            assert_eq!(job.error_reason.as_deref(), Some("MASTER_EXIT"));
            assert_eq!(state_change_count, 1);
        });
    }

    #[test]
    fn startup_reconcile_preserves_unexpired_window_after_ahd_restart() {
        with_test_db_handle(|db| {
            seed_active_recovery_session(
                db,
                "s_reconcile_recovery_live",
                "a_reconcile_recovery_live",
                "BUSY",
            );
            {
                let conn = db.conn();
                begin_master_recovery_window_sync(
                    &conn,
                    "s_reconcile_recovery_live",
                    111,
                    5,
                    true,
                    1_000,
                    90,
                )
                .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let reconciled =
                reconcile_master_recovery_windows_with_runner_sync(db, 1_050, "ahd-test", &runner)
                    .unwrap();

            assert_eq!(reconciled, 0);
            assert_eq!(
                query_window_phase(db, "s_reconcile_recovery_live"),
                "DETECTED"
            );
            assert_eq!(
                query_session_status(db, "s_reconcile_recovery_live"),
                "ACTIVE"
            );
            assert_eq!(query_agent_state(db, "a_reconcile_recovery_live"), "BUSY");
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn startup_reconcile_expires_old_window_and_cascades_workers() {
        with_test_db_handle(|db| {
            seed_active_recovery_session(
                db,
                "s_reconcile_recovery_expired",
                "a_reconcile_recovery_expired",
                "BUSY",
            );
            {
                let conn = db.conn();
                begin_master_recovery_window_sync(
                    &conn,
                    "s_reconcile_recovery_expired",
                    111,
                    5,
                    true,
                    1_000,
                    10,
                )
                .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let reconciled =
                reconcile_master_recovery_windows_with_runner_sync(db, 1_020, "ahd-test", &runner)
                    .unwrap();

            assert_eq!(reconciled, 1);
            assert_eq!(
                query_window_phase(db, "s_reconcile_recovery_expired"),
                "FAILED"
            );
            assert_eq!(
                query_session_exit_reason(db, "s_reconcile_recovery_expired").as_deref(),
                Some("MASTER_REVIVE_WINDOW_EXPIRED")
            );
            assert_eq!(
                query_session_status(db, "s_reconcile_recovery_expired"),
                "FAILED"
            );
            assert_eq!(
                query_agent_state(db, "a_reconcile_recovery_expired"),
                "KILLED"
            );
        });
    }

    #[test]
    fn startup_reconcile_alive_verifying_window_does_not_complete() {
        with_test_db_handle(|db| {
            seed_active_recovery_session(
                db,
                "s_reconcile_recovery_alive_master",
                "a_reconcile_recovery_alive_master",
                "BUSY",
            );
            {
                let conn = db.conn();
                conn.execute(
                    "UPDATE sessions SET master_pid = ?2 WHERE id = ?1",
                    rusqlite::params![
                        "s_reconcile_recovery_alive_master",
                        i64::from(std::process::id())
                    ],
                )
                .unwrap();
                begin_master_recovery_window_sync(
                    &conn,
                    "s_reconcile_recovery_alive_master",
                    i64::from(std::process::id()),
                    5,
                    true,
                    1_000,
                    300,
                )
                .unwrap();
                update_master_recovery_phase_sync(
                    &conn,
                    "s_reconcile_recovery_alive_master",
                    5,
                    "MASTER_VERIFYING",
                    1_001,
                )
                .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let reconciled =
                reconcile_master_recovery_windows_with_runner_sync(db, 1_100, "ahd-test", &runner)
                    .unwrap();

            assert_eq!(reconciled, 0);
            assert_eq!(
                query_window_phase(db, "s_reconcile_recovery_alive_master"),
                "MASTER_VERIFYING"
            );
            assert_eq!(
                query_session_status(db, "s_reconcile_recovery_alive_master"),
                "ACTIVE"
            );
            assert_eq!(
                query_agent_state(db, "a_reconcile_recovery_alive_master"),
                "BUSY"
            );
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn startup_reconcile_expires_window_when_master_dead() {
        with_test_db_handle(|db| {
            seed_active_recovery_session(
                db,
                "s_reconcile_recovery_dead_master",
                "a_reconcile_recovery_dead_master",
                "BUSY",
            );
            let dead_pid = i64::from(i32::MAX);
            {
                let conn = db.conn();
                conn.execute(
                    "UPDATE sessions SET master_pid = ?2 WHERE id = ?1",
                    rusqlite::params!["s_reconcile_recovery_dead_master", dead_pid],
                )
                .unwrap();
                begin_master_recovery_window_sync(
                    &conn,
                    "s_reconcile_recovery_dead_master",
                    dead_pid,
                    5,
                    true,
                    1_000,
                    10,
                )
                .unwrap();
                update_master_recovery_phase_sync(
                    &conn,
                    "s_reconcile_recovery_dead_master",
                    5,
                    "WORKERS_REPROVISIONING",
                    1_001,
                )
                .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let reconciled =
                reconcile_master_recovery_windows_with_runner_sync(db, 2_000, "ahd-test", &runner)
                    .unwrap();

            assert_eq!(reconciled, 1);
            assert_eq!(
                query_window_phase(db, "s_reconcile_recovery_dead_master"),
                "FAILED"
            );
            assert_eq!(
                query_agent_state(db, "a_reconcile_recovery_dead_master"),
                "KILLED"
            );
            assert_eq!(
                query_session_exit_reason(db, "s_reconcile_recovery_dead_master").as_deref(),
                Some("MASTER_REVIVE_WINDOW_EXPIRED")
            );
        });
    }

    #[test]
    fn startup_reconcile_preserves_window_when_master_dead_and_unexpired() {
        with_test_db_handle(|db| {
            seed_active_recovery_session(
                db,
                "s_reconcile_recovery_dead_unexpired",
                "a_reconcile_recovery_dead_unexpired",
                "BUSY",
            );
            let dead_pid = i64::from(i32::MAX);
            {
                let conn = db.conn();
                conn.execute(
                    "UPDATE sessions SET master_pid = ?2 WHERE id = ?1",
                    rusqlite::params!["s_reconcile_recovery_dead_unexpired", dead_pid],
                )
                .unwrap();
                begin_master_recovery_window_sync(
                    &conn,
                    "s_reconcile_recovery_dead_unexpired",
                    dead_pid,
                    5,
                    true,
                    1_000,
                    90,
                )
                .unwrap();
                update_master_recovery_phase_sync(
                    &conn,
                    "s_reconcile_recovery_dead_unexpired",
                    5,
                    "WORKERS_REPROVISIONING",
                    1_001,
                )
                .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let reconciled =
                reconcile_master_recovery_windows_with_runner_sync(db, 1_050, "ahd-test", &runner)
                    .unwrap();

            assert_eq!(reconciled, 0);
            assert_eq!(
                query_window_phase(db, "s_reconcile_recovery_dead_unexpired"),
                "WORKERS_REPROVISIONING"
            );
            assert_eq!(
                query_session_status(db, "s_reconcile_recovery_dead_unexpired"),
                "ACTIVE"
            );
            assert_eq!(
                query_agent_state(db, "a_reconcile_recovery_dead_unexpired"),
                "BUSY"
            );
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn startup_reconcile_leaves_terminal_window_untouched() {
        for (session_id, agent_id, phase) in [
            (
                "s_reconcile_completed",
                "a_reconcile_completed",
                "COMPLETED",
            ),
            ("s_reconcile_failed", "a_reconcile_failed", "FAILED"),
            ("s_reconcile_fused", "a_reconcile_fused", "FUSED"),
        ] {
            with_test_db_handle(|db| {
                seed_active_recovery_session(db, session_id, agent_id, "BUSY");
                {
                    let conn = db.conn();
                    begin_master_recovery_window_sync(&conn, session_id, 111, 5, true, 1_000, 10)
                        .unwrap();
                    match phase {
                        "COMPLETED" => {
                            complete_master_recovery_window_sync(&conn, session_id, 5, 1_001)
                                .unwrap();
                        }
                        other => {
                            update_master_recovery_phase_sync(&conn, session_id, 5, other, 1_001)
                                .unwrap();
                        }
                    }
                }
                let runner = FakeSystemctl::with_scopes(Vec::new());

                let reconciled = reconcile_master_recovery_windows_with_runner_sync(
                    db, 2_000, "ahd-test", &runner,
                )
                .unwrap();

                assert_eq!(reconciled, 0);
                assert_eq!(query_window_phase(db, session_id), phase);
                assert_eq!(query_session_status(db, session_id), "ACTIVE");
                assert_eq!(query_agent_state(db, agent_id), "BUSY");
                assert!(runner.stopped.borrow().is_empty());
            });
        }
    }

    #[test]
    fn startup_reconcile_runs_recovery_windows_before_orphan_scope_cleanup() {
        with_test_db_handle(|db| {
            seed_active_recovery_session(db, "s_reconcile_order", "a_reconcile_order", "BUSY");
            {
                let conn = db.conn();
                begin_master_recovery_window_sync(
                    &conn,
                    "s_reconcile_order",
                    111,
                    5,
                    true,
                    1_000,
                    10,
                )
                .unwrap();
            }
            let events = Rc::new(RefCell::new(Vec::<String>::new()));
            let state_dir = tempfile::TempDir::new().unwrap();
            let daemon_marker = crate::tmux::compute_socket_name(state_dir.path());
            let runner = RecordingSystemctl::new(
                vec![ScopeUnit {
                    unit: "run-order-agent.scope".to_string(),
                    description: format!("ccbd-agent-a_reconcile_order@{daemon_marker}"),
                }],
                events.clone(),
            );

            reconcile_startup_sync_with_state_dir_and_runner(
                db,
                Some(state_dir.path()),
                &runner,
                1_020,
                false,
            )
            .unwrap();

            let events = events.borrow();
            let recovery_list = events
                .iter()
                .position(|event| event == "list:recovery")
                .expect("recovery-window reconcile should list scopes before cascade");
            let orphan_list = events
                .iter()
                .position(|event| event == "list:orphan")
                .expect("ordinary orphan cleanup should run");
            assert!(
                recovery_list < orphan_list,
                "recovery-window reconcile must run before ordinary orphan scope cleanup: {events:?}"
            );
        });
    }

    #[test]
    fn clean_worker_runtime_resources_clears_runtime_registries_without_systemd() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_registry_cleanup",
                &[("a_registry_cleanup", "BUSY")],
            );
            let parser = std::sync::Arc::new(std::sync::Mutex::new(vt100::Parser::new(24, 80, 0)));
            crate::marker::parser_registry::register("a_registry_cleanup".to_string(), parser);
            register_fake_pidfd("a_registry_cleanup");
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_registry_cleanup",
                &["a_registry_cleanup".to_string()],
                "MASTER_EXIT",
                None,
                false,
                &runner,
            )
            .unwrap();

            assert!(outcome.registry_cleanup_count >= 1);
            assert!(!crate::marker::parser_registry::contains(
                "a_registry_cleanup"
            ));
            assert!(crate::monitor::remove("a_registry_cleanup").is_none());
        });
    }

    #[tokio::test]
    async fn clean_worker_runtime_resources_kills_agent_tmux_session_before_returning() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = crate::tmux::TmuxServer::new(tmp.path());
        let session_id = "s_master_death_tmux_cleanup";
        let agent_id = "a_tmux_cleanup";
        let agent_session = crate::tmux::agent_session_name(agent_id);
        let pipes = tmp.path().join("pipes");
        std::fs::create_dir_all(&pipes).unwrap();
        let fifo_path = pipes.join(format!("{agent_id}.fifo"));
        std::fs::write(&fifo_path, b"").unwrap();
        server
            .ensure_session_sync(&agent_session, tmp.path())
            .unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        with_test_db_handle(|db| {
            seed_master_death_session(db, session_id, &[(agent_id, "BUSY")]);
            crate::agent_io::register(
                agent_id.to_string(),
                crate::agent_io::AgentIoEntry {
                    session_id: session_id.to_string(),
                    pane_id: crate::tmux::TmuxPaneId("%1".to_string()),
                    expected_pid: None,
                    reader_handle,
                    fifo_path: fifo_path.clone(),
                    socket_name: server.socket_name().to_string(),
                    idle_scan_enabled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                        true,
                    )),
                },
            );

            let runner = FakeSystemctl::with_scopes(Vec::new());
            clean_worker_runtime_resources_with_runner_sync(
                db,
                session_id,
                &[agent_id.to_string()],
                "MASTER_EXIT",
                None,
                true,
                &runner,
            )
            .unwrap();

            let has_session = std::process::Command::new("tmux")
                .args([
                    "-L",
                    server.socket_name(),
                    "has-session",
                    "-t",
                    &agent_session,
                ])
                .output()
                .unwrap();
            assert!(
                !has_session.status.success(),
                "master-death cleanup must synchronously reap worker tmux session before returning"
            );
            assert!(!crate::agent_io::contains(agent_id));
        });

        let _ = std::process::Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
    }

    #[test]
    fn clean_worker_runtime_resources_records_scope_failure_and_uses_pidfd_fallback() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_scope_fallback",
                &[("a_scope_fallback", "BUSY")],
            );
            register_fake_pidfd("a_scope_fallback");
            let runner = FakeSystemctl {
                list_result: Ok(vec![ScopeUnit {
                    unit: "run-a-scope-fallback.scope".to_string(),
                    description: "ccbd-agent-a_scope_fallback@ahd-test-sock".to_string(),
                }]),
                stopped: RefCell::new(Vec::new()),
                stop_result: Err(std::io::ErrorKind::TimedOut),
            };

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_scope_fallback",
                &["a_scope_fallback".to_string()],
                "MASTER_EXIT",
                Some("ahd-test-sock"),
                false,
                &runner,
            )
            .unwrap();

            assert_eq!(outcome.scope_stop_failures, 1);
            assert_eq!(outcome.pidfd_kill_failures, 1);
            assert!(
                runner
                    .stopped
                    .borrow()
                    .contains(&"run-a-scope-fallback.scope".to_string())
            );
        });
    }

    #[test]
    fn clean_worker_runtime_resources_stops_matching_scope_on_success() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_scope_success",
                &[("a_scope_success", "BUSY")],
            );
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "run-a-scope-success.scope".to_string(),
                description: "ccbd-agent-a_scope_success@ahd-test-sock".to_string(),
            }]);

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_scope_success",
                &["a_scope_success".to_string()],
                "MASTER_EXIT",
                Some("ahd-test-sock"),
                false,
                &runner,
            )
            .unwrap();

            assert_eq!(outcome.scope_stop_failures, 0);
            assert!(
                runner
                    .stopped
                    .borrow()
                    .contains(&"run-a-scope-success.scope".to_string())
            );
        });
    }

    #[test]
    fn clean_worker_runtime_resources_preserves_anchor_for_active_work_revive() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_preserve_anchor",
                &[("a_preserve_anchor", "BUSY")],
            );
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "run-a-preserve-anchor.scope".to_string(),
                description: "ccbd-agent-a_preserve_anchor@ahd-test-sock".to_string(),
            }]);

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_preserve_anchor",
                &["a_preserve_anchor".to_string()],
                "MASTER_EXIT",
                Some("ahd-test-sock"),
                true,
                &runner,
            )
            .unwrap();

            assert_eq!(outcome.scope_stop_failures, 0);
            assert_eq!(
                runner.stopped.borrow().as_slice(),
                ["run-a-preserve-anchor.scope"],
                "ActiveWork revive cleanup must not stop the session anchor"
            );
        });
    }

    #[test]
    fn clean_worker_runtime_resources_stops_anchor_when_not_preserved() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_stop_anchor",
                &[("a_stop_anchor", "IDLE")],
            );
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_stop_anchor",
                &["a_stop_anchor".to_string()],
                "MASTER_EXIT",
                Some("ahd-test-sock"),
                false,
                &runner,
            )
            .unwrap();

            assert_eq!(outcome.scope_stop_failures, 0);
            let expected_anchor = unit_name_for_session("s_master_death_stop_anchor");
            assert_eq!(
                runner.stopped.borrow().as_slice(),
                [expected_anchor],
                "non-revive cleanup must still stop the session anchor"
            );
        });
    }

    #[test]
    fn clean_worker_runtime_resources_is_idempotent_for_already_cleaned_worker() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_idempotent",
                &[("a_idempotent", "KILLED")],
            );
            let runner = FakeSystemctl::with_scopes(Vec::new());

            let first = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_idempotent",
                &["a_idempotent".to_string()],
                "MASTER_EXIT",
                None,
                false,
                &runner,
            )
            .unwrap();
            let second = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_idempotent",
                &["a_idempotent".to_string()],
                "MASTER_EXIT",
                None,
                false,
                &runner,
            )
            .unwrap();

            let session_status: String = db
                .conn()
                .query_row(
                    "SELECT status FROM sessions WHERE id = 's_master_death_idempotent'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(first.db_killed_count, 0);
            assert_eq!(second.db_killed_count, 0);
            assert_eq!(session_status, "ACTIVE");
        });
    }

    #[test]
    fn clean_worker_runtime_resources_degrades_when_scope_and_pidfd_cleanup_fail() {
        with_test_db_handle(|db| {
            seed_master_death_session(
                db,
                "s_master_death_double_failure",
                &[("a_double_failure", "BUSY")],
            );
            let parser = std::sync::Arc::new(std::sync::Mutex::new(vt100::Parser::new(24, 80, 0)));
            crate::marker::parser_registry::register("a_double_failure".to_string(), parser);
            let runner = FakeSystemctl {
                list_result: Ok(vec![ScopeUnit {
                    unit: "run-a-double-failure.scope".to_string(),
                    description: "ccbd-agent-a_double_failure@ahd-test-sock".to_string(),
                }]),
                stopped: RefCell::new(Vec::new()),
                stop_result: Err(std::io::ErrorKind::TimedOut),
            };

            let outcome = clean_worker_runtime_resources_with_runner_sync(
                db,
                "s_master_death_double_failure",
                &["a_double_failure".to_string()],
                "MASTER_EXIT",
                Some("ahd-test-sock"),
                false,
                &runner,
            )
            .unwrap();

            let agent_state: String = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = 'a_double_failure'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(outcome.scope_stop_failures, 1);
            assert_eq!(outcome.pidfd_kill_failures, 1);
            assert_eq!(agent_state, "KILLED");
            assert!(!crate::marker::parser_registry::contains(
                "a_double_failure"
            ));
        });
    }

    #[test]
    fn test_cascade_kill_session_agents_counts_active_only() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(1)).unwrap();
                insert_agent_sync(&conn, "a2", "s1", "bash", "BUSY", Some(2)).unwrap();
                insert_agent_sync(&conn, "a3", "s1", "bash", "CRASHED", Some(3)).unwrap();
            }
            let count = cascade_kill_session_agents_sync(db, "s1", "MASTER_DEATH").unwrap();
            let second_count = cascade_kill_session_agents_sync(db, "s1", "MASTER_DEATH").unwrap();
            let killed_count: i64 = db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM agents WHERE state = 'KILLED'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let session_status: String = db
                .conn()
                .query_row("SELECT status FROM sessions WHERE id = 's1'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 2);
            assert_eq!(second_count, 0);
            assert_eq!(killed_count, 2);
            assert_eq!(session_status, "KILLED");
        });
    }

    #[test]
    fn test_cascade_kill_session_agents_cleans_closed_session() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_closed", "p_closed", "/tmp/closed").unwrap();
                conn.execute(
                    "UPDATE sessions SET status = 'CLOSED' WHERE id = 's_closed'",
                    [],
                )
                .unwrap();
                insert_agent_sync(&conn, "a_closed", "s_closed", "bash", "IDLE", Some(1)).unwrap();
            }

            let count =
                cascade_kill_session_agents_sync(db, "s_closed", "SESSION_CLEANUP").unwrap();
            let state: String = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = 'a_closed'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let status: String = db
                .conn()
                .query_row(
                    "SELECT status FROM sessions WHERE id = 's_closed'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(count, 1);
            assert_eq!(state, "KILLED");
            assert_eq!(status, "CLOSED");
        });
    }

    #[test]
    fn test_cascade_kill_session_agents_stops_matching_agent_scopes() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(1)).unwrap();
                insert_agent_sync(&conn, "a2", "s1", "bash", "BUSY", Some(2)).unwrap();
            }
            let runner = FakeSystemctl::with_scopes(vec![
                ScopeUnit {
                    unit: "run-a1.scope".to_string(),
                    description: "ccbd-agent-a1@ahd-test-sock".to_string(),
                },
                ScopeUnit {
                    unit: "run-a2.scope".to_string(),
                    description: "ccbd-agent-a2@ahd-test-sock".to_string(),
                },
                ScopeUnit {
                    unit: "run-foreign.scope".to_string(),
                    description: "ccbd-agent-a3@other-daemon".to_string(),
                },
            ]);

            let count = cascade_kill_session_agents_with_runner_sync(
                db,
                "s1",
                "SESSION_KILL",
                Some("ahd-test-sock"),
                &runner,
            )
            .unwrap();

            assert_eq!(count, 2);
            assert_eq!(
                runner.stopped.borrow().as_slice(),
                [
                    "run-a1.scope",
                    "run-a2.scope",
                    unit_name_for_session("s1").as_str()
                ]
            );
        });
    }

    #[test]
    fn test_cascade_kill_session_agents_skips_anchor_when_daemon_marker_absent() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(1)).unwrap();
            }
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "run-a1.scope".to_string(),
                description: "ccbd-agent-a1@ahd-test-sock".to_string(),
            }]);

            let count = cascade_kill_session_agents_with_runner_sync(
                db,
                "s1",
                "ANCHOR_UNIT_STOPPED",
                None,
                &runner,
            )
            .unwrap();

            assert_eq!(count, 1);
            assert!(
                !runner
                    .stopped
                    .borrow()
                    .contains(&unit_name_for_session("s1"))
            );
        });
    }

    #[test]
    fn test_reconcile_startup_crashes_dead_pid_and_fails_dispatched_job() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", "BUSY", Some(999_999_999)).unwrap();
                crate::db::jobs::insert_job_sync(&conn, "job_1", "a1", None, "echo hi").unwrap();
                conn.execute(
                    "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_1'",
                    [],
                )
                .unwrap();
            }
            let count = reconcile_startup_sync(db).unwrap();
            let (state, error_reason): (String, Option<String>) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.error_reason FROM agents JOIN jobs ON jobs.agent_id = agents.id WHERE agents.id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(count, 1);
            assert_eq!(state, "CRASHED");
            assert_eq!(error_reason.as_deref(), Some("STARTUP_RECONCILE_DEAD_PID"));
        });
    }

    #[test]
    fn test_reconcile_crashed_agent_removes_sandbox_dir() {
        with_test_db_handle(|db| {
            let state_dir = tempfile::TempDir::new().unwrap();
            let sandbox_dir = state_dir.path().join("sandboxes").join("s1").join("a1");
            std::fs::create_dir_all(&sandbox_dir).unwrap();
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "s1", "bash", "BUSY", Some(999_999_999)).unwrap();
            }

            let count = reconcile_active_agents_to_crashed_sync(
                db,
                Some(state_dir.path()),
                Some(&crate::tmux::compute_socket_name(state_dir.path())),
            )
            .unwrap();

            assert_eq!(count, 1);
            assert!(!sandbox_dir.exists());
        });
    }

    #[test]
    fn test_remove_agent_sandbox_dir_removes_materialized_home() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let sandbox_dir = state_dir.path().join("sandboxes").join("s1").join("a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let materialized_home = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let cleanup_home = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        fs::create_dir_all(materialized_home.join(".claude")).unwrap();
        fs::write(
            materialized_home.join(".claude/.credentials.json"),
            b"secret",
        )
        .unwrap();

        assert_eq!(cleanup_home, materialized_home);

        remove_agent_sandbox_dir_sync(state_dir.path(), "s1", "a1");

        assert!(!sandbox_dir.exists());
        assert!(!materialized_home.exists());
    }

    /// #27: a normal shutdown used to delete the operator's real session along
    /// with the sandbox. The records must reach the project first.
    #[test]
    fn test_remove_agent_sandbox_dir_archives_session_records_to_the_project() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let project = tempfile::TempDir::new().unwrap();
        let sandbox_dir = crate::sandbox::path::resolve_sandbox_dir(
            state_dir.path(),
            "s1",
            "a1",
            project.path(),
        )
        .unwrap();
        let home = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        fs::create_dir_all(home.join(".codex/sessions")).unwrap();
        fs::write(home.join(".codex/sessions/rollout.jsonl"), b"{\"turn\":1}").unwrap();

        remove_agent_sandbox_dir_sync(state_dir.path(), "s1", "a1");

        assert!(!home.exists(), "the sandbox is still destroyed");
        assert!(!sandbox_dir.exists());
        let archived = project
            .path()
            .join(".ah/sessions/s1/a1/codex/.codex/sessions/rollout.jsonl");
        assert_eq!(
            fs::read_to_string(&archived).unwrap(),
            "{\"turn\":1}",
            "the session record must survive the sandbox at {}",
            archived.display()
        );
    }

    /// Losing a session is unrecoverable; leaving a sandbox on disk is not.
    #[test]
    fn test_remove_agent_sandbox_dir_keeps_the_home_when_archiving_fails() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let project = tempfile::TempDir::new().unwrap();
        // A file where the archive root needs a directory makes the copy fail.
        fs::write(project.path().join(".ah"), b"not a directory").unwrap();
        let sandbox_dir = crate::sandbox::path::resolve_sandbox_dir(
            state_dir.path(),
            "s_fail",
            "a1",
            project.path(),
        )
        .unwrap();
        let home = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        fs::create_dir_all(home.join(".codex/sessions")).unwrap();
        fs::write(home.join(".codex/sessions/rollout.jsonl"), b"{\"turn\":1}").unwrap();

        remove_agent_sandbox_dir_sync(state_dir.path(), "s_fail", "a1");

        assert!(
            home.join(".codex/sessions/rollout.jsonl").is_file(),
            "an unarchivable session must not be destroyed"
        );
        assert!(sandbox_dir.exists(), "the marker must survive for a retry");
        fs::remove_dir_all(&home).unwrap();
    }

    /// A crash preserves the home for recovery. The record of which project
    /// owns that home has to survive with it, or the eventual cleanup has
    /// nowhere to hand the session records.
    #[test]
    fn test_preserving_home_cleanup_keeps_the_project_marker() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let project = tempfile::TempDir::new().unwrap();
        let sandbox_dir = crate::sandbox::path::resolve_sandbox_dir(
            state_dir.path(),
            "s_crash",
            "a1",
            project.path(),
        )
        .unwrap();
        fs::create_dir_all(sandbox_dir.join("scratch")).unwrap();
        let home = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        fs::create_dir_all(home.join(".codex/sessions")).unwrap();
        fs::write(home.join(".codex/sessions/rollout.jsonl"), b"{\"turn\":1}").unwrap();

        remove_agent_sandbox_dir_preserving_home_sync(state_dir.path(), "s_crash", "a1");

        assert!(!sandbox_dir.join("scratch").exists());
        assert_eq!(
            crate::sandbox::session_archive::read_project_root_marker(&sandbox_dir).as_deref(),
            Some(project.path()),
            "the preserved home must keep knowing which project owns it"
        );

        // And the eventual destruction can therefore still archive.
        remove_agent_sandbox_dir_sync(state_dir.path(), "s_crash", "a1");
        assert!(
            project
                .path()
                .join(".ah/sessions/s_crash/a1/codex/.codex/sessions/rollout.jsonl")
                .is_file()
        );
    }

    /// Sandboxes created before the marker existed must stay deletable.
    #[test]
    fn test_remove_agent_sandbox_dir_destroys_a_sandbox_without_a_project_marker() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let sandbox_dir = state_dir
            .path()
            .join("sandboxes")
            .join("s_legacy")
            .join("a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let home = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        fs::create_dir_all(home.join(".codex/sessions")).unwrap();
        fs::write(home.join(".codex/sessions/rollout.jsonl"), b"{\"turn\":1}").unwrap();

        remove_agent_sandbox_dir_sync(state_dir.path(), "s_legacy", "a1");

        assert!(!home.exists());
        assert!(!sandbox_dir.exists());
    }

    #[test]
    fn test_reconcile_dead_pid_removes_materialized_home_but_alive_reattach_preserves_it() {
        with_test_db_handle(|db| {
            let state_dir = tempfile::TempDir::new().unwrap();
            let dead_sandbox = state_dir.path().join("sandboxes").join("s1").join("a_dead");
            let alive_sandbox = state_dir
                .path()
                .join("sandboxes")
                .join("s1")
                .join("a_alive");
            fs::create_dir_all(&dead_sandbox).unwrap();
            fs::create_dir_all(&alive_sandbox).unwrap();
            let dead_home = sandbox_home_for_sandbox_dir(&dead_sandbox).unwrap();
            let alive_home = sandbox_home_for_sandbox_dir(&alive_sandbox).unwrap();
            fs::create_dir_all(dead_home.join(".codex")).unwrap();
            fs::create_dir_all(alive_home.join(".codex")).unwrap();
            fs::write(dead_home.join(".codex/auth.json"), b"dead").unwrap();
            fs::write(alive_home.join(".codex/auth.json"), b"alive").unwrap();
            let pipes = state_dir.path().join("pipes");
            fs::create_dir_all(&pipes).unwrap();
            fs::write(pipes.join("a_alive.fifo"), b"").unwrap();
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a_dead", "s1", "bash", "BUSY", Some(999_999_999))
                    .unwrap();
                insert_agent_sync(
                    &conn,
                    "a_alive",
                    "s1",
                    "bash",
                    "IDLE",
                    Some(std::process::id() as i64),
                )
                .unwrap();
            }

            let count = reconcile_active_agents_to_crashed_sync(
                db,
                Some(state_dir.path()),
                Some(&crate::tmux::compute_socket_name(state_dir.path())),
            )
            .unwrap();

            assert_eq!(count, 1);
            assert!(!dead_sandbox.exists());
            assert!(!dead_home.exists());
            assert!(alive_sandbox.exists());
            assert!(alive_home.exists());
            let _ = crate::monitor::remove("a_alive");
            crate::agent_io::registry::cleanup_agent_runtime_resources("a_alive", Some("s_alive"));
        });
    }

    #[test]
    fn test_reconcile_startup_keeps_alive_pid_active() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(
                    &conn,
                    "a_alive",
                    "s1",
                    "bash",
                    "IDLE",
                    Some(std::process::id() as i64),
                )
                .unwrap();
            }
            let count = reconcile_startup_sync(db).unwrap();
            let state: String = db
                .conn()
                .query_row("SELECT state FROM agents WHERE id = 'a_alive'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
            assert_eq!(state, "IDLE");
            let _ = crate::monitor::remove("a_alive");
        });
    }

    #[tokio::test]
    async fn startup_reconcile_restores_claude_gateway_master_and_worker_idempotently() {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = init(db_file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let seed_dir = tempfile::TempDir::new().unwrap();
        let seed_path = seed_dir.path().join(".credentials.json");
        fs::write(
            &seed_path,
            serde_json::json!({
                "claudeAiOauth": {
                    "accessToken": "seed-access",
                    "refreshToken": "seed-refresh",
                    "expiresAt": 4_102_444_800_000_i64
                }
            })
            .to_string(),
        )
        .unwrap();
        let pipes = state_dir.path().join("pipes");
        fs::create_dir_all(&pipes).unwrap();
        fs::write(pipes.join("a_claude_alive.fifo"), b"").unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_claude_alive", "p1", "/tmp/foo").unwrap();
            conn.execute(
                "UPDATE sessions SET status = 'ACTIVE' WHERE id = 's_claude_alive'",
                [],
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_claude_alive",
                "s_claude_alive",
                "claude",
                "IDLE",
                Some(std::process::id() as i64),
            )
            .unwrap();
        }
        let gateway =
            Arc::new(crate::claude_gateway::ClaudeGatewayService::new_with_seed_path(seed_path));
        let socket_name = crate::tmux::compute_socket_name(state_dir.path());

        for _ in 0..2 {
            let count = super::reconcile_startup_with_tmux_socket_and_gateway(
                db.clone(),
                state_dir.path().to_path_buf(),
                Some(socket_name.clone()),
                Some(gateway.clone()),
            )
            .await
            .unwrap();
            assert_eq!(count, 0);
            assert!(
                crate::claude_gateway::gateway_worker_topology(
                    &state_dir
                        .path()
                        .join("sandboxes")
                        .join("s_claude_alive")
                        .join("master"),
                    "s_claude_alive",
                )
                .unwrap()
                .host_uds_path
                .exists()
            );
            assert!(
                crate::claude_gateway::gateway_worker_topology(
                    &state_dir
                        .path()
                        .join("sandboxes")
                        .join("s_claude_alive")
                        .join("a_claude_alive"),
                    "a_claude_alive",
                )
                .unwrap()
                .host_uds_path
                .exists()
            );
        }

        let _ = crate::monitor::remove("a_claude_alive");
        crate::agent_io::registry::cleanup_agent_runtime_resources(
            "a_claude_alive",
            Some("s_claude_alive"),
        );
        gateway.deregister("s_claude_alive").await;
        gateway.deregister("a_claude_alive").await;
    }

    #[test]
    fn test_reconcile_startup_preserves_prompt_pending_agent() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_pending", "p_pending", "/tmp/pending").unwrap();
                insert_agent_sync(
                    &conn,
                    "a_pending",
                    "s_pending",
                    "bash",
                    STATE_PROMPT_PENDING,
                    Some(999_999_999),
                )
                .unwrap();
                crate::db::jobs::insert_job_sync(&conn, "job_pending", "a_pending", None, "queued")
                    .unwrap();
            }

            let count = reconcile_startup_sync(db).unwrap();
            let (state, job_status, state_change_count): (String, String, i64) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status, COUNT(events.seq_id) \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     LEFT JOIN events ON events.agent_id = agents.id \
                     WHERE agents.id = 'a_pending'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(count, 0);
            assert_eq!(state, STATE_PROMPT_PENDING);
            assert_eq!(job_status, "QUEUED");
            assert_eq!(state_change_count, 0);
        });
    }

    struct FakeSystemctl {
        list_result: Result<Vec<ScopeUnit>, std::io::ErrorKind>,
        stopped: RefCell<Vec<String>>,
        stop_result: Result<(), std::io::ErrorKind>,
    }

    impl FakeSystemctl {
        fn with_scopes(scopes: Vec<ScopeUnit>) -> Self {
            Self {
                list_result: Ok(scopes),
                stopped: RefCell::new(Vec::new()),
                stop_result: Ok(()),
            }
        }
    }

    impl SystemctlRunner for FakeSystemctl {
        fn list_scope_units(&self) -> Result<Vec<ScopeUnit>, std::io::Error> {
            self.list_result
                .clone()
                .map_err(|kind| std::io::Error::new(kind, "fake systemctl failure"))
        }

        fn stop_unit(&self, unit: &str) -> Result<(), std::io::Error> {
            self.stopped.borrow_mut().push(unit.to_string());
            self.stop_result
                .map_err(|kind| std::io::Error::new(kind, "fake systemctl stop failure"))
        }
    }

    struct RecordingSystemctl {
        scopes: Vec<ScopeUnit>,
        events: Rc<RefCell<Vec<String>>>,
        list_calls: RefCell<usize>,
    }

    impl RecordingSystemctl {
        fn new(scopes: Vec<ScopeUnit>, events: Rc<RefCell<Vec<String>>>) -> Self {
            Self {
                scopes,
                events,
                list_calls: RefCell::new(0),
            }
        }
    }

    impl SystemctlRunner for RecordingSystemctl {
        fn list_scope_units(&self) -> Result<Vec<ScopeUnit>, std::io::Error> {
            let mut list_calls = self.list_calls.borrow_mut();
            *list_calls += 1;
            let label = if *list_calls == 1 {
                "list:recovery"
            } else {
                "list:orphan"
            };
            self.events.borrow_mut().push(label.to_string());
            Ok(self.scopes.clone())
        }

        fn stop_unit(&self, unit: &str) -> Result<(), std::io::Error> {
            self.events.borrow_mut().push(format!("stop:{unit}"));
            Ok(())
        }
    }

    struct ThreadSafeRecordingSystemctl {
        scopes: Vec<ScopeUnit>,
        stopped: Arc<Mutex<Vec<String>>>,
    }

    impl ThreadSafeRecordingSystemctl {
        fn new(scopes: Vec<ScopeUnit>, stopped: Arc<Mutex<Vec<String>>>) -> Self {
            Self { scopes, stopped }
        }
    }

    impl SystemctlRunner for ThreadSafeRecordingSystemctl {
        fn list_scope_units(&self) -> Result<Vec<ScopeUnit>, std::io::Error> {
            Ok(self.scopes.clone())
        }

        fn stop_unit(&self, unit: &str) -> Result<(), std::io::Error> {
            self.stopped.lock().unwrap().push(unit.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_reconcile_startup_retains_live_agent_scope_and_reaps_orphan() {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = init(db_file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let daemon_marker = crate::tmux::compute_socket_name(state_dir.path());
        {
            let conn = db.conn();
            insert_session_sync(&conn, "sess_reconcile_live", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(
                &conn,
                "a_reconcile_live",
                "sess_reconcile_live",
                "bash",
                "PROMPT_PENDING",
                None,
            )
            .unwrap();
        }
        let stopped = Arc::new(Mutex::new(Vec::<String>::new()));
        let runner = Arc::new(ThreadSafeRecordingSystemctl::new(
            vec![
                ScopeUnit {
                    unit: "run-live-agent.scope".to_string(),
                    description: format!("ccbd-agent-a_reconcile_live@{daemon_marker}"),
                },
                ScopeUnit {
                    unit: "run-orphan-agent.scope".to_string(),
                    description: format!("ccbd-agent-a_reconcile_orphan@{daemon_marker}"),
                },
            ],
            stopped.clone(),
        ));

        let count = super::reconcile_startup_with_tmux_socket_and_runner_for_test(
            db,
            state_dir.path().to_path_buf(),
            Some(daemon_marker),
            runner,
            2_000,
            false,
        )
        .await
        .unwrap();

        assert_eq!(count, 1);
        assert_eq!(
            stopped.lock().unwrap().as_slice(),
            ["run-orphan-agent.scope"]
        );
    }

    #[test]
    fn test_reconcile_orphan_scopes_never_stops_foreign_marker_overlapping_agent_ids() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "sess_dead", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "sess_dead", "bash", "CRASHED", Some(1)).unwrap();
                insert_agent_sync(&conn, "a2", "sess_dead", "bash", "KILLED", Some(2)).unwrap();
            }
            let marker_a = "ahd-marker-a";
            let marker_b = "ahd-marker-b";
            let runner = FakeSystemctl::with_scopes(vec![
                ScopeUnit {
                    unit: "run-foreign-a1.scope".to_string(),
                    description: format!("ccbd-agent-a1@{marker_b}"),
                },
                ScopeUnit {
                    unit: "run-foreign-a2.scope".to_string(),
                    description: format!("ccbd-agent-a2@{marker_b}"),
                },
                ScopeUnit {
                    unit: "run-own-orphan.scope".to_string(),
                    description: format!("ccbd-agent-a_orphan@{marker_a}"),
                },
            ]);

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, marker_a, false).unwrap();

            assert_eq!(count, 1);
            assert_eq!(runner.stopped.borrow().as_slice(), ["run-own-orphan.scope"]);
        });
    }

    #[tokio::test]
    async fn test_reconcile_startup_respects_marker_ownership_for_overlapping_agent_ids() {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = init(db_file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let marker_a = crate::tmux::compute_socket_name(state_dir.path());
        let marker_b = "ahd-foreign-marker";
        {
            let conn = db.conn();
            insert_session_sync(&conn, "sess_dead", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "sess_dead", "bash", "CRASHED", Some(1)).unwrap();
            insert_agent_sync(&conn, "a2", "sess_dead", "bash", "KILLED", Some(2)).unwrap();
        }
        let stopped = Arc::new(Mutex::new(Vec::<String>::new()));
        let runner = Arc::new(ThreadSafeRecordingSystemctl::new(
            vec![
                ScopeUnit {
                    unit: "run-foreign-a1.scope".to_string(),
                    description: format!("ccbd-agent-a1@{marker_b}"),
                },
                ScopeUnit {
                    unit: "run-foreign-a2.scope".to_string(),
                    description: format!("ccbd-agent-a2@{marker_b}"),
                },
                ScopeUnit {
                    unit: "run-own-orphan.scope".to_string(),
                    description: format!("ccbd-agent-a_orphan@{marker_a}"),
                },
            ],
            stopped.clone(),
        ));

        let count = super::reconcile_startup_with_tmux_socket_and_runner_for_test(
            db,
            state_dir.path().to_path_buf(),
            Some(marker_a),
            runner,
            2_000,
            false,
        )
        .await
        .unwrap();

        assert_eq!(count, 1);
        assert_eq!(stopped.lock().unwrap().as_slice(), ["run-own-orphan.scope"]);
    }

    #[tokio::test]
    async fn test_reconcile_startup_with_ambient_marker_never_stops_scopes() {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = init(db_file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let leaked_marker = "ahd-9819d8d7587886a9";
        let stopped = Arc::new(Mutex::new(Vec::<String>::new()));
        let runner = Arc::new(ThreadSafeRecordingSystemctl::new(
            vec![ScopeUnit {
                unit: "run-live-a1.scope".to_string(),
                description: format!("ccbd-agent-a1@{leaked_marker}"),
            }],
            stopped.clone(),
        ));

        let count = super::reconcile_startup_with_ambient_marker_and_runner_for_test(
            db,
            state_dir.path().to_path_buf(),
            Some(leaked_marker.to_string()),
            runner,
            2_000,
            false,
        )
        .await
        .unwrap();

        assert_eq!(count, 0);
        assert!(stopped.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[serial_test::serial(global_env)]
    async fn test_reconcile_startup_from_env_marker_is_ambient_and_never_stops_scopes() {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = init(db_file.path()).unwrap();
        let state_dir_a = tempfile::TempDir::new().unwrap();
        let marker_a = crate::tmux::compute_socket_name(state_dir_a.path());
        let marker_b = "ahd-foreignmarker0001";
        let _env_guard = EnvGuard::set("AH_STATE_DIR", state_dir_a.path());
        let stopped = Arc::new(Mutex::new(Vec::<String>::new()));
        let runner = Arc::new(ThreadSafeRecordingSystemctl::new(
            vec![
                ScopeUnit {
                    unit: "run-foreign-a1.scope".to_string(),
                    description: format!("ccbd-agent-a1@{marker_b}"),
                },
                ScopeUnit {
                    unit: "run-foreign-a2.scope".to_string(),
                    description: format!("ccbd-agent-a2@{marker_b}"),
                },
                ScopeUnit {
                    unit: "run-env-owned-orphan.scope".to_string(),
                    description: format!("ccbd-agent-a_orphan@{marker_a}"),
                },
            ],
            stopped.clone(),
        ));

        let count = super::reconcile_startup_from_ambient_env_with_runner_for_test(
            db, runner, 2_000, false,
        )
        .await
        .unwrap();

        assert_eq!(count, 0);
        assert!(stopped.lock().unwrap().is_empty());
    }

    #[test]
    fn test_reconcile_orphan_scopes_skips_foreign_daemon_scopes() {
        with_test_db_handle(|db| {
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "ccbd-tmux-deadbeefcafebabe.scope".to_string(),
                description: "legacy Python ccb tmux scope".to_string(),
            }]);

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, "ccbd-own", false).unwrap();

            assert_eq!(count, 0);
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn test_reconcile_orphan_scopes_dry_run_does_not_stop() {
        with_test_db_handle(|db| {
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "run-123.scope".to_string(),
                description: "ccbd-agent-a_dead@ccbd-own".to_string(),
            }]);

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, "ccbd-own", true).unwrap();

            assert_eq!(count, 1);
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn test_reconcile_orphan_scopes_force_mode_stops() {
        with_test_db_handle(|db| {
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "run-123.scope".to_string(),
                description: "ccbd-agent-a_dead@ccbd-own".to_string(),
            }]);

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, "ccbd-own", false).unwrap();

            assert_eq!(count, 1);
            assert_eq!(runner.stopped.borrow().as_slice(), ["run-123.scope"]);
        });
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn test_reconcile_orphan_scopes_defaults_to_real_stop() {
        unsafe {
            std::env::remove_var("CCBD_RECONCILE_DRY_RUN");
            std::env::remove_var("CCBD_RECONCILE_FORCE");
        }

        assert!(!reconcile_orphan_scopes_dry_run_enabled());
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn test_reconcile_orphan_scopes_dry_run_escape_hatch() {
        unsafe {
            std::env::set_var("CCBD_RECONCILE_DRY_RUN", "1");
            std::env::remove_var("CCBD_RECONCILE_FORCE");
        }

        assert!(reconcile_orphan_scopes_dry_run_enabled());

        unsafe {
            std::env::remove_var("CCBD_RECONCILE_DRY_RUN");
        }
    }

    #[test]
    fn test_reconcile_orphan_scopes_skips_known_agent_with_stale_marker() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "sess_dead", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a_dead", "sess_dead", "bash", "CRASHED", Some(1))
                    .unwrap();
            }
            let runner = FakeSystemctl::with_scopes(vec![
                ScopeUnit {
                    unit: "run-stale-agent.scope".to_string(),
                    description: "ccbd-agent-a_dead@ccbd-old-generation".to_string(),
                },
                ScopeUnit {
                    unit: "run-foreign-agent.scope".to_string(),
                    description: "ccbd-agent-foreign@ccbd-old-generation".to_string(),
                },
            ]);

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, "ccbd-new-generation", false)
                    .unwrap();

            assert_eq!(count, 0);
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn test_reconcile_orphan_scopes_keeps_active_session_scope() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "sess_active", "p1", "/tmp/foo").unwrap();
                insert_agent_sync(&conn, "a1", "sess_active", "bash", "IDLE", Some(1)).unwrap();
            }
            let runner = FakeSystemctl::with_scopes(vec![ScopeUnit {
                unit: "run-123.scope".to_string(),
                description: "ccbd-agent-a1@ccbd-own sess_active".to_string(),
            }]);

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, "ccbd-own", false).unwrap();

            assert_eq!(count, 0);
            assert!(runner.stopped.borrow().is_empty());
        });
    }

    #[test]
    fn test_reconcile_orphan_scopes_handles_missing_systemctl_gracefully() {
        with_test_db_handle(|db| {
            let runner = FakeSystemctl {
                list_result: Err(std::io::ErrorKind::NotFound),
                stopped: RefCell::new(Vec::new()),
                stop_result: Ok(()),
            };

            let count =
                reconcile_orphan_scopes_with_runner_sync(db, &runner, "ccbd-own", false).unwrap();

            assert_eq!(count, 0);
            assert!(runner.stopped.borrow().is_empty());
        });
    }
}
