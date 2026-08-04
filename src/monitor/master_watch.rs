use crate::completion::reader::{LogCursorMap, read_provider_assistant_progress_after_cursors};
use crate::db::Db;
use crate::db::master_recovery::resolve_master_revive_readiness_timeout_secs;
use crate::db::system::{
    MasterDeathSessionActivity, cascade_kill_session_agents, clean_worker_runtime_resources_sync,
    snapshot_master_death_session_activity,
};
use crate::error::CcbdError;
use crate::master_revival::{
    MasterDeathDecision, MasterReviveAttemptDecision, MasterTransitionOutcome,
    begin_master_recovery_readiness_wait_for_master_watch,
    begin_master_recovery_window_for_snapshot, classify_master_death,
    complete_claimed_master_transition, complete_master_recovery_window_for_master_watch,
    fail_master_recovery_readiness_for_master_watch, mark_master_recovery_non_revive_terminal,
    mark_master_recovery_phase, mark_master_recovery_ready_for_master_watch,
    mark_session_closed_after_idle_master_death, master_recovery_effective_readiness_timeout,
    master_recovery_verifying_window_expected_generation, master_runtime_matches,
    master_spawn_lock, persist_revived_master_cmd, query_master_revive_next_retry_at,
    record_master_revive_attempt, recovered_workers_ready_sync,
    remove_master_monitor_key_if_generation_matches, try_claim_master_transition,
};
use crate::monitor::master_reaper::{
    FailedReviveMasterReapTarget, ReviveMasterReadinessCheck,
    inject_master_continue_instruction_best_effort, prepare_revive_master_readiness_check,
    register_revived_master_watch_and_prepare_readiness,
    reprovision_declared_workers_after_master_revive, spawn_master_confirm_timer,
    spawn_replacement_master_pane, write_master_revival_redispatch_marker,
};
use crate::provider::home_layout::sandbox_home_for_sandbox_dir;
use crate::rpc::Ctx;
use crate::sandbox::{EnvState, path};
use crate::tmux::{TmuxPaneId, TmuxServer};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
#[cfg(unix)]
use tokio::io::{Interest, unix::AsyncFd};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterDeathSource {
    Pidfd,
    StartupRearmDead,
    Patrol,
}

#[derive(Debug, Clone)]
struct ActiveMasterWatchRow {
    session_id: String,
    master_pid: i64,
    master_generation: i64,
    master_pane_id: Option<String>,
    absolute_path: String,
    master_cmd: Option<String>,
}

pub async fn handle_master_death_detected(
    ctx: &Ctx,
    session_id: String,
    expected_pid: i64,
    expected_generation: i64,
    master_cmd: String,
    source: MasterDeathSource,
) -> Result<(), CcbdError> {
    let spawn_lock = master_spawn_lock(&session_id);
    let _spawn_guard = spawn_lock.lock().await;
    match classify_master_death(&ctx.db, &session_id, expected_pid, expected_generation)? {
        MasterDeathDecision::Revive => {
            tracing::warn!(
                session_id = %session_id,
                expected_pid,
                expected_generation,
                source = ?source,
                "master death detected; routing to revive path"
            );
            revive_master_after_exit_locked(
                session_id,
                expected_pid,
                expected_generation,
                ctx.db.clone(),
                ctx.tmux_server.clone(),
                master_cmd,
                ctx.state_dir.clone(),
                ctx.env_state.clone(),
                ctx.daemon_unit.clone(),
            )
            .await
        }
        MasterDeathDecision::IntentionalExit | MasterDeathDecision::Stale => {
            mark_master_recovery_non_revive_terminal(
                &ctx.db,
                &session_id,
                expected_generation,
                unixepoch(),
            )?;
            tracing::info!(
                session_id = %session_id,
                expected_pid,
                expected_generation,
                source = ?source,
                "master death ignored for non-active or stale session"
            );
            Ok(())
        }
    }
}

pub async fn rearm_active_master_watches_on_startup(ctx: &Ctx) -> Result<usize, CcbdError> {
    let rows = query_active_master_watch_rows(&ctx.db)?;
    let mut armed_or_detected = 0;
    for row in rows {
        if arm_or_route_master_watch(ctx, row, MasterDeathSource::StartupRearmDead).await? {
            armed_or_detected += 1;
        }
    }
    Ok(armed_or_detected)
}

pub(crate) async fn patrol_active_masters_once(ctx: &Ctx) -> Result<usize, CcbdError> {
    let rows = query_active_master_watch_rows(&ctx.db)?;
    let mut detected = 0;
    for row in rows {
        let key = crate::master_revival::master_monitor_key(&row.session_id, row.master_generation);
        if !crate::monitor::contains(&key)
            && arm_or_route_master_watch(ctx, row.clone(), MasterDeathSource::Patrol).await?
        {
            if !crate::monitor::contains(&key) {
                detected += 1;
            }
            continue;
        }

        if !master_process_is_alive(row.master_pid) {
            let master_cmd = resolve_master_cmd_for_watch(&row);
            handle_master_death_detected(
                ctx,
                row.session_id,
                row.master_pid,
                row.master_generation,
                master_cmd,
                MasterDeathSource::Patrol,
            )
            .await?;
            detected += 1;
        }
    }
    Ok(detected)
}

pub fn master_process_is_alive(pid: i64) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    crate::monitor::pidfd_open(pid).is_ok()
}

async fn arm_or_route_master_watch(
    ctx: &Ctx,
    row: ActiveMasterWatchRow,
    dead_source: MasterDeathSource,
) -> Result<bool, CcbdError> {
    if row.master_pid <= 0 {
        tracing::warn!(
            session_id = %row.session_id,
            master_pid = row.master_pid,
            "active session has invalid master_pid; skipping master watch arm"
        );
        return Ok(false);
    }
    let key = crate::master_revival::master_monitor_key(&row.session_id, row.master_generation);
    if crate::monitor::contains(&key) {
        return Ok(false);
    }
    let master_cmd = resolve_master_cmd_for_watch(&row);
    let pid = i32::try_from(row.master_pid)
        .map_err(|_| CcbdError::PtyIoError(format!("invalid master pid: {}", row.master_pid)))?;
    let pidfd = match crate::monitor::pidfd_open(pid) {
        Ok(pidfd) => pidfd,
        Err(CcbdError::AgentUnexpectedExit { .. }) => {
            handle_master_death_detected(
                ctx,
                row.session_id,
                row.master_pid,
                row.master_generation,
                master_cmd,
                dead_source,
            )
            .await?;
            return Ok(true);
        }
        Err(err) => {
            tracing::warn!(
                session_id = %row.session_id,
                master_pid = row.master_pid,
                error = %err,
                "failed to open master pidfd while arming watch"
            );
            return Ok(false);
        }
    };

    if !stored_master_pane_still_matches(ctx, &row).await {
        handle_master_death_detected(
            ctx,
            row.session_id,
            row.master_pid,
            row.master_generation,
            master_cmd,
            dead_source,
        )
        .await?;
        return Ok(true);
    }

    let task_fd = pidfd
        .try_clone()
        .map_err(|err| CcbdError::PtyIoError(format!("clone master pidfd for watcher: {err}")))?;
    crate::monitor::register(key, pidfd);
    spawn_master_pidfd_watch_task(
        row.session_id.clone(),
        row.master_pid,
        row.master_generation,
        task_fd,
        ctx.db.clone(),
        ctx.tmux_server.clone(),
        master_cmd.clone(),
        ctx.state_dir.clone(),
        ctx.env_state.clone(),
        ctx.daemon_unit.clone(),
    );
    if let Some(window_expected_generation) =
        master_recovery_verifying_window_expected_generation(&ctx.db, &row.session_id)?
    {
        let Some(pane_id) = row.master_pane_id.as_deref() else {
            return Ok(true);
        };
        let pane = match TmuxPaneId::parse(pane_id) {
            Ok(pane) => pane,
            Err(err) => {
                tracing::warn!(
                    session_id = %row.session_id,
                    pane_id,
                    error = %err,
                    "cannot resume master recovery readiness because stored pane id is invalid"
                );
                return Ok(true);
            }
        };
        tracing::info!(
            session_id = %row.session_id,
            expected_pid = row.master_pid,
            runtime_generation = row.master_generation,
            window_expected_generation,
            "resuming in-flight master recovery readiness after startup rearm"
        );
        resume_master_recovery_readiness(
            ctx,
            &row.session_id,
            row.master_pid,
            row.master_generation,
            window_expected_generation,
            &pane,
            None,
            prepare_revive_master_readiness_check(
                &row.session_id,
                &master_cmd,
                &master_sandbox_home_for_watch_row(ctx, &row)?,
            )?,
        )
        .await?;
    }
    Ok(true)
}

async fn stored_master_pane_still_matches(ctx: &Ctx, row: &ActiveMasterWatchRow) -> bool {
    let Some(pane_id) = row.master_pane_id.as_deref() else {
        tracing::warn!(
            session_id = %row.session_id,
            master_pid = row.master_pid,
            "active master row has no master_pane_id; treating master as dead for watch arm"
        );
        return false;
    };
    let pane = match TmuxPaneId::parse(pane_id) {
        Ok(pane) => pane,
        Err(err) => {
            tracing::warn!(
                session_id = %row.session_id,
                pane_id,
                error = %err,
                "stored master_pane_id is invalid; treating master as dead"
            );
            return false;
        }
    };
    match ctx.tmux_server.get_pane_pid(pane).await {
        Ok(pid) if i64::from(pid) == row.master_pid => true,
        Ok(pid) => {
            tracing::warn!(
                session_id = %row.session_id,
                expected_pid = row.master_pid,
                actual_pid = pid,
                "stored master pane pid mismatch; treating master as dead"
            );
            false
        }
        Err(err) => {
            tracing::warn!(
                session_id = %row.session_id,
                pane_id,
                error = %err,
                "stored master pane missing; treating master as dead"
            );
            false
        }
    }
}

fn query_active_master_watch_rows(db: &Db) -> Result<Vec<ActiveMasterWatchRow>, CcbdError> {
    let conn = db.conn();
    let mut stmt = conn
        .prepare(
            "SELECT sessions.id, sessions.master_pid, sessions.master_generation,
                    sessions.master_pane_id, projects.absolute_path, sessions.master_cmd
             FROM sessions
             JOIN projects ON projects.id = sessions.project_id
             WHERE sessions.status = 'ACTIVE' AND sessions.master_pid > 0
             ORDER BY sessions.created_at ASC, sessions.id ASC",
        )
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("prepare active master watches: {err}"))
        })?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ActiveMasterWatchRow {
                session_id: row.get(0)?,
                master_pid: row.get(1)?,
                master_generation: row.get(2)?,
                master_pane_id: row.get(3)?,
                absolute_path: row.get(4)?,
                master_cmd: row.get(5)?,
            })
        })
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("query active master watches: {err}"))
        })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|err| {
        CcbdError::DbConstraintViolation(format!("collect active master watches: {err}"))
    })
}

fn master_sandbox_home_for_watch_row(
    ctx: &Ctx,
    row: &ActiveMasterWatchRow,
) -> Result<PathBuf, CcbdError> {
    if ctx.env_state.unsafe_no_sandbox {
        return Ok(PathBuf::from(&row.absolute_path));
    }
    let sandbox_dir = path::resolve_sandbox_dir(&ctx.state_dir, &row.session_id, "master")?;
    sandbox_home_for_sandbox_dir(&sandbox_dir)
}

fn resolve_master_cmd_for_watch(row: &ActiveMasterWatchRow) -> String {
    if let Some(cmd) = row.master_cmd.as_deref()
        && !cmd.trim().is_empty()
    {
        return cmd.to_string();
    }
    let config_cmd =
        crate::cli::config::load_project_config(&PathBuf::from(&row.absolute_path).join("ah.toml"))
            .map(|config| config.master.cmd)
            .unwrap_or_else(|_| crate::cli::config::default_master_cmd());
    tracing::warn!(
        session_id = %row.session_id,
        fallback_cmd = %config_cmd,
        "sessions.master_cmd is missing; falling back for master watch recovery"
    );
    config_cmd
}

pub async fn master_watch_patrol_loop(ctx: Ctx, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(err) = patrol_active_masters_once(&ctx).await {
            tracing::warn!(error = %err, "master watch patrol tick failed");
        }
    }
}

pub fn resolve_master_watch_patrol_interval() -> Duration {
    let secs = std::env::var("AH_MASTER_WATCH_PATROL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(10);
    Duration::from_secs(secs)
}

fn ctx_from_watch_parts(
    db: Db,
    tmux_server: Arc<TmuxServer>,
    state_dir: PathBuf,
    env_state: EnvState,
    daemon_unit: Option<String>,
) -> Ctx {
    Ctx {
        db,
        state_dir,
        env_state,
        daemon_unit,
        tmux_server,
        claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
    }
}

pub fn spawn_master_pidfd_watch_task(
    session_id: String,
    expected_pid: i64,
    expected_generation: i64,
    pidfd: crate::monitor::MonitorHandle,
    db: Db,
    tmux_server: Arc<TmuxServer>,
    master_cmd: String,
    state_dir: PathBuf,
    env_state: EnvState,
    daemon_unit: Option<String>,
) {
    #[cfg(windows)]
    {
        let _ = (
            expected_pid,
            pidfd,
            db,
            tmux_server,
            master_cmd,
            state_dir,
            env_state,
            daemon_unit,
        );
        tokio::spawn(async move {
            tracing::warn!(
                session_id = %session_id,
                expected_generation,
                "Windows master process watcher is not implemented until M1"
            );
        });
    }

    #[cfg(unix)]
    tokio::spawn(async move {
        let async_fd = match AsyncFd::with_interest(pidfd, Interest::READABLE) {
            Ok(fd) => fd,
            Err(err) => {
                tracing::warn!(session_id = %session_id, error = %err, "failed to create AsyncFd for master pidfd");
                remove_master_monitor_key_if_generation_matches(&session_id, expected_generation);
                return;
            }
        };

        if let Err(err) = async_fd.readable().await {
            tracing::warn!(session_id = %session_id, error = %err, "master pidfd readiness wait failed");
            remove_master_monitor_key_if_generation_matches(&session_id, expected_generation);
            return;
        }

        tracing::info!(session_id = %session_id, expected_pid, expected_generation, "master process exited");

        let ctx = ctx_from_watch_parts(
            db.clone(),
            tmux_server.clone(),
            state_dir.clone(),
            env_state.clone(),
            daemon_unit.clone(),
        );
        if let Err(err) = handle_master_death_detected(
            &ctx,
            session_id.clone(),
            expected_pid,
            expected_generation,
            master_cmd.clone(),
            MasterDeathSource::Pidfd,
        )
        .await
        {
            tracing::warn!(session_id = %session_id, error = %err, "master death handling failed");
        }

        remove_master_monitor_key_if_generation_matches(&session_id, expected_generation);
    });
}

pub fn monitor_key(session_id: &str) -> String {
    format!("master:{session_id}")
}

#[cfg(test)]
async fn revive_master_after_exit(
    session_id: String,
    expected_pid: i64,
    expected_generation: i64,
    db: Db,
    tmux_server: Arc<TmuxServer>,
    master_cmd: String,
    state_dir: PathBuf,
    env_state: EnvState,
    daemon_unit: Option<String>,
) -> Result<(), CcbdError> {
    let spawn_lock = master_spawn_lock(&session_id);
    let _spawn_guard = spawn_lock.lock().await;
    revive_master_after_exit_locked(
        session_id,
        expected_pid,
        expected_generation,
        db,
        tmux_server,
        master_cmd,
        state_dir,
        env_state,
        daemon_unit,
    )
    .await
}

async fn revive_master_after_exit_locked(
    session_id: String,
    expected_pid: i64,
    expected_generation: i64,
    db: Db,
    tmux_server: Arc<TmuxServer>,
    master_cmd: String,
    state_dir: PathBuf,
    env_state: EnvState,
    daemon_unit: Option<String>,
) -> Result<(), CcbdError> {
    let result = revive_master_after_exit_windowed(
        session_id.clone(),
        expected_pid,
        expected_generation,
        db.clone(),
        tmux_server.clone(),
        master_cmd,
        state_dir.clone(),
        env_state.clone(),
        daemon_unit.clone(),
    )
    .await;
    if let Err(err) = &result
        && let Err(mark_err) =
            mark_master_recovery_phase(&db, &session_id, expected_generation, "FAILED", unixepoch())
    {
        tracing::warn!(
            session_id = %session_id,
            expected_generation,
            error = %err,
            mark_error = %mark_err,
            "failed to mark master recovery window FAILED after revive error"
        );
    }
    if result.is_err() {
        let ctx = ctx_from_watch_parts(db, tmux_server, state_dir, env_state, daemon_unit);
        crate::monitor::master_reaper::reap_failed_revive_master(
            &ctx,
            &session_id,
            FailedReviveMasterReapTarget::ClaimedGeneration {
                generation: expected_generation + 1,
            },
        )
        .await;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn revive_master_after_exit_windowed(
    session_id: String,
    expected_pid: i64,
    expected_generation: i64,
    db: Db,
    tmux_server: Arc<TmuxServer>,
    master_cmd: String,
    state_dir: PathBuf,
    env_state: EnvState,
    daemon_unit: Option<String>,
) -> Result<(), CcbdError> {
    let snapshot = snapshot_master_death_session_activity(&db, &session_id)?;
    begin_master_recovery_window_for_snapshot(
        &db,
        &session_id,
        expected_pid,
        expected_generation,
        snapshot.classification,
        unixepoch(),
    )?;
    clean_workers_after_master_death(
        &db,
        &session_id,
        &snapshot.worker_ids_to_reap,
        snapshot.classification,
        &tmux_server,
        &env_state,
    )?;
    if snapshot.classification == MasterDeathSessionActivity::IdleNoWork {
        close_idle_master_death_without_revive(
            &db,
            &session_id,
            expected_pid,
            expected_generation,
        )?;
        return Ok(());
    }
    mark_master_recovery_phase(
        &db,
        &session_id,
        expected_generation,
        "WORKERS_REAPED",
        unixepoch(),
    )?;
    if !wait_for_master_revive_retry_backoff(&db, &session_id, expected_pid, expected_generation)
        .await?
    {
        return Ok(());
    }
    let Some(claimed_generation) =
        claim_master_revive_generation(&db, &session_id, expected_pid, expected_generation)?
    else {
        return Ok(());
    };
    if !record_master_revive_spawn_attempt(
        &db,
        &session_id,
        expected_pid,
        expected_generation,
        claimed_generation,
    )? {
        return Ok(());
    }
    let redispatch_marker_path = write_redispatch_marker_for_master_revive(
        &state_dir,
        &session_id,
        expected_pid,
        expected_generation,
        claimed_generation,
        &snapshot.worker_ids_to_reap,
    );
    let spawned = spawn_replacement_master_pane(
        &db,
        &tmux_server,
        &state_dir,
        &env_state,
        daemon_unit.as_deref(),
        &session_id,
        &master_cmd,
        redispatch_marker_path.as_deref(),
    );
    let spawned = spawned.await?;
    #[cfg(test)]
    master_revive_after_spawn_before_finalize_test_hook(
        &session_id,
        claimed_generation,
        spawned.new_pid,
        &spawned.pane,
    );
    let Some(outcome) = complete_claimed_master_transition(
        &db,
        &session_id,
        claimed_generation,
        spawned.new_pid,
        &spawned.pane.0,
        &spawned.master_sandbox_home,
    )?
    else {
        let readiness_ctx = ctx_from_watch_parts(
            db.clone(),
            tmux_server.clone(),
            state_dir.clone(),
            env_state.clone(),
            daemon_unit.clone(),
        );
        crate::monitor::master_reaper::reap_failed_revive_master(
            &readiness_ctx,
            &session_id,
            FailedReviveMasterReapTarget::SpawnedOrphanAfterFinalizeStale {
                master_pid: spawned.new_pid,
                generation: claimed_generation,
                pane: spawned.pane.clone(),
            },
        )
        .await;
        tracing::warn!(
            session_id = %session_id,
            claimed_generation,
            "master revive finalize went stale after spawn; killed orphan pane"
        );
        mark_master_recovery_phase(&db, &session_id, expected_generation, "FAILED", unixepoch())?;
        return Ok(());
    };
    mark_master_recovery_phase(
        &db,
        &session_id,
        expected_generation,
        "MASTER_RUNNING",
        unixepoch(),
    )?;
    persist_revived_master_cmd(&db, &session_id, &master_cmd)?;
    let readiness_check = register_revived_master_watch_and_prepare_readiness(
        &db,
        &tmux_server,
        &state_dir,
        &env_state,
        daemon_unit.clone(),
        &session_id,
        spawned.new_pid,
        outcome.generation,
        master_cmd,
        &spawned.master_sandbox_home,
        outcome.monitor_key,
    )?;
    inject_master_continue_instruction_best_effort(
        tmux_server.as_ref(),
        &spawned.pane,
        redispatch_marker_path.as_deref(),
        |tmux, pane, text| {
            let tmux = tmux.clone();
            async move {
                tmux.send_keys_literal(pane.clone(), text).await?;
                tmux.send_keys_keysym(pane, "Enter".to_string()).await
            }
        },
    )
    .await?;
    let readiness_ctx = ctx_from_watch_parts(
        db.clone(),
        tmux_server.clone(),
        state_dir.clone(),
        env_state.clone(),
        daemon_unit.clone(),
    );
    resume_master_recovery_readiness(
        &readiness_ctx,
        &session_id,
        spawned.new_pid,
        outcome.generation,
        expected_generation,
        &spawned.pane,
        Some(format!("{session_id}:{}", outcome.generation)),
        readiness_check,
    )
    .await?;
    spawn_master_confirm_timer(db, session_id, spawned.new_pid, outcome.generation);
    Ok(())
}

#[cfg(test)]
type MasterReviveAfterSpawnBeforeFinalizeHook =
    dyn Fn(&str, i64, i64, &TmuxPaneId) + Send + Sync + 'static;

#[cfg(test)]
static MASTER_REVIVE_AFTER_SPAWN_BEFORE_FINALIZE_HOOK: std::sync::LazyLock<
    std::sync::Mutex<Option<std::sync::Arc<MasterReviveAfterSpawnBeforeFinalizeHook>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(test)]
fn master_revive_after_spawn_before_finalize_test_hook(
    session_id: &str,
    claimed_generation: i64,
    new_pid: i64,
    pane: &TmuxPaneId,
) {
    let hook = MASTER_REVIVE_AFTER_SPAWN_BEFORE_FINALIZE_HOOK
        .lock()
        .expect("master revive after-spawn hook mutex poisoned")
        .clone();
    if let Some(hook) = hook {
        hook(session_id, claimed_generation, new_pid, pane);
    }
}

fn clean_workers_after_master_death(
    db: &Db,
    session_id: &str,
    worker_ids_to_reap: &[String],
    classification: MasterDeathSessionActivity,
    tmux_server: &TmuxServer,
    env_state: &EnvState,
) -> Result<(), CcbdError> {
    let daemon_marker = env_state
        .systemd_run_available
        .then(|| tmux_server.socket_name().to_string());
    let cleanup = clean_worker_runtime_resources_sync(
        db,
        session_id,
        worker_ids_to_reap,
        "MASTER_EXIT",
        daemon_marker.as_deref(),
        classification == MasterDeathSessionActivity::ActiveWork,
    )?;
    tracing::warn!(
        session_id = %session_id,
        classification = ?classification,
        workers = worker_ids_to_reap.len(),
        db_killed = cleanup.db_killed_count,
        scope_stop_failures = cleanup.scope_stop_failures,
        pidfd_kill_failures = cleanup.pidfd_kill_failures,
        registry_cleanup_count = cleanup.registry_cleanup_count,
        "master death worker cleanup completed"
    );
    Ok(())
}

fn close_idle_master_death_without_revive(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<(), CcbdError> {
    mark_session_closed_after_idle_master_death(db, session_id)?;
    mark_master_recovery_phase(db, session_id, expected_generation, "FAILED", unixepoch())?;
    tracing::info!(
        session_id = %session_id,
        expected_pid,
        expected_generation,
        "idle master death reaped workers without reviving master"
    );
    Ok(())
}

async fn wait_for_master_revive_retry_backoff(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<bool, CcbdError> {
    let now = unixepoch();
    if let Some(next_retry_at) =
        query_master_revive_next_retry_at(db, session_id, expected_pid, expected_generation)?
    {
        if now < next_retry_at {
            let delay_secs = (next_retry_at - now) as u64;
            tracing::warn!(
                session_id = %session_id,
                expected_pid,
                expected_generation,
                delay_secs,
                "master revive delayed by retry backoff"
            );
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
            if classify_master_death(db, session_id, expected_pid, expected_generation)?
                != MasterDeathDecision::Revive
            {
                tracing::info!(
                    session_id = %session_id,
                    expected_pid,
                    expected_generation,
                    "master revive backoff woke to stale or non-active session"
                );
                mark_master_recovery_phase(
                    db,
                    session_id,
                    expected_generation,
                    "FAILED",
                    unixepoch(),
                )?;
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn claim_master_revive_generation(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
) -> Result<Option<i64>, CcbdError> {
    match try_claim_master_transition(db, session_id, expected_pid, expected_generation)? {
        MasterTransitionOutcome::Claimed => Ok(Some(expected_generation + 1)),
        MasterTransitionOutcome::Stale | MasterTransitionOutcome::NoChange => {
            tracing::info!(
                session_id = %session_id,
                expected_pid,
                expected_generation,
                "master revive skipped because transition claim is stale"
            );
            mark_master_recovery_phase(db, session_id, expected_generation, "FAILED", unixepoch())?;
            Ok(None)
        }
    }
}

fn record_master_revive_spawn_attempt(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    claimed_generation: i64,
) -> Result<bool, CcbdError> {
    match record_master_revive_attempt(
        db,
        session_id,
        expected_pid,
        claimed_generation,
        unixepoch(),
    )? {
        MasterReviveAttemptDecision::Spawn {
            retry_count,
            next_retry_at,
        } => {
            mark_master_recovery_phase(
                db,
                session_id,
                expected_generation,
                "MASTER_SPAWNING",
                unixepoch(),
            )?;
            tracing::warn!(
                session_id = %session_id,
                retry_count,
                next_retry_at,
                "master revive attempt recorded before spawning replacement"
            );
            Ok(true)
        }
        MasterReviveAttemptDecision::Fused => {
            mark_master_recovery_phase(db, session_id, expected_generation, "FUSED", unixepoch())?;
            tracing::error!(
                session_id = %session_id,
                claimed_generation,
                "master revive fuse threshold reached before spawning replacement"
            );
            Ok(false)
        }
        MasterReviveAttemptDecision::Stale => {
            tracing::info!(
                session_id = %session_id,
                claimed_generation,
                "master revive attempt went stale after transition claim"
            );
            mark_master_recovery_phase(db, session_id, expected_generation, "FAILED", unixepoch())?;
            Ok(false)
        }
    }
}

fn write_redispatch_marker_for_master_revive(
    state_dir: &Path,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    claimed_generation: i64,
    worker_ids_to_reap: &[String],
) -> Option<PathBuf> {
    match write_master_revival_redispatch_marker(
        state_dir,
        session_id,
        expected_pid,
        expected_generation,
        claimed_generation,
        worker_ids_to_reap,
    ) {
        Ok(marker_path) => {
            tracing::warn!(
                session_id = %session_id,
                marker = %marker_path.display(),
                workers = worker_ids_to_reap.len(),
                "master revive re-dispatch marker written"
            );
            Some(marker_path)
        }
        Err(err) => {
            tracing::warn!(
                session_id = %session_id,
                error = %err,
                "failed to write master revive re-dispatch marker; continuing revive"
            );
            None
        }
    }
}

async fn resume_master_recovery_readiness(
    ctx: &Ctx,
    session_id: &str,
    expected_pid: i64,
    runtime_expected_generation: i64,
    window_expected_generation: i64,
    pane: &TmuxPaneId,
    begin_readiness_token: Option<String>,
    readiness_check: ReviveMasterReadinessCheck,
) -> Result<(), CcbdError> {
    if let Some(readiness_token) = begin_readiness_token.as_deref() {
        begin_master_recovery_readiness_wait_for_master_watch(
            &ctx.db,
            session_id,
            window_expected_generation,
            readiness_check.mode_name(),
            readiness_token,
            unixepoch(),
        )?;
    }
    let readiness_timeout = match master_recovery_effective_readiness_timeout(
        &ctx.db,
        session_id,
        unixepoch(),
        master_revive_readiness_timeout_secs(session_id),
    )? {
        Some(timeout) if timeout > 0 => Duration::from_secs(timeout as u64),
        _ => {
            fail_readiness_then_cascade_and_reap(
                ctx,
                session_id,
                window_expected_generation,
                readiness_check.deadline_exhausted_reason(),
                "MASTER_REVIVE_READINESS_TIMEOUT",
                expected_pid,
                runtime_expected_generation,
                pane,
            )
            .await?;
            tracing::warn!(
                session_id = %session_id,
                expected_generation = window_expected_generation,
                readiness_mode = readiness_check.mode_name(),
                readiness_strength = readiness_check.strength(),
                "master revive readiness skipped because recovery window has no remaining budget"
            );
            return Ok(());
        }
    };
    if !revive_master_readiness_ready(
        &readiness_check,
        &ctx.db,
        session_id,
        expected_pid,
        runtime_expected_generation,
        readiness_timeout,
    )
    .await?
    {
        fail_readiness_then_cascade_and_reap(
            ctx,
            session_id,
            window_expected_generation,
            readiness_check.timeout_reason(),
            "MASTER_REVIVE_READINESS_TIMEOUT",
            expected_pid,
            runtime_expected_generation,
            pane,
        )
        .await?;
        tracing::warn!(
            session_id = %session_id,
            expected_pid,
            runtime_expected_generation,
            window_expected_generation,
            readiness_mode = readiness_check.mode_name(),
            readiness_strength = readiness_check.strength(),
            "master revive readiness timed out; recovery window will not be completed"
        );
        return Ok(());
    }
    mark_master_recovery_ready_for_master_watch(
        &ctx.db,
        session_id,
        window_expected_generation,
        readiness_check.ready_reason(),
        unixepoch(),
    )?;
    mark_master_recovery_phase(
        &ctx.db,
        session_id,
        window_expected_generation,
        "WORKERS_REPROVISIONING",
        unixepoch(),
    )?;
    let captured_intents = reprovision_declared_workers_after_master_revive(
        session_id,
        ctx.db.clone(),
        ctx.tmux_server.clone(),
        ctx.state_dir.clone(),
        ctx.env_state.clone(),
        ctx.daemon_unit.clone(),
    )
    .await?;
    if !captured_intents.is_empty() {
        tracing::info!(
            session_id = %session_id,
            captured_intents = captured_intents.len(),
            "master revive interrupted jobs handled during atomic worker reprovision"
        );
    } else {
        tracing::info!(
            session_id = %session_id,
            "master revive found no captured interrupted jobs to requeue"
        );
    }
    let recovered_worker_ids = captured_intents
        .iter()
        .map(|intent| intent.agent_id.clone())
        .collect::<Vec<_>>();
    if !recovered_worker_gate_ready(ctx, session_id, &recovered_worker_ids).await? {
        fail_readiness_then_cascade_and_reap(
            ctx,
            session_id,
            window_expected_generation,
            "worker-readiness-timeout",
            "MASTER_REVIVE_WORKER_READINESS_TIMEOUT",
            expected_pid,
            runtime_expected_generation,
            pane,
        )
        .await?;
        tracing::warn!(
            session_id = %session_id,
            recovered_workers = recovered_worker_ids.len(),
            "master revive worker readiness gate timed out; recovery window will not be completed"
        );
        return Ok(());
    }
    complete_master_recovery_window_for_master_watch(
        &ctx.db,
        session_id,
        window_expected_generation,
        unixepoch(),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn fail_readiness_then_cascade_and_reap(
    ctx: &Ctx,
    session_id: &str,
    window_expected_generation: i64,
    window_failure_reason: &str,
    cascade_reason: &str,
    expected_pid: i64,
    runtime_expected_generation: i64,
    pane: &TmuxPaneId,
) -> Result<(), CcbdError> {
    fail_master_recovery_readiness_for_master_watch(
        &ctx.db,
        session_id,
        window_expected_generation,
        window_failure_reason,
        unixepoch(),
    )?;
    cascade_kill_session_agents(
        ctx.db.clone(),
        session_id.to_string(),
        cascade_reason.to_string(),
    )
    .await?;
    crate::monitor::master_reaper::reap_failed_revive_master(
        ctx,
        session_id,
        FailedReviveMasterReapTarget::RuntimeGeneration {
            master_pid: expected_pid,
            generation: runtime_expected_generation,
            pane: pane.clone(),
        },
    )
    .await;
    Ok(())
}

async fn recovered_worker_gate_ready(
    ctx: &Ctx,
    session_id: &str,
    recovered_worker_ids: &[String],
) -> Result<bool, CcbdError> {
    let worker_timeout_secs = master_recovery_effective_readiness_timeout(
        &ctx.db,
        session_id,
        unixepoch(),
        master_revive_readiness_timeout_secs(session_id),
    )?
    .unwrap_or_default()
    .min(30);
    if worker_timeout_secs <= 0 {
        return Ok(false);
    }
    wait_for_recovered_workers_ready(
        &ctx.db,
        recovered_worker_ids,
        Duration::from_secs(worker_timeout_secs as u64),
    )
    .await
}

async fn revive_master_readiness_ready(
    check: &ReviveMasterReadinessCheck,
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    timeout: Duration,
) -> Result<bool, CcbdError> {
    match check {
        ReviveMasterReadinessCheck::Ack {
            provider,
            log_root,
            cursors,
        } => {
            revive_master_readiness_ack(
                db,
                session_id,
                expected_pid,
                expected_generation,
                provider,
                log_root,
                cursors,
                timeout,
            )
            .await
        }
        // Declared degradation: this master cannot report readiness itself, so
        // the only claim ah makes is that the replacement process is alive and
        // still the runtime's master. Nothing is read from the pane.
        ReviveMasterReadinessCheck::Started => {
            #[cfg(test)]
            if let Some(ready) = revive_master_readiness_ack_override(session_id) {
                return Ok(ready);
            }
            Ok(master_runtime_matches(db, session_id, expected_pid, expected_generation)?
                && master_process_is_alive(expected_pid))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn revive_master_readiness_ack(
    db: &Db,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    provider: &str,
    log_root: &Path,
    cursors: &LogCursorMap,
    timeout: Duration,
) -> Result<bool, CcbdError> {
    #[cfg(test)]
    if let Some(ready) = revive_master_readiness_ack_override(session_id) {
        return Ok(ready);
    }

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !master_runtime_matches(db, session_id, expected_pid, expected_generation)? {
            return Ok(false);
        }
        if !master_process_is_alive(expected_pid) {
            return Ok(false);
        }
        if read_provider_assistant_progress_after_cursors(provider, log_root, cursors).map_err(
            |err| {
                CcbdError::PtyIoError(format!(
                    "read {provider} revive readiness transcript: {err}"
                ))
            },
        )? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// Pane-text readiness probing was removed: a settled pane reports that the
// screen stopped changing, not that the revived master resumed work. The only
// readiness signal is the provider's own transcript progress above.

#[cfg(test)]
static REVIVE_MASTER_READINESS_ACK_OVERRIDES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, bool>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
static REVIVE_MASTER_READINESS_TIMEOUT_OVERRIDES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, i64>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

fn master_revive_readiness_timeout_secs(_session_id: &str) -> i64 {
    #[cfg(test)]
    if let Some(timeout_secs) = revive_master_readiness_timeout_override(_session_id) {
        return timeout_secs;
    }
    resolve_master_revive_readiness_timeout_secs()
}

#[cfg(test)]
pub(crate) fn revive_master_readiness_ack_override(session_id: &str) -> Option<bool> {
    REVIVE_MASTER_READINESS_ACK_OVERRIDES
        .lock()
        .expect("revive master readiness ack override mutex poisoned")
        .get(session_id)
        .copied()
}

#[cfg(test)]
fn revive_master_readiness_timeout_override(session_id: &str) -> Option<i64> {
    REVIVE_MASTER_READINESS_TIMEOUT_OVERRIDES
        .lock()
        .expect("revive master readiness timeout override mutex poisoned")
        .get(session_id)
        .copied()
}

#[cfg(test)]
struct ReviveMasterReadinessAckOverrideGuard {
    session_id: String,
}

#[cfg(test)]
struct ReviveMasterReadinessTimeoutOverrideGuard {
    session_id: String,
}

#[cfg(test)]
impl Drop for ReviveMasterReadinessAckOverrideGuard {
    fn drop(&mut self) {
        REVIVE_MASTER_READINESS_ACK_OVERRIDES
            .lock()
            .expect("revive master readiness ack override mutex poisoned")
            .remove(&self.session_id);
    }
}

#[cfg(test)]
impl Drop for ReviveMasterReadinessTimeoutOverrideGuard {
    fn drop(&mut self) {
        REVIVE_MASTER_READINESS_TIMEOUT_OVERRIDES
            .lock()
            .expect("revive master readiness timeout override mutex poisoned")
            .remove(&self.session_id);
    }
}

#[cfg(test)]
fn set_revive_master_readiness_ack_override(
    session_id: &str,
    ready: bool,
) -> ReviveMasterReadinessAckOverrideGuard {
    REVIVE_MASTER_READINESS_ACK_OVERRIDES
        .lock()
        .expect("revive master readiness ack override mutex poisoned")
        .insert(session_id.to_string(), ready);
    ReviveMasterReadinessAckOverrideGuard {
        session_id: session_id.to_string(),
    }
}

#[cfg(test)]
fn set_revive_master_readiness_timeout_override(
    session_id: &str,
    timeout_secs: i64,
) -> ReviveMasterReadinessTimeoutOverrideGuard {
    REVIVE_MASTER_READINESS_TIMEOUT_OVERRIDES
        .lock()
        .expect("revive master readiness timeout override mutex poisoned")
        .insert(session_id.to_string(), timeout_secs);
    ReviveMasterReadinessTimeoutOverrideGuard {
        session_id: session_id.to_string(),
    }
}

async fn wait_for_recovered_workers_ready(
    db: &Db,
    agent_ids: &[String],
    timeout: Duration,
) -> Result<bool, CcbdError> {
    if agent_ids.is_empty() {
        return Ok(true);
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if recovered_workers_ready_sync(db, agent_ids)? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn unixepoch() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{
        begin_master_recovery_window_for_snapshot,
        complete_master_recovery_window_for_master_watch, mark_master_recovery_non_revive_terminal,
        mark_master_recovery_phase, patrol_active_masters_once,
        rearm_active_master_watches_on_startup, recovered_workers_ready_sync,
        revive_master_after_exit, spawn_master_pidfd_watch_task,
    };
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::master_recovery::{
        AnchorCascadeDecision, begin_master_recovery_readiness_wait_sync,
        begin_master_recovery_window_sync, complete_master_recovery_window_sync,
        decide_anchor_cascade_sync, fail_master_recovery_readiness_sync,
        mark_master_recovery_ready_sync, update_master_recovery_phase_sync,
    };
    use crate::db::recovery::{
        AgentSpawnSpec, persist_agent_spawn_spec_sync, query_agent_recovery_intent_sync,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::db::system::{
        MasterDeathSessionActivity, cascade_kill_session_agents_sync,
        clean_worker_runtime_resources_sync, snapshot_master_death_session_activity,
    };
    use crate::error::CcbdError;
    use crate::monitor::master_reaper::{
        FailedReviveMasterReapEvent, FailedReviveMasterReapTarget,
        MASTER_REVIVE_CONTINUE_INSTRUCTION, ReviveMasterReadinessCheck,
        build_master_revive_command, inject_master_continue_instruction_best_effort,
        master_revival_redispatch_marker_path, prepare_revive_master_readiness_check,
        reap_failed_revive_master, set_failed_revive_master_reap_recorder,
        write_master_revival_redispatch_marker,
    };
    use crate::monitor::{contains, pidfd_open, register};
    use crate::rpc::Ctx;
    use crate::rpc::handlers::handle_agent_spawn;
    use crate::sandbox::EnvState;
    use crate::tmux::{TmuxPaneId, TmuxServer, master_session_name};
    use rusqlite::OptionalExtension;
    use serde_json::json;
    use std::ffi::OsString;
    use std::fs;
    use std::process::Command;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    static GLOBAL_ENV_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    struct EnvVarGuard {
        key: &'static str,
        old_value: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let old_value = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old_value }
        }
    }

    struct MasterReviveAfterSpawnBeforeFinalizeHookGuard;

    impl MasterReviveAfterSpawnBeforeFinalizeHookGuard {
        fn set(hook: impl Fn(&str, i64, i64, &TmuxPaneId) + Send + Sync + 'static) -> Self {
            *super::MASTER_REVIVE_AFTER_SPAWN_BEFORE_FINALIZE_HOOK
                .lock()
                .expect("master revive after-spawn hook mutex poisoned") = Some(Arc::new(hook));
            Self
        }
    }

    impl Drop for MasterReviveAfterSpawnBeforeFinalizeHookGuard {
        fn drop(&mut self) {
            *super::MASTER_REVIVE_AFTER_SPAWN_BEFORE_FINALIZE_HOOK
                .lock()
                .expect("master revive after-spawn hook mutex poisoned") = None;
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(old_value) = self.old_value.take() {
                    std::env::set_var(self.key, old_value);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    async fn sleep_ms(ms: u64) {
        tokio::task::spawn_blocking(move || std::thread::sleep(Duration::from_millis(ms)))
            .await
            .unwrap();
    }

    async fn wait_until_monitor_absent(key: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if !contains(key) {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    async fn wait_until_path_exists(path: &std::path::Path) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if path.exists() {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    async fn wait_for_session_status(db: &db::Db, session_id: &str, expected: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(25);
        while Instant::now() < deadline {
            let status = db
                .conn()
                .query_row(
                    "SELECT status FROM sessions WHERE id = ?1",
                    [session_id],
                    |row| row.get::<_, String>(0),
                )
                .ok();
            if status.as_deref() == Some(expected) {
                return true;
            }
            sleep_ms(100).await;
        }
        false
    }

    async fn wait_for_agent_state(db: &db::Db, agent_id: &str, expected: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let state = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = ?1",
                    [agent_id],
                    |row| row.get::<_, String>(0),
                )
                .ok();
            if state.as_deref() == Some(expected) {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    async fn wait_for_agent_state_any(db: &db::Db, agent_id: &str, expected: &[&str]) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let state = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = ?1",
                    [agent_id],
                    |row| row.get::<_, String>(0),
                )
                .ok();
            if state
                .as_deref()
                .is_some_and(|state| expected.contains(&state))
            {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    async fn run_orchestrator_until_agent_busy(ctx: &Ctx, db: &db::Db, agent_id: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if crate::orchestrator::run_once(ctx).await.unwrap_or(false)
                && query_agent_state(db, agent_id) == "BUSY"
            {
                return true;
            }
            if query_agent_state(db, agent_id) == "BUSY" {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    async fn wait_for_job_status(db: &db::Db, job_id: &str, expected: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let status = db
                .conn()
                .query_row("SELECT status FROM jobs WHERE id = ?1", [job_id], |row| {
                    row.get::<_, String>(0)
                })
                .ok();
            if status.as_deref() == Some(expected) {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    fn cleanup_test_tmux_server(tmux: &TmuxServer) {
        let _ = Command::new("tmux")
            .args(["-L", tmux.socket_name(), "kill-server"])
            .output();
    }

    fn query_agent_state(db: &db::Db, agent_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    }

    fn query_session_status_sync(db: &db::Db, session_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT status FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    }

    fn query_master_recovery_phase(db: &db::Db, session_id: &str) -> Option<String> {
        db.conn()
            .query_row(
                "SELECT phase FROM master_recovery_windows WHERE session_id = ?1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap()
    }

    fn seed_lifecycle_session(
        db: &db::Db,
        session_id: &str,
        agent_id: &str,
        job_id: Option<&str>,
        agent_state: &str,
    ) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            session_id,
            "p_lifecycle",
            "/tmp/master-revive-lifecycle",
        )
        .unwrap();
        conn.execute(
            "UPDATE sessions
             SET status = 'ACTIVE', master_pid = 111, master_generation = 5
             WHERE id = ?1",
            [session_id],
        )
        .unwrap();
        insert_agent_sync(&conn, agent_id, session_id, "claude", agent_state, Some(10)).unwrap();
        if let Some(job_id) = job_id {
            insert_job_sync(
                &conn,
                job_id,
                agent_id,
                Some("req-lifecycle"),
                "continue work",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = ?2 WHERE id = ?1",
                rusqlite::params![job_id, 900_i64],
            )
            .unwrap();
        }
    }

    fn write_claude_assistant_progress_after_cursor(
        log_root: &std::path::Path,
    ) -> crate::completion::reader::LogCursorMap {
        let file = log_root.join("project/session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        let stale = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"STALE"}]}}"#;
        fs::write(&file, format!("{stale}\n")).unwrap();
        let cursors =
            crate::completion::reader::collect_provider_log_cursors("claude", log_root).unwrap();
        let progress = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"STARTED"}]}}"#;
        fs::write(&file, format!("{stale}\n{progress}\n")).unwrap();
        cursors
    }

    fn query_master_recovery_count(db: &db::Db, session_id: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM master_recovery_windows WHERE session_id = ?1",
                [session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    }

    #[test]
    fn master_recovery_master_watch_updates_window_phases() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_phase", "p_phase", "/tmp/phase").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = 's_phase'",
                [],
            )
            .unwrap();
        }

        begin_master_recovery_window_for_snapshot(
            &db,
            "s_phase",
            111,
            5,
            MasterDeathSessionActivity::ActiveWork,
            1_000,
        )
        .unwrap();
        assert_eq!(
            query_master_recovery_phase(&db, "s_phase").as_deref(),
            Some("DETECTED")
        );
        mark_master_recovery_phase(&db, "s_phase", 5, "WORKERS_REAPED", 1_001).unwrap();
        assert_eq!(
            query_master_recovery_phase(&db, "s_phase").as_deref(),
            Some("WORKERS_REAPED")
        );
        mark_master_recovery_phase(&db, "s_phase", 5, "MASTER_SPAWNING", 1_002).unwrap();
        assert_eq!(
            query_master_recovery_phase(&db, "s_phase").as_deref(),
            Some("MASTER_SPAWNING")
        );
        mark_master_recovery_phase(&db, "s_phase", 5, "MASTER_RUNNING", 1_003).unwrap();
        assert_eq!(
            query_master_recovery_phase(&db, "s_phase").as_deref(),
            Some("MASTER_RUNNING")
        );
        mark_master_recovery_phase(&db, "s_phase", 5, "WORKERS_REPROVISIONING", 1_004).unwrap();
        assert_eq!(
            query_master_recovery_phase(&db, "s_phase").as_deref(),
            Some("WORKERS_REPROVISIONING")
        );
        complete_master_recovery_window_for_master_watch(&db, "s_phase", 5, 1_005).unwrap();
        assert_eq!(
            query_master_recovery_phase(&db, "s_phase").as_deref(),
            Some("COMPLETED")
        );
    }

    #[test]
    fn master_recovery_master_watch_failure_marks_window_failed() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_failed", "p_failed", "/tmp/failed").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = 's_failed'",
                [],
            )
            .unwrap();
        }
        begin_master_recovery_window_for_snapshot(
            &db,
            "s_failed",
            111,
            5,
            MasterDeathSessionActivity::ActiveWork,
            1_000,
        )
        .unwrap();

        mark_master_recovery_phase(&db, "s_failed", 5, "FAILED", 1_001).unwrap();

        assert_eq!(
            query_master_recovery_phase(&db, "s_failed").as_deref(),
            Some("FAILED")
        );
    }

    #[test]
    fn master_recovery_cutover_inflight_does_not_create_recovery_window() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_cutover", "p_cutover", "/tmp/cutover").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = 's_cutover'",
                [],
            )
            .unwrap();
        }

        mark_master_recovery_non_revive_terminal(&db, "s_cutover", 5, 1_000).unwrap();

        assert_eq!(query_master_recovery_count(&db, "s_cutover"), 0);
    }

    #[test]
    fn master_recovery_idle_worker_master_death_reap_only_terminalizes_window() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_idle_window", "p_idle", "/tmp/idle").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = 's_idle_window'",
                [],
            )
            .unwrap();
        }
        begin_master_recovery_window_for_snapshot(
            &db,
            "s_idle_window",
            111,
            5,
            MasterDeathSessionActivity::IdleNoWork,
            1_000,
        )
        .unwrap();

        mark_master_recovery_phase(&db, "s_idle_window", 5, "FAILED", 1_001).unwrap();

        assert_eq!(
            query_master_recovery_phase(&db, "s_idle_window").as_deref(),
            Some("FAILED")
        );
    }

    #[test]
    fn master_recovery_fuse_marks_window_fused() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_fused", "p_fused", "/tmp/fused").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = 's_fused'",
                [],
            )
            .unwrap();
        }
        begin_master_recovery_window_for_snapshot(
            &db,
            "s_fused",
            111,
            5,
            MasterDeathSessionActivity::ActiveWork,
            1_000,
        )
        .unwrap();

        mark_master_recovery_phase(&db, "s_fused", 5, "FUSED", 1_001).unwrap();

        assert_eq!(
            query_master_recovery_phase(&db, "s_fused").as_deref(),
            Some("FUSED")
        );
    }

    #[test]
    fn revive_worker_spawning_timeout_blocks_completed() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_worker_gate", "p_worker_gate", "/tmp/worker-gate")
                .unwrap();
            insert_agent_sync(
                &conn,
                "a_worker_gate",
                "s_worker_gate",
                "bash",
                "SPAWNING",
                Some(12),
            )
            .unwrap();
        }

        assert!(
            !recovered_workers_ready_sync(&db, &["a_worker_gate".to_string()]).unwrap(),
            "SPAWNING recovered worker must block COMPLETED"
        );
        db.conn()
            .execute(
                "UPDATE agents SET state = 'IDLE' WHERE id = 'a_worker_gate'",
                [],
            )
            .unwrap();
        assert!(recovered_workers_ready_sync(&db, &["a_worker_gate".to_string()]).unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revive_readiness_timeout_does_not_complete() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            project_dir.path().join("ah.toml"),
            format!(
                r#"version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[agents.a1]
provider = "bash"
"#,
                shared_credentials_dir.path().display()
            ),
        )
        .unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_readiness_timeout_{}", uuid::Uuid::new_v4());
        let _timeout = super::set_revive_master_readiness_timeout_override(&session_id, 10);
        let agent_id = format!("ag_readiness_timeout_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_readiness_timeout_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
        }

        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let _probe = super::set_revive_master_readiness_ack_override(&session_id, false);
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            "i=0; while true; do i=$((i+1)); printf '\\033[2J\\033[H%s' \"$i\"; sleep 0.1; done"
                .to_string(),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert_ne!(
            query_master_recovery_phase(&db, &session_id).as_deref(),
            Some("COMPLETED"),
            "readiness timeout must never mark the recovery window COMPLETED"
        );
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revive_readiness_success_completes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            project_dir.path().join("ah.toml"),
            format!(
                r#"version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[agents.a1]
provider = "bash"
"#,
                shared_credentials_dir.path().display()
            ),
        )
        .unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_readiness_success_{}", uuid::Uuid::new_v4());
        let _timeout = super::set_revive_master_readiness_timeout_override(&session_id, 10);
        let agent_id = format!("ag_readiness_success_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_readiness_success_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
        }

        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let _probe = super::set_revive_master_readiness_ack_override(&session_id, true);
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            "printf ready; sleep 30".to_string(),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            query_master_recovery_phase(&db, &session_id).as_deref(),
            Some("COMPLETED")
        );
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revive_ack_ready_completes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            project_dir.path().join("ah.toml"),
            format!(
                r#"version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[agents.a1]
provider = "bash"
"#,
                shared_credentials_dir.path().display()
            ),
        )
        .unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_ack_ready_{}", uuid::Uuid::new_v4());
        let _timeout = super::set_revive_master_readiness_timeout_override(&session_id, 10);
        let agent_id = format!("ag_ack_ready_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_ack_ready_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
        }

        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let _ack = super::set_revive_master_readiness_ack_override(&session_id, true);
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            "printf ready; sleep 30".to_string(),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        let (phase, mode, reason): (String, String, String) = db
            .conn()
            .query_row(
                "SELECT phase, readiness_mode, ready_reason
                 FROM master_recovery_windows WHERE session_id = ?1",
                [&session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(phase, "COMPLETED");
        assert_eq!(mode, "ack");
        assert_eq!(reason, "ack-assistant-progress");
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revive_ack_timeout_without_semantic_signal_does_not_complete() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_ack_timeout_{}", uuid::Uuid::new_v4());
        let _timeout = super::set_revive_master_readiness_timeout_override(&session_id, 10);
        let agent_id = format!("ag_ack_timeout_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_ack_timeout_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
        }

        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let _ack = super::set_revive_master_readiness_ack_override(&session_id, false);
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            "printf '继续'; sleep 30".to_string(),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        let (phase, mode, reason): (String, String, String) = db
            .conn()
            .query_row(
                "SELECT phase, readiness_mode, ready_reason
                 FROM master_recovery_windows WHERE session_id = ?1",
                [&session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(phase, "FAILED");
        assert_eq!(mode, "ack");
        assert_eq!(reason, "ack-timeout");
        assert!(
            wait_for_agent_state(&db, &agent_id, "KILLED").await,
            "ack timeout should cascade instead of completing from pane echo"
        );
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        cleanup_test_tmux_server(&tmux);
    }

    #[test]
    fn revive_readiness_never_accepts_injected_pane_echo() {
        // The revive-continue instruction echoes back into the pane verbatim.
        // Readiness reads transcript semantics, so that echo is not progress.
        assert!(
            !crate::completion::parser::provider_log_line_has_assistant_progress(
                "claude",
                super::super::master_reaper::MASTER_REVIVE_CONTINUE_INSTRUCTION
            ),
            "readiness ack uses transcript semantics, not pane text"
        );
    }

    #[test]
    fn master_revive_lifecycle_happy_claude_ack_completes_without_zombie() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let log_root = tempfile::TempDir::new().unwrap();
        seed_lifecycle_session(
            &db,
            "s_lifecycle_happy",
            "a_lifecycle_happy",
            Some("j_lifecycle_happy"),
            "BUSY",
        );
        let now = 1_000;

        let snapshot = snapshot_master_death_session_activity(&db, "s_lifecycle_happy").unwrap();
        assert_eq!(
            snapshot.classification,
            MasterDeathSessionActivity::ActiveWork
        );
        let window = {
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_lifecycle_happy", 111, 5, true, now, 90)
                .unwrap()
        };
        assert_eq!(window.phase, "DETECTED");
        clean_worker_runtime_resources_sync(
            &db,
            "s_lifecycle_happy",
            &snapshot.worker_ids_to_reap,
            "MASTER_DEATH_REVIVE",
            None,
            true,
        )
        .unwrap();
        assert!(
            query_agent_recovery_intent_sync(&db.conn(), "a_lifecycle_happy")
                .unwrap()
                .is_some(),
            "active worker cleanup should capture recovery intent before reprovision"
        );
        {
            let conn = db.conn();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_happy",
                5,
                "WORKERS_REAPED",
                now + 1,
            )
            .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_happy",
                5,
                "MASTER_SPAWNING",
                now + 2,
            )
            .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_happy",
                5,
                "MASTER_RUNNING",
                now + 3,
            )
            .unwrap();
            begin_master_recovery_readiness_wait_sync(
                &conn,
                "s_lifecycle_happy",
                5,
                "ack",
                "s_lifecycle_happy:5",
                now + 4,
            )
            .unwrap();
        }

        let cursors = write_claude_assistant_progress_after_cursor(log_root.path());
        assert!(
            crate::completion::reader::read_provider_assistant_progress_after_cursors(
                "claude",
                log_root.path(),
                &cursors,
            )
            .unwrap(),
            "Claude transcript assistant progress is the semantic revive ACK"
        );
        let (phase, mode, ready_reason, ready_at, job_status) = {
            let conn = db.conn();
            mark_master_recovery_ready_sync(
                &conn,
                "s_lifecycle_happy",
                5,
                "ack-assistant-progress",
                now + 5,
            )
            .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_happy",
                5,
                "WORKERS_REPROVISIONING",
                now + 6,
            )
            .unwrap();
            conn.execute(
                "UPDATE agents SET state = 'IDLE', pid = 20 WHERE id = 'a_lifecycle_happy'",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs
                 SET status = 'QUEUED', error_reason = 'RECOVERY_REQUEUED:0'
                 WHERE id = 'j_lifecycle_happy'",
                [],
            )
            .unwrap();
            complete_master_recovery_window_sync(&conn, "s_lifecycle_happy", 5, now + 7).unwrap();

            let (phase, mode, ready_reason, ready_at) = conn
                .query_row(
                    "SELECT phase, readiness_mode, ready_reason, ready_at
                     FROM master_recovery_windows WHERE session_id = 's_lifecycle_happy'",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                        ))
                    },
                )
                .unwrap();
            let job = query_job_sync(&conn, "j_lifecycle_happy").unwrap().unwrap();
            (phase, mode, ready_reason, ready_at, job.status)
        };
        assert_eq!(phase, "COMPLETED");
        assert_eq!(mode, "ack");
        assert_eq!(ready_reason, "ack-assistant-progress");
        assert_eq!(ready_at, Some(now + 5));
        assert_eq!(
            query_session_status_sync(&db, "s_lifecycle_happy"),
            "ACTIVE"
        );
        assert_eq!(query_agent_state(&db, "a_lifecycle_happy"), "IDLE");
        assert_eq!(job_status, "QUEUED");
    }

    #[test]
    fn master_revive_lifecycle_hung_claude_ack_timeout_fails_and_cascades() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let log_root = tempfile::TempDir::new().unwrap();
        seed_lifecycle_session(
            &db,
            "s_lifecycle_hung",
            "a_lifecycle_hung",
            Some("j_lifecycle_hung"),
            "BUSY",
        );
        let now = 2_000;
        let snapshot = snapshot_master_death_session_activity(&db, "s_lifecycle_hung").unwrap();
        assert_eq!(
            snapshot.classification,
            MasterDeathSessionActivity::ActiveWork
        );
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_lifecycle_hung", 111, 5, true, now, 90)
                .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_hung",
                5,
                "WORKERS_REAPED",
                now + 1,
            )
            .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_hung",
                5,
                "MASTER_SPAWNING",
                now + 2,
            )
            .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_hung",
                5,
                "MASTER_RUNNING",
                now + 3,
            )
            .unwrap();
            begin_master_recovery_readiness_wait_sync(
                &conn,
                "s_lifecycle_hung",
                5,
                "ack",
                "s_lifecycle_hung:5",
                now + 4,
            )
            .unwrap();
        }

        let echo_file = log_root.path().join("project/session.jsonl");
        fs::create_dir_all(echo_file.parent().unwrap()).unwrap();
        fs::write(&echo_file, "继续\n").unwrap();
        let cursors =
            crate::completion::reader::collect_provider_log_cursors("claude", log_root.path())
                .unwrap();
        assert!(
            !crate::completion::reader::read_provider_assistant_progress_after_cursors(
                "claude",
                log_root.path(),
                &cursors,
            )
            .unwrap(),
            "pane echo / wizard text must not satisfy semantic ACK"
        );
        {
            let conn = db.conn();
            fail_master_recovery_readiness_sync(
                &conn,
                "s_lifecycle_hung",
                5,
                "ack-timeout",
                now + 35,
            )
            .unwrap();
        }
        cascade_kill_session_agents_sync(
            &db,
            "s_lifecycle_hung",
            "MASTER_REVIVE_READINESS_TIMEOUT",
        )
        .unwrap();

        let (phase, ready_reason, ready_at): (String, String, Option<i64>) = db
            .conn()
            .query_row(
                "SELECT phase, ready_reason, ready_at
                 FROM master_recovery_windows WHERE session_id = 's_lifecycle_hung'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(phase, "FAILED");
        assert_eq!(ready_reason, "ack-timeout");
        assert_eq!(ready_at, None);
        assert_eq!(query_session_status_sync(&db, "s_lifecycle_hung"), "KILLED");
        assert_eq!(query_agent_state(&db, "a_lifecycle_hung"), "KILLED");
    }

    #[test]
    fn master_revive_lifecycle_idle_master_death_reaps_without_active_defer_window() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_lifecycle_session(&db, "s_lifecycle_idle", "a_lifecycle_idle", None, "IDLE");
        let now = 3_000;

        let snapshot = snapshot_master_death_session_activity(&db, "s_lifecycle_idle").unwrap();
        assert_eq!(
            snapshot.classification,
            MasterDeathSessionActivity::IdleNoWork
        );
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, "s_lifecycle_idle", 111, 5, false, now, 90)
                .unwrap();
        }
        mark_master_recovery_non_revive_terminal(&db, "s_lifecycle_idle", 5, now + 1).unwrap();
        cascade_kill_session_agents_sync(&db, "s_lifecycle_idle", "IDLE_MASTER_EXIT").unwrap();

        assert_eq!(
            query_master_recovery_phase(&db, "s_lifecycle_idle").as_deref(),
            Some("FAILED")
        );
        assert_eq!(query_session_status_sync(&db, "s_lifecycle_idle"), "KILLED");
        assert_eq!(query_agent_state(&db, "a_lifecycle_idle"), "KILLED");
    }

    #[test]
    fn master_revive_lifecycle_anchor_decision_expired_window_cascades_now() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_lifecycle_session(
            &db,
            "s_lifecycle_deadline",
            "a_lifecycle_deadline",
            Some("j_lifecycle_deadline"),
            "BUSY",
        );
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(
                &conn,
                "s_lifecycle_deadline",
                111,
                5,
                true,
                4_000,
                10,
            )
            .unwrap();
        }

        let decision =
            decide_anchor_cascade_sync(&db.conn(), "s_lifecycle_deadline", 4_011).unwrap();

        assert_eq!(
            decision,
            AnchorCascadeDecision::CascadeNow {
                reason: "MASTER_REVIVE_WINDOW_EXPIRED"
            }
        );
        assert_eq!(
            query_master_recovery_phase(&db, "s_lifecycle_deadline").as_deref(),
            Some("FAILED")
        );
    }

    #[test]
    fn master_revive_lifecycle_startup_reconcile_verifying_window_policy_is_deterministic() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_lifecycle_session(
            &db,
            "s_lifecycle_startup",
            "a_lifecycle_startup",
            Some("j_lifecycle_startup"),
            "BUSY",
        );
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(
                &conn,
                "s_lifecycle_startup",
                111,
                5,
                true,
                5_000,
                90,
            )
            .unwrap();
            update_master_recovery_phase_sync(
                &conn,
                "s_lifecycle_startup",
                5,
                "MASTER_VERIFYING",
                5_001,
            )
            .unwrap();
        }

        let active_decision =
            decide_anchor_cascade_sync(&db.conn(), "s_lifecycle_startup", 5_050).unwrap();
        assert_eq!(
            active_decision,
            AnchorCascadeDecision::Defer {
                until: 5_091,
                phase: "MASTER_VERIFYING".to_string()
            },
            "unexpired verifying windows are preserved for startup rearm/readiness resume"
        );
        let expired_decision =
            decide_anchor_cascade_sync(&db.conn(), "s_lifecycle_startup", 5_092).unwrap();
        assert_eq!(
            expired_decision,
            AnchorCascadeDecision::CascadeNow {
                reason: "MASTER_REVIVE_WINDOW_EXPIRED"
            }
        );
        assert_eq!(
            query_master_recovery_phase(&db, "s_lifecycle_startup").as_deref(),
            Some("FAILED")
        );
    }

    #[test]
    fn master_revive_without_readiness_ack_degrades_to_started_not_pane_text() {
        let temp = tempfile::TempDir::new().unwrap();

        let check =
            prepare_revive_master_readiness_check("s_started_mode", "bash -lc true", temp.path())
                .unwrap();

        assert_eq!(check.mode_name(), "started");
        assert_eq!(check.strength(), "degraded");
        assert_eq!(check.ready_reason(), "started-process-alive");
    }

    #[test]
    fn master_revive_with_readiness_ack_provider_uses_transcript_ack() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".codex/sessions")).unwrap();

        let check = prepare_revive_master_readiness_check("s_ack_mode", "codex", temp.path())
            .unwrap();

        assert_eq!(check.mode_name(), "ack");
        assert_eq!(check.strength(), "semantic");
        match check {
            super::ReviveMasterReadinessCheck::Ack {
                provider, log_root, ..
            } => {
                assert_eq!(provider, "codex");
                assert_eq!(log_root, temp.path().join(".codex/sessions"));
            }
            other => panic!("expected transcript ack readiness, got {other:?}"),
        }
    }

    fn seed_failed_revive_master_runtime(
        db: &db::Db,
        session_id: &str,
        agent_id: &str,
        master_pid: i64,
        generation: i64,
        pane_id: &str,
    ) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            session_id,
            &format!("p_{session_id}"),
            "/tmp/failed-revive-master",
        )
        .unwrap();
        conn.execute(
            "UPDATE sessions
             SET status = 'ACTIVE', master_pid = ?2, master_generation = ?3,
                 master_pane_id = ?4
             WHERE id = ?1",
            rusqlite::params![session_id, master_pid, generation, pane_id],
        )
        .unwrap();
        insert_agent_sync(&conn, agent_id, session_id, "claude", "BUSY", Some(10)).unwrap();
    }

    fn failed_revive_reap_events(
        events: &Arc<Mutex<Vec<FailedReviveMasterReapEvent>>>,
    ) -> Vec<FailedReviveMasterReapEvent> {
        events
            .lock()
            .expect("failed revive master reap event mutex poisoned")
            .clone()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revive_failure_reaps_orphan_gen2_master() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = "s_failed_reap";
        let agent_id = "a_failed_reap";
        let pane = TmuxPaneId::parse("%77").unwrap();
        seed_failed_revive_master_runtime(&db, session_id, agent_id, 222, 6, &pane.0);
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, session_id, 111, 5, true, 1_000, 90).unwrap();
            begin_master_recovery_readiness_wait_sync(
                &conn,
                session_id,
                5,
                "ack",
                "s_failed_reap:6",
                1_001,
            )
            .unwrap();
            fail_master_recovery_readiness_sync(&conn, session_id, 5, "ack-timeout", 1_030)
                .unwrap();
        }
        cascade_kill_session_agents_sync(&db, session_id, "MASTER_REVIVE_READINESS_TIMEOUT")
            .unwrap();
        let ctx = test_ctx(db.clone(), state_dir.path());
        let (events, _guard) = set_failed_revive_master_reap_recorder(session_id);

        reap_failed_revive_master(
            &ctx,
            session_id,
            FailedReviveMasterReapTarget::RuntimeGeneration {
                master_pid: 222,
                generation: 6,
                pane,
            },
        )
        .await;

        let events = failed_revive_reap_events(&events);
        assert!(events.contains(&FailedReviveMasterReapEvent::Sigkill {
            pid: 222,
            generation: 6
        }));
        assert!(events.contains(&FailedReviveMasterReapEvent::PaneKill {
            pane: "%77".to_string(),
            generation: 6
        }));
        assert!(events.contains(&FailedReviveMasterReapEvent::WatchRemoved {
            generation: 6,
            removed: false
        }));
        assert_eq!(
            query_master_recovery_phase(&db, session_id).as_deref(),
            Some("FAILED")
        );
        assert_eq!(query_session_status_sync(&db, session_id), "KILLED");
        assert_eq!(query_agent_state(&db, agent_id), "KILLED");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revive_failure_master_reap_is_generation_fenced() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = "s_failed_reap_stale";
        let pane = TmuxPaneId::parse("%78").unwrap();
        seed_failed_revive_master_runtime(&db, session_id, "a_failed_reap_stale", 333, 7, "%new");
        let ctx = test_ctx(db.clone(), state_dir.path());
        let (events, _guard) = set_failed_revive_master_reap_recorder(session_id);

        reap_failed_revive_master(
            &ctx,
            session_id,
            FailedReviveMasterReapTarget::RuntimeGeneration {
                master_pid: 222,
                generation: 6,
                pane,
            },
        )
        .await;

        assert!(
            failed_revive_reap_events(&events).is_empty(),
            "stale failed-revive reap must not kill a newer master generation"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revive_error_reaps_claimed_gen2_master() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = "s_failed_reap_error";
        seed_failed_revive_master_runtime(&db, session_id, "a_failed_reap_error", 444, 6, "%79");
        let ctx = test_ctx(db.clone(), state_dir.path());
        let (events, _guard) = set_failed_revive_master_reap_recorder(session_id);

        reap_failed_revive_master(
            &ctx,
            session_id,
            FailedReviveMasterReapTarget::ClaimedGeneration { generation: 6 },
        )
        .await;

        let events = failed_revive_reap_events(&events);
        assert!(events.contains(&FailedReviveMasterReapEvent::Sigkill {
            pid: 444,
            generation: 6
        }));
        assert!(events.contains(&FailedReviveMasterReapEvent::PaneKill {
            pane: "%79".to_string(),
            generation: 6
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revive_finalize_stale_after_spawn_reaps_spawned_orphan_master() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_finalize_stale_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_finalize_stale_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_finalize_stale_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(
                &conn,
                "job_finalize_stale_active",
                &agent_id,
                None,
                "in flight",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs
                 SET status = 'DISPATCHED', dispatched_at = unixepoch()
                 WHERE id = 'job_finalize_stale_active'",
                [],
            )
            .unwrap();
        }

        let hook_seen = Arc::new(Mutex::new(None::<(i64, String)>));
        let hook_seen_for_hook = hook_seen.clone();
        let hook_db = db.clone();
        let hook_session_id = session_id.clone();
        let _hook_guard = MasterReviveAfterSpawnBeforeFinalizeHookGuard::set(
            move |session_id, claimed_generation, new_pid, pane| {
                if session_id != hook_session_id {
                    return;
                }
                hook_db
                    .conn()
                    .execute(
                        "UPDATE sessions
                         SET master_generation = ?2
                         WHERE id = ?1 AND master_generation = ?3",
                        rusqlite::params![session_id, claimed_generation + 1, claimed_generation],
                    )
                    .unwrap();
                *hook_seen_for_hook
                    .lock()
                    .expect("finalize stale hook seen mutex poisoned") =
                    Some((new_pid, pane.0.clone()));
            },
        );
        let (events, _recorder_guard) = set_failed_revive_master_reap_recorder(&session_id);
        let tmux = Arc::new(TmuxServer::new(&state_dir));

        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            "sleep 60".to_string(),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        let (spawned_pid, spawned_pane) = hook_seen
            .lock()
            .expect("finalize stale hook seen mutex poisoned")
            .clone()
            .expect("test hook must observe the spawned replacement before finalize");
        let events = failed_revive_reap_events(&events);
        assert!(events.contains(&FailedReviveMasterReapEvent::Sigkill {
            pid: spawned_pid,
            generation: 6
        }));
        assert!(events.contains(&FailedReviveMasterReapEvent::WatchRemoved {
            generation: 6,
            removed: false
        }));
        assert!(events.contains(&FailedReviveMasterReapEvent::PaneKill {
            pane: spawned_pane,
            generation: 6
        }));
        assert_eq!(
            query_master_recovery_phase(&db, &session_id).as_deref(),
            Some("FAILED")
        );

        db.conn()
            .execute(
                "UPDATE sessions SET status = 'KILLED' WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_readiness_timeout_reaps_failed_revive_master() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = "s_failed_reap_worker_gate";
        let pane = TmuxPaneId::parse("%80").unwrap();
        seed_failed_revive_master_runtime(
            &db,
            session_id,
            "a_failed_reap_worker_gate",
            555,
            6,
            &pane.0,
        );
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, session_id, 111, 5, true, 1_000, 90).unwrap();
            fail_master_recovery_readiness_sync(
                &conn,
                session_id,
                5,
                "worker-readiness-timeout",
                1_040,
            )
            .unwrap();
        }
        cascade_kill_session_agents_sync(&db, session_id, "MASTER_REVIVE_WORKER_READINESS_TIMEOUT")
            .unwrap();
        let ctx = test_ctx(db.clone(), state_dir.path());
        let (events, _guard) = set_failed_revive_master_reap_recorder(session_id);

        reap_failed_revive_master(
            &ctx,
            session_id,
            FailedReviveMasterReapTarget::RuntimeGeneration {
                master_pid: 555,
                generation: 6,
                pane,
            },
        )
        .await;

        let events = failed_revive_reap_events(&events);
        assert!(events.contains(&FailedReviveMasterReapEvent::Sigkill {
            pid: 555,
            generation: 6
        }));
        assert!(events.contains(&FailedReviveMasterReapEvent::PaneKill {
            pane: "%80".to_string(),
            generation: 6
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn happy_revive_readiness_path_does_not_reap_master() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = "s_happy_no_reap";
        let pane = TmuxPaneId::parse("%81").unwrap();
        seed_failed_revive_master_runtime(&db, session_id, "a_happy_no_reap", 666, 6, &pane.0);
        let now = super::unixepoch();
        {
            let conn = db.conn();
            begin_master_recovery_window_sync(&conn, session_id, 111, 5, true, now, 90).unwrap();
        }
        let ctx = test_ctx(db.clone(), state_dir.path());
        let (events, _guard) = set_failed_revive_master_reap_recorder(session_id);
        let _probe = super::set_revive_master_readiness_ack_override(session_id, true);

        super::resume_master_recovery_readiness(
            &ctx,
            session_id,
            666,
            6,
            5,
            &pane,
            Some("s_happy_no_reap:6".to_string()),
            ReviveMasterReadinessCheck::Ack {
                provider: "claude".to_string(),
                log_root: state_dir.path().join("transcripts"),
                cursors: crate::completion::reader::LogCursorMap::new(),
            },
        )
        .await
        .unwrap();

        assert!(
            failed_revive_reap_events(&events).is_empty(),
            "successful revive readiness must not reap the healthy gen-2 master"
        );
        assert_eq!(
            query_master_recovery_phase(&db, session_id).as_deref(),
            Some("COMPLETED")
        );
    }

    fn test_ctx(db: db::Db, state_dir: &std::path::Path) -> Ctx {
        Ctx {
            db,
            state_dir: state_dir.to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(state_dir)),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        }
    }

    async fn spawn_test_master_pane(
        tmux: &TmuxServer,
        project_id: &str,
        cwd: &std::path::Path,
        command: &str,
    ) -> (TmuxPaneId, i64) {
        let session = master_session_name(project_id);
        tmux.ensure_session(session.clone(), cwd.to_path_buf())
            .await
            .unwrap();
        let pane = tmux
            .spawn_window(
                session,
                format!("master_{}", uuid::Uuid::new_v4().simple()),
                cwd.to_path_buf(),
                vec!["sh".into(), "-lc".into(), command.into()],
            )
            .await
            .unwrap();
        let pid = tmux.get_pane_pid(pane.clone()).await.unwrap();
        (pane, i64::from(pid))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_rearm_active_master_registers_watch_and_later_exit_routes_existing_path() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let session_id = format!("sess_rearm_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_rearm_{}", uuid::Uuid::new_v4());
        let (pane, pid) = spawn_test_master_pane(
            &ctx.tmux_server,
            &project_id,
            project_dir.path(),
            "sleep 60",
        )
        .await;

        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = ?2, master_generation = 7,
                     master_pane_id = ?3, master_cmd = 'sleep 60'
                 WHERE id = ?1",
                rusqlite::params![&session_id, pid, pane.0],
            )
            .unwrap();
        }

        let armed = rearm_active_master_watches_on_startup(&ctx).await.unwrap();
        let key = crate::master_revival::master_monitor_key(&session_id, 7);
        assert_eq!(armed, 1);
        assert!(contains(&key));
        ctx.tmux_server.kill_pane(pane).await.unwrap();
        assert!(wait_until_monitor_absent(&key).await);
        let _ = ctx
            .tmux_server
            .kill_session(master_session_name(&project_id))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_rearm_dead_master_immediately_routes_existing_path() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let session_id = format!("sess_rearm_dead_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_rearm_dead_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_rearm_dead_{}", uuid::Uuid::new_v4());
        let spawn_marker = state_dir.join("startup-rearm-dead-master-spawned");

        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 999999999, master_generation = 3,
                     master_pane_id = '%missing', master_cmd = ?2
                 WHERE id = ?1",
                rusqlite::params![
                    &session_id,
                    format!("touch {}; sleep 5", spawn_marker.display())
                ],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(42)).unwrap();
            insert_job_sync(&conn, "job_rearm_dead", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_rearm_dead'",
                [],
            )
            .unwrap();
        }

        let detected = rearm_active_master_watches_on_startup(&ctx).await.unwrap();
        assert_eq!(detected, 1);
        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        assert!(wait_until_path_exists(&spawn_marker).await);
        let _ = ctx
            .tmux_server
            .kill_session(master_session_name(&project_id))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_rearm_resumes_readiness_for_alive_verifying_window() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let session_id = format!("sess_rearm_verify_{}", uuid::Uuid::new_v4());
        let _timeout = super::set_revive_master_readiness_timeout_override(&session_id, 10);
        let agent_id = format!("ag_rearm_verify_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_rearm_verify_{}", uuid::Uuid::new_v4());
        let (pane, pid) = spawn_test_master_pane(
            &ctx.tmux_server,
            &project_id,
            project_dir.path(),
            "printf ready; sleep 30",
        )
        .await;
        let _probe = super::set_revive_master_readiness_ack_override(&session_id, true);

        {
            let now = super::unixepoch();
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = ?2, master_generation = 6,
                     master_pane_id = ?3, master_cmd = 'printf ready; sleep 30'
                 WHERE id = ?1",
                rusqlite::params![&session_id, pid, pane.0],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            begin_master_recovery_window_sync(&conn, &session_id, 111, 5, true, now, 300).unwrap();
            begin_master_recovery_readiness_wait_sync(
                &conn,
                &session_id,
                5,
                "probe",
                "startup-resume-token",
                now + 1,
            )
            .unwrap();
        }

        let armed = rearm_active_master_watches_on_startup(&ctx).await.unwrap();

        assert_eq!(armed, 1);
        assert_eq!(
            query_master_recovery_phase(&db, &session_id).as_deref(),
            Some("COMPLETED")
        );
        let _ = ctx
            .tmux_server
            .kill_session(master_session_name(&project_id))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_rearm_hung_verifying_window_fails_then_cascades() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let session_id = format!("sess_rearm_hung_verify_{}", uuid::Uuid::new_v4());
        let _timeout = super::set_revive_master_readiness_timeout_override(&session_id, 10);
        let agent_id = format!("ag_rearm_hung_verify_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_rearm_hung_verify_{}", uuid::Uuid::new_v4());
        let (pane, pid) = spawn_test_master_pane(
            &ctx.tmux_server,
            &project_id,
            project_dir.path(),
            "i=0; while true; do i=$((i+1)); printf '\\033[2J\\033[H%s' \"$i\"; sleep 0.1; done",
        )
        .await;
        let _probe = super::set_revive_master_readiness_ack_override(&session_id, false);

        {
            let now = super::unixepoch();
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = ?2, master_generation = 6,
                     master_pane_id = ?3, master_cmd = 'hung'
                 WHERE id = ?1",
                rusqlite::params![&session_id, pid, pane.0],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            begin_master_recovery_window_sync(&conn, &session_id, 111, 5, true, now, 300).unwrap();
            begin_master_recovery_readiness_wait_sync(
                &conn,
                &session_id,
                5,
                "probe",
                "startup-hung-resume-token",
                now + 1,
            )
            .unwrap();
        }

        let armed = rearm_active_master_watches_on_startup(&ctx).await.unwrap();

        assert_eq!(armed, 1);
        assert_eq!(
            query_master_recovery_phase(&db, &session_id).as_deref(),
            Some("FAILED")
        );
        assert!(wait_for_session_status(&db, &session_id, "KILLED").await);
        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        let _ = ctx
            .tmux_server
            .kill_session(master_session_name(&project_id))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_rearm_skips_terminal_or_non_verifying_window() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let mut cases = Vec::new();

        for (suffix, phase) in [("running", "MASTER_RUNNING"), ("completed", "COMPLETED")] {
            let session_id = format!("sess_rearm_skip_{suffix}_{}", uuid::Uuid::new_v4());
            let agent_id = format!("ag_rearm_skip_{suffix}_{}", uuid::Uuid::new_v4());
            let project_id = format!("p_rearm_skip_{suffix}_{}", uuid::Uuid::new_v4());
            let case_project_dir = project_dir.path().join(suffix);
            std::fs::create_dir_all(&case_project_dir).unwrap();
            let (pane, pid) = spawn_test_master_pane(
                &ctx.tmux_server,
                &project_id,
                &case_project_dir,
                "printf ready; sleep 30",
            )
            .await;

            {
                let now = super::unixepoch();
                let conn = db.conn();
                insert_session_sync(
                    &conn,
                    &session_id,
                    &project_id,
                    case_project_dir.to_str().unwrap(),
                )
                .unwrap();
                conn.execute(
                    "UPDATE sessions
                     SET status = 'ACTIVE', master_pid = ?2, master_generation = 6,
                         master_pane_id = ?3, master_cmd = 'printf ready; sleep 30'
                     WHERE id = ?1",
                    rusqlite::params![&session_id, pid, pane.0],
                )
                .unwrap();
                insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
                begin_master_recovery_window_sync(&conn, &session_id, 111, 5, true, now, 300)
                    .unwrap();
                if phase == "COMPLETED" {
                    complete_master_recovery_window_sync(&conn, &session_id, 5, now + 1).unwrap();
                } else {
                    update_master_recovery_phase_sync(&conn, &session_id, 5, phase, now + 1)
                        .unwrap();
                }
            }
            cases.push((session_id, agent_id, project_id, phase.to_string()));
        }

        let armed = rearm_active_master_watches_on_startup(&ctx).await.unwrap();

        assert_eq!(armed, cases.len());
        for (session_id, agent_id, project_id, phase) in cases {
            assert_eq!(query_master_recovery_phase(&db, &session_id), Some(phase));
            assert_eq!(query_agent_state(&db, &agent_id), "BUSY");
            let _ = ctx
                .tmux_server
                .kill_session(master_session_name(&project_id))
                .await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn patrol_detects_dead_active_master_when_monitor_missing() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let session_id = format!("sess_patrol_dead_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_patrol_dead_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_patrol_dead_{}", uuid::Uuid::new_v4());
        let spawn_marker = state_dir.join("patrol-dead-master-spawned");

        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 999999999, master_generation = 4,
                     master_pane_id = '%missing', master_cmd = ?2
                 WHERE id = ?1",
                rusqlite::params![
                    &session_id,
                    format!("touch {}; sleep 5", spawn_marker.display())
                ],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(43)).unwrap();
            insert_job_sync(&conn, "job_patrol_dead", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_patrol_dead'",
                [],
            )
            .unwrap();
        }

        let detected = patrol_active_masters_once(&ctx).await.unwrap();
        assert_eq!(detected, 1);
        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        assert!(wait_until_path_exists(&spawn_marker).await);
        let _ = ctx
            .tmux_server
            .kill_session(master_session_name(&project_id))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pidfd_and_patrol_double_fire_only_handles_once() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let ctx = test_ctx(db.clone(), &state_dir);
        let session_id = format!("sess_double_fire_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_double_fire_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_double_fire_{}", uuid::Uuid::new_v4());
        let spawn_marker = state_dir.join("double-fire-master-spawned");

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.2; exit 0")
            .spawn()
            .unwrap();
        let pid = i64::from(child.id());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = ?2, master_generation = 2,
                     master_cmd = ?3
                 WHERE id = ?1",
                rusqlite::params![
                    &session_id,
                    pid,
                    format!("touch {}; sleep 5", spawn_marker.display())
                ],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(44)).unwrap();
            insert_job_sync(&conn, "job_double_fire", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_double_fire'",
                [],
            )
            .unwrap();
        }

        let pidfd = pidfd_open(child.id() as i32).unwrap();
        let task_fd = pidfd.try_clone().unwrap();
        let key = crate::master_revival::master_monitor_key(&session_id, 2);
        register(key.clone(), pidfd);
        spawn_master_pidfd_watch_task(
            session_id.clone(),
            pid,
            2,
            task_fd,
            db.clone(),
            ctx.tmux_server.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            ctx.env_state.clone(),
            None,
        );

        assert!(wait_until_monitor_absent(&key).await);
        let _ = child.wait();
        let detected = patrol_active_masters_once(&ctx).await.unwrap();
        assert!(detected <= 1);
        assert!(wait_until_path_exists(&spawn_marker).await);
        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        let generation: i64 = db
            .conn()
            .query_row(
                "SELECT master_generation FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(generation, 3);
        let retry_count: i64 = db
            .conn()
            .query_row(
                "SELECT master_retry_count FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retry_count, 1);
        let _ = ctx
            .tmux_server
            .kill_session(master_session_name(&project_id))
            .await;
    }

    #[serial_test::serial(global_env)]
    #[tokio::test(flavor = "multi_thread")]
    async fn master_revive_stale_inflight_dispatch_failure_does_not_overwrite_requeued_job() {
        let _global_env_guard = GLOBAL_ENV_TEST_LOCK.lock().await;
        let enter_delay = EnvVarGuard::set("CCB_TMUX_ENTER_DELAY", "2.0");
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_inflight_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_inflight_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_inflight_{}", uuid::Uuid::new_v4());
        let job_id = "job_stale_inflight_master_revive";
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let ctx = Ctx {
            db: db.clone(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux.clone(),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        };
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
        }

        handle_agent_spawn(
            json!({
                "session_id": session_id,
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();
        assert!(
            wait_for_agent_state(&db, &agent_id, "IDLE").await,
            "initial worker must become dispatchable"
        );
        insert_job_sync(
            &db.conn(),
            job_id,
            &agent_id,
            Some("req-stale-inflight-master-revive"),
            "sleep 30\n",
        )
        .unwrap();

        let dispatch_ctx = ctx.clone();
        let dispatch_task =
            tokio::spawn(async move { crate::orchestrator::run_once(&dispatch_ctx).await });
        assert!(
            wait_for_job_status(&db, job_id, "DISPATCHED").await,
            "first dispatch should claim the job before master death"
        );

        let old_pane = crate::agent_io::pane_id(&agent_id)
            .expect("initial worker pane must be registered")
            .0;
        let old_pid: i64 = db
            .conn()
            .query_row("SELECT pid FROM agents WHERE id = ?1", [&agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        let spawn_marker = state_dir.join("master-inflight-spawned");
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();
        assert!(wait_until_path_exists(&spawn_marker).await);
        assert!(
            wait_for_agent_state(&db, &agent_id, "IDLE").await,
            "reprovisioned worker must become ready before redispatch"
        );
        let new_pane = crate::agent_io::pane_id(&agent_id)
            .expect("reprovisioned worker pane must be registered")
            .0;
        let new_pid: i64 = db
            .conn()
            .query_row("SELECT pid FROM agents WHERE id = ?1", [&agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_ne!(
            old_pid, new_pid,
            "test must exercise a real worker process replacement"
        );
        tracing::debug!(
            old_pane,
            new_pane,
            old_pid,
            new_pid,
            "stale in-flight reprovisioned worker"
        );

        dispatch_task.await.unwrap().unwrap();
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(
            job.status, "QUEUED",
            "stale in-flight dispatch failure must not overwrite the recovery-requeued job: {:?}",
            job.error_reason
        );
        assert_eq!(job.error_reason.as_deref(), Some("RECOVERY_REQUEUED:0"));

        drop(enter_delay);
        // Drive orchestrator ticks until the requeued job is redispatched. run_once()
        // can legitimately return false on a tick where the reprovisioned worker is
        // not IDLE-ready yet, but the requeued job row must remain continuously visible.
        for _ in 0..50 {
            let _ = crate::orchestrator::run_once(&ctx).await.unwrap();
            let job = query_job_sync(&db.conn(), job_id)
                .unwrap()
                .expect("requeued job row must remain visible during redispatch polling");
            if job.status == "DISPATCHED" {
                break;
            }
            sleep_ms(100).await;
        }
        assert!(
            wait_for_agent_state_any(&db, &agent_id, &["WAITING_FOR_ACK", "BUSY"]).await,
            "requeued job must be redispatched to the reprovisioned worker; state={}, job={:?}",
            query_agent_state(&db, &agent_id),
            query_job_sync(&db.conn(), job_id).unwrap()
        );
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "DISPATCHED");
        assert!(
            job.error_reason.is_none(),
            "successful redispatch should clear recovery marker"
        );

        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        let _ = tmux
            .kill_session(crate::tmux::agent_session_name(&agent_id))
            .await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn active_work_master_death_reaps_worker_revives_master_and_requires_redispatch_marker() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_batch_f_active_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_batch_f_active_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_batch_f_active_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(&conn, "job_batch_f_active", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_batch_f_active'",
                [],
            )
            .unwrap();
        }

        let spawn_marker = state_dir.join("batch-f-active-master-spawned");
        let redispatch_marker = master_revival_redispatch_marker_path(&state_dir, &session_id, 6);
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        assert!(wait_until_path_exists(&spawn_marker).await);
        let marker_body = std::fs::read_to_string(&redispatch_marker).unwrap_or_else(|err| {
            panic!(
                "expected redispatch marker at {}: {err}",
                redispatch_marker.display()
            )
        });
        assert!(marker_body.contains(&session_id));
        assert!(marker_body.contains(&agent_id));
        assert!(marker_body.contains("\"revived_generation\":6"));
        assert!(marker_body.contains("\"redispatch_required\":true"));
        assert!(marker_body.contains("re-dispatch"));
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn active_work_master_revival_reprovisions_declared_killed_worker() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_gap1b_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_gap1b_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_gap1b_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            persist_agent_spawn_spec_sync(
                &conn,
                &AgentSpawnSpec {
                    agent_id: agent_id.clone(),
                    provider: "bash".to_string(),
                    env: std::collections::HashMap::from([(
                        "REVIVE_WORKER_ENV".to_string(),
                        "1".to_string(),
                    )]),
                    hooks: std::collections::HashMap::new(),
                    plugins: Vec::new(),
                    skills: Vec::new(),
                    bundle: Vec::new(),
                    settings: serde_json::Map::new(),
                    bundle_digest: None,
                    sandbox_overrides: Default::default(),
                    hook_push_enabled: false,
                },
                "hash-gap1b",
            )
            .unwrap();
            insert_job_sync(&conn, "job_gap1b", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_gap1b'",
                [],
            )
            .unwrap();
        }

        let spawn_marker = state_dir.join("gap1b-master-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_until_path_exists(&spawn_marker).await);
        assert!(
            wait_for_agent_state(&db, &agent_id, "IDLE").await,
            "declared worker should be re-provisioned after master revive instead of remaining KILLED"
        );
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        let _ = tmux
            .kill_session(crate::tmux::agent_session_name(&agent_id))
            .await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn idle_master_death_reaps_without_revive() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_batch_f_idle_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_batch_f_idle_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_batch_f_idle_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "IDLE", Some(10)).unwrap();
        }

        let spawn_marker = state_dir.join("batch-f-idle-master-spawned");
        let redispatch_marker = master_revival_redispatch_marker_path(&state_dir, &session_id, 6);
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}", spawn_marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        assert!(
            !spawn_marker.exists(),
            "IdleNoWork master death must reap workers without reviving master"
        );
        assert!(
            !redispatch_marker.exists(),
            "IdleNoWork master death must not create a re-dispatch marker"
        );
        assert!(wait_for_session_status(&db, &session_id, "CLOSED").await);
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revived_master_env_contains_state_dir_socket_and_redispatch_marker() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            project_dir.path().join("ah.toml"),
            format!(
                r#"version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[agents.a1]
provider = "bash"
"#,
                shared_credentials_dir.path().display()
            ),
        )
        .unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_batch_g_env_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_batch_g_env_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_batch_g_env_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(&conn, "job_batch_g_env", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_batch_g_env'",
                [],
            )
            .unwrap();
        }

        let master_sandbox_dir =
            crate::sandbox::path::resolve_sandbox_dir(&state_dir, &session_id, "master").unwrap();
        let expected_home =
            crate::provider::home_layout::sandbox_home_for_sandbox_dir(&master_sandbox_dir)
                .unwrap();
        let env_capture = state_dir.join("batch-g-revived-master-env");
        let redispatch_marker = master_revival_redispatch_marker_path(&state_dir, &session_id, 6);
        let expected_socket = state_dir.join("ahd.sock");
        let claude_script = state_dir.join("claude");
        std::fs::write(
            &claude_script,
            format!(
                "#!/bin/sh\nprintf '%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n%s\\n' \"$AH_STATE_DIR\" \"$CCB_SOCKET\" \"$AH_MASTER_ROLE\" \"$AH_REDISPATCH_MARKER\" \"$HOME\" \"$CLAUDE_CONFIG_DIR\" \"$CLAUDE_SECURESTORAGE_CONFIG_DIR\" \"${{CLAUDE_CODE_USE_GATEWAY:-}}\" \"${{ANTHROPIC_AUTH_TOKEN:-}}\" \"$AH_ROLE\" \"$AH_SESSION_ID\" \"${{AH_AGENT_ID:-}}\" > {}\nsleep 5\n",
                env_capture.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&claude_script).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
        std::fs::set_permissions(&claude_script, permissions).unwrap();
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            claude_script.display().to_string(),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: false,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        assert!(wait_until_path_exists(&env_capture).await);
        let env_lines = std::fs::read_to_string(&env_capture)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_eq!(env_lines[0], state_dir.display().to_string());
        assert_eq!(env_lines[1], expected_socket.display().to_string());
        assert_eq!(env_lines[2], "managed");
        assert_eq!(env_lines[3], redispatch_marker.display().to_string());
        assert_eq!(env_lines[4], expected_home.display().to_string());
        assert_eq!(
            env_lines[5],
            expected_home.join(".claude").display().to_string(),
            "revived master CLAUDE_CONFIG_DIR must point at its sandbox .claude"
        );
        assert_eq!(
            env_lines[6],
            shared_credentials_dir.path().display().to_string(),
            "revived master CLAUDE_SECURESTORAGE_CONFIG_DIR must point at shared credentials dir"
        );
        assert_eq!(env_lines[7], "");
        assert_eq!(env_lines[8], "");
        assert_eq!(env_lines[9], "master");
        assert_eq!(env_lines[10], session_id);
        assert_eq!(env_lines[11], "");
        assert!(redispatch_marker.exists());
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[test]
    fn master_revive_marker_requires_literal_continue_instruction() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let session_id = format!("sess_continue_marker_{}", uuid::Uuid::new_v4());

        let marker = write_master_revival_redispatch_marker(
            state_dir.path(),
            &session_id,
            111,
            5,
            6,
            &["ag_continue".to_string()],
        )
        .unwrap();

        let body = std::fs::read_to_string(marker).unwrap();
        assert!(
            body.contains("\"continue_required\":true"),
            "revived master marker must tell ahd/UI that literal continue injection is required"
        );
        assert!(
            body.contains(&format!(
                "\"continue_instruction\":\"{}\"",
                MASTER_REVIVE_CONTINUE_INSTRUCTION
            )),
            "revived master marker must carry the exact continue instruction"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn master_revive_continue_injection_sends_literal_continue() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let tmux = TmuxServer::new(state_dir.path());
        let pane = TmuxPaneId("%continue:1.0".to_string());
        let expected_pane_id = pane.0.clone();
        let observed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

        let writer_observed = observed.clone();
        inject_master_continue_instruction_best_effort(
            &tmux,
            &pane,
            None,
            move |_, actual_pane, text| async move {
                assert_eq!(actual_pane.0, expected_pane_id);
                writer_observed.lock().unwrap().push(text);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(
            observed.lock().unwrap().clone(),
            vec![MASTER_REVIVE_CONTINUE_INSTRUCTION.to_string()]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn master_revive_continue_injection_failure_keeps_marker_and_does_not_block() {
        let state_dir = tempfile::TempDir::new().unwrap();
        let tmux = TmuxServer::new(state_dir.path());
        let pane = TmuxPaneId("%continue-fail:1.0".to_string());
        let marker = state_dir.path().join("redispatch-marker.json");
        std::fs::write(&marker, "{}").unwrap();

        let result = inject_master_continue_instruction_best_effort(
            &tmux,
            &pane,
            Some(&marker),
            |_, _, text| async move {
                assert_eq!(text, MASTER_REVIVE_CONTINUE_INSTRUCTION);
                Err(CcbdError::PtyIoError("synthetic tmux write failure".into()))
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "continue injection is best-effort and must not block master revive"
        );
        assert!(
            marker.exists(),
            "failed continue injection must leave redispatch marker for later recovery"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn master_revive_worker_reprovision_requeues_captured_interrupted_job() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_interrupted_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_interrupted_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_interrupted_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(7)).unwrap();
            persist_agent_spawn_spec_sync(
                &conn,
                &AgentSpawnSpec {
                    agent_id: agent_id.clone(),
                    provider: "bash".to_string(),
                    env: std::collections::HashMap::from([(
                        "MASTER_REVIVE_REQUEUE_ENV".to_string(),
                        "1".to_string(),
                    )]),
                    hooks: std::collections::HashMap::new(),
                    plugins: Vec::new(),
                    skills: Vec::new(),
                    bundle: Vec::new(),
                    settings: serde_json::Map::new(),
                    bundle_digest: None,
                    sandbox_overrides: Default::default(),
                    hook_push_enabled: false,
                },
                "hash-master-requeue",
            )
            .unwrap();
            insert_job_sync(
                &conn,
                "job_interrupted_master_revive",
                &agent_id,
                Some("req-master-revive"),
                "resume interrupted worker task",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs
                 SET status = 'DISPATCHED',
                     dispatched_at = unixepoch(),
                     cancel_requested = 0,
                     requires_physical_evidence = 1,
                     requires_test_evidence = 1
                 WHERE id = 'job_interrupted_master_revive'",
                [],
            )
            .unwrap();
        }

        let spawn_marker = state_dir.join("master-requeue-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_until_path_exists(&spawn_marker).await);
        assert!(wait_for_session_status(&db, &session_id, "ACTIVE").await);
        assert!(
            wait_for_agent_state(&db, &agent_id, "IDLE").await,
            "full master revive should reprovision worker before requeue"
        );
        let (status, prompt_text): (String, String) = db
            .conn()
            .query_row(
                "SELECT status, prompt_text FROM jobs WHERE id = 'job_interrupted_master_revive'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "QUEUED");
        assert_eq!(prompt_text, "resume interrupted worker task");
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        let _ = tmux
            .kill_session(crate::tmux::agent_session_name(&agent_id))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn master_revive_recovered_job_waits_for_realign_readiness_before_dispatch() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();

        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_readiness_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_readiness_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_readiness_{}", uuid::Uuid::new_v4());
        let job_id = "job_readiness_gate_master_revive";
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(7)).unwrap();
            persist_agent_spawn_spec_sync(
                &conn,
                &AgentSpawnSpec {
                    agent_id: agent_id.clone(),
                    provider: "bash".to_string(),
                    env: std::collections::HashMap::new(),
                    hooks: std::collections::HashMap::new(),
                    plugins: Vec::new(),
                    skills: Vec::new(),
                    bundle: Vec::new(),
                    settings: serde_json::Map::new(),
                    bundle_digest: None,
                    sandbox_overrides: Default::default(),
                    hook_push_enabled: false,
                },
                "hash-master-readiness",
            )
            .unwrap();
            insert_job_sync(
                &conn,
                job_id,
                &agent_id,
                Some("req-master-readiness"),
                "sleep 5\n",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs
                 SET status = 'DISPATCHED',
                     dispatched_at = unixepoch()
                 WHERE id = ?1",
                [job_id],
            )
            .unwrap();
        }

        let spawn_marker = state_dir.join("master-readiness-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let _probe = super::set_revive_master_readiness_ack_override(&session_id, true);
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_until_path_exists(&spawn_marker).await);
        let state_after_realign = query_agent_state(&db, &agent_id);
        assert!(
            matches!(state_after_realign.as_str(), "IDLE" | "BUSY"),
            "revive now waits for recovered worker readiness before returning; got {state_after_realign}"
        );
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "QUEUED");
        assert_eq!(job.error_reason.as_deref(), Some("RECOVERY_REQUEUED:0"));

        assert!(
            wait_for_agent_state(&db, &agent_id, "IDLE").await,
            "init-probe readiness should eventually publish recovered worker as IDLE"
        );
        let ctx = Ctx {
            db: db.clone(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux.clone(),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        };
        assert!(crate::orchestrator::run_once(&ctx).await.unwrap());
        let state_after_dispatch = query_agent_state(&db, &agent_id);
        let job_after_dispatch = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert!(
            wait_for_agent_state(&db, &agent_id, "BUSY").await,
            "recovered job must be delivered after readiness and enter BUSY; state_after_dispatch={state_after_dispatch}, job_status={}, job_error_reason={:?}",
            job_after_dispatch.status,
            job_after_dispatch.error_reason
        );
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "DISPATCHED");
        assert!(job.error_reason.is_none());

        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        let _ = tmux
            .kill_session(crate::tmux::agent_session_name(&agent_id))
            .await;
        cleanup_test_tmux_server(&tmux);
    }

    #[serial_test::serial(global_env)]
    #[tokio::test(flavor = "multi_thread")]
    async fn master_revive_recovered_job_survives_stale_pane_dispatch_and_retries_new_pane() {
        let _global_env_guard = GLOBAL_ENV_TEST_LOCK.lock().await;
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_stale_pane_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_stale_pane_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_stale_pane_{}", uuid::Uuid::new_v4());
        let job_id = "job_stale_pane_master_revive";
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(7)).unwrap();
            persist_agent_spawn_spec_sync(
                &conn,
                &AgentSpawnSpec {
                    agent_id: agent_id.clone(),
                    provider: "bash".to_string(),
                    env: std::collections::HashMap::new(),
                    hooks: std::collections::HashMap::new(),
                    plugins: Vec::new(),
                    skills: Vec::new(),
                    bundle: Vec::new(),
                    settings: serde_json::Map::new(),
                    bundle_digest: None,
                    sandbox_overrides: Default::default(),
                    hook_push_enabled: false,
                },
                "hash-master-stale-pane",
            )
            .unwrap();
            insert_job_sync(
                &conn,
                job_id,
                &agent_id,
                Some("req-master-stale-pane"),
                "sleep 5\n",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs
                 SET status = 'DISPATCHED',
                     dispatched_at = unixepoch()
                 WHERE id = ?1",
                [job_id],
            )
            .unwrap();
        }

        let spawn_marker = state_dir.join("master-stale-pane-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let _probe = super::set_revive_master_readiness_ack_override(&session_id, true);
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", spawn_marker.display()),
            state_dir.clone(),
            EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(wait_until_path_exists(&spawn_marker).await);
        assert!(
            wait_for_agent_state(&db, &agent_id, "IDLE").await,
            "worker must be reprovisioned before recovered job dispatch"
        );
        let new_pane = crate::agent_io::pane_id(&agent_id)
            .expect("reprovision should register the new worker pane");
        assert_ne!(new_pane.0, "%999999");

        let stale_entry = crate::agent_io::AgentIoEntry {
            session_id: session_id.clone(),
            pane_id: TmuxPaneId("%999999".to_string()),
            expected_pid: None,
            reader_handle: tokio::spawn(async {}),
            fifo_path: state_dir.join("pipes").join("stale-pane-test.fifo"),
            socket_name: tmux.socket_name().to_string(),
            idle_scan_enabled: Arc::new(AtomicBool::new(false)),
        };
        crate::agent_io::register(agent_id.clone(), stale_entry);
        assert_eq!(
            crate::agent_io::pane_id(&agent_id).map(|pane| pane.0),
            Some("%999999".to_string()),
            "test pins a stale runtime pane binding before dispatch"
        );

        let ctx = Ctx {
            db: db.clone(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux.clone(),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        };
        assert!(
            run_orchestrator_until_agent_busy(&ctx, &db, &agent_id).await,
            "recovered job must be delivered to the live pane and enter BUSY"
        );
        assert_eq!(
            crate::agent_io::pane_id(&agent_id).map(|pane| pane.0),
            Some(new_pane.0.clone()),
            "dispatch must refresh the stale runtime pane binding before sending"
        );
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "DISPATCHED");
        assert!(job.error_reason.is_none());

        let _ = tmux.kill_session(master_session_name(&project_id)).await;
        let _ = tmux
            .kill_session(crate::tmux::agent_session_name(&agent_id))
            .await;
        cleanup_test_tmux_server(&tmux);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "manual true-scope dogfood harness; requires a real ah pane and external master kill"]
    async fn dogfood_master_revive_redispatch_marker_true_scope() {
        // Manual Batch F acceptance:
        // 1. Start/attach a green ah-managed master pane.
        // 2. From that pane, dispatch a real worker task that runs for more than 10 seconds.
        // 3. From an external shell, kill -9 the master pid.
        // 4. Confirm ahd reaps the old worker, revives master, and the revived master observes the
        //    recovery marker before re-dispatching the lost work to successful completion.
        // 5. Repeat with an idle master: kill -9 should reap workers but not revive master.
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_master_watch_revives_active_session_on_master_exit() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_master_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_mw_{}", uuid::Uuid::new_v4());

        {
            let conn = db.conn();
            insert_session_sync(&conn, &session_id, "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", None).unwrap();
            insert_job_sync(
                &conn,
                "job_master_watch_active",
                &agent_id,
                None,
                "in flight",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_master_watch_active'",
                [],
            )
            .unwrap();
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.2; exit 0")
            .spawn()
            .unwrap();
        let pidfd = pidfd_open(child.id() as i32).unwrap();
        let task_fd = pidfd.try_clone().unwrap();
        let key = crate::master_revival::master_monitor_key(&session_id, 0);
        register(key.clone(), pidfd);

        let tmux = Arc::new(TmuxServer::new(&state_dir));
        spawn_master_pidfd_watch_task(
            session_id.clone(),
            0,
            0,
            task_fd,
            db.clone(),
            tmux,
            "/bin/sleep 5".to_string(),
            state_dir,
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        );

        assert!(wait_until_monitor_absent(&key).await);
        let _ = child.wait();

        let session_status: String = db
            .conn()
            .query_row(
                "SELECT status FROM sessions WHERE id = ?",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(session_status, "ACTIVE");

        let agent_state: String = db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                [&agent_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(agent_state, "KILLED");
        assert!(!contains(&key));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_master_revive_production_lock_blocks_spawn_and_stale_cas_skips_spawn() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_lock_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_lock_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_lock_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(&conn, "job_lock_active", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_lock_active'",
                [],
            )
            .unwrap();
        }

        let marker = state_dir.join("master-lock-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let lock = crate::master_revival::master_spawn_lock(&session_id);
        let guard = lock.lock().await;
        let task = tokio::spawn(revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        ));

        sleep_ms(150).await;
        assert!(
            !marker.exists(),
            "revive_master_after_exit must acquire master_spawn_lock before spawning"
        );
        let generation_before_unlock: i64 = db
            .conn()
            .query_row(
                "SELECT master_generation FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(generation_before_unlock, 5);

        drop(guard);
        task.await.unwrap().unwrap();
        assert!(wait_until_path_exists(&marker).await);
        let generation_after_spawn: i64 = db
            .conn()
            .query_row(
                "SELECT master_generation FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(generation_after_spawn, 6);

        let stale_marker = state_dir.join("master-stale-spawned");
        {
            let conn = db.conn();
            let stale_agent_id = format!("ag_lock_stale_{}", uuid::Uuid::new_v4());
            insert_agent_sync(
                &conn,
                &stale_agent_id,
                &session_id,
                "bash",
                "BUSY",
                Some(11),
            )
            .unwrap();
            insert_job_sync(
                &conn,
                "job_lock_stale_active",
                &stale_agent_id,
                None,
                "in flight",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_lock_stale_active'",
                [],
            )
            .unwrap();
        }
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}", stale_marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();
        assert!(
            !stale_marker.exists(),
            "stale CAS must return before spawning a replacement master"
        );

        db.conn()
            .execute(
                "UPDATE sessions SET status = 'KILLED' WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_master_revive_reaps_worker_before_backoff_sleep() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_backoff_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_backoff_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_backoff_{}", uuid::Uuid::new_v4());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(&conn, "job_backoff", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE',
                     master_pid = 111,
                     master_generation = 5,
                     master_next_retry_at = ?2
                 WHERE id = ?1",
                rusqlite::params![&session_id, now + 1],
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_backoff'",
                [],
            )
            .unwrap();
        }

        let marker = state_dir.join("master-backoff-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        let task = tokio::spawn(revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}; sleep 5", marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        ));

        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        assert!(
            !marker.exists(),
            "worker must be reaped before delayed master revive is allowed to spawn"
        );
        task.await.unwrap().unwrap();
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_master_revive_fuse_reaps_worker_and_does_not_spawn() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_fuse_reap_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_fuse_reap_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_fuse_reap_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(&conn, "job_fuse_reap", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE',
                     master_pid = 111,
                     master_generation = 5,
                     master_retry_count = 4
                 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_fuse_reap'",
                [],
            )
            .unwrap();
        }

        let marker = state_dir.join("master-fuse-spawned");
        let tmux = Arc::new(TmuxServer::new(&state_dir));
        revive_master_after_exit(
            session_id.clone(),
            111,
            5,
            db.clone(),
            tmux.clone(),
            format!("touch {}", marker.display()),
            state_dir.clone(),
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(
            !marker.exists(),
            "fused revive must not spawn replacement master"
        );
        assert!(wait_for_session_status(&db, &session_id, "FAILED").await);
        assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_master_revive_startup_crash_loop_is_backed_off_and_fused() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        let project_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let session_id = format!("sess_crash_loop_{}", uuid::Uuid::new_v4());
        let agent_id = format!("ag_crash_loop_{}", uuid::Uuid::new_v4());
        let project_id = format!("p_crash_loop_{}", uuid::Uuid::new_v4());
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &session_id,
                &project_id,
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            insert_agent_sync(&conn, &agent_id, &session_id, "bash", "BUSY", Some(10)).unwrap();
            insert_job_sync(&conn, "job_crash_loop", &agent_id, None, "in flight").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_crash_loop'",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions SET master_retry_count = 4 WHERE id = ?1",
                [&session_id],
            )
            .unwrap();
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1; exit 0")
            .spawn()
            .unwrap();
        let pidfd = pidfd_open(child.id() as i32).unwrap();
        let task_fd = pidfd.try_clone().unwrap();
        let key = crate::master_revival::master_monitor_key(&session_id, 0);
        register(key.clone(), pidfd);

        let tmux = Arc::new(TmuxServer::new(&state_dir));
        spawn_master_pidfd_watch_task(
            session_id.clone(),
            0,
            0,
            task_fd,
            db.clone(),
            tmux.clone(),
            "sleep 0.1; exit 0".to_string(),
            state_dir,
            crate::sandbox::EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            None,
        );

        assert!(wait_for_session_status(&db, &session_id, "FAILED").await);
        let _ = child.wait();
        let retry_count: i64 = db
            .conn()
            .query_row(
                "SELECT master_retry_count FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retry_count, 5);
        let generation: i64 = db
            .conn()
            .query_row(
                "SELECT master_generation FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            generation <= 5,
            "startup crash loop must be bounded by fuse threshold, got generation {generation}"
        );
        let agent_state: String = db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [&agent_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(agent_state, "KILLED");
        assert!(wait_until_monitor_absent(&key).await);
        let _ = tmux.kill_session(master_session_name(&project_id)).await;
    }

    #[test]
    fn test_master_revive_fallback_generated_command() {
        use crate::sandbox::EnvState;
        use std::collections::HashMap;

        let mut master_env_vars = HashMap::new();
        master_env_vars.insert("AH_STATE_DIR".to_string(), "/tmp/state".to_string());
        crate::process_identity::inject_master_identity(&mut master_env_vars, "test-session-123");
        master_env_vars.insert("CCB_SOCKET".to_string(), "/tmp/state/ahd.sock".to_string());
        master_env_vars.insert("AH_MASTER_ROLE".to_string(), "managed".to_string());

        let env_state = EnvState {
            systemd_run_available: false,
            unsafe_no_sandbox: false,
            under_systemd: false,
        };

        let cmd = build_master_revive_command(
            "test-project",
            "test-cmd",
            &env_state,
            None,
            &master_env_vars,
            &crate::sandbox::SandboxOverrides::default(),
        );

        // Assert the generated command contains 'env' then '-u' then 'AH_AGENT_ID'
        assert_eq!(cmd[0], "env");
        assert_eq!(cmd[1], "-u");
        assert_eq!(cmd[2], "AH_AGENT_ID");

        // Assert it contains AH_ROLE=master and AH_SESSION_ID=test-session-123
        assert!(cmd.iter().any(|arg| arg == "AH_ROLE=master"));
        assert!(
            cmd.iter()
                .any(|arg| arg == "AH_SESSION_ID=test-session-123")
        );

        // Assert it does NOT contain any AH_AGENT_ID=<value> set token
        assert!(!cmd.iter().any(|arg| arg.starts_with("AH_AGENT_ID=")));
    }
}
