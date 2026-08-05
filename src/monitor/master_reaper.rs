use crate::completion::reader::{LogCursorMap, collect_provider_log_cursors};
use crate::db::Db;
use crate::db::sessions::query_session_by_id;
use crate::error::CcbdError;
use crate::master_revival::{
    confirm_master_stable, master_runtime_generation_matches,
    remove_master_monitor_key_if_generation_matches,
};
use crate::provider::home_layout::sandbox_home_for_sandbox_dir;
use crate::rpc::Ctx;
use crate::rpc::handlers::{RealignAgentParams, spawn_realign_agent};
use crate::sandbox::{EnvState, SandboxOverrides, path, systemd};
use crate::tmux::{TmuxPaneId, TmuxServer, master_session_name, sanitize_tmux_name};
use rusqlite::{OptionalExtension, params};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

pub(crate) const MASTER_REVIVE_CONTINUE_INSTRUCTION: &str = "继续";

pub(crate) struct SpawnedRevivedMaster {
    pub(crate) pane: TmuxPaneId,
    pub(crate) new_pid: i64,
    pub(crate) master_sandbox_home: PathBuf,
}

pub(crate) async fn spawn_replacement_master_pane(
    db: &Db,
    tmux_server: &TmuxServer,
    state_dir: &Path,
    env_state: &EnvState,
    daemon_unit: Option<&str>,
    session_id: &str,
    master_cmd: &str,
    redispatch_marker_path: Option<&Path>,
) -> Result<SpawnedRevivedMaster, CcbdError> {
    let session = query_session_by_id(db.clone(), session_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::IpcInvalidRequest(format!("session not found: {session_id}")))?;
    let master_cwd: PathBuf = session.absolute_path.clone().into();
    let master_session = master_session_name(&session.project_id);
    let mut master_env_vars = HashMap::new();
    master_env_vars.insert("AH_STATE_DIR".to_string(), state_dir.display().to_string());
    crate::process_identity::inject_master_identity(&mut master_env_vars, session_id);
    master_env_vars.insert(
        "CCB_SOCKET".to_string(),
        state_dir.join("ahd.sock").display().to_string(),
    );
    master_env_vars.insert("AH_MASTER_ROLE".to_string(), "managed".to_string());
    if let Some(marker_path) = redispatch_marker_path {
        master_env_vars.insert(
            "AH_REDISPATCH_MARKER".to_string(),
            marker_path.display().to_string(),
        );
    }
    // A revived master must come back with the same author-configured
    // environment the original seat had; the revive path has no payload, so it
    // re-reads the project config. Runtime variables were inserted above and
    // stay authoritative.
    for (key, value) in revive_master_seat_env(&master_cwd) {
        master_env_vars.entry(key).or_insert(value);
    }
    let sandbox_overrides = SandboxOverrides::default();
    let master_sandbox_home = if env_state.unsafe_no_sandbox {
        master_cwd.clone()
    } else {
        let sandbox_dir = path::resolve_sandbox_dir(state_dir, session_id, "master", &master_cwd)?;
        let home_root = sandbox_home_for_sandbox_dir(&sandbox_dir)?;
        let provider = revive_master_provider(master_cmd);
        master_env_vars.extend(crate::provider::home_layout::provider_home_env(
            provider.as_deref().unwrap_or_default(),
            &home_root,
        ));
        if provider.as_deref() == Some("claude") {
            let shared_credentials_dir =
                revive_claude_shared_credentials_dir(&master_cwd)?.ok_or_else(|| {
                    CcbdError::EnvironmentNotSupported {
                        details: "providers.claude.shared_credentials_dir is required for Claude master revive".into(),
                    }
                })?;
            let shared_credentials_dir =
                crate::provider::home_layout::validate_claude_shared_credentials_dir(
                    &shared_credentials_dir,
                )?;
            master_env_vars.insert(
                "CLAUDE_SECURESTORAGE_CONFIG_DIR".to_string(),
                shared_credentials_dir.display().to_string(),
            );
        }
        home_root
    };
    let tmux_cmd = build_master_revive_command(
        &session.project_id,
        master_cmd,
        env_state,
        daemon_unit,
        &master_env_vars,
        &sandbox_overrides,
    );
    tmux_server
        .ensure_session(master_session.clone(), master_cwd.clone())
        .await?;
    let pane = tmux_server
        .spawn_window(
            master_session,
            sanitize_tmux_name(&session.project_id),
            master_cwd,
            tmux_cmd,
        )
        .await?;
    let title = format!(
        "master ({})",
        master_cmd.split_whitespace().next().unwrap_or(master_cmd)
    );
    let _ = tmux_server.set_pane_title(pane.clone(), &title).await;
    let new_pid = tmux_server.get_pane_pid(pane.clone()).await? as i64;
    Ok(SpawnedRevivedMaster {
        pane,
        new_pid,
        master_sandbox_home,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn register_revived_master_watch_and_prepare_readiness(
    db: &Db,
    tmux_server: &Arc<TmuxServer>,
    state_dir: &Path,
    env_state: &EnvState,
    daemon_unit: Option<String>,
    session_id: &str,
    new_pid: i64,
    generation: i64,
    master_cmd: String,
    master_sandbox_home: &Path,
    monitor_key: String,
) -> Result<ReviveMasterReadinessCheck, CcbdError> {
    let pidfd = crate::monitor::pidfd_open(new_pid as i32)?;
    let task_fd = pidfd.try_clone().map_err(|err| {
        CcbdError::PtyIoError(format!("failed to clone revived master pidfd: {err}"))
    })?;
    crate::monitor::register(monitor_key, pidfd);
    let readiness_check =
        prepare_revive_master_readiness_check(session_id, &master_cmd, master_sandbox_home)?;
    crate::monitor::master_watch::spawn_master_pidfd_watch_task(
        session_id.to_string(),
        new_pid,
        generation,
        task_fd,
        db.clone(),
        tmux_server.clone(),
        master_cmd,
        state_dir.to_path_buf(),
        env_state.clone(),
        daemon_unit,
    );
    Ok(readiness_check)
}

/// How a revived master's readiness is established.
///
/// `Ack` is the semantic signal: the seat's own transcript shows assistant
/// progress after the revive instruction. `Started` is the declared degradation
/// for a master that is not an agent seat at all — a wrapper script or a bare
/// shell — where the only honest statement is that the process came up. Neither
/// form reads terminal text: a settled pane says the screen stopped changing,
/// not that the master resumed work.
#[derive(Debug, Clone)]
pub(crate) enum ReviveMasterReadinessCheck {
    Ack {
        provider: String,
        log_root: PathBuf,
        cursors: LogCursorMap,
    },
    /// The master seat cannot report readiness itself; see the warning logged
    /// when this variant is chosen for which capability was missing.
    Started,
}

impl ReviveMasterReadinessCheck {
    pub(crate) fn mode_name(&self) -> &'static str {
        match self {
            Self::Ack { .. } => "ack",
            Self::Started => "started",
        }
    }

    pub(crate) fn strength(&self) -> &'static str {
        match self {
            Self::Ack { .. } => "semantic",
            Self::Started => "degraded",
        }
    }

    pub(crate) fn ready_reason(&self) -> &'static str {
        match self {
            Self::Ack { .. } => "ack-assistant-progress",
            Self::Started => "started-process-alive",
        }
    }

    pub(crate) fn timeout_reason(&self) -> &'static str {
        match self {
            Self::Ack { .. } => "ack-timeout",
            Self::Started => "started-timeout",
        }
    }

    pub(crate) fn deadline_exhausted_reason(&self) -> &'static str {
        match self {
            Self::Ack { .. } => "ack-deadline-exhausted",
            Self::Started => "started-deadline-exhausted",
        }
    }
}

pub(crate) fn prepare_revive_master_readiness_check(
    session_id: &str,
    master_cmd: &str,
    master_sandbox_home: &Path,
) -> Result<ReviveMasterReadinessCheck, CcbdError> {
    let provider = revive_master_provider(master_cmd);

    #[cfg(not(test))]
    let _ = session_id;
    #[cfg(test)]
    if crate::monitor::master_watch::revive_master_readiness_ack_override(session_id).is_some() {
        let provider = provider.unwrap_or_else(|| "claude".to_string());
        return Ok(ReviveMasterReadinessCheck::Ack {
            log_root: crate::completion::log_layout::provider_log_root_in_home(
                &provider,
                master_sandbox_home,
            )
            .unwrap_or_else(|| master_sandbox_home.to_path_buf()),
            provider,
            cursors: LogCursorMap::new(),
        });
    }

    let Some(provider) = provider else {
        tracing::warn!(
            master_cmd,
            "master command names no provider ah knows; revive readiness degrades to process start"
        );
        return Ok(ReviveMasterReadinessCheck::Started);
    };
    if !crate::provider::manifest::provider_capabilities(&provider)
        .is_some_and(|capabilities| capabilities.readiness_ack)
    {
        tracing::warn!(
            provider,
            "master provider does not declare readiness_ack; revive readiness degrades to process start"
        );
        return Ok(ReviveMasterReadinessCheck::Started);
    }

    let log_root =
        crate::completion::log_layout::provider_log_root_in_home(&provider, master_sandbox_home)
            .ok_or_else(|| CcbdError::EnvironmentNotSupported {
                details: format!(
                    "provider '{provider}' has no transcript root for master revive readiness"
                ),
            })?;
    let cursors = collect_provider_log_cursors(&provider, &log_root).map_err(|err| {
        CcbdError::PtyIoError(format!(
            "collect {provider} revive readiness transcript cursors: {err}"
        ))
    })?;
    Ok(ReviveMasterReadinessCheck::Ack {
        provider,
        log_root,
        cursors,
    })
}

/// Provider that runs a revived master.
///
/// The revive path knows only the stored master command, so the provider is
/// whatever that command names — and nothing when it names no provider ah
/// knows. Unlike the spawn path there is no default: inventing a provider here
/// would hand a revived seat a home and credentials belonging to a provider it
/// never ran.
pub(crate) fn revive_master_provider(master_cmd: &str) -> Option<String> {
    crate::cli::config::provider_name_from_command(master_cmd)
}

/// Author-configured environment for a revived master: the project `[env]` with
/// `[master].env` layered on top, exactly as the spawn path merges them. A
/// config that cannot be read yields nothing rather than failing the revive —
/// bringing the master back matters more than its extra variables.
fn revive_master_seat_env(project_root: &Path) -> HashMap<String, String> {
    let Ok(config_path) = crate::cli::config::find_config(project_root) else {
        return HashMap::new();
    };
    match crate::cli::config::load_project_config(&config_path) {
        Ok(config) => {
            let mut env = config.env;
            env.extend(config.master.env);
            env
        }
        Err(err) => {
            tracing::warn!(error = %err, "revive could not read master env from project config");
            HashMap::new()
        }
    }
}

fn revive_claude_shared_credentials_dir(project_root: &Path) -> Result<Option<PathBuf>, CcbdError> {
    let config_path = crate::cli::config::find_config(project_root).map_err(|err| {
        CcbdError::EnvironmentNotSupported {
            details: format!("load Claude shared credentials config: {err}"),
        }
    })?;
    let config = crate::cli::config::load_project_config(&config_path).map_err(|err| {
        CcbdError::EnvironmentNotSupported {
            details: format!("load Claude shared credentials config: {err}"),
        }
    })?;
    Ok(config.providers.claude.shared_credentials_dir)
}

async fn reap_claimed_revive_master_after_error_best_effort(
    ctx: &Ctx,
    session_id: &str,
    claimed_generation: i64,
) {
    let runtime = match query_session_master_runtime_for_generation(
        &ctx.db,
        session_id,
        claimed_generation,
    ) {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::warn!(
                session_id,
                claimed_generation,
                error = %err,
                "failed to query claimed revived master runtime after revive error"
            );
            return;
        }
    };
    let Some((master_pid, pane_id)) = runtime else {
        tracing::debug!(
            session_id,
            claimed_generation,
            "no claimed revived master runtime to reap after revive error"
        );
        return;
    };
    let Some(pane_id) = pane_id else {
        tracing::warn!(
            session_id,
            master_pid,
            claimed_generation,
            "claimed revived master has no pane id during failure reap"
        );
        reap_failed_revive_master_process_and_watch_best_effort(
            session_id,
            master_pid,
            claimed_generation,
        );
        return;
    };
    let pane = match TmuxPaneId::parse(&pane_id) {
        Ok(pane) => pane,
        Err(err) => {
            tracing::warn!(
                session_id,
                pane_id,
                error = %err,
                "claimed revived master pane id is invalid during failure reap"
            );
            reap_failed_revive_master_process_and_watch_best_effort(
                session_id,
                master_pid,
                claimed_generation,
            );
            return;
        }
    };
    reap_failed_revive_master_runtime(ctx, session_id, master_pid, claimed_generation, &pane, true)
        .await;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailedReviveMasterReapTarget {
    RuntimeGeneration {
        master_pid: i64,
        generation: i64,
        pane: TmuxPaneId,
    },
    ClaimedGeneration {
        generation: i64,
    },
    SpawnedOrphanAfterFinalizeStale {
        master_pid: i64,
        generation: i64,
        pane: TmuxPaneId,
    },
}

pub(crate) async fn reap_failed_revive_master(
    ctx: &Ctx,
    session_id: &str,
    target: FailedReviveMasterReapTarget,
) {
    match target {
        FailedReviveMasterReapTarget::RuntimeGeneration {
            master_pid,
            generation,
            pane,
        } => {
            reap_failed_revive_master_runtime(ctx, session_id, master_pid, generation, &pane, true)
                .await;
        }
        FailedReviveMasterReapTarget::ClaimedGeneration { generation } => {
            reap_claimed_revive_master_after_error_best_effort(ctx, session_id, generation).await;
        }
        FailedReviveMasterReapTarget::SpawnedOrphanAfterFinalizeStale {
            master_pid,
            generation,
            pane,
        } => {
            reap_failed_revive_master_runtime(
                ctx, session_id, master_pid, generation, &pane, false,
            )
            .await;
        }
    }
}

async fn reap_failed_revive_master_runtime(
    ctx: &Ctx,
    session_id: &str,
    master_pid: i64,
    generation: i64,
    pane: &TmuxPaneId,
    fence_runtime_generation: bool,
) {
    if fence_runtime_generation {
        match master_runtime_generation_matches(&ctx.db, session_id, master_pid, generation) {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    session_id,
                    master_pid,
                    generation,
                    "skipping failed revive master reap because session runtime no longer matches"
                );
                return;
            }
            Err(err) => {
                tracing::warn!(
                    session_id,
                    master_pid,
                    generation,
                    error = %err,
                    "failed to fence failed revive master reap against session runtime"
                );
                return;
            }
        }
    }

    reap_failed_revive_master_process_and_watch_best_effort(session_id, master_pid, generation);

    let pane_label = pane.0.clone();
    if !kill_failed_revive_master_pane(
        ctx.tmux_server.as_ref(),
        session_id,
        pane,
        master_pid,
        generation,
    )
    .await
    {
        tracing::debug!(
            session_id,
            master_pid,
            generation,
            pane = %pane_label,
            "failed revived master pane was not killed"
        );
    }
}

fn reap_failed_revive_master_process_and_watch_best_effort(
    session_id: &str,
    master_pid: i64,
    generation: i64,
) {
    if let Err(err) = sigkill_failed_revive_master_process(session_id, master_pid, generation) {
        tracing::warn!(
            session_id,
            master_pid,
            generation,
            error = %err,
            "failed to SIGKILL failed revived master process"
        );
    }
    let removed = remove_master_monitor_key_if_generation_matches(session_id, generation);
    record_failed_revive_master_reap_event(
        session_id,
        FailedReviveMasterReapEvent::WatchRemoved {
            generation,
            removed,
        },
    );
}

fn query_session_master_runtime_for_generation(
    db: &Db,
    session_id: &str,
    generation: i64,
) -> Result<Option<(i64, Option<String>)>, CcbdError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT master_pid, master_pane_id FROM sessions
         WHERE id = ?1 AND master_generation = ?2",
        params![session_id, generation],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
    )
    .optional()
    .map_err(|err| {
        CcbdError::DbConstraintViolation(format!("query claimed revived master runtime: {err}"))
    })
}

fn sigkill_failed_revive_master_process(
    session_id: &str,
    master_pid: i64,
    generation: i64,
) -> Result<(), CcbdError> {
    if record_failed_revive_master_reap_event(
        session_id,
        FailedReviveMasterReapEvent::Sigkill {
            pid: master_pid,
            generation,
        },
    ) {
        return Ok(());
    }

    let key = crate::master_revival::master_monitor_key(session_id, generation);
    if let Some(result) = crate::monitor::with_borrowed(&key, crate::monitor::pidfd_send_sigkill) {
        return result;
    }

    let pid = i32::try_from(master_pid).map_err(|_| {
        CcbdError::PtyIoError(format!("invalid failed revived master pid: {master_pid}"))
    })?;
    #[cfg(windows)]
    {
        let _ = pid;
        return Err(CcbdError::EnvironmentNotSupported {
            details: "Windows failed revive master cleanup requires the M1 Job Object reaper"
                .to_string(),
        });
    }
    #[cfg(unix)]
    {
        // SAFETY: kill is called with a plain pid obtained from the session row and SIGKILL.
        let result = unsafe { libc::kill(pid, libc::SIGKILL) };
        if result == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        Err(CcbdError::PtyIoError(format!(
            "kill({pid}, SIGKILL) failed: {err}"
        )))
    }
}

async fn kill_failed_revive_master_pane(
    tmux_server: &TmuxServer,
    session_id: &str,
    pane: &TmuxPaneId,
    expected_pid: i64,
    generation: i64,
) -> bool {
    if record_failed_revive_master_reap_event(
        session_id,
        FailedReviveMasterReapEvent::PaneKill {
            pane: pane.0.clone(),
            generation,
        },
    ) {
        return true;
    }
    tmux_server
        .kill_pane_if_owned(pane.clone(), expected_pid)
        .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailedReviveMasterReapEvent {
    Sigkill { pid: i64, generation: i64 },
    PaneKill { pane: String, generation: i64 },
    WatchRemoved { generation: i64, removed: bool },
}

#[cfg(test)]
static FAILED_REVIVE_MASTER_REAP_RECORDERS: LazyLock<
    Mutex<HashMap<String, Arc<Mutex<Vec<FailedReviveMasterReapEvent>>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
pub(crate) struct FailedReviveMasterReapRecorderGuard {
    session_id: String,
}

#[cfg(test)]
impl Drop for FailedReviveMasterReapRecorderGuard {
    fn drop(&mut self) {
        FAILED_REVIVE_MASTER_REAP_RECORDERS
            .lock()
            .expect("failed revive master reap recorder mutex poisoned")
            .remove(&self.session_id);
    }
}

#[cfg(test)]
pub(crate) fn set_failed_revive_master_reap_recorder(
    session_id: &str,
) -> (
    Arc<Mutex<Vec<FailedReviveMasterReapEvent>>>,
    FailedReviveMasterReapRecorderGuard,
) {
    let events = Arc::new(Mutex::new(Vec::new()));
    FAILED_REVIVE_MASTER_REAP_RECORDERS
        .lock()
        .expect("failed revive master reap recorder mutex poisoned")
        .insert(session_id.to_string(), events.clone());
    (
        events,
        FailedReviveMasterReapRecorderGuard {
            session_id: session_id.to_string(),
        },
    )
}

#[cfg(test)]
fn record_failed_revive_master_reap_event(
    session_id: &str,
    event: FailedReviveMasterReapEvent,
) -> bool {
    let Some(events) = FAILED_REVIVE_MASTER_REAP_RECORDERS
        .lock()
        .expect("failed revive master reap recorder mutex poisoned")
        .get(session_id)
        .cloned()
    else {
        return false;
    };
    events
        .lock()
        .expect("failed revive master reap event mutex poisoned")
        .push(event);
    true
}

#[cfg(not(test))]
fn record_failed_revive_master_reap_event(
    _session_id: &str,
    _event: FailedReviveMasterReapEvent,
) -> bool {
    false
}

pub(crate) async fn reprovision_declared_workers_after_master_revive(
    session_id: &str,
    db: Db,
    tmux_server: Arc<TmuxServer>,
    state_dir: PathBuf,
    env_state: EnvState,
    daemon_unit: Option<String>,
) -> Result<Vec<crate::db::recovery::AgentRecoveryIntent>, CcbdError> {
    let stored_specs = {
        let conn = db.conn();
        crate::db::recovery::query_agent_spawn_specs_for_session_sync(&conn, session_id)?
    };
    if stored_specs.is_empty() {
        return Ok(Vec::new());
    }
    let session = query_session_by_id(db.clone(), session_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::IpcInvalidRequest(format!("session not found: {session_id}")))?;
    let project_root = PathBuf::from(&session.absolute_path);
    let claude_shared_credentials_dir = if stored_specs
        .iter()
        .any(|stored| stored.spec.provider == "claude")
    {
        revive_claude_shared_credentials_dir(&project_root)?
    } else {
        None
    };
    let captured_intents = collect_master_revive_recovery_intents_before_reprovision(
        &db,
        session_id,
        stored_specs
            .iter()
            .map(|stored| stored.spec.agent_id.as_str()),
    )?;
    let ctx = Ctx {
        db: db.clone(),
        state_dir,
        env_state,
        daemon_unit,
        tmux_server,
        claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
    };
    for stored in stored_specs {
        let agent_id = stored.spec.agent_id.clone();
        let agent = RealignAgentParams {
            agent_id: stored.spec.agent_id.clone(),
            provider: stored.spec.provider.clone(),
            env: stored.spec.env.clone(),
            hooks: stored.spec.hooks.clone(),
            plugins: stored.spec.plugins.clone(),
            skills: stored.spec.skills.clone(),
            bundle: stored.spec.bundle.clone(),
            settings: stored.spec.settings.clone(),
            bundle_digest: stored.spec.bundle_digest.clone(),
            sandbox_overrides: stored.spec.sandbox_overrides.clone(),
            hook_push_enabled: stored.spec.hook_push_enabled,
            claude_shared_credentials_dir: (stored.spec.provider == "claude")
                .then(|| claude_shared_credentials_dir.clone())
                .flatten(),
        };
        let captured_intent = captured_intents
            .iter()
            .find(|intent| intent.agent_id == agent_id)
            .cloned();
        if let Err(err) = revive_reprovision_one_worker(
            &ctx,
            session_id,
            &agent,
            stored.config_hash.as_str(),
            captured_intent,
        )
        .await
        {
            if let Err(restore_err) = restore_killed_worker_spawn_spec(&ctx.db, session_id, &stored)
            {
                tracing::warn!(
                    session_id,
                    agent_id = %agent_id,
                    error = %restore_err,
                    "restore killed worker spawn spec failed during revive re-provision"
                );
            }
            tracing::warn!(
                session_id,
                agent_id = %agent_id,
                error = %err,
                "master revive worker re-provision failed; restored killed worker snapshot"
            );
        }
    }
    Ok(captured_intents)
}

async fn revive_reprovision_one_worker(
    ctx: &Ctx,
    session_id: &str,
    agent: &RealignAgentParams,
    expected_hash: &str,
    captured_intent: Option<crate::db::recovery::AgentRecoveryIntent>,
) -> Result<(), CcbdError> {
    // ISSUE-13 §3a: master-revive replays the stored BARE snapshot env; an empty config_env
    // re-merges to the same bare env, so the reprovisioned hash matches expected_hash.
    let config_env = std::collections::HashMap::new();
    spawn_realign_agent(
        ctx,
        session_id,
        agent,
        expected_hash,
        false,
        true,
        &config_env,
        captured_intent,
    )
    .await
}

fn collect_master_revive_recovery_intents_before_reprovision<'a>(
    db: &Db,
    session_id: &str,
    agent_ids: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<crate::db::recovery::AgentRecoveryIntent>, CcbdError> {
    let conn = db.conn();
    let mut intents = Vec::new();
    for agent_id in agent_ids {
        let Some(intent) = crate::db::recovery::query_agent_recovery_intent_sync(&conn, agent_id)?
        else {
            tracing::debug!(
                session_id,
                agent_id,
                "no captured recovery intent before master revive worker reprovision"
            );
            continue;
        };
        tracing::info!(
            session_id,
            agent_id,
            interrupted_job_id = ?intent.interrupted_job_id,
            "captured master revive recovery intent in memory before worker reprovision"
        );
        intents.push(intent);
    }
    Ok(intents)
}

fn restore_killed_worker_spawn_spec(
    db: &Db,
    session_id: &str,
    stored: &crate::db::recovery::StoredAgentSpawnSpec,
) -> Result<(), CcbdError> {
    let conn = db.conn();
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM agents WHERE id = ? LIMIT 1",
            params![stored.spec.agent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("query revive restore worker: {err}"))
        })?;
    if exists.is_none() {
        crate::db::agents::insert_agent_sync(
            &conn,
            &stored.spec.agent_id,
            session_id,
            &stored.spec.provider,
            "KILLED",
            None,
        )?;
    }
    crate::db::recovery::persist_agent_spawn_spec_sync(&conn, &stored.spec, &stored.config_hash)
}

pub(crate) fn spawn_master_confirm_timer(
    db: Db,
    session_id: String,
    expected_pid: i64,
    expected_generation: i64,
) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        if let Err(err) = confirm_master_stable(&db, &session_id, expected_pid, expected_generation)
        {
            tracing::warn!(session_id = %session_id, error = %err, "master stable confirm failed");
        }
    });
}

pub(crate) fn build_master_revive_command(
    project_id: &str,
    master_cmd: &str,
    env_state: &EnvState,
    daemon_unit: Option<&str>,
    master_env_vars: &HashMap<String, String>,
    sandbox_overrides: &SandboxOverrides,
) -> Vec<String> {
    systemd::master_command_with_env(
        project_id,
        master_cmd,
        env_state,
        daemon_unit,
        master_env_vars,
        sandbox_overrides,
    )
}

pub(crate) fn master_revival_redispatch_marker_path(
    state_dir: &Path,
    session_id: &str,
    revived_generation: i64,
) -> PathBuf {
    state_dir
        .join("master-revival")
        .join(session_id)
        .join(format!("redispatch-generation-{revived_generation}.json"))
}

pub(crate) async fn inject_master_continue_instruction_best_effort<F, Fut>(
    tmux_server: &TmuxServer,
    pane: &crate::tmux::TmuxPaneId,
    redispatch_marker_path: Option<&Path>,
    writer: F,
) -> Result<(), CcbdError>
where
    F: FnOnce(&TmuxServer, crate::tmux::TmuxPaneId, String) -> Fut,
    Fut: std::future::Future<Output = Result<(), CcbdError>>,
{
    tracing::info!(
        pane = %pane.0,
        marker = ?redispatch_marker_path.map(|path| path.display().to_string()),
        "injecting continue instruction into revived master pane"
    );
    match writer(
        tmux_server,
        pane.clone(),
        MASTER_REVIVE_CONTINUE_INSTRUCTION.to_string(),
    )
    .await
    {
        Ok(()) => {
            tracing::info!(pane = %pane.0, "continue instruction injected into revived master pane");
        }
        Err(err) => {
            tracing::warn!(
                pane = %pane.0,
                error = %err,
                marker = ?redispatch_marker_path.map(|path| path.display().to_string()),
                "failed to inject continue instruction into revived master pane; revive continues"
            );
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn requeue_master_revive_interrupted_jobs_after_reprovision(
    db: &Db,
    session_id: &str,
    captured_intents: &[crate::db::recovery::AgentRecoveryIntent],
) -> Result<usize, CcbdError> {
    if captured_intents.is_empty() {
        tracing::debug!(
            session_id,
            "no captured interrupted jobs to requeue after master revive"
        );
        return Ok(0);
    }

    let mut requeued = 0;
    for intent in captured_intents {
        tracing::info!(
            session_id,
            agent_id = %intent.agent_id,
            interrupted_job_id = ?intent.interrupted_job_id,
            "requeueing in-memory captured interrupted job after master revive worker reprovision"
        );
        requeued +=
            crate::db::recovery::requeue_interrupted_job_from_captured_intent_standalone_sync(
                db, intent,
            )?;
    }
    if requeued > 0 {
        crate::db::jobs::notify_runtime_job_changed();
    }
    Ok(requeued)
}

pub(crate) fn write_master_revival_redispatch_marker(
    state_dir: &Path,
    session_id: &str,
    expected_pid: i64,
    observed_generation: i64,
    revived_generation: i64,
    worker_ids_to_reap: &[String],
) -> std::io::Result<PathBuf> {
    let marker_path =
        master_revival_redispatch_marker_path(state_dir, session_id, revived_generation);
    if let Some(parent) = marker_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::json!({
        "session_id": session_id,
        "expected_pid": expected_pid,
        "observed_generation": observed_generation,
        "revived_generation": revived_generation,
        "worker_ids_to_reap": worker_ids_to_reap,
        "redispatch_required": true,
        "continue_required": true,
        "continue_instruction": MASTER_REVIVE_CONTINUE_INSTRUCTION,
        "hint": "Master was revived after master death; workers were reaped; inspect ah ps/logs and re-dispatch missing work."
    })
    .to_string();
    std::fs::write(&marker_path, body)?;
    Ok(marker_path)
}
