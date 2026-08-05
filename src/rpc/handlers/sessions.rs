use super::params::{extension_config_from_params, required_str};
use crate::db::agents::query_agent;
use crate::db::schema::Session;
use crate::db::sessions::{
    create_session, list_session_summaries, query_session_by_id, set_session_master_pane_id,
    update_session_config_hash, update_session_master_cmd,
};
use crate::db::system::{
    cascade_kill_session_agents, cascade_kill_session_agents_for_daemon,
    remove_agent_sandbox_dir_sync, session_agent_ids,
};
use crate::error::CcbdError;
use crate::master_revival::{
    mark_session_intentional_killed, master_monitor_key, record_spawned_master_runtime,
    record_spawned_master_runtime_after_claim,
};
use crate::monitor;
use crate::monitor::master_watch::spawn_master_pidfd_watch_task;
use crate::monitor::session_watch::{spawn_session_watch_task, unit_name_for_session};
use crate::provider::bundles::{BundleRole, resolve_bundles_for_provider};
use crate::provider::extensions::ExtensionConfig;
use crate::provider::fingerprint::{ConfigFingerprintInput, ConfigRole, compute_config_hash};
use crate::provider::home_layout::{
    HomeLayoutRole, HookPushContext,
    prepare_home_layout_with_extensions_for_slot_and_claude_credentials,
};
use crate::rpc::Ctx;
use crate::sandbox::{SandboxOverrides, path, systemd};
use crate::tmux::{
    TmuxPaneId, TmuxWindowSize, agent_session_name, master_session_name, sanitize_tmux_name,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

static SESSION_WINDOW_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) fn session_window_lock(session_id: &str) -> Arc<AsyncMutex<()>> {
    let mut locks = SESSION_WINDOW_LOCKS
        .lock()
        .expect("session window locks mutex poisoned");
    locks
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

pub async fn handle_session_create(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let project_id = required_str(&params, "project_id")?;
    let absolute_path = required_str(&params, "absolute_path")?;
    let session_id = format!("sess_{}", Uuid::new_v4());

    create_session(
        ctx.db.clone(),
        session_id.clone(),
        project_id.to_string(),
        absolute_path.to_string(),
    )
    .await?;

    if session_anchors_enabled(ctx) {
        let unit_name = unit_name_for_session(&session_id);
        if let Err(err) = create_session_anchor(&unit_name) {
            let _ = cascade_kill_session_agents(
                ctx.db.clone(),
                session_id.clone(),
                "ANCHOR_CREATE_FAILED".to_string(),
            )
            .await;
            return Err(err);
        }
        spawn_session_watch_task(session_id.clone(), unit_name, Arc::new(ctx.db.clone()));
    }

    Ok(json!({ "session_id": session_id }))
}

pub async fn handle_session_kill(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let session_id = required_str(&params, "session_id")?;
    let force = params
        .get("force")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let session = query_session_by_id(ctx.db.clone(), session_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::IpcInvalidRequest(format!("session not found: {session_id}")))?;
    let agent_ids = session_agent_ids(ctx.db.clone(), session_id.to_string()).await?;

    if is_terminal_session_status(&session.status) {
        mark_terminal_session_killed(&ctx.db, session_id)?;
        if session_anchors_enabled(ctx) {
            stop_session_anchor(&unit_name_for_session(session_id));
        }
        let killed =
            mark_terminal_session_agents_killed_db_only(&ctx.db, session_id, "SESSION_KILL")?;
        for agent_id in &agent_ids {
            ctx.claude_gateway.deregister(agent_id).await;
            remove_agent_sandbox_dir_sync(&ctx.state_dir, session_id, agent_id);
        }
        ctx.claude_gateway.deregister(session_id).await;
        remove_agent_sandbox_dir_sync(&ctx.state_dir, session_id, "master");
        return Ok(json!({
            "session_id": session_id,
            "state": "KILLED",
            "killed_agents": killed,
            "master_pane_killed": false,
        }));
    }

    let master_pid = query_session_master_pid(&ctx.db, session_id)?;

    mark_session_intentional_killed(&ctx.db, session_id)?;

    if session_anchors_enabled(ctx) {
        stop_session_anchor(&unit_name_for_session(session_id));
    }

    let killed = cascade_kill_session_agents_for_daemon(
        ctx.db.clone(),
        session_id.to_string(),
        "SESSION_KILL".to_string(),
        ctx.tmux_server.socket_name().to_string(),
    )
    .await?;
    for agent_id in &agent_ids {
        let expected_pid = match query_agent(ctx.db.clone(), agent_id.to_string()).await? {
            Some(agent) => agent.pid,
            None => {
                tracing::warn!(session_id, agent_id = %agent_id, "agent missing during session kill; skipping tmux agent teardown");
                None
            }
        };
        if let Some(pane_id) = crate::agent_io::pane_id(agent_id) {
            if let Some(expected_pid) = expected_pid {
                let _ = ctx
                    .tmux_server
                    .kill_pane_if_owned(pane_id, expected_pid)
                    .await;
            }
        }
        if let Some(expected_pid) = expected_pid {
            let _ = ctx
                .tmux_server
                .kill_session_if_owned(agent_session_name(agent_id), expected_pid)
                .await;
        } else {
            tracing::warn!(session_id, agent_id = %agent_id, "agent has no expected pid; skipping tmux agent session kill");
        }
    }
    for agent_id in &agent_ids {
        ctx.claude_gateway.deregister(agent_id).await;
        remove_agent_sandbox_dir_sync(&ctx.state_dir, session_id, agent_id);
    }
    ctx.claude_gateway.deregister(session_id).await;
    remove_agent_sandbox_dir_sync(&ctx.state_dir, session_id, "master");
    if force {
        tracing::debug!(
            session_id,
            "session.kill force requested; runtime teardown is already best-effort by default"
        );
    }
    let master_pane_killed = if let Some(master_pane_id) = session.master_pane_id {
        match TmuxPaneId::parse(&master_pane_id) {
            Ok(pane_id) if master_pid > 0 => {
                ctx.tmux_server
                    .kill_pane_if_owned(pane_id, master_pid)
                    .await
            }
            Ok(_) => false,
            Err(err) => {
                tracing::warn!(session_id, pane_id = %master_pane_id, error = %err, "invalid stored master pane id");
                false
            }
        }
    } else {
        false
    };
    if master_pid > 0 {
        let _ = ctx
            .tmux_server
            .kill_session_if_owned(master_session_name(&session.project_id), master_pid)
            .await;
    } else {
        tracing::warn!(
            session_id,
            project_id = %session.project_id,
            master_pid,
            "session has no expected master pid; skipping master tmux session kill"
        );
    }
    Ok(json!({
        "session_id": session_id,
        "state": "KILLED",
        "killed_agents": killed,
        "master_pane_killed": master_pane_killed,
    }))
}

pub(super) fn is_terminal_session_status(status: &str) -> bool {
    // Only these session statuses are terminal and eligible for DB-only cleanup.
    // Any other non-ACTIVE status intentionally falls through to guarded tmux teardown;
    // update this allowlist when adding a terminal session status.
    matches!(status, "KILLED" | "FAILED" | "CLOSED")
}

fn mark_terminal_session_killed(db: &crate::db::Db, session_id: &str) -> Result<(), CcbdError> {
    db.conn()
        .execute(
            "UPDATE sessions SET status = 'KILLED', master_state = 'IDLE' WHERE id = ?1 AND status != 'ACTIVE'",
            [session_id],
        )
        .map_err(|err| CcbdError::DbConstraintViolation(format!("mark terminal session killed: {err}")))?;
    Ok(())
}

pub(super) fn mark_terminal_session_agents_killed_db_only(
    db: &crate::db::Db,
    session_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn.transaction().map_err(|err| {
        CcbdError::DbConstraintViolation(format!("begin terminal agent cleanup: {err}"))
    })?;
    let agent_ids = {
        let mut stmt = tx
            .prepare(
                "SELECT id FROM agents WHERE session_id = ?1 AND state NOT IN ('CRASHED', 'KILLED')",
            )
            .map_err(|err| CcbdError::DbConstraintViolation(format!("prepare terminal agent cleanup: {err}")))?;
        let rows = stmt
            .query_map([session_id], |row| row.get::<_, String>(0))
            .map_err(|err| {
                CcbdError::DbConstraintViolation(format!("query terminal agent cleanup: {err}"))
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|err| {
            CcbdError::DbConstraintViolation(format!("collect terminal agent cleanup: {err}"))
        })?
    };
    let mut changed = 0;
    for agent_id in agent_ids {
        let previous_state = tx
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [&agent_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(|err| {
                CcbdError::DbConstraintViolation(format!("query terminal agent state: {err}"))
            })?;
        let rows = tx
            .execute(
                "UPDATE agents SET state = 'KILLED', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ?1 AND session_id = ?2 AND state NOT IN ('CRASHED', 'KILLED')",
                (&agent_id, session_id),
            )
            .map_err(|err| CcbdError::DbConstraintViolation(format!("mark terminal agent killed: {err}")))?;
        if rows == 1 {
            crate::db::jobs::mark_dispatched_jobs_failed_for_agent_conn_collect_sync(
                &tx, &agent_id, reason,
            )?;
            let payload = serde_json::json!({
                "from": previous_state,
                "to": "KILLED",
                "reason": reason,
            })
            .to_string();
            tx.execute(
                "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?1, NULL, 'state_change', ?2)",
                (&agent_id, payload),
            )
            .map_err(|err| CcbdError::DbConstraintViolation(format!("insert terminal killed state_change: {err}")))?;
            changed += 1;
        }
    }
    tx.commit().map_err(|err| {
        CcbdError::DbConstraintViolation(format!("commit terminal agent cleanup: {err}"))
    })?;
    Ok(changed)
}

fn query_session_master_pid(db: &crate::db::Db, session_id: &str) -> Result<i64, CcbdError> {
    db.conn()
        .query_row(
            "SELECT master_pid FROM sessions WHERE id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .map_err(|err| CcbdError::DbConstraintViolation(format!("query session master pid: {err}")))
}

pub(super) fn session_anchors_enabled(ctx: &Ctx) -> bool {
    ctx.env_state.systemd_run_available
        && (ctx.env_state.unsafe_no_sandbox || ctx.env_state.under_systemd)
}

fn create_session_anchor(unit_name: &str) -> Result<(), CcbdError> {
    let output = Command::new("systemd-run")
        .args([
            "--user",
            &format!("--unit={unit_name}"),
            "--remain-after-exit",
            "/usr/bin/true",
        ])
        .output()
        .map_err(|err| CcbdError::EnvironmentNotSupported {
            details: format!("create session anchor {unit_name}: {err}"),
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(CcbdError::EnvironmentNotSupported {
        details: format!(
            "create session anchor {unit_name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    })
}

pub(super) fn stop_session_anchor(unit_name: &str) {
    match Command::new("systemctl")
        .args(["--user", "stop", unit_name])
        .output()
    {
        Ok(output) if output.status.success() => {}
        Ok(output) => tracing::warn!(
            unit = %unit_name,
            status = ?output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "failed to stop session anchor"
        ),
        Err(err) => tracing::warn!(unit = %unit_name, error = %err, "failed to run systemctl stop"),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpawnMasterPaneParams {
    pub(crate) session_id: String,
    pub(crate) cmd: String,
    /// Provider that runs this master seat, as resolved by the config layer.
    /// `None` means the caller did not resolve one, and the daemon derives it
    /// from `cmd` the same way the config layer would.
    pub(crate) provider: Option<String>,
    /// Author-configured environment for this master seat: the project `[env]`
    /// with `[master].env` layered on top.
    pub(crate) seat_env: HashMap<String, String>,
    pub(crate) tmux_window_size: TmuxWindowSize,
    pub(crate) extensions: ExtensionConfig,
    pub(crate) extra_env: HashMap<String, String>,
    pub(crate) claimed_master_generation: Option<i64>,
    pub(crate) claude_shared_credentials_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpawnMasterPaneOutcome {
    pub(crate) pane_id: String,
    pub(crate) home_root: Option<PathBuf>,
    pub(crate) new_pid: Option<i64>,
    pub(crate) generation: Option<i64>,
}

#[derive(Debug, Clone)]
pub(super) struct MasterPanePlan {
    pub(super) session: Session,
    pub(super) master_cwd: PathBuf,
    pub(super) master_env_vars: HashMap<String, String>,
    pub(super) sandbox_overrides: SandboxOverrides,
    pub(super) home_root: Option<PathBuf>,
    pub(super) extensions: ExtensionConfig,
}

pub async fn handle_session_spawn_master_pane(
    params: Value,
    ctx: &Ctx,
) -> Result<Value, CcbdError> {
    let session_id = required_str(&params, "session_id")?;
    let cmd = required_str(&params, "cmd")?;
    let claimed_master_generation = params
        .get("_claimed_master_generation")
        .and_then(Value::as_i64);
    let tmux_window_size = parse_tmux_window_size(&params)?;
    let extensions = extension_config_from_params(&params)?;
    let extra_env = parse_extra_env(&params)?;
    // The master's own env plus the project env, merged server-side exactly the
    // way agent.spawn does it, so spawn and realign cannot diverge.
    let seat_env = parse_env_map(&params, "env")?;
    let config_env = parse_env_map(&params, "config_env")?;
    let claude_shared_credentials_dir = optional_pathbuf(&params, "claude_shared_credentials_dir")?;
    let provider = params
        .get("provider")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .map(str::to_string);
    let outcome = spawn_master_pane_inner(
        ctx,
        SpawnMasterPaneParams {
            session_id: session_id.to_string(),
            cmd: cmd.to_string(),
            provider,
            seat_env: merge_seat_env(config_env, seat_env),
            tmux_window_size,
            extensions,
            extra_env,
            claimed_master_generation,
            claude_shared_credentials_dir,
        },
    )
    .await?;

    Ok(json!({ "pane_id": outcome.pane_id }))
}

fn parse_tmux_window_size(params: &Value) -> Result<TmuxWindowSize, CcbdError> {
    match params.get("tmux_window_size") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|err| {
            CcbdError::IpcInvalidRequest(format!("invalid tmux_window_size: {err}"))
        }),
        None => Ok(TmuxWindowSize::Fixed),
    }
}

/// Layers the master seat's own env over the project env. The seat wins on
/// collisions, matching how an agent's `env` overrides the project `[env]`.
pub(crate) fn merge_seat_env(
    config_env: HashMap<String, String>,
    seat_env: HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = config_env;
    merged.extend(seat_env);
    merged
}

fn parse_env_map(params: &Value, key: &str) -> Result<HashMap<String, String>, CcbdError> {
    match params.get(key) {
        Some(value) => serde_json::from_value::<HashMap<String, String>>(value.clone())
            .map_err(|err| CcbdError::IpcInvalidRequest(format!("invalid {key}: {err}"))),
        None => Ok(HashMap::new()),
    }
}

fn parse_extra_env(params: &Value) -> Result<HashMap<String, String>, CcbdError> {
    if let Some(value) = params
        .get("extra_env")
        .or_else(|| params.get("extra_env_vars"))
    {
        serde_json::from_value::<HashMap<String, String>>(value.clone())
            .map_err(|err| CcbdError::IpcInvalidRequest(format!("invalid extra_env: {err}")))
    } else {
        Ok(HashMap::new())
    }
}

pub(crate) fn optional_pathbuf(params: &Value, key: &str) -> Result<Option<PathBuf>, CcbdError> {
    match params.get(key) {
        Some(value) => serde_json::from_value::<Option<PathBuf>>(value.clone())
            .map_err(|err| CcbdError::IpcInvalidRequest(format!("invalid {key}: {err}"))),
        None => Ok(None),
    }
}

pub(super) async fn prepare_master_pane_plan(
    ctx: &Ctx,
    params: &SpawnMasterPaneParams,
) -> Result<MasterPanePlan, CcbdError> {
    let session = query_session_by_id(ctx.db.clone(), params.session_id.clone())
        .await?
        .ok_or_else(|| {
            CcbdError::IpcInvalidRequest(format!("session not found: {}", params.session_id))
        })?;
    let master_cwd: std::path::PathBuf = session.absolute_path.clone().into();
    let provider = params.resolved_provider();
    let resolved_bundles = resolve_bundles_for_provider(
        &master_cwd,
        &provider,
        BundleRole::Master,
        &params.extensions,
    )?;
    let extensions = resolved_bundles.extensions;
    let hook_generation = if let Some(generation) = params.claimed_master_generation {
        generation
    } else {
        let conn = ctx.db.conn();
        conn.query_row(
            "SELECT master_generation + 1 FROM sessions WHERE id = ?1",
            [&params.session_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("query master generation: {err}"))
        })?
    };
    let mut master_env_vars = build_master_spawn_env_vars(
        &params.session_id,
        params.seat_env.clone(),
        params.extra_env.clone(),
    );
    let sandbox_overrides = SandboxOverrides::default();
    let mut home_root = None;
    let master_sandbox_dir = if ctx.env_state.unsafe_no_sandbox {
        None
    } else {
        Some(path::resolve_sandbox_dir(
            &ctx.state_dir,
            &params.session_id,
            "master",
            &master_cwd,
        )?)
    };
    if let Some(dir) = master_sandbox_dir.as_ref() {
        let hook_push_ctx = HookPushContext {
            agent_id: format!("master:{}:{hook_generation}", params.session_id),
            provider: provider.clone(),
            ahd_socket_path: ctx.state_dir.join("ahd.sock"),
            enabled: true,
        };
        let home_overrides = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
            &provider,
            dir,
            &master_cwd,
            HomeLayoutRole::Master,
            "master",
            &extensions,
            Some(&hook_push_ctx),
            claude_shared_credentials_dir_for_provider(
                &provider,
                params.claude_shared_credentials_dir.as_deref(),
            ),
        )?;
        home_root = Some(home_overrides.home_root);
        master_env_vars.extend(home_overrides.extra_env);
    }

    Ok(MasterPanePlan {
        session,
        master_cwd,
        master_env_vars,
        sandbox_overrides,
        home_root,
        extensions,
    })
}

impl SpawnMasterPaneParams {
    /// Provider that runs this master seat. The caller's resolved value wins;
    /// otherwise the provider is derived from `cmd`, falling back to the default
    /// master provider exactly as the config layer does.
    pub(crate) fn resolved_provider(&self) -> String {
        crate::cli::config::resolve_master_provider(self.provider.as_deref(), &self.cmd)
    }
}

/// The shared Claude credentials directory only travels to a Claude seat; every
/// other provider must never receive it.
pub(crate) fn claude_shared_credentials_dir_for_provider<'a>(
    provider: &str,
    shared_credentials_dir: Option<&'a Path>,
) -> Option<&'a Path> {
    (crate::provider::manifest::canonicalize_provider_name(provider) == "claude")
        .then_some(shared_credentials_dir)
        .flatten()
}

/// Builds the master's spawn environment.
///
/// Author-configured values go in first and ah's own runtime variables are
/// injected after, so a project cannot accidentally (or deliberately) redirect
/// the seat's identity, state directory or daemon socket.
fn build_master_spawn_env_vars(
    session_id: &str,
    seat_env: HashMap<String, String>,
    runtime_env: HashMap<String, String>,
) -> HashMap<String, String> {
    let mut env = seat_env;
    env.extend(runtime_env);
    crate::process_identity::inject_master_identity(&mut env, session_id);
    strip_claude_gateway_env(&mut env);
    env
}

pub(crate) fn strip_claude_gateway_env(env: &mut HashMap<String, String>) {
    env.remove("CLAUDE_CODE_USE_GATEWAY");
    env.remove("ANTHROPIC_AUTH_TOKEN");
    env.remove("AH_CLAUDE_GATEWAY_HOST_UDS");
    env.remove(crate::claude_gateway::GATEWAY_SANDBOX_ROOT_ENV);
}

pub(crate) async fn spawn_master_pane_inner(
    ctx: &Ctx,
    params: SpawnMasterPaneParams,
) -> Result<SpawnMasterPaneOutcome, CcbdError> {
    let plan = prepare_master_pane_plan(ctx, &params).await?;
    spawn_prepared_master_pane(ctx, params, plan, true).await
}

pub(super) async fn spawn_prepared_master_pane(
    ctx: &Ctx,
    params: SpawnMasterPaneParams,
    plan: MasterPanePlan,
    arm_revival_watch: bool,
) -> Result<SpawnMasterPaneOutcome, CcbdError> {
    let tmux_cmd = systemd::master_command_with_env(
        &plan.session.project_id,
        &params.cmd,
        &ctx.env_state,
        ctx.daemon_unit.as_deref(),
        &plan.master_env_vars,
        &plan.sandbox_overrides,
    );
    let master_session = master_session_name(&plan.session.project_id);
    ctx.tmux_server
        .ensure_session_with_window_size(
            master_session.clone(),
            plan.master_cwd.clone(),
            params.tmux_window_size,
        )
        .await?;
    let pane = ctx
        .tmux_server
        .spawn_window(
            master_session,
            sanitize_tmux_name(&plan.session.project_id),
            plan.master_cwd,
            tmux_cmd,
        )
        .await?;
    let title = format!(
        "master ({})",
        params.cmd.split_whitespace().next().unwrap_or(&params.cmd)
    );
    let _ = ctx.tmux_server.set_pane_title(pane.clone(), &title).await;
    set_session_master_pane_id(ctx.db.clone(), params.session_id.clone(), pane.0.clone()).await?;
    let config_hash = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Master {
            cmd: &params.cmd,
            env: &params.seat_env,
        },
        hooks: &plan.extensions.hooks,
        plugins: &plan.extensions.plugins,
        skills: &plan.extensions.skills,
        settings: &plan.extensions.settings,
        bundle: plan.extensions.bundle_digest.as_ref(),
    })?;
    update_session_config_hash(ctx.db.clone(), params.session_id.clone(), config_hash).await?;
    update_session_master_cmd(
        ctx.db.clone(),
        params.session_id.clone(),
        params.cmd.clone(),
    )
    .await?;

    let mut new_pid = None;
    let mut generation = None;
    match ctx.tmux_server.get_pane_pid(pane.clone()).await {
        Ok(pid) => {
            let recorded_generation = if let Some(claimed_generation) =
                params.claimed_master_generation
            {
                match record_spawned_master_runtime_after_claim(
                    &ctx.db,
                    &params.session_id,
                    &pane.0,
                    i64::from(pid),
                    claimed_generation,
                )? {
                    Some(generation) => generation,
                    None => {
                        let _ = ctx.tmux_server.kill_pane(pane.clone()).await;
                        tracing::warn!(
                            session_id = %params.session_id,
                            claimed_generation,
                            "claimed master spawn went stale after pane creation; killed orphan pane"
                        );
                        return Ok(SpawnMasterPaneOutcome {
                            pane_id: pane.0,
                            home_root: plan.home_root,
                            new_pid: Some(i64::from(pid)),
                            generation: None,
                        });
                    }
                }
            } else {
                record_spawned_master_runtime(&ctx.db, &params.session_id, &pane.0, i64::from(pid))?
            };
            new_pid = Some(i64::from(pid));
            generation = Some(recorded_generation);
            if arm_revival_watch
                && let Err(err) = arm_master_revival_watch(
                    ctx,
                    &params.session_id,
                    i64::from(pid),
                    recorded_generation,
                    &params.cmd,
                )
            {
                tracing::warn!(
                    session_id = %params.session_id,
                    pid,
                    generation = recorded_generation,
                    error = %err,
                    "failed to arm master revive watcher after spawn"
                );
            }
        }
        Err(err) => {
            tracing::warn!(session_id = %params.session_id, error = %err, "failed to get master pane pid");
        }
    }

    Ok(SpawnMasterPaneOutcome {
        pane_id: pane.0,
        home_root: plan.home_root,
        new_pid,
        generation,
    })
}

pub(super) fn arm_master_revival_watch(
    ctx: &Ctx,
    session_id: &str,
    expected_pid: i64,
    expected_generation: i64,
    master_cmd: &str,
) -> Result<(), CcbdError> {
    let expected_pid = i32::try_from(expected_pid)
        .map_err(|_| CcbdError::PtyIoError(format!("invalid master pid: {expected_pid}")))?;
    let pidfd = monitor::pidfd_open(expected_pid)?;
    let task_fd = pidfd
        .try_clone()
        .map_err(|err| CcbdError::PtyIoError(format!("clone master pidfd for watcher: {err}")))?;
    let key = master_monitor_key(session_id, expected_generation);
    monitor::register(key, pidfd);
    spawn_master_pidfd_watch_task(
        session_id.to_string(),
        i64::from(expected_pid),
        expected_generation,
        task_fd,
        ctx.db.clone(),
        ctx.tmux_server.clone(),
        master_cmd.to_string(),
        ctx.state_dir.clone(),
        ctx.env_state.clone(),
        ctx.daemon_unit.clone(),
    );
    Ok(())
}

pub async fn handle_session_list(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let all = params.get("all").and_then(Value::as_bool).unwrap_or(false);
    let sessions = list_session_summaries(ctx.db.clone()).await?;
    let sessions = sessions
        .into_iter()
        .filter(|session| all || !is_terminal_session_status(&session.status))
        .map(|session| {
            json!({
                "id": session.id,
                "project_id": session.project_id,
                "absolute_path": session.absolute_path,
                "status": session.status,
                "master_state": session.master_state,
                "master_pane_id": session.master_pane_id,
                "db_tracked_agents": session.db_tracked_agents,
                "active_agents": session.active_agents,
                "created_at": session.created_at,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({ "sessions": sessions }))
}

#[cfg(test)]
mod master_cutover_tests {
    use super::super::master_cutover::{
        MasterCutoverMasterParams, MasterCutoverRequest, rollback_master_cutover_scope,
        run_master_cutover_with_spawn, wait_for_master_readiness,
    };
    use super::*;
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{claim_next_job_sync, insert_job_sync, query_job_sync};
    use crate::db::master_cutovers::{
        MasterCutoverClaim, MasterCutoverUpdate, claim_master_cutover, get_active_master_cutover,
        get_master_cutover, update_master_cutover_spawn_metadata, update_master_cutover_state,
    };
    use crate::db::recovery::query_agent_spawn_spec_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::master_cutover::{
        HandoffBundleInput, claude_project_conversation_dir, write_handoff_bundle,
    };
    use crate::rpc::handlers::RealignAgentParams;
    use crate::sandbox::EnvState;
    use crate::tmux::TmuxServer;
    use serde_json::Map;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn terminal_session_status_allowlist_is_explicit() {
        assert!(is_terminal_session_status("KILLED"));
        assert!(is_terminal_session_status("FAILED"));
        assert!(is_terminal_session_status("CLOSED"));
        assert!(!is_terminal_session_status("ACTIVE"));
        assert!(!is_terminal_session_status("SPAWNING"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_list_filters_terminal_by_default_and_keeps_active_agents_alias() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        {
            let conn = ctx.db.conn();
            for (session_id, status, path) in [
                ("sess_active", "ACTIVE", "/tmp/active"),
                ("sess_closed", "CLOSED", "/tmp/closed"),
                ("sess_failed", "FAILED", "/tmp/failed"),
                ("sess_killed", "KILLED", "/tmp/killed"),
            ] {
                insert_session_sync(&conn, session_id, session_id, path).unwrap();
                conn.execute(
                    "UPDATE sessions SET status = ?1 WHERE id = ?2",
                    (status, session_id),
                )
                .unwrap();
            }
            insert_agent_sync(
                &conn,
                "agent_active",
                "sess_active",
                "bash",
                "IDLE",
                Some(123),
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "agent_crashed",
                "sess_active",
                "bash",
                "CRASHED",
                Some(456),
            )
            .unwrap();
        }

        let default = handle_session_list(json!({}), &ctx).await.unwrap();
        let sessions = default["sessions"].as_array().unwrap();
        assert_eq!(sessions.len(), 1, "default session.list: {default}");
        assert_eq!(sessions[0]["id"], "sess_active");
        assert_eq!(sessions[0]["db_tracked_agents"], 1);
        assert_eq!(sessions[0]["active_agents"], 1);
        assert_eq!(
            sessions[0]["db_tracked_agents"], sessions[0]["active_agents"],
            "active_agents must remain a compatibility alias"
        );
        assert!(
            sessions[0].get("live_agents").is_none(),
            "live_agents remains RuntimeSnapshot-only, not session.list: {default}"
        );

        let all = handle_session_list(json!({"all": true}), &ctx)
            .await
            .unwrap();
        let statuses = all["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|session| session["status"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(statuses, vec!["ACTIVE", "CLOSED", "FAILED", "KILLED"]);
    }

    #[test]
    fn terminal_agent_cleanup_rolls_back_when_job_transition_insert_fails() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "sess_terminal_atomic", "p1", "/tmp/foo").unwrap();
            conn.execute(
                "UPDATE sessions SET status = 'FAILED' WHERE id = 'sess_terminal_atomic'",
                [],
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "agent_terminal_atomic",
                "sess_terminal_atomic",
                "bash",
                "IDLE",
                Some(123),
            )
            .unwrap();
            insert_job_sync(
                &conn,
                "job_terminal_atomic",
                "agent_terminal_atomic",
                None,
                "one",
            )
            .unwrap();
        }
        claim_next_job_sync(&db, "agent_terminal_atomic")
            .unwrap()
            .unwrap();

        crate::db::jobs::fail_next_job_transition_for_test();
        let err = mark_terminal_session_agents_killed_db_only(
            &db,
            "sess_terminal_atomic",
            "SESSION_KILL",
        )
        .unwrap_err();
        assert!(err.to_string().contains("forced job transition failure"));

        let conn = db.conn();
        let agent_state: String = conn
            .query_row(
                "SELECT state FROM agents WHERE id = 'agent_terminal_atomic'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let job = query_job_sync(&conn, "job_terminal_atomic")
            .unwrap()
            .unwrap();
        let failed_transition_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM job_transitions
                 WHERE job_id = 'job_terminal_atomic' AND new_status = 'FAILED'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(agent_state, "IDLE");
        assert_eq!(job.status, "DISPATCHED");
        assert_eq!(failed_transition_count, 0);
    }

    fn test_ctx(state_dir: PathBuf) -> Ctx {
        let file = tempfile::NamedTempFile::new().unwrap();
        Ctx {
            db: db::init(file.path()).unwrap(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: false,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(&state_dir)),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        }
    }

    fn request(tmp: &tempfile::TempDir, old_home: &std::path::Path) -> MasterCutoverRequest {
        let shared_credentials_dir = tmp.path().join("shared-claude-credentials");
        std::fs::create_dir_all(&shared_credentials_dir).unwrap();
        MasterCutoverRequest {
            project_id: "ccbd-rust".to_string(),
            absolute_path: "/home/sevenx/coding/ccbd-rust".to_string(),
            cwd: PathBuf::from("/home/sevenx/coding/ccbd-rust"),
            old_home: old_home.to_path_buf(),
            old_master_pid: Some(111),
            ah_state_dir: Some(tmp.path().join("state")),
            ah_socket_path: tmp.path().join("state").join("ahd.sock"),
            master: MasterCutoverMasterParams {
                env: HashMap::new(),
                cmd: "claude --continue".to_string(),
                provider: Some("claude".to_string()),
                readiness_timeout_s: 1,
                hooks: HashMap::new(),
                plugins: Vec::new(),
                skills: Vec::new(),
                bundle: Vec::new(),
                settings: Map::new(),
                tmux_window_size: TmuxWindowSize::Fixed,
                claude_shared_credentials_dir: Some(shared_credentials_dir),
            },
            agents: vec![RealignAgentParams {
                agent_id: "w1".to_string(),
                provider: "bash".to_string(),
                env: HashMap::from([("WORKER_ENV".to_string(), "1".to_string())]),
                hooks: HashMap::new(),
                plugins: Vec::new(),
                skills: Vec::new(),
                bundle: Vec::new(),
                settings: Map::new(),
                bundle_digest: None,
                sandbox_overrides: Default::default(),
                hook_push_enabled: false,
                claude_shared_credentials_dir: None,
            }],
            wait: true,
            print_attach: true,
        }
    }

    fn seed_old_conversation(old_home: &std::path::Path) {
        let cwd = PathBuf::from("/home/sevenx/coding/ccbd-rust");
        let source_dir = claude_project_conversation_dir(old_home, &cwd);
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(source_dir.join("conversation.jsonl"), b"old conversation").unwrap();
    }

    fn seed_claude_credentials(home: &std::path::Path) {
        let path = home.join(".claude/.credentials.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
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
    }

    fn restore_env(key: &str, value: Option<std::ffi::OsString>) {
        unsafe {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn master_pane_plan_materializes_the_declared_provider_not_claude() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        create_session(
            ctx.db.clone(),
            "s_codex_master".to_string(),
            "p_codex_master".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        let params = SpawnMasterPaneParams {
            session_id: "s_codex_master".to_string(),
            cmd: "codex".to_string(),
            provider: Some("codex".to_string()),
            seat_env: HashMap::new(),
            tmux_window_size: TmuxWindowSize::Fixed,
            extensions: ExtensionConfig::default(),
            extra_env: HashMap::new(),
            claimed_master_generation: None,
            // A stray Claude credentials directory must not reach a codex seat.
            claude_shared_credentials_dir: Some(tmp.path().to_path_buf()),
        };

        let plan = prepare_master_pane_plan(&ctx, &params).await.unwrap();
        let home_root = plan.home_root.expect("master home root");

        assert_eq!(
            plan.master_env_vars.get("CODEX_HOME").map(String::as_str),
            Some(home_root.join(".codex").display().to_string().as_str())
        );
        assert!(!plan.master_env_vars.contains_key("CLAUDE_CONFIG_DIR"));
        assert!(
            !plan
                .master_env_vars
                .contains_key("CLAUDE_SECURESTORAGE_CONFIG_DIR")
        );
        assert!(
            !home_root.join(".claude").exists(),
            "a codex master must not be given a Claude home"
        );
        let rules = std::fs::read_to_string(home_root.join(".codex/AGENTS.md"))
            .expect("codex master rules document");
        assert!(
            rules.contains("ah master ack-ready"),
            "codex master must receive the master kernel"
        );
    }

    /// The master seat gets the same env treatment agents already had: the
    /// project env, the seat's own env on top of it, and ah's runtime variables
    /// on top of both — a project cannot redirect the seat's identity or socket.
    #[tokio::test(flavor = "current_thread")]
    async fn master_spawn_env_carries_project_and_seat_env_without_overriding_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(tmp.path().join("state"));
        ctx.env_state.unsafe_no_sandbox = true;
        create_session(
            ctx.db.clone(),
            "s_master_env".to_string(),
            "p_master_env".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        let params = SpawnMasterPaneParams {
            session_id: "s_master_env".to_string(),
            cmd: "sleep 60".to_string(),
            provider: None,
            seat_env: HashMap::from([
                ("STUDIO_MCP_URL".to_string(), "https://studio.example".to_string()),
                ("SHARED".to_string(), "from-seat".to_string()),
                ("AH_SESSION_ID".to_string(), "hijacked".to_string()),
            ]),
            tmux_window_size: TmuxWindowSize::Fixed,
            extensions: ExtensionConfig::default(),
            extra_env: HashMap::new(),
            claimed_master_generation: None,
            claude_shared_credentials_dir: None,
        };

        let plan = prepare_master_pane_plan(&ctx, &params).await.unwrap();

        assert_eq!(
            plan.master_env_vars.get("STUDIO_MCP_URL").map(String::as_str),
            Some("https://studio.example")
        );
        assert_eq!(
            plan.master_env_vars.get("SHARED").map(String::as_str),
            Some("from-seat")
        );
        assert_eq!(
            plan.master_env_vars.get("AH_SESSION_ID").map(String::as_str),
            Some("s_master_env"),
            "ah runtime identity must win over author-configured env"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn initial_master_spawn_env_contains_process_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(tmp.path().join("state"));
        ctx.env_state.unsafe_no_sandbox = true;
        create_session(
            ctx.db.clone(),
            "s_master_identity".to_string(),
            "p_master_identity".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        let params = SpawnMasterPaneParams {
            session_id: "s_master_identity".to_string(),
            cmd: "sleep 60".to_string(),
            provider: None,
            seat_env: HashMap::new(),
            tmux_window_size: TmuxWindowSize::Fixed,
            extensions: ExtensionConfig::default(),
            extra_env: HashMap::from([
                ("AH_ROLE".to_string(), "worker".to_string()),
                ("AH_SESSION_ID".to_string(), "wrong-session".to_string()),
                ("AH_AGENT_ID".to_string(), "wrong-agent".to_string()),
                ("CLAUDE_CODE_USE_GATEWAY".to_string(), "0".to_string()),
                (
                    "ANTHROPIC_AUTH_TOKEN".to_string(),
                    "not-a-gateway-token".to_string(),
                ),
                ("USER_FLAG".to_string(), "1".to_string()),
            ]),
            claimed_master_generation: None,
            claude_shared_credentials_dir: None,
        };

        let plan = prepare_master_pane_plan(&ctx, &params).await.unwrap();

        assert_eq!(
            plan.master_env_vars.get("AH_ROLE").map(String::as_str),
            Some("master")
        );
        assert_eq!(
            plan.master_env_vars
                .get("AH_SESSION_ID")
                .map(String::as_str),
            Some("s_master_identity")
        );
        assert!(!plan.master_env_vars.contains_key("AH_AGENT_ID"));
        assert_eq!(
            plan.master_env_vars.get("USER_FLAG").map(String::as_str),
            Some("1")
        );
        assert!(!plan.master_env_vars.contains_key("CLAUDE_CODE_USE_GATEWAY"));
        assert!(!plan.master_env_vars.contains_key("ANTHROPIC_AUTH_TOKEN"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn master_cutover_spawn_env_contains_process_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(tmp.path().join("state"));
        ctx.env_state.unsafe_no_sandbox = true;
        create_session(
            ctx.db.clone(),
            "s_cutover_identity".to_string(),
            "p_cutover_identity".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        let params = SpawnMasterPaneParams {
            session_id: "s_cutover_identity".to_string(),
            cmd: "claude --continue".to_string(),
            provider: None,
            seat_env: HashMap::new(),
            tmux_window_size: TmuxWindowSize::Fixed,
            extensions: ExtensionConfig::default(),
            extra_env: HashMap::from([
                ("AH_STATE_DIR".to_string(), "/tmp/ah-state".to_string()),
                (
                    "CCB_SOCKET".to_string(),
                    "/tmp/ah-state/ahd.sock".to_string(),
                ),
                ("AH_CUTOVER_ID".to_string(), "cutover-1".to_string()),
                (
                    "AH_MASTER_HANDOFF".to_string(),
                    "/tmp/ah-state/cutovers/cutover-1/handoff.md".to_string(),
                ),
                ("AH_MASTER_ROLE".to_string(), "managed".to_string()),
                ("AH_AGENT_ID".to_string(), "wrong-agent".to_string()),
            ]),
            claimed_master_generation: None,
            claude_shared_credentials_dir: None,
        };

        let plan = prepare_master_pane_plan(&ctx, &params).await.unwrap();

        assert_eq!(
            plan.master_env_vars.get("AH_ROLE").map(String::as_str),
            Some("master")
        );
        assert_eq!(
            plan.master_env_vars
                .get("AH_SESSION_ID")
                .map(String::as_str),
            Some("s_cutover_identity")
        );
        assert!(!plan.master_env_vars.contains_key("AH_AGENT_ID"));
        assert_eq!(
            plan.master_env_vars
                .get("AH_MASTER_ROLE")
                .map(String::as_str),
            Some("managed")
        );
    }

    fn spawn_ack_when_verifying(ctx: &Ctx) {
        let db = ctx.db.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                let cutover_id = {
                    let conn = db.conn();
                    conn.query_row(
                        "SELECT id FROM master_cutovers WHERE state = 'VERIFYING' LIMIT 1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .ok()
                };
                if let Some(cutover_id) = cutover_id {
                    let _ = crate::db::master_cutovers::mark_master_cutover_ack_ready(
                        &db,
                        &cutover_id,
                        "ack",
                    );
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        });
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(global_env)]
    async fn terminal_session_kill_does_not_touch_stale_master_pane_collision() {
        which::which("tmux").expect("tmux binary required for isolated tmux regression");
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let live_session = crate::tmux::master_session_name("p_live_collision");
        ctx.tmux_server
            .ensure_session(live_session.clone(), tmp.path().to_path_buf())
            .await
            .unwrap();
        let live_pane = ctx
            .tmux_server
            .spawn_window(
                live_session.clone(),
                "live".to_string(),
                tmp.path().to_path_buf(),
                vec!["sh".into(), "-lc".into(), "sleep 60".into()],
            )
            .await
            .unwrap();
        let live_pid = ctx
            .tmux_server
            .get_pane_pid(live_pane.clone())
            .await
            .unwrap();

        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "sess_dead_collision",
                "p_dead_collision",
                tmp.path().join("dead").to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'FAILED', master_pane_id = ?1, master_pid = 111, master_generation = 1
                 WHERE id = 'sess_dead_collision'",
                [&live_pane.0],
            )
            .unwrap();
        }

        let result = handle_session_kill(
            json!({"session_id": "sess_dead_collision", "force": true}),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(result["state"], "KILLED");
        assert_eq!(
            ctx.tmux_server
                .get_pane_pid(live_pane.clone())
                .await
                .unwrap(),
            live_pid
        );
        let status: String = ctx
            .db
            .conn()
            .query_row(
                "SELECT status FROM sessions WHERE id = 'sess_dead_collision'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "KILLED");

        let _ = ctx.tmux_server.kill_session(live_session).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(global_env)]
    async fn terminal_session_kill_does_not_touch_live_agent_registry_collision() {
        use crate::agent_io::registry::{AgentIoEntry, contains, register};
        use std::sync::atomic::AtomicBool;

        which::which("tmux").expect("tmux binary required for isolated tmux regression");
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let agent_id = "a1";
        let live_agent_session = crate::tmux::agent_session_name(agent_id);
        ctx.tmux_server
            .ensure_session(live_agent_session.clone(), tmp.path().to_path_buf())
            .await
            .unwrap();
        let live_pane = ctx
            .tmux_server
            .spawn_window(
                live_agent_session.clone(),
                "live-agent".to_string(),
                tmp.path().to_path_buf(),
                vec!["sh".into(), "-lc".into(), "sleep 60".into()],
            )
            .await
            .unwrap();
        let live_pid = ctx
            .tmux_server
            .get_pane_pid(live_pane.clone())
            .await
            .unwrap();
        let fifo_path = ctx.state_dir.join("pipes").join("a1.fifo");
        std::fs::create_dir_all(fifo_path.parent().unwrap()).unwrap();
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        register(
            agent_id.to_string(),
            AgentIoEntry {
                session_id: "sess_live_registry_collision".to_string(),
                pane_id: live_pane.clone(),
                expected_pid: Some(i64::from(live_pid)),
                reader_handle,
                fifo_path,
                socket_name: ctx.tmux_server.socket_name().to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );

        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "sess_dead_registry_collision",
                "p_dead_registry_collision",
                tmp.path().join("dead").to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions SET status = 'FAILED' WHERE id = 'sess_dead_registry_collision'",
                [],
            )
            .unwrap();
            crate::db::agents::insert_agent_sync(
                &conn,
                agent_id,
                "sess_dead_registry_collision",
                "bash",
                "IDLE",
                Some(1234),
            )
            .unwrap();
        }

        let result = handle_session_kill(
            json!({"session_id": "sess_dead_registry_collision", "force": true}),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(result["state"], "KILLED");
        assert_eq!(result["killed_agents"], 1);
        assert_eq!(
            ctx.tmux_server
                .get_pane_pid(live_pane.clone())
                .await
                .unwrap(),
            live_pid
        );
        assert!(contains(agent_id), "live registry entry must remain");

        let _ = ctx.tmux_server.kill_session(live_agent_session).await;
        let _ = crate::agent_io::registry::remove(agent_id);
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(global_env)]
    async fn active_session_kill_skips_reused_master_session_without_pid_ownership() {
        which::which("tmux").expect("tmux binary required for isolated tmux regression");
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let project_id = "p_master_reuse";
        let session_id = "sess_old_master_reuse";
        let master_session = crate::tmux::master_session_name(project_id);
        ctx.tmux_server
            .ensure_session(master_session.clone(), tmp.path().to_path_buf())
            .await
            .unwrap();
        let live_pane = ctx
            .tmux_server
            .spawn_window(
                master_session.clone(),
                "live-master".to_string(),
                tmp.path().to_path_buf(),
                vec!["sh".into(), "-lc".into(), "sleep 60".into()],
            )
            .await
            .unwrap();
        let live_pid = ctx
            .tmux_server
            .get_pane_pid(live_pane.clone())
            .await
            .unwrap();

        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                session_id,
                project_id,
                tmp.path().join("old").to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions SET status = 'ACTIVE', master_pid = ?1, master_generation = 1 WHERE id = ?2",
                (i64::from(live_pid) + 100_000, session_id),
            )
            .unwrap();
        }

        let result = handle_session_kill(json!({"session_id": session_id}), &ctx)
            .await
            .unwrap();

        assert_eq!(result["state"], "KILLED");
        assert_eq!(
            ctx.tmux_server
                .get_pane_pid(live_pane.clone())
                .await
                .unwrap(),
            live_pid
        );

        let _ = ctx.tmux_server.kill_session(master_session).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(global_env)]
    async fn daemon_master_cutover_orders_seed_before_spawn_and_writes_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let old_home = tmp.path().join("old-home");
        let source_home = tmp.path().join("source-home");
        std::fs::create_dir_all(&source_home).unwrap();
        seed_claude_credentials(&source_home);
        seed_old_conversation(&old_home);
        let previous_home = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", &source_home);
        }
        let ctx = test_ctx(tmp.path().join("state"));
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_home = Arc::new(Mutex::new(None::<PathBuf>));
        let order_for_spawn = order.clone();
        let seen_home_for_spawn = seen_home.clone();
        let current_pid = i64::from(std::process::id());

        let response = run_master_cutover_with_spawn(
            &ctx,
            request(&tmp, &old_home),
            move |_ctx, _params, plan| {
                let order = order_for_spawn.clone();
                let seen_home = seen_home_for_spawn.clone();
                Box::pin(async move {
                    spawn_ack_when_verifying(_ctx);
                    let master_home = plan.home_root.clone().expect("master home");
                    let provisioned = _ctx
                        .db
                        .conn()
                        .query_row("SELECT state FROM agents WHERE id = 'w1'", [], |row| {
                            row.get::<_, String>(0)
                        })
                        .expect("declared worker must be provisioned before master spawn");
                    assert_eq!(provisioned, "SPAWNING");
                    let stored = query_agent_spawn_spec_sync(&_ctx.db.conn(), "w1")
                        .unwrap()
                        .expect("declared worker spawn spec must be persisted");
                    assert_eq!(stored.spec.provider, "bash");
                    assert_eq!(stored.spec.env["WORKER_ENV"], "1");
                    let seeded = claude_project_conversation_dir(
                        &master_home,
                        &PathBuf::from("/home/sevenx/coding/ccbd-rust"),
                    )
                    .join("conversation.jsonl");
                    assert!(seeded.exists(), "seed must land before spawn");
                    *seen_home.lock().unwrap() = Some(master_home);
                    order.lock().unwrap().push("spawn".to_string());
                    Ok(SpawnMasterPaneOutcome {
                        pane_id: "%42".to_string(),
                        home_root: plan.home_root,
                        new_pid: Some(current_pid),
                        generation: Some(7),
                    })
                })
            },
        )
        .await
        .unwrap();
        restore_env("HOME", previous_home);

        assert_eq!(response["pane_id"], "%42");
        let tmux_attach_command = response["tmux_attach_command"].as_str().unwrap();
        assert!(tmux_attach_command.starts_with("tmux -L "));
        assert!(tmux_attach_command.contains(ctx.tmux_server.socket_name()));
        assert!(
            !tmux_attach_command.contains("ahd.sock"),
            "tmux attach command must use the tmux socket, not the ahd RPC socket"
        );
        assert_eq!(*order.lock().unwrap(), vec!["spawn"]);
        let session_id = response["session_id"].as_str().unwrap();
        let active = get_active_master_cutover(&ctx.db, session_id)
            .unwrap()
            .expect("active cutover");
        assert_eq!(active.state, "ACTIVE");
        assert_eq!(active.new_master_pid, Some(current_pid));
        assert_eq!(active.new_master_generation, Some(7));
        assert_eq!(active.new_master_pane_id.as_deref(), Some("%42"));
        let master_home = seen_home.lock().unwrap().clone().unwrap();
        assert!(
            claude_project_conversation_dir(
                &master_home,
                &PathBuf::from("/home/sevenx/coding/ccbd-rust")
            )
            .join("conversation.jsonl")
            .exists()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(global_env)]
    async fn daemon_master_cutover_requires_ack_before_active() {
        let tmp = tempfile::tempdir().unwrap();
        let old_home = tmp.path().join("old-home");
        let source_home = tmp.path().join("source-home");
        std::fs::create_dir_all(&source_home).unwrap();
        seed_claude_credentials(&source_home);
        seed_old_conversation(&old_home);
        let previous_home = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", &source_home);
        }
        let ctx = test_ctx(tmp.path().join("state"));
        let current_pid = i64::from(std::process::id());

        let response = run_master_cutover_with_spawn(
            &ctx,
            request(&tmp, &old_home),
            move |ctx, _params, plan| {
                spawn_ack_when_verifying(ctx);
                Box::pin(async move {
                    Ok(SpawnMasterPaneOutcome {
                        pane_id: "%44".to_string(),
                        home_root: plan.home_root,
                        new_pid: Some(current_pid),
                        generation: Some(9),
                    })
                })
            },
        )
        .await
        .unwrap();
        restore_env("HOME", previous_home);

        let session_id = response["session_id"].as_str().unwrap();
        let active = get_active_master_cutover(&ctx.db, session_id)
            .unwrap()
            .expect("active cutover");
        assert_eq!(active.state, "ACTIVE");
        assert!(active.ack_ready_at.is_some());
        assert_eq!(active.readiness_mode.as_deref(), Some("ack"));
        assert_eq!(response["readiness_mode"], "ack");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_master_readiness_fails_fast_when_verifying_master_pid_is_dead() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let session_id = "s_dead_verify";
        let cutover_id = "cutover_dead_verify";
        create_session(
            ctx.db.clone(),
            session_id.to_string(),
            "p_dead_verify".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        assert_eq!(
            claim_master_cutover(
                &ctx.db,
                cutover_id,
                session_id,
                Some(111),
                &ctx.state_dir.display().to_string(),
                &ctx.state_dir.join("ahd.sock").display().to_string(),
                &ctx.state_dir
                    .join("cutovers")
                    .join(cutover_id)
                    .join("handoff.md")
                    .display()
                    .to_string(),
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );
        assert_eq!(
            update_master_cutover_state(&ctx.db, cutover_id, "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &ctx.db,
                cutover_id,
                "SPAWNING",
                "VERIFYING",
                999_999_999,
                3,
                "%404",
            )
            .unwrap(),
            MasterCutoverUpdate::Updated
        );

        let err = wait_for_master_readiness(&ctx, cutover_id, Duration::from_millis(200))
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("exited before readiness"),
            "expected process-death error, got {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readiness_times_out_without_ack_from_master() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let session_id = "s_probe_timeout";
        let cutover_id = "cutover_probe_timeout";
        create_session(
            ctx.db.clone(),
            session_id.to_string(),
            "p_probe_timeout".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        assert_eq!(
            claim_master_cutover(
                &ctx.db,
                cutover_id,
                session_id,
                Some(111),
                &ctx.state_dir.display().to_string(),
                &ctx.state_dir.join("ahd.sock").display().to_string(),
                &ctx.state_dir
                    .join("cutovers")
                    .join(cutover_id)
                    .join("handoff.md")
                    .display()
                    .to_string(),
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );
        assert_eq!(
            update_master_cutover_state(&ctx.db, cutover_id, "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &ctx.db,
                cutover_id,
                "SPAWNING",
                "VERIFYING",
                i64::from(std::process::id()),
                4,
                "%missing",
            )
            .unwrap(),
            MasterCutoverUpdate::Updated
        );

        let err = wait_for_master_readiness(&ctx, cutover_id, Duration::from_millis(120))
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("readiness timed out"),
            "expected ack timeout, got {err}"
        );
        let cutover = get_master_cutover(&ctx.db, cutover_id)
            .unwrap()
            .expect("cutover row");
        assert_eq!(cutover.state, "VERIFYING");
        assert!(cutover.ack_ready_at.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_master_pane_does_not_arm_revival_watch_before_active() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(tmp.path().join("state"));
        ctx.env_state.unsafe_no_sandbox = true;
        create_session(
            ctx.db.clone(),
            "s_watch_delay".to_string(),
            "p_watch_delay".to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();

        let params = SpawnMasterPaneParams {
            session_id: "s_watch_delay".to_string(),
            cmd: "sleep 60".to_string(),
            provider: None,
            seat_env: HashMap::new(),
            tmux_window_size: TmuxWindowSize::Fixed,
            extensions: ExtensionConfig::default(),
            extra_env: HashMap::new(),
            claimed_master_generation: None,
            claude_shared_credentials_dir: None,
        };
        let plan = prepare_master_pane_plan(&ctx, &params).await.unwrap();
        let outcome = spawn_prepared_master_pane(&ctx, params, plan, false)
            .await
            .unwrap();
        let generation = outcome.generation.expect("master generation");
        let key = master_monitor_key("s_watch_delay", generation);
        let armed_before_active = monitor::remove(&key).is_some();
        let _ = ctx
            .tmux_server
            .kill_pane(TmuxPaneId::parse(&outcome.pane_id).unwrap())
            .await;

        assert!(
            !armed_before_active,
            "master revive watcher must not arm before cutover reaches ACTIVE"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rollback_master_cutover_scope_kills_new_pane_but_keeps_old_master_session() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let project_id = "p_scope_rollback";
        let session_id = "s_scope_rollback";
        let cutover_id = "cutover_scope_rollback";
        create_session(
            ctx.db.clone(),
            session_id.to_string(),
            project_id.to_string(),
            tmp.path().display().to_string(),
        )
        .await
        .unwrap();
        {
            let conn = ctx.db.conn();
            crate::db::agents::insert_agent_sync(
                &conn,
                "w_scope_rollback",
                session_id,
                "bash",
                "SPAWNING",
                Some(1234),
            )
            .unwrap();
        }
        let master_session = master_session_name(project_id);
        ctx.tmux_server
            .ensure_session(master_session.clone(), tmp.path().to_path_buf())
            .await
            .unwrap();
        let _old_pane = ctx
            .tmux_server
            .spawn_window(
                master_session.clone(),
                "old-master".to_string(),
                tmp.path().to_path_buf(),
                vec!["sleep".to_string(), "60".to_string()],
            )
            .await
            .unwrap();
        let new_pane = ctx
            .tmux_server
            .spawn_window(
                master_session.clone(),
                "new-master".to_string(),
                tmp.path().to_path_buf(),
                vec!["sleep".to_string(), "60".to_string()],
            )
            .await
            .unwrap();
        let handoff_dir = ctx.state_dir.join("cutovers").join(cutover_id);
        std::fs::create_dir_all(&handoff_dir).unwrap();
        std::fs::write(handoff_dir.join("handoff.md"), b"handoff").unwrap();
        let master_sandbox = ctx
            .state_dir
            .join("sandboxes")
            .join(session_id)
            .join("master");
        let worker_sandbox = ctx
            .state_dir
            .join("sandboxes")
            .join(session_id)
            .join("w_scope_rollback");
        std::fs::create_dir_all(&master_sandbox).unwrap();
        std::fs::create_dir_all(&worker_sandbox).unwrap();
        assert_eq!(
            claim_master_cutover(
                &ctx.db,
                cutover_id,
                session_id,
                Some(111),
                &ctx.state_dir.display().to_string(),
                &ctx.state_dir.join("ahd.sock").display().to_string(),
                &handoff_dir.join("handoff.md").display().to_string(),
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );
        assert_eq!(
            update_master_cutover_state(&ctx.db, cutover_id, "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &ctx.db,
                cutover_id,
                "SPAWNING",
                "VERIFYING",
                9999,
                77,
                &new_pane.0,
            )
            .unwrap(),
            MasterCutoverUpdate::Updated
        );

        rollback_master_cutover_scope(&ctx, cutover_id, session_id)
            .await
            .unwrap();

        assert!(
            ctx.tmux_server
                .window_exists(master_session.clone(), "old-master".to_string())
                .await
                .unwrap(),
            "rollback must not kill the shared master tmux session or old master window"
        );
        assert!(!handoff_dir.exists());
        assert!(!master_sandbox.exists());
        assert!(!worker_sandbox.exists());
        let cutover = get_master_cutover(&ctx.db, cutover_id)
            .unwrap()
            .expect("cutover row");
        assert_eq!(cutover.state, "FAILED");

        let _ = ctx.tmux_server.kill_session(master_session).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(global_env)]
    async fn daemon_master_cutover_spawn_failure_marks_failed_and_keeps_old_master() {
        let tmp = tempfile::tempdir().unwrap();
        let old_home = tmp.path().join("old-home");
        let source_home = tmp.path().join("source-home");
        std::fs::create_dir_all(&source_home).unwrap();
        seed_claude_credentials(&source_home);
        seed_old_conversation(&old_home);
        let previous_home = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", &source_home);
        }
        let ctx = test_ctx(tmp.path().join("state"));
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let calls_for_spawn = calls.clone();

        let err = run_master_cutover_with_spawn(
            &ctx,
            request(&tmp, &old_home),
            move |_ctx, _params, _plan| {
                let calls = calls_for_spawn.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push("spawn".to_string());
                    Err(CcbdError::EnvironmentNotSupported {
                        details: "injected spawn failure".to_string(),
                    })
                })
            },
        )
        .await
        .unwrap_err();
        restore_env("HOME", previous_home);

        assert!(err.to_string().contains("injected spawn failure"));
        assert_eq!(*calls.lock().unwrap(), vec!["spawn"]);
        let conn = ctx.db.conn();
        let state: String = conn
            .query_row("SELECT state FROM master_cutovers", [], |row| row.get(0))
            .unwrap();
        assert_eq!(state, "FAILED");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_session_master_cutover_method_registered() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let response = crate::rpc::router::dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "session.master_cutover",
                "params": {},
                "id": 77
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(obj["id"], 77);
        assert_eq!(obj["error"]["code"], -32000);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_master_ack_ready_method_registered_and_rejects_missing_cutover() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(tmp.path().join("state"));
        let response = crate::rpc::router::dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "master.ack_ready",
                "params": {"cutover_id": "cutover_missing"},
                "id": 78
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(obj["id"], 78);
        assert_eq!(obj["error"]["code"], -32000);
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("not in VERIFYING")
        );
    }

    #[test]
    fn handoff_and_master_rules_include_ack_ready_instruction() {
        let tmp = tempfile::tempdir().unwrap();
        let handoff = write_handoff_bundle(
            tmp.path(),
            &HandoffBundleInput {
                cutover_id: "cutover_rules",
                session_id: "sess_rules",
                socket_path: &tmp.path().join("ahd.sock"),
                state_dir: tmp.path(),
            },
        )
        .unwrap();
        let handoff_body = std::fs::read_to_string(handoff).unwrap();
        assert!(handoff_body.contains("ah master ack-ready --cutover-id \"$AH_CUTOVER_ID\""));
        assert!(
            crate::provider::builtin::MASTER_KERNEL
                .contains("ah master ack-ready --cutover-id \"$AH_CUTOVER_ID\"")
        );
    }
}
