use super::params::required_str;
use super::sessions::{
    MasterPanePlan, SpawnMasterPaneOutcome, SpawnMasterPaneParams, arm_master_revival_watch,
    prepare_master_pane_plan, spawn_prepared_master_pane,
};
use crate::db::master_cutovers::{
    MasterCutoverClaim, MasterCutoverUpdate, claim_master_cutover, get_master_cutover,
    mark_master_cutover_ack_ready, update_master_cutover_spawn_metadata,
    update_master_cutover_state,
};
use crate::db::sessions::{create_session, master_tell_begin, master_tell_failed};
use crate::db::system::{remove_agent_sandbox_dir_sync, session_agent_ids};
use crate::error::CcbdError;
use crate::master_cutover::{
    HandoffBundleInput, seed_claude_project_conversation, write_handoff_bundle,
};
use crate::monitor::master_watch::master_process_is_alive;
use crate::provider::extensions::ExtensionConfig;
use crate::provider::fingerprint::{ConfigFingerprintInput, ConfigRole, compute_config_hash};
use crate::rpc::Ctx;
use crate::rpc::handlers::{RealignAgentParams, spawn_realign_agent};
use crate::tmux::{TmuxPaneId, TmuxWindowSize, master_session_name};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use uuid::Uuid;

pub(super) async fn rollback_master_cutover_scope(
    ctx: &Ctx,
    cutover_id: &str,
    session_id: &str,
) -> Result<(), CcbdError> {
    let cutover = match get_master_cutover(&ctx.db, cutover_id) {
        Ok(cutover) => cutover,
        Err(err) => {
            tracing::warn!(%cutover_id, error = %err, "failed to load master cutover during rollback");
            None
        }
    };

    if let Some(cutover) = cutover.as_ref() {
        if let Some(pane_id) = cutover.new_master_pane_id.as_deref() {
            match TmuxPaneId::parse(pane_id) {
                Ok(pane_id) => {
                    if let Some(expected_pid) = cutover.new_master_pid {
                        let _ = ctx
                            .tmux_server
                            .kill_pane_if_owned(pane_id, expected_pid)
                            .await;
                    } else {
                        tracing::warn!(%cutover_id, pane_id = %pane_id.0, "cutover has no expected master pid; skipping pane kill during rollback");
                    }
                }
                Err(err) => {
                    tracing::warn!(%cutover_id, pane_id, error = %err, "invalid cutover master pane id during rollback");
                }
            }
        } else {
            tracing::warn!(%cutover_id, "master cutover rollback has no new master pane id");
        }
    } else {
        tracing::warn!(%cutover_id, "master cutover rollback has no cutover row; cannot kill new master pane");
    }

    if let Some(cutover) = cutover.as_ref()
        && matches!(
            cutover.state.as_str(),
            "PREPARING" | "SPAWNING" | "VERIFYING"
        )
    {
        if let Err(err) = update_master_cutover_state(&ctx.db, cutover_id, &cutover.state, "FAILED")
        {
            tracing::warn!(%cutover_id, error = %err, "failed to mark master cutover FAILED during rollback");
        }
    }

    if let Some(cutover) = cutover.as_ref() {
        let handoff_path = PathBuf::from(&cutover.handoff_path);
        if let Some(handoff_dir) = handoff_path.parent()
            && let Err(err) = std::fs::remove_dir_all(handoff_dir)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%cutover_id, path = %handoff_dir.display(), error = %err, "failed to remove master cutover handoff dir");
        }
    }

    let agent_ids = match session_agent_ids(ctx.db.clone(), session_id.to_string()).await {
        Ok(agent_ids) => agent_ids,
        Err(err) => {
            tracing::warn!(%session_id, %cutover_id, error = %err, "failed to list session agents during master cutover rollback");
            Vec::new()
        }
    };
    for agent_id in agent_ids {
        crate::agent_io::cleanup_agent_runtime_resources(&agent_id, Some(session_id));
        remove_agent_sandbox_dir_sync(&ctx.state_dir, session_id, &agent_id);
    }

    if let Some(cutover) = cutover.as_ref()
        && let Some(generation) = cutover.new_master_generation
    {
        let _ = crate::master_revival::remove_master_monitor_key_if_generation_matches(
            session_id, generation,
        );
    }

    remove_agent_sandbox_dir_sync(&ctx.state_dir, session_id, "master");

    {
        let conn = ctx.db.conn();
        match conn.execute(
            "UPDATE sessions SET status = 'FAILED' WHERE id = ?1 AND status = 'ACTIVE'",
            [session_id],
        ) {
            Ok(changes) => {
                if changes > 0 {
                    crate::orchestrator::pubsub::notify_runtime_changed(
                        crate::runtime_events::RuntimeSnapshotReason::InventoryChanged,
                    );
                }
            }
            Err(err) => {
                tracing::warn!(%session_id, %cutover_id, error = %err, "failed to mark rollback session FAILED");
            }
        }
    }

    // TODO(followup): startup reconcile in-flight cutovers.
    Ok(())
}

#[derive(Debug, Deserialize)]
pub(super) struct MasterCutoverMasterParams {
    #[serde(default)]
    pub(super) cmd: String,
    #[serde(default)]
    pub(super) provider: Option<String>,
    #[serde(default)]
    pub(super) env: HashMap<String, String>,
    #[serde(default = "default_master_readiness_timeout_s")]
    pub(super) readiness_timeout_s: u64,
    #[serde(default)]
    pub(super) hooks: HashMap<String, Vec<crate::provider::extensions::HookGroup>>,
    #[serde(default)]
    pub(super) plugins: Vec<String>,
    #[serde(default)]
    pub(super) skills: Vec<String>,
    #[serde(default)]
    pub(super) bundle: Vec<String>,
    #[serde(default)]
    pub(super) settings: Map<String, Value>,
    #[serde(default)]
    pub(super) tmux_window_size: TmuxWindowSize,
    #[serde(default)]
    pub(super) claude_shared_credentials_dir: Option<PathBuf>,
}

fn default_master_readiness_timeout_s() -> u64 {
    120
}

/// Master readiness has exactly one form: the successor master reports it
/// itself by running `ah master ack-ready`. Terminal output is never a readiness
/// signal — a settled pane says the screen stopped changing, not that the
/// successor loaded the handoff.
pub(super) const MASTER_READINESS_MODE: &str = "ack";

/// Fails when the master's provider cannot report readiness, naming the missing
/// capability instead of falling back to a guess.
fn ensure_master_can_ack_readiness(provider: Option<&str>, cmd: &str) -> Result<(), CcbdError> {
    let provider = crate::cli::config::resolve_master_provider(provider, cmd);
    let can_ack = crate::provider::manifest::provider_capabilities(&provider)
        .is_some_and(|capabilities| capabilities.readiness_ack);
    if can_ack {
        return Ok(());
    }
    Err(CcbdError::EnvironmentNotSupported {
        details: format!(
            "master cutover needs readiness reported by the master seat, but provider \
             '{provider}' does not declare the 'readiness_ack' capability"
        ),
    })
}

fn readiness_timeout(readiness_timeout_s: u64) -> Duration {
    Duration::from_secs(readiness_timeout_s.clamp(30, 600))
}

pub(super) async fn wait_for_master_readiness(
    ctx: &Ctx,
    cutover_id: &str,
    timeout: Duration,
) -> Result<String, CcbdError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let cutover = get_master_cutover(&ctx.db, cutover_id)?.ok_or_else(|| {
            CcbdError::IpcInvalidRequest(format!("master cutover not found: {cutover_id}"))
        })?;
        if cutover.state != "VERIFYING" {
            return Err(CcbdError::IpcInvalidRequest(format!(
                "master cutover {cutover_id} left VERIFYING before readiness"
            )));
        }
        if cutover.ack_ready_at.is_some() {
            return Ok(cutover
                .readiness_mode
                .unwrap_or_else(|| MASTER_READINESS_MODE.to_string()));
        }
        if cutover
            .new_master_pid
            .is_some_and(|pid| !master_process_is_alive(pid))
        {
            return Err(CcbdError::IpcInvalidRequest(format!(
                "master cutover {cutover_id} master process exited before readiness"
            )));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CcbdError::IpcInvalidRequest(format!(
                "master cutover {cutover_id} readiness timed out"
            )));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct MasterCutoverRequest {
    pub(super) project_id: String,
    pub(super) absolute_path: String,
    pub(super) cwd: PathBuf,
    pub(super) old_home: PathBuf,
    pub(super) old_master_pid: Option<i64>,
    pub(super) ah_state_dir: Option<PathBuf>,
    pub(super) ah_socket_path: PathBuf,
    pub(super) master: MasterCutoverMasterParams,
    #[serde(default)]
    pub(super) agents: Vec<RealignAgentParams>,
    #[serde(default)]
    pub(super) wait: bool,
    #[serde(default)]
    pub(super) print_attach: bool,
}

type SpawnMasterFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SpawnMasterPaneOutcome, CcbdError>> + Send + 'a>>;

pub async fn handle_session_master_cutover(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let request: MasterCutoverRequest = serde_json::from_value(params).map_err(|err| {
        CcbdError::IpcInvalidRequest(format!("invalid master cutover params: {err}"))
    })?;
    run_master_cutover_with_spawn(ctx, request, |ctx, params, plan| {
        Box::pin(spawn_prepared_master_pane(ctx, params, plan, false))
    })
    .await
}

pub async fn handle_master_ack_ready(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let cutover_id = required_str(&params, "cutover_id")?;
    match mark_master_cutover_ack_ready(&ctx.db, cutover_id, "ack")? {
        MasterCutoverUpdate::Updated => Ok(json!({
            "cutover_id": cutover_id,
            "readiness_mode": "ack",
            "ack_ready": true,
        })),
        MasterCutoverUpdate::Stale => Err(CcbdError::IpcInvalidRequest(format!(
            "master cutover {cutover_id} not in VERIFYING"
        ))),
    }
}

pub(super) async fn run_master_cutover_with_spawn<S>(
    ctx: &Ctx,
    request: MasterCutoverRequest,
    spawn: S,
) -> Result<Value, CcbdError>
where
    S: for<'a> Fn(&'a Ctx, SpawnMasterPaneParams, MasterPanePlan) -> SpawnMasterFuture<'a>,
{
    // Refuse before creating any state: a master that cannot report readiness
    // would leave the cutover stuck in VERIFYING until it timed out.
    ensure_master_can_ack_readiness(request.master.provider.as_deref(), &request.master.cmd)?;
    let session_id = format!("sess_{}", Uuid::new_v4());
    let cutover_id = format!("cutover-{session_id}");
    let state_dir = request
        .ah_state_dir
        .clone()
        .unwrap_or_else(|| ctx.state_dir.clone());
    let handoff_path = state_dir
        .join("cutovers")
        .join(&cutover_id)
        .join("handoff.md");
    tracing::info!(%session_id, %cutover_id, "master cutover creating session");
    create_session(
        ctx.db.clone(),
        session_id.clone(),
        request.project_id.clone(),
        request.absolute_path.clone(),
    )
    .await?;

    match claim_master_cutover(
        &ctx.db,
        &cutover_id,
        &session_id,
        request.old_master_pid,
        &state_dir.display().to_string(),
        &request.ah_socket_path.display().to_string(),
        &handoff_path.display().to_string(),
    )? {
        MasterCutoverClaim::Claimed => {
            tracing::info!(%session_id, %cutover_id, "master cutover claimed PREPARING");
        }
        MasterCutoverClaim::AlreadyActive => {
            tracing::warn!(%session_id, %cutover_id, "master cutover rejected; active cutover already exists");
            return Err(CcbdError::IpcInvalidRequest(format!(
                "active master cutover already exists for session {session_id}"
            )));
        }
    }

    let mut current_state = "PREPARING";
    let result = async {
        let mut extra_env = HashMap::from([
            ("AH_STATE_DIR".to_string(), state_dir.display().to_string()),
            (
                "CCB_SOCKET".to_string(),
                request.ah_socket_path.display().to_string(),
            ),
            ("AH_CUTOVER_ID".to_string(), cutover_id.clone()),
            (
                "AH_MASTER_HANDOFF".to_string(),
                handoff_path.display().to_string(),
            ),
            ("AH_MASTER_ROLE".to_string(), "managed".to_string()),
        ]);
        let spawn_params = SpawnMasterPaneParams {
            session_id: session_id.clone(),
            cmd: request.master.cmd.clone(),
            provider: request.master.provider.clone(),
            seat_env: super::sessions::merge_seat_env(
                HashMap::new(),
                request.master.env.clone(),
            ),
            tmux_window_size: request.master.tmux_window_size,
            extensions: ExtensionConfig {
                hooks: request.master.hooks.clone(),
                plugins: request.master.plugins.clone(),
                skills: request.master.skills.clone(),
                bundle: request.master.bundle.clone(),
                settings: request.master.settings.clone(),
                ..Default::default()
            },
            extra_env: std::mem::take(&mut extra_env),
            claimed_master_generation: None,
            claude_shared_credentials_dir: request.master.claude_shared_credentials_dir.clone(),
        };
        let plan = prepare_master_pane_plan(ctx, &spawn_params).await?;
        let master_home = plan.home_root.clone().ok_or_else(|| {
            CcbdError::EnvironmentNotSupported {
                details: "master cutover requires sandboxed master home".to_string(),
            }
        })?;
        tracing::info!(%session_id, %cutover_id, master_home = %master_home.display(), "master cutover seeding handoff before spawn");
        write_handoff_bundle(
            &state_dir,
            &HandoffBundleInput {
                cutover_id: &cutover_id,
                session_id: &session_id,
                socket_path: &request.ah_socket_path,
                state_dir: &state_dir,
            },
        )?;
        let _seed = seed_claude_project_conversation(
            &request.old_home,
            &master_home,
            &request.cwd,
            &handoff_path,
        )?;
        for agent in &request.agents {
            let expected_hash = compute_config_hash(&ConfigFingerprintInput {
                role: ConfigRole::Agent {
                    provider: &agent.provider,
                    env: &agent.env,
                },
                hooks: &agent.hooks,
                plugins: &agent.plugins,
                skills: &agent.skills,
                settings: &agent.settings,
                bundle: agent.bundle_digest.as_ref(),
            })?;
            tracing::info!(
                %session_id,
                %cutover_id,
                agent_id = %agent.agent_id,
                provider = %agent.provider,
                "master cutover provisioning declared worker before master spawn"
            );
            // ISSUE-13 §3a: cutover computes expected_hash over agent.env (no project
            // config_env in the cutover request); an empty config_env keeps the server-side
            // merge equal to that, so agent.rs's stored hash matches expected_hash.
            let config_env = HashMap::new();
            spawn_realign_agent(
                ctx,
                &session_id,
                agent,
                &expected_hash,
                false,
                false,
                &config_env,
                None,
            )
            .await?;
        }
        if update_master_cutover_state(&ctx.db, &cutover_id, "PREPARING", "SPAWNING")?
            != MasterCutoverUpdate::Updated
        {
            tracing::warn!(%session_id, %cutover_id, "master cutover PREPARING->SPAWNING CAS stale");
            return Err(CcbdError::IpcInvalidRequest("master cutover stale before spawn".into()));
        }
        current_state = "SPAWNING";
        tracing::info!(%session_id, %cutover_id, "master cutover spawning managed master");
        let outcome = spawn(ctx, spawn_params, plan).await?;
        let new_pid = outcome.new_pid.ok_or_else(|| {
            CcbdError::EnvironmentNotSupported {
                details: "master cutover spawn did not report new master pid".to_string(),
            }
        })?;
        let generation = outcome.generation.ok_or_else(|| {
            CcbdError::EnvironmentNotSupported {
                details: "master cutover spawn did not report new master generation".to_string(),
            }
        })?;
        if update_master_cutover_spawn_metadata(
            &ctx.db,
            &cutover_id,
            "SPAWNING",
            "VERIFYING",
            new_pid,
            generation,
            &outcome.pane_id,
        )? != MasterCutoverUpdate::Updated
        {
            tracing::warn!(%session_id, %cutover_id, "master cutover SPAWNING->VERIFYING CAS stale");
            return Err(CcbdError::IpcInvalidRequest("master cutover stale after spawn".into()));
        }
        current_state = "VERIFYING";
        let readiness_mode = wait_for_master_readiness(
            ctx,
            &cutover_id,
            readiness_timeout(request.master.readiness_timeout_s),
        )
        .await?;
        tracing::info!(%session_id, %cutover_id, readiness_mode, "master cutover readiness accepted");
        if update_master_cutover_state(&ctx.db, &cutover_id, "VERIFYING", "ACTIVE")?
            != MasterCutoverUpdate::Updated
        {
            tracing::warn!(%session_id, %cutover_id, "master cutover VERIFYING->ACTIVE CAS stale");
            return Err(CcbdError::IpcInvalidRequest("master cutover stale before ACTIVE".into()));
        }
        current_state = "ACTIVE";
        if let Err(err) =
            arm_master_revival_watch(ctx, &session_id, new_pid, generation, &request.master.cmd)
        {
            tracing::warn!(%session_id, %cutover_id, error = %err, "failed to arm master revive watcher after ACTIVE");
        }
        tracing::info!(%session_id, %cutover_id, "master cutover ACTIVE");
        let attach_command = format!("ah attach master --session {session_id}");
        let tmux_attach_command = format!(
            "tmux -L {} attach -t {}",
            ctx.tmux_server.socket_name(),
            master_session_name(&request.project_id)
        );
        let _ = (request.wait, request.print_attach);
        Ok(json!({
            "cutover_id": cutover_id,
            "session_id": session_id,
            "pane_id": outcome.pane_id,
            "attach_command": attach_command,
            "tmux_attach_command": tmux_attach_command,
            "handoff_path": handoff_path,
            "readiness_mode": readiness_mode,
        }))
    }
    .await;

    if let Err(err) = result.as_ref() {
        if current_state != "ACTIVE" {
            if let Err(rollback_err) =
                rollback_master_cutover_scope(ctx, &cutover_id, &session_id).await
            {
                tracing::warn!(%session_id, %cutover_id, error = %rollback_err, "master cutover rollback failed; possible live-master/sandbox leak");
            }
            tracing::warn!(%session_id, %cutover_id, state = current_state, error = %err, "master cutover failed; old master left running");
        }
    }
    result
}
pub async fn handle_master_tell_begin(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let session_id = required_str(&params, "session_id")?.to_string();
    let request_id = required_str(&params, "request_id")?.to_string();
    let pane_id = required_str(&params, "pane_id")?.to_string();
    let changes = master_tell_begin(ctx.db.clone(), session_id.clone(), request_id.clone()).await?;
    if changes == 0 {
        return Err(CcbdError::IpcInvalidRequest(format!(
            "active session not found: {session_id}"
        )));
    }
    tracing::info!(
        command = "ah.tell",
        target = "master",
        session_id = %session_id,
        pane_id = %pane_id,
        request_id = %request_id,
        stage = "BEGIN",
        result = "registered",
        "registered master tell request"
    );
    Ok(json!({
        "session_id": session_id,
        "request_id": request_id,
        "pane_id": pane_id,
        "registered": true,
    }))
}

pub async fn handle_master_tell_failed(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let session_id = required_str(&params, "session_id")?.to_string();
    let request_id = required_str(&params, "request_id")?.to_string();
    let pane_id = params.get("pane_id").and_then(Value::as_str).unwrap_or("");
    let stage = params
        .get("stage")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN");
    let reason = params
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let changes =
        master_tell_failed(ctx.db.clone(), session_id.clone(), request_id.clone()).await?;
    tracing::warn!(
        command = "ah.tell",
        target = "master",
        session_id = %session_id,
        pane_id = %pane_id,
        request_id = %request_id,
        stage = %stage,
        result = "delivery_failed",
        reason = %reason,
        cleared_pending = changes > 0,
        "master tell delivery failed"
    );
    Ok(json!({
        "session_id": session_id,
        "request_id": request_id,
        "cleared_pending": changes > 0,
    }))
}
