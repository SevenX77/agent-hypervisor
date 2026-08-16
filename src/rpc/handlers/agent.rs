use super::params::{extension_config_from_params, required_i64, required_str};
use super::sessions::session_window_lock;
use crate::db::agents::{
    insert_agent_with_lifecycle_id, query_agent, query_agent_state, update_agent_config_hash,
};
use crate::db::agents_lifecycle::mark_agent_killed;
use crate::db::events::{insert_event, query_event_by_request_id, query_events_since};
use crate::db::events_progress::record_send_progress;
use crate::db::sessions::{apply_master_notify_event, query_session_by_id};
use crate::db::state_machine::mark_agent_waiting_for_ack;
use crate::db::system::remove_agent_sandbox_dir_sync;
use crate::error::CcbdError;
use crate::home_materialization::{
    HomeLayoutRole, HookPushContext, is_ccb_sandbox_home,
    prepare_home_layout_with_extensions_for_slot_and_claude_credentials,
};
use crate::marker::{
    MarkerMatcher, PromptTimerScanContext, TimerKind, parser_registry, registry,
    spawn_marker_timer_task_with_prompt,
};
use crate::monitor;
use crate::monitor::agent_watch::spawn_agent_pidfd_watch_task;
use crate::provider::bundles::{BundleRole, resolve_bundles_for_provider};
use crate::provider::fingerprint::{ConfigFingerprintInput, ConfigRole, compute_config_hash};
use crate::provider::manifest::{compute_recovery_args, press_enter_after_paste};
use crate::rpc::Ctx;
use crate::runtime_observation::intake::{
    HookCompletionObservation, WorkingObservation, observe_hook_completion, observe_working,
};
use crate::sandbox::{SandboxOverrides, path, systemd};
use crate::tmux::agent_session_name;
#[cfg(unix)]
use nix::sys::stat::Mode;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub async fn handle_agent_spawn(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    handle_agent_spawn_with_recovery(params, ctx, false).await
}

pub(crate) enum AgentSpawnDbAction {
    InsertDefault,
    ReplaceKilledAndRequeue {
        expected_hash: String,
        captured_intent: Option<crate::db::recovery::AgentRecoveryIntent>,
    },
}

fn persist_agent_spawn_snapshot_after_success(
    db: &crate::db::Db,
    spec: crate::db::recovery::AgentSpawnSpec,
    config_hash: &str,
) -> Result<bool, CcbdError> {
    let conn = db.conn();
    match crate::db::recovery::persist_agent_spawn_spec_sync(&conn, &spec, config_hash) {
        Ok(()) => Ok(true),
        Err(err) => {
            tracing::error!(
                agent_id = %spec.agent_id,
                error = %err,
                "failed to persist agent spawn recovery snapshot after successful spawn"
            );
            Ok(false)
        }
    }
}

pub(super) async fn handle_agent_spawn_with_recovery(
    params: Value,
    ctx: &Ctx,
    is_recovery: bool,
) -> Result<Value, CcbdError> {
    handle_agent_spawn_with_db_action(params, ctx, is_recovery, AgentSpawnDbAction::InsertDefault)
        .await
}

pub(crate) async fn handle_agent_spawn_with_db_action(
    params: Value,
    ctx: &Ctx,
    is_recovery: bool,
    db_action: AgentSpawnDbAction,
) -> Result<Value, CcbdError> {
    let session_id = required_str(&params, "session_id")?;
    let agent_id = required_str(&params, "agent_id")?;
    if agent_id.starts_with("master:") {
        return Err(CcbdError::IpcInvalidRequest(
            "agent_id prefix 'master:' is reserved for master hook sentinels".to_string(),
        ));
    }
    let provider = required_str(&params, "provider")?;
    let manifest = crate::provider::manifest::try_get_manifest(provider)?;
    let lifecycle_id = crate::runtime_observation::store::new_lifecycle_id();
    let extensions = extension_config_from_params(&params)?;
    let extra_env_vars = match params.get("extra_env_vars") {
        Some(value) => {
            serde_json::from_value::<HashMap<String, String>>(value.clone()).map_err(|err| {
                CcbdError::IpcInvalidRequest(format!("invalid extra_env_vars: {err}"))
            })?
        }
        None => HashMap::new(),
    };
    // ISSUE-13 §3a: the project-level `[env]` (config_env) is carried raw and merged
    // SERVER-SIDE, so both spawn and realign build the fingerprinted env through the
    // same helper. Clients no longer pre-merge (which is what let the two sides diverge).
    let config_env = match params.get("config_env") {
        Some(value) => serde_json::from_value::<HashMap<String, String>>(value.clone())
            .map_err(|err| CcbdError::IpcInvalidRequest(format!("invalid config_env: {err}")))?,
        None => HashMap::new(),
    };
    let mut claude_shared_credentials_dir =
        super::sessions::optional_pathbuf(&params, "claude_shared_credentials_dir")?;
    let sandbox_overrides = match params.get("sandbox_overrides") {
        Some(value) => {
            serde_json::from_value::<SandboxOverrides>(value.clone()).map_err(|err| {
                CcbdError::IpcInvalidRequest(format!("invalid sandbox_overrides: {err}"))
            })?
        }
        None => SandboxOverrides::default(),
    };
    if let Some(existing) = query_agent(ctx.db.clone(), agent_id.to_string()).await?
        && existing.state != "KILLED"
    {
        return Err(CcbdError::AgentAlreadyExists(agent_id.to_string()));
    }

    let session = query_session_by_id(ctx.db.clone(), session_id.to_string())
        .await?
        .ok_or_else(|| {
            CcbdError::DbConstraintViolation(format!("session not found: {session_id}"))
        })?;
    let agent_cwd: std::path::PathBuf = session.absolute_path.clone().into();
    if manifest.provider_name == "claude" && claude_shared_credentials_dir.is_none() {
        claude_shared_credentials_dir = load_claude_shared_credentials_dir(&agent_cwd)?;
    }
    let resolved_bundles = resolve_bundles_for_provider(
        &agent_cwd,
        manifest.provider_name,
        BundleRole::Worker,
        &extensions,
    )?;
    let extensions = resolved_bundles.extensions;

    let sandbox_guard = if ctx.env_state.unsafe_no_sandbox {
        None
    } else {
        Some(path::SandboxDirGuard::new(path::resolve_sandbox_dir(
            &ctx.state_dir,
            session_id,
            agent_id,
            &agent_cwd,
        )?))
    };
    // ISSUE-13 §3b: capture the bare declared env (config_env ⊕ agent.env) BEFORE any
    // runtime injection. This clone is what feeds the config fingerprint; the injected
    // vars (CCB_SOCKET/AH_*/HOME/IS_SANDBOX) are added strictly after this point, so they
    // can never leak into the hash. `spawn_env_vars` remains the real (injected) process env.
    let bare_env = bare_config_env(&config_env, &extra_env_vars);
    let mut spawn_env_vars = build_agent_spawn_env_vars_for_hook_push(
        &ctx.state_dir,
        session_id,
        agent_id,
        bare_env.clone(),
    );
    let hook_push_enabled = hook_push_enabled_from_spawn_params(&params);
    let mut recovery_args = Vec::new();
    if let Some(dir) = sandbox_guard.as_ref().and_then(|guard| guard.path())
        && manifest.requires_home_materialization
    {
        let hook_push_ctx = HookPushContext {
            agent_id: agent_id.to_string(),
            provider: manifest.provider_name.to_string(),
            lifecycle_id: lifecycle_id.clone(),
            ahd_socket_path: ctx.state_dir.join("ahd.sock"),
            enabled: hook_push_enabled,
        };
        let home_overrides = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
            manifest.provider_name,
            dir,
            &agent_cwd,
            HomeLayoutRole::Worker,
            agent_id,
            &extensions,
            Some(&hook_push_ctx),
            claude_shared_credentials_dir.as_deref(),
        )?;
        if is_recovery {
            recovery_args =
                compute_recovery_args(manifest.provider_name, &home_overrides.home_root);
        }
        spawn_env_vars.extend(home_overrides.extra_env);
        if manifest.provider_name == "claude" {
            super::sessions::strip_claude_gateway_env(&mut spawn_env_vars);
        }
        inject_is_sandbox_env_if_needed(
            manifest.provider_name,
            &manifest.command,
            &home_overrides.home_root,
            &mut spawn_env_vars,
        );
    } else if is_recovery {
        recovery_args = compute_recovery_args(manifest.provider_name, &agent_cwd);
    }
    let cmd = systemd::wrap_command_with_recovery_and_sandbox_overrides(
        agent_id,
        &session.project_id,
        ctx.tmux_server.socket_name(),
        &ctx.env_state,
        systemd::RecoverySpawn {
            is_recovery,
            args: recovery_args,
        },
        ctx.daemon_unit.as_deref(),
        &manifest,
        &spawn_env_vars,
        &sandbox_overrides,
    );
    tracing::debug!(agent_id, provider = %manifest.provider_name, cmd_len = cmd.len(), "spawn cmd built");
    tracing::info!(agent_id = %agent_id, "spawn cmd: {}", cmd.join(" "));

    #[cfg(windows)]
    {
        let _ = (cmd, sandbox_guard);
        return Err(CcbdError::EnvironmentNotSupported {
            details: "Windows agent spawn requires the M2 ConPTY stream path".to_string(),
        });
    }

    #[cfg(unix)]
    {
        let fifo_dir = ctx.state_dir.join("pipes");
        if let Err(err) = fs::create_dir_all(&fifo_dir) {
            return Err(CcbdError::EnvironmentNotSupported {
                details: format!("create fifo dir {}: {err}", fifo_dir.display()),
            });
        }
        let fifo_path = fifo_dir.join(format!("{agent_id}.fifo"));
        let _ = fs::remove_file(&fifo_path);
        crate::db::common::spawn_db("agent_io::mkfifo", {
            let fifo_path = fifo_path.clone();
            move || {
                nix::unistd::mkfifo(&fifo_path, Mode::S_IRUSR | Mode::S_IWUSR).map_err(|err| {
                    CcbdError::TmuxCommandFailed {
                        cmd: format!("mkfifo {}", fifo_path.display()),
                        stderr: err.to_string(),
                        exit: -1,
                    }
                })
            }
        })
        .await?;
        let fifo_file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo_path)
        {
            Ok(file) => file,
            Err(err) => {
                let _ = fs::remove_file(&fifo_path);
                return Err(CcbdError::EnvironmentNotSupported {
                    details: format!("open fifo {}: {err}", fifo_path.display()),
                });
            }
        };

        let tmux = ctx.tmux_server.clone();
        let agent_session = agent_session_name(agent_id);
        let lock = session_window_lock(session_id);
        let pane_id = {
            let _guard = lock.lock().await;
            if let Err(err) = tmux
                .ensure_session(agent_session.clone(), agent_cwd.clone())
                .await
            {
                drop(fifo_file);
                let _ = fs::remove_file(&fifo_path);
                return Err(err);
            }
            match tmux
                .spawn_window(agent_session, agent_id.to_string(), agent_cwd, cmd)
                .await
            {
                Ok(pane_id) => pane_id,
                Err(err) => {
                    drop(fifo_file);
                    let _ = fs::remove_file(&fifo_path);
                    return Err(err);
                }
            }
        };
        let title = format!("{agent_id} ({provider})");
        let _ = tmux.set_pane_title(pane_id.clone(), &title).await;
        let pid = match tmux.get_pane_pid(pane_id.clone()).await {
            Ok(pid) => pid,
            Err(err) => {
                cleanup_spawn_resources(&tmux, Some(pane_id.clone()), Some(fifo_file), &fifo_path)
                    .await;
                return Err(err);
            }
        };
        if let Err(err) = tmux
            .pipe_pane_to_fifo(pane_id.clone(), fifo_path.clone())
            .await
        {
            cleanup_spawn_resources(&tmux, Some(pane_id.clone()), Some(fifo_file), &fifo_path)
                .await;
            return Err(err);
        }

        let pidfd = match monitor::pidfd_open(pid) {
            Ok(fd) => fd,
            Err(err) => {
                cleanup_spawn_resources(&tmux, Some(pane_id.clone()), Some(fifo_file), &fifo_path)
                    .await;
                return Err(err);
            }
        };
        let pidfd_for_task = match pidfd.try_clone() {
            Ok(fd) => fd,
            Err(err) => {
                cleanup_spawn_resources(&tmux, Some(pane_id.clone()), Some(fifo_file), &fifo_path)
                    .await;
                return Err(CcbdError::EnvironmentNotSupported {
                    details: format!("clone agent pidfd for {agent_id}: {err}"),
                });
            }
        };

        let config_hash = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Agent {
                provider,
                env: &bare_env,
            },
            hooks: &extensions.hooks,
            plugins: &extensions.plugins,
            skills: &extensions.skills,
            settings: &extensions.settings,
            bundle: extensions.bundle_digest.as_ref(),
        })?;
        let spawn_spec = crate::db::recovery::AgentSpawnSpec {
            agent_id: agent_id.to_string(),
            provider: provider.to_string(),
            // ISSUE-13 §3b/§3c: persist the BARE declared env, not the injected one. On
            // recovery/revive the snapshot is replayed as `extra_env_vars` and re-injected,
            // so a bare snapshot recomputes H(bare_env) == stored config_hash — keeping the
            // sole hash writer (agent.rs) consistent without the removed realign overwrite.
            env: bare_env.clone(),
            hooks: extensions.hooks.clone(),
            plugins: extensions.plugins.clone(),
            skills: extensions.skills.clone(),
            bundle: extensions.bundle.clone(),
            settings: extensions.settings.clone(),
            bundle_digest: extensions.bundle_digest.clone(),
            sandbox_overrides: sandbox_overrides.clone(),
            hook_push_enabled,
        };
        match db_action {
            AgentSpawnDbAction::InsertDefault => {
                if let Err(err) = insert_agent_with_lifecycle_id(
                    ctx.db.clone(),
                    agent_id.to_string(),
                    session_id.to_string(),
                    provider.to_string(),
                    lifecycle_id.clone(),
                    "SPAWNING".to_string(),
                    Some(pid as i64),
                )
                .await
                {
                    cleanup_spawn_resources(
                        &tmux,
                        Some(pane_id.clone()),
                        Some(fifo_file),
                        &fifo_path,
                    )
                    .await;
                    return Err(err);
                }
                update_agent_config_hash(ctx.db.clone(), agent_id.to_string(), config_hash.clone())
                    .await?;
                let _ =
                    persist_agent_spawn_snapshot_after_success(&ctx.db, spawn_spec, &config_hash)?;
            }
            AgentSpawnDbAction::ReplaceKilledAndRequeue {
                expected_hash,
                captured_intent,
            } => {
                if let Err(err) = crate::db::recovery::replace_killed_agent_and_requeue_job_sync(
                    &ctx.db,
                    session_id,
                    &spawn_spec,
                    &expected_hash,
                    pid as i64,
                    &lifecycle_id,
                    captured_intent.as_ref(),
                ) {
                    cleanup_spawn_resources(
                        &tmux,
                        Some(pane_id.clone()),
                        Some(fifo_file),
                        &fifo_path,
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        monitor::register(agent_id.to_string(), pidfd);
        let parser_handle = Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0)));
        let matcher = Arc::new(MarkerMatcher::from_manifest(&manifest));
        parser_registry::register(agent_id.to_string(), parser_handle.clone());
        let idle_scan_enabled = Arc::new(AtomicBool::new(false));
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(128);
        let reader_handle =
            crate::agent_io::spawn_agent_io_reader_task(agent_id.to_string(), fifo_file, output_tx);
        crate::marker::spawn_perception_stream_processor_task(
            agent_id.to_string(),
            ctx.db.clone(),
            parser_handle.clone(),
            output_rx,
            crate::marker::PerceptionStreamConfig {
                matcher: matcher.clone(),
                stability_ms: manifest.stability_ms,
                idle_scan_enabled: idle_scan_enabled.clone(),
            },
        );
        let pane_id_for_startup = pane_id.clone();
        let response_pane_id = pane_id.0.clone();
        crate::agent_io::register(
            agent_id.to_string(),
            crate::agent_io::AgentIoEntry {
                session_id: session_id.to_string(),
                pane_id,
                expected_pid: Some(i64::from(pid)),
                reader_handle,
                fifo_path,
                socket_name: ctx.tmux_server.socket_name().to_string(),
                idle_scan_enabled: idle_scan_enabled.clone(),
            },
        );
        spawn_agent_pidfd_watch_task(
            agent_id.to_string(),
            session_id.to_string(),
            pid,
            pidfd_for_task,
            Arc::new(ctx.db.clone()),
            Some(ctx.claude_gateway.clone()),
        );
        crate::provider::init_probe_task::spawn_init_probe_task(
            agent_id.to_string(),
            ctx.tmux_server.clone(),
            pane_id_for_startup,
            Arc::new(ctx.db.clone()),
            provider.to_string(),
            ctx.state_dir.clone(),
            matcher,
            manifest.init_probe,
            Duration::from_secs(manifest.readiness_timeout_s.into()),
            crate::provider::init_probe_task::STABLE_UNKNOWN_STARTUP_GRACE,
            idle_scan_enabled,
        );
        let _sandbox_dir = sandbox_guard.map(path::SandboxDirGuard::release);

        Ok(json!({ "state": "SPAWNING", "pid": pid, "pane_id": response_pane_id }))
    }
}

fn load_claude_shared_credentials_dir(
    project_root: &Path,
) -> Result<Option<std::path::PathBuf>, CcbdError> {
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

pub(crate) fn hook_push_enabled_from_spawn_params(params: &Value) -> bool {
    params
        .get("hook_push_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// ISSUE-13 §3a: the fingerprinted "bare" env = exactly the user-declared config —
/// the project-level `[env]` (`config_env`) with the agent's own `[agents.X.env]`
/// (`agent_env`) overlaid (agent overrides project). It deliberately excludes every
/// runtime-injected/derived var (CCB_SOCKET, AH_ROLE/SESSION/AGENT, HOME, IS_SANDBOX…),
/// which are added only after this point. Both the spawn side and the realign side build
/// their fingerprint env through this one helper, so the two can never diverge — the exact
/// asymmetry that forced every agent to DRIFT on the first `ah up`.
pub(crate) fn bare_config_env(
    config_env: &HashMap<String, String>,
    agent_env: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = config_env.clone();
    merged.extend(agent_env.clone());
    merged
}

pub(crate) fn build_agent_spawn_env_vars_for_hook_push(
    state_dir: &std::path::Path,
    session_id: &str,
    agent_id: &str,
    mut extra_env_vars: HashMap<String, String>,
) -> HashMap<String, String> {
    extra_env_vars.insert(
        "CCB_SOCKET".to_string(),
        state_dir.join("ahd.sock").display().to_string(),
    );
    crate::process_identity::inject_worker_identity(&mut extra_env_vars, session_id, agent_id);
    extra_env_vars
}

pub(crate) fn should_inject_is_sandbox(
    provider_name: &str,
    command: &[&str],
    home_root: &Path,
) -> bool {
    provider_name == "claude"
        && command
            .iter()
            .any(|arg| *arg == "--dangerously-skip-permissions")
        && is_ccb_sandbox_home(home_root)
}

pub(crate) fn inject_is_sandbox_env_if_needed(
    provider_name: &str,
    command: &[&str],
    home_root: &Path,
    env: &mut HashMap<String, String>,
) {
    if should_inject_is_sandbox(provider_name, command, home_root) {
        env.insert("IS_SANDBOX".to_string(), "1".to_string());
    }
}

#[cfg(test)]
mod ra2_tests {
    use super::persist_agent_spawn_snapshot_after_success;
    use crate::db::agents::insert_agent_sync;
    use crate::db::recovery::query_agent_spawn_spec_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::provider::extensions::HookGroup;
    use std::collections::HashMap;

    fn test_db() -> crate::db::Db {
        let file = tempfile::NamedTempFile::new().unwrap();
        crate::db::init(file.path()).unwrap()
    }

    fn sample_spec(agent_id: &str) -> crate::db::recovery::AgentSpawnSpec {
        crate::db::recovery::AgentSpawnSpec {
            agent_id: agent_id.to_string(),
            provider: "codex".to_string(),
            env: HashMap::from([("RA2_ENV".to_string(), "1".to_string())]),
            hooks: HashMap::<String, Vec<HookGroup>>::new(),
            plugins: vec!["github@openai-curated".to_string()],
            skills: Vec::new(),
            bundle: Vec::new(),
            settings: serde_json::Map::new(),
            bundle_digest: None,
            sandbox_overrides: Default::default(),
            hook_push_enabled: false,
        }
    }

    #[test]
    fn ra2_agent_spawn_success_persists_resolved_snapshot() {
        let db = test_db();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/ra2").unwrap();
            insert_agent_sync(&conn, "ra2_spawn", "s1", "codex", "SPAWNING", Some(123)).unwrap();
        }

        assert!(
            persist_agent_spawn_snapshot_after_success(&db, sample_spec("ra2_spawn"), "hash1")
                .unwrap()
        );
        let stored = query_agent_spawn_spec_sync(&db.conn(), "ra2_spawn")
            .unwrap()
            .unwrap();
        assert_eq!(stored.spec.agent_id, "ra2_spawn");
        assert_eq!(stored.spec.provider, "codex");
        assert_eq!(stored.spec.env["RA2_ENV"], "1");
        assert_eq!(stored.config_hash, "hash1");
    }

    #[test]
    fn ra2_agent_spawn_snapshot_write_failure_is_graceful() {
        let db = test_db();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/ra2").unwrap();
            insert_agent_sync(
                &conn,
                "ra2_spawn_no_snapshot",
                "s1",
                "codex",
                "SPAWNING",
                Some(123),
            )
            .unwrap();
        }

        let wrote_snapshot = persist_agent_spawn_snapshot_after_success(
            &db,
            sample_spec("missing_agent_for_fk_failure"),
            "hash1",
        )
        .unwrap();
        assert!(!wrote_snapshot);
        let state: String = db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = 'ra2_spawn_no_snapshot'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "SPAWNING");
        assert!(
            query_agent_spawn_spec_sync(&db.conn(), "ra2_spawn_no_snapshot")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn hook_push_worker_spawn_env_injects_deterministic_ccb_socket() {
        let state_dir = tempfile::tempdir().unwrap();
        let env = super::build_agent_spawn_env_vars_for_hook_push(
            state_dir.path(),
            "s1",
            "a1",
            HashMap::from([
                (
                    "CCB_SOCKET".to_string(),
                    "/tmp/stale-host-socket.sock".to_string(),
                ),
                ("AH_ROLE".to_string(), "master".to_string()),
                ("AH_SESSION_ID".to_string(), "wrong-session".to_string()),
                ("AH_AGENT_ID".to_string(), "wrong-agent".to_string()),
                ("USER_FLAG".to_string(), "1".to_string()),
            ]),
        );

        assert_eq!(
            env.get("CCB_SOCKET").map(String::as_str),
            Some(state_dir.path().join("ahd.sock").to_str().unwrap()),
            "worker spawn must inject deterministic daemon socket, not inherit host CCB_SOCKET"
        );
        assert_eq!(env.get("AH_ROLE").map(String::as_str), Some("worker"));
        assert_eq!(env.get("AH_SESSION_ID").map(String::as_str), Some("s1"));
        assert_eq!(env.get("AH_AGENT_ID").map(String::as_str), Some("a1"));
        assert_eq!(env.get("USER_FLAG").map(String::as_str), Some("1"));
    }

    #[test]
    fn worker_recovery_respawn_env_contains_process_identity() {
        let state_dir = tempfile::tempdir().unwrap();
        let env = super::build_agent_spawn_env_vars_for_hook_push(
            state_dir.path(),
            "s_recovery",
            "a_recovery",
            HashMap::from([("USER_FLAG".to_string(), "recovery".to_string())]),
        );

        assert_eq!(env.get("AH_ROLE").map(String::as_str), Some("worker"));
        assert_eq!(
            env.get("AH_SESSION_ID").map(String::as_str),
            Some("s_recovery")
        );
        assert_eq!(
            env.get("AH_AGENT_ID").map(String::as_str),
            Some("a_recovery")
        );
    }

    #[test]
    fn master_revive_worker_reprovision_uses_worker_identity_env() {
        let state_dir = tempfile::tempdir().unwrap();
        let env = super::build_agent_spawn_env_vars_for_hook_push(
            state_dir.path(),
            "s_reprovision",
            "a_reprovision",
            HashMap::from([("USER_FLAG".to_string(), "reprovision".to_string())]),
        );

        assert_eq!(env.get("AH_ROLE").map(String::as_str), Some("worker"));
        assert_eq!(
            env.get("AH_SESSION_ID").map(String::as_str),
            Some("s_reprovision")
        );
        assert_eq!(
            env.get("AH_AGENT_ID").map(String::as_str),
            Some("a_reprovision")
        );
    }

    #[test]
    fn claude_dangerous_sandbox_home_injects_is_sandbox() {
        let command = ["claude", "--dangerously-skip-permissions"];
        assert!(super::should_inject_is_sandbox(
            "claude",
            &command,
            std::path::Path::new("/home/user/.cache/ah/sandboxes/abc123")
        ));
    }

    #[test]
    fn claude_dangerous_non_sandbox_home_does_not_inject_is_sandbox() {
        let command = ["claude", "--dangerously-skip-permissions"];
        assert!(!super::should_inject_is_sandbox(
            "claude",
            &command,
            std::path::Path::new("/home/user")
        ));
    }

    #[test]
    fn codex_sandbox_home_does_not_inject_is_sandbox() {
        let command = ["codex"];
        assert!(!super::should_inject_is_sandbox(
            "codex",
            &command,
            std::path::Path::new("/home/user/.cache/ah/sandboxes/abc123")
        ));
    }

    #[test]
    fn claude_without_dangerous_flag_does_not_inject_is_sandbox() {
        let command = ["claude"];
        assert!(!super::should_inject_is_sandbox(
            "claude",
            &command,
            std::path::Path::new("/home/user/.cache/ah/sandboxes/abc123")
        ));
    }

    #[test]
    fn claude_dangerous_sandbox_home_adds_is_sandbox_to_env() {
        let command = ["claude", "--dangerously-skip-permissions"];
        let mut env = HashMap::new();
        super::inject_is_sandbox_env_if_needed(
            "claude",
            &command,
            std::path::Path::new("/home/user/.cache/ah/sandboxes/abc123"),
            &mut env,
        );

        assert_eq!(env.get("IS_SANDBOX").map(String::as_str), Some("1"));
    }

    #[test]
    fn claude_dangerous_non_sandbox_home_does_not_add_is_sandbox_to_env() {
        let command = ["claude", "--dangerously-skip-permissions"];
        let mut env = HashMap::new();
        super::inject_is_sandbox_env_if_needed(
            "claude",
            &command,
            std::path::Path::new("/home/user"),
            &mut env,
        );

        assert!(!env.contains_key("IS_SANDBOX"));
    }

    #[test]
    fn codex_sandbox_home_does_not_add_is_sandbox_to_env() {
        let command = ["codex"];
        let mut env = HashMap::new();
        super::inject_is_sandbox_env_if_needed(
            "codex",
            &command,
            std::path::Path::new("/home/user/.cache/ah/sandboxes/abc123"),
            &mut env,
        );

        assert!(!env.contains_key("IS_SANDBOX"));
    }

    #[test]
    fn claude_without_dangerous_flag_does_not_add_is_sandbox_to_env() {
        let command = ["claude"];
        let mut env = HashMap::new();
        super::inject_is_sandbox_env_if_needed(
            "claude",
            &command,
            std::path::Path::new("/home/user/.cache/ah/sandboxes/abc123"),
            &mut env,
        );

        assert!(!env.contains_key("IS_SANDBOX"));
    }

    #[test]
    fn hook_push_enabled_spawn_param_defaults_false() {
        assert!(!super::hook_push_enabled_from_spawn_params(
            &serde_json::json!({})
        ));
        assert!(!super::hook_push_enabled_from_spawn_params(
            &serde_json::json!({"hook_push_enabled": "true"})
        ));
    }

    #[test]
    fn hook_push_enabled_spawn_param_accepts_true() {
        assert!(super::hook_push_enabled_from_spawn_params(
            &serde_json::json!({"hook_push_enabled": true})
        ));
    }
}

pub async fn handle_agent_kill(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let agent_id = required_str(&params, "agent_id")?;
    let agent = query_agent(ctx.db.clone(), agent_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::AgentNotFound(agent_id.to_string()))?;

    let changes = mark_agent_killed(
        ctx.db.clone(),
        agent_id.to_string(),
        "SIGKILL_BY_DAEMON".to_string(),
    )
    .await?;
    if changes == 0 {
        return Ok(json!({ "state": agent.state }));
    }

    if let Some(pid) = agent.pid {
        #[cfg(windows)]
        {
            tracing::warn!(
                agent_id,
                pid,
                "Windows agent.kill cannot terminate processes until M1 Job Object support"
            );
        }
        #[cfg(unix)]
        {
            // SAFETY: pid comes from the agents table and SIGKILL is a constant.
            let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                tracing::warn!(agent_id, pid, error = %err, "failed to SIGKILL agent pid");
            }
        }
    } else {
        tracing::warn!(agent_id, "agent.kill found no pid in database");
    }
    ctx.claude_gateway.deregister(agent_id).await;
    remove_agent_sandbox_dir_sync(&ctx.state_dir, &agent.session_id, agent_id);

    Ok(json!({ "state": "KILLED" }))
}

pub async fn handle_agent_notify(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let agent_id = required_str(&params, "agent_id")?.to_string();
    let event = normalize_hook_event(required_str(&params, "event")?);
    let provider = params
        .get("provider")
        .and_then(Value::as_str)
        .map(crate::provider::manifest::canonicalize_provider_name)
        .map(str::to_string);
    let event_id = params
        .get("event_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let lifecycle_id = params
        .get("lifecycle_id")
        .and_then(Value::as_str)
        .map(str::to_string);

    if let Some((session_id, master_generation)) = parse_master_agent_id(&agent_id)? {
        return handle_master_notify(
            agent_id,
            session_id,
            master_generation,
            event,
            provider,
            event_id,
            ctx,
        )
        .await;
    }

    let agent = query_agent(ctx.db.clone(), agent_id.clone())
        .await?
        .ok_or_else(|| CcbdError::AgentNotFound(agent_id.clone()))?;
    if let Some(provider) = provider.as_deref()
        && provider != agent.provider
    {
        return Err(CcbdError::IpcInvalidRequest(format!(
            "provider mismatch for agent {agent_id}: expected {}, got {provider}",
            agent.provider
        )));
    }
    if let Some(reported_lifecycle_id) = lifecycle_id.as_deref()
        && reported_lifecycle_id != agent.lifecycle_id
    {
        tracing::warn!(
            agent_id = %agent_id,
            provider = %agent.provider,
            reported_lifecycle_id,
            current_lifecycle_id = %agent.lifecycle_id,
            event = %event,
            "ignored stale provider hook from an earlier lifecycle"
        );
        return Ok(json!({
            "agent_id": agent_id,
            "event": event,
            "role": "worker",
            "provider": agent.provider,
            "event_id": event_id,
            "lifecycle_id": reported_lifecycle_id,
            "accepted": true,
            "ignored_stale": true,
            "transitioned": false,
            "changes": 0,
            "affected_job_id": null,
        }));
    }
    let provider = provider.unwrap_or(agent.provider);

    let provider_turn_id = params
        .get("provider_turn_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let provider_session_id = params
        .get("provider_session_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let provider_invocation_num = params
        .get("provider_invocation_num")
        .and_then(Value::as_u64);
    let provider_fully_idle = params.get("provider_fully_idle").and_then(Value::as_bool);
    let provider_termination_reason = params
        .get("provider_termination_reason")
        .and_then(Value::as_str)
        .map(str::to_string);
    let provider_error = params
        .get("provider_error")
        .and_then(Value::as_str)
        .map(str::to_string);

    let prompt_fingerprint = params
        .get("prompt_fingerprint")
        .and_then(Value::as_str)
        .map(str::to_string);
    let disposition = classify_worker_hook(
        &provider,
        &event,
        prompt_fingerprint.is_some(),
        provider_fully_idle,
        provider_error.is_some(),
    );

    if disposition == WorkerHookDisposition::IgnoreUnverifiedCompletion {
        tracing::warn!(
            agent_id = %agent_id,
            provider = %provider,
            event_id = ?event_id,
            provider_error = ?provider_error,
            provider_termination_reason = ?provider_termination_reason,
            "ignored Antigravity Stop hook without clean fully-idle completion proof"
        );
        return Ok(json!({
            "agent_id": agent_id,
            "event": event,
            "role": "worker",
            "provider": provider,
            "event_id": event_id,
            "lifecycle_id": lifecycle_id,
            "accepted": true,
            "ignored_unverified_completion": true,
            "transitioned": false,
            "changes": 0,
            "affected_job_id": null,
        }));
    }

    if disposition == WorkerHookDisposition::IgnoreUncorrelated {
        tracing::warn!(
            agent_id = %agent_id,
            provider = %provider,
            event_id = ?event_id,
            "ignored UserPromptSubmit hook without a prompt fingerprint"
        );
        return Ok(json!({
            "agent_id": agent_id,
            "event": event,
            "role": "worker",
            "provider": provider,
            "event_id": event_id,
            "lifecycle_id": lifecycle_id,
            "accepted": true,
            "ignored_uncorrelated": true,
            "transitioned": false,
            "changes": 0,
            "affected_job_id": null,
        }));
    }

    if disposition == WorkerHookDisposition::Working {
        let evidence_id = event_id
            .as_deref()
            .map(|id| format!("hook:{id}"))
            .unwrap_or_else(|| {
                format!(
                    "hook:{event}:{}:{}:{}",
                    agent.lifecycle_id,
                    provider_session_id.as_deref().unwrap_or("unknown-session"),
                    provider_invocation_num
                        .map(|number| number.to_string())
                        .as_deref()
                        .unwrap_or("unknown-invocation")
                )
            });
        let outcome = observe_working(WorkingObservation {
            db: ctx.db.clone(),
            agent_id: agent_id.clone(),
            provider: provider.clone(),
            expected_lifecycle_id: agent.lifecycle_id.clone(),
            source: crate::runtime_observation::EvidenceSource::OfficialHook,
            evidence_id,
            provider_turn_id: provider_turn_id.clone(),
            prompt_fingerprint,
        })
        .await?;
        if let Some(job_id) = outcome.affected_job.as_deref()
            && (outcome.state_changed || outcome.observation_inserted)
        {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        tracing::info!(
            agent_id = %agent_id,
            provider = %provider,
            event_id = ?event_id,
            provider_turn_id = ?provider_turn_id,
            provider_session_id = ?provider_session_id,
            provider_invocation_num = ?provider_invocation_num,
            provider_fully_idle = ?provider_fully_idle,
            state_changed = outcome.state_changed,
            observation_inserted = outcome.observation_inserted,
            affected_job_id = ?outcome.affected_job,
            "processed provider-native working hook"
        );
        return Ok(json!({
            "agent_id": agent_id,
            "event": event,
            "role": "worker",
            "provider": provider,
            "event_id": event_id,
            "lifecycle_id": lifecycle_id,
            "accepted": true,
            "ignored_stale": false,
            "provider_fully_idle": provider_fully_idle,
            "transitioned": outcome.state_changed,
            "changes": usize::from(outcome.state_changed),
            "observation_inserted": outcome.observation_inserted,
            "affected_job_id": outcome.affected_job,
        }));
    }

    if disposition == WorkerHookDisposition::Unsupported {
        tracing::info!(
            hook_source = "agent.notify",
            role = "worker",
            agent_id = %agent_id,
            provider = %provider,
            event = %event,
            event_id = ?event_id,
            transitioned = false,
            reason = "unsupported_worker_event",
            "ignored unsupported worker hook"
        );
        return Ok(json!({
            "agent_id": agent_id,
            "event": event,
            "role": "worker",
            "provider": provider,
            "event_id": event_id,
            "accepted": true,
            "unsupported": true,
            "transitioned": false,
            "changes": 0,
            "affected_job_id": null,
        }));
    }

    let reply = params
        .get("reply")
        .and_then(Value::as_str)
        .map(str::to_string);

    tracing::info!(
        agent_id = %agent_id,
        provider = %provider,
        event = %event,
        event_id = ?event_id,
        "received agent.notify hook"
    );

    let (changes, affected_job_id) = observe_hook_completion(HookCompletionObservation {
        db: ctx.db.clone(),
        agent_id: agent_id.clone(),
        provider: provider.clone(),
        hook_event: event.clone(),
        event_id: event_id.clone(),
        reply,
        expected_lifecycle_id: lifecycle_id.clone(),
    })
    .await?;
    tracing::info!(
        changes,
        affected_job_id = ?affected_job_id,
        transitioned = changes > 0,
        "processed agent.notify hook"
    );
    if changes > 0 {
        if let Some(job_id) = &affected_job_id {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        crate::completion::registry::cancel(&agent_id);
        crate::orchestrator::wake_up();
    }

    Ok(json!({
        "agent_id": agent_id,
        "event": event,
        "role": "worker",
        "provider": provider,
        "event_id": event_id,
        "lifecycle_id": lifecycle_id,
        "accepted": true,
        "ignored_stale": false,
        "transitioned": changes > 0,
        "changes": changes,
        "affected_job_id": affected_job_id,
    }))
}

fn normalize_hook_event(event: &str) -> String {
    event.to_ascii_lowercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerHookDisposition {
    Working,
    Completion,
    IgnoreUncorrelated,
    IgnoreUnverifiedCompletion,
    Unsupported,
}

fn classify_worker_hook(
    provider: &str,
    event: &str,
    has_prompt_fingerprint: bool,
    provider_fully_idle: Option<bool>,
    provider_has_error: bool,
) -> WorkerHookDisposition {
    if event == "userpromptsubmit" {
        return if has_prompt_fingerprint {
            WorkerHookDisposition::Working
        } else {
            WorkerHookDisposition::IgnoreUncorrelated
        };
    }
    if provider == "antigravity" && event == "preinvocation" {
        return WorkerHookDisposition::Working;
    }
    if event != "stop" {
        return WorkerHookDisposition::Unsupported;
    }
    if provider != "antigravity" {
        return WorkerHookDisposition::Completion;
    }
    if provider_has_error || provider_fully_idle.is_none() {
        return WorkerHookDisposition::IgnoreUnverifiedCompletion;
    }
    if provider_fully_idle == Some(false) {
        WorkerHookDisposition::Working
    } else {
        WorkerHookDisposition::Completion
    }
}

fn parse_master_agent_id(agent_id: &str) -> Result<Option<(String, i64)>, CcbdError> {
    let Some(rest) = agent_id.strip_prefix("master:") else {
        return Ok(None);
    };
    let Some((session_id, generation)) = rest.rsplit_once(':') else {
        return Err(CcbdError::IpcInvalidRequest(format!(
            "invalid master agent_id sentinel: {agent_id}"
        )));
    };
    if session_id.is_empty() {
        return Err(CcbdError::IpcInvalidRequest(format!(
            "invalid master agent_id sentinel: {agent_id}"
        )));
    }
    let generation = generation.parse::<i64>().map_err(|err| {
        CcbdError::IpcInvalidRequest(format!(
            "invalid master generation in agent_id {agent_id}: {err}"
        ))
    })?;
    Ok(Some((session_id.to_string(), generation)))
}

async fn handle_master_notify(
    agent_id: String,
    session_id: String,
    master_generation: i64,
    event: String,
    provider: Option<String>,
    event_id: Option<String>,
    ctx: &Ctx,
) -> Result<Value, CcbdError> {
    let (new_state, clear_pending_request) = match event.as_str() {
        "userpromptsubmit" => ("BUSY", false),
        "stop" => ("IDLE", true),
        _ => {
            return Err(CcbdError::IpcInvalidRequest(format!(
                "unsupported event for master agent.notify: {event}"
            )));
        }
    };
    let provider = provider.unwrap_or_else(|| "unknown".to_string());

    tracing::info!(
        hook_source = "agent.notify",
        role = "master",
        event = %event,
        agent_id = %agent_id,
        session_id = %session_id,
        provider = %provider,
        event_id = ?event_id,
        master_generation,
        "received hook"
    );

    let transition = apply_master_notify_event(
        ctx.db.clone(),
        session_id.clone(),
        master_generation,
        new_state.to_string(),
        clear_pending_request,
    )
    .await?
    .ok_or_else(|| {
        CcbdError::IpcInvalidRequest(format!("active session not found: {session_id}"))
    })?;
    let ignored_stale = transition.current_generation != master_generation;

    tracing::info!(
        hook_source = "agent.notify",
        role = "master",
        event = %event,
        agent_id = %agent_id,
        session_id = %session_id,
        request_id = ?transition.request_id,
        master_generation,
        current_master_generation = transition.current_generation,
        previous_state = %transition.previous_state,
        new_state = %transition.new_state,
        transitioned = transition.transitioned,
        ignored_stale,
        reason = if ignored_stale { "stale_generation" } else { "ok" },
        "processed hook"
    );

    Ok(json!({
        "agent_id": agent_id,
        "event": event,
        "role": "master",
        "provider": provider,
        "event_id": event_id,
        "session_id": session_id,
        "request_id": transition.request_id,
        "master_generation": master_generation,
        "current_master_generation": transition.current_generation,
        "accepted": true,
        "transitioned": transition.transitioned,
        "ignored_stale": ignored_stale,
        "previous_state": transition.previous_state,
        "new_state": transition.new_state,
    }))
}

pub async fn handle_agent_send(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let agent_id = required_str(&params, "agent_id")?;
    let text = required_str(&params, "text")?;
    let request_id = params.get("request_id").and_then(Value::as_str);

    if let Some(request_id) = request_id {
        let existing =
            query_event_by_request_id(ctx.db.clone(), agent_id.to_string(), request_id.to_string())
                .await?;
        if let Some(existing) = existing {
            let payload: Value = serde_json::from_str(&existing.payload).map_err(|err| {
                CcbdError::IpcInvalidRequest(format!(
                    "invalid command_received payload for seq_id={}: {err}",
                    existing.seq_id
                ))
            })?;
            let status = payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("PENDING");
            let state = query_agent_state(ctx.db.clone(), agent_id.to_string())
                .await?
                .map(|(state, _)| state)
                .ok_or_else(|| CcbdError::AgentNotFound(agent_id.to_string()))?;
            return match status {
                "SENT" | "PENDING" => Ok(json!({
                    "state": state,
                    "seq_id": existing.seq_id,
                })),
                "FAILED" => Err(CcbdError::PtyIoError(format!(
                    "previous attempt seq_id={} failed; retry with new request_id",
                    existing.seq_id
                ))),
                other => Err(CcbdError::IpcInvalidRequest(format!(
                    "unknown command_received status: {other}"
                ))),
            };
        }
    }

    let agent = query_agent(ctx.db.clone(), agent_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::AgentNotFound(agent_id.to_string()))?;
    let state = agent.state.clone();
    let cas_ok =
        mark_agent_waiting_for_ack(ctx.db.clone(), agent_id.to_string(), agent.state_version)
            .await?;
    if !cas_ok {
        let current_state = query_agent_state(ctx.db.clone(), agent_id.to_string())
            .await?
            .map(|(state, _)| state)
            .unwrap_or_else(|| state.clone());
        return Err(CcbdError::AgentWrongState { current_state });
    }
    crate::agent_io::set_idle_scan_enabled(agent_id, false);
    let manifest = crate::provider::manifest::get_manifest(&agent.provider);
    let matcher = Arc::new(MarkerMatcher::from_manifest(&manifest));

    let pane_id = if let Some(pane_id) = crate::agent_io::pane_id(agent_id) {
        pane_id
    } else {
        return Err(CcbdError::PtyIoError(format!(
            "tmux pane not registered for {agent_id}"
        )));
    };

    let pending_payload = json!({ "cmd": text, "status": "PENDING" }).to_string();
    let seq_id = {
        match insert_event(
            ctx.db.clone(),
            agent_id.to_string(),
            request_id.map(str::to_string),
            "command_received".to_string(),
            pending_payload.clone(),
        )
        .await
        {
            Ok(seq_id) => seq_id,
            Err(CcbdError::DuplicateRequest { existing_seq_id }) => {
                let request_id = request_id.ok_or_else(|| {
                    CcbdError::IpcInvalidRequest("duplicate request without request_id".into())
                })?;
                let existing = query_event_by_request_id(
                    ctx.db.clone(),
                    agent_id.to_string(),
                    request_id.to_string(),
                )
                .await?
                .ok_or_else(|| {
                    CcbdError::DbConstraintViolation(format!(
                        "duplicate event seq_id={existing_seq_id} not found"
                    ))
                })?;
                let payload: Value = serde_json::from_str(&existing.payload).map_err(|err| {
                    CcbdError::IpcInvalidRequest(format!(
                        "invalid command_received payload for seq_id={existing_seq_id}: {err}"
                    ))
                })?;
                let status = payload
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("PENDING");
                return match status {
                    "SENT" | "PENDING" => Ok(json!({
                        "state": state,
                        "seq_id": existing.seq_id,
                    })),
                    "FAILED" => Err(CcbdError::PtyIoError(format!(
                        "previous attempt seq_id={existing_seq_id} failed; retry with new request_id"
                    ))),
                    other => Err(CcbdError::IpcInvalidRequest(format!(
                        "unknown command_received status: {other}"
                    ))),
                };
            }
            Err(err) => return Err(err),
        }
    };

    let capture_baseline = ctx.tmux_server.capture_pane(pane_id.clone()).await.ok();
    let write_result = crate::agent_io::send_text_to_pane_with_options(
        ctx.tmux_server.clone(),
        agent_id,
        &agent.provider,
        pane_id,
        text.to_string(),
        press_enter_after_paste(&agent.provider, text),
    )
    .await;

    let write_error = write_result.as_ref().err().map(|err| err.to_string());
    let final_status = if write_result.is_ok() {
        "SENT"
    } else {
        "FAILED"
    };
    let final_payload = json!({ "cmd": text, "status": final_status });
    record_send_progress(
        ctx.db.clone(),
        seq_id,
        final_payload,
        agent_id.to_string(),
        write_result.is_ok(),
    )
    .await?;

    if let Some(write_error) = write_error {
        return Err(CcbdError::PtyIoError(write_error));
    }

    let parser_handle = parser_registry::get(agent_id).ok_or_else(|| {
        CcbdError::PtyIoError(format!("parser not in PARSER_REGISTRY for {agent_id}"))
    })?;
    let marker_handle = spawn_marker_timer_task_with_prompt(
        agent_id.to_string(),
        TimerKind::Busy,
        Arc::new(ctx.db.clone()),
        parser_handle,
        Some(PromptTimerScanContext {
            provider: agent.provider.clone(),
            state_dir: ctx.state_dir.clone(),
            tmux: ctx.tmux_server.clone(),
            matcher: matcher.clone(),
        }),
    );
    registry::register(agent_id.to_string(), marker_handle);
    if let Some(capture_baseline) = capture_baseline {
        super::ack::spawn_new_capture_seed(
            ctx.db.clone(),
            ctx.tmux_server.clone(),
            agent_id.to_string(),
            agent.provider.clone(),
            ctx.state_dir.clone(),
            capture_baseline,
            matcher,
            super::ack::AckEvidenceMode::for_provider(&agent.provider),
        );
    }

    Ok(json!({
        "state": crate::db::state_machine::STATE_WAITING_FOR_ACK,
        "seq_id": seq_id,
    }))
}

pub async fn handle_agent_read(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let agent_id = required_str(&params, "agent_id")?;
    let since_event_id = required_i64(&params, "since_event_id")?;

    if query_agent(ctx.db.clone(), agent_id.to_string())
        .await?
        .is_none()
    {
        return Err(CcbdError::AgentNotFound(agent_id.to_string()));
    }

    let events = query_events_since(ctx.db.clone(), agent_id.to_string(), since_event_id).await?;

    Ok(json!({ "events": format_events(events), "is_truncated": false }))
}

pub async fn handle_agent_watch(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let agent_id = required_str(&params, "agent_id")?.to_string();
    let since_event_id = required_i64(&params, "since_event_id")?;
    let timeout_secs = params.get("timeout").and_then(Value::as_u64).unwrap_or(30);
    let mut rx = crate::orchestrator::pubsub::subscribe_agent_output();

    if query_agent(ctx.db.clone(), agent_id.clone())
        .await?
        .is_none()
    {
        return Err(CcbdError::AgentNotFound(agent_id));
    }

    let events = query_events_since(ctx.db.clone(), agent_id.clone(), since_event_id).await?;
    if !events.is_empty() {
        return Ok(json!({ "events": format_events(events), "is_truncated": false }));
    }

    let wait_future = async {
        loop {
            match rx.recv().await {
                Ok(updated_agent_id) if updated_agent_id == agent_id => {
                    let events =
                        query_events_since(ctx.db.clone(), agent_id.clone(), since_event_id)
                            .await?;
                    if !events.is_empty() {
                        return Ok(json!({
                            "events": format_events(events),
                            "is_truncated": false,
                        }));
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let events =
                        query_events_since(ctx.db.clone(), agent_id.clone(), since_event_id)
                            .await?;
                    if !events.is_empty() {
                        return Ok(json!({
                            "events": format_events(events),
                            "is_truncated": false,
                        }));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(CcbdError::IpcInvalidRequest(
                        "agent output subscription closed".into(),
                    ));
                }
            }
        }
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), wait_future).await {
        Ok(result) => result,
        Err(_) => Ok(json!({ "events": [], "is_truncated": false })),
    }
}

fn format_events(events: Vec<crate::db::schema::Event>) -> Vec<Value> {
    events
        .into_iter()
        .map(|event| {
            json!({
                "seq_id": event.seq_id,
                "event_type": event.event_type,
                "payload": event.payload,
                "created_at": event.created_at,
            })
        })
        .collect()
}

async fn cleanup_spawn_resources(
    tmux: &crate::tmux::TmuxServer,
    pane_id: Option<crate::tmux::TmuxPaneId>,
    fifo_file: Option<std::fs::File>,
    fifo_path: &std::path::Path,
) {
    if let Some(pane_id) = pane_id {
        let _ = tmux.kill_pane(pane_id).await;
    }
    drop(fifo_file);
    let _ = fs::remove_file(fifo_path);
}

#[cfg(test)]
mod hook_status_tests {
    use super::{WorkerHookDisposition, classify_worker_hook};

    #[test]
    fn codex_and_claude_prompt_hooks_require_dispatch_correlation() {
        assert_eq!(
            classify_worker_hook("codex", "userpromptsubmit", true, None, false),
            WorkerHookDisposition::Working
        );
        assert_eq!(
            classify_worker_hook("claude", "userpromptsubmit", false, None, false),
            WorkerHookDisposition::IgnoreUncorrelated
        );
    }

    #[test]
    fn antigravity_stop_requires_clean_fully_idle_proof() {
        assert_eq!(
            classify_worker_hook("antigravity", "preinvocation", false, None, false),
            WorkerHookDisposition::Working
        );
        assert_eq!(
            classify_worker_hook("antigravity", "stop", false, Some(false), false),
            WorkerHookDisposition::Working
        );
        assert_eq!(
            classify_worker_hook("antigravity", "stop", false, Some(true), false),
            WorkerHookDisposition::Completion
        );
        assert_eq!(
            classify_worker_hook("antigravity", "stop", false, None, false),
            WorkerHookDisposition::IgnoreUnverifiedCompletion
        );
        assert_eq!(
            classify_worker_hook("antigravity", "stop", false, Some(true), true),
            WorkerHookDisposition::IgnoreUnverifiedCompletion
        );
    }
}
