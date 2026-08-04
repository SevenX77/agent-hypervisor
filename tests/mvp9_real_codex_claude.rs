#![cfg(target_os = "linux")]
use ah::cli::config::{
    AgentConfig, ClaudeProviderConfig, MasterConfig, ProjectConfig, ProviderConfigs, SandboxConfig,
};
use ah::cli::rpc_client::{CliError, RpcClient, RpcFuture};
use ah::cli::start::start_project;
use ah::db;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_spawn, handle_agent_watch, handle_job_submit, handle_job_wait,
    handle_session_create, handle_session_kill, handle_session_list,
    handle_session_spawn_master_pane,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, compute_socket_name};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

mod common;

struct RealHarness {
    ctx: Ctx,
    sessions: Arc<Mutex<Vec<String>>>,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl RealHarness {
    fn new() -> Self {
        common::hard_gate("codex");
        common::hard_gate("claude");
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let socket_name = compute_socket_name(state_dir.path());
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: true,
                unsafe_no_sandbox: false,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new_with_policy(
                state_dir.path(),
                common::scope_policy_for_test(&socket_name),
            )),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        ah::orchestrator::spawn_orchestrator_task(ctx.clone());
        Self {
            ctx,
            sessions: Arc::new(Mutex::new(Vec::new())),
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }
}

impl Drop for RealHarness {
    fn drop(&mut self) {
        for session_id in self.sessions.lock().unwrap().iter() {
            stop_anchor(session_id);
        }
        let _ = Command::new("tmux")
            .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
            .output();
    }
}

struct HandlerClient {
    ctx: Ctx,
    sessions: Arc<Mutex<Vec<String>>>,
}

impl RpcClient for HandlerClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            match method {
                "session.list" => handle_session_list(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "session.create" => {
                    let result = handle_session_create(params, &self.ctx)
                        .await
                        .map_err(map_rpc_error)?;
                    if let Some(session_id) = result["session_id"].as_str() {
                        self.sessions.lock().unwrap().push(session_id.to_string());
                    }
                    Ok(result)
                }
                "agent.spawn" => handle_agent_spawn(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "agent.watch" => handle_agent_watch(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "session.spawn_master_pane" => handle_session_spawn_master_pane(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "session.kill" => handle_session_kill(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                other => Err(CliError::InvalidResponse(format!(
                    "unexpected method in real mvp9 client: {other}"
                ))),
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_config_parse_and_batch_spawn_real() {
    if std::env::var("CCB_TEST_SKIP_REAL_PROVIDER").as_deref() == Ok("1") {
        return;
    }
    let h = RealHarness::new();
    let mut agents = BTreeMap::new();
    agents.insert(
        "ag_real_codex".to_string(),
        AgentConfig {
            provider: "codex".to_string(),
            env: HashMap::new(),
            hooks: Default::default(),
            plugins: Default::default(),
            skills: Default::default(),
            bundle: Default::default(),
            settings: Default::default(),
        },
    );
    agents.insert(
        "ag_real_claude".to_string(),
        AgentConfig {
            provider: "claude".to_string(),
            env: HashMap::new(),
            hooks: Default::default(),
            plugins: Default::default(),
            skills: Default::default(),
            bundle: Default::default(),
            settings: Default::default(),
        },
    );
    let Some(shared_credentials_dir) = real_claude_shared_credentials_dir() else {
        return;
    };
    let config = ProjectConfig {
        version: "1".to_string(),
        master: MasterConfig {
            cmd: "claude".to_string(),
            cmd_explicit: false,
            provider: None,
            readiness_timeout_s: 120,
            enabled: false,
            window_size: Default::default(),
            hooks: Default::default(),
            plugins: Default::default(),
            skills: Default::default(),
            bundle: Default::default(),
            settings: Default::default(),
        },
        completion: Default::default(),
        daemon: Default::default(),
        providers: ProviderConfigs {
            claude: ClaudeProviderConfig {
                shared_credentials_dir: Some(shared_credentials_dir),
            },
        },
        env: HashMap::new(),
        sandbox: SandboxConfig::default(),
        agents,
    };
    let client = HandlerClient {
        ctx: h.ctx.clone(),
        sessions: h.sessions.clone(),
    };
    let summary = start_project(
        &client,
        config,
        std::path::Path::new("mvp9-real.toml"),
        h.ctx.state_dir.clone(),
        true,
    )
    .await
    .unwrap();

    assert_eq!(summary.agents.len(), 2);
    assert_eq!(
        agent_state(&h.ctx, "ag_real_codex").as_deref(),
        Some("IDLE")
    );
    assert_eq!(
        agent_state(&h.ctx, "ag_real_claude").as_deref(),
        Some("IDLE")
    );

    let codex_job = submit_job(&h, "ag_real_codex", "Reply with exactly: codex-ok\n").await;
    let claude_job = submit_job(&h, "ag_real_claude", "Reply with exactly: claude-ok\n").await;
    let codex = wait_job(&h, &codex_job).await;
    let claude = wait_job(&h, &claude_job).await;
    assert!(codex.contains("codex-ok"));
    assert!(claude.contains("claude-ok"));
    let _ = handle_session_kill(
        json!({ "session_id": summary.session_id, "force": true }),
        &h.ctx,
    )
    .await;
}

fn real_claude_shared_credentials_dir() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(path) = std::env::var("CLAUDE_SECURESTORAGE_CONFIG_DIR") {
        candidates.push(PathBuf::from(path));
    }
    if let Ok(home) = std::env::var("HOME") {
        candidates.push(PathBuf::from(home).join(".claude"));
    }
    candidates.into_iter().find(|path| {
        path.is_absolute()
            && path.is_dir()
            && std::fs::symlink_metadata(path)
                .map(|metadata| !metadata.file_type().is_symlink())
                .unwrap_or(false)
    })
}

async fn submit_job(h: &RealHarness, agent_id: &str, text: &str) -> String {
    let result = handle_job_submit(
        json!({
            "agent_id": agent_id,
            "text": text,
            "request_id": format!("req_{}", uuid::Uuid::new_v4()),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    result["job_id"].as_str().unwrap().to_string()
}

async fn wait_job(h: &RealHarness, job_id: &str) -> String {
    let result = handle_job_wait(json!({ "job_id": job_id, "timeout": 120 }), &h.ctx)
        .await
        .unwrap();
    assert_eq!(result["status"], "COMPLETED");
    result["reply_text"].as_str().unwrap().to_string()
}

fn map_rpc_error(err: ah::error::CcbdError) -> CliError {
    CliError::Rpc {
        code: -32000,
        message: err.to_string(),
    }
}

fn agent_state(ctx: &Ctx, agent_id: &str) -> Option<String> {
    ctx.db
        .conn()
        .query_row(
            "SELECT state FROM agents WHERE id = ?1",
            [agent_id],
            |row| row.get(0),
        )
        .ok()
}

fn stop_anchor(session_id: &str) {
    let _ = Command::new("systemctl")
        .args([
            "--user",
            "stop",
            &format!("ahd-session-{session_id}.service"),
        ])
        .output();
}
