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
use ah::runtime_events::{RuntimeSnapshotReason, RuntimeSnapshotRequest, build_runtime_snapshot};
use ah::runtime_observation::{
    EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderOccupancy,
    ProviderProcessState, ProviderTurnState, ResolvedDimension,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, agent_session_name, compute_socket_name};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

mod common;

struct RealHarness {
    ctx: Ctx,
    sessions: Arc<Mutex<Vec<String>>>,
    rpc_server: tokio::task::JoinHandle<()>,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl RealHarness {
    async fn new() -> Self {
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
        let rpc_socket = state_dir.path().join("ahd.sock");
        let server_ctx = ctx.clone();
        let server_socket = rpc_socket.clone();
        let rpc_server = tokio::spawn(async move {
            ah::rpc::run_server(&server_socket, server_ctx)
                .await
                .unwrap();
        });
        for _ in 0..100 {
            if rpc_socket.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            rpc_socket.exists(),
            "real hook test RPC socket did not become ready at {}",
            rpc_socket.display()
        );
        Self {
            ctx,
            sessions: Arc::new(Mutex::new(Vec::new())),
            rpc_server,
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }
}

impl Drop for RealHarness {
    fn drop(&mut self) {
        self.rpc_server.abort();
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
    run_batch_spawn_real(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_hook_push_delivers_native_events_real() {
    run_batch_spawn_real(true).await;
}

async fn run_batch_spawn_real(hook_push_enabled: bool) {
    if std::env::var("CCB_TEST_SKIP_REAL_PROVIDER").as_deref() == Ok("1") {
        return;
    }
    let h = RealHarness::new().await;
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
    agents.insert(
        "ag_real_antigravity".to_string(),
        AgentConfig {
            provider: "antigravity".to_string(),
            env: HashMap::new(),
            hooks: Default::default(),
            plugins: Default::default(),
            skills: Default::default(),
            bundle: Default::default(),
            settings: Default::default(),
        },
    );
    agents.insert(
        "ag_real_bash".to_string(),
        AgentConfig {
            provider: "bash".to_string(),
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
    let mut completion = ah::cli::config::CompletionConfig::default();
    completion.hook_push_enabled = hook_push_enabled;
    let config = ProjectConfig {
        version: "1".to_string(),
        master: MasterConfig {
            cmd: "claude".to_string(),
            cmd_explicit: false,
            provider: None,
            env: Default::default(),
            readiness_timeout_s: 120,
            enabled: false,
            window_size: Default::default(),
            hooks: Default::default(),
            plugins: Default::default(),
            skills: Default::default(),
            bundle: Default::default(),
            settings: Default::default(),
        },
        completion,
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

    assert_eq!(summary.agents.len(), 4);
    for agent_id in [
        "ag_real_codex",
        "ag_real_claude",
        "ag_real_antigravity",
        "ag_real_bash",
    ] {
        assert_eq!(agent_state(&h.ctx, agent_id).as_deref(), Some("IDLE"));
    }

    let codex_job = submit_job(&h, "ag_real_codex", "Reply with exactly: codex-ok\n").await;
    let claude_job = submit_job(&h, "ag_real_claude", "Reply with exactly: claude-ok\n").await;
    let antigravity_job = submit_job(
        &h,
        "ag_real_antigravity",
        "Reply with exactly: antigravity-ok\n",
    )
    .await;
    let bash_job = submit_job(&h, "ag_real_bash", "printf 'bash-ok\\n'\n").await;
    let codex = wait_job(&h, "codex", &codex_job).await;
    let claude = wait_job(&h, "claude", &claude_job).await;
    let antigravity = wait_job(&h, "antigravity", &antigravity_job).await;
    let bash = wait_job(&h, "bash", &bash_job).await;
    assert!(codex.contains("codex-ok"));
    assert!(claude.contains("claude-ok"));
    assert!(antigravity.contains("antigravity-ok"));
    assert!(bash.contains("bash-ok"));

    for (agent_id, job_id) in [
        ("ag_real_codex", codex_job.as_str()),
        ("ag_real_claude", claude_job.as_str()),
        ("ag_real_antigravity", antigravity_job.as_str()),
        ("ag_real_bash", bash_job.as_str()),
    ] {
        assert_provider_turn_observations(&h.ctx, agent_id, job_id, hook_push_enabled);
    }

    let snapshot = build_runtime_snapshot(
        &h.ctx,
        RuntimeSnapshotRequest {
            reason: RuntimeSnapshotReason::Initial,
            config_path: None,
            workspace_path: Some(h.ctx.state_dir.display().to_string()),
            sequence: 1,
        },
    )
    .await
    .unwrap();
    for provider in ["codex", "claude", "antigravity", "bash"] {
        let agent = snapshot
            .agents
            .iter()
            .find(|agent| agent.provider == provider)
            .unwrap_or_else(|| panic!("missing runtime snapshot for provider {provider}"));
        assert!(agent.tmux_alive, "{provider} process should still be live");
        assert_eq!(
            agent.provider_status.occupancy,
            ProviderOccupancy::Available
        );
        assert!(matches!(
            agent.provider_status.process,
            ResolvedDimension::Known {
                value: ProviderProcessState::Alive,
                ..
            }
        ));
        assert!(matches!(
            agent.provider_status.turn,
            ResolvedDimension::Known {
                value: ProviderTurnState::Ready,
                ..
            }
        ));
    }
    let _ = handle_session_kill(
        json!({ "session_id": summary.session_id, "force": true }),
        &h.ctx,
    )
    .await;
}

fn assert_provider_turn_observations(
    ctx: &Ctx,
    agent_id: &str,
    job_id: &str,
    hook_push_enabled: bool,
) {
    let conn = ctx.db.conn();
    let mut statement = conn
        .prepare(
            "SELECT observation_json
             FROM provider_status_observations
             WHERE agent_id = ?1
             ORDER BY seq_id ASC",
        )
        .unwrap();
    let observations = statement
        .query_map([agent_id], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str::<ProviderObservation>(&row.unwrap()).unwrap())
        .collect::<Vec<_>>();
    let has_turn = |expected| {
        observations.iter().any(|observation| {
            observation.turn_id.as_deref() == Some(job_id)
                && observation.kind == ProviderObservationKind::Turn(expected)
        })
    };

    assert!(
        has_turn(ProviderTurnState::Queued),
        "{agent_id} missed Queued"
    );
    assert!(
        has_turn(ProviderTurnState::Delivering),
        "{agent_id} missed Delivering"
    );
    assert!(
        has_turn(ProviderTurnState::Delivered),
        "{agent_id} missed Delivered"
    );
    assert!(
        has_turn(ProviderTurnState::Working),
        "{agent_id} missed Working"
    );
    let has_working_source = |source| {
        observations.iter().any(|observation| {
            observation.turn_id.as_deref() == Some(job_id)
                && observation.kind == ProviderObservationKind::Turn(ProviderTurnState::Working)
                && observation.source == source
        })
    };
    match (agent_id, hook_push_enabled) {
        ("ag_real_codex" | "ag_real_claude" | "ag_real_antigravity", true) => assert!(
            has_working_source(EvidenceSource::OfficialHook),
            "{agent_id} hook-enabled run did not use its native working hook"
        ),
        ("ag_real_codex" | "ag_real_claude" | "ag_real_antigravity", false) => assert!(
            has_working_source(EvidenceSource::Transcript),
            "{agent_id} hook-disabled run did not fall back to transcript evidence"
        ),
        ("ag_real_bash", _) => assert!(
            has_working_source(EvidenceSource::TerminalPane),
            "bash working evidence must remain terminal-pane based"
        ),
        _ => unreachable!("unexpected real provider agent {agent_id}"),
    }
    assert!(
        has_turn(ProviderTurnState::Completed),
        "{agent_id} missed Completed"
    );
    if hook_push_enabled && agent_id != "ag_real_bash" {
        assert!(
            observations
                .iter()
                .any(|observation| observation.source == EvidenceSource::OfficialHook),
            "{agent_id} hook-enabled run received no native hook observation"
        );
    }
    assert!(
        observations.iter().any(|observation| {
            observation.turn_id.is_none()
                && observation.kind == ProviderObservationKind::Turn(ProviderTurnState::Ready)
        }),
        "{agent_id} missed Ready"
    );
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

async fn wait_job(h: &RealHarness, provider: &str, job_id: &str) -> String {
    let result = handle_job_wait(json!({ "job_id": job_id, "timeout": 45 }), &h.ctx)
        .await
        .unwrap_or_else(|error| {
            let diagnostic_dir = std::env::temp_dir().join(format!(
                "ah-real-provider-failure-{}",
                uuid::Uuid::new_v4()
            ));
            let diagnostic_status = Command::new("cp")
                .arg("-a")
                .arg(&h.ctx.state_dir)
                .arg(&diagnostic_dir)
                .status()
                .ok();
            let conn = h.ctx.db.conn();
            let (agent_id, session_id): (String, String) = conn
                .query_row(
                    "SELECT jobs.agent_id, agents.session_id FROM jobs JOIN agents ON agents.id=jobs.agent_id WHERE jobs.id=?1",
                    [job_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let sandbox_dir = h
                .ctx
                .state_dir
                .join("sandboxes")
                .join(session_id)
                .join(&agent_id);
            let provider_home = ah::home_materialization::sandbox_home_for_sandbox_dir(&sandbox_dir)
                .unwrap();
            let diagnostic_home_status = Command::new("cp")
                .arg("-a")
                .arg(&provider_home)
                .arg(diagnostic_dir.join("provider-home"))
                .status()
                .ok();
            let pane = Command::new("tmux")
                .args([
                    "-L",
                    h.ctx.tmux_server.socket_name(),
                    "capture-pane",
                    "-p",
                    "-t",
                    &agent_session_name(&agent_id),
                ])
                .output()
                .ok()
                .map(|output| String::from_utf8_lossy(&output.stdout).to_string());
            let mut agent_statement = conn
                .prepare("SELECT id, provider, state, sub_state, error_code FROM agents ORDER BY id")
                .unwrap();
            let agents = agent_statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let mut job_statement = conn
                .prepare("SELECT id, agent_id, status, error_reason FROM jobs ORDER BY id")
                .unwrap();
            let jobs = job_statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            panic!(
                "{provider} job {job_id} did not complete: {error}; agents={agents:?}; jobs={jobs:?}; diagnostic_dir={}; diagnostic_copy={diagnostic_status:?}; diagnostic_home_copy={diagnostic_home_status:?}; pane={pane:?}",
                diagnostic_dir.display()
            )
        });
    if result["status"] != "COMPLETED" {
        let agent_id: String = h
            .ctx
            .db
            .conn()
            .query_row("SELECT agent_id FROM jobs WHERE id=?1", [job_id], |row| {
                row.get(0)
            })
            .unwrap();
        let pane = Command::new("tmux")
            .args([
                "-L",
                h.ctx.tmux_server.socket_name(),
                "capture-pane",
                "-p",
                "-e",
                "-t",
                &agent_session_name(&agent_id),
            ])
            .output()
            .ok()
            .map(|output| String::from_utf8_lossy(&output.stdout).to_string());
        panic!(
            "{provider} job {job_id} returned a terminal non-completed result: {result}; pane={pane:?}"
        );
    }
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
