#![cfg(unix)]

mod common;

use ah::cli::config::{
    AgentConfig, MasterConfig, ProjectConfig, SandboxConfig, load_project_config,
};
use ah::cli::rpc_client::{CliError, RpcClient, RpcFuture};
use ah::cli::start::start_project;
use ah::db;
use ah::db::agents::{insert_agent, query_agent};
use ah::db::jobs::{insert_job, query_job};
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_send, handle_agent_spawn, handle_job_cancel, handle_session_create,
    handle_session_kill, handle_session_list, handle_session_spawn_master_pane,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, agent_session_name, compute_socket_name};
use common::{DaemonIdentityEnvGuard, scope_policy_for_test};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Harness {
    ctx: Ctx,
    _env_guard: DaemonIdentityEnvGuard,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl Harness {
    fn new() -> Self {
        which::which("tmux").expect("tmux binary required for mvp9 acceptance tests");
        let env_guard = DaemonIdentityEnvGuard::scrub();
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let state_dir_path = state_dir.path().to_path_buf();
        let socket_name = compute_socket_name(state_dir.path());
        let policy = scope_policy_for_test(&socket_name);
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir_path.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new_with_policy(state_dir.path(), policy)),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        Self {
            ctx,
            _env_guard: env_guard,
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = Command::new("tmux")
            .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
            .output();
        let socket_path = format!(
            "/tmp/tmux-{}/{}",
            unsafe { libc::geteuid() },
            self.ctx.tmux_server.socket_name()
        );
        let _ = std::fs::remove_file(socket_path);
    }
}

fn tmux_sessions(h: &Harness) -> Vec<String> {
    let output = Command::new("tmux")
        .args([
            "-L",
            h.ctx.tmux_server.socket_name(),
            "list-sessions",
            "-F",
            "#{session_name}",
        ])
        .output()
        .unwrap();
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(ToString::to_string)
        .collect()
}

async fn wait_for_tmux_sessions(h: &Harness, expected: &[&str], timeout: Duration) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    let mut sessions = tmux_sessions(h);
    while Instant::now() < deadline {
        if expected
            .iter()
            .all(|name| sessions.iter().any(|session| session == name))
        {
            return sessions;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        sessions = tmux_sessions(h);
    }
    sessions
}

struct HandlerClient {
    ctx: Ctx,
}

impl RpcClient for HandlerClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            match method {
                "session.list" => handle_session_list(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "session.create" => handle_session_create(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "agent.spawn" => handle_agent_spawn(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "session.kill" => handle_session_kill(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                "session.spawn_master_pane" => handle_session_spawn_master_pane(params, &self.ctx)
                    .await
                    .map_err(map_rpc_error),
                other => Err(CliError::InvalidResponse(format!(
                    "unexpected method in test client: {other}"
                ))),
            }
        })
    }
}

struct FailingSpawnClient {
    spawn_calls: AtomicUsize,
}

impl RpcClient for FailingSpawnClient {
    fn call<'a>(&'a self, method: &'a str, _params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            match method {
                "session.list" => Ok(json!({ "sessions": [] })),
                "session.create" => Ok(json!({ "session_id": "sess_fail" })),
                "session.spawn_master_pane" => Ok(json!({ "pane_id": "%0" })),
                "agent.spawn" => {
                    let call = self.spawn_calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        Ok(json!({ "state": "SPAWNING", "pid": 123, "pane_id": "%1" }))
                    } else {
                        Err(CliError::Rpc {
                            code: -32000,
                            message: "spawn failed".into(),
                        })
                    }
                }
                "session.kill" => {
                    Ok(json!({ "session_id": "sess_fail", "state": "KILLED", "killed_agents": 1 }))
                }
                other => Err(CliError::InvalidResponse(format!(
                    "unexpected method in failing client: {other}"
                ))),
            }
        })
    }
}

struct RecordingStartClient {
    calls: Mutex<Vec<(String, Value)>>,
}

impl RpcClient for RecordingStartClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_string(), params.clone()));
            match method {
                "session.list" => Ok(json!({ "sessions": [] })),
                "session.create" => Ok(json!({ "session_id": "sess_env" })),
                "session.spawn_master_pane" => Ok(json!({ "pane_id": "%0" })),
                "agent.spawn" => Ok(json!({ "state": "SPAWNING", "pid": 123, "pane_id": "%1" })),
                other => Err(CliError::InvalidResponse(format!(
                    "unexpected method in recording client: {other}"
                ))),
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_config_parse_and_batch_spawn() {
    let h = Harness::new();
    let source_config_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("ah.toml");
    let config_fixture = tempfile::TempDir::new().unwrap();
    let shared_credentials_dir = tempfile::TempDir::new().unwrap();
    let config_path = config_fixture.path().join("ah.toml");
    // The shipped template already declares [providers.claude]; repoint it at a
    // temp directory instead of appending a second table.
    let config_body = std::fs::read_to_string(&source_config_path)
        .unwrap()
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("shared_credentials_dir") {
                format!(
                    "shared_credentials_dir = \"{}\"",
                    shared_credentials_dir.path().display()
                )
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&config_path, config_body).unwrap();
    let mut config = load_project_config(&config_path).unwrap();
    config.master.enabled = false;
    config.agents = config
        .agents
        .into_iter()
        .take(2)
        .collect::<BTreeMap<_, _>>();
    let client = HandlerClient { ctx: h.ctx.clone() };

    let summary = start_project(
        &client,
        config,
        &config_path,
        h.ctx.state_dir.clone(),
        false,
    )
    .await
    .unwrap();

    assert!(summary.session_id.starts_with("sess_"));
    assert_eq!(summary.agents.len(), 2);
    for agent in &summary.agents {
        let db_agent = query_agent(h.ctx.db.clone(), agent.agent_id.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db_agent.session_id, summary.session_id);
        assert_eq!(db_agent.provider, "bash");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_recycles_killed_agent_slot_on_restart() {
    let h = Harness::new();
    let config = toml::from_str::<ProjectConfig>(
        r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "bash"
"#,
    )
    .unwrap();
    let client = HandlerClient { ctx: h.ctx.clone() };
    let project_root = h.ctx.state_dir.join("restart-project");
    std::fs::create_dir_all(&project_root).unwrap();

    let first = start_project(
        &client,
        config.clone(),
        std::path::Path::new("test-ah.toml"),
        project_root.clone(),
        false,
    )
    .await
    .unwrap();
    handle_session_kill(
        json!({
            "session_id": first.session_id,
            "force": true,
        }),
        &h.ctx,
    )
    .await
    .unwrap();

    let second = start_project(
        &client,
        config,
        std::path::Path::new("test-ah.toml"),
        project_root,
        false,
    )
    .await
    .unwrap();

    assert_ne!(second.session_id, first.session_id);
    let db_agent = query_agent(h.ctx.db.clone(), "a1".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db_agent.session_id, second.session_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_rollback_on_spawn_failure() {
    let config = toml::from_str::<ProjectConfig>(
        r#"
version = "1"

[agents.a1]
provider = "bash"

[agents.a2]
provider = "bash"
"#,
    )
    .unwrap();
    let config_path = std::path::Path::new("test-ah.toml");
    let client = FailingSpawnClient {
        spawn_calls: AtomicUsize::new(0),
    };

    let err = start_project(
        &client,
        config,
        config_path,
        std::env::current_dir().unwrap(),
        false,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("rolled back session sess_fail"));
    assert_eq!(client.spawn_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_passes_merged_env_to_agent_spawn() {
    let mut global_env = std::collections::HashMap::new();
    global_env.insert("GLOBAL_ONLY".to_string(), "1".to_string());
    global_env.insert("OVERRIDE_ME".to_string(), "global".to_string());
    let mut agent_env = std::collections::HashMap::new();
    agent_env.insert("OVERRIDE_ME".to_string(), "agent".to_string());
    agent_env.insert("AGENT_ONLY".to_string(), "2".to_string());
    let mut agents = BTreeMap::new();
    agents.insert(
        "ag_env".to_string(),
        AgentConfig {
            provider: "bash".to_string(),
            env: agent_env,
            hooks: Default::default(),
            plugins: Default::default(),
            skills: Default::default(),
            bundle: Default::default(),
            settings: Default::default(),
        },
    );
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
        completion: Default::default(),
        daemon: Default::default(),
        providers: Default::default(),
        env: global_env,
        sandbox: SandboxConfig::default(),
        agents,
    };
    let client = RecordingStartClient {
        calls: Mutex::new(Vec::new()),
    };

    start_project(
        &client,
        config,
        std::path::Path::new("test-ah.toml"),
        std::env::current_dir().unwrap(),
        false,
    )
    .await
    .unwrap();

    let calls = client.calls.lock().unwrap();
    let (_, spawn_params) = calls
        .iter()
        .find(|(method, _)| method == "agent.spawn")
        .unwrap();
    // ISSUE-13 §3a: the client stopped pre-merging project [env] into the agent env.
    // It now ships two RAW halves and the SERVER merges them (bare_config_env):
    //   extra_env_vars = the bare `[agents.X.env]` only
    //   config_env     = the project-level top-level `[env]`
    // This test pins the CLIENT payload shape (RecordingStartClient short-circuits before
    // the server merge). The server-side merge itself is driven end-to-end over the real
    // RPC path by tests/issue13_respawn_storm.rs (unchanged_config_env_does_not_drift /
    // config_env_edit_still_drifts) and by the audited commit A (af32fa8).

    // extra_env_vars = bare agent.env — and must NOT smuggle the project globals anymore.
    assert_eq!(spawn_params["extra_env_vars"]["AGENT_ONLY"], "2");
    assert_eq!(spawn_params["extra_env_vars"]["OVERRIDE_ME"], "agent");
    assert!(
        spawn_params["extra_env_vars"]["GLOBAL_ONLY"].is_null(),
        "regression: client must not pre-merge project [env] into extra_env_vars"
    );

    // config_env = project-level [env], carried as its own top-level param.
    assert_eq!(spawn_params["config_env"]["GLOBAL_ONLY"], "1");
    assert_eq!(spawn_params["config_env"]["OVERRIDE_ME"], "global");

    // Override contract intact: the server merges config_env then overlays agent.env (agent
    // wins) — identical to bare_config_env(config_env, agent.env). Assert the effective
    // OVERRIDE_ME resolves to the agent value via that equivalent merge.
    let to_map = |v: &Value| -> std::collections::HashMap<String, String> {
        v.as_object()
            .unwrap()
            .iter()
            .map(|(k, val)| (k.clone(), val.as_str().unwrap().to_string()))
            .collect()
    };
    let mut effective = to_map(&spawn_params["config_env"]);
    effective.extend(to_map(&spawn_params["extra_env_vars"]));
    assert_eq!(
        effective["OVERRIDE_ME"], "agent",
        "agent [env] must override project [env] in the server-side merge"
    );
    assert_eq!(effective["GLOBAL_ONLY"], "1");
    assert_eq!(effective["AGENT_ONLY"], "2");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_session_kill_cleans_resources() {
    let h = Harness::new();
    let session = handle_session_create(
        json!({
            "project_id": "p_kill",
            "absolute_path": h.ctx.state_dir.display().to_string(),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let session_id = session["session_id"].as_str().unwrap().to_string();
    for agent_id in ["ag_kill_1", "ag_kill_2"] {
        handle_agent_spawn(
            json!({
                "session_id": session_id,
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &h.ctx,
        )
        .await
        .unwrap();
    }

    let result = handle_session_kill(
        json!({
            "session_id": session_id,
            "force": true,
        }),
        &h.ctx,
    )
    .await
    .unwrap();

    assert_eq!(result["state"], "KILLED");
    assert_eq!(result["killed_agents"], 2);
    for agent_id in ["ag_kill_1", "ag_kill_2"] {
        let agent = query_agent(h.ctx.db.clone(), agent_id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(agent.state, "KILLED");
        assert!(!ah::agent_io::contains(agent_id));
        assert!(
            !h.ctx
                .state_dir
                .join("pipes")
                .join(format!("{agent_id}.fifo"))
                .exists()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_session_kill_cleans_prompt_pending_agent_session_without_force() {
    let h = Harness::new();
    let session = handle_session_create(
        json!({
            "project_id": "p_kill_prompt_pending",
            "absolute_path": h.ctx.state_dir.display().to_string(),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let session_id = session["session_id"].as_str().unwrap().to_string();
    let agent_id = "ag_kill_prompt_pending";
    h.ctx
        .tmux_server
        .ensure_session(agent_session_name(agent_id), h.ctx.state_dir.clone())
        .await
        .unwrap();
    let pane_pid = Command::new("tmux")
        .args([
            "-L",
            h.ctx.tmux_server.socket_name(),
            "display-message",
            "-p",
            "-t",
            &agent_session_name(agent_id),
            "#{pane_pid}",
        ])
        .output()
        .unwrap();
    assert!(pane_pid.status.success());
    let pane_pid = String::from_utf8_lossy(&pane_pid.stdout)
        .trim()
        .parse::<i64>()
        .unwrap();
    ah::db::agents::insert_agent(
        h.ctx.db.clone(),
        agent_id.to_string(),
        session_id.clone(),
        "bash".to_string(),
        "PROMPT_PENDING".to_string(),
        Some(pane_pid),
    )
    .await
    .unwrap();

    let result = handle_session_kill(
        json!({
            "session_id": session_id,
        }),
        &h.ctx,
    )
    .await
    .unwrap();

    assert_eq!(result["state"], "KILLED");
    let agent = query_agent(h.ctx.db.clone(), agent_id.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agent.state, "KILLED");
    let sessions = tmux_sessions(&h);
    assert!(
        !sessions
            .iter()
            .any(|session| session == "agent_ag_kill_prompt_pending"),
        "prompt pending agent tmux session should be removed; sessions={sessions:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reconcile_cleans_dead_pids_on_boot() {
    let h = Harness::new();
    ah::db::sessions::insert_session(
        h.ctx.db.clone(),
        "s_reconcile".to_string(),
        "p_reconcile".to_string(),
        h.ctx.state_dir.display().to_string(),
    )
    .await
    .unwrap();
    ah::db::agents::insert_agent(
        h.ctx.db.clone(),
        "ag_reconcile".to_string(),
        "s_reconcile".to_string(),
        "bash".to_string(),
        "BUSY".to_string(),
        Some(999_999_999),
    )
    .await
    .unwrap();
    insert_job(
        h.ctx.db.clone(),
        "job_reconcile".to_string(),
        "ag_reconcile".to_string(),
        None,
        "echo lost\n".to_string(),
    )
    .await
    .unwrap();
    h.ctx
        .db
        .conn()
        .execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_reconcile'",
            [],
        )
        .unwrap();

    let count = ah::db::system::reconcile_startup(h.ctx.db.clone())
        .await
        .unwrap();
    let agent = query_agent(h.ctx.db.clone(), "ag_reconcile".to_string())
        .await
        .unwrap()
        .unwrap();
    let job = query_job(h.ctx.db.clone(), "job_reconcile".to_string())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(count, 1);
    assert_eq!(agent.state, "CRASHED");
    assert_eq!(
        agent.error_code.as_deref(),
        Some("STARTUP_RECONCILE_DEAD_PID")
    );
    assert_eq!(job.status, "FAILED");
    assert_eq!(
        job.error_reason.as_deref(),
        Some("STARTUP_RECONCILE_DEAD_PID")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reconcile_crashes_alive_agent_when_fifo_missing() {
    let h = Harness::new();
    ah::db::sessions::insert_session(
        h.ctx.db.clone(),
        "s_reconcile_fifo".to_string(),
        "p_reconcile_fifo".to_string(),
        h.ctx.state_dir.display().to_string(),
    )
    .await
    .unwrap();
    ah::db::agents::insert_agent(
        h.ctx.db.clone(),
        "ag_reconcile_fifo".to_string(),
        "s_reconcile_fifo".to_string(),
        "bash".to_string(),
        "BUSY".to_string(),
        Some(std::process::id() as i64),
    )
    .await
    .unwrap();
    insert_job(
        h.ctx.db.clone(),
        "job_reconcile_fifo".to_string(),
        "ag_reconcile_fifo".to_string(),
        None,
        "echo lost\n".to_string(),
    )
    .await
    .unwrap();
    h.ctx
        .db
        .conn()
        .execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = 'job_reconcile_fifo'",
            [],
        )
        .unwrap();

    let count =
        ah::db::system::reconcile_startup_with_state_dir(h.ctx.db.clone(), h.ctx.state_dir.clone())
            .await
            .unwrap();
    let agent = query_agent(h.ctx.db.clone(), "ag_reconcile_fifo".to_string())
        .await
        .unwrap()
        .unwrap();
    let job = query_job(h.ctx.db.clone(), "job_reconcile_fifo".to_string())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(count, 1);
    assert_eq!(agent.state, "CRASHED");
    assert_eq!(
        agent.error_code.as_deref(),
        Some("STARTUP_RECONCILE_FIFO_REATTACH_FAILED")
    );
    assert_eq!(job.status, "FAILED");
    assert_eq!(
        job.error_reason.as_deref(),
        Some("STARTUP_RECONCILE_FIFO_REATTACH_FAILED")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_launcher_batch_spawn_uses_independent_agent_sessions() {
    let h = Harness::new();
    let config = toml::from_str::<ProjectConfig>(
        r#"
version = "1"

[agents.a1]
provider = "bash"

[agents.a2]
provider = "bash"

[agents.a3]
provider = "bash"
"#,
    )
    .unwrap();
    let client = HandlerClient { ctx: h.ctx.clone() };
    let project_root = h.ctx.state_dir.join("ccb-project");
    std::fs::create_dir_all(&project_root).unwrap();

    let summary = start_project(
        &client,
        config,
        std::path::Path::new("test-ah.toml"),
        project_root,
        false,
    )
    .await
    .unwrap();

    assert_eq!(summary.agents.len(), 3);
    let expected_sessions = ["agent_a1", "agent_a2", "agent_a3"];
    // 20s, not 5s: the wait returns as soon as every session exists, so the
    // budget only matters when the full suite is competing for the machine —
    // where a slow tmux spawn was reading as a missing session.
    let sessions = wait_for_tmux_sessions(&h, &expected_sessions, Duration::from_secs(20)).await;
    for agent_id in ["a1", "a2", "a3"] {
        assert!(
            sessions
                .iter()
                .any(|session| session == &format!("agent_{agent_id}")),
            "missing independent tmux session for {agent_id}; sessions={sessions:?}"
        );
    }
    assert!(
        !sessions.iter().any(|session| session == "ccbd-agents"),
        "legacy shared session should not be created; sessions={sessions:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_agent_spawn_serializes_window_creation() {
    let h = Harness::new();
    let session = handle_session_create(
        json!({
            "project_id": "p_concurrent",
            "absolute_path": h.ctx.state_dir.join("concurrent").display().to_string(),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let session_id = session["session_id"].as_str().unwrap().to_string();

    let f1 = handle_agent_spawn(
        json!({ "session_id": session_id, "agent_id": "ag_c1", "provider": "bash" }),
        &h.ctx,
    );
    let f2 = handle_agent_spawn(
        json!({ "session_id": session_id, "agent_id": "ag_c2", "provider": "bash" }),
        &h.ctx,
    );
    let f3 = handle_agent_spawn(
        json!({ "session_id": session_id, "agent_id": "ag_c3", "provider": "bash" }),
        &h.ctx,
    );
    let (r1, r2, r3) = tokio::join!(f1, f2, f3);
    r1.unwrap();
    r2.unwrap();
    r3.unwrap();

    let expected_sessions = ["agent_ag_c1", "agent_ag_c2", "agent_ag_c3"];
    // 20s, not 5s: the wait returns as soon as every session exists, so the
    // budget only matters when the full suite is competing for the machine —
    // where a slow tmux spawn was reading as a missing session.
    let sessions = wait_for_tmux_sessions(&h, &expected_sessions, Duration::from_secs(20)).await;
    for agent_id in ["ag_c1", "ag_c2", "ag_c3"] {
        assert!(
            sessions
                .iter()
                .any(|session| session == &format!("agent_{agent_id}")),
            "missing independent tmux session for {agent_id}; sessions={sessions:?}"
        );
    }
    assert!(
        !sessions.iter().any(|session| session == "ccbd-agents"),
        "legacy shared session should not be created; sessions={sessions:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_job_cancel_queued() {
    let h = Harness::new();
    ah::db::sessions::insert_session(
        h.ctx.db.clone(),
        "s_cancel_q".to_string(),
        "p_cancel_q".to_string(),
        h.ctx.state_dir.join("cancel-q").display().to_string(),
    )
    .await
    .unwrap();
    ah::db::agents::insert_agent(
        h.ctx.db.clone(),
        "ag_cancel_q".to_string(),
        "s_cancel_q".to_string(),
        "bash".to_string(),
        "IDLE".to_string(),
        Some(std::process::id() as i64),
    )
    .await
    .unwrap();
    insert_job(
        h.ctx.db.clone(),
        "job_cancel_q".to_string(),
        "ag_cancel_q".to_string(),
        None,
        "echo queued\n".to_string(),
    )
    .await
    .unwrap();

    let result = handle_job_cancel(json!({ "job_id": "job_cancel_q" }), &h.ctx)
        .await
        .unwrap();
    let job = query_job(h.ctx.db.clone(), "job_cancel_q".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["status"], "CANCELLED");
    assert_eq!(job.status, "CANCELLED");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_job_cancel_dispatched_sends_sigint() {
    let h = Harness::new();
    let session = handle_session_create(
        json!({
            "project_id": "p_cancel_d",
            "absolute_path": h.ctx.state_dir.join("cancel-d").display().to_string(),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let session_id = session["session_id"].as_str().unwrap().to_string();
    handle_agent_spawn(
        json!({ "session_id": session_id, "agent_id": "ag_cancel_d", "provider": "bash" }),
        &h.ctx,
    )
    .await
    .unwrap();
    wait_for_agent_state(&h, "ag_cancel_d", "IDLE", Duration::from_secs(10)).await;

    let sent = handle_agent_send(
        json!({
            "agent_id": "ag_cancel_d",
            "text": "sleep 10; echo should_not_finish\n",
            "request_id": "req_cancel_d"
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let seq_id = sent["seq_id"].as_i64().unwrap();
    insert_job(
        h.ctx.db.clone(),
        "job_cancel_d".to_string(),
        "ag_cancel_d".to_string(),
        None,
        "sleep 10; echo should_not_finish\n".to_string(),
    )
    .await
    .unwrap();
    h.ctx
        .db
        .conn()
        .execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch(), dispatched_at_seq_id = ? WHERE id = 'job_cancel_d'",
            [seq_id],
        )
        .unwrap();
    let start = Instant::now();

    let result = handle_job_cancel(json!({ "job_id": "job_cancel_d" }), &h.ctx)
        .await
        .unwrap();
    assert_eq!(result["status"], "CANCEL_REQUESTED");
    wait_for_job_status(
        &h,
        result["job_id"].as_str().unwrap(),
        "CANCELLED",
        Duration::from_secs(20),
    )
    .await;
    assert!(start.elapsed() < Duration::from_secs(20));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_antigravity_cancel_dispatched_sends_escape() {
    let h = Harness::new();
    let project_path = h.ctx.state_dir.join("cancel-agy");
    let session = handle_session_create(
        json!({
            "project_id": "p_cancel_agy",
            "absolute_path": project_path.display().to_string(),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let session_id = session["session_id"].as_str().unwrap().to_string();
    let agent_session = agent_session_name("ag_cancel_agy");
    h.ctx
        .tmux_server
        .ensure_session(agent_session.clone(), project_path.clone())
        .await
        .unwrap();
    let pane = h
        .ctx
        .tmux_server
        .spawn_window(
            agent_session,
            "ag_cancel_agy".to_string(),
            project_path,
            vec![
                "bash".to_string(),
                "--noprofile".to_string(),
                "--norc".to_string(),
                "-i".to_string(),
            ],
        )
        .await
        .unwrap();
    insert_agent(
        h.ctx.db.clone(),
        "ag_cancel_agy".to_string(),
        session_id.clone(),
        "antigravity".to_string(),
        "BUSY".to_string(),
        Some(std::process::id() as i64),
    )
    .await
    .unwrap();
    let fifo_path = h.ctx.state_dir.join("pipes").join("ag_cancel_agy.fifo");
    std::fs::create_dir_all(fifo_path.parent().unwrap()).unwrap();
    std::fs::write(&fifo_path, b"").unwrap();
    let reader_handle = tokio::spawn(async {
        std::future::pending::<()>().await;
    });
    ah::agent_io::register(
        "ag_cancel_agy".to_string(),
        ah::agent_io::AgentIoEntry {
            session_id: session_id.clone(),
            pane_id: pane.clone(),
            expected_pid: None,
            reader_handle,
            fifo_path,
            socket_name: h.ctx.tmux_server.socket_name().to_string(),
            idle_scan_enabled: Arc::new(AtomicBool::new(true)),
        },
    );

    let escape_record = h.ctx.state_dir.join("antigravity-cancel-byte.bin");
    let ready_marker = h.ctx.state_dir.join("antigravity-cancel-ready.marker");
    let probe = format!(
        "import os,pathlib,select,sys,termios,tty; \
         fd=sys.stdin.fileno(); old=termios.tcgetattr(fd); tty.setraw(fd); \
         pathlib.Path({:?}).write_bytes(b'ready'); \
         data=sys.stdin.buffer.read(1); ready,_,_=select.select([sys.stdin],[],[],0.2); \
         data += os.read(fd,16) if ready else b''; \
         termios.tcsetattr(fd, termios.TCSADRAIN, old); \
         pathlib.Path({:?}).write_bytes(data)",
        ready_marker.display().to_string(),
        escape_record.display().to_string(),
    );
    let command = format!("python3 -c {probe:?}");
    h.ctx
        .tmux_server
        .send_keys_literal(pane.clone(), command)
        .await
        .unwrap();
    h.ctx
        .tmux_server
        .send_keys_keysym(pane.clone(), "Enter".to_string())
        .await
        .unwrap();
    let seq_id = 0;
    let ready_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < ready_deadline && !ready_marker.exists() {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        ready_marker.exists(),
        "antigravity cancel probe did not enter raw-mode read before timeout: {}",
        ready_marker.display()
    );

    insert_job(
        h.ctx.db.clone(),
        "job_cancel_agy".to_string(),
        "ag_cancel_agy".to_string(),
        None,
        "antigravity busy prompt\n".to_string(),
    )
    .await
    .unwrap();
    h.ctx
        .db
        .conn()
        .execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch(), dispatched_at_seq_id = ? WHERE id = 'job_cancel_agy'",
            [seq_id],
        )
        .unwrap();

    let result = handle_job_cancel(json!({ "job_id": "job_cancel_agy" }), &h.ctx)
        .await
        .unwrap();
    assert_eq!(result["status"], "CANCEL_REQUESTED");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && !escape_record.exists() {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let recorded = std::fs::read(&escape_record).unwrap_or_default();
    assert_eq!(
        recorded,
        vec![0x1b],
        "antigravity cancel must send a single Escape byte, got {recorded:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_requested_skips_prompt_only_swallow() {
    let h = Harness::new();
    ah::db::sessions::insert_session(
        h.ctx.db.clone(),
        "s_prompt_cancel".to_string(),
        "p_prompt_cancel".to_string(),
        h.ctx.state_dir.join("prompt-cancel").display().to_string(),
    )
    .await
    .unwrap();
    ah::db::agents::insert_agent(
        h.ctx.db.clone(),
        "ag_prompt_cancel".to_string(),
        "s_prompt_cancel".to_string(),
        "bash".to_string(),
        "IDLE".to_string(),
        Some(std::process::id() as i64),
    )
    .await
    .unwrap();
    let before = ah::db::events::insert_event(
        h.ctx.db.clone(),
        "ag_prompt_cancel".to_string(),
        None,
        "command_received".to_string(),
        r#"{"cmd":"sleep 10","status":"SENT"}"#.to_string(),
    )
    .await
    .unwrap();
    ah::db::events::insert_event(
        h.ctx.db.clone(),
        "ag_prompt_cancel".to_string(),
        None,
        "output_chunk".to_string(),
        r#"{"text":"$ "}"#.to_string(),
    )
    .await
    .unwrap();
    insert_job(
        h.ctx.db.clone(),
        "job_prompt_cancel".to_string(),
        "ag_prompt_cancel".to_string(),
        None,
        "sleep 10\n".to_string(),
    )
    .await
    .unwrap();
    ah::db::jobs::claim_next_job(h.ctx.db.clone(), "ag_prompt_cancel".to_string())
        .await
        .unwrap();
    ah::db::jobs::update_dispatched_seq_id(
        h.ctx.db.clone(),
        "job_prompt_cancel".to_string(),
        before,
    )
    .await
    .unwrap();
    ah::db::agents::update_agent_state(
        h.ctx.db.clone(),
        "ag_prompt_cancel".to_string(),
        "BUSY".to_string(),
    )
    .await
    .unwrap();
    ah::db::jobs::request_dispatched_job_cancel(h.ctx.db.clone(), "job_prompt_cancel".to_string())
        .await
        .unwrap();

    let (changes, affected_job) = ah::db::state_machine::mark_agent_idle_matched(
        h.ctx.db.clone(),
        "ag_prompt_cancel".to_string(),
    )
    .await
    .unwrap();
    if changes > 0 {
        // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
        if let Some(job_id) = affected_job {
            ah::orchestrator::pubsub::notify_job_update(&job_id);
        }
        ah::orchestrator::wake_up();
    }
    let job = query_job(h.ctx.db.clone(), "job_prompt_cancel".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, "CANCELLED");
}

async fn wait_for_agent_state(h: &Harness, agent_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let agent = query_agent(h.ctx.db.clone(), agent_id.to_string())
            .await
            .unwrap()
            .unwrap();
        if agent.state == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("agent {agent_id} did not reach {expected}");
}

async fn wait_for_job_status(h: &Harness, job_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let job = query_job(h.ctx.db.clone(), job_id.to_string())
            .await
            .unwrap()
            .unwrap();
        if job.status == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("job {job_id} did not reach {expected}");
}

fn map_rpc_error(err: ah::error::CcbdError) -> CliError {
    CliError::Rpc {
        code: -32000,
        message: err.to_string(),
    }
}
