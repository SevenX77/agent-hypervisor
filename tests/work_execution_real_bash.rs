#![cfg(target_os = "linux")]

use ah::db;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_spawn, handle_job_submit, handle_job_wait, handle_session_create,
    handle_session_kill,
};
use ah::runtime_observation::{
    EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderTurnState,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, compute_socket_name};
use serde_json::json;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod common;

struct RealBashHarness {
    ctx: Ctx,
    sessions: Arc<Mutex<Vec<String>>>,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl RealBashHarness {
    fn new() -> Self {
        assert!(
            which::which("bash").is_ok()
                && which::which("tmux").is_ok()
                && common::can_use_systemd_run(),
            "real Bash test requires bash, tmux, and user systemd; set CCB_TEST_SKIP_REAL_PROVIDER=1 to opt out"
        );
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
            claude_gateway: Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        ah::orchestrator::spawn_orchestrator_task(ctx.clone());
        Self {
            ctx,
            sessions: Arc::new(Mutex::new(Vec::new())),
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }

    async fn create_session(&self) -> String {
        let result = handle_session_create(
            json!({
                "project_id": "work-execution-real-bash",
                "absolute_path": self.ctx.state_dir.display().to_string(),
            }),
            &self.ctx,
        )
        .await
        .unwrap();
        let session_id = result["session_id"].as_str().unwrap().to_string();
        self.sessions.lock().unwrap().push(session_id.clone());
        session_id
    }
}

impl Drop for RealBashHarness {
    fn drop(&mut self) {
        for session_id in self.sessions.lock().unwrap().iter() {
            let _ = Command::new("systemctl")
                .args([
                    "--user",
                    "stop",
                    &format!("ahd-session-{session_id}.service"),
                ])
                .output();
        }
        let _ = Command::new("tmux")
            .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
            .output();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_bash_roundtrip_uses_the_declared_terminal_fallback() {
    if std::env::var("CCB_TEST_SKIP_REAL_PROVIDER").as_deref() == Ok("1") {
        return;
    }

    let harness = RealBashHarness::new();
    let session_id = harness.create_session().await;
    let agent_id = "work_execution_real_bash";
    handle_agent_spawn(
        json!({
            "session_id": session_id,
            "agent_id": agent_id,
            "provider": "bash",
        }),
        &harness.ctx,
    )
    .await
    .unwrap();
    wait_for_agent_state(&harness.ctx, agent_id, "IDLE", Duration::from_secs(20)).await;

    let submitted = handle_job_submit(
        json!({
            "agent_id": agent_id,
            "text": "printf 'bash-terminal-fallback-ok\\n'\n",
            "request_id": format!("req_{}", uuid::Uuid::new_v4()),
        }),
        &harness.ctx,
    )
    .await
    .unwrap();
    let job_id = submitted["job_id"].as_str().unwrap().to_string();
    let result = handle_job_wait(
        json!({
            "job_id": job_id,
            "timeout": 20,
        }),
        &harness.ctx,
    )
    .await
    .unwrap();

    assert_eq!(result["status"], "COMPLETED");
    assert!(
        result["reply_text"]
            .as_str()
            .is_some_and(|reply| reply.contains("bash-terminal-fallback-ok"))
    );
    assert_terminal_turn_observations(&harness.ctx, agent_id, &job_id);
    wait_for_agent_state(&harness.ctx, agent_id, "IDLE", Duration::from_secs(5)).await;

    let _ = handle_session_kill(
        json!({ "session_id": session_id, "force": true }),
        &harness.ctx,
    )
    .await;
}

fn assert_terminal_turn_observations(ctx: &Ctx, agent_id: &str, job_id: &str) {
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

    for expected in [ProviderTurnState::Working, ProviderTurnState::Completed] {
        assert!(
            observations.iter().any(|observation| {
                observation.turn_id.as_deref() == Some(job_id)
                    && observation.source == EvidenceSource::TerminalPane
                    && observation.kind == ProviderObservationKind::Turn(expected)
            }),
            "missing Bash {expected:?} observation from the declared terminal fallback"
        );
    }
}

async fn wait_for_agent_state(ctx: &Ctx, agent_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let current = ctx
            .db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .ok();
        if current.as_deref() == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for agent {agent_id} state {expected}");
}
