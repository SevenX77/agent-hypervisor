#![cfg(target_os = "linux")]

use ah::db;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_spawn, handle_job_submit, handle_job_wait, handle_session_create,
    handle_session_kill,
};
use ah::runtime_events::{RuntimeSnapshotReason, RuntimeSnapshotRequest, build_runtime_snapshot};
use ah::runtime_observation::{
    EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderTurnState,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, compute_socket_name};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod common;

struct RealBashHarness {
    ctx: Ctx,
    sessions: Arc<Mutex<Vec<String>>>,
    workspace: tempfile::TempDir,
    systemd_available: bool,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl RealBashHarness {
    fn new() -> Self {
        assert!(
            which::which("bash").is_ok() && which::which("tmux").is_ok(),
            "real Bash test requires bash and tmux; set CCB_TEST_SKIP_REAL_PROVIDER=1 to opt out"
        );
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let workspace = tempfile::TempDir::new().unwrap();
        let systemd_available = common::can_use_systemd_run();
        let socket_name = compute_socket_name(state_dir.path());
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: systemd_available,
                unsafe_no_sandbox: !systemd_available,
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
            workspace,
            systemd_available,
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }

    async fn create_session(&self) -> String {
        let result = handle_session_create(
            json!({
                "project_id": "work-execution-real-bash",
                "absolute_path": self.workspace.path().display().to_string(),
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
        if self.systemd_available {
            for session_id in self.sessions.lock().unwrap().iter() {
                let _ = Command::new("systemctl")
                    .args([
                        "--user",
                        "stop",
                        &format!("ahd-session-{session_id}.service"),
                    ])
                    .output();
            }
        }
        let _ = Command::new("tmux")
            .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
            .output();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_package_handoff_keeps_agent_state_and_bindings_isolated() {
    if std::env::var("CCB_TEST_SKIP_REAL_PROVIDER").as_deref() == Ok("1") {
        return;
    }

    let harness = RealBashHarness::new();
    copy_dir_all(&scenario_root(), harness.workspace.path());
    assert_scenario_package_loaded(harness.workspace.path());
    let session_id = harness.create_session().await;
    for agent_id in ["a1", "a4"] {
        handle_agent_spawn(
            json!({
                "session_id": session_id,
                "agent_id": agent_id,
                // The package's real providers are covered by materialization tests. Bash
                // makes this execution/handoff scenario deterministic and credential-free.
                "provider": "bash",
            }),
            &harness.ctx,
        )
        .await
        .unwrap();
        wait_for_agent_state(&harness.ctx, agent_id, "IDLE", Duration::from_secs(20)).await;
    }

    let implementation_binding = execution_binding(
        harness.workspace.path(),
        "STEP-IMPLEMENT",
        "TASK-OPTIMIZE-SCENARIO-CONFIG",
        "ATTEMPT-IMPLEMENT",
        "RUN-IMPLEMENT",
        "CONTEXT-IMPLEMENT",
        "EPISODE-IMPLEMENT",
        "implementation",
    );
    let (implementation_job, implementation_result) = run_bound_job(
        &harness.ctx,
        "a1",
        "grep -q '^hook_push_enabled = true$' ah.toml && ! grep -q '^hook_push_events' ah.toml && mkdir -p handoff && printf 'scenario-config-contract=optimized\\n' > handoff/implementation.receipt && printf 'impl-handoff-ready\\n'\n",
        implementation_binding.clone(),
    )
    .await;
    assert_eq!(implementation_result["status"], "COMPLETED");
    assert_eq!(
        implementation_result["governance_binding"],
        implementation_binding
    );
    assert!(
        implementation_result["reply_text"]
            .as_str()
            .is_some_and(|reply| reply.contains("impl-handoff-ready"))
    );
    assert_terminal_turn_observations(&harness.ctx, "a1", &implementation_job);
    wait_for_agent_state(&harness.ctx, "a1", "IDLE", Duration::from_secs(5)).await;

    let audit_binding = execution_binding(
        harness.workspace.path(),
        "STEP-AUDIT",
        "TASK-AUDIT-SCENARIO-CONFIG",
        "ATTEMPT-AUDIT",
        "RUN-AUDIT",
        "CONTEXT-AUDIT",
        "EPISODE-AUDIT",
        "audit",
    );
    let (audit_job, audit_result) = run_bound_job(
        &harness.ctx,
        "a4",
        "test -f .ah/rules/a4.md && grep -qx 'scenario-config-contract=optimized' handoff/implementation.receipt && printf 'implementation_receipt=accepted\\n' > handoff/audit.receipt && printf 'audit-handoff-accepted\\n'\n",
        audit_binding.clone(),
    )
    .await;
    assert_eq!(audit_result["status"], "COMPLETED");
    assert_eq!(audit_result["governance_binding"], audit_binding);
    assert!(
        audit_result["reply_text"]
            .as_str()
            .is_some_and(|reply| reply.contains("audit-handoff-accepted"))
    );
    assert_terminal_turn_observations(&harness.ctx, "a4", &audit_job);
    wait_for_agent_state(&harness.ctx, "a4", "IDLE", Duration::from_secs(5)).await;

    assert_ne!(implementation_job, audit_job);
    assert_turn_isolation(&harness.ctx, "a1", &audit_job);
    assert_turn_isolation(&harness.ctx, "a4", &implementation_job);
    let (implementation_lifecycle, audit_lifecycle) = {
        let conn = harness.ctx.db.conn();
        let lifecycle = |agent_id: &str| {
            conn.query_row(
                "SELECT lifecycle_id FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        };
        (lifecycle("a1"), lifecycle("a4"))
    };
    assert!(!implementation_lifecycle.is_empty());
    assert!(!audit_lifecycle.is_empty());
    assert_ne!(implementation_lifecycle, audit_lifecycle);
    assert_eq!(
        std::fs::read_to_string(harness.workspace.path().join("handoff/audit.receipt")).unwrap(),
        "implementation_receipt=accepted\n"
    );

    let snapshot = build_runtime_snapshot(
        &harness.ctx,
        RuntimeSnapshotRequest {
            reason: RuntimeSnapshotReason::Initial,
            config_path: Some(
                harness
                    .workspace
                    .path()
                    .join("ah.toml")
                    .display()
                    .to_string(),
            ),
            workspace_path: Some(harness.workspace.path().display().to_string()),
            sequence: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(snapshot.schema_version, 3);
    assert!(snapshot.agents.iter().any(|agent| agent.agent_id == "a1"));
    assert!(snapshot.agents.iter().any(|agent| agent.agent_id == "a4"));
    for (job_id, expected_binding) in [
        (&implementation_job, &implementation_binding),
        (&audit_job, &audit_binding),
    ] {
        let job = snapshot
            .jobs
            .iter()
            .find(|job| &job.job_id == job_id)
            .unwrap();
        assert_eq!(job.status, "COMPLETED");
        let stored: Value =
            serde_json::from_str(job.governance_binding_json.as_ref().unwrap()).unwrap();
        assert_eq!(&stored, expected_binding);
    }

    let _ = handle_session_kill(
        json!({ "session_id": session_id, "force": true }),
        &harness.ctx,
    )
    .await;
}

fn scenario_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/work_execution/scenario_packages/dev-programming")
}

fn copy_dir_all(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let source = entry.path();
        let target = to.join(entry.file_name());
        if source.is_dir() {
            copy_dir_all(&source, &target);
        } else {
            std::fs::copy(source, target).unwrap();
        }
    }
}

fn assert_scenario_package_loaded(workspace: &Path) {
    let config: Value = std::fs::read_to_string(workspace.join("ah.toml"))
        .unwrap()
        .parse::<toml::Value>()
        .map(|value| serde_json::to_value(value).unwrap())
        .unwrap();
    assert_eq!(config["version"], "1");
    assert_eq!(config["agents"]["a1"]["provider"], "codex");
    assert_eq!(config["agents"]["a4"]["provider"], "claude");
    assert!(workspace.join(".ah/rules/a1.md").is_file());
    assert!(workspace.join(".ah/rules/a4.md").is_file());
}

#[allow(clippy::too_many_arguments)]
fn execution_binding(
    workspace: &Path,
    plan_step_id: &str,
    task_id: &str,
    attempt_id: &str,
    run_id: &str,
    context_id: &str,
    episode_id: &str,
    work_phase: &str,
) -> Value {
    json!({
        "schema_version": 1,
        "roadmap_stream": "ah-release",
        "roadmap_node_id": "SCENARIO-PACK-E2E",
        "plan_id": "PLAN-AH-OPTIMIZATION",
        "plan_revision": "sha256:scenario-plan-v1",
        "plan_step_id": plan_step_id,
        "task_id": task_id,
        "attempt_id": attempt_id,
        "run_id": run_id,
        "context_id": context_id,
        "episode_id": episode_id,
        "module_ref": "work_execution",
        "capability_refs": ["provider_dispatch", "durable_handoff"],
        "target_spec_locator": "examples/scenarios/dev-programming/ah.toml",
        "target_spec_revision": "sha256:scenario-config-v1",
        "work_phase": work_phase,
        "physical_scope": ["ah.toml", ".ah/rules", "handoff"],
        "semantic_scope": ["scenario.config_compatibility", "work_execution.handoff"],
        "worktree_path": workspace.display().to_string(),
        "program_revision": "sha256:ah-program-v1",
        "topology_revision": "sha256:dev-programming-v1"
    })
}

async fn run_bound_job(
    ctx: &Ctx,
    agent_id: &str,
    command: &str,
    binding: Value,
) -> (String, Value) {
    let submitted = handle_job_submit(
        json!({
            "agent_id": agent_id,
            "text": command,
            "request_id": format!("scenario-{}-{}", agent_id, uuid::Uuid::new_v4()),
            "governance_binding": binding,
        }),
        ctx,
    )
    .await
    .unwrap();
    let job_id = submitted["job_id"].as_str().unwrap().to_string();
    let result = handle_job_wait(json!({ "job_id": job_id, "timeout": 30 }), ctx)
        .await
        .unwrap();
    (job_id, result)
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

fn assert_turn_isolation(ctx: &Ctx, agent_id: &str, foreign_job_id: &str) {
    let count: i64 = ctx
        .db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM provider_status_observations WHERE agent_id = ?1 AND turn_id = ?2",
            [agent_id, foreign_job_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 0,
        "{agent_id} consumed observations for {foreign_job_id}"
    );
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
