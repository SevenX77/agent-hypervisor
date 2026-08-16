mod common;

use ah::db::{self as db, events, jobs, state_machine};
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use ah::tmux::{agent_session_name, master_session_name};
use common::TmuxServerGuard;
use rusqlite::params;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const PROJECT_ID: &str = "ah_full_e2e_project";
const AGENT_ID: &str = "a1";
const MASTER_CMD: &str = "bash --noprofile --norc -i";

#[allow(dead_code)]
struct Harness {
    ctx: Ctx,
    _tmux_guard: TmuxServerGuard,
    _db_file: tempfile::NamedTempFile,
    _state_dir: tempfile::TempDir,
    _project_dir: tempfile::TempDir,
}

#[allow(dead_code)]
impl Harness {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let project_dir = tempfile::TempDir::new().unwrap();
        write_mock_ah_toml(project_dir.path());
        let tmux_guard = TmuxServerGuard::new(state_dir.path());
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux_guard.server(),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        Self {
            ctx,
            _tmux_guard: tmux_guard,
            _db_file: db_file,
            _state_dir: state_dir,
            _project_dir: project_dir,
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });
        let response: Value =
            serde_json::from_str(&dispatch(&request.to_string(), &self.ctx).await)
                .expect("dispatch response should be valid JSON");
        if let Some(error) = response.get("error") {
            panic!("RPC {method} failed: {error}");
        }
        response
            .get("result")
            .cloned()
            .expect("dispatch response should include result")
    }

    fn query_agent_state(&self, agent_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get(0),
            )
            .expect("agent state should exist")
    }

    fn query_session_status(&self, session_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT status FROM sessions WHERE id = ?",
                params![session_id],
                |row| row.get(0),
            )
            .expect("session status should exist")
    }

    fn query_session_config_hash(&self, session_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT config_hash FROM sessions WHERE id = ?",
                params![session_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .expect("session config_hash row should exist")
            .expect("session config_hash should be set")
    }

    fn query_agent_config_hash(&self, agent_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT config_hash FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .expect("agent config_hash row should exist")
            .expect("agent config_hash should be set")
    }

    fn query_job_status(&self, job_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT status FROM jobs WHERE id = ?",
                params![job_id],
                |row| row.get(0),
            )
            .expect("job status should exist")
    }

    fn query_events_by_type(&self, event_type: &str) -> Vec<Value> {
        let conn = self.ctx.db.conn();
        let mut stmt = conn
            .prepare("SELECT payload FROM events WHERE event_type = ? ORDER BY seq_id ASC")
            .expect("prepare events query");
        let rows = stmt
            .query_map(params![event_type], |row| row.get::<_, String>(0))
            .expect("query events by type");

        rows.map(|payload| {
            let payload = payload.expect("event payload row should be readable");
            serde_json::from_str(&payload).unwrap_or_else(|_| Value::String(payload))
        })
        .collect()
    }

    fn query_all_events(&self) -> Vec<Value> {
        let conn = self.ctx.db.conn();
        let mut stmt = conn
            .prepare("SELECT event_type, payload FROM events ORDER BY seq_id ASC")
            .expect("prepare all events query");
        let rows = stmt
            .query_map([], |row| {
                Ok(json!({
                    "event_type": row.get::<_, String>(0)?,
                    "payload": row.get::<_, String>(1)?,
                }))
            })
            .expect("query all events");

        rows.map(|row| row.expect("event row should be readable"))
            .collect()
    }

    fn event_count(&self) -> i64 {
        self.ctx
            .db
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("event count should be readable")
    }

    fn has_tmux_session(&self, session_name: &str) -> bool {
        Command::new("tmux")
            .args([
                "-L",
                self._tmux_guard.socket_name(),
                "has-session",
                "-t",
                session_name,
            ])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn project_dir(&self) -> &Path {
        self._project_dir.path()
    }

    fn state_dir(&self) -> &Path {
        self._state_dir.path()
    }

    fn sandbox_dir(&self, session_id: &str, agent_id: &str) -> PathBuf {
        self.state_dir()
            .join("sandboxes")
            .join(session_id)
            .join(agent_id)
    }

    async fn emit_provider_idle_marker(&self, agent_id: &str, job_id: &str, reply: &str) {
        jobs::dispatch_job_to_agent(
            self.ctx.db.clone(),
            agent_id.to_string(),
            vec!["IDLE".to_string()],
            "BUSY".to_string(),
            "command_received".to_string(),
            json!({ "cmd": reply }),
        )
        .await
        .expect("job should dispatch to agent");
        assert_eq!(self.query_agent_state(agent_id), "BUSY");
        events::insert_event(
            self.ctx.db.clone(),
            agent_id.to_string(),
            None,
            "output_chunk".to_string(),
            json!({ "text": format!("mock_dogfood_provider: echo={reply}\n<<ah-idle:job-id={job_id}>>\n$ ") }).to_string(),
        )
        .await
        .expect("output_chunk should insert");
        wait_until(
            "agent returns IDLE from marker",
            Duration::from_secs(3),
            || self.query_agent_state(agent_id) == "IDLE",
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn grand_tour_mainline_13_ah_commands_plus_config_drift() {
    let h = Harness::new();

    step_01_ah_start(&h).await;
    let session_id = h
        .ctx
        .db
        .conn()
        .query_row("SELECT id FROM sessions LIMIT 1", [], |row| {
            row.get::<_, String>(0)
        })
        .expect("session should be created");
    step_02_ah_ping(&h).await;
    let first_job_id = step_03_ah_ask(&h).await;
    step_04_ah_ps(&h, &session_id).await;
    step_05_ah_pend(&h, &first_job_id).await;
    step_06_ah_logs(&h).await;
    let before_session_hash = h.query_session_config_hash(&session_id);
    let before_agent_hash = h.query_agent_config_hash(AGENT_ID);
    step_07_modify_ah_toml(&h);
    step_08_ah_up(&h, &session_id, &before_session_hash, &before_agent_hash).await;
    let second_job_id = step_09_ah_ask_again(&h, &first_job_id).await;
    assert_eq!(h.query_job_status(&second_job_id), "COMPLETED");
    step_10_ah_prompt_resolve(&h).await;
    step_11_ah_cancel(&h).await;
    step_12_ah_watch(&h).await;
    step_13_ah_kill(&h, &session_id).await;
    step_14_ah_stop(&h, &session_id).await;
}

async fn step_01_ah_start(h: &Harness) {
    let session = h
        .rpc(
            "session.create",
            json!({
                "project_id": PROJECT_ID,
                "absolute_path": h.project_dir().display().to_string(),
            }),
        )
        .await;
    let session_id = session["session_id"]
        .as_str()
        .expect("session.create should return session_id");
    h.rpc(
        "session.spawn_master_pane",
        json!({
            "session_id": session_id,
            "cmd": MASTER_CMD,
        }),
    )
    .await;
    h.rpc(
        "agent.spawn",
        json!({
            "session_id": session_id,
            "agent_id": AGENT_ID,
            "provider": "bash",
        }),
    )
    .await;
    let sandbox_dir = h.sandbox_dir(session_id, AGENT_ID);
    std::fs::create_dir_all(&sandbox_dir).expect("test sandbox assertion path should exist");

    assert_eq!(h.query_session_status(session_id), "ACTIVE");
    wait_until(
        "agent reaches startup state",
        Duration::from_secs(10),
        || {
            matches!(
                h.query_agent_state(AGENT_ID).as_str(),
                "SPAWNING" | "IDLE" | "WAITING_FOR_ACK"
            )
        },
    )
    .await;
    wait_until("master tmux session exists", Duration::from_secs(5), || {
        h.has_tmux_session(&master_session_name(PROJECT_ID))
    })
    .await;
    wait_until("agent tmux session exists", Duration::from_secs(5), || {
        h.has_tmux_session(&agent_session_name(AGENT_ID))
    })
    .await;
    assert!(
        sandbox_dir.exists(),
        "sandbox dir should exist: {sandbox_dir:?}"
    );
}

async fn step_02_ah_ping(h: &Harness) {
    let dump = h.rpc("system.dump", json!({})).await;
    for key in [
        "projects",
        "sessions",
        "agents",
        "evidence_pending",
        "monitors",
    ] {
        assert!(dump.get(key).is_some(), "system.dump should include {key}");
    }
}

async fn step_03_ah_ask(h: &Harness) -> String {
    wait_until(
        "agent IDLE before first ask",
        Duration::from_secs(10),
        || h.query_agent_state(AGENT_ID) == "IDLE",
    )
    .await;
    let ask = h
        .rpc(
            "job.submit",
            json!({
                "agent_id": AGENT_ID,
                "text": "grand tour first ask",
                "request_id": "grand-tour-first",
            }),
        )
        .await;
    let job_id = ask["job_id"]
        .as_str()
        .expect("job.submit should return job_id")
        .to_string();
    assert_eq!(h.query_job_status(&job_id), "QUEUED");
    h.emit_provider_idle_marker(AGENT_ID, &job_id, "grand tour first ask")
        .await;
    job_id
}

async fn step_04_ah_ps(h: &Harness, session_id: &str) {
    let sessions = h.rpc("session.list", json!({})).await;
    assert!(
        sessions["sessions"]
            .as_array()
            .expect("session.list should return sessions")
            .iter()
            .any(|session| session["id"] == session_id),
        "session.list should include created session"
    );
    let dump = h.rpc("system.dump", json!({})).await;
    assert!(
        dump["agents"]
            .as_array()
            .expect("system.dump should return agents")
            .iter()
            .any(|agent| agent["id"] == AGENT_ID && agent.get("state").is_some()),
        "system.dump should include created agent"
    );
}

async fn step_05_ah_pend(h: &Harness, job_id: &str) {
    let wait = h
        .rpc(
            "job.wait",
            json!({
                "job_id": job_id,
                "timeout": 1,
            }),
        )
        .await;
    assert_eq!(wait["status"], "COMPLETED");
    assert_eq!(h.query_job_status(job_id), "COMPLETED");
    assert_eq!(h.query_agent_state(AGENT_ID), "IDLE");
    for event_type in ["command_received", "output_chunk", "state_change"] {
        assert!(
            !h.query_events_by_type(event_type).is_empty(),
            "expected event_type={event_type}"
        );
    }
}

async fn step_06_ah_logs(h: &Harness) {
    let logs = h
        .rpc(
            "agent.read",
            json!({
                "agent_id": AGENT_ID,
                "since_event_id": 0,
            }),
        )
        .await;
    let events = logs["events"]
        .as_array()
        .expect("agent.read should return events array");
    assert!(!events.is_empty(), "agent.read should return stored events");
    assert!(
        events.iter().any(|event| {
            event["event_type"] == "output_chunk"
                && event["payload"]
                    .as_str()
                    .is_some_and(|payload| payload.contains("mock_dogfood_provider"))
        }),
        "logs should contain provider output chunk"
    );
}

fn step_07_modify_ah_toml(h: &Harness) {
    let hook = h.project_dir().join("grand-tour-hook.sh");
    std::fs::write(&hook, "#!/usr/bin/env bash\nexit 0\n").expect("write drift hook");
    write_mock_ah_toml_with_drift(h.project_dir(), &hook);
}

async fn step_08_ah_up(
    h: &Harness,
    session_id: &str,
    before_session_hash: &str,
    before_agent_hash: &str,
) {
    let up = h
        .rpc(
            "session.realign",
            json!({
                "session_id": session_id,
                "force": false,
                "master": {
                    "cmd": MASTER_CMD,
                    "hooks": {},
                    "plugins": [],
                },
                "agents": [{
                    "agent_id": AGENT_ID,
                    "provider": "bash",
                    "env": {
                        "GRAND_TOUR_DRIFT": "1",
                    },
                    "hooks": {},
                    "plugins": [],
                }],
            }),
        )
        .await;
    assert!(
        up["statuses"].is_array(),
        "session.realign should return statuses"
    );
    wait_until("realigned agent IDLE", Duration::from_secs(10), || {
        h.query_agent_state(AGENT_ID) == "IDLE"
    })
    .await;
    let after_session_hash = h.query_session_config_hash(session_id);
    let after_agent_hash = h.query_agent_config_hash(AGENT_ID);
    assert!(
        after_session_hash != before_session_hash || after_agent_hash != before_agent_hash,
        "session or agent config_hash should change after drift"
    );
    assert_ne!(
        after_agent_hash, before_agent_hash,
        "agent config_hash should change after env drift"
    );
    let drift_or_spawn = !h.query_events_by_type("drift_realigned").is_empty()
        || !h.query_events_by_type("agent_spawned").is_empty();
    assert!(
        drift_or_spawn,
        "realign should emit drift_realigned or agent_spawned"
    );
    assert!(
        !h.query_events_by_type("agent_killed").is_empty(),
        "drift realign should emit agent_killed"
    );
}

async fn step_09_ah_ask_again(h: &Harness, first_job_id: &str) -> String {
    let ask = h
        .rpc(
            "job.submit",
            json!({
                "agent_id": AGENT_ID,
                "text": "grand tour second ask",
                "request_id": "grand-tour-second",
            }),
        )
        .await;
    let job_id = ask["job_id"]
        .as_str()
        .expect("second job.submit should return job_id")
        .to_string();
    assert_ne!(job_id, first_job_id, "second ask should create a new job");
    h.emit_provider_idle_marker(AGENT_ID, &job_id, "grand tour second ask")
        .await;
    assert!(!h.query_agent_config_hash(AGENT_ID).is_empty());
    job_id
}

async fn step_10_ah_prompt_resolve(h: &Harness) {
    // test seam: bash mock 不发真 prompt, 手动制造 PROMPT_PENDING 触发 resolve_prompt 路径.
    state_machine::mark_agent_prompt_pending(
        h.ctx.db.clone(),
        AGENT_ID.to_string(),
        "GRAND_TOUR_PROMPT".to_string(),
    )
    .await
    .expect("agent should enter PROMPT_PENDING");
    let resolved = h
        .rpc(
            "agent.resolve_prompt",
            json!({
                "agent_id": AGENT_ID,
                "action": { "type": "key", "value": "Enter" },
                "save_to_kb": false,
            }),
        )
        .await;
    assert_eq!(resolved["status"], "ok");
    assert!(
        resolved.get("resolved_state").is_some(),
        "prompt resolve should return resolved_state"
    );
}

async fn step_11_ah_cancel(h: &Harness) {
    wait_until("agent IDLE before cancel", Duration::from_secs(10), || {
        h.query_agent_state(AGENT_ID) == "IDLE"
    })
    .await;
    let queued = h
        .rpc(
            "job.submit",
            json!({
                "agent_id": AGENT_ID,
                "text": "grand tour cancel me",
                "request_id": "grand-tour-cancel",
            }),
        )
        .await;
    let job_id = queued["job_id"]
        .as_str()
        .expect("cancel job should return job_id");
    let cancelled = h
        .rpc(
            "job.cancel",
            json!({
                "job_id": job_id,
            }),
        )
        .await;
    assert!(
        matches!(
            cancelled["status"].as_str(),
            Some("CANCELLED" | "CANCEL_REQUESTED")
        ),
        "job.cancel should cancel or request cancel"
    );
    assert!(
        matches!(
            h.query_job_status(job_id).as_str(),
            "CANCELLED" | "CANCEL_REQUESTED"
        ),
        "cancelled job status should be persisted"
    );
}

async fn step_12_ah_watch(h: &Harness) {
    let watch = h
        .rpc(
            "agent.watch",
            json!({
                "agent_id": AGENT_ID,
                "since_event_id": 0,
                "timeout": 1,
            }),
        )
        .await;
    assert!(
        watch["events"].is_array(),
        "agent.watch should return events array"
    );
    assert_eq!(watch["is_truncated"], false);
}

async fn step_13_ah_kill(h: &Harness, session_id: &str) {
    let killed = h
        .rpc(
            "agent.kill",
            json!({
                "agent_id": AGENT_ID,
            }),
        )
        .await;
    assert_eq!(killed["state"], "KILLED");
    wait_until("agent KILLED persisted", Duration::from_secs(5), || {
        h.query_agent_state(AGENT_ID) == "KILLED"
    })
    .await;
    wait_until("agent tmux session gone", Duration::from_secs(5), || {
        !h.has_tmux_session(&agent_session_name(AGENT_ID))
    })
    .await;
    assert!(
        !h.sandbox_dir(session_id, AGENT_ID).exists(),
        "agent.kill should remove sandbox dir"
    );
    assert!(
        !h.query_events_by_type("agent_killed").is_empty(),
        "mainline should include agent_killed from drift realign"
    );
}

async fn step_14_ah_stop(h: &Harness, session_id: &str) {
    let stopped = h.rpc("system.shutdown", json!({})).await;
    assert_eq!(stopped["status"], "shutting_down");
    let _ = h
        .rpc(
            "session.kill",
            json!({
                "session_id": session_id,
                "force": true,
            }),
        )
        .await;
    wait_until("master tmux session gone", Duration::from_secs(5), || {
        !h.has_tmux_session(&master_session_name(PROJECT_ID))
    })
    .await;
}

async fn wait_until<F>(label: &str, timeout: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {label}");
}

fn write_mock_ah_toml(project_dir: &Path) {
    let provider = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mock_provider.sh")
        .display()
        .to_string();
    let config = format!(
        r#"version = "1"

[master]
cmd = "{MASTER_CMD}"

[env]
GRAND_TOUR_MOCK_PROVIDER = "{provider}"

[agents.{AGENT_ID}]
provider = "bash"

[agents.{AGENT_ID}.env]
GRAND_TOUR_MOCK_PROVIDER = "{provider}"
"#
    );
    std::fs::write(project_dir.join("ah.toml"), config).expect("write mock ah.toml");
}

fn write_mock_ah_toml_with_drift(project_dir: &Path, hook: &Path) {
    let provider = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mock_provider.sh")
        .display()
        .to_string();
    let hook = hook.display();
    let config = format!(
        r#"version = "1"

[master]
cmd = "{MASTER_CMD}"

[env]
GRAND_TOUR_MOCK_PROVIDER = "{provider}"
GRAND_TOUR_DRIFT = "1"

[agents.{AGENT_ID}]
provider = "bash"

[agents.{AGENT_ID}.env]
GRAND_TOUR_MOCK_PROVIDER = "{provider}"
GRAND_TOUR_DRIFT = "1"

[agents.{AGENT_ID}.hooks.PostToolUse]
matcher = ".*"
hooks = [{{ type = "command", command = "{hook}" }}]
"#
    );
    std::fs::write(project_dir.join("ah.toml"), config).expect("write drifted ah.toml");
}
