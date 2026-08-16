#![cfg(target_os = "linux")]
mod common;

use ah::db;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_spawn, handle_session_create, handle_session_kill, handle_system_dump,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, compute_socket_name};
use common::{DaemonIdentityEnvGuard, can_use_systemd_run, scope_policy_for_test};
use serde_json::{Value, json};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Harness {
    ctx: Ctx,
    anchors: Arc<Mutex<Vec<String>>>,
    _env_guard: DaemonIdentityEnvGuard,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl Harness {
    fn new() -> Option<Self> {
        which::which("tmux").expect("tmux binary required for mvp11 acceptance tests");
        if !can_use_systemd_run() {
            if std::env::var("CI").is_ok() {
                panic!(
                    "CI environment lacks user-systemd; mvp11 lifecycle acceptance cannot run; refusing silent skip false green"
                );
            }
            eprintln!("skipping mvp11 systemd anchor acceptance: user systemd unavailable");
            return None;
        }

        let env_guard = DaemonIdentityEnvGuard::scrub();
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let state_dir_path = state_dir.path().to_path_buf();
        let socket_name = compute_socket_name(state_dir.path());
        let policy = scope_policy_for_test(&socket_name);
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir_path,
            env_state: EnvState {
                systemd_run_available: true,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new_with_policy(state_dir.path(), policy)),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        Some(Self {
            ctx,
            anchors: Arc::new(Mutex::new(Vec::new())),
            _env_guard: env_guard,
            _state_dir: state_dir,
            _db_file: db_file,
        })
    }

    async fn create_session(&self, project_id: &str) -> String {
        let result = handle_session_create(
            json!({
                "project_id": project_id,
                "absolute_path": self.ctx.state_dir.join(project_id).display().to_string(),
            }),
            &self.ctx,
        )
        .await
        .unwrap();
        let session_id = result["session_id"].as_str().unwrap().to_string();
        self.anchors
            .lock()
            .unwrap()
            .push(unit_name_for_session(&session_id));
        session_id
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        for unit in self.anchors.lock().unwrap().iter() {
            stop_unit(unit);
        }
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

#[tokio::test(flavor = "multi_thread")]
async fn test_no_master_pidfd_watch_after_session_create() {
    let Some(h) = Harness::new() else {
        return;
    };
    let session_id = h.create_session("p_mvp11_no_master").await;
    let unit_name = unit_name_for_session(&session_id);
    wait_for(
        || unit_is_active(&unit_name),
        Duration::from_secs(5),
        "anchor active after session.create",
    )
    .await;

    let dump = handle_system_dump(json!({}), &h.ctx).await.unwrap();
    let monitors = string_array(&dump["monitors"]);
    assert!(
        !monitors.iter().any(|key| key.starts_with("master:")),
        "unexpected master monitor keys: {monitors:?}"
    );
    assert!(
        monitors
            .iter()
            .any(|key| key == &format!("anchor:{session_id}")),
        "missing anchor monitor key in {monitors:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_ah_kill_triggers_cascade_only() {
    let Some(h) = Harness::new() else {
        return;
    };
    let session_id = h.create_session("p_mvp11_explicit_kill").await;
    let unit_name = unit_name_for_session(&session_id);
    spawn_bash(&h, &session_id, "ag_mvp11_explicit_kill").await;

    handle_session_kill(json!({ "session_id": session_id }), &h.ctx)
        .await
        .unwrap();

    assert_killed(&h, &session_id, "ag_mvp11_explicit_kill", &unit_name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_systemctl_stop_anchor_triggers_cascade() {
    let Some(h) = Harness::new() else {
        return;
    };
    let session_id = h.create_session("p_mvp11_anchor_stop").await;
    let unit_name = unit_name_for_session(&session_id);
    spawn_bash(&h, &session_id, "ag_mvp11_anchor_stop").await;

    stop_unit(&unit_name);

    assert_killed(&h, &session_id, "ag_mvp11_anchor_stop", &unit_name).await;
}

async fn spawn_bash(h: &Harness, session_id: &str, agent_id: &str) {
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
    wait_for_agent_state(h, agent_id, "IDLE", Duration::from_secs(10)).await;
}

async fn assert_killed(h: &Harness, session_id: &str, agent_id: &str, unit_name: &str) {
    wait_for(
        || !unit_is_active(unit_name),
        Duration::from_secs(5),
        "anchor inactive",
    )
    .await;
    wait_for_anchor_monitor_absent(session_id, Duration::from_secs(10)).await;
    wait_for_agent_state(h, agent_id, "KILLED", Duration::from_secs(5)).await;
    wait_for(
        || session_status(&h.ctx, session_id).as_deref() == Some("KILLED"),
        Duration::from_secs(5),
        "session status killed",
    )
    .await;
}

async fn wait_for_agent_state(h: &Harness, agent_id: &str, expected: &str, timeout: Duration) {
    wait_for(
        || {
            query_agent_blocking(&h.ctx, agent_id)
                .as_ref()
                .is_some_and(|agent| agent["state"] == expected)
        },
        timeout,
        &format!("agent {agent_id} state {expected}"),
    )
    .await;
}

async fn wait_for_anchor_monitor_absent(session_id: &str, timeout: Duration) {
    let monitor_key = format!("anchor:{session_id}");
    wait_for(
        || !ah::monitor::contains(&monitor_key),
        timeout,
        &format!("anchor monitor {monitor_key} removed"),
    )
    .await;
}

fn query_agent_blocking(ctx: &Ctx, agent_id: &str) -> Option<Value> {
    ctx.db
        .conn()
        .query_row(
            "SELECT state FROM agents WHERE id = ?1",
            [agent_id],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .map(|state| json!({ "state": state }))
}

fn session_status(ctx: &Ctx, session_id: &str) -> Option<String> {
    ctx.db
        .conn()
        .query_row(
            "SELECT status FROM sessions WHERE id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .ok()
}

async fn wait_for(mut condition: impl FnMut() -> bool, timeout: Duration, label: &str) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {label}");
}

fn unit_name_for_session(session_id: &str) -> String {
    format!("ahd-session-{session_id}.service")
}

fn unit_is_active(unit_name: &str) -> bool {
    Command::new("systemctl")
        .args(["--user", "is-active", unit_name])
        .output()
        .map(|output| {
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "active"
        })
        .unwrap_or(false)
}

fn stop_unit(unit_name: &str) {
    let _ = Command::new("systemctl")
        .args(["--user", "stop", unit_name])
        .output();
}

fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}
