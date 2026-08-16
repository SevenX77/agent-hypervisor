#![cfg(target_os = "linux")]
mod common;

use ah::db;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_send, handle_agent_spawn, handle_session_create, handle_session_kill,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, compute_socket_name};
use common::{can_use_systemd_run, scope_policy_for_test};
use serde_json::json;
use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Harness {
    ctx: Ctx,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl Harness {
    fn new() -> Option<Self> {
        which::which("tmux").expect("tmux binary required for orphan reap acceptance test");
        if !can_use_systemd_run() {
            if std::env::var("CI").is_ok() {
                panic!("orphan reap acceptance requires user systemd");
            }
            eprintln!("skipping orphan reap acceptance: user systemd unavailable");
            return None;
        }

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
                scope_policy_for_test(&socket_name),
            )),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        Some(Self {
            ctx,
            _state_dir: state_dir,
            _db_file: db_file,
        })
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = Command::new("tmux")
            .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
            .output();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn session_kill_reaps_disowned_grandchild_in_agent_scope() {
    let Some(h) = Harness::new() else {
        return;
    };
    let session_id = handle_session_create(
        json!({
            "project_id": "orphan_reap",
            "absolute_path": h.ctx.state_dir.display().to_string(),
        }),
        &h.ctx,
    )
    .await
    .unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_string();
    let agent_id = format!("ag_orphan_{}", uuid::Uuid::new_v4().simple());
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
    wait_for_agent_state(&h, &agent_id, "IDLE", Duration::from_secs(10)).await;

    let scope = wait_for_agent_scope(
        h.ctx.tmux_server.socket_name(),
        &agent_id,
        Duration::from_secs(10),
    )
    .await;
    let pid_file = h.ctx.state_dir.join(format!("{agent_id}.sleep.pid"));
    let marker = format!("ah-orphan-reap-{}", uuid::Uuid::new_v4().simple());
    let command = format!(
        "sleep 1000 & echo $! > {}; disown; echo {}",
        shell_quote(&pid_file.display().to_string()),
        marker
    );
    handle_agent_send(
        json!({
            "agent_id": agent_id,
            "text": command,
            "request_id": format!("req_{}", uuid::Uuid::new_v4().simple()),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let sleep_pid = wait_for_pid_file(&pid_file, Duration::from_secs(10)).await;
    wait_for(
        || scope_cgroup_pids(&scope).contains(&sleep_pid),
        Duration::from_secs(10),
        "sleep pid recorded inside agent scope cgroup",
    )
    .await;

    handle_session_kill(json!({ "session_id": session_id }), &h.ctx)
        .await
        .unwrap();

    wait_for(
        || !pid_alive(sleep_pid),
        Duration::from_secs(10),
        "disowned sleep killed by session.kill",
    )
    .await;
    wait_for(
        || !scope_unit_active(&scope),
        Duration::from_secs(10),
        "agent scope inactive after session.kill",
    )
    .await;
}

async fn wait_for_agent_state(h: &Harness, agent_id: &str, expected: &str, timeout: Duration) {
    wait_for(
        || {
            h.ctx
                .db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = ?1",
                    [agent_id],
                    |row| row.get::<_, String>(0),
                )
                .is_ok_and(|state| state == expected)
        },
        timeout,
        &format!("agent {agent_id} state {expected}"),
    )
    .await;
}

async fn wait_for_agent_scope(daemon_marker: &str, agent_id: &str, timeout: Duration) -> String {
    wait_for_value(
        || find_agent_scope(daemon_marker, agent_id),
        timeout,
        &format!("scope for {agent_id}"),
    )
    .await
}

async fn wait_for_pid_file(path: &std::path::Path, timeout: Duration) -> i32 {
    wait_for_value(
        || fs::read_to_string(path).ok()?.trim().parse::<i32>().ok(),
        timeout,
        &format!("pid file {}", path.display()),
    )
    .await
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

async fn wait_for_value<T>(mut f: impl FnMut() -> Option<T>, timeout: Duration, label: &str) -> T {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(value) = f() {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {label}");
}

fn find_agent_scope(daemon_marker: &str, agent_id: &str) -> Option<String> {
    let output = Command::new("systemctl")
        .args([
            "--user",
            "list-units",
            "--all",
            "--type=scope",
            "--no-legend",
            "--plain",
            "--no-pager",
        ])
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let needle = format!("ah-agent-{agent_id}@{daemon_marker}");
    stdout.lines().find_map(|line| {
        if line.contains(&needle) {
            line.split_whitespace().next().map(str::to_string)
        } else {
            None
        }
    })
}

fn scope_cgroup_pids(scope: &str) -> Vec<i32> {
    let output = Command::new("systemctl")
        .args(["--user", "show", scope, "-P", "ControlGroup"])
        .output()
        .ok();
    let Some(output) = output else {
        return Vec::new();
    };
    let control_group = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if control_group.is_empty() {
        return Vec::new();
    }
    let path = std::path::Path::new("/sys/fs/cgroup")
        .join(control_group.trim_start_matches('/'))
        .join("cgroup.procs");
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse::<i32>().ok())
        .collect()
}

fn scope_unit_active(scope: &str) -> bool {
    Command::new("systemctl")
        .args(["--user", "is-active", scope])
        .output()
        .map(|output| {
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "active"
        })
        .unwrap_or(false)
}

fn pid_alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
