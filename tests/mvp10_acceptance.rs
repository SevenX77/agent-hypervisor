#![cfg(target_os = "linux")]
mod common;

use ah::cli::rpc_client::{RpcClient, UnixRpcClient};
use ah::db;
use ah::tmux::scope::unit_name_for_socket;
use ah::tmux::{TmuxServer, compute_socket_name};
use common::{can_use_systemd_run, scope_policy_for_test, scrub_daemon_identity_env};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn wait_timeout(&mut self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.child.try_wait().unwrap().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tmux_server_in_scope() {
    which::which("tmux").expect("tmux binary required for mvp10 acceptance tests");
    if !can_use_systemd_run() {
        panic!("AC1 requires user systemd; running without it");
    }
    let state_dir = tempfile::tempdir().unwrap();
    let socket_name = compute_socket_name(state_dir.path());
    let unit_name = unit_name_for_socket(&socket_name);
    let server = TmuxServer::new_with_policy(state_dir.path(), scope_policy_for_test(&socket_name));

    server
        .ensure_session(
            "mvp10_scope_probe".to_string(),
            state_dir.path().to_path_buf(),
        )
        .await
        .expect("ensure tmux session in scope");
    wait_for(
        || {
            scope_unit_exists(&format!("{unit_name}.scope"))
                && !tmux_pids_for_socket(&socket_name).is_empty()
        },
        Duration::from_secs(5),
        "tmux server in scope",
    );
    let pid = tmux_pids_for_socket(&socket_name)
        .into_iter()
        .next()
        .expect("tmux pid for socket");
    let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
    assert!(
        cgroup.contains("ahd-agents.slice") && cgroup.contains("ahd-tmux-"),
        "unexpected tmux cgroup for pid {pid}: {cgroup}"
    );

    let _ = Command::new("tmux")
        .args(["-L", &socket_name, "kill-server"])
        .output();
    let _ = std::fs::remove_file(tmux_socket_path(&socket_name));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_main_sigterm_cleans_resources() {
    which::which("tmux").expect("tmux binary required for mvp10 acceptance tests");
    let xdg_state = tempfile::tempdir().unwrap();
    let state_dir = xdg_state.path().join("ah");
    let socket_name = compute_socket_name(&state_dir);
    let daemon_socket = state_dir.join("ahd.sock");
    let tmux_socket = tmux_socket_path(&socket_name);

    let mut command = Command::new(env!("CARGO_BIN_EXE_ahd"));
    command
        .env("XDG_STATE_HOME", xdg_state.path())
        .env("AH_STATE_DIR", &state_dir)
        .env("AH_UNSAFE_NO_SANDBOX", "1")
        .env_remove("AH_ENV");
    let child = scrub_daemon_identity_env(&mut command)
        .spawn()
        .expect("spawn ah daemon");
    let mut child = ChildGuard::new(child);
    wait_for(
        || daemon_socket.exists(),
        Duration::from_secs(5),
        "daemon socket",
    );

    let client = UnixRpcClient::new(daemon_socket);
    let session = client
        .call(
            "session.create",
            json!({
                "project_id": "mvp10_sigterm",
                "absolute_path": state_dir.display().to_string(),
            }),
        )
        .await
        .expect("create session");
    let session_id = session["session_id"].as_str().unwrap().to_string();
    client
        .call(
            "agent.spawn",
            json!({
                "session_id": session_id,
                "agent_id": "ag_mvp10_sigterm",
                "provider": "bash",
            }),
        )
        .await
        .expect("spawn agent");
    wait_for(
        || tmux_socket_alive(&socket_name),
        Duration::from_secs(5),
        "tmux socket alive",
    );

    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    assert!(
        child.wait_timeout(Duration::from_secs(5)),
        "ah daemon did not exit after SIGTERM"
    );
    wait_for(
        || !tmux_socket_alive(&socket_name) && !tmux_socket.exists(),
        Duration::from_secs(5),
        "tmux cleanup after SIGTERM",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_startup_reconcile_cleans_stale_sockets() {
    which::which("tmux").expect("tmux binary required for mvp10 acceptance tests");
    let socket_dir = PathBuf::from(format!("/tmp/tmux-{}", unsafe { libc::geteuid() }));
    std::fs::create_dir_all(&socket_dir).unwrap();
    let fake_socket = socket_dir.join("ahd-fakedeadbeef0000");
    std::fs::write(&fake_socket, b"stale").unwrap();

    let db_file = tempfile::NamedTempFile::new().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let db = db::init(db_file.path()).unwrap();
    db::system::reconcile_startup_with_tmux_socket(
        db,
        state_dir.path().to_path_buf(),
        Some("ahd-live-current".to_string()),
    )
    .await
    .unwrap();

    assert!(
        !fake_socket.exists(),
        "startup reconcile did not remove stale socket {}",
        fake_socket.display()
    );
}

#[test]
fn test_main_sigkill_systemd_cleans() {
    which::which("tmux").expect("tmux binary required for mvp10 acceptance tests");
    if !can_use_systemd_run() {
        panic!("AC3 requires user systemd; running on CI without it");
    }

    let state_dir = tempfile::tempdir().unwrap();
    let socket_name = compute_socket_name(state_dir.path());
    let tmux_scope = format!("{}.scope", unit_name_for_socket(&socket_name));
    let unit = format!("ah-test-victim-{}", std::process::id());
    let scope_unit = format!("{unit}.scope");
    let setenv = format!("--setenv=AH_TEST_WRAPPER_SCOPE={scope_unit}");
    let unit_arg = format!("--unit={unit}");
    let state_dir_arg = state_dir.path().display().to_string();

    let mut command = Command::new("systemd-run");
    let child = scrub_daemon_identity_env(&mut command)
        .args([
            "--user",
            "--scope",
            "--collect",
            &unit_arg,
            &setenv,
            "--",
            env!("CARGO_BIN_EXE_ahd_test_helper"),
            "--hold-tmux",
            "--state-dir",
            &state_dir_arg,
        ])
        .spawn()
        .expect("spawn systemd-run wrapper scope");
    let mut child = ChildGuard::new(child);

    wait_for(
        || scope_unit_exists(&tmux_scope) && tmux_socket_alive(&socket_name),
        Duration::from_secs(5),
        "helper tmux scope",
    );

    let status = Command::new("systemctl")
        .args(["--user", "stop", &scope_unit])
        .status()
        .expect("stop wrapper scope");
    assert!(
        status.success(),
        "systemctl stop {scope_unit} failed: {status}"
    );

    wait_for(
        || !scope_unit_exists(&tmux_scope) && !tmux_socket_alive(&socket_name),
        Duration::from_secs(5),
        "tmux scope cleanup after wrapper stop",
    );
    let _ = std::fs::remove_file(tmux_socket_path(&socket_name));
    assert!(
        child.wait_timeout(Duration::from_secs(5)),
        "systemd-run wrapper did not exit after scope stop"
    );
}

#[ignore = "CI runs this after the full acceptance suite"]
#[test]
fn test_no_orphan_tmux_after_test_suite() {
    which::which("tmux").expect("tmux binary required for mvp10 acceptance tests");
    let socket_dir = PathBuf::from(format!("/tmp/tmux-{}", unsafe { libc::geteuid() }));
    let mut stale = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&socket_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("ah-") && !tmux_socket_alive(&name) {
                stale.push(name);
            }
        }
    }
    assert!(stale.is_empty(), "stale ahd tmux sockets: {stale:?}");
}

fn wait_for(mut pred: impl FnMut() -> bool, timeout: Duration, label: &str) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {label}");
}

fn tmux_socket_alive(socket_name: &str) -> bool {
    match Command::new("tmux")
        .args(["-L", socket_name, "ls-sessions"])
        .output()
    {
        Ok(output) if output.status.success() => true,
        Ok(output) if String::from_utf8_lossy(&output.stderr).contains("unknown command") => {
            Command::new("tmux")
                .args(["-L", socket_name, "list-sessions"])
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }
        _ => false,
    }
}

fn scope_unit_exists(unit: &str) -> bool {
    Command::new("systemctl")
        .args([
            "--user",
            "list-units",
            "--type=scope",
            "--all",
            "--no-legend",
        ])
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .any(|line| line.split_whitespace().next() == Some(unit))
        })
        .unwrap_or(false)
}

fn tmux_pids_for_socket(socket_name: &str) -> Vec<i32> {
    let output = Command::new("ps")
        .args(["-eo", "pid,args"])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields
                .windows(2)
                .any(|window| window[0] == "-L" && window[1] == socket_name)
            {
                fields.first().and_then(|pid| pid.parse::<i32>().ok())
            } else {
                None
            }
        })
        .collect()
}

fn tmux_socket_path(socket_name: &str) -> PathBuf {
    Path::new("/tmp")
        .join(format!("tmux-{}", unsafe { libc::geteuid() }))
        .join(socket_name)
}
