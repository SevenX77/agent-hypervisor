#![cfg(target_os = "linux")]
use ah::db;
use ah::tmux::scope::unit_name_for_socket;
use ah::tmux::{TmuxServer, agent_session_name};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

static DEV_STATE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn dev_state_lock() -> MutexGuard<'static, ()> {
    DEV_STATE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn require_tmux() {
    which::which("tmux").expect("tmux binary required for r1 master exit shutdown tests");
}

fn dev_state_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("dev_state")
}

fn cleanup_dev_state(state_dir: &Path) {
    let server = TmuxServer::new(state_dir);
    let _ = Command::new("systemctl")
        .args([
            "--user",
            "stop",
            &format!("{}.scope", unit_name_for_socket(server.socket_name())),
        ])
        .output();
    for suffix in ["", "-shm", "-wal"] {
        let _ = std::fs::remove_file(state_dir.join(format!("ahd.sqlite{suffix}")));
    }
    let _ = std::fs::remove_file(state_dir.join("ahd.sock"));
    let _ = Command::new("tmux")
        .args(["-L", server.socket_name(), "kill-server"])
        .output();
}

struct DevStateCleanupGuard {
    state_dir: PathBuf,
}

impl DevStateCleanupGuard {
    fn new(state_dir: &Path) -> Self {
        Self {
            state_dir: state_dir.to_path_buf(),
        }
    }
}

impl Drop for DevStateCleanupGuard {
    fn drop(&mut self) {
        cleanup_dev_state(&self.state_dir);
    }
}

fn spawn_daemon(state_dir: &Path) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_ahd"))
        .env("AH_ENV", "dev")
        .env_remove("AH_SOCKET")
        .env("AH_STATE_DIR", state_dir)
        .env("AH_UNSAFE_NO_SANDBOX", "1")
        .spawn()
        .expect("spawn ah");
    wait_for_socket(&state_dir.join("ahd.sock"), Duration::from_secs(5));
    child
}

fn wait_for_socket(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("ah socket did not appear at {}", path.display());
}

fn rpc_call(socket_path: &Path, id: u64, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(socket_path).expect("connect ah socket");
    let request = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    writeln!(stream, "{request}").expect("write rpc request");

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).expect("read rpc response");
    let value: Value = serde_json::from_str(response.trim()).expect("parse rpc response");
    assert!(
        value.get("error").is_none(),
        "rpc {method} returned error: {value}"
    );
    value["result"].clone()
}

fn create_session(socket_path: &Path, id: u64, project_id: &str, project_dir: &Path) -> String {
    let result = rpc_call(
        socket_path,
        id,
        "session.create",
        json!({
            "project_id": project_id,
            "absolute_path": project_dir,
        }),
    );
    result["session_id"].as_str().unwrap().to_string()
}

fn spawn_master(socket_path: &Path, id: u64, session_id: &str) -> String {
    let result = rpc_call(
        socket_path,
        id,
        "session.spawn_master_pane",
        json!({
            "session_id": session_id,
            "cmd": "sleep 60",
        }),
    );
    result["pane_id"].as_str().unwrap().to_string()
}

fn spawn_agent(socket_path: &Path, id: u64, session_id: &str, agent_id: &str) {
    let _ = rpc_call(
        socket_path,
        id,
        "agent.spawn",
        json!({
            "session_id": session_id,
            "agent_id": agent_id,
            "provider": "bash",
        }),
    );
}

fn submit_job(socket_path: &Path, id: u64, agent_id: &str, request_id: &str) -> String {
    let result = rpc_call(
        socket_path,
        id,
        "job.submit",
        json!({
            "agent_id": agent_id,
            "text": "sleep 30; echo master death in-flight\n",
            "request_id": request_id,
        }),
    );
    result["job_id"].as_str().unwrap().to_string()
}

fn pane_pid(state_dir: &Path, pane_id: &str) -> i32 {
    let server = TmuxServer::new(state_dir);
    let output = Command::new("tmux")
        .args([
            "-L",
            server.socket_name(),
            "display-message",
            "-p",
            "-t",
            pane_id,
            "#{pane_pid}",
        ])
        .output()
        .expect("tmux display-message");
    assert!(
        output.status.success(),
        "tmux display-message failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap()
}

fn kill_pid(pid: i32) {
    // SAFETY: pid comes from tmux's #{pane_pid}; SIGTERM is a constant signal.
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
}

fn wait_for_daemon_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll ah child") {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

fn wait_for_output(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if child.try_wait().expect("poll ah child").is_some() {
            return child.wait_with_output().expect("collect ah output");
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let _ = child.kill();
    let output = child.wait_with_output().expect("collect killed ah output");
    panic!(
        "ah did not exit within {:?}; stderr={}",
        timeout,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn terminate_daemon(mut child: Child) {
    // SAFETY: child.id is the ah process spawned by this test.
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let _ = wait_for_daemon_exit(&mut child, Duration::from_secs(5));
    let _ = child.kill();
}

fn kill_daemon_without_cleanup(mut child: Child, socket_path: &Path) {
    // SAFETY: child.id is the ahd process spawned by this test; SIGKILL avoids graceful tmux cleanup.
    unsafe {
        libc::kill(child.id() as i32, libc::SIGKILL);
    }
    let _ = wait_for_daemon_exit(&mut child, Duration::from_secs(5));
    let _ = std::fs::remove_file(socket_path);
}

fn active_agent_count(state_dir: &Path) -> i64 {
    let db = db::init(&state_dir.join("ahd.sqlite")).unwrap();
    db.conn()
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE state NOT IN ('CRASHED', 'KILLED')",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn wait_for_active_agent_count(state_dir: &Path, expected: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if active_agent_count(state_dir) == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "active agent count did not become {expected} within {timeout:?}; latest={}",
        active_agent_count(state_dir)
    );
}

fn wait_for_agent_state(state_dir: &Path, agent_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut latest = String::new();
    while Instant::now() < deadline {
        let db = db::init(&state_dir.join("ahd.sqlite")).unwrap();
        latest = db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_default();
        if latest == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("agent {agent_id} did not become {expected} within {timeout:?}; latest={latest}");
}

fn master_runtime(state_dir: &Path, session_id: &str) -> (i64, i64, String) {
    let db = db::init(&state_dir.join("ahd.sqlite")).unwrap();
    db.conn()
        .query_row(
            "SELECT master_pid, master_generation, status FROM sessions WHERE id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .unwrap()
}

fn assert_master_not_revived(
    state_dir: &Path,
    session_id: &str,
    old_pid: i64,
    old_generation: i64,
    timeout: Duration,
) -> (i64, i64, String) {
    let deadline = Instant::now() + timeout;
    let mut latest = master_runtime(state_dir, session_id);
    while Instant::now() < deadline {
        latest = master_runtime(state_dir, session_id);
        assert_eq!(
            latest.2, "CLOSED",
            "idle master death should migrate to CLOSED without reviving master"
        );
        assert_eq!(
            latest.0, old_pid,
            "idle master death should not install a replacement master pid"
        );
        assert_eq!(
            latest.1, old_generation,
            "idle master death should not advance master generation"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    latest
}

fn wait_for_master_revive(
    state_dir: &Path,
    session_id: &str,
    old_pid: i64,
    old_generation: i64,
    timeout: Duration,
) -> (i64, i64, String) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let runtime = master_runtime(state_dir, session_id);
        if runtime.0 != old_pid && runtime.1 > old_generation && runtime.2 == "ACTIVE" {
            return runtime;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "master for {session_id} was not revived within {timeout:?}; latest={:?}",
        master_runtime(state_dir, session_id)
    );
}

fn agent_session_exists(state_dir: &Path, agent_id: &str) -> bool {
    let server = TmuxServer::new(state_dir);
    Command::new("tmux")
        .args([
            "-L",
            server.socket_name(),
            "has-session",
            "-t",
            &agent_session_name(agent_id),
        ])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn agent_session_pane_pid(state_dir: &Path, agent_id: &str) -> Option<i32> {
    let server = TmuxServer::new(state_dir);
    let output = Command::new("tmux")
        .args([
            "-L",
            server.socket_name(),
            "display-message",
            "-p",
            "-t",
            &agent_session_name(agent_id),
            "#{pane_pid}",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

fn wait_for_agent_session_replaced_or_absent(
    state_dir: &Path,
    agent_id: &str,
    old_pid: i32,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match agent_session_pane_pid(state_dir, agent_id) {
            None => return,
            Some(pid) if pid != old_pid => return,
            Some(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    panic!(
        "old worker tmux session for {agent_id} was not reaped/replaced within {timeout:?}; latest_pid={:?}",
        agent_session_pane_pid(state_dir, agent_id)
    );
}

fn wait_for_agent_session_replaced(
    state_dir: &Path,
    agent_id: &str,
    old_pid: i32,
    timeout: Duration,
) -> i32 {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match agent_session_pane_pid(state_dir, agent_id) {
            Some(pid) if pid != old_pid => return pid,
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    panic!(
        "worker tmux session for {agent_id} was not re-provisioned with a new pane pid within {timeout:?}; latest_pid={:?}",
        agent_session_pane_pid(state_dir, agent_id)
    );
}

fn ah_process_count(state_dir: &Path) -> usize {
    let ah_exe = env!("CARGO_BIN_EXE_ahd");
    let state_needle = format!("AH_STATE_DIR={}", state_dir.display());
    std::fs::read_dir("/proc")
        .expect("read /proc")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .chars()
                .all(|c| c.is_ascii_digit())
        })
        .filter(|entry| {
            let proc_dir = entry.path();
            let cmdline = std::fs::read(proc_dir.join("cmdline")).unwrap_or_default();
            let environ = std::fs::read(proc_dir.join("environ")).unwrap_or_default();
            String::from_utf8_lossy(&cmdline).contains(ah_exe)
                && String::from_utf8_lossy(&environ).contains(&state_needle)
        })
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn second_daemon_exits_without_stealing_live_socket() {
    let _guard = dev_state_lock();
    let state_dir = dev_state_dir();
    std::fs::create_dir_all(&state_dir).unwrap();
    cleanup_dev_state(&state_dir);
    let _state_cleanup = DevStateCleanupGuard::new(&state_dir);
    let socket_path = state_dir.join("ahd.sock");

    let child = spawn_daemon(&state_dir);
    UnixStream::connect(&socket_path).expect("first daemon socket should accept connections");

    let second = Command::new(env!("CARGO_BIN_EXE_ahd"))
        .env("AH_ENV", "dev")
        .env_remove("AH_SOCKET")
        .env("AH_STATE_DIR", &state_dir)
        .env("AH_UNSAFE_NO_SANDBOX", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn second ah");
    let output = wait_for_output(second, Duration::from_secs(5));
    let second_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "second ah should exit successfully, output={second_output}"
    );
    assert!(
        second_output.contains("already running"),
        "second ah output should explain singleton exit: {second_output}"
    );
    UnixStream::connect(&socket_path).expect("first daemon socket should remain connected");
    assert_eq!(
        ah_process_count(&state_dir),
        1,
        "only first ah for this test state dir should remain"
    );

    terminate_daemon(child);
    cleanup_dev_state(&state_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn active_master_raw_exit_reaps_old_worker_then_revives_master() {
    let _guard = dev_state_lock();
    require_tmux();
    let state_dir = dev_state_dir();
    std::fs::create_dir_all(&state_dir).unwrap();
    cleanup_dev_state(&state_dir);
    let _state_cleanup = DevStateCleanupGuard::new(&state_dir);
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();

    let mut child = spawn_daemon(&state_dir);
    let session_id = create_session(
        &socket_path,
        1,
        "p_master_exit_shutdown",
        project_dir.path(),
    );
    let master_pane = spawn_master(&socket_path, 2, &session_id);
    let agent_id = "ag_master_exit_shutdown";
    spawn_agent(&socket_path, 3, &session_id, agent_id);
    let _job_id = submit_job(&socket_path, 4, agent_id, "master-death-in-flight");
    let before = master_runtime(&state_dir, &session_id);
    assert_eq!(before.2, "ACTIVE");
    assert_eq!(active_agent_count(&state_dir), 1);
    assert!(agent_session_exists(&state_dir, agent_id));
    let old_agent_pid = agent_session_pane_pid(&state_dir, agent_id)
        .expect("old worker tmux session should expose a pane pid before master exit");

    kill_pid(pane_pid(&state_dir, &master_pane));

    wait_for_agent_session_replaced_or_absent(
        &state_dir,
        agent_id,
        old_agent_pid,
        Duration::from_secs(8),
    );
    let after = wait_for_master_revive(
        &state_dir,
        &session_id,
        before.0,
        before.1,
        Duration::from_secs(8),
    );
    assert_ne!(after.0, before.0);
    assert!(after.1 > before.1);
    assert_eq!(after.2, "ACTIVE");
    wait_for_active_agent_count(&state_dir, 1, Duration::from_secs(8));
    let new_agent_pid = wait_for_agent_session_replaced(
        &state_dir,
        agent_id,
        old_agent_pid,
        Duration::from_secs(8),
    );
    assert_ne!(new_agent_pid, old_agent_pid);
    let status = wait_for_daemon_exit(&mut child, Duration::from_secs(3));
    assert!(
        status.is_none(),
        "ahd should stay alive after corrected active master death revive"
    );

    terminate_daemon(child);
    cleanup_dev_state(&state_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn ahd_restart_rearms_inherited_active_master_then_later_exit_is_detected() {
    let _guard = dev_state_lock();
    require_tmux();
    let state_dir = dev_state_dir();
    std::fs::create_dir_all(&state_dir).unwrap();
    cleanup_dev_state(&state_dir);
    let _state_cleanup = DevStateCleanupGuard::new(&state_dir);
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();

    let child = spawn_daemon(&state_dir);
    let session_id = create_session(&socket_path, 101, "p_restart_rearm", project_dir.path());
    let master_pane = spawn_master(&socket_path, 102, &session_id);
    let agent_id = "ag_restart_rearm";
    spawn_agent(&socket_path, 103, &session_id, agent_id);
    let _job_id = submit_job(&socket_path, 104, agent_id, "restart-rearm-in-flight");
    let before = master_runtime(&state_dir, &session_id);
    let old_agent_pid = agent_session_pane_pid(&state_dir, agent_id)
        .expect("old worker tmux session should expose a pane pid before daemon restart");

    kill_daemon_without_cleanup(child, &socket_path);
    assert!(agent_session_exists(&state_dir, agent_id));

    let mut restarted = spawn_daemon(&state_dir);
    kill_pid(pane_pid(&state_dir, &master_pane));

    wait_for_agent_session_replaced_or_absent(
        &state_dir,
        agent_id,
        old_agent_pid,
        Duration::from_secs(12),
    );
    let after = wait_for_master_revive(
        &state_dir,
        &session_id,
        before.0,
        before.1,
        Duration::from_secs(12),
    );
    assert_ne!(after.0, before.0);
    assert!(after.1 > before.1);
    assert_eq!(after.2, "ACTIVE");
    let status = wait_for_daemon_exit(&mut restarted, Duration::from_secs(3));
    assert!(
        status.is_none(),
        "restarted ahd should stay alive after inherited master death"
    );

    terminate_daemon(restarted);
    cleanup_dev_state(&state_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_master_raw_exit_reaps_worker_without_reviving_master_or_stopping_ahd() {
    let _guard = dev_state_lock();
    require_tmux();
    let state_dir = dev_state_dir();
    std::fs::create_dir_all(&state_dir).unwrap();
    cleanup_dev_state(&state_dir);
    let _state_cleanup = DevStateCleanupGuard::new(&state_dir);
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();

    let mut child = spawn_daemon(&state_dir);
    let session_id = create_session(
        &socket_path,
        10,
        "p_idle_master_exit_no_revive",
        project_dir.path(),
    );
    let master_pane = spawn_master(&socket_path, 11, &session_id);
    let agent_id = "ag_idle_master_exit_reap";
    spawn_agent(&socket_path, 12, &session_id, agent_id);
    wait_for_agent_state(&state_dir, agent_id, "IDLE", Duration::from_secs(8));
    let before = master_runtime(&state_dir, &session_id);
    assert_eq!(before.2, "ACTIVE");
    assert_eq!(active_agent_count(&state_dir), 1);

    kill_pid(pane_pid(&state_dir, &master_pane));

    wait_for_active_agent_count(&state_dir, 0, Duration::from_secs(8));
    assert!(
        !agent_session_exists(&state_dir, agent_id),
        "idle master death should still reap old worker runtime"
    );
    let after = assert_master_not_revived(
        &state_dir,
        &session_id,
        before.0,
        before.1,
        Duration::from_secs(3),
    );
    assert_eq!(after.2, "CLOSED");
    let status = wait_for_daemon_exit(&mut child, Duration::from_secs(3));
    assert!(
        status.is_none(),
        "ahd should remain resident after idle master death; B does not mean daemon shutdown"
    );
    assert_eq!(active_agent_count(&state_dir), 0);

    terminate_daemon(child);
    cleanup_dev_state(&state_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn session_kill_marks_intentional_and_does_not_revive_master() {
    let _guard = dev_state_lock();
    require_tmux();
    let state_dir = dev_state_dir();
    std::fs::create_dir_all(&state_dir).unwrap();
    cleanup_dev_state(&state_dir);
    let _state_cleanup = DevStateCleanupGuard::new(&state_dir);
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();

    let mut child = spawn_daemon(&state_dir);
    let session_id = create_session(
        &socket_path,
        20,
        "p_session_kill_no_revive",
        project_dir.path(),
    );
    let _master_pane = spawn_master(&socket_path, 21, &session_id);
    spawn_agent(&socket_path, 22, &session_id, "ag_session_kill_no_revive");
    let before = master_runtime(&state_dir, &session_id);
    assert_eq!(before.2, "ACTIVE");
    assert_eq!(active_agent_count(&state_dir), 1);

    let _ = rpc_call(
        &socket_path,
        23,
        "session.kill",
        json!({ "session_id": session_id }),
    );
    std::thread::sleep(Duration::from_secs(2));
    let after = master_runtime(&state_dir, &session_id);
    assert_eq!(after.2, "KILLED");
    assert_eq!(
        after.1, before.1,
        "intentional session.kill should not advance master generation via corrected revive"
    );
    let status = wait_for_daemon_exit(&mut child, Duration::from_secs(1));
    assert!(
        status.is_none(),
        "session.kill should not stop ahd while proving master death is intentional"
    );

    terminate_daemon(child);
    cleanup_dev_state(&state_dir);
}
