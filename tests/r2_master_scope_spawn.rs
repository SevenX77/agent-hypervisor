#![cfg(target_os = "linux")]
use ah::db;
use ah::tmux::{TmuxServer, agent_session_name};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

static SCOPE_DOGFOOD_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Clone)]
struct MasterScopeEvidence {
    pane_pid: i32,
    oom_score_adj: String,
}

fn require_scope_environment() {
    which::which("tmux").expect("tmux binary required for master scope dogfood");
    which::which("systemd-run").expect("systemd-run binary required for master scope dogfood");
    let output = Command::new("systemd-run")
        .args(["--user", "--scope", "--collect", "--", "true"])
        .output()
        .expect("probe systemd-run --user --scope");
    assert!(
        output.status.success(),
        "systemd user scope is required; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn cleanup_state(state_dir: &Path) {
    let server = TmuxServer::new(state_dir);
    let _ = Command::new("tmux")
        .args(["-L", server.socket_name(), "kill-server"])
        .output();
    let _ = std::fs::remove_dir_all(state_dir);
}

fn spawn_daemon(state_dir: &Path) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_ahd"))
        .env("AH_ENV", "dev")
        .env_remove("AH_SOCKET")
        .env("AH_STATE_DIR", state_dir)
        .spawn()
        .expect("spawn ahd for master scope dogfood");
    wait_for_socket(&state_dir.join("ahd.sock"), Duration::from_secs(8));
    child
}

fn terminate_daemon(mut child: Child) {
    // SAFETY: child.id is the ahd process spawned by this test.
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().expect("poll ahd child").is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn wait_for_socket(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("ahd socket did not appear at {}", path.display());
}

fn rpc_call(socket_path: &Path, id: u64, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(socket_path).expect("connect ahd socket");
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
            "text": "sleep 30; echo master death scope dogfood\n",
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
        "active agent count did not become {expected}; latest={}",
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
    panic!("agent {agent_id} did not become {expected}; latest={latest}");
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
        "master for {session_id} did not revive; latest={:?}",
        master_runtime(state_dir, session_id)
    );
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
        assert_eq!(latest.2, "ACTIVE");
        assert_eq!(latest.0, old_pid);
        assert_eq!(latest.1, old_generation);
        std::thread::sleep(Duration::from_millis(100));
    }
    latest
}

fn process_alive(pid: i32) -> bool {
    // SAFETY: kill(pid, 0) probes for process existence without sending a signal.
    unsafe { libc::kill(pid, 0) == 0 }
}

fn kill_master(pid: i64) {
    // SAFETY: pid comes from ahd's recorded master runtime.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
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

fn agent_scope_unit(state_dir: &Path, agent_id: &str) -> Option<String> {
    agent_scope_line(state_dir, agent_id)
        .and_then(|line| line.split_whitespace().next().map(str::to_string))
}

fn agent_scope_line(state_dir: &Path, agent_id: &str) -> Option<String> {
    let server = TmuxServer::new(state_dir);
    let needle = format!("ah-agent-{agent_id}@{}", server.socket_name());
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
        .expect("systemctl list-units");
    assert!(
        output.status.success(),
        "systemctl list-units failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find(|line| line.contains(&needle))
        .map(str::to_string)
}

fn wait_for_agent_scope_gone(state_dir: &Path, agent_id: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if agent_scope_unit(state_dir, agent_id).is_none() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "agent scope for {agent_id} did not disappear; latest={:?}",
        agent_scope_unit(state_dir, agent_id)
    );
}

fn master_scope_evidence(state_dir: &Path, session_id: &str, pane_id: &str) -> MasterScopeEvidence {
    let pid = pane_pid(state_dir, pane_id);
    assert!(process_alive(pid), "master pane pid {pid} should be alive");
    let recorded_pid = master_runtime(state_dir, session_id).0 as i32;
    assert_eq!(
        recorded_pid, pid,
        "DB master_pid should match tmux pane_pid after scope spawn"
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut oom_score_adj = String::new();
    while Instant::now() < deadline {
        oom_score_adj =
            std::fs::read_to_string(format!("/proc/{pid}/oom_score_adj")).unwrap_or_default();
        if oom_score_adj.trim() == "500" {
            return MasterScopeEvidence {
                pane_pid: pid,
                oom_score_adj: "500".to_string(),
            };
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    MasterScopeEvidence {
        pane_pid: pid,
        oom_score_adj: oom_score_adj.trim().to_string(),
    }
}

fn new_state_dir(label: &str) -> PathBuf {
    tempfile::Builder::new()
        .prefix(label)
        .tempdir()
        .unwrap()
        .keep()
}

#[test]
#[ignore = "requires real systemd --user scope and tmux; run explicitly for master scope dogfood"]
fn master_true_scope_spawn_sets_oom_score_and_pidfd_target_is_alive() {
    let _guard = SCOPE_DOGFOOD_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    require_scope_environment();
    let state_dir = new_state_dir("ah-master-scope-spawn-");
    cleanup_state(&state_dir);
    std::fs::create_dir_all(&state_dir).unwrap();
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();
    let daemon = spawn_daemon(&state_dir);

    let session_id = create_session(&socket_path, 1, "p_master_scope_spawn", project_dir.path());
    let master_pane = spawn_master(&socket_path, 2, &session_id);
    let evidence = master_scope_evidence(&state_dir, &session_id, &master_pane);

    eprintln!(
        "dogfood spawn evidence: pane_pid={} oom_score_adj={} alive={}",
        evidence.pane_pid,
        evidence.oom_score_adj,
        process_alive(evidence.pane_pid)
    );
    assert_eq!(evidence.oom_score_adj, "500");
    assert!(process_alive(evidence.pane_pid));

    terminate_daemon(daemon);
    cleanup_state(&state_dir);
}

#[test]
#[ignore = "requires real systemd --user scope and tmux; run explicitly for master scope dogfood"]
fn active_master_true_scope_death_reaps_worker_and_revives() {
    let _guard = SCOPE_DOGFOOD_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    require_scope_environment();
    let state_dir = new_state_dir("ah-master-scope-active-");
    cleanup_state(&state_dir);
    std::fs::create_dir_all(&state_dir).unwrap();
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();
    let mut daemon = spawn_daemon(&state_dir);

    let session_id = create_session(
        &socket_path,
        10,
        "p_master_scope_active",
        project_dir.path(),
    );
    let master_pane = spawn_master(&socket_path, 11, &session_id);
    let master_evidence = master_scope_evidence(&state_dir, &session_id, &master_pane);
    assert_eq!(master_evidence.oom_score_adj, "500");

    let agent_id = "ag_master_scope_active";
    spawn_agent(&socket_path, 12, &session_id, agent_id);
    wait_for_agent_state(&state_dir, agent_id, "IDLE", Duration::from_secs(10));
    let worker_scope_line_before =
        agent_scope_line(&state_dir, agent_id).expect("worker scope should exist");
    let worker_scope = agent_scope_unit(&state_dir, agent_id).expect("worker scope should exist");
    let _job_id = submit_job(&socket_path, 13, agent_id, "master-scope-active");
    let before = master_runtime(&state_dir, &session_id);

    kill_master(before.0);

    wait_for_active_agent_count(&state_dir, 0, Duration::from_secs(10));
    wait_for_agent_scope_gone(&state_dir, agent_id, Duration::from_secs(10));
    let worker_scope_line_after = agent_scope_line(&state_dir, agent_id);
    assert!(!agent_session_exists(&state_dir, agent_id));
    let after = wait_for_master_revive(
        &state_dir,
        &session_id,
        before.0,
        before.1,
        Duration::from_secs(10),
    );
    assert_eq!(after.2, "ACTIVE");
    assert!(!process_alive(master_evidence.pane_pid));
    assert!(worker_scope.ends_with(".scope"));
    eprintln!(
        "dogfood active evidence: killed_master_pid={} oom_score_adj={} worker_scope_before={:?} worker_scope_after={:?} revived_pid={} generation {}->{} status={}",
        master_evidence.pane_pid,
        master_evidence.oom_score_adj,
        worker_scope_line_before,
        worker_scope_line_after,
        after.0,
        before.1,
        after.1,
        after.2
    );

    assert!(
        daemon.try_wait().unwrap().is_none(),
        "ahd should stay alive after active master death"
    );
    terminate_daemon(daemon);
    cleanup_state(&state_dir);
}

#[test]
#[ignore = "requires real systemd --user scope and tmux; run explicitly for master scope dogfood"]
fn idle_master_true_scope_death_reaps_worker_without_revive() {
    let _guard = SCOPE_DOGFOOD_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    require_scope_environment();
    let state_dir = new_state_dir("ah-master-scope-idle-");
    cleanup_state(&state_dir);
    std::fs::create_dir_all(&state_dir).unwrap();
    let socket_path = state_dir.join("ahd.sock");
    let project_dir = tempfile::TempDir::new().unwrap();
    let mut daemon = spawn_daemon(&state_dir);

    let session_id = create_session(&socket_path, 20, "p_master_scope_idle", project_dir.path());
    let master_pane = spawn_master(&socket_path, 21, &session_id);
    let master_evidence = master_scope_evidence(&state_dir, &session_id, &master_pane);
    assert_eq!(master_evidence.oom_score_adj, "500");

    let agent_id = "ag_master_scope_idle";
    spawn_agent(&socket_path, 22, &session_id, agent_id);
    wait_for_agent_state(&state_dir, agent_id, "IDLE", Duration::from_secs(10));
    let worker_scope_line_before =
        agent_scope_line(&state_dir, agent_id).expect("worker scope should exist");
    let worker_scope = agent_scope_unit(&state_dir, agent_id).expect("worker scope should exist");
    let before = master_runtime(&state_dir, &session_id);

    kill_master(before.0);

    wait_for_active_agent_count(&state_dir, 0, Duration::from_secs(10));
    wait_for_agent_scope_gone(&state_dir, agent_id, Duration::from_secs(10));
    let worker_scope_line_after = agent_scope_line(&state_dir, agent_id);
    assert!(!agent_session_exists(&state_dir, agent_id));
    let after = assert_master_not_revived(
        &state_dir,
        &session_id,
        before.0,
        before.1,
        Duration::from_secs(3),
    );
    assert_eq!(after.2, "ACTIVE");
    assert!(!process_alive(master_evidence.pane_pid));
    assert!(worker_scope.ends_with(".scope"));
    eprintln!(
        "dogfood idle evidence: killed_master_pid={} oom_score_adj={} worker_scope_before={:?} worker_scope_after={:?} master_pid_after={} generation {}->{} status={}",
        master_evidence.pane_pid,
        master_evidence.oom_score_adj,
        worker_scope_line_before,
        worker_scope_line_after,
        after.0,
        before.1,
        after.1,
        after.2
    );

    assert!(
        daemon.try_wait().unwrap().is_none(),
        "ahd should stay alive after idle master death"
    );
    terminate_daemon(daemon);
    cleanup_state(&state_dir);
}
