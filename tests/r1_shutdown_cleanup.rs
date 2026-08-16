#![cfg(unix)]

use ah::db::{self as db, agents, sessions};
use ah::tmux::scope::ScopePolicy;
use ah::tmux::{TmuxServer, agent_session_name, master_session_name};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn require_tmux() {
    which::which("tmux").expect("tmux binary required for r1 shutdown cleanup tests");
}

fn dev_state_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("dev_state")
}

fn cleanup_dev_state_db(state_dir: &std::path::Path) {
    for suffix in ["", "-shm", "-wal"] {
        let _ = std::fs::remove_file(state_dir.join(format!("ahd.sqlite{suffix}")));
    }
    let _ = std::fs::remove_file(state_dir.join("ahd.sock"));
}

fn cleanup_server(server: &TmuxServer) {
    let _ = Command::new("tmux")
        .args(["-L", server.socket_name(), "kill-server"])
        .output();
}

fn has_session(server: &TmuxServer, session_name: &str) -> bool {
    Command::new("tmux")
        .args([
            "-L",
            server.socket_name(),
            "has-session",
            "-t",
            session_name,
        ])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn wait_for_socket(path: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("ccbd socket did not appear at {}", path.display());
}

fn terminate_and_wait(mut child: Child, timeout: Duration) {
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll ccbd child") {
            assert!(status.success(), "ccbd exited with {status}");
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let _ = child.kill();
    panic!("ccbd did not exit after SIGTERM");
}

#[tokio::test(flavor = "multi_thread")]
async fn daemon_shutdown_removes_agent_and_master_sessions() {
    require_tmux();
    let state_dir = dev_state_dir();
    std::fs::create_dir_all(&state_dir).unwrap();
    cleanup_dev_state_db(&state_dir);

    let server = TmuxServer::new_with_policy(&state_dir, ScopePolicy::None);
    cleanup_server(&server);

    let project_id = "r1_shutdown_project";
    let session_id = "r1_shutdown_session";
    let agent_ids = ["r1_shutdown_a1", "r1_shutdown_a2"];
    let master_session = master_session_name(project_id);
    let agent_sessions = agent_ids
        .iter()
        .map(|agent_id| agent_session_name(agent_id))
        .collect::<Vec<_>>();

    let db_path = state_dir.join("ahd.sqlite");
    let db = db::init(&db_path).unwrap();
    sessions::insert_session(
        db.clone(),
        session_id.to_string(),
        project_id.to_string(),
        state_dir.display().to_string(),
    )
    .await
    .unwrap();
    for agent_id in agent_ids {
        agents::insert_agent(
            db.clone(),
            agent_id.to_string(),
            session_id.to_string(),
            "bash".to_string(),
            "UNKNOWN".to_string(),
            None,
        )
        .await
        .unwrap();
    }
    drop(db);

    server
        .ensure_session(master_session.clone(), state_dir.clone())
        .await
        .unwrap();
    for session_name in &agent_sessions {
        server
            .ensure_session(session_name.clone(), state_dir.clone())
            .await
            .unwrap();
    }
    assert!(has_session(&server, &master_session));
    for session_name in &agent_sessions {
        assert!(has_session(&server, session_name));
    }

    let child = Command::new(env!("CARGO_BIN_EXE_ahd"))
        .env("CCB_ENV", "dev")
        .env("CCBD_UNSAFE_NO_SANDBOX", "1")
        .env_remove("CCB_SOCKET")
        .env_remove("AH_STATE_DIR")
        .env_remove("CCBD_STATE_DIR")
        .env_remove("XDG_STATE_HOME")
        .spawn()
        .expect("spawn ccbd");
    wait_for_socket(&state_dir.join("ahd.sock"), Duration::from_secs(5));
    terminate_and_wait(child, Duration::from_secs(5));

    assert!(!has_session(&server, &master_session));
    for session_name in &agent_sessions {
        assert!(!has_session(&server, session_name));
    }

    cleanup_server(&server);
    cleanup_dev_state_db(&state_dir);
}
