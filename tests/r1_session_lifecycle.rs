mod common;

use ah::agent_io::registry::{AgentIoEntry, cleanup_agent_runtime_resources, contains, register};
use ah::marker::{TimerKind, parser_registry, registry, spawn_marker_timer_task};
use ah::tmux::{TmuxPaneId, agent_session_name};
use common::TmuxServerGuard;
use std::process::Command;
use std::sync::{Arc, Mutex};

fn require_tmux() {
    which::which("tmux").expect("tmux binary required for r1 session lifecycle tests");
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_session_lifecycle_removes_session() {
    require_tmux();
    let tmp = tempfile::tempdir().unwrap();
    let server = TmuxServerGuard::new(tmp.path());
    let session_name = "r1-session-lifecycle";

    let result = async {
        server
            .ensure_session(session_name.to_string(), tmp.path().to_path_buf())
            .await?;
        server.kill_session(session_name.to_string()).await?;
        let has_session = Command::new("tmux")
            .args([
                "-L",
                server.socket_name(),
                "has-session",
                "-t",
                session_name,
            ])
            .output()
            .expect("tmux has-session should run");
        assert!(
            !has_session.status.success(),
            "tmux session should be gone after kill-session"
        );
        Ok::<(), ah::error::AhError>(())
    }
    .await;

    result.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ensure_session_locks_pty_size_and_window_size_manual() {
    require_tmux();
    let tmp = tempfile::tempdir().unwrap();
    let server = TmuxServerGuard::new(tmp.path());
    let session_name = "r1-pty-lock";

    let result = async {
        server
            .ensure_session(session_name.to_string(), tmp.path().to_path_buf())
            .await?;

        let size = Command::new("tmux")
            .args([
                "-L",
                server.socket_name(),
                "display-message",
                "-t",
                session_name,
                "-p",
                "#{window_width}x#{window_height}",
            ])
            .output()
            .expect("tmux display-message should run");
        assert!(size.status.success());
        assert_eq!(String::from_utf8_lossy(&size.stdout).trim(), "150x60");

        let option = Command::new("tmux")
            .args([
                "-L",
                server.socket_name(),
                "show-options",
                "-t",
                session_name,
                "window-size",
            ])
            .output()
            .expect("tmux show-options should run");
        assert!(option.status.success());
        assert_eq!(
            String::from_utf8_lossy(&option.stdout).trim(),
            "window-size manual"
        );

        Ok::<(), ah::error::AhError>(())
    }
    .await;

    result.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_agent_runtime_resources_kills_agent_session() {
    require_tmux();
    let tmp = tempfile::tempdir().unwrap();
    let server = TmuxServerGuard::new(tmp.path());

    let agent_id = "r1_cleanup_agent";
    let session_id = "r1_cleanup_session";
    let session_name = agent_session_name(agent_id);
    let pipes = tmp.path().join("pipes");
    let sandbox = tmp.path().join("sandboxes").join(session_id).join(agent_id);
    std::fs::create_dir_all(&pipes).unwrap();
    std::fs::create_dir_all(&sandbox).unwrap();
    let fifo_path = pipes.join("r1_cleanup_agent.fifo");
    std::fs::write(&fifo_path, b"").unwrap();
    let reader_handle = tokio::spawn(async {
        std::future::pending::<()>().await;
    });
    let db_file = tempfile::NamedTempFile::new().unwrap();
    let db = ah::db::init(db_file.path()).unwrap();
    let parser = Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0)));
    parser_registry::register(agent_id.to_string(), parser.clone());
    let marker_handle =
        spawn_marker_timer_task(agent_id.to_string(), TimerKind::Busy, Arc::new(db), parser);
    registry::register(agent_id.to_string(), marker_handle);

    let result = async {
        server
            .ensure_session(session_name.clone(), tmp.path().to_path_buf())
            .await?;
        register(
            agent_id.to_string(),
            AgentIoEntry {
                session_id: session_id.to_string(),
                pane_id: TmuxPaneId("%1".to_string()),
                expected_pid: None,
                reader_handle,
                fifo_path: fifo_path.clone(),
                socket_name: server.socket_name().to_string(),
                idle_scan_enabled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            },
        );

        cleanup_agent_runtime_resources(agent_id, Some(session_id));

        let has_session = Command::new("tmux")
            .args([
                "-L",
                server.socket_name(),
                "has-session",
                "-t",
                &session_name,
            ])
            .output()
            .expect("tmux has-session should run");
        assert!(
            !has_session.status.success(),
            "agent tmux session should be gone after cleanup"
        );
        assert!(!contains(agent_id));
        assert!(!fifo_path.exists());
        assert!(!sandbox.exists());
        assert!(!parser_registry::contains(agent_id));
        assert!(!registry::contains(agent_id));
        Ok::<(), ah::error::AhError>(())
    }
    .await;

    result.unwrap();
}
