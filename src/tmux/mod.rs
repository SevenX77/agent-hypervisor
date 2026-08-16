pub mod error;
pub mod pane;
pub mod scope;
pub mod session;

use sha2::{Digest, Sha256};
use std::path::Path;

pub use error::TmuxError;
pub use pane::TmuxPaneId;
pub use session::{TmuxPaneRuntime, TmuxServer, TmuxWindowSize};

pub fn agent_session_name(agent_id: &str) -> String {
    format!("agent_{agent_id}")
}

/// tmux targets treat `.`, `:`, and whitespace as syntax, so names must use a
/// conservative component alphabet before they are passed to `-t` or `-n`.
pub(crate) fn sanitize_tmux_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn master_session_name(project_id: &str) -> String {
    format!("master_{}", sanitize_tmux_name(project_id))
}

pub fn compute_socket_name(state_dir: &Path) -> String {
    let canonical = state_dir
        .canonicalize()
        .unwrap_or_else(|_| state_dir.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(canonical.display().to_string().as_bytes());
    let hex = format!("{:x}", hasher.finalize());
    format!("ahd-{}", &hex[..16])
}

#[cfg(all(test, unix))]
mod tests {
    use super::{TmuxServer, agent_session_name, compute_socket_name, master_session_name};
    use crate::tmux::pane::TmuxPaneId;
    use crate::tmux::session::parse_pane_pid_for_test;
    use nix::sys::stat::Mode;
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    use std::process::Command;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    fn require_tmux() {
        which::which("tmux").expect("tmux binary required for tmux module tests");
    }

    fn cleanup_server(server: &TmuxServer) {
        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
        let socket_path = format!(
            "/tmp/tmux-{}/{}",
            unsafe { libc::geteuid() },
            server.socket_name()
        );
        let _ = std::fs::remove_file(socket_path);
    }

    async fn wait_for_pane_text(
        server: &TmuxServer,
        pane: &TmuxPaneId,
        needle: &str,
        timeout: Duration,
    ) -> Result<(), crate::error::CcbdError> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let capture = server.capture_pane(pane.clone()).await?;
            if capture.contains(needle) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(crate::error::CcbdError::TmuxCommandFailed {
            cmd: "wait_for_pane_text".into(),
            stderr: format!("pane did not contain {needle:?} within {timeout:?}"),
            exit: -1,
        })
    }

    #[test]
    fn test_compute_socket_name_deterministic() {
        let tmp = tempfile::tempdir().unwrap();
        let first = compute_socket_name(tmp.path());
        let second = compute_socket_name(tmp.path());

        assert_eq!(first, second);
        assert!(first.starts_with("ahd-"));
        assert_eq!(first.len(), "ahd-".len() + 16);
    }

    #[test]
    fn test_session_name_helpers() {
        assert_eq!(agent_session_name("a1"), "agent_a1");
        assert_eq!(agent_session_name("foo"), "agent_foo");
        assert_eq!(master_session_name("project_xyz"), "master_project_xyz");
    }

    #[test]
    fn test_master_session_name_sanitizes_tmux_target_separators() {
        let dotted = master_session_name("my.app.v2");
        assert_eq!(dotted, "master_my_app_v2");
        assert!(!dotted.contains('.'));
        assert!(!dotted.contains(':'));
        assert!(
            dotted
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        );
        assert_eq!(master_session_name("a:b"), "master_a_b");
        assert_eq!(master_session_name("a1"), "master_a1");
    }

    #[test]
    fn test_pane_id_parse() {
        assert_eq!(TmuxPaneId::parse("%7").unwrap(), TmuxPaneId("%7".into()));
        assert!(TmuxPaneId::parse("7").is_err());
    }

    #[test]
    fn test_pid_parse_error() {
        assert!(parse_pane_pid_for_test("not-a-pid").is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_full_lifecycle() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());

        let result = async {
            server
                .ensure_session("test_full_lifecycle".into(), tmp.path().to_path_buf())
                .await?;
            let pane = server
                .spawn_window(
                    "test_full_lifecycle".into(),
                    "test-agent".into(),
                    tmp.path().to_path_buf(),
                    vec!["bash".into()],
                )
                .await?;
            let pid = server.get_pane_pid(pane.clone()).await?;
            assert!(pid > 0);

            let fifo_path = tmp.path().join("agent.fifo");
            nix::unistd::mkfifo(&fifo_path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
            let mut fifo = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fifo_path)
                .unwrap();

            server
                .pipe_pane_to_fifo(pane.clone(), fifo_path.clone())
                .await?;
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut seen = String::new();
                let mut buf = [0_u8; 256];
                while Instant::now() < deadline && !seen.contains("echo hello") {
                    match fifo.read(&mut buf) {
                        Ok(0) => continue,
                        Ok(n) => seen.push_str(&String::from_utf8_lossy(&buf[..n])),
                        Err(_) => break,
                    }
                }
                let _ = tx.send(seen);
            });

            server
                .send_keys_literal(pane.clone(), "echo hello".into())
                .await?;
            server
                .send_keys_keysym(pane.clone(), "Enter".into())
                .await?;

            let seen = rx.recv_timeout(Duration::from_secs(6)).unwrap_or_default();
            assert!(
                seen.contains("echo hello"),
                "fifo output did not contain command echo; seen={seen:?}"
            );

            server.kill_pane(pane).await?;
            Ok::<(), crate::error::CcbdError>(())
        }
        .await;

        cleanup_server(&server);
        result.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_master_session_name_with_dot_is_targetable() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());
        let session = master_session_name("my.app.v2");

        let result = async {
            server
                .ensure_session(session.clone(), tmp.path().to_path_buf())
                .await?;
            server
                .spawn_window(
                    session,
                    "my_app_v2".into(),
                    tmp.path().to_path_buf(),
                    vec!["bash".into(), "-lc".into(), "printf ready; sleep 1".into()],
                )
                .await?;
            Ok::<(), crate::error::CcbdError>(())
        }
        .await;

        cleanup_server(&server);
        result.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_spawn_window_fast_exit_keeps_pane_and_server_alive() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());
        let session = "test_fast_exit_remain";

        let result = async {
            server
                .ensure_session(session.to_string(), tmp.path().to_path_buf())
                .await?;

            let initial_pane = server
                .spawn_window(
                    session.to_string(),
                    "initial-fast-exit".to_string(),
                    tmp.path().to_path_buf(),
                    vec!["sh".into(), "-lc".into(), "exit 0".into()],
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                server
                    .window_exists(session.to_string(), "initial-fast-exit".to_string())
                    .await?,
                "fast-exit respawn of reusable initial window must keep the window alive"
            );
            let panes = server
                .list_panes(format!("{session}:initial-fast-exit"))
                .await?;
            assert!(
                panes.iter().any(|pane| pane == &initial_pane),
                "fast-exit respawn must leave its pane via remain-on-exit"
            );

            let new_pane = server
                .spawn_window(
                    session.to_string(),
                    "new-fast-exit".to_string(),
                    tmp.path().to_path_buf(),
                    vec!["sh".into(), "-lc".into(), "exit 0".into()],
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                server
                    .window_exists(session.to_string(), "new-fast-exit".to_string())
                    .await?,
                "fast-exit new-window path must keep the window alive"
            );
            let panes = server
                .list_panes(format!("{session}:new-fast-exit"))
                .await?;
            assert!(
                panes.iter().any(|pane| pane == &new_pane),
                "fast-exit new-window path must leave its pane via remain-on-exit"
            );

            assert!(
                server
                    .window_exists(session.to_string(), "initial-fast-exit".to_string())
                    .await?,
                "tmux server must remain alive after fast-exit panes"
            );

            Ok::<(), crate::error::CcbdError>(())
        }
        .await;

        cleanup_server(&server);
        result.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_load_paste_delete_multiline() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());

        let result = async {
            server
                .ensure_session("test_buffer_session".into(), tmp.path().to_path_buf())
                .await?;
            let pane = server
                .spawn_window(
                    "test_buffer_session".into(),
                    "test-buffer-agent".into(),
                    tmp.path().to_path_buf(),
                    vec![
                        "env".into(),
                        "PS1=$ ".into(),
                        "bash".into(),
                        "--noprofile".into(),
                        "--norc".into(),
                        "-i".into(),
                    ],
                )
                .await?;
            wait_for_pane_text(&server, &pane, "$", Duration::from_secs(15)).await?;

            let fifo_path = tmp.path().join("buffer-agent.fifo");
            nix::unistd::mkfifo(&fifo_path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
            let mut fifo = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&fifo_path)
                .unwrap();
            server
                .pipe_pane_to_fifo(pane.clone(), fifo_path.clone())
                .await?;
            tokio::time::sleep(Duration::from_secs(1)).await;

            let seen_output = Arc::new(Mutex::new(String::new()));
            let seen_for_reader = seen_output.clone();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(15);
                let mut buf = [0_u8; 512];
                while Instant::now() < deadline
                    && !seen_for_reader
                        .lock()
                        .is_ok_and(|seen| seen.contains("line 1") && seen.contains("line 2"))
                {
                    match fifo.read(&mut buf) {
                        Ok(0) => std::thread::sleep(Duration::from_millis(50)),
                        Ok(n) => {
                            if let Ok(mut seen) = seen_for_reader.lock() {
                                seen.push_str(&String::from_utf8_lossy(&buf[..n]));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        Err(_) => break,
                    }
                }
            });

            let buffer_name = "ccbd-test-buffer";
            let text = "for i in 1 2; do\n  echo \"line $i\"\ndone\n";
            for _ in 0..5 {
                server
                    .load_buffer(buffer_name.to_string(), text.to_string())
                    .await?;
                server
                    .paste_buffer(pane.clone(), buffer_name.to_string())
                    .await?;
                server.send_enter(pane.clone()).await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                if seen_output
                    .lock()
                    .is_ok_and(|seen| seen.contains("line 1") && seen.contains("line 2"))
                {
                    break;
                }
            }
            server.delete_buffer(buffer_name.to_string()).await?;

            let deadline = Instant::now() + Duration::from_secs(15);
            let seen = loop {
                let seen = seen_output
                    .lock()
                    .map(|seen| seen.clone())
                    .unwrap_or_default();
                if seen.contains("line 1") && seen.contains("line 2") {
                    break seen;
                }
                if Instant::now() >= deadline {
                    break seen;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            assert!(
                seen.contains("line 1"),
                "fifo output missing line 1; seen={seen:?}"
            );
            assert!(
                seen.contains("line 2"),
                "fifo output missing line 2; seen={seen:?}"
            );
            assert!(
                !seen.contains("syntax error near unexpected token"),
                "multiline paste produced shell syntax error; seen={seen:?}"
            );

            server.kill_pane(pane).await?;
            Ok::<(), crate::error::CcbdError>(())
        }
        .await;

        cleanup_server(&server);
        result.unwrap();
    }
}
