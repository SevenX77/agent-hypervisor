pub mod reader;
pub mod registry;
pub mod writer;

use crate::error::AhError;

pub use reader::spawn_agent_io_reader_task;
pub use registry::{
    AgentIoEntry, RuntimeCleanupPolicy, cleanup_agent_runtime_resources,
    cleanup_agent_runtime_resources_with_policy, contains, init_probe_binding, pane_id, register,
    remove, set_idle_scan_enabled, update_pane_id,
};
pub use writer::{send_text_to_pane, send_text_to_pane_with_options};

pub async fn send_text_to_registered_pane(
    agent_id: &str,
    provider: &str,
    text: String,
) -> Result<(), AhError> {
    let Some((pane, socket_name)) = registry::pane_binding(agent_id) else {
        tracing::warn!(
            agent_id,
            "agent pane not registered; skipping text injection"
        );
        return Ok(());
    };
    let tmux = std::sync::Arc::new(crate::tmux::TmuxServer::from_socket_name(socket_name));
    send_text_to_pane(tmux, agent_id, provider, pane, text).await
}

pub async fn shutdown_reader(
    agent_id: &str,
    expected_session_id: Option<&str>,
) -> Result<(), AhError> {
    registry::cleanup_agent_runtime_resources(agent_id, expected_session_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tmux::{TmuxServer, agent_session_name};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[tokio::test]
    async fn shutdown_reader_with_stale_session_does_not_cleanup_recycled_live_entry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = TmuxServer::new(tmp.path());
        let agent_id = "agent_io_stale_shutdown_live";
        let live_session_id = "sess_agent_io_stale_shutdown_live";
        let old_session_id = "sess_agent_io_stale_shutdown_old";
        let tmux_session = agent_session_name(agent_id);
        let pipes = tmp.path().join("pipes");
        std::fs::create_dir_all(&pipes).unwrap();
        let fifo_path = pipes.join(format!("{agent_id}.fifo"));
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        let result = server
            .ensure_session_sync(&tmux_session, tmp.path())
            .and_then(|()| {
                server.spawn_window_sync(
                    &tmux_session,
                    "live",
                    tmp.path(),
                    &["sh", "-lc", "sleep 60"],
                )
            });
        let live_pane = result.unwrap();
        let live_pid = server.get_pane_pid_sync(&live_pane).unwrap();
        registry::register(
            agent_id.to_string(),
            registry::AgentIoEntry {
                session_id: live_session_id.to_string(),
                pane_id: live_pane.clone(),
                expected_pid: Some(i64::from(live_pid)),
                reader_handle,
                fifo_path: fifo_path.clone(),
                socket_name: server.socket_name().to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );

        let result = async {
            shutdown_reader(agent_id, Some(old_session_id)).await?;

            assert!(
                registry::contains(agent_id),
                "live registry entry must remain"
            );
            assert!(fifo_path.exists(), "live fifo must remain");
            assert_eq!(server.get_pane_pid_sync(&live_pane)?, live_pid);
            assert!(server.session_exists_sync(&tmux_session)?);
            Ok::<(), AhError>(())
        }
        .await;

        let _ = registry::remove(agent_id);
        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
        result.unwrap();
    }
}
