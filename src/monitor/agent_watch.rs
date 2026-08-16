//! Agent pidfd readiness task that marks unexpected process exit.

use crate::db::{self as db, Db};
use crate::monitor::MonitorHandle;
use crate::platform::sys::proc_info::ProcessLiveness;
use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{Interest, unix::AsyncFd};
#[cfg(unix)]
use tokio::time::Duration;

/// Spawn an async pidfd watcher for one agent process.
pub fn spawn_agent_pidfd_watch_task(
    agent_id: String,
    session_id: String,
    pid: i32,
    pidfd: MonitorHandle,
    db: Arc<Db>,
    claude_gateway: Option<Arc<crate::claude_gateway::ClaudeGatewayService>>,
) {
    #[cfg(windows)]
    {
        let _ = (session_id, pid, pidfd, db, claude_gateway);
        tokio::spawn(async move {
            tracing::warn!(
                agent_id = %agent_id,
                "Windows agent process watcher is not implemented until M1"
            );
        });
    }

    #[cfg(unix)]
    tokio::spawn(async move {
        let async_fd = match AsyncFd::with_interest(pidfd, Interest::READABLE) {
            Ok(async_fd) => async_fd,
            Err(err) => {
                tracing::warn!(agent_id = %agent_id, error = %err, "failed to create AsyncFd for agent pidfd");
                cleanup(&agent_id, &session_id, claude_gateway.as_ref()).await;
                return;
            }
        };

        loop {
            let mut guard = match async_fd.readable().await {
                Ok(guard) => guard,
                Err(err) => {
                    tracing::warn!(agent_id = %agent_id, error = %err, "agent pidfd readiness wait failed");
                    cleanup(&agent_id, &session_id, claude_gateway.as_ref()).await;
                    return;
                }
            };

            tokio::time::sleep(Duration::from_millis(50)).await;
            match kill_zero_check(pid) {
                ProcessLiveness::Dead => break,
                ProcessLiveness::Alive => {
                    tracing::debug!(agent_id = %agent_id, pid, "agent pidfd readable but process is still alive; continuing watch");
                    guard.clear_ready();
                    continue;
                }
                ProcessLiveness::Unknown => {
                    tracing::warn!(agent_id = %agent_id, pid, "agent pidfd readable but process liveness is unknown; preserving agent");
                    guard.clear_ready();
                    continue;
                }
            }
        }

        let raw = crate::platform::sys::proc_info::raw_fd(async_fd.get_ref());
        let exit_code = waitid_exit_code(raw);
        tracing::info!(
            agent_id = %agent_id,
            pid,
            exit_code = ?exit_code,
            "agent pidfd confirmed dead"
        );
        match db::agents::query_agent_state(db.as_ref().clone(), agent_id.clone()).await {
            Ok(Some((state, _))) if state == db::state_machine::STATE_PROMPT_PENDING => {
                tracing::info!(
                    agent_id = %agent_id,
                    "agent pidfd exit ignored while agent is PROMPT_PENDING"
                );
                return;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(agent_id = %agent_id, error = %err, "failed to query agent state before pidfd crash handling");
            }
        }
        if let Err(err) = db::agents_lifecycle::mark_agent_crashed_with_exit(
            db.as_ref().clone(),
            agent_id.clone(),
            exit_code,
        )
        .await
        {
            tracing::warn!(agent_id = %agent_id, error = %err, "failed to persist agent pidfd exit");
        }
        if let Some(handle) = crate::marker::registry::take(&agent_id) {
            let _ = handle.cancel_tx.send(());
        }
        let _ = crate::marker::parser_registry::remove(&agent_id);

        // Recoverable CRASHED home preservation relies on mark_agent_crashed_sync having already
        // popped the registry entry; this later Default cleanup must remain a no-op for that case.
        cleanup(&agent_id, &session_id, claude_gateway.as_ref()).await;
    });
}

fn kill_zero_check(pid: i32) -> ProcessLiveness {
    crate::platform::sys::proc_info::kill_zero_check(pid)
}

fn proc_state(pid: i32) -> Option<u8> {
    crate::platform::sys::proc_info::proc_state(pid)
}

fn waitid_exit_code(pidfd_raw: i32) -> Option<i32> {
    crate::platform::sys::proc_info::waitid_exit_code(pidfd_raw)
}

async fn cleanup(
    agent_id: &str,
    session_id: &str,
    claude_gateway: Option<&Arc<crate::claude_gateway::ClaudeGatewayService>>,
) {
    if let Some(claude_gateway) = claude_gateway {
        claude_gateway.deregister(agent_id).await;
    }
    let _ = crate::agent_io::shutdown_reader(agent_id, Some(session_id)).await;
    let _ = crate::monitor::remove(agent_id);
}

#[cfg(all(test, unix))]
mod tests {
    use super::{ProcessLiveness, kill_zero_check, proc_state, spawn_agent_pidfd_watch_task};
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::agents_lifecycle::mark_agent_killed_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::{STATE_PROMPT_PENDING, mark_agent_prompt_pending_sync};
    use crate::monitor::{contains, pidfd_open, register, remove};
    use rusqlite::OptionalExtension;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    async fn sleep_ms(ms: u64) {
        tokio::task::spawn_blocking(move || std::thread::sleep(Duration::from_millis(ms)))
            .await
            .unwrap();
    }

    async fn wait_for_state(db: &db::Db, agent_id: &str, expected: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let state: Option<String> = {
                let conn = db.conn();
                conn.query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                    row.get(0)
                })
                .optional()
                .unwrap()
            };
            if state.as_deref() == Some(expected) {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    async fn wait_until_monitor_absent(agent_id: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if !contains(agent_id) {
                return true;
            }
            sleep_ms(50).await;
        }
        false
    }

    fn wait_for_proc_state(pid: i32, expected: u8) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if proc_state(pid) == Some(expected) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_watch_marks_crashed_and_cleans_registry() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let agent_id = format!("ag_pidfd_{}", uuid::Uuid::new_v4());

        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, &agent_id, "s1", "bash", "IDLE", None).unwrap();
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.2; exit 0")
            .spawn()
            .unwrap();
        let pidfd = pidfd_open(child.id() as i32).unwrap();
        let task_fd = pidfd.try_clone().unwrap();
        register(agent_id.clone(), pidfd);

        spawn_agent_pidfd_watch_task(
            agent_id.clone(),
            "s1".to_string(),
            child.id() as i32,
            task_fd,
            Arc::new(db.clone()),
            None,
        );
        tokio::task::spawn_blocking(move || {
            let _ = child.wait();
        });

        assert!(wait_for_state(&db, &agent_id, "CRASHED").await);
        assert!(wait_until_monitor_absent(&agent_id).await);

        let (state, exit_code, payload): (String, Option<i64>, String) = db
            .conn()
            .query_row(
                "SELECT agents.state, agents.exit_code, events.payload FROM agents JOIN events ON events.agent_id = agents.id WHERE agents.id = ?",
                [agent_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(state, "CRASHED");
        assert_eq!(exit_code, None);
        assert_eq!(payload["reason"], "EXIT_CODE_UNAVAILABLE_NON_CHILD");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_watch_does_not_overwrite_killed() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let agent_id = format!("ag_pidfd_killed_{}", uuid::Uuid::new_v4());

        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, &agent_id, "s1", "bash", "IDLE", None).unwrap();
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .spawn()
            .unwrap();
        let pidfd = pidfd_open(child.id() as i32).unwrap();
        let task_fd = pidfd.try_clone().unwrap();
        register(agent_id.clone(), pidfd);
        mark_agent_killed_sync(&db, &agent_id, "TEST_KILL").unwrap();

        spawn_agent_pidfd_watch_task(
            agent_id.clone(),
            "s1".to_string(),
            child.id() as i32,
            task_fd,
            Arc::new(db.clone()),
            None,
        );
        child.kill().unwrap();

        assert!(wait_for_state(&db, &agent_id, "KILLED").await);
        let _ = child.wait();
        sleep_ms(300).await;

        let (state, event_count): (String, i64) = db
            .conn()
            .query_row(
                "SELECT agents.state, COUNT(events.seq_id) FROM agents LEFT JOIN events ON events.agent_id = agents.id WHERE agents.id = ?",
                [agent_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "KILLED");
        assert_eq!(event_count, 1);

        let _ = remove(&agent_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_watch_skips_prompt_pending_crash() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        let agent_id = format!("ag_pidfd_pending_{}", uuid::Uuid::new_v4());

        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, &agent_id, "s1", "bash", "IDLE", None).unwrap();
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .spawn()
            .unwrap();
        let pidfd = pidfd_open(child.id() as i32).unwrap();
        let task_fd = pidfd.try_clone().unwrap();
        register(agent_id.clone(), pidfd);
        mark_agent_prompt_pending_sync(&db, &agent_id, "UNKNOWN_PROMPT").unwrap();

        spawn_agent_pidfd_watch_task(
            agent_id.clone(),
            "s1".to_string(),
            child.id() as i32,
            task_fd,
            Arc::new(db.clone()),
            None,
        );
        child.kill().unwrap();
        let _ = child.wait();
        sleep_ms(300).await;

        let (state, exit_code): (String, Option<i64>) = db
            .conn()
            .query_row(
                "SELECT state, exit_code FROM agents WHERE id = ?",
                [agent_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, STATE_PROMPT_PENDING);
        assert_eq!(exit_code, None);

        let _ = remove(&agent_id);
    }

    #[test]
    fn test_agent_watch_kill_zero_alive_process_returns_alive() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .spawn()
            .unwrap();

        assert_eq!(kill_zero_check(child.id() as i32), ProcessLiveness::Alive);

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn test_agent_watch_kill_zero_dead_process_returns_dead() {
        let mut child = Command::new("sh").arg("-c").arg("exit 0").spawn().unwrap();
        let pid = child.id() as i32;
        let _ = child.wait();

        assert_eq!(kill_zero_check(pid), ProcessLiveness::Dead);
    }

    #[test]
    fn test_agent_watch_kill_zero_treats_zombie_as_dead() {
        let mut child = Command::new("sh").arg("-c").arg("exit 0").spawn().unwrap();
        let pid = child.id() as i32;

        assert!(
            wait_for_proc_state(pid, b'Z'),
            "child did not become zombie before wait"
        );
        assert_eq!(kill_zero_check(pid), ProcessLiveness::Dead);

        let _ = child.wait();
    }
}
