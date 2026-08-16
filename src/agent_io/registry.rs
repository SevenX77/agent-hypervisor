use crate::tmux::{TmuxPaneId, agent_session_name};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

pub struct AgentIoEntry {
    pub session_id: String,
    pub pane_id: TmuxPaneId,
    pub expected_pid: Option<i64>,
    pub reader_handle: tokio::task::JoinHandle<()>,
    pub fifo_path: PathBuf,
    pub socket_name: String,
    pub idle_scan_enabled: Arc<AtomicBool>,
}

pub static TMUX_PANE_MAP: LazyLock<Arc<Mutex<HashMap<String, AgentIoEntry>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCleanupPolicy {
    Default,
    PreserveRecoverableCrashedHome,
}

pub fn register(agent_id: String, entry: AgentIoEntry) {
    match TMUX_PANE_MAP.lock() {
        Ok(mut map) => {
            map.insert(agent_id, entry);
        }
        Err(err) => tracing::warn!(error = %err, "TMUX_PANE_MAP mutex poisoned during register"),
    }
}

pub fn pane_id(agent_id: &str) -> Option<TmuxPaneId> {
    TMUX_PANE_MAP
        .lock()
        .ok()
        .and_then(|map| map.get(agent_id).map(|entry| entry.pane_id.clone()))
}

pub fn update_pane_id(agent_id: &str, pane_id: TmuxPaneId) -> bool {
    match TMUX_PANE_MAP.lock() {
        Ok(mut map) => {
            let Some(entry) = map.get_mut(agent_id) else {
                return false;
            };
            entry.pane_id = pane_id;
            true
        }
        Err(err) => {
            tracing::warn!(error = %err, "TMUX_PANE_MAP mutex poisoned during pane refresh");
            false
        }
    }
}

pub fn pane_binding(agent_id: &str) -> Option<(TmuxPaneId, String)> {
    TMUX_PANE_MAP.lock().ok().and_then(|map| {
        map.get(agent_id)
            .map(|entry| (entry.pane_id.clone(), entry.socket_name.clone()))
    })
}

pub fn init_probe_binding(agent_id: &str) -> Option<(TmuxPaneId, Arc<AtomicBool>)> {
    TMUX_PANE_MAP.lock().ok().and_then(|map| {
        map.get(agent_id)
            .map(|entry| (entry.pane_id.clone(), entry.idle_scan_enabled.clone()))
    })
}

pub fn set_idle_scan_enabled(agent_id: &str, enabled: bool) {
    match TMUX_PANE_MAP.lock() {
        Ok(map) => {
            if let Some(entry) = map.get(agent_id) {
                entry.idle_scan_enabled.store(enabled, Ordering::SeqCst);
            }
        }
        Err(err) => {
            tracing::warn!(error = %err, "TMUX_PANE_MAP mutex poisoned during idle scan toggle");
        }
    }
}

pub fn remove(agent_id: &str) -> Option<AgentIoEntry> {
    TMUX_PANE_MAP
        .lock()
        .ok()
        .and_then(|mut map| map.remove(agent_id))
}

pub fn contains(agent_id: &str) -> bool {
    TMUX_PANE_MAP
        .lock()
        .is_ok_and(|map| map.contains_key(agent_id))
}

pub fn cleanup_agent_runtime_resources(agent_id: &str, expected_session_id: Option<&str>) {
    cleanup_agent_runtime_resources_with_policy(
        agent_id,
        expected_session_id,
        RuntimeCleanupPolicy::Default,
    );
}

pub fn cleanup_agent_runtime_resources_with_policy(
    agent_id: &str,
    expected_session_id: Option<&str>,
    policy: RuntimeCleanupPolicy,
) {
    let entry = match TMUX_PANE_MAP.lock() {
        Ok(mut map) => {
            if let Some(expected_session_id) = expected_session_id
                && let Some(entry) = map.get(agent_id)
                && entry.session_id != expected_session_id
            {
                tracing::warn!(
                    agent_id,
                    expected_session_id,
                    actual_session_id = %entry.session_id,
                    "runtime registry session ownership mismatch; skipping cleanup"
                );
                return;
            }
            map.remove(agent_id)
        }
        Err(err) => {
            tracing::warn!(error = %err, "TMUX_PANE_MAP mutex poisoned during cleanup");
            None
        }
    };

    if let Some(entry) = entry {
        entry.reader_handle.abort();
        if let Err(err) = std::fs::remove_file(&entry.fifo_path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(agent_id, error = %err, "failed to remove agent fifo");
        }

        let server = crate::tmux::TmuxServer::from_socket_name(entry.socket_name.clone());
        let state_dir = state_dir_from_fifo_path(&entry.fifo_path);
        capture_pane_at_death(agent_id, &server, &entry.pane_id, state_dir.as_deref());
        let session_name = agent_session_name(agent_id);
        let should_kill_session = match entry.expected_pid {
            Some(expected_pid) => {
                if entry.pane_id.0.starts_with('%') {
                    let _ = server.kill_pane_if_owned_sync(&entry.pane_id, expected_pid);
                }
                server.kill_session_if_owned_sync(&session_name, expected_pid)
            }
            None => true,
        };
        if should_kill_session {
            if entry.expected_pid.is_none() {
                if let Err(err) = server.kill_session_sync(&session_name) {
                    tracing::warn!(agent_id, session_name = %session_name, error = %err, "failed to kill agent session during cleanup");
                } else {
                    tracing::info!(agent_id, session_name = %session_name, "killed agent session during cleanup");
                }
            }
        } else if entry.expected_pid.is_some() {
            // Fallback: expected_pid was Some, but kill_session_if_owned_sync returned false (ownership mismatch or dead).
            // Check if the session is genuinely orphaned (no live processes).
            if !has_live_process_in_session(&server, &session_name) {
                if let Err(err) = server.kill_session_sync(&session_name) {
                    tracing::warn!(agent_id, session_name = %session_name, error = %err, "failed to kill orphaned agent session during cleanup fallback");
                } else {
                    tracing::info!(agent_id, session_name = %session_name, "killed orphaned agent session during cleanup fallback");
                }
            }
        }

        if let Some(state_dir) = state_dir_from_fifo_path(&entry.fifo_path) {
            match policy {
                RuntimeCleanupPolicy::Default => {
                    crate::db::system::remove_agent_sandbox_dir_sync(
                        &state_dir,
                        &entry.session_id,
                        agent_id,
                    );
                }
                RuntimeCleanupPolicy::PreserveRecoverableCrashedHome => {
                    let sandbox_dir = state_dir
                        .join("sandboxes")
                        .join(&entry.session_id)
                        .join(agent_id);
                    if let Err(err) = std::fs::remove_dir_all(&sandbox_dir)
                        && err.kind() != std::io::ErrorKind::NotFound
                    {
                        tracing::warn!(
                            ?sandbox_dir,
                            error = %err,
                            "failed to remove preserved-home agent sandbox dir"
                        );
                    }
                }
            }
        }
    }

    if let Some(handle) = crate::marker::registry::take(agent_id) {
        let _ = handle.cancel_tx.send(());
    }
    crate::completion::registry::cancel(agent_id);
    let _ = crate::marker::parser_registry::remove(agent_id);
    let _ = crate::monitor::remove(agent_id);
}

fn state_dir_from_fifo_path(fifo_path: &std::path::Path) -> Option<std::path::PathBuf> {
    fifo_path
        .parent()
        .filter(|pipes_dir| pipes_dir.file_name().and_then(|name| name.to_str()) == Some("pipes"))
        .and_then(|pipes_dir| pipes_dir.parent())
        .map(std::path::Path::to_path_buf)
}

fn has_live_process_in_session(server: &crate::tmux::TmuxServer, session_name: &str) -> bool {
    let panes = match server.list_panes_sync(session_name) {
        Ok(panes) => panes,
        Err(err) => return live_process_assumed_on_list_error(session_name, &err),
    };
    for pane in panes {
        if let Ok(pid) = server.get_pane_pid_sync(&pane) {
            match crate::platform::sys::proc_info::kill_zero_check(pid) {
                crate::platform::sys::proc_info::ProcessLiveness::Alive
                | crate::platform::sys::proc_info::ProcessLiveness::Unknown => {
                    return true;
                }
                _ => {}
            }
        }
    }
    false
}

/// Decide liveness when `list_panes_sync` fails during the orphan-reap fallback.
///
/// A genuinely-missing target (session/pane/server gone) means there is no live process, so the
/// caller may safely reap the session by name (`false`). Any other failure — transient I/O or a
/// non-"missing" command error — is inconclusive: we assume a live process exists and skip the
/// kill (`true`), keeping the fallback fail-closed. This matches the existing seam discipline in
/// `find_pane_in_session_by_pid_sync` / `target_has_expected_pid_sync`, which never kill on an
/// ambiguous error. Collapsing every error to `false` (the prior behavior) was fail-OPEN: a
/// momentary tmux hiccup could reap a session whose panes are still alive.
fn live_process_assumed_on_list_error(session_name: &str, err: &crate::error::AhError) -> bool {
    if crate::tmux::session::tmux_target_missing(err) {
        return false;
    }
    tracing::warn!(
        session_name,
        error = %err,
        "failed to list panes for orphan check; assuming live process and skipping kill"
    );
    true
}

fn capture_pane_at_death(
    agent_id: &str,
    server: &crate::tmux::TmuxServer,
    pane_id: &TmuxPaneId,
    state_dir: Option<&std::path::Path>,
) {
    let capture = match server.capture_pane_sync(pane_id) {
        Ok(capture) => capture,
        Err(err) => {
            tracing::warn!(agent_id, pane_id = %pane_id.0, error = %err, "failed to capture pane before cleanup");
            return;
        }
    };
    let Some(state_dir) = state_dir else {
        tracing::warn!(
            agent_id,
            "state_dir unavailable; skipping pane death evidence capture"
        );
        return;
    };
    let evidence_path = state_dir
        .join("evidence")
        .join(agent_id)
        .join("pane_at_death.txt");
    if let Some(parent) = evidence_path.parent()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        tracing::warn!(agent_id, path = %parent.display(), error = %err, "failed to create pane death evidence directory");
        return;
    }
    if let Err(err) = std::fs::write(&evidence_path, capture) {
        tracing::warn!(agent_id, path = %evidence_path.display(), error = %err, "failed to write pane death evidence");
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentIoEntry, cleanup_agent_runtime_resources, contains, register};
    use crate::tmux::{TmuxPaneId, TmuxServer, agent_session_name};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[tokio::test]
    async fn test_cleanup_agent_runtime_resources_removes_fifo_and_sandbox() {
        let tmp = tempfile::TempDir::new().unwrap();
        let pipes = tmp.path().join("pipes");
        let session_id = "s_cleanup";
        let sandbox = tmp
            .path()
            .join("sandboxes")
            .join(session_id)
            .join("ag_cleanup");
        std::fs::create_dir_all(&pipes).unwrap();
        std::fs::create_dir_all(&sandbox).unwrap();
        let home_root =
            crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox).unwrap();
        std::fs::create_dir_all(home_root.join(".codex")).unwrap();
        std::fs::write(home_root.join(".codex/auth.json"), b"token").unwrap();
        let fifo_path = pipes.join("ag_cleanup.fifo");
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        register(
            "ag_cleanup".to_string(),
            AgentIoEntry {
                session_id: session_id.to_string(),
                pane_id: TmuxPaneId("%1".to_string()),
                expected_pid: None,
                reader_handle,
                fifo_path: fifo_path.clone(),
                socket_name: "missing-test-socket".to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );

        cleanup_agent_runtime_resources("ag_cleanup", Some(session_id));

        assert!(!contains("ag_cleanup"));
        assert!(!fifo_path.exists());
        assert!(!sandbox.exists());
        assert!(!home_root.exists());
    }

    #[tokio::test]
    async fn test_cleanup_agent_runtime_resources_kills_agent_session() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = TmuxServer::new(tmp.path());

        let agent_id = "ag_cleanup_session";
        let session_id = "s_cleanup_session";
        let session_name = agent_session_name(agent_id);
        let pipes = tmp.path().join("pipes");
        let sandbox = tmp.path().join("sandboxes").join(session_id).join(agent_id);
        std::fs::create_dir_all(&pipes).unwrap();
        std::fs::create_dir_all(&sandbox).unwrap();
        let fifo_path = pipes.join("ag_cleanup_session.fifo");
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        let result = (|| {
            server.ensure_session_sync(&session_name, tmp.path())?;
            register(
                agent_id.to_string(),
                AgentIoEntry {
                    session_id: session_id.to_string(),
                    pane_id: TmuxPaneId("%1".to_string()),
                    expected_pid: None,
                    reader_handle,
                    fifo_path: fifo_path.clone(),
                    socket_name: server.socket_name().to_string(),
                    idle_scan_enabled: Arc::new(AtomicBool::new(true)),
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
                .map_err(crate::tmux::error::TmuxError::from)?;
            assert!(!has_session.status.success());
            assert!(!contains(agent_id));
            assert!(!fifo_path.exists());
            assert!(!sandbox.exists());
            Ok::<(), crate::error::AhError>(())
        })();

        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
        result.unwrap();
    }

    #[tokio::test]
    async fn cleanup_agent_runtime_resources_skips_stale_pane_owner() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = TmuxServer::new(tmp.path());
        let agent_id = "ag_stale_cleanup";
        let session_id = "s_stale_cleanup";
        let session_name = agent_session_name(agent_id);
        let pipes = tmp.path().join("pipes");
        std::fs::create_dir_all(&pipes).unwrap();
        let fifo_path = pipes.join("ag_stale_cleanup.fifo");
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        let result = (|| {
            server.ensure_session_sync(&session_name, tmp.path())?;
            let live_pane = server.spawn_window_sync(
                &session_name,
                "victim",
                tmp.path(),
                &["sh", "-lc", "sleep 60"],
            )?;
            let live_pid = server.get_pane_pid_sync(&live_pane)?;
            register(
                agent_id.to_string(),
                AgentIoEntry {
                    session_id: session_id.to_string(),
                    pane_id: live_pane.clone(),
                    expected_pid: Some(i64::from(live_pid) + 100_000),
                    reader_handle,
                    fifo_path: fifo_path.clone(),
                    socket_name: server.socket_name().to_string(),
                    idle_scan_enabled: Arc::new(AtomicBool::new(true)),
                },
            );

            cleanup_agent_runtime_resources(agent_id, Some(session_id));

            assert_eq!(server.get_pane_pid_sync(&live_pane)?, live_pid);
            Ok::<(), crate::error::AhError>(())
        })();

        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
        result.unwrap();
    }

    #[tokio::test]
    async fn cleanup_agent_runtime_resources_resolves_owned_pane_before_session_kill() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = TmuxServer::new(tmp.path());
        let agent_id = "ag_reattach_multi";
        let session_id = "s_reattach_multi";
        let session_name = agent_session_name(agent_id);
        let pipes = tmp.path().join("pipes");
        std::fs::create_dir_all(&pipes).unwrap();
        let fifo_path = pipes.join("ag_reattach_multi.fifo");
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        let result = (|| {
            server.ensure_session_sync(&session_name, tmp.path())?;
            let _other_pane = server.spawn_window_sync(
                &session_name,
                "other",
                tmp.path(),
                &["sh", "-lc", "sleep 60"],
            )?;
            let owned_pane = server.spawn_window_sync(
                &session_name,
                "owned",
                tmp.path(),
                &["sh", "-lc", "sleep 60"],
            )?;
            let owned_pid = server.get_pane_pid_sync(&owned_pane)?;
            register(
                agent_id.to_string(),
                AgentIoEntry {
                    session_id: session_id.to_string(),
                    pane_id: TmuxPaneId(format!("reattached:{agent_id}")),
                    expected_pid: Some(i64::from(owned_pid)),
                    reader_handle,
                    fifo_path: fifo_path.clone(),
                    socket_name: server.socket_name().to_string(),
                    idle_scan_enabled: Arc::new(AtomicBool::new(true)),
                },
            );

            cleanup_agent_runtime_resources(agent_id, Some(session_id));

            assert!(!server.session_exists_sync(&session_name)?);
            Ok::<(), crate::error::AhError>(())
        })();

        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
        result.unwrap();
    }

    /// B2 (a5 RED): a reattached registry entry whose `expected_pid` no longer matches the
    /// session's (now-dead) pane leaks the tmux session today. `cleanup` calls
    /// `kill_session_if_owned_sync`, which finds no pane reporting `expected_pid` and returns
    /// false; because `expected_pid.is_some()`, the explicit `kill_session_sync` fallback is never
    /// reached, so the orphaned session survives.
    ///
    /// Contract: when `kill_if_owned` fails and the session is genuinely orphaned (no live process
    /// among its panes), a fallback must reap the session by name. The fallback MUST stay gated on
    /// orphan-ness so it does not regress `cleanup_agent_runtime_resources_skips_stale_pane_owner`
    /// (a session with a LIVE mismatched-pid pane must still be skipped).
    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_reaps_orphaned_session_when_expected_pid_dead_and_unmatched() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = TmuxServer::new(tmp.path());
        let agent_id = "ag_orphan_leak_b2";
        let session_id = "s_orphan_leak_b2";
        let session_name = agent_session_name(agent_id);
        let pipes = tmp.path().join("pipes");
        std::fs::create_dir_all(&pipes).unwrap();
        let fifo_path = pipes.join(format!("{agent_id}.fifo"));
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        let result = (|| {
            server.ensure_session_sync(&session_name, tmp.path())?;
            // One pane; `spawn_window_sync` sets remain-on-exit so the pane persists as [dead]
            // after we kill it, leaving an orphaned-but-existing session.
            let pane = server.spawn_window_sync(
                &session_name,
                "agent",
                tmp.path(),
                &["sh", "-lc", "exec sleep 600"],
            )?;
            let pane_pid = server.get_pane_pid_sync(&pane)?;

            // A separate, already-dead pid that does NOT match the pane's pid — models a stale
            // reattach record / pid drift after a daemon restart.
            let mut dead = std::process::Command::new("true").spawn().unwrap();
            let dead_pid = i64::from(dead.id());
            dead.wait().unwrap();
            assert_ne!(
                dead_pid,
                i64::from(pane_pid),
                "the stale expected_pid must differ from the session's pane pid"
            );

            // Kill the pane's process so the session is genuinely orphaned. We poll on the SAME
            // liveness predicate the code-under-test uses (`kill_zero_check`), which treats a
            // zombie as Dead — so this converges the instant the kernel reaps^Wmarks the process a
            // zombie, independent of tmux's own (CI-contention-prone) reap timing. Waiting on full
            // reap (`kill(pid,0) == ESRCH`) was the historical flake source (#84 pattern).
            unsafe {
                libc::kill(pane_pid, libc::SIGKILL);
            }
            use crate::platform::sys::proc_info::{ProcessLiveness, kill_zero_check};
            for _ in 0..500 {
                if kill_zero_check(pane_pid) == ProcessLiveness::Dead {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert_eq!(
                kill_zero_check(pane_pid),
                ProcessLiveness::Dead,
                "pane process must be dead-or-zombie so the session is orphaned \
                 (poll on the fallback's own kill_zero_check predicate, not full reap)"
            );
            assert!(
                server.session_exists_sync(&session_name)?,
                "orphaned session should still exist before cleanup"
            );

            register(
                agent_id.to_string(),
                AgentIoEntry {
                    session_id: session_id.to_string(),
                    // Non-`%` reattach placeholder → `kill_pane_if_owned_sync` is skipped, matching
                    // the real reattach registration path (db::system reattach).
                    pane_id: TmuxPaneId(format!("reattached:{agent_id}")),
                    expected_pid: Some(dead_pid),
                    reader_handle,
                    fifo_path: fifo_path.clone(),
                    socket_name: server.socket_name().to_string(),
                    idle_scan_enabled: Arc::new(AtomicBool::new(true)),
                },
            );

            cleanup_agent_runtime_resources(agent_id, Some(session_id));

            // RED today: the orphaned session leaks (kill_if_owned failed, no fallback).
            assert!(
                !server.session_exists_sync(&session_name)?,
                "orphaned tmux session must be reaped by fallback cleanup (B2 leak)"
            );
            Ok::<(), crate::error::AhError>(())
        })();

        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
        result.unwrap();
    }

    /// B2 hardening (a5): when `list_panes_sync` fails inside the orphan-reap fallback, the
    /// liveness decision must stay fail-closed. Only a genuine "target missing" error (session /
    /// pane / server gone) counts as "no live process" and thus reapable; every other error is
    /// inconclusive and must be treated as "assume live, skip the kill".
    ///
    /// This is a contract test of the decision boundary — not the tmux wire format — so it feeds
    /// `live_process_assumed_on_list_error` constructed errors rather than depending on a specific
    /// tmux version's stderr wording (which is why it is a focused unit test, not an integration
    /// test: a real running session never yields a *non-missing* `list-panes` failure on demand).
    ///
    /// Rollback self-check: the prior code collapsed every list error to `return false`
    /// (fail-OPEN). Reverting the fix would make the transient-error assertion below go RED,
    /// because the decision would flip to `false` (reap) instead of `true` (skip).
    #[test]
    fn list_panes_error_is_fail_closed_unless_target_missing() {
        use crate::error::AhError;

        // Genuinely-missing target: session/server gone => no live process => reap is allowed.
        let missing = AhError::TmuxCommandFailed {
            cmd: "tmux list-panes".to_string(),
            stderr: "can't find session: ah-ag_x".to_string(),
            exit: 1,
        };
        assert!(
            !super::live_process_assumed_on_list_error("ah-ag_x", &missing),
            "a genuine target-missing list error must read as 'no live process' (reapable)"
        );

        // Non-"missing" transient failure (e.g. interrupted I/O): inconclusive => assume a live
        // process exists => skip the kill (fail-closed).
        let transient = AhError::TmuxCommandFailed {
            cmd: "tmux list-panes".to_string(),
            stderr: "read error: Interrupted system call".to_string(),
            exit: 1,
        };
        assert!(
            super::live_process_assumed_on_list_error("ah-ag_x", &transient),
            "a non-missing list error must be fail-closed: assume live and skip the kill"
        );
    }
}
