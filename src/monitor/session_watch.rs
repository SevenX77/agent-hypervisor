use crate::db::master_recovery::{AnchorCascadeDecision, decide_anchor_cascade_sync};
use crate::db::{self as db, Db};
use rusqlite::OptionalExtension;
#[cfg(unix)]
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

const WATCH_INTERVAL: Duration = Duration::from_secs(3);
const DEBOUNCE_RETRIES: u32 = 2;
const DEBOUNCE_RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub fn unit_name_for_session(session_id: &str) -> String {
    format!("ahd-session-{session_id}.service")
}

pub fn spawn_session_watch_task(session_id: String, unit_name: String, db: Arc<Db>) {
    let key = format!("anchor:{session_id}");
    match placeholder_fd() {
        Ok(fd) => crate::monitor::register(key.clone(), fd),
        Err(err) => tracing::warn!(
            session_id = %session_id,
            error = %err,
            "failed to register anchor monitor placeholder"
        ),
    }

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(WATCH_INTERVAL);
        loop {
            interval.tick().await;
            if unit_is_active(&unit_name) {
                continue;
            }

            let mut confirmed = true;
            for retry_idx in 0..DEBOUNCE_RETRIES {
                tokio::time::sleep(DEBOUNCE_RETRY_INTERVAL).await;
                if unit_is_active(&unit_name) {
                    tracing::info!(
                        session_id = %session_id,
                        unit = %unit_name,
                        retry = retry_idx + 1,
                        "anchor unit recovered after transient inactive; debounce absorbed"
                    );
                    confirmed = false;
                    break;
                }
            }
            if !confirmed {
                continue;
            }

            if handle_confirmed_anchor_inactive(db.clone(), &session_id, &unit_name, &key).await {
                break;
            }
        }
    });
}

async fn handle_confirmed_anchor_inactive(
    db: Arc<Db>,
    session_id: &str,
    unit_name: &str,
    monitor_key: &str,
) -> bool {
    let now = unixepoch();
    let decision = {
        let conn = db.conn();
        decide_anchor_cascade_sync(&conn, session_id, now)
    };
    let reason = match decision {
        Ok(AnchorCascadeDecision::Defer { until, phase }) => {
            tracing::info!(
                session_id,
                unit = %unit_name,
                phase,
                until,
                now,
                "anchor unit inactive but master recovery window is active; deferring cascade"
            );
            return false;
        }
        Ok(AnchorCascadeDecision::CascadeNow { reason }) => {
            if reason == "MASTER_RECOVERY_COMPLETED"
                && completed_recovery_master_is_alive(&db, session_id)
            {
                tracing::info!(
                    session_id,
                    unit = %unit_name,
                    reason,
                    "completed-window but master alive; re-arming anchor watch instead of cascading"
                );
                return false;
            }
            reason.to_string()
        }
        Err(err) => {
            tracing::warn!(
                session_id,
                unit = %unit_name,
                error = %err,
                "master recovery cascade decision failed; cascading conservatively"
            );
            "ANCHOR_UNIT_STOPPED".to_string()
        }
    };

    tracing::warn!(
        session_id,
        unit = %unit_name,
        reason,
        "anchor unit confirmed inactive after debounce; cascading agent kill"
    );
    if let Err(err) =
        db::system::cascade_kill_session_agents((*db).clone(), session_id.to_string(), reason).await
    {
        tracing::warn!(
            session_id,
            unit = %unit_name,
            error = %err,
            "failed to sync killed session after anchor stopped"
        );
    }
    let _ = crate::monitor::remove(monitor_key);
    true
}

fn completed_recovery_master_is_alive(db: &Db, session_id: &str) -> bool {
    let master_pid = {
        let conn = db.conn();
        conn.query_row(
            "SELECT master_pid FROM sessions WHERE id = ?1 AND status = 'ACTIVE'",
            [session_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
    };
    match master_pid {
        Ok(Some(master_pid)) => crate::monitor::master_watch::master_process_is_alive(master_pid),
        Ok(None) => false,
        Err(err) => {
            tracing::warn!(
                session_id,
                error = %err,
                "failed to query master pid for completed recovery cascade guard"
            );
            false
        }
    }
}

fn unixepoch() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

fn unit_is_active(unit_name: &str) -> bool {
    unit_is_active_with_program(unit_name, Path::new("systemctl"))
}

fn unit_is_active_with_program(unit_name: &str, program: &Path) -> bool {
    match Command::new(program)
        .args(["--user", "is-active", unit_name])
        .output()
    {
        Ok(output) => unit_is_active_from_stdout(&String::from_utf8_lossy(&output.stdout)),
        Err(err) => {
            tracing::warn!(
                unit = %unit_name,
                error = %err,
                "systemctl is-active failed; treating unit as alive"
            );
            true
        }
    }
}

fn unit_is_active_from_stdout(stdout: &str) -> bool {
    let status = stdout.trim();
    if matches!(status, "inactive" | "failed") {
        return false;
    }
    if status != "active" {
        tracing::debug!(status, "ambiguous unit state; treating unit as alive");
    }
    true
}

fn placeholder_fd() -> std::io::Result<crate::monitor::MonitorHandle> {
    #[cfg(windows)]
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Windows monitor placeholder is not implemented until M1",
        ))
    }

    #[cfg(unix)]
    {
        // The pidfd registry is also used as a lightweight monitor registry for
        // diagnostics. Anchor watchers do not have a process fd, so /dev/null is
        // registered as a cheap owned placeholder and closed on remove().
        File::open("/dev/null").map(crate::monitor::MonitorHandle::from)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        handle_confirmed_anchor_inactive, unit_is_active_from_stdout, unit_is_active_with_program,
        unit_name_for_session,
    };
    use crate::agent_io::registry::{AgentIoEntry, contains, register};
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::insert_job_sync;
    use crate::db::master_recovery::{
        begin_master_recovery_window_sync, complete_master_recovery_window_sync,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::home_materialization::sandbox_home_for_sandbox_dir;
    use crate::tmux::TmuxPaneId;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn test_unit_name_for_session() {
        assert_eq!(
            unit_name_for_session("sess_abc"),
            "ahd-session-sess_abc.service"
        );
    }

    #[test]
    fn test_unit_is_active_treats_inactive_as_dead() {
        assert!(!unit_is_active_from_stdout("inactive\n"));
        assert!(!unit_is_active_from_stdout("failed\n"));
    }

    #[test]
    fn test_unit_is_active_treats_unknown_status_as_alive() {
        assert!(unit_is_active_from_stdout("activating\n"));
        assert!(unit_is_active_from_stdout("unknown\n"));
        assert!(unit_is_active_from_stdout(""));
    }

    #[test]
    fn test_unit_is_active_treats_command_error_as_alive() {
        assert!(unit_is_active_with_program(
            "ahd-session-missing.service",
            Path::new("/definitely/not/a/systemctl")
        ));
    }

    struct SessionWatchFixture {
        db: Arc<crate::db::Db>,
        state_dir: tempfile::TempDir,
        session_id: &'static str,
        agent_id: &'static str,
        home_root: std::path::PathBuf,
    }

    fn session_watch_fixture(
        session_id: &'static str,
        agent_id: &'static str,
        agent_state: &str,
        with_dispatched_job: bool,
    ) -> SessionWatchFixture {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(db_file.path()).unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let project_dir = tempfile::TempDir::new().unwrap();
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                session_id,
                "p_session_watch_race",
                project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'ACTIVE', master_pid = 111, master_generation = 5
                 WHERE id = ?1",
                [session_id],
            )
            .unwrap();
            insert_agent_sync(&conn, agent_id, session_id, "codex", agent_state, Some(123))
                .unwrap();
            if with_dispatched_job {
                insert_job_sync(&conn, "job_session_watch", agent_id, None, "in flight").unwrap();
                conn.execute(
                    "UPDATE jobs
                     SET status = 'DISPATCHED', dispatched_at = unixepoch()
                     WHERE id = 'job_session_watch'",
                    [],
                )
                .unwrap();
            }
        }

        let pipes_dir = state_dir.path().join("pipes");
        let sandbox_dir = state_dir
            .path()
            .join("sandboxes")
            .join(session_id)
            .join(agent_id);
        std::fs::create_dir_all(&pipes_dir).unwrap();
        std::fs::create_dir_all(&sandbox_dir).unwrap();
        let home_root = sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        std::fs::create_dir_all(home_root.join(".codex")).unwrap();
        std::fs::write(home_root.join(".codex/auth.json"), b"token").unwrap();
        let fifo_path = pipes_dir.join(format!("{agent_id}.fifo"));
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        register(
            agent_id.to_string(),
            AgentIoEntry {
                session_id: session_id.to_string(),
                pane_id: TmuxPaneId("%race:1.0".to_string()),
                expected_pid: None,
                reader_handle,
                fifo_path: fifo_path.clone(),
                socket_name: "missing-session-watch-race-socket".to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );
        SessionWatchFixture {
            db: Arc::new(db),
            state_dir,
            session_id,
            agent_id,
            home_root,
        }
    }

    fn query_agent_state(db: &crate::db::Db, agent_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn query_job_error_reason(db: &crate::db::Db) -> Option<String> {
        db.conn()
            .query_row(
                "SELECT error_reason FROM jobs WHERE id = 'job_session_watch'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn query_master_recovery_phase(db: &crate::db::Db, session_id: &str) -> Option<String> {
        db.conn()
            .query_row(
                "SELECT phase FROM master_recovery_windows WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .ok()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_defers_for_unexpired_activework_window() {
        let fixture = session_watch_fixture(
            "s_session_watch_window",
            "a_session_watch_window",
            "BUSY",
            true,
        );
        {
            let conn = fixture.db.conn();
            begin_master_recovery_window_sync(
                &conn,
                fixture.session_id,
                111,
                5,
                true,
                super::unixepoch(),
                90,
            )
            .unwrap();
        }
        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_window.service",
            "anchor:s_session_watch_window",
        )
        .await;

        assert!(
            !cascaded,
            "anchor-inactive cascade must defer while an active-work recovery window is open"
        );
        assert!(contains(fixture.agent_id));
        assert!(
            fixture.home_root.exists(),
            "revive deferral must preserve worker home"
        );
        assert_eq!(query_agent_state(&fixture.db, fixture.agent_id), "BUSY");
        drop(fixture.state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_cascades_expired_window_with_expired_reason() {
        let fixture = session_watch_fixture(
            "s_session_watch_expired",
            "a_session_watch_expired",
            "BUSY",
            true,
        );
        {
            let conn = fixture.db.conn();
            begin_master_recovery_window_sync(&conn, fixture.session_id, 111, 5, true, 1, 10)
                .unwrap();
        }
        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_expired.service",
            "anchor:s_session_watch_expired",
        )
        .await;

        assert!(
            cascaded,
            "expired master recovery window should allow anchor cascade"
        );
        assert!(!contains(fixture.agent_id));
        assert!(
            !fixture.home_root.exists(),
            "legal anchor cascade must still run the real runtime cleanup path"
        );
        assert_eq!(query_agent_state(&fixture.db, fixture.agent_id), "KILLED");
        assert_eq!(
            query_job_error_reason(&fixture.db).as_deref(),
            Some("MASTER_REVIVE_WINDOW_EXPIRED")
        );
        assert_eq!(
            query_master_recovery_phase(&fixture.db, fixture.session_id).as_deref(),
            Some("FAILED")
        );
        drop(fixture.state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_creates_detected_window_for_toctou_active_work_and_defers() {
        let fixture = session_watch_fixture(
            "s_session_watch_toctou",
            "a_session_watch_toctou",
            "BUSY",
            true,
        );
        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_toctou.service",
            "anchor:s_session_watch_toctou",
        )
        .await;

        assert!(
            !cascaded,
            "active work with no existing window should create a DETECTED window and defer"
        );
        assert_eq!(
            query_master_recovery_phase(&fixture.db, fixture.session_id).as_deref(),
            Some("DETECTED")
        );
        assert!(fixture.home_root.exists());
        assert_eq!(query_agent_state(&fixture.db, fixture.agent_id), "BUSY");
        drop(fixture.state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_completed_window_with_live_master_does_not_cascade() {
        let fixture = session_watch_fixture(
            "s_session_watch_completed_live",
            "a_session_watch_completed_live",
            "BUSY",
            true,
        );
        fixture
            .db
            .conn()
            .execute(
                "UPDATE sessions SET master_pid = ?2 WHERE id = ?1",
                rusqlite::params![fixture.session_id, i64::from(std::process::id())],
            )
            .unwrap();
        {
            let conn = fixture.db.conn();
            begin_master_recovery_window_sync(
                &conn,
                fixture.session_id,
                i64::from(std::process::id()),
                5,
                true,
                1_000,
                90,
            )
            .unwrap();
            complete_master_recovery_window_sync(&conn, fixture.session_id, 5, 1_001).unwrap();
        }

        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_completed_live.service",
            "anchor:s_session_watch_completed_live",
        )
        .await;

        assert!(
            !cascaded,
            "completed recovery with a live master should re-arm instead of cascading"
        );
        assert!(contains(fixture.agent_id));
        assert!(fixture.home_root.exists());
        assert_eq!(query_agent_state(&fixture.db, fixture.agent_id), "BUSY");
        assert_eq!(
            query_master_recovery_phase(&fixture.db, fixture.session_id).as_deref(),
            Some("COMPLETED")
        );
        drop(fixture.state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_completed_window_with_dead_master_cascades() {
        let fixture = session_watch_fixture(
            "s_session_watch_completed_dead",
            "a_session_watch_completed_dead",
            "BUSY",
            true,
        );
        let dead_pid = i64::from(i32::MAX);
        fixture
            .db
            .conn()
            .execute(
                "UPDATE sessions SET master_pid = ?2 WHERE id = ?1",
                rusqlite::params![fixture.session_id, dead_pid],
            )
            .unwrap();
        {
            let conn = fixture.db.conn();
            begin_master_recovery_window_sync(
                &conn,
                fixture.session_id,
                dead_pid,
                5,
                true,
                1_000,
                90,
            )
            .unwrap();
            complete_master_recovery_window_sync(&conn, fixture.session_id, 5, 1_001).unwrap();
        }

        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_completed_dead.service",
            "anchor:s_session_watch_completed_dead",
        )
        .await;

        assert!(
            cascaded,
            "completed recovery with a dead master should still cascade"
        );
        assert!(!contains(fixture.agent_id));
        assert!(!fixture.home_root.exists());
        assert_eq!(query_agent_state(&fixture.db, fixture.agent_id), "KILLED");
        assert_eq!(
            query_job_error_reason(&fixture.db).as_deref(),
            Some("MASTER_RECOVERY_COMPLETED")
        );
        drop(fixture.state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_cascades_no_window_idle_no_work() {
        let fixture = session_watch_fixture(
            "s_session_watch_idle",
            "a_session_watch_idle",
            "IDLE",
            false,
        );
        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_idle.service",
            "anchor:s_session_watch_idle",
        )
        .await;

        assert!(cascaded);
        assert!(!contains(fixture.agent_id));
        assert!(!fixture.home_root.exists());
        assert_eq!(query_agent_state(&fixture.db, fixture.agent_id), "KILLED");
        assert!(query_master_recovery_phase(&fixture.db, fixture.session_id).is_none());
        drop(fixture.state_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_watch_cascades_non_active_session() {
        let fixture = session_watch_fixture(
            "s_session_watch_non_active",
            "a_session_watch_non_active",
            "BUSY",
            true,
        );
        fixture
            .db
            .conn()
            .execute(
                "UPDATE sessions SET status = 'FAILED' WHERE id = ?1",
                [fixture.session_id],
            )
            .unwrap();

        let cascaded = handle_confirmed_anchor_inactive(
            fixture.db.clone(),
            fixture.session_id,
            "ahd-session-s_session_watch_non_active.service",
            "anchor:s_session_watch_non_active",
        )
        .await;

        assert!(cascaded);
        assert_eq!(
            query_agent_state(&fixture.db, fixture.agent_id),
            "KILLED",
            "CascadeNow(SESSION_NOT_ACTIVE) should still route through the existing cascade cleanup path"
        );
        assert!(!fixture.home_root.exists());
        assert_eq!(
            query_job_error_reason(&fixture.db).as_deref(),
            Some("SESSION_NOT_ACTIVE")
        );
        drop(fixture.state_dir);
    }
}
