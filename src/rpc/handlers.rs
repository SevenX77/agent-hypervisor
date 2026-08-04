mod ack;
mod agent;
mod events;
mod evidence;
mod jobs;
mod master_cutover;
mod params;
mod prompt;
mod realign;
mod runtime;
mod sessions;
mod system;

#[allow(unused_imports)]
pub(crate) use ack::{
    ACK_IDLE_SCAN_REOPEN_DELAY_MS, CAPTURE_SEED_POLL_MS, CAPTURE_SEED_STABILITY_MS,
    spawn_new_capture_seed,
};
pub use ack::{
    AckBusyOutcome, ack_mark_busy_or_resolve, fallback_ack_to_crashed, fallback_ack_to_stuck,
};
pub use agent::{
    handle_agent_kill, handle_agent_notify, handle_agent_read, handle_agent_send,
    handle_agent_spawn, handle_agent_watch,
};
pub use events::{handle_event_subscribe, stream_event_subscribe};
pub use evidence::{
    handle_agent_assert_state, handle_agent_discard_evidence, handle_evidence_insert,
    handle_job_has_evidence, handle_job_mark_requires_evidence,
};
pub use jobs::{handle_job_cancel, handle_job_submit, handle_job_wait};
pub use master_cutover::{
    handle_master_ack_ready, handle_master_tell_begin, handle_master_tell_failed,
    handle_session_master_cutover,
};
pub use prompt::{handle_agent_learn_rule, handle_agent_resolve_prompt};
pub(crate) use realign::{RealignAgentParams, spawn_realign_agent};
pub use realign::{handle_agent_realign, handle_session_realign};
pub use runtime::{handle_runtime_snapshot, stream_runtime_subscribe};
pub use sessions::{
    handle_session_create, handle_session_kill, handle_session_list,
    handle_session_spawn_master_pane,
};
pub use system::{handle_system_dump, handle_system_shutdown};

#[cfg(all(test, unix))]
mod tests {
    use super::ack::capture_seed_matches;
    use super::events::stuck_frame_for_filter;
    use super::params::should_press_enter_after_paste;
    use super::{
        CAPTURE_SEED_POLL_MS, CAPTURE_SEED_STABILITY_MS, handle_agent_assert_state,
        handle_agent_discard_evidence, handle_agent_kill, handle_agent_read, handle_agent_send,
        handle_agent_spawn, handle_agent_watch, handle_event_subscribe, handle_job_submit,
        handle_job_wait, handle_session_create, handle_session_kill,
        handle_session_spawn_master_pane, handle_system_dump, stream_event_subscribe,
    };
    use crate::db;
    use crate::db::agents::{insert_agent_sync, query_agent_state_sync, update_agent_state_sync};
    use crate::db::events::{UNKNOWN_PATTERN_STABLE, insert_event_and_notify, insert_event_sync};
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::mark_agent_unknown_sync;
    use crate::error::CcbdError;
    use crate::marker::MarkerMatcher;
    use crate::provider::manifest::{
        CompletionSignalKind, IdleDetectionMode, InitProbeKind, ProviderManifest,
    };
    use crate::rpc::Ctx;
    use crate::sandbox::EnvState;
    use crate::tmux::TmuxServer;
    use serde_json::{Value, json};
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;
    use uuid::Uuid;

    static TMUX_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn test_ctx() -> Ctx {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        Ctx {
            db: db::init(file.path()).unwrap(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(&state_dir)),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        }
    }

    fn seed_claude_credentials(home: &std::path::Path) {
        let path = home.join(".claude/.credentials.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            json!({
                "claudeAiOauth": {
                    "accessToken": "seed-access",
                    "refreshToken": "seed-refresh",
                    "expiresAt": 4_102_444_800_000_i64
                }
            })
            .to_string(),
        )
        .unwrap();
    }

    async fn sleep_ms(ms: u64) {
        tokio::task::spawn_blocking(move || std::thread::sleep(Duration::from_millis(ms)))
            .await
            .unwrap();
    }

    fn command_count(ctx: &Ctx, agent_id: &str) -> i64 {
        let conn = ctx.db.conn();
        conn.query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'command_received'",
            [agent_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn seed_session_agent(ctx: &Ctx, agent_id: &str) {
        let conn = ctx.db.conn();
        insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
        insert_agent_sync(&conn, agent_id, "s1", "bash", "BUSY", Some(123)).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stuck_frame_for_filter_finds_latest_stuck_state_change() {
        let ctx = test_ctx();
        seed_session_agent(&ctx, "a1");
        let conn = ctx.db.conn();
        let first_stuck = insert_event_sync(
            &conn,
            "a1",
            None,
            "state_change",
            r#"{"to":"STUCK","job_id":"j1","signal_kinds":["state_machine"],"elapsed_secs":5}"#,
        )
        .unwrap();
        insert_event_sync(
            &conn,
            "a1",
            None,
            "state_change",
            r#"{"to":"IDLE","job_id":"j1"}"#,
        )
        .unwrap();
        let latest_stuck = insert_event_sync(
            &conn,
            "a1",
            None,
            "state_change",
            r#"{"to":"STUCK","job_id":"j2","signal_kinds":["health:completion"],"elapsed_secs":7}"#,
        )
        .unwrap();
        // Release the single-connection MutexGuard before awaiting
        // stuck_frame_for_filter, which re-locks the same Mutex internally
        // (query_last_event_of_type_matching_payload -> ctx.db.conn()).
        // Holding the guard across the await self-deadlocks.
        drop(conn);

        let frame = stuck_frame_for_filter(&ctx, &json!({"agent_id":"a1"}))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame["event_id"], latest_stuck);
        assert_eq!(frame["job_id"], "j2");

        let filtered = stuck_frame_for_filter(&ctx, &json!({"agent_id":"a1","job_id":"j1"}))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(filtered["event_id"], first_stuck);
        assert_eq!(filtered["job_id"], "j1");
    }

    #[test]
    fn test_antigravity_paste_enter_decision_is_provider_based() {
        assert!(!should_press_enter_after_paste("antigravity", "foo\n"));
        assert!(should_press_enter_after_paste("antigravity", "foo"));
        assert!(should_press_enter_after_paste("codex", "foo\n"));
    }

    #[test]
    fn test_observed_stability_capture_seed_never_marks_direct_idle() {
        let manifest = ProviderManifest {
            provider_name: "fake-observed",
            auth_mount_paths: vec![],
            command: &["fake"],
            resume_args: &[],
            env_passthrough: &[],
            injected_env_vars: &[],
            readiness_timeout_s: 1,
            requires_home_materialization: false,
            init_probe: InitProbeKind::Bash,
            idle_detection_mode: IdleDetectionMode::ObservedStability,
            stability_ms: 300,
            idle_anti_pattern: "",
            completion_signal: CompletionSignalKind::LogOnly,
            capabilities: crate::provider::manifest::ProviderCapabilities {
                rules_target: false,
                completion_signal: false,
                readiness_ack: false,
                bundles: false,
                settings: false,
            },
        };
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let mut parser = vt100::Parser::new(200, 200, 0);
        parser.process(b"old screen\nREADY_PROMPT\nnew output\n");

        assert!(!capture_seed_matches(&parser, &matcher));
    }

    #[test]
    fn test_capture_seed_does_not_match_historical_prompt() {
        let matcher = MarkerMatcher::default();
        let mut parser = vt100::Parser::new(200, 200, 0);
        parser.process(b"$ first command\noutput\n$ sleep 3\n");

        assert!(!capture_seed_matches(&parser, &matcher));
    }

    #[test]
    fn test_capture_seed_poll_and_stability_windows() {
        assert_eq!(CAPTURE_SEED_POLL_MS, 50);
        assert_eq!(CAPTURE_SEED_STABILITY_MS, 500);
    }

    async fn wait_for_state(ctx: &Ctx, agent_id: &str, expected: &str) {
        let timeout = Duration::from_secs(15);
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            let state = query_agent_state_sync(&ctx.db, agent_id)
                .unwrap()
                .map(|(state, _)| state);
            if state.as_deref() == Some(expected) {
                return;
            }
            sleep_ms(100).await;
        }
        panic!("agent {agent_id} did not reach {expected} within {timeout:?}");
    }

    async fn cleanup_agent(ctx: &Ctx, agent_id: &str) {
        let _ = handle_agent_kill(json!({ "agent_id": agent_id }), ctx).await;
        sleep_ms(300).await;
    }

    fn seed_unknown_with_evidence(ctx: &Ctx, agent_id: &str) -> String {
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_evidence", "p_evidence", "/tmp/t4-evidence").unwrap();
            insert_agent_sync(&conn, agent_id, "s_evidence", "bash", "BUSY", Some(1)).unwrap();
        }
        mark_agent_unknown_sync(
            &ctx.db,
            agent_id,
            "PTY_MARKER_TIMEOUT",
            b"pane".to_vec(),
            serde_json::json!(["rule"]),
        )
        .unwrap();
        ctx.db
            .conn()
            .query_row(
                "SELECT id FROM evidence WHERE agent_id = ?",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn restore_env(key: &str, old_value: Option<std::ffi::OsString>) {
        unsafe {
            if let Some(old_value) = old_value {
                std::env::set_var(key, old_value);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    struct EnvGuard {
        key: &'static str,
        old_value: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let old_value = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old_value }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            restore_env(self.key, self.old_value.clone());
        }
    }

    fn insert_session_with_temp_dir(
        ctx: &Ctx,
        session_id: &str,
        project_id: &str,
    ) -> tempfile::TempDir {
        let project_dir = tempfile::TempDir::new().unwrap();
        let conn = ctx.db.conn();
        insert_session_sync(
            &conn,
            session_id,
            project_id,
            project_dir.path().to_string_lossy().as_ref(),
        )
        .unwrap();
        project_dir
    }

    fn seed_two_idle_agents(ctx: &Ctx) {
        let conn = ctx.db.conn();
        insert_session_sync(&conn, "s_events", "p_events", "/tmp/t4-events").unwrap();
        insert_agent_sync(&conn, "ag_events_a", "s_events", "bash", "IDLE", Some(1)).unwrap();
        insert_agent_sync(&conn, "ag_events_b", "s_events", "bash", "IDLE", Some(2)).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_event_subscribe_backfills_global_unknown_pattern_with_agent_filter() {
        let ctx = test_ctx();
        seed_two_idle_agents(&ctx);
        insert_event_and_notify(
            ctx.db.clone(),
            "ag_events_a".to_string(),
            None,
            UNKNOWN_PATTERN_STABLE.to_string(),
            r#"{"category_hint":"StartupReadiness"}"#.to_string(),
        )
        .await
        .unwrap();
        insert_event_and_notify(
            ctx.db.clone(),
            "ag_events_b".to_string(),
            None,
            UNKNOWN_PATTERN_STABLE.to_string(),
            r#"{"category_hint":"RuntimeMarker"}"#.to_string(),
        )
        .await
        .unwrap();

        let frame = handle_event_subscribe(
            json!({
                "kind": "unknown_pattern",
                "agent_id": "ag_events_b",
                "since_seq_id": 0
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(frame["kind"], "unknown_pattern");
        assert_eq!(frame["agent_id"], "ag_events_b");
        assert_eq!(frame["job_id"], Value::Null);
        assert_eq!(frame["payload"]["category_hint"], "RuntimeMarker");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_event_subscribe_backfills_without_job_id() {
        let ctx = test_ctx();
        seed_two_idle_agents(&ctx);
        let inserted = insert_event_and_notify(
            ctx.db.clone(),
            "ag_events_a".to_string(),
            None,
            UNKNOWN_PATTERN_STABLE.to_string(),
            r#"{"category_hint":"StartupReadiness"}"#.to_string(),
        )
        .await
        .unwrap();
        let mut out = Vec::new();

        stream_event_subscribe(
            json!({
                "kind": ["unknown_pattern"],
                "since_seq_id": inserted.event_id - 1
            }),
            &ctx,
            &mut out,
        )
        .await
        .unwrap();
        let line = String::from_utf8(out).unwrap();
        let frame: Value = serde_json::from_str(line.trim()).unwrap();

        assert_eq!(frame["event_id"], inserted.event_id);
        assert_eq!(frame["kind"], "unknown_pattern");
        assert_eq!(frame["agent_id"], "ag_events_a");
    }

    fn tmux_pane_current_path(ctx: &Ctx, pane_id: &str) -> String {
        let output = Command::new("tmux")
            .args([
                "-L",
                ctx.tmux_server.socket_name(),
                "display-message",
                "-p",
                "-t",
                pane_id,
                "#{pane_current_path}",
            ])
            .output()
            .expect("tmux display-message should run");
        assert!(
            output.status.success(),
            "tmux display-message failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    async fn wait_for_file(path: &std::path::Path, timeout: Duration) -> String {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if let Ok(data) = std::fs::read_to_string(path)
                && !data.is_empty()
            {
                return data;
            }
            sleep_ms(100).await;
        }
        panic!("{} was not written within {timeout:?}", path.display());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_session_create_returns_session_id() {
        let ctx = test_ctx();
        let result = handle_session_create(
            json!({
                "project_id": "p1",
                "absolute_path": "/tmp/t4-session",
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert!(result["session_id"].as_str().unwrap().starts_with("sess_"));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(global_env)]
    async fn test_handle_session_create_no_anchor_unit() {
        let mut ctx = test_ctx();
        ctx.env_state.systemd_run_available = true;
        ctx.env_state.unsafe_no_sandbox = false;
        let bin_dir = tempfile::TempDir::new().unwrap();
        let log_file = tempfile::NamedTempFile::new().unwrap();
        let systemd_run = bin_dir.path().join("systemd-run");
        std::fs::write(
            &systemd_run,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
                log_file.path().display()
            ),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&systemd_run).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&systemd_run, perms).unwrap();
        let old_path = std::env::var_os("PATH");
        let new_path = format!(
            "{}:{}",
            bin_dir.path().display(),
            old_path
                .as_deref()
                .map(|path| path.to_string_lossy())
                .unwrap_or_default()
        );
        unsafe {
            std::env::set_var("PATH", new_path);
        }

        let result = handle_session_create(
            json!({
                "project_id": "p1",
                "absolute_path": "/tmp/t4-session-no-anchor",
            }),
            &ctx,
        )
        .await
        .unwrap();

        restore_env("PATH", old_path);
        assert!(result["session_id"].as_str().unwrap().starts_with("sess_"));
        let log = std::fs::read_to_string(log_file.path()).unwrap_or_default();
        assert!(
            !log.contains("ahd-session-"),
            "session.create should not create systemd anchor unit, log: {log}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_session_spawn_master_pane_returns_pane_id() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let master_dir = tempfile::TempDir::new().unwrap();
        let master_path = master_dir.path().to_string_lossy().to_string();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_master", "p_master", &master_path).unwrap();
        }

        let result = handle_session_spawn_master_pane(
            json!({
                "session_id": "s_master",
                "cmd": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert!(result["pane_id"].as_str().unwrap().starts_with('%'));
        let pane_path = Command::new("tmux")
            .args([
                "-L",
                ctx.tmux_server.socket_name(),
                "display-message",
                "-p",
                "-t",
                result["pane_id"].as_str().unwrap(),
                "#{pane_current_path}",
            ])
            .output()
            .expect("tmux display-message should run");
        assert!(
            pane_path.status.success(),
            "tmux display-message failed: {}",
            String::from_utf8_lossy(&pane_path.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&pane_path.stdout).trim(),
            master_path
        );
        let stored: String = ctx
            .db
            .conn()
            .query_row(
                "SELECT master_pane_id FROM sessions WHERE id = 's_master'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, result["pane_id"].as_str().unwrap());
        let _ = Command::new("tmux")
            .args(["-L", ctx.tmux_server.socket_name(), "kill-server"])
            .output();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(global_env)]
    async fn test_handle_session_spawn_master_pane_uses_isolated_claude_home() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let host_home = tempfile::TempDir::new().unwrap();
        let cache_home = tempfile::TempDir::new().unwrap();
        let project_dir = tempfile::TempDir::new().unwrap();
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        let env_file = tempfile::NamedTempFile::new().unwrap();
        let env_path = env_file.path().to_path_buf();
        seed_claude_credentials(host_home.path());
        let _home_guard = EnvGuard::set("HOME", host_home.path());
        let _cache_guard = EnvGuard::set("XDG_CACHE_HOME", cache_home.path());
        let mut ctx = test_ctx();
        ctx.env_state.unsafe_no_sandbox = false;
        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "s_master_isolated",
                "p_master_isolated",
                project_dir.path().to_string_lossy().as_ref(),
            )
            .unwrap();
        }

        let cmd = format!(
            "printf 'HOME=%s\\nCLAUDE_CONFIG_DIR=%s\\n' \"$HOME\" \"$CLAUDE_CONFIG_DIR\" > {}; sleep 30",
            env_path.display()
        );
        let result = handle_session_spawn_master_pane(
            json!({
                "session_id": "s_master_isolated",
                "cmd": cmd,
                "claude_shared_credentials_dir": shared_credentials_dir.path().display().to_string(),
            }),
            &ctx,
        )
        .await
        .unwrap();
        let env_dump = wait_for_file(&env_path, Duration::from_secs(5)).await;
        let _ = handle_session_kill(json!({ "session_id": "s_master_isolated" }), &ctx).await;
        let _ = Command::new("tmux")
            .args(["-L", ctx.tmux_server.socket_name(), "kill-server"])
            .output();

        assert!(result["pane_id"].as_str().unwrap().starts_with('%'));
        let home = env_dump
            .lines()
            .find_map(|line| line.strip_prefix("HOME="))
            .unwrap();
        let claude_config_dir = env_dump
            .lines()
            .find_map(|line| line.strip_prefix("CLAUDE_CONFIG_DIR="))
            .unwrap();
        assert_ne!(home, host_home.path().to_string_lossy());
        assert!(
            home.starts_with(&cache_home.path().join("ah/sandboxes").display().to_string()),
            "master HOME must be an ah sandbox, env dump: {env_dump}"
        );
        assert_eq!(
            claude_config_dir,
            format!("{home}/.claude"),
            "master CLAUDE_CONFIG_DIR must point at its sandbox .claude, env dump: {env_dump}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_session_kill_cleans_up_master_pane() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let session_name = format!("test-master-{}", Uuid::new_v4());
        ctx.tmux_server
            .ensure_session(session_name.clone(), std::env::temp_dir())
            .await
            .unwrap();
        let pane_id = ctx
            .tmux_server
            .spawn_window(
                session_name.clone(),
                "master".to_string(),
                std::env::temp_dir(),
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "printf ready; sleep 300".to_string(),
                ],
            )
            .await
            .unwrap();
        let master_pid = ctx.tmux_server.get_pane_pid(pane_id.clone()).await.unwrap();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_kill_master", "p_kill_master", "/tmp/kill-master")
                .unwrap();
            conn.execute(
                "UPDATE sessions SET master_pane_id = ?, master_pid = ? WHERE id = ?",
                rusqlite::params![pane_id.0.clone(), master_pid, "s_kill_master"],
            )
            .unwrap();
        }

        let result = handle_session_kill(json!({ "session_id": "s_kill_master" }), &ctx)
            .await
            .unwrap();

        assert_eq!(result["state"], "KILLED");
        assert_eq!(result["master_pane_killed"], true);
        assert!(ctx.tmux_server.get_pane_pid(pane_id).await.is_err());
        let _ = Command::new("tmux")
            .args(["-L", ctx.tmux_server.socket_name(), "kill-server"])
            .output();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(global_env)]
    async fn test_session_kill_burns_master_sandbox() {
        let ctx = test_ctx();
        let cache_home = tempfile::TempDir::new().unwrap();
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        unsafe {
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
        }
        let master_sandbox_dir = crate::sandbox::path::resolve_sandbox_dir(
            &ctx.state_dir,
            "s_kill_master_sandbox",
            "master",
        )
        .unwrap();
        let master_home =
            crate::provider::home_layout::sandbox_home_for_sandbox_dir(&master_sandbox_dir)
                .unwrap();
        std::fs::create_dir_all(master_home.join(".claude")).unwrap();
        std::fs::write(master_home.join(".claude/.credentials.json"), b"secret").unwrap();
        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "s_kill_master_sandbox",
                "p_kill_master_sandbox",
                "/tmp/kill-master-sandbox",
            )
            .unwrap();
        }

        handle_session_kill(json!({ "session_id": "s_kill_master_sandbox" }), &ctx)
            .await
            .unwrap();
        restore_env("XDG_CACHE_HOME", old_cache);

        assert!(!master_sandbox_dir.exists());
        assert!(!master_home.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_spawn_returns_idle_and_inserts_pid() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let project_dir = insert_session_with_temp_dir(&ctx, "s1", "p1");
        let agent_id = format!("ag_spawn_{}", Uuid::new_v4());
        let result = handle_agent_spawn(
            json!({
                "session_id": "s1",
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();

        let pid: Option<i64> = {
            let conn = ctx.db.conn();
            conn.query_row(
                "SELECT pid FROM agents WHERE id = ?",
                [agent_id.as_str()],
                |row| row.get(0),
            )
            .unwrap()
        };

        assert_eq!(result["state"], "SPAWNING");
        assert!(result["pid"].as_i64().unwrap() > 0);
        let pane_id = result["pane_id"].as_str().unwrap();
        assert!(pane_id.starts_with('%'));
        assert_eq!(
            tmux_pane_current_path(&ctx, pane_id),
            project_dir.path().to_string_lossy().as_ref()
        );
        assert!(pid.is_some());
        wait_for_state(&ctx, &agent_id, "IDLE").await;
        cleanup_agent(&ctx, &agent_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_spawn_ignores_layout_hint_route() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let _project_dir = insert_session_with_temp_dir(&ctx, "s_layout_ignored", "p_layout");
        let agent_id = format!("ag_layout_{}", Uuid::new_v4());
        let result = handle_agent_spawn(
            json!({
                "session_id": "s_layout_ignored",
                "agent_id": agent_id,
                "provider": "bash",
                "layout_parent_pane_id": "%999999",
                "layout_direction": "right",
                "layout_percent": 40,
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(result["state"], "SPAWNING");
        assert!(result["pane_id"].as_str().unwrap().starts_with('%'));
        wait_for_state(&ctx, &agent_id, "IDLE").await;
        cleanup_agent(&ctx, &agent_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_job_submit_queues_job() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_job", "p_job", "/tmp/t8-job").unwrap();
            insert_agent_sync(&conn, "ag_job", "s_job", "bash", "IDLE", Some(123)).unwrap();
        }

        let result = handle_job_submit(
            json!({
                "agent_id": "ag_job",
                "text": "echo from job\n",
                "request_id": "job-req-1",
            }),
            &ctx,
        )
        .await
        .unwrap();

        let job_id = result["job_id"].as_str().unwrap();
        assert!(job_id.starts_with("job_"));
        assert_eq!(result["status"], "QUEUED");
        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.agent_id, "ag_job");
        assert_eq!(job.prompt_text, "echo from job\n");
        assert_eq!(job.request_id.as_deref(), Some("job-req-1"));
        assert_eq!(job.status, "QUEUED");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_job_submit_is_idempotent_by_request_id() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_job", "p_job", "/tmp/t8-job").unwrap();
            insert_agent_sync(&conn, "ag_job", "s_job", "bash", "BUSY", Some(123)).unwrap();
        }

        let first = handle_job_submit(
            json!({
                "agent_id": "ag_job",
                "text": "echo first\n",
                "request_id": "same-req",
            }),
            &ctx,
        )
        .await
        .unwrap();
        let second = handle_job_submit(
            json!({
                "agent_id": "ag_job",
                "text": "echo second\n",
                "request_id": "same-req",
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(first["job_id"], second["job_id"]);
        let job = query_job_sync(&ctx.db.conn(), first["job_id"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(job.prompt_text, "echo first\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_job_submit_rejects_missing_or_terminal_agent() {
        let ctx = test_ctx();
        let missing = handle_job_submit(
            json!({
                "agent_id": "missing",
                "text": "echo nope\n",
            }),
            &ctx,
        )
        .await
        .unwrap_err();
        assert!(matches!(missing, CcbdError::AgentNotFound(agent) if agent == "missing"));

        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_job", "p_job", "/tmp/t8-job").unwrap();
            insert_agent_sync(&conn, "ag_dead", "s_job", "bash", "CRASHED", Some(123)).unwrap();
        }
        let terminal = handle_job_submit(
            json!({
                "agent_id": "ag_dead",
                "text": "echo nope\n",
            }),
            &ctx,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(terminal, CcbdError::AgentWrongState { current_state } if current_state == "CRASHED")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_job_wait_fast_path_completed() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_job_wait", "p_job_wait", "/tmp/t8-wait").unwrap();
            insert_agent_sync(&conn, "ag_wait", "s_job_wait", "bash", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, "job_wait_done", "ag_wait", None, "echo done\n").unwrap();
            conn.execute(
                "UPDATE jobs SET status='COMPLETED', reply_text='done\n', completed_at=unixepoch() WHERE id='job_wait_done'",
                [],
            )
            .unwrap();
        }

        let result = handle_job_wait(
            json!({
                "job_id": "job_wait_done",
                "timeout": 1,
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(result["status"], "COMPLETED");
        assert_eq!(result["reply_text"], "done\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_job_wait_rejects_missing_job() {
        let ctx = test_ctx();
        let err = handle_job_wait(
            json!({
                "job_id": "missing",
                "timeout": 1,
            }),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, CcbdError::IpcInvalidRequest(message) if message.contains("job_id not found"))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_job_wait_times_out_for_queued_job() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "s_job_wait_timeout",
                "p_job_wait_timeout",
                "/tmp/t8-wait-timeout",
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "ag_wait_timeout",
                "s_job_wait_timeout",
                "bash",
                "IDLE",
                Some(123),
            )
            .unwrap();
            insert_job_sync(
                &conn,
                "job_wait_timeout",
                "ag_wait_timeout",
                None,
                "echo later\n",
            )
            .unwrap();
        }

        let err = handle_job_wait(
            json!({
                "job_id": "job_wait_timeout",
                "timeout": 0,
            }),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, CcbdError::PtyIoError(message) if message.contains("Timeout")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_send_idempotent() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let _project_dir = insert_session_with_temp_dir(&ctx, "s1", "p1");
        let agent_id = format!("ag_send_{}", Uuid::new_v4());
        handle_agent_spawn(
            json!({
                "session_id": "s1",
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();
        wait_for_state(&ctx, &agent_id, "IDLE").await;

        let first = handle_agent_send(
            json!({
                "agent_id": agent_id,
                "text": "sleep 1; echo first\n",
                "request_id": "A",
            }),
            &ctx,
        )
        .await
        .unwrap();
        let second = handle_agent_send(
            json!({
                "agent_id": agent_id,
                "text": "echo second\n",
                "request_id": "A",
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert_eq!(first["seq_id"], second["seq_id"]);
        assert_eq!(command_count(&ctx, &agent_id), 1);
        cleanup_agent(&ctx, &agent_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_read_streams_output_chunk() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let _project_dir = insert_session_with_temp_dir(&ctx, "s1", "p1");
        let agent_id = format!("ag_read_{}", Uuid::new_v4());
        handle_agent_spawn(
            json!({
                "session_id": "s1",
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();
        wait_for_state(&ctx, &agent_id, "IDLE").await;
        handle_agent_send(
            json!({
                "agent_id": agent_id,
                "text": "echo handler_read\n",
                "request_id": "read-1",
            }),
            &ctx,
        )
        .await
        .unwrap();
        sleep_ms(500).await;

        let result = handle_agent_read(
            json!({
                "agent_id": agent_id,
                "since_event_id": 0,
            }),
            &ctx,
        )
        .await
        .unwrap();
        let events = result["events"].as_array().unwrap();

        assert!(
            events.iter().any(|event| event["payload"]
                .as_str()
                .unwrap_or("")
                .contains("handler_read")),
            "events={events:?}"
        );
        cleanup_agent(&ctx, &agent_id).await;
    }

    #[test]
    fn test_agent_read_allows_crashed_state() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/t4-crashed-read").unwrap();
            insert_agent_sync(&conn, "ag_crashed", "s1", "bash", "CRASHED", None).unwrap();
        }

        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(handle_agent_read(
                json!({
                    "agent_id": "ag_crashed",
                    "since_event_id": 0,
                }),
                &ctx,
            ))
            .unwrap();

        assert_eq!(result["is_truncated"], false);
        assert_eq!(result["events"].as_array().unwrap().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_watch_fast_path_returns_existing_events() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_watch", "p_watch", "/tmp/t8-watch").unwrap();
            insert_agent_sync(&conn, "ag_watch", "s_watch", "bash", "IDLE", Some(123)).unwrap();
            conn.execute(
                "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES ('ag_watch', NULL, 'output_chunk', '{\"text\":\"watch-ok\"}')",
                [],
            )
            .unwrap();
        }

        let result = handle_agent_watch(
            json!({
                "agent_id": "ag_watch",
                "since_event_id": 0,
                "timeout": 1,
            }),
            &ctx,
        )
        .await
        .unwrap();

        let events = result["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_type"], "output_chunk");
        assert!(events[0]["payload"].as_str().unwrap().contains("watch-ok"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_watch_times_out_empty() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "s_watch_empty",
                "p_watch_empty",
                "/tmp/t8-watch-empty",
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "ag_watch_empty",
                "s_watch_empty",
                "bash",
                "IDLE",
                Some(123),
            )
            .unwrap();
        }

        let result = handle_agent_watch(
            json!({
                "agent_id": "ag_watch_empty",
                "since_event_id": 0,
                "timeout": 0,
            }),
            &ctx,
        )
        .await
        .unwrap();

        assert!(result["events"].as_array().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_kill_marks_killed_and_is_idempotent_on_repeat() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let _project_dir = insert_session_with_temp_dir(&ctx, "s1", "p1");
        let agent_id = format!("ag_kill_{}", Uuid::new_v4());
        handle_agent_spawn(
            json!({
                "session_id": "s1",
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();
        wait_for_state(&ctx, &agent_id, "IDLE").await;

        let result = handle_agent_kill(json!({ "agent_id": agent_id }), &ctx)
            .await
            .unwrap();
        let repeat = handle_agent_kill(json!({ "agent_id": agent_id }), &ctx)
            .await
            .unwrap();
        let missing = handle_agent_kill(json!({ "agent_id": "missing_kill" }), &ctx)
            .await
            .unwrap_err();

        let (state, payload): (String, String) = ctx
            .db
            .conn()
            .query_row(
                "SELECT agents.state, events.payload FROM agents JOIN events ON events.agent_id = agents.id WHERE agents.id = ? AND events.event_type = 'state_change' AND events.payload LIKE '%SIGKILL_BY_DAEMON%'",
                [agent_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(result["state"], "KILLED");
        assert_eq!(repeat["state"], "KILLED");
        assert!(matches!(
            missing,
            crate::error::CcbdError::AgentNotFound(agent) if agent == "missing_kill"
        ));
        assert_eq!(state, "KILLED");
        assert_eq!(payload["to"], "KILLED");
        assert_eq!(payload["reason"], "SIGKILL_BY_DAEMON");
        assert!(!crate::monitor::contains(&agent_id));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_kill_removes_sandbox_dir() {
        let ctx = test_ctx();
        let agent_id = format!("ag_kill_sandbox_{}", Uuid::new_v4());
        let sandbox_dir = ctx
            .state_dir
            .join("sandboxes")
            .join("s_kill_sandbox")
            .join(&agent_id);
        std::fs::create_dir_all(&sandbox_dir).unwrap();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_kill_sandbox", "p1", "/tmp/t4-kill-sandbox").unwrap();
            insert_agent_sync(&conn, &agent_id, "s_kill_sandbox", "bash", "IDLE", None).unwrap();
        }

        handle_agent_kill(json!({ "agent_id": agent_id }), &ctx)
            .await
            .unwrap();

        assert!(!sandbox_dir.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_session_kill_removes_all_agent_sandbox_dirs() {
        let ctx = test_ctx();
        let agent_a = format!("ag_session_kill_a_{}", Uuid::new_v4());
        let agent_b = format!("ag_session_kill_b_{}", Uuid::new_v4());
        let sandbox_a = ctx
            .state_dir
            .join("sandboxes")
            .join("s_session_kill_sandbox")
            .join(&agent_a);
        let sandbox_b = ctx
            .state_dir
            .join("sandboxes")
            .join("s_session_kill_sandbox")
            .join(&agent_b);
        std::fs::create_dir_all(&sandbox_a).unwrap();
        std::fs::create_dir_all(&sandbox_b).unwrap();
        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "s_session_kill_sandbox",
                "p1",
                "/tmp/t4-session-kill-sandbox",
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                &agent_a,
                "s_session_kill_sandbox",
                "bash",
                "IDLE",
                None,
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                &agent_b,
                "s_session_kill_sandbox",
                "bash",
                "BUSY",
                None,
            )
            .unwrap();
        }

        handle_session_kill(json!({ "session_id": "s_session_kill_sandbox" }), &ctx)
            .await
            .unwrap();

        assert!(!sandbox_a.exists());
        assert!(!sandbox_b.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_assert_state_reviews_evidence() {
        let ctx = test_ctx();
        let agent_id = format!("ag_assert_{}", Uuid::new_v4());
        let evidence_id = seed_unknown_with_evidence(&ctx, &agent_id);

        let result = handle_agent_assert_state(
            json!({
                "agent_id": agent_id,
                "state": "IDLE",
                "evidence_id": evidence_id,
            }),
            &ctx,
        )
        .await
        .unwrap();
        let (state, sub_state, evidence_status, asserted): (
            String,
            Option<String>,
            String,
            Option<String>,
        ) = ctx
            .db
            .conn()
            .query_row(
                "SELECT agents.state, agents.sub_state, evidence.status, evidence.l3_asserted_state FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id = ?",
                [agent_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert_eq!(result["state"], "IDLE");
        assert_eq!(result["sub_state"], "Asserted");
        assert_eq!(state, "IDLE");
        assert_eq!(sub_state.as_deref(), Some("Asserted"));
        assert_eq!(evidence_status, "REVIEWED");
        assert_eq!(asserted.as_deref(), Some("IDLE"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_assert_state_rejects_wrong_agent() {
        let ctx = test_ctx();
        let evidence_id = seed_unknown_with_evidence(&ctx, "ag_owner");
        {
            let conn = ctx.db.conn();
            insert_agent_sync(&conn, "ag_other", "s_evidence", "bash", "UNKNOWN", Some(2)).unwrap();
        }

        let err = handle_agent_assert_state(
            json!({
                "agent_id": "ag_other",
                "state": "IDLE",
                "evidence_id": evidence_id,
            }),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, CcbdError::DbEvidenceNotFound { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_discard_evidence_is_idempotent() {
        let ctx = test_ctx();
        let evidence_id = seed_unknown_with_evidence(&ctx, "ag_discard");

        let first = handle_agent_discard_evidence(json!({ "evidence_id": evidence_id }), &ctx)
            .await
            .unwrap();
        let second = handle_agent_discard_evidence(json!({ "evidence_id": evidence_id }), &ctx)
            .await
            .unwrap();

        assert_eq!(first["status"], "DISCARDED");
        assert_eq!(second["status"], "DISCARDED");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_system_dump_excludes_pane_bytes() {
        let ctx = test_ctx();
        seed_unknown_with_evidence(&ctx, "ag_dump");

        let dump = handle_system_dump(json!({}), &ctx).await.unwrap();

        assert_eq!(dump["projects"].as_array().unwrap().len(), 1);
        assert_eq!(dump["sessions"].as_array().unwrap().len(), 1);
        assert_eq!(dump["agents"].as_array().unwrap().len(), 1);
        assert_eq!(dump["evidence_pending"].as_array().unwrap().len(), 1);
        assert!(!dump.to_string().contains("pane_bytes"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_agent_send_rejects_unknown_with_new_request() {
        let _tmux_guard = TMUX_TEST_LOCK.lock().await;
        let ctx = test_ctx();
        let agent_id = format!("ag_unknown_send_{}", Uuid::new_v4());
        let _project_dir = insert_session_with_temp_dir(&ctx, "s_unknown", "p_unknown");
        let _ = handle_agent_spawn(
            json!({
                "session_id": "s_unknown",
                "agent_id": agent_id,
                "provider": "bash",
            }),
            &ctx,
        )
        .await
        .unwrap();
        wait_for_state(&ctx, &agent_id, "IDLE").await;
        {
            let conn = ctx.db.conn();
            update_agent_state_sync(&conn, &agent_id, "UNKNOWN").unwrap();
        }

        let err = handle_agent_send(
            json!({
                "agent_id": agent_id,
                "text": "echo recover\n",
                "request_id": "recover-1",
            }),
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            CcbdError::AgentWrongState { current_state } if current_state == "UNKNOWN"
        ));
        assert_eq!(command_count(&ctx, &agent_id), 0);
        let _ = handle_agent_kill(json!({ "agent_id": agent_id }), &ctx).await;
        let _ = crate::marker::registry::take(&agent_id);
    }
}
