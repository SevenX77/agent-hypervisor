#![cfg(unix)]

mod common;

use ah::db;
use ah::db::agents::query_agent_state;
use ah::db::sessions::insert_session;
use ah::home_materialization::prepare_home_layout;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_kill, handle_agent_read, handle_agent_send, handle_agent_spawn,
};
use ah::sandbox::EnvState;
use ah::tmux::{TmuxServer, compute_socket_name};
use common::scope_policy_for_test;
use serde_json::{Value, json};
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct Harness {
    ctx: Ctx,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl Harness {
    fn new() -> Self {
        which::which("tmux").expect("tmux binary required for mvp7 acceptance tests");
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let state_dir_path = state_dir.path().to_path_buf();
        let socket_name = compute_socket_name(state_dir.path());
        let policy = scope_policy_for_test(&socket_name);
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir_path.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new_with_policy(state_dir.path(), policy)),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        Self {
            ctx,
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }

    async fn insert_session(&self, session_id: &str) {
        insert_session(
            self.ctx.db.clone(),
            session_id.to_string(),
            format!("p_{session_id}"),
            format!("/tmp/{session_id}"),
        )
        .await
        .unwrap();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = Command::new("tmux")
            .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
            .output();
        let socket_path = format!(
            "/tmp/tmux-{}/{}",
            unsafe { libc::geteuid() },
            self.ctx.tmux_server.socket_name()
        );
        let _ = std::fs::remove_file(socket_path);
    }
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

async fn spawn_bash(h: &Harness, session_id: &str, agent_id: &str) -> i64 {
    let result = handle_agent_spawn(
        json!({
            "session_id": session_id,
            "agent_id": agent_id,
            "provider": "bash",
        }),
        &h.ctx,
    )
    .await
    .unwrap();

    assert_eq!(result["state"], "SPAWNING");
    let pid = result["pid"].as_i64().unwrap();
    wait_for_state(h, agent_id, "IDLE", Duration::from_secs(10)).await;
    pid
}

async fn send_text(h: &Harness, agent_id: &str, text: &str) {
    handle_agent_send(
        json!({
            "agent_id": agent_id,
            "text": text,
            "request_id": format!("req_{}", uuid::Uuid::new_v4()),
        }),
        &h.ctx,
    )
    .await
    .unwrap();
}

async fn read_events(h: &Harness, agent_id: &str) -> Vec<Value> {
    handle_agent_read(
        json!({
            "agent_id": agent_id,
            "since_event_id": 0,
        }),
        &h.ctx,
    )
    .await
    .unwrap()["events"]
        .as_array()
        .unwrap()
        .clone()
}

async fn wait_for_output_containing(
    h: &Harness,
    agent_id: &str,
    timeout: Duration,
    predicates: &[&str],
) -> String {
    let deadline = Instant::now() + timeout;
    let mut last_combined = String::new();
    while Instant::now() < deadline {
        let combined = read_events(h, agent_id)
            .await
            .into_iter()
            .filter(|event| event["event_type"] == "output_chunk")
            .filter_map(|event| event["payload"].as_str().map(str::to_string))
            .collect::<Vec<_>>()
            .join("\n");
        if predicates.iter().all(|needle| combined.contains(needle)) {
            return combined;
        }
        last_combined = combined;
        sleep_ms(50).await;
    }
    panic!(
        "output for {agent_id} did not contain {predicates:?} within {timeout:?}; output={last_combined:?}"
    );
}

async fn wait_for_state(h: &Harness, agent_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let state = query_agent_state(h.ctx.db.clone(), agent_id.to_string())
            .await
            .unwrap()
            .map(|(state, _)| state);
        if state.as_deref() == Some(expected) {
            return;
        }
        sleep_ms(50).await;
    }
    panic!("agent {agent_id} did not reach {expected} within {timeout:?}");
}

async fn current_state(db: db::Db, agent_id: &str) -> Option<String> {
    query_agent_state(db, agent_id.to_string())
        .await
        .unwrap()
        .map(|(state, _)| state)
}

#[test]
fn test_codex_auth_mount_passthrough() {
    // Codex auth is now supplied by a materialized provider HOME. The systemd
    // wrapper receives env vars pointing at the host-side sandbox home directly.
    let home = tempfile::tempdir().unwrap();
    let codex_dir = home.path().join(".codex");
    std::fs::create_dir_all(&codex_dir).unwrap();
    std::fs::write(codex_dir.join("auth.json"), "{}").unwrap();
    std::fs::write(codex_dir.join("installation_id"), "id").unwrap();
    let _env = EnvGuard::set(home.path(), &home.path().join(".cache"));

    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let overrides = prepare_home_layout("codex", sandbox.path(), workspace.path()).unwrap();

    assert!(overrides.home_root.join(".codex/auth.json").exists());
    assert_eq!(
        overrides.extra_env.get("HOME").unwrap(),
        &overrides.home_root.display().to_string()
    );
    assert_eq!(
        overrides.extra_env.get("CODEX_HOME").unwrap(),
        &overrides.home_root.join(".codex").display().to_string()
    );
}

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    old_home: Option<std::ffi::OsString>,
    old_cache: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(home: &std::path::Path, cache_home: &std::path::Path) -> Self {
        let lock = ENV_LOCK.lock().expect("env lock poisoned");
        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        unsafe {
            std::env::set_var("HOME", home);
            std::env::set_var("XDG_CACHE_HOME", cache_home);
        }
        Self {
            _lock: lock,
            old_home,
            old_cache,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        restore_env("HOME", self.old_home.as_ref());
        restore_env("XDG_CACHE_HOME", self.old_cache.as_ref());
    }
}

fn restore_env(key: &str, value: Option<&std::ffi::OsString>) {
    unsafe {
        match value {
            Some(old_value) => std::env::set_var(key, old_value),
            None => std::env::remove_var(key),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiline_paste_preserves_newlines() {
    let h = Harness::new();
    h.insert_session("s_m7_paste").await;
    let agent_id = format!("ag_m7_paste_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_m7_paste", &agent_id).await;

    send_text(
        &h,
        &agent_id,
        "for i in 1 2; do\n  echo \"line $i\"\ndone\n",
    )
    .await;

    let output =
        wait_for_output_containing(&h, &agent_id, Duration::from_secs(5), &["line 1", "line 2"])
            .await;
    assert!(
        !output.contains("syntax error near unexpected token"),
        "multiline paste caused shell syntax error; output={output:?}"
    );
    wait_for_state(&h, &agent_id, "IDLE", Duration::from_secs(5)).await;
    let _ = handle_agent_kill(json!({ "agent_id": agent_id }), &h.ctx).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires playground codex login and provider availability"]
async fn test_true_codex_smoke_idle_roundtrip() {
    panic!("manual playground smoke hook for MVP7 final validation");
}
