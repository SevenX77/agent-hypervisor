#![cfg(target_os = "linux")]
use ah::agent_io::{
    AgentIoEntry, RuntimeCleanupPolicy, cleanup_agent_runtime_resources_with_policy, register,
};
use ah::cli::config::{AgentConfig, DaemonConfig, MasterConfig, ProjectConfig, SandboxConfig};
use ah::cli::rpc_client::{CliError, RpcClient, RpcFuture};
use ah::cli::start::{
    ahd_reset_failed_is_best_effort, build_ahd_systemd_run_command,
    build_ahd_systemd_run_command_with_env, should_skip_systemd_bootstrap_for_cgroup,
    start_project,
};
use ah::db;
use ah::monitor::agent_watch::spawn_agent_pidfd_watch_task;
use ah::monitor::pidfd_open;
use ah::provider::home_layout::{
    AuthMaterializationErrorCode, materialize_auth_file_with_ladder, sandbox_home_for_sandbox_dir,
};
use ah::provider::manifest::{compute_recovery_args, get_manifest};
use ah::sandbox::EnvState;
use ah::sandbox::systemd::{RecoverySpawn, wrap_command, wrap_command_with_recovery};
use ah::tmux::TmuxPaneId;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::future;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn unsafe_env_state() -> EnvState {
    EnvState {
        systemd_run_available: false,
        unsafe_no_sandbox: true,
        under_systemd: false,
    }
}

fn fixture_project_config() -> ProjectConfig {
    let mut agents = BTreeMap::new();
    agents.insert(
        "a1".to_string(),
        AgentConfig {
            provider: "codex".to_string(),
            env: HashMap::new(),
            hooks: HashMap::new(),
            plugins: Vec::new(),
            skills: Vec::new(),
            bundle: Vec::new(),
            settings: serde_json::Map::new(),
        },
    );
    ProjectConfig {
        version: "1".to_string(),
        master: MasterConfig {
            enabled: false,
            ..MasterConfig::default()
        },
        completion: Default::default(),
        daemon: DaemonConfig::default(),
        providers: Default::default(),
        env: HashMap::new(),
        sandbox: SandboxConfig::default(),
        agents,
    }
}

struct RecordingClient {
    calls: Mutex<Vec<(String, Value)>>,
    session_list_response: Value,
}

impl RecordingClient {
    fn with_sessions(sessions: Value) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            session_list_response: sessions,
        }
    }

    fn methods(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, _)| method.clone())
            .collect()
    }
}

impl Default for RecordingClient {
    fn default() -> Self {
        Self::with_sessions(json!({ "sessions": [] }))
    }
}

impl RpcClient for RecordingClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_string(), params));
            match method {
                "session.list" => Ok(self.session_list_response.clone()),
                "session.create" => Ok(json!({ "session_id": "created-session" })),
                "session.realign" => Ok(json!({ "results": [] })),
                "session.spawn_master_pane" => Ok(json!({ "pane_id": "%0" })),
                "agent.spawn" => Ok(json!({ "pane_id": "%1", "pid": 1234 })),
                "session.kill" => Ok(json!({})),
                _ => Err(CliError::InvalidResponse(format!(
                    "unexpected method {method}"
                ))),
            }
        })
    }
}

fn session_summary(id: &str, absolute_path: &Path, active_agents: i64) -> Value {
    json!({
        "id": id,
        "project_id": absolute_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("project"),
        "absolute_path": absolute_path.display().to_string(),
        "status": "ACTIVE",
        "active_agents": active_agents,
        "created_at": 1,
    })
}

async fn wait_for_agent_state(db: &db::Db, agent_id: &str, expected: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if db::agents::query_agent(db.clone(), agent_id.to_string())
            .await
            .unwrap()
            .is_some_and(|agent| agent.state == expected)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn register_agent_io_entry(
    state_dir: &Path,
    session_id: &str,
    agent_id: &str,
) -> (PathBuf, PathBuf) {
    let fifo_dir = state_dir.join("pipes");
    std::fs::create_dir_all(&fifo_dir).unwrap();
    let fifo_path = fifo_dir.join(format!("{agent_id}.fifo"));
    std::fs::write(&fifo_path, b"").unwrap();
    let sandbox = state_dir.join("sandboxes").join(session_id).join(agent_id);
    let home = sandbox_home_for_sandbox_dir(&sandbox).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    register(
        agent_id.to_string(),
        AgentIoEntry {
            session_id: session_id.to_string(),
            pane_id: TmuxPaneId("%1".to_string()),
            expected_pid: None,
            reader_handle: tokio::spawn(async { future::pending::<()>().await }),
            fifo_path: fifo_path.clone(),
            socket_name: "missing-socket".to_string(),
            idle_scan_enabled: Arc::new(AtomicBool::new(true)),
        },
    );
    (fifo_path, home)
}

fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
}

#[test]
fn pr7_provider_recovery_args_codex_exact_uuid_from_rollout_jsonl() {
    let temp = tempfile::tempdir().unwrap();
    let session_dir = temp.path().join(".codex/sessions/2026/06/11/session-id");
    std::fs::create_dir_all(&session_dir).unwrap();
    std::fs::write(
        session_dir.join("rollout-abc.jsonl"),
        r#"{"timestamp":"2026-06-11T00:00:00Z","type":"session_meta","payload":{"id":"550e8400-e29b-41d4-a716-446655440000","cwd":"/tmp/project"}}"#,
    )
    .unwrap();

    assert_eq!(
        compute_recovery_args("codex", temp.path()),
        vec![
            "resume".to_string(),
            "550e8400-e29b-41d4-a716-446655440000".to_string()
        ]
    );
}

#[test]
fn pr7_provider_recovery_args_codex_ignores_non_session_meta_rollout() {
    let temp = tempfile::tempdir().unwrap();
    let session_dir = temp.path().join(".codex/sessions/2026/06/11/session-id");
    std::fs::create_dir_all(&session_dir).unwrap();
    std::fs::write(
        session_dir.join("rollout-abc.jsonl"),
        r#"{"timestamp":"2026-06-11T00:00:00Z","type":"event_msg","payload":{"id":"550e8400-e29b-41d4-a716-446655440000"}}"#,
    )
    .unwrap();

    assert_eq!(
        compute_recovery_args("codex", temp.path()),
        vec!["resume".to_string(), "--last".to_string()]
    );
}

#[test]
fn pr7_provider_recovery_args_codex_falls_back_to_last() {
    let temp = tempfile::tempdir().unwrap();

    assert_eq!(
        compute_recovery_args("codex", temp.path()),
        vec!["resume".to_string(), "--last".to_string()]
    );
}

#[test]
fn pr7_realign_recovery_contract_computes_dynamic_codex_args_then_wraps_command() {
    let temp = tempfile::tempdir().unwrap();
    let session_dir = temp.path().join(".codex/sessions/2026/06/11/session-id");
    std::fs::create_dir_all(&session_dir).unwrap();
    std::fs::write(
        session_dir.join("rollout-abc.jsonl"),
        r#"{"timestamp":"2026-06-11T00:00:00Z","type":"session_meta","payload":{"id":"550e8400-e29b-41d4-a716-446655440000"}}"#,
    )
    .unwrap();
    let recovery_args = compute_recovery_args("codex", temp.path());
    let manifest = get_manifest("codex");

    let cmd = wrap_command_with_recovery(
        "a1",
        "project",
        "daemon",
        &unsafe_env_state(),
        RecoverySpawn {
            is_recovery: true,
            args: recovery_args,
        },
        None,
        &manifest,
        &HashMap::new(),
    );

    assert!(cmd.ends_with(&[
        "resume".to_string(),
        "550e8400-e29b-41d4-a716-446655440000".to_string()
    ]));
}

#[test]
fn pr7_provider_recovery_args_antigravity_continue() {
    let temp = tempfile::tempdir().unwrap();

    assert_eq!(
        compute_recovery_args("antigravity", temp.path()),
        vec!["--continue".to_string()]
    );
}

#[test]
fn pr7_systemd_dynamic_recovery_args_override_static_resume_args() {
    let manifest = get_manifest("codex");
    let cmd = wrap_command_with_recovery(
        "a1",
        "project",
        "daemon",
        &unsafe_env_state(),
        RecoverySpawn {
            is_recovery: true,
            args: vec!["resume".to_string(), "uuid-123".to_string()],
        },
        None,
        &manifest,
        &HashMap::new(),
    );

    assert!(cmd.ends_with(&["resume".to_string(), "uuid-123".to_string()]));
    assert!(!cmd.ends_with(&["resume".to_string(), "--last".to_string()]));
}

#[test]
fn pr7_systemd_claude_recovery_keeps_static_continue_fallback() {
    let manifest = get_manifest("claude");
    let cmd = wrap_command(
        "a1",
        "project",
        "daemon",
        &unsafe_env_state(),
        true,
        None,
        &manifest,
        &HashMap::new(),
    );

    assert!(cmd.ends_with(&["--continue".to_string()]));
}

#[test]
fn pr7_systemd_non_recovery_does_not_append_resume_args() {
    let manifest = get_manifest("claude");
    let cmd = wrap_command(
        "a1",
        "project",
        "daemon",
        &unsafe_env_state(),
        false,
        None,
        &manifest,
        &HashMap::new(),
    );

    assert!(!cmd.contains(&"--continue".to_string()));
}

#[tokio::test]
async fn pr7_startup_reconcile_preserves_dead_codex_home() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("ah.db");
    let state_dir = temp.path().join("state");
    let db = db::init(&db_path).unwrap();
    db::sessions::create_session(
        db.clone(),
        "s1".to_string(),
        "p1".to_string(),
        temp.path().display().to_string(),
    )
    .await
    .unwrap();
    db::agents::insert_agent(
        db.clone(),
        "a1".to_string(),
        "s1".to_string(),
        "codex".to_string(),
        "IDLE".to_string(),
        Some(9_999_999),
    )
    .await
    .unwrap();
    let sandbox = state_dir.join("sandboxes/s1/a1");
    let home = sandbox_home_for_sandbox_dir(&sandbox).unwrap();
    std::fs::create_dir_all(&home).unwrap();

    db::system::reconcile_startup_with_state_dir(db, state_dir)
        .await
        .unwrap();

    assert!(
        home.exists(),
        "recoverable provider home must survive startup reconcile"
    );
}

#[tokio::test]
async fn pr7_startup_reconcile_deletes_dead_bash_home() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("ah.db");
    let state_dir = temp.path().join("state");
    let db = db::init(&db_path).unwrap();
    db::sessions::create_session(
        db.clone(),
        "s1".to_string(),
        "p1".to_string(),
        temp.path().display().to_string(),
    )
    .await
    .unwrap();
    db::agents::insert_agent(
        db.clone(),
        "a1".to_string(),
        "s1".to_string(),
        "bash".to_string(),
        "IDLE".to_string(),
        Some(9_999_999),
    )
    .await
    .unwrap();
    let sandbox = state_dir.join("sandboxes/s1/a1");
    let home = sandbox_home_for_sandbox_dir(&sandbox).unwrap();
    std::fs::create_dir_all(&home).unwrap();

    db::system::reconcile_startup_with_state_dir(db, state_dir)
        .await
        .unwrap();

    assert!(
        !home.exists(),
        "non-recoverable bash home should still be collected"
    );
}

#[test]
fn pr7_orphan_gc_preserves_recovery_eligible_crashed_home() {
    assert!(db::system::recovery_eligible_orphan_scope_should_be_preserved("codex", "CRASHED"));
    assert!(!db::system::recovery_eligible_orphan_scope_should_be_preserved("bash", "CRASHED"));
}

#[tokio::test]
async fn pr7_runtime_cleanup_preserves_recoverable_crashed_home_but_removes_fifo() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let fifo_dir = state_dir.join("pipes");
    std::fs::create_dir_all(&fifo_dir).unwrap();
    let fifo_path = fifo_dir.join("a1.fifo");
    std::fs::write(&fifo_path, b"").unwrap();
    let sandbox = state_dir.join("sandboxes/s1/a1");
    let home = sandbox_home_for_sandbox_dir(&sandbox).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    register(
        "a1".to_string(),
        AgentIoEntry {
            session_id: "s1".to_string(),
            pane_id: TmuxPaneId("%1".to_string()),
            expected_pid: None,
            reader_handle: tokio::spawn(async { future::pending::<()>().await }),
            fifo_path: fifo_path.clone(),
            socket_name: "missing-socket".to_string(),
            idle_scan_enabled: Arc::new(AtomicBool::new(true)),
        },
    );

    cleanup_agent_runtime_resources_with_policy(
        "a1",
        Some("s1"),
        RuntimeCleanupPolicy::PreserveRecoverableCrashedHome,
    );

    assert!(
        !fifo_path.exists(),
        "fifo/runtime resource should still be cleaned"
    );
    assert!(
        home.exists(),
        "recoverable crashed home should be preserved"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pr7_agent_watch_crash_path_preserves_codex_recoverable_home() {
    let temp = tempfile::tempdir().unwrap();
    let db = db::init(&temp.path().join("ah.db")).unwrap();
    let state_dir = temp.path().join("state");
    let agent_id = format!("pr7_codex_{}", uuid::Uuid::new_v4());
    db::sessions::create_session(
        db.clone(),
        "s1".to_string(),
        "p1".to_string(),
        temp.path().display().to_string(),
    )
    .await
    .unwrap();
    db::agents::insert_agent(
        db.clone(),
        agent_id.clone(),
        "s1".to_string(),
        "codex".to_string(),
        "IDLE".to_string(),
        None,
    )
    .await
    .unwrap();
    let (fifo_path, home) = register_agent_io_entry(&state_dir, "s1", &agent_id);
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 0.2; exit 0")
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let pidfd = pidfd_open(pid).unwrap();
    let task_fd = pidfd.try_clone().unwrap();

    spawn_agent_pidfd_watch_task(
        agent_id.clone(),
        "s1".to_string(),
        pid,
        task_fd,
        Arc::new(db.clone()),
        None,
    );
    tokio::task::spawn_blocking(move || {
        let _ = child.wait();
    });

    assert!(wait_for_agent_state(&db, &agent_id, "CRASHED").await);
    assert!(!fifo_path.exists(), "crash path should still remove fifo");
    assert!(
        home.exists(),
        "agent_watch CRASHED codex path must preserve recoverable home"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pr7_agent_watch_crash_path_deletes_bash_home() {
    let temp = tempfile::tempdir().unwrap();
    let db = db::init(&temp.path().join("ah.db")).unwrap();
    let state_dir = temp.path().join("state");
    let agent_id = format!("pr7_bash_{}", uuid::Uuid::new_v4());
    db::sessions::create_session(
        db.clone(),
        "s1".to_string(),
        "p1".to_string(),
        temp.path().display().to_string(),
    )
    .await
    .unwrap();
    db::agents::insert_agent(
        db.clone(),
        agent_id.clone(),
        "s1".to_string(),
        "bash".to_string(),
        "IDLE".to_string(),
        None,
    )
    .await
    .unwrap();
    let (_fifo_path, home) = register_agent_io_entry(&state_dir, "s1", &agent_id);
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 0.2; exit 0")
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let pidfd = pidfd_open(pid).unwrap();
    let task_fd = pidfd.try_clone().unwrap();

    spawn_agent_pidfd_watch_task(
        agent_id.clone(),
        "s1".to_string(),
        pid,
        task_fd,
        Arc::new(db.clone()),
        None,
    );
    tokio::task::spawn_blocking(move || {
        let _ = child.wait();
    });

    assert!(wait_for_agent_state(&db, &agent_id, "CRASHED").await);
    assert!(
        !home.exists(),
        "agent_watch CRASHED bash path should keep deleting home"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pr7_agent_watch_killed_path_deletes_codex_home() {
    let temp = tempfile::tempdir().unwrap();
    let db = db::init(&temp.path().join("ah.db")).unwrap();
    let state_dir = temp.path().join("state");
    let agent_id = format!("pr7_killed_{}", uuid::Uuid::new_v4());
    db::sessions::create_session(
        db.clone(),
        "s1".to_string(),
        "p1".to_string(),
        temp.path().display().to_string(),
    )
    .await
    .unwrap();
    db::agents::insert_agent(
        db.clone(),
        agent_id.clone(),
        "s1".to_string(),
        "codex".to_string(),
        "KILLED".to_string(),
        None,
    )
    .await
    .unwrap();
    let (_fifo_path, home) = register_agent_io_entry(&state_dir, "s1", &agent_id);
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 0.2; exit 0")
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let pidfd = pidfd_open(pid).unwrap();
    let task_fd = pidfd.try_clone().unwrap();

    spawn_agent_pidfd_watch_task(
        agent_id.clone(),
        "s1".to_string(),
        pid,
        task_fd,
        Arc::new(db.clone()),
        None,
    );
    tokio::task::spawn_blocking(move || {
        let _ = child.wait();
    });

    assert!(wait_for_agent_state(&db, &agent_id, "KILLED").await);
    // Poll instead of sleeping a fixed 300ms: the deletion happens on the watch
    // task, which loses races to a loaded machine and made this test flaky.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while home.exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !home.exists(),
        "explicit KILLED path should keep deleting recoverable-provider home"
    );
}

/// Sandboxes now hold links to the operator's real credential files, so the
/// cleanup that runs on every kill must unlink the entry and never follow it.
/// Getting this wrong would delete the host login the whole fleet shares.
#[test]
fn pr7_removing_a_sandbox_home_never_deletes_the_linked_host_credential() {
    let temp = tempfile::tempdir().unwrap();
    let host_home = temp.path().join("host");
    let sandbox_home = temp.path().join("sandbox");
    std::fs::create_dir_all(host_home.join(".codex")).unwrap();
    std::fs::create_dir_all(sandbox_home.join(".codex")).unwrap();
    let host_credential = host_home.join(".codex/auth.json");
    std::fs::write(&host_credential, "host-token").unwrap();

    materialize_auth_file_with_ladder(&host_home, &sandbox_home, ".codex/auth.json").unwrap();
    assert!(is_symlink(&sandbox_home.join(".codex/auth.json")));

    std::fs::remove_dir_all(&sandbox_home).unwrap();

    assert!(
        host_credential.is_file(),
        "removing a sandbox must not follow the credential link into the host store"
    );
    assert_eq!(
        std::fs::read_to_string(&host_credential).unwrap(),
        "host-token"
    );
}

#[tokio::test]
async fn pr7_cli_start_without_existing_session_still_creates() {
    let temp = tempfile::tempdir().unwrap();
    let client = RecordingClient::with_sessions(json!({ "sessions": [] }));

    start_project(
        &client,
        fixture_project_config(),
        &temp.path().join("ah.toml"),
        temp.path().to_path_buf(),
        false,
    )
    .await
    .unwrap();

    assert!(client.methods().contains(&"session.create".to_string()));
}

#[tokio::test]
async fn pr7_cli_start_unique_recoverable_session_realigns_instead_of_create() {
    let temp = tempfile::tempdir().unwrap();
    let client = RecordingClient::with_sessions(json!({
        "sessions": [session_summary("existing-session", temp.path(), 0)]
    }));

    start_project(
        &client,
        fixture_project_config(),
        &temp.path().join("ah.toml"),
        temp.path().to_path_buf(),
        false,
    )
    .await
    .unwrap();

    let methods = client.methods();
    assert!(methods.contains(&"session.realign".to_string()));
    assert!(!methods.contains(&"session.create".to_string()));
}

#[tokio::test]
async fn pr7_cli_start_multiple_recoverable_sessions_errors_deterministically() {
    let temp = tempfile::tempdir().unwrap();
    let client = RecordingClient::with_sessions(json!({
        "sessions": [
            session_summary("existing-session-a", temp.path(), 0),
            session_summary("existing-session-b", temp.path(), 0)
        ]
    }));

    let result = start_project(
        &client,
        fixture_project_config(),
        &temp.path().join("ah.toml"),
        temp.path().to_path_buf(),
        false,
    )
    .await;

    assert!(
        result
            .err()
            .is_some_and(|err| err.to_string().contains("multiple recoverable sessions"))
    );
}

#[tokio::test]
async fn pr7_session_list_includes_all_crashed_active_session_with_zero_active_agents() {
    let temp = tempfile::tempdir().unwrap();
    let db = db::init(&temp.path().join("ah.db")).unwrap();
    db::sessions::create_session(
        db.clone(),
        "s1".to_string(),
        "p1".to_string(),
        temp.path().display().to_string(),
    )
    .await
    .unwrap();
    db::agents::insert_agent(
        db.clone(),
        "a1".to_string(),
        "s1".to_string(),
        "codex".to_string(),
        "CRASHED".to_string(),
        Some(9_999_999),
    )
    .await
    .unwrap();

    let summaries = db::sessions::list_session_summaries(db).await.unwrap();

    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, "s1");
    assert_eq!(summaries[0].status, "ACTIVE");
    assert_eq!(summaries[0].active_agents, 0);
}

#[test]
fn pr7_ahd_systemd_bootstrap_builds_restartable_user_service_command() {
    let cmd = build_ahd_systemd_run_command(Path::new("/bin/ahd"), Path::new("/tmp/ah-state"));

    assert!(cmd.contains(&"systemd-run".to_string()));
    assert!(cmd.contains(&"--user".to_string()));
    assert!(cmd.contains(&"--unit=ahd.service".to_string()));
    assert!(cmd.contains(&"--property=Restart=on-failure".to_string()));
    assert!(cmd.contains(&"--property=RestartSec=1s".to_string()));
    assert!(cmd.contains(&"--property=StartLimitIntervalSec=60".to_string()));
    assert!(cmd.contains(&"--property=StartLimitBurst=5".to_string()));
}

#[test]
fn pr7_ahd_systemd_bootstrap_propagates_existing_passthrough_env() {
    let env = vec![
        ("PATH".to_string(), "/fake/bin:/usr/bin".to_string()),
        ("LANG".to_string(), "en_US.UTF-8".to_string()),
    ];
    let cmd = build_ahd_systemd_run_command_with_env(
        Path::new("/bin/ahd"),
        Path::new("/tmp/ah-state"),
        &env,
    );

    assert_adjacent_setenv(&cmd, "AH_STATE_DIR=/tmp/ah-state");
    assert_adjacent_setenv(&cmd, "PATH=/fake/bin:/usr/bin");
    assert_adjacent_setenv(&cmd, "LANG=en_US.UTF-8");
    assert!(!cmd.contains(&"SHELL=".to_string()));
}

#[test]
fn pr7_ahd_systemd_bootstrap_skips_recursion_inside_ahd_service() {
    assert!(should_skip_systemd_bootstrap_for_cgroup(
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/ahd.service",
        "ahd.service"
    ));
    assert!(!should_skip_systemd_bootstrap_for_cgroup(
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/ah-p1.service",
        "ah-p2.service"
    ));
    assert!(!should_skip_systemd_bootstrap_for_cgroup(
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/interactive.scope",
        "ahd.service"
    ));
}

#[test]
fn pr7_ahd_reset_failed_is_best_effort() {
    assert!(ahd_reset_failed_is_best_effort("ahd.service"));
}

fn assert_adjacent_setenv(cmd: &[String], expected: &str) {
    assert!(
        cmd.windows(2)
            .any(|pair| pair[0] == "--setenv" && pair[1] == expected),
        "missing adjacent --setenv {expected} in {cmd:?}"
    );
}

#[test]
fn pr7_auth_ladder_replaces_a_stale_private_copy_with_the_shared_store() {
    let temp = tempfile::tempdir().unwrap();
    let source_home = temp.path().join("src");
    let sandbox_home = temp.path().join("home");
    std::fs::create_dir_all(source_home.join(".codex")).unwrap();
    std::fs::create_dir_all(sandbox_home.join(".codex")).unwrap();
    std::fs::write(source_home.join(".codex/auth.json"), "{}").unwrap();
    std::fs::write(sandbox_home.join(".codex/auth.json"), "stale").unwrap();

    materialize_auth_file_with_ladder(&source_home, &sandbox_home, ".codex/auth.json").unwrap();
    let sandbox_path = sandbox_home.join(".codex/auth.json");

    assert_eq!(std::fs::read_to_string(&sandbox_path).unwrap(), "{}");
    assert!(
        is_symlink(&sandbox_path),
        "a sandbox carried over from the copying era must be migrated onto the shared store"
    );
}

#[test]
fn pr7_auth_ladder_reports_source_missing() {
    let temp = tempfile::tempdir().unwrap();
    let err = materialize_auth_file_with_ladder(
        temp.path(),
        &temp.path().join("home"),
        ".codex/auth.json",
    )
    .unwrap_err();

    assert_eq!(err, AuthMaterializationErrorCode::AuthProviderTokenMissing);
}

#[test]
fn pr7_auth_ladder_reports_unwritable_target_mount_fail() {
    let temp = tempfile::tempdir().unwrap();
    let source_home = temp.path().join("src");
    let sandbox_home = temp.path().join("home");
    std::fs::create_dir_all(source_home.join(".codex")).unwrap();
    std::fs::create_dir_all(sandbox_home.join(".codex/auth.json")).unwrap();
    std::fs::write(source_home.join(".codex/auth.json"), "{}").unwrap();

    let err = materialize_auth_file_with_ladder(&source_home, &sandbox_home, ".codex/auth.json")
        .unwrap_err();

    assert_eq!(err, AuthMaterializationErrorCode::AuthSandboxMountFail);
}
