mod common;

use ah::agent_io::registry::{AgentIoEntry, cleanup_agent_runtime_resources, register};
use ah::state_layout::{StateLayoutRequest, resolve_state_layout};
use common::TmuxServerGuard;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, MutexGuard};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

fn set_env(key: &str, value: &Path) {
    unsafe {
        std::env::set_var(key, value);
    }
}

fn set_env_str(key: &str, value: &str) {
    unsafe {
        std::env::set_var(key, value);
    }
}

fn remove_env(key: &str) {
    unsafe {
        std::env::remove_var(key);
    }
}

fn clear_state_env() {
    for key in [
        "AH_STATE_DIR",
        "XDG_STATE_HOME",
        "AH_ENV",
        "AH_SOCKET",
        "AH_RS_STATE_HOME",
    ] {
        remove_env(key);
    }
}

fn write_config(dir: &Path) {
    std::fs::write(
        dir.join("ah.toml"),
        r#"
version = "1"

[agents.a1]
provider = "codex"
"#,
    )
    .expect("write ah.toml");
}

fn project_id_for(dir: &Path) -> String {
    let canonical = dir.canonicalize().expect("canonical project dir");
    let mut hasher = Sha256::new();
    hasher.update(canonical.display().to_string().as_bytes());
    format!("{:x}", hasher.finalize())[..8].to_string()
}

fn expected_project_state(home: &Path, config_dir: &Path) -> PathBuf {
    home.join(".local")
        .join("state")
        .join("ah")
        .join(project_id_for(config_dir))
}

fn resolve(cwd: &Path, config_path: Option<PathBuf>) -> ah::state_layout::StateLayout {
    resolve_state_layout(&StateLayoutRequest {
        cwd: cwd.to_path_buf(),
        config_path,
    })
}

#[test]
fn repo_cwd_with_ah_toml_resolves_to_project_xdg_state_not_repo_ah_rs() {
    let _guard = env_lock();
    clear_state_env();
    let home = tempfile::tempdir().unwrap();
    set_env("HOME", home.path());

    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        repo.join("ah.toml").is_file(),
        "fixture repo must have ah.toml"
    );

    let layout = resolve(repo, None);
    let expected = expected_project_state(home.path(), repo);

    assert_eq!(
        layout.state_dir, expected,
        "cwd=repo with ah.toml must use per-project ~/.local/state/ah/<project_id>, not ProjectDirs ah or repo-local state"
    );
    assert!(
        !layout.state_dir.starts_with(repo.join(".ah")),
        "state_dir must not be inside repo .ah: {}",
        layout.state_dir.display()
    );
}

#[test]
fn different_config_dirs_get_different_stable_state_dirs_and_socket_names() {
    let _guard = env_lock();
    clear_state_env();
    let home = tempfile::tempdir().unwrap();
    set_env("HOME", home.path());

    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    write_config(left.path());
    write_config(right.path());

    let left_first = resolve(left.path(), Some(left.path().join("ah.toml")));
    let left_second = resolve(left.path(), Some(left.path().join("ah.toml")));
    let right_layout = resolve(right.path(), Some(right.path().join("ah.toml")));

    assert_eq!(
        left_first.state_dir, left_second.state_dir,
        "same config dir must resolve to stable state_dir"
    );
    assert_eq!(
        left_first.project_id, left_second.project_id,
        "same config dir must resolve to stable project_id"
    );
    assert_ne!(
        left_first.state_dir, right_layout.state_dir,
        "different ah.toml directories must not share state_dir"
    );
    assert_ne!(
        ah::tmux::compute_socket_name(&left_first.state_dir),
        ah::tmux::compute_socket_name(&right_layout.state_dir),
        "different project state dirs must produce different tmux socket names"
    );
}

#[test]
fn state_layout_priority_chain_honors_explicit_overrides_before_project_discovery() {
    let _guard = env_lock();
    clear_state_env();
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let explicit = tempfile::tempdir().unwrap();
    let xdg = tempfile::tempdir().unwrap();
    write_config(project.path());
    set_env("HOME", home.path());

    set_env("AH_STATE_DIR", explicit.path());
    let layout = resolve(project.path(), Some(project.path().join("ah.toml")));
    assert_eq!(
        layout.state_dir,
        explicit.path(),
        "AH_STATE_DIR must be the highest priority state override"
    );

    remove_env("AH_STATE_DIR");
    set_env("XDG_STATE_HOME", xdg.path());
    let layout = resolve(project.path(), Some(project.path().join("ah.toml")));
    assert_eq!(
        layout.state_dir,
        xdg.path().join("ah"),
        "explicit XDG_STATE_HOME must still be honored for test isolation compatibility"
    );
}

#[test]
fn config_path_directory_wins_over_cwd_discovery_and_dev_fallback() {
    let _guard = env_lock();
    clear_state_env();
    let home = tempfile::tempdir().unwrap();
    let cwd_project = tempfile::tempdir().unwrap();
    let config_project = tempfile::tempdir().unwrap();
    write_config(cwd_project.path());
    write_config(config_project.path());
    set_env("HOME", home.path());
    set_env_str("AH_ENV", "dev");

    let layout = resolve(
        cwd_project.path(),
        Some(config_project.path().join("ah.toml")),
    );

    assert_eq!(
        layout.state_dir,
        expected_project_state(home.path(), config_project.path()),
        "--config directory must beat cwd discovery and AH_ENV=dev fallback"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pane_at_death_evidence_is_written_under_state_dir_not_relative_ah() {
    which::which("tmux").expect("tmux binary required for pane_at_death evidence test");
    let _guard = env_lock();
    let original_cwd = std::env::current_dir().unwrap();
    let temp_cwd = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp_cwd.path()).unwrap();

    let agent_id = "ag_pr1_pane_death";
    let server = TmuxServerGuard::new(state_dir.path());
    server
        .ensure_session("pr1-pane-death".to_string(), temp_cwd.path().to_path_buf())
        .await
        .expect("ensure tmux session");
    let pane = server
        .spawn_window(
            "pr1-pane-death".to_string(),
            "agent".to_string(),
            temp_cwd.path().to_path_buf(),
            vec![
                "bash".to_string(),
                "-lc".to_string(),
                "printf 'pane death marker\\n'; sleep 30".to_string(),
            ],
        )
        .await
        .expect("spawn pane");

    let pipes = state_dir.path().join("pipes");
    std::fs::create_dir_all(&pipes).unwrap();
    let fifo_path = pipes.join(format!("{agent_id}.fifo"));
    std::fs::write(&fifo_path, b"").unwrap();
    let reader_handle = tokio::spawn(async {
        std::future::pending::<()>().await;
    });
    register(
        agent_id.to_string(),
        AgentIoEntry {
            session_id: "s_pr1_pane_death".to_string(),
            pane_id: pane,
            expected_pid: None,
            reader_handle,
            fifo_path,
            socket_name: server.socket_name().to_string(),
            idle_scan_enabled: std::sync::Arc::new(AtomicBool::new(true)),
        },
    );

    cleanup_agent_runtime_resources(agent_id, Some("s_pr1_pane_death"));
    std::env::set_current_dir(original_cwd).unwrap();

    assert!(
        state_dir
            .path()
            .join("evidence")
            .join(agent_id)
            .join("pane_at_death.txt")
            .is_file(),
        "pane_at_death.txt must be written under state_dir/evidence/<agent>/, not relative .ah/agents/"
    );
    assert!(
        !temp_cwd
            .path()
            .join(".ah")
            .join("agents")
            .join(agent_id)
            .join("pane_at_death.txt")
            .exists(),
        "pane_at_death must not create a relative .ah/agents directory in daemon cwd"
    );
}
