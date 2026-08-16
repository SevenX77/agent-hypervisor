#![cfg(unix)]

use ah::home_materialization::{
    HomeLayoutRole, prepare_home_layout, prepare_home_layout_with_claude_credentials,
    prepare_home_layout_with_role_and_claude_credentials,
};
use serde_json::json;
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn test_provider_home_layout_materialization() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let workspace_key = project_root
        .path()
        .canonicalize()
        .unwrap()
        .display()
        .to_string();

    std::fs::write(
        host_home.path().join(".claude.json"),
        serde_json::to_string_pretty(&json!({
            "trusted": true,
            "hasCompletedOnboarding": true,
            "lastOnboardingVersion": "2.1.116"
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::create_dir_all(host_home.path().join(".claude")).unwrap();
    std::fs::write(
        host_home.path().join(".claude/.credentials.json"),
        "{\"token\":\"host\"}\n",
    )
    .unwrap();
    std::fs::create_dir_all(host_home.path().join(".codex")).unwrap();
    std::fs::write(
        host_home.path().join(".codex/auth.json"),
        "{\"token\":\"codex-host\"}\n",
    )
    .unwrap();

    let _env = EnvGuard::set(host_home.path(), cache_home.path());

    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let claude = prepare_home_layout_with_claude_credentials(
        "claude",
        sandbox_root.path(),
        project_root.path(),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let claude_trust_path = claude.home_root.join(".claude.json");
    assert!(
        std::fs::symlink_metadata(&claude_trust_path)
            .unwrap()
            .file_type()
            .is_file()
    );
    let claude_trust = read_json(claude_trust_path.as_path());
    assert_eq!(claude_trust["trusted"], true);
    assert_eq!(
        claude_trust["projects"][workspace_key.as_str()]["hasTrustDialogAccepted"],
        true
    );
    assert!(claude_trust["projects"].get("/home/agent").is_none());
    let claude_config_dir_state =
        read_json(claude.home_root.join(".claude/.claude.json").as_path());
    assert_eq!(claude_config_dir_state["trusted"], true);
    assert_eq!(claude_config_dir_state["hasCompletedOnboarding"], true);
    assert_eq!(claude_config_dir_state["lastOnboardingVersion"], "2.1.116");
    let claude_settings = read_json(claude.home_root.join(".claude/settings.json").as_path());
    assert_eq!(claude_settings["skipDangerousModePermissionPrompt"], true);
    let credentials = claude.home_root.join(".claude/.credentials.json");
    assert!(
        !credentials.exists(),
        "Claude credentials must not be linked or copied into sandbox: {}",
        credentials.display()
    );
    assert_eq!(
        std::fs::read_to_string(host_home.path().join(".claude/.credentials.json")).unwrap(),
        "{\"token\":\"host\"}\n",
        "host Claude seed credential remains only in host HOME"
    );
    assert_eq!(
        claude.extra_env.get("CLAUDE_CONFIG_DIR").unwrap(),
        &claude.home_root.join(".claude").display().to_string()
    );
    assert_eq!(
        claude.extra_env.get("CLAUDE_SECURESTORAGE_CONFIG_DIR"),
        Some(&shared_credentials_dir.path().display().to_string())
    );
    assert!(!claude.extra_env.contains_key("CLAUDE_CODE_USE_GATEWAY"));
    assert!(!claude.extra_env.contains_key("ANTHROPIC_AUTH_TOKEN"));
    assert!(claude.extra_env.get("ANTHROPIC_BASE_URL").is_none());
    assert_eq!(
        claude.extra_env.get("HOME").unwrap(),
        &claude.home_root.display().to_string()
    );

    let codex = prepare_home_layout("codex", sandbox_root.path(), project_root.path()).unwrap();
    let codex_config_path = codex.home_root.join(".codex/config.toml");
    assert!(codex_config_path.is_file());
    let codex_config: toml::Value = std::fs::read_to_string(&codex_config_path)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        codex_config["projects"][workspace_key.as_str()]["trust_level"].as_str(),
        Some("trusted")
    );
    assert!(codex_config["projects"].get("/home/agent").is_none());
    assert_eq!(
        codex_config["tui"]["model_availability_nux"]["gpt-5.5"].as_integer(),
        Some(4)
    );
    let codex_auth = codex.home_root.join(".codex/auth.json");
    assert!(
        std::fs::symlink_metadata(&codex_auth)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_link(&codex_auth).unwrap(),
        host_home.path().join(".codex/auth.json")
    );
    assert_eq!(
        std::fs::read_to_string(&codex_auth).unwrap(),
        "{\"token\":\"codex-host\"}\n"
    );
    assert!(codex.home_root.join(".codex/sessions").is_dir());
    assert_eq!(
        codex.extra_env.get("CODEX_HOME").unwrap(),
        &codex.home_root.join(".codex").display().to_string()
    );
    assert_eq!(
        codex.extra_env.get("HOME").unwrap(),
        &codex.home_root.display().to_string()
    );
    assert!(!codex.extra_env.contains_key("CODEX_SESSION_ROOT"));
}

#[test]
#[serial_test::serial(global_env)]
fn antigravity_trusts_workspace_without_overwriting_settings() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let workspace_key = project_root
        .path()
        .canonicalize()
        .unwrap()
        .display()
        .to_string();
    let source_agy = host_home.path().join(".gemini/antigravity-cli");
    std::fs::create_dir_all(&source_agy).unwrap();
    std::fs::write(
        source_agy.join("settings.json"),
        serde_json::to_string_pretty(&json!({
            "colorScheme": "dark",
            "model": "test-model",
            "trustedWorkspaces": ["/host/project", "/home/agent"]
        }))
        .unwrap(),
    )
    .unwrap();
    let _env = EnvGuard::set(host_home.path(), cache_home.path());

    let antigravity =
        prepare_home_layout("antigravity", sandbox_root.path(), project_root.path()).unwrap();

    let settings = read_json(
        antigravity
            .home_root
            .join(".gemini/antigravity-cli/settings.json")
            .as_path(),
    );
    assert_eq!(settings["colorScheme"], "dark");
    assert_eq!(settings["model"], "test-model");
    let trusted = settings["trustedWorkspaces"].as_array().unwrap();
    assert!(trusted.iter().any(|value| value == "/host/project"));
    assert!(trusted.iter().any(|value| value == workspace_key.as_str()));
    assert!(!trusted.iter().any(|value| value == "/home/agent"));
    assert_eq!(
        antigravity.extra_env.get("HOME").unwrap(),
        &antigravity.home_root.display().to_string()
    );
}

#[test]
#[serial_test::serial(global_env)]
fn antigravity_oauth_token_is_shared_with_the_host_store() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let source_agy = host_home.path().join(".gemini/antigravity-cli");
    std::fs::create_dir_all(&source_agy).unwrap();
    std::fs::write(source_agy.join("antigravity-oauth-token"), "host-token\n").unwrap();
    std::fs::set_permissions(
        source_agy.join("antigravity-oauth-token"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let source_mode = 0o600;
    let _env = EnvGuard::set(host_home.path(), cache_home.path());

    let antigravity =
        prepare_home_layout("antigravity", sandbox_root.path(), project_root.path()).unwrap();

    let source = source_agy.join("antigravity-oauth-token");
    let target = antigravity
        .home_root
        .join(".gemini/antigravity-cli/antigravity-oauth-token");
    let metadata = std::fs::symlink_metadata(&target).unwrap();
    assert!(
        metadata.file_type().is_symlink(),
        "{} must share the host login store, not hold a private copy",
        target.display()
    );
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "host-token\n");
    // With one shared file there is no second copy to protect, and ah must not
    // loosen the permissions of the store the operator owns.
    assert_eq!(
        std::fs::metadata(&source).unwrap().permissions().mode() & 0o777,
        source_mode,
        "materialization must leave the host store's permissions alone"
    );

    // A private copy went stale the moment any seat refreshed its token, which
    // is what stranded antigravity seats on credentials nobody else could renew.
    // One shared file makes every refresh visible everywhere.
    std::fs::write(&source, "host-token-refreshed\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "host-token-refreshed\n",
        "the seat must see a refresh performed anywhere else"
    );
    std::fs::write(&target, "seat-refreshed\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(&source).unwrap(),
        "seat-refreshed\n",
        "a refresh by this seat must reach the host store"
    );
}

#[test]
#[serial_test::serial(global_env)]
fn antigravity_onboarding_defaults_to_complete_when_source_missing() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(host_home.path(), cache_home.path());

    let antigravity =
        prepare_home_layout("antigravity", sandbox_root.path(), project_root.path()).unwrap();

    let onboarding = read_json(
        antigravity
            .home_root
            .join(".gemini/antigravity-cli/cache/onboarding.json")
            .as_path(),
    );
    assert_eq!(onboarding["consumerOnboardingComplete"], true);
    assert_eq!(onboarding["enterpriseOnboardingComplete"], false);
    assert_eq!(onboarding["onboardingComplete"], true);
}

#[test]
#[serial_test::serial(global_env)]
fn antigravity_onboarding_copies_existing_source() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let source_cache = host_home.path().join(".gemini/antigravity-cli/cache");
    std::fs::create_dir_all(&source_cache).unwrap();
    std::fs::write(
        source_cache.join("onboarding.json"),
        serde_json::to_string_pretty(&json!({
            "consumerOnboardingComplete": true,
            "enterpriseOnboardingComplete": true,
            "onboardingComplete": true,
            "sourceMarker": "host"
        }))
        .unwrap(),
    )
    .unwrap();
    let _env = EnvGuard::set(host_home.path(), cache_home.path());

    let antigravity =
        prepare_home_layout("antigravity", sandbox_root.path(), project_root.path()).unwrap();

    let onboarding = read_json(
        antigravity
            .home_root
            .join(".gemini/antigravity-cli/cache/onboarding.json")
            .as_path(),
    );
    assert_eq!(onboarding["onboardingComplete"], true);
    assert_eq!(onboarding["sourceMarker"], "host");
}

#[test]
#[serial_test::serial(global_env)]
fn builtin_system_layer_materializes_master_rules_for_master() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(host_home.path(), cache_home.path());
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let master = prepare_home_layout_with_role_and_claude_credentials(
        "claude",
        sandbox_root.path(),
        project_root.path(),
        HomeLayoutRole::Master,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let rules = std::fs::read_to_string(master.home_root.join(".claude/CLAUDE.md")).unwrap();

    assert!(rules.contains("ah Master Coordination Kernel"));
    assert!(rules.contains("Default ah Master Scenario"));
    assert!(!rules.contains("ah Worker Coordination Kernel"));
}

#[test]
#[serial_test::serial(global_env)]
fn builtin_system_layer_materializes_worker_rules_for_all_worker_providers() {
    let host_home = tempfile::tempdir().unwrap();
    let cache_home = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(host_home.path(), cache_home.path());

    let claude_sandbox = tempfile::tempdir().unwrap();
    let codex_sandbox = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let claude = prepare_home_layout_with_claude_credentials(
        "claude",
        claude_sandbox.path(),
        project_root.path(),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let codex = prepare_home_layout("codex", codex_sandbox.path(), project_root.path()).unwrap();

    for rules_path in [
        claude.home_root.join(".claude/CLAUDE.md"),
        codex.home_root.join(".codex/AGENTS.md"),
    ] {
        let rules = std::fs::read_to_string(&rules_path).unwrap();
        assert!(
            rules.contains("ah Worker Coordination Kernel"),
            "{} missing worker rules",
            rules_path.display()
        );
        assert!(rules.contains("Grep-before-claim"));
        assert!(rules.contains("Default ah Worker Scenario"));
        assert!(!rules.contains("ah Master Coordination Kernel"));
    }
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
}

impl EnvGuard {
    fn set(home: &Path, cache_home: &Path) -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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

fn restore_env(key: &str, old_value: Option<&OsString>) {
    unsafe {
        match old_value {
            Some(old_value) => std::env::set_var(key, old_value),
            None => std::env::remove_var(key),
        }
    }
}
