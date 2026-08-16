#![cfg(unix)]

use ah::home_materialization::{
    HomeLayoutRole, HookPushContext, prepare_home_layout_with_extensions_for_slot,
};
use ah::provider::bundles::{BundleRole, resolve_bundles_for_provider};
use ah::provider::extensions::ExtensionConfig;
use ah::provider::fingerprint::{
    BundleDigest, ConfigFingerprintInput, ConfigRole, compute_config_hash,
};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct HostFixture {
    _lock: MutexGuard<'static, ()>,
    _host_home: tempfile::TempDir,
    _cache_home: tempfile::TempDir,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
    old_user: Option<OsString>,
}

impl HostFixture {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let host_home = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        write(
            &host_home.path().join(".codex/hooks.json"),
            r#"{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [{"type": "command", "command": "/host/existing.sh", "timeout": 9}]
      }
    ]
  }
}
"#,
        );
        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        let old_user = std::env::var_os("USER");
        unsafe {
            std::env::set_var("HOME", host_home.path());
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
            std::env::set_var("USER", "__pr3_codex_bundle_no_passwd_user__");
        }
        Self {
            _lock: lock,
            _host_home: host_home,
            _cache_home: cache_home,
            old_home,
            old_cache,
            old_user,
        }
    }
}

impl Drop for HostFixture {
    fn drop(&mut self) {
        restore_env("HOME", self.old_home.as_ref());
        restore_env("XDG_CACHE_HOME", self.old_cache.as_ref());
        restore_env("USER", self.old_user.as_ref());
    }
}

fn restore_env(key: &str, old: Option<&OsString>) {
    unsafe {
        match old {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn make_executable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }
}

fn write_codex_bundle(project: &Path) {
    let root = project.join(".ah/bundles/codex-x");
    write(
        &root.join("bundle.toml"),
        r#"name = "codex-x"
version = "1"

[skills]
include = ["s"]

[hooks]
PostToolUse = [{ matcher = "Bash", hooks = [{ type = "command", command = "hooks/guard.sh", timeout = 5 }] }]

[rules]
worker = "rules/worker.md"
"#,
    );
    write(
        &root.join("skills/s/SKILL.md"),
        "---\nname: s\ndescription: Codex bundle skill fixture.\n---\n\n# Skill S\n",
    );
    let hook = root.join("hooks/guard.sh");
    write(&hook, "#!/usr/bin/env bash\necho codex bundle hook\n");
    make_executable(&hook);
    write(
        &root.join("rules/worker.md"),
        "## codex bundle worker rules\n\n- PR-3 bundle worker rule sentinel.\n",
    );
}

fn write_master_rules_bundle(project: &Path) {
    let root = project.join(".ah/bundles/master-rules");
    write(
        &root.join("bundle.toml"),
        r#"name = "master-rules"
version = "1"

[rules]
master = "rules/master.md"
"#,
    );
    write(
        &root.join("rules/master.md"),
        "## codex master bundle rules must be rejected\n",
    );
}

fn write_project_skill(project: &Path) {
    write(
        &project.join(".ah/skills/local/SKILL.md"),
        "---\nname: local\ndescription: Local codex skill fixture.\n---\n\n# Local\n",
    );
}

fn write_project_slot_rules(project: &Path, slot_id: &str) {
    write(
        &project.join(format!(".ah/rules/{slot_id}.md")),
        "## project slot rules\n\n- Project slot rule sentinel.\n",
    );
}

fn codex_hook_ctx(sandbox: &Path, slot_id: &str) -> HookPushContext {
    HookPushContext {
        agent_id: slot_id.to_string(),
        provider: "codex".to_string(),
        lifecycle_id: format!("lifecycle-{slot_id}"),
        ahd_socket_path: sandbox.join("ahd.sock"),
        enabled: true,
    }
}

fn resolve_codex_worker(project: &Path, bundle: &str) -> ExtensionConfig {
    let base = ExtensionConfig {
        bundle: vec![bundle.to_string()],
        ..Default::default()
    };
    resolve_bundles_for_provider(project, "codex", BundleRole::Worker, &base)
        .unwrap()
        .extensions
}

fn materialize_codex_worker(
    project: &Path,
    sandbox: &Path,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_ctx: Option<&HookPushContext>,
) -> PathBuf {
    prepare_home_layout_with_extensions_for_slot(
        "codex",
        sandbox,
        project,
        HomeLayoutRole::Worker,
        slot_id,
        extensions,
        hook_ctx,
    )
    .unwrap()
    .home_root
}

fn read_json(path: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn count_ah_notify_hooks(hooks: &Value) -> usize {
    let Some(groups) = hooks["hooks"]["Stop"].as_array() else {
        return 0;
    };
    groups
        .iter()
        .flat_map(|group| group["hooks"].as_array().into_iter().flatten())
        .filter(|item| {
            item["command"]
                .as_str()
                .is_some_and(|command| command.contains("ah agent notify"))
        })
        .count()
}

#[test]
fn codex_bundle_skill_symlinks_into_codex_home() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_codex_bundle(project.path());

    let extensions = resolve_codex_worker(project.path(), "codex-x");
    let home = materialize_codex_worker(project.path(), sandbox.path(), "a1", &extensions, None);

    let skill_link = home.join(".codex/skills/s");
    assert!(
        skill_link
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "codex bundle skill must be a symlink: {}",
        skill_link.display()
    );
    let expected = project
        .path()
        .join(".ah/bundles/codex-x/skills/s")
        .canonicalize()
        .unwrap();
    assert_eq!(std::fs::read_link(&skill_link).unwrap(), expected);
    assert!(
        std::fs::read_to_string(skill_link.join("SKILL.md"))
            .unwrap()
            .contains("name: s")
    );
}

#[test]
fn codex_bundle_hooks_merge_with_existing_hooks_and_hook_push_is_idempotent() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_codex_bundle(project.path());

    let extensions = resolve_codex_worker(project.path(), "codex-x");
    let hook_ctx = codex_hook_ctx(sandbox.path(), "a1");
    let home = materialize_codex_worker(
        project.path(),
        sandbox.path(),
        "a1",
        &extensions,
        Some(&hook_ctx),
    );
    let home = materialize_codex_worker(
        project.path(),
        sandbox.path(),
        "a1",
        &extensions,
        Some(&hook_ctx),
    );

    let hooks = read_json(&home.join(".codex/hooks.json"));
    assert_eq!(
        hooks["hooks"]["PreToolUse"][0]["hooks"][0]["command"], "/host/existing.sh",
        "host codex hooks must be preserved"
    );
    let bundle_cmd = hooks["hooks"]["PostToolUse"][0]["hooks"][0]["command"]
        .as_str()
        .unwrap();
    assert!(
        bundle_cmd.ends_with(".codex/hooks/guard.sh"),
        "bundle hook command must point at codex hook symlink: {bundle_cmd}"
    );
    assert!(
        home.join(".codex/hooks/guard.sh")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "bundle hook script must be symlinked into .codex/hooks"
    );
    assert_eq!(
        count_ah_notify_hooks(&hooks),
        1,
        "hook-push must remain idempotent across repeated materialization"
    );

    let config = std::fs::read_to_string(home.join(".codex/config.toml")).unwrap();
    let parsed: toml::Value = config.parse().unwrap();
    assert_eq!(parsed["features"]["hooks"].as_bool(), Some(true));
}

#[test]
fn codex_worker_rules_order_is_kernel_then_bundle_then_project_slot() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_codex_bundle(project.path());
    write_project_slot_rules(project.path(), "a1");

    let extensions = resolve_codex_worker(project.path(), "codex-x");
    let home = materialize_codex_worker(project.path(), sandbox.path(), "a1", &extensions, None);
    let agents_md = std::fs::read_to_string(home.join(".codex/AGENTS.md")).unwrap();

    let kernel = agents_md.find("# ah Worker Coordination Kernel").unwrap();
    let bundle = agents_md.find("codex bundle worker rules").unwrap();
    let project_rules = agents_md.find("project slot rules").unwrap();
    assert!(
        kernel < bundle && bundle < project_rules,
        "codex AGENTS.md must be ordered kernel -> bundle worker rules -> project slot rules"
    );
}

#[test]
fn codex_master_bundle_rules_are_a_direct_configuration_error() {
    let project = tempfile::tempdir().unwrap();
    write_master_rules_bundle(project.path());
    let base = ExtensionConfig {
        bundle: vec!["master-rules".to_string()],
        ..Default::default()
    };

    let err = resolve_bundles_for_provider(project.path(), "codex", BundleRole::Master, &base)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("codex master bundle rules are unsupported"),
        "must hard-error directly without optional schema or deferred materialization; got: {err}"
    );
}

#[test]
fn codex_without_bundle_keeps_home_layout_skills_v1_hook_push_and_fingerprint_stable() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_codex_bundle(project.path());
    write_project_skill(project.path());

    let base = ExtensionConfig {
        skills: vec!["local".to_string()],
        ..Default::default()
    };
    let hook_ctx = codex_hook_ctx(sandbox.path(), "plain");
    let home = materialize_codex_worker(
        project.path(),
        sandbox.path(),
        "plain",
        &base,
        Some(&hook_ctx),
    );
    let overrides = prepare_home_layout_with_extensions_for_slot(
        "codex",
        sandbox.path(),
        project.path(),
        HomeLayoutRole::Worker,
        "plain",
        &base,
        Some(&hook_ctx),
    )
    .unwrap();

    assert_eq!(
        overrides.extra_env.get("CODEX_HOME"),
        Some(&overrides.home_root.join(".codex").display().to_string())
    );
    assert!(home.join(".codex/config.toml").is_file());
    assert!(
        !home.join(".codex/hooks/guard.sh").exists(),
        "unreferenced bundle must not materialize codex bundle hooks"
    );

    let local_skill = home.join(".codex/skills/local");
    assert!(
        local_skill
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "codex skills v1 must keep symlinking project skills"
    );

    let hooks = read_json(&home.join(".codex/hooks.json"));
    assert_eq!(
        count_ah_notify_hooks(&hooks),
        1,
        "codex hook-push zero-regression: exactly one ah Stop hook"
    );

    let env = HashMap::from([("CODEX_HOME".to_string(), "/tmp/codex".to_string())]);
    let hooks = HashMap::new();
    let plugins = Vec::new();
    let skills = Vec::new();
    let without_bundle = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Agent {
            provider: "codex",
            env: &env,
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &serde_json::Map::new(),
        bundle: None,
    })
    .unwrap();
    let empty_bundle = BundleDigest::default();
    let with_empty_bundle = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Agent {
            provider: "codex",
            env: &env,
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &serde_json::Map::new(),
        bundle: Some(&empty_bundle),
    })
    .unwrap();
    assert_eq!(
        without_bundle, with_empty_bundle,
        "empty bundle digest must not perturb existing fingerprint hash"
    );
}
