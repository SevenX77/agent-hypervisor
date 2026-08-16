#![cfg(unix)]

use ah::home_materialization::{
    HomeLayoutRole, HookPushContext, prepare_home_layout_with_extensions_for_slot,
};
use ah::provider::bundles::{BundleRole, resolve_bundles_for_provider};
use ah::provider::extensions::ExtensionConfig;
use serde_json::{Value, json};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct HostFixture {
    _lock: MutexGuard<'static, ()>,
    host_home: tempfile::TempDir,
    _cache_home: tempfile::TempDir,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
    old_user: Option<OsString>,
    old_token: Option<OsString>,
}

impl HostFixture {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let host_home = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        write(
            &host_home.path().join(".gemini/config/hooks.json"),
            &serde_json::to_string_pretty(&json!({
                "user-hooks": {
                    "Stop": [{
                        "type": "command",
                        "command": "/host/user-stop",
                        "timeout": 9
                    }]
                }
            }))
            .unwrap(),
        );
        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        let old_user = std::env::var_os("USER");
        let old_token = std::env::var_os("AGY_BUNDLE_TOKEN");
        unsafe {
            std::env::set_var("HOME", host_home.path());
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
            std::env::set_var("USER", "__pr4_antigravity_bundle_no_passwd_user__");
            std::env::set_var("AGY_BUNDLE_TOKEN", "resolved-secret");
        }
        Self {
            _lock: lock,
            host_home,
            _cache_home: cache_home,
            old_home,
            old_cache,
            old_user,
            old_token,
        }
    }

    fn host_home(&self) -> &Path {
        self.host_home.path()
    }
}

impl Drop for HostFixture {
    fn drop(&mut self) {
        restore_env("HOME", self.old_home.as_ref());
        restore_env("XDG_CACHE_HOME", self.old_cache.as_ref());
        restore_env("USER", self.old_user.as_ref());
        restore_env("AGY_BUNDLE_TOKEN", self.old_token.as_ref());
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

fn write_antigravity_bundle(project: &Path) {
    let root = project.join(".ah/bundles/agy-x");
    write(
        &root.join("bundle.toml"),
        r#"name = "agy-x"
version = "1"

[skills]
include = ["s"]

[hooks]
PostToolUse = [{ matcher = "Bash", hooks = [{ type = "command", command = "hooks/guard.sh", timeout = 5000 }] }]

[rules]
worker = "rules/worker.md"

[[mcp.servers]]
name = "agy-stdio"
transport = "stdio"
command = "node"
args = ["server.js"]
env = { AGY_BUNDLE_TOKEN = "${AGY_BUNDLE_TOKEN}" }
"#,
    );
    write(
        &root.join("skills/s/SKILL.md"),
        "---\nname: s\ndescription: Antigravity bundle skill fixture.\n---\n\n# Skill S\n",
    );
    let hook = root.join("hooks/guard.sh");
    write(&hook, "#!/usr/bin/env bash\necho antigravity bundle hook\n");
    make_executable(&hook);
    write(
        &root.join("rules/worker.md"),
        "## antigravity bundle worker rules\n\n- PR-4 bundle worker rule sentinel.\n",
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
        "## antigravity master bundle rules must be rejected\n",
    );
}

fn write_project_slot_rules(project: &Path, slot_id: &str) {
    write(
        &project.join(format!(".ah/rules/{slot_id}.md")),
        "## project slot rules\n\n- Project slot rule sentinel.\n",
    );
}

fn antigravity_hook_ctx(sandbox: &Path, slot_id: &str) -> HookPushContext {
    HookPushContext {
        agent_id: slot_id.to_string(),
        provider: "antigravity".to_string(),
        lifecycle_id: format!("lifecycle-{slot_id}"),
        ahd_socket_path: sandbox.join("ahd.sock"),
        enabled: true,
    }
}

fn resolve_antigravity_worker(project: &Path, bundle: &str) -> ExtensionConfig {
    let base = ExtensionConfig {
        bundle: vec![bundle.to_string()],
        ..Default::default()
    };
    resolve_bundles_for_provider(project, "antigravity", BundleRole::Worker, &base)
        .unwrap()
        .extensions
}

fn materialize_antigravity_worker(
    project: &Path,
    sandbox: &Path,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_ctx: Option<&HookPushContext>,
) -> PathBuf {
    prepare_home_layout_with_extensions_for_slot(
        "antigravity",
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
    let Some(items) = hooks["ah-completion-push"]["Stop"].as_array() else {
        return 0;
    };
    items
        .iter()
        .filter(|item| {
            item["command"]
                .as_str()
                .is_some_and(|command| command.contains("ah agent notify"))
        })
        .count()
}

#[test]
fn antigravity_bundle_skill_symlinks_into_gemini_config_skills() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_antigravity_bundle(project.path());

    let extensions = resolve_antigravity_worker(project.path(), "agy-x");
    let home =
        materialize_antigravity_worker(project.path(), sandbox.path(), "a1", &extensions, None);

    let skill_link = home.join(".gemini/config/skills/s");
    assert!(
        skill_link
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "antigravity bundle skill must be a symlink: {}",
        skill_link.display()
    );
    let expected = project
        .path()
        .join(".ah/bundles/agy-x/skills/s")
        .canonicalize()
        .unwrap();
    assert_eq!(std::fs::read_link(&skill_link).unwrap(), expected);
}

#[test]
fn antigravity_bundle_hooks_translate_to_named_hooks_and_enable_gate() {
    let fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_antigravity_bundle(project.path());

    let extensions = resolve_antigravity_worker(project.path(), "agy-x");
    let hook_ctx = antigravity_hook_ctx(sandbox.path(), "a1");
    materialize_antigravity_worker(
        project.path(),
        sandbox.path(),
        "a1",
        &extensions,
        Some(&hook_ctx),
    );
    let home = materialize_antigravity_worker(
        project.path(),
        sandbox.path(),
        "a1",
        &extensions,
        Some(&hook_ctx),
    );

    let hooks = read_json(&home.join(".gemini/config/hooks.json"));
    assert_eq!(
        hooks["user-hooks"]["Stop"][0]["command"],
        "/host/user-stop",
        "host antigravity hooks must be preserved from {}",
        fixture.host_home().display()
    );
    let bundle_cmd = hooks["ah-bundle"]["PostToolUse"][0]["hooks"][0]["command"]
        .as_str()
        .unwrap();
    assert!(
        bundle_cmd.ends_with(".gemini/config/hooks/guard.sh"),
        "bundle hook command must point at antigravity hook symlink: {bundle_cmd}"
    );
    assert_eq!(hooks["ah-bundle"]["PostToolUse"][0]["matcher"], "Bash");
    assert_eq!(
        hooks["ah-bundle"]["PostToolUse"][0]["hooks"][0]["timeout"],
        5000
    );
    assert!(
        home.join(".gemini/config/hooks/guard.sh")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "bundle hook script must be symlinked into .gemini/config/hooks"
    );
    assert_eq!(
        count_ah_notify_hooks(&hooks),
        1,
        "hook-push must stay idempotent beside bundle hooks"
    );

    let config = read_json(&home.join(".gemini/config/config.json"));
    let settings = read_json(&home.join(".gemini/config/settings.json"));
    assert_eq!(config["enableJsonHooks"], true);
    assert_eq!(settings["enableJsonHooks"], true);
}

#[test]
fn antigravity_worker_rules_order_is_kernel_then_bundle_then_project_slot() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_antigravity_bundle(project.path());
    write_project_slot_rules(project.path(), "a1");

    let extensions = resolve_antigravity_worker(project.path(), "agy-x");
    let home =
        materialize_antigravity_worker(project.path(), sandbox.path(), "a1", &extensions, None);
    let agents_md = std::fs::read_to_string(home.join(".gemini/AGENTS.md")).unwrap();

    let kernel = agents_md.find("# ah Worker Coordination Kernel").unwrap();
    let bundle = agents_md.find("antigravity bundle worker rules").unwrap();
    let project_rules = agents_md.find("project slot rules").unwrap();
    assert!(
        kernel < bundle && bundle < project_rules,
        "antigravity AGENTS.md must be ordered kernel -> bundle worker rules -> project slot rules"
    );
}

#[test]
fn antigravity_bundle_mcp_writes_mcp_config_json() {
    let _fixture = HostFixture::new();
    let project = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    write_antigravity_bundle(project.path());

    let extensions = resolve_antigravity_worker(project.path(), "agy-x");
    let home =
        materialize_antigravity_worker(project.path(), sandbox.path(), "a1", &extensions, None);
    let mcp = read_json(&home.join(".gemini/config/mcp_config.json"));

    assert_eq!(mcp["mcpServers"]["agy-stdio"]["command"], "node");
    assert_eq!(mcp["mcpServers"]["agy-stdio"]["args"][0], "server.js");
    assert_eq!(
        mcp["mcpServers"]["agy-stdio"]["env"]["AGY_BUNDLE_TOKEN"],
        "resolved-secret"
    );
}

#[test]
fn antigravity_master_bundle_rules_are_a_direct_configuration_error() {
    let project = tempfile::tempdir().unwrap();
    write_master_rules_bundle(project.path());
    let base = ExtensionConfig {
        bundle: vec!["master-rules".to_string()],
        ..Default::default()
    };

    let err =
        resolve_bundles_for_provider(project.path(), "antigravity", BundleRole::Master, &base)
            .unwrap_err()
            .to_string();

    assert!(
        err.contains("antigravity master bundle rules are unsupported"),
        "must hard-error directly without deferred materialization; got: {err}"
    );
}
