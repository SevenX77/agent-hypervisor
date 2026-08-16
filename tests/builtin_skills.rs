#![cfg(unix)]

use ah::home_materialization::{
    HomeLayoutRole, prepare_home_layout_with_extensions_for_slot_and_claude_credentials,
};
use ah::provider::builtin;
use ah::provider::extensions::ExtensionConfig;
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
        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        let old_user = std::env::var_os("USER");
        unsafe {
            std::env::set_var("HOME", host_home.path());
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
            std::env::set_var("USER", "builtin-skills-test");
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
        unsafe {
            restore_env("HOME", &self.old_home);
            restore_env("XDG_CACHE_HOME", &self.old_cache);
            restore_env("USER", &self.old_user);
        }
    }
}

unsafe fn restore_env(key: &str, old: &Option<OsString>) {
    match old {
        Some(value) => unsafe { std::env::set_var(key, value) },
        None => unsafe { std::env::remove_var(key) },
    }
}

fn prepare(
    provider: &str,
    role: HomeLayoutRole,
    slot: &str,
) -> (tempfile::TempDir, tempfile::TempDir, PathBuf) {
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = (provider == "claude")
        .then(tempfile::tempdir)
        .transpose()
        .unwrap();
    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        provider,
        sandbox.path(),
        workspace.path(),
        role,
        slot,
        &ExtensionConfig::default(),
        None,
        shared_credentials_dir.as_ref().map(|dir| dir.path()),
    )
    .unwrap();
    (sandbox, workspace, layout.home_root)
}

fn provider_skills_dir(home_root: &Path, provider: &str) -> PathBuf {
    match provider {
        "claude" => home_root.join(".claude/skills"),
        "codex" => home_root.join(".codex/skills"),
        "antigravity" => home_root.join(".gemini/config/skills"),
        other => panic!("unexpected provider {other}"),
    }
}

fn skill_md_path(home_root: &Path, provider: &str, skill: &str) -> PathBuf {
    provider_skills_dir(home_root, provider)
        .join(skill)
        .join("SKILL.md")
}

fn skill_dir_path(home_root: &Path, provider: &str) -> PathBuf {
    skill_md_path(home_root, provider, "ah-commands")
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn master_role_gets_builtin_ah_commands_for_all_wired_providers_without_extension_declaration() {
    let _fixture = HostFixture::new();

    for (provider, slot) in [
        ("claude", "master"),
        ("codex", "master"),
        ("antigravity", "master"),
    ] {
        let (_sandbox, _workspace, home_root) = prepare(provider, HomeLayoutRole::Master, slot);
        let skill_dir = skill_dir_path(&home_root, provider);
        let dir_metadata = std::fs::symlink_metadata(&skill_dir).unwrap();
        assert!(
            !dir_metadata.file_type().is_symlink(),
            "builtin skill directory must be written into the sandbox, not symlinked"
        );

        let skill_path = skill_md_path(&home_root, provider, "ah-commands");
        let metadata = std::fs::symlink_metadata(&skill_path).unwrap();
        assert!(
            !metadata.file_type().is_symlink(),
            "builtin skill must be written into the sandbox, not symlinked"
        );
        let skill = std::fs::read_to_string(skill_path).unwrap();
        assert!(skill.contains("agent-facing"));
        assert!(skill.contains("ah ps"));
    }
}

#[test]
fn workers_do_not_get_master_only_ah_commands_builtin_skill() {
    let _fixture = HostFixture::new();

    for (provider, slot) in [("claude", "a4"), ("codex", "a1"), ("antigravity", "a3")] {
        let (_sandbox, _workspace, home_root) = prepare(provider, HomeLayoutRole::Worker, slot);
        assert!(
            !skill_md_path(&home_root, provider, "ah-commands").exists(),
            "{provider} worker {slot} must not receive master-only ah-commands"
        );
    }
}

#[test]
fn master_role_gets_self_knowledge_builtin_skills_for_all_wired_providers() {
    let _fixture = HostFixture::new();

    for skill in ["ah-config", "ah-runtime-state"] {
        for (provider, slot) in [
            ("claude", "master"),
            ("codex", "master"),
            ("antigravity", "master"),
        ] {
            let (_sandbox, _workspace, home_root) = prepare(provider, HomeLayoutRole::Master, slot);
            let skill_path = skill_md_path(&home_root, provider, skill);
            assert!(
                skill_path.exists(),
                "{provider} master must receive builtin skill {skill}"
            );
            let skill_dir = skill_path.parent().unwrap();
            let dir_metadata = std::fs::symlink_metadata(skill_dir).unwrap();
            assert!(
                !dir_metadata.file_type().is_symlink(),
                "builtin skill directory must be written into the sandbox, not symlinked"
            );
        }
    }
}

#[test]
fn workers_do_not_get_self_knowledge_builtin_skills() {
    let _fixture = HostFixture::new();

    for skill in ["ah-config", "ah-runtime-state"] {
        for (provider, slot) in [("claude", "a4"), ("codex", "a1"), ("antigravity", "a3")] {
            let (_sandbox, _workspace, home_root) = prepare(provider, HomeLayoutRole::Worker, slot);
            assert!(
                !skill_md_path(&home_root, provider, skill).exists(),
                "{provider} worker {slot} must not receive master-only {skill}"
            );
        }
    }
}

#[test]
fn master_role_gets_ah_operate_builtin_skill_for_all_wired_providers() {
    let _fixture = HostFixture::new();

    for (provider, slot) in [
        ("claude", "master"),
        ("codex", "master"),
        ("antigravity", "master"),
    ] {
        let (_sandbox, _workspace, home_root) = prepare(provider, HomeLayoutRole::Master, slot);
        let skill_path = skill_md_path(&home_root, provider, "ah-operate");
        assert!(
            skill_path.exists(),
            "{provider} master must receive builtin skill ah-operate"
        );
        let skill_dir = skill_path.parent().unwrap();
        let dir_metadata = std::fs::symlink_metadata(skill_dir).unwrap();
        assert!(
            !dir_metadata.file_type().is_symlink(),
            "builtin skill directory must be written into the sandbox, not symlinked"
        );
        let metadata = std::fs::symlink_metadata(&skill_path).unwrap();
        assert!(
            !metadata.file_type().is_symlink(),
            "builtin skill must be written into the sandbox, not symlinked"
        );
    }
}

#[test]
fn workers_do_not_get_ah_operate_builtin_skill() {
    let _fixture = HostFixture::new();

    for (provider, slot) in [("claude", "a4"), ("codex", "a1"), ("antigravity", "a3")] {
        let (_sandbox, _workspace, home_root) = prepare(provider, HomeLayoutRole::Worker, slot);
        assert!(
            !skill_md_path(&home_root, provider, "ah-operate").exists(),
            "{provider} worker {slot} must not receive master-only ah-operate"
        );
    }
}

#[test]
fn master_kernel_points_to_ah_commands_skill_instead_of_command_enumeration() {
    assert!(!builtin::MASTER_KERNEL.contains("`ah watch <agent_id>`"));
    assert!(!builtin::MASTER_KERNEL.contains("`ah logs <agent_id>`"));
    assert!(!builtin::MASTER_KERNEL.contains("`ah ps`"));
    assert!(!builtin::MASTER_KERNEL.contains("`ah attach`"));
    assert!(builtin::MASTER_KERNEL.contains("ah-commands"));
    assert!(builtin::MASTER_KERNEL.contains("ah-config"));
    assert!(builtin::MASTER_KERNEL.contains("ah-runtime-state"));
    assert!(builtin::MASTER_KERNEL.contains("ah <command> --help"));
    assert!(builtin::MASTER_KERNEL.contains("ah master ack-ready"));
    assert!(builtin::MASTER_KERNEL.contains("`ah ask"));
}

#[test]
fn ah_commands_skill_frontmatter_and_scope_are_valid() {
    let skill_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("assets/builtin/skills/ah-commands/SKILL.md");
    let skill = std::fs::read_to_string(skill_path).unwrap();
    assert!(skill.contains("name: ah-commands"));
    let description = skill
        .lines()
        .find(|line| line.starts_with("description: "))
        .unwrap();
    assert!(description.starts_with("description: Use when"));
    assert!(description.contains("wait for a dispatched job to finish"));
    assert!(description.contains("follow or retrieve a running agent's output"));
    assert!(skill.contains("agent"));
    assert!(skill.contains("dispatch"));
    for excluded in ["ah start", "ah setup", "ah bundle"] {
        assert!(
            !skill.contains(excluded),
            "ah-commands skill must not document operational command item {excluded:?}"
        );
    }
}

#[test]
fn project_skill_named_ah_commands_is_rejected_before_materialization() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let skill_dir = workspace.path().join(".ah/skills/ah-commands");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "---\nname: ah-commands\n---\n").unwrap();

    let extensions = ExtensionConfig {
        skills: vec!["ah-commands".to_string()],
        ..ExtensionConfig::default()
    };
    let err = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Master,
        "master",
        &extensions,
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("skill name \"ah-commands\" is reserved by an ah builtin skill"));
    assert!(message.contains("rename the project skill"));
}

#[test]
fn builtin_ah_commands_materialization_is_idempotent_for_prior_builtin_write() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Master,
        "master",
        &ExtensionConfig::default(),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let skill_path = skill_md_path(&layout.home_root, "claude", "ah-commands");
    std::fs::write(&skill_path, "stale builtin skill").unwrap();

    prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Master,
        "master",
        &ExtensionConfig::default(),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    let skill = std::fs::read_to_string(skill_path).unwrap();
    assert!(skill.contains("name: ah-commands"));
    assert!(skill.contains("ah ps"));
}

#[test]
fn self_knowledge_skill_frontmatter_and_source_spot_checks_are_valid() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config =
        std::fs::read_to_string(manifest_dir.join("assets/builtin/skills/ah-config/SKILL.md"))
            .unwrap();
    assert!(config.contains("name: ah-config"));
    assert!(config.contains("description: Use when"));
    assert!(config.contains("readiness_timeout_s"));
    assert!(config.contains(".claude/CLAUDE.md"));
    assert!(config.contains(".codex/AGENTS.md"));
    assert!(config.contains(".gemini/AGENTS.md"));
    assert!(config.contains("settings"));
    assert!(config.contains("only the Claude provider applies it today"));
    assert!(config.contains("There is no direct `[master].mcp`"));

    let runtime = std::fs::read_to_string(
        manifest_dir.join("assets/builtin/skills/ah-runtime-state/SKILL.md"),
    )
    .unwrap();
    assert!(runtime.contains("name: ah-runtime-state"));
    assert!(runtime.contains("description: Use when"));
    assert!(runtime.contains("runtime_state"));
    assert!(runtime.contains("worker_tmux_expected_count"));
    assert!(runtime.contains("RUNNING is not a DB enum"));
    assert!(!runtime.contains("runtime_phase"));
}

#[test]
fn ah_operate_skill_frontmatter_and_playbook_spot_checks_are_valid() {
    let skill = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/builtin/skills/ah-operate/SKILL.md"),
    )
    .unwrap();
    assert!(skill.contains("name: ah-operate"));
    assert!(skill.contains("description: Use when driving an ah master/stack"));
    assert!(skill.contains("ah tell master"));
    assert!(skill.contains("ah events --format json"));
    assert!(skill.contains("ah pend <job_id>"));
    assert!(skill.contains("PROMPT_PENDING"));
    assert!(skill.contains("status-line is rendered residue"));
}
