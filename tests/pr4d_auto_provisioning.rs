#![cfg(unix)]

use ah::error::CcbdError;
use ah::home_materialization::{
    HomeLayoutRole, prepare_home_layout_with_extensions_for_slot_and_claude_credentials,
};
use ah::provider::extensions::ExtensionConfig;
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn id_only_plugin_unchanged_regression() {
    let fixture = HostFixture::new();
    fixture.install_claude_plugin("github@openai-curated");
    let fake_git = fixture.install_fake_git(FakeGitMode::Success);
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Worker,
        "worker",
        &plugins_config(["github@openai-curated"]),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    assert!(
        fake_git.invocations().is_empty(),
        "ID-only plugins must not invoke git clone"
    );
    assert!(
        !fixture.git_cache_root().exists(),
        "ID-only plugins must not create PR4d git cache"
    );
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    assert_eq!(
        settings["enabledPlugins"]["github@openai-curated"], true,
        "ID-only plugin must still be enabled through the PR4c settings path"
    );
    assert!(
        layout
            .home_root
            .join(".claude/plugins/github@openai-curated")
            .exists(),
        "PR4d should expose the logical plugin name directly under .claude/plugins/"
    );
}

#[test]
fn git_url_plugin_first_install_clones_and_symlinks() {
    let fixture = HostFixture::new();
    let fake_git = fixture.install_fake_git(FakeGitMode::Success);
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Worker,
        "worker",
        &plugins_config(["rust-expert@git@github.com:foo/bar.git#main"]),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    let cache = fixture.git_cache_root().join("github.com/foo/bar/main");
    assert!(
        cache.join("plugin.json").is_file(),
        "first install should clone worktree content into {}",
        cache.display()
    );
    assert!(
        fake_git
            .invocations()
            .iter()
            .any(|line| line.contains("clone") && line.contains("github.com:foo/bar.git")),
        "git clone subprocess should be called for a git-enabled plugin"
    );
    assert_eq!(
        std::fs::read_link(layout.home_root.join(".claude/plugins/rust-expert")).unwrap(),
        cache
    );
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    assert_eq!(settings["enabledPlugins"]["rust-expert"], true);
}

#[test]
fn git_url_plugin_cache_hit_skips_clone() {
    let fixture = HostFixture::new();
    let fake_git = fixture.install_fake_git(FakeGitMode::Success);
    let cache = fixture.git_cache_root().join("github.com/foo/bar/main");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(cache.join("plugin.json"), "{}\n").unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Worker,
        "worker",
        &plugins_config(["rust-expert@git@github.com:foo/bar.git#main"]),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    assert!(
        fake_git.invocations().is_empty(),
        "cache hit must skip git clone"
    );
    assert_eq!(
        std::fs::read_link(layout.home_root.join(".claude/plugins/rust-expert")).unwrap(),
        cache
    );
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    assert_eq!(settings["enabledPlugins"]["rust-expert"], true);
}

#[test]
fn git_url_plugin_clone_fail_barrier_blocks_start() {
    let fixture = HostFixture::new();
    let fake_git = fixture.install_fake_git(FakeGitMode::Fail);
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let result = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Worker,
        "worker",
        &plugins_config(["bad@git@nonexistent.invalid:foo/bar.git#main"]),
        None,
        Some(shared_credentials_dir.path()),
    );

    assert!(
        matches!(result, Err(CcbdError::EnvironmentNotSupported { .. })),
        "clone failure must block startup with a typed environment error: {result:?}"
    );
    assert!(
        fake_git
            .invocations()
            .iter()
            .any(|line| line.contains("nonexistent.invalid:foo/bar.git")),
        "barrier must come from the PR4d git clone path, not the legacy local-cache lookup"
    );
    assert!(
        !sandbox.path().join(".claude/plugins/bad").exists(),
        "failed provisioning must not leave a sandbox plugin symlink"
    );
    assert!(
        find_tmp_dirs(&fixture.git_cache_root()).is_empty(),
        "failed provisioning must clean temporary clone directories"
    );
}

#[test]
fn git_url_plugin_ssh_private_repo_parses_and_inherits_credentials() {
    let fixture = HostFixture::new();
    let fake_git = fixture.install_fake_git(FakeGitMode::Success);
    fixture.set_ssh_auth_sock("/tmp/ccbd-pr4d-agent.sock");
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        HomeLayoutRole::Worker,
        "worker",
        &plugins_config(["legacy-mod@git@git@internal.com:ops/mod.git#v1.2"]),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    let invocations = fake_git.invocations();
    assert!(
        invocations
            .iter()
            .any(|line| line.contains("git@internal.com:ops/mod.git")),
        "SSH URL must keep the inner git@host delimiter intact: {invocations:?}"
    );
    assert!(
        invocations
            .iter()
            .any(|line| line.contains("SSH_AUTH_SOCK=/tmp/ccbd-pr4d-agent.sock")),
        "git subprocess must inherit SSH_AUTH_SOCK: {invocations:?}"
    );
    assert_eq!(
        std::fs::read_link(layout.home_root.join(".claude/plugins/legacy-mod")).unwrap(),
        fixture.git_cache_root().join("internal.com/ops/mod/v1.2")
    );
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    assert_eq!(settings["enabledPlugins"]["legacy-mod"], true);
    assert!(
        !layout.home_root.join(".ssh").exists(),
        "SSH credentials must stay on the host and not be copied into sandbox HOME"
    );
}

fn plugins_config<const N: usize>(plugins: [&str; N]) -> ExtensionConfig {
    ExtensionConfig {
        hooks: HashMap::new(),
        plugins: plugins.into_iter().map(str::to_string).collect(),
        skills: Vec::new(),
        ..Default::default()
    }
}

fn read_json(path: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn find_tmp_dirs(root: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if !root.exists() {
        return dirs;
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".tmp"))
                {
                    dirs.push(path.clone());
                }
                stack.push(path);
            }
        }
    }
    dirs
}

struct FakeGit {
    log_path: PathBuf,
}

impl FakeGit {
    fn invocations(&self) -> Vec<String> {
        std::fs::read_to_string(&self.log_path)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }
}

#[derive(Clone, Copy)]
enum FakeGitMode {
    Success,
    Fail,
}

struct HostFixture {
    _lock: MutexGuard<'static, ()>,
    _host_home: tempfile::TempDir,
    _cache_home: tempfile::TempDir,
    _bin_dir: tempfile::TempDir,
    host_home_path: PathBuf,
    cache_home_path: PathBuf,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
    old_path: Option<OsString>,
    old_user: Option<OsString>,
    old_ssh_auth_sock: Option<OsString>,
}

impl HostFixture {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let host_home = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        let bin_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(host_home.path().join(".claude/plugins/cache")).unwrap();
        std::fs::create_dir_all(host_home.path().join(".codex/plugins/cache")).unwrap();
        std::fs::write(host_home.path().join(".claude.json"), "{}\n").unwrap();

        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        let old_path = std::env::var_os("PATH");
        let old_user = std::env::var_os("USER");
        let old_ssh_auth_sock = std::env::var_os("SSH_AUTH_SOCK");
        unsafe {
            std::env::set_var("HOME", host_home.path());
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
            std::env::set_var("USER", "__ccbd_pr4d_no_passwd_user__");
            std::env::remove_var("SSH_AUTH_SOCK");
        }

        Self {
            _lock: lock,
            host_home_path: host_home.path().to_path_buf(),
            cache_home_path: cache_home.path().to_path_buf(),
            _host_home: host_home,
            _cache_home: cache_home,
            _bin_dir: bin_dir,
            old_home,
            old_cache,
            old_path,
            old_user,
            old_ssh_auth_sock,
        }
    }

    fn install_claude_plugin(&self, name: &str) -> PathBuf {
        let path = self.host_home_path.join(".claude/plugins/cache").join(name);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("plugin.json"), "{}\n").unwrap();
        path
    }

    fn git_cache_root(&self) -> PathBuf {
        self.cache_home_path.join("ah/cache/git")
    }

    fn install_fake_git(&self, mode: FakeGitMode) -> FakeGit {
        let log_path = self.cache_home_path.join("fake-git.log");
        let script_path = self._bin_dir.path().join("git");
        let exit_code = match mode {
            FakeGitMode::Success => 0,
            FakeGitMode::Fail => 44,
        };
        let script = format!(
            r#"#!/usr/bin/env bash
set -eu
printf 'SSH_AUTH_SOCK=%s args=%s\n' "${{SSH_AUTH_SOCK:-}}" "$*" >> "{log}"
dest="${{@:$#}}"
if [ "{exit_code}" -ne 0 ]; then
  exit "{exit_code}"
fi
mkdir -p "$dest"
printf '{{}}\n' > "$dest/plugin.json"
"#,
            log = log_path.display(),
            exit_code = exit_code,
        );
        std::fs::write(&script_path, script).unwrap();
        let mut permissions = std::fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script_path, permissions).unwrap();

        let old_path = self.old_path.clone().unwrap_or_default();
        let mut path = OsString::from(self._bin_dir.path());
        path.push(":");
        path.push(old_path);
        unsafe {
            std::env::set_var("PATH", path);
        }
        FakeGit { log_path }
    }

    fn set_ssh_auth_sock(&self, value: &str) {
        unsafe {
            std::env::set_var("SSH_AUTH_SOCK", value);
        }
    }
}

impl Drop for HostFixture {
    fn drop(&mut self) {
        restore_env("HOME", self.old_home.as_ref());
        restore_env("XDG_CACHE_HOME", self.old_cache.as_ref());
        restore_env("PATH", self.old_path.as_ref());
        restore_env("USER", self.old_user.as_ref());
        restore_env("SSH_AUTH_SOCK", self.old_ssh_auth_sock.as_ref());
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
