use ah::home_materialization::{
    HomeLayoutRole, compose_rules_with_layers,
    prepare_home_layout_with_extensions_for_slot_and_claude_credentials,
};
use ah::provider::builtin;
use ah::provider::extensions::ExtensionConfig;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    _home: tempfile::TempDir,
    _cache: tempfile::TempDir,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
    old_user: Option<OsString>,
}

impl EnvGuard {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let home = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        let old_user = std::env::var_os("USER");
        unsafe {
            std::env::set_var("HOME", home.path());
            std::env::set_var("XDG_CACHE_HOME", cache.path());
            std::env::set_var("USER", "dev-scenario-test");
        }
        Self {
            _lock: lock,
            _home: home,
            _cache: cache,
            old_home,
            old_cache,
            old_user,
        }
    }
}

impl Drop for EnvGuard {
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

fn scenario_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/work_execution/scenario_packages/dev-programming")
}

fn rules_file(slot: &str) -> PathBuf {
    scenario_root().join(".ah/rules").join(format!("{slot}.md"))
}

fn read_slot(slot: &str) -> String {
    std::fs::read_to_string(rules_file(slot)).unwrap()
}

fn copy_dir_all(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let source = entry.path();
        let target = to.join(entry.file_name());
        if source.is_dir() {
            copy_dir_all(&source, &target);
        } else {
            std::fs::copy(&source, &target).unwrap();
        }
    }
}

fn provider_destination(provider: &str) -> &'static str {
    match provider {
        "claude" => ".claude/CLAUDE.md",
        "codex" => ".codex/AGENTS.md",
        "antigravity" => ".gemini/AGENTS.md",
        other => panic!("unexpected provider {other}"),
    }
}

fn slot_sentinel(slot: &str) -> &'static str {
    match slot {
        "master" => "PM/CEO-lite",
        "a1" => "When assigned to IMPLEMENT",
        "a2" => "When assigned to IMPLEMENT",
        "a3" => "You do design, architecture, and domain analysis",
        "a4" => "Second review / audit",
        other => panic!("unexpected slot {other}"),
    }
}

fn kernel_forbidden_fragments() -> Vec<String> {
    [
        (builtin::MASTER_KERNEL, "ah master ack-ready"),
        (builtin::MASTER_KERNEL, "ah ask"),
        (builtin::MASTER_KERNEL, "killing ah-managed"),
        (builtin::WORKER_KERNEL, "Never self-dispatch"),
        (builtin::WORKER_KERNEL, "host system paths"),
        (builtin::WORKER_KERNEL, "Never bypass OAuth"),
    ]
    .into_iter()
    .map(|(kernel, needle)| {
        kernel
            .lines()
            .find(|line| line.contains(needle))
            .unwrap_or_else(|| panic!("kernel line containing {needle:?} must exist"))
            .to_string()
    })
    .collect()
}

#[test]
#[serial_test::serial(dev_scenario_template_env)]
fn dev_programming_scenario_materializes_all_slots_to_provider_rules_targets() {
    let _env = EnvGuard::new();
    let project = tempfile::tempdir().unwrap();
    copy_dir_all(&scenario_root().join(".ah"), &project.path().join(".ah"));

    let config: toml::Value = std::fs::read_to_string(scenario_root().join("ah.toml"))
        .unwrap()
        .parse()
        .unwrap();
    let mut slots = Vec::new();
    slots.push((
        "master".to_string(),
        config["master"]["cmd"].as_str().unwrap().to_string(),
        HomeLayoutRole::Master,
    ));
    let agents = config["agents"].as_table().unwrap();
    for slot in ["a1", "a2", "a3", "a4"] {
        slots.push((
            slot.to_string(),
            agents[slot]["provider"].as_str().unwrap().to_string(),
            HomeLayoutRole::Worker,
        ));
    }

    for (slot, provider, role) in slots {
        let sandbox = tempfile::tempdir().unwrap();
        let shared_credentials_dir = (provider == "claude")
            .then(tempfile::tempdir)
            .transpose()
            .unwrap();
        let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
            &provider,
            sandbox.path(),
            project.path(),
            role,
            &slot,
            &ExtensionConfig::default(),
            None,
            shared_credentials_dir.as_ref().map(|dir| dir.path()),
        )
        .unwrap();
        let rules = std::fs::read_to_string(layout.home_root.join(provider_destination(&provider)))
            .unwrap();
        let kernel = match role {
            HomeLayoutRole::Master => builtin::MASTER_KERNEL,
            HomeLayoutRole::Worker => builtin::WORKER_KERNEL,
        };
        assert!(
            rules.starts_with(kernel.trim_end()),
            "{slot} rules must start with kernel"
        );
        assert!(
            rules.contains(slot_sentinel(&slot)),
            "{slot} rules must include scenario sentinel"
        );
    }
}

#[test]
fn dev_programming_scenario_layers_keep_kernel_bundle_slot_order() {
    let slot = read_slot("a1");
    let bundle = "sentinel bundle layer";
    let composed = compose_rules_with_layers(builtin::WORKER_KERNEL, &[bundle.to_string()], &slot);
    let kernel_pos = composed.find(builtin::WORKER_KERNEL.trim_end()).unwrap();
    let bundle_pos = composed.find(bundle).unwrap();
    let slot_pos = composed.find("When assigned to IMPLEMENT").unwrap();
    assert!(
        kernel_pos < bundle_pos && bundle_pos < slot_pos,
        "expected kernel < bundle < slot ordering"
    );
}

#[test]
fn dev_programming_scenario_layers_compose_all_slot_sentinels() {
    for slot in ["master", "a1", "a2", "a3", "a4"] {
        let body = read_slot(slot);
        let kernel = if slot == "master" {
            builtin::MASTER_KERNEL
        } else {
            builtin::WORKER_KERNEL
        };
        let composed = compose_rules_with_layers(kernel, &[], &body);
        assert!(composed.starts_with(kernel.trim_end()));
        assert!(composed.contains(slot_sentinel(slot)));
    }
}

#[test]
fn dev_programming_scenario_layers_do_not_duplicate_builtin_kernel_text() {
    let forbidden = kernel_forbidden_fragments();
    for slot in ["master", "a1", "a2", "a3", "a4"] {
        let body = read_slot(slot);
        for forbidden in &forbidden {
            assert!(
                !body.contains(forbidden),
                "{slot}.md must not duplicate kernel phrase {forbidden:?}"
            );
        }
    }
}

#[test]
fn dev_programming_codex_slots_are_byte_identical() {
    let a1 = std::fs::read(rules_file("a1")).unwrap();
    let a2 = std::fs::read(rules_file("a2")).unwrap();
    assert_eq!(a1, a2);
}
