//! PR2 T1: Config Drift Check —— tests-first 红灯测试套件。
//!
//! 红线 (user 原话): "配置定向不允许出错" —— 三个 CLI 的配置目录变量必须 100% 指向
//! 物化 provider HOME (`HomeOverrides.home_root`),
//! 旧的空操作变量必须不再注入, 且 provider materialization 绝不能污染宿主真实 HOME。
//!
//! 本套件最初覆盖 T2 配置定向红线:
//!   - Claude 必须用 `CLAUDE_CONFIG_DIR` 指向物化 `.claude`;
//!   - Codex 必须用 `CODEX_HOME` 指向物化 `.codex`。
//! `host_config_*` / `host_home_file_inventory_*` 两个回归守护测试在当前实现下应为绿
//! (materialization 只写沙盒 home, 不碰宿主), 作为未来 regression 的护栏。
//!
//! hermetic: 全部用 tempfile 构造 host HOME + sandbox dir, 不读写真实 `~/.claude` /
//! `~/.codex`。本测试只验证 env 被设置, 不能证明变量名被真实 CLI 读取;
//! 真 CLI smoke (验证 `CLAUDE_CONFIG_DIR` / `CODEX_HOME` 真被识别) 走 VPS-gated,
//! 不在本文件实现 (见 tasks.md T1)。

use ah::home_materialization::{
    HomeOverrides, prepare_home_layout, prepare_home_layout_with_claude_credentials,
};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;
use std::sync::{LazyLock, Mutex, MutexGuard};

/// `HOME` / `XDG_CACHE_HOME` 是进程级全局状态, 与仓库其它测试共享。串行化本文件内的
/// 测试函数, 避免并行改 env 互相踩 (同 `tests/r1_master_exit_shutdown.rs:12` 模式)。
static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// hermetic host HOME fixture: 持锁 + 设置 `HOME`/`XDG_CACHE_HOME` 到 tempdir +
/// 写入三个 provider materialization 所需的宿主配置文件。Drop 时还原 env。
struct HostFixture {
    _lock: MutexGuard<'static, ()>,
    _host_home: tempfile::TempDir,
    _cache_home: tempfile::TempDir,
    host_home_path: std::path::PathBuf,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
}

impl HostFixture {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let host_home = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        let host = host_home.path();

        // 与 tests/mvp12_home_layout.rs 同样的宿主配置文件集合, 保证 provider 都能
        // 正常 materialize, 不因缺文件而提前 error。
        std::fs::write(host.join(".claude.json"), "{\"trusted\":true}\n").unwrap();
        std::fs::create_dir_all(host.join(".claude")).unwrap();
        std::fs::write(
            host.join(".claude/.credentials.json"),
            "{\"token\":\"host\"}\n",
        )
        .unwrap();

        std::fs::create_dir_all(host.join(".codex")).unwrap();
        std::fs::write(
            host.join(".codex/auth.json"),
            "{\"token\":\"codex-host\"}\n",
        )
        .unwrap();

        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        unsafe {
            std::env::set_var("HOME", host);
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
        }

        let host_home_path = host.to_path_buf();
        Self {
            _lock: lock,
            _host_home: host_home,
            _cache_home: cache_home,
            host_home_path,
            old_home,
            old_cache,
        }
    }

    fn host_home(&self) -> &Path {
        &self.host_home_path
    }
}

impl Drop for HostFixture {
    fn drop(&mut self) {
        unsafe {
            match &self.old_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
            match &self.old_cache {
                Some(v) => std::env::set_var("XDG_CACHE_HOME", v),
                None => std::env::remove_var("XDG_CACHE_HOME"),
            }
        }
    }
}

/// 递归快照: 相对路径 -> (mtime_secs, mtime_nanos)。用 `symlink_metadata` 不跟随软链,
/// 既能比对内容时间戳, 也能比对文件清单 (key 集合)。
fn snapshot_tree(root: &Path) -> BTreeMap<String, (u64, u32)> {
    let mut out = BTreeMap::new();
    walk_tree(root, root, &mut out);
    out
}

fn walk_tree(base: &Path, dir: &Path, out: &mut BTreeMap<String, (u64, u32)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let rel = path
            .strip_prefix(base)
            .unwrap_or(&path)
            .display()
            .to_string();
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| (d.as_secs(), d.subsec_nanos()))
            .unwrap_or((0, 0));
        out.insert(rel, mtime);
        if meta.is_dir() {
            walk_tree(base, &path, out);
        }
    }
}

/// 在同一 host fixture 下 materialize 三个 provider, 各自用独立 sandbox dir。
fn materialize_all() -> (HomeOverrides, HomeOverrides) {
    let claude_dir = tempfile::tempdir().unwrap();
    let codex_dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let claude = prepare_home_layout_with_claude_credentials(
        "claude",
        claude_dir.path(),
        workspace.path(),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let codex = prepare_home_layout("codex", codex_dir.path(), workspace.path()).unwrap();

    (claude, codex)
}

/// 红线 1: provider 配置定向必须落在物化 HOME 内。
///
/// Claude 的 `CLAUDE_CONFIG_DIR`、Codex 的 `CODEX_HOME` 指向物化子目录。
#[test]
fn config_redirect_env_points_to_sandbox_roots() {
    let _fixture = HostFixture::new();
    let (claude, codex) = materialize_all();

    assert_eq!(
        claude.extra_env.get("CLAUDE_CONFIG_DIR"),
        Some(&claude.home_root.join(".claude").display().to_string()),
        "Claude 配置目录变量 CLAUDE_CONFIG_DIR 必须指向物化 .claude 根, 严禁漏到宿主"
    );
    assert_eq!(
        codex.extra_env.get("CODEX_HOME"),
        Some(&codex.home_root.join(".codex").display().to_string()),
        "Codex 配置目录变量 CODEX_HOME 必须指向物化 .codex 根"
    );
    assert_eq!(
        claude.extra_env.get("HOME"),
        Some(&claude.home_root.display().to_string())
    );
    assert_eq!(
        codex.extra_env.get("HOME"),
        Some(&codex.home_root.display().to_string())
    );
}

/// 红线 2: 四个经实证为 CLI 不读取的空操作变量必须不再注入。
///
/// 当前实现下为红: Claude 仍注入 `CLAUDE_PROJECTS_ROOT`(带 S) + `CLAUDE_PROJECT_ROOT`(不带 S),
/// Codex 仍注入 `CODEX_SESSION_ROOT`。
#[test]
fn noop_legacy_env_vars_not_injected() {
    let _fixture = HostFixture::new();
    let (claude, codex) = materialize_all();
    let claude_env = claude.extra_env;
    let codex_env = codex.extra_env;

    assert!(
        !claude_env.contains_key("CLAUDE_PROJECTS_ROOT"),
        "空操作变量 CLAUDE_PROJECTS_ROOT (带 S) 必须不再注入, 实测仍有: {:?}",
        claude_env.get("CLAUDE_PROJECTS_ROOT")
    );
    assert!(
        !claude_env.contains_key("CLAUDE_PROJECT_ROOT"),
        "空操作变量 CLAUDE_PROJECT_ROOT (不带 S, 与上一个是两个不同旧变量) 必须不再注入, 实测仍有: {:?}",
        claude_env.get("CLAUDE_PROJECT_ROOT")
    );
    assert!(
        !codex_env.contains_key("CODEX_SESSION_ROOT"),
        "空操作变量 CODEX_SESSION_ROOT 必须不再注入, 实测仍有: {:?}",
        codex_env.get("CODEX_SESSION_ROOT")
    );
}

/// 红线 3a: Claude OAuth 凭据必须不进入 sandbox HOME。
/// Claude 走 gateway fake JWT; Codex 仍保留 host -> sandbox symlink auth 契约。
#[test]
fn provider_auth_files_follow_provider_contracts() {
    let fixture = HostFixture::new();

    let claude_dir = tempfile::tempdir().unwrap();
    let codex_dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let claude = prepare_home_layout_with_claude_credentials(
        "claude",
        claude_dir.path(),
        workspace.path(),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let codex = prepare_home_layout("codex", codex_dir.path(), workspace.path()).unwrap();

    let host_claude = fixture.host_home().join(".claude/.credentials.json");
    let sandbox_claude = claude.home_root.join(".claude/.credentials.json");
    assert!(
        !sandbox_claude.exists(),
        "Claude credentials must not be linked or copied into sandbox: {}",
        sandbox_claude.display()
    );
    assert_eq!(
        std::fs::read_to_string(&host_claude).unwrap(),
        "{\"token\":\"host\"}\n",
        "host Claude seed credential remains only in host HOME"
    );
    assert_eq!(
        claude.extra_env.get("CLAUDE_SECURESTORAGE_CONFIG_DIR"),
        Some(&shared_credentials_dir.path().display().to_string())
    );
    assert!(!claude.extra_env.contains_key("CLAUDE_CODE_USE_GATEWAY"));
    assert!(!claude.extra_env.contains_key("ANTHROPIC_AUTH_TOKEN"));
    assert!(
        claude.extra_env.get("ANTHROPIC_BASE_URL").is_none(),
        "provider home materialization should not bake a bridge URL into static env"
    );
    std::fs::write(&host_claude, "{\"token\":\"host-refreshed\"}\n").unwrap();
    assert!(
        !sandbox_claude.exists(),
        "host Claude token refresh must not appear in sandbox credentials path"
    );

    let host_codex = fixture.host_home().join(".codex/auth.json");
    let sandbox_codex = codex.home_root.join(".codex/auth.json");
    assert!(
        std::fs::symlink_metadata(&sandbox_codex)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read_link(&sandbox_codex).unwrap(), host_codex);
    std::fs::write(&host_codex, "{\"token\":\"codex-refreshed\"}\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(&sandbox_codex).unwrap(),
        "{\"token\":\"codex-refreshed\"}\n"
    );
}

/// 红线 3a: provider materialization 绝不改宿主真实配置文件的 mtime。
/// (回归守护: 当前实现写沙盒不写宿主, 预期绿。)
#[test]
fn host_config_mtime_does_not_change() {
    let fixture = HostFixture::new();
    let before = snapshot_tree(fixture.host_home());
    let _ = materialize_all();
    let after = snapshot_tree(fixture.host_home());

    assert_eq!(
        before, after,
        "provider materialization 修改了宿主 HOME 文件的 mtime, 判定为配置定向泄漏"
    );
}

/// 红线 3b: provider materialization 后宿主 HOME 文件清单必须完全相等 (无残留新文件)。
/// 覆盖 design §3.2 文件残留检测, 不只依赖 mtime 间接判断。(回归守护, 预期绿。)
#[test]
fn host_home_file_inventory_does_not_change() {
    let fixture = HostFixture::new();
    let before: Vec<String> = snapshot_tree(fixture.host_home()).into_keys().collect();
    let _ = materialize_all();
    let after: Vec<String> = snapshot_tree(fixture.host_home()).into_keys().collect();

    assert_eq!(
        before, after,
        "provider materialization 在宿主 HOME 产生/删除了文件, 判定为配置定向泄漏或残留"
    );
}
