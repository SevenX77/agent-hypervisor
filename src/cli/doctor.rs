use crate::cli::rpc_client::{CliError, RpcClient};
use crate::provider::manifest::{ProviderManifest, known_provider_manifests};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorStatus {
    Pass,
    Fail,
    Warn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCheck {
    pub name: String,
    pub status: DoctorStatus,
    pub detail: String,
    pub suggestion: Option<String>,
}

pub async fn run_doctor(
    client: &impl RpcClient,
    project_dir: Option<&Path>,
) -> Result<Vec<DoctorCheck>, CliError> {
    let mut checks = Vec::new();
    checks.extend(system_binary_checks());
    checks.extend(crate::cli::wsl::wsl_onboarding_checks());
    checks.push(daemon_check(client).await);
    checks.push(check_tmux_orphans());
    checks.push(check_legacy_shared_session());
    if let Some(project_dir) = project_dir {
        checks.push(check_legacy_repo_state(project_dir));
    }
    checks.extend(provider_health_checks(home_dir()));
    if let Some(project_dir) = project_dir {
        checks.extend(permission_checks(project_dir));
    }
    Ok(checks)
}

pub fn system_binary_checks() -> Vec<DoctorCheck> {
    ["tmux", "systemd-run"]
        .into_iter()
        .map(|binary| match which::which(binary) {
            Ok(path) => pass(format!("binary:{binary}"), path.display().to_string()),
            Err(_) => fail(
                format!("binary:{binary}"),
                "not found in PATH",
                format!("install {binary} or adjust PATH"),
            ),
        })
        .collect()
}

async fn daemon_check(client: &impl RpcClient) -> DoctorCheck {
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.call("system.dump", json!({})),
    )
    .await
    {
        Ok(Ok(_)) => pass("daemon", "ccbd responded to system.dump"),
        Ok(Err(err)) => fail(
            "daemon",
            err.to_string(),
            "start it with scripts/start-daemon.sh",
        ),
        Err(_) => fail(
            "daemon",
            "timed out after 5s",
            "check ahd logs and socket path",
        ),
    }
}

pub fn provider_health_checks(home: Option<PathBuf>) -> Vec<DoctorCheck> {
    let Some(home) = home else {
        return vec![warn(
            "provider:home",
            "home directory unavailable",
            "set HOME before running provider checks",
        )];
    };
    known_provider_manifests()
        .into_iter()
        .map(|manifest| provider_auth_check(&home, &manifest))
        .collect()
}

/// Per-provider auth diagnosis with a printed remedy (#4, decision 0006).
///
/// The old check only asked "does some auth directory exist", which passed a
/// store that was logged out, unparseable, or — the dangerous case — a
/// symlink reaching into the Windows environment, where one refresh forks the
/// token chain and kills a login.
fn provider_auth_check(home: &Path, manifest: &ProviderManifest) -> DoctorCheck {
    use crate::provider::auth_store::{AuthStoreStatus, check_auth_store, login_remedy};

    let provider = manifest.provider_name;
    let name = format!("provider:{provider}");
    if manifest.auth_mount_paths.is_empty() {
        return pass(name, "no login required".to_string());
    }
    match check_auth_store(provider, home, None) {
        AuthStoreStatus::Healthy { path } => pass(name, path.display().to_string()),
        AuthStoreStatus::NotCheckable { reason } => pass(name, reason.to_string()),
        AuthStoreStatus::Missing { path } => warn(
            name,
            format!("no login in this environment ({} is missing)", path.display()),
            format!("sign in: {}", login_remedy(provider)),
        ),
        AuthStoreStatus::LoggedOut { path } => warn(
            name,
            format!("logged out ({} holds a logout stub)", path.display()),
            format!("sign in: {}", login_remedy(provider)),
        ),
        AuthStoreStatus::Unreadable { path, details } => warn(
            name,
            format!("unreadable credential store ({}: {details})", path.display()),
            format!("re-create it: {}", login_remedy(provider)),
        ),
        AuthStoreStatus::ForeignEnvironment { path, target } => fail(
            name,
            format!(
                "credential store reaches into another environment ({} -> {}); one token chain \
                 cannot serve two environments — the first refresh on either side kills the other",
                path.display(),
                target.display()
            ),
            format!("rm {} && {}", path.display(), login_remedy(provider)),
        ),
    }
}

pub fn permission_checks(cwd: &Path) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    checks.push(check_read_write("permissions:cwd", cwd));
    let ccb_dir = cwd.join(".ccb");
    if ccb_dir.exists() {
        checks.push(check_read_write("permissions:.ccb", &ccb_dir));
    } else {
        checks.push(warn(
            "permissions:.ccb",
            ".ccb directory not present",
            "it will be created when daemon state is initialized",
        ));
    }
    checks
}

fn check_tmux_orphans() -> DoctorCheck {
    let mut alive = 0;
    let mut stale = 0;
    let mut timeouts = 0;

    for name in ahd_tmux_socket_names() {
        match tmux_ls_sessions(&name) {
            TmuxLsSessionsResult::Alive => alive += 1,
            TmuxLsSessionsResult::Stale => stale += 1,
            TmuxLsSessionsResult::Timeout => {
                stale += 1;
                timeouts += 1;
            }
        }
    }

    if stale == 0 {
        pass(
            "tmux server orphans",
            format!("0 stale ({alive} alive, timeouts: {timeouts})"),
        )
    } else {
        warn(
            "tmux server orphans",
            format!(
                "WARN {stale} stale ({alive} alive, timeouts: {timeouts}), run scripts/cleanup_orphan_tmux.sh"
            ),
            "run scripts/cleanup_orphan_tmux.sh",
        )
    }
}

fn check_legacy_shared_session() -> DoctorCheck {
    let socket_sessions = ahd_tmux_socket_names()
        .into_iter()
        .filter_map(|socket_name| {
            tmux_list_session_names(&socket_name)
                .ok()
                .map(|sessions| (socket_name, sessions))
        })
        .collect::<Vec<_>>();
    legacy_shared_session_check_from_sessions(&socket_sessions)
}

fn check_legacy_repo_state(cwd: &Path) -> DoctorCheck {
    let legacy = std::fs::read_dir(cwd)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            name.starts_with(".ccb-rs").then_some(name)
        })
        .collect::<Vec<_>>();

    if legacy.is_empty() {
        pass("legacy repo state", "0 .ccb-rs* entries")
    } else {
        warn(
            "legacy repo state",
            format!("found repo-local state entries: {}", legacy.join(", ")),
            "remove stale repo-local state with rm -rf .ccb-rs*",
        )
    }
}

pub fn legacy_shared_session_check_from_sessions(
    socket_sessions: &[(String, Vec<String>)],
) -> DoctorCheck {
    const LEGACY_SHARED_SESSION: &str = "ccbd-agents";
    let legacy_sockets = socket_sessions
        .iter()
        .filter_map(|(socket_name, sessions)| {
            sessions
                .iter()
                .any(|session| session == LEGACY_SHARED_SESSION)
                .then_some(socket_name.as_str())
        })
        .collect::<Vec<_>>();

    if legacy_sockets.is_empty() {
        pass(
            "tmux legacy shared session",
            "0 legacy ccbd-agents sessions",
        )
    } else {
        warn(
            "tmux legacy shared session",
            format!(
                "found legacy ccbd-agents session on socket(s): {}",
                legacy_sockets.join(", ")
            ),
            format!("tmux -L {} kill-session -t ccbd-agents", legacy_sockets[0]),
        )
    }
}

fn ahd_tmux_socket_names() -> Vec<String> {
    #[cfg(not(unix))]
    {
        Vec::new()
    }
    #[cfg(unix)]
    {
        let socket_dir = format!("/tmp/tmux-{}", unsafe { libc::geteuid() });
        let Ok(entries) = std::fs::read_dir(socket_dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                (name.starts_with("ahd-") || name.starts_with("ccbd-")).then_some(name)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TmuxLsSessionsResult {
    Alive,
    Stale,
    Timeout,
}

fn tmux_ls_sessions(socket_name: &str) -> TmuxLsSessionsResult {
    match tmux_list_session_names(socket_name) {
        Ok(_) => return TmuxLsSessionsResult::Alive,
        Err(TmuxSessionListError::Timeout) => return TmuxLsSessionsResult::Timeout,
        Err(TmuxSessionListError::Stale) => {}
    }
    TmuxLsSessionsResult::Stale
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TmuxSessionListError {
    Stale,
    Timeout,
}

fn tmux_list_session_names(socket_name: &str) -> Result<Vec<String>, TmuxSessionListError> {
    let output = Command::new("timeout")
        .args([
            "2s",
            "tmux",
            "-L",
            socket_name,
            "ls-sessions",
            "-F",
            "#{session_name}",
        ])
        .output();
    match output {
        Ok(output) if output.status.success() => Ok(parse_tmux_session_names(&output.stdout)),
        Ok(output) if output.status.code() == Some(124) => Err(TmuxSessionListError::Timeout),
        Ok(output) if String::from_utf8_lossy(&output.stderr).contains("unknown command") => {
            let output = Command::new("timeout")
                .args([
                    "2s",
                    "tmux",
                    "-L",
                    socket_name,
                    "list-sessions",
                    "-F",
                    "#{session_name}",
                ])
                .output();
            match output {
                Ok(output) if output.status.success() => {
                    Ok(parse_tmux_session_names(&output.stdout))
                }
                Ok(output) if output.status.code() == Some(124) => {
                    Err(TmuxSessionListError::Timeout)
                }
                _ => Err(TmuxSessionListError::Stale),
            }
        }
        _ => Err(TmuxSessionListError::Stale),
    }
}

fn parse_tmux_session_names(stdout: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub fn print_doctor(checks: &[DoctorCheck]) {
    for check in checks {
        let (mark, color) = match check.status {
            DoctorStatus::Pass => ("✓", "\x1b[32m"),
            DoctorStatus::Fail => ("✗", "\x1b[31m"),
            DoctorStatus::Warn => ("!", "\x1b[33m"),
        };
        println!("{color}{mark}\x1b[0m {} - {}", check.name, check.detail);
        if let Some(suggestion) = &check.suggestion {
            println!("  suggestion: {suggestion}");
        }
    }
}

pub fn has_failures(checks: &[DoctorCheck]) -> bool {
    checks
        .iter()
        .any(|check| check.status == DoctorStatus::Fail)
}

fn check_read_write(name: &'static str, path: &Path) -> DoctorCheck {
    if !path.exists() {
        return fail(
            name,
            "path does not exist",
            "create the path or choose another cwd",
        );
    }
    let readable = std::fs::read_dir(path).is_ok();
    let probe = path.join(".ccb-doctor-write-test");
    let writable = std::fs::write(&probe, b"ok")
        .and_then(|_| std::fs::remove_file(&probe))
        .is_ok();
    match (readable, writable) {
        (true, true) => pass(name, path.display().to_string()),
        (false, true) => fail(name, "not readable", "fix directory read permissions"),
        (true, false) => fail(name, "not writable", "fix directory write permissions"),
        (false, false) => fail(
            name,
            "not readable or writable",
            "fix directory permissions",
        ),
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn pass(name: impl Into<String>, detail: impl Into<String>) -> DoctorCheck {
    DoctorCheck {
        name: name.into(),
        status: DoctorStatus::Pass,
        detail: detail.into(),
        suggestion: None,
    }
}

fn fail(
    name: impl Into<String>,
    detail: impl Into<String>,
    suggestion: impl Into<String>,
) -> DoctorCheck {
    DoctorCheck {
        name: name.into(),
        status: DoctorStatus::Fail,
        detail: detail.into(),
        suggestion: Some(suggestion.into()),
    }
}

fn warn(
    name: impl Into<String>,
    detail: impl Into<String>,
    suggestion: impl Into<String>,
) -> DoctorCheck {
    DoctorCheck {
        name: name.into(),
        status: DoctorStatus::Warn,
        detail: detail.into(),
        suggestion: Some(suggestion.into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::provider::manifest::known_provider_manifests;

    use super::{
        DoctorStatus, check_legacy_repo_state, legacy_shared_session_check_from_sessions,
        parse_tmux_session_names, permission_checks, provider_health_checks, system_binary_checks,
    };
    use crate::cli::rpc_client::{CliError, RpcClient, RpcFuture};
    use serde_json::Value;

    #[test]
    fn test_provider_health_missing_home_warns() {
        let checks = provider_health_checks(None);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
    }

    #[test]
    fn test_provider_health_checks_match_known_provider_manifests() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut actual = provider_health_checks(Some(tmp.path().to_path_buf()))
            .into_iter()
            .map(|check| check.name)
            .collect::<Vec<_>>();
        actual.sort();

        let mut expected = known_provider_manifests()
            .into_iter()
            .map(|manifest| format!("provider:{}", manifest.provider_name))
            .collect::<Vec<_>>();
        expected.sort();

        assert_eq!(actual, expected);
        assert!(actual.contains(&"provider:antigravity".to_string()));
        assert!(!actual.contains(&"provider:gemini".to_string()));
        assert!(!actual.contains(&"provider:anthropic".to_string()));
    }

    #[test]
    fn test_permission_checks_current_tempdir_passes() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join(".ccb")).unwrap();
        let checks = permission_checks(tmp.path());
        assert!(
            checks
                .iter()
                .all(|check| check.status == DoctorStatus::Pass)
        );
    }

    #[derive(Clone)]
    struct FailingClient;

    impl RpcClient for FailingClient {
        fn call<'a>(&'a self, _method: &'a str, _params: Value) -> RpcFuture<'a> {
            Box::pin(async { Err(CliError::Config("expected test daemon failure".to_string())) })
        }
    }

    #[tokio::test]
    async fn run_doctor_without_project_dir_skips_cwd_permission_checks() {
        let checks = super::run_doctor(&FailingClient, None).await.unwrap();

        assert!(!checks.iter().any(|check| check.name == "permissions:cwd"));
        assert!(!checks.iter().any(|check| check.name == "permissions:.ccb"));
    }

    #[test]
    fn test_system_binary_checks_do_not_require_bwrap() {
        let checks = system_binary_checks();
        assert!(!checks.iter().any(|check| check.name == "binary:bwrap"));
    }

    #[test]
    fn test_legacy_shared_session_check_warns_when_ccbd_agents_exists() {
        let socket_sessions = vec![(
            "ccbd-test".to_string(),
            vec!["agent_a1".to_string(), "ccbd-agents".to_string()],
        )];
        let check = legacy_shared_session_check_from_sessions(&socket_sessions);

        assert_eq!(check.status, DoctorStatus::Warn);
        assert!(check.detail.contains("ccbd-test"));
        assert_eq!(
            check.suggestion.as_deref(),
            Some("tmux -L ccbd-test kill-session -t ccbd-agents")
        );
    }

    #[test]
    fn test_legacy_shared_session_check_passes_without_ccbd_agents() {
        let socket_sessions = vec![("ccbd-test".to_string(), vec!["agent_a1".to_string()])];
        let check = legacy_shared_session_check_from_sessions(&socket_sessions);

        assert_eq!(check.status, DoctorStatus::Pass);
    }

    #[test]
    fn test_legacy_repo_state_warns_only_for_old_ccb_rs_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join(".ah")).unwrap();
        std::fs::create_dir(tmp.path().join(".ccb-rs.aborted-123")).unwrap();

        let check = check_legacy_repo_state(tmp.path());

        assert_eq!(check.status, DoctorStatus::Warn);
        assert!(check.detail.contains(".ccb-rs.aborted-123"));
        assert!(!check.detail.contains(".ah"));
        assert_eq!(
            check.suggestion.as_deref(),
            Some("remove stale repo-local state with rm -rf .ccb-rs*")
        );
    }

    #[test]
    fn test_parse_tmux_session_names() {
        let names = parse_tmux_session_names(b"ccbd-agents\nagent_a1\n\n");
        assert_eq!(names, vec!["ccbd-agents", "agent_a1"]);
    }
}
