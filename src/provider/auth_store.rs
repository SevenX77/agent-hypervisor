//! Where each provider keeps its login on this host, and whether that login
//! belongs to this environment.
//!
//! One OAuth chain has one active environment (decision 0006). An environment
//! is a home directory on one operating system: the Windows profile, the WSL
//! distro's home and a macOS home are three different environments, and a
//! token chain shared between two of them dies — refresh tokens rotate, so the
//! side that refreshes less often ends up holding a rotated-away ancestor.
//! ah's job here is to be the doorman: detect a store that is missing, logged
//! out, or reaching into a foreign environment, and route the operator to the
//! provider's own login flow. ah never reads, writes or moves credential
//! values itself.

pub use crate::provider::AuthStoreSpec;
use std::path::{Path, PathBuf};

/// How a provider stores its login on a given operating system.
///
/// The spec is per-OS because the storage shape is: on Linux every provider
/// keeps a regular file under the home, while on macOS Claude keeps the OAuth
/// tokens in the Keychain — there is no file to inspect, only a status command
/// to run. Baking the Linux shape into shared logic is how a check ends up
/// lying on other platforms.
/// The provider's official way into a browser login, when it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginCommand {
    pub argv: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthStoreStatus {
    Healthy {
        path: PathBuf,
    },
    /// No store at all: the environment has never logged in.
    Missing {
        path: PathBuf,
    },
    /// The provider wrote an explicit logged-out marker (claude writes an
    /// `expiresAt: 0` stub on logout).
    LoggedOut {
        path: PathBuf,
    },
    /// The store exists but is not a parseable credential file.
    Unreadable {
        path: PathBuf,
        details: String,
    },
    /// The store reaches into a different environment: a symlink whose target
    /// crosses the WSL/Windows interop boundary, or a file living on a
    /// Windows-mounted filesystem. Sharing a chain across that boundary is
    /// how logins die (decision 0006).
    ForeignEnvironment {
        path: PathBuf,
        target: PathBuf,
    },
    /// Nothing to check on this OS (unmanaged, or inspectable only through
    /// the provider's own tooling).
    NotCheckable {
        reason: &'static str,
    },
}

impl AuthStoreStatus {
    /// Whether the persisted store itself proves that a seat can attempt
    /// authentication. `NotCheckable` deliberately stays false: callers must
    /// run the provider-owned status command instead of treating ignorance as
    /// a healthy login.
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy { .. })
    }
}

/// The storage spec for a provider on one OS (`std::env::consts::OS` values).
pub fn auth_store_spec(provider: &str, os: &str) -> AuthStoreSpec {
    crate::provider::adapter(provider)
        .map(|adapter| adapter.auth_spec().store_for_os(os))
        .unwrap_or(AuthStoreSpec::Unmanaged)
}

/// The provider's login doorway. For a first-run TUI this is the provider
/// executable itself; the provider-owned auth spec declares how AH safely
/// reaches its login challenge.
pub fn login_command(provider: &str) -> Option<LoginCommand> {
    crate::provider::adapter(provider)
        .and_then(|adapter| adapter.auth_spec().login_argv(false))
        .map(|argv| LoginCommand { argv })
}

/// The one-line remedy for an unhealthy store, ready to paste into a shell.
pub fn login_remedy(provider: &str) -> String {
    match login_command(provider) {
        Some(command) => command.argv.join(" "),
        None => format!("no login flow is declared for provider `{provider}`"),
    }
}

/// Checks a provider's host store for the current OS and home.
///
/// `claude_store_override` carries the project's
/// `providers.claude.shared_credentials_dir`, which is where claude seats
/// actually read from; the default spec path is used when no override is
/// configured.
pub fn check_auth_store(
    provider: &str,
    home: &Path,
    claude_store_override: Option<&Path>,
) -> AuthStoreStatus {
    check_auth_store_for(provider, home, claude_store_override, std::env::consts::OS)
}

/// The check with the OS supplied, so callers and tests can pin behaviour
/// instead of inheriting the machine they happen to run on.
pub fn check_auth_store_for(
    provider: &str,
    home: &Path,
    claude_store_override: Option<&Path>,
    os: &str,
) -> AuthStoreStatus {
    check_auth_store_for_os(
        provider,
        home,
        claude_store_override,
        os,
        &windows_interop_target,
    )
}

fn check_auth_store_for_os(
    provider: &str,
    home: &Path,
    claude_store_override: Option<&Path>,
    os: &str,
    foreign_boundary: &dyn Fn(&Path) -> Option<PathBuf>,
) -> AuthStoreStatus {
    let provider = crate::provider::manifest::canonicalize_provider_name(provider);
    let path = match auth_store_spec(provider, os) {
        AuthStoreSpec::File { relative_path } => {
            if provider == "claude"
                && let Some(dir) = claude_store_override
            {
                dir.join(".credentials.json")
            } else {
                home.join(relative_path)
            }
        }
        AuthStoreSpec::OsCredentialService { .. } => {
            return AuthStoreStatus::NotCheckable {
                reason: "stored in the OS credential service; use the provider's status command",
            };
        }
        AuthStoreSpec::Unmanaged => {
            return AuthStoreStatus::NotCheckable {
                reason: "not managed by ah on this OS",
            };
        }
    };

    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return AuthStoreStatus::Missing { path };
        }
        Err(err) => {
            return AuthStoreStatus::Unreadable {
                path,
                details: err.to_string(),
            };
        }
    };

    // A symlink out of this environment is the #18 fuse: the provider writes
    // via rename, which replaces the link with a private file on the first
    // refresh — the chain forks and the other environment's login dies later.
    if metadata.file_type().is_symlink() {
        let target = std::fs::read_link(&path).unwrap_or_default();
        let resolved = if target.is_absolute() {
            target.clone()
        } else {
            path.parent().unwrap_or(Path::new("/")).join(&target)
        };
        if let Some(boundary) = foreign_boundary(&resolved) {
            return AuthStoreStatus::ForeignEnvironment {
                path,
                target: boundary,
            };
        }
    }
    let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
    if let Some(boundary) = foreign_boundary(&canonical) {
        return AuthStoreStatus::ForeignEnvironment {
            path,
            target: boundary,
        };
    }

    match provider {
        "claude" => classify_claude_store(path),
        "codex" => classify_codex_store(path),
        "antigravity" => classify_antigravity_store(path),
        _ => AuthStoreStatus::Healthy { path },
    }
}

fn classify_antigravity_store(path: PathBuf) -> AuthStoreStatus {
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) => {
            return AuthStoreStatus::Unreadable {
                path,
                details: err.to_string(),
            };
        }
    };
    if raw.trim().is_empty() {
        return AuthStoreStatus::LoggedOut { path };
    }
    let parsed: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(parsed) => parsed,
        Err(err) => {
            return AuthStoreStatus::Unreadable {
                path,
                details: err.to_string(),
            };
        }
    };
    let has_refresh_token = parsed
        .get("token")
        .and_then(|token| token.get("refresh_token"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|token| !token.is_empty());
    if has_refresh_token {
        AuthStoreStatus::Healthy { path }
    } else {
        AuthStoreStatus::LoggedOut { path }
    }
}

/// Expiry restraint: `expiresAt` in the past is NOT a dead login — the access
/// token expired and the refresh token will renew it. The only file states
/// that mean "log in again" are absence, the explicit logged-out stub, and a
/// file that does not parse.
fn classify_claude_store(path: PathBuf) -> AuthStoreStatus {
    let parsed = match read_json(&path) {
        Ok(value) => value,
        Err(details) => return AuthStoreStatus::Unreadable { path, details },
    };
    let oauth = parsed.get("claudeAiOauth").unwrap_or(&parsed);
    let expires_at = oauth.get("expiresAt").and_then(serde_json::Value::as_i64);
    if expires_at == Some(0) {
        return AuthStoreStatus::LoggedOut { path };
    }
    AuthStoreStatus::Healthy { path }
}

fn classify_codex_store(path: PathBuf) -> AuthStoreStatus {
    let parsed = match read_json(&path) {
        Ok(value) => value,
        Err(details) => return AuthStoreStatus::Unreadable { path, details },
    };
    let has_tokens = parsed
        .get("tokens")
        .and_then(|tokens| tokens.get("refresh_token"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|token| !token.is_empty());
    let has_api_key = parsed
        .get("OPENAI_API_KEY")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|key| !key.is_empty());
    if has_tokens || has_api_key {
        AuthStoreStatus::Healthy { path }
    } else {
        AuthStoreStatus::LoggedOut { path }
    }
}

fn read_json(path: &Path) -> Result<serde_json::Value, String> {
    let raw = std::fs::read_to_string(path).map_err(|err| err.to_string())?;
    serde_json::from_str(&raw).map_err(|err| err.to_string())
}

/// Returns the interop mount a path lives on, if any (Linux/WSL only).
fn windows_interop_target(path: &Path) -> Option<PathBuf> {
    crate::home_materialization::windows_interop_mount_point(path).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn no_boundary(_: &Path) -> Option<PathBuf> {
        None
    }

    fn mnt_boundary(path: &Path) -> Option<PathBuf> {
        path.starts_with("/mnt/c").then(|| PathBuf::from("/mnt/c"))
    }

    #[test]
    fn the_spec_is_per_os_not_one_shape() {
        assert_eq!(
            auth_store_spec("claude", "linux"),
            AuthStoreSpec::File {
                relative_path: ".claude/.credentials.json"
            }
        );
        assert!(matches!(
            auth_store_spec("claude", "macos"),
            AuthStoreSpec::OsCredentialService { .. }
        ),);
        assert_eq!(
            auth_store_spec("claude", "windows"),
            AuthStoreSpec::Unmanaged
        );
        assert_eq!(
            auth_store_spec("codex", "windows"),
            AuthStoreSpec::Unmanaged
        );
    }

    #[test]
    fn a_missing_store_asks_for_a_first_login() {
        let home = tempfile::TempDir::new().unwrap();

        let status = check_auth_store_for_os("codex", home.path(), None, "linux", &no_boundary);

        assert!(matches!(status, AuthStoreStatus::Missing { .. }));
        assert!(!status.is_healthy());
    }

    #[test]
    fn an_opaque_store_is_not_healthy_without_a_provider_status_probe() {
        let status = AuthStoreStatus::NotCheckable {
            reason: "stored in the OS credential service",
        };

        assert!(!status.is_healthy());
    }

    #[test]
    fn a_codex_store_with_a_refresh_token_is_healthy() {
        let home = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(home.path().join(".codex")).unwrap();
        fs::write(
            home.path().join(".codex/auth.json"),
            r#"{"tokens":{"refresh_token":"r1","access_token":"a1"},"last_refresh":"2026-07-15"}"#,
        )
        .unwrap();

        let status = check_auth_store_for_os("codex", home.path(), None, "linux", &no_boundary);

        assert!(status.is_healthy(), "got {status:?}");
    }

    #[test]
    fn a_claude_logged_out_stub_is_reported_as_logged_out() {
        let home = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(home.path().join(".claude")).unwrap();
        fs::write(
            home.path().join(".claude/.credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"","expiresAt":0}}"#,
        )
        .unwrap();

        let status = check_auth_store_for_os("claude", home.path(), None, "linux", &no_boundary);

        assert!(matches!(status, AuthStoreStatus::LoggedOut { .. }));
    }

    /// `expiresAt` in the past means the access token expired, not the login:
    /// the refresh token renews it. Nagging for a login here would be wrong.
    #[test]
    fn an_expired_access_token_is_not_a_dead_login() {
        let home = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(home.path().join(".claude")).unwrap();
        fs::write(
            home.path().join(".claude/.credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"a1","refreshToken":"r1","expiresAt":1000}}"#,
        )
        .unwrap();

        let status = check_auth_store_for_os("claude", home.path(), None, "linux", &no_boundary);

        assert!(status.is_healthy(), "got {status:?}");
    }

    /// The #18 fuse: a symlink whose target crosses into the Windows
    /// environment forks the chain on the first refresh.
    #[cfg(unix)]
    #[test]
    fn a_symlink_into_the_windows_environment_is_foreign() {
        let home = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(home.path().join(".claude")).unwrap();
        std::os::unix::fs::symlink(
            "/mnt/c/Users/someone/.claude/.credentials.json",
            home.path().join(".claude/.credentials.json"),
        )
        .unwrap();

        let status = check_auth_store_for_os("claude", home.path(), None, "linux", &mnt_boundary);

        assert!(
            matches!(status, AuthStoreStatus::ForeignEnvironment { .. }),
            "got {status:?}"
        );
        assert!(!status.is_healthy());
    }

    #[test]
    fn the_claude_override_is_where_seats_actually_read() {
        let home = tempfile::TempDir::new().unwrap();
        let shared = tempfile::TempDir::new().unwrap();
        fs::write(
            shared.path().join(".credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"a1","expiresAt":99}}"#,
        )
        .unwrap();

        let status = check_auth_store_for_os(
            "claude",
            home.path(),
            Some(shared.path()),
            "linux",
            &no_boundary,
        );

        match status {
            AuthStoreStatus::Healthy { path } => {
                assert_eq!(path, shared.path().join(".credentials.json"));
            }
            other => panic!("expected healthy at the override, got {other:?}"),
        }
    }

    #[test]
    fn garbage_in_the_store_is_unreadable_not_healthy() {
        let home = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(home.path().join(".codex")).unwrap();
        fs::write(home.path().join(".codex/auth.json"), "not json").unwrap();

        let status = check_auth_store_for_os("codex", home.path(), None, "linux", &no_boundary);

        assert!(matches!(status, AuthStoreStatus::Unreadable { .. }));
    }

    #[test]
    fn every_provider_has_a_pasteable_remedy() {
        assert_eq!(login_remedy("codex"), "codex login");
        assert_eq!(login_remedy("claude"), "claude auth login");
        let agy = login_remedy("antigravity");
        assert_eq!(agy, "agy");
        assert_eq!(login_command("antigravity").unwrap().argv, &["agy"]);
    }

    #[test]
    fn an_empty_antigravity_token_file_is_logged_out() {
        let home = tempfile::TempDir::new().unwrap();
        let dir = home.path().join(".gemini/antigravity-cli");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("antigravity-oauth-token"), "").unwrap();

        let status =
            check_auth_store_for_os("antigravity", home.path(), None, "linux", &no_boundary);

        assert!(matches!(status, AuthStoreStatus::LoggedOut { .. }));
    }

    #[test]
    fn antigravity_requires_a_refreshable_token_shape() {
        let home = tempfile::TempDir::new().unwrap();
        let dir = home.path().join(".gemini/antigravity-cli");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("antigravity-oauth-token"),
            r#"{"auth_method":"oauth-personal","token":{"access_token":"a","refresh_token":"r","expiry":"2026-08-10T00:00:00Z","token_type":"Bearer"}}"#,
        )
        .unwrap();

        let status =
            check_auth_store_for_os("antigravity", home.path(), None, "linux", &no_boundary);

        assert!(matches!(status, AuthStoreStatus::Healthy { .. }));
    }
}
