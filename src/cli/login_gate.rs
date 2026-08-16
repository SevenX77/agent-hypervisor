//! Login gate for `ah start` and the explicit `ah login` recovery command.
//!
//! Provider Adapters own storage paths, official commands, headless routes,
//! terminal markers, and upstream documentation. This gate only orders the
//! providers required by one project and refuses to start seats until the
//! corresponding provider flow has produced causal completion evidence.

use crate::cli::config::ProjectConfig;
use crate::cli::rpc_client::CliError;
use crate::provider::auth_flow::{
    authentication_is_healthy, headless_environment, run_official_login,
};
use crate::provider::auth_store::{AuthStoreStatus, check_auth_store_for, login_remedy};
use std::collections::BTreeSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

/// Every provider the project would spawn a seat for.
pub fn providers_in_config(config: &ProjectConfig) -> BTreeSet<String> {
    let mut providers = BTreeSet::new();
    if config.master.enabled {
        providers.insert(config.master.resolved_provider());
    }
    for agent in config.agents.values() {
        providers.insert(
            crate::provider::manifest::canonicalize_provider_name(&agent.provider).to_string(),
        );
    }
    providers.remove("bash");
    providers
}

/// Checks every provider the config uses and starts its official login flow
/// when the caller is attached to an operator terminal.
pub fn ensure_provider_logins(
    config: &ProjectConfig,
    home: &Path,
    claude_store_override: Option<&Path>,
    interactive: bool,
) -> Result<(), CliError> {
    ensure_provider_logins_for_os(
        config,
        home,
        claude_store_override,
        interactive,
        std::env::consts::OS,
    )
}

fn ensure_provider_logins_for_os(
    config: &ProjectConfig,
    home: &Path,
    claude_store_override: Option<&Path>,
    interactive: bool,
    os: &str,
) -> Result<(), CliError> {
    for provider in providers_in_config(config) {
        let override_path = provider_store_override(&provider, claude_store_override);
        let status = check_auth_store_for(&provider, home, override_path, os);
        if authentication_is_healthy(&provider, home, override_path, os) {
            continue;
        }
        if let AuthStoreStatus::ForeignEnvironment { path, target } = &status {
            return Err(CliError::Config(foreign_store_message(
                &provider, path, target,
            )));
        }
        if !interactive {
            return Err(CliError::Config(format!(
                "{provider}: {}. OAuth consent needs an operator terminal; connect with SSH and run `ah login {provider} --headless` (provider fallback: `{}`)",
                describe(&status),
                login_remedy(&provider)
            )));
        }

        let headless = headless_environment();
        eprintln!(
            "{provider}: {}; starting the provider's official {} login flow",
            describe(&status),
            if headless { "headless" } else { "interactive" }
        );
        run_official_login(&provider, home, override_path, headless)
            .map_err(|err| CliError::Config(err.to_string()))?;

        let after = check_auth_store_for(&provider, home, override_path, os);
        if !authentication_is_healthy(&provider, home, override_path, os) {
            return Err(CliError::Config(format!(
                "{provider}: login returned but the store is still {}; run `ah login {provider} --headless` and retry",
                describe(&after)
            )));
        }
    }
    Ok(())
}

/// Explicit recovery entry for an operator who needs to authenticate before
/// unattended startup. `force_headless` selects the provider's official
/// device-code or authorization-code-paste route even outside SSH.
pub fn login_provider(
    provider: &str,
    home: &Path,
    claude_store_override: Option<&Path>,
    interactive: bool,
    force_headless: bool,
) -> Result<(), CliError> {
    let provider = crate::provider::canonical_name(provider)
        .ok_or_else(|| CliError::Config(format!("unknown provider `{provider}`")))?;
    if provider == "bash" {
        return Err(CliError::Config(
            "provider `bash` does not require login".to_string(),
        ));
    }
    let override_path = provider_store_override(provider, claude_store_override);
    let status = check_auth_store_for(provider, home, override_path, std::env::consts::OS);
    if authentication_is_healthy(provider, home, override_path, std::env::consts::OS) {
        eprintln!("{provider}: login is already healthy in this environment");
        return Ok(());
    }
    if let AuthStoreStatus::ForeignEnvironment { path, target } = &status {
        return Err(CliError::Config(foreign_store_message(
            provider, path, target,
        )));
    }
    if !interactive {
        return Err(CliError::Config(format!(
            "{provider}: login requires an interactive terminal; connect with SSH and run `ah login {provider} --headless`"
        )));
    }

    run_official_login(
        provider,
        home,
        override_path,
        force_headless || headless_environment(),
    )
    .map_err(|err| CliError::Config(err.to_string()))
}

pub fn stdin_and_stdout_are_terminal() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn provider_store_override<'a>(
    provider: &str,
    claude_store_override: Option<&'a Path>,
) -> Option<&'a Path> {
    (provider == "claude")
        .then_some(claude_store_override)
        .flatten()
}

fn describe(status: &AuthStoreStatus) -> String {
    match status {
        AuthStoreStatus::Healthy { .. } => "ok".to_string(),
        AuthStoreStatus::NotCheckable { reason } => {
            format!("not directly checkable ({reason})")
        }
        AuthStoreStatus::Missing { path } => {
            format!(
                "no login in this environment ({} is missing)",
                path.display()
            )
        }
        AuthStoreStatus::LoggedOut { path } => {
            format!("logged out ({} holds a logout stub)", path.display())
        }
        AuthStoreStatus::Unreadable { path, details } => {
            format!(
                "unreadable credential store ({}: {details})",
                path.display()
            )
        }
        AuthStoreStatus::ForeignEnvironment { path, target } => format!(
            "credential store reaches into another environment ({} -> {})",
            path.display(),
            target.display()
        ),
    }
}

fn foreign_store_message(provider: &str, path: &Path, target: &PathBuf) -> String {
    format!(
        "{provider}: {} points into the Windows environment ({}). One refresh-token chain cannot serve two environments. Remove the link and sign in to this environment instead:\n  rm {}\n  ah login {provider} --headless",
        path.display(),
        target.display(),
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(raw: &str) -> ProjectConfig {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), raw).unwrap();
        crate::cli::config::load_project_config(file.path()).unwrap()
    }

    #[test]
    fn a_shell_only_project_needs_no_login() {
        let config = config(
            "version = \"1\"\n\n[master]\nenabled = false\n\n[agents.a1]\nprovider = \"bash\"\n",
        );

        assert!(providers_in_config(&config).is_empty());
        let home = tempfile::TempDir::new().unwrap();
        assert!(ensure_provider_logins_for_os(&config, home.path(), None, false, "linux").is_ok());
    }

    #[test]
    fn every_seat_provider_is_collected_once() {
        let config = config(
            "version = \"1\"\n\n[master]\nprovider = \"codex\"\n\n\
             [agents.a1]\nprovider = \"codex\"\n\n[agents.a2]\nprovider = \"antigravity\"\n",
        );

        assert_eq!(
            providers_in_config(&config).into_iter().collect::<Vec<_>>(),
            vec!["antigravity".to_string(), "codex".to_string()]
        );
    }

    #[test]
    fn a_missing_login_fails_non_interactively_with_headless_recovery() {
        let config = config(
            "version = \"1\"\n\n[master]\nenabled = false\n\n[agents.a1]\nprovider = \"codex\"\n",
        );
        let home = tempfile::TempDir::new().unwrap();

        let err =
            ensure_provider_logins_for_os(&config, home.path(), None, false, "linux").unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("ah login codex --headless"),
            "got: {message}"
        );
        assert!(
            message.contains("no login in this environment"),
            "got: {message}"
        );
    }

    #[test]
    fn a_healthy_store_passes_without_interaction() {
        let config = config(
            "version = \"1\"\n\n[master]\nenabled = false\n\n[agents.a1]\nprovider = \"codex\"\n",
        );
        let home = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(home.path().join(".codex")).unwrap();
        std::fs::write(
            home.path().join(".codex/auth.json"),
            r#"{"tokens":{"refresh_token":"r1"}}"#,
        )
        .unwrap();

        assert!(ensure_provider_logins_for_os(&config, home.path(), None, false, "linux").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_foreign_store_fails_even_when_login_is_available() {
        let config = config(
            "version = \"1\"\n\n[master]\nenabled = false\n\n[agents.a1]\nprovider = \"claude\"\n\n\
             [providers.claude]\nshared_credentials_dir = \"~/.claude\"\n",
        );
        let home = tempfile::TempDir::new().unwrap();
        let shared = home.path().join("shared");
        std::fs::create_dir_all(&shared).unwrap();
        std::os::unix::fs::symlink(
            "/mnt/c/Users/someone/.claude/.credentials.json",
            shared.join(".credentials.json"),
        )
        .unwrap();

        let err =
            ensure_provider_logins_for_os(&config, home.path(), Some(&shared), false, "linux")
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("claude")
                && (err.contains("rm ") || err.contains("no login") || err.contains("unreadable")),
            "got: {err}"
        );
    }

    #[test]
    fn bash_is_rejected_by_the_explicit_login_entry() {
        let home = tempfile::TempDir::new().unwrap();
        let err = login_provider("bash", home.path(), None, true, true).unwrap_err();
        assert!(err.to_string().contains("does not require login"));
    }
}
