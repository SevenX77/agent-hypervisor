//! The login doorman for `ah start` (decision 0006).
//!
//! Before a project's seats are spawned, every provider the project uses must
//! have a healthy login in THIS environment. When one is missing, the doorman
//! does what `aws sso login` taught operators to expect: in an interactive
//! terminal it launches the provider's own login flow right there and
//! continues once it succeeds; anywhere else it fails with a remedy the
//! operator can paste. ah itself never touches a credential value — the
//! provider's login program and the operator's browser do all of it.

use crate::cli::config::ProjectConfig;
use crate::cli::rpc_client::CliError;
use crate::provider::auth_store::{
    AuthStoreStatus, check_auth_store_for, login_command, login_remedy,
};
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
    // A shell needs no login.
    providers.remove("bash");
    providers
}

/// Checks every provider the config uses and, where allowed, opens the door.
///
/// `interactive` should be true only when stdin and stdout are the operator's
/// terminal — a login flow launched anywhere else would hang a pipeline.
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
        let status = check_auth_store_for(&provider, home, claude_store_override, os);
        if status.is_healthy() {
            continue;
        }
        if let AuthStoreStatus::ForeignEnvironment { path, target } = &status {
            // Not fixable by logging in: the store must stop reaching across
            // the boundary first, and it is the operator's file to change.
            return Err(CliError::Config(foreign_store_message(
                &provider, path, target,
            )));
        }
        if interactive && let Some(command) = login_command(&provider) {
            eprintln!(
                "{provider}: {} — launching `{}` (complete the sign-in in your browser)",
                describe(&status),
                command.argv.join(" ")
            );
            run_login_in_terminal(command.argv)?;
            let after = check_auth_store_for(&provider, home, claude_store_override, os);
            if after.is_healthy() {
                continue;
            }
            return Err(CliError::Config(format!(
                "{provider}: login finished but the store is still {}; run `{}` manually and retry",
                describe(&after),
                login_remedy(&provider)
            )));
        }
        return Err(CliError::Config(format!(
            "{provider}: {}. Sign in to this environment first: {}",
            describe(&status),
            login_remedy(&provider)
        )));
    }
    Ok(())
}

pub fn stdin_and_stdout_are_terminal() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn describe(status: &AuthStoreStatus) -> String {
    match status {
        AuthStoreStatus::Healthy { .. } | AuthStoreStatus::NotCheckable { .. } => "ok".to_string(),
        AuthStoreStatus::Missing { path } => {
            format!("no login in this environment ({} is missing)", path.display())
        }
        AuthStoreStatus::LoggedOut { path } => {
            format!("logged out ({} holds a logout stub)", path.display())
        }
        AuthStoreStatus::Unreadable { path, details } => {
            format!("unreadable credential store ({}: {details})", path.display())
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
        "{provider}: {} points into the Windows environment ({}). One token chain cannot serve \
         two environments — the first refresh on either side kills the other (decision 0006). \
         Remove the link and sign in to this environment instead:\n  rm {}\n  {}",
        path.display(),
        target.display(),
        path.display(),
        login_remedy(provider)
    )
}

/// Runs the provider's login program attached to the operator's terminal.
fn run_login_in_terminal(argv: &[&str]) -> Result<(), CliError> {
    let mut command = std::process::Command::new(argv[0]);
    command.args(&argv[1..]);
    // Point CLIs that honour $BROWSER at the opener so the sign-in page pops
    // instead of printing a URL; in WSL that opener is the bridge to the
    // Windows browser (`ah setup --fix` installs it).
    if std::env::var_os("BROWSER").is_none() && which::which("xdg-open").is_ok() {
        command.env("BROWSER", "xdg-open");
    }
    let status = command
        .status()
        .map_err(|err| {
            CliError::Config(format!("could not launch `{}`: {err}", argv.join(" ")))
        })?;
    if !status.success() {
        return Err(CliError::Config(format!(
            "`{}` exited with {status}; sign in and retry",
            argv.join(" ")
        )));
    }
    Ok(())
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
        assert!(
            ensure_provider_logins_for_os(&config, home.path(), None, false, "linux").is_ok()
        );
    }

    #[test]
    fn every_seat_provider_is_collected_once() {
        let config = config(
            "version = \"1\"\n\n[master]\nprovider = \"codex\"\n\n\
             [agents.a1]\nprovider = \"codex\"\n\n[agents.a2]\nprovider = \"antigravity\"\n",
        );

        let providers = providers_in_config(&config);

        assert_eq!(
            providers.into_iter().collect::<Vec<_>>(),
            vec!["antigravity".to_string(), "codex".to_string()]
        );
    }

    #[test]
    fn a_missing_login_fails_non_interactively_with_a_pasteable_remedy() {
        let config = config(
            "version = \"1\"\n\n[master]\nenabled = false\n\n[agents.a1]\nprovider = \"codex\"\n",
        );
        let home = tempfile::TempDir::new().unwrap();

        let err =
            ensure_provider_logins_for_os(&config, home.path(), None, false, "linux").unwrap_err();

        let message = err.to_string();
        assert!(message.contains("codex login"), "got: {message}");
        assert!(message.contains("no login in this environment"), "got: {message}");
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

        assert!(
            ensure_provider_logins_for_os(&config, home.path(), None, false, "linux").is_ok()
        );
    }

    /// The foreign-link case must not be "fixed" by launching a login over it:
    /// the login would write through or replace the link and fork the chain.
    #[cfg(unix)]
    #[test]
    fn a_foreign_store_fails_even_interactively_with_removal_instructions() {
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

        // Interactive=true: the doorman must still refuse rather than launch
        // a login onto a foreign link. Only run where /mnt/c is actually a
        // Windows mount (WSL); elsewhere the link is just dangling → Missing.
        let result =
            ensure_provider_logins_for_os(&config, home.path(), Some(&shared), false, "linux");

        let err = result.unwrap_err().to_string();
        // On WSL the /mnt/c target is a real interop mount and the message
        // carries removal instructions; on native Linux the link is merely
        // dangling and reads as an unreadable store. Both refuse to proceed.
        assert!(
            err.contains("claude")
                && (err.contains("rm ") || err.contains("no login") || err.contains("unreadable")),
            "got: {err}"
        );
    }
}
