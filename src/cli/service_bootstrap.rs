use crate::cli::rpc_client::CliError;
use crate::cli::service_unit::{
    ServiceUnitError, atomic_write_unit, derive_unit_name, render_unit_file,
    resolve_user_systemd_dir_from_env,
};
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
use std::process::{Command, Output};

pub trait SystemctlRunner {
    fn run(&self, args: &[&str]) -> io::Result<Output>;
}

#[derive(Debug, Default)]
pub struct RealSystemctlRunner;

impl SystemctlRunner for RealSystemctlRunner {
    fn run(&self, args: &[&str]) -> io::Result<Output> {
        Command::new("systemctl").arg("--user").args(args).output()
    }
}

#[derive(Debug)]
pub enum PersistentBootstrapError {
    Recoverable(String),
    Fatal(String),
}

impl PersistentBootstrapError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }

    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::Recoverable(_))
    }

    pub fn into_cli_error(self) -> CliError {
        CliError::Config(self.to_string())
    }
}

impl fmt::Display for PersistentBootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recoverable(message) | Self::Fatal(message) => f.write_str(message),
        }
    }
}

impl From<ServiceUnitError> for PersistentBootstrapError {
    fn from(err: ServiceUnitError) -> Self {
        Self::Recoverable(format!("failed to render ahd systemd user unit: {err}"))
    }
}

pub fn user_manager_available_from(
    xdg_runtime_dir: Option<&str>,
    probe_output: Option<&str>,
) -> bool {
    if xdg_runtime_dir.is_none_or(|value| value.is_empty()) {
        return false;
    }
    matches!(
        probe_output.map(str::trim),
        Some("running" | "degraded" | "starting" | "maintenance" | "initializing")
    )
}

pub fn systemd_user_bootstrap_available() -> bool {
    let xdg_runtime_dir = std::env::var("XDG_RUNTIME_DIR").ok();
    let output = Command::new("systemctl")
        .args(["--user", "is-system-running"])
        .output();
    let Ok(output) = output else {
        return false;
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    user_manager_available_from(xdg_runtime_dir.as_deref(), Some(stdout.trim()))
}

pub fn collect_passthrough_env() -> Vec<(String, String)> {
    crate::provider::manifest::ENV_PASSTHROUGH
        .iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .collect()
}

pub fn bootstrap_persistent_unit(
    runner: &impl SystemctlRunner,
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
    socket_accepting: bool,
) -> Result<String, PersistentBootstrapError> {
    let systemd_dir =
        resolve_user_systemd_dir_from_env().map_err(PersistentBootstrapError::from)?;
    bootstrap_persistent_unit_in_dir(
        runner,
        &systemd_dir,
        ahd_bin,
        state_dir,
        env,
        socket_accepting,
    )
}

pub fn bootstrap_persistent_unit_in_dir(
    runner: &impl SystemctlRunner,
    systemd_dir: &Path,
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
    socket_accepting: bool,
) -> Result<String, PersistentBootstrapError> {
    let unit_name = derive_unit_name(state_dir);
    let unit_path = systemd_dir.join(&unit_name);
    let desired = render_unit_file(&unit_name, ahd_bin, state_dir, env)?;
    migrate_legacy_transient_unit(runner, socket_accepting)?;

    let existing = fs::read_to_string(&unit_path).ok();
    if let Some(existing) = existing.as_deref() {
        ensure_existing_unit_is_safe_to_overwrite(&unit_path, existing, &desired)?;
    }
    let changed = existing.as_deref() != Some(desired.as_str());

    if changed {
        atomic_write_unit(&unit_path, &desired).map_err(|err| {
            PersistentBootstrapError::Recoverable(format!(
                "failed to write ahd systemd user unit {}: {err}",
                unit_path.display()
            ))
        })?;
    }

    run_required(runner, &["daemon-reload"], FailureKind::Recoverable)?;

    run_reset_failed_best_effort(runner, &unit_name);
    run_required(runner, &["enable", &unit_name], FailureKind::Fatal)?;

    // The current caller reaches this path only after the socket is not accepting.
    // Restart handles both inactive units and active-but-hung daemons.
    let action = if socket_accepting && !changed {
        "start"
    } else {
        "restart"
    };
    run_required(runner, &[action, &unit_name], FailureKind::Fatal)?;

    Ok(unit_name)
}

/// Removes the generated unit for `state_dir`, the counterpart of
/// [`bootstrap_persistent_unit`].
///
/// `ah stop` is the graceful teardown of a stack, so it must leave nothing that
/// starts that stack again: an `enabled` unit survives the daemon and relaunches
/// it at the next login, for a state directory the operator has finished with.
///
/// Only units ah generated for this exact state directory are touched — the
/// `AH_STATE_DIR` marker is checked first, so a hand-written unit that happens
/// to share the name is left alone.
pub fn teardown_persistent_unit(runner: &impl SystemctlRunner, state_dir: &Path) {
    let Ok(systemd_dir) = resolve_user_systemd_dir_from_env() else {
        return;
    };
    teardown_persistent_unit_in_dir(runner, &systemd_dir, state_dir);
}

pub fn teardown_persistent_unit_in_dir(
    runner: &impl SystemctlRunner,
    systemd_dir: &Path,
    state_dir: &Path,
) {
    let unit_name = derive_unit_name(state_dir);
    let unit_path = systemd_dir.join(&unit_name);
    let Ok(content) = fs::read_to_string(&unit_path) else {
        // Nothing generated for this stack: a transient run, or already removed.
        return;
    };
    let marker = generated_state_dir_marker(&content);
    if marker.as_deref() != Some(&state_dir.display().to_string()) {
        tracing::warn!(
            unit = unit_name,
            path = %unit_path.display(),
            "leaving systemd user unit alone: it was not generated by ah for this state dir"
        );
        return;
    }

    match runner.run(&["disable", &unit_name]) {
        Ok(output) if output.status.success() => {}
        Ok(output) => tracing::warn!(
            unit = unit_name,
            status = ?output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "systemctl disable failed while tearing down the ahd unit"
        ),
        Err(err) => tracing::warn!(
            unit = unit_name,
            error = %err,
            "systemctl disable failed to execute while tearing down the ahd unit"
        ),
    }

    if let Err(err) = fs::remove_file(&unit_path) {
        tracing::warn!(
            unit = unit_name,
            path = %unit_path.display(),
            error = %err,
            "failed to remove the ahd generated systemd user unit"
        );
        return;
    }

    let _ = runner.run(&["daemon-reload"]);
    run_reset_failed_best_effort(runner, &unit_name);
    tracing::info!(
        unit = unit_name,
        state_dir = %state_dir.display(),
        "removed the ahd generated systemd user unit on stop"
    );
}

pub fn gc_stale_units(runner: &impl SystemctlRunner) {
    let Ok(systemd_dir) = resolve_user_systemd_dir_from_env() else {
        return;
    };
    gc_stale_units_in_dir(runner, &systemd_dir);
}

pub fn gc_stale_units_in_dir(runner: &impl SystemctlRunner, systemd_dir: &Path) {
    let entries = match fs::read_dir(systemd_dir) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!(
                systemd_dir = %systemd_dir.display(),
                error = %err,
                "failed to scan ahd systemd user units for stale generated units"
            );
            return;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                tracing::warn!(
                    systemd_dir = %systemd_dir.display(),
                    error = %err,
                    "failed to read systemd user unit directory entry during ahd stale-unit GC"
                );
                continue;
            }
        };
        let Some(unit_name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !is_generated_hash_unit_filename(&unit_name) {
            continue;
        }

        let unit_path = entry.path();
        let content = match fs::read_to_string(&unit_path) {
            Ok(content) => content,
            Err(err) => {
                tracing::warn!(
                    unit = unit_name,
                    path = %unit_path.display(),
                    error = %err,
                    "failed to read ahd systemd user unit during stale-unit GC"
                );
                continue;
            }
        };
        let Some(state_dir) = generated_state_dir_marker(&content) else {
            continue;
        };
        if Path::new(&state_dir).exists() {
            continue;
        }

        match runner.run(&["disable", "--now", &unit_name]) {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                tracing::warn!(
                    unit = unit_name,
                    state_dir = %state_dir,
                    status = ?output.status,
                    stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                    "systemctl disable --now failed during ahd stale-unit GC"
                );
            }
            Err(err) => {
                tracing::warn!(
                    unit = unit_name,
                    state_dir = %state_dir,
                    error = %err,
                    "systemctl disable --now failed to execute during ahd stale-unit GC"
                );
            }
        }

        match fs::remove_file(&unit_path) {
            Ok(()) => {
                tracing::info!(
                    unit = unit_name,
                    state_dir = %state_dir,
                    path = %unit_path.display(),
                    "removed stale ahd generated systemd user unit"
                );
            }
            Err(err) => {
                tracing::warn!(
                    unit = unit_name,
                    state_dir = %state_dir,
                    path = %unit_path.display(),
                    error = %err,
                    "failed to remove stale ahd generated systemd user unit"
                );
            }
        }
    }
}

fn is_generated_hash_unit_filename(name: &str) -> bool {
    let Some(hash) = name
        .strip_prefix("ah-")
        .and_then(|value| value.strip_suffix(".service"))
    else {
        return false;
    };
    hash.len() == 16 && hash.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn migrate_legacy_transient_unit(
    runner: &impl SystemctlRunner,
    socket_accepting: bool,
) -> Result<(), PersistentBootstrapError> {
    let output = match runner.run(&[
        "show",
        "ahd.service",
        "-p",
        "FragmentPath",
        "-p",
        "LoadState",
        "-p",
        "ActiveState",
        "-p",
        "Transient",
    ]) {
        Ok(output) => output,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "failed to inspect legacy ahd.service before persistent bootstrap"
            );
            return Ok(());
        }
    };
    if !output.status.success() {
        return Ok(());
    }

    let show = parse_unit_show_output(&String::from_utf8_lossy(&output.stdout));
    if show.load_state.as_deref() == Some("not-found") || !show.is_active() {
        return Ok(());
    }

    if show.transient != Some(true) {
        tracing::warn!(
            fragment_path = show.fragment_path.as_deref().unwrap_or_default(),
            transient = ?show.transient,
            "legacy ahd.service is not known transient; leaving it untouched and using hashed unit"
        );
        return Ok(());
    }

    if socket_accepting {
        return Ok(());
    }

    tracing::info!("migrating legacy transient ahd.service before persistent bootstrap");
    run_required(runner, &["stop", "ahd.service"], FailureKind::Fatal)
}

fn ensure_existing_unit_is_safe_to_overwrite(
    unit_path: &Path,
    existing: &str,
    desired: &str,
) -> Result<(), PersistentBootstrapError> {
    let expected_state_dir = generated_state_dir_marker(desired).ok_or_else(|| {
        PersistentBootstrapError::Fatal(format!(
            "generated ahd unit for {} is missing AH_STATE_DIR marker",
            unit_path.display()
        ))
    })?;
    let existing_state_dir = generated_state_dir_marker(existing).ok_or_else(|| {
        PersistentBootstrapError::Fatal(format!(
            "refusing to overwrite non-ah-generated unit file {}",
            unit_path.display()
        ))
    })?;
    if existing_state_dir != expected_state_dir {
        return Err(PersistentBootstrapError::Fatal(format!(
            "refusing to overwrite ahd unit {} for different AH_STATE_DIR: existing={}, desired={}",
            unit_path.display(),
            existing_state_dir,
            expected_state_dir
        )));
    }
    Ok(())
}

fn generated_state_dir_marker(content: &str) -> Option<String> {
    content
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("# ah-generated unit; AH_STATE_DIR="))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[derive(Debug, Default)]
struct UnitShow {
    fragment_path: Option<String>,
    load_state: Option<String>,
    active_state: Option<String>,
    transient: Option<bool>,
}

impl UnitShow {
    fn is_active(&self) -> bool {
        matches!(
            self.active_state.as_deref(),
            Some("active" | "activating" | "reloading")
        )
    }
}

fn parse_unit_show_output(output: &str) -> UnitShow {
    let mut show = UnitShow::default();
    for line in output.lines() {
        if let Some(value) = line.strip_prefix("FragmentPath=") {
            show.fragment_path = non_empty_string(value);
        } else if let Some(value) = line.strip_prefix("LoadState=") {
            show.load_state = non_empty_string(value);
        } else if let Some(value) = line.strip_prefix("ActiveState=") {
            show.active_state = non_empty_string(value);
        } else if let Some(value) = line.strip_prefix("Transient=") {
            show.transient = match value.trim() {
                "yes" => Some(true),
                "no" => Some(false),
                _ => None,
            };
        }
    }
    show
}

fn non_empty_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

pub fn linger_note_for_user(user: &str, output: &str) -> Option<String> {
    let linger = output
        .lines()
        .find_map(|line| line.strip_prefix("Linger="))
        .map(str::trim)?;
    if linger == "no" {
        Some(format!(
            "note: ahd user service is enabled for the user manager; boot-before-login/logout survival requires: loginctl enable-linger {user}"
        ))
    } else {
        None
    }
}

pub fn detect_linger_note() -> Option<String> {
    let user = std::env::var("USER")
        .ok()
        .filter(|value| !value.is_empty())?;
    let output = Command::new("loginctl")
        .args(["show-user", &user, "-p", "Linger"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    linger_note_for_user(&user, &stdout)
}

#[derive(Clone, Copy)]
enum FailureKind {
    Recoverable,
    Fatal,
}

fn run_required(
    runner: &impl SystemctlRunner,
    args: &[&str],
    failure_kind: FailureKind,
) -> Result<(), PersistentBootstrapError> {
    let output = runner.run(args).map_err(|err| {
        classified_error(
            failure_kind,
            format!(
                "systemctl --user {} failed to execute: {err}",
                args.join(" ")
            ),
        )
    })?;
    if output.status.success() {
        return Ok(());
    }
    Err(classified_error(
        failure_kind,
        format!(
            "systemctl --user {} failed: status={}, stderr={}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    ))
}

fn run_reset_failed_best_effort(runner: &impl SystemctlRunner, unit_name: &str) {
    match runner.run(&["reset-failed", unit_name]) {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            tracing::warn!(
                unit = unit_name,
                status = ?output.status,
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "systemctl reset-failed failed before ahd bootstrap"
            );
        }
        Err(err) => {
            tracing::warn!(
                unit = unit_name,
                error = %err,
                "systemctl reset-failed failed before ahd bootstrap"
            );
        }
    }
}

fn classified_error(kind: FailureKind, message: String) -> PersistentBootstrapError {
    match kind {
        FailureKind::Recoverable => PersistentBootstrapError::Recoverable(message),
        FailureKind::Fatal => PersistentBootstrapError::Fatal(message),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fs;
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;
    use std::process::{ExitStatus, Output};

    /// #24: a stopped stack must not come back at the next login. The unit is
    /// disabled *and* removed, so nothing under `default.target.wants/` points
    /// at a daemon the operator shut down.
    #[test]
    fn teardown_disables_and_removes_the_generated_unit() {
        let systemd_dir = tempfile::TempDir::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let runner = FakeRunner::new();
        bootstrap_persistent_unit_in_dir(
            &runner,
            systemd_dir.path(),
            Path::new("/usr/bin/ahd"),
            state_dir.path(),
            &[],
            false,
        )
        .unwrap();
        let unit_name = derive_unit_name(state_dir.path());
        assert!(systemd_dir.path().join(&unit_name).exists());

        let runner = FakeRunner::new();
        teardown_persistent_unit_in_dir(&runner, systemd_dir.path(), state_dir.path());

        assert!(
            !systemd_dir.path().join(&unit_name).exists(),
            "the unit file must be gone"
        );
        let calls = runner.calls();
        assert!(
            calls.iter().any(|call| call == &["disable", &unit_name]),
            "the unit must be disabled, or it still auto-starts: {calls:?}"
        );
        assert!(
            calls.iter().any(|call| call == &["daemon-reload"]),
            "systemd must be told the unit is gone: {calls:?}"
        );
    }

    /// The teardown is keyed on ah's own marker, so a unit that merely shares
    /// the generated name — someone else's, or one pointing at another stack —
    /// is never deleted.
    #[test]
    fn teardown_leaves_a_unit_it_did_not_generate_alone() {
        let systemd_dir = tempfile::TempDir::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let unit_name = derive_unit_name(state_dir.path());
        let unit_path = systemd_dir.path().join(&unit_name);
        fs::write(&unit_path, "[Unit]
Description=hand written
").unwrap();

        let runner = FakeRunner::new();
        teardown_persistent_unit_in_dir(&runner, systemd_dir.path(), state_dir.path());

        assert!(unit_path.exists(), "a foreign unit must survive");
        assert!(
            runner.calls().is_empty(),
            "no systemctl action on a unit ah did not generate: {:?}",
            runner.calls()
        );
    }

    #[test]
    fn teardown_is_a_no_op_when_no_unit_exists() {
        let systemd_dir = tempfile::TempDir::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let runner = FakeRunner::new();

        teardown_persistent_unit_in_dir(&runner, systemd_dir.path(), state_dir.path());

        assert!(runner.calls().is_empty());
    }

    #[derive(Debug)]
    struct FakeRunner {
        calls: RefCell<Vec<Vec<String>>>,
        failures: Vec<Vec<String>>,
        errors: Vec<Vec<String>>,
        stdout: Vec<(Vec<String>, String)>,
    }

    impl FakeRunner {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                failures: Vec::new(),
                errors: Vec::new(),
                stdout: Vec::new(),
            }
        }

        fn failing(args: &[&str]) -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                failures: vec![args.iter().map(|arg| (*arg).to_string()).collect()],
                errors: Vec::new(),
                stdout: Vec::new(),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.borrow().clone()
        }

        fn with_stdout(mut self, args: &[&str], stdout: &str) -> Self {
            self.stdout.push((
                args.iter().map(|arg| (*arg).to_string()).collect(),
                stdout.to_string(),
            ));
            self
        }

        fn erroring(args: &[&str]) -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                failures: Vec::new(),
                errors: vec![args.iter().map(|arg| (*arg).to_string()).collect()],
                stdout: Vec::new(),
            }
        }
    }

    impl SystemctlRunner for FakeRunner {
        fn run(&self, args: &[&str]) -> std::io::Result<Output> {
            let call = args
                .iter()
                .map(|arg| (*arg).to_string())
                .collect::<Vec<_>>();
            self.calls.borrow_mut().push(call.clone());
            if self.errors.iter().any(|error| error == &call) {
                return Err(std::io::Error::other("fake systemctl failure"));
            }
            let success = !self.failures.iter().any(|failure| failure == &call);
            let stdout = self
                .stdout
                .iter()
                .find_map(|(candidate, stdout)| (candidate == &call).then_some(stdout.as_str()))
                .unwrap_or("");
            Ok(output(if success { 0 } else { 1 }, stdout))
        }
    }

    fn output(code: i32, stdout: &str) -> Output {
        Output {
            status: ExitStatus::from_raw(code),
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    fn setup() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let systemd_dir = tmp.path().join("systemd/user");
        let state_dir = tmp.path().join("state");
        let ahd_bin = tmp.path().join("bin/ahd");
        fs::create_dir_all(ahd_bin.parent().unwrap()).unwrap();
        fs::write(&ahd_bin, "").unwrap();
        fs::create_dir_all(&state_dir).unwrap();
        (tmp, systemd_dir, state_dir, ahd_bin)
    }

    fn show_args_with_active_state() -> [&'static str; 10] {
        [
            "show",
            "ahd.service",
            "-p",
            "FragmentPath",
            "-p",
            "LoadState",
            "-p",
            "ActiveState",
            "-p",
            "Transient",
        ]
    }

    fn default_show_stdout() -> &'static str {
        "FragmentPath=\nLoadState=not-found\nActiveState=inactive\nTransient=no\n"
    }

    fn expected_show_call() -> Vec<&'static str> {
        vec![
            "show",
            "ahd.service",
            "-p",
            "FragmentPath",
            "-p",
            "LoadState",
            "-p",
            "ActiveState",
            "-p",
            "Transient",
        ]
    }

    fn marker_for(path: &std::path::Path) -> String {
        format!("# ah-generated unit; AH_STATE_DIR={}\n", path.display())
    }

    fn disable_now_call(unit: &str) -> Vec<String> {
        vec!["disable".into(), "--now".into(), unit.into()]
    }

    #[test]
    fn service_bootstrap_user_manager_probe_accepts_real_manager_states() {
        for state in [
            "running",
            "degraded",
            "starting",
            "maintenance",
            "initializing",
        ] {
            assert!(user_manager_available_from(
                Some("/run/user/1000"),
                Some(state)
            ));
        }
    }

    #[test]
    fn service_bootstrap_user_manager_probe_rejects_offline_missing_runtime_or_failed_probe() {
        assert!(!user_manager_available_from(
            Some("/run/user/1000"),
            Some("offline")
        ));
        assert!(!user_manager_available_from(None, Some("running")));
        assert!(!user_manager_available_from(Some(""), Some("running")));
        assert!(!user_manager_available_from(Some("/run/user/1000"), None));
    }

    #[test]
    fn service_bootstrap_linger_note_parses_linger_status() {
        assert!(linger_note_for_user("alice", "Linger=no").is_some());
        assert!(linger_note_for_user("alice", "Linger=yes").is_none());
        assert!(linger_note_for_user("alice", "").is_none());
    }

    #[test]
    fn service_bootstrap_fresh_install_reloads_enables_and_restarts() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());

        let unit = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap();

        assert!(unit.starts_with("ah-"));
        assert!(systemd_dir.join(&unit).exists());
        assert_eq!(
            runner.calls(),
            vec![
                expected_show_call(),
                vec!["daemon-reload"],
                vec!["reset-failed", unit.as_str()],
                vec!["enable", unit.as_str()],
                vec!["restart", unit.as_str()],
            ]
        );
    }

    #[test]
    fn service_bootstrap_idempotent_install_does_not_rewrite_but_enables_and_starts() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());
        let unit = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap();
        let unit_path = systemd_dir.join(&unit);
        let before = fs::metadata(&unit_path).unwrap().modified().unwrap();

        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());
        let second = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap();

        assert_eq!(unit, second);
        assert_eq!(
            fs::metadata(&unit_path).unwrap().modified().unwrap(),
            before
        );
        assert_eq!(
            runner.calls(),
            vec![
                expected_show_call(),
                vec!["daemon-reload"],
                vec!["reset-failed", unit.as_str()],
                vec!["enable", unit.as_str()],
                vec!["restart", unit.as_str()],
            ]
        );
    }

    #[test]
    fn service_bootstrap_changed_content_reloads_and_restarts() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);
        fs::create_dir_all(&systemd_dir).unwrap();
        fs::write(
            systemd_dir.join(&unit),
            format!("{}old content\n", marker_for(&state_dir)),
        )
        .unwrap();
        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert_eq!(
            runner.calls(),
            vec![
                expected_show_call(),
                vec!["daemon-reload"],
                vec!["reset-failed", unit.as_str()],
                vec!["enable", unit.as_str()],
                vec!["restart", unit.as_str()],
            ]
        );
    }

    #[test]
    fn service_bootstrap_reset_failed_failure_is_ignored() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);
        let runner = FakeRunner::failing(&["reset-failed", &unit])
            .with_stdout(&show_args_with_active_state(), default_show_stdout());

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert!(
            runner
                .calls()
                .contains(&vec!["enable".into(), unit.clone()])
        );
        assert!(runner.calls().contains(&vec!["restart".into(), unit]));
    }

    #[test]
    fn service_bootstrap_daemon_reload_failure_is_recoverable() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::failing(&["daemon-reload"])
            .with_stdout(&show_args_with_active_state(), default_show_stdout());

        let err = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap_err();

        assert!(err.is_recoverable());
    }

    #[test]
    fn service_bootstrap_enable_and_restart_failures_are_fatal() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);

        let enable_runner = FakeRunner::failing(&["enable", &unit])
            .with_stdout(&show_args_with_active_state(), default_show_stdout());
        assert!(
            bootstrap_persistent_unit_in_dir(
                &enable_runner,
                &systemd_dir,
                &ahd_bin,
                &state_dir,
                &[],
                false,
            )
            .unwrap_err()
            .is_fatal()
        );

        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);
        let start_runner = FakeRunner::failing(&["restart", &unit])
            .with_stdout(&show_args_with_active_state(), default_show_stdout());
        assert!(
            bootstrap_persistent_unit_in_dir(
                &start_runner,
                &systemd_dir,
                &ahd_bin,
                &state_dir,
                &[],
                false,
            )
            .unwrap_err()
            .is_fatal()
        );
    }

    #[test]
    fn service_bootstrap_stops_active_transient_ahd_before_install() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::new().with_stdout(
            &show_args_with_active_state(),
            "FragmentPath=\nLoadState=loaded\nActiveState=active\nTransient=yes\n",
        );
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert_eq!(
            runner.calls(),
            vec![
                expected_show_call(),
                vec!["stop", "ahd.service"],
                vec!["daemon-reload"],
                vec!["reset-failed", unit.as_str()],
                vec!["enable", unit.as_str()],
                vec!["restart", unit.as_str()],
            ]
        );
    }

    #[test]
    fn service_bootstrap_does_not_stop_installed_legacy_ahd_service() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::new().with_stdout(
            &show_args_with_active_state(),
            "FragmentPath=/home/alice/.config/systemd/user/ahd.service\nLoadState=loaded\nActiveState=active\nTransient=no\n",
        );

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert!(
            !runner
                .calls()
                .iter()
                .any(|call| call == &vec!["stop".to_string(), "ahd.service".to_string()])
        );
    }

    #[test]
    fn service_bootstrap_does_not_stop_inactive_legacy_ahd_service() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::new().with_stdout(
            &show_args_with_active_state(),
            "FragmentPath=\nLoadState=loaded\nActiveState=inactive\nTransient=yes\n",
        );

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert!(
            !runner
                .calls()
                .iter()
                .any(|call| call == &vec!["stop".to_string(), "ahd.service".to_string()])
        );
    }

    #[test]
    fn service_bootstrap_ignores_missing_or_unreadable_legacy_ahd_service() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());
        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();
        assert!(
            !runner
                .calls()
                .iter()
                .any(|call| call == &vec!["stop".to_string(), "ahd.service".to_string()])
        );

        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::erroring(&show_args_with_active_state());
        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();
        assert!(
            !runner
                .calls()
                .iter()
                .any(|call| call == &vec!["stop".to_string(), "ahd.service".to_string()])
        );
    }

    #[test]
    fn service_bootstrap_stop_transient_failure_is_fatal() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::failing(&["stop", "ahd.service"]).with_stdout(
            &show_args_with_active_state(),
            "FragmentPath=\nLoadState=loaded\nActiveState=active\nTransient=yes\n",
        );

        let err = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap_err();

        assert!(err.is_fatal());
    }

    #[test]
    fn service_bootstrap_stops_real_transient_unit_by_transient_field() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::new().with_stdout(
            &show_args_with_active_state(),
            "FragmentPath=/run/user/1001/systemd/transient/ahd.service\nLoadState=loaded\nActiveState=active\nTransient=yes\n",
        );

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert!(
            runner
                .calls()
                .iter()
                .any(|call| call == &vec!["stop".to_string(), "ahd.service".to_string()])
        );
    }

    #[test]
    fn service_bootstrap_does_not_stop_installed_legacy_ahd_service_in_local_share() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let runner = FakeRunner::new().with_stdout(
            &show_args_with_active_state(),
            "FragmentPath=/home/alice/.local/share/systemd/user/ahd.service\nLoadState=loaded\nActiveState=active\nTransient=no\n",
        );

        bootstrap_persistent_unit_in_dir(&runner, &systemd_dir, &ahd_bin, &state_dir, &[], false)
            .unwrap();

        assert!(
            !runner
                .calls()
                .iter()
                .any(|call| call == &vec!["stop".to_string(), "ahd.service".to_string()])
        );
    }

    #[test]
    fn service_bootstrap_rejects_hash_collision_with_different_state_dir_marker() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);
        let other_state = state_dir.parent().unwrap().join("other-state");
        fs::create_dir_all(&systemd_dir).unwrap();
        fs::write(systemd_dir.join(&unit), marker_for(&other_state)).unwrap();
        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());

        let err = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap_err();

        assert!(err.is_fatal());
    }

    #[test]
    fn service_bootstrap_rejects_existing_unit_without_ah_marker() {
        let (_tmp, systemd_dir, state_dir, ahd_bin) = setup();
        let unit = crate::cli::service_unit::derive_unit_name(&state_dir);
        fs::create_dir_all(&systemd_dir).unwrap();
        fs::write(systemd_dir.join(&unit), "[Service]\nExecStart=/bin/true\n").unwrap();
        let runner =
            FakeRunner::new().with_stdout(&show_args_with_active_state(), default_show_stdout());

        let err = bootstrap_persistent_unit_in_dir(
            &runner,
            &systemd_dir,
            &ahd_bin,
            &state_dir,
            &[],
            false,
        )
        .unwrap_err();

        assert!(err.is_fatal());
    }

    #[test]
    fn service_bootstrap_gc_stale_generated_unit_disables_and_deletes_file() {
        let tmp = tempfile::tempdir().unwrap();
        let systemd_dir = tmp.path().join("systemd/user");
        fs::create_dir_all(&systemd_dir).unwrap();
        let unit = "ah-deadbeef0badf00d.service";
        let stale_state_dir = tmp.path().join("missing-state");
        let unit_path = systemd_dir.join(unit);
        fs::write(&unit_path, marker_for(&stale_state_dir)).unwrap();
        let runner = FakeRunner::new();

        gc_stale_units_in_dir(&runner, &systemd_dir);

        assert!(runner.calls().contains(&disable_now_call(unit)));
        assert!(!unit_path.exists());
    }

    #[test]
    fn service_bootstrap_gc_live_generated_unit_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let systemd_dir = tmp.path().join("systemd/user");
        let live_state_dir = tmp.path().join("live-state");
        fs::create_dir_all(&systemd_dir).unwrap();
        fs::create_dir_all(&live_state_dir).unwrap();
        let unit = "ah-1111111111111111.service";
        let unit_path = systemd_dir.join(unit);
        fs::write(&unit_path, marker_for(&live_state_dir)).unwrap();
        let runner = FakeRunner::new();

        gc_stale_units_in_dir(&runner, &systemd_dir);

        assert!(runner.calls().is_empty());
        assert!(unit_path.exists());
    }

    #[test]
    fn service_bootstrap_gc_non_generated_unit_is_never_touched() {
        let tmp = tempfile::tempdir().unwrap();
        let systemd_dir = tmp.path().join("systemd/user");
        fs::create_dir_all(&systemd_dir).unwrap();
        let unit_path = systemd_dir.join("ah-2222222222222222.service");
        fs::write(&unit_path, "[Service]\nExecStart=/bin/true\n").unwrap();
        let runner = FakeRunner::new();

        gc_stale_units_in_dir(&runner, &systemd_dir);

        assert!(runner.calls().is_empty());
        assert!(unit_path.exists());
    }

    #[test]
    fn service_bootstrap_gc_disable_failure_still_deletes_file() {
        let tmp = tempfile::tempdir().unwrap();
        let systemd_dir = tmp.path().join("systemd/user");
        fs::create_dir_all(&systemd_dir).unwrap();
        let unit = "ah-3333333333333333.service";
        let stale_state_dir = tmp.path().join("missing-state");
        let unit_path = systemd_dir.join(unit);
        fs::write(&unit_path, marker_for(&stale_state_dir)).unwrap();
        let runner = FakeRunner::failing(&["disable", "--now", unit]);

        gc_stale_units_in_dir(&runner, &systemd_dir);

        assert!(runner.calls().contains(&disable_now_call(unit)));
        assert!(!unit_path.exists());
    }

    #[test]
    fn service_bootstrap_gc_mixed_files_only_removes_stale_generated_unit() {
        let tmp = tempfile::tempdir().unwrap();
        let systemd_dir = tmp.path().join("systemd/user");
        let live_state_dir = tmp.path().join("live-state");
        fs::create_dir_all(&systemd_dir).unwrap();
        fs::create_dir_all(&live_state_dir).unwrap();
        let stale_unit = "ah-4444444444444444.service";
        let live_unit = "ah-5555555555555555.service";
        let user_unit = "ah-6666666666666666.service";
        let scope = "ahd-tmux-x.scope";
        let stale_path = systemd_dir.join(stale_unit);
        let live_path = systemd_dir.join(live_unit);
        let user_path = systemd_dir.join(user_unit);
        let scope_path = systemd_dir.join(scope);
        fs::write(&stale_path, marker_for(&tmp.path().join("missing-state"))).unwrap();
        fs::write(&live_path, marker_for(&live_state_dir)).unwrap();
        fs::write(&user_path, "[Service]\nExecStart=/bin/true\n").unwrap();
        fs::write(&scope_path, "[Scope]\n").unwrap();
        let runner = FakeRunner::new();

        gc_stale_units_in_dir(&runner, &systemd_dir);

        assert_eq!(runner.calls(), vec![disable_now_call(stale_unit)]);
        assert!(!stale_path.exists());
        assert!(live_path.exists());
        assert!(user_path.exists());
        assert!(scope_path.exists());
    }
}
