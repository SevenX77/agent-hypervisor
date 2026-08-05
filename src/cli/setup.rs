use crate::cli::doctor::{DoctorCheck, system_binary_checks};
use crate::cli::prereq::{
    FixOwner, PrereqStatus, RuntimePrerequisite, runtime_prerequisite_from_doctor,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SetupOptions {
    pub check: bool,
    pub fix: bool,
    pub yes: bool,
    pub json: bool,
    pub resume: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupOverallStatus {
    Pass,
    Warn,
    Fail,
    Fixed,
    NeedsWslShutdown,
    NeedsWindowsReboot,
    NeedsDistroReopen,
    NeedsDistroFirstLaunch,
    Unsupported,
    PermissionDenied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupPhase {
    Phase0Check,
    Phase1Distro,
    Phase2WindowsHost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NextAction {
    pub kind: String,
    pub message: String,
    pub command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupEnvelope {
    pub schema_version: u32,
    pub operation_id: String,
    pub overall_status: SetupOverallStatus,
    pub phase: SetupPhase,
    pub selected_distro: Option<String>,
    pub next_action: Option<NextAction>,
    pub resume_command: Option<String>,
    pub steps: Vec<RuntimePrerequisite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupRun {
    pub envelope: SetupEnvelope,
    pub output: String,
    pub exit_code: i32,
}

#[derive(Debug)]
pub enum SetupError {
    Io(io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetupError::Io(err) => write!(f, "{err}"),
            SetupError::Json(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SetupError {}

impl From<io::Error> for SetupError {
    fn from(value: io::Error) -> Self {
        SetupError::Io(value)
    }
}

impl From<serde_json::Error> for SetupError {
    fn from(value: serde_json::Error) -> Self {
        SetupError::Json(value)
    }
}

pub fn run_setup(options: SetupOptions) -> Result<SetupRun, SetupError> {
    let checks = collect_setup_doctor_checks();
    let selected_distro = std::env::var("WSL_DISTRO_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let steps = setup_prerequisites_from_checks(checks);
    let operation_id = uuid::Uuid::new_v4().to_string();
    let envelope = if options.fix {
        let runner = RealSetupRunner;
        let paths = SetupPaths::default();
        build_phase1_fix_envelope(
            steps,
            selected_distro,
            operation_id,
            options,
            &runner,
            &paths,
        )?
    } else {
        build_setup_envelope(steps, selected_distro, operation_id, options)
    };
    let exit_code = exit_code_for_overall_status(envelope.overall_status);
    let output = if options.json {
        format!("{}\n", serde_json::to_string_pretty(&envelope)?)
    } else {
        render_setup_text(&envelope, options)
    };
    Ok(SetupRun {
        envelope,
        output,
        exit_code,
    })
}

pub fn collect_setup_doctor_checks() -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    checks.extend(system_binary_checks());
    checks.extend(crate::cli::wsl::wsl_onboarding_checks());
    checks
}

pub fn setup_prerequisites_from_checks(checks: Vec<DoctorCheck>) -> Vec<RuntimePrerequisite> {
    checks
        .into_iter()
        .map(runtime_prerequisite_from_doctor)
        .filter(|step| step.owner == FixOwner::AhRuntime)
        .collect()
}

pub fn build_setup_envelope(
    steps: Vec<RuntimePrerequisite>,
    selected_distro: Option<String>,
    operation_id: String,
    options: SetupOptions,
) -> SetupEnvelope {
    let overall_status = overall_status_for_steps(&steps);
    let next_action = next_action_for(overall_status, options);

    SetupEnvelope {
        schema_version: 1,
        operation_id,
        overall_status,
        phase: SetupPhase::Phase0Check,
        selected_distro,
        next_action,
        // next_action.command is the immediate command to run now; resume_command
        // is only populated after a boundary exit leaves persisted setup state.
        resume_command: None,
        steps,
    }
}

fn overall_status_for_steps(steps: &[RuntimePrerequisite]) -> SetupOverallStatus {
    if steps
        .iter()
        .any(|step| step.status == PrereqStatus::NeedsWindowsReboot)
    {
        SetupOverallStatus::NeedsWindowsReboot
    } else if steps
        .iter()
        .any(|step| step.status == PrereqStatus::NeedsWslShutdown)
    {
        SetupOverallStatus::NeedsWslShutdown
    } else if steps
        .iter()
        .any(|step| step.status == PrereqStatus::NeedsDistroFirstLaunch)
    {
        SetupOverallStatus::NeedsDistroFirstLaunch
    } else if steps
        .iter()
        .any(|step| step.status == PrereqStatus::NeedsDistroReopen)
    {
        SetupOverallStatus::NeedsDistroReopen
    } else if steps
        .iter()
        .any(|step| step.status == PrereqStatus::PermissionDenied)
    {
        SetupOverallStatus::PermissionDenied
    } else if steps
        .iter()
        .any(|step| step.status == PrereqStatus::Unsupported)
    {
        SetupOverallStatus::Unsupported
    } else if steps.iter().any(|step| step.status == PrereqStatus::Fail) {
        SetupOverallStatus::Fail
    } else if steps.iter().any(|step| step.status == PrereqStatus::Fixed) {
        SetupOverallStatus::Fixed
    } else if steps.iter().any(|step| step.status == PrereqStatus::Warn) {
        SetupOverallStatus::Warn
    } else {
        SetupOverallStatus::Pass
    }
}

fn next_action_for(
    overall_status: SetupOverallStatus,
    options: SetupOptions,
) -> Option<NextAction> {
    match overall_status {
        SetupOverallStatus::Fail if options.fix => Some(NextAction {
            kind: "manual_fix".to_string(),
            message: "Apply the listed manual guidance, then rerun ah setup --check.".to_string(),
            command: Some("ah setup --check".to_string()),
        }),
        SetupOverallStatus::Fail => Some(NextAction {
            kind: "run_fix".to_string(),
            message: "Run ah setup --fix after Phase 1 support lands, or apply the listed manual guidance."
                .to_string(),
            command: Some("ah setup --fix".to_string()),
        }),
        SetupOverallStatus::Warn => Some(NextAction {
            kind: "review_warnings".to_string(),
            message: "Review warnings; no machine state was changed.".to_string(),
            command: Some("ah setup --check".to_string()),
        }),
        SetupOverallStatus::Unsupported => Some(NextAction {
            kind: "unsupported_manual_fix".to_string(),
            message: "This distro is not supported by ah setup --fix; apply the listed manual guidance."
                .to_string(),
            command: Some("ah setup --check".to_string()),
        }),
        SetupOverallStatus::PermissionDenied => Some(NextAction {
            kind: "permission_required".to_string(),
            message: "Required sudo/admin permission was denied; resolve permissions and rerun setup."
                .to_string(),
            command: Some("ah setup --fix".to_string()),
        }),
        SetupOverallStatus::NeedsWslShutdown => Some(wsl_shutdown_next_action()),
        _ => None,
    }
}

pub fn exit_code_for_overall_status(status: SetupOverallStatus) -> i32 {
    match status {
        SetupOverallStatus::Pass | SetupOverallStatus::Warn | SetupOverallStatus::Fixed => 0,
        SetupOverallStatus::Fail | SetupOverallStatus::PermissionDenied => 1,
        SetupOverallStatus::Unsupported => 2,
        SetupOverallStatus::NeedsWslShutdown => 10,
        SetupOverallStatus::NeedsWindowsReboot => 11,
        SetupOverallStatus::NeedsDistroReopen | SetupOverallStatus::NeedsDistroFirstLaunch => 12,
    }
}

pub fn render_setup_text(envelope: &SetupEnvelope, options: SetupOptions) -> String {
    let mut out = String::new();
    if options.fix {
        out.push_str("ah setup result\n");
    } else if options.check {
        out.push_str("ah setup plan (read-only)\n");
        out.push_str("Check mode: no fixes were applied.\n");
    } else {
        out.push_str("ah setup plan (read-only)\n");
        out.push_str(
            "Default mode: no fixes were applied. Use --fix in Phase 1 to apply supported fixes.\n",
        );
    }
    if options.yes {
        out.push_str("--yes has no effect in Phase 0 because no mutation is implemented.\n");
    }
    if options.resume {
        out.push_str("--resume found no Phase 0 boundary state; probes were re-run from current machine state.\n");
    }
    out.push_str(&format!("overall_status: {:?}\n", envelope.overall_status));

    for step in &envelope.steps {
        out.push_str(&format!(
            "- {}: {:?} - {}",
            step.id, step.status, step.detail
        ));
        if let Some(suggestion) = &step.suggestion {
            out.push_str(&format!("\n  suggestion: {suggestion}"));
        }
        out.push('\n');
    }

    if let Some(action) = &envelope.next_action {
        out.push_str(&format!("next_action: {}\n", action.message));
        if let Some(command) = &action.command {
            out.push_str(&format!("next_command: {command}\n"));
        }
    }
    if let Some(resume_command) = &envelope.resume_command {
        out.push_str(&format!("resume_command: {resume_command}\n"));
    }
    out.push_str("status_check: ah setup --check\n");
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingRestart {
    None,
    WslShutdown,
    DistroReopen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistroSetupState {
    pub schema_version: u32,
    pub operation_id: String,
    pub phase: SetupPhase,
    pub boundary: String,
    pub distro_name: Option<String>,
    pub last_completed_step: Option<String>,
    pub pending_restart: PendingRestart,
    pub wsl_conf_hash_before: Option<String>,
    pub wsl_conf_hash_after: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SetupPaths {
    pub state_dir: PathBuf,
    pub wsl_conf: PathBuf,
    pub os_release: PathBuf,
}

impl Default for SetupPaths {
    fn default() -> Self {
        Self {
            state_dir: crate::state_layout::resolve_neutral_state_layout().state_dir,
            wsl_conf: PathBuf::from("/etc/wsl.conf"),
            os_release: PathBuf::from("/etc/os-release"),
        }
    }
}

impl SetupPaths {
    fn state_file(&self) -> PathBuf {
        self.state_dir.join("setup").join("state.json")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CommandResult {
    fn success(&self) -> bool {
        self.status == 0
    }
}

pub trait SetupRunner {
    fn command_exists(&self, program: &str) -> bool;
    fn run_sudo(&self, args: &[&str], non_interactive: bool) -> io::Result<CommandResult>;
    fn confirm(&self, prompt: &str, assume_yes: bool) -> io::Result<bool>;
}

struct RealSetupRunner;

impl SetupRunner for RealSetupRunner {
    fn command_exists(&self, program: &str) -> bool {
        which::which(program).is_ok()
    }

    fn run_sudo(&self, args: &[&str], non_interactive: bool) -> io::Result<CommandResult> {
        let mut cmd = Command::new("sudo");
        if non_interactive {
            cmd.arg("-n");
        }
        cmd.args(args);
        let output = cmd.output()?;
        Ok(CommandResult {
            status: output.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }

    fn confirm(&self, prompt: &str, assume_yes: bool) -> io::Result<bool> {
        if assume_yes {
            return Ok(true);
        }
        eprint!("{prompt} [y/N] ");
        io::stderr().flush()?;
        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "YES"))
    }
}

pub fn build_phase1_fix_envelope(
    mut steps: Vec<RuntimePrerequisite>,
    selected_distro: Option<String>,
    operation_id: String,
    options: SetupOptions,
    runner: &impl SetupRunner,
    paths: &SetupPaths,
) -> Result<SetupEnvelope, SetupError> {
    let is_wsl = steps
        .iter()
        .any(|step| step.id == "wsl" && step.status == PrereqStatus::Pass);

    if !is_wsl {
        mark_outside_wsl_manual(&mut steps);
        return Ok(SetupEnvelope {
            schema_version: 1,
            operation_id,
            overall_status: overall_status_for_steps(&steps),
            phase: SetupPhase::Phase1Distro,
            selected_distro,
            next_action: Some(NextAction {
                kind: "manual_fix".to_string(),
                message: "ah setup --fix only applies distro-local mutations inside WSL; apply the listed manual guidance."
                    .to_string(),
                command: Some("ah setup --check".to_string()),
            }),
            resume_command: None,
            steps,
        });
    }

    if let Some(state) = read_setup_state(paths)? {
        if state.pending_restart == PendingRestart::WslShutdown
            && step_passes(&steps, "wsl:systemd-user")
        {
            let _ = std::fs::remove_file(paths.state_file());
        }
    }

    if !apt_supported(paths, runner) {
        mark_unsupported_distro(&mut steps);
        let overall_status = overall_status_for_steps(&steps);
        return Ok(SetupEnvelope {
            schema_version: 1,
            operation_id,
            overall_status,
            phase: SetupPhase::Phase1Distro,
            selected_distro,
            next_action: next_action_for(overall_status, options),
            resume_command: None,
            steps,
        });
    }

    let mut changed = Vec::new();
    if step_needs_fix(&steps, "wsl:tmux") || step_needs_fix(&steps, "binary:tmux") {
        let outcome = install_tmux_if_needed(runner, options.yes)?;
        apply_step_outcome(&mut steps, "wsl:tmux", &outcome);
        apply_step_outcome(&mut steps, "binary:tmux", &outcome);
        if outcome.status == PrereqStatus::Fixed {
            changed.push("Installed tmux with apt-get".to_string());
        }
    }

    if step_needs_fix(&steps, "wsl:browser-bridge") {
        let outcome = install_browser_bridge_if_needed(runner, options.yes)?;
        apply_step_outcome(&mut steps, "wsl:browser-bridge", &outcome);
        if outcome.status == PrereqStatus::Fixed {
            changed.push(
                "Installed the WSL-to-Windows browser opener at /usr/local/bin/xdg-open"
                    .to_string(),
            );
        }
    }

    let mut wsl_hash_before = None;
    let mut wsl_hash_after = None;
    if step_needs_fix(&steps, "wsl:systemd-user") || step_needs_fix(&steps, "binary:systemd-run") {
        let edit = plan_wsl_conf_systemd_enable(&read_optional(&paths.wsl_conf)?);
        match edit {
            WslConfEdit::AlreadyEnabled => {
                mark_step_fixed_detail(
                    &mut steps,
                    "wsl:systemd-user",
                    PrereqStatus::NeedsWslShutdown,
                    "wsl.conf already has [boot] systemd=true, but the systemd user session is not available yet",
                    Some("Run wsl --shutdown from PowerShell, then reopen the distro and run ah setup --check".to_string()),
                );
            }
            WslConfEdit::Malformed { detail } => {
                mark_step_fixed_detail(
                    &mut steps,
                    "wsl:systemd-user",
                    PrereqStatus::Fail,
                    detail,
                    Some("review /etc/wsl.conf manually, then rerun ah setup --fix".to_string()),
                );
            }
            WslConfEdit::Write { before, after } => {
                if runner.confirm(
                    "ah setup needs sudo to update /etc/wsl.conf with [boot] systemd=true",
                    options.yes,
                )? {
                    wsl_hash_before = Some(hash_text(&before));
                    wsl_hash_after = Some(hash_text(&after));
                    let result = write_wsl_conf_with_sudo(runner, &after, options.yes)?;
                    if result.success() {
                        mark_step_fixed_detail(
                            &mut steps,
                            "wsl:systemd-user",
                            PrereqStatus::NeedsWslShutdown,
                            "Updated /etc/wsl.conf to set [boot] systemd=true",
                            Some("Run wsl --shutdown from PowerShell, then reopen the distro and run ah setup --check".to_string()),
                        );
                        mark_step_fixed_detail(
                            &mut steps,
                            "binary:systemd-run",
                            PrereqStatus::NeedsWslShutdown,
                            "WSL systemd enablement is pending WSL shutdown",
                            Some("Run wsl --shutdown from PowerShell, then reopen the distro and run ah setup --check".to_string()),
                        );
                        changed
                            .push("Updated /etc/wsl.conf to set [boot] systemd=true".to_string());
                    } else {
                        mark_step_fixed_detail(
                            &mut steps,
                            "wsl:systemd-user",
                            PrereqStatus::Fail,
                            format!("failed to write /etc/wsl.conf: {}", result.stderr.trim()),
                            Some("fix sudo permissions or edit /etc/wsl.conf manually".to_string()),
                        );
                    }
                } else {
                    mark_step_fixed_detail(
                        &mut steps,
                        "wsl:systemd-user",
                        PrereqStatus::PermissionDenied,
                        "user declined /etc/wsl.conf update",
                        Some("rerun ah setup --fix and approve the sudo prompt, or edit /etc/wsl.conf manually".to_string()),
                    );
                }
            }
        }
    }

    let overall_status = overall_status_for_steps(&steps);
    if overall_status == SetupOverallStatus::NeedsWslShutdown {
        write_setup_state(
            paths,
            &DistroSetupState {
                schema_version: 1,
                operation_id: operation_id.clone(),
                phase: SetupPhase::Phase1Distro,
                boundary: "distro".to_string(),
                distro_name: selected_distro.clone(),
                last_completed_step: changed.last().cloned(),
                pending_restart: PendingRestart::WslShutdown,
                wsl_conf_hash_before: wsl_hash_before,
                wsl_conf_hash_after: wsl_hash_after,
                created_at: now_unix(),
                updated_at: now_unix(),
                last_error: None,
            },
        )?;
    }

    Ok(SetupEnvelope {
        schema_version: 1,
        operation_id,
        overall_status,
        phase: SetupPhase::Phase1Distro,
        selected_distro,
        next_action: next_action_for(overall_status, options),
        resume_command: (overall_status == SetupOverallStatus::NeedsWslShutdown)
            .then(|| "ah setup --resume --check".to_string()),
        steps,
    })
}

/// Whether `--fix` should act on a step.
///
/// A Warn counts when the step declares a fix: the browser bridge warns rather
/// than fails (sign-in still works without the pop), and gating fixes on Fail
/// alone silently skipped it — found live, when `ah setup --fix` reported the
/// warn back instead of installing the opener.
fn step_needs_fix(steps: &[RuntimePrerequisite], id: &str) -> bool {
    steps.iter().any(|step| {
        step.id == id
            && (step.status == PrereqStatus::Fail
                || (step.status == PrereqStatus::Warn && step.fix_available))
    })
}

fn step_passes(steps: &[RuntimePrerequisite], id: &str) -> bool {
    steps
        .iter()
        .any(|step| step.id == id && step.status == PrereqStatus::Pass)
}

fn mark_outside_wsl_manual(steps: &mut [RuntimePrerequisite]) {
    for step in steps {
        if step.status == PrereqStatus::Fail
            && matches!(step.id.as_str(), "binary:tmux" | "binary:systemd-run")
        {
            step.detail = format!("{}; ah setup --fix only mutates WSL distros", step.detail);
            step.suggestion = Some(format!(
                "install {} manually for this host, or run ah setup inside WSL",
                step.id.trim_start_matches("binary:")
            ));
        }
    }
}

fn mark_unsupported_distro(steps: &mut [RuntimePrerequisite]) {
    for step in steps {
        if matches!(step.id.as_str(), "wsl:tmux" | "binary:tmux")
            && step.status == PrereqStatus::Fail
        {
            step.status = PrereqStatus::Unsupported;
            step.detail = "unsupported distro package manager: apt-get not available or distro is not Ubuntu/Debian"
                .to_string();
            step.suggestion = Some(
                "install tmux manually with your distro package manager, then rerun ah setup --check"
                    .to_string(),
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixOutcome {
    pub status: PrereqStatus,
    pub detail: String,
    pub suggestion: Option<String>,
}

fn apply_step_outcome(steps: &mut [RuntimePrerequisite], id: &str, outcome: &FixOutcome) {
    mark_step_fixed_detail(
        steps,
        id,
        outcome.status,
        outcome.detail.clone(),
        outcome.suggestion.clone(),
    );
}

fn mark_step_fixed_detail(
    steps: &mut [RuntimePrerequisite],
    id: &str,
    status: PrereqStatus,
    detail: impl Into<String>,
    suggestion: Option<String>,
) {
    if let Some(step) = steps.iter_mut().find(|step| step.id == id) {
        step.status = status;
        step.detail = detail.into();
        step.suggestion = suggestion;
    }
}

/// The sign-in doorman (decision 0006) launches provider login flows that
/// open a browser; inside WSL that needs a hand-off to the Windows side. The
/// shim is what `wslview` does at its core, with zero package dependencies.
const BROWSER_BRIDGE_SCRIPT: &str = "#!/bin/sh
# ah setup: WSL browser bridge - hand URLs to the Windows default browser.
exec /mnt/c/Windows/System32/rundll32.exe url.dll,FileProtocolHandler \"$1\"
";

fn install_browser_bridge_if_needed(
    runner: &impl SetupRunner,
    yes: bool,
) -> io::Result<FixOutcome> {
    if runner.command_exists("xdg-open") || runner.command_exists("wslview") {
        return Ok(FixOutcome {
            status: PrereqStatus::Pass,
            detail: "a browser opener is already installed".to_string(),
            suggestion: None,
        });
    }
    if !runner.confirm(
        "ah setup needs sudo to install /usr/local/bin/xdg-open (opens URLs in the Windows browser)",
        yes,
    )? {
        return Ok(FixOutcome {
            status: PrereqStatus::PermissionDenied,
            detail: "user declined the browser bridge installation".to_string(),
            suggestion: Some(
                "rerun ah setup --fix and approve, or install the wslu package manually".to_string(),
            ),
        });
    }
    // The shim text contains no single quotes, so it can sit inside one
    // single-quoted shell word verbatim.
    let script = format!(
        "printf '%s' '{BROWSER_BRIDGE_SCRIPT}' > /usr/local/bin/xdg-open && chmod 755 /usr/local/bin/xdg-open"
    );
    let result = runner.run_sudo(&["sh", "-c", &script], yes)?;
    if result.success() {
        Ok(FixOutcome {
            status: PrereqStatus::Fixed,
            detail: "installed /usr/local/bin/xdg-open".to_string(),
            suggestion: None,
        })
    } else {
        Ok(FixOutcome {
            status: PrereqStatus::Fail,
            detail: format!("failed to write /usr/local/bin/xdg-open: {}", result.stderr.trim()),
            suggestion: Some("install the wslu package manually, then rerun ah setup --check".to_string()),
        })
    }
}

fn install_tmux_if_needed(runner: &impl SetupRunner, yes: bool) -> io::Result<FixOutcome> {
    if runner.command_exists("tmux") {
        return Ok(FixOutcome {
            status: PrereqStatus::Pass,
            detail: "tmux already installed".to_string(),
            suggestion: None,
        });
    }
    if !runner.confirm("ah setup needs sudo to install tmux with apt-get", yes)? {
        return Ok(FixOutcome {
            status: PrereqStatus::PermissionDenied,
            detail: "user declined tmux installation".to_string(),
            suggestion: Some(
                "rerun ah setup --fix and approve the sudo prompt, or install tmux manually"
                    .to_string(),
            ),
        });
    }

    let update = runner.run_sudo(&["apt-get", "update"], yes)?;
    if !update.success() {
        return Ok(FixOutcome {
            status: PrereqStatus::Fail,
            detail: format!("apt-get update failed: {}", update.stderr.trim()),
            suggestion: Some("resolve apt errors, then rerun ah setup --fix".to_string()),
        });
    }
    let install = runner.run_sudo(&["apt-get", "install", "-y", "tmux"], yes)?;
    if !install.success() {
        return Ok(FixOutcome {
            status: PrereqStatus::Fail,
            detail: format!("apt-get install -y tmux failed: {}", install.stderr.trim()),
            suggestion: Some(
                "resolve apt errors or install tmux manually, then rerun ah setup --check"
                    .to_string(),
            ),
        });
    }
    Ok(FixOutcome {
        status: PrereqStatus::Fixed,
        detail: "Installed tmux with apt-get".to_string(),
        suggestion: None,
    })
}

fn write_wsl_conf_with_sudo(
    runner: &impl SetupRunner,
    content: &str,
    yes: bool,
) -> io::Result<CommandResult> {
    let script = format!("cat > /etc/wsl.conf <<'AH_WSL_CONF'\n{content}AH_WSL_CONF\n");
    runner.run_sudo(&["sh", "-c", &script], yes)
}

fn apt_supported(paths: &SetupPaths, runner: &impl SetupRunner) -> bool {
    runner.command_exists("apt-get")
        && os_release_is_debian_family(
            &read_optional(&paths.os_release)
                .ok()
                .flatten()
                .unwrap_or_default(),
        )
}

pub fn os_release_is_debian_family(content: &str) -> bool {
    let mut ids = Vec::new();
    for line in content.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key == "ID" || key == "ID_LIKE" {
            ids.extend(
                value
                    .trim_matches('"')
                    .split_whitespace()
                    .map(|part| part.to_ascii_lowercase()),
            );
        }
    }
    ids.iter().any(|id| id == "ubuntu" || id == "debian")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WslConfEdit {
    AlreadyEnabled,
    Write { before: String, after: String },
    Malformed { detail: String },
}

pub fn plan_wsl_conf_systemd_enable(content: &Option<String>) -> WslConfEdit {
    let before = content.clone().unwrap_or_default();
    if let Err(detail) = validate_wsl_conf(&before) {
        return WslConfEdit::Malformed { detail };
    }

    let mut lines = before
        .lines()
        .map(ToOwned::to_owned)
        .collect::<Vec<String>>();
    let mut boot_start = None;
    let mut boot_end = lines.len();
    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            if trimmed.eq_ignore_ascii_case("[boot]") {
                boot_start = Some(idx);
            } else if boot_start.is_some() {
                boot_end = idx;
                break;
            }
        }
    }

    if let Some(start) = boot_start {
        for line in lines.iter_mut().take(boot_end).skip(start + 1) {
            let trimmed = line.trim();
            if let Some((key, _)) = trimmed.split_once('=') {
                if key.trim().eq_ignore_ascii_case("systemd") {
                    if trimmed.eq_ignore_ascii_case("systemd=true") {
                        return WslConfEdit::AlreadyEnabled;
                    }
                    *line = "systemd=true".to_string();
                    return WslConfEdit::Write {
                        before,
                        after: join_wsl_conf_lines(&lines),
                    };
                }
            }
        }
        lines.insert(start + 1, "systemd=true".to_string());
    } else {
        if !lines.is_empty() && lines.last().is_some_and(|line| !line.trim().is_empty()) {
            lines.push(String::new());
        }
        lines.push("[boot]".to_string());
        lines.push("systemd=true".to_string());
    }

    WslConfEdit::Write {
        before,
        after: join_wsl_conf_lines(&lines),
    }
}

fn validate_wsl_conf(content: &str) -> Result<(), String> {
    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }
        if trimmed.starts_with('[') && trimmed.ends_with(']') && trimmed.len() > 2 {
            continue;
        }
        if trimmed.contains('=') {
            continue;
        }
        return Err(format!(
            "malformed /etc/wsl.conf line {}: {trimmed}",
            idx + 1
        ));
    }
    Ok(())
}

fn join_wsl_conf_lines(lines: &[String]) -> String {
    let mut out = lines.join("\n");
    out.push('\n');
    out
}

fn read_optional(path: &Path) -> io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(Some(content)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn read_setup_state(paths: &SetupPaths) -> Result<Option<DistroSetupState>, SetupError> {
    let Some(content) = read_optional(&paths.state_file())? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_str(&content)?))
}

fn write_setup_state(paths: &SetupPaths, state: &DistroSetupState) -> Result<(), SetupError> {
    let state_file = paths.state_file();
    if let Some(parent) = state_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(state_file, serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

fn hash_text(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn wsl_shutdown_next_action() -> NextAction {
    NextAction {
        kind: "needs_wsl_shutdown".to_string(),
        message: "Updated distro-local setup. Next run PowerShell: wsl --shutdown. This stops all running WSL distros, not only this distro. Reopen the distro, then run ah setup --resume --check. Inspect status any time with ah setup --check."
            .to_string(),
        command: Some("powershell.exe -NoProfile -Command \"wsl --shutdown\"".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommandResult, PendingRestart, SetupOptions, SetupOverallStatus, SetupPaths, SetupRunner,
        WslConfEdit, build_phase1_fix_envelope, build_setup_envelope, exit_code_for_overall_status,
        os_release_is_debian_family, plan_wsl_conf_systemd_enable, read_setup_state,
        setup_prerequisites_from_checks,
    };
    use crate::cli::doctor::{DoctorCheck, DoctorStatus};
    use crate::cli::prereq::{FixOwner, PrereqStatus};
    use crate::cli::wsl::{SystemdUserStatus, TmuxCheck, WslDetection, wsl_onboarding_checks_from};

    #[test]
    fn setup_filters_to_ah_runtime_prerequisites() {
        let steps = setup_prerequisites_from_checks(vec![
            DoctorCheck {
                name: "wsl:tmux".to_string(),
                status: DoctorStatus::Fail,
                detail: "not found".to_string(),
                suggestion: Some("install tmux".to_string()),
            },
            DoctorCheck {
                name: "provider:claude".to_string(),
                status: DoctorStatus::Warn,
                detail: "missing auth".to_string(),
                suggestion: None,
            },
            DoctorCheck {
                name: "daemon".to_string(),
                status: DoctorStatus::Fail,
                detail: "not running".to_string(),
                suggestion: None,
            },
        ]);

        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].id, "wsl:tmux");
        assert_eq!(steps[0].owner, FixOwner::AhRuntime);
        assert_eq!(steps[0].status, PrereqStatus::Fail);
    }

    #[test]
    fn json_envelope_has_phase0_contract_fields() {
        let steps = setup_prerequisites_from_checks(vec![DoctorCheck {
            name: "wsl:systemd-user".to_string(),
            status: DoctorStatus::Fail,
            detail: "not running".to_string(),
            suggestion: Some("enable systemd".to_string()),
        }]);
        let envelope = build_setup_envelope(
            steps,
            Some("Ubuntu".to_string()),
            "op-test".to_string(),
            SetupOptions {
                check: true,
                json: true,
                ..SetupOptions::default()
            },
        );
        let value = serde_json::to_value(&envelope).unwrap();

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["operation_id"], "op-test");
        assert_eq!(value["overall_status"], "fail");
        assert_eq!(value["phase"], "phase0_check");
        assert_eq!(value["selected_distro"], "Ubuntu");
        assert_eq!(value["resume_command"], serde_json::Value::Null);
        assert!(value["next_action"]["command"].as_str().is_some());
        assert_eq!(value["steps"][0]["id"], "wsl:systemd-user");
    }

    #[test]
    fn setup_status_matches_doctor_for_injected_wsl_checks() {
        let doctor_checks = wsl_onboarding_checks_from(
            WslDetection {
                is_wsl: true,
                source: Some("env"),
                container_hint: false,
            },
            SystemdUserStatus::MissingTool,
            TmuxCheck {
                available: false,
                detail: "not found in PATH".to_string(),
            },
        );
        let systemd_status = doctor_checks
            .iter()
            .find(|check| check.name == "wsl:systemd-user")
            .map(|check| check.status)
            .unwrap();
        let tmux_status = doctor_checks
            .iter()
            .find(|check| check.name == "wsl:tmux")
            .map(|check| check.status)
            .unwrap();

        let setup_steps = setup_prerequisites_from_checks(doctor_checks);

        assert_eq!(systemd_status, DoctorStatus::Fail);
        assert_eq!(tmux_status, DoctorStatus::Fail);
        assert_eq!(
            setup_steps
                .iter()
                .find(|step| step.id == "wsl:systemd-user")
                .map(|step| step.status),
            Some(PrereqStatus::Fail)
        );
        assert_eq!(
            setup_steps
                .iter()
                .find(|step| step.id == "wsl:tmux")
                .map(|step| step.status),
            Some(PrereqStatus::Fail)
        );
    }

    #[test]
    fn fix_placeholder_is_read_only_and_machine_distinguishable() {
        let steps = setup_prerequisites_from_checks(vec![DoctorCheck {
            name: "binary:tmux".to_string(),
            status: DoctorStatus::Fail,
            detail: "not found in PATH".to_string(),
            suggestion: Some("install tmux".to_string()),
        }]);
        let envelope = build_setup_envelope(
            steps,
            None,
            "op-test".to_string(),
            SetupOptions {
                fix: true,
                ..SetupOptions::default()
            },
        );

        assert_eq!(envelope.overall_status, SetupOverallStatus::Fail);
        assert_eq!(exit_code_for_overall_status(envelope.overall_status), 1);
        assert_eq!(
            envelope
                .next_action
                .as_ref()
                .map(|action| action.kind.as_str()),
            Some("manual_fix")
        );
        assert_eq!(envelope.resume_command, None);
    }

    #[derive(Default)]
    struct FakeRunner {
        apt_get: bool,
        tmux: bool,
        confirm: bool,
        commands: std::cell::RefCell<Vec<Vec<String>>>,
    }

    impl SetupRunner for FakeRunner {
        fn command_exists(&self, program: &str) -> bool {
            match program {
                "apt-get" => self.apt_get,
                "tmux" => self.tmux,
                _ => false,
            }
        }

        fn run_sudo(
            &self,
            args: &[&str],
            _non_interactive: bool,
        ) -> std::io::Result<CommandResult> {
            self.commands
                .borrow_mut()
                .push(args.iter().map(|arg| (*arg).to_string()).collect());
            Ok(CommandResult {
                status: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }

        fn confirm(&self, _prompt: &str, _assume_yes: bool) -> std::io::Result<bool> {
            Ok(self.confirm)
        }
    }

    /// Found live: the bridge warns instead of failing, and the fix gate only
    /// looked at Fail — so `ah setup --fix` reported the warn back without
    /// installing anything. A warn with a declared fix must be fixable.
    #[test]
    fn a_warn_step_with_a_declared_fix_is_gated_into_the_fix_path() {
        let warn_step = super::runtime_prerequisite_from_doctor(DoctorCheck {
            name: "wsl:browser-bridge".to_string(),
            status: DoctorStatus::Warn,
            detail: "no opener".to_string(),
            suggestion: None,
        });
        assert!(warn_step.fix_available);
        assert!(super::step_needs_fix(&[warn_step], "wsl:browser-bridge"));

        let warn_no_fix = super::runtime_prerequisite_from_doctor(DoctorCheck {
            name: "tmux server orphans".to_string(),
            status: DoctorStatus::Warn,
            detail: "1 stale".to_string(),
            suggestion: None,
        });
        assert!(
            !super::step_needs_fix(&[warn_no_fix], "tmux server orphans"),
            "a warn without a fix stays diagnostic"
        );
    }

    #[test]
    fn browser_bridge_fix_writes_the_shim_through_sudo() {
        let runner = FakeRunner {
            apt_get: true,
            tmux: true,
            confirm: true,
            commands: std::cell::RefCell::new(Vec::new()),
        };

        let outcome = super::install_browser_bridge_if_needed(&runner, true).unwrap();

        assert_eq!(outcome.status, PrereqStatus::Fixed);
        let commands = runner.commands.borrow();
        let script = commands
            .iter()
            .flatten()
            .find(|arg| arg.contains("/usr/local/bin/xdg-open"))
            .expect("the fix must write the opener");
        assert!(script.contains("rundll32.exe"), "got: {script}");
        assert!(script.contains("chmod 755"), "got: {script}");
    }

    #[test]
    fn browser_bridge_fix_respects_a_decline_and_an_existing_opener() {
        let declined = FakeRunner {
            apt_get: true,
            tmux: true,
            confirm: false,
            commands: std::cell::RefCell::new(Vec::new()),
        };
        let outcome = super::install_browser_bridge_if_needed(&declined, false).unwrap();
        assert_eq!(outcome.status, PrereqStatus::PermissionDenied);
        assert!(declined.commands.borrow().is_empty(), "no sudo on decline");

        struct HasOpener;
        impl SetupRunner for HasOpener {
            fn command_exists(&self, program: &str) -> bool {
                program == "xdg-open"
            }
            fn run_sudo(&self, _: &[&str], _: bool) -> std::io::Result<CommandResult> {
                panic!("must not run sudo when an opener exists");
            }
            fn confirm(&self, _: &str, _: bool) -> std::io::Result<bool> {
                Ok(true)
            }
        }
        let outcome = super::install_browser_bridge_if_needed(&HasOpener, true).unwrap();
        assert_eq!(outcome.status, PrereqStatus::Pass);
    }

    fn wsl_failing_checks() -> Vec<DoctorCheck> {
        vec![
            DoctorCheck {
                name: "wsl".to_string(),
                status: DoctorStatus::Pass,
                detail: "detected via env".to_string(),
                suggestion: None,
            },
            DoctorCheck {
                name: "wsl:tmux".to_string(),
                status: DoctorStatus::Fail,
                detail: "not found in PATH".to_string(),
                suggestion: Some("install tmux".to_string()),
            },
            DoctorCheck {
                name: "wsl:systemd-user".to_string(),
                status: DoctorStatus::Fail,
                detail: "not running".to_string(),
                suggestion: Some("enable systemd".to_string()),
            },
        ]
    }

    #[test]
    fn wsl_conf_edit_adds_boot_systemd_and_is_idempotent() {
        let input = Some("[automount]\nenabled=true\n".to_string());
        let edit = plan_wsl_conf_systemd_enable(&input);
        let WslConfEdit::Write { before, after } = edit else {
            panic!("expected write edit");
        };

        assert_eq!(before, "[automount]\nenabled=true\n");
        assert!(after.contains("[automount]\nenabled=true\n"));
        assert!(after.contains("[boot]\nsystemd=true\n"));
        assert_eq!(
            plan_wsl_conf_systemd_enable(&Some(after)),
            WslConfEdit::AlreadyEnabled
        );
    }

    #[test]
    fn wsl_conf_edit_preserves_boot_section_and_replaces_systemd_value() {
        let input =
            Some("[boot]\ncommand=echo hi\nsystemd=false\n\n[user]\ndefault=me\n".to_string());
        let WslConfEdit::Write { after, .. } = plan_wsl_conf_systemd_enable(&input) else {
            panic!("expected write edit");
        };

        assert!(after.contains("[boot]\ncommand=echo hi\nsystemd=true\n"));
        assert!(after.contains("[user]\ndefault=me\n"));
    }

    #[test]
    fn wsl_conf_malformed_stops_before_rewrite() {
        let edit = plan_wsl_conf_systemd_enable(&Some("[boot]\nnot a valid key\n".to_string()));

        assert!(matches!(edit, WslConfEdit::Malformed { .. }));
    }

    #[test]
    fn non_apt_distro_returns_unsupported_without_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let os_release = tmp.path().join("os-release");
        std::fs::write(&os_release, "ID=fedora\nID_LIKE=\"rhel fedora\"\n").unwrap();
        let runner = FakeRunner {
            apt_get: false,
            tmux: false,
            confirm: true,
            commands: Default::default(),
        };
        let paths = SetupPaths {
            state_dir: tmp.path().join("state"),
            wsl_conf: tmp.path().join("wsl.conf"),
            os_release,
        };

        let envelope = build_phase1_fix_envelope(
            setup_prerequisites_from_checks(wsl_failing_checks()),
            Some("Fedora".to_string()),
            "op-test".to_string(),
            SetupOptions {
                fix: true,
                yes: true,
                ..SetupOptions::default()
            },
            &runner,
            &paths,
        )
        .unwrap();

        assert_eq!(envelope.overall_status, SetupOverallStatus::Unsupported);
        assert_eq!(
            envelope
                .next_action
                .as_ref()
                .map(|action| action.kind.as_str()),
            Some("unsupported_manual_fix")
        );
        assert!(runner.commands.borrow().is_empty());
    }

    #[test]
    fn phase1_fix_installs_tmux_writes_state_and_returns_wsl_shutdown_contract() {
        let tmp = tempfile::tempdir().unwrap();
        let os_release = tmp.path().join("os-release");
        std::fs::write(&os_release, "ID=ubuntu\nID_LIKE=debian\n").unwrap();
        let wsl_conf = tmp.path().join("wsl.conf");
        std::fs::write(&wsl_conf, "[automount]\nenabled=true\n").unwrap();
        let runner = FakeRunner {
            apt_get: true,
            tmux: false,
            confirm: true,
            commands: Default::default(),
        };
        let paths = SetupPaths {
            state_dir: tmp.path().join("state"),
            wsl_conf,
            os_release,
        };

        let envelope = build_phase1_fix_envelope(
            setup_prerequisites_from_checks(wsl_failing_checks()),
            Some("Ubuntu".to_string()),
            "op-test".to_string(),
            SetupOptions {
                fix: true,
                yes: true,
                ..SetupOptions::default()
            },
            &runner,
            &paths,
        )
        .unwrap();

        assert_eq!(
            envelope.overall_status,
            SetupOverallStatus::NeedsWslShutdown
        );
        let action = envelope.next_action.as_ref().unwrap();
        assert_eq!(action.kind, "needs_wsl_shutdown");
        assert_eq!(
            action.command.as_deref(),
            Some("powershell.exe -NoProfile -Command \"wsl --shutdown\"")
        );
        assert!(action.message.contains("stops all running WSL distros"));
        assert!(action.message.contains("ah setup --check"));
        assert_eq!(
            envelope.resume_command.as_deref(),
            Some("ah setup --resume --check")
        );

        let commands = runner.commands.borrow();
        assert_eq!(commands[0], vec!["apt-get", "update"]);
        assert_eq!(commands[1], vec!["apt-get", "install", "-y", "tmux"]);
        assert_eq!(commands[2][0..2], ["sh".to_string(), "-c".to_string()]);

        let state = read_setup_state(&paths).unwrap().unwrap();
        assert_eq!(state.operation_id, "op-test");
        assert_eq!(state.pending_restart, PendingRestart::WslShutdown);
        assert_eq!(state.distro_name.as_deref(), Some("Ubuntu"));
        assert!(state.wsl_conf_hash_before.is_some());
        assert!(state.wsl_conf_hash_after.is_some());
    }

    #[test]
    fn observed_state_wins_clears_pending_restart_state_when_systemd_passes() {
        let tmp = tempfile::tempdir().unwrap();
        let os_release = tmp.path().join("os-release");
        std::fs::write(&os_release, "ID=ubuntu\n").unwrap();
        let state_dir = tmp.path().join("state");
        let state_file = state_dir.join("setup").join("state.json");
        std::fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        std::fs::write(
            &state_file,
            serde_json::json!({
                "schema_version": 1,
                "operation_id": "old-op",
                "phase": "phase1_distro",
                "boundary": "distro",
                "distro_name": "Ubuntu",
                "last_completed_step": "Updated /etc/wsl.conf to set [boot] systemd=true",
                "pending_restart": "wsl_shutdown",
                "wsl_conf_hash_before": null,
                "wsl_conf_hash_after": null,
                "created_at": 1,
                "updated_at": 1,
                "last_error": null
            })
            .to_string(),
        )
        .unwrap();
        let runner = FakeRunner {
            apt_get: true,
            tmux: true,
            confirm: true,
            commands: Default::default(),
        };
        let paths = SetupPaths {
            state_dir,
            wsl_conf: tmp.path().join("wsl.conf"),
            os_release,
        };
        let checks = vec![
            DoctorCheck {
                name: "wsl".to_string(),
                status: DoctorStatus::Pass,
                detail: "detected via env".to_string(),
                suggestion: None,
            },
            DoctorCheck {
                name: "wsl:tmux".to_string(),
                status: DoctorStatus::Pass,
                detail: "tmux 3.4".to_string(),
                suggestion: None,
            },
            DoctorCheck {
                name: "wsl:systemd-user".to_string(),
                status: DoctorStatus::Pass,
                detail: "available".to_string(),
                suggestion: None,
            },
        ];

        let envelope = build_phase1_fix_envelope(
            setup_prerequisites_from_checks(checks),
            Some("Ubuntu".to_string()),
            "new-op".to_string(),
            SetupOptions {
                fix: true,
                yes: true,
                resume: true,
                ..SetupOptions::default()
            },
            &runner,
            &paths,
        )
        .unwrap();

        assert_eq!(envelope.overall_status, SetupOverallStatus::Pass);
        assert!(!state_file.exists());
    }

    #[test]
    fn os_release_debian_family_detection_is_exact_enough() {
        assert!(os_release_is_debian_family("ID=ubuntu\n"));
        assert!(os_release_is_debian_family(
            "ID=fedora\nID_LIKE=\"debian ubuntu\"\n"
        ));
        assert!(!os_release_is_debian_family(
            "ID=fedora\nID_LIKE=\"rhel\"\n"
        ));
    }
}
