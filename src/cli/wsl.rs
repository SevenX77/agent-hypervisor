use crate::cli::doctor::{DoctorCheck, DoctorStatus};
use std::process::Command;

pub const WSL_README_URL: &str = "https://github.com/SevenX77/ah#windows-wsl2";

pub const SYSTEMD_GUIDANCE: &str = "WSL detected, but the systemd user session is not available. ah needs systemd user services/scopes. Enable systemd in WSL: add [boot]\nsystemd=true to /etc/wsl.conf, then run \"wsl --shutdown\" in PowerShell and reopen your distro. Verify: systemctl --user is-system-running. See: https://github.com/SevenX77/ah#windows-wsl2";

pub const TMUX_GUIDANCE: &str = "WSL detected, but tmux is not installed or not on PATH. ah runs every agent in tmux. Install it inside WSL, e.g.: sudo apt update && sudo apt install -y tmux. Verify: tmux -V. See: https://github.com/SevenX77/ah#windows-wsl2";

#[derive(Debug, Clone, Copy, Default)]
pub struct WslProbeInputs<'a> {
    pub osrelease: Option<&'a str>,
    pub proc_version: Option<&'a str>,
    pub wsl_distro_name: Option<&'a str>,
    pub wsl_interop: Option<&'a str>,
    pub container_hint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WslDetection {
    pub is_wsl: bool,
    pub source: Option<&'static str>,
    pub container_hint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemdUserStatus {
    Available,
    MissingTool,
    MissingRuntimeDir,
    NotRunning { observed: String },
    ProbeFailed { err: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TmuxProbe<'a> {
    pub found: bool,
    pub version: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TmuxCheck {
    pub available: bool,
    pub detail: String,
}

pub fn detect_wsl(inputs: WslProbeInputs<'_>) -> WslDetection {
    let source = if wsl_text_signal(inputs.osrelease) {
        Some("kernel")
    } else if wsl_text_signal(inputs.proc_version) {
        Some("proc")
    } else if env_signal(inputs.wsl_distro_name) || env_signal(inputs.wsl_interop) {
        Some("env")
    } else {
        None
    };

    WslDetection {
        is_wsl: source.is_some(),
        source,
        container_hint: inputs.container_hint,
    }
}

fn wsl_text_signal(value: Option<&str>) -> bool {
    value
        .map(str::to_ascii_lowercase)
        .is_some_and(|value| value.contains("microsoft") || value.contains("wsl"))
}

fn env_signal(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

pub fn classify_systemd_user_probe(
    xdg_runtime_dir: Option<&str>,
    systemd_run_found: bool,
    systemctl_found: bool,
    output_status: Option<i32>,
    stdout: Option<&str>,
    exec_error: Option<&str>,
) -> SystemdUserStatus {
    if xdg_runtime_dir.is_none_or(|value| value.trim().is_empty()) {
        return SystemdUserStatus::MissingRuntimeDir;
    }
    if !systemd_run_found || !systemctl_found {
        return SystemdUserStatus::MissingTool;
    }

    if let Some(stdout) = stdout {
        let observed = stdout.trim();
        if is_accepted_systemd_user_state(observed) {
            return SystemdUserStatus::Available;
        }
        if !observed.is_empty() {
            return SystemdUserStatus::NotRunning {
                observed: observed.to_string(),
            };
        }
    }

    if let Some(err) = exec_error.filter(|err| !err.trim().is_empty()) {
        return SystemdUserStatus::ProbeFailed {
            err: err.to_string(),
        };
    }

    SystemdUserStatus::NotRunning {
        observed: output_status
            .map(|status| format!("exit status {status}"))
            .unwrap_or_else(|| "no status".to_string()),
    }
}

fn is_accepted_systemd_user_state(value: &str) -> bool {
    matches!(
        value.trim(),
        "running" | "degraded" | "starting" | "maintenance" | "initializing"
    )
}

pub fn tmux_check_from_probe(probe: TmuxProbe<'_>) -> TmuxCheck {
    if !probe.found {
        return TmuxCheck {
            available: false,
            detail: "not found in PATH".to_string(),
        };
    }

    TmuxCheck {
        available: true,
        detail: probe
            .version
            .map(str::trim)
            .filter(|version| !version.is_empty())
            .unwrap_or("found in PATH")
            .to_string(),
    }
}

pub fn wsl_onboarding_checks() -> Vec<DoctorCheck> {
    let detection = current_wsl_detection();
    if !detection.is_wsl {
        return Vec::new();
    }
    let mut checks = wsl_onboarding_checks_from(
        detection,
        current_systemd_user_status(),
        current_tmux_check(),
    );
    checks.push(browser_bridge_check(current_browser_opener()));
    checks
}

/// Where WSL can hand a URL to the Windows default browser, if anywhere.
fn current_browser_opener() -> Option<std::path::PathBuf> {
    which::which("xdg-open")
        .or_else(|_| which::which("wslview"))
        .ok()
}

/// Provider sign-in flows open a browser; inside WSL that needs a bridge to
/// the Windows side (decision 0006's doorman launches `codex login` and
/// friends right in the terminal). Without one the flows still work — the URL
/// prints and the operator clicks it — so a missing bridge warns rather than
/// fails, and `ah setup --fix` installs a zero-dependency opener.
pub fn browser_bridge_check(opener: Option<std::path::PathBuf>) -> DoctorCheck {
    match opener {
        Some(path) => DoctorCheck {
            name: "wsl:browser-bridge".to_string(),
            status: DoctorStatus::Pass,
            detail: path.display().to_string(),
            suggestion: None,
        },
        None => DoctorCheck {
            name: "wsl:browser-bridge".to_string(),
            status: DoctorStatus::Warn,
            detail: "no xdg-open/wslview in WSL; provider sign-in flows cannot pop the Windows browser"
                .to_string(),
            suggestion: Some(
                "run ah setup --fix (installs a WSL-to-Windows opener at /usr/local/bin/xdg-open)"
                    .to_string(),
            ),
        },
    }
}

pub fn wsl_onboarding_checks_from(
    detection: WslDetection,
    systemd_status: SystemdUserStatus,
    tmux_check: TmuxCheck,
) -> Vec<DoctorCheck> {
    if !detection.is_wsl {
        return Vec::new();
    }

    let mut checks = Vec::new();
    checks.push(DoctorCheck {
        name: "wsl".to_string(),
        status: DoctorStatus::Pass,
        detail: wsl_detection_detail(&detection),
        suggestion: None,
    });

    let (systemd_status_value, systemd_detail, systemd_suggestion) = match systemd_status {
        SystemdUserStatus::Available => (DoctorStatus::Pass, "available".to_string(), None),
        SystemdUserStatus::MissingTool => (
            DoctorStatus::Fail,
            "systemd-run or systemctl not found in PATH".to_string(),
            Some(SYSTEMD_GUIDANCE.to_string()),
        ),
        SystemdUserStatus::MissingRuntimeDir => (
            DoctorStatus::Fail,
            "XDG_RUNTIME_DIR is missing".to_string(),
            Some(SYSTEMD_GUIDANCE.to_string()),
        ),
        SystemdUserStatus::NotRunning { observed } => (
            DoctorStatus::Fail,
            format!("not running ({observed})"),
            Some(SYSTEMD_GUIDANCE.to_string()),
        ),
        SystemdUserStatus::ProbeFailed { err } => (
            DoctorStatus::Fail,
            format!("probe failed ({err})"),
            Some(SYSTEMD_GUIDANCE.to_string()),
        ),
    };
    checks.push(DoctorCheck {
        name: "wsl:systemd-user".to_string(),
        status: systemd_status_value,
        detail: systemd_detail,
        suggestion: systemd_suggestion,
    });

    checks.push(DoctorCheck {
        name: "wsl:tmux".to_string(),
        status: if tmux_check.available {
            DoctorStatus::Pass
        } else {
            DoctorStatus::Fail
        },
        detail: tmux_check.detail,
        suggestion: (!tmux_check.available).then(|| TMUX_GUIDANCE.to_string()),
    });

    checks
}

fn wsl_detection_detail(detection: &WslDetection) -> String {
    let source = detection.source.unwrap_or("unknown");
    if detection.container_hint {
        format!("detected via {source}; container hint present")
    } else {
        format!("detected via {source}")
    }
}

pub fn start_preflight_error() -> Option<String> {
    start_preflight_from(
        current_wsl_detection(),
        current_systemd_user_status(),
        current_tmux_check(),
    )
    .err()
}

pub fn start_preflight_from(
    detection: WslDetection,
    systemd_status: SystemdUserStatus,
    tmux_check: TmuxCheck,
) -> Result<(), String> {
    if !detection.is_wsl {
        return Ok(());
    }
    if systemd_status != SystemdUserStatus::Available {
        return Err(SYSTEMD_GUIDANCE.to_string());
    }
    if !tmux_check.available {
        return Err(TMUX_GUIDANCE.to_string());
    }
    Ok(())
}

fn current_wsl_detection() -> WslDetection {
    let osrelease = std::fs::read_to_string("/proc/sys/kernel/osrelease").ok();
    let proc_version = std::fs::read_to_string("/proc/version").ok();
    let wsl_distro_name = std::env::var("WSL_DISTRO_NAME").ok();
    let wsl_interop = std::env::var("WSL_INTEROP").ok();
    detect_wsl(WslProbeInputs {
        osrelease: osrelease.as_deref(),
        proc_version: proc_version.as_deref(),
        wsl_distro_name: wsl_distro_name.as_deref(),
        wsl_interop: wsl_interop.as_deref(),
        container_hint: container_hint(),
    })
}

fn current_systemd_user_status() -> SystemdUserStatus {
    let xdg_runtime_dir = std::env::var("XDG_RUNTIME_DIR").ok();
    let systemd_run_found = which::which("systemd-run").is_ok();
    let systemctl_found = which::which("systemctl").is_ok();
    let output = if systemctl_found {
        Command::new("systemctl")
            .args(["--user", "is-system-running"])
            .output()
    } else {
        return classify_systemd_user_probe(
            xdg_runtime_dir.as_deref(),
            systemd_run_found,
            systemctl_found,
            None,
            None,
            None,
        );
    };

    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            classify_systemd_user_probe(
                xdg_runtime_dir.as_deref(),
                systemd_run_found,
                systemctl_found,
                output.status.code(),
                Some(stdout.trim()),
                None,
            )
        }
        Err(err) => classify_systemd_user_probe(
            xdg_runtime_dir.as_deref(),
            systemd_run_found,
            systemctl_found,
            None,
            None,
            Some(&err.to_string()),
        ),
    }
}

fn current_tmux_check() -> TmuxCheck {
    let found = which::which("tmux").is_ok();
    let version = if found {
        Command::new("tmux")
            .arg("-V")
            .output()
            .ok()
            .and_then(|output| {
                output
                    .status
                    .success()
                    .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
            })
    } else {
        None
    };
    tmux_check_from_probe(TmuxProbe {
        found,
        version: version.as_deref(),
    })
}

fn container_hint() -> bool {
    std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists()
        || std::fs::read_to_string("/proc/1/cgroup")
            .ok()
            .is_some_and(|cgroup| {
                ["docker", "kubepods", "containerd", "libpod"]
                    .iter()
                    .any(|needle| cgroup.contains(needle))
            })
}

#[cfg(test)]
mod tests {
    #[test]
    fn browser_bridge_check_passes_with_an_opener_and_warns_without() {
        let with = super::browser_bridge_check(Some(std::path::PathBuf::from(
            "/usr/local/bin/xdg-open",
        )));
        assert!(matches!(with.status, DoctorStatus::Pass));

        let without = super::browser_bridge_check(None);
        assert!(matches!(without.status, DoctorStatus::Warn));
        assert!(without.suggestion.unwrap().contains("ah setup --fix"));
    }

    use super::{
        SystemdUserStatus, TmuxProbe, WSL_README_URL, WslDetection, WslProbeInputs,
        classify_systemd_user_probe, detect_wsl, start_preflight_from, tmux_check_from_probe,
        wsl_onboarding_checks_from,
    };
    use crate::cli::doctor::DoctorStatus;

    #[test]
    fn detect_wsl_matches_osrelease_microsoft_case_insensitive() {
        let detection = detect_wsl(WslProbeInputs {
            osrelease: Some("5.15.90.1-Microsoft-standard"),
            proc_version: None,
            wsl_distro_name: None,
            wsl_interop: None,
            container_hint: false,
        });

        assert!(detection.is_wsl);
        assert_eq!(detection.source.as_deref(), Some("kernel"));
        assert!(!detection.container_hint);
    }

    #[test]
    fn detect_wsl_matches_standard_wsl2_osrelease() {
        let detection = detect_wsl(WslProbeInputs {
            osrelease: Some("5.15.153.1-microsoft-standard-WSL2"),
            proc_version: None,
            wsl_distro_name: None,
            wsl_interop: None,
            container_hint: false,
        });

        assert!(detection.is_wsl);
    }

    #[test]
    fn detect_wsl_matches_proc_version() {
        let detection = detect_wsl(WslProbeInputs {
            osrelease: None,
            proc_version: Some("Linux version 5.10.16.3-microsoft-standard-WSL2"),
            wsl_distro_name: None,
            wsl_interop: None,
            container_hint: false,
        });

        assert!(detection.is_wsl);
        assert_eq!(detection.source.as_deref(), Some("proc"));
    }

    #[test]
    fn detect_wsl_uses_env_as_supplement() {
        let detection = detect_wsl(WslProbeInputs {
            osrelease: None,
            proc_version: None,
            wsl_distro_name: Some("Ubuntu"),
            wsl_interop: None,
            container_hint: false,
        });

        assert!(detection.is_wsl);
        assert_eq!(detection.source.as_deref(), Some("env"));
    }

    #[test]
    fn detect_wsl_returns_false_without_any_signal() {
        let detection = detect_wsl(WslProbeInputs {
            osrelease: Some("6.8.0-generic"),
            proc_version: Some("Linux version 6.8.0-generic"),
            wsl_distro_name: None,
            wsl_interop: None,
            container_hint: false,
        });

        assert!(!detection.is_wsl);
        assert_eq!(detection.source, None);
    }

    #[test]
    fn detect_wsl_preserves_container_hint() {
        let detection = detect_wsl(WslProbeInputs {
            osrelease: Some("5.15.153.1-microsoft-standard-WSL2"),
            proc_version: None,
            wsl_distro_name: None,
            wsl_interop: None,
            container_hint: true,
        });

        assert!(detection.is_wsl);
        assert!(detection.container_hint);
    }

    #[test]
    fn classify_systemd_user_probe_reports_missing_runtime_dir() {
        assert_eq!(
            classify_systemd_user_probe(None, true, true, Some(1), Some("running"), None),
            SystemdUserStatus::MissingRuntimeDir
        );
    }

    #[test]
    fn classify_systemd_user_probe_reports_missing_tools() {
        assert_eq!(
            classify_systemd_user_probe(Some("/run/user/1000"), false, true, None, None, None),
            SystemdUserStatus::MissingTool
        );
        assert_eq!(
            classify_systemd_user_probe(Some("/run/user/1000"), true, false, None, None, None),
            SystemdUserStatus::MissingTool
        );
    }

    #[test]
    fn classify_systemd_user_probe_accepts_user_manager_states_from_stdout() {
        for status in [
            "running",
            "degraded",
            "starting",
            "initializing",
            "maintenance",
        ] {
            assert_eq!(
                classify_systemd_user_probe(
                    Some("/run/user/1000"),
                    true,
                    true,
                    Some(1),
                    Some(status),
                    None
                ),
                SystemdUserStatus::Available
            );
        }
    }

    #[test]
    fn classify_systemd_user_probe_rejects_non_running_states() {
        for status in ["offline", "failed", "stopping"] {
            assert_eq!(
                classify_systemd_user_probe(
                    Some("/run/user/1000"),
                    true,
                    true,
                    Some(0),
                    Some(status),
                    None
                ),
                SystemdUserStatus::NotRunning {
                    observed: status.to_string()
                }
            );
        }
    }

    #[test]
    fn classify_systemd_user_probe_reports_exec_error() {
        assert_eq!(
            classify_systemd_user_probe(
                Some("/run/user/1000"),
                true,
                true,
                None,
                None,
                Some("permission denied")
            ),
            SystemdUserStatus::ProbeFailed {
                err: "permission denied".to_string()
            }
        );
    }

    #[test]
    fn tmux_check_reports_found_with_version_detail() {
        let check = tmux_check_from_probe(TmuxProbe {
            found: true,
            version: Some("tmux 3.4"),
        });

        assert!(check.available);
        assert_eq!(check.detail, "tmux 3.4");
    }

    #[test]
    fn tmux_check_reports_not_found() {
        let check = tmux_check_from_probe(TmuxProbe {
            found: false,
            version: None,
        });

        assert!(!check.available);
        assert_eq!(check.detail, "not found in PATH");
    }

    #[test]
    fn wsl_onboarding_checks_return_empty_for_non_wsl() {
        let checks = wsl_onboarding_checks_from(
            WslDetection {
                is_wsl: false,
                source: None,
                container_hint: false,
            },
            SystemdUserStatus::MissingRuntimeDir,
            tmux_check_from_probe(TmuxProbe {
                found: false,
                version: None,
            }),
        );

        assert!(checks.is_empty());
    }

    #[test]
    fn wsl_onboarding_checks_fail_when_systemd_is_missing() {
        let checks = wsl_onboarding_checks_from(
            WslDetection {
                is_wsl: true,
                source: Some("kernel"),
                container_hint: false,
            },
            SystemdUserStatus::MissingRuntimeDir,
            tmux_check_from_probe(TmuxProbe {
                found: true,
                version: Some("tmux 3.4"),
            }),
        );

        let systemd = checks
            .iter()
            .find(|check| check.name == "wsl:systemd-user")
            .unwrap();
        assert_eq!(systemd.status, DoctorStatus::Fail);
        assert!(
            systemd
                .suggestion
                .as_deref()
                .unwrap()
                .contains("/etc/wsl.conf")
        );
    }

    #[test]
    fn wsl_onboarding_checks_fail_when_tmux_is_missing() {
        let checks = wsl_onboarding_checks_from(
            WslDetection {
                is_wsl: true,
                source: Some("kernel"),
                container_hint: false,
            },
            SystemdUserStatus::Available,
            tmux_check_from_probe(TmuxProbe {
                found: false,
                version: None,
            }),
        );

        let tmux = checks
            .iter()
            .find(|check| check.name == "wsl:tmux")
            .unwrap();
        assert_eq!(tmux.status, DoctorStatus::Fail);
        assert!(tmux.suggestion.as_deref().unwrap().contains("sudo apt"));
    }

    #[test]
    fn wsl_onboarding_checks_pass_when_requirements_are_available() {
        let checks = wsl_onboarding_checks_from(
            WslDetection {
                is_wsl: true,
                source: Some("kernel"),
                container_hint: true,
            },
            SystemdUserStatus::Available,
            tmux_check_from_probe(TmuxProbe {
                found: true,
                version: Some("tmux 3.4"),
            }),
        );

        assert_eq!(checks.len(), 3);
        assert!(
            checks
                .iter()
                .all(|check| check.status == DoctorStatus::Pass)
        );
        assert!(checks[0].detail.contains("container hint"));
    }

    #[test]
    fn start_preflight_does_not_block_non_wsl() {
        let result = start_preflight_from(
            WslDetection {
                is_wsl: false,
                source: None,
                container_hint: false,
            },
            SystemdUserStatus::MissingRuntimeDir,
            tmux_check_from_probe(TmuxProbe {
                found: false,
                version: None,
            }),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn start_preflight_fails_fast_for_wsl_without_systemd() {
        let err = start_preflight_from(
            WslDetection {
                is_wsl: true,
                source: Some("kernel"),
                container_hint: false,
            },
            SystemdUserStatus::NotRunning {
                observed: "offline".to_string(),
            },
            tmux_check_from_probe(TmuxProbe {
                found: true,
                version: Some("tmux 3.4"),
            }),
        )
        .unwrap_err();

        assert!(err.contains("systemd user session"));
        assert!(err.contains(WSL_README_URL));
    }

    #[test]
    fn start_preflight_fails_fast_for_wsl_without_tmux() {
        let err = start_preflight_from(
            WslDetection {
                is_wsl: true,
                source: Some("kernel"),
                container_hint: false,
            },
            SystemdUserStatus::Available,
            tmux_check_from_probe(TmuxProbe {
                found: false,
                version: None,
            }),
        )
        .unwrap_err();

        assert!(err.contains("tmux is not installed"));
        assert!(err.contains(WSL_README_URL));
    }
}
