//! Sandbox environment checks and command assembly helpers for MVP2.

pub mod path;
pub mod session_archive;
pub mod systemd;

use crate::error::AhError;
use serde::{Deserialize, Serialize};
use std::process::Stdio;
use std::time::{Duration, Instant};

const USER_MANAGER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SandboxOverrides {
    #[serde(default)]
    pub extra_ro_binds: Vec<ReadOnlyBind>,
    #[serde(default)]
    pub extra_binds: Vec<ReadWriteBind>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReadOnlyBind {
    pub host_path: String,
    pub sandbox_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReadWriteBind {
    pub host_path: String,
    pub sandbox_path: String,
}

/// Result of startup-time sandbox environment detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvState {
    pub systemd_run_available: bool,
    pub unsafe_no_sandbox: bool,
    pub under_systemd: bool,
}

/// Check whether the host can run MVP2 sandboxed agents.
pub fn check_environment() -> Result<EnvState, AhError> {
    check_environment_with(
        std::env::var("AH_UNSAFE_NO_SANDBOX").as_deref() == Ok("1"),
        || which::which("systemd-run").is_ok(),
        std::env::var_os("INVOCATION_ID").is_some(),
        xdg_runtime_dir_present,
        user_manager_reachable,
    )
}

fn check_environment_with(
    unsafe_no_sandbox: bool,
    systemd_run_probe: impl FnOnce() -> bool,
    under_systemd: bool,
    xdg_runtime_dir_probe: impl FnOnce() -> bool,
    user_manager_reachable_probe: impl FnOnce() -> bool,
) -> Result<EnvState, AhError> {
    let systemd_run_available = systemd_run_probe();

    if unsafe_no_sandbox {
        tracing::warn!("AH_UNSAFE_NO_SANDBOX=1 detected; running without systemd scope wrapper");
        return Ok(EnvState {
            systemd_run_available,
            unsafe_no_sandbox,
            under_systemd,
        });
    }

    if !systemd_run_available {
        return Err(AhError::EnvironmentNotSupported {
            details:
                "systemd-run not found in PATH; agent-hypervisor requires Linux + systemd user session"
                    .into(),
        });
    }
    if !xdg_runtime_dir_probe() {
        return Err(AhError::EnvironmentNotSupported {
            details: "systemd user manager environment is missing; XDG_RUNTIME_DIR is required for systemd-run --user --scope".into(),
        });
    }
    if !user_manager_reachable_probe() {
        return Err(AhError::EnvironmentNotSupported {
            details: "systemd user manager is not reachable; systemd-run --user --scope would fail"
                .into(),
        });
    }

    Ok(EnvState {
        systemd_run_available,
        unsafe_no_sandbox,
        under_systemd,
    })
}

fn xdg_runtime_dir_present() -> bool {
    env_var_present("XDG_RUNTIME_DIR")
}

fn env_var_present(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|value| !value.is_empty())
}

fn user_manager_reachable() -> bool {
    user_manager_reachable_with_timeout(USER_MANAGER_PROBE_TIMEOUT)
}

fn user_manager_reachable_with_timeout(timeout: Duration) -> bool {
    let mut child = match std::process::Command::new("systemctl")
        .args(["--user", "is-system-running"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => {
                return child
                    .wait_with_output()
                    .is_ok_and(|output| user_manager_probe_succeeded(&output));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return false;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

fn user_manager_probe_succeeded(output: &std::process::Output) -> bool {
    if output.status.success() {
        return true;
    }
    let status = String::from_utf8_lossy(&output.stdout)
        .trim()
        .to_ascii_lowercase();
    matches!(
        status.as_str(),
        "running" | "degraded" | "starting" | "initializing" | "maintenance"
    )
}

#[cfg(test)]
mod tests {
    use super::check_environment_with;
    use crate::error::AhError;

    #[test]
    fn test_check_environment_bypass_allows_missing_tools() {
        let state = check_environment_with(true, || false, false, || false, || false).unwrap();

        assert!(state.unsafe_no_sandbox);
        assert!(!state.systemd_run_available);
    }

    #[test]
    fn test_check_environment_records_under_systemd() {
        let state = check_environment_with(false, || true, true, || true, || true).unwrap();

        assert!(state.under_systemd);
    }

    #[test]
    fn test_check_environment_requires_systemd_without_bypass() {
        let err = check_environment_with(false, || false, false, || true, || true).unwrap_err();

        assert!(matches!(err, AhError::EnvironmentNotSupported { .. }));
    }

    #[test]
    fn test_check_environment_requires_xdg_runtime_dir_without_bypass() {
        let err = check_environment_with(false, || true, false, || false, || true).unwrap_err();

        assert!(matches!(err, AhError::EnvironmentNotSupported { .. }));
        assert!(err.to_string().contains("XDG_RUNTIME_DIR"));
    }

    #[test]
    fn test_check_environment_allows_xdg_runtime_dir_without_dbus_address() {
        let state = check_environment_with(false, || true, false, || true, || true).unwrap();

        assert!(state.systemd_run_available);
    }

    #[test]
    fn test_check_environment_requires_reachable_user_bus_without_bypass() {
        let err = check_environment_with(false, || true, false, || true, || false).unwrap_err();

        assert!(matches!(err, AhError::EnvironmentNotSupported { .. }));
        assert!(err.to_string().contains("systemd user manager"));
    }
}
