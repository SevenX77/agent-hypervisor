//! Sandbox environment checks and command assembly helpers for MVP2.

pub mod path;
pub mod session_archive;
pub mod systemd;

use crate::error::CcbdError;
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
pub fn check_environment() -> Result<EnvState, CcbdError> {
    check_environment_with(
        std::env::var("CCBD_UNSAFE_NO_SANDBOX").as_deref() == Ok("1"),
        effective_user_is_root(),
        || which::which("systemd-run").is_ok(),
        std::env::var_os("INVOCATION_ID").is_some(),
        xdg_runtime_dir_present,
        user_manager_reachable,
    )
}

fn check_environment_with(
    unsafe_no_sandbox: bool,
    effective_user_is_root: bool,
    systemd_run_probe: impl FnOnce() -> bool,
    under_systemd: bool,
    xdg_runtime_dir_probe: impl FnOnce() -> bool,
    user_manager_reachable_probe: impl FnOnce() -> bool,
) -> Result<EnvState, CcbdError> {
    if effective_user_is_root {
        return Err(CcbdError::EnvironmentNotSupported {
            details: "agent provider execution as root is forbidden; initialize WSL with a non-root default user and run ah/ahd as that user".into(),
        });
    }

    if unsafe_no_sandbox {
        return Err(CcbdError::EnvironmentNotSupported {
            details: "CCBD_UNSAFE_NO_SANDBOX=1 is forbidden for agent provider execution; the required systemd user scope may not be replaced by bare execution".into(),
        });
    }

    let systemd_run_available = systemd_run_probe();

    if !systemd_run_available {
        return Err(CcbdError::EnvironmentNotSupported {
            details:
                "systemd-run not found in PATH; ccbd-rust requires Linux + systemd user session"
                    .into(),
        });
    }
    if !xdg_runtime_dir_probe() {
        return Err(CcbdError::EnvironmentNotSupported {
            details: "systemd user manager environment is missing; XDG_RUNTIME_DIR is required for systemd-run --user --scope".into(),
        });
    }
    if !user_manager_reachable_probe() {
        return Err(CcbdError::EnvironmentNotSupported {
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

#[cfg(unix)]
fn effective_user_is_root() -> bool {
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn effective_user_is_root() -> bool {
    false
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
    use crate::error::CcbdError;

    #[test]
    fn test_check_environment_rejects_unsafe_no_sandbox() {
        let err =
            check_environment_with(true, false, || false, false, || false, || false).unwrap_err();

        assert!(matches!(err, CcbdError::EnvironmentNotSupported { .. }));
        assert!(err.to_string().contains("CCBD_UNSAFE_NO_SANDBOX=1"));
    }

    #[test]
    fn test_check_environment_rejects_root_before_any_probe() {
        let err = check_environment_with(
            false,
            true,
            || panic!("must not probe as root"),
            false,
            || panic!("must not probe as root"),
            || panic!("must not probe as root"),
        )
        .unwrap_err();

        assert!(matches!(err, CcbdError::EnvironmentNotSupported { .. }));
        assert!(err.to_string().contains("as root is forbidden"));
    }

    #[test]
    fn test_check_environment_records_under_systemd() {
        let state = check_environment_with(false, false, || true, true, || true, || true).unwrap();

        assert!(state.under_systemd);
    }

    #[test]
    fn test_check_environment_requires_systemd_without_bypass() {
        let err =
            check_environment_with(false, false, || false, false, || true, || true).unwrap_err();

        assert!(matches!(err, CcbdError::EnvironmentNotSupported { .. }));
    }

    #[test]
    fn test_check_environment_requires_xdg_runtime_dir_without_bypass() {
        let err =
            check_environment_with(false, false, || true, false, || false, || true).unwrap_err();

        assert!(matches!(err, CcbdError::EnvironmentNotSupported { .. }));
        assert!(err.to_string().contains("XDG_RUNTIME_DIR"));
    }

    #[test]
    fn test_check_environment_allows_xdg_runtime_dir_without_dbus_address() {
        let state = check_environment_with(false, false, || true, false, || true, || true).unwrap();

        assert!(state.systemd_run_available);
    }

    #[test]
    fn test_check_environment_requires_reachable_user_bus_without_bypass() {
        let err =
            check_environment_with(false, false, || true, false, || true, || false).unwrap_err();

        assert!(matches!(err, CcbdError::EnvironmentNotSupported { .. }));
        assert!(err.to_string().contains("systemd user manager"));
    }
}
