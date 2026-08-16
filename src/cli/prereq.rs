use crate::cli::doctor::{DoctorCheck, DoctorStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FixOwner {
    AhRuntime,
    ProviderExternal,
    UserProject,
    DiagnosticOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrereqStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
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
pub enum PrivilegeClass {
    None,
    DistroSudo,
    WindowsAdmin,
    UserOwned,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBoundary {
    CurrentProcess,
    WslDistro,
    WindowsHost,
    UserProject,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartRequirement {
    None,
    NeedsWslShutdown,
    NeedsWindowsReboot,
    NeedsDistroReopen,
    NeedsDistroFirstLaunch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrerequisiteMetadata {
    pub owner: FixOwner,
    pub fix_available: bool,
    pub privilege: PrivilegeClass,
    pub boundary: ExecutionBoundary,
    pub restart: RestartRequirement,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePrerequisite {
    pub id: String,
    pub status: PrereqStatus,
    pub owner: FixOwner,
    pub fix_available: bool,
    pub privilege: PrivilegeClass,
    pub boundary: ExecutionBoundary,
    pub restart: RestartRequirement,
    pub detail: String,
    pub suggestion: Option<String>,
    pub resume_token: Option<String>,
}

pub fn runtime_prerequisite_from_doctor(check: DoctorCheck) -> RuntimePrerequisite {
    let metadata = metadata_for(&check.name);
    RuntimePrerequisite {
        id: check.name,
        status: status_from_doctor(check.status),
        owner: metadata.owner,
        fix_available: metadata.fix_available,
        privilege: metadata.privilege,
        boundary: metadata.boundary,
        restart: metadata.restart,
        detail: check.detail,
        suggestion: check.suggestion,
        resume_token: None,
    }
}

pub fn metadata_for(id: &str) -> PrerequisiteMetadata {
    match id {
        "binary:tmux" => PrerequisiteMetadata {
            owner: FixOwner::AhRuntime,
            fix_available: true,
            privilege: PrivilegeClass::DistroSudo,
            boundary: ExecutionBoundary::WslDistro,
            restart: RestartRequirement::None,
        },
        "binary:systemd-run" => PrerequisiteMetadata {
            owner: FixOwner::AhRuntime,
            fix_available: true,
            privilege: PrivilegeClass::DistroSudo,
            boundary: ExecutionBoundary::WslDistro,
            restart: RestartRequirement::NeedsWslShutdown,
        },
        "wsl" => PrerequisiteMetadata {
            owner: FixOwner::AhRuntime,
            fix_available: false,
            privilege: PrivilegeClass::None,
            boundary: ExecutionBoundary::WslDistro,
            restart: RestartRequirement::None,
        },
        "wsl:systemd-user" => PrerequisiteMetadata {
            owner: FixOwner::AhRuntime,
            fix_available: true,
            privilege: PrivilegeClass::DistroSudo,
            boundary: ExecutionBoundary::WslDistro,
            restart: RestartRequirement::NeedsWslShutdown,
        },
        "wsl:tmux" => PrerequisiteMetadata {
            owner: FixOwner::AhRuntime,
            fix_available: true,
            privilege: PrivilegeClass::DistroSudo,
            boundary: ExecutionBoundary::WslDistro,
            restart: RestartRequirement::None,
        },
        // The sign-in doorman (decision 0006) launches provider login flows
        // that open a browser; inside WSL that needs the bridge to Windows.
        "wsl:browser-bridge" => PrerequisiteMetadata {
            owner: FixOwner::AhRuntime,
            fix_available: true,
            privilege: PrivilegeClass::DistroSudo,
            boundary: ExecutionBoundary::WslDistro,
            restart: RestartRequirement::None,
        },
        "daemon" | "tmux server orphans" | "tmux legacy shared session" => PrerequisiteMetadata {
            owner: FixOwner::DiagnosticOnly,
            fix_available: false,
            privilege: PrivilegeClass::None,
            boundary: ExecutionBoundary::CurrentProcess,
            restart: RestartRequirement::None,
        },
        "legacy repo state" | "permissions:cwd" | "permissions:.ah" => PrerequisiteMetadata {
            owner: FixOwner::UserProject,
            fix_available: false,
            privilege: PrivilegeClass::UserOwned,
            boundary: ExecutionBoundary::UserProject,
            restart: RestartRequirement::None,
        },
        "provider:home" => PrerequisiteMetadata {
            owner: FixOwner::ProviderExternal,
            fix_available: false,
            privilege: PrivilegeClass::External,
            boundary: ExecutionBoundary::External,
            restart: RestartRequirement::None,
        },
        id if id.starts_with("provider:") => PrerequisiteMetadata {
            owner: FixOwner::ProviderExternal,
            fix_available: false,
            privilege: PrivilegeClass::External,
            boundary: ExecutionBoundary::External,
            restart: RestartRequirement::None,
        },
        _ => PrerequisiteMetadata {
            owner: FixOwner::DiagnosticOnly,
            fix_available: false,
            privilege: PrivilegeClass::None,
            boundary: ExecutionBoundary::CurrentProcess,
            restart: RestartRequirement::None,
        },
    }
}

pub fn status_from_doctor(status: DoctorStatus) -> PrereqStatus {
    match status {
        DoctorStatus::Pass => PrereqStatus::Pass,
        DoctorStatus::Warn => PrereqStatus::Warn,
        DoctorStatus::Fail => PrereqStatus::Fail,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionBoundary, FixOwner, PrivilegeClass, RestartRequirement, metadata_for,
        runtime_prerequisite_from_doctor,
    };
    use crate::cli::doctor::{DoctorCheck, DoctorStatus};

    #[test]
    fn classifies_known_runtime_and_external_checks() {
        assert_eq!(metadata_for("wsl:tmux").owner, FixOwner::AhRuntime);
        assert!(metadata_for("wsl:tmux").fix_available);
        assert_eq!(
            metadata_for("wsl:systemd-user").restart,
            RestartRequirement::NeedsWslShutdown
        );
        assert_eq!(
            metadata_for("provider:claude").owner,
            FixOwner::ProviderExternal
        );
        assert_eq!(metadata_for("permissions:cwd").owner, FixOwner::UserProject);
        assert_eq!(
            metadata_for("tmux server orphans").owner,
            FixOwner::DiagnosticOnly
        );
    }

    #[test]
    fn converts_doctor_check_to_runtime_prerequisite_with_metadata() {
        let prereq = runtime_prerequisite_from_doctor(DoctorCheck {
            name: "wsl:systemd-user".to_string(),
            status: DoctorStatus::Fail,
            detail: "not running".to_string(),
            suggestion: Some("enable systemd".to_string()),
        });

        assert_eq!(prereq.id, "wsl:systemd-user");
        assert_eq!(prereq.owner, FixOwner::AhRuntime);
        assert_eq!(prereq.privilege, PrivilegeClass::DistroSudo);
        assert_eq!(prereq.boundary, ExecutionBoundary::WslDistro);
        assert_eq!(prereq.restart, RestartRequirement::NeedsWslShutdown);
        assert_eq!(prereq.suggestion.as_deref(), Some("enable systemd"));
    }
}
