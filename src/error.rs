use thiserror::Error;

#[derive(Error, Debug)]
pub enum AhError {
    #[error("database constraint violation: {0}")]
    DbConstraintViolation(String),

    #[error("database evidence not found: {details}")]
    DbEvidenceNotFound { details: String },

    #[error("agent not found: {0}")]
    AgentNotFound(String),

    #[error("agent already exists: {0}")]
    AgentAlreadyExists(String),

    #[error("pty open failed: {0}")]
    PtyOpenFailed(String),

    #[error("pty I/O error: {0}")]
    PtyIoError(String),

    #[error("invalid IPC request: {0}")]
    IpcInvalidRequest(String),

    #[error("sandbox user namespace disabled: {details}")]
    SandboxUserNsDisabled { details: String },

    #[error("sandbox mount failed: {details}")]
    SandboxMountFailed { details: String },

    #[error("environment not supported: {details}")]
    EnvironmentNotSupported { details: String },

    #[error("agent unexpected exit: {details}")]
    AgentUnexpectedExit { details: String },

    #[error("agent is not in IDLE state: current_state={current_state}")]
    AgentWrongState { current_state: String },

    #[error("startup marker timeout: {details}")]
    StartupMarkerTimeout { details: String },

    #[error("PTY marker timeout: {details}")]
    PtyMarkerTimeout { details: String },

    #[error("duplicate request for existing event seq_id={existing_seq_id}")]
    DuplicateRequest { existing_seq_id: i64 },

    #[error("database runtime panic: {details}")]
    DatabaseRuntimePanic { details: String },

    #[error("tmux binary not found in PATH")]
    TmuxNotFound,

    #[error("tmux command failed: {cmd}: exit={exit}, stderr={stderr}")]
    TmuxCommandFailed {
        cmd: String,
        stderr: String,
        exit: i32,
    },
}

impl AhError {
    /// Convert this error into the ah JSON-RPC error object.
    pub fn to_rpc_error(&self) -> serde_json::Value {
        let (error_code, details, current_state) = match self {
            Self::DbConstraintViolation(_) => ("DB_CONSTRAINT_VIOLATION", None, None),
            Self::DbEvidenceNotFound { details } => ("DB_EVIDENCE_NOT_FOUND", Some(details), None),
            Self::AgentNotFound(_) => ("AGENT_NOT_FOUND", None, None),
            Self::AgentAlreadyExists(_) => ("AGENT_ALREADY_EXISTS", None, None),
            Self::PtyOpenFailed(_) => ("PTY_OPEN_FAILED", None, None),
            Self::PtyIoError(_) => ("PTY_IO_ERROR", None, None),
            Self::IpcInvalidRequest(_) => ("IPC_INVALID_REQUEST", None, None),
            Self::SandboxUserNsDisabled { details } => {
                ("SANDBOX_USER_NS_DISABLED", Some(details), None)
            }
            Self::SandboxMountFailed { details } => ("SANDBOX_MOUNT_FAILED", Some(details), None),
            Self::EnvironmentNotSupported { details } => {
                ("ENVIRONMENT_NOT_SUPPORTED", Some(details), None)
            }
            Self::AgentUnexpectedExit { details } => ("AGENT_UNEXPECTED_EXIT", Some(details), None),
            Self::AgentWrongState { current_state } => {
                ("AGENT_WRONG_STATE", None, Some(current_state))
            }
            Self::StartupMarkerTimeout { details } => {
                ("STARTUP_MARKER_TIMEOUT", Some(details), None)
            }
            Self::PtyMarkerTimeout { details } => ("PTY_MARKER_TIMEOUT", Some(details), None),
            Self::DatabaseRuntimePanic { details } => ("DB_RUNTIME_PANIC", Some(details), None),
            Self::TmuxNotFound => ("TMUX_NOT_FOUND", None, None),
            Self::TmuxCommandFailed { stderr, .. } => ("TMUX_COMMAND_FAILED", Some(stderr), None),
            Self::DuplicateRequest { .. } => {
                unreachable!("DuplicateRequest is an internal DB sentinel, not an RPC error")
            }
        };

        let mut data = serde_json::json!({
            "error_code": error_code,
        });
        if let Some(details) = details {
            data["details"] = serde_json::json!(details);
        }
        if let Some(current_state) = current_state {
            data["current_state"] = serde_json::json!(current_state);
        }

        serde_json::json!({
            "code": -32000,
            "message": self.to_string(),
            "data": data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::AhError;

    fn assert_rpc_error(error: AhError, expected_code: &str, expected_message: &str) {
        let obj = error.to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], expected_code);
        assert!(obj["message"].as_str().unwrap().contains(expected_message));
    }

    #[test]
    fn test_agent_not_found_to_rpc_error() {
        let obj = AhError::AgentNotFound("ag_1".into()).to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "AGENT_NOT_FOUND");
        assert!(obj["message"].as_str().unwrap().contains("ag_1"));
    }

    #[test]
    fn test_ipc_invalid_request_to_rpc_error() {
        let obj = AhError::IpcInvalidRequest("missing field 'method'".into()).to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["message"]
                .as_str()
                .unwrap()
                .contains("missing field 'method'")
        );
    }

    #[test]
    fn test_db_constraint_violation_round_trip() {
        assert_rpc_error(
            AhError::DbConstraintViolation("foreign key failed".into()),
            "DB_CONSTRAINT_VIOLATION",
            "foreign key failed",
        );
    }

    #[test]
    fn test_agent_not_found_round_trip() {
        assert_rpc_error(
            AhError::AgentNotFound("ag_missing".into()),
            "AGENT_NOT_FOUND",
            "ag_missing",
        );
    }

    #[test]
    fn test_db_evidence_not_found_round_trip() {
        let obj = AhError::DbEvidenceNotFound {
            details: "evidence_id=evi_missing".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "DB_EVIDENCE_NOT_FOUND");
        assert_eq!(obj["data"]["details"], "evidence_id=evi_missing");
    }

    #[test]
    fn test_agent_already_exists_round_trip() {
        assert_rpc_error(
            AhError::AgentAlreadyExists("ag_1".into()),
            "AGENT_ALREADY_EXISTS",
            "ag_1",
        );
    }

    #[test]
    fn test_pty_open_failed_round_trip() {
        assert_rpc_error(
            AhError::PtyOpenFailed("openpty failed".into()),
            "PTY_OPEN_FAILED",
            "openpty failed",
        );
    }

    #[test]
    fn test_pty_io_error_round_trip() {
        assert_rpc_error(
            AhError::PtyIoError("broken pipe".into()),
            "PTY_IO_ERROR",
            "broken pipe",
        );
    }

    #[test]
    fn test_ipc_invalid_request_round_trip() {
        assert_rpc_error(
            AhError::IpcInvalidRequest("bad json".into()),
            "IPC_INVALID_REQUEST",
            "bad json",
        );
    }

    #[test]
    fn test_sandbox_user_ns_disabled_round_trip() {
        let obj = AhError::SandboxUserNsDisabled {
            details: "kernel setting disabled".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "SANDBOX_USER_NS_DISABLED");
        assert_eq!(obj["data"]["details"], "kernel setting disabled");
    }

    #[test]
    fn test_sandbox_mount_failed_round_trip() {
        let obj = AhError::SandboxMountFailed {
            details: "forbidden path".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "SANDBOX_MOUNT_FAILED");
        assert_eq!(obj["data"]["details"], "forbidden path");
    }

    #[test]
    fn test_environment_not_supported_round_trip() {
        let obj = AhError::EnvironmentNotSupported {
            details: "systemd-run missing".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "ENVIRONMENT_NOT_SUPPORTED");
        assert_eq!(obj["data"]["details"], "systemd-run missing");
    }

    #[test]
    fn test_agent_unexpected_exit_round_trip() {
        let obj = AhError::AgentUnexpectedExit {
            details: "pid already dead".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "AGENT_UNEXPECTED_EXIT");
        assert_eq!(obj["data"]["details"], "pid already dead");
    }

    #[test]
    fn test_agent_wrong_state_round_trip() {
        let obj = AhError::AgentWrongState {
            current_state: "BUSY".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "AGENT_WRONG_STATE");
        assert_eq!(obj["data"]["current_state"], "BUSY");
    }

    #[test]
    fn test_startup_marker_timeout_round_trip() {
        let obj = AhError::StartupMarkerTimeout {
            details: "no prompt".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "STARTUP_MARKER_TIMEOUT");
        assert_eq!(obj["data"]["details"], "no prompt");
    }

    #[test]
    fn test_pty_marker_timeout_round_trip() {
        let obj = AhError::PtyMarkerTimeout {
            details: "busy timeout".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "PTY_MARKER_TIMEOUT");
        assert_eq!(obj["data"]["details"], "busy timeout");
    }

    #[test]
    fn test_db_runtime_panic_round_trip() {
        let obj = AhError::DatabaseRuntimePanic {
            details: "spawned task panicked: SqliteFailure(BUSY)".into(),
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "DB_RUNTIME_PANIC");
        assert!(obj["data"]["details"].as_str().unwrap().contains("BUSY"));
    }

    #[test]
    fn test_tmux_not_found_round_trip() {
        assert_rpc_error(AhError::TmuxNotFound, "TMUX_NOT_FOUND", "tmux binary");
    }

    #[test]
    fn test_tmux_command_failed_round_trip() {
        let obj = AhError::TmuxCommandFailed {
            cmd: "tmux -L ah-test has-session -t nope".into(),
            stderr: "no server running".into(),
            exit: 1,
        }
        .to_rpc_error();

        assert_eq!(obj["code"], -32000);
        assert_eq!(obj["data"]["error_code"], "TMUX_COMMAND_FAILED");
        assert_eq!(obj["data"]["details"], "no server running");
        assert!(obj["message"].as_str().unwrap().contains("tmux -L ah-test"));
    }
}
