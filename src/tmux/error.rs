use crate::error::AhError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TmuxError {
    #[error("tmux binary not found in PATH")]
    BinaryNotFound,

    #[error("tmux command failed: {cmd}: exit={exit}, stderr={stderr}")]
    CommandFailed {
        cmd: String,
        stderr: String,
        exit: i32,
    },

    #[error("invalid tmux pane id: {0}")]
    ParsePaneId(String),

    #[error("invalid tmux pane pid: {0}")]
    ParsePid(String),

    #[error("tmux I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<TmuxError> for AhError {
    fn from(err: TmuxError) -> Self {
        match err {
            TmuxError::BinaryNotFound => Self::TmuxNotFound,
            TmuxError::CommandFailed { cmd, stderr, exit } => {
                Self::TmuxCommandFailed { cmd, stderr, exit }
            }
            TmuxError::ParsePaneId(value) => Self::TmuxCommandFailed {
                cmd: "tmux parse pane id".into(),
                stderr: format!("invalid pane id: {value}"),
                exit: -1,
            },
            TmuxError::ParsePid(value) => Self::TmuxCommandFailed {
                cmd: "tmux display-message #{pane_pid}".into(),
                stderr: format!("invalid pane pid: {value}"),
                exit: -1,
            },
            TmuxError::Io(err) if err.kind() == std::io::ErrorKind::NotFound => Self::TmuxNotFound,
            TmuxError::Io(err) => Self::TmuxCommandFailed {
                cmd: "tmux".into(),
                stderr: err.to_string(),
                exit: -1,
            },
        }
    }
}
