//! Platform abstraction boundary for OS-specific process and supervisor behavior.

use crate::error::AhError;
use std::path::Path;

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "macos")]
pub mod macos;

#[cfg(windows)]
pub mod windows;

#[cfg(target_os = "linux")]
pub use linux as sys;

#[cfg(target_os = "macos")]
pub use macos as sys;

#[cfg(windows)]
pub use windows as sys;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: i32,
    pub generation: Option<i64>,
    pub start_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessExit {
    Exited { pid: i32, exit_code: Option<i32> },
    WatchLost { pid: i32, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeHandle {
    pub id: String,
    pub owner_session_id: Option<String>,
    pub owner_agent_id: Option<String>,
    pub process_group_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CascadeTarget {
    Session { session_id: String },
    Agent { agent_id: String },
    TmuxServer { socket_name: String },
    Master { session_id: String, generation: i64 },
}

pub trait ProcessWatcher {
    type WatchHandle;

    fn process_is_alive(pid: i32) -> bool;
    fn register_watch(key: String, handle: Self::WatchHandle);
    fn remove_watch(key: &str) -> Option<Self::WatchHandle>;
}

pub trait ProcessReaper {
    fn sigkill(identity: &ProcessIdentity) -> Result<(), AhError>;
}

pub trait ScopeManager {
    fn stop_scope(scope: &ScopeHandle) -> Result<(), AhError>;
}

pub trait ServiceSupervisor {
    fn install_or_restart(service_name: &str) -> Result<(), AhError>;
}

pub trait DaemonIdentity {
    fn current_daemon_unit() -> Option<String>;
}

pub trait ProcInfo {
    fn process_state(pid: i32) -> Option<u8>;
}

pub trait PlatformDiagnostics {
    fn check_environment() -> Result<(), AhError>;
    fn socket_dir_for_uid(uid: u32) -> std::path::PathBuf {
        Path::new("/tmp").join(format!("tmux-{uid}"))
    }
}
