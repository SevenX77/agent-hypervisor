//! Linux pidfd registry and syscall helpers.

use crate::error::AhError;
use std::collections::HashMap;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::sync::{Arc, LazyLock, Mutex};

pub type MonitorHandle = OwnedFd;
pub type BorrowedMonitorHandle<'a> = BorrowedFd<'a>;

/// Process-file-descriptor registry keyed by agent or master monitor key.
pub static PIDFD_REGISTRY: LazyLock<Arc<Mutex<HashMap<String, OwnedFd>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Open a pidfd for a live process id.
pub fn pidfd_open(pid: i32) -> Result<OwnedFd, AhError> {
    // SAFETY: pidfd_open is called with a plain pid and flags=0. It returns a new
    // owned file descriptor on success, or -1 with errno set on failure.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0_u32) };
    if raw < 0 {
        let err = std::io::Error::last_os_error();
        return match err.raw_os_error() {
            Some(libc::ESRCH) => Err(AhError::AgentUnexpectedExit {
                details: format!("pid {pid} is not alive"),
            }),
            Some(errno) => Err(AhError::SandboxMountFailed {
                details: format!("pidfd_open({pid}) failed with errno {errno}: {err}"),
            }),
            None => Err(AhError::SandboxMountFailed {
                details: format!("pidfd_open({pid}) failed: {err}"),
            }),
        };
    }

    // SAFETY: raw is a fresh file descriptor returned by pidfd_open above, so
    // OwnedFd becomes the unique owner and will close it on Drop.
    Ok(unsafe { OwnedFd::from_raw_fd(raw as RawFd) })
}

/// Send SIGKILL through a pidfd.
pub fn pidfd_send_sigkill(pidfd: BorrowedFd<'_>) -> Result<(), AhError> {
    // SAFETY: the borrowed pidfd remains valid for the duration of this call,
    // siginfo is null as allowed by pidfd_send_signal, and flags=0.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            libc::SIGKILL,
            std::ptr::null::<libc::siginfo_t>(),
            0_u32,
        )
    };
    if result < 0 {
        let err = std::io::Error::last_os_error();
        return Err(AhError::PtyIoError(format!(
            "pidfd_send_signal(SIGKILL) failed: {err}"
        )));
    }

    Ok(())
}

/// Register or replace a pidfd for a key.
pub fn register(key: String, fd: OwnedFd) {
    match PIDFD_REGISTRY.lock() {
        Ok(mut registry) => {
            registry.insert(key, fd);
        }
        Err(err) => tracing::warn!(error = %err, "pidfd registry mutex poisoned during register"),
    }
}

/// Remove a pidfd from the registry, transferring ownership to the caller.
pub fn remove(key: &str) -> Option<OwnedFd> {
    match PIDFD_REGISTRY.lock() {
        Ok(mut registry) => registry.remove(key),
        Err(err) => {
            tracing::warn!(error = %err, "pidfd registry mutex poisoned during remove");
            None
        }
    }
}

/// Borrow a registered pidfd while the registry lock is held.
pub fn with_borrowed<R>(key: &str, f: impl FnOnce(BorrowedFd<'_>) -> R) -> Option<R> {
    match PIDFD_REGISTRY.lock() {
        Ok(registry) => registry.get(key).map(|fd| f(fd.as_fd())),
        Err(err) => {
            tracing::warn!(error = %err, "pidfd registry mutex poisoned during borrow");
            None
        }
    }
}

/// Return true when a key has a registered pidfd.
pub fn contains(key: &str) -> bool {
    match PIDFD_REGISTRY.lock() {
        Ok(registry) => registry.contains_key(key),
        Err(err) => {
            tracing::warn!(error = %err, "pidfd registry mutex poisoned during contains");
            false
        }
    }
}

/// Return all registered monitor keys in stable order for diagnostics.
pub fn list_keys() -> Vec<String> {
    match PIDFD_REGISTRY.lock() {
        Ok(registry) => {
            let mut keys = registry.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            keys
        }
        Err(err) => {
            tracing::warn!(error = %err, "pidfd registry mutex poisoned during list_keys");
            Vec::new()
        }
    }
}
