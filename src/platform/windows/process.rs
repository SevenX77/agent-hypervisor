//! Windows process monitor handle stubs for the M0 compile gate.

use crate::error::AhError;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Mutex};
use windows_sys::Win32::Foundation::HANDLE;

#[derive(Debug)]
pub struct MonitorHandle {
    raw: HANDLE,
}

unsafe impl Send for MonitorHandle {}

#[derive(Clone, Copy, Debug)]
pub struct BorrowedMonitorHandle<'a> {
    raw: HANDLE,
    _lifetime: PhantomData<&'a MonitorHandle>,
}

impl MonitorHandle {
    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self { raw: self.raw })
    }

    pub fn borrowed(&self) -> BorrowedMonitorHandle<'_> {
        BorrowedMonitorHandle {
            raw: self.raw,
            _lifetime: PhantomData,
        }
    }
}

pub static PIDFD_REGISTRY: LazyLock<Arc<Mutex<HashMap<String, MonitorHandle>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

pub fn pidfd_open(_pid: i32) -> Result<MonitorHandle, AhError> {
    Err(AhError::EnvironmentNotSupported {
        details: "Windows process handle monitoring is not implemented until M1".to_string(),
    })
}

pub fn pidfd_send_sigkill(handle: BorrowedMonitorHandle<'_>) -> Result<(), AhError> {
    let _ = handle.raw;
    Err(AhError::EnvironmentNotSupported {
        details: "Windows process termination through monitor handle is not implemented until M1"
            .to_string(),
    })
}

pub fn register(key: String, handle: MonitorHandle) {
    match PIDFD_REGISTRY.lock() {
        Ok(mut registry) => {
            registry.insert(key, handle);
        }
        Err(err) => {
            tracing::warn!(error = %err, "Windows monitor registry mutex poisoned during register")
        }
    }
}

pub fn remove(key: &str) -> Option<MonitorHandle> {
    match PIDFD_REGISTRY.lock() {
        Ok(mut registry) => registry.remove(key),
        Err(err) => {
            tracing::warn!(error = %err, "Windows monitor registry mutex poisoned during remove");
            None
        }
    }
}

pub fn with_borrowed<R>(key: &str, f: impl FnOnce(BorrowedMonitorHandle<'_>) -> R) -> Option<R> {
    match PIDFD_REGISTRY.lock() {
        Ok(registry) => registry.get(key).map(|handle| f(handle.borrowed())),
        Err(err) => {
            tracing::warn!(error = %err, "Windows monitor registry mutex poisoned during borrow");
            None
        }
    }
}

pub fn contains(key: &str) -> bool {
    match PIDFD_REGISTRY.lock() {
        Ok(registry) => registry.contains_key(key),
        Err(err) => {
            tracing::warn!(error = %err, "Windows monitor registry mutex poisoned during contains");
            false
        }
    }
}

pub fn list_keys() -> Vec<String> {
    match PIDFD_REGISTRY.lock() {
        Ok(registry) => {
            let mut keys = registry.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            keys
        }
        Err(err) => {
            tracing::warn!(error = %err, "Windows monitor registry mutex poisoned during list_keys");
            Vec::new()
        }
    }
}
