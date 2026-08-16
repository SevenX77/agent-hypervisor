//! Clipboard Port and operating-system Adapter used by terminal interactions.

use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum ClipboardError {
    #[error("no supported system clipboard command is available")]
    Unavailable,
    #[error("could not start clipboard backend `{backend}`: {details}")]
    Launch {
        backend: &'static str,
        details: String,
    },
    #[error("clipboard backend `{backend}` failed")]
    Failed { backend: &'static str },
    #[error("clipboard backend `{backend}` did not return UTF-8 text")]
    InvalidText { backend: &'static str },
}

/// Stable Port used by domain interaction code. Clipboard contents never
/// enter provider contracts or lifecycle state.
pub trait ClipboardPort {
    fn copy_text(&self, value: &str) -> Result<(), ClipboardError>;
    fn paste_text(&self) -> Result<String, ClipboardError>;
}

/// Host clipboard Adapter. Backend selection is isolated here so OAuth and
/// provider code remain independent of WSL, Wayland, X11 and macOS details.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClipboardAdapter;

impl ClipboardPort for SystemClipboardAdapter {
    fn copy_text(&self, value: &str) -> Result<(), ClipboardError> {
        copy_text(value)
    }

    fn paste_text(&self) -> Result<String, ClipboardError> {
        paste_text()
    }
}

#[cfg(target_os = "linux")]
fn copy_text(value: &str) -> Result<(), ClipboardError> {
    if is_wsl() && which::which("clip.exe").is_ok() {
        return write_command("clip.exe", &[], value);
    }
    if which::which("wl-copy").is_ok() {
        return write_command("wl-copy", &[], value);
    }
    if which::which("xclip").is_ok() {
        return write_command("xclip", &["-selection", "clipboard"], value);
    }
    Err(ClipboardError::Unavailable)
}

#[cfg(target_os = "linux")]
fn paste_text() -> Result<String, ClipboardError> {
    if is_wsl() && which::which("powershell.exe").is_ok() {
        return read_command(
            "powershell.exe",
            &[
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::OutputEncoding=[Text.Encoding]::UTF8; Get-Clipboard -Raw",
            ],
        );
    }
    if which::which("wl-paste").is_ok() {
        return read_command("wl-paste", &["--no-newline"]);
    }
    if which::which("xclip").is_ok() {
        return read_command("xclip", &["-selection", "clipboard", "-o"]);
    }
    Err(ClipboardError::Unavailable)
}

#[cfg(target_os = "macos")]
fn copy_text(value: &str) -> Result<(), ClipboardError> {
    write_command("pbcopy", &[], value)
}

#[cfg(target_os = "macos")]
fn paste_text() -> Result<String, ClipboardError> {
    read_command("pbpaste", &[])
}

#[cfg(windows)]
fn copy_text(value: &str) -> Result<(), ClipboardError> {
    write_command("clip.exe", &[], value)
}

#[cfg(windows)]
fn paste_text() -> Result<String, ClipboardError> {
    read_command(
        "powershell.exe",
        &[
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "[Console]::OutputEncoding=[Text.Encoding]::UTF8; Get-Clipboard -Raw",
        ],
    )
}

#[cfg(target_os = "linux")]
fn is_wsl() -> bool {
    std::env::var_os("WSL_DISTRO_NAME").is_some()
        || std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .is_ok_and(|release| release.to_ascii_lowercase().contains("microsoft"))
}

fn write_command(backend: &'static str, args: &[&str], value: &str) -> Result<(), ClipboardError> {
    let mut child = Command::new(backend)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| ClipboardError::Launch {
            backend,
            details: err.to_string(),
        })?;
    child
        .stdin
        .take()
        .expect("clipboard stdin is piped")
        .write_all(value.as_bytes())
        .map_err(|err| ClipboardError::Launch {
            backend,
            details: err.to_string(),
        })?;
    let status = child.wait().map_err(|err| ClipboardError::Launch {
        backend,
        details: err.to_string(),
    })?;
    if status.success() {
        Ok(())
    } else {
        Err(ClipboardError::Failed { backend })
    }
}

fn read_command(backend: &'static str, args: &[&str]) -> Result<String, ClipboardError> {
    let output = Command::new(backend)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|err| ClipboardError::Launch {
            backend,
            details: err.to_string(),
        })?;
    if !output.status.success() {
        return Err(ClipboardError::Failed { backend });
    }
    String::from_utf8(output.stdout).map_err(|_| ClipboardError::InvalidText { backend })
}
