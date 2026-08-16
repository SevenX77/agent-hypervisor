//! Browser-opening Port and operating-system Adapter.

use std::process::{Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum BrowserOpenError {
    #[error("no system browser opener is available")]
    Unavailable,
    #[error("could not start browser opener `{opener}`: {details}")]
    Launch { opener: String, details: String },
    #[error("browser opener `{opener}` failed")]
    Failed { opener: String },
}

/// Stable Port used after an authorization URL has been reconstructed and
/// validated. Provider code does not select an operating-system bridge.
pub trait BrowserOpenerPort {
    fn open_url(&self, url: &str) -> Result<(), BrowserOpenError>;
}

/// Host Adapter. On WSL this deliberately resolves the `xdg-open` shim that
/// `ah setup --fix` installs to hand URLs to the Windows default browser.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemBrowserOpenerAdapter;

impl BrowserOpenerPort for SystemBrowserOpenerAdapter {
    fn open_url(&self, url: &str) -> Result<(), BrowserOpenError> {
        open_url(url)
    }
}

#[cfg(target_os = "linux")]
fn open_url(url: &str) -> Result<(), BrowserOpenError> {
    let opener = which::which("xdg-open")
        .or_else(|_| which::which("wslview"))
        .map_err(|_| BrowserOpenError::Unavailable)?;
    run_opener(opener.display().to_string(), &opener, &[url])
}

#[cfg(target_os = "macos")]
fn open_url(url: &str) -> Result<(), BrowserOpenError> {
    run_opener("open".to_string(), std::path::Path::new("open"), &[url])
}

#[cfg(windows)]
fn open_url(url: &str) -> Result<(), BrowserOpenError> {
    run_opener(
        "rundll32.exe".to_string(),
        std::path::Path::new("rundll32.exe"),
        &["url.dll,FileProtocolHandler", url],
    )
}

fn run_opener(
    label: String,
    program: &std::path::Path,
    args: &[&str],
) -> Result<(), BrowserOpenError> {
    let status = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|err| BrowserOpenError::Launch {
            opener: label.clone(),
            details: err.to_string(),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(BrowserOpenError::Failed { opener: label })
    }
}
