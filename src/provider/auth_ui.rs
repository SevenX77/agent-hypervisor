//! Provider-neutral terminal interaction for authorization-code challenges.

use crate::platform::clipboard::{ClipboardPort, SystemClipboardAdapter};
use std::io::{self, IsTerminal, Read, Write};

#[derive(Debug, thiserror::Error)]
pub enum AuthUiError {
    #[error("terminal input failed: {0}")]
    Input(#[from] io::Error),
    #[error("authentication was cancelled")]
    Cancelled,
}

pub fn request_authorization_code(provider: &str, url: &str) -> Result<String, AuthUiError> {
    if !io::stdin().is_terminal() {
        return read_manual_code();
    }

    let clipboard = SystemClipboardAdapter;
    eprintln!();
    eprintln!("{} sign-in", display_provider(provider));
    eprintln!();
    eprintln!("Open this URL in your browser:");
    eprintln!("------------------------------------------------------------");
    eprintln!("{url}");
    eprintln!("------------------------------------------------------------");
    eprintln!("[c] copy URL");
    eprintln!();
    eprintln!("After authorizing, copy the code shown in the browser.");
    eprintln!("[v] paste and submit code");

    loop {
        match read_menu_key()? {
            b'c' | b'C' => match clipboard.copy_text(url) {
                Ok(()) => eprintln!("Link copied."),
                Err(err) => eprintln!("Could not copy the link: {err}"),
            },
            b'v' | b'V' => {
                // A provider can need a few seconds to exchange the code.
                // Acknowledge the operator's key immediately so that external
                // latency is never mistaken for a missed paste action.
                eprintln!("Reading authorization code from the clipboard...");
                match clipboard.paste_text() {
                    Ok(value) => match normalize_authorization_code(&value) {
                        Ok(code) => {
                            eprintln!("Authorization code:");
                            eprintln!(
                                "------------------------------------------------------------"
                            );
                            eprintln!("{code}");
                            eprintln!(
                                "------------------------------------------------------------"
                            );
                            return Ok(code);
                        }
                        Err(reason) => {
                            eprintln!("Clipboard does not contain an authorization code: {reason}")
                        }
                    },
                    Err(err) => eprintln!("Could not read the clipboard: {err}"),
                }
            }
            3 => {
                eprintln!();
                return Err(AuthUiError::Cancelled);
            }
            // Windows Terminal can emit focus/control escape sequences while
            // attaching to the WSL tmux client. Ignore every key except the
            // two documented actions and Ctrl+C so those bytes do not add
            // misleading prompts to the login transcript.
            _ => {}
        }
    }
}

pub fn prepare_device_code(
    provider: &str,
    url: &str,
    device_code: &str,
) -> Result<(), AuthUiError> {
    let clipboard = SystemClipboardAdapter;
    eprintln!();
    eprintln!("{} sign-in", display_provider(provider));
    eprintln!();
    eprintln!("Open this URL in your browser:");
    eprintln!("------------------------------------------------------------");
    eprintln!("{url}");
    eprintln!("------------------------------------------------------------");
    eprintln!();
    eprintln!("One-time code:");
    eprintln!("------------------------------------------------------------");
    eprintln!("{device_code}");
    eprintln!("------------------------------------------------------------");

    if !io::stdin().is_terminal() {
        return Ok(());
    }

    eprintln!("[c] copy code");
    loop {
        match read_menu_key()? {
            b'c' | b'C' => match clipboard.copy_text(device_code) {
                Ok(()) => {
                    eprintln!("Code copied. Complete sign-in in the browser.");
                    eprintln!("Waiting for sign-in confirmation...");
                    return Ok(());
                }
                Err(err) => eprintln!("\rCould not copy the code: {err}"),
            },
            3 => {
                eprintln!();
                return Err(AuthUiError::Cancelled);
            }
            _ => {}
        }
    }
}

fn display_provider(provider: &str) -> String {
    let mut chars = provider.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "Provider".to_string(),
    }
}

fn normalize_authorization_code(raw: &str) -> Result<String, &'static str> {
    let code = raw.trim();
    if code.is_empty() {
        return Err("the clipboard is empty");
    }
    if code.starts_with("https://") || code.starts_with("http://") {
        return Err("it still contains the sign-in link");
    }
    if code.len() > 4096
        || code
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err("the value is not a single authorization-code token");
    }
    Ok(code.to_string())
}

fn read_manual_code() -> Result<String, AuthUiError> {
    eprint!("Authorization code: ");
    io::stderr().flush().ok();
    let mut code = String::new();
    io::stdin().read_line(&mut code)?;
    normalize_authorization_code(&code)
        .map_err(|reason| io::Error::new(io::ErrorKind::InvalidInput, reason).into())
}

fn read_menu_key() -> io::Result<u8> {
    #[cfg(unix)]
    let _raw_mode = UnixRawModeGuard::enable()?;

    let mut key = [0_u8; 1];
    io::stdin().read_exact(&mut key)?;
    Ok(key[0])
}

#[cfg(unix)]
struct UnixRawModeGuard {
    original: libc::termios,
}

#[cfg(unix)]
impl UnixRawModeGuard {
    fn enable() -> io::Result<Self> {
        let fd = libc::STDIN_FILENO;
        let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
        // SAFETY: `original` points to valid writable storage and stdin is the
        // exact descriptor whose mode is restored by this guard.
        if unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: tcgetattr initialized `original` after the successful call.
        let original = unsafe { original.assume_init() };
        let mut raw = original;
        // SAFETY: `raw` is an initialized termios value owned by this scope.
        unsafe { libc::cfmakeraw(&mut raw) };
        // SAFETY: `raw` remains valid for the duration of the call.
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { original })
    }
}

#[cfg(unix)]
impl Drop for UnixRawModeGuard {
    fn drop(&mut self) {
        // SAFETY: `original` was read from stdin and stays initialized until
        // this guard restores it.
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.original);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{display_provider, normalize_authorization_code};

    #[test]
    fn clipboard_code_accepts_one_token_without_exposing_it() {
        assert_eq!(
            normalize_authorization_code("  code-value#state-value\r\n").unwrap(),
            "code-value#state-value"
        );
    }

    #[test]
    fn sign_in_link_cannot_be_submitted_as_the_authorization_code() {
        assert_eq!(
            normalize_authorization_code("https://claude.com/cai/oauth/authorize?state=s1"),
            Err("it still contains the sign-in link")
        );
    }

    #[test]
    fn authorization_code_cannot_inject_terminal_controls() {
        assert_eq!(
            normalize_authorization_code("code\u{1b}[2J"),
            Err("the value is not a single authorization-code token")
        );
    }

    #[test]
    fn provider_heading_is_readable() {
        assert_eq!(display_provider("claude"), "Claude");
        assert_eq!(display_provider(""), "Provider");
    }
}
