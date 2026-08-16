//! macOS service supervisor compile skeleton.

use crate::error::AhError;
use crate::provider::manifest::ENV_PASSTHROUGH;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceUnitError {
    #[error("service value contains unsupported control character")]
    InvalidValue,

    #[error("ahd binary path must be absolute: {0}")]
    RelativeAhdPath(PathBuf),

    #[error("neither XDG_CONFIG_HOME nor HOME is set for user service dir")]
    MissingUserConfigHome,
}

pub fn derive_unit_name(state_dir: &Path) -> String {
    let normalized = normalized_absolute_path(state_dir);
    let mut hasher = Sha256::new();
    hasher.update(normalized.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    format!("ah-{}.service", &hex[..16])
}

pub fn build_ahd_systemd_run_command(ahd_bin: &Path, state_dir: &Path) -> Vec<String> {
    let env = ENV_PASSTHROUGH
        .iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .collect::<Vec<_>>();
    build_ahd_systemd_run_command_with_env(ahd_bin, state_dir, &env)
}

pub fn build_ahd_systemd_run_command_with_parent(
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
    parent_unit: Option<&str>,
) -> Vec<String> {
    let _ = parent_unit;
    build_ahd_systemd_run_command_with_env(ahd_bin, state_dir, env)
}

pub fn build_ahd_systemd_run_command_with_env(
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
) -> Vec<String> {
    let mut cmd = vec![
        ahd_bin.display().to_string(),
        "--state-dir".to_string(),
        state_dir.display().to_string(),
    ];
    for key in ENV_PASSTHROUGH {
        if let Some((_, value)) = env.iter().find(|(candidate, _)| candidate == key) {
            cmd.insert(0, format!("{key}={value}"));
            cmd.insert(0, "env".to_string());
        }
    }
    cmd
}

pub fn ahd_reset_failed_is_best_effort(_unit: &str) -> bool {
    tracing::warn!("macOS: systemctl reset-failed unsupported until PR-5 launchd supervisor");
    true
}

pub fn escape_systemd_env_value(value: &str) -> Result<String, ServiceUnitError> {
    escape_common(value, false)
}

pub fn escape_systemd_exec_token(value: &str) -> Result<String, ServiceUnitError> {
    escape_common(value, true)
}

pub fn render_unit_file(
    unit_name: &str,
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
) -> Result<String, ServiceUnitError> {
    if !ahd_bin.is_absolute() {
        return Err(ServiceUnitError::RelativeAhdPath(ahd_bin.to_path_buf()));
    }
    let _ = (unit_name, env);
    Ok(format!(
        "# macOS launchd plist rendering lands in PR-5; AH_STATE_DIR={}\n",
        normalized_absolute_path(state_dir).display()
    ))
}

pub fn resolve_user_systemd_dir(
    xdg_config_home: Option<&str>,
    home: Option<&str>,
) -> Result<PathBuf, ServiceUnitError> {
    if let Some(xdg_config_home) = xdg_config_home.filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(xdg_config_home).join("LaunchAgents"));
    }
    if let Some(home) = home.filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(home).join("Library/LaunchAgents"));
    }
    Err(ServiceUnitError::MissingUserConfigHome)
}

pub fn resolve_user_systemd_dir_from_env() -> Result<PathBuf, ServiceUnitError> {
    let xdg_config_home = env::var("XDG_CONFIG_HOME").ok();
    let home = env::var("HOME").ok();
    resolve_user_systemd_dir(xdg_config_home.as_deref(), home.as_deref())
}

pub fn atomic_write_unit(path: &Path, content: &str) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unit path has no parent: {}", path.display()),
        )
    })?;
    fs::create_dir_all(parent)?;

    let temp_path = unique_temp_path(parent, path);
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    temp.write_all(content.as_bytes())?;
    temp.sync_all()?;
    drop(temp);
    match fs::rename(&temp_path, path) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = fs::remove_file(&temp_path);
            Err(err)
        }
    }
}

pub fn unsupported_service_error(action: &str) -> AhError {
    AhError::EnvironmentNotSupported {
        details: format!("macOS: service supervisor action '{action}' is unsupported until PR-5"),
    }
}

fn normalized_absolute_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()
                .map(|cwd| cwd.join(path))
                .unwrap_or_else(|_| path.to_path_buf())
        }
    })
}

fn unique_temp_path(parent: &Path, target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("unit");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nanos))
}

fn escape_common(value: &str, double_dollar: bool) -> Result<String, ServiceUnitError> {
    if value.chars().any(char::is_control) {
        return Err(ServiceUnitError::InvalidValue);
    }

    let mut out = String::with_capacity(value.len());
    let mut needs_quotes = value.is_empty();
    for ch in value.chars() {
        match ch {
            '$' if double_dollar => out.push_str("$$"),
            '%' => out.push_str("%%"),
            '"' => {
                needs_quotes = true;
                out.push_str("\\\"");
            }
            '\\' => {
                needs_quotes = true;
                out.push_str("\\\\");
            }
            ch if ch.is_whitespace() => {
                needs_quotes = true;
                out.push(ch);
            }
            ch => out.push(ch),
        }
    }

    if needs_quotes {
        Ok(format!("\"{out}\""))
    } else {
        Ok(out)
    }
}
