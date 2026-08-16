use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct StateLayoutRequest {
    pub cwd: PathBuf,
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateLayout {
    pub state_dir: PathBuf,
    pub project_id: Option<String>,
}

/// Why a CLI command could not be pinned to a project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateLayoutError {
    /// No `ah.toml` anywhere above the working directory.
    NoProject { cwd: PathBuf },
    /// An explicit config path that cannot identify a project.
    BadConfigPath { path: PathBuf, details: String },
}

impl std::fmt::Display for StateLayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoProject { cwd } => write!(
                f,
                "no ah.toml found in {} or any parent directory; cd into a project or pass --config <path/to/ah.toml>",
                cwd.display()
            ),
            Self::BadConfigPath { path, details } => {
                write!(f, "--config {}: {details}", path.display())
            }
        }
    }
}

/// The one answer to "which project's stack is this CLI command talking to".
///
/// Modeled on git/cargo project discovery: an explicit config path is made
/// absolute against the working directory and must exist; without one the
/// directories above `cwd` are searched for `ah.toml`; and when neither
/// resolves the command fails instead of falling back to a shared `default`
/// state dir. That fallback let unrelated projects collide in one database
/// (#46), and hashing a bare `--config ah.toml`'s empty parent sent every such
/// invocation to one directory (#43). Environment overrides keep their
/// priority; `CCB_ENV=dev` keeps the in-repo dev state dir (decision 0005).
pub fn resolve_cli_state_layout(
    cwd: &Path,
    config_path: Option<&Path>,
) -> Result<StateLayout, StateLayoutError> {
    if let Some(dir) =
        non_empty_env_path("AH_STATE_DIR").or_else(|| non_empty_env_path("CCBD_STATE_DIR"))
    {
        return Ok(StateLayout {
            state_dir: dir,
            project_id: None,
        });
    }
    if let Some(dir) = non_empty_env_path("XDG_STATE_HOME") {
        return Ok(StateLayout {
            state_dir: dir.join("ccbd"),
            project_id: None,
        });
    }

    if let Some(path) = config_path {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        };
        let config_dir = if absolute.is_dir() {
            absolute
        } else if absolute.is_file() {
            match absolute.parent() {
                Some(parent) => parent.to_path_buf(),
                None => {
                    return Err(StateLayoutError::BadConfigPath {
                        path: absolute,
                        details: "has no parent directory".to_string(),
                    });
                }
            }
        } else {
            return Err(StateLayoutError::BadConfigPath {
                path: absolute,
                details: "does not exist".to_string(),
            });
        };
        // Canonicalization must succeed here: hashing an unresolved path is
        // exactly how the empty string became a shared project id.
        let canonical =
            config_dir
                .canonicalize()
                .map_err(|err| StateLayoutError::BadConfigPath {
                    path: config_dir.clone(),
                    details: format!("cannot canonicalize: {err}"),
                })?;
        return Ok(project_layout_for_dir(&canonical));
    }

    if std::env::var("CCB_ENV").as_deref() == Ok("dev") {
        return Ok(StateLayout {
            state_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("dev_state"),
            project_id: None,
        });
    }

    if let Some(config_dir) = find_config_dir_from_cwd(cwd) {
        return Ok(project_layout_for_dir(&config_dir));
    }

    Err(StateLayoutError::NoProject {
        cwd: cwd.to_path_buf(),
    })
}

pub fn resolve_state_layout(request: &StateLayoutRequest) -> StateLayout {
    if let Some(dir) =
        non_empty_env_path("AH_STATE_DIR").or_else(|| non_empty_env_path("CCBD_STATE_DIR"))
    {
        return StateLayout {
            state_dir: dir,
            project_id: None,
        };
    }

    if let Some(dir) = non_empty_env_path("XDG_STATE_HOME") {
        return StateLayout {
            state_dir: dir.join("ccbd"),
            project_id: None,
        };
    }

    if let Some(config_dir) = request.config_path.as_ref().and_then(config_dir_for_path) {
        return project_layout_for_dir(&config_dir);
    }

    if std::env::var("CCB_ENV").as_deref() == Ok("dev") {
        return StateLayout {
            state_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("dev_state"),
            project_id: None,
        };
    }

    if let Some(config_dir) = find_config_dir_from_cwd(&request.cwd) {
        return project_layout_for_dir(&config_dir);
    }

    tracing::warn!(
        cwd = %request.cwd.display(),
        "no ah.toml found upward; falling back to default state root"
    );
    StateLayout {
        state_dir: default_state_root().join("default"),
        project_id: None,
    }
}

pub fn resolve_neutral_state_layout() -> StateLayout {
    if let Some(dir) =
        non_empty_env_path("AH_STATE_DIR").or_else(|| non_empty_env_path("CCBD_STATE_DIR"))
    {
        return StateLayout {
            state_dir: dir,
            project_id: None,
        };
    }

    if let Some(dir) = non_empty_env_path("XDG_STATE_HOME") {
        return StateLayout {
            state_dir: dir.join("ccbd"),
            project_id: None,
        };
    }

    if std::env::var("CCB_ENV").as_deref() == Ok("dev") {
        return StateLayout {
            state_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("dev_state"),
            project_id: None,
        };
    }

    StateLayout {
        state_dir: default_state_root().join("default"),
        project_id: None,
    }
}

pub fn resolve_state_dir_for_config(config_dir: &Path) -> PathBuf {
    resolve_state_layout(&StateLayoutRequest {
        cwd: config_dir.to_path_buf(),
        config_path: Some(config_dir.join("ah.toml")),
    })
    .state_dir
}

pub fn resolve_state_dir_for_cwd(cwd: &Path) -> PathBuf {
    resolve_state_layout(&StateLayoutRequest {
        cwd: cwd.to_path_buf(),
        config_path: None,
    })
    .state_dir
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn config_dir_for_path(path: &PathBuf) -> Option<PathBuf> {
    if path.is_dir() {
        Some(path.clone())
    } else {
        path.parent().map(Path::to_path_buf)
    }
}

fn find_config_dir_from_cwd(cwd: &Path) -> Option<PathBuf> {
    let mut current = if cwd.is_file() {
        cwd.parent()?.to_path_buf()
    } else {
        cwd.to_path_buf()
    };

    loop {
        if current.join("ah.toml").is_file() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn project_layout_for_dir(config_dir: &Path) -> StateLayout {
    let project_id = project_id_for_dir(config_dir);
    StateLayout {
        state_dir: default_state_root().join(&project_id),
        project_id: Some(project_id),
    }
}

fn project_id_for_dir(config_dir: &Path) -> String {
    let canonical = config_dir
        .canonicalize()
        .unwrap_or_else(|_| config_dir.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(canonical.display().to_string().as_bytes());
    format!("{:x}", hasher.finalize())[..8].to_string()
}

/// The root every per-stack state directory lives under. `ah reclaim` walks it
/// to find stacks nobody is running any more.
pub fn default_state_root() -> PathBuf {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("state")
        .join("ah")
}

#[cfg(test)]
mod tests {
    use super::{StateLayoutRequest, resolve_state_layout};
    use std::ffi::OsString;
    use std::path::Path;

    struct EnvGuard {
        key: &'static str,
        old: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &Path) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old }
        }

        fn set_str(key: &'static str, value: &str) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old }
        }

        fn remove(key: &'static str) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, old }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.old {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn state_env_takes_priority_over_explicit_config_path() {
        let env_state = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let config_path = project.path().join("ah.toml");
        std::fs::write(&config_path, "version = \"1\"\n").unwrap();
        let _guard = EnvGuard::set("AH_STATE_DIR", env_state.path());

        let layout = resolve_state_layout(&StateLayoutRequest {
            cwd: project.path().to_path_buf(),
            config_path: Some(config_path),
        });

        assert_eq!(layout.state_dir, env_state.path());
        assert!(layout.project_id.is_none());
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn xdg_state_takes_priority_over_explicit_config_path() {
        let xdg_state = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let config_path = project.path().join("ah.toml");
        std::fs::write(&config_path, "version = \"1\"\n").unwrap();
        let _ah_guard = EnvGuard::remove("AH_STATE_DIR");
        let _ccbd_guard = EnvGuard::remove("CCBD_STATE_DIR");
        let _xdg_guard = EnvGuard::set("XDG_STATE_HOME", xdg_state.path());
        let _dev_guard = EnvGuard::set_str("CCB_ENV", "dev");

        let layout = resolve_state_layout(&StateLayoutRequest {
            cwd: project.path().to_path_buf(),
            config_path: Some(config_path),
        });

        assert_eq!(layout.state_dir, xdg_state.path().join("ccbd"));
        assert!(layout.project_id.is_none());
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn dev_mode_takes_priority_over_cwd_project_discovery() {
        let project = tempfile::tempdir().unwrap();
        std::fs::write(project.path().join("ah.toml"), "version = \"1\"\n").unwrap();
        let _ah_guard = EnvGuard::remove("AH_STATE_DIR");
        let _ccbd_guard = EnvGuard::remove("CCBD_STATE_DIR");
        let _xdg_guard = EnvGuard::remove("XDG_STATE_HOME");
        let _dev_guard = EnvGuard::set_str("CCB_ENV", "dev");

        let layout = resolve_state_layout(&StateLayoutRequest {
            cwd: project.path().to_path_buf(),
            config_path: None,
        });

        assert_eq!(
            layout.state_dir,
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("dev_state")
        );
        assert!(layout.project_id.is_none());
    }
}
