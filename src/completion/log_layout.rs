use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogRootResolution {
    Available(PathBuf),
    Unavailable(LogSignalUnavailable),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSignalUnavailable {
    pub reason: &'static str,
}

impl LogRootResolution {
    pub fn expect_available(self) -> PathBuf {
        match self {
            Self::Available(path) => path,
            Self::Unavailable(unavailable) => {
                panic!("expected available log root, got {}", unavailable.reason)
            }
        }
    }

    pub fn expect_unavailable_reason(self) -> &'static str {
        match self {
            Self::Available(path) => {
                panic!("expected unavailable log root, got {}", path.display())
            }
            Self::Unavailable(unavailable) => unavailable.reason,
        }
    }
}

pub fn resolve_agent_log_root(
    state_dir: &Path,
    session_id: &str,
    agent_id: &str,
    provider: &str,
    unsafe_no_sandbox: bool,
) -> LogRootResolution {
    if unsafe_no_sandbox {
        return unavailable("unsafe_no_sandbox_shared_home");
    }

    let sandbox_dir = state_dir.join("sandboxes").join(session_id).join(agent_id);
    let home_root = match crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox_dir) {
        Ok(path) => path,
        Err(_) => return unavailable("sandbox_home_unavailable"),
    };
    let Some(root) = provider_log_root_in_home(provider, &home_root) else {
        return unavailable("unsupported_provider");
    };

    match std::fs::read_dir(&root) {
        Ok(_) => LogRootResolution::Available(root),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => unavailable("log_root_missing"),
        Err(_) => unavailable("log_root_unreadable"),
    }
}

/// Where a provider writes its session transcripts inside a sandbox home. This
/// is the single owner of that fact; both worker completion and master revive
/// readiness read it from here rather than spelling the path themselves.
pub fn provider_log_root_in_home(provider: &str, home_root: &Path) -> Option<PathBuf> {
    match crate::provider::manifest::canonicalize_provider_name(provider) {
        "codex" => Some(home_root.join(".codex/sessions")),
        "claude" => Some(home_root.join(".claude/projects")),
        "antigravity" => Some(home_root.join(".gemini/antigravity-cli")),
        _ => None,
    }
}

fn unavailable(reason: &'static str) -> LogRootResolution {
    LogRootResolution::Unavailable(LogSignalUnavailable { reason })
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[test]
    fn codex_log_root_recomputes_from_state_session_agent() {
        let temp = tempfile::TempDir::new().unwrap();
        let state_dir = temp.path();
        let sandbox_dir = state_dir.join("sandboxes").join("s1").join("a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let home_root =
            crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let sessions = home_root.join(".codex/sessions");
        fs::create_dir_all(&sessions).unwrap();

        let root =
            super::resolve_agent_log_root(state_dir, "s1", "a1", "codex", false).expect_available();

        assert_eq!(root, sessions);
    }

    #[test]
    fn claude_log_root_recomputes_from_state_session_agent() {
        let temp = tempfile::TempDir::new().unwrap();
        let state_dir = temp.path();
        let sandbox_dir = state_dir.join("sandboxes").join("s1").join("a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let home_root =
            crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let projects = home_root.join(".claude/projects");
        fs::create_dir_all(&projects).unwrap();

        let root = super::resolve_agent_log_root(state_dir, "s1", "a1", "claude", false)
            .expect_available();

        assert_eq!(root, projects);
    }

    #[test]
    fn antigravity_log_root_recomputes_from_state_session_agent() {
        let temp = tempfile::TempDir::new().unwrap();
        let state_dir = temp.path();
        let sandbox_dir = state_dir.join("sandboxes").join("s1").join("a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let home_root =
            crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let antigravity = home_root.join(".gemini/antigravity-cli");
        fs::create_dir_all(&antigravity).unwrap();

        let root = super::resolve_agent_log_root(state_dir, "s1", "a1", "antigravity", false)
            .expect_available();

        assert_eq!(root, antigravity);
    }

    #[test]
    fn unsafe_no_sandbox_disables_provider_log_signal() {
        let temp = tempfile::TempDir::new().unwrap();

        for provider in ["codex", "claude", "antigravity"] {
            let result = super::resolve_agent_log_root(temp.path(), "s1", "a1", provider, true);
            assert_eq!(
                result.expect_unavailable_reason(),
                "unsafe_no_sandbox_shared_home"
            );
        }
    }

    #[test]
    fn missing_log_root_returns_ui_fallback_immediately() {
        let temp = tempfile::TempDir::new().unwrap();
        let state_dir = temp.path();
        let sandbox_dir = state_dir.join("sandboxes").join("s1").join("a1");
        fs::create_dir_all(&sandbox_dir).unwrap();

        let result = super::resolve_agent_log_root(state_dir, "s1", "a1", "codex", false);

        assert_eq!(result.expect_unavailable_reason(), "log_root_missing");
    }
}
