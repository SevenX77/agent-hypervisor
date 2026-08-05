//! Sandbox filesystem path resolution.

use crate::error::CcbdError;
use std::path::{Path, PathBuf};

/// Resolve and create the per-agent sandbox directory below the ccbd state dir.
///
/// `project_root` is recorded next to the sandbox so destruction can hand the
/// provider's session records back to the project that owns them (decision
/// 0004). Taking it as a parameter rather than looking it up later is what
/// makes that ownership impossible to forget at a new call site.
pub fn resolve_sandbox_dir(
    state_dir: &Path,
    session_id: &str,
    agent_id: &str,
    project_root: &Path,
) -> Result<PathBuf, CcbdError> {
    validate_id_charset("session_id", session_id)?;
    validate_id_charset("agent_id", agent_id)?;

    let sandbox_dir = state_dir.join("sandboxes").join(session_id).join(agent_id);
    std::fs::create_dir_all(&sandbox_dir).map_err(|err| CcbdError::SandboxMountFailed {
        details: format!("create sandbox dir {}: {err}", sandbox_dir.display()),
    })?;
    crate::sandbox::session_archive::write_project_root_marker(&sandbox_dir, project_root)?;

    Ok(sandbox_dir)
}

pub(crate) struct SandboxDirGuard {
    path: Option<PathBuf>,
}

impl SandboxDirGuard {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    pub(crate) fn release(mut self) -> PathBuf {
        self.path.take().expect("SandboxDirGuard released twice")
    }

    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

impl Drop for SandboxDirGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        match crate::provider::home_layout::sandbox_home_for_sandbox_dir(&path) {
            Ok(home_root) => {
                // A failed spawn usually leaves an empty home, but a spawn that
                // failed while recovering onto a preserved home would take that
                // home's session records with it, so the same archive-first rule
                // applies here (decision 0004).
                if !archive_before_discarding_home(&path, &home_root) {
                    return;
                }
                if let Err(err) = std::fs::remove_dir_all(&home_root)
                    && err.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        ?home_root,
                        ?err,
                        "SandboxDirGuard home cleanup failed in Drop"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    ?path,
                    ?err,
                    "SandboxDirGuard failed to resolve sandbox home"
                );
            }
        }
        if let Err(err) = std::fs::remove_dir_all(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(?path, ?err, "SandboxDirGuard cleanup failed in Drop");
        }
    }
}

/// Returns whether the home may be discarded.
fn archive_before_discarding_home(sandbox_dir: &Path, home_root: &Path) -> bool {
    use crate::sandbox::session_archive::{ArchiveOutcome, archive_session_records};

    let (session_id, agent_id) = split_sandbox_dir_ids(sandbox_dir);
    match archive_session_records(sandbox_dir, home_root, &session_id, &agent_id) {
        ArchiveOutcome::Archived { destination, files } => {
            tracing::info!(
                files,
                destination = %destination.display(),
                "archived session records before discarding a sandbox home"
            );
            true
        }
        ArchiveOutcome::NothingToArchive | ArchiveOutcome::NoProjectRoot => true,
        ArchiveOutcome::Failed(details) => {
            tracing::error!(
                home_root = %home_root.display(),
                details,
                "keeping a sandbox home whose session records could not be archived"
            );
            false
        }
    }
}

/// The sandbox dir is `<state>/sandboxes/<session_id>/<agent_id>`, so its last
/// two components are the archive key.
fn split_sandbox_dir_ids(sandbox_dir: &Path) -> (String, String) {
    let agent_id = sandbox_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown-agent".to_string());
    let session_id = sandbox_dir
        .parent()
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown-session".to_string());
    (session_id, agent_id)
}

fn validate_id_charset(field: &str, value: &str) -> Result<(), CcbdError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(CcbdError::IpcInvalidRequest(format!(
            "invalid {field} for sandbox path: {value}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SandboxDirGuard, resolve_sandbox_dir};
    use crate::error::CcbdError;
    use crate::provider::home_layout::sandbox_home_for_sandbox_dir;

    #[test]
    fn test_resolve_sandbox_dir_creates_directory() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = resolve_sandbox_dir(tmp.path(), "sess_abc", "ag_1", tmp.path()).unwrap();

        assert_eq!(
            dir,
            tmp.path().join("sandboxes").join("sess_abc").join("ag_1")
        );
        assert!(dir.is_dir());
    }

    #[test]
    fn test_resolve_sandbox_dir_includes_session_id() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = resolve_sandbox_dir(tmp.path(), "sess_abc", "ag_1", tmp.path()).unwrap();

        assert_eq!(
            dir,
            tmp.path().join("sandboxes").join("sess_abc").join("ag_1")
        );
        assert!(dir.is_dir());
    }

    #[test]
    fn test_resolve_sandbox_dir_isolates_agents_by_session() {
        let tmp = tempfile::TempDir::new().unwrap();
        let left = resolve_sandbox_dir(tmp.path(), "sess_abc", "ag_1", tmp.path()).unwrap();
        let right = resolve_sandbox_dir(tmp.path(), "sess_def", "ag_1", tmp.path()).unwrap();

        assert_ne!(left, right);
        assert_eq!(
            left,
            tmp.path().join("sandboxes").join("sess_abc").join("ag_1")
        );
        assert_eq!(
            right,
            tmp.path().join("sandboxes").join("sess_def").join("ag_1")
        );
    }

    #[test]
    fn test_resolve_sandbox_dir_rejects_invalid_session_id() {
        let tmp = tempfile::TempDir::new().unwrap();
        let empty = resolve_sandbox_dir(tmp.path(), "", "ag_1", tmp.path()).unwrap_err();
        let traversal = resolve_sandbox_dir(tmp.path(), "../escape", "ag_1", tmp.path()).unwrap_err();

        assert!(matches!(empty, CcbdError::IpcInvalidRequest(_)));
        assert!(matches!(traversal, CcbdError::IpcInvalidRequest(_)));
    }

    #[test]
    fn test_resolve_sandbox_dir_rejects_path_traversal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let err = resolve_sandbox_dir(tmp.path(), "sess_abc", "../escape", tmp.path()).unwrap_err();

        assert!(matches!(err, CcbdError::IpcInvalidRequest(_)));
    }

    #[test]
    fn test_sandbox_dir_guard_drop_removes_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("agent_home");
        std::fs::create_dir_all(&dir).unwrap();

        {
            let _guard = SandboxDirGuard::new(dir.clone());
        }

        assert!(!dir.exists());
    }

    #[test]
    fn test_sandbox_dir_guard_drop_removes_materialized_home() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = resolve_sandbox_dir(tmp.path(), "sess_guard", "ag_guard", tmp.path()).unwrap();
        let home_root = sandbox_home_for_sandbox_dir(&dir).unwrap();
        std::fs::create_dir_all(home_root.join(".codex")).unwrap();
        std::fs::write(home_root.join(".codex/auth.json"), b"token").unwrap();

        {
            let _guard = SandboxDirGuard::new(dir.clone());
        }

        assert!(!dir.exists());
        assert!(!home_root.exists());
    }

    /// A spawn that fails on a home recovered from an earlier run must not take
    /// that run's session records with it.
    #[test]
    fn test_sandbox_dir_guard_drop_archives_session_records_first() {
        let tmp = tempfile::TempDir::new().unwrap();
        let project = tmp.path().join("project");
        let dir = resolve_sandbox_dir(tmp.path(), "sess_arch", "ag_arch", &project).unwrap();
        let home_root = sandbox_home_for_sandbox_dir(&dir).unwrap();
        std::fs::create_dir_all(home_root.join(".codex/sessions")).unwrap();
        std::fs::write(home_root.join(".codex/sessions/r.jsonl"), b"prior run").unwrap();

        {
            let _guard = SandboxDirGuard::new(dir.clone());
        }

        assert!(!home_root.exists());
        assert_eq!(
            std::fs::read_to_string(
                project.join(".ah/sessions/sess_arch/ag_arch/codex/.codex/sessions/r.jsonl")
            )
            .unwrap(),
            "prior run"
        );
    }

    #[test]
    fn test_sandbox_dir_guard_release_keeps_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("agent_home");
        std::fs::create_dir_all(&dir).unwrap();

        {
            let guard = SandboxDirGuard::new(dir.clone());
            let released = guard.release();
            assert_eq!(released, dir);
        }

        assert!(dir.is_dir());
    }

    #[test]
    fn test_sandbox_dir_guard_handles_panic() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("agent_home");
        std::fs::create_dir_all(&dir).unwrap();
        let panic_result = std::panic::catch_unwind({
            let dir = dir.clone();
            move || {
                let _guard = SandboxDirGuard::new(dir);
                panic!("simulate spawn panic");
            }
        });

        assert!(panic_result.is_err());
        assert!(!dir.exists());
    }
}
