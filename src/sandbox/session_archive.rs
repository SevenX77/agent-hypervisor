//! Hand a sandbox's session records to the project before the sandbox dies.
//!
//! A provider writes its transcripts inside the sandbox home: codex rollouts,
//! claude project records, antigravity conversations. Those are the operator's
//! real development sessions — project assets, not ah artifacts (decision
//! 0004) — and every path that destroys a sandbox used to delete them along
//! with the home, which is how a normal window close lost a session for good
//! while a crash kept it (#27).
//!
//! So destruction archives first. The records land under the project's own
//! directory, because an asset that only exists inside ah's state dir is one
//! reclaim away from gone.

use crate::error::CcbdError;
use std::fs;
use std::path::{Path, PathBuf};

/// Name of the marker written into the sandbox dir at creation time, holding
/// the project root that owns the sandbox.
const PROJECT_ROOT_MARKER: &str = "project-root";

/// Where archives land inside the project, relative to its root.
const ARCHIVE_RELATIVE_ROOT: &str = ".ah/sessions";

/// What a provider leaves behind that is worth keeping, relative to the
/// sandbox home.
///
/// This is deliberately narrower than
/// [`crate::completion::log_layout::provider_log_root_in_home`]. That answers
/// "where does the provider write while it runs" and drives completion
/// signals; this answers "what is worth keeping once it has stopped". For
/// antigravity the two differ by the whole installed CLI — 18 MB of `bin/` per
/// sandbox measured on a live machine — so archiving the run-time root would
/// copy binaries into the operator's repository.
struct ProviderRecordSet {
    provider: &'static str,
    entries: &'static [&'static str],
}

const RECORD_SETS: &[ProviderRecordSet] = &[
    ProviderRecordSet {
        provider: "codex",
        entries: &[".codex/sessions", ".codex/history.jsonl"],
    },
    ProviderRecordSet {
        provider: "claude",
        entries: &[".claude/projects"],
    },
    ProviderRecordSet {
        provider: "antigravity",
        entries: &[
            ".gemini/antigravity-cli/conversations",
            ".gemini/antigravity-cli/conversation_summaries.db",
            ".gemini/antigravity-cli/history.jsonl",
            ".gemini/antigravity-cli/brain",
            ".gemini/antigravity-cli/knowledge",
        ],
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveOutcome {
    /// Records were copied into the project. Destruction may proceed.
    Archived { destination: PathBuf, files: usize },
    /// The home held no provider records. Destruction may proceed.
    NothingToArchive,
    /// No project root is known for this sandbox, so there is nowhere to hand
    /// records to. Distinct from a failure: destruction proceeds, because a
    /// sandbox created before the marker existed must not become undeletable.
    NoProjectRoot,
    /// A destination was known and the copy failed. Destruction must stop.
    Failed(String),
}

impl ArchiveOutcome {
    /// Whether the caller is allowed to destroy the sandbox home.
    pub fn permits_destruction(&self) -> bool {
        !matches!(self, Self::Failed(_))
    }
}

/// Records which project owns a sandbox, so destruction knows where to hand
/// the session records back.
///
/// A marker file rather than a database lookup: kill paths may drop the
/// session row before the sandbox is cleaned, and the master-death cleanup in
/// `ahd` runs without a handle to the session at all. The marker travels with
/// the sandbox, so every destruction path reads the same answer.
pub fn write_project_root_marker(sandbox_dir: &Path, project_root: &Path) -> Result<(), CcbdError> {
    let marker = sandbox_dir.join(PROJECT_ROOT_MARKER);
    fs::write(&marker, format!("{}\n", project_root.display())).map_err(|err| {
        CcbdError::SandboxMountFailed {
            details: format!("write sandbox project marker {}: {err}", marker.display()),
        }
    })
}

/// Whether a directory entry is the project marker, so cleanup paths that keep
/// a sandbox home can keep its ownership record with it.
pub fn is_project_root_marker(file_name: &std::ffi::OsStr) -> bool {
    file_name == std::ffi::OsStr::new(PROJECT_ROOT_MARKER)
}

pub fn read_project_root_marker(sandbox_dir: &Path) -> Option<PathBuf> {
    let raw = fs::read_to_string(sandbox_dir.join(PROJECT_ROOT_MARKER)).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(PathBuf::from(trimmed))
}

/// Whether a sandbox home holds any provider session records at all.
///
/// Callers that must decide whether destroying a home would lose something ask
/// this first, rather than inferring it from an archive attempt.
pub fn holds_session_records(home_root: &Path) -> bool {
    RECORD_SETS.iter().any(|set| {
        set.entries
            .iter()
            .any(|entry| holds_records(&home_root.join(entry)))
    })
}

/// Copies every provider record set found in `home_root` into the owning
/// project, keyed by session and agent.
pub fn archive_session_records(
    sandbox_dir: &Path,
    home_root: &Path,
    session_id: &str,
    agent_id: &str,
) -> ArchiveOutcome {
    let Some(project_root) = read_project_root_marker(sandbox_dir) else {
        return ArchiveOutcome::NoProjectRoot;
    };
    archive_session_records_into(&project_root, home_root, session_id, agent_id)
}

/// The archive step with the destination supplied directly, so it is testable
/// without a marker on disk.
pub fn archive_session_records_into(
    project_root: &Path,
    home_root: &Path,
    session_id: &str,
    agent_id: &str,
) -> ArchiveOutcome {
    let present: Vec<(&ProviderRecordSet, Vec<PathBuf>)> = RECORD_SETS
        .iter()
        .filter_map(|set| {
            let found: Vec<PathBuf> = set
                .entries
                .iter()
                .map(|entry| home_root.join(entry))
                .filter(|path| holds_records(path))
                .collect();
            (!found.is_empty()).then_some((set, found))
        })
        .collect();
    if present.is_empty() {
        return ArchiveOutcome::NothingToArchive;
    }

    let archive_root = project_root.join(ARCHIVE_RELATIVE_ROOT);
    if let Err(err) = ensure_self_ignoring_dir(&archive_root) {
        return ArchiveOutcome::Failed(err);
    }
    let destination = archive_root.join(session_id).join(agent_id);

    let mut files = 0usize;
    for (set, sources) in present {
        for source in sources {
            let relative = source.strip_prefix(home_root).unwrap_or(&source);
            let target = destination.join(set.provider).join(relative);
            match copy_records(&source, &target) {
                Ok(count) => files += count,
                Err(err) => {
                    return ArchiveOutcome::Failed(format!(
                        "archive {} to {}: {err}",
                        source.display(),
                        target.display()
                    ));
                }
            }
        }
    }

    ArchiveOutcome::Archived { destination, files }
}

/// Whether a record path actually holds something worth keeping.
///
/// Presence is not enough: ah materializes an empty `.codex/sessions` into every
/// codex sandbox, so an existence check would leave an empty archive folder and
/// a `.gitignore` in the project for every agent that never said anything. ah's
/// own leftovers are exactly what decision 0004 forbids.
fn holds_records(path: &Path) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() {
        return false;
    }
    if metadata.is_file() {
        return metadata.len() > 0;
    }
    if !metadata.is_dir() {
        return false;
    }
    fs::read_dir(path)
        .is_ok_and(|entries| entries.flatten().any(|entry| holds_records(&entry.path())))
}

/// Creates the archive root carrying its own `.gitignore`, the way cargo marks
/// `target/` and pytest marks `.pytest_cache/`. The archive belongs to the
/// project but not to its history, and saying so inside the directory keeps ah
/// out of the operator's own `.gitignore`.
fn ensure_self_ignoring_dir(archive_root: &Path) -> Result<(), String> {
    fs::create_dir_all(archive_root)
        .map_err(|err| format!("create {}: {err}", archive_root.display()))?;
    let ignore = archive_root.join(".gitignore");
    if ignore.exists() {
        return Ok(());
    }
    fs::write(&ignore, "*\n").map_err(|err| format!("write {}: {err}", ignore.display()))
}

/// Copies a file or directory tree, skipping symlinks.
///
/// Skipping is a safety property, not an optimisation: a sandbox home reaches
/// the host credential store through symlinks (decision 0003), so a copy that
/// followed links could walk a token into the project directory. Records
/// themselves are always real files.
fn copy_records(source: &Path, target: &Path) -> Result<usize, std::io::Error> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, target)?;
        return Ok(1);
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    fs::create_dir_all(target)?;
    let mut copied = 0usize;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        copied += copy_records(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn records_land_in_the_project_keyed_by_session_and_agent() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        write(
            &home.join(".codex/sessions/2026/rollout-abc.jsonl"),
            "{\"turn\":1}\n",
        );
        write(&home.join(".codex/history.jsonl"), "{\"cmd\":\"ls\"}\n");

        let outcome = archive_session_records_into(&project, &home, "sess_1", "worker_a");

        let ArchiveOutcome::Archived { destination, files } = outcome else {
            panic!("expected an archive, got {outcome:?}");
        };
        assert_eq!(files, 2);
        assert_eq!(
            destination,
            project.join(".ah/sessions/sess_1/worker_a"),
            "the archive must be addressable by session and agent"
        );
        assert!(
            destination
                .join("codex/.codex/sessions/2026/rollout-abc.jsonl")
                .is_file()
        );
        assert!(destination.join("codex/.codex/history.jsonl").is_file());
    }

    #[test]
    fn every_provider_record_set_is_archived_from_one_home() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        write(&home.join(".codex/sessions/r.jsonl"), "codex\n");
        write(&home.join(".claude/projects/-tmp-p/s.jsonl"), "claude\n");
        write(
            &home.join(".gemini/antigravity-cli/conversations/c.json"),
            "agy\n",
        );

        let outcome = archive_session_records_into(&project, &home, "sess_1", "master");

        let ArchiveOutcome::Archived { destination, files } = outcome else {
            panic!("expected an archive, got {outcome:?}");
        };
        assert_eq!(files, 3);
        assert!(destination.join("codex/.codex/sessions/r.jsonl").is_file());
        assert!(
            destination
                .join("claude/.claude/projects/-tmp-p/s.jsonl")
                .is_file()
        );
        assert!(
            destination
                .join("antigravity/.gemini/antigravity-cli/conversations/c.json")
                .is_file()
        );
    }

    /// The record set exists precisely so the provider's installed binaries do
    /// not follow its conversations into the operator's repository.
    #[test]
    fn the_installed_provider_cli_is_not_archived() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        write(
            &home.join(".gemini/antigravity-cli/conversations/c.json"),
            "agy\n",
        );
        write(&home.join(".gemini/antigravity-cli/bin/agy"), "ELF\n");
        write(&home.join(".gemini/antigravity-cli/cache/blob"), "cache\n");

        let ArchiveOutcome::Archived { destination, files } =
            archive_session_records_into(&project, &home, "sess_1", "a1")
        else {
            panic!("expected an archive");
        };

        assert_eq!(files, 1, "only the conversation is a project asset");
        assert!(
            !destination
                .join("antigravity/.gemini/antigravity-cli/bin")
                .exists()
        );
        assert!(
            !destination
                .join("antigravity/.gemini/antigravity-cli/cache")
                .exists()
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_symlinks_are_never_copied_into_the_project() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        let host_secret = temp.path().join("host-credentials.json");
        fs::write(&host_secret, "{\"refresh_token\":\"secret\"}").unwrap();
        write(&home.join(".claude/projects/-tmp-p/s.jsonl"), "claude\n");
        std::os::unix::fs::symlink(&host_secret, home.join(".claude/projects/linked.json"))
            .unwrap();

        let ArchiveOutcome::Archived { destination, files } =
            archive_session_records_into(&project, &home, "sess_1", "a1")
        else {
            panic!("expected an archive");
        };

        assert_eq!(files, 1);
        assert!(
            !destination
                .join("claude/.claude/projects/linked.json")
                .exists(),
            "a symlink out of the sandbox must not be followed into the project"
        );
    }

    #[test]
    fn the_archive_directory_ignores_itself() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        write(&home.join(".codex/sessions/r.jsonl"), "codex\n");

        archive_session_records_into(&project, &home, "sess_1", "a1");

        let ignore = project.join(".ah/sessions/.gitignore");
        assert_eq!(
            fs::read_to_string(&ignore).unwrap(),
            "*\n",
            "the archive must keep itself out of the project's history"
        );
    }

    #[test]
    fn a_home_without_records_reports_nothing_to_archive() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        fs::create_dir_all(home.join(".cache")).unwrap();

        assert_eq!(
            archive_session_records_into(&project, &home, "sess_1", "a1"),
            ArchiveOutcome::NothingToArchive
        );
        assert!(
            !project.exists(),
            "an empty archive must not create directories in the project"
        );
    }

    /// ah creates an empty transcript directory in every sandbox it prepares.
    /// An agent that never spoke must leave nothing behind in the project.
    #[test]
    fn an_agent_that_wrote_nothing_leaves_no_folder_in_the_project() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        fs::create_dir_all(home.join(".codex/sessions")).unwrap();
        fs::write(home.join(".codex/history.jsonl"), "").unwrap();
        fs::create_dir_all(home.join(".claude/projects")).unwrap();

        assert_eq!(
            archive_session_records_into(&project, &home, "sess_1", "a1"),
            ArchiveOutcome::NothingToArchive
        );
        assert!(!project.exists());
    }

    #[test]
    fn archiving_twice_overwrites_instead_of_failing() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        write(&home.join(".codex/sessions/r.jsonl"), "turn one\n");
        archive_session_records_into(&project, &home, "sess_1", "a1");
        write(
            &home.join(".codex/sessions/r.jsonl"),
            "turn one\nturn two\n",
        );

        let outcome = archive_session_records_into(&project, &home, "sess_1", "a1");

        assert!(matches!(outcome, ArchiveOutcome::Archived { .. }));
        let archived = project.join(".ah/sessions/sess_1/a1/codex/.codex/sessions/r.jsonl");
        assert_eq!(
            fs::read_to_string(archived).unwrap(),
            "turn one\nturn two\n"
        );
    }

    #[test]
    fn the_marker_round_trips_the_project_root() {
        let temp = tempfile::TempDir::new().unwrap();
        let sandbox_dir = temp.path().join("sandboxes/s1/a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let project = temp.path().join("some project");

        write_project_root_marker(&sandbox_dir, &project).unwrap();

        assert_eq!(read_project_root_marker(&sandbox_dir), Some(project));
    }

    /// A sandbox created before the marker existed must stay deletable, so a
    /// missing marker is reported apart from a failure.
    #[test]
    fn a_sandbox_without_a_marker_reports_no_project_root() {
        let temp = tempfile::TempDir::new().unwrap();
        let sandbox_dir = temp.path().join("sandboxes/s1/a1");
        let home = temp.path().join("home");
        fs::create_dir_all(&sandbox_dir).unwrap();
        write(&home.join(".codex/sessions/r.jsonl"), "codex\n");

        let outcome = archive_session_records(&sandbox_dir, &home, "s1", "a1");

        assert_eq!(outcome, ArchiveOutcome::NoProjectRoot);
        assert!(outcome.permits_destruction());
    }

    #[test]
    fn a_failed_archive_forbids_destruction() {
        let failed = ArchiveOutcome::Failed("disk full".into());

        assert!(!failed.permits_destruction());
        assert!(ArchiveOutcome::NothingToArchive.permits_destruction());
        assert!(ArchiveOutcome::NoProjectRoot.permits_destruction());
    }

    /// An unusable destination must be reported as a failure, never as a
    /// silent success — the sandbox is deleted on the strength of this answer.
    #[test]
    fn an_unusable_destination_fails_instead_of_reporting_success() {
        let temp = tempfile::TempDir::new().unwrap();
        let project = temp.path().join("project");
        let home = temp.path().join("home");
        write(&home.join(".codex/sessions/r.jsonl"), "codex\n");
        // A regular file where the archive root has to be a directory.
        write(&project.join(".ah"), "not a directory\n");

        let outcome = archive_session_records_into(&project, &home, "sess_1", "a1");

        assert!(
            matches!(outcome, ArchiveOutcome::Failed(_)),
            "got {outcome:?}"
        );
        assert!(!outcome.permits_destruction());
    }
}
