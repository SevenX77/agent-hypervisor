//! Reclaiming ah's own leftovers.
//!
//! D1 keeps the state database bounded and D2 hands session records to the
//! project, but both only run on the normal path. A crash, a power cut or a
//! `kill -9` leaves a sandbox home, a tmux socket, a systemd unit and a state
//! directory with no owner to collect them: on one machine, 812 of 978 sandbox
//! homes belonged to stacks that no longer existed. This is the explicit entry
//! point that collects them, so the operator never has to reach for `rm -rf`
//! (decision 0004, slice D).
//!
//! Two rules shape everything here. Nothing owned by a running daemon is ever
//! touched, and the project's session archive is never touched at all — ah
//! reclaims its own artifacts, not the project's assets.

use crate::sandbox::session_archive::{self, ArchiveOutcome};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Leftovers younger than this are left alone by default: a stack that died
/// minutes ago may be under investigation, and a reclaim command must not tidy
/// away the evidence. Mirrors `docker system prune --filter until=`.
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    /// A sandbox home under `~/.cache/ah/sandboxes/<hash>`.
    SandboxHome,
    /// A per-stack state directory under the state root.
    StateDir,
    /// A tmux socket whose server is gone.
    TmuxSocket,
}

impl ItemKind {
    pub fn label(&self) -> &'static str {
        match self {
            Self::SandboxHome => "sandbox home",
            Self::StateDir => "state dir",
            Self::TmuxSocket => "tmux socket",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimItem {
    pub kind: ItemKind,
    pub path: PathBuf,
    pub bytes: u64,
    /// Why this is reclaimable, shown in the report so the operator can judge.
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReclaimPlan {
    pub items: Vec<ReclaimItem>,
    /// Leftovers that matched but were younger than the age floor.
    pub skipped_too_recent: usize,
    /// Leftovers skipped because a live daemon still owns them.
    pub skipped_in_use: usize,
}

impl ReclaimPlan {
    pub fn bytes(&self) -> u64 {
        self.items.iter().map(|item| item.bytes).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    pub removed: usize,
    pub bytes_freed: u64,
    /// Items left in place, with the reason — an archive that failed, or a
    /// removal that errored.
    pub kept: Vec<(PathBuf, String)>,
}

/// Everything the survey needs, supplied by the caller so the scan is testable
/// without a real machine.
pub struct ReclaimScope {
    pub sandbox_root: PathBuf,
    pub state_root: PathBuf,
    pub tmux_socket_dir: Option<PathBuf>,
    pub min_age: Duration,
    /// State directories whose daemon is alive. Anything reachable from one of
    /// these is off limits.
    pub live_state_dirs: HashSet<PathBuf>,
    pub now: SystemTime,
    /// Whether a tmux server is listening on a socket. Injected so the survey
    /// can be tested without a tmux binary.
    pub tmux_server_alive: Box<dyn Fn(&Path) -> bool>,
}

/// Asks tmux itself whether a socket still has a server behind it. A stale
/// socket file outlives its server, so the file's existence proves nothing.
pub fn tmux_server_alive(socket: &Path) -> bool {
    std::process::Command::new("tmux")
        .arg("-S")
        .arg(socket)
        .arg("list-sessions")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Surveys what could be reclaimed without changing anything.
pub fn survey(scope: &ReclaimScope) -> ReclaimPlan {
    let mut plan = ReclaimPlan::default();
    let claimed = claimed_sandbox_homes(&scope.state_root);

    survey_sandbox_homes(scope, &claimed, &mut plan);
    survey_state_dirs(scope, &mut plan);
    survey_tmux_sockets(scope, &mut plan);
    plan.items.sort_by(|a, b| b.bytes.cmp(&a.bytes));
    plan
}

/// Every sandbox home some existing state directory still points at.
///
/// The home's name is a hash of its `<state>/sandboxes/<session>/<agent>` path,
/// so the claim is recomputed rather than read from anywhere: a home whose
/// pointer directory is gone can have no owner by construction.
fn claimed_sandbox_homes(state_root: &Path) -> HashSet<PathBuf> {
    let mut claimed = HashSet::new();
    let Ok(state_dirs) = std::fs::read_dir(state_root) else {
        return claimed;
    };
    for state_dir in state_dirs.flatten() {
        let sandboxes = state_dir.path().join("sandboxes");
        let Ok(sessions) = std::fs::read_dir(&sandboxes) else {
            continue;
        };
        for session in sessions.flatten() {
            let Ok(agents) = std::fs::read_dir(session.path()) else {
                continue;
            };
            for agent in agents.flatten() {
                if let Ok(home) =
                    crate::home_materialization::sandbox_home_for_sandbox_dir(&agent.path())
                {
                    claimed.insert(home);
                }
            }
        }
    }
    claimed
}

fn survey_sandbox_homes(scope: &ReclaimScope, claimed: &HashSet<PathBuf>, plan: &mut ReclaimPlan) {
    let Ok(entries) = std::fs::read_dir(&scope.sandbox_root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if claimed.contains(&path) {
            plan.skipped_in_use += 1;
            continue;
        }
        if !older_than(&path, scope) {
            plan.skipped_too_recent += 1;
            continue;
        }
        plan.items.push(ReclaimItem {
            kind: ItemKind::SandboxHome,
            bytes: directory_size(&path),
            path,
            reason: "no state directory points at this home".to_string(),
        });
    }
}

fn survey_state_dirs(scope: &ReclaimScope, plan: &mut ReclaimPlan) {
    let Ok(entries) = std::fs::read_dir(&scope.state_root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if scope.live_state_dirs.contains(&path) {
            plan.skipped_in_use += 1;
            continue;
        }
        if !older_than(&path, scope) {
            plan.skipped_too_recent += 1;
            continue;
        }
        plan.items.push(ReclaimItem {
            kind: ItemKind::StateDir,
            bytes: directory_size(&path),
            path,
            reason: "no daemon holds this stack's socket".to_string(),
        });
    }
}

fn survey_tmux_sockets(scope: &ReclaimScope, plan: &mut ReclaimPlan) {
    let Some(socket_dir) = scope.tmux_socket_dir.as_ref() else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(socket_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        // Only ah's own sockets; a socket for someone else's tmux is not ours
        // to collect.
        if !name.starts_with("ahd-") {
            continue;
        }
        if (scope.tmux_server_alive)(&path) {
            plan.skipped_in_use += 1;
            continue;
        }
        plan.items.push(ReclaimItem {
            kind: ItemKind::TmuxSocket,
            path,
            bytes: 0,
            reason: "no tmux server is listening".to_string(),
        });
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    /// Where to put session records that belong to no known project.
    ///
    /// An orphan home is orphaned precisely because the directory holding its
    /// project marker is gone, so ah cannot hand its records back the way
    /// decision 0004 requires. Deleting them anyway would break the rule the
    /// whole decision exists for, so such homes are kept until the operator
    /// names a destination.
    pub archive_unattributable_to: Option<PathBuf>,
}

/// Executes a surveyed plan.
///
/// Sandbox homes go through the same archive-first rule as any other
/// destruction (decision 0004 D2): a home whose session records cannot reach a
/// destination is kept, not reclaimed.
pub fn execute(plan: &ReclaimPlan, options: &ExecuteOptions) -> ReclaimReport {
    let mut report = ReclaimReport::default();
    for item in &plan.items {
        if item.kind == ItemKind::SandboxHome
            && let Err(reason) = hand_over_records(&item.path, options)
        {
            report.kept.push((item.path.clone(), reason));
            continue;
        }
        let removed = if item.kind == ItemKind::TmuxSocket {
            std::fs::remove_file(&item.path)
        } else {
            std::fs::remove_dir_all(&item.path)
        };
        match removed {
            Ok(()) => {
                report.removed += 1;
                report.bytes_freed += item.bytes;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => report.kept.push((item.path.clone(), err.to_string())),
        }
    }
    report
}

/// Hands a home's session records to a destination, or explains why it cannot.
///
/// `Ok` means the home may now be destroyed: either its records were copied
/// out, or there were none to copy.
fn hand_over_records(home_root: &Path, options: &ExecuteOptions) -> Result<(), String> {
    let key = home_root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_string());
    let Some(destination) = options.archive_unattributable_to.as_ref() else {
        return if session_archive::holds_session_records(home_root) {
            Err(
                "holds session records but no project owns it; pass --archive-to <dir> to keep them"
                    .to_string(),
            )
        } else {
            Ok(())
        };
    };
    match session_archive::archive_session_records_into(destination, home_root, "orphans", &key) {
        ArchiveOutcome::Archived { .. } | ArchiveOutcome::NothingToArchive => Ok(()),
        ArchiveOutcome::NoProjectRoot => Ok(()),
        ArchiveOutcome::Failed(details) => Err(details),
    }
}

fn older_than(path: &Path, scope: &ReclaimScope) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    scope
        .now
        .duration_since(modified)
        .map(|age| age >= scope.min_age)
        .unwrap_or(false)
}

fn directory_size(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            continue;
        }
        total += if metadata.is_dir() {
            directory_size(&entry.path())
        } else {
            metadata.len()
        };
    }
    total
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn scope_for(temp: &Path) -> ReclaimScope {
        ReclaimScope {
            sandbox_root: temp.join("cache/ah/sandboxes"),
            state_root: temp.join("state/ah"),
            tmux_socket_dir: None,
            min_age: Duration::from_secs(0),
            live_state_dirs: HashSet::new(),
            now: SystemTime::now(),
            tmux_server_alive: Box::new(|_| false),
        }
    }

    fn make_home(temp: &Path, name: &str, bytes: usize) -> PathBuf {
        let home = temp.join("cache/ah/sandboxes").join(name);
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join("blob"), vec![b'x'; bytes]).unwrap();
        home
    }

    #[test]
    fn a_home_no_state_dir_points_at_is_reclaimable() {
        let temp = tempfile::TempDir::new().unwrap();
        let orphan = make_home(temp.path(), "deadbeef0000", 128);
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();

        let plan = survey(&scope_for(temp.path()));

        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].path, orphan);
        assert_eq!(plan.items[0].kind, ItemKind::SandboxHome);
        assert!(plan.bytes() >= 128);
    }

    /// The claim is recomputed from the pointer directory, so a home that is
    /// still addressed by a state dir must survive.
    #[test]
    fn a_home_a_state_dir_still_points_at_is_left_alone() {
        let temp = tempfile::TempDir::new().unwrap();
        let sandbox_dir = temp.path().join("state/ah/proj/sandboxes/s1/a1");
        fs::create_dir_all(&sandbox_dir).unwrap();
        let home = crate::home_materialization::sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join("blob"), b"live").unwrap();
        let mut scope = scope_for(temp.path());
        scope.sandbox_root = home.parent().unwrap().to_path_buf();
        scope.live_state_dirs = [temp.path().join("state/ah/proj")].into_iter().collect();

        let plan = survey(&scope);

        assert!(
            !plan.items.iter().any(|item| item.path == home),
            "a claimed home must not be reclaimed: {:?}",
            plan.items
        );
        assert!(plan.skipped_in_use >= 1);
        fs::remove_dir_all(&home).ok();
    }

    #[test]
    fn a_state_dir_with_a_live_daemon_is_left_alone() {
        let temp = tempfile::TempDir::new().unwrap();
        let live = temp.path().join("state/ah/live");
        let dead = temp.path().join("state/ah/dead");
        fs::create_dir_all(&live).unwrap();
        fs::create_dir_all(&dead).unwrap();
        fs::write(dead.join("ahd.sqlite"), vec![b'x'; 4096]).unwrap();
        let mut scope = scope_for(temp.path());
        scope.live_state_dirs = [live.clone()].into_iter().collect();

        let plan = survey(&scope);

        let paths: Vec<_> = plan.items.iter().map(|item| item.path.clone()).collect();
        assert!(paths.contains(&dead));
        assert!(!paths.contains(&live));
    }

    /// A stack that died an hour ago may be under investigation.
    #[test]
    fn recent_leftovers_are_below_the_age_floor() {
        let temp = tempfile::TempDir::new().unwrap();
        make_home(temp.path(), "freshfresh00", 64);
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();
        let mut scope = scope_for(temp.path());
        scope.min_age = Duration::from_secs(7 * 24 * 60 * 60);

        let plan = survey(&scope);

        assert!(plan.is_empty(), "got {:?}", plan.items);
        assert_eq!(plan.skipped_too_recent, 1);
    }

    #[test]
    fn surveying_changes_nothing_on_disk() {
        let temp = tempfile::TempDir::new().unwrap();
        let orphan = make_home(temp.path(), "deadbeef0000", 128);
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();

        let plan = survey(&scope_for(temp.path()));

        assert!(!plan.is_empty());
        assert!(orphan.is_dir(), "a survey must not delete anything");
    }

    #[test]
    fn executing_frees_what_the_survey_reported() {
        let temp = tempfile::TempDir::new().unwrap();
        let orphan = make_home(temp.path(), "deadbeef0000", 4096);
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();
        let plan = survey(&scope_for(temp.path()));
        let promised = plan.bytes();

        let report = execute(&plan, &ExecuteOptions::default());

        assert!(!orphan.exists());
        assert_eq!(report.removed, 1);
        assert_eq!(
            report.bytes_freed, promised,
            "the report must match what the survey promised"
        );
        assert!(report.kept.is_empty());
    }

    /// A home nobody can attribute to a project still holds someone's work.
    /// Deleting it would break the rule this whole decision exists for.
    #[test]
    fn an_orphan_home_holding_records_is_kept_until_a_destination_is_named() {
        let temp = tempfile::TempDir::new().unwrap();
        let orphan = make_home(temp.path(), "deadbeef0000", 16);
        fs::create_dir_all(orphan.join(".codex/sessions")).unwrap();
        fs::write(orphan.join(".codex/sessions/rollout.jsonl"), b"real work").unwrap();
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();
        let plan = survey(&scope_for(temp.path()));

        let report = execute(&plan, &ExecuteOptions::default());

        assert_eq!(report.removed, 0);
        assert!(orphan.is_dir(), "a home holding records must not vanish");
        assert_eq!(report.kept.len(), 1);
        assert!(report.kept[0].1.contains("--archive-to"));
    }

    #[test]
    fn naming_a_destination_archives_the_orphan_and_reclaims_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let orphan = make_home(temp.path(), "deadbeef0000", 16);
        fs::create_dir_all(orphan.join(".codex/sessions")).unwrap();
        fs::write(orphan.join(".codex/sessions/rollout.jsonl"), b"real work").unwrap();
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();
        let plan = survey(&scope_for(temp.path()));
        let destination = temp.path().join("rescued");

        let report = execute(
            &plan,
            &ExecuteOptions {
                archive_unattributable_to: Some(destination.clone()),
            },
        );

        assert_eq!(report.removed, 1);
        assert!(!orphan.exists());
        assert_eq!(
            fs::read_to_string(
                destination
                    .join(".ah/sessions/orphans/deadbeef0000/codex/.codex/sessions/rollout.jsonl")
            )
            .unwrap(),
            "real work"
        );
    }

    /// The project's archive is a project asset; reclaim never sees it.
    #[test]
    fn the_project_session_archive_is_out_of_scope() {
        let temp = tempfile::TempDir::new().unwrap();
        let project_archive = temp.path().join("project/.ah/sessions/s1/a1");
        fs::create_dir_all(&project_archive).unwrap();
        fs::write(project_archive.join("transcript.jsonl"), b"kept").unwrap();
        fs::create_dir_all(temp.path().join("state/ah")).unwrap();
        fs::create_dir_all(temp.path().join("cache/ah/sandboxes")).unwrap();

        let plan = survey(&scope_for(temp.path()));
        execute(&plan, &ExecuteOptions::default());

        assert!(
            project_archive.join("transcript.jsonl").is_file(),
            "reclaim must never touch the project's session archive"
        );
    }

    #[test]
    fn byte_sizes_render_for_humans() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(2048), "2.0 KB");
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.0 MB");
    }
}
