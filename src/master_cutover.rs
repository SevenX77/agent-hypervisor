use std::path::{Path, PathBuf};
use std::{fs, io};

use crate::error::AhError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffBundleInput<'a> {
    pub cutover_id: &'a str,
    pub session_id: &'a str,
    pub socket_path: &'a Path,
    pub state_dir: &'a Path,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationSeedResult {
    Seeded {
        target_dir: PathBuf,
        copied_files: Vec<PathBuf>,
    },
    Fallback {
        handoff_path: PathBuf,
        reason: String,
        first_prompt: String,
    },
}

pub fn claude_project_dir_key_for_cwd(cwd: &Path) -> String {
    cwd.canonicalize()
        .unwrap_or_else(|_| cwd.to_path_buf())
        .display()
        .to_string()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

pub fn claude_project_conversation_dir(home: &Path, cwd: &Path) -> PathBuf {
    home.join(".claude")
        .join("projects")
        .join(claude_project_dir_key_for_cwd(cwd))
}

pub fn write_handoff_bundle(
    state_dir: &Path,
    input: &HandoffBundleInput<'_>,
) -> Result<PathBuf, AhError> {
    let handoff_dir = state_dir.join("cutovers").join(input.cutover_id);
    fs::create_dir_all(&handoff_dir)
        .map_err(|err| file_err("create handoff dir", &handoff_dir, err))?;
    let handoff_path = handoff_dir.join("handoff.md");
    let body = format!(
        "\
# ah master cutover handoff

current_target: ah-managed Master PM
cutover_id: {cutover_id}
session_id: {session_id}
socket_path: {socket_path}
state_dir: {state_dir}
attach_command: ah attach master --session {session_id}

constraints:
- Read AH_MASTER_HANDOFF, confirm you have accepted this PM handoff, then immediately run: ah master ack-ready --cutover-id \"$AH_CUTOVER_ID\"
- Use ah ask/ps/logs/pend/cancel/kill for dispatch and inspection.
- Do not use ah after accepting this cutover unless rollback is explicitly requested.
- If master death reaped workers, inspect ah ps/logs and re-dispatch missing work.

re-dispatch policy:
- Use a new request_id shaped cutover-{cutover_id}-retry-<n>-<agent>.
- Confirm missing/failed/killed work before submitting replacement ah ask jobs.

inflight_tasks:
- request_id: unknown
  agent_id: unknown
  prompt_summary: unknown at cutover handoff generation
  status_at_cutover: unknown
  redispatch_policy: if missing/failed/killed after revive, submit a new ah ask
",
        cutover_id = input.cutover_id,
        session_id = input.session_id,
        socket_path = input.socket_path.display(),
        state_dir = input.state_dir.display(),
    );
    fs::write(&handoff_path, body)
        .map_err(|err| file_err("write handoff bundle", &handoff_path, err))?;
    Ok(handoff_path)
}

pub fn seed_claude_project_conversation(
    old_home: &Path,
    master_home: &Path,
    cwd: &Path,
    handoff_path: &Path,
) -> Result<ConversationSeedResult, AhError> {
    let source_dir = claude_project_conversation_dir(old_home, cwd);
    if !source_dir.is_dir() {
        let reason = format!(
            "dash-escaped Claude conversation store not found: {}",
            source_dir.display()
        );
        tracing::warn!(reason = %reason, "master cutover conversation seed fallback");
        return Ok(fallback_result(handoff_path, reason));
    }

    let target_dir = claude_project_conversation_dir(master_home, cwd);
    let mut copied_files = Vec::new();
    if let Err(err) = copy_conversation_store(&source_dir, &target_dir, &mut copied_files) {
        let reason = format!(
            "failed to copy dash-escaped Claude conversation store {} -> {}: {err}",
            source_dir.display(),
            target_dir.display()
        );
        tracing::warn!(reason = %reason, "master cutover conversation seed fallback");
        return Ok(fallback_result(handoff_path, reason));
    }

    Ok(ConversationSeedResult::Seeded {
        target_dir,
        copied_files,
    })
}

fn copy_conversation_store(
    source_dir: &Path,
    target_dir: &Path,
    copied_files: &mut Vec<PathBuf>,
) -> io::Result<()> {
    fs::create_dir_all(target_dir)?;
    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target_dir.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_conversation_store(&source_path, &target_path, copied_files)?;
        } else if file_type.is_file() {
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &target_path)?;
            copied_files.push(target_path);
        }
    }
    Ok(())
}

fn fallback_result(handoff_path: &Path, reason: String) -> ConversationSeedResult {
    ConversationSeedResult::Fallback {
        handoff_path: handoff_path.to_path_buf(),
        reason,
        first_prompt: format!(
            "Read AH_MASTER_HANDOFF={} before taking over; no conversation seed was available. After accepting the handoff, run: ah master ack-ready --cutover-id \"$AH_CUTOVER_ID\"",
            handoff_path.display()
        ),
    }
}

fn file_err(action: &str, path: &Path, err: io::Error) -> AhError {
    AhError::EnvironmentNotSupported {
        details: format!("{action} {}: {err}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConversationSeedResult, HandoffBundleInput, claude_project_conversation_dir,
        claude_project_dir_key_for_cwd, seed_claude_project_conversation, write_handoff_bundle,
    };

    #[test]
    fn cutover_writes_handoff_bundle_to_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        let socket_path = tmp.path().join("ah.sock");
        let handoff_path = write_handoff_bundle(
            &state_dir,
            &HandoffBundleInput {
                cutover_id: "cutover_123",
                session_id: "session_abc",
                socket_path: &socket_path,
                state_dir: &state_dir,
            },
        )
        .unwrap();

        assert_eq!(
            handoff_path,
            state_dir
                .join("cutovers")
                .join("cutover_123")
                .join("handoff.md")
        );
        let body = std::fs::read_to_string(&handoff_path).unwrap();
        assert!(body.contains("cutover_123"));
        assert!(body.contains("session_abc"));
        assert!(body.contains(socket_path.to_string_lossy().as_ref()));
        assert!(body.contains("re-dispatch policy"));
        assert!(body.contains("inflight_tasks:"));
    }

    #[test]
    fn cutover_seeds_claude_project_conversation_into_dash_escaped_project_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let old_home = tmp.path().join("old-home");
        let master_home = tmp.path().join("master-home");
        let cwd = std::path::Path::new("/home/sevenx/coding/agent-hypervisor");
        let source_dir = claude_project_conversation_dir(&old_home, cwd);
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(
            source_dir.join("conversation.jsonl"),
            b"{\"type\":\"msg\"}\n",
        )
        .unwrap();

        let handoff_path = tmp.path().join("handoff.md");
        std::fs::write(&handoff_path, "handoff").unwrap();
        let result =
            seed_claude_project_conversation(&old_home, &master_home, cwd, &handoff_path).unwrap();

        let expected_dir = master_home
            .join(".claude")
            .join("projects")
            .join("-home-sevenx-coding-agent-hypervisor");
        assert_eq!(
            result,
            ConversationSeedResult::Seeded {
                target_dir: expected_dir.clone(),
                copied_files: vec![expected_dir.join("conversation.jsonl")],
            }
        );
        assert_eq!(
            std::fs::read_to_string(expected_dir.join("conversation.jsonl")).unwrap(),
            "{\"type\":\"msg\"}\n"
        );
        assert!(
            !master_home
                .join(".claude/projects/home/sevenx/coding/agent-hypervisor")
                .exists()
        );
    }

    #[test]
    fn cutover_seed_target_matches_claude_continue_lookup_key() {
        let cwd = std::path::Path::new("/var/tmp/work spaces/project.one_x");
        let master_home = std::path::Path::new("/tmp/master-home");
        assert_eq!(
            claude_project_dir_key_for_cwd(cwd),
            "-var-tmp-work-spaces-project-one-x"
        );
        assert_eq!(
            claude_project_conversation_dir(master_home, cwd),
            master_home
                .join(".claude")
                .join("projects")
                .join("-var-tmp-work-spaces-project-one-x")
        );
    }

    #[test]
    fn cutover_falls_back_to_handoff_bundle_when_conversation_store_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let old_home = tmp.path().join("old-home");
        let master_home = tmp.path().join("master-home");
        let cwd = std::path::Path::new("/home/sevenx/coding/agent-hypervisor");
        let raw_wrong_dir = old_home.join(".claude/projects/home/sevenx/coding/agent-hypervisor");
        std::fs::create_dir_all(&raw_wrong_dir).unwrap();
        std::fs::write(raw_wrong_dir.join("conversation.jsonl"), b"wrong").unwrap();
        let handoff_path = tmp.path().join("handoff.md");
        std::fs::write(&handoff_path, "handoff").unwrap();

        let result =
            seed_claude_project_conversation(&old_home, &master_home, cwd, &handoff_path).unwrap();

        match result {
            ConversationSeedResult::Fallback {
                handoff_path: actual_handoff,
                reason,
                first_prompt,
            } => {
                assert_eq!(actual_handoff, handoff_path);
                assert!(reason.contains("dash-escaped"));
                assert!(first_prompt.contains("AH_MASTER_HANDOFF"));
                assert!(first_prompt.contains("no conversation seed"));
            }
            ConversationSeedResult::Seeded { .. } => {
                panic!("missing dash-escaped store must fallback")
            }
        }
        assert!(
            !master_home
                .join(".claude/projects/-home-sevenx-coding-agent-hypervisor")
                .exists()
        );
    }
}
