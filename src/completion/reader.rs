use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::completion::parser::{
    LogParseResult, parse_provider_log_line, provider_log_line_has_assistant_progress,
};

pub type LogCursorMap = BTreeMap<PathBuf, u64>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogReadState {
    pub cursors: LogCursorMap,
    pub user_entry_seen_paths: BTreeSet<PathBuf>,
    pub user_prompt_fingerprints: BTreeMap<PathBuf, String>,
}

impl LogReadState {
    pub fn from_cursors(cursors: LogCursorMap) -> Self {
        Self {
            cursors,
            user_entry_seen_paths: BTreeSet::new(),
            user_prompt_fingerprints: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCompletion {
    pub parsed: LogParseResult,
    pub raw_path: PathBuf,
    pub raw_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogActivity {
    pub parsed: LogParseResult,
    pub raw_path: PathBuf,
    pub raw_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogTailReadResult {
    pub activities: Vec<LogActivity>,
    pub completions: Vec<LogCompletion>,
    pub completion_prompt_fingerprints: BTreeMap<(PathBuf, u64), String>,
    pub cursors: LogCursorMap,
    pub state: LogReadState,
}

pub fn collect_provider_log_cursors(provider: &str, log_root: &Path) -> io::Result<LogCursorMap> {
    let mut files = Vec::new();
    collect_provider_log_files(provider, log_root, &mut files)?;
    files.sort();

    let mut cursors = LogCursorMap::new();
    for path in files {
        let len = fs::metadata(&path)?.len();
        cursors.insert(path, len);
    }
    Ok(cursors)
}

pub fn read_provider_log_tail(
    provider: &str,
    log_root: &Path,
    cursors: &LogCursorMap,
) -> io::Result<LogTailReadResult> {
    read_provider_log_tail_with_state(
        provider,
        log_root,
        &LogReadState::from_cursors(cursors.clone()),
    )
}

pub fn read_provider_assistant_progress_after_cursors(
    provider: &str,
    log_root: &Path,
    cursors: &LogCursorMap,
) -> io::Result<bool> {
    let mut files = Vec::new();
    collect_provider_log_files(provider, log_root, &mut files)?;
    files.sort();

    for path in files {
        let bytes = fs::read(&path)?;
        let consumed_offset = cursors.get(&path).copied().unwrap_or(0);
        let mut line_start = parse_start_offset(&bytes, consumed_offset);
        while line_start < bytes.len() {
            let relative_end = bytes[line_start..]
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap_or(bytes.len() - line_start);
            let line_end = line_start + relative_end;
            let next_line_start = if line_end < bytes.len() && bytes[line_end] == b'\n' {
                line_end + 1
            } else {
                line_end
            };
            let line = &bytes[line_start..line_end];
            if !line.is_empty() {
                let line = String::from_utf8_lossy(line);
                if provider_log_line_has_assistant_progress(provider, line.as_ref()) {
                    return Ok(true);
                }
            }
            if next_line_start == line_start {
                break;
            }
            line_start = next_line_start;
        }
    }

    Ok(false)
}

#[cfg(test)]
pub(crate) fn has_pending_tasks_in_transcript(bytes: &[u8]) -> bool {
    crate::provider::adapter("antigravity")
        .is_some_and(|adapter| adapter.transcript_has_pending_work(bytes))
}

pub fn read_provider_log_tail_with_state(
    provider: &str,
    log_root: &Path,
    state: &LogReadState,
) -> io::Result<LogTailReadResult> {
    let mut activities = Vec::new();
    let mut completions = Vec::new();
    let mut completion_prompt_fingerprints = BTreeMap::new();
    let mut updated_state = state.clone();
    let mut files = Vec::new();
    collect_provider_log_files(provider, log_root, &mut files)?;
    files.sort();

    for path in files {
        let bytes = fs::read(&path)?;
        let consumed_offset = state.cursors.get(&path).copied().unwrap_or(0);
        let start = parse_start_offset(&bytes, consumed_offset);
        let requires_user_entry = provider_requires_user_entry(provider);
        if requires_user_entry && consumed_offset > bytes.len() as u64 {
            updated_state.user_entry_seen_paths.remove(&path);
            updated_state.user_prompt_fingerprints.remove(&path);
        }
        let mut seen_user_entry =
            requires_user_entry && updated_state.user_entry_seen_paths.contains(&path);

        let mut line_start = start;
        while line_start < bytes.len() {
            let relative_end = bytes[line_start..]
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap_or(bytes.len() - line_start);
            let line_end = line_start + relative_end;
            let next_line_start = if line_end < bytes.len() && bytes[line_end] == b'\n' {
                line_end + 1
            } else {
                line_end
            };
            let line = &bytes[line_start..line_end];
            if line.is_empty() {
                line_start = next_line_start;
                continue;
            }
            let line = String::from_utf8_lossy(line);
            let parsed = parse_provider_log_line(provider, line.as_ref());
            match parsed {
                LogParseResult::TurnStarted { .. } => {
                    activities.push(LogActivity {
                        parsed,
                        raw_path: path.clone(),
                        raw_offset: next_line_start as u64,
                    });
                }
                LogParseResult::UserMessage {
                    ref prompt_fingerprint,
                    ..
                } if requires_user_entry => {
                    seen_user_entry = true;
                    updated_state.user_entry_seen_paths.insert(path.clone());
                    if let Some(prompt_fingerprint) = prompt_fingerprint {
                        updated_state
                            .user_prompt_fingerprints
                            .insert(path.clone(), prompt_fingerprint.clone());
                    } else {
                        updated_state.user_prompt_fingerprints.remove(&path);
                    }
                    activities.push(LogActivity {
                        parsed,
                        raw_path: path.clone(),
                        raw_offset: next_line_start as u64,
                    });
                }
                LogParseResult::TurnComplete { .. } if !requires_user_entry || seen_user_entry => {
                    let completion_is_deferred = crate::provider::adapter(provider)
                        .is_some_and(|adapter| adapter.completion_is_deferred(&bytes, line_end));
                    if completion_is_deferred {
                        // Defer completion: do not push to completions
                    } else {
                        completions.push(LogCompletion {
                            parsed,
                            raw_path: path.clone(),
                            raw_offset: next_line_start as u64,
                        });
                        if let Some(prompt_fingerprint) =
                            updated_state.user_prompt_fingerprints.get(&path)
                        {
                            completion_prompt_fingerprints.insert(
                                (path.clone(), next_line_start as u64),
                                prompt_fingerprint.clone(),
                            );
                        }
                        if requires_user_entry {
                            seen_user_entry = false;
                            updated_state.user_entry_seen_paths.remove(&path);
                            updated_state.user_prompt_fingerprints.remove(&path);
                        }
                    }
                }
                _ => {}
            }
            if next_line_start == line_start {
                break;
            }
            line_start = next_line_start;
        }

        updated_state.cursors.insert(path, bytes.len() as u64);
    }

    Ok(LogTailReadResult {
        activities,
        completions,
        completion_prompt_fingerprints,
        cursors: updated_state.cursors.clone(),
        state: updated_state,
    })
}

fn parse_start_offset(bytes: &[u8], consumed_offset: u64) -> usize {
    let mut start = consumed_offset.min(bytes.len() as u64) as usize;
    if start > 0 && start < bytes.len() && bytes[start - 1] != b'\n' {
        start = bytes[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| start + position + 1)
            .unwrap_or(bytes.len());
    }
    start
}

fn collect_provider_log_files(
    provider: &str,
    root: &Path,
    files: &mut Vec<PathBuf>,
) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_provider_log_files(provider, &path, files)?;
        } else if file_type.is_file() && provider_log_file_matches(provider, &path) {
            files.push(path);
        }
    }

    Ok(())
}

fn provider_log_file_matches(provider: &str, path: &Path) -> bool {
    crate::provider::adapter(provider).is_some_and(|adapter| adapter.transcript_file_matches(path))
}

fn provider_requires_user_entry(provider: &str) -> bool {
    crate::provider::adapter(provider)
        .is_some_and(|adapter| adapter.transcript_requires_user_entry())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use crate::completion::parser::LogParseResult;

    use super::{
        LogCompletion, LogCursorMap, LogReadState, read_provider_assistant_progress_after_cursors,
        read_provider_log_tail, read_provider_log_tail_with_state,
    };

    fn codex_complete(turn_id: &str, reply: &str) -> String {
        format!(
            r#"{{"type":"event_msg","payload":{{"type":"task_complete","turn_id":"{turn_id}","last_agent_message":"{reply}"}}}}"#
        )
    }

    fn claude_user(content: &str) -> String {
        format!(r#"{{"type":"user","message":{{"role":"user","content":"{content}"}}}}"#)
    }

    fn claude_end_turn(reply: &str) -> String {
        format!(
            r#"{{"type":"assistant","message":{{"type":"message","role":"assistant","content":[{{"type":"text","text":"{reply}"}}],"stop_reason":"end_turn"}}}}"#
        )
    }

    fn claude_assistant_progress(text: &str) -> String {
        format!(
            r#"{{"type":"assistant","message":{{"type":"message","role":"assistant","content":[{{"type":"text","text":"{text}"}}]}}}}"#
        )
    }

    fn write_antigravity_fixture(
        root: &std::path::Path,
        conversation_id: &str,
        fixture: &str,
    ) -> std::path::PathBuf {
        let file = root
            .join("brain")
            .join(conversation_id)
            .join(".system_generated/logs/transcript.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, fixture).unwrap();
        file
    }

    #[test]
    fn cursor_mid_line_drops_partial_first_line() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("2026/06/rollout-session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        let old = codex_complete("old-turn", "OLD");
        let new = codex_complete("new-turn", "NEW");
        fs::write(&file, format!("{old}\n{new}\n")).unwrap();

        let mut cursors = BTreeMap::new();
        cursors.insert(file.clone(), (old.len() / 2) as u64);

        let result = read_provider_log_tail("codex", temp.path(), &cursors).unwrap();

        assert_eq!(
            result.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: Some("new-turn".to_string()),
                    reply: Some("NEW".to_string()),
                },
                raw_path: file.clone(),
                raw_offset: fs::metadata(&file).unwrap().len(),
            }]
        );
        assert_eq!(
            result.cursors.get(&file).copied(),
            Some(fs::metadata(&file).unwrap().len())
        );
    }

    #[test]
    fn dynamic_reglob_reads_file_created_after_dispatch() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("nested/rollout-new.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, format!("{}\n", codex_complete("turn-1", "PONG"))).unwrap();

        let cursors = LogCursorMap::new();

        let result = read_provider_log_tail("codex", temp.path(), &cursors).unwrap();

        assert_eq!(
            result.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: Some("turn-1".to_string()),
                    reply: Some("PONG".to_string()),
                },
                raw_path: file.clone(),
                raw_offset: fs::metadata(&file).unwrap().len(),
            }]
        );
        assert_eq!(
            result.cursors.get(&file).copied(),
            Some(fs::metadata(&file).unwrap().len())
        );
    }

    #[test]
    fn missing_root_can_be_polled_until_the_first_codex_transcript_appears() {
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().join("sessions");

        let before =
            read_provider_log_tail_with_state("codex", &root, &LogReadState::default()).unwrap();
        assert!(before.activities.is_empty());
        assert!(before.completions.is_empty());

        let file = root.join("2026/08/rollout-session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(
            &file,
            concat!(
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-1\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-1\",\"last_agent_message\":\"done\"}}\n",
            ),
        )
        .unwrap();

        let after = read_provider_log_tail_with_state("codex", &root, &before.state).unwrap();
        assert_eq!(after.activities.len(), 1);
        assert_eq!(after.completions.len(), 1);
    }

    #[test]
    fn complete_before_cursor_is_ignored() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("rollout-session.jsonl");
        let old = codex_complete("old-turn", "OLD");
        let new = codex_complete("new-turn", "NEW");
        fs::write(&file, format!("{old}\n{new}\n")).unwrap();

        let mut cursors = LogCursorMap::new();
        cursors.insert(file.clone(), (old.len() + 1) as u64);

        let result = read_provider_log_tail("codex", temp.path(), &cursors).unwrap();

        assert_eq!(
            result.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: Some("new-turn".to_string()),
                    reply: Some("NEW".to_string()),
                },
                raw_path: file,
                raw_offset: fs::metadata(temp.path().join("rollout-session.jsonl"))
                    .unwrap()
                    .len(),
            }]
        );
    }

    #[test]
    fn complete_event_consumed_once_by_path_offset() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("rollout-session.jsonl");
        fs::write(&file, format!("{}\n", codex_complete("turn-1", "PONG"))).unwrap();

        let cursors = LogCursorMap::new();
        let first = read_provider_log_tail("codex", temp.path(), &cursors).unwrap();
        let second = read_provider_log_tail("codex", temp.path(), &first.cursors).unwrap();

        assert_eq!(
            first.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: Some("turn-1".to_string()),
                    reply: Some("PONG".to_string()),
                },
                raw_path: file.clone(),
                raw_offset: fs::metadata(&file).unwrap().len(),
            }]
        );
        assert!(second.completions.is_empty());
        assert_eq!(
            second.cursors.get(&file).copied(),
            Some(fs::metadata(&file).unwrap().len())
        );
    }

    #[test]
    fn claude_end_turn_without_prior_user_entry_after_cursor_is_not_completed() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("project/session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, format!("{}\n", claude_end_turn("STALE"))).unwrap();

        let result = read_provider_log_tail("claude", temp.path(), &LogCursorMap::new()).unwrap();

        assert!(result.completions.is_empty());
        assert_eq!(
            result.cursors.get(&file).copied(),
            Some(fs::metadata(&file).unwrap().len())
        );
    }

    #[test]
    fn claude_user_entry_then_end_turn_after_cursor_completes() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("project/session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(
            &file,
            format!(
                "{}\n{}\n",
                claude_user("echo PONG"),
                claude_end_turn("PONG")
            ),
        )
        .unwrap();

        let result = read_provider_log_tail("claude", temp.path(), &LogCursorMap::new()).unwrap();

        assert_eq!(
            result.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: None,
                    reply: Some("PONG".to_string()),
                },
                raw_path: file.clone(),
                raw_offset: fs::metadata(temp.path().join("project/session.jsonl"))
                    .unwrap()
                    .len(),
            }]
        );
        let completion_offset = fs::metadata(&file).unwrap().len();
        assert_eq!(
            result
                .completion_prompt_fingerprints
                .get(&(file, completion_offset)),
            Some(&crate::runtime_observation::prompt_fingerprint("echo PONG"))
        );
    }

    #[test]
    fn claude_assistant_progress_after_cursor_is_readiness_signal() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("project/session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        let stale = claude_assistant_progress("STALE");
        fs::write(&file, format!("{stale}\n")).unwrap();
        let cursors = super::collect_provider_log_cursors("claude", temp.path()).unwrap();

        assert!(
            !read_provider_assistant_progress_after_cursors("claude", temp.path(), &cursors)
                .unwrap()
        );

        fs::write(
            &file,
            format!("{stale}\n{}\n", claude_assistant_progress("STARTED")),
        )
        .unwrap();
        assert!(
            read_provider_assistant_progress_after_cursors("claude", temp.path(), &cursors)
                .unwrap()
        );
    }

    #[test]
    fn claude_user_entry_then_end_turn_across_separate_reads_completes() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("project/session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, format!("{}\n", claude_user("echo PONG"))).unwrap();

        let first =
            read_provider_log_tail_with_state("claude", temp.path(), &LogReadState::default())
                .unwrap();
        assert!(first.completions.is_empty());
        fs::write(
            &file,
            format!(
                "{}\n{}\n",
                claude_user("echo PONG"),
                claude_end_turn("PONG")
            ),
        )
        .unwrap();

        let second =
            read_provider_log_tail_with_state("claude", temp.path(), &first.state).unwrap();

        assert_eq!(
            second.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: None,
                    reply: Some("PONG".to_string()),
                },
                raw_path: file,
                raw_offset: fs::metadata(temp.path().join("project/session.jsonl"))
                    .unwrap()
                    .len(),
            }]
        );
    }

    #[test]
    fn claude_stale_end_turn_across_ticks_without_user_entry_is_rejected() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("project/session.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, format!("{}\n", claude_end_turn("STALE-1"))).unwrap();

        let first =
            read_provider_log_tail_with_state("claude", temp.path(), &LogReadState::default())
                .unwrap();
        assert!(first.completions.is_empty());
        fs::write(
            &file,
            format!(
                "{}\n{}\n",
                claude_end_turn("STALE-1"),
                claude_end_turn("STALE-2")
            ),
        )
        .unwrap();

        let second =
            read_provider_log_tail_with_state("claude", temp.path(), &first.state).unwrap();

        assert!(second.completions.is_empty());
        assert_eq!(
            second.cursors.get(&file).copied(),
            Some(
                fs::metadata(temp.path().join("project/session.jsonl"))
                    .unwrap()
                    .len()
            )
        );
    }

    #[test]
    fn codex_task_complete_after_cursor_completes_without_user_entry() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("rollout-session.jsonl");
        fs::write(&file, format!("{}\n", codex_complete("turn-1", "PONG"))).unwrap();

        let result = read_provider_log_tail("codex", temp.path(), &LogCursorMap::new()).unwrap();

        assert_eq!(
            result.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: Some("turn-1".to_string()),
                    reply: Some("PONG".to_string()),
                },
                raw_path: file,
                raw_offset: fs::metadata(temp.path().join("rollout-session.jsonl"))
                    .unwrap()
                    .len(),
            }]
        );
    }

    #[test]
    fn antigravity_transcript_after_user_entry_completes() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = write_antigravity_fixture(
            temp.path(),
            "conv-1",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/final_reply.jsonl"
            )),
        );

        let result =
            read_provider_log_tail("antigravity", temp.path(), &LogCursorMap::new()).unwrap();

        assert_eq!(
            result.completions,
            vec![LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: None,
                    reply: Some("The requested summary is complete.".to_string()),
                },
                raw_path: file.clone(),
                raw_offset: fs::metadata(&file).unwrap().len(),
            }]
        );
    }

    #[test]
    fn antigravity_multi_turn_transcript_completes_each_user_turn() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = write_antigravity_fixture(
            temp.path(),
            "conv-1",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/multi_turn.jsonl"
            )),
        );

        let result =
            read_provider_log_tail("antigravity", temp.path(), &LogCursorMap::new()).unwrap();

        assert_eq!(result.completions.len(), 2);
        assert_eq!(
            result.completions[0],
            LogCompletion {
                parsed: LogParseResult::TurnComplete {
                    turn_id: None,
                    reply: Some("First answer".to_string()),
                },
                raw_path: file.clone(),
                raw_offset: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/antigravity_log/multi_turn.jsonl"
                ))
                .lines()
                .take(2)
                .map(|line| line.len() as u64 + 1)
                .sum(),
            }
        );
        assert_eq!(
            result.completions[1].parsed,
            LogParseResult::TurnComplete {
                turn_id: None,
                reply: Some("Second answer".to_string()),
            }
        );
    }

    #[test]
    fn antigravity_tool_call_permission_and_cancel_samples_do_not_complete() {
        let temp = tempfile::TempDir::new().unwrap();
        for (idx, fixture) in [
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/tool_call_in_progress.jsonl"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/tool_failure_no_final.jsonl"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/permission_required_no_final.jsonl"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/cancelled_no_final.jsonl"
            )),
        ]
        .into_iter()
        .enumerate()
        {
            write_antigravity_fixture(temp.path(), &format!("conv-{idx}"), fixture);
        }

        let result =
            read_provider_log_tail("antigravity", temp.path(), &LogCursorMap::new()).unwrap();

        assert!(result.completions.is_empty());
    }

    #[test]
    fn antigravity_final_without_user_entry_after_cursor_is_not_completed() {
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp
            .path()
            .join("brain/conv-1/.system_generated/logs/transcript.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        let stale_final = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/final_reply.jsonl"
        ))
        .lines()
        .nth(1)
        .unwrap();
        fs::write(&file, format!("{stale_final}\n")).unwrap();

        let result =
            read_provider_log_tail("antigravity", temp.path(), &LogCursorMap::new()).unwrap();

        assert!(result.completions.is_empty());
        assert_eq!(
            result.cursors.get(&file).copied(),
            Some(fs::metadata(&file).unwrap().len())
        );
    }

    #[test]
    fn antigravity_pure_narration_with_pending_task_does_not_complete() {
        let temp = tempfile::TempDir::new().unwrap();
        let _file = write_antigravity_fixture(
            temp.path(),
            "conv-1",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/premature_completion_waiting.jsonl"
            )),
        );

        let result =
            read_provider_log_tail("antigravity", temp.path(), &LogCursorMap::new()).unwrap();

        assert!(
            result.completions.is_empty(),
            "Completions should be empty because task is pending"
        );
    }

    #[test]
    fn test_has_pending_tasks_in_transcript_canceled_and_finished() {
        // Start a task
        let start_line = r#"{"step_index":0,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-07-09T08:58:13Z","content":"Created At: 2026-07-09T08:58:13Z\nCompleted At: 2026-07-09T08:58:13Z\nTask: teststate-0000/task-201\nStatus: RUNNING\nLog: /home/testuser/.cache/ah/sandboxes/testhost/.gemini/antigravity-cli/brain/teststate-0000/.system_generated/tasks_task-201.log"}"#;

        // Scenario 1: Task finished (regression assertion)
        let finished_line = r#"{"step_index":1,"source":"SYSTEM","type":"EVENT_MSG","status":"DONE","created_at":"2026-07-09T08:58:14Z","content":"Task id \"teststate-0000/task-201\" finished"}"#;
        let finished_transcript = format!("{}\n{}", start_line, finished_line);
        assert!(
            !super::has_pending_tasks_in_transcript(finished_transcript.as_bytes()),
            "Finished task should not be pending"
        );

        // Scenario 2: Task canceled (with single 'l' and was)
        let canceled_line = r#"{"step_index":1,"source":"SYSTEM","type":"EVENT_MSG","status":"DONE","created_at":"2026-07-09T08:58:14Z","content":"Task id \"teststate-0000/task-201\" was canceled with reason: User requested cancellation"}"#;
        let canceled_transcript = format!("{}\n{}", start_line, canceled_line);
        assert!(
            !super::has_pending_tasks_in_transcript(canceled_transcript.as_bytes()),
            "Canceled task should not be pending"
        );

        // Scenario 3: Task cancelled (British double L variant, without was)
        let cancelled_line1 = r#"{"step_index":1,"source":"SYSTEM","type":"EVENT_MSG","status":"DONE","created_at":"2026-07-09T08:58:14Z","content":"Task id \"teststate-0000/task-201\" cancelled"}"#;
        let cancelled_transcript1 = format!("{}\n{}", start_line, cancelled_line1);
        assert!(
            !super::has_pending_tasks_in_transcript(cancelled_transcript1.as_bytes()),
            "Cancelled (double L) task should not be pending"
        );

        // Scenario 4: Task cancelled (British double L variant, with was)
        let cancelled_line2 = r#"{"step_index":1,"source":"SYSTEM","type":"EVENT_MSG","status":"DONE","created_at":"2026-07-09T08:58:14Z","content":"Task id \"teststate-0000/task-201\" was cancelled"}"#;
        let cancelled_transcript2 = format!("{}\n{}", start_line, cancelled_line2);
        assert!(
            !super::has_pending_tasks_in_transcript(cancelled_transcript2.as_bytes()),
            "Was cancelled (double L) task should not be pending"
        );
    }

    #[test]
    fn test_has_pending_tasks_in_transcript_fixtures_regression() {
        let finished_str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/finished.jsonl"
        ));
        let canceled_str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/canceled.jsonl"
        ));

        assert!(
            !super::has_pending_tasks_in_transcript(finished_str.as_bytes()),
            "Finished fixture should not be pending"
        );
        assert!(
            !super::has_pending_tasks_in_transcript(canceled_str.as_bytes()),
            "Canceled fixture should not be pending"
        );
    }
}
