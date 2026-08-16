use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionTerminality {
    Terminal,
    DeferredBackgroundWork { reason: String },
}

pub fn check_pending_tasks_from_log_root(
    state_dir: &std::path::Path,
    session_id: &str,
    agent_id: &str,
    provider: &str,
) -> Option<bool> {
    let log_root = match crate::completion::log_layout::resolve_agent_log_root(
        state_dir, session_id, agent_id, provider, false,
    ) {
        crate::completion::log_layout::LogRootResolution::Available(root) => root,
        _ => return None,
    };

    let cursors = match crate::completion::reader::collect_provider_log_cursors(provider, &log_root)
    {
        Ok(cursors) => cursors,
        _ => return None,
    };

    let mut checked_any = false;
    for path in cursors.keys() {
        if let Ok(bytes) = std::fs::read(path) {
            checked_any = true;
            if crate::provider::adapter(provider)
                .is_some_and(|adapter| adapter.transcript_has_pending_work(&bytes))
            {
                return Some(true);
            }
        }
    }

    if checked_any { Some(false) } else { None }
}

pub fn classify_terminality(
    provider: &str,
    candidate_reply: &str,
    _pane_snapshot: Option<&str>,
    _prompt_text: Option<&str>,
    has_pending_tasks: Option<bool>,
) -> CompletionTerminality {
    crate::provider::adapter(provider)
        .map(|adapter| adapter.classify_terminality(candidate_reply, has_pending_tasks))
        .unwrap_or(CompletionTerminality::Terminal)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogParseResult {
    TurnStarted {
        turn_id: Option<String>,
    },
    TurnComplete {
        turn_id: Option<String>,
        reply: Option<String>,
    },
    UserMessage {
        turn_id: Option<String>,
        prompt_fingerprint: Option<String>,
    },
    NotTerminal,
    UnknownDegrade {
        reason: String,
    },
}

pub fn parse_provider_log_line(provider: &str, line: &str) -> LogParseResult {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return LogParseResult::NotTerminal;
    };

    crate::provider::adapter(provider)
        .map(|adapter| adapter.parse_transcript_value(&value))
        .unwrap_or(LogParseResult::NotTerminal)
}

pub fn provider_log_line_has_assistant_progress(provider: &str, line: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return false;
    };

    crate::provider::adapter(provider)
        .is_some_and(|adapter| adapter.transcript_has_assistant_progress(&value))
}

#[cfg(test)]
mod tests {
    use super::{
        LogParseResult, parse_provider_log_line, provider_log_line_has_assistant_progress,
    };

    #[test]
    fn codex_task_complete_emits_turn_complete() {
        let line = r#"{"timestamp":"2026-06-02T02:03:43.476Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","last_agent_message":"PONG"}}"#;

        let parsed = parse_provider_log_line("codex", line);

        assert_eq!(
            parsed,
            LogParseResult::TurnComplete {
                turn_id: Some("turn-1".to_string()),
                reply: Some("PONG".to_string()),
            }
        );
    }

    #[test]
    fn codex_task_started_emits_turn_started() {
        let line = r#"{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}"#;

        let parsed = parse_provider_log_line("codex", line);

        assert_eq!(
            parsed,
            LogParseResult::TurnStarted {
                turn_id: Some("turn-1".to_string()),
            }
        );
    }

    #[test]
    fn claude_user_message_carries_prompt_fingerprint() {
        let line = r#"{"type":"user","promptId":"prompt-1","message":{"role":"user","content":"echo PONG\n"}}"#;

        let parsed = parse_provider_log_line("claude", line);

        assert_eq!(
            parsed,
            LogParseResult::UserMessage {
                turn_id: Some("prompt-1".to_string()),
                prompt_fingerprint: Some(crate::runtime_observation::prompt_fingerprint(
                    "echo PONG\n"
                )),
            }
        );
    }

    #[test]
    fn codex_agent_message_without_task_complete_is_not_terminal() {
        let line = r#"{"type":"event_msg","payload":{"type":"agent_message","message":"PONG","phase":"final_answer"}}"#;

        let parsed = parse_provider_log_line("codex", line);

        assert_eq!(parsed, LogParseResult::NotTerminal);
    }

    #[test]
    fn claude_end_turn_emits_turn_complete() {
        let line = r#"{"type":"assistant","message":{"model":"claude-opus-4-8","type":"message","role":"assistant","content":[{"type":"text","text":"PONG"}],"stop_reason":"end_turn"}}"#;

        let parsed = parse_provider_log_line("claude", line);

        assert_eq!(
            parsed,
            LogParseResult::TurnComplete {
                turn_id: None,
                reply: Some("PONG".to_string()),
            }
        );
    }

    #[test]
    fn claude_assistant_progress_does_not_require_end_turn() {
        let progress = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"working"}]}}"#;
        let user = r#"{"type":"user","message":{"role":"user","content":"继续"}}"#;
        let pane_echo = "继续";

        assert!(provider_log_line_has_assistant_progress("claude", progress));
        assert!(!provider_log_line_has_assistant_progress("claude", user));
        assert!(!provider_log_line_has_assistant_progress(
            "claude", pane_echo
        ));
    }

    #[test]
    fn claude_thinking_endturn_line_then_text_line() {
        let thinking_line = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"thinking","thinking":"Working through it"}],"stop_reason":"end_turn"}}"#;
        let text_line = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"ANSWER"}],"stop_reason":"end_turn"}}"#;

        let thinking_parsed = parse_provider_log_line("claude", thinking_line);
        let text_parsed = parse_provider_log_line("claude", text_line);

        assert_eq!(thinking_parsed, LogParseResult::NotTerminal);
        assert_eq!(
            text_parsed,
            LogParseResult::TurnComplete {
                turn_id: None,
                reply: Some("ANSWER".to_string()),
            }
        );
    }

    #[test]
    fn claude_text_only_endturn_still_completes() {
        let line = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"X"}],"stop_reason":"end_turn"}}"#;

        let parsed = parse_provider_log_line("claude", line);

        assert_eq!(
            parsed,
            LogParseResult::TurnComplete {
                turn_id: None,
                reply: Some("X".to_string()),
            }
        );
    }

    #[test]
    fn claude_tool_use_is_busy_not_terminal() {
        let line = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"echo PONG"}}],"stop_reason":"tool_use"}}"#;

        let parsed = parse_provider_log_line("claude", line);

        assert_eq!(parsed, LogParseResult::NotTerminal);
    }

    #[test]
    fn claude_unknown_stop_reason_warns_and_falls_back() {
        let line = r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"PONG"}],"stop_reason":"paused_for_unknown_reason"}}"#;

        let parsed = parse_provider_log_line("claude", line);

        assert_eq!(
            parsed,
            LogParseResult::UnknownDegrade {
                reason: "claude_unknown_stop_reason".to_string(),
            }
        );
    }

    #[test]
    fn antigravity_user_input_emits_user_message_baseline() {
        let line = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/final_reply.jsonl"
        ))
        .lines()
        .next()
        .unwrap();

        let parsed = parse_provider_log_line("antigravity", line);

        assert_eq!(
            parsed,
            LogParseResult::UserMessage {
                turn_id: None,
                prompt_fingerprint: Some(crate::runtime_observation::prompt_fingerprint(
                    "Summarize the current state"
                )),
            }
        );
    }

    #[test]
    fn antigravity_user_request_wrapper_excludes_runtime_metadata_from_fingerprint() {
        let line = r#"{"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"<USER_REQUEST>\nReply with exactly: antigravity-ok\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nruntime-only\n</ADDITIONAL_METADATA>"}"#;

        let parsed = parse_provider_log_line("antigravity", line);

        assert_eq!(
            parsed,
            LogParseResult::UserMessage {
                turn_id: None,
                prompt_fingerprint: Some(crate::runtime_observation::prompt_fingerprint(
                    "Reply with exactly: antigravity-ok\n"
                )),
            }
        );
    }

    #[test]
    fn antigravity_final_planner_response_emits_turn_complete() {
        let line = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/final_reply.jsonl"
        ))
        .lines()
        .nth(1)
        .unwrap();

        let parsed = parse_provider_log_line("antigravity", line);

        assert_eq!(
            parsed,
            LogParseResult::TurnComplete {
                turn_id: None,
                reply: Some("The requested summary is complete.".to_string()),
            }
        );
    }

    #[test]
    fn antigravity_planner_response_with_tool_call_is_not_terminal() {
        let line = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/tool_call_in_progress.jsonl"
        ))
        .lines()
        .nth(1)
        .unwrap();

        let parsed = parse_provider_log_line("antigravity", line);

        assert_eq!(parsed, LogParseResult::NotTerminal);
    }

    #[test]
    fn antigravity_cancelled_planner_response_is_not_terminal() {
        let line = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/antigravity_log/cancelled_no_final.jsonl"
        ))
        .lines()
        .nth(1)
        .unwrap();

        let parsed = parse_provider_log_line("antigravity", line);

        assert_eq!(parsed, LogParseResult::NotTerminal);
    }

    #[test]
    fn bad_json_line_is_not_terminal_without_panic() {
        let parsed = parse_provider_log_line("codex", "{not json");

        assert_eq!(parsed, LogParseResult::NotTerminal);
    }

    #[test]
    fn missing_payload_or_message_fields_do_not_panic() {
        assert_eq!(
            parse_provider_log_line("codex", r#"{"type":"event_msg"}"#),
            LogParseResult::NotTerminal
        );
        assert_eq!(
            parse_provider_log_line("claude", r#"{"type":"assistant"}"#),
            LogParseResult::NotTerminal
        );
    }

    #[test]
    fn test_classify_terminality_antigravity_deferred() {
        use super::{CompletionTerminality, classify_terminality};

        let cases = [
            "等后台 cargo test 跑完，我再看最终结果。",
            "waiting for background cargo test",
            "still running in the background",
            "I'll wait for cargo test",
            "will report once it finishes",
            "后台仍在跑",
            "跑完后我再汇报",
            "完成后我再报告",
        ];

        for case in cases {
            assert_eq!(
                classify_terminality("antigravity", case, None, None, None),
                CompletionTerminality::DeferredBackgroundWork {
                    reason: "ANTIGRAVITY_DEFERRED_BACKGROUND_WORK".to_string()
                },
                "Expected '{}' to be deferred",
                case
            );
        }
    }

    #[test]
    fn test_classify_terminality_antigravity_terminal() {
        use super::{CompletionTerminality, classify_terminality};

        let cases = [
            "test result: ok. 5 passed",
            "The requested summary is complete.",
            "I have updated the file.",
        ];

        for case in cases {
            assert_eq!(
                classify_terminality("antigravity", case, None, None, None),
                CompletionTerminality::Terminal,
                "Expected '{}' to be terminal",
                case
            );
        }
    }

    #[test]
    fn test_classify_terminality_other_providers_unchanged() {
        use super::{CompletionTerminality, classify_terminality};

        let case = "等后台 cargo test 跑完，我再看最终结果。";
        assert_eq!(
            classify_terminality("claude", case, None, None, None),
            CompletionTerminality::Terminal
        );
        assert_eq!(
            classify_terminality("codex", case, None, None, None),
            CompletionTerminality::Terminal
        );
    }

    #[test]
    fn test_classify_terminality_heuristic_acceptance() {
        use super::{CompletionTerminality, classify_terminality};

        // Criteria 1: Contains '5 passed' AND deferred phrase -> must be DeferredBackgroundWork
        let case1 = "Done. 5 passed. But I am still running background cargo.";
        assert_eq!(
            classify_terminality("antigravity", case1, None, None, None),
            CompletionTerminality::DeferredBackgroundWork {
                reason: "ANTIGRAVITY_DEFERRED_BACKGROUND_WORK".to_string()
            },
            "Expected '5 passed' with still running to be deferred"
        );

        // Criteria 2: Contains '5 passed' but NO deferred phrase -> must be Terminal
        let case2 = "Done. Test result: 5 passed; 0 failed.";
        assert_eq!(
            classify_terminality("antigravity", case2, None, None, None),
            CompletionTerminality::Terminal,
            "Expected pure '5 passed' without deferred phrase to be terminal"
        );
    }

    #[test]
    fn test_classify_terminality_authoritative_signal() {
        use super::{
            CompletionTerminality, check_pending_tasks_from_log_root, classify_terminality,
        };
        use std::fs;

        let temp = tempfile::TempDir::new().unwrap();
        let state_dir = temp.path();
        let session_id = "test-session";
        let agent_id = "test-agent";

        // Setup the real directory structure
        let sandbox_dir = state_dir.join("sandboxes").join(session_id).join(agent_id);
        fs::create_dir_all(&sandbox_dir).unwrap();

        let home_root =
            crate::home_materialization::sandbox_home_for_sandbox_dir(&sandbox_dir).unwrap();
        let log_dir =
            home_root.join(".gemini/antigravity-cli/brain/conv-uuid-1234/.system_generated/logs");
        fs::create_dir_all(&log_dir).unwrap();
        let transcript_path = log_dir.join("transcript.jsonl");

        // The reply text must be neutral prose (no waiting/completion words)
        let neutral_reply = "The current operation is active.";

        // --- Phase 1: Test with pending tasks ---
        let pending_fixture = concat!(
            r#"{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"start"}"#,
            "\n",
            r#"{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","tool_calls":[{"name":"run_command","args":{"CommandLine":"cargo test"}}]}"#,
            "\n",
            r#"{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","content":"Task: test-agent/task-404\nStatus: RUNNING\nLog: /path/to/log"}"#,
            "\n"
        );
        fs::write(&transcript_path, pending_fixture).unwrap();

        let has_pending =
            check_pending_tasks_from_log_root(state_dir, session_id, agent_id, "antigravity");
        assert_eq!(
            classify_terminality("antigravity", neutral_reply, None, None, has_pending),
            CompletionTerminality::DeferredBackgroundWork {
                reason: "ANTIGRAVITY_DEFERRED_BACKGROUND_WORK".to_string()
            },
            "Expected neutral text with pending task in transcript to be deferred"
        );

        // --- Phase 2: Test with all tasks closed (finished) ---
        let finished_fixture = concat!(
            r#"{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"start"}"#,
            "\n",
            r#"{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","tool_calls":[{"name":"run_command","args":{"CommandLine":"cargo test"}}]}"#,
            "\n",
            r#"{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","content":"Task: test-agent/task-404\nStatus: RUNNING\nLog: /path/to/log"}"#,
            "\n",
            r#"{"step_index":3,"source":"SYSTEM","type":"EVENT_MSG","status":"DONE","content":"Task id \"test-agent/task-404\" finished with result:\nSuccess"}"#,
            "\n"
        );
        fs::write(&transcript_path, finished_fixture).unwrap();

        let has_pending_closed =
            check_pending_tasks_from_log_root(state_dir, session_id, agent_id, "antigravity");
        let term_closed =
            classify_terminality("antigravity", neutral_reply, None, None, has_pending_closed);
        assert_eq!(
            term_closed,
            CompletionTerminality::Terminal,
            "Expected neutral text with completed tasks in transcript to be terminal"
        );
    }
}
