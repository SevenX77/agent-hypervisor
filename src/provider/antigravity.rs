use super::{
    AuthStoreSpec, HookStatusUse, ObservationSourceSpec, ProviderAdapter, ProviderAuthSpec,
    ProviderAuthStoreSpec, ProviderHookSpec, ProviderLoginDriverSpec, ProviderObservationSpec,
    ProviderTerminalControlSpec,
};
use crate::completion::parser::{CompletionTerminality, LogParseResult};
use crate::provider::manifest::{
    ANTIGRAVITY_INJECTED_ENV, CompletionSignalKind, ENV_PASSTHROUGH, IdleDetectionMode,
    InitProbeKind, ProviderCapabilities, ProviderManifest,
};
use crate::runtime_observation::prompt_fingerprint;
use regex::Regex;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

pub struct AntigravityAdapter;
pub static ANTIGRAVITY: AntigravityAdapter = AntigravityAdapter;

const AUTH_STORES: &[ProviderAuthStoreSpec] = &[
    ProviderAuthStoreSpec {
        os: "linux",
        store: AuthStoreSpec::File {
            relative_path: ".gemini/antigravity-cli/antigravity-oauth-token",
        },
    },
    ProviderAuthStoreSpec {
        os: "macos",
        store: AuthStoreSpec::File {
            relative_path: ".gemini/antigravity-cli/antigravity-oauth-token",
        },
    },
];

static AUTH_SPEC: ProviderAuthSpec = ProviderAuthSpec {
    assessed_cli: "agy 1.1.11",
    documentation_url: Some("https://antigravity.google/docs/cli-install"),
    stores: AUTH_STORES,
    login: ProviderLoginDriverSpec::StartupTui {
        local_argv: &["agy"],
        headless_argv: &[
            "env",
            "SSH_CONNECTION=127.0.0.1:50000:127.0.0.1:22",
            "BROWSER=/bin/false",
            "agy",
        ],
        select_prompt_markers: &[
            "You are currently not signed in",
            "Select login method:",
            "1. Google OAuth",
        ],
        challenge_markers: &["Open the URL below in your browser:"],
        code_prompt_markers: &[
            "After authenticating, copy the code displayed in the browser",
            "authorization code...",
        ],
        failure_markers: &["Got an error: token exchange failed:"],
        authorization_url_required_query_keys: &[
            "client_id",
            "code_challenge",
            "code_challenge_method",
            "redirect_uri",
            "response_type",
            "scope",
            "state",
        ],
    },
    status_argv: None,
    config_dir_env: None,
};

static TERMINAL_CONTROL_SPEC: ProviderTerminalControlSpec = ProviderTerminalControlSpec {
    composer_tail_lines: 40,
    composer_start_markers: &["›", ">"],
    paste_expand_guards: &[],
    collapsed_paste_markers: &["pasted text", "pasted content"],
};

const HOOKS_DOC: &str = "https://www.antigravity.google/docs/hooks";
const HOOKS: &[ProviderHookSpec] = &[
    ProviderHookSpec::available(
        "PreToolUse",
        "before each tool call",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PostToolUse",
        "after each tool call",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::materialized(
        "PreInvocation",
        "before every model invocation; may repeat within one user turn",
        HookStatusUse::WorkingCandidate,
        &["conversationId", "invocationNum"],
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PostInvocation",
        "after every model invocation; may repeat within one user turn",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::materialized(
        "Stop",
        "when execution stops; completion is valid only when fullyIdle is true",
        HookStatusUse::CompletionWhenIdleCandidate,
        &["conversationId", "terminationReason", "error", "fullyIdle"],
        HOOKS_DOC,
    ),
];
const WORKING_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::Transcript("USER_EXPLICIT/USER_INPUT"),
    ObservationSourceSpec::OfficialHook("PreInvocation"),
];
const COMPLETION_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::Transcript("MODEL/PLANNER_RESPONSE/DONE without pending tasks"),
    ObservationSourceSpec::OfficialHook("Stop"),
];

static OBSERVATION_SPEC: ProviderObservationSpec = ProviderObservationSpec {
    assessed_cli: "agy 1.1.11 / interactive tmux / version-scoped embedded contract",
    hooks: HOOKS,
    hook_config_path: Some("$HOME/.gemini/config/hooks.json; enableJsonHooks=true"),
    hook_config_schema: Some(
        "top-level named hooks; Stop/PreInvocation/PostInvocation use direct handler arrays; tool events use matcher groups",
    ),
    hooks_enabled_by_default: false,
    working_sources: WORKING_SOURCES,
    completion_sources: COMPLETION_SOURCES,
    real_hook_delivery_verified: true,
    real_hook_delivery_evidence: Some(
        "tests/mvp9_real_codex_claude.rs::test_launcher_hook_push_delivers_native_events_real; WSL2; 2026-08-10",
    ),
};

impl ProviderAdapter for AntigravityAdapter {
    fn name(&self) -> &'static str {
        "antigravity"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["gemini"]
    }

    fn manifest(&self) -> ProviderManifest {
        ProviderManifest {
            provider_name: self.name(),
            auth_mount_paths: vec![".gemini/antigravity-cli"],
            // Antigravity has no premise-matched unattended permission policy
            // wired here yet. Keep its native permission boundary; operations
            // that require approval must block instead of bypassing it.
            command: &["agy"],
            resume_args: &[],
            env_passthrough: ENV_PASSTHROUGH,
            injected_env_vars: ANTIGRAVITY_INJECTED_ENV,
            readiness_timeout_s: 60,
            requires_home_materialization: true,
            init_probe: InitProbeKind::Antigravity,
            idle_detection_mode: IdleDetectionMode::LineEndRegex,
            stability_ms: 300,
            idle_anti_pattern: r"(?m)^\s*esc to cancel\b",
            completion_signal: CompletionSignalKind::LogOnly,
            capabilities: ProviderCapabilities {
                rules_target: true,
                completion_signal: true,
                readiness_ack: true,
                bundles: false,
                settings: false,
            },
        }
    }

    fn auth_spec(&self) -> &'static ProviderAuthSpec {
        &AUTH_SPEC
    }

    fn observation_spec(&self) -> &'static ProviderObservationSpec {
        &OBSERVATION_SPEC
    }

    fn terminal_control_spec(&self) -> &'static ProviderTerminalControlSpec {
        &TERMINAL_CONTROL_SPEC
    }

    fn recovery_args(&self, sandbox_home: &Path) -> Vec<String> {
        match latest_conversation(sandbox_home) {
            Some(path) => match path.file_stem().and_then(|stem| stem.to_str()) {
                Some(conversation_id) if !conversation_id.is_empty() => {
                    vec!["--conversation".to_string(), conversation_id.to_string()]
                }
                _ => {
                    tracing::warn!(
                        ?path,
                        "antigravity recovery falling back to --continue: invalid conversation file"
                    );
                    vec!["--continue".to_string()]
                }
            },
            None => {
                tracing::warn!(
                    ?sandbox_home,
                    "antigravity recovery falling back to --continue: no conversation file found"
                );
                vec!["--continue".to_string()]
            }
        }
    }

    fn recovery_supported(&self) -> bool {
        true
    }

    fn cancel_keysyms(&self) -> &'static [&'static str] {
        &["Escape"]
    }

    fn transcript_root(&self, home_root: &Path) -> Option<PathBuf> {
        Some(home_root.join(".gemini/antigravity-cli"))
    }

    fn rules_target(&self, home_root: &Path) -> Option<PathBuf> {
        Some(home_root.join(".gemini/AGENTS.md"))
    }

    fn transcript_file_matches(&self, path: &Path) -> bool {
        let components = path
            .components()
            .filter_map(|component| component.as_os_str().to_str())
            .collect::<Vec<_>>();
        let len = components.len();
        len >= 5
            && components[len - 5] == "brain"
            && components[len - 3] == ".system_generated"
            && components[len - 2] == "logs"
            && components[len - 1] == "transcript.jsonl"
    }

    fn transcript_requires_user_entry(&self) -> bool {
        true
    }

    fn parse_transcript_value(&self, value: &Value) -> LogParseResult {
        if value.get("source").and_then(Value::as_str) == Some("USER_EXPLICIT")
            && value.get("type").and_then(Value::as_str) == Some("USER_INPUT")
        {
            return LogParseResult::UserMessage {
                turn_id: None,
                prompt_fingerprint: value
                    .get("content")
                    .and_then(Value::as_str)
                    .map(user_request)
                    .map(prompt_fingerprint),
            };
        }

        if value.get("source").and_then(Value::as_str) != Some("MODEL")
            || value.get("type").and_then(Value::as_str) != Some("PLANNER_RESPONSE")
            || value.get("status").and_then(Value::as_str) != Some("DONE")
        {
            return LogParseResult::NotTerminal;
        }

        if value
            .get("tool_calls")
            .and_then(Value::as_array)
            .is_some_and(|tool_calls| !tool_calls.is_empty())
        {
            return LogParseResult::NotTerminal;
        }

        LogParseResult::TurnComplete {
            turn_id: None,
            reply: value
                .get("content")
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    }

    fn completion_is_deferred(&self, transcript: &[u8], line_end: usize) -> bool {
        has_pending_tasks(&transcript[..line_end])
    }

    fn transcript_has_pending_work(&self, transcript: &[u8]) -> bool {
        has_pending_tasks(transcript)
    }

    fn classify_terminality(
        &self,
        candidate_reply: &str,
        has_pending_tasks: Option<bool>,
    ) -> CompletionTerminality {
        if let Some(pending) = has_pending_tasks {
            return if pending {
                deferred()
            } else {
                CompletionTerminality::Terminal
            };
        }

        let reply_lower = candidate_reply.to_lowercase();
        for phrase in [
            "waiting for",
            "still running",
            "running in the background",
            "background cargo",
            "i'll wait",
            "will report",
            "i'll update",
            "once it finishes",
        ] {
            if reply_lower.contains(phrase) {
                return deferred();
            }
        }

        for phrase in [
            "等待",
            "等后台",
            "还在跑",
            "仍在运行",
            "仍在跑",
            "跑完后",
            "稍后汇报",
            "完成后我再报告",
        ] {
            if candidate_reply.contains(phrase) {
                return deferred();
            }
        }

        let background_run = Regex::new(r"后台.*跑").expect("static regex");
        let complete_report = Regex::new(r"完成后.*报告").expect("static regex");
        if background_run.is_match(candidate_reply) || complete_report.is_match(candidate_reply) {
            return deferred();
        }

        CompletionTerminality::Terminal
    }
}

fn deferred() -> CompletionTerminality {
    CompletionTerminality::DeferredBackgroundWork {
        reason: "ANTIGRAVITY_DEFERRED_BACKGROUND_WORK".to_string(),
    }
}

fn user_request(content: &str) -> &str {
    const OPEN: &str = "<USER_REQUEST>";
    const CLOSE: &str = "</USER_REQUEST>";
    let Some(after_open) = content.strip_prefix(OPEN) else {
        return content;
    };
    let after_open = after_open.trim_start_matches(['\r', '\n']);
    let Some(close_offset) = after_open.find(CLOSE) else {
        return content;
    };
    &after_open[..close_offset]
}

fn has_pending_tasks(bytes: &[u8]) -> bool {
    let task_id = Regex::new(r"[a-zA-Z0-9\-]+/task-\d+").expect("static regex");
    let task_finished =
        Regex::new(r#"Task id "([^"]+)" (?:was )?(?:finished|cancell?ed)"#).expect("static regex");
    let mut started_tasks = HashSet::new();
    let mut finished_tasks = HashSet::new();

    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        let Some(content) = value.get("content").and_then(Value::as_str) else {
            continue;
        };
        if content.contains("Status: RUNNING") || content.contains("running as a background task") {
            for capture in task_id.captures_iter(content) {
                started_tasks.insert(capture[0].to_string());
            }
        }
        for capture in task_finished.captures_iter(content) {
            finished_tasks.insert(capture[1].to_string());
        }
    }

    started_tasks.difference(&finished_tasks).next().is_some()
}

fn latest_conversation(sandbox_home: &Path) -> Option<PathBuf> {
    let conversations_root = sandbox_home.join(".gemini/antigravity-cli/conversations");
    let mut conversations = Vec::new();
    collect_conversations(&conversations_root, &mut conversations);
    conversations.sort_by(|left, right| {
        let left_mtime = left
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok();
        let right_mtime = right
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok();
        left_mtime.cmp(&right_mtime).then_with(|| left.cmp(right))
    });
    conversations.pop()
}

fn collect_conversations(dir: &Path, conversations: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(?dir, error = %err, "failed to scan antigravity conversations directory");
            }
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
            continue;
        };
        if matches!(extension, "db" | "pb") {
            conversations.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::has_pending_tasks;

    #[test]
    fn pending_task_detection_requires_an_unclosed_task_identity() {
        let pending = br#"{"content":"Task: a/task-1\nStatus: RUNNING"}"#;
        assert!(has_pending_tasks(pending));

        let closed = br#"{"content":"Task: a/task-1\nStatus: RUNNING"}
{"content":"Task id \"a/task-1\" finished with result:\nSuccess"}
"#;
        assert!(!has_pending_tasks(closed));
    }
}
