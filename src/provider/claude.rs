use super::{
    AuthChallengeKind, AuthStoreSpec, HookStatusUse, ObservationSourceSpec, ProviderAdapter,
    ProviderAuthSpec, ProviderAuthStoreSpec, ProviderHookSpec, ProviderLoginDriverSpec,
    ProviderObservationSpec, ProviderTerminalControlSpec,
};
use crate::completion::parser::LogParseResult;
use crate::provider::manifest::{
    CLAUDE_INJECTED_ENV, CompletionSignalKind, ENV_PASSTHROUGH, IdleDetectionMode, InitProbeKind,
    ProviderCapabilities, ProviderManifest,
};
use crate::runtime_observation::prompt_fingerprint;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub struct ClaudeAdapter;
pub static CLAUDE: ClaudeAdapter = ClaudeAdapter;

const AUTH_STORES: &[ProviderAuthStoreSpec] = &[
    ProviderAuthStoreSpec {
        os: "linux",
        store: AuthStoreSpec::File {
            relative_path: ".claude/.credentials.json",
        },
    },
    ProviderAuthStoreSpec {
        os: "macos",
        store: AuthStoreSpec::OsCredentialService {
            probe_hint: "claude auth status --json",
        },
    },
];

static AUTH_SPEC: ProviderAuthSpec = ProviderAuthSpec {
    assessed_cli: "claude-code 2.1.223",
    documentation_url: Some("https://code.claude.com/docs/en/authentication"),
    stores: AUTH_STORES,
    login: ProviderLoginDriverSpec::Command {
        local_argv: &["claude", "auth", "login"],
        headless_argv: &["claude", "auth", "login"],
        challenge: AuthChallengeKind::AuthorizationCodePaste,
        challenge_markers: &["If the browser didn't open, visit:"],
        code_prompt_markers: &["Paste code here if prompted >"],
        failure_markers: &["Login failed:"],
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
    status_argv: Some(&["claude", "auth", "status", "--json"]),
    config_dir_env: Some("CLAUDE_CONFIG_DIR"),
};

static TERMINAL_CONTROL_SPEC: ProviderTerminalControlSpec = ProviderTerminalControlSpec {
    composer_tail_lines: 40,
    composer_start_markers: &["❯", ">"],
    paste_expand_guards: &["paste again to expand"],
    collapsed_paste_markers: &["pasted text", "pasted content"],
};

const HOOKS_DOC: &str = "https://code.claude.com/docs/en/hooks";
const HOOKS: &[ProviderHookSpec] = &[
    ProviderHookSpec::available(
        "SessionStart",
        "when a session starts or resumes",
        HookStatusUse::LifecycleCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "Setup",
        "during repository setup",
        HookStatusUse::NotUsed,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "InstructionsLoaded",
        "after instructions are loaded",
        HookStatusUse::LifecycleCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::materialized(
        "UserPromptSubmit",
        "when the user submits a prompt, before Claude processes it",
        HookStatusUse::WorkingCandidate,
        &["session_id", "prompt"],
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "UserPromptExpansion",
        "when a user slash command expands",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "MessageDisplay",
        "when Claude Code displays a message",
        HookStatusUse::NotUsed,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PreToolUse",
        "before each tool call",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PermissionRequest",
        "when Claude requests tool permission",
        HookStatusUse::ApprovalCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PermissionDenied",
        "after a permission request is denied",
        HookStatusUse::ApprovalCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PostToolUse",
        "after each successful tool call",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PostToolUseFailure",
        "after a failed tool call",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PostToolBatch",
        "after a batch of tool calls",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "Notification",
        "when Claude Code emits a notification",
        HookStatusUse::NotUsed,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SubagentStart",
        "when a subagent starts",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SubagentStop",
        "when a subagent finishes",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "TaskCreated",
        "when Claude creates an internal task",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "TaskCompleted",
        "when Claude completes an internal task",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::materialized(
        "Stop",
        "when the main Claude agent finishes responding",
        HookStatusUse::CompletionCandidate,
        &["session_id", "last_assistant_message"],
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "StopFailure",
        "when a response stops because of an API error",
        HookStatusUse::CompletionCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "TeammateIdle",
        "when a teammate is about to become idle",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "ConfigChange",
        "when Claude Code configuration changes",
        HookStatusUse::NotUsed,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "CwdChanged",
        "when the working directory changes",
        HookStatusUse::NotUsed,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "DirectoryAdded",
        "when a directory is added to the session",
        HookStatusUse::NotUsed,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "FileChanged",
        "when Claude Code observes a file change",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "WorktreeCreate",
        "when Claude Code creates a worktree",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "WorktreeRemove",
        "when Claude Code removes a worktree",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PreCompact",
        "before context compaction",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PostCompact",
        "after context compaction",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SessionEnd",
        "when a Claude Code session ends",
        HookStatusUse::LifecycleCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "Elicitation",
        "when an MCP server requests user input",
        HookStatusUse::ApprovalCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "ElicitationResult",
        "after an MCP elicitation resolves",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
];
const WORKING_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::OfficialHook("UserPromptSubmit"),
    ObservationSourceSpec::Transcript("user prompt + assistant progress"),
];
const COMPLETION_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::OfficialHook("Stop"),
    ObservationSourceSpec::Transcript("assistant stop_reason"),
];

static OBSERVATION_SPEC: ProviderObservationSpec = ProviderObservationSpec {
    assessed_cli: "Claude Code 2.1.223 / interactive tmux",
    hooks: HOOKS,
    hook_config_path: Some("$CLAUDE_CONFIG_DIR/settings.json"),
    hook_config_schema: Some("hooks.<Event>[] matcher groups containing hooks[] command handlers"),
    hooks_enabled_by_default: false,
    working_sources: WORKING_SOURCES,
    completion_sources: COMPLETION_SOURCES,
    real_hook_delivery_verified: true,
    real_hook_delivery_evidence: Some(
        "tests/mvp9_real_codex_claude.rs::test_launcher_hook_push_delivers_native_events_real; WSL2; 2026-08-10",
    ),
};

impl ProviderAdapter for ClaudeAdapter {
    fn name(&self) -> &'static str {
        "claude"
    }

    fn manifest(&self) -> ProviderManifest {
        ProviderManifest {
            provider_name: self.name(),
            auth_mount_paths: vec![".anthropic", ".claude"],
            // `dontAsk` executes only operations admitted by the materialized
            // allow rules and rejects every other permission request without
            // turning a provider prompt into an authority path.
            command: &["claude", "--permission-mode", "dontAsk"],
            resume_args: &["--continue"],
            env_passthrough: ENV_PASSTHROUGH,
            injected_env_vars: CLAUDE_INJECTED_ENV,
            readiness_timeout_s: 60,
            requires_home_materialization: true,
            init_probe: InitProbeKind::Claude,
            idle_detection_mode: IdleDetectionMode::ObservedStability,
            stability_ms: 300,
            idle_anti_pattern: r"(?im)\b(?:esc to interrupt|Architecting|Reading\s+\d+\s+files?|ctrl\+o to expand|paste again to expand)\b",
            completion_signal: CompletionSignalKind::LogOnly,
            capabilities: ProviderCapabilities {
                rules_target: true,
                completion_signal: true,
                readiness_ack: true,
                bundles: true,
                settings: true,
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

    fn recovery_args(&self, _sandbox_home: &Path) -> Vec<String> {
        vec!["--continue".to_string()]
    }

    fn recovery_supported(&self) -> bool {
        true
    }

    fn transcript_root(&self, home_root: &Path) -> Option<PathBuf> {
        Some(home_root.join(".claude/projects"))
    }

    fn rules_target(&self, home_root: &Path) -> Option<PathBuf> {
        Some(home_root.join(".claude/CLAUDE.md"))
    }

    fn transcript_file_matches(&self, path: &Path) -> bool {
        path.extension().and_then(|extension| extension.to_str()) == Some("jsonl")
    }

    fn transcript_requires_user_entry(&self) -> bool {
        true
    }

    fn parse_transcript_value(&self, value: &Value) -> LogParseResult {
        if is_user_entry(value) {
            return LogParseResult::UserMessage {
                turn_id: value
                    .get("promptId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                prompt_fingerprint: user_prompt(value).as_deref().map(prompt_fingerprint),
            };
        }

        if value.get("type").and_then(Value::as_str) != Some("assistant") {
            return LogParseResult::NotTerminal;
        }
        let Some(message) = value.get("message") else {
            return LogParseResult::NotTerminal;
        };
        if message.get("type").and_then(Value::as_str) != Some("message")
            || message.get("role").and_then(Value::as_str) != Some("assistant")
        {
            return LogParseResult::NotTerminal;
        }

        match message.get("stop_reason").and_then(Value::as_str) {
            Some("end_turn" | "stop_sequence" | "max_tokens") => {
                let Some(reply) = text_reply(message) else {
                    return LogParseResult::NotTerminal;
                };
                LogParseResult::TurnComplete {
                    turn_id: None,
                    reply: Some(reply),
                }
            }
            Some("tool_use") => LogParseResult::NotTerminal,
            Some(stop_reason) => {
                tracing::warn!(stop_reason, "unknown Claude stop_reason in completion log");
                LogParseResult::UnknownDegrade {
                    reason: "claude_unknown_stop_reason".to_string(),
                }
            }
            None => {
                tracing::warn!("missing Claude stop_reason in completion log");
                LogParseResult::UnknownDegrade {
                    reason: "claude_missing_stop_reason".to_string(),
                }
            }
        }
    }

    fn transcript_has_assistant_progress(&self, value: &Value) -> bool {
        if value.get("type").and_then(Value::as_str) != Some("assistant") {
            return false;
        }
        let Some(message) = value.get("message") else {
            return false;
        };
        if message.get("type").and_then(Value::as_str) != Some("message")
            || message.get("role").and_then(Value::as_str) != Some("assistant")
        {
            return false;
        }
        message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| {
                content.iter().any(|item| {
                    matches!(
                        item.get("type").and_then(Value::as_str),
                        Some("text" | "tool_use" | "thinking")
                    )
                })
            })
    }
}

fn user_prompt(value: &Value) -> Option<String> {
    let content = value.get("message")?.get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    let text = content
        .as_array()?
        .iter()
        .filter_map(|item| {
            (item.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| item.get("text").and_then(Value::as_str))
                .flatten()
        })
        .collect::<Vec<_>>()
        .join("");
    (!text.is_empty()).then_some(text)
}

fn is_user_entry(value: &Value) -> bool {
    if value.get("type").and_then(Value::as_str) == Some("user") {
        return true;
    }
    value
        .get("message")
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str)
        == Some("user")
}

fn text_reply(message: &Value) -> Option<String> {
    let content = message.get("content")?.as_array()?;
    let text_parts = content
        .iter()
        .filter_map(|item| {
            if item.get("type").and_then(Value::as_str) == Some("text") {
                item.get("text").and_then(Value::as_str)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if text_parts.is_empty() {
        None
    } else {
        Some(text_parts.join(""))
    }
}
