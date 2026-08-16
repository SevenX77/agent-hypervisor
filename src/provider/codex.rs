use super::{
    AuthChallengeKind, AuthStoreSpec, HookStatusUse, ObservationSourceSpec, ProviderAdapter,
    ProviderAuthSpec, ProviderAuthStoreSpec, ProviderHookSpec, ProviderLoginDriverSpec,
    ProviderObservationSpec, ProviderTerminalControlSpec,
};
use crate::completion::parser::LogParseResult;
use crate::provider::manifest::{
    CODEX_INJECTED_ENV, CompletionSignalKind, ENV_PASSTHROUGH, IdleDetectionMode, InitProbeKind,
    ProviderCapabilities, ProviderManifest,
};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

pub struct CodexAdapter;
pub static CODEX: CodexAdapter = CodexAdapter;

const AUTH_STORES: &[ProviderAuthStoreSpec] = &[
    ProviderAuthStoreSpec {
        os: "linux",
        store: AuthStoreSpec::File {
            relative_path: ".codex/auth.json",
        },
    },
    ProviderAuthStoreSpec {
        os: "macos",
        store: AuthStoreSpec::File {
            relative_path: ".codex/auth.json",
        },
    },
];

static AUTH_SPEC: ProviderAuthSpec = ProviderAuthSpec {
    assessed_cli: "codex-cli 0.142.5",
    documentation_url: Some("https://developers.openai.com/codex/auth/"),
    stores: AUTH_STORES,
    login: ProviderLoginDriverSpec::Command {
        local_argv: &["codex", "login"],
        headless_argv: &["codex", "login", "--device-auth"],
        challenge: AuthChallengeKind::DeviceCode {
            code_pattern: r"\b[A-Z0-9]{4}-[A-Z0-9]{5}\b",
        },
        challenge_markers: &[
            "Follow these steps to sign in with ChatGPT using device code authorization:",
            "Open this link in your browser",
        ],
        code_prompt_markers: &["Enter this one-time code"],
        failure_markers: &[],
        authorization_url_required_query_keys: &[],
    },
    status_argv: Some(&["codex", "login", "status"]),
    config_dir_env: None,
};

static TERMINAL_CONTROL_SPEC: ProviderTerminalControlSpec = ProviderTerminalControlSpec {
    composer_tail_lines: 40,
    composer_start_markers: &["›"],
    paste_expand_guards: &["paste again to expand"],
    collapsed_paste_markers: &["pasted text", "pasted content"],
};

const HOOKS_DOC: &str = "https://developers.openai.com/codex/hooks";
const HOOKS: &[ProviderHookSpec] = &[
    ProviderHookSpec::available(
        "PreToolUse",
        "before each tool call",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "PermissionRequest",
        "when Codex requests tool permission",
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
    ProviderHookSpec::materialized(
        "UserPromptSubmit",
        "after prompt submission and before Codex processes the turn",
        HookStatusUse::WorkingCandidate,
        &["turn_id", "prompt"],
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SubagentStop",
        "when a subagent finishes",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::materialized(
        "Stop",
        "when the main agent finishes responding",
        HookStatusUse::CompletionCandidate,
        &["turn_id", "last_assistant_message"],
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SessionStart",
        "on startup, resume, clear, or compact session lifecycle entry",
        HookStatusUse::LifecycleCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SubagentStart",
        "when a subagent starts",
        HookStatusUse::ActivityCandidate,
        HOOKS_DOC,
    ),
    ProviderHookSpec::available(
        "SessionEnd",
        "when a Codex session ends",
        HookStatusUse::LifecycleCandidate,
        HOOKS_DOC,
    ),
];
const WORKING_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::Transcript("event_msg.task_started"),
    ObservationSourceSpec::OfficialHook("UserPromptSubmit"),
];
const COMPLETION_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::Transcript("event_msg.task_complete"),
    ObservationSourceSpec::OfficialHook("Stop"),
];

static OBSERVATION_SPEC: ProviderObservationSpec = ProviderObservationSpec {
    assessed_cli: "codex-cli 0.142.5 / interactive tmux",
    hooks: HOOKS,
    hook_config_path: Some("$CODEX_HOME/hooks.json; [features].hooks=true in config.toml"),
    hook_config_schema: Some("hooks.<Event>[] matcher groups containing hooks[] command handlers"),
    hooks_enabled_by_default: false,
    working_sources: WORKING_SOURCES,
    completion_sources: COMPLETION_SOURCES,
    real_hook_delivery_verified: true,
    real_hook_delivery_evidence: Some(
        "tests/mvp9_real_codex_claude.rs::test_launcher_hook_push_delivers_native_events_real; WSL2; 2026-08-10",
    ),
};

impl ProviderAdapter for CodexAdapter {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn manifest(&self) -> ProviderManifest {
        ProviderManifest {
            provider_name: self.name(),
            auth_mount_paths: vec![".codex", ".config/gcloud"],
            command: &[
                "codex",
                "-c",
                "disable_paste_burst=true",
                "-c",
                "trust_level=\"trusted\"",
                "-c",
                "approval_policy=\"never\"",
                "-c",
                "sandbox_mode=\"workspace-write\"",
            ],
            resume_args: &[],
            env_passthrough: ENV_PASSTHROUGH,
            injected_env_vars: CODEX_INJECTED_ENV,
            readiness_timeout_s: 60,
            requires_home_materialization: true,
            init_probe: InitProbeKind::Codex,
            idle_detection_mode: IdleDetectionMode::ObservedStability,
            stability_ms: 300,
            idle_anti_pattern: r"(?im)\besc to interrupt\b|Hooks need review|Trust all and continue|Continue without trusting",
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
        match latest_rollout(sandbox_home) {
            Some(path) => match session_id_from_rollout(&path) {
                Some(session_id) => vec!["resume".to_string(), session_id],
                None => {
                    tracing::warn!(
                        ?path,
                        "codex recovery falling back to --last: invalid rollout metadata"
                    );
                    vec!["resume".to_string(), "--last".to_string()]
                }
            },
            None => {
                tracing::warn!(
                    ?sandbox_home,
                    "codex recovery falling back to --last: no rollout metadata found"
                );
                vec!["resume".to_string(), "--last".to_string()]
            }
        }
    }

    fn recovery_supported(&self) -> bool {
        true
    }

    fn transcript_root(&self, home_root: &Path) -> Option<PathBuf> {
        Some(home_root.join(".codex/sessions"))
    }

    fn rules_target(&self, home_root: &Path) -> Option<PathBuf> {
        Some(home_root.join(".codex/AGENTS.md"))
    }

    fn transcript_file_matches(&self, path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
    }

    fn parse_transcript_value(&self, value: &Value) -> LogParseResult {
        if value.get("type").and_then(Value::as_str) != Some("event_msg") {
            return LogParseResult::NotTerminal;
        }
        let Some(payload) = value.get("payload") else {
            return LogParseResult::NotTerminal;
        };
        if payload.get("type").and_then(Value::as_str) == Some("task_started") {
            return LogParseResult::TurnStarted {
                turn_id: payload
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            };
        }
        if payload.get("type").and_then(Value::as_str) != Some("task_complete") {
            if payload.get("type").and_then(Value::as_str) == Some("agent_message")
                && payload.get("phase").and_then(Value::as_str) == Some("final_answer")
            {
                tracing::debug!(
                    payload_type = "agent_message",
                    phase = "final_answer",
                    "ignored terminal-looking codex log line without task_complete"
                );
            }
            return LogParseResult::NotTerminal;
        }

        LogParseResult::TurnComplete {
            turn_id: payload
                .get("turn_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            reply: payload
                .get("last_agent_message")
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    }
}

fn latest_rollout(sandbox_home: &Path) -> Option<PathBuf> {
    let sessions_root = sandbox_home.join(".codex/sessions");
    let mut rollouts = Vec::new();
    collect_rollouts(&sessions_root, &mut rollouts);
    rollouts.sort_by(|left, right| {
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
    rollouts.pop()
}

fn collect_rollouts(dir: &Path, rollouts: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(?dir, error = %err, "failed to scan codex sessions directory");
            }
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rollouts(&path, rollouts);
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.starts_with("rollout-") && file_name.ends_with(".jsonl") {
            rollouts.push(path);
        }
    }
}

fn session_id_from_rollout(path: &Path) -> Option<String> {
    let file = fs::File::open(path)
        .map_err(|err| {
            tracing::warn!(?path, error = %err, "failed to open codex rollout metadata");
            err
        })
        .ok()?;
    let mut first_line = String::new();
    BufReader::new(file)
        .read_line(&mut first_line)
        .map_err(|err| {
            tracing::warn!(?path, error = %err, "failed to read codex rollout metadata");
            err
        })
        .ok()?;
    let value: Value = serde_json::from_str(first_line.trim())
        .map_err(|err| {
            tracing::warn!(?path, error = %err, "failed to parse codex rollout metadata");
            err
        })
        .ok()?;
    if value.get("type").and_then(Value::as_str) != Some("session_meta") {
        return None;
    }
    let id = value
        .get("payload")
        .and_then(|payload| payload.get("id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    uuid::Uuid::parse_str(id).ok()?;
    Some(id.to_string())
}
