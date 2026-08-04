use crate::error::CcbdError;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

#[derive(Debug, Clone)]
pub struct ProviderManifest {
    pub provider_name: &'static str,
    pub auth_mount_paths: Vec<&'static str>,
    /// Actual command and arguments used to spawn this provider.
    pub command: &'static [&'static str],
    /// Arguments appended only when recovering a crashed worker for this provider.
    pub resume_args: &'static [&'static str],
    /// Host environment variable names allowed into the sandbox.
    pub env_passthrough: &'static [&'static str],
    /// Environment variables injected by ccbd. These override passthrough.
    pub injected_env_vars: &'static [(&'static str, &'static str)],
    pub readiness_timeout_s: u32,
    pub requires_home_materialization: bool,
    pub init_probe: InitProbeKind,
    pub idle_detection_mode: IdleDetectionMode,
    pub stability_ms: u64,
    pub idle_anti_pattern: &'static str,
    pub completion_signal: CompletionSignalKind,
    /// Optional features this provider declares support for. Orchestration gates
    /// behaviour on these declarations instead of on provider names, so adding a
    /// provider means declaring what it supports rather than editing branches.
    pub capabilities: ProviderCapabilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionSignalKind {
    LogOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// ah can materialize a role rules document into this provider's home.
    pub rules_target: bool,
    /// The provider can push an end-of-turn completion signal back to ahd.
    pub completion_signal: bool,
    /// A master seat on this provider reports readiness explicitly: it runs
    /// `ah master ack-ready` during cutover, and its transcript records
    /// assistant progress that the revive path can read.
    pub readiness_ack: bool,
    /// `bundle` references can be materialized for this provider.
    pub bundles: bool,
    /// A `settings` table can be materialized for this provider.
    pub settings: bool,
}

impl ProviderCapabilities {
    const NONE: Self = Self {
        rules_target: false,
        completion_signal: false,
        readiness_ack: false,
        bundles: false,
        settings: false,
    };

    /// Capabilities the master role requires from whichever provider runs it.
    /// A master without rules never learns the role contract, one without a
    /// completion signal never reports end of turn, and one without readiness
    /// ack cannot be handed over by cutover and can only be revived to the
    /// weaker "process started" claim.
    pub fn missing_for_master(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.rules_target {
            missing.push("rules_target");
        }
        if !self.completion_signal {
            missing.push("completion_signal");
        }
        if !self.readiness_ack {
            missing.push("readiness_ack");
        }
        missing
    }
}

/// Capabilities declared by `provider`, or `None` when the name is unknown.
pub fn provider_capabilities(provider: &str) -> Option<ProviderCapabilities> {
    MANIFESTS
        .get(canonicalize_provider_name(provider))
        .map(|manifest| manifest.capabilities)
}

pub fn is_recovery_eligible_provider(provider: &str) -> bool {
    let provider = canonicalize_provider_name(provider);
    matches!(provider, "codex" | "claude" | "antigravity")
}

pub fn compute_recovery_args(provider: &str, sandbox_home: &Path) -> Vec<String> {
    match canonicalize_provider_name(provider) {
        "claude" => vec!["--continue".to_string()],
        "antigravity" => antigravity_recovery_args(sandbox_home),
        "codex" => codex_recovery_args(sandbox_home),
        _ => Vec::new(),
    }
}

fn codex_recovery_args(sandbox_home: &Path) -> Vec<String> {
    match latest_codex_rollout(sandbox_home) {
        Some(path) => match codex_session_id_from_rollout(&path) {
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

fn antigravity_recovery_args(sandbox_home: &Path) -> Vec<String> {
    match latest_antigravity_conversation(sandbox_home) {
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

fn latest_antigravity_conversation(sandbox_home: &Path) -> Option<PathBuf> {
    let conversations_root = sandbox_home.join(".gemini/antigravity-cli/conversations");
    let mut conversations = Vec::new();
    collect_antigravity_conversations(&conversations_root, &mut conversations);
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

fn collect_antigravity_conversations(dir: &Path, conversations: &mut Vec<PathBuf>) {
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
        // The extension check intentionally excludes SQLite sidecars like *.db-wal and *.db-shm.
        if matches!(extension, "db" | "pb") {
            conversations.push(path);
        }
    }
}

fn latest_codex_rollout(sandbox_home: &Path) -> Option<PathBuf> {
    let sessions_root = sandbox_home.join(".codex/sessions");
    let mut rollouts = Vec::new();
    collect_codex_rollouts(&sessions_root, &mut rollouts);
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

fn collect_codex_rollouts(dir: &Path, rollouts: &mut Vec<PathBuf>) {
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
            collect_codex_rollouts(&path, rollouts);
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

fn codex_session_id_from_rollout(path: &Path) -> Option<String> {
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
    let value: serde_json::Value = serde_json::from_str(first_line.trim())
        .map_err(|err| {
            tracing::warn!(?path, error = %err, "failed to parse codex rollout metadata");
            err
        })
        .ok()?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("session_meta") {
        return None;
    }
    let id = value
        .get("payload")
        .and_then(|payload| payload.get("id"))
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())?;
    uuid::Uuid::parse_str(id).ok()?;
    Some(id.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleDetectionMode {
    LineEndRegex,
    ObservedStability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitProbeKind {
    Bash,
    Codex,
    Claude,
    Antigravity,
    OpenCode,
    Unknown,
}

impl InitProbeKind {
    pub fn build(self) -> Box<dyn crate::provider::init_probe::InitGateProbe> {
        match self {
            Self::Bash => Box::new(crate::provider::init_probe::BashInitProbe),
            Self::Codex => Box::new(crate::provider::init_probe::CodexInitProbe),
            Self::Claude => Box::new(crate::provider::init_probe::ClaudeInitProbe),
            Self::Antigravity => Box::new(crate::provider::init_probe::AntigravityInitProbe),
            Self::OpenCode | Self::Unknown => Box::new(crate::provider::init_probe::BashInitProbe),
        }
    }
}

pub const ENV_PASSTHROUGH: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "CCB_BACKEND_ENV",
    "CCB_CCBD_MIN_POLL_INTERVAL_S",
    "CCB_CLAUDE_READY_TIMEOUT_S",
    "CCB_DEBUG",
    "CCB_GEMINI_READY_TIMEOUT_S",
    "CCB_KEEPER_PID",
    "CCB_KEEPER_PING_TIMEOUT_S",
    "CCB_LANG",
    "CCB_MASTER_CLAUDE_PID",
    "CCB_PER_AGENT_SUBCGROUP",
    "CCB_NO_ATTACH",
    "CCB_REPLY_LANG",
    "CCB_JOB_ID",
    "CCB_SOCKET",
    "CCB_STDIN_ENCODING",
    "CCB_TMUX_ENTER_DELAY",
    "CCB_TMUX_SECOND_ENTER_DELAY",
    "CCB_TMUX_SOCKET",
    "CCB_TMUX_SOCKET_PATH",
    "CCB_VERIFY_DELIVERY",
    "CCB_VERIFY_POST_DELAY_MS",
    "CCB_VERIFY_RETRY_KEYCODES",
    "CCB_VERSION",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "GEMINI_API_KEY",
    "GOOGLE_API_BASE",
    "GOOGLE_API_KEY",
    "GOOGLE_GENAI_USE_VERTEXAI",
    "HOME",
    // Proxy settings decide whether a sandboxed agent can reach its provider at
    // all. Dropping them leaves the agent offline on any machine that routes
    // through a proxy, where a provider CLI reports it as "not signed in"
    // rather than as a network failure. Both cases are honoured because
    // different runtimes read different ones.
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "NO_PROXY",
    "no_proxy",
    "LANG",
    "LC_ALL",
    "LC_MESSAGES",
    "LOCALAPPDATA",
    "OPENAI_API_BASE",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_ORG_ID",
    "OPENAI_ORGANIZATION",
    "PATH",
    "PYTHONPATH",
    "PYTHONUNBUFFERED",
    "SHELL",
    "SYSTEMROOT",
    "TERM",
    "TMP",
    "TEMP",
    "TMPDIR",
    "USER",
    "USERPROFILE",
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
];

pub const CLAUDE_INJECTED_ENV: &[(&str, &str)] = &[
    ("CCB_CLAUDE_SKILLS", "true"),
    ("CCB_CLAUDE_READY_TIMEOUT_S", "60.0"),
    ("CCB_CLAUDE_MD_MODE", "route"),
    ("CCB_REPLY_LANG", "zh"),
    ("CCB_LANG", "zh"),
    ("CCB_CTX_TRANSFER_LAST_N", "20"),
    ("CCB_CTX_TRANSFER_ENABLED", "true"),
];

pub const CODEX_INJECTED_ENV: &[(&str, &str)] = &[
    ("CCB_TMUX_ENTER_DELAY", "0.5"),
    ("CCB_TMUX_SECOND_ENTER_DELAY", "0.0"),
];

pub const ANTIGRAVITY_INJECTED_ENV: &[(&str, &str)] = &[("CCB_GEMINI_READY_TIMEOUT_S", "60.0")];

// Reserved for future provider wiring; no opencode manifest is added in G11.1.
pub const OPENCODE_INJECTED_ENV: &[(&str, &str)] = &[("CCB_SESSION_ID", "<session_id>")];
pub const PANE_LOG_INJECTED_ENV: &[(&str, &str)] = &[
    ("CCB_PANE_LOG_POLL_INTERVAL", "2.0"),
    ("CCB_SYNC_TIMEOUT", "3600"),
];

const BASH_INJECTED_ENV: &[(&str, &str)] = &[("PS1", "$ ")];
pub const VALID_PROVIDER_NAMES: &[&str] = &["bash", "codex", "claude", "antigravity"];

pub fn canonicalize_provider_name(raw: &str) -> &str {
    match raw {
        "gemini" => "antigravity",
        _ => raw,
    }
}

pub static MANIFESTS: LazyLock<HashMap<&'static str, ProviderManifest>> = LazyLock::new(|| {
    let mut manifests = HashMap::new();
    manifests.insert(
        "bash",
        ProviderManifest {
            provider_name: "bash",
            auth_mount_paths: vec![],
            command: &["bash", "--noprofile", "--norc", "-i"],
            resume_args: &[],
            env_passthrough: ENV_PASSTHROUGH,
            injected_env_vars: BASH_INJECTED_ENV,
            readiness_timeout_s: 10,
            requires_home_materialization: false,
            init_probe: InitProbeKind::Bash,
            idle_detection_mode: IdleDetectionMode::LineEndRegex,
            stability_ms: 0,
            idle_anti_pattern: "",
            completion_signal: CompletionSignalKind::LogOnly,
            capabilities: ProviderCapabilities::NONE,
        },
    );
    manifests.insert(
        "codex",
        ProviderManifest {
            provider_name: "codex",
            auth_mount_paths: vec![".codex", ".config/gcloud"],
            command: &[
                "codex",
                "--dangerously-bypass-approvals-and-sandbox",
                "--dangerously-bypass-hook-trust",
                "-c",
                "disable_paste_burst=true",
                "-c",
                "trust_level=\"trusted\"",
                "-c",
                "approval_policy=\"never\"",
                "-c",
                "sandbox_mode=\"danger-full-access\"",
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
        },
    );
    manifests.insert(
        "claude",
        ProviderManifest {
            provider_name: "claude",
            auth_mount_paths: vec![".anthropic", ".claude"],
            // mvp12 M12.6: --dangerously-skip-permissions bypasses trust dialog + permission prompts (sandbox)
            command: &["claude", "--dangerously-skip-permissions"],
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
        },
    );
    manifests.insert(
        "antigravity",
        ProviderManifest {
            provider_name: "antigravity",
            auth_mount_paths: vec![".gemini/antigravity-cli"],
            command: &["agy", "--dangerously-skip-permissions"],
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
        },
    );
    manifests
});

pub fn get_manifest(provider: &str) -> ProviderManifest {
    try_get_manifest(provider).unwrap_or_else(|err| panic!("{err}"))
}

pub fn try_get_manifest(provider: &str) -> Result<ProviderManifest, CcbdError> {
    let canonical = canonicalize_provider_name(provider);
    MANIFESTS
        .get(canonical)
        .cloned()
        .ok_or_else(|| unknown_provider_error(provider))
}

pub fn is_valid_provider(provider: &str) -> bool {
    MANIFESTS.contains_key(canonicalize_provider_name(provider))
}

pub fn valid_provider_names() -> &'static [&'static str] {
    VALID_PROVIDER_NAMES
}

pub fn valid_provider_names_csv() -> String {
    valid_provider_names().join(", ")
}

pub fn unknown_provider_message(provider: &str) -> String {
    format!(
        "unknown provider {provider:?}; valid providers: {}",
        valid_provider_names_csv()
    )
}

fn unknown_provider_error(provider: &str) -> CcbdError {
    CcbdError::EnvironmentNotSupported {
        details: unknown_provider_message(provider),
    }
}

pub fn known_provider_manifests() -> Vec<ProviderManifest> {
    ["codex", "claude", "antigravity"]
        .into_iter()
        .map(get_manifest)
        .collect()
}

pub fn cancel_keysyms_for_provider(provider: &str) -> &'static [&'static str] {
    match canonicalize_provider_name(provider) {
        "antigravity" => &["Escape"],
        _ => &["C-c"],
    }
}

pub fn collect_spawn_env(
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<(String, String)> {
    let mut env = HashMap::new();
    for key in manifest.env_passthrough {
        if manifest.provider_name == "claude" && is_claude_gateway_blocked_host_env(key) {
            continue;
        }
        if let Ok(value) = std::env::var(key) {
            env.insert((*key).to_string(), value);
        }
    }
    for (key, value) in manifest.injected_env_vars {
        env.insert((*key).to_string(), (*value).to_string());
    }
    for (key, value) in extra_env_vars {
        if manifest.provider_name == "claude" && !is_claude_gateway_allowed_extra_env(key, value) {
            continue;
        }
        env.insert(key.clone(), value.clone());
    }
    let mut env = env.into_iter().collect::<Vec<_>>();
    env.sort_by(|(left, _), (right, _)| left.cmp(right));
    env
}

fn is_claude_gateway_blocked_host_env(key: &str) -> bool {
    matches!(
        key,
        "ANTHROPIC_API_KEY" | "ANTHROPIC_AUTH_TOKEN" | "ANTHROPIC_BASE_URL"
    )
}

fn is_claude_gateway_allowed_extra_env(key: &str, value: &str) -> bool {
    match key {
        "ANTHROPIC_API_KEY" => false,
        "ANTHROPIC_AUTH_TOKEN" => crate::claude_gateway::fake_jwt_worker_id(value).is_ok(),
        "ANTHROPIC_BASE_URL" => is_localhost_gateway_base_url(value),
        _ => true,
    }
}

fn is_localhost_gateway_base_url(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("http://") else {
        return false;
    };
    let Some((host, port)) = rest.rsplit_once(':') else {
        return false;
    };
    matches!(host, "localhost" | "127.0.0.1") && port.parse::<u16>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::{
        CompletionSignalKind, IdleDetectionMode, InitProbeKind, MANIFESTS,
        cancel_keysyms_for_provider, canonicalize_provider_name, collect_spawn_env,
        compute_recovery_args, get_manifest, is_valid_provider, try_get_manifest,
        valid_provider_names,
    };
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Duration;

    fn antigravity_conversations_dir(sandbox_home: &Path) -> PathBuf {
        sandbox_home.join(".gemini/antigravity-cli/conversations")
    }

    fn write_antigravity_conversation(sandbox_home: &Path, file_name: &str) -> PathBuf {
        let dir = antigravity_conversations_dir(sandbox_home);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(file_name);
        fs::write(&path, b"conversation").unwrap();
        path
    }

    #[test]
    fn codex_idle_anti_pattern_matches_real_working_line_not_idle_composer() {
        let manifest = get_manifest("codex");
        let anti_pattern = regex::Regex::new(manifest.idle_anti_pattern).unwrap();

        assert!(anti_pattern.is_match("• Working (4s • esc to interrupt)"));
        assert!(anti_pattern.is_match("Hooks need review"));
        assert!(anti_pattern.is_match("Trust all and continue"));
        assert!(anti_pattern.is_match("Continue without trusting"));
        assert!(!anti_pattern.is_match("› Run /review on my current changes"));
        assert!(!anti_pattern.is_match("  gpt-5.5 default · /tmp/x"));
    }

    #[test]
    fn test_builtin_providers_registered() {
        for provider in ["bash", "codex", "claude", "antigravity"] {
            assert!(
                MANIFESTS.contains_key(provider),
                "missing provider {provider}"
            );
            assert_eq!(get_manifest(provider).provider_name, provider);
        }
    }

    #[test]
    /// An agent that cannot reach the network cannot authenticate, and the
    /// provider CLIs report that as "not signed in" rather than as a network
    /// failure — so proxy settings have to survive the spawn env filter on every
    /// provider that talks to a remote API.
    #[test]
    fn every_networked_provider_passes_proxy_settings_into_the_sandbox() {
        for provider in ["claude", "codex", "antigravity"] {
            let passthrough = get_manifest(provider).env_passthrough;
            for key in [
                "HTTP_PROXY",
                "http_proxy",
                "HTTPS_PROXY",
                "https_proxy",
                "ALL_PROXY",
                "all_proxy",
                "NO_PROXY",
                "no_proxy",
            ] {
                assert!(
                    passthrough.contains(&key),
                    "{provider} must pass {key} through; without it the agent is offline"
                );
            }
        }
    }

    fn canonicalize_provider_name_maps_gemini_alias_only() {
        assert_eq!(canonicalize_provider_name("gemini"), "antigravity");
        assert_eq!(canonicalize_provider_name("antigravity"), "antigravity");
        assert_eq!(canonicalize_provider_name("codex"), "codex");
        assert_eq!(canonicalize_provider_name("claude"), "claude");
        assert_eq!(canonicalize_provider_name("bash"), "bash");
        assert_eq!(canonicalize_provider_name("unknown"), "unknown");
        assert_eq!(canonicalize_provider_name("Gemini"), "Gemini");
    }

    #[test]
    fn gemini_alias_resolves_to_antigravity_manifest() {
        assert!(is_valid_provider("gemini"));
        let manifest = try_get_manifest("gemini").unwrap();

        assert_eq!(manifest.provider_name, "antigravity");
        assert_eq!(manifest.command, ["agy", "--dangerously-skip-permissions"]);
        assert_eq!(cancel_keysyms_for_provider("gemini"), ["Escape"]);
    }

    #[test]
    fn unknown_provider_is_hard_error_not_bash() {
        let err = try_get_manifest("claud").unwrap_err();
        let message = err.to_string();

        assert!(message.contains("claud"));
        for provider in ["bash", "codex", "claude", "antigravity"] {
            assert!(message.contains(provider), "{message}");
        }
    }

    #[test]
    fn test_codex_auth_mounts_are_non_empty() {
        assert!(!get_manifest("codex").auth_mount_paths.is_empty());
    }

    #[test]
    fn test_provider_commands_and_probe_kinds_match_calibration() {
        let codex = get_manifest("codex");
        assert_eq!(
            codex.command,
            [
                "codex",
                "--dangerously-bypass-approvals-and-sandbox",
                "--dangerously-bypass-hook-trust",
                "-c",
                "disable_paste_burst=true",
                "-c",
                "trust_level=\"trusted\"",
                "-c",
                "approval_policy=\"never\"",
                "-c",
                "sandbox_mode=\"danger-full-access\"",
            ]
        );
        assert_eq!(codex.init_probe, InitProbeKind::Codex);
        assert_eq!(codex.stability_ms, 300);
        assert!(codex.resume_args.is_empty());
        assert_eq!(codex.completion_signal, CompletionSignalKind::LogOnly);

        let claude = get_manifest("claude");
        assert_eq!(claude.command, ["claude", "--dangerously-skip-permissions"]);
        assert_eq!(claude.init_probe, InitProbeKind::Claude);
        assert_eq!(claude.stability_ms, 300);
        assert_eq!(claude.resume_args, ["--continue"]);
        assert_eq!(claude.completion_signal, CompletionSignalKind::LogOnly);

        let antigravity = get_manifest("antigravity");
        assert_eq!(antigravity.provider_name, "antigravity");
        assert_eq!(
            antigravity.command,
            ["agy", "--dangerously-skip-permissions"]
        );
        assert!(
            antigravity
                .auth_mount_paths
                .contains(&".gemini/antigravity-cli"),
            "antigravity OAuth state should be mounted"
        );
        assert_eq!(
            antigravity.idle_detection_mode,
            IdleDetectionMode::LineEndRegex
        );
        assert_eq!(antigravity.init_probe, InitProbeKind::Antigravity);
        assert!(
            antigravity.idle_anti_pattern.contains("esc to cancel"),
            "antigravity busy status line must suppress idle"
        );
        assert_eq!(antigravity.completion_signal, CompletionSignalKind::LogOnly);
    }

    #[test]
    fn test_antigravity_cancel_uses_single_escape() {
        assert_eq!(cancel_keysyms_for_provider("antigravity"), ["Escape"]);
        assert_eq!(cancel_keysyms_for_provider("codex"), ["C-c"]);
    }

    #[test]
    fn antigravity_recovery_args_uses_newest_db_and_ignores_sidecars() {
        let sandbox_home = tempfile::TempDir::new().unwrap();
        write_antigravity_conversation(
            sandbox_home.path(),
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb.db",
        );
        thread::sleep(Duration::from_millis(20));
        write_antigravity_conversation(
            sandbox_home.path(),
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.db",
        );
        thread::sleep(Duration::from_millis(20));
        write_antigravity_conversation(
            sandbox_home.path(),
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb.db-wal",
        );
        write_antigravity_conversation(
            sandbox_home.path(),
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb.db-shm",
        );

        assert_eq!(
            compute_recovery_args("antigravity", sandbox_home.path()),
            [
                "--conversation".to_string(),
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string()
            ]
        );
    }

    #[test]
    fn antigravity_recovery_args_uses_newest_pb_over_db() {
        let sandbox_home = tempfile::TempDir::new().unwrap();
        write_antigravity_conversation(
            sandbox_home.path(),
            "33333333-3333-4333-8333-333333333333.db",
        );
        thread::sleep(Duration::from_millis(20));
        write_antigravity_conversation(
            sandbox_home.path(),
            "44444444-4444-4444-8444-444444444444.pb",
        );

        assert_eq!(
            compute_recovery_args("antigravity", sandbox_home.path()),
            [
                "--conversation".to_string(),
                "44444444-4444-4444-8444-444444444444".to_string()
            ]
        );
    }

    #[test]
    fn antigravity_recovery_args_falls_back_to_continue_without_conversation_file() {
        let sandbox_home = tempfile::TempDir::new().unwrap();

        assert_eq!(
            compute_recovery_args("antigravity", sandbox_home.path()),
            ["--continue".to_string()]
        );
    }

    #[test]
    fn compute_recovery_args_antigravity_routes_to_conversation_id() {
        let sandbox_home = tempfile::TempDir::new().unwrap();
        write_antigravity_conversation(
            sandbox_home.path(),
            "55555555-5555-4555-8555-555555555555.db",
        );

        assert_eq!(
            compute_recovery_args("antigravity", sandbox_home.path()),
            [
                "--conversation".to_string(),
                "55555555-5555-4555-8555-555555555555".to_string()
            ]
        );
    }

    #[test]
    fn test_bash_has_zero_stability() {
        let manifest = get_manifest("bash");

        assert_eq!(manifest.stability_ms, 0);
        assert_eq!(
            manifest.idle_detection_mode,
            IdleDetectionMode::LineEndRegex
        );
        assert_eq!(manifest.init_probe, InitProbeKind::Bash);
        assert_eq!(manifest.command, ["bash", "--noprofile", "--norc", "-i"]);
        assert!(manifest.resume_args.is_empty());
        assert_eq!(manifest.injected_env_vars, [("PS1", "$ ")]);
    }

    #[test]
    fn explicit_bash_provider_still_valid() {
        let manifest = try_get_manifest("bash").unwrap();

        assert_eq!(manifest.provider_name, "bash");
        assert_eq!(manifest.command, ["bash", "--noprofile", "--norc", "-i"]);
        assert_eq!(manifest.injected_env_vars, [("PS1", "$ ")]);
    }

    #[test]
    fn valid_provider_names_are_single_truth_for_public_set() {
        assert_eq!(
            valid_provider_names(),
            ["bash", "codex", "claude", "antigravity"]
        );
    }

    #[test]
    fn test_real_provider_manifest_parity_fields_are_populated() {
        for provider in ["codex", "claude", "antigravity"] {
            let manifest = get_manifest(provider);
            assert!(!manifest.env_passthrough.is_empty(), "{provider}");
            assert!(!manifest.injected_env_vars.is_empty(), "{provider}");
            assert!(manifest.readiness_timeout_s > 0, "{provider}");
            assert!(
                matches!(
                    manifest.init_probe,
                    InitProbeKind::Codex | InitProbeKind::Claude | InitProbeKind::Antigravity
                ),
                "{provider}"
            );
        }
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn test_collect_spawn_env_precedence() {
        unsafe {
            std::env::set_var("ANTHROPIC_API_KEY", "host-key");
            std::env::set_var("ANTHROPIC_AUTH_TOKEN", "host-token");
            std::env::set_var("ANTHROPIC_BASE_URL", "https://api.anthropic.com");
            std::env::set_var("CCB_CLAUDE_MD_MODE", "host-mode");
        }
        let mut extra = HashMap::new();
        extra.insert("CCB_CLAUDE_MD_MODE".to_string(), "extra-mode".to_string());
        extra.insert(
            "ANTHROPIC_AUTH_TOKEN".to_string(),
            crate::claude_gateway::fake_worker_jwt("worker-a"),
        );
        extra.insert(
            "ANTHROPIC_BASE_URL".to_string(),
            "http://localhost:49152".to_string(),
        );
        let env = collect_spawn_env(&get_manifest("claude"), &extra);

        assert!(!env.iter().any(|(key, _)| key == "ANTHROPIC_API_KEY"));
        assert!(!env.contains(&("ANTHROPIC_AUTH_TOKEN".to_string(), "host-token".to_string())));
        assert!(!env.contains(&(
            "ANTHROPIC_BASE_URL".to_string(),
            "https://api.anthropic.com".to_string()
        )));
        assert!(env.iter().any(|(key, value)| {
            key == "ANTHROPIC_AUTH_TOKEN"
                && crate::claude_gateway::fake_jwt_worker_id(value).as_deref() == Ok("worker-a")
        }));
        assert!(env.contains(&(
            "ANTHROPIC_BASE_URL".to_string(),
            "http://localhost:49152".to_string()
        )));
        assert!(env.contains(&("CCB_CLAUDE_MD_MODE".to_string(), "extra-mode".to_string())));

        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
            std::env::remove_var("ANTHROPIC_AUTH_TOKEN");
            std::env::remove_var("ANTHROPIC_BASE_URL");
            std::env::remove_var("CCB_CLAUDE_MD_MODE");
        }
    }
}
