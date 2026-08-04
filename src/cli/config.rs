use crate::cli::rpc_client::CliError;
pub use crate::provider::extensions::{ExtensionConfig, HookGroup, HookItem};
use crate::provider::manifest::{
    canonicalize_provider_name, is_valid_provider, unknown_provider_message,
};
use crate::provider::skills::parse_skill_refs;
use crate::tmux::TmuxWindowSize;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    pub version: String,
    #[serde(default)]
    pub master: MasterConfig,
    #[serde(default)]
    pub completion: CompletionConfig,
    #[serde(default)]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub providers: ProviderConfigs,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub sandbox: SandboxConfig,
    pub agents: BTreeMap<String, AgentConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CompletionConfig {
    #[serde(default)]
    pub hook_push_enabled: bool,
}

impl Default for CompletionConfig {
    fn default() -> Self {
        Self {
            hook_push_enabled: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub cmd: String,
    /// True when `cmd` came from the config file rather than from the resolved
    /// provider's default. Only an author-written `cmd` can conflict with an
    /// author-written `provider`.
    pub cmd_explicit: bool,
    pub provider: Option<String>,
    pub readiness_timeout_s: u64,
    pub enabled: bool,
    pub window_size: TmuxWindowSize,
    pub hooks: HashMap<String, Vec<HookGroup>>,
    pub plugins: Vec<String>,
    pub skills: Vec<String>,
    pub bundle: Vec<String>,
    pub settings: serde_json::Map<String, serde_json::Value>,
}

/// Wire shape of `[master]`. `cmd` is optional here so that the resolved
/// provider can supply the launch command when the author did not write one.
#[derive(Debug, Deserialize)]
struct MasterConfigWire {
    #[serde(default)]
    cmd: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default = "default_master_readiness_timeout_s")]
    readiness_timeout_s: u64,
    #[serde(default = "default_master_enabled")]
    enabled: bool,
    #[serde(default)]
    window_size: TmuxWindowSize,
    #[serde(default)]
    hooks: HashMap<String, Vec<HookGroup>>,
    #[serde(default)]
    plugins: Vec<String>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_bundle_refs")]
    bundle: Vec<String>,
    #[serde(default)]
    settings: serde_json::Map<String, serde_json::Value>,
}

impl<'de> Deserialize<'de> for MasterConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = MasterConfigWire::deserialize(deserializer)?;
        let written_cmd = wire
            .cmd
            .as_deref()
            .map(str::trim)
            .filter(|cmd| !cmd.is_empty())
            .map(str::to_string);
        let provider = wire
            .provider
            .as_deref()
            .map(str::trim)
            .filter(|provider| !provider.is_empty())
            .map(|provider| canonicalize_provider_name(provider).to_string());
        let cmd_explicit = written_cmd.is_some();
        let cmd = written_cmd.unwrap_or_else(|| {
            default_master_cmd_for_provider(provider.as_deref().unwrap_or(DEFAULT_MASTER_PROVIDER))
        });
        Ok(Self {
            cmd,
            cmd_explicit,
            provider,
            readiness_timeout_s: wire.readiness_timeout_s,
            enabled: wire.enabled,
            window_size: wire.window_size,
            hooks: wire.hooks,
            plugins: wire.plugins,
            skills: wire.skills,
            bundle: wire.bundle,
            settings: wire.settings,
        })
    }
}

impl MasterConfig {
    /// The provider that actually runs this master. An explicit `provider` wins;
    /// otherwise it is derived from the first word of `cmd`. Every consumer reads
    /// this instead of re-deriving the provider from `cmd` or defaulting to a
    /// provider name of its own.
    pub fn resolved_provider(&self) -> String {
        resolve_master_provider(self.provider.as_deref(), &self.cmd)
    }
}

/// The one place that answers "which provider runs this master seat".
///
/// A declared provider wins. Otherwise the provider is implied by the first word
/// of the master command, when that word names a provider ah knows. A command
/// that is a script or an unrelated binary implies nothing, and the master falls
/// back to the default master provider.
pub fn resolve_master_provider(provider: Option<&str>, cmd: &str) -> String {
    provider
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .map(|provider| canonicalize_provider_name(provider).to_string())
        .or_else(|| provider_name_from_command(cmd))
        .unwrap_or_else(|| DEFAULT_MASTER_PROVIDER.to_string())
}

/// Provider implied by a master command string, or `None` when its first word is
/// not a provider ah knows.
pub fn provider_name_from_command(cmd: &str) -> Option<String> {
    let binary = cmd
        .split_whitespace()
        .next()
        .map(Path::new)
        .and_then(Path::file_name)
        .and_then(|binary| binary.to_str())?;
    let canonical = canonicalize_provider_name(binary);
    is_valid_provider(canonical).then(|| canonical.to_string())
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            cmd: default_master_cmd_for_provider(DEFAULT_MASTER_PROVIDER),
            cmd_explicit: false,
            provider: None,
            readiness_timeout_s: default_master_readiness_timeout_s(),
            enabled: default_master_enabled(),
            window_size: TmuxWindowSize::Fixed,
            hooks: HashMap::new(),
            plugins: Vec::new(),
            skills: Vec::new(),
            bundle: Vec::new(),
            settings: serde_json::Map::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DaemonConfig {}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProviderConfigs {
    #[serde(default)]
    pub claude: ClaudeProviderConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClaudeProviderConfig {
    #[serde(default)]
    pub shared_credentials_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    pub provider: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub hooks: HashMap<String, Vec<HookGroup>>,
    #[serde(default)]
    pub plugins: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_bundle_refs")]
    pub bundle: Vec<String>,
    #[serde(default)]
    pub settings: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SandboxConfig {
    #[serde(default)]
    pub additional_ro_binds: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
}

pub fn load_project_config(path: &Path) -> Result<ProjectConfig, CliError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        CliError::Config(format!("failed to read config {}: {err}", path.display()))
    })?;
    reject_removed_layout_field(&raw)?;
    let mut config: ProjectConfig = toml::from_str(&raw)?;
    normalize_project_config(&mut config, std::env::var_os("HOME").map(PathBuf::from).as_deref());
    let diagnostics = validate_project_config(&config);
    if let Some(diagnostic) = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    {
        return Err(CliError::Config(diagnostic.message.clone()));
    }
    Ok(config)
}

/// Prints the non-fatal diagnostics of a loaded config to stderr.
///
/// Loading only rejects errors, so warnings — a master provider that cannot
/// carry part of the role, for one — would otherwise stay invisible until the
/// dependent behaviour silently went missing.
pub fn warn_config_diagnostics(config: &ProjectConfig) {
    for diagnostic in validate_project_config(config)
        .iter()
        .filter(|diagnostic| diagnostic.severity == DiagnosticSeverity::Warning)
    {
        eprintln!("warning: {}", diagnostic.message);
    }
}

pub fn find_config(start_dir: &Path) -> Result<PathBuf, CliError> {
    find_config_with_env(start_dir, std::env::var_os("CCB_CONFIG_PATH"))
}

pub fn validate_project_config(config: &ProjectConfig) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if config.version != "1" {
        diagnostics.push(error("ah.toml version must be \"1\""));
    }
    if !config.sandbox.additional_ro_binds.is_empty() {
        diagnostics.push(error(
            "additional_ro_binds is not supported because systemd-run --scope does not accept BindReadOnlyPaths (which is a service-unit-only property)"
        ));
    }
    if config.agents.is_empty() {
        diagnostics.push(error("ah.toml must define at least one [agents.<id>]"));
    }
    if let Err(err) = parse_skill_refs(&config.master.skills) {
        diagnostics.push(error(format!("invalid master skills: {err}")));
    }
    if let Err(err) = validate_bundle_refs(&config.master.bundle) {
        diagnostics.push(error(format!("invalid master bundle: {err}")));
    }
    validate_master_provider_config(config, &mut diagnostics);
    validate_claude_provider_config(config, &mut diagnostics);
    for (agent_id, agent) in &config.agents {
        if !is_valid_agent_id(agent_id) {
            diagnostics.push(error(format!(
                "invalid agent id {agent_id:?}; use ASCII alphanumeric, '_' or '-'"
            )));
        }
        if agent.provider.trim().is_empty() {
            diagnostics.push(error(format!(
                "agent {agent_id:?} must define a non-empty provider"
            )));
        } else if !is_valid_provider(&agent.provider) {
            diagnostics.push(error(format!(
                "agent {agent_id:?} has {}; fix provider spelling",
                unknown_provider_message(&agent.provider)
            )));
        }
        if let Err(err) = parse_skill_refs(&agent.skills) {
            diagnostics.push(error(format!(
                "agent {agent_id:?} has invalid skills: {err}"
            )));
        }
        if let Err(err) = validate_bundle_refs(&agent.bundle) {
            diagnostics.push(error(format!(
                "agent {agent_id:?} has invalid bundle: {err}"
            )));
        }
        if !agent.settings.is_empty()
            && crate::provider::manifest::provider_capabilities(&agent.provider)
                .is_some_and(|capabilities| !capabilities.settings)
        {
            diagnostics.push(error(format!(
                "agent '{agent_id}' declares settings but provider '{}' does not support the 'settings' capability",
                agent.provider
            )));
        }
    }
    diagnostics
}

/// Validates the master seat against the capabilities its provider declares.
///
/// Feature gates are per capability, not per provider name: a provider that
/// declares `bundles` may carry `master.bundle`, one that declares `settings`
/// may carry `[master.settings]`. Missing master-role capabilities are reported
/// as warnings listing what stops working, so a degraded master is visible
/// rather than silent; the operations that need those capabilities fail at their
/// own call sites.
fn validate_master_provider_config(config: &ProjectConfig, diagnostics: &mut Vec<Diagnostic>) {
    if let Some(provider) = config.master.provider.as_deref()
        && !is_valid_provider(provider)
    {
        diagnostics.push(error(format!(
            "master has {}; fix provider spelling",
            unknown_provider_message(provider)
        )));
        return;
    }

    let provider = config.master.resolved_provider();
    if let Some(declared) = config.master.provider.as_deref()
        && config.master.cmd_explicit
        && let Some(cmd_provider) = provider_name_from_command(&config.master.cmd)
        && cmd_provider != declared
    {
        diagnostics.push(error(format!(
            "master declares provider '{declared}' but cmd runs provider '{cmd_provider}'; \
             drop one of them so the master seat has a single provider"
        )));
    }

    let Some(capabilities) = crate::provider::manifest::provider_capabilities(&provider) else {
        return;
    };

    if !config.master.bundle.is_empty() && !capabilities.bundles {
        diagnostics.push(error(format!(
            "master declares bundle but provider '{provider}' does not support the 'bundles' capability"
        )));
    }
    if !config.master.settings.is_empty() && !capabilities.settings {
        diagnostics.push(error(format!(
            "master declares settings but provider '{provider}' does not support the 'settings' capability"
        )));
    }

    if !config.master.enabled {
        return;
    }
    let missing = capabilities.missing_for_master();
    if !missing.is_empty() {
        diagnostics.push(warning(format!(
            "master provider '{provider}' is missing master capabilities [{}]: {}",
            missing.join(", "),
            missing
                .iter()
                .map(|capability| master_capability_consequence(capability))
                .collect::<Vec<_>>()
                .join("; ")
        )));
    }
}

fn master_capability_consequence(capability: &str) -> &'static str {
    match capability {
        "rules_target" => "no master rules document is materialized",
        "completion_signal" => "no end-of-turn completion signal is delivered",
        "readiness_ack" => {
            "master cutover is refused and master revive readiness degrades to process start"
        }
        _ => "the dependent master behaviour is unavailable",
    }
}

fn validate_claude_provider_config(config: &ProjectConfig, diagnostics: &mut Vec<Diagnostic>) {
    if let Some(path) = config.providers.claude.shared_credentials_dir.as_ref() {
        if path.as_os_str().is_empty() || path.as_os_str().to_string_lossy().trim().is_empty() {
            diagnostics.push(error(
                "providers.claude.shared_credentials_dir must be a non-empty absolute path",
            ));
        } else if !path.is_absolute() {
            diagnostics.push(error(
                "providers.claude.shared_credentials_dir must be an absolute path",
            ));
        }
    }

    if config_uses_claude_provider(config)
        && config.providers.claude.shared_credentials_dir.is_none()
    {
        diagnostics.push(error(
            "providers.claude.shared_credentials_dir is required when master or agents use provider claude",
        ));
    }
}

fn config_uses_claude_provider(config: &ProjectConfig) -> bool {
    let master_uses_claude = config.master.enabled && config.master.resolved_provider() == "claude";
    master_uses_claude
        || config
            .agents
            .values()
            .any(|agent| agent.provider == "claude")
}

fn deserialize_bundle_refs<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BundleInput {
        Single(String),
        Many(Vec<String>),
    }

    match Option::<BundleInput>::deserialize(deserializer)? {
        Some(BundleInput::Single(value)) => Ok(vec![value]),
        Some(BundleInput::Many(values)) => Ok(values),
        None => Ok(Vec::new()),
    }
}

fn validate_bundle_refs(bundle: &[String]) -> Result<(), String> {
    for name in bundle {
        if name.is_empty() {
            return Err("bundle name must not be empty".to_string());
        }
        let path = Path::new(name);
        if path.is_absolute()
            || name.contains('\\')
            || path.components().count() != 1
            || !path
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            return Err(format!(
                "invalid bundle name {name:?}; use a single directory name"
            ));
        }
    }
    Ok(())
}

pub(crate) fn find_config_with_env(
    start_dir: &Path,
    env_path: Option<OsString>,
) -> Result<PathBuf, CliError> {
    if let Some(path) = env_path {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(CliError::Config(format!(
            "CCB_CONFIG_PATH points to missing config: {}",
            path.display()
        )));
    }

    let mut current = if start_dir.is_file() {
        start_dir.parent()
    } else {
        Some(start_dir)
    }
    .ok_or_else(|| CliError::Config(format!("invalid start dir: {}", start_dir.display())))?
    .to_path_buf();

    loop {
        let candidate = current.join("ah.toml");
        if candidate.is_file() {
            return Ok(candidate);
        }
        if !current.pop() {
            break;
        }
    }

    Err(CliError::Config(format!(
        "could not find ah.toml from {}; create one or set CCB_CONFIG_PATH",
        start_dir.display()
    )))
}

/// Provider assumed for a master whose config names neither a provider nor a
/// command ah recognizes. This keeps the historical default: an untouched
/// `[master]` runs Claude.
pub const DEFAULT_MASTER_PROVIDER: &str = "claude";

/// Launch command for a master whose config could not be read at all. Callers
/// recovering a master without its config use this instead of spelling a
/// provider binary themselves.
pub fn default_master_cmd() -> String {
    default_master_cmd_for_provider(DEFAULT_MASTER_PROVIDER)
}

/// Launch command used when the author did not write `master.cmd`.
///
/// Claude keeps its historical bare-binary default, which is a shipped contract.
/// Every other provider gets its manifest launch command, because that argument
/// set is the one ah knows starts the provider correctly inside a sandbox; the
/// command is shell-quoted since master commands run through `sh -lc`.
fn default_master_cmd_for_provider(provider: &str) -> String {
    let canonical = canonicalize_provider_name(provider);
    if canonical == DEFAULT_MASTER_PROVIDER {
        return DEFAULT_MASTER_PROVIDER.to_string();
    }
    match crate::provider::manifest::try_get_manifest(canonical) {
        Ok(manifest) => manifest
            .command
            .iter()
            .map(|part| shell_quote_command_part(part))
            .collect::<Vec<_>>()
            .join(" "),
        Err(_) => canonical.to_string(),
    }
}

fn shell_quote_command_part(part: &str) -> String {
    if !part.is_empty()
        && part
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | '='))
    {
        return part.to_string();
    }
    format!("'{}'", part.replace('\'', "'\\''"))
}

fn default_master_readiness_timeout_s() -> u64 {
    120
}

fn default_master_enabled() -> bool {
    true
}

fn reject_removed_layout_field(raw: &str) -> Result<(), CliError> {
    let value = raw.parse::<toml::Value>()?;
    if value
        .as_table()
        .is_some_and(|table| table.contains_key("layout"))
    {
        return Err(CliError::Config(
            "layout config was removed; omit the top-level layout field".into(),
        ));
    }
    Ok(())
}

/// Brings a freshly parsed config to its resolved form before anything reads it:
/// provider aliases become canonical names, and `~` in a path becomes the
/// invoking user's home. Everything downstream — validation included — sees only
/// resolved values, so no consumer has to repeat this work or guess.
///
/// `home` is passed in rather than read here so the rule is testable without
/// mutating process environment.
fn normalize_project_config(config: &mut ProjectConfig, home: Option<&Path>) {
    if let Some(provider) = config.master.provider.as_mut() {
        let canonical = canonicalize_provider_name(provider);
        if canonical != provider {
            *provider = canonical.to_string();
        }
    }
    for agent in config.agents.values_mut() {
        let canonical = canonicalize_provider_name(&agent.provider);
        if canonical != agent.provider {
            agent.provider = canonical.to_string();
        }
    }
    if let Some(path) = config.providers.claude.shared_credentials_dir.as_mut() {
        *path = expand_home_prefix(path, home);
    }
}

/// Expands a leading `~` to `home`. Everything else is returned unchanged,
/// including `~user` forms, which name a different user's home and are not
/// something ah resolves.
///
/// This exists so a committed `ah.toml` can name the host login store without
/// hardcoding one machine's absolute path, which is what the config being
/// "safe to commit and share" requires. With no home available the path is left
/// alone and validation rejects it as non-absolute, rather than silently
/// resolving to something surprising.
fn expand_home_prefix(path: &Path, home: Option<&Path>) -> PathBuf {
    let text = path.to_string_lossy();
    let Some(home) = home else {
        return path.to_path_buf();
    };
    if text == "~" {
        return home.to_path_buf();
    }
    match text.strip_prefix("~/") {
        Some(rest) => home.join(rest),
        None => path.to_path_buf(),
    }
}

fn is_valid_agent_id(agent_id: &str) -> bool {
    !agent_id.is_empty()
        && agent_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn error(message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        severity: DiagnosticSeverity::Error,
        message: message.into(),
    }
}

fn warning(message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        severity: DiagnosticSeverity::Warning,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompletionConfig, DaemonConfig, DiagnosticSeverity, MasterConfig, find_config_with_env,
        load_project_config,
    };
    use crate::tmux::TmuxWindowSize;
    use std::ffi::OsString;
    use std::path::Path;

    #[test]
    fn test_load_valid_config_without_layout() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let config = load_project_config(&path).unwrap();

        assert_eq!(config.agents["a1"].provider, "bash");
        assert!(config.sandbox.additional_ro_binds.is_empty());
        assert!(!config.completion.hook_push_enabled);
    }

    #[test]
    fn test_load_project_config_canonicalizes_gemini_provider_alias() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"

[master]
provider = "gemini"

[agents.a1]
provider = "gemini"
"#,
        )
        .unwrap();

        let config = load_project_config(&path).unwrap();

        assert_eq!(config.master.provider.as_deref(), Some("antigravity"));
        assert_eq!(config.agents["a1"].provider, "antigravity");
    }

    #[test]
    fn parses_claude_shared_credentials_dir_config() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[providers.claude]
shared_credentials_dir = "/tmp/user/.claude"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(
            config.providers.claude.shared_credentials_dir.as_deref(),
            Some(std::path::Path::new("/tmp/user/.claude"))
        );
        assert!(super::validate_project_config(&config).is_empty());
    }

    #[test]
    fn rejects_empty_claude_shared_credentials_dir_config() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[providers.claude]
shared_credentials_dir = ""

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.message.contains("shared_credentials_dir")
                && diagnostic.message.contains("non-empty")
        }));
    }

    #[test]
    fn rejects_relative_claude_shared_credentials_dir_config() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[providers.claude]
shared_credentials_dir = ".claude"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.message.contains("shared_credentials_dir")
                && diagnostic.message.contains("absolute")
        }));
    }

    #[test]
    fn rejects_claude_provider_without_shared_credentials_dir_config() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "claude"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.message.contains("shared_credentials_dir")
                && diagnostic.message.contains("required")
        }));
    }

    fn errors(config: &super::ProjectConfig) -> Vec<String> {
        super::validate_project_config(config)
            .into_iter()
            .filter(|diagnostic| diagnostic.severity == super::DiagnosticSeverity::Error)
            .map(|diagnostic| diagnostic.message)
            .collect()
    }

    fn warnings(config: &super::ProjectConfig) -> Vec<String> {
        super::validate_project_config(config)
            .into_iter()
            .filter(|diagnostic| diagnostic.severity == super::DiagnosticSeverity::Warning)
            .map(|diagnostic| diagnostic.message)
            .collect()
    }

    #[test]
    fn shared_credentials_dir_expands_a_leading_tilde_to_the_invoking_home() {
        let mut config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[providers.claude]
shared_credentials_dir = "~/.claude"

[agents.a1]
provider = "claude"
"#,
        )
        .unwrap();

        super::normalize_project_config(&mut config, Some(Path::new("/home/alice")));

        assert_eq!(
            config.providers.claude.shared_credentials_dir.as_deref(),
            Some(Path::new("/home/alice/.claude"))
        );
        assert!(
            errors(&config).is_empty(),
            "an expanded ~ path is absolute and must validate: {:?}",
            errors(&config)
        );
    }

    #[test]
    fn shared_credentials_dir_without_a_home_stays_unexpanded_and_is_rejected() {
        let mut config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[providers.claude]
shared_credentials_dir = "~/.claude"

[agents.a1]
provider = "claude"
"#,
        )
        .unwrap();

        super::normalize_project_config(&mut config, None);

        assert_eq!(
            config.providers.claude.shared_credentials_dir.as_deref(),
            Some(Path::new("~/.claude")),
            "no home means no guessing"
        );
        assert!(
            errors(&config)
                .iter()
                .any(|message| message.contains("must be an absolute path")),
            "an unexpanded ~ path must still be rejected: {:?}",
            errors(&config)
        );
    }

    #[test]
    fn shared_credentials_dir_leaves_other_tilde_forms_alone() {
        assert_eq!(
            super::expand_home_prefix(Path::new("~bob/.claude"), Some(Path::new("/home/alice"))),
            Path::new("~bob/.claude"),
            "~user names another user's home, which ah does not resolve"
        );
        assert_eq!(
            super::expand_home_prefix(Path::new("/srv/.claude"), Some(Path::new("/home/alice"))),
            Path::new("/srv/.claude")
        );
        assert_eq!(
            super::expand_home_prefix(Path::new("~"), Some(Path::new("/home/alice"))),
            Path::new("/home/alice")
        );
    }

    #[test]
    fn master_provider_codex_needs_no_claude_shared_credentials_dir() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
provider = "codex"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(config.master.resolved_provider(), "codex");
        assert!(
            errors(&config).is_empty(),
            "codex master should validate: {:?}",
            errors(&config)
        );
        assert!(
            warnings(&config).is_empty(),
            "codex declares every master capability: {:?}",
            warnings(&config)
        );
    }

    #[test]
    fn master_provider_without_cmd_takes_its_launch_command_from_the_provider() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
provider = "antigravity"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(config.master.resolved_provider(), "antigravity");
        assert!(
            config.master.cmd.starts_with("agy"),
            "master cmd should come from the antigravity manifest, got {:?}",
            config.master.cmd
        );
        assert!(!config.master.cmd_explicit);
    }

    #[test]
    fn master_provider_lacking_capabilities_warns_and_names_them() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
provider = "bash"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(
            errors(&config).is_empty(),
            "a capability-poor master is allowed to run: {:?}",
            errors(&config)
        );
        let warnings = warnings(&config);
        assert!(
            warnings.iter().any(|message| {
                message.contains("rules_target")
                    && message.contains("completion_signal")
                    && message.contains("readiness_ack")
            }),
            "warning must name every missing master capability: {warnings:?}"
        );
    }

    #[test]
    fn master_provider_conflicting_with_explicit_cmd_is_rejected() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
provider = "codex"
cmd = "claude"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(
            errors(&config)
                .iter()
                .any(|message| message.contains("single provider")),
            "conflicting provider and cmd must be rejected: {:?}",
            errors(&config)
        );
    }

    #[test]
    fn master_settings_are_gated_on_the_settings_capability() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
provider = "codex"

[master.settings]
model = "gpt-5.5"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(
            errors(&config)
                .iter()
                .any(|message| message.contains("'settings' capability")),
            "codex has no settings capability: {:?}",
            errors(&config)
        );
    }

    #[test]
    fn unknown_master_provider_is_rejected_by_name() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
provider = "clause"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(
            errors(&config)
                .iter()
                .any(|message| message.contains("unknown provider")),
            "misspelled master provider must be rejected: {:?}",
            errors(&config)
        );
    }

    #[test]
    fn non_claude_only_config_does_not_require_shared_credentials_dir() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(super::validate_project_config(&config).is_empty());
    }

    #[test]
    fn completion_hook_push_enabled_defaults_false() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(config.completion, CompletionConfig::default());
        assert!(!config.completion.hook_push_enabled);
    }

    #[test]
    fn completion_hook_push_enabled_reads_true() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[completion]
hook_push_enabled = true

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(config.completion.hook_push_enabled);
    }

    #[test]
    fn test_load_project_config_with_sandbox_additional_ro_binds() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[sandbox]
additional_ro_binds = ["/opt/tools", "/var/cache/models"]

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(
            config.sandbox.additional_ro_binds,
            vec!["/opt/tools", "/var/cache/models"]
        );
    }

    #[test]
    fn test_validate_project_config_rejects_scope_incompatible_ro_binds() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[sandbox]
additional_ro_binds = ["/opt/tools"]

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);

        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.severity == DiagnosticSeverity::Error && {
                    let message = diagnostic.message.to_lowercase();
                    message.contains("additional_ro_binds") && message.contains("scope")
                }
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn test_load_project_config_rejects_scope_incompatible_ro_binds() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"

[sandbox]
additional_ro_binds = ["/opt/tools"]

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let err = load_project_config(&path).unwrap_err();
        let message = err.to_string().to_lowercase();

        assert!(message.contains("additional_ro_binds"), "{message}");
        assert!(message.contains("scope"), "{message}");
    }

    #[test]
    fn test_load_project_config_reads_provider_settings() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master.settings]
model = "claude-opus-4-20250514"
autoCompact = false

[master.settings.statusLine]
type = "command"
command = "ah ps --format compact"

[agents.a1]
provider = "claude"

[agents.a1.settings]
model = "claude-sonnet-4-20250514"
autoCompact = true
"#,
        )
        .unwrap();

        assert_eq!(
            config.master.settings["model"],
            serde_json::json!("claude-opus-4-20250514")
        );
        assert_eq!(
            config.master.settings["autoCompact"],
            serde_json::json!(false)
        );
        assert_eq!(
            config.master.settings["statusLine"]["command"],
            serde_json::json!("ah ps --format compact")
        );
        assert_eq!(
            config.agents["a1"].settings["model"],
            serde_json::json!("claude-sonnet-4-20250514")
        );
    }

    #[test]
    fn test_load_project_config_rejects_non_claude_provider_settings() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "codex"

[agents.a1.settings]
model = "claude-sonnet-4-20250514"
"#,
        )
        .unwrap();

        let err = load_project_config(&path).unwrap_err().to_string();

        assert!(err.contains("agent 'a1' declares settings"));
        assert!(err.contains("provider 'codex' does not support the 'settings' capability"));
    }

    #[test]
    fn test_load_project_config_accepts_claude_and_default_master_settings() {
        let dir = tempfile::TempDir::new().unwrap();
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            format!(
                r#"
version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[master.settings]
model = "claude-opus-4-20250514"

[agents.a1]
provider = "claude"

[agents.a1.settings]
model = "claude-sonnet-4-20250514"
"#,
                shared_credentials_dir.path().display()
            ),
        )
        .unwrap();

        let config = load_project_config(&path).unwrap();

        assert_eq!(
            config.master.settings["model"],
            serde_json::json!("claude-opus-4-20250514")
        );
        assert_eq!(
            config.agents["a1"].settings["model"],
            serde_json::json!("claude-sonnet-4-20250514")
        );
    }

    #[test]
    fn test_master_config_default() {
        let master = MasterConfig::default();

        assert!(master.enabled);
        assert_eq!(master.cmd, "claude");
        assert_eq!(master.window_size, TmuxWindowSize::Fixed);
    }

    #[test]
    fn test_daemon_config_default() {
        let daemon = DaemonConfig::default();

        assert_eq!(daemon, DaemonConfig {});
    }

    #[test]
    fn test_load_project_config_default_daemon_when_missing() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(config.daemon, DaemonConfig {});
    }

    #[test]
    fn test_load_project_config_with_master_section() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
cmd = "opencode"
enabled = false

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(!config.master.enabled);
        assert_eq!(config.master.cmd, "opencode");
    }

    #[test]
    fn test_load_project_config_reads_master_follow_window_size() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
window_size = "follow"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(config.master.window_size, TmuxWindowSize::Follow);
    }

    #[test]
    fn test_load_project_config_reads_master_and_agent_skills() {
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        let config = toml::from_str::<super::ProjectConfig>(&format!(
            r#"
version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[master]
skills = ["master-domain"]

[agents.a1]
provider = "claude"
skills = ["worker-domain"]
"#,
            shared_credentials_dir.path().display()
        ))
        .unwrap();

        assert_eq!(config.master.skills, vec!["master-domain"]);
        assert_eq!(config.agents["a1"].skills, vec!["worker-domain"]);
        assert!(super::validate_project_config(&config).is_empty());
    }

    #[test]
    fn test_load_project_config_reads_bundle_string_and_list() {
        let shared_credentials_dir = tempfile::TempDir::new().unwrap();
        let config = toml::from_str::<super::ProjectConfig>(&format!(
            r#"
version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[master]
bundle = "domain"

[agents.a1]
provider = "claude"
bundle = ["domain", "team"]
"#,
            shared_credentials_dir.path().display()
        ))
        .unwrap();

        assert_eq!(config.master.bundle, vec!["domain"]);
        assert_eq!(config.agents["a1"].bundle, vec!["domain", "team"]);
        assert!(super::validate_project_config(&config).is_empty());
    }

    #[test]
    fn test_allows_bundle_refs_for_non_claude_provider() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "codex"
bundle = "domain"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);

        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_load_project_config_default_master_when_missing() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert!(config.master.enabled);
        assert_eq!(config.master.cmd, "claude");
    }

    #[test]
    fn test_load_project_config_empty_master_cmd_normalizes_to_claude() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
cmd = "   "

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        assert_eq!(config.master.cmd, "claude");
    }

    #[test]
    fn test_rejects_removed_layout_field() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"
layout = "diagonal"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let err = load_project_config(&path).unwrap_err();

        assert!(err.to_string().contains("layout config was removed"));
    }

    #[test]
    fn test_rejects_removed_grid_layout() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"
layout = "grid"

[agents.a1]
provider = "bash"
"#,
        )
        .unwrap();

        let err = load_project_config(&path).unwrap_err();

        assert!(err.to_string().contains("layout config was removed"));
    }

    #[test]
    fn test_rejects_empty_agents() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"
[agents]
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);

        assert_eq!(diagnostics[0].severity, DiagnosticSeverity::Error);
        assert!(diagnostics[0].message.contains("at least one"));
    }

    #[test]
    fn test_rejects_bad_agent_id() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[agents."bad/id"]
provider = "bash"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);

        assert!(diagnostics.iter().any(|d| d.message.contains("bad/id")));
    }

    #[test]
    fn test_rejects_unknown_provider_with_valid_values() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "claud"
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);
        let message = diagnostics
            .iter()
            .find(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
            .map(|diagnostic| diagnostic.message.as_str())
            .unwrap_or("");

        assert!(message.contains("claud"), "{message}");
        for provider in ["bash", "codex", "claude", "antigravity"] {
            assert!(message.contains(provider), "{message}");
        }
    }

    #[test]
    fn test_accepts_skills_for_codex_provider() {
        let config = toml::from_str::<super::ProjectConfig>(
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "codex"
skills = ["domain"]
"#,
        )
        .unwrap();

        let diagnostics = super::validate_project_config(&config);
        assert!(
            !diagnostics
                .iter()
                .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn test_load_project_config_rejects_unknown_provider() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "coddex"
"#,
        )
        .unwrap();

        let err = load_project_config(&path).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("coddex"), "{message}");
        assert!(message.contains("codex"), "{message}");
        assert!(message.contains("claude"), "{message}");
        assert!(message.contains("antigravity"), "{message}");
        assert!(message.contains("bash"), "{message}");
    }

    #[test]
    fn test_find_config_walks_up_from_cwd() {
        let root = tempfile::TempDir::new().unwrap();
        let nested = root.path().join("a/b");
        std::fs::create_dir_all(&nested).unwrap();
        let config = root.path().join("ah.toml");
        std::fs::write(
            &config,
            "version = \"1\"\n[agents.a1]\nprovider = \"bash\"\n",
        )
        .unwrap();

        let found = find_config_with_env(&nested, None).unwrap();

        assert_eq!(found, config);
    }

    #[test]
    fn test_find_config_prefers_env_path() {
        let root = tempfile::TempDir::new().unwrap();
        let env_config = root.path().join("custom.toml");
        std::fs::write(
            &env_config,
            "version = \"1\"\n[agents.env]\nprovider = \"bash\"\n",
        )
        .unwrap();
        std::fs::write(
            root.path().join("ah.toml"),
            "version = \"1\"\n[agents.local]\nprovider = \"bash\"\n",
        )
        .unwrap();

        let found = find_config_with_env(root.path(), Some(OsString::from(env_config.as_os_str())))
            .unwrap();

        assert_eq!(found, env_config);
    }
}
