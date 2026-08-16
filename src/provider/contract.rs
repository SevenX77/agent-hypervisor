use crate::completion::parser::{CompletionTerminality, LogParseResult};
use crate::provider::manifest::ProviderManifest;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// How one provider persists an authenticated session on one operating system.
///
/// This is part of the provider Adapter contract because the path and storage
/// mechanism are provider facts.  The shared auth-store checker consumes the
/// spec; it does not own another provider-name switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthStoreSpec {
    File { relative_path: &'static str },
    OsCredentialService { probe_hint: &'static str },
    Unmanaged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderAuthStoreSpec {
    pub os: &'static str,
    pub store: AuthStoreSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthChallengeKind {
    /// The official CLI prints a URL and one-time code, then polls.
    DeviceCode {
        /// Provider-owned pattern for the complete code shown by the
        /// assessed CLI. AH validates the capture before exposing the code.
        code_pattern: &'static str,
    },
    /// The official CLI prints a URL and accepts a code or redirect result.
    AuthorizationCodePaste,
}

/// How AH enters a provider's official authentication flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderLoginDriverSpec {
    None,
    /// A dedicated provider login command owns the complete interaction.
    Command {
        local_argv: &'static [&'static str],
        headless_argv: &'static [&'static str],
        challenge: AuthChallengeKind,
        /// Stable text proving that the command exposed its browser
        /// challenge. Headless authorization-code commands are observed in a
        /// private tmux pane so AH can reassemble terminal-wrapped URLs.
        challenge_markers: &'static [&'static str],
        /// Stable text proving that the provider is ready to accept the code
        /// returned by the browser.
        code_prompt_markers: &'static [&'static str],
        /// Provider-native terminal text proving that a submitted code was
        /// rejected. Only markers observed on the assessed CLI belong here.
        failure_markers: &'static [&'static str],
        /// Query keys that must survive terminal rendering before AH exposes
        /// an authorization URL to the operator.
        authorization_url_required_query_keys: &'static [&'static str],
    },
    /// Authentication is embedded in the provider's first-run TUI. AH drives
    /// only the declared login controls and leaves token exchange to the CLI.
    StartupTui {
        local_argv: &'static [&'static str],
        headless_argv: &'static [&'static str],
        select_prompt_markers: &'static [&'static str],
        challenge_markers: &'static [&'static str],
        code_prompt_markers: &'static [&'static str],
        /// Provider-native terminal text proving that a submitted code was
        /// rejected. Empty means the failure surface is not yet assessed.
        failure_markers: &'static [&'static str],
        /// Query keys that must survive terminal rendering before AH exposes
        /// an authorization URL. This prevents a hard-wrapped prefix from
        /// being mistaken for a complete provider challenge.
        authorization_url_required_query_keys: &'static [&'static str],
    },
}

/// Executable, provider-owned authentication contract.
///
/// The upstream manual is linked at the implementation boundary so future
/// maintenance starts from the exact external capability instead of repeating
/// discovery. `assessed_cli` scopes terminal markers to observed CLI versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderAuthSpec {
    pub assessed_cli: &'static str,
    pub documentation_url: Option<&'static str>,
    pub stores: &'static [ProviderAuthStoreSpec],
    pub login: ProviderLoginDriverSpec,
    pub status_argv: Option<&'static [&'static str]>,
    /// Provider-native environment variable that relocates its credential
    /// directory when the project declares an explicit shared store.
    pub config_dir_env: Option<&'static str>,
}

impl ProviderAuthSpec {
    pub fn store_for_os(&self, os: &str) -> AuthStoreSpec {
        self.stores
            .iter()
            .find(|spec| spec.os == os)
            .map(|spec| spec.store)
            .unwrap_or(AuthStoreSpec::Unmanaged)
    }

    pub fn login_argv(&self, headless: bool) -> Option<&'static [&'static str]> {
        match self.login {
            ProviderLoginDriverSpec::None => None,
            ProviderLoginDriverSpec::Command {
                local_argv,
                headless_argv,
                ..
            }
            | ProviderLoginDriverSpec::StartupTui {
                local_argv,
                headless_argv,
                ..
            } => Some(if headless { headless_argv } else { local_argv }),
        }
    }

    pub fn login_failure_markers(&self) -> &'static [&'static str] {
        match self.login {
            ProviderLoginDriverSpec::None => &[],
            ProviderLoginDriverSpec::Command {
                failure_markers, ..
            }
            | ProviderLoginDriverSpec::StartupTui {
                failure_markers, ..
            } => failure_markers,
        }
    }
}

/// A provider-native observation surface assessed by one Adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationSourceSpec {
    OfficialHook(&'static str),
    Transcript(&'static str),
    ProcessProbe,
    TerminalPane,
}

/// How one provider-native hook may contribute to AH runtime status.
///
/// `Candidate` is deliberate: an Adapter observation still has to correlate to
/// the active lifecycle and dispatched job before it can change status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookStatusUse {
    NotUsed,
    LifecycleCandidate,
    WorkingCandidate,
    ActivityCandidate,
    ApprovalCandidate,
    CompletionCandidate,
    CompletionWhenIdleCandidate,
}

/// Provider-owned specification for one external hook capability.
///
/// The documentation URL is part of the executable spec, rather than a README
/// note, so a maintainer changing hook materialization has the upstream usage
/// contract at the implementation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderHookSpec {
    pub event: &'static str,
    pub trigger: &'static str,
    pub status_use: HookStatusUse,
    /// Only fields AH currently consumes; this is not a copy of the upstream
    /// provider's complete wire schema.
    pub payload_fields_used_by_ah: &'static [&'static str],
    pub materialized_by_ah: bool,
    pub documentation_url: &'static str,
}

impl ProviderHookSpec {
    pub const fn available(
        event: &'static str,
        trigger: &'static str,
        status_use: HookStatusUse,
        documentation_url: &'static str,
    ) -> Self {
        Self {
            event,
            trigger,
            status_use,
            payload_fields_used_by_ah: &[],
            materialized_by_ah: false,
            documentation_url,
        }
    }

    pub const fn materialized(
        event: &'static str,
        trigger: &'static str,
        status_use: HookStatusUse,
        payload_fields_used_by_ah: &'static [&'static str],
        documentation_url: &'static str,
    ) -> Self {
        Self {
            event,
            trigger,
            status_use,
            payload_fields_used_by_ah,
            materialized_by_ah: true,
            documentation_url,
        }
    }
}

/// Executable, provider-local observation contract.
///
/// Native capability, AH materialization and AH state use stay separate. A
/// hook being listed here never means that AH installs or trusts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderObservationSpec {
    pub assessed_cli: &'static str,
    pub hooks: &'static [ProviderHookSpec],
    pub hook_config_path: Option<&'static str>,
    pub hook_config_schema: Option<&'static str>,
    pub hooks_enabled_by_default: bool,
    pub working_sources: &'static [ObservationSourceSpec],
    pub completion_sources: &'static [ObservationSourceSpec],
    pub real_hook_delivery_verified: bool,
    pub real_hook_delivery_evidence: Option<&'static str>,
}

/// Provider-owned terminal UI facts used only to confirm control effects.
/// These markers never become provider turn-state evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderTerminalControlSpec {
    pub composer_tail_lines: usize,
    /// Prefixes which identify this provider's active input composer. The
    /// control loop uses the last matching line so submitted prompt history
    /// is not mistaken for text that still remains in the composer.
    pub composer_start_markers: &'static [&'static str],
    pub paste_expand_guards: &'static [&'static str],
    pub collapsed_paste_markers: &'static [&'static str],
}

impl ProviderObservationSpec {
    pub fn hook(&self, event: &str) -> Option<&ProviderHookSpec> {
        self.hooks.iter().find(|hook| hook.event == event)
    }

    pub fn materialized_hook_events(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.hooks
            .iter()
            .filter(|hook| hook.materialized_by_ah)
            .map(|hook| hook.event)
    }

    pub fn uses_terminal_pane_for_turn_state(&self) -> bool {
        self.working_sources
            .contains(&ObservationSourceSpec::TerminalPane)
            || self
                .completion_sources
                .contains(&ObservationSourceSpec::TerminalPane)
    }
}

/// Stable Port between provider-neutral Run semantics and one interactive CLI.
///
/// Adapters parse provider-native facts into neutral observations. They do not
/// decide authoritative Task/Attempt state and must not write `agents.state`.
pub trait ProviderAdapter: Sync {
    fn name(&self) -> &'static str;

    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    fn manifest(&self) -> ProviderManifest;

    fn auth_spec(&self) -> &'static ProviderAuthSpec;

    fn observation_spec(&self) -> &'static ProviderObservationSpec;

    fn terminal_control_spec(&self) -> &'static ProviderTerminalControlSpec;

    fn recovery_args(&self, _sandbox_home: &Path) -> Vec<String> {
        Vec::new()
    }

    fn recovery_supported(&self) -> bool {
        false
    }

    fn cancel_keysyms(&self) -> &'static [&'static str] {
        &["C-c"]
    }

    fn hook_timeout_secs(&self) -> u64 {
        5
    }

    fn rules_target(&self, _home_root: &Path) -> Option<PathBuf> {
        None
    }

    fn transcript_root(&self, _home_root: &Path) -> Option<PathBuf> {
        None
    }

    fn transcript_file_matches(&self, _path: &Path) -> bool {
        false
    }

    fn transcript_requires_user_entry(&self) -> bool {
        false
    }

    fn parse_transcript_value(&self, _value: &Value) -> LogParseResult {
        LogParseResult::NotTerminal
    }

    fn transcript_has_assistant_progress(&self, _value: &Value) -> bool {
        false
    }

    fn transcript_has_pending_work(&self, _transcript: &[u8]) -> bool {
        false
    }

    fn completion_is_deferred(&self, _transcript: &[u8], _line_end: usize) -> bool {
        false
    }

    fn classify_terminality(
        &self,
        _candidate_reply: &str,
        _has_pending_tasks: Option<bool>,
    ) -> CompletionTerminality {
        CompletionTerminality::Terminal
    }
}
