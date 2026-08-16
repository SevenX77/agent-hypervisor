use super::{
    ObservationSourceSpec, ProviderAdapter, ProviderAuthSpec, ProviderLoginDriverSpec,
    ProviderObservationSpec, ProviderTerminalControlSpec,
};
use crate::provider::manifest::{
    CompletionSignalKind, ENV_PASSTHROUGH, IdleDetectionMode, InitProbeKind, ProviderCapabilities,
    ProviderManifest,
};

pub struct BashAdapter;
pub static BASH: BashAdapter = BashAdapter;

static AUTH_SPEC: ProviderAuthSpec = ProviderAuthSpec {
    assessed_cli: "GNU Bash 5.2.21 / no authentication",
    documentation_url: None,
    stores: &[],
    login: ProviderLoginDriverSpec::None,
    status_argv: None,
    config_dir_env: None,
};

static TERMINAL_CONTROL_SPEC: ProviderTerminalControlSpec = ProviderTerminalControlSpec {
    composer_tail_lines: 40,
    composer_start_markers: &["$"],
    paste_expand_guards: &[],
    collapsed_paste_markers: &[],
};

const WORKING_SOURCES: &[ObservationSourceSpec] = &[
    ObservationSourceSpec::ProcessProbe,
    ObservationSourceSpec::TerminalPane,
];
const COMPLETION_SOURCES: &[ObservationSourceSpec] = &[ObservationSourceSpec::TerminalPane];

static OBSERVATION_SPEC: ProviderObservationSpec = ProviderObservationSpec {
    assessed_cli: "GNU Bash 5.2.21 / interactive",
    hooks: &[],
    hook_config_path: None,
    hook_config_schema: None,
    hooks_enabled_by_default: false,
    working_sources: WORKING_SOURCES,
    completion_sources: COMPLETION_SOURCES,
    real_hook_delivery_verified: false,
    real_hook_delivery_evidence: None,
};

const INJECTED_ENV: &[(&str, &str)] = &[("PS1", "$ ")];

impl ProviderAdapter for BashAdapter {
    fn name(&self) -> &'static str {
        "bash"
    }

    fn manifest(&self) -> ProviderManifest {
        ProviderManifest {
            provider_name: self.name(),
            auth_mount_paths: vec![],
            command: &["bash", "--noprofile", "--norc", "-i"],
            resume_args: &[],
            env_passthrough: ENV_PASSTHROUGH,
            injected_env_vars: INJECTED_ENV,
            readiness_timeout_s: 10,
            requires_home_materialization: false,
            init_probe: InitProbeKind::Bash,
            idle_detection_mode: IdleDetectionMode::LineEndRegex,
            stability_ms: 0,
            idle_anti_pattern: "",
            completion_signal: CompletionSignalKind::LogOnly,
            capabilities: ProviderCapabilities {
                rules_target: false,
                completion_signal: false,
                readiness_ack: false,
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
}
