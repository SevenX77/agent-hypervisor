pub mod auth_flow;
pub mod auth_store;
mod auth_ui;
pub mod builtin;
pub mod bundles;
pub mod extensions;
pub mod fingerprint;
pub mod health_check;
pub mod home_layout;
pub mod init_probe;
pub mod init_probe_task;
pub mod plugins;
pub mod skills;

mod antigravity;
mod bash;
mod claude;
mod codex;
mod contract;
pub mod manifest;

pub use contract::{
    AuthChallengeKind, AuthStoreSpec, HookStatusUse, ObservationSourceSpec, ProviderAdapter,
    ProviderAuthSpec, ProviderAuthStoreSpec, ProviderHookSpec, ProviderLoginDriverSpec,
    ProviderObservationSpec, ProviderPromptKind, ProviderTerminalControlSpec,
};

use antigravity::ANTIGRAVITY;
use bash::BASH;
use claude::CLAUDE;
use codex::CODEX;

pub const PROVIDER_NAMES: &[&str] = &["bash", "codex", "claude", "antigravity"];

static ADAPTERS: [&dyn ProviderAdapter; 4] = [&BASH, &CODEX, &CLAUDE, &ANTIGRAVITY];

pub fn adapters() -> &'static [&'static dyn ProviderAdapter] {
    &ADAPTERS
}

pub fn adapter(raw: &str) -> Option<&'static dyn ProviderAdapter> {
    ADAPTERS
        .iter()
        .copied()
        .find(|adapter| adapter.name() == raw || adapter.aliases().contains(&raw))
}

pub fn canonical_name(raw: &str) -> Option<&'static str> {
    adapter(raw).map(ProviderAdapter::name)
}

#[cfg(test)]
mod tests {
    use super::{PROVIDER_NAMES, ProviderPromptKind, adapter, adapters};
    use crate::runtime_observation::{
        EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderOccupancy,
        ProviderProcessState, ProviderStatusInput, ProviderTurnState, reduce_provider_status,
    };
    use std::collections::BTreeSet;

    #[test]
    fn every_public_provider_has_one_adapter_and_local_observation_contract() {
        assert_eq!(adapters().len(), PROVIDER_NAMES.len());
        let names = adapters()
            .iter()
            .map(|adapter| adapter.name())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), PROVIDER_NAMES.len());

        for name in PROVIDER_NAMES {
            let adapter = adapter(name).expect("registered provider");
            assert_eq!(adapter.name(), *name);
            assert_eq!(adapter.manifest().provider_name, *name);
            assert!(!adapter.auth_spec().assessed_cli.is_empty());
            assert!(!adapter.observation_spec().assessed_cli.is_empty());
            assert!(adapter.terminal_control_spec().composer_tail_lines > 0);
        }
    }

    #[test]
    fn every_external_provider_owns_a_documented_headless_auth_route() {
        for provider in ["codex", "claude", "antigravity"] {
            let spec = adapter(provider).unwrap().auth_spec();
            assert!(
                spec.documentation_url
                    .is_some_and(|url| url.starts_with("https://")),
                "{provider} auth spec must link the upstream manual"
            );
            assert!(spec.login_argv(true).is_some(), "{provider}");
            assert!(!spec.stores.is_empty(), "{provider}");
        }

        let bash = adapter("bash").unwrap().auth_spec();
        assert!(bash.documentation_url.is_none());
        assert!(bash.login_argv(true).is_none());
    }

    #[test]
    fn antigravity_alias_resolves_at_the_adapter_boundary() {
        assert_eq!(
            adapter("gemini").map(|adapter| adapter.name()),
            Some("antigravity")
        );
        assert!(adapter("Gemini").is_none());
    }

    #[test]
    fn only_the_shell_adapter_treats_prompts_as_executable_source() {
        assert_eq!(
            adapter("bash").unwrap().prompt_kind(),
            ProviderPromptKind::ShellCommand
        );
        for provider in ["codex", "claude", "antigravity"] {
            assert_eq!(
                adapter(provider).unwrap().prompt_kind(),
                ProviderPromptKind::NaturalLanguage,
                "{provider}"
            );
        }
    }

    #[test]
    fn provider_hook_inventory_links_the_external_manual_and_declares_consumed_fields() {
        for provider in ["codex", "claude", "antigravity"] {
            let spec = adapter(provider).unwrap().observation_spec();
            assert!(!spec.hooks.is_empty());
            assert!(spec.hooks.iter().any(|hook| hook.materialized_by_ah));
            for hook in spec.hooks {
                assert!(!hook.event.is_empty());
                assert!(!hook.trigger.is_empty());
                assert!(hook.documentation_url.starts_with("https://"));
                if hook.materialized_by_ah {
                    assert!(!hook.payload_fields_used_by_ah.is_empty());
                }
            }
            assert!(!spec.hooks_enabled_by_default);
            assert!(spec.hook_config_path.is_some());
            assert!(spec.hook_config_schema.is_some());
            assert!(spec.real_hook_delivery_verified);
            assert!(spec.real_hook_delivery_evidence.is_some());
        }

        let bash = adapter("bash").unwrap().observation_spec();
        assert!(bash.hooks.is_empty());
        assert!(bash.hook_config_path.is_none());
        assert!(bash.hook_config_schema.is_none());
        assert!(!bash.real_hook_delivery_verified);
        assert!(bash.real_hook_delivery_evidence.is_none());
    }

    #[test]
    fn every_registered_provider_obeys_the_same_status_contract() {
        for provider in PROVIDER_NAMES {
            let status = reduce_provider_status(&ProviderStatusInput {
                agent_id: "a1".into(),
                session_id: "s1".into(),
                provider: (*provider).into(),
                lifecycle_id: "life-1".into(),
                turn_id: Some("job-1".into()),
                now_ms: 100,
                freshness_ms: 1_000,
                observations: vec![
                    ProviderObservation {
                        observation_id: "process-alive".into(),
                        agent_id: "a1".into(),
                        session_id: "s1".into(),
                        provider: (*provider).into(),
                        lifecycle_id: "life-1".into(),
                        turn_id: None,
                        source: EvidenceSource::ProcessProbe,
                        observed_at_ms: 90,
                        kind: ProviderObservationKind::Process(ProviderProcessState::Alive),
                    },
                    ProviderObservation {
                        observation_id: "turn-working".into(),
                        agent_id: "a1".into(),
                        session_id: "s1".into(),
                        provider: (*provider).into(),
                        lifecycle_id: "life-1".into(),
                        turn_id: Some("job-1".into()),
                        source: EvidenceSource::OfficialHook,
                        observed_at_ms: 100,
                        kind: ProviderObservationKind::Turn(ProviderTurnState::Working),
                    },
                ],
            })
            .unwrap();
            assert_eq!(status.occupancy, ProviderOccupancy::Occupied, "{provider}");
        }
    }
}
