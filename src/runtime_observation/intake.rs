//! Single intake Port for provider-exposed runtime observations.
//!
//! Hooks and transcript readers submit facts here. This Port owns source
//! normalization and delegates the legacy atomic projection transaction while
//! that transaction is extracted from `db::state_machine`.

use super::EvidenceSource;
use crate::db;
use crate::error::AhError;

#[derive(Clone)]
pub(crate) struct WorkingObservation {
    pub db: db::Db,
    pub agent_id: String,
    pub provider: String,
    pub expected_lifecycle_id: String,
    pub source: EvidenceSource,
    pub evidence_id: String,
    pub provider_turn_id: Option<String>,
    pub prompt_fingerprint: Option<String>,
}

#[derive(Clone)]
pub(crate) struct TranscriptCompletionObservation {
    pub db: db::Db,
    pub agent_id: String,
    pub provider: String,
    pub reply: Option<String>,
    pub raw_path: String,
    pub raw_offset: u64,
    pub provider_turn_id: Option<String>,
    pub expected_lifecycle_id: String,
    pub prompt_fingerprint: Option<String>,
}

#[derive(Clone)]
pub(crate) struct HookCompletionObservation {
    pub db: db::Db,
    pub agent_id: String,
    pub provider: String,
    pub hook_event: String,
    pub event_id: Option<String>,
    pub reply: Option<String>,
    pub expected_lifecycle_id: Option<String>,
}

pub(crate) async fn observe_working(
    observation: WorkingObservation,
) -> Result<db::state_machine::ProviderActivityOutcome, AhError> {
    db::state_machine::mark_agent_working_provider_event(
        observation.db,
        observation.agent_id,
        observation.provider,
        observation.expected_lifecycle_id,
        observation.source,
        observation.evidence_id,
        observation.provider_turn_id,
        observation.prompt_fingerprint,
    )
    .await
}

pub(crate) async fn observe_transcript_completion(
    observation: TranscriptCompletionObservation,
) -> Result<(usize, Option<String>), AhError> {
    db::state_machine::mark_agent_idle_log_event(
        observation.db,
        observation.agent_id,
        observation.provider,
        observation.reply,
        observation.raw_path,
        observation.raw_offset,
        observation.provider_turn_id,
        observation.expected_lifecycle_id,
        observation.prompt_fingerprint,
    )
    .await
}

pub(crate) async fn observe_hook_completion(
    observation: HookCompletionObservation,
) -> Result<(usize, Option<String>), AhError> {
    db::state_machine::mark_agent_idle_hook_event(
        observation.db,
        observation.agent_id,
        observation.provider,
        observation.hook_event,
        observation.event_id,
        observation.reply,
        observation.expected_lifecycle_id,
    )
    .await
}
