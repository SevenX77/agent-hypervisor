//! Guarded progress through one interactive CLI lifecycle.
//!
//! This is an ephemeral coordinator, not another status owner. Persisted
//! provider state remains owned by runtime observations and the database state
//! machine. The coordinator merely prevents a caller from advancing its local
//! procedure before the prior phase has causal evidence.

use crate::db::{self as db, Db};
use crate::error::CcbdError;
use crate::provider::{ObservationSourceSpec, adapter};
use crate::runtime_observation::{
    EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderTurnState,
};
use std::time::{Duration, Instant};

const DEFAULT_TASK_START_TIMEOUT: Duration = Duration::from_secs(10);
const TASK_START_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliLifecyclePhase {
    Starting,
    WaitingForStandby,
    Standby,
    DeliveringPrompt,
    WaitingForTaskStart,
    Working,
    WaitingForTaskEnd,
    Failed,
}

#[derive(Debug, Clone)]
pub struct CliLifecycleProgress {
    phase: CliLifecyclePhase,
}

impl CliLifecycleProgress {
    pub fn new(phase: CliLifecyclePhase) -> Self {
        Self { phase }
    }

    pub fn phase(&self) -> CliLifecyclePhase {
        self.phase
    }

    pub fn confirm(&mut self, next: CliLifecyclePhase) -> Result<(), CcbdError> {
        if !allowed_transition(self.phase, next) {
            return Err(CcbdError::PtyIoError(format!(
                "unconfirmed CLI lifecycle transition {:?} -> {:?}",
                self.phase, next
            )));
        }
        self.phase = next;
        Ok(())
    }
}

fn allowed_transition(from: CliLifecyclePhase, to: CliLifecyclePhase) -> bool {
    use CliLifecyclePhase::*;
    matches!(
        (from, to),
        (Starting, WaitingForStandby)
            | (WaitingForStandby, Standby)
            | (Standby, DeliveringPrompt)
            | (DeliveringPrompt, WaitingForTaskStart)
            | (WaitingForTaskStart, Working)
            | (Working, WaitingForTaskEnd)
            | (WaitingForTaskEnd, Standby)
    ) || to == Failed
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStartEvidence {
    pub observation_id: String,
    pub source: EvidenceSource,
    pub observed_at_ms: i64,
}

pub async fn await_task_started(
    db: Db,
    agent_id: &str,
    provider: &str,
    lifecycle_id: &str,
    job_id: &str,
) -> Result<TaskStartEvidence, CcbdError> {
    let timeout = std::env::var("AH_TASK_START_CONFIRM_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_TASK_START_TIMEOUT);
    await_task_started_with_timeout(db, agent_id, provider, lifecycle_id, job_id, timeout).await
}

async fn await_task_started_with_timeout(
    db: Db,
    agent_id: &str,
    provider: &str,
    lifecycle_id: &str,
    job_id: &str,
    timeout: Duration,
) -> Result<TaskStartEvidence, CcbdError> {
    let provider_adapter = adapter(provider).ok_or_else(|| CcbdError::EnvironmentNotSupported {
        details: format!("unknown provider {provider:?}"),
    })?;
    let spec = provider_adapter.observation_spec();
    let deadline = Instant::now() + timeout;

    loop {
        let database = db.clone();
        let agent_id_owned = agent_id.to_string();
        let lifecycle_id_owned = lifecycle_id.to_string();
        let job_id_owned = job_id.to_string();
        let observations = db::common::spawn_db("agent_runtime::await_task_started", move || {
            let conn = database.conn();
            crate::runtime_observation::store::query_scope_sync(
                &conn,
                &agent_id_owned,
                &lifecycle_id_owned,
                Some(&job_id_owned),
            )
        })
        .await?;

        if let Some(observation) = observations
            .iter()
            .rev()
            .find(|observation| is_confirmed_working(spec.working_sources, observation))
        {
            return Ok(TaskStartEvidence {
                observation_id: observation.observation_id.clone(),
                source: observation.source,
                observed_at_ms: observation.observed_at_ms,
            });
        }
        if Instant::now() >= deadline {
            return Err(CcbdError::PtyIoError(format!(
                "task start was not confirmed for agent {agent_id} job {job_id} provider {provider} within {timeout:?}"
            )));
        }
        tokio::time::sleep(TASK_START_POLL.min(deadline.saturating_duration_since(Instant::now())))
            .await;
    }
}

fn is_confirmed_working(
    sources: &[ObservationSourceSpec],
    observation: &ProviderObservation,
) -> bool {
    matches!(
        observation.kind,
        ProviderObservationKind::Turn(
            ProviderTurnState::Working
                | ProviderTurnState::AwaitingApproval
                | ProviderTurnState::AwaitingUser
                | ProviderTurnState::Completed
                | ProviderTurnState::Failed
        )
    ) && source_is_declared(sources, observation.source)
}

fn source_is_declared(sources: &[ObservationSourceSpec], source: EvidenceSource) -> bool {
    match source {
        EvidenceSource::OfficialHook => sources
            .iter()
            .any(|candidate| matches!(candidate, ObservationSourceSpec::OfficialHook(_))),
        EvidenceSource::Transcript => sources
            .iter()
            .any(|candidate| matches!(candidate, ObservationSourceSpec::Transcript(_))),
        EvidenceSource::ProcessProbe => sources.contains(&ObservationSourceSpec::ProcessProbe),
        EvidenceSource::TerminalPane => sources.contains(&ObservationSourceSpec::TerminalPane),
        EvidenceSource::OfficialEvent
        | EvidenceSource::CorrelatedCallback
        | EvidenceSource::ControlPlane
        | EvidenceSource::LegacyDatabase => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database(provider: &str) -> (tempfile::NamedTempFile, Db, String) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let database = crate::db::init(file.path()).unwrap();
        {
            let conn = database.conn();
            conn.execute(
                "INSERT INTO projects (id, absolute_path) VALUES ('p1', '/p1')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1', 'p1', 1)",
                [],
            )
            .unwrap();
            crate::db::agents::insert_agent_sync(
                &conn,
                "a1",
                "s1",
                provider,
                crate::db::state_machine::STATE_WAITING_FOR_ACK,
                Some(10),
            )
            .unwrap();
        }
        let lifecycle_id = database
            .conn()
            .query_row(
                "SELECT lifecycle_id FROM agents WHERE id = 'a1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        (file, database, lifecycle_id)
    }

    fn observation(provider: &str, source: EvidenceSource) -> ProviderObservation {
        ProviderObservation {
            observation_id: "working-1".into(),
            agent_id: "a1".into(),
            session_id: "s1".into(),
            provider: provider.into(),
            lifecycle_id: "life-1".into(),
            turn_id: Some("job-1".into()),
            source,
            observed_at_ms: 10,
            kind: ProviderObservationKind::Turn(ProviderTurnState::Working),
        }
    }

    #[test]
    fn lifecycle_refuses_to_skip_unconfirmed_phases() {
        let mut lifecycle = CliLifecycleProgress::new(CliLifecyclePhase::Standby);
        assert!(lifecycle.confirm(CliLifecyclePhase::Working).is_err());
        lifecycle
            .confirm(CliLifecyclePhase::DeliveringPrompt)
            .unwrap();
        lifecycle
            .confirm(CliLifecyclePhase::WaitingForTaskStart)
            .unwrap();
        lifecycle.confirm(CliLifecyclePhase::Working).unwrap();
    }

    #[test]
    fn codex_pane_change_cannot_confirm_task_start_but_hook_can() {
        let sources = adapter("codex").unwrap().observation_spec().working_sources;
        assert!(!is_confirmed_working(
            sources,
            &observation("codex", EvidenceSource::TerminalPane)
        ));
        assert!(is_confirmed_working(
            sources,
            &observation("codex", EvidenceSource::OfficialHook)
        ));
    }

    #[test]
    fn bash_declares_terminal_fallback_for_task_start() {
        let sources = adapter("bash").unwrap().observation_spec().working_sources;
        assert!(is_confirmed_working(
            sources,
            &observation("bash", EvidenceSource::TerminalPane)
        ));
    }

    #[tokio::test]
    async fn await_task_start_accepts_correlated_codex_hook_observation() {
        let (_file, database, lifecycle_id) = database("codex");
        crate::runtime_observation::store::append_for_agent_sync(
            &database.conn(),
            "hook-working",
            "a1",
            &lifecycle_id,
            Some("job-1"),
            EvidenceSource::OfficialHook,
            ProviderObservationKind::Turn(ProviderTurnState::Working),
            10,
        )
        .unwrap();

        let evidence = await_task_started_with_timeout(
            database,
            "a1",
            "codex",
            &lifecycle_id,
            "job-1",
            Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(evidence.observation_id, "hook-working");
        assert_eq!(evidence.source, EvidenceSource::OfficialHook);
    }

    #[tokio::test]
    async fn await_task_start_rejects_codex_pane_only_observation() {
        let (_file, database, lifecycle_id) = database("codex");
        crate::runtime_observation::store::append_for_agent_sync(
            &database.conn(),
            "pane-working",
            "a1",
            &lifecycle_id,
            Some("job-1"),
            EvidenceSource::TerminalPane,
            ProviderObservationKind::Turn(ProviderTurnState::Working),
            10,
        )
        .unwrap();

        let result = await_task_started_with_timeout(
            database,
            "a1",
            "codex",
            &lifecycle_id,
            "job-1",
            Duration::ZERO,
        )
        .await;

        assert!(result.is_err());
    }
}
