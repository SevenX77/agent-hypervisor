use super::*;

fn observation(
    provider: &str,
    source: EvidenceSource,
    observed_at_ms: i64,
    kind: ProviderObservationKind,
) -> ProviderObservation {
    let turn_id = match kind {
        ProviderObservationKind::Process(_) => None,
        ProviderObservationKind::Turn(_) => Some("job-1".to_owned()),
    };
    ProviderObservation {
        observation_id: format!("observation-{provider}-{source:?}-{observed_at_ms}-{kind:?}"),
        agent_id: "agent-1".to_owned(),
        session_id: "session-1".to_owned(),
        provider: provider.to_owned(),
        lifecycle_id: "lifecycle-7".to_owned(),
        turn_id,
        source,
        observed_at_ms,
        kind,
    }
}

fn input(provider: &str, observations: Vec<ProviderObservation>) -> ProviderStatusInput {
    ProviderStatusInput {
        agent_id: "agent-1".to_owned(),
        session_id: "session-1".to_owned(),
        provider: provider.to_owned(),
        lifecycle_id: "lifecycle-7".to_owned(),
        turn_id: Some("job-1".to_owned()),
        now_ms: 1_000,
        freshness_ms: 100,
        observations,
    }
}

#[test]
fn provider_neutral_status_accepts_a_future_adapter_identity() {
    let provider = "future-provider";
    let status = reduce_provider_status(&input(
        provider,
        vec![
            observation(
                provider,
                EvidenceSource::ProcessProbe,
                990,
                ProviderObservationKind::Process(ProviderProcessState::Alive),
            ),
            observation(
                provider,
                EvidenceSource::OfficialHook,
                995,
                ProviderObservationKind::Turn(ProviderTurnState::Working),
            ),
        ],
    ))
    .unwrap();

    assert_eq!(status.provider, provider);
    assert_eq!(status.occupancy, ProviderOccupancy::Occupied);
}

#[test]
fn legacy_busy_without_process_evidence_does_not_claim_occupied() {
    let status = reduce_provider_status(&input(
        "codex",
        vec![observation(
            "codex",
            EvidenceSource::LegacyDatabase,
            999,
            ProviderObservationKind::Turn(ProviderTurnState::Working),
        )],
    ))
    .unwrap();

    assert_eq!(status.occupancy, ProviderOccupancy::Unknown);
}

#[test]
fn strong_completion_wins_over_newer_terminal_busy_heuristic() {
    let status = reduce_provider_status(&input(
        "claude",
        vec![
            observation(
                "claude",
                EvidenceSource::ProcessProbe,
                990,
                ProviderObservationKind::Process(ProviderProcessState::Alive),
            ),
            observation(
                "claude",
                EvidenceSource::OfficialHook,
                980,
                ProviderObservationKind::Turn(ProviderTurnState::Completed),
            ),
            observation(
                "claude",
                EvidenceSource::TerminalPane,
                999,
                ProviderObservationKind::Turn(ProviderTurnState::Working),
            ),
        ],
    ))
    .unwrap();

    assert_eq!(status.occupancy, ProviderOccupancy::Available);
    assert!(matches!(
        status.turn,
        ResolvedDimension::Known {
            value: ProviderTurnState::Completed,
            source: EvidenceSource::OfficialHook,
            ..
        }
    ));
}

#[test]
fn equally_strong_simultaneous_disagreement_is_conflicted() {
    let status = reduce_provider_status(&input(
        "antigravity",
        vec![
            observation(
                "antigravity",
                EvidenceSource::ProcessProbe,
                990,
                ProviderObservationKind::Process(ProviderProcessState::Alive),
            ),
            observation(
                "antigravity",
                EvidenceSource::OfficialHook,
                995,
                ProviderObservationKind::Turn(ProviderTurnState::Working),
            ),
            observation(
                "antigravity",
                EvidenceSource::OfficialHook,
                995,
                ProviderObservationKind::Turn(ProviderTurnState::Completed),
            ),
        ],
    ))
    .unwrap();

    assert_eq!(status.occupancy, ProviderOccupancy::Conflicted);
    assert!(matches!(status.turn, ResolvedDimension::Conflicted { .. }));
}

#[test]
fn stale_strong_evidence_blocks_a_weak_state_upgrade() {
    let mut status_input = input(
        "codex",
        vec![
            observation(
                "codex",
                EvidenceSource::ProcessProbe,
                990,
                ProviderObservationKind::Process(ProviderProcessState::Alive),
            ),
            observation(
                "codex",
                EvidenceSource::OfficialHook,
                800,
                ProviderObservationKind::Turn(ProviderTurnState::Working),
            ),
            observation(
                "codex",
                EvidenceSource::TerminalPane,
                999,
                ProviderObservationKind::Turn(ProviderTurnState::Completed),
            ),
        ],
    );
    status_input.freshness_ms = 100;
    let status = reduce_provider_status(&status_input).unwrap();

    assert_eq!(status.occupancy, ProviderOccupancy::Unknown);
    assert!(matches!(status.turn, ResolvedDimension::Unknown { .. }));
}

#[test]
fn delayed_observation_from_an_old_lifecycle_is_rejected() {
    let mut delayed = observation(
        "codex",
        EvidenceSource::OfficialHook,
        999,
        ProviderObservationKind::Turn(ProviderTurnState::Completed),
    );
    delayed.lifecycle_id = "lifecycle-6".to_owned();

    let error = reduce_provider_status(&input("codex", vec![delayed])).unwrap_err();

    assert!(matches!(
        error,
        ProviderStatusError::IdentityMismatch {
            field: "lifecycle_id",
            ..
        }
    ));
}

#[test]
fn alive_turn_claim_for_an_exited_process_is_conflicted() {
    let status = reduce_provider_status(&input(
        "claude",
        vec![
            observation(
                "claude",
                EvidenceSource::ProcessProbe,
                999,
                ProviderObservationKind::Process(ProviderProcessState::Exited),
            ),
            observation(
                "claude",
                EvidenceSource::OfficialHook,
                998,
                ProviderObservationKind::Turn(ProviderTurnState::Working),
            ),
        ],
    ))
    .unwrap();

    assert_eq!(status.occupancy, ProviderOccupancy::Conflicted);
}

#[test]
fn completion_from_a_previous_turn_is_rejected() {
    let mut previous_turn = observation(
        "codex",
        EvidenceSource::OfficialHook,
        999,
        ProviderObservationKind::Turn(ProviderTurnState::Completed),
    );
    previous_turn.turn_id = Some("job-previous".to_owned());

    let error = reduce_provider_status(&input("codex", vec![previous_turn])).unwrap_err();

    assert!(matches!(
        error,
        ProviderStatusError::IdentityMismatch {
            field: "turn_id",
            ..
        }
    ));
}
