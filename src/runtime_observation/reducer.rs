use super::{
    ProviderObservation, ProviderObservationKind, ProviderOccupancy, ProviderProcessState,
    ProviderStatus, ProviderStatusError, ProviderStatusInput, ProviderTurnState, ResolvedDimension,
};
use std::fmt::Debug;

pub fn reduce_provider_status(
    input: &ProviderStatusInput,
) -> Result<ProviderStatus, ProviderStatusError> {
    let provider = input.provider.trim();
    if provider.is_empty() {
        return Err(ProviderStatusError::InvalidProviderIdentity(
            input.provider.clone(),
        ));
    }
    if input.freshness_ms < 0 {
        return Err(ProviderStatusError::InvalidFreshness(input.freshness_ms));
    }

    validate_observation_identities(input, provider)?;

    let process = resolve_dimension(input, "process", |observation| match observation.kind {
        ProviderObservationKind::Process(state) => Some(state),
        ProviderObservationKind::Turn(_) => None,
    });
    let turn = resolve_dimension(input, "turn", |observation| match observation.kind {
        ProviderObservationKind::Turn(state) => Some(state),
        ProviderObservationKind::Process(_) => None,
    });
    let occupancy = derive_occupancy(&process, &turn);

    Ok(ProviderStatus {
        agent_id: input.agent_id.clone(),
        session_id: input.session_id.clone(),
        provider: provider.to_owned(),
        lifecycle_id: input.lifecycle_id.clone(),
        turn_id: input.turn_id.clone(),
        process,
        turn,
        occupancy,
    })
}

fn validate_observation_identities(
    input: &ProviderStatusInput,
    provider: &str,
) -> Result<(), ProviderStatusError> {
    for (index, observation) in input.observations.iter().enumerate() {
        validate_field(index, "agent_id", &input.agent_id, &observation.agent_id)?;
        validate_field(
            index,
            "session_id",
            &input.session_id,
            &observation.session_id,
        )?;

        validate_field(index, "provider", provider, observation.provider.trim())?;
        validate_field(
            index,
            "lifecycle_id",
            &input.lifecycle_id,
            &observation.lifecycle_id,
        )?;
        let expected_turn_id = match observation.kind {
            ProviderObservationKind::Process(_) => None,
            ProviderObservationKind::Turn(_) => input.turn_id.as_deref(),
        };
        validate_optional_field(
            index,
            "turn_id",
            expected_turn_id,
            observation.turn_id.as_deref(),
        )?;
    }
    Ok(())
}

fn validate_optional_field(
    observation_index: usize,
    field: &'static str,
    expected: Option<&str>,
    actual: Option<&str>,
) -> Result<(), ProviderStatusError> {
    if expected == actual {
        return Ok(());
    }
    Err(ProviderStatusError::IdentityMismatch {
        observation_index,
        field,
        expected: expected.unwrap_or("<none>").to_owned(),
        actual: actual.unwrap_or("<none>").to_owned(),
    })
}

fn validate_field(
    observation_index: usize,
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), ProviderStatusError> {
    if expected == actual {
        return Ok(());
    }
    Err(ProviderStatusError::IdentityMismatch {
        observation_index,
        field,
        expected: expected.to_owned(),
        actual: actual.to_owned(),
    })
}

fn resolve_dimension<T, F>(
    input: &ProviderStatusInput,
    dimension: &str,
    value_of: F,
) -> ResolvedDimension<T>
where
    T: Copy + Debug + Eq,
    F: Fn(&ProviderObservation) -> Option<T>,
{
    let candidates = input
        .observations
        .iter()
        .filter_map(|observation| value_of(observation).map(|value| (observation, value)))
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return ResolvedDimension::Unknown {
            reason: format!("no {dimension} evidence"),
        };
    }

    if candidates
        .iter()
        .any(|(observation, _)| observation.observed_at_ms > input.now_ms)
    {
        return ResolvedDimension::Conflicted {
            reason: format!("{dimension} evidence is timestamped in the future"),
        };
    }

    let (fresh, stale): (Vec<_>, Vec<_>) = candidates.into_iter().partition(|(observation, _)| {
        input.now_ms - observation.observed_at_ms <= input.freshness_ms
    });

    if fresh.is_empty() {
        return ResolvedDimension::Unknown {
            reason: format!("all {dimension} evidence is stale"),
        };
    }

    let strongest_fresh = fresh
        .iter()
        .map(|(observation, _)| observation.source.precedence())
        .max()
        .expect("fresh evidence is non-empty");

    if let Some(stronger_stale) = stale
        .iter()
        .map(|(observation, _)| observation.source)
        .filter(|source| source.precedence() > strongest_fresh)
        .max_by_key(|source| source.precedence())
    {
        return ResolvedDimension::Unknown {
            reason: format!(
                "stronger {stronger_stale:?} {dimension} evidence is stale; weaker evidence cannot replace it"
            ),
        };
    }

    let strongest = fresh
        .into_iter()
        .filter(|(observation, _)| observation.source.precedence() == strongest_fresh)
        .collect::<Vec<_>>();
    let latest_at = strongest
        .iter()
        .map(|(observation, _)| observation.observed_at_ms)
        .max()
        .expect("strongest evidence is non-empty");
    let latest = strongest
        .into_iter()
        .filter(|(observation, _)| observation.observed_at_ms == latest_at)
        .collect::<Vec<_>>();
    let first_value = latest[0].1;

    if latest.iter().any(|(_, value)| *value != first_value) {
        return ResolvedDimension::Conflicted {
            reason: format!("equally strong {dimension} evidence disagrees at {latest_at}"),
        };
    }

    ResolvedDimension::Known {
        value: first_value,
        source: latest[0].0.source,
        observed_at_ms: latest_at,
    }
}

fn derive_occupancy(
    process: &ResolvedDimension<ProviderProcessState>,
    turn: &ResolvedDimension<ProviderTurnState>,
) -> ProviderOccupancy {
    if process.is_conflicted() || turn.is_conflicted() {
        return ProviderOccupancy::Conflicted;
    }

    let Some(process) = process.known_value().copied() else {
        return ProviderOccupancy::Unknown;
    };
    let turn = turn.known_value().copied();

    match process {
        ProviderProcessState::Alive => match turn {
            Some(
                ProviderTurnState::Queued
                | ProviderTurnState::Delivering
                | ProviderTurnState::Delivered
                | ProviderTurnState::Working
                | ProviderTurnState::AwaitingApproval
                | ProviderTurnState::AwaitingUser
                | ProviderTurnState::Cancelling
                | ProviderTurnState::Stalled,
            ) => ProviderOccupancy::Occupied,
            Some(
                ProviderTurnState::Ready | ProviderTurnState::Completed | ProviderTurnState::Failed,
            ) => ProviderOccupancy::Available,
            None => ProviderOccupancy::Unknown,
        },
        ProviderProcessState::Starting => ProviderOccupancy::Unavailable,
        ProviderProcessState::Exited => match turn {
            Some(turn) if turn.is_occupied() => ProviderOccupancy::Conflicted,
            _ => ProviderOccupancy::Unavailable,
        },
        ProviderProcessState::Unreachable => ProviderOccupancy::Unknown,
    }
}
