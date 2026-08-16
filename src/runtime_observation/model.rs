use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;

/// The source of an observation, ordered from strongest to weakest.
///
/// Source strength is part of the status contract. A fresh weak heuristic may
/// fill a gap, but it cannot overwrite stronger evidence that is still valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSource {
    OfficialEvent,
    OfficialHook,
    CorrelatedCallback,
    ProcessProbe,
    Transcript,
    TerminalPane,
    ControlPlane,
    LegacyDatabase,
}

impl EvidenceSource {
    pub(crate) const fn precedence(self) -> u8 {
        match self {
            Self::OfficialEvent => 60,
            Self::OfficialHook => 50,
            Self::CorrelatedCallback => 40,
            Self::ProcessProbe => 30,
            Self::Transcript => 20,
            Self::TerminalPane => 10,
            Self::ControlPlane => 5,
            Self::LegacyDatabase => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProcessState {
    Starting,
    Alive,
    Exited,
    Unreachable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTurnState {
    Ready,
    Queued,
    Delivering,
    Delivered,
    Working,
    AwaitingApproval,
    AwaitingUser,
    Cancelling,
    Stalled,
    Completed,
    Failed,
}

impl ProviderTurnState {
    pub(crate) const fn is_occupied(self) -> bool {
        matches!(
            self,
            Self::Queued
                | Self::Delivering
                | Self::Delivered
                | Self::Working
                | Self::AwaitingApproval
                | Self::AwaitingUser
                | Self::Cancelling
                | Self::Stalled
        )
    }
}

/// Stable, content-free correlation for a prompt observed through a provider
/// hook or transcript. Terminal line endings are transport details and are not
/// part of the submitted prompt identity.
pub fn prompt_fingerprint(prompt: &str) -> String {
    let canonical = prompt.trim_end_matches(['\r', '\n']);
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "dimension", content = "state", rename_all = "snake_case")]
pub enum ProviderObservationKind {
    Process(ProviderProcessState),
    Turn(ProviderTurnState),
}

/// One provider observation scoped to an exact agent/session lifecycle.
///
/// `lifecycle_id` prevents a delayed callback from a previous process from
/// mutating the status of a restarted provider session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderObservation {
    /// Stable id used to make retries idempotent.
    pub observation_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub provider: String,
    pub lifecycle_id: String,
    /// Correlates turn evidence to one job. Process evidence must leave this
    /// empty because it describes the provider process rather than a turn.
    pub turn_id: Option<String>,
    pub source: EvidenceSource,
    pub observed_at_ms: i64,
    pub kind: ProviderObservationKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderStatusInput {
    pub agent_id: String,
    pub session_id: String,
    pub provider: String,
    pub lifecycle_id: String,
    pub turn_id: Option<String>,
    pub now_ms: i64,
    pub freshness_ms: i64,
    pub observations: Vec<ProviderObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case")]
pub enum ResolvedDimension<T> {
    Known {
        value: T,
        source: EvidenceSource,
        observed_at_ms: i64,
    },
    Unknown {
        reason: String,
    },
    Conflicted {
        reason: String,
    },
}

impl<T> ResolvedDimension<T> {
    pub(crate) fn known_value(&self) -> Option<&T> {
        match self {
            Self::Known { value, .. } => Some(value),
            Self::Unknown { .. } | Self::Conflicted { .. } => None,
        }
    }

    pub(crate) const fn is_conflicted(&self) -> bool {
        matches!(self, Self::Conflicted { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOccupancy {
    Available,
    Occupied,
    Unavailable,
    Unknown,
    Conflicted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderStatus {
    pub agent_id: String,
    pub session_id: String,
    pub provider: String,
    pub lifecycle_id: String,
    pub turn_id: Option<String>,
    pub process: ResolvedDimension<ProviderProcessState>,
    pub turn: ResolvedDimension<ProviderTurnState>,
    pub occupancy: ProviderOccupancy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderStatusError {
    InvalidProviderIdentity(String),
    InvalidFreshness(i64),
    IdentityMismatch {
        observation_index: usize,
        field: &'static str,
        expected: String,
        actual: String,
    },
}

impl fmt::Display for ProviderStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProviderIdentity(provider) => {
                write!(
                    formatter,
                    "provider identity must not be empty, got {provider:?}"
                )
            }
            Self::InvalidFreshness(value) => {
                write!(formatter, "freshness_ms must be non-negative, got {value}")
            }
            Self::IdentityMismatch {
                observation_index,
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "observation {observation_index} has mismatched {field}: expected {expected:?}, got {actual:?}"
            ),
        }
    }
}

impl Error for ProviderStatusError {}
