//! Provider-neutral runtime observations and truthful status derivation.
//!
//! Provider Adapters append fenced facts here. They never decide Task/Attempt
//! state and never mutate provider status directly.

pub(crate) mod intake;
mod model;
mod reducer;
pub(crate) mod store;

pub use model::{
    EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderOccupancy,
    ProviderProcessState, ProviderStatus, ProviderStatusError, ProviderStatusInput,
    ProviderTurnState, ResolvedDimension, prompt_fingerprint,
};
pub use reducer::reduce_provider_status;

#[cfg(test)]
mod tests;
