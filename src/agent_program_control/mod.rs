//! Provider-neutral, version-bound Agent Program identity.
//!
//! Provider home materialization and terminal interaction are Agent Runtime
//! adapters. This Module owns only the immutable program record consumed by a
//! Run.

mod program_version;

pub use program_version::{ProgramVersion, ProgramVersionError};
