//! Backward-compatible provider home-layout surface.
//!
//! Work Execution now owns home materialization as part of the agent runtime.
//! The public `ah::provider::home_layout` path remains available so existing
//! integrations continue to compile without carrying two implementations.

pub use crate::home_materialization::*;
