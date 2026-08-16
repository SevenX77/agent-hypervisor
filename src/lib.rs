pub mod agent_io;
pub mod agent_program_control;
pub mod claude_gateway;
pub mod cli;
pub mod completion;
pub mod db;
pub mod env;
pub mod error;
pub mod guarded_action;
pub mod home_materialization;
pub mod lifecycle;
pub mod marker;
pub mod master_cutover;
pub mod master_revival;
pub mod monitor;
pub mod orchestrator;
pub mod outbox;
pub mod pane_diff;
pub mod platform;
pub(crate) mod process_identity;
pub mod prompt_delivery;
pub mod prompt_handler;
pub mod provider;
pub mod resource_metering;
pub mod rpc;
pub mod runtime_events;
pub mod runtime_observation;
pub mod sandbox;
pub mod state_layout;
pub mod systemd_unit;
pub mod tmux;
pub mod work_coordination;

/// Compatibility alias for the governed Work Execution implementation, where
/// SQLite is a private storage mechanism rather than the semantic state owner.
#[doc(hidden)]
pub use db as storage;
