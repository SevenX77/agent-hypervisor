//! Work-state identity boundary.
//!
//! AH never invents Roadmap, Plan, Task, Attempt, Run, Context, Episode, or
//! Program identities. A coordinator may supply an already-persisted binding;
//! this module validates and canonicalizes that binding before the transport
//! can associate it with a provider Job. Provider observations and Job state
//! remain runtime facts and cannot advance the bound Task by themselves.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub const EXECUTION_BINDING_SCHEMA_VERSION: u64 = 1;

pub const EXECUTION_BINDING_FIELDS: &[&str] = &[
    "schema_version",
    "roadmap_stream",
    "roadmap_node_id",
    "plan_id",
    "plan_revision",
    "plan_step_id",
    "task_id",
    "attempt_id",
    "run_id",
    "context_id",
    "episode_id",
    "module_ref",
    "capability_refs",
    "target_spec_locator",
    "target_spec_revision",
    "work_phase",
    "physical_scope",
    "semantic_scope",
    "worktree_path",
    "program_revision",
    "topology_revision",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBinding {
    pub schema_version: u64,
    pub roadmap_stream: String,
    pub roadmap_node_id: String,
    pub plan_id: String,
    pub plan_revision: String,
    pub plan_step_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub run_id: String,
    pub context_id: String,
    pub episode_id: String,
    pub module_ref: String,
    pub capability_refs: Vec<String>,
    pub target_spec_locator: String,
    pub target_spec_revision: String,
    pub work_phase: String,
    pub physical_scope: Vec<String>,
    pub semantic_scope: Vec<String>,
    pub worktree_path: String,
    pub program_revision: String,
    pub topology_revision: String,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoordinationError {
    #[error("invalid execution binding: {0}")]
    InvalidBinding(String),
    #[error("cannot canonicalize execution binding: {0}")]
    Serialization(String),
}

impl ExecutionBinding {
    pub fn from_value(value: Value) -> Result<Self, CoordinationError> {
        let object = value.as_object().ok_or_else(|| {
            CoordinationError::InvalidBinding("governance_binding must be an object".into())
        })?;
        let expected = EXECUTION_BINDING_FIELDS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(CoordinationError::InvalidBinding(format!(
                "governance_binding fields differ: missing={:?}, unexpected={:?}",
                expected.difference(&actual).collect::<Vec<_>>(),
                actual.difference(&expected).collect::<Vec<_>>()
            )));
        }

        let binding = serde_json::from_value::<Self>(value).map_err(|error| {
            CoordinationError::InvalidBinding(format!("decode governance_binding: {error}"))
        })?;
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<(), CoordinationError> {
        if self.schema_version != EXECUTION_BINDING_SCHEMA_VERSION {
            return Err(CoordinationError::InvalidBinding(format!(
                "governance_binding.schema_version must be {EXECUTION_BINDING_SCHEMA_VERSION}"
            )));
        }

        for (field, value) in [
            ("roadmap_stream", &self.roadmap_stream),
            ("roadmap_node_id", &self.roadmap_node_id),
            ("plan_id", &self.plan_id),
            ("plan_revision", &self.plan_revision),
            ("plan_step_id", &self.plan_step_id),
            ("task_id", &self.task_id),
            ("attempt_id", &self.attempt_id),
            ("run_id", &self.run_id),
            ("context_id", &self.context_id),
            ("episode_id", &self.episode_id),
            ("module_ref", &self.module_ref),
            ("target_spec_locator", &self.target_spec_locator),
            ("target_spec_revision", &self.target_spec_revision),
            ("work_phase", &self.work_phase),
            ("worktree_path", &self.worktree_path),
            ("program_revision", &self.program_revision),
            ("topology_revision", &self.topology_revision),
        ] {
            if value.trim().is_empty() {
                return Err(CoordinationError::InvalidBinding(format!(
                    "governance_binding.{field} must be a non-empty string"
                )));
            }
        }

        for (field, values) in [
            ("capability_refs", &self.capability_refs),
            ("physical_scope", &self.physical_scope),
            ("semantic_scope", &self.semantic_scope),
        ] {
            if values.is_empty() || values.iter().any(|value| value.trim().is_empty()) {
                return Err(CoordinationError::InvalidBinding(format!(
                    "governance_binding.{field} must be a non-empty string array"
                )));
            }
        }
        Ok(())
    }

    /// Stable JSON is used for request idempotency and restart reconstruction.
    pub fn canonical_json(&self) -> Result<String, CoordinationError> {
        let value = serde_json::to_value(self)
            .map_err(|error| CoordinationError::Serialization(error.to_string()))?;
        let object = value.as_object().ok_or_else(|| {
            CoordinationError::Serialization("typed binding did not encode as an object".into())
        })?;
        let ordered = object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        serde_json::to_string(&ordered)
            .map_err(|error| CoordinationError::Serialization(error.to_string()))
    }

    pub fn admitted_attempt(&self) -> AdmittedAttempt {
        AdmittedAttempt {
            task_id: self.task_id.clone(),
            attempt_id: self.attempt_id.clone(),
            module_id: self.module_ref.clone(),
            specification_revision: self.target_spec_revision.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedAttempt {
    pub task_id: String,
    pub attempt_id: String,
    pub module_id: String,
    pub specification_revision: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_binding() -> Value {
        json!({
            "schema_version": 1,
            "roadmap_stream": "delivery",
            "roadmap_node_id": "NODE-1",
            "plan_id": "PLAN-1",
            "plan_revision": "sha256:plan",
            "plan_step_id": "STEP-1",
            "task_id": "TASK-1",
            "attempt_id": "ATTEMPT-1",
            "run_id": "RUN-1",
            "context_id": "CONTEXT-1",
            "episode_id": "EPISODE-1",
            "module_ref": "agent_runtime",
            "capability_refs": ["provider_dispatch"],
            "target_spec_locator": "module_tree/agent_runtime.md",
            "target_spec_revision": "sha256:spec",
            "work_phase": "implementation",
            "physical_scope": ["module_tree/agent_runtime"],
            "semantic_scope": ["agent_runtime.provider_dispatch"],
            "worktree_path": "/tmp/task-1",
            "program_revision": "sha256:program",
            "topology_revision": "sha256:topology"
        })
    }

    #[test]
    fn binding_round_trip_is_canonical_and_lossless() {
        let binding = ExecutionBinding::from_value(valid_binding()).unwrap();
        let first = binding.canonical_json().unwrap();
        let second = ExecutionBinding::from_value(serde_json::from_str(&first).unwrap())
            .unwrap()
            .canonical_json()
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(binding.admitted_attempt().attempt_id, "ATTEMPT-1");
    }

    #[test]
    fn binding_rejects_missing_unknown_and_empty_identity_fields() {
        let mut missing = valid_binding();
        missing.as_object_mut().unwrap().remove("context_id");
        assert!(ExecutionBinding::from_value(missing).is_err());

        let mut unknown = valid_binding();
        unknown["invented_identity"] = json!("NOPE");
        assert!(ExecutionBinding::from_value(unknown).is_err());

        let mut empty = valid_binding();
        empty["run_id"] = json!("  ");
        assert!(ExecutionBinding::from_value(empty).is_err());
    }
}
