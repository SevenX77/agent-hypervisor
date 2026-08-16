use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramVersion {
    pub program_id: String,
    pub revision: String,
    pub provider: String,
    pub model: Option<String>,
    pub prompt_revision: String,
    pub context_route_revision: String,
    pub tool_revisions: BTreeMap<String, String>,
    pub stopping_rule_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProgramVersionError {
    #[error("program field {0} must not be empty")]
    EmptyField(&'static str),
}

impl ProgramVersion {
    pub fn validate(&self) -> Result<(), ProgramVersionError> {
        for (name, value) in [
            ("program_id", self.program_id.as_str()),
            ("revision", self.revision.as_str()),
            ("provider", self.provider.as_str()),
            ("prompt_revision", self.prompt_revision.as_str()),
            (
                "context_route_revision",
                self.context_route_revision.as_str(),
            ),
            (
                "stopping_rule_revision",
                self.stopping_rule_revision.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(ProgramVersionError::EmptyField(name));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_an_unversioned_program() {
        let version = ProgramVersion {
            program_id: "worker".into(),
            revision: String::new(),
            provider: "codex".into(),
            model: None,
            prompt_revision: "prompt-1".into(),
            context_route_revision: "route-1".into(),
            tool_revisions: BTreeMap::new(),
            stopping_rule_revision: "stop-1".into(),
        };
        assert_eq!(
            version.validate(),
            Err(ProgramVersionError::EmptyField("revision"))
        );
    }
}
