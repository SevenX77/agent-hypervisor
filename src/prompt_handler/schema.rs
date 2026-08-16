//! Schema and validation for the prompt-handler knowledge base.

use crate::error::AhError;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type PromptResult<T> = Result<T, PromptHandlerError>;

#[derive(Debug, Error)]
pub enum PromptHandlerError {
    #[error("invalid prompt knowledge base: {0}")]
    InvalidKb(String),

    #[error("invalid prompt action: {0}")]
    InvalidAction(String),

    #[error("invalid prompt fingerprint: {0}")]
    InvalidFingerprint(String),

    #[error("prompt knowledge base I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("prompt knowledge base JSON failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error("prompt knowledge base lock failed: {0}")]
    Lock(String),
}

impl From<PromptHandlerError> for AhError {
    fn from(err: PromptHandlerError) -> Self {
        Self::IpcInvalidRequest(err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptKb {
    pub version: String,
    #[serde(default)]
    pub cases: Vec<PromptCase>,
}

impl PromptKb {
    pub fn new(cases: Vec<PromptCase>) -> Self {
        Self {
            version: "1".to_string(),
            cases,
        }
    }

    pub fn validate(&self) -> PromptResult<()> {
        if self.version.trim().is_empty() {
            tracing::warn!(
                reason = "empty version",
                impact = "prompt KB cannot be trusted for automatic actions",
                "prompt KB validation failed"
            );
            return Err(PromptHandlerError::InvalidKb(
                "version must not be empty".to_string(),
            ));
        }
        for case in &self.cases {
            case.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptCase {
    pub id: String,
    pub provider: Option<String>,
    pub fingerprint: PromptFingerprint,
    pub action: Vec<PromptAction>,
    pub category: String,
    pub description: Option<String>,
    pub confidence_threshold: Option<f64>,
    #[serde(default)]
    pub used_count: u64,
    pub created_at: Option<String>,
    pub last_used_at: Option<String>,
    pub created_by: Option<String>,
    #[serde(default)]
    pub regex_flags: Vec<String>,
    pub trigger_state: Option<String>,
}

impl PromptCase {
    pub fn validate(&self) -> PromptResult<Vec<ValidatedAction>> {
        if self.id.trim().is_empty() {
            tracing::warn!(
                reason = "empty case id",
                impact = "case skipped because it cannot be audited",
                "prompt case validation failed"
            );
            return Err(PromptHandlerError::InvalidKb(
                "case id must not be empty".to_string(),
            ));
        }
        self.fingerprint.validate_with_flags(&self.regex_flags)?;
        self.action
            .iter()
            .map(PromptAction::validate)
            .collect::<PromptResult<Vec<_>>>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PromptFingerprint {
    #[serde(rename = "regex")]
    Regex { pattern: String },
}

impl PromptFingerprint {
    pub fn validate(&self) -> PromptResult<()> {
        self.validate_with_flags(&[])
    }

    pub fn validate_with_flags(&self, regex_flags: &[String]) -> PromptResult<()> {
        match self {
            Self::Regex { pattern } => {
                build_regex(pattern, regex_flags).map_err(|err| {
                    tracing::warn!(
                        reason = %err,
                        impact = "case skipped because regex cannot be compiled",
                        pattern,
                        "prompt fingerprint validation failed"
                    );
                    err
                })?;
                Ok(())
            }
        }
    }

    pub fn pattern(&self) -> &str {
        match self {
            Self::Regex { pattern } => pattern,
        }
    }
}

pub(crate) fn build_regex(pattern: &str, regex_flags: &[String]) -> PromptResult<Regex> {
    let mut builder = RegexBuilder::new(pattern);
    for flag in regex_flags {
        match flag.as_str() {
            "CaseInsensitive" => {
                builder.case_insensitive(true);
            }
            "Dotall" => {
                builder.dot_matches_new_line(true);
            }
            "Multiline" => {
                builder.multi_line(true);
            }
            unknown => {
                return Err(PromptHandlerError::InvalidFingerprint(format!(
                    "unknown regex flag {unknown:?} for pattern {pattern:?}"
                )));
            }
        }
    }
    builder.build().map_err(|err| {
        PromptHandlerError::InvalidFingerprint(format!("invalid regex pattern {pattern:?}: {err}"))
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PromptAction {
    #[serde(rename = "key")]
    Key { value: String },
    #[serde(rename = "literal")]
    Literal { value: String },
}

impl PromptAction {
    pub fn validate(&self) -> PromptResult<ValidatedAction> {
        match self {
            Self::Key { value } => validate_key(value),
            Self::Literal { value } => validate_literal(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedAction {
    Key(String),
    Literal(String),
}

fn validate_key(value: &str) -> PromptResult<ValidatedAction> {
    let normalized = value.trim();
    let allowed_named = [
        "Esc",
        "Escape",
        "Enter",
        "Up",
        "Down",
        "Left",
        "Right",
        "Tab",
        "Space",
        "BSpace",
        "Backspace",
        "C-c",
        "C-d",
        "Ctrl+C",
        "Ctrl+D",
    ];
    let is_single_alnum =
        normalized.len() == 1 && normalized.chars().all(|ch| ch.is_ascii_alphanumeric());
    if allowed_named.contains(&normalized) || is_single_alnum {
        return Ok(ValidatedAction::Key(normalized.to_string()));
    }

    tracing::warn!(
        reason = "key is outside prompt-handler whitelist",
        impact = "action rejected to avoid unsafe terminal input",
        key = normalized,
        "prompt action validation failed"
    );
    Err(PromptHandlerError::InvalidAction(format!(
        "key {normalized:?} is not whitelisted"
    )))
}

fn validate_literal(value: &str) -> PromptResult<ValidatedAction> {
    let normalized = value.trim();
    let lower = normalized.to_ascii_lowercase();
    let allowed_literals = ["yes", "no", "agree"];
    if allowed_literals.contains(&lower.as_str()) && !contains_shell_metachar(normalized) {
        return Ok(ValidatedAction::Literal(normalized.to_string()));
    }

    tracing::warn!(
        reason = "literal is outside prompt-handler whitelist or contains shell metacharacters",
        impact = "action rejected to avoid shell injection",
        literal = normalized,
        "prompt action validation failed"
    );
    Err(PromptHandlerError::InvalidAction(format!(
        "literal {normalized:?} is not whitelisted"
    )))
}

fn contains_shell_metachar(value: &str) -> bool {
    value
        .chars()
        .any(|ch| matches!(ch, '&' | '|' | ';' | '$' | '`' | '<' | '>' | '\n' | '\r'))
}

#[cfg(test)]
mod tests {
    use super::{
        PromptAction, PromptCase, PromptFingerprint, PromptHandlerError, PromptKb, ValidatedAction,
    };

    #[test]
    fn deserializes_design_schema_and_phase2_fields() {
        let raw = r#"
        {
          "version": "1",
          "cases": [
            {
              "id": "codex_update_01",
              "provider": "codex",
              "fingerprint": {
                "type": "regex",
                "pattern": "Update available!.*npm install -g @openai/codex"
              },
              "action": [
                {"type": "key", "value": "2"},
                {"type": "key", "value": "Enter"}
              ],
              "category": "auto-skip",
              "description": "Skip codex global update to avoid EACCES",
              "confidence_threshold": 0.9,
              "used_count": 42,
              "created_at": "2026-05-13T00:00:00Z",
              "last_used_at": "2026-05-13T00:00:00Z",
              "created_by": "ah-default",
              "regex_flags": ["Dotall", "Multiline"],
              "trigger_state": "WAITING_FOR_ACK"
            }
          ]
        }
        "#;

        let kb: PromptKb = serde_json::from_str(raw).unwrap();
        kb.validate().unwrap();

        let case = &kb.cases[0];
        assert_eq!(case.id, "codex_update_01");
        assert_eq!(case.provider.as_deref(), Some("codex"));
        assert_eq!(case.regex_flags, ["Dotall", "Multiline"]);
        assert_eq!(case.trigger_state.as_deref(), Some("WAITING_FOR_ACK"));
    }

    #[test]
    fn validates_key_whitelist() {
        let accepted = [
            PromptAction::Key { value: "2".into() },
            PromptAction::Key {
                value: "Enter".into(),
            },
            PromptAction::Key {
                value: "Esc".into(),
            },
            PromptAction::Key {
                value: "C-c".into(),
            },
            PromptAction::Key {
                value: "Ctrl+D".into(),
            },
        ];

        for action in accepted {
            assert!(matches!(
                action.validate().unwrap(),
                ValidatedAction::Key(_)
            ));
        }
    }

    #[test]
    fn validates_literal_whitelist() {
        let action = PromptAction::Literal {
            value: "agree".into(),
        };
        assert_eq!(
            action.validate().unwrap(),
            ValidatedAction::Literal("agree".into())
        );
    }

    #[test]
    fn rejects_shell_injection_literals_and_unknown_keys() {
        let literal = PromptAction::Literal {
            value: "yes; rm -rf /".into(),
        };
        let key = PromptAction::Key { value: "&".into() };

        assert!(matches!(
            literal.validate().unwrap_err(),
            PromptHandlerError::InvalidAction(_)
        ));
        assert!(matches!(
            key.validate().unwrap_err(),
            PromptHandlerError::InvalidAction(_)
        ));
    }

    #[test]
    fn rejects_invalid_regex() {
        let case = PromptCase {
            id: "bad".into(),
            provider: None,
            fingerprint: PromptFingerprint::Regex {
                pattern: "(".into(),
            },
            action: vec![PromptAction::Key {
                value: "Enter".into(),
            }],
            category: "auto-skip".into(),
            description: None,
            confidence_threshold: None,
            used_count: 0,
            created_at: None,
            last_used_at: None,
            created_by: None,
            regex_flags: Vec::new(),
            trigger_state: None,
        };

        assert!(matches!(
            case.validate().unwrap_err(),
            PromptHandlerError::InvalidFingerprint(_)
        ));
    }
}
