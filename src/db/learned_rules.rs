use crate::error::AhError;
use crate::prompt_handler::schema::PromptAction;
use rusqlite::params;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LearnedRuleCategory {
    StartupReadiness,
    RuntimeMarker,
    ReplyExtraction,
}

impl LearnedRuleCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::StartupReadiness => "StartupReadiness",
            Self::RuntimeMarker => "RuntimeMarker",
            Self::ReplyExtraction => "ReplyExtraction",
        }
    }

    pub fn parse(value: &str) -> Result<Self, AhError> {
        match value {
            "StartupReadiness" => Ok(Self::StartupReadiness),
            "RuntimeMarker" => Ok(Self::RuntimeMarker),
            "ReplyExtraction" => Ok(Self::ReplyExtraction),
            _ => Err(AhError::IpcInvalidRequest(format!(
                "invalid learned rule category: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleFingerprint {
    Regex { pattern: String },
}

impl RuleFingerprint {
    fn fingerprint_type(&self) -> &'static str {
        match self {
            Self::Regex { .. } => "regex",
        }
    }

    fn fingerprint_value(&self) -> &str {
        match self {
            Self::Regex { pattern } => pattern,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorAnchor {
    pub cursor_row_delta_from_match: i16,
    pub cursor_col_delta_from_match_end: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtractionAnchor {
    LastPromptEcho { prompt_markers: Vec<String> },
    NextRegex { pattern: String },
    StatusLine { pattern: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyExtractionSpec {
    pub start: ExtractionAnchor,
    pub end: ExtractionAnchor,
    pub drop_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearnedRule {
    pub id: String,
    pub provider: String,
    pub category: LearnedRuleCategory,
    pub fingerprint: RuleFingerprint,
    pub regex_flags: Vec<String>,
    pub positive_examples: Vec<String>,
    pub action: Option<Vec<PromptAction>>,
    pub extraction: Option<ReplyExtractionSpec>,
    pub cursor_anchor: Option<CursorAnchor>,
    pub source_event_seq_id: Option<i64>,
    pub enabled: bool,
}

pub fn insert_learned_rule_sync(db: &crate::db::Db, rule: &LearnedRule) -> Result<(), AhError> {
    let regex_flags_json = to_json("regex_flags", &rule.regex_flags)?;
    let positive_examples_json = to_json("positive_examples", &rule.positive_examples)?;
    let action_json = rule
        .action
        .as_ref()
        .map(|action| to_json("action", action))
        .transpose()?;
    let extraction_json = rule
        .extraction
        .as_ref()
        .map(|extraction| to_json("extraction", extraction))
        .transpose()?;
    let cursor_anchor_json = rule
        .cursor_anchor
        .as_ref()
        .map(|cursor_anchor| to_json("cursor_anchor", cursor_anchor))
        .transpose()?;

    let conn = db.conn();
    conn.execute(
        "INSERT INTO learned_rules (
            id, provider, category, fingerprint_type, fingerprint_value, regex_flags,
            positive_examples_json, action_json, extraction_json, cursor_anchor_json,
            source_event_seq_id, enabled
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(provider, category, fingerprint_type, fingerprint_value) DO UPDATE SET
            id = excluded.id,
            regex_flags = excluded.regex_flags,
            positive_examples_json = excluded.positive_examples_json,
            action_json = excluded.action_json,
            extraction_json = excluded.extraction_json,
            cursor_anchor_json = excluded.cursor_anchor_json,
            source_event_seq_id = excluded.source_event_seq_id,
            enabled = excluded.enabled",
        params![
            &rule.id,
            &rule.provider,
            rule.category.as_str(),
            rule.fingerprint.fingerprint_type(),
            rule.fingerprint.fingerprint_value(),
            &regex_flags_json,
            &positive_examples_json,
            action_json.as_deref(),
            extraction_json.as_deref(),
            cursor_anchor_json.as_deref(),
            rule.source_event_seq_id,
            rule.enabled,
        ],
    )
    .map_err(|err| crate::db::common::map_db_error("insert learned rule", err))?;
    Ok(())
}

pub fn lookup_learned_rules_sync(
    db: &crate::db::Db,
    provider: &str,
    category: LearnedRuleCategory,
) -> Result<Vec<LearnedRule>, AhError> {
    let conn = db.conn();
    let mut stmt = conn
        .prepare(
            "SELECT id, provider, category, fingerprint_type, fingerprint_value, regex_flags,
                    positive_examples_json, action_json, extraction_json, cursor_anchor_json,
                    source_event_seq_id, enabled
             FROM learned_rules
             WHERE provider = ? AND category = ? AND enabled = 1
             ORDER BY created_at ASC, id ASC",
        )
        .map_err(|err| crate::db::common::map_db_error("prepare lookup learned rules", err))?;

    let rows = stmt
        .query_map(params![provider, category.as_str()], |row| {
            row_to_learned_rule(row).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })
        })
        .map_err(|err| crate::db::common::map_db_error("lookup learned rules", err))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| crate::db::common::map_db_error("collect learned rules", err))
}

pub fn validate_learn_rule(pattern: &str, positive_examples: &[String]) -> Result<(), AhError> {
    if positive_examples.is_empty() {
        return Err(AhError::IpcInvalidRequest(
            "positive_examples must contain at least one positive example".to_string(),
        ));
    }
    if positive_examples.len() > 10 {
        return Err(AhError::IpcInvalidRequest(
            "positive_examples must contain at most 10 examples".to_string(),
        ));
    }
    if positive_examples
        .iter()
        .any(|example| example.len() > 16 * 1024)
    {
        return Err(AhError::IpcInvalidRequest(
            "positive_examples examples must be at most 16 KiB each".to_string(),
        ));
    }

    let regex = regex::Regex::new(pattern).map_err(|err| {
        AhError::IpcInvalidRequest(format!("invalid regex pattern for learned rule: {err}"))
    })?;
    for positive in positive_examples {
        if !regex.is_match(positive) {
            return Err(AhError::IpcInvalidRequest(
                "regex must match every positive example".to_string(),
            ));
        }
    }
    for negative in ["", "\n", "hello", "Working", "Thinking..."] {
        if regex.is_match(negative) {
            return Err(AhError::IpcInvalidRequest(format!(
                "regex too wide: matches trivial negative {negative:?}"
            )));
        }
    }
    Ok(())
}

fn to_json<T: Serialize>(field: &str, value: &T) -> Result<String, AhError> {
    serde_json::to_string(value)
        .map_err(|err| AhError::IpcInvalidRequest(format!("serialize {field}: {err}")))
}

fn from_json<T: for<'de> Deserialize<'de>>(field: &str, value: &str) -> Result<T, AhError> {
    serde_json::from_str(value)
        .map_err(|err| AhError::IpcInvalidRequest(format!("deserialize {field}: {err}")))
}

fn optional_json<T: for<'de> Deserialize<'de>>(
    field: &str,
    value: Option<String>,
) -> Result<Option<T>, AhError> {
    value
        .as_deref()
        .map(|json| from_json(field, json))
        .transpose()
}

fn row_to_learned_rule(row: &rusqlite::Row<'_>) -> Result<LearnedRule, AhError> {
    let category = LearnedRuleCategory::parse(&row.get::<_, String>(2).map_err(|err| {
        AhError::DbConstraintViolation(format!("read learned rule category: {err}"))
    })?)?;
    let fingerprint_type: String = row.get(3).map_err(|err| {
        AhError::DbConstraintViolation(format!("read learned rule fingerprint_type: {err}"))
    })?;
    let fingerprint_value: String = row.get(4).map_err(|err| {
        AhError::DbConstraintViolation(format!("read learned rule fingerprint_value: {err}"))
    })?;
    let fingerprint = match fingerprint_type.as_str() {
        "regex" => RuleFingerprint::Regex {
            pattern: fingerprint_value,
        },
        _ => {
            return Err(AhError::IpcInvalidRequest(format!(
                "invalid learned rule fingerprint type: {fingerprint_type}"
            )));
        }
    };

    let regex_flags_json: String = row.get(5).map_err(|err| {
        AhError::DbConstraintViolation(format!("read learned rule regex_flags: {err}"))
    })?;
    let positive_examples_json: String = row.get(6).map_err(|err| {
        AhError::DbConstraintViolation(format!("read learned rule positive_examples: {err}"))
    })?;
    Ok(LearnedRule {
        id: row.get(0).map_err(|err| {
            AhError::DbConstraintViolation(format!("read learned rule id: {err}"))
        })?,
        provider: row.get(1).map_err(|err| {
            AhError::DbConstraintViolation(format!("read learned rule provider: {err}"))
        })?,
        category,
        fingerprint,
        regex_flags: from_json("regex_flags", &regex_flags_json)?,
        positive_examples: from_json("positive_examples", &positive_examples_json)?,
        action: optional_json(
            "action",
            row.get(7).map_err(|err| {
                AhError::DbConstraintViolation(format!("read learned rule action: {err}"))
            })?,
        )?,
        extraction: optional_json(
            "extraction",
            row.get(8).map_err(|err| {
                AhError::DbConstraintViolation(format!("read learned rule extraction: {err}"))
            })?,
        )?,
        cursor_anchor: optional_json(
            "cursor_anchor",
            row.get(9).map_err(|err| {
                AhError::DbConstraintViolation(format!("read learned rule cursor_anchor: {err}"))
            })?,
        )?,
        source_event_seq_id: row.get(10).map_err(|err| {
            AhError::DbConstraintViolation(format!("read learned rule source_event_seq_id: {err}"))
        })?,
        enabled: row.get(11).map_err(|err| {
            AhError::DbConstraintViolation(format!("read learned rule enabled: {err}"))
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init;

    fn with_db<T>(test: impl FnOnce(&crate::db::Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn startup_rule(id: &str, provider: &str, pattern: &str) -> LearnedRule {
        LearnedRule {
            id: id.to_string(),
            provider: provider.to_string(),
            category: LearnedRuleCategory::StartupReadiness,
            fingerprint: RuleFingerprint::Regex {
                pattern: pattern.to_string(),
            },
            regex_flags: vec!["Multiline".to_string()],
            positive_examples: vec!["❯ Try \"fix lint errors\"\nOpus 4.8 (1M context)".to_string()],
            action: None,
            extraction: None,
            cursor_anchor: Some(CursorAnchor {
                cursor_row_delta_from_match: 0,
                cursor_col_delta_from_match_end: 1,
            }),
            source_event_seq_id: None,
            enabled: true,
        }
    }

    #[test]
    fn learned_rules_table_is_created_by_db_init() {
        with_db(|db| {
            let strict: i64 = db
                .conn()
                .query_row(
                    "SELECT strict FROM pragma_table_list WHERE name = 'learned_rules'",
                    [],
                    |row| row.get(0),
                )
                .expect("learned_rules table should exist");

            assert_eq!(strict, 1);
        });
    }

    #[test]
    fn learned_rules_dao_round_trips_startup_readiness_rule() {
        with_db(|db| {
            let rule = startup_rule("lr-startup-1", "claude", r#"(?m)^\s*❯"#);

            insert_learned_rule_sync(db, &rule).unwrap();
            let found =
                lookup_learned_rules_sync(db, "claude", LearnedRuleCategory::StartupReadiness)
                    .unwrap();

            assert_eq!(found, vec![rule]);
        });
    }

    #[test]
    fn learned_rules_dao_round_trips_all_three_categories() {
        with_db(|db| {
            let startup = startup_rule("lr-startup", "claude", r#"(?m)^\s*❯"#);
            let runtime = LearnedRule {
                id: "lr-runtime".to_string(),
                provider: "antigravity".to_string(),
                category: LearnedRuleCategory::RuntimeMarker,
                fingerprint: RuleFingerprint::Regex {
                    pattern: r#"(?m)^\? for shortcuts\b"#.to_string(),
                },
                regex_flags: vec!["Multiline".to_string()],
                positive_examples: vec![
                    "? for shortcuts                                Gemini 3.5 Flash (High)"
                        .to_string(),
                ],
                action: None,
                extraction: None,
                cursor_anchor: Some(CursorAnchor {
                    cursor_row_delta_from_match: 0,
                    cursor_col_delta_from_match_end: 0,
                }),
                source_event_seq_id: None,
                enabled: true,
            };
            let extraction = LearnedRule {
                id: "lr-extraction".to_string(),
                provider: "antigravity".to_string(),
                category: LearnedRuleCategory::ReplyExtraction,
                fingerprint: RuleFingerprint::Regex {
                    pattern: r#"(?m)^>\s"#.to_string(),
                },
                regex_flags: vec!["Multiline".to_string()],
                positive_examples: vec!["> summarize\n▸ Thought for 1s\nanswer".to_string()],
                action: None,
                extraction: Some(ReplyExtractionSpec {
                    start: ExtractionAnchor::LastPromptEcho {
                        prompt_markers: vec!["> ".to_string()],
                    },
                    end: ExtractionAnchor::NextRegex {
                        pattern: r#"(?m)^\? for shortcuts\b"#.to_string(),
                    },
                    drop_lines: vec![r#"(?m)^\s*▸\s+(?:Thought|Thinking)\b.*$"#.to_string()],
                }),
                cursor_anchor: None,
                source_event_seq_id: None,
                enabled: true,
            };

            for rule in [&startup, &runtime, &extraction] {
                insert_learned_rule_sync(db, rule).unwrap();
            }

            assert_eq!(
                lookup_learned_rules_sync(db, "claude", LearnedRuleCategory::StartupReadiness)
                    .unwrap(),
                vec![startup]
            );
            assert_eq!(
                lookup_learned_rules_sync(db, "antigravity", LearnedRuleCategory::RuntimeMarker)
                    .unwrap(),
                vec![runtime]
            );
            assert_eq!(
                lookup_learned_rules_sync(db, "antigravity", LearnedRuleCategory::ReplyExtraction)
                    .unwrap(),
                vec![extraction]
            );
        });
    }

    #[test]
    fn validate_learned_rules_accepts_regex_matching_all_positive_examples() {
        validate_learn_rule(
            r#"(?m)^\s*❯"#,
            &[r#"❯ Try "fix lint errors""#.to_string(), "❯ ".to_string()],
        )
        .expect("gap #5 prompt-prefix regex should accept noisy positives");

        validate_learn_rule(
            r#"(?m)^\? for shortcuts\b"#,
            &[
                "? for shortcuts                                Gemini 3.5 Flash (High)"
                    .to_string(),
            ],
        )
        .expect("antigravity idle marker must accept model suffix");
    }

    #[test]
    fn validate_learned_rules_rejects_regex_that_misses_any_positive_example() {
        let err = validate_learn_rule(
            r#"(?m)^\s*❯\s*$"#,
            &[r#"❯ Try "fix lint errors""#.to_string(), "❯ ".to_string()],
        )
        .expect_err("old bare-prompt regex must be rejected for gap #5 noisy positive");

        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn validate_learned_rules_rejects_empty_positive_examples() {
        let err = validate_learn_rule(r#"(?m)^\s*❯"#, &[])
            .expect_err("positive_examples must be required");

        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn validate_learned_rules_rejects_too_wide_regex() {
        for pattern in [r#"\s*"#, r#"(?m)^"#] {
            let err = validate_learn_rule(pattern, &["❯".to_string()])
                .expect_err("trivial negative matches must be rejected");

            assert!(err.to_string().contains("too wide"));
        }
        let err = validate_learn_rule(".", &["x".to_string()])
            .expect_err("dot pattern must be rejected as too wide");

        assert!(err.to_string().contains("too wide"));
    }

    #[test]
    fn validate_learned_rules_rejects_positive_example_limits() {
        let too_many = vec!["❯".to_string(); 11];
        let err = validate_learn_rule(r#"(?m)^\s*❯"#, &too_many)
            .expect_err("positive_examples must be capped at 10");
        assert!(err.to_string().contains("at most 10"));

        let too_large = format!("❯{}", "x".repeat(16 * 1024));
        let err = validate_learn_rule(r#"(?m)^\s*❯"#, &[too_large])
            .expect_err("positive_examples entries must be capped at 16 KiB");
        assert!(err.to_string().contains("16 KiB"));
    }

    #[test]
    fn validate_learned_rules_rejects_invalid_regex() {
        let err = validate_learn_rule("(", &["anything".to_string()])
            .expect_err("invalid regex must be rejected");

        assert!(err.to_string().contains("regex"));
    }
}
