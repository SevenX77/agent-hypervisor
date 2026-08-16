use crate::db::Db;
use crate::db::common::map_db_error;
use crate::error::AhError;
use crate::prompt_handler::schema::{PromptAction, build_regex};
use rusqlite::{OptionalExtension, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptFingerprintType {
    Regex,
    Hash,
}

impl PromptFingerprintType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Regex => "regex",
            Self::Hash => "hash",
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewPromptExperience {
    pub id: String,
    pub provider: Option<String>,
    pub fingerprint_type: PromptFingerprintType,
    pub fingerprint_value: String,
    pub action: Vec<PromptAction>,
    pub category: String,
    pub confidence: f64,
    pub source: String,
    pub trigger_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromptExperience {
    pub id: String,
    pub provider: Option<String>,
    pub fingerprint_type: String,
    pub fingerprint_value: String,
    pub action: Vec<PromptAction>,
    pub category: String,
    pub confidence: f64,
    pub source: String,
    pub used_count: i64,
    pub trigger_state: Option<String>,
}

pub trait PromptExperienceLookup {
    fn lookup_prompt_experience(
        &self,
        provider: &str,
        sanitized_text: &str,
        sanitized_hash_hex: &str,
    ) -> Result<Option<PromptExperience>, AhError>;

    fn record_prompt_experience(&self, experience: &NewPromptExperience) -> Result<(), AhError>;
}

impl PromptExperienceLookup for Db {
    fn lookup_prompt_experience(
        &self,
        provider: &str,
        sanitized_text: &str,
        sanitized_hash_hex: &str,
    ) -> Result<Option<PromptExperience>, AhError> {
        lookup_prompt_experience_sync(self, provider, sanitized_text, sanitized_hash_hex)
    }

    fn record_prompt_experience(&self, experience: &NewPromptExperience) -> Result<(), AhError> {
        upsert_prompt_experience_sync(self, experience)
    }
}

pub fn upsert_prompt_experience_sync(
    db: &Db,
    experience: &NewPromptExperience,
) -> Result<(), AhError> {
    let action_json = serde_json::to_string(&experience.action).map_err(|err| {
        AhError::IpcInvalidRequest(format!("serialize prompt experience action: {err}"))
    })?;
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE prompt_experience
             SET used_count = used_count + 1,
                 last_used_at = unixepoch()
             WHERE provider IS ?
               AND fingerprint_type = ?
               AND fingerprint_value = ?",
            params![
                experience.provider.as_deref(),
                experience.fingerprint_type.as_str(),
                &experience.fingerprint_value,
            ],
        )
        .map_err(|err| map_db_error("update prompt experience on upsert", err))?;
    if changes > 0 {
        return Ok(());
    }

    conn.execute(
        "INSERT INTO prompt_experience (
                id, provider, fingerprint_type, fingerprint_value, action_json, category,
                confidence, source, trigger_state
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            &experience.id,
            experience.provider.as_deref(),
            experience.fingerprint_type.as_str(),
            &experience.fingerprint_value,
            &action_json,
            &experience.category,
            experience.confidence,
            &experience.source,
            experience.trigger_state.as_deref(),
        ],
    )
    .map_err(|err| map_db_error("upsert prompt experience", err))?;
    Ok(())
}

pub fn lookup_prompt_experience_sync(
    db: &Db,
    provider: &str,
    sanitized_text: &str,
    sanitized_hash_hex: &str,
) -> Result<Option<PromptExperience>, AhError> {
    if let Some(experience) = lookup_regex_prompt_experience_sync(db, provider, sanitized_text)? {
        return Ok(Some(experience));
    }
    lookup_hash_prompt_experience_sync(db, provider, sanitized_hash_hex)
}

pub fn hash_hex(hash: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in hash {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("write to string cannot fail");
    }
    out
}

fn lookup_regex_prompt_experience_sync(
    db: &Db,
    provider: &str,
    sanitized_text: &str,
) -> Result<Option<PromptExperience>, AhError> {
    let rows = {
        let conn = db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT id, provider, fingerprint_type, fingerprint_value, action_json, category,
                        confidence, source, used_count, trigger_state
                 FROM prompt_experience
                 WHERE fingerprint_type = 'regex'
                   AND (provider = ? OR provider IS NULL)
                 ORDER BY provider IS NULL, created_at",
            )
            .map_err(|err| map_db_error("prepare regex prompt experience lookup", err))?;
        stmt.query_map(params![provider], prompt_experience_from_row)
            .map_err(|err| map_db_error("query regex prompt experience", err))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("read regex prompt experience", err))?
    };

    for row in rows {
        let Ok(regex) = build_regex(&row.fingerprint_value, &[]) else {
            tracing::warn!(
                experience_id = %row.id,
                pattern = %row.fingerprint_value,
                "invalid prompt experience regex skipped"
            );
            continue;
        };
        if regex.is_match(sanitized_text) {
            increment_used_count(db, &row.id)?;
            return Ok(Some(with_incremented_use(row)));
        }
    }
    Ok(None)
}

fn lookup_hash_prompt_experience_sync(
    db: &Db,
    provider: &str,
    sanitized_hash_hex: &str,
) -> Result<Option<PromptExperience>, AhError> {
    let row = {
        let conn = db.conn();
        conn.query_row(
            "SELECT id, provider, fingerprint_type, fingerprint_value, action_json, category,
                    confidence, source, used_count, trigger_state
             FROM prompt_experience
             WHERE fingerprint_type = 'hash'
               AND fingerprint_value = ?
               AND (provider = ? OR provider IS NULL)
             ORDER BY provider IS NULL, created_at
             LIMIT 1",
            params![sanitized_hash_hex, provider],
            prompt_experience_from_row,
        )
        .optional()
        .map_err(|err| map_db_error("lookup hash prompt experience", err))?
    };

    if let Some(row) = row {
        increment_used_count(db, &row.id)?;
        return Ok(Some(with_incremented_use(row)));
    }
    Ok(None)
}

fn prompt_experience_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PromptExperience> {
    let action_json: String = row.get(4)?;
    let action = serde_json::from_str(&action_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(PromptExperience {
        id: row.get(0)?,
        provider: row.get(1)?,
        fingerprint_type: row.get(2)?,
        fingerprint_value: row.get(3)?,
        action,
        category: row.get(5)?,
        confidence: row.get(6)?,
        source: row.get(7)?,
        used_count: row.get(8)?,
        trigger_state: row.get(9)?,
    })
}

fn increment_used_count(db: &Db, id: &str) -> Result<(), AhError> {
    db.conn()
        .execute(
            "UPDATE prompt_experience SET used_count = used_count + 1, last_used_at = unixepoch() WHERE id = ?",
            params![id],
        )
        .map_err(|err| map_db_error("increment prompt experience used_count", err))?;
    Ok(())
}

fn with_incremented_use(mut experience: PromptExperience) -> PromptExperience {
    experience.used_count += 1;
    experience
}

#[cfg(test)]
mod tests {
    use crate::db::{init, prompt_experience};
    use crate::prompt_handler::gating::hash_sanitized_text;
    use crate::prompt_handler::schema::PromptAction;

    fn with_db<T>(test: impl FnOnce(&crate::db::Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    #[test]
    fn init_creates_prompt_experience_table() {
        with_db(|db| {
            let conn = db.conn();
            let strict: i64 = conn
                .query_row(
                    "SELECT strict FROM pragma_table_list WHERE name = 'prompt_experience'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(strict, 1);
        });
    }

    #[test]
    fn upsert_increments_used_count_for_unique_fingerprint() {
        with_db(|db| {
            let first = prompt_experience::NewPromptExperience {
                id: "exp-1".into(),
                provider: Some("codex".into()),
                fingerprint_type: prompt_experience::PromptFingerprintType::Hash,
                fingerprint_value: "abc123".into(),
                action: vec![PromptAction::Key {
                    value: "Enter".into(),
                }],
                category: "auto-accept".into(),
                confidence: 0.91,
                source: "test".into(),
                trigger_state: Some("BUSY".into()),
            };
            let second = prompt_experience::NewPromptExperience {
                id: "exp-2".into(),
                ..first.clone()
            };

            prompt_experience::upsert_prompt_experience_sync(db, &first).unwrap();
            prompt_experience::upsert_prompt_experience_sync(db, &second).unwrap();

            let used_count: i64 = db
                .conn()
                .query_row(
                    "SELECT used_count FROM prompt_experience WHERE id = 'exp-1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let found = prompt_experience::lookup_prompt_experience_sync(
                db,
                "codex",
                "screen text",
                "abc123",
            )
            .unwrap()
            .unwrap();
            assert_eq!(found.id, "exp-1");
            assert_eq!(used_count, 1);
        });
    }

    #[test]
    fn upsert_increments_used_count_for_global_provider_null_fingerprint() {
        with_db(|db| {
            let first = prompt_experience::NewPromptExperience {
                id: "global-1".into(),
                provider: None,
                fingerprint_type: prompt_experience::PromptFingerprintType::Regex,
                fingerprint_value: "Global Terms".into(),
                action: vec![PromptAction::Key {
                    value: "Enter".into(),
                }],
                category: "auto-accept".into(),
                confidence: 0.91,
                source: "test".into(),
                trigger_state: None,
            };
            let second = prompt_experience::NewPromptExperience {
                id: "global-2".into(),
                ..first.clone()
            };

            prompt_experience::upsert_prompt_experience_sync(db, &first).unwrap();
            prompt_experience::upsert_prompt_experience_sync(db, &second).unwrap();

            let (rows, used_count): (i64, i64) = db
                .conn()
                .query_row(
                    "SELECT COUNT(*), MAX(used_count)
                     FROM prompt_experience
                     WHERE provider IS NULL
                       AND fingerprint_type = 'regex'
                       AND fingerprint_value = 'Global Terms'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(rows, 1);
            assert_eq!(used_count, 1);
        });
    }

    #[test]
    fn lookup_prefers_regex_over_hash() {
        with_db(|db| {
            prompt_experience::upsert_prompt_experience_sync(
                db,
                &prompt_experience::NewPromptExperience {
                    id: "hash-exp".into(),
                    provider: Some("codex".into()),
                    fingerprint_type: prompt_experience::PromptFingerprintType::Hash,
                    fingerprint_value: "hash-1".into(),
                    action: vec![PromptAction::Key { value: "1".into() }],
                    category: "auto-skip".into(),
                    confidence: 0.9,
                    source: "test".into(),
                    trigger_state: None,
                },
            )
            .unwrap();
            prompt_experience::upsert_prompt_experience_sync(
                db,
                &prompt_experience::NewPromptExperience {
                    id: "regex-exp".into(),
                    provider: Some("codex".into()),
                    fingerprint_type: prompt_experience::PromptFingerprintType::Regex,
                    fingerprint_value: "New Terms".into(),
                    action: vec![PromptAction::Key { value: "2".into() }],
                    category: "auto-skip".into(),
                    confidence: 0.9,
                    source: "test".into(),
                    trigger_state: None,
                },
            )
            .unwrap();

            let found = prompt_experience::lookup_prompt_experience_sync(
                db,
                "codex",
                "Please accept New Terms",
                "hash-1",
            )
            .unwrap()
            .unwrap();

            assert_eq!(found.id, "regex-exp");
            assert_eq!(found.action, vec![PromptAction::Key { value: "2".into() }]);
        });
    }

    #[test]
    fn lookup_matches_hash_when_regex_misses() {
        with_db(|db| {
            prompt_experience::upsert_prompt_experience_sync(
                db,
                &prompt_experience::NewPromptExperience {
                    id: "hash-exp".into(),
                    provider: Some("codex".into()),
                    fingerprint_type: prompt_experience::PromptFingerprintType::Hash,
                    fingerprint_value: "hash-2".into(),
                    action: vec![PromptAction::Key {
                        value: "Enter".into(),
                    }],
                    category: "auto-skip".into(),
                    confidence: 0.9,
                    source: "test".into(),
                    trigger_state: None,
                },
            )
            .unwrap();

            let found = prompt_experience::lookup_prompt_experience_sync(
                db,
                "codex",
                "Unmatched screen",
                "hash-2",
            )
            .unwrap()
            .unwrap();

            assert_eq!(found.id, "hash-exp");
        });
    }

    #[test]
    fn sanitized_hash_hex_is_stable_for_lookup_key() {
        let hash = hash_sanitized_text("screen");
        let hex = prompt_experience::hash_hex(&hash);

        assert_eq!(hex.len(), 64);
        assert_eq!(
            hex,
            "4cd6c2914887dd4a68e4c9ffbed8b077f048cf795d6cfa0b801d43e0ea5a1560"
        );
    }
}
