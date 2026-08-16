//! Manual prompt resolution helpers used by RPC and CLI entry points.

use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::db::events::query_last_event_of_type_sync;
use crate::db::jobs::query_dispatched_job_for_agent_sync;
use crate::db::state_machine::{
    STATE_BUSY, STATE_IDLE, STATE_PROMPT_PENDING, transit_agent_state_sync,
};
use crate::error::AhError;
use crate::prompt_handler::events::{UNKNOWN_PROMPT_DETECTED, UnknownPromptPayload};
use crate::prompt_handler::kb::{load_or_bootstrap_kb, save_kb_atomic};
use crate::prompt_handler::runner::PromptIo;
use crate::prompt_handler::schema::{PromptAction, PromptCase, PromptFingerprint, ValidatedAction};
use crate::tmux::TmuxPaneId;
use regex::escape;
use rusqlite::OptionalExtension;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvePromptResult {
    pub resolved_state: String,
    pub saved_to_kb: bool,
    pub case_id: Option<String>,
    pub action_labels: Vec<String>,
}

#[derive(Clone)]
pub struct ResolvePromptRequest {
    pub db: Db,
    pub agent_id: String,
    pub pane_id: TmuxPaneId,
    pub provider: String,
    pub kb_path: PathBuf,
    pub actions: Vec<PromptAction>,
    pub save_to_kb: bool,
}

pub fn normalize_action_value(value: serde_json::Value) -> Result<Vec<PromptAction>, AhError> {
    if value.is_array() {
        serde_json::from_value(value).map_err(|err| {
            AhError::IpcInvalidRequest(format!("invalid prompt action array: {err}"))
        })
    } else {
        serde_json::from_value::<PromptAction>(value)
            .map(|action| vec![action])
            .map_err(|err| AhError::IpcInvalidRequest(format!("invalid prompt action: {err}")))
    }
}

pub async fn resolve_prompt_with_io(
    request: ResolvePromptRequest,
    io: &dyn PromptIo,
) -> Result<ResolvePromptResult, AhError> {
    let ResolvePromptRequest {
        db,
        agent_id,
        pane_id,
        provider,
        kb_path,
        actions,
        save_to_kb,
    } = request;
    tracing::info!(
        agent_id,
        provider,
        action_count = actions.len(),
        save_to_kb,
        "resolve prompt request start"
    );
    let db_for_check = db.clone();
    let agent_id_for_check = agent_id.clone();
    spawn_db("prompt_handler::resolve_prompt_check_pending", move || {
        ensure_prompt_pending_sync(&db_for_check, &agent_id_for_check)
    })
    .await?;
    let validated = validate_actions(&actions)?;
    execute_validated_actions(io, &pane_id, &validated)?;

    let db_for_state = db.clone();
    let agent_id_for_state = agent_id.clone();
    let resolved_state = spawn_db("prompt_handler::resolve_prompt_state", move || {
        resolve_prompt_state_sync(&db_for_state, &agent_id_for_state)
    })
    .await?;

    let case_id = if save_to_kb {
        let db_for_kb = db.clone();
        let agent_id_for_kb = agent_id.clone();
        let provider_for_kb = provider.clone();
        let actions_for_kb = actions.clone();
        let kb_path_for_kb = kb_path.clone();
        Some(
            spawn_db("prompt_handler::resolve_prompt_save_kb", move || {
                save_resolved_prompt_case_sync(
                    &db_for_kb,
                    &agent_id_for_kb,
                    &provider_for_kb,
                    &kb_path_for_kb,
                    actions_for_kb,
                )
            })
            .await?,
        )
    } else {
        None
    };

    tracing::info!(
        agent_id,
        resolved_state,
        saved_to_kb = save_to_kb,
        case_id,
        "resolve prompt request complete"
    );
    Ok(ResolvePromptResult {
        resolved_state,
        saved_to_kb: save_to_kb,
        case_id,
        action_labels: validated.iter().map(action_label).collect(),
    })
}

fn ensure_prompt_pending_sync(db: &Db, agent_id: &str) -> Result<(), AhError> {
    let conn = db.conn();
    let current = conn
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            rusqlite::params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query state before prompt resolve", err))?
        .ok_or_else(|| AhError::AgentNotFound(agent_id.to_string()))?;
    if current != STATE_PROMPT_PENDING {
        tracing::warn!(
            agent_id,
            state = %current,
            impact = "manual prompt action rejected because agent is not blocked on a prompt",
            "resolve prompt rejected wrong state"
        );
        return Err(AhError::AgentWrongState {
            current_state: current,
        });
    }
    Ok(())
}

fn validate_actions(actions: &[PromptAction]) -> Result<Vec<ValidatedAction>, AhError> {
    if actions.is_empty() {
        return Err(AhError::IpcInvalidRequest(
            "action must contain at least one prompt action".to_string(),
        ));
    }
    actions
        .iter()
        .map(|action| action.validate().map_err(AhError::from))
        .collect()
}

fn execute_validated_actions(
    io: &dyn PromptIo,
    pane_id: &TmuxPaneId,
    actions: &[ValidatedAction],
) -> Result<(), AhError> {
    for action in actions {
        match action {
            ValidatedAction::Key(keysym) => {
                tracing::info!(keysym, "resolve prompt sending keysym");
                io.send_key_keysym(pane_id, keysym)?;
            }
            ValidatedAction::Literal(literal) => {
                tracing::info!(literal, "resolve prompt sending literal");
                io.send_key_literal(pane_id, literal)?;
            }
        }
    }
    Ok(())
}

fn resolve_prompt_state_sync(db: &Db, agent_id: &str) -> Result<String, AhError> {
    let mut conn = db.conn();
    let current = conn
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            rusqlite::params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query state before prompt resolve", err))?
        .ok_or_else(|| AhError::AgentNotFound(agent_id.to_string()))?;
    if current != STATE_PROMPT_PENDING {
        tracing::warn!(
            agent_id,
            state = %current,
            impact = "manual prompt action rejected because agent is not blocked on a prompt",
            "resolve prompt rejected wrong state"
        );
        return Err(AhError::AgentWrongState {
            current_state: current,
        });
    }

    let target = if query_dispatched_job_for_agent_sync(&conn, agent_id)?.is_some() {
        STATE_BUSY
    } else {
        STATE_IDLE
    };
    tracing::info!(agent_id, target, "resolve prompt state transition start");
    transit_agent_state_sync(
        &mut conn,
        agent_id,
        &[STATE_PROMPT_PENDING],
        target,
        Some("PROMPT_RESOLVED"),
    )?;
    Ok(target.to_string())
}

fn save_resolved_prompt_case_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    kb_path: &Path,
    actions: Vec<PromptAction>,
) -> Result<String, AhError> {
    let payload = latest_unknown_prompt_payload(db, agent_id)?;
    let screenshot = payload.pane_screenshot.trim();
    if screenshot.is_empty() {
        return Err(AhError::IpcInvalidRequest(
            "cannot save prompt case from empty prompt snapshot".to_string(),
        ));
    }
    let case_id = format!(
        "master_resolve_{}_{}",
        unix_timestamp(),
        short_hash(&payload.capture_hash)
    );
    let mut kb = load_or_bootstrap_kb(kb_path).map_err(AhError::from)?;
    let prompt_case = PromptCase {
        id: case_id.clone(),
        provider: Some(provider.to_string()),
        fingerprint: PromptFingerprint::Regex {
            pattern: escape(screenshot),
        },
        action: actions,
        category: "manual-resolve".to_string(),
        description: Some(format!(
            "Saved from manual prompt resolve capture_hash={}",
            payload.capture_hash
        )),
        confidence_threshold: Some(1.0),
        used_count: 0,
        created_at: Some(unix_timestamp().to_string()),
        last_used_at: None,
        created_by: Some("ah-master".to_string()),
        regex_flags: Vec::new(),
        trigger_state: Some(STATE_PROMPT_PENDING.to_string()),
    };
    prompt_case.validate().map_err(AhError::from)?;
    tracing::info!(
        agent_id,
        case_id,
        path = %kb_path.display(),
        "resolve prompt saving KB case"
    );
    kb.cases.insert(0, prompt_case);
    save_kb_atomic(kb_path, &kb).map_err(AhError::from)?;
    Ok(case_id)
}

fn latest_unknown_prompt_payload(db: &Db, agent_id: &str) -> Result<UnknownPromptPayload, AhError> {
    let conn = db.conn();
    let event = query_last_event_of_type_sync(&conn, agent_id, UNKNOWN_PROMPT_DETECTED)?
        .ok_or_else(|| {
            AhError::IpcInvalidRequest(format!(
                "agent {agent_id} has no UNKNOWN_PROMPT_DETECTED event to save"
            ))
        })?;
    serde_json::from_str(&event.payload).map_err(|err| {
        AhError::IpcInvalidRequest(format!("invalid UNKNOWN_PROMPT_DETECTED payload: {err}"))
    })
}

fn action_label(action: &ValidatedAction) -> String {
    match action {
        ValidatedAction::Key(value) | ValidatedAction::Literal(value) => value.clone(),
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn short_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

#[cfg(test)]
mod tests {
    use super::{
        ResolvePromptRequest, latest_unknown_prompt_payload, normalize_action_value,
        resolve_prompt_with_io,
    };
    use crate::db::agents::{insert_agent_sync, query_agent_sync};
    use crate::db::events::insert_event_sync;
    use crate::db::jobs::insert_job_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::{STATE_BUSY, STATE_IDLE, STATE_PROMPT_PENDING};
    use crate::db::{Db, init};
    use crate::error::AhError;
    use crate::prompt_handler::events::{UNKNOWN_PROMPT_DETECTED, UnknownPromptPayload};
    use crate::prompt_handler::kb::load_or_bootstrap_kb;
    use crate::prompt_handler::runner::{PromptIo, PromptSnapshot};
    use crate::prompt_handler::schema::PromptAction;
    use crate::tmux::TmuxPaneId;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeIo {
        sent: Mutex<Vec<String>>,
    }

    impl PromptIo for FakeIo {
        fn capture_pane(&self, _pane_id: &TmuxPaneId) -> Result<String, AhError> {
            Ok(String::new())
        }

        fn send_key_literal(&self, _pane_id: &TmuxPaneId, value: &str) -> Result<(), AhError> {
            self.sent.lock().unwrap().push(format!("literal:{value}"));
            Ok(())
        }

        fn send_key_keysym(&self, _pane_id: &TmuxPaneId, value: &str) -> Result<(), AhError> {
            self.sent.lock().unwrap().push(format!("key:{value}"));
            Ok(())
        }
    }

    fn test_db(state: &str) -> (Db, tempfile::TempDir) {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let db = init(db_file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "codex", state, Some(123)).unwrap();
        }
        (db, tempfile::TempDir::new().unwrap())
    }

    fn action() -> Vec<PromptAction> {
        vec![
            PromptAction::Key {
                value: "Down".into(),
            },
            PromptAction::Key {
                value: "Enter".into(),
            },
        ]
    }

    fn insert_unknown_event(db: &Db) {
        insert_unknown_event_with_text(db, "Manual prompt?");
    }

    fn insert_unknown_event_with_text(db: &Db, text: &str) {
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [2; 32],
                sanitized_text: text.into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );
        let conn = db.conn();
        insert_event_sync(
            &conn,
            "a1",
            None,
            UNKNOWN_PROMPT_DETECTED,
            &serde_json::to_string(&payload).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn latest_unknown_prompt_payload_returns_latest_unknown_event_only() {
        let (db, _dir) = test_db(STATE_PROMPT_PENDING);
        insert_unknown_event_with_text(&db, "first prompt?");
        insert_event_sync(
            &db.conn(),
            "a1",
            None,
            "output_chunk",
            r#"{"text":"not a prompt event"}"#,
        )
        .unwrap();
        insert_unknown_event_with_text(&db, "latest prompt?");

        let payload = latest_unknown_prompt_payload(&db, "a1").unwrap();
        assert_eq!(payload.pane_screenshot, "latest prompt?");
    }

    #[test]
    fn latest_unknown_prompt_payload_is_agent_scoped() {
        let (db, _dir) = test_db(STATE_PROMPT_PENDING);
        insert_agent_sync(
            &db.conn(),
            "a2",
            "s1",
            "codex",
            STATE_PROMPT_PENDING,
            Some(456),
        )
        .unwrap();
        insert_unknown_event_with_text(&db, "a1 prompt?");
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [3; 32],
                sanitized_text: "a2 prompt?".into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );
        insert_event_sync(
            &db.conn(),
            "a2",
            None,
            UNKNOWN_PROMPT_DETECTED,
            &serde_json::to_string(&payload).unwrap(),
        )
        .unwrap();

        let payload = latest_unknown_prompt_payload(&db, "a1").unwrap();
        assert_eq!(payload.pane_screenshot, "a1 prompt?");
    }

    #[test]
    fn action_value_accepts_single_or_array() {
        let single = serde_json::json!({"type":"key","value":"2"});
        let array = serde_json::json!([{"type":"key","value":"2"}]);

        assert_eq!(normalize_action_value(single).unwrap().len(), 1);
        assert_eq!(normalize_action_value(array).unwrap().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_pending_without_job_returns_idle() {
        let (db, dir) = test_db(STATE_PROMPT_PENDING);
        let io = FakeIo::default();

        let result = resolve_prompt_with_io(
            ResolvePromptRequest {
                db: db.clone(),
                agent_id: "a1".into(),
                pane_id: TmuxPaneId("%1".into()),
                provider: "codex".into(),
                kb_path: dir.path().join("prompt-cases.json"),
                actions: action(),
                save_to_kb: false,
            },
            &io,
        )
        .await
        .unwrap();

        assert_eq!(result.resolved_state, STATE_IDLE);
        assert_eq!(
            io.sent.lock().unwrap().as_slice(),
            ["key:Down", "key:Enter"]
        );
        assert_eq!(
            query_agent_sync(&db.conn(), "a1").unwrap().unwrap().state,
            STATE_IDLE
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_pending_with_dispatched_job_returns_busy() {
        let (db, dir) = test_db(STATE_IDLE);
        insert_job_sync(&db.conn(), "j1", "a1", None, "work").unwrap();
        let dispatched = crate::db::jobs::claim_next_job_sync(&db, "a1")
            .unwrap()
            .unwrap();
        assert_eq!(dispatched.status, "DISPATCHED");
        db.conn()
            .execute(
                "UPDATE agents SET state = ? WHERE id = 'a1'",
                [STATE_PROMPT_PENDING],
            )
            .unwrap();
        let io = FakeIo::default();

        let result = resolve_prompt_with_io(
            ResolvePromptRequest {
                db,
                agent_id: "a1".into(),
                pane_id: TmuxPaneId("%1".into()),
                provider: "codex".into(),
                kb_path: dir.path().join("prompt-cases.json"),
                actions: action(),
                save_to_kb: false,
            },
            &io,
        )
        .await
        .unwrap();

        assert_eq!(result.resolved_state, STATE_BUSY);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_rejects_non_pending_agent() {
        let (db, dir) = test_db(STATE_IDLE);
        let io = FakeIo::default();

        let err = resolve_prompt_with_io(
            ResolvePromptRequest {
                db,
                agent_id: "a1".into(),
                pane_id: TmuxPaneId("%1".into()),
                provider: "codex".into(),
                kb_path: dir.path().join("prompt-cases.json"),
                actions: action(),
                save_to_kb: false,
            },
            &io,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AhError::AgentWrongState { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_rejects_invalid_action() {
        let (db, dir) = test_db(STATE_PROMPT_PENDING);
        let io = FakeIo::default();

        let err = resolve_prompt_with_io(
            ResolvePromptRequest {
                db,
                agent_id: "a1".into(),
                pane_id: TmuxPaneId("%1".into()),
                provider: "codex".into(),
                kb_path: dir.path().join("prompt-cases.json"),
                actions: vec![PromptAction::Literal {
                    value: "yes; rm -rf /".into(),
                }],
                save_to_kb: false,
            },
            &io,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("invalid prompt action"));
        assert!(io.sent.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_save_to_kb_writes_literal_case() {
        let (db, dir) = test_db(STATE_PROMPT_PENDING);
        insert_unknown_event(&db);
        let kb_path = dir.path().join("prompt-cases.json");
        let io = FakeIo::default();

        let result = resolve_prompt_with_io(
            ResolvePromptRequest {
                db,
                agent_id: "a1".into(),
                pane_id: TmuxPaneId("%1".into()),
                provider: "codex".into(),
                kb_path: kb_path.clone(),
                actions: action(),
                save_to_kb: true,
            },
            &io,
        )
        .await
        .unwrap();

        let kb = load_or_bootstrap_kb(&kb_path).unwrap();
        let case = kb
            .cases
            .iter()
            .find(|case| Some(case.id.as_str()) == result.case_id.as_deref())
            .unwrap();
        assert_eq!(case.fingerprint.pattern(), "Manual prompt\\?");
        assert_eq!(case.action.len(), 2);
    }
}
