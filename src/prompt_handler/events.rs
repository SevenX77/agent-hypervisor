//! Event payloads and writers for prompt-handler pending prompts.

use crate::db::Db;
use crate::db::events::insert_event_and_notify;
#[cfg(test)]
use crate::db::events::insert_event_sync;
use crate::error::AhError;
use crate::prompt_handler::runner::PromptSnapshot;
use serde::{Deserialize, Serialize};

pub const UNKNOWN_PROMPT_DETECTED: &str = "UNKNOWN_PROMPT_DETECTED";
const PANE_SCREENSHOT_MAX_BYTES: usize = 4096;
const TRUNCATED_MARKER: &str = "\n[truncated]";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnknownPromptPayload {
    pub pane_screenshot: String,
    pub suggested_action: Option<String>,
    pub block_reason: String,
    pub capture_hash: String,
    pub depth: usize,
    pub provider: String,
}

impl UnknownPromptPayload {
    pub fn new(
        snapshot: &PromptSnapshot,
        block_reason: impl Into<String>,
        depth: usize,
        provider: impl Into<String>,
    ) -> Self {
        Self {
            pane_screenshot: truncate_pane_screenshot(&snapshot.sanitized_text),
            suggested_action: None,
            block_reason: block_reason.into(),
            capture_hash: hex_hash(&snapshot.sanitized_hash),
            depth,
            provider: provider.into(),
        }
    }
}

pub async fn emit_unknown_prompt_detected(
    db: Db,
    agent_id: String,
    payload: UnknownPromptPayload,
) -> Result<i64, AhError> {
    let payload_json = serde_json::to_string(&payload).map_err(|err| {
        AhError::IpcInvalidRequest(format!("serialize unknown prompt payload: {err}"))
    })?;
    insert_event_and_notify(
        db,
        agent_id,
        None,
        UNKNOWN_PROMPT_DETECTED.to_string(),
        payload_json,
    )
    .await
    .map(|frame| frame.event_id)
}

#[cfg(test)]
pub(crate) fn emit_unknown_prompt_detected_sync(
    conn: &rusqlite::Connection,
    agent_id: &str,
    payload: &UnknownPromptPayload,
) -> Result<i64, AhError> {
    tracing::info!(
        agent_id,
        block_reason = %payload.block_reason,
        depth = payload.depth,
        "unknown prompt event emit start"
    );
    let payload_json = serde_json::to_string(payload).map_err(|err| {
        AhError::IpcInvalidRequest(format!("serialize unknown prompt payload: {err}"))
    })?;
    let seq_id = insert_event_sync(conn, agent_id, None, UNKNOWN_PROMPT_DETECTED, &payload_json)
        .map_err(|err| {
            tracing::error!(
                agent_id,
                reason = %err,
                impact = "master will not see UNKNOWN_PROMPT_DETECTED event",
                "unknown prompt event emit failed"
            );
            err
        })?;
    tracing::info!(
        agent_id,
        seq_id,
        event_type = UNKNOWN_PROMPT_DETECTED,
        "unknown prompt event emit complete"
    );
    Ok(seq_id)
}

pub(crate) fn truncate_pane_screenshot(text: &str) -> String {
    if text.len() <= PANE_SCREENSHOT_MAX_BYTES {
        return text.to_string();
    }

    let mut end = PANE_SCREENSHOT_MAX_BYTES.saturating_sub(TRUNCATED_MARKER.len());
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}{}", &text[..end], TRUNCATED_MARKER)
}

pub(crate) fn hex_hash(hash: &[u8; 32]) -> String {
    hash.iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::{
        UNKNOWN_PROMPT_DETECTED, UnknownPromptPayload, emit_unknown_prompt_detected_sync,
        truncate_pane_screenshot,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::query_events_since_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::STATE_IDLE;
    use crate::db::{Db, init};
    use crate::prompt_handler::gating::hash_sanitized_text;
    use crate::prompt_handler::runner::PromptSnapshot;
    use serde_json::Value;

    fn with_test_db<T>(test: impl FnOnce(&Db, &mut rusqlite::Connection) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let mut conn = db.conn();
        insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
        insert_agent_sync(&conn, "a1", "s1", "codex", STATE_IDLE, Some(456)).unwrap();
        test(&db, &mut conn)
    }

    #[test]
    fn unknown_prompt_payload_schema_is_emitted_to_events_table() {
        with_test_db(|_db, conn| {
            let text = "Unexpected approval prompt";
            let snapshot = PromptSnapshot {
                sanitized_hash: hash_sanitized_text(text),
                sanitized_text: text.to_string(),
            };
            let payload = UnknownPromptPayload::new(&snapshot, "unknown_prompt", 0, "codex");

            let seq_id = emit_unknown_prompt_detected_sync(conn, "a1", &payload).unwrap();
            let events = query_events_since_sync(conn, "a1", 0).unwrap();

            assert_eq!(seq_id, events[0].seq_id);
            assert_eq!(events[0].event_type, UNKNOWN_PROMPT_DETECTED);
            let payload: Value = serde_json::from_str(&events[0].payload).unwrap();
            assert_eq!(payload["pane_screenshot"], text);
            assert_eq!(payload["suggested_action"], Value::Null);
            assert_eq!(payload["block_reason"], "unknown_prompt");
            assert_eq!(payload["depth"], 0);
            assert_eq!(payload["provider"], "codex");
            assert_eq!(payload["capture_hash"].as_str().unwrap().len(), 64);
        });
    }

    #[test]
    fn pane_screenshot_is_truncated_with_marker() {
        let long = "x".repeat(5000);
        let truncated = truncate_pane_screenshot(&long);

        assert!(truncated.len() <= 4096);
        assert!(truncated.ends_with("[truncated]"));
    }

    #[test]
    fn event_insert_failure_is_returned() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let snapshot = PromptSnapshot {
            sanitized_hash: [0; 32],
            sanitized_text: "prompt".to_string(),
        };
        let payload = UnknownPromptPayload::new(&snapshot, "unknown_prompt", 0, "codex");

        let err = emit_unknown_prompt_detected_sync(&conn, "missing", &payload).unwrap_err();

        assert!(err.to_string().contains("insert event"));
    }
}
