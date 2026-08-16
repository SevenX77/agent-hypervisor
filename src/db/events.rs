use crate::db::Db;
use crate::db::common::{is_unique_constraint_error, map_db_error, spawn_db};
use crate::db::schema::Event;
use crate::error::AhError;
use crate::orchestrator::pubsub::EventFrame;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

pub const UNKNOWN_PATTERN_STABLE: &str = "UNKNOWN_PATTERN_STABLE";
pub(crate) const AH_IDLE_MARKER_PREFIX: &str = "<<ah-idle:job-id=";

pub(crate) fn query_event_by_request_id_sync(
    conn: &Connection,
    agent_id: &str,
    request_id: &str,
) -> Result<Option<Event>, AhError> {
    conn.query_row(
        "SELECT seq_id, event_type, payload, created_at FROM events WHERE agent_id = ? AND request_id = ? LIMIT 1",
        params![agent_id, request_id],
        |row| {
            Ok(Event {
                seq_id: row.get(0)?,
                agent_id: agent_id.to_string(),
                request_id: Some(request_id.to_string()),
                event_type: row.get(1)?,
                payload: row.get(2)?,
                created_at: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query event by request_id", err))
}

pub(crate) fn insert_event_sync(
    conn: &Connection,
    agent_id: &str,
    request_id: Option<&str>,
    event_type: &str,
    payload: &str,
) -> Result<i64, AhError> {
    let result = conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, ?, ?, ?)",
        params![agent_id, request_id, event_type, payload],
    );

    match result {
        Ok(_) => Ok(conn.last_insert_rowid()),
        Err(err) if is_unique_constraint_error(&err) && request_id.is_some() => {
            let existing_seq_id = conn
                .query_row(
                    "SELECT seq_id FROM events WHERE agent_id = ? AND request_id = ? LIMIT 1",
                    params![agent_id, request_id],
                    |row| row.get(0),
                )
                .map_err(|select_err| {
                    map_db_error("query duplicate event by request_id", select_err)
                })?;

            Err(AhError::DuplicateRequest { existing_seq_id })
        }
        Err(err) => Err(map_db_error("insert event", err)),
    }
}

pub(crate) fn query_events_since_sync(
    conn: &Connection,
    agent_id: &str,
    since_seq_id: i64,
) -> Result<Vec<Event>, AhError> {
    let mut stmt = conn
        .prepare(
            "SELECT seq_id, request_id, event_type, payload, created_at FROM events WHERE agent_id = ? AND seq_id > ? ORDER BY seq_id ASC",
        )
        .map_err(|err| map_db_error("prepare query events since", err))?;
    let rows = stmt
        .query_map(params![agent_id, since_seq_id], |row| {
            Ok(Event {
                seq_id: row.get(0)?,
                agent_id: agent_id.to_string(),
                request_id: row.get(1)?,
                event_type: row.get(2)?,
                payload: row.get(3)?,
                created_at: row.get(4)?,
            })
        })
        .map_err(|err| map_db_error("query events since", err))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect events since", err))
}

pub(crate) fn query_last_event_of_type_sync(
    conn: &Connection,
    agent_id: &str,
    event_type: &str,
) -> Result<Option<Event>, AhError> {
    conn.query_row(
        "SELECT seq_id, request_id, event_type, payload, created_at FROM events WHERE agent_id = ? AND event_type = ? ORDER BY seq_id DESC LIMIT 1",
        params![agent_id, event_type],
        |row| {
            Ok(Event {
                seq_id: row.get(0)?,
                agent_id: agent_id.to_string(),
                request_id: row.get(1)?,
                event_type: row.get(2)?,
                payload: row.get(3)?,
                created_at: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query last event of type", err))
}

pub(crate) fn query_last_event_of_type_matching_payload_sync(
    conn: &Connection,
    agent_id: &str,
    event_type: &str,
    payload_like: &str,
    before_seq_id: Option<i64>,
) -> Result<Option<Event>, AhError> {
    conn.query_row(
        "SELECT seq_id, request_id, event_type, payload, created_at FROM events WHERE agent_id = ?1 AND event_type = ?2 AND payload LIKE ?3 AND (?4 IS NULL OR seq_id < ?4) ORDER BY seq_id DESC LIMIT 1",
        params![agent_id, event_type, payload_like, before_seq_id],
        |row| {
            Ok(Event {
                seq_id: row.get(0)?,
                agent_id: agent_id.to_string(),
                request_id: row.get(1)?,
                event_type: row.get(2)?,
                payload: row.get(3)?,
                created_at: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query last event of type matching payload", err))
}

pub(crate) fn query_events_backfill_sync(
    conn: &Connection,
    since_seq_id: i64,
    agent_id: Option<&str>,
    kinds: Option<&[String]>,
) -> Result<Vec<EventFrame>, AhError> {
    let mut stmt = conn
        .prepare(
            "SELECT e.seq_id, e.agent_id, e.event_type, e.payload, e.created_at, a.state
             FROM events e
             JOIN agents a ON a.id = e.agent_id
             WHERE e.seq_id > ?
               AND (?2 IS NULL OR e.agent_id = ?2)
             ORDER BY e.seq_id ASC",
        )
        .map_err(|err| map_db_error("prepare query events backfill", err))?;
    let rows = stmt
        .query_map(params![since_seq_id, agent_id], |row| {
            let seq_id = row.get(0)?;
            let agent_id: String = row.get(1)?;
            let event_type: String = row.get(2)?;
            let payload: String = row.get(3)?;
            let created_at: i64 = row.get(4)?;
            let state: Option<String> = row.get(5)?;
            Ok(event_frame_from_parts(
                seq_id, event_type, agent_id, state, created_at, &payload,
            ))
        })
        .map_err(|err| map_db_error("query events backfill", err))?;

    let frames = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect events backfill", err))?;
    Ok(frames
        .into_iter()
        .filter(|frame| kinds.is_none_or(|kinds| kinds.iter().any(|kind| kind == &frame.kind)))
        .collect())
}

pub async fn query_event_by_request_id(
    db: Db,
    agent_id: String,
    request_id: String,
) -> Result<Option<Event>, AhError> {
    spawn_db("events::query_event_by_request_id", move || {
        let conn = db.conn();
        query_event_by_request_id_sync(&conn, &agent_id, &request_id)
    })
    .await
}

pub async fn has_queued_starvation_alerted(
    db: Db,
    agent_id: String,
    job_id: String,
) -> Result<bool, AhError> {
    spawn_db("events::has_queued_starvation_alerted", move || {
        let conn = db.conn();
        let job_id_pattern = format!("%\"job_id\":\"{}\"%", job_id);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%\"reason\":\"QUEUED_STARVATION_ALERT\"%' AND payload LIKE ?",
            params![agent_id, job_id_pattern],
            |row| row.get(0),
        )
        .map_err(|err| map_db_error("check if queued starvation alerted", err))?;
        Ok(count > 0)
    })
    .await
}

pub async fn has_prompt_pending_suppression_escalated(
    db: Db,
    agent_id: String,
    hash: String,
) -> Result<bool, AhError> {
    spawn_db("events::has_prompt_pending_suppression_escalated", move || {
        let conn = db.conn();
        let hash_pattern = format!("%\"hash\":\"{}\"%", hash);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%\"reason\":\"PROMPT_PENDING_SUPPRESSION_ESCALATED\"%' AND payload LIKE ?",
            params![agent_id, hash_pattern],
            |row| row.get(0),
        )
        .map_err(|err| map_db_error("check if prompt pending suppression escalated", err))?;
        Ok(count > 0)
    })
    .await
}

pub async fn insert_event(
    db: Db,
    agent_id: String,
    request_id: Option<String>,
    event_type: String,
    payload: String,
) -> Result<i64, AhError> {
    let should_try_idle_marker = event_type == "output_chunk"
        && crate::db::state_machine::extract_ah_idle_marker_job_id(
            serde_json::from_str::<serde_json::Value>(&payload)
                .ok()
                .and_then(|value| {
                    value
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .as_deref()
                .unwrap_or(&payload),
        )
        .is_some();
    let marker_db = db.clone();
    let marker_agent_id = agent_id.clone();
    let seq_id = spawn_db("events::insert_event", move || {
        let conn = db.conn();
        insert_event_sync(
            &conn,
            &agent_id,
            request_id.as_deref(),
            &event_type,
            &payload,
        )
    })
    .await?;
    if should_try_idle_marker {
        match crate::db::state_machine::mark_agent_idle_matched(marker_db, marker_agent_id.clone())
            .await
        {
            Ok((changes, affected_job)) if changes > 0 => {
                if let Some(job_id) = affected_job {
                    crate::orchestrator::pubsub::notify_job_update(&job_id);
                }
                crate::orchestrator::wake_up();
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(agent_id = %marker_agent_id, error = %err, "failed to mark IDLE from ah idle marker event");
            }
        }
    }
    Ok(seq_id)
}

pub async fn insert_event_and_notify(
    db: Db,
    agent_id: String,
    request_id: Option<String>,
    event_type: String,
    payload: String,
) -> Result<EventFrame, AhError> {
    let frame = spawn_db("events::insert_event_and_notify", move || {
        let conn = db.conn();
        let seq_id = insert_event_sync(
            &conn,
            &agent_id,
            request_id.as_deref(),
            &event_type,
            &payload,
        )?;
        let state = conn
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                [&agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query agent state for event frame", err))?;
        Ok(event_frame_from_parts(
            seq_id,
            event_type,
            agent_id,
            state,
            now_unix_seconds(),
            &payload,
        ))
    })
    .await?;
    crate::orchestrator::pubsub::notify_event(frame.clone());
    Ok(frame)
}

pub async fn query_events_since(
    db: Db,
    agent_id: String,
    since_seq_id: i64,
) -> Result<Vec<Event>, AhError> {
    spawn_db("events::query_events_since", move || {
        let conn = db.conn();
        query_events_since_sync(&conn, &agent_id, since_seq_id)
    })
    .await
}

pub async fn query_last_event_of_type(
    db: Db,
    agent_id: String,
    event_type: String,
) -> Result<Option<Event>, AhError> {
    spawn_db("events::query_last_event_of_type", move || {
        let conn = db.conn();
        query_last_event_of_type_sync(&conn, &agent_id, &event_type)
    })
    .await
}

pub async fn query_last_event_of_type_matching_payload(
    db: Db,
    agent_id: String,
    event_type: String,
    payload_like: String,
    before_seq_id: Option<i64>,
) -> Result<Option<Event>, AhError> {
    spawn_db(
        "events::query_last_event_of_type_matching_payload",
        move || {
            let conn = db.conn();
            query_last_event_of_type_matching_payload_sync(
                &conn,
                &agent_id,
                &event_type,
                &payload_like,
                before_seq_id,
            )
        },
    )
    .await
}

pub async fn query_events_backfill(
    db: Db,
    since_seq_id: i64,
    agent_id: Option<String>,
    kinds: Option<Vec<String>>,
) -> Result<Vec<EventFrame>, AhError> {
    spawn_db("events::query_events_backfill", move || {
        let conn = db.conn();
        query_events_backfill_sync(&conn, since_seq_id, agent_id.as_deref(), kinds.as_deref())
    })
    .await
}

fn event_frame_from_parts(
    seq_id: i64,
    event_type: String,
    agent_id: String,
    state: Option<String>,
    created_at: i64,
    payload: &str,
) -> EventFrame {
    let payload_value = serde_json::from_str::<Value>(payload).ok();
    let job_id = payload_value
        .as_ref()
        .and_then(|value| value.get("job_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let state = payload_value
        .as_ref()
        .and_then(|value| value.get("to").or_else(|| value.get("state")))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or(state);

    EventFrame {
        event_id: seq_id,
        kind: event_type_to_frame_kind(&event_type, payload_value.as_ref()),
        agent_id,
        job_id,
        state,
        ts_unix_micro: created_at.saturating_mul(1_000_000),
        payload: payload_value,
    }
}

fn event_type_to_frame_kind(event_type: &str, payload: Option<&Value>) -> String {
    match event_type {
        UNKNOWN_PATTERN_STABLE => "unknown_pattern".to_string(),
        "UNKNOWN_PROMPT_DETECTED" => "unknown_prompt".to_string(),
        "state_change"
            if payload
                .and_then(|value| value.get("to"))
                .and_then(Value::as_str)
                == Some("STUCK") =>
        {
            "stuck".to_string()
        }
        "state_change" => "state_change".to_string(),
        other => other.to_string(),
    }
}

fn now_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        UNKNOWN_PATTERN_STABLE, insert_event_and_notify, insert_event_sync,
        query_event_by_request_id_sync, query_events_backfill_sync, query_events_since_sync,
        query_last_event_of_type_matching_payload_sync, query_last_event_of_type_sync,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::init;
    use crate::db::sessions::insert_session_sync;
    use crate::error::AhError;

    fn with_test_db<T>(test: impl FnOnce(&mut rusqlite::Connection) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let mut conn = db.conn();
        test(&mut conn)
    }

    fn seed_agent(conn: &rusqlite::Connection) {
        insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
    }

    #[test]
    fn test_insert_event_idempotent() {
        with_test_db(|conn| {
            seed_agent(conn);
            let seq_id = insert_event_sync(
                conn,
                "a1",
                Some("req-1"),
                "command_received",
                r#"{"cmd":"echo 1\n"}"#,
            )
            .unwrap();
            let err = insert_event_sync(
                conn,
                "a1",
                Some("req-1"),
                "command_received",
                r#"{"cmd":"echo 2\n"}"#,
            )
            .unwrap_err();
            assert!(
                matches!(err, AhError::DuplicateRequest { existing_seq_id } if existing_seq_id == seq_id)
            );
        });
    }

    #[test]
    fn test_insert_event_no_request_id_no_unique() {
        with_test_db(|conn| {
            seed_agent(conn);
            let seq_1 =
                insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"one"}"#).unwrap();
            let seq_2 =
                insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"two"}"#).unwrap();
            assert_ne!(seq_1, seq_2);
        });
    }

    #[test]
    fn test_query_event_by_request_id_found_and_missing() {
        with_test_db(|conn| {
            seed_agent(conn);
            let seq_id = insert_event_sync(
                conn,
                "a1",
                Some("req-1"),
                "command_received",
                r#"{"cmd":"echo 1\n"}"#,
            )
            .unwrap();
            let found = query_event_by_request_id_sync(conn, "a1", "req-1")
                .unwrap()
                .unwrap();
            let missing = query_event_by_request_id_sync(conn, "a1", "req-2").unwrap();
            assert_eq!(found.seq_id, seq_id);
            assert_eq!(found.agent_id, "a1");
            assert_eq!(found.request_id.as_deref(), Some("req-1"));
            assert!(missing.is_none());
        });
    }

    #[test]
    fn test_query_events_since() {
        with_test_db(|conn| {
            seed_agent(conn);
            insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"one"}"#).unwrap();
            let seq_2 =
                insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"two"}"#).unwrap();
            let events = query_events_since_sync(conn, "a1", seq_2 - 1).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].seq_id, seq_2);
        });
    }

    #[test]
    fn test_query_last_event_of_type_returns_latest_matching_type() {
        with_test_db(|conn| {
            seed_agent(conn);
            assert!(
                query_last_event_of_type_sync(conn, "a1", "output_chunk")
                    .unwrap()
                    .is_none()
            );

            let first =
                insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"one"}"#).unwrap();
            insert_event_sync(conn, "a1", None, "state_change", r#"{"to":"BUSY"}"#).unwrap();
            let second =
                insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"two"}"#).unwrap();

            let event = query_last_event_of_type_sync(conn, "a1", "output_chunk")
                .unwrap()
                .unwrap();
            assert_eq!(event.seq_id, second);
            assert!(event.seq_id > first);
            assert_eq!(event.payload, r#"{"text":"two"}"#);
        });
    }

    #[test]
    fn test_query_last_event_of_type_is_agent_scoped() {
        with_test_db(|conn| {
            seed_agent(conn);
            insert_agent_sync(conn, "a2", "s1", "bash", "IDLE", Some(456)).unwrap();
            insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"a1"}"#).unwrap();
            let a2_seq =
                insert_event_sync(conn, "a2", None, "output_chunk", r#"{"text":"a2"}"#).unwrap();

            let event = query_last_event_of_type_sync(conn, "a2", "output_chunk")
                .unwrap()
                .unwrap();
            assert_eq!(event.seq_id, a2_seq);
            assert_eq!(event.agent_id, "a2");
            assert_eq!(event.payload, r#"{"text":"a2"}"#);
        });
    }

    #[test]
    fn test_query_last_event_of_type_matching_payload_finds_latest_like_candidate() {
        with_test_db(|conn| {
            seed_agent(conn);
            let marker_seq = insert_event_sync(
                conn,
                "a1",
                None,
                "output_chunk",
                r#"{"text":"done <<ah-idle:job-id=j1>>"}"#,
            )
            .unwrap();
            insert_event_sync(conn, "a1", None, "output_chunk", r#"{"text":"after"}"#).unwrap();

            let event = query_last_event_of_type_matching_payload_sync(
                conn,
                "a1",
                "output_chunk",
                "%<<ah-idle:job-id=%",
                None,
            )
            .unwrap()
            .unwrap();
            assert_eq!(event.seq_id, marker_seq);

            let before = query_last_event_of_type_matching_payload_sync(
                conn,
                "a1",
                "output_chunk",
                "%<<ah-idle:job-id=%",
                Some(marker_seq),
            )
            .unwrap();
            assert!(before.is_none());
        });
    }

    #[test]
    fn test_events_schema_has_agent_type_seq_index() {
        with_test_db(|conn| {
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_events_agent_type_seq'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                sql,
                "CREATE INDEX idx_events_agent_type_seq ON events(agent_id, event_type, seq_id)"
            );
        });
    }

    #[test]
    fn test_unknown_pattern_stable_event_can_be_inserted_and_backfilled() {
        with_test_db(|conn| {
            seed_agent(conn);
            let seq_id = insert_event_sync(
                conn,
                "a1",
                None,
                UNKNOWN_PATTERN_STABLE,
                r#"{"category_hint":"StartupReadiness"}"#,
            )
            .unwrap();
            let frames = query_events_backfill_sync(
                conn,
                seq_id - 1,
                Some("a1"),
                Some(&["unknown_pattern".to_string()]),
            )
            .unwrap();

            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].event_id, seq_id);
            assert_eq!(frames[0].kind, "unknown_pattern");
            assert_eq!(
                frames[0].payload.as_ref().unwrap()["category_hint"],
                "StartupReadiness"
            );
        });
    }

    #[tokio::test]
    async fn test_insert_event_and_notify_returns_frame_with_inserted_seq_id() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            seed_agent(&conn);
        }

        let frame = insert_event_and_notify(
            db,
            "a1".to_string(),
            None,
            UNKNOWN_PATTERN_STABLE.to_string(),
            r#"{"category_hint":"StartupReadiness"}"#.to_string(),
        )
        .await
        .unwrap();

        assert!(frame.event_id > 0);
        assert_eq!(frame.kind, "unknown_pattern");
        assert_eq!(frame.agent_id, "a1");
        assert_eq!(frame.state.as_deref(), Some("IDLE"));
    }
}
