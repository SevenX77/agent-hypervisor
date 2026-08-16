use crate::db::common::map_db_error;
use crate::error::AhError;
use crate::runtime_observation::{EvidenceSource, ProviderObservation, ProviderObservationKind};
use rusqlite::{Connection, OptionalExtension, params};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn new_lifecycle_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

pub(crate) fn now_epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_for_agent_sync(
    conn: &Connection,
    observation_id: &str,
    agent_id: &str,
    expected_lifecycle_id: &str,
    turn_id: Option<&str>,
    source: EvidenceSource,
    kind: ProviderObservationKind,
    observed_at_ms: i64,
) -> Result<bool, AhError> {
    let identity = conn
        .query_row(
            "SELECT session_id, provider, lifecycle_id FROM agents WHERE id = ?1",
            [agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query provider observation identity", err))?
        .ok_or_else(|| AhError::AgentNotFound(agent_id.to_owned()))?;

    if identity.2 != expected_lifecycle_id {
        return Err(AhError::IpcInvalidRequest(format!(
            "stale provider observation for agent {agent_id}: expected lifecycle {}, got {expected_lifecycle_id}",
            identity.2
        )));
    }

    let observation = ProviderObservation {
        observation_id: observation_id.to_owned(),
        agent_id: agent_id.to_owned(),
        session_id: identity.0,
        provider: identity.1,
        lifecycle_id: identity.2,
        turn_id: turn_id.map(str::to_owned),
        source,
        observed_at_ms,
        kind,
    };
    let observation_json = serde_json::to_string(&observation).map_err(|err| {
        AhError::DbConstraintViolation(format!("serialize provider observation: {err}"))
    })?;

    let changes = conn
        .execute(
            "INSERT INTO provider_status_observations (
                 observation_id, agent_id, lifecycle_id, turn_id,
                 observation_json, observed_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(observation_id) DO NOTHING",
            params![
                observation.observation_id,
                observation.agent_id,
                observation.lifecycle_id,
                observation.turn_id,
                observation_json,
                observation.observed_at_ms,
            ],
        )
        .map_err(|err| map_db_error("insert provider observation", err))?;

    if changes == 0 {
        let existing = conn
            .query_row(
                "SELECT observation_json FROM provider_status_observations WHERE observation_id = ?1",
                [observation_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(|err| map_db_error("query duplicate provider observation", err))?;
        if existing != observation_json {
            return Err(AhError::DbConstraintViolation(format!(
                "provider observation id {observation_id:?} was reused with different content"
            )));
        }
    }

    Ok(changes == 1)
}

pub(crate) fn append_for_current_lifecycle_sync(
    conn: &Connection,
    observation_id: &str,
    agent_id: &str,
    turn_id: Option<&str>,
    source: EvidenceSource,
    kind: ProviderObservationKind,
    observed_at_ms: i64,
) -> Result<bool, AhError> {
    let lifecycle_id = conn
        .query_row(
            "SELECT lifecycle_id FROM agents WHERE id = ?1",
            [agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query current provider lifecycle", err))?
        .ok_or_else(|| AhError::AgentNotFound(agent_id.to_owned()))?;
    append_for_agent_sync(
        conn,
        observation_id,
        agent_id,
        &lifecycle_id,
        turn_id,
        source,
        kind,
        observed_at_ms,
    )
}

pub(crate) fn query_scope_sync(
    conn: &Connection,
    agent_id: &str,
    lifecycle_id: &str,
    turn_id: Option<&str>,
) -> Result<Vec<ProviderObservation>, AhError> {
    let mut statement = conn
        .prepare(
            "SELECT observation_json
             FROM provider_status_observations
             WHERE agent_id = ?1 AND lifecycle_id = ?2
             ORDER BY observed_at_ms ASC, seq_id ASC",
        )
        .map_err(|err| map_db_error("prepare provider observation query", err))?;
    let rows = statement
        .query_map(params![agent_id, lifecycle_id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|err| map_db_error("query provider observations", err))?;
    let encoded = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect provider observations", err))?;

    encoded
        .into_iter()
        .map(|json| {
            serde_json::from_str::<ProviderObservation>(&json).map_err(|err| {
                AhError::DbConstraintViolation(format!(
                    "decode provider observation for agent {agent_id}: {err}"
                ))
            })
        })
        .filter_map(|result| match result {
            Ok(observation) => {
                let in_scope = match observation.kind {
                    ProviderObservationKind::Process(_) => observation.turn_id.is_none(),
                    ProviderObservationKind::Turn(_) => observation.turn_id.as_deref() == turn_id,
                };
                in_scope.then_some(Ok(observation))
            }
            Err(err) => Some(Err(err)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::runtime_observation::{ProviderProcessState, ProviderTurnState};

    fn database() -> (tempfile::NamedTempFile, db::Db) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let database = db::init(file.path()).unwrap();
        {
            let conn = database.conn();
            conn.execute(
                "INSERT INTO projects (id, absolute_path) VALUES ('p1', '/p1')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1', 'p1', 1)",
                [],
            )
            .unwrap();
            insert_agent_sync(&conn, "a1", "s1", "codex", "IDLE", Some(10)).unwrap();
        }
        (file, database)
    }

    #[test]
    fn observation_store_fences_lifecycle_and_turn_scope() {
        let (_file, database) = database();
        let conn = database.conn();
        let lifecycle_id: String = conn
            .query_row(
                "SELECT lifecycle_id FROM agents WHERE id = 'a1'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        append_for_agent_sync(
            &conn,
            "process-alive",
            "a1",
            &lifecycle_id,
            None,
            EvidenceSource::ProcessProbe,
            ProviderObservationKind::Process(ProviderProcessState::Alive),
            100,
        )
        .unwrap();
        append_for_agent_sync(
            &conn,
            "job-one-working",
            "a1",
            &lifecycle_id,
            Some("job-1"),
            EvidenceSource::TerminalPane,
            ProviderObservationKind::Turn(ProviderTurnState::Working),
            101,
        )
        .unwrap();
        append_for_agent_sync(
            &conn,
            "job-two-complete",
            "a1",
            &lifecycle_id,
            Some("job-2"),
            EvidenceSource::OfficialHook,
            ProviderObservationKind::Turn(ProviderTurnState::Completed),
            102,
        )
        .unwrap();

        let job_one = query_scope_sync(&conn, "a1", &lifecycle_id, Some("job-1")).unwrap();
        assert_eq!(job_one.len(), 2);
        assert!(job_one.iter().any(|observation| {
            matches!(
                observation.kind,
                ProviderObservationKind::Turn(ProviderTurnState::Working)
            )
        }));
        assert!(!job_one.iter().any(|observation| {
            matches!(
                observation.kind,
                ProviderObservationKind::Turn(ProviderTurnState::Completed)
            )
        }));

        let stale = append_for_agent_sync(
            &conn,
            "stale",
            "a1",
            "old-lifecycle",
            None,
            EvidenceSource::OfficialHook,
            ProviderObservationKind::Turn(ProviderTurnState::Ready),
            103,
        );
        assert!(stale.is_err());
    }

    #[test]
    fn observation_id_retry_is_idempotent_but_collision_is_rejected() {
        let (_file, database) = database();
        let conn = database.conn();
        let lifecycle_id: String = conn
            .query_row(
                "SELECT lifecycle_id FROM agents WHERE id = 'a1'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let append = |state| {
            append_for_agent_sync(
                &conn,
                "same-id",
                "a1",
                &lifecycle_id,
                None,
                EvidenceSource::OfficialHook,
                ProviderObservationKind::Turn(state),
                100,
            )
        };
        assert_eq!(append(ProviderTurnState::Ready).unwrap(), true);
        assert_eq!(append(ProviderTurnState::Ready).unwrap(), false);
        assert!(append(ProviderTurnState::Working).is_err());
    }
}
