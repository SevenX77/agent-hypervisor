use crate::db::Db;
use crate::db::common::{is_constraint_error, map_db_error, spawn_db};
use crate::db::schema::Agent;
use crate::error::AhError;
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) fn insert_agent_sync(
    conn: &Connection,
    agent_id: &str,
    session_id: &str,
    provider: &str,
    state: &str,
    pid: Option<i64>,
) -> Result<(), AhError> {
    let lifecycle_id = crate::runtime_observation::store::new_lifecycle_id();
    insert_agent_with_lifecycle_id_sync(
        conn,
        agent_id,
        session_id,
        provider,
        &lifecycle_id,
        state,
        pid,
    )
}

pub(crate) fn insert_agent_with_lifecycle_id_sync(
    conn: &Connection,
    agent_id: &str,
    session_id: &str,
    provider: &str,
    lifecycle_id: &str,
    state: &str,
    pid: Option<i64>,
) -> Result<(), AhError> {
    if agent_id.starts_with("master:") {
        return Err(AhError::IpcInvalidRequest(
            "agent_id prefix 'master:' is reserved for master hook sentinels".to_string(),
        ));
    }
    match conn.execute(
        "INSERT INTO agents (id, session_id, provider, lifecycle_id, state, pid) VALUES (?, ?, ?, ?, ?, ?)",
        params![agent_id, session_id, provider, lifecycle_id, state, pid],
    ) {
        Ok(_) => {}
        Err(err) if is_constraint_error(&err) => {
            let existing_state = conn
                .query_row(
                    "SELECT state FROM agents WHERE id = ?",
                    params![agent_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|err| map_db_error("query existing agent for insert recycle", err))?;
            if existing_state.as_deref() != Some("KILLED") {
                return Err(AhError::AgentAlreadyExists(agent_id.to_string()));
            }
            conn.execute("DELETE FROM agents WHERE id = ?", params![agent_id])
                .map_err(|err| map_db_error("delete killed agent before insert", err))?;
            conn.execute(
                "INSERT INTO agents (id, session_id, provider, lifecycle_id, state, pid) VALUES (?, ?, ?, ?, ?, ?)",
                params![agent_id, session_id, provider, lifecycle_id, state, pid],
            )
            .map_err(|err| {
                if is_constraint_error(&err) {
                    AhError::AgentAlreadyExists(agent_id.to_string())
                } else {
                    map_db_error("insert recycled agent", err)
                }
            })?;
        }
        Err(err) => return Err(map_db_error("insert agent", err)),
    }

    notify_runtime_agent_changed();
    Ok(())
}

pub(crate) fn update_agent_state_sync(
    conn: &Connection,
    agent_id: &str,
    new_state: &str,
) -> Result<(), AhError> {
    let changes = conn
        .execute(
        "UPDATE agents SET state = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state != 'CRASHED'",
        params![new_state, agent_id],
    )
        .map_err(|err| map_db_error("update agent state", err))?;

    if changes > 0 {
        notify_runtime_agent_changed();
    }
    Ok(())
}

pub(crate) fn update_agent_config_hash_sync(
    conn: &Connection,
    agent_id: &str,
    config_hash: &str,
) -> Result<(), AhError> {
    conn.execute(
        "UPDATE agents SET config_hash = ?, updated_at = unixepoch() WHERE id = ?",
        params![config_hash, agent_id],
    )
    .map_err(|err| map_db_error("update agent config_hash", err))?;
    Ok(())
}

pub(crate) fn query_agent_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<Option<Agent>, AhError> {
    conn.query_row(
        "SELECT id, session_id, provider, lifecycle_id, state, state_version, pid, exit_code, error_code, sub_state, config_hash, created_at, updated_at FROM agents WHERE id = ?",
        params![agent_id],
        |row| {
            Ok(Agent {
                id: row.get(0)?,
                session_id: row.get(1)?,
                provider: row.get(2)?,
                lifecycle_id: row.get(3)?,
                state: row.get(4)?,
                state_version: row.get(5)?,
                pid: row.get(6)?,
                exit_code: row.get(7)?,
                error_code: row.get(8)?,
                sub_state: row.get(9)?,
                config_hash: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query agent", err))
}

pub(crate) fn query_agents_by_state_sync(
    conn: &Connection,
    state: &str,
) -> Result<Vec<Agent>, AhError> {
    let mut stmt = conn
        .prepare("SELECT id, session_id, provider, lifecycle_id, state, state_version, pid, exit_code, error_code, sub_state, config_hash, created_at, updated_at FROM agents WHERE state = ? ORDER BY updated_at ASC, id ASC")
        .map_err(|err| map_db_error("prepare query agents by state", err))?;
    let rows = stmt
        .query_map(params![state], |row| {
            Ok(Agent {
                id: row.get(0)?,
                session_id: row.get(1)?,
                provider: row.get(2)?,
                lifecycle_id: row.get(3)?,
                state: row.get(4)?,
                state_version: row.get(5)?,
                pid: row.get(6)?,
                exit_code: row.get(7)?,
                error_code: row.get(8)?,
                sub_state: row.get(9)?,
                config_hash: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        })
        .map_err(|err| map_db_error("query agents by state", err))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect agents by state", err))
}

pub(crate) fn agent_exists_sync(conn: &Connection, agent_id: &str) -> Result<bool, AhError> {
    conn.query_row(
        "SELECT 1 FROM agents WHERE id = ? LIMIT 1",
        params![agent_id],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(|err| map_db_error("check agent uniqueness", err))
}

pub(crate) fn query_agent_state_sync(
    db: &Db,
    agent_id: &str,
) -> Result<Option<(String, i64)>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT state, state_version FROM agents WHERE id = ?",
        params![agent_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(|err| map_db_error("query agent state", err))
}

pub(crate) fn delete_agent_sync(db: &Db, agent_id: &str) -> Result<(), AhError> {
    let conn = db.conn();
    let changes = conn
        .execute("DELETE FROM agents WHERE id = ?", params![agent_id])
        .map_err(|err| map_db_error("delete agent", err))?;
    if changes > 0 {
        notify_runtime_agent_changed();
    }
    Ok(())
}

fn notify_runtime_agent_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::AgentChanged,
    );
}

pub async fn insert_agent(
    db: Db,
    agent_id: String,
    session_id: String,
    provider: String,
    state: String,
    pid: Option<i64>,
) -> Result<(), AhError> {
    spawn_db("agents::insert_agent", move || {
        let conn = db.conn();
        insert_agent_sync(&conn, &agent_id, &session_id, &provider, &state, pid)
    })
    .await
}

pub(crate) async fn insert_agent_with_lifecycle_id(
    db: Db,
    agent_id: String,
    session_id: String,
    provider: String,
    lifecycle_id: String,
    state: String,
    pid: Option<i64>,
) -> Result<(), AhError> {
    spawn_db("agents::insert_agent_with_lifecycle_id", move || {
        let conn = db.conn();
        insert_agent_with_lifecycle_id_sync(
            &conn,
            &agent_id,
            &session_id,
            &provider,
            &lifecycle_id,
            &state,
            pid,
        )
    })
    .await
}

pub async fn update_agent_state(
    db: Db,
    agent_id: String,
    new_state: String,
) -> Result<(), AhError> {
    spawn_db("agents::update_agent_state", move || {
        let conn = db.conn();
        update_agent_state_sync(&conn, &agent_id, &new_state)
    })
    .await
}

pub async fn update_agent_config_hash(
    db: Db,
    agent_id: String,
    config_hash: String,
) -> Result<(), AhError> {
    spawn_db("agents::update_agent_config_hash", move || {
        let conn = db.conn();
        update_agent_config_hash_sync(&conn, &agent_id, &config_hash)
    })
    .await
}

pub async fn query_agent(db: Db, agent_id: String) -> Result<Option<Agent>, AhError> {
    spawn_db("agents::query_agent", move || {
        let conn = db.conn();
        query_agent_sync(&conn, &agent_id)
    })
    .await
}

pub async fn query_agents_by_state(db: Db, state: String) -> Result<Vec<Agent>, AhError> {
    spawn_db("agents::query_agents_by_state", move || {
        let conn = db.conn();
        query_agents_by_state_sync(&conn, &state)
    })
    .await
}

pub async fn agent_exists(db: Db, agent_id: String) -> Result<bool, AhError> {
    spawn_db("agents::agent_exists", move || {
        let conn = db.conn();
        agent_exists_sync(&conn, &agent_id)
    })
    .await
}

pub async fn query_agent_state(db: Db, agent_id: String) -> Result<Option<(String, i64)>, AhError> {
    spawn_db("agents::query_agent_state", move || {
        query_agent_state_sync(&db, &agent_id)
    })
    .await
}

pub async fn delete_agent(db: Db, agent_id: String) -> Result<(), AhError> {
    spawn_db("agents::delete_agent", move || {
        delete_agent_sync(&db, &agent_id)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        delete_agent_sync, insert_agent_sync, query_agent_sync, query_agents_by_state_sync,
        update_agent_state_sync,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};
    use crate::error::AhError;

    fn with_test_db<T>(test: impl FnOnce(&mut rusqlite::Connection) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let mut conn = db.conn();
        test(&mut conn)
    }

    fn with_test_db_handle<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_agent(conn: &rusqlite::Connection) {
        insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
    }

    #[test]
    fn test_query_agent_sub_state_defaults_none() {
        with_test_db(|conn| {
            seed_agent(conn);
            let agent = query_agent_sync(conn, "a1").unwrap().unwrap();
            assert_eq!(agent.sub_state, None);
        });
    }

    #[test]
    fn test_query_agents_by_state() {
        with_test_db(|conn| {
            seed_agent(conn);
            insert_agent_sync(conn, "a2", "s1", "bash", "BUSY", Some(456)).unwrap();

            let idle = query_agents_by_state_sync(conn, "IDLE").unwrap();
            let busy = query_agents_by_state_sync(conn, "BUSY").unwrap();

            assert_eq!(idle.len(), 1);
            assert_eq!(idle[0].id, "a1");
            assert_eq!(busy.len(), 1);
            assert_eq!(busy[0].id, "a2");
        });
    }

    #[test]
    fn test_insert_agent_duplicate_id() {
        with_test_db(|conn| {
            seed_agent(conn);
            let err = insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(456)).unwrap_err();
            assert!(matches!(err, AhError::AgentAlreadyExists(agent_id) if agent_id == "a1"));
        });
    }

    #[test]
    fn test_insert_agent_rejects_reserved_master_prefix() {
        with_test_db(|conn| {
            insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
            let err = insert_agent_sync(conn, "master:s1:1", "s1", "bash", "IDLE", Some(123))
                .unwrap_err();

            assert!(
                matches!(err, AhError::IpcInvalidRequest(message) if message.contains("reserved"))
            );
        });
    }

    #[test]
    fn bug_c_insert_agent_recycles_killed_slot() {
        with_test_db(|conn| {
            insert_session_sync(conn, "s1", "p1", "/tmp/old").unwrap();
            insert_session_sync(conn, "s2", "p2", "/tmp/new").unwrap();
            insert_agent_sync(conn, "a1", "s1", "bash", "KILLED", Some(123)).unwrap();

            insert_agent_sync(conn, "a1", "s2", "bash", "SPAWNING", Some(456)).unwrap();

            let agent = query_agent_sync(conn, "a1").unwrap().unwrap();
            assert_eq!(agent.session_id, "s2");
            assert_eq!(agent.state, "SPAWNING");
            assert_eq!(agent.pid, Some(456));
        });
    }

    #[test]
    fn test_update_agent_state_does_not_overwrite_crashed() {
        with_test_db(|conn| {
            seed_agent(conn);
            update_agent_state_sync(conn, "a1", "BUSY").unwrap();
            let state: String = conn
                .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(state, "BUSY");
            conn.execute("UPDATE agents SET state = 'CRASHED' WHERE id = 'a1'", [])
                .unwrap();
            update_agent_state_sync(conn, "a1", "BUSY").unwrap();
            let state: String = conn
                .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(state, "CRASHED");
        });
    }

    #[test]
    fn test_delete_agent_removes_row() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                seed_agent(&conn);
            }
            delete_agent_sync(db, "a1").unwrap();
            let count: i64 = db
                .conn()
                .query_row("SELECT COUNT(*) FROM agents WHERE id = 'a1'", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
        });
    }
}
