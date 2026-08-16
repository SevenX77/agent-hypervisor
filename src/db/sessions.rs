use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::db::schema::Session;
use crate::error::AhError;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: String,
    pub project_id: String,
    pub absolute_path: String,
    pub status: String,
    pub master_state: String,
    pub master_pane_id: Option<String>,
    pub master_provider: Option<String>,
    pub db_tracked_agents: i64,
    /// Deprecated compatibility alias for unversioned `session.list` consumers.
    pub active_agents: i64,
    pub created_at: i64,
}

pub(crate) fn insert_session_sync(
    conn: &Connection,
    session_id: &str,
    project_id: &str,
    absolute_path: &str,
) -> Result<(), AhError> {
    conn.execute(
        "INSERT OR IGNORE INTO projects (id, absolute_path) VALUES (?, ?)",
        params![project_id, absolute_path],
    )
    .map_err(|err| map_db_error("insert project", err))?;

    conn.execute(
        "INSERT INTO sessions (id, project_id, master_pid) VALUES (?, ?, ?)",
        params![session_id, project_id, 0_i64],
    )
    .map_err(|err| map_db_error("insert session", err))?;

    notify_runtime_inventory_changed();
    Ok(())
}

pub(crate) fn create_session_sync(
    db: &Db,
    session_id: &str,
    project_id: &str,
    absolute_path: &str,
) -> Result<(), AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin session.create", err))?;
    tx.execute(
        "INSERT OR IGNORE INTO projects (id, absolute_path) VALUES (?, ?)",
        params![project_id, absolute_path],
    )
    .map_err(|err| map_db_error("insert project", err))?;

    tx.execute(
        "INSERT INTO sessions (id, project_id, master_pid) VALUES (?, ?, ?)",
        params![session_id, project_id, 0_i64],
    )
    .map_err(|err| map_db_error("insert session", err))?;
    tx.commit()
        .map_err(|err| map_db_error("commit session.create", err))?;

    notify_runtime_inventory_changed();
    Ok(())
}

pub(crate) fn session_exists_sync(conn: &Connection, session_id: &str) -> Result<bool, AhError> {
    conn.query_row(
        "SELECT 1 FROM sessions WHERE id = ? LIMIT 1",
        params![session_id],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(|err| map_db_error("check session exists", err))
}

pub(crate) fn query_session_by_id_sync(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<Session>, AhError> {
    conn.query_row(
        "SELECT sessions.id, sessions.project_id, sessions.master_pane_id, sessions.status, \
                sessions.config_hash, sessions.master_state, sessions.created_at, projects.absolute_path \
         FROM sessions \
         JOIN projects ON projects.id = sessions.project_id \
         WHERE sessions.id = ?",
        params![session_id],
        |row| {
            Ok(Session {
                id: row.get(0)?,
                project_id: row.get(1)?,
                master_pane_id: row.get(2)?,
                status: row.get(3)?,
                config_hash: row.get(4)?,
                master_state: row.get(5)?,
                created_at: row.get(6)?,
                absolute_path: row.get(7)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query session by id", err))
}

pub(crate) fn query_active_sessions_sync(conn: &Connection) -> Result<Vec<Session>, AhError> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT sessions.id, sessions.project_id, sessions.master_pane_id, \
                    sessions.status, sessions.config_hash, sessions.master_state, sessions.created_at, projects.absolute_path \
             FROM sessions \
             JOIN agents ON agents.session_id = sessions.id \
             JOIN projects ON projects.id = sessions.project_id \
             WHERE sessions.status = 'ACTIVE' AND agents.state NOT IN ('CRASHED', 'KILLED') \
             ORDER BY sessions.created_at ASC, sessions.id ASC",
        )
        .map_err(|err| map_db_error("prepare active sessions query", err))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(Session {
                id: row.get(0)?,
                project_id: row.get(1)?,
                master_pane_id: row.get(2)?,
                status: row.get(3)?,
                config_hash: row.get(4)?,
                master_state: row.get(5)?,
                created_at: row.get(6)?,
                absolute_path: row.get(7)?,
            })
        })
        .map_err(|err| map_db_error("query active sessions", err))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect active sessions", err))
}

pub(crate) fn query_session_by_cwd_sync(
    conn: &Connection,
    absolute_path: &str,
) -> Result<Option<Session>, AhError> {
    conn.query_row(
        "SELECT sessions.id, sessions.project_id, sessions.master_pane_id, sessions.status, \
                sessions.config_hash, sessions.master_state, sessions.created_at, projects.absolute_path \
         FROM sessions \
         JOIN projects ON projects.id = sessions.project_id \
         WHERE projects.absolute_path = ? \
         ORDER BY sessions.created_at DESC \
         LIMIT 1",
        params![absolute_path],
        |row| {
            Ok(Session {
                id: row.get(0)?,
                project_id: row.get(1)?,
                master_pane_id: row.get(2)?,
                status: row.get(3)?,
                config_hash: row.get(4)?,
                master_state: row.get(5)?,
                created_at: row.get(6)?,
                absolute_path: row.get(7)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query session by cwd", err))
}

pub(crate) fn set_session_master_pane_id_sync(
    conn: &Connection,
    session_id: &str,
    pane_id: &str,
) -> Result<(), AhError> {
    conn.execute(
        "UPDATE sessions SET master_pane_id = ? WHERE id = ?",
        params![pane_id, session_id],
    )
    .map_err(|err| map_db_error("set session master pane id", err))?;
    notify_runtime_tmux_changed();
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MasterNotifyTransition {
    pub previous_state: String,
    pub new_state: String,
    pub transitioned: bool,
    pub request_id: Option<String>,
    pub current_generation: i64,
}

pub(crate) fn apply_master_notify_event_sync(
    conn: &Connection,
    session_id: &str,
    event_generation: i64,
    new_state: &str,
    clear_pending_request: bool,
) -> Result<Option<MasterNotifyTransition>, AhError> {
    let Some((previous_state, request_id, current_generation)) = conn
        .query_row(
            "SELECT master_state, master_pending_tell_request, master_generation \
             FROM sessions WHERE id = ?1 AND status = 'ACTIVE'",
            params![session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query master notify session", err))?
    else {
        return Ok(None);
    };

    if current_generation != event_generation {
        return Ok(Some(MasterNotifyTransition {
            previous_state: previous_state.clone(),
            new_state: previous_state,
            transitioned: false,
            request_id,
            current_generation,
        }));
    }

    if clear_pending_request {
        conn.execute(
            "UPDATE sessions SET master_state = ?2, master_pending_tell_request = NULL WHERE id = ?1",
            params![session_id, new_state],
        )
    } else {
        conn.execute(
            "UPDATE sessions SET master_state = ?2 WHERE id = ?1",
            params![session_id, new_state],
        )
    }
    .map_err(|err| map_db_error("apply master notify event", err))?;

    if previous_state != new_state {
        notify_runtime_inventory_changed();
    }
    Ok(Some(MasterNotifyTransition {
        transitioned: previous_state != new_state,
        previous_state,
        new_state: new_state.to_string(),
        request_id,
        current_generation,
    }))
}

pub(crate) async fn apply_master_notify_event(
    db: Db,
    session_id: String,
    event_generation: i64,
    new_state: String,
    clear_pending_request: bool,
) -> Result<Option<MasterNotifyTransition>, AhError> {
    spawn_db("sessions::apply_master_notify_event", move || {
        let conn = db.conn();
        apply_master_notify_event_sync(
            &conn,
            &session_id,
            event_generation,
            &new_state,
            clear_pending_request,
        )
    })
    .await
}

pub(crate) fn master_tell_begin_sync(
    conn: &Connection,
    session_id: &str,
    request_id: &str,
) -> Result<usize, AhError> {
    conn.execute(
        "UPDATE sessions
         SET master_pending_tell_request = ?2
         WHERE id = ?1 AND status = 'ACTIVE'",
        params![session_id, request_id],
    )
    .map_err(|err| map_db_error("master tell begin", err))
}

pub(crate) fn master_tell_failed_sync(
    conn: &Connection,
    session_id: &str,
    request_id: &str,
) -> Result<usize, AhError> {
    conn.execute(
        "UPDATE sessions
         SET master_pending_tell_request = NULL
         WHERE id = ?1
           AND master_pending_tell_request = ?2",
        params![session_id, request_id],
    )
    .map_err(|err| map_db_error("master tell failed", err))
}

pub async fn master_tell_begin(
    db: Db,
    session_id: String,
    request_id: String,
) -> Result<usize, AhError> {
    spawn_db("sessions::master_tell_begin", move || {
        let conn = db.conn();
        master_tell_begin_sync(&conn, &session_id, &request_id)
    })
    .await
}

pub async fn master_tell_failed(
    db: Db,
    session_id: String,
    request_id: String,
) -> Result<usize, AhError> {
    spawn_db("sessions::master_tell_failed", move || {
        let conn = db.conn();
        master_tell_failed_sync(&conn, &session_id, &request_id)
    })
    .await
}

pub(crate) fn update_session_config_hash_sync(
    conn: &Connection,
    session_id: &str,
    config_hash: &str,
) -> Result<(), AhError> {
    conn.execute(
        "UPDATE sessions SET config_hash = ? WHERE id = ?",
        params![config_hash, session_id],
    )
    .map_err(|err| map_db_error("update session config_hash", err))?;
    Ok(())
}

pub(crate) fn update_session_master_cmd_sync(
    conn: &Connection,
    session_id: &str,
    master_cmd: &str,
) -> Result<(), AhError> {
    conn.execute(
        "UPDATE sessions SET master_cmd = ?2 WHERE id = ?1",
        params![session_id, master_cmd],
    )
    .map_err(|err| map_db_error("update session master_cmd", err))?;
    Ok(())
}

pub(crate) fn update_session_master_provider_sync(
    conn: &Connection,
    session_id: &str,
    master_provider: &str,
) -> Result<(), AhError> {
    conn.execute(
        "UPDATE sessions SET master_provider = ?2 WHERE id = ?1",
        params![session_id, master_provider],
    )
    .map_err(|err| map_db_error("update session master_provider", err))?;
    Ok(())
}

fn notify_runtime_inventory_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::InventoryChanged,
    );
}

fn notify_runtime_tmux_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::TmuxChanged,
    );
}

pub(crate) fn list_session_summaries_sync(
    conn: &Connection,
) -> Result<Vec<SessionSummary>, AhError> {
    let mut stmt = conn
        .prepare(
            "SELECT sessions.id, sessions.project_id, projects.absolute_path, sessions.status, \
                    sessions.master_state, sessions.master_pane_id, sessions.master_provider, \
                    COALESCE(SUM(CASE WHEN agents.state NOT IN ('CRASHED', 'KILLED') THEN 1 ELSE 0 END), 0) AS db_tracked_agents, \
                    sessions.created_at \
             FROM sessions \
             JOIN projects ON projects.id = sessions.project_id \
             LEFT JOIN agents ON agents.session_id = sessions.id \
             GROUP BY sessions.id, sessions.project_id, projects.absolute_path, sessions.status, sessions.master_state, sessions.master_pane_id, sessions.master_provider, sessions.created_at \
             ORDER BY sessions.created_at ASC, sessions.id ASC",
        )
        .map_err(|err| map_db_error("prepare session summaries query", err))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(SessionSummary {
                id: row.get(0)?,
                project_id: row.get(1)?,
                absolute_path: row.get(2)?,
                status: row.get(3)?,
                master_state: row.get(4)?,
                master_pane_id: row.get(5)?,
                master_provider: row.get(6)?,
                db_tracked_agents: row.get(7)?,
                active_agents: row.get(7)?,
                created_at: row.get(8)?,
            })
        })
        .map_err(|err| map_db_error("query session summaries", err))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect session summaries", err))
}

pub async fn insert_session(
    db: Db,
    session_id: String,
    project_id: String,
    absolute_path: String,
) -> Result<(), AhError> {
    spawn_db("sessions::insert_session", move || {
        let conn = db.conn();
        insert_session_sync(&conn, &session_id, &project_id, &absolute_path)
    })
    .await
}

pub async fn create_session(
    db: Db,
    session_id: String,
    project_id: String,
    absolute_path: String,
) -> Result<(), AhError> {
    spawn_db("sessions::create_session", move || {
        create_session_sync(&db, &session_id, &project_id, &absolute_path)
    })
    .await
}

pub async fn session_exists(db: Db, session_id: String) -> Result<bool, AhError> {
    spawn_db("sessions::session_exists", move || {
        let conn = db.conn();
        session_exists_sync(&conn, &session_id)
    })
    .await
}

pub async fn query_session_by_id(db: Db, session_id: String) -> Result<Option<Session>, AhError> {
    spawn_db("sessions::query_session_by_id", move || {
        let conn = db.conn();
        query_session_by_id_sync(&conn, &session_id)
    })
    .await
}

pub async fn update_session_config_hash(
    db: Db,
    session_id: String,
    config_hash: String,
) -> Result<(), AhError> {
    spawn_db("sessions::update_session_config_hash", move || {
        let conn = db.conn();
        update_session_config_hash_sync(&conn, &session_id, &config_hash)
    })
    .await
}

pub async fn update_session_master_cmd(
    db: Db,
    session_id: String,
    master_cmd: String,
) -> Result<(), AhError> {
    spawn_db("sessions::update_session_master_cmd", move || {
        let conn = db.conn();
        update_session_master_cmd_sync(&conn, &session_id, &master_cmd)
    })
    .await
}

pub async fn update_session_master_provider(
    db: Db,
    session_id: String,
    master_provider: String,
) -> Result<(), AhError> {
    spawn_db("sessions::update_session_master_provider", move || {
        let conn = db.conn();
        update_session_master_provider_sync(&conn, &session_id, &master_provider)
    })
    .await
}

pub async fn set_session_master_pane_id(
    db: Db,
    session_id: String,
    pane_id: String,
) -> Result<(), AhError> {
    spawn_db("sessions::set_session_master_pane_id", move || {
        let conn = db.conn();
        set_session_master_pane_id_sync(&conn, &session_id, &pane_id)
    })
    .await
}

pub async fn query_active_sessions(db: Db) -> Result<Vec<Session>, AhError> {
    spawn_db("sessions::query_active_sessions", move || {
        let conn = db.conn();
        query_active_sessions_sync(&conn)
    })
    .await
}

pub async fn query_session_by_cwd(
    db: Db,
    absolute_path: String,
) -> Result<Option<Session>, AhError> {
    spawn_db("sessions::query_session_by_cwd", move || {
        let conn = db.conn();
        query_session_by_cwd_sync(&conn, &absolute_path)
    })
    .await
}

pub async fn list_session_summaries(db: Db) -> Result<Vec<SessionSummary>, AhError> {
    spawn_db("sessions::list_session_summaries", move || {
        let conn = db.conn();
        list_session_summaries_sync(&conn)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        create_session_sync, insert_session_sync, list_session_summaries_sync,
        query_active_sessions_sync, query_session_by_cwd_sync, query_session_by_id_sync,
        session_exists_sync,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::{Db, init};

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

    #[test]
    fn test_insert_session_then_agent() {
        with_test_db(|conn| {
            insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();

            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
                .unwrap();

            assert_eq!(count, 1);
        });
    }

    #[test]
    fn test_session_exists() {
        with_test_db(|conn| {
            insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
            assert!(session_exists_sync(conn, "s1").unwrap());
            assert!(!session_exists_sync(conn, "missing").unwrap());
        });
    }

    #[test]
    fn test_create_session_tx_success_and_existing_project() {
        with_test_db_handle(|db| {
            create_session_sync(db, "s1", "p1", "/tmp/foo").unwrap();
            create_session_sync(db, "s2", "p1", "/tmp/foo").unwrap();
            let count: i64 = db
                .conn()
                .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 2);
        });
    }

    #[test]
    fn test_query_session_helpers() {
        with_test_db(|conn| {
            insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_session_sync(conn, "s2", "p2", "/tmp/bar").unwrap();
            insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
            insert_agent_sync(conn, "a2", "s1", "bash", "KILLED", Some(456)).unwrap();

            let by_id = query_session_by_id_sync(conn, "s1").unwrap().unwrap();
            assert_eq!(by_id.id, "s1");
            assert_eq!(by_id.absolute_path, "/tmp/foo");
            assert!(query_session_by_id_sync(conn, "missing").unwrap().is_none());
            let by_cwd = query_session_by_cwd_sync(conn, "/tmp/bar")
                .unwrap()
                .unwrap();
            assert_eq!(by_cwd.id, "s2");
            assert_eq!(by_cwd.absolute_path, "/tmp/bar");
            let active = query_active_sessions_sync(conn).unwrap();
            assert_eq!(active.len(), 1);
            assert_eq!(active[0].absolute_path, "/tmp/foo");

            let summaries = list_session_summaries_sync(conn).unwrap();
            let s1 = summaries.iter().find(|summary| summary.id == "s1").unwrap();
            let s2 = summaries.iter().find(|summary| summary.id == "s2").unwrap();
            assert_eq!(s1.db_tracked_agents, 1);
            assert_eq!(s2.db_tracked_agents, 0);
            assert_eq!(s1.active_agents, 1);
            assert_eq!(s2.active_agents, 0);
        });
    }
}
