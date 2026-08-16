use crate::error::CcbdError;
use rusqlite::Connection;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

pub mod agents;
pub mod agents_lifecycle;
pub mod common;
pub mod events;
pub mod events_progress;
pub mod evidence;
pub mod job_state;
pub mod jobs;
pub mod learned_rules;
pub mod maintenance;
pub mod master_cutovers;
pub mod master_recovery;
pub(crate) mod perception;
pub mod prompt_experience;
pub mod recovery;
pub mod schema;
pub mod sessions;
pub mod state_machine;
pub mod state_machine_assert;
pub mod system;

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
    path: Arc<PathBuf>,
}

impl Db {
    pub fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .expect("database connection mutex poisoned")
    }

    pub(crate) fn try_conn(&self) -> Result<Option<MutexGuard<'_, Connection>>, CcbdError> {
        match self.conn.try_lock() {
            Ok(conn) => Ok(Some(conn)),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => Err(CcbdError::DatabaseRuntimePanic {
                details: "database connection mutex poisoned".to_string(),
            }),
        }
    }

    pub(crate) fn fresh_conn(&self) -> Result<Connection, CcbdError> {
        open_configured_connection(&self.path)
    }
}

pub fn init(db_path: &Path) -> Result<Db, CcbdError> {
    let conn = open_configured_connection(db_path)?;
    conn.execute_batch(schema::SCHEMA_DDL)
        .map_err(|err| CcbdError::DbConstraintViolation(format!("initialize schema: {err}")))?;
    migrate_sub_state(&conn)?;
    migrate_jobs_cancel_requested(&conn)?;
    migrate_sessions_status(&conn)?;
    migrate_sessions_master_pane_id(&conn)?;
    migrate_sessions_config_hash(&conn)?;
    migrate_sessions_master_cmd(&conn)?;
    migrate_sessions_master_provider(&conn)?;
    migrate_sessions_master_revive_columns(&conn)?;
    migrate_sessions_failed_idle_to_closed(&conn)?;
    migrate_sessions_master_state(&conn)?;
    migrate_sessions_master_pending_tell_request(&conn)?;
    migrate_evidence_record_columns(&conn)?;
    migrate_agents_lifecycle_id(&conn)?;
    migrate_agents_config_hash(&conn)?;
    migrate_agent_spawn_specs(&conn)?;
    migrate_jobs_evidence_requirements(&conn)?;
    migrate_jobs_governance_binding(&conn)?;
    migrate_master_cutovers(&conn)?;
    migrate_master_recovery_windows(&conn)?;

    Ok(Db {
        conn: Arc::new(Mutex::new(conn)),
        path: Arc::new(db_path.to_path_buf()),
    })
}

fn migrate_jobs_governance_binding(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "jobs",
        "governance_binding_json",
        "ALTER TABLE jobs ADD COLUMN governance_binding_json TEXT",
    )
}

fn migrate_agents_lifecycle_id(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "agents",
        "lifecycle_id",
        "ALTER TABLE agents ADD COLUMN lifecycle_id TEXT NOT NULL DEFAULT ''",
    )?;
    conn.execute(
        "UPDATE agents
         SET lifecycle_id = 'lifecycle_' || lower(hex(randomblob(16)))
         WHERE trim(lifecycle_id) = ''",
        [],
    )
    .map_err(|err| {
        CcbdError::DbConstraintViolation(format!("backfill agents.lifecycle_id: {err}"))
    })?;
    Ok(())
}

fn migrate_master_recovery_windows(conn: &Connection) -> Result<(), CcbdError> {
    conn.execute_batch(master_recovery::MASTER_RECOVERY_WINDOWS_DDL)
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("migrate master_recovery_windows: {err}"))
        })?;
    add_column_if_missing(
        conn,
        "master_recovery_windows",
        "readiness_mode",
        "ALTER TABLE master_recovery_windows ADD COLUMN readiness_mode TEXT",
    )?;
    add_column_if_missing(
        conn,
        "master_recovery_windows",
        "ready_at",
        "ALTER TABLE master_recovery_windows ADD COLUMN ready_at INTEGER",
    )?;
    add_column_if_missing(
        conn,
        "master_recovery_windows",
        "ready_reason",
        "ALTER TABLE master_recovery_windows ADD COLUMN ready_reason TEXT",
    )?;
    add_column_if_missing(
        conn,
        "master_recovery_windows",
        "readiness_token",
        "ALTER TABLE master_recovery_windows ADD COLUMN readiness_token TEXT",
    )?;
    migrate_master_recovery_windows_phase_check(conn)
}

fn migrate_master_recovery_windows_phase_check(conn: &Connection) -> Result<(), CcbdError> {
    let create_sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='master_recovery_windows'",
            [],
            |row| row.get(0),
        )
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("query master_recovery_windows schema: {err}"))
        })?;
    if create_sql.contains("'MASTER_VERIFYING'") {
        return Ok(());
    }

    conn.execute_batch(
        r#"
        ALTER TABLE master_recovery_windows RENAME TO master_recovery_windows_old;
        CREATE TABLE master_recovery_windows (
          session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
          expected_pid INTEGER NOT NULL,
          expected_generation INTEGER NOT NULL,
          claimed_generation INTEGER,
          phase TEXT NOT NULL CHECK(phase IN ('DETECTED','WORKERS_REAPED','MASTER_SPAWNING','MASTER_RUNNING','MASTER_VERIFYING','WORKERS_REPROVISIONING','COMPLETED','FAILED','FUSED')),
          active_work INTEGER NOT NULL,
          defer_until INTEGER NOT NULL,
          created_at INTEGER NOT NULL DEFAULT (unixepoch()),
          updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
          completed_at INTEGER,
          readiness_mode TEXT,
          ready_at INTEGER,
          ready_reason TEXT,
          readiness_token TEXT
        ) STRICT;
        INSERT INTO master_recovery_windows (
          session_id, expected_pid, expected_generation, claimed_generation,
          phase, active_work, defer_until, created_at, updated_at, completed_at,
          readiness_mode, ready_at, ready_reason, readiness_token
        )
        SELECT
          session_id, expected_pid, expected_generation, claimed_generation,
          phase, active_work, defer_until, created_at, updated_at, completed_at,
          readiness_mode, ready_at, ready_reason, readiness_token
        FROM master_recovery_windows_old;
        DROP TABLE master_recovery_windows_old;
        "#,
    )
    .map_err(|err| {
        CcbdError::DbConstraintViolation(format!(
            "migrate master_recovery_windows phase check: {err}"
        ))
    })
}

fn open_configured_connection(db_path: &Path) -> Result<Connection, CcbdError> {
    let conn = Connection::open(db_path).map_err(|err| {
        CcbdError::DbConstraintViolation(format!("open {}: {err}", db_path.display()))
    })?;

    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;
        -- Freed pages must come back without rewriting the file. This only takes
        -- effect on a database that has no tables yet; an existing one keeps its
        -- mode and is compacted by the maintenance pass instead (decision 0004).
        PRAGMA auto_vacuum = INCREMENTAL;
        "#,
    )
    .map_err(|err| CcbdError::DbConstraintViolation(format!("initialize pragmas: {err}")))?;
    Ok(conn)
}

fn migrate_master_cutovers(conn: &Connection) -> Result<(), CcbdError> {
    conn.execute_batch(master_cutovers::MASTER_CUTOVERS_DDL)
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("migrate master_cutovers: {err}"))
        })?;
    add_column_if_missing(
        conn,
        "master_cutovers",
        "ack_ready_at",
        "ALTER TABLE master_cutovers ADD COLUMN ack_ready_at INTEGER",
    )?;
    add_column_if_missing(
        conn,
        "master_cutovers",
        "readiness_mode",
        "ALTER TABLE master_cutovers ADD COLUMN readiness_mode TEXT",
    )
}

fn migrate_sessions_master_pane_id(conn: &Connection) -> Result<(), CcbdError> {
    match conn.execute("ALTER TABLE sessions ADD COLUMN master_pane_id TEXT", []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(message)))
            if message.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(err) => Err(CcbdError::DbConstraintViolation(format!(
            "migrate sessions.master_pane_id: {err}"
        ))),
    }
}

fn migrate_sessions_config_hash(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "sessions",
        "config_hash",
        "ALTER TABLE sessions ADD COLUMN config_hash TEXT",
    )
}

fn migrate_sessions_master_cmd(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "sessions",
        "master_cmd",
        "ALTER TABLE sessions ADD COLUMN master_cmd TEXT",
    )
}

fn migrate_sessions_master_provider(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "sessions",
        "master_provider",
        "ALTER TABLE sessions ADD COLUMN master_provider TEXT",
    )
}

fn migrate_sessions_master_revive_columns(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "sessions",
        "master_retry_count",
        "ALTER TABLE sessions ADD COLUMN master_retry_count INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        conn,
        "sessions",
        "master_next_retry_at",
        "ALTER TABLE sessions ADD COLUMN master_next_retry_at INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        conn,
        "sessions",
        "master_generation",
        "ALTER TABLE sessions ADD COLUMN master_generation INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        conn,
        "sessions",
        "master_last_exit_reason",
        "ALTER TABLE sessions ADD COLUMN master_last_exit_reason TEXT",
    )
}

fn migrate_sessions_failed_idle_to_closed(conn: &Connection) -> Result<(), CcbdError> {
    conn.execute(
        "UPDATE sessions
         SET status = 'CLOSED'
         WHERE status = 'FAILED' AND master_last_exit_reason = 'IDLE_MASTER_EXIT'",
        [],
    )
    .map(|_| ())
    .map_err(|err| {
        CcbdError::DbConstraintViolation(format!(
            "migrate sessions failed idle exits to closed: {err}"
        ))
    })
}

fn migrate_sessions_master_state(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "sessions",
        "master_state",
        "ALTER TABLE sessions ADD COLUMN master_state TEXT NOT NULL DEFAULT 'IDLE'",
    )
}

fn migrate_sessions_master_pending_tell_request(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "sessions",
        "master_pending_tell_request",
        "ALTER TABLE sessions ADD COLUMN master_pending_tell_request TEXT",
    )
}

fn migrate_agents_config_hash(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "agents",
        "config_hash",
        "ALTER TABLE agents ADD COLUMN config_hash TEXT",
    )
}

fn migrate_agent_spawn_specs(conn: &Connection) -> Result<(), CcbdError> {
    conn.execute_batch(recovery::AGENT_SPAWN_SPECS_DDL)
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("migrate agent_spawn_specs: {err}"))
        })?;
    conn.execute_batch(recovery::AGENT_RECOVERY_INTENTS_DDL)
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("migrate agent_recovery_intents: {err}"))
        })?;
    for (column, statement) in [
        (
            "interrupted_job_request_id",
            "ALTER TABLE agent_recovery_intents ADD COLUMN interrupted_job_request_id TEXT",
        ),
        (
            "interrupted_job_prompt_text",
            "ALTER TABLE agent_recovery_intents ADD COLUMN interrupted_job_prompt_text TEXT",
        ),
        (
            "interrupted_job_cancel_requested",
            "ALTER TABLE agent_recovery_intents ADD COLUMN interrupted_job_cancel_requested INTEGER",
        ),
        (
            "interrupted_job_requires_physical_evidence",
            "ALTER TABLE agent_recovery_intents ADD COLUMN interrupted_job_requires_physical_evidence INTEGER",
        ),
        (
            "interrupted_job_requires_test_evidence",
            "ALTER TABLE agent_recovery_intents ADD COLUMN interrupted_job_requires_test_evidence INTEGER",
        ),
        (
            "interrupted_job_governance_binding_json",
            "ALTER TABLE agent_recovery_intents ADD COLUMN interrupted_job_governance_binding_json TEXT",
        ),
    ] {
        add_column_if_missing(conn, "agent_recovery_intents", column, statement)?;
    }
    migrate_agent_recovery_intents_action_check(conn)?;
    for (column, statement) in [
        ("retry_count", recovery::AGENTS_BACKOFF_COLUMNS_DDL[0]),
        ("next_retry_at", recovery::AGENTS_BACKOFF_COLUMNS_DDL[1]),
        ("retry_exhausted", recovery::AGENTS_BACKOFF_COLUMNS_DDL[2]),
    ] {
        add_column_if_missing(conn, "agents", column, statement)?;
    }
    Ok(())
}

fn migrate_agent_recovery_intents_action_check(conn: &Connection) -> Result<(), CcbdError> {
    let create_sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='agent_recovery_intents'",
            [],
            |row| row.get(0),
        )
        .map_err(|err| {
            CcbdError::DbConstraintViolation(format!("query agent_recovery_intents schema: {err}"))
        })?;
    if create_sql.contains("'REVIVE_IDLE'") {
        return Ok(());
    }

    conn.execute_batch(
        r#"
        ALTER TABLE agent_recovery_intents RENAME TO agent_recovery_intents_old;
        CREATE TABLE agent_recovery_intents (
          agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
          session_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          previous_state TEXT NOT NULL,
          crashed_state_version INTEGER NOT NULL,
          interrupted_job_id TEXT,
          interrupted_job_status TEXT,
          interrupted_job_request_id TEXT,
          interrupted_job_prompt_text TEXT,
          interrupted_job_cancel_requested INTEGER,
          interrupted_job_requires_physical_evidence INTEGER,
          interrupted_job_requires_test_evidence INTEGER,
          interrupted_job_governance_binding_json TEXT,
          action TEXT NOT NULL CHECK(action IN ('REVIVE', 'REVIVE_IDLE', 'REAP_ONLY')),
          reason TEXT NOT NULL,
          consumed_at INTEGER,
          created_at INTEGER NOT NULL DEFAULT (unixepoch()),
          updated_at INTEGER NOT NULL DEFAULT (unixepoch())
        ) STRICT;
        INSERT INTO agent_recovery_intents (
          agent_id, session_id, provider, previous_state, crashed_state_version,
          interrupted_job_id, interrupted_job_status, interrupted_job_request_id,
          interrupted_job_prompt_text, interrupted_job_cancel_requested,
          interrupted_job_requires_physical_evidence, interrupted_job_requires_test_evidence,
          interrupted_job_governance_binding_json,
          action, reason, consumed_at, created_at, updated_at
        )
        SELECT
          agent_id, session_id, provider, previous_state, crashed_state_version,
          interrupted_job_id, interrupted_job_status, interrupted_job_request_id,
          interrupted_job_prompt_text, interrupted_job_cancel_requested,
          interrupted_job_requires_physical_evidence, interrupted_job_requires_test_evidence,
          interrupted_job_governance_binding_json,
          action, reason, consumed_at, created_at, updated_at
        FROM agent_recovery_intents_old;
        DROP TABLE agent_recovery_intents_old;
        CREATE INDEX IF NOT EXISTS idx_agent_recovery_intents_action
        ON agent_recovery_intents(action, consumed_at, created_at);
        "#,
    )
    .map_err(|err| {
        CcbdError::DbConstraintViolation(format!(
            "migrate agent_recovery_intents action check: {err}"
        ))
    })
}

fn migrate_evidence_record_columns(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "evidence",
        "job_id",
        "ALTER TABLE evidence ADD COLUMN job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE",
    )?;
    add_column_if_missing(
        conn,
        "evidence",
        "evidence_type",
        "ALTER TABLE evidence ADD COLUMN evidence_type TEXT",
    )?;
    add_column_if_missing(
        conn,
        "evidence",
        "subject_path",
        "ALTER TABLE evidence ADD COLUMN subject_path TEXT",
    )?;
    add_column_if_missing(
        conn,
        "evidence",
        "payload",
        "ALTER TABLE evidence ADD COLUMN payload TEXT",
    )?;
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_evidence_agent_job_type ON evidence(agent_id, job_id, evidence_type);
        CREATE INDEX IF NOT EXISTS idx_evidence_agent_type_path ON evidence(agent_id, evidence_type, subject_path);
        "#,
    )
    .map_err(|err| CcbdError::DbConstraintViolation(format!("migrate evidence indexes: {err}")))
}

fn migrate_jobs_evidence_requirements(conn: &Connection) -> Result<(), CcbdError> {
    add_column_if_missing(
        conn,
        "jobs",
        "requires_physical_evidence",
        "ALTER TABLE jobs ADD COLUMN requires_physical_evidence INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        conn,
        "jobs",
        "requires_test_evidence",
        "ALTER TABLE jobs ADD COLUMN requires_test_evidence INTEGER NOT NULL DEFAULT 0",
    )
}

fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    statement: &str,
) -> Result<(), CcbdError> {
    match conn.execute(statement, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(message)))
            if message.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(err) => Err(CcbdError::DbConstraintViolation(format!(
            "migrate {table}.{column}: {err}"
        ))),
    }
}

fn migrate_sessions_status(conn: &Connection) -> Result<(), CcbdError> {
    match conn.execute(
        "ALTER TABLE sessions ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE'",
        [],
    ) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(message)))
            if message.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(err) => Err(CcbdError::DbConstraintViolation(format!(
            "migrate sessions.status: {err}"
        ))),
    }
}

fn migrate_jobs_cancel_requested(conn: &Connection) -> Result<(), CcbdError> {
    match conn.execute(
        "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0",
        [],
    ) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(message)))
            if message.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(err) => Err(CcbdError::DbConstraintViolation(format!(
            "migrate jobs.cancel_requested: {err}"
        ))),
    }
}

fn migrate_sub_state(conn: &Connection) -> Result<(), CcbdError> {
    match conn.execute("ALTER TABLE agents ADD COLUMN sub_state TEXT", []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(message)))
            if message.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(err) => Err(CcbdError::DbConstraintViolation(format!(
            "migrate agents.sub_state: {err}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{init, schema};
    use rusqlite::Connection;

    #[test]
    fn test_init_creates_wal_db() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();

        let journal_mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();

        assert_eq!(journal_mode, "wal");
    }

    #[test]
    fn test_init_busy_timeout_5000() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();

        let busy_timeout: i64 = conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();

        assert_eq!(busy_timeout, 5000);
    }

    #[test]
    fn test_init_foreign_keys_on() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();

        let foreign_keys: i64 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();

        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn test_init_schema_has_sub_state() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let has_sub_state: bool = conn
            .prepare("PRAGMA table_info(agents)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .any(|name| name == "sub_state");

        assert!(has_sub_state);
    }

    #[test]
    fn test_init_schema_has_session_status() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let has_status: bool = conn
            .prepare("PRAGMA table_info(sessions)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .any(|name| name == "status");

        assert!(has_status);
    }

    #[test]
    fn test_init_schema_has_job_transitions() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();

        let table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='job_transitions'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let index_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name IN (
                    'idx_job_transitions_event_id',
                    'idx_job_transitions_job_event_id',
                    'idx_job_transitions_agent_event_id'
                )",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(table_count, 1);
        assert_eq!(index_count, 3);
    }

    #[test]
    fn test_init_migrates_pre_work_execution_schema_idempotently() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut old_schema = schema::SCHEMA_DDL.to_string();
        for addition in [
            ",\n    master_provider TEXT",
            ",\n    lifecycle_id TEXT NOT NULL DEFAULT ''",
            ",\n    governance_binding_json TEXT",
        ] {
            let previous = old_schema.clone();
            old_schema = old_schema.replace(addition, "");
            assert_ne!(
                old_schema, previous,
                "old schema fixture did not remove {addition}"
            );
        }
        let observation_ddl = r#"CREATE TABLE IF NOT EXISTS provider_status_observations (
    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id TEXT NOT NULL UNIQUE,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    lifecycle_id TEXT NOT NULL,
    turn_id TEXT,
    observation_json TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_provider_status_observations_scope
ON provider_status_observations(agent_id, lifecycle_id, turn_id, observed_at_ms);

"#;
        let previous = old_schema.clone();
        old_schema = old_schema.replace(observation_ddl, "");
        assert_ne!(
            old_schema, previous,
            "old schema fixture retained observation DDL"
        );

        let conn = Connection::open(file.path()).unwrap();
        conn.execute_batch(&old_schema).unwrap();
        conn.execute(
            "INSERT INTO projects (id, absolute_path) VALUES ('p1', '/tmp/p1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1', 'p1', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agents (id, session_id, provider, state) VALUES ('a1', 's1', 'codex', 'IDLE')",
            [],
        )
        .unwrap();
        drop(conn);

        drop(init(file.path()).unwrap());
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let job_columns = conn
            .prepare("PRAGMA table_info(jobs)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            job_columns
                .iter()
                .filter(|name| name.as_str() == "governance_binding_json")
                .count(),
            1
        );
        let agent_lifecycle_id: String = conn
            .query_row(
                "SELECT lifecycle_id FROM agents WHERE id = 'a1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(agent_lifecycle_id.starts_with("lifecycle_"));
        assert_eq!(agent_lifecycle_id.len(), "lifecycle_".len() + 32);

        let master_provider_columns: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name = 'master_provider'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(master_provider_columns, 1);

        let observation_tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'provider_status_observations'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(observation_tables, 1);
    }

    #[test]
    fn migrate_sessions_failed_idle_to_closed_is_idempotent() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                absolute_path TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            ) STRICT;
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                master_pid INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                master_last_exit_reason TEXT,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            ) STRICT;
            INSERT INTO projects (id, absolute_path) VALUES ('p1', '/tmp/p1');
            INSERT INTO sessions (id, project_id, master_pid, status, master_last_exit_reason)
            VALUES
              ('s_idle', 'p1', 0, 'FAILED', 'IDLE_MASTER_EXIT'),
              ('s_crash', 'p1', 0, 'FAILED', 'OOM_OR_CRASH'),
              ('s_active', 'p1', 0, 'ACTIVE', NULL);
            "#,
        )
        .unwrap();
        drop(conn);

        let db = init(file.path()).unwrap();
        drop(db);
        let db = init(file.path()).unwrap();
        let conn = db.conn();

        let status = |session_id: &str| -> String {
            conn.query_row(
                "SELECT status FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(status("s_idle"), "CLOSED");
        assert_eq!(status("s_crash"), "FAILED");
        assert_eq!(status("s_active"), "ACTIVE");
    }

    #[test]
    fn test_init_schema_has_session_master_pane_id() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let has_master_pane_id: bool = conn
            .prepare("PRAGMA table_info(sessions)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .any(|name| name == "master_pane_id");

        assert!(has_master_pane_id);
    }

    #[test]
    fn test_init_schema_has_master_observability_columns() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let columns = conn
            .prepare("PRAGMA table_info(sessions)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(columns.iter().any(|name| name == "master_state"));
        assert!(
            columns
                .iter()
                .any(|name| name == "master_pending_tell_request")
        );
    }

    #[test]
    fn test_init_migrates_old_sessions_master_observability_columns() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = rusqlite::Connection::open(file.path()).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                INSERT INTO projects (id, absolute_path) VALUES ('p1', '/tmp/master-observability');
                INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1', 'p1', 1);
                "#,
            )
            .unwrap();
        }

        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let row: (String, Option<String>) = conn
            .query_row(
                "SELECT master_state, master_pending_tell_request FROM sessions WHERE id = 's1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(row, ("IDLE".to_string(), None));
    }

    #[test]
    fn test_init_schema_has_evidence_table_and_indexes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let objects = conn
            .prepare("SELECT name FROM sqlite_master WHERE type IN ('table', 'index')")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(objects.iter().any(|name| name == "evidence"));
        assert!(objects.iter().any(|name| name == "idx_evidence_pending"));
        assert!(objects.iter().any(|name| name == "idx_evidence_agent"));
        assert!(
            objects
                .iter()
                .any(|name| name == "idx_evidence_agent_job_type")
        );
        assert!(
            objects
                .iter()
                .any(|name| name == "idx_evidence_agent_type_path")
        );
    }

    #[test]
    fn test_init_migrates_old_evidence_schema() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = rusqlite::Connection::open(file.path()).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    master_pane_id TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    sub_state TEXT,
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE events (
                    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                    request_id TEXT,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE evidence (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                    event_seq_id INTEGER NOT NULL REFERENCES events(seq_id),
                    pane_bytes BLOB NOT NULL,
                    failed_rules TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    l3_asserted_state TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                    request_id TEXT,
                    prompt_text TEXT NOT NULL,
                    reply_text TEXT,
                    status TEXT NOT NULL DEFAULT 'QUEUED',
                    error_reason TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    dispatched_at INTEGER,
                    dispatched_at_seq_id INTEGER,
                    completed_at INTEGER,
                    cancel_requested INTEGER NOT NULL DEFAULT 0
                ) STRICT;
                INSERT INTO projects (id, absolute_path) VALUES ('p1', '/tmp/pr1a-old');
                INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1', 'p1', 1);
                INSERT INTO agents (id, session_id, provider, state) VALUES ('a1', 's1', 'bash', 'UNKNOWN');
                INSERT INTO events (agent_id, event_type, payload) VALUES ('a1', 'state_change', '{}');
                INSERT INTO evidence (id, agent_id, event_seq_id, pane_bytes, failed_rules)
                    VALUES ('evi_old', 'a1', 1, X'01', '[]');
                "#,
            )
            .unwrap();
        }

        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let columns = conn
            .prepare("PRAGMA table_info(evidence)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for name in ["job_id", "evidence_type", "subject_path", "payload"] {
            assert!(columns.iter().any(|column| column == name));
        }
        let migrated: (String, Option<String>, Option<String>, Option<String>, Option<String>) =
            conn.query_row(
                "SELECT status, job_id, evidence_type, subject_path, payload FROM evidence WHERE id = 'evi_old'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(migrated, ("PENDING".to_string(), None, None, None, None));

        let job_columns = conn
            .prepare("PRAGMA table_info(jobs)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            job_columns
                .iter()
                .any(|name| name == "requires_physical_evidence")
        );
        assert!(
            job_columns
                .iter()
                .any(|name| name == "requires_test_evidence")
        );
    }

    #[test]
    fn test_init_migrates_old_agents_schema() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = rusqlite::Connection::open(file.path()).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                "#,
            )
            .unwrap();
        }

        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let has_sub_state: bool = conn
            .prepare("PRAGMA table_info(agents)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .any(|name| name == "sub_state");

        assert!(has_sub_state);

        let evidence_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='evidence')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(evidence_exists);
    }
}
