use crate::db::Db;
use crate::db::common::map_db_error;
use crate::error::AhError;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

pub const MASTER_CUTOVERS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS master_cutovers (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK(state IN (
        'PREPARING',
        'SPAWNING',
        'VERIFYING',
        'ACTIVE',
        'ROLLED_BACK',
        'FAILED',
        'RELEASED'
    )),
    old_master_pid INTEGER,
    new_master_pid INTEGER,
    new_master_generation INTEGER,
    new_master_pane_id TEXT,
    ah_state_dir TEXT NOT NULL,
    ah_socket_path TEXT NOT NULL,
    handoff_path TEXT NOT NULL,
    ack_ready_at INTEGER,
    readiness_mode TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    completed_at INTEGER
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_master_cutovers_active
ON master_cutovers(session_id)
WHERE state IN ('PREPARING', 'SPAWNING', 'VERIFYING', 'ACTIVE');
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterCutoverClaim {
    Claimed,
    AlreadyActive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterCutoverUpdate {
    Updated,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterCutover {
    pub id: String,
    pub session_id: String,
    pub state: String,
    pub old_master_pid: Option<i64>,
    pub new_master_pid: Option<i64>,
    pub new_master_generation: Option<i64>,
    pub new_master_pane_id: Option<String>,
    pub ah_state_dir: String,
    pub ah_socket_path: String,
    pub handoff_path: String,
    pub ack_ready_at: Option<i64>,
    pub readiness_mode: Option<String>,
}

pub fn claim_master_cutover(
    db: &Db,
    id: &str,
    session_id: &str,
    old_master_pid: Option<i64>,
    ah_state_dir: &str,
    ah_socket_path: &str,
    handoff_path: &str,
) -> Result<MasterCutoverClaim, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin master cutover claim", err))?;
    let active_exists: bool = tx
        .query_row(
            "SELECT 1 FROM master_cutovers
             WHERE session_id = ?1
               AND state IN ('PREPARING', 'SPAWNING', 'VERIFYING', 'ACTIVE')
             LIMIT 1",
            params![session_id],
            |_| Ok(true),
        )
        .optional()
        .map(|value| value.unwrap_or(false))
        .map_err(|err| map_db_error("query active master cutover", err))?;
    if active_exists {
        tx.commit()
            .map_err(|err| map_db_error("commit master cutover already active", err))?;
        return Ok(MasterCutoverClaim::AlreadyActive);
    }
    tx.execute(
        "INSERT INTO master_cutovers (
            id,
            session_id,
            state,
            old_master_pid,
            ah_state_dir,
            ah_socket_path,
            handoff_path
         ) VALUES (?1, ?2, 'PREPARING', ?3, ?4, ?5, ?6)",
        params![
            id,
            session_id,
            old_master_pid,
            ah_state_dir,
            ah_socket_path,
            handoff_path
        ],
    )
    .map_err(|err| map_db_error("insert master cutover", err))?;
    tx.commit()
        .map_err(|err| map_db_error("commit master cutover claim", err))?;
    Ok(MasterCutoverClaim::Claimed)
}

pub fn update_master_cutover_state(
    db: &Db,
    id: &str,
    expected_state: &str,
    next_state: &str,
) -> Result<MasterCutoverUpdate, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE master_cutovers
             SET state = ?3,
                 updated_at = unixepoch(),
                 completed_at = CASE
                     WHEN ?3 IN ('ROLLED_BACK', 'FAILED', 'RELEASED') THEN unixepoch()
                     ELSE completed_at
                 END
             WHERE id = ?1
               AND state = ?2",
            params![id, expected_state, next_state],
        )
        .map_err(|err| map_db_error("update master cutover state", err))?;
    if changes == 1 {
        Ok(MasterCutoverUpdate::Updated)
    } else {
        Ok(MasterCutoverUpdate::Stale)
    }
}

pub fn update_master_cutover_spawn_metadata(
    db: &Db,
    id: &str,
    expected_state: &str,
    next_state: &str,
    new_master_pid: i64,
    new_master_generation: i64,
    new_master_pane_id: &str,
) -> Result<MasterCutoverUpdate, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE master_cutovers
             SET state = ?3,
                 new_master_pid = ?4,
                 new_master_generation = ?5,
                 new_master_pane_id = ?6,
                 updated_at = unixepoch(),
                 completed_at = CASE
                     WHEN ?3 IN ('ROLLED_BACK', 'FAILED', 'RELEASED') THEN unixepoch()
                     ELSE completed_at
                 END
             WHERE id = ?1
               AND state = ?2",
            params![
                id,
                expected_state,
                next_state,
                new_master_pid,
                new_master_generation,
                new_master_pane_id
            ],
        )
        .map_err(|err| map_db_error("update master cutover spawn metadata", err))?;
    if changes == 1 {
        Ok(MasterCutoverUpdate::Updated)
    } else {
        Ok(MasterCutoverUpdate::Stale)
    }
}

pub fn mark_master_cutover_ack_ready(
    db: &Db,
    id: &str,
    readiness_mode: &str,
) -> Result<MasterCutoverUpdate, AhError> {
    let conn = db.conn();
    let changes = conn
        .execute(
            "UPDATE master_cutovers
             SET ack_ready_at = unixepoch(),
                 readiness_mode = ?2,
                 updated_at = unixepoch()
             WHERE id = ?1
               AND state = 'VERIFYING'",
            params![id, readiness_mode],
        )
        .map_err(|err| map_db_error("mark master cutover ack ready", err))?;
    if changes == 1 {
        Ok(MasterCutoverUpdate::Updated)
    } else {
        Ok(MasterCutoverUpdate::Stale)
    }
}

pub fn release_master_cutover(
    db: &Db,
    id: &str,
    expected_state: &str,
) -> Result<MasterCutoverUpdate, AhError> {
    update_master_cutover_state(db, id, expected_state, "RELEASED")
}

pub fn get_active_master_cutover(
    db: &Db,
    session_id: &str,
) -> Result<Option<MasterCutover>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT id,
                session_id,
                state,
                old_master_pid,
                new_master_pid,
                new_master_generation,
                new_master_pane_id,
                ah_state_dir,
                ah_socket_path,
                handoff_path,
                ack_ready_at,
                readiness_mode
         FROM master_cutovers
         WHERE session_id = ?1
           AND state IN ('PREPARING', 'SPAWNING', 'VERIFYING', 'ACTIVE')
         ORDER BY created_at DESC, id DESC
         LIMIT 1",
        params![session_id],
        |row| {
            Ok(MasterCutover {
                id: row.get(0)?,
                session_id: row.get(1)?,
                state: row.get(2)?,
                old_master_pid: row.get(3)?,
                new_master_pid: row.get(4)?,
                new_master_generation: row.get(5)?,
                new_master_pane_id: row.get(6)?,
                ah_state_dir: row.get(7)?,
                ah_socket_path: row.get(8)?,
                handoff_path: row.get(9)?,
                ack_ready_at: row.get(10)?,
                readiness_mode: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query active master cutover", err))
}

pub fn get_master_cutover(db: &Db, id: &str) -> Result<Option<MasterCutover>, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT id,
                session_id,
                state,
                old_master_pid,
                new_master_pid,
                new_master_generation,
                new_master_pane_id,
                ah_state_dir,
                ah_socket_path,
                handoff_path,
                ack_ready_at,
                readiness_mode
         FROM master_cutovers
         WHERE id = ?1",
        params![id],
        |row| {
            Ok(MasterCutover {
                id: row.get(0)?,
                session_id: row.get(1)?,
                state: row.get(2)?,
                old_master_pid: row.get(3)?,
                new_master_pid: row.get(4)?,
                new_master_generation: row.get(5)?,
                new_master_pane_id: row.get(6)?,
                ah_state_dir: row.get(7)?,
                ah_socket_path: row.get(8)?,
                handoff_path: row.get(9)?,
                ack_ready_at: row.get(10)?,
                readiness_mode: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(|err| map_db_error("query master cutover", err))
}

pub fn master_cutover_has_inflight_state(db: &Db, session_id: &str) -> Result<bool, AhError> {
    let conn = db.conn();
    conn.query_row(
        "SELECT 1 FROM master_cutovers
         WHERE session_id = ?1
           AND state IN ('PREPARING', 'SPAWNING', 'VERIFYING')
         LIMIT 1",
        params![session_id],
        |_| Ok(true),
    )
    .optional()
    .map(|value| value.unwrap_or(false))
    .map_err(|err| map_db_error("query in-flight master cutover", err))
}

#[cfg(test)]
mod tests {
    use super::{
        MasterCutoverClaim, MasterCutoverUpdate, claim_master_cutover, get_active_master_cutover,
        get_master_cutover, mark_master_cutover_ack_ready, release_master_cutover,
        update_master_cutover_spawn_metadata, update_master_cutover_state,
    };
    use crate::db::init;

    fn insert_session(db: &crate::db::Db, session_id: &str) {
        let conn = db.conn();
        conn.execute(
            "INSERT INTO projects (id, absolute_path) VALUES (?1, ?2)",
            [
                format!("project_{session_id}"),
                format!("/tmp/{session_id}"),
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, project_id, master_pid) VALUES (?1, ?2, 123)",
            [session_id.to_string(), format!("project_{session_id}")],
        )
        .unwrap();
    }

    #[test]
    fn schema_has_master_cutovers_table_and_active_unique_index() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let conn = db.conn();
        let table_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'master_cutovers')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let index_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_master_cutovers_active'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(table_exists);
        assert!(index_sql.contains("UNIQUE INDEX"));
        assert!(index_sql.contains("session_id"));
        assert!(index_sql.contains("'PREPARING'"));
        assert!(index_sql.contains("'SPAWNING'"));
        assert!(index_sql.contains("'VERIFYING'"));
        assert!(index_sql.contains("'ACTIVE'"));
        let ack_ready_at_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('master_cutovers') WHERE name = 'ack_ready_at')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let readiness_mode_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('master_cutovers') WHERE name = 'readiness_mode')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ack_ready_at_exists);
        assert!(readiness_mode_exists);
    }

    #[test]
    fn master_cutover_claim_is_single_active_per_session() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        insert_session(&db, "s_cutover");

        let first = claim_master_cutover(
            &db,
            "cutover_1",
            "s_cutover",
            Some(111),
            "/tmp/ah-state",
            "/tmp/ah-state/ahd.sock",
            "/tmp/ah-state/cutovers/cutover_1/handoff.md",
        )
        .unwrap();
        let second = claim_master_cutover(
            &db,
            "cutover_2",
            "s_cutover",
            Some(111),
            "/tmp/ah-state",
            "/tmp/ah-state/ahd.sock",
            "/tmp/ah-state/cutovers/cutover_2/handoff.md",
        )
        .unwrap();
        let active = get_active_master_cutover(&db, "s_cutover")
            .unwrap()
            .expect("active cutover");

        assert_eq!(first, MasterCutoverClaim::Claimed);
        assert_eq!(second, MasterCutoverClaim::AlreadyActive);
        assert_eq!(active.id, "cutover_1");
        assert_eq!(active.state, "PREPARING");

        assert_eq!(
            release_master_cutover(&db, "cutover_1", "PREPARING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            claim_master_cutover(
                &db,
                "cutover_3",
                "s_cutover",
                Some(111),
                "/tmp/ah-state",
                "/tmp/ah-state/ahd.sock",
                "/tmp/ah-state/cutovers/cutover_3/handoff.md",
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );
    }

    #[test]
    fn master_cutover_state_transitions_are_cas_guarded() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        insert_session(&db, "s_cutover_cas");
        assert_eq!(
            claim_master_cutover(
                &db,
                "cutover_cas",
                "s_cutover_cas",
                Some(222),
                "/tmp/ah-state",
                "/tmp/ah-state/ahd.sock",
                "/tmp/ah-state/cutovers/cutover_cas/handoff.md",
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );

        assert_eq!(
            update_master_cutover_state(&db, "cutover_cas", "VERIFYING", "ACTIVE").unwrap(),
            MasterCutoverUpdate::Stale
        );
        let active = get_active_master_cutover(&db, "s_cutover_cas")
            .unwrap()
            .expect("active cutover");
        assert_eq!(active.state, "PREPARING");

        assert_eq!(
            update_master_cutover_state(&db, "cutover_cas", "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        let active = get_active_master_cutover(&db, "s_cutover_cas")
            .unwrap()
            .expect("active cutover");
        assert_eq!(active.state, "SPAWNING");
    }

    #[test]
    fn master_cutover_spawn_metadata_is_cas_guarded_and_readable() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        insert_session(&db, "s_cutover_meta");
        assert_eq!(
            claim_master_cutover(
                &db,
                "cutover_meta",
                "s_cutover_meta",
                Some(333),
                "/tmp/ah-state",
                "/tmp/ah-state/ahd.sock",
                "/tmp/ah-state/cutovers/cutover_meta/handoff.md",
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &db,
                "cutover_meta",
                "SPAWNING",
                "VERIFYING",
                9001,
                7,
                "%42",
            )
            .unwrap(),
            MasterCutoverUpdate::Stale
        );
        assert_eq!(
            update_master_cutover_state(&db, "cutover_meta", "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &db,
                "cutover_meta",
                "SPAWNING",
                "VERIFYING",
                9001,
                7,
                "%42",
            )
            .unwrap(),
            MasterCutoverUpdate::Updated
        );

        let active = get_active_master_cutover(&db, "s_cutover_meta")
            .unwrap()
            .expect("active cutover");
        assert_eq!(active.state, "VERIFYING");
        assert_eq!(active.new_master_pid, Some(9001));
        assert_eq!(active.new_master_generation, Some(7));
        assert_eq!(active.new_master_pane_id.as_deref(), Some("%42"));
    }

    #[test]
    fn master_ack_ready_only_marks_verifying_cutover() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        insert_session(&db, "s_cutover_ack");
        assert_eq!(
            claim_master_cutover(
                &db,
                "cutover_ack",
                "s_cutover_ack",
                Some(444),
                "/tmp/ah-state",
                "/tmp/ah-state/ahd.sock",
                "/tmp/ah-state/cutovers/cutover_ack/handoff.md",
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );

        assert_eq!(
            mark_master_cutover_ack_ready(&db, "cutover_ack", "ack").unwrap(),
            MasterCutoverUpdate::Stale
        );
        assert_eq!(
            update_master_cutover_state(&db, "cutover_ack", "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &db,
                "cutover_ack",
                "SPAWNING",
                "VERIFYING",
                9002,
                8,
                "%43",
            )
            .unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            mark_master_cutover_ack_ready(&db, "cutover_ack", "ack").unwrap(),
            MasterCutoverUpdate::Updated
        );

        let conn = db.conn();
        let (ack_ready_at, readiness_mode): (Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT ack_ready_at, readiness_mode FROM master_cutovers WHERE id = ?1",
                ["cutover_ack"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(ack_ready_at.is_some());
        assert_eq!(readiness_mode.as_deref(), Some("ack"));
    }

    #[test]
    fn master_late_ack_after_failed_cutover_is_stale() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        insert_session(&db, "s_cutover_late_ack");
        assert_eq!(
            claim_master_cutover(
                &db,
                "cutover_late_ack",
                "s_cutover_late_ack",
                Some(444),
                "/tmp/ah-state",
                "/tmp/ah-state/ahd.sock",
                "/tmp/ah-state/cutovers/cutover_late_ack/handoff.md",
            )
            .unwrap(),
            MasterCutoverClaim::Claimed
        );
        assert_eq!(
            update_master_cutover_state(&db, "cutover_late_ack", "PREPARING", "SPAWNING").unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_spawn_metadata(
                &db,
                "cutover_late_ack",
                "SPAWNING",
                "VERIFYING",
                9002,
                8,
                "%43",
            )
            .unwrap(),
            MasterCutoverUpdate::Updated
        );
        assert_eq!(
            update_master_cutover_state(&db, "cutover_late_ack", "VERIFYING", "FAILED").unwrap(),
            MasterCutoverUpdate::Updated
        );

        assert_eq!(
            mark_master_cutover_ack_ready(&db, "cutover_late_ack", "ack").unwrap(),
            MasterCutoverUpdate::Stale
        );

        let cutover = get_master_cutover(&db, "cutover_late_ack")
            .unwrap()
            .expect("cutover row");
        assert_eq!(cutover.state, "FAILED");
        assert!(cutover.ack_ready_at.is_none());
    }
}
