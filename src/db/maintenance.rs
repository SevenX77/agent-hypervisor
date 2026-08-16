//! State-database maintenance: keep the daemon's own storage bounded.
//!
//! The state database is an ah artifact, not a project asset (decision 0004),
//! so it must stay proportional to what the daemon actually needs rather than
//! to how long the stack has been up. Two things were missing: nothing removed
//! old rows, and nothing returned freed pages to the filesystem — which is how
//! a database holding kilobytes of live state reaches gigabytes on disk (#23).
//!
//! Retention is graded by what a row is for, not by age alone. `events` carries
//! two populations: forensic rows (state changes, evidence, failures) that are
//! the only record of why something happened, and firehose rows (pane output)
//! that matter for minutes. Pruning both on one rule is what makes a retention
//! policy delete the failure you needed (#20), so the firehose is capped by
//! count and the forensic rows are kept far longer.

use crate::error::AhError;
use rusqlite::Connection;

/// Event types that stream at pane speed. They are read while an agent is live
/// and have no diagnostic value once the run is over.
const FIREHOSE_EVENT_TYPES: &[&str] = &["output_chunk"];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetentionPolicy {
    /// Newest firehose events to keep, across all agents.
    pub firehose_events: i64,
    /// Newest non-firehose events to keep. Sized so an ordinary run keeps its
    /// whole forensic history.
    pub forensic_events: i64,
    /// Newest job transition rows to keep.
    pub job_transitions: i64,
    /// Reclaim the file with a full `VACUUM` only when free pages exceed this
    /// share of the database and it is big enough for that to matter.
    pub vacuum_free_ratio: f64,
    pub vacuum_min_bytes: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            firehose_events: 20_000,
            forensic_events: 50_000,
            job_transitions: 20_000,
            vacuum_free_ratio: 0.25,
            vacuum_min_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub firehose_events_deleted: usize,
    pub forensic_events_deleted: usize,
    pub job_transitions_deleted: usize,
    pub vacuumed: bool,
}

impl MaintenanceReport {
    pub fn deleted_rows(&self) -> usize {
        self.firehose_events_deleted + self.forensic_events_deleted + self.job_transitions_deleted
    }
}

/// Applies the retention policy and returns freed space to the filesystem.
///
/// Deleting rows alone does not shrink a SQLite file: the pages land on the
/// free list and the file only ratchets upward. New databases are created with
/// incremental auto-vacuum so the pages come back cheaply; databases created
/// before that can only be compacted by rewriting the file, which is why a full
/// `VACUUM` runs solely when the waste is both large in share and in bytes.
pub fn run_state_maintenance(
    conn: &Connection,
    policy: RetentionPolicy,
) -> Result<MaintenanceReport, AhError> {
    let mut report = MaintenanceReport::default();

    report.firehose_events_deleted =
        delete_events_beyond_cap(conn, FIREHOSE_EVENT_TYPES, true, policy.firehose_events)?;
    report.forensic_events_deleted =
        delete_events_beyond_cap(conn, FIREHOSE_EVENT_TYPES, false, policy.forensic_events)?;
    report.job_transitions_deleted =
        delete_job_transitions_beyond_cap(conn, policy.job_transitions)?;

    if report.deleted_rows() > 0 {
        reclaim_space(conn, policy, &mut report)?;
    }
    Ok(report)
}

/// Keeps the newest `keep` rows of a population and deletes the rest.
///
/// `matching` selects the population: firehose types when true, everything else
/// when false — one query shape, so the two populations cannot drift apart.
fn delete_events_beyond_cap(
    conn: &Connection,
    types: &[&str],
    matching: bool,
    keep: i64,
) -> Result<usize, AhError> {
    let placeholders = types.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let predicate = if matching {
        format!("event_type IN ({placeholders})")
    } else {
        format!("event_type NOT IN ({placeholders})")
    };
    let sql = format!(
        "DELETE FROM events WHERE {predicate} AND seq_id NOT IN (
             SELECT seq_id FROM events WHERE {predicate} ORDER BY seq_id DESC LIMIT ?
         )"
    );
    let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for value in types {
        params.push(value);
    }
    for value in types {
        params.push(value);
    }
    params.push(&keep);
    conn.execute(&sql, params.as_slice())
        .map_err(|err| AhError::DbConstraintViolation(format!("prune events: {err}")))
}

fn delete_job_transitions_beyond_cap(conn: &Connection, keep: i64) -> Result<usize, AhError> {
    conn.execute(
        "DELETE FROM job_transitions WHERE job_event_id NOT IN (
             SELECT job_event_id FROM job_transitions ORDER BY job_event_id DESC LIMIT ?1
         )",
        [keep],
    )
    .map_err(|err| AhError::DbConstraintViolation(format!("prune job_transitions: {err}")))
}

fn reclaim_space(
    conn: &Connection,
    policy: RetentionPolicy,
    report: &mut MaintenanceReport,
) -> Result<(), AhError> {
    // Incremental first: on a database created with auto_vacuum=INCREMENTAL this
    // returns the freed pages immediately and costs almost nothing. On any other
    // database it is a no-op, which is why the ratio check below still exists.
    let _ = conn.execute_batch("PRAGMA incremental_vacuum;");

    let page_size = pragma_i64(conn, "page_size").unwrap_or(0);
    let page_count = pragma_i64(conn, "page_count").unwrap_or(0);
    let free_pages = pragma_i64(conn, "freelist_count").unwrap_or(0);
    let total_bytes = (page_size.max(0) as u64) * (page_count.max(0) as u64);
    let free_ratio = if page_count > 0 {
        free_pages as f64 / page_count as f64
    } else {
        0.0
    };

    if total_bytes >= policy.vacuum_min_bytes && free_ratio >= policy.vacuum_free_ratio {
        conn.execute_batch("VACUUM;")
            .map_err(|err| AhError::DbConstraintViolation(format!("vacuum state db: {err}")))?;
        report.vacuumed = true;
    }

    // The write-ahead log grows with churn and is not truncated by a checkpoint
    // alone; without this the freed space simply moves into the -wal file.
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    Ok(())
}

fn pragma_i64(conn: &Connection, name: &str) -> Option<i64> {
    conn.pragma_query_value(None, name, |row| row.get(0)).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn insert_event(conn: &Connection, event_type: &str, payload: &str) {
        conn.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES ('a1', NULL, ?1, ?2)",
            rusqlite::params![event_type, payload],
        )
        .unwrap();
    }

    fn seed_agent(conn: &Connection) {
        crate::db::sessions::insert_session_sync(conn, "s1", "p1", "/tmp/p1")
            .expect("seed session");
        crate::db::agents::insert_agent_sync(conn, "a1", "s1", "bash", "IDLE", None)
            .expect("seed agent");
    }

    #[test]
    fn firehose_events_are_capped_while_forensic_events_survive() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let database = db::init(file.path()).unwrap();
        let conn = database.conn();
        seed_agent(&conn);

        for i in 0..500 {
            insert_event(&conn, "output_chunk", &format!("{{\"text\":\"{i}\"}}"));
        }
        for i in 0..40 {
            insert_event(
                &conn,
                "state_change",
                &format!("{{\"to\":\"BUSY\",\"i\":{i}}}"),
            );
        }

        let policy = RetentionPolicy {
            firehose_events: 100,
            forensic_events: 1_000,
            ..RetentionPolicy::default()
        };
        let report = run_state_maintenance(&conn, policy).unwrap();

        let firehose: i64 = conn
            .query_row(
                "SELECT count(*) FROM events WHERE event_type = 'output_chunk'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let forensic: i64 = conn
            .query_row(
                "SELECT count(*) FROM events WHERE event_type = 'state_change'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(firehose, 100, "pane output must be capped");
        assert_eq!(
            forensic, 40,
            "state changes are the record of why something happened and must survive"
        );
        assert_eq!(report.firehose_events_deleted, 400);
        assert_eq!(report.forensic_events_deleted, 0);
    }

    #[test]
    fn the_newest_events_are_the_ones_kept() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let database = db::init(file.path()).unwrap();
        let conn = database.conn();
        seed_agent(&conn);
        for i in 0..20 {
            insert_event(&conn, "output_chunk", &format!("{{\"text\":\"{i}\"}}"));
        }

        run_state_maintenance(
            &conn,
            RetentionPolicy {
                firehose_events: 5,
                ..RetentionPolicy::default()
            },
        )
        .unwrap();

        let payloads: Vec<String> = conn
            .prepare("SELECT payload FROM events ORDER BY seq_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(payloads.len(), 5);
        assert!(
            payloads[0].contains("\"15\""),
            "retention must keep the newest rows, got {payloads:?}"
        );
    }

    #[test]
    fn maintenance_is_idempotent_and_cheap_on_a_small_database() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let database = db::init(file.path()).unwrap();
        let conn = database.conn();
        seed_agent(&conn);
        insert_event(&conn, "state_change", "{\"to\":\"IDLE\"}");

        let first = run_state_maintenance(&conn, RetentionPolicy::default()).unwrap();
        let second = run_state_maintenance(&conn, RetentionPolicy::default()).unwrap();

        assert_eq!(first.deleted_rows(), 0);
        assert_eq!(second.deleted_rows(), 0);
        assert!(
            !first.vacuumed,
            "a small tidy database must not be rewritten"
        );
    }

    /// The property this exists for: churn must not ratchet the file upward.
    #[test]
    fn repeated_churn_does_not_grow_the_database_without_bound() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let database = db::init(file.path()).unwrap();
        let conn = database.conn();
        seed_agent(&conn);
        let policy = RetentionPolicy {
            firehose_events: 200,
            ..RetentionPolicy::default()
        };
        let payload = "x".repeat(2_000);

        let mut sizes = Vec::new();
        for _ in 0..6 {
            for _ in 0..2_000 {
                insert_event(&conn, "output_chunk", &payload);
            }
            run_state_maintenance(&conn, policy).unwrap();
            sizes.push(std::fs::metadata(file.path()).unwrap().len());
        }

        let settled = sizes[sizes.len() - 1];
        let after_first = sizes[1];
        assert!(
            settled <= after_first * 2,
            "database must settle instead of ratcheting: {sizes:?}"
        );
    }
}
