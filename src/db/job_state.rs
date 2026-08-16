//! D1 — Job State Machine Gate (`ah-control-plane-refactor` Phase 1).
//!
//! # Ownership of this file
//!
//! This module was authored by **g2 (泳道2 质量门 / lane-2 gatekeeper)** as the
//! RED contract for D1. It has two clearly separated halves:
//!
//! 1. **CONTRACT SURFACE** (this half): the `JobStatus` vocabulary and the gate
//!    function *signatures*, pinned verbatim from the control-plane
//!    refactor Requirement/Design **D1**. The bodies are `todo!()` scaffolding.
//! 2. **RED CONTRACT TESTS** (`mod tests` at the bottom): the acceptance tests
//!    g2-m1 must turn GREEN by implementing the bodies.
//!
//! **g2-m1**: implement the `todo!()` bodies (and migrate the scattered
//! `UPDATE jobs SET status` call sites in `db::jobs`/`db::recovery` onto
//! `transit_job_state`). You may raise a *signature-adaptation* question to g2
//! (lane gate terminally decides) if a signature is genuinely unimplementable —
//! but do **not** weaken the asserted behavior, and do **not** edit `mod tests`.
//!
//! # Design references (do not re-derive — implement to these)
//!
//! - Transition table (design.md D1 + requeue edges, g2 lane ruling 2026-07-10):
//!   ```text
//!   QUEUED     -> DISPATCHED | CANCELLED | FAILED
//!   DISPATCHED -> COMPLETED  | FAILED    | CANCELLED | QUEUED
//!   FAILED     -> COMPLETED  | QUEUED
//!   ```
//!   Requeue edges (added by the D1 gate audit — both back real production paths):
//!   - `DISPATCHED -> QUEUED` = pre-send / IO-failure / stale-pane requeue. Used
//!     ONLY when the caller has established the agent provably never received the
//!     work (`requeue_dispatched_job_before_send_sync`,
//!     `requeue_recovered_dispatch_io_failure_sync`, the ACK pre-send path). It is
//!     NOT a reopen of a running job; production keys it on `requeue_pre_send`, and
//!     a genuine post-send failure goes `DISPATCHED -> FAILED` instead.
//!   - `FAILED -> QUEUED` = recovery / retry requeue of a genuinely failed job
//!     (`requeue_interrupted_job_from_captured_intent_sync`).
//!   - `FAILED -> COMPLETED` is NARROW, GUARDED (late-evidence reconciliation only);
//!     `FAILED -> DISPATCHED` stays illegal (re-dispatch goes via QUEUED).
//! - Gate is CAS-style, mirroring `state_machine::transit_agent_state_conn_sync`
//!   (explicit `expected_from`, so a stale caller cannot silently stomp a status
//!   another writer already moved past).
//! - `jobs.status` has exactly ONE writer: this gate (design.md D7). The gate
//!   never writes `agents.state`.
//! - The `FAILED -> COMPLETED` guard is modeled directly on the existing
//!   `state_machine::late_health_completion_stuck_allows_terminal` accept-gate
//!   (design.md D1: "model this transition's guard directly on the existing
//!   accept-gate's logic where practical, not design a new pattern from scratch").
//! - Audit rows are appended via the existing `jobs::record_job_transition_conn_sync`
//!   into `job_transitions` (single audit surface — reuse, do not reinvent).
#![allow(dead_code)] // scaffolding: bodies unimplemented until g2-m1 lands D1.

use crate::error::CcbdError;
use rusqlite::Connection;

/// Business status of a job. The DB representation is the SCREAMING-CASE string
/// stored in `jobs.status` (`'QUEUED'`, `'DISPATCHED'`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Dispatched,
    Completed,
    Cancelled,
    Failed,
}

impl JobStatus {
    /// Canonical DB string for this status (round-trips with [`JobStatus::from_db_str`]).
    pub fn as_db_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "QUEUED",
            JobStatus::Dispatched => "DISPATCHED",
            JobStatus::Completed => "COMPLETED",
            JobStatus::Cancelled => "CANCELLED",
            JobStatus::Failed => "FAILED",
        }
    }

    /// Parse a DB status string; `None` for any unrecognized value.
    pub fn from_db_str(s: &str) -> Option<JobStatus> {
        match s {
            "QUEUED" => Some(JobStatus::Queued),
            "DISPATCHED" => Some(JobStatus::Dispatched),
            "COMPLETED" => Some(JobStatus::Completed),
            "CANCELLED" => Some(JobStatus::Cancelled),
            "FAILED" => Some(JobStatus::Failed),
            _ => None,
        }
    }
}

/// D1 GATE — the single writer of `jobs.status`.
///
/// CAS-style transition. The write is applied **iff both**:
///   (a) the DB's *actual* current status equals `expected_from` (no stale
///       stomp — a caller whose belief is out of date is rejected), **and**
///   (b) `expected_from -> to` is a permitted edge of the transition table above.
///
/// Any other `(from, to)` pair, or a CAS mismatch, is **rejected** (`Err`) and
/// MUST leave `jobs.status` untouched (no partial write, no audit row). On
/// success it appends a `job_transitions` audit row carrying `reason`.
///
/// The `FAILED -> COMPLETED` edge is table-legal but additionally **guarded**:
/// it succeeds only when authoritative late-completion evidence exists for the
/// job (see module docs — modeled on `late_health_completion_stuck_allows_terminal`).
/// Absent that evidence it is rejected exactly like any illegal edge. It is NOT a
/// general "reopen any failed job" path and its guard must not be loosened.
pub(crate) fn transit_job_state(
    conn: &Connection,
    job_id: &str,
    expected_from: JobStatus,
    to: JobStatus,
    reason: &str,
) -> Result<(), CcbdError> {
    let job = crate::db::jobs::query_job_sync(conn, job_id)?
        .ok_or_else(|| CcbdError::DbConstraintViolation(format!("job {job_id} not found")))?;

    let actual_status = JobStatus::from_db_str(&job.status).ok_or_else(|| {
        CcbdError::DbConstraintViolation(format!("unrecognized job status in DB: {}", job.status))
    })?;

    if actual_status != expected_from {
        return Err(CcbdError::DbConstraintViolation(format!(
            "CAS mismatch for job {job_id}: expected {expected_from:?}, actual {actual_status:?}"
        )));
    }

    let is_legal = match (expected_from, to) {
        (JobStatus::Queued, JobStatus::Dispatched)
        | (JobStatus::Queued, JobStatus::Cancelled)
        | (JobStatus::Queued, JobStatus::Failed) => true,

        (JobStatus::Dispatched, JobStatus::Completed)
        | (JobStatus::Dispatched, JobStatus::Failed)
        | (JobStatus::Dispatched, JobStatus::Cancelled)
        | (JobStatus::Dispatched, JobStatus::Queued) => true,

        (JobStatus::Failed, JobStatus::Completed) => {
            let has_evidence = check_late_completion_evidence(conn, &job.agent_id, job_id)?;
            if !has_evidence {
                return Err(CcbdError::DbConstraintViolation(format!(
                    "FAILED -> COMPLETED transition refused for job {job_id}: no late evidence"
                )));
            }
            true
        }
        (JobStatus::Failed, JobStatus::Queued) => true,
        _ => false,
    };

    if !is_legal {
        return Err(CcbdError::DbConstraintViolation(format!(
            "illegal state transition for job {job_id} from {expected_from:?} to {to:?}"
        )));
    }

    let expected_str = expected_from.as_db_str();
    let to_str = to.as_db_str();
    match to {
        JobStatus::Dispatched => {
            let changes = conn.execute(
                "UPDATE jobs SET status = ?, dispatched_at = unixepoch() WHERE id = ? AND status = ?",
                rusqlite::params![to_str, job_id, expected_str],
            )
            .map_err(|err| crate::db::common::map_db_error("transit job status and dispatched_at", err))?;
            if changes == 0 {
                return Err(CcbdError::DbConstraintViolation(format!(
                    "CAS mismatch for job {job_id} during UPDATE: expected status {expected_str}"
                )));
            }
        }
        JobStatus::Completed | JobStatus::Cancelled | JobStatus::Failed => {
            let changes = conn.execute(
                "UPDATE jobs SET status = ?, completed_at = unixepoch() WHERE id = ? AND status = ?",
                rusqlite::params![to_str, job_id, expected_str],
            )
            .map_err(|err| crate::db::common::map_db_error("transit job status and completed_at", err))?;
            if changes == 0 {
                return Err(CcbdError::DbConstraintViolation(format!(
                    "CAS mismatch for job {job_id} during UPDATE: expected status {expected_str}"
                )));
            }
        }
        JobStatus::Queued => {
            let changes = conn.execute(
                "UPDATE jobs SET status = ?, dispatched_at = NULL, dispatched_at_seq_id = NULL, completed_at = NULL WHERE id = ? AND status = ?",
                rusqlite::params![to_str, job_id, expected_str],
            )
            .map_err(|err| crate::db::common::map_db_error("transit job status and clear execution fields", err))?;
            if changes == 0 {
                return Err(CcbdError::DbConstraintViolation(format!(
                    "CAS mismatch for job {job_id} during UPDATE: expected status {expected_str}"
                )));
            }
        }
    }

    let changed_fields: &[&str] = match to {
        JobStatus::Dispatched => &["status", "dispatched_at"],
        JobStatus::Completed => &["status", "completed_at"],
        JobStatus::Cancelled => &["status", "completed_at"],
        JobStatus::Failed => &["status", "completed_at"],
        JobStatus::Queued => &[
            "status",
            "dispatched_at",
            "dispatched_at_seq_id",
            "completed_at",
            "error_reason",
        ],
    };

    crate::db::jobs::record_job_transition_conn_sync(
        conn,
        job_id,
        Some(expected_from.as_db_str()),
        Some(to.as_db_str()),
        "job_transition",
        changed_fields,
        reason,
    )?;

    Ok(())
}

fn check_late_completion_evidence(
    conn: &Connection,
    agent_id: &str,
    dispatched_job_id: &str,
) -> Result<bool, CcbdError> {
    use rusqlite::OptionalExtension;
    use serde_json::Value;
    let payload = conn
        .query_row(
            "SELECT payload FROM events \
             WHERE agent_id = ? AND event_type = 'state_change' \
             ORDER BY seq_id DESC LIMIT 1",
            rusqlite::params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| {
            crate::db::common::map_db_error("query latest state_change for late completion", err)
        })?;
    let Some(payload) = payload else {
        return Ok(false);
    };
    let payload: Value = serde_json::from_str(&payload).unwrap_or_else(|_| serde_json::json!({}));
    let is_health_completion = payload.get("reason").and_then(Value::as_str)
        == Some("HEALTH_CHECK_STUCK")
        && payload
            .get("signal_kinds")
            .and_then(Value::as_array)
            .is_some_and(|signals| {
                signals
                    .iter()
                    .any(|signal| signal.as_str() == Some("health:completion"))
            });
    let same_job = payload.get("job_id").and_then(Value::as_str) == Some(dispatched_job_id);
    Ok(is_health_completion && same_job)
}

/// gap-patch #2 — **cancel driver + timeout takeover** (design.md D1, Gen-2 incident).
///
/// Orchestrator-authoritative forced cancel. If `job_id` is `DISPATCHED` with
/// `cancel_requested = 1` and the cancel has been pending — measured from the
/// moment it was requested — for at least `timeout_secs` as of `now_epoch`, this
/// drives `DISPATCHED -> CANCELLED` **through [`transit_job_state`]** and returns
/// `Ok(true)`. It returns `Ok(false)` when the bounded timeout has not yet
/// elapsed (or the job is not a cancel-pending `DISPATCHED` job).
///
/// CRITICAL contract: this path MUST NOT require the agent to be `IDLE`/`UNKNOWN`.
/// It exists precisely for dead / hung / unresponsive agents (and semantic
/// false-completion turns) that never acknowledge the cancel — contrast
/// `jobs::mark_dispatched_job_cancelled_if_agent_idle_sync`, which depends on
/// agent cooperation and therefore can hang forever. "Cancel can hang forever
/// with no takeover path" is the exact gap this closes.
///
/// (`now_epoch` is injected rather than read from the wall clock so the takeover
/// is deterministically testable; production callers pass `unixepoch()`.)
pub(crate) fn force_cancel_pending_dispatched_job_conn_sync(
    conn: &Connection,
    job_id: &str,
    now_epoch: i64,
    timeout_secs: i64,
) -> Result<bool, CcbdError> {
    use rusqlite::OptionalExtension;
    let job = crate::db::jobs::query_job_sync(conn, job_id)?
        .ok_or_else(|| CcbdError::DbConstraintViolation(format!("job {job_id} not found")))?;

    if job.status != "DISPATCHED" || !job.cancel_requested {
        return Ok(false);
    }

    let requested_at: Option<i64> = conn
        .query_row(
            "SELECT created_at FROM job_transitions \
             WHERE job_id = ? AND reason = 'cancel_requested' \
             ORDER BY job_event_id DESC LIMIT 1",
            rusqlite::params![job_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|err| crate::db::common::map_db_error("query cancel_requested time", err))?;

    let Some(requested_at) = requested_at else {
        return Ok(false);
    };

    if now_epoch - requested_at >= timeout_secs {
        transit_job_state(
            conn,
            job_id,
            JobStatus::Dispatched,
            JobStatus::Cancelled,
            "cancel_timeout_takeover",
        )?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) fn requeue_job_state_conn_sync(
    conn: &Connection,
    job_id: &str,
    expected_from: JobStatus,
    error_reason: Option<&str>,
    reason: &str,
) -> Result<(), CcbdError> {
    // 1. CAS pre-check to avoid updating error_reason if we will fail CAS anyway
    let job = crate::db::jobs::query_job_sync(conn, job_id)?
        .ok_or_else(|| CcbdError::DbConstraintViolation(format!("job {job_id} not found")))?;

    let actual_status = JobStatus::from_db_str(&job.status).ok_or_else(|| {
        CcbdError::DbConstraintViolation(format!("unrecognized job status in DB: {}", job.status))
    })?;

    if actual_status != expected_from {
        return Err(CcbdError::DbConstraintViolation(format!(
            "CAS mismatch for job {job_id}: expected {expected_from:?}, actual {actual_status:?}"
        )));
    }

    // 2. Set error_reason first (as it is a non-status field)
    conn.execute(
        "UPDATE jobs SET error_reason = ? WHERE id = ?",
        rusqlite::params![error_reason, job_id],
    )
    .map_err(|err| crate::db::common::map_db_error("update error_reason for requeue", err))?;

    // 3. Delegate to transit_job_state for transition check, status write, and audit row insertion
    transit_job_state(conn, job_id, expected_from, JobStatus::Queued, reason)?;
    let transition_id = conn.last_insert_rowid();
    crate::runtime_observation::store::append_for_current_lifecycle_sync(
        conn,
        &format!("requeue:{job_id}:{transition_id}"),
        &job.agent_id,
        Some(job_id),
        crate::runtime_observation::EvidenceSource::ControlPlane,
        crate::runtime_observation::ProviderObservationKind::Turn(
            crate::runtime_observation::ProviderTurnState::Queued,
        ),
        crate::runtime_observation::store::now_epoch_millis(),
    )?;

    Ok(())
}

// ============================================================================
// G2 RED CONTRACT TESTS — authored by g2 (lane gate). g2-m1 MUST NOT MODIFY.
// Turn these GREEN by implementing the bodies above (and the call-site migration
// / observability wiring the gap-patch tests assert). The test body itself is
// the acceptance contract; the master verifies it and CI is the full gate.
// ============================================================================
#[cfg(test)]
mod tests {
    use super::{JobStatus, force_cancel_pending_dispatched_job_conn_sync, transit_job_state};
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::jobs::{
        claim_next_job_sync, insert_job_sync, mark_dispatched_job_cancelled_if_agent_idle_sync,
        mark_job_failed_conn_sync, query_job_sync, request_dispatched_job_cancel_sync,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};
    use rusqlite::params;
    use serde_json::json;

    // ---- fixtures ---------------------------------------------------------

    /// Temp-file DB seeded with session `s1` + agent `a1` (IDLE), mirroring
    /// `jobs::tests::with_test_db` so these tests share the crate's conventions.
    fn with_gate_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
        }
        test(&db)
    }

    fn status(db: &Db, job_id: &str) -> String {
        query_job_sync(&db.conn(), job_id).unwrap().unwrap().status
    }

    /// Arrange a job's status directly (test fixture only — bypasses the gate to
    /// set up a precondition state; production writes go through the gate).
    fn set_status_raw(db: &Db, job_id: &str, db_status: &str) {
        let conn = db.conn();
        conn.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            params![db_status, job_id],
        )
        .unwrap();
    }

    /// Simulate an in-flight job without exercising the dispatch path.
    fn dispatch_raw(db: &Db, job_id: &str) {
        let conn = db.conn();
        conn.execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = ?",
            params![job_id],
        )
        .unwrap();
    }

    fn set_agent_state_raw(db: &Db, agent_id: &str, state: &str) {
        let conn = db.conn();
        conn.execute(
            "UPDATE agents SET state = ? WHERE id = ?",
            params![state, agent_id],
        )
        .unwrap();
    }

    /// True iff a `job_transitions` audit row exists for exactly this edge.
    fn transition_exists(db: &Db, job_id: &str, old: Option<&str>, new: Option<&str>) -> bool {
        let conn = db.conn();
        conn.query_row(
            "SELECT COUNT(*) FROM job_transitions \
             WHERE job_id = ? AND old_status IS ? AND new_status IS ?",
            params![job_id, old, new],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    /// When the cancel was requested (audit row created_at) — reference point for
    /// the bounded-timeout takeover.
    fn cancel_requested_at(db: &Db, job_id: &str) -> i64 {
        let conn = db.conn();
        conn.query_row(
            "SELECT created_at FROM job_transitions \
             WHERE job_id = ? AND reason = 'cancel_requested' \
             ORDER BY job_event_id DESC LIMIT 1",
            params![job_id],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    }

    /// Observable deferral signals surfaced for an agent (gap-patch #1). The
    /// mechanism is g2-m1's choice, but it must be a *queryable* `events` row
    /// (surfaced by `ah events`), not a fire-and-forget tracing line.
    fn dispatch_deferred_events(db: &Db, agent_id: &str) -> Vec<String> {
        let conn = db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT payload FROM events \
                 WHERE agent_id = ? AND event_type = 'dispatch_deferred' \
                 ORDER BY seq_id ASC",
            )
            .unwrap();
        let rows = stmt
            .query_map(params![agent_id], |r| r.get::<_, String>(0))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    // ---- Suite A: JobStatus vocabulary -----------------------------------

    // The gate speaks in a closed `JobStatus` enum, not stringly-typed status;
    // the enum must round-trip the on-disk vocabulary exactly.
    #[test]
    fn job_status_round_trips_db_strings() {
        let pairs = [
            (JobStatus::Queued, "QUEUED"),
            (JobStatus::Dispatched, "DISPATCHED"),
            (JobStatus::Completed, "COMPLETED"),
            (JobStatus::Cancelled, "CANCELLED"),
            (JobStatus::Failed, "FAILED"),
        ];
        for (st, s) in pairs {
            assert_eq!(st.as_db_str(), s, "{st:?} must serialize to {s}");
            assert_eq!(
                JobStatus::from_db_str(s),
                Some(st),
                "{s} must parse to {st:?}"
            );
        }
        assert_eq!(
            JobStatus::from_db_str("NOT_A_STATUS"),
            None,
            "unknown status strings must not parse"
        );
    }

    // ---- Suite B: transition table + CAS ---------------------------------

    // Every edge the design's table permits is accepted, mutates status, and is
    // audited. (FAILED->COMPLETED is excluded here — it is guarded, see Suite C.)
    #[test]
    fn gate_applies_all_legal_base_transitions() {
        let edges = [
            (JobStatus::Queued, JobStatus::Dispatched),
            (JobStatus::Queued, JobStatus::Cancelled),
            (JobStatus::Queued, JobStatus::Failed),
            (JobStatus::Dispatched, JobStatus::Completed),
            (JobStatus::Dispatched, JobStatus::Failed),
            (JobStatus::Dispatched, JobStatus::Cancelled),
            (JobStatus::Dispatched, JobStatus::Queued), // pre-send / IO-failure requeue
            (JobStatus::Failed, JobStatus::Queued),     // recovery / retry requeue
        ];
        for (from, to) in edges {
            with_gate_db(|db| {
                let job = "job_edge";
                {
                    let conn = db.conn();
                    insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
                }
                set_status_raw(db, job, from.as_db_str());

                transit_job_state(&db.conn(), job, from, to, "edge_test").unwrap_or_else(|e| {
                    panic!("legal {from:?}->{to:?} must be accepted, got {e:?}")
                });

                assert_eq!(
                    status(db, job),
                    to.as_db_str(),
                    "legal {from:?}->{to:?} must move status to {to:?}"
                );
                assert!(
                    transition_exists(db, job, Some(from.as_db_str()), Some(to.as_db_str())),
                    "legal {from:?}->{to:?} must record a job_transitions audit row"
                );
            });
        }
    }

    // Illegal edges are rejected *at the gate* (not silently no-op'd, not merely
    // discouraged by convention) and leave both status and audit trail untouched.
    #[test]
    fn gate_rejects_illegal_transitions_without_side_effects() {
        // NOTE (g2, 2026-07-10): `DISPATCHED->QUEUED` and `FAILED->QUEUED` were
        // moved OUT of this illegal set — they are now legal requeue edges (see the
        // module transition table and `gate_applies_all_legal_base_transitions`).
        // The stale "no going backwards" assertion predated the requeue design.
        let illegal = [
            (JobStatus::Queued, JobStatus::Completed), // must run before completing
            (JobStatus::Completed, JobStatus::Dispatched), // terminal, no reopen
            (JobStatus::Completed, JobStatus::Failed),
            (JobStatus::Completed, JobStatus::Queued),
            (JobStatus::Cancelled, JobStatus::Dispatched), // terminal, no reopen
            (JobStatus::Cancelled, JobStatus::Completed),
            // FAILED reopens only via the COMPLETED guard or a QUEUED requeue —
            // never straight back to DISPATCHED.
            (JobStatus::Failed, JobStatus::Dispatched),
        ];
        for (from, to) in illegal {
            with_gate_db(|db| {
                let job = "job_illegal";
                {
                    let conn = db.conn();
                    insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
                }
                set_status_raw(db, job, from.as_db_str());

                let result = transit_job_state(&db.conn(), job, from, to, "illegal_test");
                assert!(
                    result.is_err(),
                    "illegal {from:?}->{to:?} must be rejected at the gate"
                );
                assert_eq!(
                    status(db, job),
                    from.as_db_str(),
                    "rejected {from:?}->{to:?} must leave status untouched"
                );
                assert!(
                    !transition_exists(db, job, Some(from.as_db_str()), Some(to.as_db_str())),
                    "rejected {from:?}->{to:?} must not write an audit row"
                );
            });
        }
    }

    // CAS: even a table-legal edge is rejected if the caller's `expected_from`
    // does not match the DB's actual status — a stale caller cannot stomp a
    // status another writer already advanced.
    #[test]
    fn gate_rejects_stale_expected_from_on_table_legal_edge() {
        // Actual is DISPATCHED; caller still believes QUEUED and re-dispatches.
        with_gate_db(|db| {
            let job = "job_cas_dispatch";
            {
                let conn = db.conn();
                insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
            }
            set_status_raw(db, job, "DISPATCHED");

            let result = transit_job_state(
                &db.conn(),
                job,
                JobStatus::Queued,
                JobStatus::Dispatched,
                "stale",
            );
            assert!(
                result.is_err(),
                "CAS mismatch (expected QUEUED, actual DISPATCHED) must be rejected"
            );
            assert_eq!(
                status(db, job),
                "DISPATCHED",
                "a stale caller must not stomp the current status"
            );
        });

        // Actual is COMPLETED; a stale caller believes DISPATCHED and re-completes.
        with_gate_db(|db| {
            let job = "job_cas_complete";
            {
                let conn = db.conn();
                insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
            }
            set_status_raw(db, job, "COMPLETED");

            let result = transit_job_state(
                &db.conn(),
                job,
                JobStatus::Dispatched,
                JobStatus::Completed,
                "stale_recomplete",
            );
            assert!(
                result.is_err(),
                "re-completing a COMPLETED job via a stale expected_from must be rejected"
            );
            assert_eq!(status(db, job), "COMPLETED");
        });
    }

    // The caller-supplied `reason` is persisted for observability (this is the
    // gate's audit contribution — every status change is explainable).
    #[test]
    fn gate_persists_reason_into_audit_trail() {
        with_gate_db(|db| {
            let job = "job_reason";
            {
                let conn = db.conn();
                insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
            }
            transit_job_state(
                &db.conn(),
                job,
                JobStatus::Queued,
                JobStatus::Dispatched,
                "unique_reason_marker",
            )
            .unwrap();

            let reason: String = db
                .conn()
                .query_row(
                    "SELECT reason FROM job_transitions \
                     WHERE job_id = ? AND new_status = 'DISPATCHED' \
                     ORDER BY job_event_id DESC LIMIT 1",
                    params![job],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                reason, "unique_reason_marker",
                "the gate must persist the caller's reason for observability"
            );
        });
    }

    // ---- Suite C: FAILED -> COMPLETED narrow guard -----------------------

    // Safety property: a bare FAILED->COMPLETED with no late-evidence
    // reconciliation is REFUSED. This is not a general "reopen any failed job"
    // path; without authoritative late evidence the job stays FAILED.
    #[test]
    fn failed_to_completed_refused_without_late_evidence() {
        with_gate_db(|db| {
            let job = "job_no_evidence";
            {
                let conn = db.conn();
                insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
            }
            dispatch_raw(db, job);
            {
                let conn = db.conn();
                mark_job_failed_conn_sync(&conn, job, "PREMATURE_FAIL").unwrap();
            }
            assert_eq!(status(db, job), "FAILED");

            let result = transit_job_state(
                &db.conn(),
                job,
                JobStatus::Failed,
                JobStatus::Completed,
                "reopen",
            );
            assert!(
                result.is_err(),
                "FAILED->COMPLETED must be refused without late-evidence reconciliation"
            );
            assert_eq!(
                status(db, job),
                "FAILED",
                "a refused reopen must leave the job FAILED"
            );
        });
    }

    // Narrow window: when authoritative late-completion evidence for the job
    // exists (shape modeled on `late_health_completion_stuck_allows_terminal`),
    // the guarded FAILED->COMPLETED reconciliation is permitted and audited.
    #[test]
    fn failed_to_completed_allowed_with_authoritative_late_evidence() {
        with_gate_db(|db| {
            let job = "job_late_evidence";
            {
                let conn = db.conn();
                insert_job_sync(&conn, job, "a1", None, "x\n").unwrap();
            }
            dispatch_raw(db, job);
            {
                let conn = db.conn();
                mark_job_failed_conn_sync(&conn, job, "PREMATURE_FAIL").unwrap();
            }
            assert_eq!(status(db, job), "FAILED");

            // Authoritative late-completion evidence arrives after the fail,
            // referencing this job (same signal vocabulary the existing
            // health-completion accept-gate keys on).
            let evidence = json!({
                "reason": "HEALTH_CHECK_STUCK",
                "signal_kinds": ["health:completion"],
                "job_id": job,
            })
            .to_string();
            {
                let conn = db.conn();
                insert_event_sync(&conn, "a1", None, "state_change", &evidence).unwrap();
            }

            transit_job_state(
                &db.conn(),
                job,
                JobStatus::Failed,
                JobStatus::Completed,
                "late_evidence_reconciliation",
            )
            .unwrap_or_else(|e| {
                panic!("guarded FAILED->COMPLETED with late evidence must be accepted, got {e:?}")
            });

            assert_eq!(status(db, job), "COMPLETED");
            assert!(
                transition_exists(db, job, Some("FAILED"), Some("COMPLETED")),
                "late-evidence reconciliation must be audited as FAILED->COMPLETED"
            );
        });
    }

    // ---- Suite D: gap-patch #1 — queuing-reason observability -------------

    // The Gen-2 incident: a new job silently queued behind a stale in-flight job
    // for 5+ minutes with zero daemon-side signal. When dispatch is deferred
    // because the target agent is occupied by an in-flight (non-terminal) job,
    // that deferral must be OBSERVABLE and must name the occupant for diagnosis.
    #[test]
    fn dispatch_deferral_emits_observable_signal_naming_the_occupant() {
        with_gate_db(|db| {
            // a1 occupied by an in-flight, non-terminal job.
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_occupant", "a1", None, "long\n").unwrap();
            }
            dispatch_raw(db, "job_occupant");
            set_agent_state_raw(db, "a1", "BUSY");
            // a new job queues behind it.
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_waiting", "a1", None, "next\n").unwrap();
            }

            // The dispatcher polls but cannot dispatch: the agent is occupied.
            let claimed = claim_next_job_sync(db, "a1").unwrap();
            assert!(
                claimed.is_none(),
                "must not dispatch a new job while the agent is occupied"
            );

            // gap-patch #1: the deferral is observable, not a silent stall...
            let signals = dispatch_deferred_events(db, "a1");
            assert!(
                !signals.is_empty(),
                "deferring a queued job behind an in-flight job must emit an observable \
                 dispatch_deferred signal (queryable via ah events), not just a tracing line"
            );
            // ...and it names the in-flight occupant so an operator can find it
            // without hand-inspecting raw job-state history.
            assert!(
                signals.iter().any(|p| p.contains("job_occupant")),
                "the deferral signal must name the in-flight occupant job, got {signals:?}"
            );
        });
    }

    // The signal is specific to genuine deferrals: a free agent dispatching its
    // queued job normally emits no deferral noise.
    #[test]
    fn no_deferral_signal_when_agent_dispatches_normally() {
        with_gate_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_ok", "a1", None, "go\n").unwrap();
            }
            let claimed = claim_next_job_sync(db, "a1").unwrap();
            assert!(
                claimed.is_some(),
                "an IDLE agent with a queued job should dispatch normally"
            );
            assert!(
                dispatch_deferred_events(db, "a1").is_empty(),
                "no deferral signal should be emitted when nothing is actually deferred"
            );
        });
    }

    // ---- Suite E: gap-patch #2 — cancel driver + timeout takeover ---------

    // A cancel against a DISPATCHED job whose agent is hung (never acks) cannot
    // be resolved by the agent-cooperation path; past a bounded timeout the
    // orchestrator forcibly drives DISPATCHED->CANCELLED through the gate.
    #[test]
    fn cancel_timeout_takeover_forces_cancel_of_hung_agent_job() {
        with_gate_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_hang", "a1", None, "work\n").unwrap();
            }
            dispatch_raw(db, "job_hang");
            request_dispatched_job_cancel_sync(db, "job_hang").unwrap();
            // Agent is hung: BUSY and never acknowledges the cancel.
            set_agent_state_raw(db, "a1", "BUSY");

            // The existing agent-cooperation path cannot resolve it (agent != IDLE):
            // this is the "cancel hangs forever" gap.
            let idle_changes =
                mark_dispatched_job_cancelled_if_agent_idle_sync(db, "job_hang").unwrap();
            assert_eq!(
                idle_changes, 0,
                "the idle-gated cancel must NOT fire for a hung (BUSY) agent"
            );
            assert_eq!(
                status(db, "job_hang"),
                "DISPATCHED",
                "without a takeover the job stays stuck pending agent cooperation"
            );

            // Takeover: past the bounded timeout the orchestrator forces the cancel
            // regardless of agent state.
            let requested_at = cancel_requested_at(db, "job_hang");
            let fired = force_cancel_pending_dispatched_job_conn_sync(
                &db.conn(),
                "job_hang",
                requested_at + 3600, // now: an hour after the cancel was requested
                300,                 // timeout: 5 minutes
            )
            .unwrap();

            assert!(
                fired,
                "takeover must force the cancel once the timeout has elapsed"
            );
            assert_eq!(
                status(db, "job_hang"),
                "CANCELLED",
                "takeover must drive the job to CANCELLED despite the hung agent"
            );
            assert!(
                transition_exists(db, "job_hang", Some("DISPATCHED"), Some("CANCELLED")),
                "the forced takeover must go through the gate (audited DISPATCHED->CANCELLED)"
            );
        });
    }

    // The takeover is BOUNDED, not immediate: before the timeout elapses it must
    // not fire, giving a cooperating agent the chance to ack cleanly first.
    #[test]
    fn cancel_timeout_takeover_waits_for_the_bounded_timeout() {
        with_gate_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_fresh", "a1", None, "work\n").unwrap();
            }
            dispatch_raw(db, "job_fresh");
            request_dispatched_job_cancel_sync(db, "job_fresh").unwrap();
            set_agent_state_raw(db, "a1", "BUSY");

            let requested_at = cancel_requested_at(db, "job_fresh");
            // now == request time -> 0s pending < 300s timeout -> must NOT fire yet.
            let fired = force_cancel_pending_dispatched_job_conn_sync(
                &db.conn(),
                "job_fresh",
                requested_at,
                300,
            )
            .unwrap();

            assert!(
                !fired,
                "takeover must not fire before the bounded timeout has elapsed"
            );
            assert_eq!(
                status(db, "job_fresh"),
                "DISPATCHED",
                "a fresh cancel must remain pending (fast path: agent may still ack)"
            );
        });
    }
}

#[cfg(test)]
mod additional_requeue_tests {
    use super::*;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};
    use rusqlite::params;

    fn with_requeue_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
        }
        test(&db)
    }

    #[test]
    fn test_requeue_dispatched_to_queued() {
        with_requeue_db(|db| {
            let conn = db.conn();
            insert_job_sync(&conn, "job1", "a1", None, "script").unwrap();

            // Set status to DISPATCHED
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = 1000, dispatched_at_seq_id = 2000, completed_at = 3000 WHERE id = ?",
                params!["job1"],
            ).unwrap();

            // Requeue it
            requeue_job_state_conn_sync(
                &conn,
                "job1",
                JobStatus::Dispatched,
                Some("io_error"),
                "requeue_test",
            )
            .unwrap();

            // Verify
            let job = query_job_sync(&conn, "job1").unwrap().unwrap();
            assert_eq!(job.status, "QUEUED");
            assert_eq!(job.dispatched_at, None);
            assert_eq!(job.dispatched_at_seq_id, None);
            assert_eq!(job.completed_at, None);
            assert_eq!(job.error_reason.as_deref(), Some("io_error"));

            // Verify audit row
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM job_transitions WHERE job_id = 'job1' AND old_status = 'DISPATCHED' AND new_status = 'QUEUED'",
                [],
                |r| r.get(0),
            ).unwrap();
            assert_eq!(count, 1);
            let requeue_observations: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM provider_status_observations WHERE agent_id='a1' AND turn_id='job1' AND observation_id LIKE 'requeue:%' AND json_extract(observation_json, '$.kind.state')='queued'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(requeue_observations, 1);
        });
    }

    #[test]
    fn test_requeue_failed_to_queued() {
        with_requeue_db(|db| {
            let conn = db.conn();
            insert_job_sync(&conn, "job1", "a1", None, "script").unwrap();

            conn.execute(
                "UPDATE jobs SET status = 'FAILED' WHERE id = ?",
                params!["job1"],
            )
            .unwrap();

            requeue_job_state_conn_sync(
                &conn,
                "job1",
                JobStatus::Failed,
                Some("retry"),
                "requeue_test",
            )
            .unwrap();

            let job = query_job_sync(&conn, "job1").unwrap().unwrap();
            assert_eq!(job.status, "QUEUED");
            assert_eq!(job.error_reason.as_deref(), Some("retry"));
        });
    }

    #[test]
    fn test_requeue_illegal_transition() {
        with_requeue_db(|db| {
            let conn = db.conn();
            insert_job_sync(&conn, "job1", "a1", None, "script").unwrap();

            // QUEUED -> QUEUED is illegal
            let res =
                requeue_job_state_conn_sync(&conn, "job1", JobStatus::Queued, None, "requeue_test");
            assert!(res.is_err());
        });
    }
}
