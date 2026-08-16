use crate::db::Db;
use crate::db::common::{is_unique_constraint_error, map_db_error, spawn_db};
use crate::db::schema::Job;
use crate::db::state_machine::{STATE_IDLE, transit_agent_state_conn_sync};
use crate::error::CcbdError;
use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde_json::{Map, Value};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) const RECOVERY_REQUEUED_ERROR_REASON_PREFIX: &str = "RECOVERY_REQUEUED:";
pub(crate) const RECOVERY_REQUEUED_MAX_STALE_PANE_RETRIES: u32 = 1;

#[derive(Debug, Clone)]
pub struct DispatchedJob {
    pub job: Job,
    pub seq_id: i64,
    pub job_payload: Value,
}

pub(crate) fn recovery_requeued_error_reason(attempt: u32) -> String {
    format!("{RECOVERY_REQUEUED_ERROR_REASON_PREFIX}{attempt}")
}

pub(crate) fn recovery_requeued_attempt(error_reason: Option<&str>) -> Option<u32> {
    error_reason?
        .strip_prefix(RECOVERY_REQUEUED_ERROR_REASON_PREFIX)?
        .parse()
        .ok()
}

#[cfg(test)]
static FAIL_NEXT_JOB_TRANSITION_FOR_TEST: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn fail_next_job_transition_for_test() {
    FAIL_NEXT_JOB_TRANSITION_FOR_TEST.store(true, Ordering::SeqCst);
}

fn row_to_job(row: &Row<'_>) -> rusqlite::Result<Job> {
    Ok(Job {
        id: row.get(0)?,
        agent_id: row.get(1)?,
        request_id: row.get(2)?,
        prompt_text: row.get(3)?,
        reply_text: row.get(4)?,
        status: row.get(5)?,
        error_reason: row.get(6)?,
        created_at: row.get(7)?,
        dispatched_at: row.get(8)?,
        dispatched_at_seq_id: row.get(9)?,
        completed_at: row.get(10)?,
        cancel_requested: row.get::<_, i64>(11)? != 0,
        requires_physical_evidence: row.get::<_, i64>(12)? != 0,
        requires_test_evidence: row.get::<_, i64>(13)? != 0,
        governance_binding_json: row.get(14)?,
    })
}

pub(crate) fn record_job_transition_conn_sync(
    conn: &Connection,
    job_id: &str,
    old_status: Option<&str>,
    new_status: Option<&str>,
    kind: &str,
    changed_fields: &[&str],
    reason: &str,
) -> Result<(), CcbdError> {
    #[cfg(test)]
    if FAIL_NEXT_JOB_TRANSITION_FOR_TEST.swap(false, Ordering::SeqCst) {
        return Err(CcbdError::DbConstraintViolation(
            "forced job transition failure for test".to_string(),
        ));
    }

    let job = query_job_sync(conn, job_id)?.ok_or_else(|| {
        CcbdError::DbConstraintViolation(format!("job {job_id} missing after mutation"))
    })?;
    let changed_json = serde_json::to_string(changed_fields).map_err(|err| {
        CcbdError::DbConstraintViolation(format!("serialize job transition fields: {err}"))
    })?;
    conn.execute(
        "INSERT INTO job_transitions (
            job_id, agent_id, request_id, kind, old_status, new_status,
            changed_json, reason, job_created_at, dispatched_at, completed_at,
            cancel_requested, error_reason
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            job.id,
            job.agent_id,
            job.request_id,
            kind,
            old_status,
            new_status,
            changed_json,
            reason,
            job.created_at,
            job.dispatched_at,
            job.completed_at,
            i64::from(job.cancel_requested),
            job.error_reason,
        ],
    )
    .map_err(|err| map_db_error("record job transition", err))?;
    Ok(())
}

fn notify_job_changed(job_id: &str) {
    crate::orchestrator::pubsub::notify_job_update(job_id);
}

pub(crate) fn notify_runtime_job_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::JobChanged,
    );
}

pub(crate) fn insert_job_sync(
    conn: &Connection,
    id: &str,
    agent_id: &str,
    request_id: Option<&str>,
    prompt_text: &str,
) -> Result<String, CcbdError> {
    insert_job_with_binding_sync(conn, id, agent_id, request_id, prompt_text, None)
}

pub(crate) fn insert_job_with_binding_sync(
    conn: &Connection,
    id: &str,
    agent_id: &str,
    request_id: Option<&str>,
    prompt_text: &str,
    governance_binding_json: Option<&str>,
) -> Result<String, CcbdError> {
    let result = conn.execute(
        "INSERT INTO jobs (id, agent_id, request_id, prompt_text, status, governance_binding_json) VALUES (?, ?, ?, ?, 'QUEUED', ?)",
        params![id, agent_id, request_id, prompt_text, governance_binding_json],
    );

    match result {
        Ok(_) => {
            record_job_transition_conn_sync(
                conn,
                id,
                None,
                Some("QUEUED"),
                "job_transition",
                &["status", "created_at"],
                "submit",
            )?;
            crate::runtime_observation::store::append_for_current_lifecycle_sync(
                conn,
                &format!("queue:{id}"),
                agent_id,
                Some(id),
                crate::runtime_observation::EvidenceSource::ControlPlane,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Queued,
                ),
                crate::runtime_observation::store::now_epoch_millis(),
            )?;
            Ok(id.to_string())
        }
        Err(err) if is_unique_constraint_error(&err) && request_id.is_some() => {
            let existing = query_job_by_request_id_sync(conn, agent_id, request_id.unwrap())?
                .ok_or_else(|| map_db_error("query duplicate job by request_id", err))?;
            if existing.governance_binding_json.as_deref() != governance_binding_json {
                return Err(CcbdError::DbConstraintViolation(format!(
                    "request_id {} is already bound to different governance identity or scope",
                    request_id.unwrap()
                )));
            }
            Ok(existing.id)
        }
        Err(err) => Err(map_db_error("insert job", err)),
    }
}

pub(crate) fn insert_recovered_queued_job_sync(
    conn: &Connection,
    id: &str,
    agent_id: &str,
    request_id: Option<&str>,
    prompt_text: &str,
    cancel_requested: bool,
    requires_physical_evidence: bool,
    requires_test_evidence: bool,
    error_reason: &str,
) -> Result<String, CcbdError> {
    let result = conn.execute(
        "INSERT INTO jobs (
             id, agent_id, request_id, prompt_text, status, error_reason,
             cancel_requested, requires_physical_evidence, requires_test_evidence
         )
         VALUES (?, ?, ?, ?, 'QUEUED', ?, ?, ?, ?)",
        params![
            id,
            agent_id,
            request_id,
            prompt_text,
            error_reason,
            i64::from(cancel_requested),
            i64::from(requires_physical_evidence),
            i64::from(requires_test_evidence),
        ],
    );

    match result {
        Ok(_) => {
            record_job_transition_conn_sync(
                conn,
                id,
                None,
                Some("QUEUED"),
                "job_transition",
                &["status", "created_at", "error_reason"],
                "recovery_recovered_create",
            )?;
            crate::runtime_observation::store::append_for_current_lifecycle_sync(
                conn,
                &format!("recovered-queue:{id}"),
                agent_id,
                Some(id),
                crate::runtime_observation::EvidenceSource::ControlPlane,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Queued,
                ),
                crate::runtime_observation::store::now_epoch_millis(),
            )?;
            Ok(id.to_string())
        }
        Err(err) if is_unique_constraint_error(&err) && request_id.is_some() => {
            let existing = query_job_by_request_id_sync(conn, agent_id, request_id.unwrap())?
                .ok_or_else(|| map_db_error("query duplicate recovered job by request_id", err))?;
            Ok(existing.id)
        }
        Err(err) => Err(map_db_error("insert recovered queued job", err)),
    }
}

pub(crate) fn query_job_sync(conn: &Connection, job_id: &str) -> Result<Option<Job>, CcbdError> {
    conn.query_row(
        "SELECT id, agent_id, request_id, prompt_text, reply_text, status, error_reason, created_at, dispatched_at, dispatched_at_seq_id, completed_at, cancel_requested, requires_physical_evidence, requires_test_evidence, governance_binding_json FROM jobs WHERE id = ?",
        params![job_id],
        row_to_job,
    )
    .optional()
    .map_err(|err| map_db_error("query job", err))
}

pub(crate) fn query_job_by_request_id_sync(
    conn: &Connection,
    agent_id: &str,
    request_id: &str,
) -> Result<Option<Job>, CcbdError> {
    conn.query_row(
        "SELECT id, agent_id, request_id, prompt_text, reply_text, status, error_reason, created_at, dispatched_at, dispatched_at_seq_id, completed_at, cancel_requested, requires_physical_evidence, requires_test_evidence, governance_binding_json FROM jobs WHERE agent_id = ? AND request_id = ? LIMIT 1",
        params![agent_id, request_id],
        row_to_job,
    )
    .optional()
    .map_err(|err| map_db_error("query job by request_id", err))
}

pub(crate) fn has_queued_job_sync(conn: &Connection, agent_id: &str) -> Result<bool, CcbdError> {
    conn.query_row(
        "SELECT 1 FROM jobs WHERE agent_id = ? AND status = 'QUEUED' ORDER BY created_at ASC, rowid ASC LIMIT 1",
        params![agent_id],
        |_| Ok(true),
    )
    .optional()
    .map(|value| value.unwrap_or(false))
    .map_err(|err| map_db_error("query queued job exists", err))
}

pub(crate) fn query_agent_ids_with_queued_jobs_sync(
    conn: &Connection,
) -> Result<Vec<String>, CcbdError> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT agent_id
             FROM jobs
             WHERE status = 'QUEUED'
             ORDER BY created_at ASC, rowid ASC",
        )
        .map_err(|err| map_db_error("prepare queued job agent ids", err))?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|err| map_db_error("query queued job agent ids", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect queued job agent ids", err))?;
    Ok(ids)
}

pub(crate) fn claim_next_job_sync(db: &Db, agent_id: &str) -> Result<Option<Job>, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin claim next job", err))?;

    // Cancel any queued jobs for this agent that have cancel_requested = 1
    let cancelled_ids = {
        let mut stmt = tx
            .prepare(
                "SELECT id FROM jobs WHERE agent_id = ? AND status = 'QUEUED' AND cancel_requested = 1",
            )
            .map_err(|err| map_db_error("prepare query cancelled queued jobs", err))?;
        stmt.query_map(params![agent_id], |row| row.get::<_, String>(0))
            .map_err(|err| map_db_error("query cancelled queued jobs", err))?
            .collect::<Result<Vec<String>, _>>()
            .map_err(|err| map_db_error("collect cancelled queued jobs", err))?
    };

    for job_id in cancelled_ids {
        crate::db::job_state::transit_job_state(
            &tx,
            &job_id,
            crate::db::job_state::JobStatus::Queued,
            crate::db::job_state::JobStatus::Cancelled,
            "cancel_queued",
        )?;
    }

    let agent_state = tx
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent state before claim next job", err))?
        .unwrap_or_default();
    if agent_state != STATE_IDLE {
        let occupant_job_id: Option<String> = tx
            .query_row(
                "SELECT id FROM jobs WHERE agent_id = ? AND status = 'DISPATCHED' LIMIT 1",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query occupant job for deferral signal", err))?;

        if let Some(occ_id) = occupant_job_id {
            let has_queued = tx
                .query_row(
                    "SELECT COUNT(*) FROM jobs WHERE agent_id = ? AND status = 'QUEUED'",
                    params![agent_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|err| map_db_error("query queued jobs count for deferral signal", err))?
                > 0;

            if has_queued {
                let payload = serde_json::json!({
                    "occupant_job_id": occ_id,
                })
                .to_string();
                tx.execute(
                    "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'dispatch_deferred', ?)",
                    params![agent_id, payload],
                )
                .map_err(|err| map_db_error("insert dispatch_deferred event", err))?;
            }
        }

        tracing::info!(
            agent_id,
            state = %agent_state,
            reason = "agent is not IDLE",
            impact = "queued jobs remain queued and dispatch is blocked",
            "claim next job skipped"
        );
        tx.commit()
            .map_err(|err| map_db_error("commit skipped claim next job", err))?;
        return Ok(None);
    }

    let candidate_id = tx
        .query_row(
            "SELECT id FROM jobs WHERE agent_id = ? AND status = 'QUEUED' AND cancel_requested = 0 ORDER BY created_at ASC, rowid ASC LIMIT 1",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query queued job", err))?;

    let Some(job_id) = candidate_id else {
        tx.commit()
            .map_err(|err| map_db_error("commit empty claim next job", err))?;
        return Ok(None);
    };

    crate::db::job_state::transit_job_state(
        &tx,
        &job_id,
        crate::db::job_state::JobStatus::Queued,
        crate::db::job_state::JobStatus::Dispatched,
        "claim_next_job",
    )?;

    let job = Some(
        tx.query_row(
            "SELECT id, agent_id, request_id, prompt_text, reply_text, status, error_reason, created_at, dispatched_at, dispatched_at_seq_id, completed_at, cancel_requested, requires_physical_evidence, requires_test_evidence, governance_binding_json FROM jobs WHERE id = ?",
            params![job_id],
            row_to_job,
        )
        .map_err(|err| map_db_error("query claimed job", err))?
    );

    tx.commit()
        .map_err(|err| map_db_error("commit claim next job", err))?;
    Ok(job)
}

pub fn dispatch_job_to_agent_sync(
    conn: &mut Connection,
    agent_id: &str,
    expected_from_state: &[&str],
    new_state: &str,
    event_kind: &str,
    event_payload: &Value,
) -> Result<Option<DispatchedJob>, CcbdError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin dispatch job to agent", err))?;

    let current_state = tx
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent state before dispatch", err))?;
    let Some(current_state) = current_state else {
        return Err(CcbdError::AgentNotFound(agent_id.to_string()));
    };
    if !expected_from_state.is_empty() && !expected_from_state.contains(&current_state.as_str()) {
        tracing::info!(
            agent_id,
            state = %current_state,
            expected = ?expected_from_state,
            reason = "agent state is not dispatchable",
            impact = "queued jobs remain queued and dispatch is blocked",
            "dispatch job to agent rejected"
        );
        return Err(CcbdError::AgentWrongState { current_state });
    }

    let candidate_id = tx
        .query_row(
            "SELECT id FROM jobs WHERE agent_id = ? AND status = 'QUEUED' ORDER BY created_at ASC, rowid ASC LIMIT 1",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query queued job for dispatch", err))?;

    let Some(job_id) = candidate_id else {
        tx.commit()
            .map_err(|err| map_db_error("commit empty dispatch job to agent", err))?;
        return Ok(None);
    };
    crate::db::job_state::transit_job_state(
        &tx,
        &job_id,
        crate::db::job_state::JobStatus::Queued,
        crate::db::job_state::JobStatus::Dispatched,
        "dispatch_job_to_agent",
    )?;

    transit_agent_state_conn_sync(
        &tx,
        agent_id,
        expected_from_state,
        new_state,
        Some("dispatched"),
    )?;

    let job = tx
        .query_row(
            "SELECT id, agent_id, request_id, prompt_text, reply_text, status, error_reason, created_at, dispatched_at, dispatched_at_seq_id, completed_at, cancel_requested, requires_physical_evidence, requires_test_evidence, governance_binding_json FROM jobs WHERE id = ?",
            params![job_id],
            row_to_job,
        )
        .map_err(|err| map_db_error("query dispatched job", err))?;
    let job_payload = dispatch_event_payload(event_payload, &job);
    tx.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, ?, ?)",
        params![agent_id, event_kind, job_payload.to_string()],
    )
    .map_err(|err| map_db_error("insert dispatch event", err))?;
    let seq_id = tx.last_insert_rowid();

    tx.execute(
        "UPDATE jobs SET dispatched_at_seq_id = ? WHERE id = ? AND status = 'DISPATCHED'",
        params![seq_id, job.id],
    )
    .map_err(|err| map_db_error("update dispatched seq_id during dispatch", err))?;

    let job = tx
        .query_row(
            "SELECT id, agent_id, request_id, prompt_text, reply_text, status, error_reason, created_at, dispatched_at, dispatched_at_seq_id, completed_at, cancel_requested, requires_physical_evidence, requires_test_evidence, governance_binding_json FROM jobs WHERE id = ?",
            params![job.id],
            row_to_job,
        )
        .map_err(|err| map_db_error("query dispatched job with seq_id", err))?;

    crate::runtime_observation::store::append_for_current_lifecycle_sync(
        &tx,
        &format!("dispatch:{}:{seq_id}", job.id),
        agent_id,
        Some(&job.id),
        crate::runtime_observation::EvidenceSource::ControlPlane,
        crate::runtime_observation::ProviderObservationKind::Turn(
            crate::runtime_observation::ProviderTurnState::Delivering,
        ),
        crate::runtime_observation::store::now_epoch_millis(),
    )?;

    tx.commit()
        .map_err(|err| map_db_error("commit dispatch job to agent", err))?;
    Ok(Some(DispatchedJob {
        job,
        seq_id,
        job_payload,
    }))
}

pub(crate) fn mark_dispatched_job_delivered_sync(
    conn: &mut Connection,
    agent_id: &str,
    job_id: &str,
    dispatch_seq_id: i64,
) -> Result<bool, CcbdError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin mark dispatched job delivered", err))?;
    let is_current = tx
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE id = ?1 AND agent_id = ?2 AND status = 'DISPATCHED' AND dispatched_at_seq_id = ?3",
            params![job_id, agent_id, dispatch_seq_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|err| map_db_error("query dispatched job before delivery observation", err))?
        == 1;
    if !is_current {
        tx.commit()
            .map_err(|err| map_db_error("commit skipped job delivery observation", err))?;
        return Ok(false);
    }

    let observation_id = format!("delivered:{job_id}:{dispatch_seq_id}");
    let already_observed = tx
        .query_row(
            "SELECT COUNT(*) FROM provider_status_observations WHERE observation_id=?1",
            [&observation_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|err| map_db_error("query existing job delivery observation", err))?
        > 0;
    let inserted = !already_observed
        && crate::runtime_observation::store::append_for_current_lifecycle_sync(
            &tx,
            &observation_id,
            agent_id,
            Some(job_id),
            crate::runtime_observation::EvidenceSource::ControlPlane,
            crate::runtime_observation::ProviderObservationKind::Turn(
                crate::runtime_observation::ProviderTurnState::Delivered,
            ),
            crate::runtime_observation::store::now_epoch_millis(),
        )?;
    if inserted {
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?1, NULL, 'dispatch_delivery', ?2)",
            params![
                agent_id,
                serde_json::json!({
                    "job_id": job_id,
                    "dispatch_seq_id": dispatch_seq_id,
                    "transport": "tmux",
                    "status": "DELIVERED",
                })
                .to_string()
            ],
        )
        .map_err(|err| map_db_error("insert dispatch delivery event", err))?;
    }
    tx.commit()
        .map_err(|err| map_db_error("commit dispatched job delivery", err))?;
    Ok(inserted)
}

fn dispatch_event_payload(base: &Value, job: &Job) -> Value {
    let mut payload = match base {
        Value::Object(map) => map.clone(),
        _ => Map::new(),
    };
    payload
        .entry("cmd".to_string())
        .or_insert_with(|| Value::String(job.prompt_text.clone()));
    payload
        .entry("job_id".to_string())
        .or_insert_with(|| Value::String(job.id.clone()));
    payload
        .entry("status".to_string())
        .or_insert_with(|| Value::String("SENT".to_string()));
    let env = payload
        .entry("env".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(env) = env {
        env.entry("CCB_JOB_ID".to_string())
            .or_insert_with(|| Value::String(job.id.clone()));
    }
    Value::Object(payload)
}

pub(crate) fn mark_job_completed_sync(
    db: &Db,
    job_id: &str,
    reply_text: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark job completed", err))?;
    let changes = mark_job_completed_conn_sync(&tx, job_id, reply_text)?;
    tx.commit()
        .map_err(|err| map_db_error("commit mark job completed", err))?;
    Ok(changes)
}

pub(crate) fn mark_job_completed_conn_sync(
    conn: &Connection,
    job_id: &str,
    reply_text: &str,
) -> Result<usize, CcbdError> {
    let changes = conn
        .execute(
            "UPDATE jobs SET reply_text = ? WHERE id = ? AND status = 'DISPATCHED'",
            params![reply_text, job_id],
        )
        .map_err(|err| map_db_error("update job reply_text", err))?;
    if changes > 0 {
        crate::db::job_state::transit_job_state(
            conn,
            job_id,
            crate::db::job_state::JobStatus::Dispatched,
            crate::db::job_state::JobStatus::Completed,
            "completed",
        )?;
    }
    Ok(changes)
}

pub(crate) fn mark_queued_job_cancelled_sync(db: &Db, job_id: &str) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark queued job cancelled", err))?;
    let changes = mark_queued_job_cancelled_conn_sync(&tx, job_id)?;
    tx.commit()
        .map_err(|err| map_db_error("commit mark queued job cancelled", err))?;
    Ok(changes)
}

pub(crate) fn mark_queued_job_cancelled_conn_sync(
    conn: &Connection,
    job_id: &str,
) -> Result<usize, CcbdError> {
    let job = query_job_sync(conn, job_id)?;
    let Some(job) = job else {
        return Ok(0);
    };
    if job.status == "QUEUED" {
        crate::db::job_state::transit_job_state(
            conn,
            job_id,
            crate::db::job_state::JobStatus::Queued,
            crate::db::job_state::JobStatus::Cancelled,
            "cancel_queued",
        )?;
        Ok(1)
    } else {
        Ok(0)
    }
}

pub(crate) fn request_dispatched_job_cancel_sync(
    db: &Db,
    job_id: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin request dispatched job cancel", err))?;
    let changes = tx.execute(
        "UPDATE jobs SET cancel_requested = 1 WHERE id = ? AND status = 'DISPATCHED' AND cancel_requested = 0",
        params![job_id],
    )
    .map_err(|err| map_db_error("request dispatched job cancel", err))?;
    if changes > 0 {
        record_job_transition_conn_sync(
            &tx,
            job_id,
            Some("DISPATCHED"),
            Some("DISPATCHED"),
            "job_updated",
            &["cancel_requested"],
            "cancel_requested",
        )?;
    }
    tx.commit()
        .map_err(|err| map_db_error("commit request dispatched job cancel", err))?;
    Ok(changes)
}

pub(crate) fn set_job_evidence_requirements_sync(
    conn: &Connection,
    job_id: &str,
    requires_physical_evidence: bool,
    requires_test_evidence: bool,
) -> Result<usize, CcbdError> {
    conn.execute(
        "UPDATE jobs SET requires_physical_evidence = ?, requires_test_evidence = ? WHERE id = ?",
        params![
            i64::from(requires_physical_evidence),
            i64::from(requires_test_evidence),
            job_id
        ],
    )
    .map_err(|err| map_db_error("set job evidence requirements", err))
}

pub(crate) fn mark_dispatched_job_cancelled_if_agent_idle_sync(
    db: &Db,
    job_id: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin mark dispatched job cancelled if agent idle", err))?;
    let job = query_job_sync(&tx, job_id)?;
    let Some(job) = job else {
        tx.commit().map_err(|err| {
            map_db_error("commit mark dispatched job cancelled if agent idle", err)
        })?;
        return Ok(0);
    };

    if job.status == "DISPATCHED" && job.cancel_requested {
        let agent_state: Option<String> = tx
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![job.agent_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| map_db_error("query agent state for cancel_settled check", err))?;

        let agent_idle = agent_state.is_some_and(|st| st == "IDLE" || st == "UNKNOWN");
        if agent_idle {
            crate::db::job_state::transit_job_state(
                &tx,
                job_id,
                crate::db::job_state::JobStatus::Dispatched,
                crate::db::job_state::JobStatus::Cancelled,
                "cancel_settled",
            )?;
            tx.commit().map_err(|err| {
                map_db_error("commit mark dispatched job cancelled if agent idle", err)
            })?;
            return Ok(1);
        }
    }

    tx.commit()
        .map_err(|err| map_db_error("commit mark dispatched job cancelled if agent idle", err))?;
    Ok(0)
}

pub(crate) fn mark_job_cancelled_conn_sync(
    conn: &Connection,
    job_id: &str,
    reply_text: &str,
) -> Result<usize, CcbdError> {
    let changes = conn
        .execute(
            "UPDATE jobs SET reply_text = ? WHERE id = ? AND status = 'DISPATCHED'",
            params![reply_text, job_id],
        )
        .map_err(|err| map_db_error("update job reply_text for cancel", err))?;
    if changes > 0 {
        crate::db::job_state::transit_job_state(
            conn,
            job_id,
            crate::db::job_state::JobStatus::Dispatched,
            crate::db::job_state::JobStatus::Cancelled,
            "cancelled",
        )?;
    }
    Ok(changes)
}

pub(crate) fn mark_job_failed_sync(
    db: &Db,
    job_id: &str,
    error_reason: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark job failed", err))?;
    let changes = mark_job_failed_conn_sync(&tx, job_id, error_reason)?;
    tx.commit()
        .map_err(|err| map_db_error("commit mark job failed", err))?;
    Ok(changes)
}

pub(crate) fn mark_job_failed_conn_sync(
    conn: &Connection,
    job_id: &str,
    error_reason: &str,
) -> Result<usize, CcbdError> {
    let old_status_str = conn
        .query_row(
            "SELECT status FROM jobs WHERE id = ? AND status IN ('QUEUED', 'DISPATCHED')",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query job status before fail", err))?;

    let Some(old_status_str) = old_status_str else {
        return Ok(0);
    };

    let expected_from =
        crate::db::job_state::JobStatus::from_db_str(&old_status_str).ok_or_else(|| {
            CcbdError::DbConstraintViolation(format!("unrecognized job status: {old_status_str}"))
        })?;

    let changes = conn
        .execute(
            "UPDATE jobs SET error_reason = ? WHERE id = ? AND status IN ('QUEUED', 'DISPATCHED')",
            params![error_reason, job_id],
        )
        .map_err(|err| map_db_error("update job error_reason for fail", err))?;

    if changes > 0 {
        crate::db::job_state::transit_job_state(
            conn,
            job_id,
            expected_from,
            crate::db::job_state::JobStatus::Failed,
            error_reason,
        )?;
    }
    Ok(changes)
}

pub(crate) fn requeue_recovered_dispatch_io_failure_sync(
    conn: &mut Connection,
    job_id: &str,
    agent_id: &str,
    next_attempt: u32,
    reason: &str,
) -> Result<usize, CcbdError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin requeue recovered dispatch failure", err))?;
    let job = query_job_sync(&tx, job_id)?;
    let Some(job) = job else {
        tx.commit()
            .map_err(|err| map_db_error("commit requeue recovered dispatch failure", err))?;
        return Ok(0);
    };

    let mut changed = 0;
    if job.status == "DISPATCHED"
        && job.agent_id == agent_id
        && job
            .error_reason
            .as_deref()
            .is_some_and(|r| r.starts_with(RECOVERY_REQUEUED_ERROR_REASON_PREFIX))
    {
        let err_reason = recovery_requeued_error_reason(next_attempt);
        crate::db::job_state::requeue_job_state_conn_sync(
            &tx,
            job_id,
            crate::db::job_state::JobStatus::Dispatched,
            Some(&err_reason),
            reason,
        )?;
        changed = 1;
    }
    if changed == 1 {
        tx.execute(
            "UPDATE agents
             SET state = 'IDLE',
                 state_version = state_version + 1,
                 updated_at = unixepoch()
             WHERE id = ?
               AND state IN ('WAITING_FOR_ACK', 'STUCK')",
            params![agent_id],
        )
        .map_err(|err| map_db_error("restore recovered dispatch failure agent idle", err))?;
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload)
             VALUES (?, NULL, 'dispatch_requeued', ?)",
            params![
                agent_id,
                serde_json::json!({
                    "job_id": job_id,
                    "reason": reason,
                    "attempt": next_attempt,
                    "source": "recovered_stale_pane"
                })
                .to_string()
            ],
        )
        .map_err(|err| map_db_error("insert recovered dispatch requeue event", err))?;
    }
    tx.commit()
        .map_err(|err| map_db_error("commit requeue recovered dispatch failure", err))?;
    Ok(changed)
}

pub(crate) fn requeue_dispatched_job_before_send_sync(
    conn: &mut Connection,
    job_id: &str,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin requeue pre-send dispatch", err))?;
    let job = query_job_sync(&tx, job_id)?;
    let Some(job) = job else {
        tx.commit()
            .map_err(|err| map_db_error("commit requeue pre-send dispatch", err))?;
        return Ok(0);
    };

    let mut changed = 0;
    if job.status == "DISPATCHED" && job.agent_id == agent_id {
        crate::db::job_state::requeue_job_state_conn_sync(
            &tx,
            job_id,
            crate::db::job_state::JobStatus::Dispatched,
            None,
            reason,
        )?;
        changed = 1;
    }
    if changed == 1 {
        tx.execute(
            "UPDATE agents
             SET state = 'IDLE',
                 state_version = state_version + 1,
                 updated_at = unixepoch()
             WHERE id = ?
               AND state = 'WAITING_FOR_ACK'",
            params![agent_id],
        )
        .map_err(|err| map_db_error("restore pre-send dispatch agent idle", err))?;
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload)
             VALUES (?, NULL, 'dispatch_requeued', ?)",
            params![
                agent_id,
                serde_json::json!({
                    "job_id": job_id,
                    "reason": reason,
                    "source": "pre_send_readiness_recheck"
                })
                .to_string()
            ],
        )
        .map_err(|err| map_db_error("insert pre-send dispatch requeue event", err))?;
    }
    tx.commit()
        .map_err(|err| map_db_error("commit requeue pre-send dispatch", err))?;
    Ok(changed)
}

pub(crate) fn clear_recovered_dispatch_marker_sync(
    db: &Db,
    job_id: &str,
) -> Result<usize, CcbdError> {
    let conn = db.conn();
    conn.execute(
        "UPDATE jobs
         SET error_reason = NULL
         WHERE id = ?
           AND status IN ('DISPATCHED', 'COMPLETED')
           AND error_reason LIKE 'RECOVERY_REQUEUED:%'",
        params![job_id],
    )
    .map_err(|err| map_db_error("clear recovered dispatch marker", err))
}

#[allow(dead_code)]
pub(crate) fn mark_dispatched_jobs_failed_for_agent_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let (changes, _) = mark_dispatched_jobs_failed_for_agent_collect_sync(db, agent_id, reason)?;
    Ok(changes)
}

pub(crate) fn mark_dispatched_jobs_failed_for_agent_collect_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<(usize, Vec<String>), CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| map_db_error("begin mark dispatched jobs failed for agent", err))?;
    let result = mark_dispatched_jobs_failed_for_agent_conn_collect_sync(&tx, agent_id, reason)?;
    tx.commit()
        .map_err(|err| map_db_error("commit mark dispatched jobs failed for agent", err))?;
    Ok(result)
}

pub(crate) fn mark_dispatched_jobs_failed_for_agent_conn_sync(
    conn: &Connection,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let (changes, _) =
        mark_dispatched_jobs_failed_for_agent_conn_collect_sync(conn, agent_id, reason)?;
    Ok(changes)
}

pub(crate) fn mark_dispatched_jobs_failed_for_agent_conn_collect_sync(
    conn: &Connection,
    agent_id: &str,
    reason: &str,
) -> Result<(usize, Vec<String>), CcbdError> {
    let affected = query_dispatched_job_ids_for_agent_sync(conn, agent_id)?;
    let mut changes = 0;
    for job_id in &affected {
        let update_changes = conn
            .execute(
                "UPDATE jobs SET error_reason = ? WHERE id = ? AND status = 'DISPATCHED'",
                params![reason, job_id],
            )
            .map_err(|err| map_db_error("update job error_reason for agent fail", err))?;
        if update_changes > 0 {
            crate::db::job_state::transit_job_state(
                conn,
                job_id,
                crate::db::job_state::JobStatus::Dispatched,
                crate::db::job_state::JobStatus::Failed,
                reason,
            )?;
            changes += 1;
        }
    }
    Ok((changes, affected.into_iter().take(changes).collect()))
}

pub(crate) fn query_dispatched_job_ids_for_agent_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<Vec<String>, CcbdError> {
    let mut stmt = conn
        .prepare(
            "SELECT id FROM jobs WHERE agent_id = ? AND status = 'DISPATCHED' ORDER BY dispatched_at ASC, id ASC",
        )
        .map_err(|err| map_db_error("prepare dispatched job ids for agent", err))?;
    stmt.query_map(params![agent_id], |row| row.get::<_, String>(0))
        .map_err(|err| map_db_error("query dispatched job ids for agent", err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect dispatched job ids for agent", err))
}

pub(crate) fn query_dispatched_job_for_agent_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<Option<Job>, CcbdError> {
    conn.query_row(
        "SELECT id, agent_id, request_id, prompt_text, reply_text, status, error_reason, created_at, dispatched_at, dispatched_at_seq_id, completed_at, cancel_requested, requires_physical_evidence, requires_test_evidence, governance_binding_json FROM jobs WHERE agent_id = ? AND status = 'DISPATCHED' ORDER BY dispatched_at ASC, id ASC LIMIT 1",
        params![agent_id],
        row_to_job,
    )
    .optional()
    .map_err(|err| map_db_error("query dispatched job for agent", err))
}

pub(crate) fn query_starved_queued_jobs_sync(
    conn: &Connection,
    starvation_threshold_secs: i64,
) -> Result<Vec<Job>, CcbdError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let cutoff = now - starvation_threshold_secs;
    let mut stmt = conn.prepare(
        "SELECT j.id, j.agent_id, j.request_id, j.prompt_text, j.reply_text, j.status, j.error_reason, j.created_at, j.dispatched_at, j.dispatched_at_seq_id, j.completed_at, j.cancel_requested, j.requires_physical_evidence, j.requires_test_evidence, j.governance_binding_json \
         FROM jobs j \
         JOIN agents a ON j.agent_id = a.id \
         WHERE j.status = 'QUEUED' AND a.state != 'IDLE' AND j.created_at < ? \
         ORDER BY j.created_at ASC"
    ).map_err(|err| map_db_error("prepare query starved queued jobs", err))?;

    let rows = stmt
        .query_map(params![cutoff], row_to_job)
        .map_err(|err| map_db_error("query starved queued jobs", err))?;

    let mut jobs = Vec::new();
    for row in rows {
        jobs.push(row.map_err(|err| map_db_error("fetch starved queued job", err))?);
    }
    Ok(jobs)
}

pub(crate) fn update_dispatched_seq_id_sync(
    conn: &Connection,
    job_id: &str,
    seq_id: i64,
) -> Result<usize, CcbdError> {
    conn.execute(
        "UPDATE jobs SET dispatched_at_seq_id = ? WHERE id = ? AND status = 'DISPATCHED'",
        params![seq_id, job_id],
    )
    .map_err(|err| map_db_error("update job dispatched seq_id", err))
}

pub(crate) fn collect_reply_for_dispatched_job_sync(
    conn: &Connection,
    agent_id: &str,
    dispatched_at_seq_id: Option<i64>,
    prompt_text: &str,
) -> Result<String, CcbdError> {
    let Some(dispatched_at_seq_id) = dispatched_at_seq_id else {
        tracing::warn!(
            agent_id,
            "collect_reply: dispatched_at_seq_id is None, returning empty"
        );
        return Ok(String::new());
    };
    let mut stmt = conn
        .prepare(
            "SELECT payload FROM events WHERE agent_id = ? AND event_type = 'output_chunk' AND seq_id > ? ORDER BY seq_id ASC",
        )
        .map_err(|err| map_db_error("prepare collect job reply", err))?;
    let rows = stmt
        .query_map(params![agent_id, dispatched_at_seq_id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|err| map_db_error("query job reply events", err))?;

    let mut parser = vt100::Parser::new(500, 250, 5000);
    let mut chunk_count = 0u32;
    let mut raw_bytes_total = 0usize;
    let mut raw_concat = String::new();
    for payload in rows {
        let payload = payload.map_err(|err| map_db_error("collect job reply payload", err))?;
        let value: Value = serde_json::from_str(&payload).map_err(|err| {
            CcbdError::DbConstraintViolation(format!("parse output_chunk payload: {err}"))
        })?;
        if let Some(text) = value.get("text").and_then(Value::as_str) {
            chunk_count += 1;
            raw_bytes_total += text.len();
            raw_concat.push_str(text);
            parser.process(text.as_bytes());
        }
    }
    let screen_text = parser.screen().contents();
    let reply = distill_reply(&screen_text, prompt_text);

    let reply = if reply.is_empty() && raw_bytes_total > 0 {
        let fallback = distill_reply(&strip_ansi_escapes(&raw_concat), prompt_text);
        tracing::warn!(
            agent_id,
            raw_bytes_total,
            fallback_len = fallback.len(),
            "collect_reply: vt100 screen empty, using strip_ansi fallback"
        );
        fallback
    } else {
        tracing::info!(
            agent_id,
            chunk_count,
            raw_bytes_total,
            screen_text_len = screen_text.len(),
            reply_len = reply.len(),
            "collect_reply complete"
        );
        reply
    };
    Ok(reply)
}

pub(crate) fn distill_reply(raw: &str, prompt_text: &str) -> String {
    let without_ansi = strip_ansi_escapes(raw);
    let current_turn = if let Some(end) = prompt_match_end_byte(&without_ansi, prompt_text) {
        &without_ansi[end..]
    } else {
        without_ansi.as_str()
    };
    let mut lines: Vec<&str> = Vec::new();
    for line in current_turn.lines() {
        let trimmed = line.trim();
        if is_prompt_echo_line(trimmed, prompt_text) {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("thinking...")
            || trimmed.starts_with("Thinking...")
            || trimmed.starts_with("Working(")
            || trimmed.starts_with("• Working(")
            || is_reply_chrome_line(trimmed)
        {
            continue;
        }
        lines.push(line);
    }
    let mut text = lines.join("\n");
    if !prompt_text.is_empty()
        && let Some(start) = text.find(prompt_text)
    {
        let end = start + prompt_text.len();
        text.replace_range(start..end, "");
    }
    text.trim().to_string()
}

pub(crate) fn contains_prompt_text(raw: &str, prompt_text: &str) -> bool {
    if prompt_text.trim().is_empty() {
        return false;
    }
    let without_ansi = strip_ansi_escapes(raw);
    prompt_match_end_byte(&without_ansi, prompt_text).is_some()
}

fn prompt_match_end_byte(raw_without_ansi: &str, prompt_text: &str) -> Option<usize> {
    let (normalized_raw, raw_end_offsets) = normalize_whitespace_with_end_offsets(raw_without_ansi);
    let (normalized_prompt, _) = normalize_whitespace_with_end_offsets(prompt_text);
    if normalized_prompt.is_empty() {
        return None;
    }
    let start = normalized_raw.find(&normalized_prompt)?;
    let end = start + normalized_prompt.len();
    let end_chars = normalized_raw[..end].chars().count();
    raw_end_offsets.get(end_chars.checked_sub(1)?).copied()
}

fn normalize_whitespace_with_end_offsets(input: &str) -> (String, Vec<usize>) {
    let mut normalized = String::new();
    let mut end_offsets = Vec::new();
    let mut in_whitespace = false;
    for (idx, ch) in input.char_indices() {
        let end = idx + ch.len_utf8();
        if ch.is_whitespace() {
            if !normalized.is_empty() && !in_whitespace {
                normalized.push(' ');
                end_offsets.push(end);
                in_whitespace = true;
            }
            continue;
        }
        normalized.push(ch);
        end_offsets.push(end);
        in_whitespace = false;
    }
    if normalized.ends_with(' ') {
        normalized.pop();
        end_offsets.pop();
    }
    (normalized, end_offsets)
}

fn is_prompt_echo_line(trimmed: &str, prompt_text: &str) -> bool {
    if prompt_text.is_empty() || trimmed.is_empty() {
        return false;
    }
    if trimmed == prompt_text {
        return true;
    }
    let Some(without_marker) = strip_prompt_marker(trimmed) else {
        return false;
    };
    let without_marker = without_marker.trim();
    !without_marker.is_empty()
        && (without_marker == prompt_text || prompt_text.starts_with(without_marker))
}

fn strip_prompt_marker(line: &str) -> Option<&str> {
    ["> ", "❯ ", "✦ ", "› ", "* "]
        .into_iter()
        .find_map(|marker| line.strip_prefix(marker))
}

fn is_reply_chrome_line(trimmed: &str) -> bool {
    if trimmed.is_empty() {
        return false;
    }
    if is_box_drawing_line(trimmed) {
        return true;
    }
    if matches!(trimmed, ">" | "❯" | "✦" | "›") {
        return true;
    }
    if trimmed.starts_with("▸ Thought") || trimmed.starts_with("▸ Thinking") {
        return true;
    }
    trimmed.starts_with("? for shortcuts")
        || trimmed.contains("esc to cancel")
        || (trimmed.starts_with("gpt-") && trimmed.contains("·"))
}

fn is_box_drawing_line(trimmed: &str) -> bool {
    trimmed
        .chars()
        .all(|ch| ('\u{2500}'..='\u{257f}').contains(&ch))
}

pub(crate) fn strip_ansi_escapes(input: &str) -> String {
    let mut out = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }

        let Some(&next) = chars.peek() else {
            break;
        };
        match next {
            '[' => {
                let _ = chars.next();
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            ']' => {
                let _ = chars.next();
                while let Some(&c) = chars.peek() {
                    if c == '\u{07}' {
                        let _ = chars.next();
                        break;
                    }
                    if c == '\u{1b}' {
                        let _ = chars.next();
                        if chars.peek() == Some(&'\\') {
                            let _ = chars.next();
                            break;
                        }
                    } else {
                        let _ = chars.next();
                    }
                }
            }
            'P' | '_' | '^' | 'X' => {
                let _ = chars.next();
                while let Some(&c) = chars.peek() {
                    if c == '\u{1b}' {
                        let _ = chars.next();
                        if chars.peek() == Some(&'\\') {
                            let _ = chars.next();
                            break;
                        }
                    } else {
                        let _ = chars.next();
                    }
                }
            }
            _ => {
                let _ = chars.next();
            }
        }
    }
    out
}

#[allow(dead_code)]
pub(crate) fn strip_ansi_csi(input: &str) -> String {
    strip_ansi_escapes(input)
}

pub async fn insert_job(
    db: Db,
    id: String,
    agent_id: String,
    request_id: Option<String>,
    prompt_text: String,
) -> Result<String, CcbdError> {
    insert_job_with_binding(db, id, agent_id, request_id, prompt_text, None).await
}

pub async fn insert_job_with_binding(
    db: Db,
    id: String,
    agent_id: String,
    request_id: Option<String>,
    prompt_text: String,
    governance_binding_json: Option<String>,
) -> Result<String, CcbdError> {
    spawn_db("jobs::insert_job", move || {
        let mut conn = db.conn();
        let tx = conn
            .transaction()
            .map_err(|err| map_db_error("begin insert job", err))?;
        let inserted = insert_job_with_binding_sync(
            &tx,
            &id,
            &agent_id,
            request_id.as_deref(),
            &prompt_text,
            governance_binding_json.as_deref(),
        )?;
        tx.commit()
            .map_err(|err| map_db_error("commit insert job", err))?;
        Ok(inserted)
    })
    .await
    .inspect(|_| notify_runtime_job_changed())
}

pub async fn query_job(db: Db, job_id: String) -> Result<Option<Job>, CcbdError> {
    spawn_db("jobs::query_job", move || {
        let conn = db.conn();
        query_job_sync(&conn, &job_id)
    })
    .await
}

pub async fn query_job_by_request_id(
    db: Db,
    agent_id: String,
    request_id: String,
) -> Result<Option<Job>, CcbdError> {
    spawn_db("jobs::query_job_by_request_id", move || {
        let conn = db.conn();
        query_job_by_request_id_sync(&conn, &agent_id, &request_id)
    })
    .await
}

pub async fn claim_next_job(db: Db, agent_id: String) -> Result<Option<Job>, CcbdError> {
    spawn_db("jobs::claim_next_job", move || {
        claim_next_job_sync(&db, &agent_id)
    })
    .await
    .inspect(|job| {
        if job.is_some() {
            notify_runtime_job_changed();
        }
    })
}

pub async fn has_queued_job(db: Db, agent_id: String) -> Result<bool, CcbdError> {
    spawn_db("jobs::has_queued_job", move || {
        let conn = db.conn();
        has_queued_job_sync(&conn, &agent_id)
    })
    .await
}

pub(crate) async fn query_agent_ids_with_queued_jobs(db: Db) -> Result<Vec<String>, CcbdError> {
    spawn_db("jobs::query_agent_ids_with_queued_jobs", move || {
        let conn = db.conn();
        query_agent_ids_with_queued_jobs_sync(&conn)
    })
    .await
}

pub async fn dispatch_job_to_agent(
    db: Db,
    agent_id: String,
    expected_from_state: Vec<String>,
    new_state: String,
    event_kind: String,
    event_payload: Value,
) -> Result<Option<DispatchedJob>, CcbdError> {
    spawn_db("jobs::dispatch_job_to_agent", move || {
        let mut fresh_conn;
        let mut locked_conn;
        let conn = if let Some(conn) = db.try_conn()? {
            locked_conn = conn;
            &mut *locked_conn
        } else {
            fresh_conn = db.fresh_conn()?;
            &mut fresh_conn
        };
        let from_states = expected_from_state
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        dispatch_job_to_agent_sync(
            conn,
            &agent_id,
            &from_states,
            &new_state,
            &event_kind,
            &event_payload,
        )
    })
    .await
    .inspect(|job| {
        if let Some(dispatched) = job {
            notify_job_changed(&dispatched.job.id);
        }
    })
}

pub(crate) async fn mark_dispatched_job_delivered(
    db: Db,
    agent_id: String,
    job_id: String,
    dispatch_seq_id: i64,
) -> Result<bool, CcbdError> {
    spawn_db("jobs::mark_dispatched_job_delivered", move || {
        let mut conn = db.conn();
        mark_dispatched_job_delivered_sync(&mut conn, &agent_id, &job_id, dispatch_seq_id)
    })
    .await
    .inspect(|_| notify_runtime_job_changed())
}

pub async fn mark_job_completed(
    db: Db,
    job_id: String,
    reply_text: String,
) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db("jobs::mark_job_completed", move || {
        mark_job_completed_sync(&db, &job_id, &reply_text)
    })
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
        }
    })
}

pub async fn mark_queued_job_cancelled(db: Db, job_id: String) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db("jobs::mark_queued_job_cancelled", move || {
        mark_queued_job_cancelled_sync(&db, &job_id)
    })
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
            crate::orchestrator::wake_up();
        }
    })
}

pub async fn request_dispatched_job_cancel(db: Db, job_id: String) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db("jobs::request_dispatched_job_cancel", move || {
        request_dispatched_job_cancel_sync(&db, &job_id)
    })
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
            crate::orchestrator::wake_up();
        }
    })
}

pub async fn mark_dispatched_job_cancelled_if_agent_idle(
    db: Db,
    job_id: String,
) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db(
        "jobs::mark_dispatched_job_cancelled_if_agent_idle",
        move || mark_dispatched_job_cancelled_if_agent_idle_sync(&db, &job_id),
    )
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
            crate::orchestrator::wake_up();
        }
    })
}

pub async fn mark_job_failed(
    db: Db,
    job_id: String,
    error_reason: String,
) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db("jobs::mark_job_failed", move || {
        mark_job_failed_sync(&db, &job_id, &error_reason)
    })
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
        }
    })
}

pub(crate) async fn requeue_recovered_dispatch_io_failure(
    db: Db,
    job_id: String,
    agent_id: String,
    next_attempt: u32,
    reason: String,
) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db("jobs::requeue_recovered_dispatch_io_failure", move || {
        let mut fresh_conn;
        let mut locked_conn;
        let conn = if let Some(conn) = db.try_conn()? {
            locked_conn = conn;
            &mut *locked_conn
        } else {
            fresh_conn = db.fresh_conn()?;
            &mut fresh_conn
        };
        requeue_recovered_dispatch_io_failure_sync(conn, &job_id, &agent_id, next_attempt, &reason)
    })
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
            crate::orchestrator::wake_up();
        }
    })
}

pub(crate) async fn requeue_dispatched_job_before_send(
    db: Db,
    job_id: String,
    agent_id: String,
    reason: String,
) -> Result<usize, CcbdError> {
    let notify_job_id = job_id.clone();
    spawn_db("jobs::requeue_dispatched_job_before_send", move || {
        let mut fresh_conn;
        let mut locked_conn;
        let conn = if let Some(conn) = db.try_conn()? {
            locked_conn = conn;
            &mut *locked_conn
        } else {
            fresh_conn = db.fresh_conn()?;
            &mut fresh_conn
        };
        requeue_dispatched_job_before_send_sync(conn, &job_id, &agent_id, &reason)
    })
    .await
    .inspect(|changes| {
        if *changes > 0 {
            notify_job_changed(&notify_job_id);
            crate::orchestrator::wake_up();
        }
    })
}

pub(crate) async fn clear_recovered_dispatch_marker(
    db: Db,
    job_id: String,
) -> Result<usize, CcbdError> {
    spawn_db("jobs::clear_recovered_dispatch_marker", move || {
        clear_recovered_dispatch_marker_sync(&db, &job_id)
    })
    .await
}

pub async fn mark_dispatched_jobs_failed_for_agent(
    db: Db,
    agent_id: String,
    reason: String,
) -> Result<usize, CcbdError> {
    let (changes, affected_jobs) =
        spawn_db("jobs::mark_dispatched_jobs_failed_for_agent", move || {
            mark_dispatched_jobs_failed_for_agent_collect_sync(&db, &agent_id, &reason)
        })
        .await?;
    if changes > 0 {
        for job_id in affected_jobs.iter() {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        notify_runtime_job_changed();
    }
    Ok(changes)
}

pub async fn query_dispatched_job_for_agent(
    db: Db,
    agent_id: String,
) -> Result<Option<Job>, CcbdError> {
    spawn_db("jobs::query_dispatched_job_for_agent", move || {
        let conn = db.conn();
        query_dispatched_job_for_agent_sync(&conn, &agent_id)
    })
    .await
}

pub async fn query_starved_queued_jobs(
    db: Db,
    starvation_threshold_secs: i64,
) -> Result<Vec<Job>, CcbdError> {
    spawn_db("jobs::query_starved_queued_jobs", move || {
        let conn = db.conn();
        query_starved_queued_jobs_sync(&conn, starvation_threshold_secs)
    })
    .await
}

pub async fn update_dispatched_seq_id(
    db: Db,
    job_id: String,
    seq_id: i64,
) -> Result<usize, CcbdError> {
    spawn_db("jobs::update_dispatched_seq_id", move || {
        let conn = db.conn();
        update_dispatched_seq_id_sync(&conn, &job_id, seq_id)
    })
    .await
}

pub async fn collect_reply_for_dispatched_job(
    db: Db,
    agent_id: String,
    dispatched_at_seq_id: Option<i64>,
    prompt_text: String,
) -> Result<String, CcbdError> {
    spawn_db("jobs::collect_reply_for_dispatched_job", move || {
        let conn = db.conn();
        collect_reply_for_dispatched_job_sync(&conn, &agent_id, dispatched_at_seq_id, &prompt_text)
    })
    .await
}

pub async fn set_job_evidence_requirements(
    db: Db,
    job_id: String,
    requires_physical_evidence: bool,
    requires_test_evidence: bool,
) -> Result<usize, CcbdError> {
    spawn_db("jobs::set_job_evidence_requirements", move || {
        let conn = db.conn();
        set_job_evidence_requirements_sync(
            &conn,
            &job_id,
            requires_physical_evidence,
            requires_test_evidence,
        )
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        claim_next_job_sync, collect_reply_for_dispatched_job_sync, dispatch_job_to_agent_sync,
        distill_reply, insert_job_sync, insert_job_with_binding_sync,
        mark_dispatched_job_cancelled_if_agent_idle_sync, mark_dispatched_job_delivered_sync,
        mark_dispatched_jobs_failed_for_agent_sync, mark_job_completed_sync, mark_job_failed_sync,
        mark_queued_job_cancelled_sync, query_dispatched_job_for_agent_sync,
        query_job_by_request_id_sync, query_job_sync, request_dispatched_job_cancel_sync,
        set_job_evidence_requirements_sync, strip_ansi_csi, update_dispatched_seq_id_sync,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::{
        STATE_BUSY, STATE_IDLE, STATE_PROMPT_PENDING, STATE_WAITING_FOR_ACK,
    };
    use crate::db::{Db, init};
    use crate::error::CcbdError;
    use tokio::sync::broadcast;
    use tokio::time::{Duration, timeout};

    fn with_test_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
        }
        test(&db)
    }

    fn transition_rows(
        db: &Db,
        job_id: &str,
    ) -> Vec<(String, Option<String>, Option<String>, String)> {
        let conn = db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT kind, old_status, new_status, reason
                 FROM job_transitions
                 WHERE job_id = ?
                 ORDER BY job_event_id ASC",
            )
            .unwrap();
        stmt.query_map([job_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    async fn recv_until_job_update(
        updates: &mut broadcast::Receiver<String>,
        expected_job_id: &str,
    ) {
        timeout(Duration::from_secs(2), async {
            loop {
                match updates.recv().await {
                    Ok(job_id) if job_id == expected_job_id => return,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("job update channel closed before {expected_job_id} notification")
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for job update for {expected_job_id}"));
    }

    async fn recv_until_runtime_job_changed(
        updates: &mut broadcast::Receiver<crate::runtime_events::RuntimeSnapshotReason>,
    ) {
        timeout(Duration::from_secs(2), async {
            loop {
                match updates.recv().await {
                    Ok(crate::runtime_events::RuntimeSnapshotReason::JobChanged) => return,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("runtime update channel closed before JobChanged notification")
                    }
                }
            }
        })
        .await
        .expect("timed out waiting for runtime JobChanged notification");
    }

    #[test]
    fn test_distill_reply_strips_ansi_escapes() {
        let raw = "\u{1b}[31mactual reply\u{1b}[0m";

        assert_eq!(distill_reply(raw, ""), "actual reply");
    }

    #[test]
    fn governance_binding_is_persisted_and_idempotency_refuses_rebinding() {
        with_test_db(|db| {
            let conn = db.conn();
            let binding = r#"{"run_id":"RUN-1"}"#;
            insert_job_with_binding_sync(
                &conn,
                "job-1",
                "a1",
                Some("RUN-1"),
                "prompt",
                Some(binding),
            )
            .unwrap();
            let stored = query_job_sync(&conn, "job-1").unwrap().unwrap();
            assert_eq!(stored.governance_binding_json.as_deref(), Some(binding));

            let error = insert_job_with_binding_sync(
                &conn,
                "job-2",
                "a1",
                Some("RUN-1"),
                "prompt",
                Some(r#"{"run_id":"RUN-2"}"#),
            )
            .unwrap_err();
            assert!(error.to_string().contains("different governance identity"));
        });
    }

    #[test]
    fn test_distill_reply_removes_prompt_echo() {
        let raw = "echo prompt\nactual reply";

        assert_eq!(distill_reply(raw, "echo prompt"), "actual reply");
    }

    #[test]
    fn test_distill_reply_removes_prompt_marker_echo_line() {
        let raw = "> echo prompt\nactual reply";

        assert_eq!(distill_reply(raw, "echo prompt"), "actual reply");
    }

    #[test]
    fn test_distill_reply_trims_whitespace() {
        let raw = "\n  actual reply \n\n";

        assert_eq!(distill_reply(raw, ""), "actual reply");
    }

    #[test]
    fn test_distill_reply_removes_antigravity_chrome() {
        let raw = "reply PONG\n\
                   > \n\n\
                     PONG\n\n\
                   ────────────────────────────────────────────────\n\
                   >\n\
                   ────────────────────────────────────────────────\n\
                   ? for shortcuts                          Gemini 3.5 Flash (High)\n";

        assert_eq!(distill_reply(raw, "reply PONG"), "PONG");
    }

    #[test]
    fn test_distill_reply_removes_antigravity_prompt_echo_and_thought_summary() {
        let prompt = "In two sentences, what is X?";
        let raw = "> In two sentences, what is X?\n\n\
                   ▸ Thought for 1s, 306 tokens\n\
                     Defining X\n\
                     X is a thing.\n\
                   ────────────────────────────────────────────────\n\
                   >\n\
                   ? for shortcuts                          Gemini 3.5 Flash (High)\n";

        assert_eq!(distill_reply(raw, prompt), "Defining X\nX is a thing.");
    }

    #[test]
    fn test_distill_reply_removes_codex_prompt_and_status_chrome() {
        let raw = "say alpha\n\
                   › \n\
                     gpt-5.5 default · /workspace\n";

        assert_eq!(distill_reply(raw, "say alpha"), "");
    }

    #[test]
    fn test_distill_reply_anchors_soft_wrapped_antigravity_prompt() {
        let prompt = "Please reply with exactly one single word and nothing else, no punctuation no explanation no commentary whatsoever, and the one word you must reply with is: delta";
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/pane_idle/REAL-a3-idle-longprompt-wrapped.txt"
        ));

        assert_eq!(distill_reply(raw, prompt), "delta");
    }

    #[test]
    fn test_distill_reply_keeps_claude_content() {
        let raw = "summarize\nHere is the result:\n- alpha\n- beta\n❯";

        assert_eq!(
            distill_reply(raw, "summarize"),
            "Here is the result:\n- alpha\n- beta"
        );
    }

    #[test]
    fn test_strip_ansi_escapes_handles_osc_window_title() {
        let raw = "\u{1b}]0;some title\u{07}content";

        assert_eq!(strip_ansi_csi(raw), "content");
    }

    #[test]
    fn test_strip_ansi_escapes_handles_osc_with_st_terminator() {
        let raw = "\u{1b}]0;title\u{1b}\\content";

        assert_eq!(strip_ansi_csi(raw), "content");
    }

    #[test]
    fn test_strip_ansi_escapes_handles_simple_esc() {
        let raw = "\u{1b}=hello";

        assert_eq!(strip_ansi_csi(raw), "hello");
    }

    #[test]
    fn test_strip_ansi_escapes_real_codex_status_bar() {
        let raw = "Use /skills to list available skills\n\
                   \u{1b}]0;⠴ ccbd-rust•Working(0s • esc to interrupt)\u{07}\
                   › echo from a1\n\
                   \u{1b}]0;⠦ ccbd-rust\u{1b}\\clean reply";

        let stripped = strip_ansi_csi(raw);

        assert!(!stripped.contains("]0;"));
        assert!(!stripped.contains("ccbd-rust•Working"));
        assert!(stripped.contains("Use /skills to list available skills"));
        assert!(stripped.contains("› echo from a1"));
        assert!(stripped.contains("clean reply"));
    }

    #[test]
    fn test_insert_and_query_job() {
        with_test_db(|db| {
            let conn = db.conn();
            let id = insert_job_sync(&conn, "job_1", "a1", None, "hello").unwrap();
            let job = query_job_sync(&conn, &id).unwrap().unwrap();

            assert_eq!(id, "job_1");
            assert_eq!(job.agent_id, "a1");
            assert_eq!(job.prompt_text, "hello");
            assert_eq!(job.status, "QUEUED");
            assert_eq!(job.dispatched_at_seq_id, None);
            drop(conn);
            assert_eq!(
                transition_rows(db, "job_1"),
                vec![(
                    "job_transition".to_string(),
                    None,
                    Some("QUEUED".to_string()),
                    "submit".to_string(),
                )]
            );
        });
    }

    #[test]
    fn test_insert_job_idempotent_by_request_id() {
        with_test_db(|db| {
            let conn = db.conn();
            let first = insert_job_sync(&conn, "job_1", "a1", Some("req-1"), "hello").unwrap();
            let second = insert_job_sync(&conn, "job_2", "a1", Some("req-1"), "other").unwrap();
            let by_req = query_job_by_request_id_sync(&conn, "a1", "req-1")
                .unwrap()
                .unwrap();

            assert_eq!(first, "job_1");
            assert_eq!(second, "job_1");
            assert_eq!(by_req.prompt_text, "hello");
            drop(conn);
            assert_eq!(transition_rows(db, "job_1").len(), 1);
            assert!(transition_rows(db, "job_2").is_empty());
        });
    }

    #[test]
    fn test_query_missing_job_returns_none() {
        with_test_db(|db| {
            let conn = db.conn();
            assert!(query_job_sync(&conn, "job_missing").unwrap().is_none());
        });
    }

    #[test]
    fn test_claim_next_job_fifo_and_single_claim() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
                insert_job_sync(&conn, "job_2", "a1", None, "two").unwrap();
            }

            let first = claim_next_job_sync(db, "a1").unwrap().unwrap();
            let second = claim_next_job_sync(db, "a1").unwrap().unwrap();
            let third = claim_next_job_sync(db, "a1").unwrap();

            assert_eq!(first.id, "job_1");
            assert_eq!(first.status, "DISPATCHED");
            assert!(first.dispatched_at.is_some());
            assert_eq!(second.id, "job_2");
            assert!(third.is_none());
            assert_eq!(
                transition_rows(db, "job_1")[1],
                (
                    "job_transition".to_string(),
                    Some("QUEUED".to_string()),
                    Some("DISPATCHED".to_string()),
                    "claim_next_job".to_string(),
                )
            );
        });
    }

    #[test]
    fn test_claim_next_job_skips_stuck_agent() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                conn.execute("UPDATE agents SET state = 'STUCK' WHERE id = 'a1'", [])
                    .unwrap();
                insert_job_sync(&conn, "job_stuck", "a1", None, "one").unwrap();
            }

            let claimed = claim_next_job_sync(db, "a1").unwrap();
            let job = query_job_sync(&db.conn(), "job_stuck").unwrap().unwrap();

            assert!(claimed.is_none());
            assert_eq!(job.status, "QUEUED");
        });
    }

    #[test]
    fn test_claim_next_job_skips_waiting_for_ack_agent() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                conn.execute(
                    "UPDATE agents SET state = ? WHERE id = 'a1'",
                    [STATE_WAITING_FOR_ACK],
                )
                .unwrap();
                insert_job_sync(&conn, "job_waiting", "a1", None, "one").unwrap();
            }

            let claimed = claim_next_job_sync(db, "a1").unwrap();
            let job = query_job_sync(&db.conn(), "job_waiting").unwrap().unwrap();

            assert!(claimed.is_none());
            assert_eq!(job.status, "QUEUED");
        });
    }

    #[test]
    fn test_claim_next_job_skips_prompt_pending_agent() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                conn.execute(
                    "UPDATE agents SET state = ? WHERE id = 'a1'",
                    [STATE_PROMPT_PENDING],
                )
                .unwrap();
                insert_job_sync(&conn, "job_prompt_pending", "a1", None, "one").unwrap();
            }

            let claimed = claim_next_job_sync(db, "a1").unwrap();
            let job = query_job_sync(&db.conn(), "job_prompt_pending")
                .unwrap()
                .unwrap();

            assert!(claimed.is_none());
            assert_eq!(job.status, "QUEUED");
        });
    }

    #[test]
    fn test_dispatch_job_to_agent_sync_updates_metadata_atomically() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_dispatch", "a1", None, "ask").unwrap();
            }

            let mut conn = db.conn();
            let dispatched = dispatch_job_to_agent_sync(
                &mut conn,
                "a1",
                &[STATE_IDLE],
                STATE_WAITING_FOR_ACK,
                "command_received",
                &serde_json::json!({ "status": "SENT" }),
            )
            .unwrap()
            .unwrap();

            assert_eq!(dispatched.job.id, "job_dispatch");
            assert_eq!(dispatched.job.status, "DISPATCHED");
            assert_eq!(dispatched.job.dispatched_at_seq_id, Some(dispatched.seq_id));
            assert_eq!(dispatched.job_payload["cmd"], "ask");
            assert_eq!(dispatched.job_payload["job_id"], "job_dispatch");
            let (agent_state, job_status, seq_id, command_events, state_events, observations): (
                String,
                String,
                Option<i64>,
                i64,
                i64,
                i64,
            ) = conn
                .query_row(
                    "SELECT agents.state, jobs.status, jobs.dispatched_at_seq_id, \
                            (SELECT COUNT(*) FROM events WHERE agent_id = 'a1' AND event_type = 'command_received'), \
                            (SELECT COUNT(*) FROM events WHERE agent_id = 'a1' AND event_type = 'state_change'), \
                            (SELECT COUNT(*) FROM provider_status_observations WHERE agent_id = 'a1' AND turn_id = 'job_dispatch') \
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id WHERE jobs.id = 'job_dispatch'",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .unwrap();

            assert_eq!(agent_state, STATE_WAITING_FOR_ACK);
            assert_eq!(job_status, "DISPATCHED");
            assert_eq!(seq_id, Some(dispatched.seq_id));
            assert_eq!(command_events, 1);
            assert_eq!(state_events, 1);
            assert_eq!(observations, 2);
            let turn_states = {
                let mut stmt = conn
                    .prepare(
                        "SELECT json_extract(observation_json, '$.kind.state') FROM provider_status_observations WHERE agent_id='a1' AND turn_id='job_dispatch' ORDER BY rowid",
                    )
                    .unwrap();
                stmt.query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            };
            assert_eq!(turn_states, ["queued", "delivering"]);
            assert!(
                mark_dispatched_job_delivered_sync(
                    &mut conn,
                    "a1",
                    "job_dispatch",
                    dispatched.seq_id,
                )
                .unwrap()
            );
            assert!(
                !mark_dispatched_job_delivered_sync(
                    &mut conn,
                    "a1",
                    "job_dispatch",
                    dispatched.seq_id,
                )
                .unwrap()
            );
            let delivered: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM provider_status_observations WHERE agent_id='a1' AND turn_id='job_dispatch' AND json_extract(observation_json, '$.kind.state')='delivered'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(delivered, 1);
            drop(conn);
            assert_eq!(
                transition_rows(db, "job_dispatch")[1],
                (
                    "job_transition".to_string(),
                    Some("QUEUED".to_string()),
                    Some("DISPATCHED".to_string()),
                    "dispatch_job_to_agent".to_string(),
                )
            );
        });
    }

    #[test]
    fn test_dispatch_job_to_agent_sync_rejects_wrong_state_and_rolls_back() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                conn.execute("UPDATE agents SET state = ? WHERE id = 'a1'", [STATE_BUSY])
                    .unwrap();
                insert_job_sync(&conn, "job_busy", "a1", None, "ask").unwrap();
            }

            let mut conn = db.conn();
            let err = dispatch_job_to_agent_sync(
                &mut conn,
                "a1",
                &[STATE_IDLE],
                STATE_WAITING_FOR_ACK,
                "command_received",
                &serde_json::json!({ "status": "SENT" }),
            )
            .unwrap_err();
            let (agent_state, job_status, event_count): (String, String, i64) = conn
                .query_row(
                    "SELECT agents.state, jobs.status, COUNT(events.seq_id) \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     LEFT JOIN events ON events.agent_id = agents.id \
                     WHERE jobs.id = 'job_busy' \
                     GROUP BY agents.id, jobs.id",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert!(matches!(err, CcbdError::AgentWrongState { .. }));
            assert_eq!(agent_state, STATE_BUSY);
            assert_eq!(job_status, "QUEUED");
            assert_eq!(event_count, 0);
        });
    }

    #[test]
    fn test_dispatch_job_to_agent_sync_rejects_prompt_pending_and_keeps_job_queued() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                conn.execute(
                    "UPDATE agents SET state = ? WHERE id = 'a1'",
                    [STATE_PROMPT_PENDING],
                )
                .unwrap();
                insert_job_sync(&conn, "job_pending_dispatch", "a1", None, "hello").unwrap();
            }

            let mut conn = db.conn();
            let err = dispatch_job_to_agent_sync(
                &mut conn,
                "a1",
                &[STATE_IDLE],
                STATE_WAITING_FOR_ACK,
                "command_received",
                &serde_json::json!({ "status": "SENT" }),
            )
            .unwrap_err();
            let (agent_state, job_status, event_count): (String, String, i64) = conn
                .query_row(
                    "SELECT agents.state, jobs.status, COUNT(events.seq_id) \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     LEFT JOIN events ON events.agent_id = agents.id \
                     WHERE agents.id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert!(matches!(err, CcbdError::AgentWrongState { .. }));
            assert_eq!(agent_state, STATE_PROMPT_PENDING);
            assert_eq!(job_status, "QUEUED");
            assert_eq!(event_count, 0);
        });
    }

    #[test]
    fn test_dispatch_job_to_agent_sync_missing_agent_errors_without_side_effects() {
        with_test_db(|db| {
            let mut conn = db.conn();
            let err = dispatch_job_to_agent_sync(
                &mut conn,
                "missing",
                &[STATE_IDLE],
                STATE_WAITING_FOR_ACK,
                "command_received",
                &serde_json::json!({ "status": "SENT" }),
            )
            .unwrap_err();

            assert!(matches!(err, CcbdError::AgentNotFound(agent) if agent == "missing"));
        });
    }

    #[test]
    fn test_dispatch_job_to_agent_sync_no_job_has_no_side_effects() {
        with_test_db(|db| {
            let mut conn = db.conn();
            let dispatched = dispatch_job_to_agent_sync(
                &mut conn,
                "a1",
                &[STATE_IDLE],
                STATE_WAITING_FOR_ACK,
                "command_received",
                &serde_json::json!({ "status": "SENT" }),
            )
            .unwrap();
            let (agent_state, event_count): (String, i64) = conn
                .query_row(
                    "SELECT state, (SELECT COUNT(*) FROM events WHERE agent_id = 'a1') FROM agents WHERE id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();

            assert!(dispatched.is_none());
            assert_eq!(agent_state, STATE_IDLE);
            assert_eq!(event_count, 0);
        });
    }

    #[test]
    fn test_mark_job_completed_only_dispatched() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
            }

            assert_eq!(mark_job_completed_sync(db, "job_1", "reply").unwrap(), 0);
            claim_next_job_sync(db, "a1").unwrap().unwrap();
            assert_eq!(mark_job_completed_sync(db, "job_1", "reply").unwrap(), 1);

            let conn = db.conn();
            let job = query_job_sync(&conn, "job_1").unwrap().unwrap();
            assert_eq!(job.status, "COMPLETED");
            assert_eq!(job.reply_text.as_deref(), Some("reply"));
            assert!(job.completed_at.is_some());
            assert!(!job.requires_physical_evidence);
            assert!(!job.requires_test_evidence);
            drop(conn);
            assert_eq!(
                transition_rows(db, "job_1").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("DISPATCHED".to_string()),
                    Some("COMPLETED".to_string()),
                    "completed".to_string(),
                )
            );
        });
    }

    #[test]
    fn test_set_job_evidence_requirements() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
                set_job_evidence_requirements_sync(&conn, "job_1", true, true).unwrap();
            }

            let conn = db.conn();
            let job = query_job_sync(&conn, "job_1").unwrap().unwrap();
            assert!(job.requires_physical_evidence);
            assert!(job.requires_test_evidence);
        });
    }

    #[test]
    fn test_mark_job_failed_from_queued_or_dispatched() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
                insert_job_sync(&conn, "job_2", "a1", None, "two").unwrap();
            }
            claim_next_job_sync(db, "a1").unwrap().unwrap();

            assert_eq!(mark_job_failed_sync(db, "job_1", "boom").unwrap(), 1);
            assert_eq!(mark_job_failed_sync(db, "job_2", "skip").unwrap(), 1);

            let conn = db.conn();
            let job_1 = query_job_sync(&conn, "job_1").unwrap().unwrap();
            let job_2 = query_job_sync(&conn, "job_2").unwrap().unwrap();
            assert_eq!(job_1.status, "FAILED");
            assert_eq!(job_1.error_reason.as_deref(), Some("boom"));
            assert_eq!(job_2.status, "FAILED");
            assert_eq!(job_2.error_reason.as_deref(), Some("skip"));
            drop(conn);
            assert_eq!(
                transition_rows(db, "job_1").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("DISPATCHED".to_string()),
                    Some("FAILED".to_string()),
                    "boom".to_string(),
                )
            );
            assert_eq!(
                transition_rows(db, "job_2").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("QUEUED".to_string()),
                    Some("FAILED".to_string()),
                    "skip".to_string(),
                )
            );
        });
    }

    #[test]
    fn test_mark_queued_job_cancelled() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
            }

            assert_eq!(mark_queued_job_cancelled_sync(db, "job_1").unwrap(), 1);
            assert_eq!(mark_queued_job_cancelled_sync(db, "job_1").unwrap(), 0);
            let job = query_job_sync(&db.conn(), "job_1").unwrap().unwrap();
            assert_eq!(job.status, "CANCELLED");
            assert!(job.completed_at.is_some());
            assert_eq!(
                transition_rows(db, "job_1").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("QUEUED".to_string()),
                    Some("CANCELLED".to_string()),
                    "cancel_queued".to_string(),
                )
            );
            assert_eq!(transition_rows(db, "job_1").len(), 2);
        });
    }

    #[test]
    fn test_request_dispatched_job_cancel() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
            }
            claim_next_job_sync(db, "a1").unwrap().unwrap();

            assert_eq!(request_dispatched_job_cancel_sync(db, "job_1").unwrap(), 1);
            let job = query_job_sync(&db.conn(), "job_1").unwrap().unwrap();
            assert_eq!(job.status, "DISPATCHED");
            assert!(job.cancel_requested);
            assert_eq!(
                transition_rows(db, "job_1").last().unwrap(),
                &(
                    "job_updated".to_string(),
                    Some("DISPATCHED".to_string()),
                    Some("DISPATCHED".to_string()),
                    "cancel_requested".to_string(),
                )
            );
        });
    }

    #[tokio::test]
    async fn request_dispatched_job_cancel_notifies_job_and_runtime_subscribers() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "bash", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, "job_cancel_notify", "a1", None, "one").unwrap();
        }
        claim_next_job_sync(&db, "a1").unwrap().unwrap();
        let mut job_updates = crate::orchestrator::pubsub::subscribe_job_updates();
        let mut runtime_updates = crate::orchestrator::pubsub::subscribe_runtime_updates();

        assert_eq!(
            super::request_dispatched_job_cancel(db.clone(), "job_cancel_notify".into())
                .await
                .unwrap(),
            1
        );

        recv_until_job_update(&mut job_updates, "job_cancel_notify").await;
        recv_until_runtime_job_changed(&mut runtime_updates).await;
    }

    #[test]
    fn test_mark_dispatched_cancelled_if_agent_already_idle() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
            }
            claim_next_job_sync(db, "a1").unwrap().unwrap();
            request_dispatched_job_cancel_sync(db, "job_1").unwrap();

            assert_eq!(
                mark_dispatched_job_cancelled_if_agent_idle_sync(db, "job_1").unwrap(),
                1
            );
            let job = query_job_sync(&db.conn(), "job_1").unwrap().unwrap();
            assert_eq!(job.status, "CANCELLED");
            assert_eq!(
                transition_rows(db, "job_1").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("DISPATCHED".to_string()),
                    Some("CANCELLED".to_string()),
                    "cancel_settled".to_string(),
                )
            );
        });
    }

    #[test]
    fn test_mark_dispatched_jobs_failed_for_agent() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
                insert_job_sync(&conn, "job_2", "a1", None, "two").unwrap();
            }
            claim_next_job_sync(db, "a1").unwrap().unwrap();
            claim_next_job_sync(db, "a1").unwrap().unwrap();

            assert_eq!(
                mark_dispatched_jobs_failed_for_agent_sync(db, "a1", "crashed").unwrap(),
                2
            );

            let conn = db.conn();
            let job_1 = query_job_sync(&conn, "job_1").unwrap().unwrap();
            let job_2 = query_job_sync(&conn, "job_2").unwrap().unwrap();
            assert_eq!(job_1.status, "FAILED");
            assert_eq!(job_2.status, "FAILED");
            drop(conn);
            assert_eq!(
                transition_rows(db, "job_1").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("DISPATCHED".to_string()),
                    Some("FAILED".to_string()),
                    "crashed".to_string(),
                )
            );
            assert_eq!(
                transition_rows(db, "job_2").last().unwrap(),
                &(
                    "job_transition".to_string(),
                    Some("DISPATCHED".to_string()),
                    Some("FAILED".to_string()),
                    "crashed".to_string(),
                )
            );
        });
    }

    #[test]
    fn test_query_dispatched_job_for_agent() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
            }
            assert!(
                query_dispatched_job_for_agent_sync(&db.conn(), "a1")
                    .unwrap()
                    .is_none()
            );
            claim_next_job_sync(db, "a1").unwrap().unwrap();
            let dispatched = query_dispatched_job_for_agent_sync(&db.conn(), "a1")
                .unwrap()
                .unwrap();
            assert_eq!(dispatched.id, "job_1");
        });
    }

    #[test]
    fn test_update_dispatched_seq_id() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
            }
            claim_next_job_sync(db, "a1").unwrap().unwrap();
            {
                let conn = db.conn();
                assert_eq!(
                    update_dispatched_seq_id_sync(&conn, "job_1", 42).unwrap(),
                    1
                );
                let job = query_job_sync(&conn, "job_1").unwrap().unwrap();
                assert_eq!(job.dispatched_at_seq_id, Some(42));
            }
        });
    }

    #[test]
    fn test_collect_reply_for_dispatched_job_uses_seq_id_boundary() {
        with_test_db(|db| {
            let (before, after) = {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "one").unwrap();
                let before =
                    insert_event_sync(&conn, "a1", None, "output_chunk", r#"{"text":"old"}"#)
                        .unwrap();
                let after =
                    insert_event_sync(&conn, "a1", None, "output_chunk", r#"{"text":"new"}"#)
                        .unwrap();
                (before, after)
            };
            let conn = db.conn();
            let reply =
                collect_reply_for_dispatched_job_sync(&conn, "a1", Some(before), "").unwrap();

            assert!(after > before);
            assert_eq!(reply, "new");
        });
    }

    #[test]
    fn test_collect_reply_uses_vt100_to_handle_cursor_reposition() {
        with_test_db(|db| {
            let before = {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "echo from a1").unwrap();
                let before =
                    insert_event_sync(&conn, "a1", None, "command_received", r#"{"cmd":"echo"}"#)
                        .unwrap();
                insert_event_sync(
                    &conn,
                    "a1",
                    None,
                    "output_chunk",
                    "{\"text\":\"clean reply\\n\\u001b[10;1HWorking(0s)\\u001b[10;1H           \"}",
                )
                .unwrap();
                before
            };

            let reply = collect_reply_for_dispatched_job_sync(
                &db.conn(),
                "a1",
                Some(before),
                "echo from a1",
            )
            .unwrap();

            assert_eq!(reply, "clean reply");
        });
    }

    #[test]
    fn test_collect_reply_handles_status_bar_overwrite() {
        with_test_db(|db| {
            let before = {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "summarize").unwrap();
                let before =
                    insert_event_sync(&conn, "a1", None, "command_received", r#"{"cmd":"ask"}"#)
                        .unwrap();
                insert_event_sync(
                    &conn,
                    "a1",
                    None,
                    "output_chunk",
                    "{\"text\":\"\\u001b[1;1H•Working(0s • esc to interrupt)\\u001b[1;1H\\u001b[2KFinal answer\"}",
                )
                .unwrap();
                before
            };

            let reply =
                collect_reply_for_dispatched_job_sync(&db.conn(), "a1", Some(before), "summarize")
                    .unwrap();

            assert_eq!(reply, "Final answer");
            assert!(!reply.contains("Working"));
        });
    }

    #[test]
    fn test_collect_reply_handles_long_output_beyond_50_lines() {
        with_test_db(|db| {
            let before = {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "generate").unwrap();
                let before =
                    insert_event_sync(&conn, "a1", None, "command_received", r#"{"cmd":"gen"}"#)
                        .unwrap();
                let mut long_text = String::new();
                for i in 1..=100 {
                    long_text.push_str(&format!("Line {i}: content here\n"));
                }
                let payload = serde_json::json!({ "text": long_text }).to_string();
                insert_event_sync(&conn, "a1", None, "output_chunk", &payload).unwrap();
                before
            };

            let reply =
                collect_reply_for_dispatched_job_sync(&db.conn(), "a1", Some(before), "generate")
                    .unwrap();

            assert!(reply.contains("Line 1: content here"));
            assert!(reply.contains("Line 100: content here"));
        });
    }

    #[test]
    fn test_collect_reply_fallback_when_vt100_screen_empty() {
        with_test_db(|db| {
            let before = {
                let conn = db.conn();
                insert_job_sync(&conn, "job_1", "a1", None, "ask something").unwrap();
                let before =
                    insert_event_sync(&conn, "a1", None, "command_received", r#"{"cmd":"ask"}"#)
                        .unwrap();
                // Simulate output that only uses cursor positioning to clear itself,
                // leaving vt100 screen empty but raw text containing real content.
                // In practice this can happen with certain TUI frameworks that reset screen.
                let payload = serde_json::json!({
                    "text": "\x1b[2J\x1b[HReal answer from agent"
                })
                .to_string();
                insert_event_sync(&conn, "a1", None, "output_chunk", &payload).unwrap();
                before
            };

            let reply = collect_reply_for_dispatched_job_sync(
                &db.conn(),
                "a1",
                Some(before),
                "ask something",
            )
            .unwrap();

            assert!(reply.contains("Real answer from agent"));
        });
    }

    // P0-2 修复二(认领时刻 cancel 检查):recovery requeue 会把已请求取消的 job 携 cancel
    // 标志原样 requeue 成 QUEUED(recovery.rs:415),而认领 SQL 现码(jobs.rs:286 SELECT /
    // :301 UPDATE)从不检查 cancel_requested → 已取消的毒任务照样被认领重派。改后契约:
    // claim 跳过 cancel_requested=1 的 QUEUED job(不 DISPATCHED)。现码会认领 → RED。
    // 注:改后 #4 契约会把该 job 收敛到终态 CANCELLED,故此处断言"未 DISPATCHED"而非"仍 QUEUED"。
    #[test]
    fn test_claim_skips_cancel_requested_job() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_cancel", "a1", None, "poison\n").unwrap();
                conn.execute(
                    "UPDATE jobs SET cancel_requested = 1 WHERE id = 'job_cancel'",
                    [],
                )
                .unwrap();
            }

            let claimed = claim_next_job_sync(db, "a1").unwrap();
            assert!(
                claimed.is_none(),
                "claim must skip a cancel_requested QUEUED job, but claimed {claimed:?}"
            );

            let status = query_job_sync(&db.conn(), "job_cancel")
                .unwrap()
                .unwrap()
                .status;
            assert_ne!(
                status, "DISPATCHED",
                "cancel_requested job must not be dispatched, got status {status}"
            );
        });
    }

    // P0-2 修复二(取消 job 收敛终态):被跳过的 cancel_requested QUEUED job 不能永久滞留
    // QUEUED 攒垃圾;claim 在同一路径把它收敛到终态 CANCELLED(走既有 job transition 记账)。
    // 现码认领它 → DISPATCHED(非终态、且被错误重派)→ RED;a1 加 cancel 过滤 + 收敛后 →
    // CANCELLED。终态拼写锚全库统一的 CANCELLED(双 L),brief 的 "CANCELED" 系笔误。
    #[test]
    fn test_cancel_requested_queued_job_resolves_to_terminal() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_cancel_term", "a1", None, "poison\n").unwrap();
                conn.execute(
                    "UPDATE jobs SET cancel_requested = 1 WHERE id = 'job_cancel_term'",
                    [],
                )
                .unwrap();
            }

            let _ = claim_next_job_sync(db, "a1").unwrap();

            let status = query_job_sync(&db.conn(), "job_cancel_term")
                .unwrap()
                .unwrap()
                .status;
            assert_eq!(
                status, "CANCELLED",
                "cancel_requested QUEUED job must resolve to terminal CANCELLED, not linger; got {status}"
            );
        });
    }

    // P0-2 回归:cancel_requested=0 的正常 QUEUED job 仍被正常认领 DISPATCHED(cancel 过滤
    // 不误伤好路径)。现码 GREEN,修后仍 GREEN(合法回归,不回炉)。
    #[test]
    fn test_normal_queued_job_still_claimed() {
        with_test_db(|db| {
            {
                let conn = db.conn();
                insert_job_sync(&conn, "job_normal", "a1", None, "do work\n").unwrap();
            }

            let claimed = claim_next_job_sync(db, "a1")
                .unwrap()
                .expect("normal queued job must be claimed");
            assert_eq!(claimed.id, "job_normal");
            assert_eq!(claimed.status, "DISPATCHED");

            let status = query_job_sync(&db.conn(), "job_normal")
                .unwrap()
                .unwrap()
                .status;
            assert_eq!(status, "DISPATCHED");
        });
    }
}
