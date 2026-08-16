use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::db::evidence::has_job_evidence_sync;
use crate::db::jobs::{
    collect_reply_for_dispatched_job_sync, mark_dispatched_jobs_failed_for_agent_conn_sync,
    mark_job_cancelled_conn_sync, mark_job_completed_conn_sync,
    query_dispatched_job_for_agent_sync, strip_ansi_escapes,
};
use crate::error::CcbdError;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

/// 状态字符串常量 (避免散落字面量).
pub const STATE_SPAWNING: &str = "SPAWNING";
pub const STATE_SPAWNING_INTERVENTION: &str = "SPAWNING_INTERVENTION";
pub const STATE_IDLE: &str = "IDLE";
pub const STATE_WAITING_FOR_ACK: &str = "WAITING_FOR_ACK";
pub const STATE_BUSY: &str = "BUSY";
/// Agent is blocked on an interactive prompt and waiting for master resolution.
///
/// This is neither an active execution state nor a terminal state: orchestrator must not
/// dispatch new jobs to it, and lifecycle/reconcile paths must not silently crash or kill it.
pub const STATE_PROMPT_PENDING: &str = "PROMPT_PENDING";
pub const STATE_STUCK: &str = "STUCK";
pub const STATE_FAILED: &str = "FAILED";
pub const STATE_CRASHED: &str = "CRASHED";
pub const STATE_KILLED: &str = "KILLED";
pub const STATE_UNKNOWN: &str = "UNKNOWN";

pub const EVIDENCE_DENY_MESSAGE: &str = "SYSTEM DENY: Missing physical evidence. You must output a git diff or test result before finishing.";
const LOG_EVENT_TASK_COMPLETE_REASON: &str = "LOG_EVENT_TASK_COMPLETE";
const LOG_EVENT_SUB_STATE: &str = "LogEvent";
const HOOK_EVENT_SUB_STATE: &str = "HookEvent";

fn notify_runtime_agent_changed() {
    crate::orchestrator::pubsub::notify_runtime_changed(
        crate::runtime_events::RuntimeSnapshotReason::AgentChanged,
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerMatchedOutcome {
    pub changes: usize,
    pub affected_job: Option<String>,
    pub denial_message: Option<String>,
    pub deferred_nudge: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckOutcome {
    pub changes: usize,
    pub agent_id: String,
    pub job_id: Option<String>,
    pub from_state: Option<String>,
    pub event_seq_id: Option<i64>,
    pub job_resolution: String,
}

#[derive(Debug)]
struct MarkerMatchedSyncOutcome {
    changes: usize,
    denial_message: Option<String>,
    deferred_nudge: Option<String>,
}

/// 是否处于 "正在工作 / 待 ACK" 中 (后续 marker / stuck / unknown guard 复用).
///
/// 包括: SPAWNING (启动中), WAITING_FOR_ACK (T2.1.2 引入), BUSY (执行中).
pub fn is_active_state(state: &str) -> bool {
    matches!(state, STATE_SPAWNING | STATE_WAITING_FOR_ACK | STATE_BUSY)
}

/// 是否处于"等待 ACK"状态.
pub fn is_waiting_for_ack(state: &str) -> bool {
    state == STATE_WAITING_FOR_ACK
}

pub fn transit_agent_state_sync(
    conn: &mut Connection,
    agent_id: &str,
    from_state_list: &[&str],
    to_state: &str,
    reason: Option<&str>,
) -> Result<(), CcbdError> {
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin transit agent state", err))?;
    transit_agent_state_conn_inner(&tx, agent_id, from_state_list, to_state, reason, None, true)?;
    tx.commit()
        .map_err(|err| map_db_error("commit transit agent state", err))?;
    Ok(())
}

pub(crate) fn transit_agent_state_conn_sync(
    conn: &Connection,
    agent_id: &str,
    from_state_list: &[&str],
    to_state: &str,
    reason: Option<&str>,
) -> Result<(), CcbdError> {
    transit_agent_state_conn_inner(
        conn,
        agent_id,
        from_state_list,
        to_state,
        reason,
        None,
        true,
    )
    .map(|_| ())
}

fn transit_agent_state_conn_inner(
    conn: &Connection,
    agent_id: &str,
    from_state_list: &[&str],
    to_state: &str,
    reason: Option<&str>,
    expected_version: Option<i64>,
    strict: bool,
) -> Result<bool, CcbdError> {
    let current = conn
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query state for transit", err))?;

    let Some((previous_state, state_version)) = current else {
        return if strict {
            Err(CcbdError::AgentNotFound(agent_id.to_string()))
        } else {
            Ok(false)
        };
    };

    let version_matches = expected_version.is_none_or(|expected| expected == state_version);
    let from_matches =
        from_state_list.is_empty() || from_state_list.contains(&previous_state.as_str());
    if !version_matches || !from_matches {
        return if strict {
            Err(CcbdError::AgentWrongState {
                current_state: previous_state,
            })
        } else {
            Ok(false)
        };
    }

    let changes = conn
        .execute(
            "UPDATE agents SET state = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state_version = ?",
            params![to_state, agent_id, state_version],
        )
        .map_err(|err| map_db_error("transit agent state", err))?;
    if changes != 1 {
        return if strict {
            Err(CcbdError::AgentWrongState {
                current_state: previous_state,
            })
        } else {
            Ok(false)
        };
    }

    let payload = json!({
        "from": previous_state,
        "to": to_state,
        "reason": reason,
    })
    .to_string();
    conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
        params![agent_id, payload],
    )
    .map_err(|err| map_db_error("insert transit state_change", err))?;

    Ok(true)
}

/// Atomic CAS: IDLE -> WAITING_FOR_ACK.
///
/// Returns true if transition succeeded (rowcount == 1), false otherwise
/// (agent in a non-IDLE state, version mismatch, or row missing).
pub(crate) fn mark_agent_waiting_for_ack_sync(
    conn: &mut Connection,
    agent_id: &str,
    expected_version: i64,
) -> Result<bool, CcbdError> {
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent waiting for ack", err))?;
    let changed = transit_agent_state_conn_inner(
        &tx,
        agent_id,
        &[STATE_IDLE],
        STATE_WAITING_FOR_ACK,
        Some("ack_pending"),
        Some(expected_version),
        false,
    )?;
    tx.commit()
        .map_err(|err| map_db_error("commit mark agent waiting for ack", err))?;
    Ok(changed)
}

pub(crate) fn mark_agent_prompt_pending_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent prompt pending", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query state for prompt pending", err))?;

    let Some((previous_state, state_version)) = current else {
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark prompt pending missing agent", err))?;
        return Err(CcbdError::AgentNotFound(agent_id.to_string()));
    };
    let allowed = [STATE_IDLE, STATE_SPAWNING];
    if !allowed.contains(&previous_state.as_str()) {
        tracing::warn!(
            agent_id,
            state = %previous_state,
            reason,
            impact = "agent remains in its current state and no prompt resolution is pending",
            "prompt pending transition rejected"
        );
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark prompt pending invalid state", err))?;
        return Err(CcbdError::AgentWrongState {
            current_state: previous_state,
        });
    }

    tracing::info!(
        agent_id,
        from = %previous_state,
        to = STATE_PROMPT_PENDING,
        reason,
        "mark agent prompt pending start"
    );
    let changes = tx
        .execute(
            "UPDATE agents SET state = 'PROMPT_PENDING', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('IDLE', 'SPAWNING') AND state_version = ?",
            params![agent_id, state_version],
        )
        .map_err(|err| map_db_error("mark agent prompt pending", err))?;

    if changes == 1 {
        let payload = json!({
            "from": previous_state,
            "to": STATE_PROMPT_PENDING,
            "reason": reason,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert prompt pending state_change", err))?;
        let turn_id = query_dispatched_job_for_agent_sync(&tx, agent_id)?.map(|job| job.id);
        crate::runtime_observation::store::append_for_current_lifecycle_sync(
            &tx,
            &format!("prompt:{agent_id}:{state_version}:{reason}"),
            agent_id,
            turn_id.as_deref(),
            crate::runtime_observation::EvidenceSource::TerminalPane,
            crate::runtime_observation::ProviderObservationKind::Turn(
                crate::runtime_observation::ProviderTurnState::AwaitingUser,
            ),
            crate::runtime_observation::store::now_epoch_millis(),
        )?;
    } else {
        tracing::warn!(
            agent_id,
            reason,
            impact = "prompt pending transition lost a state_version race",
            "prompt pending CAS failed"
        );
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark prompt pending CAS failed", err))?;
        return Ok(0);
    }

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent prompt pending", err))?;
    tracing::info!(
        agent_id,
        to = STATE_PROMPT_PENDING,
        reason,
        "mark agent prompt pending complete"
    );
    Ok(changes)
}

#[cfg(test)]
pub(crate) fn mark_agent_idle_matched_sync(db: &Db, agent_id: &str) -> Result<usize, CcbdError> {
    mark_agent_idle_matched_outcome_sync(db, agent_id).map(|outcome| outcome.changes)
}

fn mark_agent_idle_matched_outcome_sync(
    db: &Db,
    agent_id: &str,
) -> Result<MarkerMatchedSyncOutcome, CcbdError> {
    mark_agent_idle_matched_outcome_sync_inner(db, agent_id, "MARKER_MATCHED")
}

fn mark_agent_idle_matched_outcome_sync_inner(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<MarkerMatchedSyncOutcome, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent idle matched", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version, provider, session_id FROM agents WHERE id = ?",
            params![agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query state for marker matched", err))?;

    let Some((previous_state, state_version, provider, session_id)) = current else {
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    };
    if !is_active_state(previous_state.as_str()) {
        tracing::trace!(agent_id, state = %previous_state, "marker match swallowed: agent not in active state");
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    }

    let dispatched_job_reply = if let Some(job) =
        query_dispatched_job_for_agent_sync(&tx, agent_id)?
    {
        let terminal_pane_is_authoritative =
            crate::provider::adapter(&provider).is_some_and(|adapter| {
                adapter
                    .observation_spec()
                    .uses_terminal_pane_for_turn_state()
            });
        if !terminal_pane_is_authoritative {
            tracing::trace!(
                agent_id,
                provider,
                job_id = %job.id,
                "terminal marker swallowed: provider Adapter requires semantic turn evidence"
            );
            tx.commit()
                .map_err(|err| map_db_error("commit semantic marker refusal", err))?;
            return Ok(MarkerMatchedSyncOutcome {
                changes: 0,
                denial_message: None,
                deferred_nudge: None,
            });
        }
        if crate::completion::registry::contains(agent_id) {
            tracing::trace!(
                agent_id,
                job_id = %job.id,
                "marker match deferred: completion log monitor is authoritative while active"
            );
            tx.commit()
                .map_err(|err| map_db_error("commit deferred marker match", err))?;
            return Ok(MarkerMatchedSyncOutcome {
                changes: 0,
                denial_message: None,
                deferred_nudge: None,
            });
        }
        if let Some(denial_message) = evidence_denial_for_job(&tx, agent_id, &job)? {
            insert_evidence_denied_event(&tx, agent_id, &job.id, &denial_message)?;
            tx.commit()
                .map_err(|err| map_db_error("commit evidence denied marker match", err))?;
            return Ok(MarkerMatchedSyncOutcome {
                changes: 0,
                denial_message: Some(denial_message),
                deferred_nudge: None,
            });
        }
        if let Some(marker_job_id) =
            latest_ah_idle_marker_job_id(&tx, agent_id, job.dispatched_at_seq_id)?
            && marker_job_id != job.id
        {
            tracing::warn!(
                agent_id,
                marker_job_id,
                dispatched_job_id = %job.id,
                "marker match swallowed: ah idle marker job-id mismatch"
            );
            tx.rollback()
                .map_err(|err| map_db_error("rollback marker job-id mismatch", err))?;
            return Ok(MarkerMatchedSyncOutcome {
                changes: 0,
                denial_message: None,
                deferred_nudge: None,
            });
        }
        let reply_text = collect_reply_for_dispatched_job_sync(
            &tx,
            agent_id,
            job.dispatched_at_seq_id,
            &job.prompt_text,
        )?;
        if previous_state == "BUSY" && !job.cancel_requested && is_prompt_only_reply(&reply_text) {
            tracing::trace!(agent_id, "marker match swallowed: prompt-only job reply");
            tx.rollback()
                .map_err(|err| map_db_error("rollback prompt-only marker match", err))?;
            return Ok(MarkerMatchedSyncOutcome {
                changes: 0,
                denial_message: None,
                deferred_nudge: None,
            });
        }

        let state_dir = crate::state_layout::resolve_neutral_state_layout().state_dir;
        let has_pending = crate::completion::parser::check_pending_tasks_from_log_root(
            &state_dir,
            &session_id,
            agent_id,
            &provider,
        );
        let term = crate::completion::parser::classify_terminality(
            &provider,
            &reply_text,
            None,
            Some(&job.prompt_text),
            has_pending,
        );
        if let crate::completion::parser::CompletionTerminality::DeferredBackgroundWork {
            reason: _,
        } = term
        {
            let deferred_nudge = handle_completion_deferral_sync(
                &tx,
                agent_id,
                &job.id,
                &reply_text,
                &previous_state,
                state_version,
            )?;
            tx.commit()
                .map_err(|err| map_db_error("commit deferred marker match", err))?;
            return Ok(MarkerMatchedSyncOutcome {
                changes: 0,
                denial_message: None,
                deferred_nudge,
            });
        }

        Some((job.id, reply_text, job.cancel_requested))
    } else {
        None
    };

    let changes = mark_agent_idle_matched_conn_inner(
        &tx,
        agent_id,
        state_version,
        &previous_state,
        dispatched_job_reply,
        reason,
        false,
    )?;

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent idle matched", err))?;
    Ok(MarkerMatchedSyncOutcome {
        changes,
        denial_message: None,
        deferred_nudge: None,
    })
}

fn mark_agent_idle_matched_conn_inner(
    conn: &Connection,
    agent_id: &str,
    state_version: i64,
    previous_state: &str,
    dispatched_job_reply: Option<(String, String, bool)>,
    reason: &str,
    allow_from_stuck: bool,
) -> Result<usize, CcbdError> {
    let changes = if allow_from_stuck {
        conn.execute(
            "UPDATE agents SET state = 'IDLE', sub_state = 'Matched', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY', 'STUCK') AND state_version = ?",
            params![agent_id, state_version],
        )
    } else {
        conn.execute(
            "UPDATE agents SET state = 'IDLE', sub_state = 'Matched', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY') AND state_version = ?",
            params![agent_id, state_version],
        )
    }
    .map_err(|err| map_db_error("mark agent idle matched", err))?;

    if changes == 1 {
        let completed_job_id = dispatched_job_reply
            .as_ref()
            .map(|(job_id, _, _)| job_id.clone());
        if let Some((job_id, reply_text, cancel_requested)) = dispatched_job_reply {
            if cancel_requested {
                mark_job_cancelled_conn_sync(conn, &job_id, &reply_text)?;
            } else {
                mark_job_completed_conn_sync(conn, &job_id, &reply_text)?;
            }
        }

        let payload = serde_json::json!({
            "from": previous_state,
            "to": "IDLE",
            "sub_state": "Matched",
            "reason": reason,
        })
        .to_string();
        conn.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert marker matched state_change", err))?;
        let observed_at_ms = crate::runtime_observation::store::now_epoch_millis();
        let observation_base = format!("marker:{agent_id}:{state_version}:{reason}");
        if let Some(job_id) = completed_job_id.as_deref() {
            crate::runtime_observation::store::append_for_current_lifecycle_sync(
                conn,
                &format!("{observation_base}:completed"),
                agent_id,
                Some(job_id),
                crate::runtime_observation::EvidenceSource::TerminalPane,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Completed,
                ),
                observed_at_ms,
            )?;
        }
        crate::runtime_observation::store::append_for_current_lifecycle_sync(
            conn,
            &format!("{observation_base}:ready"),
            agent_id,
            None,
            crate::runtime_observation::EvidenceSource::TerminalPane,
            crate::runtime_observation::ProviderObservationKind::Turn(
                crate::runtime_observation::ProviderTurnState::Ready,
            ),
            observed_at_ms,
        )?;
    } else {
        tracing::trace!(agent_id, "marker match swallowed: state_version CAS failed");
    }
    Ok(changes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderActivityOutcome {
    pub state_changed: bool,
    pub observation_inserted: bool,
    pub affected_job: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn mark_agent_working_provider_event_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    expected_lifecycle_id: &str,
    source: crate::runtime_observation::EvidenceSource,
    evidence_id: &str,
    provider_turn_id: Option<&str>,
    observed_prompt_fingerprint: Option<&str>,
) -> Result<ProviderActivityOutcome, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin provider working event", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version, provider, lifecycle_id FROM agents WHERE id = ?1",
            [agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query agent for provider working event", err))?;
    let Some((state, state_version, actual_provider, lifecycle_id)) = current else {
        return Ok(ProviderActivityOutcome {
            state_changed: false,
            observation_inserted: false,
            affected_job: None,
        });
    };
    if crate::provider::manifest::canonicalize_provider_name(&actual_provider)
        != crate::provider::manifest::canonicalize_provider_name(provider)
        || lifecycle_id != expected_lifecycle_id
        || !matches!(state.as_str(), STATE_WAITING_FOR_ACK | STATE_BUSY)
    {
        return Ok(ProviderActivityOutcome {
            state_changed: false,
            observation_inserted: false,
            affected_job: None,
        });
    }
    let Some(job) = query_dispatched_job_for_agent_sync(&tx, agent_id)? else {
        return Ok(ProviderActivityOutcome {
            state_changed: false,
            observation_inserted: false,
            affected_job: None,
        });
    };
    if let Some(observed) = observed_prompt_fingerprint
        && crate::runtime_observation::prompt_fingerprint(&job.prompt_text) != observed
    {
        tracing::warn!(
            agent_id,
            job_id = %job.id,
            provider,
            provider_turn_id,
            "ignored provider activity whose prompt does not match the dispatched job"
        );
        return Ok(ProviderActivityOutcome {
            state_changed: false,
            observation_inserted: false,
            affected_job: None,
        });
    }

    let state_changed = if state == STATE_WAITING_FOR_ACK {
        let changed = crate::db::perception::gate::transit_agent_perception_state_sync(
            &tx,
            agent_id,
            &[STATE_WAITING_FOR_ACK],
            STATE_BUSY,
            "PROVIDER_WORKING_OBSERVED",
            state_version,
        )?;
        if changed {
            tx.execute(
                "UPDATE agents SET sub_state = 'ProviderEvent', updated_at = unixepoch() WHERE id = ?1 AND state = 'BUSY' AND state_version = ?2",
                params![agent_id, state_version + 1],
            )
            .map_err(|err| map_db_error("annotate provider working state", err))?;
        }
        changed
    } else {
        false
    };
    if state == STATE_WAITING_FOR_ACK && !state_changed {
        tx.rollback()
            .map_err(|err| map_db_error("rollback raced provider working event", err))?;
        return Ok(ProviderActivityOutcome {
            state_changed: false,
            observation_inserted: false,
            affected_job: None,
        });
    }

    let observation_id = format!("{lifecycle_id}:{evidence_id}:working");
    let already_observed = tx
        .query_row(
            "SELECT COUNT(*) FROM provider_status_observations WHERE observation_id=?1",
            [&observation_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|err| map_db_error("query duplicate provider working observation", err))?
        > 0;
    let observation_inserted = !already_observed
        && crate::runtime_observation::store::append_for_agent_sync(
            &tx,
            &observation_id,
            agent_id,
            &lifecycle_id,
            Some(&job.id),
            source,
            crate::runtime_observation::ProviderObservationKind::Turn(
                crate::runtime_observation::ProviderTurnState::Working,
            ),
            crate::runtime_observation::store::now_epoch_millis(),
        )?;
    tx.commit()
        .map_err(|err| map_db_error("commit provider working event", err))?;
    Ok(ProviderActivityOutcome {
        state_changed,
        observation_inserted,
        affected_job: Some(job.id),
    })
}

#[cfg(test)]
pub(crate) fn mark_agent_idle_log_event_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    reply: Option<&str>,
    raw_path: &str,
    raw_offset: u64,
    provider_turn_id: Option<&str>,
    expected_lifecycle_id: &str,
    observed_prompt_fingerprint: Option<&str>,
) -> Result<usize, CcbdError> {
    mark_agent_idle_log_event_outcome_sync(
        db,
        agent_id,
        provider,
        reply,
        raw_path,
        raw_offset,
        provider_turn_id,
        expected_lifecycle_id,
        observed_prompt_fingerprint,
    )
    .map(|outcome| outcome.changes)
}

pub(crate) fn mark_agent_idle_hook_event_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    hook_event: &str,
    event_id: Option<&str>,
    reply: Option<&str>,
) -> Result<usize, CcbdError> {
    mark_agent_idle_hook_event_outcome_sync(
        db, agent_id, provider, hook_event, event_id, reply, None, None,
    )
    .map(|outcome| outcome.changes)
}

#[cfg(test)]
pub(crate) fn mark_agent_idle_hook_event_at_version_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    hook_event: &str,
    event_id: Option<&str>,
    reply: Option<&str>,
    expected_state_version: i64,
) -> Result<usize, CcbdError> {
    mark_agent_idle_hook_event_outcome_sync(
        db,
        agent_id,
        provider,
        hook_event,
        event_id,
        reply,
        None,
        Some(expected_state_version),
    )
    .map(|outcome| outcome.changes)
}

fn mark_agent_idle_hook_event_outcome_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    hook_event: &str,
    event_id: Option<&str>,
    reply: Option<&str>,
    expected_lifecycle_id: Option<&str>,
    expected_state_version: Option<i64>,
) -> Result<MarkerMatchedSyncOutcome, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent idle hook event", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version, session_id, lifecycle_id FROM agents WHERE id = ?",
            params![agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query state for hook event matched", err))?;

    let Some((previous_state, state_version, session_id, lifecycle_id)) = current else {
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    };
    if expected_lifecycle_id.is_some_and(|expected| expected != lifecycle_id) {
        tracing::warn!(
            agent_id,
            expected_lifecycle_id,
            current_lifecycle_id = %lifecycle_id,
            "hook completion fenced by provider lifecycle"
        );
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    }
    let late_health_completion_stuck_job = if previous_state == STATE_STUCK {
        if let Some(job) = query_dispatched_job_for_agent_sync(&tx, agent_id)? {
            late_health_completion_stuck_allows_terminal(&tx, agent_id, &job.id)?.then_some(job.id)
        } else {
            None
        }
    } else {
        None
    };
    if previous_state != STATE_WAITING_FOR_ACK
        && previous_state != STATE_BUSY
        && late_health_completion_stuck_job.is_none()
    {
        // A transcript can win the completion race and put the agent back in
        // IDLE before the provider's Stop hook process reaches us. The late
        // hook must not complete the job a second time, but it is still a
        // lifecycle-fenced, official provider observation. Preserve that fact
        // so hook delivery is observable instead of being coupled to whether
        // this callback happened to own the state transition.
        if let Some(expected_lifecycle_id) = expected_lifecycle_id {
            let observation_base = event_id
                .map(str::to_owned)
                .unwrap_or_else(|| format!("unkeyed-hook-{state_version}"));
            crate::runtime_observation::store::append_for_agent_sync(
                &tx,
                &format!("{observation_base}:ready"),
                agent_id,
                expected_lifecycle_id,
                None,
                crate::runtime_observation::EvidenceSource::OfficialHook,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Ready,
                ),
                crate::runtime_observation::store::now_epoch_millis(),
            )?;
            tx.commit()
                .map_err(|err| map_db_error("commit late hook observation", err))?;
        }
        tracing::trace!(agent_id, state = %previous_state, "late hook recorded without repeating completion transition");
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    }
    let Some(cas_version) = expected_state_version.or(Some(state_version)) else {
        unreachable!("state_version is always present for existing agents")
    };

    let dispatched_job_reply =
        if let Some(job) = query_dispatched_job_for_agent_sync(&tx, agent_id)? {
            if let Some(denial_message) = evidence_denial_for_job(&tx, agent_id, &job)? {
                insert_evidence_denied_event(&tx, agent_id, &job.id, &denial_message)?;
                tx.commit()
                    .map_err(|err| map_db_error("commit evidence denied hook event", err))?;
                return Ok(MarkerMatchedSyncOutcome {
                    changes: 0,
                    denial_message: Some(denial_message),
                    deferred_nudge: None,
                });
            }
            let (reply_text, reply_source) = if let Some(reply) = reply {
                (reply.to_string(), "hook")
            } else {
                (
                    collect_reply_for_dispatched_job_sync(
                        &tx,
                        agent_id,
                        job.dispatched_at_seq_id,
                        &job.prompt_text,
                    )?,
                    "screen",
                )
            };

            let state_dir = crate::state_layout::resolve_neutral_state_layout().state_dir;
            let has_pending = crate::completion::parser::check_pending_tasks_from_log_root(
                &state_dir,
                &session_id,
                agent_id,
                provider,
            );
            let term = crate::completion::parser::classify_terminality(
                provider,
                &reply_text,
                None,
                Some(&job.prompt_text),
                has_pending,
            );
            if let crate::completion::parser::CompletionTerminality::DeferredBackgroundWork {
                reason: _,
            } = term
            {
                let deferred_nudge = handle_completion_deferral_sync(
                    &tx,
                    agent_id,
                    &job.id,
                    &reply_text,
                    &previous_state,
                    cas_version,
                )?;
                tx.commit()
                    .map_err(|err| map_db_error("commit deferred hook event", err))?;
                return Ok(MarkerMatchedSyncOutcome {
                    changes: 0,
                    denial_message: None,
                    deferred_nudge,
                });
            }

            Some((job.id, reply_text, reply_source, job.cancel_requested))
        } else {
            None
        };
    let reply_source = dispatched_job_reply
        .as_ref()
        .map(|(_, _, reply_source, _)| *reply_source)
        .unwrap_or_else(|| if reply.is_some() { "hook" } else { "screen" });

    let changes = tx
        .execute(
            "UPDATE agents SET state = 'IDLE', sub_state = 'HookEvent', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('WAITING_FOR_ACK', 'BUSY', 'STUCK') AND state_version = ?",
            params![agent_id, cas_version],
        )
        .map_err(|err| map_db_error("mark agent idle hook event", err))?;

    if changes == 1 {
        let completed_job_id = dispatched_job_reply
            .as_ref()
            .map(|(job_id, _, _, _)| job_id.clone());
        if let Some((job_id, reply_text, _, cancel_requested)) = dispatched_job_reply {
            if cancel_requested {
                mark_job_cancelled_conn_sync(&tx, &job_id, &reply_text)?;
            } else {
                mark_job_completed_conn_sync(&tx, &job_id, &reply_text)?;
            }
        }

        let payload = json!({
            "from": previous_state,
            "to": STATE_IDLE,
            "sub_state": HOOK_EVENT_SUB_STATE,
            "source": "hook",
            "hook_event": hook_event,
            "provider": provider,
            "event_id": event_id,
            "schema_version": 1,
            "reply_source": reply_source,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert hook event state_change", err))?;

        if let Some(expected_lifecycle_id) = expected_lifecycle_id {
            let observation_base = event_id
                .map(str::to_owned)
                .unwrap_or_else(|| format!("unkeyed-hook-{state_version}"));
            let observed_at_ms = crate::runtime_observation::store::now_epoch_millis();
            if let Some(job_id) = completed_job_id.as_deref() {
                crate::runtime_observation::store::append_for_agent_sync(
                    &tx,
                    &format!("{observation_base}:completed"),
                    agent_id,
                    expected_lifecycle_id,
                    Some(job_id),
                    crate::runtime_observation::EvidenceSource::OfficialHook,
                    crate::runtime_observation::ProviderObservationKind::Turn(
                        crate::runtime_observation::ProviderTurnState::Completed,
                    ),
                    observed_at_ms,
                )?;
            }
            crate::runtime_observation::store::append_for_agent_sync(
                &tx,
                &format!("{observation_base}:ready"),
                agent_id,
                expected_lifecycle_id,
                None,
                crate::runtime_observation::EvidenceSource::OfficialHook,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Ready,
                ),
                observed_at_ms,
            )?;
        }
    } else {
        tracing::trace!(
            agent_id,
            "hook completion swallowed: state_version CAS failed"
        );
    }

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent idle hook event", err))?;
    Ok(MarkerMatchedSyncOutcome {
        changes,
        denial_message: None,
        deferred_nudge: None,
    })
}

fn mark_agent_idle_log_event_outcome_sync(
    db: &Db,
    agent_id: &str,
    provider: &str,
    reply: Option<&str>,
    raw_path: &str,
    raw_offset: u64,
    provider_turn_id: Option<&str>,
    expected_lifecycle_id: &str,
    observed_prompt_fingerprint: Option<&str>,
) -> Result<MarkerMatchedSyncOutcome, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent idle log event", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version, session_id, lifecycle_id FROM agents WHERE id = ?",
            params![agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|err| map_db_error("query state for log event matched", err))?;

    let Some((previous_state, state_version, session_id, lifecycle_id)) = current else {
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    };
    if lifecycle_id != expected_lifecycle_id {
        tracing::warn!(
            agent_id,
            expected_lifecycle_id,
            current_lifecycle_id = %lifecycle_id,
            "ignored stale provider transcript from an earlier lifecycle"
        );
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    }
    let late_health_completion_stuck_job = if previous_state == STATE_STUCK {
        if let Some(job) = query_dispatched_job_for_agent_sync(&tx, agent_id)? {
            late_health_completion_stuck_allows_terminal(&tx, agent_id, &job.id)?.then_some(job.id)
        } else {
            None
        }
    } else {
        None
    };
    if previous_state != STATE_WAITING_FOR_ACK
        && previous_state != STATE_BUSY
        && late_health_completion_stuck_job.is_none()
    {
        tracing::trace!(agent_id, state = %previous_state, "log completion swallowed: agent not busy or waiting for ack");
        return Ok(MarkerMatchedSyncOutcome {
            changes: 0,
            denial_message: None,
            deferred_nudge: None,
        });
    }

    let dispatched_job_reply =
        if let Some(job) = query_dispatched_job_for_agent_sync(&tx, agent_id)? {
            if let Some(observed) = observed_prompt_fingerprint
                && crate::runtime_observation::prompt_fingerprint(&job.prompt_text) != observed
            {
                tracing::warn!(
                    agent_id,
                    job_id = %job.id,
                    provider,
                    provider_turn_id,
                    "ignored provider completion whose prompt does not match the dispatched job"
                );
                return Ok(MarkerMatchedSyncOutcome {
                    changes: 0,
                    denial_message: None,
                    deferred_nudge: None,
                });
            }
            if let Some(denial_message) = evidence_denial_for_job(&tx, agent_id, &job)? {
                insert_evidence_denied_event(&tx, agent_id, &job.id, &denial_message)?;
                tx.commit()
                    .map_err(|err| map_db_error("commit evidence denied log event", err))?;
                return Ok(MarkerMatchedSyncOutcome {
                    changes: 0,
                    denial_message: Some(denial_message),
                    deferred_nudge: None,
                });
            }
            let (reply_text, reply_source) = if let Some(reply) = reply {
                (reply.to_string(), "log")
            } else {
                (
                    collect_reply_for_dispatched_job_sync(
                        &tx,
                        agent_id,
                        job.dispatched_at_seq_id,
                        &job.prompt_text,
                    )?,
                    "screen",
                )
            };

            let state_dir = crate::state_layout::resolve_neutral_state_layout().state_dir;
            let has_pending = crate::completion::parser::check_pending_tasks_from_log_root(
                &state_dir,
                &session_id,
                agent_id,
                provider,
            );
            let term = crate::completion::parser::classify_terminality(
                provider,
                &reply_text,
                None,
                Some(&job.prompt_text),
                has_pending,
            );
            if let crate::completion::parser::CompletionTerminality::DeferredBackgroundWork {
                reason: _,
            } = term
            {
                let deferred_nudge = handle_completion_deferral_sync(
                    &tx,
                    agent_id,
                    &job.id,
                    &reply_text,
                    &previous_state,
                    state_version,
                )?;
                tx.commit()
                    .map_err(|err| map_db_error("commit deferred log event", err))?;
                return Ok(MarkerMatchedSyncOutcome {
                    changes: 0,
                    denial_message: None,
                    deferred_nudge,
                });
            }

            Some((job.id, reply_text, reply_source, job.cancel_requested))
        } else {
            None
        };
    let reply_source = dispatched_job_reply
        .as_ref()
        .map(|(_, _, reply_source, _)| *reply_source)
        .unwrap_or_else(|| if reply.is_some() { "log" } else { "screen" });

    let changes = tx
        .execute(
            "UPDATE agents SET state = 'IDLE', sub_state = 'LogEvent', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('WAITING_FOR_ACK', 'BUSY', 'STUCK') AND state_version = ?",
            params![agent_id, state_version],
        )
        .map_err(|err| map_db_error("mark agent idle log event", err))?;

    if changes == 1 {
        let completed_job_id = dispatched_job_reply
            .as_ref()
            .map(|(job_id, _, _, _)| job_id.clone());
        if let Some((job_id, reply_text, _, cancel_requested)) = dispatched_job_reply {
            if cancel_requested {
                mark_job_cancelled_conn_sync(&tx, &job_id, &reply_text)?;
            } else {
                mark_job_completed_conn_sync(&tx, &job_id, &reply_text)?;
            }
        }

        let payload = json!({
            "from": previous_state,
            "to": STATE_IDLE,
            "sub_state": LOG_EVENT_SUB_STATE,
            "reason": LOG_EVENT_TASK_COMPLETE_REASON,
            "provider": provider,
            "raw_path": raw_path,
            "raw_offset": raw_offset,
            "provider_turn_id": provider_turn_id,
            "schema_version": 1,
            "reply_source": reply_source,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert log event state_change", err))?;

        let observation_base = format!("log:{raw_path}:{raw_offset}");
        let observed_at_ms = crate::runtime_observation::store::now_epoch_millis();
        if let Some(job_id) = completed_job_id.as_deref() {
            crate::runtime_observation::store::append_for_agent_sync(
                &tx,
                &format!("{observation_base}:completed"),
                agent_id,
                &lifecycle_id,
                Some(job_id),
                crate::runtime_observation::EvidenceSource::Transcript,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Completed,
                ),
                observed_at_ms,
            )?;
        }
        crate::runtime_observation::store::append_for_agent_sync(
            &tx,
            &format!("{observation_base}:ready"),
            agent_id,
            &lifecycle_id,
            None,
            crate::runtime_observation::EvidenceSource::Transcript,
            crate::runtime_observation::ProviderObservationKind::Turn(
                crate::runtime_observation::ProviderTurnState::Ready,
            ),
            observed_at_ms,
        )?;
    } else {
        tracing::trace!(
            agent_id,
            "log completion swallowed: state_version CAS failed"
        );
    }

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent idle log event", err))?;
    Ok(MarkerMatchedSyncOutcome {
        changes,
        denial_message: None,
        deferred_nudge: None,
    })
}

fn late_health_completion_stuck_allows_terminal(
    conn: &Connection,
    agent_id: &str,
    dispatched_job_id: &str,
) -> Result<bool, CcbdError> {
    let payload = conn
        .query_row(
            "SELECT payload FROM events \
             WHERE agent_id = ? AND event_type = 'state_change' \
             ORDER BY seq_id DESC LIMIT 1",
            params![agent_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| map_db_error("query latest state_change for late completion", err))?;
    let Some(payload) = payload else {
        return Ok(false);
    };
    let payload: Value = serde_json::from_str(&payload).unwrap_or_else(|_| json!({}));
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

fn latest_ah_idle_marker_job_id(
    conn: &Connection,
    agent_id: &str,
    dispatched_at_seq_id: Option<i64>,
) -> Result<Option<String>, CcbdError> {
    let since_seq_id = dispatched_at_seq_id.unwrap_or(0);
    let mut stmt = conn
        .prepare(
            "SELECT payload FROM events WHERE agent_id = ? AND event_type = 'output_chunk' AND seq_id > ? ORDER BY seq_id ASC",
        )
        .map_err(|err| map_db_error("prepare query ah idle marker", err))?;
    let rows = stmt
        .query_map(params![agent_id, since_seq_id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|err| map_db_error("query ah idle marker", err))?;

    let mut marker = None;
    for row in rows {
        let payload = row.map_err(|err| map_db_error("read ah idle marker payload", err))?;
        let text = serde_json::from_str::<Value>(&payload)
            .ok()
            .and_then(|value| {
                value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or(payload);
        if let Some(job_id) = extract_ah_idle_marker_job_id(&text) {
            marker = Some(job_id);
        }
    }
    Ok(marker)
}

pub fn extract_ah_idle_marker_job_id(text: &str) -> Option<String> {
    let start = text.find("<<ah-idle:job-id=")? + "<<ah-idle:job-id=".len();
    let rest = &text[start..];
    let end = rest.find(">>")?;
    let job_id = rest[..end].trim();
    if job_id.is_empty() {
        None
    } else {
        Some(job_id.to_string())
    }
}

fn evidence_denial_for_job(
    conn: &Connection,
    agent_id: &str,
    job: &crate::db::schema::Job,
) -> Result<Option<String>, CcbdError> {
    if job.requires_physical_evidence
        && !has_job_evidence_sync(
            conn,
            agent_id,
            &job.id,
            &["mtime_changed", "diff_generated"],
        )?
    {
        return Ok(Some(EVIDENCE_DENY_MESSAGE.to_string()));
    }
    if job.requires_test_evidence
        && !has_job_evidence_sync(conn, agent_id, &job.id, &["test_passed"])?
    {
        return Ok(Some(format!(
            "{EVIDENCE_DENY_MESSAGE} Missing required evidence_type=test_passed."
        )));
    }
    Ok(None)
}

fn insert_evidence_denied_event(
    conn: &Connection,
    agent_id: &str,
    job_id: &str,
    message: &str,
) -> Result<(), CcbdError> {
    let payload = json!({
        "job_id": job_id,
        "reason": "missing_physical_evidence",
        "message": message,
    })
    .to_string();
    conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'evidence_denied', ?)",
        params![agent_id, payload],
    )
    .map_err(|err| map_db_error("insert evidence denied event", err))?;
    Ok(())
}

fn is_prompt_only_reply(reply_text: &str) -> bool {
    let stripped = strip_ansi_escapes(reply_text);
    let text = stripped.trim();
    if text.is_empty() {
        return true;
    }
    text.len() <= 4 && matches!(text, "$" | "#" | ">" | "✦" | "❯" | "›" | "▌" | "%")
}

fn recapture_content_hash(content: &str) -> String {
    format!("{:016x}", crate::pane_diff::compute_content_hash(content))
}

fn has_completion_deferred_event(
    conn: &Connection,
    agent_id: &str,
    job_id: &str,
    candidate_hash: &str,
) -> Result<bool, CcbdError> {
    let mut stmt = conn
        .prepare(
            "SELECT payload FROM events WHERE agent_id = ? AND event_type = 'completion_deferred'",
        )
        .map_err(|err| map_db_error("prepare query completion_deferred events", err))?;
    let rows = stmt
        .query_map(params![agent_id], |row| row.get::<_, String>(0))
        .map_err(|err| map_db_error("query completion_deferred events", err))?;

    for row in rows {
        let payload_str =
            row.map_err(|err| map_db_error("read completion_deferred payload", err))?;
        if let Ok(payload) = serde_json::from_str::<Value>(&payload_str) {
            let j_id = payload.get("job_id").and_then(Value::as_str);
            let c_hash = payload.get("candidate_hash").and_then(Value::as_str);
            if j_id == Some(job_id) && c_hash == Some(candidate_hash) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn insert_completion_deferred_event(
    conn: &Connection,
    agent_id: &str,
    job_id: &str,
    reason: &str,
    candidate_hash: &str,
) -> Result<(), CcbdError> {
    let payload = json!({
        "job_id": job_id,
        "reason": reason,
        "candidate_hash": candidate_hash,
    });
    conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'completion_deferred', ?)",
        params![agent_id, payload.to_string()],
    )
    .map_err(|err| map_db_error("insert completion deferred event", err))?;
    Ok(())
}

fn handle_completion_deferral_sync(
    conn: &Connection,
    agent_id: &str,
    job_id: &str,
    candidate_reply: &str,
    previous_state: &str,
    state_version: i64,
) -> Result<Option<String>, CcbdError> {
    let hash = recapture_content_hash(candidate_reply);

    let already_nudged = has_completion_deferred_event(conn, agent_id, job_id, &hash)?;

    insert_completion_deferred_event(
        conn,
        agent_id,
        job_id,
        "ANTIGRAVITY_DEFERRED_BACKGROUND_WORK",
        &hash,
    )?;

    let changes = conn
        .execute(
            "UPDATE agents SET state = 'BUSY', sub_state = 'Deferred', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('WAITING_FOR_ACK', 'BUSY', 'STUCK') AND state_version = ?",
            params![agent_id, state_version],
        )
        .map_err(|err| map_db_error("update agent state for deferral", err))?;

    if changes == 1 {
        let payload = json!({
            "from": previous_state,
            "to": "BUSY",
            "sub_state": "Deferred",
            "reason": "ANTIGRAVITY_DEFERRED_BACKGROUND_WORK",
            "job_id": job_id,
            "schema_version": 1
        });
        conn.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload.to_string()],
        )
        .map_err(|err| map_db_error("insert deferral state_change", err))?;
    }

    if !already_nudged {
        Ok(Some("The job is still open. Wait for the background command to finish, then report the final test result. Do not stop at 'waiting for cargo test'.".to_string()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
pub(crate) fn mark_agent_stuck_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    mark_agent_stuck_outcome_sync(db, agent_id, reason).map(|outcome| outcome.changes)
}

fn mark_agent_stuck_outcome_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<StuckOutcome, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent stuck", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query state for stuck", err))?;

    let Some((previous_state, state_version)) = current else {
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark stuck missing agent", err))?;
        return Ok(StuckOutcome {
            changes: 0,
            agent_id: agent_id.to_string(),
            job_id: None,
            from_state: None,
            event_seq_id: None,
            job_resolution: "NONE".to_string(),
        });
    };
    if previous_state != STATE_BUSY && previous_state != STATE_WAITING_FOR_ACK {
        tracing::trace!(agent_id, state = %previous_state, "pane diff stuck swallowed: agent not busy or waiting for ack");
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark stuck ignored state", err))?;
        return Ok(StuckOutcome {
            changes: 0,
            agent_id: agent_id.to_string(),
            job_id: None,
            from_state: Some(previous_state),
            event_seq_id: None,
            job_resolution: "NONE".to_string(),
        });
    }
    let affected_job = query_dispatched_job_for_agent_sync(&tx, agent_id)?.map(|job| job.id);

    let changes = tx
        .execute(
            "UPDATE agents SET state = 'STUCK', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('BUSY', 'WAITING_FOR_ACK') AND state_version = ?",
            params![agent_id, state_version],
        )
        .map_err(|err| map_db_error("mark agent stuck", err))?;

    if changes == 1 {
        let job_resolution = if let Some(job_id) = affected_job.as_deref() {
            tx.execute(
                "UPDATE jobs
                 SET error_reason = ?
                 WHERE id = ?
                   AND agent_id = ?
                   AND status = 'DISPATCHED'",
                params![reason, job_id, agent_id],
            )
            .map_err(|err| map_db_error("fail dispatched job for stuck agent error_reason", err))?;
            crate::db::job_state::transit_job_state(
                &tx,
                job_id,
                crate::db::job_state::JobStatus::Dispatched,
                crate::db::job_state::JobStatus::Failed,
                reason,
            )?;
            tx.execute(
                "INSERT INTO events (agent_id, request_id, event_type, payload)
                 VALUES (?, NULL, 'job_resolution', ?)",
                params![
                    agent_id,
                    json!({
                        "job_id": job_id,
                        "job_resolution": "FAILED",
                        "reason": reason,
                        "source": "mark_agent_stuck",
                    })
                    .to_string()
                ],
            )
            .map_err(|err| map_db_error("insert stuck job_resolution", err))?;
            "FAILED"
        } else {
            "NONE"
        };
        let payload = json!({
            "from": previous_state,
            "to": "STUCK",
            "reason": reason,
            "job_id": affected_job,
            "job_resolution": job_resolution,
            "signal_kinds": ["state_machine"],
            "elapsed_secs": 0,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert stuck state_change", err))?;
        let event_seq_id = tx.last_insert_rowid();
        crate::runtime_observation::store::append_for_current_lifecycle_sync(
            &tx,
            &format!("stuck:{agent_id}:{state_version}:{reason}"),
            agent_id,
            None,
            crate::runtime_observation::EvidenceSource::ControlPlane,
            crate::runtime_observation::ProviderObservationKind::Turn(
                crate::runtime_observation::ProviderTurnState::Stalled,
            ),
            crate::runtime_observation::store::now_epoch_millis(),
        )?;
        tx.commit()
            .map_err(|err| map_db_error("commit mark agent stuck", err))?;
        return Ok(StuckOutcome {
            changes,
            agent_id: agent_id.to_string(),
            job_id: affected_job,
            from_state: Some(previous_state),
            event_seq_id: Some(event_seq_id),
            job_resolution: job_resolution.to_string(),
        });
    } else {
        tracing::trace!(
            agent_id,
            "pane diff stuck swallowed: state_version CAS failed"
        );
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark stuck CAS failed", err))?;
        return Ok(StuckOutcome {
            changes: 0,
            agent_id: agent_id.to_string(),
            job_id: affected_job,
            from_state: Some(previous_state),
            event_seq_id: None,
            job_resolution: "NONE".to_string(),
        });
    }
}

pub(crate) fn mark_agent_unknown_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
    pane_bytes: Vec<u8>,
    failed_rules: Value,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent unknown", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query state for unknown", err))?;

    let Some((previous_state, state_version)) = current else {
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark unknown missing agent", err))?;
        return Ok(0);
    };
    if !is_active_state(previous_state.as_str()) {
        tracing::trace!(agent_id, state = %previous_state, "marker timeout swallowed: agent not in active state");
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark unknown ignored state", err))?;
        return Ok(0);
    }

    let changes = tx
        .execute(
            "UPDATE agents SET state = 'UNKNOWN', error_code = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('SPAWNING', 'WAITING_FOR_ACK', 'BUSY') AND state_version = ?",
            params![reason, agent_id, state_version],
        )
        .map_err(|err| map_db_error("mark agent unknown", err))?;

    if changes == 1 {
        tx.execute(
            "UPDATE evidence SET status = 'SEALED' WHERE agent_id = ? AND status = 'PENDING'",
            params![agent_id],
        )
        .map_err(|err| map_db_error("seal pending evidence", err))?;

        let payload = json!({
            "from": previous_state,
            "to": "UNKNOWN",
            "reason": reason,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert unknown state_change", err))?;
        let event_seq_id = tx.last_insert_rowid();
        let evidence_id = format!("evi_{}", uuid::Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO evidence (id, agent_id, event_seq_id, pane_bytes, failed_rules) VALUES (?, ?, ?, ?, ?)",
            params![evidence_id, agent_id, event_seq_id, pane_bytes, failed_rules.to_string()],
        )
        .map_err(|err| map_db_error("insert evidence", err))?;
        if let Some(job) = query_dispatched_job_for_agent_sync(&tx, agent_id)? {
            if job.cancel_requested {
                mark_job_cancelled_conn_sync(&tx, &job.id, "")?;
            } else {
                mark_dispatched_jobs_failed_for_agent_conn_sync(&tx, agent_id, reason)?;
            }
        }
    } else {
        tracing::trace!(
            agent_id,
            "marker timeout swallowed: state_version CAS failed"
        );
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark unknown CAS failed", err))?;
        return Ok(0);
    }

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent unknown", err))?;
    Ok(changes)
}

pub(crate) fn mark_agent_failed_from_intervention_sync(
    db: &Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin mark agent failed", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query state for failed", err))?;

    let Some((previous_state, state_version)) = current else {
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark failed missing agent", err))?;
        return Ok(0);
    };
    if previous_state != STATE_SPAWNING_INTERVENTION {
        tx.rollback()
            .map_err(|err| map_db_error("rollback mark failed ignored state", err))?;
        return Ok(0);
    }

    let changes = tx
        .execute(
            "UPDATE agents SET state = ?, error_code = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state = ? AND state_version = ?",
            params![
                STATE_FAILED,
                reason,
                agent_id,
                STATE_SPAWNING_INTERVENTION,
                state_version
            ],
        )
        .map_err(|err| map_db_error("mark agent failed", err))?;

    if changes == 1 {
        let payload = json!({
            "from": previous_state,
            "to": STATE_FAILED,
            "reason": reason,
        })
        .to_string();
        tx.execute(
            "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
            params![agent_id, payload],
        )
        .map_err(|err| map_db_error("insert failed state_change", err))?;
    }

    tx.commit()
        .map_err(|err| map_db_error("commit mark agent failed", err))?;
    Ok(changes)
}

pub async fn mark_agent_unknown(
    db: Db,
    agent_id: String,
    reason: String,
    pane_bytes: Vec<u8>,
    failed_rules: Value,
) -> Result<(usize, Option<String>), CcbdError> {
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let result = spawn_db("state_machine::mark_agent_unknown", move || {
        mark_agent_unknown_sync(&db, &agent_id, &reason, pane_bytes, failed_rules)
    })
    .await;
    result.map(|changes| {
        if changes > 0 {
            notify_runtime_agent_changed();
        }
        let affected_job = if changes > 0 { affected_job } else { None };
        if let Some(job_id) = affected_job.as_deref() {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        (changes, affected_job)
    })
}

pub async fn mark_agent_failed_from_intervention(
    db: Db,
    agent_id: String,
    reason: String,
) -> Result<usize, CcbdError> {
    let result = spawn_db(
        "state_machine::mark_agent_failed_from_intervention",
        move || mark_agent_failed_from_intervention_sync(&db, &agent_id, &reason),
    )
    .await;
    if matches!(result, Ok(changes) if changes > 0) {
        notify_runtime_agent_changed();
    }
    result
}

pub async fn mark_agent_stuck(
    db: Db,
    agent_id: String,
    reason: String,
) -> Result<usize, CcbdError> {
    let outcome = spawn_db("state_machine::mark_agent_stuck", move || {
        mark_agent_stuck_outcome_sync(&db, &agent_id, &reason)
    })
    .await?;
    if outcome.changes > 0 {
        notify_runtime_agent_changed();
        if let Some(job_id) = outcome.job_id.as_deref() {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        let payload = json!({
            "job_id": outcome.job_id,
            "job_resolution": outcome.job_resolution,
            "signal_kinds": ["state_machine"],
            "elapsed_secs": 0,
        });
        crate::orchestrator::pubsub::notify_event(crate::orchestrator::pubsub::EventFrame {
            event_id: outcome.event_seq_id.unwrap_or(0),
            kind: "stuck".to_string(),
            agent_id: outcome.agent_id.clone(),
            job_id: outcome.job_id.clone(),
            state: Some(STATE_STUCK.to_string()),
            ts_unix_micro: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_micros() as i64)
                .unwrap_or(0),
            payload: Some(payload),
        });
    }
    Ok(outcome.changes)
}

pub async fn mark_agent_waiting_for_ack(
    db: Db,
    agent_id: String,
    expected_version: i64,
) -> Result<bool, CcbdError> {
    let result = spawn_db("state_machine::mark_agent_waiting_for_ack", move || {
        let mut conn = db.conn();
        mark_agent_waiting_for_ack_sync(&mut conn, &agent_id, expected_version)
    })
    .await;
    if matches!(result, Ok(true)) {
        notify_runtime_agent_changed();
    }
    result
}

pub async fn mark_agent_prompt_pending(
    db: Db,
    agent_id: String,
    reason: String,
) -> Result<usize, CcbdError> {
    let result = spawn_db("state_machine::mark_agent_prompt_pending", move || {
        mark_agent_prompt_pending_sync(&db, &agent_id, &reason)
    })
    .await;
    if matches!(result, Ok(changes) if changes > 0) {
        notify_runtime_agent_changed();
    }
    result
}

pub async fn transit_agent_state(
    db: Db,
    agent_id: String,
    from_state_list: Vec<String>,
    to_state: String,
    reason: Option<String>,
) -> Result<(), CcbdError> {
    let result = spawn_db("state_machine::transit_agent_state", move || {
        let mut conn = db.conn();
        let from_states = from_state_list
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        transit_agent_state_sync(
            &mut conn,
            &agent_id,
            &from_states,
            &to_state,
            reason.as_deref(),
        )
    })
    .await;
    if result.is_ok() {
        notify_runtime_agent_changed();
    }
    result
}

pub async fn mark_agent_idle_matched(
    db: Db,
    agent_id: String,
) -> Result<(usize, Option<String>), CcbdError> {
    let outcome = mark_agent_idle_matched_outcome(db, agent_id).await?;
    Ok((outcome.changes, outcome.affected_job))
}

pub async fn mark_agent_idle_log_event(
    db: Db,
    agent_id: String,
    provider: String,
    reply: Option<String>,
    raw_path: String,
    raw_offset: u64,
    provider_turn_id: Option<String>,
    expected_lifecycle_id: String,
    observed_prompt_fingerprint: Option<String>,
) -> Result<(usize, Option<String>), CcbdError> {
    let outcome = mark_agent_idle_log_event_outcome(
        db,
        agent_id,
        provider,
        reply,
        raw_path,
        raw_offset,
        provider_turn_id,
        expected_lifecycle_id,
        observed_prompt_fingerprint,
    )
    .await?;
    Ok((outcome.changes, outcome.affected_job))
}

pub async fn mark_agent_idle_hook_event(
    db: Db,
    agent_id: String,
    provider: String,
    hook_event: String,
    event_id: Option<String>,
    reply: Option<String>,
    expected_lifecycle_id: Option<String>,
) -> Result<(usize, Option<String>), CcbdError> {
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let agent_id_for_denial = agent_id.clone();
    let provider_for_nudge = provider.clone();
    spawn_db("state_machine::mark_agent_idle_hook_event", move || {
        mark_agent_idle_hook_event_outcome_sync(
            &db,
            &agent_id,
            &provider,
            &hook_event,
            event_id.as_deref(),
            reply.as_deref(),
            expected_lifecycle_id.as_deref(),
            None,
        )
    })
    .await
    .and_then(|outcome| {
        if outcome.changes > 0 {
            notify_runtime_agent_changed();
        }
        if let Some(message) = &outcome.denial_message {
            let message = message.clone();
            let agent_id = agent_id_for_denial.clone();
            let provider = provider_for_nudge.clone();
            tokio::spawn(send_evidence_denial_nudge(agent_id, provider, message));
        }
        if let Some(nudge) = &outcome.deferred_nudge {
            let nudge = nudge.clone();
            let agent_id = agent_id_for_denial.clone();
            let provider = provider_for_nudge.clone();
            tokio::spawn(send_deferred_nudge(agent_id, provider, nudge));
        }
        let affected_job = if outcome.changes > 0 {
            affected_job
        } else {
            None
        };
        if let Some(job_id) = affected_job.as_deref() {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        Ok((outcome.changes, affected_job))
    })
}

async fn send_evidence_denial_nudge(agent_id: String, provider: String, message: String) {
    #[cfg(test)]
    record_test_denial_nudge(&agent_id, &message);
    if let Err(err) =
        crate::agent_io::send_text_to_registered_pane(&agent_id, &provider, message).await
    {
        tracing::warn!(agent_id = %agent_id, error = %err, "failed to inject evidence denial");
    }
}

async fn send_deferred_nudge(agent_id: String, provider: String, message: String) {
    if let Err(err) =
        crate::agent_io::send_text_to_registered_pane(&agent_id, &provider, message).await
    {
        tracing::warn!(agent_id = %agent_id, error = %err, "failed to inject deferred nudge");
    }
}

#[cfg(test)]
static TEST_DENIAL_NUDGES: std::sync::LazyLock<std::sync::Mutex<Vec<(String, String)>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));

#[cfg(test)]
fn record_test_denial_nudge(agent_id: &str, message: &str) {
    if let Ok(mut nudges) = TEST_DENIAL_NUDGES.lock() {
        nudges.push((agent_id.to_string(), message.to_string()));
    }
}

#[cfg(test)]
fn clear_test_denial_nudges() {
    TEST_DENIAL_NUDGES.lock().unwrap().clear();
}

#[cfg(test)]
fn test_denial_nudges() -> Vec<(String, String)> {
    TEST_DENIAL_NUDGES.lock().unwrap().clone()
}

pub async fn mark_agent_idle_matched_outcome(
    db: Db,
    agent_id: String,
) -> Result<MarkerMatchedOutcome, CcbdError> {
    let provider_for_nudge = crate::db::agents::query_agent(db.clone(), agent_id.clone())
        .await?
        .map(|agent| agent.provider)
        .unwrap_or_else(|| "bash".to_string());
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let agent_id_for_denial = agent_id.clone();
    spawn_db("state_machine::mark_agent_idle_matched", move || {
        mark_agent_idle_matched_outcome_sync(&db, &agent_id)
    })
    .await
    .and_then(|outcome| {
        if outcome.changes > 0 {
            notify_runtime_agent_changed();
        }
        if let Some(message) = &outcome.denial_message {
            let message = message.clone();
            let agent_id = agent_id_for_denial.clone();
            let provider = provider_for_nudge.clone();
            tokio::spawn(send_evidence_denial_nudge(agent_id, provider, message));
        }
        if let Some(nudge) = &outcome.deferred_nudge {
            let nudge = nudge.clone();
            let agent_id = agent_id_for_denial.clone();
            let provider = provider_for_nudge.clone();
            tokio::spawn(send_deferred_nudge(agent_id, provider, nudge));
        }
        let affected_job = if outcome.changes > 0 {
            affected_job
        } else {
            None
        };
        if let Some(job_id) = affected_job.as_deref() {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        Ok(MarkerMatchedOutcome {
            changes: outcome.changes,
            affected_job,
            denial_message: outcome.denial_message,
            deferred_nudge: outcome.deferred_nudge,
        })
    })
}

pub async fn mark_agent_idle_log_event_outcome(
    db: Db,
    agent_id: String,
    provider: String,
    reply: Option<String>,
    raw_path: String,
    raw_offset: u64,
    provider_turn_id: Option<String>,
    expected_lifecycle_id: String,
    observed_prompt_fingerprint: Option<String>,
) -> Result<MarkerMatchedOutcome, CcbdError> {
    let affected_job =
        crate::db::jobs::query_dispatched_job_for_agent(db.clone(), agent_id.clone())
            .await?
            .map(|job| job.id);
    let agent_id_for_denial = agent_id.clone();
    let provider_for_nudge = provider.clone();
    spawn_db("state_machine::mark_agent_idle_log_event", move || {
        mark_agent_idle_log_event_outcome_sync(
            &db,
            &agent_id,
            &provider,
            reply.as_deref(),
            &raw_path,
            raw_offset,
            provider_turn_id.as_deref(),
            &expected_lifecycle_id,
            observed_prompt_fingerprint.as_deref(),
        )
    })
    .await
    .and_then(|outcome| {
        if outcome.changes > 0 {
            notify_runtime_agent_changed();
        }
        if let Some(message) = &outcome.denial_message {
            let message = message.clone();
            let agent_id = agent_id_for_denial.clone();
            let provider = provider_for_nudge.clone();
            tokio::spawn(send_evidence_denial_nudge(agent_id, provider, message));
        }
        if let Some(nudge) = &outcome.deferred_nudge {
            let nudge = nudge.clone();
            let agent_id = agent_id_for_denial.clone();
            let provider = provider_for_nudge.clone();
            tokio::spawn(send_deferred_nudge(agent_id, provider, nudge));
        }
        let affected_job = if outcome.changes > 0 {
            affected_job
        } else {
            None
        };
        if let Some(job_id) = affected_job.as_deref() {
            crate::orchestrator::pubsub::notify_job_update(job_id);
        }
        Ok(MarkerMatchedOutcome {
            changes: outcome.changes,
            affected_job,
            denial_message: outcome.denial_message,
            deferred_nudge: outcome.deferred_nudge,
        })
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn mark_agent_working_provider_event(
    db: Db,
    agent_id: String,
    provider: String,
    expected_lifecycle_id: String,
    source: crate::runtime_observation::EvidenceSource,
    evidence_id: String,
    provider_turn_id: Option<String>,
    observed_prompt_fingerprint: Option<String>,
) -> Result<ProviderActivityOutcome, CcbdError> {
    spawn_db(
        "state_machine::mark_agent_working_provider_event",
        move || {
            mark_agent_working_provider_event_sync(
                &db,
                &agent_id,
                &provider,
                &expected_lifecycle_id,
                source,
                &evidence_id,
                provider_turn_id.as_deref(),
                observed_prompt_fingerprint.as_deref(),
            )
        },
    )
    .await
    .inspect(|outcome| {
        if outcome.state_changed || outcome.observation_inserted {
            notify_runtime_agent_changed();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        STATE_BUSY, STATE_CRASHED, STATE_FAILED, STATE_IDLE, STATE_KILLED, STATE_PROMPT_PENDING,
        STATE_SPAWNING, STATE_SPAWNING_INTERVENTION, STATE_STUCK, STATE_UNKNOWN,
        STATE_WAITING_FOR_ACK, is_active_state, mark_agent_idle_log_event_sync,
        mark_agent_idle_matched_sync, mark_agent_prompt_pending_sync, mark_agent_stuck_sync,
        mark_agent_unknown_sync, transit_agent_state_sync,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::jobs::{
        claim_next_job_sync, insert_job_sync, query_job_sync, request_dispatched_job_cancel_sync,
        set_job_evidence_requirements_sync, update_dispatched_seq_id_sync,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};
    use crate::error::CcbdError;
    use rusqlite::params;

    fn with_test_db_handle<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_busy_agent(db: &Db, agent_id: &str) {
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_assert", "p_assert", "/tmp/assert").unwrap();
            insert_agent_sync(&conn, agent_id, "s_assert", "bash", "BUSY", Some(1)).unwrap();
        }
    }

    fn seed_dispatched_agent_job(db: &Db, agent_id: &str, state: &str, job_id: &str) -> i64 {
        {
            let conn = db.conn();
            insert_session_sync(
                &conn,
                &format!("s_{agent_id}"),
                &format!("p_{agent_id}"),
                "/tmp/log-event",
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                agent_id,
                &format!("s_{agent_id}"),
                "bash",
                STATE_IDLE,
                Some(1),
            )
            .unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, "echo PONG\n").unwrap();
        }
        claim_next_job_sync(db, agent_id).unwrap().unwrap();
        let conn = db.conn();
        let dispatch_seq = insert_event_sync(
            &conn,
            agent_id,
            None,
            "command_received",
            r#"{"cmd":"echo PONG\n","status":"SENT"}"#,
        )
        .unwrap();
        update_dispatched_seq_id_sync(&conn, job_id, dispatch_seq).unwrap();
        conn.execute(
            "UPDATE agents SET state = ?, state_version = state_version + 1 WHERE id = ?",
            rusqlite::params![state, agent_id],
        )
        .unwrap();
        dispatch_seq
    }

    fn lifecycle_id(db: &Db, agent_id: &str) -> String {
        db.conn()
            .query_row(
                "SELECT lifecycle_id FROM agents WHERE id = ?1",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn job_transition_count(db: &Db, job_id: &str, old_status: &str, new_status: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*)
                 FROM job_transitions
                 WHERE job_id = ?1 AND old_status = ?2 AND new_status = ?3",
                params![job_id, old_status, new_status],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn state_change_count(db: &Db, agent_id: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change'",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn insert_health_completion_stuck_event(db: &Db, agent_id: &str, job_id: &str) {
        insert_event_sync(
            &db.conn(),
            agent_id,
            None,
            "state_change",
            &serde_json::json!({
                "from": STATE_BUSY,
                "to": STATE_STUCK,
                "reason": "HEALTH_CHECK_STUCK",
                "job_id": job_id,
                "signal_kinds": ["health:completion"],
                "elapsed_secs": 390,
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn test_transit_agent_state_sync_updates_agent_and_emits_event() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_transit", "p_transit", "/tmp/transit").unwrap();
                insert_agent_sync(
                    &conn,
                    "a_transit",
                    "s_transit",
                    "bash",
                    STATE_SPAWNING,
                    Some(1),
                )
                .unwrap();
            }

            let mut conn = db.conn();
            transit_agent_state_sync(
                &mut conn,
                "a_transit",
                &[STATE_SPAWNING],
                STATE_IDLE,
                Some("INIT_READY"),
            )
            .unwrap();
            let (state, version, payload): (String, i64, String) = conn
                .query_row(
                    "SELECT agents.state, agents.state_version, events.payload \
                     FROM agents JOIN events ON events.agent_id = agents.id \
                     WHERE agents.id = 'a_transit' AND events.event_type = 'state_change'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(state, STATE_IDLE);
            assert_eq!(version, 2);
            assert_eq!(payload["from"], STATE_SPAWNING);
            assert_eq!(payload["to"], STATE_IDLE);
            assert_eq!(payload["reason"], "INIT_READY");
        });
    }

    #[test]
    fn test_transit_agent_state_sync_rejects_invalid_from_state_without_event() {
        with_test_db_handle(|db| {
            seed_busy_agent(db, "a_invalid_from");

            let mut conn = db.conn();
            let err = transit_agent_state_sync(
                &mut conn,
                "a_invalid_from",
                &[STATE_IDLE],
                STATE_SPAWNING,
                Some("invalid_test_transition"),
            )
            .unwrap_err();
            let state: String = conn
                .query_row(
                    "SELECT state FROM agents WHERE id = 'a_invalid_from'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            assert!(matches!(err, CcbdError::AgentWrongState { .. }));
            assert_eq!(state, STATE_BUSY);
            drop(conn);
            assert_eq!(state_change_count(db, "a_invalid_from"), 0);
        });
    }

    #[test]
    fn test_transit_agent_state_sync_missing_agent_errors_without_event() {
        with_test_db_handle(|db| {
            let mut conn = db.conn();
            let err = transit_agent_state_sync(
                &mut conn,
                "a_missing",
                &[STATE_IDLE],
                STATE_SPAWNING,
                Some("missing_test_transition"),
            )
            .unwrap_err();

            assert!(matches!(err, CcbdError::AgentNotFound(agent) if agent == "a_missing"));
            drop(conn);
            assert_eq!(state_change_count(db, "a_missing"), 0);
        });
    }

    #[test]
    fn test_mark_agent_stuck_from_busy_succeeds() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_stuck", STATE_BUSY, "job_stuck_transition");

            let changes = mark_agent_stuck_sync(db, "a_stuck", "PANE_DIFF_STUCK").unwrap();
            let state: String = db
                .conn()
                .query_row("SELECT state FROM agents WHERE id = 'a_stuck'", [], |row| {
                    row.get(0)
                })
                .unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, "STUCK");
            assert_eq!(
                job_transition_count(db, "job_stuck_transition", "DISPATCHED", "FAILED"),
                1
            );
        });
    }

    #[test]
    fn test_mark_agent_stuck_from_idle_swallowed() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_idle", "p_idle", "/tmp/idle").unwrap();
                insert_agent_sync(&conn, "a_idle", "s_idle", "bash", "IDLE", Some(1)).unwrap();
            }

            let changes = mark_agent_stuck_sync(db, "a_idle", "PANE_DIFF_STUCK").unwrap();
            let state: String = db
                .conn()
                .query_row("SELECT state FROM agents WHERE id = 'a_idle'", [], |row| {
                    row.get(0)
                })
                .unwrap();

            assert_eq!(changes, 0);
            assert_eq!(state, "IDLE");
        });
    }

    #[test]
    fn test_mark_agent_stuck_state_version_cas() {
        with_test_db_handle(|db| {
            seed_busy_agent(db, "a_cas");

            assert_eq!(
                mark_agent_stuck_sync(db, "a_cas", "PANE_DIFF_STUCK").unwrap(),
                1
            );
            assert_eq!(
                mark_agent_stuck_sync(db, "a_cas", "PANE_DIFF_STUCK").unwrap(),
                0
            );

            let (state, state_version, event_count): (String, i64, i64) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.state_version, COUNT(events.seq_id) \
                     FROM agents \
                     LEFT JOIN events ON events.agent_id = agents.id AND events.event_type = 'state_change' \
                     WHERE agents.id = 'a_cas' \
                     GROUP BY agents.id",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(state, "STUCK");
            assert_eq!(state_version, 2);
            assert_eq!(event_count, 1);
        });
    }

    #[test]
    fn test_mark_agent_stuck_emits_state_change_event() {
        with_test_db_handle(|db| {
            seed_busy_agent(db, "a_event");

            mark_agent_stuck_sync(db, "a_event", "PANE_DIFF_STUCK").unwrap();
            let payload: String = db
                .conn()
                .query_row(
                    "SELECT payload FROM events WHERE agent_id = 'a_event' AND event_type = 'state_change'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(payload["from"], "BUSY");
            assert_eq!(payload["to"], "STUCK");
            assert_eq!(payload["reason"], "PANE_DIFF_STUCK");
        });
    }

    /// Fail-closed guard (A2 cleanup — migrated out of the deleted
    /// `test_ui_recapture_can_mark_stuck_agent_idle_without_opening_live_marker_guard`,
    /// which exercised the now-removed `mark_agent_idle_recaptured*` family).
    ///
    /// Independent surviving contract: the LIVE marker-match completion path
    /// (`mark_agent_idle_matched_sync`) must NOT complete a STUCK agent. A STUCK
    /// agent has been declared hung; a stray live-marker match must never silently
    /// resurrect it to IDLE/COMPLETED. Enforced by the `is_active_state` guard
    /// (STATE_STUCK is not active). This was the *only* coverage of that guard for
    /// a non-active state, so it is preserved here rather than dropped with the
    /// recapture test. GREEN regression guard — behavior is already live.
    #[test]
    fn test_live_marker_match_does_not_complete_stuck_agent() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_matched_stuck", STATE_STUCK, "job_matched_stuck");
            insert_event_sync(
                &db.conn(),
                "a_matched_stuck",
                None,
                "output_chunk",
                r#"{"text":"stray marker reply\n"}"#,
            )
            .unwrap();

            let changes = mark_agent_idle_matched_sync(db, "a_matched_stuck").unwrap();

            let (state, status): (String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status \
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id \
                     WHERE agents.id = 'a_matched_stuck'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();

            assert_eq!(
                changes, 0,
                "live marker match must not act on a STUCK agent"
            );
            assert_eq!(
                state, STATE_STUCK,
                "STUCK agent must stay STUCK (fail-closed: no silent completion)"
            );
            assert_eq!(
                status, "DISPATCHED",
                "STUCK agent's job must not be completed by a live marker match"
            );
        });
    }

    #[test]
    fn test_mark_agent_unknown_writes_evidence() {
        with_test_db_handle(|db| {
            seed_busy_agent(db, "a_assert");
            mark_agent_unknown_sync(
                db,
                "a_assert",
                "PTY_MARKER_TIMEOUT",
                b"pane".to_vec(),
                serde_json::json!(["rule"]),
            )
            .unwrap();
            let (state, evidence_status): (String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, evidence.status FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id='a_assert'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(state, "UNKNOWN");
            assert_eq!(evidence_status, "PENDING");
        });
    }

    #[test]
    fn log_event_completes_busy_job_with_log_reply() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_log_busy", STATE_BUSY, "job_log_busy");

            let changes = mark_agent_idle_log_event_sync(
                db,
                "a_log_busy",
                "codex",
                Some("LOG PONG"),
                "/tmp/rollout.jsonl",
                42,
                Some("turn-1"),
                &lifecycle_id(db, "a_log_busy"),
                None,
            )
            .unwrap();
            let (state, sub_state, status, reply): (String, String, String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.sub_state, jobs.status, jobs.reply_text \
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id \
                     WHERE agents.id = 'a_log_busy'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(sub_state, "LogEvent");
            assert_eq!(status, "COMPLETED");
            assert_eq!(reply, "LOG PONG");
        });
    }

    #[test]
    fn late_log_event_heals_recent_health_completion_stuck_job() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_late_log_stuck", STATE_STUCK, "job_late_log_stuck");
            insert_health_completion_stuck_event(db, "a_late_log_stuck", "job_late_log_stuck");

            let changes = mark_agent_idle_log_event_sync(
                db,
                "a_late_log_stuck",
                "codex",
                Some("LATE LOG PONG"),
                "/tmp/rollout.jsonl",
                44,
                Some("turn-late"),
                &lifecycle_id(db, "a_late_log_stuck"),
                None,
            )
            .unwrap();
            let (state, sub_state, status, reply, payload): (
                String,
                String,
                String,
                String,
                String,
            ) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.sub_state, jobs.status, jobs.reply_text, events.payload \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     JOIN events ON events.agent_id = agents.id AND events.event_type = 'state_change' \
                     WHERE agents.id = 'a_late_log_stuck' \
                     ORDER BY events.seq_id DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(sub_state, "LogEvent");
            assert_eq!(status, "COMPLETED");
            assert_eq!(reply, "LATE LOG PONG");
            assert_eq!(payload["from"], STATE_STUCK);
            assert_eq!(payload["reason"], "LOG_EVENT_TASK_COMPLETE");
        });
    }

    #[test]
    fn late_hook_event_heals_recent_health_completion_stuck_job() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_late_hook_stuck", STATE_STUCK, "job_late_hook_stuck");
            insert_health_completion_stuck_event(db, "a_late_hook_stuck", "job_late_hook_stuck");

            let changes = super::mark_agent_idle_hook_event_sync(
                db,
                "a_late_hook_stuck",
                "codex",
                "stop",
                Some("evt-late-hook"),
                Some("LATE HOOK PONG"),
            )
            .unwrap();
            let (state, sub_state, status, reply, payload): (
                String,
                String,
                String,
                String,
                String,
            ) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.sub_state, jobs.status, jobs.reply_text, events.payload \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     JOIN events ON events.agent_id = agents.id AND events.event_type = 'state_change' \
                     WHERE agents.id = 'a_late_hook_stuck' \
                     ORDER BY events.seq_id DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(sub_state, "HookEvent");
            assert_eq!(status, "COMPLETED");
            assert_eq!(reply, "LATE HOOK PONG");
            assert_eq!(payload["from"], STATE_STUCK);
            assert_eq!(payload["source"], "hook");
        });
    }

    #[test]
    fn ui_marker_does_not_complete_dispatched_job_while_log_monitor_active() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(
                db,
                "a_log_monitor_active",
                STATE_BUSY,
                "job_log_monitor_active",
            );
            insert_event_sync(
                &db.conn(),
                "a_log_monitor_active",
                None,
                "output_chunk",
                r#"{"text":"REAL UI FALLBACK REPLY"}"#,
            )
            .unwrap();
            let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel();
            crate::completion::registry::register(
                "a_log_monitor_active".to_string(),
                crate::completion::registry::LogMonitorEntry {
                    provider: "claude".to_string(),
                    log_root: std::path::PathBuf::from("/tmp/claude-log-root"),
                    state: crate::completion::reader::LogReadState::default(),
                    cancel_tx,
                },
            );

            let changes = mark_agent_idle_matched_sync(db, "a_log_monitor_active").unwrap();
            let (state, status): (String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status \
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id \
                     WHERE agents.id = 'a_log_monitor_active'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            crate::completion::registry::cancel("a_log_monitor_active");

            assert_eq!(changes, 0);
            assert_eq!(state, STATE_BUSY);
            assert_eq!(status, "DISPATCHED");
        });
    }

    #[test]
    fn semantic_provider_marker_never_completes_turn_without_a_log_monitor() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_semantic_marker", STATE_BUSY, "job_semantic_marker");
            db.conn()
                .execute(
                    "UPDATE agents SET provider = 'codex' WHERE id = 'a_semantic_marker'",
                    [],
                )
                .unwrap();
            insert_event_sync(
                &db.conn(),
                "a_semantic_marker",
                None,
                "output_chunk",
                r#"{"text":"PANE LOOKS IDLE"}"#,
            )
            .unwrap();

            let changes = mark_agent_idle_matched_sync(db, "a_semantic_marker").unwrap();
            let (state, status): (String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id
                     WHERE agents.id = 'a_semantic_marker'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();

            assert_eq!(changes, 0);
            assert_eq!(state, STATE_BUSY);
            assert_eq!(status, "DISPATCHED");
        });
    }

    #[test]
    fn log_event_does_not_complete_spawning_agent() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_log_spawn", STATE_SPAWNING, "job_log_spawn");

            let changes = mark_agent_idle_log_event_sync(
                db,
                "a_log_spawn",
                "codex",
                Some("LOG PONG"),
                "/tmp/rollout.jsonl",
                42,
                Some("turn-1"),
                &lifecycle_id(db, "a_log_spawn"),
                None,
            )
            .unwrap();
            let (state, job_status): (String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status FROM agents JOIN jobs ON jobs.agent_id = agents.id WHERE agents.id = 'a_log_spawn'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();

            assert_eq!(changes, 0);
            assert_eq!(state, STATE_SPAWNING);
            assert_eq!(job_status, "DISPATCHED");
        });
    }

    #[test]
    fn log_event_state_change_reason_is_log_event_task_complete() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_log_reason", STATE_BUSY, "job_log_reason");

            mark_agent_idle_log_event_sync(
                db,
                "a_log_reason",
                "codex",
                Some("LOG PONG"),
                "/tmp/rollout.jsonl",
                42,
                Some("turn-1"),
                &lifecycle_id(db, "a_log_reason"),
                None,
            )
            .unwrap();
            let payload: String = db
                .conn()
                .query_row(
                    "SELECT payload FROM events WHERE agent_id = 'a_log_reason' AND event_type = 'state_change' ORDER BY seq_id DESC LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(payload["reason"], "LOG_EVENT_TASK_COMPLETE");
            assert_eq!(payload["provider"], "codex");
            assert_eq!(payload["raw_path"], "/tmp/rollout.jsonl");
            assert_eq!(payload["raw_offset"], 42);
            assert_eq!(payload["provider_turn_id"], "turn-1");
            assert_eq!(payload["reply_source"], "log");
        });
    }

    #[test]
    fn log_event_missing_reply_uses_screen_collection() {
        with_test_db_handle(|db| {
            let dispatch_seq =
                seed_dispatched_agent_job(db, "a_log_screen", STATE_BUSY, "job_log_screen");
            insert_event_sync(
                &db.conn(),
                "a_log_screen",
                None,
                "output_chunk",
                r#"{"text":"echo PONG\nSCREEN PONG\n"}"#,
            )
            .unwrap();

            let changes = mark_agent_idle_log_event_sync(
                db,
                "a_log_screen",
                "codex",
                None,
                "/tmp/rollout.jsonl",
                43,
                None,
                &lifecycle_id(db, "a_log_screen"),
                None,
            )
            .unwrap();
            let (reply, payload): (String, String) = db
                .conn()
                .query_row(
                    "SELECT jobs.reply_text, events.payload \
                     FROM jobs JOIN events ON events.agent_id = jobs.agent_id \
                     WHERE jobs.id = 'job_log_screen' AND events.event_type = 'state_change' \
                     ORDER BY events.seq_id DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert!(dispatch_seq > 0);
            assert_eq!(changes, 1);
            assert!(reply.contains("SCREEN PONG"), "reply was {reply:?}");
            assert_eq!(payload["reply_source"], "screen");
        });
    }

    #[test]
    fn hook_push_waiting_for_ack_to_idle_uses_hook_source_not_log_payload() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(
                db,
                "a_hook_waiting",
                STATE_WAITING_FOR_ACK,
                "job_hook_waiting",
            );

            let changes = super::mark_agent_idle_hook_event_sync(
                db,
                "a_hook_waiting",
                "codex",
                "stop",
                Some("evt-hook-waiting"),
                Some("HOOK PONG"),
            )
            .unwrap();
            let (state, sub_state, payload): (String, String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.sub_state, events.payload \
                     FROM agents JOIN events ON events.agent_id = agents.id \
                     WHERE agents.id = 'a_hook_waiting' AND events.event_type = 'state_change' \
                     ORDER BY events.seq_id DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(sub_state, "HookEvent");
            assert_eq!(payload["from"], STATE_WAITING_FOR_ACK);
            assert_eq!(payload["to"], STATE_IDLE);
            assert_eq!(payload["sub_state"], "HookEvent");
            assert_eq!(payload["source"], "hook");
            assert_eq!(payload["hook_event"], "stop");
            assert_eq!(payload["provider"], "codex");
            assert_eq!(payload["event_id"], "evt-hook-waiting");
            assert_eq!(payload["reply_source"], "hook");
            assert!(payload.get("raw_path").is_none());
            assert!(payload.get("raw_offset").is_none());
            assert!(payload.get("provider_turn_id").is_none());
        });
    }

    #[test]
    fn hook_push_busy_to_idle_completes_dispatched_job() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_hook_busy", STATE_BUSY, "job_hook_busy");

            let changes = super::mark_agent_idle_hook_event_sync(
                db,
                "a_hook_busy",
                "codex",
                "stop",
                Some("evt-hook-busy"),
                Some("HOOK PONG"),
            )
            .unwrap();
            let (state, status, reply): (String, String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status, jobs.reply_text \
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id \
                     WHERE agents.id = 'a_hook_busy'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(status, "COMPLETED");
            assert_eq!(reply, "HOOK PONG");
        });
    }

    #[test]
    fn hook_push_duplicate_idle_returns_zero() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_hook_idle", STATE_IDLE, "job_hook_idle");
            let before_version: i64 = db
                .conn()
                .query_row(
                    "SELECT state_version FROM agents WHERE id = 'a_hook_idle'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            let changes = super::mark_agent_idle_hook_event_sync(
                db,
                "a_hook_idle",
                "codex",
                "stop",
                Some("evt-hook-idle"),
                Some("HOOK PONG"),
            )
            .unwrap();
            let (state, state_version, event_count, job_status): (String, i64, i64, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, agents.state_version, COUNT(events.seq_id), jobs.status \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     LEFT JOIN events ON events.agent_id = agents.id AND events.event_type = 'state_change' \
                     WHERE agents.id = 'a_hook_idle' \
                     GROUP BY agents.id",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();

            assert_eq!(changes, 0);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(state_version, before_version);
            assert_eq!(event_count, 0);
            assert_eq!(job_status, "DISPATCHED");
        });
    }

    #[test]
    fn late_hook_after_transcript_completion_preserves_official_observation() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_late_hook", STATE_IDLE, "job_late_hook");
            let lifecycle_id = lifecycle_id(db, "a_late_hook");

            let outcome = super::mark_agent_idle_hook_event_outcome_sync(
                db,
                "a_late_hook",
                "codex",
                "stop",
                Some("evt-late-hook"),
                None,
                Some(&lifecycle_id),
                None,
            )
            .unwrap();
            let observation_json: String = db
                .conn()
                .query_row(
                    "SELECT observation_json FROM provider_status_observations WHERE observation_id = 'evt-late-hook:ready'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let observation: crate::runtime_observation::ProviderObservation =
                serde_json::from_str(&observation_json).unwrap();

            assert_eq!(outcome.changes, 0);
            assert_eq!(observation.turn_id, None);
            assert_eq!(
                observation.source,
                crate::runtime_observation::EvidenceSource::OfficialHook
            );
            assert_eq!(
                observation.kind,
                crate::runtime_observation::ProviderObservationKind::Turn(
                    crate::runtime_observation::ProviderTurnState::Ready
                )
            );
        });
    }

    #[test]
    fn hook_push_stale_state_version_loses_without_event() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_hook_stale", STATE_BUSY, "job_hook_stale");
            let stale_version: i64 = db
                .conn()
                .query_row(
                    "SELECT state_version FROM agents WHERE id = 'a_hook_stale'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            db.conn()
                .execute(
                    "UPDATE agents SET state_version = state_version + 1 WHERE id = 'a_hook_stale'",
                    [],
                )
                .unwrap();

            let changes = super::mark_agent_idle_hook_event_at_version_sync(
                db,
                "a_hook_stale",
                "codex",
                "stop",
                Some("evt-hook-stale"),
                Some("HOOK PONG"),
                stale_version,
            )
            .unwrap();
            let (state, event_count, job_status): (String, i64, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, COUNT(events.seq_id), jobs.status \
                     FROM agents \
                     JOIN jobs ON jobs.agent_id = agents.id \
                     LEFT JOIN events ON events.agent_id = agents.id AND events.event_type = 'state_change' \
                     WHERE agents.id = 'a_hook_stale' \
                     GROUP BY agents.id",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(changes, 0);
            assert_eq!(state, STATE_BUSY);
            assert_eq!(event_count, 0);
            assert_eq!(job_status, "DISPATCHED");
        });
    }

    #[test]
    fn hook_push_cancel_requested_job_is_cancelled_like_pull_v2() {
        with_test_db_handle(|db| {
            seed_dispatched_agent_job(db, "a_hook_cancel", STATE_BUSY, "job_hook_cancel");
            request_dispatched_job_cancel_sync(db, "job_hook_cancel").unwrap();

            let changes = super::mark_agent_idle_hook_event_sync(
                db,
                "a_hook_cancel",
                "codex",
                "stop",
                Some("evt-hook-cancel"),
                Some("HOOK CANCELLED"),
            )
            .unwrap();
            let (state, status, reply): (String, String, String) = db
                .conn()
                .query_row(
                    "SELECT agents.state, jobs.status, jobs.reply_text \
                     FROM agents JOIN jobs ON jobs.agent_id = agents.id \
                     WHERE agents.id = 'a_hook_cancel'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, STATE_IDLE);
            assert_eq!(status, "CANCELLED");
            assert_eq!(reply, "HOOK CANCELLED");
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f4_hook_push_evidence_denial_enters_pane_nudge_path() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let agent_id = "a_hook_denial_nudge";
        super::clear_test_denial_nudges();
        seed_dispatched_agent_job(&db, agent_id, STATE_BUSY, "job_hook_denial_nudge");
        set_job_evidence_requirements_sync(&db.conn(), "job_hook_denial_nudge", true, false)
            .unwrap();

        let outcome = super::mark_agent_idle_hook_event(
            db.clone(),
            agent_id.to_string(),
            "codex".to_string(),
            "stop".to_string(),
            Some("evt-hook-denial".to_string()),
            Some("HOOK PONG".to_string()),
            None,
        )
        .await
        .unwrap();

        assert_eq!(outcome, (0, None));
        let mut nudges = Vec::new();
        for _ in 0..50 {
            nudges = super::test_denial_nudges();
            if nudges
                .iter()
                .any(|(agent, message)| agent == agent_id && message.contains("SYSTEM DENY"))
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(
            nudges
                .iter()
                .any(|(agent, message)| agent == agent_id && message.contains("SYSTEM DENY")),
            "hook denial must be sent through the pane nudge path: {nudges:?}"
        );
    }

    #[test]
    fn test_cancel_requested_prompt_only_reply_marks_cancelled() {
        with_test_db_handle(|db| {
            seed_busy_agent(db, "a_cancel");
            let before = {
                let conn = db.conn();
                insert_job_sync(&conn, "job_cancel", "a_cancel", None, "sleep 10\n").unwrap();
                let before = insert_event_sync(
                    &conn,
                    "a_cancel",
                    None,
                    "command_received",
                    r#"{"cmd":"sleep 10\n","status":"SENT"}"#,
                )
                .unwrap();
                insert_event_sync(&conn, "a_cancel", None, "output_chunk", r#"{"text":"$ "}"#)
                    .unwrap();
                conn.execute("UPDATE agents SET state = 'IDLE' WHERE id = 'a_cancel'", [])
                    .unwrap();
                before
            };
            claim_next_job_sync(db, "a_cancel").unwrap().unwrap();
            {
                let conn = db.conn();
                update_dispatched_seq_id_sync(&conn, "job_cancel", before).unwrap();
                conn.execute("UPDATE agents SET state = 'BUSY' WHERE id = 'a_cancel'", [])
                    .unwrap();
            }
            request_dispatched_job_cancel_sync(db, "job_cancel").unwrap();

            let changes = mark_agent_idle_matched_sync(db, "a_cancel").unwrap();
            let job = query_job_sync(&db.conn(), "job_cancel").unwrap().unwrap();
            let state: String = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = 'a_cancel'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(changes, 1);
            assert_eq!(state, "IDLE");
            assert_eq!(job.status, "CANCELLED");
        });
    }

    #[test]
    fn test_is_prompt_only_reply_recognizes_known_prompts() {
        use super::is_prompt_only_reply;
        assert!(is_prompt_only_reply(""));
        assert!(is_prompt_only_reply("  "));
        assert!(is_prompt_only_reply(">"));
        assert!(is_prompt_only_reply("$"));
        assert!(is_prompt_only_reply("✦"));
        assert!(is_prompt_only_reply("❯"));
        assert!(is_prompt_only_reply("›"));
        assert!(is_prompt_only_reply("  > "));
    }

    #[test]
    fn test_is_prompt_only_reply_does_not_swallow_real_content() {
        use super::is_prompt_only_reply;
        assert!(!is_prompt_only_reply("done"));
        assert!(!is_prompt_only_reply("Hello, here is your answer"));
        assert!(!is_prompt_only_reply("> quoted text"));
        assert!(!is_prompt_only_reply("pong"));
    }

    #[test]
    fn test_state_constants_match_strings() {
        assert_eq!(STATE_SPAWNING, "SPAWNING");
        assert_eq!(STATE_SPAWNING_INTERVENTION, "SPAWNING_INTERVENTION");
        assert_eq!(STATE_IDLE, "IDLE");
        assert_eq!(STATE_WAITING_FOR_ACK, "WAITING_FOR_ACK");
        assert_eq!(STATE_BUSY, "BUSY");
        assert_eq!(STATE_PROMPT_PENDING, "PROMPT_PENDING");
        assert_eq!(STATE_STUCK, "STUCK");
        assert_eq!(STATE_FAILED, "FAILED");
        assert_eq!(STATE_CRASHED, "CRASHED");
        assert_eq!(STATE_KILLED, "KILLED");
        assert_eq!(STATE_UNKNOWN, "UNKNOWN");
    }

    #[test]
    fn test_is_active_state_covers_spawning_waiting_busy() {
        assert!(is_active_state("SPAWNING"));
        assert!(is_active_state("WAITING_FOR_ACK"));
        assert!(is_active_state("BUSY"));
        assert!(!is_active_state("IDLE"));
        assert!(!is_active_state("SPAWNING_INTERVENTION"));
        assert!(!is_active_state("PROMPT_PENDING"));
        assert!(!is_active_state("STUCK"));
        assert!(!is_active_state("FAILED"));
        assert!(!is_active_state("CRASHED"));
        assert!(!is_active_state("KILLED"));
        assert!(!is_active_state("UNKNOWN"));
    }

    #[test]
    fn test_mark_agent_prompt_pending_accepts_idle_and_spawning_rejects_active_dispatch() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_pending", "p_pending", "/tmp/pending").unwrap();
                insert_agent_sync(
                    &conn,
                    "a_spawning",
                    "s_pending",
                    "bash",
                    STATE_SPAWNING,
                    Some(0),
                )
                .unwrap();
                insert_agent_sync(&conn, "a_idle", "s_pending", "bash", STATE_IDLE, Some(1))
                    .unwrap();
                insert_agent_sync(
                    &conn,
                    "a_waiting",
                    "s_pending",
                    "bash",
                    STATE_WAITING_FOR_ACK,
                    Some(2),
                )
                .unwrap();
                insert_agent_sync(&conn, "a_busy", "s_pending", "bash", STATE_BUSY, Some(3))
                    .unwrap();
            }

            for agent_id in ["a_waiting", "a_busy"] {
                let err =
                    mark_agent_prompt_pending_sync(db, agent_id, "UNKNOWN_PROMPT").unwrap_err();
                let state: String = db
                    .conn()
                    .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert!(matches!(err, CcbdError::AgentWrongState { .. }));
                assert!(matches!(state.as_str(), STATE_WAITING_FOR_ACK | STATE_BUSY));
                assert_eq!(state_change_count(db, agent_id), 0);
            }

            for agent_id in ["a_idle", "a_spawning"] {
                let changes =
                    mark_agent_prompt_pending_sync(db, agent_id, "UNKNOWN_PROMPT").unwrap();
                let state: String = db
                    .conn()
                    .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(changes, 1);
                assert_eq!(state, STATE_PROMPT_PENDING);
                assert_eq!(state_change_count(db, agent_id), 1);
            }
        });
    }

    #[test]
    fn test_mark_agent_prompt_pending_rejects_terminal_without_event() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_terminal", "p_terminal", "/tmp/terminal").unwrap();
                insert_agent_sync(
                    &conn,
                    "a_crashed",
                    "s_terminal",
                    "bash",
                    STATE_CRASHED,
                    Some(1),
                )
                .unwrap();
            }

            let err =
                mark_agent_prompt_pending_sync(db, "a_crashed", "UNKNOWN_PROMPT").unwrap_err();
            let state: String = db
                .conn()
                .query_row(
                    "SELECT state FROM agents WHERE id = 'a_crashed'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();

            assert!(matches!(err, CcbdError::AgentWrongState { .. }));
            assert_eq!(state, STATE_CRASHED);
            assert_eq!(state_change_count(db, "a_crashed"), 0);
        });
    }

    #[test]
    fn test_prompt_pending_can_resolve_to_idle_or_busy() {
        with_test_db_handle(|db| {
            {
                let conn = db.conn();
                insert_session_sync(&conn, "s_resolve", "p_resolve", "/tmp/resolve").unwrap();
                insert_agent_sync(
                    &conn,
                    "a_idle_resolve",
                    "s_resolve",
                    "bash",
                    STATE_PROMPT_PENDING,
                    Some(1),
                )
                .unwrap();
                insert_agent_sync(
                    &conn,
                    "a_busy_resolve",
                    "s_resolve",
                    "bash",
                    STATE_PROMPT_PENDING,
                    Some(2),
                )
                .unwrap();
            }
            let mut conn = db.conn();
            transit_agent_state_sync(
                &mut conn,
                "a_idle_resolve",
                &[STATE_PROMPT_PENDING],
                STATE_IDLE,
                Some("prompt_resolved"),
            )
            .unwrap();
            transit_agent_state_sync(
                &mut conn,
                "a_busy_resolve",
                &[STATE_PROMPT_PENDING],
                STATE_BUSY,
                Some("prompt_resolved_job_in_flight"),
            )
            .unwrap();

            let states: Vec<String> = ["a_idle_resolve", "a_busy_resolve"]
                .into_iter()
                .map(|agent_id| {
                    conn.query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                        row.get(0)
                    })
                    .unwrap()
                })
                .collect();
            assert_eq!(states, [STATE_IDLE, STATE_BUSY]);
        });
    }
}
