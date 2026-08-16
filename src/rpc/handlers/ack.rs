use crate::error::CcbdError;
use crate::marker::{MarkerMatcher, MatchResult, parser_registry, registry};
use crate::pane_diff::is_meaningful_diff;
use rusqlite::OptionalExtension;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const CAPTURE_SEED_POLL_MS: u64 = 50;
pub(crate) const CAPTURE_SEED_STABILITY_MS: u64 = 500;
pub(crate) const ACK_IDLE_SCAN_REOPEN_DELAY_MS: u64 = 2_000;
const ACK_BUSY_RETRY_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckEvidenceMode {
    /// A provider hook or native session log must acknowledge the turn. Pane
    /// capture remains available only for interstitial/prompt handling.
    SemanticEvents,
    /// Providers without a native event surface (currently bash) may use pane
    /// changes as an explicitly weaker fallback.
    PaneFallback,
}

impl AckEvidenceMode {
    pub(crate) fn for_provider(provider: &str) -> Self {
        let uses_terminal_pane = crate::provider::adapter(provider).is_some_and(|adapter| {
            adapter
                .observation_spec()
                .uses_terminal_pane_for_turn_state()
        });
        if uses_terminal_pane {
            Self::PaneFallback
        } else {
            Self::SemanticEvents
        }
    }

    const fn allows_pane_working_or_completion(self) -> bool {
        matches!(self, Self::PaneFallback)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckBusyOutcome {
    MarkedBusy,
    AlreadyBusy,
    AlreadyIdle,
    PromptPending,
    Terminal,
    Deferred,
}

pub(crate) fn spawn_new_capture_seed(
    db: crate::db::Db,
    tmux: Arc<crate::tmux::TmuxServer>,
    agent_id: String,
    provider: String,
    state_dir: std::path::PathBuf,
    baseline: String,
    matcher: Arc<MarkerMatcher>,
    evidence_mode: AckEvidenceMode,
) {
    tokio::spawn(async move {
        let allow_direct_idle = evidence_mode.allows_pane_working_or_completion()
            && matcher.mode() != crate::provider::manifest::IdleDetectionMode::ObservedStability;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut processed_len = 0_usize;
        let stability = Duration::from_millis(CAPTURE_SEED_STABILITY_MS);
        let ack_started_at = tokio::time::Instant::now();
        let mut busy_marked = false;
        let mut last_meaningful_diff_at: Option<tokio::time::Instant> = None;
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(CAPTURE_SEED_POLL_MS)).await;
            if crate::db::agents::query_agent_state(db.clone(), agent_id.clone())
                .await
                .ok()
                .flatten()
                .is_none_or(|(state, _)| state != crate::db::state_machine::STATE_WAITING_FOR_ACK)
            {
                return;
            }
            let Some(pane_id) = crate::agent_io::pane_id(&agent_id) else {
                if evidence_mode == AckEvidenceMode::SemanticEvents {
                    tracing::warn!(agent_id = %agent_id, "pane unavailable during semantic ACK observation; leaving turn at DELIVERED for provider evidence");
                    return;
                }
                if let Err(err) =
                    fallback_ack_to_crashed(db.clone(), &agent_id, "pane_unregistered_during_ack")
                        .await
                {
                    tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark ACK fallback CRASHED after pane unregister");
                }
                return;
            };
            let Some(parser_handle) = parser_registry::get(&agent_id) else {
                if evidence_mode == AckEvidenceMode::SemanticEvents {
                    tracing::warn!(agent_id = %agent_id, "terminal parser unavailable during semantic ACK observation; leaving turn at DELIVERED for provider evidence");
                    return;
                }
                if let Err(err) =
                    fallback_ack_to_crashed(db.clone(), &agent_id, "reader_unregistered_during_ack")
                        .await
                {
                    tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark ACK fallback CRASHED after reader unregister");
                }
                return;
            };
            let Ok(capture) = tmux.capture_pane(pane_id.clone()).await else {
                if evidence_mode == AckEvidenceMode::SemanticEvents {
                    tracing::warn!(agent_id = %agent_id, "pane capture failed during semantic ACK observation; leaving turn at DELIVERED for provider evidence");
                    return;
                }
                if let Err(err) =
                    fallback_ack_to_stuck(db.clone(), &agent_id, "tmux_capture_failed_during_ack")
                        .await
                {
                    tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark ACK fallback STUCK after capture failure");
                }
                return;
            };
            let now = tokio::time::Instant::now();
            if !is_meaningful_diff(&baseline, &capture) {
                if evidence_mode.allows_pane_working_or_completion()
                    && !busy_marked
                    && now.duration_since(ack_started_at) >= stability
                {
                    match ack_mark_busy_or_resolve(db.clone(), &agent_id, "ACK_STABILITY_WINDOW")
                        .await
                    {
                        Ok(AckBusyOutcome::MarkedBusy | AckBusyOutcome::AlreadyBusy) => {
                            busy_marked = true;
                        }
                        Ok(outcome) => {
                            tracing::info!(agent_id = %agent_id, ?outcome, "ACK stability busy mark resolved without BUSY transition");
                            return;
                        }
                        Err(err) => {
                            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent BUSY after ACK stability window");
                            return;
                        }
                    }
                }
                if last_meaningful_diff_at
                    .is_some_and(|last_change| now.duration_since(last_change) >= stability)
                {
                    return;
                }
                continue;
            }
            let first_meaningful_diff = last_meaningful_diff_at.is_none();
            last_meaningful_diff_at = Some(now);
            if first_meaningful_diff && !busy_marked {
                if crate::prompt_handler::integration::is_prompt_handling_provider(&provider) {
                    match crate::prompt_handler::integration::scan_prompt_and_apply_outcome(
                        crate::prompt_handler::integration::PromptScanRequest {
                            db: db.clone(),
                            agent_id: agent_id.clone(),
                            provider: provider.clone(),
                            pane_id: pane_id.clone(),
                            tmux: tmux.clone(),
                            state_dir: state_dir.clone(),
                            marker_matcher: matcher.clone(),
                            max_depth: 3,
                            scan_purpose:
                                crate::prompt_handler::PromptScanPurpose::AckVisualDiff,
                        },
                    )
                    .await
                    {
                        Ok(crate::prompt_handler::integration::PromptScanDisposition::Handled {
                            depth,
                        }) => {
                            tracing::info!(
                                agent_id = %agent_id,
                                depth,
                                "prompt scan auto-handled prompt during ACK visual diff; continuing ACK loop"
                            );
                            processed_len = 0;
                            last_meaningful_diff_at = None;
                            continue;
                        }
                        Ok(crate::prompt_handler::integration::PromptScanDisposition::Pending {
                            depth,
                            block_reason,
                        }) => {
                            tracing::info!(
                                agent_id = %agent_id,
                                depth,
                                block_reason,
                                "prompt scan moved agent to PROMPT_PENDING during ACK visual diff"
                            );
                            if let Some(handle) = registry::take(&agent_id) {
                                let _ = handle.cancel_tx.send(());
                            }
                            crate::orchestrator::wake_up();
                            return;
                        }
                        Ok(crate::prompt_handler::integration::PromptScanDisposition::Deferred {
                            depth,
                            block_reason,
                        }) => {
                            tracing::info!(
                                agent_id = %agent_id,
                                depth,
                                block_reason,
                                "prompt scan deferred during ACK visual diff; resuming ACK handling"
                            );
                        }
                        Ok(crate::prompt_handler::integration::PromptScanDisposition::NoActionNeeded {
                            ..
                        }) => {
                            tracing::info!(
                                agent_id = %agent_id,
                                "prompt scan found no prompt during ACK visual diff; resuming ACK handling"
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                agent_id = %agent_id,
                                reason = %err,
                                impact = "prompt scan failed; preserving existing ACK visual diff behavior",
                                "prompt scan failed during ACK visual diff"
                            );
                        }
                    }
                }
                if !evidence_mode.allows_pane_working_or_completion() {
                    continue;
                }
                match ack_mark_busy_or_resolve(db.clone(), &agent_id, "ACK_VISUAL_DIFF").await {
                    Ok(AckBusyOutcome::MarkedBusy | AckBusyOutcome::AlreadyBusy) => {}
                    Ok(outcome) => {
                        tracing::info!(agent_id = %agent_id, ?outcome, "ACK visual diff busy mark resolved without BUSY transition");
                        return;
                    }
                    Err(err) => {
                        tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent BUSY after ACK visual diff");
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(ACK_IDLE_SCAN_REOPEN_DELAY_MS)).await;
                crate::agent_io::set_idle_scan_enabled(&agent_id, true);
                busy_marked = true;
                let matched_after_reopen = match parser_handle.lock() {
                    Ok(parser) => matcher.scan(&parser),
                    Err(err) => {
                        tracing::warn!(agent_id = %agent_id, error = %err, "parser mutex poisoned while reopening idle scan after ACK");
                        MatchResult::NoMatch
                    }
                };
                if matched_after_reopen == MatchResult::Matched {
                    match crate::db::state_machine::mark_agent_idle_matched(
                        db.clone(),
                        agent_id.clone(),
                    )
                    .await
                    {
                        Ok((changes, affected_job)) if changes > 0 => {
                            // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
                            if let Some(job_id) = affected_job {
                                crate::orchestrator::pubsub::notify_job_update(&job_id);
                            }
                            crate::orchestrator::wake_up();
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent IDLE after reopening idle scan");
                        }
                    }
                    if let Some(handle) = registry::take(&agent_id) {
                        let _ = handle.cancel_tx.send(());
                    }
                    return;
                }
                continue;
            }
            if !capture.starts_with(&baseline) && !capture.contains(&baseline) {
                let mut parser = vt100::Parser::new(200, 200, 0);
                parser.process(capture.as_bytes());
                if allow_direct_idle && capture_seed_matches(&parser, &matcher) {
                    match crate::db::state_machine::mark_agent_idle_matched(
                        db.clone(),
                        agent_id.clone(),
                    )
                    .await
                    {
                        Ok((changes, affected_job)) if changes > 0 => {
                            // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
                            if let Some(job_id) = affected_job {
                                crate::orchestrator::pubsub::notify_job_update(&job_id);
                            }
                            crate::orchestrator::wake_up();
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent IDLE from replacement tmux capture seed");
                        }
                    }
                    if let Some(handle) = registry::take(&agent_id) {
                        let _ = handle.cancel_tx.send(());
                    }
                    return;
                }
                continue;
            }
            let Some(suffix) = capture.strip_prefix(&baseline) else {
                if evidence_mode == AckEvidenceMode::SemanticEvents {
                    tracing::debug!(agent_id = %agent_id, "semantic ACK pane baseline changed; provider evidence remains authoritative");
                    continue;
                }
                if let Err(err) = fallback_ack_to_stuck(
                    db.clone(),
                    &agent_id,
                    "capture_baseline_mismatch_during_ack",
                )
                .await
                {
                    tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark ACK fallback STUCK after baseline mismatch");
                }
                return;
            };
            if suffix.len() <= processed_len {
                continue;
            }
            let delta = &suffix[processed_len..];
            processed_len = suffix.len();
            let matched = {
                match parser_handle.lock() {
                    Ok(mut parser) => {
                        parser.process(delta.as_bytes());
                        if allow_direct_idle {
                            matcher.scan(&parser)
                        } else {
                            MatchResult::NoMatch
                        }
                    }
                    Err(err) => {
                        tracing::warn!(agent_id = %agent_id, error = %err, "parser mutex poisoned during new tmux capture seed");
                        MatchResult::NoMatch
                    }
                }
            };
            if matched == MatchResult::Matched {
                match crate::db::state_machine::mark_agent_idle_matched(
                    db.clone(),
                    agent_id.clone(),
                )
                .await
                {
                    Ok((changes, affected_job)) if changes > 0 => {
                        // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
                        if let Some(job_id) = affected_job {
                            crate::orchestrator::pubsub::notify_job_update(&job_id);
                        }
                        crate::orchestrator::wake_up();
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent IDLE from new tmux capture seed");
                    }
                }
                if let Some(handle) = registry::take(&agent_id) {
                    let _ = handle.cancel_tx.send(());
                }
                return;
            }
        }
        if !evidence_mode.allows_pane_working_or_completion() {
            tracing::warn!(agent_id = %agent_id, "no provider ACK event observed within the visual observation window; leaving turn at DELIVERED for the log monitor or health policy");
            return;
        }
        if let Err(err) = fallback_ack_to_stuck(db, &agent_id, "ack_deadline_timeout").await {
            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark ACK fallback STUCK after capture seed deadline");
        }
    });
}

#[doc(hidden)]
pub async fn ack_mark_busy_or_resolve(
    db: crate::db::Db,
    agent_id: &str,
    reason: &str,
) -> Result<AckBusyOutcome, CcbdError> {
    let agent_id = agent_id.to_string();
    let reason = reason.to_string();
    for attempt in 0..ACK_BUSY_RETRY_ATTEMPTS {
        let outcome = ack_mark_busy_or_resolve_once(db.clone(), &agent_id, &reason).await?;
        if outcome != AckBusyOutcome::Deferred {
            return Ok(outcome);
        }
        if attempt + 1 < ACK_BUSY_RETRY_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(CAPTURE_SEED_POLL_MS)).await;
        }
    }
    emit_ack_busy_deferred(db, &agent_id, &reason).await?;
    Ok(AckBusyOutcome::Deferred)
}

async fn ack_mark_busy_or_resolve_once(
    db: crate::db::Db,
    agent_id: &str,
    reason: &str,
) -> Result<AckBusyOutcome, CcbdError> {
    let agent_id = agent_id.to_string();
    let reason = reason.to_string();
    crate::db::common::spawn_db("handlers::ack_mark_busy_or_resolve_once", move || {
        let mut conn = db.conn();
        let tx = conn
            .transaction()
            .map_err(|err| crate::db::common::map_db_error("begin ACK busy", err))?;
        let current = tx
            .query_row(
                "SELECT state, state_version FROM agents WHERE id = ?",
                rusqlite::params![agent_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|err| crate::db::common::map_db_error("query ACK busy state", err))?;
        let Some((state, state_version)) = current else {
            tx.commit()
                .map_err(|err| crate::db::common::map_db_error("commit missing ACK busy", err))?;
            return Ok(AckBusyOutcome::Terminal);
        };
        let outcome = match state.as_str() {
            crate::db::state_machine::STATE_WAITING_FOR_ACK => {
                let changes = tx
                    .execute(
                        "UPDATE agents
                         SET state = 'BUSY',
                             state_version = state_version + 1,
                             updated_at = unixepoch()
                         WHERE id = ?
                           AND state = 'WAITING_FOR_ACK'
                           AND state_version = ?",
                        rusqlite::params![agent_id.as_str(), state_version],
                    )
                    .map_err(|err| crate::db::common::map_db_error("mark ACK busy", err))?;
                if changes == 1 {
                    let payload = json!({
                        "from": crate::db::state_machine::STATE_WAITING_FOR_ACK,
                        "to": crate::db::state_machine::STATE_BUSY,
                        "reason": reason,
                    })
                    .to_string();
                    tx.execute(
                        "INSERT INTO events (agent_id, request_id, event_type, payload)
                         VALUES (?, NULL, 'state_change', ?)",
                        rusqlite::params![agent_id.as_str(), payload],
                    )
                    .map_err(|err| {
                        crate::db::common::map_db_error("insert ACK busy state_change", err)
                    })?;
                    let dispatched_job_id = tx
                        .query_row(
                            "SELECT id FROM jobs WHERE agent_id = ? AND status = 'DISPATCHED' ORDER BY dispatched_at ASC, id ASC LIMIT 1",
                            rusqlite::params![agent_id.as_str()],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()
                        .map_err(|err| {
                            crate::db::common::map_db_error(
                                "query ACK busy provider turn",
                                err,
                            )
                        })?;
                    if let Some(job_id) = dispatched_job_id.as_deref() {
                        crate::runtime_observation::store::append_for_current_lifecycle_sync(
                            &tx,
                            &format!("ack:{agent_id}:{state_version}:{reason}"),
                            &agent_id,
                            Some(job_id),
                            crate::runtime_observation::EvidenceSource::TerminalPane,
                            crate::runtime_observation::ProviderObservationKind::Turn(
                                crate::runtime_observation::ProviderTurnState::Working,
                            ),
                            crate::runtime_observation::store::now_epoch_millis(),
                        )?;
                    }
                    AckBusyOutcome::MarkedBusy
                } else {
                    AckBusyOutcome::Deferred
                }
            }
            crate::db::state_machine::STATE_BUSY => AckBusyOutcome::AlreadyBusy,
            crate::db::state_machine::STATE_IDLE => AckBusyOutcome::AlreadyIdle,
            crate::db::state_machine::STATE_PROMPT_PENDING => AckBusyOutcome::PromptPending,
            crate::db::state_machine::STATE_STUCK
            | crate::db::state_machine::STATE_CRASHED
            | crate::db::state_machine::STATE_KILLED => AckBusyOutcome::Terminal,
            _ => AckBusyOutcome::Terminal,
        };
        tx.commit()
            .map_err(|err| crate::db::common::map_db_error("commit ACK busy", err))?;
        Ok(outcome)
    })
    .await
    .inspect(|outcome| match outcome {
        AckBusyOutcome::AlreadyIdle | AckBusyOutcome::PromptPending => {
            crate::orchestrator::wake_up();
        }
        _ => {}
    })
}

async fn emit_ack_busy_deferred(
    db: crate::db::Db,
    agent_id: &str,
    reason: &str,
) -> Result<(), CcbdError> {
    crate::db::events::insert_event(
        db,
        agent_id.to_string(),
        None,
        "ack_busy_deferred".to_string(),
        json!({
            "reason": reason,
            "attempts": ACK_BUSY_RETRY_ATTEMPTS,
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

#[doc(hidden)]
pub async fn fallback_ack_to_stuck(
    db: crate::db::Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let agent_id = agent_id.to_string();
    let reason = reason.to_string();
    crate::db::common::spawn_db("handlers::fallback_ack_to_stuck", move || {
        let mut conn = db.conn();
        let tx = conn
            .transaction()
            .map_err(|err| crate::db::common::map_db_error("begin ACK stuck fallback", err))?;
        let current = tx
            .query_row(
                "SELECT state, state_version FROM agents WHERE id = ?",
                rusqlite::params![agent_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(|err| crate::db::common::map_db_error("query ACK fallback state", err))?;
        let Some((current_state, state_version)) = current else {
            tx.rollback()
                .map_err(|err| crate::db::common::map_db_error("rollback missing ACK fallback", err))?;
            return Ok(0);
        };
        if current_state != crate::db::state_machine::STATE_WAITING_FOR_ACK {
            tx.rollback()
                .map_err(|err| crate::db::common::map_db_error("rollback ignored ACK fallback", err))?;
            return Ok(0);
        }

        let dispatched_job_id = tx
            .query_row(
                "SELECT id FROM jobs WHERE agent_id = ? AND status = 'DISPATCHED' ORDER BY dispatched_at ASC, id ASC LIMIT 1",
                rusqlite::params![agent_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| crate::db::common::map_db_error("query ACK fallback dispatched job", err))?;

        let safe_pre_send = matches!(
            reason.as_str(),
            "pane_unregistered_during_ack"
                | "reader_unregistered_during_ack"
                | "tmux_capture_failed_during_ack"
        );
        let requeue_pre_send = dispatched_job_id.is_some() && safe_pre_send;

        let changes = if requeue_pre_send {
            tx.execute(
                "UPDATE agents
                 SET state = 'IDLE',
                     state_version = state_version + 1,
                     updated_at = unixepoch()
                 WHERE id = ?
                   AND state = 'WAITING_FOR_ACK'
                   AND state_version = ?",
                rusqlite::params![agent_id.as_str(), state_version],
            )
            .map_err(|err| crate::db::common::map_db_error("restore ACK fallback idle", err))?
        } else {
            tx.execute(
                "UPDATE agents SET state = 'STUCK', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state = 'WAITING_FOR_ACK' AND state_version = ?",
                rusqlite::params![agent_id.as_str(), state_version],
            )
            .map_err(|err| crate::db::common::map_db_error("mark ACK fallback stuck", err))?
        };
        if changes == 1 {
            let to_state = if requeue_pre_send {
                crate::db::state_machine::STATE_IDLE
            } else {
                crate::db::state_machine::STATE_STUCK
            };
            if let Some(job_id) = dispatched_job_id.as_deref() {
                if requeue_pre_send {
                    let job = crate::db::jobs::query_job_sync(&tx, job_id)?;
                    if let Some(job) = job {
                        if job.status == "DISPATCHED" && job.agent_id == agent_id.as_str() {
                            crate::db::job_state::requeue_job_state_conn_sync(
                                &tx,
                                job_id,
                                crate::db::job_state::JobStatus::Dispatched,
                                None,
                                reason.as_str(),
                            )?;
                        }
                    }
                } else {
                    let job = crate::db::jobs::query_job_sync(&tx, job_id)?;
                    if let Some(job) = job {
                        if job.status == "DISPATCHED" && job.agent_id == agent_id.as_str() {
                            tx.execute(
                                "UPDATE jobs SET error_reason = ? WHERE id = ? AND status = 'DISPATCHED'",
                                rusqlite::params![reason.as_str(), job_id],
                            )
                            .map_err(|err| crate::db::common::map_db_error("update ACK fallback job error_reason", err))?;

                            crate::db::job_state::transit_job_state(
                                &tx,
                                job_id,
                                crate::db::job_state::JobStatus::Dispatched,
                                crate::db::job_state::JobStatus::Failed,
                                reason.as_str(),
                            )?;
                        }
                    }
                }
                let job_resolution = if requeue_pre_send { "REQUEUED" } else { "FAILED" };
                tx.execute(
                    "INSERT INTO events (agent_id, request_id, event_type, payload)
                     VALUES (?, NULL, 'job_resolution', ?)",
                    rusqlite::params![
                        agent_id.as_str(),
                        json!({
                            "job_id": job_id,
                            "job_resolution": job_resolution,
                            "reason": reason,
                            "source": "ack_stuck_before_busy",
                        })
                        .to_string()
                    ],
                )
                .map_err(|err| crate::db::common::map_db_error("insert ACK fallback job_resolution", err))?;
            }
            let payload = json!({
                "from": current_state,
                "to": to_state,
                "reason": reason,
                "job_id": dispatched_job_id,
                "job_resolution": if requeue_pre_send { "REQUEUED" } else if dispatched_job_id.is_some() { "FAILED" } else { "NONE" },
            })
            .to_string();
            tx.execute(
                "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
                rusqlite::params![agent_id.as_str(), payload],
            )
            .map_err(|err| crate::db::common::map_db_error("insert ACK fallback stuck state_change", err))?;
        }
        tx.commit()
            .map_err(|err| crate::db::common::map_db_error("commit ACK stuck fallback", err))?;
        if changes == 1
            && let Some(job_id) = dispatched_job_id
        {
            crate::orchestrator::pubsub::notify_job_update(&job_id);
            crate::orchestrator::wake_up();
        }
        Ok(changes)
    })
    .await
}

#[doc(hidden)]
pub async fn fallback_ack_to_crashed(
    db: crate::db::Db,
    agent_id: &str,
    reason: &str,
) -> Result<usize, CcbdError> {
    let state = crate::db::agents::query_agent_state(db.clone(), agent_id.to_string()).await?;
    if state
        .as_ref()
        .is_none_or(|(state, _)| state != crate::db::state_machine::STATE_WAITING_FOR_ACK)
    {
        return Ok(0);
    }
    crate::db::agents_lifecycle::mark_agent_crashed_with_reason(
        db,
        agent_id.to_string(),
        None,
        reason.to_string(),
    )
    .await
}

pub(super) fn capture_seed_matches(parser: &vt100::Parser, matcher: &MarkerMatcher) -> bool {
    if matcher.mode() == crate::provider::manifest::IdleDetectionMode::ObservedStability {
        return false;
    }
    matcher.scan(parser) == MatchResult::Matched
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{dispatch_job_to_agent_sync, insert_job_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::runtime_observation::{
        EvidenceSource, ProviderObservation, ProviderObservationKind, ProviderTurnState,
    };

    #[tokio::test]
    async fn ack_busy_transition_records_working_for_the_dispatched_turn() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_ack", "p_ack", "/tmp/ack").unwrap();
            insert_agent_sync(&conn, "a_ack", "s_ack", "antigravity", "IDLE", Some(42)).unwrap();
            insert_job_sync(&conn, "job_ack", "a_ack", None, "do work").unwrap();
        }
        {
            let mut conn = db.conn();
            dispatch_job_to_agent_sync(
                &mut conn,
                "a_ack",
                &[crate::db::state_machine::STATE_IDLE],
                crate::db::state_machine::STATE_WAITING_FOR_ACK,
                "command_received",
                &json!({ "status": "SENT" }),
            )
            .unwrap()
            .unwrap();
        }

        assert_eq!(
            ack_mark_busy_or_resolve(db.clone(), "a_ack", "ACK_TEST")
                .await
                .unwrap(),
            AckBusyOutcome::MarkedBusy
        );
        let conn = db.conn();
        let (state, observation_json): (String, String) = conn
            .query_row(
                "SELECT agents.state, provider_status_observations.observation_json
                 FROM agents
                 JOIN provider_status_observations ON provider_status_observations.agent_id = agents.id
                 WHERE agents.id = 'a_ack'
                 ORDER BY provider_status_observations.seq_id DESC
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let observation: ProviderObservation = serde_json::from_str(&observation_json).unwrap();

        assert_eq!(state, crate::db::state_machine::STATE_BUSY);
        assert_eq!(observation.turn_id.as_deref(), Some("job_ack"));
        assert_eq!(observation.source, EvidenceSource::TerminalPane);
        assert_eq!(
            observation.kind,
            ProviderObservationKind::Turn(ProviderTurnState::Working)
        );
    }

    #[test]
    fn semantic_provider_mode_forbids_pane_working_and_completion() {
        assert!(!AckEvidenceMode::SemanticEvents.allows_pane_working_or_completion());
        assert!(AckEvidenceMode::PaneFallback.allows_pane_working_or_completion());
        assert_eq!(
            AckEvidenceMode::for_provider("bash"),
            AckEvidenceMode::PaneFallback
        );
        for provider in ["codex", "claude", "antigravity"] {
            assert_eq!(
                AckEvidenceMode::for_provider(provider),
                AckEvidenceMode::SemanticEvents
            );
        }
    }
}
