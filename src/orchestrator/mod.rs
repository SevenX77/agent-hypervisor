pub mod pubsub;

use crate::db;
use crate::db::recovery::RecoveryIntentAction;
use crate::error::AhError;
use crate::marker::{
    MarkerMatcher, PromptTimerScanContext, TimerKind, parser_registry, registry,
    spawn_marker_timer_task_with_prompt,
};
use crate::pane_diff::{pane_diff_watcher_loop, resolve_stuck_watch_config};
use crate::prompt_handler::integration::{
    PromptScanDisposition, PromptScanRequest, is_prompt_handling_provider,
    prompt_pending_unpark_watcher_loop, scan_prompt_and_apply_outcome,
};
use crate::prompt_handler::matcher::PromptScanPurpose;
use crate::provider::health_check::health_check_watcher_loop;
use crate::provider::manifest::is_recovery_eligible_provider;
use crate::rpc::Ctx;
use crate::rpc::handlers::{RealignAgentParams, spawn_realign_agent};
use crate::tmux::TmuxPaneId;
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

pub static WAKER: LazyLock<Notify> = LazyLock::new(Notify::new);

pub fn wake_up() {
    WAKER.notify_one();
}

#[cfg(test)]
type BeforeDispatchSendHook =
    Arc<dyn Fn(Ctx, String, TmuxPaneId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[cfg(test)]
static BEFORE_DISPATCH_SEND_HOOK: LazyLock<std::sync::Mutex<Option<BeforeDispatchSendHook>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(test)]
fn set_before_dispatch_send_hook_for_test(hook: Option<BeforeDispatchSendHook>) {
    *BEFORE_DISPATCH_SEND_HOOK.lock().unwrap() = hook;
}

#[cfg(test)]
async fn run_before_dispatch_send_hook_for_test(ctx: &Ctx, agent_id: &str, pane_id: TmuxPaneId) {
    let hook = BEFORE_DISPATCH_SEND_HOOK.lock().unwrap().clone();
    if let Some(hook) = hook {
        hook(ctx.clone(), agent_id.to_string(), pane_id).await;
    }
}

#[cfg(not(test))]
async fn run_before_dispatch_send_hook_for_test(_ctx: &Ctx, _agent_id: &str, _pane_id: TmuxPaneId) {
}

/// How often the daemon reclaims its own storage. Long enough to be invisible
/// next to real work, short enough that a busy stack cannot accumulate a day's
/// worth of pane output before the first pass.
const STATE_MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Keeps the state database bounded for as long as the daemon runs.
///
/// The first pass happens right away: a daemon that has just started is the one
/// moment where a database bloated by the previous run can be compacted without
/// competing with live traffic (decision 0004, #23).
async fn state_maintenance_loop(ctx: Ctx) {
    loop {
        let db = ctx.db.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let conn = db.conn();
            crate::db::maintenance::run_state_maintenance(
                &conn,
                crate::db::maintenance::RetentionPolicy::default(),
            )
        })
        .await;
        match outcome {
            Ok(Ok(report)) if report.deleted_rows() > 0 || report.vacuumed => tracing::info!(
                firehose_events_deleted = report.firehose_events_deleted,
                forensic_events_deleted = report.forensic_events_deleted,
                job_transitions_deleted = report.job_transitions_deleted,
                vacuumed = report.vacuumed,
                "state database maintenance reclaimed space"
            ),
            Ok(Ok(_)) => {}
            Ok(Err(err)) => tracing::warn!(error = %err, "state database maintenance failed"),
            Err(err) => tracing::warn!(error = %err, "state database maintenance task panicked"),
        }
        tokio::time::sleep(STATE_MAINTENANCE_INTERVAL).await;
    }
}

pub fn spawn_orchestrator_task(ctx: Ctx) {
    let maintenance_ctx = ctx.clone();
    tokio::spawn(async move {
        state_maintenance_loop(maintenance_ctx).await;
    });
    let watcher_ctx = ctx.clone();
    tokio::spawn(async move {
        let (watch_interval, stuck_threshold) = resolve_stuck_watch_config();
        pane_diff_watcher_loop(watcher_ctx, watch_interval, stuck_threshold).await;
    });
    let health_ctx = ctx.clone();
    tokio::spawn(async move {
        let (watch_interval, stuck_threshold) = resolve_stuck_watch_config();
        health_check_watcher_loop(health_ctx, watch_interval, stuck_threshold).await;
    });
    let prompt_pending_ctx = ctx.clone();
    tokio::spawn(async move {
        let (watch_interval, _) = resolve_stuck_watch_config();
        prompt_pending_unpark_watcher_loop(prompt_pending_ctx, watch_interval).await;
    });
    let master_watch_ctx = ctx.clone();
    tokio::spawn(async move {
        let interval = crate::monitor::master_watch::resolve_master_watch_patrol_interval();
        crate::monitor::master_watch::master_watch_patrol_loop(master_watch_ctx, interval).await;
    });

    tokio::spawn(async move {
        loop {
            if let Err(err) = run_once(&ctx).await {
                tracing::warn!(error = %err, "orchestrator tick failed");
            }
            WAKER.notified().await;
        }
    });
}

pub(crate) async fn run_once(ctx: &Ctx) -> Result<bool, AhError> {
    run_once_with_recovery_respawn(ctx, spawn_realign_agent_for_recovery).await
}

async fn run_once_with_recovery_respawn(
    ctx: &Ctx,
    respawn: RecoveryRespawnFn,
) -> Result<bool, AhError> {
    let queued_agent_ids = db::jobs::query_agent_ids_with_queued_jobs(ctx.db.clone()).await?;
    let mut did_work = false;

    for agent_id in queued_agent_ids {
        let readiness = wait_for_dispatchable_idle(
            ctx,
            &agent_id,
            Duration::from_secs(2),
            Duration::from_millis(50),
        )
        .await?;
        match readiness {
            DispatchReadiness::Ready => {}
            DispatchReadiness::Deferred { reason, retry } => {
                did_work = true;
                emit_dispatch_deferred(ctx, &agent_id, &reason).await?;
                if retry {
                    schedule_dispatch_retry(Duration::from_millis(100));
                }
                continue;
            }
        }
        let Some(agent) = db::agents::query_agent(ctx.db.clone(), agent_id.clone()).await? else {
            emit_dispatch_deferred(ctx, &agent_id, "target_missing").await?;
            did_work = true;
            continue;
        };
        if !db::jobs::has_queued_job(ctx.db.clone(), agent.id.clone()).await? {
            continue;
        }

        let registered_pane = crate::agent_io::pane_id(&agent.id);
        let Some(pane_id) = registered_pane else {
            let Some(dispatched) = dispatch_queued_job(ctx, &agent.id).await? else {
                continue;
            };
            did_work = true;
            mark_dispatch_io_failed(ctx, &agent.id, "tmux pane not registered").await;
            let _ = db::jobs::mark_job_failed(
                ctx.db.clone(),
                dispatched.job.id,
                "tmux pane not registered".to_string(),
            )
            .await;
            wake_up();
            continue;
        };
        let pane_id = resolve_current_dispatch_pane(ctx, &agent.id, pane_id).await;

        if parser_registry::get(&agent.id).is_none() {
            let Some(dispatched) = dispatch_queued_job(ctx, &agent.id).await? else {
                continue;
            };
            did_work = true;
            mark_dispatch_io_failed(ctx, &agent.id, "parser not registered").await;
            let _ = db::jobs::mark_job_failed(
                ctx.db.clone(),
                dispatched.job.id,
                "parser not registered".to_string(),
            )
            .await;
            wake_up();
            continue;
        }

        match run_dispatch_guard(ctx, &agent.id, &agent.provider, pane_id.clone()).await? {
            DispatchGuardOutcome::Permit => {}
            DispatchGuardOutcome::Refuse { retry } => {
                did_work = true;
                if retry {
                    wake_up();
                }
                continue;
            }
        }

        let Some(dispatched) = dispatch_queued_job(ctx, &agent.id).await? else {
            continue;
        };
        let job = dispatched.job;
        did_work = true;

        crate::agent_io::set_idle_scan_enabled(&agent.id, false);
        let capture_baseline = ctx.tmux_server.capture_pane(pane_id.clone()).await.ok();
        prepare_log_monitor_before_send(ctx, &agent.session_id, &agent.id, &agent.provider);
        run_before_dispatch_send_hook_for_test(ctx, &agent.id, pane_id.clone()).await;
        match wait_for_pre_send_dispatchable(ctx, &agent.id, &job.id).await? {
            DispatchReadiness::Ready => {}
            DispatchReadiness::Deferred { reason, retry } => {
                crate::completion::registry::cancel(&agent.id);
                crate::agent_io::set_idle_scan_enabled(&agent.id, true);
                let changed = db::jobs::requeue_dispatched_job_before_send(
                    ctx.db.clone(),
                    job.id.clone(),
                    agent.id.clone(),
                    format!("dispatch readiness recheck refused before send: {reason}"),
                )
                .await?;
                tracing::warn!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    pane_id = %pane_id.0,
                    reason,
                    changed,
                    "dispatch readiness recheck refused before send; requeued job without sending"
                );
                if retry {
                    schedule_dispatch_retry(Duration::from_millis(100));
                }
                continue;
            }
        }
        match run_dispatch_guard(ctx, &agent.id, &agent.provider, pane_id.clone()).await? {
            DispatchGuardOutcome::Permit => {}
            DispatchGuardOutcome::Refuse { retry } => {
                crate::completion::registry::cancel(&agent.id);
                crate::agent_io::set_idle_scan_enabled(&agent.id, true);
                let changed = db::jobs::requeue_dispatched_job_before_send(
                    ctx.db.clone(),
                    job.id.clone(),
                    agent.id.clone(),
                    "dispatch readiness recheck refused before send".to_string(),
                )
                .await?;
                tracing::warn!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    pane_id = %pane_id.0,
                    changed,
                    "dispatch readiness recheck refused before send; requeued job without sending"
                );
                if retry {
                    wake_up();
                }
                continue;
            }
        }

        let press_enter_after_paste =
            !(agent.provider == "antigravity" && job.prompt_text.ends_with('\n'));
        let send_result = crate::agent_io::send_text_to_pane_with_options(
            ctx.tmux_server.clone(),
            &agent.id,
            pane_id.clone(),
            job.prompt_text.clone(),
            press_enter_after_paste,
        )
        .await;

        if let Err(err) = send_result {
            crate::completion::registry::cancel(&agent.id);
            if stale_dispatch_failure_already_requeued(ctx, &agent.id, &job, &pane_id, &err).await?
            {
                did_work = true;
                wake_up();
                continue;
            }
            if maybe_requeue_recovered_pane_missing_dispatch(ctx, &agent.id, &job, &pane_id, &err)
                .await?
            {
                did_work = true;
                continue;
            }
            tracing::warn!(
                agent_id = %agent.id,
                error = %err,
                "send failed; cancelled completion log monitor"
            );
            mark_dispatch_io_failed(ctx, &agent.id, &format!("send failed: {err}")).await;
            match db::jobs::mark_job_failed(
                ctx.db.clone(),
                job.id.clone(),
                format!("send failed: {err}"),
            )
            .await
            {
                Ok(changes) if changes > 0 => tracing::warn!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    pane_id = %pane_id.0,
                    error = %err,
                    "dispatch send failure marked job failed"
                ),
                Ok(_) => tracing::warn!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    pane_id = %pane_id.0,
                    error = %err,
                    "dispatch send failure did not mark job failed because job state changed"
                ),
                Err(mark_err) => tracing::warn!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    pane_id = %pane_id.0,
                    error = %mark_err,
                    send_error = %err,
                    "failed to mark job failed after dispatch send failure"
                ),
            }
            wake_up();
            continue;
        }
        if db::jobs::recovery_requeued_attempt(job.error_reason.as_deref()).is_some() {
            match db::jobs::clear_recovered_dispatch_marker(ctx.db.clone(), job.id.clone()).await {
                Ok(changes) if changes > 0 => tracing::info!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    "cleared recovered dispatch retry marker after successful send"
                ),
                Ok(_) => tracing::debug!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    "recovered dispatch retry marker already cleared"
                ),
                Err(err) => tracing::warn!(
                    agent_id = %agent.id,
                    job_id = %job.id,
                    error = %err,
                    "failed to clear recovered dispatch retry marker after successful send"
                ),
            }
        }
        spawn_dispatch_ack_stability_busy(ctx.db.clone(), agent.id.clone());

        if let Some(parser_handle) = parser_registry::get(&agent.id) {
            let marker_handle = spawn_marker_timer_task_with_prompt(
                agent.id.clone(),
                TimerKind::Busy,
                Arc::new(ctx.db.clone()),
                parser_handle.clone(),
                Some(PromptTimerScanContext {
                    provider: agent.provider.clone(),
                    state_dir: ctx.state_dir.clone(),
                    tmux: ctx.tmux_server.clone(),
                    matcher: Arc::new(MarkerMatcher::from_manifest(
                        &crate::provider::manifest::get_manifest(&agent.provider),
                    )),
                }),
            );
            registry::register(agent.id.clone(), marker_handle);
            let manifest = crate::provider::manifest::get_manifest(&agent.provider);
            let matcher = MarkerMatcher::from_manifest(&manifest);
            if let Some(capture_baseline) = capture_baseline {
                crate::rpc::handlers::spawn_new_capture_seed(
                    ctx.db.clone(),
                    ctx.tmux_server.clone(),
                    agent.id.clone(),
                    agent.provider.clone(),
                    ctx.state_dir.clone(),
                    capture_baseline,
                    Arc::new(matcher),
                );
            }
            drop(parser_handle);
        }
    }

    if run_recovery_once_with_respawn(ctx, respawn).await? {
        did_work = true;
        wake_up();
    }

    Ok(did_work)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DispatchReadiness {
    Ready,
    Deferred { reason: String, retry: bool },
}

async fn wait_for_dispatchable_idle(
    ctx: &Ctx,
    agent_id: &str,
    max_wait: Duration,
    poll: Duration,
) -> Result<DispatchReadiness, AhError> {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let state = db::agents::query_agent_state(ctx.db.clone(), agent_id.to_string()).await?;
        let Some((state, _)) = state else {
            return Ok(DispatchReadiness::Deferred {
                reason: "target_missing".to_string(),
                retry: false,
            });
        };
        match state.as_str() {
            db::state_machine::STATE_IDLE => return Ok(DispatchReadiness::Ready),
            db::state_machine::STATE_BUSY | db::state_machine::STATE_WAITING_FOR_ACK => {
                if db::jobs::query_dispatched_job_for_agent(ctx.db.clone(), agent_id.to_string())
                    .await?
                    .is_some()
                {
                    return Ok(DispatchReadiness::Deferred {
                        reason: "target_not_idle".to_string(),
                        retry: false,
                    });
                }
                if max_wait.is_zero() || tokio::time::Instant::now() >= deadline {
                    return Ok(DispatchReadiness::Deferred {
                        reason: "target_not_idle".to_string(),
                        retry: true,
                    });
                }
                tokio::time::sleep(poll).await;
            }
            db::state_machine::STATE_PROMPT_PENDING
            | db::state_machine::STATE_UNKNOWN
            | db::state_machine::STATE_STUCK
            | db::state_machine::STATE_CRASHED
            | db::state_machine::STATE_KILLED => {
                return Ok(DispatchReadiness::Deferred {
                    reason: "target_not_idle".to_string(),
                    retry: false,
                });
            }
            _ => {
                return Ok(DispatchReadiness::Deferred {
                    reason: "target_not_idle".to_string(),
                    retry: false,
                });
            }
        }
    }
}

async fn wait_for_pre_send_dispatchable(
    ctx: &Ctx,
    agent_id: &str,
    job_id: &str,
) -> Result<DispatchReadiness, AhError> {
    let state = db::agents::query_agent_state(ctx.db.clone(), agent_id.to_string()).await?;
    let Some((state, _)) = state else {
        return Ok(DispatchReadiness::Deferred {
            reason: "target_missing".to_string(),
            retry: false,
        });
    };
    if state != db::state_machine::STATE_WAITING_FOR_ACK {
        return Ok(DispatchReadiness::Deferred {
            reason: "target_not_waiting_for_ack".to_string(),
            retry: false,
        });
    }
    let dispatched =
        db::jobs::query_dispatched_job_for_agent(ctx.db.clone(), agent_id.to_string()).await?;
    if dispatched.as_ref().is_some_and(|job| job.id == job_id) {
        Ok(DispatchReadiness::Ready)
    } else {
        Ok(DispatchReadiness::Deferred {
            reason: "current_job_not_dispatched".to_string(),
            retry: false,
        })
    }
}

async fn emit_dispatch_deferred(ctx: &Ctx, agent_id: &str, reason: &str) -> Result<(), AhError> {
    db::events::insert_event(
        ctx.db.clone(),
        agent_id.to_string(),
        None,
        "dispatch_deferred".to_string(),
        json!({
            "reason": reason,
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

fn schedule_dispatch_retry(delay: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        wake_up();
    });
}

#[allow(dead_code)]
async fn run_recovery_once(ctx: &Ctx) -> Result<bool, AhError> {
    run_recovery_once_with_respawn(ctx, spawn_realign_agent_for_recovery).await
}

type RecoveryRespawnFuture<'a> = Pin<Box<dyn Future<Output = Result<(), AhError>> + Send + 'a>>;
type RecoveryRespawnFn =
    for<'a> fn(&'a Ctx, &'a str, &'a RealignAgentParams, &'a str) -> RecoveryRespawnFuture<'a>;

fn spawn_realign_agent_for_recovery<'a>(
    ctx: &'a Ctx,
    session_id: &'a str,
    agent: &'a RealignAgentParams,
    expected_hash: &'a str,
) -> RecoveryRespawnFuture<'a> {
    Box::pin(async move {
        // ISSUE-13 §3a: recovery replays `agent.env` = the stored BARE snapshot env (which
        // already has config_env baked in), so an empty config_env here re-merges to the
        // same bare env and recomputes the same hash.
        let config_env = std::collections::HashMap::new();
        spawn_realign_agent(
            ctx,
            session_id,
            agent,
            expected_hash,
            true,
            true,
            &config_env,
            None,
        )
        .await
    })
}

async fn run_recovery_once_with_respawn(
    ctx: &Ctx,
    respawn: RecoveryRespawnFn,
) -> Result<bool, AhError> {
    let now = unix_timestamp();

    // P0-2: check stable agents and confirm them stable (clear backoff) after N=300 seconds
    {
        let conn = ctx.db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT id, created_at FROM agents \
             WHERE state != 'CRASHED' AND state != 'KILLED' \
               AND (retry_count > 0 OR next_retry_at IS NOT NULL OR retry_exhausted != 0)",
            )
            .map_err(|err| {
                AhError::DbConstraintViolation(format!("prepare query stable agents: {err}"))
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|err| AhError::DbConstraintViolation(format!("query stable agents: {err}")))?;
        for r in rows {
            let (id, created_at) = r.map_err(|err| {
                AhError::DbConstraintViolation(format!("collect stable agent: {err}"))
            })?;
            if now - created_at >= 300 {
                crate::db::recovery::confirm_agent_stable_sync(&conn, &id)?;
                tracing::info!(agent_id = %id, "confirmed agent stable, cleared backoff");
            }
        }
    }

    let crashed_agents =
        db::agents::query_agents_by_state(ctx.db.clone(), "CRASHED".to_string()).await?;

    for agent in crashed_agents {
        if !is_recovery_eligible_provider(&agent.provider) {
            continue;
        }
        let (retry_exhausted, next_retry_at): (i64, Option<i64>) = {
            let conn = ctx.db.conn();
            conn.query_row(
                "SELECT retry_exhausted, next_retry_at FROM agents WHERE id = ?",
                params![agent.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|err| {
                AhError::DbConstraintViolation(format!("query recovery candidate: {err}"))
            })?
        };
        if retry_exhausted != 0 || next_retry_at.is_some_and(|retry_at| retry_at > now) {
            continue;
        }

        let intent = {
            let conn = ctx.db.conn();
            crate::db::recovery::query_agent_recovery_intent_sync(&conn, &agent.id)?
        };
        let Some(intent) = intent else {
            tracing::warn!(
                agent_id = %agent.id,
                "skipping crashed worker recovery because recovery intent is missing"
            );
            let already_emitted = {
                let conn = ctx.db.conn();
                self_recovery_event_exists(&conn, &agent.id, "skipped", "missing_recovery_intent")?
            };
            if !already_emitted {
                emit_self_recovery_attempt_event(
                    ctx,
                    &agent.id,
                    "skipped",
                    "missing_recovery_intent",
                    0,
                    None,
                    agent.state_version,
                    None,
                    None,
                    None,
                )
                .await?;
            }
            continue;
        };
        if intent.action == RecoveryIntentAction::ReapOnly {
            tracing::info!(
                agent_id = %agent.id,
                previous_state = %intent.previous_state,
                "reaping crashed worker without respawn because recovery intent is REAP_ONLY"
            );
            crate::agent_io::cleanup_agent_runtime_resources(&agent.id, Some(&agent.session_id));
            db::agents::delete_agent(ctx.db.clone(), agent.id.clone()).await?;
            return Ok(true);
        }

        if intent.action == RecoveryIntentAction::ReviveIdle
            && !session_is_active(ctx, &intent.session_id)?
        {
            tracing::info!(
                agent_id = %agent.id,
                session_id = %intent.session_id,
                previous_state = %intent.previous_state,
                action = intent.action.db_str(),
                "reaping idle crashed worker without respawn because session is not ACTIVE"
            );
            crate::agent_io::cleanup_agent_runtime_resources(&agent.id, Some(&agent.session_id));
            db::agents::delete_agent(ctx.db.clone(), agent.id.clone()).await?;
            return Ok(true);
        }

        let has_snapshot = {
            let conn = ctx.db.conn();
            crate::db::recovery::query_agent_spawn_spec_sync(&conn, &agent.id)?.is_some()
        };
        if !has_snapshot {
            let already_emitted = {
                let conn = ctx.db.conn();
                self_recovery_event_exists(&conn, &agent.id, "skipped", "missing_spawn_spec")?
            };
            if !already_emitted {
                emit_self_recovery_attempt_event(
                    ctx,
                    &agent.id,
                    "skipped",
                    "missing_spawn_spec",
                    0,
                    None,
                    agent.state_version,
                    None,
                    None,
                    None,
                )
                .await?;
            }
            continue;
        }

        let claimed = {
            let conn = ctx.db.conn();
            crate::db::recovery::try_claim_agent_recovery_sync(
                &conn,
                &agent.id,
                agent.state_version,
                now,
            )?
        };
        if !claimed {
            tracing::debug!(
                agent_id = %agent.id,
                "recovery CAS lost, external state change"
            );
            continue;
        }

        recover_crashed_agent_from_snapshot_with_respawn_and_intent(
            ctx,
            &agent.id,
            respawn,
            Some(intent),
        )
        .await?;
        return Ok(true);
    }

    Ok(false)
}

#[allow(dead_code)] // direct test seam; production passes captured intent through recovery loop
async fn recover_crashed_agent_from_snapshot_with_respawn(
    ctx: &Ctx,
    agent_id: &str,
    respawn: RecoveryRespawnFn,
) -> Result<(), AhError> {
    recover_crashed_agent_from_snapshot_with_respawn_and_intent(ctx, agent_id, respawn, None).await
}

async fn recover_crashed_agent_from_snapshot_with_respawn_and_intent(
    ctx: &Ctx,
    agent_id: &str,
    respawn: RecoveryRespawnFn,
    captured_intent: Option<crate::db::recovery::AgentRecoveryIntent>,
) -> Result<(), AhError> {
    let (agent, stored, backoff) = {
        let conn = ctx.db.conn();
        let agent = db::agents::query_agent_sync(&conn, agent_id)?
            .ok_or_else(|| AhError::AgentNotFound(agent_id.to_string()))?;
        let stored = crate::db::recovery::query_agent_spawn_spec_sync(&conn, agent_id)?
            .ok_or_else(|| {
                AhError::DbConstraintViolation(format!("missing spawn spec for {agent_id}"))
            })?;
        let backoff: (i64, Option<i64>, i64) = conn
            .query_row(
                "SELECT retry_count, next_retry_at, retry_exhausted FROM agents WHERE id = ?",
                params![agent_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|err| {
                AhError::DbConstraintViolation(format!("query agent backoff for recovery: {err}"))
            })?
            .unwrap_or((0, None, 0));
        (agent, stored, backoff)
    };
    let crash_context = CrashContext {
        exit_code: agent.exit_code,
        error_code: agent.error_code.clone(),
    };
    let params = RealignAgentParams {
        agent_id: stored.spec.agent_id.clone(),
        provider: stored.spec.provider.clone(),
        env: stored.spec.env.clone(),
        hooks: stored.spec.hooks.clone(),
        plugins: stored.spec.plugins.clone(),
        skills: stored.spec.skills.clone(),
        bundle: stored.spec.bundle.clone(),
        settings: stored.spec.settings.clone(),
        bundle_digest: stored.spec.bundle_digest.clone(),
        sandbox_overrides: stored.spec.sandbox_overrides.clone(),
        hook_push_enabled: stored.spec.hook_push_enabled,
        claude_shared_credentials_dir: None,
    };

    db::agents::delete_agent(ctx.db.clone(), agent_id.to_string()).await?;
    let spawn_result = respawn(ctx, &agent.session_id, &params, &stored.config_hash).await;
    if let Err(err) = &spawn_result {
        tracing::error!(
            agent_id,
            error = %err,
            "self recovery respawn failed; restoring crashed agent row and spawn snapshot"
        );
        restore_crashed_agent_after_recovery_spawn_failure(ctx, &agent, &stored)?;
        {
            let conn = ctx.db.conn();
            conn.execute(
                "UPDATE agents SET retry_count = ?, next_retry_at = ?, retry_exhausted = ? WHERE id = ?",
                params![backoff.0, backoff.1, backoff.2, agent_id],
            )
            .map_err(|err| AhError::DbConstraintViolation(format!("restore agent backoff on spawn failure: {err}")))?;
        }
        if let Some(intent) = captured_intent.as_ref() {
            let conn = ctx.db.conn();
            crate::db::recovery::persist_agent_recovery_intent_sync(&conn, intent)?;
        }
    } else {
        {
            let conn = ctx.db.conn();
            conn.execute(
                "UPDATE agents SET retry_count = ?, next_retry_at = ?, retry_exhausted = ? WHERE id = ?",
                params![backoff.0, backoff.1, backoff.2, agent_id],
            )
            .map_err(|err| AhError::DbConstraintViolation(format!("restore agent backoff on spawn success: {err}")))?;
        }
        if let Some(intent) = captured_intent.as_ref() {
            let requeued =
                crate::db::recovery::requeue_interrupted_job_from_captured_intent_standalone_sync(
                    &ctx.db, intent,
                )?;
            if requeued > 0 {
                crate::db::jobs::notify_runtime_job_changed();
            }
        }
    }
    apply_recovery_spawn_result_with_action(
        ctx,
        agent_id,
        spawn_result,
        unix_timestamp(),
        Some(crash_context),
        captured_intent.as_ref().map(|intent| intent.action.clone()),
    )
    .await
}

fn session_is_active(ctx: &Ctx, session_id: &str) -> Result<bool, AhError> {
    let conn = ctx.db.conn();
    let status = conn
        .query_row(
            "SELECT status FROM sessions WHERE id = ?",
            params![session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| {
            AhError::DbConstraintViolation(format!("query session status for recovery: {err}"))
        })?;
    Ok(status.as_deref() == Some("ACTIVE"))
}

#[derive(Debug, Clone)]
struct CrashContext {
    exit_code: Option<i64>,
    error_code: Option<String>,
}

fn self_recovery_event_exists(
    conn: &rusqlite::Connection,
    agent_id: &str,
    action: &str,
    reason: &str,
) -> Result<bool, AhError> {
    conn.query_row(
        "SELECT 1 FROM events \
         WHERE agent_id = ? AND event_type = 'self_recovery_attempt' \
           AND payload LIKE ? AND payload LIKE ? \
         LIMIT 1",
        params![
            agent_id,
            format!("%\"action\":\"{action}\"%"),
            format!("%\"reason\":\"{reason}\"%"),
        ],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(|err| AhError::DbConstraintViolation(format!("query recovery event: {err}")))
}

#[allow(dead_code)] // direct test seam for recovery event/backoff handling
async fn apply_recovery_spawn_result(
    ctx: &Ctx,
    agent_id: &str,
    spawn_result: Result<(), AhError>,
    now: i64,
    crash_context: Option<CrashContext>,
) -> Result<(), AhError> {
    apply_recovery_spawn_result_with_action(ctx, agent_id, spawn_result, now, crash_context, None)
        .await
}

async fn apply_recovery_spawn_result_with_action(
    ctx: &Ctx,
    agent_id: &str,
    spawn_result: Result<(), AhError>,
    now: i64,
    crash_context: Option<CrashContext>,
    recovery_action: Option<RecoveryIntentAction>,
) -> Result<(), AhError> {
    enum RecoveryEvent {
        Recovered {
            retry_count: i64,
            next_retry_at: Option<i64>,
            state_version: i64,
            crash_context: Option<CrashContext>,
        },
        Failed {
            retry_count: i64,
            next_retry_at: Option<i64>,
            state_version: i64,
            error: String,
        },
    }

    let event = {
        let conn = ctx.db.conn();
        match spawn_result {
            Ok(()) => {
                crate::db::recovery::record_recovery_failure_backoff_sync(&conn, agent_id, now)?;
                let (retry_count, next_retry_at, state_version) =
                    recovery_event_state(&conn, agent_id)?;
                RecoveryEvent::Recovered {
                    retry_count,
                    next_retry_at,
                    state_version,
                    crash_context,
                }
            }
            Err(err) => {
                let stored = crate::db::recovery::query_agent_spawn_spec_sync(&conn, agent_id)?;
                conn.execute(
                    "UPDATE agents \
                     SET state = 'CRASHED', state_version = state_version + 1, updated_at = unixepoch() \
                     WHERE id = ?",
                    params![agent_id],
                )
                .map_err(|err| {
                    AhError::DbConstraintViolation(format!(
                        "restore crashed recovery state: {err}"
                    ))
                })?;
                if let Some(stored) = stored {
                    crate::db::recovery::persist_agent_spawn_spec_sync(
                        &conn,
                        &stored.spec,
                        &stored.config_hash,
                    )?;
                }
                let backoff = crate::db::recovery::record_recovery_failure_backoff_sync(
                    &conn, agent_id, now,
                )?;
                let (_, _, state_version) = recovery_event_state(&conn, agent_id)?;
                let error_message = recovery_error_message(&err);
                RecoveryEvent::Failed {
                    retry_count: backoff.retry_count,
                    next_retry_at: backoff.next_retry_at,
                    state_version,
                    error: error_message,
                }
            }
        }
    };

    match event {
        RecoveryEvent::Recovered {
            retry_count,
            next_retry_at,
            state_version,
            crash_context,
        } => {
            emit_self_recovery_attempt_event(
                ctx,
                agent_id,
                "recovered",
                "OOM_RECOVERY",
                retry_count,
                next_retry_at,
                state_version,
                None,
                crash_context.as_ref(),
                recovery_action.as_ref(),
            )
            .await?;
        }
        RecoveryEvent::Failed {
            retry_count,
            next_retry_at,
            state_version,
            error,
        } => {
            emit_self_recovery_attempt_event(
                ctx,
                agent_id,
                "failed",
                "OOM_RECOVERY",
                retry_count,
                next_retry_at,
                state_version,
                Some(error.as_str()),
                None,
                recovery_action.as_ref(),
            )
            .await?;
        }
    }
    Ok(())
}

async fn emit_self_recovery_attempt_event(
    ctx: &Ctx,
    agent_id: &str,
    action: &str,
    reason: &str,
    retry_count: i64,
    next_retry_at: Option<i64>,
    state_version: i64,
    error: Option<&str>,
    recovered_from: Option<&CrashContext>,
    recovery_action: Option<&RecoveryIntentAction>,
) -> Result<i64, AhError> {
    let mut payload = json!({
        "agent_id": agent_id,
        "reason": reason,
        "action": action,
        "retry_count": retry_count,
        "next_retry_at": next_retry_at,
        "state_version": state_version,
    });
    if let Some(error) = error {
        payload["error"] = json!(error);
    }
    if let Some(recovery_action) = recovery_action {
        payload["recovery_action"] = json!(recovery_action.db_str());
        if recovery_action == &RecoveryIntentAction::ReviveIdle {
            payload["recovery_kind"] = json!("idle_topology_restore");
        }
    }
    if let Some(recovered_from) = recovered_from {
        if let Some(exit_code) = recovered_from.exit_code {
            payload["recovered_from_exit_code"] = json!(exit_code);
        }
        if let Some(error_code) = recovered_from.error_code.as_deref() {
            payload["recovered_from_error_code"] = json!(error_code);
        }
    }
    db::events::insert_event(
        ctx.db.clone(),
        agent_id.to_string(),
        None,
        "self_recovery_attempt".to_string(),
        payload.to_string(),
    )
    .await
}

fn restore_crashed_agent_after_recovery_spawn_failure(
    ctx: &Ctx,
    agent: &crate::db::schema::Agent,
    stored: &crate::db::recovery::StoredAgentSpawnSpec,
) -> Result<(), AhError> {
    let conn = ctx.db.conn();
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM agents WHERE id = ? LIMIT 1",
            params![agent.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| AhError::DbConstraintViolation(format!("query restored agent: {err}")))?;
    if exists.is_none() {
        db::agents::insert_agent_sync(
            &conn,
            &agent.id,
            &agent.session_id,
            &agent.provider,
            "CRASHED",
            None,
        )?;
        conn.execute(
            "UPDATE agents \
             SET config_hash = ?, state_version = ?, retry_count = 0, next_retry_at = NULL, \
                 retry_exhausted = 0, updated_at = unixepoch() \
             WHERE id = ?",
            params![stored.config_hash, agent.state_version, agent.id],
        )
        .map_err(|err| {
            AhError::DbConstraintViolation(format!("restore crashed agent row: {err}"))
        })?;
    }
    crate::db::recovery::persist_agent_spawn_spec_sync(&conn, &stored.spec, &stored.config_hash)
}

fn recovery_event_state(
    conn: &rusqlite::Connection,
    agent_id: &str,
) -> Result<(i64, Option<i64>, i64), AhError> {
    conn.query_row(
        "SELECT retry_count, next_retry_at, state_version FROM agents WHERE id = ?",
        params![agent_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .map_err(|err| AhError::DbConstraintViolation(format!("query recovery event state: {err}")))
}

fn recovery_error_message(err: &AhError) -> String {
    match err {
        AhError::PtyIoError(message) => message.clone(),
        AhError::PtyOpenFailed(message) => message.clone(),
        AhError::EnvironmentNotSupported { details }
        | AhError::SandboxUserNsDisabled { details }
        | AhError::SandboxMountFailed { details }
        | AhError::AgentUnexpectedExit { details }
        | AhError::StartupMarkerTimeout { details }
        | AhError::PtyMarkerTimeout { details } => details.clone(),
        AhError::TmuxCommandFailed { stderr, .. } => stderr.clone(),
        _ => err.to_string(),
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

async fn dispatch_queued_job(
    ctx: &Ctx,
    agent_id: &str,
) -> Result<Option<db::jobs::DispatchedJob>, AhError> {
    db::jobs::dispatch_job_to_agent(
        ctx.db.clone(),
        agent_id.to_string(),
        vec![db::state_machine::STATE_IDLE.to_string()],
        db::state_machine::STATE_WAITING_FOR_ACK.to_string(),
        "command_received".to_string(),
        json!({ "status": "SENT" }),
    )
    .await
}

enum DispatchGuardOutcome {
    Permit,
    Refuse { retry: bool },
}

async fn run_dispatch_guard(
    ctx: &Ctx,
    agent_id: &str,
    provider: &str,
    pane_id: TmuxPaneId,
) -> Result<DispatchGuardOutcome, AhError> {
    if !is_prompt_handling_provider(provider) {
        return Ok(DispatchGuardOutcome::Permit);
    }

    let manifest = crate::provider::manifest::get_manifest(provider);
    let marker_matcher = Arc::new(MarkerMatcher::from_manifest(&manifest));
    let result = scan_prompt_and_apply_outcome(PromptScanRequest {
        db: ctx.db.clone(),
        agent_id: agent_id.to_string(),
        provider: provider.to_string(),
        pane_id,
        tmux: ctx.tmux_server.clone(),
        state_dir: ctx.state_dir.clone(),
        marker_matcher,
        max_depth: 3,
        scan_purpose: PromptScanPurpose::DispatchGuard,
    })
    .await;

    let retry = matches!(result, Ok(PromptScanDisposition::Handled { .. }));
    if dispatch_guard_scan_permits_dispatch(result) {
        Ok(DispatchGuardOutcome::Permit)
    } else {
        Ok(DispatchGuardOutcome::Refuse { retry })
    }
}

async fn resolve_current_dispatch_pane(
    ctx: &Ctx,
    agent_id: &str,
    pane_id: TmuxPaneId,
) -> TmuxPaneId {
    let session_name = crate::tmux::agent_session_name(agent_id);
    match ctx.tmux_server.list_panes(session_name.clone()).await {
        Ok(panes) if panes.iter().any(|pane| pane.0 == pane_id.0) => pane_id,
        Ok(panes) if panes.len() == 1 => {
            let refreshed = panes[0].clone();
            let expected_pid =
                match db::agents::query_agent(ctx.db.clone(), agent_id.to_string()).await {
                    Ok(Some(agent)) => agent.pid,
                    Ok(None) => {
                        tracing::warn!(
                            agent_id,
                            old_pane = %pane_id.0,
                            candidate_pane = %refreshed.0,
                            "could not refresh stale agent pane binding: agent row missing"
                        );
                        return pane_id;
                    }
                    Err(err) => {
                        tracing::warn!(
                            agent_id,
                            old_pane = %pane_id.0,
                            candidate_pane = %refreshed.0,
                            error = %err,
                            "could not refresh stale agent pane binding: failed to query agent pid"
                        );
                        return pane_id;
                    }
                };
            let Some(expected_pid) = expected_pid else {
                tracing::warn!(
                    agent_id,
                    old_pane = %pane_id.0,
                    candidate_pane = %refreshed.0,
                    "could not refresh stale agent pane binding: agent pid missing"
                );
                return pane_id;
            };
            let actual_pid = match ctx.tmux_server.get_pane_pid(refreshed.clone()).await {
                Ok(pid) => pid as i64,
                Err(err) => {
                    tracing::warn!(
                        agent_id,
                        old_pane = %pane_id.0,
                        candidate_pane = %refreshed.0,
                        error = %err,
                        "could not refresh stale agent pane binding: failed to query candidate pane pid"
                    );
                    return pane_id;
                }
            };
            if actual_pid != expected_pid {
                tracing::warn!(
                    agent_id,
                    old_pane = %pane_id.0,
                    candidate_pane = %refreshed.0,
                    expected_pid,
                    actual_pid,
                    "could not refresh stale agent pane binding: candidate pane pid does not match agent pid"
                );
                return pane_id;
            }
            let updated = crate::agent_io::update_pane_id(agent_id, refreshed.clone());
            tracing::warn!(
                agent_id,
                old_pane = %pane_id.0,
                new_pane = %refreshed.0,
                pid = actual_pid,
                updated,
                "refreshed stale agent pane binding before dispatch"
            );
            refreshed
        }
        Ok(panes) => {
            tracing::warn!(
                agent_id,
                pane_id = %pane_id.0,
                session_name,
                pane_count = panes.len(),
                "could not refresh stale agent pane binding: expected one live pane"
            );
            pane_id
        }
        Err(err) => {
            tracing::warn!(
                agent_id,
                pane_id = %pane_id.0,
                session_name,
                error = %err,
                "could not list agent panes before dispatch; using registered pane"
            );
            pane_id
        }
    }
}

fn dispatch_guard_scan_permits_dispatch(result: Result<PromptScanDisposition, AhError>) -> bool {
    match result {
        Ok(PromptScanDisposition::NoActionNeeded { .. }) => true,
        Ok(PromptScanDisposition::Handled { depth }) => {
            tracing::info!(
                depth,
                "dispatch guard handled interstitial before dispatch; leaving queued job for retry"
            );
            false
        }
        Ok(PromptScanDisposition::Deferred {
            depth,
            block_reason,
        })
        | Ok(PromptScanDisposition::Pending {
            depth,
            block_reason,
        }) => {
            tracing::warn!(
                depth,
                block_reason,
                "dispatch guard refused dispatch while prompt scan is unresolved"
            );
            false
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "dispatch guard scan failed closed before dispatch"
            );
            false
        }
    }
}

fn prepare_log_monitor_before_send(
    ctx: &Ctx,
    session_id: &str,
    agent_id: &str,
    provider: &str,
) -> bool {
    let root = match crate::completion::log_layout::resolve_agent_log_root(
        &ctx.state_dir,
        session_id,
        agent_id,
        provider,
        ctx.env_state.unsafe_no_sandbox,
    ) {
        crate::completion::log_layout::LogRootResolution::Available(root) => root,
        crate::completion::log_layout::LogRootResolution::Unavailable(unavailable) => {
            tracing::info!(
                agent_id,
                provider,
                reason = unavailable.reason,
                "log signal unavailable, UI-only completion"
            );
            return false;
        }
    };

    let cursors = match crate::completion::reader::collect_provider_log_cursors(provider, &root) {
        Ok(cursors) => cursors,
        Err(err) => {
            tracing::info!(
                agent_id,
                provider,
                root = %root.display(),
                error = %err,
                "log signal cursor baseline unavailable, UI-only completion"
            );
            return false;
        }
    };
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    let cursor_count = cursors.len();
    let file_list_count = cursor_count;
    let initial_state = crate::completion::reader::LogReadState::from_cursors(cursors);
    tracing::info!(
        agent_id,
        provider,
        root = %root.display(),
        cursor_count,
        file_list_count,
        "registered provider completion log monitor"
    );
    crate::completion::registry::register(
        agent_id.to_string(),
        crate::completion::registry::LogMonitorEntry {
            provider: provider.to_string(),
            log_root: root.clone(),
            state: initial_state.clone(),
            cancel_tx,
        },
    );
    crate::completion::monitor::spawn_log_monitor_task(
        ctx.db.clone(),
        agent_id.to_string(),
        provider.to_string(),
        root,
        initial_state,
        cancel_rx,
    );
    true
}

async fn mark_dispatch_io_failed(ctx: &Ctx, agent_id: &str, reason: &str) {
    if let Err(err) = db::state_machine::transit_agent_state(
        ctx.db.clone(),
        agent_id.to_string(),
        vec![db::state_machine::STATE_WAITING_FOR_ACK.to_string()],
        db::state_machine::STATE_STUCK.to_string(),
        Some(reason.to_string()),
    )
    .await
    {
        tracing::warn!(agent_id = %agent_id, error = %err, "failed to compensate dispatch IO failure");
    }
}

async fn stale_dispatch_failure_already_requeued(
    ctx: &Ctx,
    agent_id: &str,
    job: &db::schema::Job,
    pane_id: &TmuxPaneId,
    err: &AhError,
) -> Result<bool, AhError> {
    let current = db::jobs::query_job(ctx.db.clone(), job.id.clone()).await?;
    let Some(current) = current else {
        tracing::warn!(
            agent_id,
            job_id = %job.id,
            pane_id = %pane_id.0,
            error = %err,
            "dispatch send failed but job disappeared before failure compensation"
        );
        return Ok(false);
    };
    if current.agent_id == job.agent_id
        && current.status == "QUEUED"
        && db::jobs::recovery_requeued_attempt(current.error_reason.as_deref()).is_some()
    {
        tracing::warn!(
            agent_id,
            job_id = %job.id,
            pane_id = %pane_id.0,
            current_status = %current.status,
            current_error_reason = ?current.error_reason,
            error = %err,
            "stale dispatch send failed but job is already recovery-requeued; skipping failure compensation"
        );
        return Ok(true);
    }
    tracing::debug!(
        agent_id,
        job_id = %job.id,
        pane_id = %pane_id.0,
        current_status = %current.status,
        current_error_reason = ?current.error_reason,
        error = %err,
        "dispatch send failed and current job remains owned by dispatch failure path"
    );
    Ok(false)
}

async fn maybe_requeue_recovered_pane_missing_dispatch(
    ctx: &Ctx,
    agent_id: &str,
    job: &db::schema::Job,
    pane_id: &TmuxPaneId,
    err: &AhError,
) -> Result<bool, AhError> {
    if !dispatch_send_error_is_pane_missing(err) {
        return Ok(false);
    }
    let Some(attempt) = db::jobs::recovery_requeued_attempt(job.error_reason.as_deref()) else {
        tracing::warn!(
            agent_id = %agent_id,
            job_id = %job.id,
            pane_id = %pane_id.0,
            error = %err,
            "pane-missing dispatch failure is not marked recoverable; job will fail"
        );
        return Ok(false);
    };
    if attempt >= db::jobs::RECOVERY_REQUEUED_MAX_STALE_PANE_RETRIES {
        tracing::warn!(
            agent_id = %agent_id,
            job_id = %job.id,
            pane_id = %pane_id.0,
            attempt,
            max_attempts = db::jobs::RECOVERY_REQUEUED_MAX_STALE_PANE_RETRIES,
            error = %err,
            "recovered dispatch stale-pane retry limit exhausted; job will fail"
        );
        return Ok(false);
    }

    let reason = format!(
        "send failed on stale pane {pane_id}: {err}",
        pane_id = pane_id.0
    );
    let next_attempt = attempt + 1;
    let changed = db::jobs::requeue_recovered_dispatch_io_failure(
        ctx.db.clone(),
        job.id.clone(),
        agent_id.to_string(),
        next_attempt,
        reason.clone(),
    )
    .await?;
    if changed == 0 {
        tracing::warn!(
            agent_id = %agent_id,
            job_id = %job.id,
            pane_id = %pane_id.0,
            attempt,
            next_attempt,
            error = %err,
            "recovered dispatch stale-pane retry was skipped because job state changed"
        );
        return Ok(false);
    }
    tracing::warn!(
        agent_id = %agent_id,
        job_id = %job.id,
        pane_id = %pane_id.0,
        attempt,
        next_attempt,
        error = %err,
        "requeued recovered job after stale-pane dispatch failure"
    );
    Ok(true)
}

fn dispatch_send_error_is_pane_missing(err: &AhError) -> bool {
    match err {
        AhError::TmuxCommandFailed { stderr, .. } => {
            stderr.contains("can't find pane") || stderr.contains("can't find window")
        }
        _ => err.to_string().contains("can't find pane"),
    }
}

fn spawn_dispatch_ack_stability_busy(db: db::Db, agent_id: String) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(
            crate::rpc::handlers::CAPTURE_SEED_STABILITY_MS,
        ))
        .await;
        if let Err(err) = db::state_machine::transit_agent_state(
            db,
            agent_id.clone(),
            vec![db::state_machine::STATE_WAITING_FOR_ACK.to_string()],
            db::state_machine::STATE_BUSY.to_string(),
            Some("DISPATCH_ACK_STABLE".to_string()),
        )
        .await
        {
            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent BUSY after dispatch ACK stability window");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        RecoveryRespawnFuture, apply_recovery_spawn_result, dispatch_guard_scan_permits_dispatch,
        prepare_log_monitor_before_send, recover_crashed_agent_from_snapshot_with_respawn,
        resolve_current_dispatch_pane, run_once, run_once_with_recovery_respawn, run_recovery_once,
        run_recovery_once_with_respawn, set_before_dispatch_send_hook_for_test, wake_up,
    };
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::recovery::{
        AgentRecoveryIntent, AgentSpawnSpec, CapturedInterruptedJob, RecoveryIntentAction,
        persist_agent_recovery_intent_sync, persist_agent_spawn_spec_sync,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::error::AhError;
    use crate::prompt_handler::integration::{
        PromptScanDisposition, apply_prompt_pending_unpark_outcome_sync,
    };
    use crate::rpc::Ctx;
    use crate::rpc::handlers::RealignAgentParams;
    use crate::sandbox::EnvState;
    use crate::tmux::{TmuxPaneId, TmuxServer};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::{Duration, Instant};

    static BEFORE_DISPATCH_SEND_HOOK_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    struct AgentGlobalCleanup {
        agent_id: String,
    }

    impl AgentGlobalCleanup {
        fn new(agent_id: &str) -> Self {
            crate::completion::registry::cancel(agent_id);
            let _ = crate::agent_io::remove(agent_id);
            let _ = crate::marker::parser_registry::remove(agent_id);
            Self {
                agent_id: agent_id.to_string(),
            }
        }
    }

    impl Drop for AgentGlobalCleanup {
        fn drop(&mut self) {
            crate::completion::registry::cancel(&self.agent_id);
            let _ = crate::agent_io::remove(&self.agent_id);
            let _ = crate::marker::parser_registry::remove(&self.agent_id);
            let _ = crate::marker::registry::take(&self.agent_id)
                .map(|handle| handle.cancel_tx.send(()));
        }
    }

    fn test_ctx() -> Ctx {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        Ctx {
            db: db::init(file.path()).unwrap(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(&state_dir)),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        }
    }

    fn seed_session(conn: &rusqlite::Connection) {
        insert_session_sync(conn, "s1", "p1", "/tmp/foo").unwrap();
    }

    fn sample_spawn_spec(agent_id: &str, provider: &str) -> AgentSpawnSpec {
        AgentSpawnSpec {
            agent_id: agent_id.to_string(),
            provider: provider.to_string(),
            env: HashMap::from([("FOO".to_string(), "bar".to_string())]),
            hooks: HashMap::new(),
            plugins: vec!["github@openai-curated".to_string()],
            skills: Vec::new(),
            bundle: Vec::new(),
            settings: serde_json::Map::new(),
            bundle_digest: None,
            sandbox_overrides: Default::default(),
            hook_push_enabled: false,
        }
    }

    fn sample_recovery_intent(
        agent_id: &str,
        action: RecoveryIntentAction,
        interrupted_job_id: Option<&str>,
    ) -> AgentRecoveryIntent {
        AgentRecoveryIntent {
            agent_id: agent_id.to_string(),
            session_id: "s1".to_string(),
            provider: "codex".to_string(),
            previous_state: match &action {
                RecoveryIntentAction::Revive => "BUSY",
                RecoveryIntentAction::ReviveIdle => "IDLE",
                RecoveryIntentAction::ReapOnly => "IDLE",
            }
            .to_string(),
            crashed_state_version: 7,
            interrupted_job_id: interrupted_job_id.map(str::to_string),
            interrupted_job_status: interrupted_job_id.map(|_| "DISPATCHED".to_string()),
            interrupted_job: interrupted_job_id.map(|job_id| CapturedInterruptedJob {
                id: job_id.to_string(),
                request_id: None,
                prompt_text: "continue\n".to_string(),
                cancel_requested: false,
                requires_physical_evidence: false,
                requires_test_evidence: false,
            }),
            action,
            reason: "AGENT_UNEXPECTED_EXIT".to_string(),
        }
    }

    fn seed_crashed_agent(
        conn: &rusqlite::Connection,
        agent_id: &str,
        provider: &str,
        with_snapshot: bool,
    ) {
        insert_agent_sync(conn, agent_id, "s1", provider, "CRASHED", None).unwrap();
        conn.execute(
            "UPDATE agents SET config_hash = ?, state_version = 7 WHERE id = ?",
            ("hash1", agent_id),
        )
        .unwrap();
        if with_snapshot {
            persist_agent_spawn_spec_sync(conn, &sample_spawn_spec(agent_id, provider), "hash1")
                .unwrap();
        }
    }

    fn recovery_events(conn: &rusqlite::Connection, agent_id: &str) -> Vec<Value> {
        let mut stmt = conn
            .prepare(
                "SELECT payload FROM events \
                 WHERE agent_id = ? AND event_type = 'self_recovery_attempt' \
                 ORDER BY seq_id ASC",
            )
            .unwrap();
        stmt.query_map([agent_id], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
            .collect()
    }

    fn recovery_event_count(conn: &rusqlite::Connection, agent_id: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM events \
             WHERE agent_id = ? AND event_type = 'self_recovery_attempt'",
            [agent_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn fake_recovery_respawn_ok<'a>(
        ctx: &'a Ctx,
        session_id: &'a str,
        agent: &'a RealignAgentParams,
        expected_hash: &'a str,
    ) -> RecoveryRespawnFuture<'a> {
        Box::pin(async move {
            let conn = ctx.db.conn();
            insert_agent_sync(
                &conn,
                &agent.agent_id,
                session_id,
                &agent.provider,
                "IDLE",
                None,
            )?;
            crate::db::agents::update_agent_config_hash_sync(
                &conn,
                &agent.agent_id,
                expected_hash,
            )?;
            crate::db::recovery::persist_agent_spawn_spec_sync(
                &conn,
                &AgentSpawnSpec {
                    agent_id: agent.agent_id.clone(),
                    provider: agent.provider.clone(),
                    env: agent.env.clone(),
                    hooks: agent.hooks.clone(),
                    plugins: agent.plugins.clone(),
                    skills: agent.skills.clone(),
                    bundle: agent.bundle.clone(),
                    settings: agent.settings.clone(),
                    bundle_digest: agent.bundle_digest.clone(),
                    sandbox_overrides: agent.sandbox_overrides.clone(),
                    hook_push_enabled: agent.hook_push_enabled,
                },
                expected_hash,
            )?;
            Ok(())
        })
    }

    fn fake_recovery_respawn_requires_ro_bind<'a>(
        ctx: &'a Ctx,
        session_id: &'a str,
        agent: &'a RealignAgentParams,
        expected_hash: &'a str,
    ) -> RecoveryRespawnFuture<'a> {
        Box::pin(async move {
            assert_eq!(
                agent.sandbox_overrides.extra_ro_binds[0].host_path,
                "/opt/keys"
            );
            assert_eq!(
                agent.sandbox_overrides.extra_ro_binds[0].sandbox_path,
                "/mnt/keys"
            );
            fake_recovery_respawn_ok(ctx, session_id, agent, expected_hash).await
        })
    }

    fn fake_recovery_respawn_requires_hook_push<'a>(
        ctx: &'a Ctx,
        session_id: &'a str,
        agent: &'a RealignAgentParams,
        expected_hash: &'a str,
    ) -> RecoveryRespawnFuture<'a> {
        Box::pin(async move {
            assert!(
                agent.hook_push_enabled,
                "recovery realign params must replay hook_push_enabled"
            );
            fake_recovery_respawn_ok(ctx, session_id, agent, expected_hash).await
        })
    }

    fn fake_recovery_respawn_err<'a>(
        _ctx: &'a Ctx,
        _session_id: &'a str,
        _agent: &'a RealignAgentParams,
        _expected_hash: &'a str,
    ) -> RecoveryRespawnFuture<'a> {
        Box::pin(async {
            Err(AhError::PtyIoError(
                "injected spawn failed after delete".to_string(),
            ))
        })
    }

    #[test]
    fn test_wake_up_is_callable() {
        wake_up();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_dispatch_pane_refresh_rejects_single_pane_with_wrong_pid() {
        which::which("tmux").expect("tmux binary required for stale pane refresh test");
        let ctx = test_ctx();
        let agent_id = "orchestrator_stale_wrong_pid";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "bash", "IDLE", Some(1)).unwrap();
        }
        let session_name = crate::tmux::agent_session_name(agent_id);
        ctx.tmux_server
            .ensure_session(session_name.clone(), ctx.state_dir.clone())
            .await
            .unwrap();
        let candidate = ctx
            .tmux_server
            .spawn_window(
                session_name.clone(),
                "worker".to_string(),
                ctx.state_dir.clone(),
                vec![
                    "bash".to_string(),
                    "-lc".to_string(),
                    "sleep 30".to_string(),
                ],
            )
            .await
            .unwrap();
        let candidate_pid = ctx
            .tmux_server
            .get_pane_pid(candidate.clone())
            .await
            .unwrap() as i64;
        {
            let conn = ctx.db.conn();
            conn.execute(
                "UPDATE agents SET pid = ? WHERE id = ?",
                (candidate_pid + 100_000, agent_id),
            )
            .unwrap();
        }
        let stale = TmuxPaneId("%999999".to_string());
        let resolved = resolve_current_dispatch_pane(&ctx, agent_id, stale.clone()).await;

        assert_eq!(resolved.0, stale.0);
        assert_ne!(
            crate::agent_io::pane_id(agent_id).map(|pane| pane.0),
            Some(candidate.0)
        );

        let _ = ctx.tmux_server.kill_session(session_name).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_once_fails_job_without_pane() {
        let ctx = test_ctx();
        let agent_id = "orchestrator_no_pane";
        let job_id = "job_orchestrator_no_pane";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "bash", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, "echo hi\n").unwrap();
        }

        assert!(
            run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();

        assert_eq!(job.status, "FAILED");
        assert_eq!(
            job.error_reason.as_deref(),
            Some("tmux pane not registered")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn orchestrator_dispatch_waits_for_transient_busy_then_sends() {
        let ctx = test_ctx();
        let agent_id = "orchestrator_transient_busy";
        let job_id = "job_transient_busy";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "bash", "BUSY", Some(123)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, "echo hi\n").unwrap();
        }

        let flip_db = ctx.db.clone();
        let flip_agent_id = agent_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(125)).await;
            flip_db
                .conn()
                .execute(
                    "UPDATE agents SET state = 'IDLE', state_version = state_version + 1 WHERE id = ?",
                    [flip_agent_id],
                )
                .unwrap();
        });

        assert!(
            run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );

        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();
        assert_eq!(
            job.status, "FAILED",
            "transient BUSY without in-flight work should become dispatchable; no pane then fails after claim"
        );
        assert_eq!(
            job.error_reason.as_deref(),
            Some("tmux pane not registered")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn orchestrator_dispatch_defers_busy_with_inflight_job() {
        let ctx = test_ctx();
        let agent_id = "orchestrator_busy_inflight";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "bash", "BUSY", Some(123)).unwrap();
            insert_job_sync(&conn, "job_inflight", agent_id, None, "first\n").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch(), dispatched_at_seq_id = 7 WHERE id = 'job_inflight'",
                [],
            )
            .unwrap();
            insert_job_sync(&conn, "job_second", agent_id, None, "second\n").unwrap();
        }

        assert!(
            run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );

        let job = query_job_sync(&ctx.db.conn(), "job_second")
            .unwrap()
            .unwrap();
        let deferred_count: i64 = ctx
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events \
                 WHERE agent_id = ? AND event_type = 'dispatch_deferred' \
                   AND json_extract(payload, '$.reason') = 'target_not_idle'",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(job.status, "QUEUED");
        assert_eq!(deferred_count, 1);
    }

    #[test]
    fn dispatch_guard_handled_or_error_refuses_before_job_claim() {
        let ctx = test_ctx();
        let agent_id = "orchestrator_guard_refuse";
        let job_id = "job_orchestrator_guard_refuse";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "codex", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, "echo hi\n").unwrap();
        }

        assert!(!dispatch_guard_scan_permits_dispatch(Ok(
            PromptScanDisposition::Handled { depth: 1 },
        )));
        assert!(!dispatch_guard_scan_permits_dispatch(Err(
            crate::error::AhError::TmuxCommandFailed {
                cmd: "fake capture".to_string(),
                stderr: "capture failed".to_string(),
                exit: 1,
            },
        )));

        let conn = ctx.db.conn();
        let job = query_job_sync(&conn, job_id).unwrap().unwrap();
        let state: String = conn
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(job.status, "QUEUED");
        assert_eq!(state, "IDLE");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn monitor_registers_baseline_before_send() {
        let mut ctx = test_ctx();
        ctx.env_state.unsafe_no_sandbox = false;
        let agent_id = "orchestrator_monitor_baseline";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        let sandbox = ctx.state_dir.join("sandboxes").join("s1").join(agent_id);
        std::fs::create_dir_all(&sandbox).unwrap();
        let home = crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox).unwrap();
        let sessions = home.join(".codex/sessions/2026/06");
        std::fs::create_dir_all(&sessions).unwrap();
        let log = sessions.join("rollout-session.jsonl");
        std::fs::write(&log, b"{\"old\":true}\n").unwrap();
        let conn = ctx.db.conn();
        insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(&conn, agent_id, "s1", "codex", "WAITING_FOR_ACK", Some(123)).unwrap();

        let registered = prepare_log_monitor_before_send(&ctx, "s1", agent_id, "codex");

        assert!(registered);
        let cursor = crate::completion::registry::cursor_snapshot(agent_id).unwrap();
        assert_eq!(cursor.get(&log).copied(), Some(13));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_guard_capture_error_keeps_job_queued_before_log_monitor() {
        let agent_id = "orchestrator_guard_capture_err";
        let job_id = "job_orchestrator_guard_capture_err";
        let mut ctx = test_ctx();
        ctx.env_state.unsafe_no_sandbox = false;
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        let sandbox = ctx.state_dir.join("sandboxes").join("s1").join(agent_id);
        std::fs::create_dir_all(&sandbox).unwrap();
        let home = crate::provider::home_layout::sandbox_home_for_sandbox_dir(&sandbox).unwrap();
        let sessions = home.join(".codex/sessions/2026/06");
        std::fs::create_dir_all(&sessions).unwrap();
        std::fs::write(sessions.join("rollout-session.jsonl"), b"{\"old\":true}\n").unwrap();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "codex", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, "echo hi\n").unwrap();
        }
        crate::marker::parser_registry::register(
            agent_id.to_string(),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
        );
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        crate::agent_io::register(
            agent_id.to_string(),
            crate::agent_io::AgentIoEntry {
                session_id: "s1".to_string(),
                pane_id: TmuxPaneId("%999999".to_string()),
                expected_pid: None,
                reader_handle,
                fifo_path: ctx.state_dir.join("pipes").join(format!("{agent_id}.fifo")),
                socket_name: ctx.tmux_server.socket_name().to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );

        assert!(run_once(&ctx).await.unwrap());

        assert!(!crate::completion::registry::contains(agent_id));
        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "QUEUED");
        let state: String = ctx
            .db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "IDLE");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_rechecks_prompt_after_claim_before_send_and_requeues() {
        which::which("tmux").expect("tmux binary required for dispatch readiness recheck test");
        let agent_id = "orchestrator_dispatch_recheck";
        let job_id = "job_recheck";
        let prompt_text = "DO_NOT_SEND_TO_PROMPT\n";
        let ctx = test_ctx();
        let kb_path = ctx.state_dir.join("prompt-cases.json");
        let custom_kb_json = r#"{
            "version": "1",
            "cases": [
                {
                    "id": "trust_path_01",
                    "provider": null,
                    "fingerprint": {
                        "type": "regex",
                        "pattern": "(?is)New provider EULA"
                    },
                    "action": [],
                    "category": "manual-resolve",
                    "description": "Test EULA",
                    "confidence_threshold": 0.9,
                    "used_count": 0,
                    "created_at": null,
                    "last_used_at": null,
                    "created_by": "test",
                    "regex_flags": ["Dotall", "CaseInsensitive"],
                    "trigger_state": null
                }
            ]
        }"#;
        std::fs::write(&kb_path, custom_kb_json).unwrap();

        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "codex", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, prompt_text).unwrap();
        }
        crate::marker::parser_registry::register(
            agent_id.to_string(),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
        );

        let session_name = crate::tmux::agent_session_name(agent_id);
        ctx.tmux_server
            .ensure_session(session_name.clone(), ctx.state_dir.clone())
            .await
            .unwrap();
        let pane = ctx
            .tmux_server
            .spawn_window(
                session_name.clone(),
                "worker".to_string(),
                ctx.state_dir.clone(),
                vec![
                    "bash".to_string(),
                    "-lc".to_string(),
                    "printf 'mock_prompt_provider: ready\\n\\033[60;1H  › '; sleep 30".to_string(),
                ],
            )
            .await
            .unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        crate::agent_io::register(
            agent_id.to_string(),
            crate::agent_io::AgentIoEntry {
                session_id: "s1".to_string(),
                pane_id: pane.clone(),
                expected_pid: None,
                reader_handle,
                fifo_path: ctx.state_dir.join("pipes").join(format!("{agent_id}.fifo")),
                socket_name: ctx.tmux_server.socket_name().to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );
        wait_for_capture_contains(&ctx, &pane, "mock_prompt_provider: ready").await;

        let _hook_guard = BEFORE_DISPATCH_SEND_HOOK_TEST_LOCK.lock().await;
        let hook_pane = pane.clone();
        set_before_dispatch_send_hook_for_test(Some(Arc::new(move |ctx, _agent_id, _pane_id| {
            let hook_pane = hook_pane.clone();
            Box::pin(async move {
                ctx.tmux_server
                    .send_keys_literal(
                        hook_pane.clone(),
                        "printf '\\033[2J\\033[HNew provider EULA requires review before continuing.\\n\\n1) Accept terms and continue\\n2) Decline and exit\\n'; read answer"
                            .to_string(),
                    )
                    .await
                    .unwrap();
                ctx.tmux_server.send_enter(hook_pane.clone()).await.unwrap();
                wait_for_capture_contains(&ctx, &hook_pane, "New provider EULA").await;
            })
        })));

        let result = run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok).await;
        set_before_dispatch_send_hook_for_test(None);
        result.unwrap();

        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "QUEUED");
        assert!(job.dispatched_at.is_none());
        let capture = ctx.tmux_server.capture_pane(pane.clone()).await.unwrap();
        assert!(
            !capture.contains("DO_NOT_SEND_TO_PROMPT"),
            "dispatch text must not be sent into a prompt after requeue; capture:\n{capture}"
        );

        assert!(
            run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "QUEUED");
        let state: String = ctx
            .db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "PROMPT_PENDING");
        let capture = ctx.tmux_server.capture_pane(pane.clone()).await.unwrap();
        assert!(
            !capture.contains("DO_NOT_SEND_TO_PROMPT"),
            "dispatch text must not be sent into a prompt-pending agent; capture:\n{capture}"
        );

        let _ = ctx.tmux_server.kill_session(session_name).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_ack_race_no_phantom_dispatched_job() {
        which::which("tmux").expect("tmux binary required for dispatch ACK race test");
        let agent_id = "orchestrator_dispatch_ack_race";
        let job_id = "job_dispatch_ack_race";
        let prompt_text = "DO_NOT_SEND_ACK_RACE\n";
        let ctx = test_ctx();
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "bash", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, prompt_text).unwrap();
        }
        crate::marker::parser_registry::register(
            agent_id.to_string(),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
        );

        let session_name = crate::tmux::agent_session_name(agent_id);
        ctx.tmux_server
            .ensure_session(session_name.clone(), ctx.state_dir.clone())
            .await
            .unwrap();
        let pane = ctx
            .tmux_server
            .spawn_window(
                session_name.clone(),
                "worker".to_string(),
                ctx.state_dir.clone(),
                vec![
                    "bash".to_string(),
                    "-lc".to_string(),
                    "printf 'ready\\n$ '; sleep 30".to_string(),
                ],
            )
            .await
            .unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        crate::agent_io::register(
            agent_id.to_string(),
            crate::agent_io::AgentIoEntry {
                session_id: "s1".to_string(),
                pane_id: pane.clone(),
                expected_pid: None,
                reader_handle,
                fifo_path: ctx.state_dir.join("pipes").join(format!("{agent_id}.fifo")),
                socket_name: ctx.tmux_server.socket_name().to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(true)),
            },
        );
        wait_for_capture_contains(&ctx, &pane, "ready").await;

        let _hook_guard = BEFORE_DISPATCH_SEND_HOOK_TEST_LOCK.lock().await;
        set_before_dispatch_send_hook_for_test(Some(Arc::new(move |ctx, agent_id, _pane_id| {
            Box::pin(async move {
                ctx.db
                    .conn()
                    .execute(
                        "UPDATE agents SET state = 'BUSY', state_version = state_version + 1 WHERE id = ?",
                        [agent_id],
                    )
                    .unwrap();
            })
        })));

        let result = run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok).await;
        set_before_dispatch_send_hook_for_test(None);
        result.unwrap();

        let job = query_job_sync(&ctx.db.conn(), job_id).unwrap().unwrap();
        let capture = ctx.tmux_server.capture_pane(pane.clone()).await.unwrap();
        assert!(
            job.status == "QUEUED" || (job.status == "FAILED" && job.error_reason.is_some()),
            "race must resolve the claimed job instead of leaving phantom DISPATCHED: {:?}",
            job
        );
        assert!(
            !capture.contains(prompt_text.trim()),
            "prompt must not be sent after pre-send ACK race perturbation; capture:\n{capture}"
        );

        let _ = ctx.tmux_server.kill_session(session_name).await;
    }

    async fn wait_for_capture_contains(ctx: &Ctx, pane: &TmuxPaneId, needle: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last_capture = String::new();
        while Instant::now() < deadline {
            last_capture = ctx.tmux_server.capture_pane(pane.clone()).await.unwrap();
            if last_capture.contains(needle) {
                return last_capture;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "pane {} did not contain {needle:?}; last capture:\n{last_capture}",
            pane.0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rg_unparked_prompt_pending_job_is_dispatched_by_next_run_once() {
        let ctx = test_ctx();
        let agent_id = "rg_prompt_pending_no_pane";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "codex", "PROMPT_PENDING", Some(123)).unwrap();
            insert_job_sync(&conn, "rg_job", agent_id, None, "echo hi\n").unwrap();
            conn.execute(
                "UPDATE agents SET state_version = 5 WHERE id = ?",
                [agent_id],
            )
            .unwrap();
        }

        assert_eq!(
            apply_prompt_pending_unpark_outcome_sync(
                &ctx.db,
                agent_id,
                5,
                crate::prompt_handler::runner::PromptRunOutcome::NoActionNeeded { depth: 0 },
            )
            .unwrap(),
            crate::prompt_handler::integration::PromptPendingUnparkDisposition::Unparked
        );
        assert!(
            run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );

        let job = query_job_sync(&ctx.db.conn(), "rg_job").unwrap().unwrap();
        assert_eq!(job.status, "FAILED");
        assert_eq!(
            job.error_reason.as_deref(),
            Some("tmux pane not registered")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_or_unreadable_log_root_switches_to_ui_immediately() {
        let ctx = test_ctx();
        let agent_id = "orchestrator_missing_log_root";
        let _cleanup = AgentGlobalCleanup::new(agent_id);
        let conn = ctx.db.conn();
        insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
        insert_agent_sync(&conn, agent_id, "s1", "codex", "WAITING_FOR_ACK", Some(123)).unwrap();

        let registered = prepare_log_monitor_before_send(&ctx, "s1", agent_id, "codex");

        assert!(!registered);
        assert!(!crate::completion::registry::contains(agent_id));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_run_once_dispatches_idle_job_and_then_attempts_recovery() {
        let ctx = test_ctx();
        let _cleanup = AgentGlobalCleanup::new("ra2_idle_no_pane");
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            insert_agent_sync(&conn, "ra2_idle_no_pane", "s1", "bash", "IDLE", Some(123)).unwrap();
            insert_job_sync(&conn, "ra2_job", "ra2_idle_no_pane", None, "echo hi\n").unwrap();
            seed_crashed_agent(&conn, "ra2_crashed", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("ra2_crashed", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
        }

        assert!(
            run_once_with_recovery_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let job = query_job_sync(&conn, "ra2_job").unwrap().unwrap();
        assert_eq!(job.status, "FAILED");
        let events = recovery_events(&conn, "ra2_crashed");
        assert!(
            events.iter().any(|payload| {
                payload.get("action").and_then(Value::as_str) == Some("recovered")
            }),
            "run_once should append recovery after dispatch guard loop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_recovery_loop_recovers_one_due_crashed_agent_from_snapshot() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_due", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("ra2_due", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let row: (String, i64, Option<i64>, i64) = conn
            .query_row(
                "SELECT state, retry_count, next_retry_at, retry_exhausted FROM agents WHERE id = 'ra2_due'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        // P0-2 新契约:Ok 分支不再清零 retry_count,而是 increment(清零延到
        // confirm_agent_stable_sync 的 ≥300s 稳定确认)。orchestrator:848 的 Ok 分支
        // 调 record_recovery_failure_backoff_sync,把 seed 的 retry_count 0→1 并落
        // next_retry_at=Some(now+delay)。次值动态,分字段断言。
        let (state, retry_count, next_retry_at, retry_exhausted) = row;
        assert_eq!(state, "IDLE");
        assert_eq!(
            retry_count, 1,
            "P0-2: respawn 成功后 retry_count increment 到 1(清零延到 ≥300s 稳定确认)"
        );
        assert!(
            next_retry_at.is_some(),
            "P0-2: backoff 在成功 respawn 后设 next_retry_at(动态 now+delay)"
        );
        assert_eq!(retry_exhausted, 0);
        assert!(
            recovery_events(&conn, "ra2_due").iter().any(|payload| {
                payload.get("action").and_then(Value::as_str) == Some("recovered")
            })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_worker_gate_revive_intent_allows_respawn() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "rr_revive", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("rr_revive", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let state: String = conn
            .query_row(
                "SELECT state FROM agents WHERE id = 'rr_revive'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "IDLE");
        assert!(
            recovery_events(&conn, "rr_revive").iter().any(|payload| {
                payload.get("action").and_then(Value::as_str) == Some("recovered")
            })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_worker_gate_revive_idle_respawns_without_requeue() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "rr_revive_idle", "codex", true);
            insert_job_sync(
                &conn,
                "rr_revive_idle_failed_job",
                "rr_revive_idle",
                None,
                "old failed work\n",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'FAILED', error_reason = 'AGENT_UNEXPECTED_EXIT' \
                 WHERE id = 'rr_revive_idle_failed_job'",
                [],
            )
            .unwrap();
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("rr_revive_idle", RecoveryIntentAction::ReviveIdle, None),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let state: String = conn
            .query_row(
                "SELECT state FROM agents WHERE id = 'rr_revive_idle'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let queued_jobs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE agent_id = 'rr_revive_idle' AND status = 'QUEUED'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let payload = recovery_events(&conn, "rr_revive_idle")
            .into_iter()
            .find(|payload| payload.get("action").and_then(Value::as_str) == Some("recovered"))
            .unwrap();

        assert_eq!(state, "IDLE");
        assert_eq!(queued_jobs, 0);
        assert_eq!(payload["recovery_action"], "REVIVE_IDLE");
        assert_eq!(payload["recovery_kind"], "idle_topology_restore");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_worker_gate_revive_idle_respawn_failure_bumps_backoff() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "rr_revive_idle_fail", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent(
                    "rr_revive_idle_fail",
                    RecoveryIntentAction::ReviveIdle,
                    None,
                ),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_err)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let row: (String, i64, Option<i64>) = conn
            .query_row(
                "SELECT state, retry_count, next_retry_at FROM agents \
                 WHERE id = 'rr_revive_idle_fail'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let payload = recovery_events(&conn, "rr_revive_idle_fail")
            .into_iter()
            .find(|payload| payload.get("action").and_then(Value::as_str) == Some("failed"))
            .unwrap();

        assert_eq!(row.0, "CRASHED");
        assert_eq!(row.1, 1);
        assert!(row.2.is_some());
        assert_eq!(payload["recovery_action"], "REVIVE_IDLE");
        assert_eq!(payload["recovery_kind"], "idle_topology_restore");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_worker_gate_revive_idle_non_active_session_skips_without_respawn() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            conn.execute("UPDATE sessions SET status = 'KILLED' WHERE id = 's1'", [])
                .unwrap();
            seed_crashed_agent(&conn, "rr_revive_idle_inactive", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent(
                    "rr_revive_idle_inactive",
                    RecoveryIntentAction::ReviveIdle,
                    None,
                ),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE id = 'rr_revive_idle_inactive')",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(!exists);
        assert_eq!(recovery_event_count(&conn, "rr_revive_idle_inactive"), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_worker_gate_reap_only_intent_deletes_without_respawn() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "rr_reap_only", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("rr_reap_only", RecoveryIntentAction::ReapOnly, None),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE id = 'rr_reap_only')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists);
        assert_eq!(recovery_event_count(&conn, "rr_reap_only"), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rr_worker_gate_missing_intent_skips_crashed_agent_without_respawn() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "rr_missing_intent", "codex", true);
        }

        assert!(
            !run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let state: String = conn
            .query_row(
                "SELECT state FROM agents WHERE id = 'rr_missing_intent'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "CRASHED");
        assert!(
            recovery_events(&conn, "rr_missing_intent")
                .iter()
                .any(|payload| {
                    payload.get("action").and_then(Value::as_str) == Some("skipped")
                        && payload.get("reason").and_then(Value::as_str)
                            == Some("missing_recovery_intent")
                })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_recovery_restores_sandbox_overrides_from_snapshot() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_bind_snapshot", "codex", true);
            let mut spec = sample_spawn_spec("ra2_bind_snapshot", "codex");
            spec.sandbox_overrides = crate::sandbox::SandboxOverrides {
                extra_binds: vec![],
                extra_ro_binds: vec![crate::sandbox::ReadOnlyBind {
                    host_path: "/opt/keys".to_string(),
                    sandbox_path: "/mnt/keys".to_string(),
                }],
            };
            persist_agent_spawn_spec_sync(&conn, &spec, "hash1").unwrap();
        }

        recover_crashed_agent_from_snapshot_with_respawn(
            &ctx,
            "ra2_bind_snapshot",
            fake_recovery_respawn_requires_ro_bind,
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_recovered_event_includes_crash_forensics_from_deleted_row() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_crash_forensics", "codex", true);
            conn.execute(
                "UPDATE agents \
                 SET exit_code = 137, error_code = 'AGENT_UNEXPECTED_EXIT' \
                 WHERE id = 'ra2_crash_forensics'",
                [],
            )
            .unwrap();
        }

        recover_crashed_agent_from_snapshot_with_respawn(
            &ctx,
            "ra2_crash_forensics",
            fake_recovery_respawn_ok,
        )
        .await
        .unwrap();

        let payload = recovery_events(&ctx.db.conn(), "ra2_crash_forensics")
            .into_iter()
            .find(|payload| payload.get("action").and_then(Value::as_str) == Some("recovered"))
            .unwrap();
        assert_eq!(payload["recovered_from_exit_code"], 137);
        assert_eq!(
            payload["recovered_from_error_code"],
            "AGENT_UNEXPECTED_EXIT"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn f1_recovery_replays_hook_push_enabled_from_snapshot() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_hook_push_snapshot", "codex", true);
            let mut spec = sample_spawn_spec("ra2_hook_push_snapshot", "codex");
            spec.hook_push_enabled = true;
            persist_agent_spawn_spec_sync(&conn, &spec, "hash1").unwrap();
        }

        recover_crashed_agent_from_snapshot_with_respawn(
            &ctx,
            "ra2_hook_push_snapshot",
            fake_recovery_respawn_requires_hook_push,
        )
        .await
        .unwrap();

        let stored = crate::db::recovery::query_agent_spawn_spec_sync(
            &ctx.db.conn(),
            "ra2_hook_push_snapshot",
        )
        .unwrap()
        .unwrap();
        assert!(
            stored.spec.hook_push_enabled,
            "successful recovery must persist the replayed hook_push_enabled flag"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_recovery_loop_processes_at_most_one_candidate_per_tick() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_one", "codex", true);
            seed_crashed_agent(&conn, "ra2_two", "claude", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("ra2_one", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("ra2_two", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let recovered_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE event_type = 'self_recovery_attempt' AND payload LIKE '%\"action\":\"recovered\"%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(recovered_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_missing_snapshot_emits_skipped_without_cas_and_stays_crashed() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_missing_snapshot", "codex", false);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("ra2_missing_snapshot", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
        }

        assert!(!run_recovery_once(&ctx).await.unwrap());
        assert!(!run_recovery_once(&ctx).await.unwrap());
        let conn = ctx.db.conn();
        let row: (String, i64) = conn
            .query_row(
                "SELECT state, state_version FROM agents WHERE id = 'ra2_missing_snapshot'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("CRASHED".to_string(), 7));
        let events = recovery_events(&conn, "ra2_missing_snapshot");
        assert_eq!(events.len(), 1);
        let payload = &events[0];
        assert_eq!(
            payload.get("action").and_then(Value::as_str),
            Some("skipped")
        );
        assert_eq!(
            payload.get("reason").and_then(Value::as_str),
            Some("missing_spawn_spec")
        );
        assert_eq!(recovery_event_count(&conn, "ra2_missing_snapshot"), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_missing_snapshot_does_not_block_later_recoverable_agent() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_a_missing_snapshot", "codex", false);
            seed_crashed_agent(&conn, "ra2_z_due", "codex", true);
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent(
                    "ra2_a_missing_snapshot",
                    RecoveryIntentAction::Revive,
                    None,
                ),
            )
            .unwrap();
            persist_agent_recovery_intent_sync(
                &conn,
                &sample_recovery_intent("ra2_z_due", RecoveryIntentAction::Revive, None),
            )
            .unwrap();
        }

        assert!(
            run_recovery_once_with_respawn(&ctx, fake_recovery_respawn_ok)
                .await
                .unwrap()
        );
        let conn = ctx.db.conn();
        let missing_state: (String, i64) = conn
            .query_row(
                "SELECT state, state_version FROM agents WHERE id = 'ra2_a_missing_snapshot'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let due_state: String = conn
            .query_row(
                "SELECT state FROM agents WHERE id = 'ra2_z_due'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(missing_state, ("CRASHED".to_string(), 7));
        assert_eq!(recovery_event_count(&conn, "ra2_a_missing_snapshot"), 1);
        assert_eq!(due_state, "IDLE");
        assert!(
            recovery_events(&conn, "ra2_z_due").iter().any(|payload| {
                payload.get("action").and_then(Value::as_str) == Some("recovered")
            })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_respawn_failure_keeps_crashed_bumps_backoff_and_emits_failed() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_respawn_fail", "codex", true);
        }

        apply_recovery_spawn_result(
            &ctx,
            "ra2_respawn_fail",
            Err(AhError::PtyIoError("injected spawn failed".to_string())),
            1_000,
            None,
        )
        .await
        .unwrap();
        let conn = ctx.db.conn();
        let row: (String, i64, i64) = conn
            .query_row(
                "SELECT state, retry_count, retry_exhausted FROM agents WHERE id = 'ra2_respawn_fail'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, ("CRASHED".to_string(), 1, 0));
        assert!(
            recovery_events(&conn, "ra2_respawn_fail")
                .iter()
                .any(|payload| { payload.get("action").and_then(Value::as_str) == Some("failed") })
        );
    }

    // P0-2 修复一(毒任务熔断清零洞):毒任务的 respawn 恒成功(job 致崩发生在 spawn 成功
    // 之后),走 apply_recovery_spawn_result 的 Ok 分支。现码 Ok 分支(orchestrator:798)
    // `clear_recovery_backoff` 归零、从不 increment(increment 只在 :828 的 Err 分支)→
    // 连崩永不累积、熔断(retry_count>=5)被击穿。改后契约:Ok 分支停止 clear、改为
    // increment retry_count(cap 5 → retry_exhausted),清零只由 confirm_agent_stable_sync
    // 在稳定 N 分钟后做。此测试反复驱动 Ok 路径 5 次(表达 崩→respawn 成功 循环),断言
    // 熔断累积触发。现码 Ok=clear → retry_count 停 0 → RED;a1 把 Ok 改成 increment → GREEN。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_poison_task_crashloop_trips_breaker() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "p02_poison", "codex", true);
        }

        for _ in 0..5 {
            apply_recovery_spawn_result(&ctx, "p02_poison", Ok(()), 1_000, None)
                .await
                .unwrap();
        }

        let conn = ctx.db.conn();
        let (retry_count, retry_exhausted): (i64, i64) = conn
            .query_row(
                "SELECT retry_count, retry_exhausted FROM agents WHERE id = 'p02_poison'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(
            retry_count >= 5,
            "poison crash-respawn cycles must accumulate retry_count to >=5, got {retry_count}"
        );
        assert_eq!(
            retry_exhausted, 1,
            "breaker must trip (retry_exhausted=1) after poison crashloop, got {retry_exhausted}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_retry_exhausted_and_future_backoff_are_not_selected() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_exhausted", "codex", true);
            seed_crashed_agent(&conn, "ra2_future", "claude", true);
            conn.execute(
                "UPDATE agents SET retry_exhausted = 1 WHERE id = 'ra2_exhausted'",
                [],
            )
            .unwrap();
            conn.execute(
                "UPDATE agents SET next_retry_at = unixepoch() + 3600 WHERE id = 'ra2_future'",
                [],
            )
            .unwrap();
        }

        assert!(!run_recovery_once(&ctx).await.unwrap());
        let conn = ctx.db.conn();
        assert!(recovery_events(&conn, "ra2_exhausted").is_empty());
        assert!(recovery_events(&conn, "ra2_future").is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_delete_then_spawn_failure_restores_minimal_crashed_row_and_snapshot() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_restore_window", "codex", true);
        }

        recover_crashed_agent_from_snapshot_with_respawn(
            &ctx,
            "ra2_restore_window",
            fake_recovery_respawn_err,
        )
        .await
        .unwrap();
        let conn = ctx.db.conn();
        let state: String = conn
            .query_row(
                "SELECT state FROM agents WHERE id = 'ra2_restore_window'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let spec_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_spawn_specs WHERE agent_id = 'ra2_restore_window'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "CRASHED");
        assert_eq!(spec_count, 1);
        let retry_count: i64 = conn
            .query_row(
                "SELECT retry_count FROM agents WHERE id = 'ra2_restore_window'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retry_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_self_recovery_attempt_event_payload_contract() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            seed_session(&conn);
            seed_crashed_agent(&conn, "ra2_event", "codex", true);
        }

        apply_recovery_spawn_result(
            &ctx,
            "ra2_event",
            Err(AhError::PtyIoError("spawn failed".to_string())),
            1_000,
            None,
        )
        .await
        .unwrap();
        let payload = recovery_events(&ctx.db.conn(), "ra2_event")
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(payload["agent_id"], "ra2_event");
        assert_eq!(payload["reason"], "OOM_RECOVERY");
        assert_eq!(payload["action"], "failed");
        assert_eq!(payload["retry_count"], 1);
        assert_eq!(payload["next_retry_at"], 1001);
        assert_eq!(payload["state_version"], 8);
        assert_eq!(payload["error"], "spawn failed");
    }
}
