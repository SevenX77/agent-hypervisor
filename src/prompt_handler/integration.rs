//! Integration helpers that connect prompt-handler scans to ah state and events.

use crate::db::Db;
use crate::db::common::{map_db_error, spawn_db};
use crate::error::AhError;
use crate::marker::MarkerMatcher;
use crate::prompt_handler::events::{
    UNKNOWN_PROMPT_DETECTED, UnknownPromptPayload, emit_unknown_prompt_detected, hex_hash,
};
use crate::prompt_handler::kb::load_or_bootstrap_kb;
use crate::prompt_handler::llm_client::RealHaikuClassifier;
use crate::prompt_handler::matcher::{PromptScanPurpose, sanitize_pane_text};
use crate::prompt_handler::runner::{
    PromptRunOutcome, RunnerContext, TmuxPromptIo, handle_prompt_chain,
};
use crate::rpc::Ctx;
use crate::tmux::{TmuxPaneId, TmuxServer};
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

static TRANSIENT_UNKNOWN_PROMPTS: LazyLock<Mutex<HashMap<String, TransientUnknownPrompt>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
struct TransientUnknownPrompt {
    state: String,
    capture_hash: String,
    seen_count: usize,
}

#[derive(Clone)]
pub struct PromptScanRequest {
    pub db: Db,
    pub agent_id: String,
    pub provider: String,
    pub pane_id: TmuxPaneId,
    pub tmux: Arc<TmuxServer>,
    pub state_dir: PathBuf,
    pub marker_matcher: Arc<MarkerMatcher>,
    pub max_depth: usize,
    pub scan_purpose: PromptScanPurpose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptScanDisposition {
    NoActionNeeded { depth: usize },
    Handled { depth: usize },
    Deferred { depth: usize, block_reason: String },
    Pending { depth: usize, block_reason: String },
}

/// Maximum consecutive ticks a hash suppression can be active before escalating.
/// 5 ticks is chosen as a default to avoid blocking prompt unparking for too long
/// while allowing temporary stability checks to complete.
pub const SUPPRESSION_ESCALATION_TICKS: usize = 5;

#[derive(Debug, Default)]
pub struct PromptPendingUnparkState {
    pub suppressed_hashes: HashMap<String, String>,
    pub suppressed_ticks: HashMap<String, usize>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PromptPendingUnparkTickResult {
    pub scanned: usize,
    pub unparked: usize,
    pub handled: usize,
    pub suppressed: usize,
}

pub fn is_park_whitelisted(block_reason: &str) -> bool {
    block_reason == "trust_path_01"
        || block_reason == "codex_update_01"
        || block_reason.starts_with("master_resolve_")
        || block_reason.starts_with("user_")
}

pub fn is_prompt_handling_provider(provider: &str) -> bool {
    matches!(provider, "codex" | "claude")
}

pub async fn scan_prompt_and_apply_outcome(
    request: PromptScanRequest,
) -> Result<PromptScanDisposition, AhError> {
    tracing::info!(
        agent_id = %request.agent_id,
        provider = %request.provider,
        max_depth = request.max_depth,
        "prompt integration scan start"
    );

    let db = request.db.clone();
    let agent_id = request.agent_id.clone();
    let provider = request.provider.clone();
    let outcome = tokio::task::spawn_blocking(move || run_prompt_scan(request))
        .await
        .map_err(|err| AhError::DatabaseRuntimePanic {
            details: format!("prompt scan worker join failed: {err}"),
        })??;

    match outcome {
        PromptRunOutcome::NoActionNeeded { depth } => {
            if depth > 0 {
                tracing::info!(
                    agent_id,
                    depth,
                    "prompt integration auto-handled known prompt chain"
                );
                Ok(PromptScanDisposition::Handled { depth })
            } else {
                tracing::info!(agent_id, "prompt integration no prompt action needed");
                Ok(PromptScanDisposition::NoActionNeeded { depth })
            }
        }
        PromptRunOutcome::RetryLater { depth } => {
            tracing::info!(
                agent_id,
                depth,
                "prompt integration deferred scan until pane capture is non-empty"
            );
            Ok(PromptScanDisposition::Deferred {
                depth,
                block_reason: "retry_later".to_string(),
            })
        }
        PromptRunOutcome::Pending {
            snapshot,
            depth,
            block_reason,
        } => {
            let current_state = query_agent_state(db.clone(), agent_id.clone()).await?;
            if is_active_dispatch_prompt_demote_deferred_state(&current_state) {
                tracing::info!(
                    agent_id,
                    depth,
                    state = %current_state,
                    "prompt integration deferred unknown prompt during active dispatch"
                );
                return Ok(PromptScanDisposition::Deferred {
                    depth,
                    block_reason: "active_dispatch_prompt_scan".to_string(),
                });
            }
            if is_prompt_demote_deferred_state(&current_state) {
                let capture_hash = hex_hash(&snapshot.sanitized_hash);
                if transient_unknown_prompt_should_defer(
                    &agent_id,
                    &current_state,
                    &capture_hash,
                    transient_unknown_stable_scan_threshold(),
                ) {
                    tracing::info!(
                        agent_id,
                        depth,
                        state = %current_state,
                        capture_hash = %capture_hash,
                        "prompt integration deferred unknown prompt while transient screen stabilizes"
                    );
                    return Ok(PromptScanDisposition::Deferred {
                        depth,
                        block_reason: "unknown_prompt_stabilizing".to_string(),
                    });
                }
            }
            let payload = UnknownPromptPayload::new(&snapshot, &block_reason, depth, &provider);
            if is_park_whitelisted(&block_reason) {
                mark_prompt_pending_and_emit_unknown(db, agent_id, payload).await?;
                Ok(PromptScanDisposition::Pending {
                    depth,
                    block_reason,
                })
            } else {
                emit_unknown_prompt_detected(db, agent_id, payload).await?;
                Ok(PromptScanDisposition::NoActionNeeded { depth })
            }
        }
        PromptRunOutcome::DepthExceeded { snapshot, depth } => {
            let current_state = query_agent_state(db.clone(), agent_id.clone()).await?;
            if is_active_dispatch_prompt_demote_deferred_state(&current_state) {
                tracing::info!(
                    agent_id,
                    depth,
                    state = %current_state,
                    "prompt integration deferred depth-exceeded prompt during active dispatch"
                );
                return Ok(PromptScanDisposition::Deferred {
                    depth,
                    block_reason: "active_dispatch_prompt_scan".to_string(),
                });
            }
            if is_prompt_demote_deferred_state(&current_state) {
                let capture_hash = hex_hash(&snapshot.sanitized_hash);
                if transient_unknown_prompt_should_defer(
                    &agent_id,
                    &current_state,
                    &capture_hash,
                    transient_unknown_stable_scan_threshold(),
                ) {
                    tracing::info!(
                        agent_id,
                        depth,
                        state = %current_state,
                        capture_hash = %capture_hash,
                        "prompt integration deferred depth-exceeded prompt while transient screen stabilizes"
                    );
                    return Ok(PromptScanDisposition::Deferred {
                        depth,
                        block_reason: "unknown_prompt_stabilizing".to_string(),
                    });
                }
            }
            let payload = UnknownPromptPayload::new(&snapshot, "depth_exceeded", depth, &provider);
            emit_unknown_prompt_detected(db, agent_id, payload).await?;
            Ok(PromptScanDisposition::NoActionNeeded { depth })
        }
        PromptRunOutcome::ExecutorFailed { error, depth } => {
            tracing::error!(
                agent_id,
                depth,
                reason = %error,
                impact = "caller should keep existing ACK/STUCK fallback behavior",
                "prompt integration scan failed"
            );
            Err(error)
        }
    }
}

fn is_active_dispatch_prompt_demote_deferred_state(state: &str) -> bool {
    matches!(
        state,
        crate::db::state_machine::STATE_WAITING_FOR_ACK | crate::db::state_machine::STATE_BUSY
    )
}

fn is_prompt_demote_deferred_state(state: &str) -> bool {
    matches!(
        state,
        crate::db::state_machine::STATE_SPAWNING
            | crate::db::state_machine::STATE_WAITING_FOR_ACK
            | crate::db::state_machine::STATE_BUSY
    )
}

fn transient_unknown_stable_scan_threshold() -> usize {
    std::env::var("AH_PROMPT_TRANSIENT_UNKNOWN_STABLE_SCANS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3)
}

#[cfg(test)]
fn transient_unknown_prompt_disposition(
    agent_id: &str,
    state: &str,
    capture_hash: &str,
    stable_scan_threshold: usize,
) -> PromptScanDisposition {
    if transient_unknown_prompt_should_defer(agent_id, state, capture_hash, stable_scan_threshold) {
        PromptScanDisposition::Deferred {
            depth: 0,
            block_reason: "unknown_prompt_stabilizing".to_string(),
        }
    } else {
        PromptScanDisposition::Pending {
            depth: 0,
            block_reason: "unknown_prompt".to_string(),
        }
    }
}

fn transient_unknown_prompt_should_defer(
    agent_id: &str,
    state: &str,
    capture_hash: &str,
    stable_scan_threshold: usize,
) -> bool {
    if !is_prompt_demote_deferred_state(state) {
        if let Ok(mut prompts) = TRANSIENT_UNKNOWN_PROMPTS.lock() {
            prompts.remove(agent_id);
        }
        return false;
    }
    let threshold = stable_scan_threshold.max(1);
    let Ok(mut prompts) = TRANSIENT_UNKNOWN_PROMPTS.lock() else {
        return threshold > 1;
    };
    let entry = prompts
        .entry(agent_id.to_string())
        .and_modify(|entry| {
            if entry.state == state && entry.capture_hash == capture_hash {
                entry.seen_count += 1;
            } else {
                entry.state = state.to_string();
                entry.capture_hash = capture_hash.to_string();
                entry.seen_count = 1;
            }
        })
        .or_insert_with(|| TransientUnknownPrompt {
            state: state.to_string(),
            capture_hash: capture_hash.to_string(),
            seen_count: 1,
        });
    if entry.seen_count >= threshold {
        prompts.remove(agent_id);
        false
    } else {
        true
    }
}

async fn query_agent_state(db: Db, agent_id: String) -> Result<String, AhError> {
    spawn_db("prompt_handler::query_agent_state", move || {
        db.conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![&agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| map_db_error("query agent state for prompt disposition", err))?
            .ok_or(AhError::AgentNotFound(agent_id))
    })
    .await
}

fn run_prompt_scan(request: PromptScanRequest) -> Result<PromptRunOutcome, AhError> {
    let kb_path = request.state_dir.join("prompt-cases.json");
    let kb = load_or_bootstrap_kb(&kb_path).map_err(AhError::from)?;
    let current_state: String = request
        .db
        .conn()
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            params![&request.agent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| map_db_error("query agent state for prompt scan", err))?
        .ok_or_else(|| AhError::AgentNotFound(request.agent_id.clone()))?;
    let io = TmuxPromptIo::new((*request.tmux).clone());
    let ctx = RunnerContext::new(
        &request.agent_id,
        &request.pane_id,
        &request.provider,
        &io,
        &kb,
    )
    .with_current_state(&current_state)
    .with_scan_purpose(request.scan_purpose)
    .with_prompt_experience(&request.db)
    .with_llm_classifier(&RealHaikuClassifier)
    .with_marker_matcher(request.marker_matcher.as_ref());
    Ok(handle_prompt_chain(ctx, request.max_depth))
}

pub async fn prompt_pending_unpark_watcher_loop(ctx: Ctx, interval: Duration) {
    let mut state = PromptPendingUnparkState::default();
    loop {
        tokio::time::sleep(interval).await;
        if let Err(err) = prompt_pending_unpark_watcher_tick(&ctx, &mut state).await {
            tracing::warn!(error = %err, "prompt pending auto-unpark watcher tick failed");
        }
    }
}

pub async fn prompt_pending_unpark_watcher_tick(
    ctx: &Ctx,
    state: &mut PromptPendingUnparkState,
) -> Result<PromptPendingUnparkTickResult, AhError> {
    prompt_pending_unpark_watcher_tick_with_before_apply(ctx, state, |_| {}).await
}

async fn prompt_pending_unpark_watcher_tick_with_before_apply<F>(
    ctx: &Ctx,
    state: &mut PromptPendingUnparkState,
    mut before_apply: F,
) -> Result<PromptPendingUnparkTickResult, AhError>
where
    F: FnMut(&str),
{
    let agents = crate::db::agents::query_agents_by_state(
        ctx.db.clone(),
        crate::db::state_machine::STATE_PROMPT_PENDING.to_string(),
    )
    .await?;
    let mut result = PromptPendingUnparkTickResult::default();
    let mut active_agent_ids = std::collections::HashSet::new();

    for agent in agents {
        active_agent_ids.insert(agent.id.clone());
        if !is_prompt_handling_provider(&agent.provider) {
            continue;
        }
        let Some(pane_id) = crate::agent_io::pane_id(&agent.id) else {
            tracing::warn!(
                agent_id = %agent.id,
                "prompt pending auto-unpark skipped agent without registered pane"
            );
            continue;
        };

        let mut bypass_suppression = false;
        if let Some(suppressed_hash) = state.suppressed_hashes.get(&agent.id) {
            match ctx.tmux_server.capture_pane(pane_id.clone()).await {
                Ok(capture) => {
                    if prompt_pending_scan_is_suppressed(suppressed_hash, &capture) {
                        let ticks = state.suppressed_ticks.entry(agent.id.clone()).or_insert(0);
                        *ticks += 1;
                        if *ticks > SUPPRESSION_ESCALATION_TICKS {
                            if !crate::db::events::has_prompt_pending_suppression_escalated(
                                ctx.db.clone(),
                                agent.id.clone(),
                                suppressed_hash.clone(),
                            )
                            .await?
                            {
                                tracing::warn!(
                                    agent_id = %agent.id,
                                    ticks = *ticks,
                                    "PROMPT_PENDING suppression escalated"
                                );

                                let payload = json!({
                                    "agent_id": agent.id,
                                    "ticks": *ticks,
                                    "reason": "PROMPT_PENDING_SUPPRESSION_ESCALATED",
                                    "hash": suppressed_hash.clone(),
                                });

                                let _ = crate::db::events::insert_event(
                                    ctx.db.clone(),
                                    agent.id.clone(),
                                    None,
                                    "state_change".into(),
                                    payload.to_string(),
                                )
                                .await;

                                let now_micro = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_micros()
                                    as i64;
                                crate::orchestrator::pubsub::notify_event(
                                    crate::orchestrator::pubsub::EventFrame {
                                        event_id: 0,
                                        kind: "alert".to_string(),
                                        agent_id: agent.id.clone(),
                                        job_id: None,
                                        state: Some("PROMPT_PENDING".to_string()),
                                        ts_unix_micro: now_micro,
                                        payload: Some(payload),
                                    },
                                );
                            }

                            bypass_suppression = true;
                        } else {
                            result.suppressed += 1;
                            bypass_suppression = false;
                        }
                    } else {
                        state.suppressed_hashes.remove(&agent.id);
                        state.suppressed_ticks.remove(&agent.id);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        agent_id = %agent.id,
                        error = %err,
                        "prompt pending auto-unpark pre-capture failed"
                    );
                    bypass_suppression = false;
                }
            }
        }

        if !state.suppressed_hashes.contains_key(&agent.id) || bypass_suppression {
            let manifest = crate::provider::manifest::get_manifest(&agent.provider);
            let marker_matcher = Arc::new(MarkerMatcher::from_manifest(&manifest));
            let request = PromptScanRequest {
                db: ctx.db.clone(),
                agent_id: agent.id.clone(),
                provider: agent.provider.clone(),
                pane_id,
                tmux: ctx.tmux_server.clone(),
                state_dir: ctx.state_dir.clone(),
                marker_matcher,
                max_depth: 3,
                scan_purpose: PromptScanPurpose::DispatchGuard,
            };
            let outcome = tokio::task::spawn_blocking(move || run_prompt_scan(request))
                .await
                .map_err(|err| AhError::DatabaseRuntimePanic {
                    details: format!("prompt pending auto-unpark worker join failed: {err}"),
                })??;
            result.scanned += 1;

            let suppressed_hash = prompt_pending_suppression_hash(&outcome);
            before_apply(&agent.id);
            match apply_prompt_pending_unpark_outcome_sync(
                &ctx.db,
                &agent.id,
                agent.state_version,
                outcome,
            )? {
                PromptPendingUnparkDisposition::Unparked => {
                    state.suppressed_hashes.remove(&agent.id);
                    state.suppressed_ticks.remove(&agent.id);
                    result.unparked += 1;
                    crate::orchestrator::wake_up();
                }
                PromptPendingUnparkDisposition::Handled => {
                    state.suppressed_hashes.remove(&agent.id);
                    state.suppressed_ticks.remove(&agent.id);
                    result.handled += 1;
                }
                PromptPendingUnparkDisposition::NoAction => {
                    if let Some(hash) = suppressed_hash {
                        let old_hash = state
                            .suppressed_hashes
                            .insert(agent.id.clone(), hash.clone());
                        if old_hash.as_ref() != Some(&hash) {
                            state.suppressed_ticks.remove(&agent.id);
                        }
                    } else {
                        state.suppressed_hashes.remove(&agent.id);
                        state.suppressed_ticks.remove(&agent.id);
                    }
                }
                PromptPendingUnparkDisposition::CasMiss => {
                    state.suppressed_hashes.remove(&agent.id);
                    state.suppressed_ticks.remove(&agent.id);
                }
            }
        }
    }

    state
        .suppressed_hashes
        .retain(|agent_id, _| active_agent_ids.contains(agent_id));
    state
        .suppressed_ticks
        .retain(|agent_id, _| active_agent_ids.contains(agent_id));
    Ok(result)
}

pub(crate) fn apply_prompt_pending_unpark_outcome_sync(
    db: &Db,
    agent_id: &str,
    expected_state_version: i64,
    outcome: PromptRunOutcome,
) -> Result<PromptPendingUnparkDisposition, AhError> {
    match outcome {
        PromptRunOutcome::NoActionNeeded { depth: 0 } => {
            mark_prompt_pending_idle_unparked_sync(db, agent_id, expected_state_version)
        }
        PromptRunOutcome::NoActionNeeded { depth: _ } => {
            Ok(PromptPendingUnparkDisposition::Handled)
        }
        PromptRunOutcome::Pending { .. }
        | PromptRunOutcome::DepthExceeded { .. }
        | PromptRunOutcome::RetryLater { .. } => Ok(PromptPendingUnparkDisposition::NoAction),
        PromptRunOutcome::ExecutorFailed { error, .. } => Err(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PromptPendingUnparkDisposition {
    Unparked,
    Handled,
    NoAction,
    CasMiss,
}

fn prompt_pending_suppression_hash(outcome: &PromptRunOutcome) -> Option<String> {
    match outcome {
        PromptRunOutcome::Pending { snapshot, .. }
        | PromptRunOutcome::DepthExceeded { snapshot, .. } => {
            Some(hex_hash(&snapshot.sanitized_hash))
        }
        _ => None,
    }
}

fn prompt_pending_scan_is_suppressed(suppressed_hash: &str, capture: &str) -> bool {
    let hash = hex_hash(&crate::prompt_handler::gating::hash_sanitized_text(
        &sanitize_pane_text(capture),
    ));
    hash == suppressed_hash
}

fn mark_prompt_pending_idle_unparked_sync(
    db: &Db,
    agent_id: &str,
    expected_state_version: i64,
) -> Result<PromptPendingUnparkDisposition, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin prompt pending auto-unpark", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query prompt pending auto-unpark state", err))?;

    let Some((previous_state, state_version)) = current else {
        tx.rollback().map_err(|err| {
            map_db_error("rollback prompt pending auto-unpark missing agent", err)
        })?;
        return Err(AhError::AgentNotFound(agent_id.to_string()));
    };
    if previous_state != crate::db::state_machine::STATE_PROMPT_PENDING
        || state_version != expected_state_version
    {
        tx.rollback()
            .map_err(|err| map_db_error("rollback prompt pending auto-unpark CAS miss", err))?;
        tracing::info!(
            agent_id,
            state = %previous_state,
            state_version,
            expected_state_version,
            "prompt pending auto-unpark lost state_version race"
        );
        return Ok(PromptPendingUnparkDisposition::CasMiss);
    }

    let changes = tx
        .execute(
            "UPDATE agents SET state = 'IDLE', sub_state = 'PromptIdleSelfHealed', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state = 'PROMPT_PENDING' AND state_version = ?",
            params![agent_id, expected_state_version],
        )
        .map_err(|err| map_db_error("mark prompt pending auto-unpark idle", err))?;
    if changes == 0 {
        tx.rollback()
            .map_err(|err| map_db_error("rollback prompt pending auto-unpark update miss", err))?;
        return Ok(PromptPendingUnparkDisposition::CasMiss);
    }

    let payload = json!({
        "from": crate::db::state_machine::STATE_PROMPT_PENDING,
        "to": crate::db::state_machine::STATE_IDLE,
        "sub_state": "PromptIdleSelfHealed",
        "reason": "PROMPT_PENDING_IDLE_SELF_HEALED",
    })
    .to_string();
    tx.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
        params![agent_id, payload],
    )
    .map_err(|err| map_db_error("insert prompt pending auto-unpark state_change", err))?;
    tx.commit()
        .map_err(|err| map_db_error("commit prompt pending auto-unpark", err))?;
    tracing::info!(agent_id, "prompt pending auto-unparked stable idle pane");
    Ok(PromptPendingUnparkDisposition::Unparked)
}

pub async fn mark_prompt_pending_and_emit_unknown(
    db: Db,
    agent_id: String,
    payload: UnknownPromptPayload,
) -> Result<usize, AhError> {
    spawn_db("prompt_handler::mark_pending_emit_unknown", move || {
        mark_prompt_pending_and_emit_unknown_sync(&db, &agent_id, &payload)
    })
    .await
}

pub(crate) fn mark_prompt_pending_and_emit_unknown_sync(
    db: &Db,
    agent_id: &str,
    payload: &UnknownPromptPayload,
) -> Result<usize, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction()
        .map_err(|err| map_db_error("begin prompt pending event transaction", err))?;
    let current = tx
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query state for prompt pending event", err))?;

    let Some((previous_state, state_version)) = current else {
        tx.rollback()
            .map_err(|err| map_db_error("rollback prompt pending missing agent", err))?;
        return Err(AhError::AgentNotFound(agent_id.to_string()));
    };

    let allowed = [
        crate::db::state_machine::STATE_IDLE,
        crate::db::state_machine::STATE_SPAWNING,
    ];
    if !allowed.contains(&previous_state.as_str()) {
        tx.rollback()
            .map_err(|err| map_db_error("rollback prompt pending invalid state", err))?;
        return Err(AhError::AgentWrongState {
            current_state: previous_state,
        });
    }

    tracing::info!(
        agent_id,
        from = %previous_state,
        block_reason = %payload.block_reason,
        "prompt pending transaction start"
    );
    let changes = tx
        .execute(
            "UPDATE agents SET state = 'PROMPT_PENDING', state_version = state_version + 1, updated_at = unixepoch() WHERE id = ? AND state IN ('IDLE', 'SPAWNING') AND state_version = ?",
            params![agent_id, state_version],
        )
        .map_err(|err| map_db_error("mark prompt pending for unknown prompt", err))?;
    if changes == 0 {
        tx.rollback()
            .map_err(|err| map_db_error("rollback prompt pending CAS miss", err))?;
        return Ok(0);
    }

    let state_payload = json!({
        "from": previous_state,
        "to": crate::db::state_machine::STATE_PROMPT_PENDING,
        "reason": payload.block_reason,
    })
    .to_string();
    tx.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
        params![agent_id, state_payload],
    )
    .map_err(|err| map_db_error("insert prompt pending state_change", err))?;

    let unknown_payload = serde_json::to_string(payload).map_err(|err| {
        AhError::IpcInvalidRequest(format!("serialize unknown prompt payload: {err}"))
    })?;
    tx.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, ?, ?)",
        params![agent_id, UNKNOWN_PROMPT_DETECTED, unknown_payload],
    )
    .map_err(|err| {
        tracing::error!(
            agent_id,
            reason = %err,
            impact = "agent would be pending without master-visible prompt event; transaction rolls back",
            "unknown prompt event insert failed"
        );
        map_db_error("insert unknown prompt event", err)
    })?;

    tx.commit()
        .map_err(|err| map_db_error("commit prompt pending event transaction", err))?;
    tracing::info!(
        agent_id,
        block_reason = %payload.block_reason,
        "prompt pending transaction complete"
    );
    Ok(changes)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::db::agents::{insert_agent_sync, query_agent_sync};
    use crate::db::events::query_events_since_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::db::state_machine::{
        STATE_BUSY, STATE_IDLE, STATE_PROMPT_PENDING, STATE_SPAWNING, STATE_WAITING_FOR_ACK,
    };
    use crate::db::{Db, init};
    use crate::error::AhError;
    use crate::prompt_handler::events::{UNKNOWN_PROMPT_DETECTED, UnknownPromptPayload};
    use crate::prompt_handler::runner::PromptSnapshot;
    use crate::prompt_handler::{PromptCase, PromptFingerprint, PromptKb};
    use crate::rpc::Ctx;
    use crate::sandbox::EnvState;
    use crate::tmux::{TmuxPaneId, TmuxServer};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    fn with_test_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "codex", STATE_IDLE, Some(456)).unwrap();
        }
        test(&db)
    }

    fn state_and_version(db: &Db, agent_id: &str) -> (String, i64) {
        db.conn()
            .query_row(
                "SELECT state, state_version FROM agents WHERE id = ?",
                [agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn state_version_and_sub_state(db: &Db, agent_id: &str) -> (String, i64, Option<String>) {
        db.conn()
            .query_row(
                "SELECT state, state_version, sub_state FROM agents WHERE id = ?",
                [agent_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn pending_outcome(reason: &str) -> crate::prompt_handler::runner::PromptRunOutcome {
        crate::prompt_handler::runner::PromptRunOutcome::Pending {
            snapshot: PromptSnapshot {
                sanitized_hash: [9; 32],
                sanitized_text: "Unknown menu\n› ".into(),
            },
            depth: 0,
            block_reason: reason.to_string(),
        }
    }

    struct TickHarness {
        ctx: Ctx,
        project_dir: tempfile::TempDir,
        _state_dir: tempfile::TempDir,
        _db_file: tempfile::NamedTempFile,
    }

    impl TickHarness {
        fn new() -> Self {
            which::which("tmux").expect("tmux binary required for prompt-handler tick tests");
            let db_file = tempfile::NamedTempFile::new().unwrap();
            let state_dir = tempfile::TempDir::new().unwrap();
            let project_dir = tempfile::TempDir::new().unwrap();
            let ctx = Ctx {
                db: init(db_file.path()).unwrap(),
                state_dir: state_dir.path().to_path_buf(),
                env_state: EnvState {
                    systemd_run_available: false,
                    unsafe_no_sandbox: true,
                    under_systemd: false,
                },
                daemon_unit: None,
                tmux_server: Arc::new(TmuxServer::new(state_dir.path())),
                claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
            };
            Self {
                ctx,
                project_dir,
                _state_dir: state_dir,
                _db_file: db_file,
            }
        }

        async fn spawn_pending_agent(&self, prompt: &str) -> (String, TmuxPaneId) {
            self.spawn_pending_agent_with_cmd(vec![
                "bash".to_string(),
                fixture_path().display().to_string(),
                "--prompt".to_string(),
                prompt.to_string(),
            ])
            .await
        }

        async fn spawn_pending_agent_with_cmd(&self, cmd: Vec<String>) -> (String, TmuxPaneId) {
            let agent_id = format!("ag_tick_{}", uuid::Uuid::new_v4().simple());
            let session_id = format!("s_{agent_id}");
            let session_name = format!("tmux_{agent_id}");
            insert_session_sync(
                &self.ctx.db.conn(),
                &session_id,
                "prompt-tick",
                self.project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            insert_agent_sync(
                &self.ctx.db.conn(),
                &agent_id,
                &session_id,
                "codex",
                STATE_PROMPT_PENDING,
                Some(std::process::id() as i64),
            )
            .unwrap();
            self.ctx
                .tmux_server
                .ensure_session(session_name.clone(), self.project_dir.path().to_path_buf())
                .await
                .unwrap();
            let pane = self
                .ctx
                .tmux_server
                .spawn_window(
                    session_name,
                    agent_id.clone(),
                    self.project_dir.path().to_path_buf(),
                    cmd,
                )
                .await
                .unwrap();
            register_tick_pane(&self.ctx, &agent_id, pane.clone());
            (agent_id, pane)
        }

        async fn spawn_idle_agent(&self, prompt: &str) -> (String, TmuxPaneId) {
            let agent_id = format!("ag_tick_{}", uuid::Uuid::new_v4().simple());
            let session_id = format!("s_{agent_id}");
            let session_name = format!("tmux_{agent_id}");
            insert_session_sync(
                &self.ctx.db.conn(),
                &session_id,
                "prompt-tick",
                self.project_dir.path().to_str().unwrap(),
            )
            .unwrap();
            insert_agent_sync(
                &self.ctx.db.conn(),
                &agent_id,
                &session_id,
                "codex",
                STATE_IDLE,
                Some(std::process::id() as i64),
            )
            .unwrap();
            self.ctx
                .tmux_server
                .ensure_session(session_name.clone(), self.project_dir.path().to_path_buf())
                .await
                .unwrap();
            let cmd = vec![
                "bash".to_string(),
                fixture_path().display().to_string(),
                "--prompt".to_string(),
                prompt.to_string(),
            ];
            let pane = self
                .ctx
                .tmux_server
                .spawn_window(
                    session_name,
                    agent_id.clone(),
                    self.project_dir.path().to_path_buf(),
                    cmd,
                )
                .await
                .unwrap();
            register_tick_pane(&self.ctx, &agent_id, pane.clone());
            (agent_id, pane)
        }
    }

    impl Drop for TickHarness {
        fn drop(&mut self) {
            let _ = Command::new("tmux")
                .args(["-L", self.ctx.tmux_server.socket_name(), "kill-server"])
                .output();
            let socket_path = format!(
                "/tmp/tmux-{}/{}",
                unsafe { libc::geteuid() },
                self.ctx.tmux_server.socket_name()
            );
            let _ = std::fs::remove_file(socket_path);
        }
    }

    fn fixture_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("mock_prompt_provider.sh")
    }

    fn register_tick_pane(ctx: &Ctx, agent_id: &str, pane: TmuxPaneId) {
        let fifo_path = ctx.state_dir.join("pipes").join(format!("{agent_id}.fifo"));
        std::fs::create_dir_all(fifo_path.parent().unwrap()).unwrap();
        std::fs::write(&fifo_path, b"").unwrap();
        let reader_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        crate::agent_io::register(
            agent_id.to_string(),
            crate::agent_io::AgentIoEntry {
                session_id: format!("s_{agent_id}"),
                pane_id: pane,
                expected_pid: None,
                reader_handle,
                fifo_path,
                socket_name: ctx.tmux_server.socket_name().to_string(),
                idle_scan_enabled: Arc::new(AtomicBool::new(false)),
            },
        );
    }

    async fn wait_for_tick_pane_contains(ctx: &Ctx, pane: &TmuxPaneId, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last_capture = String::new();
        while Instant::now() < deadline {
            last_capture = ctx.tmux_server.capture_pane(pane.clone()).await.unwrap();
            if last_capture.contains(needle) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("pane did not contain {needle:?}; last capture:\n{last_capture}");
    }

    #[test]
    fn rg_prompt_pending_confirmed_idle_unparks_to_idle() {
        with_test_db(|db| {
            db.conn()
                .execute(
                    "UPDATE agents SET state = 'PROMPT_PENDING', state_version = 5 WHERE id = 'a1'",
                    [],
                )
                .unwrap();

            let disposition = apply_prompt_pending_unpark_outcome_sync(
                db,
                "a1",
                5,
                crate::prompt_handler::runner::PromptRunOutcome::NoActionNeeded { depth: 0 },
            )
            .unwrap();

            assert_eq!(disposition, PromptPendingUnparkDisposition::Unparked);
            assert_eq!(state_and_version(db, "a1"), (STATE_IDLE.to_string(), 6));
        });
    }

    #[test]
    fn rg_prompt_pending_unknown_menu_stays_pending() {
        with_test_db(|db| {
            db.conn()
                .execute(
                    "UPDATE agents SET state = 'PROMPT_PENDING', state_version = 5 WHERE id = 'a1'",
                    [],
                )
                .unwrap();

            let disposition = apply_prompt_pending_unpark_outcome_sync(
                db,
                "a1",
                5,
                pending_outcome("unknown_prompt"),
            )
            .unwrap();

            assert_eq!(disposition, PromptPendingUnparkDisposition::NoAction);
            assert_eq!(
                state_and_version(db, "a1"),
                (STATE_PROMPT_PENDING.to_string(), 5)
            );
        });
    }

    #[test]
    fn rg_prompt_pending_unpark_cas_loses_after_manual_resolve() {
        with_test_db(|db| {
            db.conn()
                .execute(
                    "UPDATE agents SET state = 'PROMPT_PENDING', state_version = 5 WHERE id = 'a1'",
                    [],
                )
                .unwrap();
            db.conn()
                .execute(
                    "UPDATE agents SET state = 'IDLE', state_version = 6 WHERE id = 'a1'",
                    [],
                )
                .unwrap();

            let disposition = apply_prompt_pending_unpark_outcome_sync(
                db,
                "a1",
                5,
                crate::prompt_handler::runner::PromptRunOutcome::NoActionNeeded { depth: 0 },
            )
            .unwrap();

            assert_eq!(disposition, PromptPendingUnparkDisposition::CasMiss);
            assert_eq!(state_and_version(db, "a1"), (STATE_IDLE.to_string(), 6));
        });
    }

    #[test]
    fn rg_known_menu_handled_does_not_unpark_same_tick() {
        with_test_db(|db| {
            db.conn()
                .execute(
                    "UPDATE agents SET state = 'PROMPT_PENDING', state_version = 5 WHERE id = 'a1'",
                    [],
                )
                .unwrap();

            let disposition = apply_prompt_pending_unpark_outcome_sync(
                db,
                "a1",
                5,
                crate::prompt_handler::runner::PromptRunOutcome::NoActionNeeded { depth: 1 },
            )
            .unwrap();

            assert_eq!(disposition, PromptPendingUnparkDisposition::Handled);
            assert_eq!(
                state_and_version(db, "a1"),
                (STATE_PROMPT_PENDING.to_string(), 5)
            );
        });
    }

    #[test]
    fn rg_prompt_pending_suppression_skips_same_unknown_hash_only() {
        let hash = crate::prompt_handler::events::hex_hash(
            &crate::prompt_handler::gating::hash_sanitized_text(
                &crate::prompt_handler::matcher::sanitize_pane_text("Confirm risky action\n› "),
            ),
        );

        assert!(prompt_pending_scan_is_suppressed(
            &hash,
            "Confirm risky action\n› "
        ));
        assert!(!prompt_pending_scan_is_suppressed(&hash, "clean idle\n› "));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tick_real_unknown_menu_must_not_unpark() {
        let h = TickHarness::new();
        let (agent_id, pane) = h.spawn_pending_agent("unknown_eula").await;
        wait_for_tick_pane_contains(&h.ctx, &pane, "New provider EULA").await;

        let mut state = PromptPendingUnparkState::default();
        let result = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state)
            .await
            .unwrap();

        assert_eq!(result.scanned, 1);
        assert_eq!(result.unparked, 0);
        assert!(
            state.suppressed_hashes.contains_key(&agent_id),
            "unknown pending outcome should suppress the same stable hash for the next tick"
        );
        assert_eq!(
            state_and_version(&h.ctx.db, &agent_id).0,
            STATE_PROMPT_PENDING
        );
        if let Some(entry) = crate::agent_io::remove(&agent_id) {
            entry.reader_handle.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tick_unpark_cas_race_only_one_wins() {
        let h = TickHarness::new();
        let ready_script = r#"printf '\033[?1049h\033[2J\033[Hready\n'
printf '\033[60;1H  › '
stty raw -echo 2>/dev/null || true
while true; do
  ch=''
  IFS= read -rsn1 ch || break
  case "$ch" in $'\177'|$'\b') continue ;; esac
  printf '%s' "$ch"
  sleep 0.35
  printf '\033[D \033[D'
done"#;
        let (agent_id, pane) = h
            .spawn_pending_agent_with_cmd(vec![
                "bash".to_string(),
                "-lc".to_string(),
                ready_script.to_string(),
            ])
            .await;
        wait_for_tick_pane_contains(&h.ctx, &pane, "ready").await;
        let (_, initial_version) = state_and_version(&h.ctx.db, &agent_id);

        let mut state = PromptPendingUnparkState::default();
        let result = prompt_pending_unpark_watcher_tick_with_before_apply(
            &h.ctx,
            &mut state,
            |candidate_agent_id| {
                if candidate_agent_id == agent_id {
                    h.ctx
                        .db
                        .conn()
                        .execute(
                            "UPDATE agents SET state = 'IDLE', sub_state = 'ManualResolve', state_version = state_version + 1 WHERE id = ?",
                            [&agent_id],
                        )
                        .unwrap();
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result.scanned, 1);
        assert_eq!(result.unparked, 0);
        assert!(
            state.suppressed_hashes.is_empty(),
            "CAS miss must clear suppression state instead of treating the outcome as NoAction"
        );
        assert_eq!(
            state_version_and_sub_state(&h.ctx.db, &agent_id),
            (
                STATE_IDLE.to_string(),
                initial_version + 1,
                Some("ManualResolve".to_string())
            )
        );
        if let Some(entry) = crate::agent_io::remove(&agent_id) {
            entry.reader_handle.abort();
        }
    }

    #[test]
    fn pending_state_and_unknown_event_are_written_together() {
        with_test_db(|db| {
            let payload = UnknownPromptPayload::new(
                &PromptSnapshot {
                    sanitized_hash: [1; 32],
                    sanitized_text: "Unknown prompt".into(),
                },
                "unknown_prompt",
                0,
                "codex",
            );

            let changes = mark_prompt_pending_and_emit_unknown_sync(db, "a1", &payload).unwrap();

            assert_eq!(changes, 1);
            let conn = db.conn();
            let agent = query_agent_sync(&conn, "a1").unwrap().unwrap();
            assert_eq!(agent.state, STATE_PROMPT_PENDING);
            let events = query_events_since_sync(&conn, "a1", 0).unwrap();
            assert_eq!(events[0].event_type, "state_change");
            assert_eq!(events[1].event_type, UNKNOWN_PROMPT_DETECTED);
        });
    }

    #[test]
    fn pending_unknown_event_preserves_llm_block_reasons() {
        for reason in [
            "missing_api_key",
            "llm_error",
            "llm_timeout",
            "llm_low_confidence",
            "llm_unsafe",
            "unknown_prompt",
        ] {
            with_test_db(|db| {
                let payload = UnknownPromptPayload::new(
                    &PromptSnapshot {
                        sanitized_hash: [4; 32],
                        sanitized_text: format!("Prompt for {reason}"),
                    },
                    reason,
                    0,
                    "codex",
                );

                mark_prompt_pending_and_emit_unknown_sync(db, "a1", &payload).unwrap();

                let conn = db.conn();
                let events = query_events_since_sync(&conn, "a1", 0).unwrap();
                let unknown_payload: serde_json::Value =
                    serde_json::from_str(&events[1].payload).unwrap();
                assert_eq!(unknown_payload["block_reason"], reason);
                let state_payload: serde_json::Value =
                    serde_json::from_str(&events[0].payload).unwrap();
                assert_eq!(state_payload["reason"], reason);
            });
        }
    }

    #[test]
    fn pending_state_and_unknown_event_allows_persistent_spawning_agent() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_spawn", "p_spawn", "/tmp/spawn").unwrap();
            insert_agent_sync(
                &conn,
                "a_spawn",
                "s_spawn",
                "codex",
                STATE_SPAWNING,
                Some(789),
            )
            .unwrap();
        }
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [2; 32],
                sanitized_text: "Startup prompt".into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );

        let changes = mark_prompt_pending_and_emit_unknown_sync(&db, "a_spawn", &payload).unwrap();

        let conn = db.conn();
        let agent = query_agent_sync(&conn, "a_spawn").unwrap().unwrap();
        assert_eq!(changes, 1);
        assert_eq!(agent.state, STATE_PROMPT_PENDING);
        let events = query_events_since_sync(&conn, "a_spawn", 0).unwrap();
        assert_eq!(events[0].event_type, "state_change");
        assert_eq!(events[1].event_type, UNKNOWN_PROMPT_DETECTED);
    }

    #[test]
    fn pending_state_and_unknown_event_rejects_persistent_waiting_for_ack_agent() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_waiting", "p_waiting", "/tmp/waiting").unwrap();
            insert_agent_sync(
                &conn,
                "a_waiting",
                "s_waiting",
                "codex",
                STATE_WAITING_FOR_ACK,
                Some(789),
            )
            .unwrap();
        }
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [3; 32],
                sanitized_text: "Banner repaint".into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );

        let err =
            mark_prompt_pending_and_emit_unknown_sync(&db, "a_waiting", &payload).unwrap_err();

        let conn = db.conn();
        let agent = query_agent_sync(&conn, "a_waiting").unwrap().unwrap();
        assert!(matches!(err, AhError::AgentWrongState { .. }));
        assert_eq!(agent.state, STATE_WAITING_FOR_ACK);
        let events = query_events_since_sync(&conn, "a_waiting", 0).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn busy_unknown_prompt_scan_is_deferred_and_agent_stays_busy() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_busy", "p_busy", "/tmp/busy").unwrap();
            insert_agent_sync(&conn, "a_busy", "s_busy", "codex", STATE_BUSY, Some(789)).unwrap();
        }
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [5; 32],
                sanitized_text: "Active job banner".into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );

        let err = mark_prompt_pending_and_emit_unknown_sync(&db, "a_busy", &payload).unwrap_err();

        assert!(matches!(err, AhError::AgentWrongState { .. }));
        let conn = db.conn();
        let agent = query_agent_sync(&conn, "a_busy").unwrap().unwrap();
        assert_eq!(agent.state, STATE_BUSY);
        let events = query_events_since_sync(&conn, "a_busy", 0).unwrap();
        assert!(events.is_empty());
        assert_eq!(
            transient_unknown_prompt_disposition("a_busy", STATE_BUSY, "hash-busy", 2),
            PromptScanDisposition::Deferred {
                depth: 0,
                block_reason: "unknown_prompt_stabilizing".to_string(),
            }
        );
    }

    #[test]
    fn waiting_for_ack_unknown_prompt_scan_is_deferred_and_agent_stays_waiting() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_wait", "p_wait", "/tmp/wait").unwrap();
            insert_agent_sync(
                &conn,
                "a_wait",
                "s_wait",
                "codex",
                STATE_WAITING_FOR_ACK,
                Some(789),
            )
            .unwrap();
        }
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [6; 32],
                sanitized_text: "ACK repaint".into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );

        let err = mark_prompt_pending_and_emit_unknown_sync(&db, "a_wait", &payload).unwrap_err();

        assert!(matches!(err, AhError::AgentWrongState { .. }));
        let conn = db.conn();
        let agent = query_agent_sync(&conn, "a_wait").unwrap().unwrap();
        assert_eq!(agent.state, STATE_WAITING_FOR_ACK);
        let events = query_events_since_sync(&conn, "a_wait", 0).unwrap();
        assert!(events.is_empty());
        assert_eq!(
            transient_unknown_prompt_disposition("a_wait", STATE_WAITING_FOR_ACK, "hash-wait", 2),
            PromptScanDisposition::Deferred {
                depth: 0,
                block_reason: "unknown_prompt_stabilizing".to_string(),
            }
        );
    }

    #[test]
    fn idle_unknown_prompt_still_enters_prompt_pending() {
        with_test_db(|db| {
            let payload = UnknownPromptPayload::new(
                &PromptSnapshot {
                    sanitized_hash: [7; 32],
                    sanitized_text: "Idle prompt".into(),
                },
                "unknown_prompt",
                0,
                "codex",
            );

            let changes = mark_prompt_pending_and_emit_unknown_sync(db, "a1", &payload).unwrap();

            let conn = db.conn();
            let agent = query_agent_sync(&conn, "a1").unwrap().unwrap();
            assert_eq!(changes, 1);
            assert_eq!(agent.state, STATE_PROMPT_PENDING);
        });
    }

    #[test]
    fn spawning_unknown_prompt_still_enters_prompt_pending() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_spawn_new", "p_spawn_new", "/tmp/spawn-new").unwrap();
            insert_agent_sync(
                &conn,
                "a_spawn_new",
                "s_spawn_new",
                "codex",
                STATE_SPAWNING,
                Some(789),
            )
            .unwrap();
        }
        let payload = UnknownPromptPayload::new(
            &PromptSnapshot {
                sanitized_hash: [8; 32],
                sanitized_text: "Startup prompt".into(),
            },
            "unknown_prompt",
            0,
            "codex",
        );

        let changes =
            mark_prompt_pending_and_emit_unknown_sync(&db, "a_spawn_new", &payload).unwrap();

        let conn = db.conn();
        let agent = query_agent_sync(&conn, "a_spawn_new").unwrap().unwrap();
        assert_eq!(changes, 1);
        assert_eq!(agent.state, STATE_PROMPT_PENDING);
    }

    #[test]
    fn transient_unknown_prompt_defers_once_then_promotes_when_stable() {
        let first =
            transient_unknown_prompt_disposition("a_spawn_stable", STATE_SPAWNING, "hash-a", 2);
        let second =
            transient_unknown_prompt_disposition("a_spawn_stable", STATE_SPAWNING, "hash-a", 2);

        assert_eq!(
            first,
            PromptScanDisposition::Deferred {
                depth: 0,
                block_reason: "unknown_prompt_stabilizing".to_string(),
            }
        );
        assert!(matches!(second, PromptScanDisposition::Pending { .. }));
    }

    #[test]
    fn transient_unknown_prompt_keeps_deferring_when_snapshot_changes() {
        let first = transient_unknown_prompt_disposition(
            "a_spawn_changing",
            STATE_WAITING_FOR_ACK,
            "hash-a",
            2,
        );
        let second = transient_unknown_prompt_disposition(
            "a_spawn_changing",
            STATE_WAITING_FOR_ACK,
            "hash-b",
            2,
        );

        assert!(matches!(first, PromptScanDisposition::Deferred { .. }));
        assert!(matches!(second, PromptScanDisposition::Deferred { .. }));
    }

    #[test]
    fn disposition_pending_shape_is_stable() {
        assert_eq!(
            PromptScanDisposition::Pending {
                depth: 3,
                block_reason: "depth_exceeded".into()
            },
            PromptScanDisposition::Pending {
                depth: 3,
                block_reason: "depth_exceeded".into()
            }
        );
    }

    #[test]
    fn prompt_handling_provider_gate_excludes_shell_providers() {
        assert!(is_prompt_handling_provider("codex"));
        assert!(is_prompt_handling_provider("claude"));
        assert!(!is_prompt_handling_provider("bash"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_prompt_pending_suppression_escalation_and_ttl() {
        let h = TickHarness::new();
        let (agent_id, pane) = h.spawn_pending_agent("unknown_eula").await;
        wait_for_tick_pane_contains(&h.ctx, &pane, "New provider EULA").await;

        let mut state = PromptPendingUnparkState::default();

        // 1. First tick runs the scanner and registers suppression
        let result1 = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state)
            .await
            .unwrap();
        assert_eq!(result1.scanned, 1);
        assert_eq!(result1.suppressed, 0);
        assert!(state.suppressed_hashes.contains_key(&agent_id));
        assert_eq!(state.suppressed_ticks.get(&agent_id), None);

        // 2. Run for N ticks (N = SUPPRESSION_ESCALATION_TICKS)
        // All of these should hit suppression (continue) without running scanner
        for tick in 1..=SUPPRESSION_ESCALATION_TICKS {
            let res = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state)
                .await
                .unwrap();
            assert_eq!(res.scanned, 0, "Tick {tick}: Should not run scanner");
            assert_eq!(
                res.suppressed, 1,
                "Tick {tick}: Should increment suppressed count"
            );
            assert_eq!(state.suppressed_ticks.get(&agent_id), Some(&tick));

            // Check that no escalation event exists yet
            let count_esc: i64 = h.ctx.db.conn().query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%PROMPT_PENDING_SUPPRESSION_ESCALATED%'",
                [&agent_id],
                |row| row.get(0),
            ).unwrap();
            assert_eq!(
                count_esc, 0,
                "Tick {tick}: Should not escalate before threshold"
            );
        }

        // 3. The (N + 1)th tick should escalate and re-run the scanner!
        let res_esc = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state)
            .await
            .unwrap();

        // Assert B1(i): Emit alert event
        let count_esc: i64 = h.ctx.db.conn().query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%PROMPT_PENDING_SUPPRESSION_ESCALATED%'",
            [&agent_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(
            count_esc, 1,
            "B1(i): Should emit exactly one escalation alert"
        );

        // Assert B1(ii): Re-run scanner (meaning scanned count increments)
        assert_eq!(res_esc.scanned, 1, "B1(ii): Should re-run full scanner");
        assert_eq!(
            res_esc.suppressed, 0,
            "B1(ii): Should not increment suppressed count for this tick"
        );

        // Assert B4: Agent state must remain PROMPT_PENDING (fail-closed)
        let (state_val, _) = state_and_version(&h.ctx.db, &agent_id);
        assert_eq!(
            state_val, STATE_PROMPT_PENDING,
            "B4: Agent state must remain PROMPT_PENDING (fail-closed, escalation/re-run does not alter state directly)"
        );

        // Run (N + 2)th tick (still suppressed, still escalated)
        let res_esc_2 = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state)
            .await
            .unwrap();

        // Assert that scanner is STILL re-run
        assert_eq!(
            res_esc_2.scanned, 1,
            "B1(ii)-2: Should STILL re-run scanner on subsequent escalated ticks"
        );
        assert_eq!(res_esc_2.suppressed, 0);

        // Assert that only ONE escalation event is emitted in total (deduplicated)
        let count_esc_2: i64 = h.ctx.db.conn().query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%PROMPT_PENDING_SUPPRESSION_ESCALATED%'",
            [&agent_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(
            count_esc_2, 1,
            "Deduplication: Should emit exactly one escalation alert across all escalated ticks"
        );

        // 4. Test B3: Hash changes after 2 ticks of suppression
        // First, clear everything and spawn a new agent to isolate hash change logic cleanly
        if let Some(entry) = crate::agent_io::remove(&agent_id) {
            entry.reader_handle.abort();
        }

        let (agent_id2, pane2) = h.spawn_pending_agent("unknown_eula").await;
        wait_for_tick_pane_contains(&h.ctx, &pane2, "New provider EULA").await;

        let mut state2 = PromptPendingUnparkState::default();

        // 4.1 First tick registers suppression
        let result2_1 = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state2)
            .await
            .unwrap();
        assert_eq!(result2_1.scanned, 1);
        assert!(state2.suppressed_hashes.contains_key(&agent_id2));

        // 4.2 Run 2 ticks of suppression
        for tick in 1..=2 {
            let res = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state2)
                .await
                .unwrap();
            assert_eq!(res.scanned, 0);
            assert_eq!(res.suppressed, 1);
            assert_eq!(state2.suppressed_ticks.get(&agent_id2), Some(&tick));
        }

        let old_hash2 = state2.suppressed_hashes.get(&agent_id2).cloned();

        // 4.3 Send keyboard inputs to change pane content and thus pane hash
        h.ctx
            .tmux_server
            .send_keys_literal(pane2.clone(), "1".to_string())
            .await
            .unwrap();
        h.ctx.tmux_server.send_enter(pane2.clone()).await.unwrap();
        wait_for_tick_pane_contains(&h.ctx, &pane2, "selected=1").await;

        // 4.4 Run tick on hash change
        let res_change = prompt_pending_unpark_watcher_tick(&h.ctx, &mut state2)
            .await
            .unwrap();

        // Assert B3: ticks and hashes are removed/cleared
        assert_eq!(
            state2.suppressed_ticks.get(&agent_id2),
            None,
            "B3: suppressed_ticks should be cleared on hash change"
        );
        assert_ne!(
            state2.suppressed_hashes.get(&agent_id2),
            old_hash2.as_ref(),
            "B3: suppressed_hashes should have changed to the new hash"
        );
        assert_eq!(
            res_change.scanned, 1,
            "B3: Should run scanner on hash change"
        );
        assert_eq!(res_change.suppressed, 0);

        if let Some(entry) = crate::agent_io::remove(&agent_id2) {
            entry.reader_handle.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fix_c_compliance() {
        let h = TickHarness::new();
        let manifest = crate::provider::manifest::get_manifest("codex");
        let marker_matcher = Arc::new(MarkerMatcher::from_manifest(&manifest));

        // C1 (ghost_input)
        {
            let (agent_id_c1, pane_c1) = h.spawn_idle_agent("ghost_input").await;
            wait_for_tick_pane_contains(&h.ctx, &pane_c1, "echo hello").await;

            let disp_c1 = scan_prompt_and_apply_outcome(PromptScanRequest {
                db: h.ctx.db.clone(),
                agent_id: agent_id_c1.clone(),
                provider: "codex".to_string(),
                pane_id: pane_c1.clone(),
                tmux: h.ctx.tmux_server.clone(),
                state_dir: h.ctx.state_dir.clone(),
                marker_matcher: marker_matcher.clone(),
                max_depth: 3,
                scan_purpose: PromptScanPurpose::Direct,
            })
            .await
            .unwrap();

            let state_c1 = query_agent_state(h.ctx.db.clone(), agent_id_c1.clone())
                .await
                .unwrap();
            let conn = h.ctx.db.conn();
            let events_c1 = query_events_since_sync(&conn, &agent_id_c1, 0).unwrap();

            assert_eq!(state_c1, STATE_IDLE, "C1: Agent must remain IDLE");
            assert!(
                !matches!(disp_c1, PromptScanDisposition::Pending { .. }),
                "C1: Disposition must not be Pending"
            );
            assert!(
                events_c1
                    .iter()
                    .any(|e| e.event_type == UNKNOWN_PROMPT_DETECTED),
                "C1: UNKNOWN_PROMPT_DETECTED event must be emitted"
            );

            if let Some(entry) = crate::agent_io::remove(&agent_id_c1) {
                entry.reader_handle.abort();
            }
        }

        // C2 (push_notif)
        {
            let (agent_id_c2, pane_c2) = h.spawn_idle_agent("push_notif").await;
            wait_for_tick_pane_contains(&h.ctx, &pane_c2, "[Notification]").await;

            let disp_c2 = scan_prompt_and_apply_outcome(PromptScanRequest {
                db: h.ctx.db.clone(),
                agent_id: agent_id_c2.clone(),
                provider: "codex".to_string(),
                pane_id: pane_c2.clone(),
                tmux: h.ctx.tmux_server.clone(),
                state_dir: h.ctx.state_dir.clone(),
                marker_matcher: marker_matcher.clone(),
                max_depth: 3,
                scan_purpose: PromptScanPurpose::Direct,
            })
            .await
            .unwrap();

            let state_c2 = query_agent_state(h.ctx.db.clone(), agent_id_c2.clone())
                .await
                .unwrap();
            let conn = h.ctx.db.conn();
            let events_c2 = query_events_since_sync(&conn, &agent_id_c2, 0).unwrap();

            assert_eq!(state_c2, STATE_IDLE, "C2: Agent must remain IDLE");
            assert!(
                !matches!(disp_c2, PromptScanDisposition::Pending { .. }),
                "C2: Disposition must not be Pending"
            );
            assert!(
                events_c2
                    .iter()
                    .any(|e| e.event_type == UNKNOWN_PROMPT_DETECTED),
                "C2: UNKNOWN_PROMPT_DETECTED event must be emitted"
            );

            if let Some(entry) = crate::agent_io::remove(&agent_id_c2) {
                entry.reader_handle.abort();
            }
        }

        // C3 (Arbitrary stable_unknown)
        {
            let (agent_id_c3, pane_c3) = h.spawn_idle_agent("stable_unknown").await;
            wait_for_tick_pane_contains(&h.ctx, &pane_c3, "Mystery provider").await;

            let disp_c3 = scan_prompt_and_apply_outcome(PromptScanRequest {
                db: h.ctx.db.clone(),
                agent_id: agent_id_c3.clone(),
                provider: "codex".to_string(),
                pane_id: pane_c3.clone(),
                tmux: h.ctx.tmux_server.clone(),
                state_dir: h.ctx.state_dir.clone(),
                marker_matcher: marker_matcher.clone(),
                max_depth: 3,
                scan_purpose: PromptScanPurpose::Direct,
            })
            .await
            .unwrap();

            let state_c3 = query_agent_state(h.ctx.db.clone(), agent_id_c3.clone())
                .await
                .unwrap();
            let conn = h.ctx.db.conn();
            let events_c3 = query_events_since_sync(&conn, &agent_id_c3, 0).unwrap();

            assert_eq!(state_c3, STATE_IDLE, "C3: Agent must remain IDLE");
            assert!(
                !matches!(disp_c3, PromptScanDisposition::Pending { .. }),
                "C3: Disposition must not be Pending"
            );
            assert!(
                events_c3
                    .iter()
                    .any(|e| e.event_type == UNKNOWN_PROMPT_DETECTED),
                "C3: UNKNOWN_PROMPT_DETECTED event must be emitted"
            );

            if let Some(entry) = crate::agent_io::remove(&agent_id_c3) {
                entry.reader_handle.abort();
            }
        }

        // C4 (Whitelisted known dialog with empty actions - MUST PARK)
        {
            let kb_path = h.ctx.state_dir.join("prompt-cases.json");
            let case = PromptCase {
                id: "trust_path_01".to_string(), // In whitelist
                provider: None,
                fingerprint: PromptFingerprint::Regex {
                    pattern: "(?is)Mystery provider".to_string(),
                },
                action: vec![], // Empty actions!
                category: "manual-resolve".to_string(),
                description: Some("Test whitelisted dialog".to_string()),
                confidence_threshold: Some(0.9),
                used_count: 0,
                created_at: None,
                last_used_at: None,
                created_by: Some("test".to_string()),
                regex_flags: vec!["Dotall".to_string(), "CaseInsensitive".to_string()],
                trigger_state: None,
            };
            let kb = PromptKb::new(vec![case]);
            crate::prompt_handler::kb::save_kb_atomic(&kb_path, &kb).unwrap();

            let loaded_kb = crate::prompt_handler::kb::load_or_bootstrap_kb(&kb_path).unwrap();

            let (agent_id_c4, pane_c4) = h.spawn_idle_agent("stable_unknown").await;
            wait_for_tick_pane_contains(&h.ctx, &pane_c4, "Mystery provider").await;

            let disp_c4 = scan_prompt_and_apply_outcome(PromptScanRequest {
                db: h.ctx.db.clone(),
                agent_id: agent_id_c4.clone(),
                provider: "codex".to_string(),
                pane_id: pane_c4.clone(),
                tmux: h.ctx.tmux_server.clone(),
                state_dir: h.ctx.state_dir.clone(),
                marker_matcher: marker_matcher.clone(),
                max_depth: 3,
                scan_purpose: PromptScanPurpose::Direct,
            })
            .await
            .unwrap();

            let state_c4 = query_agent_state(h.ctx.db.clone(), agent_id_c4.clone())
                .await
                .unwrap();
            let conn = h.ctx.db.conn();
            let events_c4 = query_events_since_sync(&conn, &agent_id_c4, 0).unwrap();

            assert_eq!(
                state_c4, STATE_PROMPT_PENDING,
                "C4: Whitelisted dialog must park agent to PROMPT_PENDING"
            );
            assert!(
                matches!(disp_c4, PromptScanDisposition::Pending { .. }),
                "C4: Disposition must be Pending"
            );
            assert!(
                events_c4
                    .iter()
                    .any(|e| e.event_type == UNKNOWN_PROMPT_DETECTED),
                "C4: UNKNOWN_PROMPT_DETECTED event must be emitted"
            );

            if let Some(entry) = crate::agent_io::remove(&agent_id_c4) {
                entry.reader_handle.abort();
            }
        }

        // C5 (KnownAction regression - executes actions, does not park)
        {
            let kb_path = h.ctx.state_dir.join("prompt-cases.json");
            let _ = std::fs::remove_file(kb_path);

            let (agent_id_c5, pane_c5) = h.spawn_idle_agent("codex_update_ready").await;
            wait_for_tick_pane_contains(&h.ctx, &pane_c5, "Update available").await;

            let disp_c5 = scan_prompt_and_apply_outcome(PromptScanRequest {
                db: h.ctx.db.clone(),
                agent_id: agent_id_c5.clone(),
                provider: "codex".to_string(),
                pane_id: pane_c5.clone(),
                tmux: h.ctx.tmux_server.clone(),
                state_dir: h.ctx.state_dir.clone(),
                marker_matcher: marker_matcher.clone(),
                max_depth: 3,
                scan_purpose: PromptScanPurpose::Direct,
            })
            .await
            .unwrap();

            let state_c5 = query_agent_state(h.ctx.db.clone(), agent_id_c5.clone())
                .await
                .unwrap();
            assert_eq!(
                state_c5, STATE_IDLE,
                "C5: Automated KnownAction must not park"
            );
            assert!(
                matches!(disp_c5, PromptScanDisposition::Handled { .. }),
                "C5: Disposition must be Handled"
            );

            if let Some(entry) = crate::agent_io::remove(&agent_id_c5) {
                entry.reader_handle.abort();
            }
        }
    }
}
