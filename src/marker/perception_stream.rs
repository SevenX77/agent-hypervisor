use crate::db::{self as db, Db};
use crate::marker::{MarkerMatcher, MatchResult, parser_registry, registry};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

#[derive(Clone)]
pub struct PerceptionStreamConfig {
    pub matcher: Arc<MarkerMatcher>,
    pub stability_ms: u64,
    pub idle_scan_enabled: Arc<AtomicBool>,
}

impl Default for PerceptionStreamConfig {
    fn default() -> Self {
        Self {
            matcher: Arc::new(MarkerMatcher::default()),
            stability_ms: 0,
            idle_scan_enabled: Arc::new(AtomicBool::new(true)),
        }
    }
}

pub fn spawn_perception_stream_processor_task(
    agent_id: String,
    db: Db,
    parser: Arc<Mutex<vt100::Parser>>,
    output_rx: mpsc::Receiver<Vec<u8>>,
    config: PerceptionStreamConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        process_perception_stream(agent_id, db, parser, output_rx, config).await;
    })
}

async fn process_perception_stream(
    agent_id: String,
    db: Db,
    parser: Arc<Mutex<vt100::Parser>>,
    mut output_rx: mpsc::Receiver<Vec<u8>>,
    config: PerceptionStreamConfig,
) {
    let matcher = config.matcher;
    let stability = Duration::from_millis(config.stability_ms);
    let mut pending_stability_match = false;
    let mut skip_scan_after_stability_noise = false;

    loop {
        let read_from_pending_stability;
        let chunk = if pending_stability_match && config.stability_ms > 0 {
            match timeout(stability, output_rx.recv()).await {
                Err(_) => {
                    mark_idle_after_marker_match(
                        &agent_id,
                        db.clone(),
                        "stable marker match",
                        TimerCancelPolicy::Always,
                    )
                    .await;
                    pending_stability_match = false;
                    continue;
                }
                Ok(Some(chunk)) => {
                    read_from_pending_stability = true;
                    chunk
                }
                Ok(None) => break,
            }
        } else {
            match output_rx.recv().await {
                Some(chunk) => {
                    read_from_pending_stability = false;
                    chunk
                }
                None => break,
            }
        };

        if read_from_pending_stability {
            pending_stability_match = false;
            skip_scan_after_stability_noise = true;
        }

        match parser.lock() {
            Ok(mut parser) => parser.process(&chunk),
            Err(err) => {
                tracing::warn!(agent_id = %agent_id, error = %err, "parser mutex poisoned in perception stream processor");
                continue;
            }
        }

        let text = String::from_utf8_lossy(&chunk).to_string();
        let payload = serde_json::json!({ "text": text }).to_string();
        if let Err(err) = db::events::insert_event(
            db.clone(),
            agent_id.clone(),
            None,
            "output_chunk".into(),
            payload,
        )
        .await
        {
            tracing::warn!(agent_id = %agent_id, error = %err, "failed to persist fifo output chunk");
        } else {
            crate::orchestrator::pubsub::notify_agent_output(&agent_id);
        }

        let matched = if !config.idle_scan_enabled.load(Ordering::SeqCst) {
            MatchResult::NoMatch
        } else if skip_scan_after_stability_noise {
            skip_scan_after_stability_noise = false;
            MatchResult::NoMatch
        } else {
            match parser.lock() {
                Ok(parser) => matcher.scan(&parser),
                Err(err) => {
                    tracing::warn!(agent_id = %agent_id, error = %err, "parser mutex poisoned during marker scan");
                    MatchResult::NoMatch
                }
            }
        };

        match matched {
            MatchResult::Matched => {
                if config.stability_ms > 0 {
                    pending_stability_match = true;
                } else {
                    mark_idle_after_marker_match(
                        &agent_id,
                        db.clone(),
                        "marker match",
                        TimerCancelPolicy::OnStateChange,
                    )
                    .await;
                }
            }
            MatchResult::NoMatch => {
                pending_stability_match = false;
                registry::reset(&agent_id);
            }
        }
    }

    let _ = parser_registry::remove(&agent_id);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerCancelPolicy {
    Always,
    OnStateChange,
}

async fn mark_idle_after_marker_match(
    agent_id: &str,
    db: Db,
    reason: &'static str,
    timer_cancel_policy: TimerCancelPolicy,
) {
    match db::state_machine::mark_agent_idle_matched(db, agent_id.to_string()).await {
        Ok((changes, affected_job)) if changes > 0 => {
            // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
            if let Some(job_id) = affected_job {
                crate::orchestrator::pubsub::notify_job_update(&job_id);
            }
            crate::orchestrator::wake_up();
            if matches!(timer_cancel_policy, TimerCancelPolicy::OnStateChange)
                && let Some(handle) = registry::take(agent_id)
            {
                let _ = handle.cancel_tx.send(());
            }
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(agent_id, error = %err, "failed to mark agent IDLE after {reason}");
        }
    }

    if matches!(timer_cancel_policy, TimerCancelPolicy::Always)
        && let Some(handle) = registry::take(agent_id)
    {
        let _ = handle.cancel_tx.send(());
    }
}
