//! Marker timeout task for startup UNKNOWN and long-running BUSY STUCK fallback.

use crate::db::{self as db, Db};
use crate::marker::MarkerMatcher;
use crate::marker::parser_registry::ParserHandle;
use crate::tmux::TmuxServer;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
// mvp13 Stage 3E: PaneDiffWatcher is the primary BUSY hang detector. This wide
// deadline is only a final fallback when pane diff detection fails entirely.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(10_800);

/// Timer mode for startup readiness or command completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerKind {
    Startup,
    Busy,
}

/// Channels used by readers and terminal paths to reset or cancel a marker timer.
pub struct MarkerTimerHandle {
    pub reset_tx: watch::Sender<Instant>,
    pub cancel_tx: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct PromptTimerScanContext {
    pub provider: String,
    pub state_dir: PathBuf,
    pub tmux: Arc<TmuxServer>,
    pub matcher: Arc<MarkerMatcher>,
}

/// Spawn a marker timeout task with the default MVP3 timeout.
pub fn spawn_marker_timer_task(
    agent_id: String,
    kind: TimerKind,
    db: Arc<Db>,
    parser_handle: ParserHandle,
) -> MarkerTimerHandle {
    spawn_marker_timer_task_with_prompt(agent_id, kind, db, parser_handle, None)
}

pub fn spawn_marker_timer_task_with_prompt(
    agent_id: String,
    kind: TimerKind,
    db: Arc<Db>,
    parser_handle: ParserHandle,
    prompt_ctx: Option<PromptTimerScanContext>,
) -> MarkerTimerHandle {
    let timeout = match kind {
        TimerKind::Startup => STARTUP_TIMEOUT,
        TimerKind::Busy => BUSY_TIMEOUT,
    };
    spawn_marker_timer_task_with_timeout(agent_id, kind, timeout, db, parser_handle, prompt_ctx)
}

fn spawn_marker_timer_task_with_timeout(
    agent_id: String,
    kind: TimerKind,
    timeout: Duration,
    db: Arc<Db>,
    parser_handle: ParserHandle,
    prompt_ctx: Option<PromptTimerScanContext>,
) -> MarkerTimerHandle {
    let (reset_tx, mut reset_rx) = watch::channel(Instant::now());
    let (cancel_tx, mut cancel_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut prompt_reset_at: Option<Instant> = None;
        loop {
            let last_reset = prompt_reset_at
                .unwrap_or_else(|| *reset_rx.borrow_and_update())
                .max(*reset_rx.borrow_and_update());
            let deadline = last_reset + timeout;
            let sleep_for = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = &mut cancel_rx => return,
                _ = tokio::time::sleep(sleep_for) => {}
                changed = reset_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue;
                }
            }

            match kind {
                TimerKind::Busy => {
                    if let Some(prompt_ctx) = prompt_ctx.clone() {
                        match scan_prompt_before_busy_timeout(
                            db.as_ref().clone(),
                            &agent_id,
                            prompt_ctx,
                        )
                        .await
                        {
                            BusyTimeoutPromptScan::ResetTimer => {
                                prompt_reset_at = Some(Instant::now());
                                continue;
                            }
                            BusyTimeoutPromptScan::StopTimer => return,
                            BusyTimeoutPromptScan::ContinueStuck => {}
                        }
                    }

                    // mvp13 Stage 3E: BUSY timeout means business-level hang, not
                    // system-level UNKNOWN. The normal path should be PaneDiffWatcher;
                    // this 3h timer is only the last fallback.
                    match db::state_machine::mark_agent_stuck(
                        db.as_ref().clone(),
                        agent_id.clone(),
                        "BUSY_MARKER_TIMEOUT_STUCK".to_string(),
                    )
                    .await
                    {
                        Ok(changes) if changes > 0 => {
                            tracing::info!(agent_id = %agent_id, "BUSY timeout marked agent STUCK");
                            crate::orchestrator::wake_up();
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent STUCK after BUSY marker timeout");
                        }
                    }
                }
                TimerKind::Startup => {
                    let pane_bytes = match parser_handle.lock() {
                        Ok(parser) => parser.screen().contents().into_bytes(),
                        Err(err) => {
                            tracing::warn!(agent_id = %agent_id, error = %err, "parser mutex poisoned during marker timeout snapshot");
                            Vec::new()
                        }
                    };
                    let failed_rules = serde_json::json!(["[\\$#>✦]\\s*$"]);
                    match db::state_machine::mark_agent_unknown(
                        db.as_ref().clone(),
                        agent_id.clone(),
                        "STARTUP_MARKER_TIMEOUT".to_string(),
                        pane_bytes,
                        failed_rules,
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
                            tracing::warn!(agent_id = %agent_id, error = %err, "failed to mark agent UNKNOWN after STARTUP marker timeout");
                        }
                    }
                }
            }
            return;
        }
    });

    MarkerTimerHandle {
        reset_tx,
        cancel_tx,
    }
}

enum BusyTimeoutPromptScan {
    ResetTimer,
    StopTimer,
    ContinueStuck,
}

async fn scan_prompt_before_busy_timeout(
    db: Db,
    agent_id: &str,
    prompt_ctx: PromptTimerScanContext,
) -> BusyTimeoutPromptScan {
    if !crate::prompt_handler::integration::is_prompt_handling_provider(&prompt_ctx.provider) {
        tracing::info!(
            agent_id,
            provider = %prompt_ctx.provider,
            "prompt scan skipped before BUSY timeout for non-interactive-prompt provider"
        );
        return BusyTimeoutPromptScan::ContinueStuck;
    }

    let Some(pane_id) = crate::agent_io::pane_id(agent_id) else {
        tracing::warn!(
            agent_id,
            impact = "BUSY timeout falls back to STUCK because pane is not registered",
            "prompt scan skipped before BUSY timeout"
        );
        return BusyTimeoutPromptScan::ContinueStuck;
    };

    tracing::info!(agent_id, "prompt scan start before BUSY timeout");
    match crate::prompt_handler::integration::scan_prompt_and_apply_outcome(
        crate::prompt_handler::integration::PromptScanRequest {
            db,
            agent_id: agent_id.to_string(),
            provider: prompt_ctx.provider,
            pane_id,
            tmux: prompt_ctx.tmux,
            state_dir: prompt_ctx.state_dir,
            marker_matcher: prompt_ctx.matcher,
            max_depth: 3,
            scan_purpose: crate::prompt_handler::PromptScanPurpose::Direct,
        },
    )
    .await
    {
        Ok(crate::prompt_handler::integration::PromptScanDisposition::Handled { depth }) => {
            tracing::info!(
                agent_id,
                depth,
                "prompt scan auto-handled prompt before BUSY timeout; resetting timer"
            );
            BusyTimeoutPromptScan::ResetTimer
        }
        Ok(crate::prompt_handler::integration::PromptScanDisposition::Pending {
            depth,
            block_reason,
        }) => {
            tracing::info!(
                agent_id,
                depth,
                block_reason,
                "prompt scan moved agent to PROMPT_PENDING before BUSY timeout"
            );
            crate::orchestrator::wake_up();
            BusyTimeoutPromptScan::StopTimer
        }
        Ok(crate::prompt_handler::integration::PromptScanDisposition::Deferred {
            depth,
            block_reason,
        }) => {
            tracing::info!(
                agent_id,
                depth,
                block_reason,
                "prompt scan deferred before BUSY timeout; continuing STUCK fallback"
            );
            BusyTimeoutPromptScan::ContinueStuck
        }
        Ok(crate::prompt_handler::integration::PromptScanDisposition::NoActionNeeded {
            ..
        }) => {
            tracing::info!(
                agent_id,
                "prompt scan found no prompt before BUSY timeout; continuing STUCK fallback"
            );
            BusyTimeoutPromptScan::ContinueStuck
        }
        Err(err) => {
            tracing::warn!(
                agent_id,
                reason = %err,
                impact = "prompt scan failed; preserving BUSY timeout STUCK fallback",
                "prompt scan failed before BUSY timeout"
            );
            BusyTimeoutPromptScan::ContinueStuck
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BUSY_TIMEOUT, TimerKind, spawn_marker_timer_task_with_timeout};
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::jobs::{claim_next_job_sync, insert_job_sync, update_dispatched_seq_id_sync};
    use crate::db::sessions::insert_session_sync;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    async fn sleep_ms(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn test_db_with_agent(state: &str) -> db::Db {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, "a1", "s1", "bash", state, Some(1)).unwrap();
        }
        db
    }

    /// Like `test_db_with_agent("BUSY")` but also seeds a DISPATCHED job for the
    /// agent, so the STUCK path writes `jobs.error_reason` — the column the
    /// operator observed carrying the wrong `PANE_DIFF_STUCK` value. Mirrors the
    /// dispatched-job seeding used in `db::state_machine` tests.
    fn test_db_with_busy_dispatched_job(agent_id: &str, job_id: &str) -> db::Db {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/foo").unwrap();
            insert_agent_sync(&conn, agent_id, "s1", "bash", "IDLE", Some(1)).unwrap();
            insert_job_sync(&conn, job_id, agent_id, None, "echo PONG\n").unwrap();
        }
        // Claim moves the job to DISPATCHED; then force the agent to BUSY so the
        // 3h marker-timeout fallback has a live dispatched job to resolve.
        claim_next_job_sync(&db, agent_id).unwrap().unwrap();
        {
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
                "UPDATE agents SET state = 'BUSY', state_version = state_version + 1 WHERE id = ?",
                [agent_id],
            )
            .unwrap();
        }
        db
    }

    /// Module A / A1' contract — RED under current code.
    ///
    /// The 3h BUSY marker timeout (`timer.rs` `TimerKind::Busy`, the
    /// `mark_agent_stuck` call at ~line 113) is a *business-level* hang fallback —
    /// its own comment says so: "BUSY timeout means business-level hang, not
    /// system-level UNKNOWN ... this 3h timer is only the last fallback." It never
    /// reads pane-diff content, so the STUCK it records must NOT be attributed to
    /// pane-diff. Every persisted audit sink — the `jobs.error_reason` column and
    /// the `state_change` event payload `reason` — must read
    /// `BUSY_MARKER_TIMEOUT_STUCK`, never the pane-diff string `PANE_DIFF_STUCK`.
    ///
    /// Naming (a4's call per work order): the reason string is an audit label that
    /// must name the *detector* that fired. `PANE_DIFF_STUCK` asserts the pane-diff
    /// watcher saw a frozen pane; the marker timeout saw no such thing — it only
    /// elapsed. `BUSY_MARKER_TIMEOUT_STUCK` mirrors its sibling on the startup path
    /// (`STARTUP_MARKER_TIMEOUT`, timer.rs:138) and keeps the `_STUCK` suffix of the
    /// stuck-reason family (`PANE_DIFF_STUCK` / `HEALTH_CHECK_STUCK`). Mislabeling
    /// here is exactly the live bug operator hit: an agent mis-marked STUCK by a
    /// non-pane-diff detector, yet `error_reason == PANE_DIFF_STUCK`.
    ///
    /// Liveness regression face (GREEN before and after): the STUCK *transition*
    /// itself must still fire. The fix corrects the reason string only — it must
    /// not delete the STUCK path; a genuine business hang must still escalate.
    ///
    /// RED under current code: `mark_agent_stuck_outcome_sync` hardcodes
    /// `"PANE_DIFF_STUCK"` for every caller (state_machine.rs:1483/1498/1508/1522),
    /// with no discrimination of the timer caller. Rolling back a1's reason fix
    /// returns this test to red. Anchored on falling-through DB rows (jobs column +
    /// event payload), not on any internal API shape — a1 is free to thread the
    /// reason however (new param / dedicated fn) so long as these sinks read right.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_busy_marker_timeout_stuck_reason_is_not_pane_diff() {
        let db = test_db_with_busy_dispatched_job("a1", "job_busy_marker_timeout");
        let _handle = spawn_marker_timer_task_with_timeout(
            "a1".into(),
            TimerKind::Busy,
            Duration::from_millis(20),
            Arc::new(db.clone()),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
            None,
        );

        sleep_ms(150).await;

        // Liveness (regression guard): a genuine BUSY hang must still escalate to
        // STUCK — the reason fix must not delete the transition.
        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            state, "STUCK",
            "BUSY marker timeout must still drive the STUCK transition (liveness preserved)"
        );

        // Contract sink #1 — the persisted jobs.error_reason column (the column the
        // operator observed carrying the wrong value).
        let error_reason: Option<String> = db
            .conn()
            .query_row(
                "SELECT error_reason FROM jobs WHERE id = 'job_busy_marker_timeout'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_ne!(
            error_reason.as_deref(),
            Some("PANE_DIFF_STUCK"),
            "BUSY marker timeout is not pane-diff triggered; jobs.error_reason must not be PANE_DIFF_STUCK"
        );
        assert_eq!(
            error_reason.as_deref(),
            Some("BUSY_MARKER_TIMEOUT_STUCK"),
            "jobs.error_reason must name the marker-timeout fallback detector"
        );

        // Contract sink #2 — the STUCK state_change event payload reason.
        let payload: String = db
            .conn()
            .query_row(
                "SELECT payload FROM events WHERE agent_id = 'a1' \
                 AND event_type = 'state_change' AND payload LIKE '%\"to\":\"STUCK\"%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["to"], "STUCK");
        assert_ne!(
            payload["reason"], "PANE_DIFF_STUCK",
            "state_change audit reason must not mislabel a marker timeout as pane-diff"
        );
        assert_eq!(
            payload["reason"], "BUSY_MARKER_TIMEOUT_STUCK",
            "state_change audit reason must name the marker-timeout fallback detector"
        );
    }

    #[test]
    fn test_busy_timeout_default_is_three_hours() {
        assert_eq!(BUSY_TIMEOUT, Duration::from_secs(10_800));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_busy_timeout_marks_stuck() {
        let db = test_db_with_agent("BUSY");
        let _handle = spawn_marker_timer_task_with_timeout(
            "a1".into(),
            TimerKind::Busy,
            Duration::from_millis(20),
            Arc::new(db.clone()),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
            None,
        );

        sleep_ms(80).await;

        let (state, error_code): (String, Option<String>) = db
            .conn()
            .query_row(
                "SELECT state, error_code FROM agents WHERE id = 'a1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "STUCK");
        assert_eq!(error_code, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_timer_cancel_prevents_unknown() {
        let db = test_db_with_agent("BUSY");
        let handle = spawn_marker_timer_task_with_timeout(
            "a1".into(),
            TimerKind::Busy,
            Duration::from_millis(20),
            Arc::new(db.clone()),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
            None,
        );
        let _ = handle.cancel_tx.send(());

        sleep_ms(80).await;

        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "BUSY");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_timer_reset_extends_deadline() {
        let db = test_db_with_agent("BUSY");
        let handle = spawn_marker_timer_task_with_timeout(
            "a1".into(),
            TimerKind::Busy,
            Duration::from_millis(80),
            Arc::new(db.clone()),
            Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0))),
            None,
        );

        sleep_ms(50).await;
        handle.reset_tx.send(Instant::now()).unwrap();
        sleep_ms(50).await;
        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "BUSY");

        sleep_ms(80).await;
        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "STUCK");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_startup_timeout_still_marks_unknown_and_writes_evidence_snapshot() {
        let db = test_db_with_agent("SPAWNING");
        let parser = Arc::new(Mutex::new(vt100::Parser::new(200, 200, 0)));
        parser.lock().unwrap().process(b"no prompt yet\n");
        let _handle = spawn_marker_timer_task_with_timeout(
            "a1".into(),
            TimerKind::Startup,
            Duration::from_millis(20),
            Arc::new(db.clone()),
            parser,
            None,
        );

        sleep_ms(80).await;

        let (state, error_code, pane_bytes, failed_rules): (
            String,
            Option<String>,
            Vec<u8>,
            String,
        ) = db
            .conn()
            .query_row(
                "SELECT agents.state, agents.error_code, evidence.pane_bytes, evidence.failed_rules \
                 FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id = 'a1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, "UNKNOWN");
        assert_eq!(error_code.as_deref(), Some("STARTUP_MARKER_TIMEOUT"));
        assert!(!pane_bytes.is_empty());
        assert!(failed_rules.contains("[\\\\$#>✦]\\\\s*$"));
    }
}
