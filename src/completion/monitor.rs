use std::path::Path;
use std::time::{Duration, Instant};

use crate::completion::parser::LogParseResult;
use crate::completion::reader::{LogCursorMap, LogReadState, read_provider_log_tail_with_state};
use crate::db;
use crate::error::CcbdError;
use crate::runtime_observation::EvidenceSource;
use crate::runtime_observation::intake::{
    TranscriptCompletionObservation, WorkingObservation, observe_transcript_completion,
    observe_working,
};

pub const LOG_MONITOR_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub const MAX_LOG_MONITOR_WAIT: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogMonitorTickOutcome {
    pub activity_observed: bool,
    pub completed: bool,
    pub woke_orchestrator: bool,
    pub cursors: LogCursorMap,
    pub state: LogReadState,
}

pub async fn run_log_monitor_tick(
    db: db::Db,
    agent_id: &str,
    provider: &str,
    log_root: &Path,
    state: LogReadState,
    expected_lifecycle_id: &str,
) -> Result<LogMonitorTickOutcome, CcbdError> {
    let read = read_provider_log_tail_with_state(provider, log_root, &state)
        .map_err(|err| CcbdError::PtyIoError(format!("read provider log tail: {err}")))?;
    let updated_cursors = read.cursors.clone();
    let updated_state = read.state.clone();
    let completion_prompt_fingerprints = read.completion_prompt_fingerprints.clone();
    let mut activity_observed = false;

    for activity in read.activities {
        let (provider_turn_id, prompt_fingerprint) = match activity.parsed {
            LogParseResult::TurnStarted { turn_id } => (turn_id, None),
            LogParseResult::UserMessage {
                turn_id,
                prompt_fingerprint,
            } => (turn_id, prompt_fingerprint),
            _ => continue,
        };
        let raw_path = activity.raw_path.to_string_lossy().to_string();
        let outcome = observe_working(WorkingObservation {
            db: db.clone(),
            agent_id: agent_id.to_string(),
            provider: provider.to_string(),
            expected_lifecycle_id: expected_lifecycle_id.to_string(),
            source: EvidenceSource::Transcript,
            evidence_id: format!("log:{raw_path}:{}", activity.raw_offset),
            provider_turn_id,
            prompt_fingerprint,
        })
        .await?;
        activity_observed |= outcome.observation_inserted;
    }

    for completion in read.completions {
        let LogParseResult::TurnComplete { turn_id, reply } = completion.parsed else {
            continue;
        };
        let raw_path = completion.raw_path.to_string_lossy().to_string();
        let prompt_fingerprint = completion_prompt_fingerprints
            .get(&(completion.raw_path.clone(), completion.raw_offset))
            .cloned();
        match observe_transcript_completion(TranscriptCompletionObservation {
            db: db.clone(),
            agent_id: agent_id.to_string(),
            provider: provider.to_string(),
            reply,
            raw_path,
            raw_offset: completion.raw_offset,
            provider_turn_id: turn_id,
            expected_lifecycle_id: expected_lifecycle_id.to_string(),
            prompt_fingerprint,
        })
        .await
        {
            Ok((changes, affected_job)) if changes > 0 => {
                if let Some(job_id) = affected_job {
                    crate::orchestrator::pubsub::notify_job_update(&job_id);
                }
                crate::orchestrator::wake_up();
                return Ok(LogMonitorTickOutcome {
                    activity_observed,
                    completed: true,
                    woke_orchestrator: true,
                    cursors: updated_cursors,
                    state: updated_state,
                });
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(agent_id, provider, error = %err, "failed to mark agent IDLE from log event");
            }
        }
    }

    Ok(LogMonitorTickOutcome {
        activity_observed,
        completed: false,
        woke_orchestrator: false,
        cursors: updated_cursors,
        state: updated_state,
    })
}

fn log_monitor_observed_progress(before: &LogReadState, after: &LogReadState) -> bool {
    after
        .cursors
        .iter()
        .any(|(path, after_offset)| *after_offset > before.cursors.get(path).copied().unwrap_or(0))
}

pub fn spawn_log_monitor_task(
    db: db::Db,
    agent_id: String,
    provider: String,
    log_root: std::path::PathBuf,
    initial_state: LogReadState,
    expected_lifecycle_id: String,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut state = initial_state;
        let mut deadline = Instant::now() + MAX_LOG_MONITOR_WAIT;
        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                _ = tokio::time::sleep(LOG_MONITOR_POLL_INTERVAL) => {}
            }

            if Instant::now() >= deadline {
                let active_state = db::agents::query_agent_state(db.clone(), agent_id.clone())
                    .await
                    .ok()
                    .flatten()
                    .map(|(state, sub_state)| format!("{state}/{sub_state}"));
                let last_cursor = state
                    .cursors
                    .iter()
                    .next_back()
                    .map(|(path, offset)| format!("{}:{offset}", path.display()));
                tracing::warn!(
                    agent_id = %agent_id,
                    provider = %provider,
                    active_state = ?active_state,
                    last_cursor = ?last_cursor,
                    cursor_count = state.cursors.len(),
                    "log monitor reached max wait; enabling UI recapture before health STUCK fallback"
                );
                crate::completion::registry::cancel(&agent_id);
                break;
            }

            match db::agents::query_agent_state(db.clone(), agent_id.clone()).await {
                Ok(Some((state, _)))
                    if state == db::state_machine::STATE_WAITING_FOR_ACK
                        || state == db::state_machine::STATE_BUSY => {}
                Ok(_) => {
                    crate::completion::registry::cancel(&agent_id);
                    break;
                }
                Err(err) => {
                    tracing::warn!(agent_id = %agent_id, error = %err, "log monitor failed to query agent state");
                    break;
                }
            }

            match run_log_monitor_tick(
                db.clone(),
                &agent_id,
                &provider,
                &log_root,
                state.clone(),
                &expected_lifecycle_id,
            )
            .await
            {
                Ok(outcome) => {
                    let observed_progress = log_monitor_observed_progress(&state, &outcome.state);
                    state = outcome.state;
                    crate::completion::registry::update_state(&agent_id, state.clone());
                    if observed_progress {
                        deadline = Instant::now() + MAX_LOG_MONITOR_WAIT;
                    }
                    if outcome.completed {
                        crate::completion::registry::cancel(&agent_id);
                        break;
                    }
                }
                Err(err) => {
                    tracing::warn!(agent_id = %agent_id, provider = %provider, error = %err, "log monitor tick failed; keeping UI fallback active");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use crate::completion::reader::LogReadState;
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync, update_dispatched_seq_id_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::db::{self as db, Db};

    use super::{
        LOG_MONITOR_POLL_INTERVAL, MAX_LOG_MONITOR_WAIT, log_monitor_observed_progress,
        run_log_monitor_tick,
    };

    fn codex_complete(turn_id: &str, reply: &str) -> String {
        format!(
            r#"{{"type":"event_msg","payload":{{"type":"task_complete","turn_id":"{turn_id}","last_agent_message":"{reply}"}}}}"#
        )
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

    fn seed_busy_dispatched_job(db: &Db, agent_id: &str, job_id: &str) {
        seed_dispatched_job(db, agent_id, job_id, "BUSY");
    }

    fn seed_waiting_dispatched_job(db: &Db, agent_id: &str, job_id: &str) {
        seed_dispatched_job(db, agent_id, job_id, "WAITING_FOR_ACK");
    }

    fn seed_dispatched_job(db: &Db, agent_id: &str, job_id: &str, state: &str) {
        let conn = db.conn();
        insert_session_sync(&conn, "s_log", "p_log", "/tmp/log").unwrap();
        insert_agent_sync(&conn, agent_id, "s_log", "codex", state, Some(123)).unwrap();
        insert_job_sync(&conn, job_id, agent_id, None, "echo PONG\n").unwrap();
        conn.execute(
            "UPDATE jobs SET status='DISPATCHED', dispatched_at=unixepoch() WHERE id=?",
            [job_id],
        )
        .unwrap();
        let seq = insert_event_sync(
            &conn,
            agent_id,
            None,
            "command_received",
            r#"{"cmd":"echo PONG\n","status":"SENT"}"#,
        )
        .unwrap();
        update_dispatched_seq_id_sync(&conn, job_id, seq).unwrap();
    }

    fn seed_busy_dispatched_job_with_provider(
        db: &Db,
        agent_id: &str,
        job_id: &str,
        provider: &str,
    ) {
        seed_dispatched_job_with_provider(db, agent_id, job_id, provider, "BUSY");
    }

    fn seed_waiting_dispatched_job_with_provider(
        db: &Db,
        agent_id: &str,
        job_id: &str,
        provider: &str,
    ) {
        seed_dispatched_job_with_provider(db, agent_id, job_id, provider, "WAITING_FOR_ACK");
    }

    fn seed_dispatched_job_with_provider(
        db: &Db,
        agent_id: &str,
        job_id: &str,
        provider: &str,
        state: &str,
    ) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            "s_log_provider",
            "p_log_provider",
            "/tmp/log-provider",
        )
        .unwrap();
        insert_agent_sync(
            &conn,
            agent_id,
            "s_log_provider",
            provider,
            state,
            Some(123),
        )
        .unwrap();
        insert_job_sync(&conn, job_id, agent_id, None, "echo PONG\n").unwrap();
        conn.execute(
            "UPDATE jobs SET status='DISPATCHED', dispatched_at=unixepoch() WHERE id=?",
            [job_id],
        )
        .unwrap();
        let seq = insert_event_sync(
            &conn,
            agent_id,
            None,
            "command_received",
            r#"{"cmd":"echo PONG\n","status":"SENT"}"#,
        )
        .unwrap();
        update_dispatched_seq_id_sync(&conn, job_id, seq).unwrap();
    }

    fn write_antigravity_transcript(root: &std::path::Path, fixture: &str) -> std::path::PathBuf {
        let file = root.join("brain/conv-1/.system_generated/logs/transcript.jsonl");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, fixture).unwrap();
        file
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn monitor_wakes_orchestrator_and_notifies_job_update_on_complete() {
        let agent_id = "completion_monitor_job_update";
        let job_id = "job_completion_monitor_job_update";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_busy_dispatched_job(&db, agent_id, job_id);
        let root = tempfile::TempDir::new().unwrap();
        let log = root.path().join("rollout-session.jsonl");
        fs::write(&log, format!("{}\n", codex_complete("turn-1", "PONG"))).unwrap();
        let mut updates = crate::orchestrator::pubsub::subscribe_job_updates();

        let outcome = run_log_monitor_tick(
            db.clone(),
            agent_id,
            "codex",
            root.path(),
            LogReadState::default(),
            &lifecycle_id(&db, agent_id),
        )
        .await
        .unwrap();

        assert!(outcome.completed);
        assert!(outcome.woke_orchestrator);
        let job_update = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match updates.recv().await {
                    Ok(update_job_id) if update_job_id == job_id => break update_job_id,
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(err) => panic!("job update channel closed before {job_id} arrived: {err}"),
                }
            }
        })
        .await
        .expect("timed out waiting for job update");
        assert_eq!(job_update, job_id);
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(job.status, "COMPLETED");
        assert_eq!(job.reply_text.as_deref(), Some("PONG"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn codex_task_started_is_the_working_transition_not_pane_motion() {
        let agent_id = "codex_provider_started";
        let job_id = "job_codex_provider_started";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_waiting_dispatched_job(&db, agent_id, job_id);
        let root = tempfile::TempDir::new().unwrap();
        let log = root.path().join("rollout-session.jsonl");
        fs::write(
            &log,
            r#"{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-started"}}
"#,
        )
        .unwrap();

        let outcome = run_log_monitor_tick(
            db.clone(),
            agent_id,
            "codex",
            root.path(),
            LogReadState::default(),
            &lifecycle_id(&db, agent_id),
        )
        .await
        .unwrap();

        let (state, sub_state): (String, String) = db
            .conn()
            .query_row(
                "SELECT state, sub_state FROM agents WHERE id=?1",
                [agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let source: String = db
            .conn()
            .query_row(
                "SELECT json_extract(observation_json, '$.source') FROM provider_status_observations WHERE agent_id=?1 AND turn_id=?2 AND json_extract(observation_json, '$.kind.state')='working' ORDER BY observed_at_ms DESC LIMIT 1",
                rusqlite::params![agent_id, job_id],
                |row| row.get(0),
            )
            .unwrap();

        assert!(outcome.activity_observed);
        assert!(!outcome.completed);
        assert_eq!(state, "BUSY");
        assert_eq!(sub_state, "ProviderEvent");
        assert_eq!(source, "transcript");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mismatched_claude_user_message_cannot_claim_the_dispatched_job() {
        let agent_id = "claude_prompt_mismatch";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_waiting_dispatched_job_with_provider(
            &db,
            agent_id,
            "job_claude_prompt_mismatch",
            "claude",
        );
        let root = tempfile::TempDir::new().unwrap();
        let log = root.path().join("project/session.jsonl");
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(
            &log,
            r#"{"type":"user","message":{"role":"user","content":"different prompt"}}
{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"wrong completion"}],"stop_reason":"end_turn"}}
"#,
        )
        .unwrap();

        let outcome = run_log_monitor_tick(
            db.clone(),
            agent_id,
            "claude",
            root.path(),
            LogReadState::default(),
            &lifecycle_id(&db, agent_id),
        )
        .await
        .unwrap();
        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id=?1", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();

        assert!(!outcome.activity_observed);
        assert!(!outcome.completed);
        assert_eq!(state, "WAITING_FOR_ACK");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn monitor_marks_antigravity_idle_from_transcript_done_marker() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_busy_dispatched_job_with_provider(&db, "a_ag_log", "job_ag_log", "antigravity");
        db.conn()
            .execute(
                "UPDATE jobs SET prompt_text='Summarize the current state' WHERE id='job_ag_log'",
                [],
            )
            .unwrap();
        let root = tempfile::TempDir::new().unwrap();
        write_antigravity_transcript(
            root.path(),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antigravity_log/final_reply.jsonl"
            )),
        );

        let outcome = run_log_monitor_tick(
            db.clone(),
            "a_ag_log",
            "antigravity",
            root.path(),
            LogReadState::default(),
            &lifecycle_id(&db, "a_ag_log"),
        )
        .await
        .unwrap();
        let (state, sub_state): (String, String) = db
            .conn()
            .query_row(
                "SELECT state, sub_state FROM agents WHERE id = 'a_ag_log'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let job = query_job_sync(&db.conn(), "job_ag_log").unwrap().unwrap();

        assert!(outcome.completed);
        assert_eq!(state, "IDLE");
        assert_eq!(sub_state, "LogEvent");
        assert_eq!(job.status, "COMPLETED");
        assert_eq!(
            job.reply_text.as_deref(),
            Some("The requested summary is complete.")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pull_fallback_completes_when_hook_push_never_transitions() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_busy_dispatched_job(
            &db,
            "a_hook_failed_pull_fallback",
            "job_hook_failed_pull_fallback",
        );
        let root = tempfile::TempDir::new().unwrap();
        let log = root.path().join("rollout-session.jsonl");
        fs::write(
            &log,
            format!("{}\n", codex_complete("turn-fallback", "PULL PONG")),
        )
        .unwrap();

        let outcome = run_log_monitor_tick(
            db.clone(),
            "a_hook_failed_pull_fallback",
            "codex",
            root.path(),
            LogReadState::default(),
            &lifecycle_id(&db, "a_hook_failed_pull_fallback"),
        )
        .await
        .unwrap();
        let job = query_job_sync(&db.conn(), "job_hook_failed_pull_fallback")
            .unwrap()
            .unwrap();
        let state: String = db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id='a_hook_failed_pull_fallback'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(outcome.completed);
        assert!(outcome.woke_orchestrator);
        assert_eq!(state, "IDLE");
        assert_eq!(job.status, "COMPLETED");
        assert_eq!(job.reply_text.as_deref(), Some("PULL PONG"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_error_keeps_ui_fallback_enabled() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = db::init(file.path()).unwrap();
        seed_busy_dispatched_job(&db, "a_schema", "job_schema");
        let root = tempfile::TempDir::new().unwrap();
        let log = root.path().join("project/session.jsonl");
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(
            &log,
            r#"{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"PONG"}],"stop_reason":"schema_drift"}}"#,
        )
        .unwrap();

        let outcome = run_log_monitor_tick(
            db.clone(),
            "a_schema",
            "claude",
            root.path(),
            LogReadState::default(),
            &lifecycle_id(&db, "a_schema"),
        )
        .await
        .unwrap();

        assert!(!outcome.completed);
        assert!(!outcome.woke_orchestrator);
        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id='a_schema'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state, "BUSY");
    }

    #[test]
    fn log_monitor_wait_is_decoupled_from_stuck_threshold() {
        assert!(LOG_MONITOR_POLL_INTERVAL < Duration::from_secs(1));
        assert!(MAX_LOG_MONITOR_WAIT > crate::pane_diff::DEFAULT_STUCK_THRESHOLD);
    }

    #[test]
    fn log_monitor_progress_renews_when_cursor_advances() {
        let log = std::path::PathBuf::from("/tmp/rollout-progress.jsonl");
        let before = LogReadState::from_cursors([(log.clone(), 10)].into());
        let after = LogReadState::from_cursors([(log, 42)].into());

        assert!(log_monitor_observed_progress(&before, &after));
    }

    #[test]
    fn log_monitor_progress_does_not_renew_when_cursor_is_unchanged() {
        let log = std::path::PathBuf::from("/tmp/rollout-progress.jsonl");
        let before = LogReadState::from_cursors([(log.clone(), 42)].into());
        let after = LogReadState::from_cursors([(log, 42)].into());

        assert!(!log_monitor_observed_progress(&before, &after));
    }
}
