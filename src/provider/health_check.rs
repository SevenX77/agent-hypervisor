use crate::db::state_machine::{STATE_BUSY, STATE_SPAWNING, STATE_WAITING_FOR_ACK};
use crate::error::CcbdError;
use crate::provider::manifest::{CompletionSignalKind, get_manifest};
use crate::rpc::Ctx;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// STARVATION ALERT THRESHOLD FOR QUEUED JOBS
/// 300 seconds (5 minutes) is a reasonable time to wait for a job to start,
/// considering agent spawning or other tasks should finish within this window.
pub const QUEUED_STARVATION_THRESHOLD_SECS: i64 = 300;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheckResult {
    pub alive: bool,
    pub dead_layers: Vec<String>,
    pub last_progress_ts: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheckObservation {
    pub agent_id: String,
    pub provider: String,
    pub state: String,
    pub pane_capture: String,
    pub pane_capture_ok: bool,
    pub last_output_ts: Option<i64>,
    pub last_marker_ts: Option<i64>,
    /// Unix-seconds dispatch time of the agent's current DISPATCHED job, if any.
    /// The completion-staleness window is floored to this so an agent that sat
    /// idle for a long time before being re-dispatched is judged only on
    /// stagnation *since the current task started* — not on pre-dispatch idle.
    pub dispatched_at: Option<i64>,
    pub now_ts: i64,
}

pub fn health_check_observe(
    observation: &HealthCheckObservation,
    stuck_threshold_secs: i64,
) -> HealthCheckResult {
    // Floor the progress baseline to the current job's dispatch time. Without this,
    // an agent that sat idle for a long time and is then freshly re-dispatched would
    // be judged "completion layer dead" the instant it goes BUSY — because its last
    // output/marker predates the new task by more than the threshold (G0 false-STUCK).
    // The staleness window must only count stagnation *since the current task started*.
    let last_progress_ts = observation
        .last_marker_ts
        .unwrap_or(0)
        .max(observation.last_output_ts.unwrap_or(0))
        .max(observation.dispatched_at.unwrap_or(0));
    let mut dead_layers = Vec::new();

    if !observation.pane_capture_ok {
        dead_layers.push("tmux".to_string());
    }

    if observation.state == STATE_SPAWNING {
        let probe = get_manifest(&observation.provider).init_probe.build();
        if !probe.detect(&observation.pane_capture) {
            dead_layers.push("predicate".to_string());
        }
    }

    if is_working_state(&observation.state)
        && last_progress_ts > 0
        && observation.now_ts.saturating_sub(last_progress_ts) > stuck_threshold_secs
    {
        dead_layers.push("completion".to_string());
    }

    HealthCheckResult {
        alive: dead_layers.is_empty(),
        dead_layers,
        last_progress_ts,
    }
}

pub async fn escalate_health_stuck(
    ctx: &Ctx,
    observation: &HealthCheckObservation,
    stuck_threshold_secs: i64,
) -> Result<usize, CcbdError> {
    let result = health_check_observe(observation, stuck_threshold_secs);
    if result.alive || !is_active_state(&observation.state) {
        return Ok(0);
    }

    if result.dead_layers.iter().any(|layer| layer == "completion")
        && observation.pane_capture_ok
        && get_manifest(&observation.provider).completion_signal == CompletionSignalKind::LogOnly
    {
        let job_id = crate::db::jobs::query_dispatched_job_for_agent(
            ctx.db.clone(),
            observation.agent_id.clone(),
        )
        .await?
        .map(|job| job.id);

        let payload = json!({
            "from": observation.state,
            "to": observation.state,
            "reason": "STOPPED_UNDECLARED_ALERT",
            "job_id": job_id,
            "elapsed_secs": observation.now_ts.saturating_sub(result.last_progress_ts),
        });

        let _ = crate::db::events::insert_event(
            ctx.db.clone(),
            observation.agent_id.clone(),
            None,
            "state_change".into(),
            payload.to_string(),
        )
        .await?;

        crate::orchestrator::pubsub::notify_event(crate::orchestrator::pubsub::EventFrame {
            event_id: 0,
            kind: "alert".to_string(),
            agent_id: observation.agent_id.clone(),
            job_id: job_id.clone(),
            state: Some(observation.state.clone()),
            ts_unix_micro: now_unix_micro(),
            payload: Some(json!({
                "job_id": job_id,
                "elapsed_secs": observation.now_ts.saturating_sub(result.last_progress_ts),
            })),
        });

        return Ok(0);
    }

    let changes = crate::db::state_machine::mark_agent_stuck(
        ctx.db.clone(),
        observation.agent_id.clone(),
        "HEALTH_CHECK_STUCK".to_string(),
    )
    .await?;
    if changes == 0 {
        return Ok(0);
    }
    tracing::info!(
        agent_id = %observation.agent_id,
        provider = %observation.provider,
        from = %observation.state,
        dead_layers = ?result.dead_layers,
        elapsed_secs = observation.now_ts.saturating_sub(result.last_progress_ts),
        dispatched_at = observation.dispatched_at,
        last_output_ts = observation.last_output_ts,
        last_marker_ts = observation.last_marker_ts,
        "health check marked agent STUCK"
    );

    let job_id = crate::db::jobs::query_dispatched_job_for_agent(
        ctx.db.clone(),
        observation.agent_id.clone(),
    )
    .await?
    .map(|job| job.id);
    let signal_kinds: Vec<String> = result
        .dead_layers
        .iter()
        .map(|layer| format!("health:{layer}"))
        .collect();
    let payload = json!({
        "from": observation.state,
        "to": "STUCK",
        "reason": "HEALTH_CHECK_STUCK",
        "job_id": job_id,
        "signal_kinds": signal_kinds,
        "elapsed_secs": observation.now_ts.saturating_sub(result.last_progress_ts),
    });
    let _ = crate::db::events::insert_event(
        ctx.db.clone(),
        observation.agent_id.clone(),
        None,
        "state_change".into(),
        payload.to_string(),
    )
    .await?;
    crate::orchestrator::pubsub::notify_event(crate::orchestrator::pubsub::EventFrame {
        event_id: 0,
        kind: "stuck".to_string(),
        agent_id: observation.agent_id.clone(),
        job_id: job_id.clone(),
        state: Some("STUCK".to_string()),
        ts_unix_micro: now_unix_micro(),
        payload: Some(json!({
            "job_id": job_id,
            "signal_kinds": signal_kinds,
            "elapsed_secs": observation.now_ts.saturating_sub(result.last_progress_ts),
        })),
    });
    Ok(changes)
}

pub async fn health_check_watcher_loop(ctx: Ctx, interval: Duration, stuck_threshold: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(err) = health_check_watcher_tick(&ctx, stuck_threshold).await {
            tracing::warn!(error = %err, "health check watcher tick failed");
        }
    }
}

async fn health_check_watcher_tick(ctx: &Ctx, stuck_threshold: Duration) -> Result<(), CcbdError> {
    for state in [STATE_SPAWNING, STATE_WAITING_FOR_ACK, STATE_BUSY] {
        let agents =
            crate::db::agents::query_agents_by_state(ctx.db.clone(), state.to_string()).await?;
        for agent in agents {
            let (pane_capture_ok, pane_capture) =
                if let Some(pane_id) = crate::agent_io::pane_id(&agent.id) {
                    match ctx.tmux_server.capture_pane(pane_id).await {
                        Ok(capture) => (true, capture),
                        Err(_) => (false, String::new()),
                    }
                } else {
                    (false, String::new())
                };
            let (last_output_ts, last_marker_ts) =
                query_last_progress(ctx.db.clone(), agent.id.clone()).await?;
            let dispatched_at =
                crate::db::jobs::query_dispatched_job_for_agent(ctx.db.clone(), agent.id.clone())
                    .await?
                    .and_then(|job| job.dispatched_at);
            let observation = HealthCheckObservation {
                agent_id: agent.id,
                provider: agent.provider,
                state: agent.state,
                pane_capture,
                pane_capture_ok,
                last_output_ts,
                last_marker_ts,
                dispatched_at,
                now_ts: now_unix_secs(),
            };
            let _ =
                escalate_health_stuck(ctx, &observation, stuck_threshold.as_secs() as i64).await?;
        }
    }

    // Check for queued job starvation
    let starved_jobs = crate::db::jobs::query_starved_queued_jobs(
        ctx.db.clone(),
        QUEUED_STARVATION_THRESHOLD_SECS,
    )
    .await?;
    for job in starved_jobs {
        let agent_id = job.agent_id.clone();
        let job_id = job.id.clone();

        // deduplicate: only alert once per job
        if !crate::db::events::has_queued_starvation_alerted(
            ctx.db.clone(),
            agent_id.clone(),
            job_id.clone(),
        )
        .await?
        {
            let elapsed_secs = now_unix_secs().saturating_sub(job.created_at);
            tracing::warn!(
                agent_id = %agent_id,
                job_id = %job_id,
                elapsed_secs = elapsed_secs,
                "QUEUED job starvation detected"
            );

            let payload = json!({
                "from": "QUEUED",
                "to": "QUEUED",
                "reason": "QUEUED_STARVATION_ALERT",
                "job_id": Some(job_id.clone()),
                "elapsed_secs": elapsed_secs,
            });

            let _ = crate::db::events::insert_event(
                ctx.db.clone(),
                agent_id.clone(),
                None,
                "state_change".into(),
                payload.to_string(),
            )
            .await?;

            crate::orchestrator::pubsub::notify_event(crate::orchestrator::pubsub::EventFrame {
                event_id: 0,
                kind: "alert".to_string(),
                agent_id: agent_id.clone(),
                job_id: Some(job_id.clone()),
                state: Some("QUEUED".to_string()),
                ts_unix_micro: now_unix_micro(),
                payload: Some(json!({
                    "job_id": job_id,
                    "elapsed_secs": elapsed_secs,
                })),
            });
        }
    }

    Ok(())
}

async fn query_last_progress(
    db: crate::db::Db,
    agent_id: String,
) -> Result<(Option<i64>, Option<i64>), CcbdError> {
    let last_output = crate::db::events::query_last_event_of_type(
        db.clone(),
        agent_id.clone(),
        "output_chunk".to_string(),
    )
    .await?
    .map(|event| event.created_at);

    let marker_like = format!("%{}%", crate::db::events::AH_IDLE_MARKER_PREFIX);
    let mut before_seq_id = None;
    let last_marker = loop {
        let Some(event) = crate::db::events::query_last_event_of_type_matching_payload(
            db.clone(),
            agent_id.clone(),
            "output_chunk".to_string(),
            marker_like.clone(),
            before_seq_id,
        )
        .await?
        else {
            break None;
        };
        before_seq_id = Some(event.seq_id);
        let payload: Value = serde_json::from_str(&event.payload).unwrap_or_else(|_| json!({}));
        if payload
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains(crate::db::events::AH_IDLE_MARKER_PREFIX))
        {
            break Some(event.created_at);
        }
    };
    Ok((last_output, last_marker))
}

fn is_active_state(state: &str) -> bool {
    matches!(state, STATE_SPAWNING | STATE_WAITING_FOR_ACK | STATE_BUSY)
}

fn is_working_state(state: &str) -> bool {
    matches!(state, STATE_WAITING_FOR_ACK | STATE_BUSY)
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn now_unix_micro() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::init;
    use crate::db::jobs::{insert_job_sync, query_job_sync, update_dispatched_seq_id_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::rpc::Ctx;
    use crate::sandbox::EnvState;
    use crate::tmux::TmuxServer;
    use rusqlite::params;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    fn observation() -> HealthCheckObservation {
        HealthCheckObservation {
            agent_id: "dogfood_a1".into(),
            provider: "bash".into(),
            state: "BUSY".into(),
            pane_capture: "$ ".into(),
            pane_capture_ok: true,
            last_output_ts: Some(100),
            last_marker_ts: Some(100),
            dispatched_at: None,
            now_ts: 105,
        }
    }

    #[test]
    fn test_health_check_three_layers_alive() {
        let result = health_check_observe(&observation(), 300);
        assert!(result.alive, "all layers ok should be alive");
        assert!(result.dead_layers.is_empty());
    }

    #[test]
    fn test_health_check_pane_dead() {
        let mut obs = observation();
        obs.pane_capture_ok = false;
        let result = health_check_observe(&obs, 300);
        assert_eq!(result.dead_layers, ["tmux"]);
    }

    #[test]
    fn test_health_check_predicate_dead() {
        let mut obs = observation();
        obs.state = "SPAWNING".into();
        obs.pane_capture = "Welcome to Claude Code\nSetup Wizard".into();
        obs.provider = "claude".into();
        let result = health_check_observe(&obs, 300);
        assert_eq!(result.dead_layers, ["predicate"]);
    }

    #[test]
    fn test_health_check_completion_stale() {
        let mut obs = observation();
        obs.last_output_ts = Some(1);
        obs.last_marker_ts = Some(1);
        obs.now_ts = 400;
        let result = health_check_observe(&obs, 300);
        assert_eq!(result.dead_layers, ["completion"]);
    }

    #[test]
    fn test_health_check_does_not_false_stuck_on_recent_redispatch_despite_stale_progress() {
        // G0 regression: a LogOnly agent that sat idle for a long time (very old
        // last_output/last_marker) and was then freshly re-dispatched must NOT be
        // marked completion-layer dead. The staleness window is floored to the
        // current job's dispatch time, so only post-dispatch stagnation counts.
        let mut obs = observation();
        obs.provider = "codex".into();
        obs.last_output_ts = Some(1); // last progress ages ago (pre-dispatch idle)
        obs.last_marker_ts = Some(1);
        obs.dispatched_at = Some(390); // but the current job was dispatched just now
        obs.now_ts = 400; // only 10s since dispatch — well under threshold
        let result = health_check_observe(&obs, 300);
        assert!(
            result.dead_layers.is_empty(),
            "freshly re-dispatched agent must not be STUCK from pre-dispatch idle (G0); got {:?}",
            result.dead_layers
        );
    }

    #[test]
    fn health_check_progress_uses_newest_of_marker_and_output() {
        let mut obs = observation();
        obs.provider = "codex".into();
        obs.last_marker_ts = Some(10);
        obs.last_output_ts = Some(395);
        obs.dispatched_at = Some(20);
        obs.now_ts = 400;

        let result = health_check_observe(&obs, 300);

        assert_eq!(result.last_progress_ts, 395);
        assert!(
            result.dead_layers.is_empty(),
            "fresh pane output must keep the completion layer alive even when an older marker exists; got {:?}",
            result.dead_layers
        );
    }

    #[test]
    fn health_check_fresh_dispatch_still_floors_to_dispatched_at() {
        let mut obs = observation();
        obs.provider = "codex".into();
        obs.last_marker_ts = Some(10);
        obs.last_output_ts = Some(20);
        obs.dispatched_at = Some(395);
        obs.now_ts = 400;

        let result = health_check_observe(&obs, 300);

        assert_eq!(result.last_progress_ts, 395);
        assert!(
            result.dead_layers.is_empty(),
            "dispatch time must remain the lower bound for progress freshness; got {:?}",
            result.dead_layers
        );
    }

    #[test]
    fn test_health_check_completion_stale_when_dispatched_long_ago_without_progress() {
        // Genuine stuck must still fire: dispatched long ago AND no progress since.
        let mut obs = observation();
        obs.provider = "codex".into();
        obs.last_output_ts = Some(1);
        obs.last_marker_ts = Some(1);
        obs.dispatched_at = Some(1); // dispatched long ago too
        obs.now_ts = 400;
        let result = health_check_observe(&obs, 300);
        assert_eq!(
            result.dead_layers,
            ["completion"],
            "genuine post-dispatch stagnation must still mark completion layer dead"
        );
    }

    fn seed_agent(conn: &rusqlite::Connection, agent_id: &str) {
        insert_agent_sync(conn, agent_id, "s1", "bash", "BUSY", Some(123)).unwrap();
    }

    fn insert_output_at(conn: &rusqlite::Connection, agent_id: &str, text: &str, ts: i64) -> i64 {
        let seq_id = insert_event_sync(
            conn,
            agent_id,
            None,
            "output_chunk",
            &serde_json::json!({ "text": text }).to_string(),
        )
        .unwrap();
        conn.execute(
            "UPDATE events SET created_at = ? WHERE seq_id = ?",
            params![ts, seq_id],
        )
        .unwrap();
        seq_id
    }

    fn test_ctx(db: crate::db::Db) -> Ctx {
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        Ctx {
            db,
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

    fn seed_codex_dispatched_job(db: &crate::db::Db, agent_id: &str, job_id: &str) {
        let conn = db.conn();
        insert_session_sync(
            &conn,
            "s_codex_health",
            "p_codex_health",
            "/tmp/codex-health",
        )
        .unwrap();
        insert_agent_sync(
            &conn,
            agent_id,
            "s_codex_health",
            "codex",
            "BUSY",
            Some(123),
        )
        .unwrap();
        insert_job_sync(&conn, job_id, agent_id, None, "say alpha\n").unwrap();
        conn.execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = 10 WHERE id = ?",
            [job_id],
        )
        .unwrap();
        let dispatch_seq = insert_event_sync(
            &conn,
            agent_id,
            None,
            "command_received",
            r#"{"cmd":"say alpha\n","status":"SENT"}"#,
        )
        .unwrap();
        update_dispatched_seq_id_sync(&conn, job_id, dispatch_seq).unwrap();
        insert_output_at(&conn, agent_id, "say alpha\nalpha\n", 11);
    }

    #[tokio::test]
    async fn test_query_last_progress_has_no_events() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
            seed_agent(&conn, "a1");
        }

        let progress = query_last_progress(db, "a1".to_string()).await.unwrap();
        assert_eq!(progress, (None, None));
    }

    #[tokio::test]
    async fn test_query_last_progress_tracks_last_output_and_last_marker_separately() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
            seed_agent(&conn, "a1");
            seed_agent(&conn, "a2");
            insert_output_at(&conn, "a1", "plain output", 10);
            insert_output_at(&conn, "a1", "done <<ah-idle:job-id=j1>>", 20);
            insert_output_at(&conn, "a1", "final output", 30);
            insert_output_at(&conn, "a2", "done <<ah-idle:job-id=other>>", 40);
        }

        let progress = query_last_progress(db.clone(), "a1".to_string())
            .await
            .unwrap();
        assert_eq!(progress, (Some(30), Some(20)));

        let isolated = query_last_progress(db, "a2".to_string()).await.unwrap();
        assert_eq!(isolated, (Some(40), Some(40)));
    }

    #[tokio::test]
    async fn test_query_last_progress_ignores_output_without_marker() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
            seed_agent(&conn, "a1");
            insert_output_at(&conn, "a1", "plain output", 10);
        }

        let progress = query_last_progress(db, "a1".to_string()).await.unwrap();
        assert_eq!(progress, (Some(10), None));
    }

    #[tokio::test]
    #[ignore = "manual perf guard for #94; main validation runs this serially"]
    async fn test_query_last_progress_100k_events_is_bounded() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/p1").unwrap();
            seed_agent(&conn, "a1");
            for idx in 0..100_000 {
                insert_output_at(&conn, "a1", &format!("chunk {idx}"), idx);
            }
            insert_output_at(&conn, "a1", "done <<ah-idle:job-id=j1>>", 100_001);
            insert_output_at(&conn, "a1", "final output", 100_002);
        }

        let started = Instant::now();
        let progress = query_last_progress(db, "a1".to_string()).await.unwrap();
        assert_eq!(progress, (Some(100_002), Some(100_001)));
        assert!(
            started.elapsed().as_millis() < 50,
            "bounded query should not materialize 100k events"
        );
    }

    fn seed_queued_job(
        db: &crate::db::Db,
        agent_id: &str,
        job_id: &str,
        agent_state: &str,
        created_at: i64,
    ) {
        let conn = db.conn();
        let _ = insert_session_sync(
            &conn,
            "s_queued_health",
            "p_queued_health",
            "/tmp/queued-health",
        );
        let _ = insert_agent_sync(
            &conn,
            agent_id,
            "s_queued_health",
            "antigravity",
            agent_state,
            Some(123),
        );
        let _ = insert_job_sync(&conn, job_id, agent_id, None, "test queued\n");
        conn.execute(
            "UPDATE jobs SET status = 'QUEUED', created_at = ? WHERE id = ?",
            params![created_at, job_id],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_queued_job_starvation_watchdog_all_scenarios() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let ctx = test_ctx(db.clone());

        // A1: Starved QUEUED job, agent is BUSY (non-IDLE)
        let agent_a1 = "agent_a1";
        let job_a1 = "job_a1";
        let threshold = QUEUED_STARVATION_THRESHOLD_SECS;
        // Seed job queued at (now - threshold - 10)
        let starved_ts = now_unix_secs() - threshold - 10;
        seed_queued_job(&db, agent_a1, job_a1, "BUSY", starved_ts);

        // Run tick
        health_check_watcher_tick(&ctx, Duration::from_secs(330))
            .await
            .unwrap();

        // Assert A1: Exactly one alert event is issued
        let count_a1: i64 = db.conn().query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%QUEUED_STARVATION_ALERT%'",
            [agent_a1],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count_a1, 1, "A1: Should emit exactly one alert event");

        // Assert A2: Job status is still QUEUED (fail-closed)
        let status_a2: String = db
            .conn()
            .query_row("SELECT status FROM jobs WHERE id = ?", [job_a1], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status_a2, "QUEUED", "A2: Job status must remain QUEUED");

        // A3: Job QUEUED but not starved (duration < threshold)
        let agent_a3 = "agent_a3";
        let job_a3 = "job_a3";
        let recent_ts = now_unix_secs() - threshold + 10;
        seed_queued_job(&db, agent_a3, job_a3, "BUSY", recent_ts);

        health_check_watcher_tick(&ctx, Duration::from_secs(330))
            .await
            .unwrap();

        let count_a3: i64 = db.conn().query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%QUEUED_STARVATION_ALERT%'",
            [agent_a3],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(
            count_a3, 0,
            "A3: Should not alert if queued duration is within threshold"
        );

        // A4: Job starved but agent is IDLE (ready to dispatch)
        let agent_a4 = "agent_a4";
        let job_a4 = "job_a4";
        seed_queued_job(&db, agent_a4, job_a4, "IDLE", starved_ts);

        health_check_watcher_tick(&ctx, Duration::from_secs(330))
            .await
            .unwrap();

        let count_a4: i64 = db.conn().query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%QUEUED_STARVATION_ALERT%'",
            [agent_a4],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count_a4, 0, "A4: Should not alert if agent is IDLE");

        // A5: Deduplication - run tick again for the starved job_a1
        health_check_watcher_tick(&ctx, Duration::from_secs(330))
            .await
            .unwrap();

        let count_a5: i64 = db.conn().query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' AND payload LIKE '%QUEUED_STARVATION_ALERT%'",
            [agent_a1],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(
            count_a5, 1,
            "A5: Alert must be deduplicated (only one event issued)"
        );
    }

    // ---- P0-1 RED contract tests (a4 executor writes; a1 turns GREEN) ----
    //
    // New contract: the pane is never a completion signal. When the completion
    // layer goes stale, health check must emit STOPPED_UNDECLARED_ALERT and leave
    // agent/job state untouched — it must NOT pane-recapture the agent to IDLE nor
    // mark it STUCK. Tests 3/4 are RED under current code (which mutates state via
    // pane recapture / mark_agent_stuck) and only pass once a1 deletes those
    // paths. Test 5 is a GREEN regression guard on the untouched log/hook path.

    fn count_stopped_undeclared_alerts(db: &crate::db::Db, agent_id: &str) -> i64 {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' \
                 AND payload LIKE '%\"reason\":\"STOPPED_UNDECLARED_ALERT\"%'",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// Test 3 — health completion-stale with an idle-looking pane must NOT mark the
    /// agent complete; the pane is not a completion signal, so it only emits
    /// STOPPED_UNDECLARED_ALERT.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_health_check_pane_idle_does_not_mark_complete() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let agent_id = "a_p01_health_pane_idle";
        let job_id = "job_p01_health_pane_idle";
        seed_codex_dispatched_job(&db, agent_id, job_id);
        let ctx = test_ctx(db.clone());
        let observation = HealthCheckObservation {
            agent_id: agent_id.to_string(),
            provider: "codex".to_string(),
            state: "BUSY".to_string(),
            pane_capture: "say alpha\nalpha\n› \n  gpt-5.5 default · /workspace\n".to_string(),
            pane_capture_ok: true,
            last_output_ts: Some(11),
            last_marker_ts: None,
            dispatched_at: Some(10),
            now_ts: 400,
        };

        // RED now: current code pane-recaptures this idle-looking pane → marks the
        // agent IDLE and the job COMPLETED. New contract: pane never completes.
        escalate_health_stuck(&ctx, &observation, 300)
            .await
            .unwrap();

        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(
            state, "BUSY",
            "idle-looking pane must NOT mark agent complete (stays BUSY)"
        );
        assert_ne!(
            job.status, "COMPLETED",
            "idle-looking pane must NOT complete the job"
        );
        assert_eq!(
            count_stopped_undeclared_alerts(&db, agent_id),
            1,
            "idle pane with no hook/log completion must emit STOPPED_UNDECLARED_ALERT"
        );
    }

    /// Test 4 — dispatched, pane static, hook AND log both absent: the agent has
    /// stopped without declaring completion. Emit STOPPED_UNDECLARED_ALERT and do
    /// NOT mutate state (neither STUCK nor IDLE).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_completion_dual_absence_emits_stopped_undeclared_alert() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let agent_id = "a_p01_dual_absence";
        let job_id = "job_p01_dual_absence";
        seed_codex_dispatched_job(&db, agent_id, job_id);
        // Pane shows only the echoed command (no assistant reply, no idle marker):
        // completion is stale and neither hook nor log declared completion.
        db.conn()
            .execute(
                "UPDATE events SET payload = ? WHERE agent_id = ? AND event_type = 'output_chunk'",
                [
                    serde_json::json!({ "text": "say alpha\n" }).to_string(),
                    agent_id.to_string(),
                ],
            )
            .unwrap();
        let ctx = test_ctx(db.clone());
        let observation = HealthCheckObservation {
            agent_id: agent_id.to_string(),
            provider: "codex".to_string(),
            state: "BUSY".to_string(),
            pane_capture: "say alpha\n› \n  gpt-5.5 default · /workspace\n".to_string(),
            pane_capture_ok: true,
            last_output_ts: Some(11),
            last_marker_ts: None,
            dispatched_at: Some(10),
            now_ts: 400,
        };

        // RED now: current code marks the agent STUCK. New contract: no state
        // mutation, only a STOPPED_UNDECLARED_ALERT audit event.
        escalate_health_stuck(&ctx, &observation, 300)
            .await
            .unwrap();

        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            state, "BUSY",
            "dual-absence must NOT mutate agent state (neither STUCK nor IDLE)"
        );
        assert_eq!(
            count_stopped_undeclared_alerts(&db, agent_id),
            1,
            "dual-absence must emit exactly one STOPPED_UNDECLARED_ALERT audit event"
        );
    }

    /// Test 5 — regression guard: provider-native transcript completion still
    /// marks the agent IDLE and completes the job. Terminal marker inference is
    /// intentionally excluded for Codex.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_completion_still_works_from_hook_or_log() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        let agent_id = "a_p01_log_completion";
        let job_id = "job_p01_log_completion";
        seed_codex_dispatched_job(&db, agent_id, job_id);

        let lifecycle_id: String = db
            .conn()
            .query_row(
                "SELECT lifecycle_id FROM agents WHERE id = ?",
                [agent_id],
                |row| row.get(0),
            )
            .unwrap();
        let (changes, affected_job) =
            crate::runtime_observation::intake::observe_transcript_completion(
                crate::runtime_observation::intake::TranscriptCompletionObservation {
                    db: db.clone(),
                    agent_id: agent_id.to_string(),
                    provider: "codex".to_string(),
                    reply: Some("alpha".to_string()),
                    raw_path: "/tmp/codex-rollout.jsonl".to_string(),
                    raw_offset: 42,
                    provider_turn_id: None,
                    expected_lifecycle_id: lifecycle_id,
                    prompt_fingerprint: None,
                },
            )
            .await
            .unwrap();

        let state: String = db
            .conn()
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap();
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert!(
            changes > 0,
            "log-signal completion must transition the agent"
        );
        assert_eq!(affected_job.as_deref(), Some(job_id));
        assert_eq!(
            state, "IDLE",
            "log-signal completion must still mark the agent IDLE"
        );
        assert_eq!(
            job.status, "COMPLETED",
            "log-signal completion must still complete the job"
        );
    }
}
