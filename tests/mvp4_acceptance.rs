mod common;

use ah::db;
use ah::db::agents::{insert_agent, query_agent_state};
use ah::db::sessions::insert_session;
use ah::db::state_machine::mark_agent_unknown;
use ah::error::AhError;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_assert_state, handle_agent_discard_evidence, handle_agent_kill, handle_agent_read,
    handle_agent_send, handle_agent_spawn,
};
use ah::sandbox::EnvState;
use common::TmuxServerGuard;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

struct Harness {
    ctx: Ctx,
    _tmux_guard: TmuxServerGuard,
    _state_dir: tempfile::TempDir,
    _db_file: tempfile::NamedTempFile,
}

impl Harness {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let state_dir_path = state_dir.path().to_path_buf();
        let tmux_guard = TmuxServerGuard::new(&state_dir_path);
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir_path.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux_guard.server(),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        Self {
            ctx,
            _tmux_guard: tmux_guard,
            _state_dir: state_dir,
            _db_file: db_file,
        }
    }

    async fn insert_session(&self, session_id: &str) {
        insert_session(
            self.ctx.db.clone(),
            session_id.to_string(),
            format!("p_{session_id}"),
            format!("/tmp/{session_id}"),
        )
        .await
        .unwrap();
    }
}

async fn sleep_ms(ms: u64) {
    tokio::task::spawn_blocking(move || std::thread::sleep(Duration::from_millis(ms)))
        .await
        .unwrap();
}

async fn spawn_bash(h: &Harness, session_id: &str, agent_id: &str) {
    let result = handle_agent_spawn(
        json!({
            "session_id": session_id,
            "agent_id": agent_id,
            "provider": "bash",
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    assert_eq!(result["state"], "SPAWNING");
    wait_for_state(h, agent_id, "IDLE", Duration::from_secs(10)).await;
}

async fn send_text(h: &Harness, agent_id: &str, text: &str, request_id: &str) -> Value {
    handle_agent_send(
        json!({
            "agent_id": agent_id,
            "text": text,
            "request_id": request_id,
        }),
        &h.ctx,
    )
    .await
    .unwrap()
}

async fn wait_for_state(h: &Harness, agent_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let state = query_agent_state(h.ctx.db.clone(), agent_id.to_string())
            .await
            .unwrap()
            .map(|(state, _)| state);
        if state.as_deref() == Some(expected) {
            return;
        }
        sleep_ms(50).await;
    }
    panic!("agent {agent_id} did not reach {expected} within {timeout:?}");
}

async fn wait_for_event(
    h: &Harness,
    agent_id: &str,
    timeout: Duration,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let result = handle_agent_read(
            json!({
                "agent_id": agent_id,
                "since_event_id": 0,
            }),
            &h.ctx,
        )
        .await
        .unwrap();
        if let Some(event) = result["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| pred(event))
        {
            return event.clone();
        }
        sleep_ms(50).await;
    }
    panic!("event for {agent_id} did not appear within {timeout:?}");
}

async fn create_unknown_evidence(h: &Harness, agent_id: &str, session_id: &str) -> String {
    insert_session(
        h.ctx.db.clone(),
        session_id.to_string(),
        format!("p_{session_id}"),
        format!("/tmp/{session_id}"),
    )
    .await
    .unwrap();
    insert_agent(
        h.ctx.db.clone(),
        agent_id.to_string(),
        session_id.to_string(),
        "bash".to_string(),
        "BUSY".to_string(),
        Some(1),
    )
    .await
    .unwrap();
    let (changes, affected_job) = mark_agent_unknown(
        h.ctx.db.clone(),
        agent_id.to_string(),
        "PTY_MARKER_TIMEOUT".to_string(),
        b"pane snapshot".to_vec(),
        serde_json::json!(["[\\$#>✦]\\s*$"]),
    )
    .await
    .unwrap();
    if changes > 0 {
        // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
        if let Some(job_id) = affected_job {
            ah::orchestrator::pubsub::notify_job_update(&job_id);
        }
        ah::orchestrator::wake_up();
    }
    evidence_id(h, agent_id, "PENDING")
}

fn evidence_id(h: &Harness, agent_id: &str, status: &str) -> String {
    h.ctx
        .db
        .conn()
        .query_row(
            "SELECT id FROM evidence WHERE agent_id = ? AND status = ? ORDER BY created_at DESC LIMIT 1",
            [agent_id, status],
            |row| row.get(0),
        )
        .unwrap()
}

fn evidence_counts(h: &Harness, agent_id: &str) -> (i64, i64) {
    let conn = h.ctx.db.conn();
    let sealed = conn
        .query_row(
            "SELECT COUNT(*) FROM evidence WHERE agent_id = ? AND status='SEALED'",
            [agent_id],
            |row| row.get(0),
        )
        .unwrap();
    let pending = conn
        .query_row(
            "SELECT COUNT(*) FROM evidence WHERE agent_id = ? AND status='PENDING'",
            [agent_id],
            |row| row.get(0),
        )
        .unwrap();
    (sealed, pending)
}

async fn cleanup_agent(h: &Harness, agent_id: &str) {
    let _ = handle_agent_kill(json!({ "agent_id": agent_id }), &h.ctx).await;
    sleep_ms(150).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ac1_timeout_writes_evidence_transaction() {
    let h = Harness::new();
    h.insert_session("s_ac1").await;
    let agent_id = format!("ag_m4_ac1_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac1", &agent_id).await;

    send_text(&h, &agent_id, "sleep 30\n", "ac1-sleep").await;
    wait_for_state(&h, &agent_id, "UNKNOWN", Duration::from_secs(7)).await;

    let (state, error_code, event_seq_id, pane_len, failed_rules): (
        String,
        Option<String>,
        i64,
        i64,
        String,
    ) = h
        .ctx
        .db
        .conn()
        .query_row(
            "SELECT agents.state, agents.error_code, evidence.event_seq_id, length(evidence.pane_bytes), evidence.failed_rules FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id = ? AND evidence.status='PENDING'",
            [agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    let event = wait_for_event(&h, &agent_id, EVENT_TIMEOUT, |event| {
        event["seq_id"].as_i64() == Some(event_seq_id)
            && event["payload"].as_str().is_some_and(|p| {
                p.contains("\"to\":\"UNKNOWN\"") && p.contains("PTY_MARKER_TIMEOUT")
            })
    })
    .await;

    assert_eq!(state, "UNKNOWN");
    assert_eq!(error_code.as_deref(), Some("PTY_MARKER_TIMEOUT"));
    assert!(pane_len > 0);
    assert!(failed_rules.contains("[\\\\$#>✦]\\\\s*$"));
    assert_eq!(event["event_type"], "state_change");
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ac2_assert_state_reviews_evidence() {
    let h = Harness::new();
    h.insert_session("s_ac2").await;
    let agent_id = format!("ag_m4_ac2_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac2", &agent_id).await;
    send_text(&h, &agent_id, "sleep 30\n", "ac2-sleep").await;
    wait_for_state(&h, &agent_id, "UNKNOWN", Duration::from_secs(7)).await;
    let evidence_id = evidence_id(&h, &agent_id, "PENDING");

    let result = handle_agent_assert_state(
        json!({
            "agent_id": agent_id,
            "state": "IDLE",
            "evidence_id": evidence_id,
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let (state, sub_state, status, asserted): (String, Option<String>, String, Option<String>) = h
        .ctx
        .db
        .conn()
        .query_row(
            "SELECT agents.state, agents.sub_state, evidence.status, evidence.l3_asserted_state FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id = ?",
            [agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();

    assert_eq!(result["state"], "IDLE");
    assert_eq!(state, "IDLE");
    assert_eq!(sub_state.as_deref(), Some("Asserted"));
    assert_eq!(status, "REVIEWED");
    assert_eq!(asserted.as_deref(), Some("IDLE"));
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ac3_discard_evidence_does_not_touch_agent() {
    let h = Harness::new();
    let agent_id = format!("ag_m4_ac3_{}", uuid::Uuid::new_v4());
    let evidence_id = create_unknown_evidence(&h, &agent_id, "s_ac3").await;

    let discarded = handle_agent_discard_evidence(json!({ "evidence_id": evidence_id }), &h.ctx)
        .await
        .unwrap();
    let (state, status): (String, String) = h
        .ctx
        .db
        .conn()
        .query_row(
            "SELECT agents.state, evidence.status FROM agents JOIN evidence ON evidence.agent_id = agents.id WHERE agents.id = ?",
            [agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(discarded["status"], "DISCARDED");
    assert_eq!(state, "UNKNOWN");
    assert_eq!(status, "DISCARDED");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ac4_unknown_send_recovers_with_new_request_and_idempotency() {
    let h = Harness::new();
    h.insert_session("s_ac4").await;
    let agent_id = format!("ag_m4_ac4_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac4", &agent_id).await;
    send_text(&h, &agent_id, "sleep 30\n", "ac4-sleep").await;
    wait_for_state(&h, &agent_id, "UNKNOWN", Duration::from_secs(7)).await;

    let first = send_text(&h, &agent_id, "echo recover\n", "ac4-recover").await;
    let second = send_text(&h, &agent_id, "echo recover again\n", "ac4-recover").await;

    assert_eq!(first["state"], "WAITING_FOR_ACK");
    assert_eq!(first["seq_id"], second["seq_id"]);
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ac5_repeated_unknown_seals_previous_evidence() {
    let h = Harness::new();
    let agent_id = format!("ag_m4_ac5_{}", uuid::Uuid::new_v4());
    create_unknown_evidence(&h, &agent_id, "s_ac5").await;
    h.ctx
        .db
        .conn()
        .execute(
            "UPDATE agents SET state='BUSY' WHERE id=?",
            [agent_id.as_str()],
        )
        .unwrap();
    let (changes, affected_job) = mark_agent_unknown(
        h.ctx.db.clone(),
        agent_id.clone(),
        "PTY_MARKER_TIMEOUT".to_string(),
        b"second".to_vec(),
        serde_json::json!(["[\\$#>✦]\\s*$"]),
    )
    .await
    .unwrap();
    if changes > 0 {
        // R-2 (mvp12): notify hoisted from state_machine wrapper for clearer dispatcher boundary.
        if let Some(job_id) = affected_job {
            ah::orchestrator::pubsub::notify_job_update(&job_id);
        }
        ah::orchestrator::wake_up();
    }

    assert_eq!(evidence_counts(&h, &agent_id), (1, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn ac6_assert_state_boundaries() {
    let h = Harness::new();
    let owner_evidence = create_unknown_evidence(&h, "ag_owner", "s_ac6").await;
    insert_agent(
        h.ctx.db.clone(),
        "ag_other".to_string(),
        "s_ac6".to_string(),
        "bash".to_string(),
        "UNKNOWN".to_string(),
        Some(2),
    )
    .await
    .unwrap();
    insert_agent(
        h.ctx.db.clone(),
        "ag_busy".to_string(),
        "s_ac6".to_string(),
        "bash".to_string(),
        "BUSY".to_string(),
        Some(3),
    )
    .await
    .unwrap();
    {
        let conn = h.ctx.db.conn();
        conn.execute(
            "INSERT INTO events (agent_id, event_type, payload) VALUES ('ag_busy', 'state_change', '{}')",
            [],
        )
        .unwrap();
        let seq_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO evidence (id, agent_id, event_seq_id, pane_bytes, failed_rules) VALUES ('evi_busy', 'ag_busy', ?, X'01', '[]')",
            [seq_id],
        )
        .unwrap();
    }

    let missing = handle_agent_assert_state(
        json!({"agent_id": "ag_owner", "state": "IDLE", "evidence_id": "evi_missing"}),
        &h.ctx,
    )
    .await
    .unwrap_err();
    let mismatch = handle_agent_assert_state(
        json!({"agent_id": "ag_other", "state": "IDLE", "evidence_id": owner_evidence}),
        &h.ctx,
    )
    .await
    .unwrap_err();
    let wrong_state = handle_agent_assert_state(
        json!({"agent_id": "ag_busy", "state": "IDLE", "evidence_id": "evi_busy"}),
        &h.ctx,
    )
    .await
    .unwrap_err();
    let invalid = handle_agent_assert_state(
        json!({"agent_id": "ag_owner", "state": "BUSY", "evidence_id": evidence_id(&h, "ag_owner", "PENDING")}),
        &h.ctx,
    )
    .await
    .unwrap_err();

    assert!(matches!(missing, AhError::DbEvidenceNotFound { .. }));
    assert!(matches!(mismatch, AhError::DbEvidenceNotFound { .. }));
    assert!(matches!(wrong_state, AhError::AgentWrongState { .. }));
    assert!(matches!(invalid, AhError::IpcInvalidRequest(_)));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ac7_full_fallback_loop_closes() {
    let h = Harness::new();
    h.insert_session("s_ac7").await;
    let agent_id = format!("ag_m4_ac7_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac7", &agent_id).await;
    send_text(&h, &agent_id, "sleep 30\n", "ac7-sleep").await;
    wait_for_state(&h, &agent_id, "UNKNOWN", Duration::from_secs(7)).await;
    let evidence_id = evidence_id(&h, &agent_id, "PENDING");

    wait_for_event(&h, &agent_id, EVENT_TIMEOUT, |event| {
        event["payload"].as_str().is_some_and(|payload| {
            payload.contains("\"to\":\"UNKNOWN\"") && payload.contains("PTY_MARKER_TIMEOUT")
        })
    })
    .await;
    handle_agent_assert_state(
        json!({
            "agent_id": agent_id,
            "state": "IDLE",
            "evidence_id": evidence_id,
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let sent = send_text(&h, &agent_id, "\u{3}echo done\n", "ac7-done").await;
    wait_for_state(&h, &agent_id, "IDLE", EVENT_TIMEOUT).await;

    assert_eq!(sent["state"], "WAITING_FOR_ACK");
    let (state, sub_state): (String, Option<String>) = h
        .ctx
        .db
        .conn()
        .query_row(
            "SELECT state, sub_state FROM agents WHERE id=?",
            [agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "IDLE");
    assert_eq!(sub_state.as_deref(), Some("Matched"));
    cleanup_agent(&h, &agent_id).await;
}
