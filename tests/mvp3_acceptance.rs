mod common;

use ah::db;
use ah::db::agents::query_agent_state;
use ah::db::sessions::insert_session;
use ah::error::AhError;
use ah::rpc::Ctx;
use ah::rpc::handlers::{
    handle_agent_kill, handle_agent_read, handle_agent_send, handle_agent_spawn,
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

async fn spawn_bash(h: &Harness, session_id: &str, agent_id: &str) -> i64 {
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
    let pid = result["pid"].as_i64().unwrap();
    wait_for_state(h, agent_id, "IDLE", Duration::from_secs(10)).await;
    pid
}

async fn send_text(h: &Harness, agent_id: &str, text: &str) -> Value {
    handle_agent_send(
        json!({
            "agent_id": agent_id,
            "text": text,
            "request_id": format!("req_{}", uuid::Uuid::new_v4()),
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

async fn wait_for_marker_after(
    h: &Harness,
    agent_id: &str,
    since_seq_id: i64,
    timeout: Duration,
) -> Value {
    wait_for_event(h, agent_id, timeout, |event| {
        event["seq_id"].as_i64().unwrap_or_default() > since_seq_id
            && event["event_type"] == "state_change"
            && event["payload"]
                .as_str()
                .is_some_and(|p| p.contains("\"to\":\"IDLE\"") && p.contains("MARKER_MATCHED"))
    })
    .await
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

fn max_seq(h: &Harness, agent_id: &str) -> i64 {
    h.ctx
        .db
        .conn()
        .query_row(
            "SELECT COALESCE(MAX(seq_id), 0) FROM events WHERE agent_id = ?",
            [agent_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn command_count(h: &Harness, agent_id: &str) -> i64 {
    h.ctx
        .db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'command_received'",
            [agent_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn agent_row(h: &Harness, agent_id: &str) -> (String, Option<String>, Option<String>) {
    h.ctx
        .db
        .conn()
        .query_row(
            "SELECT state, sub_state, error_code FROM agents WHERE id = ?",
            [agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

async fn cleanup_agent(h: &Harness, agent_id: &str) {
    let _ = handle_agent_kill(json!({ "agent_id": agent_id }), &h.ctx).await;
    sleep_ms(150).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ac1_spawn_reaches_idle_matched() {
    let h = Harness::new();
    h.insert_session("s_ac1").await;
    let agent_id = format!("ag_m3_ac1_{}", uuid::Uuid::new_v4());

    spawn_bash(&h, "s_ac1", &agent_id).await;
    let event = wait_for_marker_after(&h, &agent_id, 0, EVENT_TIMEOUT).await;
    let (state, sub_state, error_code) = agent_row(&h, &agent_id);

    assert_eq!(event["event_type"], "state_change");
    assert_eq!(state, "IDLE");
    assert_eq!(sub_state.as_deref(), Some("Matched"));
    assert_eq!(error_code, None);
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ac2_send_transitions_busy_then_idle() {
    let h = Harness::new();
    h.insert_session("s_ac2").await;
    let agent_id = format!("ag_m3_ac2_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac2", &agent_id).await;
    let since = max_seq(&h, &agent_id);

    let sent = send_text(&h, &agent_id, "echo ok\n").await;
    let state = query_agent_state(h.ctx.db.clone(), agent_id.clone())
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(sent["state"], "WAITING_FOR_ACK");
    assert_eq!(state, "WAITING_FOR_ACK");

    wait_for_marker_after(&h, &agent_id, since, EVENT_TIMEOUT).await;
    let (state, sub_state, _) = agent_row(&h, &agent_id);
    assert_eq!(state, "IDLE");
    assert_eq!(sub_state.as_deref(), Some("Matched"));
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ac3_ansi_output_does_not_hide_prompt_marker() {
    let h = Harness::new();
    h.insert_session("s_ac3").await;
    let agent_id = format!("ag_m3_ac3_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac3", &agent_id).await;
    let since = max_seq(&h, &agent_id);

    send_text(&h, &agent_id, "printf '\\033[2J\\033[H'; echo done\n").await;
    wait_for_marker_after(&h, &agent_id, since, EVENT_TIMEOUT).await;

    let (state, sub_state, _) = agent_row(&h, &agent_id);
    assert_eq!(state, "IDLE");
    assert_eq!(sub_state.as_deref(), Some("Matched"));
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ac4_busy_timeout_marks_unknown() {
    let h = Harness::new();
    h.insert_session("s_ac4").await;
    let agent_id = format!("ag_m3_ac4_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac4", &agent_id).await;

    send_text(&h, &agent_id, "sleep 30\n").await;
    wait_for_state(&h, &agent_id, "UNKNOWN", Duration::from_secs(7)).await;
    let (state, _, error_code) = agent_row(&h, &agent_id);

    assert_eq!(state, "UNKNOWN");
    assert_eq!(error_code.as_deref(), Some("PTY_MARKER_TIMEOUT"));
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ac5_periodic_output_resets_busy_timer() {
    let h = Harness::new();
    h.insert_session("s_ac5").await;
    let agent_id = format!("ag_m3_ac5_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac5", &agent_id).await;

    send_text(
        &h,
        &agent_id,
        "for i in 1 2 3 4 5; do sleep 2; echo $i; done\n",
    )
    .await;
    wait_for_state(&h, &agent_id, "IDLE", Duration::from_secs(13)).await;
    let (state, _, error_code) = agent_row(&h, &agent_id);

    assert_eq!(state, "IDLE");
    assert_eq!(error_code, None);
    cleanup_agent(&h, &agent_id).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ac6_three_agents_match_independently() {
    let h = Harness::new();
    h.insert_session("s_ac6").await;
    let agents: Vec<String> = (0..3)
        .map(|_| format!("ag_m3_ac6_{}", uuid::Uuid::new_v4()))
        .collect();

    for agent_id in &agents {
        spawn_bash(&h, "s_ac6", agent_id).await;
    }

    for agent_id in &agents {
        let (state, sub_state, _) = agent_row(&h, agent_id);
        assert_eq!(state, "IDLE");
        assert_eq!(sub_state.as_deref(), Some("Matched"));
    }

    for agent_id in &agents {
        cleanup_agent(&h, agent_id).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ac7_busy_agent_rejects_second_new_send() {
    let h = Harness::new();
    h.insert_session("s_ac7").await;
    let agent_id = format!("ag_m3_ac7_{}", uuid::Uuid::new_v4());
    spawn_bash(&h, "s_ac7", &agent_id).await;

    let first = handle_agent_send(
        json!({
            "agent_id": agent_id,
            "text": "sleep 5\n",
            "request_id": "first",
        }),
        &h.ctx,
    )
    .await
    .unwrap();
    let second = handle_agent_send(
        json!({
            "agent_id": agent_id,
            "text": "echo should_not_run\n",
            "request_id": "second",
        }),
        &h.ctx,
    )
    .await
    .unwrap_err();

    assert_eq!(first["state"], "WAITING_FOR_ACK");
    assert!(matches!(
        second,
        AhError::AgentWrongState { current_state } if current_state == "WAITING_FOR_ACK"
    ));
    assert_eq!(command_count(&h, &agent_id), 1);
    cleanup_agent(&h, &agent_id).await;
}
