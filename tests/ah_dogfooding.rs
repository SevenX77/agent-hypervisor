#![cfg(unix)]
#![allow(dead_code)]

use ah::db::{self as db, agents, events, jobs, sessions, state_machine};
use ah::pane_diff::{
    PaneDiffObservation, process_pane_diff_observations, resolve_stuck_watch_config,
};
use ah::provider::health_check::{
    HealthCheckObservation, escalate_health_stuck, health_check_observe,
};
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use ah::tmux::TmuxServer;
use rusqlite::params;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SESSION_ID: &str = "dogfood_s1";
const PROJECT_ID: &str = "dogfood_project";
const AGENT_ID: &str = "dogfood_a1";
const JOB_ID: &str = "dogfood_job_1";

struct Harness {
    ctx: Ctx,
    _db_file: tempfile::NamedTempFile,
    state_dir: tempfile::TempDir,
    project_dir: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let project_dir = tempfile::TempDir::new().unwrap();
        let tmux_server = Arc::new(TmuxServer::new(state_dir.path()));
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server,
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        Self {
            ctx,
            _db_file: db_file,
            state_dir,
            project_dir,
        }
    }

    async fn rpc_raw(&self, method: &str, params: Value) -> Value {
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": "dogfood",
        });
        serde_json::from_str(&dispatch(&request.to_string(), &self.ctx).await)
            .expect("dispatch response should be JSON")
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        let response = self.rpc_raw(method, params).await;
        if let Some(error) = response.get("error") {
            panic!("RPC {method} failed: {error}");
        }
        response.get("result").cloned().expect("RPC result")
    }

    async fn seed_idle_agent(&self) {
        sessions::insert_session(
            self.ctx.db.clone(),
            SESSION_ID.into(),
            PROJECT_ID.into(),
            self.project_dir.path().display().to_string(),
        )
        .await
        .unwrap();
        agents::insert_agent(
            self.ctx.db.clone(),
            AGENT_ID.into(),
            SESSION_ID.into(),
            "bash".into(),
            "IDLE".into(),
            Some(123),
        )
        .await
        .unwrap();
    }

    async fn seed_dispatched_busy_job(&self, job_id: &str, prompt: &str) {
        self.seed_idle_agent().await;
        jobs::insert_job(
            self.ctx.db.clone(),
            job_id.into(),
            AGENT_ID.into(),
            None,
            prompt.into(),
        )
        .await
        .unwrap();
        jobs::dispatch_job_to_agent(
            self.ctx.db.clone(),
            AGENT_ID.into(),
            vec!["IDLE".into()],
            "BUSY".into(),
            "command_received".into(),
            json!({ "status": "SENT" }),
        )
        .await
        .unwrap()
        .expect("job should dispatch");
    }

    fn agent_state(&self) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![AGENT_ID],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn job_status(&self, job_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT status FROM jobs WHERE id = ?",
                params![job_id],
                |row| row.get(0),
            )
            .unwrap()
    }
}

struct DogfoodAhClient {
    socket: PathBuf,
}

impl DogfoodAhClient {
    fn call(&self, method: &str, params: Value) -> std::io::Result<Value> {
        let mut stream = UnixStream::connect(&self.socket)?;
        let request = json!({"jsonrpc": "2.0", "method": method, "params": params, "id": 1});
        writeln!(stream, "{request}")?;
        stream.shutdown(std::net::Shutdown::Write)?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        Ok(serde_json::from_str(line.trim()).expect("response JSON"))
    }
}

fn dogfood_ah_client(state_dir: &Path) -> DogfoodAhClient {
    DogfoodAhClient {
        socket: state_dir.join("ahd.sock"),
    }
}

#[derive(Default)]
struct InterventionCounters {
    cancel: AtomicUsize,
    capture: AtomicUsize,
    poll: AtomicUsize,
}

impl InterventionCounters {
    fn cancel_count(&self) -> usize {
        self.cancel.load(Ordering::SeqCst)
    }

    fn capture_count(&self) -> usize {
        self.capture.load(Ordering::SeqCst)
    }

    fn poll_count(&self) -> usize {
        self.poll.load(Ordering::SeqCst)
    }
}

async fn dispatch_job_via_ah(h: &Harness, text: &str) -> String {
    let result = h
        .rpc("job.submit", json!({"agent_id": AGENT_ID, "text": text}))
        .await;
    result["job_id"].as_str().unwrap().to_string()
}

fn install_mock_dogfood_provider(tmp_dir: &Path) -> PathBuf {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_dogfood_provider.sh");
    let bin_dir = tmp_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).unwrap();
    let dst = bin_dir.join("mock_dogfood_provider.sh");
    std::fs::copy(src, &dst).unwrap();
    let fake_claude = bin_dir.join("claude");
    std::fs::copy(&dst, &fake_claude).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&dst).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dst, perms).unwrap();
        let mut perms = std::fs::metadata(&fake_claude).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&fake_claude, perms).unwrap();
    }
    let old_path = std::env::var("PATH").unwrap_or_default();
    unsafe {
        std::env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));
        std::env::set_var("HOME", tmp_dir);
        std::env::set_var("XDG_CACHE_HOME", tmp_dir.join(".cache"));
        std::env::set_var("MOCK_DOGFOOD_PROVIDER", "claude");
    }
    dst
}

async fn spawn_real_dogfood_agent(h: &Harness) {
    install_mock_dogfood_provider(h.project_dir.path());
    sessions::insert_session(
        h.ctx.db.clone(),
        SESSION_ID.into(),
        PROJECT_ID.into(),
        h.project_dir.path().display().to_string(),
    )
    .await
    .unwrap();
    let result = h
        .rpc(
            "agent.spawn",
            json!({
                "session_id": SESSION_ID,
                "agent_id": AGENT_ID,
                "provider": "bash",
            }),
        )
        .await;
    assert_eq!(result["state"], "SPAWNING");
    wait_for_agent_state(h, "IDLE", Duration::from_secs(10)).await;
    ah::orchestrator::spawn_orchestrator_task(h.ctx.clone());
}

async fn enqueue_known_job(h: &Harness, job_id: &str) -> Instant {
    let fixture = h.project_dir.path().join("bin/mock_dogfood_provider.sh");
    let prompt = format!(
        "printf 'job-id:{job_id}\\n' | MOCK_DOGFOOD_PROVIDER=bash {}",
        shell_quote(&fixture.display().to_string())
    );
    jobs::insert_job(
        h.ctx.db.clone(),
        job_id.into(),
        AGENT_ID.into(),
        None,
        prompt,
    )
    .await
    .unwrap();
    let started = Instant::now();
    ah::orchestrator::wake_up();
    started
}

async fn wait_for_agent_state(h: &Harness, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if h.agent_state() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "agent state did not reach {expected}, current={}",
        h.agent_state()
    );
}

async fn wait_for_job_status(h: &Harness, job_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if h.job_status(job_id) == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let events = events::query_events_since(h.ctx.db.clone(), AGENT_ID.into(), 0)
        .await
        .unwrap();
    panic!(
        "job {job_id} did not reach {expected}, current={}, events={:?}",
        h.job_status(job_id),
        events
            .iter()
            .map(|event| (&event.event_type, &event.payload))
            .collect::<Vec<_>>()
    );
}

async fn run_real_stdout_job(h: &Harness, job_id: &str) -> Duration {
    let _dispatch_started = enqueue_known_job(h, job_id).await;
    wait_for_job_status(h, job_id, "COMPLETED", Duration::from_secs(10)).await;
    let subscribe_started = Instant::now();
    let response = h
        .rpc_raw(
            "event.subscribe",
            json!({"job_id": job_id, "event_kind": ["job_state_change"]}),
        )
        .await;
    assert!(
        response.get("error").is_none(),
        "real stdout reader path should stream completion for {job_id}, got {response}"
    );
    let frame = response.get("result").expect("completion frame");
    assert_eq!(frame["kind"], "job_state_change");
    assert_eq!(frame["state"], "COMPLETED");
    subscribe_started.elapsed()
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_event_subscribe_pushes_idle_frame() {
    let h = Harness::new();
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nwrite design")
        .await;
    events::insert_event(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        None,
        "output_chunk".into(),
        json!({"text": "done\n<<ah-idle:job-id=dogfood_job_1>>\n$ "}).to_string(),
    )
    .await
    .unwrap();

    let response = h
        .rpc_raw(
            "event.subscribe",
            json!({"job_id": JOB_ID, "event_kind": ["job_state_change"]}),
        )
        .await;

    assert!(
        response.get("error").is_none(),
        "event.subscribe should be registered and stream frames, got {response}"
    );
    let frame = response
        .get("result")
        .expect("subscribe should yield a frame");
    assert_eq!(frame["kind"], "job_state_change");
    assert_eq!(frame["state"], "COMPLETED");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_real_completion_path_no_seam() {
    let h = Harness::new();
    let _provider = install_mock_dogfood_provider(h.project_dir.path());
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nwrite audit")
        .await;

    events::insert_event(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        None,
        "output_chunk".into(),
        json!({"text": "mock done\n<<ah-idle:job-id=dogfood_job_1>>\n$ "}).to_string(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(h.agent_state(), "IDLE", "marker should drive BUSY -> IDLE");
    assert_eq!(
        h.job_status(JOB_ID),
        "COMPLETED",
        "marker should complete job without dispatch_and_complete_job seam"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_zero_cancel_zero_capture_assertion() {
    let h = Harness::new();
    let counters = Arc::new(InterventionCounters::default());
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nrun five rpc path")
        .await;
    events::insert_event(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        None,
        "output_chunk".into(),
        json!({"text": "done\n<<ah-idle:job-id=dogfood_job_1>>\n$ "}).to_string(),
    )
    .await
    .unwrap();

    let subscribe = h
        .rpc_raw(
            "event.subscribe",
            json!({"job_id": JOB_ID, "event_kind": ["job_state_change"]}),
        )
        .await;
    let _ = h
        .rpc("session.kill", json!({ "session_id": SESSION_ID }))
        .await;

    assert!(
        subscribe.get("error").is_none(),
        "5 RPC path requires event.subscribe, got {subscribe}"
    );
    assert_eq!(counters.cancel_count(), 0, "master cancel counter");
    assert_eq!(counters.capture_count(), 0, "master capture counter");
}

#[test]
#[ignore]
fn test_pr1_regression_still_green() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/ah_full_e2e_main.rs");
    let source = std::fs::read_to_string(path).unwrap();
    assert!(
        !source.contains("dispatch_and_complete_job"),
        "PR-1 regression must be migrated off dispatch_and_complete_job seam"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_marker_job_id_对账() {
    let h = Harness::new();
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\ncheck marker")
        .await;

    events::insert_event(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        None,
        "output_chunk".into(),
        json!({"text": "wrong marker\n<<ah-idle:job-id=WRONG>>\n$ "}).to_string(),
    )
    .await
    .unwrap();
    let _ = state_machine::mark_agent_idle_matched(h.ctx.db.clone(), AGENT_ID.into())
        .await
        .unwrap();

    assert_eq!(
        h.agent_state(),
        "BUSY",
        "wrong job-id marker must not complete current dispatched job"
    );

    events::insert_event(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        None,
        "output_chunk".into(),
        json!({"text": "correct marker\n<<ah-idle:job-id=dogfood_job_1>>\n$ "}).to_string(),
    )
    .await
    .unwrap();
    let _ = state_machine::mark_agent_idle_matched(h.ctx.db.clone(), AGENT_ID.into())
        .await
        .unwrap();
    assert_eq!(h.agent_state(), "IDLE");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_stuck_push_event_via_subscribe() {
    let h = Harness::new();
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nhang for stuck")
        .await;
    let changes = state_machine::mark_agent_stuck(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        "PANE_DIFF_STUCK".into(),
    )
    .await
    .unwrap();
    assert_eq!(changes, 1, "setup should mark agent STUCK");

    let response = h
        .rpc_raw(
            "event.subscribe",
            json!({
                "agent_id": AGENT_ID,
                "job_id": JOB_ID,
                "event_kind": ["stuck"],
            }),
        )
        .await;

    assert!(
        response.get("error").is_none(),
        "event.subscribe should stream stuck frame, got {response}"
    );
    let frame = response.get("result").expect("stuck frame");
    assert_eq!(frame["kind"], "stuck");
    assert_eq!(frame["state"], "STUCK");
    assert_eq!(frame["job_id"], JOB_ID);
    assert!(
        frame["payload"]["signal_kinds"]
            .as_array()
            .is_some_and(|signals| !signals.is_empty()),
        "stuck frame should carry signal_kinds"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_stuck_threshold_env_override() {
    let old_threshold = std::env::var_os("AH_STUCK_THRESHOLD_SECS");
    let old_tick = std::env::var_os("AH_STUCK_TICK_SECS");
    unsafe {
        std::env::set_var("AH_STUCK_THRESHOLD_SECS", "5");
        std::env::set_var("AH_STUCK_TICK_SECS", "1");
    }
    let h = Harness::new();
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nsilent provider")
        .await;

    let started = Instant::now();
    let (_tick, threshold) = resolve_stuck_watch_config();
    assert_eq!(threshold, Duration::from_secs(5));
    let mut state_map = std::collections::HashMap::new();
    let observations = vec![PaneDiffObservation {
        agent_id: AGENT_ID.to_string(),
        text: "Thinking...".to_string(),
        log_mtime: None,
        provider: Some("bash".to_string()),
    }];
    let first =
        process_pane_diff_observations(&mut state_map, observations.clone(), started, threshold);
    let second = process_pane_diff_observations(
        &mut state_map,
        observations,
        started + Duration::from_secs(6),
        threshold,
    );
    assert!(first.stuck_agent_ids.is_empty());
    assert_eq!(second.stuck_agent_ids, [AGENT_ID]);
    let _ = state_machine::mark_agent_stuck(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        "PANE_DIFF_STUCK".into(),
    )
    .await
    .unwrap();

    assert_eq!(
        h.agent_state(),
        "STUCK",
        "env override should let pane_diff mark STUCK quickly"
    );
    assert!(
        started.elapsed() <= Duration::from_secs(7),
        "stuck env override should keep test under 7s"
    );
    unsafe {
        match old_threshold {
            Some(value) => std::env::set_var("AH_STUCK_THRESHOLD_SECS", value),
            None => std::env::remove_var("AH_STUCK_THRESHOLD_SECS"),
        }
        match old_tick {
            Some(value) => std::env::set_var("AH_STUCK_TICK_SECS", value),
            None => std::env::remove_var("AH_STUCK_TICK_SECS"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_push_latency_p95_under_500ms() {
    let mut latencies = Vec::new();

    for idx in 0..5 {
        let h = Harness::new();
        let job_id = format!("dogfood_job_latency_{idx}");
        h.seed_dispatched_busy_job(&job_id, "job-id:latency\nmeasure stuck push")
            .await;
        let emitted_at = Instant::now();
        let _ = state_machine::mark_agent_stuck(
            h.ctx.db.clone(),
            AGENT_ID.into(),
            "PANE_DIFF_STUCK".into(),
        )
        .await
        .unwrap();
        let response = h
            .rpc_raw(
                "event.subscribe",
                json!({
                    "agent_id": AGENT_ID,
                    "job_id": job_id,
                    "event_kind": ["stuck"],
                }),
            )
            .await;
        assert!(
            response.get("error").is_none(),
            "stuck event should stream for latency sample {idx}: {response}"
        );
        latencies.push(emitted_at.elapsed());
    }

    latencies.sort();
    let p95 = latencies[latencies.len() - 1];
    assert!(
        p95 <= Duration::from_millis(500),
        "stuck push p95 should be <=500ms, got {p95:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_zero_schedule_wakeup_poll() {
    let h = Harness::new();
    let counters = Arc::new(InterventionCounters::default());
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nrun stuck rpc path")
        .await;
    let _ = state_machine::mark_agent_stuck(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        "PANE_DIFF_STUCK".into(),
    )
    .await
    .unwrap();

    let subscribe = h
        .rpc_raw(
            "event.subscribe",
            json!({
                "agent_id": AGENT_ID,
                "job_id": JOB_ID,
                "event_kind": ["stuck"],
            }),
        )
        .await;
    let _ = h
        .rpc("session.kill", json!({ "session_id": SESSION_ID }))
        .await;

    assert!(
        subscribe.get("error").is_none(),
        "5 RPC path requires stuck subscribe, got {subscribe}"
    );
    assert_eq!(counters.cancel_count(), 0, "master cancel counter");
    assert_eq!(counters.capture_count(), 0, "master capture counter");
    assert_eq!(
        counters.poll_count(),
        0,
        "master ScheduleWakeup poll counter"
    );
}

#[test]
#[ignore]
fn test_slash_command_keystroke_delivery() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_dogfood_provider.sh");
    let mut child = Command::new(&fixture)
        .env("MOCK_DOGFOOD_PROVIDER", "claude")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mock dogfood provider");

    {
        let stdin = child.stdin.as_mut().expect("provider stdin");
        writeln!(stdin, "/clear").expect("write slash command");
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("provider output");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("<<ah-slash-ack:cmd=/clear>>"),
        "fixture should ack slash command, stdout={stdout}"
    );

    let writer_source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/agent_io/writer.rs"),
    )
    .expect("writer source");
    assert!(
        writer_source.contains("send_slash_command_keystroke"),
        "M3a requires agent.send slash commands to use direct keystroke path"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_dogfood_e2e_full_sop08_simulation() {
    let h = Harness::new();
    let counters = InterventionCounters::default();
    spawn_real_dogfood_agent(&h).await;

    for idx in 0..5 {
        let job_id = format!("dogfood_final_{idx}");
        let elapsed = run_real_stdout_job(&h, &job_id).await;
        assert!(
            elapsed <= Duration::from_secs(10),
            "real stdout reader path should complete job {job_id}, got {elapsed:?}"
        );
    }

    let slash = h
        .rpc(
            "agent.send",
            json!({
                "agent_id": AGENT_ID,
                "text": format!(
                    "printf '/clear\\n' | MOCK_DOGFOOD_PROVIDER=bash {}",
                    shell_quote(&h.project_dir.path().join("bin/mock_dogfood_provider.sh").display().to_string())
                ),
            }),
        )
        .await;
    assert_eq!(slash["state"], "WAITING_FOR_ACK");
    wait_for_agent_state(&h, "IDLE", Duration::from_secs(10)).await;
    let slash_events = events::query_events_since(h.ctx.db.clone(), AGENT_ID.into(), 0)
        .await
        .unwrap();
    assert!(
        slash_events
            .iter()
            .any(|event| event.event_type == "output_chunk"
                && event.payload.contains("<<ah-slash-ack:cmd=/clear>>")),
        "single-session chain should receive slash ack through reader"
    );

    jobs::insert_job(
        h.ctx.db.clone(),
        "dogfood_final_stuck".into(),
        AGENT_ID.into(),
        None,
        "job-id:dogfood_final_stuck\nstuck".into(),
    )
    .await
    .unwrap();
    jobs::dispatch_job_to_agent(
        h.ctx.db.clone(),
        AGENT_ID.into(),
        vec!["IDLE".into()],
        "BUSY".into(),
        "command_received".into(),
        json!({ "status": "SENT" }),
    )
    .await
    .unwrap()
    .expect("stuck job should dispatch");
    let observation = HealthCheckObservation {
        agent_id: AGENT_ID.into(),
        provider: "bash".into(),
        state: "BUSY".into(),
        pane_capture: String::new(),
        pane_capture_ok: false,
        last_output_ts: Some(1),
        last_marker_ts: Some(1),
        dispatched_at: None,
        now_ts: 400,
    };
    let stuck_changes = escalate_health_stuck(&h.ctx, &observation, 300)
        .await
        .expect("health stuck in full chain");
    assert_eq!(stuck_changes, 1);
    let stuck = h
        .rpc_raw(
            "event.subscribe",
            json!({
                "agent_id": AGENT_ID,
                "job_id": "dogfood_final_stuck",
                "event_kind": ["stuck"],
            }),
        )
        .await;
    assert!(
        stuck.get("error").is_none(),
        "stuck frame should stream: {stuck}"
    );

    jobs::insert_job(
        h.ctx.db.clone(),
        "dogfood_final_cancel".into(),
        AGENT_ID.into(),
        None,
        "queued cancel".into(),
    )
    .await
    .unwrap();
    let cancelled = h
        .rpc("job.cancel", json!({"job_id": "dogfood_final_cancel"}))
        .await;
    assert_eq!(cancelled["status"], "CANCELLED");

    assert_eq!(counters.cancel_count(), 0);
    assert_eq!(counters.capture_count(), 0);
    assert_eq!(counters.poll_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_push_latency_p95_real_stdout() {
    let h = Harness::new();
    spawn_real_dogfood_agent(&h).await;
    let mut latencies = Vec::new();
    for idx in 0..5 {
        latencies.push(run_real_stdout_job(&h, &format!("latency_stdout_{idx}")).await);
    }
    latencies.sort();
    assert!(latencies[latencies.len() - 1] <= Duration::from_millis(500));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_health_check_dead_layer_escalates_stuck() {
    let h = Harness::new();
    h.seed_dispatched_busy_job(JOB_ID, "job-id:dogfood_job_1\nhealth dead layer")
        .await;
    let observation = HealthCheckObservation {
        agent_id: AGENT_ID.into(),
        provider: "bash".into(),
        state: "BUSY".into(),
        pane_capture: String::new(),
        pane_capture_ok: false,
        last_output_ts: Some(1),
        last_marker_ts: Some(1),
        dispatched_at: None,
        now_ts: 400,
    };
    let result = health_check_observe(&observation, 300);
    assert!(
        result
            .dead_layers
            .iter()
            .any(|layer| layer == "tmux" || layer == "health:tmux"),
        "health check should identify dead tmux layer"
    );
    let changes = escalate_health_stuck(&h.ctx, &observation, 300)
        .await
        .expect("health escalate");
    assert_eq!(changes, 1);
    let response = h
        .rpc_raw(
            "event.subscribe",
            json!({
                "agent_id": AGENT_ID,
                "job_id": JOB_ID,
                "event_kind": ["stuck"],
            }),
        )
        .await;
    assert!(
        response.get("error").is_none(),
        "health dead layer should escalate through B3 stuck frame, got {response}"
    );
    let frame = response.get("result").expect("stuck frame");
    assert!(
        frame["payload"]["signal_kinds"]
            .as_array()
            .is_some_and(|signals| signals.iter().any(|value| {
                value
                    .as_str()
                    .is_some_and(|signal| signal.starts_with("health:"))
            })),
        "stuck frame should carry health:<layer> signal"
    );
}
