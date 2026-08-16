//! Executable end-to-end wire-format proof for state-contract PR1
//! (RuntimeSnapshot `schema_version` 3). This is the JSON surface Studio actually
//! consumes: `ah events --format json` reaches server method `runtime.subscribe`,
//! which serializes the exact same `RuntimeSnapshot` struct produced by
//! `build_runtime_snapshot`. These tests drive BOTH real emission paths on a fully
//! isolated stack:
//!   1. `rpc::router::dispatch("runtime.subscribe", ..)` — the router entry the daemon
//!      reaches after CLI parse + IPC transport (same path as tests/e2e_kill_path_ownership_a4.rs).
//!   2. `rpc::handlers::stream_runtime_subscribe` — the streaming NDJSON path whose exact
//!      bytes `ah events --format json` prints to Studio.
//!
//! ISOLATION: each `Stack` owns a file-backed temp DB (`AH_STATE_DIR`-independent),
//! a tempdir state dir, and a unique `tmux -L <socket>` via `TmuxServerGuard` (Drop kills
//! that socket + removes the socket file). No tmux/pid/DB resource ever touches the live
//! operator stack. Tests that create no tmux resources still get a fresh isolated socket.
//!
//! NON-TAUTOLOGY: the migration test asserts a field FLIPS (FAILED -> CLOSED) only after a
//! restart runs `db::init`; a pre-PR1 build (no migration) would leave it FAILED. The wire
//! test pins `"schema_version":3`, provider-neutral status, and the RENAME
//! (`db_tracked_agents` present, legacy `active_agents` absent) against the raw
//! serialized bytes — a pre-Work-Execution snapshot would fail these checks.

mod common;

use ah::db;
use ah::rpc::Ctx;
use ah::rpc::handlers::stream_runtime_subscribe;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use ah::tmux::{TmuxPaneId, TmuxServer, agent_session_name};
use common::TmuxServerGuard;
use rusqlite::params;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

fn build_ctx(db_path: &Path, state_dir: &Path, tmux: Arc<TmuxServer>) -> Ctx {
    Ctx {
        db: db::init(db_path).unwrap(),
        state_dir: state_dir.to_path_buf(),
        env_state: EnvState {
            systemd_run_available: false,
            unsafe_no_sandbox: false,
            under_systemd: false,
        },
        daemon_unit: None,
        tmux_server: tmux,
        claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
    }
}

struct Stack {
    ctx: Ctx,
    guard: TmuxServerGuard,
    db_path: PathBuf,
    state_dir: PathBuf,
    _db_file: tempfile::NamedTempFile,
    _state_dir_guard: tempfile::TempDir,
}

impl Stack {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir_guard = tempfile::TempDir::new().unwrap();
        let state_dir = state_dir_guard.path().to_path_buf();
        let db_path = db_file.path().to_path_buf();
        let guard = TmuxServerGuard::new(&state_dir);
        let ctx = build_ctx(&db_path, &state_dir, guard.server());
        Self {
            ctx,
            guard,
            db_path,
            state_dir,
            _db_file: db_file,
            _state_dir_guard: state_dir_guard,
        }
    }

    /// Simulate a daemon (re)start: re-run `db::init` on the SAME file so the PR1
    /// FAILED+IDLE_MASTER_EXIT -> CLOSED backfill migration executes exactly as it does
    /// when a real daemon boots against an existing DB.
    fn restart(&mut self) {
        self.ctx = build_ctx(&self.db_path, &self.state_dir, self.guard.server());
    }

    /// The router entry `ah` reaches for `runtime.subscribe` after IPC. Returns the
    /// serialized snapshot (JSON-RPC `result`), i.e. the wire object Studio parses.
    async fn subscribe_snapshot(&self) -> Value {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "runtime.subscribe",
            "params": {},
            "id": 1,
        });
        let response: Value =
            serde_json::from_str(&dispatch(&request.to_string(), &self.ctx).await)
                .expect("dispatch response should be valid JSON");
        assert!(
            response.get("error").is_none(),
            "runtime.subscribe router error: {:?}",
            response.get("error")
        );
        response
            .get("result")
            .cloned()
            .expect("runtime.subscribe should return a result")
    }

    /// Capture the EXACT NDJSON bytes `ah events --format json` prints: run the streaming
    /// subscribe long enough to emit its initial snapshot line, then parse that first line.
    async fn stream_first_line(&self) -> Value {
        let params = json!({ "interval_ms": 60_000 });
        let mut buf: Vec<u8> = Vec::new();
        // stream_runtime_subscribe never returns (it loops); the initial snapshot is written
        // before the loop, so a short timeout captures exactly that first line.
        let _ = tokio::time::timeout(
            Duration::from_millis(500),
            stream_runtime_subscribe(params, &self.ctx, &mut buf),
        )
        .await;
        let text = String::from_utf8(buf).expect("stream bytes should be utf8");
        let first = text
            .lines()
            .find(|line| !line.trim().is_empty())
            .expect("streaming subscribe should emit at least one snapshot line");
        serde_json::from_str(first).expect("streamed snapshot line should be valid JSON")
    }

    fn seed_session(
        &self,
        session_id: &str,
        project_id: &str,
        status: &str,
        master_pid: i64,
        exit_reason: Option<&str>,
    ) {
        let conn = self.ctx.db.conn();
        conn.execute(
            "INSERT OR IGNORE INTO projects (id, absolute_path) VALUES (?, ?)",
            params![project_id, format!("/tmp/e2e-a4-pr1/{project_id}")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, project_id, master_pid, status, master_last_exit_reason) \
             VALUES (?, ?, ?, ?, ?)",
            params![session_id, project_id, master_pid, status, exit_reason],
        )
        .unwrap();
    }

    fn seed_agent(&self, agent_id: &str, session_id: &str, state: &str, pid: Option<i64>) {
        let lifecycle_id = format!("lifecycle_{agent_id}");
        self.ctx
            .db
            .conn()
            .execute(
                "INSERT INTO agents (id, session_id, provider, lifecycle_id, state, pid) VALUES (?, ?, 'bash', ?, ?, ?)",
                params![agent_id, session_id, lifecycle_id, state, pid],
            )
            .unwrap();
    }

    fn seed_job(&self, job_id: &str, agent_id: &str) {
        self.ctx
            .db
            .conn()
            .execute(
                "INSERT INTO jobs (id, agent_id, prompt_text, status) VALUES (?, ?, 'do the thing', 'QUEUED')",
                params![job_id, agent_id],
            )
            .unwrap();
    }

    fn db_status(&self, session_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT status FROM sessions WHERE id = ?",
                params![session_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    async fn spawn_live_agent_pane(&self, agent_id: &str) -> (TmuxPaneId, i32) {
        let session_name = agent_session_name(agent_id);
        let cwd = self.state_dir.clone();
        self.ctx
            .tmux_server
            .ensure_session(session_name.clone(), cwd.clone())
            .await
            .unwrap();
        let pane = self
            .ctx
            .tmux_server
            .spawn_window(
                session_name,
                "live".to_string(),
                cwd,
                vec!["sh".into(), "-lc".into(), "sleep 60".into()],
            )
            .await
            .unwrap();
        let pid = self
            .ctx
            .tmux_server
            .get_pane_pid(pane.clone())
            .await
            .unwrap();
        (pane, pid)
    }
}

fn session_by_id<'a>(snapshot: &'a Value, session_id: &str) -> &'a Value {
    snapshot["sessions"]
        .as_array()
        .expect("sessions must be an array")
        .iter()
        .find(|session| session["session_id"] == session_id)
        .unwrap_or_else(|| panic!("session {session_id} missing from snapshot: {snapshot}"))
}

/// (1) WIRE-FORMAT CONTRACT: schema_version 3, provider-neutral status, the
/// renamed/added per-session keys, and the top-level Job receipt containers —
/// asserted against the raw serialized JSON of BOTH emission paths
/// (`runtime.subscribe` router + `ah events` streaming bytes).
#[tokio::test(flavor = "current_thread")]
async fn e2e_wire_format_pins_schema_v3_and_work_execution_keys() {
    let stack = Stack::new();
    stack.seed_session("s_wire", "p_wire", "ACTIVE", 0, None);
    stack.seed_agent("a_wire", "s_wire", "IDLE", None);
    stack.seed_job("j_wire", "a_wire");

    let snap = stack.subscribe_snapshot().await;

    // schema_version is exactly 3 (pre-Work-Execution snapshots emitted 1 or 2).
    assert_eq!(snap["schema_version"].as_u64(), Some(3), "snapshot: {snap}");
    assert!(!matches!(snap["schema_version"].as_u64(), Some(1 | 2)));

    // Top-level PR1 job containers exist with the right JSON types.
    assert!(snap["jobs"].is_array(), "jobs must be an array: {snap}");
    assert!(snap["job_events"].is_array(), "job_events must be an array");
    assert_eq!(
        snap["job_events"].as_array().unwrap().len(),
        0,
        "job_events emission is PR2; empty array is the correct PR1 shape"
    );
    assert!(
        snap["job_event_cursor"].is_i64() || snap["job_event_cursor"].is_u64(),
        "job_event_cursor must be an integer: {snap}"
    );
    assert_eq!(snap["job_event_cursor"].as_i64(), Some(0));
    assert!(
        !snap["jobs"].as_array().unwrap().is_empty(),
        "seeded QUEUED job should surface in jobs[]"
    );

    // Per-session object carries the new/renamed keys, and NOT the legacy `active_agents`.
    let sess = session_by_id(&snap, "s_wire");
    for key in [
        "status",
        "master_last_exit_reason",
        "db_tracked_agents",
        "live_agents",
        "cleanup_required",
        "safe_to_cleanup",
    ] {
        assert!(
            sess.get(key).is_some(),
            "session object missing new key `{key}`: {sess}"
        );
    }
    assert!(
        sess.get("active_agents").is_none(),
        "renamed away: session must NOT carry legacy `active_agents`: {sess}"
    );
    assert_eq!(
        sess["db_tracked_agents"].as_i64(),
        Some(1),
        "one non-terminal agent should be db_tracked"
    );
    assert!(sess["live_agents"].is_i64() || sess["live_agents"].is_u64());
    assert!(sess["cleanup_required"].is_boolean());
    assert!(sess["safe_to_cleanup"].is_boolean());

    let agent = snap["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|agent| agent["agent_id"] == "a_wire")
        .unwrap_or_else(|| panic!("agent a_wire missing from snapshot: {snap}"));
    let provider_status = &agent["provider_status"];
    assert_eq!(provider_status["agent_id"], "a_wire");
    assert_eq!(provider_status["provider"], "bash");
    assert!(
        provider_status["lifecycle_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert!(provider_status["process"]["resolution"].is_string());
    assert!(provider_status["turn"]["resolution"].is_string());
    assert!(provider_status["occupancy"].is_string());

    let job = snap["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["job_id"] == "j_wire")
        .unwrap_or_else(|| panic!("job j_wire missing from snapshot: {snap}"));
    assert_eq!(job["provider"], "bash");
    assert!(job.get("governance_binding_json").is_some());

    // Raw-bytes non-tautology: pin the literal wire tokens a v2 snapshot could not produce.
    let wire = serde_json::to_string(&snap).unwrap();
    assert!(
        wire.contains("\"schema_version\":3"),
        "wire bytes must pin schema_version 3: {wire}"
    );
    assert!(wire.contains("provider_status"));
    assert!(wire.contains("governance_binding_json"));
    assert!(
        wire.contains("db_tracked_agents"),
        "renamed key must appear in wire bytes"
    );
    assert!(
        !wire.contains("active_agents"),
        "pre-PR1 key `active_agents` must be gone from wire bytes: {wire}"
    );

    // Cross-check: the EXACT streaming bytes `ah events --format json` prints carry the same contract.
    let streamed = stack.stream_first_line().await;
    assert_eq!(
        streamed["schema_version"].as_u64(),
        Some(3),
        "streaming path must also emit schema_version 3: {streamed}"
    );
    let streamed_sess = session_by_id(&streamed, "s_wire");
    assert!(
        streamed_sess.get("db_tracked_agents").is_some(),
        "streaming session object must carry db_tracked_agents"
    );
    assert!(
        streamed_sess.get("active_agents").is_none(),
        "streaming session object must not carry active_agents"
    );
    assert!(streamed["job_events"].is_array());
    assert!(streamed["job_event_cursor"].is_i64() || streamed["job_event_cursor"].is_u64());
    assert!(streamed["agents"][0].get("provider_status").is_some());
}

/// (2) MIGRATION END-TO-END: a FAILED session whose master_last_exit_reason is
/// IDLE_MASTER_EXIT must read CLOSED in the live snapshot AFTER a daemon restart runs
/// `db::init`; a differently-reasoned FAILED session must stay FAILED. The flip proves the
/// migration ran (non-tautological: same row is FAILED before restart, CLOSED after).
#[tokio::test(flavor = "current_thread")]
async fn e2e_migration_failed_idle_exit_to_closed_on_restart() {
    let mut stack = Stack::new();
    stack.seed_session("s_idle", "p_idle", "FAILED", 0, Some("IDLE_MASTER_EXIT"));
    // Control: FAILED but NOT an idle exit -> migration must leave it FAILED.
    stack.seed_session("s_crash", "p_crash", "FAILED", 0, Some("OOM_OR_CRASH"));

    // Before restart: the seeded rows are untouched (this init did not see them).
    assert_eq!(stack.db_status("s_idle"), "FAILED");
    let before = stack.subscribe_snapshot().await;
    assert_eq!(
        session_by_id(&before, "s_idle")["status"],
        "FAILED",
        "pre-restart the idle-exit session must still read FAILED (migration not yet run)"
    );
    assert_eq!(session_by_id(&before, "s_crash")["status"], "FAILED");

    // Restart -> db::init runs migrate_sessions_failed_idle_to_closed.
    stack.restart();

    let after = stack.subscribe_snapshot().await;
    let idle = session_by_id(&after, "s_idle");
    assert_eq!(
        idle["status"], "CLOSED",
        "FAILED + IDLE_MASTER_EXIT must migrate to CLOSED after restart: {idle}"
    );
    assert_eq!(
        idle["master_last_exit_reason"], "IDLE_MASTER_EXIT",
        "migration must preserve master_last_exit_reason"
    );
    assert_eq!(
        stack.db_status("s_idle"),
        "CLOSED",
        "DB row must be CLOSED too"
    );
    // Targeted: the non-idle crash stays FAILED.
    assert_eq!(
        session_by_id(&after, "s_crash")["status"],
        "FAILED",
        "non-idle FAILED session must NOT be migrated"
    );
}

/// (3) CLEANUP-SIGNAL SANITY — cleanup_required == true for a terminal session with a
/// genuinely LIVE residual worker (real tmux pane on the isolated socket + live pid).
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial(state_contract_pr1_tmux)]
async fn e2e_cleanup_required_true_for_terminal_with_live_residual() {
    which::which("tmux").expect("tmux required for cleanup-required residual e2e");
    let stack = Stack::new();
    let (live_pane, live_pid) = stack.spawn_live_agent_pane("a_resid").await;

    // Terminal (FAILED) session that still owns a live agent worker (tmux + pid residual).
    stack.seed_session("s_resid", "p_resid", "FAILED", 0, None);
    stack.seed_agent("a_resid", "s_resid", "IDLE", Some(i64::from(live_pid)));

    let snap = stack.subscribe_snapshot().await;
    let sess = session_by_id(&snap, "s_resid");
    assert_eq!(
        sess["cleanup_required"], true,
        "terminal session with a live residual worker must require cleanup: {sess}"
    );
    assert_eq!(
        sess["live_agents"].as_i64(),
        Some(1),
        "the live tmux worker must be counted as a live agent (proves live-tmux detection)"
    );

    let _ = stack
        .guard
        .server()
        .kill_session(agent_session_name("a_resid"))
        .await;
    let _ = live_pane; // pane dies with the killed session
}

/// (5) PROBE-TRUTH (#53) — after an abnormal master death the DB session goes terminal
/// (no ACTIVE inventory) while the runtime's own tmux server is demonstrably alive holding
/// the remain-on-exit dead pane. `tmux_server_alive` must reflect a live probe of the
/// runtime's own socket, not inventory bookkeeping — gating the probe on ACTIVE sessions
/// made the forensic state remain-on-exit deliberately keeps unrepresentable to hosts.
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial(state_contract_pr1_tmux)]
async fn e2e_tmux_server_alive_is_probe_truth_without_active_inventory() {
    which::which("tmux").expect("tmux required for probe-truth e2e");
    let stack = Stack::new();
    // A live pane on the runtime's own socket keeps the server alive — the same
    // server-with-residue shape an abnormal master death leaves behind.
    let (_pane, _pid) = stack.spawn_live_agent_pane("a_probe").await;
    stack.seed_session("s_probe", "p_probe", "FAILED", 0, None);

    let snap = stack.subscribe_snapshot().await;
    assert_eq!(
        snap["tmux_server_alive"], true,
        "the server holds a pane on its own socket; the snapshot must say so: {snap}"
    );

    let _ = stack
        .guard
        .server()
        .kill_session(agent_session_name("a_probe"))
        .await;
}

/// (4) CLEANUP-SIGNAL SANITY — an ACTIVE session yields cleanup_required == false &&
/// safe_to_cleanup == false; a terminal session with no residual and no recovery window
/// yields safe_to_cleanup == true (the drivable safe_to_cleanup=true case). No tmux needed.
#[tokio::test(flavor = "current_thread")]
async fn e2e_cleanup_signals_active_false_and_terminal_safe() {
    let stack = Stack::new();
    stack.seed_session("s_active", "p_active", "ACTIVE", 0, None);
    stack.seed_session("s_safe", "p_safe", "FAILED", 0, None);

    let snap = stack.subscribe_snapshot().await;

    let active = session_by_id(&snap, "s_active");
    assert_eq!(
        active["cleanup_required"], false,
        "ACTIVE session must not require cleanup: {active}"
    );
    assert_eq!(
        active["safe_to_cleanup"], false,
        "ACTIVE session is never safe_to_cleanup: {active}"
    );

    let safe = session_by_id(&snap, "s_safe");
    assert_eq!(
        safe["cleanup_required"], false,
        "terminal session with no residual has nothing to clean: {safe}"
    );
    assert_eq!(
        safe["safe_to_cleanup"], true,
        "terminal + no recovery window => safe_to_cleanup: {safe}"
    );
}
