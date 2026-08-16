//! Executable incident proof for the 2026-07-08 stale-session kill cascade.
//!
//! Authoritative incident: `research/incident-stale-session-kill-cascade-2026-07-08.md`.
//!
//! These tests drive `session.kill` through the real RPC router (`rpc::router::dispatch`) —
//! the exact server-side entry point reached by `ah kill <id> --session` after CLI parse +
//! IPC transport — on a fully isolated stack (unique tmux socket via `TmuxServerGuard`,
//! tempdir state, file-backed temp DB). a1's regression tests call `handle_session_kill`
//! directly; this suite adds the router/CLI-path layer above them and reproduces the live
//! incident collision: a dead session whose stored tmux locators (master_pane_id /
//! master_session_name) or whose agent_id have been recycled by a *different live stack* on
//! the shared socket. Acceptance for every scenario: the live victim SURVIVES.
//!
//! ISOLATION: `TmuxServerGuard::new` derives a socket name from the per-test tempdir and its
//! `Drop` kills that socket + removes the socket file. The live victim tmux resources are
//! created on that same isolated socket, never the operator's real `ahd-*` socket.

mod common;

use ah::db::{self as db};
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use ah::tmux::{TmuxPaneId, agent_session_name, master_session_name};
use common::TmuxServerGuard;
use rusqlite::params;
use serde_json::{Value, json};

struct Stack {
    ctx: Ctx,
    guard: TmuxServerGuard,
    _db_file: tempfile::NamedTempFile,
    _state_dir: tempfile::TempDir,
}

impl Stack {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let guard = TmuxServerGuard::new(state_dir.path());
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: false,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: guard.server(),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        Self {
            ctx,
            guard,
            _db_file: db_file,
            _state_dir: state_dir,
        }
    }

    /// Drive `session.kill` through the router exactly as the daemon does for `ah kill --session`.
    async fn session_kill(&self, session_id: &str, force: bool) -> Value {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "session.kill",
            "params": { "session_id": session_id, "force": force },
            "id": 1,
        });
        let response: Value =
            serde_json::from_str(&dispatch(&request.to_string(), &self.ctx).await)
                .expect("dispatch response should be valid JSON");
        assert!(
            response.get("error").is_none(),
            "session.kill router error: {:?}",
            response.get("error")
        );
        response
            .get("result")
            .cloned()
            .expect("session.kill should return a result")
    }

    fn insert_session(&self, session_id: &str, project_id: &str) {
        let conn = self.ctx.db.conn();
        conn.execute(
            "INSERT OR IGNORE INTO projects (id, absolute_path) VALUES (?, ?)",
            params![project_id, format!("/tmp/e2e-a4/{project_id}")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, project_id, master_pid) VALUES (?, ?, 0)",
            params![session_id, project_id],
        )
        .unwrap();
    }

    fn session_status(&self, session_id: &str) -> String {
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

    async fn pane_pid(&self, pane: &TmuxPaneId) -> i32 {
        self.ctx
            .tmux_server
            .get_pane_pid(pane.clone())
            .await
            .unwrap()
    }

    async fn pane_alive(&self, pane: &TmuxPaneId) -> bool {
        self.ctx
            .tmux_server
            .get_pane_pid(pane.clone())
            .await
            .is_ok()
    }

    /// Spawn a real long-lived pane on the isolated socket and return (pane, live pid).
    async fn spawn_live_pane(&self, session_name: &str, window: &str) -> (TmuxPaneId, i32) {
        let cwd = self._state_dir.path().to_path_buf();
        self.ctx
            .tmux_server
            .ensure_session(session_name.to_string(), cwd.clone())
            .await
            .unwrap();
        let pane = self
            .ctx
            .tmux_server
            .spawn_window(
                session_name.to_string(),
                window.to_string(),
                cwd,
                vec!["sh".into(), "-lc".into(), "sleep 60".into()],
            )
            .await
            .unwrap();
        let pid = self.pane_pid(&pane).await;
        (pane, pid)
    }
}

fn require_tmux() {
    which::which("tmux").expect("tmux binary required for kill-path ownership e2e");
}

/// Scenario A (FIX-2): terminal session whose stored `master_pane_id` (and `master_session_name`)
/// collide with a LIVE master stack on the shared socket. `session.kill` must be pure DB/unit
/// cleanup and must NOT kill the recycled live pane/session.
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial(kill_path_ownership_e2e)]
async fn e2e_terminal_kill_spares_recycled_master_pane_and_session() {
    require_tmux();
    let stack = Stack::new();
    let project_id = "p_collide_master"; // shared project => master_session_name also collides
    let master_session = master_session_name(project_id);
    let (live_pane, live_pid) = stack.spawn_live_pane(&master_session, "live-master").await;

    // Dead session references the SAME project (so master_session_name collides) and stores the
    // LIVE pane id as its stale master_pane_id, with a bogus master_pid (the incident's %0 reuse).
    stack.insert_session("sess_dead_master", project_id);
    stack
        .ctx
        .db
        .conn()
        .execute(
            "UPDATE sessions SET status = 'FAILED', master_pane_id = ?1, master_pid = 111, \
             master_generation = 1 WHERE id = 'sess_dead_master'",
            params![live_pane.0],
        )
        .unwrap();

    let result = stack.session_kill("sess_dead_master", true).await;

    assert_eq!(result["state"], "KILLED");
    assert_eq!(
        result["master_pane_killed"], false,
        "terminal path must not report killing a master pane"
    );
    assert_eq!(stack.session_status("sess_dead_master"), "KILLED");
    // The live victim must survive untouched.
    assert!(
        stack.pane_alive(&live_pane).await,
        "recycled live master pane must survive terminal session kill"
    );
    assert_eq!(
        stack.pane_pid(&live_pane).await,
        live_pid,
        "live master pane pid must be unchanged"
    );
    assert!(
        stack
            .ctx
            .tmux_server
            .get_pane_pid(live_pane.clone())
            .await
            .is_ok()
    );

    let _ = stack.guard.server().kill_session(master_session).await;
}

/// Scenario B: dead session references a bare `agent_id` that a DIFFERENT live session has
/// recycled in the in-memory runtime registry. `session.kill` on the dead session must mark the
/// DB agent KILLED (DB-only) WITHOUT touching the live registry entry / live pane.
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial(kill_path_ownership_e2e)]
async fn e2e_terminal_kill_spares_recycled_agent_registry_entry() {
    use ah::agent_io::{AgentIoEntry, contains, register, remove};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    require_tmux();
    let stack = Stack::new();
    let agent_id = "a1";
    let live_agent_session = agent_session_name(agent_id);
    let (live_pane, live_pid) = stack
        .spawn_live_pane(&live_agent_session, "live-agent")
        .await;

    // Live agent registered under the bare agent_id, owned by a DIFFERENT (live) session.
    let fifo_path = stack._state_dir.path().join("pipes").join("a1.fifo");
    std::fs::create_dir_all(fifo_path.parent().unwrap()).unwrap();
    std::fs::write(&fifo_path, b"").unwrap();
    let reader_handle = tokio::spawn(async { std::future::pending::<()>().await });
    register(
        agent_id.to_string(),
        AgentIoEntry {
            session_id: "sess_live_registry".to_string(),
            pane_id: live_pane.clone(),
            expected_pid: Some(i64::from(live_pid)),
            reader_handle,
            fifo_path,
            socket_name: stack.ctx.tmux_server.socket_name().to_string(),
            idle_scan_enabled: Arc::new(AtomicBool::new(true)),
        },
    );

    // Dead session with a DB agent row that reuses the same bare agent_id.
    stack.insert_session("sess_dead_registry", "p_dead_registry");
    {
        let conn = stack.ctx.db.conn();
        conn.execute(
            "UPDATE sessions SET status = 'FAILED' WHERE id = 'sess_dead_registry'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agents (id, session_id, provider, state, pid) VALUES (?, ?, 'bash', 'IDLE', 1234)",
            params![agent_id, "sess_dead_registry"],
        )
        .unwrap();
    }

    let result = stack.session_kill("sess_dead_registry", true).await;

    assert_eq!(result["state"], "KILLED");
    assert_eq!(result["killed_agents"], 1);
    // Live victim survives: registry entry intact and pane still alive with same pid.
    assert!(contains(agent_id), "live registry entry must remain");
    assert!(
        stack.pane_alive(&live_pane).await,
        "recycled live agent pane must survive terminal session kill"
    );
    assert_eq!(stack.pane_pid(&live_pane).await, live_pid);

    let _ = remove(agent_id);
    let _ = stack.guard.server().kill_session(live_agent_session).await;
}

/// Scenario C (the #3 vector, at the router layer): an ACTIVE session whose recorded `master_pid`
/// no longer matches the live pane sitting under its `master_session_name` (pid reuse). The
/// ownership-guarded master session kill must find no matching pane and SKIP, sparing the live
/// master that reused the tmux session name.
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial(kill_path_ownership_e2e)]
async fn e2e_active_kill_spares_reused_master_session_without_pid_ownership() {
    require_tmux();
    let stack = Stack::new();
    let project_id = "p_active_reuse";
    let session_id = "sess_active_reuse";
    let master_session = master_session_name(project_id);
    let (live_pane, live_pid) = stack.spawn_live_pane(&master_session, "live-master").await;

    stack.insert_session(session_id, project_id);
    stack
        .ctx
        .db
        .conn()
        .execute(
            "UPDATE sessions SET status = 'ACTIVE', master_pid = ?1, master_generation = 1 \
             WHERE id = ?2",
            params![i64::from(live_pid) + 100_000, session_id],
        )
        .unwrap();

    let result = stack.session_kill(session_id, false).await;

    assert_eq!(result["state"], "KILLED");
    // The live master pane (recycled tmux session name, different pid) must survive.
    assert!(
        stack.pane_alive(&live_pane).await,
        "reused live master session must survive active session kill without pid ownership"
    );
    assert_eq!(stack.pane_pid(&live_pane).await, live_pid);

    let _ = stack.guard.server().kill_session(master_session).await;
}
