#![cfg(target_os = "linux")]
//! Issue #13 — RED contract tests for the config-fingerprint spawn/realign
//! asymmetry (respawn storm root cause) and the storm amplifiers.
//!
//! Authored by g1 (泳道1 gatekeeper) TESTS-FIRST. These are the frozen contract
//! for the GREEN implementation (g1-m1). DO NOT MODIFY this file as part of GREEN —
//! master + CI are its gate. Rationale + per-test intent: ISSUE-13-DESIGN-BOUNDARY.md §6.
//!
//! Every test drives real RPCs through the router against a real tmux server +
//! sqlite, provider `bash`, `unsafe_no_sandbox` (so the injection asymmetry
//! reproduces via AH_SOCKET + AH_* alone). Critically, each agent is spawned via
//! the REAL `agent.spawn` path so the stored `config_hash` is the *injected* one —
//! the gap existing fingerprint tests miss by seeding the raw hash directly.
//!
//! Run: CARGO_BUILD_JOBS=1 cargo test --test issue13_respawn_storm -- --test-threads=1

mod common;

use ah::db;
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use common::TmuxServerGuard;
use rusqlite::params;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MASTER_CMD: &str = "bash --noprofile --norc -i";
const AGENT_PROVIDER: &str = "bash";
/// ISSUE-13-DESIGN-BOUNDARY §4b: minimum inter-respawn stagger the storm-hardening
/// commit must introduce so a mass drift does not burst the single-threaded tmux server.
const MIN_RESPAWN_STAGGER_MS: u64 = 500;

type Env = HashMap<String, String>;

struct AgentDecl {
    id: String,
    env: Env,
}

impl AgentDecl {
    fn new(id: &str, env: Env) -> Self {
        Self {
            id: id.to_string(),
            env,
        }
    }
}

struct Harness {
    ctx: Ctx,
    session_id: String,
    _tmux_guard: TmuxServerGuard,
    _db_file: tempfile::NamedTempFile,
    _state_dir: tempfile::TempDir,
    _project_dir: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let project_dir = tempfile::TempDir::new().unwrap();
        let tmux_guard = TmuxServerGuard::new(state_dir.path());
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux_guard.server(),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let mut harness = Self {
            ctx,
            session_id: String::new(),
            _tmux_guard: tmux_guard,
            _db_file: db_file,
            _state_dir: state_dir,
            _project_dir: project_dir,
        };
        let created = harness
            .rpc(
                "session.create",
                json!({
                    "project_id": format!("i13p{suffix}"),
                    "absolute_path": harness._project_dir.path().display().to_string(),
                }),
            )
            .await;
        harness.session_id = created["session_id"]
            .as_str()
            .expect("session.create should return session_id")
            .to_string();
        harness
            .rpc(
                "session.spawn_master_pane",
                json!({ "session_id": harness.session_id, "cmd": MASTER_CMD }),
            )
            .await;
        harness
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        let request = json!({ "jsonrpc": "2.0", "method": method, "params": params, "id": "i13" });
        let response: Value =
            serde_json::from_str(&dispatch(&request.to_string(), &self.ctx).await)
                .expect("dispatch response should be valid JSON");
        if let Some(error) = response.get("error") {
            panic!("RPC {method} failed: {error}");
        }
        response
            .get("result")
            .cloned()
            .expect("dispatch response should include result")
    }

    /// Spawn an agent through the real `agent.spawn` path so the stored hash is the
    /// injected one. `config_env` is the project-level `[env]` (top-level, server merges).
    async fn spawn_agent(&self, agent_id: &str, agent_env: &Env, config_env: &Env) {
        self.rpc(
            "agent.spawn",
            json!({
                "session_id": self.session_id,
                "agent_id": agent_id,
                "provider": AGENT_PROVIDER,
                "extra_env_vars": agent_env,
                "config_env": config_env,
            }),
        )
        .await;
        self.wait_for_agent_state(agent_id, "IDLE", Duration::from_secs(15))
            .await;
    }

    async fn realign(&self, agents: &[AgentDecl], config_env: &Env) -> Value {
        let agent_values: Vec<Value> = agents
            .iter()
            .map(|a| {
                json!({
                    "agent_id": a.id,
                    "provider": AGENT_PROVIDER,
                    "env": a.env,
                    "hooks": {},
                    "plugins": [],
                })
            })
            .collect();
        self.rpc(
            "session.realign",
            json!({
                "session_id": self.session_id,
                "force": false,
                "config_env": config_env,
                "master": { "cmd": MASTER_CMD, "hooks": {}, "plugins": [] },
                "agents": agent_values,
            }),
        )
        .await
    }

    fn agent_state(&self, agent_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_default()
    }

    async fn wait_for_agent_state(&self, agent_id: &str, expected: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.agent_state(agent_id) == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "agent {agent_id} did not reach {expected}; last state = {}",
            self.agent_state(agent_id)
        );
    }
}

/// Pull a single agent's realign verdict out of the `session.realign` result, ignoring
/// the master entry. Statuses seen in production: NO_CHANGE / NEW / REALIGNED /
/// SKIPPED_BUSY / ORPHAN.
fn agent_status(result: &Value, agent_id: &str) -> String {
    let statuses = result
        .get("statuses")
        .or_else(|| result.get("results"))
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("realign result missing statuses/results array: {result}"));
    statuses
        .iter()
        .find(|entry| entry.get("agent_id").and_then(Value::as_str) == Some(agent_id))
        .and_then(|entry| entry.get("status").and_then(Value::as_str))
        .unwrap_or_else(|| panic!("no realign status for agent {agent_id} in {result}"))
        .to_string()
}

fn env_of(pairs: &[(&str, &str)]) -> Env {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// Commit A — core spawn/realign normalization contracts
// ---------------------------------------------------------------------------

/// CORE (Leak A): an agent whose declared config is unchanged must NOT drift. The
/// stored hash (from real spawn, over the INJECTED env: AH_SOCKET + AH_*) must equal
/// the realign hash (over the raw declared env). RED today: stored ≠ recomputed ⇒ every
/// first `ah up` force-respawns every agent.
#[tokio::test]
async fn spawn_then_realign_unchanged_config_is_no_change() {
    let h = Harness::new().await;
    let empty = Env::new();
    h.spawn_agent("a1", &empty, &empty).await;

    let result = h
        .realign(&[AgentDecl::new("a1", empty.clone())], &empty)
        .await;

    assert_eq!(
        agent_status(&result, "a1"),
        "NO_CHANGE",
        "unchanged declared config must NOT drift; injected identity vars (AH_SOCKET/AH_*) \
         must be excluded from the fingerprint on the spawn side. result={result}"
    );
}

/// LEAK B + server-side merge (o1 #4): the project-level `[env]` (config_env) must be
/// merged the SAME way on both spawn and realign (server-side), so an unchanged `[env]`
/// does not phantom-drift. RED today: config_env is dropped on the realign payload and
/// the injected asymmetry fires anyway.
#[tokio::test]
async fn unchanged_config_env_does_not_drift() {
    let h = Harness::new().await;
    let empty = Env::new();
    let config_env = env_of(&[("RUSTUP_HOME", "/opt/rustup")]);
    h.spawn_agent("a1", &empty, &config_env).await;

    let result = h
        .realign(&[AgentDecl::new("a1", empty.clone())], &config_env)
        .await;

    assert_eq!(
        agent_status(&result, "a1"),
        "NO_CHANGE",
        "project [env] must be server-merged consistently on both sides; an unchanged \
         [env] must not force a respawn. result={result}"
    );
}

/// OVER-CORRECTION GUARD: a real `agent.env` change must STILL be detected as drift.
/// Guards against the fix collapsing the fingerprint to "never drift".
#[tokio::test]
async fn agent_env_edit_still_drifts() {
    let h = Harness::new().await;
    let empty = Env::new();
    h.spawn_agent("a1", &empty, &empty).await;

    let result = h
        .realign(
            &[AgentDecl::new("a1", env_of(&[("USER_FLAG", "1")]))],
            &empty,
        )
        .await;

    assert_eq!(
        agent_status(&result, "a1"),
        "REALIGNED",
        "a genuine agent.env change must still drift-realign. result={result}"
    );
}

/// OVER-EXCLUSION GUARD: a real project `[env]` change must drift — config_env must
/// stay INSIDE the fingerprint. Guards against "fix" that silently drops config_env
/// (which would make the unchanged-case pass for the wrong reason).
#[tokio::test]
async fn config_env_edit_still_drifts() {
    let h = Harness::new().await;
    let empty = Env::new();
    h.spawn_agent("a1", &empty, &env_of(&[("SHARED", "v1")]))
        .await;

    let result = h
        .realign(
            &[AgentDecl::new("a1", empty.clone())],
            &env_of(&[("SHARED", "v2")]),
        )
        .await;

    assert_eq!(
        agent_status(&result, "a1"),
        "REALIGNED",
        "a genuine project [env] change must drift-realign; config_env must remain in \
         the fingerprint. result={result}"
    );
}

/// IDEMPOTENCY / single hash-writer guard (o1 #5): after a drift respawn, realigning
/// again with the same config must be NO_CHANGE. If the stored hash (written by the
/// spawn path) ever diverges from what realign recomputes, this becomes a permanent
/// respawn loop. GREEN today (consistent-by-clobber); it protects the §3c refactor
/// that removes realign's competing write.
#[tokio::test]
async fn realign_is_idempotent_no_drift_loop() {
    let h = Harness::new().await;
    let empty = Env::new();
    let edited = env_of(&[("EDIT", "1")]);
    h.spawn_agent("a1", &empty, &empty).await;

    let first = h
        .realign(&[AgentDecl::new("a1", edited.clone())], &empty)
        .await;
    assert_eq!(
        agent_status(&first, "a1"),
        "REALIGNED",
        "the edit must drift once. result={first}"
    );
    h.wait_for_agent_state("a1", "IDLE", Duration::from_secs(15))
        .await;

    let second = h
        .realign(&[AgentDecl::new("a1", edited.clone())], &empty)
        .await;
    assert_eq!(
        agent_status(&second, "a1"),
        "NO_CHANGE",
        "realign must be idempotent: after respawn the stored hash must equal the \
         recomputed hash (single hash-write authority), else an unbreakable drift loop. \
         result={second}"
    );
}

// ---------------------------------------------------------------------------
// Commit B — SIGKILL reaping (storm amplifier)
//
// The SIGKILL-reap contract (drift-realign must physically terminate the old process,
// not merely DB-mark it KILLED) is NOT anchored here on purpose. In this no-systemd /
// no-sandbox harness the agent process is a direct tmux child, and realign's respawn
// reuses the window in place (`respawn-window -k`, src/tmux/session.rs), so tmux reaps
// the old process regardless of any daemon-side SIGKILL — a test here would pass for
// harness-specific reasons and gate nothing. The orphan only manifests when the process
// outlives its pane (systemd scope). Its RED test therefore belongs in a systemd-scope
// e2e, authored alongside GREEN commit B. See ISSUE-13-DESIGN-BOUNDARY §4a.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Commit C — respawn stagger (storm amplifier)
// ---------------------------------------------------------------------------

/// STAGGER (o1 #3): when many agents drift at once (e.g. one project `[env]` edit),
/// the respawns must be spread over time, not fired in a tight burst, so the
/// single-threaded tmux server is not flooded with simultaneous pane creation. RED
/// today: no inter-respawn spacing exists. Observable contract: total realign wall-time
/// for K drifting agents is at least (K-1) * MIN_RESPAWN_STAGGER_MS.
#[tokio::test]
async fn simultaneous_drift_respawns_are_staggered() {
    let h = Harness::new().await;
    let empty = Env::new();
    let ids = ["a1", "a2", "a3", "a4", "a5"];
    for id in ids {
        h.spawn_agent(id, &empty, &empty).await;
    }

    let burst = env_of(&[("BURST", "1")]);
    let decls: Vec<AgentDecl> = ids
        .iter()
        .map(|id| AgentDecl::new(id, burst.clone()))
        .collect();

    let start = Instant::now();
    let result = h.realign(&decls, &empty).await;
    let elapsed = start.elapsed();

    for id in ids {
        assert_eq!(
            agent_status(&result, id),
            "REALIGNED",
            "every agent must drift-realign so the timing measures real respawns. result={result}"
        );
    }

    let floor = Duration::from_millis(MIN_RESPAWN_STAGGER_MS * (ids.len() as u64 - 1));
    assert!(
        elapsed >= floor,
        "simultaneous drift of {} agents must stagger respawns (>= {floor:?} total to \
         smooth the tmux pane-creation burst); realign took only {elapsed:?}",
        ids.len()
    );
}
