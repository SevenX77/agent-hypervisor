mod common;

use ah::db;
use ah::provider::extensions::{HookGroup, HookItem};
use ah::provider::fingerprint::{ConfigFingerprintInput, ConfigRole, compute_config_hash};
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use common::TmuxServerGuard;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MASTER_CMD: &str = "bash --noprofile --norc -i";
const AGENT_PROVIDER: &str = "bash";

#[test]
fn provider_settings_change_config_fingerprint() {
    let env = HashMap::new();
    let hooks = HashMap::new();
    let plugins = Vec::new();
    let skills = Vec::new();
    let empty_settings = serde_json::Map::new();
    let mut settings = serde_json::Map::new();
    settings.insert(
        "model".to_string(),
        Value::String("claude-sonnet-4-20250514".to_string()),
    );

    let without_settings = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Agent {
            provider: "claude",
            env: &env,
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &empty_settings,
        bundle: None,
    })
    .unwrap();
    let with_settings = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Agent {
            provider: "claude",
            env: &env,
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &settings,
        bundle: None,
    })
    .unwrap();

    assert_ne!(without_settings, with_settings);
}

struct Harness {
    ctx: Ctx,
    session_id: String,
    agent_a: String,
    agent_b: String,
    agent_new: String,
    _tmux_guard: TmuxServerGuard,
    _db_file: tempfile::NamedTempFile,
    _state_dir: tempfile::TempDir,
    _project_dir: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let project_dir = tempfile::TempDir::new().unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let session_id = format!("s4e{suffix}");
        let agent_a = format!("a1{suffix}");
        let agent_b = format!("a2{suffix}");
        let agent_new = format!("anew{suffix}");
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
        seed_base_rows(
            &ctx.db.conn(),
            project_dir.path(),
            &session_id,
            &agent_a,
            &agent_b,
        );
        Self {
            ctx,
            session_id,
            agent_a,
            agent_b,
            agent_new,
            _tmux_guard: tmux_guard,
            _db_file: db_file,
            _state_dir: state_dir,
            _project_dir: project_dir,
        }
    }
}

#[derive(Clone)]
struct MasterSpec {
    cmd: String,
    hooks: HashMap<String, Vec<HookGroup>>,
    plugins: Vec<String>,
    skills: Vec<String>,
}

impl MasterSpec {
    fn new(plugins: &[&str], hooks: HashMap<String, Vec<HookGroup>>) -> Self {
        Self {
            cmd: MASTER_CMD.to_string(),
            hooks,
            plugins: plugins.iter().map(|plugin| plugin.to_string()).collect(),
            skills: Vec::new(),
        }
    }

    fn hash(&self) -> String {
        compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Master {
                cmd: &self.cmd,
                env: &std::collections::HashMap::new(),
            },
            hooks: &self.hooks,
            plugins: &self.plugins,
            skills: &self.skills,
            settings: &serde_json::Map::new(),
            bundle: None,
        })
        .expect("PR4e fingerprint API should compute a deterministic master hash")
    }

    fn rpc_value(&self) -> Value {
        json!({
            "cmd": self.cmd,
            "hooks": self.hooks,
            "plugins": self.plugins,
            "skills": self.skills
        })
    }
}

#[derive(Clone)]
struct AgentSpec {
    agent_id: String,
    provider: String,
    env: HashMap<String, String>,
    hooks: HashMap<String, Vec<HookGroup>>,
    plugins: Vec<String>,
    skills: Vec<String>,
}

impl AgentSpec {
    fn new(agent_id: &str, plugins: &[&str], hooks: HashMap<String, Vec<HookGroup>>) -> Self {
        Self {
            agent_id: agent_id.to_string(),
            provider: AGENT_PROVIDER.to_string(),
            env: HashMap::new(),
            hooks,
            plugins: plugins.iter().map(|plugin| plugin.to_string()).collect(),
            skills: Vec::new(),
        }
    }

    fn hash(&self) -> String {
        compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Agent {
                provider: &self.provider,
                env: &self.env,
            },
            hooks: &self.hooks,
            plugins: &self.plugins,
            skills: &self.skills,
            settings: &serde_json::Map::new(),
            bundle: None,
        })
        .expect("PR4e fingerprint API should compute a deterministic agent hash")
    }

    fn rpc_value(&self) -> Value {
        json!({
            "agent_id": self.agent_id,
            "provider": self.provider,
            "env": self.env,
            "hooks": self.hooks,
            "plugins": self.plugins,
            "skills": self.skills
        })
    }
}

fn seed_base_rows(
    conn: &Connection,
    project_dir: &std::path::Path,
    session_id: &str,
    agent_a: &str,
    agent_b: &str,
) {
    conn.execute(
        "INSERT INTO projects (id, absolute_path) VALUES ('p4e', ?)",
        [project_dir.display().to_string()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (id, project_id, master_pid, master_pane_id) VALUES (?, 'p4e', 1, '%1')",
        params![session_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO agents (id, session_id, provider, state, pid) VALUES (?, ?, 'bash', 'IDLE', 11)",
        params![agent_a, session_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO agents (id, session_id, provider, state, pid) VALUES (?, ?, 'bash', 'IDLE', 12)",
        params![agent_b, session_id],
    )
    .unwrap();
}

fn set_session_hash(conn: &Connection, session_id: &str, hash: &str) {
    conn.execute(
        "UPDATE sessions SET config_hash = ? WHERE id = ?",
        params![hash, session_id],
    )
    .expect("PR4e must add sessions.config_hash and allow storing the running master fingerprint");
}

fn set_agent_hash(conn: &Connection, agent_id: &str, hash: &str) {
    conn.execute(
        "UPDATE agents SET config_hash = ? WHERE id = ?",
        params![hash, agent_id],
    )
    .expect("PR4e must add agents.config_hash and allow storing the running agent fingerprint");
}

fn set_agent_state(conn: &Connection, agent_id: &str, state: &str) {
    conn.execute(
        "UPDATE agents SET state = ?, state_version = state_version + 1 WHERE id = ?",
        params![state, agent_id],
    )
    .unwrap();
}

async fn session_realign(
    ctx: &Ctx,
    session_id: &str,
    force: bool,
    master: &MasterSpec,
    agents: &[AgentSpec],
) -> Value {
    let request = json!({
        "jsonrpc": "2.0",
        "id": "pr4e",
        "method": "session.realign",
        "params": {
            "session_id": session_id,
            "force": force,
            "master": master.rpc_value(),
            "agents": agents.iter().map(AgentSpec::rpc_value).collect::<Vec<_>>()
        }
    });
    assert!(
        request.to_string().find("expected_hash").is_none(),
        "PR4e safety contract: RPC input must be raw config only; server recomputes expected hash"
    );
    let response = dispatch(&request.to_string(), ctx).await;
    let value: Value = serde_json::from_str(&response).unwrap();
    assert!(
        value.get("error").is_none(),
        "session.realign should be registered and return a PR4e diff/alignment result, got {value}"
    );
    value["result"].clone()
}

async fn agent_realign(ctx: &Ctx, session_id: &str, force: bool, agent: &AgentSpec) -> Value {
    let request = json!({
        "jsonrpc": "2.0",
        "id": "pr4e-agent",
        "method": "agent.realign",
        "params": {
            "session_id": session_id,
            "force": force,
            "agent_id": agent.agent_id,
            "provider": agent.provider,
            "env": agent.env,
            "hooks": agent.hooks,
            "plugins": agent.plugins,
            "skills": agent.skills
        }
    });
    let response = dispatch(&request.to_string(), ctx).await;
    let value: Value = serde_json::from_str(&response).unwrap();
    assert!(
        value.get("error").is_none(),
        "agent.realign should not route through master diff, got {value}"
    );
    value["result"].clone()
}

fn command_hooks(command: &str) -> HashMap<String, Vec<HookGroup>> {
    HashMap::from([(
        "PreToolUse".to_string(),
        vec![HookGroup {
            matcher: "*".to_string(),
            hooks: vec![HookItem {
                hook_type: "command".to_string(),
                command: command.to_string(),
                timeout: None,
            }],
        }],
    )])
}

fn no_hooks() -> HashMap<String, Vec<HookGroup>> {
    HashMap::new()
}

fn result_text(value: &Value) -> String {
    value.to_string()
}

#[test]
fn skills_are_part_of_config_fingerprint() {
    let base = AgentSpec::new("a1", &[], no_hooks());
    let mut with_skill = base.clone();
    with_skill.skills.push("domain-review".to_string());
    assert_ne!(
        base.hash(),
        with_skill.hash(),
        "skills drift must change the agent config fingerprint"
    );

    let base_master = MasterSpec::new(&[], no_hooks());
    let mut master_with_skill = base_master.clone();
    master_with_skill.skills.push("master-domain".to_string());
    assert_ne!(
        base_master.hash(),
        master_with_skill.hash(),
        "master skills drift must change the master config fingerprint"
    );
}

fn event_count(conn: &Connection, agent_id: &str, event_type: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = ?",
        params![agent_id, event_type],
        |row| row.get(0),
    )
    .unwrap()
}

fn agent_state(conn: &Connection, agent_id: &str) -> String {
    conn.query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
        row.get(0)
    })
    .unwrap()
}

fn master_pane_id(conn: &Connection, session_id: &str) -> Option<String> {
    conn.query_row(
        "SELECT master_pane_id FROM sessions WHERE id = ?",
        [session_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn master_pid(conn: &Connection, session_id: &str) -> i64 {
    conn.query_row(
        "SELECT master_pid FROM sessions WHERE id = ?",
        [session_id],
        |row| row.get(0),
    )
    .unwrap()
}

async fn wait_for_agent_state(ctx: &Ctx, agent_id: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if agent_state(&ctx.db.conn(), agent_id) == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agent {agent_id} did not reach {expected}");
}

#[tokio::test]
async fn no_change_reports_up_to_date() {
    let harness = Harness::new();
    let master = MasterSpec::new(&[], no_hooks());
    let agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &master.hash());
        set_agent_hash(&conn, &harness.agent_a, &agent.hash());
    }

    let result = session_realign(&harness.ctx, &harness.session_id, false, &master, &[agent]).await;
    let text = result_text(&result);

    assert!(
        text.contains("NO_CHANGE") || text.contains("up to date"),
        "unchanged raw config must report NO_CHANGE/up-to-date after server-side hash recompute, got {text}"
    );
}

#[tokio::test]
async fn plugin_drift_realigns_agent() {
    let harness = Harness::new();
    let master = MasterSpec::new(&[], no_hooks());
    let old_agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    let new_agent = AgentSpec::new(&harness.agent_a, &["github@openai-curated"], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &master.hash());
        set_agent_hash(&conn, &harness.agent_a, &old_agent.hash());
    }

    let result = session_realign(
        &harness.ctx,
        &harness.session_id,
        false,
        &master,
        &[new_agent],
    )
    .await;
    let text = result_text(&result);

    assert!(
        text.contains("DRIFT"),
        "plugin change must report DRIFT: {text}"
    );
    assert!(
        text.contains("plugins changed"),
        "plugin change must explain the field-level drift: {text}"
    );
    assert!(
        text.contains("drift_realigned") || text.contains("REALIGNED"),
        "non-BUSY plugin drift should trigger agent.realign and commit a new hash: {text}"
    );
    wait_for_agent_state(&harness.ctx, &harness.agent_a, "IDLE").await;
    let conn = harness.ctx.db.conn();
    assert!(
        event_count(&conn, &harness.agent_a, "agent_killed") >= 1,
        "Stage 3 must record a physical agent kill for plugin drift"
    );
    assert!(
        event_count(&conn, &harness.agent_a, "agent_spawned") >= 1,
        "Stage 4 must record a physical agent spawn for plugin drift"
    );
}

#[tokio::test]
async fn hook_drift_realigns_agent() {
    let harness = Harness::new();
    let master = MasterSpec::new(&[], no_hooks());
    let old_agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    let new_agent = AgentSpec::new(&harness.agent_a, &[], command_hooks("echo checked"));
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &master.hash());
        set_agent_hash(&conn, &harness.agent_a, &old_agent.hash());
    }

    let result = session_realign(
        &harness.ctx,
        &harness.session_id,
        false,
        &master,
        &[new_agent],
    )
    .await;
    let text = result_text(&result);

    assert!(
        text.contains("DRIFT"),
        "hook change must report DRIFT: {text}"
    );
    assert!(
        text.contains("hooks changed"),
        "hook change must explain the field-level drift: {text}"
    );
    assert!(
        text.contains("drift_realigned") || text.contains("REALIGNED"),
        "non-BUSY hook drift should trigger agent.realign and commit a new hash: {text}"
    );
    wait_for_agent_state(&harness.ctx, &harness.agent_a, "IDLE").await;
    let conn = harness.ctx.db.conn();
    assert!(
        event_count(&conn, &harness.agent_a, "agent_killed") >= 1,
        "Stage 3 must record a physical agent kill for hook drift"
    );
    assert!(
        event_count(&conn, &harness.agent_a, "agent_spawned") >= 1,
        "Stage 4 must record a physical agent spawn for hook drift"
    );
}

#[tokio::test]
async fn orphan_agent_is_reported() {
    let harness = Harness::new();
    let master = MasterSpec::new(&[], no_hooks());
    let agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    let orphan = AgentSpec::new(&harness.agent_b, &[], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &master.hash());
        set_agent_hash(&conn, &harness.agent_a, &agent.hash());
        set_agent_hash(&conn, &harness.agent_b, &orphan.hash());
    }

    let result = session_realign(&harness.ctx, &harness.session_id, false, &master, &[agent]).await;
    let text = result_text(&result);

    assert!(
        text.contains("ORPHAN"),
        "deleted config agent must be reported as ORPHAN: {text}"
    );
    assert!(
        text.contains(&harness.agent_b),
        "ORPHAN report must identify the DB-only agent id: {text}"
    );
    let state: String = harness
        .ctx
        .db
        .conn()
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            [harness.agent_b.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(state, "KILLED", "ORPHAN must be audit-only without --force");
}

#[tokio::test]
async fn busy_agent_skip_and_force_realign() {
    let harness = Harness::new();
    let master = MasterSpec::new(&[], no_hooks());
    let old_agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    let new_agent = AgentSpec::new(&harness.agent_a, &["github@openai-curated"], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &master.hash());
        set_agent_hash(&conn, &harness.agent_a, &old_agent.hash());
        set_agent_state(&conn, &harness.agent_a, "BUSY");
    }

    let skipped = session_realign(
        &harness.ctx,
        &harness.session_id,
        false,
        &master,
        std::slice::from_ref(&new_agent),
    )
    .await;
    let skipped_text = result_text(&skipped);
    assert!(
        skipped_text.contains("SKIPPED_BUSY"),
        "BUSY drift must be skipped without --force: {skipped_text}"
    );
    {
        let conn = harness.ctx.db.conn();
        assert_eq!(
            event_count(&conn, &harness.agent_a, "agent_killed"),
            0,
            "BUSY audit path must not kill without --force"
        );
        assert_eq!(
            event_count(&conn, &harness.agent_a, "agent_spawned"),
            0,
            "BUSY audit path must not spawn without --force"
        );
    }

    let forced = session_realign(
        &harness.ctx,
        &harness.session_id,
        true,
        &master,
        &[new_agent],
    )
    .await;
    let forced_text = result_text(&forced);
    assert!(
        forced_text.contains("drift_realigned") || forced_text.contains("REALIGNED"),
        "--force must allow BUSY agent kill + full rebuild: {forced_text}"
    );
    wait_for_agent_state(&harness.ctx, &harness.agent_a, "IDLE").await;
    let conn = harness.ctx.db.conn();
    assert!(
        event_count(&conn, &harness.agent_a, "agent_killed") >= 1,
        "BUSY --force must execute Stage 3 kill"
    );
    assert!(
        event_count(&conn, &harness.agent_a, "agent_spawned") >= 1,
        "BUSY --force must execute Stage 4 spawn"
    );
}

#[tokio::test]
async fn master_drift_audit_only_by_default() {
    let harness = Harness::new();
    let old_master = MasterSpec::new(&[], no_hooks());
    let new_master = MasterSpec::new(&["github@openai-curated"], no_hooks());
    let agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &old_master.hash());
        set_agent_hash(&conn, &harness.agent_a, &agent.hash());
    }

    let result = session_realign(
        &harness.ctx,
        &harness.session_id,
        false,
        &new_master,
        &[agent],
    )
    .await;
    let text = result_text(&result);

    assert!(
        text.contains("DRIFT"),
        "master hash mismatch must report DRIFT: {text}"
    );
    assert!(
        text.contains("master") || text.contains("session"),
        "master drift report must identify the master/session role: {text}"
    );
    assert!(
        !text.contains("master_realign") && !text.contains("REALIGNED"),
        "master drift must be audit-only by default, got {text}"
    );
    let conn = harness.ctx.db.conn();
    assert_eq!(
        master_pane_id(&conn, &harness.session_id).as_deref(),
        Some("%1"),
        "master audit-only drift must not replace the master pane"
    );
    assert_eq!(
        event_count(&conn, &harness.agent_a, "agent_killed"),
        0,
        "master audit-only drift must not kill agents"
    );
    assert_eq!(
        event_count(&conn, &harness.agent_a, "agent_spawned"),
        0,
        "master audit-only drift must not spawn agents"
    );
}

#[tokio::test]
async fn master_drift_force_triggers_realign() {
    let harness = Harness::new();
    let old_master = MasterSpec::new(&[], no_hooks());
    let new_master = MasterSpec::new(&["github@openai-curated"], no_hooks());
    let agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &old_master.hash());
        set_agent_hash(&conn, &harness.agent_a, &agent.hash());
    }

    let result = session_realign(
        &harness.ctx,
        &harness.session_id,
        true,
        &new_master,
        &[agent],
    )
    .await;
    let text = result_text(&result);

    assert!(
        text.contains("master_realign") || text.contains("REALIGNED"),
        "--force must trigger full master pane realign when master hash drifts: {text}"
    );
    let conn = harness.ctx.db.conn();
    assert_ne!(
        master_pid(&conn, &harness.session_id),
        1,
        "master --force must physically spawn a replacement master process"
    );
}

#[tokio::test]
async fn test_agent_realign_does_not_trigger_master_force() {
    let harness = Harness::new();
    let old_master = MasterSpec::new(&[], no_hooks());
    let old_agent = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    let new_agent = AgentSpec::new(&harness.agent_a, &["github@openai-curated"], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &old_master.hash());
        set_agent_hash(&conn, &harness.agent_a, &old_agent.hash());
    }

    let result = agent_realign(&harness.ctx, &harness.session_id, true, &new_agent).await;
    let text = result_text(&result);

    assert!(
        text.contains("REALIGNED"),
        "agent.realign force should realign the target agent: {text}"
    );
    wait_for_agent_state(&harness.ctx, &harness.agent_a, "IDLE").await;
    let conn = harness.ctx.db.conn();
    assert_eq!(
        master_pane_id(&conn, &harness.session_id).as_deref(),
        Some("%1"),
        "agent.realign force must not stop or respawn the master pane"
    );
    assert!(
        event_count(&conn, &harness.agent_a, "agent_killed") >= 1,
        "agent.realign force must still execute agent Stage 3"
    );
    assert!(
        event_count(&conn, &harness.agent_a, "agent_spawned") >= 1,
        "agent.realign force must still execute agent Stage 4"
    );
}

#[tokio::test]
async fn new_agent_is_spawned() {
    let harness = Harness::new();
    let master = MasterSpec::new(&[], no_hooks());
    let existing = AgentSpec::new(&harness.agent_a, &[], no_hooks());
    let new_agent = AgentSpec::new(&harness.agent_new, &["github@openai-curated"], no_hooks());
    {
        let conn = harness.ctx.db.conn();
        set_session_hash(&conn, &harness.session_id, &master.hash());
        set_agent_hash(&conn, &harness.agent_a, &existing.hash());
    }

    let result = session_realign(
        &harness.ctx,
        &harness.session_id,
        false,
        &master,
        &[existing, new_agent],
    )
    .await;
    let text = result_text(&result);

    assert!(
        text.contains("NEW"),
        "config-only agent block must be classified as NEW: {text}"
    );
    assert!(
        text.contains(&harness.agent_new),
        "NEW report must identify the config-only agent id: {text}"
    );
    assert!(
        text.contains("spawned") || text.contains("SPAWNED"),
        "NEW agent should spawn through agent.spawn instead of realign/kill: {text}"
    );
    wait_for_agent_state(&harness.ctx, &harness.agent_new, "IDLE").await;
    let conn = harness.ctx.db.conn();
    assert_eq!(
        agent_state(&conn, &harness.agent_new),
        "IDLE",
        "NEW agent must not remain stuck in SPAWNING"
    );
    assert!(
        event_count(&conn, &harness.agent_new, "agent_spawned") >= 1,
        "NEW agent must execute the physical spawn path"
    );
    assert_eq!(
        event_count(&conn, &harness.agent_new, "agent_killed"),
        0,
        "NEW agent must not go through kill/realign"
    );
}
