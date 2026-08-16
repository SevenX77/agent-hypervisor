#![cfg(target_os = "linux")]
#![allow(dead_code)]

// PR-3 Grand Tour: cargo test --test ah_full_e2e_realign_extra -- --include-ignored --test-threads=1
// PR-3 scope: ORPHAN + BUSY + ERROR (crash detection + recovery known-gap)
// PR-4 future: ERROR recovery src fix
// PR-5+ future: master cmd drift, session-level aggregate status

mod common;

use ah::db;
use ah::provider::extensions::{HookGroup, HookItem};
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use ah::tmux::agent_session_name;
use common::TmuxServerGuard;
use rusqlite::params;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const PROJECT_ID: &str = "ah_full_e2e_pr3_project";
const AGENT_A1: &str = "a1";
const AGENT_A2: &str = "a2";
const AGENT_CRASH: &str = "a_crash";
const MASTER_CMD: &str = "bash --noprofile --norc -i";
const AGENT_PROVIDER: &str = "claude";
const MOCK_ECHO: &str = "ECHO";
const MOCK_BUSY: &str = "BUSY";
const MOCK_CRASH: &str = "CRASH";

struct Harness {
    ctx: Ctx,
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
        let tmux_guard = TmuxServerGuard::new(state_dir.path());
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: false,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: tmux_guard.server(),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        Self {
            ctx,
            _tmux_guard: tmux_guard,
            _db_file: db_file,
            _state_dir: state_dir,
            _project_dir: project_dir,
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        let response = self.rpc_raw(method, params).await;
        if let Some(error) = response.get("error") {
            panic!("RPC {method} failed: {error}");
        }
        response
            .get("result")
            .cloned()
            .expect("dispatch response should include result")
    }

    async fn rpc_raw(&self, method: &str, params: Value) -> Value {
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": "pr3",
        });
        serde_json::from_str(&dispatch(&request.to_string(), &self.ctx).await)
            .expect("dispatch response should be valid JSON")
    }

    fn project_dir(&self) -> &Path {
        self._project_dir.path()
    }

    fn sandbox_path(&self, session_id: &str, agent_id: &str, sub_path: &str) -> PathBuf {
        if sub_path.starts_with(".claude/")
            || sub_path.starts_with(".gemini/")
            || sub_path.starts_with(".codex/")
        {
            return self.provider_home_path(session_id, agent_id).join(sub_path);
        }
        self.ctx
            .state_dir
            .join("sandboxes")
            .join(session_id)
            .join(agent_id)
            .join(sub_path)
    }

    fn provider_home_path(&self, session_id: &str, agent_id: &str) -> PathBuf {
        let sandbox_dir = self
            .ctx
            .state_dir
            .join("sandboxes")
            .join(session_id)
            .join(agent_id);
        let sandbox_path = sandbox_dir
            .canonicalize()
            .unwrap_or_else(|_| sandbox_dir.to_path_buf());
        let mut hasher = Sha256::new();
        hasher.update(sandbox_path.to_string_lossy().as_bytes());
        let digest = hasher.finalize();
        let project_id_short = digest
            .iter()
            .take(6)
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let cache_root = std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(std::env::temp_dir)
                    .join(".cache")
            });
        cache_root.join("ah/sandboxes").join(project_id_short)
    }

    fn query_agent_state(&self, agent_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get(0),
            )
            .expect("agent state should exist")
    }

    fn query_agent_row_count(&self, agent_id: &str) -> i64 {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get(0),
            )
            .expect("agent row count should query")
    }

    fn query_agent_pid(&self, agent_id: &str) -> i64 {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT pid FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .expect("agent pid row should exist")
            .expect("agent pid should be set")
    }

    fn query_agent_config_hash(&self, agent_id: &str) -> String {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT config_hash FROM agents WHERE id = ?",
                params![agent_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .expect("agent config_hash row should exist")
            .expect("agent config_hash should be set")
    }

    fn query_agent_pane_id(&self, agent_id: &str) -> Option<String> {
        ah::agent_io::pane_id(agent_id).map(|pane_id| pane_id.0)
    }

    fn query_agent_events(&self, agent_id: &str, event_type: &str) -> Vec<Value> {
        let conn = self.ctx.db.conn();
        let mut stmt = conn
            .prepare(
                "SELECT payload FROM events \
                 WHERE agent_id = ? AND event_type = ? ORDER BY seq_id ASC",
            )
            .expect("agent events query should prepare");
        stmt.query_map(params![agent_id, event_type], |row| row.get::<_, String>(0))
            .expect("agent events query should run")
            .map(|payload| parse_event_payload(&payload.expect("event payload should read")))
            .collect()
    }

    fn query_events_by_type(&self, event_type: &str) -> Vec<Value> {
        let conn = self.ctx.db.conn();
        let mut stmt = conn
            .prepare("SELECT payload FROM events WHERE event_type = ? ORDER BY seq_id ASC")
            .expect("events query should prepare");
        stmt.query_map(params![event_type], |row| row.get::<_, String>(0))
            .expect("events query should run")
            .map(|payload| parse_event_payload(&payload.expect("event payload should read")))
            .collect()
    }

    fn query_agent_last_error(&self, agent_id: &str) -> (Option<String>, Option<i64>) {
        self.ctx
            .db
            .conn()
            .query_row(
                "SELECT error_code, exit_code FROM agents WHERE id = ?",
                params![agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("agent error row should exist")
    }

    fn update_agent_state_direct(&self, agent_id: &str, state: &str) {
        self.ctx
            .db
            .conn()
            .execute(
                "UPDATE agents SET state = ?, state_version = state_version + 1, updated_at = unixepoch() WHERE id = ?",
                params![state, agent_id],
            )
            .expect("direct agent state update should succeed");
    }

    fn assert_sandbox_file(&self, session_id: &str, agent_id: &str, sub_path: &str) -> PathBuf {
        let path = self.sandbox_path(session_id, agent_id, sub_path);
        assert!(
            path.exists(),
            "sandbox file should exist: {}",
            path.display()
        );
        path
    }

    fn assert_symlink_target(
        &self,
        session_id: &str,
        agent_id: &str,
        sub_path: &str,
        expected_target: &Path,
    ) {
        let path = self.assert_sandbox_file(session_id, agent_id, sub_path);
        let target = std::fs::read_link(&path)
            .unwrap_or_else(|err| panic!("{} should be a symlink: {err}", path.display()));
        assert_eq!(
            target,
            expected_target,
            "symlink target mismatch for {}",
            path.display()
        );
    }

    fn has_tmux_session(&self, session_name: &str) -> bool {
        Command::new("tmux")
            .args([
                "-L",
                self.ctx.tmux_server.socket_name(),
                "has-session",
                "-t",
                session_name,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn list_tmux_panes(&self, session_name: &str) -> Vec<String> {
        let output = Command::new("tmux")
            .args([
                "-L",
                self.ctx.tmux_server.socket_name(),
                "list-panes",
                "-t",
                session_name,
                "-F",
                "#{pane_id}",
            ])
            .output()
            .expect("tmux list-panes should run");
        if !output.status.success() {
            return Vec::new();
        }
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::to_string)
            .collect()
    }

    async fn wait_for_tmux_pane_gone(&self, agent_id: &str, timeout: Duration) {
        let session_name = agent_session_name(agent_id);
        let old_pane = self.query_agent_pane_id(agent_id);
        wait_until("agent tmux pane gone", timeout, || {
            if let Some(pane) = old_pane.as_ref()
                && self
                    .list_tmux_panes(&session_name)
                    .iter()
                    .any(|item| item == pane)
            {
                return false;
            }
            !self.has_tmux_session(&session_name) || self.list_tmux_panes(&session_name).is_empty()
        })
        .await;
    }

    async fn wait_for_tmux_pane_present(&self, agent_id: &str, pane_id: &str, timeout: Duration) {
        let session_name = agent_session_name(agent_id);
        wait_until("agent tmux pane present", timeout, || {
            self.list_tmux_panes(&session_name)
                .iter()
                .any(|pane| pane == pane_id)
        })
        .await;
    }
}

fn parse_event_payload(payload: &str) -> Value {
    serde_json::from_str(payload).unwrap_or_else(|_| Value::String(payload.to_string()))
}

fn wait_for_resume_marker(path: &Path, timeout: Duration) -> String {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(content) = std::fs::read_to_string(path)
            && !content.trim().is_empty()
        {
            return content;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "resume marker not present at {} within {:?}",
        path.display(),
        timeout
    );
}

fn assert_marker_contains(path: &Path, flag: &str) {
    let content = wait_for_resume_marker(path, Duration::from_secs(10));
    assert!(
        content.contains(flag),
        "resume marker {} should contain {flag:?}, actual: {content:?}",
        path.display()
    );
}

#[derive(Clone, Default)]
struct AgentFixture {
    env: HashMap<String, String>,
    hooks: HashMap<String, Vec<HookGroup>>,
    plugins: Vec<String>,
}

impl AgentFixture {
    fn spec(&self, agent_id: &str) -> AgentSpec {
        AgentSpec {
            agent_id: agent_id.to_string(),
            provider: AGENT_PROVIDER.to_string(),
            env: self.env.clone(),
            hooks: self.hooks.clone(),
            plugins: self.plugins.clone(),
        }
    }
}

#[derive(Clone)]
struct MasterSpec {
    cmd: String,
    hooks: HashMap<String, Vec<HookGroup>>,
    plugins: Vec<String>,
}

impl Default for MasterSpec {
    fn default() -> Self {
        Self {
            cmd: MASTER_CMD.to_string(),
            hooks: HashMap::new(),
            plugins: Vec::new(),
        }
    }
}

impl MasterSpec {
    fn rpc_value(&self) -> Value {
        json!({
            "cmd": self.cmd,
            "hooks": self.hooks,
            "plugins": self.plugins,
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
}

impl AgentSpec {
    fn rpc_value(&self) -> Value {
        json!({
            "agent_id": self.agent_id,
            "provider": self.provider,
            "env": self.env,
            "hooks": self.hooks,
            "plugins": self.plugins,
        })
    }
}

fn build_realign_extra_ah_toml(project_dir: &Path, agents: &[(&str, &AgentFixture)]) -> PathBuf {
    let path = project_dir.join("ah.toml");
    let mut config = format!(
        r#"version = "1"

[master]
cmd = "{MASTER_CMD}"
"#
    );
    for (agent_id, fixture) in agents {
        config.push_str(&format!(
            r#"
[agents.{agent_id}]
provider = "{AGENT_PROVIDER}"
"#
        ));
        if !fixture.plugins.is_empty() {
            let plugins = fixture
                .plugins
                .iter()
                .map(|plugin| format!("{plugin:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            config.push_str(&format!("plugins = [{plugins}]\n"));
        }
        if !fixture.env.is_empty() {
            config.push_str(&format!("\n[agents.{agent_id}.env]\n"));
            for (key, value) in &fixture.env {
                config.push_str(&format!("{key} = {value:?}\n"));
            }
        }
        if !fixture.hooks.is_empty() {
            config.push_str(&format!("\n[agents.{agent_id}.hooks]\n"));
            config.push_str(
                &toml::to_string(&fixture.hooks).expect("hooks should serialize to toml"),
            );
        }
    }
    std::fs::write(&path, config).expect("write PR-3 ah.toml");
    path
}

fn realign_payload(session_id: &str, master: &MasterSpec, agents: &[AgentSpec]) -> Value {
    json!({
        "session_id": session_id,
        "force": false,
        "master": master.rpc_value(),
        "agents": agents.iter().map(AgentSpec::rpc_value).collect::<Vec<_>>(),
    })
}

fn write_hook_script(project_dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = project_dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create hook parent dir");
    }
    std::fs::write(&path, body).expect("write hook script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&path)
            .expect("hook metadata should exist")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).expect("chmod hook script");
    }
    path
}

fn command_hooks(command: &Path) -> HashMap<String, Vec<HookGroup>> {
    HashMap::from([(
        "PreToolUse".to_string(),
        vec![HookGroup {
            matcher: "*".to_string(),
            hooks: vec![HookItem {
                hook_type: "command".to_string(),
                command: command.display().to_string(),
                timeout: None,
            }],
        }],
    )])
}

async fn run_realign(h: &Harness, mut payload: Value, force: bool) -> Value {
    payload["force"] = json!(force);
    let result = h.rpc("session.realign", payload).await;
    result
        .get("statuses")
        .cloned()
        .expect("session.realign result should include statuses")
}

fn install_fake_claude(project_dir: &Path) -> PathBuf {
    install_fake_claude_with_behavior(project_dir, MOCK_ECHO)
}

fn install_fake_claude_with_behavior(project_dir: &Path, behavior: &str) -> PathBuf {
    assert!(
        [MOCK_ECHO, MOCK_BUSY, MOCK_CRASH].contains(&behavior),
        "unsupported fake claude behavior: {behavior}"
    );
    let bin_dir = project_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).expect("create fake claude bin dir");
    let fake_claude = bin_dir.join("claude");
    let script = format!(
        r#"#!/usr/bin/env bash
behavior="${{GRAND_TOUR_MOCK_BEHAVIOR:-{behavior}}}"
if [[ -n "${{GRAND_TOUR_RESUME_ARG_MARKER:-}}" ]]; then
  printf '%s\n' "$@" > "$GRAND_TOUR_RESUME_ARG_MARKER"
fi
if [[ "$behavior" == "CRASH" ]]; then
  printf 'status Sonnet\n────────\n  ❯ '
  sleep 1
  exit 1
fi

printf 'status Sonnet\n────────\n  ❯ '
if [[ "$behavior" == "BUSY" ]]; then
  if IFS= read -r line; then
    printf '\nmock claude busy: %s\n' "$line"
    while true; do sleep 3600; done
  fi
  exit 0
fi

while IFS= read -r line; do
  printf '\nmock claude: %s\n  ❯ ' "$line"
done
"#
    );
    std::fs::write(&fake_claude, script).expect("write fake claude");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&fake_claude)
            .expect("fake claude metadata should exist")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_claude, permissions).expect("chmod fake claude");
    }
    let old_path = std::env::var("PATH").unwrap_or_default();
    let new_path = format!("{}:{old_path}", bin_dir.display());
    unsafe {
        std::env::set_var("HOME", project_dir);
        std::env::set_var("XDG_CACHE_HOME", project_dir.join(".cache"));
        std::env::set_var("PATH", new_path);
        std::env::set_var("GRAND_TOUR_MOCK_BEHAVIOR", behavior);
    }
    fake_claude
}

async fn wait_for_agent_state(h: &Harness, agent_id: &str, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut current = h.query_agent_state(agent_id);
    while Instant::now() < deadline {
        if current == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        current = h.query_agent_state(agent_id);
    }
    panic!("timed out waiting for agent {agent_id} state {expected}; current={current}");
}

async fn wait_until<F>(label: &str, timeout: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {label}");
}

async fn wait_for_pid_gone(pid: i64) {
    wait_until("old pid gone", Duration::from_secs(3), || {
        !process_exists(pid)
    })
    .await;
}

fn process_exists(pid: i64) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

fn agent_status<'a>(statuses: &'a Value, agent_id: &str) -> &'a Value {
    statuses
        .as_array()
        .expect("session.realign statuses should be an array")
        .iter()
        .find(|status| status["agent_id"] == agent_id)
        .unwrap_or_else(|| panic!("missing per-agent status for {agent_id}: {statuses}"))
}

struct MatrixState {
    session_id: String,
    master: MasterSpec,
    a1: AgentSpec,
}

struct BusyDriftState {
    agent: AgentSpec,
    fixture: AgentFixture,
    hook_path: PathBuf,
}

struct CrashState {
    agent: AgentSpec,
}

async fn baseline_setup(h: &Harness) -> MatrixState {
    install_fake_claude_with_behavior(h.project_dir(), MOCK_BUSY);
    let mut fixture = AgentFixture::default();
    fixture.env.insert(
        "GRAND_TOUR_MOCK_BEHAVIOR".to_string(),
        MOCK_BUSY.to_string(),
    );
    build_realign_extra_ah_toml(
        h.project_dir(),
        &[(AGENT_A1, &fixture), (AGENT_A2, &fixture)],
    );
    let master = MasterSpec::default();
    let a1 = fixture.spec(AGENT_A1);
    let a2 = fixture.spec(AGENT_A2);

    let created = h
        .rpc(
            "session.create",
            json!({
                "project_id": PROJECT_ID,
                "absolute_path": h.project_dir(),
            }),
        )
        .await;
    let session_id = created["session_id"]
        .as_str()
        .expect("session.create should return session_id")
        .to_string();

    h.rpc(
        "session.spawn_master_pane",
        json!({
            "session_id": session_id,
            "cmd": master.cmd,
            "hooks": master.hooks,
            "plugins": master.plugins,
        }),
    )
    .await;
    for agent in [&a1, &a2] {
        h.rpc(
            "agent.spawn",
            json!({
                "session_id": session_id,
                "agent_id": agent.agent_id,
                "provider": agent.provider,
                "extra_env_vars": agent.env,
                "hooks": agent.hooks,
                "plugins": agent.plugins,
            }),
        )
        .await;
        wait_for_agent_state(h, &agent.agent_id, "IDLE", Duration::from_secs(10)).await;
        assert!(
            h.has_tmux_session(&agent_session_name(&agent.agent_id)),
            "{} tmux session should exist",
            agent.agent_id
        );
        h.assert_sandbox_file(&session_id, &agent.agent_id, "");
    }

    MatrixState {
        session_id,
        master,
        a1,
    }
}

async fn case_06_orphan_audit_only(h: &Harness, state: &MatrixState) {
    let old_state = h.query_agent_state(AGENT_A2);
    let old_pid = h.query_agent_pid(AGENT_A2);
    let old_pane = h
        .query_agent_pane_id(AGENT_A2)
        .expect("a2 pane should be registered before ORPHAN audit");
    let old_hash = h.query_agent_config_hash(AGENT_A2);
    let old_killed_events = h.query_agent_events(AGENT_A2, "agent_killed").len();

    let fixture = AgentFixture::default();
    build_realign_extra_ah_toml(h.project_dir(), &[(AGENT_A1, &fixture)]);
    let payload = realign_payload(
        &state.session_id,
        &state.master,
        std::slice::from_ref(&state.a1),
    );
    let statuses = run_realign(h, payload, false).await;
    let a2_status = agent_status(&statuses, AGENT_A2);
    assert_eq!(a2_status["status"], "ORPHAN");

    assert_eq!(
        h.query_agent_row_count(AGENT_A2),
        1,
        "a2 DB row must remain"
    );
    assert_eq!(
        h.query_agent_state(AGENT_A2),
        old_state,
        "audit-only must not change state"
    );
    assert_eq!(
        h.query_agent_pid(AGENT_A2),
        old_pid,
        "audit-only must not change pid"
    );
    assert_eq!(
        h.query_agent_config_hash(AGENT_A2),
        old_hash,
        "audit-only must not change config_hash"
    );
    wait_until("a2 pid remains alive", Duration::from_secs(3), || {
        process_exists(old_pid)
    })
    .await;
    h.wait_for_tmux_pane_present(AGENT_A2, &old_pane, Duration::from_secs(3))
        .await;

    let killed_events = h.query_agent_events(AGENT_A2, "agent_killed");
    assert_eq!(
        killed_events.len(),
        old_killed_events,
        "audit-only must not add agent_killed events"
    );
    assert!(
        !killed_events
            .iter()
            .any(|event| event["reason"].as_str() == Some("ORPHAN_FORCE_CLEANUP")),
        "audit-only must not emit ORPHAN_FORCE_CLEANUP: {killed_events:?}"
    );

    println!(
        "case_06 PASS orphan audit-only state={old_state} pid={old_pid} pane={old_pane} hash={old_hash} killed_events={old_killed_events} pid_pane_retry=30x100ms"
    );
}

async fn case_07_orphan_force_cleanup(h: &Harness, state: &MatrixState) {
    let old_pid = h.query_agent_pid(AGENT_A2);
    let old_pane = h
        .query_agent_pane_id(AGENT_A2)
        .expect("a2 pane should be registered before ORPHAN force cleanup");

    let payload = realign_payload(
        &state.session_id,
        &state.master,
        std::slice::from_ref(&state.a1),
    );
    let statuses = run_realign(h, payload, true).await;
    let a2_status = agent_status(&statuses, AGENT_A2);
    assert_eq!(a2_status["status"], "ORPHAN");
    assert_eq!(a2_status["action"], "KILLED");

    assert_eq!(
        h.query_agent_row_count(AGENT_A2),
        1,
        "a2 DB row must be retained"
    );
    assert_eq!(h.query_agent_state(AGENT_A2), "KILLED");
    let killed_events = h.query_agent_events(AGENT_A2, "agent_killed");
    assert!(
        killed_events
            .iter()
            .any(|event| event["reason"].as_str() == Some("ORPHAN_FORCE_CLEANUP")),
        "force cleanup should emit ORPHAN_FORCE_CLEANUP: {killed_events:?}"
    );

    // mark_agent_killed triggers cleanup_agent_runtime_resources, which removes pane registry/runtime.
    h.wait_for_tmux_pane_gone(AGENT_A2, Duration::from_secs(3))
        .await;
    wait_until("a2 pid gone", Duration::from_secs(3), || {
        !process_exists(old_pid)
    })
    .await;

    println!(
        "case_07 PASS orphan force action=KILLED state=KILLED db_row_retained=1 old_pid={old_pid} old_pane={old_pane} event=ORPHAN_FORCE_CLEANUP pid_pane_retry=30x100ms"
    );
}

async fn case_08_busy_skip(h: &Harness, state: &MatrixState) -> BusyDriftState {
    assert_eq!(
        h.query_agent_state(AGENT_A1),
        "IDLE",
        "a1 must start case_08 IDLE"
    );
    let old_pid = h.query_agent_pid(AGENT_A1);
    let old_pane = h
        .query_agent_pane_id(AGENT_A1)
        .expect("a1 pane should be registered before BUSY skip");
    let old_hash = h.query_agent_config_hash(AGENT_A1);
    let old_skipped_events = h.query_agent_events(AGENT_A1, "drift_skipped").len();

    let submitted = h
        .rpc(
            "job.submit",
            json!({
                "agent_id": AGENT_A1,
                "text": "hold busy for PR-3\n",
                "request_id": "pr3-busy-skip",
            }),
        )
        .await;
    assert_eq!(submitted["status"], "QUEUED");
    let dispatched = ah::db::jobs::dispatch_job_to_agent(
        h.ctx.db.clone(),
        AGENT_A1.to_string(),
        vec!["IDLE".to_string()],
        "WAITING_FOR_ACK".to_string(),
        "command_received".to_string(),
        json!({ "status": "SENT" }),
    )
    .await
    .expect("job dispatch should run")
    .expect("queued job should dispatch");
    assert_eq!(submitted["job_id"], dispatched.job.id);
    let pane = ah::agent_io::pane_id(AGENT_A1).expect("a1 pane should be registered for dispatch");
    ah::agent_io::send_text_to_pane(
        h.ctx.tmux_server.clone(),
        AGENT_A1,
        "bash",
        pane,
        dispatched.job.prompt_text,
    )
    .await
    .expect("dispatch should send prompt to pane");
    tokio::time::sleep(Duration::from_millis(300)).await;
    // NOTE: test seam - BUSY is advanced manually with the real state-machine API:
    // 1. send_text_to_pane sends the real job prompt to the mock pane, not a SQL stub.
    // 2. transit_agent_state records the real transition with reason="DISPATCH_ACK_STABLE".
    // This bypasses the orchestrator background ACK stability task because that task is
    // difficult to trigger deterministically inside the in-process harness. BUSY is still
    // driven by a real job.submit -> real pane prompt path, not by a stub bypass. See
    // pr3-design.md section 2 BUSY flow for the intended production path.
    ah::db::state_machine::transit_agent_state(
        h.ctx.db.clone(),
        AGENT_A1.to_string(),
        vec!["WAITING_FOR_ACK".to_string(), "IDLE".to_string()],
        "BUSY".to_string(),
        Some("DISPATCH_ACK_STABLE".to_string()),
    )
    .await
    .expect("ACK stability should mark BUSY");
    wait_for_agent_state(h, AGENT_A1, "BUSY", Duration::from_secs(10)).await;

    let hook_path = write_hook_script(
        h.project_dir(),
        "hooks/pr3-busy-skip.sh",
        "#!/bin/sh\nexit 0\n",
    );
    let mut drift = AgentFixture::default();
    drift.env.insert(
        "GRAND_TOUR_MOCK_BEHAVIOR".to_string(),
        MOCK_BUSY.to_string(),
    );
    drift
        .env
        .insert("GRAND_TOUR_BUSY_DRIFT".to_string(), "v2".to_string());
    drift.hooks = command_hooks(&hook_path);
    build_realign_extra_ah_toml(h.project_dir(), &[(AGENT_A1, &drift)]);
    let drift_agent = drift.spec(AGENT_A1);
    let payload = realign_payload(
        &state.session_id,
        &state.master,
        std::slice::from_ref(&drift_agent),
    );
    assert_eq!(payload["agents"][0]["env"]["GRAND_TOUR_BUSY_DRIFT"], "v2");
    assert!(
        payload["agents"][0]["hooks"]
            .as_object()
            .is_some_and(|hooks| !hooks.is_empty()),
        "mixed drift payload should include hooks"
    );

    let statuses = run_realign(h, payload, false).await;
    let a1_status = agent_status(&statuses, AGENT_A1);
    assert_eq!(a1_status["status"], "SKIPPED_BUSY");

    let skipped_events = h.query_agent_events(AGENT_A1, "drift_skipped");
    assert_eq!(
        skipped_events.len(),
        old_skipped_events + 1,
        "BUSY skip should add one drift_skipped event"
    );
    let skipped = skipped_events
        .last()
        .expect("drift_skipped event should exist");
    assert_eq!(skipped["state"], "BUSY");
    assert!(
        skipped.get("reason").and_then(Value::as_str).is_some(),
        "drift_skipped payload should contain reason: {skipped}"
    );

    assert_eq!(h.query_agent_state(AGENT_A1), "BUSY");
    assert_eq!(
        h.query_agent_pid(AGENT_A1),
        old_pid,
        "skip must not change pid"
    );
    assert_eq!(
        h.query_agent_pane_id(AGENT_A1).as_deref(),
        Some(old_pane.as_str()),
        "skip must not change pane"
    );
    assert_eq!(
        h.query_agent_config_hash(AGENT_A1),
        old_hash,
        "skip must not change hash"
    );
    wait_until("a1 busy pid remains alive", Duration::from_secs(3), || {
        process_exists(old_pid)
    })
    .await;
    h.wait_for_tmux_pane_present(AGENT_A1, &old_pane, Duration::from_secs(3))
        .await;

    println!(
        "case_08 PASS busy skip job_status=QUEUED state=BUSY status=SKIPPED_BUSY old_pid={old_pid} old_pane={old_pane} hash={old_hash} skipped_events={} pid_pane_retry=30x100ms",
        skipped_events.len()
    );

    BusyDriftState {
        agent: drift_agent,
        fixture: drift,
        hook_path,
    }
}

async fn case_09_busy_force_realign(h: &Harness, state: &MatrixState, busy: &BusyDriftState) {
    assert_eq!(
        h.query_agent_state(AGENT_A1),
        "BUSY",
        "a1 must start force realign BUSY"
    );
    let old_pid = h.query_agent_pid(AGENT_A1);
    let old_pane = h
        .query_agent_pane_id(AGENT_A1)
        .expect("a1 pane should be registered before BUSY force realign");
    let old_hash = h.query_agent_config_hash(AGENT_A1);

    let payload = realign_payload(
        &state.session_id,
        &state.master,
        std::slice::from_ref(&busy.agent),
    );
    let statuses = run_realign(h, payload, true).await;
    let a1_status = agent_status(&statuses, AGENT_A1);
    assert_eq!(a1_status["status"], "REALIGNED");

    wait_for_agent_state(h, AGENT_A1, "IDLE", Duration::from_secs(10)).await;
    let new_pid = h.query_agent_pid(AGENT_A1);
    let new_pane = h
        .query_agent_pane_id(AGENT_A1)
        .expect("a1 pane should be registered after BUSY force realign");
    let new_hash = h.query_agent_config_hash(AGENT_A1);
    assert_ne!(new_pid, old_pid, "force realign should replace pid");
    assert_ne!(new_pane, old_pane, "force realign should replace pane");
    assert_ne!(new_hash, old_hash, "force realign should update hash");
    wait_for_pid_gone(old_pid).await;
    wait_until("old a1 pane gone", Duration::from_secs(3), || {
        !h.list_tmux_panes(&agent_session_name(AGENT_A1))
            .iter()
            .any(|pane| pane == &old_pane)
    })
    .await;

    let spawned_events = h.query_agent_events(AGENT_A1, "agent_spawned");
    assert!(
        spawned_events
            .iter()
            .any(|event| event["reason"].as_str() == Some("DRIFT_REALIGN")),
        "force realign should leave a DRIFT_REALIGN spawn event; pre-delete DRIFT_FORCE_REALIGN kill event is cascaded with old row: {spawned_events:?}"
    );
    h.assert_symlink_target(
        &state.session_id,
        AGENT_A1,
        ".claude/hooks/pr3-busy-skip.sh",
        &busy.hook_path,
    );

    println!(
        "case_09 PASS busy force status=REALIGNED spawn_event=DRIFT_REALIGN old_pid={old_pid} new_pid={new_pid} old_pane={old_pane} new_pane={new_pane} old_hash={old_hash} new_hash={new_hash} hook_symlink=.claude/hooks/pr3-busy-skip.sh->{} pid_pane_retry=30x100ms",
        busy.hook_path.display()
    );
}

async fn case_10_error_crash_detection(
    h: &Harness,
    state: &MatrixState,
    busy: &BusyDriftState,
) -> CrashState {
    let mut crash_fixture = AgentFixture::default();
    crash_fixture.env.insert(
        "GRAND_TOUR_MOCK_BEHAVIOR".to_string(),
        MOCK_CRASH.to_string(),
    );
    build_realign_extra_ah_toml(
        h.project_dir(),
        &[(AGENT_A1, &busy.fixture), (AGENT_CRASH, &crash_fixture)],
    );
    let crash_agent = crash_fixture.spec(AGENT_CRASH);
    let payload = realign_payload(
        &state.session_id,
        &state.master,
        &[busy.agent.clone(), crash_agent.clone()],
    );
    let statuses = run_realign(h, payload, false).await;
    let crash_status = agent_status(&statuses, AGENT_CRASH);
    assert_eq!(crash_status["status"], "NEW");

    // This validates the natural crash path. Only case_11 may use SQL fallback if needed.
    wait_for_agent_state(h, AGENT_CRASH, "CRASHED", Duration::from_secs(15)).await;
    let (error_code, exit_code) = h.query_agent_last_error(AGENT_CRASH);
    assert_eq!(error_code.as_deref(), Some("AGENT_UNEXPECTED_EXIT"));
    assert!(
        exit_code.is_none_or(|code| code != 0),
        "CRASH mode should persist non-zero exit_code when available; pidfd non-child path may store NULL: {exit_code:?}"
    );
    h.wait_for_tmux_pane_gone(AGENT_CRASH, Duration::from_secs(3))
        .await;

    println!(
        "case_10 PASS crash detection agent={AGENT_CRASH} state=CRASHED error_code=AGENT_UNEXPECTED_EXIT exit_code={:?} pane_cleanup=ok pid_pane_retry=30x100ms",
        exit_code
    );

    CrashState { agent: crash_agent }
}

async fn case_11_error_recovery(
    h: &Harness,
    state: &MatrixState,
    busy: &BusyDriftState,
    crash: &CrashState,
) {
    assert_eq!(h.query_agent_state(AGENT_CRASH), "CRASHED");
    let crashed_pid = h.query_agent_pid(AGENT_CRASH);
    let marker_path = h.project_dir().join("pr6-resume-argv.txt");
    let _ = std::fs::remove_file(&marker_path);
    assert!(
        !marker_path.exists(),
        "resume marker must not exist before CRASHED recovery path; IDLE/BUSY drift must not inject resume"
    );

    let mut recovery_fixture = AgentFixture::default();
    recovery_fixture.env.insert(
        "GRAND_TOUR_MOCK_BEHAVIOR".to_string(),
        MOCK_ECHO.to_string(),
    );
    recovery_fixture.env.insert(
        "GRAND_TOUR_RESUME_ARG_MARKER".to_string(),
        marker_path.display().to_string(),
    );
    build_realign_extra_ah_toml(
        h.project_dir(),
        &[(AGENT_A1, &busy.fixture), (AGENT_CRASH, &recovery_fixture)],
    );
    let recovery_agent = recovery_fixture.spec(AGENT_CRASH);
    let payload = realign_payload(
        &state.session_id,
        &state.master,
        &[busy.agent.clone(), recovery_agent],
    );
    let statuses = run_realign(h, payload, false).await;
    let recovery_status = agent_status(&statuses, AGENT_CRASH);
    assert_eq!(recovery_status["status"], "REALIGNED");
    wait_for_agent_state(h, AGENT_CRASH, "IDLE", Duration::from_secs(10)).await;
    let recovered_pid = h.query_agent_pid(AGENT_CRASH);
    assert_ne!(
        recovered_pid, crashed_pid,
        "recovery realign should replace crashed provider process"
    );
    assert_marker_contains(&marker_path, "--continue");

    let spawned_events = h.query_agent_events(AGENT_CRASH, "agent_spawned");
    let recovery_event = spawned_events
        .last()
        .unwrap_or_else(|| panic!("recovery should leave agent_spawned event: {spawned_events:?}"));
    assert_eq!(recovery_event["reason"], "DRIFT_REALIGN");
    assert!(
        recovery_event["is_recovery"].as_bool() == Some(true),
        "recovery spawn event should carry is_recovery=true: {recovery_event:?}"
    );

    println!(
        "case_11 PASS recovery status=REALIGNED old_pid={crashed_pid} new_pid={recovered_pid} marker={} event=DRIFT_REALIGN is_recovery=true",
        marker_path.display()
    );
    let _ = crash;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn grand_tour_realign_extra_matrix() {
    let h = Harness::new();
    let state = baseline_setup(&h).await;
    case_06_orphan_audit_only(&h, &state).await;
    case_07_orphan_force_cleanup(&h, &state).await;
    let busy = case_08_busy_skip(&h, &state).await;
    case_09_busy_force_realign(&h, &state, &busy).await;
    let crash = case_10_error_crash_detection(&h, &state, &busy).await;
    case_11_error_recovery(&h, &state, &busy, &crash).await;
}
