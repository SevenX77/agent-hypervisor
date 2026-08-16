#![cfg(unix)]

use ah::db;
use ah::db::jobs::{dispatch_job_to_agent, query_job};
use ah::provider::manifest::{collect_spawn_env, get_manifest};
use ah::rpc::Ctx;
use ah::rpc::router::dispatch;
use ah::sandbox::EnvState;
use ah::tmux::TmuxServer;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const HOOK_PATH: &str = "assets/hooks/evidence-hook.sh";

struct Harness {
    ctx: Ctx,
    _db_file: tempfile::NamedTempFile,
    _state_dir: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let ctx = Ctx {
            db: db::init(db_file.path()).unwrap(),
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(state_dir.path())),
            claude_gateway: std::sync::Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };
        seed_base_rows(&ctx.db.conn());
        Self {
            ctx,
            _db_file: db_file,
            _state_dir: state_dir,
        }
    }
}

fn seed_base_rows(conn: &Connection) {
    conn.execute(
        "INSERT INTO projects (id, absolute_path) VALUES ('p1b', '/tmp/pr1b')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (id, project_id, master_pid) VALUES ('s1b', 'p1b', 1)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO agents (id, session_id, provider, state, pid) VALUES ('a1', 's1b', 'claude', 'IDLE', 123)",
        [],
    )
    .unwrap();
}

fn insert_job_row(conn: &Connection, job_id: &str, agent_id: &str, status: &str) {
    conn.execute(
        "INSERT INTO jobs (id, agent_id, request_id, prompt_text, status)
         VALUES (?, ?, NULL, 'edit src/lib.rs', ?)",
        params![job_id, agent_id, status],
    )
    .unwrap();
}

fn insert_read_evidence(conn: &Connection, job_id: &str, path: &str) {
    conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload)
         VALUES ('a1', NULL, 'test_seed', '{}')",
        [],
    )
    .unwrap();
    let seq_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO evidence (
            id, agent_id, event_seq_id, pane_bytes, failed_rules, status,
            job_id, evidence_type, subject_path, payload
        ) VALUES (?, 'a1', ?, X'', '[]', 'PENDING', ?, 'read', ?, '{}')",
        params![
            format!("evi_{job_id}_{}", path.replace('/', "_")),
            seq_id,
            job_id,
            path
        ],
    )
    .unwrap();
}

async fn rpc_call(ctx: &Ctx, method: &str, params: Value) -> Value {
    let response = dispatch(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        })
        .to_string(),
        ctx,
    )
    .await;
    let response: Value = serde_json::from_str(&response).unwrap();
    assert!(
        response.get("error").is_none(),
        "{method} returned error: {}",
        response["error"]
    );
    response["result"].clone()
}

#[tokio::test]
async fn evidence_insert_rpc_records_read() {
    let h = Harness::new();
    insert_job_row(&h.ctx.db.conn(), "job_read", "a1", "DISPATCHED");

    let result = rpc_call(
        &h.ctx,
        "evidence.insert",
        json!({
            "agent_id": "a1",
            "job_id": "job_read",
            "evidence_type": "read",
            "subject_path": "src/lib.rs",
            "payload": {"tool_name": "Read"}
        }),
    )
    .await;

    let evidence_id = result["evidence_id"].as_str().expect("missing evidence_id");
    assert!(result["recorded"].as_bool().unwrap_or(false));
    let conn = h.ctx.db.conn();
    let row: (String, String, String, String) = conn
        .query_row(
            "SELECT job_id, evidence_type, subject_path, payload FROM evidence WHERE id = ?",
            [evidence_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(row.0, "job_read");
    assert_eq!(row.1, "read");
    assert_eq!(row.2, "src/lib.rs");
    assert!(row.3.contains("Read"));
    let recorded_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id = 'a1' AND event_type = 'evidence_recorded'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded_events, 1);
}

#[tokio::test]
async fn job_has_evidence_is_job_and_path_scoped() {
    let h = Harness::new();
    let conn = h.ctx.db.conn();
    insert_job_row(&conn, "job_target", "a1", "DISPATCHED");
    insert_job_row(&conn, "job_other", "a1", "DISPATCHED");
    insert_read_evidence(&conn, "job_other", "src/lib.rs");
    insert_read_evidence(&conn, "job_target", "src/other.rs");

    let absent = rpc_call(
        &h.ctx,
        "job.has_evidence",
        json!({
            "job_id": "job_target",
            "evidence_type": "read",
            "subject_path": "src/lib.rs"
        }),
    )
    .await;
    assert_eq!(absent["has_evidence"].as_bool(), Some(false));

    insert_read_evidence(&conn, "job_target", "src/lib.rs");
    let present = rpc_call(
        &h.ctx,
        "job.has_evidence",
        json!({
            "job_id": "job_target",
            "evidence_type": "read",
            "subject_path": "src/lib.rs"
        }),
    )
    .await;
    assert_eq!(present["has_evidence"].as_bool(), Some(true));
}

#[tokio::test]
async fn job_mark_requires_evidence_sets_physical_gate() {
    let h = Harness::new();
    insert_job_row(&h.ctx.db.conn(), "job_arm", "a1", "DISPATCHED");

    for _ in 0..2 {
        let result = rpc_call(
            &h.ctx,
            "job.mark_requires_evidence",
            json!({ "job_id": "job_arm" }),
        )
        .await;
        assert_eq!(result["job_id"], "job_arm");
        assert_eq!(result["requires_physical_evidence"].as_bool(), Some(true));
    }

    let job = query_job(h.ctx.db.clone(), "job_arm".to_string())
        .await
        .unwrap()
        .unwrap();
    assert!(job.requires_physical_evidence);
    assert!(!job.requires_test_evidence);

    let missing_response = dispatch(
        &json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "job.mark_requires_evidence",
            "params": {"job_id": "job_missing"}
        })
        .to_string(),
        &h.ctx,
    )
    .await;
    let missing: Value = serde_json::from_str(&missing_response).unwrap();
    assert_eq!(missing["error"]["code"], "IPC_INVALID_REQUEST");
}

#[tokio::test]
async fn mark_requires_evidence_then_mark_idle_denies_without_read() {
    let h = Harness::new();
    {
        let conn = h.ctx.db.conn();
        conn.execute(
            "UPDATE agents SET state = 'BUSY', state_version = state_version + 1 WHERE id = 'a1'",
            [],
        )
        .unwrap();
        insert_job_row(&conn, "job_wire", "a1", "DISPATCHED");
    }

    let result = rpc_call(
        &h.ctx,
        "job.mark_requires_evidence",
        json!({ "job_id": "job_wire" }),
    )
    .await;
    assert_eq!(result["requires_physical_evidence"].as_bool(), Some(true));

    let outcome = db::state_machine::mark_agent_idle_matched(h.ctx.db.clone(), "a1".to_string())
        .await
        .unwrap();
    assert_eq!(outcome.0, 0);
    assert_eq!(outcome.1, None);

    let denial = evidence_denied_payload(&h.ctx.db).expect("missing evidence_denied event");
    assert!(
        denial.contains("physical_evidence") || denial.contains("physical evidence"),
        "denial should name the physical evidence gate: {denial}"
    );
    assert!(
        denial.contains("SYSTEM DENY"),
        "denial should inject a SYSTEM DENY prompt: {denial}"
    );
    assert_eq!(
        state_and_job_status(&h.ctx.db, "job_wire"),
        ("BUSY".to_string(), "DISPATCHED".to_string())
    );
}

#[tokio::test]
async fn dispatched_job_env_contains_ah_job_id() {
    let h = Harness::new();
    let old_socket = std::env::var_os("AH_SOCKET");
    let old_job = std::env::var_os("AH_JOB_ID");
    unsafe {
        std::env::set_var("AH_SOCKET", "/tmp/pr1b-ahd.sock");
        std::env::set_var("AH_JOB_ID", "job_env_one");
    }
    let spawn_env = collect_spawn_env(&get_manifest("claude"), &HashMap::new());
    restore_env("AH_SOCKET", old_socket.as_ref());
    restore_env("AH_JOB_ID", old_job.as_ref());

    assert_env_contains(&spawn_env, "AH_SOCKET", "/tmp/pr1b-ahd.sock");
    assert_env_contains(&spawn_env, "AH_JOB_ID", "job_env_one");

    let conn = h.ctx.db.conn();
    insert_job_row(&conn, "job_env_one", "a1", "QUEUED");
    insert_job_row(&conn, "job_env_two", "a1", "QUEUED");
    let first = dispatch_job_to_agent(
        h.ctx.db.clone(),
        "a1".to_string(),
        vec!["IDLE".to_string()],
        "WAITING_FOR_ACK".to_string(),
        "command_received".to_string(),
        json!({"status": "SENT"}),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.job.id, "job_env_one");
    assert_eq!(
        first.job_payload["env"]["AH_JOB_ID"].as_str(),
        Some("job_env_one"),
        "dispatched command payload must carry per-job AH_JOB_ID, not a static agent env"
    );
}

#[test]
fn evidence_hook_protocol_python_read_and_write() {
    let recorder = RpcRecorder::spawn(false);
    let read_output = run_hook(
        &recorder.socket_path,
        "job_hook",
        json!({
            "tool_name": "Read",
            "tool_input": {"file_path": "src/lib.rs"}
        }),
    );
    assert!(
        read_output.status.success(),
        "hook failed: {}",
        String::from_utf8_lossy(&read_output.stderr)
    );

    for input in [
        json!({"tool_name": "Edit", "tool_input": {"file_path": "src/lib.rs"}}),
        json!({"tool_name": "Write", "tool_input": {"file_path": "src/lib.rs"}}),
        json!({"tool_name": "MultiEdit", "tool_input": {"file_path": "src/lib.rs"}}),
        json!({"tool_name": "read_file", "tool_input": {"path": "src/lib.rs"}}),
        json!({"tool_name": "replace", "tool_input": {"path": "src/lib.rs"}}),
        json!({"tool_name": "write_file", "tool_input": {"path": "src/lib.rs"}}),
    ] {
        let output = run_hook(&recorder.socket_path, "job_hook", input);
        assert!(
            output.status.success(),
            "hook failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let script = std::fs::read_to_string(HOOK_PATH).expect("evidence hook script must exist");
    assert!(
        !script.contains("jq"),
        "PR-1b hook must use python3 -c JSON parsing, not jq"
    );

    let methods = recorder.methods();
    assert!(methods.iter().any(|method| method == "evidence.insert"));
    assert!(methods.iter().any(|method| method == "job.has_evidence"));
    assert!(
        methods
            .iter()
            .all(|method| method != "job.mark_requires_evidence"),
        "PR-1b hook must not arm physical evidence gate; methods={methods:?}"
    );
}

#[test]
fn evidence_hook_deny_allow_outputs_match_claude_protocol() {
    let deny_recorder = RpcRecorder::spawn(false);
    let claude_deny = run_hook(
        &deny_recorder.socket_path,
        "job_hook_deny",
        json!({
            "tool_name": "Edit",
            "tool_input": {"file_path": "src/lib.rs"}
        }),
    );
    assert!(
        claude_deny.status.success(),
        "hook failed: {}",
        String::from_utf8_lossy(&claude_deny.stderr)
    );
    let claude: Value = serde_json::from_slice(&claude_deny.stdout).unwrap();
    assert_eq!(
        claude["hookSpecificOutput"]["permissionDecision"].as_str(),
        Some("deny")
    );
    assert!(
        claude["hookSpecificOutput"]["permissionDecisionReason"]
            .as_str()
            .unwrap_or_default()
            .contains("Evidence Required")
    );

    let allow_recorder = RpcRecorder::spawn(true);
    let allow = run_hook(
        &allow_recorder.socket_path,
        "job_hook_allow",
        json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "src/lib.rs"}
        }),
    );
    assert!(
        allow.status.success(),
        "hook failed: {}",
        String::from_utf8_lossy(&allow.stderr)
    );
    let allow_json: Value = serde_json::from_slice(&allow.stdout).unwrap();
    assert_eq!(
        allow_json["hookSpecificOutput"]["permissionDecision"].as_str(),
        Some("allow")
    );
    assert!(
        allow_recorder
            .methods()
            .iter()
            .all(|method| method != "job.mark_requires_evidence"),
        "PR-1b hook must not arm physical evidence gate"
    );
}

#[test]
fn hook_fails_open_on_rpc_error() {
    let missing_socket_dir = tempfile::tempdir().unwrap();
    let missing_socket = missing_socket_dir.path().join("missing.sock");
    let socket_fail = run_hook(
        &missing_socket,
        "job_hook_fail_open",
        json!({
            "tool_name": "Edit",
            "tool_input": {"file_path": "src/lib.rs"}
        }),
    );
    assert!(
        socket_fail.status.success(),
        "hook must fail open on socket errors: {}",
        String::from_utf8_lossy(&socket_fail.stderr)
    );
    assert_allow_output(&socket_fail.stdout);
    assert!(
        String::from_utf8_lossy(&socket_fail.stderr).contains("fail-open"),
        "stderr should record fail-open reason: {}",
        String::from_utf8_lossy(&socket_fail.stderr)
    );

    let error_recorder = RpcRecorder::spawn_rpc_error();
    let rpc_fail = run_hook(
        &error_recorder.socket_path,
        "job_hook_fail_open",
        json!({
            "tool_name": "replace",
            "tool_input": {"path": "src/lib.rs"}
        }),
    );
    assert!(
        rpc_fail.status.success(),
        "hook must fail open on RPC errors: {}",
        String::from_utf8_lossy(&rpc_fail.stderr)
    );
    assert_allow_output(&rpc_fail.stdout);
    assert!(
        String::from_utf8_lossy(&rpc_fail.stderr).contains("forced rpc failure"),
        "stderr should include RPC error message: {}",
        String::from_utf8_lossy(&rpc_fail.stderr)
    );
}

fn state_and_job_status(db: &db::Db, job_id: &str) -> (String, String) {
    db.conn()
        .query_row(
            "SELECT agents.state, jobs.status
             FROM agents JOIN jobs ON jobs.agent_id = agents.id
             WHERE agents.id = 'a1' AND jobs.id = ?",
            [job_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

fn evidence_denied_payload(db: &db::Db) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT payload
             FROM events
             WHERE agent_id = 'a1' AND event_type = 'evidence_denied'
             ORDER BY seq_id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok()
}

fn assert_env_contains(env: &[(String, String)], key: &str, expected: &str) {
    assert!(
        env.iter()
            .any(|(actual_key, value)| actual_key == key && value == expected),
        "spawn env missing {key}={expected}; env={env:?}"
    );
}

fn assert_allow_output(stdout: &[u8]) {
    let output: Value = serde_json::from_slice(stdout).unwrap();
    let decision = output
        .pointer("/hookSpecificOutput/permissionDecision")
        .or_else(|| output.get("decision"))
        .and_then(Value::as_str);
    assert_eq!(decision, Some("allow"), "unexpected hook output: {output}");
}

fn restore_env(key: &str, old: Option<&OsString>) {
    unsafe {
        if let Some(value) = old {
            std::env::set_var(key, value);
        } else {
            std::env::remove_var(key);
        }
    }
}

struct HookRun {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_hook(socket_path: &Path, job_id: &str, stdin_json: Value) -> HookRun {
    let mut child = Command::new("bash")
        .arg(HOOK_PATH)
        .env("AH_SOCKET", socket_path)
        .env("AH_JOB_ID", job_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn evidence hook");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(stdin_json.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    HookRun {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

struct RpcRecorder {
    socket_path: PathBuf,
    methods: Arc<Mutex<Vec<String>>>,
    _socket_dir: tempfile::TempDir,
    _thread: std::thread::JoinHandle<()>,
}

impl RpcRecorder {
    fn spawn(has_read_evidence: bool) -> Self {
        Self::spawn_with(RecorderMode::Normal { has_read_evidence })
    }

    fn spawn_rpc_error() -> Self {
        Self::spawn_with(RecorderMode::Error)
    }

    fn spawn_with(mode: RecorderMode) -> Self {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("ahd.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let methods = Arc::new(Mutex::new(Vec::new()));
        let thread_methods = methods.clone();
        let thread = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut request = String::new();
                        let _ = stream.read_to_string(&mut request);
                        for line in request.lines().filter(|line| !line.trim().is_empty()) {
                            let value: Value = serde_json::from_str(line).unwrap_or(Value::Null);
                            let method = value
                                .get("method")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string();
                            thread_methods.lock().unwrap().push(method.clone());
                            let response = match mode {
                                RecorderMode::Normal { has_read_evidence } => {
                                    let result = match method.as_str() {
                                        "evidence.insert" => {
                                            json!({"evidence_id": "evi_hook", "recorded": true})
                                        }
                                        "job.has_evidence" => {
                                            json!({"has_evidence": has_read_evidence})
                                        }
                                        "job.mark_requires_evidence" => {
                                            json!({"job_id": "job_hook", "requires_physical_evidence": true})
                                        }
                                        _ => json!({}),
                                    };
                                    json!({
                                        "jsonrpc": "2.0",
                                        "id": value.get("id").cloned().unwrap_or(Value::Null),
                                        "result": result,
                                    })
                                }
                                RecorderMode::Error => json!({
                                    "jsonrpc": "2.0",
                                    "id": value.get("id").cloned().unwrap_or(Value::Null),
                                    "error": {"code": -32000, "message": "forced rpc failure"},
                                }),
                            };
                            let _ = writeln!(stream, "{response}");
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            socket_path,
            methods,
            _socket_dir: socket_dir,
            _thread: thread,
        }
    }

    fn methods(&self) -> Vec<String> {
        self.methods.lock().unwrap().clone()
    }
}

#[derive(Clone, Copy)]
enum RecorderMode {
    Normal { has_read_evidence: bool },
    Error,
}
