#![cfg(unix)]

use ah::claude_gateway::{
    ClaudeGatewayService, ClaudeUpstream, CredentialEvent, GatewayCore, GatewayRequest,
    GatewayResponse, ProductionUpstream, RecordedCredentialEvents, TokenSet, UpstreamError,
    UpstreamResult, fake_worker_jwt, gateway_worker_topology, read_seed_credentials,
    register_worker, run_internal_bridge, validate_credential_path_not_wsl_windows_mount,
    write_seed_credentials_guarded,
};
use ah::home_materialization::prepare_home_layout_with_claude_credentials;
use serde_json::json;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn ac_single_flight_expired_workers_refresh_once() {
    let upstream = Arc::new(RecordingUpstream::new());
    let gateway = Arc::new(GatewayCore::new(
        expired_token("old", "real-refresh"),
        upstream.clone(),
        RecordedCredentialEvents::default(),
    ));
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|idx| {
            let gateway = gateway.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                gateway
                    .forward_messages(worker_request(&format!("worker-{idx}")))
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        assert_eq!(handle.join().unwrap().status, 200);
    }
    assert_eq!(upstream.refresh_calls(), 1);
    assert_eq!(upstream.message_calls(), 8);
}

#[test]
#[serial_test::serial(global_env)]
fn ac_zero_credentials_worker_home_has_no_real_token_bytes() {
    let _env = TestHomeEnv::new();
    let host_cred = _env.host_home.path().join(".claude/.credentials.json");
    std::fs::create_dir_all(host_cred.parent().unwrap()).unwrap();
    std::fs::write(
        &host_cred,
        json!({
            "accessToken": "real-access-token",
            "refreshToken": "real-refresh-token"
        })
        .to_string(),
    )
    .unwrap();

    let sandbox_root = tempfile::tempdir().unwrap();
    let project_root = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let claude = prepare_home_layout_with_claude_credentials(
        "claude",
        sandbox_root.path(),
        project_root.path(),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    assert!(!claude.home_root.join(".claude/.credentials.json").exists());
    assert!(!path_tree_contains(&claude.home_root, "real-refresh-token"));
    assert_eq!(
        claude.extra_env.get("CLAUDE_SECURESTORAGE_CONFIG_DIR"),
        Some(&shared_credentials_dir.path().display().to_string())
    );
    assert!(!claude.extra_env.contains_key("CLAUDE_CODE_USE_GATEWAY"));
    assert!(!claude.extra_env.contains_key("ANTHROPIC_AUTH_TOKEN"));
    assert!(claude.extra_env.get("ANTHROPIC_BASE_URL").is_none());
}

#[test]
fn ac_rewrite_upstream_sees_real_token_not_fake_jwt() {
    let upstream = Arc::new(RecordingUpstream::new());
    let gateway = GatewayCore::new(
        valid_token("real-access-token", "real-refresh"),
        upstream.clone(),
        RecordedCredentialEvents::default(),
    );

    gateway
        .forward_messages(worker_request("worker-a"))
        .unwrap();

    let fake = fake_worker_jwt("worker-a");
    let headers = upstream.last_message_headers();
    assert_eq!(
        header_value(&headers, "authorization"),
        Some("Bearer real-access-token")
    );
    assert!(headers.iter().all(|(_, value)| !value.contains(&fake)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_uds_channel_isolation_rejects_wrong_worker_jwt() {
    let upstream = Arc::new(RecordingUpstream::new());
    let gateway = Arc::new(GatewayCore::new(
        valid_token("real-access-token", "real-refresh"),
        upstream.clone(),
        RecordedCredentialEvents::default(),
    ));
    let tmp = tempfile::tempdir().unwrap();
    let uds = tmp.path().join("ah-gateway.sock");
    let listener = register_worker(gateway, "worker-b".to_string(), uds.clone()).unwrap();

    let response = tokio::task::spawn_blocking(move || {
        let mut stream = UnixStream::connect(uds).unwrap();
        write!(
            stream,
            "POST /v1/messages HTTP/1.1\r\nauthorization: Bearer {}\r\ncontent-length: 2\r\n\r\n{{}}",
            fake_worker_jwt("worker-a")
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    })
    .await
    .unwrap();
    listener.shutdown().await;

    assert!(response.starts_with("HTTP/1.1 403"));
    assert_eq!(upstream.message_calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_uds_header_limit_returns_400() {
    let upstream = Arc::new(RecordingUpstream::new());
    let gateway = Arc::new(GatewayCore::new(
        valid_token("real-access-token", "real-refresh"),
        upstream,
        RecordedCredentialEvents::default(),
    ));
    let tmp = tempfile::tempdir().unwrap();
    let uds = tmp.path().join("ah-gateway.sock");
    let listener = register_worker(gateway, "worker-a".to_string(), uds.clone()).unwrap();
    let response = tokio::task::spawn_blocking(move || {
        let mut stream = UnixStream::connect(uds).unwrap();
        let large = "a".repeat(9 * 1024);
        write!(stream, "GET / HTTP/1.1\r\nx-large: {large}\r\n\r\n").unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    })
    .await
    .unwrap();
    listener.shutdown().await;
    assert!(response.starts_with("HTTP/1.1 400"));
}

#[test]
fn ac_failure_cache_suppresses_invalid_grant_refreshes_and_records_event() {
    let upstream = Arc::new(RecordingUpstream::new_invalid_grant());
    let events = RecordedCredentialEvents::default();
    let gateway = GatewayCore::new(
        expired_token("old", "revoked-refresh"),
        upstream.clone(),
        events.clone(),
    );

    let first = gateway
        .forward_messages(worker_request("worker-a"))
        .unwrap_err();
    let second = gateway
        .forward_messages(worker_request("worker-a"))
        .unwrap_err();

    assert_eq!(first.error_code, "AH_CLAUDE_GATEWAY_REFRESH_INVALID_GRANT");
    assert_eq!(second.error_code, first.error_code);
    assert_eq!(upstream.refresh_calls(), 1);
    assert!(events.snapshot().iter().any(|event| matches!(
        event,
        CredentialEvent::RefreshFailed { upstream_error, .. } if upstream_error == "invalid_grant"
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_bridge_dynamic_ports_do_not_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let uds1 = tmp.path().join("one.sock");
    let uds2 = tmp.path().join("two.sock");
    let _echo1 = spawn_uds_echo(&uds1);
    let _echo2 = spawn_uds_echo(&uds2);
    let port1 = tmp.path().join("one.port");
    let port2 = tmp.path().join("two.port");
    let bridge1 = tokio::spawn({
        let uds = uds1.clone();
        let port = port1.clone();
        async move { run_internal_bridge(&uds, Some(&port)).await }
    });
    let bridge2 = tokio::spawn({
        let uds = uds2.clone();
        let port = port2.clone();
        async move { run_internal_bridge(&uds, Some(&port)).await }
    });

    let p1 = wait_for_port_file(&port1).await;
    let p2 = wait_for_port_file(&port2).await;
    assert_ne!(p1, p2);
    assert_tcp_echo(p1, b"one");
    assert_tcp_echo(p2, b"two");

    bridge1.abort();
    bridge2.abort();
}

#[test]
fn ac_bridge_wrapper_fail_fast_path_is_observable() {
    let shell = ah::claude_gateway::bridge_wrapper_shell(
        "claude",
        Path::new("/bin/ah"),
        Path::new("/var/run/ah-gateway.sock"),
        Path::new("/tmp/sandbox"),
    );
    assert!(shell.contains("bridge.err"));
    assert!(shell.contains("exit 126"));
    assert!(shell.contains("ANTHROPIC_BASE_URL=\"http://localhost:$port\""));
    assert!(!shell.contains("exec sh -lc \"$1\""));
}

#[test]
fn ac_bridge_wrapper_waits_up_to_five_seconds_with_clear_timeout_error() {
    let shell = ah::claude_gateway::bridge_wrapper_shell(
        "claude",
        Path::new("/bin/ah"),
        Path::new("/var/run/ah-gateway.sock"),
        Path::new("/tmp/sandbox"),
    );

    assert!(shell.contains("while [ \"$i\" -lt 50 ]"));
    assert!(shell.contains("sleep 0.1"));
    assert!(shell.contains("bridge process did not write port file within 5s"));
    assert!(shell.contains("exit 126"));
    assert!(!shell.contains("for i in 1 2 3 4 5"));
}

#[test]
fn ac_gateway_host_uds_path_stays_short_for_long_sandbox_root() {
    let tmp = tempfile::tempdir().unwrap();
    let long_root = tmp.path().join("a".repeat(180)).join("worker-sandbox");
    let topology = gateway_worker_topology(&long_root, "ag-long-worker-id").unwrap();
    let host_path = topology.host_uds_path.to_string_lossy();

    assert!(
        host_path.len() < 100,
        "host UDS path must fit Unix socket path limits: {host_path}"
    );
    assert!(
        !topology.host_uds_path.starts_with(&long_root),
        "host UDS path must not inherit long sandbox root"
    );
    assert_eq!(
        topology.sandbox_uds_path,
        Path::new("/var/run/ah-gateway.sock")
    );
}

#[test]
fn ac_bridge_wrapper_executes_inner_with_gateway_base_url() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_ah = tmp.path().join("fake-ah");
    let script = r#"#!/bin/sh
port_file=
while [ "$#" -gt 0 ]; do
  case "$1" in
    internal-bridge) shift ;;
    --port-file) port_file="$2"; shift 2 ;;
    --uds) shift 2 ;;
    *) shift ;;
  esac
done
echo 48231 > "$port_file"
echo $$ > "$port_file.pid"
exec >/dev/null
while :; do sleep 1; done
"#;
    std::fs::write(&fake_ah, script).unwrap();
    let mut perms = std::fs::metadata(&fake_ah).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_ah, perms).unwrap();

    let inner_marker = tmp.path().join("inner.marker");
    let env_marker = tmp.path().join("base-url.txt");
    let inner = format!(
        "printf '%s' \"$ANTHROPIC_BASE_URL\" > {}; printf ran > {}",
        shell_quote_for_test(&env_marker),
        shell_quote_for_test(&inner_marker),
    );
    let shell = ah::claude_gateway::bridge_wrapper_shell(
        &inner,
        &fake_ah,
        Path::new("/tmp/unused-gateway.sock"),
        tmp.path(),
    );

    let output = Command::new("sh").arg("-lc").arg(&shell).output().unwrap();
    if let Ok(pid) = std::fs::read_to_string(tmp.path().join("bridge.port.pid")) {
        let _ = Command::new("kill").arg(pid.trim()).status();
    }

    assert!(
        output.status.success(),
        "wrapper failed: status={:?} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(std::fs::read_to_string(inner_marker).unwrap(), "ran");
    assert_eq!(
        std::fs::read_to_string(env_marker).unwrap(),
        "http://localhost:48231"
    );
}

#[test]
fn ac_wsl_mount_guard_rejects_windows_credentials_path() {
    assert!(
        validate_credential_path_not_wsl_windows_mount(Path::new(
            "/mnt/c/Users/alice/.claude/.credentials.json"
        ))
        .is_err()
    );
}

#[test]
fn addendum_seed_reader_accepts_real_claude_oauth_schema_and_expired_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join(".credentials.json");
    std::fs::write(
        &path,
        json!({
            "claudeAiOauth": {
                "accessToken": "stored-access",
                "refreshToken": "stored-refresh",
                "expiresAt": 0
            }
        })
        .to_string(),
    )
    .unwrap();

    let token = read_seed_credentials(&path).unwrap();

    assert_eq!(token.access_token, "stored-access");
    assert_eq!(token.refresh_token, "stored-refresh");
    assert!(token.expires_at <= SystemTime::UNIX_EPOCH);
}

#[test]
fn addendum_seed_writeback_rotates_refresh_token_atomically_for_linux_path() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join(".credentials.json");
    std::fs::write(
        &path,
        json!({
            "claudeAiOauth": {
                "accessToken": "old-access",
                "refreshToken": "old-refresh",
                "expiresAt": 0
            }
        })
        .to_string(),
    )
    .unwrap();
    let refreshed = valid_token("new-access", "new-refresh");

    write_seed_credentials_guarded(&path, &refreshed).unwrap();
    let stored = read_seed_credentials(&path).unwrap();

    assert_eq!(stored.access_token, "new-access");
    assert_eq!(stored.refresh_token, "new-refresh");
    assert!(stored.expires_at > SystemTime::now());
}

#[test]
fn addendum_wsl_guard_skips_writeback_without_touching_windows_path() {
    let token = valid_token("access", "refresh");

    write_seed_credentials_guarded(
        Path::new("/mnt/c/Users/alice/.claude/.credentials.json"),
        &token,
    )
    .unwrap();
}

#[test]
fn addendum_wsl_guard_skips_writeback_when_symlink_targets_windows_mount() {
    let tmp = tempfile::tempdir().unwrap();
    let link = tmp.path().join(".credentials.json");
    std::os::unix::fs::symlink("/mnt/c/Users/alice/.claude/.credentials.json", &link).unwrap();
    let token = valid_token("access", "refresh");

    write_seed_credentials_guarded(&link, &token).unwrap();

    assert!(link.is_symlink());
    assert!(!tmp.path().join(".credentials.json.tmp").exists());
}

#[test]
fn addendum_transient_refresh_errors_do_not_poison_failure_cache() {
    let upstream = Arc::new(TransientRefreshUpstream::new());
    let gateway = GatewayCore::new(
        expired_token("old", "real-refresh"),
        upstream.clone(),
        RecordedCredentialEvents::default(),
    );

    let first = gateway
        .forward_messages(worker_request("worker-a"))
        .unwrap_err();
    let second = gateway
        .forward_messages(worker_request("worker-a"))
        .unwrap_err();

    assert_eq!(first.error_code, "AH_CLAUDE_GATEWAY_REFRESH_FAILED");
    assert_eq!(second.error_code, "AH_CLAUDE_GATEWAY_REFRESH_FAILED");
    assert_eq!(upstream.refresh_calls(), 2);
}

#[test]
fn addendum_production_refresh_maps_only_400_invalid_grant_to_invalid_grant() {
    let tmp = tempfile::tempdir().unwrap();
    let seed = tmp.path().join(".credentials.json");
    std::fs::write(
        &seed,
        json!({
            "claudeAiOauth": {
                "accessToken": "old-access",
                "refreshToken": "old-refresh",
                "expiresAt": 0
            }
        })
        .to_string(),
    )
    .unwrap();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/token", server.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let (mut stream, _) = server.accept().unwrap();
        let mut buf = [0_u8; 4096];
        let _ = stream.read(&mut buf).unwrap();
        stream
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\ncontent-length: 25\r\n\r\n{\"error\":\"invalid_grant\"}",
            )
            .unwrap();
    });
    let upstream = ProductionUpstream::new_with_urls(seed, url, "http://127.0.0.1".to_string());

    let err = upstream.refresh("old-refresh").unwrap_err();
    handle.join().unwrap();

    assert!(matches!(err, UpstreamError::InvalidGrant { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addendum_service_register_master_is_idempotent_across_reconcile() {
    let tmp = tempfile::tempdir().unwrap();
    let seed = tmp.path().join(".credentials.json");
    std::fs::write(
        &seed,
        json!({
            "claudeAiOauth": {
                "accessToken": "seed-access",
                "refreshToken": "seed-refresh",
                "expiresAt": 32503680000000_i64
            }
        })
        .to_string(),
    )
    .unwrap();
    let service = ClaudeGatewayService::new_with_seed_path(seed);
    let sandbox = tmp.path().join("sandboxes/session-a/master");
    std::fs::create_dir_all(&sandbox).unwrap();

    let first = service
        .register_master("session-a", &sandbox)
        .await
        .unwrap();
    let second = service
        .register_master("session-a", &sandbox)
        .await
        .unwrap();

    assert_eq!(first.host_uds_path, second.host_uds_path);
    assert!(second.host_uds_path.exists());
    service.deregister("session-a").await;
}

fn worker_request(worker_id: &str) -> GatewayRequest {
    GatewayRequest {
        worker_id: worker_id.to_string(),
        method: "POST".to_string(),
        path: "/v1/messages".to_string(),
        headers: vec![(
            "authorization".to_string(),
            format!("Bearer {}", fake_worker_jwt(worker_id)),
        )],
        body: b"{}".to_vec(),
    }
}

fn valid_token(access_token: &str, refresh_token: &str) -> TokenSet {
    TokenSet {
        access_token: access_token.to_string(),
        refresh_token: refresh_token.to_string(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
    }
}

fn expired_token(access_token: &str, refresh_token: &str) -> TokenSet {
    TokenSet {
        access_token: access_token.to_string(),
        refresh_token: refresh_token.to_string(),
        expires_at: SystemTime::now() - Duration::from_secs(1),
    }
}

fn header_value<'a>(headers: &'a [(String, String)], key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

fn shell_quote_for_test(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn path_tree_contains(root: &Path, needle: &str) -> bool {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return false,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path_tree_contains(&path, needle) {
                return true;
            }
        } else if path.is_file()
            && std::fs::read_to_string(&path)
                .map(|content| content.contains(needle))
                .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

struct RecordingUpstream {
    refresh_calls: AtomicUsize,
    message_calls: AtomicUsize,
    last_message_headers: Mutex<Vec<(String, String)>>,
    invalid_grant: bool,
}

struct TransientRefreshUpstream {
    refresh_calls: AtomicUsize,
}

impl TransientRefreshUpstream {
    fn new() -> Self {
        Self {
            refresh_calls: AtomicUsize::new(0),
        }
    }

    fn refresh_calls(&self) -> usize {
        self.refresh_calls.load(Ordering::SeqCst)
    }
}

impl ah::claude_gateway::ClaudeUpstream for TransientRefreshUpstream {
    fn refresh(&self, _refresh_token: &str) -> UpstreamResult<TokenSet> {
        self.refresh_calls.fetch_add(1, Ordering::SeqCst);
        Err(UpstreamError::Http {
            status: 502,
            body: "network down".to_string(),
        })
    }

    fn messages(&self, _request: GatewayRequest) -> UpstreamResult<GatewayResponse> {
        unreachable!("refresh failure should prevent message forwarding")
    }
}

impl RecordingUpstream {
    fn new() -> Self {
        Self {
            refresh_calls: AtomicUsize::new(0),
            message_calls: AtomicUsize::new(0),
            last_message_headers: Mutex::new(Vec::new()),
            invalid_grant: false,
        }
    }

    fn new_invalid_grant() -> Self {
        Self {
            invalid_grant: true,
            ..Self::new()
        }
    }

    fn refresh_calls(&self) -> usize {
        self.refresh_calls.load(Ordering::SeqCst)
    }

    fn message_calls(&self) -> usize {
        self.message_calls.load(Ordering::SeqCst)
    }

    fn last_message_headers(&self) -> Vec<(String, String)> {
        self.last_message_headers.lock().unwrap().clone()
    }
}

impl ah::claude_gateway::ClaudeUpstream for RecordingUpstream {
    fn refresh(&self, refresh_token: &str) -> UpstreamResult<TokenSet> {
        self.refresh_calls.fetch_add(1, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(20));
        if self.invalid_grant {
            return Err(UpstreamError::InvalidGrant {
                body: "invalid_grant: seed credential revoked".to_string(),
            });
        }
        assert_eq!(refresh_token, "real-refresh");
        Ok(valid_token("real-access-refreshed", "real-refresh-rotated"))
    }

    fn messages(&self, request: GatewayRequest) -> UpstreamResult<GatewayResponse> {
        self.message_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_message_headers.lock().unwrap() = request.headers;
        Ok(GatewayResponse {
            status: 200,
            headers: vec![],
            body: b"{\"ok\":true}".to_vec(),
        })
    }
}

struct TestHomeEnv {
    host_home: tempfile::TempDir,
    _cache_home: tempfile::TempDir,
    old_home: Option<OsString>,
    old_cache_home: Option<OsString>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl TestHomeEnv {
    fn new() -> Self {
        let lock = ENV_LOCK.lock().unwrap();
        let host_home = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        let old_home = std::env::var_os("HOME");
        let old_cache_home = std::env::var_os("XDG_CACHE_HOME");
        unsafe {
            std::env::set_var("HOME", host_home.path());
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
        }
        Self {
            host_home,
            _cache_home: cache_home,
            old_home,
            old_cache_home,
            _lock: lock,
        }
    }
}

impl Drop for TestHomeEnv {
    fn drop(&mut self) {
        restore_env("HOME", self.old_home.take());
        restore_env("XDG_CACHE_HOME", self.old_cache_home.take());
    }
}

fn restore_env(key: &str, value: Option<OsString>) {
    unsafe {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

fn spawn_uds_echo(path: &Path) -> tokio::task::JoinHandle<()> {
    let path = path.to_path_buf();
    tokio::spawn(async move {
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(path).unwrap();
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0_u8; 64];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
            });
        }
    })
}

async fn wait_for_port_file(path: &Path) -> u16 {
    for _ in 0..50 {
        if let Ok(content) = std::fs::read_to_string(path) {
            if let Ok(port) = content.parse::<u16>() {
                return port;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port file did not appear: {}", path.display());
}

fn assert_tcp_echo(port: u16, payload: &[u8]) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.write_all(payload).unwrap();
    let mut buf = vec![0_u8; payload.len()];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(buf, payload);
}
