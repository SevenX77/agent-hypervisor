use serde_json::Value;
#[cfg(unix)]
use serde_json::json;
use std::error::Error;
use std::fmt;
use std::future::Future;
#[cfg(windows)]
use std::io;
#[cfg(unix)]
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::pin::Pin;

#[derive(Debug)]
pub enum CliError {
    Config(String),
    DaemonNotRunning(PathBuf),
    DaemonNotAccepting(PathBuf, std::io::Error),
    Io(std::io::Error),
    Rpc { code: i64, message: String },
    DaemonClosedConnection,
    InvalidJson(serde_json::Error),
    InvalidResponse(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(f, "{message}"),
            Self::DaemonNotRunning(path) => {
                write!(f, "ahd daemon is not running at {}", path.display())
            }
            Self::DaemonNotAccepting(path, err) => write!(
                f,
                "ahd daemon socket exists but is not accepting connections at {}: {}",
                path.display(),
                err
            ),
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Rpc { code, message } => write!(f, "RPC error {code}: {message}"),
            Self::DaemonClosedConnection => write!(
                f,
                "daemon closed the connection without replying (it may have been stopped or restarted); check the ahd service logs (journalctl --user -u <ahd unit>)"
            ),
            Self::InvalidJson(err) => write!(f, "invalid JSON response from daemon: {err}"),
            Self::InvalidResponse(message) => write!(f, "invalid response from daemon: {message}"),
        }
    }
}

impl Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> Self {
        Self::InvalidJson(err)
    }
}

impl From<toml::de::Error> for CliError {
    fn from(err: toml::de::Error) -> Self {
        Self::Config(format!("invalid ah.toml: {err}"))
    }
}

pub type RpcFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, CliError>> + Send + 'a>>;

pub trait RpcClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a>;
}

#[derive(Clone)]
pub struct UnixRpcClient {
    socket: PathBuf,
}

impl UnixRpcClient {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }
}

impl RpcClient for UnixRpcClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move { rpc_call(&self.socket, method, params) })
    }
}

pub fn exit_code(err: &CliError) -> i32 {
    match err {
        CliError::DaemonNotRunning(_)
        | CliError::DaemonNotAccepting(_, _)
        | CliError::DaemonClosedConnection => 1,
        CliError::Rpc { .. } => 2,
        CliError::InvalidJson(_) | CliError::InvalidResponse(_) | CliError::Config(_) => 3,
        CliError::Io(_) => 1,
    }
}

pub fn resolve_socket_path() -> PathBuf {
    resolve_socket_path_for_config(None)
}

pub fn resolve_socket_path_for_config(config_path: Option<&Path>) -> PathBuf {
    let socket_override = std::env::var("CCB_SOCKET").ok().map(PathBuf::from);
    resolve_socket_path_for_config_inner(
        config_path,
        socket_override,
        crate::state_layout::resolve_neutral_state_layout,
    )
}

fn resolve_socket_path_for_config_inner(
    config_path: Option<&Path>,
    socket_override: Option<PathBuf>,
    neutral_layout: impl FnOnce() -> crate::state_layout::StateLayout,
) -> PathBuf {
    if let Some(path) = socket_override {
        return path;
    }
    if config_path.is_none() {
        return neutral_layout().state_dir.join("ahd.sock");
    }
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    crate::state_layout::resolve_state_layout(&crate::state_layout::StateLayoutRequest {
        cwd,
        config_path: config_path.map(Path::to_path_buf),
    })
    .state_dir
    .join("ahd.sock")
}

pub fn rpc_call(socket: &Path, method: &str, params: Value) -> Result<Value, CliError> {
    #[cfg(windows)]
    {
        let _ = (method, params);
        return Err(CliError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Windows RPC Named Pipe client is not implemented for {}",
                socket.display()
            ),
        )));
    }

    #[cfg(unix)]
    {
        if !socket.exists() {
            return Err(CliError::DaemonNotRunning(socket.to_path_buf()));
        }

        let mut stream = UnixStream::connect(socket).map_err(|err| {
            if err.kind() == std::io::ErrorKind::ConnectionRefused {
                CliError::DaemonNotAccepting(socket.to_path_buf(), err)
            } else {
                CliError::Io(err)
            }
        })?;
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });
        stream.write_all(request.to_string().as_bytes())?;
        stream.write_all(b"\n")?;
        stream.shutdown(std::net::Shutdown::Write)?;

        let mut raw = String::new();
        stream.read_to_string(&mut raw)?;
        let response = parse_rpc_response(&raw)?;

        if let Some(error) = response.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32000);
            return Err(CliError::Rpc {
                code,
                message: rpc_error_message(error),
            });
        }

        response
            .get("result")
            .cloned()
            .ok_or_else(|| CliError::InvalidResponse("missing result field".into()))
    }
}

/// Renders a daemon error for a human.
///
/// The error code alone ("ENVIRONMENT_NOT_SUPPORTED") says a guard fired but not
/// which one or what to change, so the daemon's `details` — the sentence that
/// names the offending value and the fix — is what the operator actually needs.
/// The code is kept as a prefix so log greps and existing habits still work.
pub(crate) fn rpc_error_message(error: &Value) -> String {
    let data = error.get("data");
    let code = data
        .and_then(|data| data.get("error_code"))
        .and_then(Value::as_str);
    let details = data
        .and_then(|data| data.get("details"))
        .and_then(Value::as_str)
        .filter(|details| !details.trim().is_empty());
    match (code, details) {
        (Some(code), Some(details)) => format!("{code}: {details}"),
        (Some(code), None) => code.to_string(),
        (None, Some(details)) => details.to_string(),
        (None, None) => error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown RPC error")
            .to_string(),
    }
}

pub fn parse_rpc_response(raw: &str) -> Result<Value, CliError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CliError::DaemonClosedConnection);
    }
    serde_json::from_str(trimmed).map_err(CliError::InvalidJson)
}

pub fn rpc_stream_first(socket: &Path, method: &str, params: Value) -> Result<Value, CliError> {
    #[cfg(windows)]
    {
        let _ = (method, params);
        return Err(CliError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Windows RPC Named Pipe streaming client is not implemented for {}",
                socket.display()
            ),
        )));
    }

    #[cfg(unix)]
    {
        if !socket.exists() {
            return Err(CliError::DaemonNotRunning(socket.to_path_buf()));
        }

        let mut stream = UnixStream::connect(socket).map_err(|err| {
            if err.kind() == std::io::ErrorKind::ConnectionRefused {
                CliError::DaemonNotAccepting(socket.to_path_buf(), err)
            } else {
                CliError::Io(err)
            }
        })?;
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });
        stream.write_all(request.to_string().as_bytes())?;
        stream.write_all(b"\n")?;

        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        if line.trim().is_empty() {
            return Err(CliError::InvalidResponse("empty stream response".into()));
        }
        serde_json::from_str(line.trim()).map_err(CliError::InvalidJson)
    }
}

pub fn rpc_stream_lines<F>(
    socket: &Path,
    method: &str,
    params: Value,
    on_line: F,
) -> Result<(), CliError>
where
    F: FnMut(&str) -> Result<(), CliError>,
{
    #[cfg(windows)]
    {
        let _ = (method, params, on_line);
        if !socket.exists() {
            return Err(CliError::DaemonNotRunning(socket.to_path_buf()));
        }
        return Err(CliError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "Windows RPC Named Pipe streaming client is not implemented for {}",
                socket.display()
            ),
        )));
    }

    #[cfg(unix)]
    {
        let mut on_line = on_line;
        if !socket.exists() {
            return Err(CliError::DaemonNotRunning(socket.to_path_buf()));
        }

        let mut stream = UnixStream::connect(socket).map_err(|err| {
            if err.kind() == std::io::ErrorKind::ConnectionRefused {
                CliError::DaemonNotAccepting(socket.to_path_buf(), err)
            } else {
                CliError::Io(err)
            }
        })?;
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });
        stream.write_all(request.to_string().as_bytes())?;
        stream.write_all(b"\n")?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                return Ok(());
            }
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if !trimmed.is_empty() {
                on_line(trimmed)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CliError, parse_rpc_response, resolve_socket_path_for_config_inner, rpc_error_message};
    use crate::state_layout::StateLayout;
    use serde_json::json;

    #[test]
    fn rpc_error_message_carries_the_daemon_detail_not_just_the_code() {
        let error = json!({
            "code": -32000,
            "message": "environment not supported",
            "data": {
                "error_code": "ENVIRONMENT_NOT_SUPPORTED",
                "details": "providers.claude.shared_credentials_dir is on a Windows interop filesystem (9p mounted at /mnt/d)"
            }
        });

        let rendered = rpc_error_message(&error);

        assert!(rendered.starts_with("ENVIRONMENT_NOT_SUPPORTED: "));
        assert!(
            rendered.contains("Windows interop filesystem"),
            "the operator needs the reason, not only the code: {rendered}"
        );
    }

    #[test]
    fn rpc_error_message_falls_back_to_code_then_message() {
        assert_eq!(
            rpc_error_message(&json!({"data": {"error_code": "AGENT_NOT_FOUND"}})),
            "AGENT_NOT_FOUND"
        );
        assert_eq!(
            rpc_error_message(&json!({"message": "boom"})),
            "boom"
        );
    }

    #[test]
    fn parse_rpc_response_reports_closed_connection_for_empty_body() {
        for raw in ["", "  \n"] {
            let err = parse_rpc_response(raw).unwrap_err();
            assert!(matches!(err, CliError::DaemonClosedConnection));
            let message = err.to_string();
            assert!(message.contains("closed the connection"));
            assert!(message.contains("journalctl"));
        }
    }

    #[test]
    fn parse_rpc_response_keeps_invalid_json_for_non_empty_garbage() {
        let err = parse_rpc_response("not json").unwrap_err();
        assert!(matches!(err, CliError::InvalidJson(_)));
    }

    #[test]
    fn parse_rpc_response_accepts_valid_json() {
        let response = parse_rpc_response(r#"{"result":{"ok":true}}"#).unwrap();
        assert_eq!(response["result"]["ok"], true);
    }

    #[test]
    fn no_config_socket_resolution_ignores_ambient_cwd_project() {
        let neutral = tempfile::tempdir().unwrap();
        let socket = resolve_socket_path_for_config_inner(None, None, || StateLayout {
            state_dir: neutral.path().to_path_buf(),
            project_id: None,
        });

        assert_eq!(socket, neutral.path().join("ahd.sock"));
    }

    #[test]
    fn socket_override_takes_priority_over_explicit_config() {
        let neutral = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let config_path = project.path().join("ah.toml");
        std::fs::write(&config_path, "version = \"1\"\n").unwrap();
        let leaked_socket = neutral.path().join("live").join("ahd.sock");

        let socket =
            resolve_socket_path_for_config_inner(Some(&config_path), Some(leaked_socket), || {
                StateLayout {
                    state_dir: neutral.path().to_path_buf(),
                    project_id: None,
                }
            });

        assert_eq!(socket, neutral.path().join("live").join("ahd.sock"));
    }
}
