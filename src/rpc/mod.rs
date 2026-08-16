use crate::db::Db;
use crate::sandbox::EnvState;
use crate::tmux::TmuxServer;
use std::io;
#[cfg(unix)]
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixListener;

pub mod handlers;
pub mod router;

#[derive(Clone)]
pub struct Ctx {
    pub db: Db,
    pub state_dir: PathBuf,
    pub env_state: EnvState,
    pub daemon_unit: Option<String>,
    pub tmux_server: Arc<TmuxServer>,
    pub claude_gateway: Arc<crate::claude_gateway::ClaudeGatewayService>,
}

#[cfg(windows)]
pub async fn run_server(socket_path: &Path, ctx: Ctx) -> io::Result<()> {
    let _ = (socket_path, ctx);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Windows RPC Named Pipe transport is not implemented until M0 IPC boundary work",
    ))
}

#[cfg(unix)]
pub async fn run_server(socket_path: &Path, ctx: Ctx) -> io::Result<()> {
    let Some(listener) = bind_rpc_listener(socket_path)? else {
        return Ok(());
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    }

    tracing::info!(?socket_path, "UDS RPC server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let ctx = ctx.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if let Some(params) =
                            stream_subscribe_params(line.trim_end(), "event.subscribe")
                        {
                            if let Err(err) =
                                handlers::stream_event_subscribe(params, &ctx, &mut writer).await
                            {
                                tracing::warn!(error = %err, "event.subscribe stream failed");
                            }
                            break;
                        }
                        if let Some(params) =
                            stream_subscribe_params(line.trim_end(), "runtime.subscribe")
                        {
                            if let Err(err) =
                                handlers::stream_runtime_subscribe(params, &ctx, &mut writer).await
                            {
                                tracing::warn!(error = %err, "runtime.subscribe stream failed");
                            }
                            break;
                        }
                        let response = router::dispatch(line.trim_end(), &ctx).await;
                        if writer.write_all(response.as_bytes()).await.is_err() {
                            break;
                        }
                        if writer.write_all(b"\n").await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "UDS read failed");
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(unix)]
fn stream_subscribe_params(line: &str, method: &str) -> Option<serde_json::Value> {
    let request: serde_json::Value = serde_json::from_str(line).ok()?;
    if request.get("jsonrpc").and_then(serde_json::Value::as_str) != Some("2.0") {
        return None;
    }
    if request.get("method").and_then(serde_json::Value::as_str) != Some(method) {
        return None;
    }
    Some(
        request
            .get("params")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
}

#[cfg(unix)]
fn bind_rpc_listener(socket_path: &Path) -> io::Result<Option<UnixListener>> {
    match UnixListener::bind(socket_path) {
        Ok(listener) => Ok(Some(listener)),
        Err(err) if err.kind() == io::ErrorKind::AddrInUse => {
            if StdUnixStream::connect(socket_path).is_ok() {
                tracing::warn!(?socket_path, "another ah is already running; exiting");
                return Ok(None);
            }

            tracing::warn!(?socket_path, "removing stale ah socket before rebinding");
            std::fs::remove_file(socket_path)?;
            UnixListener::bind(socket_path).map(Some)
        }
        Err(err) => Err(err),
    }
}
