use crate::error::AhError;
use crate::rpc::Ctx;
use crate::runtime_events::{
    RuntimeSnapshotReason, RuntimeSnapshotRequest, build_runtime_snapshot,
    runtime_snapshot_fingerprint,
};
use serde_json::Value;
use std::time::Duration;

pub async fn handle_runtime_snapshot(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let snapshot = build_runtime_snapshot(
        ctx,
        RuntimeSnapshotRequest {
            reason: RuntimeSnapshotReason::Initial,
            config_path: optional_string(&params, "config_path"),
            workspace_path: optional_string(&params, "workspace_path"),
            sequence: 1,
        },
    )
    .await?;
    serde_json::to_value(snapshot)
        .map_err(|err| AhError::IpcInvalidRequest(format!("serialize runtime snapshot: {err}")))
}

pub async fn stream_runtime_subscribe<W>(
    params: Value,
    ctx: &Ctx,
    writer: &mut W,
) -> Result<(), AhError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let config_path = optional_string(&params, "config_path");
    let workspace_path = optional_string(&params, "workspace_path");
    let interval_ms = params
        .get("interval_ms")
        .and_then(Value::as_u64)
        .unwrap_or(1_000)
        .clamp(100, 60_000);
    let mut sequence = 1_u64;
    let initial = build_runtime_snapshot(
        ctx,
        RuntimeSnapshotRequest {
            reason: RuntimeSnapshotReason::Initial,
            config_path: config_path.clone(),
            workspace_path: workspace_path.clone(),
            sequence,
        },
    )
    .await?;
    let mut last_fingerprint = runtime_snapshot_fingerprint(&initial)?;
    write_runtime_snapshot(writer, &initial).await?;

    let mut rx = crate::orchestrator::pubsub::subscribe_runtime_updates();
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    loop {
        let reason = tokio::select! {
            recv = rx.recv() => match recv {
                Ok(reason) => reason,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => RuntimeSnapshotReason::InventoryChanged,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(AhError::IpcInvalidRequest("runtime subscription closed".into()));
                }
            },
            _ = interval.tick() => RuntimeSnapshotReason::TmuxChanged,
        };
        let candidate = build_runtime_snapshot(
            ctx,
            RuntimeSnapshotRequest {
                reason,
                config_path: config_path.clone(),
                workspace_path: workspace_path.clone(),
                sequence: sequence + 1,
            },
        )
        .await?;
        let fingerprint = runtime_snapshot_fingerprint(&candidate)?;
        if fingerprint == last_fingerprint {
            continue;
        }
        sequence += 1;
        last_fingerprint = fingerprint;
        write_runtime_snapshot(writer, &candidate).await?;
    }
}

async fn write_runtime_snapshot<W>(
    writer: &mut W,
    snapshot: &crate::runtime_events::RuntimeSnapshot,
) -> Result<(), AhError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    let line = serde_json::to_string(snapshot)
        .map_err(|err| AhError::IpcInvalidRequest(format!("serialize runtime snapshot: {err}")))?;
    writer
        .write_all(line.as_bytes())
        .await
        .map_err(|err| AhError::PtyIoError(format!("write runtime snapshot: {err}")))?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|err| AhError::PtyIoError(format!("write runtime snapshot newline: {err}")))?;
    Ok(())
}

fn optional_string(params: &Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}
