use super::params::required_str;
use crate::db::events::query_last_event_of_type_matching_payload;
use crate::db::jobs::query_job;
use crate::error::AhError;
use crate::rpc::Ctx;
use serde_json::{Value, json};

pub async fn handle_event_subscribe(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    if event_kind_includes(&params, "stuck") && params.get("since_seq_id").is_none() {
        if let Some(frame) = stuck_frame_for_filter(ctx, &params).await? {
            return Ok(frame);
        }
    }
    if let Some(frame) = subscription_backfill(ctx, &params)
        .await?
        .into_iter()
        .next()
    {
        return serde_json::to_value(frame)
            .map_err(|err| AhError::IpcInvalidRequest(format!("serialize event frame: {err}")));
    }
    if should_use_job_terminal_subscription(&params) {
        let job_id = required_str(&params, "job_id")?.to_string();
        return event_frame_for_job(ctx, &job_id)
            .await?
            .ok_or_else(|| AhError::PtyIoError("Timeout waiting for event frame".into()));
    }
    Err(AhError::PtyIoError(
        "Timeout waiting for event frame".into(),
    ))
}

pub async fn stream_event_subscribe<W>(
    params: Value,
    ctx: &Ctx,
    writer: &mut W,
) -> Result<(), AhError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    if let Some(frame) = subscription_backfill(ctx, &params)
        .await?
        .into_iter()
        .next()
    {
        let value = serde_json::to_value(frame)
            .map_err(|err| AhError::IpcInvalidRequest(format!("serialize event frame: {err}")))?;
        write_event_frame(writer, value).await?;
        return Ok(());
    }

    if event_kind_includes(&params, "stuck") && params.get("since_seq_id").is_none() {
        if let Some(frame) = stuck_frame_for_filter(ctx, &params).await? {
            write_event_frame(writer, frame).await?;
            return Ok(());
        }
    }

    if !should_use_job_terminal_subscription(&params) {
        let mut rx = crate::orchestrator::pubsub::subscribe_events();
        loop {
            match rx.recv().await {
                Ok(frame) if event_frame_matches_filter(&frame, &params) => {
                    let value = serde_json::to_value(frame).map_err(|err| {
                        AhError::IpcInvalidRequest(format!("serialize event frame: {err}"))
                    })?;
                    write_event_frame(writer, value).await?;
                    return Ok(());
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    if let Some(frame) = subscription_backfill(ctx, &params)
                        .await?
                        .into_iter()
                        .next()
                    {
                        let value = serde_json::to_value(frame).map_err(|err| {
                            AhError::IpcInvalidRequest(format!("serialize event frame: {err}"))
                        })?;
                        write_event_frame(writer, value).await?;
                        return Ok(());
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(AhError::IpcInvalidRequest(
                        "event subscription closed".into(),
                    ));
                }
            }
        }
    }

    let job_id = required_str(&params, "job_id")?.to_string();
    if let Some(frame) = event_frame_for_job(ctx, &job_id).await? {
        write_event_frame(writer, frame).await?;
        return Ok(());
    }

    let mut rx = crate::orchestrator::pubsub::subscribe_job_updates();
    loop {
        match rx.recv().await {
            Ok(updated_job_id) if updated_job_id == job_id => {
                if let Some(frame) = event_frame_for_job(ctx, &job_id).await? {
                    write_event_frame(writer, frame).await?;
                    return Ok(());
                }
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                if let Some(frame) = event_frame_for_job(ctx, &job_id).await? {
                    write_event_frame(writer, frame).await?;
                    return Ok(());
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err(AhError::IpcInvalidRequest(
                    "event subscription closed".into(),
                ));
            }
        }
    }
}

async fn write_event_frame<W>(writer: &mut W, frame: Value) -> Result<(), AhError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    writer
        .write_all(frame.to_string().as_bytes())
        .await
        .map_err(|err| AhError::PtyIoError(format!("write event frame: {err}")))?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|err| AhError::PtyIoError(format!("write event frame newline: {err}")))?;
    Ok(())
}

fn requested_event_kinds(params: &Value) -> Option<Vec<String>> {
    let value = params.get("kind").or_else(|| params.get("event_kind"))?;
    match value {
        Value::Array(kinds) => Some(
            kinds
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
        ),
        Value::String(value) => Some(vec![value.clone()]),
        _ => Some(Vec::new()),
    }
}

fn event_kind_includes(params: &Value, kind: &str) -> bool {
    match requested_event_kinds(params) {
        Some(kinds) => kinds.iter().any(|value| value == kind),
        None => kind == "job_state_change",
    }
}

fn should_use_job_terminal_subscription(params: &Value) -> bool {
    params.get("job_id").and_then(Value::as_str).is_some()
        && requested_event_kinds(params)
            .map(|kinds| kinds.iter().any(|kind| kind == "job_state_change"))
            .unwrap_or(true)
}

async fn subscription_backfill(
    ctx: &Ctx,
    params: &Value,
) -> Result<Vec<crate::orchestrator::pubsub::EventFrame>, AhError> {
    let Some(since_seq_id) = params.get("since_seq_id").and_then(Value::as_i64) else {
        return Ok(Vec::new());
    };
    let agent_id = params
        .get("agent_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let kinds = requested_event_kinds(params);
    let job_id = params
        .get("job_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let frames =
        crate::db::events::query_events_backfill(ctx.db.clone(), since_seq_id, agent_id, kinds)
            .await?;
    Ok(frames
        .into_iter()
        .filter(|frame| {
            job_id
                .as_deref()
                .is_none_or(|job_id| frame.job_id.as_deref() == Some(job_id))
        })
        .collect())
}

fn event_frame_matches_filter(
    frame: &crate::orchestrator::pubsub::EventFrame,
    params: &Value,
) -> bool {
    if let Some(kinds) = requested_event_kinds(params)
        && !kinds.iter().any(|kind| kind == &frame.kind)
    {
        return false;
    }
    if let Some(agent_id) = params.get("agent_id").and_then(Value::as_str)
        && frame.agent_id != agent_id
    {
        return false;
    }
    if let Some(job_id) = params.get("job_id").and_then(Value::as_str)
        && frame.job_id.as_deref() != Some(job_id)
    {
        return false;
    }
    true
}

pub(super) async fn stuck_frame_for_filter(
    ctx: &Ctx,
    params: &Value,
) -> Result<Option<Value>, AhError> {
    let agent_id = match params.get("agent_id").and_then(Value::as_str) {
        Some(agent_id) => agent_id.to_string(),
        None => {
            let job_id = match params.get("job_id").and_then(Value::as_str) {
                Some(job_id) => job_id,
                None => return Ok(None),
            };
            let job = query_job(ctx.db.clone(), job_id.to_string())
                .await?
                .ok_or_else(|| AhError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;
            job.agent_id
        }
    };
    let job_id_filter = params.get("job_id").and_then(Value::as_str);
    let mut before_seq_id = None;
    loop {
        let Some(event) = query_last_event_of_type_matching_payload(
            ctx.db.clone(),
            agent_id.clone(),
            "state_change".to_string(),
            "%STUCK%".to_string(),
            before_seq_id,
        )
        .await?
        else {
            return Ok(None);
        };
        before_seq_id = Some(event.seq_id);
        let payload: Value = serde_json::from_str(&event.payload).unwrap_or_else(|_| json!({}));
        if payload.get("to").and_then(Value::as_str) != Some("STUCK") {
            continue;
        }
        let payload_job_id = payload.get("job_id").and_then(Value::as_str);
        if let Some(job_id) = job_id_filter
            && payload_job_id != Some(job_id)
        {
            continue;
        }
        let signal_kinds = payload
            .get("signal_kinds")
            .cloned()
            .unwrap_or_else(|| json!(["state_machine"]));
        let elapsed_secs = payload
            .get("elapsed_secs")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let frame_payload = json!({
            "job_id": payload_job_id,
            "signal_kinds": signal_kinds,
            "elapsed_secs": elapsed_secs,
        });
        return Ok(Some(json!({
            "event_id": event.seq_id,
            "kind": "stuck",
            "agent_id": agent_id,
            "job_id": payload_job_id,
            "state": "STUCK",
            "ts_unix_micro": now_unix_micro(),
            "payload": frame_payload,
        })));
    }
}

async fn event_frame_for_job(ctx: &Ctx, job_id: &str) -> Result<Option<Value>, AhError> {
    let job = query_job(ctx.db.clone(), job_id.to_string())
        .await?
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;
    if !matches!(job.status.as_str(), "COMPLETED" | "FAILED" | "CANCELLED") {
        return Ok(None);
    }
    let payload = json!({
        "job_id": job.id,
        "status": job.status,
        "reply_text": job.reply_text,
        "error_reason": job.error_reason,
    });
    Ok(Some(json!({
        "event_id": job.completed_at.unwrap_or(0),
        "kind": "job_state_change",
        "agent_id": job.agent_id,
        "job_id": job.id,
        "state": job.status,
        "ts_unix_micro": now_unix_micro(),
        "payload": payload,
    })))
}

fn now_unix_micro() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
}
