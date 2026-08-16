use super::params::required_str;
use crate::db::Db;
use crate::db::agents::query_agent;
use crate::db::jobs::{
    insert_job, mark_dispatched_job_cancelled_if_agent_idle, mark_queued_job_cancelled, query_job,
    request_dispatched_job_cancel,
};
use crate::error::AhError;
use crate::rpc::Ctx;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

pub async fn handle_job_submit(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let agent_id = required_str(&params, "agent_id")?;
    let prompt_text = required_str(&params, "text")?;
    let request_id = params
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::to_string);

    let agent = query_agent(ctx.db.clone(), agent_id.to_string())
        .await?
        .ok_or_else(|| AhError::AgentNotFound(agent_id.to_string()))?;
    if matches!(agent.state.as_str(), "CRASHED" | "KILLED") {
        return Err(AhError::AgentWrongState {
            current_state: agent.state,
        });
    }

    let job_id = format!("job_{}", Uuid::new_v4());
    let returned_job_id = insert_job(
        ctx.db.clone(),
        job_id,
        agent_id.to_string(),
        request_id,
        prompt_text.to_string(),
    )
    .await?;
    crate::orchestrator::wake_up();

    Ok(json!({
        "job_id": returned_job_id,
        "status": "QUEUED",
    }))
}

pub async fn handle_job_wait(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let job_id = required_str(&params, "job_id")?.to_string();
    let timeout_secs = params.get("timeout").and_then(Value::as_u64).unwrap_or(30);
    let mut rx = crate::orchestrator::pubsub::subscribe_job_updates();

    if let Some(result) = terminal_job_response(ctx, &job_id).await? {
        return Ok(result);
    }

    let wait_future = async {
        loop {
            match rx.recv().await {
                Ok(updated_job_id) if updated_job_id == job_id => {
                    if let Some(result) = terminal_job_response(ctx, &job_id).await? {
                        return Ok(result);
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    if let Some(result) = terminal_job_response(ctx, &job_id).await? {
                        return Ok(result);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(AhError::IpcInvalidRequest(
                        "job update subscription closed".into(),
                    ));
                }
            }
        }
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), wait_future).await {
        Ok(result) => result,
        Err(_) => Err(AhError::PtyIoError(
            "Timeout waiting for job completion".into(),
        )),
    }
}

async fn terminal_job_response(ctx: &Ctx, job_id: &str) -> Result<Option<Value>, AhError> {
    let job = query_job(ctx.db.clone(), job_id.to_string())
        .await?
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;
    if matches!(job.status.as_str(), "COMPLETED" | "FAILED" | "CANCELLED") {
        Ok(Some(json!({
            "job_id": job.id,
            "status": job.status,
            "reply_text": job.reply_text,
            "error_reason": job.error_reason,
        })))
    } else {
        Ok(None)
    }
}

pub async fn handle_job_cancel(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let job_id = required_str(&params, "job_id")?.to_string();
    let job = query_job(ctx.db.clone(), job_id.clone())
        .await?
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;

    match job.status.as_str() {
        "QUEUED" => {
            let _ = mark_queued_job_cancelled(ctx.db.clone(), job_id.clone()).await?;
            Ok(json!({ "job_id": job_id, "status": "CANCELLED" }))
        }
        "DISPATCHED" => {
            let _ = request_dispatched_job_cancel(ctx.db.clone(), job_id.clone()).await?;
            if mark_dispatched_job_cancelled_if_agent_idle(ctx.db.clone(), job_id.clone()).await?
                > 0
            {
                return Ok(json!({ "job_id": job_id, "status": "CANCELLED" }));
            }
            let pane_id = crate::agent_io::pane_id(&job.agent_id).ok_or_else(|| {
                AhError::PtyIoError(format!("tmux pane not registered for {}", job.agent_id))
            })?;
            let agent = query_agent(ctx.db.clone(), job.agent_id.clone())
                .await?
                .ok_or_else(|| AhError::AgentNotFound(job.agent_id.clone()))?;
            for cancel_keysym in
                crate::provider::manifest::cancel_keysyms_for_provider(&agent.provider)
            {
                ctx.tmux_server
                    .send_keys_keysym(pane_id.clone(), (*cancel_keysym).to_string())
                    .await?;
            }
            let _ =
                mark_dispatched_job_cancelled_if_agent_idle(ctx.db.clone(), job_id.clone()).await?;
            spawn_cancel_settlement_watch(ctx.db.clone(), job_id.clone());
            Ok(json!({ "job_id": job_id, "status": "CANCEL_REQUESTED" }))
        }
        "COMPLETED" | "FAILED" | "CANCELLED" => Ok(json!({
            "job_id": job_id,
            "status": job.status,
        })),
        other => Err(AhError::IpcInvalidRequest(format!(
            "job {job_id} is in unknown status {other}"
        ))),
    }
}

fn spawn_cancel_settlement_watch(db: Db, job_id: String) {
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        while tokio::time::Instant::now() < deadline {
            match mark_dispatched_job_cancelled_if_agent_idle(db.clone(), job_id.clone()).await {
                Ok(changes) if changes > 0 => return,
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(job_id = %job_id, error = %err, "cancel settlement watch failed");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}
