use super::params::required_str;
use crate::db::Db;
use crate::db::agents::query_agent;
use crate::db::jobs::{
    insert_job_with_binding, mark_dispatched_job_cancelled_if_agent_idle,
    mark_queued_job_cancelled, query_job, request_dispatched_job_cancel,
};
use crate::error::CcbdError;
use crate::guarded_action::{ActionAssessment, ActionLoopPhase, run_guarded_action};
use crate::rpc::Ctx;
use crate::work_coordination::ExecutionBinding;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

pub async fn handle_job_submit(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let agent_id = required_str(&params, "agent_id")?;
    let supplied_prompt = required_str(&params, "text")?;
    let binding = params.get("governance_binding").cloned();
    let binding_json = binding.map(validate_governance_binding).transpose()?;
    let prompt_text = match binding_json.as_deref() {
        Some(binding) => format!(
            "System-owned execution identity and scope. Preserve every identity, stay inside the declared physical and semantic scope, and do not treat completion as acceptance or Effect proof.\n{binding}\n\nAssigned work:\n{supplied_prompt}"
        ),
        None => supplied_prompt.to_string(),
    };
    let mut request_id = params
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::to_string);

    let agent = query_agent(ctx.db.clone(), agent_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::AgentNotFound(agent_id.to_string()))?;
    if matches!(agent.state.as_str(), "CRASHED" | "KILLED") {
        return Err(CcbdError::AgentWrongState {
            current_state: agent.state,
        });
    }

    let job_id = format!("job_{}", Uuid::new_v4());
    if request_id.is_none() && binding_json.is_some() {
        request_id = Some(machine_request_id(&job_id));
    }
    let returned_job_id = insert_job_with_binding(
        ctx.db.clone(),
        job_id,
        agent_id.to_string(),
        request_id,
        prompt_text,
        binding_json,
    )
    .await?;
    crate::orchestrator::wake_up();

    Ok(json!({
        "job_id": returned_job_id,
        "status": "QUEUED",
    }))
}

fn validate_governance_binding(value: Value) -> Result<String, CcbdError> {
    ExecutionBinding::from_value(value)
        .and_then(|binding| binding.canonical_json())
        .map_err(|error| CcbdError::IpcInvalidRequest(error.to_string()))
}

pub async fn handle_job_wait(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
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
                    return Err(CcbdError::IpcInvalidRequest(
                        "job update subscription closed".into(),
                    ));
                }
            }
        }
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), wait_future).await {
        Ok(result) => result,
        Err(_) => Err(CcbdError::PtyIoError(
            "Timeout waiting for job completion".into(),
        )),
    }
}

async fn terminal_job_response(ctx: &Ctx, job_id: &str) -> Result<Option<Value>, CcbdError> {
    let job = query_job(ctx.db.clone(), job_id.to_string())
        .await?
        .ok_or_else(|| CcbdError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;
    if matches!(
        job.status.as_str(),
        "COMPLETED" | "FAILED" | "CANCELLED" | "KILLED"
    ) {
        let provider = query_agent(ctx.db.clone(), job.agent_id.clone())
            .await?
            .ok_or_else(|| CcbdError::AgentNotFound(job.agent_id.clone()))?
            .provider;
        Ok(Some(json!({
            "job_id": job.id,
            "agent_id": job.agent_id,
            "provider": provider,
            "request_id": job.request_id,
            "status": job.status,
            "reply_text": job.reply_text,
            "error_reason": job.error_reason,
            "governance_binding": job
                .governance_binding_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<Value>(value).ok()),
        })))
    } else {
        Ok(None)
    }
}

fn machine_request_id(job_id: &str) -> String {
    format!("request_{job_id}")
}

pub async fn handle_job_cancel(params: Value, ctx: &Ctx) -> Result<Value, CcbdError> {
    let job_id = required_str(&params, "job_id")?.to_string();
    let job = query_job(ctx.db.clone(), job_id.clone())
        .await?
        .ok_or_else(|| CcbdError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;

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
                CcbdError::PtyIoError(format!("tmux pane not registered for {}", job.agent_id))
            })?;
            let agent = query_agent(ctx.db.clone(), job.agent_id.clone())
                .await?
                .ok_or_else(|| CcbdError::AgentNotFound(job.agent_id.clone()))?;
            let outcome = run_guarded_action(
                "request_provider_cancel",
                Duration::from_secs(2),
                Duration::from_millis(50),
                {
                    let db = ctx.db.clone();
                    let job_id = job_id.clone();
                    move || {
                        let db = db.clone();
                        let job_id = job_id.clone();
                        async move {
                            let _ = mark_dispatched_job_cancelled_if_agent_idle(
                                db.clone(),
                                job_id.clone(),
                            )
                            .await?;
                            query_job(db, job_id).await?.ok_or_else(|| {
                                CcbdError::IpcInvalidRequest(
                                    "cancelled job disappeared during confirmation".to_string(),
                                )
                            })
                        }
                    }
                },
                {
                    let tmux = ctx.tmux_server.clone();
                    let pane_id = pane_id.clone();
                    let provider = agent.provider.clone();
                    move || async move {
                        for cancel_keysym in
                            crate::provider::manifest::cancel_keysyms_for_provider(&provider)
                        {
                            tmux.send_keys_keysym(pane_id.clone(), (*cancel_keysym).to_string())
                                .await?;
                        }
                        Ok(())
                    }
                },
                |before, after| assess_cancel_transition(&before.value.status, &after.value.status),
            )
            .await;

            match outcome {
                Ok(_) => Ok(json!({ "job_id": job_id, "status": "CANCELLED" })),
                Err(error) if error.phase == ActionLoopPhase::TimedOut => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %error,
                        "provider cancel was not yet confirmed; keeping the job in CANCEL_REQUESTED settlement"
                    );
                    spawn_cancel_settlement_watch(ctx.db.clone(), job_id.clone());
                    Ok(json!({ "job_id": job_id, "status": "CANCEL_REQUESTED" }))
                }
                Err(error) => Err(CcbdError::PtyIoError(error.to_string())),
            }
        }
        "COMPLETED" | "FAILED" | "CANCELLED" => Ok(json!({
            "job_id": job_id,
            "status": job.status,
        })),
        other => Err(CcbdError::IpcInvalidRequest(format!(
            "job {job_id} is in unknown status {other}"
        ))),
    }
}

fn assess_cancel_transition(before_status: &str, after_status: &str) -> ActionAssessment {
    if before_status == "DISPATCHED" && after_status == "CANCELLED" {
        ActionAssessment::Confirmed
    } else {
        ActionAssessment::Mismatch {
            reason: format!("job remains {after_status} after provider cancel request"),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_binding() -> Value {
        json!({
            "schema_version": 1,
            "roadmap_stream": "delivery",
            "roadmap_node_id": "NODE-1",
            "plan_id": "PLAN-1",
            "plan_revision": "sha256:plan",
            "plan_step_id": "STEP-1",
            "task_id": "TASK-1",
            "attempt_id": "ATTEMPT-1",
            "run_id": "RUN-1",
            "context_id": "CONTEXT-1",
            "episode_id": "EPISODE-1",
            "module_ref": "agent_runtime",
            "capability_refs": ["provider_dispatch"],
            "target_spec_locator": "module_tree/agent_runtime.md",
            "target_spec_revision": "sha256:spec",
            "work_phase": "implementation",
            "physical_scope": ["module_tree/agent_runtime"],
            "semantic_scope": ["agent_runtime.provider_dispatch"],
            "worktree_path": "/tmp/task-1",
            "program_revision": "sha256:program",
            "topology_revision": "sha256:topology"
        })
    }

    #[test]
    fn governance_binding_is_exact_and_canonical() {
        let canonical = validate_governance_binding(valid_binding()).unwrap();
        let parsed: Value = serde_json::from_str(&canonical).unwrap();
        assert_eq!(parsed["run_id"], "RUN-1");
        assert_eq!(
            parsed.as_object().unwrap().len(),
            crate::work_coordination::EXECUTION_BINDING_FIELDS.len()
        );
    }

    #[test]
    fn governance_binding_refuses_missing_identity() {
        let mut binding = valid_binding();
        binding.as_object_mut().unwrap().remove("context_id");
        let error = validate_governance_binding(binding).unwrap_err();
        assert!(error.to_string().contains("context_id"));
    }

    #[test]
    fn provider_cancel_requires_a_causal_terminal_transition() {
        assert_eq!(
            assess_cancel_transition("DISPATCHED", "CANCELLED"),
            ActionAssessment::Confirmed
        );
        assert!(matches!(
            assess_cancel_transition("DISPATCHED", "DISPATCHED"),
            ActionAssessment::Mismatch { .. }
        ));
        assert!(matches!(
            assess_cancel_transition("CANCELLED", "CANCELLED"),
            ActionAssessment::Mismatch { .. }
        ));
    }

    #[test]
    fn machine_request_identity_is_derived_from_the_unique_job_identity() {
        let first = machine_request_id("job_first");
        let second = machine_request_id("job_second");
        assert_eq!(first, "request_job_first");
        assert_eq!(second, "request_job_second");
        assert_ne!(first, second);
    }
}
