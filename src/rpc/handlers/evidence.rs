use super::params::required_str;
use crate::db::evidence::{discard_evidence, has_job_evidence_for_path, insert_evidence_record};
use crate::db::jobs::{query_job, set_job_evidence_requirements};
use crate::db::state_machine_assert::assert_state_to_idle;
use crate::error::AhError;
use crate::rpc::Ctx;
use serde_json::{Value, json};

pub async fn handle_evidence_insert(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let agent_id = required_str(&params, "agent_id")?;
    let job_id = required_str(&params, "job_id")?;
    let evidence_type = required_str(&params, "evidence_type")?;
    let subject_path = required_str(&params, "subject_path")?;
    let payload = params.get("payload").cloned().unwrap_or_else(|| json!({}));

    let evidence_id = insert_evidence_record(
        ctx.db.clone(),
        agent_id.to_string(),
        Some(job_id.to_string()),
        evidence_type.to_string(),
        Some(subject_path.to_string()),
        payload,
    )
    .await?;

    Ok(json!({
        "evidence_id": evidence_id,
        "recorded": true,
    }))
}

pub async fn handle_job_has_evidence(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let job_id = required_str(&params, "job_id")?;
    let evidence_type = required_str(&params, "evidence_type")?;
    let subject_path = required_str(&params, "subject_path")?;

    let has_evidence = has_job_evidence_for_path(
        ctx.db.clone(),
        job_id.to_string(),
        evidence_type.to_string(),
        subject_path.to_string(),
    )
    .await?;

    Ok(json!({ "has_evidence": has_evidence }))
}

pub async fn handle_job_mark_requires_evidence(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let job_id = required_str(&params, "job_id")?;
    let job = query_job(ctx.db.clone(), job_id.to_string())
        .await?
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("job_id not found: {job_id}")))?;
    set_job_evidence_requirements(
        ctx.db.clone(),
        job_id.to_string(),
        true,
        job.requires_test_evidence,
    )
    .await?;

    Ok(json!({
        "job_id": job_id,
        "requires_physical_evidence": true,
        "requires_test_evidence": job.requires_test_evidence,
    }))
}

pub async fn handle_agent_assert_state(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let agent_id = required_str(&params, "agent_id")?;
    let state = required_str(&params, "state")?;
    let evidence_id = required_str(&params, "evidence_id")?;
    if state != "IDLE" {
        return Err(AhError::IpcInvalidRequest(
            "assert_state only accepts state=IDLE".into(),
        ));
    }

    let _outcome = assert_state_to_idle(
        ctx.db.clone(),
        agent_id.to_string(),
        evidence_id.to_string(),
    )
    .await?;

    Ok(json!({ "state": "IDLE", "sub_state": "Asserted" }))
}

pub async fn handle_agent_discard_evidence(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let evidence_id = required_str(&params, "evidence_id")?;
    discard_evidence(ctx.db.clone(), evidence_id.to_string()).await?;

    Ok(json!({ "status": "DISCARDED" }))
}
