use super::params::{
    optional_bool, optional_json_field, optional_string_array, parse_rule_fingerprint,
    required_str, required_string_array,
};
use crate::db::agents::query_agent;
use crate::db::events::insert_event_and_notify;
use crate::db::learned_rules::{
    CursorAnchor, LearnedRule, LearnedRuleCategory, ReplyExtractionSpec, RuleFingerprint,
    insert_learned_rule_sync, validate_learn_rule,
};
use crate::error::AhError;
use crate::rpc::Ctx;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

pub async fn handle_agent_resolve_prompt(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let agent_id = required_str(&params, "agent_id")?.to_string();
    let action_value = params
        .get("action")
        .cloned()
        .ok_or_else(|| AhError::IpcInvalidRequest("missing field 'action'".to_string()))?;
    let save_to_kb = optional_bool(&params, "save_to_kb", false)?;
    tracing::info!(
        agent_id,
        save_to_kb,
        "agent.resolve_prompt request received"
    );

    let actions = crate::prompt_handler::resolve::normalize_action_value(action_value)?;
    let agent = query_agent(ctx.db.clone(), agent_id.clone())
        .await?
        .ok_or_else(|| AhError::AgentNotFound(agent_id.clone()))?;
    let pane_id = crate::agent_io::pane_id(&agent_id)
        .ok_or_else(|| AhError::PtyIoError(format!("tmux pane not registered for {agent_id}")))?;
    let io = crate::prompt_handler::runner::TmuxPromptIo::new((*ctx.tmux_server).clone());
    let result = crate::prompt_handler::resolve::resolve_prompt_with_io(
        crate::prompt_handler::resolve::ResolvePromptRequest {
            db: ctx.db.clone(),
            agent_id: agent_id.clone(),
            pane_id,
            provider: agent.provider,
            kb_path: ctx.state_dir.join("prompt-cases.json"),
            actions,
            save_to_kb,
        },
        &io,
    )
    .await?;

    if result.resolved_state == crate::db::state_machine::STATE_IDLE {
        crate::orchestrator::wake_up();
    }

    Ok(json!({
        "status": "ok",
        "resolved_state": result.resolved_state,
        "saved_to_kb": result.saved_to_kb,
        "case_id": result.case_id,
        "action_sent": result.action_labels,
    }))
}

pub async fn handle_agent_learn_rule(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let agent_id = required_str(&params, "agent_id")?.to_string();
    let category = LearnedRuleCategory::parse(required_str(&params, "category")?)?;
    let fingerprint = parse_rule_fingerprint(&params)?;
    let RuleFingerprint::Regex { pattern } = &fingerprint;
    let regex_flags = optional_string_array(&params, "regex_flags")?.unwrap_or_default();
    let positive_examples = required_string_array(&params, "positive_examples")?;
    validate_learn_rule(pattern, &positive_examples)?;

    let agent = query_agent(ctx.db.clone(), agent_id.clone()).await?;
    let provider = match params.get("provider") {
        Some(value) => {
            let provider = value
                .as_str()
                .ok_or_else(|| AhError::IpcInvalidRequest("invalid field 'provider'".into()))?;
            let provider = crate::provider::manifest::canonicalize_provider_name(provider);
            if let Some(agent) = &agent {
                if provider != agent.provider {
                    return Err(AhError::IpcInvalidRequest(format!(
                        "provider {provider:?} does not match agent provider {:?}",
                        agent.provider
                    )));
                }
            }
            provider.to_string()
        }
        None => agent
            .as_ref()
            .map(|agent| agent.provider.clone())
            .unwrap_or_else(|| "unknown".to_string()),
    };

    let action =
        match params.get("action") {
            Some(Value::Null) | None => None,
            Some(value) => Some(serde_json::from_value(value.clone()).map_err(|err| {
                AhError::IpcInvalidRequest(format!("invalid field 'action': {err}"))
            })?),
        };
    let cursor_anchor = optional_json_field::<CursorAnchor>(&params, "cursor_anchor")?;
    let extraction = optional_json_field::<ReplyExtractionSpec>(&params, "extraction")?;
    if action.is_some() {
        return Err(AhError::IpcInvalidRequest(
            "action must be null for learned rules".to_string(),
        ));
    }
    if category != LearnedRuleCategory::ReplyExtraction && extraction.is_some() {
        return Err(AhError::IpcInvalidRequest(
            "extraction is only allowed for ReplyExtraction learned rules".to_string(),
        ));
    }
    let source_event_seq_id = match params.get("source_event_seq_id") {
        Some(Value::Null) | None => None,
        Some(value) => Some(value.as_i64().ok_or_else(|| {
            AhError::IpcInvalidRequest("invalid field 'source_event_seq_id'".to_string())
        })?),
    };

    let rule = LearnedRule {
        id: format!("lr_{}", Uuid::new_v4()),
        provider,
        category,
        fingerprint,
        regex_flags,
        positive_examples,
        action,
        extraction,
        cursor_anchor,
        source_event_seq_id,
        enabled: true,
    };
    insert_learned_rule_sync(&ctx.db, &rule)?;
    if query_agent(ctx.db.clone(), agent_id.clone())
        .await?
        .is_some()
    {
        insert_event_and_notify(
            ctx.db.clone(),
            agent_id.clone(),
            None,
            "rule_learned".to_string(),
            json!({
                "rule_id": rule.id,
                "provider": rule.provider,
                "category": rule.category,
                "source_event_seq_id": rule.source_event_seq_id,
            })
            .to_string(),
        )
        .await?;
    } else {
        tracing::debug!(
            agent_id = %agent_id,
            "agent not found; rule persisted, skipping rule_learned event"
        );
    }
    let init_probe_rescan_started = if rule.category == LearnedRuleCategory::StartupReadiness {
        crate::provider::init_probe_task::respawn_init_probe_for_agent(
            agent_id.clone(),
            ctx.tmux_server.clone(),
            Arc::new(ctx.db.clone()),
            ctx.state_dir.clone(),
        )
        .await?
    } else {
        false
    };
    crate::orchestrator::wake_up();

    Ok(json!({
        "status": "ok",
        "rule_id": rule.id,
        "provider": rule.provider,
        "category": rule.category,
        "init_probe_rescan_started": init_probe_rescan_started,
    }))
}
