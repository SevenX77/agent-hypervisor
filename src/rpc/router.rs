use crate::error::AhError;
use crate::rpc::Ctx;
use crate::rpc::handlers::{
    handle_agent_assert_state, handle_agent_discard_evidence, handle_agent_kill,
    handle_agent_learn_rule, handle_agent_notify, handle_agent_read, handle_agent_realign,
    handle_agent_resolve_prompt, handle_agent_send, handle_agent_spawn, handle_agent_watch,
    handle_event_subscribe, handle_evidence_insert, handle_job_cancel, handle_job_has_evidence,
    handle_job_mark_requires_evidence, handle_job_submit, handle_job_wait, handle_master_ack_ready,
    handle_master_tell_begin, handle_master_tell_failed, handle_runtime_snapshot,
    handle_session_create, handle_session_kill, handle_session_list, handle_session_master_cutover,
    handle_session_realign, handle_session_spawn_master_pane, handle_system_dump,
    handle_system_shutdown,
};
use serde_json::{Value, json};

const METHODS: &[&str] = &[
    "session.create",
    "session.kill",
    "session.spawn_master_pane",
    "session.master_cutover",
    "master.ack_ready",
    "master.tell_begin",
    "master.tell_failed",
    "session.realign",
    "session.list",
    "agent.spawn",
    "agent.realign",
    "agent.send",
    "agent.read",
    "agent.watch",
    "agent.kill",
    "agent.notify",
    "agent.resolve_prompt",
    "agent.learn_rule",
    "agent.assert_state",
    "agent.discard_evidence",
    "evidence.insert",
    "event.subscribe",
    "runtime.snapshot",
    "runtime.subscribe",
    "job.has_evidence",
    "job.mark_requires_evidence",
    "job.submit",
    "job.wait",
    "job.cancel",
    "system.dump",
    "system.shutdown",
];

pub async fn dispatch(line: &str, ctx: &Ctx) -> String {
    let request: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(err) => {
            return error_response(
                Value::Null,
                AhError::IpcInvalidRequest(format!("malformed JSON: {err}")),
            );
        }
    };

    let id = request.get("id").cloned().unwrap_or(Value::Null);

    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return error_response(
            id,
            AhError::IpcInvalidRequest("missing or invalid jsonrpc version".into()),
        );
    }

    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return error_response(
            id,
            AhError::IpcInvalidRequest("missing field 'method'".into()),
        );
    };

    if !METHODS.contains(&method) {
        return error_response(
            id,
            AhError::IpcInvalidRequest(format!("unknown method: {method}")),
        );
    }

    let params = request.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "session.create" => handle_session_create(params, ctx).await,
        "session.kill" => handle_session_kill(params, ctx).await,
        "session.spawn_master_pane" => handle_session_spawn_master_pane(params, ctx).await,
        "session.master_cutover" => handle_session_master_cutover(params, ctx).await,
        "master.ack_ready" => handle_master_ack_ready(params, ctx).await,
        "master.tell_begin" => handle_master_tell_begin(params, ctx).await,
        "master.tell_failed" => handle_master_tell_failed(params, ctx).await,
        "session.realign" => handle_session_realign(params, ctx).await,
        "session.list" => handle_session_list(params, ctx).await,
        "agent.spawn" => handle_agent_spawn(params, ctx).await,
        "agent.realign" => handle_agent_realign(params, ctx).await,
        "agent.send" => handle_agent_send(params, ctx).await,
        "agent.read" => handle_agent_read(params, ctx).await,
        "agent.watch" => handle_agent_watch(params, ctx).await,
        "agent.kill" => handle_agent_kill(params, ctx).await,
        "agent.notify" => handle_agent_notify(params, ctx).await,
        "agent.resolve_prompt" => handle_agent_resolve_prompt(params, ctx).await,
        "agent.learn_rule" => handle_agent_learn_rule(params, ctx).await,
        "agent.assert_state" => handle_agent_assert_state(params, ctx).await,
        "agent.discard_evidence" => handle_agent_discard_evidence(params, ctx).await,
        "evidence.insert" => handle_evidence_insert(params, ctx).await,
        "event.subscribe" => handle_event_subscribe(params, ctx).await,
        "runtime.snapshot" => handle_runtime_snapshot(params, ctx).await,
        "runtime.subscribe" => handle_runtime_snapshot(params, ctx).await,
        "job.has_evidence" => handle_job_has_evidence(params, ctx).await,
        "job.mark_requires_evidence" => handle_job_mark_requires_evidence(params, ctx).await,
        "job.submit" => handle_job_submit(params, ctx).await,
        "job.wait" => handle_job_wait(params, ctx).await,
        "job.cancel" => handle_job_cancel(params, ctx).await,
        "system.dump" => handle_system_dump(params, ctx).await,
        "system.shutdown" => handle_system_shutdown(params, ctx).await,
        _ => unreachable!("method whitelist checked above"),
    };

    match result {
        Ok(value) => success_response(id, value),
        Err(AhError::IpcInvalidRequest(message))
            if method == "job.mark_requires_evidence" && message.contains("job_id not found") =>
        {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": "IPC_INVALID_REQUEST",
                    "message": format!("invalid IPC request: {message}"),
                    "data": {"error_code": "IPC_INVALID_REQUEST"}
                }
            })
            .to_string()
        }
        Err(AhError::DuplicateRequest { existing_seq_id }) => error_response(
            id,
            AhError::IpcInvalidRequest(format!(
                "internal duplicate request sentinel leaked for seq_id={existing_seq_id}"
            )),
        ),
        Err(err) => error_response(id, err),
    }
}

fn success_response(id: Value, result: Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
    .to_string()
}

fn error_response(id: Value, err: AhError) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": err.to_rpc_error(),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::learned_rules::{LearnedRuleCategory, lookup_learned_rules_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::rpc::Ctx;
    use crate::sandbox::EnvState;
    use crate::tmux::TmuxServer;
    use serde_json::{Value, json};
    use std::sync::Arc;

    fn test_ctx() -> Ctx {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        Ctx {
            db: db::init(file.path()).unwrap(),
            state_dir: state_dir.clone(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(&state_dir)),
            claude_gateway: Arc::new(crate::claude_gateway::ClaudeGatewayService::new()),
        }
    }

    fn seed_agent(ctx: &Ctx, agent_id: &str, state: &str) {
        let conn = ctx.db.conn();
        insert_session_sync(&conn, "s_notify", "p_notify", "/tmp/notify").unwrap();
        insert_agent_sync(&conn, agent_id, "s_notify", "codex", state, Some(123)).unwrap();
    }

    fn seed_dispatched_job(ctx: &Ctx, agent_id: &str, job_id: &str) {
        let conn = ctx.db.conn();
        insert_job_sync(&conn, job_id, agent_id, None, "echo PONG\n").unwrap();
        conn.execute(
            "UPDATE jobs SET status='DISPATCHED', dispatched_at=unixepoch() WHERE id=?",
            [job_id],
        )
        .unwrap();
    }

    fn seed_master_session(ctx: &Ctx, session_id: &str, generation: i64) {
        let conn = ctx.db.conn();
        insert_session_sync(&conn, session_id, "p_master_notify", "/tmp/master-notify").unwrap();
        conn.execute(
            "UPDATE sessions SET master_generation = ?2 WHERE id = ?1",
            rusqlite::params![session_id, generation],
        )
        .unwrap();
    }

    fn master_state(ctx: &Ctx, session_id: &str) -> String {
        ctx.db
            .conn()
            .query_row(
                "SELECT master_state FROM sessions WHERE id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn test_dispatch_invalid_json_returns_ipc_invalid_request() {
        let ctx = test_ctx();
        let response = dispatch("not json", &ctx).await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["jsonrpc"], "2.0");
        assert_eq!(obj["id"], Value::Null);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
    }

    #[tokio::test]
    async fn test_dispatch_unknown_method_returns_ipc_invalid_request() {
        let ctx = test_ctx();
        let response = dispatch(r#"{"jsonrpc":"2.0","method":"weird.foo","id":1}"#, &ctx).await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 1);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("weird.foo")
        );
    }

    #[tokio::test]
    async fn test_dispatch_known_method_stub() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.spawn","params":{},"id":2}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 2);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'session_id'")
        );
    }

    #[tokio::test]
    async fn test_agent_notify_rejects_missing_agent_id() {
        let ctx = test_ctx();
        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "event": "stop",
                    "provider": "codex",
                    "event_id": "evt-missing-agent"
                },
                "id": 21
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 21);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("agent_id")
        );
    }

    #[tokio::test]
    async fn test_agent_notify_rejects_unknown_agent() {
        let ctx = test_ctx();
        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "agent_id": "missing_agent",
                    "event": "stop",
                    "provider": "codex",
                    "event_id": "evt-unknown-agent"
                },
                "id": 22
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 22);
        assert_eq!(obj["error"]["data"]["error_code"], "AGENT_NOT_FOUND");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing_agent")
        );
    }

    #[tokio::test]
    async fn test_agent_notify_worker_userpromptsubmit_is_unsupported_noop() {
        let ctx = test_ctx();
        seed_agent(&ctx, "ag_notify_busy", "BUSY");

        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "agent_id": "ag_notify_busy",
                    "event": "userpromptsubmit",
                    "provider": "codex",
                    "event_id": "evt-ups"
                },
                "id": 23
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 23);
        assert_eq!(obj["result"]["accepted"], true);
        assert_eq!(obj["result"]["unsupported"], true);
        assert_eq!(obj["result"]["transitioned"], false);
        assert_eq!(master_state(&ctx, "s_notify"), "IDLE");
        let state: String = ctx
            .db
            .conn()
            .query_row(
                "SELECT state FROM agents WHERE id = 'ag_notify_busy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "BUSY");
    }

    #[tokio::test]
    async fn test_agent_notify_master_userpromptsubmit_sets_busy_without_agent_row_or_request_id() {
        let ctx = test_ctx();
        seed_master_session(&ctx, "s_master_notify", 7);

        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "agent_id": "master:s_master_notify:7",
                    "event": "userpromptsubmit",
                    "provider": "claude",
                    "event_id": "evt-master-ups"
                },
                "id": 24
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 24);
        assert_eq!(obj["result"]["accepted"], true);
        assert_eq!(obj["result"]["role"], "master");
        assert_eq!(obj["result"]["transitioned"], true);
        assert_eq!(master_state(&ctx, "s_master_notify"), "BUSY");
    }

    #[tokio::test]
    async fn test_agent_notify_master_stop_sets_idle_and_clears_pending_request() {
        let ctx = test_ctx();
        seed_master_session(&ctx, "s_master_stop", 3);
        ctx.db
            .conn()
            .execute(
                "UPDATE sessions SET master_state = 'BUSY', master_pending_tell_request = 'tell_123' WHERE id = 's_master_stop'",
                [],
            )
            .unwrap();

        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "agent_id": "master:s_master_stop:3",
                    "event": "stop",
                    "provider": "claude",
                    "event_id": "evt-master-stop"
                },
                "id": 25
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 25);
        assert_eq!(obj["result"]["transitioned"], true);
        assert_eq!(obj["result"]["request_id"], "tell_123");
        let row: (String, Option<String>) = ctx
            .db
            .conn()
            .query_row(
                "SELECT master_state, master_pending_tell_request FROM sessions WHERE id = 's_master_stop'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("IDLE".to_string(), None));
    }

    #[tokio::test]
    async fn test_agent_notify_master_stale_generation_is_accepted_but_ignored() {
        let ctx = test_ctx();
        seed_master_session(&ctx, "s_master_stale", 8);
        ctx.db
            .conn()
            .execute(
                "UPDATE sessions SET master_state = 'BUSY' WHERE id = 's_master_stale'",
                [],
            )
            .unwrap();

        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "agent_id": "master:s_master_stale:7",
                    "event": "stop",
                    "provider": "claude",
                    "event_id": "evt-master-stale"
                },
                "id": 26
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 26);
        assert_eq!(obj["result"]["accepted"], true);
        assert_eq!(obj["result"]["ignored_stale"], true);
        assert_eq!(obj["result"]["transitioned"], false);
        assert_eq!(master_state(&ctx, "s_master_stale"), "BUSY");
    }

    #[tokio::test]
    async fn test_master_tell_begin_records_pending_request_without_marking_busy() {
        let ctx = test_ctx();
        seed_master_session(&ctx, "s_master_tell", 1);

        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "master.tell_begin",
                "params": {
                    "session_id": "s_master_tell",
                    "request_id": "tell_test",
                    "pane_id": "%42"
                },
                "id": 27
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 27);
        assert_eq!(obj["result"]["registered"], true);
        let row: (String, Option<String>) = ctx
            .db
            .conn()
            .query_row(
                "SELECT master_state, master_pending_tell_request FROM sessions WHERE id = 's_master_tell'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("IDLE".to_string(), Some("tell_test".to_string())));
    }

    #[tokio::test]
    async fn test_agent_notify_success_cancels_pull_monitor_and_late_log_is_idempotent() {
        let ctx = test_ctx();
        seed_agent(&ctx, "ag_notify_push_wins", "BUSY");
        seed_dispatched_job(&ctx, "ag_notify_push_wins", "job_notify_push_wins");
        let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel();
        crate::completion::registry::register(
            "ag_notify_push_wins".to_string(),
            crate::completion::registry::LogMonitorEntry {
                provider: "codex".to_string(),
                log_root: tempfile::tempdir().unwrap().path().to_path_buf(),
                state: crate::completion::reader::LogReadState::default(),
                cancel_tx,
            },
        );

        let response = dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "agent.notify",
                "params": {
                    "agent_id": "ag_notify_push_wins",
                    "event": "stop",
                    "provider": "codex",
                    "event_id": "evt-push-wins",
                    "reply": "HOOK PONG"
                },
                "id": 24
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["result"]["transitioned"], true);
        assert_eq!(obj["result"]["affected_job_id"], "job_notify_push_wins");
        assert!(
            !crate::completion::registry::contains("ag_notify_push_wins"),
            "push success must cancel the pull monitor registry entry"
        );

        let root = tempfile::TempDir::new().unwrap();
        std::fs::write(
            root.path().join("rollout-session.jsonl"),
            r#"{"type":"event_msg","payload":{"type":"task_complete","turn_id":"late","last_agent_message":"LATE LOG PONG"}}"#,
        )
        .unwrap();
        let late = crate::completion::monitor::run_log_monitor_tick(
            ctx.db.clone(),
            "ag_notify_push_wins",
            "codex",
            root.path(),
            crate::completion::reader::LogReadState::default(),
        )
        .await
        .unwrap();
        let job = query_job_sync(&ctx.db.conn(), "job_notify_push_wins")
            .unwrap()
            .unwrap();

        assert!(!late.completed);
        assert_eq!(job.status, "COMPLETED");
        assert_eq!(job.reply_text.as_deref(), Some("HOOK PONG"));
    }

    #[tokio::test]
    async fn test_dispatch_session_kill_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"session.kill","params":{},"id":6}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 6);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'session_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_session_spawn_master_pane_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"session.spawn_master_pane","params":{},"id":10}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 10);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'session_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_job_cancel_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"job.cancel","params":{},"id":8}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 8);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'job_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_session_list_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(r#"{"jsonrpc":"2.0","method":"session.list","id":9}"#, &ctx).await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 9);
        assert!(obj["result"]["sessions"].as_array().is_some());
    }

    #[tokio::test]
    async fn test_dispatch_job_submit_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"job.submit","params":{},"id":3}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 3);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'agent_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_job_wait_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"job.wait","params":{},"id":4}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 4);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'job_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_agent_watch_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.watch","params":{},"id":5}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 5);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'agent_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_agent_resolve_prompt_method_registered() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.resolve_prompt","params":{},"id":11}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 11);
        assert_eq!(obj["error"]["code"], -32000);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("missing or invalid field 'agent_id'")
        );
    }

    #[tokio::test]
    async fn test_dispatch_agent_learn_rule_method_registered_for_learned_rules() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.learn_rule","params":{"agent_id":"ag1","category":"StartupReadiness","fingerprint":{"type":"regex","pattern":"(?m)^\\s*❯"},"positive_examples":["❯ Try \"fix lint errors\""]},"id":12}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 12);
        assert_eq!(obj["result"]["status"], "ok");
    }

    #[tokio::test]
    async fn test_dispatch_agent_learn_rule_persists_noisy_positive_rule() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.learn_rule","params":{"agent_id":"ag1","provider":"claude","category":"StartupReadiness","fingerprint":{"type":"regex","pattern":"(?m)^\\s*❯"},"positive_examples":["❯ Try \"fix lint errors\"\nOpus 4.8 (1M context)"],"cursor_anchor":{"cursor_row_delta_from_match":0,"cursor_col_delta_from_match_end":1}},"id":13}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 13);
        assert_eq!(obj["result"]["status"], "ok");

        let found =
            lookup_learned_rules_sync(&ctx.db, "claude", LearnedRuleCategory::StartupReadiness)
                .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].provider, "claude");
        assert_eq!(
            found[0].positive_examples,
            vec!["❯ Try \"fix lint errors\"\nOpus 4.8 (1M context)".to_string()]
        );
    }

    #[tokio::test]
    async fn test_dispatch_agent_learn_rule_rejects_too_tight_regex_without_persisting() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.learn_rule","params":{"agent_id":"ag1","provider":"claude","category":"StartupReadiness","fingerprint":{"type":"regex","pattern":"(?m)^\\s*❯\\s*$"},"positive_examples":["❯ Try \"fix lint errors\""]},"id":14}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 14);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("positive")
        );
        let found =
            lookup_learned_rules_sync(&ctx.db, "claude", LearnedRuleCategory::StartupReadiness)
                .unwrap();
        assert!(found.is_empty());
    }

    #[tokio::test]
    async fn test_dispatch_agent_learn_rule_rejects_action_for_runtime_marker() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.learn_rule","params":{"agent_id":"ag1","provider":"claude","category":"RuntimeMarker","fingerprint":{"type":"regex","pattern":"(?m)^READY"},"positive_examples":["READY"],"action":[{"type":"key","value":"Enter"}]},"id":15}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 15);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(obj["error"]["message"].as_str().unwrap().contains("action"));
    }

    #[tokio::test]
    async fn test_dispatch_agent_learn_rule_rejects_extraction_for_non_reply_extraction() {
        let ctx = test_ctx();
        let response = dispatch(
            r#"{"jsonrpc":"2.0","method":"agent.learn_rule","params":{"agent_id":"ag1","provider":"claude","category":"StartupReadiness","fingerprint":{"type":"regex","pattern":"(?m)^READY"},"positive_examples":["READY"],"extraction":{"start":{"LastPromptEcho":{"prompt_markers":["> "]}},"end":{"NextRegex":{"pattern":"(?m)^READY"}},"drop_lines":[]}},"id":16}"#,
            &ctx,
        )
        .await;
        let obj: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 16);
        assert_eq!(obj["error"]["data"]["error_code"], "IPC_INVALID_REQUEST");
        assert!(
            obj["error"]["message"]
                .as_str()
                .unwrap()
                .contains("extraction")
        );
    }
}
