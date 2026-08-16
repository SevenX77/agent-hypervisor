use super::agent::{AgentSpawnDbAction, bare_config_env, handle_agent_spawn_with_db_action};
use super::params::{optional_bool, required_str};
use super::sessions::{
    handle_session_spawn_master_pane, session_anchors_enabled, stop_session_anchor,
};
use crate::db::agents::delete_agent;
use crate::db::agents_lifecycle::mark_agent_killed;
use crate::db::events::insert_event;
use crate::db::sessions::{query_session_by_id, update_session_config_hash};
use crate::error::AhError;
use crate::master_revival::{
    MasterTransitionOutcome, master_spawn_lock, query_master_runtime, try_claim_master_transition,
};
use crate::monitor::session_watch::unit_name_for_session;
use crate::provider::bundles::{BundleRole, digest_for_bundles};
use crate::provider::fingerprint::BundleDigest;
use crate::provider::fingerprint::{ConfigFingerprintInput, ConfigRole, compute_config_hash};
use crate::rpc::Ctx;
use crate::sandbox::SandboxOverrides;
use crate::tmux::TmuxWindowSize;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// ISSUE-13 §4b (commit C): minimum spacing between consecutive destructive respawns in a
/// single realign, so a mass drift (e.g. one project `[env]` edit drifting every agent) is
/// spread over time instead of bursting simultaneous tmux pane creation on the
/// single-threaded tmux server. K drifting agents ⇒ realign wall-time ≥ (K-1) × this.
const MIN_RESPAWN_STAGGER_MS: u64 = 500;

#[derive(Debug, Deserialize, Serialize)]
struct RealignMasterParams {
    #[serde(default)]
    cmd: String,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    hooks: HashMap<String, Vec<crate::provider::extensions::HookGroup>>,
    #[serde(default)]
    plugins: Vec<String>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    bundle: Vec<String>,
    #[serde(default)]
    settings: Map<String, Value>,
    #[serde(default)]
    bundle_digest: Option<BundleDigest>,
    #[serde(default)]
    tmux_window_size: TmuxWindowSize,
    #[serde(default)]
    claude_shared_credentials_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RealignAgentParams {
    pub(crate) agent_id: String,
    pub(crate) provider: String,
    #[serde(default)]
    pub(crate) env: HashMap<String, String>,
    #[serde(default)]
    pub(crate) hooks: HashMap<String, Vec<crate::provider::extensions::HookGroup>>,
    #[serde(default)]
    pub(crate) plugins: Vec<String>,
    #[serde(default)]
    pub(crate) skills: Vec<String>,
    #[serde(default)]
    pub(crate) bundle: Vec<String>,
    #[serde(default)]
    pub(crate) settings: Map<String, Value>,
    #[serde(default)]
    pub(crate) bundle_digest: Option<BundleDigest>,
    #[serde(default)]
    pub(crate) sandbox_overrides: SandboxOverrides,
    #[serde(default)]
    pub(crate) hook_push_enabled: bool,
    #[serde(default)]
    pub(crate) claude_shared_credentials_dir: Option<PathBuf>,
}

#[derive(Debug)]
struct RunningAgentConfigHash {
    id: String,
    provider: String,
    state: String,
    config_hash: Option<String>,
}

fn populate_bundle_digests(
    project_root: &std::path::Path,
    master: &mut RealignMasterParams,
    agents: &mut [RealignAgentParams],
) -> Result<(), AhError> {
    if master.bundle_digest.is_none() {
        master.bundle_digest =
            digest_for_bundles(project_root, BundleRole::Master, &master.bundle)?;
    }
    for agent in agents {
        if agent.bundle_digest.is_none() {
            agent.bundle_digest =
                digest_for_bundles(project_root, BundleRole::Worker, &agent.bundle)?;
        }
    }
    Ok(())
}

pub async fn handle_session_realign(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let session_id = required_str(&params, "session_id")?.to_string();
    let force = optional_bool(&params, "force", false)?;
    let skip_master = optional_bool(&params, "_skip_master", false)?;
    let mut master: RealignMasterParams = serde_json::from_value(
        params
            .get("master")
            .cloned()
            .ok_or_else(|| AhError::IpcInvalidRequest("missing field 'master'".into()))?,
    )
    .map_err(|err| AhError::IpcInvalidRequest(format!("invalid master: {err}")))?;
    let mut agents: Vec<RealignAgentParams> = serde_json::from_value(
        params
            .get("agents")
            .cloned()
            .ok_or_else(|| AhError::IpcInvalidRequest("missing field 'agents'".into()))?,
    )
    .map_err(|err| AhError::IpcInvalidRequest(format!("invalid agents: {err}")))?;
    // ISSUE-13 §3a: project-level `[env]` carried raw; the server merges it into each
    // agent's fingerprint env via the same `bare_config_env` helper the spawn side uses.
    let config_env: HashMap<String, String> = match params.get("config_env") {
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|err| AhError::IpcInvalidRequest(format!("invalid config_env: {err}")))?,
        None => HashMap::new(),
    };

    let session = query_session_by_id(ctx.db.clone(), session_id.clone())
        .await?
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("session not found: {session_id}")))?;
    let project_root = std::path::PathBuf::from(&session.absolute_path);
    populate_bundle_digests(&project_root, &mut master, &mut agents)?;
    let mut results = Vec::new();
    if skip_master {
        results.push(json!({
            "role": "master",
            "status": "SKIPPED",
            "message": "master diff skipped for agent.realign",
        }));
    } else if session.config_hash.as_deref()
        == Some(
            compute_config_hash(&ConfigFingerprintInput {
                role: ConfigRole::Master {
                    cmd: &master.cmd,
                    env: &master.env,
                },
                hooks: &master.hooks,
                plugins: &master.plugins,
                skills: &master.skills,
                settings: &master.settings,
                bundle: master.bundle_digest.as_ref(),
            })?
            .as_str(),
        )
    {
        results.push(json!({
            "role": "master",
            "status": "NO_CHANGE",
            "message": "master up to date",
        }));
    } else if force {
        let expected_master_hash = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Master {
                cmd: &master.cmd,
                env: &master.env,
            },
            hooks: &master.hooks,
            plugins: &master.plugins,
            skills: &master.skills,
            settings: &master.settings,
            bundle: master.bundle_digest.as_ref(),
        })?;
        let spawn_lock = master_spawn_lock(&session_id);
        let _master_spawn_guard = spawn_lock.lock().await;
        let runtime = query_master_runtime(&ctx.db, &session_id)?.ok_or_else(|| {
            AhError::IpcInvalidRequest(format!("active master runtime not found: {session_id}"))
        })?;
        match try_claim_master_transition(&ctx.db, &session_id, runtime.pid, runtime.generation)? {
            MasterTransitionOutcome::Claimed => {}
            MasterTransitionOutcome::Stale | MasterTransitionOutcome::NoChange => {
                results.push(json!({
                    "role": "master",
                    "status": "NO_CHANGE",
                    "message": "master realign skipped; newer master already present",
                }));
                return Ok(json!({ "session_id": session_id, "results": results }));
            }
        }
        let claimed_generation = runtime.generation + 1;
        if session_anchors_enabled(ctx) {
            stop_session_anchor(&unit_name_for_session(&session_id));
        }
        let _spawned = handle_session_spawn_master_pane(
            json!({
                "session_id": session_id.clone(),
                "cmd": master.cmd.clone(),
                "hooks": master.hooks.clone(),
                "plugins": master.plugins.clone(),
                "skills": master.skills.clone(),
                "bundle": master.bundle.clone(),
                "settings": master.settings.clone(),
                "tmux_window_size": master.tmux_window_size,
                "_claimed_master_generation": claimed_generation,
                "claude_shared_credentials_dir": master.claude_shared_credentials_dir.clone(),
            }),
            ctx,
        )
        .await?;
        update_session_config_hash(ctx.db.clone(), session_id.clone(), expected_master_hash)
            .await?;
        results.push(json!({
            "role": "master",
            "status": "REALIGNED",
            "action": "master_realign",
            "message": "master DRIFT force REALIGNED",
        }));
    } else {
        results.push(json!({
            "role": "master",
            "status": "DRIFT",
            "message": "master DRIFT audit-only",
        }));
    }

    let running_agents = running_agent_hashes(ctx, &session_id)?;
    let requested_ids = agents
        .iter()
        .map(|agent| agent.agent_id.clone())
        .collect::<std::collections::HashSet<_>>();

    let mut destructive_respawns = 0usize;
    for agent in &agents {
        // ISSUE-13 §3b: fingerprint the SAME bare env the spawn side captured —
        // merge(config_env, agent.env) — never the raw agent.env alone (Leak B).
        let expected_env = bare_config_env(&config_env, &agent.env);
        let expected_hash = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Agent {
                provider: &agent.provider,
                env: &expected_env,
            },
            hooks: &agent.hooks,
            plugins: &agent.plugins,
            skills: &agent.skills,
            settings: &agent.settings,
            bundle: agent.bundle_digest.as_ref(),
        })?;
        let Some(running) = running_agents
            .iter()
            .find(|running| running.id == agent.agent_id)
        else {
            spawn_realign_agent(
                ctx,
                &session_id,
                agent,
                &expected_hash,
                false,
                false,
                &config_env,
                None,
            )
            .await?;
            results.push(json!({
                "agent_id": agent.agent_id,
                "status": "NEW",
                "action": "spawned",
                "message": format!("NEW agent {} spawned", agent.agent_id),
            }));
            continue;
        };
        let reason = drift_reason(running, agent);
        if running.state == "CRASHED" {
            stagger_before_respawn(&mut destructive_respawns).await;
            delete_agent(ctx.db.clone(), running.id.clone()).await?;
            spawn_realign_agent(
                ctx,
                &session_id,
                agent,
                &expected_hash,
                true,
                true,
                &config_env,
                None,
            )
            .await?;
            insert_event(
                ctx.db.clone(),
                agent.agent_id.clone(),
                None,
                "drift_realigned".to_string(),
                json!({ "reason": reason }).to_string(),
            )
            .await?;
            results.push(json!({
                "agent_id": agent.agent_id,
                "status": "REALIGNED",
                "event": "drift_realigned",
                "reason": reason,
                "message": format!("DRIFT {reason} REALIGNED drift_realigned"),
            }));
            continue;
        }
        if running.config_hash.as_deref() == Some(expected_hash.as_str()) {
            results.push(json!({
                "agent_id": agent.agent_id,
                "status": "NO_CHANGE",
                "message": "agent up to date",
            }));
            continue;
        }
        if running.state == "BUSY" && !force {
            insert_event(
                ctx.db.clone(),
                running.id.clone(),
                None,
                "drift_skipped".to_string(),
                json!({ "reason": reason, "state": running.state }).to_string(),
            )
            .await?;
            results.push(json!({
                "agent_id": agent.agent_id,
                "status": "SKIPPED_BUSY",
                "reason": reason,
                "message": format!("SKIPPED_BUSY: {reason}"),
            }));
            continue;
        }
        let destructive_reason = if running.state == "BUSY" {
            "DRIFT_FORCE_REALIGN"
        } else {
            "DRIFT_REALIGN"
        };
        stagger_before_respawn(&mut destructive_respawns).await;
        let _ = mark_agent_killed(
            ctx.db.clone(),
            running.id.clone(),
            destructive_reason.to_string(),
        )
        .await?;
        delete_agent(ctx.db.clone(), running.id.clone()).await?;
        spawn_realign_agent(
            ctx,
            &session_id,
            agent,
            &expected_hash,
            true,
            false,
            &config_env,
            None,
        )
        .await?;
        insert_event(
            ctx.db.clone(),
            agent.agent_id.clone(),
            None,
            "drift_realigned".to_string(),
            json!({ "reason": reason }).to_string(),
        )
        .await?;
        results.push(json!({
            "agent_id": agent.agent_id,
            "status": "REALIGNED",
            "event": "drift_realigned",
            "reason": reason,
            "message": format!("DRIFT {reason} REALIGNED drift_realigned"),
        }));
    }

    for running in &running_agents {
        if requested_ids.contains(&running.id) {
            continue;
        }
        if force {
            let _ = mark_agent_killed(
                ctx.db.clone(),
                running.id.clone(),
                "ORPHAN_FORCE_CLEANUP".to_string(),
            )
            .await?;
            insert_event(
                ctx.db.clone(),
                running.id.clone(),
                None,
                "agent_killed".to_string(),
                json!({ "reason": "ORPHAN_FORCE_CLEANUP" }).to_string(),
            )
            .await?;
            results.push(json!({
                "agent_id": running.id,
                "status": "ORPHAN",
                "action": "KILLED",
                "message": format!("ORPHAN {} force cleanup", running.id),
            }));
        } else {
            results.push(json!({
                "agent_id": running.id,
                "status": "ORPHAN",
                "message": format!("ORPHAN {} audit-only", running.id),
            }));
        }
    }

    Ok(json!({ "statuses": results }))
}

pub async fn handle_agent_realign(params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    let session_id = required_str(&params, "session_id")?.to_string();
    let force = optional_bool(&params, "force", false)?;
    // ISSUE-13 §3a: forward the project `[env]` so agent.realign fingerprints the same
    // bare env as session.realign (agent.env alone would drop config_env — Leak B).
    let config_env = params
        .get("config_env")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let agent: RealignAgentParams = serde_json::from_value(params)
        .map_err(|err| AhError::IpcInvalidRequest(format!("invalid agent realign: {err}")))?;
    handle_session_realign(
        json!({
            "session_id": session_id,
            "force": force,
            "_skip_master": true,
            "config_env": config_env,
            "master": {
                "cmd": "",
                "hooks": {},
            "plugins": [],
            "skills": [],
            "bundle": [],
            "settings": {},
            "bundle_digest": null,
        },
            "agents": [agent],
        }),
        ctx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn spawn_realign_agent(
    ctx: &Ctx,
    session_id: &str,
    agent: &RealignAgentParams,
    expected_hash: &str,
    killed_before_spawn: bool,
    is_recovery: bool,
    config_env: &HashMap<String, String>,
    captured_intent: Option<crate::db::recovery::AgentRecoveryIntent>,
) -> Result<(), AhError> {
    let uses_atomic_replacement = captured_intent.is_some();
    let db_action = if uses_atomic_replacement {
        AgentSpawnDbAction::ReplaceKilledAndRequeue {
            expected_hash: expected_hash.to_string(),
            captured_intent,
        }
    } else {
        AgentSpawnDbAction::InsertDefault
    };
    handle_agent_spawn_with_db_action(
        json!({
            "session_id": session_id,
            "agent_id": agent.agent_id.clone(),
            "provider": agent.provider.clone(),
            // ISSUE-13 §3a: forward raw agent.env + project config_env; the server
            // (agent.rs) merges them so the respawn's stored hash is built from the same
            // bare env realign compared against — no client-side pre-merge to diverge.
            "extra_env_vars": agent.env.clone(),
            "config_env": config_env.clone(),
            "hooks": agent.hooks.clone(),
            "plugins": agent.plugins.clone(),
            "skills": agent.skills.clone(),
            "bundle": agent.bundle.clone(),
            "settings": agent.settings.clone(),
            "bundle_digest": agent.bundle_digest.clone(),
            "sandbox_overrides": agent.sandbox_overrides.clone(),
            "hook_push_enabled": agent.hook_push_enabled,
            "claude_shared_credentials_dir": agent.claude_shared_credentials_dir.clone(),
        }),
        ctx,
        is_recovery,
        db_action,
    )
    .await?;
    if !uses_atomic_replacement {
        // ISSUE-13 §3c: the spawn success path (agent.rs) is the SOLE writer of
        // agents.config_hash. It has just persisted H(bare_env) for this respawn; realign
        // no longer clobbers it with a separately-computed hash (which is what masked the
        // spawn/realign asymmetry and left a latent divergent-drift-loop footgun). Because
        // §3a builds both sides' bare env from the same helper, agent.rs's hash already
        // equals the next realign's expected hash. We still refresh the recovery snapshot
        // (bare env) and clear the crash backoff.
        persist_realign_snapshot_after_success(ctx, agent, expected_hash, config_env).await?;
    }
    if killed_before_spawn {
        insert_event(
            ctx.db.clone(),
            agent.agent_id.clone(),
            None,
            "agent_killed".to_string(),
            json!({ "reason": "DRIFT_REALIGN" }).to_string(),
        )
        .await?;
    }
    insert_event(
        ctx.db.clone(),
        agent.agent_id.clone(),
        None,
        "agent_spawned".to_string(),
        json!({
            "reason": if killed_before_spawn { "DRIFT_REALIGN" } else { "NEW" },
            "is_recovery": is_recovery,
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

async fn persist_realign_snapshot_after_success(
    ctx: &Ctx,
    agent: &RealignAgentParams,
    expected_hash: &str,
    config_env: &HashMap<String, String>,
) -> Result<(), AhError> {
    let spec = crate::db::recovery::AgentSpawnSpec {
        agent_id: agent.agent_id.clone(),
        provider: agent.provider.clone(),
        // ISSUE-13 §3b/§3c: store the BARE merged env (config_env ⊕ agent.env), matching
        // what agent.rs persisted and what expected_hash was computed over, so a later
        // recovery replay recomputes the same hash instead of drifting.
        env: bare_config_env(config_env, &agent.env),
        hooks: agent.hooks.clone(),
        plugins: agent.plugins.clone(),
        skills: agent.skills.clone(),
        bundle: agent.bundle.clone(),
        settings: agent.settings.clone(),
        bundle_digest: agent.bundle_digest.clone(),
        sandbox_overrides: agent.sandbox_overrides.clone(),
        hook_push_enabled: agent.hook_push_enabled,
    };
    let conn = ctx.db.conn();
    crate::db::recovery::persist_agent_spawn_spec_sync(&conn, &spec, expected_hash)?;
    crate::db::recovery::clear_recovery_backoff_sync(&conn, &agent.agent_id)?;
    Ok(())
}

#[cfg(test)]
mod ra2_tests {
    use super::{RealignAgentParams, persist_realign_snapshot_after_success};
    use crate::db::agents::insert_agent_sync;
    use crate::db::recovery::query_agent_spawn_spec_sync;
    use crate::db::sessions::insert_session_sync;
    use crate::rpc::Ctx;
    use crate::sandbox::EnvState;
    use crate::tmux::TmuxServer;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_ctx() -> Ctx {
        let file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap().keep();
        Ctx {
            db: crate::db::init(file.path()).unwrap(),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn ra2_realign_success_refreshes_snapshot_and_clears_retry_exhausted() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/ra2").unwrap();
            insert_agent_sync(&conn, "ra2_realign", "s1", "codex", "CRASHED", None).unwrap();
            conn.execute(
                "UPDATE agents SET retry_count = 5, retry_exhausted = 1 WHERE id = 'ra2_realign'",
                [],
            )
            .unwrap();
        }
        let agent = RealignAgentParams {
            agent_id: "ra2_realign".to_string(),
            provider: "codex".to_string(),
            env: HashMap::from([("AFTER_REALIGN".to_string(), "1".to_string())]),
            hooks: HashMap::new(),
            plugins: vec!["github@openai-curated".to_string()],
            skills: Vec::new(),
            bundle: Vec::new(),
            settings: serde_json::Map::new(),
            bundle_digest: None,
            sandbox_overrides: crate::sandbox::SandboxOverrides {
                extra_binds: vec![],
                extra_ro_binds: vec![crate::sandbox::ReadOnlyBind {
                    host_path: "/opt/realign".to_string(),
                    sandbox_path: "/mnt/realign".to_string(),
                }],
            },
            hook_push_enabled: true,
            claude_shared_credentials_dir: None,
        };

        persist_realign_snapshot_after_success(&ctx, &agent, "hash2", &HashMap::new())
            .await
            .unwrap();
        let stored = query_agent_spawn_spec_sync(&ctx.db.conn(), "ra2_realign")
            .unwrap()
            .unwrap();
        assert_eq!(stored.config_hash, "hash2");
        assert_eq!(stored.spec.env["AFTER_REALIGN"], "1");
        assert!(stored.spec.hook_push_enabled);
        assert_eq!(
            stored.spec.sandbox_overrides.extra_ro_binds[0].host_path,
            "/opt/realign"
        );
        let row: (i64, Option<i64>, i64) = ctx
            .db
            .conn()
            .query_row(
                "SELECT retry_count, next_retry_at, retry_exhausted FROM agents WHERE id = 'ra2_realign'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, (0, None, 0));
    }
}

fn running_agent_hashes(
    ctx: &Ctx,
    session_id: &str,
) -> Result<Vec<RunningAgentConfigHash>, AhError> {
    let conn = ctx.db.conn();
    let mut stmt = conn
        .prepare(
            "SELECT id, provider, state, config_hash FROM agents \
             WHERE session_id = ? AND state != 'KILLED' ORDER BY id ASC",
        )
        .map_err(|err| AhError::DbConstraintViolation(format!("prepare realign agents: {err}")))?;
    let rows = stmt
        .query_map([session_id], |row| {
            Ok(RunningAgentConfigHash {
                id: row.get(0)?,
                provider: row.get(1)?,
                state: row.get(2)?,
                config_hash: row.get(3)?,
            })
        })
        .map_err(|err| AhError::DbConstraintViolation(format!("query realign agents: {err}")))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| AhError::DbConstraintViolation(format!("collect realign agents: {err}")))
}

/// ISSUE-13 §4b (commit C): space out consecutive destructive respawns within one realign.
/// The first respawn runs immediately; every subsequent one first waits
/// `MIN_RESPAWN_STAGGER_MS` so a mass drift does not burst simultaneous tmux pane creation.
/// `count` is the number of destructive respawns already performed this realign; it is
/// incremented here so the caller only has to invoke this before each destructive spawn.
async fn stagger_before_respawn(count: &mut usize) {
    if *count > 0 {
        tokio::time::sleep(Duration::from_millis(MIN_RESPAWN_STAGGER_MS)).await;
    }
    *count += 1;
}

fn drift_reason(running: &RunningAgentConfigHash, expected: &RealignAgentParams) -> &'static str {
    if running.provider != expected.provider {
        "provider changed"
    } else if !expected.plugins.is_empty() {
        "plugins changed"
    } else if !expected.skills.is_empty() {
        "skills changed"
    } else if expected.bundle_digest.is_some() {
        "bundle changed"
    } else if !expected.hooks.is_empty() {
        "hooks changed"
    } else if !expected.env.is_empty() {
        "env changed"
    } else {
        "config changed"
    }
}
