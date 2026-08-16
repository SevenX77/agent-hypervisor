use crate::db::common::{map_db_error, spawn_db};
use crate::error::AhError;
use crate::rpc::Ctx;
use crate::tmux::{TmuxPaneId, agent_session_name, master_session_name};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSnapshotReason {
    Initial,
    InventoryChanged,
    TmuxChanged,
    AgentChanged,
    JobChanged,
    Shutdown,
    DaemonAbsent,
    DaemonLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeState {
    Active,
    Inactive,
    Starting,
    Degraded,
}

const STARTING_WINDOW_SECS: i64 = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSnapshotRequest {
    pub reason: RuntimeSnapshotReason,
    pub config_path: Option<String>,
    pub workspace_path: Option<String>,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInactiveInput {
    pub reason: RuntimeSnapshotReason,
    pub config_path: Option<String>,
    pub workspace_path: Option<String>,
    pub state_dir: Option<String>,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub schema_version: u16,
    pub event: String,
    pub sequence: u64,
    pub reason: RuntimeSnapshotReason,
    pub runtime_state: RuntimeState,
    pub config_path: Option<String>,
    pub workspace_path: Option<String>,
    pub state_dir: Option<String>,
    pub tmux_socket: Option<String>,
    pub ahd_alive: bool,
    pub active: bool,
    pub ahd_has_inventory: bool,
    pub tmux_server_alive: bool,
    pub master_tmux_alive: bool,
    pub worker_tmux_alive: bool,
    pub worker_tmux_expected_count: usize,
    pub sessions: Vec<RuntimeSessionSnapshot>,
    pub agents: Vec<RuntimeAgentSnapshot>,
    pub jobs: Vec<RuntimeJobSnapshot>,
    pub job_events: Vec<RuntimeJobEventSnapshot>,
    pub job_event_cursor: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionSnapshot {
    pub session_id: String,
    pub project_id: String,
    pub path: String,
    pub status: String,
    pub master_state: String,
    pub master_tmux_session: String,
    pub master_tmux_alive: bool,
    pub master_pane_id: Option<String>,
    pub master_pid: Option<i64>,
    pub master_last_exit_reason: Option<String>,
    pub db_tracked_agents: i64,
    pub live_agents: i64,
    pub cleanup_required: bool,
    pub safe_to_cleanup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAgentSnapshot {
    pub agent_id: String,
    pub session_id: String,
    pub provider: String,
    pub state: String,
    pub sub_state: Option<String>,
    pub pid: Option<i64>,
    pub tmux_session: String,
    pub tmux_alive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeJobSnapshot {
    pub job_id: String,
    pub agent_id: String,
    pub request_id: Option<String>,
    pub status: String,
    pub cancel_requested: bool,
    pub created_at: i64,
    pub dispatched_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error_reason: Option<String>,
    pub requires_physical_evidence: bool,
    pub requires_test_evidence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeJobEventSnapshot {
    pub event_id: i64,
    pub kind: String,
    pub job_id: String,
    pub agent_id: String,
    pub request_id: Option<String>,
    pub old_status: Option<String>,
    pub new_status: Option<String>,
    pub changed: Vec<String>,
    pub cancel_requested: bool,
    pub created_at: Option<i64>,
    pub dispatched_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error_reason: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct InventorySession {
    id: String,
    project_id: String,
    absolute_path: String,
    status: String,
    master_state: String,
    master_pane_id: Option<String>,
    master_pid: i64,
    master_last_exit_reason: Option<String>,
    db_tracked_agents: i64,
    created_at: i64,
}

#[derive(Debug, Clone)]
struct InventoryRecoveryWindow {
    session_id: String,
    phase: String,
    defer_until: i64,
}

#[derive(Debug, Clone)]
struct RuntimeInventory {
    sessions: Vec<InventorySession>,
    agents: Vec<InventoryAgent>,
    recovery_windows: Vec<InventoryRecoveryWindow>,
    jobs: Vec<RuntimeJobSnapshot>,
    job_events: Vec<RuntimeJobEventSnapshot>,
    job_event_cursor: i64,
}

#[derive(Debug, Clone)]
struct InventoryAgent {
    id: String,
    session_id: String,
    provider: String,
    state: String,
    sub_state: Option<String>,
    pid: Option<i64>,
    created_at: i64,
}

pub fn inactive_runtime_snapshot(input: RuntimeInactiveInput) -> RuntimeSnapshot {
    RuntimeSnapshot {
        schema_version: 2,
        event: "snapshot".to_string(),
        sequence: input.sequence,
        reason: input.reason,
        runtime_state: RuntimeState::Inactive,
        config_path: input.config_path,
        workspace_path: input.workspace_path,
        state_dir: input.state_dir,
        tmux_socket: None,
        ahd_alive: false,
        active: false,
        ahd_has_inventory: false,
        tmux_server_alive: false,
        master_tmux_alive: false,
        worker_tmux_alive: false,
        worker_tmux_expected_count: 0,
        sessions: Vec::new(),
        agents: Vec::new(),
        jobs: Vec::new(),
        job_events: Vec::new(),
        job_event_cursor: 0,
    }
}

pub async fn build_runtime_snapshot(
    ctx: &Ctx,
    request: RuntimeSnapshotRequest,
) -> Result<RuntimeSnapshot, AhError> {
    let workspace_path = request.workspace_path.clone();
    let inventory = query_runtime_inventory(ctx.db.clone(), workspace_path.clone()).await?;
    let sessions = inventory.sessions;
    let agents = inventory.agents;
    let recovery_windows = inventory
        .recovery_windows
        .into_iter()
        .map(|window| (window.session_id.clone(), window))
        .collect::<HashMap<_, _>>();
    let ahd_has_inventory = sessions.iter().any(|session| session.status == "ACTIVE");
    // Probe-truth (#53): the server's liveness is a fact about the runtime's own socket,
    // not about the inventory. After an abnormal master death the DB session goes
    // terminal while `remain-on-exit` deliberately keeps the server alive holding the
    // forensic dead pane — gating this probe on ACTIVE inventory made that state
    // unrepresentable to hosts consuming the snapshot.
    let tmux_server_alive = ctx.tmux_server.server_running().await.unwrap_or(false);

    let mut master_alive_by_session = HashMap::new();
    let mut all_active_masters_alive = true;
    for session in &sessions {
        let master_tmux_session = master_session_name(&session.project_id);
        let master_tmux_alive = if session.status == "ACTIVE" {
            master_runtime_alive(ctx, session, &master_tmux_session).await
        } else {
            false
        };
        if session.status == "ACTIVE" {
            all_active_masters_alive &= master_tmux_alive;
        }
        master_alive_by_session.insert(session.id.clone(), master_tmux_alive);
    }

    let active_agents = agents
        .iter()
        .filter(|agent| !is_terminal_agent_state(&agent.state))
        .collect::<Vec<_>>();
    let worker_tmux_expected_count = active_agents.len();
    let mut all_active_workers_alive = true;
    let mut agent_snapshots = Vec::with_capacity(agents.len());
    for agent in &agents {
        let tmux_session = agent_session_name(&agent.id);
        let tmux_alive = if is_terminal_agent_state(&agent.state) {
            false
        } else {
            ctx.tmux_server
                .session_exists(tmux_session.clone())
                .await
                .unwrap_or(false)
        };
        if !is_terminal_agent_state(&agent.state) {
            all_active_workers_alive &= tmux_alive;
        }
        agent_snapshots.push(RuntimeAgentSnapshot {
            agent_id: agent.id.clone(),
            session_id: agent.session_id.clone(),
            provider: agent.provider.clone(),
            state: agent.state.clone(),
            sub_state: agent.sub_state.clone(),
            pid: agent.pid,
            tmux_session,
            tmux_alive,
        });
    }
    let mut session_snapshots = Vec::with_capacity(sessions.len());
    let now = now_epoch_seconds();
    for session in &sessions {
        let master_tmux_session = master_session_name(&session.project_id);
        let master_tmux_alive = *master_alive_by_session.get(&session.id).unwrap_or(&false);
        let session_agents = agent_snapshots
            .iter()
            .filter(|agent| agent.session_id == session.id)
            .collect::<Vec<_>>();
        let live_agents = session_agents
            .iter()
            .filter(|agent| agent.tmux_alive)
            .count() as i64;
        let cleanup_required =
            cleanup_required_for_session(session, master_tmux_alive, &session_agents);
        let safe_to_cleanup =
            safe_to_cleanup_for_session(&session.status, recovery_windows.get(&session.id), now);
        session_snapshots.push(RuntimeSessionSnapshot {
            session_id: session.id.clone(),
            project_id: session.project_id.clone(),
            path: session.absolute_path.clone(),
            status: session.status.clone(),
            master_state: session.master_state.clone(),
            master_tmux_session,
            master_tmux_alive,
            master_pane_id: session.master_pane_id.clone(),
            master_pid: positive_pid(session.master_pid),
            master_last_exit_reason: session.master_last_exit_reason.clone(),
            db_tracked_agents: session.db_tracked_agents,
            live_agents,
            cleanup_required,
            safe_to_cleanup,
        });
    }

    let master_tmux_alive = ahd_has_inventory && all_active_masters_alive;
    let worker_tmux_alive = all_active_workers_alive;
    let active = ahd_has_inventory && master_tmux_alive && worker_tmux_alive;
    let degraded = ahd_has_inventory
        && (sessions
            .iter()
            .zip(session_snapshots.iter())
            .any(|(session, snapshot)| session_is_degraded(session, snapshot, now))
            || agents
                .iter()
                .zip(agent_snapshots.iter())
                .any(|(agent, snapshot)| agent_is_degraded(agent, snapshot, now)));
    let runtime_state = if active {
        RuntimeState::Active
    } else if degraded {
        RuntimeState::Degraded
    } else if ahd_has_inventory {
        RuntimeState::Starting
    } else {
        RuntimeState::Inactive
    };

    Ok(RuntimeSnapshot {
        schema_version: 2,
        event: "snapshot".to_string(),
        sequence: request.sequence,
        reason: request.reason,
        runtime_state,
        config_path: request.config_path,
        workspace_path,
        state_dir: Some(ctx.state_dir.display().to_string()),
        tmux_socket: Some(ctx.tmux_server.socket_name().to_string()),
        ahd_alive: true,
        active,
        ahd_has_inventory,
        tmux_server_alive,
        master_tmux_alive,
        worker_tmux_alive,
        worker_tmux_expected_count,
        sessions: session_snapshots,
        agents: agent_snapshots,
        jobs: inventory.jobs,
        job_events: inventory.job_events,
        job_event_cursor: inventory.job_event_cursor,
    })
}

pub fn runtime_snapshot_fingerprint(snapshot: &RuntimeSnapshot) -> Result<String, AhError> {
    let mut value = serde_json::to_value(snapshot)
        .map_err(|err| AhError::IpcInvalidRequest(format!("serialize runtime snapshot: {err}")))?;
    if let Value::Object(map) = &mut value {
        map.remove("sequence");
        map.remove("reason");
    }
    serde_json::to_string(&value)
        .map_err(|err| AhError::IpcInvalidRequest(format!("fingerprint runtime snapshot: {err}")))
}

async fn master_runtime_alive(
    ctx: &Ctx,
    session: &InventorySession,
    master_tmux_session: &str,
) -> bool {
    if !ctx
        .tmux_server
        .session_exists(master_tmux_session.to_string())
        .await
        .unwrap_or(false)
    {
        return false;
    }
    let Some(pane_id) = session.master_pane_id.as_deref() else {
        return false;
    };
    let Ok(pane_id) = TmuxPaneId::parse(pane_id) else {
        return false;
    };
    match ctx.tmux_server.get_pane_pid(pane_id).await {
        Ok(pid) => session.master_pid <= 0 || i64::from(pid) == session.master_pid,
        Err(_) => false,
    }
}

async fn query_runtime_inventory(
    db: crate::db::Db,
    workspace_path: Option<String>,
) -> Result<RuntimeInventory, AhError> {
    spawn_db("runtime_events::query_inventory", move || {
        let conn = db.conn();
        query_runtime_inventory_sync(&conn, workspace_path.as_deref())
    })
    .await
}

fn query_runtime_inventory_sync(
    conn: &Connection,
    workspace_path: Option<&str>,
) -> Result<RuntimeInventory, AhError> {
    let sessions = {
        let mut stmt = conn
            .prepare(
                "SELECT sessions.id, sessions.project_id, projects.absolute_path, sessions.status,
                        sessions.master_state, sessions.master_pane_id, sessions.master_pid,
                        sessions.master_last_exit_reason,
                        COALESCE(SUM(CASE WHEN agents.state NOT IN ('CRASHED', 'KILLED') THEN 1 ELSE 0 END), 0) AS db_tracked_agents,
                        sessions.created_at
                 FROM sessions
                 JOIN projects ON projects.id = sessions.project_id
                 LEFT JOIN agents ON agents.session_id = sessions.id
                 WHERE (?1 IS NULL OR projects.absolute_path = ?1)
                 GROUP BY sessions.id, sessions.project_id, projects.absolute_path, sessions.status,
                          sessions.master_state, sessions.master_pane_id, sessions.master_pid, sessions.created_at
                          , sessions.master_last_exit_reason
                 ORDER BY sessions.created_at ASC, sessions.id ASC",
            )
            .map_err(|err| map_db_error("prepare runtime sessions query", err))?;
        let rows = stmt
            .query_map(params![workspace_path], |row| {
                Ok(InventorySession {
                    id: row.get(0)?,
                    project_id: row.get(1)?,
                    absolute_path: row.get(2)?,
                    status: row.get(3)?,
                    master_state: row.get(4)?,
                    master_pane_id: row.get(5)?,
                    master_pid: row.get(6)?,
                    master_last_exit_reason: row.get(7)?,
                    db_tracked_agents: row.get(8)?,
                    created_at: row.get(9)?,
                })
            })
            .map_err(|err| map_db_error("query runtime sessions", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect runtime sessions", err))?
    };
    let agents = {
        let mut stmt = conn
            .prepare(
                "SELECT agents.id, agents.session_id, agents.provider, agents.state,
                        agents.sub_state, agents.pid, agents.created_at
                 FROM agents
                 JOIN sessions ON sessions.id = agents.session_id
                 JOIN projects ON projects.id = sessions.project_id
                 WHERE (?1 IS NULL OR projects.absolute_path = ?1)
                 ORDER BY agents.created_at ASC, agents.id ASC",
            )
            .map_err(|err| map_db_error("prepare runtime agents query", err))?;
        let rows = stmt
            .query_map(params![workspace_path], |row| {
                Ok(InventoryAgent {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    provider: row.get(2)?,
                    state: row.get(3)?,
                    sub_state: row.get(4)?,
                    pid: row.get(5)?,
                    created_at: row.get(6)?,
                })
            })
            .map_err(|err| map_db_error("query runtime agents", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect runtime agents", err))?
    };
    let recovery_windows = {
        let mut stmt = conn
            .prepare(
                "SELECT master_recovery_windows.session_id,
                        master_recovery_windows.phase,
                        master_recovery_windows.defer_until
                 FROM master_recovery_windows
                 JOIN sessions ON sessions.id = master_recovery_windows.session_id
                 JOIN projects ON projects.id = sessions.project_id
                 WHERE (?1 IS NULL OR projects.absolute_path = ?1)
                 ORDER BY master_recovery_windows.created_at ASC,
                          master_recovery_windows.session_id ASC",
            )
            .map_err(|err| map_db_error("prepare runtime recovery windows query", err))?;
        let rows = stmt
            .query_map(params![workspace_path], |row| {
                Ok(InventoryRecoveryWindow {
                    session_id: row.get(0)?,
                    phase: row.get(1)?,
                    defer_until: row.get(2)?,
                })
            })
            .map_err(|err| map_db_error("query runtime recovery windows", err))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_db_error("collect runtime recovery windows", err))?
    };
    let recent_terminal_cutoff = now_epoch_seconds().saturating_sub(24 * 60 * 60);
    let jobs = query_runtime_jobs_sync(conn, workspace_path, recent_terminal_cutoff)?;
    let (job_events, job_event_cursor) = query_runtime_job_events_sync(conn, workspace_path)?;
    Ok(RuntimeInventory {
        sessions,
        agents,
        recovery_windows,
        jobs,
        job_events,
        job_event_cursor,
    })
}

fn query_runtime_jobs_sync(
    conn: &Connection,
    workspace_path: Option<&str>,
    recent_terminal_cutoff: i64,
) -> Result<Vec<RuntimeJobSnapshot>, AhError> {
    let mut stmt = conn
        .prepare(
            "SELECT jobs.id, jobs.agent_id, jobs.request_id, jobs.status,
                    jobs.cancel_requested, jobs.created_at, jobs.dispatched_at,
                    jobs.completed_at, jobs.error_reason,
                    jobs.requires_physical_evidence, jobs.requires_test_evidence
             FROM jobs
             JOIN agents ON agents.id = jobs.agent_id
             JOIN sessions ON sessions.id = agents.session_id
             JOIN projects ON projects.id = sessions.project_id
             WHERE (?1 IS NULL OR projects.absolute_path = ?1)
               AND (
                    jobs.status IN ('QUEUED', 'DISPATCHED')
                    OR (
                        jobs.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'KILLED')
                        AND COALESCE(jobs.completed_at, jobs.created_at) >= ?2
                    )
               )
             ORDER BY jobs.created_at ASC, jobs.id ASC
             LIMIT 500",
        )
        .map_err(|err| map_db_error("prepare runtime jobs query", err))?;
    let rows = stmt
        .query_map(params![workspace_path, recent_terminal_cutoff], |row| {
            Ok(RuntimeJobSnapshot {
                job_id: row.get(0)?,
                agent_id: row.get(1)?,
                request_id: row.get(2)?,
                status: row.get(3)?,
                cancel_requested: row.get::<_, i64>(4)? != 0,
                created_at: row.get(5)?,
                dispatched_at: row.get(6)?,
                completed_at: row.get(7)?,
                error_reason: row.get(8)?,
                requires_physical_evidence: row.get::<_, i64>(9)? != 0,
                requires_test_evidence: row.get::<_, i64>(10)? != 0,
            })
        })
        .map_err(|err| map_db_error("query runtime jobs", err))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect runtime jobs", err))
}

fn query_runtime_job_events_sync(
    conn: &Connection,
    workspace_path: Option<&str>,
) -> Result<(Vec<RuntimeJobEventSnapshot>, i64), AhError> {
    let job_event_cursor = conn
        .query_row(
            "SELECT COALESCE(MAX(job_transitions.job_event_id), 0)
             FROM job_transitions
             JOIN jobs ON jobs.id = job_transitions.job_id
             JOIN agents ON agents.id = job_transitions.agent_id
             JOIN sessions ON sessions.id = agents.session_id
             JOIN projects ON projects.id = sessions.project_id
             WHERE (?1 IS NULL OR projects.absolute_path = ?1)",
            params![workspace_path],
            |row| row.get(0),
        )
        .map_err(|err| map_db_error("query runtime job event cursor", err))?;
    let mut stmt = conn
        .prepare(
            "SELECT job_transitions.job_event_id, job_transitions.kind,
                    job_transitions.job_id, job_transitions.agent_id,
                    job_transitions.request_id, job_transitions.old_status,
                    job_transitions.new_status, job_transitions.changed_json,
                    job_transitions.cancel_requested, job_transitions.job_created_at,
                    job_transitions.dispatched_at, job_transitions.completed_at,
                    job_transitions.error_reason, job_transitions.reason
             FROM job_transitions
             JOIN jobs ON jobs.id = job_transitions.job_id
             JOIN agents ON agents.id = job_transitions.agent_id
             JOIN sessions ON sessions.id = agents.session_id
             JOIN projects ON projects.id = sessions.project_id
             WHERE (?1 IS NULL OR projects.absolute_path = ?1)
             ORDER BY job_transitions.job_event_id DESC
             LIMIT 500",
        )
        .map_err(|err| map_db_error("prepare runtime job events query", err))?;
    let rows = stmt
        .query_map(params![workspace_path], |row| {
            let changed_json: String = row.get(7)?;
            Ok(RuntimeJobEventSnapshot {
                event_id: row.get(0)?,
                kind: row.get(1)?,
                job_id: row.get(2)?,
                agent_id: row.get(3)?,
                request_id: row.get(4)?,
                old_status: row.get(5)?,
                new_status: row.get(6)?,
                changed: serde_json::from_str(&changed_json).unwrap_or_default(),
                cancel_requested: row.get::<_, i64>(8)? != 0,
                created_at: row.get(9)?,
                dispatched_at: row.get(10)?,
                completed_at: row.get(11)?,
                error_reason: row.get(12)?,
                reason: row.get(13)?,
            })
        })
        .map_err(|err| map_db_error("query runtime job events", err))?;
    let mut job_events = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| map_db_error("collect runtime job events", err))?;
    job_events.reverse();
    Ok((job_events, job_event_cursor))
}

fn is_terminal_agent_state(state: &str) -> bool {
    matches!(state, "CRASHED" | "KILLED")
}

fn is_terminal_session_status(status: &str) -> bool {
    matches!(status, "KILLED" | "FAILED" | "CLOSED")
}

fn is_terminal_recovery_phase(phase: &str) -> bool {
    matches!(phase, "COMPLETED" | "FAILED" | "FUSED")
}

fn cleanup_required_for_session(
    session: &InventorySession,
    master_tmux_alive: bool,
    agents: &[&RuntimeAgentSnapshot],
) -> bool {
    is_terminal_session_status(&session.status)
        && (master_tmux_alive
            || positive_pid(session.master_pid).is_some()
            || agents.iter().any(|agent| agent.tmux_alive)
            || agents.iter().any(|agent| agent.pid.is_some())
            || agents
                .iter()
                .any(|agent| !is_terminal_agent_state(&agent.state)))
}

fn safe_to_cleanup_for_session(
    status: &str,
    recovery_window: Option<&InventoryRecoveryWindow>,
    now: i64,
) -> bool {
    is_terminal_session_status(status)
        && match recovery_window {
            None => true,
            Some(window) if is_terminal_recovery_phase(&window.phase) => true,
            Some(window) if now > window.defer_until => true,
            Some(_) => false,
        }
}

fn session_is_degraded(
    session: &InventorySession,
    snapshot: &RuntimeSessionSnapshot,
    now: i64,
) -> bool {
    if session.status != "ACTIVE" || snapshot.master_tmux_alive {
        return false;
    }
    if session.master_pane_id.is_some() {
        return true;
    }
    !within_starting_window(session.created_at, now)
}

fn agent_is_degraded(agent: &InventoryAgent, snapshot: &RuntimeAgentSnapshot, now: i64) -> bool {
    if is_terminal_agent_state(&agent.state) || snapshot.tmux_alive {
        return false;
    }
    if agent.state != "SPAWNING" {
        return true;
    }
    !within_starting_window(agent.created_at, now)
}

fn within_starting_window(created_at: i64, now: i64) -> bool {
    now.saturating_sub(created_at) < STARTING_WINDOW_SECS
}

fn now_epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn positive_pid(pid: i64) -> Option<i64> {
    (pid > 0).then_some(pid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{claim_next_job_sync, insert_job_sync, mark_job_completed_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::rpc::Ctx;
    use crate::sandbox::EnvState;
    use crate::tmux::TmuxServer;
    use serde_json::json;
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

    #[tokio::test]
    async fn empty_inventory_snapshot_is_inactive() {
        let ctx = test_ctx();

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: Some("/tmp/project/ah.toml".into()),
                workspace_path: Some("/tmp/project".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert_eq!(snapshot.schema_version, 2);
        assert_eq!(snapshot.event, "snapshot");
        assert_eq!(snapshot.reason, RuntimeSnapshotReason::Initial);
        assert!(!snapshot.active);
        assert!(!snapshot.ahd_has_inventory);
        assert_eq!(snapshot.runtime_state, RuntimeState::Inactive);
        assert!(snapshot.sessions.is_empty());
        assert!(snapshot.agents.is_empty());
        assert!(snapshot.jobs.is_empty());
        assert!(snapshot.job_events.is_empty());
        assert_eq!(snapshot.job_event_cursor, 0);
    }

    #[tokio::test]
    async fn missing_tmux_for_active_inventory_is_degraded_not_active() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_1", "proj", "/tmp/project").unwrap();
            conn.execute(
                "UPDATE sessions SET master_pid = 123, master_pane_id = '%404' WHERE id = 'sess_1'",
                [],
            )
            .unwrap();
            insert_agent_sync(&conn, "a1", "sess_1", "codex", "IDLE", Some(456)).unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: Some("/tmp/project/ah.toml".into()),
                workspace_path: Some("/tmp/project".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert!(snapshot.ahd_has_inventory);
        assert!(!snapshot.active);
        assert_eq!(snapshot.runtime_state, RuntimeState::Degraded);
        assert!(!snapshot.master_tmux_alive);
        assert!(!snapshot.worker_tmux_alive);
        assert_eq!(snapshot.worker_tmux_expected_count, 1);
        assert_eq!(snapshot.sessions[0].master_tmux_session, "master_proj");
        assert_eq!(snapshot.sessions[0].db_tracked_agents, 1);
        assert_eq!(snapshot.sessions[0].live_agents, 0);
        assert_eq!(snapshot.sessions[0].master_last_exit_reason, None);
        assert!(!snapshot.sessions[0].cleanup_required);
        assert!(!snapshot.sessions[0].safe_to_cleanup);
        assert_eq!(snapshot.agents[0].tmux_session, "agent_a1");
    }

    #[tokio::test]
    async fn starting_snapshot_is_not_degraded() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_starting", "proj", "/tmp/project").unwrap();
            insert_agent_sync(
                &conn,
                "a_starting",
                "sess_starting",
                "codex",
                "SPAWNING",
                Some(456),
            )
            .unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: Some("/tmp/project/ah.toml".into()),
                workspace_path: Some("/tmp/project".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert!(snapshot.ahd_has_inventory);
        assert!(!snapshot.active);
        assert_eq!(snapshot.runtime_state, RuntimeState::Starting);
        assert_ne!(snapshot.runtime_state, RuntimeState::Degraded);
        assert_eq!(
            serde_json::to_value(snapshot.runtime_state).unwrap(),
            serde_json::json!("starting")
        );
    }

    #[tokio::test]
    async fn in_session_dispatch_spawning_worker_is_starting_not_degraded() {
        let ctx = test_ctx();
        let (master_pane, master_pid) = {
            let session_name = master_session_name("proj");
            ctx.tmux_server
                .ensure_session(session_name.clone(), ctx.state_dir.clone())
                .await
                .unwrap();
            let pane = ctx
                .tmux_server
                .spawn_window(
                    session_name,
                    "master".to_string(),
                    ctx.state_dir.clone(),
                    vec!["sh".into(), "-lc".into(), "sleep 30".into()],
                )
                .await
                .unwrap();
            let pid = ctx.tmux_server.get_pane_pid(pane.clone()).await.unwrap();
            (pane, i64::from(pid))
        };
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_old", "proj", "/tmp/project").unwrap();
            conn.execute(
                "UPDATE sessions
                    SET created_at = unixepoch() - 10000,
                        master_pid = ?1,
                        master_pane_id = ?2
                  WHERE id = 'sess_old'",
                params![master_pid, master_pane.0],
            )
            .unwrap();
            insert_agent_sync(&conn, "a_new", "sess_old", "codex", "SPAWNING", Some(456)).unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: Some("/tmp/project/ah.toml".into()),
                workspace_path: Some("/tmp/project".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert!(snapshot.ahd_has_inventory);
        assert!(!snapshot.active);
        assert_eq!(snapshot.runtime_state, RuntimeState::Starting);
        assert_ne!(snapshot.runtime_state, RuntimeState::Degraded);
    }

    #[tokio::test]
    async fn cold_start_no_agents_is_starting() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_no_agents", "proj", "/tmp/project").unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: Some("/tmp/project/ah.toml".into()),
                workspace_path: Some("/tmp/project".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert!(snapshot.ahd_has_inventory);
        assert!(!snapshot.active);
        assert_eq!(snapshot.runtime_state, RuntimeState::Starting);
        assert_ne!(snapshot.runtime_state, RuntimeState::Degraded);
        assert_eq!(snapshot.worker_tmux_expected_count, 0);
    }

    #[tokio::test]
    async fn starting_past_timeout_is_degraded() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_stale_starting", "proj", "/tmp/project").unwrap();
            conn.execute(
                "UPDATE sessions SET created_at = unixepoch() - 10000 WHERE id = 'sess_stale_starting'",
                [],
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_stale_starting",
                "sess_stale_starting",
                "codex",
                "SPAWNING",
                Some(456),
            )
            .unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: Some("/tmp/project/ah.toml".into()),
                workspace_path: Some("/tmp/project".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert!(snapshot.ahd_has_inventory);
        assert!(!snapshot.active);
        assert_eq!(snapshot.runtime_state, RuntimeState::Degraded);
    }

    #[test]
    fn inactive_client_snapshot_is_jsonl_safe() {
        let snapshot = inactive_runtime_snapshot(RuntimeInactiveInput {
            reason: RuntimeSnapshotReason::DaemonAbsent,
            config_path: Some("/tmp/project/ah.toml".into()),
            workspace_path: Some("/tmp/project".into()),
            state_dir: Some("/tmp/ah-state".into()),
            sequence: 1,
        });

        let line = serde_json::to_string(&snapshot).unwrap();
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert!(!line.contains('\n'));
        assert_eq!(value["event"], "snapshot");
        assert_eq!(value["reason"], "daemon_absent");
        assert_eq!(value["active"], false);
        assert_eq!(value["runtime_state"], "inactive");
    }

    #[tokio::test]
    async fn runtime_snapshot_rpc_method_is_registered() {
        let ctx = test_ctx();
        let response = crate::rpc::router::dispatch(
            &json!({
                "jsonrpc": "2.0",
                "method": "runtime.snapshot",
                "params": {
                    "config_path": "/tmp/project/ah.toml",
                    "workspace_path": "/tmp/project"
                },
                "id": 99
            })
            .to_string(),
            &ctx,
        )
        .await;
        let obj: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(obj["id"], 99);
        assert_eq!(obj["result"]["schema_version"], 2);
        assert_eq!(obj["result"]["event"], "snapshot");
        assert_eq!(obj["result"]["reason"], "initial");
        assert_eq!(obj["result"]["jobs"], serde_json::json!([]));
        assert_eq!(obj["result"]["job_events"], serde_json::json!([]));
        assert_eq!(obj["result"]["job_event_cursor"], 0);
    }

    #[test]
    fn runtime_reason_serializes_job_changed() {
        assert_eq!(
            serde_json::to_value(RuntimeSnapshotReason::JobChanged).unwrap(),
            serde_json::json!("job_changed")
        );
    }

    #[tokio::test]
    async fn snapshot_v2_projects_jobs_and_job_events() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_jobs", "proj_jobs", "/tmp/project-jobs").unwrap();
            insert_agent_sync(&conn, "a_jobs", "sess_jobs", "codex", "IDLE", Some(456)).unwrap();
            conn.execute(
                "INSERT INTO jobs (
                    id, agent_id, request_id, prompt_text, status, cancel_requested,
                    requires_physical_evidence, requires_test_evidence
                 ) VALUES ('job_v2', 'a_jobs', 'req-v2', 'prompt', 'QUEUED', 1, 1, 0)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO job_transitions (
                    job_id, agent_id, request_id, kind, old_status, new_status,
                    changed_json, reason, job_created_at, dispatched_at, completed_at,
                    cancel_requested, error_reason
                 ) VALUES (
                    'job_v2', 'a_jobs', 'req-v2', 'job_updated', NULL, NULL,
                    '[\"cancel_requested\"]', 'test', 10, NULL, NULL, 1, NULL
                 )",
                [],
            )
            .unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: None,
                workspace_path: Some("/tmp/project-jobs".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert_eq!(snapshot.schema_version, 2);
        assert_eq!(snapshot.jobs.len(), 1);
        assert_eq!(snapshot.jobs[0].job_id, "job_v2");
        assert_eq!(snapshot.jobs[0].request_id.as_deref(), Some("req-v2"));
        assert_eq!(snapshot.jobs[0].status, "QUEUED");
        assert!(snapshot.jobs[0].cancel_requested);
        assert!(snapshot.jobs[0].requires_physical_evidence);
        assert_eq!(snapshot.job_events.len(), 1);
        assert_eq!(snapshot.job_events[0].event_id, 1);
        assert_eq!(snapshot.job_events[0].kind, "job_updated");
        assert_eq!(snapshot.job_events[0].changed, vec!["cancel_requested"]);
        assert_eq!(snapshot.job_event_cursor, 1);
    }

    #[tokio::test]
    async fn snapshot_job_events_populates_from_real_completion_transition() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "sess_real_job", "proj_real_job", "/tmp/real-job").unwrap();
            insert_agent_sync(
                &conn,
                "a_real_job",
                "sess_real_job",
                "codex",
                "IDLE",
                Some(456),
            )
            .unwrap();
            insert_job_sync(&conn, "job_real", "a_real_job", Some("req-real"), "prompt").unwrap();
        }
        claim_next_job_sync(&ctx.db, "a_real_job").unwrap().unwrap();
        mark_job_completed_sync(&ctx.db, "job_real", "done").unwrap();

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::JobChanged,
                config_path: None,
                workspace_path: Some("/tmp/real-job".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        let completed = snapshot
            .job_events
            .iter()
            .find(|event| {
                event.job_id == "job_real" && event.new_status.as_deref() == Some("COMPLETED")
            })
            .expect("completed transition should be present");
        assert_eq!(completed.kind, "job_transition");
        assert_eq!(completed.old_status.as_deref(), Some("DISPATCHED"));
        assert_eq!(completed.request_id.as_deref(), Some("req-real"));
        assert_eq!(completed.reason, "completed");
        assert_eq!(completed.event_id, snapshot.job_event_cursor);
    }

    #[tokio::test]
    async fn job_events_snapshot_returns_newest_bounded_window_in_ascending_order() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(
                &conn,
                "sess_many_events",
                "proj_many_events",
                "/tmp/many-events",
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_many_events",
                "sess_many_events",
                "codex",
                "IDLE",
                Some(456),
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs (id, agent_id, request_id, prompt_text, status)
                 VALUES ('job_many_events', 'a_many_events', 'req-many', 'prompt', 'DISPATCHED')",
                [],
            )
            .unwrap();
            for i in 1..=600 {
                conn.execute(
                    "INSERT INTO job_transitions (
                        job_id, agent_id, request_id, kind, old_status, new_status,
                        changed_json, reason, job_created_at, dispatched_at, completed_at,
                        cancel_requested, error_reason
                     ) VALUES (
                        'job_many_events', 'a_many_events', 'req-many', 'job_updated',
                        NULL, NULL, '[\"cancel_requested\"]', ?1, 10, NULL, NULL, 0, NULL
                     )",
                    [format!("event-{i}")],
                )
                .unwrap();
            }
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: None,
                workspace_path: Some("/tmp/many-events".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        let ids = snapshot
            .job_events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        assert_eq!(snapshot.job_event_cursor, 600);
        assert_eq!(snapshot.job_events.len(), 500);
        assert_eq!(ids.first().copied(), Some(101));
        assert_eq!(ids.last().copied(), Some(600));
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(!ids.contains(&100));
    }

    #[tokio::test]
    async fn terminal_session_cleanup_fields_follow_strict_recovery_window_rule() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_absent", "p_absent", "/tmp/absent").unwrap();
            insert_session_sync(
                &conn,
                "s_active_window",
                "p_active_window",
                "/tmp/active-window",
            )
            .unwrap();
            insert_session_sync(
                &conn,
                "s_terminal_window",
                "p_terminal_window",
                "/tmp/terminal-window",
            )
            .unwrap();
            insert_session_sync(
                &conn,
                "s_expired_window",
                "p_expired_window",
                "/tmp/expired-window",
            )
            .unwrap();
            conn.execute(
                "UPDATE sessions
                 SET status = 'FAILED', master_pid = 0, master_last_exit_reason = 'IDLE_MASTER_EXIT'
                 WHERE id IN ('s_absent', 's_active_window', 's_terminal_window', 's_expired_window')",
                [],
            )
            .unwrap();
            insert_agent_sync(&conn, "a_absent", "s_absent", "codex", "KILLED", None).unwrap();
            insert_agent_sync(
                &conn,
                "a_active_window",
                "s_active_window",
                "codex",
                "KILLED",
                None,
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_terminal_window",
                "s_terminal_window",
                "codex",
                "KILLED",
                None,
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_expired_window",
                "s_expired_window",
                "codex",
                "KILLED",
                None,
            )
            .unwrap();
            conn.execute(
                "INSERT INTO master_recovery_windows (
                    session_id, expected_pid, expected_generation, phase, active_work, defer_until
                 ) VALUES
                 ('s_active_window', 1, 1, 'MASTER_VERIFYING', 0, unixepoch() + 3600),
                 ('s_terminal_window', 1, 1, 'COMPLETED', 0, unixepoch() + 3600),
                 ('s_expired_window', 1, 1, 'MASTER_VERIFYING', 0, unixepoch() - 1)",
                [],
            )
            .unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: None,
                workspace_path: None,
                sequence: 1,
            },
        )
        .await
        .unwrap();
        let session = |id: &str| {
            snapshot
                .sessions
                .iter()
                .find(|session| session.session_id == id)
                .unwrap()
        };

        assert!(session("s_absent").safe_to_cleanup);
        assert!(!session("s_absent").cleanup_required);
        assert_eq!(
            session("s_absent").master_last_exit_reason.as_deref(),
            Some("IDLE_MASTER_EXIT")
        );
        assert!(!session("s_active_window").safe_to_cleanup);
        assert!(session("s_terminal_window").safe_to_cleanup);
        assert!(session("s_expired_window").safe_to_cleanup);
    }

    #[tokio::test]
    async fn cleanup_required_and_live_agents_reflect_lingering_agent_resources() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_cleanup", "p_cleanup", "/tmp/cleanup").unwrap();
            conn.execute(
                "UPDATE sessions SET status = 'FAILED', master_pid = 0 WHERE id = 's_cleanup'",
                [],
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_cleanup_live",
                "s_cleanup",
                "codex",
                "KILLED",
                Some(123),
            )
            .unwrap();
            insert_agent_sync(&conn, "a_cleanup_db", "s_cleanup", "codex", "IDLE", None).unwrap();
        }

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: None,
                workspace_path: Some("/tmp/cleanup".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        let session = &snapshot.sessions[0];
        assert_eq!(session.db_tracked_agents, 1);
        assert_eq!(session.live_agents, 0);
        assert!(session.cleanup_required);
        assert!(session.safe_to_cleanup);
    }

    #[tokio::test]
    async fn live_agents_counts_alive_tmux_agent_sessions() {
        let ctx = test_ctx();
        {
            let conn = ctx.db.conn();
            insert_session_sync(&conn, "s_live_agents", "p_live_agents", "/tmp/live-agents")
                .unwrap();
            insert_agent_sync(
                &conn,
                "a_live_agent",
                "s_live_agents",
                "codex",
                "IDLE",
                Some(123),
            )
            .unwrap();
        }
        ctx.tmux_server
            .ensure_session(agent_session_name("a_live_agent"), ctx.state_dir.clone())
            .await
            .unwrap();

        let snapshot = build_runtime_snapshot(
            &ctx,
            RuntimeSnapshotRequest {
                reason: RuntimeSnapshotReason::Initial,
                config_path: None,
                workspace_path: Some("/tmp/live-agents".into()),
                sequence: 1,
            },
        )
        .await
        .unwrap();

        assert_eq!(snapshot.sessions[0].db_tracked_agents, 1);
        assert_eq!(snapshot.sessions[0].live_agents, 1);
    }

    #[test]
    fn runtime_fingerprint_includes_job_and_cleanup_fields() {
        let mut snapshot = inactive_runtime_snapshot(RuntimeInactiveInput {
            reason: RuntimeSnapshotReason::DaemonAbsent,
            config_path: None,
            workspace_path: None,
            state_dir: None,
            sequence: 1,
        });
        let base = runtime_snapshot_fingerprint(&snapshot).unwrap();
        snapshot.sequence = 2;
        snapshot.reason = RuntimeSnapshotReason::DaemonLost;
        assert_eq!(runtime_snapshot_fingerprint(&snapshot).unwrap(), base);

        snapshot.job_event_cursor = 1;
        assert_ne!(runtime_snapshot_fingerprint(&snapshot).unwrap(), base);
        let with_cursor = runtime_snapshot_fingerprint(&snapshot).unwrap();
        snapshot.job_events.push(RuntimeJobEventSnapshot {
            event_id: 1,
            kind: "job_updated".into(),
            job_id: "job1".into(),
            agent_id: "a1".into(),
            request_id: None,
            old_status: None,
            new_status: None,
            changed: vec!["cancel_requested".into()],
            cancel_requested: true,
            created_at: Some(1),
            dispatched_at: None,
            completed_at: None,
            error_reason: None,
            reason: "test".into(),
        });
        assert_ne!(
            runtime_snapshot_fingerprint(&snapshot).unwrap(),
            with_cursor
        );
        let with_job_event = runtime_snapshot_fingerprint(&snapshot).unwrap();
        snapshot.sessions.push(RuntimeSessionSnapshot {
            session_id: "s1".into(),
            project_id: "p1".into(),
            path: "/tmp/p1".into(),
            status: "FAILED".into(),
            master_state: "IDLE".into(),
            master_tmux_session: "master_p1".into(),
            master_tmux_alive: false,
            master_pane_id: None,
            master_pid: None,
            master_last_exit_reason: Some("IDLE_MASTER_EXIT".into()),
            db_tracked_agents: 0,
            live_agents: 0,
            cleanup_required: false,
            safe_to_cleanup: true,
        });
        assert_ne!(
            runtime_snapshot_fingerprint(&snapshot).unwrap(),
            with_job_event
        );
    }
}
