use crate::db::Db;
use crate::error::AhError;
use crate::provider::extensions::HookGroup;
use crate::provider::fingerprint::BundleDigest;
use crate::sandbox::SandboxOverrides;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::collections::HashMap;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
type ReplaceKilledAgentAfterDeleteHook = Box<dyn Fn() + Send + 'static>;

#[cfg(test)]
static REPLACE_KILLED_AGENT_AFTER_DELETE_HOOK: OnceLock<
    Mutex<Option<ReplaceKilledAgentAfterDeleteHook>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) fn set_replace_killed_agent_after_delete_test_hook(
    hook: Option<ReplaceKilledAgentAfterDeleteHook>,
) {
    *REPLACE_KILLED_AGENT_AFTER_DELETE_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("replace killed agent test hook mutex poisoned") = hook;
}

#[cfg(test)]
fn run_replace_killed_agent_after_delete_test_hook() {
    if let Some(hook) = REPLACE_KILLED_AGENT_AFTER_DELETE_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("replace killed agent test hook mutex poisoned")
        .as_ref()
    {
        hook();
    }
}

#[cfg(not(test))]
fn run_replace_killed_agent_after_delete_test_hook() {}

pub const AGENT_SPAWN_SPECS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS agent_spawn_specs (
  agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
  spec_version INTEGER NOT NULL DEFAULT 1,
  provider TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;
"#;

pub const AGENT_RECOVERY_INTENTS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS agent_recovery_intents (
  agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  previous_state TEXT NOT NULL,
  crashed_state_version INTEGER NOT NULL,
  interrupted_job_id TEXT,
  interrupted_job_status TEXT,
  interrupted_job_request_id TEXT,
  interrupted_job_prompt_text TEXT,
  interrupted_job_cancel_requested INTEGER,
  interrupted_job_requires_physical_evidence INTEGER,
  interrupted_job_requires_test_evidence INTEGER,
  action TEXT NOT NULL CHECK(action IN ('REVIVE', 'REVIVE_IDLE', 'REAP_ONLY')),
  reason TEXT NOT NULL,
  consumed_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_agent_recovery_intents_action
ON agent_recovery_intents(action, consumed_at, created_at);
"#;

pub const AGENTS_BACKOFF_COLUMNS_DDL: &[&str] = &[
    "ALTER TABLE agents ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE agents ADD COLUMN next_retry_at INTEGER",
    "ALTER TABLE agents ADD COLUMN retry_exhausted INTEGER NOT NULL DEFAULT 0",
];

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentSpawnSpec {
    pub agent_id: String,
    pub provider: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub hooks: HashMap<String, Vec<HookGroup>>,
    #[serde(default)]
    pub plugins: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub bundle: Vec<String>,
    #[serde(default)]
    pub settings: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub bundle_digest: Option<BundleDigest>,
    #[serde(default)]
    pub sandbox_overrides: SandboxOverrides,
    #[serde(default)]
    pub hook_push_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAgentSpawnSpec {
    pub spec: AgentSpawnSpec,
    pub config_hash: String,
    pub spec_version: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryBackoff {
    pub retry_count: i64,
    pub next_retry_at: Option<i64>,
    pub retry_exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryIntentAction {
    Revive,
    ReviveIdle,
    ReapOnly,
}

impl RecoveryIntentAction {
    fn as_db_str(&self) -> &'static str {
        match self {
            Self::Revive => "REVIVE",
            Self::ReviveIdle => "REVIVE_IDLE",
            Self::ReapOnly => "REAP_ONLY",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, AhError> {
        match value {
            "REVIVE" => Ok(Self::Revive),
            "REVIVE_IDLE" => Ok(Self::ReviveIdle),
            "REAP_ONLY" => Ok(Self::ReapOnly),
            other => Err(AhError::DbConstraintViolation(format!(
                "unknown recovery intent action: {other}"
            ))),
        }
    }

    pub(crate) fn db_str(&self) -> &'static str {
        self.as_db_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedInterruptedJob {
    pub id: String,
    pub request_id: Option<String>,
    pub prompt_text: String,
    pub cancel_requested: bool,
    pub requires_physical_evidence: bool,
    pub requires_test_evidence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentRecoveryIntent {
    pub agent_id: String,
    pub session_id: String,
    pub provider: String,
    pub previous_state: String,
    pub crashed_state_version: i64,
    pub interrupted_job_id: Option<String>,
    pub interrupted_job_status: Option<String>,
    pub interrupted_job: Option<CapturedInterruptedJob>,
    pub action: RecoveryIntentAction,
    pub reason: String,
}

pub(crate) fn persist_agent_recovery_intent_sync(
    conn: &Connection,
    intent: &AgentRecoveryIntent,
) -> Result<(), AhError> {
    tracing::info!(
        agent_id = %intent.agent_id,
        action = intent.action.as_db_str(),
        previous_state = %intent.previous_state,
        interrupted_job_id = ?intent.interrupted_job_id,
        "persisting agent recovery intent"
    );
    conn.execute(
        "INSERT INTO agent_recovery_intents (
            agent_id, session_id, provider, previous_state, crashed_state_version,
            interrupted_job_id, interrupted_job_status, interrupted_job_request_id,
            interrupted_job_prompt_text, interrupted_job_cancel_requested,
            interrupted_job_requires_physical_evidence, interrupted_job_requires_test_evidence,
            action, reason, consumed_at, updated_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, unixepoch())
         ON CONFLICT(agent_id) DO UPDATE SET
            session_id = excluded.session_id,
            provider = excluded.provider,
            previous_state = excluded.previous_state,
            crashed_state_version = excluded.crashed_state_version,
            interrupted_job_id = excluded.interrupted_job_id,
            interrupted_job_status = excluded.interrupted_job_status,
            interrupted_job_request_id = excluded.interrupted_job_request_id,
            interrupted_job_prompt_text = excluded.interrupted_job_prompt_text,
            interrupted_job_cancel_requested = excluded.interrupted_job_cancel_requested,
            interrupted_job_requires_physical_evidence = excluded.interrupted_job_requires_physical_evidence,
            interrupted_job_requires_test_evidence = excluded.interrupted_job_requires_test_evidence,
            action = excluded.action,
            reason = excluded.reason,
            consumed_at = NULL,
            updated_at = unixepoch()",
        params![
            intent.agent_id,
            intent.session_id,
            intent.provider,
            intent.previous_state,
            intent.crashed_state_version,
            intent.interrupted_job_id,
            intent.interrupted_job_status,
            intent
                .interrupted_job
                .as_ref()
                .and_then(|job| job.request_id.as_deref()),
            intent.interrupted_job.as_ref().map(|job| job.prompt_text.as_str()),
            intent
                .interrupted_job
                .as_ref()
                .map(|job| i64::from(job.cancel_requested)),
            intent
                .interrupted_job
                .as_ref()
                .map(|job| i64::from(job.requires_physical_evidence)),
            intent
                .interrupted_job
                .as_ref()
                .map(|job| i64::from(job.requires_test_evidence)),
            intent.action.as_db_str(),
            intent.reason,
        ],
    )
    .map_err(|err| {
        AhError::DbConstraintViolation(format!("persist agent recovery intent: {err}"))
    })?;
    Ok(())
}

pub(crate) fn query_agent_recovery_intent_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<Option<AgentRecoveryIntent>, AhError> {
    conn.query_row(
        "SELECT agent_id, session_id, provider, previous_state, crashed_state_version,
                interrupted_job_id, interrupted_job_status, interrupted_job_request_id,
                interrupted_job_prompt_text, interrupted_job_cancel_requested,
                interrupted_job_requires_physical_evidence, interrupted_job_requires_test_evidence,
                action, reason
         FROM agent_recovery_intents
         WHERE agent_id = ? AND consumed_at IS NULL",
        params![agent_id],
        |row| {
            let action: String = row.get(12)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<i64>>(11)?,
                action,
                row.get::<_, String>(13)?,
            ))
        },
    )
    .optional()
    .map_err(|err| AhError::DbConstraintViolation(format!("query agent recovery intent: {err}")))?
    .map(
        |(
            agent_id,
            session_id,
            provider,
            previous_state,
            crashed_state_version,
            interrupted_job_id,
            interrupted_job_status,
            interrupted_job_request_id,
            interrupted_job_prompt_text,
            interrupted_job_cancel_requested,
            interrupted_job_requires_physical_evidence,
            interrupted_job_requires_test_evidence,
            action,
            reason,
        )| {
            let interrupted_job = match (interrupted_job_id.as_ref(), interrupted_job_prompt_text) {
                (Some(job_id), Some(prompt_text)) => Some(CapturedInterruptedJob {
                    id: job_id.clone(),
                    request_id: interrupted_job_request_id,
                    prompt_text,
                    cancel_requested: interrupted_job_cancel_requested.unwrap_or(0) != 0,
                    requires_physical_evidence: interrupted_job_requires_physical_evidence
                        .unwrap_or(0)
                        != 0,
                    requires_test_evidence: interrupted_job_requires_test_evidence.unwrap_or(0)
                        != 0,
                }),
                _ => None,
            };
            Ok(AgentRecoveryIntent {
                agent_id,
                session_id,
                provider,
                previous_state,
                crashed_state_version,
                interrupted_job_id,
                interrupted_job_status,
                interrupted_job,
                action: RecoveryIntentAction::from_db_str(&action)?,
                reason,
            })
        },
    )
    .transpose()
}

pub(crate) fn requeue_interrupted_job_from_captured_intent_sync(
    conn: &Connection,
    intent: &AgentRecoveryIntent,
) -> Result<usize, AhError> {
    if intent.action != RecoveryIntentAction::Revive {
        tracing::debug!(
            agent_id = %intent.agent_id,
            action = intent.action.as_db_str(),
            "requeue skipped for non-revive recovery intent"
        );
        return Ok(0);
    }
    let Some(job_id) = intent.interrupted_job_id.as_deref() else {
        tracing::debug!(
            agent_id = %intent.agent_id,
            "requeue skipped because captured recovery intent has no interrupted job"
        );
        return Ok(0);
    };

    tracing::info!(
        agent_id = %intent.agent_id,
        job_id,
        "requeueing interrupted job from captured recovery intent"
    );
    let job = crate::db::jobs::query_job_sync(conn, job_id)?;
    let mut changed = 0;
    if let Some(job) = job {
        if job.status == "FAILED" && job.agent_id == intent.agent_id {
            let err_reason = crate::db::jobs::recovery_requeued_error_reason(0);
            crate::db::job_state::requeue_job_state_conn_sync(
                conn,
                job_id,
                crate::db::job_state::JobStatus::Failed,
                Some(&err_reason),
                "recovery_requeue",
            )?;
            changed = 1;
        }
    }
    if changed == 1 {
        return Ok(1);
    }

    let Some(captured_job) = intent.interrupted_job.as_ref() else {
        tracing::warn!(
            agent_id = %intent.agent_id,
            job_id,
            "captured interrupted job payload was missing for requeue"
        );
        return Ok(0);
    };
    tracing::info!(
        agent_id = %intent.agent_id,
        job_id = %captured_job.id,
        "re-inserting interrupted job from captured recovery intent value"
    );
    let marker = crate::db::jobs::recovery_requeued_error_reason(0);
    let inserted_id = crate::db::jobs::insert_recovered_queued_job_sync(
        conn,
        &captured_job.id,
        &intent.agent_id,
        captured_job.request_id.as_deref(),
        &captured_job.prompt_text,
        captured_job.cancel_requested,
        captured_job.requires_physical_evidence,
        captured_job.requires_test_evidence,
        &marker,
    )?;
    if inserted_id != captured_job.id {
        return Err(AhError::DbConstraintViolation(format!(
            "captured interrupted job restored as unexpected id: expected {}, got {}",
            captured_job.id, inserted_id
        )));
    }
    Ok(1)
}

pub(crate) fn requeue_interrupted_job_from_captured_intent_standalone_sync(
    db: &Db,
    intent: &AgentRecoveryIntent,
) -> Result<usize, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| {
            AhError::DbConstraintViolation(format!(
                "begin requeue interrupted job from captured intent: {err}"
            ))
        })?;
    let requeued = requeue_interrupted_job_from_captured_intent_sync(&tx, intent)?;
    tx.commit().map_err(|err| {
        AhError::DbConstraintViolation(format!(
            "commit requeue interrupted job from captured intent: {err}"
        ))
    })?;
    Ok(requeued)
}

pub(crate) fn replace_killed_agent_and_requeue_job_sync(
    db: &Db,
    session_id: &str,
    spec: &AgentSpawnSpec,
    config_hash: &str,
    pid: i64,
    captured_intent: Option<&AgentRecoveryIntent>,
) -> Result<usize, AhError> {
    let mut conn = db.conn();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| {
            AhError::DbConstraintViolation(format!(
                "begin replace killed agent and requeue job: {err}"
            ))
        })?;

    let current_state: Option<String> = tx
        .query_row(
            "SELECT state FROM agents WHERE id = ?",
            params![spec.agent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| {
            AhError::DbConstraintViolation(format!(
                "query killed agent before atomic replacement: {err}"
            ))
        })?;
    if current_state.as_deref() != Some("KILLED") {
        return Err(AhError::DbConstraintViolation(format!(
            "agent {} is not KILLED before atomic replacement",
            spec.agent_id
        )));
    }

    tx.execute("DELETE FROM agents WHERE id = ?", params![spec.agent_id])
        .map_err(|err| AhError::DbConstraintViolation(format!("delete killed agent: {err}")))?;
    run_replace_killed_agent_after_delete_test_hook();

    crate::db::agents::insert_agent_sync(
        &tx,
        &spec.agent_id,
        session_id,
        &spec.provider,
        "SPAWNING",
        Some(pid),
    )?;
    crate::db::agents::update_agent_config_hash_sync(&tx, &spec.agent_id, config_hash)?;
    persist_agent_spawn_spec_sync(&tx, spec, config_hash)?;
    clear_recovery_backoff_sync(&tx, &spec.agent_id)?;

    let requeued = match captured_intent {
        Some(intent) => requeue_interrupted_job_from_captured_intent_sync(&tx, intent)?,
        None => 0,
    };

    tx.commit().map_err(|err| {
        AhError::DbConstraintViolation(format!(
            "commit replace killed agent and requeue job: {err}"
        ))
    })?;
    if requeued > 0 {
        crate::db::jobs::notify_runtime_job_changed();
    }
    Ok(requeued)
}

pub(crate) fn persist_agent_spawn_spec_sync(
    conn: &Connection,
    spec: &AgentSpawnSpec,
    config_hash: &str,
) -> Result<(), AhError> {
    let spec_json = serde_json::to_string(spec).map_err(|err| {
        AhError::DbConstraintViolation(format!("serialize agent spawn spec: {err}"))
    })?;
    conn.execute(
        "INSERT INTO agent_spawn_specs (agent_id, spec_version, provider, config_hash, spec_json, updated_at) \
         VALUES (?, 1, ?, ?, ?, unixepoch()) \
         ON CONFLICT(agent_id) DO UPDATE SET \
             spec_version = agent_spawn_specs.spec_version + 1, \
             provider = excluded.provider, \
             config_hash = excluded.config_hash, \
             spec_json = excluded.spec_json, \
             updated_at = unixepoch()",
        params![spec.agent_id, spec.provider, config_hash, spec_json],
    )
    .map_err(|err| AhError::DbConstraintViolation(format!("persist agent spawn spec: {err}")))?;
    Ok(())
}

pub(crate) fn query_agent_spawn_spec_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<Option<StoredAgentSpawnSpec>, AhError> {
    let row = conn
        .query_row(
            "SELECT spec_json, config_hash, spec_version, updated_at FROM agent_spawn_specs WHERE agent_id = ?",
            params![agent_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|err| AhError::DbConstraintViolation(format!("query agent spawn spec: {err}")))?;
    row.map(|(spec_json, config_hash, spec_version, updated_at)| {
        let spec = serde_json::from_str(&spec_json).map_err(|err| {
            AhError::DbConstraintViolation(format!("deserialize agent spawn spec: {err}"))
        })?;
        Ok(StoredAgentSpawnSpec {
            spec,
            config_hash,
            spec_version,
            updated_at,
        })
    })
    .transpose()
}

pub(crate) fn query_agent_spawn_specs_for_session_sync(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<StoredAgentSpawnSpec>, AhError> {
    let mut stmt = conn
        .prepare(
            "SELECT s.spec_json, s.config_hash, s.spec_version, s.updated_at \
             FROM agent_spawn_specs s \
             JOIN agents a ON a.id = s.agent_id \
             WHERE a.session_id = ? \
             ORDER BY a.created_at ASC, a.id ASC",
        )
        .map_err(|err| {
            AhError::DbConstraintViolation(format!(
                "prepare query session agent spawn specs: {err}"
            ))
        })?;
    let rows = stmt
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(|err| {
            AhError::DbConstraintViolation(format!("query session agent spawn specs: {err}"))
        })?;
    rows.map(|row| {
        let (spec_json, config_hash, spec_version, updated_at) = row.map_err(|err| {
            AhError::DbConstraintViolation(format!("collect session agent spawn specs: {err}"))
        })?;
        let spec = serde_json::from_str(&spec_json).map_err(|err| {
            AhError::DbConstraintViolation(format!("deserialize session agent spawn spec: {err}"))
        })?;
        Ok(StoredAgentSpawnSpec {
            spec,
            config_hash,
            spec_version,
            updated_at,
        })
    })
    .collect()
}

pub(crate) fn try_claim_agent_recovery_sync(
    conn: &Connection,
    agent_id: &str,
    expected_state_version: i64,
    now: i64,
) -> Result<bool, AhError> {
    let changed = conn
        .execute(
            "UPDATE agents \
             SET state_version = state_version + 1, updated_at = unixepoch() \
             WHERE id = ? \
               AND state = 'CRASHED' \
               AND state_version = ? \
               AND (retry_exhausted = 0 OR retry_exhausted IS NULL) \
               AND (next_retry_at IS NULL OR next_retry_at <= ?)",
            params![agent_id, expected_state_version, now],
        )
        .map_err(|err| AhError::DbConstraintViolation(format!("claim agent recovery: {err}")))?;
    Ok(changed == 1)
}

pub(crate) fn record_recovery_failure_backoff_sync(
    conn: &Connection,
    agent_id: &str,
    now: i64,
) -> Result<RecoveryBackoff, AhError> {
    let current = conn
        .query_row(
            "SELECT retry_count FROM agents WHERE id = ?",
            params![agent_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|err| AhError::DbConstraintViolation(format!("query recovery backoff: {err}")))?;
    let Some(current_retry_count) = current else {
        return Err(AhError::AgentNotFound(agent_id.to_string()));
    };
    let retry_count = (current_retry_count + 1).min(5);
    let retry_exhausted = retry_count >= 5;
    let next_retry_at = if retry_exhausted {
        None
    } else {
        let delay = match retry_count {
            1 => 1,
            2 => 2,
            _ => 4,
        };
        Some(now + delay)
    };
    conn.execute(
        "UPDATE agents \
         SET retry_count = ?, next_retry_at = ?, retry_exhausted = ?, updated_at = unixepoch() \
         WHERE id = ?",
        params![
            retry_count,
            next_retry_at,
            if retry_exhausted { 1 } else { 0 },
            agent_id
        ],
    )
    .map_err(|err| AhError::DbConstraintViolation(format!("record recovery backoff: {err}")))?;
    Ok(RecoveryBackoff {
        retry_count,
        next_retry_at,
        retry_exhausted,
    })
}

pub(crate) fn clear_recovery_backoff_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<(), AhError> {
    conn.execute(
        "UPDATE agents \
         SET retry_count = 0, next_retry_at = NULL, retry_exhausted = 0, updated_at = unixepoch() \
         WHERE id = ?",
        params![agent_id],
    )
    .map_err(|err| AhError::DbConstraintViolation(format!("clear recovery backoff: {err}")))?;
    Ok(())
}

/// P0-2 seam (signature stub — a1 fills the body).
///
/// K8s CrashLoopBackOff clear-on-stability: unconditionally clears the recovery
/// backoff (retry_count/next_retry_at/retry_exhausted) for a `agent_id` that has
/// run stable for >= N minutes. The N-minute stability gate lives in the caller
/// (health/recovery tick), analogous to `master_revival::confirm_master_stable`
/// (which resets `master_retry_count` when a revived master is confirmed stable).
///
/// Body is intentionally `unimplemented!` (zero production logic) so the a4 RED
/// contract test panics until a1 implements the real clear.
#[allow(dead_code)] // P0-2 seam: a1 wires this into the health/recovery stability tick
pub(crate) fn confirm_agent_stable_sync(conn: &Connection, agent_id: &str) -> Result<(), AhError> {
    clear_recovery_backoff_sync(conn, agent_id)
}

#[allow(dead_code)] // wired into orchestrator run_once in R-A.2
pub(crate) fn try_claim_agent_recovery(
    db: Db,
    agent_id: String,
    expected_state_version: i64,
    now: i64,
) -> Result<bool, AhError> {
    let conn = db.conn();
    try_claim_agent_recovery_sync(&conn, &agent_id, expected_state_version, now)
}

#[cfg(test)]
mod tests {
    use super::{
        AgentRecoveryIntent, AgentSpawnSpec, CapturedInterruptedJob, RecoveryIntentAction,
        clear_recovery_backoff_sync, confirm_agent_stable_sync, persist_agent_spawn_spec_sync,
        query_agent_spawn_spec_sync, record_recovery_failure_backoff_sync,
        replace_killed_agent_and_requeue_job_sync,
        requeue_interrupted_job_from_captured_intent_standalone_sync,
        set_replace_killed_agent_after_delete_test_hook, try_claim_agent_recovery_sync,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync};
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};
    use crate::provider::extensions::{HookGroup, HookItem};
    use crate::sandbox::{ReadOnlyBind, SandboxOverrides};
    use rusqlite::Connection;
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn with_db<T>(test: impl FnOnce(&Db) -> T) -> T {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        test(&db)
    }

    fn seed_crashed_agent(conn: &Connection) {
        insert_session_sync(conn, "s1", "p1", "/tmp/ra1").unwrap();
        insert_agent_sync(conn, "a1", "s1", "codex", "CRASHED", None).unwrap();
        conn.execute(
            "UPDATE agents SET config_hash = 'hash1', state_version = 7 WHERE id = 'a1'",
            [],
        )
        .unwrap();
    }

    fn sample_spec(agent_id: &str, provider: &str) -> AgentSpawnSpec {
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let hooks = HashMap::from([(
            "PreToolUse".to_string(),
            vec![HookGroup {
                matcher: "*".to_string(),
                hooks: vec![HookItem {
                    hook_type: "command".to_string(),
                    command: "echo checked".to_string(),
                    timeout: Some(3),
                }],
            }],
        )]);
        AgentSpawnSpec {
            agent_id: agent_id.to_string(),
            provider: provider.to_string(),
            env,
            hooks,
            plugins: vec!["github@openai-curated".to_string()],
            skills: Vec::new(),
            bundle: Vec::new(),
            settings: serde_json::Map::new(),
            bundle_digest: None,
            sandbox_overrides: SandboxOverrides::default(),
            hook_push_enabled: false,
        }
    }

    fn table_columns(conn: &Connection, table: &str) -> Vec<String> {
        conn.prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn table_exists(conn: &Connection, table: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
            [table],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn index_exists(conn: &Connection, index: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name=?)",
            [index],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn captured_revive_intent(agent_id: &str, job_id: &str) -> AgentRecoveryIntent {
        AgentRecoveryIntent {
            agent_id: agent_id.to_string(),
            session_id: "s1".to_string(),
            provider: "codex".to_string(),
            previous_state: "BUSY".to_string(),
            crashed_state_version: 8,
            interrupted_job_id: Some(job_id.to_string()),
            interrupted_job_status: Some("DISPATCHED".to_string()),
            interrupted_job: Some(CapturedInterruptedJob {
                id: job_id.to_string(),
                request_id: None,
                prompt_text: "continue\n".to_string(),
                cancel_requested: false,
                requires_physical_evidence: false,
                requires_test_evidence: false,
            }),
            action: RecoveryIntentAction::Revive,
            reason: "AGENT_UNEXPECTED_EXIT".to_string(),
        }
    }

    #[test]
    fn rr_worker_schema_new_db_has_agent_recovery_intents_and_index() {
        with_db(|db| {
            let conn = db.conn();
            assert!(
                table_exists(&conn, "agent_recovery_intents"),
                "agent_recovery_intents table should exist"
            );
            let columns = table_columns(&conn, "agent_recovery_intents");
            for column in [
                "agent_id",
                "session_id",
                "provider",
                "previous_state",
                "crashed_state_version",
                "interrupted_job_id",
                "interrupted_job_status",
                "interrupted_job_request_id",
                "interrupted_job_prompt_text",
                "interrupted_job_cancel_requested",
                "interrupted_job_requires_physical_evidence",
                "interrupted_job_requires_test_evidence",
                "action",
                "reason",
                "consumed_at",
            ] {
                assert!(
                    columns.iter().any(|name| name == column),
                    "missing {column}"
                );
            }
            assert!(
                index_exists(&conn, "idx_agent_recovery_intents_action"),
                "agent_recovery_intents action index should exist"
            );
            let create_sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='agent_recovery_intents'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(create_sql.contains("'REVIVE_IDLE'"));
        });
    }

    #[test]
    fn rr_worker_migration_old_db_adds_agent_recovery_intents() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            conn.execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                "#,
            )
            .unwrap();
        }

        let db = init(file.path()).unwrap();
        let conn = db.conn();
        assert!(table_exists(&conn, "agent_recovery_intents"));
        assert!(index_exists(&conn, "idx_agent_recovery_intents_action"));
    }

    #[test]
    fn rr_worker_migration_old_recovery_intent_check_accepts_revive_idle() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            conn.execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agent_recovery_intents (
                    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    previous_state TEXT NOT NULL,
                    crashed_state_version INTEGER NOT NULL,
                    interrupted_job_id TEXT,
                    interrupted_job_status TEXT,
                    interrupted_job_request_id TEXT,
                    interrupted_job_prompt_text TEXT,
                    interrupted_job_cancel_requested INTEGER,
                    interrupted_job_requires_physical_evidence INTEGER,
                    interrupted_job_requires_test_evidence INTEGER,
                    action TEXT NOT NULL CHECK(action IN ('REVIVE', 'REAP_ONLY')),
                    reason TEXT NOT NULL,
                    consumed_at INTEGER,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                "#,
            )
            .unwrap();
        }

        let db = init(file.path()).unwrap();
        let conn = db.conn();
        insert_session_sync(&conn, "s1", "p1", "/tmp/rr-revive-idle").unwrap();
        insert_agent_sync(&conn, "a_revive_idle", "s1", "codex", "CRASHED", None).unwrap();
        let intent = AgentRecoveryIntent {
            agent_id: "a_revive_idle".to_string(),
            session_id: "s1".to_string(),
            provider: "codex".to_string(),
            previous_state: "IDLE".to_string(),
            crashed_state_version: 2,
            interrupted_job_id: None,
            interrupted_job_status: None,
            interrupted_job: None,
            action: RecoveryIntentAction::ReviveIdle,
            reason: "AGENT_UNEXPECTED_EXIT".to_string(),
        };

        super::persist_agent_recovery_intent_sync(&conn, &intent)
            .expect("migrated recovery intent CHECK must allow REVIVE_IDLE");
        assert_eq!(
            super::query_agent_recovery_intent_sync(&conn, "a_revive_idle")
                .unwrap()
                .unwrap()
                .action,
            RecoveryIntentAction::ReviveIdle
        );
    }

    #[test]
    fn rr_worker_requeue_uses_captured_intent_after_agent_delete_cascades_intent() {
        with_db(|db| {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/rr-worker").unwrap();
            insert_agent_sync(&conn, "a_requeue", "s1", "codex", "CRASHED", None).unwrap();
            insert_job_sync(&conn, "job_requeue", "a_requeue", None, "continue\n").unwrap();
            conn.execute(
                "UPDATE jobs \
                 SET status = 'FAILED', dispatched_at = unixepoch(), dispatched_at_seq_id = 42, \
                     completed_at = unixepoch(), error_reason = 'AGENT_UNEXPECTED_EXIT' \
                 WHERE id = 'job_requeue'",
                [],
            )
            .unwrap();
            let captured = captured_revive_intent("a_requeue", "job_requeue");

            conn.execute("DELETE FROM agents WHERE id = 'a_requeue'", [])
                .unwrap();
            insert_agent_sync(&conn, "a_requeue", "s1", "codex", "IDLE", None).unwrap();
            drop(conn);
            let changed =
                requeue_interrupted_job_from_captured_intent_standalone_sync(db, &captured)
                    .expect("captured intent should drive requeue after DB intent is gone");

            assert_eq!(changed, 1);
            let conn = db.conn();
            let job = query_job_sync(&conn, "job_requeue").unwrap().unwrap();
            assert_eq!(job.status, "QUEUED");
            assert_eq!(job.prompt_text, "continue\n");
            assert_eq!(job.dispatched_at, None);
            assert_eq!(job.dispatched_at_seq_id, None);
            assert_eq!(job.completed_at, None);
            assert_eq!(job.error_reason.as_deref(), Some("RECOVERY_REQUEUED:0"));
            let transition_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*)
                     FROM job_transitions
                     WHERE job_id = 'job_requeue'
                       AND old_status IS NULL
                       AND new_status = 'QUEUED'
                       AND reason = 'recovery_recovered_create'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(transition_count, 1);
        });
    }

    #[test]
    fn rr_worker_atomic_replace_keeps_job_visible_during_uncommitted_cascade() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/rr-atomic").unwrap();
            insert_agent_sync(&conn, "a_atomic", "s1", "codex", "KILLED", Some(111)).unwrap();
            insert_job_sync(
                &conn,
                "job_atomic",
                "a_atomic",
                Some("req-atomic"),
                "continue atomic\n",
            )
            .unwrap();
            conn.execute(
                "UPDATE jobs \
                 SET status = 'DISPATCHED', dispatched_at = unixepoch(), dispatched_at_seq_id = 7 \
                 WHERE id = 'job_atomic'",
                [],
            )
            .unwrap();
        }

        let mut intent = captured_revive_intent("a_atomic", "job_atomic");
        intent.interrupted_job_status = Some("DISPATCHED".to_string());
        intent.interrupted_job = Some(CapturedInterruptedJob {
            id: "job_atomic".to_string(),
            request_id: Some("req-atomic".to_string()),
            prompt_text: "continue atomic\n".to_string(),
            cancel_requested: true,
            requires_physical_evidence: true,
            requires_test_evidence: false,
        });
        let spec = sample_spec("a_atomic", "codex");
        let observer = db.fresh_conn().unwrap();
        let (deleted_tx, deleted_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        set_replace_killed_agent_after_delete_test_hook(Some(Box::new(move || {
            deleted_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
        })));

        let worker_db = db.clone();
        let worker = thread::spawn(move || {
            replace_killed_agent_and_requeue_job_sync(
                &worker_db,
                "s1",
                &spec,
                "hash-atomic",
                222,
                Some(&intent),
            )
        });

        deleted_rx.recv().unwrap();
        let visible_during_cascade = query_job_sync(&observer, "job_atomic")
            .unwrap()
            .expect("job must remain visible to other connections before atomic commit");
        assert_eq!(visible_during_cascade.status, "DISPATCHED");
        assert_eq!(
            visible_during_cascade.request_id.as_deref(),
            Some("req-atomic")
        );

        resume_tx.send(()).unwrap();
        let requeued = worker.join().unwrap().unwrap();
        set_replace_killed_agent_after_delete_test_hook(None);
        assert_eq!(requeued, 1);

        let job = query_job_sync(&db.conn(), "job_atomic").unwrap().unwrap();
        assert_eq!(job.id, "job_atomic");
        assert_eq!(job.agent_id, "a_atomic");
        assert_eq!(job.request_id.as_deref(), Some("req-atomic"));
        assert_eq!(job.prompt_text, "continue atomic\n");
        assert_eq!(job.status, "QUEUED");
        assert_eq!(job.dispatched_at, None);
        assert_eq!(job.dispatched_at_seq_id, None);
        assert_eq!(job.completed_at, None);
        assert!(job.cancel_requested);
        assert!(job.requires_physical_evidence);
        assert!(!job.requires_test_evidence);
        assert_eq!(job.error_reason.as_deref(), Some("RECOVERY_REQUEUED:0"));

        let agent = crate::db::agents::query_agent_sync(&db.conn(), "a_atomic")
            .unwrap()
            .unwrap();
        assert_eq!(agent.state, "SPAWNING");
        assert_eq!(agent.pid, Some(222));
        let stored = query_agent_spawn_spec_sync(&db.conn(), "a_atomic")
            .unwrap()
            .unwrap();
        assert_eq!(stored.config_hash, "hash-atomic");
    }

    #[test]
    fn rr_worker_requeue_only_interrupted_failed_job_not_ordinary_failed_job() {
        with_db(|db| {
            let conn = db.conn();
            insert_session_sync(&conn, "s1", "p1", "/tmp/rr-worker").unwrap();
            insert_agent_sync(&conn, "a_requeue", "s1", "codex", "CRASHED", None).unwrap();
            insert_job_sync(&conn, "job_interrupted", "a_requeue", None, "continue\n").unwrap();
            insert_job_sync(&conn, "job_ordinary", "a_requeue", None, "old failure\n").unwrap();
            conn.execute(
                "UPDATE jobs SET status = 'FAILED', error_reason = 'AGENT_UNEXPECTED_EXIT', completed_at = unixepoch()",
                [],
            )
            .unwrap();

            let captured = captured_revive_intent("a_requeue", "job_interrupted");
            drop(conn);
            let changed =
                requeue_interrupted_job_from_captured_intent_standalone_sync(db, &captured)
                    .expect("only captured interrupted job should be requeued");

            assert_eq!(changed, 1);
            let conn = db.conn();
            assert_eq!(
                query_job_sync(&conn, "job_interrupted")
                    .unwrap()
                    .unwrap()
                    .status,
                "QUEUED"
            );
            assert_eq!(
                query_job_sync(&conn, "job_ordinary")
                    .unwrap()
                    .unwrap()
                    .status,
                "FAILED"
            );
            let transition_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*)
                     FROM job_transitions
                     WHERE job_id = 'job_interrupted'
                       AND old_status = 'FAILED'
                       AND new_status = 'QUEUED'
                       AND reason = 'recovery_requeue'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(transition_count, 1);
        });
    }

    #[test]
    fn ra1_schema_new_db_has_agent_spawn_specs_and_backoff_columns() {
        with_db(|db| {
            let conn = db.conn();
            let create_sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='agent_spawn_specs'",
                    [],
                    |row| row.get(0),
                )
                .expect("agent_spawn_specs table should exist");
            assert!(create_sql.contains("agent_id TEXT PRIMARY KEY"));
            assert!(create_sql.contains("spec_version INTEGER NOT NULL DEFAULT 1"));
            assert!(create_sql.contains("provider TEXT NOT NULL"));
            assert!(create_sql.contains("config_hash TEXT NOT NULL"));
            assert!(create_sql.contains("spec_json TEXT NOT NULL"));
            assert!(create_sql.contains("updated_at INTEGER NOT NULL DEFAULT (unixepoch())"));
            assert!(create_sql.contains("STRICT"));

            let columns = table_columns(&conn, "agents");
            for column in ["retry_count", "next_retry_at", "retry_exhausted"] {
                assert!(
                    columns.iter().any(|name| name == column),
                    "missing {column}"
                );
            }
        });
    }

    #[test]
    fn ra1_migration_old_db_adds_specs_backoff_and_is_idempotent() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(file.path()).unwrap();
            conn.execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                "#,
            )
            .unwrap();
        }

        init(file.path()).unwrap();
        let db = init(file.path()).expect("R-A.1 migrations must be idempotent");
        let conn = db.conn();
        let specs_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_spawn_specs')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(specs_exists);
        let columns = table_columns(&conn, "agents");
        for column in ["retry_count", "next_retry_at", "retry_exhausted"] {
            assert!(
                columns.iter().any(|name| name == column),
                "missing {column}"
            );
        }
    }

    #[test]
    fn ra1_agent_spawn_specs_cascade_delete_with_agent() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            persist_agent_spawn_spec_sync(&conn, &sample_spec("a1", "codex"), "hash1").unwrap();
            conn.execute("DELETE FROM agents WHERE id = 'a1'", [])
                .unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM agent_spawn_specs WHERE agent_id = 'a1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        });
    }

    #[test]
    fn ra1_snapshot_round_trips_overwrites_and_rebuilds_realign_params_fields() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            let first = sample_spec("a1", "codex");
            persist_agent_spawn_spec_sync(&conn, &first, "hash1").unwrap();
            let stored = query_agent_spawn_spec_sync(&conn, "a1").unwrap().unwrap();
            assert_eq!(stored.spec, first);
            assert_eq!(stored.config_hash, "hash1");

            let mut second = sample_spec("a1", "claude");
            second.env.insert("NEW".to_string(), "1".to_string());
            second.plugins.push("extra@local".to_string());
            second.skills.push("domain-review".to_string());
            persist_agent_spawn_spec_sync(&conn, &second, "hash2").unwrap();
            let overwritten = query_agent_spawn_spec_sync(&conn, "a1").unwrap().unwrap();
            assert_eq!(overwritten.spec.agent_id, "a1");
            assert_eq!(overwritten.spec.provider, "claude");
            assert_eq!(overwritten.spec.env["NEW"], "1");
            assert!(overwritten.spec.hooks.contains_key("PreToolUse"));
            assert!(
                overwritten
                    .spec
                    .plugins
                    .contains(&"extra@local".to_string())
            );
            assert!(
                overwritten
                    .spec
                    .skills
                    .contains(&"domain-review".to_string())
            );
            assert_eq!(overwritten.config_hash, "hash2");
            assert!(overwritten.spec_version >= stored.spec_version);
            assert!(overwritten.updated_at >= stored.updated_at);
        });
    }

    #[test]
    fn ra1_snapshot_round_trips_sandbox_overrides() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            let mut spec = sample_spec("a1", "codex");
            spec.sandbox_overrides = SandboxOverrides {
                extra_binds: vec![],
                extra_ro_binds: vec![ReadOnlyBind {
                    host_path: "/opt/keys".to_string(),
                    sandbox_path: "/mnt/keys".to_string(),
                }],
            };

            persist_agent_spawn_spec_sync(&conn, &spec, "hash-binds").unwrap();

            let stored = query_agent_spawn_spec_sync(&conn, "a1").unwrap().unwrap();
            assert_eq!(
                stored.spec.sandbox_overrides.extra_ro_binds[0].host_path,
                "/opt/keys"
            );
            assert_eq!(
                stored.spec.sandbox_overrides.extra_ro_binds[0].sandbox_path,
                "/mnt/keys"
            );
        });
    }

    #[test]
    fn ra1_old_snapshot_without_sandbox_overrides_defaults_empty() {
        let spec: AgentSpawnSpec = serde_json::from_str(
            r#"{
                "agent_id": "old_a1",
                "provider": "bash",
                "env": {"OLD": "1"},
                "hooks": {},
                "plugins": []
            }"#,
        )
        .unwrap();

        assert!(spec.sandbox_overrides.extra_ro_binds.is_empty());
        assert!(spec.bundle.is_empty());
        assert!(spec.bundle_digest.is_none());
    }

    #[test]
    fn ra1_spawn_spec_serialization_never_contains_resolved_mcp_secret() {
        let mut spec = sample_spec("a1", "claude");
        spec.bundle = vec!["mcp".to_string()];
        spec.bundle_digest = Some(crate::provider::fingerprint::BundleDigest {
            bundles: vec![crate::provider::fingerprint::BundleDigestEntry {
                name: "mcp".to_string(),
                digest: "manifest contains ${ACME_KEY}".to_string(),
            }],
        });

        let json = serde_json::to_string(&spec).unwrap();

        assert!(json.contains("${ACME_KEY}"));
        assert!(!json.contains("secret-value"));
    }

    #[test]
    fn ra1_missing_snapshot_is_valid_none() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            let missing = query_agent_spawn_spec_sync(&conn, "a1").unwrap();
            assert!(missing.is_none());
        });
    }

    #[test]
    fn ra1_recovery_claim_cas_bumps_version_but_keeps_crashed() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            assert!(try_claim_agent_recovery_sync(&conn, "a1", 7, 1_000).unwrap());
            let row: (String, i64) = conn
                .query_row(
                    "SELECT state, state_version FROM agents WHERE id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(row, ("CRASHED".to_string(), 8));
            assert!(!try_claim_agent_recovery_sync(&conn, "a1", 7, 1_000).unwrap());
        });
    }

    #[test]
    fn ra1_recovery_claim_cas_allows_only_one_concurrent_winner() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ra1.db");
        {
            let db = init(&db_path).unwrap();
            let conn = db.conn();
            seed_crashed_agent(&conn);
        }
        let barrier = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|_| {
                let db_path = db_path.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let conn = Connection::open(db_path).unwrap();
                    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
                    barrier.wait();
                    try_claim_agent_recovery_sync(&conn, "a1", 7, 1_000).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let winners = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1);
    }

    #[test]
    fn ra1_backoff_failure_sequence_caps_and_exhausts_without_leaving_crashed() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            let expected = [
                (1, Some(1_001), false),
                (2, Some(1_002), false),
                (3, Some(1_004), false),
                (4, Some(1_004), false),
                (5, None, true),
            ];
            for (retry_count, next_retry_at, retry_exhausted) in expected {
                let backoff = record_recovery_failure_backoff_sync(&conn, "a1", 1_000).unwrap();
                assert_eq!(backoff.retry_count, retry_count);
                assert_eq!(backoff.next_retry_at, next_retry_at);
                assert_eq!(backoff.retry_exhausted, retry_exhausted);
                let state: String = conn
                    .query_row("SELECT state FROM agents WHERE id = 'a1'", [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(state, "CRASHED");
            }
        });
    }

    #[test]
    fn ra1_clear_recovery_backoff_resets_retry_fields() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            for _ in 0..5 {
                let _ = record_recovery_failure_backoff_sync(&conn, "a1", 1_000).unwrap();
            }
            clear_recovery_backoff_sync(&conn, "a1").unwrap();
            let row: (i64, Option<i64>, i64) = conn
                .query_row(
                    "SELECT retry_count, next_retry_at, retry_exhausted FROM agents WHERE id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(row, (0, None, 0));
        });
    }

    // P0-2 修复一(K8s CrashLoopBackOff 清零语义):清零 backoff 只在 agent 稳定运行 >= N
    // 分钟(=300s)后发生,经新缝 confirm_agent_stable_sync 无条件清零(N 由 caller/health
    // tick 把关,类比 master_revival::confirm_master_stable)。此测试 seed retry_count=3 →
    // 调 confirm_agent_stable_sync → 断言 backoff 被清零。现码该缝仅 `unimplemented!` 签名桩
    // → panic(RED);a1 填真身后 GREEN。
    #[test]
    fn test_stable_agent_clears_backoff_after_n_minutes() {
        with_db(|db| {
            let conn = db.conn();
            seed_crashed_agent(&conn);
            for _ in 0..3 {
                let _ = record_recovery_failure_backoff_sync(&conn, "a1", 1_000).unwrap();
            }
            let before: i64 = conn
                .query_row(
                    "SELECT retry_count FROM agents WHERE id = 'a1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(before, 3, "precondition: backoff accumulated to 3");

            confirm_agent_stable_sync(&conn, "a1").unwrap();

            let row: (i64, Option<i64>, i64) = conn
                .query_row(
                    "SELECT retry_count, next_retry_at, retry_exhausted FROM agents WHERE id = 'a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(
                row,
                (0, None, 0),
                "confirm_agent_stable_sync must clear backoff after >=N-min stability"
            );
        });
    }

    #[test]
    fn ra2_fresh_db_and_migrated_db_recovery_schema_are_equivalent() {
        let fresh_file = tempfile::NamedTempFile::new().unwrap();
        let fresh = init(fresh_file.path()).unwrap();
        let fresh_conn = fresh.conn();
        let fresh_specs_sql: String = fresh_conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='agent_spawn_specs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let fresh_agent_columns = table_columns(&fresh_conn, "agents");

        let migrated_file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = Connection::open(migrated_file.path()).unwrap();
            conn.execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    absolute_path TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    master_pid INTEGER NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    error_code TEXT,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                ) STRICT;
                "#,
            )
            .unwrap();
        }
        let migrated = init(migrated_file.path()).unwrap();
        let migrated_conn = migrated.conn();
        let migrated_specs_sql: String = migrated_conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='agent_spawn_specs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let migrated_agent_columns = table_columns(&migrated_conn, "agents");

        assert_eq!(fresh_specs_sql, migrated_specs_sql);
        for column in ["retry_count", "next_retry_at", "retry_exhausted"] {
            assert_eq!(
                fresh_agent_columns.iter().any(|name| name == column),
                migrated_agent_columns.iter().any(|name| name == column),
                "fresh/migrated mismatch for {column}"
            );
        }
    }
}
