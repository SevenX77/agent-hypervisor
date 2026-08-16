pub const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    absolute_path TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    master_pid INTEGER NOT NULL,
    master_pane_id TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    config_hash TEXT,
    master_cmd TEXT,
    master_provider TEXT,
    master_retry_count INTEGER NOT NULL DEFAULT 0,
    master_next_retry_at INTEGER NOT NULL DEFAULT 0,
    master_generation INTEGER NOT NULL DEFAULT 0,
    master_last_exit_reason TEXT,
    master_state TEXT NOT NULL DEFAULT 'IDLE' CHECK(master_state IN ('IDLE','BUSY')),
    master_pending_tell_request TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE TABLE IF NOT EXISTS master_cutovers (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK(state IN (
        'PREPARING',
        'SPAWNING',
        'VERIFYING',
        'ACTIVE',
        'ROLLED_BACK',
        'FAILED',
        'RELEASED'
    )),
    old_master_pid INTEGER,
    new_master_pid INTEGER,
    new_master_generation INTEGER,
    new_master_pane_id TEXT,
    ah_state_dir TEXT NOT NULL,
    ah_socket_path TEXT NOT NULL,
    handoff_path TEXT NOT NULL,
    ack_ready_at INTEGER,
    readiness_mode TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    completed_at INTEGER
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_master_cutovers_active
ON master_cutovers(session_id)
WHERE state IN ('PREPARING', 'SPAWNING', 'VERIFYING', 'ACTIVE');

CREATE TABLE IF NOT EXISTS master_recovery_windows (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    expected_pid INTEGER NOT NULL,
    expected_generation INTEGER NOT NULL,
    claimed_generation INTEGER,
    phase TEXT NOT NULL CHECK(phase IN ('DETECTED','WORKERS_REAPED','MASTER_SPAWNING','MASTER_RUNNING','MASTER_VERIFYING','WORKERS_REPROVISIONING','COMPLETED','FAILED','FUSED')),
    active_work INTEGER NOT NULL,
    defer_until INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    completed_at INTEGER,
    readiness_mode TEXT,
    ready_at INTEGER,
    ready_reason TEXT,
    readiness_token TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    lifecycle_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    state_version INTEGER NOT NULL DEFAULT 1,
    pid INTEGER,
    exit_code INTEGER,
    error_code TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    sub_state TEXT,
    config_hash TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER,
    retry_exhausted INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_agents_active ON agents(state) WHERE state NOT IN ('CRASHED');

CREATE TABLE IF NOT EXISTS agent_spawn_specs (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    spec_version INTEGER NOT NULL DEFAULT 1,
    provider TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

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

CREATE TABLE IF NOT EXISTS events (
    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    request_id TEXT,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_events_agent_seq ON events(agent_id, seq_id);
CREATE INDEX IF NOT EXISTS idx_events_agent_type_seq ON events(agent_id, event_type, seq_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotent ON events(agent_id, request_id) WHERE request_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS provider_status_observations (
    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id TEXT NOT NULL UNIQUE,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    lifecycle_id TEXT NOT NULL,
    turn_id TEXT,
    observation_json TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_provider_status_observations_scope
ON provider_status_observations(agent_id, lifecycle_id, turn_id, observed_at_ms);

CREATE TABLE IF NOT EXISTS evidence (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    event_seq_id INTEGER NOT NULL REFERENCES events(seq_id),
    pane_bytes BLOB NOT NULL,
    failed_rules TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    l3_asserted_state TEXT,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    evidence_type TEXT,
    subject_path TEXT,
    payload TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_evidence_pending ON evidence(status, created_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_evidence_agent ON evidence(agent_id);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    request_id TEXT,
    prompt_text TEXT NOT NULL,
    reply_text TEXT,
    status TEXT NOT NULL DEFAULT 'QUEUED',
    error_reason TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    dispatched_at INTEGER,
    dispatched_at_seq_id INTEGER,
    completed_at INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    requires_physical_evidence INTEGER NOT NULL DEFAULT 0,
    requires_test_evidence INTEGER NOT NULL DEFAULT 0,
    governance_binding_json TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(agent_id, status, created_at) WHERE status IN ('QUEUED', 'DISPATCHED');
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotent ON jobs(agent_id, request_id) WHERE request_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_transitions (
    job_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    request_id TEXT,
    kind TEXT NOT NULL CHECK(kind IN ('job_transition', 'job_updated')),
    old_status TEXT,
    new_status TEXT,
    changed_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    job_created_at INTEGER,
    dispatched_at INTEGER,
    completed_at INTEGER,
    cancel_requested INTEGER NOT NULL,
    error_reason TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_job_transitions_event_id ON job_transitions(job_event_id);
CREATE INDEX IF NOT EXISTS idx_job_transitions_job_event_id ON job_transitions(job_id, job_event_id);
CREATE INDEX IF NOT EXISTS idx_job_transitions_agent_event_id ON job_transitions(agent_id, job_event_id);

CREATE TABLE IF NOT EXISTS prompt_experience (
    id TEXT PRIMARY KEY,
    provider TEXT,
    fingerprint_type TEXT NOT NULL,
    fingerprint_value TEXT NOT NULL,
    action_json TEXT NOT NULL,
    category TEXT NOT NULL,
    confidence REAL NOT NULL,
    source TEXT NOT NULL,
    used_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    last_used_at INTEGER NOT NULL DEFAULT (unixepoch()),
    trigger_state TEXT,
    UNIQUE(provider, fingerprint_type, fingerprint_value)
) STRICT;

CREATE TABLE IF NOT EXISTS learned_rules (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    category TEXT NOT NULL CHECK(category IN (
        'StartupReadiness',
        'RuntimeMarker',
        'ReplyExtraction'
    )),
    fingerprint_type TEXT NOT NULL CHECK(fingerprint_type IN ('regex')),
    fingerprint_value TEXT NOT NULL,
    regex_flags TEXT NOT NULL DEFAULT '[]',
    positive_examples_json TEXT NOT NULL,
    action_json TEXT,
    extraction_json TEXT,
    cursor_anchor_json TEXT,
    source_event_seq_id INTEGER REFERENCES events(seq_id),
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE(provider, category, fingerprint_type, fingerprint_value)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_learned_rules_lookup
ON learned_rules(provider, category, enabled, created_at);

-- ==========================================================================
-- R1 outbox transport (Completion Protocol Part R1 / JC-1)
-- --------------------------------------------------------------------------
-- JC-1 single transport-level dedup ledger. Checked at the outbox-consume
-- boundary BEFORE routing by `kind` (design R1-Q2). Keyed on the producer-minted
-- outbox `event_id` (a ULID/UUIDv7 string, or the reserved `selfcheck:` id), NOT
-- reused from `events.event_id` (which does not exist as a column) nor from
-- `job_transitions.job_event_id` (a disjoint AUTOINCREMENT namespace). A first-seen
-- INSERT proceeds to the handler effect in the SAME transaction; a conflicting
-- INSERT (0 rows) means already-applied -> drop + reap without re-dispatching.
CREATE TABLE IF NOT EXISTS outbox_consumed (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    consumed_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

-- STUB SINK for the not-yet-built F3 consumer (`job_done`/`job_fail`). R2-T2 owns the
-- real `apply_job_done_declaration_sync` that writes `job_transitions`
-- (DISPATCHED->COMPLETED/FAILED). This table exists ONLY so the JC-1 transport dedup
-- gate has an observable F3-side effect to pin under test: a replayed `job_done` must
-- not double-insert here. When R2 lands, the F3 branch of `consume_record` is
-- repointed at `job_transitions` and this table is dropped. See design R1-Q2 / JC-1.
CREATE TABLE IF NOT EXISTS outbox_job_declaration_stub (
    event_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    attempt_cookie TEXT,
    reply_text TEXT,
    reason TEXT,
    applied_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

-- Error-book retry bookkeeping for cold-scan replay (design R1-Q3). A record that
-- deserializes but whose handler effect fails (e.g. references an agent/dispatch that
-- no longer exists) is retried across cold-scans up to N times, then quarantined to
-- `outbox/dead/` rather than dropped or hot-looped.
CREATE TABLE IF NOT EXISTS outbox_apply_failures (
    event_id TEXT PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;
"#;

#[derive(Debug, Clone)]
pub struct Project {
    pub id: String,
    pub absolute_path: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub project_id: String,
    pub master_pane_id: Option<String>,
    pub status: String,
    pub config_hash: Option<String>,
    pub master_state: String,
    pub created_at: i64,
    pub absolute_path: String,
}

#[derive(Debug, Clone)]
pub struct Agent {
    pub id: String,
    pub session_id: String,
    pub provider: String,
    pub lifecycle_id: String,
    pub state: String,
    pub state_version: i64,
    pub pid: Option<i64>,
    pub exit_code: Option<i64>,
    pub error_code: Option<String>,
    pub sub_state: Option<String>,
    pub config_hash: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub seq_id: i64,
    pub agent_id: String,
    pub request_id: Option<String>,
    pub event_type: String,
    pub payload: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct Evidence {
    pub id: String,
    pub agent_id: String,
    pub event_seq_id: i64,
    pub status: String,
    pub job_id: Option<String>,
    pub evidence_type: Option<String>,
    pub subject_path: Option<String>,
    pub payload: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub agent_id: String,
    pub request_id: Option<String>,
    pub prompt_text: String,
    pub reply_text: Option<String>,
    pub status: String,
    pub error_reason: Option<String>,
    pub created_at: i64,
    pub dispatched_at: Option<i64>,
    pub dispatched_at_seq_id: Option<i64>,
    pub completed_at: Option<i64>,
    pub cancel_requested: bool,
    pub requires_physical_evidence: bool,
    pub requires_test_evidence: bool,
    pub governance_binding_json: Option<String>,
}
