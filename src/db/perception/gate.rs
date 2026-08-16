//! C1 — Perception write gate: the single entry point that may mutate `agents.state`.
//!
//! All state producers (health check, marker timer, hook/log paths) must route state
//! changes through this one function instead of issuing `UPDATE agents SET state` in their
//! own call frame. Phase 1 only *builds* the gate and proves it works against a synthetic
//! caller (the acceptance tests) — no producer is migrated yet (that is Phase 2).
//!
//! g1-m1: implement the body to satisfy `super::phase1_acceptance`. The observable contract:
//! CAS on `expected_version`; honor the `from_states` guard; on a matched write, transition
//! `agents.state` to `to_state`, bump `state_version` by exactly one, and record a
//! `state_change` audit event carrying the caller-supplied `reason` **verbatim** (see the
//! existing `state_machine::transit_agent_state_conn_inner` payload convention — do not
//! hardcode a reason). Return `Ok(true)` on a matched write, `Ok(false)` on a rejected
//! precondition (missing agent / from-state mismatch / stale version). This is the only
//! place a raw `agents.state` write is permitted; the CI grep rule enforces that.

use crate::db::common::map_db_error;
use crate::error::AhError;
use rusqlite::{Connection, OptionalExtension, params};

/// The single arbitration entry point for `agents.state`.
///
/// Returns `Ok(true)` iff the CAS write applied; `Ok(false)` for a rejected precondition.
pub(crate) fn transit_agent_perception_state_sync(
    conn: &mut Connection,
    agent_id: &str,
    from_states: &[&str],
    to_state: &str,
    reason: &str,
    expected_version: i64,
) -> Result<bool, AhError> {
    // 1. Query current state and state_version
    let current: Option<(String, i64)> = conn
        .query_row(
            "SELECT state, state_version FROM agents WHERE id = ?",
            params![agent_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|err| map_db_error("query agent state in perception gate", err))?;

    let (current_state, current_version) = match current {
        Some(val) => val,
        None => return Ok(false), // Agent not found
    };

    // 2. Check CAS version
    if current_version != expected_version {
        return Ok(false);
    }

    // 3. Check from_states guard
    if !from_states.contains(&current_state.as_str()) {
        return Ok(false);
    }

    // 4. Update the state and version
    let new_version = expected_version + 1;
    let rows_affected = conn
        .execute(
            "UPDATE agents SET state = ?, state_version = ? WHERE id = ? AND state_version = ?",
            params![to_state, new_version, agent_id, expected_version],
        )
        .map_err(|err| map_db_error("update agent state in perception gate", err))?;

    if rows_affected == 0 {
        return Ok(false);
    }

    // 5. Insert audit event
    let payload = serde_json::json!({
        "from": current_state,
        "to": to_state,
        "reason": reason,
    })
    .to_string();

    conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'state_change', ?)",
        params![agent_id, payload],
    )
    .map_err(|err| map_db_error("insert state_change event in perception gate", err))?;

    Ok(true)
}
