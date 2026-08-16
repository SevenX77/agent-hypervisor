//! C2 — `perception_events` channel.
//!
//! Decision (master, citing design.md 118-119 / requirements line 35): perception events
//! are a **typed convention over the existing `events` table**, not a new dedicated table.
//! A producer emits exactly one append-only `events` row; the `events.agent_id` column
//! carries the agent and the `events.payload` JSON carries the requirements-C2 fields
//! `layer` / `verdict` / `epoch` / `observed_at` / `detail`.
//!
//! g1-m1: implement both functions to satisfy `super::phase1_acceptance`. Reuse the existing
//! `events::insert_event_sync` INSERT path (or an equivalent single append into `events`) —
//! do **not** create a `perception_events` table, and do **not** expose any UPDATE/DELETE of
//! emitted rows to producer code (append-only).

use super::types::{PerceptionEvent, PerceptionLayer, Verdict};
use crate::db::common::map_db_error;
use crate::error::AhError;
use rusqlite::{Connection, params};
use serde_json::Value;

/// Emit one append-only perception event into the `events`-table channel.
///
/// Returns the `events.seq_id` of the inserted row.
pub(crate) fn emit_perception_event_sync(
    conn: &Connection,
    agent_id: &str,
    layer: PerceptionLayer,
    verdict: Verdict,
    epoch: i64,
    observed_at: i64,
    detail: &Value,
) -> Result<i64, AhError> {
    let layer_str = match layer {
        PerceptionLayer::Os => "Os",
        PerceptionLayer::Log => "Log",
        PerceptionLayer::Hook => "Hook",
    };
    let verdict_str = match verdict {
        Verdict::True => "True",
        Verdict::False => "False",
        Verdict::Unknown => "Unknown",
    };

    let payload = serde_json::json!({
        "layer": layer_str,
        "verdict": verdict_str,
        "epoch": epoch,
        "observed_at": observed_at,
        "detail": detail,
    })
    .to_string();

    let result = conn.execute(
        "INSERT INTO events (agent_id, request_id, event_type, payload) VALUES (?, NULL, 'perception', ?)",
        params![agent_id, payload],
    );

    match result {
        Ok(_) => Ok(conn.last_insert_rowid()),
        Err(err) => Err(map_db_error("insert perception event", err)),
    }
}

/// Read back every perception event for `agent_id`, decoded into [`PerceptionEvent`]s,
/// oldest-first.
pub(crate) fn query_perception_events_sync(
    conn: &Connection,
    agent_id: &str,
) -> Result<Vec<PerceptionEvent>, AhError> {
    let mut stmt = conn
        .prepare("SELECT payload FROM events WHERE agent_id = ? AND event_type = 'perception' ORDER BY seq_id ASC")
        .map_err(|err| map_db_error("prepare query perception events", err))?;

    let rows = stmt
        .query_map(params![agent_id], |row| {
            let payload: String = row.get(0)?;
            Ok(payload)
        })
        .map_err(|err| map_db_error("query perception events", err))?;

    let mut events = Vec::new();
    for payload_res in rows {
        let payload_str =
            payload_res.map_err(|err| map_db_error("get perception event row", err))?;
        let value: serde_json::Value = serde_json::from_str(&payload_str).map_err(|err| {
            AhError::DbConstraintViolation(format!(
                "invalid json in perception event payload: {err}"
            ))
        })?;

        let layer_str = value["layer"]
            .as_str()
            .ok_or_else(|| AhError::DbConstraintViolation("missing layer field".to_string()))?;
        let layer = match layer_str {
            "Os" | "os" => PerceptionLayer::Os,
            "Log" | "log" => PerceptionLayer::Log,
            "Hook" | "hook" => PerceptionLayer::Hook,
            other => {
                return Err(AhError::DbConstraintViolation(format!(
                    "unknown layer: {other}"
                )));
            }
        };

        let verdict_str = value["verdict"]
            .as_str()
            .ok_or_else(|| AhError::DbConstraintViolation("missing verdict field".to_string()))?;
        let verdict = match verdict_str {
            "True" | "true" => Verdict::True,
            "False" | "false" => Verdict::False,
            "Unknown" | "unknown" => Verdict::Unknown,
            other => {
                return Err(AhError::DbConstraintViolation(format!(
                    "unknown verdict: {other}"
                )));
            }
        };

        let epoch = value["epoch"].as_i64().ok_or_else(|| {
            AhError::DbConstraintViolation("missing or invalid epoch field".to_string())
        })?;

        let observed_at = value["observed_at"].as_i64().ok_or_else(|| {
            AhError::DbConstraintViolation("missing or invalid observed_at field".to_string())
        })?;

        let detail = value["detail"].clone();

        events.push(PerceptionEvent {
            agent_id: agent_id.to_string(),
            layer,
            verdict,
            epoch,
            observed_at,
            detail,
        });
    }

    Ok(events)
}
