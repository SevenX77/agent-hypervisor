use crate::cli::rpc_client::CliError;
use crate::tmux::compute_socket_name;
use serde_json::Value;
use std::io::Write;
use std::path::Path;
use tabled::Tabled;

#[derive(Tabled)]
pub struct SessionRow {
    pub session_id: String,
    pub project_id: String,
    pub path: String,
    pub status: String,
    pub master_state: String,
    pub db_tracked_agents: String,
}

#[derive(Tabled)]
pub struct AgentRow {
    pub agent_id: String,
    pub provider: String,
    pub state: String,
    pub sub_state: String,
    pub pid: String,
}

pub fn array_len(value: &Value, key: &str) -> usize {
    value.get(key).and_then(Value::as_array).map_or(0, Vec::len)
}

pub fn agent_row(agent: &Value) -> AgentRow {
    AgentRow {
        agent_id: string_field(agent, "id"),
        provider: string_field(agent, "provider"),
        state: string_field(agent, "state"),
        sub_state: option_string_field(agent, "sub_state"),
        pid: option_i64_field(agent, "pid"),
    }
}

pub fn session_row(session: &Value) -> SessionRow {
    SessionRow {
        session_id: string_field(session, "id"),
        project_id: string_field(session, "project_id"),
        path: string_field(session, "absolute_path"),
        status: string_field(session, "status"),
        master_state: string_field(session, "master_state"),
        db_tracked_agents: option_i64_field(session, "db_tracked_agents"),
    }
}

pub fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_string()
}

pub fn option_string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_string()
}

pub fn option_i64_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_i64)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "-".into())
}

pub fn print_tmux_hint(socket: &Path) -> Result<(), CliError> {
    let state_dir = socket.parent().ok_or_else(|| {
        CliError::InvalidResponse(format!(
            "socket path has no parent directory: {}",
            socket.display()
        ))
    })?;
    let tmux_socket = compute_socket_name(state_dir);
    println!();
    println!("\x1b[2m💡 To inspect live tmux sessions: tmux -L {tmux_socket} ls\x1b[0m");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{print_terminal_job, session_row};
    use serde_json::json;

    #[test]
    fn session_row_includes_status_and_db_tracked_agent_count() {
        let row = session_row(&json!({
            "id": "sess_1",
            "project_id": "project",
            "absolute_path": "/tmp/project",
            "status": "CLOSED",
            "master_state": "IDLE",
            "db_tracked_agents": 2,
            "active_agents": 99
        }));

        assert_eq!(row.session_id, "sess_1");
        assert_eq!(row.status, "CLOSED");
        assert_eq!(row.db_tracked_agents, "2");
    }

    #[test]
    fn cancelled_and_killed_jobs_are_not_successful_terminal_results() {
        for status in ["CANCELLED", "KILLED"] {
            let error = print_terminal_job(json!({"status": status})).unwrap_err();
            assert!(error.to_string().contains(status));
        }
    }
}

pub fn print_terminal_job(result: Value) -> Result<(), CliError> {
    match result.get("status").and_then(Value::as_str) {
        Some("COMPLETED") => {
            if let Some(reply_text) = result.get("reply_text").and_then(Value::as_str) {
                print!("{reply_text}");
                std::io::stdout().flush()?;
            }
            Ok(())
        }
        Some("FAILED") => {
            let reason = result
                .get("error_reason")
                .and_then(Value::as_str)
                .unwrap_or("job failed");
            eprintln!("{reason}");
            std::process::exit(2);
        }
        Some("CANCELLED") => Err(CliError::InvalidResponse(
            "job terminated with status CANCELLED".into(),
        )),
        Some("KILLED") => Err(CliError::InvalidResponse(
            "job terminated with status KILLED".into(),
        )),
        Some(other) => Err(CliError::InvalidResponse(format!(
            "job.wait returned non-terminal status {other}"
        ))),
        None => Err(CliError::InvalidResponse(
            "job.wait missing status field".into(),
        )),
    }
}

pub fn parse_event_payload(event: &Value) -> Result<Value, CliError> {
    let payload = event
        .get("payload")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::InvalidResponse("event missing payload string".into()))?;
    serde_json::from_str(payload).map_err(CliError::InvalidJson)
}
