//! T4.2.1: ah attach <agent_id> maps to agent_<agent_id>.

use ah::tmux::agent_session_name;
use std::process::Command;

#[test]
fn attach_session_name_resolves_to_agent_session_name() {
    assert_eq!(agent_session_name("a1"), "agent_a1");
    assert_eq!(agent_session_name("agent-42"), "agent_agent-42");
}

#[test]
fn attach_requires_agent_id_argument() {
    let output = Command::new(env!("CARGO_BIN_EXE_ah"))
        .args(["attach"])
        .env_remove("AH_SOCKET")
        .env_remove("AH_STATE_DIR")
        .env_remove("XDG_STATE_HOME")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("required") || stderr.contains("Usage:"));
}

#[test]
fn attach_help_documents_agent_id() {
    let output = Command::new(env!("CARGO_BIN_EXE_ah"))
        .args(["attach", "--help"])
        .env_remove("AH_SOCKET")
        .env_remove("AH_STATE_DIR")
        .env_remove("XDG_STATE_HOME")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("agent_id") || stdout.contains("<AGENT_ID>"));
    assert!(stdout.contains("agent_<agent_id>"));
}
