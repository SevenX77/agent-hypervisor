use ah::tmux::{agent_session_name, master_session_name};

#[test]
fn agent_session_name_is_public_and_consistent() {
    assert_eq!(agent_session_name("a1"), "agent_a1");
    assert_eq!(agent_session_name("agent-42"), "agent_agent-42");
}

#[test]
fn master_session_name_is_public_and_consistent() {
    assert_eq!(master_session_name("project_xyz"), "master_project_xyz");
    assert_eq!(
        master_session_name("agent-hypervisor"),
        "master_agent-hypervisor"
    );
}
