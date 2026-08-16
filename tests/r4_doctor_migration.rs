//! T4.3.2: doctor warns about legacy ah-agents shared sessions.

use ah::cli::doctor::{DoctorStatus, legacy_shared_session_check_from_sessions};

#[test]
fn doctor_warns_when_ah_agents_session_exists() {
    let socket_sessions = vec![(
        "ah-test".to_string(),
        vec!["agent_a1".to_string(), "ah-agents".to_string()],
    )];

    let check = legacy_shared_session_check_from_sessions(&socket_sessions);

    assert_eq!(check.status, DoctorStatus::Warn);
    assert!(check.detail.contains("ah-test"));
    assert_eq!(
        check.suggestion.as_deref(),
        Some("tmux -L ah-test kill-session -t ah-agents")
    );
}

#[test]
fn doctor_passes_when_ah_agents_session_absent() {
    let socket_sessions = vec![(
        "ah-test".to_string(),
        vec!["agent_a1".to_string(), "master_p1".to_string()],
    )];

    let check = legacy_shared_session_check_from_sessions(&socket_sessions);

    assert_eq!(check.status, DoctorStatus::Pass);
    assert!(check.detail.contains("0 legacy"));
}
