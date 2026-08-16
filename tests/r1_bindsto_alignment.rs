#![cfg(target_os = "linux")]
use ah::tmux::scope::{ScopePolicy, UnitConfig, wrap_in_scope};

fn args(command: &std::process::Command) -> Vec<String> {
    command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn tmux_scope_bindsto_uses_ah_service() {
    let policy = ScopePolicy::Systemd(UnitConfig {
        unit_name: "ahd-tmux-test1234".to_string(),
        slice: "ahd-agents.slice".to_string(),
        binds_to: Some("ahd.service".to_string()),
    });
    let command = wrap_in_scope("tmux", &["-L", "sock", "new-session"], &policy);
    let args = args(&command);

    assert!(args.contains(&"--property=BindsTo=ahd.service".to_string()));
    assert!(args.contains(&"--property=PartOf=ahd.service".to_string()));
    assert!(!args.contains(&"--property=BindsTo=agent-hypervisor.service".to_string()));
    assert!(!args.contains(&"--property=PartOf=agent-hypervisor.service".to_string()));
}
