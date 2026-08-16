//! systemd-run command wrapping for agent processes.

pub use crate::platform::sys::scope::RecoverySpawn;
use crate::provider::manifest::ProviderManifest;
use crate::sandbox::{EnvState, SandboxOverrides};
use std::collections::HashMap;

/// Wrap the provider entrypoint in `systemd-run --user --scope`.
pub fn wrap_command(
    agent_id: &str,
    project_id: &str,
    daemon_marker: &str,
    env_state: &EnvState,
    // True only when session.realign matched a CRASHED agent.
    is_recovery: bool,
    daemon_unit: Option<&str>,
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<String> {
    crate::platform::sys::scope::wrap_command(
        agent_id,
        project_id,
        daemon_marker,
        env_state,
        is_recovery,
        daemon_unit,
        manifest,
        extra_env_vars,
    )
}

pub fn wrap_command_with_recovery(
    agent_id: &str,
    project_id: &str,
    daemon_marker: &str,
    env_state: &EnvState,
    recovery: RecoverySpawn,
    daemon_unit: Option<&str>,
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<String> {
    crate::platform::sys::scope::wrap_command_with_recovery(
        agent_id,
        project_id,
        daemon_marker,
        env_state,
        recovery,
        daemon_unit,
        manifest,
        extra_env_vars,
    )
}

pub fn wrap_command_with_recovery_and_sandbox_overrides(
    agent_id: &str,
    project_id: &str,
    daemon_marker: &str,
    env_state: &EnvState,
    recovery: RecoverySpawn,
    daemon_unit: Option<&str>,
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
    sandbox_overrides: &SandboxOverrides,
) -> Vec<String> {
    crate::platform::sys::scope::wrap_command_with_recovery_and_sandbox_overrides(
        agent_id,
        project_id,
        daemon_marker,
        env_state,
        recovery,
        daemon_unit,
        manifest,
        extra_env_vars,
        sandbox_overrides,
    )
}

pub fn master_command(
    project_id: &str,
    cmd: &str,
    env_state: &EnvState,
    daemon_unit: Option<&str>,
) -> Vec<String> {
    crate::platform::sys::scope::master_command(project_id, cmd, env_state, daemon_unit)
}

pub fn master_command_with_env(
    project_id: &str,
    cmd: &str,
    env_state: &EnvState,
    daemon_unit: Option<&str>,
    extra_env_vars: &HashMap<String, String>,
    sandbox_overrides: &SandboxOverrides,
) -> Vec<String> {
    crate::platform::sys::scope::master_command_with_env(
        project_id,
        cmd,
        env_state,
        daemon_unit,
        extra_env_vars,
        sandbox_overrides,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        master_command, master_command_with_env, wrap_command,
        wrap_command_with_recovery_and_sandbox_overrides,
    };
    use crate::provider::manifest::get_manifest;
    use crate::sandbox::{EnvState, ReadOnlyBind, SandboxOverrides};
    use std::collections::HashMap;

    fn env_state(unsafe_no_sandbox: bool) -> EnvState {
        EnvState {
            systemd_run_available: !unsafe_no_sandbox,
            unsafe_no_sandbox,
            under_systemd: !unsafe_no_sandbox,
        }
    }

    fn env_state_with_systemd(under_systemd: bool) -> EnvState {
        EnvState {
            systemd_run_available: true,
            unsafe_no_sandbox: false,
            under_systemd,
        }
    }

    fn argv_strings(args: &[String]) -> Vec<String> {
        args.to_vec()
    }

    fn extra_env() -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }

    fn assert_collect_after_scope_before_separator(cmd: &[String]) {
        let scope = cmd.iter().position(|arg| arg == "--scope").unwrap();
        let collect = cmd.iter().position(|arg| arg == "--collect").unwrap();
        let separator = cmd.iter().position(|arg| arg == "--").unwrap();

        assert!(scope < collect, "--collect must appear after --scope");
        assert!(collect < separator, "--collect must appear before --");
    }

    #[test]
    fn test_wrap_command_uses_systemd_scope() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );
        let argv = argv_strings(&cmd);

        assert_eq!(argv[0], "systemd-run");
        assert!(argv.contains(&"--user".to_string()));
        assert!(argv.contains(&"--scope".to_string()));
        assert_collect_after_scope_before_separator(&argv);
        assert!(argv.contains(&"--slice=ah-p1-agents.slice".to_string()));
        assert!(argv.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(argv.contains(&"--property=PartOf=ahd.service".to_string()));
        assert!(argv.contains(&"--description=ah-agent-ag_1@ah-test".to_string()));
        assert!(!argv.contains(&"bwrap".to_string()));
        assert!(argv.contains(&"bash".to_string()));
        let separator = argv.iter().position(|arg| arg == "--").unwrap();
        assert_eq!(argv[separator + 1], "env");
        assert!(argv.contains(&"PS1=$ ".to_string()));
        assert!(!argv.contains(&"--no-block".to_string()));
    }

    #[test]
    fn test_wrap_command_applies_read_only_bind_overrides_before_separator() {
        let overrides = SandboxOverrides {
            extra_binds: vec![],
            extra_ro_binds: vec![ReadOnlyBind {
                host_path: "/opt/tools".to_string(),
                sandbox_path: "/mnt/tools".to_string(),
            }],
        };
        let cmd = wrap_command_with_recovery_and_sandbox_overrides(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            super::RecoverySpawn {
                is_recovery: false,
                args: Vec::new(),
            },
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
            &overrides,
        );

        let bind = "--property=BindReadOnlyPaths=/opt/tools:/mnt/tools".to_string();
        let bind_pos = cmd
            .iter()
            .position(|arg| arg == &bind)
            .expect("bind property");
        let separator = cmd.iter().position(|arg| arg == "--").unwrap();
        assert!(bind_pos < separator);
    }

    #[test]
    fn test_wrap_command_description_contains_daemon_marker() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-1234567890abcdef",
            &env_state_with_systemd(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--description=ah-agent-ag_1@ah-1234567890abcdef".to_string()));
    }

    #[test]
    fn test_wrap_command_binds_to_daemon_not_session_anchor() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(
            !cmd.iter()
                .any(|arg| arg.starts_with("--property=BindsTo=ahd-session-"))
        );
    }

    #[test]
    fn test_wrap_command_uses_project_slice_hierarchy() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--slice=ah-p1-agents.slice".to_string()));
        assert!(!cmd.contains(&"--slice=ah-agents.slice".to_string()));
    }

    #[test]
    fn test_master_command_uses_systemd_run_workspace_slice() {
        let cmd = master_command(
            "p1",
            "claude",
            &env_state_with_systemd(true),
            Some("ahd.service"),
        );

        assert_eq!(cmd[0], "systemd-run");
        assert!(cmd.contains(&"--user".to_string()));
        assert!(cmd.contains(&"--scope".to_string()));
        assert_collect_after_scope_before_separator(&cmd);
        assert!(!cmd.contains(&"--property=OOMScoreAdjust=500".to_string()));
        assert!(cmd.contains(&"--slice=ah-p1-workspace.slice".to_string()));
        assert!(cmd.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ahd.service".to_string()));
        assert!(
            cmd.iter()
                .any(|arg| arg.contains("/proc/self/oom_score_adj"))
        );
        assert!(cmd.iter().any(|arg| arg.contains("exec sh -lc")));
        assert!(cmd.contains(&"claude".to_string()));
    }

    #[test]
    fn test_master_command_passes_complex_argv_through_sh_lc() {
        let master_cmd = r#"claude --model "sonnet latest" --permission-mode acceptEdits"#;
        let cmd = master_command(
            "p1",
            master_cmd,
            &env_state_with_systemd(true),
            Some("ahd.service"),
        );
        let sh_pos = cmd.iter().position(|arg| arg == "sh").unwrap();

        assert_eq!(cmd[sh_pos], "sh");
        assert_eq!(cmd[sh_pos + 1], "-lc");
        assert!(cmd[sh_pos + 2].contains("/proc/self/oom_score_adj"));
        assert!(cmd[sh_pos + 2].contains("exec sh -lc"));
        assert_eq!(cmd[sh_pos + 3], "sh");
        assert_eq!(cmd[sh_pos + 4], master_cmd);
    }

    #[test]
    fn test_master_command_preserves_quotes_and_spacing_as_single_shell_arg() {
        let master_cmd = r#"claude   --model "sonnet latest"   /remote-control"#;
        let cmd = master_command(
            "p1",
            master_cmd,
            &env_state_with_systemd(false),
            Some("ahd.service"),
        );
        let sh_pos = cmd.iter().position(|arg| arg == "sh").unwrap();

        assert_eq!(cmd[sh_pos + 1], "-lc");
        assert!(cmd[sh_pos + 2].contains("/proc/self/oom_score_adj"));
        assert_eq!(cmd[sh_pos + 3], "sh");
        assert_eq!(cmd[sh_pos + 4], master_cmd);
        assert_eq!(cmd.iter().filter(|arg| *arg == master_cmd).count(), 1);
    }

    #[test]
    fn test_master_command_injects_isolated_home_env_before_shell() {
        let mut extra_env = HashMap::new();
        extra_env.insert("HOME".to_string(), "/tmp/ah-home".to_string());
        extra_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            "/tmp/ah-home/.claude".to_string(),
        );

        let cmd = master_command_with_env(
            "p1",
            "claude",
            &env_state_with_systemd(false),
            None,
            &extra_env,
            &Default::default(),
        );
        let env_pos = cmd.iter().position(|arg| arg == "env").unwrap();

        assert_eq!(cmd[env_pos], "env");
        assert!(cmd.contains(&"HOME=/tmp/ah-home".to_string()));
        assert!(cmd.contains(&"CLAUDE_CONFIG_DIR=/tmp/ah-home/.claude".to_string()));
        assert_eq!(cmd[env_pos + 3], "sh");
        assert_eq!(cmd[env_pos + 4], "-lc");
        assert!(cmd[env_pos + 5].contains("/proc/self/oom_score_adj"));
        assert_eq!(cmd[env_pos + 6], "sh");
        assert_eq!(cmd[env_pos + 7], "claude");
    }

    #[test]
    fn spawn_master_pane_accepts_cutover_extra_env() {
        let mut extra_env = HashMap::new();
        extra_env.insert("AH_CUTOVER_ID".to_string(), "cutover-sess_1".to_string());
        extra_env.insert(
            "AH_MASTER_HANDOFF".to_string(),
            "/tmp/ah-state/cutovers/cutover-sess_1/handoff.md".to_string(),
        );

        let cmd = master_command_with_env(
            "p1",
            "claude --continue /remote-control",
            &env_state_with_systemd(false),
            None,
            &extra_env,
            &Default::default(),
        );
        let env_pos = cmd.iter().position(|arg| arg == "env").unwrap();
        let sh_pos = cmd.iter().position(|arg| arg == "sh").unwrap();

        assert!(env_pos < sh_pos);
        assert!(cmd.contains(&"AH_CUTOVER_ID=cutover-sess_1".to_string()));
        assert!(cmd.contains(
            &"AH_MASTER_HANDOFF=/tmp/ah-state/cutovers/cutover-sess_1/handoff.md".to_string()
        ));
    }

    #[test]
    fn cutover_master_env_contains_ah_socket_and_ah_state_dir() {
        let mut extra_env = HashMap::new();
        extra_env.insert("AH_STATE_DIR".to_string(), "/tmp/ah-state".to_string());
        extra_env.insert(
            "AH_SOCKET".to_string(),
            "/tmp/ah-state/ahd.sock".to_string(),
        );
        extra_env.insert("AH_CUTOVER_ID".to_string(), "cutover-sess_1".to_string());
        extra_env.insert(
            "AH_MASTER_HANDOFF".to_string(),
            "/tmp/ah-state/cutovers/cutover-sess_1/handoff.md".to_string(),
        );
        extra_env.insert("AH_MASTER_ROLE".to_string(), "managed".to_string());

        let cmd = master_command_with_env(
            "p1",
            "claude --continue /remote-control",
            &env_state_with_systemd(false),
            None,
            &extra_env,
            &Default::default(),
        );

        assert!(cmd.contains(&"AH_SOCKET=/tmp/ah-state/ahd.sock".to_string()));
        assert!(cmd.contains(&"AH_STATE_DIR=/tmp/ah-state".to_string()));
        assert!(cmd.contains(&"AH_MASTER_ROLE=managed".to_string()));
        assert!(cmd.contains(&"AH_CUTOVER_ID=cutover-sess_1".to_string()));
        assert!(cmd.contains(
            &"AH_MASTER_HANDOFF=/tmp/ah-state/cutovers/cutover-sess_1/handoff.md".to_string()
        ));
    }

    #[test]
    fn ordinary_start_master_env_unchanged() {
        let cmd = master_command(
            "p1",
            "claude --continue /remote-control",
            &env_state_with_systemd(false),
            None,
        );
        let joined = cmd.join("\n");

        for key in [
            "AH_CUTOVER_ID=",
            "AH_MASTER_HANDOFF=",
            "AH_MASTER_ROLE=",
            "AH_SOCKET=",
            "AH_STATE_DIR=",
        ] {
            assert!(
                !joined.contains(key),
                "ordinary master command must not contain cutover env {key}: {cmd:?}"
            );
        }
    }

    #[test]
    fn test_master_command_unsafe_path_does_not_inject_oom_wrapper() {
        let cmd = master_command("p1", "claude", &env_state(true), Some("ahd.service"));

        assert!(!cmd.iter().any(|arg| arg.contains("oom_score_adj")));
        assert!(!cmd.contains(&"--property=OOMScoreAdjust=500".to_string()));
        let sh_pos = cmd.iter().position(|arg| arg == "sh").unwrap();
        assert_eq!(cmd[sh_pos + 1], "-lc");
        assert_eq!(cmd[sh_pos + 2], "claude");
    }

    #[test]
    fn test_wrap_command_under_systemd_uses_slice_and_binds_to() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state_with_systemd(true),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--slice=ah-p1-agents.slice".to_string()));
        assert!(cmd.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ahd.service".to_string()));
    }

    #[test]
    fn test_wrap_command_binds_to_detected_ah_service() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state_with_systemd(true),
            false,
            Some("ah-p1.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--slice=ah-p1-agents.slice".to_string()));
        assert!(cmd.contains(&"--property=BindsTo=ah-p1.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ah-p1.service".to_string()));
        assert!(
            !cmd.iter()
                .any(|arg| arg == "--property=BindsTo=ahd.service")
        );
    }

    #[test]
    fn test_wrap_command_no_detected_daemon_unit_omits_dependencies() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state_with_systemd(true),
            false,
            None,
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--slice=ah-p1-agents.slice".to_string()));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=BindsTo=")));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=PartOf=")));
    }

    #[test]
    fn test_wrap_command_dev_mode_omits_slice_and_binds_to() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state_with_systemd(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"systemd-run".to_string()));
        assert!(cmd.contains(&"--user".to_string()));
        assert!(cmd.contains(&"--scope".to_string()));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--slice=")));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=BindsTo=")));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=PartOf=")));
    }

    #[test]
    fn test_master_command_under_systemd_uses_workspace_slice() {
        let cmd = master_command(
            "p1",
            "claude",
            &env_state_with_systemd(true),
            Some("ahd.service"),
        );

        assert!(cmd.contains(&"--slice=ah-p1-workspace.slice".to_string()));
        assert!(cmd.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ahd.service".to_string()));
    }

    #[test]
    fn test_master_command_binds_to_detected_ah_service() {
        let cmd = master_command(
            "p1",
            "claude",
            &env_state_with_systemd(true),
            Some("ah-p1.service"),
        );

        assert!(cmd.contains(&"--slice=ah-p1-workspace.slice".to_string()));
        assert!(cmd.contains(&"--property=BindsTo=ah-p1.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ah-p1.service".to_string()));
        assert!(
            !cmd.iter()
                .any(|arg| arg == "--property=BindsTo=ahd.service")
        );
    }

    #[test]
    fn test_master_command_no_detected_daemon_unit_omits_dependencies() {
        let cmd = master_command("p1", "claude", &env_state_with_systemd(true), None);

        assert!(cmd.contains(&"--slice=ah-p1-workspace.slice".to_string()));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=BindsTo=")));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=PartOf=")));
    }

    #[test]
    fn test_master_command_dev_mode_no_slice() {
        let cmd = master_command(
            "p1",
            "claude",
            &env_state_with_systemd(false),
            Some("ahd.service"),
        );

        assert_eq!(cmd[0], "systemd-run");
        assert!(cmd.contains(&"--user".to_string()));
        assert!(cmd.contains(&"--scope".to_string()));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--slice=")));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=BindsTo=")));
        assert!(!cmd.iter().any(|arg| arg.starts_with("--property=PartOf=")));
        assert!(cmd.contains(&"claude".to_string()));
    }

    #[test]
    fn test_wrap_command_bypass_returns_provider() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(true),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );
        let argv = argv_strings(&cmd);

        assert_eq!(argv.first().map(String::as_str), Some("env"));
        assert!(argv.contains(&"PS1=$ ".to_string()));
        assert_eq!(
            &argv[argv.len() - 4..],
            ["bash", "--noprofile", "--norc", "-i"]
        );
    }

    #[test]
    fn test_wrap_command_appends_extra_env_after_manifest_env() {
        let mut extra = std::collections::HashMap::new();
        extra.insert("AH_GLOBAL".to_string(), "1".to_string());
        extra.insert("PS1".to_string(), "override> ".to_string());

        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra,
        );

        assert!(cmd.contains(&"AH_GLOBAL=1".to_string()));
        assert!(cmd.contains(&"PS1=override> ".to_string()));
    }

    #[test]
    fn test_wrap_command_systemd_scope_binds_to_daemon() {
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            false,
            Some("ahd.service"),
            &get_manifest("bash"),
            &extra_env(),
        );

        assert!(cmd.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ahd.service".to_string()));
    }

    #[test]
    fn test_wrap_command_with_recovery_appends_resume_args() {
        let claude_recovery = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(true),
            true,
            Some("ahd.service"),
            &get_manifest("claude"),
            &extra_env(),
        );
        assert!(claude_recovery.contains(&"claude".to_string()));
        assert!(claude_recovery.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(claude_recovery.contains(&"--continue".to_string()));

        let claude_new = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(true),
            false,
            Some("ahd.service"),
            &get_manifest("claude"),
            &extra_env(),
        );
        assert!(!claude_new.contains(&"--continue".to_string()));

        for provider in ["bash", "codex"] {
            let cmd = wrap_command(
                "ag_1",
                "p1",
                "ah-test",
                &env_state(true),
                true,
                Some("ahd.service"),
                &get_manifest(provider),
                &extra_env(),
            );
            assert!(
                !cmd.contains(&"--continue".to_string()),
                "{provider} should not add Claude resume args: {cmd:?}"
            );
        }
    }

    #[test]
    #[serial_test::serial(global_env)]
    fn test_wrap_command_injects_passthrough_and_forced_env() {
        unsafe {
            std::env::set_var("ANTHROPIC_API_KEY", "host-anthropic");
        }
        let cmd = wrap_command(
            "ag_1",
            "p1",
            "ah-test",
            &env_state(false),
            false,
            Some("ahd.service"),
            &get_manifest("claude"),
            &extra_env(),
        );
        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
        }

        assert!(cmd.contains(&"--property=BindsTo=ahd.service".to_string()));
        assert!(cmd.contains(&"--property=PartOf=ahd.service".to_string()));
        assert!(
            !cmd.contains(&"ANTHROPIC_API_KEY=host-anthropic".to_string()),
            "host Anthropic API key must not leak into sandbox command: {cmd:?}"
        );
        assert!(cmd.contains(&"AH_CLAUDE_MD_MODE=route".to_string()));
    }
}
