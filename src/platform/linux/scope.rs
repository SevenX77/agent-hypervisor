//! Linux systemd-run scope command assembly.

use crate::provider::manifest::{ProviderManifest, collect_spawn_env};
use crate::sandbox::{EnvState, SandboxOverrides};
use crate::tmux::scope::{ScopePolicy, UnitConfig};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySpawn {
    pub is_recovery: bool,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeUnit {
    pub(crate) unit: String,
    pub(crate) description: String,
}

pub trait SystemctlRunner {
    fn list_scope_units(&self) -> Result<Vec<ScopeUnit>, io::Error>;
    fn unit_is_active(&self, _unit: &str) -> Result<bool, io::Error> {
        Ok(false)
    }
    fn stop_unit(&self, unit: &str) -> Result<(), io::Error>;
}

pub struct RealSystemctlRunner;

impl SystemctlRunner for RealSystemctlRunner {
    fn list_scope_units(&self) -> Result<Vec<ScopeUnit>, io::Error> {
        let output = Command::new("systemctl")
            .args([
                "--user",
                "list-units",
                "--all",
                "--type=scope",
                "--no-legend",
                "--plain",
                "--no-pager",
            ])
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "systemctl list-units failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(parse_systemctl_scope_units(&String::from_utf8_lossy(
            &output.stdout,
        )))
    }

    fn stop_unit(&self, unit: &str) -> Result<(), io::Error> {
        let output = Command::new("systemctl")
            .args(["--user", "--no-block", "stop", unit])
            .output()?;
        if output.status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "systemctl --no-block stop {unit} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )))
        }
    }

    fn unit_is_active(&self, unit: &str) -> Result<bool, io::Error> {
        let output = Command::new("systemctl")
            .args(["--user", "is-active", "--quiet", unit])
            .output()?;
        Ok(output.status.success())
    }
}

pub fn parse_systemctl_scope_units(output: &str) -> Vec<ScopeUnit> {
    output
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let unit = parts.next()?;
            if !unit.ends_with(".scope") {
                return None;
            }
            let description = line
                .splitn(5, char::is_whitespace)
                .nth(4)
                .unwrap_or("")
                .trim()
                .to_string();
            Some(ScopeUnit {
                unit: unit.to_string(),
                description,
            })
        })
        .collect()
}

pub fn is_own_ccbd_scope(scope: &ScopeUnit, daemon_marker: &str) -> bool {
    scope.description.contains(&format!("@{daemon_marker}"))
}

pub fn is_orphan_scope(scope: &ScopeUnit, live_refs: &HashSet<String>) -> bool {
    let searchable = format!("{} {}", scope.unit, scope.description);
    !live_refs
        .iter()
        .any(|reference| searchable.contains(reference))
}

pub fn wrap_in_scope(base_cmd: &str, base_args: &[&str], policy: &ScopePolicy) -> Command {
    let unit = match policy {
        ScopePolicy::None => {
            let mut command = Command::new(base_cmd);
            command.args(base_args);
            return command;
        }
        ScopePolicy::Systemd(unit) => unit,
    };

    let mut command = Command::new("systemd-run");
    command.args([
        "--user",
        "--scope",
        "--collect",
        &format!("--unit={}", unit.unit_name),
        &format!("--slice={}", unit.slice),
    ]);
    if let Some(binds_to) = &unit.binds_to {
        command.arg(format!("--property=BindsTo={binds_to}"));
        command.arg(format!("--property=PartOf={binds_to}"));
    }
    command.arg("--").arg(base_cmd).args(base_args);
    command
}

pub fn unit_name_for_socket(socket_name: &str) -> String {
    let suffix = socket_name
        .strip_prefix("ahd-")
        .or_else(|| socket_name.strip_prefix("ccbd-"))
        .unwrap_or(socket_name);
    format!("ahd-tmux-{}", &suffix[..suffix.len().min(8)])
}

pub fn detect_scope_policy(socket_name: &str) -> ScopePolicy {
    detect_scope_policy_with_daemon_unit(socket_name, None)
}

pub fn detect_scope_policy_with_daemon_unit(
    socket_name: &str,
    daemon_unit: Option<&str>,
) -> ScopePolicy {
    if !systemd_run_available() {
        return ScopePolicy::None;
    }

    ScopePolicy::Systemd(UnitConfig {
        unit_name: unit_name_for_socket(socket_name),
        slice: "ahd-agents.slice".to_string(),
        binds_to: daemon_unit.map(str::to_string),
    })
}

pub fn active_daemon_unit_or_none(daemon_unit: Option<String>) -> Option<String> {
    active_daemon_unit_or_none_with_runner(daemon_unit, &RealSystemctlRunner)
}

pub fn active_daemon_unit_or_none_with_runner(
    daemon_unit: Option<String>,
    runner: &dyn SystemctlRunner,
) -> Option<String> {
    let Some(unit) = daemon_unit else {
        return None;
    };
    match runner.unit_is_active(&unit) {
        Ok(true) => Some(unit),
        Ok(false) => {
            tracing::warn!(
                unit = %unit,
                "derived daemon unit is not active; spawning child scopes without BindsTo/PartOf"
            );
            None
        }
        Err(err) => {
            tracing::warn!(
                unit = %unit,
                error = %err,
                "failed to verify derived daemon unit; spawning child scopes without BindsTo/PartOf"
            );
            None
        }
    }
}

fn systemd_run_available() -> bool {
    Command::new("systemd-run")
        .args(["--user", "--scope", "--", "true"])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

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
    let recovery = RecoverySpawn {
        is_recovery,
        args: Vec::new(),
    };
    if env_state.unsafe_no_sandbox {
        return command_with_env_prefix(manifest, extra_env_vars, &recovery);
    }

    let mut cmd = vec![
        "systemd-run".to_string(),
        "--user".to_string(),
        "--scope".to_string(),
        "--collect".to_string(),
        format!("--description=ccbd-agent-{agent_id}@{daemon_marker}"),
    ];
    if env_state.under_systemd {
        cmd.push(format!("--slice={}", agent_slice_for_project(project_id)));
        append_daemon_unit_dependencies(&mut cmd, daemon_unit);
    }
    cmd.push("--".to_string());
    cmd.extend(command_with_env_prefix(manifest, extra_env_vars, &recovery));
    cmd
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
    wrap_command_with_recovery_and_sandbox_overrides(
        agent_id,
        project_id,
        daemon_marker,
        env_state,
        recovery,
        daemon_unit,
        manifest,
        extra_env_vars,
        &SandboxOverrides::default(),
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
    if env_state.unsafe_no_sandbox {
        return command_with_env_prefix(manifest, extra_env_vars, &recovery);
    }

    let mut cmd = vec![
        "systemd-run".to_string(),
        "--user".to_string(),
        "--scope".to_string(),
        "--collect".to_string(),
        format!("--description=ccbd-agent-{agent_id}@{daemon_marker}"),
    ];
    if env_state.under_systemd {
        cmd.push(format!("--slice={}", agent_slice_for_project(project_id)));
        append_daemon_unit_dependencies(&mut cmd, daemon_unit);
    }
    append_read_only_bind_overrides(&mut cmd, sandbox_overrides);
    cmd.push("--".to_string());
    cmd.extend(command_with_env_prefix(manifest, extra_env_vars, &recovery));
    cmd
}

pub fn master_command(
    project_id: &str,
    cmd: &str,
    env_state: &EnvState,
    daemon_unit: Option<&str>,
) -> Vec<String> {
    master_command_with_env(
        project_id,
        cmd,
        env_state,
        daemon_unit,
        &HashMap::new(),
        &SandboxOverrides::default(),
    )
}

pub fn master_command_with_env(
    project_id: &str,
    cmd: &str,
    env_state: &EnvState,
    daemon_unit: Option<&str>,
    extra_env_vars: &HashMap<String, String>,
    sandbox_overrides: &SandboxOverrides,
) -> Vec<String> {
    if env_state.unsafe_no_sandbox || !env_state.systemd_run_available {
        return master_shell_command_with_env_prefix(cmd, extra_env_vars);
    }

    let mut command = vec![
        "systemd-run".to_string(),
        "--user".to_string(),
        "--scope".to_string(),
        "--collect".to_string(),
    ];
    if env_state.under_systemd {
        command.push(format!(
            "--slice={}",
            workspace_slice_for_project(project_id)
        ));
        append_daemon_unit_dependencies(&mut command, daemon_unit);
    }
    append_read_only_bind_overrides(&mut command, sandbox_overrides);
    command.push("--".to_string());
    command.extend(master_shell_command_with_env_prefix(cmd, extra_env_vars));
    command
}

fn append_daemon_unit_dependencies(cmd: &mut Vec<String>, daemon_unit: Option<&str>) {
    let Some(unit) = daemon_unit else {
        return;
    };
    cmd.push(format!("--property=BindsTo={unit}"));
    cmd.push(format!("--property=PartOf={unit}"));
}

fn append_read_only_bind_overrides(cmd: &mut Vec<String>, overrides: &SandboxOverrides) {
    for bind in &overrides.extra_binds {
        cmd.push(format!(
            "--property=BindPaths={}:{}",
            bind.host_path, bind.sandbox_path
        ));
    }
    for bind in &overrides.extra_ro_binds {
        cmd.push(format!(
            "--property=BindReadOnlyPaths={}:{}",
            bind.host_path, bind.sandbox_path
        ));
    }
}

fn agent_slice_for_project(project_id: &str) -> String {
    format!("ccb-{}-ccbd-agents.slice", sanitize_project_id(project_id))
}

fn workspace_slice_for_project(project_id: &str) -> String {
    format!(
        "ccb-{}-ccbd-workspace.slice",
        sanitize_project_id(project_id)
    )
}

fn sanitize_project_id(project_id: &str) -> String {
    let sanitized = project_id
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || byte == b'-' {
                byte as char
            } else {
                '-'
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "project"
    } else {
        sanitized
    }
    .to_string()
}

fn command_with_env_prefix(
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
    recovery: &RecoverySpawn,
) -> Vec<String> {
    let mut cmd = Vec::new();
    let spawn_env = collect_spawn_env(manifest, extra_env_vars);
    let is_master = extra_env_vars.get("AH_ROLE").map(|s| s.as_str()) == Some("master");
    if !spawn_env.is_empty() || is_master {
        cmd.push("env".to_string());
        if is_master {
            cmd.push("-u".to_string());
            cmd.push("AH_AGENT_ID".to_string());
        }
        cmd.extend(
            spawn_env
                .into_iter()
                .map(|(key, value)| format!("{key}={value}")),
        );
    }
    cmd.extend(manifest.command.iter().map(|part| (*part).to_string()));
    if recovery.is_recovery {
        if recovery.args.is_empty() {
            cmd.extend(manifest.resume_args.iter().map(|part| (*part).to_string()));
        } else {
            cmd.extend(recovery.args.iter().cloned());
        }
    }
    if manifest.provider_name == "claude"
        && extra_env_vars
            .get("CLAUDE_CODE_USE_GATEWAY")
            .map(String::as_str)
            == Some("1")
        && let Some(shell) = claude_gateway_bridge_shell(&cmd.join(" "), extra_env_vars, None)
    {
        return vec!["sh".to_string(), "-lc".to_string(), shell];
    }
    cmd
}

fn claude_gateway_bind_context(extra_env_vars: &HashMap<String, String>) -> Option<PathBuf> {
    let sandbox_root = extra_env_vars.get(crate::claude_gateway::GATEWAY_SANDBOX_ROOT_ENV)?;
    Some(PathBuf::from(sandbox_root))
}

fn claude_gateway_bridge_shell(
    inner: &str,
    extra_env_vars: &HashMap<String, String>,
    ah_bin: Option<&Path>,
) -> Option<String> {
    claude_gateway_bridge_shell_with_ah_resolver(inner, extra_env_vars, ah_bin, || {
        crate::home_materialization::resolve_current_ah_binary_strict()
    })
}

#[cfg(test)]
fn claude_gateway_bridge_shell_for_exe_path(
    inner: &str,
    extra_env_vars: &HashMap<String, String>,
    current_exe: &Path,
) -> Option<String> {
    claude_gateway_bridge_shell_with_ah_resolver(inner, extra_env_vars, None, || {
        crate::home_materialization::resolve_ah_binary_strict(current_exe).ok_or_else(|| {
            format!(
                "cannot resolve ah binary next to ahd: no sibling ah beside {}",
                current_exe.display()
            )
        })
    })
}

fn claude_gateway_bridge_shell_with_ah_resolver(
    inner: &str,
    extra_env_vars: &HashMap<String, String>,
    ah_bin: Option<&Path>,
    resolve_ah_bin: impl FnOnce() -> Result<PathBuf, String>,
) -> Option<String> {
    let sandbox_root = claude_gateway_bind_context(extra_env_vars)?;
    let ah_bin = ah_bin
        .map(Path::to_path_buf)
        .map_or_else(resolve_ah_bin, Ok);
    Some(match ah_bin {
        Ok(ah_bin) => crate::claude_gateway::bridge_wrapper_shell(
            inner,
            &ah_bin,
            Path::new(crate::claude_gateway::SANDBOX_UDS_PATH),
            &sandbox_root,
        ),
        Err(err) => format!("echo {} >&2; exit 126", shell_quote(&err)),
    })
}

fn shell_quote(input: &str) -> String {
    format!("'{}'", input.replace('\'', "'\\''"))
}

fn master_shell_command_with_env_prefix(
    shell_cmd: &str,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<String> {
    let mut cmd = Vec::new();
    let is_master = extra_env_vars.get("AH_ROLE").map(|s| s.as_str()) == Some("master");
    if !extra_env_vars.is_empty() || is_master {
        cmd.push("env".to_string());
        if is_master {
            cmd.push("-u".to_string());
            cmd.push("AH_AGENT_ID".to_string());
        }
        let mut env_entries = extra_env_vars
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        env_entries.sort();
        cmd.extend(env_entries);
    }
    let command = if extra_env_vars
        .get("CLAUDE_CODE_USE_GATEWAY")
        .map(String::as_str)
        == Some("1")
        && let Some(command) = claude_gateway_bridge_shell(shell_cmd, extra_env_vars, None)
    {
        command
    } else {
        shell_cmd.to_string()
    };
    cmd.extend([
        "sh".to_string(),
        "-lc".to_string(),
        "echo 500 > /proc/self/oom_score_adj 2>/dev/null || { echo 'ccbd master failed to set oom_score_adj=500' >&2; exit 126; }; exec sh -lc \"$1\"".to_string(),
        "sh".to_string(),
        command,
    ]);
    cmd
}

#[cfg(test)]
mod tests {
    use super::{
        ScopeUnit, SystemctlRunner, active_daemon_unit_or_none_with_runner,
        claude_gateway_bridge_shell, claude_gateway_bridge_shell_for_exe_path,
        detect_scope_policy_with_daemon_unit, wrap_in_scope,
    };
    use std::io;
    use std::process::Command;

    struct UnitActiveRunner {
        active: bool,
    }

    impl SystemctlRunner for UnitActiveRunner {
        fn list_scope_units(&self) -> Result<Vec<ScopeUnit>, io::Error> {
            Ok(Vec::new())
        }

        fn unit_is_active(&self, _unit: &str) -> Result<bool, io::Error> {
            Ok(self.active)
        }

        fn stop_unit(&self, _unit: &str) -> Result<(), io::Error> {
            Ok(())
        }
    }

    fn command_args(command: &std::process::Command) -> Vec<String> {
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[cfg(unix)]
    fn fake_bridge_ah(dir: &std::path::Path) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("fake-ah");
        std::fs::write(
            &path,
            r#"#!/bin/sh
port_file=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --port-file)
      port_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
printf '45678' > "$port_file"
sleep 1
"#,
        )
        .unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    #[test]
    fn inactive_declared_daemon_unit_omits_binds_to_from_tmux_scope() {
        let daemon_unit = active_daemon_unit_or_none_with_runner(
            Some("ah-test.service".to_string()),
            &UnitActiveRunner { active: false },
        );
        let policy =
            detect_scope_policy_with_daemon_unit("ahd-abc123def456789a", daemon_unit.as_deref());
        let command = wrap_in_scope("tmux", &["-L", "test", "new-session"], &policy);
        let args = command_args(&command);

        assert_eq!(command.get_program(), "systemd-run");
        assert!(
            !args
                .iter()
                .any(|arg| arg.starts_with("--property=BindsTo="))
        );
        assert!(!args.iter().any(|arg| arg.starts_with("--property=PartOf=")));
    }

    #[test]
    fn active_declared_daemon_unit_keeps_binds_to_in_tmux_scope() {
        let daemon_unit = active_daemon_unit_or_none_with_runner(
            Some("ah-test.service".to_string()),
            &UnitActiveRunner { active: true },
        );
        let policy =
            detect_scope_policy_with_daemon_unit("ahd-abc123def456789a", daemon_unit.as_deref());
        let command = wrap_in_scope("tmux", &["-L", "test", "new-session"], &policy);
        let args = command_args(&command);

        assert_eq!(command.get_program(), "systemd-run");
        assert!(args.contains(&"--property=BindsTo=ah-test.service".to_string()));
        assert!(args.contains(&"--property=PartOf=ah-test.service".to_string()));
    }

    #[test]
    #[cfg(unix)]
    fn claude_gateway_wrapper_uses_explicit_sandbox_root_not_short_uds_parent() {
        use std::collections::HashMap;

        let tools = tempfile::tempdir().unwrap();
        let fake_ah = fake_bridge_ah(tools.path());
        let root_a = tempfile::tempdir().unwrap();
        let root_b = tempfile::tempdir().unwrap();
        let out_a = root_a.path().join("inner.out");
        let out_b = root_b.path().join("inner.out");

        let mut env_a = HashMap::new();
        env_a.insert("CLAUDE_CODE_USE_GATEWAY".to_string(), "1".to_string());
        env_a.insert(
            "AH_CLAUDE_GATEWAY_HOST_UDS".to_string(),
            "/tmp/ah-gw-worker-a.sock".to_string(),
        );
        env_a.insert(
            crate::claude_gateway::GATEWAY_SANDBOX_ROOT_ENV.to_string(),
            root_a.path().display().to_string(),
        );
        let mut env_b = env_a.clone();
        env_b.insert(
            "AH_CLAUDE_GATEWAY_HOST_UDS".to_string(),
            "/tmp/ah-gw-worker-b.sock".to_string(),
        );
        env_b.insert(
            crate::claude_gateway::GATEWAY_SANDBOX_ROOT_ENV.to_string(),
            root_b.path().display().to_string(),
        );

        let shell_a = claude_gateway_bridge_shell(
            &format!("printf '%s' \"$ANTHROPIC_BASE_URL\" > {}", out_a.display()),
            &env_a,
            Some(&fake_ah),
        )
        .unwrap();
        let shell_b = claude_gateway_bridge_shell(
            &format!("printf '%s' \"$ANTHROPIC_BASE_URL\" > {}", out_b.display()),
            &env_b,
            Some(&fake_ah),
        )
        .unwrap();

        assert!(shell_a.contains(&root_a.path().join("bridge.port").display().to_string()));
        assert!(shell_a.contains(&root_a.path().join("bridge.err").display().to_string()));
        assert!(!shell_a.contains("rm -f /bridge.port;"));
        assert!(shell_b.contains(&root_b.path().join("bridge.port").display().to_string()));
        assert!(shell_b.contains(&root_b.path().join("bridge.err").display().to_string()));
        assert!(!shell_b.contains("rm -f /bridge.port;"));

        let status_a = Command::new("sh").args(["-lc", &shell_a]).status().unwrap();
        let status_b = Command::new("sh").args(["-lc", &shell_b]).status().unwrap();
        assert!(status_a.success());
        assert!(status_b.success());

        assert_eq!(
            std::fs::read_to_string(root_a.path().join("bridge.port")).unwrap(),
            "45678"
        );
        assert_eq!(
            std::fs::read_to_string(root_b.path().join("bridge.port")).unwrap(),
            "45678"
        );
        assert!(root_a.path().join("bridge.err").exists());
        assert!(root_b.path().join("bridge.err").exists());
        assert_ne!(
            root_a.path().join("bridge.port"),
            root_b.path().join("bridge.port")
        );
        assert_eq!(
            std::fs::read_to_string(out_a).unwrap(),
            "http://localhost:45678"
        );
        assert_eq!(
            std::fs::read_to_string(out_b).unwrap(),
            "http://localhost:45678"
        );
    }

    #[test]
    fn claude_gateway_wrapper_resolves_sibling_ah_when_not_injected() {
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let ah_path = dir.path().join("ah");
        std::fs::write(&ah_path, b"#!/bin/sh\n").unwrap();
        let current_exe = dir.path().join("ahd");
        let sandbox_root = dir.path().join("sandbox");
        std::fs::create_dir_all(&sandbox_root).unwrap();
        let mut extra_env = HashMap::new();
        extra_env.insert("CLAUDE_CODE_USE_GATEWAY".to_string(), "1".to_string());
        extra_env.insert(
            crate::claude_gateway::GATEWAY_SANDBOX_ROOT_ENV.to_string(),
            sandbox_root.display().to_string(),
        );

        let shell =
            claude_gateway_bridge_shell_for_exe_path("claude", &extra_env, &current_exe).unwrap();

        assert!(shell.contains(&format!("'{}'", ah_path.display())));
        assert!(!shell.contains(&format!("'{}'", current_exe.display())));
    }

    #[test]
    fn claude_gateway_wrapper_fails_closed_when_sibling_ah_is_absent() {
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let current_exe = dir.path().join("ahd");
        let sandbox_root = dir.path().join("sandbox");
        std::fs::create_dir_all(&sandbox_root).unwrap();
        let mut extra_env = HashMap::new();
        extra_env.insert("CLAUDE_CODE_USE_GATEWAY".to_string(), "1".to_string());
        extra_env.insert(
            crate::claude_gateway::GATEWAY_SANDBOX_ROOT_ENV.to_string(),
            sandbox_root.display().to_string(),
        );

        let shell =
            claude_gateway_bridge_shell_for_exe_path("claude", &extra_env, &current_exe).unwrap();

        assert!(shell.contains("cannot resolve ah binary next to ahd"));
        assert!(shell.contains("exit 126"));
        assert!(!shell.contains(" ah internal-bridge"));
    }

    #[test]
    fn test_spawn_command_scrubs_inherited_env_master() {
        use crate::platform::sys::scope::EnvState;
        use crate::platform::sys::scope::master_command_with_env;
        use std::collections::HashMap;

        let mut extra_env = HashMap::new();
        crate::process_identity::inject_master_identity(&mut extra_env, "test-session");

        let env_state = EnvState {
            under_systemd: false,
            unsafe_no_sandbox: true,
            systemd_run_available: false,
        };

        let cmd = master_command_with_env(
            "test-project",
            "test-cmd",
            &env_state,
            None,
            &extra_env,
            &Default::default(),
        );

        assert_eq!(cmd[0], "env");
        assert_eq!(cmd[1], "-u");
        assert_eq!(cmd[2], "AH_AGENT_ID");
        assert!(cmd.iter().any(|arg| arg == "AH_ROLE=master"));
        assert!(cmd.iter().any(|arg| arg == "AH_SESSION_ID=test-session"));
        assert!(!cmd.iter().any(|arg| arg.starts_with("AH_AGENT_ID=")));
    }

    #[test]
    fn test_spawn_command_scrubs_inherited_env_worker() {
        use crate::platform::sys::scope::wrap_command_with_recovery_and_sandbox_overrides;
        use crate::platform::sys::scope::{EnvState, RecoverySpawn};
        use std::collections::HashMap;

        let mut extra_env = HashMap::new();
        crate::process_identity::inject_worker_identity(
            &mut extra_env,
            "test-session",
            "test-agent",
        );

        let env_state = EnvState {
            under_systemd: false,
            unsafe_no_sandbox: true,
            systemd_run_available: false,
        };

        let manifest = crate::provider::manifest::get_manifest("claude");

        let cmd = wrap_command_with_recovery_and_sandbox_overrides(
            "test-agent",
            "test-project",
            "test-marker",
            &env_state,
            RecoverySpawn {
                is_recovery: false,
                args: vec![],
            },
            None,
            &manifest,
            &extra_env,
            &Default::default(),
        );

        assert_eq!(cmd[0], "env");
        assert_ne!(cmd[1], "-u");
        assert!(cmd.iter().any(|arg| arg == "AH_ROLE=worker"));
        assert!(cmd.iter().any(|arg| arg == "AH_SESSION_ID=test-session"));
        assert!(cmd.iter().any(|arg| arg == "AH_AGENT_ID=test-agent"));
    }
}
