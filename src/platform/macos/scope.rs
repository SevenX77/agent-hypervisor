//! macOS scope compile skeleton.

use crate::provider::manifest::{ProviderManifest, collect_spawn_env};
use crate::sandbox::{EnvState, SandboxOverrides};
use crate::tmux::scope::ScopePolicy;
use std::collections::{HashMap, HashSet};
use std::io;
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
        Err(io::Error::other(
            "macOS: systemctl scope listing is unsupported until PR-4 ownership reconcile",
        ))
    }

    fn unit_is_active(&self, unit: &str) -> Result<bool, io::Error> {
        Err(io::Error::other(format!(
            "macOS: execution-scope observation for {unit} is unsupported"
        )))
    }

    fn stop_unit(&self, unit: &str) -> Result<(), io::Error> {
        Err(io::Error::other(format!(
            "macOS: systemctl stop {unit} is unsupported until PR-4 ownership cascade"
        )))
    }
}

pub fn parse_systemctl_scope_units(_output: &str) -> Vec<ScopeUnit> {
    Vec::new()
}

pub fn is_own_ccbd_scope(_scope: &ScopeUnit, _daemon_marker: &str) -> bool {
    false
}

pub fn is_orphan_scope(_scope: &ScopeUnit, _live_refs: &HashSet<String>) -> bool {
    false
}

pub fn wrap_in_scope(base_cmd: &str, base_args: &[&str], _policy: &ScopePolicy) -> Command {
    let mut command = Command::new(base_cmd);
    command.args(base_args);
    command
}

pub fn unit_name_for_socket(socket_name: &str) -> String {
    let suffix = socket_name
        .strip_prefix("ahd-")
        .or_else(|| socket_name.strip_prefix("ccbd-"))
        .unwrap_or(socket_name);
    format!("ahd-tmux-{}", &suffix[..suffix.len().min(8)])
}

pub fn detect_scope_policy(_socket_name: &str) -> ScopePolicy {
    ScopePolicy::None
}

pub fn detect_scope_policy_with_daemon_unit(
    _socket_name: &str,
    _daemon_unit: Option<&str>,
) -> ScopePolicy {
    ScopePolicy::None
}

pub fn active_daemon_unit_or_none(_daemon_unit: Option<String>) -> Option<String> {
    None
}

pub fn active_daemon_unit_or_none_with_runner(
    _daemon_unit: Option<String>,
    _runner: &dyn SystemctlRunner,
) -> Option<String> {
    None
}

pub fn wrap_command(
    agent_id: &str,
    project_id: &str,
    daemon_marker: &str,
    env_state: &EnvState,
    is_recovery: bool,
    daemon_unit: Option<&str>,
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<String> {
    let _ = (agent_id, project_id, daemon_marker, env_state, daemon_unit);
    let recovery = RecoverySpawn {
        is_recovery,
        args: Vec::new(),
    };
    command_with_env_prefix(manifest, extra_env_vars, &recovery)
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
    let _ = (
        agent_id,
        project_id,
        daemon_marker,
        env_state,
        daemon_unit,
        sandbox_overrides,
    );
    command_with_env_prefix(manifest, extra_env_vars, &recovery)
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
    let _ = (project_id, env_state, daemon_unit, sandbox_overrides);
    shell_command_with_env_prefix(cmd, extra_env_vars)
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
    cmd
}

fn shell_command_with_env_prefix(
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
    cmd.extend(["sh".to_string(), "-lc".to_string(), shell_cmd.to_string()]);
    cmd
}
