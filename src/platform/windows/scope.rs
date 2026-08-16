//! Windows scope command stubs for the M0 compile gate.

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
        Ok(Vec::new())
    }

    fn stop_unit(&self, _unit: &str) -> Result<(), io::Error> {
        Ok(())
    }
}

pub fn parse_systemctl_scope_units(_output: &str) -> Vec<ScopeUnit> {
    Vec::new()
}

pub fn is_own_ah_scope(_scope: &ScopeUnit, _daemon_marker: &str) -> bool {
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
        .or_else(|| socket_name.strip_prefix("ah-"))
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
    _agent_id: &str,
    _project_id: &str,
    _daemon_marker: &str,
    env_state: &EnvState,
    is_recovery: bool,
    _daemon_unit: Option<&str>,
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<String> {
    let recovery = RecoverySpawn {
        is_recovery,
        args: Vec::new(),
    };
    let _ = env_state;
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
    _agent_id: &str,
    _project_id: &str,
    _daemon_marker: &str,
    _env_state: &EnvState,
    recovery: RecoverySpawn,
    _daemon_unit: Option<&str>,
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
    _sandbox_overrides: &SandboxOverrides,
) -> Vec<String> {
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
    _project_id: &str,
    cmd: &str,
    _env_state: &EnvState,
    _daemon_unit: Option<&str>,
    extra_env_vars: &HashMap<String, String>,
    _sandbox_overrides: &SandboxOverrides,
) -> Vec<String> {
    shell_command_with_env_prefix(cmd, extra_env_vars)
}

fn command_with_env_prefix(
    manifest: &ProviderManifest,
    extra_env_vars: &HashMap<String, String>,
    recovery: &RecoverySpawn,
) -> Vec<String> {
    let mut cmd = Vec::new();
    for (key, value) in collect_spawn_env(manifest, extra_env_vars) {
        cmd.push(format!("{key}={value}"));
    }
    cmd.extend(manifest.command.iter().map(|part| (*part).to_string()));
    if recovery.is_recovery {
        cmd.extend(recovery.args.iter().cloned());
    }
    cmd
}

fn shell_command_with_env_prefix(
    cmd: &str,
    extra_env_vars: &HashMap<String, String>,
) -> Vec<String> {
    let mut command = Vec::new();
    for (key, value) in extra_env_vars {
        command.push(format!("{key}={value}"));
    }
    command.push(cmd.to_string());
    command
}
