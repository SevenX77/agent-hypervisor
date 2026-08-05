use crate::cli::config::{ProjectConfig, find_config, load_project_config};
use crate::cli::output::string_field;
use crate::cli::rpc_client::{CliError, RpcClient};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct StartOptions {
    pub config_path: Option<PathBuf>,
    pub cwd: PathBuf,
    pub wait: bool,
}

#[derive(Debug, Clone)]
pub struct StartSummary {
    pub session_id: String,
    pub agents: Vec<SpawnedAgent>,
}

#[derive(Debug, Clone)]
pub struct SpawnedAgent {
    pub agent_id: String,
    pub provider: String,
    pub pid: Option<i64>,
}

pub fn build_ahd_systemd_run_command(ahd_bin: &Path, state_dir: &Path) -> Vec<String> {
    crate::platform::sys::service::build_ahd_systemd_run_command(ahd_bin, state_dir)
}

pub fn build_ahd_systemd_run_command_with_env(
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
) -> Vec<String> {
    crate::platform::sys::service::build_ahd_systemd_run_command_with_env(ahd_bin, state_dir, env)
}

pub fn build_ahd_systemd_run_command_with_parent(
    ahd_bin: &Path,
    state_dir: &Path,
    env: &[(String, String)],
    parent_unit: Option<&str>,
) -> Vec<String> {
    crate::platform::sys::service::build_ahd_systemd_run_command_with_parent(
        ahd_bin,
        state_dir,
        env,
        parent_unit,
    )
}

pub fn should_skip_systemd_bootstrap_for_cgroup(cgroup: &str, target_unit: &str) -> bool {
    crate::systemd_unit::detect_current_service_unit_from_cgroup(cgroup).as_deref()
        == Some(target_unit)
}

pub fn ahd_reset_failed_is_best_effort(unit: &str) -> bool {
    crate::platform::sys::service::ahd_reset_failed_is_best_effort(unit)
}

pub async fn start_from_options(
    client: &impl RpcClient,
    options: StartOptions,
) -> Result<StartSummary, CliError> {
    let config_path = match options.config_path {
        Some(path) => path,
        None => find_config(&options.cwd)?,
    };
    let config = load_project_config(&config_path)?;
    crate::cli::config::warn_config_diagnostics(&config);
    start_project(client, config, &config_path, options.cwd, options.wait).await
}

pub async fn start_project(
    client: &impl RpcClient,
    config: ProjectConfig,
    config_path: &Path,
    project_root: PathBuf,
    wait: bool,
) -> Result<StartSummary, CliError> {
    let canonical_project_root = project_root
        .canonicalize()
        .unwrap_or_else(|_| project_root.clone());
    let project_id = canonical_project_root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("project")
        .to_string();
    if let Some(session_id) = find_existing_start_session(client, &canonical_project_root).await? {
        let result = client
            .call(
                "session.realign",
                build_realign_payload(&session_id, &config, false),
            )
            .await?;
        let _ = result;
        let agents = config
            .agents
            .iter()
            .map(|(agent_id, agent)| SpawnedAgent {
                agent_id: agent_id.clone(),
                provider: agent.provider.clone(),
                pid: None,
            })
            .collect::<Vec<_>>();
        if wait {
            wait_until_agents_idle(client, &agents).await?;
        }
        return Ok(StartSummary { session_id, agents });
    }

    let session = client
        .call(
            "session.create",
            json!({
                "project_id": project_id,
                "absolute_path": canonical_project_root.display().to_string(),
            }),
        )
        .await?;
    let session_id = string_field(&session, "session_id");
    if session_id == "-" {
        return Err(CliError::InvalidResponse(
            "session.create missing session_id".into(),
        ));
    }

    let mut agents = Vec::new();
    let has_master = config.master.enabled;
    if has_master {
        let result = client
            .call(
                "session.spawn_master_pane",
                json!({
                    "session_id": session_id,
                    "cmd": config.master.cmd.clone(),
                    "provider": config.master.resolved_provider(),
                    // Same two channels the agents get: the master's own env,
                    // and the project env the server merges in.
                    "env": config.master.env,
                    "config_env": config.env,
                    "hooks": config.master.hooks,
                    "plugins": config.master.plugins,
                    "skills": config.master.skills,
                    "bundle": config.master.bundle,
                    "settings": config.master.settings,
                    "tmux_window_size": config.master.window_size,
                    "claude_shared_credentials_dir": config.providers.claude.shared_credentials_dir.clone(),
                }),
            )
            .await?;
        result
            .get("pane_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CliError::InvalidResponse("session.spawn_master_pane missing pane_id".into())
            })?;
    }
    for (agent_id, agent) in config.agents.iter() {
        let sandbox_overrides = json!({
            "extra_ro_binds": config.sandbox.additional_ro_binds
                .iter()
                .map(|path| json!({
                    "host_path": path,
                    "sandbox_path": path,
                }))
                .collect::<Vec<_>>(),
        });
        let result = client
            .call(
                "agent.spawn",
                json!({
                    "session_id": session_id,
                    "agent_id": agent_id,
                    "provider": agent.provider,
                    // ISSUE-13 §3a: send raw agent.env + project [env] separately; the
                    // server merges them (clients no longer pre-merge, which is what let the
                    // spawn side and realign side build divergent fingerprint envs).
                    "extra_env_vars": agent.env,
                    "config_env": config.env,
                    "hooks": agent.hooks,
                    "plugins": agent.plugins,
                    "skills": agent.skills,
                    "bundle": agent.bundle,
                    "settings": agent.settings,
                    "hook_push_enabled": config.completion.hook_push_enabled,
                    "sandbox_overrides": sandbox_overrides,
                    "claude_shared_credentials_dir": config.providers.claude.shared_credentials_dir.clone(),
                }),
            )
            .await;

        match result {
            Ok(value) => {
                value
                    .get("pane_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        CliError::InvalidResponse("agent.spawn missing pane_id".into())
                    })?;
                agents.push(SpawnedAgent {
                    agent_id: agent_id.clone(),
                    provider: agent.provider.clone(),
                    pid: value.get("pid").and_then(Value::as_i64),
                });
            }
            Err(err) => {
                let rollback = client
                    .call(
                        "session.kill",
                        json!({
                            "session_id": session_id,
                            "force": true,
                        }),
                    )
                    .await;
                if let Err(rollback_err) = rollback {
                    return Err(CliError::Config(format!(
                        "ccb start failed while spawning {agent_id} from {}; session.kill rollback failed after original error ({err}): {rollback_err}",
                        config_path.display()
                    )));
                }
                return Err(CliError::Config(format!(
                    "ccb start failed while spawning {agent_id} from {}; rolled back session {session_id}: {err}",
                    config_path.display()
                )));
            }
        }
    }

    if wait {
        wait_until_agents_idle(client, &agents).await?;
    }

    Ok(StartSummary { session_id, agents })
}

async fn find_existing_start_session(
    client: &impl RpcClient,
    project_root: &Path,
) -> Result<Option<String>, CliError> {
    let listed = client.call("session.list", json!({})).await?;
    let sessions = listed
        .get("sessions")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::InvalidResponse("session.list missing sessions".into()))?;
    let cwd = project_root.display().to_string();
    let matches = sessions
        .iter()
        .filter(|session| {
            session.get("status").and_then(Value::as_str) == Some("ACTIVE")
                && session
                    .get("absolute_path")
                    .and_then(Value::as_str)
                    .is_some_and(|absolute_path| absolute_path == cwd)
        })
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(string_field(matches[0], "id"))),
        _ => Err(CliError::Config(format!(
            "multiple recoverable sessions match {}; cannot choose one",
            project_root.display()
        ))),
    }
}

fn build_realign_payload(session_id: &str, config: &ProjectConfig, force: bool) -> Value {
    json!({
        "session_id": session_id,
        "force": force,
        // ISSUE-13 §3a: carry project [env] so realign fingerprints the same bare env the
        // spawn side stored (dropping it here is Leak B — phantom drift on every `ah up`).
        "config_env": config.env,
        "master": {
            "cmd": config.master.cmd,
            "provider": config.master.resolved_provider(),
            "env": config.master.env,
            "hooks": config.master.hooks,
            "plugins": config.master.plugins,
            "skills": config.master.skills,
            "bundle": config.master.bundle,
            "settings": config.master.settings,
            "tmux_window_size": config.master.window_size,
            "claude_shared_credentials_dir": config.providers.claude.shared_credentials_dir.clone(),
        },
        "agents": config.agents.iter().map(|(agent_id, agent)| {
            json!({
                "agent_id": agent_id,
                "provider": agent.provider,
                "env": agent.env,
                "hooks": agent.hooks,
                "plugins": agent.plugins,
                "skills": agent.skills,
                "bundle": agent.bundle,
                "settings": agent.settings,
                "claude_shared_credentials_dir": config.providers.claude.shared_credentials_dir.clone(),
            })
        }).collect::<Vec<_>>()
    })
}

async fn wait_until_agents_idle(
    client: &impl RpcClient,
    agents: &[SpawnedAgent],
) -> Result<(), CliError> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut remaining = agents
        .iter()
        .map(|agent| agent.agent_id.clone())
        .collect::<Vec<_>>();

    while !remaining.is_empty() && Instant::now() < deadline {
        let mut still_waiting = Vec::new();
        for agent_id in remaining {
            let result = client
                .call(
                    "agent.watch",
                    json!({
                        "agent_id": agent_id,
                        "since_event_id": 0,
                        "timeout": 1,
                    }),
                )
                .await?;
            let is_idle = result
                .get("events")
                .and_then(Value::as_array)
                .is_some_and(|events| {
                    events.iter().any(|event| {
                        event["event_type"] == "state_change"
                            && event["payload"]
                                .as_str()
                                .is_some_and(|payload| payload.contains("\"to\":\"IDLE\""))
                    })
                });
            if !is_idle {
                still_waiting.push(agent_id);
            }
        }
        remaining = still_waiting;
    }

    if remaining.is_empty() {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "timed out waiting for agents to become IDLE: {}",
            remaining.join(", ")
        )))
    }
}

pub fn print_start_summary(summary: &StartSummary) {
    println!("session_id={}", summary.session_id);
    for agent in &summary.agents {
        let pid = agent
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".into());
        println!(
            "agent_id={} provider={} pid={}",
            agent.agent_id, agent.provider, pid
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{should_skip_systemd_bootstrap_for_cgroup, start_project};
    use crate::cli::config::{
        AgentConfig, MasterConfig, ProjectConfig, SandboxConfig, load_project_config,
    };
    use crate::cli::rpc_client::{CliError, RpcClient, RpcFuture};
    use crate::tmux::TmuxWindowSize;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    struct RecordingClient {
        calls: Mutex<Vec<(String, Value)>>,
        sessions: Value,
    }

    impl RpcClient for RecordingClient {
        fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
            Box::pin(async move {
                let mut calls = self.calls.lock().unwrap();
                calls.push((method.to_string(), params));
                let call_index = calls.len();
                drop(calls);
                match method {
                    "session.list" => Ok(self.sessions.clone()),
                    "session.create" => Ok(json!({ "session_id": "sess_start" })),
                    "session.realign" => Ok(json!({ "results": [] })),
                    "session.spawn_master_pane" => Ok(json!({ "pane_id": "%0" })),
                    "agent.spawn" => Ok(json!({
                        "state": "SPAWNING",
                        "pid": 123,
                        "pane_id": format!("%{call_index}")
                    })),
                    other => Err(CliError::InvalidResponse(format!(
                        "unexpected method in recording client: {other}"
                    ))),
                }
            })
        }
    }

    #[test]
    fn service_bootstrap_recursion_guard_skips_only_matching_unit() {
        let cgroup = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/ah-p1.service";

        assert!(should_skip_systemd_bootstrap_for_cgroup(
            cgroup,
            "ah-p1.service"
        ));
        assert!(!should_skip_systemd_bootstrap_for_cgroup(
            cgroup,
            "ah-p2.service"
        ));
        assert!(!should_skip_systemd_bootstrap_for_cgroup(
            "0::/user.slice/user-1000.slice/user@1000.service/app.slice/interactive.scope",
            "ah-p1.service"
        ));
    }

    fn recording_client() -> RecordingClient {
        RecordingClient {
            calls: Mutex::new(Vec::new()),
            sessions: json!({ "sessions": [] }),
        }
    }

    #[tokio::test]
    async fn test_start_project_spawns_master_when_enabled() {
        let mut config = project_config(true);
        config.master.window_size = TmuxWindowSize::Follow;
        let client = recording_client();

        start_project(
            &client,
            config,
            std::path::Path::new("test-ah.toml"),
            std::env::current_dir().unwrap(),
            false,
        )
        .await
        .unwrap();

        let calls = client.calls.lock().unwrap();
        assert_eq!(calls[0].0, "session.list");
        assert_eq!(calls[1].0, "session.create");
        assert_eq!(calls[2].0, "session.spawn_master_pane");
        assert_eq!(calls[2].1["cmd"], "claude");
        assert_eq!(calls[2].1["tmux_window_size"], "follow");
        assert!(calls[2].1.get("auto_shutdown_on_master_exit").is_none());
        let agent_calls = calls
            .iter()
            .filter(|(method, _)| method == "agent.spawn")
            .collect::<Vec<_>>();
        assert_eq!(agent_calls.len(), 4);
        assert!(agent_calls[0].1.get("layout_parent_pane_id").is_none());
        assert!(agent_calls[0].1.get("layout_direction").is_none());
        assert!(agent_calls[0].1.get("layout_percent").is_none());
    }

    #[tokio::test]
    async fn test_start_project_passes_additional_ro_binds_to_agent_spawn() {
        let mut config = project_config(false);
        config.sandbox.additional_ro_binds = vec!["/opt/tools".to_string()];
        let client = recording_client();

        start_project(
            &client,
            config,
            std::path::Path::new("test-ah.toml"),
            std::env::current_dir().unwrap(),
            false,
        )
        .await
        .unwrap();

        let calls = client.calls.lock().unwrap();
        let first_agent = calls
            .iter()
            .find(|(method, _)| method == "agent.spawn")
            .unwrap();
        assert_eq!(
            first_agent.1["sandbox_overrides"]["extra_ro_binds"][0]["host_path"],
            "/opt/tools"
        );
        assert_eq!(
            first_agent.1["sandbox_overrides"]["extra_ro_binds"][0]["sandbox_path"],
            "/opt/tools"
        );
    }

    #[tokio::test]
    async fn test_start_project_skips_master_when_disabled() {
        let config = project_config(false);
        let client = recording_client();

        start_project(
            &client,
            config,
            std::path::Path::new("test-ah.toml"),
            std::env::current_dir().unwrap(),
            false,
        )
        .await
        .unwrap();

        let calls = client.calls.lock().unwrap();
        assert!(
            !calls
                .iter()
                .any(|(method, _)| method == "session.spawn_master_pane")
        );
        let first_agent = calls
            .iter()
            .find(|(method, _)| method == "agent.spawn")
            .unwrap();
        assert!(first_agent.1.get("layout_parent_pane_id").is_none());
    }

    #[tokio::test]
    async fn hook_push_enabled_defaults_false_in_agent_spawn_payload() {
        let config = project_config(false);
        let client = recording_client();

        start_project(
            &client,
            config,
            std::path::Path::new("test-ah.toml"),
            std::env::current_dir().unwrap(),
            false,
        )
        .await
        .unwrap();

        let calls = client.calls.lock().unwrap();
        let first_agent = calls
            .iter()
            .find(|(method, _)| method == "agent.spawn")
            .unwrap();
        assert_eq!(first_agent.1["hook_push_enabled"].as_bool(), Some(false));
    }

    #[tokio::test]
    async fn hook_push_enabled_true_flows_to_agent_spawn_payload() {
        let mut config = project_config(false);
        config.completion.hook_push_enabled = true;
        let client = recording_client();

        start_project(
            &client,
            config,
            std::path::Path::new("test-ah.toml"),
            std::env::current_dir().unwrap(),
            false,
        )
        .await
        .unwrap();

        let calls = client.calls.lock().unwrap();
        let first_agent = calls
            .iter()
            .find(|(method, _)| method == "agent.spawn")
            .unwrap();
        assert_eq!(first_agent.1["hook_push_enabled"].as_bool(), Some(true));
    }

    #[tokio::test]
    async fn gemini_provider_alias_flows_to_agent_spawn_as_antigravity() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ah.toml");
        std::fs::write(
            &path,
            r#"
version = "1"

[master]
enabled = false

[agents.a1]
provider = "gemini"
"#,
        )
        .unwrap();
        let config = load_project_config(&path).unwrap();
        let client = recording_client();

        start_project(&client, config, &path, dir.path().to_path_buf(), false)
            .await
            .unwrap();

        let calls = client.calls.lock().unwrap();
        let first_agent = calls
            .iter()
            .find(|(method, _)| method == "agent.spawn")
            .unwrap();
        assert_eq!(first_agent.1["provider"], "antigravity");
    }

    fn project_config(master_enabled: bool) -> ProjectConfig {
        let mut agents = BTreeMap::new();
        for (id, provider) in [
            ("a1", "codex"),
            ("a2", "codex"),
            ("a3", "antigravity"),
            ("a4", "claude"),
        ] {
            agents.insert(
                id.to_string(),
                AgentConfig {
                    provider: provider.to_string(),
                    env: Default::default(),
                    hooks: Default::default(),
                    plugins: Default::default(),
                    skills: Default::default(),
                    bundle: Default::default(),
                    settings: Default::default(),
                },
            );
        }
        ProjectConfig {
            version: "1".to_string(),
            master: MasterConfig {
                cmd: "claude".to_string(),
                cmd_explicit: false,
                provider: None,
                env: Default::default(),
                readiness_timeout_s: 120,
                enabled: master_enabled,
                window_size: Default::default(),
                hooks: Default::default(),
                plugins: Default::default(),
                skills: Default::default(),
                bundle: Default::default(),
                settings: Default::default(),
            },
            completion: Default::default(),
            daemon: Default::default(),
            providers: Default::default(),
            env: Default::default(),
            sandbox: SandboxConfig::default(),
            agents,
        }
    }
}
