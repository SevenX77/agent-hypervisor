use crate::cli::config::{ProjectConfig, find_config, load_project_config};
use crate::cli::rpc_client::{CliError, RpcClient};
use serde_json::{Value, json};
use std::path::PathBuf;

pub struct UpOptions {
    pub config_path: Option<PathBuf>,
    pub cwd: PathBuf,
    pub force: bool,
}

pub async fn run_up<C: RpcClient>(client: &C, options: UpOptions) -> Result<(), CliError> {
    let config_path = match options.config_path {
        Some(path) => path,
        None => find_config(&options.cwd)?,
    };
    let config = load_project_config(&config_path)?;
    crate::cli::config::warn_config_diagnostics(&config);
    let result = run_up_with_config(client, config, options.cwd, options.force).await?;
    println!("{result}");
    Ok(())
}

async fn run_up_with_config<C: RpcClient>(
    client: &C,
    config: ProjectConfig,
    cwd: PathBuf,
    force: bool,
) -> Result<Value, CliError> {
    let session_id = resolve_realign_session_id(client, cwd).await?;
    let result = client
        .call(
            "session.realign",
            json!({
                "session_id": session_id,
                "force": force,
                // ISSUE-13 §3a: carry project [env] so the server merges it into each
                // agent's fingerprint env the same way the spawn side did. Without this the
                // realign side drops config.env (Leak B) and every agent phantom-drifts.
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
                "agents": config.agents.into_iter().map(|(agent_id, agent)| {
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
            }),
        )
        .await?;
    Ok(result)
}

async fn resolve_realign_session_id<C: RpcClient>(
    client: &C,
    cwd: PathBuf,
) -> Result<String, CliError> {
    let listed = client.call("session.list", json!({})).await?;
    let sessions = listed
        .get("sessions")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::InvalidResponse("session.list missing sessions".into()))?;
    let active_sessions = sessions
        .iter()
        .filter(|session| session.get("status").and_then(Value::as_str) == Some("ACTIVE"))
        .collect::<Vec<_>>();

    match active_sessions.len() {
        0 => Err(CliError::Config(
            "no running session; run `ah start` first".into(),
        )),
        1 => session_id_from_summary(active_sessions[0]),
        _ => {
            let cwd = canonical_path_string(cwd);
            let matches = active_sessions
                .iter()
                .filter(|session| {
                    session
                        .get("absolute_path")
                        .and_then(Value::as_str)
                        .is_some_and(|absolute_path| absolute_path == cwd)
                })
                .collect::<Vec<_>>();
            match matches.len() {
                1 => session_id_from_summary(matches[0]),
                0 => Err(CliError::Config(format!(
                    "multiple running sessions; cannot choose one for {cwd}"
                ))),
                _ => Err(CliError::Config(format!(
                    "multiple running sessions match {cwd}; cannot choose one"
                ))),
            }
        }
    }
}

fn session_id_from_summary(session: &Value) -> Result<String, CliError> {
    session
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| CliError::InvalidResponse("session.list session missing id".into()))
}

fn canonical_path_string(path: PathBuf) -> String {
    path.canonicalize().unwrap_or(path).display().to_string()
}

#[cfg(test)]
mod tests {
    use super::run_up_with_config;
    use crate::cli::config::{AgentConfig, MasterConfig, ProjectConfig, SandboxConfig};
    use crate::cli::rpc_client::{CliError, RpcClient, RpcFuture};
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
                self.calls
                    .lock()
                    .unwrap()
                    .push((method.to_string(), params));
                match method {
                    "session.list" => Ok(self.sessions.clone()),
                    "session.realign" => Ok(json!({ "results": [] })),
                    other => Err(CliError::InvalidResponse(format!(
                        "unexpected method in recording client: {other}"
                    ))),
                }
            })
        }
    }

    #[tokio::test]
    async fn test_up_resolves_real_session_id_from_session_list() {
        let client = RecordingClient {
            calls: Mutex::new(Vec::new()),
            sessions: json!({
                "sessions": [{
                    "id": "sess_abc",
                    "project_id": "p",
                    "absolute_path": "/x",
                    "status": "ACTIVE",
                    "active_agents": 0,
                    "created_at": 0
                }]
            }),
        };

        run_up_with_config(&client, project_config(), "/x".into(), true)
            .await
            .unwrap();

        let calls = client.calls.lock().unwrap();
        let realign = calls
            .iter()
            .find(|(method, _)| method == "session.realign")
            .expect("session.realign should be called");
        assert_eq!(realign.1["session_id"], "sess_abc");
    }

    #[tokio::test]
    async fn test_up_errors_when_no_session_running() {
        let client = RecordingClient {
            calls: Mutex::new(Vec::new()),
            sessions: json!({ "sessions": [] }),
        };

        let err = run_up_with_config(&client, project_config(), "/x".into(), true)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("no running session"));
        let calls = client.calls.lock().unwrap();
        assert!(!calls.iter().any(|(method, _)| method == "session.realign"));
    }

    #[tokio::test]
    async fn test_up_ignores_terminal_sessions_when_matching_cwd() {
        let client = RecordingClient {
            calls: Mutex::new(Vec::new()),
            sessions: json!({
                "sessions": [
                    {
                        "id": "sess_failed",
                        "project_id": "p",
                        "absolute_path": "/x",
                        "status": "FAILED",
                        "active_agents": 0,
                        "created_at": 0
                    },
                    {
                        "id": "sess_killed",
                        "project_id": "p",
                        "absolute_path": "/x",
                        "status": "KILLED",
                        "active_agents": 0,
                        "created_at": 1
                    },
                    {
                        "id": "sess_active",
                        "project_id": "p",
                        "absolute_path": "/x",
                        "status": "ACTIVE",
                        "active_agents": 0,
                        "created_at": 2
                    }
                ]
            }),
        };

        run_up_with_config(&client, project_config(), "/x".into(), true)
            .await
            .unwrap();

        let calls = client.calls.lock().unwrap();
        let realign = calls
            .iter()
            .find(|(method, _)| method == "session.realign")
            .expect("session.realign should be called");
        assert_eq!(realign.1["session_id"], "sess_active");
    }

    fn project_config() -> ProjectConfig {
        let mut agents = BTreeMap::new();
        agents.insert(
            "a1".to_string(),
            AgentConfig {
                provider: "bash".to_string(),
                env: Default::default(),
                hooks: Default::default(),
                plugins: Default::default(),
                skills: Default::default(),
                bundle: Default::default(),
                settings: Default::default(),
            },
        );
        ProjectConfig {
            version: "1".to_string(),
            master: MasterConfig {
                cmd: "claude".to_string(),
                cmd_explicit: false,
                provider: None,
                env: Default::default(),
                readiness_timeout_s: 120,
                enabled: true,
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
