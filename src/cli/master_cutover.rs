use crate::cli::config::ProjectConfig;
use crate::cli::output::string_field;
use crate::cli::rpc_client::{CliError, RpcClient};
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct MasterCutoverOptions {
    pub config: ProjectConfig,
    pub project_root: PathBuf,
    pub state_dir: PathBuf,
    pub socket_path: PathBuf,
    pub old_home: PathBuf,
    pub old_master_pid: Option<i64>,
    pub wait: bool,
    pub print_attach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterCutoverSummary {
    pub cutover_id: String,
    pub session_id: String,
    pub pane_id: String,
    pub attach_command: String,
    pub tmux_attach_command: String,
    pub handoff_path: PathBuf,
}

pub async fn run_master_cutover(
    client: &impl RpcClient,
    options: MasterCutoverOptions,
) -> Result<MasterCutoverSummary, CliError> {
    let project_id = options
        .project_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project")
        .to_string();
    tracing::info!(project_id = %project_id, "requesting daemon master cutover orchestration");
    let response = client
        .call(
            "session.master_cutover",
            json!({
                "project_id": project_id,
                "absolute_path": options.project_root.display().to_string(),
                "cwd": options.project_root.display().to_string(),
                "old_home": options.old_home.display().to_string(),
                "old_master_pid": options.old_master_pid,
                "ah_state_dir": options.state_dir.display().to_string(),
                "ah_socket_path": options.socket_path.display().to_string(),
                "wait": options.wait,
                "print_attach": options.print_attach,
                "master": {
                    "cmd": options.config.master.cmd,
                    "provider": options.config.master.resolved_provider(),
                    "env": options.config.master.env,
                    "readiness_timeout_s": options.config.master.readiness_timeout_s,
                    "hooks": options.config.master.hooks,
                    "plugins": options.config.master.plugins,
                    "skills": options.config.master.skills,
                    "bundle": options.config.master.bundle,
                    "settings": options.config.master.settings,
                    "tmux_window_size": options.config.master.window_size,
                    "claude_shared_credentials_dir": options.config.providers.claude.shared_credentials_dir.clone(),
                },
                "agents": options.config.agents.iter().map(|(agent_id, agent)| {
                    let mut merged_env = options.config.env.clone();
                    merged_env.extend(agent.env.clone());
                    let sandbox_overrides = json!({
                        "extra_ro_binds": options.config.sandbox.additional_ro_binds
                            .iter()
                            .map(|path| json!({
                                "host_path": path,
                                "sandbox_path": path,
                            }))
                            .collect::<Vec<_>>(),
                    });
                    json!({
                        "agent_id": agent_id,
                        "provider": agent.provider,
                        "env": merged_env,
                        "hooks": agent.hooks,
                        "plugins": agent.plugins,
                        "skills": agent.skills,
                        "bundle": agent.bundle,
                        "settings": agent.settings,
                        "sandbox_overrides": sandbox_overrides,
                    })
                }).collect::<Vec<_>>(),
            }),
        )
        .await?;
    let session_id = string_field(&response, "session_id");
    if session_id == "-" {
        tracing::warn!("master cutover response missing session_id");
        return Err(CliError::InvalidResponse(
            "session.master_cutover missing session_id".into(),
        ));
    }
    let cutover_id = string_field(&response, "cutover_id");
    let pane_id = string_field(&response, "pane_id");
    if pane_id == "-" {
        tracing::warn!(cutover_id = %cutover_id, session_id = %session_id, "master cutover response missing pane_id");
        return Err(CliError::InvalidResponse(
            "session.master_cutover missing pane_id".into(),
        ));
    }
    tracing::info!(
        cutover_id = %cutover_id,
        session_id = %session_id,
        pane_id = %pane_id,
        "master cutover spawned managed master"
    );

    Ok(MasterCutoverSummary {
        cutover_id,
        session_id: session_id.clone(),
        pane_id,
        attach_command: string_field(&response, "attach_command"),
        tmux_attach_command: string_field(&response, "tmux_attach_command"),
        handoff_path: response
            .get("handoff_path")
            .and_then(|value| value.as_str())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                options
                    .state_dir
                    .join("cutovers")
                    .join("unknown")
                    .join("handoff.md")
            }),
    })
}

pub fn print_master_cutover_summary(summary: &MasterCutoverSummary) {
    println!("session_id={}", summary.session_id);
    println!("pane_id={}", summary.pane_id);
    println!("{}", summary.tmux_attach_command);
    println!("{}", summary.attach_command);
}

#[cfg(test)]
mod tests {
    use super::{MasterCutoverOptions, run_master_cutover};
    use crate::cli::config::{
        AgentConfig, ClaudeProviderConfig, MasterConfig, ProjectConfig, ProviderConfigs,
        SandboxConfig,
    };
    use crate::cli::rpc_client::{CliError, RpcClient, RpcFuture};
    use crate::provider::extensions::HookGroup;
    use serde_json::{Value, json};
    use std::cell::RefCell;
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;

    struct FakeRpcClient {
        calls: RefCell<Vec<(String, Value)>>,
    }

    impl FakeRpcClient {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(String, Value)> {
            self.calls.borrow().clone()
        }
    }

    impl RpcClient for FakeRpcClient {
        fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
            self.calls
                .borrow_mut()
                .push((method.to_string(), params.clone()));
            Box::pin(async move {
                match method {
                    "session.master_cutover" => Ok(json!({
                        "cutover_id": "cutover-sess_cutover",
                        "session_id": "sess_cutover",
                        "pane_id": "%42",
                        "attach_command": "ah attach master --session sess_cutover",
                        "tmux_attach_command": "tmux -S /tmp/ahd.sock attach -t master_ccbd-rust",
                        "handoff_path": "/tmp/state/cutovers/cutover-sess_cutover/handoff.md",
                    })),
                    other => Err(CliError::InvalidResponse(format!(
                        "unexpected fake rpc method: {other}"
                    ))),
                }
            })
        }
    }

    fn config() -> ProjectConfig {
        let mut agents = BTreeMap::new();
        agents.insert(
            "a1".to_string(),
            AgentConfig {
                provider: "bash".to_string(),
                env: HashMap::new(),
                hooks: HashMap::<String, Vec<HookGroup>>::new(),
                plugins: Vec::new(),
                skills: Vec::new(),
                bundle: Vec::new(),
                settings: serde_json::Map::new(),
            },
        );
        ProjectConfig {
            version: "1".to_string(),
            master: MasterConfig::default(),
            completion: Default::default(),
            daemon: Default::default(),
            providers: ProviderConfigs {
                claude: ClaudeProviderConfig {
                    shared_credentials_dir: None,
                },
            },
            env: HashMap::new(),
            sandbox: SandboxConfig::default(),
            agents,
        }
    }

    fn options(tmp: &tempfile::TempDir) -> MasterCutoverOptions {
        let shared_credentials_dir = tmp.path().join("shared-claude-credentials");
        std::fs::create_dir_all(&shared_credentials_dir).unwrap();
        let mut config = config();
        config.providers.claude.shared_credentials_dir = Some(shared_credentials_dir);
        MasterCutoverOptions {
            config,
            project_root: PathBuf::from("/home/sevenx/coding/ccbd-rust"),
            state_dir: tmp.path().join("state"),
            socket_path: tmp.path().join("state").join("ahd.sock"),
            old_home: tmp.path().join("old-home"),
            old_master_pid: Some(12345),
            wait: true,
            print_attach: true,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutover_cli_sends_claude_shared_credentials_dir_in_master_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let client = FakeRpcClient::new();
        let options = options(&tmp);
        let expected = options
            .config
            .providers
            .claude
            .shared_credentials_dir
            .clone()
            .expect("fixture must configure shared credentials dir")
            .display()
            .to_string();

        let _summary = run_master_cutover(&client, options).await.unwrap();

        let calls = client.calls();
        let request = calls
            .iter()
            .find(|(method, _)| method == "session.master_cutover")
            .expect("master cutover call");
        assert_eq!(
            request.1["master"]["claude_shared_credentials_dir"],
            expected
        );
        assert!(
            !request.1["master"]["claude_shared_credentials_dir"]
                .as_str()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutover_cli_merges_top_level_env_into_agent_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let client = FakeRpcClient::new();
        let mut options = options(&tmp);
        options
            .config
            .env
            .insert("TOP_LEVEL_ONLY".to_string(), "from-top".to_string());
        options
            .config
            .env
            .insert("OVERRIDDEN".to_string(), "from-top".to_string());
        options
            .config
            .agents
            .get_mut("a1")
            .unwrap()
            .env
            .insert("OVERRIDDEN".to_string(), "from-agent".to_string());

        let _summary = run_master_cutover(&client, options).await.unwrap();

        let calls = client.calls();
        let request = calls
            .iter()
            .find(|(method, _)| method == "session.master_cutover")
            .expect("master cutover call");
        let agent_env = &request.1["agents"][0]["env"];
        assert_eq!(agent_env["TOP_LEVEL_ONLY"], "from-top");
        assert_eq!(agent_env["OVERRIDDEN"], "from-agent");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutover_cli_passes_additional_ro_binds_to_agent_realign_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let client = FakeRpcClient::new();
        let mut options = options(&tmp);
        options.config.sandbox.additional_ro_binds = vec!["/opt/tools".to_string()];

        let _summary = run_master_cutover(&client, options).await.unwrap();

        let calls = client.calls();
        let request = calls
            .iter()
            .find(|(method, _)| method == "session.master_cutover")
            .expect("master cutover call");
        assert_eq!(
            request.1["agents"][0]["sandbox_overrides"]["extra_ro_binds"][0]["host_path"],
            "/opt/tools"
        );
        assert_eq!(
            request.1["agents"][0]["sandbox_overrides"]["extra_ro_binds"][0]["sandbox_path"],
            "/opt/tools"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutover_cli_sends_single_daemon_master_cutover_request() {
        let tmp = tempfile::tempdir().unwrap();
        let client = FakeRpcClient::new();

        let summary = run_master_cutover(&client, options(&tmp)).await.unwrap();

        let calls = client.calls();
        let methods = calls
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>();
        assert_eq!(methods, vec!["session.master_cutover"]);
        let request = calls
            .iter()
            .find(|(method, _)| method == "session.master_cutover")
            .expect("master cutover call");
        assert_eq!(
            request.1.get("cwd").and_then(Value::as_str),
            Some("/home/sevenx/coding/ccbd-rust")
        );
        assert!(request.1.get("old_home").is_some());
        assert_eq!(
            request.1.get("old_master_pid").and_then(Value::as_i64),
            Some(12345)
        );
        assert!(request.1.get("master").is_some());
        assert!(request.1.get("agents").is_some());
        assert_eq!(summary.pane_id, "%42");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutover_does_not_call_session_kill() {
        let tmp = tempfile::tempdir().unwrap();
        let client = FakeRpcClient::new();

        let _summary = run_master_cutover(&client, options(&tmp)).await.unwrap();

        assert!(
            client
                .calls()
                .iter()
                .all(|(method, _)| method != "session.kill")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cutover_prints_attach_master_command() {
        let tmp = tempfile::tempdir().unwrap();
        let client = FakeRpcClient::new();

        let summary = run_master_cutover(&client, options(&tmp)).await.unwrap();

        assert!(
            summary
                .attach_command
                .contains("ah attach master --session sess_cutover")
        );
        assert!(summary.tmux_attach_command.contains("tmux -S"));
        assert!(summary.tmux_attach_command.contains("master_ccbd-rust"));
    }
}
