use ah::cli::bundle::{
    BundleListOptions, BundleValidateOptions, run_bundle_list, run_bundle_validate,
};
use ah::cli::config_cmd::{migrate_stub, run_config_validate};
use ah::cli::doctor::{has_failures, print_doctor, run_doctor};
use ah::cli::logs::run_logs;
use ah::cli::master_cutover::{
    MasterCutoverOptions, print_master_cutover_summary, run_master_cutover,
};
use ah::cli::output::{
    agent_row, array_len, parse_event_payload, print_terminal_job, print_tmux_hint, session_row,
    string_field,
};
use ah::cli::prompt::{PromptResolveOptions, run_prompt_resolve};
use ah::cli::rpc_client::{
    CliError, RpcClient, UnixRpcClient, exit_code, resolve_socket_path_for_config,
    rpc_stream_first, rpc_stream_lines,
};
use ah::cli::setup::{SetupOptions, run_setup};
use ah::cli::start::{
    StartOptions, ahd_reset_failed_is_best_effort, build_ahd_systemd_run_command_with_parent,
    print_start_summary, should_skip_systemd_bootstrap_for_cgroup, start_from_options,
};
use ah::cli::up::{UpOptions, run_up};
use ah::cli::{
    service_bootstrap::{
        RealSystemctlRunner, bootstrap_persistent_unit, collect_passthrough_env,
        detect_linger_note, gc_stale_units, systemd_user_bootstrap_available,
    },
    service_unit::derive_unit_name,
};
#[cfg(unix)]
use ah::tmux::compute_socket_name;
use ah::tmux::{TmuxPaneId, TmuxServer, agent_session_name, master_session_name};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tabled::Table;

#[derive(Parser)]
#[command(name = "ah", version, about = "Agent Hypervisor CLI")]
struct Cli {
    /// Path to ah.toml.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Internal TCP-to-UDS bridge for Claude gateway sandboxes.
    #[command(hide = true)]
    InternalBridge {
        #[arg(long)]
        uds: PathBuf,
        #[arg(long)]
        port_file: Option<PathBuf>,
    },
    /// Probe the daemon liveness.
    Ping,
    /// Print the CLI version.
    Version,
    /// List sessions, agents, and pending evidence.
    Ps {
        /// Show terminal sessions too.
        #[arg(long)]
        all: bool,
    },
    /// Print a single runtime snapshot projection as JSON and exit.
    Status {
        /// Format output as JSON.
        #[arg(long, default_value_t = true)]
        json: bool,
    },
    /// Start a project from ah.toml.
    Start {
        #[arg(long)]
        wait: bool,
    },
    /// Audit and align running sessions with ah.toml.
    Up {
        #[arg(long)]
        force: bool,
    },
    /// Submit an ask job to an agent.
    Ask {
        agent_id: String,
        text: String,
        #[arg(long)]
        wait: bool,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Asynchronously deliver text to the master pane.
    Tell {
        target: String,
        text: String,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Wait for a submitted job to finish.
    Pend { job_id: String },
    /// Cancel a queued or running job.
    Cancel { job_id: String },
    /// Kill an agent, or a whole session with --session.
    Kill {
        target_id: String,
        #[arg(long)]
        session: bool,
        #[arg(long)]
        force: bool,
    },
    /// Stream agent output events.
    Watch {
        agent_id: String,
        #[arg(long, default_value_t = 0)]
        since_event_id: i64,
    },
    /// Stream runtime lifecycle snapshots as JSON lines.
    Events {
        #[arg(long, default_value = "json")]
        format: String,
    },
    /// Print stored output for an agent.
    Logs {
        agent_id: String,
        #[arg(long, default_value_t = 0)]
        since: i64,
    },
    /// Attach to an agent or master tmux session.
    Attach {
        /// "master", "agent", or a legacy agent id. Legacy ids map to tmux session agent_<agent_id>.
        target: String,
        /// Agent id when using `ah attach agent <agent_id>`.
        subject: Option<String>,
        #[arg(long)]
        session: Option<String>,
    },
    /// Shut down the daemon gracefully.
    Stop,
    /// Manage the ah-managed master process.
    Master {
        #[command(subcommand)]
        cmd: MasterCmd,
    },
    /// Manage agents.
    Agent {
        #[command(subcommand)]
        cmd: AgentCmd,
    },
    /// Run local environment diagnostics.
    Doctor,
    /// Check or prepare ah runtime prerequisites.
    Setup {
        #[arg(long)]
        check: bool,
        #[arg(long)]
        fix: bool,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        resume: bool,
    },
    /// Validate or migrate project configuration.
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Inspect and validate plugin bundles.
    Bundle {
        #[command(subcommand)]
        cmd: BundleCmd,
    },
    /// Resolve an interactive prompt.
    Prompt {
        #[command(subcommand)]
        cmd: PromptCmd,
    },
}

#[derive(Subcommand)]
enum ConfigCmd {
    /// Validate an ah.toml file.
    Validate {
        #[arg(long)]
        config: PathBuf,
    },
    /// Print migration guidance for legacy .ccb/ccb.config.
    Migrate,
}

#[derive(Subcommand)]
enum BundleCmd {
    /// Validate referenced, named, or all local bundles.
    Validate {
        #[arg(long)]
        all: bool,
        names: Vec<String>,
    },
    /// List local bundles and config references.
    List,
}

#[derive(Subcommand)]
enum PromptCmd {
    /// Send an action to a PROMPT_PENDING agent.
    Resolve {
        agent_id: String,
        #[arg(long, conflicts_with = "keys")]
        action: Option<String>,
        #[arg(long, conflicts_with = "action")]
        keys: Option<String>,
        #[arg(long)]
        save_to_kb: bool,
    },
}

#[derive(Subcommand)]
enum MasterCmd {
    /// Cut over the current master into ah-managed master.
    Cutover {
        #[arg(long)]
        wait: bool,
        #[arg(long)]
        print_attach: bool,
    },
    /// Report successor master readiness to ahd.
    AckReady {
        #[arg(long)]
        cutover_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    /// Notify ahd about an agent lifecycle event.
    Notify {
        #[arg(long)]
        agent_id: String,
        #[arg(long)]
        event: String,
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        event_id: Option<String>,
        #[arg(long)]
        hook_json: bool,
        #[arg(long)]
        hook_debug_log: Option<PathBuf>,
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Host-visible outbox dir for the journal-first durable write (R1-T1). Defaults to
        /// `{socket_parent}/outbox/{agent_id}` when omitted.
        #[arg(long)]
        outbox_dir: Option<PathBuf>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    let socket = resolve_socket_path_for_config(cli.config.as_deref());
    let client = UnixRpcClient::new(socket);
    let result = match cli.cmd {
        None => default_action(&client, cli.config).await,
        Some(Cmd::InternalBridge { uds, port_file }) => {
            ah::claude_gateway::run_internal_bridge(&uds, port_file.as_deref())
                .await
                .map_err(CliError::Io)
        }
        Some(Cmd::Ping) => cmd_ping(&client).await,
        Some(Cmd::Version) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some(Cmd::Ps { all }) => cmd_ps(&client, all).await,
        Some(Cmd::Status { json }) => cmd_status(&client, json).await,
        Some(Cmd::Start { wait }) => cmd_start(&client, cli.config, wait).await,
        Some(Cmd::Up { force }) => {
            let cwd = std::env::current_dir().map_err(CliError::Io);
            match cwd {
                Ok(cwd) => {
                    run_up(
                        &client,
                        UpOptions {
                            config_path: cli.config,
                            cwd,
                            force,
                        },
                    )
                    .await
                }
                Err(err) => Err(err),
            }
        }
        Some(Cmd::Ask {
            agent_id,
            text,
            wait,
            request_id,
        }) => cmd_ask(&client, agent_id, text, wait, request_id).await,
        Some(Cmd::Tell {
            target,
            text,
            session,
            request_id,
        }) => cmd_tell(&client, target, text, session, request_id).await,
        Some(Cmd::Pend { job_id }) => cmd_pend(&client, job_id).await,
        Some(Cmd::Cancel { job_id }) => cmd_cancel(&client, job_id).await,
        Some(Cmd::Kill {
            target_id,
            session,
            force,
        }) => cmd_kill(&client, target_id, session, force).await,
        Some(Cmd::Watch {
            agent_id,
            since_event_id,
        }) => cmd_watch(&client, agent_id, since_event_id).await,
        Some(Cmd::Events { format }) => cmd_events(cli.config, format).await,
        Some(Cmd::Logs { agent_id, since }) => run_logs(&client, &agent_id, since).await,
        Some(Cmd::Attach {
            target,
            subject,
            session,
        }) => cmd_attach(&client, &target, subject.as_deref(), session.as_deref()).await,
        Some(Cmd::Stop) => cmd_stop(&client).await,
        Some(Cmd::Master { cmd }) => match cmd {
            MasterCmd::Cutover { wait, print_attach } => {
                cmd_master_cutover(&client, cli.config, wait, print_attach).await
            }
            MasterCmd::AckReady { cutover_id } => cmd_master_ack_ready(&client, cutover_id).await,
        },
        Some(Cmd::Agent { cmd }) => match cmd {
            AgentCmd::Notify {
                agent_id,
                event,
                provider,
                event_id,
                hook_json,
                hook_debug_log,
                socket,
                outbox_dir,
            } => {
                let notify_client = socket
                    .map(UnixRpcClient::new)
                    .unwrap_or_else(|| UnixRpcClient::new(client.socket().to_path_buf()));
                // R1-T1: resolve the journal target — explicit override, else derive from the
                // socket both sides agree on.
                let outbox_dir = outbox_dir.or_else(|| {
                    ah::outbox::default_agent_outbox_dir(notify_client.socket(), &agent_id)
                });
                cmd_agent_notify(
                    &notify_client,
                    agent_id,
                    event,
                    provider,
                    event_id,
                    hook_json,
                    hook_debug_log,
                    outbox_dir,
                )
                .await
            }
        },
        Some(Cmd::Doctor) => cmd_doctor(&client, cli.config.as_deref()).await,
        Some(Cmd::Setup {
            check,
            fix,
            yes,
            json,
            resume,
        }) => cmd_setup(check, fix, yes, json, resume),
        Some(Cmd::Config { cmd }) => match cmd {
            ConfigCmd::Validate { config } => run_config_validate(&config),
            ConfigCmd::Migrate => cmd_config_migrate(),
        },
        Some(Cmd::Bundle { cmd }) => {
            let cwd = std::env::current_dir().map_err(CliError::Io);
            match cwd {
                Ok(cwd) => match cmd {
                    BundleCmd::Validate { all, names } => {
                        run_bundle_validate(BundleValidateOptions {
                            config_path: cli.config,
                            cwd,
                            all,
                            names,
                        })
                    }
                    BundleCmd::List => run_bundle_list(BundleListOptions {
                        config_path: cli.config,
                        cwd,
                    }),
                },
                Err(err) => Err(err),
            }
        }
        Some(Cmd::Prompt { cmd }) => match cmd {
            PromptCmd::Resolve {
                agent_id,
                action,
                keys,
                save_to_kb,
            } => {
                run_prompt_resolve(
                    &client,
                    PromptResolveOptions {
                        agent_id,
                        action_json: action,
                        keys,
                        save_to_kb,
                    },
                )
                .await
            }
        },
    };

    if let Err(err) = result {
        let code = exit_code(&err);
        eprintln!("\x1b[31m{err}\x1b[0m");
        if matches!(
            err,
            CliError::DaemonNotRunning(_) | CliError::DaemonNotAccepting(_, _)
        ) {
            eprintln!("Start it with: ah start");
        }
        std::process::exit(code);
    }
}

fn cmd_setup(check: bool, fix: bool, yes: bool, json: bool, resume: bool) -> Result<(), CliError> {
    let run = run_setup(SetupOptions {
        check,
        fix,
        yes,
        json,
        resume,
    })
    .map_err(|err| CliError::Config(format!("failed to render setup output: {err}")))?;
    print!("{}", run.output);
    if run.exit_code != 0 {
        std::process::exit(run.exit_code);
    }
    Ok(())
}

async fn default_action(client: &UnixRpcClient, config: Option<PathBuf>) -> Result<(), CliError> {
    check_nested_environment()?;
    ensure_daemon_running(client.socket())?;
    let cwd = std::env::current_dir()?;
    let summary = start_from_options(
        client,
        StartOptions {
            config_path: config,
            cwd,
            wait: true,
        },
    )
    .await?;
    print_start_summary(&summary);
    println!("Session ready. Attach via: ah attach <agent_id>");
    Ok(())
}

async fn cmd_master_cutover(
    client: &UnixRpcClient,
    config_path: Option<PathBuf>,
    wait: bool,
    print_attach: bool,
) -> Result<(), CliError> {
    ensure_daemon_running(client.socket())?;
    let cwd = std::env::current_dir()?;
    let config_path = match config_path {
        Some(path) => path,
        None => ah::cli::config::find_config(&cwd)?,
    };
    let config = ah::cli::config::load_project_config(&config_path)?;
    let state_dir = client
        .socket()
        .parent()
        .ok_or_else(|| CliError::Config("daemon socket has no parent state dir".into()))?
        .to_path_buf();
    let summary = run_master_cutover(
        client,
        MasterCutoverOptions {
            config,
            project_root: cwd,
            state_dir,
            socket_path: client.socket().to_path_buf(),
            old_home: std::env::var_os("HOME")
                .map(PathBuf::from)
                .ok_or_else(|| CliError::Config("HOME is not set for master cutover".into()))?,
            old_master_pid: Some(i64::from(std::process::id())),
            wait,
            print_attach,
        },
    )
    .await?;
    print_master_cutover_summary(&summary);
    Ok(())
}

async fn cmd_master_ack_ready(
    client: &UnixRpcClient,
    cutover_id: Option<String>,
) -> Result<(), CliError> {
    let cutover_id = match cutover_id {
        Some(id) => id,
        None => std::env::var("AH_CUTOVER_ID").map_err(|_| {
            CliError::Config("missing --cutover-id and AH_CUTOVER_ID is not set".into())
        })?,
    };
    let result = client
        .call(
            "master.ack_ready",
            json!({
                "cutover_id": cutover_id,
                "pid": i64::from(std::process::id()),
                "observed_socket": client.socket().display().to_string(),
            }),
        )
        .await?;
    println!("cutover_id={}", string_field(&result, "cutover_id"));
    println!("readiness_mode={}", string_field(&result, "readiness_mode"));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_agent_notify(
    client: &UnixRpcClient,
    agent_id: String,
    event: String,
    provider: Option<String>,
    event_id: Option<String>,
    hook_json: bool,
    hook_debug_log: Option<PathBuf>,
    outbox_dir: Option<PathBuf>,
) -> Result<(), CliError> {
    // R1-T1 / CP-R1.1 — journal-first: make the report durable in the outbox BEFORE any RPC.
    // Invariant: exit 0 ⇔ a durable outbox record exists. A journal failure is loud + non-zero;
    // an RPC failure (below) is exit-0-safe precisely because the durable file is the guarantee.
    let event_id = event_id.unwrap_or_else(ah::outbox::new_event_id);
    let journaled = if let Some(outbox_dir) = outbox_dir.as_deref() {
        let record = ah::outbox::OutboxRecord {
            event_id: event_id.clone(),
            kind: ah::outbox::OutboxKind::HookEvent,
            agent_id: agent_id.clone(),
            provider: provider.clone(),
            event: Some(event.clone()),
            attempt_cookie: std::env::var("AH_JOB_ATTEMPT_COOKIE").ok(),
            job_id: std::env::var("AH_JOB_ID").ok(),
            reply_text: None,
            reason: None,
            hook_fired_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|d| d.as_secs() as i64),
            payload: None,
        };
        if let Err(err) = ah::outbox::journal_record(outbox_dir, &record) {
            // Nothing durable landed → loud, non-zero exit. NEVER a silent exit-0.
            let message = format!(
                "outbox journal failed for agent {agent_id} at {}: {err}",
                outbox_dir.display()
            );
            append_hook_debug_log(
                hook_debug_log.as_deref(),
                &agent_id,
                &event,
                provider.as_deref(),
                1,
                "",
                &message,
            );
            return Err(CliError::Io(std::io::Error::new(err.kind(), message)));
        }
        true
    } else {
        false
    };

    let mut params = json!({
        "agent_id": agent_id.clone(),
        "event": event.clone(),
        "event_id": event_id,
    });
    if let Some(provider) = provider.as_ref() {
        params["provider"] = Value::String(provider.clone());
    }
    match client.call("agent.notify", params).await {
        Ok(result) => {
            let output = format_agent_notify_output(&result, hook_json);
            append_hook_debug_log(
                hook_debug_log.as_deref(),
                &agent_id,
                &event,
                provider.as_deref(),
                0,
                &output,
                "",
            );
            print!("{output}");
            Ok(())
        }
        Err(err) if journaled => {
            // RPC is a demoted fast-path optimization; durability was already achieved at the
            // rename(). ahd will pick the record up via cold-scan on its next start (R1-T2).
            // Exit 0 with the allow-stop output so the harness turn ends cleanly.
            let synthetic = json!({
                "agent_id": agent_id,
                "event": event,
                "transitioned": false,
            });
            let output = format_agent_notify_output(&synthetic, hook_json);
            append_hook_debug_log(
                hook_debug_log.as_deref(),
                &agent_id,
                &event,
                provider.as_deref(),
                0,
                &output,
                &format!("rpc unavailable, record journaled durably: {err}"),
            );
            print!("{output}");
            Ok(())
        }
        Err(err) => {
            // Not journaled (no outbox dir resolvable): preserve the legacy non-zero exit.
            append_hook_debug_log(
                hook_debug_log.as_deref(),
                &agent_id,
                &event,
                provider.as_deref(),
                1,
                "",
                &err.to_string(),
            );
            Err(err)
        }
    }
}

fn format_agent_notify_output(result: &Value, hook_json: bool) -> String {
    if hook_json {
        "{}\n".to_string()
    } else {
        format!(
            "agent_id={}\nevent={}\ntransitioned={}\n",
            string_field(result, "agent_id"),
            string_field(result, "event"),
            result
                .get("transitioned")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        )
    }
}

fn append_hook_debug_log(
    path: Option<&Path>,
    agent_id: &str,
    event: &str,
    provider: Option<&str>,
    exit_code: i32,
    stdout: &str,
    stderr: &str,
) {
    let Some(path) = path else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    else {
        return;
    };
    let ts_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let _ = writeln!(file, "timestamp_unix={ts_unix}");
    let _ = writeln!(file, "agent_id={agent_id}");
    let _ = writeln!(file, "event={event}");
    let _ = writeln!(file, "provider={}", provider.unwrap_or(""));
    let _ = writeln!(file, "argv=ah agent notify");
    let _ = writeln!(file, "exit={exit_code}");
    let _ = writeln!(file, "stdout<<EOF\n{stdout}EOF");
    let _ = writeln!(file, "stderr<<EOF\n{stderr}EOF");
}

fn ensure_daemon_running(socket: &Path) -> Result<(), CliError> {
    if socket.exists() {
        if daemon_socket_accepts(socket) {
            return Ok(());
        }
        eprintln!("Removing stale socket {}", socket.display());
        let _ = std::fs::remove_file(socket);
    }

    if let Some(message) = ah::cli::wsl::start_preflight_error() {
        return Err(CliError::Config(message));
    }

    let ahd_bin = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|dir| dir.join("ahd")))
        .filter(|p| p.is_file())
        .ok_or_else(|| CliError::Config("cannot locate ahd binary next to ah".to_string()))?;

    let state_dir = socket.parent().unwrap();
    std::fs::create_dir_all(state_dir).map_err(|e| {
        CliError::Config(format!(
            "failed to create state dir {}: {e}",
            state_dir.display()
        ))
    })?;

    if std::env::var("CCB_ENV").as_deref() == Ok("dev") {
        for ext in ["", "-wal", "-shm"] {
            let p = state_dir.join(format!("ahd.sqlite{ext}"));
            let _ = std::fs::remove_file(&p);
        }
    }

    let log_path = state_dir.join("ahd.log");
    let log_file = std::fs::File::create(&log_path).map_err(|e| {
        CliError::Config(format!("failed to create log {}: {e}", log_path.display()))
    })?;

    eprintln!("Starting ahd daemon (log: {})...", log_path.display());
    let unit_name = derive_unit_name(state_dir);
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    if !systemd_user_bootstrap_available()
        || should_skip_systemd_bootstrap_for_cgroup(&cgroup, &unit_name)
    {
        spawn_ahd_direct(&ahd_bin, state_dir, &log_file)?;
    } else {
        let runner = RealSystemctlRunner;
        let env = collect_passthrough_env();
        match bootstrap_persistent_unit(&runner, &ahd_bin, state_dir, &env, false) {
            Ok(unit_name) => {
                eprintln!("ahd unit: {unit_name} (systemctl --user status {unit_name})");
                gc_stale_units(&runner);
                if let Some(note) = detect_linger_note() {
                    eprintln!("{note}");
                }
            }
            Err(err) if err.is_recoverable() => {
                tracing::warn!(
                    error = %err,
                    "persistent ahd systemd unit bootstrap failed; falling back to transient systemd-run"
                );
                run_transient_systemd_bootstrap(&ahd_bin, state_dir, &log_file)?;
            }
            Err(err) => {
                return Err(err.into_cli_error());
            }
        }
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if socket.exists() && daemon_socket_accepts(socket) {
            eprintln!("ahd daemon ready.");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(CliError::Config(format!(
        "ahd failed to start within 10s (socket {} not accepting connections)",
        socket.display()
    )))
}

#[cfg(unix)]
fn daemon_socket_accepts(socket: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(socket).is_ok()
}

#[cfg(windows)]
fn daemon_socket_accepts(_socket: &Path) -> bool {
    false
}

fn run_transient_systemd_bootstrap(
    ahd_bin: &Path,
    state_dir: &Path,
    log_file: &std::fs::File,
) -> Result<(), CliError> {
    ahd_reset_failed_is_best_effort("ahd.service");
    let parent_scope = ah::systemd_unit::detect_current_scope_or_service();
    let cmd =
        build_ahd_systemd_run_command_with_parent(ahd_bin, state_dir, &[], parent_scope.as_deref());
    let (program, args) = cmd
        .split_first()
        .ok_or_else(|| CliError::Config("failed to build ahd systemd bootstrap command".into()))?;
    match Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file.try_clone().unwrap())
        .env_remove("INVOCATION_ID")
        .status()
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            tracing::warn!(
                status = ?status,
                "systemd-run ahd bootstrap failed; falling back to direct ahd spawn"
            );
            spawn_ahd_direct(ahd_bin, state_dir, log_file)
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "systemd-run ahd bootstrap failed to execute; falling back to direct ahd spawn"
            );
            spawn_ahd_direct(ahd_bin, state_dir, log_file)
        }
    }
}

fn spawn_ahd_direct(
    ahd_bin: &Path,
    state_dir: &Path,
    log_file: &std::fs::File,
) -> Result<(), CliError> {
    Command::new(ahd_bin)
        .stdin(std::process::Stdio::null())
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file.try_clone().unwrap())
        .env_remove("INVOCATION_ID")
        .env("AH_STATE_DIR", state_dir)
        .spawn()
        .map_err(|e| CliError::Config(format!("failed to spawn ahd: {e}")))?;
    Ok(())
}

fn check_nested_environment() -> Result<(), CliError> {
    let tmux_env = std::env::var("TMUX").ok();
    let cgroup_data = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    if let Some(reason) = detect_nesting(tmux_env.as_deref(), &cgroup_data) {
        return Err(CliError::Config(format!(
            "Agent Nesting Forbidden: 当前已在 ahd 环境内, 不能再启动 ah ({reason})"
        )));
    }
    Ok(())
}

fn detect_nesting(tmux_env: Option<&str>, cgroup_data: &str) -> Option<String> {
    if let Some(tmux_env) = tmux_env
        && (tmux_env.contains("/ahd-") || tmux_env.starts_with("ahd-"))
    {
        return Some("via TMUX env".to_string());
    }
    if cgroup_data.contains("/ccb-") || cgroup_data.contains("ahd-agent-") {
        return Some("via cgroup".to_string());
    }
    None
}

fn attach_session_name(agent_id: &str) -> String {
    agent_session_name(agent_id)
}

fn resolve_attach_session_name(
    target: &str,
    subject: Option<&str>,
    session_id: Option<&str>,
    sessions: Option<&Value>,
) -> Result<String, CliError> {
    match target {
        "master" => {
            if subject.is_some() {
                return Err(CliError::Config(
                    "usage: ah attach master [--session <session_id>]".into(),
                ));
            }
            resolve_master_attach_session_name(sessions, session_id)
        }
        "agent" => {
            let agent_id = subject
                .filter(|value| !value.is_empty())
                .ok_or_else(|| CliError::Config("usage: ah attach agent <agent_id>".into()))?;
            Ok(agent_session_name(agent_id))
        }
        _ => Ok(attach_session_name(target)),
    }
}

fn resolve_master_attach_session_name(
    sessions: Option<&Value>,
    session_id: Option<&str>,
) -> Result<String, CliError> {
    let session = resolve_master_session(sessions, session_id)?;
    let project_id = session
        .get("project_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CliError::InvalidResponse("session.list session missing project_id".into())
        })?;
    Ok(master_session_name(project_id))
}

fn resolve_master_session<'a>(
    sessions: Option<&'a Value>,
    session_id: Option<&str>,
) -> Result<&'a Value, CliError> {
    let sessions = sessions
        .and_then(|value| value.get("sessions"))
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::InvalidResponse("session.list missing sessions".into()))?;
    let candidates = sessions
        .iter()
        .filter(|session| {
            if let Some(expected_id) = session_id {
                session.get("id").and_then(Value::as_str) == Some(expected_id)
            } else {
                session.get("status").and_then(Value::as_str) == Some("ACTIVE")
            }
        })
        .collect::<Vec<_>>();
    let session = match candidates.len() {
        0 if session_id.is_some() => {
            return Err(CliError::Config(format!(
                "session not found: {}",
                session_id.unwrap()
            )));
        }
        0 => {
            return Err(CliError::Config(
                "no active session with a master pane; run `ah start` first".into(),
            ));
        }
        1 => candidates[0],
        _ => {
            return Err(CliError::Config(
                "multiple active sessions; pass --session <session_id>".into(),
            ));
        }
    };
    let master_pane_id = session
        .get("master_pane_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    if master_pane_id.is_none() {
        let id = session
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        return Err(CliError::Config(format!(
            "session {id} has no master pane; run `ah start` with master enabled first"
        )));
    }
    Ok(session)
}

fn resolve_master_tell_target<'a>(
    sessions: &'a Value,
    session_id: Option<&str>,
) -> Result<(&'a str, TmuxPaneId), CliError> {
    let session = resolve_master_session(Some(sessions), session_id)?;
    let session_id = session
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::InvalidResponse("session.list session missing id".into()))?;
    let pane_id = session
        .get("master_pane_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CliError::InvalidResponse("session.list session missing master_pane_id".into())
        })?;
    let pane = TmuxPaneId::parse(pane_id)
        .map_err(|err| CliError::Config(format!("stored master_pane_id is invalid: {err}")))?;
    Ok((session_id, pane))
}

fn generated_tell_request_id() -> String {
    format!(
        "tell_{}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default(),
        uuid::Uuid::new_v4().simple()
    )
}

const PASTE_EXPAND_GUARDS: &[&str] = &["paste again to expand"];

fn bottom_composer_region(capture: &str) -> String {
    let mut lines = capture.lines().rev().take(8).collect::<Vec<_>>();
    lines.reverse();
    lines.join("\n").to_ascii_lowercase()
}

fn contains_paste_expand_guard(capture: &str) -> bool {
    let bottom = bottom_composer_region(capture);
    PASTE_EXPAND_GUARDS
        .iter()
        .any(|phrase| bottom.contains(phrase))
}

fn bottom_contains_tell_body(capture: &str, text: &str) -> bool {
    let needle = text.trim();
    !needle.is_empty() && bottom_composer_region(capture).contains(&needle.to_ascii_lowercase())
}

async fn report_master_tell_failed(
    client: &UnixRpcClient,
    session_id: &str,
    request_id: &str,
    pane_id: &str,
    stage: &str,
    reason: &str,
) {
    let _ = client
        .call(
            "master.tell_failed",
            json!({
                "session_id": session_id,
                "request_id": request_id,
                "pane_id": pane_id,
                "stage": stage,
                "reason": reason,
            }),
        )
        .await;
}

async fn cmd_tell(
    client: &UnixRpcClient,
    target: String,
    text: String,
    session: Option<String>,
    request_id: Option<String>,
) -> Result<(), CliError> {
    if target != "master" {
        return Err(CliError::Config(
            "usage: ah tell master <text> [--session <session_id>] [--request-id <id>]".into(),
        ));
    }
    let sessions = client.call("session.list", json!({})).await?;
    let (session_id, pane) = resolve_master_tell_target(&sessions, session.as_deref())?;
    let request_id = request_id.unwrap_or_else(generated_tell_request_id);
    client
        .call(
            "master.tell_begin",
            json!({
                "session_id": session_id,
                "request_id": request_id,
                "pane_id": pane.0,
            }),
        )
        .await?;

    let state_dir = client
        .socket()
        .parent()
        .ok_or_else(|| CliError::Config("daemon socket has no parent directory".into()))?;
    let tmux = TmuxServer::new(state_dir);
    let buffer_name = format!("ah-tell-{}", request_id.replace([':', '/', '.'], "_"));
    let pane_id = pane.0.clone();

    if let Err(err) = tmux.load_buffer(buffer_name.clone(), text.clone()).await {
        report_master_tell_failed(
            client,
            session_id,
            &request_id,
            &pane_id,
            "LOAD_BUFFER",
            &err.to_string(),
        )
        .await;
        return Err(CliError::Config(format!(
            "DELIVERY_FAILED request_id={request_id} stage=LOAD_BUFFER reason={err}"
        )));
    }
    let paste_result = tmux.paste_buffer(pane.clone(), buffer_name.clone()).await;
    let _ = tmux.delete_buffer(buffer_name).await;
    if let Err(err) = paste_result {
        report_master_tell_failed(
            client,
            session_id,
            &request_id,
            &pane_id,
            "PASTE",
            &err.to_string(),
        )
        .await;
        return Err(CliError::Config(format!(
            "DELIVERY_FAILED request_id={request_id} stage=PASTE reason={err}"
        )));
    }
    if let Err(err) = tmux
        .send_keys_keysym(pane.clone(), "Enter".to_string())
        .await
    {
        report_master_tell_failed(
            client,
            session_id,
            &request_id,
            &pane_id,
            "PASTE_ENTER",
            &err.to_string(),
        )
        .await;
        return Err(CliError::Config(format!(
            "DELIVERY_FAILED request_id={request_id} stage=PASTE_ENTER reason={err}"
        )));
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    match tmux.capture_pane(pane.clone()).await {
        Ok(capture) if contains_paste_expand_guard(&capture) => {
            if let Err(err) = tmux
                .send_keys_keysym(pane.clone(), "Enter".to_string())
                .await
            {
                report_master_tell_failed(
                    client,
                    session_id,
                    &request_id,
                    &pane_id,
                    "SEND_ENTER_TO_EXPAND",
                    &err.to_string(),
                )
                .await;
                return Err(CliError::Config(format!(
                    "DELIVERY_FAILED request_id={request_id} stage=SEND_ENTER_TO_EXPAND reason={err}"
                )));
            }
        }
        Ok(_) => {}
        Err(err) => {
            report_master_tell_failed(
                client,
                session_id,
                &request_id,
                &pane_id,
                "DETECT_EXPAND_PROMPT",
                &err.to_string(),
            )
            .await;
            return Err(CliError::Config(format!(
                "DELIVERY_FAILED request_id={request_id} stage=DETECT_EXPAND_PROMPT reason={err}"
            )));
        }
    }

    let mut last_reason = "composer_not_cleared".to_string();
    for _ in 0..8 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        match tmux.capture_pane(pane.clone()).await {
            Ok(capture) => {
                if contains_paste_expand_guard(&capture) {
                    last_reason = "paste_expand_prompt_still_visible".to_string();
                    continue;
                }
                if bottom_contains_tell_body(&capture, &text) {
                    last_reason = "tell_body_still_visible_in_composer".to_string();
                    continue;
                }
                println!(
                    "delivered request_id={request_id}; waiting for master UserPromptSubmit/Stop hooks is observable via ah ps/logs"
                );
                return Ok(());
            }
            Err(err) => {
                last_reason = err.to_string();
                break;
            }
        }
    }

    report_master_tell_failed(
        client,
        session_id,
        &request_id,
        &pane_id,
        "VERIFY_PANE_CLEARED",
        &last_reason,
    )
    .await;
    Err(CliError::Config(format!(
        "DELIVERY_FAILED_UNCONFIRMED request_id={request_id} stage=VERIFY_PANE_CLEARED reason={last_reason}"
    )))
}

async fn cmd_attach(
    client: &UnixRpcClient,
    target: &str,
    subject: Option<&str>,
    session_id: Option<&str>,
) -> Result<(), CliError> {
    let socket = tmux_socket_path_from_daemon_socket(client.socket())?;
    if !socket.exists() {
        return Err(CliError::Config(format!(
            "tmux socket not found: {}. Is a session running?",
            socket.display()
        )));
    }
    let sessions = if target == "master" {
        Some(client.call("session.list", json!({})).await?)
    } else {
        None
    };
    let session_name = resolve_attach_session_name(target, subject, session_id, sessions.as_ref())?;
    exec_tmux_attach(socket, session_name)
}

async fn cmd_stop(client: &UnixRpcClient) -> Result<(), CliError> {
    client.call("system.shutdown", json!({})).await?;
    // The daemon is down; now remove what started it. An enabled unit outlives
    // the process and relaunches this stack at the next login, so leaving it
    // behind would make `ah stop` a pause rather than a teardown (#24).
    if let Some(state_dir) = client.socket().parent() {
        crate::teardown_ahd_unit(state_dir);
    }
    eprintln!("ccbd shutting down.");
    Ok(())
}

fn teardown_ahd_unit(state_dir: &std::path::Path) {
    if !systemd_user_bootstrap_available() {
        return;
    }
    ah::cli::service_bootstrap::teardown_persistent_unit(&RealSystemctlRunner, state_dir);
}

async fn cmd_ping(client: &UnixRpcClient) -> Result<(), CliError> {
    let result = client.call("system.dump", json!({})).await?;
    let sessions = array_len(&result, "sessions");
    let agents = array_len(&result, "agents");

    println!("ok=true socket={}", client.socket().display());
    println!("sessions={sessions} agents={agents}");
    Ok(())
}

async fn cmd_status(client: &impl RpcClient, json: bool) -> Result<(), CliError> {
    let result = status_snapshot_json(client, json).await?;
    println!("{result}");
    Ok(())
}

async fn status_snapshot_json(client: &impl RpcClient, _json: bool) -> Result<String, CliError> {
    let result = client.call("runtime.snapshot", json!({})).await?;
    Ok(serde_json::to_string_pretty(&result)?)
}

async fn cmd_ps(client: &UnixRpcClient, all: bool) -> Result<(), CliError> {
    let sessions = client.call("session.list", json!({"all": all})).await?;
    let session_rows = sessions
        .get("sessions")
        .and_then(Value::as_array)
        .map(|sessions| sessions.iter().map(session_row).collect::<Vec<_>>())
        .unwrap_or_default();
    println!("sessions");
    println!("{}", Table::new(session_rows));

    let dump = client.call("system.dump", json!({})).await?;
    let rows = dump
        .get("agents")
        .and_then(Value::as_array)
        .map(|agents| agents.iter().map(agent_row).collect::<Vec<_>>())
        .unwrap_or_default();
    println!();
    println!("agents");
    println!("{}", Table::new(rows));
    print_tmux_hint(client.socket())
}

async fn cmd_doctor(client: &UnixRpcClient, config_path: Option<&Path>) -> Result<(), CliError> {
    let project_dir = config_path.and_then(|path| path.parent());
    let checks = run_doctor(client, project_dir).await?;
    print_doctor(&checks);
    if has_failures(&checks) {
        Err(CliError::Config("doctor found failed checks".into()))
    } else {
        Ok(())
    }
}

fn cmd_config_migrate() -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    migrate_stub(&cwd)
}

async fn cmd_start(
    client: &UnixRpcClient,
    config: Option<PathBuf>,
    wait: bool,
) -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    let config_path = resolve_start_config_path(config, &cwd)?;
    ensure_daemon_running(client.socket())?;
    let summary = start_from_options(
        client,
        StartOptions {
            config_path: Some(config_path),
            cwd,
            wait,
        },
    )
    .await?;
    print_start_summary(&summary);
    Ok(())
}

fn resolve_start_config_path(config: Option<PathBuf>, cwd: &Path) -> Result<PathBuf, CliError> {
    let config_path = match config {
        Some(path) => path,
        None => ah::cli::config::find_config(cwd)?,
    };
    let _config = ah::cli::config::load_project_config(&config_path)?;
    Ok(config_path)
}

async fn cmd_ask(
    client: &UnixRpcClient,
    agent_id: String,
    text: String,
    wait: bool,
    request_id: Option<String>,
) -> Result<(), CliError> {
    let mut params = json!({
        "agent_id": agent_id,
        "text": text,
    });
    if let Some(request_id) = request_id {
        params["request_id"] = Value::String(request_id);
    }
    let result = client.call("job.submit", params).await?;
    let job_id = string_field(&result, "job_id");
    let status = string_field(&result, "status");
    println!("job_id={job_id} status={status}");
    if wait {
        print_terminal_job(wait_for_job(client, &job_id).await?)?;
    }
    Ok(())
}

async fn cmd_pend(client: &UnixRpcClient, job_id: String) -> Result<(), CliError> {
    print_terminal_job(wait_for_job(client, &job_id).await?)?;
    Ok(())
}

async fn cmd_cancel(client: &UnixRpcClient, job_id: String) -> Result<(), CliError> {
    let result = client
        .call(
            "job.cancel",
            json!({
                "job_id": job_id,
            }),
        )
        .await?;
    let job_id = string_field(&result, "job_id");
    let status = string_field(&result, "status");
    println!("job_id={job_id} status={status}");
    Ok(())
}

async fn cmd_kill(
    client: &UnixRpcClient,
    target_id: String,
    session: bool,
    force: bool,
) -> Result<(), CliError> {
    let (method, params) = if session {
        (
            "session.kill",
            json!({
                "session_id": target_id,
                "force": force,
            }),
        )
    } else {
        (
            "agent.kill",
            json!({
                "agent_id": target_id,
            }),
        )
    };
    let result = client.call(method, params).await?;
    println!("state={}", string_field(&result, "state"));
    Ok(())
}

async fn cmd_watch(
    client: &UnixRpcClient,
    agent_id: String,
    mut since_event_id: i64,
) -> Result<(), CliError> {
    loop {
        let result = client
            .call(
                "agent.watch",
                json!({
                    "agent_id": agent_id,
                    "since_event_id": since_event_id,
                    "timeout": 30,
                }),
            )
            .await?;
        let Some(events) = result.get("events").and_then(Value::as_array) else {
            return Err(CliError::InvalidResponse(
                "agent.watch missing events array".into(),
            ));
        };
        for event in events {
            if let Some(seq_id) = event.get("seq_id").and_then(Value::as_i64) {
                since_event_id = since_event_id.max(seq_id);
            }
            match event.get("event_type").and_then(Value::as_str) {
                Some("output_chunk") => {
                    let payload = parse_event_payload(event)?;
                    if let Some(text) = payload.get("text").and_then(Value::as_str) {
                        print!("{text}");
                        std::io::stdout().flush()?;
                    }
                }
                Some("state_change") => {
                    let payload = parse_event_payload(event)?;
                    println!("\n--- state_change {} ---", payload);
                }
                _ => {}
            }
        }
    }
}

async fn cmd_events(config: Option<PathBuf>, format: String) -> Result<(), CliError> {
    if format != "json" {
        return Err(CliError::Config(
            "events currently supports only --format json".to_string(),
        ));
    }

    let cwd = std::env::current_dir()?;
    let config_path = match config {
        Some(path) => absolutize_path(&cwd, path),
        None => ah::cli::config::find_config(&cwd)?,
    };
    let workspace_path = config_path
        .parent()
        .map(|path| path.display().to_string())
        .ok_or_else(|| {
            CliError::Config(format!(
                "config path has no parent directory: {}",
                config_path.display()
            ))
        })?;
    let socket = resolve_socket_path_for_config(Some(&config_path));
    let state_dir = socket.parent().map(|path| path.display().to_string());
    let mut sequence = 1_u64;
    let mut last_local_fingerprint = None::<String>;

    loop {
        let params = runtime_subscribe_params(&config_path);
        let mut streamed_this_connection = false;
        match rpc_stream_lines(&socket, "runtime.subscribe", params, |line| {
            streamed_this_connection = true;
            println!("{line}");
            std::io::stdout().flush()?;
            Ok(())
        }) {
            Ok(()) => {
                // The daemon closed the stream — an `ah stop` or a daemon
                // restart, both normal lifecycle events for a long-lived
                // subscriber. Emit a local inactive snapshot so consumers see
                // the runtime go down, then keep reconnecting instead of
                // exiting (a GUI supervisor would otherwise freeze on the
                // last active snapshot). Reset the local fingerprint: the
                // last LOCAL snapshot predates the connection, and matching
                // it would dedup away this down-edge.
                if streamed_this_connection {
                    last_local_fingerprint = None;
                }
                let snapshot = ah::runtime_events::inactive_runtime_snapshot(
                    ah::runtime_events::RuntimeInactiveInput {
                        reason: ah::runtime_events::RuntimeSnapshotReason::DaemonLost,
                        config_path: Some(config_path.display().to_string()),
                        workspace_path: Some(workspace_path.clone()),
                        state_dir: state_dir.clone(),
                        sequence,
                    },
                );
                print_local_runtime_snapshot_if_changed(&snapshot, &mut last_local_fingerprint)?;
                sequence = sequence.saturating_add(1);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(CliError::DaemonNotRunning(_)) | Err(CliError::DaemonNotAccepting(_, _)) => {
                let snapshot = ah::runtime_events::inactive_runtime_snapshot(
                    ah::runtime_events::RuntimeInactiveInput {
                        reason: ah::runtime_events::RuntimeSnapshotReason::DaemonAbsent,
                        config_path: Some(config_path.display().to_string()),
                        workspace_path: Some(workspace_path.clone()),
                        state_dir: state_dir.clone(),
                        sequence,
                    },
                );
                print_local_runtime_snapshot_if_changed(&snapshot, &mut last_local_fingerprint)?;
                sequence = sequence.saturating_add(1);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(CliError::Io(_)) => {
                if streamed_this_connection {
                    last_local_fingerprint = None;
                }
                let snapshot = ah::runtime_events::inactive_runtime_snapshot(
                    ah::runtime_events::RuntimeInactiveInput {
                        reason: ah::runtime_events::RuntimeSnapshotReason::DaemonLost,
                        config_path: Some(config_path.display().to_string()),
                        workspace_path: Some(workspace_path.clone()),
                        state_dir: state_dir.clone(),
                        sequence,
                    },
                );
                print_local_runtime_snapshot_if_changed(&snapshot, &mut last_local_fingerprint)?;
                sequence = sequence.saturating_add(1);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// The daemon's state dir is already derived from the config path, so the
/// subscription must NOT filter sessions by workspace path: sessions record
/// the project's absolute path (`ah start` cwd), while the config may live
/// elsewhere entirely (Studio keeps transient configs under the OS temp dir).
/// Sending the config's parent as `workspace_path` made the inventory filter
/// match nothing, so every snapshot reported an inactive runtime.
fn runtime_subscribe_params(config_path: &Path) -> Value {
    json!({
        "config_path": config_path.display().to_string(),
    })
}

fn absolutize_path(cwd: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

fn print_local_runtime_snapshot_if_changed(
    snapshot: &ah::runtime_events::RuntimeSnapshot,
    last_fingerprint: &mut Option<String>,
) -> Result<(), CliError> {
    let fingerprint = ah::runtime_events::runtime_snapshot_fingerprint(snapshot)
        .map_err(|err| CliError::InvalidResponse(err.to_string()))?;
    if last_fingerprint.as_deref() == Some(fingerprint.as_str()) {
        return Ok(());
    }
    let line = serde_json::to_string(snapshot)?;
    println!("{line}");
    std::io::stdout().flush()?;
    *last_fingerprint = Some(fingerprint);
    Ok(())
}

async fn wait_for_job(client: &UnixRpcClient, job_id: &str) -> Result<Value, CliError> {
    let frame = rpc_stream_first(
        client.socket(),
        "event.subscribe",
        json!({
            "job_id": job_id,
            "event_kind": ["job_state_change"],
        }),
    )?;
    let state = frame
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::InvalidResponse("event frame missing state".into()))?;
    if !matches!(state, "COMPLETED" | "FAILED" | "CANCELLED" | "KILLED") {
        return Err(CliError::InvalidResponse(format!(
            "non-terminal event frame state={state}"
        )));
    }
    frame
        .get("payload")
        .cloned()
        .ok_or_else(|| CliError::InvalidResponse("event frame missing payload".into()))
}

fn tmux_socket_path_from_daemon_socket(socket: &Path) -> Result<PathBuf, CliError> {
    #[cfg(windows)]
    {
        let _ = socket;
        return Err(CliError::InvalidResponse(
            "Windows attach is not implemented until the ConPTY multiplexer attach path exists"
                .to_string(),
        ));
    }

    #[cfg(unix)]
    {
        let state_dir = socket.parent().ok_or_else(|| {
            CliError::InvalidResponse(format!(
                "socket path has no parent directory: {}",
                socket.display()
            ))
        })?;
        let socket_name = compute_socket_name(state_dir);
        Ok(PathBuf::from(format!(
            "/tmp/tmux-{}/{}",
            unsafe { libc::geteuid() },
            socket_name
        )))
    }
}

fn prepare_attach_command(socket: &Path, session_name: &str) -> Vec<String> {
    vec![
        "tmux".to_string(),
        "-S".to_string(),
        socket.display().to_string(),
        "attach".to_string(),
        "-t".to_string(),
        session_name.to_string(),
    ]
}

fn exec_tmux_attach(socket: PathBuf, session_name: String) -> ! {
    #[cfg(windows)]
    {
        let _ = (socket, session_name);
        eprintln!(
            "Windows attach is not implemented until the ConPTY multiplexer attach path exists"
        );
        std::process::exit(1);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let cmd = prepare_attach_command(&socket, &session_name);
        let err = std::process::Command::new(&cmd[0]).args(&cmd[1..]).exec();
        eprintln!("exec tmux attach failed: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, Cmd, MasterCmd, attach_session_name, bottom_contains_tell_body, cmd_agent_notify,
        contains_paste_expand_guard, detect_nesting, format_agent_notify_output,
        prepare_attach_command, resolve_attach_session_name, resolve_start_config_path,
        runtime_subscribe_params, status_snapshot_json,
    };
    use ah::cli::rpc_client::{RpcClient, RpcFuture, UnixRpcClient};
    use clap::Parser;
    use serde_json::Value;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    struct RecordingClient {
        calls: Mutex<Vec<(String, Value)>>,
        response: Value,
    }

    impl RpcClient for RecordingClient {
        fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push((method.to_string(), params));
                Ok(self.response.clone())
            })
        }
    }

    struct CurrentDirGuard {
        previous: PathBuf,
    }

    impl CurrentDirGuard {
        fn enter(path: &Path) -> Self {
            let previous = std::env::current_dir().unwrap();
            std::env::set_current_dir(path).unwrap();
            Self { previous }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.previous).unwrap();
        }
    }

    fn write_minimal_config(path: &Path) {
        let shared_credentials_dir = path.parent().unwrap().join("shared-claude-credentials");
        std::fs::create_dir_all(&shared_credentials_dir).unwrap();
        std::fs::write(
            path,
            format!(
                r#"
version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[agents.a1]
provider = "bash"
"#,
                shared_credentials_dir.display()
            ),
        )
        .unwrap();
    }

    #[tokio::test]
    #[serial_test::serial(global_env)]
    async fn start_without_config_errors_before_daemon_socket() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("state").join("ccbd.sock");
        let _cwd = CurrentDirGuard::enter(dir.path());
        let client = UnixRpcClient::new(socket.clone());

        let err = super::cmd_start(&client, None, false).await.unwrap_err();

        assert!(
            err.to_string().contains("could not find ah.toml"),
            "unexpected error: {err}"
        );
        assert!(!socket.exists());
    }

    #[test]
    fn start_config_resolution_accepts_valid_discoverable_config() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("ah.toml");
        write_minimal_config(&config_path);

        let resolved = resolve_start_config_path(None, dir.path()).unwrap();

        assert_eq!(resolved, config_path);
    }

    #[test]
    fn test_prepare_attach_command_returns_tmux_attach() {
        let session_name = attach_session_name("a1");
        let cmd = prepare_attach_command(Path::new("/tmp/tmux-1001/ahd-test"), &session_name);

        assert_eq!(
            cmd,
            vec![
                "tmux".to_string(),
                "-S".to_string(),
                "/tmp/tmux-1001/ahd-test".to_string(),
                "attach".to_string(),
                "-t".to_string(),
                "agent_a1".to_string(),
            ]
        );
    }

    #[test]
    fn ah_cli_has_master_cutover_subcommand() {
        let cli = Cli::parse_from(["ah", "master", "cutover", "--wait", "--print-attach"]);

        match cli.cmd {
            Some(Cmd::Master {
                cmd: MasterCmd::Cutover { wait, print_attach },
            }) => {
                assert!(wait);
                assert!(print_attach);
            }
            _ => panic!("expected master cutover command"),
        }
    }

    #[test]
    fn ah_cli_parses_ps_all() {
        let cli = Cli::parse_from(["ah", "ps", "--all"]);

        match cli.cmd {
            Some(Cmd::Ps { all }) => assert!(all),
            _ => panic!("expected ps command"),
        }
    }

    #[test]
    fn ah_cli_parses_status_json_default() {
        let cli = Cli::parse_from(["ah", "status"]);

        match cli.cmd {
            Some(Cmd::Status { json }) => assert!(json),
            _ => panic!("expected status command"),
        }
    }

    #[tokio::test]
    async fn status_json_uses_runtime_snapshot_rpc_and_emits_schema_v2_json() {
        let client = RecordingClient {
            calls: Mutex::new(Vec::new()),
            response: json!({
                "schema_version": 2,
                "event": "snapshot",
                "sessions": []
            }),
        };

        let rendered = status_snapshot_json(&client, true).await.unwrap();
        let parsed: Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["schema_version"], 2);

        let calls = client.calls.lock().unwrap();
        assert_eq!(
            calls.as_slice(),
            [("runtime.snapshot".to_string(), json!({}))]
        );
    }

    #[test]
    fn ah_cli_parses_agent_notify_stop_command() {
        let cli = Cli::parse_from([
            "ah",
            "agent",
            "notify",
            "--agent-id",
            "ag_notify",
            "--event",
            "stop",
            "--provider",
            "codex",
            "--event-id",
            "evt-cli",
            "--socket",
            "/tmp/ahd.sock",
        ]);

        match cli.cmd {
            Some(Cmd::Agent {
                cmd:
                    super::AgentCmd::Notify {
                        agent_id,
                        event,
                        provider,
                        event_id,
                        hook_json,
                        hook_debug_log,
                        socket,
                        outbox_dir,
                    },
            }) => {
                assert_eq!(agent_id, "ag_notify");
                assert_eq!(event, "stop");
                assert_eq!(provider.as_deref(), Some("codex"));
                assert_eq!(event_id.as_deref(), Some("evt-cli"));
                assert!(!hook_json);
                assert!(hook_debug_log.is_none());
                assert_eq!(socket.as_deref(), Some(Path::new("/tmp/ahd.sock")));
                assert!(outbox_dir.is_none());
            }
            _ => panic!("expected agent notify command"),
        }
    }

    #[test]
    fn ah_cli_parses_events_json_command() {
        let cli = Cli::parse_from(["ah", "--config", "/tmp/project/ah.toml", "events"]);

        match cli.cmd {
            Some(Cmd::Events { format }) => {
                assert_eq!(format, "json");
                assert_eq!(
                    cli.config.as_deref(),
                    Some(Path::new("/tmp/project/ah.toml"))
                );
            }
            _ => panic!("expected events command"),
        }
    }

    #[test]
    fn runtime_subscribe_params_do_not_filter_by_workspace() {
        // The config parent is NOT the workspace (Studio keeps transient
        // configs under the OS temp dir); filtering inventory by it made
        // every snapshot report an inactive runtime.
        let params = runtime_subscribe_params(Path::new("/tmp/skill-studio-ah/x/claude/ah.toml"));

        assert_eq!(
            params.get("config_path").and_then(|value| value.as_str()),
            Some("/tmp/skill-studio-ah/x/claude/ah.toml")
        );
        assert!(params.get("workspace_path").is_none());
    }

    #[test]
    fn ah_cli_parses_tell_master_command() {
        let cli = Cli::parse_from([
            "ah",
            "tell",
            "master",
            "do this",
            "--session",
            "s1",
            "--request-id",
            "tell_1",
        ]);

        match cli.cmd {
            Some(Cmd::Tell {
                target,
                text,
                session,
                request_id,
            }) => {
                assert_eq!(target, "master");
                assert_eq!(text, "do this");
                assert_eq!(session.as_deref(), Some("s1"));
                assert_eq!(request_id.as_deref(), Some("tell_1"));
            }
            _ => panic!("expected tell command"),
        }
    }

    #[test]
    fn ah_cli_parses_agent_notify_hook_json_flag() {
        let cli = Cli::parse_from([
            "ah",
            "agent",
            "notify",
            "--agent-id",
            "ag_notify",
            "--event",
            "stop",
            "--hook-json",
            "--hook-debug-log",
            "/tmp/ah-hooks/ag_notify.log",
        ]);

        match cli.cmd {
            Some(Cmd::Agent {
                cmd:
                    super::AgentCmd::Notify {
                        hook_json,
                        hook_debug_log,
                        ..
                    },
            }) => {
                assert!(hook_json);
                assert_eq!(
                    hook_debug_log.as_deref(),
                    Some(Path::new("/tmp/ah-hooks/ag_notify.log"))
                );
            }
            _ => panic!("expected agent notify command"),
        }
    }

    #[test]
    fn agent_notify_hook_json_formats_empty_object_only() {
        let result = json!({
            "agent_id": "ag_notify",
            "event": "stop",
            "transitioned": true,
        });

        assert_eq!(format_agent_notify_output(&result, true), "{}\n");
        assert_eq!(
            format_agent_notify_output(&result, false),
            "agent_id=ag_notify\nevent=stop\ntransitioned=true\n"
        );
    }

    // R1-T1 / CP-R1.1 — the exit-code invariant, driven through the real cmd_agent_notify:
    // exit 0 ⇔ a durable outbox record exists. A dead socket makes the RPC fail; the outcome
    // is decided purely by whether the journal landed.

    #[tokio::test]
    async fn agent_notify_journals_then_exits_zero_when_rpc_is_down() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("outbox").join("a1");
        // A socket that does not exist → the fast-path RPC will fail.
        let client = UnixRpcClient::new(tmp.path().join("nonexistent-ahd.sock"));

        let res = cmd_agent_notify(
            &client,
            "a1".to_string(),
            "stop".to_string(),
            Some("claude".to_string()),
            None,
            true,
            None,
            Some(outbox.clone()),
        )
        .await;

        assert!(
            res.is_ok(),
            "a journaled record must exit 0 even when RPC is down"
        );
        let jsons: Vec<_> = std::fs::read_dir(&outbox)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".json")
            })
            .collect();
        assert_eq!(
            jsons.len(),
            1,
            "exit 0 ⇔ exactly one durable record on disk"
        );
    }

    #[tokio::test]
    async fn agent_notify_exits_nonzero_when_journal_fails() {
        let tmp = tempfile::tempdir().unwrap();
        // Parent of the outbox dir is a FILE → the journal cannot make the record durable.
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"not a dir").unwrap();
        let outbox = blocker.join("outbox").join("a1");
        let client = UnixRpcClient::new(tmp.path().join("nonexistent-ahd.sock"));

        let res = cmd_agent_notify(
            &client,
            "a1".to_string(),
            "stop".to_string(),
            None,
            None,
            true,
            None,
            Some(outbox),
        )
        .await;

        assert!(
            res.is_err(),
            "a failed journal must be a loud non-zero exit, never a silent exit-0"
        );
    }

    #[test]
    fn test_attach_session_name_maps_agent_id() {
        assert_eq!(attach_session_name("a1"), "agent_a1");
        assert_eq!(attach_session_name("agent-42"), "agent_agent-42");
    }

    #[test]
    fn attach_master_maps_to_master_session_name() {
        let sessions = json!({
            "sessions": [
                {
                    "id": "s1",
                    "project_id": "ccbd-rust",
                    "status": "ACTIVE",
                    "master_pane_id": "%42"
                }
            ]
        });

        let session_name =
            resolve_attach_session_name("master", None, None, Some(&sessions)).unwrap();

        assert_eq!(session_name, "master_ccbd-rust");
    }

    #[test]
    fn legacy_attach_agent_still_maps_to_agent_session_name() {
        let session_name = resolve_attach_session_name("a1", None, None, None).unwrap();

        assert_eq!(session_name, "agent_a1");
    }

    #[test]
    fn attach_master_errors_when_no_master_pane() {
        let sessions = json!({
            "sessions": [
                {
                    "id": "s1",
                    "project_id": "ccbd-rust",
                    "status": "ACTIVE"
                }
            ]
        });

        let err = resolve_attach_session_name("master", None, None, Some(&sessions)).unwrap_err();

        assert!(
            err.to_string().contains("master pane"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn tell_verify_ignores_historical_body_outside_bottom_composer() {
        let capture =
            "old prompt body\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8\nline 9\n  >";

        assert!(!bottom_contains_tell_body(capture, "old prompt body"));
    }

    #[test]
    fn tell_verify_detects_guard_in_bottom_composer() {
        let capture = "transcript\n\npaste again to expand";

        assert!(contains_paste_expand_guard(capture));
    }

    #[test]
    fn test_check_nested_environment_detects_tmux_var() {
        let reason = detect_nesting(Some("/tmp/tmux-1001/ahd-1234567890abcdef,1,0"), "");

        assert!(reason.unwrap().contains("TMUX"));
    }

    #[test]
    fn test_check_nested_environment_detects_cgroup() {
        let reason = detect_nesting(None, "0::/user.slice/ccb-project-ahd-agents.slice\n");

        assert!(reason.unwrap().contains("cgroup"));
    }

    #[test]
    fn test_check_nested_environment_passes_normal() {
        assert_eq!(detect_nesting(None, "0::/user.slice/session.scope\n"), None);
    }
}
