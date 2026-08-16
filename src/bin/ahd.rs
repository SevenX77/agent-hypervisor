use ah::{cli, env, orchestrator, rpc, storage as db};
use ah::{
    monitor::{master_watch, session_watch::unit_name_for_session},
    outbox, sandbox,
    tmux::{TmuxServer, agent_session_name, master_session_name},
};
use std::io;
use std::path::Path;
use std::process::Command;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tracing_subscriber::EnvFilter;

#[derive(Debug, PartialEq, Eq)]
enum AhdCliAction {
    RunDaemon,
    PrintVersion,
    PrintHelp,
    UnknownFlag(String),
}

fn classify_ahd_args(args: &[String]) -> AhdCliAction {
    match args.first().map(String::as_str) {
        None => AhdCliAction::RunDaemon,
        Some("--version" | "-V") => AhdCliAction::PrintVersion,
        Some("--help" | "-h") => AhdCliAction::PrintHelp,
        Some(flag) if flag.starts_with('-') => AhdCliAction::UnknownFlag(flag.to_string()),
        Some(_) => AhdCliAction::RunDaemon,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match classify_ahd_args(&args) {
        AhdCliAction::RunDaemon => {}
        AhdCliAction::PrintVersion => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        AhdCliAction::PrintHelp => {
            println!("Usage: ahd [--version|-V] [--help|-h]");
            return ExitCode::SUCCESS;
        }
        AhdCliAction::UnknownFlag(flag) => {
            eprintln!("unknown option: {flag}");
            return ExitCode::from(2);
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let sandbox_env = match sandbox::check_environment() {
        Ok(env_state) => env_state,
        Err(err) => {
            eprintln!("{}", err.to_rpc_error());
            return ExitCode::FAILURE;
        }
    };
    if !sandbox_env.unsafe_no_sandbox && std::env::var_os("INVOCATION_ID").is_none() {
        tracing::warn!(
            "ahd not running under systemd; cascade cleanup will rely on Startup Reconcile only"
        );
    }
    let dir = env::resolve_state_dir();
    let daemon_unit = ah::platform::sys::scope::active_daemon_unit_or_none(Some(
        cli::service_unit::derive_unit_name(&dir),
    ));
    tracing::info!(?dir, "ahd starting");
    if let Err(err) = migrate_legacy_db_files(&dir) {
        tracing::error!(?dir, error = %err, "legacy database migration failed");
        return ExitCode::FAILURE;
    }

    let db_path = dir.join("ahd.sqlite");
    match db::init(&db_path) {
        Ok(db) => {
            tracing::info!(?db_path, "database initialized");
            let tmux_server = Arc::new(TmuxServer::new_with_daemon_unit(
                &dir,
                daemon_unit.as_deref(),
            ));
            let claude_gateway = Arc::new(ah::claude_gateway::ClaudeGatewayService::new());
            let reconcile_result = db::system::reconcile_startup_with_tmux_socket_and_gateway(
                db.clone(),
                dir.clone(),
                Some(tmux_server.socket_name().to_string()),
                Some(claude_gateway.clone()),
            )
            .await;

            match reconcile_result {
                Ok(count) => {
                    tracing::info!(reconciled = count, "startup reconcile complete");
                    let socket_path = dir.join("ahd.sock");
                    let ctx = rpc::Ctx {
                        db,
                        state_dir: dir.clone(),
                        env_state: sandbox_env,
                        daemon_unit,
                        tmux_server,
                        claude_gateway,
                    };
                    match master_watch::rearm_active_master_watches_on_startup(&ctx).await {
                        Ok(count) => tracing::info!(
                            rearmed_or_detected = count,
                            "startup master watch rearm complete"
                        ),
                        Err(err) => {
                            tracing::error!(error = %err, "startup master watch rearm failed");
                            return ExitCode::FAILURE;
                        }
                    }
                    // R1-T2 / CP-R1.3 — cold-scan replay BEFORE serving RPC, so there is no
                    // window where ahd is up but has not replayed its durable outbox. Records
                    // written before a kill -9 are consumed exactly once (JC-1 dedup); poison
                    // records are error-booked to outbox/dead/ instead of stalling the scan.
                    match outbox::cold_scan_all_agents(&ctx.db, &ctx.state_dir) {
                        Ok(report) => tracing::info!(
                            consumed = report.consumed,
                            duplicates = report.duplicates,
                            quarantined = report.quarantined,
                            retry_deferred = report.retry_deferred,
                            "outbox cold-scan replay complete"
                        ),
                        Err(err) => {
                            tracing::error!(error = %err, "outbox cold-scan replay failed")
                        }
                    }

                    orchestrator::spawn_orchestrator_task(ctx.clone());
                    run_until_shutdown(socket_path, ctx).await
                }
                Err(err) => {
                    tracing::error!(error = %err, "startup reconcile failed");
                    ExitCode::FAILURE
                }
            }
        }
        Err(err) => {
            tracing::error!(?db_path, error = %err, "database initialization failed");
            ExitCode::FAILURE
        }
    }
}

fn migrate_legacy_db_files(dir: &Path) -> io::Result<()> {
    for suffix in ["", "-wal", "-shm"] {
        let old_path = dir.join(format!("ah.sqlite{suffix}"));
        let new_path = dir.join(format!("ahd.sqlite{suffix}"));
        if old_path.exists() && !new_path.exists() {
            std::fs::rename(&old_path, &new_path)?;
            tracing::info!(
                from = %old_path.display(),
                to = %new_path.display(),
                "migrated legacy daemon database file"
            );
        }
    }
    Ok(())
}

#[cfg(windows)]
async fn run_until_shutdown(socket_path: std::path::PathBuf, ctx: rpc::Ctx) -> ExitCode {
    match rpc::run_server(&socket_path, ctx).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(?socket_path, error = %err, "Windows RPC server failed");
            ExitCode::FAILURE
        }
    }
}

#[cfg(unix)]
async fn run_until_shutdown(socket_path: std::path::PathBuf, ctx: rpc::Ctx) -> ExitCode {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(err) => {
            tracing::error!(error = %err, "failed to register SIGTERM handler");
            return ExitCode::FAILURE;
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(signal) => signal,
        Err(err) => {
            tracing::error!(error = %err, "failed to register SIGINT handler");
            return ExitCode::FAILURE;
        }
    };

    let shutdown_signal = async move {
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("received SIGTERM"),
            _ = sigint.recv() => tracing::info!("received SIGINT"),
        }
    };

    tokio::select! {
        result = rpc::run_server(&socket_path, ctx.clone()) => {
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    tracing::error!(?socket_path, error = %err, "UDS server failed");
                    ExitCode::FAILURE
                }
            }
        }
        _ = shutdown_signal => {
            tracing::info!("shutdown initiated, cleaning tmux resources");
            cleanup_tmux_resources(&ctx).await;
            let _ = std::fs::remove_file(&socket_path);
            ExitCode::SUCCESS
        }
    }
}

#[cfg(unix)]
async fn cleanup_tmux_resources(ctx: &rpc::Ctx) {
    let socket_name = ctx.tmux_server.socket_name().to_string();

    for session_name in shutdown_session_names(ctx) {
        if let Err(err) = ctx.tmux_server.kill_session(session_name.clone()).await {
            tracing::warn!(session_name = %session_name, error = %err, "tmux kill-session failed during shutdown");
        }
    }
    cleanup_master_sandboxes(ctx);

    cleanup_session_anchors(ctx);

    run_tmux_cleanup_command(
        Command::new("tmux")
            .args(["-L", &socket_name, "kill-server"])
            .output(),
        "tmux kill-server",
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    let socket_path = format!("/tmp/tmux-{}/{}", unsafe { libc::geteuid() }, socket_name);
    match std::fs::remove_file(&socket_path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(error = %err, path = %socket_path, "socket file remove failed"),
    }
}

fn cleanup_session_anchors(ctx: &rpc::Ctx) {
    if !shutdown_session_anchors_enabled(ctx) {
        return;
    }

    for unit_name in shutdown_anchor_unit_names(ctx) {
        stop_session_anchor(&unit_name);
    }
}

fn shutdown_session_anchors_enabled(ctx: &rpc::Ctx) -> bool {
    ctx.env_state.systemd_run_available
        && (ctx.env_state.unsafe_no_sandbox || ctx.env_state.under_systemd)
}

fn shutdown_anchor_unit_names(ctx: &rpc::Ctx) -> Vec<String> {
    active_session_ids(ctx)
        .into_iter()
        .map(|session_id| unit_name_for_session(&session_id))
        .collect()
}

fn cleanup_master_sandboxes(ctx: &rpc::Ctx) {
    for session_id in active_session_ids(ctx) {
        ah::db::system::remove_agent_sandbox_dir_sync(&ctx.state_dir, &session_id, "master");
    }
}

fn active_session_ids(ctx: &rpc::Ctx) -> Vec<String> {
    let conn = ctx.db.conn();
    let mut session_ids = Vec::new();

    match conn
        .prepare("SELECT id FROM sessions WHERE status = 'ACTIVE' ORDER BY created_at ASC, id ASC")
    {
        Ok(mut stmt) => match stmt.query_map([], |row| row.get::<_, String>(0)) {
            Ok(rows) => {
                for row in rows {
                    match row {
                        Ok(session_id) => session_ids.push(session_id),
                        Err(err) => {
                            tracing::warn!(error = %err, "active session shutdown row decode failed")
                        }
                    }
                }
            }
            Err(err) => tracing::warn!(error = %err, "active session shutdown query failed"),
        },
        Err(err) => {
            tracing::warn!(error = %err, "active session shutdown query prepare failed")
        }
    }

    session_ids
}

fn stop_session_anchor(unit_name: &str) {
    match Command::new("systemctl")
        .args(["--user", "stop", unit_name])
        .output()
    {
        Ok(output) if output.status.success() => {}
        Ok(output) => tracing::warn!(
            unit = %unit_name,
            status = ?output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "failed to stop session anchor during shutdown"
        ),
        Err(err) => {
            tracing::warn!(unit = %unit_name, error = %err, "failed to run systemctl stop during shutdown")
        }
    }
}

fn shutdown_session_names(ctx: &rpc::Ctx) -> Vec<String> {
    let conn = ctx.db.conn();
    let mut names = Vec::new();

    match conn.prepare(
        "SELECT id FROM agents WHERE state NOT IN ('CRASHED', 'KILLED') ORDER BY updated_at ASC, id ASC",
    ) {
        Ok(mut stmt) => match stmt.query_map([], |row| row.get::<_, String>(0)) {
            Ok(rows) => {
                for row in rows {
                    match row {
                        Ok(agent_id) => names.push(agent_session_name(&agent_id)),
                        Err(err) => tracing::warn!(error = %err, "active agent shutdown row decode failed"),
                    }
                }
            }
            Err(err) => tracing::warn!(error = %err, "active agent shutdown query failed"),
        },
        Err(err) => tracing::warn!(error = %err, "active agent shutdown query prepare failed"),
    }

    match conn.prepare(
        "SELECT project_id FROM sessions WHERE status = 'ACTIVE' ORDER BY created_at ASC, id ASC",
    ) {
        Ok(mut stmt) => match stmt.query_map([], |row| row.get::<_, String>(0)) {
            Ok(rows) => {
                for row in rows {
                    match row {
                        Ok(project_id) => names.push(master_session_name(&project_id)),
                        Err(err) => {
                            tracing::warn!(error = %err, "active master shutdown row decode failed")
                        }
                    }
                }
            }
            Err(err) => tracing::warn!(error = %err, "active master shutdown query failed"),
        },
        Err(err) => tracing::warn!(error = %err, "active master shutdown query prepare failed"),
    }

    names
}

fn run_tmux_cleanup_command(result: io::Result<std::process::Output>, label: &str) {
    match result {
        Ok(output) if output.status.success() => {}
        Ok(output) => tracing::warn!(
            status = ?output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "{label} failed during shutdown"
        ),
        Err(err) => tracing::warn!(error = %err, "{label} failed during shutdown"),
    }
}

#[cfg(test)]
mod tests {
    use super::{AhdCliAction, classify_ahd_args, shutdown_anchor_unit_names};
    use ah::db;
    use ah::monitor::session_watch::unit_name_for_session;
    use ah::rpc;
    use ah::sandbox::EnvState;
    use ah::tmux::TmuxServer;
    use std::sync::Arc;

    #[test]
    fn classify_ahd_args_handles_non_daemon_cli_actions() {
        assert_eq!(
            classify_ahd_args(&["--version".to_string()]),
            AhdCliAction::PrintVersion
        );
        assert_eq!(
            classify_ahd_args(&["-V".to_string()]),
            AhdCliAction::PrintVersion
        );
        assert_eq!(
            classify_ahd_args(&["--help".to_string()]),
            AhdCliAction::PrintHelp
        );
        assert_eq!(
            classify_ahd_args(&["--bogus".to_string()]),
            AhdCliAction::UnknownFlag("--bogus".to_string())
        );
        assert_eq!(classify_ahd_args(&[]), AhdCliAction::RunDaemon);
    }

    #[test]
    fn test_shutdown_anchor_unit_names_lists_active_session_anchors_only() {
        let db_file = tempfile::NamedTempFile::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let db = db::init(db_file.path()).unwrap();
        {
            let conn = db.conn();
            for (session_id, project_id, absolute_path) in [
                ("sess_active_a", "p_active_a", "/tmp/a"),
                ("sess_killed", "p_killed", "/tmp/killed"),
                ("sess_active_b", "p_active_b", "/tmp/b"),
            ] {
                conn.execute(
                    "INSERT INTO projects (id, absolute_path) VALUES (?, ?)",
                    (project_id, absolute_path),
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO sessions (id, project_id, master_pid) VALUES (?, ?, 0)",
                    (session_id, project_id),
                )
                .unwrap();
            }
            conn.execute(
                "UPDATE sessions SET status = 'KILLED' WHERE id = 'sess_killed'",
                [],
            )
            .unwrap();
        }
        let ctx = rpc::Ctx {
            db,
            state_dir: state_dir.path().to_path_buf(),
            env_state: EnvState {
                systemd_run_available: false,
                unsafe_no_sandbox: true,
                under_systemd: false,
            },
            daemon_unit: None,
            tmux_server: Arc::new(TmuxServer::new(state_dir.path())),
            claude_gateway: Arc::new(ah::claude_gateway::ClaudeGatewayService::new()),
        };

        let units = shutdown_anchor_unit_names(&ctx);

        assert_eq!(
            units,
            vec![
                unit_name_for_session("sess_active_a"),
                unit_name_for_session("sess_active_b"),
            ]
        );
    }
}
