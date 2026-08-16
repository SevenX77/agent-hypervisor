use ah::tmux::{
    TmuxServer, compute_socket_name,
    scope::{self, ScopePolicy, UnitConfig},
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let mut hold_tmux = false;
    let mut state_dir = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--hold-tmux" => hold_tmux = true,
            "--state-dir" => state_dir = args.next().map(PathBuf::from),
            other => {
                eprintln!("unknown argument: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    if !hold_tmux {
        eprintln!("--hold-tmux is required");
        return ExitCode::FAILURE;
    }
    let Some(state_dir) = state_dir else {
        eprintln!("--state-dir is required");
        return ExitCode::FAILURE;
    };
    if let Err(err) = std::fs::create_dir_all(&state_dir) {
        eprintln!("create state dir {}: {err}", state_dir.display());
        return ExitCode::FAILURE;
    }

    let socket_name = compute_socket_name(&state_dir);
    let mut policy = scope::detect_scope_policy_with_daemon_unit(&socket_name, None);
    if let Ok(wrapper_scope) = std::env::var("AH_TEST_WRAPPER_SCOPE") {
        policy = ScopePolicy::Systemd(UnitConfig {
            unit_name: scope::unit_name_for_socket(&socket_name),
            slice: "ahd-agents.slice".to_string(),
            binds_to: Some(wrapper_scope),
        });
    }

    let server = TmuxServer::new_with_policy(&state_dir, policy);
    if let Err(err) = server
        .ensure_session("ahd-test-helper".to_string(), state_dir.clone())
        .await
    {
        eprintln!("ensure tmux session: {err}");
        return ExitCode::FAILURE;
    }

    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}
