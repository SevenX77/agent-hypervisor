use crate::error::CcbdError;
use crate::tmux::{
    TmuxError, TmuxPaneId, compute_socket_name,
    scope::{self, ScopePolicy},
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TmuxWindowSize {
    #[default]
    Fixed,
    Follow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TmuxPaneRuntime {
    pub pid: i32,
    pub dead: bool,
}

#[derive(Clone, Debug)]
pub struct TmuxServer {
    socket_name: String,
    scope_policy: ScopePolicy,
}

impl TmuxServer {
    pub fn new(state_dir: &Path) -> Self {
        let socket_name = compute_socket_name(state_dir);
        let policy = scope::detect_scope_policy(&socket_name);
        Self::new_with_policy(state_dir, policy)
    }

    pub fn new_with_daemon_unit(state_dir: &Path, daemon_unit: Option<&str>) -> Self {
        let socket_name = compute_socket_name(state_dir);
        let policy = scope::detect_scope_policy_with_daemon_unit(&socket_name, daemon_unit);
        Self::new_with_policy(state_dir, policy)
    }

    pub fn new_with_policy(state_dir: &Path, policy: ScopePolicy) -> Self {
        Self {
            socket_name: compute_socket_name(state_dir),
            scope_policy: policy,
        }
    }

    pub fn from_socket_name(socket_name: String) -> Self {
        let scope_policy = scope::detect_scope_policy(&socket_name);
        Self {
            socket_name,
            scope_policy,
        }
    }

    pub fn socket_name(&self) -> &str {
        &self.socket_name
    }

    pub(crate) fn socket_path_sync(&self) -> Result<PathBuf, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "display-message",
            "-p",
            "#{socket_path}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let path = ensure_output_success("tmux", &args, &[], output)?;
        Ok(PathBuf::from(path.trim()))
    }

    pub(crate) fn ensure_session_sync(
        &self,
        session_name: &str,
        cwd: &Path,
    ) -> Result<(), CcbdError> {
        self.ensure_session_with_window_size_sync(session_name, cwd, TmuxWindowSize::Fixed)
    }

    pub(crate) fn ensure_session_with_window_size_sync(
        &self,
        session_name: &str,
        cwd: &Path,
        window_size: TmuxWindowSize,
    ) -> Result<(), CcbdError> {
        let has_session = Command::new("tmux")
            .args(["-L", &self.socket_name, "has-session", "-t", session_name])
            .output()
            .map_err(map_command_io_error)?;

        if has_session.status.success() {
            return Ok(());
        }

        let cwd_arg = cwd.display().to_string();
        let args = new_session_args(&self.socket_name, session_name, &cwd_arg, window_size);
        let output = if self.server_running_sync()? {
            Command::new("tmux")
                .args(&args)
                .output()
                .map_err(map_command_io_error)?
        } else {
            let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
            scope::wrap_in_scope("tmux", &arg_refs, &self.scope_policy)
                .output()
                .map_err(map_command_io_error)?
        };
        let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
        ensure_success("tmux", &arg_refs, output)?;

        let window_size_value = match window_size {
            TmuxWindowSize::Fixed => "manual",
            TmuxWindowSize::Follow => "latest",
        };
        let args = [
            "-L",
            &self.socket_name,
            "set-option",
            "-t",
            session_name,
            "window-size",
            window_size_value,
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn server_running_sync(&self) -> Result<bool, CcbdError> {
        let output = Command::new("tmux")
            .args(["-L", &self.socket_name, "list-sessions"])
            .output()
            .map_err(map_command_io_error)?;
        Ok(output.status.success())
    }

    pub(crate) fn session_exists_sync(&self, session_name: &str) -> Result<bool, CcbdError> {
        let output = Command::new("tmux")
            .args(["-L", &self.socket_name, "has-session", "-t", session_name])
            .output()
            .map_err(map_command_io_error)?;
        Ok(output.status.success())
    }

    pub(crate) fn spawn_window_sync(
        &self,
        session: &str,
        window: &str,
        cwd: &Path,
        cmd: &[&str],
    ) -> Result<TmuxPaneId, CcbdError> {
        if let Some(initial_window) = self.reusable_initial_window_sync(session)? {
            return self.respawn_initial_window_sync(session, &initial_window, window, cwd, cmd);
        }

        let target = format!("{session}:");
        let cwd_arg = cwd.display().to_string();
        let args = [
            "-L",
            &self.socket_name,
            "new-window",
            "-d",
            "-t",
            &target,
            "-n",
            window,
            "-c",
            &cwd_arg,
            "-P",
            "-F",
            "#{pane_id}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let pane = ensure_output_success("tmux", &args, cmd, output)
            .and_then(|stdout| TmuxPaneId::parse(stdout.trim()).map_err(CcbdError::from))?;
        self.set_remain_on_exit_sync(&pane)?;
        self.respawn_pane_sync(&pane, cwd, cmd)?;
        Ok(pane)
    }

    fn reusable_initial_window_sync(&self, session: &str) -> Result<Option<String>, CcbdError> {
        // mvp12 M12.6: when ccbd starts fresh, the tmux session doesn't exist yet
        // and list-windows fails. Treat that as "no reusable window" rather than
        // propagating the error — new-window will create the session on first call.
        let windows = match self.list_window_names_sync(session) {
            Ok(w) => w,
            Err(_) => return Ok(None),
        };
        let Some(window) = reusable_initial_window(&windows) else {
            return Ok(None);
        };
        let panes = self.list_panes_sync(&format!("{session}:{window}"))?;
        if panes.len() == 1 {
            Ok(Some(window.to_string()))
        } else {
            Ok(None)
        }
    }

    fn list_window_names_sync(&self, session: &str) -> Result<Vec<String>, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "list-windows",
            "-t",
            session,
            "-F",
            "#{window_name}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let stdout = ensure_output_success("tmux", &args, &[], output)?;
        Ok(stdout.lines().map(ToString::to_string).collect())
    }

    fn respawn_initial_window_sync(
        &self,
        session: &str,
        initial_window: &str,
        window: &str,
        cwd: &Path,
        cmd: &[&str],
    ) -> Result<TmuxPaneId, CcbdError> {
        let old_target = format!("{session}:{initial_window}");
        let rename_args = [
            "-L",
            &self.socket_name,
            "rename-window",
            "-t",
            &old_target,
            window,
        ];
        let output = Command::new("tmux")
            .args(rename_args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &rename_args, output)?;

        let target = format!("{session}:{window}");
        let pane_args = [
            "-L",
            &self.socket_name,
            "display-message",
            "-p",
            "-t",
            &target,
            "#{pane_id}",
        ];
        let output = Command::new("tmux")
            .args(pane_args)
            .output()
            .map_err(map_command_io_error)?;
        let pane = ensure_output_success("tmux", &pane_args, &[], output)
            .and_then(|stdout| TmuxPaneId::parse(stdout.trim()).map_err(CcbdError::from))?;
        self.set_remain_on_exit_sync(&pane)?;
        self.respawn_pane_sync(&pane, cwd, cmd)?;
        Ok(pane)
    }

    fn respawn_pane_sync(
        &self,
        pane: &TmuxPaneId,
        cwd: &Path,
        cmd: &[&str],
    ) -> Result<(), CcbdError> {
        let cwd_arg = cwd.display().to_string();
        let respawn_args = [
            "-L",
            &self.socket_name,
            "respawn-pane",
            "-k",
            "-t",
            &pane.0,
            "-c",
            &cwd_arg,
            "--",
        ];
        let output = Command::new("tmux")
            .args(respawn_args)
            .args(cmd)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &respawn_args, output)
    }

    pub(crate) fn window_exists_sync(
        &self,
        session: &str,
        window: &str,
    ) -> Result<bool, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "list-windows",
            "-t",
            session,
            "-F",
            "#{window_name}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let stdout = ensure_output_success("tmux", &args, &[], output)?;
        Ok(stdout.lines().any(|line| line == window))
    }

    pub(crate) fn get_pane_pid_sync(&self, pane: &TmuxPaneId) -> Result<i32, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "display-message",
            "-p",
            "-t",
            &pane.0,
            "#{pane_pid}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let stdout = ensure_output_success("tmux", &args, &[], output)?;
        stdout
            .trim()
            .parse::<i32>()
            .map_err(|_| CcbdError::from(TmuxError::ParsePid(stdout.trim().to_string())))
    }

    pub(crate) fn get_pane_runtime_sync(
        &self,
        pane: &TmuxPaneId,
    ) -> Result<TmuxPaneRuntime, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "display-message",
            "-p",
            "-t",
            &pane.0,
            "#{pane_pid}\t#{pane_dead}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let stdout = ensure_output_success("tmux", &args, &[], output)?;
        parse_pane_runtime(stdout.trim())
    }

    pub(crate) fn resize_window_sync(
        &self,
        pane: &TmuxPaneId,
        width: u16,
        height: u16,
    ) -> Result<(), CcbdError> {
        let width = width.to_string();
        let height = height.to_string();
        let args = [
            "-L",
            &self.socket_name,
            "resize-window",
            "-t",
            &pane.0,
            "-x",
            &width,
            "-y",
            &height,
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn set_remain_on_exit_sync(&self, pane: &TmuxPaneId) -> Result<(), CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "set-window-option",
            "-t",
            &pane.0,
            "remain-on-exit",
            "on",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn pipe_pane_to_fifo_sync(
        &self,
        pane: &TmuxPaneId,
        fifo: &Path,
    ) -> Result<(), CcbdError> {
        let pipe_command = format!("cat > {}", shell_quote_path(fifo));
        let args = [
            "-L",
            &self.socket_name,
            "pipe-pane",
            "-t",
            &pane.0,
            "-O",
            &pipe_command,
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn send_keys_literal_sync(
        &self,
        pane: &TmuxPaneId,
        text: &str,
    ) -> Result<(), CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "send-keys",
            "-t",
            &pane.0,
            "-l",
            text,
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn send_keys_keysym_sync(
        &self,
        pane: &TmuxPaneId,
        keysym: &str,
    ) -> Result<(), CcbdError> {
        let args = ["-L", &self.socket_name, "send-keys", "-t", &pane.0, keysym];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn send_ctrl_c_sync(&self, pane: &TmuxPaneId) -> Result<(), CcbdError> {
        self.send_keys_keysym_sync(pane, "C-c")
    }

    pub(crate) fn kill_pane_sync(&self, pane: &TmuxPaneId) -> Result<(), CcbdError> {
        let args = ["-L", &self.socket_name, "kill-pane", "-t", &pane.0];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn kill_pane_if_owned_sync(&self, pane: &TmuxPaneId, expected_pid: i64) -> bool {
        if !self.target_has_expected_pid_sync(pane, expected_pid) {
            return false;
        }
        match self.kill_pane_sync(pane) {
            Ok(()) => true,
            Err(err) if tmux_target_missing(&err) => false,
            Err(err) => {
                tracing::warn!(
                    pane_id = %pane.0,
                    expected_pid,
                    error = %err,
                    "failed to kill owned pane"
                );
                false
            }
        }
    }

    pub(crate) fn target_has_expected_pid_sync(
        &self,
        target: &TmuxPaneId,
        expected_pid: i64,
    ) -> bool {
        let actual_pid = match self.get_pane_pid_sync(target) {
            Ok(pid) => i64::from(pid),
            Err(err) if tmux_target_missing(&err) => return false,
            Err(err) => {
                tracing::warn!(
                    pane_id = %target.0,
                    expected_pid,
                    error = %err,
                    "failed to validate pane ownership; skipping kill"
                );
                return false;
            }
        };
        if actual_pid != expected_pid {
            tracing::warn!(
                pane_id = %target.0,
                expected_pid,
                actual_pid,
                "pane ownership mismatch; skipping kill"
            );
            return false;
        }
        true
    }

    pub(crate) fn kill_session_sync(&self, session_name: &str) -> Result<(), CcbdError> {
        let args = ["-L", &self.socket_name, "kill-session", "-t", session_name];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    /// Terminate an entire tmux server. Callers may use this only for a
    /// socket they own exclusively; normal AH runtime sockets are shared by
    /// many sessions and must be cleaned through their narrower owners.
    pub(crate) fn kill_server_sync(&self) -> Result<(), CcbdError> {
        let args = ["-L", &self.socket_name, "kill-server"];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn kill_session_if_owned_sync(&self, session_name: &str, expected_pid: i64) -> bool {
        let Some(_pane) = self.find_pane_in_session_by_pid_sync(session_name, expected_pid) else {
            return false;
        };
        match self.kill_session_sync(session_name) {
            Ok(()) => true,
            Err(err) if tmux_target_missing(&err) => false,
            Err(err) => {
                tracing::warn!(
                    session_name,
                    expected_pid,
                    error = %err,
                    "failed to kill owned tmux session"
                );
                false
            }
        }
    }

    pub(crate) fn set_pane_title_sync(
        &self,
        pane: &TmuxPaneId,
        title: &str,
    ) -> Result<(), CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "select-pane",
            "-t",
            &pane.0,
            "-T",
            title,
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn kill_window_sync(&self, session: &str, window: &str) -> Result<(), CcbdError> {
        let target = format!("{session}:{window}");
        let args = ["-L", &self.socket_name, "kill-window", "-t", &target];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn capture_pane_sync(&self, pane: &TmuxPaneId) -> Result<String, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "capture-pane",
            "-p",
            "-t",
            &pane.0,
            "-S",
            "-200",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_output_success("tmux", &args, &[], output)
    }

    /// Capture pane text while joining soft-wrapped terminal rows. This is
    /// intended for control surfaces whose payloads (notably OAuth URLs) are
    /// longer than the fixed tmux width. It is not provider turn-state proof.
    pub(crate) fn capture_pane_joined_sync(&self, pane: &TmuxPaneId) -> Result<String, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "capture-pane",
            "-p",
            "-J",
            "-t",
            &pane.0,
            "-S",
            "-200",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_output_success("tmux", &args, &[], output)
    }

    /// Capture terminal escape sequences as well as text. This is reserved for
    /// control-effect confirmation (for example a highlighted menu selection)
    /// and must not be used as provider turn-state evidence.
    pub(crate) fn capture_pane_control_sync(&self, pane: &TmuxPaneId) -> Result<String, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "capture-pane",
            "-p",
            "-e",
            "-t",
            &pane.0,
            "-S",
            "-200",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_output_success("tmux", &args, &[], output)
    }

    pub(crate) fn list_panes_sync(
        &self,
        window_target: &str,
    ) -> Result<Vec<TmuxPaneId>, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "list-panes",
            "-t",
            window_target,
            "-F",
            "#{pane_id}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let stdout = ensure_output_success("tmux", &args, &[], output)?;
        stdout
            .lines()
            .map(|line| TmuxPaneId::parse(line.trim()).map_err(CcbdError::from))
            .collect()
    }

    pub(crate) fn find_pane_in_session_by_pid_sync(
        &self,
        session_name: &str,
        expected_pid: i64,
    ) -> Option<TmuxPaneId> {
        let panes = match self.list_all_panes_in_session_sync(session_name) {
            Ok(panes) => panes,
            Err(err) if tmux_target_missing(&err) => return None,
            Err(err) => {
                tracing::warn!(
                    session_name,
                    expected_pid,
                    error = %err,
                    "failed to list panes for ownership validation; skipping kill"
                );
                return None;
            }
        };
        let mut observed = Vec::new();
        for pane in panes {
            match self.get_pane_pid_sync(&pane) {
                Ok(pid) => observed.push((pane, i64::from(pid))),
                Err(err) if tmux_target_missing(&err) => {}
                Err(err) => tracing::warn!(
                    session_name,
                    pane_id = %pane.0,
                    expected_pid,
                    error = %err,
                    "failed to validate pane in session; skipping pane"
                ),
            }
        }
        select_single_matching_pane(session_name, expected_pid, observed)
    }

    fn list_all_panes_in_session_sync(
        &self,
        session_name: &str,
    ) -> Result<Vec<TmuxPaneId>, CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "list-panes",
            "-a",
            "-F",
            "#{session_name}\t#{pane_id}",
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        let stdout = ensure_output_success("tmux", &args, &[], output)?;
        stdout
            .lines()
            .filter_map(|line| {
                let (actual_session, pane_id) = line.split_once('\t')?;
                (actual_session == session_name).then_some(pane_id.trim())
            })
            .map(|pane_id| TmuxPaneId::parse(pane_id).map_err(CcbdError::from))
            .collect()
    }

    pub(crate) fn load_buffer_sync(&self, buffer_name: &str, text: &str) -> Result<(), CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "load-buffer",
            "-b",
            buffer_name,
            "-",
        ];
        let mut child = Command::new("tmux")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(map_command_io_error)?;

        match child.stdin.take() {
            Some(mut stdin) => {
                if let Err(err) = stdin.write_all(text.as_bytes()) {
                    let _ = child.kill();
                    return Err(CcbdError::TmuxCommandFailed {
                        cmd: format!("tmux {}", args.join(" ")),
                        stderr: format!("stdin write failed: {err}"),
                        exit: -1,
                    });
                }
            }
            None => {
                let _ = child.kill();
                return Err(CcbdError::TmuxCommandFailed {
                    cmd: format!("tmux {}", args.join(" ")),
                    stderr: "stdin pipe unavailable".into(),
                    exit: -1,
                });
            }
        }

        let output = child.wait_with_output().map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn paste_buffer_sync(
        &self,
        pane: &TmuxPaneId,
        buffer_name: &str,
    ) -> Result<(), CcbdError> {
        let args = [
            "-L",
            &self.socket_name,
            "paste-buffer",
            "-p",
            "-t",
            &pane.0,
            "-b",
            buffer_name,
        ];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn delete_buffer_sync(&self, buffer_name: &str) -> Result<(), CcbdError> {
        let args = ["-L", &self.socket_name, "delete-buffer", "-b", buffer_name];
        let output = Command::new("tmux")
            .args(args)
            .output()
            .map_err(map_command_io_error)?;
        ensure_success("tmux", &args, output)
    }

    pub(crate) fn send_enter_sync(&self, pane: &TmuxPaneId) -> Result<(), CcbdError> {
        self.send_keys_keysym_sync(pane, "Enter")
    }

    pub async fn ensure_session(
        &self,
        session_name: String,
        cwd: PathBuf,
    ) -> Result<(), CcbdError> {
        self.ensure_session_with_window_size(session_name, cwd, TmuxWindowSize::Fixed)
            .await
    }

    pub async fn ensure_session_with_window_size(
        &self,
        session_name: String,
        cwd: PathBuf,
        window_size: TmuxWindowSize,
    ) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::ensure_session", move || {
            server.ensure_session_with_window_size_sync(&session_name, &cwd, window_size)
        })
        .await
    }

    pub async fn spawn_window(
        &self,
        session: String,
        window: String,
        cwd: PathBuf,
        cmd: Vec<String>,
    ) -> Result<TmuxPaneId, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::spawn_window", move || {
            let args = cmd.iter().map(String::as_str).collect::<Vec<_>>();
            server.spawn_window_sync(&session, &window, &cwd, &args)
        })
        .await
    }

    pub async fn window_exists(&self, session: String, window: String) -> Result<bool, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::window_exists", move || {
            server.window_exists_sync(&session, &window)
        })
        .await
    }

    pub async fn server_running(&self) -> Result<bool, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::server_running", move || server.server_running_sync())
            .await
    }

    pub async fn session_exists(&self, session_name: String) -> Result<bool, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::session_exists", move || {
            server.session_exists_sync(&session_name)
        })
        .await
    }

    pub async fn get_pane_pid(&self, pane: TmuxPaneId) -> Result<i32, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::get_pane_pid", move || {
            server.get_pane_pid_sync(&pane)
        })
        .await
    }

    pub async fn get_pane_runtime(&self, pane: TmuxPaneId) -> Result<TmuxPaneRuntime, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::get_pane_runtime", move || {
            server.get_pane_runtime_sync(&pane)
        })
        .await
    }

    pub async fn find_pane_in_session_by_pid(
        &self,
        session_name: String,
        expected_pid: i64,
    ) -> Option<TmuxPaneId> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::find_pane_in_session_by_pid", move || {
            Ok::<Option<TmuxPaneId>, CcbdError>(
                server.find_pane_in_session_by_pid_sync(&session_name, expected_pid),
            )
        })
        .await
        .unwrap_or(None)
    }

    pub async fn pipe_pane_to_fifo(
        &self,
        pane: TmuxPaneId,
        fifo: PathBuf,
    ) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::pipe_pane_to_fifo", move || {
            server.pipe_pane_to_fifo_sync(&pane, &fifo)
        })
        .await
    }

    pub async fn send_keys_literal(&self, pane: TmuxPaneId, text: String) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::send_keys_literal", move || {
            server.send_keys_literal_sync(&pane, &text)
        })
        .await
    }

    pub async fn send_keys_keysym(
        &self,
        pane: TmuxPaneId,
        keysym: String,
    ) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::send_keys_keysym", move || {
            server.send_keys_keysym_sync(&pane, &keysym)
        })
        .await
    }

    pub async fn send_ctrl_c(&self, pane: TmuxPaneId) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::send_ctrl_c", move || server.send_ctrl_c_sync(&pane))
            .await
    }

    pub async fn set_pane_title(&self, pane: TmuxPaneId, title: &str) -> Result<(), CcbdError> {
        let server = self.clone();
        let title = title.to_string();
        crate::db::common::spawn_db("tmux::set_pane_title", move || {
            server.set_pane_title_sync(&pane, &title)
        })
        .await
    }

    pub async fn kill_pane(&self, pane: TmuxPaneId) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::kill_pane", move || server.kill_pane_sync(&pane)).await
    }

    pub async fn kill_pane_if_owned(&self, pane: TmuxPaneId, expected_pid: i64) -> bool {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::kill_pane_if_owned", move || {
            Ok::<bool, CcbdError>(server.kill_pane_if_owned_sync(&pane, expected_pid))
        })
        .await
        .unwrap_or(false)
    }

    pub async fn kill_session(&self, session_name: String) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::kill_session", move || {
            server.kill_session_sync(&session_name)
        })
        .await
    }

    pub async fn kill_session_if_owned(&self, session_name: String, expected_pid: i64) -> bool {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::kill_session_if_owned", move || {
            Ok::<bool, CcbdError>(server.kill_session_if_owned_sync(&session_name, expected_pid))
        })
        .await
        .unwrap_or(false)
    }

    pub async fn kill_window(&self, session: String, window: String) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::kill_window", move || {
            server.kill_window_sync(&session, &window)
        })
        .await
    }

    pub async fn capture_pane(&self, pane: TmuxPaneId) -> Result<String, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::capture_pane", move || {
            server.capture_pane_sync(&pane)
        })
        .await
    }

    pub async fn capture_pane_control(&self, pane: TmuxPaneId) -> Result<String, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::capture_pane_control", move || {
            server.capture_pane_control_sync(&pane)
        })
        .await
    }

    pub async fn list_panes(&self, window_target: String) -> Result<Vec<TmuxPaneId>, CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::list_panes", move || {
            server.list_panes_sync(&window_target)
        })
        .await
    }

    pub async fn load_buffer(&self, buffer_name: String, text: String) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::load_buffer", move || {
            server.load_buffer_sync(&buffer_name, &text)
        })
        .await
    }

    pub async fn paste_buffer(
        &self,
        pane: TmuxPaneId,
        buffer_name: String,
    ) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::paste_buffer", move || {
            server.paste_buffer_sync(&pane, &buffer_name)
        })
        .await
    }

    pub async fn delete_buffer(&self, buffer_name: String) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::delete_buffer", move || {
            server.delete_buffer_sync(&buffer_name)
        })
        .await
    }

    pub async fn send_enter(&self, pane: TmuxPaneId) -> Result<(), CcbdError> {
        let server = self.clone();
        crate::db::common::spawn_db("tmux::send_enter", move || server.send_enter_sync(&pane)).await
    }
}

fn reusable_initial_window(windows: &[String]) -> Option<&str> {
    let [window] = windows else {
        return None;
    };
    if matches!(window.as_str(), "0" | "bash" | "sh" | "zsh" | "fish") {
        Some(window)
    } else {
        None
    }
}

fn select_single_matching_pane<I>(
    session_name: &str,
    expected_pid: i64,
    panes: I,
) -> Option<TmuxPaneId>
where
    I: IntoIterator<Item = (TmuxPaneId, i64)>,
{
    let matches = panes
        .into_iter()
        .filter_map(|(pane, pid)| (pid == expected_pid).then_some(pane))
        .collect::<Vec<_>>();
    match matches.len() {
        1 => matches.into_iter().next(),
        0 => {
            tracing::warn!(
                session_name,
                expected_pid,
                "no pane in session matched expected pid; skipping kill"
            );
            None
        }
        count => {
            tracing::warn!(
                session_name,
                expected_pid,
                count,
                "multiple panes matched expected pid; skipping kill"
            );
            None
        }
    }
}

fn new_session_args(
    socket_name: &str,
    session_name: &str,
    cwd_arg: &str,
    window_size: TmuxWindowSize,
) -> Vec<String> {
    let mut args = vec![
        "-L".to_string(),
        socket_name.to_string(),
        "new-session".to_string(),
        "-d".to_string(),
        "-s".to_string(),
        session_name.to_string(),
        "-c".to_string(),
        cwd_arg.to_string(),
    ];
    if window_size == TmuxWindowSize::Fixed {
        args.extend([
            "-x".to_string(),
            "150".to_string(),
            "-y".to_string(),
            "60".to_string(),
        ]);
    }
    args
}

fn map_command_io_error(err: std::io::Error) -> CcbdError {
    CcbdError::from(TmuxError::Io(err))
}

fn ensure_success(program: &str, args: &[&str], output: Output) -> Result<(), CcbdError> {
    ensure_output_success(program, args, &[], output).map(|_| ())
}

fn ensure_output_success(
    program: &str,
    args: &[&str],
    extra_args: &[&str],
    output: Output,
) -> Result<String, CcbdError> {
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).to_string());
    }
    let mut parts = Vec::with_capacity(args.len() + extra_args.len() + 1);
    parts.push(program.to_string());
    parts.extend(args.iter().map(|arg| (*arg).to_string()));
    parts.extend(extra_args.iter().map(|arg| (*arg).to_string()));
    let cmd = parts.join(" ");
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let exit = output.status.code().unwrap_or(-1);
    tracing::debug!(cmd = %cmd, stderr = %stderr, exit = exit, "tmux command failed");
    Err(CcbdError::from(TmuxError::CommandFailed {
        cmd,
        stderr,
        exit,
    }))
}

pub(crate) fn tmux_target_missing(err: &CcbdError) -> bool {
    match err {
        CcbdError::TmuxCommandFailed { stderr, .. } => {
            stderr.contains("can't find pane")
                || stderr.contains("can't find window")
                || stderr.contains("can't find session")
                || stderr.contains("no such pane")
                || stderr.contains("no server running")
        }
        _ => false,
    }
}

fn shell_quote_path(path: &Path) -> String {
    let value = path.display().to_string();
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn parse_pane_runtime(value: &str) -> Result<TmuxPaneRuntime, CcbdError> {
    let value = value.trim();
    let Some((pid, dead)) = value.split_once('\t') else {
        return Err(CcbdError::from(TmuxError::CommandFailed {
            cmd: "tmux display-message #{pane_pid} #{pane_dead}".into(),
            stderr: format!("invalid pane runtime: {value}"),
            exit: -1,
        }));
    };
    let pid = pid
        .parse::<i32>()
        .map_err(|_| CcbdError::from(TmuxError::ParsePid(pid.to_string())))?;
    let dead = match dead {
        "0" => false,
        "1" => true,
        _ => {
            return Err(CcbdError::from(TmuxError::CommandFailed {
                cmd: "tmux display-message #{pane_pid} #{pane_dead}".into(),
                stderr: format!("invalid pane_dead value: {dead}"),
                exit: -1,
            }));
        }
    };
    Ok(TmuxPaneRuntime { pid, dead })
}

#[cfg(test)]
pub(crate) fn parse_pane_pid_for_test(value: &str) -> Result<i32, CcbdError> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| CcbdError::from(TmuxError::ParsePid(value.trim().to_string())))
}

#[cfg(test)]
pub(crate) fn parse_pane_runtime_for_test(value: &str) -> Result<TmuxPaneRuntime, CcbdError> {
    parse_pane_runtime(value)
}

#[cfg(test)]
mod tests {
    use super::{TmuxServer, TmuxWindowSize};
    use std::process::Command;

    fn require_tmux() {
        which::which("tmux").expect("tmux binary required for tmux session tests");
    }

    fn cleanup_server(server: &TmuxServer) {
        let _ = Command::new("tmux")
            .args(["-L", server.socket_name(), "kill-server"])
            .output();
    }

    #[test]
    fn test_new_session_args_keep_fixed_geometry_only_for_fixed_mode() {
        let fixed = super::new_session_args("sock", "sess", "/tmp/project", TmuxWindowSize::Fixed);
        assert_eq!(
            fixed,
            vec![
                "-L",
                "sock",
                "new-session",
                "-d",
                "-s",
                "sess",
                "-c",
                "/tmp/project",
                "-x",
                "150",
                "-y",
                "60"
            ]
        );

        let follow =
            super::new_session_args("sock", "sess", "/tmp/project", TmuxWindowSize::Follow);
        assert_eq!(
            follow,
            vec![
                "-L",
                "sock",
                "new-session",
                "-d",
                "-s",
                "sess",
                "-c",
                "/tmp/project"
            ]
        );
    }

    #[test]
    fn pane_runtime_parser_distinguishes_live_and_retained_dead_panes() {
        assert_eq!(
            super::parse_pane_runtime_for_test("123\t0").unwrap(),
            super::TmuxPaneRuntime {
                pid: 123,
                dead: false,
            }
        );
        assert_eq!(
            super::parse_pane_runtime_for_test("456\t1\n").unwrap(),
            super::TmuxPaneRuntime {
                pid: 456,
                dead: true,
            }
        );
        assert!(super::parse_pane_runtime_for_test("123\tmaybe").is_err());
        assert!(super::parse_pane_runtime_for_test("missing-dead-flag").is_err());
    }

    #[test]
    fn test_kill_session_sync_removes_session() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());
        let session_name = "t1-1-2-kill-session-sync";

        let result = (|| {
            server.ensure_session_sync(session_name, tmp.path())?;
            server.kill_session_sync(session_name)?;
            let has_session = Command::new("tmux")
                .args([
                    "-L",
                    server.socket_name(),
                    "has-session",
                    "-t",
                    session_name,
                ])
                .output()
                .map_err(super::map_command_io_error)?;
            assert!(
                !has_session.status.success(),
                "tmux session should be gone after kill-session"
            );
            Ok::<(), crate::error::CcbdError>(())
        })();

        cleanup_server(&server);
        result.unwrap();
    }

    #[test]
    fn find_pane_in_session_by_pid_returns_none_when_no_pane_matches() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());
        let session_name = "t-find-pane-no-match";

        let result = (|| {
            server.ensure_session_sync(session_name, tmp.path())?;
            let pane = server.spawn_window_sync(
                session_name,
                "live",
                tmp.path(),
                &["sh", "-lc", "sleep 60"],
            )?;
            let live_pid = server.get_pane_pid_sync(&pane)?;
            assert!(
                server
                    .find_pane_in_session_by_pid_sync(session_name, i64::from(live_pid) + 100_000)
                    .is_none()
            );
            assert!(server.session_exists_sync(session_name)?);
            Ok::<(), crate::error::CcbdError>(())
        })();

        cleanup_server(&server);
        result.unwrap();
    }

    #[test]
    fn select_single_matching_pane_returns_none_for_duplicate_pid_matches() {
        let matches = vec![
            (super::TmuxPaneId("%1".to_string()), 42),
            (super::TmuxPaneId("%2".to_string()), 42),
        ];
        assert!(super::select_single_matching_pane("s", 42, matches).is_none());
    }

    #[test]
    fn test_ensure_session_sync_locks_size_manual() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());
        let session_name = "t1-2-1-pty-lock";

        let result = (|| {
            server.ensure_session_sync(session_name, tmp.path())?;
            let size = Command::new("tmux")
                .args([
                    "-L",
                    server.socket_name(),
                    "display-message",
                    "-t",
                    session_name,
                    "-p",
                    "#{window_width}x#{window_height}",
                ])
                .output()
                .map_err(super::map_command_io_error)?;
            let size = super::ensure_output_success(
                "tmux",
                &[
                    "-L",
                    server.socket_name(),
                    "display-message",
                    "-t",
                    session_name,
                    "-p",
                    "#{window_width}x#{window_height}",
                ],
                &[],
                size,
            )?;
            assert_eq!(size.trim(), "150x60");

            let option = Command::new("tmux")
                .args([
                    "-L",
                    server.socket_name(),
                    "show-options",
                    "-t",
                    session_name,
                    "window-size",
                ])
                .output()
                .map_err(super::map_command_io_error)?;
            let option = super::ensure_output_success(
                "tmux",
                &[
                    "-L",
                    server.socket_name(),
                    "show-options",
                    "-t",
                    session_name,
                    "window-size",
                ],
                &[],
                option,
            )?;
            assert_eq!(option.trim(), "window-size manual");
            Ok::<(), crate::error::CcbdError>(())
        })();

        cleanup_server(&server);
        result.unwrap();
    }

    #[test]
    fn test_ensure_session_sync_follow_uses_latest_window_size() {
        require_tmux();
        let tmp = tempfile::tempdir().unwrap();
        let server = TmuxServer::new(tmp.path());
        let session_name = "t1-2-2-pty-follow";

        let result = (|| {
            server.ensure_session_with_window_size_sync(
                session_name,
                tmp.path(),
                TmuxWindowSize::Follow,
            )?;
            let option = Command::new("tmux")
                .args([
                    "-L",
                    server.socket_name(),
                    "show-options",
                    "-t",
                    session_name,
                    "window-size",
                ])
                .output()
                .map_err(super::map_command_io_error)?;
            let option = super::ensure_output_success(
                "tmux",
                &[
                    "-L",
                    server.socket_name(),
                    "show-options",
                    "-t",
                    session_name,
                    "window-size",
                ],
                &[],
                option,
            )?;
            assert_eq!(option.trim(), "window-size latest");
            Ok::<(), crate::error::CcbdError>(())
        })();

        cleanup_server(&server);
        result.unwrap();
    }
}
