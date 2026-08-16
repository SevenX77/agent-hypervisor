use crate::error::AhError;
use crate::tmux::{TmuxPaneId, TmuxServer};
use std::sync::Arc;
use std::time::Duration;

pub async fn send_text_to_pane(
    tmux: Arc<TmuxServer>,
    agent_id: &str,
    pane: TmuxPaneId,
    text: String,
) -> Result<(), AhError> {
    send_text_to_pane_with_options(tmux, agent_id, pane, text, true).await
}

pub async fn send_text_to_pane_with_options(
    tmux: Arc<TmuxServer>,
    agent_id: &str,
    pane: TmuxPaneId,
    text: String,
    press_enter_after_paste: bool,
) -> Result<(), AhError> {
    if is_single_line_slash_command(&text) {
        return send_slash_command_keystroke(tmux, pane, &text).await;
    }

    let buffer_name = format!("ah-buf-{}", sanitize_buffer_name(agent_id));
    tmux.load_buffer(buffer_name.clone(), text).await?;
    let paste_result = tmux.paste_buffer(pane.clone(), buffer_name.clone()).await;

    if let Err(err) = tmux.delete_buffer(buffer_name).await {
        tracing::warn!(agent_id, error = %err, "failed to delete tmux paste buffer");
    }

    paste_result?;

    if !press_enter_after_paste {
        return Ok(());
    }

    // mvp12 M12.6 R-1 follow-up: 1:1 with Python `terminal_runtime/tmux_send.py:135-137`.
    // Python sleeps `AH_TMUX_ENTER_DELAY` (default 0.5s) between paste-buffer and send-keys Enter
    // so the provider TUI finishes rendering the pasted text before Enter triggers submit.
    // Rust mvp12 R-1 deleted the trailing `\n` conditional but missed this delay; some provider
    // TUIs swallow Enter when it hits the input loop before the paste settles.
    let enter_delay_s = env_float("AH_TMUX_ENTER_DELAY", 0.5);
    if enter_delay_s > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(enter_delay_s)).await;
    }
    tmux.send_enter(pane.clone()).await?;

    // Second-Enter fallback for cold-start CLIs that swallow the first Enter
    // (bracketed-paste race, input loop not yet ready). Default disabled (0).
    // Mirrors Python tmux_send.py:142-150.
    let second_enter_delay_s = env_float("AH_TMUX_SECOND_ENTER_DELAY", 0.0);
    if second_enter_delay_s > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(second_enter_delay_s)).await;
        tmux.send_enter(pane).await?;
    }

    Ok(())
}

pub async fn send_slash_command_keystroke(
    tmux: Arc<TmuxServer>,
    pane: TmuxPaneId,
    slash_cmd: &str,
) -> Result<(), AhError> {
    for ch in slash_cmd.chars() {
        tmux.send_keys_literal(pane.clone(), ch.to_string()).await?;
    }
    tmux.send_enter(pane).await?;
    Ok(())
}

fn is_single_line_slash_command(text: &str) -> bool {
    text.starts_with('/') && !text.contains('\n') && !text.contains('\r') && !text.trim().is_empty()
}

fn env_float(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(default)
}

fn sanitize_buffer_name(agent_id: &str) -> String {
    agent_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{is_single_line_slash_command, sanitize_buffer_name};

    #[test]
    fn test_sanitize_buffer_name_preserves_safe_agent_id_chars() {
        assert_eq!(sanitize_buffer_name("ag_foo-123"), "ag_foo-123");
    }

    #[test]
    fn test_sanitize_buffer_name_replaces_tmux_unsafe_chars() {
        assert_eq!(sanitize_buffer_name("ag/foo:bar"), "ag_foo_bar");
    }

    #[test]
    fn test_is_single_line_slash_command_accepts_slash_commands() {
        assert!(is_single_line_slash_command("/clear"));
        assert!(is_single_line_slash_command("/new"));
        assert!(is_single_line_slash_command("/help"));
    }

    #[test]
    fn test_is_single_line_slash_command_rejects_multiline_or_non_slash() {
        assert!(!is_single_line_slash_command("hello"));
        assert!(!is_single_line_slash_command("/clear\nsecond line"));
        assert!(!is_single_line_slash_command("/clear\r"));
        assert!(!is_single_line_slash_command(""));
    }
}
