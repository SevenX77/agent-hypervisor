use crate::error::AhError;
use crate::tmux::{TmuxPaneId, TmuxServer};
use std::sync::Arc;

pub async fn send_text_to_pane(
    tmux: Arc<TmuxServer>,
    agent_id: &str,
    provider: &str,
    pane: TmuxPaneId,
    text: String,
) -> Result<(), AhError> {
    send_text_to_pane_with_options(tmux, agent_id, provider, pane, text, true).await
}

pub async fn send_text_to_pane_with_options(
    tmux: Arc<TmuxServer>,
    agent_id: &str,
    provider: &str,
    pane: TmuxPaneId,
    text: String,
    press_enter_after_paste: bool,
) -> Result<(), AhError> {
    crate::prompt_delivery::deliver_prompt(
        tmux,
        agent_id,
        provider,
        pane,
        text,
        press_enter_after_paste,
    )
    .await?;
    Ok(())
}

pub async fn send_slash_command_keystroke(
    tmux: Arc<TmuxServer>,
    provider: &str,
    pane: TmuxPaneId,
    slash_cmd: &str,
) -> Result<(), AhError> {
    crate::prompt_delivery::deliver_prompt(
        tmux,
        "slash-command",
        provider,
        pane,
        slash_cmd.to_string(),
        true,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
fn is_single_line_slash_command(text: &str) -> bool {
    text.starts_with('/') && !text.contains('\n') && !text.contains('\r') && !text.trim().is_empty()
}

#[cfg(test)]
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
