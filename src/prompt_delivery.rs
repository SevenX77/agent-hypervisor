//! Verified delivery of one prompt through an interactive provider CLI.
//!
//! Terminal evidence is used only for effects the terminal can prove: paste
//! materialization and composer clearing. Provider turn start and completion
//! remain owned by hook/transcript observations.

use super::guarded_action::{
    ActionAssessment, GuardedActionError, GuardedActionOutcome, run_guarded_action,
};
use crate::error::AhError;
use crate::provider::{ProviderPromptKind, ProviderTerminalControlSpec};
use crate::tmux::{TmuxPaneId, TmuxServer};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct PromptDeliveryReceipt {
    pub paste_observations: u64,
    pub submit_observations: Option<u64>,
    pub submit_attempts: u8,
}

pub async fn deliver_prompt(
    tmux: Arc<TmuxServer>,
    agent_id: &str,
    provider: &str,
    pane: TmuxPaneId,
    text: String,
    press_enter_after_paste: bool,
) -> Result<PromptDeliveryReceipt, AhError> {
    let adapter =
        crate::provider::adapter(provider).ok_or_else(|| AhError::EnvironmentNotSupported {
            details: format!("unknown provider {provider:?}"),
        })?;
    let terminal_spec = adapter.terminal_control_spec();
    if text.trim().is_empty() {
        return Err(AhError::PtyIoError(
            "refused to deliver an empty provider prompt".to_string(),
        ));
    }
    if adapter.prompt_kind() == ProviderPromptKind::ShellCommand {
        return deliver_shell_command(tmux, agent_id, pane, text, press_enter_after_paste).await;
    }
    if is_single_line_slash_command(&text) {
        return deliver_slash_command(tmux, pane, &text, terminal_spec).await;
    }

    let timeout = action_timeout();
    let poll = action_poll_interval();
    let buffer_name = format!("ah-buf-{}", sanitize_buffer_name(agent_id));
    tmux.load_buffer(buffer_name.clone(), text.clone()).await?;

    let paste = run_guarded_action(
        "paste_prompt",
        timeout,
        poll,
        {
            let tmux = tmux.clone();
            let pane = pane.clone();
            move || {
                let tmux = tmux.clone();
                let pane = pane.clone();
                async move { tmux.capture_pane(pane).await }
            }
        },
        {
            let tmux = tmux.clone();
            let pane = pane.clone();
            let buffer_name = buffer_name.clone();
            move || async move {
                let result = tmux.paste_buffer(pane, buffer_name.clone()).await;
                if let Err(err) = tmux.delete_buffer(buffer_name).await {
                    tracing::warn!(error = %err, "failed to delete tmux paste buffer");
                }
                result
            }
        },
        {
            let text = text.clone();
            move |before, after| assess_paste(&before.value, &after.value, &text, terminal_spec)
        },
    )
    .await
    .map_err(action_error)?;

    if !press_enter_after_paste {
        return Ok(PromptDeliveryReceipt {
            paste_observations: paste.observations_examined,
            submit_observations: None,
            submit_attempts: 0,
        });
    }

    let first_submit = submit_once(
        tmux.clone(),
        pane.clone(),
        text.clone(),
        terminal_spec,
        timeout,
        poll,
    )
    .await;
    let (submit, submit_attempts) = match first_submit {
        Ok(outcome) => (outcome, 1),
        Err(first_error)
            if safe_to_repeat_enter(tmux.clone(), pane.clone(), &text, terminal_spec).await? =>
        {
            tracing::warn!(
                agent_id,
                reason = %first_error,
                "submit was not confirmed while the prompt remained visible; retrying Enter once"
            );
            (
                submit_once(tmux, pane, text, terminal_spec, timeout, poll)
                    .await
                    .map_err(action_error)?,
                2,
            )
        }
        Err(error) => return Err(action_error(error)),
    };

    Ok(PromptDeliveryReceipt {
        paste_observations: paste.observations_examined,
        submit_observations: Some(submit.observations_examined),
        submit_attempts,
    })
}

/// Bash is a line-oriented shell rather than a provider-owned TUI. While a
/// command is running, tmux retains the submitted `$ command` line and exposes
/// no active-composer marker that distinguishes it from history. Preserve
/// AH's established paste/Enter behavior for this adapter; lifecycle and turn
/// state are still governed by the Bash process/terminal observations.
async fn deliver_shell_command(
    tmux: Arc<TmuxServer>,
    agent_id: &str,
    pane: TmuxPaneId,
    text: String,
    press_enter_after_paste: bool,
) -> Result<PromptDeliveryReceipt, AhError> {
    if is_single_line_slash_command(&text) {
        for ch in text.chars() {
            tmux.send_keys_literal(pane.clone(), ch.to_string()).await?;
        }
        if press_enter_after_paste {
            tmux.send_enter(pane).await?;
        }
        return Ok(PromptDeliveryReceipt {
            paste_observations: 0,
            submit_observations: press_enter_after_paste.then_some(0),
            submit_attempts: u8::from(press_enter_after_paste),
        });
    }

    let buffer_name = format!("ah-buf-{}", sanitize_buffer_name(agent_id));
    tmux.load_buffer(buffer_name.clone(), text).await?;
    let paste_result = tmux.paste_buffer(pane.clone(), buffer_name.clone()).await;
    if let Err(err) = tmux.delete_buffer(buffer_name).await {
        tracing::warn!(agent_id, error = %err, "failed to delete tmux paste buffer");
    }
    paste_result?;

    if !press_enter_after_paste {
        return Ok(PromptDeliveryReceipt {
            paste_observations: 0,
            submit_observations: None,
            submit_attempts: 0,
        });
    }

    let enter_delay_s = env_float("AH_TMUX_ENTER_DELAY", 0.5);
    if enter_delay_s > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(enter_delay_s)).await;
    }
    tmux.send_enter(pane.clone()).await?;

    let second_enter_delay_s = env_float("AH_TMUX_SECOND_ENTER_DELAY", 0.0);
    let mut submit_attempts = 1;
    if second_enter_delay_s > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(second_enter_delay_s)).await;
        tmux.send_enter(pane).await?;
        submit_attempts = 2;
    }

    Ok(PromptDeliveryReceipt {
        paste_observations: 0,
        submit_observations: Some(0),
        submit_attempts,
    })
}

async fn submit_once(
    tmux: Arc<TmuxServer>,
    pane: TmuxPaneId,
    text: String,
    terminal_spec: &'static ProviderTerminalControlSpec,
    timeout: Duration,
    poll: Duration,
) -> Result<GuardedActionOutcome<String>, GuardedActionError<AhError>> {
    run_guarded_action(
        "submit_prompt",
        timeout,
        poll,
        {
            let tmux = tmux.clone();
            let pane = pane.clone();
            move || {
                let tmux = tmux.clone();
                let pane = pane.clone();
                async move { tmux.capture_pane(pane).await }
            }
        },
        move || {
            let tmux = tmux.clone();
            let pane = pane.clone();
            async move { tmux.send_enter(pane).await }
        },
        move |before, after| assess_submit(&before.value, &after.value, &text, terminal_spec),
    )
    .await
}

async fn safe_to_repeat_enter(
    tmux: Arc<TmuxServer>,
    pane: TmuxPaneId,
    text: &str,
    terminal_spec: &'static ProviderTerminalControlSpec,
) -> Result<bool, AhError> {
    let capture = tmux.capture_pane(pane).await?;
    Ok(composer_contains_prompt(&capture, text, terminal_spec)
        || contains_paste_expand_guard(&capture, terminal_spec))
}

async fn deliver_slash_command(
    tmux: Arc<TmuxServer>,
    pane: TmuxPaneId,
    slash_command: &str,
    terminal_spec: &'static ProviderTerminalControlSpec,
) -> Result<PromptDeliveryReceipt, AhError> {
    let timeout = action_timeout();
    let poll = action_poll_interval();
    let mut typed = String::new();
    let mut paste_observations = 0_u64;
    for ch in slash_command.chars() {
        typed.push(ch);
        let expected = typed.clone();
        let outcome = run_guarded_action(
            "type_slash_command_character",
            timeout,
            poll,
            {
                let tmux = tmux.clone();
                let pane = pane.clone();
                move || {
                    let tmux = tmux.clone();
                    let pane = pane.clone();
                    async move { tmux.capture_pane(pane).await }
                }
            },
            {
                let tmux = tmux.clone();
                let pane = pane.clone();
                move || async move { tmux.send_keys_literal(pane, ch.to_string()).await }
            },
            move |_before, after| {
                if composer_contains_prompt(&after.value, &expected, terminal_spec) {
                    ActionAssessment::Confirmed
                } else {
                    ActionAssessment::Mismatch {
                        reason: format!("composer does not contain typed prefix {expected:?}"),
                    }
                }
            },
        )
        .await
        .map_err(action_error)?;
        paste_observations += outcome.observations_examined;
    }

    let submit = submit_once(
        tmux,
        pane,
        slash_command.to_string(),
        terminal_spec,
        timeout,
        poll,
    )
    .await
    .map_err(action_error)?;
    Ok(PromptDeliveryReceipt {
        paste_observations,
        submit_observations: Some(submit.observations_examined),
        submit_attempts: 1,
    })
}

fn assess_paste(
    before: &str,
    after: &str,
    prompt: &str,
    terminal_spec: &ProviderTerminalControlSpec,
) -> ActionAssessment {
    if composer_contains_prompt(after, prompt, terminal_spec) {
        if composer_contains_prompt(before, prompt, terminal_spec)
            && normalized_terminal(before) == normalized_terminal(after)
        {
            return ActionAssessment::Mismatch {
                reason: "prompt was already visible before paste; no causal change observed".into(),
            };
        }
        return ActionAssessment::Confirmed;
    }
    if contains_collapsed_paste_receipt(after, terminal_spec)
        && !contains_collapsed_paste_receipt(before, terminal_spec)
    {
        return ActionAssessment::Confirmed;
    }
    let observed = normalized_terminal(&composer_region(after, terminal_spec));
    ActionAssessment::Mismatch {
        reason: format!(
            "pasted prompt is not visible in the provider composer; observed {:?}",
            truncate_diagnostic(&observed, 240)
        ),
    }
}

fn assess_submit(
    before: &str,
    after: &str,
    prompt: &str,
    terminal_spec: &ProviderTerminalControlSpec,
) -> ActionAssessment {
    if composer_contains_prompt(after, prompt, terminal_spec) {
        return ActionAssessment::Mismatch {
            reason: "prompt remains visible in the provider composer".into(),
        };
    }
    if contains_paste_expand_guard(after, terminal_spec) {
        return ActionAssessment::Mismatch {
            reason: "provider is still waiting for Enter at the paste expansion guard".into(),
        };
    }
    if normalized_terminal(before) == normalized_terminal(after) {
        return ActionAssessment::Mismatch {
            reason: "pane did not change after Enter".into(),
        };
    }
    ActionAssessment::Confirmed
}

fn composer_contains_prompt(
    capture: &str,
    prompt: &str,
    terminal_spec: &ProviderTerminalControlSpec,
) -> bool {
    let needle = normalized_terminal(prompt);
    !needle.is_empty()
        && normalized_terminal(&composer_region(capture, terminal_spec)).contains(&needle)
}

fn contains_paste_expand_guard(capture: &str, terminal_spec: &ProviderTerminalControlSpec) -> bool {
    let composer = normalized_terminal(&composer_region(capture, terminal_spec));
    terminal_spec
        .paste_expand_guards
        .iter()
        .any(|guard| composer.contains(guard))
}

fn contains_collapsed_paste_receipt(
    capture: &str,
    terminal_spec: &ProviderTerminalControlSpec,
) -> bool {
    let composer = normalized_terminal(&composer_region(capture, terminal_spec));
    contains_paste_expand_guard(capture, terminal_spec)
        || (terminal_spec
            .collapsed_paste_markers
            .iter()
            .any(|marker| composer.contains(marker))
            && composer.chars().any(|ch| ch.is_ascii_digit()))
}

fn composer_region(capture: &str, terminal_spec: &ProviderTerminalControlSpec) -> String {
    // Full-height tmux panes can contain dozens of blank rows after a
    // provider-owned composer. Ignore those rows before applying the bounded
    // tail window, otherwise a tall pane can hide the active input entirely.
    let mut lines = capture
        .trim_end()
        .lines()
        .rev()
        .take(terminal_spec.composer_tail_lines)
        .collect::<Vec<_>>();
    lines.reverse();
    let start = lines
        .iter()
        .rposition(|line| {
            let normalized = normalized_terminal(line);
            terminal_spec
                .composer_start_markers
                .iter()
                .any(|marker| normalized.starts_with(marker))
        })
        .unwrap_or(0);
    lines[start..].join("\n")
}

fn normalized_terminal(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn truncate_diagnostic(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn action_error(error: GuardedActionError<AhError>) -> AhError {
    AhError::PtyIoError(error.to_string())
}

fn action_timeout() -> Duration {
    env_duration_ms("AH_ACTION_CONFIRM_TIMEOUT_MS", DEFAULT_ACTION_TIMEOUT)
}

fn action_poll_interval() -> Duration {
    env_duration_ms("AH_ACTION_CONFIRM_POLL_MS", DEFAULT_POLL_INTERVAL)
}

fn env_duration_ms(key: &str, default: Duration) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn env_float(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn is_single_line_slash_command(text: &str) -> bool {
    text.starts_with('/') && !text.contains('\n') && !text.contains('\r') && !text.trim().is_empty()
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
    use super::*;

    fn codex_terminal_spec() -> &'static ProviderTerminalControlSpec {
        crate::provider::adapter("codex")
            .unwrap()
            .terminal_control_spec()
    }

    #[test]
    fn paste_requires_visible_or_new_collapsed_composer_evidence() {
        assert_eq!(
            assess_paste(
                "ready\n> ",
                "ready\n> explain this change",
                "explain this change",
                codex_terminal_spec(),
            ),
            ActionAssessment::Confirmed
        );
        assert_eq!(
            assess_paste(
                "ready\n> ",
                "ready\n[Pasted text 412 chars]\npaste again to expand",
                "long prompt",
                codex_terminal_spec(),
            ),
            ActionAssessment::Confirmed
        );
        assert!(matches!(
            assess_paste("ready", "ready", "missing", codex_terminal_spec()),
            ActionAssessment::Mismatch { .. }
        ));
    }

    #[test]
    fn submit_requires_prompt_to_leave_composer_and_pane_to_advance() {
        assert!(matches!(
            assess_submit(
                "ready\n> do it",
                "ready\n> do it",
                "do it",
                codex_terminal_spec(),
            ),
            ActionAssessment::Mismatch { .. }
        ));
        assert_eq!(
            assess_submit(
                "ready\n> do it",
                "Working (1s)\nesc to interrupt",
                "do it",
                codex_terminal_spec(),
            ),
            ActionAssessment::Confirmed
        );
    }

    #[test]
    fn historical_prompt_text_outside_composer_does_not_block_submit() {
        let historical = format!(
            "do it\n{}\n> ",
            (0..45).map(|_| "old line").collect::<Vec<_>>().join("\n")
        );
        assert!(!composer_contains_prompt(
            &historical,
            "do it",
            codex_terminal_spec()
        ));
    }

    #[test]
    fn latest_composer_marker_excludes_submitted_prompt_history() {
        let capture = "› Reply with exactly: codex-ok\nWorking (4s)\n› Explain this codebase";
        assert!(!composer_contains_prompt(
            capture,
            "Reply with exactly: codex-ok",
            codex_terminal_spec()
        ));
    }

    #[test]
    fn blank_rows_in_a_tall_pane_do_not_hide_the_composer() {
        let capture = format!("> do it\nstatus line\n{}", "\n".repeat(60));
        assert!(composer_contains_prompt(
            &capture,
            "do it",
            crate::provider::adapter("antigravity")
                .unwrap()
                .terminal_control_spec()
        ));
    }

    #[test]
    fn shell_commands_use_the_legacy_line_oriented_delivery_contract() {
        assert_eq!(
            crate::provider::adapter("bash").unwrap().prompt_kind(),
            ProviderPromptKind::ShellCommand
        );
        for provider in ["codex", "claude", "antigravity"] {
            assert_eq!(
                crate::provider::adapter(provider).unwrap().prompt_kind(),
                ProviderPromptKind::NaturalLanguage
            );
        }
    }
}
