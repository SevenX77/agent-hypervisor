use ah::error::AhError;
use ah::marker::MarkerMatcher;
use ah::prompt_handler::{
    PromptAction, PromptIo, PromptKb, PromptRunOutcome, RunnerContext, default_cases,
    handle_prompt_chain,
};
use ah::provider::manifest::get_manifest;
use ah::tmux::TmuxPaneId;
use std::sync::Mutex;
use std::time::Duration;

const CODEX_UPDATE_PROMPT: &str = "\
✨ Update available! 0.135.0 -> 0.139.0
Release notes: https://github.com/openai/codex/releases/latest
› 1. Update now (runs `npm install -g @openai/codex`)
  2. Skip
  3. Skip until next version
  Press enter to continue
";

const TRUST_PROMPT: &str = "\
Do you trust this directory?

1) Yes, trust this workspace
2) No, exit
";

const READY_PROMPT: &str = "\
worktree clean
  ›
  gpt-5.5 default · ~/coding/agent-hypervisor
";

const READY_WITH_PROBE: &str = "\
worktree clean
  › x
  gpt-5.5 default · ~/coding/agent-hypervisor
";

const CLAUDE_READY_WITH_STATUS_NOISE: &str = "\
model: Sonnet
status text with x in context
❯";

const CLAUDE_READY_WITH_INPUT_PROBE_AND_STATUS_NOISE: &str = "\
model: Sonnet
status text with x in context
❯ x";

struct ScriptedPromptIo {
    captures: Mutex<Vec<String>>,
    sent: Mutex<Vec<String>>,
}

impl ScriptedPromptIo {
    fn new(captures: &[&str]) -> Self {
        Self {
            captures: Mutex::new(
                captures
                    .iter()
                    .rev()
                    .map(|item| (*item).to_string())
                    .collect(),
            ),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn sent(&self) -> Vec<String> {
        self.sent.lock().expect("sent lock").clone()
    }
}

impl PromptIo for ScriptedPromptIo {
    fn capture_pane(&self, _pane: &TmuxPaneId) -> Result<String, AhError> {
        self.captures
            .lock()
            .expect("captures lock")
            .pop()
            .ok_or_else(|| AhError::TmuxCommandFailed {
                cmd: "capture-pane".to_string(),
                stderr: "no scripted capture left".to_string(),
                exit: 1,
            })
    }

    fn send_key_literal(&self, _pane: &TmuxPaneId, value: &str) -> Result<(), AhError> {
        self.sent
            .lock()
            .expect("sent lock")
            .push(format!("literal:{value}"));
        Ok(())
    }

    fn send_key_keysym(&self, _pane: &TmuxPaneId, value: &str) -> Result<(), AhError> {
        self.sent
            .lock()
            .expect("sent lock")
            .push(format!("key:{value}"));
        Ok(())
    }
}

fn run_codex_prompt_loop(io: &ScriptedPromptIo, kb: &PromptKb) -> PromptRunOutcome {
    run_provider_prompt_loop("codex", io, kb)
}

fn run_provider_prompt_loop(
    provider: &str,
    io: &ScriptedPromptIo,
    kb: &PromptKb,
) -> PromptRunOutcome {
    let pane = TmuxPaneId("%pr4a".to_string());
    let marker = MarkerMatcher::from_manifest(&get_manifest(provider));
    let ctx = RunnerContext::new("ag_pr4a", &pane, provider, io, kb)
        .with_marker_matcher(&marker)
        .with_action_settle_delay(Duration::ZERO);
    handle_prompt_chain(ctx, 5)
}

#[test]
fn pr4a_repeated_dialog_rechecks_outcome_and_does_not_refire_after_ready() {
    let io = ScriptedPromptIo::new(&[
        CODEX_UPDATE_PROMPT,
        CODEX_UPDATE_PROMPT,
        READY_PROMPT,
        READY_WITH_PROBE,
        READY_PROMPT,
    ]);
    let kb = PromptKb::new(default_cases());

    let _outcome = run_codex_prompt_loop(&io, &kb);

    assert_eq!(
        io.sent(),
        vec!["key:Down", "key:Enter", "literal:x", "key:BSpace"],
        "PR4a must re-evaluate outcome before refiring the same dialog action; once ready, \
         can-input is confirmed by a safe probe and immediately cleaned up with BSpace"
    );
}

#[test]
fn pr4a_dialog_capture_never_gets_naked_can_input_probe() {
    let io = ScriptedPromptIo::new(&[TRUST_PROMPT, READY_PROMPT, READY_WITH_PROBE, READY_PROMPT]);
    let kb = PromptKb::new(default_cases());

    let _outcome = run_codex_prompt_loop(&io, &kb);

    assert_eq!(
        io.sent(),
        vec!["key:1", "key:Enter", "literal:x", "key:BSpace"],
        "the active dialog must be answered before the can-input probe is attempted"
    );
}

#[test]
fn pr4a_ready_prompt_uses_probe_echo_then_bspace_before_idle() {
    let io = ScriptedPromptIo::new(&[READY_PROMPT, READY_WITH_PROBE, READY_PROMPT]);
    let kb = PromptKb::new(default_cases());

    let _outcome = run_codex_prompt_loop(&io, &kb);

    assert_eq!(
        io.sent(),
        vec!["literal:x", "key:BSpace"],
        "marker-only readiness is insufficient for PR4a; idle requires echo confirmation \
         and probe cleanup"
    );
}

#[test]
fn pr4a_claude_probe_echo_is_anchored_to_input_line_not_status_noise() {
    let io = ScriptedPromptIo::new(&[
        CLAUDE_READY_WITH_STATUS_NOISE,
        CLAUDE_READY_WITH_INPUT_PROBE_AND_STATUS_NOISE,
        CLAUDE_READY_WITH_STATUS_NOISE,
    ]);
    let kb = PromptKb::new(default_cases());

    let outcome = run_provider_prompt_loop("claude", &io, &kb);

    assert!(
        matches!(outcome, PromptRunOutcome::NoActionNeeded { depth: 0 }),
        "Claude status/model text containing x must not make probe cleanup look dirty: {outcome:?}; sent={:?}",
        io.sent()
    );
    assert_eq!(io.sent(), vec!["literal:x", "key:BSpace"]);
}

#[test]
fn pr4a_claude_non_input_screen_does_not_receive_probe_literal() {
    let io = ScriptedPromptIo::new(&["model: Sonnet\nloading context x\n"]);
    let kb = PromptKb::new(default_cases());

    let outcome = run_provider_prompt_loop("claude", &io, &kb);

    assert!(
        matches!(outcome, PromptRunOutcome::Pending { depth: 0, .. }),
        "non-input Claude screen should not be confirmed ready: {outcome:?}"
    );
    assert!(io.sent().is_empty());
}

#[test]
fn pr4a_idle_marker_without_probe_confirmation_is_pending_not_ready() {
    let io = ScriptedPromptIo::new(&[READY_PROMPT, READY_PROMPT]);
    let kb = PromptKb::new(default_cases());

    let outcome = run_codex_prompt_loop(&io, &kb);

    assert!(
        matches!(outcome, PromptRunOutcome::Pending { depth: 0, .. }),
        "IdleMarker without probe echo confirmation must not be treated as ready: {outcome:?}"
    );
    assert_eq!(io.sent(), vec!["literal:x", "key:BSpace"]);
}

#[test]
fn pr4a_bspace_is_valid_prompt_key_for_probe_cleanup() {
    let action = PromptAction::Key {
        value: "BSpace".to_string(),
    };

    assert!(
        action.validate().is_ok(),
        "BSpace must be whitelisted because the can-input probe cleanup is part of the \
         prompt action contract"
    );
}
