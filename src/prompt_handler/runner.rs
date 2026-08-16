//! Recursive prompt-handler runner that executes known prompt actions.

use crate::db::prompt_experience::{
    NewPromptExperience, PromptExperienceLookup, PromptFingerprintType,
};
use crate::error::CcbdError;
use crate::marker::MarkerMatcher;
use crate::prompt_handler::gating::{GateContext, PromptGateDecision, classify_capture};
use crate::prompt_handler::llm_client::LlmClassifier;
use crate::prompt_handler::matcher::{PromptScanPurpose, sanitize_pane_text};
use crate::prompt_handler::schema::{PromptAction, PromptKb, ValidatedAction};
use crate::tmux::{TmuxPaneId, TmuxServer};
use std::time::Duration;

const DEFAULT_ACTION_SETTLE_DELAY: Duration = Duration::from_millis(200);
const ACTION_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(2);
const CAN_INPUT_PROBE_CAPTURE_ATTEMPTS: usize = 3;
const UNKNOWN_STABLE_SCAN_THRESHOLD: usize = 3;

pub trait PromptIo: Send + Sync {
    fn capture_pane(&self, pane_id: &TmuxPaneId) -> Result<String, CcbdError>;
    fn capture_control_state(&self, pane_id: &TmuxPaneId) -> Result<String, CcbdError> {
        self.capture_pane(pane_id)
    }
    fn send_key_literal(&self, pane_id: &TmuxPaneId, value: &str) -> Result<(), CcbdError>;
    fn send_key_keysym(&self, pane_id: &TmuxPaneId, value: &str) -> Result<(), CcbdError>;
}

pub struct TmuxPromptIo {
    tmux: TmuxServer,
}

impl TmuxPromptIo {
    pub fn new(tmux: TmuxServer) -> Self {
        Self { tmux }
    }
}

impl PromptIo for TmuxPromptIo {
    fn capture_pane(&self, pane_id: &TmuxPaneId) -> Result<String, CcbdError> {
        self.tmux.capture_pane_sync(pane_id)
    }

    fn capture_control_state(&self, pane_id: &TmuxPaneId) -> Result<String, CcbdError> {
        self.tmux.capture_pane_control_sync(pane_id)
    }

    fn send_key_literal(&self, pane_id: &TmuxPaneId, value: &str) -> Result<(), CcbdError> {
        self.tmux.send_keys_literal_sync(pane_id, value)
    }

    fn send_key_keysym(&self, pane_id: &TmuxPaneId, value: &str) -> Result<(), CcbdError> {
        self.tmux.send_keys_keysym_sync(pane_id, value)
    }
}

pub struct RunnerContext<'a> {
    pub agent_id: &'a str,
    pub pane_id: &'a TmuxPaneId,
    pub provider: &'a str,
    pub current_state: &'a str,
    pub tmux_session_handle: &'a dyn PromptIo,
    pub kb_handle: &'a PromptKb,
    pub marker_matcher: Option<&'a MarkerMatcher>,
    pub prompt_experience: Option<&'a dyn PromptExperienceLookup>,
    pub llm_classifier: Option<&'a dyn LlmClassifier>,
    pub scan_purpose: PromptScanPurpose,
    pub action_settle_delay: Duration,
}

impl<'a> RunnerContext<'a> {
    pub fn new(
        agent_id: &'a str,
        pane_id: &'a TmuxPaneId,
        provider: &'a str,
        tmux_session_handle: &'a dyn PromptIo,
        kb_handle: &'a PromptKb,
    ) -> Self {
        Self {
            agent_id,
            pane_id,
            provider,
            current_state: crate::db::state_machine::STATE_BUSY,
            tmux_session_handle,
            kb_handle,
            marker_matcher: None,
            prompt_experience: None,
            llm_classifier: None,
            scan_purpose: PromptScanPurpose::Direct,
            action_settle_delay: DEFAULT_ACTION_SETTLE_DELAY,
        }
    }

    pub fn with_marker_matcher(mut self, marker_matcher: &'a MarkerMatcher) -> Self {
        self.marker_matcher = Some(marker_matcher);
        self
    }

    pub fn with_prompt_experience(
        mut self,
        prompt_experience: &'a dyn PromptExperienceLookup,
    ) -> Self {
        self.prompt_experience = Some(prompt_experience);
        self
    }

    pub fn with_llm_classifier(mut self, llm_classifier: &'a dyn LlmClassifier) -> Self {
        self.llm_classifier = Some(llm_classifier);
        self
    }

    pub fn with_action_settle_delay(mut self, delay: Duration) -> Self {
        self.action_settle_delay = delay;
        self
    }

    pub fn with_current_state(mut self, current_state: &'a str) -> Self {
        self.current_state = current_state;
        self
    }

    pub fn with_scan_purpose(mut self, scan_purpose: PromptScanPurpose) -> Self {
        self.scan_purpose = scan_purpose;
        self
    }
}

#[derive(Debug)]
pub enum PromptRunOutcome {
    NoActionNeeded {
        depth: usize,
    },
    RetryLater {
        depth: usize,
    },
    Pending {
        snapshot: PromptSnapshot,
        depth: usize,
        block_reason: String,
    },
    DepthExceeded {
        snapshot: PromptSnapshot,
        depth: usize,
    },
    ExecutorFailed {
        error: CcbdError,
        depth: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptSnapshot {
    pub sanitized_hash: [u8; 32],
    pub sanitized_text: String,
}

pub fn handle_prompt_chain(ctx: RunnerContext<'_>, max_depth: usize) -> PromptRunOutcome {
    let mut depth = 0usize;
    let mut previous_hash: Option<[u8; 32]> = None;
    let mut same_hash_skips = 0usize;
    let mut empty_capture_skips = 0usize;
    let mut unknown_stability = UnknownStability::default();
    let unknown_scan_limit = max_depth.max(UNKNOWN_STABLE_SCAN_THRESHOLD);
    let mut matched_case_id: Option<String> = None;
    let mut confirmed_action_capture: Option<String> = None;

    loop {
        tracing::info!(
            agent_id = ctx.agent_id,
            pane_id = ctx.pane_id.0,
            depth,
            max_depth,
            "prompt runner capturing pane"
        );
        let capture = match confirmed_action_capture
            .take()
            .map(Ok)
            .unwrap_or_else(|| ctx.tmux_session_handle.capture_pane(ctx.pane_id))
        {
            Ok(capture) => capture,
            Err(error) => {
                if let Some(snapshot) = unknown_stability.last_snapshot() {
                    tracing::info!(
                        agent_id = ctx.agent_id,
                        depth,
                        reason = %error,
                        "prompt runner kept last unknown prompt after follow-up capture failed"
                    );
                    return PromptRunOutcome::Pending {
                        snapshot,
                        depth,
                        block_reason: matched_case_id
                            .clone()
                            .unwrap_or_else(|| "unknown_prompt".to_string()),
                    };
                }
                tracing::error!(
                    agent_id = ctx.agent_id,
                    depth,
                    reason = %error,
                    impact = "prompt-handler cannot classify pane; caller should keep existing fallback",
                    "prompt runner capture failed"
                );
                return PromptRunOutcome::ExecutorFailed { error, depth };
            }
        };

        let decision = classify_capture(
            GateContext {
                provider: ctx.provider,
                current_state: ctx.current_state,
                kb: ctx.kb_handle,
                marker_matcher: ctx.marker_matcher,
                prompt_experience: ctx.prompt_experience,
                scan_purpose: ctx.scan_purpose,
            },
            previous_hash,
            &capture,
        );

        match decision {
            PromptGateDecision::Skip {
                sanitized_hash,
                reason,
            } => {
                unknown_stability.reset();
                if reason == crate::prompt_handler::gating::GateSkipReason::EmptyCapture {
                    empty_capture_skips += 1;
                    if empty_capture_skips <= max_depth {
                        tracing::info!(
                            agent_id = ctx.agent_id,
                            depth,
                            empty_capture_skips,
                            "prompt runner waiting for spawning pane to render"
                        );
                        if !ctx.action_settle_delay.is_zero() {
                            std::thread::sleep(ctx.action_settle_delay);
                        }
                        continue;
                    }
                    tracing::info!(
                        agent_id = ctx.agent_id,
                        depth,
                        empty_capture_skips,
                        "prompt runner deferred empty spawning capture"
                    );
                    return PromptRunOutcome::RetryLater { depth };
                }

                if reason == crate::prompt_handler::gating::GateSkipReason::SameHash {
                    same_hash_skips += 1;
                    if same_hash_skips <= max_depth {
                        tracing::info!(
                            agent_id = ctx.agent_id,
                            depth,
                            same_hash_skips,
                            "prompt runner waiting for post-action screen refresh"
                        );
                        if !ctx.action_settle_delay.is_zero() {
                            std::thread::sleep(ctx.action_settle_delay);
                        }
                        continue;
                    }
                    tracing::warn!(
                        agent_id = ctx.agent_id,
                        depth,
                        same_hash_skips,
                        max_depth,
                        reason = "same sanitized capture hash persisted after action",
                        impact = "caller should move agent to PROMPT_PENDING for manual resolution",
                        "prompt runner same-hash stuck"
                    );
                    return PromptRunOutcome::Pending {
                        snapshot: PromptSnapshot {
                            sanitized_hash,
                            sanitized_text: sanitize_pane_text(&capture),
                        },
                        depth,
                        block_reason: matched_case_id
                            .clone()
                            .unwrap_or_else(|| "unknown_prompt".to_string()),
                    };
                }

                if reason == crate::prompt_handler::gating::GateSkipReason::IdleMarker {
                    match confirm_can_input(&ctx, &capture, depth) {
                        CanInputProbe::Confirmed => {
                            tracing::info!(
                                agent_id = ctx.agent_id,
                                depth,
                                "prompt runner confirmed provider can accept input"
                            );
                            return PromptRunOutcome::NoActionNeeded { depth };
                        }
                        CanInputProbe::NotCandidate => {}
                        CanInputProbe::Failed(error) => {
                            return PromptRunOutcome::ExecutorFailed { error, depth };
                        }
                    }
                }

                tracing::info!(
                    agent_id = ctx.agent_id,
                    depth,
                    ?reason,
                    "prompt runner finished without action"
                );
                if let Some(ref case_id) = matched_case_id {
                    return PromptRunOutcome::Pending {
                        snapshot: PromptSnapshot {
                            sanitized_hash,
                            sanitized_text: sanitize_pane_text(&capture),
                        },
                        depth,
                        block_reason: case_id.clone(),
                    };
                }

                if reason == crate::prompt_handler::gating::GateSkipReason::IdleMarker
                    && crate::prompt_handler::integration::is_prompt_handling_provider(ctx.provider)
                {
                    return PromptRunOutcome::Pending {
                        snapshot: PromptSnapshot {
                            sanitized_hash,
                            sanitized_text: sanitize_pane_text(&capture),
                        },
                        depth,
                        block_reason: "unknown_prompt".to_string(),
                    };
                }
                return PromptRunOutcome::NoActionNeeded { depth };
            }
            PromptGateDecision::KnownAction {
                case_id,
                actions,
                sanitized_hash,
            } => {
                matched_case_id = Some(case_id.clone());
                unknown_stability.reset();
                if actions.is_empty() {
                    tracing::info!(
                        agent_id = ctx.agent_id,
                        depth,
                        case_id,
                        "prompt runner found known prompt with no actions, moving to pending"
                    );
                    return PromptRunOutcome::Pending {
                        snapshot: PromptSnapshot {
                            sanitized_hash,
                            sanitized_text: sanitize_pane_text(&capture),
                        },
                        depth,
                        block_reason: case_id,
                    };
                }
                if depth >= max_depth {
                    tracing::warn!(
                        agent_id = ctx.agent_id,
                        depth,
                        max_depth,
                        case_id,
                        reason = "max prompt recursion depth reached",
                        impact = "caller should move agent to PROMPT_PENDING for manual resolution",
                        "prompt runner depth exceeded"
                    );
                    return PromptRunOutcome::DepthExceeded {
                        snapshot: PromptSnapshot {
                            sanitized_hash,
                            sanitized_text: sanitize_pane_text(&capture),
                        },
                        depth,
                    };
                }

                empty_capture_skips = 0;
                tracing::info!(
                    agent_id = ctx.agent_id,
                    depth,
                    case_id,
                    action_count = actions.len(),
                    "prompt runner executing known prompt actions"
                );
                let post_action_capture =
                    match execute_actions(&ctx, &case_id, &actions, depth, capture.clone()) {
                        Ok(capture) => capture,
                        Err(error) => {
                            return PromptRunOutcome::ExecutorFailed { error, depth };
                        }
                    };
                depth += 1;
                previous_hash = Some(sanitized_hash);
                same_hash_skips = 0;
                confirmed_action_capture = Some(post_action_capture);
            }
            PromptGateDecision::Unknown {
                sanitized_hash,
                sanitized_text,
            } => {
                let stable_unknown =
                    unknown_stability.record(sanitized_hash, sanitized_text.clone());
                if !stable_unknown {
                    if unknown_stability.total_scans >= unknown_scan_limit {
                        tracing::info!(
                            agent_id = ctx.agent_id,
                            depth,
                            total_unknown_scans = unknown_stability.total_scans,
                            stable_unknown_scans = unknown_stability.stable_scans,
                            unknown_scan_limit,
                            "prompt runner deferred changing unknown prompt"
                        );
                        return PromptRunOutcome::RetryLater { depth };
                    }
                    tracing::info!(
                        agent_id = ctx.agent_id,
                        depth,
                        total_unknown_scans = unknown_stability.total_scans,
                        stable_unknown_scans = unknown_stability.stable_scans,
                        threshold = UNKNOWN_STABLE_SCAN_THRESHOLD,
                        "prompt runner observing unknown prompt stability"
                    );
                    if !ctx.action_settle_delay.is_zero() {
                        std::thread::sleep(ctx.action_settle_delay);
                    }
                    continue;
                }

                match try_llm_slow_path(&ctx, &capture, &sanitized_text, depth) {
                    LlmSlowPathDecision::Action(actions) => {
                        if depth >= max_depth {
                            tracing::warn!(
                                agent_id = ctx.agent_id,
                                depth,
                                max_depth,
                                reason =
                                    "max prompt recursion depth reached after LLM classification",
                                impact = "caller should move agent to PROMPT_PENDING for manual resolution",
                                "prompt runner depth exceeded"
                            );
                            return PromptRunOutcome::DepthExceeded {
                                snapshot: PromptSnapshot {
                                    sanitized_hash,
                                    sanitized_text,
                                },
                                depth,
                            };
                        }
                        let post_action_capture = match execute_actions(
                            &ctx,
                            "llm-haiku-4.5",
                            &actions,
                            depth,
                            capture.clone(),
                        ) {
                            Ok(capture) => capture,
                            Err(error) => {
                                return PromptRunOutcome::ExecutorFailed { error, depth };
                            }
                        };
                        depth += 1;
                        previous_hash = Some(sanitized_hash);
                        same_hash_skips = 0;
                        empty_capture_skips = 0;
                        unknown_stability.reset();
                        confirmed_action_capture = Some(post_action_capture);
                        continue;
                    }
                    LlmSlowPathDecision::Pending { reason } => {
                        tracing::info!(
                            agent_id = ctx.agent_id,
                            depth,
                            block_reason = reason,
                            "prompt runner LLM slow path moved prompt to pending"
                        );
                        return PromptRunOutcome::Pending {
                            snapshot: PromptSnapshot {
                                sanitized_hash,
                                sanitized_text,
                            },
                            depth,
                            block_reason: reason.to_string(),
                        };
                    }
                    LlmSlowPathDecision::NotAttempted => {}
                }
                tracing::info!(
                    agent_id = ctx.agent_id,
                    depth,
                    impact =
                        "caller should move agent to PROMPT_PENDING and emit unknown prompt event",
                    "prompt runner found unknown prompt"
                );
                return PromptRunOutcome::Pending {
                    snapshot: PromptSnapshot {
                        sanitized_hash,
                        sanitized_text,
                    },
                    depth,
                    block_reason: matched_case_id
                        .clone()
                        .unwrap_or_else(|| "unknown_prompt".to_string()),
                };
            }
            PromptGateDecision::LookupFailed {
                error,
                sanitized_hash: _,
                sanitized_text: _,
            } => {
                tracing::error!(
                    agent_id = ctx.agent_id,
                    depth,
                    reason = %error,
                    "prompt runner stopped after learning-layer lookup failure"
                );
                return PromptRunOutcome::ExecutorFailed {
                    error: CcbdError::DbConstraintViolation(error),
                    depth,
                };
            }
        }
    }
}

#[derive(Default)]
struct UnknownStability {
    last_hash: Option<[u8; 32]>,
    last_text: Option<String>,
    stable_scans: usize,
    total_scans: usize,
}

impl UnknownStability {
    fn record(&mut self, sanitized_hash: [u8; 32], sanitized_text: String) -> bool {
        self.total_scans += 1;
        if self
            .last_hash
            .is_some_and(|previous| previous == sanitized_hash)
        {
            self.stable_scans += 1;
        } else {
            self.last_hash = Some(sanitized_hash);
            self.last_text = Some(sanitized_text);
            self.stable_scans = 1;
        }
        self.stable_scans >= UNKNOWN_STABLE_SCAN_THRESHOLD
    }

    fn last_snapshot(&self) -> Option<PromptSnapshot> {
        Some(PromptSnapshot {
            sanitized_hash: self.last_hash?,
            sanitized_text: self.last_text.clone()?,
        })
    }

    fn reset(&mut self) {
        self.last_hash = None;
        self.last_text = None;
        self.stable_scans = 0;
        self.total_scans = 0;
    }
}

enum LlmSlowPathDecision {
    Action(Vec<PromptAction>),
    Pending { reason: &'static str },
    NotAttempted,
}

fn try_llm_slow_path(
    ctx: &RunnerContext<'_>,
    capture: &str,
    sanitized_text: &str,
    depth: usize,
) -> LlmSlowPathDecision {
    if !is_llm_steady_state(ctx.current_state) {
        tracing::info!(
            agent_id = ctx.agent_id,
            state = ctx.current_state,
            "prompt runner skipped LLM slow path for non-steady state"
        );
        return LlmSlowPathDecision::NotAttempted;
    }
    if !crate::prompt_handler::integration::is_prompt_handling_provider(ctx.provider) {
        return LlmSlowPathDecision::NotAttempted;
    }

    let Some(llm_classifier) = ctx.llm_classifier else {
        return LlmSlowPathDecision::NotAttempted;
    };
    let outcome = match llm_classifier.classify_prompt(capture, ctx.provider, ctx.current_state) {
        Ok(outcome) => outcome,
        Err(crate::prompt_handler::llm_client::LlmError::MissingKey) => {
            tracing::info!(
                agent_id = ctx.agent_id,
                depth,
                "prompt runner LLM slow path disabled because API key is not configured"
            );
            return LlmSlowPathDecision::NotAttempted;
        }
        Err(error) => {
            tracing::info!(
                agent_id = ctx.agent_id,
                depth,
                reason = %error,
                "prompt runner LLM slow path returned pending"
            );
            return LlmSlowPathDecision::Pending {
                reason: llm_error_block_reason(&error),
            };
        }
    };

    if !outcome.safe {
        tracing::info!(
            agent_id = ctx.agent_id,
            depth,
            safe = outcome.safe,
            confidence = outcome.confidence,
            "prompt runner LLM slow path confidence/safety gate returned pending"
        );
        return LlmSlowPathDecision::Pending {
            reason: "llm_unsafe",
        };
    }

    if outcome.confidence < 0.8 {
        tracing::info!(
            agent_id = ctx.agent_id,
            depth,
            safe = outcome.safe,
            confidence = outcome.confidence,
            "prompt runner LLM slow path confidence/safety gate returned pending"
        );
        return LlmSlowPathDecision::Pending {
            reason: "llm_low_confidence",
        };
    }

    let Some(prompt_experience) = ctx.prompt_experience else {
        return LlmSlowPathDecision::Action(outcome.action);
    };
    let sanitized_hash_hex = crate::db::prompt_experience::hash_hex(
        &crate::prompt_handler::gating::hash_sanitized_text(sanitized_text),
    );
    let suggested_regex = outcome
        .suggested_regex
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| regex::escape(sanitized_text));
    let experience = NewPromptExperience {
        id: format!("llm-haiku-4.5:{}:{sanitized_hash_hex}", ctx.provider),
        provider: Some(ctx.provider.to_string()),
        fingerprint_type: PromptFingerprintType::Regex,
        fingerprint_value: suggested_regex,
        action: outcome.action.clone(),
        category: outcome.category.clone(),
        confidence: outcome.confidence,
        source: "llm-haiku-4.5".to_string(),
        trigger_state: Some(ctx.current_state.to_string()),
    };
    if let Err(error) = prompt_experience.record_prompt_experience(&experience) {
        tracing::warn!(
            agent_id = ctx.agent_id,
            depth,
            reason = %error,
            "prompt runner failed to store LLM prompt experience"
        );
    }
    LlmSlowPathDecision::Action(outcome.action)
}

fn is_llm_steady_state(state: &str) -> bool {
    matches!(
        state,
        crate::db::state_machine::STATE_IDLE | crate::db::state_machine::STATE_BUSY
    )
}

fn llm_error_block_reason(error: &crate::prompt_handler::llm_client::LlmError) -> &'static str {
    match error {
        crate::prompt_handler::llm_client::LlmError::Timeout => "llm_timeout",
        crate::prompt_handler::llm_client::LlmError::Transport(_)
        | crate::prompt_handler::llm_client::LlmError::InvalidResponse(_)
        | crate::prompt_handler::llm_client::LlmError::InvalidOutput(_) => "llm_error",
        crate::prompt_handler::llm_client::LlmError::MissingKey => "unknown_prompt",
    }
}

enum CanInputProbe {
    Confirmed,
    NotCandidate,
    Failed(CcbdError),
}

fn confirm_can_input(ctx: &RunnerContext<'_>, capture: &str, depth: usize) -> CanInputProbe {
    let probe = input_probe_literal(ctx.provider);
    if probe.is_empty() || !is_input_candidate(ctx.provider, capture) {
        return CanInputProbe::NotCandidate;
    }

    if let Err(error) = ctx
        .tmux_session_handle
        .send_key_literal(ctx.pane_id, probe)
        .map_err(|error| log_probe_error(ctx, depth, "send probe literal", error))
    {
        return CanInputProbe::Failed(error);
    }

    match wait_for_probe_echo(ctx, probe, depth) {
        Ok(true) => {}
        Ok(false) => {
            tracing::info!(
                provider = ctx.provider,
                depth,
                attempts = CAN_INPUT_PROBE_CAPTURE_ATTEMPTS,
                "input probe echo not observed after bounded captures; treating as NotCandidate"
            );
            if let Err(error) = ctx
                .tmux_session_handle
                .send_key_keysym(ctx.pane_id, "BSpace")
                .map_err(|error| log_probe_error(ctx, depth, "cleanup unconfirmed probe", error))
            {
                return CanInputProbe::Failed(error);
            }
            settle_after_probe_action(ctx);
            return CanInputProbe::NotCandidate;
        }
        Err(error) => return CanInputProbe::Failed(error),
    }

    if let Err(error) = ctx
        .tmux_session_handle
        .send_key_keysym(ctx.pane_id, "BSpace")
        .map_err(|error| log_probe_error(ctx, depth, "cleanup probe literal", error))
    {
        return CanInputProbe::Failed(error);
    }

    match wait_for_probe_cleanup(ctx, probe, depth) {
        Ok(true) => {}
        Ok(false) => {
            tracing::info!(
                provider = ctx.provider,
                depth,
                attempts = CAN_INPUT_PROBE_CAPTURE_ATTEMPTS,
                "input probe residue not cleaned after bounded captures; treating as NotCandidate"
            );
            return CanInputProbe::NotCandidate;
        }
        Err(error) => return CanInputProbe::Failed(error),
    }

    CanInputProbe::Confirmed
}

fn wait_for_probe_echo(
    ctx: &RunnerContext<'_>,
    probe: &str,
    depth: usize,
) -> Result<bool, CcbdError> {
    let mut captured_once = false;
    for _ in 0..CAN_INPUT_PROBE_CAPTURE_ATTEMPTS {
        settle_after_probe_action(ctx);
        let echoed = match ctx.tmux_session_handle.capture_pane(ctx.pane_id) {
            Ok(capture) => {
                captured_once = true;
                capture
            }
            Err(_error) if captured_once => return Ok(false),
            Err(error) => {
                return Err(log_probe_error(ctx, depth, "capture probe echo", error));
            }
        };
        if probe_echoed(ctx.provider, &echoed, probe) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn wait_for_probe_cleanup(
    ctx: &RunnerContext<'_>,
    probe: &str,
    depth: usize,
) -> Result<bool, CcbdError> {
    let mut captured_once = false;
    for _ in 0..CAN_INPUT_PROBE_CAPTURE_ATTEMPTS {
        settle_after_probe_action(ctx);
        let cleaned = match ctx.tmux_session_handle.capture_pane(ctx.pane_id) {
            Ok(capture) => {
                captured_once = true;
                capture
            }
            Err(_error) if captured_once => return Ok(false),
            Err(error) => {
                return Err(log_probe_error(ctx, depth, "capture probe cleanup", error));
            }
        };
        if !probe_echoed(ctx.provider, &cleaned, probe) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn settle_after_probe_action(ctx: &RunnerContext<'_>) {
    if !ctx.action_settle_delay.is_zero() {
        std::thread::sleep(ctx.action_settle_delay);
    }
}

fn input_probe_literal(provider: &str) -> &'static str {
    match provider {
        "codex" | "claude" => "x",
        _ => "",
    }
}

fn is_input_candidate(provider: &str, capture: &str) -> bool {
    match provider {
        "codex" => capture
            .lines()
            .any(|line| line.trim_start().starts_with('›')),
        "claude" => {
            capture_contains_claude_model_marker(capture)
                && capture.lines().any(is_claude_empty_input_line)
        }
        _ => false,
    }
}

fn probe_echoed(provider: &str, capture: &str, probe: &str) -> bool {
    match provider {
        "codex" => capture
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("› {probe}"))),
        "claude" => capture
            .lines()
            .any(|line| claude_input_line_payload(line).is_some_and(|payload| payload == probe)),
        _ => false,
    }
}

fn capture_contains_claude_model_marker(capture: &str) -> bool {
    capture
        .split(|ch: char| !ch.is_alphanumeric())
        .any(|token| matches!(token, "Sonnet" | "Opus" | "Haiku"))
}

fn is_claude_empty_input_line(line: &str) -> bool {
    let Some(_) = line.trim_start().strip_prefix('❯') else {
        return false;
    };
    claude_input_line_payload(line).is_none_or(is_claude_ghost_placeholder)
}

fn is_claude_ghost_placeholder(payload: &str) -> bool {
    payload.starts_with("Try \"")
}

fn claude_input_line_payload(line: &str) -> Option<&str> {
    let payload = line.trim_start().strip_prefix('❯')?.trim();
    (!payload.is_empty()).then_some(payload)
}

fn log_probe_error(
    ctx: &RunnerContext<'_>,
    depth: usize,
    step: &'static str,
    error: CcbdError,
) -> CcbdError {
    tracing::error!(
        agent_id = ctx.agent_id,
        depth,
        step,
        reason = %error,
        impact = "can-input confirmation failed before marking provider ready",
        "prompt runner can-input probe failed"
    );
    error
}

fn execute_actions(
    ctx: &RunnerContext<'_>,
    case_id: &str,
    actions: &[PromptAction],
    depth: usize,
    mut before: String,
) -> Result<String, CcbdError> {
    for action in actions {
        let validated = action.validate().map_err(CcbdError::from)?;
        let action_name = match &validated {
            ValidatedAction::Key(_) => "startup_prompt_key",
            ValidatedAction::Literal(_) => "startup_prompt_literal",
        };
        let outcome = crate::guarded_action::run_guarded_action_sync_from_before(
            action_name,
            before,
            ACTION_CONFIRMATION_TIMEOUT,
            ctx.action_settle_delay,
            || ctx.tmux_session_handle.capture_control_state(ctx.pane_id),
            || match &validated {
                ValidatedAction::Key(keysym) => {
                    tracing::info!(
                        agent_id = ctx.agent_id,
                        depth,
                        case_id,
                        keysym,
                        "prompt runner sending keysym"
                    );
                    ctx.tmux_session_handle.send_key_keysym(ctx.pane_id, keysym)
                }
                ValidatedAction::Literal(literal) => {
                    tracing::info!(
                        agent_id = ctx.agent_id,
                        depth,
                        case_id,
                        literal,
                        "prompt runner sending literal"
                    );
                    ctx.tmux_session_handle
                        .send_key_literal(ctx.pane_id, literal)
                }
            },
            |before, after| assess_prompt_action(&validated, &before.value, &after.value),
        )
        .map_err(|error| {
            log_action_error(
                ctx,
                depth,
                case_id,
                CcbdError::PtyIoError(error.to_string()),
            )
        })?;
        before = outcome.confirmed.value;
    }
    Ok(before)
}

fn assess_prompt_action(
    action: &ValidatedAction,
    before: &str,
    after: &str,
) -> crate::guarded_action::ActionAssessment {
    use crate::guarded_action::ActionAssessment;
    if before == after {
        return ActionAssessment::Mismatch {
            reason: "terminal control state did not change after prompt action".into(),
        };
    }
    if matches!(action, ValidatedAction::Key(key) if key == "Enter")
        && sanitize_pane_text(before) == sanitize_pane_text(after)
    {
        return ActionAssessment::Mismatch {
            reason: "Enter changed decoration but did not dismiss or advance the prompt".into(),
        };
    }
    ActionAssessment::Confirmed
}

fn log_action_error(
    ctx: &RunnerContext<'_>,
    depth: usize,
    case_id: &str,
    error: CcbdError,
) -> CcbdError {
    tracing::error!(
        agent_id = ctx.agent_id,
        depth,
        case_id,
        reason = %error,
        impact = "prompt-handler stopped before continuing recursive prompt handling",
        "prompt runner action execution failed"
    );
    error
}

#[cfg(test)]
mod tests {
    use super::{
        PromptIo, PromptRunOutcome, RunnerContext, handle_prompt_chain, is_claude_empty_input_line,
        is_input_candidate, probe_echoed,
    };
    use crate::db::prompt_experience::{
        NewPromptExperience, PromptFingerprintType, upsert_prompt_experience_sync,
    };
    use crate::db::{Db, init};
    use crate::error::CcbdError;
    use crate::marker::MarkerMatcher;
    use crate::prompt_handler::llm_client::{LlmClassifier, LlmError, LlmOutcome};
    use crate::prompt_handler::matcher::PromptScanPurpose;
    use crate::prompt_handler::schema::{PromptAction, PromptCase, PromptFingerprint, PromptKb};
    use crate::prompt_handler::seeds::default_cases;
    use crate::provider::manifest::get_manifest;
    use crate::tmux::TmuxPaneId;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct FakePromptIo {
        captures: Mutex<Vec<String>>,
        sent: Mutex<Vec<String>>,
        fail_send: bool,
    }

    struct FakeLlmClassifier {
        result: Result<LlmOutcome, LlmError>,
        calls: Mutex<usize>,
    }

    impl FakeLlmClassifier {
        fn new(result: Result<LlmOutcome, LlmError>) -> Self {
            Self {
                result,
                calls: Mutex::new(0),
            }
        }

        fn success(confidence: f64, safe: bool) -> Self {
            Self::new(Ok(LlmOutcome {
                category: "auto-accept".into(),
                action: vec![PromptAction::Key {
                    value: "Enter".into(),
                }],
                confidence,
                safe,
                suggested_regex: Some("LLM learned terms".into()),
            }))
        }

        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }

    impl LlmClassifier for FakeLlmClassifier {
        fn classify_prompt(
            &self,
            _capture: &str,
            _provider: &str,
            _agent_state: &str,
        ) -> Result<LlmOutcome, LlmError> {
            *self.calls.lock().unwrap() += 1;
            self.result.clone()
        }
    }

    const CODEX_UPDATE_MENU: &str = "✨ Update available! 0.135.0 -> 0.139.0\n  Release notes: https://github.com/openai/codex/releases/latest\n› 1. Update now (runs `npm install -g @openai/codex`)\n  2. Skip\n  3. Skip until next version\n  Press enter to continue";

    impl FakePromptIo {
        fn new(captures: &[&str]) -> Self {
            Self {
                captures: Mutex::new(captures.iter().map(|value| value.to_string()).collect()),
                sent: Mutex::new(Vec::new()),
                fail_send: false,
            }
        }

        fn with_send_failure(captures: &[&str]) -> Self {
            Self {
                fail_send: true,
                ..Self::new(captures)
            }
        }

        fn sent(&self) -> Vec<String> {
            self.sent.lock().unwrap().clone()
        }

        fn remaining_captures(&self) -> usize {
            self.captures.lock().unwrap().len()
        }
    }

    impl PromptIo for FakePromptIo {
        fn capture_pane(&self, _pane_id: &TmuxPaneId) -> Result<String, CcbdError> {
            if self.captures.lock().unwrap().is_empty() {
                return Err(CcbdError::TmuxCommandFailed {
                    cmd: "fake capture".into(),
                    stderr: "no scripted capture".into(),
                    exit: 1,
                });
            }
            Ok(self.captures.lock().unwrap().remove(0))
        }

        fn send_key_literal(&self, _pane_id: &TmuxPaneId, value: &str) -> Result<(), CcbdError> {
            if self.fail_send {
                return Err(CcbdError::TmuxCommandFailed {
                    cmd: "fake send literal".into(),
                    stderr: "scripted send failure".into(),
                    exit: 1,
                });
            }
            self.sent.lock().unwrap().push(format!("literal:{value}"));
            Ok(())
        }

        fn send_key_keysym(&self, _pane_id: &TmuxPaneId, value: &str) -> Result<(), CcbdError> {
            if self.fail_send {
                return Err(CcbdError::TmuxCommandFailed {
                    cmd: "fake send keysym".into(),
                    stderr: "scripted send failure".into(),
                    exit: 1,
                });
            }
            self.sent.lock().unwrap().push(format!("key:{value}"));
            Ok(())
        }
    }

    fn pane() -> TmuxPaneId {
        TmuxPaneId("%1".to_string())
    }

    fn ctx<'a>(
        io: &'a dyn PromptIo,
        pane: &'a TmuxPaneId,
        kb: &'a PromptKb,
        marker: &'a MarkerMatcher,
    ) -> RunnerContext<'a> {
        RunnerContext::new("ag_test", pane, "codex", io, kb)
            .with_marker_matcher(marker)
            .with_action_settle_delay(Duration::ZERO)
    }

    fn spawning_ctx<'a>(
        io: &'a dyn PromptIo,
        pane: &'a TmuxPaneId,
        kb: &'a PromptKb,
        marker: &'a MarkerMatcher,
    ) -> RunnerContext<'a> {
        ctx(io, pane, kb, marker).with_current_state(crate::db::state_machine::STATE_SPAWNING)
    }

    fn ctx_with_experience<'a>(
        io: &'a dyn PromptIo,
        pane: &'a TmuxPaneId,
        kb: &'a PromptKb,
        marker: &'a MarkerMatcher,
        db: &'a Db,
    ) -> RunnerContext<'a> {
        ctx(io, pane, kb, marker).with_prompt_experience(db)
    }

    fn ctx_with_llm<'a>(
        io: &'a dyn PromptIo,
        pane: &'a TmuxPaneId,
        kb: &'a PromptKb,
        marker: &'a MarkerMatcher,
        db: &'a Db,
        llm: &'a dyn LlmClassifier,
    ) -> RunnerContext<'a> {
        ctx_with_experience(io, pane, kb, marker, db).with_llm_classifier(llm)
    }

    fn test_db() -> Db {
        let file = tempfile::NamedTempFile::new().unwrap();
        init(file.path()).unwrap()
    }

    fn stale_scrollback_capture(stale_prompt: &str, active_tail: &str) -> String {
        let filler = (0..45)
            .map(|idx| format!("completed task line {idx}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!("{stale_prompt}\n{filler}\n{active_tail}")
    }

    fn selected_menu(capture: &str, selected_text: &str) -> String {
        capture.replacen(
            selected_text,
            &format!("\u{1b}[7m{selected_text}\u{1b}[0m"),
            1,
        )
    }

    #[test]
    fn single_layer_known_prompt_then_idle_returns_no_action_needed() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let selected = selected_menu(CODEX_UPDATE_MENU, "2. Skip");
        let io = FakePromptIo::new(&[
            CODEX_UPDATE_MENU,
            &selected,
            "ready\n  › ",
            "ready\n  › x",
            "ready\n  › ",
        ]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 1 }
        ));
        assert_eq!(
            io.sent(),
            ["key:Down", "key:Enter", "literal:x", "key:BSpace"]
        );
    }

    #[test]
    fn dispatch_guard_codex_update_snapshot_handles_before_dispatch() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let selected = selected_menu(CODEX_UPDATE_MENU, "2. Skip");
        let io = FakePromptIo::new(&[
            CODEX_UPDATE_MENU,
            &selected,
            "ready\n  › ",
            "ready\n  › x",
            "ready\n  › ",
        ]);

        let outcome = handle_prompt_chain(
            ctx(&io, &pane, &kb, &marker).with_scan_purpose(PromptScanPurpose::DispatchGuard),
            3,
        );

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 1 }
        ));
        assert_eq!(
            io.sent(),
            ["key:Down", "key:Enter", "literal:x", "key:BSpace"]
        );
    }

    #[test]
    fn dispatch_guard_clean_idle_allows_dispatch() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&["ready\n  › ", "ready\n  › x", "ready\n  › "]);

        let outcome = handle_prompt_chain(
            ctx(&io, &pane, &kb, &marker).with_scan_purpose(PromptScanPurpose::DispatchGuard),
            3,
        );

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 0 }
        ));
        assert_eq!(io.sent(), ["literal:x", "key:BSpace"]);
    }

    #[test]
    fn dispatch_guard_codex_resume_ghost_idle_retries_probe_redraw_before_pending() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let ghost =
            "ready\n  › Improve documentation in @filename\n\n  gpt-5.5 default · /tmp/proj";
        let io = FakePromptIo::new(&[
            ghost,
            ghost,
            "ready\n  › x\n\n  gpt-5.5 default · /tmp/proj",
            "ready\n  › x\n\n  gpt-5.5 default · /tmp/proj",
            ghost,
        ]);

        let outcome = handle_prompt_chain(
            ctx(&io, &pane, &kb, &marker).with_scan_purpose(PromptScanPurpose::DispatchGuard),
            3,
        );

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 0 }
        ));
        assert_eq!(io.sent(), ["literal:x", "key:BSpace"]);
    }

    #[test]
    fn dispatch_guard_codex_resume_ghost_idle_never_echoes_probe_returns_pending() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let ghost =
            "ready\n  › Improve documentation in @filename\n\n  gpt-5.5 default · /tmp/proj";
        let io = FakePromptIo::new(&[ghost, ghost, ghost, ghost]);

        let outcome = handle_prompt_chain(
            ctx(&io, &pane, &kb, &marker).with_scan_purpose(PromptScanPurpose::DispatchGuard),
            3,
        );

        match outcome {
            PromptRunOutcome::Pending {
                depth,
                block_reason,
                ..
            } => {
                assert_eq!(depth, 0);
                assert_eq!(block_reason, "unknown_prompt");
            }
            other => panic!("expected never-echo probe to stay pending, got {other:?}"),
        }
        assert_eq!(io.sent(), ["literal:x", "key:BSpace"]);
    }

    #[test]
    fn dispatch_guard_capture_error_rejects_dispatch() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&[]);

        let outcome = handle_prompt_chain(
            ctx(&io, &pane, &kb, &marker).with_scan_purpose(PromptScanPurpose::DispatchGuard),
            3,
        );

        assert!(matches!(
            outcome,
            PromptRunOutcome::ExecutorFailed { depth: 0, .. }
        ));
        assert!(io.sent().is_empty());
    }

    #[test]
    fn two_layer_prompt_chain_executes_both_known_cases() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let trust = "Do you trust this directory?\n1) Yes\n2) No";
        let trust_selected = selected_menu(trust, "1) Yes");
        let update_selected = selected_menu(CODEX_UPDATE_MENU, "2. Skip");
        let io = FakePromptIo::new(&[
            trust,
            &trust_selected,
            CODEX_UPDATE_MENU,
            &update_selected,
            "ready\n  › ",
            "ready\n  › x",
            "ready\n  › ",
        ]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 2 }
        ));
        assert_eq!(
            io.sent(),
            [
                "key:1",
                "key:Enter",
                "key:Down",
                "key:Enter",
                "literal:x",
                "key:BSpace"
            ]
        );
    }

    #[test]
    fn fourth_known_prompt_exceeds_depth_three() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let trust_directory = "Do you trust this directory?\n1) Yes\n2) No";
        let trust_workspace = "Do you trust this workspace?\n1) Yes\n2) No";
        let trust_directory_selected = selected_menu(trust_directory, "1) Yes");
        let trust_workspace_selected = selected_menu(trust_workspace, "1) Yes");
        let update_selected = selected_menu(CODEX_UPDATE_MENU, "2. Skip");
        let io = FakePromptIo::new(&[
            trust_directory,
            &trust_directory_selected,
            CODEX_UPDATE_MENU,
            &update_selected,
            trust_workspace,
            &trust_workspace_selected,
            CODEX_UPDATE_MENU,
        ]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::DepthExceeded { depth: 3, .. }
        ));
        assert_eq!(
            io.sent(),
            [
                "key:1",
                "key:Enter",
                "key:Down",
                "key:Enter",
                "key:1",
                "key:Enter"
            ]
        );
    }

    #[test]
    fn enter_that_does_not_dismiss_prompt_stops_before_any_next_action() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let selected = selected_menu(CODEX_UPDATE_MENU, "2. Skip");
        let io = FakePromptIo::new(&[CODEX_UPDATE_MENU, &selected, CODEX_UPDATE_MENU]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 1);

        assert!(matches!(
            outcome,
            PromptRunOutcome::ExecutorFailed { depth: 0, .. }
        ));
        assert_eq!(io.sent(), ["key:Down", "key:Enter"]);
    }

    #[test]
    fn stale_startup_prompt_outside_active_region_does_not_send_ack_churn_action() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let capture = stale_scrollback_capture(
            "Update available! Run `npm install -g @openai/codex`",
            "PONG\n  ›",
        );
        let io = FakePromptIo::new(&[capture.as_str(), capture.as_str(), capture.as_str()]);
        let ctx = RunnerContext::new("ag_test", &pane, "codex", &io, &kb)
            .with_current_state(crate::db::state_machine::STATE_WAITING_FOR_ACK)
            .with_action_settle_delay(Duration::ZERO);

        let outcome = handle_prompt_chain(ctx, 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::Pending { depth: 0, .. }
        ));
        assert!(io.sent().is_empty());
    }

    #[test]
    fn ack_scan_purpose_ignores_startup_notification_but_direct_scan_handles_it() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let capture = CODEX_UPDATE_MENU;
        let ack_io = FakePromptIo::new(&[capture, capture, capture]);
        let selected = selected_menu(CODEX_UPDATE_MENU, "2. Skip");
        let direct_io = FakePromptIo::new(&[
            capture,
            &selected,
            "ready\n  › ",
            "ready\n  › x",
            "ready\n  › ",
        ]);
        let ack_ctx = RunnerContext::new("ag_test", &pane, "codex", &ack_io, &kb)
            .with_scan_purpose(PromptScanPurpose::AckVisualDiff)
            .with_action_settle_delay(Duration::ZERO);
        let direct_marker = MarkerMatcher::from_manifest(&get_manifest("codex"));

        let ack_outcome = handle_prompt_chain(ack_ctx, 3);
        let direct_outcome = handle_prompt_chain(
            ctx(&direct_io, &pane, &kb, &direct_marker)
                .with_scan_purpose(PromptScanPurpose::Direct),
            3,
        );

        assert!(matches!(
            ack_outcome,
            PromptRunOutcome::Pending { depth: 0, .. }
        ));
        assert!(ack_io.sent().is_empty());
        assert!(matches!(
            direct_outcome,
            PromptRunOutcome::NoActionNeeded { depth: 1 }
        ));
        assert_eq!(
            direct_io.sent(),
            ["key:Down", "key:Enter", "literal:x", "key:BSpace"]
        );
    }

    #[test]
    fn unknown_prompt_returns_pending_without_actions() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "A new prompt with no known answer",
            "A new prompt with no known answer",
            "A new prompt with no known answer",
        ]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 3);

        match outcome {
            PromptRunOutcome::Pending {
                snapshot,
                depth,
                block_reason,
            } => {
                assert_eq!(depth, 0);
                assert_eq!(block_reason, "unknown_prompt");
                assert!(snapshot.sanitized_text.contains("new prompt"));
            }
            other => panic!("expected pending outcome, got {other:?}"),
        }
        assert!(io.sent().is_empty());
    }

    #[test]
    fn claude_input_candidate_treats_try_placeholder_as_empty_but_preserves_probe_echo() {
        let ghost = "Claude Code\nOpus\n❯\u{A0}Try \"fix lint errors\"";
        let typed = "Claude Code\nOpus\n❯ x";
        let empty = "Claude Code\nSonnet\n❯ ";
        let no_input = "Claude Code\nOpus\nSetup needs attention";

        assert!(is_claude_empty_input_line("❯\u{A0}Try \"fix lint errors\""));
        assert!(is_input_candidate("claude", ghost));
        assert!(!is_claude_empty_input_line("❯ x"));
        assert!(probe_echoed("claude", typed, "x"));
        assert!(is_claude_empty_input_line("❯ "));
        assert!(is_input_candidate("claude", empty));
        assert!(!is_input_candidate("claude", no_input));
    }

    #[test]
    fn transient_unknown_then_idle_marker_does_not_enter_pending() {
        let kb = PromptKb::new(Vec::new());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&["Claude startup banner still rendering", "ready\n$ "]);
        let ctx = RunnerContext::new("ag_test", &pane, "bash", &io, &kb)
            .with_marker_matcher(&marker)
            .with_action_settle_delay(Duration::ZERO);

        let outcome = handle_prompt_chain(ctx, 3);

        assert!(
            matches!(outcome, PromptRunOutcome::NoActionNeeded { depth: 0 }),
            "transient unknown must yield to a later idle marker, got {outcome:?}"
        );
        assert_eq!(io.remaining_captures(), 0);
    }

    #[test]
    fn stable_unknown_prompt_still_enters_pending_after_three_matching_scans() {
        let kb = PromptKb::new(Vec::new());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "Manual approval required",
            "Manual approval required",
            "Manual approval required",
        ]);
        let ctx = RunnerContext::new("ag_test", &pane, "bash", &io, &kb)
            .with_marker_matcher(&marker)
            .with_action_settle_delay(Duration::ZERO);

        let outcome = handle_prompt_chain(ctx, 3);

        match outcome {
            PromptRunOutcome::Pending {
                depth,
                block_reason,
                ..
            } => {
                assert_eq!(depth, 0);
                assert_eq!(block_reason, "unknown_prompt");
            }
            other => panic!("expected stable unknown prompt to enter pending, got {other:?}"),
        }
        assert_eq!(io.remaining_captures(), 0);
    }

    #[test]
    fn changing_unknown_frames_defer_instead_of_entering_pending() {
        let kb = PromptKb::new(Vec::new());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "rendering frame A",
            "rendering frame B",
            "rendering frame A",
        ]);
        let ctx = RunnerContext::new("ag_test", &pane, "bash", &io, &kb)
            .with_marker_matcher(&marker)
            .with_action_settle_delay(Duration::ZERO);

        let outcome = handle_prompt_chain(ctx, 3);

        assert!(
            matches!(outcome, PromptRunOutcome::RetryLater { depth: 0 }),
            "changing unknown frames should defer to a later scan, got {outcome:?}"
        );
        assert_eq!(io.remaining_captures(), 0);
    }

    #[test]
    fn learned_prompt_experience_executes_known_action() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        upsert_prompt_experience_sync(
            &db,
            &NewPromptExperience {
                id: "learned_terms".into(),
                provider: Some("codex".into()),
                fingerprint_type: PromptFingerprintType::Regex,
                fingerprint_value: "New learned terms".into(),
                action: vec![PromptAction::Key {
                    value: "Enter".into(),
                }],
                category: "auto-accept".into(),
                confidence: 0.92,
                source: "test".into(),
                trigger_state: None,
            },
        )
        .unwrap();
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&[
            "New learned terms\nPress Enter to continue",
            "ready\n  › ",
            "ready\n  › x",
            "ready\n  › ",
        ]);

        let outcome = handle_prompt_chain(ctx_with_experience(&io, &pane, &kb, &marker, &db), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 1 }
        ));
        assert_eq!(io.sent(), ["key:Enter", "literal:x", "key:BSpace"]);
    }

    #[test]
    fn learned_prompt_miss_continues_to_pending() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "No learned match here",
            "No learned match here",
            "No learned match here",
        ]);

        let outcome = handle_prompt_chain(ctx_with_experience(&io, &pane, &kb, &marker, &db), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::Pending { depth: 0, .. }
        ));
        assert!(io.sent().is_empty());
    }

    #[test]
    fn llm_high_confidence_executes_action_and_writes_experience() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::success(0.91, true);
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&[
            "LLM learned terms\nPress Enter",
            "LLM learned terms\nPress Enter",
            "LLM learned terms\nPress Enter",
            "ready\n  › ",
            "ready\n  › x",
            "ready\n  › ",
        ]);

        let outcome = handle_prompt_chain(ctx_with_llm(&io, &pane, &kb, &marker, &db, &llm), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 1 }
        ));
        assert_eq!(llm.calls(), 1);
        assert_eq!(io.sent(), ["key:Enter", "literal:x", "key:BSpace"]);
        let saved = crate::db::prompt_experience::lookup_prompt_experience_sync(
            &db,
            "codex",
            "LLM learned terms\nPress Enter",
            "not-used",
        )
        .unwrap()
        .unwrap();
        assert_eq!(saved.source, "llm-haiku-4.5");
        assert_eq!(
            saved.action,
            vec![PromptAction::Key {
                value: "Enter".into()
            }]
        );
    }

    #[test]
    fn llm_low_confidence_returns_pending_without_action() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::success(0.79, true);
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "LLM low confidence prompt",
            "LLM low confidence prompt",
            "LLM low confidence prompt",
        ]);

        let outcome = handle_prompt_chain(ctx_with_llm(&io, &pane, &kb, &marker, &db, &llm), 3);

        assert_pending_reason(outcome, "llm_low_confidence");
        assert_eq!(llm.calls(), 1);
        assert!(io.sent().is_empty());
    }

    #[test]
    fn llm_unsafe_returns_pending_without_action() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::success(0.95, false);
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "LLM unsafe prompt",
            "LLM unsafe prompt",
            "LLM unsafe prompt",
        ]);

        let outcome = handle_prompt_chain(ctx_with_llm(&io, &pane, &kb, &marker, &db, &llm), 3);

        assert_pending_reason(outcome, "llm_unsafe");
        assert_eq!(llm.calls(), 1);
        assert!(io.sent().is_empty());
    }

    #[test]
    fn llm_missing_key_returns_pending_without_action() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::new(Err(LlmError::MissingKey));
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "LLM missing key prompt",
            "LLM missing key prompt",
            "LLM missing key prompt",
        ]);

        let outcome = handle_prompt_chain(ctx_with_llm(&io, &pane, &kb, &marker, &db, &llm), 3);

        assert_pending_reason(outcome, "unknown_prompt");
        assert_eq!(llm.calls(), 1);
        assert!(io.sent().is_empty());
    }

    #[test]
    fn llm_network_error_returns_pending_without_action() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::new(Err(LlmError::Transport("network".into())));
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "LLM network error prompt",
            "LLM network error prompt",
            "LLM network error prompt",
        ]);

        let outcome = handle_prompt_chain(ctx_with_llm(&io, &pane, &kb, &marker, &db, &llm), 3);

        assert_pending_reason(outcome, "llm_error");
        assert_eq!(llm.calls(), 1);
        assert!(io.sent().is_empty());
    }

    #[test]
    fn llm_timeout_returns_pending_with_timeout_reason() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::new(Err(LlmError::Timeout));
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "LLM timeout prompt",
            "LLM timeout prompt",
            "LLM timeout prompt",
        ]);

        let outcome = handle_prompt_chain(ctx_with_llm(&io, &pane, &kb, &marker, &db, &llm), 3);

        assert_pending_reason(outcome, "llm_timeout");
        assert_eq!(llm.calls(), 1);
        assert!(io.sent().is_empty());
    }

    #[test]
    fn rg_prompt_pending_idle_marker_confirmed_returns_no_action() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&["ready\n› ", "ready\n› x", "ready\n› "]);
        let ctx = RunnerContext::new("ag_test", &pane, "codex", &io, &kb)
            .with_marker_matcher(&marker)
            .with_current_state(crate::db::state_machine::STATE_PROMPT_PENDING)
            .with_action_settle_delay(Duration::ZERO);

        let outcome = handle_prompt_chain(ctx, 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::NoActionNeeded { depth: 0 }
        ));
        assert_eq!(io.sent(), ["literal:x", "key:BSpace"]);
    }

    #[test]
    fn rg_prompt_pending_unknown_menu_probe_not_candidate_stays_pending() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&[
            "Confirm risky action\n› ",
            "Confirm risky action\n› ",
            "Confirm risky action\n› ",
            "Confirm risky action\n› ",
        ]);
        let ctx = RunnerContext::new("ag_test", &pane, "codex", &io, &kb)
            .with_marker_matcher(&marker)
            .with_current_state(crate::db::state_machine::STATE_PROMPT_PENDING)
            .with_action_settle_delay(Duration::ZERO);

        let outcome = handle_prompt_chain(ctx, 3);

        assert_pending_reason(outcome, "unknown_prompt");
        assert_eq!(io.sent(), ["literal:x", "key:BSpace"]);
    }

    #[test]
    fn llm_is_not_called_for_transient_state() {
        let kb = PromptKb::new(Vec::new());
        let db = test_db();
        let llm = FakeLlmClassifier::success(0.95, true);
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "Transient startup prompt",
            "Transient startup prompt",
            "Transient startup prompt",
        ]);
        let ctx = spawning_ctx(&io, &pane, &kb, &marker)
            .with_prompt_experience(&db)
            .with_llm_classifier(&llm);

        let outcome = handle_prompt_chain(ctx, 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::Pending { depth: 0, .. }
        ));
        assert_eq!(llm.calls(), 0);
        assert!(io.sent().is_empty());
    }

    #[test]
    fn spawning_empty_captures_defer_until_ready_can_input_probe() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let io = FakePromptIo::new(&["", "", "ready\n  › ", "ready\n  › x", "ready\n  › "]);

        let outcome = handle_prompt_chain(spawning_ctx(&io, &pane, &kb, &marker), 5);

        assert!(
            matches!(outcome, PromptRunOutcome::NoActionNeeded { depth: 0 }),
            "SPAWNING empty captures must defer until the rendered TUI can pass can-input, got {outcome:?}"
        );
        assert_eq!(io.sent(), ["literal:x", "key:BSpace"]);
    }

    #[test]
    fn spawning_empty_capture_before_unknown_prompt_uses_non_empty_snapshot() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&[
            "",
            "New provider EULA\n1) Accept\n2) Decline",
            "New provider EULA\n1) Accept\n2) Decline",
            "New provider EULA\n1) Accept\n2) Decline",
        ]);

        let outcome = handle_prompt_chain(spawning_ctx(&io, &pane, &kb, &marker), 5);

        match outcome {
            PromptRunOutcome::Pending {
                snapshot,
                depth,
                block_reason,
            } => {
                assert_eq!(depth, 0);
                assert_eq!(block_reason, "unknown_prompt");
                assert!(
                    snapshot.sanitized_text.contains("New provider EULA"),
                    "pending snapshot must come from the non-empty prompt, got {:?}",
                    snapshot.sanitized_text
                );
                assert_ne!(
                    snapshot.sanitized_hash,
                    crate::prompt_handler::gating::hash_sanitized_text("")
                );
            }
            other => panic!("expected pending unknown prompt, got {other:?}"),
        }
        assert!(io.sent().is_empty());
    }

    #[test]
    fn invalid_action_is_rejected_without_terminal_input() {
        let kb = PromptKb::new(vec![PromptCase {
            id: "bad_action".into(),
            provider: Some("codex".into()),
            fingerprint: PromptFingerprint::Regex {
                pattern: "Unsafe approval".into(),
            },
            action: vec![PromptAction::Literal {
                value: "yes && rm -rf /".into(),
            }],
            category: "auto-accept".into(),
            description: None,
            confidence_threshold: None,
            used_count: 0,
            created_at: None,
            last_used_at: None,
            created_by: Some("test".into()),
            regex_flags: Vec::new(),
            trigger_state: None,
        }]);
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::new(&["Unsafe approval"]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::ExecutorFailed { depth: 0, .. }
        ));
        assert!(io.sent().is_empty());
    }

    #[test]
    fn send_failure_returns_executor_failed() {
        let kb = PromptKb::new(default_cases());
        let pane = pane();
        let marker = MarkerMatcher::default();
        let io = FakePromptIo::with_send_failure(&[CODEX_UPDATE_MENU]);

        let outcome = handle_prompt_chain(ctx(&io, &pane, &kb, &marker), 3);

        assert!(matches!(
            outcome,
            PromptRunOutcome::ExecutorFailed { depth: 0, .. }
        ));
    }

    fn assert_pending_reason(outcome: PromptRunOutcome, expected: &str) {
        match outcome {
            PromptRunOutcome::Pending {
                depth,
                block_reason,
                ..
            } => {
                assert_eq!(depth, 0);
                assert_eq!(block_reason, expected);
            }
            other => panic!("expected pending with reason {expected}, got {other:?}"),
        }
    }
}
