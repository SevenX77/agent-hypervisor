//! Four-level hash gate for prompt-handler capture classification.

use crate::db::prompt_experience::{PromptExperienceLookup, hash_hex};
use crate::marker::{MarkerMatcher, MatchResult};
use crate::prompt_handler::matcher::{
    MatchOutcome, PromptScanPurpose, active_prompt_region, match_prompt_for_scan,
    sanitize_pane_text,
};
use crate::prompt_handler::schema::{PromptAction, PromptKb};
use sha2::{Digest, Sha256};

pub struct GateContext<'a> {
    pub provider: &'a str,
    pub current_state: &'a str,
    pub kb: &'a PromptKb,
    pub marker_matcher: Option<&'a MarkerMatcher>,
    pub prompt_experience: Option<&'a dyn PromptExperienceLookup>,
    pub scan_purpose: PromptScanPurpose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptGateDecision {
    Skip {
        reason: GateSkipReason,
        sanitized_hash: [u8; 32],
    },
    KnownAction {
        case_id: String,
        actions: Vec<PromptAction>,
        sanitized_hash: [u8; 32],
    },
    Unknown {
        sanitized_hash: [u8; 32],
        sanitized_text: String,
    },
    LookupFailed {
        error: String,
        sanitized_hash: [u8; 32],
        sanitized_text: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateSkipReason {
    SameHash,
    IdleMarker,
    EmptyCapture,
}

pub fn classify_capture(
    ctx: GateContext<'_>,
    prev_hash: Option<[u8; 32]>,
    capture: &str,
) -> PromptGateDecision {
    let sanitized_text = sanitize_pane_text(capture);
    let sanitized_hash = hash_sanitized_text(&sanitized_text);
    let active_text = active_prompt_region(&sanitized_text);
    tracing::info!(
        provider = ctx.provider,
        has_prev_hash = prev_hash.is_some(),
        "prompt gate hash computed"
    );

    if ctx.current_state == crate::db::state_machine::STATE_SPAWNING
        && sanitized_text.trim().is_empty()
    {
        tracing::info!(
            provider = ctx.provider,
            reason = "empty spawning capture",
            "prompt gate deferred capture"
        );
        return PromptGateDecision::Skip {
            reason: GateSkipReason::EmptyCapture,
            sanitized_hash,
        };
    }

    if prev_hash.is_some_and(|previous| previous == sanitized_hash) {
        tracing::info!(
            provider = ctx.provider,
            reason = "same sanitized capture hash",
            "prompt gate skipped capture"
        );
        return PromptGateDecision::Skip {
            reason: GateSkipReason::SameHash,
            sanitized_hash,
        };
    }

    match match_prompt_for_scan(
        ctx.provider,
        ctx.current_state,
        &sanitized_text,
        ctx.kb,
        ctx.scan_purpose,
    ) {
        MatchOutcome::Matched {
            case_id, actions, ..
        } => {
            tracing::info!(
                provider = ctx.provider,
                case_id,
                action_count = actions.len(),
                "prompt gate classified known prompt"
            );
            PromptGateDecision::KnownAction {
                case_id,
                actions,
                sanitized_hash,
            }
        }
        MatchOutcome::NoMatch => {
            if marker_matches(ctx.marker_matcher, capture) {
                tracing::info!(
                    provider = ctx.provider,
                    reason = "idle marker matched",
                    "prompt gate skipped capture"
                );
                return PromptGateDecision::Skip {
                    reason: GateSkipReason::IdleMarker,
                    sanitized_hash,
                };
            }

            if let Some(prompt_experience) = ctx.prompt_experience {
                let active_hash = hash_sanitized_text(&active_text);
                let sanitized_hash_hex = hash_hex(&active_hash);
                match prompt_experience.lookup_prompt_experience(
                    ctx.provider,
                    &active_text,
                    &sanitized_hash_hex,
                ) {
                    Ok(Some(experience)) => {
                        tracing::info!(
                            provider = ctx.provider,
                            experience_id = %experience.id,
                            fingerprint_type = %experience.fingerprint_type,
                            action_count = experience.action.len(),
                            "prompt gate classified learned prompt"
                        );
                        return PromptGateDecision::KnownAction {
                            case_id: experience.id,
                            actions: experience.action,
                            sanitized_hash,
                        };
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(
                            provider = ctx.provider,
                            reason = %error,
                            impact = "prompt-handler cannot safely continue DB learning-layer classification",
                            "prompt gate learning-layer lookup failed"
                        );
                        return PromptGateDecision::LookupFailed {
                            error: error.to_string(),
                            sanitized_hash,
                            sanitized_text,
                        };
                    }
                }
            }
            tracing::info!(
                provider = ctx.provider,
                impact =
                    "caller should move agent to PROMPT_PENDING and emit UNKNOWN_PROMPT_DETECTED",
                "prompt gate classified unknown prompt"
            );
            PromptGateDecision::Unknown {
                sanitized_hash,
                sanitized_text,
            }
        }
    }
}

pub fn hash_sanitized_text(text: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hasher.finalize().into()
}

fn marker_matches(marker_matcher: Option<&MarkerMatcher>, capture: &str) -> bool {
    let Some(marker_matcher) = marker_matcher else {
        return false;
    };
    let mut parser = vt100::Parser::new(200, 200, 0);
    parser.process(capture.as_bytes());
    marker_matcher.scan(&parser) == MatchResult::Matched
}

#[cfg(test)]
mod tests {
    use super::{GateContext, GateSkipReason, PromptGateDecision, classify_capture};
    use crate::db::prompt_experience::{
        NewPromptExperience, PromptExperience, PromptExperienceLookup,
    };
    use crate::error::AhError;
    use crate::marker::MarkerMatcher;
    use crate::prompt_handler::gating::hash_sanitized_text;
    use crate::prompt_handler::matcher::{PromptScanPurpose, sanitize_pane_text};
    use crate::prompt_handler::schema::{PromptAction, PromptKb};
    use crate::prompt_handler::seeds::default_cases;
    use crate::provider::manifest::get_manifest;
    use std::sync::Mutex;

    struct RegexExperience {
        pattern: String,
        seen_texts: Mutex<Vec<String>>,
    }

    impl RegexExperience {
        fn new(pattern: &str) -> Self {
            Self {
                pattern: pattern.to_string(),
                seen_texts: Mutex::new(Vec::new()),
            }
        }

        fn seen_texts(&self) -> Vec<String> {
            self.seen_texts.lock().unwrap().clone()
        }
    }

    impl PromptExperienceLookup for RegexExperience {
        fn lookup_prompt_experience(
            &self,
            _provider: &str,
            sanitized_text: &str,
            _sanitized_hash_hex: &str,
        ) -> Result<Option<PromptExperience>, AhError> {
            self.seen_texts
                .lock()
                .unwrap()
                .push(sanitized_text.to_string());
            if regex::Regex::new(&self.pattern)
                .unwrap()
                .is_match(sanitized_text)
            {
                return Ok(Some(PromptExperience {
                    id: "learned_confirm".into(),
                    provider: Some("codex".into()),
                    fingerprint_type: "regex".into(),
                    fingerprint_value: self.pattern.clone(),
                    action: vec![PromptAction::Key { value: "y".into() }],
                    category: "auto-accept".into(),
                    confidence: 0.91,
                    source: "test".into(),
                    used_count: 1,
                    trigger_state: None,
                }));
            }
            Ok(None)
        }

        fn record_prompt_experience(
            &self,
            _experience: &NewPromptExperience,
        ) -> Result<(), AhError> {
            Ok(())
        }
    }

    fn ctx<'a>(kb: &'a PromptKb) -> GateContext<'a> {
        GateContext {
            provider: "codex",
            current_state: "BUSY",
            kb,
            marker_matcher: None,
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        }
    }

    fn stale_scrollback_capture(stale_prompt: &str, active_tail: &str) -> String {
        let filler = (0..45)
            .map(|idx| format!("completed task line {idx}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!("{stale_prompt}\n{filler}\n{active_tail}")
    }

    #[test]
    fn same_hash_skips_capture() {
        let kb = PromptKb::new(default_cases());
        let capture = "Working(1s)\nNo prompt here";
        let hash = hash_sanitized_text(&sanitize_pane_text(capture));

        let decision = classify_capture(ctx(&kb), Some(hash), capture);

        assert!(matches!(
            decision,
            PromptGateDecision::Skip {
                reason: GateSkipReason::SameHash,
                ..
            }
        ));
    }

    #[test]
    fn idle_marker_skips_capture() {
        let kb = PromptKb::new(default_cases());
        let marker = MarkerMatcher::default();
        let ctx = GateContext {
            provider: "bash",
            current_state: "BUSY",
            kb: &kb,
            marker_matcher: Some(&marker),
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        };

        let decision = classify_capture(ctx, None, "ready\n$ ");

        assert!(matches!(
            decision,
            PromptGateDecision::Skip {
                reason: GateSkipReason::IdleMarker,
                ..
            }
        ));
    }

    #[test]
    fn codex_update_bare_banner_is_unknown_without_idle_marker() {
        let kb = PromptKb::new(default_cases());
        let capture = "Update available! Run `npm install -g @openai/codex`";

        let decision = classify_capture(ctx(&kb), None, capture);

        assert!(matches!(decision, PromptGateDecision::Unknown { .. }));
    }

    #[test]
    fn codex_resume_update_menu_seed_beats_idle_marker() {
        let kb = PromptKb::new(default_cases());
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let ctx = GateContext {
            provider: "codex",
            current_state: "IDLE",
            kb: &kb,
            marker_matcher: Some(&marker),
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        };
        let capture = "Running scope as unit: run-rb.scope\n  ✨ Update available! 0.135.0 -> 0.139.0\n  Release notes: https://github.com/openai/codex/releases/latest\n› 1. Update now (runs `npm install -g @openai/codex`)\n  2. Skip\n  3. Skip until next version\n  Press enter to continue";

        let decision = classify_capture(ctx, None, capture);

        assert_eq!(
            decision,
            PromptGateDecision::KnownAction {
                case_id: "codex_update_01".into(),
                actions: vec![
                    PromptAction::Key {
                        value: "Down".into()
                    },
                    PromptAction::Key {
                        value: "Enter".into()
                    },
                ],
                sanitized_hash: hash_sanitized_text(&sanitize_pane_text(capture)),
            }
        );
    }

    #[test]
    fn ack_scan_ignores_startup_notification_but_direct_scan_handles_it() {
        let kb = PromptKb::new(default_cases());
        let capture = "Update available! Run `npm install -g @openai/codex`\nready\n  ›";
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let ack_ctx = GateContext {
            provider: "codex",
            current_state: "BUSY",
            kb: &kb,
            marker_matcher: None,
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::AckVisualDiff,
        };
        let direct_ctx = GateContext {
            provider: "codex",
            current_state: "BUSY",
            kb: &kb,
            marker_matcher: Some(&marker),
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        };

        let ack_decision = classify_capture(ack_ctx, None, capture);
        let direct_decision = classify_capture(direct_ctx, None, capture);

        assert!(matches!(ack_decision, PromptGateDecision::Unknown { .. }));
        assert!(matches!(
            direct_decision,
            PromptGateDecision::Skip {
                reason: GateSkipReason::IdleMarker,
                ..
            }
        ));
    }

    #[test]
    fn codex_idle_with_update_banner_is_idle_not_dismiss() {
        let kb = PromptKb::new(default_cases());
        let marker = MarkerMatcher::from_manifest(&get_manifest("codex"));
        let ctx = GateContext {
            provider: "codex",
            current_state: "IDLE",
            kb: &kb,
            marker_matcher: Some(&marker),
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        };
        let capture = "Running scope as unit: run-rb.scope\n  ✨ Update available! 0.135.0 -> 0.139.0\n  Run npm install -g @openai/codex to update.\n  See full release notes: https://github.com/openai/codex/releases/latest\n  Tip: [tui.keymap] in ~/.codex/config.toml lets you rebind supported shortcuts.\n› Improve documentation in @filename\n  gpt-5.5 default · /home/sevenx/coding/agent-hypervisor";

        let decision = classify_capture(ctx, None, capture);

        assert!(matches!(
            decision,
            PromptGateDecision::Skip {
                reason: GateSkipReason::IdleMarker,
                ..
            }
        ));
    }

    #[test]
    fn stale_startup_prompt_outside_active_region_is_unknown_in_busy_scan() {
        let kb = PromptKb::new(default_cases());
        let capture = stale_scrollback_capture(
            "Update available! Run `npm install -g @openai/codex`",
            "task complete\n  ›",
        );

        let decision = classify_capture(ctx(&kb), None, &capture);

        assert!(matches!(decision, PromptGateDecision::Unknown { .. }));
    }

    #[test]
    fn learned_prompt_outside_active_region_does_not_return_known_action() {
        let kb = PromptKb::new(Vec::new());
        let learned = RegexExperience::new(r"(?is)confirm deploy\?\s+y/n");
        let capture = stale_scrollback_capture("Confirm deploy? y/n", "deploy completed\n  ›");
        let ctx = GateContext {
            provider: "codex",
            current_state: "BUSY",
            kb: &kb,
            marker_matcher: None,
            prompt_experience: Some(&learned),
            scan_purpose: PromptScanPurpose::Direct,
        };

        let decision = classify_capture(ctx, None, &capture);

        assert!(matches!(decision, PromptGateDecision::Unknown { .. }));
        let seen_texts = learned.seen_texts();
        assert_eq!(seen_texts.len(), 1);
        assert!(
            !seen_texts[0].contains("Confirm deploy"),
            "learning lookup must not see stale scrollback: {:?}",
            seen_texts[0]
        );
        assert!(seen_texts[0].contains("deploy completed"));
    }

    #[test]
    fn unknown_returns_pending_candidate() {
        let kb = PromptKb::new(default_cases());

        let decision = classify_capture(ctx(&kb), None, "Completely new approval screen");

        match decision {
            PromptGateDecision::Unknown {
                sanitized_hash,
                sanitized_text,
            } => {
                assert_eq!(sanitized_text, "Completely new approval screen");
                assert_eq!(sanitized_hash, hash_sanitized_text(&sanitized_text));
            }
            other => panic!("expected unknown, got {other:?}"),
        }
    }

    #[test]
    fn marker_like_text_does_not_become_unknown_when_marker_matches() {
        let kb = PromptKb::new(default_cases());
        let marker = MarkerMatcher::default();
        let ctx = GateContext {
            provider: "bash",
            current_state: "BUSY",
            kb: &kb,
            marker_matcher: Some(&marker),
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        };

        let decision = classify_capture(ctx, None, "plain shell\n> ");

        assert!(matches!(
            decision,
            PromptGateDecision::Skip {
                reason: GateSkipReason::IdleMarker,
                ..
            }
        ));
    }

    #[test]
    fn spawning_empty_capture_defers_instead_of_unknown() {
        let kb = PromptKb::new(default_cases());
        let ctx = GateContext {
            provider: "codex",
            current_state: crate::db::state_machine::STATE_SPAWNING,
            kb: &kb,
            marker_matcher: None,
            prompt_experience: None,
            scan_purpose: PromptScanPurpose::Direct,
        };

        let decision = classify_capture(ctx, None, "");

        assert!(matches!(
            decision,
            PromptGateDecision::Skip {
                reason: GateSkipReason::EmptyCapture,
                ..
            }
        ));
    }
}
