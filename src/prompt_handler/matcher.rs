//! Regex matcher and screen sanitization for prompt-handler cases.

use crate::prompt_handler::schema::{
    PromptAction, PromptCase, PromptFingerprint, PromptKb, build_regex,
};
use regex::Regex;
use std::sync::OnceLock;

const ACTIVE_PROMPT_REGION_LINES: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptScanPurpose {
    Direct,
    AckVisualDiff,
    DispatchGuard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOutcome {
    Matched {
        case_id: String,
        actions: Vec<PromptAction>,
        matched_substring: String,
    },
    NoMatch,
}

pub fn match_prompt(
    provider: &str,
    current_state: &str,
    pane_text: &str,
    kb: &PromptKb,
) -> MatchOutcome {
    match_prompt_for_scan(
        provider,
        current_state,
        pane_text,
        kb,
        PromptScanPurpose::Direct,
    )
}

pub fn match_prompt_for_scan(
    provider: &str,
    current_state: &str,
    pane_text: &str,
    kb: &PromptKb,
    scan_purpose: PromptScanPurpose,
) -> MatchOutcome {
    let sanitized = sanitize_pane_text(pane_text);
    let active_region = active_prompt_region(&sanitized);
    tracing::info!(
        provider,
        cases = kb.cases.len(),
        "prompt matcher scan start"
    );

    for case in ordered_cases(kb) {
        if scan_purpose == PromptScanPurpose::AckVisualDiff && is_startup_notification_case(case) {
            tracing::info!(
                provider,
                case_id = %case.id,
                scan_purpose = ?scan_purpose,
                "prompt matcher skipped startup notification during ACK scan"
            );
            continue;
        }

        if !case_applies(provider, current_state, case) {
            tracing::info!(
                provider,
                case_id = %case.id,
                case_provider = ?case.provider,
                case_trigger_state = ?case.trigger_state,
                current_state,
                "prompt matcher skipped case gate mismatch"
            );
            continue;
        }

        let PromptFingerprint::Regex { pattern } = &case.fingerprint;
        let regex = match build_regex(pattern, &case.regex_flags) {
            Ok(regex) => regex,
            Err(err) => {
                tracing::warn!(
                    provider,
                    case_id = %case.id,
                    pattern,
                    reason = %err,
                    impact = "case skipped; prompt-handler continues scanning remaining cases",
                    "prompt matcher skipped invalid regex"
                );
                continue;
            }
        };

        let Some(captures) = regex.captures(&active_region) else {
            tracing::info!(
                provider,
                case_id = %case.id,
                "prompt matcher case did not match"
            );
            continue;
        };
        let matched_substring = capture_signature_text(&active_region, &captures);

        tracing::info!(
            provider,
            case_id = %case.id,
            actions = case.action.len(),
            "prompt matcher matched case"
        );
        return MatchOutcome::Matched {
            case_id: case.id.clone(),
            actions: case.action.clone(),
            matched_substring,
        };
    }

    tracing::info!(provider, "prompt matcher no match");
    MatchOutcome::NoMatch
}

pub fn active_prompt_region(sanitized_text: &str) -> String {
    let line_count = sanitized_text.lines().count();
    if line_count <= ACTIVE_PROMPT_REGION_LINES {
        return sanitized_text.to_string();
    }

    sanitized_text
        .lines()
        .skip(line_count - ACTIVE_PROMPT_REGION_LINES)
        .collect::<Vec<_>>()
        .join("\n")
}

fn capture_signature_text(haystack: &str, captures: &regex::Captures<'_>) -> String {
    if let Some(captured) = captures.iter().skip(1).flatten().next() {
        return captured.as_str().to_string();
    }

    let Some(full_match) = captures.get(0) else {
        return String::new();
    };

    let mut end = full_match.end();
    if full_match.as_str().matches('`').count() % 2 == 1
        && let Some(relative_end) = haystack[end..].find('`')
    {
        end += relative_end + '`'.len_utf8();
    }
    haystack[full_match.start()..end].to_string()
}

pub fn sanitize_pane_text(pane_text: &str) -> String {
    let without_ansi = crate::db::jobs::strip_ansi_escapes(pane_text);
    let mut lines = Vec::new();
    for line in without_ansi.lines() {
        let mut current = line.to_string();
        current = braille_spinner_regex()
            .replace_all(&current, "")
            .into_owned();
        current = ascii_spinner_prefix_regex()
            .replace_all(&current, "")
            .into_owned();
        current = elapsed_time_regex().replace_all(&current, "").into_owned();
        current = progress_percent_regex()
            .replace_all(&current, "")
            .into_owned();
        current = progress_bar_regex()
            .replace_all(&current, "[]")
            .into_owned();
        let trimmed = current.trim_end();
        if !trimmed.is_empty() {
            lines.push(trimmed.to_string());
        }
    }
    collapse_blankish_lines(lines.join("\n").trim())
}

fn ordered_cases(kb: &PromptKb) -> Vec<&PromptCase> {
    let mut cases = kb.cases.iter().collect::<Vec<_>>();
    cases.sort_by_key(|case| is_default_case(case));
    cases
}

fn is_default_case(case: &PromptCase) -> bool {
    case.created_by.as_deref() == Some("ah-default")
}

fn is_startup_notification_case(case: &PromptCase) -> bool {
    is_default_case(case) && matches!(case.id.as_str(), "codex_update_01" | "trust_path_01")
}

fn case_applies(provider: &str, current_state: &str, case: &PromptCase) -> bool {
    let provider_matches = case
        .provider
        .as_deref()
        .is_none_or(|case_provider| case_provider == provider);
    let state_matches = case
        .trigger_state
        .as_deref()
        .is_none_or(|trigger_state| trigger_state == current_state);
    provider_matches && state_matches
}

fn collapse_blankish_lines(text: &str) -> String {
    let mut out = Vec::new();
    let mut last_empty = false;
    for line in text.lines() {
        let trimmed = line.trim_end();
        let empty = trimmed.is_empty();
        if empty && last_empty {
            continue;
        }
        out.push(trimmed);
        last_empty = empty;
    }
    out.join("\n")
}

fn braille_spinner_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[\u{2800}-\u{28FF}]+").expect("valid braille regex"))
}

fn ascii_spinner_prefix_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*[-\\|/]\s+").expect("valid ascii spinner regex"))
}

fn elapsed_time_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d+(?:\.\d+)?s\b").expect("valid elapsed time regex"))
}

fn progress_percent_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d{1,3}%").expect("valid progress percent regex"))
}

fn progress_bar_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[[=>#\-\s]{3,}\]").expect("valid progress bar regex"))
}

#[cfg(test)]
mod tests {
    use super::{
        MatchOutcome, PromptScanPurpose, match_prompt, match_prompt_for_scan, sanitize_pane_text,
    };
    use crate::prompt_handler::schema::{PromptAction, PromptCase, PromptFingerprint, PromptKb};
    use crate::prompt_handler::seeds::default_cases;

    fn user_case(id: &str, pattern: &str, action_value: &str) -> PromptCase {
        PromptCase {
            id: id.to_string(),
            provider: Some("codex".to_string()),
            fingerprint: PromptFingerprint::Regex {
                pattern: pattern.to_string(),
            },
            action: vec![PromptAction::Key {
                value: action_value.to_string(),
            }],
            category: "auto-skip".to_string(),
            description: None,
            confidence_threshold: Some(0.9),
            used_count: 0,
            created_at: None,
            last_used_at: None,
            created_by: Some("master-manual".to_string()),
            regex_flags: Vec::new(),
            trigger_state: None,
        }
    }

    fn user_case_with_state(
        id: &str,
        pattern: &str,
        action_value: &str,
        trigger_state: Option<&str>,
    ) -> PromptCase {
        PromptCase {
            trigger_state: trigger_state.map(str::to_string),
            ..user_case(id, pattern, action_value)
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
    fn codex_update_bare_banner_no_longer_matches() {
        let kb = PromptKb::new(default_cases());
        let pane = "Update available! 0.129 -> 0.130\nRun `npm install -g @openai/codex`";

        let outcome = match_prompt("codex", "BUSY", pane, &kb);

        assert_eq!(outcome, MatchOutcome::NoMatch);
    }

    #[test]
    fn codex_update_matches_live_skip_menu_action() {
        let kb = PromptKb::new(default_cases());
        let pane = "✨ Update available! 0.135.0 -> 0.139.0\n  Release notes: https://github.com/openai/codex/releases/latest\n› 1. Update now (runs `npm install -g @openai/codex`)\n  2. Skip\n  3. Skip until next version\n  Press enter to continue";

        let outcome = match_prompt("codex", "IDLE", pane, &kb);

        assert_eq!(
            outcome,
            MatchOutcome::Matched {
                case_id: "codex_update_01".to_string(),
                actions: vec![
                    PromptAction::Key {
                        value: "Down".into()
                    },
                    PromptAction::Key {
                        value: "Enter".into()
                    },
                ],
                matched_substring: "Update available! 0.135.0 -> 0.139.0\n  Release notes: https://github.com/openai/codex/releases/latest\n› 1. Update now (runs `npm install -g @openai/codex`)\n  2. Skip\n  3. Skip until next version\n  Press enter to continue".to_string(),
            }
        );
    }

    #[test]
    fn codex_update_matches_live_skip_menu_when_install_command_wraps() {
        let kb = PromptKb::new(default_cases());
        let pane = "✨ Update available! 0.135.0 -> 0.139.0\n  Release notes: https://github.com/openai/codex/releases/latest\n› 1. Update now (runs `npm install -g @openai/co\ndex`)\n  2. Skip\n  3. Skip until next version\n  Press enter to continue";

        let outcome = match_prompt("codex", "IDLE", pane, &kb);

        assert!(matches!(
            outcome,
            MatchOutcome::Matched { case_id, actions, .. }
                if case_id == "codex_update_01"
                    && actions == vec![
                        PromptAction::Key { value: "Down".into() },
                        PromptAction::Key { value: "Enter".into() },
                    ]
        ));
    }

    #[test]
    fn trust_path_matches_builtin_accept_action() {
        let kb = PromptKb::new(default_cases());
        let pane = "Do you trust this directory?\n1) Yes\n2) No";

        let outcome = match_prompt("codex", "BUSY", pane, &kb);

        assert_eq!(
            outcome,
            MatchOutcome::Matched {
                case_id: "trust_path_01".to_string(),
                actions: vec![
                    PromptAction::Key { value: "1".into() },
                    PromptAction::Key {
                        value: "Enter".into()
                    },
                ],
                matched_substring: "Do you trust this directory".to_string(),
            }
        );
    }

    #[test]
    fn ack_scan_ignores_startup_notification_cases_without_changing_direct_scan() {
        let kb = PromptKb::new(default_cases());
        let capture = "Update available! Run `npm install -g @openai/codex`\nready\n  ›";

        let ack_outcome = match_prompt_for_scan(
            "codex",
            "BUSY",
            capture,
            &kb,
            PromptScanPurpose::AckVisualDiff,
        );
        let direct_outcome =
            match_prompt_for_scan("codex", "BUSY", capture, &kb, PromptScanPurpose::Direct);

        assert_eq!(ack_outcome, MatchOutcome::NoMatch);
        assert_eq!(direct_outcome, MatchOutcome::NoMatch);
    }

    #[test]
    fn stale_startup_prompt_outside_active_region_does_not_match_in_idle_direct_scan() {
        let kb = PromptKb::new(default_cases());
        let capture = stale_scrollback_capture(
            "Update available! Run `npm install -g @openai/codex`",
            "work complete\n  ›",
        );

        let outcome = match_prompt("codex", "IDLE", &capture, &kb);

        assert_eq!(outcome, MatchOutcome::NoMatch);
    }

    #[test]
    fn stale_mid_task_prompt_outside_active_region_does_not_refire() {
        let kb = PromptKb::new(vec![user_case(
            "confirm_delete",
            r"(?is)confirm delete\?\s+y/n",
            "y",
        )]);
        let capture = stale_scrollback_capture("Confirm delete? y/n", "delete completed\n  ›");

        let outcome = match_prompt("codex", "BUSY", &capture, &kb);

        assert_eq!(outcome, MatchOutcome::NoMatch);
    }

    #[test]
    fn foreground_multiline_bare_banner_inside_active_region_does_not_match() {
        let kb = PromptKb::new(default_cases());
        let foreground = "recent context\n".repeat(36)
            + "Update available! 0.129 -> 0.130\nRun `npm install -g @openai/codex`";

        let outcome = match_prompt("codex", "BUSY", &foreground, &kb);

        assert_eq!(outcome, MatchOutcome::NoMatch);
    }

    #[test]
    fn sanitization_removes_dynamic_noise() {
        let first = "\u{1b}[31m⠴ Working(1s) [==>   ] 20%\u{1b}[0m\nUpdate available!";
        let second = "\u{1b}[32m⠦ Working(9s) [====>] 80%\u{1b}[0m\nUpdate available!";

        assert_eq!(sanitize_pane_text(first), sanitize_pane_text(second));
    }

    #[test]
    fn user_case_has_priority_over_default_case() {
        let mut cases = default_cases();
        cases.push(user_case(
            "user_codex_update",
            r"(?is)update available!?.*npm install -g @openai/codex",
            "Esc",
        ));
        let kb = PromptKb::new(cases);

        let outcome = match_prompt(
            "codex",
            "BUSY",
            "Update available! Run `npm install -g @openai/codex`",
            &kb,
        );

        assert_eq!(
            outcome,
            MatchOutcome::Matched {
                case_id: "user_codex_update".to_string(),
                actions: vec![PromptAction::Key {
                    value: "Esc".into()
                }],
                matched_substring: "Update available! Run `npm install -g @openai/codex`"
                    .to_string(),
            }
        );
    }

    #[test]
    fn regex_flags_case_insensitive_are_applied() {
        let mut case = user_case("flagged_case", "update available", "Esc");
        case.regex_flags = vec!["CaseInsensitive".to_string()];
        let kb = PromptKb::new(vec![case]);

        let outcome = match_prompt("codex", "BUSY", "UPDATE AVAILABLE", &kb);

        assert!(matches!(
            outcome,
            MatchOutcome::Matched {
                case_id,
                ..
            } if case_id == "flagged_case"
        ));
    }

    #[test]
    fn trigger_state_filters_prompt_cases() {
        let kb = PromptKb::new(vec![user_case_with_state(
            "ack_only",
            "state guarded prompt",
            "Enter",
            Some("WAITING_FOR_ACK"),
        )]);

        let outcome = match_prompt("codex", "BUSY", "state guarded prompt", &kb);

        assert_eq!(outcome, MatchOutcome::NoMatch);
        assert!(matches!(
            match_prompt("codex", "WAITING_FOR_ACK", "state guarded prompt", &kb),
            MatchOutcome::Matched { case_id, .. } if case_id == "ack_only"
        ));
    }

    #[test]
    fn invalid_regex_is_skipped_without_blocking_next_case() {
        let kb = PromptKb::new(vec![
            user_case("bad_regex", "(", "Esc"),
            user_case("good_regex", "safe prompt", "Enter"),
        ]);

        let outcome = match_prompt("codex", "BUSY", "safe prompt", &kb);

        assert_eq!(
            outcome,
            MatchOutcome::Matched {
                case_id: "good_regex".to_string(),
                actions: vec![PromptAction::Key {
                    value: "Enter".into()
                }],
                matched_substring: "safe prompt".to_string(),
            }
        );
    }

    #[test]
    fn matched_substring_prefers_first_capture_group() {
        let kb = PromptKb::new(vec![user_case(
            "captured_update",
            r"(?is)update available!\s+([0-9.]+ -> [0-9.]+)",
            "Esc",
        )]);

        let outcome = match_prompt("codex", "BUSY", "Update available! 0.129.0 -> 0.130.0", &kb);

        match outcome {
            MatchOutcome::Matched {
                matched_substring, ..
            } => assert_eq!(matched_substring, "0.129.0 -> 0.130.0"),
            other => panic!("expected match, got {other:?}"),
        }
    }

    #[test]
    fn matched_substring_falls_back_to_full_match_without_capture_group() {
        let kb = PromptKb::new(vec![user_case("full_match", r"safe prompt", "Enter")]);

        let outcome = match_prompt("codex", "BUSY", "prefix safe prompt suffix", &kb);

        match outcome {
            MatchOutcome::Matched {
                matched_substring, ..
            } => assert_eq!(matched_substring, "safe prompt"),
            other => panic!("expected match, got {other:?}"),
        }
    }
}
