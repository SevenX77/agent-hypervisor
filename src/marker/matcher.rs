//! Prompt marker matcher over a vt100 screen buffer.

use crate::provider::manifest::{IdleDetectionMode, ProviderManifest};
use regex::Regex;

const VIEWPORT_BOTTOM_LINES: usize = 6;

/// Result of scanning the terminal screen for a prompt marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchResult {
    Matched,
    NoMatch,
}

/// Regex-backed prompt matcher.
pub struct MarkerMatcher {
    mode: IdleDetectionMode,
    regex: Regex,
    anti_regex: Option<Regex>,
}

impl Default for MarkerMatcher {
    fn default() -> Self {
        Self {
            mode: IdleDetectionMode::LineEndRegex,
            regex: Regex::new(r"[\$#>✦]\s*$").expect("valid prompt regex"),
            anti_regex: None,
        }
    }
}

impl MarkerMatcher {
    /// Create a matcher from a custom regex.
    pub fn new(prompt_regex: Regex) -> Self {
        Self {
            mode: IdleDetectionMode::LineEndRegex,
            regex: prompt_regex,
            anti_regex: None,
        }
    }

    pub fn from_manifest(manifest: &ProviderManifest) -> Self {
        Self {
            mode: manifest.idle_detection_mode,
            regex: Regex::new(prompt_regex_for_provider(manifest.provider_name))
                .expect("valid provider prompt regex"),
            anti_regex: if manifest.idle_anti_pattern.is_empty() {
                None
            } else {
                Some(Regex::new(manifest.idle_anti_pattern).expect("valid idle anti regex"))
            },
        }
    }

    pub fn mode(&self) -> IdleDetectionMode {
        self.mode
    }

    /// Scan a vt100 parser screen for a prompt marker.
    pub fn scan(&self, parser: &vt100::Parser) -> MatchResult {
        let screen = parser.screen();
        let contents = screen.contents();
        if contents.contains("<<ah-idle:job-id=") {
            return MatchResult::Matched;
        }
        let viewport_bottom = viewport_bottom_region(&contents);
        let prompt_matched = self.regex.is_match(&viewport_bottom);
        if !prompt_matched {
            return MatchResult::NoMatch;
        }
        if let Some(anti_regex) = &self.anti_regex
            && anti_regex.is_match(&viewport_bottom)
        {
            return MatchResult::NoMatch;
        }
        MatchResult::Matched
    }
}

fn viewport_bottom_region(contents: &str) -> String {
    let lines = contents.lines().collect::<Vec<_>>();
    if lines.len() <= VIEWPORT_BOTTOM_LINES {
        return contents.to_string();
    }

    lines[lines.len() - VIEWPORT_BOTTOM_LINES..].join("\n")
}

fn prompt_regex_for_provider(provider: &str) -> &'static str {
    match provider {
        "codex" => r"(?m)^\s*›(?:\s|$)",
        "claude" => r"(?m)^\s*❯(?:\s|$)",
        "antigravity" => r"(?m)^\s*\? for shortcuts\b",
        _ => r"[\$#>✦]\s*$",
    }
}

#[cfg(test)]
mod tests {
    use super::{MarkerMatcher, MatchResult};
    use crate::provider::manifest::{IdleDetectionMode, get_manifest};
    use regex::Regex;

    fn parser_with(bytes: &[u8]) -> vt100::Parser {
        let mut parser = vt100::Parser::new(200, 200, 0);
        parser.process(bytes);
        parser
    }

    fn parser_with_width(bytes: &[u8], cols: u16) -> vt100::Parser {
        let mut parser = vt100::Parser::new(80, cols, 0);
        parser.process(bytes);
        parser
    }

    #[test]
    fn test_matches_common_shell_prompts() {
        let matcher = MarkerMatcher::default();
        for prompt in [b"$ ".as_slice(), b"# ".as_slice(), b"> ".as_slice()] {
            let parser = parser_with(prompt);
            assert_eq!(matcher.scan(&parser), MatchResult::Matched);
        }
    }

    #[test]
    fn test_matches_unicode_prompt() {
        let matcher = MarkerMatcher::default();
        let parser = parser_with("✦ ".as_bytes());

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn test_ansi_colored_prompt_matches_after_vt100_processing() {
        let matcher = MarkerMatcher::default();
        let parser = parser_with(b"\x1b[32m$\x1b[0m ");

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn test_plain_output_does_not_match() {
        let matcher = MarkerMatcher::default();
        let parser = parser_with(b"hello\nworld\n");

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn test_observed_stability_matches_ready_anchor_within_bottom_viewport() {
        let matcher = MarkerMatcher {
            mode: IdleDetectionMode::ObservedStability,
            regex: Regex::new(r"READY_PROMPT").unwrap(),
            anti_regex: None,
        };
        let parser = parser_with(
            b"history outside viewport\nline 1\nline 2\nline 3\nline 4\nline 5\nREADY_PROMPT\n",
        );

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn test_observed_stability_does_not_match_unrelated_shell_dollar() {
        let matcher = MarkerMatcher {
            mode: IdleDetectionMode::ObservedStability,
            regex: Regex::new(r"READY_PROMPT").unwrap(),
            anti_regex: None,
        };
        let parser = parser_with(b"price is $5\n");

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn test_ready_anchor_above_bottom_viewport_does_not_match() {
        let matcher = MarkerMatcher::default();
        let parser = parser_with(b"$ \nline 1\nline 2\nline 3\nline 4\nline 5\nline 6\n");

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn test_ready_anchor_within_bottom_viewport_matches() {
        let matcher = MarkerMatcher::default();
        let parser =
            parser_with(b"history outside viewport\nline 1\nline 2\nline 3\nline 4\nline 5\n$ ");

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn ready_anchor_above_bottom_viewport_is_no_match() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "› \n\
             body line 1\n\
             body line 2\n\
             body line 3\n\
             body line 4\n\
             body line 5\n\
             body line 6\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn ready_anchor_within_bottom_viewport_matches() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "history outside viewport\n\
             body line 1\n\
             body line 2\n\
             body line 3\n\
             body line 4\n\
             body line 5\n\
             › \n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn test_marker_matcher_codex_suppresses_idle_when_working_spinner_present() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with("› echo from a1\n◦ Working (2s • esc to interrupt)\n".as_bytes());

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn test_marker_matcher_codex_marks_idle_when_no_spinner() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with("› \n  gpt-5.5 default · /workspace\n".as_bytes());

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn codex_ready_composer_with_esc_to_interrupt_is_busy() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "older output\n› \n◦ Working (2s • esc to interrupt)\n  gpt-5.5 default · /workspace\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn codex_ready_composer_without_busy_anchor_is_idle_after_stability() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "docs above mention esc to interrupt as a shortcut\n\
             transcript body line\n\
             transcript body line\n\
             transcript body line\n\
             transcript body line\n\
             transcript body line\n\
             transcript body line\n\
             › \n\
               gpt-5.5 default · /workspace\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn codex_hook_trust_modal_is_not_idle() {
        let manifest = get_manifest("codex");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "Hooks need review\n\
             Trust all and continue\n\
             Continue without trusting\n\
             › \n\
               gpt-5.5 default · /workspace\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn test_marker_matcher_antigravity_marks_idle_from_status_line() {
        let manifest = get_manifest("antigravity");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "Antigravity\n────────────────\n? for shortcuts                          Gemini 3.5 Flash (High)\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn test_marker_matcher_antigravity_suppresses_idle_when_cancel_status_present() {
        let manifest = get_manifest("antigravity");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "⣯ Generating...\n▸ Thought for 2s, 1k tokens\nesc to cancel                          Gemini 3.5 Flash (High)\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn antigravity_real_idle_capture_matches() {
        let bytes = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/pane_idle/REAL-a3-idle-capture.txt"
        ))
        .as_bytes();
        let mut parser = vt100::Parser::new(200, 200, 0);
        parser.process(bytes);
        let manifest = get_manifest("antigravity");
        let matcher = MarkerMatcher::from_manifest(&manifest);

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn antigravity_body_mentions_esc_to_cancel_but_bottom_ready_is_idle() {
        let manifest = get_manifest("antigravity");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "esc to cancel is mentioned in body text, not status\n\
             body response line 1\n\
             body response line 2\n\
             body response line 3\n\
             body response line 4\n\
             body response line 5\n\
             body response line 6\n\
             ? for shortcuts                          Gemini 3.5 Flash (High)\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn antigravity_bottom_generating_or_esc_to_cancel_is_busy() {
        let manifest = get_manifest("antigravity");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "body response line\n\
             ? for shortcuts                          Gemini 3.5 Flash (High)\n\
             ⣯ Generating...\n\
             esc to cancel                          Gemini 3.5 Flash (High)\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn sentinel_still_wins_before_viewport_filter() {
        let matcher = MarkerMatcher::default();
        let parser = parser_with(
            "<<ah-idle:job-id=job-123>>\n\
             long body without a prompt\n\
             still no prompt in the bottom viewport\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn claude_empty_idle_prompt_still_matches() {
        let manifest = get_manifest("claude");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with("❯\n".as_bytes());

        assert_eq!(matcher.scan(&parser), MatchResult::Matched);
    }

    #[test]
    fn claude_working_status_with_ready_composer_is_busy() {
        let manifest = get_manifest("claude");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let parser = parser_with(
            "Read src/provider/manifest.rs\n\
             ❯ \n\
             Reading 1 file (ctrl+o to expand)\n\
             Architecting (4s) · esc to interrupt\n\
             paste again to expand\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&parser), MatchResult::NoMatch);
    }

    #[test]
    fn claude_idle_marker_accepts_try_placeholder_without_matching_non_input_prompt() {
        let manifest = get_manifest("claude");
        let matcher = MarkerMatcher::from_manifest(&manifest);
        let placeholder_idle = parser_with(
            "⚠ setup warning\n\
             ▎ Meet Fable 5\n\
             transcript line\n\
             transcript line\n\
             ❯ Try \"fix lint errors\"\n\
             ──────────────────────────────────────────────\n\
               ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents\n"
                .as_bytes(),
        );
        let empty_idle = parser_with("❯\n".as_bytes());
        let non_input_prompt = parser_with(
            "Claude Code\n\
             Setup needs attention\n\
             Do you want to proceed? [y/N]\n"
                .as_bytes(),
        );

        assert_eq!(matcher.scan(&placeholder_idle), MatchResult::Matched);
        assert_eq!(matcher.scan(&empty_idle), MatchResult::Matched);
        assert_eq!(matcher.scan(&non_input_prompt), MatchResult::NoMatch);
    }

    #[test]
    fn provider_idle_markers_survive_narrow_terminal_wrapping() {
        for (provider, capture) in [
            (
                "codex",
                "transcript body\n› \n  gpt-5.5 default · /workspace/with/a/long/path\n",
            ),
            (
                "claude",
                "transcript body\n❯ Try \"fix lint errors in the whole workspace\"\n──────────────────────────────\n",
            ),
            (
                "antigravity",
                "Antigravity\n────────────────\n? for shortcuts                          Gemini 3.5 Flash (High)\n",
            ),
        ] {
            let manifest = get_manifest(provider);
            let matcher = MarkerMatcher::from_manifest(&manifest);
            let wide = parser_with_width(capture.as_bytes(), 120);
            let narrow = parser_with_width(capture.as_bytes(), 24);

            assert_eq!(matcher.scan(&wide), MatchResult::Matched, "{provider} wide");
            assert_eq!(
                matcher.scan(&narrow),
                MatchResult::Matched,
                "{provider} narrow"
            );
        }
    }

    #[test]
    fn provider_busy_markers_survive_narrow_terminal_wrapping() {
        for (provider, capture) in [
            (
                "codex",
                "› rewrite the parser\n◦ Working (2s • esc to interrupt)\n  gpt-5.5 default · /workspace/with/a/long/path\n",
            ),
            (
                "claude",
                "❯ \nReading 12 files (ctrl+o to expand)\nArchitecting (4s) · esc to interrupt\npaste again to expand\n",
            ),
            (
                "antigravity",
                "? for shortcuts                          Gemini 3.5 Flash (High)\n⣯ Generating...\nesc to cancel                          Gemini 3.5 Flash (High)\n",
            ),
        ] {
            let manifest = get_manifest(provider);
            let matcher = MarkerMatcher::from_manifest(&manifest);
            let wide = parser_with_width(capture.as_bytes(), 120);
            let narrow = parser_with_width(capture.as_bytes(), 24);

            assert_eq!(matcher.scan(&wide), MatchResult::NoMatch, "{provider} wide");
            assert_eq!(
                matcher.scan(&narrow),
                MatchResult::NoMatch,
                "{provider} narrow"
            );
        }
    }
}
