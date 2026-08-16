//! Provider TUI readiness probes.
//!
//! These predicates are direct Rust ports of Python's
//! `provider_backends/<provider>/init_probe.py` helpers. They operate on a
//! visible tmux pane capture supplied by the caller and return false for any
//! ambiguous state.

/// Probe visible pane content to determine whether a provider TUI is ready.
pub trait InitGateProbe: Send + Sync {
    fn detect(&self, capture: &str) -> bool;
}

pub struct ClaudeInitProbe;

const CLAUDE_INIT_BANNERS: &[&str] = &[
    "Welcome to Claude Code",
    "Setup Wizard",
    "Choose the text style",
    "Choose your preferred theme",
    "Let's get started",
    "Trust the files in this folder",
    "Press Enter to continue",
];
const CLAUDE_PROMPT_PREFIXES: &[&str] = &["❯ ", "❯"];
const CLAUDE_STEADY_MARKERS: &[&str] = &["Sonnet", "Haiku", "Opus"];

impl InitGateProbe for ClaudeInitProbe {
    fn detect(&self, capture: &str) -> bool {
        Self::banner_gone(capture)
            && Self::prompt_present(capture)
            && Self::steady_marker_present(capture)
    }
}

impl ClaudeInitProbe {
    fn banner_gone(capture: &str) -> bool {
        let capture_lower = capture.to_lowercase();
        for banner in CLAUDE_INIT_BANNERS {
            if capture_lower.contains(&banner.to_lowercase()) {
                return false;
            }
        }
        true
    }

    fn prompt_present(capture: &str) -> bool {
        let lines = non_empty_lines(capture);
        if lines.is_empty() {
            return false;
        }
        lines.iter().rev().take(8).any(|line| {
            CLAUDE_PROMPT_PREFIXES
                .iter()
                .any(|prefix| line.lstrip().starts_with(prefix))
        })
    }

    fn steady_marker_present(capture: &str) -> bool {
        CLAUDE_STEADY_MARKERS
            .iter()
            .any(|marker| capture.contains(marker))
    }
}

pub struct AntigravityInitProbe;

const ANTIGRAVITY_ONBOARDING_MARKERS: &[&str] = &[
    "Yes, I trust this folder",
    "Terms of Service",
    "[Next]",
    "[Done]",
];

impl InitGateProbe for AntigravityInitProbe {
    fn detect(&self, capture: &str) -> bool {
        Self::onboarding_gone(capture) && Self::shortcut_status_present(capture)
    }
}

impl AntigravityInitProbe {
    fn onboarding_gone(capture: &str) -> bool {
        let capture_lower = capture.to_lowercase();
        for marker in ANTIGRAVITY_ONBOARDING_MARKERS {
            if capture_lower.contains(&marker.to_lowercase()) {
                return false;
            }
        }
        true
    }

    fn shortcut_status_present(capture: &str) -> bool {
        capture
            .lines()
            .any(|line| line.trim_start().starts_with("? for shortcuts"))
    }
}

pub struct CodexInitProbe;

const CODEX_INIT_BANNERS: &[&str] = &[
    "Sign in with ChatGPT",
    "Trust this workspace",
    "Choose a model",
    "✦ Welcome to",
];

impl InitGateProbe for CodexInitProbe {
    fn detect(&self, capture: &str) -> bool {
        Self::banner_gone(capture) && Self::prompt_on_last_line(capture)
    }
}

impl CodexInitProbe {
    fn banner_gone(capture: &str) -> bool {
        let capture_lower = capture.to_lowercase();
        for banner in CODEX_INIT_BANNERS {
            if capture_lower.contains(&banner.to_lowercase()) {
                return false;
            }
        }
        true
    }

    fn prompt_on_last_line(capture: &str) -> bool {
        let lines = non_empty_lines(capture);
        // mvp12 M12.6: Codex v0.125+ renders a model/cwd footer after the
        // idle prompt. Search the lower prompt region while staying clear of
        // historical scrollback.
        lines
            .iter()
            .rev()
            .take(6)
            .any(|line| line.lstrip().starts_with('›'))
    }
}

pub struct BashInitProbe;

impl InitGateProbe for BashInitProbe {
    fn detect(&self, capture: &str) -> bool {
        let lines = non_empty_lines(capture);
        let Some(last) = lines.last() else {
            return false;
        };
        let trimmed = last.trim_end();
        trimmed.ends_with('$')
            || trimmed.ends_with('#')
            || trimmed.ends_with("$ ")
            || trimmed.ends_with("# ")
    }
}

trait LStrip {
    fn lstrip(&self) -> &str;
}

impl LStrip for str {
    fn lstrip(&self) -> &str {
        self.trim_start()
    }
}

fn non_empty_lines(capture: &str) -> Vec<&str> {
    capture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        AntigravityInitProbe, BashInitProbe, ClaudeInitProbe, CodexInitProbe, InitGateProbe,
    };

    #[test]
    fn codex_rejects_banner_and_accepts_idle_prompt() {
        let probe = CodexInitProbe;
        assert!(!probe.detect("Trust this workspace\n› ready"));
        assert!(probe.detect("worktree clean\n  › ask me anything"));
    }

    #[test]
    fn codex_rejects_prompt_outside_bottom_region() {
        let probe = CodexInitProbe;
        // Loosened for mvp12 M12.6 TUI drift tolerance: prompts can appear
        // above a footer, but not arbitrarily far back in scrollback.
        assert!(!probe.detect("› old prompt\n1\n2\n3\n4\n5\n6\nloading"));
    }

    #[test]
    fn codex_accepts_v0125_footer_after_prompt() {
        let probe = CodexInitProbe;
        assert!(probe.detect(
            "╭──────────────────────────────────────────────╮\n\
             │ >_ OpenAI Codex (v0.125.0)                   │\n\
             │                                              │\n\
             │ model:       gpt-5.5   /model to change      │\n\
             │ directory:   ~/coding/agent-hypervisor           │\n\
             │ permissions: YOLO mode                       │\n\
             ╰──────────────────────────────────────────────╯\n\
               Tip: New Use /fast to enable our fastest inference with increased plan usage.\n\
             › Summarize recent commits\n\
               gpt-5.5 default · ~/coding/agent-hypervisor"
        ));
    }

    #[test]
    fn claude_requires_banner_gone_prompt_and_model_marker() {
        let probe = ClaudeInitProbe;
        assert!(!probe.detect("Welcome to Claude Code\n❯\nSonnet"));
        assert!(!probe.detect("ready\n❯"));
        assert!(probe.detect("status Sonnet\n────────\n  ❯"));
    }

    #[test]
    fn claude_prompt_can_appear_in_bottom_region() {
        let probe = ClaudeInitProbe;
        assert!(probe.detect("Sonnet\n❯ \nline\nline\nline"));
    }

    #[test]
    fn claude_rejects_prompt_outside_bottom_region() {
        let probe = ClaudeInitProbe;
        assert!(!probe.detect("Sonnet\n❯ \n1\n2\n3\n4\n5\n6\n7\n8\n9\nbottom line"));
    }

    #[test]
    fn antigravity_accepts_shortcuts_status_line() {
        let probe = AntigravityInitProbe;
        assert!(
            probe.detect(
                "ready\n\n? for shortcuts                          Gemini 3.5 Flash (High)\n"
            )
        );
    }

    #[test]
    fn antigravity_rejects_theme_onboarding() {
        let probe = AntigravityInitProbe;
        assert!(!probe.detect("Choose your theme\n[Next]\n? for shortcuts"));
    }

    #[test]
    fn antigravity_rejects_terms_onboarding() {
        let probe = AntigravityInitProbe;
        assert!(!probe.detect("Terms of Service\n[Done]\n? for shortcuts"));
    }

    #[test]
    fn antigravity_rejects_empty_capture() {
        let probe = AntigravityInitProbe;
        assert!(!probe.detect(""));
    }

    #[test]
    fn banners_are_matched_case_insensitively() {
        let codex = CodexInitProbe;
        let claude = ClaudeInitProbe;

        assert!(!codex.detect("trust this workspace\n› "));
        assert!(!claude.detect("welcome to claude code\nSonnet\n❯ "));
    }

    #[test]
    fn empty_captures_are_not_ready() {
        assert!(!CodexInitProbe.detect(""));
        assert!(!ClaudeInitProbe.detect(""));
        assert!(!BashInitProbe.detect(""));
    }

    #[test]
    fn bash_detects_shell_prompt_on_last_line() {
        let probe = BashInitProbe;
        assert!(probe.detect("hello\n$ "));
        assert!(probe.detect("hello\nroot# "));
        assert!(!probe.detect("hello\nno prompt"));
    }
}
