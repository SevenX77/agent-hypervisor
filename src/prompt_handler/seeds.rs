//! Built-in prompt-handler cases used to bootstrap the user knowledge base.

use crate::prompt_handler::schema::{PromptAction, PromptCase, PromptFingerprint};

pub fn default_cases() -> Vec<PromptCase> {
    vec![codex_update_case(), trust_path_case()]
}

fn codex_update_case() -> PromptCase {
    PromptCase {
        id: "codex_update_01".to_string(),
        provider: Some("codex".to_string()),
        fingerprint: PromptFingerprint::Regex {
            pattern: r"(?is)update available!?.*?(?:^|\n)\s*›?\s*1\.\s*update now.*(?:^|\n)\s*2\.\s*skip\b.*(?:^|\n)\s*3\.\s*skip until next version\b.*press enter to continue".to_string(),
        },
        action: vec![
            PromptAction::Key {
                value: "Down".to_string(),
            },
            PromptAction::Key {
                value: "Enter".to_string(),
            },
        ],
        category: "auto-skip".to_string(),
        description: Some("Skip codex global update to avoid EACCES".to_string()),
        confidence_threshold: Some(0.9),
        used_count: 0,
        created_at: None,
        last_used_at: None,
        created_by: Some("ah-default".to_string()),
        regex_flags: vec!["Dotall".to_string(), "CaseInsensitive".to_string()],
        trigger_state: None,
    }
}

fn trust_path_case() -> PromptCase {
    PromptCase {
        id: "trust_path_01".to_string(),
        provider: None,
        fingerprint: PromptFingerprint::Regex {
            pattern: r"(?is)do you trust.{0,80}(?:directory|folder|path|workspace|project)|(?:trust|trusted).{0,80}(?:directory|folder|path|workspace|project)".to_string(),
        },
        action: vec![
            PromptAction::Key {
                value: "1".to_string(),
            },
            PromptAction::Key {
                value: "Enter".to_string(),
            },
        ],
        category: "auto-accept".to_string(),
        description: Some("Trust current project directory for interactive CLI startup".to_string()),
        confidence_threshold: Some(0.9),
        used_count: 0,
        created_at: None,
        last_used_at: None,
        created_by: Some("ah-default".to_string()),
        regex_flags: vec!["Dotall".to_string(), "CaseInsensitive".to_string()],
        trigger_state: None,
    }
}

#[cfg(test)]
mod tests {
    use super::default_cases;
    use crate::prompt_handler::schema::{PromptAction, PromptFingerprint};
    use regex::Regex;

    #[test]
    fn default_cases_include_codex_update_and_trust_path() {
        let cases = default_cases();

        assert_eq!(cases.len(), 2);
        assert_eq!(cases[0].id, "codex_update_01");
        assert_eq!(cases[1].id, "trust_path_01");
        assert_eq!(
            cases[0].action,
            vec![
                PromptAction::Key {
                    value: "Down".into()
                },
                PromptAction::Key {
                    value: "Enter".into()
                }
            ]
        );
        assert_eq!(
            cases[1].action,
            vec![
                PromptAction::Key { value: "1".into() },
                PromptAction::Key {
                    value: "Enter".into()
                }
            ]
        );
    }

    #[test]
    fn default_case_patterns_compile_and_match_expected_prompts() {
        let cases = default_cases();

        let codex_pattern = match &cases[0].fingerprint {
            PromptFingerprint::Regex { pattern } => pattern,
        };
        let trust_pattern = match &cases[1].fingerprint {
            PromptFingerprint::Regex { pattern } => pattern,
        };
        let codex = Regex::new(codex_pattern).unwrap();
        let trust = Regex::new(trust_pattern).unwrap();

        assert!(
            !codex.is_match("Update available! 0.129 -> 0.130\nRun `npm install -g @openai/codex`")
        );
        assert!(trust.is_match("Do you trust this directory?\n1) Yes\n2) No"));
        assert!(trust.is_match("Trust this workspace before running commands?"));
    }
}
