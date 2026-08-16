//! Synchronous Anthropic Haiku client for prompt slow-path classification.

use crate::prompt_handler::matcher::sanitize_pane_text;
use crate::prompt_handler::schema::PromptAction;
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use thiserror::Error;

const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const HAIKU_45_MODEL: &str = "claude-haiku-4-5-20251001";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const MAX_CONTEXT_CHARS: usize = 2000;

#[derive(Debug, Clone, PartialEq)]
pub struct LlmOutcome {
    pub category: String,
    pub action: Vec<PromptAction>,
    pub confidence: f64,
    pub safe: bool,
    pub suggested_regex: Option<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LlmError {
    #[error("missing Anthropic API key")]
    MissingKey,

    #[error("LLM request timed out")]
    Timeout,

    #[error("LLM transport failed: {0}")]
    Transport(String),

    #[error("LLM response JSON invalid: {0}")]
    InvalidResponse(String),

    #[error("LLM output JSON invalid: {0}")]
    InvalidOutput(String),
}

pub trait LlmClassifier: Send + Sync {
    fn classify_prompt(
        &self,
        capture: &str,
        provider: &str,
        agent_state: &str,
    ) -> Result<LlmOutcome, LlmError>;
}

pub trait LlmTransport: Send + Sync {
    fn post_json(&self, url: &str, api_key: &str, body: &str) -> Result<String, LlmError>;
}

pub struct RealHaikuClassifier;

impl LlmClassifier for RealHaikuClassifier {
    fn classify_prompt(
        &self,
        capture: &str,
        provider: &str,
        agent_state: &str,
    ) -> Result<LlmOutcome, LlmError> {
        call_haiku_45_sync(capture, provider, agent_state)
    }
}

pub struct UreqTransport;

impl LlmTransport for UreqTransport {
    fn post_json(&self, url: &str, api_key: &str, body: &str) -> Result<String, LlmError> {
        ureq::post(url)
            .set("x-api-key", api_key)
            .set("anthropic-version", ANTHROPIC_VERSION)
            .set("content-type", "application/json")
            .send_string(body)
            .map_err(map_ureq_error)?
            .into_string()
            .map_err(|err| LlmError::Transport(err.to_string()))
    }
}

fn map_ureq_error(err: ureq::Error) -> LlmError {
    let message = err.to_string();
    if message.to_ascii_lowercase().contains("timeout")
        || message.to_ascii_lowercase().contains("timed out")
    {
        LlmError::Timeout
    } else {
        LlmError::Transport(message)
    }
}

pub fn call_haiku_45_sync(
    capture: &str,
    provider: &str,
    agent_state: &str,
) -> Result<LlmOutcome, LlmError> {
    let api_key = resolve_anthropic_api_key().ok_or(LlmError::MissingKey)?;
    call_haiku_45_with_transport(capture, provider, agent_state, &api_key, &UreqTransport)
}

pub fn call_haiku_45_with_transport(
    capture: &str,
    provider: &str,
    agent_state: &str,
    api_key: &str,
    transport: &dyn LlmTransport,
) -> Result<LlmOutcome, LlmError> {
    call_haiku_45_with_transport_and_base_url(
        capture,
        provider,
        agent_state,
        api_key,
        &resolve_anthropic_base_url(),
        transport,
    )
}

pub fn call_haiku_45_with_transport_and_base_url(
    capture: &str,
    provider: &str,
    agent_state: &str,
    api_key: &str,
    base_url: &str,
    transport: &dyn LlmTransport,
) -> Result<LlmOutcome, LlmError> {
    let body = build_request_body(capture, provider, agent_state);
    let response = transport.post_json(&anthropic_messages_url(base_url), api_key, &body)?;
    parse_anthropic_response(&response)
}

fn build_request_body(capture: &str, provider: &str, agent_state: &str) -> String {
    let sanitized = tail_chars(&sanitize_pane_text(capture), MAX_CONTEXT_CHARS);
    json!({
        "model": HAIKU_45_MODEL,
        "max_tokens": 512,
        "temperature": 0,
        "system": "Identify whether a terminal pane shows an interactive CLI prompt. Return only one compact JSON object. Schema: {\"is_interactive_prompt\":bool,\"category\":string,\"action\":[{\"type\":\"key\",\"value\":string}],\"confidence\":number,\"safe\":bool,\"suggested_regex\":string|null}. The action field must always be a JSON array, never a string. Use an empty action array when no safe automatic keypress exists.",
        "messages": [{
            "role": "user",
            "content": format!(
                "provider: {provider}\nagent_state: {agent_state}\npane_snapshot:\n{sanitized}"
            )
        }]
    })
    .to_string()
}

fn parse_anthropic_response(response: &str) -> Result<LlmOutcome, LlmError> {
    let envelope: AnthropicResponse =
        serde_json::from_str(response).map_err(|err| LlmError::InvalidResponse(err.to_string()))?;
    let text = envelope
        .content
        .into_iter()
        .find(|content| content.kind == "text")
        .map(|content| content.text)
        .ok_or_else(|| LlmError::InvalidResponse("missing text content".to_string()))?;
    let output_text = extract_json_object(&text)
        .ok_or_else(|| LlmError::InvalidOutput("missing JSON object".to_string()))?;
    let output: LlmOutput = serde_json::from_str(output_text)
        .map_err(|err| LlmError::InvalidOutput(err.to_string()))?;
    if !output.is_interactive_prompt {
        return Err(LlmError::InvalidOutput(
            "not an interactive prompt".to_string(),
        ));
    }
    Ok(LlmOutcome {
        category: output.category,
        action: output.action,
        confidence: output.confidence,
        safe: output.safe,
        suggested_regex: output.suggested_regex,
    })
}

fn resolve_anthropic_api_key() -> Option<String> {
    std::env::var("ANTHROPIC_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| config_api_key_from_path(std::env::var_os("AH_CONFIG_PATH").map(PathBuf::from)))
        .or_else(|| config_api_key_from_path(find_cwd_config()))
        .or_else(|| config_api_key_from_path(home_config_path()))
}

fn resolve_anthropic_base_url() -> String {
    std::env::var("ANTHROPIC_BASE_URL")
        .ok()
        .and_then(non_empty_trimmed)
        .or_else(|| {
            config_base_url_from_path(std::env::var_os("AH_CONFIG_PATH").map(PathBuf::from))
        })
        .or_else(|| config_base_url_from_path(find_cwd_config()))
        .or_else(|| config_base_url_from_path(home_config_path()))
        .unwrap_or_else(|| DEFAULT_ANTHROPIC_BASE_URL.to_string())
}

fn config_api_key_from_path(path: Option<PathBuf>) -> Option<String> {
    let path = path?;
    let raw = std::fs::read_to_string(path).ok()?;
    api_key_from_toml(&raw)
}

fn api_key_from_toml(raw: &str) -> Option<String> {
    raw.parse::<toml::Value>()
        .ok()?
        .get("auth")?
        .get("anthropic")?
        .get("api_key")?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn config_base_url_from_path(path: Option<PathBuf>) -> Option<String> {
    let path = path?;
    let raw = std::fs::read_to_string(path).ok()?;
    base_url_from_toml(&raw)
}

fn base_url_from_toml(raw: &str) -> Option<String> {
    raw.parse::<toml::Value>()
        .ok()?
        .get("auth")?
        .get("anthropic")?
        .get("base_url")?
        .as_str()
        .and_then(non_empty_trimmed)
}

fn non_empty_trimmed(value: impl AsRef<str>) -> Option<String> {
    let trimmed = value.as_ref().trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn anthropic_messages_url(base_url: &str) -> String {
    format!("{}/v1/messages", base_url.trim().trim_end_matches('/'))
}

fn find_cwd_config() -> Option<PathBuf> {
    let mut current = std::env::current_dir().ok()?;
    loop {
        let candidate = current.join("ah.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn home_config_path() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".ah").join("config.toml"))
        .filter(|path| path.is_file())
}

fn tail_chars(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars().rev().take(max_chars).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

fn extract_json_object(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    (start <= end).then_some(&text[start..=end])
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContent>,
}

#[derive(Debug, Deserialize)]
struct AnthropicContent {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct LlmOutput {
    is_interactive_prompt: bool,
    category: String,
    action: Vec<PromptAction>,
    confidence: f64,
    safe: bool,
    suggested_regex: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ANTHROPIC_BASE_URL, LlmError, LlmTransport, UreqTransport, anthropic_messages_url,
        api_key_from_toml, base_url_from_toml, build_request_body, call_haiku_45_sync,
        call_haiku_45_with_transport, call_haiku_45_with_transport_and_base_url,
        parse_anthropic_response,
    };
    use crate::prompt_handler::schema::PromptAction;
    use std::sync::Mutex;

    struct FakeTransport {
        response: Result<String, LlmError>,
        requests: Mutex<Vec<FakeRequest>>,
    }

    struct FakeRequest {
        url: String,
        body: String,
    }

    impl FakeTransport {
        fn success(response: &str) -> Self {
            Self {
                response: Ok(response.to_string()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn failure(error: LlmError) -> Self {
            Self {
                response: Err(error),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl LlmTransport for FakeTransport {
        fn post_json(&self, url: &str, _api_key: &str, body: &str) -> Result<String, LlmError> {
            self.requests.lock().unwrap().push(FakeRequest {
                url: url.to_string(),
                body: body.to_string(),
            });
            self.response.clone()
        }
    }

    fn anthropic_response(output_json: &str) -> String {
        json_string(&format!(
            r#"{{
                "content": [{{
                    "type": "text",
                    "text": {output}
                }}]
            }}"#,
            output = serde_json::to_string(output_json).unwrap()
        ))
    }

    fn json_string(value: &str) -> String {
        serde_json::from_str::<serde_json::Value>(value)
            .unwrap()
            .to_string()
    }

    #[test]
    fn parses_haiku_prompt_json_from_text_content() {
        let response = anthropic_response(
            r#"{"is_interactive_prompt":true,"category":"auto-accept","action":[{"type":"key","value":"Enter"}],"confidence":0.91,"safe":true,"suggested_regex":"Accept terms"}"#,
        );

        let outcome = parse_anthropic_response(&response).unwrap();

        assert_eq!(outcome.category, "auto-accept");
        assert_eq!(
            outcome.action,
            vec![PromptAction::Key {
                value: "Enter".into()
            }]
        );
        assert_eq!(outcome.confidence, 0.91);
        assert!(outcome.safe);
        assert_eq!(outcome.suggested_regex.as_deref(), Some("Accept terms"));
    }

    #[test]
    fn mock_transport_receives_sanitized_context_and_returns_outcome() {
        let response = anthropic_response(
            r#"{"is_interactive_prompt":true,"category":"auto-skip","action":[{"type":"key","value":"2"}],"confidence":0.93,"safe":true}"#,
        );
        let transport = FakeTransport::success(&response);

        let outcome = call_haiku_45_with_transport_and_base_url(
            "\u{1b}[31mProvider prompt\u{1b}[0m\nAccept?",
            "codex",
            "BUSY",
            "test-key",
            DEFAULT_ANTHROPIC_BASE_URL,
            &transport,
        )
        .unwrap();

        assert_eq!(outcome.category, "auto-skip");
        assert_eq!(
            outcome.action,
            vec![PromptAction::Key { value: "2".into() }]
        );
        let request = transport.requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.url, "https://api.anthropic.com/v1/messages");
        assert!(request.body.contains("provider: codex"));
        assert!(request.body.contains("agent_state: BUSY"));
        assert!(request.body.contains("Provider prompt"));
        assert!(!request.body.contains("\u{1b}[31m"));
    }

    #[test]
    fn custom_base_url_is_used_for_messages_endpoint() {
        let response = anthropic_response(
            r#"{"is_interactive_prompt":true,"category":"auto-accept","action":[{"type":"key","value":"y"}],"confidence":0.94,"safe":true}"#,
        );
        let transport = FakeTransport::success(&response);

        let outcome = call_haiku_45_with_transport_and_base_url(
            "Allow command? [y/n]",
            "codex",
            "BUSY",
            "test-key",
            "https://api.jiekou.ai/anthropic/",
            &transport,
        )
        .unwrap();

        assert_eq!(outcome.category, "auto-accept");
        let request = transport.requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.url, "https://api.jiekou.ai/anthropic/v1/messages");
    }

    #[test]
    fn base_url_config_reads_auth_anthropic_table_and_defaults_to_official() {
        let base_url = base_url_from_toml(
            r#"
            [auth.anthropic]
            base_url = " https://api.jiekou.ai/anthropic/ "
            "#,
        );

        assert_eq!(
            base_url.as_deref(),
            Some("https://api.jiekou.ai/anthropic/")
        );
        assert_eq!(
            anthropic_messages_url(base_url.as_deref().unwrap()),
            "https://api.jiekou.ai/anthropic/v1/messages"
        );
        assert_eq!(
            anthropic_messages_url(DEFAULT_ANTHROPIC_BASE_URL),
            "https://api.anthropic.com/v1/messages"
        );
    }

    #[test]
    fn mock_transport_error_maps_to_llm_error() {
        let transport = FakeTransport::failure(LlmError::Transport("timeout".into()));

        let err = call_haiku_45_with_transport("prompt", "codex", "BUSY", "test-key", &transport)
            .unwrap_err();

        assert_eq!(err, LlmError::Transport("timeout".into()));
    }

    #[test]
    fn config_api_key_reads_auth_anthropic_table() {
        let key = api_key_from_toml(
            r#"
            [auth.anthropic]
            api_key = "sk-test"
            "#,
        );

        assert_eq!(key.as_deref(), Some("sk-test"));
    }

    #[test]
    #[ignore = "requires real Anthropic-compatible API credentials"]
    fn real_haiku_prompt_classification_smoke() {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .expect("set ANTHROPIC_API_KEY, e.g. from JIEKOU_API_KEY");
        let base_url = std::env::var("ANTHROPIC_BASE_URL")
            .expect("set ANTHROPIC_BASE_URL, e.g. https://api.jiekou.ai/anthropic");
        let capture = "Allow command? [y/n]";
        let body = build_request_body(capture, "codex", "BUSY");
        let raw = UreqTransport
            .post_json(&anthropic_messages_url(&base_url), &api_key, &body)
            .expect("real Haiku request should succeed");
        println!("REAL_HAIKU_RAW_RESPONSE={raw}");

        let parsed = parse_anthropic_response(&raw).expect("real Haiku response should parse");
        println!("REAL_HAIKU_PARSED_OUTCOME={parsed:?}");
        assert!(!parsed.category.trim().is_empty());
        assert!(parsed.confidence >= 0.8);
        if !parsed.safe {
            assert!(parsed.action.is_empty());
        }

        let via_public_api = call_haiku_45_sync(capture, "codex", "BUSY")
            .expect("call_haiku_45_sync should use configured base URL");
        println!("REAL_HAIKU_CALL_SYNC_OUTCOME={via_public_api:?}");
        assert!(!via_public_api.category.trim().is_empty());
        assert!(via_public_api.confidence >= 0.8);
        if !via_public_api.safe {
            assert!(via_public_api.action.is_empty());
        }
    }
}
