//! Provider-neutral orchestration of provider-owned authentication flows.
//!
//! AH never performs OAuth token exchange and never persists credential
//! values. Provider login flows are isolated in a temporary tmux server so AH
//! can recover and validate browser challenges even when a provider hard-wraps
//! its terminal output. Each fragile action is observed, dispatched once, and
//! causally confirmed before the next action.

use crate::guarded_action::{
    ActionAssessment, GuardedActionError, run_guarded_action_sync_from_before,
};
use crate::platform::browser::{BrowserOpenError, BrowserOpenerPort, SystemBrowserOpenerAdapter};
use crate::provider::auth_store::{AuthStoreStatus, check_auth_store_for};
use crate::provider::auth_ui;
use crate::provider::{AuthChallengeKind, ProviderAuthSpec, ProviderLoginDriverSpec};
use crate::tmux::{TmuxPaneId, TmuxServer};
use regex::Regex;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const LOGIN_SCREEN_TIMEOUT: Duration = Duration::from_secs(30);
const LOGIN_ACTION_TIMEOUT: Duration = Duration::from_secs(20);
const LOGIN_COMPLETION_TIMEOUT: Duration = Duration::from_secs(120);
const DEVICE_CODE_COMPLETION_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const LOGIN_POLL: Duration = Duration::from_millis(200);
const AUTH_PANE_WIDTH: u16 = 220;
const AUTH_PANE_HEIGHT: u16 = 60;

#[derive(Debug, thiserror::Error)]
pub enum AuthFlowError {
    #[error("unknown provider `{0}`")]
    UnknownProvider(String),
    #[error("provider `{0}` does not declare an authentication flow")]
    NoLoginFlow(String),
    #[error("could not run `{command}`: {details}")]
    CommandLaunch { command: String, details: String },
    #[error("provider `{provider}` login returned but authentication is still not healthy")]
    CompletionNotObserved { provider: String },
    #[error("provider `{provider}` did not confirm authentication before the deadline")]
    CompletionTimeout { provider: String },
    #[error("provider `{provider}` login UI did not reach {purpose} before the deadline")]
    UiTimeout {
        provider: String,
        purpose: &'static str,
    },
    #[error("provider `{provider}` login UI did not expose a usable HTTPS authorization URL")]
    MissingAuthorizationUrl { provider: String },
    #[error("provider `{provider}` login UI did not expose a complete device code")]
    MissingDeviceCode { provider: String },
    #[error("authorization-code interaction failed: {0}")]
    AuthorizationInteraction(String),
    #[error(
        "provider `{provider}` rejected the authorization code ({marker}); restart login and copy a fresh code from the current browser challenge"
    )]
    AuthorizationRejected {
        provider: String,
        marker: &'static str,
    },
    #[error("provider `{provider}` login process exited before authentication became healthy")]
    ProviderExited { provider: String },
    #[error(
        "provider `{provider}` did not confirm the submitted authorization code: {details}; restart login and press v after copying the complete browser code"
    )]
    AuthorizationSubmission { provider: String, details: String },
    #[error("provider `{provider}` guarded action failed: {details}")]
    GuardedAction { provider: String, details: String },
    #[error("provider `{provider}` tmux login session failed: {details}")]
    Tmux { provider: String, details: String },
}

#[derive(Debug, thiserror::Error)]
enum AuthorizationChallengeStepError {
    #[error("terminal interaction failed: {0}")]
    Terminal(String),
    #[error("provider reported `{marker}`")]
    ProviderRejected { marker: &'static str },
    #[error("provider login process exited before authentication became healthy")]
    ProviderExited,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupAuthDisposition {
    NotAuth,
    Challenge { authorization_url: String },
}

/// SSH and explicitly browserless environments should prefer the provider's
/// official headless route. A WSL desktop with `xdg-open` is not classified as
/// headless merely because DISPLAY is absent.
pub fn headless_environment() -> bool {
    std::env::var_os("SSH_CONNECTION").is_some()
        || std::env::var_os("SSH_TTY").is_some()
        || std::env::var_os("AH_HEADLESS").is_some()
}

pub fn run_official_login(
    provider: &str,
    home: &Path,
    auth_store_override: Option<&Path>,
    headless: bool,
) -> Result<(), AuthFlowError> {
    let adapter = crate::provider::adapter(provider)
        .ok_or_else(|| AuthFlowError::UnknownProvider(provider.to_string()))?;
    let spec = adapter.auth_spec();
    match spec.login {
        ProviderLoginDriverSpec::None => {
            return Err(AuthFlowError::NoLoginFlow(provider.to_string()));
        }
        ProviderLoginDriverSpec::Command {
            challenge,
            challenge_markers,
            code_prompt_markers,
            authorization_url_required_query_keys,
            ..
        } => match challenge {
            AuthChallengeKind::AuthorizationCodePaste => run_authorization_code_command_login(
                provider,
                spec,
                home,
                auth_store_override,
                headless,
                challenge_markers,
                code_prompt_markers,
                authorization_url_required_query_keys,
            ),
            AuthChallengeKind::DeviceCode { code_pattern } => run_device_code_command_login(
                provider,
                spec,
                home,
                auth_store_override,
                headless,
                challenge_markers,
                code_prompt_markers,
                authorization_url_required_query_keys,
                code_pattern,
            ),
        },
        ProviderLoginDriverSpec::StartupTui {
            select_prompt_markers,
            challenge_markers,
            code_prompt_markers,
            authorization_url_required_query_keys,
            ..
        } => run_startup_tui_login(
            provider,
            spec,
            home,
            auth_store_override,
            headless,
            select_prompt_markers,
            challenge_markers,
            code_prompt_markers,
            authorization_url_required_query_keys,
        ),
    }?;

    if authentication_is_healthy_with_spec(
        provider,
        spec,
        home,
        auth_store_override,
        std::env::consts::OS,
    ) {
        eprintln!("Login successful.");
        Ok(())
    } else {
        Err(AuthFlowError::CompletionNotObserved {
            provider: provider.to_string(),
        })
    }
}

/// Advance only the automatically safe entry action of an embedded startup
/// login. The authorization code remains human-provided through the official
/// provider pane; AH emits the URL and waits for the provider to prove ready.
pub async fn advance_startup_auth(
    provider: &str,
    server: Arc<TmuxServer>,
    pane: TmuxPaneId,
    observed_capture: &str,
) -> Result<StartupAuthDisposition, AuthFlowError> {
    let Some(adapter) = crate::provider::adapter(provider) else {
        return Ok(StartupAuthDisposition::NotAuth);
    };
    let ProviderLoginDriverSpec::StartupTui {
        select_prompt_markers,
        challenge_markers,
        code_prompt_markers,
        authorization_url_required_query_keys,
        ..
    } = adapter.auth_spec().login
    else {
        return Ok(StartupAuthDisposition::NotAuth);
    };

    if contains_all(observed_capture, challenge_markers)
        && contains_all(observed_capture, code_prompt_markers)
    {
        let joined = tokio::task::spawn_blocking({
            let server = server.clone();
            let pane = pane.clone();
            move || server.capture_pane_joined_sync(&pane)
        })
        .await
        .map_err(|err| AuthFlowError::Tmux {
            provider: provider.to_string(),
            details: format!("auth challenge capture worker failed: {err}"),
        })?
        .map_err(|err| tmux_error(provider, err))?;
        return challenge_disposition(provider, &joined, authorization_url_required_query_keys);
    }
    if !contains_all(observed_capture, select_prompt_markers) {
        return Ok(StartupAuthDisposition::NotAuth);
    }

    let provider_owned = provider.to_string();
    let before = observed_capture.to_string();
    let confirmed = tokio::task::spawn_blocking(move || {
        select_startup_login_option(
            before,
            select_prompt_markers,
            challenge_markers,
            code_prompt_markers,
            LOGIN_ACTION_TIMEOUT,
            LOGIN_POLL,
            || server.capture_pane_joined_sync(&pane),
            || server.send_keys_keysym_sync(&pane, "Enter"),
        )
    })
    .await
    .map_err(|err| AuthFlowError::Tmux {
        provider: provider_owned.clone(),
        details: format!("guarded login selection worker failed: {err}"),
    })?
    .map_err(|err| AuthFlowError::GuardedAction {
        provider: provider_owned.clone(),
        details: err.to_string(),
    })?;
    challenge_disposition(
        &provider_owned,
        &confirmed,
        authorization_url_required_query_keys,
    )
}

fn spawn_private_login(
    provider: &str,
    spec: &ProviderAuthSpec,
    auth_store_override: Option<&Path>,
    use_headless_argv: bool,
) -> Result<PrivateLoginSession, AuthFlowError> {
    let cwd = std::env::current_dir().map_err(|err| AuthFlowError::CommandLaunch {
        command: provider.to_string(),
        details: err.to_string(),
    })?;
    let socket_name = format!("ah-auth-{}", Uuid::new_v4().simple());
    let session_name = "login";
    let server = TmuxServer::from_socket_name(socket_name);
    server
        .ensure_session_sync(session_name, &cwd)
        .map_err(|err| tmux_error(provider, err))?;
    // Capture the exact socket while the server is known to be alive. A
    // successful provider command can terminate the last pane before Drop,
    // after which tmux can no longer report this path even though its socket
    // inode may still remain on disk.
    let socket_path = server.socket_path_sync().ok();
    let owned_argv =
        match private_login_argv(provider, spec, auth_store_override, use_headless_argv) {
            Ok(argv) => argv,
            Err(err) => {
                cleanup_private_server(&server, session_name, socket_path.as_deref());
                return Err(err);
            }
        };
    let argv = owned_argv.iter().map(String::as_str).collect::<Vec<_>>();
    let pane = match server.spawn_window_sync(session_name, "oauth", &cwd, &argv) {
        Ok(pane) => pane,
        Err(err) => {
            let err = tmux_error(provider, err);
            cleanup_private_server(&server, session_name, socket_path.as_deref());
            return Err(err);
        }
    };
    let login = PrivateLoginSession {
        server,
        pane,
        session_name,
        socket_path,
    };
    login
        .server
        .resize_window_sync(&login.pane, AUTH_PANE_WIDTH, AUTH_PANE_HEIGHT)
        .map_err(|err| tmux_error(provider, err))?;
    Ok(login)
}

#[allow(clippy::too_many_arguments)]
fn run_device_code_command_login(
    provider: &str,
    spec: &ProviderAuthSpec,
    home: &Path,
    auth_store_override: Option<&Path>,
    headless: bool,
    challenge_markers: &[&str],
    code_prompt_markers: &[&str],
    authorization_url_required_query_keys: &[&str],
    code_pattern: &str,
) -> Result<(), AuthFlowError> {
    // Device authorization is the provider's official manual browser-handoff
    // route. Use that declared argv locally and over SSH; `headless` controls
    // only whether AH can open the operator's browser itself.
    let login = spawn_private_login(provider, spec, auth_store_override, true)?;
    let mut required_markers = challenge_markers.to_vec();
    required_markers.extend_from_slice(code_prompt_markers);
    let challenge = wait_for_markers(
        &login.server,
        &login.pane,
        &required_markers,
        LOGIN_SCREEN_TIMEOUT,
    )
    .map_err(|details| AuthFlowError::Tmux {
        provider: provider.to_string(),
        details,
    })?
    .ok_or_else(|| AuthFlowError::UiTimeout {
        provider: provider.to_string(),
        purpose: "the device-code challenge",
    })?;
    let url =
        validated_authorization_url(provider, &challenge, authorization_url_required_query_keys)?;
    let device_code = validated_device_code(provider, &challenge, code_pattern)?;
    if let Err(err) = open_browser_after_validation(headless, &url, &SystemBrowserOpenerAdapter) {
        eprintln!("Could not open the browser automatically: {err}");
    }
    auth_ui::prepare_device_code(provider, &url, &device_code)
        .map_err(|err| AuthFlowError::AuthorizationInteraction(err.to_string()))?;
    wait_for_authentication_completion(
        provider,
        spec,
        home,
        auth_store_override,
        &login.server,
        &login.pane,
        DEVICE_CODE_COMPLETION_TIMEOUT,
    )
}

fn run_authorization_code_command_login(
    provider: &str,
    spec: &ProviderAuthSpec,
    home: &Path,
    auth_store_override: Option<&Path>,
    headless: bool,
    challenge_markers: &[&str],
    code_prompt_markers: &[&str],
    authorization_url_required_query_keys: &[&str],
) -> Result<(), AuthFlowError> {
    let login = spawn_private_login(provider, spec, auth_store_override, headless)?;
    let server = &login.server;
    let pane = &login.pane;

    let mut required_markers = challenge_markers.to_vec();
    required_markers.extend_from_slice(code_prompt_markers);
    let challenge = wait_for_markers(server, pane, &required_markers, LOGIN_SCREEN_TIMEOUT)
        .map_err(|details| AuthFlowError::Tmux {
            provider: provider.to_string(),
            details,
        })?
        .ok_or_else(|| AuthFlowError::UiTimeout {
            provider: provider.to_string(),
            purpose: "the authorization-code challenge",
        })?;

    complete_authorization_code_challenge(
        provider,
        spec,
        home,
        auth_store_override,
        headless,
        server,
        pane,
        challenge,
        code_prompt_markers,
        authorization_url_required_query_keys,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_startup_tui_login(
    provider: &str,
    spec: &ProviderAuthSpec,
    home: &Path,
    auth_store_override: Option<&Path>,
    headless: bool,
    select_prompt_markers: &[&str],
    challenge_markers: &[&str],
    code_prompt_markers: &[&str],
    authorization_url_required_query_keys: &[&str],
) -> Result<(), AuthFlowError> {
    let login = spawn_private_login(provider, spec, auth_store_override, headless)?;
    let server = &login.server;
    let pane = &login.pane;

    let selector = wait_for_markers(server, pane, select_prompt_markers, LOGIN_SCREEN_TIMEOUT)
        .map_err(|details| AuthFlowError::Tmux {
            provider: provider.to_string(),
            details,
        })?
        .ok_or_else(|| AuthFlowError::UiTimeout {
            provider: provider.to_string(),
            purpose: "the login-method selector",
        })?;

    let challenge = select_startup_login_option(
        selector,
        select_prompt_markers,
        challenge_markers,
        code_prompt_markers,
        LOGIN_ACTION_TIMEOUT,
        LOGIN_POLL,
        || server.capture_pane_joined_sync(pane),
        || server.send_keys_keysym_sync(pane, "Enter"),
    )
    .map_err(|err| AuthFlowError::GuardedAction {
        provider: provider.to_string(),
        details: err.to_string(),
    })?;

    complete_authorization_code_challenge(
        provider,
        spec,
        home,
        auth_store_override,
        headless,
        server,
        pane,
        challenge,
        code_prompt_markers,
        authorization_url_required_query_keys,
    )
}

#[allow(clippy::too_many_arguments)]
fn complete_authorization_code_challenge(
    provider: &str,
    spec: &ProviderAuthSpec,
    home: &Path,
    auth_store_override: Option<&Path>,
    headless: bool,
    server: &TmuxServer,
    pane: &TmuxPaneId,
    challenge: String,
    _code_prompt_markers: &[&str],
    authorization_url_required_query_keys: &[&str],
) -> Result<(), AuthFlowError> {
    let url =
        validated_authorization_url(provider, &challenge, authorization_url_required_query_keys)?;
    if let Err(err) = open_browser_after_validation(headless, &url, &SystemBrowserOpenerAdapter) {
        eprintln!("Could not open the browser automatically: {err}");
    }
    let code = auth_ui::request_authorization_code(provider, &url)
        .map_err(|err| AuthFlowError::AuthorizationInteraction(err.to_string()))?;

    run_guarded_action_sync_from_before(
        "paste_and_submit_authorization_code",
        (challenge, false),
        LOGIN_COMPLETION_TIMEOUT,
        LOGIN_POLL,
        || {
            let healthy = authentication_is_healthy_with_spec(
                provider,
                spec,
                home,
                auth_store_override,
                std::env::consts::OS,
            );
            // A successful dedicated login command may exit immediately after
            // persisting the credential, tearing down its private tmux server
            // before the next observation. Store/status health is authoritative
            // in that terminal condition; a missing pane is represented as an
            // empty capture instead of turning success into a tmux error.
            let capture = server.capture_pane_joined_sync(pane).unwrap_or_default();
            let process_dead = server
                .get_pane_runtime_sync(pane)
                .map_err(|err| AuthorizationChallengeStepError::Terminal(err.to_string()))?
                .dead;
            authorization_submission_observation(
                capture,
                healthy,
                process_dead,
                spec.login_failure_markers(),
            )
        },
        || {
            // Claude intentionally hides authorization-code input, so there is
            // no truthful intermediate pane observation that can prove paste
            // content. Treat paste + Enter as one provider-input transaction
            // and advance only after the provider store/status proves success.
            server
                .send_keys_literal_sync(pane, &code)
                .map_err(|err| AuthorizationChallengeStepError::Terminal(err.to_string()))?;
            server
                .send_keys_keysym_sync(pane, "Enter")
                .map_err(|err| AuthorizationChallengeStepError::Terminal(err.to_string()))?;
            eprintln!("Code submitted. Waiting for sign-in confirmation...");
            Ok(())
        },
        |_before, after| authorization_submission_assessment(&after.value.0, after.value.1),
    )
    .map_err(|err| match err.source.as_ref() {
        Some(AuthorizationChallengeStepError::ProviderRejected { marker }) => {
            AuthFlowError::AuthorizationRejected {
                provider: provider.to_string(),
                marker: *marker,
            }
        }
        Some(AuthorizationChallengeStepError::ProviderExited) => AuthFlowError::ProviderExited {
            provider: provider.to_string(),
        },
        _ => AuthFlowError::AuthorizationSubmission {
            provider: provider.to_string(),
            details: err.to_string(),
        },
    })?;

    Ok(())
}

fn authorization_submission_observation(
    capture: String,
    authentication_is_healthy: bool,
    provider_process_dead: bool,
    failure_markers: &'static [&'static str],
) -> Result<(String, bool), AuthorizationChallengeStepError> {
    if !authentication_is_healthy
        && let Some(marker) = failure_markers
            .iter()
            .find(|marker| capture.contains(**marker))
    {
        return Err(AuthorizationChallengeStepError::ProviderRejected { marker: *marker });
    }
    if !authentication_is_healthy && provider_process_dead {
        return Err(AuthorizationChallengeStepError::ProviderExited);
    }
    Ok((capture, authentication_is_healthy))
}

fn authorization_submission_assessment(
    _capture_with_scrollback: &str,
    authentication_is_healthy: bool,
) -> ActionAssessment {
    if authentication_is_healthy {
        // tmux capture-pane includes scrollback, so the pre-submit code prompt
        // can remain in the capture even after the provider prints success and
        // exits. Only the post-submit provider store/status probe is a valid
        // completion condition here; historical prompt text cannot negate it.
        ActionAssessment::Confirmed
    } else {
        ActionAssessment::Mismatch {
            reason: "provider has not confirmed a persisted authenticated session".to_string(),
        }
    }
}

fn open_browser_after_validation(
    headless: bool,
    url: &str,
    opener: &dyn BrowserOpenerPort,
) -> Result<(), BrowserOpenError> {
    if headless {
        Ok(())
    } else {
        opener.open_url(url)
    }
}

fn private_login_argv(
    provider: &str,
    spec: &ProviderAuthSpec,
    auth_store_override: Option<&Path>,
    headless: bool,
) -> Result<Vec<String>, AuthFlowError> {
    let argv = spec
        .login_argv(headless)
        .ok_or_else(|| AuthFlowError::NoLoginFlow(provider.to_string()))?;
    let mut owned = vec![
        "env".to_string(),
        "SSH_CONNECTION=127.0.0.1:50000:127.0.0.1:22".to_string(),
        "BROWSER=/bin/false".to_string(),
    ];
    if let (Some(variable), Some(path)) = (spec.config_dir_env, auth_store_override) {
        owned.push(format!("{variable}={}", path.display()));
    }
    owned.extend(argv.iter().map(|argument| (*argument).to_string()));
    Ok(owned)
}

fn apply_provider_store_override(
    command: &mut Command,
    spec: &ProviderAuthSpec,
    auth_store_override: Option<&Path>,
) {
    if let (Some(variable), Some(path)) = (spec.config_dir_env, auth_store_override) {
        command.env(variable, path);
    }
}

/// Resolve login health from the provider-owned store contract and, when the
/// store is intentionally opaque (for example a system credential service),
/// the provider-owned status command. An opaque store is never assumed valid.
pub(crate) fn authentication_is_healthy(
    provider: &str,
    home: &Path,
    auth_store_override: Option<&Path>,
    os: &str,
) -> bool {
    let Some(adapter) = crate::provider::adapter(provider) else {
        return false;
    };
    authentication_is_healthy_with_spec(
        provider,
        adapter.auth_spec(),
        home,
        auth_store_override,
        os,
    )
}

fn authentication_is_healthy_with_spec(
    provider: &str,
    spec: &ProviderAuthSpec,
    home: &Path,
    auth_store_override: Option<&Path>,
    os: &str,
) -> bool {
    match check_auth_store_for(provider, home, auth_store_override, os) {
        AuthStoreStatus::Healthy { .. } => true,
        AuthStoreStatus::NotCheckable { .. } => spec
            .status_argv
            .is_some_and(|argv| status_command_succeeds(argv, spec, auth_store_override)),
        _ => false,
    }
}

fn status_command_succeeds(
    argv: &[&str],
    spec: &ProviderAuthSpec,
    auth_store_override: Option<&Path>,
) -> bool {
    let mut command = Command::new(argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    apply_provider_store_override(&mut command, spec, auth_store_override);
    command.status().is_ok_and(|status| status.success())
}

#[allow(clippy::too_many_arguments)]
fn wait_for_authentication_completion(
    provider: &str,
    spec: &ProviderAuthSpec,
    home: &Path,
    auth_store_override: Option<&Path>,
    server: &TmuxServer,
    pane: &TmuxPaneId,
    timeout: Duration,
) -> Result<(), AuthFlowError> {
    let deadline = Instant::now() + timeout;
    loop {
        let healthy = authentication_is_healthy_with_spec(
            provider,
            spec,
            home,
            auth_store_override,
            std::env::consts::OS,
        );
        let capture = server
            .capture_pane_joined_sync(pane)
            .map_err(|err| tmux_error(provider, err))?;
        if healthy {
            return Ok(());
        }
        if let Some(marker) = spec
            .login_failure_markers()
            .iter()
            .find(|marker| capture.contains(**marker))
        {
            return Err(AuthFlowError::AuthorizationRejected {
                provider: provider.to_string(),
                marker: *marker,
            });
        }
        let runtime = server
            .get_pane_runtime_sync(pane)
            .map_err(|err| tmux_error(provider, err))?;
        if runtime.dead {
            return Err(AuthFlowError::ProviderExited {
                provider: provider.to_string(),
            });
        }
        if Instant::now() >= deadline {
            return Err(AuthFlowError::CompletionTimeout {
                provider: provider.to_string(),
            });
        }
        std::thread::sleep(LOGIN_POLL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

fn wait_for_markers(
    server: &TmuxServer,
    pane: &TmuxPaneId,
    markers: &[&str],
    timeout: Duration,
) -> Result<Option<String>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let capture = server
            .capture_pane_joined_sync(pane)
            .map_err(|err| err.to_string())?;
        if contains_all(&capture, markers) {
            return Ok(Some(capture));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(LOGIN_POLL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

#[allow(clippy::too_many_arguments)]
fn select_startup_login_option<E, Capture, Dispatch>(
    before: String,
    select_prompt_markers: &[&str],
    challenge_markers: &[&str],
    code_prompt_markers: &[&str],
    timeout: Duration,
    poll_interval: Duration,
    capture: Capture,
    dispatch: Dispatch,
) -> Result<String, GuardedActionError<E>>
where
    Capture: FnMut() -> Result<String, E>,
    Dispatch: FnOnce() -> Result<(), E>,
{
    run_guarded_action_sync_from_before(
        "select_login_option",
        before,
        timeout,
        poll_interval,
        capture,
        dispatch,
        |before, after| {
            if !contains_all(&before.value, select_prompt_markers) {
                return ActionAssessment::NotCausal;
            }
            if contains_all(&after.value, challenge_markers)
                && contains_all(&after.value, code_prompt_markers)
            {
                ActionAssessment::Confirmed
            } else {
                ActionAssessment::Mismatch {
                    reason: "login selection did not reach the authorization-code challenge"
                        .to_string(),
                }
            }
        },
    )
    .map(|outcome| outcome.confirmed.value)
}

fn contains_all(capture: &str, markers: &[&str]) -> bool {
    markers.iter().all(|marker| capture.contains(marker))
}

fn authorization_url(capture: &str) -> Option<String> {
    let start = capture.find("https://")?;
    let mut lines = capture[start..].lines();
    let first = lines.next()?.trim();
    let url_fragment = Regex::new(r"^[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+$")
        .expect("static RFC 3986 URL-fragment regex");
    if !url_fragment.is_match(first) {
        return None;
    }

    let mut url = first.to_string();
    for line in lines {
        let continuation = line.trim();
        if continuation.is_empty() || !url_fragment.is_match(continuation) {
            break;
        }
        url.push_str(continuation);
    }
    Some(url)
}

fn validated_authorization_url(
    provider: &str,
    capture: &str,
    required_query_keys: &[&str],
) -> Result<String, AuthFlowError> {
    let url = authorization_url(capture).ok_or_else(|| AuthFlowError::MissingAuthorizationUrl {
        provider: provider.to_string(),
    })?;
    let query = url.split_once('?').map(|(_, query)| query).unwrap_or("");
    let present = query
        .split('&')
        .filter_map(|pair| pair.split_once('=').map(|(key, _)| key))
        .collect::<std::collections::HashSet<_>>();
    if required_query_keys
        .iter()
        .all(|required| present.contains(required))
    {
        Ok(url)
    } else {
        Err(AuthFlowError::MissingAuthorizationUrl {
            provider: provider.to_string(),
        })
    }
}

fn validated_device_code(
    provider: &str,
    capture: &str,
    code_pattern: &str,
) -> Result<String, AuthFlowError> {
    let pattern = Regex::new(code_pattern).map_err(|_| AuthFlowError::MissingDeviceCode {
        provider: provider.to_string(),
    })?;
    pattern
        .find(capture)
        .map(|matched| matched.as_str().to_string())
        .ok_or_else(|| AuthFlowError::MissingDeviceCode {
            provider: provider.to_string(),
        })
}

fn challenge_disposition(
    provider: &str,
    capture: &str,
    required_query_keys: &[&str],
) -> Result<StartupAuthDisposition, AuthFlowError> {
    validated_authorization_url(provider, capture, required_query_keys)
        .map(|authorization_url| StartupAuthDisposition::Challenge { authorization_url })
}

fn tmux_error(provider: &str, err: impl std::fmt::Display) -> AuthFlowError {
    AuthFlowError::Tmux {
        provider: provider.to_string(),
        details: err.to_string(),
    }
}

struct PrivateLoginSession {
    server: TmuxServer,
    pane: TmuxPaneId,
    session_name: &'static str,
    socket_path: Option<std::path::PathBuf>,
}

fn cleanup_private_server(
    server: &TmuxServer,
    session_name: &str,
    known_socket_path: Option<&Path>,
) {
    // This server has a UUID-scoped socket and exists only for one login
    // transaction. tmux 3.4 on WSL can leave the socket inode behind even
    // after kill-server, so remove that exact tmux-reported socket as well.
    let socket_path = known_socket_path
        .map(Path::to_path_buf)
        .or_else(|| server.socket_path_sync().ok());
    if server.kill_server_sync().is_err() {
        let _ = server.kill_session_sync(session_name);
    }
    if let Some(socket_path) = socket_path
        && socket_path.file_name().and_then(|name| name.to_str()) == Some(server.socket_name())
    {
        let _ = std::fs::remove_file(socket_path);
    }
}

impl Drop for PrivateLoginSession {
    fn drop(&mut self) {
        cleanup_private_server(&self.server, self.session_name, self.socket_path.as_deref());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationChallengeStepError, authorization_submission_assessment,
        authorization_submission_observation, authorization_url, contains_all,
        open_browser_after_validation, select_startup_login_option, validated_authorization_url,
        validated_device_code,
    };
    use crate::guarded_action::ActionAssessment;
    use crate::platform::browser::{BrowserOpenError, BrowserOpenerPort};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingBrowserOpener {
        urls: Mutex<Vec<String>>,
    }

    impl BrowserOpenerPort for RecordingBrowserOpener {
        fn open_url(&self, url: &str) -> Result<(), BrowserOpenError> {
            self.urls.lock().unwrap().push(url.to_string());
            Ok(())
        }
    }

    #[test]
    fn validated_url_opens_once_locally_and_never_in_explicit_headless_mode() {
        let opener = RecordingBrowserOpener::default();
        let url = "https://claude.com/cai/oauth/authorize?state=s1";

        open_browser_after_validation(false, url, &opener).unwrap();
        open_browser_after_validation(true, url, &opener).unwrap();

        assert_eq!(opener.urls.lock().unwrap().as_slice(), &[url]);
    }

    #[test]
    fn healthy_provider_status_confirms_submission_despite_prompt_in_scrollback() {
        assert_eq!(
            authorization_submission_assessment(
                "Paste code here if prompted > Login successful.",
                true,
            ),
            ActionAssessment::Confirmed
        );
        assert!(matches!(
            authorization_submission_assessment("Paste code here if prompted >", false),
            ActionAssessment::Mismatch { .. }
        ));
    }

    #[test]
    fn provider_native_failure_marker_rejects_submission_without_waiting_for_timeout() {
        let result = authorization_submission_observation(
            "Paste code here if prompted > Login failed: Request failed with status code 400"
                .to_string(),
            false,
            false,
            &["Login failed:"],
        );

        assert!(matches!(
            result,
            Err(AuthorizationChallengeStepError::ProviderRejected {
                marker: "Login failed:"
            })
        ));
    }

    #[test]
    fn provider_process_exit_rejects_an_unhealthy_submission_immediately() {
        let result = authorization_submission_observation(
            "authorization code...".to_string(),
            false,
            true,
            &[],
        );

        assert!(matches!(
            result,
            Err(AuthorizationChallengeStepError::ProviderExited)
        ));
    }

    #[test]
    fn codex_device_code_is_extracted_only_when_complete() {
        let capture = "Enter this one-time code (expires in 15 minutes)\nKF2E-5707P\nDevice codes are a common phishing target.";
        let pattern = r"\b[A-Z0-9]{4}-[A-Z0-9]{5}\b";

        assert_eq!(
            validated_device_code("codex", capture, pattern).unwrap(),
            "KF2E-5707P"
        );
        assert!(matches!(
            validated_device_code("codex", "KF2E-570", pattern),
            Err(super::AuthFlowError::MissingDeviceCode { .. })
        ));
    }

    #[test]
    fn antigravity_login_markers_require_the_complete_screen() {
        let markers = [
            "You are currently not signed in",
            "Select login method:",
            "1. Google OAuth",
        ];
        assert!(contains_all(
            "Welcome. You are currently not signed in\nSelect login method:\n> 1. Google OAuth",
            &markers
        ));
        assert!(!contains_all(
            "Select login method:\n1. Google OAuth",
            &markers
        ));
    }

    #[test]
    fn joined_capture_yields_the_provider_authorization_url() {
        let capture = "Open the URL below in your browser:\nhttps://accounts.example.test/o/oauth2/auth?state=s1&code_challenge=c1\nAfter authenticating";
        assert_eq!(
            authorization_url(capture),
            Some(
                "https://accounts.example.test/o/oauth2/auth?state=s1&code_challenge=c1"
                    .to_string()
            )
        );
    }

    #[test]
    fn hard_wrapped_authorization_url_is_reassembled_and_validated() {
        let capture = "Open the URL below in your browser:\nhttps://accounts.example.test/auth?client_id=c1&code_challenge=pk\n ce&redirect_uri=http%3A%2F%2Flocalhost&response_type=code&scope=s1\n &state=state1\nAfter authenticating, paste the code";

        let url = validated_authorization_url(
            "antigravity",
            capture,
            &[
                "client_id",
                "code_challenge",
                "redirect_uri",
                "response_type",
                "scope",
                "state",
            ],
        )
        .unwrap();

        assert!(url.contains("code_challenge=pkce"));
        assert!(url.contains("response_type=code"));
        assert!(!url.contains('\n'));
    }

    #[test]
    fn truncated_authorization_url_is_rejected_before_browser_handoff() {
        let capture = "https://accounts.example.test/auth?client_id=c1&co\nAuthorization code:";

        let err = validated_authorization_url(
            "antigravity",
            capture,
            &["client_id", "code_challenge", "response_type", "state"],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            super::AuthFlowError::MissingAuthorizationUrl { .. }
        ));
    }

    #[test]
    fn claude_hard_wrapped_authorization_url_keeps_its_prefix_and_complete_query() {
        let capture = "Opening browser to sign in…\nIf the browser didn't open, visit:\nhttps://claude.com/cai/oauth/authorize?client_id=cli_123&code=true&code_challenge=VkuS\n TI3hk1J-P6kn1aEvTe0dMYswiianKuFkDEY1jdE&code_challenge_method=S256&redirect_uri=https\n %3A%2F%2Fconsole.anthropic.com%2Foauth%2Fcode%2Fcallback&response_type=code&scope=org%\n 3Acreate_api_key+user%3Aprofile+user%3Ainference+user%3Asessions%3Aclaude_code+user%3A\n mcp_servers+user%3Afile_upload&state=aEq9aq0GUpfubcoZm0conMcPtLXQc7XorVQcdd4XauY\nPaste code here if prompted >";

        let url = validated_authorization_url(
            "claude",
            capture,
            &[
                "client_id",
                "code_challenge",
                "code_challenge_method",
                "redirect_uri",
                "response_type",
                "scope",
                "state",
            ],
        )
        .unwrap();

        assert!(url.starts_with("https://claude.com/cai/oauth/authorize?client_id="));
        assert!(url.ends_with("state=aEq9aq0GUpfubcoZm0conMcPtLXQc7XorVQcdd4XauY"));
        assert!(url.contains("code_challenge=VkuSTI3hk1J-P6kn1aEvTe0dMYswiianKuFkDEY1jdE"));
        assert!(!url.contains('\n'));
    }

    #[test]
    fn claude_visible_suffix_without_url_prefix_is_rejected() {
        let visible_suffix = "e_api_key+user%3Aprofile+user%3Ainference+user%3Asessions%3Aclaude_code\nPaste code here if prompted >";

        let err = validated_authorization_url(
            "claude",
            visible_suffix,
            &[
                "client_id",
                "code_challenge",
                "code_challenge_method",
                "redirect_uri",
                "response_type",
                "scope",
                "state",
            ],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            super::AuthFlowError::MissingAuthorizationUrl { .. }
        ));
    }

    #[test]
    fn startup_login_selection_dispatches_enter_once_then_only_observes() {
        let selector = [
            "You are currently not signed in",
            "Select login method:",
            "1. Google OAuth",
        ];
        let challenge = ["Open the URL below in your browser:"];
        let code_prompt = ["authorization code..."];
        let captures = Mutex::new(VecDeque::from([
            "loading authorization challenge".to_string(),
            "Open the URL below in your browser:\nhttps://accounts.example.test/auth\nauthorization code..."
                .to_string(),
        ]));
        let dispatched = AtomicUsize::new(0);

        let confirmed = select_startup_login_option(
            "You are currently not signed in\nSelect login method:\n> 1. Google OAuth".to_string(),
            &selector,
            &challenge,
            &code_prompt,
            Duration::from_millis(20),
            Duration::ZERO,
            || Ok::<_, &'static str>(captures.lock().unwrap().pop_front().unwrap()),
            || {
                dispatched.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>(())
            },
        )
        .unwrap();

        assert_eq!(dispatched.load(Ordering::SeqCst), 1);
        assert!(confirmed.contains("https://accounts.example.test/auth"));
        assert!(captures.lock().unwrap().is_empty());
    }
}
