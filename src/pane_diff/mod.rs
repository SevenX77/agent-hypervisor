//! PaneDiffWatcher pure logic: sanitize PTY text + detect meaningful change.

use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime};

use crate::marker::{MarkerMatcher, MatchResult};
use crate::provider::manifest::{CompletionSignalKind, get_manifest};

const MIN_LENGTH_GROWTH: usize = 8;
pub const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_STUCK_THRESHOLD: Duration = Duration::from_secs(300);
const DEFAULT_UI_COMPLETION_STABLE_TICKS: usize = 2;

#[derive(Debug, Clone)]
pub struct AgentDiffState {
    pub last_meaningful_text: String,
    pub last_meaningful_at: Instant,
    pub last_content_hash: Option<u64>,
    pub last_log_mtime: Option<SystemTime>,
    pub thinking_start: Option<Instant>,
    pub last_signal_kinds: Vec<String>,
    pub ui_marker_match: Option<UiMarkerMatchState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiMarkerMatchState {
    pub content_hash: u64,
    pub consecutive_ticks: usize,
    pub first_seen_at: Instant,
}

impl AgentDiffState {
    pub fn new(initial_text: String) -> Self {
        let hash = compute_content_hash(&initial_text);
        let thinking_start = detect_thinking_spinner(&initial_text).then(Instant::now);
        Self {
            last_meaningful_text: initial_text,
            last_meaningful_at: Instant::now(),
            last_content_hash: Some(hash),
            last_log_mtime: None,
            thinking_start,
            last_signal_kinds: Vec::new(),
            ui_marker_match: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaneDiffObservation {
    pub agent_id: String,
    pub text: String,
    pub log_mtime: Option<SystemTime>,
    pub provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckSignal {
    pub agent_id: String,
    pub signal_kinds: Vec<String>,
    pub elapsed_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiCompletionRecapture {
    pub agent_id: String,
    pub pane_text: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PaneDiffTickResult {
    pub stuck_agent_ids: Vec<String>,
    pub stuck_signals: Vec<StuckSignal>,
    pub ui_completed: Vec<UiCompletionRecapture>,
}

pub fn process_pane_diff_observations(
    state_map: &mut HashMap<String, AgentDiffState>,
    observations: Vec<PaneDiffObservation>,
    now: Instant,
    stuck_threshold: Duration,
) -> PaneDiffTickResult {
    process_pane_diff_observations_with_ui_completion_stable_ticks(
        state_map,
        observations,
        now,
        stuck_threshold,
        ui_completion_stable_ticks(),
    )
}

fn process_pane_diff_observations_with_ui_completion_stable_ticks(
    state_map: &mut HashMap<String, AgentDiffState>,
    observations: Vec<PaneDiffObservation>,
    now: Instant,
    stuck_threshold: Duration,
    ui_completion_stable_ticks: usize,
) -> PaneDiffTickResult {
    let mut active_agent_ids = HashSet::new();
    let mut stuck_agent_ids = Vec::new();
    let mut stuck_signals = Vec::new();
    let mut ui_completed = Vec::new();
    let ui_completion_stable_ticks = ui_completion_stable_ticks.max(1);

    for observation in observations {
        active_agent_ids.insert(observation.agent_id.clone());
        let clean_text = sanitize_for_diff(&observation.text);
        let content_hash = compute_content_hash(&clean_text);
        let thinking_now = detect_thinking_spinner(&observation.text);
        let entry = state_map
            .entry(observation.agent_id.clone())
            .or_insert_with(|| AgentDiffState {
                last_meaningful_text: clean_text.clone(),
                last_meaningful_at: now,
                last_content_hash: Some(content_hash),
                last_log_mtime: observation.log_mtime,
                thinking_start: thinking_now.then_some(now),
                last_signal_kinds: Vec::new(),
                ui_marker_match: None,
            });

        if let Some(provider) = observation.provider.as_deref() {
            let manifest = get_manifest(provider);
            if provider_uses_ui_completion_recapture(provider, manifest.completion_signal) {
                // UI-only providers have no log monitor, so their pull fallback must
                // use the full provider matcher. The matcher applies the same bottom
                // viewport window as the live FIFO reader and enforces anti-patterns
                // such as antigravity's "esc to cancel" busy status.
                let matcher = MarkerMatcher::from_manifest(&manifest);
                let mut parser = vt100::Parser::new(200, 200, 0);
                parser.process(observation.text.as_bytes());
                let match_result = matcher.scan(&parser);
                if match_result == MatchResult::Matched {
                    let consecutive_ticks = match &entry.ui_marker_match {
                        Some(previous) if previous.content_hash == content_hash => {
                            previous.consecutive_ticks + 1
                        }
                        _ => 1,
                    };
                    let first_seen_at = entry
                        .ui_marker_match
                        .as_ref()
                        .filter(|previous| previous.content_hash == content_hash)
                        .map(|previous| previous.first_seen_at)
                        .unwrap_or(now);
                    entry.ui_marker_match = Some(UiMarkerMatchState {
                        content_hash,
                        consecutive_ticks,
                        first_seen_at,
                    });
                    tracing::info!(
                        agent_id = %observation.agent_id,
                        provider,
                        scan = ?match_result,
                        consecutive_ticks,
                        "pane_diff scan"
                    );
                    if consecutive_ticks >= ui_completion_stable_ticks {
                        let is_log_only =
                            manifest.completion_signal == CompletionSignalKind::LogOnly;
                        let log_active =
                            crate::completion::registry::contains(&observation.agent_id);
                        if !(is_log_only && log_active) {
                            ui_completed.push(UiCompletionRecapture {
                                agent_id: observation.agent_id,
                                pane_text: observation.text,
                            });
                            continue;
                        }
                    }
                } else {
                    tracing::info!(
                        agent_id = %observation.agent_id,
                        provider,
                        scan = ?match_result,
                        consecutive_ticks = 0usize,
                        "pane_diff scan"
                    );
                    entry.ui_marker_match = None;
                }
            } else {
                entry.ui_marker_match = None;
            }
        } else {
            entry.ui_marker_match = None;
        }

        let hash_changed = entry.last_content_hash != Some(content_hash);
        let mtime_changed = observation.log_mtime.is_some()
            && entry.last_log_mtime.is_some()
            && observation.log_mtime != entry.last_log_mtime;
        if is_meaningful_diff(&entry.last_meaningful_text, &observation.text)
            || hash_changed
            || mtime_changed
        {
            entry.last_meaningful_text = clean_text;
            entry.last_meaningful_at = now;
            entry.last_content_hash = Some(content_hash);
            entry.last_log_mtime = observation.log_mtime;
            entry.thinking_start = thinking_now.then_some(now);
            entry.last_signal_kinds.clear();
            continue;
        }

        if thinking_now && entry.thinking_start.is_none() {
            entry.thinking_start = Some(now);
        } else if !thinking_now {
            entry.thinking_start = None;
        }

        let elapsed = now.duration_since(entry.last_meaningful_at);
        let thinking_elapsed = entry
            .thinking_start
            .map(|started| now.duration_since(started))
            .unwrap_or(elapsed);
        if elapsed >= stuck_threshold && (!thinking_now || thinking_elapsed >= stuck_threshold) {
            let mut signal_kinds = vec!["hash".to_string()];
            if observation.log_mtime.is_some() {
                signal_kinds.push("mtime".to_string());
            }
            if thinking_now {
                signal_kinds.push("thinking".to_string());
            }
            entry.last_signal_kinds = signal_kinds.clone();
            stuck_signals.push(StuckSignal {
                agent_id: observation.agent_id.clone(),
                signal_kinds,
                elapsed_secs: elapsed.as_secs() as i64,
            });
            stuck_agent_ids.push(observation.agent_id);
        }
    }

    for agent_id in &stuck_agent_ids {
        state_map.remove(agent_id);
    }
    for recapture in &ui_completed {
        state_map.remove(&recapture.agent_id);
    }
    state_map.retain(|agent_id, _| active_agent_ids.contains(agent_id));

    PaneDiffTickResult {
        stuck_agent_ids,
        stuck_signals,
        ui_completed,
    }
}

pub async fn pane_diff_watcher_loop(
    ctx: crate::rpc::Ctx,
    interval: Duration,
    stuck_threshold: Duration,
) {
    let mut state_map = HashMap::new();
    loop {
        tokio::time::sleep(interval).await;
        if let Err(err) = pane_diff_watcher_tick(&ctx, &mut state_map, stuck_threshold).await {
            tracing::warn!(error = %err, "pane diff watcher tick failed");
        }
    }
}

async fn pane_diff_watcher_tick(
    ctx: &crate::rpc::Ctx,
    state_map: &mut HashMap<String, AgentDiffState>,
    stuck_threshold: Duration,
) -> Result<(), crate::error::AhError> {
    let busy_agents = query_ui_completion_recapture_agents(ctx.db.clone()).await?;
    tracing::info!(busy_agents = busy_agents.len(), "pane_diff watcher tick");
    let mut observations = Vec::new();

    for agent in busy_agents {
        let Some(pane_id) = crate::agent_io::pane_id(&agent.id) else {
            continue;
        };
        match ctx.tmux_server.capture_pane(pane_id).await {
            Ok(text) => observations.push(PaneDiffObservation {
                agent_id: agent.id,
                text,
                log_mtime: None,
                provider: Some(agent.provider),
            }),
            Err(err) => {
                tracing::warn!(agent_id = %agent.id, error = %err, "pane diff capture_pane failed");
            }
        }
    }

    let result =
        process_pane_diff_observations(state_map, observations, Instant::now(), stuck_threshold);
    for recapture in result.ui_completed {
        let agent_id = recapture.agent_id;
        match escalate_pane_diff_ui_recapture(ctx.db.clone(), agent_id.clone(), recapture.pane_text)
            .await
        {
            Ok(_) => {
                tracing::info!(agent_id = %agent_id, "UI recapture alert emitted");
            }
            Err(err) => {
                tracing::warn!(agent_id = %agent_id, error = %err, "UI recapture alert failed");
            }
        }
    }
    for signal in result.stuck_signals {
        match escalate_pane_diff_stuck(ctx.db.clone(), &signal).await {
            Ok(_) => {
                tracing::warn!(agent_id = %signal.agent_id, "pane diff watcher stuck alert emitted");
            }
            Err(err) => {
                tracing::warn!(agent_id = %signal.agent_id, error = %err, "escalate_pane_diff_stuck failed");
            }
        }
    }

    Ok(())
}

/// P0-1 seam (signature stub — a1 fills the alert-only body).
///
/// Replaces the deleted pane-diff `mark_agent_stuck` mutation: a pane that has
/// stopped changing past the stuck threshold must emit a `PANE_DIFF_STUCK_ALERT`
/// audit event only and MUST NOT mutate agent/job state. Body is intentionally
/// `unimplemented!` so the a4 RED contract tests panic until a1 implements it.
pub async fn escalate_pane_diff_stuck(
    db: crate::db::Db,
    signal: &StuckSignal,
) -> Result<(), crate::error::AhError> {
    let agent_id = signal.agent_id.clone();
    let agent_state = match crate::db::agents::query_agent_state(db.clone(), agent_id.clone()).await
    {
        Ok(Some((state, _))) => state,
        _ => "BUSY".to_string(),
    };
    let job_id = query_dispatched_job_id(db.clone(), agent_id.clone())
        .await
        .unwrap_or(None);

    let payload_val = serde_json::json!({
        "from": agent_state,
        "to": agent_state,
        "reason": "PANE_DIFF_STUCK_ALERT",
        "job_id": job_id,
        "signal_kinds": signal.signal_kinds,
        "elapsed_secs": signal.elapsed_secs,
    });

    let _ = crate::db::events::insert_event(
        db.clone(),
        agent_id.clone(),
        None,
        "state_change".to_string(),
        payload_val.to_string(),
    )
    .await?;

    crate::orchestrator::pubsub::notify_event(crate::orchestrator::pubsub::EventFrame {
        event_id: 0,
        kind: "alert".to_string(),
        agent_id: agent_id.clone(),
        job_id: job_id.clone(),
        state: Some(agent_state),
        ts_unix_micro: now_unix_micro(),
        payload: Some(serde_json::json!({
            "job_id": job_id,
            "signal_kinds": signal.signal_kinds,
            "elapsed_secs": signal.elapsed_secs,
        })),
    });

    Ok(())
}

/// P0-1 seam (signature stub — a1 fills the alert-only body).
///
/// Replaces the deleted pane-diff UI-completion recapture mutation
/// (`mark_agent_idle_recaptured_with_pane`): a pane that visually looks idle must
/// emit a `UI_RECAPTURE_ALERT` audit event only and MUST NOT mark the agent
/// idle/completed. Body is intentionally `unimplemented!` so the a4 RED contract
/// tests panic until a1 implements it.
pub async fn escalate_pane_diff_ui_recapture(
    db: crate::db::Db,
    agent_id: String,
    pane_text: String,
) -> Result<(), crate::error::AhError> {
    let agent_state = match crate::db::agents::query_agent_state(db.clone(), agent_id.clone()).await
    {
        Ok(Some((state, _))) => state,
        _ => "BUSY".to_string(),
    };
    let job_id = query_dispatched_job_id(db.clone(), agent_id.clone())
        .await
        .unwrap_or(None);

    let payload_val = serde_json::json!({
        "from": agent_state,
        "to": agent_state,
        "reason": "UI_RECAPTURE_ALERT",
        "job_id": job_id,
        "pane_text": pane_text,
    });

    let _ = crate::db::events::insert_event(
        db.clone(),
        agent_id.clone(),
        None,
        "state_change".to_string(),
        payload_val.to_string(),
    )
    .await?;

    crate::orchestrator::pubsub::notify_event(crate::orchestrator::pubsub::EventFrame {
        event_id: 0,
        kind: "alert".to_string(),
        agent_id: agent_id.clone(),
        job_id: job_id.clone(),
        state: Some(agent_state),
        ts_unix_micro: now_unix_micro(),
        payload: Some(serde_json::json!({
            "job_id": job_id,
            "pane_text": pane_text,
        })),
    });

    Ok(())
}

async fn query_ui_completion_recapture_agents(
    db: crate::db::Db,
) -> Result<Vec<crate::db::schema::Agent>, crate::error::AhError> {
    let mut agents =
        crate::db::agents::query_agents_by_state(db.clone(), "BUSY".to_string()).await?;
    let stuck_agents = crate::db::agents::query_agents_by_state(db, "STUCK".to_string()).await?;
    agents.extend(stuck_agents.into_iter().filter(|agent| {
        provider_uses_ui_completion_recapture(
            &agent.provider,
            get_manifest(&agent.provider).completion_signal,
        )
    }));
    Ok(agents)
}

fn provider_uses_ui_completion_recapture(
    _provider: &str,
    completion_signal: CompletionSignalKind,
) -> bool {
    completion_signal == CompletionSignalKind::LogOnly
}

async fn query_dispatched_job_id(
    db: crate::db::Db,
    agent_id: String,
) -> Result<Option<String>, crate::error::AhError> {
    Ok(
        crate::db::jobs::query_dispatched_job_for_agent(db, agent_id)
            .await?
            .map(|job| job.id),
    )
}

fn now_unix_micro() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
}

pub fn compute_content_hash(pane_content: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    pane_content.hash(&mut hasher);
    hasher.finish()
}

pub fn query_log_mtime(agent_log_path: &Path) -> Option<SystemTime> {
    std::fs::metadata(agent_log_path)
        .and_then(|metadata| metadata.modified())
        .ok()
}

pub fn detect_thinking_spinner(content: &str) -> bool {
    thinking_spinner_re().is_match(content)
}

pub fn resolve_stuck_watch_config() -> (Duration, Duration) {
    (
        env_duration_secs("AH_STUCK_TICK_SECS", DEFAULT_WATCH_INTERVAL),
        env_duration_secs("AH_STUCK_THRESHOLD_SECS", DEFAULT_STUCK_THRESHOLD),
    )
}

fn ui_completion_stable_ticks() -> usize {
    match std::env::var("AH_UI_COMPLETION_STABLE_TICKS") {
        Ok(value) => match value.parse::<usize>() {
            Ok(ticks) if ticks > 0 => ticks,
            _ => {
                tracing::warn!(
                    env = "AH_UI_COMPLETION_STABLE_TICKS",
                    value,
                    "invalid UI completion stable tick env; using default"
                );
                DEFAULT_UI_COMPLETION_STABLE_TICKS
            }
        },
        Err(_) => DEFAULT_UI_COMPLETION_STABLE_TICKS,
    }
}

fn env_duration_secs(name: &str, default: Duration) -> Duration {
    match std::env::var(name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(secs) if secs > 0 => Duration::from_secs(secs),
            _ => {
                tracing::warn!(
                    env = name,
                    value,
                    "invalid stuck watcher env; using default"
                );
                default
            }
        },
        Err(_) => default,
    }
}

/// 去掉 spinner Unicode、时间戳、ANSI escape 等“假活”装饰，留下实质文本。
pub fn sanitize_for_diff(raw: &str) -> String {
    let without_ansi = crate::db::jobs::strip_ansi_escapes(raw);
    let mut lines = Vec::new();

    for line in without_ansi.lines() {
        let line = braille_spinner_re().replace_all(line, "");
        let line = ascii_spinner_prefix_re().replace(&line, "");
        let line = line.trim();
        if line.is_empty() || thinking_line_re().is_match(line) {
            continue;
        }

        let line = trailing_time_re().replace(line, "");
        let line = line.trim();
        if !line.is_empty() {
            lines.push(line.to_string());
        }
    }

    lines.join("\n")
}

/// 判断 new_text 相比 old_text 是否含“实质增长”，而不是单纯 spinner 闪烁。
pub fn is_meaningful_diff(old_text: &str, new_text: &str) -> bool {
    let clean_old = sanitize_for_diff(old_text);
    let clean_new = sanitize_for_diff(new_text);

    let old_len = clean_old.chars().count();
    let new_len = clean_new.chars().count();
    if new_len >= old_len + MIN_LENGTH_GROWTH {
        return true;
    }

    if !clean_new.starts_with(&clean_old) && !clean_old.starts_with(&clean_new) {
        return clean_new != clean_old;
    }

    false
}

fn braille_spinner_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[\u{2800}-\u{28FF}]+").expect("valid braille regex"))
}

fn ascii_spinner_prefix_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*[-\\|/]\s+").expect("valid ascii spinner regex"))
}

fn thinking_line_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)^thinking\.\.\.\s*(?:\([^)]*\))?$").expect("valid thinking regex")
    })
}

fn thinking_spinner_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(thinking|spinner|working|⠋|⠙|⠹|⠸|⠼|⠴|⠦|⠧|⠇|⠏)")
            .expect("valid thinking spinner regex")
    })
}

fn trailing_time_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?:\s+\(\d+(?:\.\d+)?[smh](?:\s+\d+(?:\.\d+)?[smh])?\)|\s+\d+:\d+:\d+|\s+\d+m\s+\d+s)$",
        )
        .expect("valid trailing time regex")
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AgentDiffState, PaneDiffObservation, StuckSignal, compute_content_hash,
        detect_thinking_spinner, escalate_pane_diff_stuck, escalate_pane_diff_ui_recapture,
        is_meaningful_diff, process_pane_diff_observations,
        process_pane_diff_observations_with_ui_completion_stable_ticks, query_log_mtime,
        query_ui_completion_recapture_agents, sanitize_for_diff,
    };
    use crate::db::agents::insert_agent_sync;
    use crate::db::events::insert_event_sync;
    use crate::db::jobs::{insert_job_sync, query_job_sync, update_dispatched_seq_id_sync};
    use crate::db::sessions::insert_session_sync;
    use std::collections::HashMap;
    use std::fs;
    use std::time::{Duration, Instant};

    #[test]
    fn test_sanitize_strips_ansi_escapes() {
        assert_eq!(sanitize_for_diff("\u{1b}[31mhello\u{1b}[0m"), "hello");
    }

    #[test]
    fn test_sanitize_strips_braille_spinner_chars() {
        assert_eq!(sanitize_for_diff("⠋ Thinking..."), "");
    }

    #[test]
    fn test_sanitize_strips_thinking_with_timestamp() {
        assert_eq!(sanitize_for_diff("Thinking... (3m 24s)\nactual"), "actual");
    }

    #[test]
    fn test_sanitize_strips_trailing_time_seconds() {
        assert_eq!(sanitize_for_diff("Some output (12.3s)"), "Some output");
    }

    #[test]
    fn test_sanitize_collapses_blank_lines() {
        assert_eq!(sanitize_for_diff("one\n\n\n  two  \n\n"), "one\ntwo");
    }

    #[test]
    fn test_is_meaningful_diff_detects_length_growth() {
        assert!(is_meaningful_diff("abc", "abc + new tool call: read_file"));
    }

    #[test]
    fn test_is_meaningful_diff_ignores_spinner_only_change() {
        assert!(!is_meaningful_diff(
            "Thinking... (1m)",
            "Thinking... (1m 5s)"
        ));
    }

    #[test]
    fn test_is_meaningful_diff_detects_total_text_replacement() {
        assert!(is_meaningful_diff("hello world", "goodbye now"));
    }

    #[test]
    fn test_agent_diff_state_initial() {
        let before = Instant::now();
        let state = AgentDiffState::new("initial".to_string());
        let after = Instant::now();

        assert_eq!(state.last_meaningful_text, "initial");
        assert!(state.last_meaningful_at >= before);
        assert!(state.last_meaningful_at <= after + Duration::from_millis(1));
    }

    #[test]
    fn test_pane_diff_watcher_marks_stuck_after_threshold() {
        let mut state_map = HashMap::new();
        let start = Instant::now();
        let observations = vec![PaneDiffObservation {
            agent_id: "a1".to_string(),
            text: "same output".to_string(),
            log_mtime: None,
            provider: None,
        }];

        let first = process_pane_diff_observations(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(300),
        );
        let second = process_pane_diff_observations(
            &mut state_map,
            observations,
            start + Duration::from_secs(301),
            Duration::from_secs(300),
        );

        assert!(first.stuck_agent_ids.is_empty());
        assert_eq!(second.stuck_agent_ids, ["a1"]);
        assert!(!state_map.contains_key("a1"));
    }

    #[test]
    fn test_pane_diff_watcher_resets_timer_on_meaningful_change() {
        let mut state_map = HashMap::new();
        let start = Instant::now();

        process_pane_diff_observations(
            &mut state_map,
            vec![PaneDiffObservation {
                agent_id: "a1".to_string(),
                text: "short".to_string(),
                log_mtime: None,
                provider: None,
            }],
            start,
            Duration::from_secs(300),
        );
        let result = process_pane_diff_observations(
            &mut state_map,
            vec![PaneDiffObservation {
                agent_id: "a1".to_string(),
                text: "short plus meaningful new content".to_string(),
                log_mtime: None,
                provider: None,
            }],
            start + Duration::from_secs(301),
            Duration::from_secs(300),
        );

        let state = state_map.get("a1").unwrap();
        assert!(result.stuck_agent_ids.is_empty());
        assert_eq!(
            state.last_meaningful_text,
            "short plus meaningful new content"
        );
        assert_eq!(state.last_meaningful_at, start + Duration::from_secs(301));
    }

    #[test]
    fn test_pane_diff_watcher_cleans_up_state_when_agent_no_longer_busy() {
        let mut state_map = HashMap::new();
        let start = Instant::now();

        process_pane_diff_observations(
            &mut state_map,
            vec![PaneDiffObservation {
                agent_id: "a1".to_string(),
                text: "initial".to_string(),
                log_mtime: None,
                provider: None,
            }],
            start,
            Duration::from_secs(300),
        );
        process_pane_diff_observations(
            &mut state_map,
            Vec::new(),
            start + Duration::from_secs(30),
            Duration::from_secs(300),
        );

        assert!(state_map.is_empty());
    }

    #[test]
    fn test_compute_content_hash_changes_reset_timer() {
        let same_a = compute_content_hash("Thinking...\nunchanged body");
        let same_b = compute_content_hash("Thinking...\nunchanged body");
        let changed = compute_content_hash("Thinking...\nnew body");

        assert_eq!(same_a, same_b);
        assert_ne!(
            same_a, changed,
            "content hash change should reset stuck timer"
        );
    }

    #[test]
    fn test_query_log_mtime_changes_reset_timer() {
        let dir = tempfile::TempDir::new().unwrap();
        let log_path = dir.path().join("agent.log");
        fs::write(&log_path, "first\n").unwrap();
        let first = query_log_mtime(&log_path).expect("first mtime");

        std::thread::sleep(Duration::from_millis(5));
        fs::write(&log_path, "second\n").unwrap();
        let second = query_log_mtime(&log_path).expect("second mtime");

        assert!(second > first, "mtime change should reset stuck timer");
    }

    #[test]
    fn test_detect_thinking_spinner_match() {
        assert!(detect_thinking_spinner("Thinking..."));
        assert!(detect_thinking_spinner("Spinner still running"));
        assert!(detect_thinking_spinner("Working on your request"));
        assert!(!detect_thinking_spinner("final answer text"));
    }

    #[test]
    fn test_three_signals_static_marks_stuck() {
        let mut state_map = HashMap::new();
        let start = Instant::now();
        let observations = vec![PaneDiffObservation {
            agent_id: "a1".to_string(),
            text: "Thinking...".to_string(),
            log_mtime: None,
            provider: None,
        }];

        let first = process_pane_diff_observations(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(5),
        );
        let second = process_pane_diff_observations(
            &mut state_map,
            observations,
            start + Duration::from_secs(6),
            Duration::from_secs(5),
        );

        assert!(first.stuck_agent_ids.is_empty());
        assert_eq!(second.stuck_agent_ids, ["a1"]);
    }

    #[test]
    fn ui_only_marker_recapture_completes_after_stable_antigravity_ticks() {
        let mut state_map = HashMap::new();
        let start = Instant::now();
        let observations = vec![PaneDiffObservation {
            agent_id: "a_pd_stable_ticks".to_string(),
            text: "answer\n? for shortcuts                          Gemini 3.5 Flash (High)\n"
                .to_string(),
            log_mtime: None,
            provider: Some("antigravity".to_string()),
        }];
        let expected_pane_text = observations[0].text.clone();

        let first = process_pane_diff_observations(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(300),
        );
        let second = process_pane_diff_observations(
            &mut state_map,
            observations,
            start + Duration::from_secs(30),
            Duration::from_secs(300),
        );

        assert!(first.ui_completed.is_empty());
        assert!(second.stuck_agent_ids.is_empty());
        assert_eq!(second.ui_completed[0].agent_id, "a_pd_stable_ticks");
        assert_eq!(second.ui_completed[0].pane_text, expected_pane_text);
    }

    #[test]
    fn ui_completion_recapture_handles_follow_width_wrapped_provider_markers() {
        let start = Instant::now();
        for (provider, capture) in [
            (
                "codex",
                "answer\n› \n  gpt-5.5 default · /workspace/with/a/very/long/path\n",
            ),
            (
                "claude",
                "answer\n❯ Try \"fix lint errors in the whole workspace\"\n──────────────────────────────\n",
            ),
            (
                "antigravity",
                "answer\n? for shortcuts\nGemini 3.5 Flash (High)\n",
            ),
        ] {
            let mut state_map = HashMap::new();
            let result = process_pane_diff_observations_with_ui_completion_stable_ticks(
                &mut state_map,
                vec![PaneDiffObservation {
                    agent_id: format!("a_pd_follow_{provider}"),
                    text: capture.to_string(),
                    log_mtime: None,
                    provider: Some(provider.to_string()),
                }],
                start,
                Duration::from_secs(300),
                1,
            );

            assert_eq!(
                result.ui_completed.len(),
                1,
                "{provider} wrapped idle marker should recapture"
            );
            assert!(result.stuck_agent_ids.is_empty());
        }
    }

    #[test]
    fn ui_completion_recapture_keeps_follow_width_wrapped_busy_markers_busy() {
        let start = Instant::now();
        for (provider, capture) in [
            (
                "codex",
                "› rewrite the parser\n◦ Working (2s • esc to interrupt)\n  gpt-5.5 default · /workspace/with/a/very/long/path\n",
            ),
            (
                "claude",
                "❯ \nReading 12 files (ctrl+o to expand)\nArchitecting (4s) · esc to interrupt\npaste again to expand\n",
            ),
            (
                "antigravity",
                "? for shortcuts\n⣯ Generating...\nesc to cancel\nGemini 3.5 Flash (High)\n",
            ),
        ] {
            let mut state_map = HashMap::new();
            let result = process_pane_diff_observations_with_ui_completion_stable_ticks(
                &mut state_map,
                vec![PaneDiffObservation {
                    agent_id: format!("a_pd_follow_busy_{provider}"),
                    text: capture.to_string(),
                    log_mtime: None,
                    provider: Some(provider.to_string()),
                }],
                start,
                Duration::from_secs(300),
                1,
            );

            assert!(
                result.ui_completed.is_empty(),
                "{provider} wrapped busy marker should not recapture"
            );
            assert!(result.stuck_agent_ids.is_empty());
        }
    }

    #[test]
    fn ui_only_marker_recapture_completes_real_antigravity_idle_capture_after_two_ticks() {
        let bytes = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/pane_idle/REAL-a3-idle-capture.txt"
        ))
        .to_string();
        let obs = PaneDiffObservation {
            agent_id: "a_pd_real_idle_capture".to_string(),
            text: bytes.clone(),
            log_mtime: None,
            provider: Some("antigravity".to_string()),
        };
        let mut state_map = HashMap::new();
        let start = Instant::now();

        let r1 = process_pane_diff_observations(
            &mut state_map,
            vec![obs.clone()],
            start,
            Duration::from_secs(300),
        );
        let r2 = process_pane_diff_observations(
            &mut state_map,
            vec![obs],
            start + Duration::from_secs(30),
            Duration::from_secs(300),
        );

        assert!(r1.ui_completed.is_empty());
        assert_eq!(r2.ui_completed.len(), 1);
        assert_eq!(r2.ui_completed[0].agent_id, "a_pd_real_idle_capture");
        assert_eq!(r2.ui_completed[0].pane_text, bytes);
    }

    #[test]
    fn ui_only_marker_recapture_respects_antigravity_anti_pattern() {
        let mut state_map = HashMap::new();
        let start = Instant::now();
        let observations = vec![PaneDiffObservation {
            agent_id: "a_pd_anti_pattern".to_string(),
            text: "Generating...\n? for shortcuts                          Gemini 3.5 Flash (High)\nesc to cancel                          Gemini 3.5 Flash (High)\n".to_string(),
            log_mtime: None,
            provider: Some("antigravity".to_string()),
        }];

        process_pane_diff_observations(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(300),
        );
        let second = process_pane_diff_observations(
            &mut state_map,
            observations,
            start + Duration::from_secs(30),
            Duration::from_secs(300),
        );

        assert!(second.ui_completed.is_empty());
        assert!(second.stuck_agent_ids.is_empty());
    }

    #[test]
    fn ui_only_marker_recapture_ignores_historical_marker_outside_bottom_viewport() {
        let mut state_map = HashMap::new();
        let start = Instant::now();
        let observations = vec![PaneDiffObservation {
            agent_id: "a_pd_viewport".to_string(),
            text: "old\n? for shortcuts                          Gemini 3.5 Flash (High)\nline1\nline2\nline3\nline4\nline5\nline6\n"
                .to_string(),
            log_mtime: None,
            provider: Some("antigravity".to_string()),
        }];

        process_pane_diff_observations(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(300),
        );
        let second = process_pane_diff_observations(
            &mut state_map,
            observations,
            start + Duration::from_secs(30),
            Duration::from_secs(300),
        );

        assert!(second.ui_completed.is_empty());
    }

    #[test]
    fn ui_only_marker_recapture_uses_stable_tick_counter() {
        let mut state_map = HashMap::new();
        let start = Instant::now();
        let observations = vec![PaneDiffObservation {
            agent_id: "a_pd_tick_counter".to_string(),
            text: "answer\n? for shortcuts                          Gemini 3.5 Flash (High)\n"
                .to_string(),
            log_mtime: None,
            provider: Some("antigravity".to_string()),
        }];

        let first = process_pane_diff_observations_with_ui_completion_stable_ticks(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(300),
            2,
        );
        let second = process_pane_diff_observations_with_ui_completion_stable_ticks(
            &mut state_map,
            observations.clone(),
            start + Duration::from_secs(30),
            Duration::from_secs(300),
            2,
        );

        assert!(first.ui_completed.is_empty());
        assert_eq!(second.ui_completed[0].agent_id, "a_pd_tick_counter");

        let mut state_map = HashMap::new();
        let first = process_pane_diff_observations_with_ui_completion_stable_ticks(
            &mut state_map,
            observations.clone(),
            start,
            Duration::from_secs(300),
            3,
        );
        let second = process_pane_diff_observations_with_ui_completion_stable_ticks(
            &mut state_map,
            observations.clone(),
            start + Duration::from_secs(30),
            Duration::from_secs(300),
            3,
        );
        let third = process_pane_diff_observations_with_ui_completion_stable_ticks(
            &mut state_map,
            observations,
            start + Duration::from_secs(60),
            Duration::from_secs(300),
            3,
        );

        assert!(first.ui_completed.is_empty());
        assert!(second.ui_completed.is_empty());
        assert_eq!(third.ui_completed[0].agent_id, "a_pd_tick_counter");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ui_only_recapture_candidates_include_stuck_ui_only_and_codex_log_and_ui() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        {
            let conn = db.conn();
            insert_session_sync(&conn, "s_candidates", "p_candidates", "/tmp/candidates").unwrap();
            insert_agent_sync(
                &conn,
                "a_busy_ui",
                "s_candidates",
                "antigravity",
                "BUSY",
                Some(1),
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_stuck_ui",
                "s_candidates",
                "antigravity",
                "STUCK",
                Some(2),
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_stuck_codex",
                "s_candidates",
                "codex",
                "STUCK",
                Some(3),
            )
            .unwrap();
            insert_agent_sync(
                &conn,
                "a_idle_ui",
                "s_candidates",
                "antigravity",
                "IDLE",
                Some(4),
            )
            .unwrap();
        }

        let candidates = query_ui_completion_recapture_agents(db).await.unwrap();
        let mut ids = candidates
            .into_iter()
            .map(|agent| agent.id)
            .collect::<Vec<_>>();
        ids.sort();

        assert_eq!(ids, ["a_busy_ui", "a_stuck_codex", "a_stuck_ui"]);
    }

    // ---- P0-1 RED contract tests (a4 executor writes; a1 turns GREEN) ----
    //
    // New contract: pane scanning may NEVER mutate agent/job state — it may only
    // emit an alert audit event. The two escalate_pane_diff_* seams above are
    // unimplemented!() signature stubs, so these tests panic (RED) under current
    // code and only pass once a1 implements the alert-only bodies. If any of
    // these were GREEN under current code the assertion would not be crossing the
    // new production seam.

    fn count_alert_events(db: &crate::db::Db, agent_id: &str, reason: &str) -> i64 {
        let sql = format!(
            "SELECT COUNT(*) FROM events WHERE agent_id = ? AND event_type = 'state_change' \
             AND payload LIKE '%\"reason\":\"{reason}\"%'"
        );
        db.conn()
            .query_row(&sql, [agent_id], |row| row.get(0))
            .unwrap()
    }

    fn agent_state(db: &crate::db::Db, agent_id: &str) -> String {
        db.conn()
            .query_row("SELECT state FROM agents WHERE id = ?", [agent_id], |row| {
                row.get(0)
            })
            .unwrap()
    }

    /// Test 1 — pane-diff stuck emits an alert but does NOT mutate agent state.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_pane_diff_stuck_emits_alert_but_does_not_mutate_state() {
        let agent_id = "a_p01_pane_stuck_alert";
        let job_id = "job_p01_pane_stuck_alert";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        seed_busy_dispatched_job(&db, agent_id, job_id, "antigravity", "reply charlie", None);

        let signal = StuckSignal {
            agent_id: agent_id.to_string(),
            signal_kinds: vec!["hash".to_string()],
            elapsed_secs: 420,
        };
        // RED now: escalate_pane_diff_stuck is an unimplemented!() stub → panics here.
        escalate_pane_diff_stuck(db.clone(), &signal).await.unwrap();

        // Contract (a1 GREEN): exactly one alert, agent state NOT changed to STUCK.
        assert_eq!(
            count_alert_events(&db, agent_id, "PANE_DIFF_STUCK_ALERT"),
            1,
            "pane-diff stuck must emit exactly one PANE_DIFF_STUCK_ALERT audit event"
        );
        assert_eq!(
            agent_state(&db, agent_id),
            "BUSY",
            "pane-diff stuck must NOT mutate agent state to STUCK"
        );
    }

    /// Test 2 — UI completion recapture is deleted: no idle/completed mutation.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ui_completion_recapture_deleted_no_state_mutation() {
        let agent_id = "a_p01_ui_recapture_alert";
        let job_id = "job_p01_ui_recapture_alert";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        seed_busy_dispatched_job(&db, agent_id, job_id, "antigravity", "reply charlie", None);
        let pane_text =
            "answer\n? for shortcuts                          Gemini 3.5 Flash (High)\n"
                .to_string();

        // RED now: escalate_pane_diff_ui_recapture is an unimplemented!() stub → panics here.
        escalate_pane_diff_ui_recapture(db.clone(), agent_id.to_string(), pane_text)
            .await
            .unwrap();

        // Contract (a1 GREEN): no agent->idle/completed mutation, job stays DISPATCHED,
        // only an alert event is written.
        assert_eq!(
            agent_state(&db, agent_id),
            "BUSY",
            "UI recapture must NOT mark agent idle/completed"
        );
        let job = query_job_sync(&db.conn(), job_id).unwrap().unwrap();
        assert_eq!(
            job.status, "DISPATCHED",
            "UI recapture must NOT complete the job"
        );
        assert_eq!(
            count_alert_events(&db, agent_id, "UI_RECAPTURE_ALERT"),
            1,
            "UI recapture must emit exactly one UI_RECAPTURE_ALERT audit event"
        );
    }

    /// Test 6 — fail-closed 留痕: both deleted pane->state mutation paths must
    /// ALWAYS leave an alert audit event (never silently swallowed).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_pane_downgrade_paths_are_fail_closed_and_leave_audit_trail() {
        let stuck_agent = "a_p01_failclosed_stuck";
        let ui_agent = "a_p01_failclosed_ui";
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = crate::db::init(file.path()).unwrap();
        seed_busy_dispatched_job(&db, stuck_agent, "job_fc_stuck", "antigravity", "p", None);
        seed_busy_dispatched_job(&db, ui_agent, "job_fc_ui", "antigravity", "p", None);

        let signal = StuckSignal {
            agent_id: stuck_agent.to_string(),
            signal_kinds: vec!["hash".to_string(), "thinking".to_string()],
            elapsed_secs: 600,
        };
        // RED now: first seam call panics (unimplemented!()). GREEN once both
        // downgrade paths write their alert-only audit event.
        escalate_pane_diff_stuck(db.clone(), &signal).await.unwrap();
        escalate_pane_diff_ui_recapture(
            db.clone(),
            ui_agent.to_string(),
            "answer\n? for shortcuts                          Gemini 3.5 Flash (High)\n"
                .to_string(),
        )
        .await
        .unwrap();

        assert_eq!(
            count_alert_events(&db, stuck_agent, "PANE_DIFF_STUCK_ALERT"),
            1,
            "stuck downgrade must leave a PANE_DIFF_STUCK_ALERT audit event (fail-closed)"
        );
        assert_eq!(
            count_alert_events(&db, ui_agent, "UI_RECAPTURE_ALERT"),
            1,
            "UI recapture downgrade must leave a UI_RECAPTURE_ALERT audit event (fail-closed)"
        );
    }

    fn seed_busy_dispatched_job(
        db: &crate::db::Db,
        agent_id: &str,
        job_id: &str,
        provider: &str,
        prompt_text: &str,
        output_chunk: Option<&str>,
    ) {
        let conn = db.conn();
        let session_id = format!("s_{agent_id}");
        insert_session_sync(&conn, &session_id, "p_ui_only", "/tmp/ui-only").unwrap();
        insert_agent_sync(&conn, agent_id, &session_id, provider, "BUSY", Some(123)).unwrap();
        insert_job_sync(&conn, job_id, agent_id, None, prompt_text).unwrap();
        conn.execute(
            "UPDATE jobs SET status = 'DISPATCHED', dispatched_at = unixepoch() WHERE id = ?",
            [job_id],
        )
        .unwrap();
        let seq = insert_event_sync(
            &conn,
            agent_id,
            None,
            "command_received",
            &serde_json::json!({"cmd": prompt_text, "status": "SENT"}).to_string(),
        )
        .unwrap();
        update_dispatched_seq_id_sync(&conn, job_id, seq).unwrap();
        if let Some(text) = output_chunk {
            insert_event_sync(
                &conn,
                agent_id,
                None,
                "output_chunk",
                &serde_json::json!({"text": text}).to_string(),
            )
            .unwrap();
        }
    }
}
