//! Async InitGate driver for spawned provider panes.

use crate::db::events::UNKNOWN_PATTERN_STABLE;
use crate::db::learned_rules::{
    CursorAnchor, LearnedRule, LearnedRuleCategory, RuleFingerprint, lookup_learned_rules_sync,
};
use crate::db::{self, Db};
use crate::error::AhError;
use crate::marker::MarkerMatcher;
use crate::prompt_handler::integration::is_prompt_handling_provider;
use crate::prompt_handler::kb::load_or_bootstrap_kb;
use crate::prompt_handler::llm_client::RealHaikuClassifier;
use crate::prompt_handler::runner::{
    PromptRunOutcome, RunnerContext, TmuxPromptIo, handle_prompt_chain,
};
use crate::provider::manifest::InitProbeKind;
use crate::tmux::{TmuxPaneId, TmuxServer};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const POLL_FAST: Duration = Duration::from_millis(200);
const POLL_SLOW: Duration = Duration::from_millis(500);
const POLL_SWITCH: Duration = Duration::from_secs(5);
pub const STABLE_UNKNOWN_STARTUP_GRACE: Duration = Duration::from_secs(10);
const STEADY_COUNT: u32 = 2;
const UNKNOWN_PATTERN_SELF_HEALED: &str = "UNKNOWN_PATTERN_SELF_HEALED";

pub fn spawn_init_probe_task(
    agent_id: String,
    tmux: Arc<TmuxServer>,
    pane: TmuxPaneId,
    db: Arc<Db>,
    provider: String,
    state_dir: PathBuf,
    marker_matcher: Arc<MarkerMatcher>,
    probe_kind: InitProbeKind,
    deadline: Duration,
    startup_grace: Duration,
    idle_scan_enabled: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = run_init_probe_task(
            agent_id.clone(),
            tmux,
            pane,
            db,
            provider,
            state_dir,
            marker_matcher,
            probe_kind,
            deadline,
            startup_grace,
            idle_scan_enabled,
        )
        .await
        {
            tracing::warn!(agent_id = %agent_id, error = %err, "init probe task failed");
        }
    })
}

pub async fn respawn_init_probe_for_agent(
    agent_id: String,
    tmux: Arc<TmuxServer>,
    db: Arc<Db>,
    state_dir: PathBuf,
) -> Result<bool, AhError> {
    let Some(agent) = crate::db::agents::query_agent(db.as_ref().clone(), agent_id.clone()).await?
    else {
        tracing::debug!(agent_id, "agent not found; skipping init probe respawn");
        return Ok(false);
    };
    if agent.state != db::state_machine::STATE_SPAWNING_INTERVENTION {
        return Ok(false);
    }
    let Some((pane, idle_scan_enabled)) = crate::agent_io::init_probe_binding(&agent_id) else {
        tracing::debug!(
            agent_id,
            "agent pane not registered; cannot respawn init probe"
        );
        return Ok(false);
    };
    let manifest = crate::provider::manifest::get_manifest(&agent.provider);
    spawn_init_probe_task(
        agent_id,
        tmux,
        pane,
        db,
        agent.provider,
        state_dir,
        Arc::new(MarkerMatcher::from_manifest(&manifest)),
        manifest.init_probe,
        Duration::from_secs(manifest.readiness_timeout_s.into()),
        STABLE_UNKNOWN_STARTUP_GRACE,
        idle_scan_enabled,
    );
    Ok(true)
}

async fn run_init_probe_task(
    agent_id: String,
    tmux: Arc<TmuxServer>,
    pane: TmuxPaneId,
    db: Arc<Db>,
    provider: String,
    state_dir: PathBuf,
    marker_matcher: Arc<MarkerMatcher>,
    probe_kind: InitProbeKind,
    deadline: Duration,
    startup_grace: Duration,
    idle_scan_enabled: Arc<AtomicBool>,
) -> Result<(), AhError> {
    let probe = probe_kind.build();
    let start = tokio::time::Instant::now();
    let deadline_at = start + deadline;
    let mut consecutive = 0_u32;
    let mut last_capture = String::new();
    let mut stable_unknown = StableUnknownDetector::default();

    loop {
        if tokio::time::Instant::now() >= deadline_at {
            mark_unknown_after_timeout(db, agent_id, last_capture).await?;
            return Ok(());
        }

        match capture_visible(tmux.clone(), pane.clone()).await {
            Ok(capture) => {
                if capture.trim().is_empty() {
                    consecutive = 0;
                    tracing::debug!(
                        agent_id = %agent_id,
                        "init probe captured empty spawning pane; retrying"
                    );
                    sleep_init_probe_poll(start).await;
                    continue;
                }

                let learned_detected =
                    learned_startup_readiness_detected(db.as_ref(), &provider, &capture)?;
                let seed_detected = probe.detect(&capture);
                let detected = learned_detected || seed_detected;
                last_capture = capture;
                if detected {
                    if is_prompt_handling_provider(&provider) && !learned_detected {
                        match scan_startup_prompt(
                            db.clone(),
                            agent_id.clone(),
                            provider.clone(),
                            pane.clone(),
                            tmux.clone(),
                            state_dir.clone(),
                            marker_matcher.clone(),
                        )
                        .await
                        {
                            Ok(StartupPromptScan::ReadyConfirmed) => {
                                if record_readiness_match(&mut consecutive) {
                                    mark_idle_after_seed_probe(db, agent_id, idle_scan_enabled)
                                        .await?;
                                    return Ok(());
                                }
                            }
                            Ok(StartupPromptScan::HandledOrClear) => {
                                consecutive = 0;
                            }
                            Ok(StartupPromptScan::Deferred) => {
                                if provider_seed_probe_can_count_as_ready(probe_kind) {
                                    if record_readiness_match(&mut consecutive) {
                                        mark_idle_after_seed_probe(db, agent_id, idle_scan_enabled)
                                            .await?;
                                        return Ok(());
                                    }
                                } else {
                                    consecutive = 0;
                                }
                            }
                            Ok(StartupPromptScan::Pending) => {
                                if provider_seed_probe_can_count_as_ready(probe_kind) {
                                    if record_readiness_match(&mut consecutive) {
                                        mark_idle_after_seed_probe(db, agent_id, idle_scan_enabled)
                                            .await?;
                                        return Ok(());
                                    }
                                } else {
                                    consecutive = 0;
                                }
                            }
                            Err(err) => {
                                consecutive = 0;
                                tracing::debug!(
                                    agent_id = %agent_id,
                                    error = %err,
                                    "startup readiness confirmation failed; init probe will retry until deadline"
                                );
                            }
                        }
                    } else {
                        if record_readiness_match(&mut consecutive) {
                            if learned_detected {
                                mark_idle_after_learned_startup_readiness(
                                    db,
                                    agent_id,
                                    idle_scan_enabled,
                                )
                                .await?;
                            } else {
                                mark_idle_after_seed_probe(db, agent_id, idle_scan_enabled).await?;
                            }
                            return Ok(());
                        }
                    }
                } else {
                    consecutive = 0;
                    let startup_scan = scan_startup_prompt(
                        db.clone(),
                        agent_id.clone(),
                        provider.clone(),
                        pane.clone(),
                        tmux.clone(),
                        state_dir.clone(),
                        marker_matcher.clone(),
                    )
                    .await;
                    match startup_scan {
                        Ok(StartupPromptScan::ReadyConfirmed) => {
                            if record_readiness_match(&mut consecutive) {
                                mark_idle_after_seed_probe(db, agent_id, idle_scan_enabled).await?;
                                return Ok(());
                            }
                        }
                        Ok(StartupPromptScan::HandledOrClear) => {
                            if is_prompt_handling_provider(&provider) {
                                stable_unknown.reset();
                            } else if let Some(payload) = stable_unknown.record(
                                &last_capture,
                                tokio::time::Instant::now().saturating_duration_since(start),
                                startup_grace,
                            ) {
                                mark_spawning_intervention_and_emit_unknown_pattern(
                                    db.clone(),
                                    agent_id.clone(),
                                    provider.clone(),
                                    payload,
                                )
                                .await?;
                            }
                        }
                        Ok(StartupPromptScan::Deferred) => {
                            if let Some(payload) = stable_unknown.record(
                                &last_capture,
                                tokio::time::Instant::now().saturating_duration_since(start),
                                startup_grace,
                            ) {
                                mark_spawning_intervention_and_emit_unknown_pattern(
                                    db.clone(),
                                    agent_id.clone(),
                                    provider.clone(),
                                    payload,
                                )
                                .await?;
                            }
                        }
                        Ok(StartupPromptScan::Pending) => {
                            consecutive = 0;
                        }
                        Err(err) => {
                            tracing::debug!(
                                agent_id = %agent_id,
                                error = %err,
                                "startup prompt scan failed; init probe will retry until deadline"
                            );
                        }
                    }
                }
            }
            Err(err) => {
                tracing::debug!(agent_id = %agent_id, error = %err, "init probe capture failed");
                consecutive = 0;
            }
        }

        sleep_init_probe_poll(start).await;
    }
}

async fn sleep_init_probe_poll(start: tokio::time::Instant) {
    let elapsed = tokio::time::Instant::now().saturating_duration_since(start);
    let period = if elapsed < POLL_SWITCH {
        POLL_FAST
    } else {
        POLL_SLOW
    };
    tokio::time::sleep(period).await;
}

fn record_readiness_match(consecutive: &mut u32) -> bool {
    *consecutive += 1;
    *consecutive >= STEADY_COUNT
}

fn provider_seed_probe_can_count_as_ready(probe_kind: InitProbeKind) -> bool {
    matches!(probe_kind, InitProbeKind::Codex | InitProbeKind::Claude)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StableUnknownPayload {
    capture_hash: String,
    stable_scans: u32,
}

#[derive(Debug, Default)]
struct StableUnknownDetector {
    last_hash: Option<String>,
    stable_scans: u32,
}

impl StableUnknownDetector {
    fn record(
        &mut self,
        capture: &str,
        elapsed: Duration,
        startup_grace: Duration,
    ) -> Option<StableUnknownPayload> {
        if capture.trim().is_empty() {
            self.reset();
            return None;
        }

        let sanitized = crate::prompt_handler::matcher::sanitize_pane_text(capture);
        if sanitized.trim().is_empty() {
            self.reset();
            return None;
        }

        let capture_hash = stable_capture_hash(&sanitized);
        if self.last_hash.as_deref() == Some(capture_hash.as_str()) {
            self.stable_scans += 1;
        } else {
            self.last_hash = Some(capture_hash.clone());
            self.stable_scans = 1;
        }

        if elapsed < startup_grace {
            return None;
        }

        (self.stable_scans >= 3).then_some(StableUnknownPayload {
            capture_hash,
            stable_scans: self.stable_scans,
        })
    }

    fn reset(&mut self) {
        self.last_hash = None;
        self.stable_scans = 0;
    }
}

fn stable_capture_hash(sanitized_capture: &str) -> String {
    let hash: [u8; 32] = Sha256::digest(sanitized_capture.as_bytes()).into();
    crate::db::prompt_experience::hash_hex(&hash)
}

fn learned_startup_readiness_detected(
    db: &Db,
    provider: &str,
    capture: &str,
) -> Result<bool, AhError> {
    let rules = lookup_learned_rules_sync(db, provider, LearnedRuleCategory::StartupReadiness)?;
    for rule in rules {
        if learned_rule_matches_capture(&rule, capture)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn learned_rule_matches_capture(rule: &LearnedRule, capture: &str) -> Result<bool, AhError> {
    match &rule.fingerprint {
        RuleFingerprint::Regex { pattern } => {
            let regex = compile_learned_regex(rule, pattern)?;
            let Some(matched) = regex.find(capture) else {
                return Ok(false);
            };
            Ok(cursor_anchor_matches(
                capture,
                matched,
                rule.cursor_anchor.as_ref(),
            ))
        }
    }
}

fn compile_learned_regex(rule: &LearnedRule, pattern: &str) -> Result<regex::Regex, AhError> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for flag in &rule.regex_flags {
        match flag.as_str() {
            "CaseInsensitive" | "case_insensitive" | "i" => {
                builder.case_insensitive(true);
            }
            "Multiline" | "multi_line" | "m" => {
                builder.multi_line(true);
            }
            "DotMatchesNewLine" | "dot_matches_new_line" | "s" => {
                builder.dot_matches_new_line(true);
            }
            "SwapGreed" | "swap_greed" | "U" => {
                builder.swap_greed(true);
            }
            _ => {}
        }
    }
    builder.build().map_err(|err| {
        AhError::IpcInvalidRequest(format!(
            "invalid persisted learned rule regex {}: {err}",
            rule.id
        ))
    })
}

fn cursor_anchor_matches(
    capture: &str,
    matched: regex::Match<'_>,
    cursor_anchor: Option<&CursorAnchor>,
) -> bool {
    let Some(anchor) = cursor_anchor else {
        return true;
    };
    let (match_row, match_end_col) = capture_position(capture, matched.end());
    let target_row = match_row as isize + anchor.cursor_row_delta_from_match as isize;
    if target_row < 0 {
        return false;
    }
    let Some(line) = capture.lines().nth(target_row as usize) else {
        return false;
    };
    let target_col = match_end_col as isize + anchor.cursor_col_delta_from_match_end as isize;
    target_col >= 0 && target_col as usize <= line.chars().count() + 1
}

fn capture_position(capture: &str, byte_offset: usize) -> (usize, usize) {
    let mut row = 0_usize;
    let mut col = 0_usize;
    for (idx, ch) in capture.char_indices() {
        if idx >= byte_offset {
            break;
        }
        if ch == '\n' {
            row += 1;
            col = 0;
        } else {
            col += 1;
        }
    }
    (row, col)
}

enum StartupPromptScan {
    ReadyConfirmed,
    HandledOrClear,
    Deferred,
    Pending,
}

async fn scan_startup_prompt(
    db: Arc<Db>,
    agent_id: String,
    provider: String,
    pane: TmuxPaneId,
    tmux: Arc<TmuxServer>,
    state_dir: PathBuf,
    marker_matcher: Arc<MarkerMatcher>,
) -> Result<StartupPromptScan, AhError> {
    if !is_prompt_handling_provider(&provider) {
        return Ok(StartupPromptScan::HandledOrClear);
    }

    tokio::task::spawn_blocking(move || {
        let kb_path = state_dir.join("prompt-cases.json");
        let kb = load_or_bootstrap_kb(&kb_path).map_err(AhError::from)?;
        let io = TmuxPromptIo::new((*tmux).clone());
        let db_handle = db.as_ref().clone();
        let ctx = RunnerContext::new(&agent_id, &pane, &provider, &io, &kb)
            .with_current_state(db::state_machine::STATE_SPAWNING)
            .with_prompt_experience(&db_handle)
            .with_llm_classifier(&RealHaikuClassifier)
            .with_marker_matcher(marker_matcher.as_ref());

        match handle_prompt_chain(ctx, 3) {
            PromptRunOutcome::NoActionNeeded { depth } if depth > 0 => {
                Ok(StartupPromptScan::HandledOrClear)
            }
            PromptRunOutcome::NoActionNeeded { .. } => Ok(StartupPromptScan::ReadyConfirmed),
            PromptRunOutcome::RetryLater { .. } => Ok(StartupPromptScan::Deferred),
            PromptRunOutcome::Pending { .. } | PromptRunOutcome::DepthExceeded { .. } => {
                Ok(StartupPromptScan::Pending)
            }
            PromptRunOutcome::ExecutorFailed { error, .. } => Err(error),
        }
    })
    .await
    .map_err(|err| AhError::DatabaseRuntimePanic {
        details: format!("startup prompt scan worker join failed: {err}"),
    })?
}

async fn capture_visible(tmux: Arc<TmuxServer>, pane: TmuxPaneId) -> Result<String, AhError> {
    crate::db::common::spawn_db("tmux::capture_visible_pane", move || {
        let args = [
            "-L",
            tmux.socket_name(),
            "capture-pane",
            "-p",
            "-t",
            &pane.0,
        ];
        let output = Command::new("tmux").args(args).output().map_err(|err| {
            AhError::EnvironmentNotSupported {
                details: format!("run tmux capture-pane: {err}"),
            }
        })?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(AhError::TmuxCommandFailed {
                cmd: format!("tmux {}", args.join(" ")),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                exit: output.status.code().unwrap_or(-1),
            })
        }
    })
    .await
}

async fn mark_idle_after_seed_probe(
    db: Arc<Db>,
    agent_id: String,
    idle_scan_enabled: Arc<AtomicBool>,
) -> Result<(), AhError> {
    let previous_state =
        crate::db::agents::query_agent_state(db.as_ref().clone(), agent_id.clone()).await?;
    if previous_state
        .as_ref()
        .is_some_and(|(state, _)| state == db::state_machine::STATE_SPAWNING_INTERVENTION)
    {
        match db::state_machine::transit_agent_state(
            db.as_ref().clone(),
            agent_id.clone(),
            vec![db::state_machine::STATE_SPAWNING_INTERVENTION.to_string()],
            db::state_machine::STATE_IDLE.to_string(),
            Some("SEED_READINESS".to_string()),
        )
        .await
        {
            Ok(()) => {
                emit_unknown_pattern_self_healed(db.clone(), agent_id.clone()).await?;
                finish_startup_readiness(&agent_id, &idle_scan_enabled);
            }
            Err(AhError::AgentWrongState { .. }) => {}
            Err(err) => return Err(err),
        }
        return Ok(());
    }

    match db::state_machine::mark_agent_idle_matched(db.as_ref().clone(), agent_id.clone()).await {
        Ok((changes, affected_job)) if changes > 0 => {
            if let Some(job_id) = affected_job {
                crate::orchestrator::pubsub::notify_job_update(&job_id);
            }
            finish_startup_readiness(&agent_id, &idle_scan_enabled);
        }
        Ok(_) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

fn finish_startup_readiness(agent_id: &str, idle_scan_enabled: &AtomicBool) {
    crate::orchestrator::wake_up();
    idle_scan_enabled.store(true, Ordering::SeqCst);
    if let Some(handle) = crate::marker::registry::take(agent_id) {
        let _ = handle.cancel_tx.send(());
    }
}

async fn emit_unknown_pattern_self_healed(db: Arc<Db>, agent_id: String) -> Result<(), AhError> {
    let payload = json!({
        "category_hint": "StartupReadiness",
        "supersedes": UNKNOWN_PATTERN_STABLE,
        "from": db::state_machine::STATE_SPAWNING_INTERVENTION,
        "to": db::state_machine::STATE_IDLE,
        "reason": "SEED_READINESS",
    });
    crate::db::events::insert_event_and_notify(
        db.as_ref().clone(),
        agent_id,
        None,
        UNKNOWN_PATTERN_SELF_HEALED.to_string(),
        payload.to_string(),
    )
    .await?;
    Ok(())
}

async fn mark_idle_after_learned_startup_readiness(
    db: Arc<Db>,
    agent_id: String,
    idle_scan_enabled: Arc<AtomicBool>,
) -> Result<(), AhError> {
    match db::state_machine::transit_agent_state(
        db.as_ref().clone(),
        agent_id.clone(),
        vec![
            db::state_machine::STATE_SPAWNING.to_string(),
            db::state_machine::STATE_SPAWNING_INTERVENTION.to_string(),
        ],
        db::state_machine::STATE_IDLE.to_string(),
        Some("LEARNED_STARTUP_READINESS".to_string()),
    )
    .await
    {
        Ok(()) => {
            crate::orchestrator::wake_up();
            idle_scan_enabled.store(true, Ordering::SeqCst);
            if let Some(handle) = crate::marker::registry::take(&agent_id) {
                let _ = handle.cancel_tx.send(());
            }
        }
        Err(AhError::AgentWrongState { .. }) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

async fn mark_spawning_intervention_and_emit_unknown_pattern(
    db: Arc<Db>,
    agent_id: String,
    provider: String,
    stable_unknown: StableUnknownPayload,
) -> Result<(), AhError> {
    match db::state_machine::transit_agent_state(
        db.as_ref().clone(),
        agent_id.clone(),
        vec![db::state_machine::STATE_SPAWNING.to_string()],
        db::state_machine::STATE_SPAWNING_INTERVENTION.to_string(),
        Some(UNKNOWN_PATTERN_STABLE.to_string()),
    )
    .await
    {
        Ok(()) => {
            let payload = json!({
                "category_hint": "StartupReadiness",
                "provider": provider,
                "capture_hash": stable_unknown.capture_hash,
                "stable_scans": stable_unknown.stable_scans,
                "source_state": "SPAWNING",
            });
            crate::db::events::insert_event_and_notify(
                db.as_ref().clone(),
                agent_id,
                None,
                UNKNOWN_PATTERN_STABLE.to_string(),
                payload.to_string(),
            )
            .await?;
            crate::orchestrator::wake_up();
        }
        Err(AhError::AgentWrongState { .. }) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

async fn mark_unknown_after_timeout(
    db: Arc<Db>,
    agent_id: String,
    capture: String,
) -> Result<(), AhError> {
    let failed_changes = db::state_machine::mark_agent_failed_from_intervention(
        db.as_ref().clone(),
        agent_id.clone(),
        "MASTER_OFFLINE_INTERVENTION_TIMEOUT".to_string(),
    )
    .await?;
    if failed_changes > 0 {
        crate::orchestrator::wake_up();
        return Ok(());
    }

    let failed_rules = json!(["init_probe"]);
    match db::state_machine::mark_agent_unknown(
        db.as_ref().clone(),
        agent_id.clone(),
        "INIT_PROBE_TIMEOUT".to_string(),
        capture.into_bytes(),
        failed_rules,
    )
    .await
    {
        Ok((changes, affected_job)) if changes > 0 => {
            if let Some(job_id) = affected_job {
                crate::orchestrator::pubsub::notify_job_update(&job_id);
            }
            crate::orchestrator::wake_up();
        }
        Ok(_) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        STABLE_UNKNOWN_STARTUP_GRACE, StableUnknownDetector, UNKNOWN_PATTERN_SELF_HEALED,
        cursor_anchor_matches, learned_rule_matches_capture, mark_idle_after_seed_probe,
        mark_unknown_after_timeout, record_readiness_match,
    };
    use crate::db::agents::{insert_agent_sync, query_agent_state_sync};
    use crate::db::events::UNKNOWN_PATTERN_STABLE;
    use crate::db::jobs::query_dispatched_job_for_agent_sync;
    use crate::db::learned_rules::{
        CursorAnchor, LearnedRule, LearnedRuleCategory, RuleFingerprint,
    };
    use crate::db::sessions::insert_session_sync;
    use crate::db::{Db, init};
    use crate::db::{events, state_machine};
    use serde_json::Value;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn seed_spawning_agent(db: &Db, agent_id: &str, state: &str) {
        let conn = db.conn();
        insert_session_sync(&conn, "s_init_probe", "p_init_probe", "/tmp/init_probe").unwrap();
        insert_agent_sync(&conn, agent_id, "s_init_probe", "bash", state, Some(123)).unwrap();
    }

    fn startup_rule(pattern: &str, cursor_anchor: Option<CursorAnchor>) -> LearnedRule {
        LearnedRule {
            id: "lr_test".to_string(),
            provider: "claude".to_string(),
            category: LearnedRuleCategory::StartupReadiness,
            fingerprint: RuleFingerprint::Regex {
                pattern: pattern.to_string(),
            },
            regex_flags: Vec::new(),
            positive_examples: vec![r#"❯ Try "fix lint errors""#.to_string()],
            action: None,
            extraction: None,
            cursor_anchor,
            source_event_seq_id: None,
            enabled: true,
        }
    }

    #[test]
    fn learned_startup_readiness_rule_matches_claude_try_prompt() {
        let rule = startup_rule(r#"(?m)^\s*❯\s+Try "fix lint errors""#, None);

        assert!(
            learned_rule_matches_capture(
                &rule,
                "Claude Code\nOpus 4.8\n❯ Try \"fix lint errors\"\n"
            )
            .unwrap()
        );
    }

    #[test]
    fn learned_startup_readiness_rule_consumes_regex_flags() {
        let mut rule = startup_rule(r#"^\s*❯\s+Try"#, None);
        rule.regex_flags = vec!["Multiline".to_string()];

        assert!(
            learned_rule_matches_capture(&rule, "Claude Code\n❯ Try \"fix lint errors\"\n")
                .unwrap()
        );
    }

    #[test]
    fn learned_startup_readiness_rule_honors_cursor_anchor_bounds() {
        let capture = "Claude Code\n❯ Try \"fix lint errors\"\n";
        let regex = regex::Regex::new(r#"(?m)^\s*❯"#).unwrap();
        let matched = regex.find(capture).unwrap();

        assert!(cursor_anchor_matches(
            capture,
            matched,
            Some(&CursorAnchor {
                cursor_row_delta_from_match: 0,
                cursor_col_delta_from_match_end: 1,
            })
        ));
        assert!(!cursor_anchor_matches(
            capture,
            matched,
            Some(&CursorAnchor {
                cursor_row_delta_from_match: 8,
                cursor_col_delta_from_match_end: 1,
            })
        ));
    }

    #[test]
    fn stable_unknown_detector_requires_three_identical_sanitized_captures() {
        let mut detector = StableUnknownDetector::default();
        let after_grace = STABLE_UNKNOWN_STARTUP_GRACE + Duration::from_millis(1);

        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        let payload = detector
            .record(
                "Mystery startup panel",
                after_grace,
                STABLE_UNKNOWN_STARTUP_GRACE,
            )
            .unwrap();

        assert_eq!(payload.stable_scans, 3);
        assert_eq!(payload.capture_hash.len(), 64);
    }

    #[test]
    fn stable_unknown_detector_resets_on_changed_or_empty_capture() {
        let mut detector = StableUnknownDetector::default();
        let after_grace = STABLE_UNKNOWN_STARTUP_GRACE + Duration::from_millis(1);

        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Different startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record("", after_grace, STABLE_UNKNOWN_STARTUP_GRACE)
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Different startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Different startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Different startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_some()
        );
    }

    #[test]
    fn stable_unknown_detector_suppresses_payload_during_startup_grace() {
        let mut detector = StableUnknownDetector::default();
        let inside_grace = STABLE_UNKNOWN_STARTUP_GRACE - Duration::from_millis(1);

        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    inside_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    inside_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    inside_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    inside_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
    }

    #[test]
    fn stable_unknown_detector_emits_payload_after_startup_grace() {
        let mut detector = StableUnknownDetector::default();
        let after_grace = STABLE_UNKNOWN_STARTUP_GRACE + Duration::from_millis(1);

        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Mystery startup panel",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        let payload = detector
            .record(
                "Mystery startup panel",
                after_grace,
                STABLE_UNKNOWN_STARTUP_GRACE,
            )
            .unwrap();

        assert_eq!(payload.stable_scans, 3);
    }

    #[test]
    fn learned_startup_readiness_still_requires_two_steady_matches() {
        let mut consecutive = 0;

        assert!(!record_readiness_match(&mut consecutive));
        assert!(record_readiness_match(&mut consecutive));
    }

    #[tokio::test]
    async fn seed_readiness_marks_spawning_intervention_idle_without_dispatched_job() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        seed_spawning_agent(&db, "a_seed_si", state_machine::STATE_SPAWNING_INTERVENTION);
        let idle_scan_enabled = Arc::new(AtomicBool::new(false));

        mark_idle_after_seed_probe(
            Arc::new(db.clone()),
            "a_seed_si".to_string(),
            idle_scan_enabled.clone(),
        )
        .await
        .unwrap();

        let (state, _) = query_agent_state_sync(&db, "a_seed_si").unwrap().unwrap();
        assert_eq!(state, state_machine::STATE_IDLE);
        assert!(idle_scan_enabled.load(Ordering::SeqCst));
        assert!(
            query_dispatched_job_for_agent_sync(&db.conn(), "a_seed_si")
                .unwrap()
                .is_none()
        );

        let event_payload: String = db
            .conn()
            .query_row(
                "SELECT payload FROM events WHERE agent_id = ? AND event_type = ?",
                ("a_seed_si", UNKNOWN_PATTERN_SELF_HEALED),
                |row| row.get(0),
            )
            .unwrap();
        let payload: Value = serde_json::from_str(&event_payload).unwrap();
        assert_eq!(
            payload.get("supersedes").and_then(Value::as_str),
            Some(UNKNOWN_PATTERN_STABLE)
        );
    }

    #[tokio::test]
    async fn seed_readiness_still_marks_spawning_idle() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        seed_spawning_agent(&db, "a_seed_spawning", state_machine::STATE_SPAWNING);
        let idle_scan_enabled = Arc::new(AtomicBool::new(false));

        mark_idle_after_seed_probe(
            Arc::new(db.clone()),
            "a_seed_spawning".to_string(),
            idle_scan_enabled,
        )
        .await
        .unwrap();

        let (state, sub_state): (String, Option<String>) = db
            .conn()
            .query_row(
                "SELECT state, sub_state FROM agents WHERE id = ?",
                ("a_seed_spawning",),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, state_machine::STATE_IDLE);
        assert_eq!(sub_state.as_deref(), Some("Matched"));

        let event_payload: String = db
            .conn()
            .query_row(
                "SELECT payload FROM events WHERE agent_id = ? AND event_type = ?",
                ("a_seed_spawning", "state_change"),
                |row| row.get(0),
            )
            .unwrap();
        let payload: Value = serde_json::from_str(&event_payload).unwrap();
        assert_eq!(payload.get("to").and_then(Value::as_str), Some("IDLE"));
        assert_eq!(
            payload.get("sub_state").and_then(Value::as_str),
            Some("Matched")
        );
        assert_eq!(
            payload.get("reason").and_then(Value::as_str),
            Some("MARKER_MATCHED")
        );
    }

    #[tokio::test]
    async fn true_unknown_after_grace_can_intervene_and_then_fail() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = init(file.path()).unwrap();
        seed_spawning_agent(&db, "a_true_unknown", state_machine::STATE_SPAWNING);
        let mut detector = StableUnknownDetector::default();
        let after_grace = STABLE_UNKNOWN_STARTUP_GRACE + Duration::from_millis(1);

        assert!(
            detector
                .record(
                    "Unknown stable startup",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        assert!(
            detector
                .record(
                    "Unknown stable startup",
                    after_grace,
                    STABLE_UNKNOWN_STARTUP_GRACE
                )
                .is_none()
        );
        let payload = detector
            .record(
                "Unknown stable startup",
                after_grace,
                STABLE_UNKNOWN_STARTUP_GRACE,
            )
            .unwrap();

        super::mark_spawning_intervention_and_emit_unknown_pattern(
            Arc::new(db.clone()),
            "a_true_unknown".to_string(),
            "bash".to_string(),
            payload,
        )
        .await
        .unwrap();
        let (state, _) = query_agent_state_sync(&db, "a_true_unknown")
            .unwrap()
            .unwrap();
        assert_eq!(state, state_machine::STATE_SPAWNING_INTERVENTION);

        mark_unknown_after_timeout(
            Arc::new(db.clone()),
            "a_true_unknown".to_string(),
            "Unknown stable startup".to_string(),
        )
        .await
        .unwrap();
        let (state, _) = query_agent_state_sync(&db, "a_true_unknown")
            .unwrap()
            .unwrap();
        assert_eq!(state, state_machine::STATE_FAILED);

        let event_count = events::query_events_since(db, "a_true_unknown".to_string(), 0)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == UNKNOWN_PATTERN_STABLE)
            .count();
        assert_eq!(event_count, 1);
    }
}
