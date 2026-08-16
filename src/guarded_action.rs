//! Causal action--observation loop for interactive provider control.
//!
//! One invocation dispatches one side effect exactly once.  It then observes
//! until the effect's declared purpose is confirmed or the deadline expires.
//! Retrying the side effect is deliberately owned by the caller because only
//! the caller knows whether another dispatch is safe.

use std::future::Future;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionObservation<T> {
    pub sequence: u64,
    pub observed_at: Instant,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionReceipt {
    pub action: &'static str,
    pub completed_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionAssessment {
    /// The observation predates, or cannot be correlated to, this action.
    NotCausal,
    /// The observation is causal but the intended effect is not present yet.
    Mismatch { reason: String },
    /// The intended effect is present.  Only this permits the next action.
    Confirmed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardedActionOutcome<T> {
    pub before: ActionObservation<T>,
    pub receipt: ActionReceipt,
    pub confirmed: ActionObservation<T>,
    pub observations_examined: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionLoopPhase {
    ObserveBefore,
    Dispatch,
    ObserveAfter,
    TimedOut,
}

#[derive(Debug)]
pub struct GuardedActionError<E> {
    pub action: &'static str,
    pub phase: ActionLoopPhase,
    pub source: Option<E>,
    pub observations_examined: u64,
    pub last_causal_mismatch: Option<String>,
}

impl<E: std::fmt::Display> std::fmt::Display for GuardedActionError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "guarded action {:?} {:?}",
            self.action, self.phase
        )?;
        if let Some(source) = &self.source {
            write!(formatter, ": {source}")?;
        }
        if let Some(reason) = &self.last_causal_mismatch {
            write!(formatter, "; last causal mismatch: {reason}")?;
        }
        Ok(())
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for GuardedActionError<E> {}

/// Async runner used by tmux prompt delivery.
pub async fn run_guarded_action<O, E, Capture, CaptureFuture, Dispatch, DispatchFuture, Assess>(
    action: &'static str,
    timeout: Duration,
    poll_interval: Duration,
    mut capture: Capture,
    dispatch: Dispatch,
    assess: Assess,
) -> Result<GuardedActionOutcome<O>, GuardedActionError<E>>
where
    Capture: FnMut() -> CaptureFuture,
    CaptureFuture: Future<Output = Result<O, E>>,
    Dispatch: FnOnce() -> DispatchFuture,
    DispatchFuture: Future<Output = Result<(), E>>,
    Assess: Fn(&ActionObservation<O>, &ActionObservation<O>) -> ActionAssessment,
{
    let before = ActionObservation {
        sequence: 0,
        observed_at: Instant::now(),
        value: capture().await.map_err(|source| GuardedActionError {
            action,
            phase: ActionLoopPhase::ObserveBefore,
            source: Some(source),
            observations_examined: 0,
            last_causal_mismatch: None,
        })?,
    };
    dispatch().await.map_err(|source| GuardedActionError {
        action,
        phase: ActionLoopPhase::Dispatch,
        source: Some(source),
        observations_examined: 0,
        last_causal_mismatch: None,
    })?;
    let receipt = ActionReceipt {
        action,
        completed_at: Instant::now(),
    };
    let deadline = receipt.completed_at + timeout;
    let mut sequence = 0_u64;
    let mut last_causal_mismatch = None;

    loop {
        sequence += 1;
        let value = capture().await.map_err(|source| GuardedActionError {
            action,
            phase: ActionLoopPhase::ObserveAfter,
            source: Some(source),
            observations_examined: sequence - 1,
            last_causal_mismatch: last_causal_mismatch.clone(),
        })?;
        let after = ActionObservation {
            sequence,
            observed_at: Instant::now(),
            value,
        };
        match assess(&before, &after) {
            ActionAssessment::Confirmed => {
                return Ok(GuardedActionOutcome {
                    before,
                    receipt,
                    confirmed: after,
                    observations_examined: sequence,
                });
            }
            ActionAssessment::Mismatch { reason } => last_causal_mismatch = Some(reason),
            ActionAssessment::NotCausal => {}
        }

        if Instant::now() >= deadline {
            return Err(GuardedActionError {
                action,
                phase: ActionLoopPhase::TimedOut,
                source: None,
                observations_examined: sequence,
                last_causal_mismatch,
            });
        }
        if !poll_interval.is_zero() {
            tokio::time::sleep(
                poll_interval.min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }
}

/// Blocking runner used by the startup prompt handler, which already runs on
/// a blocking worker.  Its semantics intentionally match the async runner.
pub fn run_guarded_action_sync<O, E, Capture, Dispatch, Assess>(
    action: &'static str,
    timeout: Duration,
    poll_interval: Duration,
    mut capture: Capture,
    dispatch: Dispatch,
    assess: Assess,
) -> Result<GuardedActionOutcome<O>, GuardedActionError<E>>
where
    Capture: FnMut() -> Result<O, E>,
    Dispatch: FnOnce() -> Result<(), E>,
    Assess: Fn(&ActionObservation<O>, &ActionObservation<O>) -> ActionAssessment,
{
    let before = ActionObservation {
        sequence: 0,
        observed_at: Instant::now(),
        value: capture().map_err(|source| GuardedActionError {
            action,
            phase: ActionLoopPhase::ObserveBefore,
            source: Some(source),
            observations_examined: 0,
            last_causal_mismatch: None,
        })?,
    };
    dispatch().map_err(|source| GuardedActionError {
        action,
        phase: ActionLoopPhase::Dispatch,
        source: Some(source),
        observations_examined: 0,
        last_causal_mismatch: None,
    })?;
    let receipt = ActionReceipt {
        action,
        completed_at: Instant::now(),
    };
    let deadline = receipt.completed_at + timeout;
    let mut sequence = 0_u64;
    let mut last_causal_mismatch = None;

    loop {
        sequence += 1;
        let value = capture().map_err(|source| GuardedActionError {
            action,
            phase: ActionLoopPhase::ObserveAfter,
            source: Some(source),
            observations_examined: sequence - 1,
            last_causal_mismatch: last_causal_mismatch.clone(),
        })?;
        let after = ActionObservation {
            sequence,
            observed_at: Instant::now(),
            value,
        };
        match assess(&before, &after) {
            ActionAssessment::Confirmed => {
                return Ok(GuardedActionOutcome {
                    before,
                    receipt,
                    confirmed: after,
                    observations_examined: sequence,
                });
            }
            ActionAssessment::Mismatch { reason } => last_causal_mismatch = Some(reason),
            ActionAssessment::NotCausal => {}
        }

        if Instant::now() >= deadline {
            return Err(GuardedActionError {
                action,
                phase: ActionLoopPhase::TimedOut,
                source: None,
                observations_examined: sequence,
                last_causal_mismatch,
            });
        }
        if !poll_interval.is_zero() {
            std::thread::sleep(
                poll_interval.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
}

/// Variant for a caller that already captured the exact baseline used to
/// classify the action. This avoids inserting an uncorrelated extra capture
/// between prompt classification and dispatch.
pub fn run_guarded_action_sync_from_before<O, E, Capture, Dispatch, Assess>(
    action: &'static str,
    before_value: O,
    timeout: Duration,
    poll_interval: Duration,
    mut capture: Capture,
    dispatch: Dispatch,
    assess: Assess,
) -> Result<GuardedActionOutcome<O>, GuardedActionError<E>>
where
    Capture: FnMut() -> Result<O, E>,
    Dispatch: FnOnce() -> Result<(), E>,
    Assess: Fn(&ActionObservation<O>, &ActionObservation<O>) -> ActionAssessment,
{
    let before = ActionObservation {
        sequence: 0,
        observed_at: Instant::now(),
        value: before_value,
    };
    dispatch().map_err(|source| GuardedActionError {
        action,
        phase: ActionLoopPhase::Dispatch,
        source: Some(source),
        observations_examined: 0,
        last_causal_mismatch: None,
    })?;
    let receipt = ActionReceipt {
        action,
        completed_at: Instant::now(),
    };
    let deadline = receipt.completed_at + timeout;
    let mut sequence = 0_u64;
    let mut last_causal_mismatch = None;

    loop {
        sequence += 1;
        let value = capture().map_err(|source| GuardedActionError {
            action,
            phase: ActionLoopPhase::ObserveAfter,
            source: Some(source),
            observations_examined: sequence - 1,
            last_causal_mismatch: last_causal_mismatch.clone(),
        })?;
        let after = ActionObservation {
            sequence,
            observed_at: Instant::now(),
            value,
        };
        match assess(&before, &after) {
            ActionAssessment::Confirmed => {
                return Ok(GuardedActionOutcome {
                    before,
                    receipt,
                    confirmed: after,
                    observations_examined: sequence,
                });
            }
            ActionAssessment::Mismatch { reason } => last_causal_mismatch = Some(reason),
            ActionAssessment::NotCausal => {}
        }
        if Instant::now() >= deadline {
            return Err(GuardedActionError {
                action,
                phase: ActionLoopPhase::TimedOut,
                source: None,
                observations_examined: sequence,
                last_causal_mismatch,
            });
        }
        if !poll_interval.is_zero() {
            std::thread::sleep(
                poll_interval.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn async_loop_dispatches_once_and_waits_for_confirmed_effect() {
        let captures = std::sync::Mutex::new(VecDeque::from([0_u8, 0, 1]));
        let dispatched = std::sync::atomic::AtomicUsize::new(0);

        let outcome = run_guarded_action(
            "paste_prompt",
            Duration::from_millis(20),
            Duration::ZERO,
            || async { Ok::<_, &'static str>(captures.lock().unwrap().pop_front().unwrap()) },
            || async {
                dispatched.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, &'static str>(())
            },
            |_before, after| {
                if after.value == 1 {
                    ActionAssessment::Confirmed
                } else {
                    ActionAssessment::Mismatch {
                        reason: "prompt not visible".into(),
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(dispatched.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(outcome.observations_examined, 2);
    }

    #[test]
    fn sync_loop_preserves_last_causal_mismatch_on_timeout() {
        let err = run_guarded_action_sync(
            "select_login_option",
            Duration::ZERO,
            Duration::ZERO,
            || Ok::<_, &'static str>("same"),
            || Ok::<_, &'static str>(()),
            |_before, _after| ActionAssessment::Mismatch {
                reason: "selection did not move".into(),
            },
        )
        .unwrap_err();

        assert_eq!(err.phase, ActionLoopPhase::TimedOut);
        assert_eq!(
            err.last_causal_mismatch.as_deref(),
            Some("selection did not move")
        );
    }
}
