---
name: ah-operate
description: Use when operating an AH session, observing identity-bound jobs, or recovering a stalled Agent seat. It does not create Roadmap, Plan, Task, lifecycle, or authority facts.
---

# ah operation playbook

Use this when you are the operator driving an ah-managed master or stack through a multi-step task. This is process guidance, not a replacement for `ah-commands`, `ah-config`, or `ah-runtime-state`.

## Dispatch posture

Prefer `ah tell master` as the primary path for steering the master. Direct tmux injection with `tmux load-buffer`, `paste-buffer -p`, and `send-keys Enter` is fallback only.

The old tell-unreliability lesson was a daemon `master_pane_id` registration bug. Treat direct tmux injection as a workaround, not the normal operating procedure.

## Do not idle-wait

Subscribe to `ah events --format json` for RuntimeSnapshot session and agent transitions. Use `ah pend <job_id>` when you need to block on one submitted job.

Database polling is degraded or supplementary. The current product gap is that the events stream has no job-layer event feed yet; RuntimeSnapshot carries sessions and agents, not individual job rows.

## Unblocking

For `PROMPT_PENDING`, capture the pane, read the options, verify the highlighted `>` cursor position, then press Enter only when the intended option is selected. Use `ah prompt resolve` when the prompt can be resolved through ah instead of manual pane input.

For a `STUCK` dead-end, read pane truth first. Then cancel, kill, and re-dispatch when the current task is unrecoverable.

## Governed job boundary

AH executes a supplied job. It does not infer a Task from conversation and does not invent a Design, Plan, review round, or acceptance gate. When a coordinator supplies a governance binding, preserve the exact Roadmap stream, Roadmap node, Plan revision and step, Task, Attempt, Run, Context, Episode, Module, Capability, worktree, and physical/semantic scope identities. A terminal AH job is a runtime observation only; the owning coordinator disposes the Task result.

For autonomous repository delivery, use the root `autonomous_delivery_loop`. It selects only owner-consistent active Plan steps, obtains the worktree lease, and supplies the AH binding. Manual `ah ask` remains an ordinary interaction unless the caller explicitly supplies that persisted binding.

## Close-out discipline

Add only target files. Never use broad staging or update a protected ref from the worker seat.

When CI is red, disprove it first before rerunning: a parallel same-commit job with one red and one green is a flake signature, but still rerun once and look for pre-existing evidence. Never merge red.

## Escalation boundary

Escalate product-direction choices to the user. Decide engineering details, such as commit placement and fix order, autonomously.

## Operator monitors

Context monitor: read the status-line context hint on each capture. At convergence points, drive `/clear`, then re-inject orientation that points at the on-disk spec, design, or handoff files plus the next work brief.

Model and effort monitor: steady state is the strongest model with high effort. Use maximum effort only for very heavy work. If a master or worker drops to a weaker model, including from a rate-limit popup, correct it instead of letting the low-config run continue.

Quota monitor: watch usage signals, but verify freshness first. A status-line is rendered residue and can lag the actual quota refresh; on 2026-07-08 a real instance still showed a credits hint after the subscription had renewed. Confirm with the user before acting. Never silently burn credits, silently downgrade, or treat lagging UI as a live alarm.
