# Work Execution architecture

`ah` remains an L2 runtime for provider agents. It launches isolated seats,
delivers prompts, observes provider processes and turns, persists Jobs, and
returns receipts. It does not become the authority for a coordinator's
Roadmap, Plan, Task, Attempt, Run, Context, Episode, Module, or acceptance
decision.

Version 1.15 adopts the implementation and invariants of Zeroth's Work
Execution module from source revision
`24a6c38afa9c9b0e9cda1c5b2d3fac7dab337d03`, flattened into the standalone
crate while preserving `ah`'s public transport and deployment contract.

## State ownership

| State | Owner | Durable representation |
|---|---|---|
| Roadmap, Plan, Task, Attempt, Run, Context, Episode, Module and acceptance | Upstream coordinator | Supplied to `ah` as an immutable execution binding |
| Job queue, dispatch, cancellation and terminal receipt | Work Coordination / Job store | SQLite Job and append-only Job transitions |
| Agent and provider process lifecycle | Agent Program Control | Session/agent lifecycle ID plus process and tmux evidence |
| Provider turn status | Runtime Observation | Append-only observations fenced by lifecycle and turn |
| Prompt/cancel side effects | Prompt Delivery and guarded actions | Observe-before, dispatch-once, observe-after confirmation |
| Provider credentials and seat home | Provider contracts and Home Materialization | Rebuilt sandbox state; host credential chain remains authoritative |

SQLite is the durable mechanism, not an alternate semantic owner. Provider
adapters append facts through the Runtime Observation intake; they cannot
directly decide Task state or acceptance.

## Identity and handoff invariants

1. A coordinator mints and persists all upstream identities before dispatch.
2. `ah` accepts only the exact version-1 binding field set. Missing, empty, or
   unknown fields fail closed; accepted bindings are canonicalized before
   persistence.
3. A Job's binding cannot change on an idempotent retry. Reusing a request ID
   with different identity or scope is rejected.
4. Every provider process incarnation has a lifecycle ID. Observations from a
   previous incarnation cannot mutate the replacement.
5. Turn evidence is correlated to one Job. Completion for one turn cannot make
   another turn available or terminal.
6. Process status and turn status are independent. A dead process plus legacy
   BUSY evidence is a conflict, not BUSY; a live process does not imply that a
   turn is working.
7. Strong evidence cannot be overwritten by a weaker heuristic. Stale,
   missing, future-dated, or equally strong contradictory evidence is exposed
   as unknown/conflicted.
8. Dispatch and completion are not acceptance. `ah` returns execution facts;
   the upstream coordinator alone evaluates deliverables and Effects.

These rules address the old handoff failure mode: agents no longer pass a
mutable, implicit notion of “current task” through pane text. A durable binding
identifies the exact attempt, and every runtime observation is fenced to the
process and turn that produced it.

## Execution binding schema

`ah ask --binding <path>` and JSON-RPC `job.submit.governance_binding` accept
this exact schema:

```json
{
  "schema_version": 1,
  "roadmap_stream": "delivery",
  "roadmap_node_id": "NODE-1",
  "plan_id": "PLAN-1",
  "plan_revision": "sha256:plan",
  "plan_step_id": "STEP-1",
  "task_id": "TASK-1",
  "attempt_id": "ATTEMPT-1",
  "run_id": "RUN-1",
  "context_id": "CONTEXT-1",
  "episode_id": "EPISODE-1",
  "module_ref": "agent_runtime",
  "capability_refs": ["provider_dispatch"],
  "target_spec_locator": "module_tree/agent_runtime.md",
  "target_spec_revision": "sha256:spec",
  "work_phase": "implementation",
  "physical_scope": ["module_tree/agent_runtime"],
  "semantic_scope": ["agent_runtime.provider_dispatch"],
  "worktree_path": "/workspace/task-1",
  "program_revision": "sha256:program",
  "topology_revision": "sha256:topology"
}
```

Standalone human-guided use does not require a binding. In managed-team mode,
the coordinator injects `GAS_TEAM_BINDING_PATH`; an agent is not allowed to
mint its own request ID or binding, and `ask` must wait for a terminal receipt.
For natural-language agent CLIs, AH also includes the binding in the delivered
context. The `bash` adapter keeps it in durable Job/receipt state only, so
governance JSON can never be interpreted as shell source.

## Compatibility boundary

The refactor intentionally retains:

- binary names `ah` and `ahd`;
- `ah.toml` schema version 1 and existing provider/config semantics;
- existing CLI commands and default human-guided `ah ask` output;
- JSON-RPC over the project Unix socket;
- project discovery, state-directory layout, tmux/systemd ownership, and
  provider credential behavior;
- existing SQLite data through additive, idempotent startup migration.

New fields are additive except for the explicit runtime snapshot schema bump
to version 3. Consumers should select behavior by `schema_version` and treat
unknown fields as forward-compatible.
