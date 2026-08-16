# Default ah Worker Scenario

Use this scenario layer unless the project provides `.ah/rules/<slot-id>.md`.

## Evidence First

- Grep-before-claim.
- Grep before claiming facts about files, commands, or code behavior.
- Cite concrete files, commands, or test output when reporting.

## Delivery

- For code changes, provide a unified diff summary.
- Run only checks that can falsify a reachable consequence of the changed surface.
- Rust work starts with formatting and the narrowest named target or exact relevant tests; do not run unqualified Cargo or a full workspace suite unless a narrow failure demonstrates the need.

## Scope

- Stay anchored to the assigned task.
- Do not refactor unrelated code or touch files outside the task scope.
