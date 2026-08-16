# Upgrading to ah 1.15

Version 1.15 is an in-place upgrade for 1.14 users. The binaries, installer,
`ah.toml` version 1, project discovery, state directory, tmux socket, provider
names, credential layout, and existing commands are unchanged.

## Upgrade

Stop the project cleanly, run the normal installer, then start it again:

```bash
ah stop
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/SevenX77/agent-hypervisor/releases/latest/download/ah-installer.sh | sh
ah start --wait
```

No config conversion or state-directory move is required. On first daemon
startup, `ah` performs an additive SQLite migration:

- adds `sessions.master_provider`;
- adds `agents.lifecycle_id` and assigns a unique lifecycle ID to every
  existing agent row;
- creates `provider_status_observations` and its scope index;
- adds `jobs.governance_binding_json`.

The migration is idempotent, so a restart after interruption safely runs the
same checks again. Existing Jobs, sessions, agents, transcripts, sandboxes,
and provider credentials are retained.

For a conservative operational backup before upgrading:

```bash
ah stop
cp -a "${AH_STATE_DIR:-$HOME/.local/state/ah/<project-id>}" \
  "${AH_STATE_DIR:-$HOME/.local/state/ah/<project-id>}.pre-1.15"
```

Replace `<project-id>` with the resolved directory documented in the README.
Do not copy a live SQLite database while `ahd` is writing it.

## Compatibility notes

- Existing `ah ask`, `ah pend`, and JSON-RPC callers continue to work without
  execution bindings.
- `ah ask --binding <json-file>` is additive. The binding is coordinator-owned
  identity and scope; it is not required for interactive/manual operation.
- `ah pend <job-id> --json` is the new stable machine receipt. Without
  `--json`, successful output remains the reply text used by existing scripts.
- `ah events --format json` now emits snapshot schema version 3. It retains the
  existing lifecycle fields and adds `provider_status`, Job provider/binding,
  and terminal receipt detail. Integrators that reject unknown versions must
  add version 3 before upgrading.
- Legacy agent `state` and `sub_state` remain in snapshots for diagnostics.
  New automation should use `provider_status` and handle `unknown` and
  `conflicted` explicitly.

## Rollback

The schema changes are additive, so 1.14 code does not lose its expected
columns. For a guaranteed rollback, stop `ahd`, reinstall 1.14.3, and restore
the pre-upgrade state-directory backup. Never run two daemon versions against
the same state directory concurrently.
