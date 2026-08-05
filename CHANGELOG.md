# Changelog

All notable changes to `ah` are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.14.0] - 2026-08-05

### Added
- The WSL browser bridge is part of the product (decision 0006 D2b). The login
  doorman launches provider sign-in flows that open a browser, and inside WSL a
  Linux process cannot pop the Windows browser without a bridge — so on the
  machine this was built on, the bridge had been installed by hand and every
  other machine would fall back to "click the printed URL yourself".
  `ah doctor` now checks it as `wsl:browser-bridge` (a Warn when missing:
  sign-in still works, just without the pop), and `ah setup --fix` installs a
  zero-dependency opener at `/usr/local/bin/xdg-open` that hands URLs to the
  Windows default browser — the same mechanism `wslview` uses, without the
  package. The doorman also points `$BROWSER` at the opener for CLIs that
  honour it.

## [1.13.1] - 2026-08-05

### Fixed
- codex seats inherit the host's model choice. The sandbox `config.toml` is
  generated, so a seat ran whatever model codex defaults to rather than the one
  the operator pinned in `~/.codex/config.toml` — which bit for real when only
  one model had quota left: the host CLI worked while every ah seat spawned on
  the default model, hit the usage limit and stalled on an interactive
  rate-limit prompt. `model` and `model_reasoning_effort` now carry over at
  materialization; a value already present in the seat's own config stays
  authoritative, so bundle or operator overrides inside the sandbox still win.
  First slice of #6 (per-agent CLI settings for codex/antigravity).

## [1.13.0] - 2026-08-05

### Added
- ah is now the login doorman (decision 0006). One OAuth token chain has one
  active environment — an environment being a home directory on one OS: the
  Windows profile, the WSL distro and a macOS home are three. Refresh tokens
  rotate on every use, so a chain shared between two environments (a copied
  `auth.json`, a symlink across the WSL boundary) dies on whichever side
  refreshes less — measured on a real machine, where the WSL codex login was
  rotated away by the Windows side, and where the WSL claude store turned out
  to be a symlink into the Windows profile: an unlit fuse for the same
  failure. `ah start` now checks every provider the project uses before
  spawning seats: in an interactive terminal a missing login launches the
  provider's own sign-in flow right there (`codex login`,
  `claude auth login`) and start continues once it succeeds; anywhere else
  the error carries a pasteable remedy. A store reaching across the
  environment boundary is refused with removal instructions — never silently
  deleted, and never "fixed" by logging in over it. `ah doctor` diagnoses
  each provider's store with the same checks and prints the remedy (#4).
  The checks are specified per OS: on Linux every provider keeps a regular
  file; on macOS claude lives in the Keychain, so the spec marks it
  probe-only instead of pretending a file check applies. Expiry is judged
  with restraint: an expired access token is not a dead login — the refresh
  token renews it — so only absence, an explicit logout stub, an unparseable
  file or a boundary-crossing store ask for a sign-in.

## [1.12.0] - 2026-08-05

### Fixed
- Every command now answers "which project's stack am I talking to" the same
  way, modeled on git/cargo project discovery (decision 0005). An explicit
  `--config` is made absolute against the working directory and must exist —
  `--config ah.toml` used to hash the empty string, sending every project
  invoked that way to one shared state dir. Without `--config`, the CLI walks
  up from the current directory to find `ah.toml`, which the README always
  claimed and the code never did: instead every such command silently used
  `~/.local/state/ah/default`, where unrelated projects shared one database —
  observed as a brand-new project failing `ah start` with
  `AGENT_ALREADY_EXISTS` because another project's agent id was already there.
  When no `ah.toml` exists above the working directory, project-scoped commands
  now fail with an error naming the directory and the fix, the way git reports
  "not a git repository"; `ah reclaim`, `ah version`, `ah setup`,
  `ah config validate` and `ah bundle` still run anywhere. Environment
  overrides (`AH_STATE_DIR`, `XDG_STATE_HOME`, `CCB_SOCKET`) keep their
  priority, and `CCB_CONFIG_PATH` is honoured everywhere, not only by
  `ah events` (#43, #46, #15).

  **Migration:** stacks that lived in `~/.local/state/ah/default` or
  `~/.local/state/ah/e3b0c442` are not moved or deleted; commands simply stop
  resolving there. After upgrading, restart your projects (`ah start` inside
  each) and collect the dead directories with `ah reclaim`.

## [1.11.0] - 2026-08-05

### Added
- `ah reclaim` collects what crashes leave behind. Bounded retention and
  archive-before-destroy only run on the normal path; a crash, a power cut or a
  `kill -9` skips them and strands a sandbox home, a tmux socket, a systemd unit
  and a state directory with no owner — on one machine, 812 of 978 sandbox homes
  belonged to stacks that no longer existed. `ah reclaim` reports what it can
  collect and exits; `--yes` removes it. It never touches anything a running
  daemon still owns, never touches a project's `.ah/sessions/`, and defaults to
  ignoring anything younger than seven days, so a stack that died an hour ago is
  still there to investigate. A sandbox holding session records that belong to no
  known project is kept and reported rather than deleted; `--archive-to <dir>`
  names a destination for those records and reclaims the home once they are
  safely out.

## [1.10.0] - 2026-08-05

### Added
- Session records are handed to the project before a sandbox is destroyed. A
  provider writes its transcripts inside the sandbox home — codex rollouts,
  claude project records, antigravity conversations — and every path that
  destroyed a sandbox deleted them with it, so closing a window lost a real
  development session for good while a crash happened to keep it. Destruction
  now copies each provider's record set to
  `<project>/.ah/sessions/<session>/<agent>/<provider>/` first, and a sandbox
  whose records cannot be archived is left on disk rather than deleted: a
  sandbox is recoverable, a session is not. The archive directory carries its
  own `.gitignore`, so it never shows up in the project's `git status`, and
  symlinks are never followed, so the shared credential store cannot be copied
  into the project. Sandboxes created before this release carry no project
  marker and are still destroyed, with a warning (#27).

## [1.9.0] - 2026-08-04

### Added
- The state database stays bounded. Nothing removed old rows and nothing
  returned freed pages, so a database holding kilobytes of live state reached
  gigabytes on disk. Retention is graded by what a row is for rather than by age
  alone — pane-output events are capped tightly while state changes, evidence
  and failures are kept far longer, so a retention pass cannot delete the record
  of why something failed. Deleted space is actually reclaimed: new databases use
  incremental auto-vacuum, an existing one is compacted when its waste is large
  in both share and bytes, and the write-ahead log is truncated so the space does
  not simply move there. The pass runs at daemon start and every 30 minutes. On a
  real 1.9 GB database this returned 1.8 GB in 2.3 seconds (#23).

## [1.8.3] - 2026-08-04

### Fixed
- `ah stop` removes the systemd user unit it created. `ah start` writes and
  enables a per-stack `ah-<hash>.service`; stop only stopped the process, so the
  unit stayed enabled with its `default.target.wants/` symlink and relaunched a
  stack the operator had shut down at the next login — one orphan per
  start/stop cycle. Stop now disables the unit, removes the file and reloads
  systemd, keyed on ah's own `AH_STATE_DIR` marker so a unit ah did not generate
  is never touched (#24).

## [1.8.2] - 2026-08-04

### Added
- `[master.env]`, and the project `[env]`, now reach the master seat. The master
  was the only seat with no environment channel, so a host with per-project
  variables to inject had to wrap `cmd` in a shell — which also exposed secrets
  in the process table. Author-configured values are layered project-then-seat
  and ah's own runtime variables are applied last, so a project cannot redirect
  the seat's identity, state directory or daemon socket. Carried on spawn,
  realign, cutover, and restored from the project config on revive; a change to
  it moves the master's config fingerprint, so `ah up` notices (#37).

### Fixed
- A shell in the master `cmd` no longer collides with a declared `provider`.
  1.8.0's conflict rule read `bash -c '… exec claude …'` as the bash provider
  and rejected the config, which broke every host that used a wrapper to inject
  environment — the only shape available before `[master.env]` existed. A shell
  is a launcher, so the rule now fires only when the command names a different
  agent CLI (#37).

## [1.8.1] - 2026-08-04

### Fixed
- The published installer downloads from this repository. `Cargo.toml` still
  named the pre-rename repository, so the generated `ah-installer.sh` requested
  `SevenX77/ccbd-rust` and every one-line install failed with a 404 — the entry
  point in the README has been broken since the rename, in 1.7.0 as well. The
  release archive itself was always correct; only the installer's URL was wrong.

## [1.8.0] - 2026-08-04

The provider-parity release: what used to be true only for Claude is now true
for every provider. The master seat runs whichever provider declares the
capabilities the role needs, one host login is shared by every seat of every
provider, and the network configuration an agent needs to reach its provider
travels with it. Readiness stops being guessed from terminal output anywhere in
the master lifecycle, and the guard rails the previous releases specified but
never shipped are now enforced in CI.

### Fixed
- Agents inherit proxy settings (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`,
  `NO_PROXY` and their lowercase forms). Without them a sandboxed agent could
  not reach its provider on any machine that routes through a proxy, and the
  provider CLIs report that as "not signed in" rather than as a network failure
  — on the machine this was found on, every project had been hand-copying the
  same six variables into `[env]` to work around it.
- Antigravity credentials are shared with the host store instead of copied per
  sandbox. A copy went stale as soon as any seat refreshed its token: the new
  token stayed in that one sandbox while the host and every other seat kept a
  credential that a rotation elsewhere could invalidate. All providers now share
  one file, so a refresh by any seat is a refresh for all of them, and sandboxes
  left over from the copying era are migrated on their next materialization.
- Shell scripts and other text files are pinned to LF via `.gitattributes`. A
  Windows checkout rewrote them to CRLF, which broke the CI gate script under
  bash with `set: pipefail: invalid option name` — in WSL the same working tree
  is both the Windows checkout and the Linux runtime.

### Added
- `providers.claude.shared_credentials_dir` accepts a leading `~`, expanded at
  config load to the home of whoever runs `ah`. A committed `ah.toml` no longer
  has to hardcode one machine's absolute path to stay valid. Without a resolvable
  home the path is left alone and rejected as non-absolute rather than guessed at.
- The shared credentials directory is refused when it sits on a Windows drive
  mounted into WSL. A token refresh rewrites that store in place, which needs
  POSIX ownership and atomic rename; the check reads `/proc/self/mounts` and
  reports the mount point and filesystem it found, so a Linux directory that
  merely lives under `/mnt` is unaffected.
- `scripts/ci/check_state_write_gate.sh`, the CI grep rule the perception write
  arbiter has specified since 1.6.0 but never shipped: `agents.state` may only be
  written by `db/perception/gate.rs`. Pre-migration call sites are baselined as a
  ratchet — new or grown direct writes fail, and a shrunk baseline fails until it
  is lowered, so migration progress is recorded instead of silently stalling. Now
  a CI step, and the two acceptance tests that assert it exists finally pass.
- The master seat runs any provider that declares the capabilities the role
  needs, instead of being wired to Claude. Provider manifests now declare
  `rules_target`, `completion_signal`, `readiness_ack`, `bundles` and
  `settings`; master materialization, revive and config validation gate on those
  declarations rather than on provider names, so `[master] provider = "codex"`
  (or `antigravity`) gets that provider's home layout, its rules document
  carrying the master kernel, its hooks, and no Claude credentials. Adding a
  provider means declaring what it supports, not editing branches. See
  `docs/decisions/0001-master-provider-capability-negotiation.md`.
- `master.provider` is the authoritative field. `master.cmd` defaults to the
  resolved provider's launch command, and a `cmd` that names a different
  provider than `provider` is rejected instead of silently winning.
- `ah config validate` warns, naming each missing capability and what it costs,
  when the master's provider cannot carry the full role.

### Changed
- Master readiness is only ever reported by the master itself. The pane-text
  readiness probe is deleted from both the cutover and revive paths: cutover
  waits for `ah master ack-ready` and refuses providers without the
  `readiness_ack` capability, while revive waits for transcript progress and
  otherwise degrades to an explicitly labelled `started` mode. A settled pane
  says the screen stopped changing, not that the master is ready — this closes
  the last route by which pane text re-entered lifecycle state after 1.5.0.

### Fixed
- CLI errors from the daemon render the daemon's explanation, not just the error
  code: `ENVIRONMENT_NOT_SUPPORTED` alone said a guard fired without saying which
  one or what to change.
- Config and docs caught up with the shared-credentials requirement shipped in
  1.7.0. The project's own `ah.toml` and the `examples/ah.toml` template now
  declare `[providers.claude] shared_credentials_dir`; both previously failed
  `ah config validate` with `providers.claude.shared_credentials_dir is
  required when master or agents use provider claude`, because an enabled
  master runs `claude` by default. README documents the key, the single shared
  login store behind it, and the fail-closed validation.

## [1.7.0] - 2026-07-13

The shared-credentials and modular-decoupling release. Multiple agent
sandboxes now ride a single interactive login instead of each cloning
credential files that refresh out from under one another, and the daemon's
largest control-plane files are split along ownership lines so the
master-revival/reap saga, the RPC session handlers, and IO perception each
sit behind a narrow module boundary.

### Added
- Shared secure-storage credentials for `claude` seats: each seat is injected
  with `CLAUDE_SECURESTORAGE_CONFIG_DIR` pointing at one shared credentials
  store while `CLAUDE_CONFIG_DIR` stays per-sandbox, so a single host login is
  shared across every worker sandbox and the host without mutual logout, and a
  token refresh writes back in place rather than orphaning the other seats.
  Configuration is fail-closed: a `claude` seat without a configured shared
  credentials directory aborts rather than silently falling back to an
  isolated login (#151).

### Changed
- Control-plane decomposition (behavior-preserving). The master-revival saga's
  execution chain — spawn replacement, failed-revive reap, worker
  reprovision, redispatch marker, confirm timer — moves out of `master_watch`
  into a dedicated `master_reaper` module behind a single reap entry point, so
  every failed-revive exit routes through exactly one reaper and is pinned by
  the existing failure-class tests plus a new finalize-stale reap test
  (#156, #158). The master-cutover RPC handlers split out of the sessions
  handler (#155), and the agent IO reader is passivated with perception moving
  to its own marker stream (#153). No behavior change to the revival, cutover,
  or perception contracts.

### Fixed
- Gateway `ah_bin` resolution goes through the shared resolver so the bridge
  invokes the correct sibling `ah` binary instead of the daemon path (#149).

## [1.6.0] - 2026-07-12

The control-plane arbitration release: agent state and job status stop being
written from scattered call sites and move behind explicit single-writer
gates, while daemon-to-agent events gain durable, exactly-once delivery that
survives a restart. This is the structural follow-through on the
perception-reliability work in 1.5.0 — where 1.5.0 removed pane-text guessing,
1.6.0 makes the authoritative writers the *only* writers. The incident classes
driving each fix remain documented in `logs/operator-observation-log.md`,
which ships with this release.

### Added
- Perception write arbiter: a single sanctioned entry point for agent-state
  writes, an event channel, and a CI grep gate that keeps stray writers out
  of the lifecycle path (perception-arbiter Phase 1, #136), with the
  job-state test fixture baselined into the checker (#138).
- Job state-machine gate + timeout takeover: previously scattered job-status
  writes are migrated behind an explicit state machine, with a
  timeout-takeover path for stalled transitions (control-plane-refactor
  Phase 1, #137).
- Durable event delivery: journal-first outbox writes plus cold-scan replay /
  reap / dead-letter / ordering on daemon startup and a transport dedup
  ledger, so hook and notify events are delivered exactly once across a
  daemon restart (#142).

### Fixed
- Respawn-storm hardening (issue #13): the config fingerprint is normalized
  over bare declared env so unchanged agents no longer read as drifted,
  consecutive destructive respawns are staggered, and the agent-env
  server-side-merge wire format is pinned by test (#141).
- antigravity Stop-hook delivery: the injected hook now resolves a PATH-safe
  `ah` binary and uses the correct seconds-unit hook timeout, so end-of-turn
  completion signals actually fire (#143).

## [1.5.0] - 2026-07-10

The perception-reliability release: terminal-text guessing is removed from the
agent lifecycle root-and-branch — pane scanning becomes alert-only and can
never invent agent state — while completion detection moves to explicit,
authoritative signals (provider transcripts, hooks, a reworked completion
state machine). The incident classes driving each fix are documented in
`logs/operator-observation-log.md`, which ships with this release.

### Added
- antigravity pending-task detector: yield-and-wait turns (harness-internal
  background tasks) no longer produce false completions (#122), with the
  authoritative transcript signal wired via the agent log root and the
  `'5 passed'` escape hatch removed (#123).
- Lifecycle watchdogs: QUEUED-starvation alerting and PROMPT_PENDING
  suppression escalation (#125).
- Process/environment hygiene for spawned agents: identity injection,
  tmux test-leak isolation, and teardown-escape fixes (module B, #130).

### Fixed
- **Pane poison inferers deleted (P0-1)**: pane text can no longer be
  promoted into completion or lifecycle state anywhere — alert-only (#127);
  the unknown→park inference is likewise deleted, parking now happens only
  via a known-dialog whitelist (#126).
- Circuit-breaker recovery three-layer hole and claim-time cancel check
  (P0-2): cancelling a queued job now lands cleanly instead of desyncing the
  agent queue (#128).
- Completion state-machine domain: stuck-reason parameterization (the stall
  reason now names the layer that actually detected it) and recapture
  dead-code removal (module A, #129).
- Inherited identity environment variables are scrubbed at the spawn command
  boundary and on the master-revive fallback path (#120, #121).
- `[sandbox] additional_ro_binds` is now rejected at config validation with
  a clear error — the option translated to a service-only systemd property
  that crashed every agent at spawn (#131).
- Windows msvc check unbroken by gating a unix-only test (#132); the
  orphan-session reap test is de-flaked under the parallel harness (#133).
- Pane fixtures relocated into the test tree and desensitized (#124).

## [1.4.0] - 2026-07-09

The state-contract release: a verified, spoof-resistant contract between the
daemon's database, the runtime, and every process it spawns. All six contract
surfaces were end-to-end verified in isolation before release.

### Added
- State snapshot schema v2 with automatic migration of existing state
  databases (#112).
- `CLOSED` session lifecycle state with explicit close semantics (#113) and
  job-state emission for consumers (#114).
- `ah status --json` one-shot machine-readable snapshot; `ah ps` gains a
  status column and `--all` (#115).
- Bare-start guard: `ah start` validates project configuration before
  launching the daemon, so an unconfigured directory errors out instead of
  polluting state (#117).
- Agent identity environment: every ah-spawned process now carries
  `AH_AGENT_ID`, `AH_SESSION_ID`, and `AH_ROLE` (`worker`/`master`), injected
  at all spawn/respawn loci through one shared helper; caller-supplied
  identity values are overwritten (spoof-resistant) (#118).
- `ah`-commands builtin skill and self-knowledge skills for masters
  (#108, #109).
- dev-programming scenario template with fidelity tests (#107).
- Kill-path ownership guard (#110).

### Fixed
- Orphan-scope reconcile is anchored to the daemon's own marker: scopes
  carrying a foreign marker are never touched, and a daemon whose identity
  came from ambient environment refuses stop-capable operations entirely
  (#117).
- `BindsTo`/`PartOf` unit dependencies are only emitted when the declared
  daemon unit is verified active, fixing agent spawn on non-systemd/bare
  starts (#117).
- State-directory resolution follows the documented priority contract
  (`AH_STATE_DIR` > `CCBD_STATE_DIR` > `XDG_STATE_HOME` > explicit config >
  dev mode > project discovery) (#117).
- `ahd --version`/`--help` answer without starting a daemon; RPC EOF errors
  are diagnosable (#106).
- Test de-flakes: cancel-request notification and completion-dispatch tests
  (#111, #116).

## [1.3.4] - 2026-07-06

### Added
- `ah events` runtime snapshots now include a `starting` runtime_state for the
  cold-start window before master/worker tmux runtime has been recorded.
  Consumers such as Studio should clean up only `degraded` runtimes; `starting`
  means startup is still in progress and must be left alone.

### Fixed
- Claude workers spawned into an ah sandbox HOME with
  `--dangerously-skip-permissions` now receive `IS_SANDBOX=1` directly from the
  daemon's provider spawn path, so sandbox identity no longer depends on the
  harness config template carrying a duplicate `[env] IS_SANDBOX` entry.

## [1.3.3] - 2026-07-06

### Fixed
- `ah events` no longer exits when the daemon closes the subscription stream
  (`ah stop` or a daemon restart). It now emits a local inactive snapshot so
  consumers see the runtime go down, then keeps reconnecting — a GUI
  supervisor would otherwise freeze on the last active snapshot. The local
  fingerprint resets after a live connection so the down-edge is never
  dedup-suppressed, while pure connect-failure loops stay quiet.

## [1.3.2] - 2026-07-06

### Added
- `CLAUDE_CODE_OAUTH_TOKEN` joined the daemon env passthrough whitelist, so a
  host launcher can hand a long-lived `claude setup-token` credential to the
  daemon and every master/worker it spawns inherits it — without persisting
  the token into config files, the sqlite inventory, or spawn-cmd logs.

### Fixed
- `ah events` no longer filters runtime inventory by the config file's parent
  directory. Sessions record the project's absolute path (the `ah start`
  cwd), while the config may live elsewhere (Studio keeps transient configs
  under the OS temp dir), so the filter matched nothing and every snapshot
  reported an inactive runtime even while master and workers were alive.
  The daemon's state dir is already scoped to the config, so the
  subscription reports that daemon's full inventory.

## [1.3.1] - 2026-07-06

### Added
- `ah events --format json`, a stable runtime lifecycle event source for
  GUI and service integrations. The command writes an initial full snapshot,
  then full JSONL snapshots whenever ahd inventory, master tmux, or worker
  tmux state changes.
- Runtime snapshot schema v1 with ahd inventory, tmux socket/server health,
  master liveness, worker liveness, session summaries, and agent summaries.

### Changed
- Runtime state changes are now broadcast from daemon-owned paths: session
  inventory, master runtime, worker lifecycle, recovery, and state machine
  transitions. Clients can subscribe instead of polling `ah ps` or probing
  tmux directly.
- If ahd is absent, `ah events` emits an inactive snapshot and keeps retrying
  the daemon stream.

## [1.3.0] — 2026-07-05

### Added
- `ah tell master "<text>"` — an async command for the operator to send an
  instruction to the master agent. It delivers into the master's pane and
  returns immediately without blocking on the master's turn. Master
  observability is now first-class: a `UserPromptSubmit` hook flips
  `master_state` to `BUSY` (a real "started working" signal, not merely
  "delivered") and a `Stop` hook flips it back to `IDLE`; both events are
  written to the daemon log and `master_state` is surfaced by `ah ps`.
- Studio provisioning for Windows/WSL2 — PowerShell provisioning that
  enables WSL2, installs the distro, runs an in-distro `ah` install and
  first-launch checks, with idempotent re-runs and bare-invocation resume.
- Configurable installer landing directory via `AH_INSTALL_DIR`.
- Opt-in tmux "follow terminal" sizing.
- Windows compile scaffolding (M0) and a ConPTY spike. Foundational only —
  the runtime still targets Linux and Windows-via-WSL2; native Windows is
  not yet shipped.

### Fixed
- Dispatch-ACK race that could leave a job marked DISPATCHED while its
  prompt was never delivered, then later misjudged as STUCK.
- Health-check false-positive STUCK for tasks that were long-running but
  still alive.
- Studio handoff: the default master command is now plain `claude`, and
  no-config socket resolution is isolated to avoid ambient cwd state.

## [1.2.0] — 2026-07-02

### Added
- Plugin bundle system completed across providers — antigravity bundle
  adaptation plus the bundle CLI and bundle-aware realign/recovery, so a
  project's skills/hooks/plugins are materialized into each provider's
  native layout on spawn and re-aligned on `ah up`.

### Fixed
- Antigravity premature completion — a deferred background-work gate now
  prevents a worker from being reported COMPLETE before its real work
  (including post-response background tasks) has actually finished.

### Changed
- `agent.notify` Stop-hook receipts are now logged (both receive and
  outcome), so daemon logs show whether a provider's completion push
  actually fired — previously an invisible blind spot during incidents.

## [1.1.0] — 2026-07-02

### Added
- Plugin/skill bundle foundation — agent skills injected from `ah.toml`,
  the Claude plugin-bundle spine, cross-provider MCP translation, and
  Codex bundle adaptation.
- macOS groundwork — a platform abstraction layer (OS-specific behavior
  moved behind traits) and a kqueue-based process watcher. Release binaries
  remain Linux-only; native macOS support is on the roadmap.
- Windows (WSL2) onboarding preflight checks.
- README — Requirements table and a full Windows (WSL2) setup guide.

### Fixed
- Completion-detection fallbacks hardened.
- A revived master now resolves its Claude config directory correctly.

## [1.0.0] — 2026-07-01

First public release. `ah` is a Linux-native L2 orchestration daemon
(`ahd`) and CLI (`ah`) for running multiple AI agent CLIs — Codex, Claude,
Antigravity, or an explicit shell provider — in isolated tmux-backed
workspaces. The daemon owns state, sessions, workers, recovery, and event
streams; the CLI drives it over JSON-RPC on a Unix socket.

[1.3.1]: https://github.com/SevenX77/ah/releases/tag/v1.3.1
[1.3.0]: https://github.com/SevenX77/ah/releases/tag/v1.3.0
[1.2.0]: https://github.com/SevenX77/ah/releases/tag/v1.2.0
[1.1.0]: https://github.com/SevenX77/ah/releases/tag/v1.1.0
[1.0.0]: https://github.com/SevenX77/ah/releases/tag/v1.0.0
