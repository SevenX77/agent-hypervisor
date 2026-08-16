# Documentation Roadmap

**Goal: this repository must be self-sufficient.** A new user — human or agent — with a clean Linux/WSL box should get from `git clone` to a working provider agent using only what is in this repo: no reading the source of host applications, no asking the maintainer, no reverse-engineering `src/` for credential paths and binary names.

## How these gaps were found

A fresh agent session attempted a first-time Antigravity setup with only this repo available. It succeeded, but needed **four out-of-repo lookups**:

1. Read a host application's source to learn the Windows→WSL credential-bridging pattern for claude/codex.
2. Asked the maintainer how Antigravity auth works (and the first answer turned out to be wrong — the Windows build has no auth file to bridge).
3. Web-searched the official installer command for the Antigravity CLI.
4. Read `src/provider/home_layout.rs` and `src/provider/manifest.rs` to learn the expected credential paths and that the Antigravity binary is `agy`, not `antigravity`.

Each lookup marks a missing document. The items below are ordered by how hard each gap blocks a new user.

---

## P0 — blocks first-time users

### 1. `docs/providers.md` — provider integration matrix

One section per provider (`claude` / `codex` / `antigravity` / `bash`), with fixed fields:

- **Binary name and official install command.** Note: Antigravity's binary is **`agy`**, not `antigravity`.
- **Login procedure, including *where* it must happen.** Antigravity must be logged in **inside WSL/Linux**: the Windows build stores its OAuth token in Windows Credential Manager, so there is no file to symlink; the Linux build writes `~/.gemini/antigravity-cli/antigravity-oauth-token`, which is the path ah reads.
- **Credential path(s) ah reads from the host home.** Today this contract exists only as `SANDBOX_HOME_LINKS` in `src/provider/home_layout.rs`.
- **How the sandbox materializes credentials** (symlink vs 0600 private copy — providers differ).
- **Crash-recovery mechanism** (`--continue`, `resume <id>`, `--conversation <id>`).
- **Security disclosure:** ah launches agents with permission-bypass flags (`--dangerously-skip-permissions`, `--dangerously-bypass-approvals-and-sandbox`). Users must learn this from the docs, not from `ps`.

*Acceptance:* for each provider, a user goes from "CLI not installed" to "`ah ask` answered" following only this page.

### 2. `ah doctor --provider <name>` — executable setup checks

Docs go stale; doctor doesn't. Extend doctor with per-provider readiness checks:

- provider binary on PATH — and **not** a `/mnt/*` Windows binary (this check currently lives in a host application; it belongs in ah itself)
- credential file present at the expected path
- minimum version checks
- on failure, print the exact fix command (install one-liner, "run `agy` once inside WSL to log in", …)

*Acceptance:* every manual check a user would perform from `docs/providers.md` has a corresponding doctor line with a printed remedy.

### 3. `docs/windows.md` — Windows (WSL2) end-to-end guide

The README's WSL section ends at "install and log in to your agent CLIs inside WSL" — which is exactly where the real friction starts. Cover:

- per-provider login inside WSL, including the Windows credential-bridge patterns for claude (symlink `.credentials.json`, link-not-copy so token refresh propagates) and codex (copy `auth.json` + chmod 600), and why Antigravity cannot be bridged at all
- driving ah from the Windows side (`wsl -e ah …`)
- project layout advice: expand the existing `/mnt/c` IO warning into concrete guidance (where to put the workspace, how to sync artifacts back to a Windows checkout)

---

## P1 — blocks integrators

### 4. `docs/integration.md` — the programmatic contract

The README says integrators may "speak JSON-RPC to the Unix socket", but the protocol is undocumented. Cover:

- JSON-RPC method list, params, and request/response examples
- the **`ah ask --wait` stdout contract**: exactly how and where the reply text is returned (it lands in `jobs.reply_text` in SQLite; document the supported CLI retrieval path)
- the job lifecycle state machine (`QUEUED → …`) as a diagram
- the `ah events` snapshot schema (move the detail out of README; README links here)
- the worker-side protocol: injected identity env (`AH_AGENT_ID`, `AH_SESSION_ID`, `AH_ROLE`), and the notify/escalate flow (`ah agent notify`) — when a worker should use it and what the daemon does in response

### 5. `examples/` — real recipes instead of toys

Today `examples/ah.toml` configures three bash agents, and `examples/scenarios/dev-programming` ships with no README. Add commented, runnable examples:

- a multi-provider `ah.toml` (e.g. claude master + antigravity workers)
- a **one-shot fan-out** script: spawn N isolated agents, one `ask` each, collect the N replies, tear down — the pattern for independent sampling / map-reduce over agents
- consuming `ah events --format json` from another process

### 6. Completion/terminality semantics — docs plus code cleanup

`src/completion/parser.rs` hardcodes text heuristics for Antigravity terminality: a reply containing `"5 passed"` is forced Terminal (scenario-specific leakage), and EN/CN word lists ("waiting for…", "等待…") mark a turn as deferred background work. Two tasks:

- move the heuristics into per-provider calibration data (a config or const table with provenance comments), and remove the scenario-specific entries
- document, per provider, how ah decides a turn is complete — and the known false-positive/false-negative modes, so integrators know when a reply can be misclassified

---

## P2 — hygiene

### 7. `docs/concepts.md` — architecture overview

ahd / tmux workspaces / sandbox homes / the state machine, in one page. Define **master** vs **worker** — the README uses both terms throughout without defining either.

### 8. Naming consistency

Repository, runtime, environment, state, service, and error identifiers use the
`Agent Hypervisor` / `agent-hypervisor` / `ah` naming family consistently.

---

## Acceptance gate for the whole roadmap

**The newcomer test.** Give a fresh agent session (Claude / Codex / Antigravity) this repository and a clean WSL, with one task: *"make one Antigravity worker answer one `ah ask`."*

Pass = **zero out-of-repo lookups**. The number of external lookups required is the measurable documentation-quality metric. Baseline (2026-07-09): **4**.
