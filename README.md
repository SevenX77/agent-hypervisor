# ah

`ah` is an L2 orchestration daemon and CLI for running multiple AI agent CLIs in isolated tmux-backed workspaces. The daemon (`ahd`) owns state, sessions, workers, recovery, and event streams; the CLI (`ah`) drives it over JSON-RPC on a Unix socket.

Use it when you want one project-level control plane to coordinate provider agents such as Codex, Claude, Antigravity, or an explicit shell provider.

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## Install

One-line install (Linux x86_64, prebuilt binaries — no Rust toolchain needed):

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/SevenX77/ah/releases/latest/download/ah-installer.sh | sh
```

This installs both `ah` and `ahd`. Release archives and checksums are on the [Releases page](https://github.com/SevenX77/ah/releases).

That is all you need — see [Quick Start](#quick-start) below. (Building from source is only relevant if you want to contribute; see [Development](#development).)

### Requirements

| Dependency | Why | Check |
|---|---|---|
| Linux x86_64 — native, or **Windows via WSL2** (see below) | `ah` supervises processes with Linux pidfd and manages lifecycles with systemd | `uname -m` → `x86_64` |
| `tmux` | every agent runs in its own tmux-backed workspace | `tmux -V` |
| systemd with a user session | agent process trees run in systemd user scopes, so crashed daemons never leak orphans | `systemctl --user is-system-running` |
| The provider CLIs you configure | `ah` orchestrates them, it does not bundle them — install and log in to the provider CLIs yourself first | see table below |

Provider names in `ah.toml` do not always match the binary they launch — check the binary, not the provider name:

| Provider name in `ah.toml` | Binary `ah` launches | Verify with |
|---|---|---|
| `claude` | `claude` | `claude --version` |
| `codex` | `codex` | `codex --version` |
| `antigravity` (alias: `gemini`) | **`agy`** | `agy --version` |
| `bash` | `bash` | built-in, nothing to install |

macOS is not supported yet; native support (kqueue-based supervision) is on the roadmap.

### Windows (WSL2)

`ah` runs fully supported inside WSL2 — that is a real Linux environment, so everything above applies unchanged. Setup once:

1. In PowerShell (admin): `wsl --install`, then reboot. This installs WSL2 with Ubuntu.
2. Open Ubuntu and confirm systemd is active: `systemctl --user is-system-running`. On WSL installs from before 2023 it may be off — enable it by adding to `/etc/wsl.conf`:
   ```ini
   [boot]
   systemd=true
   ```
   then run `wsl --shutdown` in PowerShell and reopen Ubuntu.
3. Install tmux if missing: `sudo apt install -y tmux`.
4. Install your agent CLIs (claude/codex/…) **inside WSL** and log in there — agents live in the Linux world, not on the Windows side.
5. Run the one-line `ah` installer above, inside WSL.

Practical notes:

- Keep your projects in the WSL filesystem (e.g. `~/projects/...`), not under `/mnt/c/...` — cross-boundary file IO is much slower.
- Everything `ah` owns (daemon socket, tmux panes, state) lives inside WSL. Drive it from a WSL shell; from the Windows side you can shell through with `wsl -e ah ps` etc.

## Quick Start

Create `ah.toml` in a project directory:

```toml
version = "1"

# Required whenever a seat runs `claude` — see below. The master runs `claude`
# by default, so most configs need this even with no claude agent.
[providers.claude]
shared_credentials_dir = "~/.claude"

[agents.a1]
provider = "codex"

[agents.a2]
provider = "claude"
```

Start the project:

```bash
ah start
```

`ah start` locates `ah.toml`, starts `ahd` if needed, creates a session, and spawns the configured agents. Use `--config <path>` to point at a specific config file and `--wait` to wait until agents are ready:

```bash
ah --config ./ah.toml start --wait
```

`ahd` can also be run directly, but the normal entry point is `ah start` because it performs daemon bootstrap and then drives the project config.

### Choosing the master's provider

The master is a role, not a provider. Point it at whichever agent CLI you want:

```toml
[master]
provider = "codex"
```

`provider` is authoritative. Leave `cmd` out and the master launches with that provider's own command; write both and they must agree, or the config is rejected. With neither set, the master runs `claude`.

What a master gets depends on what its provider declares it supports:

| Capability | What it powers | `claude` | `codex` | `antigravity` | `bash` |
|---|---|---|---|---|---|
| `rules_target` | master rules document (`.ah/rules/master.md` + the built-in kernel) | ✅ | ✅ | ✅ | — |
| `completion_signal` | end-of-turn signal back to `ahd` | ✅ | ✅ | ✅ | — |
| `readiness_ack` | `ah master cutover` readiness, and semantic revive readiness | ✅ | ✅ | ✅ | — |
| `bundles` | `master.bundle` | ✅ | — | — | — |
| `settings` | `[master.settings]` | ✅ | — | — | — |

`ah config validate` warns when the master's provider is missing capabilities and names what stops working. A master that cannot report readiness still starts and still revives — its revive readiness degrades to "the process is running" — but `ah master cutover` refuses, because cutover means the successor accepted the handoff and only the successor can say that. Readiness is never inferred from what the pane displays.

### Claude seats share one login

Every seat that runs `claude` needs `providers.claude.shared_credentials_dir`. That means each `provider = "claude"` agent — **and the master, when it runs Claude, which it does by default**. The value points at your host Claude login directory, normally `~/.claude`:

```toml
[providers.claude]
shared_credentials_dir = "~/.claude"
```

A leading `~` is expanded to the home of whoever runs `ah`, which keeps a committed config portable across machines; anything else must be an absolute path. Either way it has to resolve to a directory that already exists and is not a symlink. Log in once on the host (run `claude`, then `/login`) before `ah start`.

Every sandbox still gets its own `CLAUDE_CONFIG_DIR`, but all of them — and the host — are pointed at this one credential store through `CLAUDE_SECURESTORAGE_CONFIG_DIR`. So a single host login covers every seat without mutual logout, and when a token refreshes it is written back in place instead of leaving the other seats holding a copy that just went stale.

The requirement is fail-closed: a `claude` seat with no configured directory aborts instead of silently falling back to an isolated per-sandbox login. `ah config validate` reports it as:

```text
providers.claude.shared_credentials_dir is required when master or agents use provider claude
```

If nothing in your project runs Claude, set `[master] enabled = false` (or point `master.cmd` at another CLI) and the requirement goes away.

On WSL2 this directory must live on the Linux filesystem. A token refresh rewrites the credential store in place, which needs POSIX ownership and atomic rename — neither of which a Windows drive mounted into WSL provides. `ah` refuses such a directory at spawn time and names the mount it found, so `/mnt/c/Users/you/.claude` fails fast instead of corrupting your login later. The check reads the live mount table, so a Linux directory that merely lives under `/mnt` is fine.

### Everything runs in the background

After `ah start` returns, you will not see any new window or UI — that is expected. The daemon runs as a systemd user service, and the master and every agent run detached inside a dedicated tmux server. Your terminal stays yours; the agents are long-lived and keep working without it.

To open the master's interactive screen and talk to it directly:

```bash
ah attach master
```

Your terminal switches into the live master session (e.g. a running `claude` UI) — type to converse as usual. To leave without stopping anything, press `Ctrl-b` then `d` (tmux detach); the master keeps running in the background.

Workers can be inspected the same way (`ah attach agent a1`), but prefer watching over typing — keystrokes land in the agent's input and can disturb a busy agent. The supported way to interact with workers is `ah ask` from your own shell.

You never have to attach at all: `ah ask` / `ah ps` / `ah watch` / `ah events` / `ah logs` drive and observe everything from the outside.

## CLI

Implemented top-level commands:

```text
ah ping
ah version
ah ps
ah start [--wait]
ah up [--force]
ah ask <agent_id> <text> [--wait] [--request-id <id>]
ah tell master <text> [--session <session_id>] [--request-id <id>]
ah pend <job_id>
ah cancel <job_id>
ah kill <target_id> [--session] [--force]
ah watch <agent_id> [--since-event-id <id>]
ah events --format json
ah logs <agent_id> [--since <id>]
ah attach <target> [subject] [--session <session_id>]
ah stop
ah doctor
ah config validate --config <path>
ah config migrate
ah prompt resolve <agent_id> [--action <value> | --keys <value>] [--save-to-kb]
ah master cutover [--wait] [--print-attach]
ah master ack-ready [--cutover-id <id>]
ah agent notify --agent-id <id> --event <event> [--provider <name>] [--event-id <id>] [--hook-json] [--hook-debug-log <path>] [--socket <path>]
```

Common workflow:

```bash
ah start --wait
ah ask a1 "Inspect the failing test and propose the smallest fix" --wait
ah ps
ah watch a1
ah logs a1
ah attach agent a1
```

## Runtime Events

Use `ah events --format json` when another process needs the authoritative
runtime lifecycle state for a workspace:

```bash
ah --config ./ah.toml events --format json
```

The command writes pure JSON Lines to stdout. Logs and diagnostics go to
stderr. The first line is always a complete snapshot of the current runtime
state; every later line is also a complete snapshot, not a patch. Consumers do
not need to merge deltas or run tmux probes themselves.

`active` is true only when all three runtime facts are true:

1. ahd inventory has an active session for the selected workspace.
2. The corresponding master tmux session and pane are alive.
3. Every non-terminal worker/agent tmux session expected by ahd is alive.

If ahd is not running, `ah events` emits an inactive snapshot with
`ahd_alive: false`, `reason: "daemon_absent"`, then keeps running and retries
the subscription. If ahd later accepts the stream, the next line is the daemon
snapshot. If the daemon stream drops, the CLI emits `reason: "daemon_lost"` and
keeps retrying.

Snapshot schema version 1:

```json
{
  "schema_version": 1,
  "event": "snapshot",
  "sequence": 1,
  "reason": "initial",
  "runtime_state": "active",
  "config_path": "/path/to/ah.toml",
  "workspace_path": "/path/to/project",
  "state_dir": "/home/me/.local/state/ah/project",
  "tmux_socket": "ahd-0b624d24e71a6307",
  "ahd_alive": true,
  "active": true,
  "ahd_has_inventory": true,
  "tmux_server_alive": true,
  "master_tmux_alive": true,
  "worker_tmux_alive": true,
  "worker_tmux_expected_count": 1,
  "sessions": [
    {
      "session_id": "sess_xxx",
      "project_id": "project",
      "path": "/path/to/project",
      "status": "ACTIVE",
      "master_state": "IDLE",
      "master_tmux_session": "master_project",
      "master_tmux_alive": true,
      "master_pane_id": "%1",
      "master_pid": 1234,
      "active_agents": 1
    }
  ],
  "agents": [
    {
      "agent_id": "a1",
      "session_id": "sess_xxx",
      "provider": "codex",
      "state": "IDLE",
      "sub_state": "Matched",
      "pid": 5678,
      "tmux_session": "agent_a1",
      "tmux_alive": true
    }
  ]
}
```

## `ah.toml`

The config schema is defined by `src/cli/config.rs`.

Top-level fields:

| Field | Type | Notes |
|---|---|---|
| `version` | string | Must be `"1"`. |
| `agents` | table | Required. At least one `[agents.<id>]` entry. |
| `master` | table | Optional. Defaults are applied when missing. |
| `providers` | table | Optional container. `[providers.claude]` is required when the master or any agent runs `claude`. |
| `completion` | table | Optional. |
| `daemon` | table | Optional, currently empty. |
| `env` | table of strings | Optional project environment values. |
| `sandbox` | table | Optional sandbox settings. |

Agent fields:

```toml
[agents.a1]
provider = "codex"

[agents.a1.env]
FOO = "bar"
```

| Field | Type | Notes |
|---|---|---|
| `provider` | string | Required. Valid values: `codex`, `claude`, `antigravity`, `bash`. Misspellings are hard errors. |
| `env` | table of strings | Optional extra environment for the agent. |
| `hooks` | table | Optional provider hook config. |
| `plugins` | array of strings | Optional provider plugin names. |

Master fields:

```toml
[master]
enabled = true
cmd = "claude"
readiness_timeout_s = 120
plugins = []
```

| Field | Type | Notes |
|---|---|---|
| `enabled` | bool | Defaults to `true`. |
| `cmd` | string | Defaults to the resolved provider's launch command (`claude` for Claude). Rejected when it names a different provider than `provider` does. |
| `provider` | optional string | Authoritative. Valid values: `codex`, `claude`, `antigravity`, `bash`. When unset, the provider is read from the first word of `cmd`, falling back to `claude`. Drives the master's home layout, rules target, hooks, and readiness — see [Choosing the master's provider](#choosing-the-masters-provider). |
| `readiness_timeout_s` | integer | Defaults to `120`. |
| `hooks` | table | Optional. |
| `plugins` | array of strings | Optional. |

Provider fields:

```toml
[providers.claude]
shared_credentials_dir = "~/.claude"
```

| Field | Type | Notes |
|---|---|---|
| `shared_credentials_dir` | string (path) | Required when the master or any agent runs `claude`. Absolute path (or `~/...`) to an existing, non-symlink directory holding the host Claude login; a directory on a Windows drive mounted into WSL is refused. Injected into every claude seat as `CLAUDE_SECURESTORAGE_CONFIG_DIR`, while `CLAUDE_CONFIG_DIR` stays per-sandbox. See [Claude seats share one login](#claude-seats-share-one-login). |

Completion fields:

```toml
[completion]
hook_push_enabled = false
```

Sandbox fields:

```toml
[sandbox]
additional_ro_binds = ["/opt/tools"]
```

## Editable Agent Rules

ah injects a rules document into each provider home at sandbox preparation time. The source is:

```text
[fixed ah coordination kernel] + [project .ah/rules/<slot-id>.md if present, otherwise built-in default]
```

The slot id is the agent id from `ah.toml`, such as `a1` or `a2`. The master slot id is `master`.

Provider destinations are selected automatically:

| Provider | Injected file |
|---|---|
| `claude` | `.claude/CLAUDE.md` |
| `antigravity` | `.gemini/AGENTS.md` |
| `codex` | `.codex/AGENTS.md` |

Editable examples in this repository:

```text
.ah/rules/master.md
.ah/rules/a1.md
```

Built-in defaults live at:

```text
assets/builtin/defaults/master.md
assets/builtin/defaults/worker.md
```

To customize an agent, create a file matching its slot:

```bash
mkdir -p .ah/rules
$EDITOR .ah/rules/a1.md
```

ah always prepends its fixed coordination kernel, so project rules can focus on scenario-specific behavior.

## Provider Names

Valid provider names are:

```text
codex
claude
antigravity
bash
```

`bash` is a real explicit provider. Unknown provider names such as `claud` or `coddex` fail config validation and do not silently fall back to bash.

## Integration Model

External integrators typically:

1. Write an `ah.toml` with one `[agents.<id>]` table per slot.
2. Add `.ah/rules/<slot-id>.md` files to define scenario-specific behavior.
3. Start the daemon/session with `ah start`.
4. Drive work through the CLI (`ah ask`, `ah pend`, `ah watch`, `ah events`, `ah logs`, `ah ps`, `ah attach`) or by speaking JSON-RPC to the Unix socket used by `ah`.

The daemon stores state in SQLite under the resolved ah state directory and uses tmux for provider panes. The CLI is the supported public control surface for v1.

## Project State & Runtime Data

ah keeps a deliberate split between **project-local config** and **machine-local runtime state**:

| Kind | What | Where | In your repo? |
|---|---|---|---|
| Config | `ah.toml`, `.ah/rules/<slot>.md` | inside your project | Yes — safe to commit and share |
| Runtime state | SQLite db, sandboxes, tmux sockets, pipes, logs, evidence | `~/.local/state/ah/<project-id>/` | No — machine-local, per-checkout |

Runtime state lives **outside** the project tree on purpose: sandboxes hold materialized provider credentials, and the database and sockets are machine-bound — you never want them committed to git or handed to a teammate who clones your repo. (This mirrors `claude`, which keeps session history under `~/.claude/projects/<path>/`, not in your project.)

`<project-id>` is the first 8 hex characters of the SHA-256 of the project's absolute, symlink-resolved path — specifically the directory where `ah.toml` is found by walking up from your current directory.

### Finding a project's runtime data

The most reliable option is to **pin the location yourself** so you always know where it is:

```bash
export AH_STATE_DIR="$HOME/.local/state/ah/my-project"   # or anywhere you like
```

If you did not pin it, print the auto-derived directory for the project you are in:

```bash
# walks up to ah.toml, then hashes its canonical path (on macOS use `shasum -a 256`)
d=$PWD; while [ "$d" != / ] && [ ! -e "$d/ah.toml" ]; do d=$(dirname "$d"); done
echo "$HOME/.local/state/ah/$(printf '%s' "$(cd "$d" && pwd -P)" | sha256sum | cut -c1-8)"
```

Or just look for the most recently active one:

```bash
ls -lt ~/.local/state/ah/
```

Inside that directory you will find `ahd.sqlite` (jobs, sessions, recovery state), `ahd.log`, `sandboxes/`, `pipes/`, and evidence/recovery subdirectories.

### Overriding the location

| Env var | Effect |
|---|---|
| `AH_STATE_DIR` | Use this exact path as the state root (highest priority). Set `AH_STATE_DIR="$PWD/.ah-state"` to keep state inside the project. |
| `XDG_STATE_HOME` | Use `$XDG_STATE_HOME/ccbd` as the root instead of `~/.local/state/ah`. |

With no `ah.toml` found by walking up and no override set, ah falls back to `~/.local/state/ah/default/`.

## Development

Only needed if you are working on ah itself (end users should use the
prebuilt installer above). With a Rust toolchain:

```bash
cargo build --release   # binaries land in target/release/{ah,ahd}
cargo test              # full test suite
```

Or install straight from source:

```bash
cargo install --git https://github.com/SevenX77/ah --bin ah --bin ahd
```

## License

MIT — see [LICENSE](LICENSE).
