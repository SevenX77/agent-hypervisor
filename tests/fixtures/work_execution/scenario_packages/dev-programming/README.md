# Dev Programming Scenario

This is an installable scenario layer template for your project's programming
stack: a master PM plus three worker roles across codex, antigravity, and
claude.

ah composes `[embedded kernel] + [bundle layers] + [.ah/rules/<slot>.md or factory default]`
and injects the result into the provider-specific rules file. Bundle layers are
usually empty; this template does not include a bundle. This template supplies
the `.ah/rules/<slot>.md` layer. Do not restate kernel content in slot files.

## Slot mapping

| Slot | Provider | Destination |
| --- | --- | --- |
| `master` | `claude` | `.claude/CLAUDE.md` |
| `a1` | `codex` | `.codex/AGENTS.md` |
| `a2` | `codex` | `.codex/AGENTS.md` |
| `a3` | `antigravity` | `.gemini/AGENTS.md` |
| `a4` | `claude` | `.claude/CLAUDE.md` |

Destinations are relative to each agent's isolated per-sandbox provider home
(each agent gets its own home_root), so identical paths across agents do not
collide.

## Install

1. Install the binaries:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/SevenX77/ah/releases/latest/download/ah-installer.sh | sh`.
2. Copy this directory's `ah.toml` and `.ah/` into your project root.
3. Edit `ah.toml` and each slot file for your provider accounts and project
   rules. The master provider is determined by `[master] cmd` in `ah.toml`, not
   by the master slot file.
4. Start `ahd`, then run `ah up`.
5. Dispatch work with `ah ask <agent_id> "<task>"`.

ah auto-injects builtin skills such as `ah-commands`, `ah-config`,
`ah-runtime-state`, and `ah-operate` into the managed master sandbox.

codex (`a1`/`a2`) is one role running as two interchangeable instances.
