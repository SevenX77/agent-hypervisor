---
name: ah-config
description: Use when you need to understand or edit ah project configuration, scenario rules, project skills, bundles, provider landing files, or Claude CLI settings materialized into ah-managed homes.
---

# ah configuration surface

ah project configuration starts at `ah.toml`. The loader searches upward from the current directory, unless `AH_CONFIG_PATH` points at a specific config file. The top-level TOML fields are `version`, `master`, `completion`, `daemon`, `env`, `sandbox`, and `agents`.

## Project layout

- `ah.toml` is the project configuration file.
- `.ah/rules/<slot>.md` is an optional slot-specific rules override, for example `.ah/rules/master.md` or `.ah/rules/a1.md`.
- `.ah/skills/<name>/SKILL.md` is a project skill. A skill reference must be a single directory name under `.ah/skills`.
- `.ah/bundles/<name>/bundle.toml` is a bundle manifest. Bundle names are single directory names under `.ah/bundles`.

## `ah.toml` fields

Top level:

- `version: String`
- `[master]`
- `[completion]`
- `[daemon]`
- `env: table`
- `[sandbox]`
- `[agents.<id>]`

`[master]` fields:

- `cmd: String`
- `provider: Option<String>`
- `readiness_timeout_s: u64`
- `enabled: bool`
- `window_size`
- `hooks`
- `plugins`
- `skills`
- `bundle`
- `settings`

`[agents.<id>]` fields:

- `provider: String`
- `env`
- `hooks`
- `plugins`
- `skills`
- `bundle`
- `settings`

`[completion]` currently has `hook_push_enabled`. `[daemon]` is currently empty. `[sandbox]` currently has `additional_ro_binds`.

Provider `settings` are accepted only for Claude provider entries today. Master settings are allowed when `[master].provider` is unset because the master defaults to Claude; non-Claude providers with non-empty `settings` are rejected during config validation, because only the Claude provider applies it today.

There is no direct `[master].mcp` or `[agents.<id>].mcp` field in `ah.toml`. MCP configuration is carried by `ExtensionConfig` and bundle contributions, not by those direct config structs.

## Rules composition

ah writes provider rules from:

1. role kernel
2. bundle rule layers
3. slot override, or the role default when no `.ah/rules/<slot>.md` exists

The joiner between those sections is exactly a horizontal separator made from blank lines around `---`.

Provider rule landing files:

- Claude: `.claude/CLAUDE.md`
- Codex: `.codex/AGENTS.md`
- Antigravity: `.gemini/AGENTS.md`

The role kernel and role defaults are embedded in the ah binary with `include_str!`, so editing those files changes a future build, not a running binary. Current master-role rules are only materialized for provider `claude`; non-Claude master rules return without writing. Worker rules are wired for all three providers.

## Provider CLI configuration

Claude CLI settings are materialized into the ah-managed sandbox home at `.claude/settings.json`. The path is owned by the Claude home layout, and `materialize_claude_settings` creates or updates that JSON file while preserving existing keys it does not manage.

Today that materializer writes ah-owned defaults such as `skipDangerousModePermissionPrompt`, `permissions.defaultMode`, hooks, and enabled plugins. Model, `statusLine`, and `autoCompact` are provider CLI settings that belong in this same settings file when ah config carries them. Merge settings; do not blindly replace the user's existing JSON object.
