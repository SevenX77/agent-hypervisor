use crate::error::AhError;
use crate::provider::builtin::{self, BuiltinSkillScope};
use crate::provider::extensions::{
    ExtensionConfig, HookGroup, HookItem, McpServerConfig, McpTransport,
};
use crate::provider::plugins::{ResolvedPlugin, resolve_plugins_for_provider};
use crate::provider::skills::{
    ResolvedSkill, parse_skill_refs, plan_claude_skill_materialization,
    plan_codex_skill_materialization, resolve_project_skills,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use toml::Value as TomlValue;

const WHITELIST: &[&str] = &[".ssh", ".gitconfig", ".git-credentials", ".netrc"];
const PROVIDER_AUTH_WHITELIST: &[&str] = &[
    ".codex/auth.json",
    ".codex/installation_id",
    ".gemini/antigravity-cli/antigravity-oauth-token",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeOverrides {
    pub home_root: PathBuf,
    pub extra_env: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookPushContext {
    pub agent_id: String,
    pub provider: String,
    pub ahd_socket_path: PathBuf,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeLayoutRole {
    Master,
    Worker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMaterializationErrorCode {
    AuthProviderTokenMissing,
    AuthSandboxMountFail,
}

pub fn materialize_auth_file_with_ladder(
    source_home: &Path,
    home_root: &Path,
    relative_path: &str,
) -> Result<(), AuthMaterializationErrorCode> {
    if !PROVIDER_AUTH_WHITELIST.contains(&relative_path) {
        tracing::warn!(relative_path, "provider auth path is not whitelisted");
        return Err(AuthMaterializationErrorCode::AuthSandboxMountFail);
    }
    let source = source_home.join(relative_path);
    if !source.is_file() {
        tracing::warn!(
            source = %source.display(),
            "provider auth source file missing"
        );
        return Err(AuthMaterializationErrorCode::AuthProviderTokenMissing);
    }
    let target = home_root.join(relative_path);
    // Every seat points at the host's single credential file. A provider that
    // refreshes its token rewrites that one file in place, so the new token is
    // immediately shared by the host and every other seat — the property a
    // per-sandbox copy cannot have, where each seat silently drifts onto its own
    // token and a rotation elsewhere strands it.
    match symlink_auth_file_checked(&source, &target) {
        Ok(()) => Ok(()),
        Err(err) => {
            tracing::warn!(
                source = %source.display(),
                target = %target.display(),
                error = %err,
                "provider auth symlink failed; falling back to copy"
            );
            copy_auth_file(&source, &target, false)
        }
    }
}

pub fn prepare_home_layout(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
) -> Result<HomeOverrides, AhError> {
    prepare_home_layout_with_role(
        provider,
        sandbox_dir,
        workspace_path,
        HomeLayoutRole::Worker,
    )
}

pub fn prepare_home_layout_with_claude_credentials(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
    claude_shared_credentials_dir: Option<&Path>,
) -> Result<HomeOverrides, AhError> {
    prepare_home_layout_with_role_and_claude_credentials(
        provider,
        sandbox_dir,
        workspace_path,
        HomeLayoutRole::Worker,
        claude_shared_credentials_dir,
    )
}

pub fn prepare_home_layout_with_role(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
    role: HomeLayoutRole,
) -> Result<HomeOverrides, AhError> {
    prepare_home_layout_with_extensions(
        provider,
        sandbox_dir,
        workspace_path,
        role,
        &ExtensionConfig::default(),
        None,
    )
}

pub fn prepare_home_layout_with_role_and_claude_credentials(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
    role: HomeLayoutRole,
    claude_shared_credentials_dir: Option<&Path>,
) -> Result<HomeOverrides, AhError> {
    prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        provider,
        sandbox_dir,
        workspace_path,
        role,
        default_rules_slot_id(role),
        &ExtensionConfig::default(),
        None,
        claude_shared_credentials_dir,
    )
}

pub fn prepare_home_layout_with_extensions(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
    role: HomeLayoutRole,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
) -> Result<HomeOverrides, AhError> {
    prepare_home_layout_with_extensions_for_slot(
        provider,
        sandbox_dir,
        workspace_path,
        role,
        default_rules_slot_id(role),
        extensions,
        hook_push_ctx,
    )
}

pub fn prepare_home_layout_with_extensions_for_slot(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
    role: HomeLayoutRole,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
) -> Result<HomeOverrides, AhError> {
    prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        provider,
        sandbox_dir,
        workspace_path,
        role,
        slot_id,
        extensions,
        hook_push_ctx,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
    provider: &str,
    sandbox_dir: &Path,
    workspace_path: &Path,
    role: HomeLayoutRole,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
    claude_shared_credentials_dir: Option<&Path>,
) -> Result<HomeOverrides, AhError> {
    let source_home = materialization_source_home()?;
    let home_root = sandbox_home_for_sandbox_dir(sandbox_dir)?;
    let workspace_key = workspace_trust_key(workspace_path);
    fs::create_dir_all(&home_root)
        .map_err(|err| home_err("create sandbox home", &home_root, err))?;
    let overrides = match provider {
        "claude" => prepare_claude_overrides(
            &source_home,
            &home_root,
            &workspace_key,
            workspace_path,
            role,
            slot_id,
            &extensions,
            hook_push_ctx,
            claude_shared_credentials_dir,
        ),
        "codex" => prepare_codex_overrides(
            &source_home,
            &home_root,
            &workspace_key,
            workspace_path,
            role,
            slot_id,
            &extensions,
            hook_push_ctx,
        ),
        "antigravity" => prepare_antigravity_overrides(
            &source_home,
            &home_root,
            &workspace_key,
            workspace_path,
            role,
            slot_id,
            &extensions,
            hook_push_ctx,
        ),
        _ => {
            materialize_unwired_provider_skills(provider, &extensions.skills)?;
            // No provider-specific layout is wired, but the seat still runs in
            // its own sandbox home, so HOME must point there.
            let extra_env = home_env(&home_root, []);
            Ok(HomeOverrides {
                home_root,
                extra_env,
            })
        }
    }?;
    materialize_sandbox_home_links(&source_home, &overrides.home_root);
    Ok(overrides)
}

fn prepare_claude_overrides(
    source_home: &Path,
    home_root: &Path,
    workspace_key: &str,
    project_root: &Path,
    role: HomeLayoutRole,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
    shared_credentials_dir: Option<&Path>,
) -> Result<HomeOverrides, AhError> {
    let layout = ClaudeHomeLayout::for_home(home_root);
    fs::create_dir_all(&layout.claude_dir)
        .map_err(|err| home_err("create claude dir", &layout.claude_dir, err))?;
    fs::create_dir_all(&layout.projects_root)
        .map_err(|err| home_err("create claude projects", &layout.projects_root, err))?;
    fs::create_dir_all(&layout.session_env_root)
        .map_err(|err| home_err("create claude session env", &layout.session_env_root, err))?;
    materialize_builtin_rules(
        role,
        "claude",
        home_root,
        project_root,
        slot_id,
        &extensions.rules,
    )?;
    materialize_trust(source_home, &layout, workspace_key)?;
    let skills = resolve_provider_skills(project_root, extensions)?;
    materialize_claude_skills(&layout, &skills)?;
    materialize_builtin_skills(&layout.claude_dir.join("skills"), role)?;
    let plugins = resolve_plugins_for_provider("claude", source_home, &extensions.plugins)?;
    materialize_claude_plugins(&layout, &plugins)?;
    let mut hook_specs = materialize_claude_hooks(source_home, &layout, &extensions.hooks)?;
    if let Some(ctx) = active_hook_push_ctx(hook_push_ctx, "claude") {
        if role == HomeLayoutRole::Master {
            hook_specs.push(materialized_ah_hook(ctx, "UserPromptSubmit"));
        }
        hook_specs.push(materialized_ah_hook(ctx, "Stop"));
    }
    materialize_claude_mcp(&layout, workspace_key, &extensions.mcp)?;
    materialize_claude_settings(
        source_home,
        &layout,
        &extensions.settings,
        &hook_specs,
        &plugins,
    )?;
    Ok(HomeOverrides {
        home_root: home_root.to_path_buf(),
        extra_env: claude_home_env(home_root, shared_credentials_dir)?,
    })
}

fn prepare_codex_overrides(
    source_home: &Path,
    home_root: &Path,
    workspace_key: &str,
    project_root: &Path,
    role: HomeLayoutRole,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
) -> Result<HomeOverrides, AhError> {
    let codex_home = home_root.join(".codex");
    prepare_managed_codex_home(
        source_home,
        &codex_home,
        workspace_key,
        project_root,
        role,
        slot_id,
        extensions,
        hook_push_ctx,
    )?;
    Ok(HomeOverrides {
        home_root: home_root.to_path_buf(),
        extra_env: home_env(home_root, [("CODEX_HOME", ".codex")]),
    })
}

fn prepare_antigravity_overrides(
    source_home: &Path,
    home_root: &Path,
    workspace_key: &str,
    project_root: &Path,
    role: HomeLayoutRole,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
) -> Result<HomeOverrides, AhError> {
    let layout = AntigravityHomeLayout::for_home(home_root);
    fs::create_dir_all(&layout.antigravity_dir).map_err(|err| {
        home_err(
            "create antigravity config dir",
            &layout.antigravity_dir,
            err,
        )
    })?;
    let skills = resolve_provider_skills(project_root, extensions)?;
    materialize_antigravity_skills(&layout, &skills)?;
    materialize_builtin_skills(&layout.skills_dir, role)?;
    ensure_json_file(&layout.settings_path)?;
    materialize_antigravity_settings(source_home, &layout, workspace_key)?;
    materialize_antigravity_onboarding(source_home, &layout)?;
    materialize_builtin_rules(
        role,
        "antigravity",
        home_root,
        project_root,
        slot_id,
        &extensions.rules,
    )?;
    let hook_specs = materialize_hooks(
        source_home,
        &layout.hooks_path.with_file_name("hooks"),
        &extensions.hooks,
    )?;
    if !hook_specs.is_empty() {
        merge_antigravity_hooks(source_home, &layout, &hook_specs)?;
    }
    if let Some(ctx) = active_hook_push_ctx(hook_push_ctx, "antigravity") {
        materialize_antigravity_hooks(source_home, &layout, ctx)?;
    }
    if !hook_specs.is_empty() || active_hook_push_ctx(hook_push_ctx, "antigravity").is_some() {
        materialize_antigravity_json_hooks_gate(&layout)?;
    }
    materialize_antigravity_mcp(&layout, &extensions.mcp)?;

    Ok(HomeOverrides {
        home_root: home_root.to_path_buf(),
        extra_env: home_env(home_root, []),
    })
}

fn copy_antigravity_hooks_if_missing(
    source_home: &Path,
    layout: &AntigravityHomeLayout,
) -> Result<(), AhError> {
    let source_hooks = source_home.join(".gemini/config/hooks.json");
    if source_hooks.is_file() && !layout.hooks_path.exists() {
        if let Some(parent) = layout.hooks_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| home_err("create antigravity hooks parent", parent, err))?;
        }
        fs::copy(&source_hooks, &layout.hooks_path)
            .map_err(|err| home_err("copy antigravity hooks", &layout.hooks_path, err))?;
    }
    Ok(())
}

fn materialize_antigravity_hooks(
    source_home: &Path,
    layout: &AntigravityHomeLayout,
    ctx: &HookPushContext,
) -> Result<(), AhError> {
    copy_antigravity_hooks_if_missing(source_home, layout)?;
    let mut root = read_json_object(&layout.hooks_path).unwrap_or_default();
    inject_antigravity_hook_push(&mut root, ctx);
    write_json_object(&layout.hooks_path, &root)
}

fn merge_antigravity_hooks(
    source_home: &Path,
    layout: &AntigravityHomeLayout,
    hooks: &[MaterializedHook],
) -> Result<(), AhError> {
    copy_antigravity_hooks_if_missing(source_home, layout)?;
    let mut root = read_json_object(&layout.hooks_path).unwrap_or_default();
    let named_hook = object_entry(&mut root, "ah-bundle");
    named_hook.clear();
    for hook in hooks {
        let event = named_hook
            .entry(hook.event.clone())
            .or_insert_with(|| Value::Array(vec![]));
        if !event.is_array() {
            *event = Value::Array(vec![]);
        }
        let Some(event_hooks) = event.as_array_mut() else {
            continue;
        };
        let mut hook_item = Map::new();
        hook_item.insert(
            "type".to_string(),
            Value::String(hook.item.hook_type.clone()),
        );
        hook_item.insert(
            "command".to_string(),
            Value::String(hook.item.command.clone()),
        );
        if let Some(timeout) = hook.item.timeout {
            hook_item.insert("timeout".to_string(), Value::from(timeout));
        }
        event_hooks.push(serde_json::json!({
            "matcher": hook.matcher,
            "hooks": [Value::Object(hook_item)],
        }));
    }
    write_json_object(&layout.hooks_path, &root)
}

fn materialize_antigravity_json_hooks_gate(layout: &AntigravityHomeLayout) -> Result<(), AhError> {
    for path in [&layout.config_path, &layout.config_settings_path] {
        let mut root = read_json_object(path).unwrap_or_default();
        root.insert("enableJsonHooks".to_string(), Value::Bool(true));
        write_json_object(path, &root)?;
    }
    Ok(())
}

fn inject_antigravity_hook_push(root: &mut Map<String, Value>, ctx: &HookPushContext) {
    let named_hook = object_entry(root, "ah-completion-push");
    let stop = named_hook
        .entry("Stop".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    if !stop.is_array() {
        *stop = Value::Array(vec![]);
    }
    let Some(stop_hooks) = stop.as_array_mut() else {
        return;
    };
    remove_ah_owned_hook_groups(stop_hooks);
    let item = build_ah_hook_command(ctx, "stop");
    stop_hooks.push(serde_json::json!({
        "matcher": "",
        "hooks": [{
            "type": item.hook_type,
            "command": item.command,
            "timeout": item.timeout,
        }],
    }));
}

fn materialize_antigravity_settings(
    source_home: &Path,
    layout: &AntigravityHomeLayout,
    workspace_key: &str,
) -> Result<(), AhError> {
    let source_settings = source_home.join(".gemini/antigravity-cli/settings.json");
    if source_settings.is_file() {
        fs::copy(&source_settings, &layout.settings_path)
            .map_err(|err| home_err("copy antigravity settings", &layout.settings_path, err))?;
    }

    let mut settings = read_json_object(&layout.settings_path).unwrap_or_default();
    let trusted_workspaces = settings
        .entry("trustedWorkspaces".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    if !trusted_workspaces.is_array() {
        *trusted_workspaces = Value::Array(vec![]);
    }
    let Some(workspaces) = trusted_workspaces.as_array_mut() else {
        return write_json_object(&layout.settings_path, &settings);
    };
    workspaces.retain(|value| value.as_str() != Some("/home/agent"));
    if !workspaces
        .iter()
        .any(|value| value.as_str() == Some(workspace_key))
    {
        workspaces.push(Value::String(workspace_key.to_string()));
    }
    write_json_object(&layout.settings_path, &settings)
}

fn materialize_antigravity_onboarding(
    source_home: &Path,
    layout: &AntigravityHomeLayout,
) -> Result<(), AhError> {
    fs::create_dir_all(&layout.cache_dir).map_err(|err| {
        home_err(
            "create antigravity onboarding cache",
            &layout.cache_dir,
            err,
        )
    })?;
    let source_onboarding = source_home.join(".gemini/antigravity-cli/cache/onboarding.json");
    if source_onboarding.is_file() {
        fs::copy(&source_onboarding, &layout.onboarding_path)
            .map_err(|err| home_err("copy antigravity onboarding", &layout.onboarding_path, err))?;
        return Ok(());
    }

    let mut onboarding = Map::new();
    onboarding.insert("consumerOnboardingComplete".to_string(), Value::Bool(true));
    onboarding.insert(
        "enterpriseOnboardingComplete".to_string(),
        Value::Bool(false),
    );
    onboarding.insert("onboardingComplete".to_string(), Value::Bool(true));
    write_json_object(&layout.onboarding_path, &onboarding)
}

fn materialize_builtin_rules(
    role: HomeLayoutRole,
    provider: &str,
    home_root: &Path,
    project_root: &Path,
    slot_id: &str,
    bundle_layers: &[String],
) -> Result<(), AhError> {
    // Role rules land wherever the provider declares a rules target. The master
    // role is not special-cased by provider name: a master on any provider with
    // a rules target receives the master kernel, which is what teaches it the
    // role contract including how to report cutover readiness.
    let Some(target) = builtin_rules_target(provider, home_root) else {
        return Ok(());
    };
    let content = composed_rules_for_slot(role, project_root, slot_id, bundle_layers)?;
    write_builtin_rules(&target, &content)
}

fn builtin_rules_target(provider: &str, home_root: &Path) -> Option<PathBuf> {
    match provider {
        "claude" => Some(home_root.join(".claude/CLAUDE.md")),
        "codex" => Some(home_root.join(".codex/AGENTS.md")),
        "antigravity" => Some(home_root.join(".gemini/AGENTS.md")),
        _ => None,
    }
}

fn write_builtin_rules(path: &Path, content: &str) -> Result<(), AhError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| home_err("create builtin rules parent", parent, err))?;
    }
    fs::write(path, content).map_err(|err| home_err("write builtin rules", path, err))
}

pub fn compose_rules(kernel: &str, override_or_default: &str) -> String {
    compose_rules_with_layers(kernel, &[], override_or_default)
}

pub fn compose_rules_with_layers(
    kernel: &str,
    bundle_layers: &[String],
    override_or_default: &str,
) -> String {
    let mut parts = Vec::with_capacity(bundle_layers.len() + 2);
    parts.push(kernel.trim_end().to_string());
    parts.extend(bundle_layers.iter().map(|layer| layer.trim().to_string()));
    parts.push(override_or_default.trim_start().to_string());
    parts.join("\n\n---\n\n")
}

fn composed_rules_for_slot(
    role: HomeLayoutRole,
    project_root: &Path,
    slot_id: &str,
    bundle_layers: &[String],
) -> Result<String, AhError> {
    let kernel = role_kernel(role);
    let default = role_default_rules(role);
    let override_path = project_root.join(".ah/rules").join(format!("{slot_id}.md"));
    let body = match fs::read_to_string(&override_path) {
        Ok(body) => body,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => default.to_string(),
        Err(err) => return Err(home_err("read project rules override", &override_path, err)),
    };
    Ok(compose_rules_with_layers(kernel, bundle_layers, &body))
}

fn role_kernel(role: HomeLayoutRole) -> &'static str {
    match role {
        HomeLayoutRole::Master => builtin::MASTER_KERNEL,
        HomeLayoutRole::Worker => builtin::WORKER_KERNEL,
    }
}

fn role_default_rules(role: HomeLayoutRole) -> &'static str {
    match role {
        HomeLayoutRole::Master => builtin::DEFAULT_MASTER,
        HomeLayoutRole::Worker => builtin::DEFAULT_WORKER,
    }
}

fn default_rules_slot_id(role: HomeLayoutRole) -> &'static str {
    match role {
        HomeLayoutRole::Master => "master",
        HomeLayoutRole::Worker => "worker",
    }
}

fn materialize_sandbox_home_links(source_home: &Path, home_root: &Path) {
    for relative in WHITELIST {
        link_into_sandbox(source_home, home_root, relative);
    }
    for relative in PROVIDER_AUTH_WHITELIST {
        link_auth_file_into_sandbox(source_home, home_root, relative);
    }
}

fn link_into_sandbox(source_home: &Path, home_root: &Path, relative: &str) {
    let source = source_home.join(relative);
    if !source.exists() {
        return;
    }
    let target = home_root.join(relative);
    let Some(parent) = target.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    if target.is_symlink() {
        if same_resolved_path(&target, &source) {
            return;
        }
        if fs::remove_file(&target).is_err() {
            return;
        }
    } else if target.exists() {
        return;
    }
    #[cfg(unix)]
    {
        if let Err(err) = std::os::unix::fs::symlink(&source, &target) {
            tracing::warn!(
                source = %source.display(),
                target = %target.display(),
                %err,
                "failed to symlink sandbox home whitelist entry"
            );
        }
    }
}

fn link_auth_file_into_sandbox(source_home: &Path, home_root: &Path, relative: &str) {
    match materialize_auth_file_with_ladder(source_home, home_root, relative) {
        Ok(()) | Err(AuthMaterializationErrorCode::AuthProviderTokenMissing) => {}
        Err(err) => {
            tracing::warn!(
                relative,
                ?err,
                "failed to materialize provider auth into sandbox"
            );
        }
    }
}

fn materialize_trust(
    source_home: &Path,
    layout: &ClaudeHomeLayout,
    workspace_key: &str,
) -> Result<(), AhError> {
    let source_trust = source_home.join(".claude.json");
    if !layout.trust_path.exists() && source_trust.is_file() {
        copy_if_missing(&source_trust, &layout.trust_path);
    }
    if !layout.config_dir_state_path.exists() && source_trust.is_file() {
        copy_if_missing(&source_trust, &layout.config_dir_state_path);
    }
    ensure_trust_file(&layout.trust_path)?;
    ensure_trust_file(&layout.config_dir_state_path)?;
    ensure_claude_workspace_trust(&layout.trust_path, workspace_key)?;
    ensure_claude_workspace_trust(&layout.config_dir_state_path, workspace_key)
}

#[derive(Debug, Clone)]
struct MaterializedHook {
    event: String,
    matcher: String,
    item: HookItem,
}

pub fn build_ah_hook_command(ctx: &HookPushContext, event: &str) -> HookItem {
    let socket = ctx.ahd_socket_path.display();
    let hook_debug_log = hook_debug_log_path(ctx)
        .map(|path| format!(" --hook-debug-log {}", path.display()))
        .unwrap_or_default();
    let ah_bin = resolve_current_ah_binary();
    HookItem {
        hook_type: "command".to_string(),
        command: format!(
            "AH_SOCKET={socket} {ah_bin} agent notify --agent-id {} --event {event} --provider {} --socket {socket} --hook-json{hook_debug_log}",
            ctx.agent_id, ctx.provider
        ),
        timeout: Some(hook_timeout_for_provider(&ctx.provider)),
    }
}

/// Resolve the `ah` command for injected hooks. Some agent backends spawn hooks
/// in an environment that does not inherit `PATH`, where a bare `ah` silently
/// returns 127. When an `ah` binary sits next to the current executable, return
/// its absolute path; otherwise fall back to the bare `ah` command.
pub(crate) fn resolve_ah_binary(current_exe: &Path) -> String {
    resolve_ah_binary_strict(current_exe)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "ah".to_string())
}

pub(crate) fn resolve_current_ah_binary() -> String {
    std::env::current_exe()
        .map(|exe| resolve_ah_binary(&exe))
        .unwrap_or_else(|_| "ah".to_string())
}

/// Resolve the sibling `ah` binary without falling back to PATH.
pub(crate) fn resolve_ah_binary_strict(current_exe: &Path) -> Option<PathBuf> {
    if let Some(dir) = current_exe.parent() {
        let candidate = dir.join("ah");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

pub(crate) fn resolve_current_ah_binary_strict() -> Result<PathBuf, String> {
    let current_exe =
        std::env::current_exe().map_err(|err| format!("cannot read current executable: {err}"))?;
    resolve_ah_binary_strict(&current_exe).ok_or_else(|| {
        format!(
            "cannot resolve ah binary next to ahd: no sibling ah beside {}",
            current_exe.display()
        )
    })
}

fn hook_timeout_for_provider(provider: &str) -> u64 {
    match provider {
        "antigravity" => 5,
        _ => 5,
    }
}

fn hook_debug_log_path(ctx: &HookPushContext) -> Option<PathBuf> {
    let safe_agent_id = ctx
        .agent_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    ctx.ahd_socket_path.parent().map(|state_dir| {
        state_dir
            .join("hooks-debug")
            .join(format!("{safe_agent_id}.log"))
    })
}

fn active_hook_push_ctx<'a>(
    hook_push_ctx: Option<&'a HookPushContext>,
    provider: &str,
) -> Option<&'a HookPushContext> {
    hook_push_ctx.filter(|ctx| ctx.enabled && ctx.provider == provider)
}

fn materialized_ah_hook(ctx: &HookPushContext, event: &str) -> MaterializedHook {
    MaterializedHook {
        event: event.to_string(),
        matcher: "*".to_string(),
        item: build_ah_hook_command(ctx, &event.to_ascii_lowercase()),
    }
}

fn materialize_claude_hooks(
    source_home: &Path,
    layout: &ClaudeHomeLayout,
    hooks: &HashMap<String, Vec<HookGroup>>,
) -> Result<Vec<MaterializedHook>, AhError> {
    materialize_hooks(source_home, &layout.claude_dir.join("hooks"), hooks)
}

fn resolve_skills(project_root: &Path, skills: &[String]) -> Result<Vec<ResolvedSkill>, AhError> {
    if skills.is_empty() {
        return Ok(Vec::new());
    }
    let refs = parse_skill_refs(skills)?;
    resolve_project_skills(project_root, &refs)
}

fn resolve_provider_skills(
    project_root: &Path,
    extensions: &ExtensionConfig,
) -> Result<Vec<ResolvedSkill>, AhError> {
    reject_builtin_skill_names(
        extensions.skills.iter().map(String::as_str),
        "project skill",
    )?;
    let mut skills = resolve_skills(project_root, &extensions.skills)?;
    reject_builtin_skill_names(
        extensions
            .resolved_skills
            .iter()
            .map(|skill| skill.name.as_str()),
        "bundle skill",
    )?;
    skills.extend(extensions.resolved_skills.iter().cloned());
    Ok(skills)
}

fn reject_builtin_skill_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
    source: &str,
) -> Result<(), AhError> {
    for name in names {
        if builtin::BUILTIN_SKILLS
            .iter()
            .any(|skill| skill.name == name)
        {
            return Err(AhError::EnvironmentNotSupported {
                details: format!(
                    "skill name {name:?} is reserved by an ah builtin skill; rename the {source}"
                ),
            });
        }
    }
    Ok(())
}

fn materialize_claude_skills(
    layout: &ClaudeHomeLayout,
    skills: &[ResolvedSkill],
) -> Result<(), AhError> {
    for item in plan_claude_skill_materialization(&layout.claude_dir, skills) {
        force_symlink(&item.source_dir, &item.target_dir)?;
    }
    Ok(())
}

fn materialize_codex_skills(codex_home: &Path, skills: &[ResolvedSkill]) -> Result<(), AhError> {
    for item in plan_codex_skill_materialization(codex_home, skills) {
        force_symlink(&item.source_dir, &item.target_dir)?;
    }
    Ok(())
}

fn materialize_antigravity_skills(
    layout: &AntigravityHomeLayout,
    skills: &[ResolvedSkill],
) -> Result<(), AhError> {
    for skill in skills {
        force_symlink(&skill.source_dir, &layout.skills_dir.join(&skill.name))?;
    }
    Ok(())
}

fn materialize_builtin_skills(skills_dir: &Path, role: HomeLayoutRole) -> Result<(), AhError> {
    let skills: Vec<_> = builtin::BUILTIN_SKILLS
        .iter()
        .filter(|skill| match skill.scope {
            BuiltinSkillScope::MasterOnly => role == HomeLayoutRole::Master,
            BuiltinSkillScope::AllAgents => true,
        })
        .collect();
    if skills.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(skills_dir)
        .map_err(|err| home_err("create builtin skills dir", skills_dir, err))?;
    for skill in skills {
        let skill_dir = skills_dir.join(skill.name);
        if let Ok(metadata) = fs::symlink_metadata(&skill_dir) {
            if metadata.file_type().is_symlink() || metadata.is_file() {
                fs::remove_file(&skill_dir).map_err(|err| {
                    home_err("remove existing builtin skill path", &skill_dir, err)
                })?;
            } else if metadata.is_dir() {
                fs::remove_dir_all(&skill_dir).map_err(|err| {
                    home_err("remove existing builtin skill dir", &skill_dir, err)
                })?;
            }
        }
        fs::create_dir_all(&skill_dir)
            .map_err(|err| home_err("create builtin skill dir", &skill_dir, err))?;
        let skill_md = skill_dir.join("SKILL.md");
        fs::write(&skill_md, skill.skill_md)
            .map_err(|err| home_err("write builtin skill", &skill_md, err))?;
    }
    Ok(())
}

fn materialize_unwired_provider_skills(provider: &str, skills: &[String]) -> Result<(), AhError> {
    if skills.is_empty() {
        return Ok(());
    }
    Err(AhError::EnvironmentNotSupported {
        details: format!("skills injection target is not wired for provider {provider:?}"),
    })
}

fn materialize_hooks(
    source_home: &Path,
    target_dir: &Path,
    hooks: &HashMap<String, Vec<HookGroup>>,
) -> Result<Vec<MaterializedHook>, AhError> {
    let mut materialized = Vec::new();
    for (event, groups) in hooks {
        for group in groups {
            for item in &group.hooks {
                let source = resolve_extension_source(source_home, &item.command);
                if !source.is_file() {
                    return Err(AhError::EnvironmentNotSupported {
                        details: format!("hook script not found: {}", source.display()),
                    });
                }
                let file_name =
                    source
                        .file_name()
                        .ok_or_else(|| AhError::EnvironmentNotSupported {
                            details: format!("hook script has no filename: {}", source.display()),
                        })?;
                let target = target_dir.join(file_name);
                force_symlink(&source, &target)?;
                let mut item = item.clone();
                item.command = target.display().to_string();
                materialized.push(MaterializedHook {
                    event: event.clone(),
                    matcher: group.matcher.clone(),
                    item,
                });
            }
        }
    }
    Ok(materialized)
}

fn materialize_claude_settings(
    _source_home: &Path,
    layout: &ClaudeHomeLayout,
    provider_settings: &Map<String, Value>,
    hooks: &[MaterializedHook],
    plugins: &[ResolvedPlugin],
) -> Result<(), AhError> {
    ensure_json_file(&layout.settings_path)?;
    let mut settings = read_json_object(&layout.settings_path).unwrap_or_default();
    merge_provider_settings(&mut settings, provider_settings);
    settings.insert(
        "skipDangerousModePermissionPrompt".to_string(),
        Value::Bool(true),
    );
    let permissions = settings
        .entry("permissions".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(perms) = permissions {
        perms
            .entry("defaultMode".to_string())
            .or_insert_with(|| Value::String("bypassPermissions".to_string()));
    }
    inject_claude_hooks(&mut settings, hooks);
    let enabled_plugins = object_entry(&mut settings, "enabledPlugins");
    for plugin in plugins {
        enabled_plugins.insert(plugin.name.clone(), Value::Bool(true));
    }
    write_json_object(&layout.settings_path, &settings)
}

fn merge_provider_settings(
    settings: &mut Map<String, Value>,
    provider_settings: &Map<String, Value>,
) {
    for (key, value) in provider_settings {
        match (settings.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(incoming)) => {
                merge_provider_settings(existing, incoming);
            }
            _ => {
                settings.insert(key.clone(), value.clone());
            }
        }
    }
}

fn inject_claude_hooks(settings: &mut Map<String, Value>, hooks: &[MaterializedHook]) {
    let hooks_root = object_entry(settings, "hooks");
    for hook in hooks {
        let event = hooks_root
            .entry(hook.event.clone())
            .or_insert_with(|| Value::Array(vec![]));
        if !event.is_array() {
            *event = Value::Array(vec![]);
        }
        let Some(event_hooks) = event.as_array_mut() else {
            continue;
        };
        remove_ah_owned_hook_groups(event_hooks);
        let mut hook_item = Map::new();
        hook_item.insert(
            "type".to_string(),
            Value::String(hook.item.hook_type.clone()),
        );
        hook_item.insert(
            "command".to_string(),
            Value::String(hook.item.command.clone()),
        );
        if let Some(timeout) = hook.item.timeout {
            hook_item.insert("timeout".to_string(), Value::from(timeout));
        }
        event_hooks.push(serde_json::json!({
            "matcher": hook.matcher,
            "hooks": [Value::Object(hook_item)],
        }));
    }
}

fn materialize_claude_plugins(
    layout: &ClaudeHomeLayout,
    plugins: &[ResolvedPlugin],
) -> Result<(), AhError> {
    for plugin in plugins {
        if !plugin.cache_dir.is_dir() {
            return Err(AhError::EnvironmentNotSupported {
                details: format!(
                    "claude plugin cache not found for {}: {}",
                    plugin.name,
                    plugin.cache_dir.display()
                ),
            });
        }
        force_symlink(
            &plugin.cache_dir,
            &layout.claude_dir.join("plugins/cache").join(&plugin.name),
        )?;
        force_symlink(
            &plugin.cache_dir,
            &layout.claude_dir.join("plugins").join(&plugin.name),
        )?;
    }
    Ok(())
}

fn prepare_managed_codex_home(
    source_home: &Path,
    codex_home: &Path,
    workspace_key: &str,
    project_root: &Path,
    role: HomeLayoutRole,
    slot_id: &str,
    extensions: &ExtensionConfig,
    hook_push_ctx: Option<&HookPushContext>,
) -> Result<(), AhError> {
    fs::create_dir_all(codex_home).map_err(|err| home_err("create codex home", codex_home, err))?;
    let Some(home_root) = codex_home.parent() else {
        return Err(AhError::EnvironmentNotSupported {
            details: format!("codex home has no parent: {}", codex_home.display()),
        });
    };
    materialize_builtin_rules(
        role,
        "codex",
        home_root,
        project_root,
        slot_id,
        &extensions.rules,
    )?;
    let session_root = codex_home.join("sessions");
    fs::create_dir_all(&session_root)
        .map_err(|err| home_err("create codex sessions", &session_root, err))?;
    let target_config = codex_home.join("config.toml");
    if !target_config.exists() {
        fs::write(&target_config, "# ah agent-local codex config\n")
            .map_err(|err| home_err("write codex config", &target_config, err))?;
    }
    ensure_codex_workspace_trust(&target_config, workspace_key)?;
    inherit_codex_model_settings(source_home, &target_config)?;
    let skills = resolve_provider_skills(project_root, extensions)?;
    materialize_codex_skills(codex_home, &skills)?;
    materialize_builtin_skills(&codex_home.join("skills"), role)?;
    let plugins = resolve_plugins_for_provider("codex", source_home, &extensions.plugins)?;
    materialize_codex_plugins(codex_home, &plugins)?;
    enable_codex_plugins(&target_config, &plugins)?;
    materialize_codex_mcp(&target_config, &extensions.mcp)?;

    let source_hooks = source_home.join(".codex/hooks.json");
    let target_hooks = codex_home.join("hooks.json");
    if source_hooks.is_file() && !target_hooks.exists() {
        if let Some(parent) = target_hooks.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| home_err("create codex hooks parent", parent, err))?;
        }
        fs::copy(&source_hooks, &target_hooks)
            .map_err(|err| home_err("copy codex hooks", &target_hooks, err))?;
    }

    let hook_specs = materialize_hooks(source_home, &codex_home.join("hooks"), &extensions.hooks)?;
    if !hook_specs.is_empty() {
        merge_codex_hooks(codex_home, &hook_specs)?;
    }

    if let Some(ctx) = active_hook_push_ctx(hook_push_ctx, "codex") {
        merge_codex_hook_push(codex_home, ctx)?;
    }

    if !hook_specs.is_empty() || active_hook_push_ctx(hook_push_ctx, "codex").is_some() {
        enable_codex_hooks(&target_config)?;
    }

    let source_version = source_home.join(".codex/version.json");
    let target_version = codex_home.join("version.json");
    if source_version.is_file() && !target_version.exists() {
        let _ = fs::copy(&source_version, &target_version);
    }
    let target_migration = codex_home.join(".personality_migration");
    if !target_migration.exists() {
        let _ = fs::write(&target_migration, "ok\n");
    }
    Ok(())
}

fn merge_codex_hooks(codex_home: &Path, hooks: &[MaterializedHook]) -> Result<(), AhError> {
    let hooks_path = codex_home.join("hooks.json");
    let mut root = read_codex_hooks_for_hook_push(&hooks_path);
    let hooks_root = object_entry(&mut root, "hooks");

    let hooks_dir_str = codex_home.join("hooks").display().to_string();

    // Clean up previously-materialized bundle hooks
    for (_event, event_val) in hooks_root.iter_mut() {
        if let Some(groups) = event_val.as_array_mut() {
            groups.retain_mut(|group| {
                if let Some(items) = group.get_mut("hooks").and_then(|h| h.as_array_mut()) {
                    items.retain(|item| {
                        if let Some(command) = item.get("command").and_then(|c| c.as_str()) {
                            !command.starts_with(&hooks_dir_str)
                        } else {
                            true
                        }
                    });
                    !items.is_empty()
                } else {
                    true
                }
            });
        }
    }

    // Now, insert the new bundle hooks
    for hook in hooks {
        let event_val = hooks_root
            .entry(hook.event.clone())
            .or_insert_with(|| Value::Array(vec![]));
        if !event_val.is_array() {
            *event_val = Value::Array(vec![]);
        }
        let Some(event_hooks) = event_val.as_array_mut() else {
            continue;
        };

        let mut hook_item = Map::new();
        hook_item.insert(
            "type".to_string(),
            Value::String(hook.item.hook_type.clone()),
        );
        hook_item.insert(
            "command".to_string(),
            Value::String(hook.item.command.clone()),
        );
        if let Some(timeout) = hook.item.timeout {
            hook_item.insert("timeout".to_string(), Value::from(timeout));
        }

        let mut merged = false;
        for group in event_hooks.iter_mut() {
            if group.get("matcher").and_then(|m| m.as_str()) == Some(&hook.matcher) {
                if let Some(items) = group.get_mut("hooks").and_then(|h| h.as_array_mut()) {
                    if !items.iter().any(|item| {
                        item.get("command").and_then(|c| c.as_str()) == Some(&hook.item.command)
                    }) {
                        items.push(Value::Object(hook_item.clone()));
                    }
                    merged = true;
                    break;
                }
            }
        }

        if !merged {
            event_hooks.push(serde_json::json!({
                "matcher": hook.matcher,
                "hooks": [Value::Object(hook_item)],
            }));
        }
    }

    write_json_object(&hooks_path, &root)
}

fn enable_codex_hooks(path: &Path) -> Result<(), AhError> {
    let mut root = read_codex_config_for_hook_push(path);
    if !root.is_table() {
        tracing::warn!(
            path = %path.display(),
            "codex config root is not a table while enabling hook push; starting from empty config"
        );
        root = TomlValue::Table(toml::map::Map::new());
    }
    let root_table = root.as_table_mut().expect("root was normalized to table");
    let features = table_entry(root_table, "features");
    features.remove("codex_hooks");
    features.insert("hooks".to_string(), TomlValue::Boolean(true));
    write_codex_config(path, &root)
}

fn merge_codex_hook_push(codex_home: &Path, ctx: &HookPushContext) -> Result<(), AhError> {
    let hooks_path = codex_home.join("hooks.json");
    let mut root = read_codex_hooks_for_hook_push(&hooks_path);
    let hooks_root = object_entry(&mut root, "hooks");
    let stop = hooks_root
        .entry("Stop".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    if !stop.is_array() {
        *stop = Value::Array(vec![]);
    }
    let Some(stop_hooks) = stop.as_array_mut() else {
        return Ok(());
    };
    remove_ah_owned_hook_groups(stop_hooks);
    let item = build_ah_hook_command(ctx, "stop");
    stop_hooks.push(serde_json::json!({
        "matcher": "*",
        "hooks": [{
            "type": item.hook_type,
            "command": item.command,
            "timeout": item.timeout,
        }],
    }));
    write_json_object(&hooks_path, &root)
}

fn remove_ah_owned_hook_groups(groups: &mut Vec<Value>) {
    groups.retain(|group| {
        !group["hooks"]
            .as_array()
            .map(|items| items.iter().any(is_ah_owned_hook_item))
            .unwrap_or(false)
    });
}

fn is_ah_owned_hook_item(item: &Value) -> bool {
    item["command"]
        .as_str()
        .map(|command| command.contains("ah agent notify"))
        .unwrap_or(false)
}

fn read_codex_config_for_hook_push(path: &Path) -> TomlValue {
    match fs::read_to_string(path) {
        Ok(data) => match data.parse::<TomlValue>() {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to parse codex config while enabling hook push; starting from empty config"
                );
                TomlValue::Table(toml::map::Map::new())
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            TomlValue::Table(toml::map::Map::new())
        }
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to read codex config while enabling hook push; starting from empty config"
            );
            TomlValue::Table(toml::map::Map::new())
        }
    }
}

fn read_codex_hooks_for_hook_push(path: &Path) -> Map<String, Value> {
    match fs::read_to_string(path) {
        Ok(data) => match serde_json::from_str::<Value>(&data) {
            Ok(Value::Object(map)) => map,
            Ok(_) => {
                tracing::warn!(
                    path = %path.display(),
                    "codex hooks root is not an object while injecting hook push; starting from empty hooks"
                );
                Map::new()
            }
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to parse codex hooks while injecting hook push; starting from empty hooks"
                );
                Map::new()
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Map::new(),
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to read codex hooks while injecting hook push; starting from empty hooks"
            );
            Map::new()
        }
    }
}

fn materialize_codex_plugins(codex_home: &Path, plugins: &[ResolvedPlugin]) -> Result<(), AhError> {
    for plugin in plugins {
        if !plugin.cache_dir.is_dir() {
            return Err(AhError::EnvironmentNotSupported {
                details: format!(
                    "codex plugin cache not found for {}: {}",
                    plugin.name,
                    plugin.cache_dir.display()
                ),
            });
        }
        force_symlink(
            &plugin.cache_dir,
            &codex_home.join("plugins/cache").join(&plugin.name),
        )?;
    }
    Ok(())
}

fn enable_codex_plugins(path: &Path, plugins: &[ResolvedPlugin]) -> Result<(), AhError> {
    if plugins.is_empty() {
        return Ok(());
    }
    let data = fs::read_to_string(path).unwrap_or_default();
    let mut root = data
        .parse::<TomlValue>()
        .unwrap_or_else(|_| TomlValue::Table(toml::map::Map::new()));
    if !root.is_table() {
        root = TomlValue::Table(toml::map::Map::new());
    }
    let root_table = root.as_table_mut().expect("root was normalized to table");
    let plugins_table = table_entry(root_table, "plugins");
    for plugin in plugins {
        let plugin_table = table_entry(plugins_table, &plugin.name);
        plugin_table.insert("enabled".to_string(), TomlValue::Boolean(true));
    }
    write_codex_config(path, &root)
}

fn materialize_claude_mcp(
    layout: &ClaudeHomeLayout,
    workspace_key: &str,
    servers: &[McpServerConfig],
) -> Result<(), AhError> {
    let servers = filter_mcp_for_provider("claude", servers)?;
    if servers.is_empty() {
        return Ok(());
    }
    for path in [&layout.trust_path, &layout.config_dir_state_path] {
        let mut root = read_json_object(path).unwrap_or_default();
        let projects = object_entry(&mut root, "projects");
        let workspace = object_entry(projects, workspace_key);
        let mcp_servers = object_entry(workspace, "mcpServers");
        for server in &servers {
            mcp_servers.insert(
                server.name.clone(),
                render_claude_mcp_server(server, &host_env_var)?,
            );
        }
        write_json_object(path, &root)?;
    }
    Ok(())
}

fn render_claude_mcp_server<F>(server: &McpServerConfig, lookup: &F) -> Result<Value, AhError>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    let mut value = Map::new();
    match server.transport {
        McpTransport::Stdio => {
            value.insert(
                "command".to_string(),
                Value::String(resolve_secret_placeholders(
                    server.command.as_deref().unwrap_or_default(),
                    &server.name,
                    lookup,
                )?),
            );
            value.insert(
                "args".to_string(),
                Value::Array(
                    server
                        .args
                        .iter()
                        .map(|arg| {
                            resolve_secret_placeholders(arg, &server.name, lookup)
                                .map(Value::String)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
            value.insert(
                "env".to_string(),
                Value::Object(resolve_secret_map(&server.env, &server.name, lookup)?),
            );
        }
        McpTransport::Http | McpTransport::Sse => {
            value.insert(
                "url".to_string(),
                Value::String(resolve_secret_placeholders(
                    server.url.as_deref().unwrap_or_default(),
                    &server.name,
                    lookup,
                )?),
            );
            value.insert(
                "headers".to_string(),
                Value::Object(resolve_secret_map(&server.headers, &server.name, lookup)?),
            );
        }
    }
    Ok(Value::Object(value))
}

fn materialize_codex_mcp(path: &Path, servers: &[McpServerConfig]) -> Result<(), AhError> {
    let servers = filter_mcp_for_provider("codex", servers)?;
    if servers.is_empty() {
        return Ok(());
    }
    let data = fs::read_to_string(path).unwrap_or_default();
    let mut root = data
        .parse::<TomlValue>()
        .unwrap_or_else(|_| TomlValue::Table(toml::map::Map::new()));
    if !root.is_table() {
        root = TomlValue::Table(toml::map::Map::new());
    }
    let root_table = root.as_table_mut().expect("root was normalized to table");
    let mcp_servers = table_entry(root_table, "mcp_servers");
    for server in &servers {
        let table = table_entry(mcp_servers, &server.name);
        table.clear();
        table.insert(
            "command".to_string(),
            TomlValue::String(resolve_secret_placeholders(
                server.command.as_deref().unwrap_or_default(),
                &server.name,
                &host_env_var,
            )?),
        );
        if !server.args.is_empty() {
            table.insert(
                "args".to_string(),
                TomlValue::Array(
                    server
                        .args
                        .iter()
                        .map(|arg| {
                            resolve_secret_placeholders(arg, &server.name, &host_env_var)
                                .map(TomlValue::String)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
        }
        if !server.env.is_empty() {
            table.insert(
                "env".to_string(),
                TomlValue::Table(resolve_secret_toml_map(&server.env, &server.name)?),
            );
        }
    }
    write_codex_config(path, &root)
}

fn materialize_antigravity_mcp(
    layout: &AntigravityHomeLayout,
    servers: &[McpServerConfig],
) -> Result<(), AhError> {
    let servers = filter_mcp_for_provider("antigravity", servers)?;
    if servers.is_empty() {
        return Ok(());
    }
    let mut root = read_json_object(&layout.mcp_config_path).unwrap_or_default();
    let mcp_servers = object_entry(&mut root, "mcpServers");
    for server in &servers {
        mcp_servers.insert(
            server.name.clone(),
            render_antigravity_mcp_server(server, &host_env_var)?,
        );
    }
    write_json_object(&layout.mcp_config_path, &root)
}

fn render_antigravity_mcp_server<F>(server: &McpServerConfig, lookup: &F) -> Result<Value, AhError>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    let mut value = Map::new();
    match server.transport {
        McpTransport::Stdio => {
            value.insert(
                "command".to_string(),
                Value::String(resolve_secret_placeholders(
                    server.command.as_deref().unwrap_or_default(),
                    &server.name,
                    lookup,
                )?),
            );
            value.insert(
                "args".to_string(),
                Value::Array(
                    server
                        .args
                        .iter()
                        .map(|arg| {
                            resolve_secret_placeholders(arg, &server.name, lookup)
                                .map(Value::String)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
            value.insert(
                "env".to_string(),
                Value::Object(resolve_secret_map(&server.env, &server.name, lookup)?),
            );
        }
        McpTransport::Http | McpTransport::Sse => {
            value.insert(
                "serverUrl".to_string(),
                Value::String(resolve_secret_placeholders(
                    server.url.as_deref().unwrap_or_default(),
                    &server.name,
                    lookup,
                )?),
            );
            if !server.headers.is_empty() {
                value.insert(
                    "headers".to_string(),
                    Value::Object(resolve_secret_map(&server.headers, &server.name, lookup)?),
                );
            }
        }
    }
    Ok(Value::Object(value))
}

fn filter_mcp_for_provider<'a>(
    provider: &str,
    servers: &'a [McpServerConfig],
) -> Result<Vec<&'a McpServerConfig>, AhError> {
    let mut supported = Vec::new();
    for server in servers {
        if mcp_transport_supported(provider, server.transport) {
            supported.push(server);
        } else if server.optional {
            tracing::warn!(
                provider,
                server = %server.name,
                transport = ?server.transport,
                "skipping optional unsupported bundle MCP server"
            );
        } else {
            return Err(AhError::EnvironmentNotSupported {
                details: format!(
                    "bundle MCP server {:?} uses unsupported transport {:?} for provider {provider}",
                    server.name, server.transport
                ),
            });
        }
    }
    Ok(supported)
}

fn mcp_transport_supported(provider: &str, transport: McpTransport) -> bool {
    match provider {
        "claude" | "antigravity" => true,
        "codex" => transport == McpTransport::Stdio,
        _ => false,
    }
}

fn resolve_secret_map<F>(
    map: &HashMap<String, String>,
    server_name: &str,
    lookup: &F,
) -> Result<Map<String, Value>, AhError>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    let mut resolved = Map::new();
    for (key, value) in map {
        resolved.insert(
            key.clone(),
            Value::String(resolve_secret_placeholders(value, server_name, lookup)?),
        );
    }
    Ok(resolved)
}

fn resolve_secret_toml_map(
    map: &HashMap<String, String>,
    server_name: &str,
) -> Result<toml::map::Map<String, TomlValue>, AhError> {
    let mut resolved = toml::map::Map::new();
    for (key, value) in map {
        resolved.insert(
            key.clone(),
            TomlValue::String(resolve_secret_placeholders(
                value,
                server_name,
                &host_env_var,
            )?),
        );
    }
    Ok(resolved)
}

fn host_env_var(name: &str) -> Result<String, std::env::VarError> {
    std::env::var(name)
}

fn resolve_secret_placeholders<F>(
    value: &str,
    server_name: &str,
    lookup: &F,
) -> Result<String, AhError>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    let mut output = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(AhError::EnvironmentNotSupported {
                details: format!(
                    "bundle MCP server {server_name:?} contains an invalid environment placeholder"
                ),
            });
        };
        let var = &after[..end];
        let resolved = lookup(var).map_err(|_| AhError::EnvironmentNotSupported {
            details: format!(
                "bundle MCP server {server_name:?} requires {var}, not set in current environment"
            ),
        })?;
        output.push_str(&resolved);
        rest = &after[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}

/// The directory every sandbox home is created under. `ah reclaim` walks it to
/// find homes no state directory points at any more.
pub fn sandbox_home_root() -> Result<PathBuf, AhError> {
    Ok(xdg_cache_root()?.join("ah/sandboxes"))
}

pub fn sandbox_home_for_sandbox_dir(sandbox_dir: &Path) -> Result<PathBuf, AhError> {
    let sandbox_path = sandbox_dir
        .canonicalize()
        .unwrap_or_else(|_| sandbox_dir.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(sandbox_path.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    let project_id_short = digest
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(xdg_cache_root()?
        .join("ah/sandboxes")
        .join(project_id_short))
}

fn resolve_extension_source(source_home: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        source_home.join(path)
    }
}

fn workspace_trust_key(workspace_path: &Path) -> String {
    workspace_path
        .canonicalize()
        .unwrap_or_else(|_| workspace_path.to_path_buf())
        .display()
        .to_string()
}

fn xdg_cache_root() -> Result<PathBuf, AhError> {
    if let Some(cache) = std::env::var_os("XDG_CACHE_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(cache));
    }
    Ok(env_home()?.join(".cache"))
}

fn materialization_source_home() -> Result<PathBuf, AhError> {
    let env_home = env_home()?;
    let passwd_home = std::env::var("USER")
        .ok()
        .and_then(|user| passwd_home_for_user(&user));
    Ok(resolve_materialization_source_home(env_home, passwd_home))
}

fn resolve_materialization_source_home(env_home: PathBuf, passwd_home: Option<PathBuf>) -> PathBuf {
    if is_ah_sandbox_home(&env_home)
        && let Some(passwd_home) = passwd_home
    {
        return passwd_home;
    }
    env_home
}

pub(crate) fn is_ah_sandbox_home(path: &Path) -> bool {
    let path = path.to_string_lossy();
    path.contains("/.cache/ah/sandboxes/") || path.contains("/.cache/ah/sandboxes/")
}

fn passwd_home_for_user(user: &str) -> Option<PathBuf> {
    let passwd = fs::read_to_string("/etc/passwd").ok()?;
    passwd.lines().find_map(|line| {
        let mut fields = line.split(':');
        let name = fields.next()?;
        if name != user {
            return None;
        }
        let _password = fields.next()?;
        let _uid = fields.next()?;
        let _gid = fields.next()?;
        let _gecos = fields.next()?;
        let home = fields.next()?;
        if home.is_empty() {
            None
        } else {
            Some(PathBuf::from(home))
        }
    })
}

fn env_home() -> Result<PathBuf, AhError> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| AhError::EnvironmentNotSupported {
            details: "HOME is not set for provider home materialization".into(),
        })
}

/// Environment that points a provider at an already materialized sandbox home.
///
/// This is the same mapping the materialization functions apply, exposed for
/// callers that re-enter an existing home instead of building one — the master
/// revive path in particular. Provider-specific home variables live here only,
/// so no caller spells `CLAUDE_CONFIG_DIR` or `CODEX_HOME` on its own.
pub fn provider_home_env(provider: &str, home_root: &Path) -> HashMap<String, String> {
    match crate::provider::manifest::canonicalize_provider_name(provider) {
        "claude" => home_env(home_root, [("CLAUDE_CONFIG_DIR", ".claude")]),
        "codex" => home_env(home_root, [("CODEX_HOME", ".codex")]),
        _ => home_env(home_root, []),
    }
}

fn home_env<const N: usize>(
    home_root: &Path,
    entries: [(&str, &str); N],
) -> HashMap<String, String> {
    let mut env = HashMap::from([("HOME".to_string(), home_root.display().to_string())]);
    for (key, relative) in entries {
        env.insert(
            key.to_string(),
            home_root.join(relative).display().to_string(),
        );
    }
    env
}

pub(crate) fn validate_claude_shared_credentials_dir(path: &Path) -> Result<PathBuf, AhError> {
    if path.as_os_str().is_empty() || path.as_os_str().to_string_lossy().trim().is_empty() {
        return Err(AhError::EnvironmentNotSupported {
            details: "providers.claude.shared_credentials_dir must be non-empty".into(),
        });
    }
    if !path.is_absolute() {
        return Err(AhError::EnvironmentNotSupported {
            details: format!(
                "providers.claude.shared_credentials_dir must be absolute: {}",
                path.display()
            ),
        });
    }
    let metadata = fs::symlink_metadata(path).map_err(|err| AhError::EnvironmentNotSupported {
        details: format!(
            "providers.claude.shared_credentials_dir is not usable: {}: {err}",
            path.display()
        ),
    })?;
    if metadata.file_type().is_symlink() {
        return Err(AhError::EnvironmentNotSupported {
            details: format!(
                "providers.claude.shared_credentials_dir must not be a symlink: {}",
                path.display()
            ),
        });
    }
    if !metadata.is_dir() {
        return Err(AhError::EnvironmentNotSupported {
            details: format!(
                "providers.claude.shared_credentials_dir must be a directory: {}",
                path.display()
            ),
        });
    }
    if let Some(mount) = windows_interop_mount_for_path(path, &read_mount_table()) {
        return Err(AhError::EnvironmentNotSupported {
            details: format!(
                "providers.claude.shared_credentials_dir is on a Windows interop filesystem \
                 ({} mounted at {}): {}. A token refresh rewrites this store in place, which \
                 needs POSIX ownership and atomic rename; keep it on the Linux filesystem, \
                 e.g. ~/.claude",
                mount.filesystem,
                mount.mount_point,
                path.display()
            ),
        });
    }
    Ok(path.to_path_buf())
}

/// A mount that ah refuses to host credentials on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowsInteropMount {
    pub(crate) mount_point: String,
    pub(crate) filesystem: String,
}

fn read_mount_table() -> String {
    fs::read_to_string("/proc/self/mounts").unwrap_or_default()
}

/// The Windows interop mount point backing `path`, if any — the auth-store
/// doorman uses this to spot a credential store reaching across the WSL
/// boundary (decision 0006).
pub fn windows_interop_mount_point(path: &Path) -> Option<String> {
    windows_interop_mount_for_path(path, &read_mount_table()).map(|mount| mount.mount_point)
}

/// Returns the mount backing `path` when that mount is a Windows drive exposed
/// into Linux (WSL's `drvfs`, whether carried over `9p` or mounted directly).
///
/// The decision is made from the live mount table rather than from how the path
/// is spelled: `/mnt/anything` is only suspicious by convention, while the mount
/// table is the fact. A Linux directory that merely lives under `/mnt` is
/// therefore accepted, and a Windows drive mounted somewhere unusual is still
/// caught.
pub(crate) fn windows_interop_mount_for_path(
    path: &Path,
    mount_table: &str,
) -> Option<WindowsInteropMount> {
    let path = path.to_string_lossy();
    let mut best: Option<WindowsInteropMount> = None;
    let mut best_len = 0usize;
    for line in mount_table.lines() {
        let mut fields = line.split_whitespace();
        let (_device, mount_point, filesystem) = match (fields.next(), fields.next(), fields.next())
        {
            (Some(device), Some(mount_point), Some(filesystem)) => {
                (device, mount_point, filesystem)
            }
            _ => continue,
        };
        let options = fields.next().unwrap_or("");
        let covers = path == mount_point
            || mount_point == "/"
            || path.starts_with(&format!("{mount_point}/"));
        if !covers || mount_point.len() < best_len {
            continue;
        }
        // WSL exposes Windows drives as drvfs, nowadays transported over 9p with
        // `aname=drvfs` in the options. A 9p mount without that marker is an
        // ordinary network share and is left alone.
        let is_windows_interop =
            filesystem == "drvfs" || (filesystem == "9p" && options.contains("aname=drvfs"));
        best_len = mount_point.len();
        best = is_windows_interop.then(|| WindowsInteropMount {
            mount_point: mount_point.to_string(),
            filesystem: filesystem.to_string(),
        });
    }
    best
}

fn claude_home_env(
    home_root: &Path,
    shared_credentials_dir: Option<&Path>,
) -> Result<HashMap<String, String>, AhError> {
    let mut env = home_env(home_root, [("CLAUDE_CONFIG_DIR", ".claude")]);
    let shared_credentials_dir =
        shared_credentials_dir.ok_or_else(|| AhError::EnvironmentNotSupported {
            details: "providers.claude.shared_credentials_dir is required for Claude".into(),
        })?;
    let shared_credentials_dir = validate_claude_shared_credentials_dir(shared_credentials_dir)?;
    env.insert(
        "CLAUDE_SECURESTORAGE_CONFIG_DIR".to_string(),
        shared_credentials_dir.display().to_string(),
    );
    Ok(env)
}

fn ensure_trust_file(path: &Path) -> Result<(), AhError> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| home_err("create trust parent", parent, err))?;
    }
    fs::write(path, "{}\n").map_err(|err| home_err("write trust file", path, err))
}

fn ensure_claude_workspace_trust(path: &Path, workspace_key: &str) -> Result<(), AhError> {
    let mut root = read_json_object(path).unwrap_or_default();
    root.insert("trusted".to_string(), Value::Bool(true));
    let projects = object_entry(&mut root, "projects");
    remove_legacy_workspace_json_key(projects, workspace_key);
    let workspace = object_entry(projects, workspace_key);
    workspace.insert("hasTrustDialogAccepted".to_string(), Value::Bool(true));
    workspace
        .entry("allowedTools".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    workspace
        .entry("mcpContextUris".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    workspace
        .entry("mcpServers".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    workspace
        .entry("enabledMcpjsonServers".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    workspace
        .entry("disabledMcpjsonServers".to_string())
        .or_insert_with(|| Value::Array(vec![]));
    workspace
        .entry("projectOnboardingSeenCount".to_string())
        .or_insert_with(|| Value::from(1));
    write_json_object(path, &root)
}

fn object_entry<'a>(map: &'a mut Map<String, Value>, key: &str) -> &'a mut Map<String, Value> {
    let value = map
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    value
        .as_object_mut()
        .expect("value was normalized to object")
}

fn ensure_codex_workspace_trust(path: &Path, workspace_key: &str) -> Result<(), AhError> {
    let data = fs::read_to_string(path).unwrap_or_default();
    let mut root = data
        .parse::<TomlValue>()
        .unwrap_or_else(|_| TomlValue::Table(toml::map::Map::new()));
    let Some(root_table) = root.as_table_mut() else {
        root = TomlValue::Table(toml::map::Map::new());
        root.as_table_mut()
            .expect("root was normalized to table")
            .insert(
                "projects".to_string(),
                TomlValue::Table(toml::map::Map::new()),
            );
        write_codex_config(path, &root)?;
        return ensure_codex_workspace_trust(path, workspace_key);
    };
    let projects = table_entry(root_table, "projects");
    remove_legacy_workspace_toml_key(projects, workspace_key);
    let workspace = table_entry(projects, workspace_key);
    workspace.insert(
        "trust_level".to_string(),
        TomlValue::String("trusted".to_string()),
    );
    let tui = table_entry(root_table, "tui");
    let model_availability_nux = table_entry(tui, "model_availability_nux");
    model_availability_nux.insert("gpt-5.5".to_string(), TomlValue::Integer(4));
    write_codex_config(path, &root)
}

/// Carries the operator's model choice into a codex seat (#6).
///
/// The sandbox config is generated, so without this a seat runs whatever model
/// codex defaults to — not the one the operator pinned on the host. That bit
/// for real when only one model had quota left: the host CLI used the pinned
/// model and worked while every ah seat spawned on the default model and hit
/// the usage limit. Seat-local values win; the host's choice is a default, so
/// a bundle or operator override inside the sandbox stays authoritative.
fn inherit_codex_model_settings(source_home: &Path, target_config: &Path) -> Result<(), AhError> {
    const MODEL_KEYS: [&str; 2] = ["model", "model_reasoning_effort"];

    let source_path = source_home.join(".codex/config.toml");
    let Ok(source_raw) = fs::read_to_string(&source_path) else {
        return Ok(());
    };
    let Ok(source) = source_raw.parse::<TomlValue>() else {
        return Ok(());
    };
    let Some(source_table) = source.as_table() else {
        return Ok(());
    };

    let target_raw = fs::read_to_string(target_config).unwrap_or_default();
    let mut target = target_raw
        .parse::<TomlValue>()
        .unwrap_or_else(|_| TomlValue::Table(toml::map::Map::new()));
    let Some(target_table) = target.as_table_mut() else {
        return Ok(());
    };

    let mut changed = false;
    for key in MODEL_KEYS {
        if let Some(value) = source_table.get(key)
            && !target_table.contains_key(key)
        {
            target_table.insert(key.to_string(), value.clone());
            changed = true;
        }
    }
    if changed {
        write_codex_config(target_config, &target)?;
    }
    Ok(())
}

fn table_entry<'a>(
    table: &'a mut toml::map::Map<String, TomlValue>,
    key: &str,
) -> &'a mut toml::map::Map<String, TomlValue> {
    let value = table
        .entry(key.to_string())
        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()));
    if !value.is_table() {
        *value = TomlValue::Table(toml::map::Map::new());
    }
    value.as_table_mut().expect("value was normalized to table")
}

fn remove_legacy_workspace_json_key(map: &mut Map<String, Value>, workspace_key: &str) {
    if workspace_key != "/home/agent" {
        map.remove("/home/agent");
    }
}

fn remove_legacy_workspace_toml_key(
    table: &mut toml::map::Map<String, TomlValue>,
    workspace_key: &str,
) {
    if workspace_key != "/home/agent" {
        table.remove("/home/agent");
    }
}

fn write_codex_config(path: &Path, root: &TomlValue) -> Result<(), AhError> {
    let data = toml::to_string_pretty(root).map_err(|err| AhError::EnvironmentNotSupported {
        details: format!("serialize codex config {}: {err}", path.display()),
    })?;
    fs::write(path, data).map_err(|err| home_err("write codex config", path, err))
}

fn ensure_json_file(path: &Path) -> Result<(), AhError> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| home_err("create json parent", parent, err))?;
    }
    fs::write(path, "{}\n").map_err(|err| home_err("write json file", path, err))
}

fn copy_if_missing(source: &Path, target: &Path) {
    if target.exists() || !source.is_file() {
        return;
    }
    let Some(parent) = target.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    let _ = fs::copy(source, target);
}

fn symlink_auth_file_checked(source: &Path, target: &Path) -> Result<(), std::io::Error> {
    let Some(parent) = target.parent() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "symlink target has no parent",
        ));
    };
    fs::create_dir_all(parent)?;
    if target.is_symlink() && same_resolved_path(target, source) {
        return Ok(());
    }
    // A sandbox left over from a version that copied credentials still holds a
    // private file here. Leaving it would keep that seat on a token nobody else
    // can refresh, so the copy is replaced by the shared link; the host store is
    // the one source of truth the operator logs into.
    if target.is_symlink() || target.exists() {
        tracing::info!(
            target = %target.display(),
            "replacing a private provider credential copy with the shared host store"
        );
        fs::remove_file(target)?;
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source, target)
    }
    #[cfg(not(unix))]
    {
        let _ = source;
        let _ = target;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "auth symlink requires unix",
        ))
    }
}

fn copy_auth_file(
    source: &Path,
    target: &Path,
    require_real_file: bool,
) -> Result<(), AuthMaterializationErrorCode> {
    let Some(parent) = target.parent() else {
        tracing::warn!(target = %target.display(), "provider auth target has no parent");
        return Err(AuthMaterializationErrorCode::AuthSandboxMountFail);
    };
    fs::create_dir_all(parent).map_err(|err| {
        tracing::warn!(
            parent = %parent.display(),
            error = %err,
            "failed to create provider auth target parent"
        );
        AuthMaterializationErrorCode::AuthSandboxMountFail
    })?;
    if target.is_symlink() || target.is_file() {
        fs::remove_file(target).map_err(|err| {
            tracing::warn!(
                target = %target.display(),
                error = %err,
                "failed to remove existing provider auth target"
            );
            AuthMaterializationErrorCode::AuthSandboxMountFail
        })?;
    } else if target.exists() {
        tracing::warn!(
            target = %target.display(),
            "provider auth target exists but is not a file"
        );
        return Err(AuthMaterializationErrorCode::AuthSandboxMountFail);
    }
    fs::copy(source, target).map_err(|err| {
        tracing::warn!(
            source = %source.display(),
            target = %target.display(),
            error = %err,
            "failed to copy provider auth file"
        );
        AuthMaterializationErrorCode::AuthSandboxMountFail
    })?;
    #[cfg(unix)]
    fs::set_permissions(target, fs::Permissions::from_mode(0o600)).map_err(|err| {
        tracing::warn!(
            target = %target.display(),
            error = %err,
            "failed to set provider auth file permissions"
        );
        AuthMaterializationErrorCode::AuthSandboxMountFail
    })?;
    if !target.is_file() || (require_real_file && target.is_symlink()) {
        tracing::warn!(
            target = %target.display(),
            "provider auth target verification failed"
        );
        return Err(AuthMaterializationErrorCode::AuthSandboxMountFail);
    }
    Ok(())
}

fn force_symlink(source: &Path, target: &Path) -> Result<(), AhError> {
    let Some(parent) = target.parent() else {
        return Err(AhError::EnvironmentNotSupported {
            details: format!("symlink target has no parent: {}", target.display()),
        });
    };
    fs::create_dir_all(parent).map_err(|err| home_err("create symlink parent", parent, err))?;
    if target.is_symlink() || target.is_file() {
        fs::remove_file(target)
            .map_err(|err| home_err("remove existing symlink target", target, err))?;
    } else if target.is_dir() {
        fs::remove_dir_all(target)
            .map_err(|err| home_err("remove existing symlink directory", target, err))?;
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source, target)
            .map_err(|err| home_err("create symlink", target, err))
    }
    #[cfg(not(unix))]
    {
        let _ = source;
        Err(AhError::EnvironmentNotSupported {
            details: "provider extension symlinks require unix".into(),
        })
    }
}

fn read_json_object(path: &Path) -> Option<Map<String, Value>> {
    let data = fs::read_to_string(path).ok()?;
    match serde_json::from_str::<Value>(&data).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

fn write_json_object(path: &Path, payload: &Map<String, Value>) -> Result<(), AhError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| home_err("create json parent", parent, err))?;
    }
    let data =
        serde_json::to_string_pretty(payload).map_err(|err| AhError::EnvironmentNotSupported {
            details: format!("serialize json {}: {err}", path.display()),
        })? + "\n";
    fs::write(path, data).map_err(|err| home_err("write json object", path, err))
}

fn same_resolved_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn home_err(action: &str, path: &Path, err: std::io::Error) -> AhError {
    AhError::EnvironmentNotSupported {
        details: format!("{action} {}: {err}", path.display()),
    }
}

struct ClaudeHomeLayout {
    claude_dir: PathBuf,
    projects_root: PathBuf,
    session_env_root: PathBuf,
    settings_path: PathBuf,
    trust_path: PathBuf,
    config_dir_state_path: PathBuf,
}

impl ClaudeHomeLayout {
    fn for_home(home_root: &Path) -> Self {
        let claude_dir = home_root.join(".claude");
        Self {
            claude_dir: claude_dir.clone(),
            projects_root: claude_dir.join("projects"),
            session_env_root: claude_dir.join("session-env"),
            settings_path: claude_dir.join("settings.json"),
            trust_path: home_root.join(".claude.json"),
            config_dir_state_path: claude_dir.join(".claude.json"),
        }
    }
}

struct AntigravityHomeLayout {
    antigravity_dir: PathBuf,
    cache_dir: PathBuf,
    skills_dir: PathBuf,
    settings_path: PathBuf,
    config_path: PathBuf,
    config_settings_path: PathBuf,
    hooks_path: PathBuf,
    mcp_config_path: PathBuf,
    onboarding_path: PathBuf,
}

impl AntigravityHomeLayout {
    fn for_home(home_root: &Path) -> Self {
        let antigravity_dir = home_root.join(".gemini/antigravity-cli");
        let config_dir = home_root.join(".gemini/config");
        let cache_dir = antigravity_dir.join("cache");
        Self {
            antigravity_dir: antigravity_dir.clone(),
            cache_dir: cache_dir.clone(),
            skills_dir: config_dir.join("skills"),
            settings_path: antigravity_dir.join("settings.json"),
            config_path: config_dir.join("config.json"),
            config_settings_path: config_dir.join("settings.json"),
            hooks_path: config_dir.join("hooks.json"),
            mcp_config_path: config_dir.join("mcp_config.json"),
            onboarding_path: cache_dir.join("onboarding.json"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::inherit_codex_model_settings;

    /// #6: a seat must run the model the operator pinned on the host, not
    /// whatever codex defaults to. Seat-local values stay authoritative.
    #[test]
    fn codex_seat_inherits_the_host_model_choice_without_overriding_local_values() {
        let temp = tempfile::TempDir::new().unwrap();
        let source_home = temp.path().join("host");
        std::fs::create_dir_all(source_home.join(".codex")).unwrap();
        std::fs::write(
            source_home.join(".codex/config.toml"),
            "model = \"gpt-5.3-codex-spark\"
model_reasoning_effort = \"low\"
",
        )
        .unwrap();
        let inherit_target = temp.path().join("seat-config.toml");
        std::fs::write(
            &inherit_target,
            "# ah agent-local codex config
",
        )
        .unwrap();

        inherit_codex_model_settings(&source_home, &inherit_target).unwrap();

        let seat = std::fs::read_to_string(&inherit_target).unwrap();
        assert!(
            seat.contains("model = \"gpt-5.3-codex-spark\""),
            "got: {seat}"
        );
        assert!(
            seat.contains("model_reasoning_effort = \"low\""),
            "got: {seat}"
        );

        // A seat that already pinned its own model keeps it.
        let local_target = temp.path().join("seat-local.toml");
        std::fs::write(
            &local_target,
            "model = \"seat-pinned\"
",
        )
        .unwrap();
        inherit_codex_model_settings(&source_home, &local_target).unwrap();
        let local = std::fs::read_to_string(&local_target).unwrap();
        assert!(local.contains("model = \"seat-pinned\""), "got: {local}");
        assert!(
            local.contains("model_reasoning_effort = \"low\""),
            "the missing key still inherits: {local}"
        );
    }

    use super::{HomeLayoutRole, builtin, resolve_materialization_source_home};
    use std::path::{Path, PathBuf};

    #[test]
    fn test_materialization_source_home_keeps_normal_home() {
        let env_home = PathBuf::from("/tmp/normal-home");
        let resolved = resolve_materialization_source_home(
            env_home.clone(),
            Some(PathBuf::from("/home/user")),
        );

        assert_eq!(resolved, env_home);
    }

    #[test]
    fn test_materialization_source_home_uses_passwd_home_from_nested_ah_sandbox() {
        let env_home = PathBuf::from("/home/user/.cache/ah/sandboxes/abc123");
        let passwd_home = PathBuf::from("/home/user");
        let resolved = resolve_materialization_source_home(env_home, Some(passwd_home.clone()));

        assert_eq!(resolved, passwd_home);
    }

    #[test]
    fn test_antigravity_builtin_rules_target_is_gemini_agents_md() {
        use super::builtin_rules_target;
        use tempfile::TempDir;

        let home = TempDir::new().unwrap();

        assert_eq!(
            builtin_rules_target("antigravity", home.path()),
            Some(home.path().join(".gemini/AGENTS.md"))
        );
    }

    #[test]
    fn test_antigravity_worker_materializes_agents_md_in_gemini_dir() {
        use super::{HomeLayoutRole, builtin, materialize_builtin_rules};
        use tempfile::TempDir;

        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();

        materialize_builtin_rules(
            HomeLayoutRole::Worker,
            "antigravity",
            home.path(),
            project.path(),
            "worker",
            &[],
        )
        .unwrap();

        let rules_path = home.path().join(".gemini/AGENTS.md");
        let rules = std::fs::read_to_string(rules_path).unwrap();
        assert!(rules.contains(builtin::WORKER_KERNEL.trim()));
        assert!(rules.contains("Default ah Worker Scenario"));
    }

    #[test]
    fn test_antigravity_overrides_materializes_worker_rules() {
        use super::{HomeLayoutRole, builtin, prepare_antigravity_overrides};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();

        let overrides = prepare_antigravity_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &crate::provider::extensions::ExtensionConfig::default(),
            None,
        )
        .unwrap();
        assert_eq!(overrides.home_root, target.path());

        let rules = std::fs::read_to_string(target.path().join(".gemini/AGENTS.md")).unwrap();
        assert!(rules.contains(builtin::WORKER_KERNEL.trim()));
        assert!(rules.contains("Default ah Worker Scenario"));
        assert!(!target.path().join(".gemini/config/config.json").exists());
        assert!(!target.path().join(".gemini/config/settings.json").exists());
    }

    #[test]
    fn rules_compose_keeps_kernel_prefix_and_body() {
        let composed = super::compose_rules("KERNEL", "BODY");

        assert!(composed.starts_with("KERNEL"));
        assert!(composed.contains("BODY"));
    }

    #[test]
    fn rules_loader_uses_user_slot_doc_or_role_default() {
        let project = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join(".ah/rules")).unwrap();
        std::fs::write(project.path().join(".ah/rules/a1.md"), "USER A1 RULES").unwrap();

        let user_doc =
            super::composed_rules_for_slot(HomeLayoutRole::Worker, project.path(), "a1", &[])
                .unwrap();
        let default_doc =
            super::composed_rules_for_slot(HomeLayoutRole::Worker, project.path(), "a2", &[])
                .unwrap();

        assert!(user_doc.contains("USER A1 RULES"));
        assert!(!user_doc.contains("Grep-before-claim"));
        assert!(default_doc.contains("Grep-before-claim"));
        assert!(user_doc.contains("ah Worker Coordination Kernel"));
        assert!(default_doc.contains("ah Worker Coordination Kernel"));
    }

    #[test]
    fn composed_rules_go_to_provider_specific_destinations() {
        let project = tempfile::TempDir::new().unwrap();
        let claude_home = tempfile::TempDir::new().unwrap();
        let antigravity_home = tempfile::TempDir::new().unwrap();
        let codex_home = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join(".ah/rules")).unwrap();
        std::fs::write(project.path().join(".ah/rules/a1.md"), "CUSTOM SLOT A1").unwrap();

        super::materialize_builtin_rules(
            HomeLayoutRole::Worker,
            "claude",
            claude_home.path(),
            project.path(),
            "a1",
            &[],
        )
        .unwrap();
        super::materialize_builtin_rules(
            HomeLayoutRole::Worker,
            "antigravity",
            antigravity_home.path(),
            project.path(),
            "a1",
            &[],
        )
        .unwrap();
        super::materialize_builtin_rules(
            HomeLayoutRole::Worker,
            "codex",
            codex_home.path(),
            project.path(),
            "a1",
            &[],
        )
        .unwrap();

        for path in [
            claude_home.path().join(".claude/CLAUDE.md"),
            antigravity_home.path().join(".gemini/AGENTS.md"),
            codex_home.path().join(".codex/AGENTS.md"),
        ] {
            let content = std::fs::read_to_string(path).unwrap();
            assert!(content.contains("ah Worker Coordination Kernel"));
            assert!(content.contains("CUSTOM SLOT A1"));
        }
    }

    /// Verbatim excerpt of a real WSL2 mount table (Ubuntu-24.04): the Linux root
    /// is ext4, Windows drives arrive as 9p carrying `aname=drvfs`.
    const WSL_MOUNTS: &str = "/dev/sdd / ext4 rw,relatime,discard,errors=remount-ro,data=ordered 0 0\n\
C:\\134 /mnt/c 9p rw,noatime,aname=drvfs;path=C:\\;uid=0;gid=0;symlinkroot=/mnt/,cache=0x5 0 0\n\
D:\\134 /mnt/d 9p rw,noatime,aname=drvfs;path=D:\\;uid=0;gid=0;symlinkroot=/mnt/,cache=0x5 0 0\n\
share /mnt/linux-share 9p rw,relatime,trans=virtio,version=9p2000.L 0 0\n";

    /// Every provider credential ah materializes must stay one shared file.
    ///
    /// A per-sandbox copy drifts the moment the provider refreshes its token:
    /// the new token lands in that sandbox only, while the host and every other
    /// seat keep a token that a rotation can invalidate. Sharing one file means a
    /// refresh by any seat is a refresh for all of them.
    #[cfg(unix)]
    #[test]
    fn provider_credentials_are_shared_with_the_host_not_copied_per_sandbox() {
        for relative in [
            ".codex/auth.json",
            ".gemini/antigravity-cli/antigravity-oauth-token",
        ] {
            let source_home = tempfile::TempDir::new().unwrap();
            let sandbox_home = tempfile::TempDir::new().unwrap();
            let source = source_home.path().join(relative);
            std::fs::create_dir_all(source.parent().unwrap()).unwrap();
            std::fs::write(&source, "{\"token\":\"original\"}").unwrap();

            super::materialize_auth_file_with_ladder(
                source_home.path(),
                sandbox_home.path(),
                relative,
            )
            .unwrap();

            let target = sandbox_home.path().join(relative);
            assert!(
                target.is_symlink(),
                "{relative} must be shared with the host, not copied"
            );

            // A refresh inside the sandbox rewrites the file in place, the way
            // the provider CLIs do; the host must see the new token.
            std::fs::write(&target, "{\"token\":\"refreshed\"}").unwrap();
            assert_eq!(
                std::fs::read_to_string(&source).unwrap(),
                "{\"token\":\"refreshed\"}",
                "{relative}: a refresh in the sandbox must reach the host store"
            );
        }
    }

    /// Upgrading must not leave old sandboxes on their private copies, or the
    /// seats that already exist keep exactly the drift this change removes.
    #[cfg(unix)]
    #[test]
    fn a_legacy_private_credential_copy_is_migrated_to_the_shared_store() {
        let relative = ".gemini/antigravity-cli/antigravity-oauth-token";
        let source_home = tempfile::TempDir::new().unwrap();
        let sandbox_home = tempfile::TempDir::new().unwrap();
        let source = source_home.path().join(relative);
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, "host-token").unwrap();
        let target = sandbox_home.path().join(relative);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "stale-private-copy").unwrap();

        super::materialize_auth_file_with_ladder(source_home.path(), sandbox_home.path(), relative)
            .unwrap();

        assert!(target.is_symlink(), "the private copy must be replaced");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "host-token");
    }

    #[test]
    fn credentials_on_a_windows_drive_are_identified_by_the_mount_table() {
        for path in [
            "/mnt/c/Users/alice/.claude",
            "/mnt/d/coding/creds",
            "/mnt/d",
        ] {
            let mount = super::windows_interop_mount_for_path(Path::new(path), WSL_MOUNTS)
                .unwrap_or_else(|| panic!("{path} sits on a Windows drive"));
            assert_eq!(mount.filesystem, "9p");
        }
    }

    #[test]
    fn credentials_on_the_linux_filesystem_are_accepted() {
        // Including a path under /mnt that is not a Windows drive: the mount
        // table decides, not the spelling.
        for path in ["/root/.claude", "/home/alice/.claude", "/mnt/linux-share/x"] {
            assert_eq!(
                super::windows_interop_mount_for_path(Path::new(path), WSL_MOUNTS),
                None,
                "{path} is on the Linux filesystem"
            );
        }
    }

    #[test]
    fn shared_credentials_dir_on_a_windows_drive_is_refused_with_a_reason() {
        let mount = super::windows_interop_mount_for_path(
            Path::new("/mnt/c/Users/alice/.claude"),
            WSL_MOUNTS,
        )
        .expect("windows drive");
        assert_eq!(mount.mount_point, "/mnt/c");
    }

    #[test]
    fn master_rules_land_on_every_provider_with_a_rules_target() {
        let project = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join(".ah/rules")).unwrap();
        std::fs::write(project.path().join(".ah/rules/master.md"), "CUSTOM MASTER").unwrap();

        for (provider, rules_path) in [
            ("claude", ".claude/CLAUDE.md"),
            ("codex", ".codex/AGENTS.md"),
            ("antigravity", ".gemini/AGENTS.md"),
        ] {
            let home = tempfile::TempDir::new().unwrap();
            super::materialize_builtin_rules(
                HomeLayoutRole::Master,
                provider,
                home.path(),
                project.path(),
                "master",
                &[],
            )
            .unwrap();

            let content = std::fs::read_to_string(home.path().join(rules_path))
                .unwrap_or_else(|err| panic!("{provider} master rules missing: {err}"));
            assert!(
                content.contains("ah master ack-ready"),
                "{provider} master must receive the kernel that defines cutover readiness"
            );
            assert!(
                content.contains("CUSTOM MASTER"),
                "{provider} master must receive the project's master slot rules"
            );
        }
    }

    #[test]
    fn master_and_worker_slots_honor_project_rules() {
        let project = tempfile::TempDir::new().unwrap();
        let master_home = tempfile::TempDir::new().unwrap();
        let worker_home = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join(".ah/rules")).unwrap();
        std::fs::write(project.path().join(".ah/rules/master.md"), "CUSTOM MASTER").unwrap();
        std::fs::write(project.path().join(".ah/rules/a1.md"), "CUSTOM WORKER").unwrap();

        super::materialize_builtin_rules(
            HomeLayoutRole::Master,
            "claude",
            master_home.path(),
            project.path(),
            "master",
            &[],
        )
        .unwrap();
        super::materialize_builtin_rules(
            HomeLayoutRole::Worker,
            "claude",
            worker_home.path(),
            project.path(),
            "a1",
            &[],
        )
        .unwrap();

        let master = std::fs::read_to_string(master_home.path().join(".claude/CLAUDE.md")).unwrap();
        let worker = std::fs::read_to_string(worker_home.path().join(".claude/CLAUDE.md")).unwrap();
        assert!(master.contains("ah Master Coordination Kernel"));
        assert!(master.contains("CUSTOM MASTER"));
        assert!(!master.contains("CUSTOM WORKER"));
        assert!(worker.contains("ah Worker Coordination Kernel"));
        assert!(worker.contains("CUSTOM WORKER"));
        assert!(!worker.contains("CUSTOM MASTER"));
    }

    #[test]
    fn master_kernel_does_not_reference_aspirational_commands() {
        assert!(!builtin::MASTER_KERNEL.contains("ah brief"));
        assert!(!builtin::MASTER_KERNEL.contains("notify escalate"));
    }

    #[test]
    fn test_antigravity_hook_push_enables_json_hooks_gate_and_preserves_config() {
        use super::{
            HomeLayoutRole, HookPushContext, prepare_antigravity_overrides, read_json_object,
        };
        use std::path::PathBuf;
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let config_dir = target.path().join(".gemini/config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join("config.json"), r#"{"existing":"config"}"#).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{"existing":"settings"}"#,
        )
        .unwrap();

        let ctx = HookPushContext {
            agent_id: "a1".to_string(),
            provider: "antigravity".to_string(),
            ahd_socket_path: PathBuf::from("/tmp/ahd.sock"),
            enabled: true,
        };

        prepare_antigravity_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &crate::provider::extensions::ExtensionConfig::default(),
            Some(&ctx),
        )
        .unwrap();

        let config = read_json_object(&config_dir.join("config.json")).unwrap();
        assert_eq!(config["enableJsonHooks"].as_bool(), Some(true));
        assert_eq!(config["existing"].as_str(), Some("config"));

        let settings = read_json_object(&config_dir.join("settings.json")).unwrap();
        assert_eq!(settings["enableJsonHooks"].as_bool(), Some(true));
        assert_eq!(settings["existing"].as_str(), Some("settings"));
    }

    #[test]
    fn claude_master_hook_push_installs_ups_and_stop_with_master_sentinel() {
        use super::{HomeLayoutRole, HookPushContext, prepare_claude_overrides, read_json_object};
        use std::path::PathBuf;
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let shared = TempDir::new().unwrap();
        let ctx = HookPushContext {
            agent_id: "master:s_test:7".to_string(),
            provider: "claude".to_string(),
            ahd_socket_path: PathBuf::from("/tmp/ahd.sock"),
            enabled: true,
        };

        prepare_claude_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Master,
            "master",
            &crate::provider::extensions::ExtensionConfig::default(),
            Some(&ctx),
            Some(shared.path()),
        )
        .unwrap();

        let settings = read_json_object(&target.path().join(".claude/settings.json")).unwrap();
        let hooks = settings["hooks"].as_object().unwrap();
        let ups = hooks["UserPromptSubmit"].to_string();
        let stop = hooks["Stop"].to_string();
        assert!(ups.contains("--agent-id master:s_test:7"));
        assert!(ups.contains("--event userpromptsubmit"));
        assert!(ups.contains("hooks-debug/master_s_test_7.log"));
        assert!(stop.contains("--event stop"));
    }

    #[test]
    fn materialized_ah_hook_passes_real_event_to_command() {
        use super::{HookPushContext, materialized_ah_hook};
        use std::path::PathBuf;

        let ctx = HookPushContext {
            agent_id: "a1".to_string(),
            provider: "claude".to_string(),
            ahd_socket_path: PathBuf::from("/tmp/ahd.sock"),
            enabled: true,
        };

        let hook = materialized_ah_hook(&ctx, "UserPromptSubmit");
        assert!(hook.item.command.contains("--event userpromptsubmit"));
        assert!(!hook.item.command.contains("--event stop"));
    }

    #[test]
    fn test_codex_overrides_creates_version_and_migration() {
        use super::{ExtensionConfig, HomeLayoutRole, prepare_codex_overrides};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();

        let source_codex = source.path().join(".codex");
        std::fs::create_dir_all(&source_codex).unwrap();
        std::fs::write(source_codex.join("version.json"), r#"{"v":"1.0"}"#).unwrap();

        let workspace = TempDir::new().unwrap();
        let overrides = prepare_codex_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &ExtensionConfig::default(),
            None,
        )
        .unwrap();
        assert_eq!(overrides.home_root, target.path());

        assert!(target.path().join(".codex/version.json").exists());
        assert!(target.path().join(".codex/.personality_migration").exists());
        assert!(target.path().join(".codex/config.toml").exists());
    }

    #[test]
    fn test_claude_settings_has_bypass_and_permissions() {
        use super::{ExtensionConfig, HomeLayoutRole, prepare_claude_overrides, read_json_object};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let shared = TempDir::new().unwrap();

        std::fs::write(source.path().join(".claude.json"), "{}").unwrap();

        let workspace = TempDir::new().unwrap();
        let _ = prepare_claude_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &ExtensionConfig::default(),
            None,
            Some(shared.path()),
        )
        .unwrap();

        let settings = read_json_object(&target.path().join(".claude/settings.json")).unwrap();
        assert_eq!(settings["skipDangerousModePermissionPrompt"], true);
        assert_eq!(settings["permissions"]["defaultMode"], "bypassPermissions");
    }

    #[test]
    fn claude_home_env_uses_shared_secure_storage_dir() {
        use super::{ExtensionConfig, HomeLayoutRole, prepare_claude_overrides, read_json_object};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let shared = TempDir::new().unwrap();
        std::fs::write(source.path().join(".claude.json"), "{}").unwrap();

        let overrides = prepare_claude_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &ExtensionConfig::default(),
            None,
            Some(shared.path()),
        )
        .unwrap();

        assert_eq!(
            overrides.extra_env.get("HOME").map(String::as_str),
            Some(target.path().to_str().unwrap())
        );
        assert_eq!(
            overrides
                .extra_env
                .get("CLAUDE_CONFIG_DIR")
                .map(String::as_str),
            Some(target.path().join(".claude").to_str().unwrap())
        );
        assert_eq!(
            overrides
                .extra_env
                .get("CLAUDE_SECURESTORAGE_CONFIG_DIR")
                .map(String::as_str),
            Some(shared.path().to_str().unwrap())
        );
        assert!(!overrides.extra_env.contains_key("CLAUDE_CODE_USE_GATEWAY"));
        assert!(!overrides.extra_env.contains_key("ANTHROPIC_AUTH_TOKEN"));
        assert!(!target.path().join(".claude/.credentials.json").exists());

        let settings = read_json_object(&target.path().join(".claude/settings.json")).unwrap();
        assert_eq!(settings["skipDangerousModePermissionPrompt"], true);
    }

    #[cfg(unix)]
    #[test]
    fn claude_shared_credentials_dir_rejects_symlink_path() {
        use super::{ExtensionConfig, HomeLayoutRole, prepare_claude_overrides};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let shared = TempDir::new().unwrap();
        let link_parent = TempDir::new().unwrap();
        let link = link_parent.path().join("claude-link");
        std::os::unix::fs::symlink(shared.path(), &link).unwrap();

        let err = prepare_claude_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &ExtensionConfig::default(),
            None,
            Some(&link),
        )
        .unwrap_err();

        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[test]
    fn claude_settings_merge_provider_settings_from_extensions() {
        use super::{ExtensionConfig, HomeLayoutRole, prepare_claude_overrides, read_json_object};
        use serde_json::{Map, Value, json};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let shared = TempDir::new().unwrap();
        let target_claude = target.path().join(".claude");
        std::fs::create_dir_all(&target_claude).unwrap();
        std::fs::write(
            target_claude.join("settings.json"),
            r#"{"existing":"keep","statusLine":{"padding":1}}"#,
        )
        .unwrap();

        let mut settings = Map::new();
        settings.insert(
            "model".to_string(),
            Value::String("claude-opus-4-20250514".to_string()),
        );
        settings.insert("autoCompact".to_string(), Value::Bool(false));
        settings.insert(
            "statusLine".to_string(),
            json!({"type":"command","command":"ah ps --format compact"}),
        );
        let extensions = ExtensionConfig {
            settings,
            ..Default::default()
        };

        prepare_claude_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Master,
            "master",
            &extensions,
            None,
            Some(shared.path()),
        )
        .unwrap();

        let settings = read_json_object(&target_claude.join("settings.json")).unwrap();
        assert_eq!(settings["existing"], "keep");
        assert_eq!(settings["model"], "claude-opus-4-20250514");
        assert_eq!(settings["autoCompact"], false);
        assert_eq!(settings["statusLine"]["type"], "command");
        assert_eq!(settings["statusLine"]["command"], "ah ps --format compact");
        assert_eq!(settings["statusLine"]["padding"], 1);
        assert_eq!(settings["skipDangerousModePermissionPrompt"], true);
        assert_eq!(settings["permissions"]["defaultMode"], "bypassPermissions");
    }

    #[test]
    fn test_claude_config_dir_receives_onboarding_state() {
        use super::{ExtensionConfig, HomeLayoutRole, prepare_claude_overrides, read_json_object};
        use tempfile::TempDir;

        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let shared = TempDir::new().unwrap();

        std::fs::write(
            source.path().join(".claude.json"),
            r#"{"hasCompletedOnboarding":true,"lastOnboardingVersion":"2.1.116"}"#,
        )
        .unwrap();

        let workspace = TempDir::new().unwrap();
        let _ = prepare_claude_overrides(
            source.path(),
            target.path(),
            &workspace.path().display().to_string(),
            workspace.path(),
            HomeLayoutRole::Worker,
            "worker",
            &ExtensionConfig::default(),
            None,
            Some(shared.path()),
        )
        .unwrap();

        let config_dir_state =
            read_json_object(&target.path().join(".claude/.claude.json")).unwrap();
        assert_eq!(config_dir_state["hasCompletedOnboarding"], true);
        assert_eq!(config_dir_state["lastOnboardingVersion"], "2.1.116");

        let root_state = read_json_object(&target.path().join(".claude.json")).unwrap();
        assert_eq!(root_state["trusted"], true);
        assert_eq!(config_dir_state["trusted"], true);
    }

    #[test]
    fn mcp_secret_placeholders_resolve_only_at_render_time() {
        let resolved = super::resolve_secret_placeholders(
            "Bearer ${ACME_KEY}",
            "remote",
            &|name| match name {
                "ACME_KEY" => Ok("secret-value".to_string()),
                _ => Err(std::env::VarError::NotPresent),
            },
        )
        .unwrap();

        assert_eq!(resolved, "Bearer secret-value");
    }

    #[test]
    fn mcp_missing_secret_reports_variable_name_not_value() {
        let err = super::resolve_secret_placeholders("Bearer ${ACME_KEY}", "remote", &|_| {
            Err(std::env::VarError::NotPresent)
        })
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("ACME_KEY"), "{message}");
        assert!(!message.contains("secret-value"), "{message}");
    }

    #[test]
    fn claude_mcp_stdio_renders_command_args_and_env() {
        use crate::provider::extensions::{McpServerConfig, McpTransport};
        use std::collections::HashMap;

        let server = McpServerConfig {
            name: "context7".to_string(),
            transport: McpTransport::Stdio,
            command: Some("npx".to_string()),
            args: vec!["-y".to_string(), "@upstash/context7-mcp".to_string()],
            env: HashMap::from([(
                "CONTEXT7_TOKEN".to_string(),
                "${CONTEXT7_TOKEN}".to_string(),
            )]),
            url: None,
            headers: HashMap::new(),
            optional: false,
        };
        let rendered = super::render_claude_mcp_server(&server, &|name| match name {
            "CONTEXT7_TOKEN" => Ok("secret-value".to_string()),
            _ => Err(std::env::VarError::NotPresent),
        })
        .unwrap();

        assert_eq!(rendered["command"], "npx");
        assert_eq!(rendered["args"][0], "-y");
        assert_eq!(rendered["env"]["CONTEXT7_TOKEN"], "secret-value");
    }

    #[test]
    fn antigravity_mcp_remote_uses_server_url_not_legacy_keys() {
        use crate::provider::extensions::{McpServerConfig, McpTransport};
        use std::collections::HashMap;

        let server = McpServerConfig {
            name: "remote".to_string(),
            transport: McpTransport::Http,
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            url: Some("https://mcp.example.test/sse".to_string()),
            headers: HashMap::from([(
                "Authorization".to_string(),
                "Bearer ${ACME_KEY}".to_string(),
            )]),
            optional: false,
        };
        let rendered = super::render_antigravity_mcp_server(&server, &|name| match name {
            "ACME_KEY" => Ok("secret-value".to_string()),
            _ => Err(std::env::VarError::NotPresent),
        })
        .unwrap();

        assert_eq!(rendered["serverUrl"], "https://mcp.example.test/sse");
        assert!(rendered.get("url").is_none());
        assert!(rendered.get("httpUrl").is_none());
        assert_eq!(rendered["headers"]["Authorization"], "Bearer secret-value");
    }

    #[test]
    fn codex_mcp_remote_is_unsupported_unless_optional() {
        use crate::provider::extensions::{McpServerConfig, McpTransport};
        use std::collections::HashMap;

        let required = McpServerConfig {
            name: "remote".to_string(),
            transport: McpTransport::Http,
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            url: Some("https://mcp.example.test/sse".to_string()),
            headers: HashMap::new(),
            optional: false,
        };
        let mut optional = required.clone();
        optional.optional = true;

        assert!(super::filter_mcp_for_provider("codex", &[required]).is_err());
        assert!(
            super::filter_mcp_for_provider("codex", &[optional])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn claude_mcp_remote_writes_workspace_servers_to_both_trust_files() {
        use crate::provider::extensions::{McpServerConfig, McpTransport};
        use std::collections::HashMap;
        use tempfile::TempDir;

        let home = TempDir::new().unwrap();
        let layout = super::ClaudeHomeLayout::for_home(home.path());
        let server = McpServerConfig {
            name: "remote".to_string(),
            transport: McpTransport::Http,
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            url: Some("https://mcp.example.test/sse".to_string()),
            headers: HashMap::new(),
            optional: false,
        };

        super::materialize_claude_mcp(&layout, "/workspace", &[server]).unwrap();

        for path in [&layout.trust_path, &layout.config_dir_state_path] {
            let root = super::read_json_object(path).unwrap();
            assert_eq!(
                root["projects"]["/workspace"]["mcpServers"]["remote"]["url"],
                "https://mcp.example.test/sse"
            );
        }
    }

    #[test]
    fn codex_mcp_stdio_writes_mcp_servers_table() {
        use crate::provider::extensions::{McpServerConfig, McpTransport};
        use std::collections::HashMap;
        use tempfile::TempDir;

        let home = TempDir::new().unwrap();
        let config = home.path().join("config.toml");
        std::fs::write(&config, "# local codex config\n").unwrap();
        let server = McpServerConfig {
            name: "context7".to_string(),
            transport: McpTransport::Stdio,
            command: Some("npx".to_string()),
            args: vec!["-y".to_string()],
            env: HashMap::new(),
            url: None,
            headers: HashMap::new(),
            optional: false,
        };

        super::materialize_codex_mcp(&config, &[server]).unwrap();

        let data = std::fs::read_to_string(config).unwrap();
        assert!(data.contains("[mcp_servers.context7]"), "{data}");
        assert!(data.contains("command = \"npx\""), "{data}");
        assert!(data.contains("args = [\"-y\"]"), "{data}");
    }

    #[test]
    fn antigravity_mcp_stdio_writes_mcp_config_json() {
        use crate::provider::extensions::{McpServerConfig, McpTransport};
        use std::collections::HashMap;
        use tempfile::TempDir;

        let home = TempDir::new().unwrap();
        let layout = super::AntigravityHomeLayout::for_home(home.path());
        let server = McpServerConfig {
            name: "context7".to_string(),
            transport: McpTransport::Stdio,
            command: Some("npx".to_string()),
            args: vec!["-y".to_string()],
            env: HashMap::new(),
            url: None,
            headers: HashMap::new(),
            optional: false,
        };

        super::materialize_antigravity_mcp(&layout, &[server]).unwrap();

        let root = super::read_json_object(&layout.mcp_config_path).unwrap();
        assert_eq!(root["mcpServers"]["context7"]["command"], "npx");
        assert_eq!(root["mcpServers"]["context7"]["args"][0], "-y");
    }

    #[test]
    fn no_mcp_does_not_create_antigravity_mcp_config() {
        use tempfile::TempDir;

        let home = TempDir::new().unwrap();
        let layout = super::AntigravityHomeLayout::for_home(home.path());

        super::materialize_antigravity_mcp(&layout, &[]).unwrap();

        assert!(!layout.mcp_config_path.exists());
    }

    // A1: injected-hook timeout is expressed in the provider's own units.
    // antigravity's hooks.json timeout is seconds (default 30), so the old
    // millisecond-shaped 5000 was interpreted as ~83 minutes and blocked the
    // agent loop. All providers now use 5 (seconds); other branches unchanged.
    #[test]
    fn hook_timeout_for_provider_is_seconds_for_all_providers() {
        use super::hook_timeout_for_provider;

        assert_eq!(hook_timeout_for_provider("antigravity"), 5);
        assert_eq!(hook_timeout_for_provider("claude"), 5);
        assert_eq!(hook_timeout_for_provider("codex"), 5);
        assert_eq!(hook_timeout_for_provider("unknown"), 5);
    }

    // A2: injected hooks run in spawn environments that may not inherit PATH,
    // where a bare `ah` silently returns 127. Prefer the `ah` binary sitting
    // next to the current executable, resolved to an absolute path.
    #[test]
    fn resolve_ah_binary_prefers_sibling_absolute_path() {
        use super::resolve_ah_binary;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let ah_path = dir.path().join("ah");
        std::fs::write(&ah_path, b"#!/bin/sh\n").unwrap();
        let exe = dir.path().join("ahd");

        let resolved = resolve_ah_binary(&exe);

        assert_eq!(resolved, ah_path.display().to_string());
        assert!(std::path::Path::new(&resolved).is_absolute());
    }

    #[test]
    fn resolve_ah_binary_falls_back_to_bare_command_when_absent() {
        use super::resolve_ah_binary;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let exe = dir.path().join("ahd");

        assert_eq!(resolve_ah_binary(&exe), "ah");
    }

    #[test]
    fn resolve_ah_binary_strict_prefers_sibling_absolute_path() {
        use super::resolve_ah_binary_strict;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let ah_path = dir.path().join("ah");
        std::fs::write(&ah_path, b"#!/bin/sh\n").unwrap();
        let exe = dir.path().join("ahd");

        let resolved = resolve_ah_binary_strict(&exe).unwrap();

        assert_eq!(resolved, ah_path);
        assert!(resolved.is_absolute());
    }

    #[test]
    fn resolve_ah_binary_strict_returns_none_when_absent() {
        use super::resolve_ah_binary_strict;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let exe = dir.path().join("ahd");

        assert_eq!(resolve_ah_binary_strict(&exe), None);
    }
}
