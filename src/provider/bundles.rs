use crate::error::AhError;
use crate::provider::extensions::{
    ExtensionConfig, HookGroup, HookItem, McpServerConfig, McpTransport,
};
use crate::provider::fingerprint::{BundleDigest, BundleDigestEntry, deterministic_json};
use crate::provider::skills::ResolvedSkill;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleRole {
    Master,
    Worker,
}

#[derive(Debug, Clone)]
pub struct ResolvedBundles {
    pub extensions: ExtensionConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleInspection {
    pub name: String,
    pub version: String,
    pub skills_count: usize,
    pub hook_count: usize,
    pub rules_count: usize,
    pub mcp_count: usize,
}

#[derive(Debug, Deserialize)]
struct BundleManifest {
    name: String,
    version: String,
    #[serde(default)]
    skills: BundleSkillsManifest,
    #[serde(default)]
    hooks: HashMap<String, Vec<HookGroup>>,
    #[serde(default)]
    rules: BundleRulesManifest,
    #[serde(default)]
    mcp: BundleMcpManifest,
}

#[derive(Debug, Default, Deserialize)]
struct BundleSkillsManifest {
    #[serde(default)]
    include: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct BundleRulesManifest {
    master: Option<String>,
    worker: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct BundleMcpManifest {
    #[serde(default)]
    servers: Vec<McpServerConfig>,
}

#[derive(Debug)]
struct BundleContribution {
    name: String,
    skills: Vec<ResolvedSkill>,
    hooks: HashMap<String, Vec<HookGroup>>,
    rules: Vec<String>,
    mcp: Vec<McpServerConfig>,
    digest: String,
}

pub fn resolve_bundles_for_provider(
    project_root: &Path,
    provider: &str,
    role: BundleRole,
    base: &ExtensionConfig,
) -> Result<ResolvedBundles, AhError> {
    if base.bundle.is_empty() {
        return Ok(ResolvedBundles {
            extensions: base.clone(),
        });
    }
    let contributions = base
        .bundle
        .iter()
        .map(|name| resolve_bundle(project_root, name, role))
        .collect::<Result<Vec<_>, _>>()?;
    validate_bundle_capabilities(provider, role, &contributions)?;
    let mut extensions = base.clone();
    merge_contributions(&mut extensions, &contributions)?;
    extensions.bundle_digest = Some(BundleDigest {
        bundles: contributions
            .iter()
            .map(|bundle| BundleDigestEntry {
                name: bundle.name.clone(),
                digest: bundle.digest.clone(),
            })
            .collect(),
    });
    Ok(ResolvedBundles { extensions })
}

pub fn digest_for_bundles(
    project_root: &Path,
    role: BundleRole,
    bundle_names: &[String],
) -> Result<Option<BundleDigest>, AhError> {
    if bundle_names.is_empty() {
        return Ok(None);
    }
    let entries = bundle_names
        .iter()
        .map(|name| {
            let bundle = resolve_bundle(project_root, name, role)?;
            Ok(BundleDigestEntry {
                name: bundle.name,
                digest: bundle.digest,
            })
        })
        .collect::<Result<Vec<_>, AhError>>()?;
    Ok(Some(BundleDigest { bundles: entries }))
}

pub fn list_bundle_names(project_root: &Path) -> Result<Vec<String>, AhError> {
    let bundles_root = project_root.join(".ah/bundles");
    if !bundles_root.exists() {
        return Ok(Vec::new());
    }
    if !bundles_root.is_dir() {
        return Err(bundle_err(format!(
            "bundle root is not a directory: {}",
            bundles_root.display()
        )));
    }
    let mut names = fs::read_dir(&bundles_root)
        .map_err(|err| bundle_err(format!("read bundle root: {err}")))?
        .filter_map(|entry| {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => return Some(Err(bundle_err(format!("read bundle entry: {err}")))),
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) => return Some(Err(bundle_err(format!("read bundle file type: {err}")))),
            };
            if !file_type.is_dir() {
                return None;
            }
            Some(Ok(entry.file_name().to_string_lossy().to_string()))
        })
        .collect::<Result<Vec<_>, AhError>>()?;
    names.sort();
    Ok(names)
}

pub fn inspect_bundle(project_root: &Path, name: &str) -> Result<BundleInspection, AhError> {
    let worker = resolve_bundle(project_root, name, BundleRole::Worker)?;
    let master = resolve_bundle(project_root, name, BundleRole::Master)?;
    Ok(BundleInspection {
        name: worker.name,
        version: "1".to_string(),
        skills_count: worker.skills.len(),
        hook_count: worker
            .hooks
            .values()
            .flat_map(|groups| groups.iter())
            .map(|group| group.hooks.len())
            .sum(),
        rules_count: worker.rules.len() + master.rules.len(),
        mcp_count: worker.mcp.len(),
    })
}

fn resolve_bundle(
    project_root: &Path,
    name: &str,
    role: BundleRole,
) -> Result<BundleContribution, AhError> {
    validate_bundle_name(name)?;
    let bundles_root = project_root.join(".ah/bundles");
    let bundle_root = bundles_root.join(name);
    if !bundle_root.is_dir() {
        return Err(bundle_err(format!(
            "bundle {name:?} not found: {}",
            bundle_root.display()
        )));
    }
    let manifest_path = bundle_root.join("bundle.toml");
    if !manifest_path.is_file() {
        return Err(bundle_err(format!(
            "bundle {name:?} missing bundle.toml: {}",
            manifest_path.display()
        )));
    }
    let canonical_root = bundle_root
        .canonicalize()
        .map_err(|err| bundle_err(format!("resolve bundle {name:?}: {err}")))?;
    let raw = fs::read_to_string(&manifest_path).map_err(|err| {
        bundle_err(format!(
            "read bundle manifest for {name:?} at {}: {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: BundleManifest = toml::from_str(&raw)
        .map_err(|err| bundle_err(format!("parse bundle {name:?} bundle.toml: {err}")))?;
    if manifest.name != name {
        return Err(bundle_err(format!(
            "bundle directory {name:?} has manifest name {:?}",
            manifest.name
        )));
    }
    if manifest.version != "1" {
        return Err(bundle_err(format!(
            "bundle {name:?} has unsupported version {:?}",
            manifest.version
        )));
    }

    let skills = resolve_bundle_skills(&canonical_root, &manifest)?;
    let hooks = resolve_bundle_hooks(&canonical_root, &manifest.hooks)?;
    let rules = resolve_bundle_rules(&canonical_root, &manifest.rules, role)?;
    let mcp = resolve_bundle_mcp(&manifest.mcp)?;
    let digest = compute_bundle_digest(&canonical_root, &manifest_path, &skills, &hooks, &rules)?;

    Ok(BundleContribution {
        name: name.to_string(),
        skills,
        hooks,
        rules,
        mcp,
        digest,
    })
}

fn validate_bundle_capabilities(
    provider: &str,
    role: BundleRole,
    contributions: &[BundleContribution],
) -> Result<(), AhError> {
    if provider == "claude" {
        return Ok(());
    }
    for contribution in contributions {
        if provider == "codex" {
            if role == BundleRole::Master && !contribution.rules.is_empty() {
                return Err(bundle_err(format!(
                    "codex master bundle rules are unsupported for bundle {:?}",
                    contribution.name
                )));
            }
            continue;
        }
        if provider == "antigravity" {
            if role == BundleRole::Master && !contribution.rules.is_empty() {
                return Err(bundle_err(format!(
                    "antigravity master bundle rules are unsupported for bundle {:?}",
                    contribution.name
                )));
            }
            continue;
        }
        if !contribution.skills.is_empty() {
            return Err(bundle_err(format!(
                "bundle {:?} includes skills, but PR-2 supports bundle skills only for provider claude; provider {provider:?} must wait for PR-3/PR-4",
                contribution.name
            )));
        }
        if !contribution.hooks.is_empty() {
            return Err(bundle_err(format!(
                "bundle {:?} includes hooks, but PR-2 supports bundle hooks only for provider claude; provider {provider:?} must wait for PR-3/PR-4",
                contribution.name
            )));
        }
        if !contribution.rules.is_empty() {
            return Err(bundle_err(format!(
                "bundle {:?} includes {:?} rules, but PR-2 supports bundle rules only for provider claude; provider {provider:?} must wait for PR-3/PR-4",
                contribution.name, role
            )));
        }
    }
    Ok(())
}

fn merge_contributions(
    extensions: &mut ExtensionConfig,
    contributions: &[BundleContribution],
) -> Result<(), AhError> {
    let mut skills_by_name = extensions
        .skills
        .iter()
        .map(|name| (name.clone(), SkillSource::Project))
        .collect::<BTreeMap<_, _>>();
    for skill in &extensions.resolved_skills {
        skills_by_name.insert(
            skill.name.clone(),
            SkillSource::Path(skill.source_dir.clone()),
        );
    }

    for contribution in contributions {
        for skill in &contribution.skills {
            match skills_by_name.get(&skill.name) {
                Some(SkillSource::Path(existing)) if existing == &skill.source_dir => {}
                Some(SkillSource::Project) => {
                    return Err(bundle_err(format!(
                        "bundle skill {} conflicts with project skill of the same name",
                        skill.name
                    )));
                }
                Some(SkillSource::Path(existing)) => {
                    return Err(bundle_err(format!(
                        "bundle skill {} conflicts: {} vs {}",
                        skill.name,
                        existing.display(),
                        skill.source_dir.display()
                    )));
                }
                None => {
                    skills_by_name.insert(
                        skill.name.clone(),
                        SkillSource::Path(skill.source_dir.clone()),
                    );
                    extensions.resolved_skills.push(skill.clone());
                }
            }
        }

        for (event, groups) in &contribution.hooks {
            let target = extensions.hooks.entry(event.clone()).or_default();
            for group in groups {
                if !target.iter().any(|existing| existing == group) {
                    target.push(group.clone());
                }
            }
        }

        extensions.rules.extend(contribution.rules.iter().cloned());

        for server in &contribution.mcp {
            match extensions
                .mcp
                .iter()
                .find(|existing| existing.name == server.name)
            {
                Some(existing) if existing == server => {}
                Some(_) => {
                    return Err(bundle_err(format!(
                        "bundle MCP server {} conflicts with an existing server of the same name",
                        server.name
                    )));
                }
                None => extensions.mcp.push(server.clone()),
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
enum SkillSource {
    Project,
    Path(PathBuf),
}

fn resolve_bundle_skills(
    bundle_root: &Path,
    manifest: &BundleManifest,
) -> Result<Vec<ResolvedSkill>, AhError> {
    let skills_root = bundle_root.join("skills");
    if !skills_root.exists() {
        return Ok(Vec::new());
    }
    let names = if manifest.skills.include.is_empty() {
        let mut names = fs::read_dir(&skills_root)
            .map_err(|err| bundle_err(format!("read bundle skills: {err}")))?
            .map(|entry| {
                let entry = entry.map_err(|err| bundle_err(format!("read bundle skill: {err}")))?;
                Ok(entry.file_name().to_string_lossy().to_string())
            })
            .collect::<Result<Vec<_>, AhError>>()?;
        names.sort();
        names
    } else {
        manifest.skills.include.clone()
    };

    names
        .iter()
        .map(|name| {
            validate_bundle_name(name)?;
            let source_dir = confined_path(bundle_root, &format!("skills/{name}"))?;
            if !source_dir.is_dir() {
                return Err(bundle_err(format!(
                    "bundle skill {name:?} directory not found: {}",
                    source_dir.display()
                )));
            }
            let skill_md = source_dir.join("SKILL.md");
            if !skill_md.is_file() {
                return Err(bundle_err(format!(
                    "bundle skill {name:?} missing SKILL.md: {}",
                    skill_md.display()
                )));
            }
            Ok(ResolvedSkill {
                name: name.clone(),
                source_dir,
            })
        })
        .collect()
}

fn resolve_bundle_hooks(
    bundle_root: &Path,
    hooks: &HashMap<String, Vec<HookGroup>>,
) -> Result<HashMap<String, Vec<HookGroup>>, AhError> {
    let mut resolved = HashMap::new();
    for (event, groups) in hooks {
        let mut resolved_groups = Vec::new();
        for group in groups {
            let mut group = group.clone();
            for item in &mut group.hooks {
                item.command = resolved_hook_command(bundle_root, item)?
                    .display()
                    .to_string();
            }
            resolved_groups.push(group);
        }
        resolved.insert(event.clone(), resolved_groups);
    }
    Ok(resolved)
}

fn resolved_hook_command(bundle_root: &Path, item: &HookItem) -> Result<PathBuf, AhError> {
    if item.hook_type != "command" {
        return Err(bundle_err(format!(
            "bundle hook type {:?} is unsupported in PR-1",
            item.hook_type
        )));
    }
    let path = confined_path(bundle_root, &item.command)?;
    if !path.is_file() {
        return Err(bundle_err(format!(
            "bundle hook script not found: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn resolve_bundle_rules(
    bundle_root: &Path,
    rules: &BundleRulesManifest,
    role: BundleRole,
) -> Result<Vec<String>, AhError> {
    let path = match role {
        BundleRole::Master => rules.master.as_deref(),
        BundleRole::Worker => rules.worker.as_deref(),
    };
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let path = confined_path(bundle_root, path)?;
    let content = fs::read_to_string(&path)
        .map_err(|err| bundle_err(format!("read bundle rules {}: {err}", path.display())))?;
    Ok(vec![content])
}

fn resolve_bundle_mcp(manifest: &BundleMcpManifest) -> Result<Vec<McpServerConfig>, AhError> {
    let mut servers = Vec::new();
    for server in &manifest.servers {
        validate_mcp_name(&server.name)?;
        match server.transport {
            McpTransport::Stdio => {
                if server.command.as_deref().is_none_or(str::is_empty) {
                    return Err(bundle_err(format!(
                        "bundle MCP server {:?} with stdio transport requires command",
                        server.name
                    )));
                }
                if server.url.is_some() {
                    return Err(bundle_err(format!(
                        "bundle MCP server {:?} with stdio transport must not set url",
                        server.name
                    )));
                }
            }
            McpTransport::Http | McpTransport::Sse => {
                if server.url.as_deref().is_none_or(str::is_empty) {
                    return Err(bundle_err(format!(
                        "bundle MCP server {:?} with remote transport requires url",
                        server.name
                    )));
                }
                if server.command.is_some() || !server.args.is_empty() {
                    return Err(bundle_err(format!(
                        "bundle MCP server {:?} with remote transport must not set command/args",
                        server.name
                    )));
                }
            }
        }
        for value in server.env.values().chain(server.headers.values()) {
            validate_placeholders(value)?;
        }
        servers.push(server.clone());
    }
    Ok(servers)
}

fn validate_mcp_name(name: &str) -> Result<(), AhError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(bundle_err(format!(
            "invalid bundle MCP server name {name:?}; use ASCII alphanumeric, '_' or '-'"
        )));
    }
    Ok(())
}

fn validate_placeholders(value: &str) -> Result<(), AhError> {
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(bundle_err(format!(
                "bundle MCP placeholder in {value:?} is missing closing '}}'"
            )));
        };
        let name = &after[..end];
        if name.is_empty()
            || !name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            || name.as_bytes()[0].is_ascii_digit()
        {
            return Err(bundle_err(format!(
                "invalid bundle MCP environment variable placeholder ${{{name}}}"
            )));
        }
        rest = &after[end + 1..];
    }
    Ok(())
}

fn compute_bundle_digest(
    bundle_root: &Path,
    manifest_path: &Path,
    skills: &[ResolvedSkill],
    hooks: &HashMap<String, Vec<HookGroup>>,
    rules: &[String],
) -> Result<String, AhError> {
    let mut parts = Vec::<(String, String)>::new();
    digest_file(bundle_root, manifest_path, &mut parts)?;
    for skill in skills {
        digest_tree(bundle_root, &skill.source_dir, &mut parts)?;
    }
    for groups in hooks.values() {
        for group in groups {
            for item in &group.hooks {
                digest_file(bundle_root, Path::new(&item.command), &mut parts)?;
            }
        }
    }
    for (idx, content) in rules.iter().enumerate() {
        parts.push((format!("rules-layer:{idx}"), sha256_hex(content.as_bytes())));
    }
    parts.sort_by(|left, right| left.0.cmp(&right.0));
    let json = deterministic_json(json!(parts))?;
    Ok(sha256_hex(json.as_bytes()))
}

fn digest_tree(
    bundle_root: &Path,
    root: &Path,
    parts: &mut Vec<(String, String)>,
) -> Result<(), AhError> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(&path)
            .map_err(|err| bundle_err(format!("read bundle tree {}: {err}", path.display())))?
        {
            let entry =
                entry.map_err(|err| bundle_err(format!("read bundle tree entry: {err}")))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|err| bundle_err(format!("read bundle file type: {err}")))?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                digest_file(bundle_root, &path, parts)?;
            }
        }
    }
    Ok(())
}

fn digest_file(
    bundle_root: &Path,
    path: &Path,
    parts: &mut Vec<(String, String)>,
) -> Result<(), AhError> {
    let canonical = path
        .canonicalize()
        .map_err(|err| bundle_err(format!("resolve bundle file {}: {err}", path.display())))?;
    if !canonical.starts_with(bundle_root) {
        return Err(bundle_err(format!(
            "bundle file escapes bundle root: {}",
            path.display()
        )));
    }
    let relative = canonical
        .strip_prefix(bundle_root)
        .map_err(|err| bundle_err(format!("relativize bundle file: {err}")))?
        .to_string_lossy()
        .to_string();
    let bytes = fs::read(&canonical)
        .map_err(|err| bundle_err(format!("read bundle file {}: {err}", canonical.display())))?;
    parts.push((relative, sha256_hex(&bytes)));
    Ok(())
}

fn confined_path(bundle_root: &Path, raw: &str) -> Result<PathBuf, AhError> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(bundle_err(format!("bundle path {raw:?} must be relative")));
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(bundle_err(format!(
            "bundle path {raw:?} must stay within the bundle"
        )));
    }
    let candidate = bundle_root.join(path);
    let canonical = candidate
        .canonicalize()
        .map_err(|err| bundle_err(format!("resolve bundle path {raw:?}: {err}")))?;
    if !canonical.starts_with(bundle_root) {
        return Err(bundle_err(format!(
            "bundle path {raw:?} escapes bundle root"
        )));
    }
    Ok(canonical)
}

fn validate_bundle_name(name: &str) -> Result<(), AhError> {
    if name.is_empty()
        || name.contains('\\')
        || !Path::new(name)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        || Path::new(name).components().count() != 1
    {
        return Err(bundle_err(format!(
            "invalid bundle name {name:?}; use a single directory name"
        )));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn bundle_err(details: impl Into<String>) -> AhError {
    AhError::EnvironmentNotSupported {
        details: details.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_bundle(project: &Path) {
        let root = project.join(".ah/bundles/domain");
        fs::create_dir_all(root.join("skills/doc")).unwrap();
        fs::create_dir_all(root.join("hooks")).unwrap();
        fs::create_dir_all(root.join("rules")).unwrap();
        fs::write(root.join("skills/doc/SKILL.md"), "skill").unwrap();
        fs::write(root.join("hooks/guard.sh"), "#!/bin/sh\n").unwrap();
        fs::write(root.join("rules/worker.md"), "worker").unwrap();
        fs::write(
            root.join("bundle.toml"),
            r#"
name = "domain"
version = "1"

[skills]
include = ["doc"]

[hooks]
Stop = [{ command = "hooks/guard.sh" }]

[rules]
worker = "rules/worker.md"
"#,
        )
        .unwrap();
    }

    fn write_mcp_only_bundle(project: &Path) {
        let root = project.join(".ah/bundles/mcp-only");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("bundle.toml"),
            r#"
name = "mcp-only"
version = "1"

[[mcp.servers]]
name = "context7"
transport = "stdio"
command = "npx"
args = ["-y", "@upstash/context7-mcp"]
env = { CONTEXT7_TOKEN = "${CONTEXT7_TOKEN}" }
"#,
        )
        .unwrap();
    }

    #[test]
    fn resolves_bundle_contribution_and_digest() {
        let project = tempfile::tempdir().unwrap();
        write_bundle(project.path());
        let base = ExtensionConfig {
            bundle: vec!["domain".to_string()],
            ..Default::default()
        };
        let resolved =
            resolve_bundles_for_provider(project.path(), "claude", BundleRole::Worker, &base)
                .unwrap();

        assert_eq!(resolved.extensions.resolved_skills[0].name, "doc");
        assert!(resolved.extensions.hooks.contains_key("Stop"));
        assert_eq!(resolved.extensions.rules, vec!["worker".to_string()]);
        assert_eq!(
            resolved.extensions.bundle_digest.unwrap().bundles[0].name,
            "domain"
        );
    }

    #[test]
    fn rejects_missing_bundle_with_clear_name() {
        let project = tempfile::tempdir().unwrap();
        let base = ExtensionConfig {
            bundle: vec!["missing".to_string()],
            ..Default::default()
        };
        let err = resolve_bundles_for_provider(project.path(), "claude", BundleRole::Worker, &base)
            .unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn rejects_path_escape() {
        let project = tempfile::tempdir().unwrap();
        let root = project.path().join(".ah/bundles/domain");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("bundle.toml"),
            r#"
name = "domain"
version = "1"

[rules]
worker = "../escape.md"
"#,
        )
        .unwrap();
        let base = ExtensionConfig {
            bundle: vec!["domain".to_string()],
            ..Default::default()
        };
        assert!(
            resolve_bundles_for_provider(project.path(), "claude", BundleRole::Worker, &base)
                .is_err()
        );
    }

    #[test]
    fn non_claude_provider_rejects_non_mcp_bundle_content() {
        let project = tempfile::tempdir().unwrap();
        write_bundle(project.path());
        let base = ExtensionConfig {
            bundle: vec!["domain".to_string()],
            ..Default::default()
        };
        let err =
            resolve_bundles_for_provider(project.path(), "unknown", BundleRole::Worker, &base)
                .unwrap_err();
        assert!(err.to_string().contains("includes skills"));
    }

    #[test]
    fn non_claude_provider_accepts_mcp_only_bundle() {
        let project = tempfile::tempdir().unwrap();
        write_mcp_only_bundle(project.path());
        let base = ExtensionConfig {
            bundle: vec!["mcp-only".to_string()],
            ..Default::default()
        };
        let resolved =
            resolve_bundles_for_provider(project.path(), "codex", BundleRole::Worker, &base)
                .unwrap();

        assert_eq!(resolved.extensions.mcp[0].name, "context7");
        assert_eq!(
            resolved.extensions.mcp[0].env["CONTEXT7_TOKEN"],
            "${CONTEXT7_TOKEN}"
        );
        assert!(
            !serde_json::to_string(&resolved.extensions.bundle_digest)
                .unwrap()
                .contains("super-secret-value")
        );
    }

    #[test]
    fn mcp_manifest_changes_bundle_digest_without_resolved_secret() {
        let project = tempfile::tempdir().unwrap();
        write_mcp_only_bundle(project.path());
        let base = ExtensionConfig {
            bundle: vec!["mcp-only".to_string()],
            ..Default::default()
        };
        let first =
            resolve_bundles_for_provider(project.path(), "claude", BundleRole::Worker, &base)
                .unwrap()
                .extensions
                .bundle_digest
                .unwrap();
        fs::write(
            project.path().join(".ah/bundles/mcp-only/bundle.toml"),
            r#"
name = "mcp-only"
version = "1"

[[mcp.servers]]
name = "context7"
transport = "stdio"
command = "npx"
args = ["-y", "@upstash/context7-mcp", "--verbose"]
env = { CONTEXT7_TOKEN = "${CONTEXT7_TOKEN}" }
"#,
        )
        .unwrap();
        let second =
            resolve_bundles_for_provider(project.path(), "claude", BundleRole::Worker, &base)
                .unwrap()
                .extensions
                .bundle_digest
                .unwrap();
        let serialized = serde_json::to_string(&second).unwrap();

        assert_ne!(first, second);
        assert!(serialized.contains("mcp-only"));
        assert!(!serialized.contains("secret-value"));
    }
}
