use crate::error::AhError;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginSpec {
    IdOnly(String),
    Git(GitUrlSpec),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitUrlSpec {
    pub name: String,
    pub url: String,
    pub host: String,
    pub owner: String,
    pub repo: String,
    pub reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPlugin {
    pub name: String,
    pub cache_dir: PathBuf,
}

pub fn parse_plugin_spec(raw: &str) -> Result<PluginSpec, AhError> {
    let Some((name, rest)) = raw.split_once("@git@") else {
        return Ok(PluginSpec::IdOnly(raw.to_string()));
    };
    if name.is_empty() {
        return Err(plugin_err(format!("plugin git spec has empty name: {raw}")));
    }
    let (url, reference) = match rest.rsplit_once('#') {
        Some((url, reference)) => (url, reference),
        None => (rest, "main"),
    };
    if url.is_empty() {
        return Err(plugin_err(format!("plugin git spec has empty url: {raw}")));
    }
    if reference.is_empty() {
        return Err(plugin_err(format!("plugin git spec has empty ref: {raw}")));
    }
    let (host, owner, repo) = parse_git_cache_key(url)?;
    Ok(PluginSpec::Git(GitUrlSpec {
        name: name.to_string(),
        url: url.to_string(),
        host,
        owner,
        repo,
        reference: reference.to_string(),
    }))
}

pub fn resolve_plugins_for_provider(
    provider: &str,
    source_home: &Path,
    plugins: &[String],
) -> Result<Vec<ResolvedPlugin>, AhError> {
    let mut resolved = Vec::new();
    for raw in plugins {
        match parse_plugin_spec(raw)? {
            PluginSpec::IdOnly(name) => resolved.push(resolve_id_only(provider, source_home, name)),
            PluginSpec::Git(spec) => {
                let cache_dir = clone_or_update(&spec)?;
                resolved.push(ResolvedPlugin {
                    name: spec.name,
                    cache_dir,
                });
            }
        }
    }
    Ok(resolved)
}

fn resolve_id_only(provider: &str, source_home: &Path, name: String) -> ResolvedPlugin {
    let provider_cache = match provider {
        "claude" => ".claude/plugins/cache",
        "codex" => ".codex/plugins/cache",
        _ => "",
    };
    ResolvedPlugin {
        cache_dir: source_home.join(provider_cache).join(&name),
        name,
    }
}

fn clone_or_update(spec: &GitUrlSpec) -> Result<PathBuf, AhError> {
    let target = git_cache_root()?
        .join(&spec.host)
        .join(&spec.owner)
        .join(&spec.repo)
        .join(sanitize_segment(&spec.reference).ok_or_else(|| {
            plugin_err(format!(
                "invalid git ref for plugin {}: {}",
                spec.name, spec.reference
            ))
        })?);
    if is_non_empty_dir(&target) {
        return Ok(target);
    }

    let tmp_parent = git_cache_root()?.join(".tmp");
    fs::create_dir_all(&tmp_parent).map_err(|err| {
        plugin_err(format!(
            "create git plugin tmp {}: {err}",
            tmp_parent.display()
        ))
    })?;
    let tmp = tmp_parent.join(format!(
        "{}-{}-{}",
        spec.name,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    if tmp.exists() {
        let _ = fs::remove_dir_all(&tmp);
    }

    let clone = Command::new("git")
        .args(["-c", "core.hooksPath=/dev/null", "clone", &spec.url])
        .arg(&tmp)
        .output()
        .map_err(|err| plugin_err(format!("spawn git clone for plugin {}: {err}", spec.name)))?;

    if !clone.status.success() {
        let _ = fs::remove_dir_all(&tmp);
        cleanup_empty_dir(&tmp_parent);
        let stderr = String::from_utf8_lossy(&clone.stderr);
        return Err(plugin_err(format!(
            "git clone failed for plugin {} from {}#{}: {}",
            spec.name,
            spec.url,
            spec.reference,
            stderr.trim()
        )));
    }

    if spec.reference != "main" {
        let checkout = Command::new("git")
            .current_dir(&tmp)
            .args(["checkout", &spec.reference])
            .output()
            .map_err(|err| {
                let _ = fs::remove_dir_all(&tmp);
                cleanup_empty_dir(&tmp_parent);
                plugin_err(format!(
                    "spawn git checkout for plugin {}: {err}",
                    spec.name
                ))
            })?;
        if !checkout.status.success() {
            let _ = fs::remove_dir_all(&tmp);
            cleanup_empty_dir(&tmp_parent);
            let stderr = String::from_utf8_lossy(&checkout.stderr);
            return Err(plugin_err(format!(
                "git checkout failed for plugin {} ref {}: {}",
                spec.name,
                spec.reference,
                stderr.trim()
            )));
        }
    }

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            let _ = fs::remove_dir_all(&tmp);
            cleanup_empty_dir(&tmp_parent);
            plugin_err(format!(
                "create git plugin cache parent {}: {err}",
                parent.display()
            ))
        })?;
    }
    if is_non_empty_dir(&target) {
        let _ = fs::remove_dir_all(&tmp);
        cleanup_empty_dir(&tmp_parent);
        return Ok(target);
    }
    fs::rename(&tmp, &target).map_err(|err| {
        let _ = fs::remove_dir_all(&tmp);
        cleanup_empty_dir(&tmp_parent);
        plugin_err(format!(
            "move git plugin cache {} -> {}: {err}",
            tmp.display(),
            target.display()
        ))
    })?;
    cleanup_empty_dir(&tmp_parent);
    Ok(target)
}

fn git_cache_root() -> Result<PathBuf, AhError> {
    let cache_root = match std::env::var_os("XDG_CACHE_HOME").filter(|value| !value.is_empty()) {
        Some(cache) => PathBuf::from(cache),
        None => env_home()?.join(".cache"),
    };
    Ok(cache_root.join("ah/cache/git"))
}

fn parse_git_cache_key(url: &str) -> Result<(String, String, String), AhError> {
    let without_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("ssh://"));
    let (host, path) = if let Some(rest) = without_scheme {
        let (host, path) = rest
            .split_once('/')
            .ok_or_else(|| plugin_err(format!("git url missing owner/repo path: {url}")))?;
        (host, path)
    } else if let Some((_, rest)) = url.split_once('@') {
        let (host, path) = rest
            .split_once(':')
            .ok_or_else(|| plugin_err(format!("ssh git url missing host:path: {url}")))?;
        (host, path)
    } else {
        let (host, path) = url
            .split_once(':')
            .ok_or_else(|| plugin_err(format!("git url missing host:path: {url}")))?;
        (host, path)
    };

    let mut segments = path.split('/').collect::<Vec<_>>();
    if segments.len() < 2 {
        return Err(plugin_err(format!(
            "git url path must include owner/repo: {url}"
        )));
    }
    let repo = segments.pop().unwrap().trim_end_matches(".git");
    let owner = segments.join("/");
    let Some(host) = sanitize_segment(host) else {
        return Err(plugin_err(format!("invalid git host in url: {url}")));
    };
    let Some(owner) = sanitize_path(&owner) else {
        return Err(plugin_err(format!("invalid git owner path in url: {url}")));
    };
    let Some(repo) = sanitize_segment(repo) else {
        return Err(plugin_err(format!("invalid git repo in url: {url}")));
    };
    Ok((host, owner, repo))
}

fn sanitize_path(path: &str) -> Option<String> {
    let mut clean = Vec::new();
    for segment in path.split('/') {
        clean.push(sanitize_segment(segment)?);
    }
    Some(clean.join("/"))
}

fn sanitize_segment(segment: &str) -> Option<String> {
    if segment.is_empty() || segment == "." || segment == ".." || segment.contains('\\') {
        return None;
    }
    Some(segment.to_string())
}

fn is_non_empty_dir(path: &Path) -> bool {
    path.is_dir()
        && fs::read_dir(path)
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false)
}

fn cleanup_empty_dir(path: &Path) {
    let _ = fs::remove_dir(path);
}

fn env_home() -> Result<PathBuf, AhError> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| plugin_err("HOME is not set for git plugin provisioning"))
}

fn plugin_err(details: impl Into<String>) -> AhError {
    AhError::EnvironmentNotSupported {
        details: details.into(),
    }
}
