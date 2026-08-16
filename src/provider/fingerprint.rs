use crate::error::AhError;
use crate::provider::extensions::HookGroup;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub enum ConfigRole<'a> {
    Master {
        cmd: &'a str,
        env: &'a HashMap<String, String>,
    },
    Agent {
        provider: &'a str,
        env: &'a HashMap<String, String>,
    },
}

pub struct ConfigFingerprintInput<'a> {
    pub role: ConfigRole<'a>,
    pub hooks: &'a HashMap<String, Vec<HookGroup>>,
    pub plugins: &'a [String],
    pub skills: &'a [String],
    pub settings: &'a Map<String, Value>,
    pub bundle: Option<&'a BundleDigest>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleDigest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bundles: Vec<BundleDigestEntry>,
}

impl BundleDigest {
    pub fn is_empty(&self) -> bool {
        self.bundles.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleDigestEntry {
    pub name: String,
    pub digest: String,
}

pub fn compute_config_hash(input: &ConfigFingerprintInput<'_>) -> Result<String, AhError> {
    let role = match &input.role {
        ConfigRole::Master { cmd, env } => json!({
            "kind": "master",
            "cmd": cmd,
            "env": env,
        }),
        ConfigRole::Agent { provider, env } => json!({
            "kind": "agent",
            "provider": provider,
            "env": env,
        }),
    };
    let mut plugins = input.plugins.to_vec();
    plugins.sort();
    let mut skills = input.skills.to_vec();
    skills.sort();
    let mut root = Map::new();
    root.insert("role".to_string(), role);
    root.insert("hooks".to_string(), json!(input.hooks));
    root.insert("plugins".to_string(), json!(plugins));
    root.insert("skills".to_string(), json!(skills));
    if !input.settings.is_empty() {
        root.insert(
            "settings".to_string(),
            Value::Object(input.settings.clone()),
        );
    }
    if let Some(bundle) = input.bundle.filter(|bundle| !bundle.is_empty()) {
        root.insert("bundle".to_string(), json!(bundle));
    }
    let value = Value::Object(root);
    let json = deterministic_json(value)?;
    let digest = Sha256::digest(json.as_bytes());
    Ok(format!("{digest:x}"))
}

pub fn deterministic_json(value: Value) -> Result<String, AhError> {
    serde_json::to_string(&sort_value(value))
        .map_err(|err| AhError::IpcInvalidRequest(format!("serialize config fingerprint: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bundle_digest_keeps_existing_hash_stable() {
        let env = HashMap::from([("A".to_string(), "1".to_string())]);
        let hooks = HashMap::new();
        let plugins = vec!["p".to_string()];
        let skills = vec!["s".to_string()];
        let without_bundle = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Agent {
                provider: "claude",
                env: &env,
            },
            hooks: &hooks,
            plugins: &plugins,
            skills: &skills,
            settings: &Map::new(),
            bundle: None,
        })
        .unwrap();
        let empty_bundle = BundleDigest::default();
        let with_empty_bundle = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Agent {
                provider: "claude",
                env: &env,
            },
            hooks: &hooks,
            plugins: &plugins,
            skills: &skills,
            settings: &Map::new(),
            bundle: Some(&empty_bundle),
        })
        .unwrap();

        assert_eq!(without_bundle, with_empty_bundle);
    }

    #[test]
    fn non_empty_bundle_digest_changes_hash() {
        let hooks = HashMap::new();
        let plugins = Vec::new();
        let skills = Vec::new();
        let without_bundle = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Master {
                cmd: "claude",
                env: &HashMap::new(),
            },
            hooks: &hooks,
            plugins: &plugins,
            skills: &skills,
            settings: &Map::new(),
            bundle: None,
        })
        .unwrap();
        let bundle = BundleDigest {
            bundles: vec![BundleDigestEntry {
                name: "domain".to_string(),
                digest: "abc".to_string(),
            }],
        };
        let with_bundle = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Master {
                cmd: "claude",
                env: &HashMap::new(),
            },
            hooks: &hooks,
            plugins: &plugins,
            skills: &skills,
            settings: &Map::new(),
            bundle: Some(&bundle),
        })
        .unwrap();

        assert_ne!(without_bundle, with_bundle);
    }

    #[test]
    fn bundle_hash_uses_placeholder_digest_not_resolved_secret() {
        let hooks = HashMap::new();
        let plugins = Vec::new();
        let skills = Vec::new();
        let bundle = BundleDigest {
            bundles: vec![BundleDigestEntry {
                name: "mcp".to_string(),
                digest: "manifest contains ${ACME_KEY}".to_string(),
            }],
        };
        let hash = compute_config_hash(&ConfigFingerprintInput {
            role: ConfigRole::Master {
                cmd: "claude",
                env: &HashMap::new(),
            },
            hooks: &hooks,
            plugins: &plugins,
            skills: &skills,
            settings: &Map::new(),
            bundle: Some(&bundle),
        })
        .unwrap();

        assert!(!hash.contains("secret-value"));
        assert!(!hash.contains("ACME_KEY"));
    }
}

fn sort_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = Map::new();
            let mut entries = map.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, value) in entries {
                sorted.insert(key, sort_value(value));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(sort_value).collect()),
        value => value,
    }
}
