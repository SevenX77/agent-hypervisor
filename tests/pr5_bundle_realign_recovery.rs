use ah::cli::rpc_client::{CliError, RpcClient, RpcFuture};
use ah::cli::up::{UpOptions, run_up};
use ah::db::recovery::AgentSpawnSpec;
use ah::provider::bundles::{BundleRole, digest_for_bundles, resolve_bundles_for_provider};
use ah::provider::extensions::ExtensionConfig;
use ah::provider::fingerprint::{ConfigFingerprintInput, ConfigRole, compute_config_hash};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn write_worker_bundle(project: &Path, rule: &str) {
    let root = project.join(".ah/bundles/domain");
    write(
        &root.join("bundle.toml"),
        r#"name = "domain"
version = "1"

[skills]
include = ["s"]

[rules]
worker = "rules/worker.md"
"#,
    );
    write(&root.join("skills/s/SKILL.md"), "# Skill\n");
    write(&root.join("rules/worker.md"), rule);
}

#[test]
fn bundle_content_change_realigns_agent() {
    let project = tempfile::tempdir().unwrap();
    write_worker_bundle(project.path(), "worker rules v1\n");
    let names = vec!["domain".to_string()];

    let first = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    write(
        &project.path().join(".ah/bundles/domain/rules/worker.md"),
        "worker rules v2\n",
    );
    let second = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();

    assert_ne!(first, second);
    assert_eq!(second.bundles[0].name, "domain");
}

#[test]
fn bundle_digest_covers_materialized_content() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path().join(".ah/bundles/domain");
    write(
        &root.join("bundle.toml"),
        r#"name = "domain"
version = "1"

[skills]
include = ["s"]

[hooks]
PostToolUse = [{ command = "hooks/guard.sh" }]

[rules]
worker = "rules/worker.md"
"#,
    );
    write(&root.join("skills/s/SKILL.md"), "# Skill v1\n");
    write(
        &root.join("hooks/guard.sh"),
        "#!/usr/bin/env bash\necho v1\n",
    );
    write(&root.join("rules/worker.md"), "rules v1\n");
    let names = vec!["domain".to_string()];
    let baseline = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();

    write(&root.join("skills/s/SKILL.md"), "# Skill v2\n");
    let skill_changed = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    assert_ne!(baseline, skill_changed);

    write(&root.join("skills/s/SKILL.md"), "# Skill v1\n");
    write(
        &root.join("hooks/guard.sh"),
        "#!/usr/bin/env bash\necho v2\n",
    );
    let hook_changed = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    assert_ne!(baseline, hook_changed);

    write(
        &root.join("hooks/guard.sh"),
        "#!/usr/bin/env bash\necho v1\n",
    );
    write(&root.join("rules/worker.md"), "rules v2\n");
    let rules_changed = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    assert_ne!(baseline, rules_changed);
}

#[test]
fn master_bundle_drift_is_audit_only_until_force() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path().join(".ah/bundles/domain");
    write(
        &root.join("bundle.toml"),
        r#"name = "domain"
version = "1"

[rules]
master = "rules/master.md"
"#,
    );
    write(&root.join("rules/master.md"), "master rules v1\n");
    let names = vec!["domain".to_string()];
    let hooks = HashMap::new();
    let plugins = Vec::new();
    let skills = Vec::new();
    let first_digest = digest_for_bundles(project.path(), BundleRole::Master, &names)
        .unwrap()
        .unwrap();
    let first_hash = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Master {
            cmd: "claude",
            env: &std::collections::HashMap::new(),
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &serde_json::Map::new(),
        bundle: Some(&first_digest),
    })
    .unwrap();

    write(&root.join("rules/master.md"), "master rules v2\n");
    let second_digest = digest_for_bundles(project.path(), BundleRole::Master, &names)
        .unwrap()
        .unwrap();
    let second_hash = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Master {
            cmd: "claude",
            env: &std::collections::HashMap::new(),
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &serde_json::Map::new(),
        bundle: Some(&second_digest),
    })
    .unwrap();

    assert_ne!(first_hash, second_hash);
}

#[test]
fn crash_recovery_rematerializes_current_bundle() {
    let project = tempfile::tempdir().unwrap();
    write_worker_bundle(project.path(), "worker rules v1\n");
    let names = vec!["domain".to_string()];
    let env = HashMap::new();
    let hooks = HashMap::new();
    let plugins = Vec::new();
    let skills = Vec::new();

    let first_digest = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    let first_hash = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Agent {
            provider: "claude",
            env: &env,
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &serde_json::Map::new(),
        bundle: Some(&first_digest),
    })
    .unwrap();

    write(
        &project.path().join(".ah/bundles/domain/rules/worker.md"),
        "worker rules v2\n",
    );
    let second_digest = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    let second_hash = compute_config_hash(&ConfigFingerprintInput {
        role: ConfigRole::Agent {
            provider: "claude",
            env: &env,
        },
        hooks: &hooks,
        plugins: &plugins,
        skills: &skills,
        settings: &serde_json::Map::new(),
        bundle: Some(&second_digest),
    })
    .unwrap();

    assert_ne!(first_hash, second_hash);
}

#[test]
fn master_revive_reprovisions_current_bundle_worker() {
    let project = tempfile::tempdir().unwrap();
    write_worker_bundle(project.path(), "worker rules v1\n");
    let names = vec!["domain".to_string()];
    let stale_digest = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    write(
        &project.path().join(".ah/bundles/domain/rules/worker.md"),
        "worker rules after master revive\n",
    );
    let current_digest = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();

    assert_ne!(stale_digest, current_digest);
    assert_eq!(current_digest.bundles[0].name, "domain");
}

#[test]
fn recovery_bundle_snapshot() {
    let project = tempfile::tempdir().unwrap();
    write_worker_bundle(project.path(), "worker rules v1\n");
    let names = vec!["domain".to_string()];
    let digest = digest_for_bundles(project.path(), BundleRole::Worker, &names)
        .unwrap()
        .unwrap();
    let mut settings = serde_json::Map::new();
    settings.insert(
        "model".to_string(),
        Value::String("claude-sonnet-4-20250514".to_string()),
    );
    let spec = AgentSpawnSpec {
        agent_id: "a1".to_string(),
        provider: "claude".to_string(),
        env: HashMap::new(),
        hooks: HashMap::new(),
        plugins: Vec::new(),
        skills: Vec::new(),
        bundle: names,
        settings: settings.clone(),
        bundle_digest: Some(digest.clone()),
        sandbox_overrides: Default::default(),
        hook_push_enabled: true,
    };

    let json = serde_json::to_string(&spec).unwrap();
    let restored: AgentSpawnSpec = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.bundle, vec!["domain"]);
    assert_eq!(restored.settings, settings);
    assert_eq!(restored.bundle_digest, Some(digest));
    assert!(restored.hook_push_enabled);

    let old_spec_json = r#"{
        "agent_id": "a1",
        "provider": "claude"
    }"#;
    let old_spec: AgentSpawnSpec = serde_json::from_str(old_spec_json).unwrap();
    assert!(old_spec.bundle.is_empty());
    assert!(old_spec.settings.is_empty());
    assert!(old_spec.bundle_digest.is_none());
}

#[test]
fn provider_capability_checks_use_current_bundle_shape() {
    let project = tempfile::tempdir().unwrap();
    write(
        &project.path().join(".ah/bundles/domain/bundle.toml"),
        r#"name = "domain"
version = "1"

[[mcp.servers]]
name = "ctx"
transport = "stdio"
command = "npx"
"#,
    );
    let base = ExtensionConfig {
        bundle: vec!["domain".to_string()],
        ..Default::default()
    };
    resolve_bundles_for_provider(project.path(), "codex", BundleRole::Master, &base).unwrap();

    write(
        &project.path().join(".ah/bundles/domain/bundle.toml"),
        r#"name = "domain"
version = "1"

[rules]
master = "rules/master.md"
"#,
    );
    write(
        &project.path().join(".ah/bundles/domain/rules/master.md"),
        "master rules now unsupported for codex\n",
    );

    let err = resolve_bundles_for_provider(project.path(), "codex", BundleRole::Master, &base)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("codex master bundle rules are unsupported")
    );
}

#[tokio::test]
async fn ah_up_payload_preserves_bundle_refs_for_daemon_digest() {
    let project = tempfile::tempdir().unwrap();
    let shared_credentials_dir = project.path().join("shared-claude-credentials");
    std::fs::create_dir_all(&shared_credentials_dir).unwrap();
    write(
        &project.path().join("ah.toml"),
        &format!(
            r#"version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[master]
bundle = ["master-bundle"]

[agents.a1]
provider = "claude"
bundle = ["worker-bundle"]
"#,
            shared_credentials_dir.display()
        ),
    );
    let client = RecordingClient {
        calls: Mutex::new(Vec::new()),
        sessions: json!({
            "sessions": [{
                "id": "sess_pr5",
                "status": "ACTIVE",
                "absolute_path": project.path().display().to_string()
            }]
        }),
    };

    run_up(
        &client,
        UpOptions {
            config_path: Some(project.path().join("ah.toml")),
            cwd: project.path().to_path_buf(),
            force: false,
        },
    )
    .await
    .unwrap();

    let calls = client.calls.lock().unwrap();
    let (_, params) = calls
        .iter()
        .find(|(method, _)| method == "session.realign")
        .expect("session.realign should be called");
    assert_eq!(params["master"]["bundle"], json!(["master-bundle"]));
    assert_eq!(params["agents"][0]["bundle"], json!(["worker-bundle"]));
    assert!(params["master"].get("bundle_digest").is_none());
    assert!(params["agents"][0].get("bundle_digest").is_none());
}

struct RecordingClient {
    calls: Mutex<Vec<(String, Value)>>,
    sessions: Value,
}

impl RpcClient for RecordingClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_string(), params));
            match method {
                "session.list" => Ok(self.sessions.clone()),
                "session.realign" => Ok(json!({ "status": "NO_CHANGE", "results": [] })),
                other => Err(CliError::InvalidResponse(format!(
                    "unexpected RPC method: {other}"
                ))),
            }
        })
    }
}
