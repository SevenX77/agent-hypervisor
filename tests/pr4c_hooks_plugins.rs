#![cfg(unix)]

use ah::cli::rpc_client::{CliError, RpcClient, RpcFuture};
use ah::cli::start::{StartOptions, start_from_options};
use ah::home_materialization::{
    HookPushContext, build_ah_hook_command, prepare_home_layout_with_extensions,
    prepare_home_layout_with_extensions_for_slot_and_claude_credentials,
};
use ah::provider::extensions::{ExtensionConfig, HookGroup, HookItem};
use ah::provider::manifest::{collect_spawn_env, get_manifest};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn claude_hook_materializes_settings_and_symlink() {
    let fixture = HostFixture::new();
    let script = fixture.write_host_file("scripts/audit.sh", "#!/bin/sh\nexit 0\n");
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &hooks_config("PreToolUse", script.to_str().unwrap()),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    let hook = &settings["hooks"]["PreToolUse"][0];

    assert_eq!(hook["matcher"], "*");
    assert_eq!(hook["hooks"][0]["type"], "command");
    assert_eq!(
        hook["hooks"][0]["command"],
        layout
            .home_root
            .join(".claude/hooks/audit.sh")
            .display()
            .to_string()
    );
    assert_eq!(
        std::fs::read_link(layout.home_root.join(".claude/hooks/audit.sh")).unwrap(),
        script
    );
}

#[test]
fn hook_push_context_extends_prepare_home_layout_signature() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let socket = sandbox.path().join("state/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_hook_ctx".to_string(),
        provider: "claude".to_string(),
        lifecycle_id: "lifecycle-ag-hook-ctx".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &ExtensionConfig::default(),
        Some(&hook_ctx),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    let command = settings["hooks"]["Stop"][0]["hooks"][0]["command"]
        .as_str()
        .unwrap();

    assert!(command.contains("agent notify --agent-id"));
    assert!(command.contains("--agent-id ag_hook_ctx"));
    assert!(command.contains(&format!("--socket {}", socket.display())));
    assert!(command.contains("--hook-json"));
    assert!(command.contains("--hook-debug-log"));
    assert!(command.contains("/hooks-debug/ag_hook_ctx.log"));
}

#[test]
fn hook_push_disabled_context_does_not_inject_ah_hook() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let hook_ctx = HookPushContext {
        agent_id: "ag_hook_ctx_off".to_string(),
        provider: "claude".to_string(),
        lifecycle_id: "lifecycle-ag-hook-ctx-off".to_string(),
        ahd_socket_path: sandbox.path().join("state/ahd.sock"),
        enabled: false,
    };

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &ExtensionConfig::default(),
        Some(&hook_ctx),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));

    assert!(
        settings["hooks"]["Stop"].is_null(),
        "flag-off path must not inject ah-owned Stop hook: {settings:?}"
    );
}

#[test]
fn hook_push_command_bakes_agent_id_socket_and_ccb_socket() {
    let socket = PathBuf::from("/tmp/ah-hook-push/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_cmd".to_string(),
        provider: "codex".to_string(),
        lifecycle_id: "lifecycle-ag-cmd".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let item = build_ah_hook_command(&hook_ctx, "stop");

    assert_eq!(item.hook_type, "command");
    assert_eq!(item.timeout, Some(5));
    assert!(
        item.command
            .contains(&format!("CCB_SOCKET={}", socket.display()))
    );
    assert!(item.command.contains("agent notify --agent-id"));
    assert!(item.command.contains("--agent-id ag_cmd"));
    assert!(item.command.contains("--event stop"));
    assert!(item.command.contains("--provider codex"));
    assert!(
        item.command
            .contains(&format!("--socket {}", socket.display()))
    );
    assert!(item.command.contains("--hook-json"));
    assert!(item.command.contains("--hook-debug-log"));
    assert!(item.command.contains("/hooks-debug/ag_cmd.log"));
}

#[test]
fn hook_push_command_uses_provider_timeout_units_and_hook_json() {
    let socket = PathBuf::from("/tmp/ah-hook-push/state/ahd.sock");

    for (provider, timeout) in [("codex", 5), ("claude", 5), ("antigravity", 5)] {
        let hook_ctx = HookPushContext {
            agent_id: format!("ag_{provider}"),
            provider: provider.to_string(),
            lifecycle_id: format!("lifecycle-ag-{provider}"),
            ahd_socket_path: socket.clone(),
            enabled: true,
        };

        let item = build_ah_hook_command(&hook_ctx, "Stop");

        assert_eq!(item.timeout, Some(timeout), "{provider}");
        assert!(
            item.command.contains("agent notify --agent-id"),
            "{provider}"
        );
        assert!(item.command.contains("--hook-json"), "{provider}");
        assert!(item.command.contains("--hook-debug-log"), "{provider}");
    }
}

#[test]
fn codex_plugin_materializes_config_and_cache() {
    let fixture = HostFixture::new();
    let host_plugin = fixture
        .host_home()
        .join(".codex/plugins/cache/github@openai-curated");
    std::fs::create_dir_all(&host_plugin).unwrap();
    std::fs::write(host_plugin.join("plugin.json"), "{}\n").unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions(
        "codex",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &plugins_config(["github@openai-curated"]),
        None,
    )
    .unwrap();
    let config: toml::Value = std::fs::read_to_string(layout.home_root.join(".codex/config.toml"))
        .unwrap()
        .parse()
        .unwrap();

    assert_eq!(
        config["plugins"]["github@openai-curated"]["enabled"].as_bool(),
        Some(true)
    );
    assert_eq!(
        std::fs::read_link(
            layout
                .home_root
                .join(".codex/plugins/cache/github@openai-curated")
        )
        .unwrap(),
        host_plugin
    );
}

#[test]
fn hook_script_emits_permission_decision_protocol() {
    let _fixture = HostFixture::new();
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("ahd.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0_u8; 128];
        let _ = stream.read(&mut buffer).unwrap();
    });

    let old_socket = std::env::var_os("CCB_SOCKET");
    unsafe {
        std::env::set_var("CCB_SOCKET", &socket_path);
    }
    let env = collect_spawn_env(&get_manifest("claude"), &Default::default());
    restore_env("CCB_SOCKET", old_socket.as_ref());

    assert!(
        env.iter()
            .any(|(key, value)| key == "CCB_SOCKET" && value == socket_path.to_str().unwrap()),
        "provider env must pass CCB_SOCKET through so hooks can reach ccbd"
    );

    let stdout = json!({
        "hookSpecificOutput": {
            "permissionDecision": "allow",
            "permissionDecisionReason": "socket reachable"
        }
    });
    assert_eq!(stdout["hookSpecificOutput"]["permissionDecision"], "allow");
    drop(handle);
}

#[tokio::test]
async fn config_start_payload_forwards_hooks_and_plugins() {
    let fixture = HostFixture::new();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    fixture.write_host_file(
        "ah.toml",
        &format!(
            r#"
version = "1"

[providers.claude]
shared_credentials_dir = "{}"

[master]
enabled = true
plugins = ["claude-audit"]

[master.hooks]
PreToolUse = ["./scripts/master-audit.sh"]

[agents.a1]
provider = "claude"
plugins = ["github@openai-curated"]

[agents.a1.hooks]
PreToolUse = ["./scripts/audit.sh"]
"#,
            shared_credentials_dir.path().display()
        ),
    );
    let client = RecordingClient::default();

    start_from_options(
        &client,
        StartOptions {
            config_path: Some(fixture.host_home().join("ah.toml")),
            cwd: fixture.host_home().to_path_buf(),
            wait: false,
        },
    )
    .await
    .unwrap();

    let calls = client.calls.lock().unwrap();
    let master = calls
        .iter()
        .find(|(method, _)| method == "session.spawn_master_pane")
        .unwrap();
    let agent = calls
        .iter()
        .find(|(method, _)| method == "agent.spawn")
        .unwrap();

    assert!(master.1.get("hooks").is_some());
    assert!(master.1.get("plugins").is_some());
    assert!(agent.1.get("hooks").is_some());
    assert!(agent.1.get("plugins").is_some());
}

#[test]
fn claude_hook_push_injection_preserves_user_hooks() {
    let fixture = HostFixture::new();
    let script = fixture.write_host_file("scripts/user-stop.sh", "#!/bin/sh\nexit 0\n");
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let socket = sandbox.path().join("state/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_claude_push".to_string(),
        provider: "claude".to_string(),
        lifecycle_id: "lifecycle-ag-claude-push".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &hooks_config("Stop", script.to_str().unwrap()),
        Some(&hook_ctx),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));
    let stop_hooks = settings["hooks"]["Stop"].as_array().unwrap();
    let commands = collect_claude_commands(stop_hooks);

    assert!(
        commands
            .iter()
            .any(|command| command.ends_with(".claude/hooks/user-stop.sh")),
        "user Stop hook must be preserved: {commands:?}"
    );
    assert!(
        commands.iter().any(|command| {
            command.contains("agent notify --agent-id")
                && command.contains("--agent-id ag_claude_push")
                && command.contains(&format!("--socket {}", socket.display()))
        }),
        "ah-owned Stop hook must be injected: {commands:?}"
    );
}

#[test]
fn f3_claude_hook_push_injection_is_idempotent() {
    let fixture = HostFixture::new();
    let script = fixture.write_host_file("scripts/user-stop.sh", "#!/bin/sh\nexit 0\n");
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let hook_ctx = HookPushContext {
        agent_id: "ag_claude_idempotent".to_string(),
        provider: "claude".to_string(),
        lifecycle_id: "lifecycle-ag-claude-idempotent".to_string(),
        ahd_socket_path: sandbox.path().join("state/ahd.sock"),
        enabled: true,
    };
    let config = hooks_config("Stop", script.to_str().unwrap());

    let first = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &config,
        Some(&hook_ctx),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let second = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &config,
        Some(&hook_ctx),
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    assert_eq!(first.home_root, second.home_root);
    let settings = read_json(&second.home_root.join(".claude/settings.json"));
    let commands = collect_claude_commands(settings["hooks"]["Stop"].as_array().unwrap());
    assert_eq!(count_ah_notify_commands(&commands), 1);
    assert!(
        commands
            .iter()
            .any(|command| command.ends_with(".claude/hooks/user-stop.sh")),
        "user Stop hook must be preserved: {commands:?}"
    );
}

#[test]
fn codex_hook_push_injection_writes_hooks_json_and_feature_flag() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let socket = sandbox.path().join("state/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_codex_push".to_string(),
        provider: "codex".to_string(),
        lifecycle_id: "lifecycle-ag-codex-push".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let layout = prepare_home_layout_with_extensions(
        "codex",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();
    let config: toml::Value = std::fs::read_to_string(layout.home_root.join(".codex/config.toml"))
        .unwrap()
        .parse()
        .unwrap();
    let hooks_json = read_json(&layout.home_root.join(".codex/hooks.json"));
    let hooks_text = hooks_json.to_string();

    assert_eq!(config["features"]["hooks"].as_bool(), Some(true));
    assert!(
        config
            .get("features")
            .and_then(|features| features.get("codex_hooks"))
            .is_none(),
        "deprecated codex_hooks feature must not be written: {config:?}"
    );
    assert!(
        hooks_json["hooks"]["Stop"].is_array(),
        "Stop hook must be present"
    );
    assert!(
        hooks_json["hooks"]["UserPromptSubmit"].is_array(),
        "UserPromptSubmit hook must provide provider-native turn-start evidence"
    );
    assert!(hooks_text.contains("agent notify --agent-id"));
    assert!(hooks_text.contains("--agent-id ag_codex_push"));
    assert!(hooks_text.contains(&format!("--socket {}", socket.display())));
    assert!(hooks_text.contains("--hook-json"));
    assert!(hooks_text.contains("--hook-debug-log"));
}

#[test]
fn codex_hook_push_injection_preserves_user_hooks_json() {
    let fixture = HostFixture::new();
    std::fs::create_dir_all(fixture.host_home().join(".codex")).unwrap();
    std::fs::write(
        fixture.host_home().join(".codex/hooks.json"),
        serde_json::to_string_pretty(&json!({
            "hooks": {
                "Stop": [{
                    "hooks": [{
                        "type": "command",
                        "command": "/usr/local/bin/user-stop"
                    }]
                }],
                "PreToolUse": [{
                    "matcher": "Bash",
                    "hooks": [{
                        "type": "command",
                        "command": "/usr/local/bin/user-pre"
                    }]
                }]
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let socket = sandbox.path().join("state/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_codex_merge".to_string(),
        provider: "codex".to_string(),
        lifecycle_id: "lifecycle-ag-codex-merge".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let layout = prepare_home_layout_with_extensions(
        "codex",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();
    let hooks_json = read_json(&layout.home_root.join(".codex/hooks.json"));
    let stop_hooks = hooks_json["hooks"]["Stop"].as_array().unwrap();
    let hooks_text = hooks_json.to_string();

    assert!(hooks_text.contains("/usr/local/bin/user-stop"));
    assert!(hooks_text.contains("/usr/local/bin/user-pre"));
    assert!(
        stop_hooks.len() >= 2,
        "ah-owned Stop hook must be appended, not replace user Stop hooks: {stop_hooks:?}"
    );
    assert!(hooks_text.contains("--agent-id ag_codex_merge"));
    assert!(hooks_text.contains(&format!("--socket {}", socket.display())));
}

#[test]
fn f3_codex_hook_push_injection_is_idempotent() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let hook_ctx = HookPushContext {
        agent_id: "ag_codex_idempotent".to_string(),
        provider: "codex".to_string(),
        lifecycle_id: "lifecycle-ag-codex-idempotent".to_string(),
        ahd_socket_path: sandbox.path().join("state/ahd.sock"),
        enabled: true,
    };

    let first = prepare_home_layout_with_extensions(
        "codex",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();
    let second = prepare_home_layout_with_extensions(
        "codex",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();

    assert_eq!(first.home_root, second.home_root);
    let hooks_json = read_json(&second.home_root.join(".codex/hooks.json"));
    for event in ["UserPromptSubmit", "Stop"] {
        let commands = collect_claude_commands(hooks_json["hooks"][event].as_array().unwrap());
        assert_eq!(count_ah_notify_commands(&commands), 1, "event={event}");
    }
}

#[test]
fn antigravity_hook_push_injection_writes_global_named_stop_hook_and_preserves_settings() {
    let fixture = HostFixture::new();
    let source_agy = fixture.host_home().join(".gemini/antigravity-cli");
    std::fs::create_dir_all(&source_agy).unwrap();
    std::fs::write(
        source_agy.join("settings.json"),
        serde_json::to_string_pretty(&json!({
            "colorScheme": "dark",
            "model": "gemini-3-pro",
            "trustedWorkspaces": ["/home/agent", "/already/trusted"]
        }))
        .unwrap(),
    )
    .unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let socket = sandbox.path().join("state/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_antigravity_push".to_string(),
        provider: "antigravity".to_string(),
        lifecycle_id: "lifecycle-ag-antigravity-push".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let layout = prepare_home_layout_with_extensions(
        "antigravity",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();
    let hooks_json = read_json(&layout.home_root.join(".gemini/config/hooks.json"));
    let settings = read_json(
        &layout
            .home_root
            .join(".gemini/antigravity-cli/settings.json"),
    );
    let hook = &hooks_json["ah-completion-push"]["Stop"][0];
    let invocation_hook = &hooks_json["ah-completion-push"]["PreInvocation"][0];
    let command = hook["command"].as_str().unwrap();

    assert!(hook.get("matcher").is_none());
    assert!(invocation_hook.get("matcher").is_none());
    assert_eq!(hook["type"], "command");
    assert_eq!(hook["timeout"], 5);
    assert!(command.contains("CCB_SOCKET="));
    assert!(command.contains("agent notify --agent-id"));
    assert!(command.contains("--agent-id ag_antigravity_push"));
    assert!(command.contains("--event stop"));
    assert!(command.contains("--provider antigravity"));
    assert!(command.contains(&format!("--socket {}", socket.display())));
    assert!(command.contains("--hook-json"));
    assert!(command.contains("--hook-debug-log"));
    assert!(
        invocation_hook["command"]
            .as_str()
            .unwrap()
            .contains("--event preinvocation")
    );
    assert!(command.contains("/hooks-debug/ag_antigravity_push.log"));
    assert_eq!(settings["colorScheme"], "dark");
    assert_eq!(settings["model"], "gemini-3-pro");
    assert!(
        settings["trustedWorkspaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_str() == Some("/already/trusted"))
    );
    assert!(
        settings["trustedWorkspaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_str() == Some(&workspace.path().display().to_string()))
    );
}

#[test]
fn antigravity_hook_push_disabled_context_does_not_write_hooks_json() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let hook_ctx = HookPushContext {
        agent_id: "ag_antigravity_off".to_string(),
        provider: "antigravity".to_string(),
        lifecycle_id: "lifecycle-ag-antigravity-off".to_string(),
        ahd_socket_path: sandbox.path().join("state/ahd.sock"),
        enabled: false,
    };

    let layout = prepare_home_layout_with_extensions(
        "antigravity",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();

    assert!(
        !layout.home_root.join(".gemini/config/hooks.json").exists(),
        "flag-off path must not inject antigravity hooks.json"
    );
}

#[test]
fn antigravity_hook_push_injection_preserves_user_hooks_json() {
    let fixture = HostFixture::new();
    std::fs::create_dir_all(fixture.host_home().join(".gemini/config")).unwrap();
    std::fs::write(
        fixture.host_home().join(".gemini/config/hooks.json"),
        serde_json::to_string_pretty(&json!({
            "user-completion": {
                "Stop": [{
                    "type": "command",
                    "command": "/usr/local/bin/user-agy-stop",
                    "timeout": 9
                }],
                "PreToolUse": [{
                    "matcher": "Bash",
                    "hooks": [{
                        "type": "command",
                        "command": "/usr/local/bin/user-agy-pre"
                    }]
                }]
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let socket = sandbox.path().join("state/ahd.sock");
    let hook_ctx = HookPushContext {
        agent_id: "ag_antigravity_merge".to_string(),
        provider: "antigravity".to_string(),
        lifecycle_id: "lifecycle-ag-antigravity-merge".to_string(),
        ahd_socket_path: socket.clone(),
        enabled: true,
    };

    let layout = prepare_home_layout_with_extensions(
        "antigravity",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();
    let hooks_json = read_json(&layout.home_root.join(".gemini/config/hooks.json"));
    let hooks_text = hooks_json.to_string();

    assert!(hooks_text.contains("/usr/local/bin/user-agy-stop"));
    assert!(hooks_text.contains("/usr/local/bin/user-agy-pre"));
    assert!(hooks_text.contains("--agent-id ag_antigravity_merge"));
    assert!(hooks_text.contains(&format!("--socket {}", socket.display())));
    assert!(
        hooks_json["ah-completion-push"]["Stop"].is_array(),
        "ah-owned Stop named hook must be appended beside user named hooks: {hooks_json:?}"
    );
    assert!(
        hooks_json["ah-completion-push"]["PreInvocation"].is_array(),
        "ah-owned PreInvocation hook must provide provider-native working evidence"
    );
}

#[test]
fn f3_antigravity_hook_push_injection_is_idempotent() {
    let fixture = HostFixture::new();
    std::fs::create_dir_all(fixture.host_home().join(".gemini/config")).unwrap();
    std::fs::write(
        fixture.host_home().join(".gemini/config/hooks.json"),
        serde_json::to_string_pretty(&json!({
            "user-completion": {
                "Stop": [{
                    "type": "command",
                    "command": "/usr/local/bin/user-agy-stop"
                }]
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let hook_ctx = HookPushContext {
        agent_id: "ag_antigravity_idempotent".to_string(),
        provider: "antigravity".to_string(),
        lifecycle_id: "lifecycle-ag-antigravity-idempotent".to_string(),
        ahd_socket_path: sandbox.path().join("state/ahd.sock"),
        enabled: true,
    };

    let first = prepare_home_layout_with_extensions(
        "antigravity",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();
    let second = prepare_home_layout_with_extensions(
        "antigravity",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &ExtensionConfig::default(),
        Some(&hook_ctx),
    )
    .unwrap();

    assert_eq!(first.home_root, second.home_root);
    let hooks_json = read_json(&second.home_root.join(".gemini/config/hooks.json"));
    for event in ["PreInvocation", "Stop"] {
        let commands =
            collect_direct_commands(hooks_json["ah-completion-push"][event].as_array().unwrap());
        assert_eq!(count_ah_notify_commands(&commands), 1, "event={event}");
    }
    assert!(
        hooks_json
            .to_string()
            .contains("/usr/local/bin/user-agy-stop")
    );
}

#[test]
fn claude_plugin_materializes_enabled_plugins() {
    let fixture = HostFixture::new();
    let host_plugin = fixture
        .host_home()
        .join(".claude/plugins/cache/claude-audit");
    std::fs::create_dir_all(&host_plugin).unwrap();
    std::fs::write(host_plugin.join("plugin.json"), "{}\n").unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &plugins_config(["claude-audit"]),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();
    let settings = read_json(&layout.home_root.join(".claude/settings.json"));

    assert_eq!(settings["enabledPlugins"]["claude-audit"], true);
    assert_eq!(
        std::fs::read_link(layout.home_root.join(".claude/plugins/cache/claude-audit")).unwrap(),
        host_plugin
    );
}

#[test]
fn provider_home_layout_without_skills_does_not_create_skills_dir() {
    let _fixture = HostFixture::new();

    for (provider, relative) in [
        ("claude", ".claude/skills"),
        ("codex", ".codex/skills"),
        ("antigravity", ".gemini/config/skills"),
    ] {
        let sandbox = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();

        let shared_credentials_dir = tempfile::tempdir().unwrap();
        let layout = if provider == "claude" {
            prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
                provider,
                sandbox.path(),
                workspace.path(),
                ah::home_materialization::HomeLayoutRole::Worker,
                "worker",
                &ExtensionConfig::default(),
                None,
                Some(shared_credentials_dir.path()),
            )
            .unwrap()
        } else {
            prepare_home_layout_with_extensions(
                provider,
                sandbox.path(),
                workspace.path(),
                ah::home_materialization::HomeLayoutRole::Worker,
                &ExtensionConfig::default(),
                None,
            )
            .unwrap()
        };

        assert!(
            !layout.home_root.join(relative).exists(),
            "default no-skills path must not create {relative} for {provider}"
        );
    }
}

#[test]
fn claude_skill_materializes_project_skill_symlink() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let shared_credentials_dir = tempfile::tempdir().unwrap();
    let skill_dir = workspace.path().join(".ah/skills/domain-review");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: domain-review\ndescription: Domain review\n---\n",
    )
    .unwrap();

    let layout = prepare_home_layout_with_extensions_for_slot_and_claude_credentials(
        "claude",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        "worker",
        &skills_config(["domain-review"]),
        None,
        Some(shared_credentials_dir.path()),
    )
    .unwrap();

    assert_eq!(
        std::fs::read_link(layout.home_root.join(".claude/skills/domain-review")).unwrap(),
        skill_dir.canonicalize().unwrap()
    );
}

#[test]
fn codex_skill_materializes_project_skill_symlink() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let skill_dir = workspace.path().join(".ah/skills/domain-review");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: domain-review\ndescription: Domain review\n---\n",
    )
    .unwrap();

    let layout = prepare_home_layout_with_extensions(
        "codex",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &skills_config(["domain-review"]),
        None,
    )
    .unwrap();

    assert_eq!(
        std::fs::read_link(layout.home_root.join(".codex/skills/domain-review")).unwrap(),
        skill_dir.canonicalize().unwrap()
    );
}

#[test]
fn antigravity_skill_materializes_project_skill_symlink() {
    let _fixture = HostFixture::new();
    let sandbox = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let skill_dir = workspace.path().join(".ah/skills/domain-review");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: domain-review\n---\n",
    )
    .unwrap();

    let layout = prepare_home_layout_with_extensions(
        "antigravity",
        sandbox.path(),
        workspace.path(),
        ah::home_materialization::HomeLayoutRole::Worker,
        &skills_config(["domain-review"]),
        None,
    )
    .unwrap();

    assert_eq!(
        std::fs::read_link(layout.home_root.join(".gemini/config/skills/domain-review")).unwrap(),
        skill_dir.canonicalize().unwrap()
    );
}

fn hooks_config(event: &str, command: &str) -> ExtensionConfig {
    ExtensionConfig {
        hooks: HashMap::from([(
            event.to_string(),
            vec![HookGroup {
                matcher: "*".to_string(),
                hooks: vec![HookItem {
                    hook_type: "command".to_string(),
                    command: command.to_string(),
                    timeout: None,
                }],
            }],
        )]),
        plugins: Vec::new(),
        skills: Vec::new(),
        ..Default::default()
    }
}

fn plugins_config<const N: usize>(plugins: [&str; N]) -> ExtensionConfig {
    ExtensionConfig {
        hooks: HashMap::new(),
        plugins: plugins.into_iter().map(str::to_string).collect(),
        skills: Vec::new(),
        ..Default::default()
    }
}

fn skills_config<const N: usize>(skills: [&str; N]) -> ExtensionConfig {
    ExtensionConfig {
        hooks: HashMap::new(),
        plugins: Vec::new(),
        skills: skills.into_iter().map(str::to_string).collect(),
        ..Default::default()
    }
}

fn collect_claude_commands(hooks: &[Value]) -> Vec<String> {
    hooks
        .iter()
        .flat_map(|group| group["hooks"].as_array().into_iter().flatten())
        .filter_map(|item| item["command"].as_str().map(str::to_string))
        .collect()
}

fn collect_direct_commands(hooks: &[Value]) -> Vec<String> {
    hooks
        .iter()
        .filter_map(|item| item["command"].as_str().map(str::to_string))
        .collect()
}

fn count_ah_notify_commands(commands: &[String]) -> usize {
    commands
        .iter()
        .filter(|command| command.contains("agent notify --agent-id"))
        .count()
}

#[derive(Default)]
struct RecordingClient {
    calls: Mutex<Vec<(String, Value)>>,
}

impl RpcClient for RecordingClient {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_string(), params));
            match method {
                "session.list" => Ok(json!({ "sessions": [] })),
                "session.create" => Ok(json!({ "session_id": "sess_pr4c" })),
                "session.spawn_master_pane" => Ok(json!({ "pane_id": "%0" })),
                "agent.spawn" => Ok(json!({
                    "state": "SPAWNING",
                    "pid": 123,
                    "pane_id": "%1"
                })),
                other => Err(CliError::InvalidResponse(format!(
                    "unexpected method in recording client: {other}"
                ))),
            }
        })
    }
}

struct HostFixture {
    _lock: MutexGuard<'static, ()>,
    _host_home: tempfile::TempDir,
    _cache_home: tempfile::TempDir,
    host_home_path: PathBuf,
    old_home: Option<OsString>,
    old_cache: Option<OsString>,
    old_user: Option<OsString>,
}

impl HostFixture {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let host_home = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(host_home.path().join(".claude")).unwrap();
        std::fs::create_dir_all(host_home.path().join(".codex/plugins/cache")).unwrap();
        std::fs::create_dir_all(host_home.path().join(".gemini")).unwrap();
        std::fs::write(host_home.path().join(".claude.json"), "{}\n").unwrap();
        std::fs::write(
            host_home.path().join(".gemini/settings.json"),
            serde_json::to_string_pretty(&json!({
                "security": {"auth": {"selectedType": "oauth-personal"}}
            }))
            .unwrap(),
        )
        .unwrap();

        let old_home = std::env::var_os("HOME");
        let old_cache = std::env::var_os("XDG_CACHE_HOME");
        let old_user = std::env::var_os("USER");
        unsafe {
            std::env::set_var("HOME", host_home.path());
            std::env::set_var("XDG_CACHE_HOME", cache_home.path());
            std::env::set_var("USER", "__ccbd_pr4c_no_passwd_user__");
        }
        let host_home_path = host_home.path().to_path_buf();
        Self {
            _lock: lock,
            _host_home: host_home,
            _cache_home: cache_home,
            host_home_path,
            old_home,
            old_cache,
            old_user,
        }
    }

    fn host_home(&self) -> &Path {
        &self.host_home_path
    }

    fn write_host_file(&self, relative: &str, contents: &str) -> PathBuf {
        let path = self.host_home_path.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for HostFixture {
    fn drop(&mut self) {
        restore_env("HOME", self.old_home.as_ref());
        restore_env("XDG_CACHE_HOME", self.old_cache.as_ref());
        restore_env("USER", self.old_user.as_ref());
    }
}

fn restore_env(key: &str, old_value: Option<&OsString>) {
    unsafe {
        match old_value {
            Some(old_value) => std::env::set_var(key, old_value),
            None => std::env::remove_var(key),
        }
    }
}

fn read_json(path: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}
