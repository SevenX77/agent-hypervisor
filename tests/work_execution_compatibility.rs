use std::path::Path;
use std::process::Command;

#[test]
fn legacy_home_layout_public_path_remains_available() {
    let legacy = ah::provider::home_layout::provider_home_env("codex", Path::new("/tmp/home"));
    let current = ah::home_materialization::provider_home_env("codex", Path::new("/tmp/home"));
    assert_eq!(legacy, current);
}

#[test]
fn legacy_cli_surface_remains_present_with_additive_execution_flags() {
    let help = Command::new(env!("CARGO_BIN_EXE_ah"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    for command in [
        "ping", "version", "ps", "status", "start", "up", "ask", "tell", "pend", "cancel", "kill",
        "watch", "events", "logs", "attach", "stop", "reclaim", "doctor", "setup", "config",
        "bundle", "prompt", "master", "agent",
    ] {
        assert!(help.contains(command), "missing legacy command {command}");
    }

    // This manifest is the public 1.14.3 CLI contract. New flags may be added, but a
    // release cannot silently remove a legacy command path or long option.
    for (command_path, legacy_options) in [
        (&["ping"][..], &["--config"][..]),
        (&["version"][..], &["--config"][..]),
        (&["ps"][..], &["--all", "--config"][..]),
        (&["status"][..], &["--config", "--json"][..]),
        (&["start"][..], &["--config", "--wait"][..]),
        (&["up"][..], &["--config", "--force"][..]),
        (&["ask"][..], &["--config", "--request-id", "--wait"][..]),
        (
            &["tell"][..],
            &["--config", "--request-id", "--session"][..],
        ),
        (&["pend"][..], &["--config"][..]),
        (&["cancel"][..], &["--config"][..]),
        (&["kill"][..], &["--config", "--force", "--session"][..]),
        (&["watch"][..], &["--config", "--since-event-id"][..]),
        (&["events"][..], &["--config", "--format"][..]),
        (&["logs"][..], &["--config", "--since"][..]),
        (&["attach"][..], &["--config", "--session"][..]),
        (&["stop"][..], &["--config"][..]),
        (
            &["reclaim"][..],
            &["--archive-to", "--config", "--older-than-days", "--yes"][..],
        ),
        (&["doctor"][..], &["--config"][..]),
        (
            &["setup"][..],
            &[
                "--check", "--config", "--fix", "--json", "--resume", "--yes",
            ][..],
        ),
        (
            &["master", "cutover"][..],
            &["--config", "--print-attach", "--wait"][..],
        ),
        (
            &["master", "ack-ready"][..],
            &["--config", "--cutover-id"][..],
        ),
        (
            &["agent", "notify"][..],
            &[
                "--agent-id",
                "--config",
                "--event",
                "--event-id",
                "--hook-debug-log",
                "--hook-json",
                "--outbox-dir",
                "--provider",
                "--socket",
            ][..],
        ),
        (&["config", "validate"][..], &["--config"][..]),
        (&["config", "migrate"][..], &["--config"][..]),
        (&["bundle", "validate"][..], &["--all", "--config"][..]),
        (&["bundle", "list"][..], &["--config"][..]),
        (
            &["prompt", "resolve"][..],
            &["--action", "--config", "--keys", "--save-to-kb"][..],
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_ah"))
            .args(command_path)
            .arg("--help")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} --help failed",
            command_path.join(" ")
        );
        let command_help = String::from_utf8(output.stdout).unwrap();
        for option in legacy_options {
            assert!(
                command_help.contains(option),
                "{} help lost legacy option {option}",
                command_path.join(" ")
            );
        }
    }

    let ask_help = Command::new(env!("CARGO_BIN_EXE_ah"))
        .args(["ask", "--help"])
        .output()
        .unwrap();
    assert!(ask_help.status.success());
    let ask_help = String::from_utf8(ask_help.stdout).unwrap();
    for contract in [
        "<AGENT_ID>",
        "<TEXT>",
        "--wait",
        "--request-id",
        "--binding",
    ] {
        assert!(ask_help.contains(contract), "ask help lost {contract}");
    }

    let pend_help = Command::new(env!("CARGO_BIN_EXE_ah"))
        .args(["pend", "--help"])
        .output()
        .unwrap();
    assert!(pend_help.status.success());
    let pend_help = String::from_utf8(pend_help.stdout).unwrap();
    assert!(pend_help.contains("<JOB_ID>"));
    assert!(pend_help.contains("--json"));
}
