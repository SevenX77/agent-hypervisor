use std::path::Path;
use std::process::Command;

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn write_ah_toml(project: &Path, body: &str) {
    let shared_credentials_dir = project.join("shared-claude-credentials");
    std::fs::create_dir_all(&shared_credentials_dir).unwrap();
    write(
        &project.join("ah.toml"),
        &format!(
            "version = \"1\"\n\n[providers.claude]\nshared_credentials_dir = \"{}\"\n\n{body}\n",
            shared_credentials_dir.display()
        ),
    );
}

fn write_bundle(project: &Path, name: &str) {
    let root = project.join(".ah/bundles").join(name);
    write(
        &root.join("bundle.toml"),
        &format!(
            r#"name = "{name}"
version = "1"

[skills]
include = ["s"]

[hooks]
PostToolUse = [{{ command = "hooks/guard.sh" }}]

[rules]
worker = "rules/worker.md"

[[mcp.servers]]
name = "ctx"
transport = "stdio"
command = "npx"
env = {{ CONTEXT_TOKEN = "${{CONTEXT_TOKEN}}" }}
"#
        ),
    );
    write(&root.join("skills/s/SKILL.md"), "# Skill\n");
    write(
        &root.join("hooks/guard.sh"),
        "#!/usr/bin/env bash\nexit 0\n",
    );
    write(&root.join("rules/worker.md"), "worker rules\n");
}

fn write_invalid_bundle(project: &Path, name: &str) {
    let root = project.join(".ah/bundles").join(name);
    write(
        &root.join("bundle.toml"),
        &format!(
            r#"name = "{name}"
version = "1"

[rules]
worker = "../escape.md"
"#
        ),
    );
}

fn ah_command(project: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ah"));
    cmd.current_dir(project)
        .env_remove("AH_SOCKET")
        .env_remove("AH_STATE_DIR")
        .env_remove("XDG_STATE_HOME");
    cmd
}

#[test]
fn bundle_cli_subcommands_parse() {
    let output = Command::new(env!("CARGO_BIN_EXE_ah"))
        .args(["bundle", "--help"])
        .env_remove("AH_SOCKET")
        .env_remove("AH_STATE_DIR")
        .env_remove("XDG_STATE_HOME")
        .output()
        .unwrap();

    assert!(output.status.success(), "stderr={}", stderr(&output));
    let stdout = stdout(&output);
    assert!(stdout.contains("validate"));
    assert!(stdout.contains("list"));
}

#[test]
fn bundle_validate_selects_referenced_explicit_and_all() {
    let project = tempfile::tempdir().unwrap();
    write_bundle(project.path(), "used");
    write_invalid_bundle(project.path(), "unused-bad");
    write_ah_toml(
        project.path(),
        r#"[agents.a1]
provider = "claude"
bundle = ["used"]
"#,
    );

    let output = ah_command(project.path())
        .args(["bundle", "validate"])
        .output()
        .unwrap();

    assert!(output.status.success(), "stderr={}", stderr(&output));
    let stdout = stdout(&output);
    assert!(stdout.contains("VALID used"));
    assert!(!stdout.contains("unused-bad"));
}

#[test]
fn bundle_validate_reports_success_and_failures() {
    let project = tempfile::tempdir().unwrap();
    write_bundle(project.path(), "used");
    write_invalid_bundle(project.path(), "bad");
    write_ah_toml(
        project.path(),
        r#"[agents.a1]
provider = "claude"
bundle = ["used"]
"#,
    );

    let explicit = ah_command(project.path())
        .args(["bundle", "validate", "bad"])
        .output()
        .unwrap();
    assert!(!explicit.status.success());
    assert!(stderr(&explicit).contains("bad"));
    assert!(stderr(&explicit).contains("must stay within the bundle"));

    let all = ah_command(project.path())
        .args(["bundle", "validate", "--all"])
        .output()
        .unwrap();
    assert!(!all.status.success());
    assert!(stderr(&all).contains("bad"));
}

#[test]
fn bundle_list_sorts_references_and_status() {
    let project = tempfile::tempdir().unwrap();
    write_bundle(project.path(), "zeta");
    write_bundle(project.path(), "alpha");
    write_invalid_bundle(project.path(), "bad");
    write_ah_toml(
        project.path(),
        r#"[master]
bundle = ["zeta"]

[agents.a2]
provider = "claude"
bundle = ["alpha", "zeta"]
"#,
    );

    let output = ah_command(project.path())
        .args(["bundle", "list"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stdout = stdout(&output);
    let alpha = stdout.find("alpha\t").unwrap();
    let bad = stdout.find("bad\t").unwrap();
    let zeta = stdout.find("zeta\t").unwrap();
    assert!(alpha < bad && bad < zeta, "{stdout}");
    assert!(stdout.contains("zeta\t1\t1\t1\t1\t1\ta2,master\tOK"));
    assert!(stdout.contains("bad\t-\t-\t-\t-\t-\t-\tERROR"));
}

#[test]
fn bundle_cli_does_not_print_secret_values() {
    let project = tempfile::tempdir().unwrap();
    write_bundle(project.path(), "used");
    write_ah_toml(
        project.path(),
        r#"[agents.a1]
provider = "claude"
bundle = ["used"]
"#,
    );

    let output = ah_command(project.path())
        .env("CONTEXT_TOKEN", "super-secret-test-value")
        .args(["bundle", "validate", "--all"])
        .output()
        .unwrap();

    assert!(output.status.success(), "stderr={}", stderr(&output));
    assert!(!stdout(&output).contains("super-secret-test-value"));
    assert!(!stderr(&output).contains("super-secret-test-value"));
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
