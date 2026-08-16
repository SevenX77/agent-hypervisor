use crate::cli::config::{DiagnosticSeverity, load_project_config, validate_project_config};
use crate::cli::rpc_client::CliError;
use std::path::Path;

pub fn run_config_validate(config_path: &Path) -> Result<(), CliError> {
    let config = load_project_config(config_path)?;
    let diagnostics = validate_project_config(&config);
    for diagnostic in &diagnostics {
        let label = match diagnostic.severity {
            DiagnosticSeverity::Error => "error",
            DiagnosticSeverity::Warning => "warning",
        };
        println!("{label}: {}", diagnostic.message);
    }
    if diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    {
        return Err(CliError::Config(format!(
            "{} failed validation",
            config_path.display()
        )));
    }
    println!("ok: {}", config_path.display());
    Ok(())
}

pub fn migrate_stub(cwd: &Path) -> Result<(), CliError> {
    let old = cwd.join(".ah").join("ah.config");
    let new = cwd.join("ah.toml");
    if old.exists() && !new.exists() {
        println!(
            "found legacy {}; create {} using examples/ah.toml as a template",
            old.display(),
            new.display()
        );
        Ok(())
    } else {
        Err(CliError::Config(
            "no legacy .ah/ah.config migration candidate found".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::run_config_validate;

    #[test]
    fn test_config_validate_valid_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("ah.toml");
        std::fs::write(
            &path,
            "version = \"1\"\n[master]\nenabled = false\n[agents.a1]\nprovider = \"bash\"\n",
        )
        .unwrap();
        run_config_validate(&path).unwrap();
    }

    #[test]
    fn test_config_validate_invalid_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("ah.toml");
        std::fs::write(&path, "version = \"2\"\n[agents]\n").unwrap();
        assert!(run_config_validate(&path).is_err());
    }
}
