use crate::cli::config::{find_config, load_project_config};
use crate::cli::rpc_client::CliError;
use crate::provider::bundles::{
    BundleInspection, BundleRole, inspect_bundle, list_bundle_names, resolve_bundles_for_provider,
};
use crate::provider::extensions::ExtensionConfig;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct BundleValidateOptions {
    pub config_path: Option<PathBuf>,
    pub cwd: PathBuf,
    pub all: bool,
    pub names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BundleListOptions {
    pub config_path: Option<PathBuf>,
    pub cwd: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BundleListReport {
    pub output: String,
    pub has_errors: bool,
}

pub fn run_bundle_validate(options: BundleValidateOptions) -> Result<(), CliError> {
    let report = validate_report(options)?;
    print!("{report}");
    Ok(())
}

pub fn run_bundle_list(options: BundleListOptions) -> Result<(), CliError> {
    let report = list_report(options)?;
    print!("{}", report.output);
    if report.has_errors {
        return Err(CliError::Config(
            "one or more bundles failed validation".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_report(options: BundleValidateOptions) -> Result<String, CliError> {
    let loaded = LoadedBundleConfig::load(options.config_path, options.cwd)?;
    if options.all && !options.names.is_empty() {
        return Err(CliError::Config(
            "use either --all or explicit bundle names, not both".to_string(),
        ));
    }

    let targets = if options.all {
        list_bundle_names(&loaded.project_root).map_err(bundle_cli_error)?
    } else if !options.names.is_empty() {
        sorted_unique(options.names)
    } else {
        loaded.references.keys().cloned().collect()
    };

    if targets.is_empty() {
        return Ok("VALID no bundles\n".to_string());
    }

    let mut lines = Vec::new();
    let mut errors = Vec::new();
    for name in targets {
        match validate_one(&loaded, &name) {
            Ok(inspection) => {
                lines.push(format!(
                    "VALID {} version={} skills={} hooks={} rules={} mcp={}",
                    inspection.name,
                    inspection.version,
                    inspection.skills_count,
                    inspection.hook_count,
                    inspection.rules_count,
                    inspection.mcp_count
                ));
            }
            Err(err) => errors.push(format!("{name}: {err}")),
        }
    }

    if errors.is_empty() {
        lines.push(String::new());
        Ok(lines.join("\n"))
    } else {
        Err(CliError::Config(format!(
            "bundle validation failed:\n{}",
            errors.join("\n")
        )))
    }
}

pub fn list_report(options: BundleListOptions) -> Result<BundleListReport, CliError> {
    let loaded = LoadedBundleConfig::load(options.config_path, options.cwd)?;
    let mut names = list_bundle_names(&loaded.project_root).map_err(bundle_cli_error)?;
    for name in loaded.references.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    names.sort();

    let mut output =
        String::from("NAME\tVERSION\tSKILLS\tHOOKS\tRULES\tMCP\tREFERENCED_BY\tSTATUS\n");
    let mut has_errors = false;
    for name in names {
        let refs = loaded
            .references
            .get(&name)
            .map(|refs| refs.iter().cloned().collect::<Vec<_>>().join(","))
            .unwrap_or_else(|| "-".to_string());
        match validate_one(&loaded, &name) {
            Ok(inspection) => {
                output.push_str(&format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\tOK\n",
                    inspection.name,
                    inspection.version,
                    inspection.skills_count,
                    inspection.hook_count,
                    inspection.rules_count,
                    inspection.mcp_count,
                    refs
                ));
            }
            Err(err) => {
                has_errors = true;
                output.push_str(&format!("{name}\t-\t-\t-\t-\t-\t{refs}\tERROR: {err}\n"));
            }
        }
    }

    Ok(BundleListReport { output, has_errors })
}

struct LoadedBundleConfig {
    project_root: PathBuf,
    references: BTreeMap<String, BTreeSet<String>>,
    provider_checks: BTreeMap<String, Vec<ProviderCheck>>,
}

#[derive(Debug, Clone)]
struct ProviderCheck {
    provider: String,
    role: BundleRole,
}

impl LoadedBundleConfig {
    fn load(config_path: Option<PathBuf>, cwd: PathBuf) -> Result<Self, CliError> {
        let config_path = match config_path {
            Some(path) => path,
            None => find_config(&cwd)?,
        };
        let config = load_project_config(&config_path)?;
        let project_root = config_path
            .parent()
            .ok_or_else(|| CliError::Config("config path has no parent directory".to_string()))?
            .to_path_buf();
        let mut references = BTreeMap::<String, BTreeSet<String>>::new();
        let mut provider_checks = BTreeMap::<String, Vec<ProviderCheck>>::new();

        let master_provider = config.master.resolved_provider();
        for name in &config.master.bundle {
            references
                .entry(name.clone())
                .or_default()
                .insert("master".to_string());
            provider_checks
                .entry(name.clone())
                .or_default()
                .push(ProviderCheck {
                    provider: master_provider.to_string(),
                    role: BundleRole::Master,
                });
        }

        for (agent_id, agent) in &config.agents {
            for name in &agent.bundle {
                references
                    .entry(name.clone())
                    .or_default()
                    .insert(agent_id.clone());
                provider_checks
                    .entry(name.clone())
                    .or_default()
                    .push(ProviderCheck {
                        provider: agent.provider.clone(),
                        role: BundleRole::Worker,
                    });
            }
        }

        Ok(Self {
            project_root,
            references,
            provider_checks,
        })
    }
}

fn validate_one(loaded: &LoadedBundleConfig, name: &str) -> Result<BundleInspection, CliError> {
    let inspection = inspect_bundle(&loaded.project_root, name).map_err(bundle_cli_error)?;
    if let Some(checks) = loaded.provider_checks.get(name) {
        for check in checks {
            let base = ExtensionConfig {
                bundle: vec![name.to_string()],
                ..Default::default()
            };
            resolve_bundles_for_provider(&loaded.project_root, &check.provider, check.role, &base)
                .map_err(bundle_cli_error)?;
        }
    }
    Ok(inspection)
}

fn sorted_unique(names: Vec<String>) -> Vec<String> {
    names
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn bundle_cli_error(err: crate::error::AhError) -> CliError {
    CliError::Config(err.to_string())
}
