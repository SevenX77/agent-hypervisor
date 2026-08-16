use crate::error::AhError;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRef {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSkill {
    pub name: String,
    pub source_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillMaterialization {
    pub source_dir: PathBuf,
    pub target_dir: PathBuf,
}

pub fn parse_skill_refs(raw: &[String]) -> Result<Vec<SkillRef>, AhError> {
    raw.iter()
        .map(|name| {
            validate_skill_name(name)?;
            Ok(SkillRef { name: name.clone() })
        })
        .collect()
}

pub fn resolve_project_skills(
    project_root: &Path,
    refs: &[SkillRef],
) -> Result<Vec<ResolvedSkill>, AhError> {
    let skills_root = project_root.join(".ah/skills");
    let canonical_project = project_root
        .canonicalize()
        .map_err(|err| skill_err(format!("project root not found: {err}")))?;
    let canonical_root = skills_root
        .canonicalize()
        .map_err(|err| skill_err(format!("project skills root not found: {err}")))?;
    if !canonical_root.starts_with(&canonical_project) {
        return Err(skill_err(format!(
            "project skills root escapes project: {}",
            skills_root.display()
        )));
    }
    refs.iter()
        .map(|skill_ref| {
            let source_dir = skills_root.join(&skill_ref.name);
            if !source_dir.is_dir() {
                return Err(skill_err(format!(
                    "skill directory not found for {}: {}",
                    skill_ref.name,
                    source_dir.display()
                )));
            }
            let canonical_source = source_dir.canonicalize().map_err(|err| {
                skill_err(format!(
                    "resolve skill directory for {}: {err}",
                    skill_ref.name
                ))
            })?;
            if !canonical_source.starts_with(&canonical_root) {
                return Err(skill_err(format!(
                    "skill {} escapes project .ah/skills: {}",
                    skill_ref.name,
                    source_dir.display()
                )));
            }
            let skill_md = canonical_source.join("SKILL.md");
            if !skill_md.is_file() {
                return Err(skill_err(format!(
                    "skill {} missing SKILL.md: {}",
                    skill_ref.name,
                    source_dir.join("SKILL.md").display()
                )));
            }
            Ok(ResolvedSkill {
                name: skill_ref.name.clone(),
                source_dir: canonical_source,
            })
        })
        .collect()
}

pub fn plan_claude_skill_materialization(
    claude_dir: &Path,
    resolved: &[ResolvedSkill],
) -> Vec<SkillMaterialization> {
    plan_skill_materialization(claude_dir, resolved)
}

pub fn plan_codex_skill_materialization(
    codex_home: &Path,
    resolved: &[ResolvedSkill],
) -> Vec<SkillMaterialization> {
    plan_skill_materialization(codex_home, resolved)
}

fn plan_skill_materialization(
    provider_skills_home: &Path,
    resolved: &[ResolvedSkill],
) -> Vec<SkillMaterialization> {
    resolved
        .iter()
        .map(|skill| SkillMaterialization {
            source_dir: skill.source_dir.clone(),
            target_dir: provider_skills_home.join("skills").join(&skill.name),
        })
        .collect()
}

fn validate_skill_name(name: &str) -> Result<(), AhError> {
    if name.is_empty() {
        return Err(skill_err("skill name must not be empty"));
    }
    let path = Path::new(name);
    if path.is_absolute() {
        return Err(skill_err(format!(
            "invalid skill name {name:?}: absolute paths are not allowed"
        )));
    }
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) if !name.contains('\\') => Ok(()),
        _ => Err(skill_err(format!(
            "invalid skill name {name:?}: use a single directory name under .ah/skills"
        ))),
    }
}

fn skill_err(details: impl Into<String>) -> AhError {
    AhError::EnvironmentNotSupported {
        details: details.into(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn parse_skill_refs_accepts_simple_names() {
        let refs = parse_skill_refs(&["domain-review".to_string()]).unwrap();
        assert_eq!(
            refs,
            vec![SkillRef {
                name: "domain-review".to_string()
            }]
        );
    }

    #[test]
    fn parse_skill_refs_rejects_traversal_absolute_and_empty_names() {
        for name in ["", "../x", "/tmp/x", "nested/x", "."] {
            assert!(
                parse_skill_refs(&[name.to_string()]).is_err(),
                "expected {name:?} to be rejected"
            );
        }
    }

    #[test]
    fn resolve_project_skills_requires_directory_and_skill_md() {
        let project = tempfile::tempdir().unwrap();
        let refs = parse_skill_refs(&["missing".to_string()]).unwrap();
        assert!(resolve_project_skills(project.path(), &refs).is_err());

        let skill_dir = project.path().join(".ah/skills/no-md");
        std::fs::create_dir_all(&skill_dir).unwrap();
        let refs = parse_skill_refs(&["no-md".to_string()]).unwrap();
        assert!(resolve_project_skills(project.path(), &refs).is_err());
    }

    #[test]
    fn resolve_project_skills_accepts_skill_with_skill_md() {
        let project = tempfile::tempdir().unwrap();
        let skill_dir = project.path().join(".ah/skills/domain");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "---\nname: domain\n---\n").unwrap();
        let refs = parse_skill_refs(&["domain".to_string()]).unwrap();
        let resolved = resolve_project_skills(project.path(), &refs).unwrap();
        assert_eq!(resolved[0].name, "domain");
        assert_eq!(resolved[0].source_dir, skill_dir.canonicalize().unwrap());
    }

    #[test]
    fn resolve_project_skills_rejects_symlink_escape() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("SKILL.md"), "---\nname: escaped\n---\n").unwrap();
        let skills_root = project.path().join(".ah/skills");
        std::fs::create_dir_all(&skills_root).unwrap();
        std::os::unix::fs::symlink(outside.path(), skills_root.join("escaped")).unwrap();
        let refs = parse_skill_refs(&["escaped".to_string()]).unwrap();
        assert!(resolve_project_skills(project.path(), &refs).is_err());
    }

    #[test]
    fn plan_claude_skill_materialization_targets_claude_skills_dir() {
        let resolved = vec![ResolvedSkill {
            name: "domain".to_string(),
            source_dir: PathBuf::from("/project/.ah/skills/domain"),
        }];
        let plan = plan_claude_skill_materialization(Path::new("/home/.claude"), &resolved);
        assert_eq!(
            plan,
            vec![SkillMaterialization {
                source_dir: PathBuf::from("/project/.ah/skills/domain"),
                target_dir: PathBuf::from("/home/.claude/skills/domain"),
            }]
        );
    }

    #[test]
    fn plan_codex_skill_materialization_targets_codex_skills_dir() {
        let resolved = vec![ResolvedSkill {
            name: "domain".to_string(),
            source_dir: PathBuf::from("/project/.ah/skills/domain"),
        }];
        let plan = plan_codex_skill_materialization(Path::new("/home/.codex"), &resolved);
        assert_eq!(
            plan,
            vec![SkillMaterialization {
                source_dir: PathBuf::from("/project/.ah/skills/domain"),
                target_dir: PathBuf::from("/home/.codex/skills/domain"),
            }]
        );
    }
}
