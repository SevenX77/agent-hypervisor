//! Built-in ah rule resources embedded into the ah binary.

pub const MASTER_KERNEL: &str = include_str!("../../assets/builtin/master_kernel.md");
pub const WORKER_KERNEL: &str = include_str!("../../assets/builtin/worker_kernel.md");
pub const DEFAULT_MASTER: &str = include_str!("../../assets/builtin/defaults/master.md");
pub const DEFAULT_WORKER: &str = include_str!("../../assets/builtin/defaults/worker.md");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinSkillScope {
    MasterOnly,
    AllAgents,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinSkill {
    pub name: &'static str,
    pub skill_md: &'static str,
    pub scope: BuiltinSkillScope,
}

pub const BUILTIN_SKILLS: &[BuiltinSkill] = &[
    BuiltinSkill {
        name: "ah-commands",
        skill_md: include_str!("../../assets/builtin/skills/ah-commands/SKILL.md"),
        scope: BuiltinSkillScope::MasterOnly,
    },
    BuiltinSkill {
        name: "ah-config",
        skill_md: include_str!("../../assets/builtin/skills/ah-config/SKILL.md"),
        scope: BuiltinSkillScope::MasterOnly,
    },
    BuiltinSkill {
        name: "ah-runtime-state",
        skill_md: include_str!("../../assets/builtin/skills/ah-runtime-state/SKILL.md"),
        scope: BuiltinSkillScope::MasterOnly,
    },
    BuiltinSkill {
        name: "ah-operate",
        skill_md: include_str!("../../assets/builtin/skills/ah-operate/SKILL.md"),
        scope: BuiltinSkillScope::MasterOnly,
    },
];
