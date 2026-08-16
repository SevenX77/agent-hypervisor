//! Provider-native metering boundary.
//!
//! The migrated AH sources expose no reliable structured per-Run usage record.
//! Callers therefore receive an explicit unmetered result instead of a pane-
//! derived or fabricated number.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeteringResult {
    Native {
        provider: String,
        fields: std::collections::BTreeMap<String, u64>,
    },
    Unmetered {
        provider: String,
        reason: String,
    },
}

pub fn unmetered(provider: impl Into<String>, reason: impl Into<String>) -> MeteringResult {
    MeteringResult::Unmetered {
        provider: provider.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absence_of_native_usage_is_explicit() {
        assert!(matches!(
            unmetered("antigravity", "provider exposes no structured usage event"),
            MeteringResult::Unmetered { .. }
        ));
    }
}
