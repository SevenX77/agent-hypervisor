pub fn detect_current_service_unit() -> Option<String> {
    if let Ok(val) = std::env::var("AH_SERVICE_UNIT") {
        if !val.is_empty() {
            return Some(val);
        }
    }
    crate::platform::sys::identity::detect_current_service_unit()
}

pub fn detect_current_scope_or_service() -> Option<String> {
    crate::platform::sys::identity::detect_current_scope_or_service()
}

pub fn detect_current_service_unit_from_cgroup(cgroup: &str) -> Option<String> {
    crate::platform::sys::identity::detect_current_service_unit_from_cgroup(cgroup)
}

#[cfg(test)]
mod tests {
    use super::detect_current_service_unit_from_cgroup;

    #[test]
    fn detects_ah_service_with_user_manager_prefix() {
        let cgroup = "0::/user.slice/user-1001.slice/user@1001.service/app.slice/ahd.service";

        assert_eq!(
            detect_current_service_unit_from_cgroup(cgroup).as_deref(),
            Some("ahd.service")
        );
    }

    #[test]
    fn detects_ah_project_service_with_user_manager_prefix() {
        let cgroup = "0::/user.slice/user-1001.slice/user@1001.service/app.slice/ah-p1.service";

        assert_eq!(
            detect_current_service_unit_from_cgroup(cgroup).as_deref(),
            Some("ah-p1.service")
        );
    }

    #[test]
    fn ignores_scope_after_daemon_service() {
        let cgroup = "0::/user.slice/user-1001.slice/user@1001.service/app.slice/ahd.service/session-1.scope";

        assert_eq!(
            detect_current_service_unit_from_cgroup(cgroup).as_deref(),
            Some("ahd.service")
        );
    }

    #[test]
    fn user_manager_service_only_is_not_daemon_unit() {
        let cgroup = "0::/user.slice/user-1001.slice/user@1001.service";

        assert_eq!(detect_current_service_unit_from_cgroup(cgroup), None);
    }

    #[test]
    fn session_scope_and_slices_are_not_daemon_units() {
        for cgroup in [
            "0::/init.scope",
            "0::/user.slice/user-1001.slice",
            "0::/user.slice/user-1001.slice/user@1001.service/app.slice/app-org.gnome.Terminal.slice/vte-spawn.scope",
        ] {
            assert_eq!(detect_current_service_unit_from_cgroup(cgroup), None);
        }
    }

    #[test]
    fn unescapes_systemd_unit_segments_before_matching() {
        let cgroup = "0::/user.slice/user-1001.slice/user@1001.service/app.slice/ah\\x2dp1.service";

        assert_eq!(
            detect_current_service_unit_from_cgroup(cgroup).as_deref(),
            Some("ah-p1.service")
        );
    }
}
