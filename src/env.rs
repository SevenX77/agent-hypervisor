use std::path::PathBuf;

pub fn resolve_state_dir() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let dir = crate::state_layout::resolve_state_dir_for_cwd(&cwd);

    std::fs::create_dir_all(&dir).expect("failed to create ah state directory");
    dir
}
