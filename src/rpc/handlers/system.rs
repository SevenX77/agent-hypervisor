use crate::db::system::system_dump;
use crate::error::AhError;
use crate::rpc::Ctx;
use serde_json::{Value, json};

pub async fn handle_system_dump(_params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    system_dump(ctx.db.clone()).await
}

pub async fn handle_system_shutdown(_params: Value, ctx: &Ctx) -> Result<Value, AhError> {
    crate::master_revival::mark_all_sessions_intentional_shutdown(&ctx.db)?;
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        #[cfg(unix)]
        unsafe {
            libc::kill(libc::getpid(), libc::SIGTERM)
        };
        #[cfg(windows)]
        std::process::exit(0);
    });
    Ok(json!({"status": "shutting_down"}))
}
