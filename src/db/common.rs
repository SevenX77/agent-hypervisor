use crate::error::AhError;
use rusqlite::Error as SqlError;

pub(crate) fn is_constraint_error(err: &SqlError) -> bool {
    matches!(
        err,
        SqlError::SqliteFailure(sqlite_err, _)
            if sqlite_err.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

pub(crate) fn is_unique_constraint_error(err: &SqlError) -> bool {
    matches!(
        err,
        SqlError::SqliteFailure(sqlite_err, _)
            if sqlite_err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
    )
}

pub(crate) fn map_db_error(context: &str, err: SqlError) -> AhError {
    AhError::DbConstraintViolation(format!("{context}: {err}"))
}

pub(crate) async fn spawn_db<T, F>(op: &'static str, f: F) -> Result<T, AhError>
where
    F: FnOnce() -> Result<T, AhError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|join_err| AhError::DatabaseRuntimePanic {
            details: format!("{op}: {join_err}"),
        })?
}
