use color_eyre::eyre::{Result as Res, WrapErr};
use sqlx::ConnectOptions;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SqlCtx {
    pub write_pool: SqlitePool,
    pub read_pool: SqlitePool,
    _ephemeral_directory: Option<Arc<tempfile::TempDir>>,
}

impl SqlCtx {
    /// Open an isolated process-local SQLite database.
    ///
    /// The name is retained for API compatibility, but this deliberately uses
    /// a temporary file rather than `sqlite::memory:`. SQLx may replace an
    /// invalidated pooled in-memory connection; replacing that connection
    /// silently creates a fresh empty database. Keeping the temporary file
    /// alive across cloned contexts gives both pools a stable database while
    /// retaining ephemeral lifetime and cleanup.
    pub async fn memory() -> Res<Self> {
        Self::ephemeral_file().await
    }

    /// Open an isolated file-backed SQLite database that is removed when all
    /// clones of this context are dropped.
    pub async fn ephemeral_file() -> Res<Self> {
        let directory = Arc::new(tempfile::tempdir().wrap_err("failed creating sqlite temp directory")?);
        let path = directory.path().join("database.sqlite");
        let url = format!("sqlite://{}", path.display());
        let mut context = Self::open_file_url(&url).await?;
        context._ephemeral_directory = Some(directory);
        Ok(context)
    }

    pub async fn url(url: &str) -> Res<Self> {
        if is_memory_url(url) {
            return Self::memory().await;
        }
        Self::open_file_url(url).await
    }

    async fn open_file_url(url: &str) -> Res<Self> {
        let connect_options = SqliteConnectOptions::from_str(url)
            .wrap_err_with(|| format!("failed parsing sqlite url: {url}"))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            //.busy_timeout(std::time::Duration::from_secs(90))
            .disable_statement_logging();

        let read_pool = SqlitePoolOptions::new()
            .max_connections(4)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(connect_options.clone())
            .await
            .wrap_err_with(|| format!("failed opening sqlite read pool: {url}"))?;
        let write_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(connect_options)
            .await
            .wrap_err_with(|| format!("failed opening sqlite write pool: {url}"))?;

        Ok(Self {
            write_pool,
            read_pool,
            _ephemeral_directory: None,
        })
    }
}

fn is_memory_url(url: &str) -> bool {
    url.contains(":memory:") || url.contains("mode=memory")
}

/// Register the `sqlite-vec` extension as a sqlite auto-extension so every
/// subsequently opened sqlite connection has vector search available.
/// Safe to call multiple times; the registration is guarded by a `OnceLock`.
pub fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        let entry_point: unsafe extern "C" fn(
            *mut libsqlite3_sys::sqlite3,
            *mut *mut std::ffi::c_char,
            *const libsqlite3_sys::sqlite3_api_routines,
        ) -> i32 = std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ());
        libsqlite3_sys::sqlite3_auto_extension(Some(entry_point));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_schema_query_roundtrip() -> Res<()> {
        let ctx = SqlCtx::memory().await?;
        sqlx::query("CREATE TABLE durable_schema (value INTEGER)")
            .execute(&ctx.write_pool)
            .await?;

        sqlx::query("INSERT INTO durable_schema VALUES (1)")
            .execute(&ctx.write_pool)
            .await?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM durable_schema")
            .fetch_one(&ctx.read_pool)
            .await?;
        assert_eq!(count, 1);
        Ok(())
    }
}
