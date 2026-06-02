use color_eyre::eyre::{Result as Res, WrapErr};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct SqlCtx {
    pub write_pool: SqlitePool,
    pub read_pool: SqlitePool,
}

impl SqlCtx {
    pub fn from_single_pool(pool: SqlitePool) -> Self {
        Self {
            write_pool: pool.clone(),
            read_pool: pool,
        }
    }

    pub fn from_rw_pools(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            write_pool,
            read_pool,
        }
    }
}

pub fn sqlite_file_connect_options(path: impl AsRef<Path>) -> Res<SqliteConnectOptions> {
    Ok(SqliteConnectOptions::new()
        .filename(path.as_ref())
        .create_if_missing(true))
}

pub fn sqlite_file_connect_options_with_wal_busy(
    path: impl AsRef<Path>,
    busy_timeout: Duration,
) -> Res<SqliteConnectOptions> {
    Ok(sqlite_file_connect_options(path)?
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(busy_timeout))
}

pub async fn open_sqlite_pool(
    database_path: impl AsRef<Path>,
    connect_options: SqliteConnectOptions,
    max_connections: u32,
) -> Res<SqlitePool> {
    let database_path = database_path.as_ref().to_path_buf();
    open_sqlite_pool_with_context(
        &database_path,
        connect_options,
        max_connections,
        "error initializing sqlite db",
    )
    .await
}

pub async fn open_sqlite_rw_pools(
    database_path: impl AsRef<Path>,
    connect_options: SqliteConnectOptions,
    read_max_connections: u32,
    write_max_connections: u32,
) -> Res<(SqlitePool, SqlitePool)> {
    let database_path = database_path.as_ref().to_path_buf();
    let read_pool = open_sqlite_pool_with_context(
        &database_path,
        connect_options.clone(),
        read_max_connections,
        "failed connecting big repo sqlite read pool",
    )
    .await?;
    let write_pool = open_sqlite_pool_with_context(
        &database_path,
        connect_options,
        write_max_connections,
        "failed connecting big repo sqlite write pool",
    )
    .await?;

    Ok((read_pool, write_pool))
}

async fn open_sqlite_pool_with_context(
    database_path: &Path,
    connect_options: SqliteConnectOptions,
    max_connections: u32,
    context: &'static str,
) -> Res<SqlitePool> {
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(connect_options)
        .await
        .wrap_err_with(|| format!("{context}: {}", database_path.display()))
}
