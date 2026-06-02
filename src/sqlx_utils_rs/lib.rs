use color_eyre::eyre::{Result as Res, WrapErr};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub fn sqlite_file_url(path: impl AsRef<Path>) -> String {
    format!("sqlite://{}", path.as_ref().display())
}

pub fn sqlite_file_connect_options(database_url: &str) -> Res<SqliteConnectOptions> {
    Ok(SqliteConnectOptions::from_str(database_url)
        .wrap_err_with(|| format!("invalid sqlite url: {database_url}"))?
        .create_if_missing(true))
}

pub fn sqlite_file_connect_options_with_wal_busy(
    database_url: &str,
    busy_timeout: Duration,
) -> Res<SqliteConnectOptions> {
    Ok(sqlite_file_connect_options(database_url)?
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(busy_timeout))
}

pub async fn open_sqlite_pool(
    database_url: &str,
    connect_options: SqliteConnectOptions,
    max_connections: u32,
) -> Res<SqlitePool> {
    open_sqlite_pool_with_context(
        database_url,
        connect_options,
        max_connections,
        "error initializing sqlite db",
    )
    .await
}

pub async fn open_sqlite_rw_pools(
    database_url: &str,
    connect_options: SqliteConnectOptions,
    read_max_connections: u32,
    write_max_connections: u32,
) -> Res<(SqlitePool, SqlitePool)> {
    let read_pool = open_sqlite_pool_with_context(
        database_url,
        connect_options.clone(),
        read_max_connections,
        "failed connecting big repo sqlite read pool",
    )
    .await?;
    let write_pool = open_sqlite_pool_with_context(
        database_url,
        connect_options,
        write_max_connections,
        "failed connecting big repo sqlite write pool",
    )
    .await?;

    Ok((read_pool, write_pool))
}

async fn open_sqlite_pool_with_context(
    database_url: &str,
    connect_options: SqliteConnectOptions,
    max_connections: u32,
    context: &'static str,
) -> Res<SqlitePool> {
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(connect_options)
        .await
        .wrap_err_with(|| format!("{context}: {database_url}"))
}
