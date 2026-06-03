use color_eyre::eyre::{Result as Res, WrapErr};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::ConnectOptions;
use sqlx::SqlitePool;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct SqlCtx {
    pub write_pool: SqlitePool,
    pub read_pool: SqlitePool,
}

impl SqlCtx {
    pub async fn memory() -> Res<Self> {
        let connect_options = SqliteConnectOptions::from_str("sqlite::memory:")?;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .wrap_err("failed opening sqlite memory context")?;

        Ok(Self {
            write_pool: pool.clone(),
            read_pool: pool,
        })
    }

    pub async fn url(url: &str) -> Res<Self> {
        if is_memory_url(url) {
            return Self::memory().await;
        }

        let connect_options = SqliteConnectOptions::from_str(url)
            .wrap_err_with(|| format!("failed parsing sqlite url: {url}"))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            //.busy_timeout(std::time::Duration::from_secs(90))
            .disable_statement_logging();

        let read_pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(connect_options.clone())
            .await
            .wrap_err_with(|| format!("failed opening sqlite read pool: {url}"))?;
        let write_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .wrap_err_with(|| format!("failed opening sqlite write pool: {url}"))?;

        Ok(Self {
            write_pool,
            read_pool,
        })
    }
}

fn is_memory_url(url: &str) -> bool {
    url.contains(":memory:")
}
