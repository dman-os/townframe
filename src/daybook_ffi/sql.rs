use crate::interlude::*;

pub struct SqlCtx {
    db_pool: sqlx::SqlitePool,
}

/// Configuration for SQLite storage
#[derive(Debug, Clone)]
pub struct Config {
    /// SQLite database URL
    pub database_url: String,
}

impl SqlCtx {
    pub async fn new(config: Config) -> Res<Self> {
        use std::str::FromStr;

        // Ensure the database directory exists for file-based databases
        if !config.database_url.starts_with("sqlite::memory:") {
            if let Some(path) = config.database_url.strip_prefix("sqlite://") {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    std::fs::create_dir_all(parent).wrap_err_with(|| {
                        format!("Failed to create database directory: {}", parent.display())
                    })?;
                }
            }
        }

        let db_pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str(&config.database_url)?
                .create_if_missing(true),
        )
        .await
        .wrap_err("error initializing sqlite db")?;
        // Initialize schema
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS kvstore (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                "#,
        )
        .execute(&db_pool)
        .await?;

        Ok(Self { db_pool })
    }
}

pub mod kv {
    use super::*;

    const TABLE: &str = "kvstore";

    pub async fn get(cx: &crate::Ctx, key: &str) -> Res<Option<String>> {
        let rec =
            sqlx::query_scalar::<_, String>(&format!("SELECT value FROM {TABLE} WHERE key = ?1"))
                .bind(key)
                .fetch_optional(&cx.sql.db_pool)
                .await?;
        Ok(rec)
    }

    pub async fn set(cx: &crate::Ctx, key: &str, value: &str) -> Res<()> {
        sqlx::query(&format!(
            "INSERT INTO {TABLE}(key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        ))
        .bind(key)
        .bind(value)
        .execute(&cx.sql.db_pool)
        .await?;
        Ok(())
    }
}
