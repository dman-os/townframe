use crate::interlude::*;

pub struct SqlCtx {
    db_pool: sqlx::SqlitePool,
}

impl SqlCtx {
    pub async fn new() -> Res<Self> {
        use std::str::FromStr;
        let db_pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str("sqlite:///tmp/daybook.db")?
                .create_if_missing(true),
        )
        .await
        .unwrap_or_log();
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

    pub(crate) fn pool(&self) -> &sqlx::SqlitePool {
        &self.db_pool
    }
}

pub mod kv {
    use super::*;

    const TABLE: &str = "kvstore";

    pub async fn get(cx: &crate::Ctx, key: &str) -> Res<Option<String>> {
        let rec =
            sqlx::query_scalar::<_, String>(&format!("SELECT value FROM {TABLE} WHERE key = ?1"))
                .bind(key)
                .fetch_optional(cx.sql.pool())
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
        .execute(cx.sql.pool())
        .await?;
        Ok(())
    }
}
