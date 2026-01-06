use crate::interlude::*;

use wflow_core::kvstore::*;

/// A SQLite-backed key-value store implementation.
///
/// Each instance uses a separate table in the database, allowing multiple stores
/// to coexist in the same database.
#[derive(Clone)]
pub struct SqliteKvStore {
    db_pool: sqlx::SqlitePool,
    table_name: Arc<str>,
}

impl SqliteKvStore {
    /// Create a new SQLite-backed key-value store.
    ///
    /// This will create the table if it doesn't exist. The table name should be
    /// a valid SQL identifier (no quotes needed, but will be sanitized).
    pub async fn new(db_pool: sqlx::SqlitePool, table_name: impl Into<Arc<str>>) -> Res<Self> {
        let table_name = table_name.into();
        // Sanitize table name to prevent SQL injection
        if !table_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(ferr!("invalid table name: {}", table_name));
        }

        // Versioned table: value + monotonic version
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    key     BLOB PRIMARY KEY,
                    value   BLOB NOT NULL,
                    version INTEGER NOT NULL
                )
                "#
        ))
        .execute(&db_pool)
        .await
        .wrap_err_with(|| format!("failed to create table: {}", table_name))?;

        Ok(Self {
            db_pool,
            table_name,
        })
    }
}

#[async_trait]
impl KvStore for SqliteKvStore {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let row = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            r#"SELECT value FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key)
        .fetch_optional(&self.db_pool)
        .await?;

        Ok(row.map(|v| v.into_boxed_slice().into()))
    }

    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        let mut tx = self.db_pool.begin().await?;

        let old = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            r#"SELECT value FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key.as_ref())
        .fetch_optional(&mut *tx)
        .await?;

        sqlx::query(&format!(
            r#"
            INSERT INTO "{}"(key, value, version)
            VALUES (?1, ?2, 1)
            ON CONFLICT(key) DO UPDATE
            SET value = excluded.value,
                version = version + 1
            "#,
            self.table_name
        ))
        .bind(key.as_ref())
        .bind(value.as_ref())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(old.map(|v| v.into_boxed_slice().into()))
    }

    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let mut tx = self.db_pool.begin().await?;

        let old = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            r#"SELECT value FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;

        sqlx::query(&format!(
            r#"DELETE FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(old.map(|v| v.into_boxed_slice().into()))
    }

    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        let mut tx = self.db_pool.begin().await?;

        // Step 1: fetch current value as INTEGER (or 0 if missing)
        let current: Option<Vec<u8>> = sqlx::query_scalar(&format!(
            r#"SELECT value FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;

        let current = if let Some(bytes) = current {
            if bytes.len() != 8 {
                eyre::bail!("value is not a i64: byte len {len} != 8", len = bytes.len());
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            i64::from_le_bytes(buf)
        } else {
            0
        };

        // Step 2: compute new value in Rust
        let next = current
            .checked_add(delta)
            .ok_or_else(|| ferr!("i64 overflow in increment"))?;

        // Step 3: encode *exactly* 8 bytes
        let encoded = next.to_le_bytes();

        // Step 4: write bytes + bump version atomically
        sqlx::query(&format!(
            r#"
        INSERT INTO "{}"(key, value, version)
        VALUES (?1, ?2, 1)
        ON CONFLICT(key) DO UPDATE
        SET value = excluded.value,
            version = version + 1
        "#,
            self.table_name
        ))
        .bind(key)
        .bind(&encoded[..])
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(next)
    }

    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        // Snapshot = (value, version)
        let row = sqlx::query_as::<_, (Vec<u8>, i64)>(&format!(
            r#"SELECT value, version FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key)
        .fetch_optional(&self.db_pool)
        .await?;

        let snapshot_value = row.as_ref().map(|(v, _)| -> Arc<[u8]> { v.clone().into() });
        let snapshot_version = row.map(|(_, v)| v).unwrap_or(0);

        let key: Arc<[u8]> = key.into();
        let store = self.clone();
        let table = self.table_name.clone();

        let current_cb = {
            let snapshot_value = snapshot_value.clone();
            move || snapshot_value.clone()
        };

        let swap_cb = move |value: Arc<[u8]>| -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>> {
            let store = store.clone();
            let key = key.clone();
            let table = table.clone();

            Box::pin(async move {
                let mut tx = store.db_pool.begin().await?;

                let result = if snapshot_version == 0 {
                    // Insert only if absent
                    sqlx::query(&format!(
                        r#"
                        INSERT INTO "{}"(key, value, version)
                        SELECT ?1, ?2, 1
                        WHERE NOT EXISTS (SELECT 1 FROM "{}" WHERE key = ?1)
                        "#,
                        table, table
                    ))
                    .bind(key.as_ref())
                    .bind(value.as_ref())
                    .execute(&mut *tx)
                    .await?
                } else {
                    // Update only if version matches
                    sqlx::query(&format!(
                        r#"
                        UPDATE "{}"
                        SET value = ?2,
                            version = version + 1
                        WHERE key = ?1 AND version = ?3
                        "#,
                        table
                    ))
                    .bind(key.as_ref())
                    .bind(value.as_ref())
                    .bind(snapshot_version)
                    .execute(&mut *tx)
                    .await?
                };

                if result.rows_affected() == 1 {
                    tx.commit().await?;
                    Ok(Ok(()))
                } else {
                    tx.rollback().await?;
                    let next = store.new_cas(&key).await?;
                    Ok(Err(CasError::CasFailed(next)))
                }
            })
        };

        Ok(CasGuard::new(current_cb, swap_cb))
    }
}
