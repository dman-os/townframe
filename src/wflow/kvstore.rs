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

        // Create table if it doesn't exist
        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    key BLOB PRIMARY KEY,
                    value BLOB NOT NULL
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

// Implement KvStore for Arc<SqliteKvStore> to match the pattern used by DHashMap
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

        Ok(row.map(|v| -> Arc<[u8]> { v.into_boxed_slice().into() }))
    }

    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        // Get old value first
        let old_value = self.get(&key).await?;

        sqlx::query(&format!(
            r#"
                INSERT INTO "{}"(key, value) VALUES (?1, ?2)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                "#,
            self.table_name
        ))
        .bind(key.as_ref())
        .bind(value.as_ref())
        .execute(&self.db_pool)
        .await?;

        Ok(old_value)
    }

    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        // Get old value first
        let old_value = self.get(key).await?;

        sqlx::query(&format!(
            r#"DELETE FROM "{}" WHERE key = ?1"#,
            self.table_name
        ))
        .bind(key)
        .execute(&self.db_pool)
        .await?;

        Ok(old_value)
    }

    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        // Use CAS to atomically increment
        const MAX_CAS_RETRIES: usize = 100;
        let mut cas = self.new_cas(key).await?;
        for _attempt in 0..MAX_CAS_RETRIES {
            let current = cas.current();
            let current_value = if let Some(bytes) = current {
                // Try to parse as i64 (little-endian, 8 bytes)
                if bytes.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes);
                    i64::from_le_bytes(buf)
                } else {
                    return Err(ferr!(
                        "cannot increment: value is not a valid i64 (expected 8 bytes, got {})",
                        bytes.len()
                    ));
                }
            } else {
                0
            };

            let new_value = current_value
                .checked_add(delta)
                .ok_or_else(|| ferr!("integer overflow in increment"))?;

            // Store new value as little-endian bytes
            let new_bytes: Arc<[u8]> = new_value.to_le_bytes().into();
            match cas.swap(new_bytes).await? {
                Ok(()) => return Ok(new_value),
                Err(CasError::CasFailed(new_guard)) => {
                    cas = new_guard;
                    // Retry with new guard
                }
                Err(CasError::StoreError(err)) => return Err(err),
            }
        }
        Err(ferr!(
            "failed to increment after {MAX_CAS_RETRIES} CAS retries: concurrent modifications",
        ))
    }

    // FIXME: this is trigger code 5 database is locked issues
    // even without waiting for 5 seconds. The cause seems out of
    // line with other reported errors
    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        // Take a snapshot of the current value
        let snapshot = self.get(key).await?;
        let key: Arc<[u8]> = key.into();
        let store = self.clone();
        let table_name = self.table_name.clone();

        let current_cb = {
            let snapshot = snapshot.clone();
            move || snapshot.clone()
        };

        let swap_cb = move |value: Arc<[u8]>| -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>> {
            let store = store.clone();
            let key = key.clone();
            let snapshot = snapshot.clone();
            let table_name = table_name.clone();

            Box::pin(async move {
                // Use a transaction to ensure atomicity
                let mut tx = store.db_pool.begin().await?;
                
                // Get current value within transaction
                let current = sqlx::query_scalar::<_, Vec<u8>>(
                    &format!(r#"SELECT value FROM "{}" WHERE key = ?1"#, table_name),
                )
                .bind(key.as_ref())
                .fetch_optional(&mut *tx)
                .await?;
                
                let current: Option<Arc<[u8]>> = current.map(|v| -> Arc<[u8]> { v.into_boxed_slice().into() });
                
                // Compare with snapshot
                if current.as_ref().map(|v| v.as_ref()) == snapshot.as_ref().map(|v| v.as_ref()) {
                    // Values match, perform swap
                    sqlx::query(&format!(
                        r#"
                            INSERT INTO "{}"(key, value) VALUES (?1, ?2)
                            ON CONFLICT(key) DO UPDATE SET value = excluded.value
                            "#,
                        table_name
                    ))
                    .bind(key.as_ref())
                    .bind(value.as_ref())
                    .execute(&mut *tx)
                    .await?;
                    
                    tx.commit().await?;
                    Ok(Ok(()))
                } else {
                    // Values don't match, rollback and create new guard with updated snapshot
                    tx.rollback().await?;
                    let new_guard = store.new_cas(&key).await?;
                    Ok(Err(CasError::CasFailed(new_guard)))
                }
            })
        };

        Ok(CasGuard::new(current_cb, swap_cb))
    }
}

