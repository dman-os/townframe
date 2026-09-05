//! SQLite state for one embedder-owned delta walker.

use big_sync_core::delta_walker_sparse_state::{
    DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
};
use big_sync_core::delta_walker_state::{
    DeltaWalkerProgress, DeltaWalkerStateError, DeltaWalkerStateRepo, DeltaWalkerStateResult,
    DeltaWalkerStateTransaction,
};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::sync::Arc;
use utils_rs::prelude::async_trait;

#[derive(Clone)]
pub struct SqliteDeltaWalkerStateRepo {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    namespace: Arc<str>,
    consumer_id: Arc<str>,
}

impl SqliteDeltaWalkerStateRepo {
    pub async fn new(
        read_pool: SqlitePool,
        write_pool: SqlitePool,
        namespace: impl Into<Arc<str>>,
        consumer_id: impl Into<Arc<str>>,
    ) -> DeltaWalkerStateResult<Self> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS delta_walker_progress (
                namespace TEXT NOT NULL
              , consumer_id TEXT NOT NULL
              , upstream_revision INTEGER NOT NULL
              , PRIMARY KEY(namespace, consumer_id)
            ) STRICT"#,
        )
        .execute(&write_pool)
        .await
        .map_err(backend)?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS delta_walker_key_state (
                namespace TEXT NOT NULL
              , consumer_id TEXT NOT NULL
              , state_key BLOB NOT NULL
              , state_value BLOB NOT NULL
              , PRIMARY KEY(namespace, consumer_id, state_key)
            ) STRICT"#,
        )
        .execute(&write_pool)
        .await
        .map_err(backend)?;
        let namespace = namespace.into();
        let consumer_id = consumer_id.into();
        sqlx::query(
            "INSERT OR IGNORE INTO delta_walker_progress(namespace, consumer_id, upstream_revision) VALUES (?, ?, 0)",
        )
        .bind(namespace.as_ref())
        .bind(consumer_id.as_ref())
        .execute(&write_pool)
        .await
        .map_err(backend)?;
        Ok(Self {
            read_pool,
            write_pool,
            namespace,
            consumer_id,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    pub async fn begin(&self) -> DeltaWalkerStateResult<SqliteDeltaWalkerStateTransaction<'_>> {
        let transaction = self
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(backend)?;
        Ok(self.begin_with_context(transaction))
    }

    pub fn begin_with_context<'a>(
        &'a self,
        transaction: Transaction<'a, Sqlite>,
    ) -> SqliteDeltaWalkerStateTransaction<'a> {
        SqliteDeltaWalkerStateTransaction {
            transaction: Some(transaction),
            namespace: Arc::clone(&self.namespace),
            consumer_id: Arc::clone(&self.consumer_id),
        }
    }
}

#[async_trait]
impl DeltaWalkerStateRepo for SqliteDeltaWalkerStateRepo {
    type Context<'a>
        = Transaction<'a, Sqlite>
    where
        Self: 'a;
    type Transaction<'a>
        = SqliteDeltaWalkerStateTransaction<'a>
    where
        Self: 'a;

    async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
        let revision: Option<i64> = sqlx::query_scalar(
            "SELECT upstream_revision FROM delta_walker_progress WHERE namespace = ? AND consumer_id = ?",
        )
        .bind(self.namespace.as_ref())
        .bind(self.consumer_id.as_ref())
        .fetch_optional(&self.read_pool)
        .await
        .map_err(backend)?;
        Ok(DeltaWalkerProgress {
            upstream_revision: revision.unwrap_or_default().try_into().map_err(backend)?,
        })
    }

    async fn begin<'a>(&'a self) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
        SqliteDeltaWalkerStateRepo::begin(self).await
    }

    async fn begin_with_context<'a>(
        &'a self,
        context: Self::Context<'a>,
    ) -> DeltaWalkerStateResult<Self::Transaction<'a>> {
        Ok(SqliteDeltaWalkerStateRepo::begin_with_context(
            self, context,
        ))
    }
}

#[async_trait]
impl DeltaWalkerSparseStateRepo for SqliteDeltaWalkerStateRepo {
    async fn get(&self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
        sqlx::query_scalar(
            "SELECT state_value FROM delta_walker_key_state WHERE namespace = ? AND consumer_id = ? AND state_key = ?",
        )
        .bind(self.namespace.as_ref())
        .bind(self.consumer_id.as_ref())
        .bind(key)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(backend)
    }

    async fn get_many(&self, keys: &[Vec<u8>]) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT state_key, state_value FROM delta_walker_key_state WHERE namespace = ",
        );
        query.push_bind(self.namespace.as_ref());
        query.push(" AND consumer_id = ");
        query.push_bind(self.consumer_id.as_ref());
        query.push(" AND state_key IN (");
        let mut separated = query.separated(", ");
        for key in keys {
            separated.push_bind(key.as_slice());
        }
        separated.push_unseparated(")");
        let rows = query
            .build()
            .fetch_all(&self.read_pool)
            .await
            .map_err(backend)?;
        rows.into_iter()
            .map(|row| {
                use sqlx::Row;
                Ok((
                    row.try_get("state_key").map_err(backend)?,
                    row.try_get("state_value").map_err(backend)?,
                ))
            })
            .collect()
    }
}

pub struct SqliteDeltaWalkerStateTransaction<'a> {
    transaction: Option<Transaction<'a, Sqlite>>,
    namespace: Arc<str>,
    consumer_id: Arc<str>,
}

impl<'a> SqliteDeltaWalkerStateTransaction<'a> {
    fn tx(&mut self) -> DeltaWalkerStateResult<&mut Transaction<'a, Sqlite>> {
        self.transaction.as_mut().ok_or_else(|| {
            backend(std::io::Error::other(
                "sqlite delta walker state transaction missing",
            ))
        })
    }

    async fn current_revision(&mut self) -> DeltaWalkerStateResult<u64> {
        let namespace = Arc::clone(&self.namespace);
        let consumer_id = Arc::clone(&self.consumer_id);
        let revision: i64 = sqlx::query_scalar(
            "SELECT upstream_revision FROM delta_walker_progress WHERE namespace = ? AND consumer_id = ?",
        )
        .bind(namespace.as_ref())
        .bind(consumer_id.as_ref())
        .fetch_one(&mut **self.tx()?)
        .await
        .map_err(backend)?;
        revision.try_into().map_err(backend)
    }
}

#[async_trait]
impl<'a> DeltaWalkerStateTransaction for SqliteDeltaWalkerStateTransaction<'a> {
    type Context = Transaction<'a, Sqlite>;

    fn context_mut(&mut self) -> &mut Self::Context {
        self.transaction
            .as_mut()
            .expect("sqlite delta walker state transaction present")
    }

    async fn progress(&mut self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
        Ok(DeltaWalkerProgress {
            upstream_revision: self.current_revision().await?,
        })
    }

    async fn advance_from(&mut self, expected: u64, next: u64) -> DeltaWalkerStateResult<()> {
        if next <= expected {
            return Err(DeltaWalkerStateError::NonAdvancingRevision {
                current: expected,
                next,
            });
        }
        let changed = sqlx::query(
            "UPDATE delta_walker_progress SET upstream_revision = ? WHERE namespace = ? AND consumer_id = ? AND upstream_revision = ?",
        )
        .bind(i64::try_from(next).map_err(backend)?)
        .bind(self.namespace.as_ref())
        .bind(self.consumer_id.as_ref())
        .bind(i64::try_from(expected).map_err(backend)?)
        .execute(&mut **self.tx()?)
        .await
        .map_err(backend)?
        .rows_affected();
        if changed != 1 {
            return Err(DeltaWalkerStateError::StaleProgress { expected });
        }
        Ok(())
    }

    async fn commit(mut self) -> DeltaWalkerStateResult<()> {
        self.transaction
            .take()
            .expect("sqlite delta walker state transaction present")
            .commit()
            .await
            .map_err(backend)
    }

    async fn rollback(mut self) -> DeltaWalkerStateResult<()> {
        self.transaction
            .take()
            .expect("sqlite delta walker state transaction present")
            .rollback()
            .await
            .map_err(backend)
    }
}

#[async_trait]
impl<'a> DeltaWalkerSparseStateTransaction for SqliteDeltaWalkerStateTransaction<'a> {
    async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
        let namespace = Arc::clone(&self.namespace);
        let consumer_id = Arc::clone(&self.consumer_id);
        sqlx::query_scalar(
            "SELECT state_value FROM delta_walker_key_state WHERE namespace = ? AND consumer_id = ? AND state_key = ?",
        )
        .bind(namespace.as_ref())
        .bind(consumer_id.as_ref())
        .bind(key)
        .fetch_optional(&mut **self.tx()?)
        .await
        .map_err(backend)
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()> {
        sqlx::query(
            "INSERT INTO delta_walker_key_state(namespace, consumer_id, state_key, state_value) VALUES (?, ?, ?, ?) ON CONFLICT(namespace, consumer_id, state_key) DO UPDATE SET state_value = excluded.state_value",
        )
        .bind(self.namespace.as_ref())
        .bind(self.consumer_id.as_ref())
        .bind(key)
        .bind(value)
        .execute(&mut **self.tx()?)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()> {
        sqlx::query(
            "DELETE FROM delta_walker_key_state WHERE namespace = ? AND consumer_id = ? AND state_key = ?",
        )
        .bind(self.namespace.as_ref())
        .bind(self.consumer_id.as_ref())
        .bind(key)
        .execute(&mut **self.tx()?)
        .await
        .map_err(backend)?;
        Ok(())
    }
}

fn backend<E: std::error::Error + Send + Sync + 'static>(error: E) -> DeltaWalkerStateError {
    DeltaWalkerStateError::Backend(Box::new(error))
}
