//! SQLite state for one embedder-owned delta walker.

use crate::sqlite::queries;
use big_sync_core::delta_walker_sparse_state::{
    DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
};
use big_sync_core::delta_walker_state::{
    DeltaWalkerProgress, DeltaWalkerStateError, DeltaWalkerStateRepo, DeltaWalkerStateResult,
    DeltaWalkerStateTransaction,
};
use sqlite_rs::{Database, Sql, Tx};
use std::sync::Arc;
use utils_rs::prelude::async_trait;

#[derive(Clone)]
pub struct SqliteDeltaWalkerStateRepo {
    sql: Database,
    namespace: Arc<str>,
    consumer_id: Arc<str>,
}

impl SqliteDeltaWalkerStateRepo {
    pub async fn new(
        database: Database,
        namespace: impl Into<Arc<str>>,
        consumer_id: impl Into<Arc<str>>,
    ) -> DeltaWalkerStateResult<Self> {
        database
            .apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await
            .map_err(backend)?;
        let namespace: Arc<str> = namespace.into();
        let consumer_id: Arc<str> = consumer_id.into();
        queries::walker_init_progress(&database, namespace.to_string(), consumer_id.to_string())
            .await
            .map_err(backend)?;
        Ok(Self {
            sql: database,
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

    /// The reader id a source log's retention registration for a consumer
    /// identity must use.
    ///
    /// The `("namespace", "consumer_id")` pair IS the consumer identity — it
    /// keys every row in this repo — so deriving the retention reader id from it
    /// means one consumer cannot register two retention rows and two consumers
    /// cannot share one, without either caller spelling the pair out.
    pub fn retention_reader_id_of(namespace: &str, consumer_id: &str) -> String {
        format!("{namespace}/{consumer_id}")
    }

    /// The reader id a source log's retention registration for this consumer
    /// must use.
    pub fn retention_reader_id(&self) -> String {
        Self::retention_reader_id_of(self.namespace(), self.consumer_id())
    }

    pub async fn begin(&self) -> DeltaWalkerStateResult<SqliteDeltaWalkerStateTransaction> {
        let transaction = self.sql.begin_write().await.map_err(backend)?;
        Ok(self.begin_with_context(transaction))
    }

    pub fn begin_with_context(&self, transaction: Tx) -> SqliteDeltaWalkerStateTransaction {
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
        = Tx
    where
        Self: 'a;
    type Transaction<'a>
        = SqliteDeltaWalkerStateTransaction
    where
        Self: 'a;

    async fn progress(&self) -> DeltaWalkerStateResult<DeltaWalkerProgress> {
        let revision = queries::walker_progress(
            &self.sql,
            self.namespace.to_string(),
            self.consumer_id.to_string(),
        )
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
        queries::walker_state_value(
            &self.sql,
            self.namespace.to_string(),
            self.consumer_id.to_string(),
            key.to_vec(),
        )
        .await
        .map_err(backend)
    }

    async fn get_many(&self, keys: &[Vec<u8>]) -> DeltaWalkerStateResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = Sql::new(
            "SELECT state_key, state_value FROM delta_walker_key_state WHERE namespace = ",
        );
        query.bind(self.namespace.as_ref());
        query.push(" AND consumer_id = ");
        query.bind(self.consumer_id.as_ref());
        query.push(" AND state_key IN ");
        query.bind_list(keys.iter().map(Vec::as_slice));
        query
            .query(&self.sql)
            .await
            .and_then(sqlite_rs::ResultSet::all)
            .map_err(backend)
    }
}

pub struct SqliteDeltaWalkerStateTransaction {
    transaction: Option<Tx>,
    namespace: Arc<str>,
    consumer_id: Arc<str>,
}

impl SqliteDeltaWalkerStateTransaction {
    fn tx(&mut self) -> DeltaWalkerStateResult<&mut Tx> {
        self.transaction.as_mut().ok_or_else(|| {
            backend(std::io::Error::other(
                "sqlite delta walker state transaction missing",
            ))
        })
    }

    async fn current_revision(&mut self) -> DeltaWalkerStateResult<u64> {
        let namespace = self.namespace.to_string();
        let consumer_id = self.consumer_id.to_string();
        let revision = queries::walker_progress(self.tx()?, namespace, consumer_id)
            .await
            .map_err(backend)?
            // `new` seeds the progress row before any transaction can start.
            .expect("delta walker progress row present");
        revision.try_into().map_err(backend)
    }
}

#[async_trait]
impl DeltaWalkerStateTransaction for SqliteDeltaWalkerStateTransaction {
    type Context = Tx;

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
        let namespace = self.namespace.to_string();
        let consumer_id = self.consumer_id.to_string();
        let changed = queries::walker_advance_progress(
            self.tx()?,
            i64::try_from(next).map_err(backend)?,
            namespace,
            consumer_id,
            i64::try_from(expected).map_err(backend)?,
        )
        .await
        .map_err(backend)?
        .rows_affected;
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
impl DeltaWalkerSparseStateTransaction for SqliteDeltaWalkerStateTransaction {
    async fn get(&mut self, key: &[u8]) -> DeltaWalkerStateResult<Option<Vec<u8>>> {
        let namespace = self.namespace.to_string();
        let consumer_id = self.consumer_id.to_string();
        queries::walker_state_value(self.tx()?, namespace, consumer_id, key.to_vec())
            .await
            .map_err(backend)
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> DeltaWalkerStateResult<()> {
        let namespace = self.namespace.to_string();
        let consumer_id = self.consumer_id.to_string();
        queries::walker_put_state(self.tx()?, namespace, consumer_id, key, value)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> DeltaWalkerStateResult<()> {
        let namespace = self.namespace.to_string();
        let consumer_id = self.consumer_id.to_string();
        queries::walker_delete_state(self.tx()?, namespace, consumer_id, key.to_vec())
            .await
            .map_err(backend)?;
        Ok(())
    }
}

fn backend<E: std::error::Error + Send + Sync + 'static>(error: E) -> DeltaWalkerStateError {
    DeltaWalkerStateError::Backend(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::delta_walker_state::contract::{
        DeltaWalkerStateContractHarness, assert_delta_walker_state_contract,
    };
    use sqlite_rs::SqliteRuntime;

    struct SqliteContractHarness {
        _sql: Database,
        repo: SqliteDeltaWalkerStateRepo,
    }

    impl SqliteContractHarness {
        async fn new() -> Self {
            let sql = SqliteRuntime::new(Default::default())
                .ephemeral(&[])
                .expect("create sqlite contract database");
            let repo = SqliteDeltaWalkerStateRepo::new(
                sql.clone(),
                "contract-namespace",
                "contract-consumer",
            )
            .await
            .expect("create sqlite delta walker state repo");
            Self { _sql: sql, repo }
        }
    }

    impl DeltaWalkerStateContractHarness for SqliteContractHarness {
        type Repo = SqliteDeltaWalkerStateRepo;

        fn repo(&self) -> &Self::Repo {
            &self.repo
        }

        fn key(&self, index: u64) -> Vec<u8> {
            format!("branch-{index}").into_bytes()
        }

        fn value(&self, index: u64) -> Vec<u8> {
            format!("heads-{index}").into_bytes()
        }
    }

    #[tokio::test]
    async fn sqlite_delta_walker_state_contract() {
        assert_delta_walker_state_contract(&SqliteContractHarness::new().await).await;
    }

    #[tokio::test]
    async fn namespaces_and_consumers_are_isolated() {
        let sql = SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .expect("create sqlite database");
        let repo_a = SqliteDeltaWalkerStateRepo::new(sql.clone(), "ns-a", "consumer-a")
            .await
            .expect("create repo a");
        let repo_b = SqliteDeltaWalkerStateRepo::new(sql.clone(), "ns-b", "consumer-b")
            .await
            .expect("create repo b");

        let mut tx = repo_a.begin().await.expect("begin repo a transaction");
        tx.put(b"key".to_vec(), b"value-a".to_vec())
            .await
            .expect("stage repo a sparse put");
        tx.advance_from(0, 5).await.expect("advance repo a cursor");
        tx.commit().await.expect("commit repo a");

        assert_eq!(
            repo_b
                .progress()
                .await
                .expect("repo b progress")
                .upstream_revision,
            0,
            "repo b cursor must not see repo a's advance"
        );
        assert_eq!(
            repo_b.get(b"key").await.expect("repo b sparse get"),
            None,
            "repo b sparse state must not see repo a's rows"
        );
    }

    #[tokio::test]
    async fn begin_with_context_round_trips_a_caller_owned_transaction() {
        let sql = SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .expect("create sqlite database");
        let repo = SqliteDeltaWalkerStateRepo::new(sql.clone(), "ctx-namespace", "ctx-consumer")
            .await
            .expect("create sqlite delta walker state repo");

        // The settlement path: the consumer's effect and the walker state
        // advance share one caller-owned transaction, so a rollback discards
        // both.
        let outer = sql.begin_write().await.expect("begin outer transaction");
        let mut tx = repo.begin_with_context(outer);
        tx.put(b"branch-1".to_vec(), b"heads-1".to_vec())
            .await
            .expect("stage sparse put in outer transaction");
        tx.advance_from(0, 1)
            .await
            .expect("stage cursor advance in outer transaction");
        tx.rollback().await.expect("rollback outer transaction");

        assert_eq!(
            repo.progress()
                .await
                .expect("progress after outer rollback")
                .upstream_revision,
            0,
            "cursor advance must roll back with the caller-owned transaction"
        );
        assert_eq!(
            repo.get(b"branch-1")
                .await
                .expect("sparse get after outer rollback"),
            None,
            "sparse put must roll back with the caller-owned transaction"
        );

        // A committed caller-owned transaction persists both.
        let outer = sql.begin_write().await.expect("begin outer transaction");
        let mut tx = repo.begin_with_context(outer);
        tx.put(b"branch-1".to_vec(), b"heads-1".to_vec())
            .await
            .expect("stage sparse put in outer transaction");
        tx.advance_from(0, 1)
            .await
            .expect("stage cursor advance in outer transaction");
        tx.commit().await.expect("commit outer transaction");

        assert_eq!(
            repo.progress()
                .await
                .expect("progress after outer commit")
                .upstream_revision,
            1
        );
        assert_eq!(
            repo.get(b"branch-1")
                .await
                .expect("sparse get after outer commit"),
            Some(b"heads-1".to_vec())
        );
    }
}
