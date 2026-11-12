use crate::BatchOp;
use crate::OrderedStore;
use crate::StoreError;
use crate::prefix_successor;
use crate::sqlite::queries;
use sqlite_rs::Database;
use sqlite_rs::SqlError;

/// Native store over one `clay_store_entry` table in a `sqlite_rs` database.
#[derive(Clone)]
pub struct SqliteStore {
    db: Database,
}

/// Splits `SqlError` at the edge: `Closed` and `Unexpected` are runtime/I/O
/// failures and become `StoreError`; the rest mean our own generated queries
/// are wrong, an invariant break, so they panic.
trait SqlResultExt<T> {
    fn or_store_error(self) -> Result<T, StoreError>;
}

impl<T> SqlResultExt<T> for Result<T, SqlError> {
    fn or_store_error(self) -> Result<T, StoreError> {
        self.map_err(|error| match error {
            SqlError::Closed | SqlError::Unexpected(_) => StoreError::backend(error),
            SqlError::InvalidParams(_)
            | SqlError::InvalidQuery(_)
            | SqlError::Decode { .. }
            | SqlError::NoRows
            | SqlError::TooManyRows => panic!("clay store query invariant broken: {error}"),
        })
    }
}

impl SqliteStore {
    /// Applies this crate's migrations to `db` (idempotent) and wraps it. The
    /// table is private to this store; sharing the file with other units is fine
    /// since each unit keeps its own migration history table.
    pub async fn open(db: Database) -> Result<Self, StoreError> {
        db.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await
            .or_store_error()?;
        Ok(Self { db })
    }
}

impl OrderedStore for SqliteStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        queries::get_entry(&self.db, key.to_vec())
            .await
            .or_store_error()
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let lower = prefix.to_vec();
        let rows = match prefix_successor(prefix) {
            Some(upper) => queries::scan_range(&self.db, lower, upper)
                .await
                .or_store_error()?
                .into_iter()
                .map(|row| (row.key, row.value))
                .collect(),
            None => queries::scan_from(&self.db, lower)
                .await
                .or_store_error()?
                .into_iter()
                .map(|row| (row.key, row.value))
                .collect(),
        };
        Ok(rows)
    }

    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError> {
        // Dropping an uncommitted `Tx` rolls it back, so any `?` below leaves
        // no trace of the ops that already ran.
        let tx = self.db.begin_write().await.or_store_error()?;
        for op in batch {
            match op {
                BatchOp::Put(key, value) => {
                    queries::put_entry(&tx, key, value).await.or_store_error()?;
                }
                BatchOp::Delete(key) => {
                    queries::delete_entry(&tx, key).await.or_store_error()?;
                }
                BatchOp::DeletePrefix(prefix) => match prefix_successor(&prefix) {
                    Some(upper) => {
                        queries::delete_range(&tx, prefix, upper)
                            .await
                            .or_store_error()?;
                    }
                    None => {
                        queries::delete_from(&tx, prefix).await.or_store_error()?;
                    }
                },
            }
        }
        tx.commit().await.or_store_error()?;
        Ok(())
    }
}
