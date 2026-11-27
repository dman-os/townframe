//! The handle that opens read snapshots and the writer transaction.

use std::future::Future;
use std::pin::Pin;

use crate::Executor;
use crate::QueryExecutor;
use crate::SqlError;
use crate::SqlResult;

/// The future a scoped transaction body returns. It may borrow the transaction it was given,
/// which is why it is boxed: a closure cannot otherwise name a result that borrows its argument.
pub type ScopedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A database handle: it opens a write transaction and read snapshots, and finishes them.
///
/// Stores (`big_automata_sqlite`, `keyline_sqlite`) are generic over this trait, not over
/// [`Database`](crate::Database), so a platform whose SQLite is a component connection can
/// implement it. The `Write` type is the whole contract between stores that share a database:
/// each store runs its queries through the one `Executor` the caller holds, so rows written by
/// different stores commit or roll back together.
///
/// - `begin_write` takes the database's one writer, exclusively, until the transaction is
///   committed, rolled back or dropped (a drop rolls back).
/// - `begin_read` is a snapshot: every query through it sees the database as of the moment it
///   began, whatever commits meanwhile.
/// - Queries through one transaction run in submission order.
/// - `supports_concurrent_readers` says whether a snapshot may be open while the writer is
///   held. Where it is false a snapshot and the writer share one connection, and a task that
///   holds a snapshot and then waits for the writer waits for itself.
///
/// # Holding the writer
///
/// A write transaction holds the one writer of the database for as long as it is open, so
/// every other writer waits. Open it, decide in memory, write, commit: do not await a network
/// reply, a timer or a channel in between. [`write`](Self::write) makes the open-and-finish
/// part structural. The transaction exists only inside the closure and is committed when the
/// closure returns `Ok` and rolled back when it returns `Err` or panics, so it cannot be kept
/// across later awaits or forgotten. It cannot stop the closure from awaiting something
/// foreign: statements are async, so the type system cannot tell them from other futures.
pub trait SqliteDb: Clone + Send + Sync + 'static {
    type Write: Executor + Send + 'static;
    type Read: QueryExecutor + Send + 'static;

    fn begin_write(&self) -> impl Future<Output = SqlResult<Self::Write>> + Send;
    fn begin_read(&self) -> impl Future<Output = SqlResult<Self::Read>> + Send;
    fn commit(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send;
    fn rollback(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send;
    fn end_read(tx: Self::Read) -> impl Future<Output = SqlResult<()>> + Send;

    /// Whether a read snapshot can be open while a write transaction is. True for the native
    /// WAL database, whose writer and readers are separate connections; false for a backend
    /// with one connection, which is what a component guest must assume of its host.
    fn supports_concurrent_readers(&self) -> bool;

    /// Run `body` in a write transaction: commit when it returns `Ok`, roll back when it
    /// returns `Err` (which is then the result) or panics.
    ///
    /// ```ignore
    /// let id = db
    ///     .write(|tx| Box::pin(async move {
    ///         let id = insert_row(tx, "a").await?;
    ///         link_row(tx, id).await?;
    ///         Ok::<_, SqlError>(id)
    ///     }))
    ///     .await?;
    /// ```
    fn write<T, E, F>(&self, body: F) -> impl Future<Output = Result<T, E>> + Send
    where
        T: Send,
        E: From<SqlError> + Send,
        F: for<'t> FnOnce(&'t mut Self::Write) -> ScopedFuture<'t, Result<T, E>> + Send,
    {
        async move {
            let mut tx = self.begin_write().await?;
            match body(&mut tx).await {
                Ok(value) => {
                    Self::commit(tx).await?;
                    Ok(value)
                }
                Err(error) => {
                    // The body's error is the cause worth reporting. Dropping the transaction
                    // rolls it back, and a rollback that fails there crashes, as it does for
                    // every dropped transaction.
                    drop(tx);
                    Err(error)
                }
            }
        }
    }

    /// Run `body` against a read snapshot and release the snapshot when it finishes.
    fn read<T, E, F>(&self, body: F) -> impl Future<Output = Result<T, E>> + Send
    where
        T: Send,
        E: From<SqlError> + Send,
        F: for<'t> FnOnce(&'t Self::Read) -> ScopedFuture<'t, Result<T, E>> + Send,
    {
        async move {
            let snapshot = self.begin_read().await?;
            let result = body(&snapshot).await;
            match result {
                Ok(value) => {
                    Self::end_read(snapshot).await?;
                    Ok(value)
                }
                Err(error) => {
                    drop(snapshot);
                    Err(error)
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SqliteDb for crate::Database {
    type Write = crate::Tx;
    type Read = crate::ReadTx;

    fn begin_write(&self) -> impl Future<Output = SqlResult<Self::Write>> + Send {
        crate::Database::begin_write(self)
    }

    fn begin_read(&self) -> impl Future<Output = SqlResult<Self::Read>> + Send {
        crate::Database::begin_read(self)
    }

    fn commit(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send {
        tx.commit()
    }

    fn rollback(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send {
        tx.rollback()
    }

    fn end_read(tx: Self::Read) -> impl Future<Output = SqlResult<()>> + Send {
        tx.rollback()
    }

    fn supports_concurrent_readers(&self) -> bool {
        crate::Database::supports_concurrent_readers(self)
    }
}
