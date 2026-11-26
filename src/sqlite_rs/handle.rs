//! The handle that opens read snapshots and the writer transaction.

use std::future::Future;

use crate::Executor;
use crate::QueryExecutor;
use crate::SqlResult;

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
pub trait SqliteDb: Clone + Send + Sync + 'static {
    type Write: Executor + Send + 'static;
    type Read: QueryExecutor + Send + 'static;

    fn begin_write(&self) -> impl Future<Output = SqlResult<Self::Write>> + Send;
    fn begin_read(&self) -> impl Future<Output = SqlResult<Self::Read>> + Send;
    fn commit(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send;
    fn rollback(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send;
    fn end_read(tx: Self::Read) -> impl Future<Output = SqlResult<()>> + Send;
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
}
