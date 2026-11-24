//! What the store needs of a database handle.

use std::future::Future;

use sqlite_rs::Executor;
use sqlite_rs::QueryExecutor;
use sqlite_rs::SqlResult;

/// A database the part store runs on: a write transaction, a read snapshot, and finishing them.
///
/// - `begin_write` takes the database's one writer, exclusively, until the transaction is
///   committed, rolled back or dropped (a drop rolls back).
/// - `begin_read` is a snapshot: every query through it sees the database as of the moment
///   it began, whatever commits meanwhile.
/// - Queries through one transaction run in submission order.
pub trait PartDb: Clone + Send + Sync + 'static {
    type Write: Executor + Send + 'static;
    type Read: QueryExecutor + Send + 'static;

    fn begin_write(&self) -> impl Future<Output = SqlResult<Self::Write>> + Send;
    fn begin_read(&self) -> impl Future<Output = SqlResult<Self::Read>> + Send;
    fn commit(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send;
    fn rollback(tx: Self::Write) -> impl Future<Output = SqlResult<()>> + Send;
    fn end_read(tx: Self::Read) -> impl Future<Output = SqlResult<()>> + Send;
}

#[cfg(not(target_arch = "wasm32"))]
impl PartDb for sqlite_rs::Database {
    type Write = sqlite_rs::Tx;
    type Read = sqlite_rs::ReadTx;

    fn begin_write(&self) -> impl Future<Output = SqlResult<Self::Write>> + Send {
        sqlite_rs::Database::begin_write(self)
    }

    fn begin_read(&self) -> impl Future<Output = SqlResult<Self::Read>> + Send {
        sqlite_rs::Database::begin_read(self)
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
