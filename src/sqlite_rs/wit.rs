//! A [`SqliteDb`] over the `townframe:sqlite` component interface.
//!
//! A wasip3 guest has no SQLite of its own. Its host hands it a `connection` resource and
//! every statement is an async call to the host, which runs it on the host's own database. This
//! module turns that resource into the same `SqliteDb` the native `Database` implements, so
//! the stores written against the trait (`keyline_sqlite`, `big_automata_sqlite`,
//! `keyhive_sqlite`, ...) run in a guest unchanged.
//!
//! The generated bindings belong to the guest, so this module cannot name them.
//! `impl_wit_sqlite_backend!` implements [`WitConnection`] for the guest's generated
//! `Connection`, and `WitDb<Connection>` is the database:
//!
//! ```text
//! guest Connection --impl_wit_sqlite_backend!--> WitConnection --> WitDb<Connection>: SqliteDb
//!     begin-transaction       --> WitWrite   (query, execute, execute-batch, commit, rollback)
//!     begin-read-transaction  --> WitRead    (query, end)
//! ```
//!
//! # What the guest can rely on
//!
//! - `begin_write` and `begin_read` map to the interface's `begin-transaction` and
//!   `begin-read-transaction`. A read transaction is a snapshot pinned when it begins and has
//!   no way to write.
//! - Statements on one transaction run in the order the guest issues them, because the guest
//!   awaits each before issuing the next.
//! - `supports_concurrent_readers` is false. The interface does not promise that a snapshot
//!   and a write transaction can be open together, so a guest must not wait for the writer
//!   while it holds a snapshot. (The native host happens to allow it, since its readers
//!   are separate WAL connections.)
//! - Results arrive fully materialized and blobs are copied each way: the interface carries
//!   `list<u8>`.
//! - Dropping a transaction or snapshot hands it back to the host, which rolls back an
//!   unfinished write transaction.

use std::future::Future;
use std::sync::Arc;

use crate::ExecuteResult;
use crate::Executor;
use crate::IntoParams;
use crate::QueryExecutor;
use crate::ResultSet;
use crate::SqlResult;
use crate::SqliteBackend;
use crate::SqliteDb;
use crate::SqliteReadBackend;

/// A guest's `connection` resource and the transactions it opens. Implemented for the guest's
/// generated types by `impl_wit_sqlite_backend!`; there is nothing to write by hand.
pub trait WitConnection: SqliteBackend + Send + Sync + 'static {
    type Transaction: SqliteBackend + Send + 'static;
    type ReadTransaction: SqliteReadBackend + Send + 'static;

    fn begin_transaction(&self) -> impl Future<Output = SqlResult<Self::Transaction>> + Send;
    fn begin_read_transaction(
        &self,
    ) -> impl Future<Output = SqlResult<Self::ReadTransaction>> + Send;
    fn commit(transaction: Self::Transaction) -> impl Future<Output = SqlResult<()>> + Send;
    fn rollback(transaction: Self::Transaction) -> impl Future<Output = SqlResult<()>> + Send;
    fn end_read(transaction: Self::ReadTransaction) -> impl Future<Output = SqlResult<()>> + Send;
}

/// The database a guest holds: its `connection` resource, shared by every clone.
pub struct WitDb<C: WitConnection>(Arc<C>);

impl<C: WitConnection> WitDb<C> {
    pub fn new(connection: C) -> Self {
        Self(Arc::new(connection))
    }

    /// The connection itself, for what the interface offers beyond the trait.
    pub fn connection(&self) -> &C {
        &self.0
    }
}

impl<C: WitConnection> Clone for WitDb<C> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// An open write transaction of a [`WitDb`].
pub struct WitWrite<C: WitConnection>(C::Transaction);

/// An open read snapshot of a [`WitDb`].
pub struct WitRead<C: WitConnection>(C::ReadTransaction);

impl<C: WitConnection> QueryExecutor for WitWrite<C> {
    fn query(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl Future<Output = SqlResult<ResultSet>> + Send {
        self.0.query(sql.to_owned(), params.into_params())
    }
}

impl<C: WitConnection> Executor for WitWrite<C> {
    fn execute(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl Future<Output = SqlResult<ExecuteResult>> + Send {
        self.0.execute(sql.to_owned(), params.into_params())
    }
}

impl<C: WitConnection> QueryExecutor for WitRead<C> {
    fn query(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> impl Future<Output = SqlResult<ResultSet>> + Send {
        self.0.query(sql.to_owned(), params.into_params())
    }
}

impl<C: WitConnection> SqliteDb for WitDb<C> {
    type Write = WitWrite<C>;
    type Read = WitRead<C>;

    async fn begin_write(&self) -> SqlResult<WitWrite<C>> {
        self.0.begin_transaction().await.map(WitWrite)
    }

    async fn begin_read(&self) -> SqlResult<WitRead<C>> {
        self.0.begin_read_transaction().await.map(WitRead)
    }

    async fn commit(tx: WitWrite<C>) -> SqlResult<()> {
        C::commit(tx.0).await
    }

    async fn rollback(tx: WitWrite<C>) -> SqlResult<()> {
        C::rollback(tx.0).await
    }

    async fn end_read(tx: WitRead<C>) -> SqlResult<()> {
        C::end_read(tx.0).await
    }

    fn supports_concurrent_readers(&self) -> bool {
        false
    }
}
