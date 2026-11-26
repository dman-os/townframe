mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use sqlite_rs::Database;
use sqlite_rs::ExecuteResult;
use sqlite_rs::ReadTx;
use sqlite_rs::SqlError;
use sqlite_rs::SqlValue;
use sqlite_rs::Tx;
use std::collections::HashSet;
use std::sync::Arc;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::WitInterfaces;
use wash_runtime::wit::WitInterface;
use wash_runtime::wit::WitWorld;
use wasmtime::component::Accessor;
use wasmtime::component::HasSelf;
use wasmtime::component::Resource;

pub mod binds_host {
    wash_runtime::wasmtime::component::bindgen!({
        world: "host",
        path: "wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        with: {
            "townframe:sqlite/sqlite-connection.connection": super::SqliteConnectionToken,
            "townframe:sqlite/sqlite-connection.transaction": super::SqliteTransactionToken,
            "townframe:sqlite/sqlite-connection.read-transaction": super::SqliteReadTransactionToken,
        }
    });
}

pub use binds_host::townframe::sqlite::sqlite_connection;
use binds_host::townframe::sqlite::types as sql_types;

/// A host-side handle to a pre-resolved sqlite database. The `db` handle and
/// file path are fully materialized by the orchestrator that creates the
/// resource (e.g. daybook facet-routine arg construction); the host impl
/// below only reads them — there is no lazy resolution.
pub struct SqliteConnectionToken {
    pub sqlite_file_path: String,
    pub db: Database,
}

/// A transaction or snapshot a guest holds. The resource table cannot be borrowed across an
/// await, so a handler clones this out of the table first. The inner `Option` is emptied when
/// the guest commits, rolls back or ends it; later calls on the resource find it finished.
struct Held<T>(Arc<tokio::sync::Mutex<Option<T>>>);

impl<T> Clone for Held<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Held<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(Some(value))))
    }

    /// Take the value out to finish it. Finishing twice is a guest bug and traps it.
    async fn take(&self) -> wasmtime::Result<T> {
        self.0
            .lock()
            .await
            .take()
            .ok_or_else(|| wasmtime::Error::msg("transaction already finalized"))
    }
}

fn finished<T>(held: &Option<T>) -> Result<&T, sql_types::QueryError> {
    held.as_ref()
        .ok_or_else(|| sql_types::QueryError::Unexpected("transaction already finalized".into()))
}

/// The write transaction behind a guest's `transaction` resource. Dropping an unfinished `Tx`
/// schedules its rollback (see sqlite_rs).
pub struct SqliteTransactionToken(Held<Tx>);

/// The read snapshot behind a guest's `read-transaction` resource.
pub struct SqliteReadTransactionToken(Held<ReadTx>);

impl SqliteConnectionToken {
    /// `begin-transaction`: take the database's writer.
    pub async fn begin_transaction(&self) -> Result<SqliteTransactionToken, sql_types::QueryError> {
        let tx = self.db.begin_write().await.map_err(to_wit_error)?;
        Ok(SqliteTransactionToken(Held::new(tx)))
    }

    /// `begin-read-transaction`: a snapshot pinned now, on a reader connection, so it does not
    /// wait for the writer and the writer does not wait for it.
    pub async fn begin_read_transaction(
        &self,
    ) -> Result<SqliteReadTransactionToken, sql_types::QueryError> {
        let snapshot = self.db.begin_read().await.map_err(to_wit_error)?;
        Ok(SqliteReadTransactionToken(Held::new(snapshot)))
    }
}

impl SqliteTransactionToken {
    fn handle(&self) -> Held<Tx> {
        self.0.clone()
    }
}

impl SqliteReadTransactionToken {
    fn handle(&self) -> Held<ReadTx> {
        self.0.clone()
    }
}

impl Held<Tx> {
    pub async fn query(
        &self,
        sql: &str,
        params: Vec<sql_types::SqlValue>,
    ) -> Result<sql_types::ResultSet, sql_types::QueryError> {
        let guard = self.0.lock().await;
        let params = host_params(params);
        finished(&guard)?
            .query(sql, &params)
            .await
            .map(to_wit_set)
            .map_err(to_wit_error)
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: Vec<sql_types::SqlValue>,
    ) -> Result<sql_types::ExecuteResult, sql_types::QueryError> {
        let guard = self.0.lock().await;
        let params = host_params(params);
        finished(&guard)?
            .execute(sql, &params)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error)
    }

    pub async fn execute_batch(
        &self,
        sql: &str,
    ) -> Result<sql_types::ExecuteResult, sql_types::QueryError> {
        let guard = self.0.lock().await;
        finished(&guard)?
            .execute_batch(sql)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error)
    }
}

impl Held<ReadTx> {
    pub async fn query(
        &self,
        sql: &str,
        params: Vec<sql_types::SqlValue>,
    ) -> Result<sql_types::ResultSet, sql_types::QueryError> {
        let guard = self.0.lock().await;
        let params = host_params(params);
        finished(&guard)?
            .query(sql, &params)
            .await
            .map(to_wit_set)
            .map_err(to_wit_error)
    }
}

impl sqlite_connection::Host for SharedWashCtx {}

impl sqlite_connection::HostConnection for SharedWashCtx {
    async fn sqlite_file_path(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<String> {
        let token = self
            .table
            .get(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(token.sqlite_file_path.clone())
    }

    async fn drop(&mut self, rep: Resource<sqlite_connection::Connection>) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl sqlite_connection::HostTransaction for SharedWashCtx {
    async fn drop(
        &mut self,
        rep: Resource<sqlite_connection::Transaction>,
    ) -> wasmtime::Result<()> {
        // Dropping an unfinished `Tx` schedules its rollback on the shared
        // workers (see sqlite_rs).
        let _token = self
            .table
            .delete(rep)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(())
    }
}

impl sqlite_connection::HostReadTransaction for SharedWashCtx {
    async fn drop(
        &mut self,
        rep: Resource<sqlite_connection::ReadTransaction>,
    ) -> wasmtime::Result<()> {
        // Dropping an unfinished `ReadTx` schedules its rollback, which releases the reader.
        let _token = self
            .table
            .delete(rep)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(())
    }
}

impl<T: 'static + Send> sqlite_connection::HostConnectionWithStore<T> for HasSelf<SharedWashCtx> {
    async fn query(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ResultSet, sql_types::QueryError>> {
        let db = connection_db(accessor, &handle)?;
        let params = host_params(params);
        Ok(db
            .query(&query, &params)
            .await
            .map(to_wit_set)
            .map_err(to_wit_error))
    }

    async fn execute(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        let db = connection_db(accessor, &handle)?;
        let params = host_params(params);
        Ok(db
            .execute(&query, &params)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error))
    }

    async fn execute_batch(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        let db = connection_db(accessor, &handle)?;
        Ok(db
            .execute_batch(&query)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error))
    }

    async fn begin_transaction(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<Result<Resource<sqlite_connection::Transaction>, sql_types::QueryError>>
    {
        let db = connection_db(accessor, &handle)?;
        let tx = match db.begin_write().await {
            Ok(tx) => tx,
            Err(err) => return Ok(Err(to_wit_error(err))),
        };
        let handle = accessor.with(|mut access| -> wasmtime::Result<_> {
            access
                .get()
                .table
                .push(SqliteTransactionToken(Held::new(tx)))
                .map_err(|err| wasmtime::Error::msg(err.to_string()))
        })?;
        Ok(Ok(handle))
    }

    async fn begin_read_transaction(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<Result<Resource<sqlite_connection::ReadTransaction>, sql_types::QueryError>>
    {
        let db = connection_db(accessor, &handle)?;
        let snapshot = match db.begin_read().await {
            Ok(snapshot) => snapshot,
            Err(err) => return Ok(Err(to_wit_error(err))),
        };
        let handle = accessor.with(|mut access| -> wasmtime::Result<_> {
            access
                .get()
                .table
                .push(SqliteReadTransactionToken(Held::new(snapshot)))
                .map_err(|err| wasmtime::Error::msg(err.to_string()))
        })?;
        Ok(Ok(handle))
    }
}

/// The database a `connection` resource names, cloned out of the resource table.
fn connection_db<T: 'static + Send>(
    accessor: &Accessor<T, HasSelf<SharedWashCtx>>,
    handle: &Resource<sqlite_connection::Connection>,
) -> wasmtime::Result<Database> {
    accessor.with(|mut access| -> wasmtime::Result<_> {
        let token = access
            .get()
            .table
            .get(handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(token.db.clone())
    })
}

fn transaction_handle<T: 'static + Send>(
    accessor: &Accessor<T, HasSelf<SharedWashCtx>>,
    handle: &Resource<sqlite_connection::Transaction>,
) -> wasmtime::Result<Held<Tx>> {
    accessor.with(|mut access| -> wasmtime::Result<_> {
        let token = access
            .get()
            .table
            .get(handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(token.handle())
    })
}

fn read_transaction_handle<T: 'static + Send>(
    accessor: &Accessor<T, HasSelf<SharedWashCtx>>,
    handle: &Resource<sqlite_connection::ReadTransaction>,
) -> wasmtime::Result<Held<ReadTx>> {
    accessor.with(|mut access| -> wasmtime::Result<_> {
        let token = access
            .get()
            .table
            .get(handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(token.handle())
    })
}

impl<T: 'static + Send> sqlite_connection::HostTransactionWithStore<T> for HasSelf<SharedWashCtx> {
    async fn query(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ResultSet, sql_types::QueryError>> {
        Ok(transaction_handle(accessor, &handle)?
            .query(&query, params)
            .await)
    }

    async fn execute(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        Ok(transaction_handle(accessor, &handle)?
            .execute(&query, params)
            .await)
    }

    async fn execute_batch(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        Ok(transaction_handle(accessor, &handle)?
            .execute_batch(&query)
            .await)
    }

    async fn commit(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Transaction>,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        let tx = transaction_handle(accessor, &handle)?.take().await?;
        Ok(tx.commit().await.map_err(to_wit_error))
    }

    async fn rollback(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::Transaction>,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        let tx = transaction_handle(accessor, &handle)?.take().await?;
        Ok(tx.rollback().await.map_err(to_wit_error))
    }
}

impl<T: 'static + Send> sqlite_connection::HostReadTransactionWithStore<T>
    for HasSelf<SharedWashCtx>
{
    async fn query(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::ReadTransaction>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ResultSet, sql_types::QueryError>> {
        Ok(read_transaction_handle(accessor, &handle)?
            .query(&query, params)
            .await)
    }

    async fn end(
        accessor: &Accessor<T, Self>,
        handle: Resource<sqlite_connection::ReadTransaction>,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        let snapshot = read_transaction_handle(accessor, &handle)?.take().await?;
        Ok(snapshot.rollback().await.map_err(to_wit_error))
    }
}

fn host_params(params: Vec<sql_types::SqlValue>) -> Vec<SqlValue> {
    params.into_iter().map(to_host_value).collect()
}

fn to_host_value(value: sql_types::SqlValue) -> SqlValue {
    match value {
        sql_types::SqlValue::Null => SqlValue::Null,
        sql_types::SqlValue::Integer(value) => SqlValue::Integer(value),
        sql_types::SqlValue::Real(value) => SqlValue::Real(value),
        sql_types::SqlValue::Text(value) => SqlValue::Text(value),
        sql_types::SqlValue::Blob(value) => SqlValue::Blob(value),
    }
}

fn to_wit_value(value: SqlValue) -> sql_types::SqlValue {
    match value {
        SqlValue::Null => sql_types::SqlValue::Null,
        SqlValue::Integer(value) => sql_types::SqlValue::Integer(value),
        SqlValue::Real(value) => sql_types::SqlValue::Real(value),
        SqlValue::Text(value) => sql_types::SqlValue::Text(value),
        SqlValue::Blob(value) => sql_types::SqlValue::Blob(value),
    }
}

fn to_wit_set(set: sqlite_rs::ResultSet) -> sql_types::ResultSet {
    let columns = set.columns.iter().cloned().collect();
    let rows = set
        .rows
        .into_iter()
        .map(|row| row.into_values().into_iter().map(to_wit_value).collect())
        .collect();
    sql_types::ResultSet { columns, rows }
}

fn to_wit_exec(out: ExecuteResult) -> sql_types::ExecuteResult {
    sql_types::ExecuteResult {
        rows_affected: out.rows_affected,
        last_insert_rowid: out.last_insert_rowid,
        result_set: to_wit_set(out.result_set),
    }
}

fn to_wit_error(err: SqlError) -> sql_types::QueryError {
    match err {
        SqlError::InvalidParams(msg) => sql_types::QueryError::InvalidParams(msg),
        SqlError::InvalidQuery(msg) => sql_types::QueryError::InvalidQuery(msg),
        SqlError::Decode { column, message } => {
            sql_types::QueryError::Unexpected(format!("decode {column}: {message}"))
        }
        SqlError::NoRows => sql_types::QueryError::InvalidQuery("query returned no rows".into()),
        SqlError::TooManyRows => {
            sql_types::QueryError::InvalidQuery("query returned more than one row".into())
        }
        SqlError::Closed => sql_types::QueryError::Unexpected("sqlite runtime closed".into()),
        SqlError::Busy(msg) => sql_types::QueryError::Busy(msg),
        SqlError::Unexpected(msg) => sql_types::QueryError::Unexpected(msg),
    }
}

/// Stateless wash runtime host plugin for `townframe:sqlite/sqlite-connection`.
/// It owns no storage: capability provisioning (resolving a `local_state_id` to
/// a `Database` + file path) is the orchestrator's job, not the plugin's. The
/// plugin only manages the wasm resource table and runs queries against the
/// pre-resolved `Database` handed to [`SqlPlugin::create_connection`].
#[derive(Default)]
pub struct SqlPlugin;

impl SqlPlugin {
    pub const ID: &str = "townframe:sqlite";
    pub fn new() -> Self {
        Self
    }

    /// Push a fully-resolved sqlite connection token into the shared resource
    /// table and return the guest-facing connection resource handle. The
    /// caller (e.g. daybook facet-routine arg construction) is responsible for
    /// having already materialized `db` and `sqlite_file_path` for the given
    /// connection.
    pub fn create_connection(
        ctx: &mut SharedWashCtx,
        token: SqliteConnectionToken,
    ) -> wasmtime::Result<Resource<sqlite_connection::Connection>> {
        ctx.table
            .push(token)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))
    }
}

#[async_trait]
impl HostPlugin for SqlPlugin {
    fn id(&self) -> &'static str {
        "townframe:sqlite"
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::new(),
            imports: HashSet::from([WitInterface::from("townframe:sqlite/sqlite-connection")]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "sqlite"
                && iface.interfaces.contains("sqlite-connection")
            {
                sqlite_connection::add_to_linker::<_, HasSelf<SharedWashCtx>>(
                    item.linker(),
                    |ctx| ctx,
                )?;
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        _resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlite_rs::Executor;
    use sqlite_rs::QueryExecutor;
    use sqlite_rs::SqlError;
    use sqlite_rs::SqliteDb;
    use sqlite_rs::WitDb;

    // The guest's side of the interface, as wit-bindgen generates it for a component: three
    // resources with async methods. Here each is wired straight to the host's tokens, so a
    // call goes through the same conversions and the same transaction handling as a call from
    // a component, minus only the component boundary. `WitDb` over these is what a wasip3
    // guest holds.
    struct Connection(SqliteConnectionToken);
    struct Transaction(SqliteTransactionToken);
    struct ReadTransaction(SqliteReadTransactionToken);

    type Rows = Result<sql_types::ResultSet, sql_types::QueryError>;
    type Executed = Result<sql_types::ExecuteResult, sql_types::QueryError>;
    type Unit = Result<(), sql_types::QueryError>;

    impl Connection {
        async fn query(&self, query: String, params: Vec<sql_types::SqlValue>) -> Rows {
            let params = host_params(params);
            let result = self.0.db.query(&query, &params).await;
            result.map(to_wit_set).map_err(to_wit_error)
        }

        async fn execute(&self, query: String, params: Vec<sql_types::SqlValue>) -> Executed {
            let params = host_params(params);
            let result = self.0.db.execute(&query, &params).await;
            result.map(to_wit_exec).map_err(to_wit_error)
        }

        async fn execute_batch(&self, query: String) -> Executed {
            let result = self.0.db.execute_batch(&query).await;
            result.map(to_wit_exec).map_err(to_wit_error)
        }

        async fn begin_transaction(&self) -> Result<Transaction, sql_types::QueryError> {
            self.0.begin_transaction().await.map(Transaction)
        }

        async fn begin_read_transaction(&self) -> Result<ReadTransaction, sql_types::QueryError> {
            self.0.begin_read_transaction().await.map(ReadTransaction)
        }
    }

    impl Transaction {
        async fn query(&self, query: String, params: Vec<sql_types::SqlValue>) -> Rows {
            self.0.0.query(&query, params).await
        }

        async fn execute(&self, query: String, params: Vec<sql_types::SqlValue>) -> Executed {
            self.0.0.execute(&query, params).await
        }

        async fn execute_batch(&self, query: String) -> Executed {
            self.0.0.execute_batch(&query).await
        }

        async fn commit(&self) -> Unit {
            let tx = self.0.0.take().await.expect("finished once");
            tx.commit().await.map_err(to_wit_error)
        }

        async fn rollback(&self) -> Unit {
            let tx = self.0.0.take().await.expect("finished once");
            tx.rollback().await.map_err(to_wit_error)
        }
    }

    impl ReadTransaction {
        async fn query(&self, query: String, params: Vec<sql_types::SqlValue>) -> Rows {
            self.0.0.query(&query, params).await
        }

        async fn end(&self) -> Unit {
            let snapshot = self.0.0.take().await.expect("ended once");
            snapshot.rollback().await.map_err(to_wit_error)
        }
    }

    sqlite_rs::impl_wit_sqlite_backend!(Connection, Transaction, ReadTransaction, sql_types,);

    /// A database the guest side reaches through `WitDb`, and the same database natively.
    fn guest_db() -> (sqlite_rs::test_support::TestDb, WitDb<Connection>) {
        let native = sqlite_rs::test_support::db(&[]).expect("test db");
        let token = SqliteConnectionToken {
            sqlite_file_path: String::new(),
            db: (*native).clone(),
        };
        (native, WitDb::new(Connection(token)))
    }

    #[tokio::test]
    async fn a_guest_gets_a_sqlite_db_over_the_interface() -> Result<(), SqlError> {
        let (native, db) = guest_db();
        assert!(!db.supports_concurrent_readers());
        native
            .execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, payload BLOB)",
            )
            .await?;

        // `write` commits on `Ok`, and everything the body wrote is there afterwards.
        let id = db
            .write(|tx| {
                Box::pin(async move {
                    let inserted = tx
                        .execute(
                            "INSERT INTO items (name, payload) VALUES (?1, ?2) RETURNING id",
                            (String::from("kept"), vec![1_u8, 2, 3]),
                        )
                        .await?;
                    inserted.result_set.one::<i64>()
                })
            })
            .await?;
        assert_eq!(id, 1);

        // It rolls back on `Err`, and that error is what comes out.
        let failed: Result<(), SqlError> = db
            .write(|tx| {
                Box::pin(async move {
                    tx.execute("INSERT INTO items (name) VALUES ('discarded')", ())
                        .await?;
                    Err(SqlError::InvalidQuery("the body failed".into()))
                })
            })
            .await;
        assert!(
            matches!(failed, Err(SqlError::InvalidQuery(ref message)) if message == "the body failed")
        );

        // A snapshot is pinned when it begins: what commits after is not in it, and it
        // cannot write.
        let snapshot = db.begin_read().await?;
        native
            .execute("INSERT INTO items (name) VALUES ('later')", ())
            .await?;
        let seen = snapshot
            .query("SELECT name FROM items ORDER BY id", ())
            .await?
            .all::<String>()?;
        assert_eq!(seen, vec!["kept"]);
        assert!(
            snapshot
                .query("INSERT INTO items (name) VALUES ('nope') RETURNING id", ())
                .await
                .is_err()
        );
        <WitDb<Connection> as SqliteDb>::end_read(snapshot).await?;

        // Blobs make the round trip intact, and a rolled back transaction leaves nothing.
        let payload = db
            .read(|snapshot| {
                Box::pin(async move {
                    snapshot
                        .query("SELECT payload FROM items WHERE id = 1", ())
                        .await?
                        .one::<Vec<u8>>()
                })
            })
            .await?;
        assert_eq!(payload, vec![1, 2, 3]);
        let tx = db.begin_write().await?;
        tx.execute("DELETE FROM items", ()).await?;
        <WitDb<Connection> as SqliteDb>::rollback(tx).await?;
        assert_eq!(
            native
                .query("SELECT COUNT(*) FROM items", ())
                .await?
                .one::<i64>()?,
            2
        );
        Ok(())
    }

    #[test]
    fn busy_crosses_the_interface_as_busy() {
        let wire = to_wit_error(SqlError::Busy("database is locked".into()));
        assert!(matches!(wire, sql_types::QueryError::Busy(_)));
    }

    #[tokio::test]
    async fn a_finished_transaction_reports_it_and_cannot_be_finished_twice() {
        let (native, _db) = guest_db();
        let token = SqliteConnectionToken {
            sqlite_file_path: String::new(),
            db: (*native).clone(),
        };
        let tx = token.begin_transaction().await.expect("begins");
        let handle = tx.handle();
        handle
            .take()
            .await
            .expect("finishes once")
            .rollback()
            .await
            .expect("rolls back");
        let error = handle
            .query("SELECT 1", Vec::new())
            .await
            .expect_err("a finished transaction answers with an error");
        assert!(
            matches!(error, sql_types::QueryError::Unexpected(ref message) if message.contains("already finalized")),
            "{error:?}"
        );
        assert!(
            handle.take().await.is_err(),
            "finishing twice traps the guest"
        );
    }
}
