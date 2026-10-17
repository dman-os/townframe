mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use sqlite_rs::{Database, ExecuteResult, SqlError, SqlValue, Tx};
use std::collections::HashSet;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::{HostPlugin, WitInterfaces};
use wash_runtime::wit::{WitInterface, WitWorld};
use wasmtime::component::{HasSelf, Resource};

pub mod binds_host {
    wash_runtime::wasmtime::component::bindgen!({
        world: "host",
        path: "wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        with: {
            "townframe:sqlite/sqlite-connection.connection": super::SqliteConnectionToken,
            "townframe:sqlite/sqlite-connection.transaction": super::SqliteTransactionToken,
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

pub struct SqliteTransactionToken {
    pub transaction: Option<Tx>,
}

impl sqlite_connection::Host for SharedWashCtx {}

impl sqlite_connection::HostConnection for SharedWashCtx {
    async fn query(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ResultSet, sql_types::QueryError>> {
        let db = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.db.clone()
        };
        let params = host_params(params);
        Ok(db
            .query(&query, &params)
            .await
            .map(to_wit_set)
            .map_err(to_wit_error))
    }

    async fn execute(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        let db = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.db.clone()
        };
        let params = host_params(params);
        Ok(db
            .execute(&query, &params)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error))
    }

    async fn execute_batch(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        let db = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.db.clone()
        };
        Ok(db
            .execute_batch(&query)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error))
    }

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

    async fn begin_transaction(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<Result<Resource<sqlite_connection::Transaction>, sql_types::QueryError>>
    {
        let db = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.db.clone()
        };
        let tx = match db.begin_write().await {
            Ok(tx) => tx,
            Err(err) => return Ok(Err(to_wit_error(err))),
        };
        let handle = self
            .table
            .push(SqliteTransactionToken {
                transaction: Some(tx),
            })
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(Ok(handle))
    }

    async fn drop(&mut self, rep: Resource<sqlite_connection::Connection>) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl sqlite_connection::HostTransaction for SharedWashCtx {
    async fn query(
        &mut self,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ResultSet, sql_types::QueryError>> {
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = match token.transaction.as_ref() {
            Some(tx) => tx,
            None => {
                return Ok(Err(sql_types::QueryError::Unexpected(
                    "transaction already finalized".into(),
                )));
            }
        };
        let params = host_params(params);
        Ok(tx
            .query(&query, &params)
            .await
            .map(to_wit_set)
            .map_err(to_wit_error))
    }

    async fn execute(
        &mut self,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = match token.transaction.as_ref() {
            Some(tx) => tx,
            None => {
                return Ok(Err(sql_types::QueryError::Unexpected(
                    "transaction already finalized".into(),
                )));
            }
        };
        let params = host_params(params);
        Ok(tx
            .execute(&query, &params)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error))
    }

    async fn execute_batch(
        &mut self,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
    ) -> wasmtime::Result<Result<sql_types::ExecuteResult, sql_types::QueryError>> {
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = match token.transaction.as_ref() {
            Some(tx) => tx,
            None => {
                return Ok(Err(sql_types::QueryError::Unexpected(
                    "transaction already finalized".into(),
                )));
            }
        };
        Ok(tx
            .execute_batch(&query)
            .await
            .map(to_wit_exec)
            .map_err(to_wit_error))
    }

    async fn commit(
        &mut self,
        handle: Resource<sqlite_connection::Transaction>,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        // `commit` is a WIT resource *method*, so `self` arrives as a `borrow`
        // handle: the guest keeps ownership and drops the handle later via
        // `[resource-drop]transaction`. Take the tx out of the token without
        // deleting the table entry; the drop will clean up the (now empty) token.
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = token
            .transaction
            .take()
            .ok_or_else(|| wasmtime::Error::msg("transaction already finalized"))?;
        match tx.commit().await {
            Ok(_) => Ok(Ok(())),
            Err(err) => Ok(Err(to_wit_error(err))),
        }
    }

    async fn rollback(
        &mut self,
        handle: Resource<sqlite_connection::Transaction>,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        // Same as `commit`: borrow method, so don't delete the table entry here.
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = token
            .transaction
            .take()
            .ok_or_else(|| wasmtime::Error::msg("transaction already finalized"))?;
        match tx.rollback().await {
            Ok(_) => Ok(Ok(())),
            Err(err) => Ok(Err(to_wit_error(err))),
        }
    }

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
