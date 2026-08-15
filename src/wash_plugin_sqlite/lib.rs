mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use sqlx::{Column, Row, TypeInfo, ValueRef};
use sqlx_utils_rs::SqlCtx;
use std::collections::HashSet;
use std::ops::DerefMut;
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

/// A host-side handle to a pre-resolved sqlite connection. The `sql` ctx and
/// file path are fully materialized by the orchestrator that creates the
/// resource (e.g. daybook facet-routine arg construction); the host impl below
/// only reads them — there is no lazy resolution.
pub struct SqliteConnectionToken {
    pub sqlite_file_path: String,
    pub sql: SqlCtx,
}

pub struct SqliteTransactionToken {
    pub transaction: Option<sqlx::Transaction<'static, sqlx::Sqlite>>,
}

impl sqlite_connection::Host for SharedWashCtx {}

impl sqlite_connection::HostConnection for SharedWashCtx {
    async fn query(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
        params: Vec<sql_types::SqlValue>,
    ) -> wasmtime::Result<Result<Vec<sql_types::ResultRow>, sql_types::QueryError>> {
        let sql = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.sql.clone()
        };

        let mut sql_query = sqlx::query(sqlx::AssertSqlSafe(query));
        for param in params {
            sql_query = bind_sql_value(sql_query, param);
        }
        let rows = match sql_query.fetch_all(&sql.read_pool).await {
            Ok(rows) => rows,
            Err(err) => return Ok(Err(query_error_from_sqlx_error(err))),
        };
        let mut result_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let result_row = match sqlite_row_to_result_row(row) {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            result_rows.push(result_row);
        }
        Ok(Ok(result_rows))
    }

    async fn query_batch(
        &mut self,
        handle: Resource<sqlite_connection::Connection>,
        query: String,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        let sql = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.sql.clone()
        };
        match sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(&sql.write_pool)
            .await
        {
            Ok(_) => Ok(Ok(())),
            Err(err) => Ok(Err(query_error_from_sqlx_error(err))),
        }
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
    ) -> wasmtime::Result<Result<Resource<sqlite_connection::Transaction>, sql_types::QueryError>> {
        let sql = {
            let token = self
                .table
                .get(&handle)
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            token.sql.clone()
        };
        let tx = match sql.write_pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(tx) => tx,
            Err(err) => return Ok(Err(query_error_from_sqlx_error(err))),
        };
        let handle = self
            .table
            .push(SqliteTransactionToken {
                transaction: Some(tx),
            })
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(Ok(handle))
    }

    async fn drop(
        &mut self,
        rep: Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<()> {
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
    ) -> wasmtime::Result<Result<Vec<sql_types::ResultRow>, sql_types::QueryError>> {
        let mut sql_query = sqlx::query(sqlx::AssertSqlSafe(query));
        for param in params {
            sql_query = bind_sql_value(sql_query, param);
        }
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = token
            .transaction
            .as_mut()
            .ok_or_else(|| wasmtime::Error::msg("transaction already finalized"))?;
        let conn: &mut sqlx::SqliteConnection = tx.deref_mut();
        let rows = match sql_query.fetch_all(conn).await {
            Ok(rows) => rows,
            Err(err) => return Ok(Err(query_error_from_sqlx_error(err))),
        };
        let mut result_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let result_row = match sqlite_row_to_result_row(row) {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            result_rows.push(result_row);
        }
        Ok(Ok(result_rows))
    }

    async fn query_batch(
        &mut self,
        handle: Resource<sqlite_connection::Transaction>,
        query: String,
    ) -> wasmtime::Result<Result<(), sql_types::QueryError>> {
        let token = self
            .table
            .get_mut(&handle)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let tx = token
            .transaction
            .as_mut()
            .ok_or_else(|| wasmtime::Error::msg("transaction already finalized"))?;
        let conn: &mut sqlx::SqliteConnection = tx.deref_mut();
        match sqlx::query(sqlx::AssertSqlSafe(query)).execute(conn).await {
            Ok(_) => Ok(Ok(())),
            Err(err) => Ok(Err(query_error_from_sqlx_error(err))),
        }
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
            Err(err) => Ok(Err(query_error_from_sqlx_error(err))),
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
            Err(err) => Ok(Err(query_error_from_sqlx_error(err))),
        }
    }

    async fn drop(
        &mut self,
        rep: Resource<sqlite_connection::Transaction>,
    ) -> wasmtime::Result<()> {
        let token = self
            .table
            .delete(rep)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        if let Some(tx) = token.transaction {
            tx.rollback()
                .await
                .inspect_err(|err| error!("rollback err: {err}"))
                .ok();
        }
        Ok(())
    }
}

fn query_error_from_sqlx_error(err: sqlx::Error) -> sql_types::QueryError {
    match err {
        sqlx::Error::Database(db_err) => {
            sql_types::QueryError::InvalidQuery(db_err.message().to_string())
        }
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Encode(_) | sqlx::Error::Decode(_) => {
            sql_types::QueryError::InvalidParams(err.to_string())
        }
        _ => sql_types::QueryError::Unexpected(err.to_string()),
    }
}

fn bind_sql_value<'query>(
    query: sqlx::query::Query<'query, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    value: sql_types::SqlValue,
) -> sqlx::query::Query<'query, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    match value {
        sql_types::SqlValue::Null => query.bind(None::<String>),
        sql_types::SqlValue::Integer(value) => query.bind(value),
        sql_types::SqlValue::Real(value) => query.bind(value),
        sql_types::SqlValue::Text(value) => query.bind(value),
        sql_types::SqlValue::Blob(value) => query.bind(value),
    }
}

fn sqlite_row_to_result_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<sql_types::ResultRow, sql_types::QueryError> {
    let mut entries = Vec::with_capacity(row.columns().len());
    for index in 0..row.columns().len() {
        let column_name = row.columns()[index].name().to_string();
        let value_ref = row
            .try_get_raw(index)
            .map_err(query_error_from_sqlx_error)?;

        let sql_value = if value_ref.is_null() {
            sql_types::SqlValue::Null
        } else {
            let type_name = value_ref.type_info().name().to_ascii_uppercase();
            match type_name.as_str() {
                "INTEGER" => {
                    let value: i64 = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    sql_types::SqlValue::Integer(value)
                }
                "REAL" => {
                    let value: f64 = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    sql_types::SqlValue::Real(value)
                }
                "TEXT" => {
                    let value: String = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    sql_types::SqlValue::Text(value)
                }
                "BLOB" => {
                    let value: Vec<u8> = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    sql_types::SqlValue::Blob(value)
                }
                _ => {
                    if let Ok(value) = row.try_get::<i64, usize>(index) {
                        sql_types::SqlValue::Integer(value)
                    } else if let Ok(value) = row.try_get::<f64, usize>(index) {
                        sql_types::SqlValue::Real(value)
                    } else if let Ok(value) = row.try_get::<String, usize>(index) {
                        sql_types::SqlValue::Text(value)
                    } else if let Ok(value) = row.try_get::<Vec<u8>, usize>(index) {
                        sql_types::SqlValue::Blob(value)
                    } else {
                        return Err(sql_types::QueryError::Unexpected(format!(
                            "unsupported sqlite value type for column '{column_name}'"
                        )));
                    }
                }
            }
        };
        entries.push(sql_types::ResultRowEntry {
            column_name,
            value: sql_value,
        });
    }
    Ok(entries)
}

/// Stateless wash runtime host plugin for `townframe:sqlite/sqlite-connection`.
/// It owns no storage: capability provisioning (resolving a `local_state_id` to
/// a `SqlCtx` + file path) is the orchestrator's job, not the plugin's. The
/// plugin only manages the wasm resource table and runs queries against the
/// pre-resolved `SqlCtx` handed to [`SqlPlugin::create_connection`].
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
    /// having already materialized `sql` and `sqlite_file_path` for the given
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
            if iface.namespace == "townframe" && iface.package == "sqlite"
                && iface.interfaces.contains("sqlite-connection") {
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