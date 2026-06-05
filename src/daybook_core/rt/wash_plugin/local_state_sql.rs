use crate::interlude::*;

use sqlx::{Column, Row, TypeInfo, ValueRef};
use sqlx_utils_rs::SqlCtx;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::{DaybookPlugin, binds_guest, sqlite_connection};

pub struct SqliteConnectionToken {
    pub local_state_id: String,
    pub sqlite_file_path: Option<String>,
    pub sql: Option<SqlCtx>,
}

impl sqlite_connection::Host for SharedWashCtx {}

impl sqlite_connection::HostConnection for SharedWashCtx {
    async fn query(
        &mut self,
        handle: wasmtime::component::Resource<sqlite_connection::Connection>,
        query: String,
        params: Vec<binds_guest::townframe::sql::types::SqlValue>,
    ) -> wasmtime::Result<
        Result<
            Vec<binds_guest::townframe::sql::types::ResultRow>,
            binds_guest::townframe::sql::types::QueryError,
        >,
    > {
        let sql = match ensure_sqlite_ctx(self, &handle).await {
            Ok(sql) => sql,
            Err(err) => {
                return Ok(Err(
                    binds_guest::townframe::sql::types::QueryError::Unexpected(err.to_string()),
                ));
            }
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
        handle: wasmtime::component::Resource<sqlite_connection::Connection>,
        query: String,
    ) -> wasmtime::Result<Result<(), binds_guest::townframe::sql::types::QueryError>> {
        let sql = match ensure_sqlite_ctx(self, &handle).await {
            Ok(sql) => sql,
            Err(err) => {
                return Ok(Err(
                    binds_guest::townframe::sql::types::QueryError::Unexpected(err.to_string()),
                ));
            }
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
        handle: wasmtime::component::Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<String> {
        ensure_sqlite_file_path(self, &handle)
            .await
            .map_err(|err| wasmtime::Error::msg(err.to_string()))
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<sqlite_connection::Connection>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

async fn ensure_sqlite_file_path(
    ctx: &mut SharedWashCtx,
    handle: &wasmtime::component::Resource<sqlite_connection::Connection>,
) -> Res<String> {
    if let Some(path) = {
        let token = ctx
            .table
            .get(handle)
            .context("error locating sqlite-connection token")?;
        token.sqlite_file_path.clone()
    } {
        return Ok(path);
    }

    let local_state_id = {
        let token = ctx
            .table
            .get(handle)
            .context("error locating sqlite-connection token")?;
        token.local_state_id.clone()
    };

    let plugin = DaybookPlugin::from_ctx(ctx);
    let sqlite_file_path = plugin
        .sqlite_local_state_repo
        .get_sqlite_file_path(&local_state_id)
        .await?;
    let sql = plugin
        .sqlite_local_state_repo
        .ensure_sqlite_ctx(&local_state_id)
        .await?;
    let sqlite_file_path = sqlite_file_path.to_string_lossy().to_string();

    {
        let token = ctx
            .table
            .get_mut(handle)
            .context("error locating sqlite-connection token")?;
        token.sqlite_file_path = Some(sqlite_file_path.clone());
        if token.sql.is_none() {
            token.sql = Some(sql);
        }
    }

    Ok(sqlite_file_path)
}

async fn ensure_sqlite_ctx(
    ctx: &mut SharedWashCtx,
    handle: &wasmtime::component::Resource<sqlite_connection::Connection>,
) -> Res<SqlCtx> {
    if let Some(sql) = {
        let token = ctx
            .table
            .get(handle)
            .context("error locating sqlite-connection token")?;
        token.sql.clone()
    } {
        return Ok(sql);
    }

    let local_state_id = {
        let token = ctx
            .table
            .get(handle)
            .context("error locating sqlite-connection token")?;
        token.local_state_id.clone()
    };
    let plugin = DaybookPlugin::from_ctx(ctx);
    let sqlite_file_path = plugin
        .sqlite_local_state_repo
        .get_sqlite_file_path(&local_state_id)
        .await?;
    let sql = plugin
        .sqlite_local_state_repo
        .ensure_sqlite_ctx(&local_state_id)
        .await?;
    let sqlite_file_path = sqlite_file_path.to_string_lossy().to_string();

    {
        let token = ctx
            .table
            .get_mut(handle)
            .context("error locating sqlite-connection token")?;
        if token.sql.is_none() {
            token.sqlite_file_path = Some(sqlite_file_path);
            token.sql = Some(sql.clone());
        }
    }

    Ok(sql)
}

fn query_error_from_sqlx_error(err: sqlx::Error) -> binds_guest::townframe::sql::types::QueryError {
    match err {
        sqlx::Error::Database(db_err) => {
            binds_guest::townframe::sql::types::QueryError::InvalidQuery(
                db_err.message().to_string(),
            )
        }
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Encode(_) | sqlx::Error::Decode(_) => {
            binds_guest::townframe::sql::types::QueryError::InvalidParams(err.to_string())
        }
        _ => binds_guest::townframe::sql::types::QueryError::Unexpected(err.to_string()),
    }
}

fn bind_sql_value<'query>(
    query: sqlx::query::Query<'query, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    value: binds_guest::townframe::sql::types::SqlValue,
) -> sqlx::query::Query<'query, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    match value {
        binds_guest::townframe::sql::types::SqlValue::Null => query.bind(None::<String>),
        binds_guest::townframe::sql::types::SqlValue::Integer(value) => query.bind(value),
        binds_guest::townframe::sql::types::SqlValue::Real(value) => query.bind(value),
        binds_guest::townframe::sql::types::SqlValue::Text(value) => query.bind(value),
        binds_guest::townframe::sql::types::SqlValue::Blob(value) => query.bind(value),
    }
}

fn sqlite_row_to_result_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<
    binds_guest::townframe::sql::types::ResultRow,
    binds_guest::townframe::sql::types::QueryError,
> {
    let mut entries = Vec::with_capacity(row.columns().len());
    for index in 0..row.columns().len() {
        let column_name = row.columns()[index].name().to_string();
        let value_ref = row
            .try_get_raw(index)
            .map_err(query_error_from_sqlx_error)?;

        let sql_value = if value_ref.is_null() {
            binds_guest::townframe::sql::types::SqlValue::Null
        } else {
            let type_name = value_ref.type_info().name().to_ascii_uppercase();
            match type_name.as_str() {
                "INTEGER" => {
                    let value: i64 = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    binds_guest::townframe::sql::types::SqlValue::Integer(value)
                }
                "REAL" => {
                    let value: f64 = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    binds_guest::townframe::sql::types::SqlValue::Real(value)
                }
                "TEXT" => {
                    let value: String = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    binds_guest::townframe::sql::types::SqlValue::Text(value)
                }
                "BLOB" => {
                    let value: Vec<u8> = row.try_get(index).map_err(query_error_from_sqlx_error)?;
                    binds_guest::townframe::sql::types::SqlValue::Blob(value)
                }
                _ => {
                    if let Ok(value) = row.try_get::<i64, usize>(index) {
                        binds_guest::townframe::sql::types::SqlValue::Integer(value)
                    } else if let Ok(value) = row.try_get::<f64, usize>(index) {
                        binds_guest::townframe::sql::types::SqlValue::Real(value)
                    } else if let Ok(value) = row.try_get::<String, usize>(index) {
                        binds_guest::townframe::sql::types::SqlValue::Text(value)
                    } else if let Ok(value) = row.try_get::<Vec<u8>, usize>(index) {
                        binds_guest::townframe::sql::types::SqlValue::Blob(value)
                    } else {
                        return Err(binds_guest::townframe::sql::types::QueryError::Unexpected(
                            format!("unsupported sqlite value type for column '{column_name}'"),
                        ));
                    }
                }
            }
        };
        entries.push(binds_guest::townframe::sql::types::ResultRowEntry {
            column_name,
            value: sql_value,
        });
    }
    Ok(entries)
}
