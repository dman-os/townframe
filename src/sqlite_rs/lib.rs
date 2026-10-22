//! Portable SQLite query and result abstractions.
//!
//! Native builds additionally expose the threaded `SqliteRuntime`; WASM builds
//! use the same values, row decoding, cardinality checks, and executor traits
//! over the `townframe:sqlite` component interface.

mod portable;
pub use portable::ExecuteResult;
pub use portable::FromValue;
pub use portable::IntoValue;
pub use portable::ResultSet;
pub use portable::Row;
pub use portable::SqlError;
pub use portable::SqlResult;
pub use portable::SqlValue;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::Database;
#[cfg(not(target_arch = "wasm32"))]
pub use native::Migration;
#[cfg(not(target_arch = "wasm32"))]
pub use native::MigrationState;
#[cfg(not(target_arch = "wasm32"))]
pub use native::ReadTx;
#[cfg(not(target_arch = "wasm32"))]
pub use native::RuntimeConfig;
#[cfg(not(target_arch = "wasm32"))]
pub use native::Savepoint;
#[cfg(not(target_arch = "wasm32"))]
pub use native::SqliteRuntime;
#[cfg(not(target_arch = "wasm32"))]
pub use native::Tx;
#[cfg(not(target_arch = "wasm32"))]
pub use native::init_sqlite_vec;
#[cfg(not(target_arch = "wasm32"))]
pub use native::migration_status;
#[cfg(not(target_arch = "wasm32"))]
pub use native::run_migrations;

#[cfg(not(target_arch = "wasm32"))]
pub use native::test_support;
#[cfg(test)]
#[cfg(test)]
#[path = "gen_tests.rs"]
pub mod gen_tests;

#[cfg(target_arch = "wasm32")]
mod query;
#[cfg(target_arch = "wasm32")]
pub use query::BackendRef;
#[cfg(target_arch = "wasm32")]
pub use query::BindTuple;
#[cfg(target_arch = "wasm32")]
pub use query::Executor;
#[cfg(target_arch = "wasm32")]
pub use query::FromRow;
#[cfg(target_arch = "wasm32")]
pub use query::IntoParams;
#[cfg(target_arch = "wasm32")]
pub use query::Json;
#[cfg(target_arch = "wasm32")]
pub use query::QueryExecutor;
#[cfg(target_arch = "wasm32")]
pub use query::Sql;
#[cfg(target_arch = "wasm32")]
pub use query::SqliteBackend;

#[cfg(not(target_arch = "wasm32"))]
pub use native::query::BackendRef;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::BindTuple;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::Executor;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::FromRow;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::IntoParams;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::Json;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::QueryExecutor;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::Sql;
#[cfg(not(target_arch = "wasm32"))]
pub use native::query::SqliteBackend;

#[cfg(test)]
pub use crate as sqlite_rs_alias;
