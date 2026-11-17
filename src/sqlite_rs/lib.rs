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
pub use native::INSTR_SQL; // INSTR
#[cfg(not(target_arch = "wasm32"))]
pub use native::INSTR_SQL_CALLERS; // INSTR
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
pub mod gen_tests;

mod query;
pub use query::BackendRef;
pub use query::BindTuple;
pub use query::Executor;
pub use query::FromRow;
pub use query::IntoParams;
pub use query::Json;
pub use query::QueryExecutor;
pub use query::Sql;
pub use query::SqliteBackend;

#[cfg(test)]
pub use crate as sqlite_rs_alias;
