//! Portable SQLite query and result abstractions.
//!
//! Native builds additionally expose `SqliteRuntime`, which runs SQLite either on a pool of
//! worker threads or inline on the calling task (`Execution`). WASM builds use the same
//! values, row decoding, cardinality checks, and executor traits over the `townframe:sqlite`
//! component interface, and `WitDb` is the `SqliteDb` a guest gets from its host connection.

mod portable;
pub use portable::ExecuteResult;
pub use portable::FromValue;
pub use portable::IntoValue;
pub use portable::Migration;
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
pub use native::Execution;
#[cfg(not(target_arch = "wasm32"))]
pub use native::INSTR_SQL; // INSTR
#[cfg(not(target_arch = "wasm32"))]
pub use native::INSTR_SQL_CALLERS; // INSTR
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

mod handle;
pub use handle::ScopedFuture;
pub use handle::SqliteDb;

mod wit;
pub use wit::WitConnection;
pub use wit::WitDb;
pub use wit::WitRead;
pub use wit::WitWrite;

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
pub use query::SqliteReadBackend;

#[cfg(test)]
pub use crate as sqlite_rs_alias;
