//! Portable SQLite runtime shared by native callers and the
//! `townframe:sqlite` WASI host.
//!
//! Three things that `sqlx`/`SqlCtx` fuse together are deliberately
//! separated here:
//!
//! ```text
//! Database handle  (cheap, lazy: no thread, no connection)
//!        |
//! SqliteRuntime    (one per process: bounded shared workers + eviction)
//!        |
//! Connections      (opened on first use, leased per op/tx, evicted idle)
//! ```
//!
//! A `Database` handle is just configuration: creating one must not pay
//! the per-database pool/thread cost noted in `daybook_core::local_state`.
//! Execution happens on the runtime's shared blocking workers; connections
//! exist only while a database is in use, plus an idle grace period.
//!
//! Concurrency model (WAL): one write connection per database leased
//! exclusively (at most one writer / one write transaction per database),
//! up to `max_readers_per_db` read connections leased per operation. A
//! transaction leases its *connection* for its whole lifetime, never a
//! worker thread: each statement is dispatched to the shared pool and the
//! connection sits thread-free between statements.
//!
//! # Boot, embedded migrations, and raw SQL
//!
//! Migration bodies are compiled into the executable, not loaded at deployment.
//! Registration is lazy; `migrate()` eagerly applies pending versions. Ordinary
//! operations also migrate on first use. `migration_status()` never applies migrations
//! or creates a missing database. History must be a contiguous prefix of the supplied
//! migration list; changed checksums and unknown versions are errors. There is no SQLx
//! history adoption. Each migration has its own transaction.
//!
//! ```no_run
//! use sqlite_rs::{Migration, RuntimeConfig, SqliteRuntime, IntoValue};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let runtime = SqliteRuntime::new(RuntimeConfig::default());
//! const MIGRATIONS: &[Migration<'static>] = &[Migration {
//!     version: 1,
//!     name: "001_init.sql",
//!     sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/sqlite/migrations/001_init.sql")),
//! }];
//! let db = runtime.database("state.sqlite", MIGRATIONS, "_app_migrations");
//! let status = db.migration_status().await?;
//! db.migrate().await?;
//! let inserted = db.execute(
//!     "INSERT INTO items (name) VALUES (:name) RETURNING id",
//!     &["Ada".into_value()],
//! ).await?;
//! let id: i64 = inserted.result_set.one()?;
//! assert_eq!(inserted.rows_affected, 1);
//! let name: Option<String> = db.query(
//!     "SELECT name FROM items WHERE id = :id", &[id.into_value()],
//! ).await?.optional()?;
//! db.execute_batch("CREATE TABLE IF NOT EXISTS marker (id INTEGER);").await?;
//! # Ok(()) }
//! ```
//!
//! Defaults: four workers, two readers per database, 30-second idle eviction,
//! an 8000-KiB page-cache budget and 64 cached statements per connection. Clones of
//! a `Database` share its pools; register each database once at boot and clone that
//! handle. Handles keep the runtime alive. `ephemeral()` returns a file-backed test
//! database whose directory is retained by its handles and in-flight operations.
//!
//! # RAII transactions, savepoints, and read snapshots
//!
//! Commit is explicit. Drop queues rollback, retaining the lease until cleanup
//! completes; use `rollback().await` to wait. Cancellation does not interrupt an
//! already-submitted statement or commit. Per-transaction submission order survives
//! cancellation. A savepoint exclusively borrows its parent; use the guard for all
//! work inside it. `release()` keeps changes, `rollback()` and Drop discard them.
//! Raw transaction-control SQL is rejected: use the typed methods instead.
//!
//! ```
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = sqlite_rs::test_support::db(&[])?;
//! db.execute_batch("CREATE TABLE items (name TEXT NOT NULL)").await?;
//! let mut tx = db.begin_write().await?;
//! tx.execute("INSERT INTO items VALUES ('kept')", &[]).await?;
//! {
//!     let mut part = tx.savepoint("import").await?;
//!     part.execute("INSERT INTO items VALUES ('discarded')", &[]).await?;
//!     let nested = part.savepoint("nested").await?;
//!     nested.release().await?;
//! }
//! tx.commit().await?;
//! let snapshot = db.begin_read().await?;
//! db.execute("INSERT INTO items VALUES ('later')", &[]).await?;
//! assert_eq!(snapshot.query("SELECT name FROM items", &[]).await?.all::<String>()?, vec!["kept"]);
//! snapshot.rollback().await?;
//! # Ok(()) }
//! # tokio::runtime::Runtime::new().unwrap().block_on(example()).unwrap();
//! ```
//!
//! Read snapshots are pinned at `begin_read()`, not at the first user query. They
//! use a reader lease, permit concurrent WAL writers, and expose no execute method.
//! `Database::query` and snapshot queries reject writes before stepping statements.
//!
//! ```compile_fail
//! # async fn example(db: sqlite_rs::Database) {
//! let snapshot = db.begin_read().await.unwrap();
//! snapshot.execute("DELETE FROM items", &[]).await.unwrap();
//! # }
//! ```
//!
//! ```compile_fail
//! # async fn example(db: sqlite_rs::Database) {
//! let mut tx = db.begin_write().await.unwrap();
//! let part = tx.savepoint("part").await.unwrap();
//! tx.commit().await.unwrap();
//! part.release().await.unwrap();
//! # }
//! ```
//!
//! # Dynamic SQL and existing Rust types
//!
//! `push` accepts trusted SQL fragments only. Values go through `bind`, `bind_list`,
//! or `bind_tuples`; identifiers go through `ident` (ASCII letters/digits/underscore,
//! optionally hyphens after the first character, quoted as one identifier). Invalid
//! identifiers panic. Empty lists produce SQLite's valid `IN ()`: false for `IN`,
//! true for `NOT IN`. Oversized statements return SQLite's parameter-limit error;
//! there is no hidden chunking or implicit change in transaction semantics.
//!
//! ```
//! use sqlite_rs::{FromRow, Row, Sql, SqlResult};
//! #[derive(Debug, PartialEq)]
//! struct Item { id: i64, name: String }
//! impl FromRow for Item {
//!     fn from_row(row: &Row) -> SqlResult<Self> {
//!         Ok(Self { id: row.get("id")?, name: row.get("name")? })
//!     }
//! }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = sqlite_rs::test_support::db(&[])?;
//! db.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)").await?;
//! Sql::new("INSERT INTO items (name) VALUES (").bind("Ada").push(")").execute(&db).await?;
//! let mut sql = Sql::new("SELECT id, name FROM ");
//! sql.ident("items").push(" WHERE (id, name) IN ").bind_tuples([(1_i64, "Ada")]);
//! let items: Vec<Item> = sql.query(&db).await?.all()?;
//! assert_eq!(items, vec![Item { id: 1, name: "Ada".into() }]);
//! let pair: (i64, String) = db.query("SELECT id, name FROM items", &[]).await?.one()?;
//! # Ok(()) }
//! # tokio::runtime::Runtime::new().unwrap().block_on(example()).unwrap();
//! ```
//!
//! `one()` requires exactly one row; `optional()` permits zero or one; both reject
//! multiple rows. `all()` accepts any count. Scalars and tuples require an exact
//! column count. Named `FromRow` mappings can select the fields they need. Decode
//! errors include the column name; text decoding is strict UTF-8. `Json<T>` supports
//! explicit fallible serialization through `into_value()` and typed JSON decoding.
//!
//! # Checked-in SQL and generated rows
//!
//! Put migrations in `sqlite/migrations/NNN_name.sql`, queries in `sqlite/queries/`,
//! then run `cargo x sql-gen <crate-dir>`. The generated `sqlite/gen.rs` contains
//! embedded `MIGRATIONS` and functions generic over `QueryExecutor` or `Executor`:
//! the same function accepts a database, transaction, savepoint, or (for reads) snapshot.
//!
//! ```sql
//! -- name: get_item :optional
//! -- param: id i64
//! -- returns: id i64, name String
//! SELECT id
//!      , name
//!   FROM items
//!  WHERE id = :id;
//! ```
//!
//! ```rust,standalone_crate
//! use sqlite_rs as sqlite_rs_alias;
//! # mod gen_tests {
//! #     pub struct ItemSummary { pub id: i64, pub name: String }
//! #     pub type ExternalRange = std::ops::Range<i64>;
//! # }
//! mod queries {
//!     include!(concat!(env!("CARGO_MANIFEST_DIR"), "/sqlite/gen.rs"));
//! }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = sqlite_rs::test_support::db(queries::MIGRATIONS)?;
//! let tx = db.begin_write().await?;
//! let inserted = queries::insert_returning(&tx, "Ada".into()).await?;
//! let item = queries::get_item(&tx, inserted.id).await?.unwrap();
//! assert_eq!(item.name, "Ada");
//! tx.commit().await?;
//! let names = queries::names_by_ids(&db, sqlite_rs::Json(vec![inserted.id])).await?;
//! assert_eq!(names, vec!["Ada"]);
//! # Ok(()) }
//! # fn main() { tokio::runtime::Runtime::new().unwrap().block_on(example()).unwrap(); }
//! ```
//!
//! Multi-column queries generate `GetItemRow` with public fields and `FromRow`;
//! single-column queries return scalars. Add `-- into: crate::items::Item` to construct
//! an existing struct instead. Its fields must be accessible and match the declared
//! result; Rust compilation checks that contract. For private fields or invariant-
//! enforcing constructors, convert a generated row with `TryFrom` instead.
//!
//! Headers support `i64`, `f64`, `String`, `Vec<u8>`, `bool`, their `Option` forms,
//! and `Json<Vec<T>>`. `json_each(:ids)` supports a static list query. The checker
//! prepares SQL and checks parameter order and result names; SQLite's dynamic types
//! still require runtime decoding. `:one`/`:optional`/`:many` writes with `RETURNING`
//! use the writer connection and return decoded rows; `:exec` returns `ExecuteResult`.
//!
//! ```text
//! cargo x sql-check --all
//! cargo x sql-gen --check --all
//! cargo x sql-migrate src/app state.sqlite --table _app_migrations --status
//! cargo x sql-migrate src/app state.sqlite --table _app_migrations --dry-run
//! cargo x sql-migrate src/app state.sqlite --table _app_migrations
//! ```
//!
//! `--dry-run` reports the pending plan without executing migration SQL. It is not
//! a rehearsal of writes against the live database.

mod migrations;
mod query;
mod transaction;
pub use migrations::{MigrationState, migration_status, run_migrations};
pub use query::{BindTuple, Executor, FromRow, IntoParams, Json, QueryExecutor, Sql};
pub use transaction::{ReadTx, Savepoint, Tx};

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use color_eyre::eyre::{Result as Res, WrapErr};
use rusqlite::Connection as RusConnection;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};
use utils_rs::expect_tags::{ERROR_CHANNEL, ERROR_MUTEX};

// ---------------------------------------------------------------------------
// Values, rows, errors: the wire shape shared with townframe:sqlite.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub trait IntoValue {
    fn into_value(self) -> SqlValue;
}
impl IntoValue for SqlValue {
    fn into_value(self) -> SqlValue {
        self
    }
}
impl IntoValue for bool {
    fn into_value(self) -> SqlValue {
        SqlValue::Integer(i64::from(self))
    }
}
impl IntoValue for &String {
    fn into_value(self) -> SqlValue {
        SqlValue::Text(self.clone())
    }
}
impl IntoValue for i64 {
    fn into_value(self) -> SqlValue {
        SqlValue::Integer(self)
    }
}
impl IntoValue for i32 {
    fn into_value(self) -> SqlValue {
        SqlValue::Integer(i64::from(self))
    }
}
impl IntoValue for f64 {
    fn into_value(self) -> SqlValue {
        SqlValue::Real(self)
    }
}
impl IntoValue for String {
    fn into_value(self) -> SqlValue {
        SqlValue::Text(self)
    }
}
impl IntoValue for &str {
    fn into_value(self) -> SqlValue {
        SqlValue::Text(self.to_string())
    }
}
impl IntoValue for Vec<u8> {
    fn into_value(self) -> SqlValue {
        SqlValue::Blob(self)
    }
}
impl IntoValue for &[u8] {
    fn into_value(self) -> SqlValue {
        SqlValue::Blob(self.to_vec())
    }
}
impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> SqlValue {
        match self {
            Some(value) => value.into_value(),
            None => SqlValue::Null,
        }
    }
}

/// Decode a value out of a row. This is the whole row-mapping surface:
/// codegen emits calls to `row.get::<T>("column")`.
pub trait FromValue: Sized {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self>;
}

impl FromValue for SqlValue {
    fn from_value(value: &SqlValue, _: &str) -> SqlResult<Self> {
        Ok(value.clone())
    }
}

macro_rules! from_value {
    ($t:ty, $pat:pat => $out:expr) => {
        impl FromValue for $t {
            fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
                match value {
                    $pat => Ok($out),
                    other => Err(SqlError::Decode {
                        column: column.to_string(),
                        message: format!("expected {}, got {other:?}", stringify!($t)),
                    }),
                }
            }
        }
    };
}
from_value!(i64, SqlValue::Integer(value) => *value);
from_value!(String, SqlValue::Text(value) => value.clone());
from_value!(Vec<u8>, SqlValue::Blob(value) => value.clone());
from_value!(bool, SqlValue::Integer(value) => *value != 0);
impl FromValue for f64 {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        match value {
            SqlValue::Real(value) => Ok(*value),
            // SQLite is dynamically typed: an integral REAL may come back
            // as INTEGER storage.
            SqlValue::Integer(value) => Ok(*value as f64),
            other => Err(SqlError::Decode {
                column: column.to_string(),
                message: format!("expected f64, got {other:?}"),
            }),
        }
    }
}
impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        match value {
            SqlValue::Null => Ok(None),
            value => T::from_value(value, column).map(Some),
        }
    }
}
impl FromValue for u64 {
    fn from_value(value: &SqlValue, column: &str) -> SqlResult<Self> {
        let integer = i64::from_value(value, column)?;
        u64::try_from(integer).map_err(|_| SqlError::Decode {
            column: column.to_string(),
            message: format!("negative integer {integer} does not fit u64"),
        })
    }
}

/// Column names once, positional values per row. (Carrying the column
/// name on every row entry is the WIT v1 mistake not repeated here.)
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResultSet {
    pub columns: Arc<[String]>,
    pub rows: Vec<Row>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    columns: Arc<[String]>,
    values: Vec<SqlValue>,
}

impl Row {
    pub fn into_values(self) -> Vec<SqlValue> {
        self.values
    }
    pub fn get<T: FromValue>(&self, column: &str) -> SqlResult<T> {
        let idx = self
            .columns
            .iter()
            .position(|name| name == column)
            .ok_or_else(|| SqlError::Decode {
                column: column.to_string(),
                message: "column not present in result".into(),
            })?;
        T::from_value(&self.values[idx], column)
    }
    pub fn get_at<T: FromValue>(&self, index: usize) -> SqlResult<T> {
        let value = self.values.get(index).ok_or_else(|| SqlError::Decode {
            column: format!("#{index}"),
            message: "column index out of range".into(),
        })?;
        T::from_value(value, self.columns.get(index).map_or("#?", String::as_str))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExecuteResult {
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
    pub result_set: ResultSet,
}

/// Mirrors `townframe:sqlite`'s `query-error` so the host maps ~1:1.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("decode error at column '{column}': {message}")]
    Decode { column: String, message: String },
    #[error("query returned no rows")]
    NoRows,
    #[error("query returned more than one row")]
    TooManyRows,
    #[error("sqlite runtime closed")]
    Closed,
    #[error("{0}")]
    Unexpected(String),
}

impl From<rusqlite::Error> for SqlError {
    fn from(err: rusqlite::Error) -> Self {
        use rusqlite::Error::*;
        match &err {
            InvalidParameterCount(..)
            | InvalidParameterName(_)
            | InvalidColumnType(..)
            | ToSqlConversionFailure(_) => SqlError::InvalidParams(err.to_string()),
            QueryReturnedNoRows | InvalidColumnName(_) | SqliteFailure(..) => {
                SqlError::InvalidQuery(err.to_string())
            }
            _ => SqlError::Unexpected(err.to_string()),
        }
    }
}

pub type SqlResult<T> = Result<T, SqlError>;

// ---------------------------------------------------------------------------
// Runtime config
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub workers: usize,
    pub max_readers_per_db: usize,
    pub idle_timeout: Duration,
    pub cache_size_kib: i64,
    pub statement_cache_size: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            max_readers_per_db: 2,
            idle_timeout: Duration::from_secs(30),
            cache_size_kib: -8000,
            statement_cache_size: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Migration<'sql> {
    pub version: i64,
    pub name: &'sql str,
    pub sql: &'sql str,
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

type Job = Box<dyn FnOnce() + Send>;

struct RuntimeInner {
    cfg: RuntimeConfig,
    tx: std::sync::mpsc::Sender<Job>,
    open_connections: Arc<AtomicUsize>,
    databases: StdMutex<Vec<std::sync::Weak<DbInner>>>,
    _eviction_lifetime: oneshot::Sender<()>,
}

impl RuntimeInner {
    async fn call<T, F>(&self, operation: F) -> SqlResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> SqlResult<T> + Send + 'static,
    {
        self.call_on(None, operation).await
    }

    async fn call_on<T, F>(
        &self,
        queue: Option<&Arc<transaction::SerialQueue>>,
        operation: F,
    ) -> SqlResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> SqlResult<T> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel::<std::thread::Result<SqlResult<T>>>();
        let job: Job = Box::new(move || {
            let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
            if let Err(Err(payload)) = reply_tx.send(out) {
                std::panic::resume_unwind(payload);
            }
        });
        match queue {
            Some(queue) => queue.enqueue(&self.tx, job)?,
            None => self.tx.send(job).map_err(|_| SqlError::Closed)?,
        }
        match reply_rx.await.map_err(|_| SqlError::Closed)? {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    #[cfg(test)]
    fn try_call<F>(&self, operation: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx.send(Box::new(operation)).is_ok()
    }
}

/// One open connection. The lease semaphore guarantees exclusivity; the
/// mutex only makes the connection shareable across worker threads.
struct ConnSlot {
    conn: Arc<StdMutex<RusConnection>>,
    counter: Arc<AtomicUsize>,
}

impl ConnSlot {
    fn open(
        path: &std::path::Path,
        cfg: &RuntimeConfig,
        counter: Arc<AtomicUsize>,
        write: bool,
    ) -> SqlResult<Self> {
        let conn = RusConnection::open(path).map_err(SqlError::from)?;
        for (pragma, value) in [
            ("journal_mode", "WAL"),
            ("foreign_keys", "ON"),
            ("busy_timeout", "5000"),
            ("synchronous", "NORMAL"),
        ] {
            conn.pragma_update(None, pragma, value)
                .map_err(SqlError::from)?;
        }
        conn.pragma_update(None, "cache_size", cfg.cache_size_kib)
            .map_err(SqlError::from)?;
        conn.set_prepared_statement_cache_capacity(cfg.statement_cache_size);
        authorize(&conn, !write);
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            conn: Arc::new(StdMutex::new(conn)),
            counter,
        })
    }
}

impl Drop for ConnSlot {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

struct DbInner {
    path: PathBuf,
    migrations: Vec<Migration<'static>>,
    migration_table: String,
    writer: StdMutex<Option<ConnSlot>>,
    writer_gate: Arc<Semaphore>,
    readers: StdMutex<Vec<ConnSlot>>,
    reader_gate: Arc<Semaphore>,
    in_use: AtomicUsize,
    last_used: StdMutex<Instant>,
    initialized: StdMutex<bool>,
    init_lock: Mutex<()>,
    /// Returned leases are transferred to the pools together under this lock
    /// before the next acquisition selects a connection.
    overflow: StdMutex<Vec<(bool, ConnSlot)>>,
    _ephemeral_directory: Option<tempfile::TempDir>,
}

/// A lazy database handle. Cloning is cheap and opens nothing.
#[derive(Clone)]
pub struct Database {
    runtime: Arc<RuntimeInner>,
    inner: Arc<DbInner>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Database")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

struct Lease {
    slot: Option<ConnSlot>,
    _permit: OwnedSemaphorePermit,
    db: Arc<DbInner>,
    write: bool,
}

impl Lease {
    fn slot(&self) -> Arc<StdMutex<RusConnection>> {
        Arc::clone(&self.slot.as_ref().expect("lease slot present").conn)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.db.return_slot(self.write, slot);
        }
        self.db.in_use.fetch_sub(1, Ordering::SeqCst);
        *self.db.last_used.lock().expect(ERROR_MUTEX) = Instant::now();
    }
}

impl DbInner {
    fn return_slot(&self, write: bool, slot: ConnSlot) {
        // Publish the returned slot before the lease releases its semaphore
        // permit, so the next acquisition observes it in `drain_overflow`.
        self.overflow.lock().expect(ERROR_MUTEX).push((write, slot));
    }
}

impl Database {
    async fn ensure_initialized(&self) -> SqlResult<()> {
        if *self.inner.initialized.lock().expect(ERROR_MUTEX) {
            return Ok(());
        }
        let _guard = self.inner.init_lock.lock().await;
        if *self.inner.initialized.lock().expect(ERROR_MUTEX) {
            return Ok(());
        }
        // Run migrations on the writer connection, then release it.
        let lease = self.lease_inner(true).await?;
        let migrations = self.inner.migrations.clone();
        let table = self.inner.migration_table.clone();
        let connection = lease.slot();
        self.runtime
            .call(move || {
                let mut conn = connection.lock().expect(ERROR_MUTEX);
                let result = run_migrations(&mut conn, &table, &migrations);
                drop(conn);
                drop(connection);
                drop(lease);
                result
            })
            .await?;
        *self.inner.initialized.lock().expect(ERROR_MUTEX) = true;
        Ok(())
    }

    async fn lease(&self, write: bool) -> SqlResult<Lease> {
        self.ensure_initialized().await?;
        self.lease_inner(write).await
    }

    async fn lease_inner(&self, write: bool) -> SqlResult<Lease> {
        let gate = if write {
            Arc::clone(&self.inner.writer_gate)
        } else {
            Arc::clone(&self.inner.reader_gate)
        };
        let permit = gate.acquire_owned().await.map_err(|_| SqlError::Closed)?;
        self.drain_overflow();
        self.inner.in_use.fetch_add(1, Ordering::SeqCst);
        let mut lease = Lease {
            slot: None,
            _permit: permit,
            db: Arc::clone(&self.inner),
            write,
        };
        let slot = if write {
            self.inner.writer.lock().expect(ERROR_MUTEX).take()
        } else {
            self.inner.readers.lock().expect(ERROR_MUTEX).pop()
        };
        match slot {
            Some(slot) => lease.slot = Some(slot),
            None => {
                let cfg = self.runtime.cfg.clone();
                let counter = Arc::clone(&self.runtime.open_connections);
                // Opening is blocking IO: run it on a worker, using a
                // scratch closure that does not need a connection.
                lease = self
                    .runtime
                    .call(move || {
                        lease.slot = Some(ConnSlot::open(&lease.db.path, &cfg, counter, write)?);
                        Ok(lease)
                    })
                    .await?;
            }
        }
        *self.inner.last_used.lock().expect(ERROR_MUTEX) = Instant::now();
        Ok(lease)
    }

    fn drain_overflow(&self) {
        let mut overflow = self.inner.overflow.lock().expect(ERROR_MUTEX);
        for (write, slot) in overflow.drain(..) {
            if write {
                let previous = self.inner.writer.lock().expect(ERROR_MUTEX).replace(slot);
                assert!(previous.is_none(), "duplicate writer connection");
            } else {
                self.inner.readers.lock().expect(ERROR_MUTEX).push(slot);
            }
        }
    }

    async fn run<T, F>(&self, write: bool, operation: F) -> SqlResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RusConnection) -> SqlResult<T> + Send + 'static,
    {
        let lease = self.lease(write).await?;
        let connection = lease.slot();
        self.runtime
            .call(move || {
                let mut conn = connection.lock().expect(ERROR_MUTEX);
                let result = operation(&mut conn);
                drop(conn);
                drop(connection);
                drop(lease);
                result
            })
            .await
    }

    /// Run a read query. Uses a reader connection (WAL: concurrent with
    /// the writer).
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        let sql = sql.to_string();
        let params = params.into_params();
        self.run(false, move |conn| run_read_query(conn, &sql, &params))
            .await
    }

    /// Execute a statement. Uses the (single) writer connection.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        let sql = sql.to_string();
        let params = params.into_params();
        self.run(true, move |conn| run_execute(conn, &sql, &params))
            .await
    }

    /// Execute a batch of statements with no parameters (DDL, pragmas).
    pub async fn execute_batch(&self, sql: &str) -> SqlResult<ExecuteResult> {
        let sql = sql.to_string();
        self.run(true, move |conn| {
            let changes_before = conn.total_changes();
            conn.execute_batch(&sql).map_err(SqlError::from)?;
            Ok(ExecuteResult {
                rows_affected: conn.total_changes() - changes_before,
                last_insert_rowid: conn.last_insert_rowid(),
                result_set: ResultSet::default(),
            })
        })
        .await
    }

    pub async fn begin_write(&self) -> SqlResult<Tx> {
        self.begin(true).await
    }

    pub async fn begin_read(&self) -> SqlResult<ReadTx> {
        Ok(ReadTx(self.begin(false).await?))
    }

    async fn begin(&self, write: bool) -> SqlResult<Tx> {
        let transaction = Tx {
            db: self.clone(),
            lease: Some(self.lease(write).await?),
            queue: Arc::new(transaction::SerialQueue::default()),
            next_savepoint: 0,
        };
        transaction
            .run(move |connection| {
                control_batch(
                    connection,
                    if write {
                        "BEGIN IMMEDIATE"
                    } else {
                        "BEGIN DEFERRED"
                    },
                    !write,
                )?;
                if !write {
                    connection.query_row("SELECT count(*) FROM sqlite_schema", [], |_| Ok(()))?;
                }
                Ok(())
            })
            .await?;
        Ok(transaction)
    }

    pub async fn migrate(&self) -> SqlResult<()> {
        self.ensure_initialized().await
    }

    pub async fn apply_migrations(
        &self,
        table: &str,
        migrations: &[Migration<'static>],
    ) -> SqlResult<()> {
        let table = table.to_owned();
        let migrations = migrations.to_vec();
        self.run(true, move |connection| {
            run_migrations(connection, &table, &migrations)
        })
        .await
    }

    /// Status of the migrations this handle was registered with.
    pub async fn migration_status(&self) -> SqlResult<Vec<(Migration<'static>, MigrationState)>> {
        self.migration_status_for(&self.inner.migration_table, &self.inner.migrations)
            .await
    }

    /// Status of any migration list against its history table, including
    /// lists other units applied to this database with `apply_migrations`.
    /// Opens a separate read-only connection: it reports on the file as it
    /// is and does not initialize (migrate) the handle.
    pub async fn migration_status_for(
        &self,
        table: &str,
        migrations: &[Migration<'static>],
    ) -> SqlResult<Vec<(Migration<'static>, MigrationState)>> {
        let database = Arc::clone(&self.inner);
        let table = table.to_owned();
        let migrations = migrations.to_vec();
        self.runtime
            .call(move || {
                let path = &database.path;
                if !path
                    .try_exists()
                    .map_err(|error| SqlError::Unexpected(error.to_string()))?
                {
                    return migration_status(
                        &RusConnection::open_in_memory()?,
                        &table,
                        &migrations,
                    );
                }
                let connection = RusConnection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                migration_status(&connection, &table, &migrations)
            })
            .await
    }

    /// Close this database's idle connections now, if unused.
    pub async fn evict_idle(&self) {
        let Ok(_writer) = self.inner.writer_gate.try_acquire() else {
            return;
        };
        let readers = u32::try_from(self.runtime.cfg.max_readers_per_db.max(1))
            .expect("reader budget fits u32");
        let Ok(_readers) = self.inner.reader_gate.try_acquire_many(readers) else {
            return;
        };
        let idle_for = self.inner.last_used.lock().expect(ERROR_MUTEX).elapsed();
        if idle_for < self.runtime.cfg.idle_timeout {
            return;
        }
        self.inner.overflow.lock().expect(ERROR_MUTEX).clear();
        *self.inner.writer.lock().expect(ERROR_MUTEX) = None;
        self.inner.readers.lock().expect(ERROR_MUTEX).clear();
        *self.inner.initialized.lock().expect(ERROR_MUTEX) = false;
    }
}

// ---------------------------------------------------------------------------
// Runtime handle
// ---------------------------------------------------------------------------

pub struct SqliteRuntime {
    inner: Arc<RuntimeInner>,
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl SqliteRuntime {
    pub fn new(cfg: RuntimeConfig) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let rx = Arc::new(StdMutex::new(rx));
        let mut workers = Vec::new();
        for worker_index in 0..cfg.workers.max(1) {
            let rx = Arc::clone(&rx);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("sqlite-rs-{worker_index}"))
                    .spawn(move || {
                        loop {
                            let job = {
                                let guard = rx.lock().expect(ERROR_MUTEX);
                                guard.recv()
                            };
                            match job {
                                Ok(job) => job(),
                                Err(_) => return,
                            }
                        }
                    })
                    .expect("failed spawning sqlite worker thread"),
            );
        }
        let (eviction_lifetime, mut eviction_stopped) = oneshot::channel();
        let inner = Arc::new(RuntimeInner {
            cfg,
            tx,
            open_connections: Arc::new(AtomicUsize::new(0)),
            databases: StdMutex::new(Vec::new()),
            _eviction_lifetime: eviction_lifetime,
        });
        // Idle eviction: one task per runtime (not per database), and
        // only where a tokio runtime already exists.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let idle_interval = inner.cfg.idle_timeout.max(Duration::from_millis(50));
            let weak_inner = Arc::downgrade(&inner);
            handle.spawn(async move {
                loop {
                    if tokio::time::timeout(idle_interval, &mut eviction_stopped)
                        .await
                        .is_ok()
                    {
                        break;
                    }
                    let Some(inner) = weak_inner.upgrade() else {
                        break;
                    };
                    let dbs: Vec<Arc<DbInner>> = {
                        let mut reg = inner.databases.lock().expect(ERROR_MUTEX);
                        reg.retain(|database| database.strong_count() > 0);
                        reg.iter().filter_map(std::sync::Weak::upgrade).collect()
                    };
                    for db in dbs {
                        Database {
                            runtime: Arc::clone(&inner),
                            inner: db,
                        }
                        .evict_idle()
                        .await;
                    }
                }
            });
        }
        Self {
            inner,
            _workers: workers,
        }
    }

    /// Register a lazy database. Opens nothing.
    pub fn database(
        &self,
        path: impl Into<PathBuf>,
        migrations: &[Migration<'static>],
        migration_table: &str,
    ) -> Database {
        self.register(path.into(), migrations, migration_table, None)
    }

    fn register(
        &self,
        path: PathBuf,
        migrations: &[Migration<'static>],
        migration_table: &str,
        ephemeral_directory: Option<tempfile::TempDir>,
    ) -> Database {
        let inner = Arc::new(DbInner {
            path,
            migrations: migrations.to_vec(),
            migration_table: migration_table.to_string(),
            writer: StdMutex::new(None),
            writer_gate: Arc::new(Semaphore::new(1)),
            readers: StdMutex::new(Vec::new()),
            reader_gate: Arc::new(Semaphore::new(self.inner.cfg.max_readers_per_db.max(1))),
            in_use: AtomicUsize::new(0),
            last_used: StdMutex::new(Instant::now()),
            initialized: StdMutex::new(false),
            init_lock: Mutex::new(()),
            overflow: StdMutex::new(Vec::new()),
            _ephemeral_directory: ephemeral_directory,
        });
        self.inner
            .databases
            .lock()
            .expect(ERROR_MUTEX)
            .push(Arc::downgrade(&inner));
        Database {
            runtime: Arc::clone(&self.inner),
            inner,
        }
    }

    /// Ephemeral file-backed database (temp file, not `:memory:`, so an
    /// eviction/reopen cannot silently swap in an empty database). The
    /// temporary directory is retained until all database handles and leases are dropped.
    pub fn ephemeral(&self, migrations: &[Migration<'static>]) -> Res<Database> {
        let directory = tempfile::tempdir().wrap_err("failed creating sqlite temp directory")?;
        let path = directory.path().join("database.sqlite");
        Ok(self.register(path, migrations, "_sqlite_rs_migrations", Some(directory)))
    }

    pub fn open_connection_count(&self) -> usize {
        self.inner.open_connections.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// Statement execution (runs on workers)
// ---------------------------------------------------------------------------

fn authorize(connection: &RusConnection, readonly: bool) {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    connection.authorizer(Some(move |context: AuthContext<'_>| match context.action {
        AuthAction::Transaction { .. } | AuthAction::Savepoint { .. } => Authorization::Deny,
        AuthAction::Pragma { pragma_name, .. }
            if [
                "table_info",
                "table_xinfo",
                "index_info",
                "index_xinfo",
                "index_list",
                "foreign_key_list",
            ]
            .iter()
            .any(|name| pragma_name.eq_ignore_ascii_case(name)) =>
        {
            Authorization::Allow
        }
        AuthAction::Pragma {
            pragma_value: Some(_),
            ..
        }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
            if readonly =>
        {
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }));
}

fn control_batch(connection: &RusConnection, sql: &str, readonly: bool) -> SqlResult<()> {
    connection
        .authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>);
    let result = connection.execute_batch(sql).map_err(SqlError::from);
    authorize(connection, readonly);
    result
}

fn bind_params(stmt: &mut rusqlite::Statement<'_>, params: &[SqlValue]) -> SqlResult<()> {
    if stmt.parameter_count() != params.len() {
        return Err(SqlError::InvalidParams(format!(
            "expected {} parameters, got {}",
            stmt.parameter_count(),
            params.len()
        )));
    }
    for (index, value) in params.iter().enumerate() {
        let idx = index + 1;
        match value {
            SqlValue::Null => stmt.raw_bind_parameter(idx, rusqlite::types::Null),
            SqlValue::Integer(value) => stmt.raw_bind_parameter(idx, *value),
            SqlValue::Real(value) => stmt.raw_bind_parameter(idx, *value),
            SqlValue::Text(value) => stmt.raw_bind_parameter(idx, value.as_str()),
            SqlValue::Blob(value) => stmt.raw_bind_parameter(idx, value.as_slice()),
        }
        .map_err(SqlError::from)?;
    }
    Ok(())
}

fn value_at(row: &rusqlite::Row<'_>, idx: usize, column: &str) -> SqlResult<SqlValue> {
    use rusqlite::types::ValueRef::*;
    match row.get_ref(idx).map_err(SqlError::from)? {
        Null => Ok(SqlValue::Null),
        Integer(value) => Ok(SqlValue::Integer(value)),
        Real(value) => Ok(SqlValue::Real(value)),
        Text(value) => std::str::from_utf8(value)
            .map(|text| SqlValue::Text(text.to_owned()))
            .map_err(|error| SqlError::Decode {
                column: column.to_owned(),
                message: error.to_string(),
            }),
        Blob(value) => Ok(SqlValue::Blob(value.to_vec())),
    }
}

fn run_read_query(
    conn: &mut RusConnection,
    sql: &str,
    params: &[SqlValue],
) -> SqlResult<ResultSet> {
    run_statement(conn, sql, params, true)
}

fn run_query(conn: &mut RusConnection, sql: &str, params: &[SqlValue]) -> SqlResult<ResultSet> {
    run_statement(conn, sql, params, false)
}

fn run_statement(
    conn: &mut RusConnection,
    sql: &str,
    params: &[SqlValue],
    readonly: bool,
) -> SqlResult<ResultSet> {
    let mut stmt = conn.prepare_cached(sql).map_err(SqlError::from)?;
    if readonly && !stmt.readonly() {
        return Err(SqlError::InvalidQuery(
            "write statement on read path".into(),
        ));
    }
    let columns: Arc<[String]> = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
        .into();
    bind_params(&mut stmt, params)?;
    let mut rows = Vec::new();
    {
        let mut query = stmt.raw_query();
        while let Some(row) = query.next().map_err(SqlError::from)? {
            let mut values = Vec::with_capacity(columns.len());
            for (idx, column) in columns.iter().enumerate() {
                values.push(value_at(row, idx, column)?);
            }
            rows.push(Row {
                columns: Arc::clone(&columns),
                values,
            });
        }
    }
    Ok(ResultSet { columns, rows })
}

fn run_execute(
    conn: &mut RusConnection,
    sql: &str,
    params: &[SqlValue],
) -> SqlResult<ExecuteResult> {
    let changes_before = conn.total_changes();
    let result_set = run_query(conn, sql, params)?;
    Ok(ExecuteResult {
        rows_affected: if conn.total_changes() == changes_before {
            0
        } else {
            conn.changes()
        },
        last_insert_rowid: conn.last_insert_rowid(),
        result_set,
    })
}

pub mod test_support {
    use super::*;

    pub struct TestDb {
        database: Database,
        pub(super) _runtime: SqliteRuntime,
    }

    impl std::ops::Deref for TestDb {
        type Target = Database;

        fn deref(&self) -> &Self::Target {
            &self.database
        }
    }

    impl QueryExecutor for TestDb {
        async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
            self.database.query(sql, params).await
        }
    }

    impl Executor for TestDb {
        async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
            self.database.execute(sql, params).await
        }
    }

    pub fn db(migrations: &[Migration<'static>]) -> Res<TestDb> {
        let runtime = SqliteRuntime::new(RuntimeConfig {
            workers: 2,
            ..Default::default()
        });
        let database = runtime.ephemeral(migrations)?;
        Ok(TestDb {
            database,
            _runtime: runtime,
        })
    }
}

/// Register the `sqlite-vec` extension as an auto-extension so every
/// subsequently opened connection has it. Guarded to run once.
pub fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        let entry_point: unsafe extern "C" fn(
            *mut libsqlite3_sys::sqlite3,
            *mut *mut std::ffi::c_char,
            *const libsqlite3_sys::sqlite3_api_routines,
        ) -> i32 = std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ());
        libsqlite3_sys::sqlite3_auto_extension(Some(entry_point));
    });
}

#[cfg(test)]
use crate as sqlite_rs_alias;

#[cfg(test)]
mod gen_tests;
#[cfg(test)]
mod surface_tests;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::gen_tests::queries::MIGRATIONS;

    fn runtime() -> SqliteRuntime {
        SqliteRuntime::new(RuntimeConfig {
            workers: 2,
            max_readers_per_db: 2,
            idle_timeout: Duration::from_millis(50),
            cache_size_kib: -2000,
            statement_cache_size: 64,
        })
    }

    #[tokio::test]
    async fn lazy_handle_opens_nothing() {
        let rt = runtime();
        let db = rt.ephemeral(&[]).expect("ephemeral");
        drop(db);
        assert_eq!(rt.open_connection_count(), 0);
    }

    #[tokio::test]
    async fn last_database_drop_releases_runtime_and_workers() -> SqlResult<()> {
        let mut runtime = SqliteRuntime::new(RuntimeConfig::default());
        let workers = std::mem::take(&mut runtime._workers);
        let lifetime = Arc::downgrade(&runtime.inner);
        let database = runtime.ephemeral(&[]).expect("ephemeral");
        drop(runtime);
        assert!(lifetime.upgrade().is_some());
        database
            .execute_batch("CREATE TABLE retained (id INTEGER)")
            .await?;
        drop(database);
        assert!(lifetime.upgrade().is_none());
        for worker in workers {
            worker.join().expect("sqlite worker panicked");
        }
        Ok(())
    }

    #[tokio::test]
    async fn migrations_and_roundtrip() -> SqlResult<()> {
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        let inserted = db
            .execute(
                "INSERT INTO items (name, payload, score) VALUES (:name, :payload, :score)",
                &[
                    SqlValue::Text("hello".into()),
                    SqlValue::Blob(vec![1, 2, 3]),
                    SqlValue::Real(1.5),
                ],
            )
            .await?;
        assert_eq!(inserted.rows_affected, 1);
        assert_eq!(inserted.last_insert_rowid, 1);

        let set = db
            .query(
                "SELECT id, name, payload, score FROM items WHERE name = :name",
                &[SqlValue::Text("hello".into())],
            )
            .await?;
        assert_eq!(set.rows.len(), 1);
        let row = &set.rows[0];
        assert_eq!(row.get::<i64>("id")?, 1);
        assert_eq!(row.get::<String>("name")?, "hello");
        assert_eq!(row.get::<Vec<u8>>("payload")?, vec![1, 2, 3]);
        assert_eq!(row.get::<f64>("score")?, 1.5);
        assert!(row.get::<String>("payload").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn named_and_positional_params_both_bind() -> SqlResult<()> {
        // rusqlite binds raw parameters positionally; named placeholders
        // occupy positional slots in order of appearance.
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        db.execute(
            "INSERT INTO items (name) VALUES (:name)",
            &[SqlValue::Text("n".into())],
        )
        .await?;
        let set = db
            .query(
                "SELECT name FROM items WHERE name = :name AND id = ?2",
                &[SqlValue::Text("n".into()), SqlValue::Integer(1)],
            )
            .await?;
        assert_eq!(set.rows.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn owned_tx_commits_and_rolls_back() -> SqlResult<()> {
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        let tx = db.begin_write().await?;
        tx.execute("INSERT INTO items (name) VALUES ('a')", &[])
            .await?;
        tx.commit().await?;
        let count =
            db.query("SELECT COUNT(*) AS c FROM items", &[]).await?.rows[0].get::<i64>("c")?;
        assert_eq!(count, 1);

        let tx = db.begin_write().await?;
        tx.execute("INSERT INTO items (name) VALUES ('c')", &[])
            .await?;
        tx.rollback().await?;
        let count =
            db.query("SELECT COUNT(*) AS c FROM items", &[]).await?.rows[0].get::<i64>("c")?;
        assert_eq!(count, 1);
        Ok(())
    }

    pub(super) async fn block_workers(runtime: &SqliteRuntime) -> Vec<std::sync::mpsc::Sender<()>> {
        let mut blockers = Vec::new();
        for _ in 0..runtime.inner.cfg.workers.max(1) {
            let (release, released) = std::sync::mpsc::channel();
            let (started, ready) = oneshot::channel();
            assert!(runtime.inner.try_call(move || {
                started.send(()).expect("worker start receiver dropped");
                assert!(released.recv().is_err());
            }));
            blockers.push(release);
            ready.await.expect("worker did not start");
        }
        blockers
    }

    #[tokio::test]
    async fn dropped_tx_rolls_back() -> SqlResult<()> {
        let runtime = runtime();
        let database = runtime.ephemeral(MIGRATIONS).expect("ephemeral");
        let transaction = database.begin_write().await?;
        transaction
            .execute("INSERT INTO items (name) VALUES ('pending')", &[])
            .await?;
        let connection = transaction
            .lease
            .as_ref()
            .expect("transaction lease")
            .slot();
        let blockers = block_workers(&runtime).await;
        drop(transaction);
        assert_eq!(database.inner.writer_gate.available_permits(), 0);
        assert!(
            database
                .inner
                .overflow
                .lock()
                .expect(ERROR_MUTEX)
                .is_empty()
        );
        let mut next = Box::pin(database.begin_write());
        assert!(
            std::future::poll_fn(|context| std::task::Poll::Ready(
                std::future::Future::poll(next.as_mut(), context).is_pending()
            ))
            .await
        );
        drop(blockers);
        let next = next.await?;
        assert!(Arc::ptr_eq(
            &connection,
            &next.lease.as_ref().expect("transaction lease").slot()
        ));
        assert!(
            next.query("SELECT name FROM items", &[])
                .await?
                .rows
                .is_empty()
        );
        next.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn abandoned_pending_rollback_discards_connection() -> SqlResult<()> {
        let runtime = runtime();
        let database = runtime.ephemeral(MIGRATIONS).expect("ephemeral");
        let mut transaction = database.begin_write().await?;
        transaction
            .execute("INSERT INTO items (name) VALUES ('abandoned')", &[])
            .await?;
        let pending = transaction::PendingRollback {
            lease: transaction.lease.take(),
        };
        drop(transaction);
        drop(pending);
        assert_eq!(database.inner.writer_gate.available_permits(), 1);
        assert_eq!(runtime.open_connection_count(), 0);
        let transaction = database.begin_write().await?;
        assert!(
            transaction
                .query("SELECT name FROM items", &[])
                .await?
                .rows
                .is_empty()
        );
        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_commit_retains_writer_until_job_finishes() -> SqlResult<()> {
        let runtime = runtime();
        let database = runtime.ephemeral(MIGRATIONS).expect("ephemeral");
        let transaction = database.begin_write().await?;
        transaction
            .execute("INSERT INTO items (name) VALUES ('committed')", &[])
            .await?;
        let blockers = block_workers(&runtime).await;
        let mut commit = Box::pin(transaction.commit());
        assert!(
            std::future::poll_fn(|context| std::task::Poll::Ready(
                std::future::Future::poll(commit.as_mut(), context).is_pending()
            ))
            .await
        );
        drop(commit);
        assert_eq!(database.inner.writer_gate.available_permits(), 0);
        drop(blockers);
        let transaction = database.begin_write().await?;
        let result = transaction.query("SELECT name FROM items", &[]).await?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get::<String>("name")?, "committed");
        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn failed_commit_rolls_back_before_reusing_connection() -> SqlResult<()> {
        let runtime = runtime();
        let database = runtime.ephemeral(&[]).expect("ephemeral");
        database
            .execute_batch(
                "CREATE TABLE parent (id INTEGER PRIMARY KEY);
             CREATE TABLE child (
                 id INTEGER PRIMARY KEY
               , parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
             );",
            )
            .await?;
        let transaction = database.begin_write().await?;
        let connection = transaction
            .lease
            .as_ref()
            .expect("transaction lease")
            .slot();
        transaction
            .execute("INSERT INTO child VALUES (1, 42)", &[])
            .await?;
        let error = transaction
            .commit()
            .await
            .expect_err("deferred constraint should fail");
        assert!(matches!(error, SqlError::InvalidQuery(_)), "{error}");
        assert!(connection.lock().expect(ERROR_MUTEX).is_autocommit());
        let transaction = database.begin_write().await?;
        assert!(Arc::ptr_eq(
            &connection,
            &transaction
                .lease
                .as_ref()
                .expect("transaction lease")
                .slot()
        ));
        assert!(
            transaction
                .query("SELECT id FROM child", &[])
                .await?
                .rows
                .is_empty()
        );
        transaction
            .execute_batch("INSERT INTO parent VALUES (42); INSERT INTO child VALUES (1, 42);")
            .await?;
        transaction.commit().await?;
        assert_eq!(
            database.query("SELECT id FROM child", &[]).await?.rows[0].get::<i64>("id")?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn worker_panic_is_resumed_on_caller_without_losing_worker() -> SqlResult<()> {
        let runtime = SqliteRuntime::new(RuntimeConfig {
            workers: 1,
            ..Default::default()
        });
        let worker = Arc::clone(&runtime.inner);
        let failure = tokio::spawn(async move {
            worker
                .call::<(), _>(|| std::panic::panic_any(String::from("worker panic payload")))
                .await
        })
        .await
        .expect_err("caller should panic");
        assert!(failure.is_panic());
        let payload = failure.into_panic();
        assert_eq!(
            payload.downcast_ref::<String>().map(String::as_str),
            Some("worker panic payload")
        );
        assert_eq!(runtime.inner.call(|| Ok(42)).await?, 42);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_utf8_is_a_decode_error() -> SqlResult<()> {
        let runtime = runtime();
        let database = runtime.ephemeral(&[]).expect("ephemeral");
        let sql = "SELECT CAST(x'80' AS TEXT) AS body";
        for result in [
            database.query(sql, &[]).await,
            database
                .execute(sql, &[])
                .await
                .map(|result| result.result_set),
        ] {
            let error = result.expect_err("invalid text must not be replaced");
            assert!(
                matches!(error, SqlError::Decode { ref column, .. } if column == "body"),
                "{error}"
            );
        }
        let transaction = database.begin_write().await?;
        let error = transaction
            .query(sql, &[])
            .await
            .expect_err("transaction should reject invalid text");
        assert!(
            matches!(error, SqlError::Decode { ref column, .. } if column == "body"),
            "{error}"
        );
        transaction.rollback().await?;
        let result = database
            .query("SELECT CAST(x'CEB1' AS TEXT) AS body, x'80' AS raw", &[])
            .await?;
        assert_eq!(
            result.rows.into_iter().next().expect("row").into_values(),
            vec![SqlValue::Text("α".into()), SqlValue::Blob(vec![0x80])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_readers_and_single_writer() -> SqlResult<()> {
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        db.execute("INSERT INTO items (name) VALUES ('seed')", &[])
            .await?;
        let mut handles = Vec::new();
        for _ in 0..8 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                db.query("SELECT name FROM items", &[])
                    .await
                    .map(|s| s.rows.len())
            }));
        }
        for h in handles {
            assert_eq!(h.await.expect("join")?, 1);
        }
        // Writers serialise through the single writer lease.
        let mut handles = Vec::new();
        for i in 0..4 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                db.execute(
                    "INSERT INTO items (name) VALUES (:name)",
                    &[SqlValue::Text(format!("w{i}"))],
                )
                .await
            }));
        }
        for h in handles {
            h.await.expect("join")?;
        }
        let count =
            db.query("SELECT COUNT(*) AS c FROM items", &[]).await?.rows[0].get::<i64>("c")?;
        assert_eq!(count, 5);
        Ok(())
    }

    #[tokio::test]
    async fn idle_eviction_closes_and_reopens() -> SqlResult<()> {
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        db.execute("INSERT INTO items (name) VALUES ('keep')", &[])
            .await?;
        assert!(rt.open_connection_count() >= 1);
        tokio::time::sleep(Duration::from_millis(80)).await;
        db.evict_idle().await;
        assert_eq!(rt.open_connection_count(), 0);
        // Reopen is transparent and data survives (file-backed).
        let set = db.query("SELECT name FROM items", &[]).await?;
        assert_eq!(set.rows.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn many_databases_share_workers() -> SqlResult<()> {
        let rt = runtime();
        let mut dbs = Vec::new();
        for _ in 0..8 {
            dbs.push(rt.ephemeral(MIGRATIONS).expect("ephemeral"));
        }
        // Sequential use: only one database has connections at a time,
        // and all share the runtime's 2 workers.
        for (i, db) in dbs.iter().enumerate() {
            db.execute(
                "INSERT INTO items (name) VALUES (:name)",
                &[SqlValue::Text(i.to_string())],
            )
            .await?;
        }
        assert!(rt.open_connection_count() <= 16);
        Ok(())
    }

    #[tokio::test]
    async fn execute_supports_returning() -> SqlResult<()> {
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        let inserted = db
            .execute(
                "INSERT INTO items (name) VALUES ('returned') RETURNING id, name",
                &[],
            )
            .await?;
        let expected = db.query("SELECT id, name FROM items", &[]).await?;
        assert_eq!(
            inserted,
            ExecuteResult {
                rows_affected: 1,
                last_insert_rowid: 1,
                result_set: expected.clone(),
            }
        );
        let selected = db
            .execute("/* read */ SELECT id, name FROM items", &[])
            .await?;
        assert_eq!(
            selected,
            ExecuteResult {
                rows_affected: 0,
                last_insert_rowid: 1,
                result_set: expected,
            }
        );
        let tx = db.begin_write().await?;
        let updated = tx.execute(
            "WITH selected AS (SELECT id FROM items) UPDATE items SET name = 'updated' WHERE id IN selected RETURNING id, name",
            &[],
        ).await?;
        assert_eq!(updated.rows_affected, 1);
        assert_eq!(
            updated.result_set,
            tx.query("SELECT id, name FROM items", &[]).await?
        );
        tx.rollback().await?;
        assert_eq!(
            db.query("SELECT name FROM items", &[]).await?.rows[0].get::<String>("name")?,
            "returned"
        );
        Ok(())
    }

    #[tokio::test]
    async fn execute_batch_counts_all_statements_without_stale_changes() -> SqlResult<()> {
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        let batch = db.execute_batch(
            "INSERT INTO items (name) VALUES ('first'); INSERT INTO items (name) VALUES ('second'); UPDATE items SET score = 1.5; SELECT 1; CREATE TABLE batch_marker (id INTEGER);"
        ).await?;
        assert_eq!(
            batch,
            ExecuteResult {
                rows_affected: 4,
                last_insert_rowid: 2,
                result_set: ResultSet::default(),
            }
        );
        assert_eq!(db.execute_batch("SELECT 1;").await?.rows_affected, 0);
        let mut tx = db.begin_write().await?;
        let savepoint = tx.savepoint("batch").await?;
        let batch = savepoint.execute_batch(
            "UPDATE items SET score = 2; INSERT INTO items (name) VALUES ('discarded'); SELECT 1;"
        ).await?;
        assert_eq!(batch.rows_affected, 3);
        savepoint.rollback().await?;
        let empty = tx.execute_batch("CREATE TABLE marker (id INTEGER)").await?;
        assert_eq!(empty.rows_affected, 0);
        let deleted = tx
            .execute_batch("DELETE FROM items WHERE id = 1; DELETE FROM items WHERE id = 2;")
            .await?;
        assert_eq!(deleted.rows_affected, 2);
        let empty = tx
            .execute("CREATE TABLE other_marker (id INTEGER)", &[])
            .await?;
        assert_eq!(empty.rows_affected, 0);
        tx.commit().await?;
        assert!(db.query("SELECT id FROM items", &[]).await?.rows.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn generated_queries_roundtrip() -> SqlResult<()> {
        // The test module is declared at crate root (cfg(test)) because
        // #[path] inside a fn resolves against the wrong directory.
        use crate::gen_tests::queries::*;
        let rt = runtime();
        let db = rt.ephemeral(MIGRATIONS).expect("ephemeral");
        insert_item(&db, "generated".to_string(), Some(vec![9, 9]), Some(2.5)).await?;
        let item = get_item(&db, 1).await?;
        assert_eq!(
            item,
            Some(GetItemRow {
                id: 1,
                name: "generated".to_string(),
                payload: Some(vec![9, 9]),
                score: Some(2.5)
            })
        );
        assert_eq!(get_item(&db, 99).await?, None);
        let names = list_names(&db).await?;
        assert_eq!(names, vec!["generated".to_string()]);
        let transaction = db.begin_write().await?;
        insert_item(&transaction, "transaction".to_string(), None, None).await?;
        assert_eq!(
            get_item(&transaction, 2).await?,
            Some(GetItemRow {
                id: 2,
                name: "transaction".to_string(),
                payload: None,
                score: None
            })
        );
        assert_eq!(get_item(&transaction, 99).await?, None);
        assert_eq!(
            list_names(&transaction).await?,
            vec!["generated".to_string(), "transaction".to_string()]
        );
        transaction.rollback().await?;
        assert_eq!(list_names(&db).await?, names);
        Ok(())
    }

    #[tokio::test]
    async fn migration_checksum_change_is_an_error() {
        let rt = runtime();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("db.sqlite");
        let db = rt.database(path.clone(), MIGRATIONS, "_sqlite_rs_migrations");
        db.execute_batch("CREATE TABLE marker (v INTEGER)")
            .await
            .expect("initial migration should succeed");
        // Re-register with a tampered migration body.
        let rt2 = runtime();
        let tampered: &[Migration] = &[Migration {
            version: 1,
            name: "init",
            sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, other TEXT);",
        }];
        let db2 = rt2.database(path, tampered, "_sqlite_rs_migrations");
        let error = db2
            .query("SELECT 1", &[])
            .await
            .expect_err("migration checksum changed");
        assert!(
            matches!(error, SqlError::Unexpected(ref message) if message == "migration 1 (init) checksum changed after being applied"),
            "{error}"
        );
    }
}
