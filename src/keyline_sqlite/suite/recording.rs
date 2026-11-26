//! A database wrapper that records the statements a store runs and how many rows they read.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use sqlite_rs::Database;
use sqlite_rs::ExecuteResult;
use sqlite_rs::Executor;
use sqlite_rs::IntoParams;
use sqlite_rs::QueryExecutor;
use sqlite_rs::ReadTx;
use sqlite_rs::ResultSet;
use sqlite_rs::SqlResult;
use sqlite_rs::SqlValue;
use sqlite_rs::SqliteDb;
use sqlite_rs::Tx;

pub(super) type Log = Arc<Mutex<BTreeMap<String, Vec<SqlValue>>>>;

/// What a [`Recording`] saw: each distinct statement text with one set of its parameters, and
/// how many statements ran and rows came back, in all.
#[derive(Default)]
pub(super) struct Counts {
    pub statements: AtomicUsize,
    pub rows: AtomicUsize,
    /// Write transactions begun: a read-only handle must leave this at zero.
    pub write_transactions: AtomicUsize,
}

impl Counts {
    pub fn take(&self) -> (usize, usize) {
        (
            self.statements.swap(0, Ordering::SeqCst),
            self.rows.swap(0, Ordering::SeqCst),
        )
    }
}

/// A database wrapper that records every statement run through its transactions.
#[derive(Clone)]
pub(super) struct Recording {
    pub db: Database,
    pub log: Log,
    pub counts: Arc<Counts>,
}

impl Recording {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            log: Log::default(),
            counts: Arc::default(),
        }
    }
}

pub(super) struct RecordingWrite {
    tx: Tx,
    log: Log,
    counts: Arc<Counts>,
}

pub(super) struct RecordingRead {
    tx: ReadTx,
    log: Log,
    counts: Arc<Counts>,
}

fn record(log: &Log, counts: &Counts, sql: &str, params: &[SqlValue]) {
    counts.statements.fetch_add(1, Ordering::SeqCst);
    log.lock()
        .expect("the log mutex is not poisoned")
        .entry(sql.to_owned())
        .or_insert_with(|| params.to_vec());
}

impl QueryExecutor for RecordingWrite {
    async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        let params = params.into_params();
        record(&self.log, &self.counts, sql, &params);
        let set = self.tx.query(sql, params).await?;
        self.counts.rows.fetch_add(set.rows.len(), Ordering::SeqCst);
        Ok(set)
    }
}

impl Executor for RecordingWrite {
    async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        let params = params.into_params();
        record(&self.log, &self.counts, sql, &params);
        self.tx.execute(sql, params).await
    }
}

impl QueryExecutor for RecordingRead {
    async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        let params = params.into_params();
        record(&self.log, &self.counts, sql, &params);
        let set = self.tx.query(sql, params).await?;
        self.counts.rows.fetch_add(set.rows.len(), Ordering::SeqCst);
        Ok(set)
    }
}

impl SqliteDb for Recording {
    type Write = RecordingWrite;
    type Read = RecordingRead;

    fn supports_concurrent_readers(&self) -> bool {
        self.db.supports_concurrent_readers()
    }

    async fn begin_write(&self) -> SqlResult<RecordingWrite> {
        self.counts
            .write_transactions
            .fetch_add(1, Ordering::SeqCst);
        Ok(RecordingWrite {
            tx: self.db.begin_write().await?,
            log: Arc::clone(&self.log),
            counts: Arc::clone(&self.counts),
        })
    }

    async fn begin_read(&self) -> SqlResult<RecordingRead> {
        Ok(RecordingRead {
            tx: self.db.begin_read().await?,
            log: Arc::clone(&self.log),
            counts: Arc::clone(&self.counts),
        })
    }

    async fn commit(tx: RecordingWrite) -> SqlResult<()> {
        tx.tx.commit().await
    }

    async fn rollback(tx: RecordingWrite) -> SqlResult<()> {
        tx.tx.rollback().await
    }

    async fn end_read(tx: RecordingRead) -> SqlResult<()> {
        tx.tx.rollback().await
    }
}
