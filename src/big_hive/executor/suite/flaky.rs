//! A database handle that breaks the n-th write statement of its transactions, whichever store
//! issues it, to see what a plan leaves behind. A write statement is one that starts with
//! `INSERT`, `UPDATE`, `DELETE` or `REPLACE`, whether it runs as an execute or as a query
//! (`INSERT .. RETURNING`).

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
use sqlite_rs::SqlError;
use sqlite_rs::SqlResult;
use sqlite_rs::SqliteDb;
use sqlite_rs::Tx;
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct Control {
    /// The write statements seen since the last [`arm`](Control::arm).
    writes: AtomicUsize,
    /// What the armed statement does.
    armed: Mutex<Option<(usize, Break)>>,
    /// Signalled when a [`Break::Hang`] statement is reached.
    pub reached: Notify,
}

#[derive(Clone, Copy)]
pub(crate) enum Break {
    /// The statement returns an error.
    Fail,
    /// The statement never returns: the task is stuck there, as a crash leaves it.
    Hang,
}

impl Control {
    /// Break the write statement numbered `at` (from 0) counted from now.
    pub(crate) fn arm(&self, at: usize, how: Break) {
        self.writes.store(0, Ordering::SeqCst);
        *self.armed.lock().unwrap() = Some((at, how));
    }

    /// Break nothing; count writes from now.
    pub(crate) fn disarm(&self) {
        self.writes.store(0, Ordering::SeqCst);
        *self.armed.lock().unwrap() = None;
    }

    /// How many write statements ran since the last arm or disarm.
    pub(crate) fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    async fn gate(&self, sql: &str) -> SqlResult<()> {
        let head = sql.trim_start().to_ascii_uppercase();
        if !["INSERT", "UPDATE", "DELETE", "REPLACE"]
            .iter()
            .any(|verb| head.starts_with(verb))
        {
            return Ok(());
        }
        let seen = self.writes.fetch_add(1, Ordering::SeqCst);
        let armed = *self.armed.lock().unwrap();
        match armed {
            Some((at, Break::Fail)) if at == seen => Err(SqlError::Unexpected(format!(
                "injected fault at write {seen}"
            ))),
            Some((at, Break::Hang)) if at == seen => {
                self.reached.notify_one();
                std::future::pending().await
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Flaky {
    pub inner: Database,
    pub control: Arc<Control>,
}

impl Flaky {
    pub(crate) fn new(inner: Database) -> Self {
        Self {
            inner,
            control: Arc::default(),
        }
    }
}

pub(crate) struct FlakyTx {
    tx: Tx,
    control: Arc<Control>,
}

impl QueryExecutor for FlakyTx {
    async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        self.control.gate(sql).await?;
        self.tx.query(sql, params).await
    }
}

impl Executor for FlakyTx {
    async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        self.control.gate(sql).await?;
        self.tx.execute(sql, params).await
    }
}

impl SqliteDb for Flaky {
    type Write = FlakyTx;
    type Read = ReadTx;

    fn supports_concurrent_readers(&self) -> bool {
        self.inner.supports_concurrent_readers()
    }

    async fn begin_write(&self) -> SqlResult<FlakyTx> {
        Ok(FlakyTx {
            tx: self.inner.begin_write().await?,
            control: Arc::clone(&self.control),
        })
    }

    async fn begin_read(&self) -> SqlResult<ReadTx> {
        self.inner.begin_read().await
    }

    async fn commit(tx: FlakyTx) -> SqlResult<()> {
        tx.tx.commit().await
    }

    async fn rollback(tx: FlakyTx) -> SqlResult<()> {
        tx.tx.rollback().await
    }

    async fn end_read(tx: ReadTx) -> SqlResult<()> {
        tx.rollback().await
    }
}
