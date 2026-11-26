//! Where a statement runs: on the calling task, or on a pool of worker threads.
//!
//! Everything above this module (leases, transactions, savepoints, migrations) is written
//! against one question: "run this closure against a connection and give me its result". How
//! that question is answered is the `Execution` of the runtime.
//!
//! - `Pool` hands each closure to one of a fixed set of OS threads through a queue, and the
//!   caller awaits a reply. Use it where the host is multi-threaded and SQLite must not stall
//!   an executor thread. Every statement costs a queue hop and a reply, including the
//!   statements of a transaction. A transaction keeps a [`SerialQueue`] so its closures run in
//!   submission order on whichever worker is free.
//! - `Inline` calls SQLite on the thread that polls the future. There are no worker threads,
//!   no queue and no reply: the `async` functions are an interface only, and a statement's
//!   future is ready the first time it is polled. Use it on a single-threaded host (a
//!   current-thread runtime, a browser, a test) where a hop would cost and buy nothing. A
//!   statement blocks its thread for as long as SQLite takes, so on a multi-threaded runtime
//!   it blocks one executor thread.
//!
//! The one real await point of `Inline` is the lease: a task that needs the writer while
//! another holds it waits on the gate (see `gate.rs`) and the thread runs other tasks. A task
//! that holds a write transaction while it awaits something unrelated, such as a network
//! reply, keeps the writer from every other task for that time. On `Pool` this is true as
//! well; on `Inline` it is the only thing that can go wrong, because nothing else yields.
//!
//! Cancellation also differs. A dropped statement future on `Pool` may already have been
//! queued and still runs to completion on a worker. On `Inline` a statement either has not
//! started or has finished: it cannot be cancelled half-way.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::Ordering;

use rusqlite::Connection as RusConnection;
use tokio::sync::oneshot;
use utils_rs::expect_tags::ERROR_MUTEX;

use super::INSTR_SQL;
use crate::SqlError;
use crate::SqlResult;
use crate::SqlValue;

pub(super) type Job = Box<dyn FnOnce() + Send>;

/// A statement body: runs `sql` with `params` on a connection. The hot paths pass one of
/// these, rather than a closure, so that `Inline` can lend the caller's `&str` and `&[SqlValue]`
/// to it while `Pool` copies them for the trip to a worker.
pub(super) type StatementFn<T> = fn(&mut RusConnection, &str, &[SqlValue]) -> SqlResult<T>;

/// How a runtime executes statements. Chosen once, in the runtime's config.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Execution {
    /// Statements run on `RuntimeConfig::workers` OS threads. For multi-threaded hosts.
    #[default]
    Pool,
    /// Statements run on the calling task, with no thread hop. For single-threaded hosts and
    /// tests. `RuntimeConfig::workers` is ignored and idle connections are evicted when a
    /// lease is taken, since there is no thread to sweep in the background.
    Inline,
}

pub(super) enum Exec {
    Pool(std::sync::mpsc::Sender<Job>),
    Inline,
}

impl Exec {
    pub(super) fn is_inline(&self) -> bool {
        matches!(self, Self::Inline)
    }

    /// Run `operation`, in submission order among the jobs of `queue` when there is one.
    pub(super) async fn call_on<T, F>(
        &self,
        queue: Option<&Arc<SerialQueue>>,
        operation: F,
    ) -> SqlResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> SqlResult<T> + Send + 'static,
    {
        let Self::Pool(sender) = self else {
            return operation();
        };
        let (reply_tx, reply_rx) = oneshot::channel::<std::thread::Result<SqlResult<T>>>();
        let job: Job = Box::new(move || {
            let instr_start = std::time::Instant::now(); // INSTR
            let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
            INSTR_SQL[3].fetch_add(1, Ordering::Relaxed); // INSTR
            INSTR_SQL[4].fetch_add(instr_start.elapsed().as_nanos() as u64, Ordering::Relaxed); // INSTR
            if let Err(Err(payload)) = reply_tx.send(out) {
                std::panic::resume_unwind(payload);
            }
        });
        match queue {
            Some(queue) => queue.enqueue(sender, job)?,
            None => sender.send(job).map_err(|_| SqlError::Closed)?,
        }
        match reply_rx.await.map_err(|_| SqlError::Closed)? {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Run one statement of `connection`. `Inline` lends `sql` and `params` to `statement`;
    /// `Pool` copies `sql` and moves `params` into the job.
    pub(super) async fn statement<T: Send + 'static>(
        &self,
        queue: Option<&Arc<SerialQueue>>,
        connection: &Arc<StdMutex<RusConnection>>,
        sql: &str,
        params: Vec<SqlValue>,
        statement: StatementFn<T>,
    ) -> SqlResult<T> {
        if self.is_inline() {
            let mut connection = connection.lock().expect(ERROR_MUTEX);
            return statement(&mut connection, sql, &params);
        }
        let connection = Arc::clone(connection);
        let sql = sql.to_owned();
        self.call_on(queue, move || {
            let mut connection = connection.lock().expect(ERROR_MUTEX);
            statement(&mut connection, &sql, &params)
        })
        .await
    }

    /// Run `job` without waiting for it: queued behind `queue` on `Pool`, at once on `Inline`.
    /// This is how a dropped transaction or savepoint rolls back.
    pub(super) fn defer(&self, queue: Option<&Arc<SerialQueue>>, job: Job) -> SqlResult<()> {
        match (self, queue) {
            (Self::Inline, _) => {
                job();
                Ok(())
            }
            (Self::Pool(sender), Some(queue)) => queue.enqueue(sender, job),
            (Self::Pool(sender), None) => sender.send(job).map_err(|_| SqlError::Closed),
        }
    }

    /// Resolves once every job queued before this call has finished. Nothing is ever queued on
    /// `Inline`.
    pub(super) async fn drain(&self, workers: usize) {
        let Self::Pool(sender) = self else {
            return;
        };
        let workers = workers.max(1);
        let rendezvous = Arc::new(std::sync::Barrier::new(workers));
        let mut done = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (reply, replied) = oneshot::channel();
            let rendezvous = Arc::clone(&rendezvous);
            sender
                .send(Box::new(move || {
                    rendezvous.wait();
                    reply.send(()).expect("drain_workers receiver dropped");
                }))
                .expect("sqlite worker pool closed while its runtime is alive");
            done.push(replied);
        }
        for replied in done {
            replied.await.expect("sqlite worker dropped a drain job");
        }
    }

    #[cfg(test)]
    pub(super) fn try_call<F>(&self, operation: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            Self::Pool(sender) => sender.send(Box::new(operation)).is_ok(),
            Self::Inline => {
                operation();
                true
            }
        }
    }
}

/// Keeps the closures of one transaction in submission order on a pool.
///
/// Workers are interchangeable, so two closures queued back to back could run at the same time
/// on two workers. The queue holds the transaction's closures and has at most one drain job in
/// the pool; that job runs them one after another. The connection is leased to the transaction,
/// never a thread.
#[derive(Default)]
pub(super) struct SerialQueue {
    state: StdMutex<(bool, VecDeque<Job>)>,
}

impl SerialQueue {
    pub(super) fn enqueue(
        self: &Arc<Self>,
        sender: &std::sync::mpsc::Sender<Job>,
        job: Job,
    ) -> SqlResult<()> {
        let mut state = self.state.lock().expect(ERROR_MUTEX);
        state.1.push_back(job);
        if !state.0 {
            state.0 = true;
            let queue = Arc::clone(self);
            if sender
                .send(Box::new(move || {
                    loop {
                        let job = {
                            let mut state = queue.state.lock().expect(ERROR_MUTEX);
                            match state.1.pop_front() {
                                Some(job) => job,
                                None => {
                                    state.0 = false;
                                    return;
                                }
                            }
                        };
                        job();
                    }
                }))
                .is_err()
            {
                state.0 = false;
                state.1.clear();
                return Err(SqlError::Closed);
            }
        }
        Ok(())
    }
}
