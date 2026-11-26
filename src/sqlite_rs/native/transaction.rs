use super::exec::SerialQueue;
use super::*;
use std::sync::atomic::AtomicBool;

/// An owned write transaction. Dropping an unfinished transaction
/// schedules a rollback on the shared workers; the connection is not
/// reused until that rollback has run (and is discarded if it cannot be
/// queued).
pub struct Tx {
    pub(crate) db: Database,
    pub(crate) lease: Option<Lease>,
    pub(super) queue: Option<Arc<SerialQueue>>,
    pub(crate) next_savepoint: u64,
}

pub(crate) struct PendingRollback {
    pub(crate) lease: Option<Lease>,
}

impl PendingRollback {
    fn commit(mut self) -> SqlResult<()> {
        let connection = self
            .lease
            .as_ref()
            .expect("pending transaction lease")
            .slot();
        let result = control_batch(&connection.lock().expect(ERROR_MUTEX), "COMMIT", false);
        drop(connection);
        if let Err(commit_error) = result {
            self.rollback().map_err(|rollback_error| {
                SqlError::Unexpected(format!(
                    "commit failed: {commit_error}; rollback failed: {rollback_error}"
                ))
            })?;
            return Err(commit_error);
        }
        self.lease.take();
        Ok(())
    }

    fn rollback(mut self) -> SqlResult<()> {
        let connection = self
            .lease
            .as_ref()
            .expect("pending transaction lease")
            .slot();
        let guard = connection.lock().expect(ERROR_MUTEX);
        if !guard.is_autocommit() {
            control_batch(
                &guard,
                "ROLLBACK",
                !self.lease.as_ref().expect("pending lease").write,
            )?;
        }
        drop(guard);
        drop(connection);
        self.lease.take();
        Ok(())
    }
}

impl Drop for PendingRollback {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.as_mut() {
            lease.slot.take();
        }
    }
}

impl Tx {
    pub(crate) async fn run<T, F>(&self, operation: F) -> SqlResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RusConnection) -> SqlResult<T> + Send + 'static,
    {
        let conn = self.lease.as_ref().expect("tx lease").slot();
        self.db
            .runtime
            .exec
            .call_on(self.queue.as_ref(), move || {
                let mut conn = conn.lock().expect(ERROR_MUTEX);
                operation(&mut conn)
            })
            .await
    }

    /// One statement of this transaction, run in submission order with the others.
    async fn run_statement<T: Send + 'static>(
        &self,
        sql: &str,
        params: Vec<SqlValue>,
        statement: StatementFn<T>,
    ) -> SqlResult<T> {
        let connection = self.lease.as_ref().expect("tx lease").connection();
        self.db
            .runtime
            .exec
            .statement(self.queue.as_ref(), connection, sql, params, statement)
            .await
    }

    pub async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        self.run_statement(sql, params.into_params(), run_query)
            .await
    }

    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        self.run_statement(sql, params.into_params(), run_execute)
            .await
    }

    pub async fn execute_batch(&self, sql: &str) -> SqlResult<ExecuteResult> {
        self.run_statement(sql, Vec::new(), run_execute_batch).await
    }

    pub async fn commit(mut self) -> SqlResult<()> {
        let pending = PendingRollback {
            lease: self.lease.take(),
        };
        self.db
            .runtime
            .exec
            .call_on(self.queue.as_ref(), move || pending.commit())
            .await
    }

    pub async fn rollback(mut self) -> SqlResult<()> {
        let pending = PendingRollback {
            lease: self.lease.take(),
        };
        self.db
            .runtime
            .exec
            .call_on(self.queue.as_ref(), move || pending.rollback())
            .await
    }
}

impl Drop for Tx {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let pending = PendingRollback { lease: Some(lease) };
            self.db
                .runtime
                .exec
                .defer(
                    self.queue.as_ref(),
                    Box::new(move || {
                        pending
                            .rollback()
                            .expect("background sqlite rollback failed");
                    }),
                )
                .expect(ERROR_CHANNEL);
        }
    }
}

pub struct ReadTx(pub(crate) Tx);

impl ReadTx {
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        self.0
            .run_statement(sql, params.into_params(), run_read_query)
            .await
    }

    pub async fn rollback(self) -> SqlResult<()> {
        self.0.rollback().await
    }
}

pub struct Savepoint<'parent> {
    parent: &'parent mut Tx,
    name: String,
    active: Arc<AtomicBool>,
}

impl Tx {
    pub async fn savepoint(&mut self, name: &str) -> SqlResult<Savepoint<'_>> {
        assert!(
            crate::query::valid_identifier(name),
            "invalid savepoint name"
        );
        self.next_savepoint = self
            .next_savepoint
            .checked_add(1)
            .expect("savepoint counter overflow");
        let name = format!("{name}_{}", self.next_savepoint);
        let guard = Savepoint {
            parent: self,
            name: name.clone(),
            active: Arc::new(AtomicBool::new(false)),
        };
        let active = Arc::clone(&guard.active);
        guard
            .parent
            .run(move |connection| {
                control_batch(connection, &format!("SAVEPOINT \"{name}\""), false)?;
                active.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await?;
        Ok(guard)
    }
}

impl Savepoint<'_> {
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        self.parent.query(sql, params).await
    }

    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        self.parent.execute(sql, params).await
    }

    pub async fn execute_batch(&self, sql: &str) -> SqlResult<ExecuteResult> {
        self.parent.execute_batch(sql).await
    }

    pub async fn savepoint(&mut self, name: &str) -> SqlResult<Savepoint<'_>> {
        self.parent.savepoint(name).await
    }

    pub async fn release(self) -> SqlResult<()> {
        let name = self.name.clone();
        let active = Arc::clone(&self.active);
        self.parent
            .run(move |connection| {
                control_batch(connection, &format!("RELEASE \"{name}\""), false)?;
                active.store(false, Ordering::SeqCst);
                Ok(())
            })
            .await
    }

    pub async fn rollback(self) -> SqlResult<()> {
        let name = self.name.clone();
        let active = Arc::clone(&self.active);
        self.parent
            .run(move |connection| {
                control_batch(
                    connection,
                    &format!("ROLLBACK TO \"{name}\"; RELEASE \"{name}\""),
                    false,
                )?;
                active.store(false, Ordering::SeqCst);
                Ok(())
            })
            .await
    }
}

impl Drop for Savepoint<'_> {
    fn drop(&mut self) {
        let connection = self
            .parent
            .lease
            .as_ref()
            .expect("transaction lease")
            .slot();
        let name = self.name.clone();
        let active = Arc::clone(&self.active);
        self.parent
            .db
            .runtime
            .exec
            .defer(
                self.parent.queue.as_ref(),
                Box::new(move || {
                    if active.swap(false, Ordering::SeqCst) {
                        let connection = connection.lock().expect(ERROR_MUTEX);
                        if !connection.is_autocommit() {
                            control_batch(
                                &connection,
                                &format!("ROLLBACK TO \"{name}\"; RELEASE \"{name}\""),
                                false,
                            )
                            .expect("background savepoint rollback failed");
                        }
                    }
                }),
            )
            .expect("sqlite runtime closed while rolling back a savepoint");
    }
}

macro_rules! query_executor {
    ($($ty:ty),+) => {$(
        impl QueryExecutor for $ty {
            async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
                self.query(sql, params).await
            }
        }
    )+};
}
query_executor!(Database, Tx, ReadTx, Savepoint<'_>);

macro_rules! executor {
    ($($ty:ty),+) => {$(
        impl Executor for $ty {
            async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
                self.execute(sql, params).await
            }
        }
    )+};
}
executor!(Database, Tx, Savepoint<'_>);
