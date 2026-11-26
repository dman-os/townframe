//! What only the pool does: the queue hop, a statement that outlives its cancelled caller, and
//! panics resumed across the hop.

use super::*;

fn runtime() -> SqliteRuntime {
    runtime_on(Execution::Pool)
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
async fn cancelled_savepoint_operations_are_ordered_before_parent_work() -> SqlResult<()> {
    let db = test_support::db_on(Execution::Pool, MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    let mut tx = db.begin_write().await?;
    let blockers = super::block_workers(runtime).await;
    let mut pending = Box::pin(tx.savepoint("cancelled_begin"));
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(blockers);
    let guard = tx.savepoint("cancelled_statement").await?;
    let blockers = super::block_workers(runtime).await;
    let mut pending = Box::pin(guard.execute("INSERT INTO items (name) VALUES ('discard')", &[]));
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(guard);
    let mut survivor = Box::pin(tx.execute("INSERT INTO items (name) VALUES ('survivor')", &[]));
    assert_pending(survivor.as_mut()).await;
    drop(blockers);
    survivor.await?;
    let guard = tx.savepoint("cancelled_release").await?;
    guard
        .execute("INSERT INTO items (name) VALUES ('released')", &[])
        .await?;
    let blockers = super::block_workers(runtime).await;
    let mut pending = Box::pin(guard.release());
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(blockers);
    tx.commit().await?;
    assert_eq!(
        db.query("SELECT name FROM items ORDER BY id", &[])
            .await?
            .all::<String>()?,
        vec!["survivor", "released"]
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_begin_rolls_back_before_writer_reuse() -> SqlResult<()> {
    let db = test_support::db_on(Execution::Pool, MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    db.migrate().await?;
    let blockers = super::block_workers(runtime).await;
    let mut pending = Box::pin(db.begin_write());
    assert_pending(pending.as_mut()).await;
    drop(pending);
    assert_eq!(db.inner.writer_gate.available_permits(), 0);
    drop(blockers);
    let tx = db.begin_write().await?;
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_open_retains_ephemeral_directory_until_worker_finishes() -> SqlResult<()> {
    let runtime = SqliteRuntime::new(RuntimeConfig {
        workers: 1,
        ..Default::default()
    });
    let database = runtime.ephemeral(&[]).expect("test db");
    let directory = database
        .inner
        .path
        .parent()
        .expect("database parent")
        .to_owned();
    let blockers = super::block_workers(&runtime).await;
    let mut pending = Box::pin(database.query("SELECT 1", ()));
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(database);
    assert!(directory.exists());
    drop(blockers);
    runtime.inner.call(|| Ok(())).await?;
    assert!(!directory.exists());
    assert_eq!(runtime.open_connection_count(), 0);
    Ok(())
}

#[tokio::test]
async fn cancelled_statement_closes_connection_before_removing_directory() -> SqlResult<()> {
    struct CloseProbe {
        directory: PathBuf,
        reply: Option<tokio::sync::oneshot::Sender<bool>>,
    }
    impl Drop for CloseProbe {
        fn drop(&mut self) {
            self.reply
                .take()
                .expect("close probe sender")
                .send(self.directory.exists())
                .expect("close probe receiver");
        }
    }
    let runtime = SqliteRuntime::new(RuntimeConfig {
        workers: 1,
        ..Default::default()
    });
    let database = runtime.ephemeral(MIGRATIONS).expect("test db");
    let directory = database
        .inner
        .path
        .parent()
        .expect("database parent")
        .to_owned();
    let (reply, closed) = tokio::sync::oneshot::channel();
    let probe = CloseProbe {
        directory: directory.clone(),
        reply: Some(reply),
    };
    database
        .run(true, move |connection| {
            connection.authorizer(Some(move |_: rusqlite::hooks::AuthContext<'_>| {
                let _: &CloseProbe = &probe;
                rusqlite::hooks::Authorization::Allow
            }));
            Ok(())
        })
        .await?;
    let blockers = super::block_workers(&runtime).await;
    let mut pending = Box::pin(database.execute("INSERT INTO items(name) VALUES ('queued')", ()));
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(database);
    assert!(directory.exists());
    drop(blockers);
    runtime.inner.call(|| Ok(())).await?;
    assert!(closed.await.expect("connection closed"));
    assert!(!directory.exists());
    Ok(())
}

#[tokio::test]
async fn drain_workers_waits_for_cancelled_jobs_on_every_worker() -> SqlResult<()> {
    let runtime = SqliteRuntime::new(RuntimeConfig {
        workers: 3,
        ..Default::default()
    });
    let database = runtime.ephemeral(&[]).expect("test db");
    let directory = database
        .inner
        .path
        .parent()
        .expect("database parent")
        .to_owned();
    let blockers = super::block_workers(&runtime).await;
    let mut pending = Box::pin(database.query("SELECT 1", ()));
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(database);
    assert_eq!(runtime.live_database_count(), 1);
    drop(blockers);
    runtime.drain_workers().await;
    assert_eq!(runtime.live_database_count(), 0);
    assert!(!directory.exists());
    Ok(())
}

#[tokio::test]
async fn a_dropped_snapshot_keeps_its_reader_until_a_worker_rolls_it_back() -> SqlResult<()> {
    let db = test_support::db_on(Execution::Pool, MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    let snapshot = db.begin_read().await?;
    let blockers = block_workers(runtime).await;
    drop(snapshot);
    assert_eq!(
        db.inner.reader_gate.available_permits(),
        runtime.inner.cfg.max_readers_per_db - 1
    );
    drop(blockers);
    let snapshot = db.begin_read().await?;
    snapshot.rollback().await?;
    Ok(())
}
