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

#[test]
fn a_connection_that_loses_the_race_to_enter_wal_waits_for_the_winner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("race.sqlite");
    // A write transaction on the still-rollback-mode file is what the
    // winner of a conversion race holds: the opener's switch reads the
    // header under it and is then refused the upgrade without a wait.
    let winner = RusConnection::open(&path).expect("open winner");
    winner
        .execute_batch("BEGIN IMMEDIATE; CREATE TABLE converted (id INTEGER);")
        .expect("winner write transaction");
    let opener = std::thread::spawn({
        let path = path.clone();
        move || ConnSlot::open(&path, &RuntimeConfig::default(), Arc::default(), true).map(drop)
    });
    std::thread::sleep(Duration::from_millis(100));
    winner.execute_batch("COMMIT").expect("winner commit");
    opener
        .join()
        .expect("opener panicked")
        .expect("the loser waits for the winner instead of failing busy");
    drop(winner);
    let mode: String = RusConnection::open(&path)
        .expect("reopen")
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode");
    assert_eq!(mode, "wal");
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
    let count = db.query("SELECT COUNT(*) AS c FROM items", &[]).await?.rows[0].get::<i64>("c")?;
    assert_eq!(count, 1);

    let tx = db.begin_write().await?;
    tx.execute("INSERT INTO items (name) VALUES ('c')", &[])
        .await?;
    tx.rollback().await?;
    let count = db.query("SELECT COUNT(*) AS c FROM items", &[]).await?.rows[0].get::<i64>("c")?;
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
    let count = db.query("SELECT COUNT(*) AS c FROM items", &[]).await?.rows[0].get::<i64>("c")?;
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
    let batch = savepoint
        .execute_batch(
            "UPDATE items SET score = 2; INSERT INTO items (name) VALUES ('discarded'); SELECT 1;",
        )
        .await?;
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
