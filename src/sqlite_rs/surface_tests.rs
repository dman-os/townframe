use super::*;
use crate::gen_tests::queries::MIGRATIONS;

async fn assert_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    assert!(
        std::future::poll_fn(|context| std::task::Poll::Ready(
            future.as_mut().poll(context).is_pending()
        ))
        .await
    );
}

#[tokio::test]
async fn builder_binds_values_lists_tuples_and_identifiers() -> SqlResult<()> {
    let db = test_support::db(MIGRATIONS).expect("test db");
    let hostile = "quote'); DROP TABLE items; --";
    Sql::new("INSERT INTO items (name) VALUES (")
        .bind(hostile)
        .push(")")
        .execute(&db)
        .await?;
    let mut sql = Sql::new("SELECT id, name FROM items WHERE (id, name) IN ");
    sql.bind_tuples([(1_i64, hostile), (2_i64, "missing")]);
    assert_eq!(
        sql.query(&db).await?.all::<(i64, String)>()?,
        vec![(1, hostile.into())]
    );
    let mut sql = Sql::new("SELECT name FROM items WHERE id IN ");
    sql.bind_list([1_i64, 2]);
    assert_eq!(sql.query(&db).await?.all::<String>()?, vec![hostile]);
    for condition in ["IN", "NOT IN"] {
        let mut sql = Sql::new(format!("SELECT name FROM items WHERE id {condition} "));
        sql.bind_list(Vec::<i64>::new());
        assert_eq!(
            sql.query(&db).await?.rows.len(),
            usize::from(condition == "NOT IN")
        );
    }
    let mut empty = Sql::new("SELECT name FROM items WHERE (id, name) IN ");
    empty.bind_tuples(Vec::<(i64, &str)>::new());
    assert!(empty.query(&db).await?.rows.is_empty());
    Sql::new("CREATE TABLE ")
        .ident("tenant-kv")
        .push(" (key TEXT)")
        .execute(&db)
        .await?;
    let mut sql = Sql::new("SELECT key FROM ");
    sql.ident("tenant-kv");
    assert!(sql.query(&db).await?.all::<String>()?.is_empty());
    assert!(
        std::panic::catch_unwind(|| Sql::new("").ident("x\"; DROP TABLE items;").clone()).is_err()
    );
    Ok(())
}

#[tokio::test]
async fn cardinality_decode_and_custom_rows() -> SqlResult<()> {
    #[derive(Debug, PartialEq)]
    struct Item {
        value: String,
    }
    impl FromRow for Item {
        fn from_row(row: &Row) -> SqlResult<Self> {
            Ok(Self {
                value: row.get("name")?,
            })
        }
    }
    let db = test_support::db(&[]).expect("test db");
    assert_eq!(
        db.query("SELECT 'named' AS name", &[])
            .await?
            .one::<Item>()?,
        Item {
            value: "named".into()
        }
    );
    assert_eq!(
        db.query("SELECT NULL", &[])
            .await?
            .one::<Option<String>>()?,
        None
    );
    assert_eq!(
        db.query("SELECT 1 WHERE 0", &[]).await?.optional::<i64>()?,
        None
    );
    assert!(matches!(
        db.query("SELECT 1 WHERE 0", &[]).await?.one::<i64>(),
        Err(SqlError::NoRows)
    ));
    let multiple = db.query("SELECT 1 UNION ALL SELECT 2", &[]).await?;
    assert!(matches!(
        multiple.clone().one::<i64>(),
        Err(SqlError::TooManyRows)
    ));
    assert!(matches!(
        multiple.optional::<i64>(),
        Err(SqlError::TooManyRows)
    ));
    assert!(matches!(
        db.query("SELECT 1, 2", &[]).await?.one::<i64>(),
        Err(SqlError::Decode { .. })
    ));
    assert!(matches!(
        db.query("SELECT 1", &[]).await?.one::<(i64, i64)>(),
        Err(SqlError::Decode { .. })
    ));
    assert!(matches!(
        db.query("SELECT NULL", &[]).await?.one::<String>(),
        Err(SqlError::Decode { .. })
    ));
    assert_eq!(
        db.query(
            "SELECT :flag, :missing",
            &[true.into_value(), Option::<bool>::None.into_value()]
        )
        .await?
        .one::<(bool, Option<bool>)>()?,
        (true, None)
    );
    Ok(())
}

#[tokio::test]
async fn closure_decoders_enforce_cardinality_and_propagate_errors() -> SqlResult<()> {
    let db = test_support::db(&[]).expect("test db");
    let offset = 10;
    let decode = |row: &Row| Ok(row.get_at::<i64>(0)? + offset);
    let single = db.query("SELECT 1", ()).await?;
    assert_eq!(single.clone().one_with(decode)?, 11);
    assert_eq!(single.clone().optional_with(decode)?, Some(11));
    assert_eq!(single.all_with(decode)?, vec![11]);

    let called = std::cell::Cell::new(false);
    let decode = |_: &Row| {
        called.set(true);
        Ok(())
    };
    let empty = db.query("SELECT 1 WHERE 0", ()).await?;
    assert!(matches!(
        empty.clone().one_with(decode),
        Err(SqlError::NoRows)
    ));
    assert_eq!(empty.clone().optional_with(decode)?, None);
    assert_eq!(empty.all_with(decode)?, Vec::<()>::new());
    let multiple = db.query("SELECT 1 UNION ALL SELECT 2", ()).await?;
    assert!(matches!(
        multiple.clone().one_with(decode),
        Err(SqlError::TooManyRows)
    ));
    assert!(matches!(
        multiple.clone().optional_with(decode),
        Err(SqlError::TooManyRows)
    ));
    assert!(!called.get());
    assert_eq!(
        multiple.all_with(|row| Ok(row.get_at::<i64>(0)? + offset))?,
        vec![11, 12]
    );

    let invalid = db.query("SELECT 'wrong type'", ()).await?;
    let decode = |row: &Row| row.get_at::<i64>(0);
    assert!(matches!(
        invalid.clone().one_with(decode),
        Err(SqlError::Decode { .. })
    ));
    assert!(matches!(
        invalid.clone().optional_with(decode),
        Err(SqlError::Decode { .. })
    ));
    assert!(matches!(
        invalid.all_with(decode),
        Err(SqlError::Decode { .. })
    ));
    assert!(matches!(
        db.query("SELECT 1 UNION ALL SELECT 'wrong type'", ())
            .await?
            .all_with(decode),
        Err(SqlError::Decode { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn parameter_containers_and_tuples_work_across_executors() -> SqlResult<()> {
    async fn read(executor: &impl QueryExecutor, params: impl IntoParams) -> SqlResult<i64> {
        executor.query("SELECT ?1 + ?2", params).await?.one()
    }
    async fn write(executor: &impl Executor, params: impl IntoParams) -> SqlResult<i64> {
        executor
            .execute("INSERT INTO items (name) VALUES (?1) RETURNING id", params)
            .await?
            .result_set
            .one()
    }

    let db = test_support::db(MIGRATIONS).expect("test db");
    let directory = db.inner.path.parent().expect("database parent").to_owned();
    assert_eq!(db._runtime.open_connection_count(), 0);
    assert_eq!(
        Database::query(&db, "SELECT 42", ()).await?.one::<i64>()?,
        42
    );
    assert!(db.inner.path.exists());
    let values = vec![1.into_value(), 2.into_value()];
    assert_eq!(read(&db, values.as_slice()).await?, 3);
    assert_eq!(read(&db, &values).await?, 3);
    let allocation = values.as_ptr();
    let values = values.into_params();
    assert_eq!(values.as_ptr(), allocation);
    assert_eq!(read(&db, values).await?, 3);
    assert_eq!(read(&db, (1_i64, 2_i64)).await?, 3);
    assert_eq!(write(&db, ("first",)).await?, 1);

    macro_rules! check_tuple {
        ($($value:expr),+) => {
            let values = ($($value,)+).into_params();
            let placeholders = vec!["?"; values.len()].join(", ");
            assert_eq!(
                db.query(&format!("SELECT {placeholders}"), ($($value,)+)).await?
                    .rows.into_iter().next().expect("one row").into_values(),
                values
            );
        };
    }
    check_tuple!(1_i64);
    check_tuple!(1_i64, 2_i64);
    check_tuple!(1_i64, 2_i64, 3_i64);
    check_tuple!(1_i64, 2_i64, 3_i64, 4_i64);
    check_tuple!(1_i64, 2_i64, 3_i64, 4_i64, 5_i64);
    check_tuple!(1_i64, 2_i64, 3_i64, 4_i64, 5_i64, 6_i64);
    check_tuple!(1_i64, 2_i64, 3_i64, 4_i64, 5_i64, 6_i64, 7_i64);
    check_tuple!(
        1_i64,
        2.5_f64,
        "text",
        String::from("owned"),
        true,
        Some(6_i64),
        None::<i64>,
        vec![7_u8, 8]
    );

    let mut transaction = db.begin_write().await?;
    assert_eq!(read(&transaction, (2_i64, 3_i64)).await?, 5);
    assert_eq!(write(&transaction, ("second",)).await?, 2);
    let savepoint = transaction.savepoint("params").await?;
    assert_eq!(read(&savepoint, (3_i64, 4_i64)).await?, 7);
    assert_eq!(write(&savepoint, ("third",)).await?, 3);
    savepoint.release().await?;
    transaction.commit().await?;
    let snapshot = db.begin_read().await?;
    assert_eq!(read(&snapshot, (4_i64, 5_i64)).await?, 9);
    assert_eq!(
        snapshot
            .query("SELECT ?1", ("snapshot",))
            .await?
            .one::<String>()?,
        "snapshot"
    );
    snapshot.rollback().await?;
    assert!(matches!(
        db.query("SELECT ?1", ()).await,
        Err(SqlError::InvalidParams(_))
    ));
    assert!(matches!(
        db.query("SELECT ?1", (1_i64, 2_i64)).await,
        Err(SqlError::InvalidParams(_))
    ));
    drop(db);
    assert!(!directory.exists());
    Ok(())
}

#[tokio::test]
async fn read_snapshot_is_pinned_at_begin_and_rejects_writes() -> SqlResult<()> {
    let db = test_support::db(MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    db.execute("INSERT INTO items (name) VALUES ('before')", &[])
        .await?;
    let snapshot = db.begin_read().await?;
    db.execute("INSERT INTO items (name) VALUES ('after')", &[])
        .await?;
    assert_eq!(
        snapshot
            .query("SELECT name FROM items", &[])
            .await?
            .all::<String>()?,
        vec!["before"]
    );
    let column_query = "SELECT COUNT(*) FROM pragma_table_info('items') WHERE name = 'name'";
    assert_eq!(db.query(column_query, ()).await?.one::<i64>()?, 1);
    assert_eq!(snapshot.query(column_query, ()).await?.one::<i64>()?, 1);
    assert_eq!(
        snapshot.query("PRAGMA TaBlE_InFo('items')", ()).await?.rows[0].get::<String>("name")?,
        "id"
    );
    for sql in [
        "DELETE FROM items",
        "INSERT INTO items (name) VALUES ('bad') RETURNING id",
        "CREATE TABLE bad (id INTEGER)",
        "BEGIN",
        "PRAGMA query_only = OFF",
        "PRAGMA writable_schema = ON",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA cache_size = 0",
        "ATTACH DATABASE ':memory:' AS bad",
    ] {
        assert!(db.query(sql, &[]).await.is_err(), "{sql}");
        assert!(snapshot.query(sql, &[]).await.is_err(), "{sql}");
    }
    let blockers = tests::block_workers(runtime).await;
    drop(snapshot);
    assert_eq!(
        db.inner.reader_gate.available_permits(),
        runtime.inner.cfg.max_readers_per_db - 1
    );
    drop(blockers);
    let snapshot = db.begin_read().await?;
    assert_eq!(
        snapshot
            .query("SELECT name FROM items ORDER BY id", &[])
            .await?
            .all::<String>()?,
        vec!["before", "after"]
    );
    snapshot.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn nested_savepoints_and_raw_transaction_control() -> SqlResult<()> {
    let db = test_support::db(MIGRATIONS).expect("test db");
    let mut tx = db.begin_write().await?;
    tx.execute("INSERT INTO items (name) VALUES ('outer')", &[])
        .await?;
    {
        let mut first = tx.savepoint("part").await?;
        first
            .execute("INSERT INTO items (name) VALUES ('part')", &[])
            .await?;
        let inner = first.savepoint("part").await?;
        inner
            .execute("INSERT INTO items (name) VALUES ('inner')", &[])
            .await?;
        inner.release().await?;
    }
    assert_eq!(
        tx.query("SELECT name FROM items", &[])
            .await?
            .all::<String>()?,
        vec!["outer"]
    );
    let guard = tx.savepoint("keep").await?;
    guard
        .execute("INSERT INTO items (name) VALUES ('keep')", &[])
        .await?;
    guard.release().await?;
    let guard = tx.savepoint("discard").await?;
    guard
        .execute("INSERT INTO items (name) VALUES ('discard')", &[])
        .await?;
    guard.rollback().await?;
    for sql in ["COMMIT", "ROLLBACK", "SAVEPOINT bypass", "BEGIN"] {
        assert!(tx.execute_batch(sql).await.is_err(), "{sql}");
        assert!(tx.query(sql, &[]).await.is_err(), "{sql}");
    }
    tx.commit().await?;
    assert_eq!(
        db.query("SELECT name FROM items ORDER BY id", &[])
            .await?
            .all::<String>()?,
        vec!["outer", "keep"]
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_savepoint_operations_are_ordered_before_parent_work() -> SqlResult<()> {
    let db = test_support::db(MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    let mut tx = db.begin_write().await?;
    let blockers = tests::block_workers(runtime).await;
    let mut pending = Box::pin(tx.savepoint("cancelled_begin"));
    assert_pending(pending.as_mut()).await;
    drop(pending);
    drop(blockers);
    let guard = tx.savepoint("cancelled_statement").await?;
    let blockers = tests::block_workers(runtime).await;
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
    let blockers = tests::block_workers(runtime).await;
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
    let db = test_support::db(MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    db.migrate().await?;
    let blockers = tests::block_workers(runtime).await;
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
async fn statement_cache_reuses_preparation_and_validates_bind_count() -> SqlResult<()> {
    let db = test_support::db(&[]).expect("test db");
    db.run(true, |connection| {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
        let prepared = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&prepared);
        connection.authorizer(Some(move |context: AuthContext<'_>| {
            if matches!(context.action, AuthAction::Select) {
                count.fetch_add(1, Ordering::SeqCst);
            }
            Authorization::Allow
        }));
        for value in [1_i64, 2] {
            assert_eq!(
                run_query(connection, "SELECT ?1", &[value.into_value()])?.one::<i64>()?,
                value
            );
        }
        assert_eq!(prepared.load(Ordering::SeqCst), 1);
        assert!(matches!(
            run_query(connection, "SELECT ?1", &[]),
            Err(SqlError::InvalidParams(_))
        ));
        assert!(matches!(
            run_query(
                connection,
                "SELECT ?1",
                &[1_i64.into_value(), 2_i64.into_value()]
            ),
            Err(SqlError::InvalidParams(_))
        ));
        assert_eq!(
            run_query(connection, "SELECT ?1", &[SqlValue::Null])?.one::<Option<i64>>()?,
            None
        );
        authorize(connection, false);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn ephemeral_database_lifetime_follows_clones_and_transactions() -> SqlResult<()> {
    let guard = test_support::db(MIGRATIONS).expect("test db");
    let database = Database::clone(&guard);
    let directory = database
        .inner
        .path
        .parent()
        .expect("database parent")
        .to_owned();
    drop(guard);
    database
        .execute("INSERT INTO items(name) VALUES (?1)", ("retained",))
        .await?;
    let transaction = database.begin_write().await?;
    drop(database);
    assert!(directory.exists());
    assert_eq!(
        transaction
            .query("SELECT name FROM items", ())
            .await?
            .one::<String>()?,
        "retained"
    );
    transaction.commit().await?;
    assert!(!directory.exists());
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
    let blockers = tests::block_workers(&runtime).await;
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
    let blockers = tests::block_workers(&runtime).await;
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
async fn shared_database_migration_histories_are_independent_and_atomic() -> SqlResult<()> {
    let database = test_support::db(MIGRATIONS).expect("test db");
    let extra = [Migration {
        version: 1,
        name: "extra",
        sql: "CREATE TABLE extra (item_id INTEGER REFERENCES items(id));",
    }];
    database.apply_migrations("extra_history", &extra).await?;
    database.apply_migrations("extra_history", &extra).await?;
    assert!(
        database
            .execute("INSERT INTO extra VALUES (1)", ())
            .await
            .is_err()
    );
    database
        .execute("INSERT INTO items(name) VALUES ('base')", ())
        .await?;
    database.execute("INSERT INTO extra VALUES (1)", ()).await?;
    assert_eq!(
        database
            .query("SELECT item_id FROM extra", ())
            .await?
            .one::<i64>()?,
        1
    );
    let invalid = [
        extra[0],
        Migration {
            version: 2,
            name: "invalid",
            sql: "CREATE TABLE partial (id INTEGER); INSERT INTO missing VALUES (1);",
        },
    ];
    assert!(
        database
            .apply_migrations("extra_history", &invalid)
            .await
            .is_err()
    );
    assert_eq!(
        database
            .query("SELECT version FROM extra_history", ())
            .await?
            .all::<i64>()?,
        vec![1]
    );
    assert!(
        !database
            .query(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'partial')",
                ()
            )
            .await?
            .one::<bool>()?
    );
    assert!(database.execute_batch("COMMIT").await.is_err());
    assert_eq!(
        database.migration_status().await?,
        vec![(MIGRATIONS[0], MigrationState::Applied)]
    );
    Ok(())
}

#[tokio::test]
async fn migration_status_is_read_only_and_embedded_migrations_apply() -> SqlResult<()> {
    let db = test_support::db(MIGRATIONS).expect("test db");
    let runtime = &db._runtime;
    assert_eq!(
        db.migration_status().await?,
        vec![(MIGRATIONS[0], MigrationState::Pending)]
    );
    assert!(!db.inner.path.exists());
    assert_eq!(runtime.open_connection_count(), 0);
    db.migrate().await?;
    db.migrate().await?;
    assert_eq!(
        db.migration_status().await?,
        vec![(MIGRATIONS[0], MigrationState::Applied)]
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM items", &[])
            .await?
            .one::<i64>()?,
        0
    );
    Ok(())
}
