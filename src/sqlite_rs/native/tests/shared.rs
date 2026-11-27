// Tests that hold under every executor. `tests.rs` includes this file once per executor, in
// modules that name it as `EXECUTION`.

fn runtime() -> SqliteRuntime {
    runtime_on(EXECUTION)
}

fn db(migrations: &[Migration<'static>]) -> test_support::TestDb {
    test_support::db_on(EXECUTION, migrations).expect("test db")
}

#[tokio::test]
async fn lazy_handle_opens_nothing() {
    let rt = runtime();
    let db = rt.ephemeral(&[]).expect("ephemeral");
    drop(db);
    assert_eq!(rt.open_connection_count(), 0);
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

#[tokio::test]
async fn builder_binds_values_lists_tuples_and_identifiers() -> SqlResult<()> {
    let db = db(MIGRATIONS);
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
    let db = db(&[]);
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
    let db = db(&[]);
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

    let db = db(MIGRATIONS);
    let directory = db.inner.path.parent().expect("database parent").to_owned();
    assert_eq!(db._runtime.open_connection_count(), 0);
    assert_eq!(
        Database::query(&db, "SELECT 42", ()).await?.one::<i64>()?,
        42
    );
    assert!(db.inner.path.exists());
    let values = vec![1_i64.into_value(), 2_i64.into_value()];
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
    let db = db(MIGRATIONS);
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
    drop(snapshot);
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
    let db = db(MIGRATIONS);
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
async fn statement_cache_reuses_preparation_and_validates_bind_count() -> SqlResult<()> {
    let db = db(&[]);
    db.run(true, |connection| {
        use rusqlite::hooks::AuthAction;
        use rusqlite::hooks::AuthContext;
        use rusqlite::hooks::Authorization;
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
    let guard = db(MIGRATIONS);
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
async fn shared_database_migration_histories_are_independent_and_atomic() -> SqlResult<()> {
    let database = db(MIGRATIONS);
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
    let db = db(MIGRATIONS);
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

#[tokio::test]
async fn ephemeral_directory_is_removed_when_last_handle_drops() -> SqlResult<()> {
    let runtime = runtime();
    let database = runtime.ephemeral(MIGRATIONS).expect("ephemeral");
    let directory = database
        .inner
        .path
        .parent()
        .expect("database path has a parent")
        .to_path_buf();
    database
        .execute_batch("CREATE TABLE retained (id INTEGER)")
        .await?;
    let clone = database.clone();
    assert_eq!(runtime.live_database_count(), 1);
    drop(database);
    assert!(directory.exists(), "a live clone retains the directory");
    drop(clone);
    assert_eq!(runtime.live_database_count(), 0);
    assert!(
        !directory.exists(),
        "ephemeral directory leaked: {directory:?}"
    );
    Ok(())
}

#[tokio::test]
async fn write_commits_on_ok_and_rolls_back_on_err() -> SqlResult<()> {
    use crate::SqliteDb;
    let db = db(MIGRATIONS);
    let id = db
        .write(|tx| {
            Box::pin(async move {
                let inserted = tx
                    .execute("INSERT INTO items (name) VALUES ('kept') RETURNING id", ())
                    .await?;
                inserted.result_set.one::<i64>()
            })
        })
        .await?;
    assert_eq!(id, 1);
    let failed: SqlResult<()> = db
        .write(|tx| {
            Box::pin(async move {
                tx.execute("INSERT INTO items (name) VALUES ('discarded')", ())
                    .await?;
                Err(SqlError::InvalidQuery("the body failed".into()))
            })
        })
        .await;
    assert!(
        matches!(failed, Err(SqlError::InvalidQuery(ref message)) if message == "the body failed")
    );
    let names = db
        .read(|snapshot| {
            Box::pin(async move {
                snapshot
                    .query("SELECT name FROM items", ())
                    .await?
                    .all::<String>()
            })
        })
        .await?;
    assert_eq!(names, vec!["kept"]);
    // Both transactions finished: the writer and the reader are free again.
    assert_eq!(db.inner.writer_gate.available_permits(), 1);
    Ok(())
}

/// A reader snapshot and writers interleaving on one thread must neither deadlock nor leak
/// the writer's changes into the snapshot. The test runtime is single-threaded, so the tasks
/// below take turns only at their `yield_now`s: each holds its snapshot or transaction across
/// a turn of the others, and there are more readers than reader connections.
#[tokio::test]
async fn snapshots_and_writers_interleave_on_one_thread_without_deadlock() -> SqlResult<()> {
    let db = db(MIGRATIONS);
    db.execute("INSERT INTO items (name) VALUES ('seed')", ())
        .await?;
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..3 {
        let db = (*db).clone();
        tasks.spawn(async move {
            let tx = db.begin_write().await?;
            tx.execute(
                "INSERT INTO items (name) VALUES (?1)",
                (format!("a{writer}"),),
            )
            .await?;
            tokio::task::yield_now().await;
            tx.execute(
                "INSERT INTO items (name) VALUES (?1)",
                (format!("b{writer}"),),
            )
            .await?;
            tx.commit().await
        });
    }
    for _ in 0..5 {
        let db = (*db).clone();
        tasks.spawn(async move {
            let snapshot = db.begin_read().await?;
            let first = snapshot
                .query("SELECT COUNT(*) FROM items", ())
                .await?
                .one::<i64>()?;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            let second = snapshot
                .query("SELECT COUNT(*) FROM items", ())
                .await?
                .one::<i64>()?;
            snapshot.rollback().await?;
            assert_eq!(first, second, "a writer's rows leaked into the snapshot");
            Ok(())
        });
    }
    while let Some(finished) = tasks.join_next().await {
        finished.expect("task panicked")?;
    }
    assert_eq!(
        db.query("SELECT COUNT(*) FROM items", ())
            .await?
            .one::<i64>()?,
        7
    );
    Ok(())
}
