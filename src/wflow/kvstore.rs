use crate::interlude::*;

use sqlite_rs::Database;
use sqlite_rs::Sql;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use wflow_core::kvstore::*;

enum KvMsg {
    BootTable {
        table: &'static str,
        resp: oneshot::Sender<Res<()>>,
    },
    Get {
        table: &'static str,
        key: Vec<u8>,
        resp: oneshot::Sender<Res<Option<Arc<[u8]>>>>,
    },
    Set {
        table: &'static str,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        resp: oneshot::Sender<Res<Option<Arc<[u8]>>>>,
    },
    Del {
        table: &'static str,
        key: Vec<u8>,
        resp: oneshot::Sender<Res<Option<Arc<[u8]>>>>,
    },
    Increment {
        table: &'static str,
        key: Vec<u8>,
        delta: i64,
        resp: oneshot::Sender<Res<i64>>,
    },
    NewCas {
        table: &'static str,
        key: Vec<u8>,
        #[expect(clippy::type_complexity)]
        resp: oneshot::Sender<Res<(Option<Arc<[u8]>>, i64)>>,
    },
    Swap {
        table: &'static str,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        snapshot_version: i64,
        #[expect(clippy::type_complexity)]
        resp: oneshot::Sender<Res<Result<(), (Option<Arc<[u8]>>, i64)>>>,
    },
}

#[derive(Clone)]
pub struct SqliteKvFactory {
    sender: mpsc::UnboundedSender<KvMsg>,
}

impl SqliteKvFactory {
    pub async fn boot(sql: Database) -> Res<Self> {
        // Use unbounded channel since the worker is single-threaded and processes messages sequentially
        let (tx, mut rx) = mpsc::unbounded_channel();
        let worker = SqliteKvWorker { sql };
        tokio::spawn(async move {
            worker.run(&mut rx).await.unwrap_or_log();
        });
        Ok(Self { sender: tx })
    }

    pub async fn open_store(&self, table_name: &'static str) -> Res<SqliteKvStore> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::BootTable {
                table: table_name,
                resp: tx,
            })
            .map_err(|_| ferr!("factory gone"))?;
        rx.await.wrap_err(ERROR_CHANNEL)??;

        Ok(SqliteKvStore {
            table_name,
            sender: self.sender.clone(),
        })
    }
}

/// A SQLite-backed key-value store implementation.
///
/// Every store owns one table whose name is chosen by the caller at
/// `open_store` time, so the SQL here is assembled with the [`Sql`] builder
/// (`ident(table)`) instead of living in checked `sqlite/queries/*.sql` files:
/// a query file names one fixed table and this crate has no fixed table to
/// name. The schema is created per table in `handle_boot` for the same reason.
#[derive(Clone)]
pub struct SqliteKvStore {
    table_name: &'static str,
    sender: mpsc::UnboundedSender<KvMsg>,
}

impl Drop for SqliteKvStore {
    fn drop(&mut self) {
        trace!(?self.table_name, "SqliteKvStore dropped");
    }
}

impl SqliteKvStore {
    fn make_cas_guard(&self, key: Arc<[u8]>, value: Option<Arc<[u8]>>, version: i64) -> CasGuard {
        let snapshot_value = value.clone();
        let current_cb = move || snapshot_value.clone();

        let store = self.clone();
        let key_for_cb = Arc::clone(&key);
        let swap_cb = move |new_value: Arc<[u8]>| -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>> {
            let store = store.clone();
            let key = Arc::clone(&key_for_cb);
            Box::pin(async move {
                let (tx, rx) = oneshot::channel();
                store
                    .sender
                    .send(KvMsg::Swap {
                        table: store.table_name,
                        key: Arc::clone(&key),
                        value: new_value,
                        snapshot_version: version,
                        resp: tx,
                    })
                    .map_err(|_| ferr!(ERROR_ACTOR))?;

                match rx.await.wrap_err(ERROR_CHANNEL)?? {
                    Ok(()) => Ok(Ok(())),
                    Err((new_v, new_ver)) => {
                        let new_guard = store.make_cas_guard(key, new_v, new_ver);
                        Ok(Err(CasError::CasFailed(new_guard)))
                    }
                }
            })
        };

        CasGuard::new(current_cb, swap_cb)
    }
}

struct SqliteKvWorker {
    sql: Database,
}

impl SqliteKvWorker {
    #[tracing::instrument(skip(self, rx), err, ret)]
    async fn run(&self, rx: &mut mpsc::UnboundedReceiver<KvMsg>) -> Res<()> {
        loop {
            let msg = match rx.recv().await {
                Some(msg) => msg,
                None => break,
            };

            match msg {
                KvMsg::BootTable { table, resp } => {
                    let result = self
                        .handle_boot(table)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_boot failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                ?table,
                                "caller dropped before receiving BootTable response"
                            )
                        })
                        .ok();
                }
                KvMsg::Get { table, key, resp } => {
                    let result = self
                        .handle_get(table, &key)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_get failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(?err, ?table, "caller dropped before receiving Get response")
                        })
                        .ok();
                }
                KvMsg::Set {
                    table,
                    key,
                    value,
                    resp,
                } => {
                    let result = self
                        .handle_set(table, key, value)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_set failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(?err, ?table, "caller dropped before receiving Set response")
                        })
                        .ok();
                }
                KvMsg::Del { table, key, resp } => {
                    let result = self
                        .handle_del(table, &key)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_del failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(?err, ?table, "caller dropped before receiving Del response")
                        })
                        .ok();
                }
                KvMsg::Increment {
                    table,
                    key,
                    delta,
                    resp,
                } => {
                    let result = self
                        .handle_increment(table, &key, delta)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_increment failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                ?table,
                                "caller dropped before receiving Increment response"
                            )
                        })
                        .ok();
                }
                KvMsg::NewCas { table, key, resp } => {
                    let result = self
                        .handle_new_cas(table, &key)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_new_cas failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                ?table,
                                "caller dropped before receiving NewCas response"
                            )
                        })
                        .ok();
                }
                KvMsg::Swap {
                    table,
                    key,
                    value,
                    snapshot_version,
                    resp,
                } => {
                    let result = self
                        .handle_swap(table, key, value, snapshot_version)
                        .await
                        .inspect_err(|err| error!(?err, ?table, "handle_swap failed"));
                    resp.send(result)
                        .inspect_err(|err| {
                            error!(
                                ?err,
                                ?table,
                                "caller dropped before receiving Swap response"
                            )
                        })
                        .ok();
                }
            }
        }
        Ok(())
    }

    async fn handle_boot(&self, table: &'static str) -> Res<()> {
        Sql::new("CREATE TABLE IF NOT EXISTS ")
            .ident(table)
            .push(
                " (
                    key     BLOB PRIMARY KEY
                  , value   BLOB NOT NULL
                  , version INTEGER NOT NULL
                ) STRICT",
            )
            .execute(&self.sql)
            .await
            .wrap_err_with(|| format!("failed to create table: {table}"))?;
        Ok(())
    }

    async fn handle_get(&self, table: &'static str, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let row = Sql::new("SELECT value FROM ")
            .ident(table)
            .push(" WHERE key = ")
            .bind(key)
            .query(&self.sql)
            .await?
            .optional::<Vec<u8>>()?;

        Ok(row.map(|val| val.into_boxed_slice().into()))
    }

    async fn handle_set(
        &self,
        table: &'static str,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    ) -> Res<Option<Arc<[u8]>>> {
        let tx = self.sql.begin_write().await?;

        let old = Sql::new("SELECT value FROM ")
            .ident(table)
            .push(" WHERE key = ")
            .bind(key.as_ref())
            .query(&tx)
            .await?
            .optional::<Vec<u8>>()?;

        Sql::new("INSERT INTO ")
            .ident(table)
            .push(" (key, value, version) VALUES (")
            .bind(key.as_ref())
            .push(", ")
            .bind(value.as_ref())
            .push(
                ", 1)
                ON CONFLICT(key) DO UPDATE
                SET value = excluded.value
                  , version = version + 1",
            )
            .execute(&tx)
            .await?;

        tx.commit().await?;
        Ok(old.map(|value| value.into_boxed_slice().into()))
    }

    async fn handle_del(&self, table: &'static str, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let tx = self.sql.begin_write().await?;

        let old = Sql::new("SELECT value FROM ")
            .ident(table)
            .push(" WHERE key = ")
            .bind(key)
            .query(&tx)
            .await?
            .optional::<Vec<u8>>()?;

        Sql::new("DELETE FROM ")
            .ident(table)
            .push(" WHERE key = ")
            .bind(key)
            .execute(&tx)
            .await?;

        tx.commit().await?;
        Ok(old.map(|value| value.into_boxed_slice().into()))
    }

    async fn handle_increment(&self, table: &'static str, key: &[u8], delta: i64) -> Res<i64> {
        let tx = self.sql.begin_write().await?;

        let current = Sql::new("SELECT value FROM ")
            .ident(table)
            .push(" WHERE key = ")
            .bind(key)
            .query(&tx)
            .await?
            .optional::<Vec<u8>>()?;

        let current_val = if let Some(bytes) = current {
            if bytes.len() != 8 {
                eyre::bail!("value is not a i64: byte len {len} != 8", len = bytes.len());
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            i64::from_le_bytes(buf)
        } else {
            0
        };

        let next = current_val
            .checked_add(delta)
            .ok_or_else(|| ferr!("i64 overflow in increment"))?;

        let encoded = next.to_le_bytes();

        Sql::new("INSERT INTO ")
            .ident(table)
            .push(" (key, value, version) VALUES (")
            .bind(key)
            .push(", ")
            .bind(&encoded[..])
            .push(
                ", 1)
                ON CONFLICT(key) DO UPDATE
                SET value = excluded.value
                  , version = version + 1",
            )
            .execute(&tx)
            .await?;

        tx.commit().await?;
        Ok(next)
    }

    async fn handle_new_cas(
        &self,
        table: &'static str,
        key: &[u8],
    ) -> Res<(Option<Arc<[u8]>>, i64)> {
        let row = Sql::new("SELECT value, version FROM ")
            .ident(table)
            .push(" WHERE key = ")
            .bind(key)
            .query(&self.sql)
            .await?
            .optional::<(Vec<u8>, i64)>()?;

        Ok(match row {
            Some((value, version)) => (Some(value.into()), version),
            None => (None, 0),
        })
    }

    async fn handle_swap(
        &self,
        table: &'static str,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        snapshot_version: i64,
    ) -> Res<Result<(), (Option<Arc<[u8]>>, i64)>> {
        let tx = self.sql.begin_write().await?;

        let result = if snapshot_version == 0 {
            Sql::new("INSERT INTO ")
                .ident(table)
                .push(" (key, value, version) SELECT ")
                .bind(key.as_ref())
                .push(", ")
                .bind(value.as_ref())
                .push(", 1 WHERE NOT EXISTS (SELECT 1 FROM ")
                .ident(table)
                .push(" WHERE key = ")
                .bind(key.as_ref())
                .push(")")
                .execute(&tx)
                .await?
        } else {
            Sql::new("UPDATE ")
                .ident(table)
                .push(" SET value = ")
                .bind(value.as_ref())
                .push(", version = version + 1 WHERE key = ")
                .bind(key.as_ref())
                .push(" AND version = ")
                .bind(snapshot_version)
                .execute(&tx)
                .await?
        };

        if result.rows_affected == 1 {
            tx.commit().await?;
            Ok(Ok(()))
        } else {
            tx.rollback().await?;
            let (new_val, new_ver) = self.handle_new_cas(table, &key).await?;
            Ok(Err((new_val, new_ver)))
        }
    }
}

#[async_trait]
impl KvStore for SqliteKvStore {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Get {
                table: self.table_name,
                key: key.to_vec(),
                resp: tx,
            })
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Set {
                table: self.table_name,
                key,
                value,
                resp: tx,
            })
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Del {
                table: self.table_name,
                key: key.to_vec(),
                resp: tx,
            })
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Increment {
                table: self.table_name,
                key: key.to_vec(),
                delta,
                resp: tx,
            })
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::NewCas {
                table: self.table_name,
                key: key.to_vec(),
                resp: tx,
            })
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        let (snapshot_value, snapshot_version) = rx.await.wrap_err(ERROR_CHANNEL)??;

        let key: Arc<[u8]> = key.into();
        Ok(self.make_cas_guard(key, snapshot_value, snapshot_version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlite_rs::QueryExecutor;
    use wflow_core::kvstore::tests::test_kv_store_concurrency;
    use wflow_core::kvstore::tests::test_kv_store_impl;

    #[tokio::test]
    async fn test_sqlite_kvstore() -> Res<()> {
        let db = sqlite_rs::test_support::db(&[])?;
        let factory = SqliteKvFactory::boot(db.clone()).await?;
        let store = factory.open_store("test_kv").await?;
        let store_dyn = Arc::new(store);
        test_kv_store_impl(Arc::clone(&store_dyn) as _).await?;
        test_kv_store_concurrency(store_dyn).await
    }

    #[tokio::test]
    async fn old_values_and_quoted_tables() -> Res<()> {
        let db = sqlite_rs::test_support::db(&[])?;
        let factory = SqliteKvFactory::boot(db.clone()).await?;
        let first = factory.open_store("test-kv").await?;
        let second = factory.open_store("select").await?;
        let reopened = factory.open_store("test-kv").await?;
        drop(factory);

        let key: Arc<[u8]> = b"'\0; DROP TABLE test_kv; --".as_slice().into();
        let value: Arc<[u8]> = b"\0\xff'value".as_slice().into();
        let empty: Arc<[u8]> = [].as_slice().into();
        assert_eq!(first.del(&key).await?, None);
        assert_eq!(first.set(Arc::clone(&key), Arc::clone(&value)).await?, None);
        assert_eq!(second.get(&key).await?, None);
        assert_eq!(reopened.get(&key).await?, Some(Arc::clone(&value)));
        assert_eq!(
            reopened.set(Arc::clone(&key), Arc::clone(&empty)).await?,
            Some(value)
        );
        assert_eq!(first.del(&key).await?, Some(empty));
        assert_eq!(reopened.get(&key).await?, None);
        assert_eq!(reopened.del(&key).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn increment_errors_preserve_value_and_version() -> Res<()> {
        let db = sqlite_rs::test_support::db(&[])?;
        let factory = SqliteKvFactory::boot(db.clone()).await?;
        let store = factory.open_store("test_kv").await?;
        let key: Arc<[u8]> = b"counter".as_slice().into();
        for (value, delta, expected_error) in [
            (vec![1, 2, 3], 1, "value is not a i64: byte len 3 != 8"),
            (
                i64::MAX.to_le_bytes().to_vec(),
                1,
                "i64 overflow in increment",
            ),
            (
                i64::MIN.to_le_bytes().to_vec(),
                -1,
                "i64 overflow in increment",
            ),
        ] {
            assert_eq!(
                store.set(Arc::clone(&key), value.as_slice().into()).await?,
                None
            );
            let guard = store.new_cas(&key).await?;
            let error = store.increment(&key, delta).await.unwrap_err();
            assert_eq!(error.to_string(), expected_error);
            assert_eq!(
                db.query(
                    "SELECT value, version FROM test_kv WHERE key = ?",
                    (key.as_ref(),)
                )
                .await?
                .one::<(Vec<u8>, i64)>()?,
                (value, 1)
            );
            guard.swap(0_i64.to_le_bytes().as_slice().into()).await??;
            assert_eq!(store.increment(&key, 1).await?, 1);
            assert_eq!(
                store.del(&key).await?,
                Some(1_i64.to_le_bytes().as_slice().into())
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn failed_writes_roll_back_changes_and_release_transaction() -> Res<()> {
        let db = sqlite_rs::test_support::db(&[])?;
        let factory = SqliteKvFactory::boot(db.clone()).await?;
        let store = factory.open_store("test_kv").await?;
        let key: Arc<[u8]> = b"counter".as_slice().into();
        let value: Arc<[u8]> = 7_i64.to_le_bytes().as_slice().into();
        assert_eq!(store.set(Arc::clone(&key), Arc::clone(&value)).await?, None);
        db.execute_batch(
            "CREATE TABLE audit (operation TEXT NOT NULL);
             CREATE TRIGGER fail_insert AFTER INSERT ON test_kv BEGIN
                 INSERT INTO audit VALUES ('insert');
                 SELECT RAISE(FAIL, 'forced write failure');
             END;
             CREATE TRIGGER fail_update AFTER UPDATE ON test_kv BEGIN
                 INSERT INTO audit VALUES ('update');
                 SELECT RAISE(FAIL, 'forced write failure');
             END;
             CREATE TRIGGER fail_delete AFTER DELETE ON test_kv BEGIN
                 INSERT INTO audit VALUES ('delete');
                 SELECT RAISE(FAIL, 'forced write failure');
             END;",
        )
        .await?;

        #[derive(Debug)]
        enum Operation {
            Set,
            Delete,
            Increment,
            SwapExisting,
            SwapMissing,
        }
        for operation in [
            Operation::Set,
            Operation::Delete,
            Operation::Increment,
            Operation::SwapExisting,
            Operation::SwapMissing,
        ] {
            let result = match operation {
                Operation::Set => store
                    .set(Arc::clone(&key), b"replacement".as_slice().into())
                    .await
                    .map(|_| ()),
                Operation::Delete => store.del(&key).await.map(|_| ()),
                Operation::Increment => store.increment(&key, 1).await.map(|_| ()),
                Operation::SwapExisting => store
                    .new_cas(&key)
                    .await?
                    .swap(b"replacement".as_slice().into())
                    .await
                    .map(|_| ()),
                Operation::SwapMissing => store
                    .new_cas(b"missing")
                    .await?
                    .swap(b"replacement".as_slice().into())
                    .await
                    .map(|_| ()),
            };
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("forced write failure"),
                "{operation:?}: {error}"
            );
            let tx = db.begin_write().await?;
            assert_eq!(
                tx.query("SELECT key, value, version FROM test_kv", ())
                    .await?
                    .all::<(Vec<u8>, Vec<u8>, i64)>()?,
                vec![(key.to_vec(), value.to_vec(), 1)],
                "{operation:?}"
            );
            assert_eq!(
                tx.query("SELECT count(*) FROM audit", ())
                    .await?
                    .one::<i64>()?,
                0,
                "{operation:?}"
            );
            tx.commit().await?;
        }

        db.execute_batch(
            "DROP TRIGGER fail_insert; DROP TRIGGER fail_update; DROP TRIGGER fail_delete;",
        )
        .await?;
        store
            .new_cas(&key)
            .await?
            .swap(Arc::clone(&value))
            .await??;
        assert_eq!(store.increment(&key, 1).await?, 8);
        assert_eq!(
            store.del(&key).await?,
            Some(8_i64.to_le_bytes().as_slice().into())
        );
        store
            .new_cas(&key)
            .await?
            .swap(Arc::clone(&value))
            .await??;
        assert_eq!(store.get(&key).await?, Some(value));
        Ok(())
    }

    #[tokio::test]
    async fn shared_database_factories_serialize_increment_and_cas() -> Res<()> {
        let db = sqlite_rs::test_support::db(&[])?;
        let first_factory = SqliteKvFactory::boot(db.clone()).await?;
        let second_factory = SqliteKvFactory::boot(db.clone()).await?;
        let first = first_factory.open_store("test_kv").await?;
        let second = second_factory.open_store("test_kv").await?;
        drop(first_factory);
        drop(second_factory);

        let increment = |store: SqliteKvStore| async move {
            for _ in 0..32 {
                store.increment(b"counter", 1).await?;
            }
            eyre::Ok(())
        };
        tokio::try_join!(increment(first.clone()), increment(second.clone()))?;
        assert_eq!(
            first.get(b"counter").await?,
            Some(64_i64.to_le_bytes().as_slice().into())
        );
        assert_eq!(
            db.query(
                "SELECT version FROM test_kv WHERE key = ?",
                (b"counter".as_slice(),)
            )
            .await?
            .one::<i64>()?,
            64
        );

        let key: Arc<[u8]> = b"cas".as_slice().into();
        for expected in [None, Some(b"winner".as_slice().into())] {
            let first_guard = first.new_cas(&key).await?;
            let second_guard = second.new_cas(&key).await?;
            assert_eq!(first_guard.current(), expected);
            assert_eq!(second_guard.current(), expected);
            let (first_result, second_result) = tokio::try_join!(
                first_guard.swap(b"winner".as_slice().into()),
                second_guard.swap(b"winner".as_slice().into()),
            )?;
            let refreshed = match (first_result, second_result) {
                (Ok(()), Err(CasError::CasFailed(guard)))
                | (Err(CasError::CasFailed(guard)), Ok(())) => guard,
                results => panic!("expected one CAS winner, got {results:?}"),
            };
            assert_eq!(refreshed.current(), Some(b"winner".as_slice().into()));
            refreshed.swap(b"winner".as_slice().into()).await??;
        }
        let guard = first.new_cas(&key).await?;
        assert_eq!(second.del(&key).await?, Some(b"winner".as_slice().into()));
        let Err(CasError::CasFailed(refreshed)) = guard.swap(b"stale".as_slice().into()).await?
        else {
            panic!("expected CAS failure after deletion");
        };
        assert_eq!(refreshed.current(), None);
        refreshed.swap(b"recreated".as_slice().into()).await??;
        assert_eq!(
            second.get(&key).await?,
            Some(b"recreated".as_slice().into())
        );
        Ok(())
    }
}
