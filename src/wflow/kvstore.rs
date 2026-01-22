use crate::interlude::*;

use std::str::FromStr;
use tokio::sync::{mpsc, oneshot};
use wflow_core::kvstore::*;

use utils_rs::expect_tags::ERROR_CHANNEL;

enum KvMsg {
    BootTable {
        table: Arc<str>,
        resp: oneshot::Sender<Res<()>>,
    },
    Get {
        table: Arc<str>,
        key: Vec<u8>,
        resp: oneshot::Sender<Res<Option<Arc<[u8]>>>>,
    },
    Set {
        table: Arc<str>,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        resp: oneshot::Sender<Res<Option<Arc<[u8]>>>>,
    },
    Del {
        table: Arc<str>,
        key: Vec<u8>,
        resp: oneshot::Sender<Res<Option<Arc<[u8]>>>>,
    },
    Increment {
        table: Arc<str>,
        key: Vec<u8>,
        delta: i64,
        resp: oneshot::Sender<Res<i64>>,
    },
    NewCas {
        table: Arc<str>,
        key: Vec<u8>,
        #[allow(clippy::type_complexity)]
        resp: oneshot::Sender<Res<(Option<Arc<[u8]>>, i64)>>,
    },
    Swap {
        table: Arc<str>,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        snapshot_version: i64,
        #[allow(clippy::type_complexity)]
        resp: oneshot::Sender<Res<Result<(), (Option<Arc<[u8]>>, i64)>>>,
    },
}

#[derive(Clone)]
pub struct SqliteKvFactory {
    sender: mpsc::UnboundedSender<KvMsg>,
}

impl SqliteKvFactory {
    pub async fn boot(db_url: &str) -> Res<Self> {
        let opts = sqlx::sqlite::SqliteConnectOptions::from_str(db_url)?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let db_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_with(opts)
            .await?;
        // Use unbounded channel since the worker is single-threaded and processes messages sequentially
        let (tx, mut rx) = mpsc::unbounded_channel();
        let worker = SqliteKvWorker { db_pool };
        tokio::spawn(async move {
            worker.run(&mut rx).await.unwrap_or_log();
        });
        Ok(Self { sender: tx })
    }

    pub async fn open_store(&self, table_name: &str) -> Res<SqliteKvStore> {
        let table: Arc<str> = table_name.into();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::BootTable {
                table: Arc::clone(&table),
                resp: tx,
            })
            .map_err(|_| ferr!("factory gone"))?;
        rx.await.wrap_err(ERROR_CHANNEL)??;

        Ok(SqliteKvStore {
            table_name: table,
            sender: self.sender.clone(),
        })
    }
}

/// A SQLite-backed key-value store implementation.
#[derive(Clone)]
pub struct SqliteKvStore {
    table_name: Arc<str>,
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
                        table: Arc::clone(&store.table_name),
                        key: Arc::clone(&key),
                        value: new_value,
                        snapshot_version: version,
                        resp: tx,
                    })
                    .map_err(|_| ferr!("worker gone"))?;

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
    db_pool: sqlx::SqlitePool,
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
                        .handle_boot(&table)
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
                        .handle_get(&table, &key)
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
                        .handle_set(&table, key, value)
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
                        .handle_del(&table, &key)
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
                        .handle_increment(&table, &key, delta)
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
                        .handle_new_cas(&table, &key)
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
                        .handle_swap(&table, key, value, snapshot_version)
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

    async fn handle_boot(&self, table: &str) -> Res<()> {
        // Sanitize table name
        if !table
            .chars()
            .all(|chr| chr.is_alphanumeric() || chr == '_' || chr == '-')
        {
            return Err(ferr!("invalid table name: {}", table));
        }

        sqlx::query(&format!(
            r#"
                CREATE TABLE IF NOT EXISTS "{table}" (
                    key     BLOB PRIMARY KEY,
                    value   BLOB NOT NULL,
                    version INTEGER NOT NULL
                )
                "#
        ))
        .execute(&self.db_pool)
        .await
        .wrap_err_with(|| format!("failed to create table: {}", table))?;
        Ok(())
    }

    async fn handle_get(&self, table: &str, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let row = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            r#"SELECT value FROM "{table}" WHERE key = ?1"#
        ))
        .bind(key)
        .fetch_optional(&self.db_pool)
        .await?;

        Ok(row.map(|val| val.into_boxed_slice().into()))
    }

    async fn handle_set(
        &self,
        table: &str,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    ) -> Res<Option<Arc<[u8]>>> {
        let mut tx = self.db_pool.begin().await?;

        let old = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            r#"SELECT value FROM "{table}" WHERE key = ?1"#
        ))
        .bind(key.as_ref())
        .fetch_optional(&mut *tx)
        .await?;

        sqlx::query(&format!(
            r#"
            INSERT INTO "{table}"(key, value, version)
            VALUES (?1, ?2, 1)
            ON CONFLICT(key) DO UPDATE
            SET value = excluded.value,
                version = version + 1
            "#
        ))
        .bind(key.as_ref())
        .bind(value.as_ref())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(old.map(|value| value.into_boxed_slice().into()))
    }

    async fn handle_del(&self, table: &str, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let mut tx = self.db_pool.begin().await?;

        let old = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            r#"SELECT value FROM "{table}" WHERE key = ?1"#
        ))
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;

        sqlx::query(&format!(r#"DELETE FROM "{table}" WHERE key = ?1"#))
            .bind(key)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(old.map(|value| value.into_boxed_slice().into()))
    }

    async fn handle_increment(&self, table: &str, key: &[u8], delta: i64) -> Res<i64> {
        let mut tx = self.db_pool.begin().await?;

        let current: Option<Vec<u8>> =
            sqlx::query_scalar(&format!(r#"SELECT value FROM "{table}" WHERE key = ?1"#))
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?;

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

        sqlx::query(&format!(
            r#"
            INSERT INTO "{table}"(key, value, version)
            VALUES (?1, ?2, 1)
            ON CONFLICT(key) DO UPDATE
            SET value = excluded.value,
                version = version + 1
            "#
        ))
        .bind(key)
        .bind(&encoded[..])
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(next)
    }

    async fn handle_new_cas(&self, table: &str, key: &[u8]) -> Res<(Option<Arc<[u8]>>, i64)> {
        let row = sqlx::query_as::<_, (Vec<u8>, i64)>(&format!(
            r#"SELECT value, version FROM "{table}" WHERE key = ?1"#
        ))
        .bind(key)
        .fetch_optional(&self.db_pool)
        .await?;

        let snapshot_value = row
            .as_ref()
            .map(|(val, _)| -> Arc<[u8]> { val.clone().into() });
        let snapshot_version = row.map(|(_, val)| val).unwrap_or(0);

        Ok((snapshot_value, snapshot_version))
    }

    async fn handle_swap(
        &self,
        table: &str,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        snapshot_version: i64,
    ) -> Res<Result<(), (Option<Arc<[u8]>>, i64)>> {
        let mut tx = self.db_pool.begin().await?;

        let result = if snapshot_version == 0 {
            sqlx::query(&format!(
                r#"
                INSERT INTO "{table}"(key, value, version)
                SELECT ?1, ?2, 1
                WHERE NOT EXISTS (SELECT 1 FROM "{table}" WHERE key = ?1)
                "#
            ))
            .bind(key.as_ref())
            .bind(value.as_ref())
            .execute(&mut *tx)
            .await
        } else {
            sqlx::query(&format!(
                r#"
                UPDATE "{table}"
                SET value = ?2,
                    version = version + 1
                WHERE key = ?1 AND version = ?3
                "#
            ))
            .bind(key.as_ref())
            .bind(value.as_ref())
            .bind(snapshot_version)
            .execute(&mut *tx)
            .await
        };

        match result {
            Ok(res) => {
                if res.rows_affected() == 1 {
                    tx.commit().await?;
                    Ok(Ok(()))
                } else {
                    let _ = tx.rollback().await;
                    let (new_val, new_ver) = self.handle_new_cas(table, &key).await?;
                    Ok(Err((new_val, new_ver)))
                }
            }
            Err(err) => {
                let _ = tx.rollback().await;
                Err(err.into())
            }
        }
    }
}

#[async_trait]
impl KvStore for SqliteKvStore {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Get {
                table: Arc::clone(&self.table_name),
                key: key.to_vec(),
                resp: tx,
            })
            .map_err(|_| ferr!("worker gone"))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Set {
                table: Arc::clone(&self.table_name),
                key,
                value,
                resp: tx,
            })
            .map_err(|_| ferr!("worker gone"))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Del {
                table: Arc::clone(&self.table_name),
                key: key.to_vec(),
                resp: tx,
            })
            .map_err(|_| ferr!("worker gone"))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::Increment {
                table: Arc::clone(&self.table_name),
                key: key.to_vec(),
                delta,
                resp: tx,
            })
            .map_err(|_| ferr!("worker gone"))?;
        rx.await.wrap_err(ERROR_CHANNEL)?
    }

    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(KvMsg::NewCas {
                table: Arc::clone(&self.table_name),
                key: key.to_vec(),
                resp: tx,
            })
            .map_err(|_| ferr!("worker gone"))?;
        let (snapshot_value, snapshot_version) = rx.await.wrap_err(ERROR_CHANNEL)??;

        let key: Arc<[u8]> = key.into();
        Ok(self.make_cas_guard(key, snapshot_value, snapshot_version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wflow_core::kvstore::tests::{test_kv_store_concurrency, test_kv_store_impl};

    #[tokio::test]
    async fn test_sqlite_kvstore() -> Res<()> {
        let factory = SqliteKvFactory::boot("sqlite::memory:").await?;
        let store = factory.open_store("test_kv").await?;
        let store_dyn = Arc::new(store);
        test_kv_store_impl(Arc::clone(&store_dyn) as _).await?;
        test_kv_store_concurrency(store_dyn).await
    }
}
