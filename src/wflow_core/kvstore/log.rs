use crate::interlude::*;

use futures::StreamExt;
use futures::stream::BoxStream;

use crate::kvstore::KvStore;
use crate::log::LogStore;
use crate::log::TailLogEntry;

pub struct KvStoreLog {
    // we use these to communicate the latest
    // written entry indices by this instance
    local_commited_idx_rx: tokio::sync::watch::Receiver<u64>,
    local_commited_idx_tx: tokio::sync::watch::Sender<u64>,
    kv_store: Arc<dyn KvStore + Send + Sync>,
}

impl KvStoreLog {
    const LATEST_ID_KEY: &[u8] = b"___kv_store_log_latest_id";

    pub async fn new(kv_store: Arc<dyn KvStore + Send + Sync>) -> Res<Self> {
        let latest_idx: u64 = kv_store
            .get(Self::LATEST_ID_KEY)
            .await?
            .map(arc_bytes_to_i64)
            .unwrap_or_default()
            .try_into()
            .unwrap();
        let (local_commited_idx_tx, local_commited_idx_rx) =
            tokio::sync::watch::channel(latest_idx);
        Ok(Self {
            local_commited_idx_tx,
            local_commited_idx_rx,
            kv_store,
        })
    }
}

fn arc_bytes_to_i64(bytes: Arc<[u8]>) -> i64 {
    if bytes.len() != 8 {
        panic!("value is not a i64: byte len {len} != 8", len = bytes.len());
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes);
    i64::from_le_bytes(buf)
}

#[async_trait]
impl LogStore for KvStoreLog {
    async fn latest_idx(&self) -> Res<u64> {
        Ok(self
            .kv_store
            .get(Self::LATEST_ID_KEY)
            .await?
            .map(arc_bytes_to_i64)
            .unwrap_or_default()
            .try_into()
            .unwrap())
    }

    async fn append(&self, entry: &[u8]) -> Res<u64> {
        // Use atomic increment to get the next log entry ID
        let idx: u64 = self
            .kv_store
            .increment(Self::LATEST_ID_KEY, 1)
            .await?
            .try_into()
            .unwrap();

        let old = self
            .kv_store
            .set(idx.to_le_bytes().into(), entry.into())
            .await?;
        assert!(old.is_none(), "fishy");
        self.local_commited_idx_tx
            .send(idx)
            // SAFE: self holds a reciever
            .unwrap();
        Ok(idx)
    }

    // FIXME: this has a bug if there are multiple KvStoreLogs using
    // the same backing KvStore. If a writer stalls between increment
    // and commit, `tail`s from other instances might observe it as
    // a crash hole.
    // - Use a CAS to fix it
    fn tail(&'_ self, offset: u64) -> BoxStream<'_, Res<TailLogEntry>> {
        futures::stream::unfold(offset, |offset| {
            let kv_store = Arc::clone(&self.kv_store);
            let mut latest_id_rx = self.local_commited_idx_rx.clone();
            async move {
                let key = offset.to_le_bytes();
                let mut last_seen_id = None;
                loop {
                    if latest_id_rx.has_changed().is_err() {
                        // this means the KvStoreLog has been dropped
                        return None;
                    }
                    match kv_store.get(&key).await {
                        // keep going when there's a value under that offset
                        Ok(Some(value)) => {
                            return Some((
                                Ok(TailLogEntry {
                                    idx: offset,
                                    val: Some(value),
                                }),
                                offset + 1,
                            ));
                        }
                        // error, we just give out an error. the can try again
                        Err(err) => return Some((Err(err), offset)),
                        // no value under offset so we wait until a new value is added
                        Ok(None) => {
                            if let Some(last_seen_id) = last_seen_id {
                                // we've already seen a None once
                                // if we're here
                                // this must be a crash hole, let's skip
                                if last_seen_id > offset {
                                    return Some((
                                        Ok(TailLogEntry {
                                            idx: offset,
                                            val: None,
                                        }),
                                        offset + 1,
                                    ));
                                }
                            }
                            let latest_id = *latest_id_rx.borrow_and_update();
                            last_seen_id = Some(latest_id);
                            if offset >= latest_id {
                                // we're up to date, wait for a new value
                                if let Err(_err) = latest_id_rx.changed().await {
                                    // KvStoreLog is dropped
                                    return None;
                                }
                            } else {
                                // we're still behind somehow
                                continue;
                            }
                        }
                    }
                }
            }
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::kvstore::CasGuard;
    use futures::FutureExt;
    use futures::channel::oneshot;
    use std::sync::Mutex;

    struct GatedStore {
        inner: Arc<dyn KvStore + Send + Sync>,
        release_first_write: Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl GatedStore {
        fn new() -> (Arc<Self>, oneshot::Sender<()>) {
            let (release, released) = oneshot::channel();
            let inner = Arc::new(Arc::new(DHashMap::<Arc<[u8]>, Arc<[u8]>>::default()));
            (
                Arc::new(Self {
                    inner,
                    release_first_write: Mutex::new(Some(released)),
                }),
                release,
            )
        }
    }

    #[async_trait]
    impl KvStore for GatedStore {
        async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
            self.inner.get(key).await
        }

        async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
            if key.as_ref() == 1u64.to_le_bytes() {
                let released = self.release_first_write.lock().unwrap().take().unwrap();
                released.await?;
            }
            self.inner.set(key, value).await
        }

        async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
            self.inner.del(key).await
        }

        async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
            self.inner.increment(key, delta).await
        }

        async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
            self.inner.new_cas(key).await
        }
    }

    #[test]
    fn legacy_split_append_skips_a_late_entry_and_regresses_notification() -> Res<()> {
        futures::executor::block_on(async {
            let (store, release) = GatedStore::new();
            let log = KvStoreLog::new(Arc::clone(&store) as _).await?;
            let mut slow = Box::pin(log.append(b"first"));
            assert!(futures::poll!(&mut slow).is_pending());
            assert_eq!(log.latest_idx().await?, 1);
            assert_eq!(store.inner.get(&1u64.to_le_bytes()).await?, None);

            assert_eq!(log.append(b"second").await?, 2);
            assert_eq!(*log.local_commited_idx_rx.borrow(), 2);
            let mut tail = log.tail(1);
            let first = tail.next().now_or_never().unwrap().unwrap()?;
            let second = tail.next().now_or_never().unwrap().unwrap()?;
            assert_eq!((first.idx, first.val), (1, None));
            assert_eq!(
                (second.idx, second.val.as_deref()),
                (2, Some(&b"second"[..]))
            );

            release.send(()).unwrap();
            assert_eq!(slow.await?, 1);
            assert_eq!(*log.local_commited_idx_rx.borrow(), 1);
            assert_eq!(log.latest_idx().await?, 2);
            assert!(tail.next().now_or_never().is_none());
            let recovered = log.tail(1).next().now_or_never().unwrap().unwrap()?;
            assert_eq!(
                (recovered.idx, recovered.val.as_deref()),
                (1, Some(&b"first"[..]))
            );
            Ok(())
        })
    }

    #[test]
    fn legacy_interrupted_append_reopens_with_an_unpublished_frontier() -> Res<()> {
        futures::executor::block_on(async {
            let (store, release) = GatedStore::new();
            {
                let log = KvStoreLog::new(Arc::clone(&store) as _).await?;
                let mut interrupted = Box::pin(log.append(b"never published"));
                assert!(futures::poll!(&mut interrupted).is_pending());
                assert_eq!(log.latest_idx().await?, 1);
            }
            assert!(release.send(()).is_err());
            let reopened = KvStoreLog::new(Arc::clone(&store.inner)).await?;
            assert_eq!(reopened.latest_idx().await?, 1);
            assert_eq!(store.inner.get(&1u64.to_le_bytes()).await?, None);
            let mut tail = reopened.tail(1);
            assert!(tail.next().now_or_never().is_none());
            assert_eq!(reopened.append(b"after restart").await?, 2);
            let hole = tail.next().now_or_never().unwrap().unwrap()?;
            let second = tail.next().now_or_never().unwrap().unwrap()?;
            assert_eq!((hole.idx, hole.val), (1, None));
            assert_eq!(
                (second.idx, second.val.as_deref()),
                (2, Some(&b"after restart"[..]))
            );
            Ok(())
        })
    }

    #[test]
    fn legacy_reader_handle_is_not_notified_by_another_writer_handle() -> Res<()> {
        futures::executor::block_on(async {
            let store: Arc<dyn KvStore + Send + Sync> =
                Arc::new(Arc::new(DHashMap::<Arc<[u8]>, Arc<[u8]>>::default()));
            let reader = KvStoreLog::new(Arc::clone(&store)).await?;
            let writer = KvStoreLog::new(store).await?;
            let mut tail = reader.tail(1);
            assert!(tail.next().now_or_never().is_none());
            assert_eq!(writer.append(b"committed").await?, 1);
            assert_eq!(reader.latest_idx().await?, 1);
            assert!(tail.next().now_or_never().is_none());
            let fresh = reader.tail(1).next().now_or_never().unwrap().unwrap()?;
            assert_eq!(
                (fresh.idx, fresh.val.as_deref()),
                (1, Some(&b"committed"[..]))
            );
            Ok(())
        })
    }
}
