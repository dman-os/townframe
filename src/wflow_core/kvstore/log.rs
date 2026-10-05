use crate::interlude::*;

use futures::{StreamExt, stream::BoxStream};

use crate::kvstore::KvStore;
use crate::log::{LogStore, TailLogEntry};

pub struct KvStoreLog {
    // we use these to communicate the latest
    // written entry indices by this instance
    local_commited_idx_rx: tokio::sync::watch::Receiver<u64>,
    local_commited_idx_tx: tokio::sync::watch::Sender<u64>,
    kv_store: Arc<dyn KvStore + Send + Sync>,
    // The previous owner has stopped before reopening this journal. Only this
    // closed prefix proves that missing reservations cannot still be committed.
    pre_open_horizon: u64,
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
            pre_open_horizon: latest_idx,
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
        futures::stream::unfold(offset.max(1), |offset| {
            let kv_store = Arc::clone(&self.kv_store);
            let mut latest_id_rx = self.local_commited_idx_rx.clone();
            let pre_open_horizon = self.pre_open_horizon;
            async move {
                let key = offset.to_le_bytes();
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
                            if offset <= pre_open_horizon {
                                return Some((
                                    Ok(TailLogEntry {
                                        idx: offset,
                                        val: None,
                                    }),
                                    offset + 1,
                                ));
                            }
                            // A newer reservation may belong to a live append.
                            // Even a later committed entry does not prove a hole.
                            if latest_id_rx.changed().await.is_err() {
                                return None;
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

    fn storage() -> Arc<dyn KvStore + Send + Sync> {
        let store = Arc::new(DHashMap::<Arc<[u8]>, Arc<[u8]>>::default());
        Arc::new(store)
    }

    #[tokio::test]
    async fn final_historical_reservation_drains() -> Res<()> {
        let store = storage();
        // This is the durable cut between append's increment and value write.
        assert_eq!(store.increment(KvStoreLog::LATEST_ID_KEY, 1).await?, 1);
        let reopened = KvStoreLog::new(store).await?;
        let mut tail = reopened.tail(1);
        let hole = tail.next().await.unwrap()?;
        assert_eq!(hole.idx, 1);
        assert!(hole.val.is_none());
        assert_eq!(reopened.append(b"after-reopen").await?, 2);
        let entry = tail.next().await.unwrap()?;
        assert_eq!(entry.idx, 2);
        assert_eq!(entry.val.as_deref(), Some(b"after-reopen".as_slice()));
        Ok(())
    }

    #[tokio::test]
    async fn live_reservation_is_not_skipped_by_later_commit() -> Res<()> {
        let store = storage();
        let log = KvStoreLog::new(Arc::clone(&store)).await?;
        assert_eq!(store.increment(KvStoreLog::LATEST_ID_KEY, 1).await?, 1);
        assert_eq!(log.append(b"later").await?, 2);
        let mut tail = log.tail(1);
        let next = tail.next();
        futures::pin_mut!(next);
        assert!(futures::poll!(&mut next).is_pending());
        assert!(
            store
                .set(1u64.to_le_bytes().into(), b"earlier".as_slice().into())
                .await?
                .is_none()
        );
        // Notify exactly as append does after committing the reserved value.
        log.local_commited_idx_tx.send(1).unwrap();
        let entry = next.await.unwrap()?;
        assert_eq!(entry.idx, 1);
        assert_eq!(entry.val.as_deref(), Some(b"earlier".as_slice()));
        let entry = tail.next().await.unwrap()?;
        assert_eq!(entry.idx, 2);
        assert_eq!(entry.val.as_deref(), Some(b"later".as_slice()));
        Ok(())
    }
}
