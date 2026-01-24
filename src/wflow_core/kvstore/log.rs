use crate::interlude::*;

use futures::{stream::BoxStream, StreamExt};

use crate::kvstore::KvStore;
use crate::log::{LogStore, TailLogEntry};

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
                            ))
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
