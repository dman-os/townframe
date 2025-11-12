use crate::interlude::*;

use futures::{stream::BoxStream, StreamExt};

use crate::KvStore;

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, entry: &[u8]) -> Res<u64>;
    async fn tail(&self, offset: u64) -> BoxStream<Res<(u64, Arc<[u8]>)>>;
}

pub struct KvStoreLog {
    latest_id: Arc<std::sync::atomic::AtomicU64>,
    latest_id_rx: tokio::sync::watch::Receiver<u64>,
    latest_id_tx: tokio::sync::watch::Sender<u64>,
    kv_store: Arc<dyn KvStore + Send + Sync>,
}

impl KvStoreLog {
    pub fn new(kv_store: Arc<dyn KvStore + Send + Sync>, latest_id: u64) -> Self {
        let (latest_id_tx, latest_id_rx) = tokio::sync::watch::channel(latest_id);
        Self {
            latest_id: Arc::new(std::sync::atomic::AtomicU64::new(latest_id)),
            latest_id_tx,
            latest_id_rx,
            kv_store,
        }
    }
}

#[async_trait]
impl LogStore for KvStoreLog {
    async fn append(&self, entry: &[u8]) -> Res<u64> {
        let key = self
            .latest_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.kv_store
            .set(key.to_le_bytes().into(), entry.into())
            .await?;
        // SAFE: can't fail because self has reciever
        self.latest_id_tx.send(key).unwrap();
        Ok(key)
    }

    async fn tail(&self, offset: u64) -> BoxStream<Res<(u64, Arc<[u8]>)>> {
        futures::stream::unfold(offset, |offset| {
            let kv_store = self.kv_store.clone();
            let mut latest_id_rx = self.latest_id_rx.clone();
            async move {
                let key = offset.to_le_bytes();
                loop {
                    if latest_id_rx.has_changed().is_err() {
                        // this means the KvStoreLog has been dropped
                        return None;
                    }
                    match kv_store.get(&key).await {
                        // keep going when there's a value under that offset
                        Ok(Some(value)) => return Some((Ok((offset, value)), offset + 1)),
                        // error, we just give out an error. the can try again
                        Err(err) => return Some((Err(err), offset)),
                        // no value under offset so we wait until a new value is added
                        Ok(None) => {
                            let latest_id = latest_id_rx.borrow_and_update().clone();
                            if offset >= latest_id {
                                // we're up to date, wait for a new value
                                if let Err(_err) = latest_id_rx.changed().await {
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
