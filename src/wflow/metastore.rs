use crate::interlude::*;

use crate::kvstore::{CasError, KvStore};
use wflow_core::metastore::{MetdataStore, PartitionsMeta, WflowMeta};

pub struct KvStoreMetadtaStore {
    kv_store: Arc<dyn KvStore + Send + Sync>,
}

impl KvStoreMetadtaStore {
    const PARTITION_STORE_KEY: &[u8] = b"_____partition-store";

    pub async fn new(
        kv_store: Arc<dyn KvStore + Send + Sync>,
        default_partitions: PartitionsMeta,
    ) -> Res<Self> {
        let this = Self { kv_store };
        // Use CAS to atomically initialize partitions if they don't exist
        let cas = this.kv_store.new_cas(Self::PARTITION_STORE_KEY).await?;
        if cas.current().is_none() {
            let partitions_bytes = serde_json::to_vec(&PartitionsMeta {
                ..default_partitions
            })
            .expect(ERROR_JSON)
            .into();
            match cas.swap(partitions_bytes).await? {
                Ok(()) => {}
                Err(CasError::CasFailed(_)) => {
                    // Another thread initialized it, that's fine
                }
                Err(CasError::StoreError(err)) => return Err(err),
            }
        }
        Ok(this)
    }
}

#[async_trait]
impl MetdataStore for KvStoreMetadtaStore {
    async fn get_wflow(&self, key: &str) -> Res<Option<WflowMeta>> {
        let meta = self.kv_store.get(key.as_bytes()).await?;
        let meta = meta.map(|raw| serde_json::from_slice::<WflowMeta>(&raw).expect(ERROR_JSON));
        Ok(meta)
    }
    async fn set_wflow(&self, key: &str, meta: &WflowMeta) -> Res<Option<WflowMeta>> {
        if key.as_bytes() == Self::PARTITION_STORE_KEY {
            panic!("don't do that");
        }
        // Use CAS to atomically set and get old value
        // This ensures we abort if another process is trying to initialize the same key
        let cas = self.kv_store.new_cas(key.as_bytes()).await?;
        let old_value = cas.current();
        let new_bytes: Arc<[u8]> = serde_json::to_vec(&meta).expect(ERROR_JSON).into();

        match cas.swap(new_bytes).await? {
            Ok(()) => {
                let old = old_value
                    .map(|raw| serde_json::from_slice::<WflowMeta>(&raw).expect(ERROR_JSON));
                Ok(old)
            }
            Err(CasError::CasFailed(_)) => {
                // Value was modified by another process, abort
                Err(ferr!("concurrent modification detected: another process modified the workflow metadata for key '{}'", key))
            }
            Err(CasError::StoreError(err)) => Err(err),
        }
    }
    async fn get_partitions(&self) -> Res<PartitionsMeta> {
        let Some(meta) = self.kv_store.get(Self::PARTITION_STORE_KEY).await? else {
            panic!("init was not right");
        };
        let meta = serde_json::from_slice::<PartitionsMeta>(&meta).expect(ERROR_JSON);
        Ok(meta)
    }
    async fn set_partitions(&self, meta: PartitionsMeta) -> Res<()> {
        // Set is atomic, no need for CAS
        let new_bytes: Arc<[u8]> = serde_json::to_vec(&meta).expect(ERROR_JSON).into();
        self.kv_store
            .set(Self::PARTITION_STORE_KEY.into(), new_bytes)
            .await?;
        Ok(())
    }
}
