use crate::interlude::*;

use crate::plugin::binds_metastore::townframe::wflow::metastore as wit;
pub use wit::*;

// Contains information about what wflows exist
#[async_trait]
pub trait MetdataStore: Send + Sync {
    async fn get_wflow(&self, key: &str) -> Res<Option<WflowMeta>>;
    async fn set_wflow(&self, key: &str, meta: &WflowMeta) -> Res<Option<WflowMeta>>;
    async fn get_partitions(&self) -> Res<PartitionsMeta>;
    async fn set_partitions(&self, meta: PartitionsMeta) -> Res<()>;
}

pub struct KvStoreMetadtaStore {
    kv_store: Arc<dyn crate::KvStore + Send + Sync>,
}

impl KvStoreMetadtaStore {
    const PARTITION_STORE_KEY: &[u8] = b"_____partition-store";

    pub async fn new(
        kv_store: Arc<dyn crate::KvStore + Send + Sync>,
        default_partitions: PartitionsMeta,
    ) -> Res<Self> {
        let this = Self { kv_store };
        if let None = this.kv_store.get(Self::PARTITION_STORE_KEY).await? {
            this.set_partitions(PartitionsMeta {
                ..default_partitions
            })
            .await?;
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
        let old = self
            .kv_store
            .set(
                key.as_bytes().into(),
                serde_json::to_vec(&meta).expect(ERROR_JSON).into(),
            )
            .await?;
        let old = old.map(|raw| serde_json::from_slice::<WflowMeta>(&raw).expect(ERROR_JSON));
        Ok(old)
    }
    async fn get_partitions(&self) -> Res<PartitionsMeta> {
        let Some(meta) = self.kv_store.get(Self::PARTITION_STORE_KEY).await? else {
            panic!("init was not right");
        };
        let meta = serde_json::from_slice::<PartitionsMeta>(&meta).expect(ERROR_JSON);
        Ok(meta)
    }
    async fn set_partitions(&self, meta: PartitionsMeta) -> Res<()> {
        let _old = self
            .kv_store
            .set(
                Self::PARTITION_STORE_KEY.into(),
                serde_json::to_vec(&meta).expect(ERROR_JSON).into(),
            )
            .await?;
        Ok(())
    }
}
