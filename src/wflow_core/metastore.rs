use crate::interlude::*;

pub use crate::gen::metastore::*;

// Contains information about what wflows exist
#[async_trait]
pub trait MetdataStore: Send + Sync {
    async fn get_wflow(&self, key: &str) -> Res<Option<WflowMeta>>;
    async fn set_wflow(&self, key: &str, meta: &WflowMeta) -> Res<Option<WflowMeta>>;
    async fn get_partitions(&self) -> Res<PartitionsMeta>;
    async fn set_partitions(&self, meta: PartitionsMeta) -> Res<()>;
}
