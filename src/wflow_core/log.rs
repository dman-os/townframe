use crate::interlude::*;

use futures::stream::BoxStream;

pub struct TailLogEntry {
    pub idx: u64,
    /// None when there's a hole at that
    /// index due to crashes
    pub val: Option<Arc<[u8]>>,
}

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, entry: &[u8]) -> Res<u64>;
    async fn tail(&self, offset: u64) -> BoxStream<Res<TailLogEntry>>;
    async fn latest_idx(&self) -> Res<u64>;
}
