use crate::interlude::*;

use futures::stream::BoxStream;

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, entry: &[u8]) -> Res<u64>;
    async fn tail(&self, offset: u64) -> BoxStream<Res<(u64, Arc<[u8]>)>>;
    async fn latest_id(&self) -> Res<u64>;
}
