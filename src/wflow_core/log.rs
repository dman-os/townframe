use crate::interlude::*;

use futures::stream::BoxStream;

pub struct TailLogEntry {
    pub idx: u64,
    /// None when there's a hole at that
    /// index due to crashes
    pub val: Option<Arc<[u8]>>,
}

#[async_trait]
/// Entries are one-based. Zero denotes an empty prefix, not an entry.
/// `tail(0)` starts at entry one; other offsets are inclusive. Historical
/// reserved holes are returned explicitly and count toward the journal prefix.
pub trait LogStore: Send + Sync {
    async fn append(&self, entry: &[u8]) -> Res<u64>;
    fn tail(&'_ self, offset: u64) -> BoxStream<'_, Res<TailLogEntry>>;
    async fn latest_idx(&self) -> Res<u64>;
}
