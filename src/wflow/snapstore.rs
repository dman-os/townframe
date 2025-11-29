use crate::interlude::*;

use crate::kvstore::{CasError, KvStore};
use wflow_core::snapstore::PartitionSnapshot;

/// Snapshot with metadata stored together
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotWithMetadata {
    entry_id: u64,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    timestamp: OffsetDateTime,
    snapshot: PartitionSnapshot,
}

/// Implementation of SnapStore backed by a KvStore.
pub struct AtomicKvSnapStore {
    kv_store: Arc<dyn KvStore + Send + Sync>,
}

impl AtomicKvSnapStore {
    /// Create a new snapstore from a trait object directly
    pub fn new(kv_store: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self { kv_store }
    }

    fn snapshot_key(partition_id: u64) -> Vec<u8> {
        format!("__snapshot_partition_{}", partition_id).into_bytes()
    }
}

#[async_trait]
impl wflow_core::snapstore::SnapStore for AtomicKvSnapStore {
    async fn save_snapshot(
        &self,
        partition_id: u64,
        entry_id: u64,
        snapshot: &PartitionSnapshot,
    ) -> Res<()> {
        let snapshot_key = Self::snapshot_key(partition_id);

        // Use CAS to prevent ABA problem - retry if the snapshot was updated
        const MAX_CAS_RETRIES: u32 = 100;
        let mut cas = self.kv_store.new_cas(&snapshot_key).await?;
        for _attempt in 0..MAX_CAS_RETRIES {
            // Check current snapshot
            let current = cas.current();
            if let Some(current_bytes) = current {
                if let Ok(current_metadata) =
                    serde_json::from_slice::<SnapshotWithMetadata>(&current_bytes)
                {
                    // Only update if the new entry_id is greater
                    if current_metadata.entry_id >= entry_id {
                        // Existing snapshot is newer or same, don't override
                        return Ok(());
                    }
                }
            }

            // Create combined snapshot with metadata
            let snapshot_with_metadata = SnapshotWithMetadata {
                entry_id,
                timestamp: OffsetDateTime::now_utc(),
                snapshot: snapshot.clone(),
            };

            // Serialize and attempt CAS swap
            let new_bytes: Arc<[u8]> = serde_json::to_vec(&snapshot_with_metadata)
                .expect(ERROR_JSON)
                .into();

            match cas.swap(new_bytes).await? {
                Ok(()) => {
                    // Successfully saved
                    return Ok(());
                }
                Err(CasError::CasFailed(new_guard)) => {
                    // Snapshot was modified, retry with new guard
                    cas = new_guard;
                    continue;
                }
                Err(CasError::StoreError(err)) => {
                    return Err(err);
                }
            }
        }
        Err(ferr!(
            "failed to save snapshot after {MAX_CAS_RETRIES} CAS retries: concurrent modifications"
        ))
    }

    async fn load_latest_snapshot(
        &self,
        partition_id: u64,
    ) -> Res<Option<(u64, PartitionSnapshot)>> {
        let snapshot_key = Self::snapshot_key(partition_id);
        let snapshot_bytes = match self.kv_store.get(&snapshot_key).await? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let snapshot_with_metadata: SnapshotWithMetadata =
            serde_json::from_slice(&snapshot_bytes).wrap_err("failed to deserialize snapshot")?;

        Ok(Some((
            snapshot_with_metadata.entry_id,
            snapshot_with_metadata.snapshot,
        )))
    }
}
