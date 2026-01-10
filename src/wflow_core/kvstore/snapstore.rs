use crate::interlude::*;

use crate::kvstore::{CasError, KvStore};
use crate::snapstore::{PartitionSnapshot, PartitionSnapshotRef};

/// Snapshot with metadata stored together
#[derive(Debug, Deserialize)]
struct SnapshotWithMetadata {
    entry_id: u64,
    #[allow(dead_code)]
    timestamp: Timestamp,
    snapshot: PartitionSnapshot,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotMetadata {
    entry_id: u64,
    timestamp: Timestamp,
    blob_id: Uuid,
}

#[derive(Debug, Serialize)]
struct SnapshotWithMetadataRef<'a, 'b> {
    entry_id: u64,
    timestamp: Timestamp,
    snapshot: PartitionSnapshotRef<'a, 'b>,
}

/// Implementation of SnapStore backed by a KvStore.
pub struct KvSnapStore {
    kv_store: Arc<dyn KvStore + Send + Sync>,
}

impl KvSnapStore {
    /// Create a new snapstore from a trait object directly
    pub fn new(kv_store: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self { kv_store }
    }

    fn blob_key(blob_id: &Uuid) -> Vec<u8> {
        format!("__snapshot_blob_{}", blob_id).into_bytes()
    }

    fn meta_key(partition_id: u64) -> Vec<u8> {
        format!("__snapshot_meta_partition_{}", partition_id).into_bytes()
    }
}

#[async_trait]
impl crate::snapstore::SnapStore for KvSnapStore {
    type Snapshot = Arc<[u8]>;

    #[tracing::instrument(skip(self, snapshot))]
    fn prepare_snapshot(
        &self,
        _partition_id: u64,
        entry_id: u64,
        snapshot: PartitionSnapshotRef,
    ) -> Res<Self::Snapshot> {
        let timestamp = Timestamp::now();

        // Create combined snapshot with metadata for the blob
        let snapshot_with_metadata = SnapshotWithMetadataRef {
            entry_id,
            timestamp,
            snapshot,
        };

        let blob = serde_json::to_vec(&snapshot_with_metadata)
            .expect(ERROR_JSON)
            .into();

        Ok(blob)
    }

    #[tracing::instrument(skip(self, snapshot))]
    async fn save_snapshot(
        &self,
        partition_id: u64,
        entry_id: u64,
        snapshot: Self::Snapshot,
    ) -> Res<()> {
        let meta_key = Self::meta_key(partition_id);

        // Use CAS on the metadata key to prevent ABA problem
        const MAX_CAS_RETRIES: u32 = 100;
        let mut cas = self.kv_store.new_cas(&meta_key).await?;

        // 1. Initial check: is there already a newer snapshot?
        if let Some(current_bytes) = cas.current() {
            if let Ok(meta) = serde_json::from_slice::<SnapshotMetadata>(&current_bytes) {
                if meta.entry_id >= entry_id {
                    return Ok(());
                }
            }
        }

        // 2. Generate new blob ID and write it ONCE
        let new_blob_id = Uuid::new_v4();
        let new_blob_key = Self::blob_key(&new_blob_id);
        self.kv_store
            .set(new_blob_key.clone().into(), snapshot)
            .await?;

        // 3. Retry loop for the metadata CAS
        for _attempt in 0..MAX_CAS_RETRIES {
            let current_bytes = cas.current();
            let current_meta = if let Some(bytes) = current_bytes {
                if let Ok(meta) = serde_json::from_slice::<SnapshotMetadata>(&bytes) {
                    if meta.entry_id >= entry_id {
                        // Someone else committed a newer version while we were writing our blob.
                        // Clean up our blob and exit.
                        self.kv_store.del(&new_blob_key).await?;
                        return Ok(());
                    }
                    Some(meta)
                } else {
                    None
                }
            } else {
                None
            };

            // Prepare new metadata pointing to our already-written blob
            let new_meta = SnapshotMetadata {
                entry_id,
                timestamp: Timestamp::now(),
                blob_id: new_blob_id,
            };
            let new_meta_bytes: Arc<[u8]> = serde_json::to_vec(&new_meta).expect(ERROR_JSON).into();

            // Then update the metadata key using CAS
            match cas.swap(new_meta_bytes).await? {
                Ok(()) => {
                    // Successfully committed our metadata.
                    // Now we can safely delete the old blob if there was one.
                    if let Some(old_meta) = current_meta {
                        let old_blob_key = Self::blob_key(&old_meta.blob_id);
                        self.kv_store.del(&old_blob_key).await?;
                    }
                    return Ok(());
                }
                Err(CasError::CasFailed(new_guard)) => {
                    // Metadata was modified by someone else, retry the check and swap.
                    cas = new_guard;
                    continue;
                }
                Err(CasError::StoreError(err)) => {
                    // Fatal store error, clean up our blob.
                    let _ = self.kv_store.del(&new_blob_key).await;
                    return Err(err);
                }
            }
        }

        // Exhausted retries
        let _ = self.kv_store.del(&new_blob_key).await;
        Err(ferr!(
            "failed to save snapshot after {MAX_CAS_RETRIES} CAS retries: concurrent modifications"
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn load_latest_snapshot(
        &self,
        partition_id: u64,
    ) -> Res<Option<(u64, PartitionSnapshot)>> {
        let meta_key = Self::meta_key(partition_id);
        let meta_bytes = match self.kv_store.get(&meta_key).await? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let meta: SnapshotMetadata = serde_json::from_slice(&meta_bytes)
            .wrap_err("failed to deserialize snapshot metadata")?;

        let blob_key = Self::blob_key(&meta.blob_id);
        let snapshot_bytes = match self.kv_store.get(&blob_key).await? {
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
