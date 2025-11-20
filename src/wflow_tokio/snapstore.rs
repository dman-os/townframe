use crate::interlude::*;

use crate::{CasError, KvStore};
use std::collections::HashMap;
use wflow_core::partition::effects::PartitionEffect;
use wflow_core::partition::state::PartitionJobsState;

/// Snapshot data including both jobs state and effects state
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub jobs: PartitionJobsState,
    #[serde(with = "effect_map_serde")]
    pub effects: HashMap<wflow_core::partition::effects::EffectId, PartitionEffect>,
}

mod effect_map_serde {
    use super::*;
    use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S>(
        map: &HashMap<wflow_core::partition::effects::EffectId, PartitionEffect>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map_serializer = serializer.serialize_map(Some(map.len()))?;
        for (k, v) in map {
            let key = format!("{}_{}", k.entry_id, k.effect_idx);
            map_serializer.serialize_entry(&key, v)?;
        }
        map_serializer.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<wflow_core::partition::effects::EffectId, PartitionEffect>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: HashMap<String, PartitionEffect> = HashMap::deserialize(deserializer)?;
        let mut result = HashMap::new();
        for (key, value) in map {
            let parts: Vec<&str> = key.split('_').collect();
            if parts.len() == 2 {
                if let (Ok(entry_id), Ok(effect_idx)) =
                    (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                {
                    result.insert(
                        wflow_core::partition::effects::EffectId {
                            entry_id,
                            effect_idx,
                        },
                        value,
                    );
                }
            }
        }
        Ok(result)
    }
}

/// A store for saving and loading partition state snapshots.
///
/// Snapshots allow the partition worker to recover from the last known good state
/// instead of replaying the entire log from the beginning.
#[async_trait]
pub trait SnapStore: Send + Sync {
    /// Save a snapshot of the partition state.
    ///
    /// The snapshot should include the entry ID up to which the state is valid,
    /// allowing the partition to resume from that point.
    async fn save_snapshot(
        &self,
        partition_id: u64,
        entry_id: u64,
        snapshot: &PartitionSnapshot,
    ) -> Res<()>;

    /// Load the latest snapshot for the given partition.
    ///
    /// Returns the entry ID and snapshot if a snapshot exists, or None if no snapshot is available.
    async fn load_latest_snapshot(
        &self,
        partition_id: u64,
    ) -> Res<Option<(u64, PartitionSnapshot)>>;
}

/// Implementation of SnapStore backed by a KvStore.
pub struct AtomicKvSnapStore {
    kv_store: Arc<dyn KvStore + Send + Sync>,
}

impl AtomicKvSnapStore {
    pub fn new<S: KvStore + Send + Sync + 'static>(kv_store: Arc<S>) -> Self {
        Self {
            kv_store: kv_store as Arc<dyn KvStore + Send + Sync>,
        }
    }

    fn snapshot_key(partition_id: u64) -> Vec<u8> {
        format!("__snapshot_partition_{}", partition_id).into_bytes()
    }

    fn metadata_key(partition_id: u64) -> Vec<u8> {
        format!("__snapshot_meta_partition_{}", partition_id).into_bytes()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotMetadata {
    entry_id: u64,
    timestamp: OffsetDateTime,
}

#[async_trait]
impl SnapStore for AtomicKvSnapStore {
    async fn save_snapshot(
        &self,
        partition_id: u64,
        entry_id: u64,
        snapshot: &PartitionSnapshot,
    ) -> Res<()> {
        // Serialize the snapshot
        let snapshot_bytes: Arc<[u8]> = serde_json::to_vec(snapshot).expect(ERROR_JSON).into();

        // Use CAS to save the snapshot to avoid overriding a newer snapshot
        let snapshot_key = Self::snapshot_key(partition_id);
        let cas = self.kv_store.new_cas(&snapshot_key).await?;

        // Check if there's an existing snapshot with a higher entry_id
        if let Some(_existing_bytes) = cas.current() {
            // Load metadata to get the entry_id
            let meta_key = Self::metadata_key(partition_id);
            if let Some(metadata_bytes) = self.kv_store.get(&meta_key).await? {
                if let Ok(metadata) = serde_json::from_slice::<SnapshotMetadata>(&metadata_bytes) {
                    if metadata.entry_id >= entry_id {
                        // Existing snapshot is newer or same, don't override
                        return Ok(());
                    }
                }
            }
        }

        // Save the snapshot using CAS
        match cas.swap(snapshot_bytes).await? {
            Ok(()) => {
                // Save metadata with entry_id and timestamp
                let metadata = SnapshotMetadata {
                    entry_id,
                    timestamp: OffsetDateTime::now_utc(),
                };
                let metadata_bytes: Arc<[u8]> =
                    serde_json::to_vec(&metadata).expect(ERROR_JSON).into();

                let meta_key = Self::metadata_key(partition_id);
                self.kv_store.set(meta_key.into(), metadata_bytes).await?;
                Ok(())
            }
            Err(CasError::CasFailed(_)) => {
                // Another process saved a snapshot, that's fine - we'll try again next time
                Ok(())
            }
            Err(CasError::StoreError(err)) => Err(err),
        }
    }

    async fn load_latest_snapshot(
        &self,
        partition_id: u64,
    ) -> Res<Option<(u64, PartitionSnapshot)>> {
        // Load metadata first to get the entry_id
        let meta_key = Self::metadata_key(partition_id);
        let metadata_bytes = match self.kv_store.get(&meta_key).await? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let metadata: SnapshotMetadata = serde_json::from_slice(&metadata_bytes)
            .wrap_err("failed to deserialize snapshot metadata")?;

        // Load the snapshot
        let snapshot_key = Self::snapshot_key(partition_id);
        let snapshot_bytes = match self.kv_store.get(&snapshot_key).await? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let snapshot: PartitionSnapshot = serde_json::from_slice(&snapshot_bytes)
            .wrap_err("failed to serialize partition snapshot")?;

        Ok(Some((metadata.entry_id, snapshot)))
    }
}
