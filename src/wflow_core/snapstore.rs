use crate::interlude::*;

use crate::partition::effects::{EffectId, PartitionEffect};
use crate::partition::state::PartitionJobsState;
use std::collections::HashMap;

/// Snapshot data including both jobs state and effects state
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionSnapshot {
    pub jobs: PartitionJobsState,
    #[serde(with = "effect_map_serde")]
    pub effects: HashMap<EffectId, PartitionEffect>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionSnapshotRef<'a, 'b> {
    pub jobs: &'a PartitionJobsState,
    #[serde(with = "effect_map_serde")]
    pub effects: &'b HashMap<EffectId, PartitionEffect>,
}

mod effect_map_serde {
    use super::*;
    use serde::{ser::SerializeMap, Deserializer, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S>(
        map: &HashMap<EffectId, PartitionEffect>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map_serializer = serializer.serialize_map(Some(map.len()))?;
        for (key_item, val) in map {
            let key = format!("{}_{}", key_item.entry_id, key_item.effect_idx);
            map_serializer.serialize_entry(&key, val)?;
        }
        map_serializer.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<EffectId, PartitionEffect>, D::Error>
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
                        EffectId {
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
    type Snapshot;

    fn prepare_snapshot(
        &self,
        partition_id: u64,
        entry_id: u64,
        snapshot: PartitionSnapshotRef,
    ) -> Res<Self::Snapshot>;

    /// Save a snapshot of the partition state.
    ///
    /// The snapshot should include the entry ID up to which the state is valid,
    /// allowing the partition to resume from that point.
    ///
    /// This method should use CAS to prevent overwriting a newer snapshot (ABA problem).
    async fn save_snapshot(
        &self,
        partition_id: u64,
        entry_id: u64,
        snapshot: Self::Snapshot,
    ) -> Res<()>;

    /// Load the latest snapshot for the given partition.
    ///
    /// Returns the entry ID and snapshot if a snapshot exists, or None if no snapshot is available.
    async fn load_latest_snapshot(
        &self,
        partition_id: u64,
    ) -> Res<Option<(u64, PartitionSnapshot)>>;
}
