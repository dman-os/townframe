use crate::interlude::*;

use std::collections::HashMap;
use tokio::sync::Mutex;

use wflow_core::partition::effects;
use wflow_core::partition::state::PartitionJobsState;

#[derive(Debug)]
pub struct PartitionWorkingState {
    // FIXME: this probably should go into the metastore
    pub last_applied_entry_id: std::sync::atomic::AtomicU64,
    pub jobs: Mutex<PartitionJobsState>,
    pub effects: Mutex<HashMap<effects::EffectId, effects::PartitionEffect>>,
}

impl Default for PartitionWorkingState {
    fn default() -> Self {
        Self {
            last_applied_entry_id: std::sync::atomic::AtomicU64::new(0),
            jobs: Mutex::new(PartitionJobsState::default()),
            effects: Mutex::new(HashMap::new()),
        }
    }
}
