use crate::interlude::*;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use tokio::sync::RwLock;

use wflow_core::partition::effects;
use wflow_core::partition::state::PartitionJobsState;

/// Counts of active and archived jobs, used for change notifications
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobCounts {
    pub active: usize,
    pub archive: usize,
}

#[derive(Debug)]
pub struct PartitionWorkingState {
    // FIXME: this probably should go into the metastore
    pub last_applied_entry_id: AtomicU64,
    jobs: RwLock<PartitionJobsState>,
    effects: RwLock<HashMap<effects::EffectId, effects::PartitionEffect>>,
    // Change notification channel - sends JobCounts whenever counts change
    change_tx: tokio::sync::watch::Sender<JobCounts>,
    change_rx: tokio::sync::watch::Receiver<JobCounts>,
}

impl PartitionWorkingState {
    /// Create a new PartitionWorkingState with change listeners
    pub fn new(
        initial_entry_id: u64,
        initial_jobs: PartitionJobsState,
        initial_effects: HashMap<effects::EffectId, effects::PartitionEffect>,
    ) -> Self {
        let initial_counts = JobCounts {
            active: initial_jobs.active.len(),
            archive: initial_jobs.archive.len(),
        };
        let (change_tx, change_rx) = tokio::sync::watch::channel(initial_counts);
        Self {
            last_applied_entry_id: AtomicU64::new(initial_entry_id),
            jobs: RwLock::new(initial_jobs),
            effects: RwLock::new(initial_effects),
            change_tx,
            change_rx,
        }
    }

    /// Get a read lock on jobs state
    pub async fn read_jobs(&self) -> tokio::sync::RwLockReadGuard<'_, PartitionJobsState> {
        self.jobs.read().await
    }

    /// Get a write lock on jobs state (no automatic notification - caller must notify)
    pub async fn write_jobs(&self) -> tokio::sync::RwLockWriteGuard<'_, PartitionJobsState> {
        self.jobs.write().await
    }

    /// Get a read lock on effects state
    pub async fn read_effects(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, HashMap<effects::EffectId, effects::PartitionEffect>>
    {
        self.effects.read().await
    }

    /// Get a write lock on effects state (no automatic notification - caller must notify)
    pub async fn write_effects(
        &self,
    ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<effects::EffectId, effects::PartitionEffect>>
    {
        self.effects.write().await
    }

    /// Get current job counts without holding a lock
    pub async fn get_job_counts(&self) -> JobCounts {
        let jobs = self.jobs.read().await;
        JobCounts {
            active: jobs.active.len(),
            archive: jobs.archive.len(),
        }
    }

    /// Notify listeners of count changes (call this after updating state)
    pub fn notify_counts_changed(&self, counts: JobCounts) {
        // Ignore errors - receivers may have been dropped
        let _ = self.change_tx.send(counts);
    }

    /// Get a receiver for count change notifications
    pub fn change_receiver(&self) -> tokio::sync::watch::Receiver<JobCounts> {
        self.change_rx.clone()
    }
}

impl Default for PartitionWorkingState {
    fn default() -> Self {
        Self::new(0, PartitionJobsState::default(), HashMap::new())
    }
}
