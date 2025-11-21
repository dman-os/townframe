use crate::interlude::*;

use std::collections::HashMap;
use tokio::sync::{watch, RwLock};

use wflow_core::partition::effects;
use wflow_core::partition::state::PartitionJobsState;

#[derive(Debug)]
pub struct PartitionWorkingState {
    // FIXME: this probably should go into the metastore
    pub last_applied_entry_id: std::sync::atomic::AtomicU64,
    jobs: RwLock<PartitionJobsState>,
    effects: RwLock<HashMap<effects::EffectId, effects::PartitionEffect>>,
    // Change notification channel - sends () whenever state is modified
    change_tx: watch::Sender<()>,
    change_rx: watch::Receiver<()>,
}

impl PartitionWorkingState {
    /// Create a new PartitionWorkingState with change listeners
    pub fn new(
        initial_entry_id: u64,
        initial_jobs: PartitionJobsState,
        initial_effects: HashMap<effects::EffectId, effects::PartitionEffect>,
    ) -> Self {
        let (change_tx, change_rx) = watch::channel(());
        Self {
            last_applied_entry_id: std::sync::atomic::AtomicU64::new(initial_entry_id),
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

    /// Get a write lock on jobs state and notify listeners
    pub async fn write_jobs(&self) -> PartitionWorkingStateWriteGuard<'_, PartitionJobsState> {
        let guard = self.jobs.write().await;
        PartitionWorkingStateWriteGuard {
            guard,
            change_tx: self.change_tx.clone(),
        }
    }

    /// Get a read lock on effects state
    pub async fn read_effects(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, HashMap<effects::EffectId, effects::PartitionEffect>>
    {
        self.effects.read().await
    }

    /// Get a write lock on effects state and notify listeners
    pub async fn write_effects(
        &self,
    ) -> PartitionWorkingStateWriteGuard<'_, HashMap<effects::EffectId, effects::PartitionEffect>>
    {
        let guard = self.effects.write().await;
        PartitionWorkingStateWriteGuard {
            guard,
            change_tx: self.change_tx.clone(),
        }
    }

    /// Get a receiver for change notifications
    pub fn change_receiver(&self) -> watch::Receiver<()> {
        self.change_rx.clone()
    }
}

/// A write guard that notifies change listeners when dropped
pub struct PartitionWorkingStateWriteGuard<'a, T> {
    guard: tokio::sync::RwLockWriteGuard<'a, T>,
    change_tx: watch::Sender<()>,
}

impl<T> std::ops::Deref for PartitionWorkingStateWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> std::ops::DerefMut for PartitionWorkingStateWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<T> Drop for PartitionWorkingStateWriteGuard<'_, T> {
    fn drop(&mut self) {
        // Notify listeners that state has changed
        // Ignore errors - receivers may have been dropped
        let _ = self.change_tx.send(());
    }
}

impl Default for PartitionWorkingState {
    fn default() -> Self {
        Self::new(0, PartitionJobsState::default(), HashMap::new())
    }
}
