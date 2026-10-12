use std::collections::BTreeMap;

use thiserror::Error;
use tokio::sync::broadcast;

use crate::backend::{ApplicationBackend, Command};

use super::transaction::{MutationError, TransactionId, TransactionStatus};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionChange<K, V> {
    Upsert { key: K, value: V },
    Remove { key: K },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionEvent<K, V> {
    pub notification_revision: u64,
    pub snapshot: BTreeMap<K, V>,
    pub change: Option<CollectionChange<K, V>>,
    pub transaction: Option<(TransactionId, TransactionStatus)>,
}

/// A normalized local collection with a synchronized base and optimistic overlays.
///
/// Source cursors intentionally do not appear here. A source adapter owns cursor semantics,
/// handoff, deduplication, and ordering before applying a projection to this collection.
pub struct Collection<K, V> {
    base: BTreeMap<K, V>,
    pending: BTreeMap<TransactionId, PendingTransaction<K, V>>,
    statuses: BTreeMap<TransactionId, TransactionStatus>,
    notification_revision: u64,
    events: broadcast::Sender<CollectionEvent<K, V>>,
}

struct PendingTransaction<K, V> {
    changes: Vec<CollectionChange<K, V>>,
}

impl<K, V> Default for Collection<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Collection<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            base: BTreeMap::new(),
            pending: BTreeMap::new(),
            statuses: BTreeMap::new(),
            notification_revision: 0,
            events,
        }
    }

    pub fn from_base(base: impl IntoIterator<Item = (K, V)>) -> Self {
        let mut collection = Self::new();
        collection.base.extend(base);
        collection
    }

    pub fn notification_revision(&self) -> u64 {
        self.notification_revision
    }

    pub fn snapshot(&self) -> BTreeMap<K, V> {
        self.visible_snapshot()
    }

    pub fn get(&self, key: &K) -> Option<V> {
        // Point reads must not clone the whole projection. Latest optimistic intent wins.
        for pending in self.pending.values().rev() {
            for change in pending.changes.iter().rev() {
                match change {
                    CollectionChange::Upsert {
                        key: changed,
                        value,
                    } if changed == key => return Some(value.clone()),
                    CollectionChange::Remove { key: changed } if changed == key => return None,
                    CollectionChange::Upsert { .. } | CollectionChange::Remove { .. } => {}
                }
            }
        }
        self.base.get(key).cloned()
    }

    pub fn live_query(&self) -> LiveQuery<K, V> {
        LiveQuery {
            current: self.visible_snapshot(),
            updates: self.events.subscribe(),
        }
    }

    /// Applies a source-ordered delta. Ordering and deduplication belong to the source adapter.
    pub fn apply_authoritative(
        &mut self,
        change: CollectionChange<K, V>,
    ) -> Result<u64, CollectionError> {
        apply_change(&mut self.base, &change);
        Ok(self.emit(None, None))
    }

    pub fn apply_authoritative_batch(
        &mut self,
        changes: impl IntoIterator<Item = CollectionChange<K, V>>,
    ) -> Result<u64, CollectionError> {
        for change in changes {
            apply_change(&mut self.base, &change);
        }
        Ok(self.emit(None, None))
    }

    /// Replaces the source projection while retaining pending optimistic overlays.
    pub fn replace_authoritative(
        &mut self,
        rows: impl IntoIterator<Item = CollectionChange<K, V>>,
    ) -> Result<u64, CollectionError> {
        self.base.clear();
        for change in rows {
            apply_change(&mut self.base, &change);
        }
        Ok(self.emit(None, None))
    }

    pub fn apply_optimistic(
        &mut self,
        transaction: TransactionId,
        change: CollectionChange<K, V>,
    ) -> Result<TransactionStatus, CollectionError> {
        if self.pending.contains_key(&transaction) {
            return Err(CollectionError::DuplicateTransaction(transaction));
        }
        self.pending.insert(
            transaction,
            PendingTransaction {
                changes: vec![change.clone()],
            },
        );
        self.statuses
            .insert(transaction, TransactionStatus::LocallyApplied);
        self.emit(
            Some(change),
            Some((transaction, TransactionStatus::LocallyApplied)),
        );
        Ok(TransactionStatus::LocallyApplied)
    }

    /// Commits the optimistic changes into the local authoritative base after durable acceptance.
    pub fn accept(
        &mut self,
        transaction: TransactionId,
    ) -> Result<TransactionStatus, CollectionError> {
        let pending = self
            .pending
            .remove(&transaction)
            .ok_or(CollectionError::UnknownTransaction(transaction))?;
        for change in pending.changes {
            apply_change(&mut self.base, &change);
        }
        self.statuses
            .insert(transaction, TransactionStatus::Accepted);
        self.emit(None, Some((transaction, TransactionStatus::Accepted)));
        Ok(TransactionStatus::Accepted)
    }

    /// Submits an already-applied optimistic mutation and commits it at the successful response.
    pub async fn submit<B>(
        &mut self,
        backend: &B,
        transaction: TransactionId,
        command: Command,
    ) -> Result<TransactionStatus, MutationError>
    where
        B: ApplicationBackend + ?Sized,
    {
        let result = backend.submit(command.clone()).await;
        match result {
            Ok(receipt) if receipt.command != command.id => {
                self.rollback_after_submit_failure(transaction);
                Err(MutationError::CommandIdMismatch {
                    expected: command.id,
                    actual: receipt.command,
                })
            }
            Ok(receipt) if receipt.accepted => Ok(self
                .accept(transaction)
                .expect("submitted optimistic transaction disappeared")),
            Ok(_) => {
                self.rollback_after_submit_failure(transaction);
                Err(MutationError::Rejected)
            }
            Err(error) => {
                self.rollback_after_submit_failure(transaction);
                Err(MutationError::Backend(error))
            }
        }
    }

    pub fn rollback(
        &mut self,
        transaction: TransactionId,
    ) -> Result<TransactionStatus, CollectionError> {
        if self.pending.remove(&transaction).is_none() {
            return Err(CollectionError::UnknownTransaction(transaction));
        }
        self.statuses
            .insert(transaction, TransactionStatus::RolledBack);
        self.emit(None, Some((transaction, TransactionStatus::RolledBack)));
        Ok(TransactionStatus::RolledBack)
    }

    pub fn transaction_status(&self, transaction: TransactionId) -> Option<TransactionStatus> {
        self.statuses.get(&transaction).copied()
    }
    /// Releases a settled receipt after its consumer has incorporated the outcome.
    /// Pending transactions cannot be forgotten: their overlays still belong to this runtime.
    pub fn forget_completed(&mut self, transaction: TransactionId) -> Option<TransactionStatus> {
        assert!(
            !self.pending.contains_key(&transaction),
            "cannot forget a pending transaction"
        );
        self.statuses.remove(&transaction)
    }

    fn rollback_after_submit_failure(&mut self, transaction: TransactionId) {
        self.rollback(transaction)
            .expect("submitted optimistic transaction disappeared");
    }

    fn visible_snapshot(&self) -> BTreeMap<K, V> {
        let mut visible = self.base.clone();
        for pending in self.pending.values() {
            for change in &pending.changes {
                apply_change(&mut visible, change);
            }
        }
        visible
    }

    fn emit(
        &mut self,
        change: Option<CollectionChange<K, V>>,
        transaction: Option<(TransactionId, TransactionStatus)>,
    ) -> u64 {
        self.notification_revision += 1;
        if self.events.receiver_count() == 0 {
            return self.notification_revision;
        }
        let event = CollectionEvent {
            notification_revision: self.notification_revision,
            snapshot: self.visible_snapshot(),
            change,
            transaction,
        };
        let _ = self.events.send(event);
        self.notification_revision
    }
}

fn apply_change<K, V>(rows: &mut BTreeMap<K, V>, change: &CollectionChange<K, V>)
where
    K: Ord + Clone,
    V: Clone,
{
    match change {
        CollectionChange::Upsert { key, value } => {
            rows.insert(key.clone(), value.clone());
        }
        CollectionChange::Remove { key } => {
            rows.remove(key);
        }
    }
}

pub struct LiveQuery<K, V> {
    current: BTreeMap<K, V>,
    updates: broadcast::Receiver<CollectionEvent<K, V>>,
}

impl<K, V> LiveQuery<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn current(&self) -> &BTreeMap<K, V> {
        &self.current
    }

    pub fn into_current(self) -> BTreeMap<K, V> {
        self.current
    }

    pub fn try_update(
        &mut self,
    ) -> Result<Option<CollectionEvent<K, V>>, broadcast::error::TryRecvError> {
        let event = self.updates.try_recv()?;
        self.current = event.snapshot.clone();
        Ok(Some(event))
    }

    pub async fn update(&mut self) -> Result<CollectionEvent<K, V>, broadcast::error::RecvError> {
        let event = self.updates.recv().await?;
        self.current = event.snapshot.clone();
        Ok(event)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CollectionError {
    #[error("transaction {0:?} already exists")]
    DuplicateTransaction(TransactionId),
    #[error("transaction {0:?} does not exist")]
    UnknownTransaction(TransactionId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BackendError, CommandId, MockApplicationBackend};

    #[test]
    fn live_query_reads_local_state_before_any_refresh() {
        let collection = Collection::from_base([(1_u32, "local")]);
        let query = collection.live_query();

        assert_eq!(query.current().get(&1), Some(&"local"));
    }

    #[test]
    fn live_query_receives_authoritative_updates() {
        let mut collection = Collection::from_base([(1_u32, "old")]);
        let mut query = collection.live_query();
        collection
            .apply_authoritative(CollectionChange::Upsert {
                key: 1,
                value: "new",
            })
            .unwrap();

        let event = query.try_update().unwrap().unwrap();
        assert_eq!(event.notification_revision, 1);
        assert_eq!(query.current().get(&1), Some(&"new"));
    }

    #[tokio::test]
    async fn accepted_command_commits_optimistic_changes_without_a_followup_event() {
        let backend = MockApplicationBackend::new();
        let mut collection = Collection::from_base([(1_u32, "old")]);
        let transaction = TransactionId(1);
        collection
            .apply_optimistic(
                transaction,
                CollectionChange::Upsert {
                    key: 1,
                    value: "new",
                },
            )
            .unwrap();

        let status = collection
            .submit(
                &backend,
                transaction,
                crate::backend::Command {
                    id: CommandId(7),
                    name: "update".to_owned(),
                    arguments: serde_json::Value::Null,
                },
            )
            .await
            .unwrap();

        assert_eq!(status, TransactionStatus::Accepted);
        assert_eq!(collection.get(&1), Some("new"));
        assert_eq!(collection.transaction_status(transaction), Some(status));
    }

    #[tokio::test]
    async fn rejected_command_response_rolls_back_optimistic_changes() {
        let backend = MockApplicationBackend::new();
        backend.queue_submit_response(Ok(crate::backend::CommandReceipt {
            command: CommandId(9),
            accepted: false,
        }));
        let mut collection = Collection::from_base([(1_u32, "old")]);
        let transaction = TransactionId(9);
        collection
            .apply_optimistic(
                transaction,
                CollectionChange::Upsert {
                    key: 1,
                    value: "new",
                },
            )
            .unwrap();

        assert_eq!(
            collection
                .submit(
                    &backend,
                    transaction,
                    crate::backend::Command {
                        id: CommandId(9),
                        name: "update".to_owned(),
                        arguments: serde_json::Value::Null,
                    },
                )
                .await,
            Err(MutationError::Rejected)
        );
        assert_eq!(collection.get(&1), Some("old"));
        assert_eq!(
            collection.transaction_status(transaction),
            Some(TransactionStatus::RolledBack)
        );
    }

    #[tokio::test]
    async fn backend_rejection_and_transport_failure_roll_back_optimistic_changes() {
        let backend = MockApplicationBackend::new();
        let mut collection = Collection::from_base([(1_u32, "old")]);
        let transaction = TransactionId(2);
        collection
            .apply_optimistic(
                transaction,
                CollectionChange::Upsert {
                    key: 1,
                    value: "new",
                },
            )
            .unwrap();
        backend.queue_submit_error(BackendError::Rejected("no".to_owned()));

        assert_eq!(
            collection
                .submit(
                    &backend,
                    transaction,
                    crate::backend::Command {
                        id: CommandId(8),
                        name: "update".to_owned(),
                        arguments: serde_json::Value::Null,
                    },
                )
                .await,
            Err(MutationError::Backend(BackendError::Rejected(
                "no".to_owned()
            )))
        );
        assert_eq!(collection.get(&1), Some("old"));
        assert_eq!(
            collection.transaction_status(transaction),
            Some(TransactionStatus::RolledBack)
        );
    }

    #[test]
    fn authoritative_snapshot_and_later_event_update_the_projection() {
        let mut collection = Collection::new();
        collection
            .replace_authoritative([CollectionChange::Upsert {
                key: "title",
                value: "server",
            }])
            .unwrap();
        collection
            .apply_authoritative(CollectionChange::Upsert {
                key: "title",
                value: "corrected",
            })
            .unwrap();
        assert_eq!(collection.get(&"title"), Some("corrected"));
    }
}
