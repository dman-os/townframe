//! Owner-scoped contribution registrations (ADR 017 §9).
//!
//! A [`RegistrationCollection`] is the substrate primitive behind "guests register rows
//! and the rows live exactly as long as their writer": each writer gets a [`WriterId`],
//! rows are physically keyed `(WriterId, local key)` so two writers never collide on the
//! same local key, `put`/`remove` are compare-and-set against the row version the writer
//! last saw, and dropping a writer withdraws all of its rows in one transaction.
//!
//! Reads go through the collection's [`Projection`]: it is the same leased waist every
//! other collection uses, with composite keys.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use crate::projection::Projection;
use crate::replica::Entry;
use crate::replica::Replica;
use crate::types::Change;
use crate::types::MutationError;
use crate::types::Version;
use crate::types::WriterId;

#[derive(Debug)]
struct Inner<K, V> {
    replica: Replica<(WriterId, K), V>,
    projection: Projection<(WriterId, K), V>,
    next_writer: u64,
    /// Local keys per writer, so closure withdraws exactly that writer's rows.
    rows: BTreeMap<WriterId, BTreeSet<K>>,
}

/// The authority side of an owner-scoped contribution collection.
#[derive(Debug)]
pub struct RegistrationCollection<K, V> {
    inner: Arc<Mutex<Inner<K, V>>>,
}

impl<K: Ord + Clone, V: Clone> RegistrationCollection<K, V> {
    pub fn new(runtime_epoch: [u64; 2]) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                replica: Replica::new(1),
                projection: Projection::new(runtime_epoch),
                next_writer: 0,
                rows: BTreeMap::new(),
            })),
        }
    }

    /// Mint a writer. The returned handle owns its rows: dropping it withdraws them in
    /// one transaction.
    pub fn writer(&self) -> RegistrationWriter<K, V> {
        let mut inner = self.lock();
        inner.next_writer += 1;
        RegistrationWriter {
            id: WriterId(inner.next_writer),
            inner: Arc::clone(&self.inner),
        }
    }

    /// The read waist over this collection's rows, keyed `(writer, local key)`.
    pub fn projection(&self) -> Projection<(WriterId, K), V> {
        self.lock().projection.clone()
    }

    /// Host-local authoritative snapshot. Reading through a lease is the contract for
    /// consumers; this is the convenience the authority itself (and its tests) uses.
    pub fn snapshot(&self) -> BTreeMap<(WriterId, K), Entry<V>> {
        self.lock()
            .replica
            .rows()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect()
    }

    /// Withdraw every registration and begin a new projection generation: outstanding
    /// leases report `ResetRequired` and re-bootstrap into the empty collection.
    pub fn reset(&self) {
        let notifications;
        let mut inner = self.lock();
        let next = inner.replica.incarnation() + 1;
        inner.replica.reset(next);
        inner.rows.clear();
        notifications = inner.projection.reset_deferred();
        drop(inner);
        notifications.dispatch();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V>> {
        self.inner.lock().expect("registration state poisoned")
    }
}

/// One registrant's write handle. Mutations are compare-and-set: `based_on` must equal
/// the version the collection currently holds for the key (`None` meaning absent), and a
/// mismatch returns [`MutationError::Conflict`] carrying the actual version so the
/// writer can rebase and retry — the handle stays usable.
#[derive(Debug)]
pub struct RegistrationWriter<K: Ord + Clone, V: Clone> {
    id: WriterId,
    inner: Arc<Mutex<Inner<K, V>>>,
}

impl<K: Ord + Clone, V: Clone> RegistrationWriter<K, V> {
    pub fn id(&self) -> WriterId {
        self.id
    }

    /// Register or replace `key`, returning the new row version.
    pub fn put(
        &mut self,
        key: K,
        based_on: Option<Version>,
        value: V,
    ) -> Result<Version, MutationError> {
        let notifications;
        let mut inner = self.lock();
        let physical = (self.id, key.clone());
        let current = inner.replica.get(&physical).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        let change = inner.replica.plan_put(&physical, None, value);
        let version = change.row_version();
        inner.replica.apply(std::slice::from_ref(&change));
        inner.rows.entry(self.id).or_default().insert(key);
        notifications = inner.projection.publish_deferred(vec![change]);
        drop(inner);
        notifications.dispatch();
        Ok(version)
    }

    /// Withdraw `key`. Idempotent when the row is already absent and no version was
    /// claimed: the end state is what the writer asked for.
    pub fn remove(&mut self, key: K, based_on: Option<Version>) -> Result<(), MutationError> {
        let notifications;
        let mut inner = self.lock();
        let physical = (self.id, key.clone());
        let current = inner.replica.get(&physical).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        if current.is_none() {
            return Ok(());
        }
        let change = inner.replica.plan_remove(&physical);
        inner.replica.apply(std::slice::from_ref(&change));
        if let Some(keys) = inner.rows.get_mut(&self.id) {
            keys.remove(&key);
        }
        notifications = inner.projection.publish_deferred(vec![change]);
        drop(inner);
        notifications.dispatch();
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V>> {
        self.inner.lock().expect("registration state poisoned")
    }
}

impl<K: Ord + Clone, V: Clone> Drop for RegistrationWriter<K, V> {
    /// Writer closure withdraws every row it still holds, as one transaction: the
    /// collection cannot outlive a registrant's interest by accident.
    fn drop(&mut self) {
        let notifications;
        let mut inner = self.lock();
        let Some(keys) = inner.rows.remove(&self.id) else {
            return;
        };
        if keys.is_empty() {
            return;
        }
        let changes: Vec<Change<(WriterId, K), V>> = keys
            .iter()
            .map(|key| inner.replica.plan_remove(&(self.id, key.clone())))
            .collect();
        inner.replica.apply(&changes);
        notifications = inner.projection.publish_deferred(changes);
        drop(inner);
        notifications.dispatch();
    }
}

#[cfg(test)]
#[path = "registration_tests_notifications.rs"]
mod tests;
