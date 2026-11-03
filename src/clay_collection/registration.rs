//! Owner-scoped contribution registrations (ADR 017 §9).
//!
//! A [`RegistrationCollection`] is the substrate primitive behind "guests register rows
//! and the rows live exactly as long as their writer": each writer gets a [`WriterId`],
//! rows are physically keyed by `(WriterId, local key)` so two writers never collide on
//! the same local key, `put`/`remove` are compare-and-set against the row version the
//! writer last saw, and dropping a writer withdraws all of its rows in one transaction.
//!
//! Reads go through the collection's [`Projection`]: it is the same leased waist every
//! other collection uses. The physical key type `P` is whatever that projection is keyed
//! by; the default is the `(WriterId, K)` tuple, and a byte-keyed waist supplies an
//! encoding of the pair instead.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use crate::projection::PendingNotifications;
use crate::projection::Projection;
use crate::replica::Entry;
use crate::replica::Replica;
use crate::source::Source;
use crate::types::Change;
use crate::types::MutationError;
use crate::types::Version;
use crate::types::WriterId;

struct Inner<K, V, P, S: Source<P, V>> {
    replica: Replica<P, V>,
    projection: Projection<P, V, S>,
    physical: fn(WriterId, &K) -> P,
    next_writer: u64,
    /// Local keys per writer, so closure withdraws exactly that writer's rows.
    rows: BTreeMap<WriterId, BTreeSet<K>>,
}

/// The authority side of an owner-scoped contribution collection.
pub struct RegistrationCollection<K, V, P = (WriterId, K), S: Source<P, V> = ()> {
    inner: Arc<Mutex<Inner<K, V, P, S>>>,
}

impl<K, V, P, S: Source<P, V>> std::fmt::Debug for RegistrationCollection<K, V, P, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationCollection")
            .finish_non_exhaustive()
    }
}

impl<K, V, P, S: Source<P, V>> Clone for RegistrationCollection<K, V, P, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K: Ord + Clone, V: Clone> RegistrationCollection<K, V> {
    pub fn new(runtime_epoch: [u64; 2]) -> Self {
        Self::with_source(
            (),
            NonZeroUsize::new(256).unwrap(),
            runtime_epoch,
            |writer, key| (writer, key.clone()),
        )
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationCollection<K, V, P, S> {
    pub fn with_source(
        source: S,
        replay_capacity: NonZeroUsize,
        runtime_epoch: [u64; 2],
        physical: fn(WriterId, &K) -> P,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                replica: Replica::new(1),
                projection: Projection::with_source(source, replay_capacity, runtime_epoch),
                physical,
                next_writer: 0,
                rows: BTreeMap::new(),
            })),
        }
    }

    /// Mint a writer. The returned handle owns its rows: dropping it withdraws them in
    /// one transaction.
    pub fn writer(&self) -> RegistrationWriter<K, V, P, S> {
        let mut inner = self.lock();
        inner.next_writer = inner
            .next_writer
            .checked_add(1)
            .expect("writer identity exhausted");
        RegistrationWriter {
            id: WriterId(inner.next_writer),
            inner: Arc::clone(&self.inner),
        }
    }

    /// The read waist over this collection's rows, keyed by physical key.
    pub fn projection(&self) -> Projection<P, V, S> {
        self.lock().projection.clone()
    }

    /// Host-local authoritative snapshot. Reading through a lease is the contract for
    /// consumers; this is the convenience the authority itself (and its tests) uses.
    pub fn snapshot(&self) -> BTreeMap<P, Entry<V>> {
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

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V, P, S>> {
        self.inner.lock().expect("registration state poisoned")
    }
}

/// One registrant's write handle. Mutations are compare-and-set: `based_on` must equal
/// the version the collection currently holds for the key (`None` meaning absent), and a
/// mismatch returns [`MutationError::Conflict`] carrying the actual version so the
/// writer can rebase and retry — the handle stays usable.
pub struct RegistrationWriter<
    K: Ord + Clone,
    V: Clone,
    P: Ord + Clone = (WriterId, K),
    S: Source<P, V> = (),
> {
    id: WriterId,
    inner: Arc<Mutex<Inner<K, V, P, S>>>,
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> std::fmt::Debug
    for RegistrationWriter<K, V, P, S>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationWriter")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationWriter<K, V, P, S> {
    pub fn id(&self) -> WriterId {
        self.id
    }

    pub fn version(&self, key: &K) -> Option<Version> {
        let inner = self.lock();
        inner
            .replica
            .get(&(inner.physical)(self.id, key))
            .map(|entry| entry.version)
    }

    pub fn row_count(&self) -> usize {
        self.lock().rows.get(&self.id).map_or(0, BTreeSet::len)
    }

    /// Register or replace `key`, returning the new row version.
    pub fn put(
        &mut self,
        key: K,
        based_on: Option<Version>,
        value: V,
    ) -> Result<Version, MutationError> {
        let (version, notifications) = self.put_deferred(key, based_on, value)?;
        notifications.dispatch();
        Ok(version)
    }

    pub fn put_deferred(
        &mut self,
        key: K,
        based_on: Option<Version>,
        value: V,
    ) -> Result<(Version, PendingNotifications), MutationError> {
        let mut inner = self.lock();
        let physical = (inner.physical)(self.id, &key);
        let current = inner.replica.get(&physical).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        let change = inner.replica.plan_put(&physical, None, value);
        let version = change.row_version();
        inner.replica.apply(std::slice::from_ref(&change));
        inner.rows.entry(self.id).or_default().insert(key);
        let notifications = inner.projection.publish_deferred(vec![change]);
        drop(inner);
        Ok((version, notifications))
    }

    /// Withdraw `key`, returning the version of the removal. Idempotent when the row is
    /// already absent and no version was claimed: the end state is what the writer asked
    /// for, nothing is published, and the result is `None`.
    pub fn remove(
        &mut self,
        key: K,
        based_on: Option<Version>,
    ) -> Result<Option<Version>, MutationError> {
        let (version, notifications) = self.remove_deferred(key, based_on)?;
        notifications.dispatch();
        Ok(version)
    }

    pub fn remove_deferred(
        &mut self,
        key: K,
        based_on: Option<Version>,
    ) -> Result<(Option<Version>, PendingNotifications), MutationError> {
        let mut inner = self.lock();
        let physical = (inner.physical)(self.id, &key);
        let current = inner.replica.get(&physical).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        if current.is_none() {
            return Ok((None, PendingNotifications::default()));
        }
        let change = inner.replica.plan_remove(&physical);
        let version = change.row_version();
        inner.replica.apply(std::slice::from_ref(&change));
        inner
            .replica
            .retire_authoritative_removal(&physical, version);
        if let Some(keys) = inner.rows.get_mut(&self.id) {
            keys.remove(&key);
        }
        let notifications = inner.projection.publish_deferred(vec![change]);
        drop(inner);
        Ok((Some(version), notifications))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V, P, S>> {
        self.inner.lock().expect("registration state poisoned")
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> Drop
    for RegistrationWriter<K, V, P, S>
{
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
        let physical = inner.physical;
        let changes: Vec<Change<P, V>> = keys
            .iter()
            .map(|key| inner.replica.plan_remove(&physical(self.id, key)))
            .collect();
        inner.replica.apply(&changes);
        for change in &changes {
            inner
                .replica
                .retire_authoritative_removal(change.key(), change.row_version());
        }
        notifications = inner.projection.publish_deferred(changes);
        drop(inner);
        notifications.dispatch();
    }
}

#[cfg(test)]
#[path = "registration_tests_notifications.rs"]
mod tests;

#[cfg(test)]
#[path = "registration_tests_semantics.rs"]
mod semantics_tests;
