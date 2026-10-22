use std::collections::BTreeMap;

use crate::types::BaseDelta;
use crate::types::ScopeId;
use crate::types::Version;

/// One authoritative row as retained by a replica.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry<V> {
    pub version: Version,
    pub value: V,
}

/// Latest row per key.
///
/// A replica is not a log: an overwrite replaces the prior row, so a consumer that
/// missed intermediate revisions still converges on the same value. `seq` is only ever
/// raised by deltas from the authority — the replica does not author rows, which is why
/// a hop that merely relays data cannot accidentally mint a version that outranks the
/// node's.
///
/// `owners` is authority-side bookkeeping only: which scope contributed which row, so a
/// scope's teardown can plan its removals. It is never part of a delta, because a
/// consumer relaying rows has no business removing them, and ownership is not something
/// a downstream hop needs to know to render.
#[derive(Debug)]
pub struct Replica<K, V> {
    incarnation: u64,
    rows: BTreeMap<K, Entry<V>>,
    owners: BTreeMap<K, ScopeId>,
    tombstones: BTreeMap<K, Version>,
    seen: u64,
}

impl<K: Ord + Clone, V: Clone> Replica<K, V> {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            rows: BTreeMap::new(),
            owners: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            seen: 0,
        }
    }

    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    pub fn rows(&self) -> impl Iterator<Item = (&K, &Entry<V>)> {
        self.rows.iter()
    }

    pub fn get(&self, key: &K) -> Option<&Entry<V>> {
        self.rows.get(key)
    }

    pub fn version(&self, key: &K) -> Option<Version> {
        self.rows
            .get(key)
            .map(|entry| entry.version)
            .or_else(|| self.tombstones.get(key).copied())
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.rows.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Adopt a later incarnation, discarding every row: counters from the old epoch
    /// cannot be compared with the new ones, so keeping them would be a silent
    /// mis-ordering rather than stale data.
    pub fn reset(&mut self, incarnation: u64) {
        assert!(
            incarnation >= self.incarnation,
            "replica went backwards from incarnation {} to {}",
            self.incarnation,
            incarnation
        );
        self.incarnation = incarnation;
        self.rows.clear();
        self.owners.clear();
        self.tombstones.clear();
        self.seen = 0;
    }

    pub fn apply(&mut self, incarnation: u64, deltas: &[BaseDelta<K, V>]) {
        if incarnation != self.incarnation {
            return;
        }
        for delta in deltas {
            match delta {
                BaseDelta::Upsert {
                    key,
                    version,
                    value,
                } => {
                    if version.incarnation != self.incarnation {
                        continue;
                    }
                    self.seen = self.seen.max(version.seq);
                    if self.version(key).is_some_and(|current| current >= *version) {
                        continue;
                    }
                    self.tombstones.remove(key);
                    self.rows.insert(
                        key.clone(),
                        Entry {
                            version: *version,
                            value: value.clone(),
                        },
                    );
                }
                BaseDelta::Remove { key, version } => {
                    if version.incarnation != self.incarnation {
                        continue;
                    }
                    self.seen = self.seen.max(version.seq);
                    if self.version(key).is_some_and(|current| current >= *version) {
                        continue;
                    }
                    self.rows.remove(key);
                    self.owners.remove(key);
                    self.tombstones.insert(key.clone(), *version);
                }
            }
        }
    }

    /// Mint a version and record the row's contributor. The row itself is not written
    /// yet: planning rather than applying keeps the caller able to diff the visible set
    /// against its pre-write state once, as one transition.
    pub fn plan_put(&mut self, key: &K, owner: Option<ScopeId>, value: V) -> BaseDelta<K, V> {
        self.seen += 1;
        if let Some(owner) = owner {
            self.owners.insert(key.clone(), owner);
        }
        BaseDelta::Upsert {
            key: key.clone(),
            version: Version::new(self.incarnation, self.seen),
            value,
        }
    }

    /// Mint a removal whether or not the row is present: a delete is idempotent, and an
    /// absent row still needs a version so a pending overlay parked on this key has
    /// something to be satisfied by.
    pub fn plan_remove(&mut self, key: &K) -> BaseDelta<K, V> {
        self.seen += 1;
        BaseDelta::Remove {
            key: key.clone(),
            version: Version::new(self.incarnation, self.seen),
        }
    }

    /// Plan the withdrawal of every row an owner contributed, so a scope's teardown is a
    /// single visible transition rather than a cascade of partial removals.
    pub fn plan_remove_owner(&mut self, owner: ScopeId) -> Vec<BaseDelta<K, V>> {
        let doomed: Vec<K> = self
            .owners
            .iter()
            .filter(|(_, scope)| **scope == owner)
            .map(|(key, _)| key.clone())
            .collect();
        doomed
            .into_iter()
            .map(|key| self.plan_remove(&key))
            .collect()
    }

    pub fn version_now(&self) -> Version {
        Version::new(self.incarnation, self.seen)
    }
}
