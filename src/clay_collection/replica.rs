use std::collections::BTreeMap;

use crate::types::Change;
use crate::types::RowEvidence;
use crate::types::ScopeId;
use crate::types::Version;

/// One authoritative row as retained by a replica.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry<V> {
    pub version: Version,
    pub value: V,
    pub evidence: RowEvidence,
    pub write_context: Option<Vec<u8>>,
}

/// Latest row per key.
///
/// A replica is not a log: an overwrite replaces the prior row, so a consumer that
/// missed intermediate revisions still converges on the same value. Rows arrive as
/// [`Change`]s whose `row_version` was minted by whoever is authoritative for the row;
/// the replica adopts that incarnation and never mints versions of its own except
/// through the `plan_*` calls an authority uses to author transitions. Applying is
/// idempotent and order-safe within an incarnation: a change whose version does not
/// outrank what the replica already holds for the key is skipped.
///
/// `owners` is authority-side bookkeeping only: which scope contributed which row, so a
/// scope's teardown can plan its removals. It is never part of a change, because a
/// reader has no business removing rows, and ownership is not something a mirror needs
/// to render.
#[derive(Clone, Debug, PartialEq)]
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

    pub fn apply(&mut self, changes: &[Change<K, V>]) {
        for change in changes {
            assert_eq!(
                change.row_version().incarnation,
                self.incarnation,
                "replica incarnation changed without an explicit reset"
            );
        }
        for change in changes {
            let row_version = change.row_version();
            self.seen = self.seen.max(row_version.seq);
            if self
                .version(change.key())
                .is_some_and(|current| current >= row_version)
            {
                continue;
            }
            match change {
                Change::Upsert {
                    key,
                    value,
                    evidence,
                    write_context,
                    ..
                } => {
                    self.tombstones.remove(key);
                    self.rows.insert(
                        key.clone(),
                        Entry {
                            version: row_version,
                            value: value.clone(),
                            evidence: *evidence,
                            write_context: write_context.clone(),
                        },
                    );
                }
                Change::Remove { key, .. } => {
                    self.rows.remove(key);
                    self.owners.remove(key);
                    self.tombstones.insert(key.clone(), row_version);
                }
            }
        }
    }

    /// Mint a version and record the row's contributor. The row itself is not written
    /// yet: planning rather than applying keeps the caller able to diff the visible set
    /// against its pre-write state once, as one transition.
    pub fn plan_put(&mut self, key: &K, owner: Option<ScopeId>, value: V) -> Change<K, V> {
        self.seen += 1;
        if let Some(owner) = owner {
            self.owners.insert(key.clone(), owner);
        }
        Change::Upsert {
            key: key.clone(),
            row_version: Version::new(self.incarnation, self.seen),
            value,
            evidence: RowEvidence::Authoritative,
            write_context: None,
        }
    }

    /// Mint a removal whether or not the row is present: a delete is idempotent, and an
    /// absent row still needs a version so a pending overlay parked on this key has
    /// something to be satisfied by.
    pub fn plan_remove(&mut self, key: &K) -> Change<K, V> {
        self.seen += 1;
        Change::Remove {
            key: key.clone(),
            row_version: Version::new(self.incarnation, self.seen),
        }
    }

    /// Plan the withdrawal of every row an owner contributed, so a scope's teardown is a
    /// single visible transition rather than a cascade of partial removals.
    pub fn plan_remove_owner(&mut self, owner: ScopeId) -> Vec<Change<K, V>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn upsert(incarnation: u64, seq: u64) -> Change<u32, &'static str> {
        Change::Upsert {
            key: 1,
            row_version: Version::new(incarnation, seq),
            value: "value",
            evidence: RowEvidence::Authoritative,
            write_context: None,
        }
    }

    #[test]
    fn tombstones_reject_replayed_and_stale_resurrections() {
        let mut replica = Replica::new(1);
        replica.apply(&[upsert(1, 1)]);
        let removal = Change::Remove {
            key: 1,
            row_version: Version::new(1, 3),
        };
        replica.apply(std::slice::from_ref(&removal));
        let removed = replica.clone();
        replica.apply(&[upsert(1, 1), upsert(1, 2), upsert(1, 3), removal]);
        assert_eq!(replica, removed);
        assert_eq!(replica.version(&1), Some(Version::new(1, 3)));
        replica.apply(&[upsert(1, 4)]);
        assert_eq!(replica.get(&1).unwrap().version, Version::new(1, 4));
        replica.apply(&[Change::Remove {
            key: 1,
            row_version: Version::new(1, 3),
        }]);
        assert!(replica.contains_key(&1));
    }

    #[test]
    fn changes_cannot_mix_or_switch_incarnations() {
        let mut replica = Replica::new(2);
        replica.apply(&[upsert(2, 1)]);
        let initial = replica.clone();
        for incarnation in [1, 3] {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                replica.apply(&[upsert(2, 2), upsert(incarnation, 3)]);
            }));
            assert!(result.is_err());
            assert_eq!(replica, initial);
        }
    }

    #[test]
    fn explicit_reset_discards_rows_owners_tombstones_and_sequence() {
        let mut replica = Replica::new(1);
        let owned = replica.plan_put(&1, Some(ScopeId(1)), "value");
        replica.apply(&[owned]);
        replica.apply(&[Change::Remove {
            key: 2,
            row_version: Version::new(1, 3),
        }]);
        replica.reset(2);
        assert_eq!(replica, Replica::new(2));
        let change = replica.plan_put(&2, None, "new");
        assert_eq!(change.row_version(), Version::new(2, 1));
        replica.apply(&[change]);
        assert_eq!(replica.get(&2).unwrap().value, "new");
    }

    #[test]
    #[should_panic(expected = "replica went backwards")]
    fn explicit_reset_cannot_move_backwards() {
        let mut replica = Replica::<u32, &'static str>::new(2);
        replica.reset(1);
    }
}
