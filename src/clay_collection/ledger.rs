use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::types::OperationId;
use crate::types::ScopeId;
use crate::types::Version;
use crate::types::WriteOutcome;

/// Why an overlay is still resident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayWait {
    /// The write has not resolved yet.
    Outcome,
    /// Accepted; hold until base reaches the version it landed at, so the committed row
    /// arrives by replication instead of the row flashing back to its pre-write value.
    Base { incarnation: u64, seq: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Overlay<K, V> {
    pub id: OperationId,
    pub key: K,
    /// `None` is an optimistic delete: the row reads as absent until replication brings
    /// the removal back.
    pub value: Option<V>,
    pub writer: ScopeId,
    pub wait: OverlayWait,
}

/// Segregated whole-row writes, keyed by operation and by row so the newest pending
/// write to a key is the visible one while older ones still exist to be retired.
#[derive(Debug)]
pub struct Ledger<K, V> {
    overlays: BTreeMap<OperationId, Overlay<K, V>>,
    by_key: BTreeMap<K, BTreeSet<OperationId>>,
}

impl<K: Ord + Clone, V: Clone> Default for Ledger<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V: Clone> Ledger<K, V> {
    pub fn new() -> Self {
        Self {
            overlays: BTreeMap::new(),
            by_key: BTreeMap::new(),
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.by_key.keys()
    }

    pub fn len(&self) -> usize {
        self.overlays.len()
    }

    pub fn is_empty(&self) -> bool {
        self.overlays.is_empty()
    }

    pub fn get(&self, id: OperationId) -> Option<&Overlay<K, V>> {
        self.overlays.get(&id)
    }

    /// The value this key currently shows from the pending layer, if it has one. The
    /// option distinguishes "this key has a pending write" from "no pending write"; the
    /// write itself may be an optimistic delete, hence the outer option.
    pub fn pending(&self, key: &K) -> Option<Option<&V>> {
        let ids = self.by_key.get(key)?;
        let newest = ids.last()?;
        self.overlays
            .get(newest)
            .map(|overlay| overlay.value.as_ref())
    }

    pub fn put(&mut self, overlay: Overlay<K, V>) {
        self.by_key
            .entry(overlay.key.clone())
            .or_default()
            .insert(overlay.id);
        self.overlays.insert(overlay.id, overlay);
    }

    pub fn remove(&mut self, id: OperationId) -> Option<Overlay<K, V>> {
        let overlay = self.overlays.remove(&id)?;
        if let Some(ids) = self.by_key.get_mut(&overlay.key) {
            ids.remove(&id);
            if ids.is_empty() {
                self.by_key.remove(&overlay.key);
            }
        }
        Some(overlay)
    }

    pub fn clear(&mut self) {
        self.overlays.clear();
        self.by_key.clear();
    }

    /// Apply the write's outcome: rejection withdraws it outright, acceptance parks it
    /// until replication carries the committed row back.
    pub fn settle(&mut self, id: OperationId, outcome: WriteOutcome) -> Option<Overlay<K, V>> {
        if !self.overlays.contains_key(&id) {
            return None;
        }
        match outcome {
            WriteOutcome::Durable { version } => {
                let entry = self.overlays.get_mut(&id)?;
                entry.wait = OverlayWait::Base {
                    incarnation: version.incarnation,
                    seq: version.seq,
                };
                None
            }
            WriteOutcome::Rejected | WriteOutcome::Unknown => self.remove(id),
        }
    }

    /// Retire overlays whose parked base version has now been reached. Called with the
    /// keys a batch touched, because that is exactly when base can have advanced far
    /// enough — a waiting overlay never becomes satisfiable any other way. Base and the
    /// parked target are compared as whole versions, so a row re-authored under a later
    /// incarnation cannot satisfy a target minted under the old one.
    pub fn retire_satisfied<'a>(
        &mut self,
        touched: impl IntoIterator<Item = &'a K>,
        base_version: impl Fn(&K) -> Option<Version>,
    ) -> Vec<OperationId>
    where
        K: 'a,
    {
        let mut retired = Vec::new();
        for key in touched {
            let Some(ids) = self.by_key.get(key).cloned() else {
                continue;
            };
            let Some(base) = base_version(key) else {
                continue;
            };
            for id in ids {
                let satisfied = self.overlays.get(&id).is_some_and(|overlay| {
                    matches!(
                        overlay.wait,
                        OverlayWait::Base { incarnation, seq }
                            if incarnation == base.incarnation && base.seq >= seq
                    )
                });
                if satisfied {
                    retired.push(id);
                }
            }
        }
        for id in retired.iter().copied() {
            self.remove(id);
        }
        retired
    }

    /// The overlays one scope authored. Disposal withdraws these without touching the
    /// authority's own rows; the base rows an owner contributed are the replica's to
    /// remove, and both land in one transaction.
    pub fn ids_for_scope(&self, scope: ScopeId) -> Vec<OperationId> {
        self.overlays
            .values()
            .filter(|overlay| overlay.writer == scope)
            .map(|overlay| overlay.id)
            .collect()
    }
}
