//! The host materializer (ADR 017 §9): how a [`super::RegistrationCollection`] combines
//! its scopes' contributions into the consumer-facing projection.
//!
//! The materializer is chosen once, at construction, and there is no default: every
//! constructor either names a [`Materializer`] or is documented as [`Materializer::Union`].
//! The authority keeps contributions per `(scope, local key)` and recomputes the
//! materialized rows of every local key a mutation touches, inside the same projection
//! transaction as the mutation. That is what makes cleanup automatic for merged views:
//! closing, uninstalling, removing or clearing the winning contribution reveals the next
//! one in the very transaction that withdraws it, with no intermediate state.
//!
//! A recomputed row is published only when the contributions behind it changed, judged by
//! the `(scope, row version)` list that produced it, so recomputing an untouched key is a
//! no-op. Under [`Materializer::Union`] the published row version is the contribution's own.
//! Under every other policy a consumer row is not any one contribution, and revealing an
//! older contribution must still outrank what a mirror holds, so each change publishes a
//! freshly minted version from the collection's counter.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::Inner;
use super::ScopeState;
use crate::projection::ProjectionDispatch;
use crate::replica::Entry;
use crate::source::Source;
use crate::types::Change;
use crate::types::RegistrationScopeId;
use crate::types::RowEvidence;
use crate::types::Version;

/// Host-supplied precedence of a scope under [`Materializer::Priority`]; the highest wins.
///
/// Ranks are unique among live scopes. A tie has no principled winner (scope ids are
/// in-process and installation order is not stable), so creating or installing a scope
/// whose rank another live scope holds is a programming error and panics. The host
/// prevents ties by construction: derive the rank from a fixed precedence table over
/// plug identities, never from arrival order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScopeRank(pub u64);

/// One scope's row for a local key, as handed to [`Materializer::Group`]'s `merge`.
#[derive(Clone, Debug, PartialEq)]
pub struct Contribution<V> {
    pub scope: RegistrationScopeId,
    pub version: Version,
    pub value: V,
}

/// The explicit merge policy of a collection, with the functions that shape the
/// consumer-facing key (and, for `Group`, value).
///
/// Every policy but `Union` keys the consumer row by the local key alone, through `key`,
/// which must be injective. The projection waist is unchanged; only the key and value
/// shape differ.
pub enum Materializer<K, V, P> {
    /// Every scope's rows side by side. Consumer key `(scope, local key)` through `key`,
    /// which must be injective over that pair.
    Union {
        key: fn(RegistrationScopeId, &K) -> P,
    },
    /// One row per local key whose value is `merge` of every contribution, in a
    /// deterministic scope order: installation scopes by installation key bytes, then
    /// incarnation scopes by mint order. `merge` is called with a non-empty slice.
    Group {
        key: fn(&K) -> P,
        merge: fn(&[Contribution<V>]) -> V,
    },
    /// The contribution of the highest-ranked scope wins. Incarnation scopes take their
    /// rank at `create_ranked_scope`; installation scopes take `installation_rank` of
    /// their installation key, so a reopened durable collection restores the same ranks.
    Priority {
        key: fn(&K) -> P,
        installation_rank: fn(&[u8]) -> ScopeRank,
    },
    /// The contribution with the greatest host row version wins: the most recently
    /// accepted write, never a guest clock. After a durable reopen the versions are
    /// reassigned in store scan order, so cross-installation recency does not survive
    /// a restart.
    HostOrderedLww { key: fn(&K) -> P },
    /// At most one scope may hold a local key. A `put` (or install) naming a key another
    /// live scope holds, staged scopes included, fails with
    /// [`crate::MutationError::Collision`].
    RejectCollision { key: fn(&K) -> P },
}

impl<K, V, P> Clone for Materializer<K, V, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V, P> Copy for Materializer<K, V, P> {}

/// `(scope, row version)` of every contribution behind one consumer row.
type Sig = Vec<(RegistrationScopeId, Version)>;

type Group<'a, V> = Vec<(RegistrationScopeId, &'a Entry<V>)>;

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> Inner<K, V, P, S> {
    /// A new scope's rank, checked against the policy: `Priority` scopes must carry a
    /// unique rank, others must not carry one.
    pub(super) fn checked_rank(
        &self,
        rank: Option<ScopeRank>,
        except: Option<RegistrationScopeId>,
    ) -> Option<ScopeRank> {
        match (&self.materializer, rank) {
            (Materializer::Priority { .. }, Some(rank)) => {
                assert!(
                    self.scopes
                        .iter()
                        .all(|(id, state)| Some(*id) == except || state.rank != Some(rank)),
                    "scope rank {rank:?} is already held by a live scope; ranks must be unique"
                );
                Some(rank)
            }
            (Materializer::Priority { .. }, None) => {
                panic!("a Priority collection needs a ranked scope")
            }
            (_, Some(_)) => panic!("only a Priority collection takes scope ranks"),
            (_, None) => None,
        }
    }

    /// The scope, other than `except`, that holds `key`, whether published or staged.
    pub(super) fn holder(
        &self,
        key: &K,
        except: Option<RegistrationScopeId>,
    ) -> Option<RegistrationScopeId> {
        self.scopes
            .iter()
            .find(|(id, state)| Some(**id) != except && state.rows.contains_key(key))
            .map(|(id, _)| *id)
    }

    pub(super) fn rejects_collisions(&self) -> bool {
        matches!(self.materializer, Materializer::RejectCollision { .. })
    }

    /// The consumer rows for `key`, each with the contributions behind it. Staged scopes
    /// contribute nothing.
    fn groups(&self, key: &K) -> Vec<(P, Group<'_, V>)> {
        let mut contributions: Vec<(RegistrationScopeId, &ScopeState<K, V>, &Entry<V>)> = self
            .scopes
            .iter()
            .filter(|(_, state)| !state.staged)
            .filter_map(|(id, state)| state.rows.get(key).map(|entry| (*id, state, entry)))
            .collect();
        match &self.materializer {
            Materializer::Union { key: physical } => contributions
                .into_iter()
                .map(|(id, _, entry)| (physical(id, key), vec![(id, entry)]))
                .collect(),
            Materializer::Group { key: consumer, .. } => {
                if contributions.is_empty() {
                    return Vec::new();
                }
                contributions.sort_by_key(|(id, state, _)| {
                    (
                        state.install_key.is_none(),
                        state.install_key.as_deref(),
                        *id,
                    )
                });
                vec![(
                    consumer(key),
                    contributions
                        .into_iter()
                        .map(|(id, _, entry)| (id, entry))
                        .collect(),
                )]
            }
            Materializer::Priority { key: consumer, .. } => contributions
                .into_iter()
                .max_by_key(|(_, state, _)| state.rank.expect("Priority scope without a rank"))
                .map(|(id, _, entry)| (consumer(key), vec![(id, entry)]))
                .into_iter()
                .collect(),
            Materializer::HostOrderedLww { key: consumer } => contributions
                .into_iter()
                .max_by_key(|(_, _, entry)| entry.version)
                .map(|(id, _, entry)| (consumer(key), vec![(id, entry)]))
                .into_iter()
                .collect(),
            Materializer::RejectCollision { key: consumer } => {
                assert!(
                    contributions.len() <= 1,
                    "RejectCollision collection holds one key in two scopes"
                );
                contributions
                    .into_iter()
                    .map(|(id, _, entry)| (consumer(key), vec![(id, entry)]))
                    .collect()
            }
        }
    }

    fn render(&self, group: &Group<'_, V>) -> (V, RowEvidence, Option<Vec<u8>>) {
        if let Materializer::Group { merge, .. } = &self.materializer {
            let contributions: Vec<_> = group
                .iter()
                .map(|(scope, entry)| Contribution {
                    scope: *scope,
                    version: entry.version,
                    value: entry.value.clone(),
                })
                .collect();
            return (merge(&contributions), RowEvidence::Authoritative, None);
        }
        let (_, entry) = group[0];
        (
            entry.value.clone(),
            entry.evidence,
            entry.write_context.clone(),
        )
    }

    /// The materialized rows of every published scope, for the authority-side snapshot.
    /// A row's version is the newest contributing version: the projection's own row
    /// versions under non-`Union` policies are minted per change and not retained.
    pub(super) fn materialized(&self) -> BTreeMap<P, Entry<V>> {
        let keys: BTreeSet<&K> = self
            .scopes
            .values()
            .filter(|state| !state.staged)
            .flat_map(|state| state.rows.keys())
            .collect();
        let mut rows = BTreeMap::new();
        for key in keys {
            for (consumer, group) in self.groups(key) {
                let (value, evidence, write_context) = self.render(&group);
                let version = group.iter().map(|(_, entry)| entry.version).max().unwrap();
                rows.insert(
                    consumer,
                    Entry {
                        version,
                        value,
                        evidence,
                        write_context,
                    },
                );
            }
        }
        rows
    }

    fn sigs(&self, keys: &BTreeSet<K>) -> BTreeMap<P, Sig> {
        keys.iter()
            .flat_map(|key| self.groups(key))
            .map(|(consumer, group)| {
                (
                    consumer,
                    group
                        .iter()
                        .map(|(scope, entry)| (*scope, entry.version))
                        .collect(),
                )
            })
            .collect()
    }

    /// The transition from `before` (taken by [`Self::sigs`] ahead of a mutation) to the
    /// current materialization of `keys`. `reserved` is a version the caller already
    /// handed out for this mutation; the first minted change consumes it.
    fn changes(
        &mut self,
        before: BTreeMap<P, Sig>,
        keys: &BTreeSet<K>,
        mut reserved: Option<Version>,
    ) -> Vec<Change<P, V>> {
        let mut upserts = Vec::new();
        let mut after = BTreeSet::new();
        for key in keys {
            for (consumer, group) in self.groups(key) {
                let sig: Sig = group
                    .iter()
                    .map(|(scope, entry)| (*scope, entry.version))
                    .collect();
                if before.get(&consumer) != Some(&sig) {
                    let (value, evidence, write_context) = self.render(&group);
                    upserts.push((consumer.clone(), sig, value, evidence, write_context));
                }
                after.insert(consumer);
            }
        }
        let mut changes = Vec::with_capacity(before.len() + upserts.len());
        for consumer in before.into_keys().filter(|key| !after.contains(key)) {
            let row_version = reserved.take().unwrap_or_else(|| self.next_version());
            changes.push(Change::Remove {
                key: consumer,
                row_version,
            });
        }
        let union = matches!(self.materializer, Materializer::Union { .. });
        for (consumer, sig, value, evidence, write_context) in upserts {
            let row_version = if union {
                sig[0].1
            } else {
                reserved.take().unwrap_or_else(|| self.next_version())
            };
            changes.push(Change::Upsert {
                key: consumer,
                row_version,
                value,
                evidence,
                write_context,
            });
        }
        changes
    }

    /// Run `mutate`, then publish the recomputed materialization of `keys` as one
    /// projection transaction (none if nothing visible changed). `keys` must cover every
    /// local key whose contributions `mutate` can change.
    pub(super) fn commit<R>(
        &mut self,
        keys: &BTreeSet<K>,
        reserved: Option<Version>,
        mutate: impl FnOnce(&mut Self) -> R,
    ) -> (R, Vec<ProjectionDispatch<P, V, S::Selector>>) {
        let before = self.sigs(keys);
        let output = mutate(self);
        let changes = self.changes(before, keys, reserved);
        let projection = if changes.is_empty() {
            Vec::new()
        } else {
            vec![self.projection.publish_deferred(changes)]
        };
        (output, projection)
    }

    /// Every published materialized row as upserts: the republication after a reset or a
    /// durable reopen, when nothing is `before`.
    pub(super) fn full_changes(&mut self) -> Vec<Change<P, V>> {
        let keys: BTreeSet<K> = self
            .scopes
            .values()
            .flat_map(|state| state.rows.keys().cloned())
            .collect();
        self.changes(BTreeMap::new(), &keys, None)
    }
}
