//! The causal map: owner-scoped contribution to one logical keyspace (ADR 017 §9,
//! ADR 025 §4, and the 2026-11-04 collection model in the Clay ledger).
//!
//! A [`RegistrationCollection`] projects one value per logical key `K`. Every live
//! writer scope holds at most one candidate per key, a value or a tombstone, stamped
//! with a [`Stamp`] `(lamport, writer id)`. Readers see the winner, the candidate with
//! the highest stamp; a winning tombstone, or no candidates, means the key is absent.
//! The projected value is a [`Stamped`] so a reader can hand the stamp back as a
//! [`WriteBasis`]. The projection's `row_version` is only a feed position: retracting
//! a scope can promote an older stamp, which is published under a fresh `row_version`.
//!
//! Scope retention decides when candidates die, and retraction removes a scope's
//! candidates, tombstones included, recomputing only the keys it touched, in one
//! projection transaction:
//!
//! - incarnation scopes ([`RegistrationCollection::create_scope`]) die when the owner
//!   handle is closed or dropped; writers do not own the scope's lifetime;
//! - installation scopes die on [`RegistrationCollection::uninstall`];
//! - permanent scopes are installation scopes that have no uninstall path.
//!
//! A scope minted with [`RegistrationCollection::create_staged_scope`] belongs to an
//! initializing incarnation (ADR 024 §5). Its candidates receive stamps but are
//! invisible to readers and to other scopes until
//! [`RegistrationCollection::commit_scopes`] publishes the named scopes together.
//! Closing a staged scope discards its candidates unseen.
//!
//! Every mutation commits under the collection lock and returns a
//! [`RegistrationDispatch`]: the reader wakes and the retired values it owes. The
//! caller dispatches it after releasing its own locks.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Waker;

use crate::projection::Projection;
use crate::projection::ProjectionDispatch;
use crate::source::Source;
use crate::types::Change;
use crate::types::MutationError;
use crate::types::RegistrationScopeId;
use crate::types::RowEvidence;
use crate::types::Stamp;
use crate::types::Stamped;
use crate::types::Version;
use crate::types::WriteBasis;
use crate::types::WriterId;
use durable::Durable;

/// How long a durable scope's candidates live.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retention {
    /// Retracted by [`RegistrationCollection::uninstall`].
    Installation,
    /// Never retracted by lifecycle: not by `uninstall`, owner close, or `reset`.
    Permanent,
}

/// One scope's claim on one key: `slot` is the value, `None` a tombstone.
pub(crate) struct Candidate<V> {
    pub(crate) stamp: Stamp,
    pub(crate) slot: Option<V>,
}

struct ScopeState<K> {
    writer: WriterId,
    /// `None` for incarnation scopes.
    retention: Option<Retention>,
    staged: bool,
    /// Index of the keys this scope holds a candidate for, so retraction visits only
    /// them.
    keys: BTreeSet<K>,
}

struct Inner<K, V, S: Source<K, Stamped<V>>> {
    projection: Projection<K, Stamped<V>, S>,
    next_scope: u64,
    version: Version,
    /// Live scopes; removing a scope also revokes its writers.
    scopes: BTreeMap<RegistrationScopeId, ScopeState<K>>,
    /// Every candidate, staged scopes' included, by key.
    candidates: BTreeMap<K, BTreeMap<RegistrationScopeId, Candidate<V>>>,
    /// Set by the durable constructor; `None` for volatile collections.
    durable: Option<Durable<K, V>>,
    /// Installation scope key to the in-process id of its entry in `scopes`.
    /// These scopes have no owner handle: only `install`/`uninstall` change them.
    installations: BTreeMap<Vec<u8>, RegistrationScopeId>,
    /// Keys with a durable write between its stamp and its publication, and the tasks
    /// waiting to write them next; see `InstalledWriter`.
    inflight: BTreeMap<K, Vec<Waker>>,
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> Inner<K, V, S> {
    fn next_scope_id(&mut self) -> RegistrationScopeId {
        self.next_scope = self
            .next_scope
            .checked_add(1)
            .expect("registration scope identity exhausted");
        RegistrationScopeId(self.next_scope)
    }

    fn next_version(&mut self) -> Version {
        self.version.seq = self
            .version
            .seq
            .checked_add(1)
            .expect("row version exhausted");
        self.version
    }

    fn add_scope(
        &mut self,
        writer: WriterId,
        retention: Option<Retention>,
        staged: bool,
    ) -> RegistrationScopeId {
        let id = self.next_scope_id();
        assert!(
            self.scopes
                .insert(
                    id,
                    ScopeState {
                        writer,
                        retention,
                        staged,
                        keys: BTreeSet::new(),
                    },
                )
                .is_none()
        );
        id
    }

    /// The highest-stamped candidate `viewer` can see: every unstaged scope's, plus the
    /// viewer's own. Equal stamps (two live scopes of one writer) break by scope id.
    fn winner(
        &self,
        key: &K,
        viewer: Option<RegistrationScopeId>,
    ) -> Option<(RegistrationScopeId, &Candidate<V>)> {
        self.candidates
            .get(key)?
            .iter()
            .filter(|(id, _)| Some(**id) == viewer || !self.scopes[*id].staged)
            .max_by(|(a_id, a), (b_id, b)| (&a.stamp, a_id).cmp(&(&b.stamp, b_id)))
            .map(|(id, candidate)| (*id, candidate))
    }

    /// What readers see for `key`: the winning value's identity, `None` when absent.
    fn visible(&self, key: &K) -> Option<(RegistrationScopeId, Stamp)> {
        let (id, candidate) = self.winner(key, None)?;
        candidate.slot.as_ref()?;
        Some((id, candidate.stamp.clone()))
    }

    fn visible_row(&self, key: &K) -> Option<Stamped<V>> {
        let (_, candidate) = self.winner(key, None)?;
        Some(Stamped {
            stamp: candidate.stamp.clone(),
            value: candidate.slot.clone()?,
        })
    }

    /// Stamp a write by scope `id` to `key`, or refuse a failed compare-and-set. Pure:
    /// nothing changes until the caller sets the candidate.
    fn plan_write(
        &self,
        id: RegistrationScopeId,
        key: &K,
        basis: &WriteBasis,
    ) -> Result<Stamp, MutationError> {
        let writer = self
            .scopes
            .get(&id)
            .ok_or(MutationError::Closed)?
            .writer
            .clone();
        // The scope sees its own staged candidates on top of the published ones.
        let winner = self
            .winner(key, Some(id))
            .map(|(_, candidate)| (candidate.stamp.clone(), candidate.slot.is_some()));
        let observed = match basis {
            WriteBasis::Concurrent => 0,
            WriteBasis::Observed(stamp) => stamp.lamport,
            WriteBasis::CompareAndSet(expected) => {
                // A winning tombstone reads as absent: readers never see its stamp.
                let current = winner
                    .as_ref()
                    .filter(|(_, live)| *live)
                    .map(|(stamp, _)| stamp);
                if current != expected.as_ref() {
                    return Err(MutationError::Conflict {
                        current: current.cloned(),
                    });
                }
                // Supersede the tombstone too when the comparison was against absence.
                winner.as_ref().map_or(0, |(stamp, _)| stamp.lamport)
            }
        };
        // A scope's lamport on a key only grows, so its stamps never repeat.
        let own = self
            .candidates
            .get(key)
            .and_then(|holders| holders.get(&id))
            .map_or(0, |candidate| candidate.stamp.lamport);
        Ok(Stamp {
            lamport: observed.max(own).checked_add(1).expect("lamport exhausted"),
            writer,
        })
    }

    fn set_candidate(
        &mut self,
        id: RegistrationScopeId,
        key: K,
        candidate: Candidate<V>,
        retired: &mut Vec<Candidate<V>>,
    ) {
        self.scopes.get_mut(&id).unwrap().keys.insert(key.clone());
        retired.extend(
            self.candidates
                .entry(key)
                .or_default()
                .insert(id, candidate),
        );
    }

    fn take_candidate(&mut self, id: RegistrationScopeId, key: &K) -> Candidate<V> {
        let holders = self.candidates.get_mut(key).unwrap();
        let candidate = holders.remove(&id).expect("scope key index out of sync");
        if holders.is_empty() {
            self.candidates.remove(key);
        }
        candidate
    }

    /// Drop a scope and retract every candidate it holds.
    fn remove_scope(&mut self, id: RegistrationScopeId, retired: &mut Vec<Candidate<V>>) {
        if let Some(state) = self.scopes.remove(&id) {
            for key in &state.keys {
                retired.push(self.take_candidate(id, key));
            }
        }
    }

    /// Run `change` and publish the net effect on `touched` keys as one transaction:
    /// an upsert where the visible winner changed to a value, a remove where it changed
    /// to absent, nothing where readers see the same row. Keys outside `touched` must
    /// not change visibility.
    fn mutate(
        &mut self,
        touched: BTreeSet<K>,
        change: impl FnOnce(&mut Self, &mut Vec<Candidate<V>>),
    ) -> RegistrationDispatch<K, V, S> {
        let before: Vec<_> = touched
            .into_iter()
            .map(|key| {
                let visible = self.visible(&key);
                (key, visible)
            })
            .collect();
        let mut retired = Vec::new();
        change(self, &mut retired);
        let mut changes = Vec::new();
        for (key, before) in before {
            if before == self.visible(&key) {
                continue;
            }
            let row_version = self.next_version();
            changes.push(match self.visible_row(&key) {
                Some(value) => Change::Upsert {
                    key,
                    row_version,
                    value,
                    evidence: RowEvidence::Authoritative,
                    write_context: None,
                },
                None => Change::Remove { key, row_version },
            });
        }
        RegistrationDispatch {
            output: (),
            projection: if changes.is_empty() {
                Vec::new()
            } else {
                vec![self.projection.publish_deferred(changes)]
            },
            retired,
        }
    }
}

/// The authority side of an owner-scoped contribution collection.
pub struct RegistrationCollection<K, V, S: Source<K, Stamped<V>> = ()> {
    inner: Arc<Mutex<Inner<K, V, S>>>,
}

impl<K, V, S: Source<K, Stamped<V>>> std::fmt::Debug for RegistrationCollection<K, V, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationCollection")
            .finish_non_exhaustive()
    }
}

impl<K, V, S: Source<K, Stamped<V>>> Clone for RegistrationCollection<K, V, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Identity equality: two handles are equal when they name the same collection.
impl<K, V, S: Source<K, Stamped<V>>> PartialEq for RegistrationCollection<K, V, S> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<K, V, S: Source<K, Stamped<V>>> Eq for RegistrationCollection<K, V, S> {}

impl<K: Ord + Clone, V: Clone> RegistrationCollection<K, V> {
    pub fn new(runtime_epoch: [u64; 2]) -> Self {
        Self::with_source((), NonZeroUsize::new(256).unwrap(), runtime_epoch)
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> RegistrationCollection<K, V, S> {
    pub fn with_source(source: S, replay_capacity: NonZeroUsize, runtime_epoch: [u64; 2]) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                projection: Projection::with_source(source, replay_capacity, runtime_epoch),
                next_scope: 0,
                version: Version::new(1, 0),
                scopes: BTreeMap::new(),
                candidates: BTreeMap::new(),
                durable: None,
                installations: BTreeMap::new(),
                inflight: BTreeMap::new(),
            })),
        }
    }

    /// Mint an incarnation scope owner whose writes carry `writer`. Closing or
    /// dropping it retracts all its candidates in one transaction; writers minted from
    /// it convey mutation authority without retaining the owner.
    pub fn create_scope(&self, writer: WriterId) -> RegistrationScope<K, V, S> {
        self.mint(writer, false)
    }

    /// Mint a scope whose candidates stay invisible until [`Self::commit_scopes`] names
    /// it. Closing it first discards its candidates unseen.
    pub fn create_staged_scope(&self, writer: WriterId) -> RegistrationScope<K, V, S> {
        self.mint(writer, true)
    }

    /// Publish all candidates of the named staged scopes as one projection transaction.
    /// Closed or already committed scopes are skipped. All owners must belong here.
    ///
    /// Mutation happens under the collection lock; wakes are returned, not fired.
    /// The caller must dispatch the result after releasing host locks.
    pub fn commit_scopes(
        &self,
        scopes: &[&RegistrationScope<K, V, S>],
    ) -> RegistrationDispatch<K, V, S> {
        self.validate_scopes(scopes);
        let mut inner = self.lock();
        let staged: Vec<_> = scopes
            .iter()
            .map(|scope| scope.id)
            .filter(|id| inner.scopes.get(id).is_some_and(|state| state.staged))
            .collect();
        let touched = staged
            .iter()
            .flat_map(|id| inner.scopes[id].keys.iter().cloned())
            .collect();
        inner.mutate(touched, |inner, _| {
            for id in staged {
                inner.scopes.get_mut(&id).unwrap().staged = false;
            }
        })
    }

    /// Retract the named scopes in one transaction. Wakes are returned, not fired;
    /// the caller must dispatch the result after releasing host locks.
    pub fn close_scopes(
        &self,
        scopes: &[&RegistrationScope<K, V, S>],
    ) -> RegistrationDispatch<K, V, S> {
        self.validate_scopes(scopes);
        let mut inner = self.lock();
        let touched = scopes
            .iter()
            .filter_map(|scope| inner.scopes.get(&scope.id))
            .flat_map(|state| state.keys.iter().cloned())
            .collect();
        inner.mutate(touched, |inner, retired| {
            for scope in scopes {
                inner.remove_scope(scope.id, retired);
            }
        })
    }

    fn validate_scopes(&self, scopes: &[&RegistrationScope<K, V, S>]) {
        assert!(
            scopes.iter().all(|scope| self == &scope.collection),
            "registration scope belongs to another collection"
        );
    }

    fn mint(&self, writer: WriterId, staged: bool) -> RegistrationScope<K, V, S> {
        let id = self.lock().add_scope(writer, None, staged);
        RegistrationScope {
            id,
            collection: self.clone(),
        }
    }

    /// The read waist over this collection's winners.
    pub fn projection(&self) -> Projection<K, Stamped<V>, S> {
        self.lock().projection.clone()
    }

    /// The published winner of `key`; staged candidates are excluded.
    pub fn get(&self, key: &K) -> Option<Stamped<V>> {
        self.lock().visible_row(key)
    }

    /// Host-local snapshot of every published winner. Consumers use projection leases
    /// rather than this authority convenience.
    pub fn snapshot(&self) -> BTreeMap<K, Stamped<V>> {
        let inner = self.lock();
        inner
            .candidates
            .keys()
            .filter_map(|key| Some((key.clone(), inner.visible_row(key)?)))
            .collect()
    }

    /// Retract every incarnation scope and begin a new projection generation:
    /// outstanding leases report `ResetRequired`; old writers remain closed after
    /// re-bootstrap. Installation and permanent scopes survive and their winners are
    /// republished into the new generation. Wakes are returned, not fired; the caller
    /// must dispatch the result after releasing host locks.
    pub fn reset(&self) -> RegistrationDispatch<K, V, S> {
        let mut inner = self.lock();
        inner.version = Version::new(
            inner
                .version
                .incarnation
                .checked_add(1)
                .expect("registration generation exhausted"),
            0,
        );
        let mut retired = Vec::new();
        let doomed: Vec<_> = inner
            .scopes
            .iter()
            .filter(|(_, state)| state.retention.is_none())
            .map(|(id, _)| *id)
            .collect();
        for id in doomed {
            inner.remove_scope(id, &mut retired);
        }
        let mut projection = vec![inner.projection.reset_deferred()];
        let keys: Vec<_> = inner.candidates.keys().cloned().collect();
        let mut republished = Vec::new();
        for key in keys {
            if let Some(value) = inner.visible_row(&key) {
                republished.push(Change::Upsert {
                    key,
                    row_version: inner.next_version(),
                    value,
                    evidence: RowEvidence::Authoritative,
                    write_context: None,
                });
            }
        }
        if !republished.is_empty() {
            projection.push(inner.projection.publish_deferred(republished));
        }
        RegistrationDispatch {
            output: (),
            projection,
            retired,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V, S>> {
        self.inner.lock().expect("registration state poisoned")
    }
}

pub struct RegistrationScope<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>> = ()> {
    id: RegistrationScopeId,
    collection: RegistrationCollection<K, V, S>,
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> RegistrationScope<K, V, S> {
    pub fn id(&self) -> RegistrationScopeId {
        self.id
    }

    pub fn writer(&self) -> Result<RegistrationWriter<K, V, S>, MutationError> {
        if !self.collection.lock().scopes.contains_key(&self.id) {
            return Err(MutationError::Closed);
        }
        Ok(RegistrationWriter {
            id: self.id,
            collection: self.collection.clone(),
        })
    }

    /// Retract this scope's candidates. Wakes are returned, not fired; the caller must
    /// dispatch the result after releasing host locks. Dropping the scope closes and
    /// dispatches immediately.
    pub fn close(&self) -> RegistrationDispatch<K, V, S> {
        self.collection.close_scopes(&[self])
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> Drop for RegistrationScope<K, V, S> {
    /// Scope-owner closure retracts its candidates, even if mutation handles remain
    /// alive. Staged candidates leave authority state without reaching a projection.
    fn drop(&mut self) {
        self.close().dispatch();
    }
}

/// A scope's mutation handle, not its owner. Writes are stamped with the scope's
/// [`WriterId`] and a lamport derived from the [`WriteBasis`]; only
/// [`WriteBasis::CompareAndSet`] can be refused.
pub struct RegistrationWriter<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>> = ()> {
    id: RegistrationScopeId,
    collection: RegistrationCollection<K, V, S>,
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> std::fmt::Debug
    for RegistrationWriter<K, V, S>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationWriter")
            .field("scope_id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> RegistrationWriter<K, V, S> {
    pub fn scope_id(&self) -> RegistrationScopeId {
        self.id
    }

    /// Set this scope's candidate for `key` to `value`, replacing its previous one,
    /// and return the new stamp. Wakes are returned, not fired; the caller must
    /// dispatch after releasing host locks.
    pub fn put(
        &self,
        key: K,
        basis: WriteBasis,
        value: V,
    ) -> Result<RegistrationDispatch<K, V, S, Stamp>, MutationError> {
        self.write(key, basis, Some(value))
    }

    /// Set this scope's candidate for `key` to a tombstone. It hides lower-stamped
    /// candidates until this scope retracts it, at which point they resurface.
    pub fn delete(
        &self,
        key: K,
        basis: WriteBasis,
    ) -> Result<RegistrationDispatch<K, V, S, Stamp>, MutationError> {
        self.write(key, basis, None)
    }

    fn write(
        &self,
        key: K,
        basis: WriteBasis,
        slot: Option<V>,
    ) -> Result<RegistrationDispatch<K, V, S, Stamp>, MutationError> {
        let mut inner = self.collection.lock();
        let stamp = inner.plan_write(self.id, &key, &basis)?;
        let id = self.id;
        let candidate = Candidate {
            stamp: stamp.clone(),
            slot,
        };
        let dispatch = inner.mutate(BTreeSet::from([key.clone()]), |inner, retired| {
            inner.set_candidate(id, key, candidate, retired);
        });
        drop(inner);
        Ok(dispatch.with_output(stamp))
    }
}

/// The wakes and deferred drops a committed mutation owes, plus the mutation's own
/// output (`T`, e.g. the new stamp). The mutation is already committed when this
/// exists; [`Self::dispatch`] fires the wakes and returns the output.
#[must_use = "dispatch after releasing host authority locks"]
pub struct RegistrationDispatch<K, V, S: Source<K, Stamped<V>> = (), T = ()> {
    output: T,
    /// Usually one transaction; a reset also republishes the surviving scopes' winners
    /// into the new generation.
    projection: Vec<ProjectionDispatch<K, Stamped<V>, S::Selector>>,
    /// Candidates the mutation retracted; their values drop in [`Self::dispatch`],
    /// outside every lock.
    retired: Vec<Candidate<V>>,
}

impl<K, V, S: Source<K, Stamped<V>>, T> std::fmt::Debug for RegistrationDispatch<K, V, S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationDispatch")
            .finish_non_exhaustive()
    }
}

impl<K, V, S: Source<K, Stamped<V>>> RegistrationDispatch<K, V, S> {
    fn with_output<T>(self, output: T) -> RegistrationDispatch<K, V, S, T> {
        RegistrationDispatch {
            output,
            projection: self.projection,
            retired: self.retired,
        }
    }
}

impl<K, V, S: Source<K, Stamped<V>>, T> RegistrationDispatch<K, V, S, T> {
    /// The mutation's output, readable before the wakes fire.
    pub fn output(&self) -> &T {
        &self.output
    }

    /// Fire the wakes and drop the retracted values, then return the mutation's
    /// output. Call after releasing host locks.
    pub fn dispatch(self) -> T {
        for projection in self.projection {
            projection.dispatch();
        }
        drop(self.retired);
        self.output
    }
}

#[path = "registration_durable.rs"]
mod durable;
pub use durable::DurableCodec;
pub use durable::InstalledWriteError;
pub use durable::InstalledWriter;

#[cfg(test)]
#[path = "registration_tests_notifications.rs"]
mod tests;

#[cfg(test)]
#[path = "registration_tests_semantics.rs"]
mod semantics_tests;

#[cfg(test)]
#[path = "registration_tests_durable.rs"]
mod durable_tests;
