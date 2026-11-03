//! Owner-scoped contribution registrations (ADR 017 §9 and ADR 025 §4).
//!
//! A [`RegistrationCollection`] mints host-owned [`RegistrationScope`]s. Rows are
//! physically keyed by `(RegistrationScopeId, local key)`. Writers share the scope's
//! compare-and-set state, but do not retain its owner: dropping a writer preserves rows,
//! while closing or dropping the scope withdraws its rows and invalidates every writer.
//!
//! A scope minted with [`RegistrationCollection::create_staged_scope`] belongs to an
//! initializing incarnation (ADR 024 §5). Its mutations receive authoritative versions
//! but remain invisible until [`RegistrationCollection::commit_scopes`] publishes the
//! named scopes together. Closing a staged scope discards its rows unseen.
//!
//! Reads use the collection's [`Projection`], whose rows are the collection's explicit
//! [`Materializer`] applied to the scopes' contributions. Under
//! [`Materializer::Union`] the key is the `(RegistrationScopeId, K)` pair (a byte-keyed
//! waist supplies an injective encoding of it); every other policy keys by `K`.
//! Every mutation, scope close and (re)installation recomputes the affected materialized
//! keys inside its own single projection transaction.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use crate::projection::Projection;
use crate::projection::ProjectionDispatch;
use crate::replica::Entry;
use crate::source::Source;
use crate::types::MutationError;
use crate::types::RegistrationScopeId;
use crate::types::RowEvidence;
use crate::types::Version;
use clay_store::StoreError;
use durable::Durable;

struct ScopeState<K, V> {
    rows: BTreeMap<K, Entry<V>>,
    staged: bool,
    /// Precedence under [`Materializer::Priority`]; `None` under every other policy.
    rank: Option<ScopeRank>,
    /// The stable installation key of an installation scope; `None` for incarnation
    /// scopes. Orders [`Materializer::Group`] contributions independently of the
    /// in-process scope id, which is not stable across restarts.
    install_key: Option<Vec<u8>>,
}

struct Inner<K, V, P, S: Source<P, V>> {
    projection: Projection<P, V, S>,
    materializer: Materializer<K, V, P>,
    next_scope: u64,
    version: Version,
    /// Local rows per live scope; removing the scope also revokes its writers.
    scopes: BTreeMap<RegistrationScopeId, ScopeState<K, V>>,
    /// Set by the durable constructor; `None` for volatile collections.
    durable: Option<Durable<K, V>>,
    /// Installation scope key to the in-process id of its entry in `scopes`.
    /// These scopes have no owner handle: only `install`/`uninstall` change them.
    installations: BTreeMap<Vec<u8>, RegistrationScopeId>,
}

impl<K, V, P, S: Source<P, V>> Inner<K, V, P, S> {
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
}

/// The authority side of an owner-scoped contribution collection.
pub struct RegistrationCollection<K, V, P = (RegistrationScopeId, K), S: Source<P, V> = ()> {
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

/// Identity equality: two handles are equal when they name the same collection.
impl<K, V, P, S: Source<P, V>> PartialEq for RegistrationCollection<K, V, P, S> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<K, V, P, S: Source<P, V>> Eq for RegistrationCollection<K, V, P, S> {}

impl<K: Ord + Clone, V: Clone> RegistrationCollection<K, V> {
    /// A [`Materializer::Union`] collection with the tuple physical key.
    pub fn new(runtime_epoch: [u64; 2]) -> Self {
        Self::with_source(
            (),
            NonZeroUsize::new(256).unwrap(),
            runtime_epoch,
            |scope, key| (scope, key.clone()),
        )
    }
}

/// Why [`RegistrationCollection::install`] refused: the store failed, or the rows would
/// break a [`Materializer::RejectCollision`] collection. Neither leaves a visible change.
#[derive(Debug)]
pub enum InstallError {
    Store(StoreError),
    /// A row's key is already held by `holder`.
    Collision {
        holder: RegistrationScopeId,
    },
}

impl From<StoreError> for InstallError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationCollection<K, V, P, S> {
    /// A [`Materializer::Union`] collection: every scope's rows side by side under the
    /// injective `physical` key. Use [`Self::with_materializer`] for any other policy.
    pub fn with_source(
        source: S,
        replay_capacity: NonZeroUsize,
        runtime_epoch: [u64; 2],
        physical: fn(RegistrationScopeId, &K) -> P,
    ) -> Self {
        Self::with_materializer(
            source,
            replay_capacity,
            runtime_epoch,
            Materializer::Union { key: physical },
        )
    }

    /// A collection whose projection is `materializer` applied to its scopes.
    pub fn with_materializer(
        source: S,
        replay_capacity: NonZeroUsize,
        runtime_epoch: [u64; 2],
        materializer: Materializer<K, V, P>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                projection: Projection::with_source(source, replay_capacity, runtime_epoch),
                materializer,
                next_scope: 0,
                version: Version::new(1, 0),
                scopes: BTreeMap::new(),
                durable: None,
                installations: BTreeMap::new(),
            })),
        }
    }

    /// Mint a scope owner. Dropping it withdraws all its rows in one transaction;
    /// writers minted from it convey mutation authority without retaining the owner.
    /// Panics under [`Materializer::Priority`], which needs [`Self::create_ranked_scope`].
    pub fn create_scope(&self) -> RegistrationScope<K, V, P, S> {
        self.mint(false, None)
    }

    /// Mint a scope whose rows stay out of the projection until
    /// [`Self::commit_scopes`] names it. Closing it first discards its rows unseen.
    /// Panics under [`Materializer::Priority`], which needs
    /// [`Self::create_ranked_staged_scope`].
    pub fn create_staged_scope(&self) -> RegistrationScope<K, V, P, S> {
        self.mint(true, None)
    }

    /// [`Self::create_scope`] for a [`Materializer::Priority`] collection. Panics if
    /// another live scope holds `rank`, or under any other policy.
    pub fn create_ranked_scope(&self, rank: ScopeRank) -> RegistrationScope<K, V, P, S> {
        self.mint(false, Some(rank))
    }

    /// [`Self::create_staged_scope`] for a [`Materializer::Priority`] collection.
    pub fn create_ranked_staged_scope(&self, rank: ScopeRank) -> RegistrationScope<K, V, P, S> {
        self.mint(true, Some(rank))
    }

    /// Publish all rows in the named staged scopes as one projection transaction.
    /// Closed or already committed scopes are skipped. All owners must belong here.
    ///
    /// Mutation happens under the collection lock; wakes are returned, not fired.
    /// The caller must dispatch the result after releasing host locks.
    pub fn commit_scopes(
        &self,
        scopes: &[&RegistrationScope<K, V, P, S>],
    ) -> RegistrationDispatch<K, V, P, S> {
        self.validate_scopes(scopes);
        let mut inner = self.lock();
        let keys: BTreeSet<K> = scopes
            .iter()
            .filter_map(|scope| inner.scopes.get(&scope.id))
            .filter(|state| state.staged)
            .flat_map(|state| state.rows.keys().cloned())
            .collect();
        let ((), projection) = inner.commit(&keys, None, |inner| {
            for scope in scopes {
                if let Some(state) = inner.scopes.get_mut(&scope.id) {
                    state.staged = false;
                }
            }
        });
        let dispatch = RegistrationDispatch {
            output: (),
            projection,
            rows: Vec::new(),
            entries: Vec::new(),
            keys: Vec::new(),
        };
        drop(inner);
        dispatch
    }

    /// Withdraw the named scopes in one transaction. Wakes are returned, not fired;
    /// the caller must dispatch the result after releasing host locks.
    pub fn close_scopes(
        &self,
        scopes: &[&RegistrationScope<K, V, P, S>],
    ) -> RegistrationDispatch<K, V, P, S> {
        self.validate_scopes(scopes);
        let mut inner = self.lock();
        let keys: BTreeSet<K> = scopes
            .iter()
            .filter_map(|scope| inner.scopes.get(&scope.id))
            .filter(|state| !state.staged)
            .flat_map(|state| state.rows.keys().cloned())
            .collect();
        let (rows, projection) = inner.commit(&keys, None, |inner| {
            scopes
                .iter()
                .filter_map(|scope| inner.scopes.remove(&scope.id))
                .map(|state| state.rows)
                .collect::<Vec<_>>()
        });
        let dispatch = RegistrationDispatch {
            output: (),
            projection,
            rows,
            entries: Vec::new(),
            keys: Vec::new(),
        };
        drop(inner);
        dispatch
    }

    fn validate_scopes(&self, scopes: &[&RegistrationScope<K, V, P, S>]) {
        assert!(
            scopes.iter().all(|scope| self == &scope.collection),
            "registration scope belongs to another collection"
        );
    }

    fn mint(&self, staged: bool, rank: Option<ScopeRank>) -> RegistrationScope<K, V, P, S> {
        let mut inner = self.lock();
        let rank = inner.checked_rank(rank, None);
        let id = inner.next_scope_id();
        assert!(
            inner
                .scopes
                .insert(
                    id,
                    ScopeState {
                        rows: BTreeMap::new(),
                        staged,
                        rank,
                        install_key: None,
                    }
                )
                .is_none()
        );
        RegistrationScope {
            id,
            collection: self.clone(),
        }
    }

    /// The read waist over this collection's materialized rows.
    pub fn projection(&self) -> Projection<P, V, S> {
        self.lock().projection.clone()
    }

    /// Host-local snapshot of the materialized rows of published scopes. Staged scope
    /// rows are excluded; consumers use projection leases rather than this authority
    /// convenience. Under [`Materializer::Union`] a row's version is its own; under
    /// other policies it is the newest contributing version.
    pub fn snapshot(&self) -> BTreeMap<P, Entry<V>> {
        self.lock().materialized()
    }

    /// Withdraw every scope and begin a new projection generation: outstanding
    /// leases report `ResetRequired`; old writers remain closed after re-bootstrap.
    /// Wakes are returned, not fired; the caller must dispatch the result after
    /// releasing host locks.
    pub fn reset(&self) -> RegistrationDispatch<K, V, P, S> {
        let mut inner = self.lock();
        inner.version = Version::new(
            inner
                .version
                .incarnation
                .checked_add(1)
                .expect("registration generation exhausted"),
            0,
        );
        // Installation scopes are durable: they survive the generation change and
        // are republished into it. Everything else is withdrawn.
        let installed: BTreeSet<RegistrationScopeId> =
            inner.installations.values().copied().collect();
        let mut rows = Vec::new();
        let mut retained = BTreeMap::new();
        for (id, state) in std::mem::take(&mut inner.scopes) {
            if installed.contains(&id) {
                retained.insert(id, state);
            } else {
                rows.push(state.rows);
            }
        }
        for state in retained.values_mut() {
            for entry in state.rows.values_mut() {
                entry.version = inner.next_version();
            }
        }
        inner.scopes = retained;
        let mut projection = vec![inner.projection.reset_deferred()];
        let republished = inner.full_changes();
        if !republished.is_empty() {
            projection.push(inner.projection.publish_deferred(republished));
        }
        let dispatch = RegistrationDispatch {
            output: (),
            projection,
            rows,
            entries: Vec::new(),
            keys: Vec::new(),
        };
        drop(inner);
        dispatch
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V, P, S>> {
        self.inner.lock().expect("registration state poisoned")
    }
}

pub struct RegistrationScope<
    K: Ord + Clone,
    V: Clone,
    P: Ord + Clone = (RegistrationScopeId, K),
    S: Source<P, V> = (),
> {
    id: RegistrationScopeId,
    collection: RegistrationCollection<K, V, P, S>,
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationScope<K, V, P, S> {
    pub fn id(&self) -> RegistrationScopeId {
        self.id
    }

    pub fn writer(&self) -> Result<RegistrationWriter<K, V, P, S>, MutationError> {
        if !self.collection.lock().scopes.contains_key(&self.id) {
            return Err(MutationError::Closed);
        }
        Ok(RegistrationWriter {
            id: self.id,
            collection: self.collection.clone(),
        })
    }

    /// Withdraw this scope's rows. Wakes are returned, not fired; the caller must
    /// dispatch the result after releasing host locks. Dropping the scope closes
    /// and dispatches immediately.
    pub fn close(&self) -> RegistrationDispatch<K, V, P, S> {
        self.collection.close_scopes(&[self])
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> Drop
    for RegistrationScope<K, V, P, S>
{
    /// Scope-owner closure withdraws its rows, even if mutation handles remain alive.
    /// Staged rows leave authority state without ever reaching a projection.
    fn drop(&mut self) {
        self.close().dispatch();
    }
}

/// A scope's mutation handle, not its owner. `based_on` must equal the current row
/// version (`None` means absent); mismatches report the actual version for retry.
pub struct RegistrationWriter<
    K: Ord + Clone,
    V: Clone,
    P: Ord + Clone = (RegistrationScopeId, K),
    S: Source<P, V> = (),
> {
    id: RegistrationScopeId,
    collection: RegistrationCollection<K, V, P, S>,
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> std::fmt::Debug
    for RegistrationWriter<K, V, P, S>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationWriter")
            .field("scope_id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationWriter<K, V, P, S> {
    pub fn scope_id(&self) -> RegistrationScopeId {
        self.id
    }

    pub fn version(&self, key: &K) -> Option<Version> {
        self.collection
            .lock()
            .scopes
            .get(&self.id)
            .and_then(|state| state.rows.get(key))
            .map(|entry| entry.version)
    }

    pub fn row_count(&self) -> usize {
        self.collection
            .lock()
            .scopes
            .get(&self.id)
            .map_or(0, |state| state.rows.len())
    }

    /// Register or replace `key`, returning the new row version. Under
    /// [`Materializer::RejectCollision`] a key another live scope holds is refused with
    /// [`MutationError::Collision`]. Wakes are returned, not fired; the caller must
    /// dispatch after releasing host locks.
    pub fn put(
        &self,
        key: K,
        based_on: Option<Version>,
        value: V,
    ) -> Result<RegistrationDispatch<K, V, P, S, Version>, MutationError> {
        let mut inner = self.collection.lock();
        let state = inner.scopes.get(&self.id).ok_or(MutationError::Closed)?;
        let current = state.rows.get(&key).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        if inner.rejects_collisions()
            && let Some(holder) = inner.holder(&key, Some(self.id))
        {
            return Err(MutationError::Collision { holder });
        }
        let version = inner.next_version();
        let touched = BTreeSet::from([key.clone()]);
        let (previous, projection) = inner.commit(&touched, None, |inner| {
            let rows = &mut inner.scopes.get_mut(&self.id).unwrap().rows;
            let previous = rows.remove_entry(&key);
            rows.insert(
                key,
                Entry {
                    version,
                    value,
                    evidence: RowEvidence::Authoritative,
                    write_context: None,
                },
            );
            previous
        });
        let dispatch = RegistrationDispatch {
            projection,
            entries: previous.into_iter().collect(),
            ..RegistrationDispatch::default()
        };
        drop(inner);
        Ok(dispatch.with_output(version))
    }

    /// Withdraw `key`, returning its removal version. An absent row with no claimed
    /// version is an idempotent no-op and returns `None` without publication.
    /// Wakes are returned, not fired; the caller must dispatch after releasing host
    /// locks.
    pub fn remove(
        &self,
        key: K,
        based_on: Option<Version>,
    ) -> Result<RegistrationDispatch<K, V, P, S, Option<Version>>, MutationError> {
        let mut inner = self.collection.lock();
        let state = inner.scopes.get(&self.id).ok_or(MutationError::Closed)?;
        let current = state.rows.get(&key).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        if current.is_none() {
            drop(inner);
            return Ok(RegistrationDispatch {
                keys: vec![key],
                ..RegistrationDispatch::default()
            }
            .with_output(None));
        }
        let version = inner.next_version();
        let touched = BTreeSet::from([key]);
        let (previous, projection) = inner.commit(&touched, Some(version), |inner| {
            let key = touched.first().unwrap();
            inner
                .scopes
                .get_mut(&self.id)
                .unwrap()
                .rows
                .remove_entry(key)
                .unwrap()
        });
        let dispatch = RegistrationDispatch {
            projection,
            entries: vec![previous],
            keys: touched.into_iter().collect(),
            ..RegistrationDispatch::default()
        };
        drop(inner);
        Ok(dispatch.with_output(Some(version)))
    }

    /// Withdraw every row of this scope. Wakes are returned, not fired; the caller
    /// must dispatch after releasing host locks.
    pub fn clear(&self) -> Result<RegistrationDispatch<K, V, P, S>, MutationError> {
        let mut inner = self.collection.lock();
        let state = inner.scopes.get(&self.id).ok_or(MutationError::Closed)?;
        let touched: BTreeSet<K> = state.rows.keys().cloned().collect();
        let (rows, projection) = inner.commit(&touched, None, |inner| {
            std::mem::take(&mut inner.scopes.get_mut(&self.id).unwrap().rows)
        });
        let dispatch = RegistrationDispatch {
            projection,
            rows: vec![rows],
            ..RegistrationDispatch::default()
        };
        drop(inner);
        Ok(dispatch)
    }
}

/// The wakes and deferred drops a committed mutation owes, plus the mutation's own
/// output (`T`, e.g. the new row version). The mutation is already committed when
/// this exists; [`Self::dispatch`] fires the wakes and returns the output.
#[must_use = "dispatch after releasing host authority locks"]
pub struct RegistrationDispatch<K, V, P = (RegistrationScopeId, K), S: Source<P, V> = (), T = ()> {
    output: T,
    /// Usually one transaction; a reset of a durable collection also republishes
    /// its installation rows into the new generation.
    projection: Vec<ProjectionDispatch<P, V, S::Selector>>,
    rows: Vec<BTreeMap<K, Entry<V>>>,
    entries: Vec<(K, Entry<V>)>,
    keys: Vec<K>,
}

impl<K, V, P, S: Source<P, V>> Default for RegistrationDispatch<K, V, P, S> {
    fn default() -> Self {
        Self {
            output: (),
            projection: Vec::new(),
            rows: Vec::new(),
            entries: Vec::new(),
            keys: Vec::new(),
        }
    }
}

impl<K, V, P, S: Source<P, V>, T> std::fmt::Debug for RegistrationDispatch<K, V, P, S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationDispatch")
            .finish_non_exhaustive()
    }
}

impl<K, V, P, S: Source<P, V>> RegistrationDispatch<K, V, P, S> {
    fn with_output<T>(self, output: T) -> RegistrationDispatch<K, V, P, S, T> {
        RegistrationDispatch {
            output,
            projection: self.projection,
            rows: self.rows,
            entries: self.entries,
            keys: self.keys,
        }
    }
}

impl<K, V, P, S: Source<P, V>, T> RegistrationDispatch<K, V, P, S, T> {
    /// The mutation's output, readable before the wakes fire.
    pub fn output(&self) -> &T {
        &self.output
    }

    /// Fire the wakes and drop the withdrawn rows, then return the mutation's output.
    /// Call after releasing host locks.
    pub fn dispatch(self) -> T {
        for projection in self.projection {
            projection.dispatch();
        }
        self.output
    }
}

#[path = "registration_durable.rs"]
mod durable;
pub use durable::DurableCodec;

#[path = "registration_materialize.rs"]
mod materialize;
pub use materialize::Contribution;
pub use materialize::Materializer;
pub use materialize::ScopeRank;

#[cfg(test)]
#[path = "registration_tests_notifications.rs"]
mod tests;

#[cfg(test)]
#[path = "registration_tests_semantics.rs"]
mod semantics_tests;

#[cfg(test)]
#[path = "registration_tests_durable.rs"]
mod durable_tests;

#[cfg(test)]
#[path = "registration_tests_materialize.rs"]
mod materialize_tests;
