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
//! Reads use the collection's [`Projection`]. The default physical key is the
//! `(RegistrationScopeId, K)` tuple; a byte-keyed waist supplies an injective encoding
//! of that pair instead.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use crate::projection::Projection;
use crate::projection::ProjectionDispatch;
use crate::replica::Entry;
use crate::source::Source;
use crate::types::Change;
use crate::types::MutationError;
use crate::types::RegistrationScopeId;
use crate::types::RowEvidence;
use crate::types::Version;

struct ScopeState<K, V> {
    rows: BTreeMap<K, Entry<V>>,
    staged: bool,
}

struct Inner<K, V, P, S: Source<P, V>> {
    projection: Projection<P, V, S>,
    physical: fn(RegistrationScopeId, &K) -> P,
    next_scope: u64,
    version: Version,
    /// Local rows per live scope; removing the scope also revokes its writers.
    scopes: BTreeMap<RegistrationScopeId, ScopeState<K, V>>,
}

impl<K, V, P, S: Source<P, V>> Inner<K, V, P, S> {
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
    pub fn new(runtime_epoch: [u64; 2]) -> Self {
        Self::with_source(
            (),
            NonZeroUsize::new(256).unwrap(),
            runtime_epoch,
            |scope, key| (scope, key.clone()),
        )
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationCollection<K, V, P, S> {
    pub fn with_source(
        source: S,
        replay_capacity: NonZeroUsize,
        runtime_epoch: [u64; 2],
        physical: fn(RegistrationScopeId, &K) -> P,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                projection: Projection::with_source(source, replay_capacity, runtime_epoch),
                physical,
                next_scope: 0,
                version: Version::new(1, 0),
                scopes: BTreeMap::new(),
            })),
        }
    }

    /// Mint a scope owner. Dropping it withdraws all its rows in one transaction;
    /// writers minted from it convey mutation authority without retaining the owner.
    pub fn create_scope(&self) -> RegistrationScope<K, V, P, S> {
        self.mint(false)
    }

    /// Mint a scope whose rows stay out of the projection until
    /// [`Self::commit_scopes`] names it. Closing it first discards its rows unseen.
    pub fn create_staged_scope(&self) -> RegistrationScope<K, V, P, S> {
        self.mint(true)
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
        let physical = inner.physical;
        let mut changes = Vec::new();
        for scope in scopes {
            let Some(state) = inner.scopes.get_mut(&scope.id) else {
                continue;
            };
            if !state.staged {
                continue;
            }
            state.staged = false;
            for (key, entry) in &state.rows {
                changes.push(Change::Upsert {
                    key: physical(scope.id, key),
                    row_version: entry.version,
                    value: entry.value.clone(),
                    evidence: entry.evidence,
                    write_context: entry.write_context.clone(),
                });
            }
        }
        let dispatch = RegistrationDispatch {
            output: (),
            projection: if changes.is_empty() {
                None
            } else {
                Some(inner.projection.publish_deferred(changes))
            },
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
        let mut dispatch = RegistrationDispatch::default();
        let mut changes = Vec::new();
        for scope in scopes {
            if let Some(state) = inner.scopes.remove(&scope.id) {
                if !state.staged {
                    for key in state.rows.keys() {
                        changes.push(Change::Remove {
                            key: (inner.physical)(scope.id, key),
                            row_version: inner.next_version(),
                        });
                    }
                }
                dispatch.rows.push(state.rows);
            }
        }
        if !changes.is_empty() {
            dispatch.projection = Some(inner.projection.publish_deferred(changes));
        }
        drop(inner);
        dispatch
    }

    fn validate_scopes(&self, scopes: &[&RegistrationScope<K, V, P, S>]) {
        assert!(
            scopes.iter().all(|scope| self == &scope.collection),
            "registration scope belongs to another collection"
        );
    }

    fn mint(&self, staged: bool) -> RegistrationScope<K, V, P, S> {
        let mut inner = self.lock();
        inner.next_scope = inner
            .next_scope
            .checked_add(1)
            .expect("registration scope identity exhausted");
        let id = RegistrationScopeId(inner.next_scope);
        assert!(
            inner
                .scopes
                .insert(
                    id,
                    ScopeState {
                        rows: BTreeMap::new(),
                        staged
                    }
                )
                .is_none()
        );
        RegistrationScope {
            id,
            collection: self.clone(),
        }
    }

    /// The read waist over this collection's rows, keyed by physical key.
    pub fn projection(&self) -> Projection<P, V, S> {
        self.lock().projection.clone()
    }

    /// Host-local snapshot of published rows. Staged scope rows are excluded;
    /// consumers use projection leases rather than this authority convenience.
    pub fn snapshot(&self) -> BTreeMap<P, Entry<V>> {
        let inner = self.lock();
        inner
            .scopes
            .iter()
            .filter(|(_, state)| !state.staged)
            .flat_map(|(id, state)| {
                state
                    .rows
                    .iter()
                    .map(|(key, entry)| ((inner.physical)(*id, key), entry.clone()))
            })
            .collect()
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
        let rows = std::mem::take(&mut inner.scopes)
            .into_values()
            .map(|state| state.rows)
            .collect();
        let dispatch = RegistrationDispatch {
            output: (),
            projection: Some(inner.projection.reset_deferred()),
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

    /// Register or replace `key`, returning the new row version. Wakes are returned,
    /// not fired; the caller must dispatch after releasing host locks.
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
        let staged = state.staged;
        let version = inner.next_version();
        let change = if staged {
            None
        } else {
            Some(Change::Upsert {
                key: (inner.physical)(self.id, &key),
                row_version: version,
                value: value.clone(),
                evidence: RowEvidence::Authoritative,
                write_context: None,
            })
        };
        let rows = &mut inner.scopes.get_mut(&self.id).unwrap().rows;
        let mut dispatch = RegistrationDispatch::default();
        if let Some(previous) = rows.remove_entry(&key) {
            dispatch.entries.push(previous);
        }
        assert!(
            rows.insert(
                key,
                Entry {
                    version,
                    value,
                    evidence: RowEvidence::Authoritative,
                    write_context: None
                }
            )
            .is_none()
        );
        if let Some(change) = change {
            dispatch.projection = Some(inner.projection.publish_deferred(vec![change]));
        }
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
        let state = inner
            .scopes
            .get_mut(&self.id)
            .ok_or(MutationError::Closed)?;
        let current = state.rows.get(&key).map(|entry| entry.version);
        if current != based_on {
            return Err(MutationError::Conflict { current });
        }
        let mut dispatch = RegistrationDispatch::default();
        let Some(previous) = state.rows.remove_entry(&key) else {
            dispatch.keys.push(key);
            drop(inner);
            return Ok(dispatch.with_output(None));
        };
        let staged = state.staged;
        dispatch.entries.push(previous);
        let version = inner.next_version();
        if !staged {
            dispatch.projection = Some(inner.projection.publish_deferred(vec![Change::Remove {
                key: (inner.physical)(self.id, &key),
                row_version: version,
            }]));
        }
        dispatch.keys.push(key);
        drop(inner);
        Ok(dispatch.with_output(Some(version)))
    }

    /// Withdraw every row of this scope. Wakes are returned, not fired; the caller
    /// must dispatch after releasing host locks.
    pub fn clear(&self) -> Result<RegistrationDispatch<K, V, P, S>, MutationError> {
        let mut inner = self.collection.lock();
        let state = inner
            .scopes
            .get_mut(&self.id)
            .ok_or(MutationError::Closed)?;
        let rows = std::mem::take(&mut state.rows);
        let staged = state.staged;
        let mut dispatch = RegistrationDispatch::default();
        if !staged && !rows.is_empty() {
            let changes = rows
                .keys()
                .map(|key| Change::Remove {
                    key: (inner.physical)(self.id, key),
                    row_version: inner.next_version(),
                })
                .collect();
            dispatch.projection = Some(inner.projection.publish_deferred(changes));
        }
        dispatch.rows.push(rows);
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
    projection: Option<ProjectionDispatch<P, V, S::Selector>>,
    rows: Vec<BTreeMap<K, Entry<V>>>,
    entries: Vec<(K, Entry<V>)>,
    keys: Vec<K>,
}

impl<K, V, P, S: Source<P, V>> Default for RegistrationDispatch<K, V, P, S> {
    fn default() -> Self {
        Self {
            output: (),
            projection: None,
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
        if let Some(projection) = self.projection {
            projection.dispatch();
        }
        self.output
    }
}

#[cfg(test)]
#[path = "registration_tests_notifications.rs"]
mod tests;

#[cfg(test)]
#[path = "registration_tests_semantics.rs"]
mod semantics_tests;
