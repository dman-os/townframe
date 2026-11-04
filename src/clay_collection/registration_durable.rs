//! Durable installation and permanent scopes over an [`OrderedStore`] (ADR 031 §§2
//! and 4).
//!
//! A durable [`RegistrationCollection`] holds two kinds of scope. Incarnation scopes
//! (`create_scope`) are volatile and never touch the store. Installation and permanent
//! scopes are keyed by caller-chosen stable bytes (e.g. the owning micro id), have no
//! owner handle and no runtime writers, and are replaced whole by
//! [`RegistrationCollection::install`]; only installation scopes can be removed, by
//! [`RegistrationCollection::uninstall`]. Both persist first and only then change memory
//! and the projection.
//!
//! Store keys are `len(collection) ‖ collection ‖ len(scope) ‖ scope ‖ local_key` with
//! big-endian `u32` lengths, so no collection prefix is a prefix of another collection's,
//! no scope prefix is a prefix of another scope's within a collection, and a collection
//! or a single scope is one contiguous key range.
//!
//! A store value is one candidate:
//! `retention:u8 ‖ tag:u8 ‖ lamport:u64be ‖ len(writer) ‖ writer ‖ value`, where `tag` is
//! `0` for a value (encoded by the codec after the header) and `1` for a tombstone (no
//! value bytes). Retention and writer belong to the scope but repeat on every row, so a
//! row is self-describing and winners recompute identically after a reload from the
//! stored stamps alone.
//!
//! An installed scope's in-process [`RegistrationScopeId`] comes from the same counter
//! as incarnation scopes, so it stays `Copy`; it is not stable across restarts. The
//! stable identity is the installation key bytes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use clay_store::BatchOp;
use clay_store::OrderedStore;
use clay_store::StoreError;

use super::Candidate;
use super::Inner;
use super::RegistrationCollection;
use super::RegistrationDispatch;
use super::Retention;
use crate::source::Source;
use crate::types::MutationError;
use crate::types::RegistrationScopeId;
use crate::types::Stamp;
use crate::types::Stamped;
use crate::types::WriteBasis;
use crate::types::WriterId;

/// How a durable collection turns its keys and values into store bytes. A key or value
/// that fails to decode is corrupt storage, an invariant break: decoders panic.
pub struct DurableCodec<K, V> {
    pub encode_key: fn(&K) -> Vec<u8>,
    pub decode_key: fn(&[u8]) -> K,
    pub encode_value: fn(&V) -> Vec<u8>,
    pub decode_value: fn(&[u8]) -> V,
}

impl<K, V> Clone for DurableCodec<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for DurableCodec<K, V> {}

type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + 'a>>;

/// Object-safe face of [`OrderedStore`]'s write side (the scan happens once, at
/// construction, while the concrete store type is still known), so the collection need not be generic over its
/// store. The futures stay `!Send`.
trait ErasedStore {
    fn apply(&self, batch: Vec<BatchOp>) -> StoreFuture<'_, ()>;
}

impl<St: OrderedStore> ErasedStore for St {
    fn apply(&self, batch: Vec<BatchOp>) -> StoreFuture<'_, ()> {
        Box::pin(OrderedStore::apply(self, batch))
    }
}

pub(super) struct Durable<K, V> {
    store: Arc<dyn ErasedStore + Send + Sync>,
    collection_id: Vec<u8>,
    codec: DurableCodec<K, V>,
}

impl<K, V> Clone for Durable<K, V> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            collection_id: self.collection_id.clone(),
            codec: self.codec,
        }
    }
}

fn push_length_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("store key component exceeds u32::MAX bytes");
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(bytes);
}

fn split_length_prefixed<'a>(bytes: &'a [u8], what: &str) -> (&'a [u8], &'a [u8]) {
    let (length, rest) = bytes
        .split_first_chunk::<4>()
        .unwrap_or_else(|| panic!("store {what} missing length"));
    let length = u32::from_be_bytes(*length) as usize;
    assert!(rest.len() >= length, "store {what} shorter than its length");
    rest.split_at(length)
}

/// Key range of a whole collection.
pub(super) fn collection_prefix(collection_id: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + collection_id.len());
    push_length_prefixed(&mut out, collection_id);
    out
}

/// Key range of one installed scope within a collection.
pub(super) fn scope_prefix(collection_id: &[u8], scope_key: &[u8]) -> Vec<u8> {
    let mut out = collection_prefix(collection_id);
    push_length_prefixed(&mut out, scope_key);
    out
}

pub(super) fn row_key(scope_prefix: &[u8], local_key: &[u8]) -> Vec<u8> {
    [scope_prefix, local_key].concat()
}

/// Inverse of [`row_key`] for a key already known to sit under `collection_prefix`.
pub(super) fn split_row_key(collection_prefix_len: usize, key: &[u8]) -> (&[u8], &[u8]) {
    split_length_prefixed(&key[collection_prefix_len..], "key")
}

pub(super) fn encode_row<K, V>(
    codec: DurableCodec<K, V>,
    retention: Retention,
    candidate: &Candidate<V>,
) -> Vec<u8> {
    let mut out = vec![
        match retention {
            Retention::Installation => 0,
            Retention::Permanent => 1,
        },
        u8::from(candidate.slot.is_none()),
    ];
    out.extend_from_slice(&candidate.stamp.lamport.to_be_bytes());
    push_length_prefixed(&mut out, candidate.stamp.writer.as_bytes());
    if let Some(value) = &candidate.slot {
        out.extend_from_slice(&(codec.encode_value)(value));
    }
    out
}

pub(super) fn decode_row<K, V>(
    codec: DurableCodec<K, V>,
    bytes: &[u8],
) -> (Retention, Candidate<V>) {
    let ([retention, tag], rest) = bytes
        .split_first_chunk::<2>()
        .expect("store row missing header");
    let retention = match retention {
        0 => Retention::Installation,
        1 => Retention::Permanent,
        other => panic!("store row has unknown retention {other}"),
    };
    let (lamport, rest) = rest
        .split_first_chunk::<8>()
        .expect("store row missing lamport");
    let (writer, value) = split_length_prefixed(rest, "row writer");
    let slot = match tag {
        0 => Some((codec.decode_value)(value)),
        1 => {
            assert!(value.is_empty(), "store tombstone carries value bytes");
            None
        }
        other => panic!("store row has unknown tag {other}"),
    };
    let stamp = Stamp {
        lamport: u64::from_be_bytes(*lamport),
        writer: WriterId::from_encoded(writer),
    };
    (retention, Candidate { stamp, slot })
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> Inner<K, V, S> {
    /// The scope for an installation key, created empty if new. A scope's retention and
    /// writer are fixed at creation; disagreeing later is a caller bug, reported as the
    /// error so the caller can panic after releasing the collection lock.
    fn installed_scope(
        &mut self,
        scope_key: &[u8],
        retention: Retention,
        writer: &WriterId,
    ) -> Result<RegistrationScopeId, &'static str> {
        if let Some(id) = self.installations.get(scope_key) {
            let state = &self.scopes[id];
            if state.retention != Some(retention) {
                return Err("changed retention");
            }
            if &state.writer != writer {
                return Err("changed writer");
            }
            return Ok(*id);
        }
        let id = self.add_scope(writer.clone(), Some(retention), false);
        self.installations.insert(scope_key.to_vec(), id);
        Ok(id)
    }
}

impl<K: Ord + Clone, V: Clone> RegistrationCollection<K, V> {
    /// [`Self::with_durable_source`] with no source.
    pub async fn new_durable<St: OrderedStore + Send + Sync + 'static>(
        store: Arc<St>,
        collection_id: Vec<u8>,
        codec: DurableCodec<K, V>,
        runtime_epoch: [u64; 2],
    ) -> Result<Self, StoreError> {
        Self::with_durable_source(
            store,
            collection_id,
            codec,
            (),
            NonZeroUsize::new(256).unwrap(),
            runtime_epoch,
        )
        .await
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> RegistrationCollection<K, V, S> {
    /// Build a durable collection: scan the collection's key range, rebuild its
    /// installed scopes with their stored stamps, publish the resulting winners as one
    /// transaction, and only then return, so a reader's first bootstrap page already
    /// contains them.
    ///
    /// `St` must be `Send + Sync` because the collection handle is; a store whose futures
    /// are `!Send` is fine, its handle is not.
    pub async fn with_durable_source<St: OrderedStore + Send + Sync + 'static>(
        store: Arc<St>,
        collection_id: Vec<u8>,
        codec: DurableCodec<K, V>,
        source: S,
        replay_capacity: NonZeroUsize,
        runtime_epoch: [u64; 2],
    ) -> Result<Self, StoreError> {
        let prefix = collection_prefix(&collection_id);
        let stored = store.scan_prefix(&prefix).await?;
        let collection = Self::with_source(source, replay_capacity, runtime_epoch);
        let mut inner = collection.lock();
        inner.durable = Some(Durable {
            store,
            collection_id,
            codec,
        });
        let mut touched = BTreeSet::new();
        let mut loaded = Vec::with_capacity(stored.len());
        for (key, value) in stored {
            let (scope_key, local_key) = split_row_key(prefix.len(), &key);
            let local_key = (codec.decode_key)(local_key);
            let (retention, candidate) = decode_row(codec, &value);
            touched.insert(local_key.clone());
            loaded.push((scope_key.to_vec(), retention, local_key, candidate));
        }
        let dispatch = inner.mutate(touched, |inner, retired| {
            for (scope_key, retention, key, candidate) in loaded {
                let writer = candidate.stamp.writer.clone();
                let id = inner
                    .installed_scope(&scope_key, retention, &writer)
                    .expect("store holds an installation scope with inconsistent rows");
                inner.set_candidate(id, key, candidate, retired);
            }
        });
        drop(inner);
        // No leases exist yet, so there is nobody to wake.
        dispatch.dispatch();
        Ok(collection)
    }

    fn durable_handle(&self) -> Durable<K, V> {
        self.lock()
            .durable
            .clone()
            .expect("install/uninstall on a volatile collection")
    }

    /// Replace installed scope `scope_key` with exactly `rows` (`None` is a tombstone)
    /// written by `writer`: one atomic store `apply` (`DeletePrefix` of the scope plus a
    /// `Put` per row), then one projection transaction covering the keys whose winner
    /// changed. A row the scope already holds with an identical value keeps its stamp, a
    /// changed value or tombstone gets the scope's previous lamport plus one, and a new
    /// row is created concurrently at lamport 1. Rows the scope holds that `rows` omits,
    /// runtime writes included, are removed. A `StoreError` returns before any visible
    /// change. Wakes are returned, not fired.
    ///
    /// A scope's `retention` and `writer` are fixed by its first install; passing
    /// different ones later panics. Not safe against a concurrent `install`/`uninstall`
    /// of the same scope, or an [`InstalledWriter`] write to it, across an await: the
    /// supervisor serializes installation on its single-threaded executor.
    pub async fn install(
        &self,
        scope_key: &[u8],
        retention: Retention,
        writer: WriterId,
        rows: BTreeMap<K, Option<V>>,
    ) -> Result<RegistrationDispatch<K, V, S>, StoreError> {
        let durable = self.durable_handle();
        let codec = durable.codec;
        let scope = self.lock().installed_scope(scope_key, retention, &writer);
        let id = scope.unwrap_or_else(|mismatch| {
            panic!("installation scope {scope_key:?} {mismatch}");
        });
        let prefix = scope_prefix(&durable.collection_id, scope_key);
        let mut batch = Vec::with_capacity(rows.len() + 1);
        batch.push(BatchOp::DeletePrefix(prefix.clone()));
        // A row the scope already holds with identical bytes keeps its stamp; a changed
        // value or tombstone moves one above the scope's previous lamport, so an
        // updated row is never outranked by a write that observed the old one; a new
        // row is a concurrent creation at lamport 1.
        let rows: Vec<_> = {
            let inner = self.lock();
            rows.into_iter()
                .map(|(key, slot)| {
                    let previous = inner
                        .candidates
                        .get(&key)
                        .and_then(|holders| holders.get(&id));
                    let lamport = match previous {
                        None => 1,
                        Some(previous) => {
                            let unchanged = match (&previous.slot, &slot) {
                                (None, None) => true,
                                (Some(old), Some(new)) => {
                                    (codec.encode_value)(old) == (codec.encode_value)(new)
                                }
                                _ => false,
                            };
                            if unchanged {
                                previous.stamp.lamport
                            } else {
                                previous
                                    .stamp
                                    .lamport
                                    .checked_add(1)
                                    .expect("lamport exhausted")
                            }
                        }
                    };
                    let candidate = Candidate {
                        stamp: Stamp {
                            lamport,
                            writer: writer.clone(),
                        },
                        slot,
                    };
                    batch.push(BatchOp::Put(
                        row_key(&prefix, &(codec.encode_key)(&key)),
                        encode_row(codec, retention, &candidate),
                    ));
                    (key, candidate)
                })
                .collect()
        };
        durable.store.apply(batch).await?;

        let mut inner = self.lock();
        let mut touched = inner.scopes[&id].keys.clone();
        touched.extend(rows.iter().map(|(key, _)| key.clone()));
        let dispatch = inner.mutate(touched, |inner, retired| {
            for key in std::mem::take(&mut inner.scopes.get_mut(&id).unwrap().keys) {
                retired.push(inner.take_candidate(id, &key));
            }
            for (key, candidate) in rows {
                inner.set_candidate(id, key, candidate, retired);
            }
        });
        drop(inner);
        Ok(dispatch)
    }

    /// Remove installed scope `scope_key`: one store `DeletePrefix`, then one
    /// projection transaction covering the keys whose winner changed, so values it
    /// shadowed resurface. An unknown scope is a no-op. A `StoreError` returns before
    /// any visible change. Wakes are returned, not fired.
    ///
    /// Panics if the scope is [`Retention::Permanent`]: permanent scopes have no
    /// uninstall path.
    pub async fn uninstall(
        &self,
        scope_key: &[u8],
    ) -> Result<RegistrationDispatch<K, V, S>, StoreError> {
        let durable = self.durable_handle();
        let retention = {
            let inner = self.lock();
            let id = inner.installations.get(scope_key);
            id.map(|id| inner.scopes[id].retention)
        };
        assert!(
            retention.is_none_or(|retention| retention == Some(Retention::Installation)),
            "permanent scope {scope_key:?} cannot be uninstalled"
        );
        let prefix = scope_prefix(&durable.collection_id, scope_key);
        durable
            .store
            .apply(vec![BatchOp::DeletePrefix(prefix)])
            .await?;

        let mut inner = self.lock();
        let Some(id) = inner.installations.remove(scope_key) else {
            drop(inner);
            return Ok(RegistrationDispatch {
                output: (),
                projection: Vec::new(),
                retired: Vec::new(),
            });
        };
        let touched = inner.scopes[&id].keys.clone();
        let dispatch = inner.mutate(touched, |inner, retired| inner.remove_scope(id, retired));
        drop(inner);
        Ok(dispatch)
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> RegistrationCollection<K, V, S> {
    /// A writer for installed scope `scope_key`, the runtime counterpart of
    /// [`Self::install`]: its writes persist, survive a reload, and are retracted with
    /// the scope by `uninstall`. `Closed` if the scope is not installed.
    pub fn installed_writer(
        &self,
        scope_key: &[u8],
    ) -> Result<InstalledWriter<K, V, S>, MutationError> {
        let id = *self
            .lock()
            .installations
            .get(scope_key)
            .ok_or(MutationError::Closed)?;
        Ok(InstalledWriter {
            id,
            scope_key: scope_key.to_vec(),
            collection: self.clone(),
        })
    }
}

/// Why an [`InstalledWriter`] write failed: the same refusals as an incarnation writer,
/// or the store. A failed write leaves no visible change.
#[derive(Debug)]
pub enum InstalledWriteError {
    Mutation(MutationError),
    Store(StoreError),
}

impl From<StoreError> for InstalledWriteError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Mutation handle of an installation or permanent scope. Like
/// [`crate::RegistrationWriter`] it stamps from a [`WriteBasis`], but a write persists
/// before it becomes visible, so it is async and can fail with a [`StoreError`].
pub struct InstalledWriter<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>> = ()> {
    id: RegistrationScopeId,
    scope_key: Vec<u8>,
    collection: RegistrationCollection<K, V, S>,
}

/// Exclusive right to write one key durably, held from stamping until publication.
struct KeySlot<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> {
    collection: RegistrationCollection<K, V, S>,
    key: K,
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> KeySlot<K, V, S> {
    async fn acquire(collection: &RegistrationCollection<K, V, S>, key: &K) -> Self {
        std::future::poll_fn(|cx| {
            let mut inner = collection.lock();
            match inner.inflight.get_mut(key) {
                Some(waiters) => {
                    waiters.push(cx.waker().clone());
                    Poll::Pending
                }
                None => {
                    inner.inflight.insert(key.clone(), Vec::new());
                    Poll::Ready(())
                }
            }
        })
        .await;
        Self {
            collection: collection.clone(),
            key: key.clone(),
        }
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> Drop for KeySlot<K, V, S> {
    fn drop(&mut self) {
        let waiters = self.collection.lock().inflight.remove(&self.key);
        for waker in waiters.into_iter().flatten() {
            waker.wake();
        }
    }
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<Option<V>>>> InstalledWriter<K, V, S> {
    /// Set this scope's candidate for `key` to `value` and persist it. Wakes are
    /// returned, not fired; the caller must dispatch after releasing host locks.
    pub async fn put(
        &self,
        key: K,
        basis: WriteBasis,
        value: V,
    ) -> Result<RegistrationDispatch<K, V, S, Stamp>, InstalledWriteError> {
        self.write(key, basis, Some(value)).await
    }

    /// Set this scope's candidate for `key` to a persisted tombstone.
    pub async fn delete(
        &self,
        key: K,
        basis: WriteBasis,
    ) -> Result<RegistrationDispatch<K, V, S, Stamp>, InstalledWriteError> {
        self.write(key, basis, None).await
    }

    /// Stamp and check under the lock, persist with the lock released, then publish.
    ///
    /// The store write is an await point, so two writers could otherwise both pass a
    /// compare-and-set against the same winner, or both mint the same lamport. A
    /// per-key [`KeySlot`] makes durable writers of one key take turns across that
    /// await, whichever scope they belong to; the second stamps against what the first
    /// published. Incarnation writers do not take the slot (they cannot wait), so after
    /// the store write the stamping check runs again and, if an incarnation write
    /// slipped in and failed it, the store row is restored and the write reports the
    /// conflict. The same check covers the scope being uninstalled meanwhile, so no
    /// orphan row outlives it.
    async fn write(
        &self,
        key: K,
        basis: WriteBasis,
        slot: Option<V>,
    ) -> Result<RegistrationDispatch<K, V, S, Stamp>, InstalledWriteError> {
        let durable = self.collection.durable_handle();
        let codec = durable.codec;
        let _slot = KeySlot::acquire(&self.collection, &key).await;
        let store_key = row_key(
            &scope_prefix(&durable.collection_id, &self.scope_key),
            &(codec.encode_key)(&key),
        );
        let (stamp, retention, restore) = {
            let inner = self.collection.lock();
            let stamp = inner
                .plan_write(self.id, &key, &basis)
                .map_err(InstalledWriteError::Mutation)?;
            let retention = inner.scopes[&self.id]
                .retention
                .expect("installed writer over an incarnation scope");
            // Only a compare-and-set can need the previous row back.
            let restore = matches!(basis, WriteBasis::CompareAndSet(_)).then(|| {
                inner
                    .candidates
                    .get(&key)
                    .and_then(|holders| holders.get(&self.id))
                    .map(|previous| encode_row(codec, retention, previous))
            });
            (stamp, retention, restore)
        };
        let candidate = Candidate {
            stamp: stamp.clone(),
            slot,
        };
        durable
            .store
            .apply(vec![BatchOp::Put(
                store_key.clone(),
                encode_row(codec, retention, &candidate),
            )])
            .await?;

        // Check and publish under one lock hold, so nothing slips in between.
        let published = {
            let mut inner = self.collection.lock();
            inner.plan_write(self.id, &key, &basis).map(|_| {
                let id = self.id;
                inner.mutate(BTreeSet::from([key.clone()]), |inner, retired| {
                    inner.set_candidate(id, key, candidate, retired);
                })
            })
        };
        match published {
            Ok(dispatch) => Ok(dispatch.with_output(stamp)),
            Err(refusal) => {
                let undo = match (&refusal, restore.flatten()) {
                    (MutationError::Conflict { .. }, Some(previous)) => {
                        BatchOp::Put(store_key, previous)
                    }
                    _ => BatchOp::Delete(store_key),
                };
                durable.store.apply(vec![undo]).await?;
                Err(InstalledWriteError::Mutation(refusal))
            }
        }
    }
}
