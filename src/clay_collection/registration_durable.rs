//! Durable installation scopes over an [`OrderedStore`] (ADR 025 §§2 and 4).
//!
//! A durable [`RegistrationCollection`] holds two kinds of scope. Incarnation scopes
//! (`create_scope`) are volatile and never touch the store. Installation scopes are
//! keyed by caller-chosen stable bytes (e.g. the owning micro id), have no owner handle
//! and no runtime writers, and are replaced or removed whole by
//! [`RegistrationCollection::install`] / [`RegistrationCollection::uninstall`], which
//! persist first and only then change memory and the projection.
//!
//! Store keys are `len(collection) ‖ collection ‖ len(scope) ‖ scope ‖ local_key` with
//! big-endian `u32` lengths, so no collection prefix is a prefix of another collection's,
//! no scope prefix is a prefix of another scope's within a collection, and a collection
//! or a single scope is one contiguous key range.
//!
//! An installation scope's in-process [`RegistrationScopeId`] comes from the same counter
//! as incarnation scopes, so physical keys stay injective and the id stays `Copy`; it is
//! not stable across restarts. The stable identity is the installation key bytes.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use clay_store::BatchOp;
use clay_store::OrderedStore;
use clay_store::StoreError;

use super::Inner;
use super::RegistrationCollection;
use super::RegistrationDispatch;
use super::ScopeState;
use crate::replica::Entry;
use crate::source::Source;
use crate::types::Change;
use crate::types::RegistrationScopeId;
use crate::types::RowEvidence;

/// How a durable collection turns its rows into store bytes. A key or value that fails
/// to decode is corrupt storage, an invariant break: decoders panic.
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

/// Key range of a whole collection.
pub(super) fn collection_prefix(collection_id: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + collection_id.len());
    push_length_prefixed(&mut out, collection_id);
    out
}

/// Key range of one installation scope within a collection.
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
    let rest = &key[collection_prefix_len..];
    let (length, rest) = rest
        .split_first_chunk::<4>()
        .expect("store key missing scope length");
    let length = u32::from_be_bytes(*length) as usize;
    assert!(rest.len() >= length, "store key shorter than its scope");
    rest.split_at(length)
}

impl<K, V, P, S: Source<P, V>> Inner<K, V, P, S> {
    /// The scope for an installation key, created empty if new.
    fn installation_scope(&mut self, scope_key: &[u8]) -> RegistrationScopeId {
        if let Some(id) = self.installations.get(scope_key) {
            return *id;
        }
        let id = self.next_scope_id();
        assert!(
            self.scopes
                .insert(
                    id,
                    ScopeState {
                        rows: BTreeMap::new(),
                        staged: false,
                    },
                )
                .is_none()
        );
        self.installations.insert(scope_key.to_vec(), id);
        id
    }
}

impl<K: Ord + Clone, V: Clone> RegistrationCollection<K, V> {
    /// [`Self::with_durable_source`] with the default physical key and no source.
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
            |scope, key| (scope, key.clone()),
        )
        .await
    }
}

impl<K: Ord + Clone, V: Clone, P: Ord + Clone, S: Source<P, V>> RegistrationCollection<K, V, P, S> {
    /// Build a durable collection: scan the collection's key range, rebuild its
    /// installation scopes, publish their rows as one transaction, and only then return,
    /// so a reader's first bootstrap page already contains them.
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
        physical: fn(RegistrationScopeId, &K) -> P,
    ) -> Result<Self, StoreError> {
        let prefix = collection_prefix(&collection_id);
        let stored = store.scan_prefix(&prefix).await?;
        let collection = Self::with_source(source, replay_capacity, runtime_epoch, physical);
        let mut inner = collection.lock();
        inner.durable = Some(Durable {
            store,
            collection_id,
            codec,
        });
        let mut changes = Vec::with_capacity(stored.len());
        for (key, value) in stored {
            let (scope_key, local_key) = split_row_key(prefix.len(), &key);
            let id = inner.installation_scope(scope_key);
            let local_key = (codec.decode_key)(local_key);
            let entry = Entry {
                version: inner.next_version(),
                value: (codec.decode_value)(&value),
                evidence: RowEvidence::Authoritative,
                write_context: None,
            };
            changes.push(upsert(physical, id, &local_key, &entry));
            inner
                .scopes
                .get_mut(&id)
                .unwrap()
                .rows
                .insert(local_key, entry);
        }
        if !changes.is_empty() {
            // No leases exist yet, so there is nobody to wake.
            inner.projection.publish(changes);
        }
        drop(inner);
        Ok(collection)
    }

    fn durable_handle(&self) -> Durable<K, V> {
        self.lock()
            .durable
            .clone()
            .expect("install/uninstall on a volatile collection")
    }

    /// Replace installation scope `scope_key` with exactly `rows`: one atomic store
    /// `apply` (`DeletePrefix` of the scope plus a `Put` per row), then one projection
    /// transaction with an upsert per row and a remove per row that is gone. A
    /// `StoreError` returns before any visible change. Wakes are returned, not fired.
    ///
    /// Not safe against a concurrent `install`/`uninstall` of the same scope across an
    /// await: the supervisor serializes installation on its single-threaded executor.
    pub async fn install(
        &self,
        scope_key: &[u8],
        rows: BTreeMap<K, V>,
    ) -> Result<RegistrationDispatch<K, V, P, S>, StoreError> {
        let durable = self.durable_handle();
        let codec = durable.codec;
        let prefix = scope_prefix(&durable.collection_id, scope_key);
        let mut batch = Vec::with_capacity(rows.len() + 1);
        batch.push(BatchOp::DeletePrefix(prefix.clone()));
        for (key, value) in &rows {
            batch.push(BatchOp::Put(
                row_key(&prefix, &(codec.encode_key)(key)),
                (codec.encode_value)(value),
            ));
        }
        durable.store.apply(batch).await?;

        let mut inner = self.lock();
        let id = inner.installation_scope(scope_key);
        let physical = inner.physical;
        let mut changes = Vec::new();
        let mut fresh = BTreeMap::new();
        for (key, value) in rows {
            let entry = Entry {
                version: inner.next_version(),
                value,
                evidence: RowEvidence::Authoritative,
                write_context: None,
            };
            changes.push(upsert(physical, id, &key, &entry));
            fresh.insert(key, entry);
        }
        let previous = std::mem::replace(&mut inner.scopes.get_mut(&id).unwrap().rows, fresh);
        for key in previous.keys() {
            if !inner.scopes[&id].rows.contains_key(key) {
                changes.push(Change::Remove {
                    key: physical(id, key),
                    row_version: inner.next_version(),
                });
            }
        }
        let mut dispatch = RegistrationDispatch::default();
        if !changes.is_empty() {
            dispatch
                .projection
                .push(inner.projection.publish_deferred(changes));
        }
        dispatch.rows.push(previous);
        drop(inner);
        Ok(dispatch)
    }

    /// Remove installation scope `scope_key`: one store `DeletePrefix`, then one
    /// projection transaction removing its rows. An unknown scope is a no-op. A
    /// `StoreError` returns before any visible change. Wakes are returned, not fired.
    pub async fn uninstall(
        &self,
        scope_key: &[u8],
    ) -> Result<RegistrationDispatch<K, V, P, S>, StoreError> {
        let durable = self.durable_handle();
        let prefix = scope_prefix(&durable.collection_id, scope_key);
        durable
            .store
            .apply(vec![BatchOp::DeletePrefix(prefix)])
            .await?;

        let mut inner = self.lock();
        let mut dispatch = RegistrationDispatch::default();
        let Some(id) = inner.installations.remove(scope_key) else {
            drop(inner);
            return Ok(dispatch);
        };
        let state = inner.scopes.remove(&id).unwrap();
        let physical = inner.physical;
        let changes: Vec<_> = state
            .rows
            .keys()
            .map(|key| Change::Remove {
                key: physical(id, key),
                row_version: inner.next_version(),
            })
            .collect();
        if !changes.is_empty() {
            dispatch
                .projection
                .push(inner.projection.publish_deferred(changes));
        }
        dispatch.rows.push(state.rows);
        drop(inner);
        Ok(dispatch)
    }
}

fn upsert<K, V: Clone, P>(
    physical: fn(RegistrationScopeId, &K) -> P,
    id: RegistrationScopeId,
    key: &K,
    entry: &Entry<V>,
) -> Change<P, V> {
    Change::Upsert {
        key: physical(id, key),
        row_version: entry.version,
        value: entry.value.clone(),
        evidence: entry.evidence,
        write_context: entry.write_context.clone(),
    }
}
