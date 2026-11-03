//! Durable installation and permanent scopes over an [`OrderedStore`] (ADR 025 §§2
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

use clay_store::BatchOp;
use clay_store::OrderedStore;
use clay_store::StoreError;

use super::Candidate;
use super::Inner;
use super::RegistrationCollection;
use super::RegistrationDispatch;
use super::Retention;
use crate::source::Source;
use crate::types::RegistrationScopeId;
use crate::types::Stamp;
use crate::types::Stamped;
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
        writer: WriterId::new(writer),
    };
    (retention, Candidate { stamp, slot })
}

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> Inner<K, V, S> {
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

impl<K: Ord + Clone, V: Clone, S: Source<K, Stamped<V>>> RegistrationCollection<K, V, S> {
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

    /// Replace installed scope `scope_key` with exactly `rows` (`None` is a tombstone),
    /// stamped lamport `1` by `writer`: one atomic store `apply` (`DeletePrefix` of the
    /// scope plus a `Put` per row), then one projection transaction covering the keys
    /// whose winner changed. Reinstalling an unchanged row reproduces its stamp. A
    /// `StoreError` returns before any visible change. Wakes are returned, not fired.
    ///
    /// A scope's `retention` and `writer` are fixed by its first install; passing
    /// different ones later panics. Not safe against a concurrent `install`/`uninstall`
    /// of the same scope across an await: the supervisor serializes installation on its
    /// single-threaded executor.
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
        let stamp = Stamp { lamport: 1, writer };
        let rows: Vec<_> = rows
            .into_iter()
            .map(|(key, slot)| {
                let candidate = Candidate {
                    stamp: stamp.clone(),
                    slot,
                };
                batch.push(BatchOp::Put(
                    row_key(&prefix, &(codec.encode_key)(&key)),
                    encode_row(codec, retention, &candidate),
                ));
                (key, candidate)
            })
            .collect();
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
