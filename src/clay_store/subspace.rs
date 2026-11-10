use crate::BatchOp;
use crate::OrderedStore;
use crate::StoreError;
use std::sync::Arc;

/// A namespaced view of one store: every key is stored under a length-prefixed namespace and
/// the prefix is stripped again on scans, so two subspaces of the same store can never see or
/// clobber each other's keys, whatever bytes their keys contain.
///
/// The namespace is encoded as a big-endian `u32` byte length followed by the name's bytes.
/// Length-prefixing makes the namespaces prefix-free (no encoded namespace is a prefix of
/// another), which is what keeps the scan range `[ns, ns-successor)` of one subspace disjoint
/// from every other, and what lets `DeletePrefix(b"")` inside a subspace clear only that
/// subspace.
pub struct Subspace<St> {
    store: Arc<St>,
    namespace: Vec<u8>,
}

impl<St: OrderedStore> Subspace<St> {
    pub(crate) fn new(store: Arc<St>, name: &str) -> Self {
        let length = u32::try_from(name.len()).expect("a namespace name fits in u32");
        let mut namespace = Vec::with_capacity(4 + name.len());
        namespace.extend_from_slice(&length.to_be_bytes());
        namespace.extend_from_slice(name.as_bytes());
        Self { store, namespace }
    }

    fn qualify(&self, key: &[u8]) -> Vec<u8> {
        let mut qualified = Vec::with_capacity(self.namespace.len() + key.len());
        qualified.extend_from_slice(&self.namespace);
        qualified.extend_from_slice(key);
        qualified
    }
}

impl<St: OrderedStore> OrderedStore for Subspace<St> {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.store.get(&self.qualify(key)).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let mut entries = self.store.scan_prefix(&self.qualify(prefix)).await?;
        for (key, _) in &mut entries {
            // The backend only returns keys starting with the qualified prefix, which itself
            // starts with the namespace; a miss here is a backend contract violation.
            key.drain(..self.namespace.len());
        }
        Ok(entries)
    }

    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError> {
        let batch = batch
            .into_iter()
            .map(|op| match op {
                BatchOp::Put(key, value) => BatchOp::Put(self.qualify(&key), value),
                BatchOp::Delete(key) => BatchOp::Delete(self.qualify(&key)),
                BatchOp::DeletePrefix(prefix) => BatchOp::DeletePrefix(self.qualify(&prefix)),
            })
            .collect();
        self.store.apply(batch).await
    }
}
