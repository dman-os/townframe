use crate::BatchOp;
use crate::OrderedStore;
use crate::StoreError;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;

/// Volatile store over a `BTreeMap`, for tests and mock setups. It never fails.
#[derive(Default)]
pub struct MemoryStore {
    entries: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OrderedStore for MemoryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.entries.lock().expect("poisoned").get(key).cloned())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let entries = self.entries.lock().expect("poisoned");
        Ok(prefix_range(&entries, prefix)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn apply(&self, batch: Vec<BatchOp>) -> Result<(), StoreError> {
        // No op can fail, so applying under one lock guard is already atomic.
        let mut entries = self.entries.lock().expect("poisoned");
        for op in batch {
            match op {
                BatchOp::Put(key, value) => {
                    entries.insert(key, value);
                }
                BatchOp::Delete(key) => {
                    entries.remove(&key);
                }
                BatchOp::DeletePrefix(prefix) => {
                    let doomed: Vec<Vec<u8>> = prefix_range(&entries, &prefix)
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in doomed {
                        entries.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }
}

fn prefix_range<'a>(
    entries: &'a BTreeMap<Vec<u8>, Vec<u8>>,
    prefix: &'a [u8],
) -> impl Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)> {
    entries
        .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
        .take_while(move |(key, _)| key.starts_with(prefix))
}
