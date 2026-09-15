//! An in-memory [`WillowStore`], used as the reference implementation.

use crate::interlude::*;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use willow25::prelude::*;

use crate::store::{
    AreaPage, AreaReadLimits, EntryKey, InsertOutcome, StoreError, WillowStore, prunes,
};

/// Identifies one stored entry.
///
/// `(namespace, subspace, path)` is exactly the identity Willow gives an entry: two entries
/// agreeing on all three always prefix prune each other, so a store retains at most one of
/// them. The timestamp is a property of the surviving entry, not part of the key.
type StoredKey = (NamespaceId, SubspaceId, Path);

struct StoredEntry {
    entry: AuthorisedEntry,
    /// `None` when the entry's payload has not arrived.
    payload: Option<Vec<u8>>,
}

/// An in-memory [`WillowStore`].
///
/// This is the reference implementation: it defines the behaviour the SQLite store must
/// match, and the shared contract suite is written against it.
#[derive(Default)]
pub struct MemStore {
    entries: RwLock<BTreeMap<StoredKey, StoredEntry>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<StoredKey, StoredEntry>> {
        self.entries.read().expect("mem store lock poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<StoredKey, StoredEntry>> {
        self.entries.write().expect("mem store lock poisoned")
    }
}

/// The least possible [`SubspaceId`], used as the lower bound of a whole-namespace scan.
///
/// `SubspaceId` orders bytewise over `as_bytes()`, and `as_bytes()` returns the same bytes
/// whichever variant backs the id (`VerifyingKeyOrDummy` keeps the raw encoding when it is
/// not a curve point). The all-zero encoding is therefore the minimum. This is byte-identical
/// to `order_theory::LeastElement::least()`, which `willow25` implements as
/// `Self::from_bytes(&[0; 32])`, but `willow25` does not re-export `order_theory`, so the
/// bytes are spelled out here rather than adding a dependency.
fn least_subspace_id() -> SubspaceId {
    SubspaceId::from_bytes(&[0; SUBSPACE_ID_WIDTH])
}

/// The lowest key a scan of `namespace_id`, optionally restricted to one subspace, can start
/// at.
fn scan_start(namespace_id: &NamespaceId, subspace_id: Option<&SubspaceId>) -> StoredKey {
    (
        namespace_id.clone(),
        subspace_id.cloned().unwrap_or_else(least_subspace_id),
        Path::new(),
    )
}

/// Whether `key` still belongs to the namespace and subspace a scan started in.
fn in_scan(key: &StoredKey, namespace_id: &NamespaceId, subspace_id: Option<&SubspaceId>) -> bool {
    key.0 == *namespace_id
        && subspace_id.is_none_or(|subspace_id| key.1 == *subspace_id)
}

/// Inserts `entry`, applying Willow prefix pruning. The caller holds the write lock.
///
/// Re-inserting the entry that is already stored is a no-op, and the no-op is observable: it
/// suppresses pruning, so the entries this entry prunes stay in place. `willow25`'s in-memory
/// store returns early on that case for the same reason, and the `store_pruning` corpus
/// depends on it.
///
/// `payload` is `Some` when the caller supplied one. `None` means the entry arrived without
/// a payload: the existing payload is retained when the stored entry describes the same
/// entry, and otherwise recorded as absent.
fn insert_locked(
    entries: &mut BTreeMap<StoredKey, StoredEntry>,
    entry: AuthorisedEntry,
    payload: Option<Vec<u8>>,
) -> InsertOutcome {
    let namespace_id = entry.namespace_id().clone();
    let subspace_id = entry.subspace_id().clone();
    let path = entry.path().clone();
    let key = (namespace_id.clone(), subspace_id.clone(), path.clone());

    // Re-inserting a stored entry suppresses the pruning below, so the prune set is empty.
    // The doc comment on this function says why that is observable rather than an optimisation.
    let already_stored = entries
        .get(&key)
        .is_some_and(|existing| existing.entry == entry);

    // An existing entry prunes the new one when its path is a prefix of the new path and it
    // is newer. Each candidate is one key lookup, because the map is ordered by path within
    // the subspace and a path has few prefixes.
    for prefix in path.all_prefixes() {
        if let Some(existing) = entries.get(&(namespace_id.clone(), subspace_id.clone(), prefix))
            && existing.entry.is_newer_than(&entry)
        {
            return InsertOutcome::Outdated;
        }
    }

    // The payload is retained only when the stored entry describes the same entry; otherwise
    // the retained bytes would not correspond to the new metadata.
    let payload = match payload {
        Some(payload) => Some(payload),
        None => entries
            .get(&key)
            .filter(|existing| existing.entry.entry_eq(&entry))
            .and_then(|existing| existing.payload.clone()),
    };

    // The paths prefixed by `path` are exactly the keys from `path` up to the first key that
    // is not prefixed by it, so everything the new entry prunes is one contiguous range.
    let pruned: Vec<StoredKey> = if already_stored {
        Vec::new()
    } else {
        entries
            .range(key.clone()..)
            .take_while(|(key, _)| {
                key.0 == namespace_id && key.1 == subspace_id && path.is_prefix_of(&key.2)
            })
            .filter(|(_, existing)| prunes(&existing.entry, &entry))
            .map(|(key, _)| key.clone())
            .collect()
    };

    for pruned_key in &pruned {
        entries.remove(pruned_key);
    }

    entries.insert(
        key,
        StoredEntry {
            entry,
            payload,
        },
    );

    InsertOutcome::Inserted {
        pruned: pruned.len(),
    }
}

#[async_trait]
impl WillowStore for MemStore {
    async fn insert_entry(&self, entry: AuthorisedEntry) -> Result<InsertOutcome, StoreError> {
        let mut entries = self.write();
        Ok(insert_locked(&mut entries, entry, None))
    }

    async fn insert_entry_with_payload(
        &self,
        entry: AuthorisedEntry,
        payload: &[u8],
    ) -> Result<InsertOutcome, StoreError> {
        if payload.len() as u64 != entry.payload_length() {
            return Err(StoreError::PayloadLengthMismatch {
                expected: entry.payload_length(),
                actual: payload.len() as u64,
            });
        }
        if PayloadDigest::from_payload(payload) != *entry.payload_digest() {
            return Err(StoreError::PayloadDigestMismatch);
        }

        let mut entries = self.write();
        Ok(insert_locked(&mut entries, entry, Some(payload.to_vec())))
    }

    async fn get_entry(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<Option<AuthorisedEntry>, StoreError> {
        Ok(self
            .read()
            .get(&(namespace_id.clone(), subspace_id.clone(), path.clone()))
            .map(|stored| stored.entry.clone()))
    }

    async fn get_payload(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .read()
            .get(&(namespace_id.clone(), subspace_id.clone(), path.clone()))
            .and_then(|stored| stored.payload.clone()))
    }

    async fn read_area(
        &self,
        namespace_id: &NamespaceId,
        area: &Area,
        resume_after: Option<&EntryKey>,
        limits: AreaReadLimits,
    ) -> Result<AreaPage, StoreError> {
        let entries = self.read();
        let subspace_id = area.subspace();
        let start = scan_start(namespace_id, subspace_id);

        let lower = match resume_after {
            Some((resume_subspace, resume_path)) => Bound::Excluded(std::cmp::max(
                start,
                (
                    namespace_id.clone(),
                    resume_subspace.clone(),
                    resume_path.clone(),
                ),
            )),
            None => Bound::Included(start),
        };

        let mut matching = entries
            .range((lower, Bound::Unbounded))
            .take_while(|(key, _)| in_scan(key, namespace_id, subspace_id))
            .filter(|(_, stored)| area.includes(stored.entry.entry()))
            .map(|(_, stored)| &stored.entry);

        let mut page = AreaPage {
            entries: Vec::new(),
            next: None,
        };

        while page.entries.len() < limits.max_entries.get() {
            let Some(entry) = matching.next() else {
                // The scan ended, so this page drained the query.
                return Ok(page);
            };
            page.entries.push(entry.clone());
        }

        // The page filled. Peek one entry past it: only then is there a following page, and
        // the caller resumes strictly after this page's last entry.
        if matching.next().is_some() {
            let last = page
                .entries
                .last()
                .expect("a full page always contains at least one entry");
            page.next = Some((last.subspace_id().clone(), last.path().clone()));
        }

        Ok(page)
    }

    async fn forget_entry(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<bool, StoreError> {
        Ok(self
            .write()
            .remove(&(namespace_id.clone(), subspace_id.clone(), path.clone()))
            .is_some())
    }

    async fn forget_area(&self, namespace_id: &NamespaceId, area: &Area) -> Result<(), StoreError> {
        let mut entries = self.write();
        let subspace_id = area.subspace();

        let doomed: Vec<StoredKey> = entries
            .range(scan_start(namespace_id, subspace_id)..)
            .take_while(|(key, _)| in_scan(key, namespace_id, subspace_id))
            .filter(|(_, stored)| area.includes(stored.entry.entry()))
            .map(|(key, _)| key.clone())
            .collect();

        for key in doomed {
            entries.remove(&key);
        }

        Ok(())
    }

    async fn forget_namespace(&self, namespace_id: &NamespaceId) -> Result<(), StoreError> {
        self.write().retain(|key, _| key.0 != *namespace_id);
        Ok(())
    }

    async fn flush(&self) -> Result<(), StoreError> {
        // An in-memory store has nothing to persist.
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;

    use crate::store::contract::contract_suite;

    fn assert_send_sync<T: Send + Sync + 'static>() {}

    #[test]
    fn mem_store_is_send_sync_and_object_safe() {
        assert_send_sync::<MemStore>();

        let store: Arc<dyn WillowStore> = Arc::new(MemStore::new());
        let shared: Arc<dyn WillowStore> = Arc::clone(&store);
        assert!(Arc::ptr_eq(&store, &shared));
    }

    /// The whole point of owning the store is that it can be driven from BigRepo's
    /// multi-threaded tasks, which the upstream stores cannot be.
    #[tokio::test(flavor = "multi_thread")]
    async fn mem_store_can_be_driven_from_another_thread() -> Result<(), tokio::task::JoinError> {
        let store: Arc<dyn WillowStore> = Arc::new(MemStore::new());

        tokio::spawn({
            let store = Arc::clone(&store);
            async move { store.flush().await.expect("flush succeeds") }
        })
        .await
    }

    /// `least_subspace_id` rests on `SubspaceId` ordering bytewise over `as_bytes()`. Pin
    /// that for both variants: a decoded curve point and an encoding that is not one.
    #[test]
    fn the_all_zero_subspace_id_is_the_least() {
        let least = least_subspace_id();
        assert!(least.as_bytes() == &[0; SUBSPACE_ID_WIDTH]);

        // A byte pattern that is not a curve point, so `SubspaceId` retains it as a `Dummy`
        // rather than a `VerifyingKey`.
        let dummy_bytes = (0u16..=255)
            .map(|fill| [fill as u8; SUBSPACE_ID_WIDTH])
            .chain((0u16..=255).map(|fill| {
                let mut bytes = [0x5a; SUBSPACE_ID_WIDTH];
                bytes[0] = fill as u8;
                bytes
            }))
            .find(|bytes| ed25519_dalek::VerifyingKey::from_bytes(bytes).is_err())
            .expect("ed25519 has byte patterns that are not curve points");

        let dummy = SubspaceId::from_bytes(&dummy_bytes);
        // The raw encoding survives into the `Dummy` variant, so ordering cannot depend on
        // which variant backs an id.
        assert!(dummy.as_bytes() == &dummy_bytes);
        assert!(dummy != least);

        let samples = [
            SubspaceId::from_bytes(&[0x01; SUBSPACE_ID_WIDTH]),
            SubspaceId::from_bytes(&[0x80; SUBSPACE_ID_WIDTH]),
            SubspaceId::from_bytes(&[0xff; SUBSPACE_ID_WIDTH]),
            dummy,
        ];
        for sample in &samples {
            assert!(
                least <= *sample,
                "the all-zero subspace id must be the least, but {least:?} > {sample:?}",
            );
        }
    }

    #[tokio::test]
    async fn mem_store_satisfies_the_store_contract() {
        contract_suite(Arc::new(MemStore::new())).await;
    }
}
