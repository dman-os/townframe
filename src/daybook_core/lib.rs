#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use samod::DocumentId;
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use struct_patch::Patch;
    pub use utils_rs::am::AmCtx;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use crate::interlude::*;

pub mod config;
pub mod blobs;
pub mod drawer;
#[allow(unused)]
pub mod repos;
pub mod stores;
pub mod tables;
pub mod triage;

pub mod wash_plugin;

#[cfg(test)]
mod e2e;

#[cfg(test)]
mod tincans;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
uniffi::custom_type!(OffsetDateTime, i64, {
    remote,
    lower: |dt| dt.unix_timestamp(),
    try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
        .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
});

#[cfg(feature = "uniffi")]
uniffi::custom_type!(Uuid, Vec<u8>, {
    remote,
    lower: |uuid| uuid.as_bytes().to_vec(),
    try_lift: |bytes: Vec<u8>| {
        uuid::Uuid::from_slice(&bytes)
            .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
    }
});

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeHashSet(pub Arc<[automerge::ChangeHash]>);

impl autosurgeon::Hydrate for ChangeHashSet {
    fn hydrate_seq<D: autosurgeon::ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
    ) -> Result<Self, autosurgeon::HydrateError> {
        let inner: Arc<[automerge::ChangeHash]> = autosurgeon::Hydrate::hydrate_seq(doc, obj)?;
        Ok(ChangeHashSet(inner))
    }
}

impl autosurgeon::Reconcile for ChangeHashSet {
    type Key<'a> = ();

    fn reconcile<R: autosurgeon::Reconciler>(&self, reconciler: R) -> Result<(), R::Error> {
        autosurgeon::Reconcile::reconcile(&self.0, reconciler)
    }
}

impl std::ops::Deref for ChangeHashSet {
    type Target = [automerge::ChangeHash];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[cfg(feature = "uniffi")]
uniffi::custom_type!(ChangeHashSet, Vec<String>, {
    remote,
    lower: |hash| utils_rs::am::serialize_commit_heads(&hash.0),
    try_lift: |strings: Vec<String>| {
        Ok(ChangeHashSet(utils_rs::am::parse_commit_heads(&strings).to_anyhow()?))
    }
});

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use autosurgeon::{hydrate_prop, reconcile_prop};

    #[test]
    fn test_change_hash_set_hydrate_seq() {
        let mut doc = automerge::AutoCommit::new();
        let list_id = doc
            .put_object(automerge::ROOT, "heads", automerge::ObjType::List)
            .unwrap();

        // Create some change hashes
        let hash1 = automerge::ChangeHash([1u8; 32]);
        let hash2 = automerge::ChangeHash([2u8; 32]);
        let hash3 = automerge::ChangeHash([3u8; 32]);

        // Insert hashes as bytes (convert [u8; 32] to Vec<u8>)
        doc.insert(&list_id, 0, hash1.0.to_vec()).unwrap();
        doc.insert(&list_id, 1, hash2.0.to_vec()).unwrap();
        doc.insert(&list_id, 2, hash3.0.to_vec()).unwrap();

        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        let heads = hydrated.0.as_ref();

        assert_eq!(heads.len(), 3);
        assert_eq!(heads[0], hash1);
        assert_eq!(heads[1], hash2);
        assert_eq!(heads[2], hash3);
    }

    #[test]
    fn test_change_hash_set_reconcile() {
        let mut doc = automerge::AutoCommit::new();
        let hash1 = automerge::ChangeHash([1u8; 32]);
        let hash2 = automerge::ChangeHash([2u8; 32]);
        let original = ChangeHashSet(Arc::from([hash1, hash2]));

        reconcile_prop(&mut doc, automerge::ROOT, "heads", &original).unwrap();

        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        assert_eq!(hydrated, original);
    }

    #[test]
    fn test_change_hash_set_round_trip() {
        let mut doc = automerge::AutoCommit::new();
        let hashes = vec![
            automerge::ChangeHash([1u8; 32]),
            automerge::ChangeHash([2u8; 32]),
            automerge::ChangeHash([3u8; 32]),
        ];
        let original = ChangeHashSet(Arc::from(hashes.clone()));

        // Reconcile into document
        reconcile_prop(&mut doc, automerge::ROOT, "heads", &original).unwrap();

        // Hydrate back
        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        assert_eq!(hydrated, original);
    }

    #[test]
    fn test_change_hash_set_empty() {
        let mut doc = automerge::AutoCommit::new();
        let original = ChangeHashSet(Arc::from([]));

        reconcile_prop(&mut doc, automerge::ROOT, "heads", &original).unwrap();

        let hydrated: ChangeHashSet = hydrate_prop(&doc, automerge::ROOT, "heads").unwrap();
        assert_eq!(hydrated, original);
    }
}

pub fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        sqlite_vec::sqlite3_vec_init();
    });
}
