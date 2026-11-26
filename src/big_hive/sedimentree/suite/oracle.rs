//! The dumbest store that answers the same reads: a map of items per tree, no rows, no `rev`,
//! no cache. A save adds the items one at a time (a lower digest replaces the payload of a
//! head), then keeps exactly what the library's own `minimize` of the whole tree keeps.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use sedimentree_core::blob::has_meta::HasBlobMeta;
use sedimentree_core::codec::decode::DecodeFields;
use sedimentree_core::codec::encode::Encode;
use sedimentree_core::codec::encode::EncodeFields;
use sedimentree_core::codec::schema::Schema;
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::fragment::Fragment;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::LooseCommit;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_core::sedimentree::Sedimentree;

use crate::sedimentree::Save;

/// An item as the tables hold it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Row<T> {
    pub payload: T,
    pub signed: Vec<u8>,
    pub blob: Vec<u8>,
    pub pcs: Option<[u8; 32]>,
}

impl<T: Schema + EncodeFields + DecodeFields + Encode + HasBlobMeta> Row<T> {
    pub(crate) fn of(save: Save<T>) -> Self {
        let (signed, payload, blob) = save.verified.into_full_parts();
        Self {
            payload,
            signed: signed.as_bytes().to_vec(),
            blob: blob.into_contents(),
            pcs: save.pcs_update_hash,
        }
    }

    fn digest(&self) -> Digest<T> {
        Digest::hash(&self.payload)
    }
}

#[derive(Clone, Default)]
pub(crate) struct OracleTree {
    pub live: bool,
    pub commits: BTreeMap<CommitId, Row<LooseCommit>>,
    pub fragments: BTreeMap<CommitId, Row<Fragment>>,
}

#[derive(Clone, Default)]
pub(crate) struct Oracle {
    pub trees: BTreeMap<SedimentreeId, OracleTree>,
}

/// Add `row` under `head`, or not: an existing payload is replaced only by a lower digest.
fn add<T: Schema + EncodeFields + DecodeFields + Encode + HasBlobMeta>(
    rows: &mut BTreeMap<CommitId, Row<T>>,
    head: CommitId,
    row: Row<T>,
) {
    match rows.get(&head) {
        Some(old) if old.digest() <= row.digest() => {}
        _ => {
            rows.insert(head, row);
        }
    }
}

impl OracleTree {
    /// The tree the rows are.
    pub(crate) fn tree(&self) -> Sedimentree {
        Sedimentree::new(
            self.fragments
                .values()
                .map(|row| row.payload.clone())
                .collect(),
            self.commits
                .values()
                .map(|row| row.payload.clone())
                .collect(),
        )
    }

    fn keys(&self) -> (BTreeSet<CommitId>, BTreeSet<CommitId>) {
        (
            self.commits.keys().copied().collect(),
            self.fragments.keys().copied().collect(),
        )
    }

    fn digests(&self) -> Vec<[u8; 32]> {
        self.commits
            .values()
            .map(|row| row.digest().into_bytes())
            .chain(self.fragments.values().map(|row| row.digest().into_bytes()))
            .collect()
    }
}

impl Oracle {
    fn tree_mut(&mut self, id: SedimentreeId) -> &mut OracleTree {
        self.trees.entry(id).or_default()
    }

    /// Whether the tree changed.
    pub(crate) fn save(
        &mut self,
        id: SedimentreeId,
        commits: Vec<Save<LooseCommit>>,
        fragments: Vec<Save<Fragment>>,
    ) -> bool {
        let tree = self.tree_mut(id);
        let was_live = tree.live;
        let before = (tree.keys(), tree.digests());
        tree.live = true;
        for save in commits {
            let row = Row::of(save);
            add(&mut tree.commits, row.payload.head(), row);
        }
        for save in fragments {
            let row = Row::of(save);
            add(&mut tree.fragments, row.payload.head(), row);
        }
        let minimal = tree.tree().minimize(&CountLeadingZeroBytes);
        let commit_heads: BTreeSet<_> = minimal.loose_commits().map(LooseCommit::head).collect();
        let fragment_heads: BTreeSet<_> = minimal.fragments().map(Fragment::head).collect();
        tree.commits.retain(|head, _| commit_heads.contains(head));
        tree.fragments
            .retain(|head, _| fragment_heads.contains(head));
        !was_live || before != (tree.keys(), tree.digests())
    }

    pub(crate) fn register(&mut self, id: SedimentreeId) -> bool {
        !std::mem::replace(&mut self.tree_mut(id).live, true)
    }

    /// Retire the tree: every row goes and the id is dead.
    pub(crate) fn retire(&mut self, id: SedimentreeId) -> bool {
        let tree = self.tree_mut(id);
        let was_live = std::mem::replace(&mut tree.live, false);
        tree.commits.clear();
        tree.fragments.clear();
        was_live
    }

    pub(crate) fn delete_commit(&mut self, id: SedimentreeId, head: CommitId) -> bool {
        self.tree_mut(id).commits.remove(&head).is_some()
    }

    pub(crate) fn delete_commits(&mut self, id: SedimentreeId) -> bool {
        !std::mem::take(&mut self.tree_mut(id).commits).is_empty()
    }

    pub(crate) fn delete_fragment(&mut self, id: SedimentreeId, head: CommitId) -> bool {
        self.tree_mut(id).fragments.remove(&head).is_some()
    }

    pub(crate) fn delete_fragments(&mut self, id: SedimentreeId) -> bool {
        !std::mem::take(&mut self.tree_mut(id).fragments).is_empty()
    }

    /// The blobs of the items of `id` encrypted under `pcs`.
    pub(crate) fn blobs_under(&self, id: SedimentreeId, pcs: [u8; 32]) -> Vec<Vec<u8>> {
        let Some(tree) = self.trees.get(&id) else {
            return Vec::new();
        };
        let mut blobs: Vec<_> = tree
            .commits
            .values()
            .filter(|row| row.pcs == Some(pcs))
            .map(|row| row.blob.clone())
            .chain(
                tree.fragments
                    .values()
                    .filter(|row| row.pcs == Some(pcs))
                    .map(|row| row.blob.clone()),
            )
            .collect();
        blobs.sort();
        blobs
    }
}
