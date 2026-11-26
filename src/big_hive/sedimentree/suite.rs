//! The laws, a differential against a naive oracle, and what SQLite adds: crashes and reopening,
//! stored-state invariants, query plans and statement counts, the change feed, the upstream
//! `MinimizedTreeStore` conformance checks, planted bugs.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use future_form::Sendable;
use sedimentree_core::blob::Blob;
use sedimentree_core::blob::verified::VerifiedBlobMeta;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::fragment::Fragment;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::LooseCommit;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_core::test_utils::commit_id_with_depth;
use sqlite_rs::Database;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;
use subduction_crypto::signer::memory::MemorySigner;
use subduction_crypto::verified_meta::VerifiedMeta;

use big_automata::rev::Entry;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::RevSource;

use super::Save;
use super::SedimentreeStore;
use crate::MIGRATIONS;

mod conformance;
mod crash;
mod differential;
mod invariants;
mod laws;
mod oracle;
mod plans;
mod rev;

pub(crate) type Store = SedimentreeStore<Database, CountLeadingZeroBytes>;

pub(crate) static RUNTIME: LazyLock<SqliteRuntime> = LazyLock::new(|| {
    SqliteRuntime::new(RuntimeConfig {
        workers: 2,
        ..RuntimeConfig::default()
    })
});

/// A new, empty database on its own ephemeral file, migrated.
pub(crate) fn fresh_db() -> Database {
    RUNTIME
        .ephemeral(MIGRATIONS)
        .expect("an ephemeral database opens")
}

pub(crate) fn store_on(db: &Database) -> Store {
    SedimentreeStore::new(db.clone(), CountLeadingZeroBytes)
}

pub(crate) fn tree_id(n: u8) -> SedimentreeId {
    SedimentreeId::new([n; 32])
}

/// How many distinct commit ids a generated history draws from, and the depth of each under
/// [`CountLeadingZeroBytes`]: a few boundaries (depth above 0) among ordinary commits, so
/// fragments have heads and covered ranges.
pub(crate) const POOL: usize = 8;
const DEPTHS: [u8; POOL] = [0, 0, 1, 0, 2, 0, 1, 0];

pub(crate) fn pooled(index: u8) -> CommitId {
    commit_id_with_depth(DEPTHS[usize::from(index) % POOL], index % POOL as u8)
}

/// An item as a generator draws it, before it is sealed: a pooled head, pooled relations, and a
/// byte that makes two items of one head differ (an equivocation).
#[derive(Clone, Debug)]
pub(crate) enum Spec {
    Commit {
        head: u8,
        parents: Vec<u8>,
        blob: u8,
        pcs: Option<u8>,
    },
    Fragment {
        head: u8,
        boundary: Vec<u8>,
        checkpoints: Vec<u8>,
        blob: u8,
        pcs: Option<u8>,
    },
}

/// The PCS update hash a spec's number stands for.
pub(crate) fn pcs_hash(pcs: u8) -> [u8; 32] {
    [pcs.wrapping_add(1); 32]
}

pub(crate) static SIGNER: LazyLock<MemorySigner> =
    LazyLock::new(|| MemorySigner::from_bytes(&[42; 32]));

pub(crate) async fn seal_commit(
    tree: SedimentreeId,
    head: CommitId,
    parents: BTreeSet<CommitId>,
    blob: u8,
    pcs: Option<u8>,
) -> Save<LooseCommit> {
    let verified = VerifiedMeta::seal::<Sendable, _>(
        &*SIGNER,
        (tree, head, parents),
        VerifiedBlobMeta::new(Blob::new(vec![blob])),
    )
    .await
    .expect("a commit with pooled parents seals");
    Save {
        verified,
        pcs_update_hash: pcs.map(pcs_hash),
    }
}

pub(crate) async fn seal_fragment(
    tree: SedimentreeId,
    head: CommitId,
    boundary: BTreeSet<CommitId>,
    checkpoints: Vec<CommitId>,
    blob: u8,
    pcs: Option<u8>,
) -> Save<Fragment> {
    let verified = VerifiedMeta::seal::<Sendable, _>(
        &*SIGNER,
        (tree, head, boundary, checkpoints),
        VerifiedBlobMeta::new(Blob::new(vec![blob])),
    )
    .await
    .expect("a fragment with pooled relations seals");
    Save {
        verified,
        pcs_update_hash: pcs.map(pcs_hash),
    }
}

/// Seal `specs` for `tree`, as the commits and the fragments of one save.
pub(crate) async fn seal(
    tree: SedimentreeId,
    specs: &[Spec],
) -> (Vec<Save<LooseCommit>>, Vec<Save<Fragment>>) {
    let mut commits = Vec::new();
    let mut fragments = Vec::new();
    for spec in specs {
        match spec {
            Spec::Commit {
                head,
                parents,
                blob,
                pcs,
            } => commits.push(
                seal_commit(
                    tree,
                    pooled(*head),
                    parents.iter().map(|parent| pooled(*parent)).collect(),
                    *blob,
                    *pcs,
                )
                .await,
            ),
            Spec::Fragment {
                head,
                boundary,
                checkpoints,
                blob,
                pcs,
            } => fragments.push(
                seal_fragment(
                    tree,
                    pooled(*head),
                    boundary.iter().map(|id| pooled(*id)).collect(),
                    checkpoints.iter().map(|id| pooled(*id)).collect(),
                    *blob,
                    *pcs,
                )
                .await,
            ),
        }
    }
    (commits, fragments)
}

/// Every entry of the window `(after, latest]`, read in one page.
pub(crate) async fn entries_after(store: &Store, after: u64) -> Vec<Entry<SedimentreeId, ()>> {
    let until = store.latest_rev().await.unwrap();
    if until <= after {
        return Vec::new();
    }
    let mut page = Page::default();
    let limits = Limits {
        max_entries: std::num::NonZeroUsize::MAX,
        max_bytes: None,
    };
    store
        .read(&(), after, until, limits, &mut page)
        .await
        .unwrap();
    assert_eq!(page.through, until);
    page.entries
}
