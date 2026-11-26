//! The store against the oracle, after every operation of generated histories; and bugs planted
//! on purpose, which the same differential must catch.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use futures::executor::block_on;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::Config;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_core::sedimentree::Sedimentree;
use sqlite_rs::Database;
use sqlite_rs::SqliteDb;
use subduction_core::minimized_tree_store::Lookup;
use subduction_core::minimized_tree_store::MinimizedTreeStore;
use subduction_core::storage::traits::Storage;

use super::Spec;
use super::Store;
use super::fresh_db;
use super::oracle::Oracle;
use super::pooled;
use super::seal;
use super::store_on;
use super::tree_id;
use crate::sedimentree::Change;
use crate::sedimentree::ChangeFeed;
use crate::sedimentree::Mutation;
use crate::sedimentree::write::PLANT;
use crate::sedimentree::write::Plant;

/// The trees a history touches.
const TREES: u8 = 3;
const PCS_HASHES: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) enum Op {
    Save(u8, Vec<Spec>),
    DeleteCommit(u8, u8),
    DeleteCommits(u8),
    DeleteFragment(u8, u8),
    DeleteFragments(u8),
    Register(u8),
    Retire(u8),
}

impl Op {
    fn tree(&self) -> u8 {
        match self {
            Self::Save(tree, _)
            | Self::DeleteCommit(tree, _)
            | Self::DeleteCommits(tree)
            | Self::DeleteFragment(tree, _)
            | Self::DeleteFragments(tree)
            | Self::Register(tree)
            | Self::Retire(tree) => *tree,
        }
    }
}

fn pool_index() -> impl Strategy<Value = u8> {
    0..super::POOL as u8
}

fn spec() -> impl Strategy<Value = Spec> {
    let pcs = prop::option::of(0..PCS_HASHES);
    let commit = (pool_index(), vec(pool_index(), 0..3), 0..3_u8, pcs.clone()).prop_map(
        |(head, parents, blob, pcs)| Spec::Commit {
            // Parents below the head keep the history acyclic.
            parents: parents
                .into_iter()
                .filter(|parent| *parent < head)
                .collect(),
            head,
            blob,
            pcs,
        },
    );
    // Mostly boundary heads (the pool's depths above 0), so fragments cover ranges.
    let fragment_head = prop_oneof![3 => prop::sample::select(vec![2_u8, 4, 6]), 1 => pool_index()];
    let fragment = (
        fragment_head,
        vec(pool_index(), 0..3),
        vec(pool_index(), 0..4),
        0..2_u8,
        pcs,
    )
        .prop_map(|(head, boundary, checkpoints, blob, pcs)| Spec::Fragment {
            head,
            boundary,
            checkpoints,
            blob,
            pcs,
        });
    prop_oneof![3 => commit, 2 => fragment]
}

pub(crate) fn op() -> impl Strategy<Value = Op> {
    let tree = 1..=TREES;
    prop_oneof![
        10 => (tree.clone(), vec(spec(), 1..5)).prop_map(|(tree, specs)| Op::Save(tree, specs)),
        1 => (tree.clone(), pool_index()).prop_map(|(tree, head)| Op::DeleteCommit(tree, head)),
        1 => tree.clone().prop_map(Op::DeleteCommits),
        1 => (tree.clone(), pool_index()).prop_map(|(tree, head)| Op::DeleteFragment(tree, head)),
        1 => tree.clone().prop_map(Op::DeleteFragments),
        1 => tree.clone().prop_map(Op::Register),
        1 => tree.prop_map(Op::Retire),
    ]
}

/// A store, the oracle it must equal, a second store over the same file that holds its own
/// cache (what another process reading the file sees), and a feed from the start.
struct Harness {
    db: Database,
    store: Store,
    reader: Store,
    oracle: Oracle,
    feed: ChangeFeed<Database>,
    /// The `rev` each tree had when it last changed.
    revs: BTreeMap<SedimentreeId, i64>,
    step: usize,
}

impl Harness {
    fn new() -> Self {
        let db = fresh_db();
        let store = store_on(&db);
        Self {
            feed: store.changes_after(0),
            reader: store_on(&db),
            store,
            db,
            oracle: Oracle::default(),
            revs: BTreeMap::new(),
            step: 0,
        }
    }

    async fn apply(&mut self, op: &Op) {
        let id = tree_id(op.tree());
        let (mutation, changed) = match op {
            Op::Save(_, specs) => {
                let (commits, fragments) = seal(id, specs).await;
                let changed = self.oracle.save(id, commits.clone(), fragments.clone());
                (Mutation::Save { commits, fragments }, changed)
            }
            Op::DeleteCommit(_, head) => (
                Mutation::DeleteCommit(pooled(*head)),
                self.oracle.delete_commit(id, pooled(*head)),
            ),
            Op::DeleteCommits(_) => (Mutation::DeleteCommits, self.oracle.delete_commits(id)),
            Op::DeleteFragment(_, head) => (
                Mutation::DeleteFragment(pooled(*head)),
                self.oracle.delete_fragment(id, pooled(*head)),
            ),
            Op::DeleteFragments(_) => (Mutation::DeleteFragments, self.oracle.delete_fragments(id)),
            Op::Register(_) => (Mutation::Register, self.oracle.register(id)),
            Op::Retire(_) => (Mutation::Retire, self.oracle.retire(id)),
        };

        let before = self.store.latest_rev().await.expect("latest rev");
        if self.step % 3 == 2 {
            // Through a transaction of the caller's, and never told it committed: skipping
            // `committed` is only slower.
            let tx = self.db.begin_write().await.expect("begin");
            let staged = self
                .store
                .apply_in(&tx, id, mutation)
                .await
                .expect("apply_in");
            assert_eq!(staged.changed(), changed, "step {}: {op:?}", self.step);
            Database::commit(tx).await.expect("commit");
        } else {
            self.store.apply(id, mutation).await.expect("apply");
        }
        self.step += 1;

        let after = self.store.latest_rev().await.expect("latest rev");
        let changes = self.feed.poll().await.expect("poll");
        if changed {
            assert!(
                after > before,
                "step {}: {op:?} changed nothing visible to the rev",
                self.step
            );
            let live = self.oracle.trees[&id].live;
            assert_eq!(
                changes,
                vec![Change {
                    tree_id: id,
                    rev: after,
                    live
                }],
                "step {}: {op:?}",
                self.step
            );
            assert!(self.revs.get(&id).is_none_or(|old| *old < after));
            self.revs.insert(id, after);
        } else {
            assert_eq!(
                after, before,
                "step {}: {op:?} moved the rev without changing",
                self.step
            );
            assert!(
                changes.is_empty(),
                "step {}: {op:?}: {changes:?}",
                self.step
            );
        }
        self.check_tree(&self.store, id).await;
        self.check_tree(&self.reader, id).await;
    }

    /// Every read of the tree `id` through `store` is the oracle's.
    async fn check_tree(&self, store: &Store, id: SedimentreeId) {
        let at = self.step;
        let empty = Default::default();
        let model = self.oracle.trees.get(&id).unwrap_or(&empty);
        let heads_of = |tree: &Sedimentree| {
            let mut heads = tree.heads(&CountLeadingZeroBytes);
            heads.sort();
            heads
        };

        assert_eq!(
            Storage::<future_form::Sendable>::contains_sedimentree_id(store, id)
                .await
                .unwrap(),
            model.live,
            "step {at}: contains"
        );
        let commit_ids: BTreeSet<CommitId> = model.commits.keys().copied().collect();
        let fragment_ids: BTreeSet<CommitId> = model.fragments.keys().copied().collect();
        assert_eq!(
            Storage::<future_form::Sendable>::list_commit_ids(store, id)
                .await
                .unwrap(),
            commit_ids.iter().copied().collect(),
            "step {at}: commit ids"
        );
        assert_eq!(
            Storage::<future_form::Sendable>::list_fragment_ids(store, id)
                .await
                .unwrap(),
            fragment_ids.iter().copied().collect(),
            "step {at}: fragment ids"
        );

        let mut commits: Vec<_> = Storage::<future_form::Sendable>::load_loose_commits(store, id)
            .await
            .unwrap()
            .into_iter()
            .map(|c| {
                (
                    c.payload().head(),
                    c.signed().as_bytes().to_vec(),
                    c.blob().contents().clone(),
                )
            })
            .collect();
        commits.sort();
        let expected: Vec<_> = model
            .commits
            .iter()
            .map(|(head, row)| (*head, row.signed.clone(), row.blob.clone()))
            .collect();
        assert_eq!(commits, expected, "step {at}: commits");
        let mut fragments: Vec<_> = Storage::<future_form::Sendable>::load_fragments(store, id)
            .await
            .unwrap()
            .into_iter()
            .map(|f| {
                (
                    f.payload().head(),
                    f.signed().as_bytes().to_vec(),
                    f.blob().contents().clone(),
                )
            })
            .collect();
        fragments.sort();
        let expected: Vec<_> = model
            .fragments
            .iter()
            .map(|(head, row)| (*head, row.signed.clone(), row.blob.clone()))
            .collect();
        assert_eq!(fragments, expected, "step {at}: fragments");

        for head in 0..super::POOL as u8 {
            let head = pooled(head);
            let one = Storage::<future_form::Sendable>::load_loose_commit(store, id, head)
                .await
                .unwrap();
            assert_eq!(
                one.map(|c| c.signed().as_bytes().to_vec()),
                model.commits.get(&head).map(|row| row.signed.clone()),
                "step {at}: load commit {head:?}"
            );
            let one = Storage::<future_form::Sendable>::load_fragment(store, id, head)
                .await
                .unwrap();
            assert_eq!(
                one.map(|f| f.signed().as_bytes().to_vec()),
                model.fragments.get(&head).map(|row| row.signed.clone()),
                "step {at}: load fragment {head:?}"
            );
        }
        let mut metas = Storage::<future_form::Sendable>::load_loose_commit_metas(store, id)
            .await
            .unwrap();
        metas.sort_by_key(|commit| commit.head());
        assert_eq!(
            metas,
            model
                .commits
                .values()
                .map(|row| row.payload.clone())
                .collect::<Vec<_>>(),
            "step {at}: commit metas"
        );
        let mut metas = Storage::<future_form::Sendable>::load_fragment_metas(store, id)
            .await
            .unwrap();
        metas.sort_by_key(|fragment| fragment.head());
        assert_eq!(
            metas,
            model
                .fragments
                .values()
                .map(|row| row.payload.clone())
                .collect::<Vec<_>>(),
            "step {at}: fragment metas"
        );

        let tree = store.tree(id).await.unwrap();
        assert_eq!(tree.is_some(), model.live, "step {at}: tree existence");
        if let Some(tree) = &tree {
            assert!(tree.tree() == &model.tree(), "step {at}: the tree");
            assert_eq!(
                heads_of(tree.tree()),
                heads_of(&model.tree()),
                "step {at}: heads"
            );
            let mut heads = store.heads(id).await.unwrap().unwrap();
            heads.sort();
            assert_eq!(heads, heads_of(&model.tree()), "step {at}: store heads");
        }
        match (
            MinimizedTreeStore::<future_form::Sendable>::get(store, id)
                .await
                .unwrap(),
            model.live,
        ) {
            (Lookup::Hit(hit), true) => assert!(hit.tree() == &model.tree(), "step {at}: a hit"),
            (Lookup::Miss(_), false) => {}
            (Lookup::Hit(_), false) => panic!("step {at}: a hit on a tree that does not exist"),
            (Lookup::Miss(_), true) => panic!("step {at}: a miss on a tree that exists"),
        }
        for pcs in 0..PCS_HASHES {
            let hash = super::pcs_hash(pcs);
            let mut blobs = store.blobs_under(id, hash).await.unwrap();
            blobs.sort();
            assert_eq!(
                blobs,
                if model.live {
                    self.oracle.blobs_under(id, hash)
                } else {
                    Vec::new()
                },
                "step {at}: blobs under {pcs}"
            );
        }
    }

    /// Every tree and the id listing.
    async fn check_all(&self) {
        let live: BTreeSet<_> = self
            .oracle
            .trees
            .iter()
            .filter(|(_, tree)| tree.live)
            .map(|(id, _)| *id)
            .collect();
        for store in [&self.store, &self.reader] {
            assert_eq!(
                Storage::<future_form::Sendable>::load_all_sedimentree_ids(store)
                    .await
                    .unwrap(),
                live.iter().copied().collect(),
                "live ids"
            );
            for tree in 1..=TREES {
                self.check_tree(store, tree_id(tree)).await;
            }
        }
    }
}

async fn run(ops: &[Op]) {
    let mut harness = Harness::new();
    for op in ops {
        harness.apply(op).await;
    }
    harness.check_all().await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// After every operation of a generated history, every read of the store, through the
    /// handle that wrote and through one over the same file that never saw the write, is the
    /// oracle's; the `rev` moves exactly when the oracle changed; and the feed names that tree
    /// at that `rev`.
    #[test]
    fn the_store_equals_the_oracle_after_every_operation(ops in vec(op(), 1..20)) {
        block_on(run(&ops));
    }

    /// The final tree is the minimum of the whole history however its commits were saved, as
    /// long as the fragments come after them: in one batch, or the commits one at a time (in
    /// order, or reversed) and then the fragments in one batch. (Without equivocation: see the
    /// pinned law about it, and the design note on fragments that arrive before their commits.)
    #[test]
    fn commit_order_does_not_change_the_minimum_of_the_whole_history(specs in vec(spec(), 1..10)) {
        block_on(async {
            let id = tree_id(1);
            let mut seen = BTreeSet::new();
            let specs: Vec<_> = specs
                .into_iter()
                .filter(|spec| match spec {
                    Spec::Commit { head, .. } => seen.insert((0, *head)),
                    Spec::Fragment { head, .. } => seen.insert((1, *head)),
                })
                .collect();
            let (commits, fragments) = seal(id, &specs).await;
            let expected = Sedimentree::new(
                fragments.iter().map(|save| save.verified.payload().clone()).collect(),
                commits.iter().map(|save| save.verified.payload().clone()).collect(),
            )
            .minimize(&CountLeadingZeroBytes);

            let batched = store_on(&fresh_db());
            batched
                .apply(id, Mutation::Save { commits: commits.clone(), fragments: fragments.clone() })
                .await
                .unwrap();
            let mut stores = vec![batched];
            for reversed in [false, true] {
                let store = store_on(&fresh_db());
                let mut singles = commits.clone();
                if reversed {
                    singles.reverse();
                }
                for single in singles {
                    store
                        .apply(id, Mutation::Save { commits: vec![single], fragments: vec![] })
                        .await
                        .unwrap();
                }
                store
                    .apply(id, Mutation::Save { commits: vec![], fragments: fragments.clone() })
                    .await
                    .unwrap();
                stores.push(store);
            }
            for store in &stores {
                let tree = store.tree(id).await.unwrap().unwrap();
                prop_assert!(tree.tree() == &expected);
            }
            Ok(())
        })?;
    }
}

/// A bug planted in the store is caught by the differential within a bounded search: the
/// test fails if no generated history makes the store differ from the oracle.
fn assert_caught(plant: Plant) {
    PLANT.with(|slot| slot.set(Some(plant)));
    let config = Config {
        cases: 300,
        max_shrink_iters: 0,
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner =
        TestRunner::new_with_rng(config, TestRng::deterministic_rng(RngAlgorithm::ChaCha));
    let outcome = runner.run(&vec(op(), 1..20), |ops| {
        std::panic::catch_unwind(AssertUnwindSafe(|| block_on(run(&ops))))
            .map_err(|_| TestCaseError::fail("diverged"))
    });
    PLANT.with(|slot| slot.set(None));
    assert!(outcome.is_err(), "the differential did not catch {plant:?}");
}

#[test]
fn a_save_that_leaves_covered_rows_in_place_is_caught() {
    assert_caught(Plant::KeepCovered);
}

#[test]
fn a_save_that_stores_a_covered_item_is_caught() {
    assert_caught(Plant::StoreCovered);
}

#[test]
fn a_delete_that_does_not_move_the_rev_is_caught() {
    assert_caught(Plant::SilentDelete);
}

#[test]
fn a_cache_that_does_not_validate_its_rev_is_caught() {
    assert_caught(Plant::StaleCache);
}
