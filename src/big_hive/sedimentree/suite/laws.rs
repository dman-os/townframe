//! The laws of `sedimentree.rs`, one named test each: what a hand-built history does to the
//! rows, the `rev` and the caller's transaction.

use future_form::Sendable;
use futures::executor::block_on;
use sedimentree_core::loose_commit::id::CommitId;
use sqlite_rs::Database;
use sqlite_rs::SqliteDb;
use subduction_core::minimized_tree_store::MinimizedTreeStore;
use subduction_core::minimized_tree_store::Populate;
use subduction_core::storage::traits::Storage;

use super::Spec;
use super::Store;
use super::entries_after;
use super::fresh_db;
use super::pooled;
use super::seal;
use super::store_on;
use super::tree_id;
use crate::sedimentree::Mutation;
use big_automata::rev::RevSource;

/// Heads of the pool by role: commits 0, 1 and 3 are ordinary, 2 is a boundary (depth 1), 4 is
/// deeper (depth 2).
const C0: u8 = 0;
const C1: u8 = 1;
const B2: u8 = 2;
const C3: u8 = 3;
const D4: u8 = 4;

fn commit(head: u8, parents: &[u8], blob: u8) -> Spec {
    Spec::Commit {
        head,
        parents: parents.to_vec(),
        blob,
        pcs: None,
    }
}

fn fragment(head: u8, boundary: &[u8], checkpoints: &[u8], blob: u8) -> Spec {
    Spec::Fragment {
        head,
        boundary: boundary.to_vec(),
        checkpoints: checkpoints.to_vec(),
        blob,
        pcs: None,
    }
}

async fn save(store: &Store, tree: u8, specs: &[Spec]) -> (usize, Option<u64>) {
    let id = tree_id(tree);
    let (commits, fragments) = seal(id, specs).await;
    let tx = store.db.begin_write().await.expect("begin");
    let mut batch = store.batch();
    let applied = store
        .apply_in(&tx, &mut batch, id, Mutation::Save { commits, fragments })
        .await
        .expect("save");
    Database::commit(tx).await.expect("commit");
    let outcome = (applied.saved, batch.rev());
    store.committed(batch);
    outcome
}

async fn commit_heads(store: &Store, tree: u8) -> Vec<CommitId> {
    let mut heads: Vec<_> = Storage::<Sendable>::list_commit_ids(store, tree_id(tree))
        .await
        .unwrap()
        .into_iter()
        .collect();
    heads.sort();
    heads
}

async fn fragment_heads(store: &Store, tree: u8) -> Vec<CommitId> {
    let mut heads: Vec<_> = Storage::<Sendable>::list_fragment_ids(store, tree_id(tree))
        .await
        .unwrap()
        .into_iter()
        .collect();
    heads.sort();
    heads
}

/// How many rows the tables physically hold, by a count of their own, not through the store.
async fn physical_rows(db: &Database) -> (i64, i64) {
    let count = async |table: &str| {
        db.query(&format!("SELECT count(*) AS n FROM {table}"), ())
            .await
            .unwrap()
            .rows[0]
            .get::<i64>("n")
            .unwrap()
    };
    (
        count("sedimentree_commits").await,
        count("sedimentree_fragments").await,
    )
}

/// A chain of commits, then the fragment of the boundary commit 2: the fragment covers 2 and
/// its ancestors 1 and 0, and the commit above it, 3, stays.
async fn chain_then_fragment(store: &Store) {
    save(store, 1, &[commit(C0, &[], 0)]).await;
    save(store, 1, &[commit(C1, &[C0], 0)]).await;
    save(store, 1, &[commit(B2, &[C1], 0)]).await;
    save(store, 1, &[commit(C3, &[B2], 0)]).await;
}

#[test]
fn law_1_a_fragment_deletes_the_commits_it_covers_in_its_own_transaction() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        chain_then_fragment(&store).await;
        assert_eq!(commit_heads(&store, 1).await.len(), 4);
        assert_eq!(physical_rows(&db).await, (4, 0));

        let (saved, rev) = save(&store, 1, &[fragment(B2, &[5], &[C1, C0], 0)]).await;
        assert_eq!((saved, rev.is_some()), (1, true));
        // Only 3 is left, and the rows (blobs with them) are physically gone, not hidden.
        assert_eq!(commit_heads(&store, 1).await, vec![pooled(C3)]);
        assert_eq!(fragment_heads(&store, 1).await, vec![pooled(B2)]);
        assert_eq!(physical_rows(&db).await, (1, 1));
    });
}

#[test]
fn law_1_a_commit_that_arrives_covered_is_never_stored() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        // The fragment of boundary commit 2 first, then commit 2 itself: it heads the stored
        // fragment, so it is covered on arrival.
        save(&store, 1, &[fragment(B2, &[5], &[C1, C0], 0)]).await;
        let rev = store.latest_rev().await.unwrap();
        let (saved, changed) = save(&store, 1, &[commit(B2, &[C1], 0)]).await;
        assert_eq!((saved, changed), (0, None));
        assert_eq!(physical_rows(&db).await, (0, 1));
        assert_eq!(store.latest_rev().await.unwrap(), rev);
    });
}

/// Pinned, and a design question (see the report): a commit is covered by the range of the
/// boundary commit it descends to *in the commits the tree holds*, and a pruned commit is no
/// longer held. So once commit 2 is pruned, commit 1 arriving again, though the fragment lists
/// it among its checkpoints, anchors to nothing and is stored loose. In-memory minimization
/// (subduction's own) behaves the same; a rule that also drops a commit listed in a stored
/// fragment's head or checkpoints would make pruned stay pruned.
#[test]
fn law_1_a_pruned_commit_that_arrives_again_is_stored_again_once_its_anchor_is_gone() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        chain_then_fragment(&store).await;
        save(&store, 1, &[fragment(B2, &[5], &[C1, C0], 0)]).await;
        assert_eq!(physical_rows(&db).await, (1, 1));
        let (saved, _) = save(&store, 1, &[commit(C1, &[C0], 0)]).await;
        assert_eq!(saved, 1);
        assert_eq!(physical_rows(&db).await, (2, 1));
    });
}

/// Pinned: pruning is not undone by a later payload of a stored head. The fragment of head 4
/// with boundary 6 supports the fragment of head 6, which is pruned; a second fragment of head
/// 4 (boundary 0, an equivocation) then replaces the first by digest and does not support it,
/// and fragment 6 does not come back. The minimum of the whole history differs.
#[test]
fn law_1_an_equivocating_fragment_does_not_restore_what_the_one_it_replaced_pruned() {
    block_on(async {
        let specs = [
            fragment(4, &[6], &[], 0),
            fragment(6, &[], &[], 0),
            fragment(4, &[0], &[], 0),
        ];
        let db = fresh_db();
        let store = store_on(&db);
        for spec in &specs {
            save(&store, 1, std::slice::from_ref(spec)).await;
        }
        let incremental = store.tree(tree_id(1)).await.unwrap().unwrap();

        let whole = store_on(&fresh_db());
        save(&whole, 1, &specs).await;
        let whole = whole.tree(tree_id(1)).await.unwrap().unwrap();
        assert!(incremental.tree() != whole.tree());
        assert_eq!(whole.tree().fragments().count(), 2);
        assert_eq!(incremental.tree().fragments().count(), 1);
    });
}

#[test]
fn law_1_a_deeper_fragment_deletes_the_fragment_it_dominates() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        save(&store, 1, &[fragment(B2, &[5], &[C1], 0)]).await;
        assert_eq!(fragment_heads(&store, 1).await, vec![pooled(B2)]);
        // Deeper, and its range holds the shallower fragment's head and boundary.
        save(&store, 1, &[fragment(D4, &[5], &[B2], 0)]).await;
        assert_eq!(fragment_heads(&store, 1).await, vec![pooled(D4)]);
        assert_eq!(physical_rows(&db).await, (0, 1));
        // And the dominated one is not stored again.
        let (saved, rev) = save(&store, 1, &[fragment(B2, &[5], &[C1], 0)]).await;
        assert_eq!((saved, rev), (0, None));
    });
}

#[test]
fn law_1_of_two_payloads_of_one_head_the_lowest_digest_stays_in_either_order() {
    block_on(async {
        let mut winners = Vec::new();
        for order in [[1, 2], [2, 1]] {
            let db = fresh_db();
            let store = store_on(&db);
            for blob in order {
                save(&store, 1, &[commit(C1, &[C0], blob)]).await;
            }
            assert_eq!(physical_rows(&db).await, (1, 0));
            let kept = Storage::<Sendable>::load_loose_commit(&store, tree_id(1), pooled(C1))
                .await
                .unwrap()
                .unwrap();
            winners.push(kept.signed().as_bytes().to_vec());
        }
        assert_eq!(winners[0], winners[1]);
    });
}

#[test]
fn law_2_the_rev_moves_exactly_when_the_tree_changes() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        let id = tree_id(1);
        let mut last = store.latest_rev().await.unwrap();
        assert_eq!(last, 0);
        let mut moved = async |expect: bool, what: &str| {
            let now = store.latest_rev().await.unwrap();
            assert_eq!(now > last, expect, "{what}");
            assert!(now >= last);
            last = now;
        };

        save(&store, 1, &[commit(C0, &[], 0)]).await;
        moved(true, "the first save").await;
        save(&store, 1, &[commit(C0, &[], 0)]).await;
        moved(false, "a duplicate").await;
        store.apply(id, Mutation::Register).await.unwrap();
        moved(false, "registering a live tree").await;
        store
            .apply(id, Mutation::DeleteCommit(pooled(C1)))
            .await
            .unwrap();
        moved(false, "deleting a commit that is not there").await;
        store.apply(id, Mutation::DeleteFragments).await.unwrap();
        moved(false, "deleting fragments of a tree without any").await;
        store
            .apply(id, Mutation::DeleteCommit(pooled(C0)))
            .await
            .unwrap();
        moved(true, "deleting a commit").await;
        store.apply(id, Mutation::Retire).await.unwrap();
        moved(true, "retiring").await;
        store.apply(id, Mutation::Retire).await.unwrap();
        moved(false, "retiring a dead tree").await;
        store.apply(id, Mutation::DeleteCommits).await.unwrap();
        moved(false, "deleting from a dead tree").await;
        store.apply(id, Mutation::Register).await.unwrap();
        moved(true, "registering a dead tree").await;
        // Another tree's change does not move this tree's row.
        let before = Storage::<Sendable>::contains_sedimentree_id(&store, id)
            .await
            .unwrap();
        save(&store, 2, &[commit(C0, &[], 0)]).await;
        moved(true, "another tree").await;
        assert!(before);
    });
}

#[test]
fn law_3_a_rolled_back_mutation_leaves_no_row_no_rev_and_no_feed_entry() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        save(&store, 1, &[commit(C0, &[], 0)]).await;
        let rev = store.latest_rev().await.unwrap();

        let tx = db.begin_write().await.unwrap();
        let mut batch = store.batch();
        for tree in [1, 2] {
            let (commits, fragments) = seal(tree_id(tree), &[commit(C1, &[C0], 0)]).await;
            let applied = store
                .apply_in(
                    &tx,
                    &mut batch,
                    tree_id(tree),
                    Mutation::Save { commits, fragments },
                )
                .await
                .unwrap();
            assert!(applied.changed);
        }
        // The transaction reads its own writes; nobody else does.
        assert_eq!(store.latest_rev().await.unwrap(), rev);
        Database::rollback(tx).await.unwrap();

        assert_eq!(store.latest_rev().await.unwrap(), rev);
        assert_eq!(commit_heads(&store, 1).await, vec![pooled(C0)]);
        assert!(
            !Storage::<Sendable>::contains_sedimentree_id(&store, tree_id(2))
                .await
                .unwrap()
        );
        assert!(entries_after(&store, rev).await.is_empty());
        // The next commit reuses the revs the rollback never consumed.
        save(&store, 1, &[commit(C1, &[C0], 0)]).await;
        assert_eq!(store.latest_rev().await.unwrap(), rev + 1);
    });
}

#[test]
fn law_3_the_trees_of_one_batch_share_a_rev_and_two_batches_of_one_transaction_do_not() {
    block_on(async {
        let db = fresh_db();
        let store = store_on(&db);
        let tx = db.begin_write().await.unwrap();
        let mut first = store.batch();
        let mut second = store.batch();
        for (batch, trees) in [(&mut first, [1, 2]), (&mut second, [3, 3])] {
            for tree in trees {
                let (commits, fragments) = seal(tree_id(tree), &[commit(C0, &[], 0)]).await;
                store
                    .apply_in(
                        &tx,
                        batch,
                        tree_id(tree),
                        Mutation::Save { commits, fragments },
                    )
                    .await
                    .unwrap();
            }
        }
        Database::commit(tx).await.unwrap();
        assert_eq!((first.rev(), second.rev()), (Some(1), Some(2)));
        store.committed(first);
        store.committed(second);
        let entries = entries_after(&store, 0).await;
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.key, entry.rev))
                .collect::<Vec<_>>(),
            vec![(tree_id(1), 1), (tree_id(2), 1), (tree_id(3), 2)]
        );
    });
}

#[test]
fn law_4_a_tree_never_comes_back_with_a_rev_a_reader_may_hold() {
    block_on(async {
        let store = store_on(&fresh_db());
        let id = tree_id(1);
        // A reader misses a tree never written, and holds that generation.
        let unseen = match MinimizedTreeStore::<Sendable>::get(&store, id)
            .await
            .unwrap()
        {
            subduction_core::minimized_tree_store::Lookup::Miss(generation) => generation,
            subduction_core::minimized_tree_store::Lookup::Hit(_) => {
                panic!("an unwritten tree hit")
            }
        };
        store.apply(id, Mutation::Register).await.unwrap();
        store.apply(id, Mutation::Retire).await.unwrap();
        // The tree reads as absent again, but not as never written.
        let populated =
            MinimizedTreeStore::<Sendable>::populate(&store, id, Default::default(), unseen)
                .await
                .unwrap();
        assert_eq!(populated, Populate::Stale);

        let tombstoned = match MinimizedTreeStore::<Sendable>::get(&store, id)
            .await
            .unwrap()
        {
            subduction_core::minimized_tree_store::Lookup::Miss(generation) => generation,
            subduction_core::minimized_tree_store::Lookup::Hit(_) => panic!("a retired tree hit"),
        };
        store.apply(id, Mutation::Register).await.unwrap();
        store.apply(id, Mutation::Retire).await.unwrap();
        let populated =
            MinimizedTreeStore::<Sendable>::populate(&store, id, Default::default(), tombstoned)
                .await
                .unwrap();
        assert_eq!(populated, Populate::Stale);
    });
}

#[test]
fn law_6_a_second_store_sees_a_write_through_its_own_cache() {
    block_on(async {
        let db = fresh_db();
        let (writer, reader) = (store_on(&db), store_on(&db));
        save(&writer, 1, &[commit(C0, &[], 0)]).await;
        let first = reader.tree(tree_id(1)).await.unwrap().unwrap();
        assert_eq!(first.tree().loose_commits().count(), 1);
        // The reader's cache now holds the tree at the old rev.
        save(&writer, 1, &[commit(C1, &[C0], 0)]).await;
        let second = reader.tree(tree_id(1)).await.unwrap().unwrap();
        assert_eq!(second.tree().loose_commits().count(), 2);
        writer.apply(tree_id(1), Mutation::Retire).await.unwrap();
        assert!(reader.tree(tree_id(1)).await.unwrap().is_none());
    });
}

#[test]
fn the_pcs_update_hash_is_a_column_the_caller_supplies() {
    block_on(async {
        let store = store_on(&fresh_db());
        let id = tree_id(1);
        let encrypted = Spec::Commit {
            head: C0,
            parents: vec![],
            blob: 7,
            pcs: Some(0),
        };
        let other = Spec::Commit {
            head: C1,
            parents: vec![C0],
            blob: 8,
            pcs: Some(1),
        };
        let plain = commit(C3, &[B2], 9);
        save(&store, 1, &[encrypted, other, plain]).await;
        assert_eq!(
            store.blobs_under(id, super::pcs_hash(0)).await.unwrap(),
            vec![vec![7]]
        );
        assert_eq!(
            store.blobs_under(id, super::pcs_hash(1)).await.unwrap(),
            vec![vec![8]]
        );
        assert!(
            store
                .blobs_under(id, super::pcs_hash(2))
                .await
                .unwrap()
                .is_empty()
        );
        // A fragment under the same update joins it; covering a commit removes its blob.
        save(
            &store,
            1,
            &[Spec::Commit {
                head: B2,
                parents: vec![C1],
                blob: 11,
                pcs: None,
            }],
        )
        .await;
        save(
            &store,
            1,
            &[Spec::Fragment {
                head: B2,
                boundary: vec![5],
                checkpoints: vec![C1, C0],
                blob: 10,
                pcs: Some(0),
            }],
        )
        .await;
        assert_eq!(
            store.blobs_under(id, super::pcs_hash(0)).await.unwrap(),
            vec![vec![10]]
        );
    });
}
