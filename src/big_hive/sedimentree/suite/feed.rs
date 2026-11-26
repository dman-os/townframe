//! The change feed: a second reader sees every commit, in this process and across them.

use futures::executor::block_on;
use sedimentree_core::id::SedimentreeId;

use super::Spec;
use super::fresh_db;
use super::seal;
use super::store_on;
use super::tree_id;
use crate::sedimentree::Change;
use crate::sedimentree::Mutation;

fn one_commit() -> Vec<Spec> {
    vec![Spec::Commit {
        head: 0,
        parents: vec![],
        blob: 0,
        pcs: None,
    }]
}

async fn save(store: &super::Store, id: SedimentreeId) {
    let (commits, fragments) = seal(id, &one_commit()).await;
    store
        .apply(id, Mutation::Save { commits, fragments })
        .await
        .unwrap();
}

#[test]
fn a_store_over_the_same_file_tails_the_writers_commits_by_polling() {
    block_on(async {
        let db = fresh_db();
        let (writer, other_process) = (store_on(&db), store_on(&db));
        // Tail from now: nothing before it.
        save(&writer, tree_id(9)).await;
        let mut feed = other_process.changes_after(other_process.latest_rev().await.unwrap());
        assert!(feed.poll().await.unwrap().is_empty());

        save(&writer, tree_id(1)).await;
        save(&writer, tree_id(2)).await;
        let changes = feed.poll().await.unwrap();
        assert_eq!(
            changes
                .iter()
                .map(|change| change.tree_id)
                .collect::<Vec<_>>(),
            vec![tree_id(1), tree_id(2)]
        );
        assert!(changes.iter().all(|change| change.live));
        assert!(feed.poll().await.unwrap().is_empty());
        assert_eq!(feed.cursor(), changes[1].rev);
    });
}

#[test]
fn a_tree_changed_twice_is_one_change_at_its_latest_rev_and_a_delete_says_not_live() {
    block_on(async {
        let store = store_on(&fresh_db());
        let mut feed = store.changes_after(0);
        save(&store, tree_id(1)).await;
        let (commits, fragments) = seal(
            tree_id(1),
            &[Spec::Commit {
                head: 1,
                parents: vec![0],
                blob: 0,
                pcs: None,
            }],
        )
        .await;
        store
            .apply(tree_id(1), Mutation::Save { commits, fragments })
            .await
            .unwrap();
        save(&store, tree_id(2)).await;
        store.apply(tree_id(2), Mutation::Retire).await.unwrap();

        let latest = store.latest_rev().await.unwrap();
        let changes = feed.poll().await.unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(
            changes[0],
            Change {
                tree_id: tree_id(1),
                rev: latest - 2,
                live: true
            }
        );
        assert_eq!(
            changes[1],
            Change {
                tree_id: tree_id(2),
                rev: latest,
                live: false
            }
        );
    });
}

#[test]
fn a_poll_reads_every_page() {
    block_on(async {
        let store = store_on(&fresh_db());
        // More trees than a page (3 under test).
        for tree in 0..10_u8 {
            store
                .apply(tree_id(tree), Mutation::Register)
                .await
                .unwrap();
        }
        let mut feed = store.changes_after(0);
        let changes = feed.poll().await.unwrap();
        assert_eq!(changes.len(), 10);
        assert!(changes.windows(2).all(|pair| pair[0].rev < pair[1].rev));
        assert!(feed.poll().await.unwrap().is_empty());
    });
}

#[test]
fn a_commit_in_this_process_wakes_the_feed() {
    block_on(async {
        let store = store_on(&fresh_db());
        let mut feed = store.changes_after(store.latest_rev().await.unwrap());
        let writer = store.clone();
        let (changes, ()) = futures::join!(feed.next_local(), save(&writer, tree_id(1)));
        assert_eq!(
            changes
                .unwrap()
                .iter()
                .map(|change| change.tree_id)
                .collect::<Vec<_>>(),
            vec![tree_id(1)]
        );
    });
}

#[test]
fn a_commit_never_reported_to_the_store_is_found_by_the_next_poll() {
    block_on(async {
        use sqlite_rs::SqliteDb;
        let db = fresh_db();
        let store = store_on(&db);
        let mut feed = store.changes_after(0);
        let (commits, fragments) = seal(tree_id(1), &one_commit()).await;
        let tx = db.begin_write().await.unwrap();
        let staged = store
            .apply_in(&tx, tree_id(1), Mutation::Save { commits, fragments })
            .await
            .unwrap();
        sqlite_rs::Database::commit(tx).await.unwrap();
        drop(staged);
        assert_eq!(feed.poll().await.unwrap().len(), 1);
    });
}
