//! What SQLite adds to the laws: an interrupted write leaves nothing, and a reopened file holds
//! exactly what committed.

use std::path::Path;

use futures::executor::block_on;
use sqlite_rs::Database;
use sqlite_rs::MigrationState;

use super::RUNTIME;
use super::Spec;
use super::Store;
use super::invariants::assert_invariants;
use super::invariants::dump;
use super::seal;
use super::store_on;
use super::tree_id;
use crate::MIGRATION_TABLE;
use crate::MIGRATIONS;
use crate::sedimentree::Mutation;

fn open_file(path: &Path) -> Database {
    RUNTIME.database(path, MIGRATIONS, MIGRATION_TABLE)
}

fn commit(head: u8, parents: &[u8]) -> Spec {
    Spec::Commit {
        head,
        parents: parents.to_vec(),
        blob: 0,
        pcs: Some(0),
    }
}

async fn save(store: &Store, tree: u8, specs: &[Spec]) {
    let (commits, fragments) = seal(tree_id(tree), specs).await;
    store
        .apply(tree_id(tree), Mutation::Save { commits, fragments })
        .await
        .unwrap();
}

/// A write transaction that never commits, abandoned part way (dropped, as a crashed task would
/// leave it), leaves nothing; and a file reopened later holds exactly what committed.
#[test]
fn an_interrupted_write_leaves_nothing_and_reopening_sees_the_committed_state() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hive.sqlite");
        let db = open_file(&path);
        let store = store_on(&db);

        save(&store, 1, &[commit(0, &[]), commit(1, &[0])]).await;
        save(&store, 2, &[commit(0, &[])]).await;
        save(
            &store,
            1,
            &[Spec::Fragment {
                head: 2,
                boundary: vec![5],
                checkpoints: vec![1, 0],
                blob: 3,
                pcs: None,
            }],
        )
        .await;
        assert_invariants(&db).await;
        let committed = dump(&db).await;
        let rev = store.latest_rev().await.unwrap();

        // Applied inside the transaction, and the transaction abandoned: a fragment that
        // prunes, a retire, a new tree.
        let tx = db.begin_write().await.unwrap();
        for (tree, mutation) in [
            (1, Mutation::Retire),
            (2, {
                let (commits, fragments) =
                    seal(tree_id(2), &[commit(1, &[0]), commit(3, &[1])]).await;
                Mutation::Save { commits, fragments }
            }),
            (3, Mutation::Register),
        ] {
            let staged = store.apply_in(&tx, tree_id(tree), mutation).await.unwrap();
            assert!(staged.changed());
        }
        drop(tx);
        assert_eq!(dump(&db).await, committed);
        drop(store);
        drop(db);

        let reopened_db = open_file(&path);
        assert_eq!(dump(&reopened_db).await, committed);
        assert_invariants(&reopened_db).await;
        let reopened = store_on(&reopened_db);
        assert_eq!(reopened.latest_rev().await.unwrap(), rev);
        // And it carries on from the committed rev.
        save(&reopened, 2, &[commit(1, &[0])]).await;
        assert_eq!(reopened.latest_rev().await.unwrap(), rev + 1);
        assert_invariants(&reopened_db).await;
    });
}

#[test]
fn migrations_apply_from_empty_and_reopening_applies_nothing_twice() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hive.sqlite");
        let db = open_file(&path);
        assert!(
            db.migration_status()
                .await
                .unwrap()
                .iter()
                .all(|(_, state)| *state == MigrationState::Pending),
            "a new file has applied nothing"
        );
        db.migrate().await.unwrap();
        db.migrate().await.unwrap();
        db.apply_migrations(MIGRATION_TABLE, MIGRATIONS)
            .await
            .unwrap();
        let status = db.migration_status().await.unwrap();
        assert_eq!(status.len(), MIGRATIONS.len());
        assert!(
            status
                .iter()
                .all(|(_, state)| *state == MigrationState::Applied)
        );
        drop(db);
        let again = open_file(&path);
        again.migrate().await.unwrap();
        assert_invariants(&again).await;
    });
}
