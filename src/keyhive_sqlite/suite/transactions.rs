//! Writes in a transaction the caller owns: what commits and rolls back with it, and what a
//! snapshot does and does not see.

use std::sync::Arc;

use future_form::Local;
use futures::executor::block_on;
use keyhive_core::store::KeyhiveStore;
use sqlite_rs::Database;
use sqlite_rs::QueryExecutor;
use sqlite_rs::SqliteDb;

use super::POOL;
use super::Ref;
use super::Store;
use super::content_key;
use super::fresh_store;

const TABLES: [&str; 9] = [
    "kh_dek",
    "kh_key_pair",
    "kh_prekey_op",
    "kh_cgka_op",
    "kh_doc_epoch",
    "kh_cgka_op_pred",
    "kh_content_heads",
    "kh_content_key",
    "kh_pending_event",
];

async fn rows(db: &impl QueryExecutor) -> Vec<(&'static str, i64)> {
    let mut counts = Vec::new();
    for table in TABLES {
        let count = db
            .query(&format!("SELECT count(*) FROM {table}"), ())
            .await
            .unwrap()
            .one()
            .unwrap();
        counts.push((table, count));
    }
    counts
}

/// One of everything, through a session of `tx`.
async fn write_everything(store: &Store, tx: &sqlite_rs::Tx) {
    let pool = &*POOL;
    let s = store.session(tx);
    s.put_key_pairs(&pool.key_pairs).await.unwrap();
    let ops: Vec<_> = pool.key_ops.iter().map(|(_, op)| Arc::clone(op)).collect();
    s.append_prekey_ops(pool.individuals[0], &ops[..1])
        .await
        .unwrap();
    let cgka: Vec<_> = pool
        .cgka_ops
        .iter()
        .filter(|(d, _)| *d == 0)
        .map(|(_, op)| Arc::clone(op))
        .collect();
    s.append_cgka_ops(pool.docs[0], &cgka).await.unwrap();
    s.put_content_heads(pool.docs[0], &pool.refs).await.unwrap();
    s.cache_content_key(pool.docs[0], &pool.refs[0], &content_key(0, 0))
        .await
        .unwrap();
    s.put_pending_events(&pool.events).await.unwrap();
}

#[test]
fn a_rolled_back_transaction_leaves_nothing_and_a_later_write_starts_clean() {
    block_on(async {
        let (db, store) = fresh_store();
        let tx = db.begin_write().await.unwrap();
        write_everything(&store, &tx).await;
        // The transaction reads its own writes, DEKs included.
        let counts = rows(&tx).await;
        assert!(counts.iter().all(|(_, n)| *n > 0), "{counts:?}");
        assert_eq!(
            store.session(&tx).load_key_pairs().await.unwrap().len(),
            POOL.key_pairs.len()
        );
        Database::rollback(tx).await.unwrap();

        let counts = rows(&db).await;
        assert!(counts.iter().all(|(_, n)| *n == 0), "{counts:?}");
        assert!(
            KeyhiveStore::<Local, Ref>::load_key_pairs(&store)
                .await
                .unwrap()
                .is_empty()
        );

        // A DEK made inside the rolled-back transaction is gone with it: the next write makes
        // its own, and its secrets read back.
        KeyhiveStore::<Local, Ref>::put_key_pairs(&store, &POOL.key_pairs)
            .await
            .unwrap();
        assert_eq!(
            KeyhiveStore::<Local, Ref>::load_key_pairs(&store)
                .await
                .unwrap()
                .len(),
            POOL.key_pairs.len()
        );
    });
}

#[test]
fn a_dropped_transaction_rolls_back_too() {
    block_on(async {
        let (db, store) = fresh_store();
        {
            let tx = db.begin_write().await.unwrap();
            write_everything(&store, &tx).await;
        }
        let counts = rows(&db).await;
        assert!(counts.iter().all(|(_, n)| *n == 0), "{counts:?}");
    });
}

#[test]
fn keyhive_rows_commit_with_the_rows_of_another_store_in_one_transaction() {
    block_on(async {
        let (db, store) = fresh_store();
        db.execute_batch("CREATE TABLE other_store (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        let tx = db.begin_write().await.unwrap();
        write_everything(&store, &tx).await;
        tx.execute("INSERT INTO other_store (id) VALUES (1)", ())
            .await
            .unwrap();
        // A snapshot taken now is from before the commit: it sees none of either.
        let before = db.begin_read().await.unwrap();
        Database::commit(tx).await.unwrap();

        assert!(
            store
                .session(&before)
                .load_key_pairs()
                .await
                .unwrap()
                .is_empty()
        );
        let other: i64 = before
            .query("SELECT count(*) FROM other_store", ())
            .await
            .unwrap()
            .one()
            .unwrap();
        assert_eq!(other, 0);
        Database::end_read(before).await.unwrap();

        let after = db.begin_read().await.unwrap();
        assert_eq!(
            store.session(&after).load_key_pairs().await.unwrap().len(),
            POOL.key_pairs.len()
        );
        let other: i64 = after
            .query("SELECT count(*) FROM other_store", ())
            .await
            .unwrap()
            .one()
            .unwrap();
        assert_eq!(other, 1);
        Database::end_read(after).await.unwrap();
    });
}

#[test]
fn a_failed_write_through_the_trait_leaves_nothing() {
    block_on(async {
        let (db, store) = fresh_store();
        // Key pairs, then content keys under a table that will not take them: the second write
        // fails inside its own transaction, and the first is already committed by its own.
        KeyhiveStore::<Local, Ref>::put_key_pairs(&store, &POOL.key_pairs)
            .await
            .unwrap();
        db.execute_batch("DROP TABLE kh_content_key").await.unwrap();
        let failed = KeyhiveStore::<Local, Ref>::cache_content_key(
            &store,
            POOL.docs[0],
            &POOL.refs[0],
            &content_key(0, 0),
        )
        .await;
        assert!(failed.is_err());
        // The DEK the failed write created in its transaction did not survive it.
        let deks: i64 = db
            .query(
                "SELECT count(*) FROM kh_dek WHERE dek_id = 'content_key'",
                (),
            )
            .await
            .unwrap()
            .one()
            .unwrap();
        assert_eq!(deks, 0);
    });
}

#[test]
fn a_documents_epoch_counts_the_operations_it_holds_and_only_what_commits() {
    block_on(async {
        let (db, store) = fresh_store();
        let pool = &*POOL;
        let ops_of = |doc: usize| -> Vec<_> {
            pool.cgka_ops
                .iter()
                .filter(|(d, _)| *d == doc)
                .map(|(_, op)| Arc::clone(op))
                .collect()
        };
        let (zero, one) = (ops_of(0), ops_of(1));
        assert!(zero.len() >= 2 && !one.is_empty());

        let tx = db.begin_write().await.unwrap();
        assert_eq!(
            epoch_of(&store, &tx, pool.docs[0]).await,
            0,
            "no operation, epoch 0"
        );
        store
            .session(&tx)
            .append_cgka_ops(pool.docs[0], &zero[..1])
            .await
            .unwrap();
        assert_eq!(epoch_of(&store, &tx, pool.docs[0]).await, 1);
        // An operation already held is ignored by the insert, and so does not move the epoch.
        store
            .session(&tx)
            .append_cgka_ops(pool.docs[0], &zero[..1])
            .await
            .unwrap();
        assert_eq!(epoch_of(&store, &tx, pool.docs[0]).await, 1);
        store
            .session(&tx)
            .append_cgka_ops(pool.docs[0], &zero[1..])
            .await
            .unwrap();
        assert_eq!(epoch_of(&store, &tx, pool.docs[0]).await, zero.len() as u64);
        assert_eq!(
            epoch_of(&store, &tx, pool.docs[1]).await,
            0,
            "another document's epoch is its own"
        );
        Database::rollback(tx).await.unwrap();

        // The rollback took the bumps with it.
        let snapshot = db.begin_read().await.unwrap();
        assert_eq!(epoch_of(&store, &snapshot, pool.docs[0]).await, 0);
        Database::end_read(snapshot).await.unwrap();

        let tx = db.begin_write().await.unwrap();
        store
            .session(&tx)
            .append_cgka_ops(pool.docs[1], &one)
            .await
            .unwrap();
        Database::commit(tx).await.unwrap();
        let snapshot = db.begin_read().await.unwrap();
        assert_eq!(
            epoch_of(&store, &snapshot, pool.docs[1]).await,
            one.len() as u64
        );
        assert_eq!(epoch_of(&store, &snapshot, pool.docs[0]).await, 0);
        Database::end_read(snapshot).await.unwrap();
    });
}

async fn epoch_of(
    store: &Store,
    tx: &impl QueryExecutor,
    doc: keyhive_core::principal::document::id::DocumentId,
) -> u64 {
    store.session(tx).doc_epoch(doc).await.unwrap()
}
