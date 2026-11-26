//! Inserts in a transaction the caller owns: what commits and rolls back with it, what the
//! verification memo does and does not carry, and what a reopened store makes of the rows.

use std::collections::BTreeMap;

use future_form::Local;
use futures::executor::block_on;
use keyhive_crypto::identifier::Identifier;
use keyline::contract::Keyline;
use keyline::contract::Version;
use keyline::power::Power;
use keyline::test_utils::assume_verified;
use keyline::test_utils::conformance::ALICE;
use keyline::test_utils::conformance::BOB;
use keyline::test_utils::conformance::CAROL;
use keyline::test_utils::conformance::DOC;
use keyline::test_utils::conformance::OWNERS;
use keyline::test_utils::conformance::d;
use keyline::test_utils::conformance::r;
use keyline::test_utils::id;
use sqlite_rs::Database;
use sqlite_rs::QueryExecutor;

use super::fresh_db;
use super::sign;
use crate::Error;
use crate::SqliteKeyline;
use crate::StoredFault;
use sqlite_rs::SqliteDb;

type Store = SqliteKeyline<(), Database>;

async fn count(db: &impl QueryExecutor) -> i64 {
    db.query("SELECT count(*) FROM keyline_certificates", ())
        .await
        .unwrap()
        .one()
        .unwrap()
}

async fn stored_version(db: &impl QueryExecutor) -> i64 {
    db.query("SELECT version FROM keyline_meta", ())
        .await
        .unwrap()
        .one()
        .unwrap()
}

async fn members(store: &Store, subject: u8) -> BTreeMap<Identifier, Power> {
    Keyline::<Local>::members(store, id(subject))
        .await
        .unwrap()
        .value
}

/// The document's owners, and Alice an owner.
fn root() -> Vec<keyline::certificate::VerifiedCertificate<()>> {
    vec![
        sign(d(DOC, OWNERS, DOC, Power::Admin)),
        sign(d(OWNERS, ALICE, OWNERS, Power::Admin)),
    ]
}

/// A batch that adds a member and revokes another route: it moves `members(DOC)`.
fn grants() -> Vec<keyline::certificate::VerifiedCertificate<()>> {
    let bob = d(ALICE, BOB, DOC, Power::Edit);
    vec![
        sign(bob),
        sign(d(ALICE, CAROL, DOC, Power::Read)),
        sign(r::<()>(ALICE, &bob)),
    ]
}

#[test]
fn a_rolled_back_batch_is_invisible_and_leaves_the_version_alone() {
    block_on(async {
        let db = fresh_db();
        let store = Store::new(db.clone());
        let v1 = Keyline::<Local>::insert(&store, root()).await.unwrap();
        let before = members(&store, DOC).await;
        let certificates_before = Keyline::<Local>::certificates(&store).await.unwrap().value;
        let snapshot = db.begin_read().await.unwrap();

        let tx = db.begin_write().await.unwrap();
        let staged = store.insert_in(&tx, grants()).await.unwrap();
        assert!(staged.changed());
        assert_eq!(staged.version(), v1.next());
        // The transaction reads its own writes: the rows, and the moved version.
        assert_eq!(count(&tx).await, 5);
        assert_eq!(stored_version(&tx).await, 2);
        // A snapshot taken before does not, and neither does a reader that starts now.
        assert_eq!(count(&snapshot).await, 2);
        assert_eq!(count(&db).await, 2);

        Database::rollback(tx).await.unwrap();
        drop(staged);

        assert_eq!(count(&db).await, 2, "no rows");
        assert_eq!(stored_version(&db).await, 1, "no version bump");
        assert_eq!(Keyline::<Local>::version(&store).await.unwrap(), v1);
        assert_eq!(members(&store, DOC).await, before);
        assert_eq!(
            Keyline::<Local>::certificates(&store).await.unwrap().value,
            certificates_before
        );
        // A store that never saw the transaction agrees.
        assert_eq!(members(&Store::new(db.clone()), DOC).await, before);
        // And the version the rolled back batch would have taken is the next insert's.
        assert_eq!(
            Keyline::<Local>::insert(&store, grants()).await.unwrap(),
            v1.next()
        );
        assert_ne!(members(&store, DOC).await, before);
    });
}

#[test]
fn a_grant_commits_and_rolls_back_with_the_rows_of_other_stores() {
    block_on(async {
        let db = fresh_db();
        db.execute_batch("CREATE TABLE sedimentree_parts (part TEXT NOT NULL)")
            .await
            .unwrap();
        let store = Store::new(db.clone());
        let other_rows = || async {
            db.query("SELECT count(*) FROM sedimentree_parts", ())
                .await
                .unwrap()
                .one::<i64>()
                .unwrap()
        };

        let tx = db.begin_write().await.unwrap();
        tx.execute("INSERT INTO sedimentree_parts VALUES ('discarded')", ())
            .await
            .unwrap();
        let staged = store.insert_in(&tx, root()).await.unwrap();
        Database::rollback(tx).await.unwrap();
        drop(staged);
        assert_eq!((other_rows().await, count(&db).await), (0, 0));

        let tx = db.begin_write().await.unwrap();
        tx.execute("INSERT INTO sedimentree_parts VALUES ('kept')", ())
            .await
            .unwrap();
        let staged = store.insert_in(&tx, root()).await.unwrap();
        // Nothing outside sees the commit until it happens, and the version does not move
        // before it.
        assert_eq!(
            Keyline::<Local>::version(&store).await.unwrap(),
            Version::INITIAL
        );
        Database::commit(tx).await.unwrap();
        let version = staged.version();
        store.committed(staged);
        assert_eq!((other_rows().await, count(&db).await), (1, 2));
        assert_eq!(Keyline::<Local>::version(&store).await.unwrap(), version);
        assert!(members(&store, DOC).await.contains_key(&id(OWNERS)));
    });
}

#[test]
fn a_commit_the_store_was_never_told_of_is_still_answered_correctly() {
    block_on(async {
        let db = fresh_db();
        let store = Store::new(db.clone());
        Keyline::<Local>::insert(&store, root()).await.unwrap();
        let warmed = members(&store, DOC).await;

        let tx = db.begin_write().await.unwrap();
        drop(store.insert_in(&tx, grants()).await.unwrap());
        Database::commit(tx).await.unwrap();

        // The store was never told of version 2: the next query reads the rows and verifies them.
        let after = members(&store, DOC).await;
        assert_ne!(after, warmed);
        assert_eq!(after, members(&Store::new(db), DOC).await);
        assert_eq!(after, reference(&[root(), grants()]).await);
    });
}

#[test]
fn commits_reported_out_of_order_leave_the_memo_correct() {
    block_on(async {
        let db = fresh_db();
        let store = Store::new(db.clone());
        let mut staged = Vec::new();
        for batch in [root(), grants()] {
            let tx = db.begin_write().await.unwrap();
            staged.push(store.insert_in(&tx, batch).await.unwrap());
            Database::commit(tx).await.unwrap();
        }
        // The second commit is reported first, then the first.
        let first = staged.remove(0);
        store.committed(staged.remove(0));
        store.committed(first);
        let expected = reference(&[root(), grants()]).await;
        assert_eq!(members(&store, DOC).await, expected);
        assert_eq!(members(&Store::new(db), DOC).await, expected);
    });
}

#[test]
fn a_batch_adds_one_version_however_many_certificates_and_duplicates_none() {
    block_on(async {
        let db = fresh_db();
        let store = Store::new(db.clone());
        let first = vec![assume_verified(d(DOC, OWNERS, DOC, Power::Admin))];
        let v1 = Keyline::<Local>::insert(&store, first.clone())
            .await
            .unwrap();
        // One new certificate among a duplicate and a repeat.
        let mixed: Vec<_> = first
            .iter()
            .cloned()
            .chain(
                [
                    d(DOC, ALICE, DOC, Power::Edit),
                    d(DOC, ALICE, DOC, Power::Edit),
                ]
                .map(assume_verified),
            )
            .collect();
        let v2 = Keyline::<Local>::insert(&store, mixed.clone())
            .await
            .unwrap();
        assert_eq!(v2, v1.next());
        assert_eq!(Keyline::<Local>::insert(&store, mixed).await.unwrap(), v2);
        assert_eq!(
            Keyline::<Local>::insert(&store, Vec::new()).await.unwrap(),
            v2
        );
        assert_eq!((count(&db).await, stored_version(&db).await), (2, 2));
    });
}

/// More rows than one statement's bound variables allow: the batch is split into statements,
/// in one transaction, under one version.
#[test]
fn a_batch_larger_than_one_statement_is_one_transaction() {
    block_on(async {
        let db = fresh_db();
        let store = Store::new(db.clone());
        let batch: Vec<_> = (1..=60_u8)
            .flat_map(|audience| (1..=60_u8).map(move |subject| (audience, subject)))
            .map(|(audience, subject)| assume_verified(d(DOC, audience, subject, Power::Read)))
            .collect();
        assert!(batch.len() > 3_000);
        let version = Keyline::<Local>::insert(&store, batch.clone())
            .await
            .unwrap();
        assert_eq!(version, Version::INITIAL.next());
        assert_eq!(count(&db).await, 3_600);
        assert_eq!(
            Keyline::<Local>::certificates(&store)
                .await
                .unwrap()
                .value
                .len(),
            3_600
        );
    });
}

#[test]
fn a_row_modified_outside_the_backend_fails_the_query_that_reads_it() {
    block_on(async {
        let db = fresh_db();
        let store = Store::new(db.clone());
        Keyline::<Local>::insert(&store, root()).await.unwrap();
        // A signature byte flipped on disk.
        db.execute(
            "UPDATE keyline_certificates
                SET signed = CAST(substr(signed, 1, length(signed) - 1)
                          || CASE substr(signed, -1) WHEN x'00' THEN x'01' ELSE x'00' END AS BLOB)
              WHERE kind = 0 AND audience = ?",
            vec![sqlite_rs::SqlValue::Blob(id(OWNERS).to_bytes().to_vec())],
        )
        .await
        .unwrap();
        let reopened = Store::new(db);
        let failure = Keyline::<Local>::members(&reopened, id(DOC))
            .await
            .unwrap_err();
        assert!(
            matches!(
                failure,
                Error::Stored {
                    source: StoredFault::Verify(_) | StoredFault::Identity { .. },
                    ..
                }
            ),
            "{failure:?}"
        );
        // Answers that read the rows without evaluating them do not verify.
        assert_eq!(
            Keyline::<Local>::certificates(&reopened)
                .await
                .unwrap()
                .value
                .len(),
            2
        );
    });
}

/// `members(DOC)` of `MemoryKeyline` over the same batches: what a store must answer however
/// it got there.
async fn reference(
    batches: &[Vec<keyline::certificate::VerifiedCertificate<()>>],
) -> BTreeMap<Identifier, Power> {
    let memory = keyline_memory::MemoryKeyline::<()>::new();
    for batch in batches {
        Keyline::<Local>::insert(&memory, batch.clone())
            .await
            .unwrap();
    }
    Keyline::<Local>::members(&memory, id(DOC))
        .await
        .unwrap()
        .value
}
