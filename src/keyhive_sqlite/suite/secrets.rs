//! Secrets at rest: nothing secret is in the file in the clear, rows are bound to their keys, a
//! KEK rotation is incremental, and a modified row is an error.

use std::fs;
use std::sync::Arc;

use future_form::Local;
use futures::executor::block_on;
use keyhive_core::store::KeyhiveStore;
use sqlite_rs::Database;
use sqlite_rs::SqliteDb;

use super::Keks;
use super::POOL;
use super::Ref;
use super::Store;
use super::content_key;
use super::differential::Reopenable;
use super::kek;
use crate::Error;
use crate::Kek;
use crate::SecretFault;
use crate::StoredFault;

type Fault = Result<(), Error>;

async fn fill(store: &Store) {
    let pool = &*POOL;
    KeyhiveStore::<Local, Ref>::put_key_pairs(store, &pool.key_pairs)
        .await
        .unwrap();
    for (d, doc) in pool.docs.iter().enumerate() {
        for (r, content) in pool.refs.iter().enumerate() {
            KeyhiveStore::<Local, Ref>::cache_content_key(store, *doc, content, &content_key(d, r))
                .await
                .unwrap();
        }
    }
}

async fn all_read(store: &Store) -> Fault {
    KeyhiveStore::<Local, Ref>::load_key_pairs(store).await?;
    for (d, doc) in POOL.docs.iter().enumerate() {
        for (r, content) in POOL.refs.iter().enumerate() {
            let key = KeyhiveStore::<Local, Ref>::get_cached_content_key(store, *doc, content)
                .await?
                .expect("kept");
            assert_eq!(key, content_key(d, r));
        }
    }
    Ok(())
}

#[test]
fn no_secret_is_in_the_database_files_in_the_clear() {
    block_on(async {
        let hive = Reopenable::new();
        fill(&hive.store).await;
        // A row that is not secret, for the positive control: the scan below can find bytes.
        KeyhiveStore::<Local, Ref>::put_content_heads(&hive.store, POOL.docs[0], &POOL.refs)
            .await
            .unwrap();
        let dir = hive.directory();
        let mut files = Vec::new();
        for entry in fs::read_dir(dir).unwrap() {
            files.push(fs::read(entry.unwrap().path()).unwrap());
        }
        let found = |needle: &[u8]| {
            files
                .iter()
                .any(|file| file.windows(needle.len()).any(|window| window == needle))
        };
        assert!(found(&POOL.refs[0]), "the scan cannot see a plain value");
        assert!(
            found(POOL.key_pairs[0].0.as_bytes()),
            "public keys are plain"
        );
        for (_, secret) in &POOL.key_pairs {
            assert!(!found(secret.as_slice()), "a secret key is in the file");
        }
        assert!(
            !found(content_key(0, 0).as_slice()),
            "a content key is in the file"
        );
    });
}

#[test]
fn a_wrong_or_missing_kek_is_an_error_not_wrong_secrets() {
    block_on(async {
        let mut hive = Reopenable::new();
        fill(&hive.store).await;
        all_read(&hive.store).await.unwrap();

        let wrong = Store::new(hive.db.clone(), Kek::new(1, [8; 32]));
        let Err(Error::Secret { source, .. }) = all_read(&wrong).await else {
            panic!("a wrong key opens nothing");
        };
        assert!(matches!(source, SecretFault::Open), "{source}");

        let retired = Store::new(hive.db.clone(), Kek::new(2, [8; 32]));
        let Err(Error::Secret { source, .. }) = all_read(&retired).await else {
            panic!("a key of another version opens nothing");
        };
        assert!(matches!(source, SecretFault::NoKek(1)), "{source}");

        // The right key, on a reopened file.
        hive.reopen();
        all_read(&hive.store).await.unwrap();
    });
}

#[test]
fn a_kek_rotation_rewraps_deks_without_touching_secret_rows() {
    block_on(async {
        let hive = Reopenable::new();
        fill(&hive.store).await;
        async fn sealed(db: &Database) -> String {
            db.query("SELECT group_concat(hex(sealed)) FROM kh_key_pair", ())
                .await
                .unwrap()
                .one::<String>()
                .unwrap()
        }
        let before = sealed(&hive.db).await;

        let both = || Keks(vec![kek(), Kek::new(2, [9; 32])]);
        let rotated = Store::new(hive.db.clone(), both());
        all_read(&rotated).await.unwrap();
        let tx = hive.db.begin_write().await.unwrap();
        assert_eq!(rotated.session(&tx).rewrap_deks().await.unwrap(), 2);
        assert_eq!(rotated.session(&tx).rewrap_deks().await.unwrap(), 0);
        Database::commit(tx).await.unwrap();
        assert_eq!(
            sealed(&hive.db).await,
            before,
            "the secret rows are not rewritten"
        );

        // The old key can be retired.
        let only_new = Store::new(hive.db.clone(), Kek::new(2, [9; 32]));
        all_read(&only_new).await.unwrap();
        let Err(Error::Secret { .. }) = all_read(&hive.store).await else {
            panic!("the old key no longer opens anything");
        };
        // New secrets are sealed under the DEK that already exists, wrapped by the new key.
        KeyhiveStore::<Local, Ref>::cache_content_key(
            &only_new,
            POOL.docs[0],
            &[0xAB; 32],
            &content_key(9, 9),
        )
        .await
        .unwrap();
    });
}

#[test]
fn a_secret_row_moved_to_another_key_does_not_open() {
    block_on(async {
        let hive = Reopenable::new();
        fill(&hive.store).await;
        // The first key pair's secret, under the second's public key, and a content key moved to
        // another document: each was sealed against the row it was in.
        let (a, b) = (POOL.key_pairs[0].0, POOL.key_pairs[1].0);
        hive.db
            .execute(
                "UPDATE kh_key_pair SET sealed = (SELECT sealed FROM kh_key_pair WHERE public_key = ?), nonce = (SELECT nonce FROM kh_key_pair WHERE public_key = ?) WHERE public_key = ?",
                (a.as_bytes().to_vec(), a.as_bytes().to_vec(), b.as_bytes().to_vec()),
            )
            .await
            .unwrap();
        let Err(Error::Secret { source, .. }) =
            KeyhiveStore::<Local, Ref>::load_key_pairs(&hive.store)
                .await
                .map(|_| ())
        else {
            panic!("a moved secret opened");
        };
        assert!(matches!(source, SecretFault::Open), "{source}");

        let hive = Reopenable::new();
        fill(&hive.store).await;
        let moved: Ref = [0xAB; 32];
        hive.db
            .execute(
                "UPDATE kh_content_key SET content_ref = ? WHERE doc_id = ? AND content_ref = ?",
                (
                    keyhive_codec::traits::Encode::encode(&moved)
                        .as_bytes()
                        .to_vec(),
                    keyhive_crypto::identifier::Identifier::from(POOL.docs[0])
                        .to_bytes()
                        .to_vec(),
                    keyhive_codec::traits::Encode::encode(&POOL.refs[0])
                        .as_bytes()
                        .to_vec(),
                ),
            )
            .await
            .unwrap();
        let Err(Error::Secret { source, .. }) =
            KeyhiveStore::<Local, Ref>::get_cached_content_key(&hive.store, POOL.docs[0], &moved)
                .await
        else {
            panic!("a moved content key opened");
        };
        assert!(matches!(source, SecretFault::Open), "{source}");
    });
}

#[test]
fn a_modified_operation_row_is_an_error() {
    block_on(async {
        let pool = &*POOL;
        let hive = Reopenable::new();
        let ops: Vec<_> = pool
            .cgka_ops
            .iter()
            .filter(|(d, _)| *d == 0)
            .map(|(_, op)| Arc::clone(op))
            .collect();
        KeyhiveStore::<Local, Ref>::append_cgka_ops(&hive.store, pool.docs[0], &ops)
            .await
            .unwrap();
        // One operation's frame stored under another's digest: it verifies, and is not that row.
        hive.db
            .execute(
                "UPDATE kh_cgka_op SET signed = (SELECT signed FROM kh_cgka_op WHERE seq = 2) WHERE seq = 1",
                (),
            )
            .await
            .unwrap();
        let Err(Error::Stored { source, .. }) =
            KeyhiveStore::<Local, Ref>::load_cgka_ops(&hive.store, pool.docs[0]).await
        else {
            panic!("a swapped row loaded");
        };
        assert!(matches!(source, StoredFault::Identity), "{source}");

        // A frame that is not one at all.
        hive.db
            .execute("UPDATE kh_cgka_op SET signed = X'00' WHERE seq = 1", ())
            .await
            .unwrap();
        let Err(Error::Stored { source, .. }) =
            KeyhiveStore::<Local, Ref>::load_cgka_ops(&hive.store, pool.docs[0]).await
        else {
            panic!("garbage loaded");
        };
        assert!(matches!(source, StoredFault::Frame(_)), "{source}");
    });
}
