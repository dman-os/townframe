use super::*;

/// Two publishes of different bytes to one `dest`, one on the fast path and one forced onto
/// the check-and-rename fallback, racing: exactly one wins, the other is told `AlreadyExists`,
/// and `dest` holds the winner's bytes (the DEK relies on first-writer-wins).
#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn mixed_path_publishes_keep_the_first_writer() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let dest = temp_dir.path().join("dek");
    for round in 0..200 {
        if dest.exists() {
            std::fs::remove_file(&dest)?;
        }
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let racers: Vec<_> = [true, false]
            .into_iter()
            .map(|try_renameat2| {
                let tmp = temp_dir.path().join(format!("{round}.{try_renameat2}.tmp"));
                std::fs::write(&tmp, format!("{try_renameat2}"))?;
                let (dest, start) = (dest.clone(), std::sync::Arc::clone(&start));
                Ok(std::thread::spawn(move || {
                    start.wait();
                    (
                        try_renameat2,
                        publish_no_replace_blocking(&tmp, &dest, try_renameat2),
                    )
                }))
            })
            .collect::<io::Result<_>>()?;
        let outcomes: Vec<_> = racers
            .into_iter()
            .map(|racer| racer.join().expect("a publish does not panic"))
            .collect();
        let winners: Vec<_> = outcomes
            .iter()
            .filter(|(_, outcome)| outcome.is_ok())
            .map(|(try_renameat2, _)| *try_renameat2)
            .collect();
        let [winner] = winners[..] else {
            panic!("round {round}: exactly one publish wins, got {outcomes:?}");
        };
        for (_, outcome) in &outcomes {
            if let Err(error) = outcome {
                assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "round {round}");
            }
        }
        assert_eq!(
            std::fs::read_to_string(&dest)?,
            format!("{winner}"),
            "round {round}"
        );
    }
    Ok(())
}

use big_sync_core::BuckId;
use sqlite_rs::SqliteRuntime;

#[tokio::test]
async fn save_file_if_absent_writes_exact_bytes_and_is_idempotent() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    let store = SqliteBigRepoStore::new(
        SqliteRuntime::new(Default::default()).ephemeral(&[])?,
        "file-if-absent",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let storage = FsKeyhiveStorage::new(root.clone(), store)?;
    let parent_dir = root.join(LOCAL_SECRETS_SUBDIR);
    let hash = StorageHash::new([42u8; 32]);
    let data = b"secret material".to_vec();

    assert!(
        storage
            .save_file_if_absent(parent_dir.clone(), hash, data.clone())
            .await?
    );

    let written = tokio::fs::read(
        root.join(LOCAL_SECRETS_SUBDIR)
            .join(format!("{}.bin", hash.to_hex())),
    )
    .await?;
    assert_eq!(written, data);

    // Same hash with different data must be refused and must not clobber.
    assert!(
        !storage
            .save_file_if_absent(parent_dir.clone(), hash, b"clobber".to_vec())
            .await?
    );
    let unchanged = tokio::fs::read(
        root.join(LOCAL_SECRETS_SUBDIR)
            .join(format!("{}.bin", hash.to_hex())),
    )
    .await?;
    assert_eq!(unchanged, data);

    // A different hash still writes.
    let other = StorageHash::new([43u8; 32]);
    assert!(
        storage
            .save_file_if_absent(parent_dir.clone(), other, b"other".to_vec())
            .await?
    );

    Ok(())
}

async fn keyring_test_storage(label: &str) -> Res<(FsKeyhiveStorage, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    let store = SqliteBigRepoStore::new(
        SqliteRuntime::new(Default::default()).ephemeral(&[])?,
        format!("keyhive-storage-test-{label}"),
        BuckId::MAX_LEVEL,
    )
    .await?;
    let secret_store = SecretStore::boot().await.expect("mock keyring boot");
    let storage = FsKeyhiveStorage::with_secret_store(root.clone(), store, Arc::new(secret_store))?;
    Ok((storage, temp_dir))
}

#[tokio::test]
async fn keyring_mode_round_trips_local_secrets() -> Res<()> {
    let (storage, _temp_dir) = keyring_test_storage("roundtrip").await?;
    assert!(storage.uses_keyring_secrets());

    let hash = StorageHash::new([7u8; 32]);
    let data = b"keyring secret".to_vec();

    assert!(storage.save_local_secret(hash, data.clone()).await?);
    let loaded = storage.load_local_secrets().await?;
    assert_eq!(loaded, vec![(hash, data.clone())]);

    assert!(
        !storage
            .save_local_secret(hash, b"updated secret".to_vec())
            .await?
    );
    assert_eq!(
        storage.load_local_secrets().await?,
        vec![(hash, b"updated secret".to_vec())]
    );

    // A second secret survives alongside the first.
    let other = StorageHash::new([8u8; 32]);
    storage.save_local_secret(other, b"more".to_vec()).await?;
    let loaded = storage.load_local_secrets().await?;
    assert_eq!(loaded.len(), 2);

    // Deleting removes the blob and the envelope stays valid.
    storage.delete_local_secret(hash).await?;
    let loaded = storage.load_local_secrets().await?;
    assert_eq!(loaded, vec![(other, b"more".to_vec())]);

    Ok(())
}

#[tokio::test]
async fn keyring_mode_round_trips_prekey_secrets() -> Res<()> {
    let (storage, _temp_dir) = keyring_test_storage("prekey").await?;

    assert!(storage.load_prekey_secrets().await?.is_none());
    storage.save_prekey_secrets(b"prekey blob".to_vec()).await?;
    assert_eq!(
        storage.load_prekey_secrets().await?,
        Some(b"prekey blob".to_vec())
    );
    // Second write overwrites; last write wins.
    storage.save_prekey_secrets(b"updated".to_vec()).await?;
    assert_eq!(
        storage.load_prekey_secrets().await?,
        Some(b"updated".to_vec())
    );

    Ok(())
}

#[tokio::test]
async fn file_mode_round_trips_prekey_secrets_via_sqlite() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    let store = SqliteBigRepoStore::new(
        SqliteRuntime::new(Default::default()).ephemeral(&[])?,
        "prekey-file",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let storage = FsKeyhiveStorage::new(root.clone(), store)?;
    assert!(!storage.uses_keyring_secrets());

    // File mode now persists to sqlite encrypted under a file-fallback
    // DEK; nothing is written to the legacy prekey file on disk.
    storage.save_prekey_secrets(b"file blob".to_vec()).await?;
    assert_eq!(
        storage.load_prekey_secrets().await?,
        Some(b"file blob".to_vec())
    );
    let dek_path = root
        .join(DEKS_SUBDIR)
        .join(format!("{DEK_ID_PREKEY_SIDECAR}.v0.bin"));
    assert!(tokio::fs::try_exists(&dek_path).await?);

    Ok(())
}

#[tokio::test]
async fn legacy_files_are_imported_into_sqlite_on_first_boot() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    // Simulate the pre-sqlite layout.
    let secrets_dir = root.join(LOCAL_SECRETS_SUBDIR);
    tokio::fs::create_dir_all(&secrets_dir).await?;
    let hash = StorageHash::new([9u8; 32]);
    tokio::fs::write(
        secrets_dir.join(format!("{}.bin", hash.to_hex())),
        b"legacy secret",
    )
    .await?;
    tokio::fs::write(root.join(PREKEY_SECRETS_FILE), b"legacy prekey").await?;
    let reservations_dir = root.join(RESERVATIONS_SUBDIR);
    tokio::fs::create_dir_all(&reservations_dir).await?;
    let doc_id = [11u8; 32];
    let expected_reservation = DocReservation {
        magic: DOC_RESERVATION_MAGIC,
        doc_id,
        signing_key: [12u8; 32],
        initial_keys: Vec::new(),
        initial_content: None,
    };
    tokio::fs::write(
        reservations_dir.join(format!("{}.bin", StorageHash::new(doc_id).to_hex())),
        bincode::serialize(&expected_reservation)?,
    )
    .await?;

    let store = SqliteBigRepoStore::new(
        SqliteRuntime::new(Default::default()).ephemeral(&[])?,
        "legacy-import",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let storage = BigRepoKeyhiveStorage::fs(store.clone(), root.clone()).await?;

    // Local secrets round-trip through the sqlite blob store.
    let loaded = storage.load_local_secrets().await?;
    assert_eq!(loaded, vec![(hash, b"legacy secret".to_vec())]);
    assert_eq!(
        storage.load_prekey_secrets().await?,
        Some(b"legacy prekey".to_vec())
    );
    let reservations = storage.list_doc_reservations().await?;
    assert_eq!(reservations, vec![expected_reservation.clone()]);
    assert_eq!(
        storage.load_doc_reservation(doc_id).await?,
        Some(expected_reservation)
    );

    // A partial first import must resume for an id not yet in sqlite.
    let second_hash = StorageHash::new([10u8; 32]);
    tokio::fs::write(
        secrets_dir.join(format!("{}.bin", second_hash.to_hex())),
        b"second legacy secret",
    )
    .await?;
    // Idempotent: a second boot does not duplicate blobs.
    let storage2 = BigRepoKeyhiveStorage::fs(store, root).await?;
    assert_eq!(
        storage2.load_prekey_secrets().await?,
        Some(b"legacy prekey".to_vec())
    );
    let loaded2 = storage2.load_local_secrets().await?;
    assert_eq!(loaded2.len(), 2);
    assert!(loaded2.contains(&(second_hash, b"second legacy secret".to_vec())));
    assert_eq!(storage2.list_doc_reservations().await?.len(), 1);

    Ok(())
}

#[tokio::test]
async fn restart_reopens_store_and_decrypts_written_blobs() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    tokio::fs::create_dir_all(&root).await?;
    let db_path = root.join("sqlite.db");

    // First "boot": file-backed sqlite, file-fallback DEKs.
    let sql1 =
        SqliteRuntime::new(Default::default()).database(&db_path, &[], "_sqlite_rs_migrations");
    let store1 = SqliteBigRepoStore::new(sql1, "restart", BuckId::MAX_LEVEL).await?;
    let storage1 = FsKeyhiveStorage::new(root.clone(), store1)?;
    let hash = StorageHash::new([31u8; 32]);
    storage1
        .save_local_secret(hash, b"persisted secret".to_vec())
        .await?;
    storage1
        .save_prekey_secrets(b"persisted prekey".to_vec())
        .await?;
    drop(storage1);

    // Second "boot": reopen the same sqlite file with a fresh store; the
    // blobs must decrypt under the DEKs read back from the deks/ files.
    let sql2 =
        SqliteRuntime::new(Default::default()).database(&db_path, &[], "_sqlite_rs_migrations");
    let store2 = SqliteBigRepoStore::new(sql2, "restart", BuckId::MAX_LEVEL).await?;
    let storage2 = FsKeyhiveStorage::new(root.clone(), store2)?;
    assert_eq!(
        storage2.load_local_secrets().await?,
        vec![(hash, b"persisted secret".to_vec())]
    );
    assert_eq!(
        storage2.load_prekey_secrets().await?,
        Some(b"persisted prekey".to_vec())
    );

    // The key files are reused across boots; no higher-version key appears
    // out of nowhere while key rotation is deferred.
    let dek_dir = root.join(DEKS_SUBDIR);
    assert!(tokio::fs::try_exists(dek_dir.join(format!("{DEK_ID_PREKEY_SIDECAR}.v0.bin"))).await?);
    let mut entries = tokio::fs::read_dir(&dek_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        assert!(
            !name.contains(".v1."),
            "unexpected higher-version key file: {name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn envelope_row_records_metadata_not_the_dek() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    let store = SqliteBigRepoStore::new(
        SqliteRuntime::new(Default::default()).ephemeral(&[])?,
        "envelope",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let secret_store = SecretStore::boot().await.expect("mock keyring boot");
    let storage = FsKeyhiveStorage::with_secret_store(root, store.clone(), Arc::new(secret_store))?;

    storage.save_prekey_secrets(b"sidecar".to_vec()).await?;

    // The envelope row records metadata only. The raw DEK must never land
    // in sqlite: a backup would otherwise hold ciphertext and key
    // together. The DEK source (keyring entry) is the protection on
    // desktop; wrapped_dek is reserved for the future KMS-wrapped form.
    let dek = store
        .load_dek(DEK_ID_PREKEY_SIDECAR, 0)
        .await?
        .expect("envelope row");
    assert!(dek.wrapped_dek.is_empty());
    assert_eq!(dek.kek_version, 0);
    assert_eq!(dek.algorithm, DEK_ALGORITHM_KEYRING);

    Ok(())
}

#[tokio::test]
async fn reservation_round_trips_through_live_path() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    let store = SqliteBigRepoStore::new(
        SqliteRuntime::new(Default::default()).ephemeral(&[])?,
        "reservation-live",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let storage = BigRepoKeyhiveStorage::fs_with_secret_store(store, root.clone()).await?;

    let doc_id = [51u8; 32];
    let reservation = DocReservation {
        magic: DOC_RESERVATION_MAGIC,
        doc_id,
        signing_key: [52u8; 32],
        initial_keys: vec![(vec![1, 2], [54u8; 32])],
        initial_content: Some(vec![1, 2, 3]),
    };

    storage.save_doc_reservation(&reservation).await?;
    assert_eq!(
        storage.load_doc_reservation(doc_id).await?,
        Some(reservation.clone())
    );
    assert_eq!(
        storage.list_doc_reservations().await?,
        vec![reservation.clone()]
    );
    storage.delete_doc_reservation(doc_id).await?;
    assert_eq!(storage.load_doc_reservation(doc_id).await?, None);

    Ok(())
}

#[tokio::test]
async fn corrupt_records_fail_loudly() -> Res<()> {
    let temp_dir = tempfile::tempdir()?;
    let root = temp_dir.path().to_path_buf();
    tokio::fs::create_dir_all(&root).await?;
    let db_path = root.join("sqlite.db");
    let sql =
        SqliteRuntime::new(Default::default()).database(&db_path, &[], "_sqlite_rs_migrations");
    let store = SqliteBigRepoStore::new(sql, "corrupt", BuckId::MAX_LEVEL).await?;
    let storage = FsKeyhiveStorage::new(root.clone(), store.clone())?;

    // (a) A blob row with a non-12-byte nonce must fail with Corrupt.
    let bad_nonce_hash = StorageHash::new([41u8; 32]);
    store
        .save_dek(DEK_ID_LOCAL_SECRET, 0, Vec::new(), 0, "chacha20poly1305")
        .await?;
    store
        .save_secret_blob(
            SecretBlobKind::LocalSecret,
            bad_nonce_hash.as_bytes(),
            DEK_ID_LOCAL_SECRET,
            0,
            b"ct".to_vec(),
            vec![1, 2, 3, 4, 5],
        )
        .await?;
    let err = storage.load_local_secrets().await.unwrap_err();
    assert!(matches!(err, FsKeyhiveStorageError::Corrupt(_)));
    store
        .delete_secret_blob(SecretBlobKind::LocalSecret, bad_nonce_hash.as_bytes())
        .await?;

    // (b) Tampered ciphertext (authenticated encryption) must fail.
    let hash = StorageHash::new([42u8; 32]);
    storage
        .save_local_secret(hash, b"original".to_vec())
        .await?;
    let row = store
        .load_secret_blob(SecretBlobKind::LocalSecret, hash.as_bytes())
        .await?
        .expect("tampered row");
    let moved_hash = StorageHash::new([43u8; 32]);
    assert!(
        store
            .save_secret_blob(
                SecretBlobKind::LocalSecret,
                moved_hash.as_bytes(),
                &row.dek_id,
                row.dek_version,
                row.ciphertext.clone(),
                row.nonce.clone(),
            )
            .await?
    );
    let err = storage
        .read_secret_blob(SecretBlobKind::LocalSecret, moved_hash.as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, FsKeyhiveStorageError::Secrets(_)));
    store
        .delete_secret_blob(SecretBlobKind::LocalSecret, moved_hash.as_bytes())
        .await?;

    let mut ciphertext = row.ciphertext.clone();
    let last = ciphertext.len() - 1;
    ciphertext[last] ^= 0xFF;
    store
        .save_secret_blob(
            SecretBlobKind::LocalSecret,
            hash.as_bytes(),
            &row.dek_id,
            row.dek_version,
            ciphertext,
            row.nonce.clone(),
        )
        .await?;
    let err = storage.load_local_secrets().await.unwrap_err();
    assert!(matches!(err, FsKeyhiveStorageError::Secrets(_)));

    // (c) A corrupt DEK file (wrong length) must fail, not silently
    // regenerate a key that would brick every existing blob.
    let dek_path = root
        .join(DEKS_SUBDIR)
        .join(format!("{DEK_ID_PREKEY_SIDECAR}.v0.bin"));
    tokio::fs::create_dir_all(root.join(DEKS_SUBDIR)).await?;
    tokio::fs::write(&dek_path, b"1234567").await?;
    let err = storage
        .save_prekey_secrets(b"x".to_vec())
        .await
        .unwrap_err();
    assert!(matches!(err.kind(), io::ErrorKind::Other));

    Ok(())
}
