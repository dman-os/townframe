use super::*;
use crate::encrypt::Padding;
use crate::encrypt::encrypt_bytes;
use crate::encrypt::encrypt_with_rs;
use crate::encrypt::has_pair_tags;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocsMessage;
use big_cabinet_core::doc::WriteReceipt;
use futures::future::BoxFuture;
use service_client_native_rs::InlineRoute;
use service_client_native_rs::Transport;
use sqlite_rs::SqliteRuntime;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;

#[derive(Default)]
struct GcControl {
    request: Notify,
    complete: Notify,
    started: AtomicBool,
}

impl GcControl {
    fn config(self: Arc<Self>) -> iroh_blobs::store::GcConfig {
        iroh_blobs::store::GcConfig {
            interval: std::time::Duration::ZERO,
            add_protected: Some(Arc::new(move |_| {
                let control = Arc::clone(&self);
                Box::pin(async move {
                    if control.started.swap(true, Ordering::Relaxed) {
                        control.complete.notify_one();
                    }
                    control.request.notified().await;
                    iroh_blobs::store::ProtectOutcome::Continue
                })
            })),
        }
    }

    async fn run_once(&self) {
        self.request.notify_one();
        self.complete.notified().await;
    }
}

struct State {
    current: DatumsRead,
    history: BTreeMap<Vec<ChangeHash>, DatumsRead>,
    reads: Vec<ReadDatums>,
    writes: Vec<WriteDatums>,
    conflict_at: Option<usize>,
    read_error: Option<DocsError>,
    gc_on_write: Option<Arc<GcControl>>,
}

struct DocsStub {
    doc_id: DocId,
    capability: CapId,
    state: Mutex<State>,
}

impl DocsStub {
    fn new(datums: BTreeMap<DatumKey, DatumRead>) -> Arc<Self> {
        let current = DatumsRead {
            heads: vec![ChangeHash([1; 32])],
            datums,
        };
        Arc::new(Self {
            doc_id: DocId::new([42; 32]),
            capability: CapId::from_bytes([7; 16]),
            state: Mutex::new(State {
                history: [(current.heads.clone(), current.clone())].into(),
                current,
                reads: Vec::new(),
                writes: Vec::new(),
                conflict_at: None,
                read_error: None,
                gc_on_write: None,
            }),
        })
    }

    fn read(&self, request: ReadDatums) -> Result<DatumsRead, DocsError> {
        assert!(!request.optimistic);
        assert_eq!(request.capability, self.capability);
        let mut state = self.state.lock().unwrap();
        state.reads.push(request.clone());
        if let Some(error) = &state.read_error {
            return Err(error.clone());
        }
        if request.doc_id != self.doc_id {
            return Err(DocsError::Denied);
        }
        let mut read = match request.at {
            At::Current => state.current.clone(),
            At::Heads(heads) => state
                .history
                .get(&heads)
                .cloned()
                .ok_or(DocsError::UnknownHeads { heads })?,
        };
        if let Some(keys) = request.datums {
            read.datums.retain(|key, _| keys.contains(key));
        }
        Ok(read)
    }

    fn write(&self, request: WriteDatums) -> Result<WriteReceipt, DocsError> {
        assert_eq!(request.capability, self.capability);
        assert_eq!(request.doc_id, self.doc_id);
        assert!(request.remove.is_empty());
        assert_eq!(
            request.command.as_ref().unwrap().as_str().len(),
            "big-blobs-v1-".len() + 64
        );
        let mut state = self.state.lock().unwrap();
        state.writes.push(request.clone());
        let WriteMode::IfDocUnchanged { heads } = request.mode else {
            panic!("reconciliation must use CAS");
        };
        if state.conflict_at == Some(state.writes.len()) || heads != state.current.heads {
            return Err(DocsError::Conflict {
                heads: state.current.heads.clone(),
                datums: BTreeMap::new(),
            });
        }
        let heads = vec![ChangeHash(
            [u8::try_from(state.writes.len()).unwrap() + 1; 32],
        )];
        let clock = state.writes.len() as u64;
        for (key, value) in request.set {
            state.current.datums.insert(
                key,
                DatumRead::Accepted {
                    value,
                    version: DatumVersion {
                        heads: heads.clone(),
                        clock,
                    },
                },
            );
        }
        state.current.heads = heads.clone();
        let current = state.current.clone();
        state.history.insert(heads.clone(), current);
        Ok(WriteReceipt {
            doc_id: self.doc_id.clone(),
            heads,
        })
    }
}

impl InlineRoute<DocsProtocol> for DocsStub {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            match message {
                DocsMessage::ReadDatums(message) => {
                    message
                        .tx
                        .send(self.read(message.inner.params).map(Into::into))
                        .await
                        .unwrap();
                }
                DocsMessage::WriteDatums(message) => {
                    let gc = self.state.lock().unwrap().gc_on_write.clone();
                    if let Some(gc) = gc {
                        gc.run_once().await;
                    }
                    message
                        .tx
                        .send(self.write(message.inner.params).map(Into::into))
                        .await
                        .unwrap();
                }
                _ => panic!("reconciliation may only use signed document reads and writes"),
            }
        })
    }
}

struct Harness {
    docs: Arc<DocsStub>,
    store: Store,
    provider: Arc<CipherBlobProvider>,
    roots: PairRoots,
}

impl Harness {
    async fn new(plain: &[u8]) -> Res<Self> {
        Self::with_options(plain, Default::default()).await
    }

    async fn with_options(plain: &[u8], options: iroh_blobs::store::mem::Options) -> Res<Self> {
        let (memory, virtuals) = iroh_blobs::store::mem::MemStore::new_with_virtuals(options);
        let store = Store::from(memory);
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;
        let runtime = SqliteRuntime::new(Default::default());
        let roots = PairRoots::boot(runtime.ephemeral(&[])?).await?;
        Ok(Self {
            docs: DocsStub::new([(blob_key(), accepted(blob_value(plain)))].into()),
            store,
            provider,
            roots,
        })
    }

    fn reconciler(&self) -> DocumentBlobs {
        DocumentBlobs::new(
            Client::new(Transport::direct(Arc::clone(&self.docs))),
            self.docs.capability,
            self.store.clone(),
            Arc::clone(&self.provider),
            self.roots.clone(),
        )
    }

    async fn reconcile(&self) -> Res<DocumentReconcile> {
        self.reconciler()
            .reconcile(&RequestContext::root(), &self.docs.doc_id)
            .await
    }

    async fn claims(&self) -> Res<DocumentClaims> {
        self.reconciler()
            .claims(&RequestContext::root(), &self.docs.doc_id)
            .await
    }

    async fn put(&self, bytes: &[u8]) -> Res<TempTag> {
        let (tag, hash) = ensure_stored(
            &self.store,
            futures::stream::iter([Ok(Bytes::copy_from_slice(bytes))]),
        )
        .await?;
        assert_eq!(hash, Hash::new(bytes));
        Ok(tag)
    }

    async fn assert_pending_pair(&self, ciphertext: Hash) -> Res<()> {
        let pending = self.roots.unresolved().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].cipher_hash, ciphertext);
        assert_eq!(
            pending[0].claimed_by,
            Some((self.docs.doc_id.to_string(), "main".to_owned()))
        );
        Ok(())
    }

    fn current(&self) -> DatumsRead {
        self.docs.state.lock().unwrap().current.clone()
    }

    fn replace(&self, key: DatumKey, datum: DatumRead) {
        let mut state = self.docs.state.lock().unwrap();
        state.current.datums.insert(key, datum);
        let current = state.current.clone();
        state.history.insert(current.heads.clone(), current);
    }

    fn cipher(&self, key: &DatumKey) -> CipherBlob {
        let current = self.current();
        let DatumRead::Accepted { value, .. } = &current.datums[key] else {
            panic!("cipher is not accepted");
        };
        serde_json::from_value(value.clone()).unwrap()
    }
}

fn blob_key() -> DatumKey {
    DatumKey::from_text(Blob::ID, "main").unwrap()
}

fn accepted(value: Value) -> DatumRead {
    DatumRead::Accepted {
        value,
        version: DatumVersion::default(),
    }
}

fn blob_value(plain: &[u8]) -> Value {
    serde_json::json!({
        "mime": "application/octet-stream", "digest": BlobId::new(*Hash::new(plain).as_bytes()).digest_str(),
        "lengthOctets": plain.len(), "urls": ["https://example.invalid/original"],
        "futureMetadata": {"preserve": true},
    })
}

async fn existing(
    plain: &[u8],
    encoding: EncodingParams,
) -> Res<(Harness, Vec<u8>, DatumKey, MasterKey)> {
    let harness = Harness::new(plain).await?;
    let key = MasterKey::random();
    let ciphertext = encrypt_with_rs(&key, plain, encoding.record_size, encoding.padding);
    let jwk_key = DatumKey::from_text(Jwk::ID, "legacy-domain")?;
    let cipher_key = DatumKey::from_text(CipherBlob::ID, "legacy-domain/main")?;
    harness.replace(
        jwk_key.clone(),
        accepted(serde_json::to_value(JwkOct::from_master_key(&key))?),
    );
    let cipher = serde_json::json!({
        "representation": {"digest": BlobId::new(*Hash::new(&ciphertext).as_bytes()).digest_str(), "lengthOctets": ciphertext.len()},
        "contentEncoding": CONTENT_ENCODING_AES128GCM,
        "encodingParameters": encoding.to_encoding_parameters(),
        "keyRef": format!("db+facet:///self/{jwk_key}"),
        "keyRefHeads": [ChangeHash([1; 32]).to_string()],
    });
    harness.replace(cipher_key.clone(), accepted(cipher));
    let mut blob = blob_value(plain);
    blob["urls"]
        .as_array_mut()
        .unwrap()
        .push(Value::String(format!(
            "db+blob:///{}?via={cipher_key}",
            BlobId::new(*Hash::new(plain).as_bytes())
        )));
    harness.replace(blob_key(), accepted(blob));
    Ok((harness, ciphertext, cipher_key, key))
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_hashed_representation_without_via_hint_still_resolves() -> Res<()> {
    let plain = b"legacy hashed representation";
    let harness = Harness::new(plain).await?;
    let _tag = harness.put(plain).await?;
    let key = MasterKey::random();
    let encoding = EncodingParams::DEFAULT;
    let ciphertext = encrypt_with_rs(&key, plain, encoding.record_size, encoding.padding);
    let plaintext = BlobId::new(*Hash::new(plain).as_bytes());
    let (jwk_key, cipher_key) =
        legacy_representation_keys(&harness.docs.doc_id, &blob_key(), &plaintext)?;
    harness.replace(
        jwk_key.clone(),
        accepted(serde_json::to_value(JwkOct::from_master_key(&key))?),
    );
    let cipher = serde_json::json!({
        "representation": {"digest": BlobId::new(*Hash::new(&ciphertext).as_bytes()).digest_str(), "lengthOctets": ciphertext.len()},
        "contentEncoding": CONTENT_ENCODING_AES128GCM,
        "encodingParameters": encoding.to_encoding_parameters(),
        "keyRef": format!("db+facet:///self/{jwk_key}"),
        "keyRefHeads": [ChangeHash([1; 32]).to_string()],
    });
    harness.replace(cipher_key.clone(), accepted(cipher));
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty(), "{:?}", report.deferred);
    assert_eq!(report.retained.len(), 1);
    assert_eq!(report.retained[0].cipher_key, cipher_key);
    assert_eq!(
        report.retained[0].ciphertext.to_bytes32(),
        *Hash::new(&ciphertext).as_bytes()
    );
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn in_doc_key_resolves_at_cipher_heads_after_jwk_overwrite() -> Res<()> {
    let plain = b"concurrent encryptor key pin";
    let harness = Harness::new(plain).await?;
    let _tag = harness.put(plain).await?;
    let report = harness.reconcile().await?;
    assert_eq!(report.retained.len(), 1);
    let (jwk_key, _) = representation_keys(&blob_key())?;
    let other = MasterKey::random();
    harness.docs.state.lock().unwrap().current.datums.insert(
        jwk_key,
        accepted(serde_json::to_value(JwkOct::from_master_key(&other))?),
    );
    let again = harness.reconcile().await?;
    assert!(again.deferred.is_empty(), "{:?}", again.deferred);
    assert_eq!(again.retained, report.retained);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_key_and_cipher_in_one_transaction_and_reuses_the_signed_pinned_state() -> Res<()> {
    let plain = vec![0x63; 4 * 65536 + 39];
    let harness = Harness::new(&plain).await?;
    let _tag = harness.put(&plain).await?;
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    let retained = &report.retained[0];
    // Naming convention (ADR 026 §3): cipherBlob/<blob slot>, jwk/<same>.
    assert_eq!(
        retained.cipher_key,
        DatumKey::from_text(CipherBlob::ID, "main")?
    );
    let cipher = harness.cipher(&retained.cipher_key);
    let jwk_key = {
        let state = harness.docs.state.lock().unwrap();
        assert_eq!(state.writes.len(), 1);
        assert_eq!(state.writes[0].set.len(), 2);
        assert!(!state.writes[0].set.contains_key(&blob_key()));
        let jwk_key = DatumKey::from_text(Jwk::ID, "main")?;
        let jwk_value = &state.writes[0].set[&jwk_key];
        assert!(state.writes[0].set.contains_key(&retained.cipher_key));
        assert_eq!(
            cipher.key_ref,
            format!("db+facet:///{}/{jwk_key}", harness.docs.doc_id)
        );
        // Minted in the same transaction as the cipherBlob, so it cannot
        // name that write's own heads; empty means "the cipherBlob's heads".
        assert!(cipher.key_ref_heads.is_empty());
        let key = master_key(jwk_value).unwrap();
        let expected = encrypt_bytes(&key, &plain);
        assert_eq!(
            retained.ciphertext.to_bytes32(),
            *Hash::new(&expected).as_bytes()
        );
        assert_eq!(retained.length_octets, expected.len() as u64);
        assert_eq!(report.claims.heads, report.heads);
        assert_eq!(
            report.claims.representations,
            BTreeMap::from([(retained.ciphertext, retained.length_octets)])
        );
        assert!(report.claims.pins.is_empty());
        assert!(report.claims.deferred.is_empty());
        assert_eq!(report.claims.is_claimed(&retained.ciphertext), Some(true));
        // The encryptor no longer writes `Blob.urls` (ADR 026 §3).
        assert_eq!(
            state.current.datums[&blob_key()],
            accepted(blob_value(&plain))
        );
        jwk_key
    };
    harness
        .assert_pending_pair(Hash::from(retained.ciphertext.to_bytes32()))
        .await?;
    assert!(has_pair_tags(&harness.store, Hash::from(retained.ciphertext.to_bytes32())).await?);
    let again = harness.reconcile().await?;
    assert!(again.deferred.is_empty());
    assert_eq!(again.retained, report.retained);
    harness
        .assert_pending_pair(Hash::from(retained.ciphertext.to_bytes32()))
        .await?;
    let state = harness.docs.state.lock().unwrap();
    assert_eq!(state.writes.len(), 1);
    assert!(
        state
            .reads
            .iter()
            .any(|read| read.at == At::Heads(vec![ChangeHash([2; 32])])
                && read.datums.as_ref().unwrap().contains(&jwk_key))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_bytes_defer_without_key_or_cipher_writes_and_arrival_completes() -> Res<()> {
    let harness = Harness::new(b"late bytes").await?;
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::BytesUnavailable {
            ciphertext: None,
            ..
        }
    ));
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    let _tag = harness.put(b"late bytes").await?;
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stored_plaintext_survives_gc_during_the_commit_write() -> Res<()> {
    let plain = vec![0x4b; 2 * 65536 + 31];
    let gc = Arc::new(GcControl::default());
    let harness = Harness::with_options(
        &plain,
        iroh_blobs::store::mem::Options {
            gc_config: Some(Arc::clone(&gc).config()),
        },
    )
    .await?;
    let imported = harness.put(&plain).await?;
    drop(harness.put(b"unclaimed garbage").await?);
    harness.docs.state.lock().unwrap().gc_on_write = Some(Arc::clone(&gc));
    drop(imported);
    let report = harness.reconcile().await?;
    assert!(
        !harness
            .store
            .blobs()
            .has(Hash::new(b"unclaimed garbage"))
            .await?
    );
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    assert_eq!(harness.docs.state.lock().unwrap().writes.len(), 1);
    assert_eq!(
        harness
            .store
            .blobs()
            .get_bytes(Hash::new(&plain))
            .await?
            .as_ref(),
        plain
    );
    assert!(
        has_pair_tags(
            &harness.store,
            Hash::from(report.retained[0].ciphertext.to_bytes32()),
        )
        .await?
    );
    harness
        .assert_pending_pair(Hash::from(report.retained[0].ciphertext.to_bytes32()))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_outboard_without_readable_bytes_defers_before_staging_keys() -> Res<()> {
    let plain = vec![0x28; 65536 + 19];
    let harness = Harness::new(&plain).await?;
    let outboard = bao_tree::io::outboard::PreOrderMemOutboard::create(
        &plain,
        iroh_blobs::store::IROH_BLOCK_SIZE,
    );
    let hash = Hash::new(&plain);
    harness
        .store
        .blobs()
        .add_virtual_with_outboard(
            hash,
            plain.len() as u64,
            outboard.data,
            "unavailable-source",
        )
        .await?;
    assert!(harness.store.blobs().has(hash).await?);
    assert!(harness.store.sync_reader(hash).await?.is_none());
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert_eq!(report.deferred.len(), 1);
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::BytesUnavailable {
            ciphertext: None,
            ..
        }
    ));
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    assert!(harness.roots.unresolved().await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_conflict_leaves_no_claim_and_restart_mints_a_fresh_pair() -> Res<()> {
    let plain = b"crash after encryption before claim";
    let harness = Harness::new(plain).await?;
    let _tag = harness.put(plain).await?;
    harness.docs.state.lock().unwrap().conflict_at = Some(1);
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert_eq!(report.claims.is_claimed(&BlobId::new([88; 32])), None);
    assert!(
        report
            .claims
            .release_candidates(&BTreeSet::from([BlobId::new([88; 32])]))
            .is_empty()
    );
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::Document {
            error: DocsError::Conflict { .. },
            ..
        }
    ));
    // The §11 pass ran before the commit point, so its pair is rooted in
    // the PairRoots ledger with no claim naming it; the boot drain is
    // what releases such orphans (ADR 026 §10's loser path).
    assert_eq!(harness.roots.unresolved().await?.len(), 1);
    let orphan = harness.roots.unresolved().await?[0].cipher_hash;
    let restarted = harness.reconciler();
    let report = restarted
        .reconcile(&RequestContext::root(), &harness.docs.doc_id)
        .await?;
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    // The JWK was never durably staged apart from the cipherBlob, so the
    // retry mints a fresh key and therefore a different representation.
    assert_ne!(
        Hash::from(report.retained[0].ciphertext.to_bytes32()),
        orphan
    );
    {
        let state = harness.docs.state.lock().unwrap();
        assert_eq!(state.writes.len(), 2);
        assert_eq!(state.writes[0].set.len(), 2);
        assert_eq!(state.writes[1].set.len(), 2);
        assert_ne!(state.writes[0].command, state.writes[1].command);
    }
    // Both the orphan and the claimed pair stay rooted until the drain.
    let pending = harness.roots.unresolved().await?;
    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .any(|row| row.cipher_hash == Hash::from(report.retained[0].ciphertext.to_bytes32()))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filesystem_restart_restores_virtual_pairs_and_finishes_interrupted_claims() -> Res<()> {
    use iroh_blobs::store::fs::FsStore;
    use iroh_blobs::store::fs::options::Options;

    for (interrupted_claim, removed_claim) in [(false, false), (true, false), (false, true)] {
        let dir = tempfile::tempdir()?;
        let plain = vec![0x57; 3 * 65536 + 71];
        let runtime = SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[])?;
        let (filesystem, virtuals) =
            FsStore::load_with_virtuals(dir.path().join("blobs.db"), Options::new(dir.path()))
                .await?;
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;
        let harness = Harness {
            docs: DocsStub::new([(blob_key(), accepted(blob_value(&plain)))].into()),
            store: Store::from(filesystem),
            provider,
            roots: PairRoots::boot(sql.clone()).await?,
        };
        if interrupted_claim {
            harness.docs.state.lock().unwrap().conflict_at = Some(1);
        }
        let tag = harness.put(&plain).await?;
        let first = harness.reconcile().await?;
        let pending = harness.roots.unresolved().await?;
        let ciphertext = if interrupted_claim {
            assert!(first.retained.is_empty());
            assert_eq!(pending.len(), 1);
            pending[0].cipher_hash
        } else {
            assert!(first.deferred.is_empty());
            assert_eq!(first.retained.len(), 1);
            assert_eq!(pending.len(), 1);
            Hash::from(first.retained[0].ciphertext.to_bytes32())
        };
        harness.assert_pending_pair(ciphertext).await?;
        if removed_claim {
            let mut state = harness.docs.state.lock().unwrap();
            assert!(
                state
                    .current
                    .datums
                    .remove(&first.retained[0].cipher_key)
                    .is_some()
            );
            state.current.heads = vec![ChangeHash([99; 32])];
            let current = state.current.clone();
            state.history.insert(current.heads.clone(), current);
        }
        let expected = harness.store.blobs().get_bytes(ciphertext).await?;
        drop(tag);
        let Harness {
            docs,
            store,
            provider,
            roots,
        } = harness;
        drop(provider);
        drop(roots);
        store.shutdown().await?;
        drop(store);
        drop(virtuals);

        let (filesystem, virtuals) =
            FsStore::load_with_virtuals(dir.path().join("blobs.db"), Options::new(dir.path()))
                .await?;
        let provider = Arc::new(CipherBlobProvider::new());
        provider.register(&virtuals)?;
        let harness = Harness {
            docs,
            store: Store::from(filesystem),
            provider,
            roots: PairRoots::boot(sql).await?,
        };
        assert!(harness.store.blobs().get_bytes(ciphertext).await.is_err());
        harness.assert_pending_pair(ciphertext).await?;
        if removed_claim {
            let claims = harness.claims().await?;
            let id = BlobId::new(*ciphertext.as_bytes());
            assert_eq!(claims.is_claimed(&id), Some(false));
            assert_eq!(claims.release_candidates(&[id].into()), [id].into());
            assert!(has_pair_tags(&harness.store, ciphertext).await?);
            harness.assert_pending_pair(ciphertext).await?;
            drop_pair_tags(&harness.store, ciphertext).await?;
            harness.provider.remove(&ciphertext);
            assert!(!has_pair_tags(&harness.store, ciphertext).await?);
            harness.assert_pending_pair(ciphertext).await?;
            harness.roots.clear(ciphertext).await?;
            assert!(harness.roots.unresolved().await?.is_empty());
            harness.store.shutdown().await?;
            continue;
        }
        let restored = harness.reconcile().await?;
        assert!(restored.deferred.is_empty());
        assert_eq!(restored.retained.len(), 1);
        if interrupted_claim {
            // No durable JWK survived the conflicted commit, so the retry
            // mints a fresh pair; the orphan stays in PairRoots for the drain.
            let fresh = Hash::from(restored.retained[0].ciphertext.to_bytes32());
            assert_ne!(fresh, ciphertext);
            assert_eq!(
                harness.store.blobs().get_bytes(fresh).await?.len() as u64,
                restored.retained[0].length_octets
            );
            assert!(has_pair_tags(&harness.store, fresh).await?);
            assert_eq!(harness.roots.unresolved().await?.len(), 2);
            assert_eq!(harness.docs.state.lock().unwrap().writes.len(), 2);
            harness.store.shutdown().await?;
            continue;
        }
        assert_eq!(
            restored.retained[0].ciphertext.to_bytes32(),
            *ciphertext.as_bytes()
        );
        assert_eq!(harness.store.blobs().get_bytes(ciphertext).await?, expected);
        assert!(has_pair_tags(&harness.store, ciphertext).await?);
        harness.assert_pending_pair(ciphertext).await?;
        {
            let state = harness.docs.state.lock().unwrap();
            assert_eq!(state.writes.len(), 1);
        }
        harness.store.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_conflict_does_not_retry_or_process_more_stale_blobs() -> Res<()> {
    let harness = Harness::new(b"conflict").await?;
    let _tag = harness.put(b"conflict").await?;
    harness.replace(
        DatumKey::from_text(Blob::ID, "second")?,
        accepted(blob_value(b"conflict")),
    );
    harness.docs.state.lock().unwrap().conflict_at = Some(1);
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert_eq!(report.deferred.len(), 1);
    assert_eq!(harness.docs.state.lock().unwrap().writes.len(), 1);
    // The §11 pass precedes the commit point, so exactly one orphan pair
    // is rooted for the PairRoots drain; the second blob is not processed.
    assert_eq!(harness.roots.unresolved().await?.len(), 1);
    assert_eq!(harness.current().datums.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_encoding_is_preserved_without_fallback_representation_or_key_reads() -> Res<()> {
    let (harness, _, cipher_key, _) = existing(b"future encoding", EncodingParams::DEFAULT).await?;
    let _tag = harness.put(b"future encoding").await?;
    let mut cipher = serde_json::to_value(harness.cipher(&cipher_key))?;
    cipher["contentEncoding"] = "future-cipher".into();
    cipher["encodingParameters"] = serde_json::json!({"future": [1, 2, 3]});
    harness.replace(cipher_key.clone(), accepted(cipher.clone()));
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert!(
        matches!(&report.deferred[0].reason, DeferralReason::UnsupportedEncoding { content_encoding } if content_encoding == "future-cipher")
    );
    let state = harness.docs.state.lock().unwrap();
    assert!(state.writes.is_empty());
    assert_eq!(state.reads.len(), 1);
    assert!(
        matches!(&state.current.datums[&cipher_key], DatumRead::Accepted { value, .. } if value == &cipher)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_encoding_parameters_preserve_extensions_and_reconstruct_both_padding_policies()
-> Res<()> {
    let plain = b"typed framing with future metadata";
    for padding in [Padding::Padme] {
        let encoding = EncodingParams::new(4096, padding)?;
        let (harness, ciphertext, legacy_cipher_key, _) = existing(plain, encoding).await?;
        let _tag = harness.put(plain).await?;
        let mut cipher = serde_json::to_value(harness.cipher(&legacy_cipher_key))?;
        let extension = serde_json::json!({
            "preserve": true,
            "nested": {"values": [1, null, {"future": "value"}]},
        });
        cipher["encodingParameters"]["futureMetadata"] = extension.clone();
        cipher["representation"]["futureMetadata"] = extension.clone();
        cipher["futureMetadata"] = extension;
        let _plaintext_id = BlobId::new(*Hash::new(plain).as_bytes());
        let (_, cipher_key) = representation_keys(&blob_key())?;
        harness
            .docs
            .state
            .lock()
            .unwrap()
            .current
            .datums
            .remove(&legacy_cipher_key)
            .unwrap();
        harness.replace(cipher_key.clone(), accepted(cipher.clone()));
        harness.replace(blob_key(), accepted(blob_value(plain)));
        let typed = harness.cipher(&cipher_key);
        assert_eq!(typed.encoding_parameters.record_size, encoding.record_size);
        assert_eq!(
            typed.encoding_parameters.padding,
            match padding {
                Padding::Padme => "padme",
            }
        );
        assert_eq!(serde_json::to_value(typed)?, cipher);
        let report = harness.reconcile().await?;
        assert!(report.deferred.is_empty());
        assert_eq!(report.retained.len(), 1);
        let retained = &report.retained[0];
        assert_eq!(retained.cipher_key, cipher_key);
        assert_eq!(
            retained.ciphertext.to_bytes32(),
            *Hash::new(&ciphertext).as_bytes()
        );
        assert_eq!(retained.length_octets, ciphertext.len() as u64);
        assert_eq!(
            harness
                .store
                .blobs()
                .get_bytes(Hash::new(&ciphertext))
                .await?
                .as_ref(),
            ciphertext
        );
        assert_eq!(report.claims.is_claimed(&retained.ciphertext), Some(true));
        harness
            .assert_pending_pair(Hash::from(retained.ciphertext.to_bytes32()))
            .await?;
        {
            let state = harness.docs.state.lock().unwrap();
            // Reuse writes nothing: no `Blob.urls` update (ADR 026 §3).
            assert!(state.writes.is_empty());
            assert!(matches!(
                &state.current.datums[&blob_key()],
                DatumRead::Accepted { value, .. } if value == &blob_value(plain)
            ));
            assert!(matches!(
                &state.current.datums[&cipher_key],
                DatumRead::Accepted { value, .. } if value == &cipher
            ));
        }
        let again = harness.reconcile().await?;
        assert!(again.deferred.is_empty());
        assert_eq!(again.retained, report.retained);
        assert!(harness.docs.state.lock().unwrap().writes.is_empty());
        assert_eq!(serde_json::to_value(harness.cipher(&cipher_key))?, cipher);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_record_sizes_fail_before_key_reads_or_rewrites() -> Res<()> {
    for record_size in [0, 17, (1 << 20) + 1, u64::MAX] {
        let (harness, _, cipher_key, _) =
            existing(b"invalid record size", EncodingParams::DEFAULT).await?;
        let mut cipher = serde_json::to_value(harness.cipher(&cipher_key))?;
        cipher["encodingParameters"]["recordSize"] = record_size.into();
        cipher["encodingParameters"]["futureMetadata"] = serde_json::json!({"preserve": true});
        harness.replace(cipher_key.clone(), accepted(cipher.clone()));
        let error = harness.reconcile().await.unwrap_err();
        assert!(error.to_string().contains("record size"), "{error:?}");
        assert!(harness.roots.unresolved().await?.is_empty());
        let state = harness.docs.state.lock().unwrap();
        assert!(state.writes.is_empty());
        assert_eq!(state.reads.len(), 1);
        assert!(matches!(
            &state.current.datums[&cipher_key],
            DatumRead::Accepted { value, .. } if value == &cipher
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_padding_defers_without_key_reads_or_rewrites_but_keeps_the_signed_claim() -> Res<()>
{
    let plain = b"future padding";
    let (harness, ciphertext, cipher_key, _) = existing(plain, EncodingParams::DEFAULT).await?;
    let _tag = harness.put(plain).await?;
    let mut cipher = serde_json::to_value(harness.cipher(&cipher_key))?;
    cipher["encodingParameters"]["padding"] = "future-padding".into();
    harness.replace(cipher_key.clone(), accepted(cipher.clone()));
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert_eq!(report.deferred.len(), 1);
    assert!(matches!(
        &report.deferred[0].reason,
        DeferralReason::UnsupportedEncoding { content_encoding }
            if content_encoding == CONTENT_ENCODING_AES128GCM
    ));
    assert_eq!(
        report
            .claims
            .is_claimed(&BlobId::new(*Hash::new(&ciphertext).as_bytes())),
        Some(true)
    );
    assert!(harness.roots.unresolved().await?.is_empty());
    let state = harness.docs.state.lock().unwrap();
    assert!(state.writes.is_empty());
    assert_eq!(state.reads.len(), 1);
    assert!(matches!(
        &state.current.datums[&cipher_key],
        DatumRead::Accepted { value, .. } if value == &cipher
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ciphertext_first_decrypts_in_bounded_windows_and_checks_plaintext_identity() -> Res<()> {
    let plain = vec![0x81; 6 * 65536 + 115];
    let encoding = EncodingParams::new(4096, Padding::Padme)?;
    let (harness, ciphertext, cipher_key, _) = existing(&plain, encoding).await?;
    let _cipher_tag = harness.put(&ciphertext).await?;
    assert!(!harness.store.blobs().has(Hash::new(&plain)).await?);
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    assert_eq!(report.retained[0].cipher_key, cipher_key);
    let decoded = harness.store.blobs().get_bytes(Hash::new(&plain)).await?;
    assert_eq!(decoded.as_ref(), plain);
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    let current = harness.current();
    let mut wrong = blob_value(&plain);
    wrong["digest"] = BlobId::new([11; 32]).digest_str().into();
    wrong["urls"] = serde_json::json!([format!(
        "db+blob:///{}?via={cipher_key}",
        BlobId::new([11; 32])
    )]);
    harness.replace(blob_key(), accepted(wrong));
    let error = harness.reconcile().await.unwrap_err();
    assert!(error.to_string().contains("decrypted plaintext differs"));
    assert!(!harness.store.blobs().has(Hash::from([11; 32])).await?);
    assert_eq!(harness.current().heads, current.heads);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_ciphertext_is_authenticated_before_import() -> Res<()> {
    let (harness, ciphertext, _, _) = existing(b"", EncodingParams::DEFAULT).await?;
    let _tag = harness.put(&ciphertext).await?;
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    assert!(harness.store.blobs().has(Hash::new(b"")).await?);
    let (harness, mut ciphertext, cipher_key, _) = existing(b"", EncodingParams::DEFAULT).await?;
    let last = ciphertext.len() - 1;
    ciphertext[last] ^= 1;
    let _tag = harness.put(&ciphertext).await?;
    let mut cipher = serde_json::to_value(harness.cipher(&cipher_key))?;
    cipher["representation"]["digest"] = BlobId::new(*Hash::new(&ciphertext).as_bytes())
        .digest_str()
        .into();
    harness.replace(cipher_key, accepted(cipher));
    let current = harness.current();
    let outcome = harness.reconcile().await;
    assert!(outcome.is_err());
    // `Hash::EMPTY` reports complete on every store, so "the empty plaintext
    // was not imported" is not observable; adoption is: a rejected ciphertext
    // must leave no pair root row and no document write behind.
    assert!(harness.roots.unresolved().await?.is_empty());
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    assert_eq!(harness.current().heads, current.heads);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn key_reads_honor_pins_and_missing_pins_or_capabilities_defer() -> Res<()> {
    let (harness, ciphertext, cipher_key, key) =
        existing(b"pinned key", EncodingParams::DEFAULT).await?;
    let _tag = harness.put(&ciphertext).await?;
    let jwk_key = DatumKey::from_text(Jwk::ID, "legacy-domain")?;
    {
        let mut state = harness.docs.state.lock().unwrap();
        state.current.heads = vec![ChangeHash([9; 32])];
        state.current.datums.insert(
            jwk_key.clone(),
            accepted(serde_json::to_value(JwkOct::from_master_key(
                &MasterKey::random(),
            ))?),
        );
    }
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty());
    let keys = MapKeySource([(Hash::new(&ciphertext), key)].into());
    assert_eq!(
        get_decrypted(&harness.store, &keys, Hash::new(&ciphertext)).await?,
        b"pinned key"
    );
    let mut cipher = serde_json::to_value(harness.cipher(&cipher_key))?;
    cipher["keyRef"] = format!("db+facet:///{}/{jwk_key}", DocId::new([55; 32])).into();
    harness.replace(cipher_key.clone(), accepted(cipher.clone()));
    let report = harness.reconcile().await?;
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::Document {
            error: DocsError::Denied,
            ..
        }
    ));
    cipher["keyRefHeads"] = serde_json::json!([]);
    harness.replace(cipher_key, accepted(cipher));
    let report = harness.reconcile().await?;
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::UnsupportedReference
    ));
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unavailable_external_key_does_not_block_other_blobs_in_the_owner_document() -> Res<()> {
    let plain = b"independent key dependencies";
    let (harness, _, cipher_key, _) = existing(plain, EncodingParams::DEFAULT).await?;
    let tag = harness.put(plain).await?;
    let mut cipher = serde_json::to_value(harness.cipher(&cipher_key))?;
    let external = DocId::new([55; 32]);
    cipher["keyRef"] = format!("db+facet:///{external}/{}/legacy-domain", Jwk::ID).into();
    harness.replace(cipher_key.clone(), accepted(cipher.clone()));
    let independent = DatumKey::from_text(Blob::ID, "second")?;
    harness.replace(independent.clone(), accepted(blob_value(plain)));
    let report = harness.reconcile().await?;
    assert_eq!(report.deferred.len(), 1);
    assert!(matches!(
        &report.deferred[0].reason,
        DeferralReason::Document { doc_id, error: DocsError::Denied } if doc_id == &external
    ));
    assert_eq!(report.retained.len(), 1);
    assert_eq!(report.retained[0].blob_key, independent);
    let state = harness.docs.state.lock().unwrap();
    assert_eq!(state.writes.len(), 1);
    assert!(matches!(
        &state.current.datums[&cipher_key],
        DatumRead::Accepted { value, .. } if value == &cipher
    ));
    drop(state);
    drop(tag);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_unaccepted_datums_and_unavailable_documents_never_supply_values() -> Res<()> {
    let harness = Harness::new(b"unaccepted").await?;
    let _tag = harness.put(b"unaccepted").await?;
    harness.replace(
        blob_key(),
        DatumRead::Pending {
            reason: "signature pending".into(),
            version: DatumVersion::default(),
            value: Some(blob_value(b"unaccepted")),
        },
    );
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::DatumUnavailable { .. }
    ));
    assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    harness.docs.state.lock().unwrap().read_error = Some(DocsError::DocNotFound);
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert!(report.deferred[0].datum_key.is_none());
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::Document {
            error: DocsError::DocNotFound,
            ..
        }
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unaccepted_key_and_cipher_slots_never_supply_values_or_get_overwritten() -> Res<()> {
    let plain = b"unaccepted representation dependencies";
    for datum in [
        DatumRead::Pending {
            reason: "signature pending".into(),
            version: DatumVersion::default(),
            value: Some(serde_json::json!({"doNotUse": true})),
        },
        DatumRead::Rejected {
            reason: "signature rejected".into(),
        },
        DatumRead::Undecodable {
            reason: "unknown schema".into(),
            version: DatumVersion::default(),
        },
    ] {
        let harness = Harness::new(plain).await?;
        let _tag = harness.put(plain).await?;
        let (jwk_key, cipher_key) = representation_keys(&blob_key())?;
        for key in [jwk_key, cipher_key] {
            let original = harness.current();
            harness.replace(key.clone(), datum.clone());
            let report = harness.reconcile().await?;
            assert!(report.retained.is_empty());
            assert_eq!(report.deferred.len(), 1);
            assert!(matches!(
                &report.deferred[0].reason,
                DeferralReason::DatumUnavailable { doc_id, datum_key }
                    if doc_id == &harness.docs.doc_id && datum_key == &key
            ));
            assert!(harness.docs.state.lock().unwrap().writes.is_empty());
            assert!(harness.roots.unresolved().await?.is_empty());
            assert_eq!(harness.current().datums[&key], datum);
            let mut state = harness.docs.state.lock().unwrap();
            state.current = original;
            let current = state.current.clone();
            state.history.insert(current.heads.clone(), current);
        }
        let (harness, _, _, _) = existing(plain, EncodingParams::DEFAULT).await?;
        let jwk_key = DatumKey::from_text(Jwk::ID, "legacy-domain")?;
        harness.replace(jwk_key.clone(), datum.clone());
        let report = harness.reconcile().await?;
        assert!(report.retained.is_empty());
        assert_eq!(report.deferred.len(), 1);
        assert!(matches!(
            &report.deferred[0].reason,
            DeferralReason::DatumUnavailable { doc_id, datum_key }
                if doc_id == &harness.docs.doc_id && datum_key == &jwk_key
        ));
        assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn inline_plaintext_preserves_metadata_and_produces_a_rooted_representation() -> Res<()> {
    let plain = b"inline plaintext";
    let harness = Harness::new(plain).await?;
    let mut value = blob_value(plain);
    value["inline"] = serde_json::to_value(plain.as_slice())?;
    harness.replace(blob_key(), accepted(value));
    let report = harness.reconcile().await?;
    assert!(report.deferred.is_empty());
    assert_eq!(report.retained.len(), 1);
    assert!(harness.store.blobs().has(Hash::new(plain)).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn claims_include_unlinked_future_representations_and_pins_without_keys_or_bytes() -> Res<()>
{
    let harness = Harness::new(b"not locally readable").await?;
    let ciphertext = BlobId::new([13; 32]);
    let pinned = BlobId::new([14; 32]);
    let cipher_key = DatumKey::from_text(CipherBlob::ID, "unlinked/future")?;
    harness.replace(
        cipher_key,
        accepted(serde_json::json!({
            "representation": {"digest": ciphertext.digest_str(), "lengthOctets": 41},
            "contentEncoding": "future-cipher",
            "encodingParameters": {"future": true},
            "keyRef": "unreadable:key",
            "keyRefHeads": [],
        })),
    );
    harness.replace(
        DatumKey::from_text(BlobPin::ID, pinned.digest_str())?,
        accepted(serde_json::json!({"lengthOctets": 53})),
    );
    let claims = harness.claims().await?;
    assert_eq!(claims.heads, harness.current().heads);
    assert_eq!(claims.representations, BTreeMap::from([(ciphertext, 41)]));
    assert_eq!(claims.pins, BTreeMap::from([(pinned, 53)]));
    assert!(claims.deferred.is_empty());
    assert_eq!(claims.is_claimed(&ciphertext), Some(true));
    assert_eq!(claims.is_claimed(&pinned), Some(true));
    assert_eq!(claims.is_claimed(&BlobId::new([15; 32])), Some(false));
    {
        let state = harness.docs.state.lock().unwrap();
        assert_eq!(state.reads.len(), 1);
        assert_eq!(state.reads[0].at, At::Current);
        assert!(state.reads[0].datums.is_none());
        assert!(state.writes.is_empty());
    }
    let report = harness.reconcile().await?;
    assert!(report.retained.is_empty());
    assert_eq!(report.claims.representations, claims.representations);
    assert_eq!(report.claims.pins, claims.pins);
    assert!(report.claims.deferred.is_empty());
    assert!(matches!(
        report.deferred[0].reason,
        DeferralReason::BytesUnavailable { .. }
    ));
    assert!(harness.roots.unresolved().await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unaccepted_claims_never_authorize_release_or_supply_digest_length_values() -> Res<()> {
    let id = BlobId::new([16; 32]);
    let known = BlobId::new([17; 32]);
    for tag in [CipherBlob::ID, BlobPin::ID] {
        for datum in [
            DatumRead::Pending {
                reason: "signature pending".into(),
                version: DatumVersion::default(),
                value: Some(serde_json::json!({
                    "representation": {"digest": id.digest_str(), "lengthOctets": 99},
                    "lengthOctets": 99,
                })),
            },
            DatumRead::Rejected {
                reason: "signature rejected".into(),
            },
            DatumRead::Undecodable {
                reason: "schema unknown".into(),
                version: DatumVersion::default(),
            },
        ] {
            let harness = Harness::new(b"pending claims").await?;
            let key = DatumKey::from_text(tag, id.digest_str())?;
            harness.replace(key.clone(), datum);
            harness.replace(
                DatumKey::from_text(BlobPin::ID, known.digest_str())?,
                accepted(serde_json::json!({"lengthOctets": 27})),
            );
            let claims = harness.claims().await?;
            assert_eq!(claims.deferred.len(), 1);
            assert_eq!(claims.deferred[0].datum_key.as_ref(), Some(&key));
            assert_eq!(claims.is_claimed(&id), None);
            assert_eq!(claims.is_claimed(&known), Some(true));
            assert!(claims.representations.is_empty());
            assert_eq!(claims.pins, BTreeMap::from([(known, 27)]));
            assert!(
                claims
                    .release_candidates(&BTreeSet::from([id, known]))
                    .is_empty()
            );
            let report = harness.reconcile().await?;
            assert!(
                report
                    .deferred
                    .iter()
                    .any(|deferred| deferred.datum_key.as_ref() == Some(&key))
            );
            assert_eq!(report.claims.is_claimed(&id), None);
            assert!(harness.docs.state.lock().unwrap().writes.is_empty());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unavailable_claim_documents_preserve_roots_and_defer_absence() -> Res<()> {
    let plain = b"protected pair";
    let harness = Harness::new(plain).await?;
    let _tag = harness.put(plain).await?;
    let retained = harness.reconcile().await?.retained.remove(0);
    let previous = BTreeSet::from([retained.ciphertext]);
    for error in [
        DocsError::Denied,
        DocsError::DocNotFound,
        DocsError::UnknownHeads {
            heads: vec![ChangeHash([99; 32])],
        },
        DocsError::Pending {
            reason: "signature pending".into(),
        },
        DocsError::AuthorityUnavailable("temporarily unavailable".into()),
    ] {
        harness.docs.state.lock().unwrap().read_error = Some(error.clone());
        let claims = harness.claims().await?;
        assert!(claims.heads.is_empty());
        assert!(claims.representations.is_empty());
        assert!(claims.pins.is_empty());
        assert_eq!(claims.is_claimed(&retained.ciphertext), None);
        assert!(claims.release_candidates(&previous).is_empty());
        assert!(matches!(
            &claims.deferred[0].reason,
            DeferralReason::Document { doc_id, error: actual } if doc_id == &harness.docs.doc_id && actual == &error
        ));
        assert!(has_pair_tags(&harness.store, Hash::from(retained.ciphertext.to_bytes32())).await?);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_candidates_require_last_representation_and_pin_claim_to_disappear() -> Res<()> {
    let plain = b"shared representation claims";
    let harness = Harness::new(plain).await?;
    let _tag = harness.put(plain).await?;
    let retained = harness.reconcile().await?.retained.remove(0);
    let alias_key = DatumKey::from_text(CipherBlob::ID, "another-owner-in-same-document")?;
    harness.replace(
        alias_key.clone(),
        accepted(serde_json::to_value(harness.cipher(&retained.cipher_key))?),
    );
    let pin_key = DatumKey::from_text(BlobPin::ID, retained.ciphertext.digest_str())?;
    harness.replace(
        pin_key.clone(),
        accepted(serde_json::json!({"lengthOctets": retained.length_octets})),
    );
    let absent = DatumRead::Absent {
        version: DatumVersion::default(),
    };
    let previous = BTreeSet::from([retained.ciphertext]);
    harness.replace(retained.cipher_key.clone(), absent.clone());
    let claims = harness.claims().await?;
    assert_eq!(claims.representations.len(), 1);
    assert_eq!(claims.is_claimed(&retained.ciphertext), Some(true));
    assert!(claims.release_candidates(&previous).is_empty());
    harness.replace(alias_key, absent.clone());
    let claims = harness.claims().await?;
    assert!(claims.representations.is_empty());
    assert_eq!(claims.is_claimed(&retained.ciphertext), Some(true));
    assert!(claims.release_candidates(&previous).is_empty());
    harness.replace(pin_key, absent);
    let claims = harness.claims().await?;
    assert_eq!(claims.is_claimed(&retained.ciphertext), Some(false));
    assert_eq!(claims.release_candidates(&previous), previous);
    assert!(has_pair_tags(&harness.store, Hash::from(retained.ciphertext.to_bytes32())).await?);
    assert_eq!(harness.docs.state.lock().unwrap().writes.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_claim_digests_and_pin_slots_cannot_be_proven_unclaimed() -> Res<()> {
    for tag in [CipherBlob::ID, BlobPin::ID] {
        let harness = Harness::new(b"unknown digest").await?;
        let key = DatumKey::from_text(tag, "unsupported-digest")?;
        harness.replace(
            key.clone(),
            accepted(serde_json::json!({
                "representation": {"digest": "future-hash:unknown", "lengthOctets": 31},
                "lengthOctets": 31,
            })),
        );
        let claims = harness.claims().await?;
        assert_eq!(claims.deferred.len(), 1);
        assert_eq!(claims.deferred[0].datum_key.as_ref(), Some(&key));
        assert_eq!(
            claims.deferred[0].reason,
            DeferralReason::UnsupportedReference
        );
        assert_eq!(claims.is_claimed(&BlobId::new([18; 32])), None);
        assert!(
            claims
                .release_candidates(&BTreeSet::from([BlobId::new([18; 32])]))
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn inconsistent_accepted_claim_lengths_fail_instead_of_releasing() -> Res<()> {
    for other_tag in [CipherBlob::ID, BlobPin::ID] {
        let harness = Harness::new(b"length invariant").await?;
        let id = BlobId::new([19; 32]);
        harness.replace(
            DatumKey::from_text(CipherBlob::ID, "first")?,
            accepted(serde_json::json!({
                "representation": {"digest": id.digest_str(), "lengthOctets": 31},
            })),
        );
        harness.replace(
            DatumKey::from_text(other_tag, id.digest_str())?,
            accepted(serde_json::json!({
                "representation": {"digest": id.digest_str(), "lengthOctets": 32},
                "lengthOctets": 32,
            })),
        );
        let error = harness.claims().await.unwrap_err();
        assert!(error.to_string().contains("length"));
        assert!(harness.docs.state.lock().unwrap().writes.is_empty());
    }
    Ok(())
}

#[test]
fn newly_reconciled_claim_lengths_must_agree_with_previously_accepted_claims() -> Res<()> {
    let retained = RetainedRepresentation {
        blob_key: blob_key(),
        cipher_key: DatumKey::from_text(CipherBlob::ID, "reconciled")?,
        plaintext: BlobId::new([20; 32]),
        ciphertext: BlobId::new([21; 32]),
        length_octets: 31,
    };
    for claims in [
        DocumentClaims {
            pins: [(retained.ciphertext, 32)].into(),
            ..Default::default()
        },
        DocumentClaims {
            representations: [(retained.ciphertext, 32)].into(),
            ..Default::default()
        },
    ] {
        let mut report = DocumentReconcile {
            claims,
            ..Default::default()
        };
        let error = record_result(
            &mut report,
            &DocId::new([42; 32]),
            &retained.blob_key,
            Ok(retained.clone()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("length"));
        assert!(report.retained.is_empty());
    }
    Ok(())
}

#[test]
fn every_owner_document_failure_stops_stale_work_and_defers_release() -> Res<()> {
    let owner = DocId::new([42; 32]);
    let known = BlobId::new([22; 32]);
    let absent = BlobId::new([23; 32]);
    for error in [
        DocsError::Denied,
        DocsError::DocNotFound,
        DocsError::UnknownHeads {
            heads: vec![ChangeHash([99; 32])],
        },
        DocsError::Conflict {
            heads: vec![ChangeHash([99; 32])],
            datums: BTreeMap::new(),
        },
        DocsError::CommandInFlight {
            command: "in-flight".to_owned().try_into()?,
        },
        DocsError::Pending {
            reason: "signature pending".into(),
        },
        DocsError::BaseNotAccepted {
            datum_key: blob_key(),
            verdict: big_cabinet_core::doc::Unaccepted::Pending {
                reason: "base not accepted".into(),
            },
        },
        DocsError::AuthorityUnavailable("temporarily unavailable".into()),
    ] {
        let mut report = DocumentReconcile {
            claims: DocumentClaims {
                representations: [(known, 31)].into(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(!record_result(
            &mut report,
            &owner,
            &blob_key(),
            Err(DeferralReason::Document {
                doc_id: owner.clone(),
                error,
            }),
        )?);
        assert_eq!(report.claims.is_claimed(&known), Some(true));
        assert_eq!(report.claims.is_claimed(&absent), None);
        assert!(
            report
                .claims
                .release_candidates(&[absent].into())
                .is_empty()
        );
        assert_eq!(report.claims.deferred, report.deferred);
    }
    Ok(())
}

#[test]
fn legacy_references_keep_identity_and_reject_unrepresentable_branches_and_conflicting_pins()
-> Res<()> {
    let doc = DocId::new([4; 32]);
    let head = ChangeHash([8; 32]);
    for prefix in ["db+facet:/", "db+facet:///"] {
        let reference = format!("{prefix}self/{}/domain/main?branch=main&at={head}", Jwk::ID);
        let (owner, key, heads) = legacy_reference(&doc, &reference).unwrap();
        assert_eq!(owner, doc);
        assert_eq!(key, DatumKey::from_text(Jwk::ID, "domain/main")?);
        assert_eq!(heads, Some(vec![head]));
    }
    for reference in [
        format!("db+facet://attacker/{doc}/{}/key", Jwk::ID),
        format!("db+facet:///self/{}/key?branch=other", Jwk::ID),
        format!("db+facet:///self/{}/../key", Jwk::ID),
        format!("db+facet:///self/{}/key?at=bad", Jwk::ID),
    ] {
        assert!(legacy_reference(&doc, &reference).is_none());
    }
    Ok(())
}

#[test]
fn command_identity_is_stable_for_exact_requests_and_changes_with_guard_or_payload() -> Res<()> {
    let mut request = WriteDatums {
        capability: CapId::from_bytes([1; 16]),
        command: None,
        doc_id: DocId::new([2; 32]),
        mode: WriteMode::IfDocUnchanged {
            heads: vec![ChangeHash([3; 32])],
        },
        set: [(blob_key(), serde_json::json!({"urls": []}))].into(),
        remove: BTreeSet::new(),
    };
    let original = command_id(&request)?;
    assert_eq!(command_id(&request)?, original);
    request.mode = WriteMode::IfDocUnchanged {
        heads: vec![ChangeHash([4; 32])],
    };
    assert_ne!(command_id(&request)?, original);
    request.mode = WriteMode::IfDocUnchanged {
        heads: vec![ChangeHash([3; 32])],
    };
    request
        .set
        .insert(blob_key(), serde_json::json!({"urls": ["changed"]}));
    assert_ne!(command_id(&request)?, original);
    Ok(())
}
