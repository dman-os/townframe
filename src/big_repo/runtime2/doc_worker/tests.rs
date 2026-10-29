use super::*;
use crate::runtime2::CausalDecryptResult;

#[test]
fn zero_applied_ops_with_heads_is_a_materialized_snapshot() {
    let mut source = automerge::AutoCommit::new();
    source.empty_change(automerge::transaction::CommitOptions::default());
    let expected_heads = source.get_heads();
    let bytes = source.save();

    let mut loaded = automerge::Automerge::new();
    assert_eq!(loaded.load_incremental(&bytes).unwrap(), 0);
    assert_eq!(loaded.get_heads(), expected_heads);

    assert!(matches!(
        LoadedDocSnapshot::from_materialized_doc(loaded, false, Vec::new(), Vec::new(), Vec::new(),),
        LoadedDocSnapshot::Ready {
            partially_decrypted: false,
            ..
        }
    ));
}

#[test]
fn decrypted_child_is_retried_after_its_parent() {
    use automerge::transaction::Transactable;

    let mut source = automerge::AutoCommit::new();
    source.put(automerge::ROOT, "parent", 1).unwrap();
    let parent_heads = source.get_heads();
    let parent = source.save();
    source.put(automerge::ROOT, "child", 2).unwrap();
    let expected_heads = source.get_heads();
    let child = source.save_after(&parent_heads);

    let snapshot = LoadedDocSnapshot::from_decrypted_plaintexts(
        vec![
            (
                BigRepoCiphertextKind::LooseCommit,
                CommitId::new([0; 32]),
                child,
            ),
            (
                BigRepoCiphertextKind::LooseCommit,
                CommitId::new([1; 32]),
                parent,
            ),
        ],
        false,
        DocumentId::new([1; 32]),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let LoadedDocSnapshot::Ready { doc, .. } = snapshot else {
        panic!("parent and child plaintexts should materialize");
    };
    assert_eq!(doc.get_heads(), expected_heads);
}

#[test]
fn non_automerge_shadow_node_contracts_out_of_materialized_history() {
    use automerge::ReadDoc;
    use automerge::transaction::Transactable;
    use sedimentree_core::blob::Blob;
    use sedimentree_core::blob::BlobMeta;
    use sedimentree_core::depth::CountLeadingZeroBytes;
    use sedimentree_core::id::SedimentreeId;
    use sedimentree_core::loose_commit::LooseCommit;
    use sedimentree_core::sedimentree::Sedimentree;
    use sedimentree_core::sedimentree::minimized::MinimizedSedimentree;

    let mut base = automerge::Automerge::new();
    base.transact(|tx| tx.put(automerge::ROOT, "base", 1))
        .unwrap();
    let base_heads = base.get_heads();
    assert_eq!(base_heads.len(), 1);
    let base_head = base_heads[0];
    let base_bytes = base.save();

    let mut left = base
        .fork()
        .with_actor(automerge::ActorId::from(b"shadow-left".as_slice()));
    left.transact(|tx| tx.put(automerge::ROOT, "left", 1))
        .unwrap();
    let left_heads = left.get_heads();
    assert_eq!(left_heads.len(), 1);
    let left_head = left_heads[0];
    let left_bytes = left.save_after(&[base_head]);

    let mut right = base
        .fork()
        .with_actor(automerge::ActorId::from(b"shadow-right".as_slice()));
    right
        .transact(|tx| tx.put(automerge::ROOT, "right", 1))
        .unwrap();
    let right_heads = right.get_heads();
    assert_eq!(right_heads.len(), 1);
    let right_head = right_heads[0];
    let right_bytes = right.save_after(&[base_head]);

    let sed_id = SedimentreeId::new([9; 32]);
    let base_id = CommitId::new(base_head.0);
    // Keep the synthetic ID away from CountLeadingZeroBytes boundaries so
    // it cannot accidentally become a fragment boundary.
    let shadow_id = CommitId::new([0x7f; 32]);
    let left_id = CommitId::new(left_head.0);
    let right_id = CommitId::new(right_head.0);
    let meta = |tag| BlobMeta::new(&Blob::new(vec![tag]));
    let commits = vec![
        LooseCommit::new(sed_id, base_id, BTreeSet::new(), meta(1)),
        LooseCommit::new(sed_id, shadow_id, BTreeSet::from([base_id]), meta(2)),
        // Sedimentree says these depend on the shadow node, while their
        // Automerge payloads still directly depend on `base_head`.
        LooseCommit::new(sed_id, left_id, BTreeSet::from([shadow_id]), meta(3)),
        LooseCommit::new(sed_id, right_id, BTreeSet::from([shadow_id]), meta(4)),
    ];
    let mut tree = MinimizedSedimentree::new(Sedimentree::new(Vec::new(), commits));
    tree.ensure_minimized(&CountLeadingZeroBytes);
    assert_eq!(
        tree.heads(&CountLeadingZeroBytes)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([left_id, right_id])
    );

    let loose: Vec<_> = tree.loose_commits().collect();
    let order = tree.topsorted_blob_order().unwrap();
    let ordered_ids: Vec<_> = order
        .iter()
        .map(|item| match item {
            SedimentreeItem::LooseCommit(index) => loose[*index].head(),
            SedimentreeItem::Fragment(_) => unreachable!(),
        })
        .collect();
    let position = |id| {
        ordered_ids
            .iter()
            .position(|candidate| *candidate == id)
            .unwrap()
    };
    assert!(position(base_id) < position(shadow_id));
    assert!(position(shadow_id) < position(left_id));
    assert!(position(shadow_id) < position(right_id));

    let snapshot = LoadedDocSnapshot::from_decrypted_plaintexts(
        ordered_ids
            .into_iter()
            .filter_map(|id| match id {
                id if id == base_id => {
                    Some((BigRepoCiphertextKind::LooseCommit, id, base_bytes.clone()))
                }
                id if id == left_id => {
                    Some((BigRepoCiphertextKind::LooseCommit, id, left_bytes.clone()))
                }
                id if id == right_id => {
                    Some((BigRepoCiphertextKind::LooseCommit, id, right_bytes.clone()))
                }
                id if id == shadow_id => None,
                _ => unreachable!(),
            })
            .collect(),
        false,
        DocumentId::new(sed_id.as_bytes()),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let LoadedDocSnapshot::Ready { doc, .. } = snapshot else {
        panic!("filtering the shadow node must leave a materializable Automerge DAG");
    };
    assert_eq!(
        doc.get_heads().into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([left_head, right_head])
    );
    assert_eq!(
        doc.get(automerge::ROOT, "base")
            .unwrap()
            .unwrap()
            .0
            .to_i64(),
        Some(1)
    );
    assert_eq!(
        doc.get(automerge::ROOT, "left")
            .unwrap()
            .unwrap()
            .0
            .to_i64(),
        Some(1)
    );
    assert_eq!(
        doc.get(automerge::ROOT, "right")
            .unwrap()
            .unwrap()
            .0
            .to_i64(),
        Some(1)
    );
}

// ── recovered-key fast path regression ─────────────────────────────
//
// A key-only checkpoint can arrive ahead of the pre-grant content it
// covers (the sync delivers them as separate pushes). The checkpoint's
// materialization walk then caches the checkpoint application key while
// the ancestor ciphertext is still missing, leaving the doc pending. Once
// the ciphertext arrives, the recovered-key fast path must not skip the
// checkpoint's causal walk — the envelope is the only source of the
// pre-grant key, so skipping it strands the doc in PendingMaterialization
// forever (missing keys).

struct FakeRecoveryDocIo {
    sed_id: sedimentree_core::id::SedimentreeId,
    content_ref: CommitId,
    checkpoint_ref: CommitId,
    checkpoint_plaintext: Vec<u8>,
    content_plaintext: Vec<u8>,
    checkpoint_key: keyhive_crypto::symmetric_key::SymmetricKey,
    content_key: keyhive_crypto::symmetric_key::SymmetricKey,
    /// False until the content ciphertext has been delivered (phase 2).
    delivered: std::sync::atomic::AtomicBool,
}

impl FakeRecoveryDocIo {
    fn tree(&self) -> sedimentree_core::sedimentree::minimized::MinimizedSedimentree {
        use sedimentree_core::blob::Blob;
        use sedimentree_core::blob::BlobMeta;
        use sedimentree_core::loose_commit::LooseCommit;
        use sedimentree_core::sedimentree::Sedimentree;
        let mut tree = Sedimentree::new(Vec::new(), Vec::new());
        let blob_meta = BlobMeta::new(&Blob::new(Vec::new()));
        tree.add_commit(LooseCommit::new(
            self.sed_id,
            self.checkpoint_ref,
            BTreeSet::new(),
            blob_meta,
        ));
        if self.delivered.load(std::sync::atomic::Ordering::Relaxed) {
            tree.add_commit(LooseCommit::new(
                self.sed_id,
                self.content_ref,
                BTreeSet::new(),
                blob_meta,
            ));
        }
        sedimentree_core::sedimentree::minimized::MinimizedSedimentree::new(tree)
    }
}

impl crate::runtime2::DocIo<Sendable> for FakeRecoveryDocIo {
    fn hydrate_tree(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        eyre::Result<Option<sedimentree_core::sedimentree::minimized::MinimizedSedimentree>>,
    > {
        let tree = self.tree();
        Sendable::from_future(async move { Ok(Some(tree)) })
    }

    fn try_causal_decrypt(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        locator: BigRepoCiphertextLocator,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<CausalDecryptResult>> {
        let (content_ref, checkpoint_ref) = (self.content_ref, self.checkpoint_ref);
        let checkpoint_plaintext = self.checkpoint_plaintext.clone();
        let content_plaintext = self.content_plaintext.clone();
        let checkpoint_key = self.checkpoint_key;
        let content_key = self.content_key;
        let delivered = self.delivered.load(std::sync::atomic::Ordering::Relaxed);
        Sendable::from_future(async move {
            if locator.commit_id == content_ref {
                // The pre-grant content key is not derivable from this
                // node's CGKA state; it only exists in the checkpoint
                // envelope.
                return Ok(CausalDecryptResult {
                    complete: Vec::new(),
                    blockers: vec![MaterializationBlocker::MissingDocumentKeys {
                        content_refs: vec![content_ref.as_bytes().to_vec()],
                    }],
                    keys: Vec::new(),
                });
            }
            if delivered {
                Ok(CausalDecryptResult {
                    complete: vec![
                        (checkpoint_ref.as_bytes().to_vec(), checkpoint_plaintext),
                        (content_ref.as_bytes().to_vec(), content_plaintext),
                    ],
                    blockers: Vec::new(),
                    keys: vec![
                        (checkpoint_ref.as_bytes().to_vec(), checkpoint_key),
                        (content_ref.as_bytes().to_vec(), content_key),
                    ],
                })
            } else {
                Ok(CausalDecryptResult {
                    complete: vec![(checkpoint_ref.as_bytes().to_vec(), checkpoint_plaintext)],
                    blockers: vec![MaterializationBlocker::MissingCiphertexts {
                        content_refs: vec![content_ref.as_bytes().to_vec()],
                    }],
                    keys: vec![(checkpoint_ref.as_bytes().to_vec(), checkpoint_key)],
                })
            }
        })
    }

    fn decrypt_with_cached_key(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        locator: BigRepoCiphertextLocator,
        _key: keyhive_crypto::symmetric_key::SymmetricKey,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<Vec<u8>>>> {
        // The cached key opens the blob it was recovered for; a content
        // item fast-pathed with a key cached by the checkpoint's walk in
        // the same pass must get the content plaintext, not the
        // checkpoint's.
        let plaintext = if locator.commit_id == self.checkpoint_ref {
            self.checkpoint_plaintext.clone()
        } else {
            self.content_plaintext.clone()
        };
        Sendable::from_future(async move { Ok(Some(plaintext)) })
    }

    fn sedimentree_heads(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        eyre::Result<Vec<sedimentree_core::loose_commit::id::CommitId>>,
    > {
        unreachable!("not exercised by this test")
    }
    fn durable_sedimentree_heads(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        eyre::Result<Vec<sedimentree_core::loose_commit::id::CommitId>>,
    > {
        unreachable!("not exercised by this test")
    }

    fn persist_initial_document(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        _staged: crate::runtime2::support::StagedAutomergeIngest,
        _initial_keys: Vec<(Vec<u8>, [u8; 32])>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        unreachable!("not exercised by this test")
    }
    fn persist_local_commits(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        _commits: Vec<(
            sedimentree_core::loose_commit::id::CommitId,
            BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
            Vec<u8>,
        )>,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        eyre::Result<BTreeSet<subduction_core::subduction::request::FragmentRequested>>,
    > {
        unreachable!("not exercised by this test")
    }
    fn current_causal_epoch(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<[u8; 32]>>> {
        Sendable::from_future(async move { Ok(None) })
    }
    fn current_cgka_ops_count(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<usize>> {
        Sendable::from_future(async move { Ok(0) })
    }

    fn ciphertext_epoch(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        _head: sedimentree_core::loose_commit::id::CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<[u8; 32]>>> {
        unreachable!("not exercised by this test")
    }
    fn persist_causal_checkpoint(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        _covered_frontier: BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        eyre::Result<
            Option<(
                sedimentree_core::loose_commit::id::CommitId,
                crate::runtime2::support::CausalCheckpoint,
                Vec<sedimentree_core::loose_commit::id::CommitId>,
            )>,
        >,
    > {
        unreachable!("not exercised by this test")
    }
    fn has_doc_write_access(
        &self,
        _doc_id: crate::DocumentId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<bool>> {
        unreachable!("not exercised by this test")
    }
    fn has_doc_fetch_access(
        &self,
        _doc_id: crate::DocumentId,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<bool>> {
        unreachable!("not exercised by this test")
    }
    fn store_fragment(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        _head: sedimentree_core::loose_commit::id::CommitId,
        _boundary: BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
        _checkpoints: Vec<sedimentree_core::loose_commit::id::CommitId>,
        _raw_blob: Vec<u8>,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        unreachable!("not exercised by this test")
    }
    fn try_decrypt_content_keyed(
        &self,
        _sed_id: sedimentree_core::id::SedimentreeId,
        _locator: BigRepoCiphertextLocator,
    ) -> <Sendable as FutureForm>::Future<'_, eyre::Result<Option<Vec<u8>>>> {
        unreachable!("not exercised by this test")
    }
}

#[tokio::test]
async fn late_content_ciphertext_is_recovered_through_checkpoint_envelope() -> Res<()> {
    use crate::changes::ChangeListenerManager;
    use automerge::ReadDoc;
    use automerge::transaction::Transactable;
    use keyhive_crypto::symmetric_key::SymmetricKey;

    let mut source = automerge::AutoCommit::new();
    source
        .put(automerge::ROOT, "title", "seed")
        .expect("failed seeding doc");
    let content_plaintext = source.save();

    let doc_id = DocumentId::new([0x5a; 32]);
    let sed_id =
        sedimentree_core::id::SedimentreeId::new(doc_id.to_bytes32().expect(ERROR_IMPOSSIBLE));
    let content_ref = CommitId::new([0x11; 32]);
    let checkpoint = CausalCheckpoint::new([0x77; 32], BTreeSet::from([content_ref]));
    let checkpoint_ref = causal_checkpoint_id(&checkpoint);
    let checkpoint_plaintext = checkpoint.encode()?;

    let io = Arc::new(FakeRecoveryDocIo {
        sed_id,
        content_ref,
        checkpoint_ref,
        checkpoint_plaintext,
        content_plaintext,
        checkpoint_key: SymmetricKey::from([0x42; 32]),
        content_key: SymmetricKey::from([0x43; 32]),
        delivered: std::sync::atomic::AtomicBool::new(false),
    });

    let (evt_tx, _evt_rx) = async_channel::unbounded::<Runtime2Evt>();
    let (runtime_cmd_tx, _runtime_cmd_rx) =
        async_channel::unbounded::<crate::runtime2::Runtime2Cmd>();
    let (change_manager, _stop) = ChangeListenerManager::boot();

    let mut worker = DocWorker2 {
        doc_id,
        sed_id,
        generation: 0,
        state: DocState::Unloaded,
        interests: crate::interest::SyncInterests::new(Arc::new(
            subduction_ephemeral::clock::std_clock::StdClock,
        )),
        usable: tokio::sync::watch::channel(false).0,
        caller_handles: Vec::new(),
        partially_decrypted: false,
        causal_epoch: None,
        cgka_ops_count: 0,
        recovered_keys: std::sync::Mutex::new(HashMap::new()),
        blocked_refs: HashSet::new(),
        causal_checkpoints: HashMap::new(),
        io: std::sync::Arc::<FakeRecoveryDocIo>::clone(&io),
        change_manager,
        runtime_cmd_tx,
        evt_tx,
        pending_fragment_requests: BTreeSet::new(),
        quiescence_waiters: Vec::new(),
    };

    // Phase 1: the checkpoint is applied but the covered content
    // ciphertext has not arrived. The walk caches the checkpoint key and
    // the doc goes pending.
    let status = worker
        .retry_materialization(BigRepoChangeOrigin::Local)
        .await?;
    assert!(matches!(status, MaterializationStatus::Pending(_)));
    assert!(matches!(worker.state, DocState::PendingMaterialization));
    assert!(
        worker
            .recovered_keys
            .lock()
            .expect("recovered-key cache lock poisoned")
            .contains_key(&checkpoint_ref),
        "the partial walk must cache the checkpoint's recovered key"
    );

    // Phase 2: the content ciphertext arrives. The retry must re-run the
    // checkpoint's causal walk (not the cached-key fast path) so the
    // envelope can recover the pre-grant content.
    io.delivered
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let status = worker
        .retry_materialization(BigRepoChangeOrigin::Local)
        .await?;
    assert!(
        matches!(
            status,
            MaterializationStatus::Ready {
                partially_decrypted: false
            }
        ),
        "late-arriving content must materialize through the checkpoint envelope: {status:?}"
    );
    assert!(worker.blocked_refs.is_empty());
    let DocState::Transient(doc) = &worker.state else {
        panic!("doc must be materialized after recovery");
    };
    assert_eq!(
        doc.get(automerge::ROOT, "title")
            .expect("title read")
            .expect("title present")
            .0
            .to_str()
            .expect("title is text"),
        "seed"
    );
    Ok(())
}

/// A fake hub that acks `RegisterDocLease` so the worker's lease
/// registration resolves without a real hub. The worker awaits the ack inline
/// in `register_bundle_lease`, so its receiver is always alive here.
fn spawn_fake_hub_ack(
    runtime_cmd_rx: async_channel::Receiver<crate::runtime2::Runtime2Cmd>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(cmd) = runtime_cmd_rx.recv().await {
            if let crate::runtime2::Runtime2Cmd::RegisterDocLease { registered, .. } = cmd {
                registered.send(()).expect(ERROR_CHANNEL);
            }
        }
    })
}

/// A worker over a fully-delivered recovery doc (materializes `Ready` on
/// the first acquisition) with a fake hub acking lease registrations.
fn ready_recovery_worker() -> (DocWorker2<Sendable>, tokio::task::JoinHandle<()>) {
    use crate::changes::ChangeListenerManager;
    use automerge::transaction::Transactable;
    use keyhive_crypto::symmetric_key::SymmetricKey;

    let mut source = automerge::AutoCommit::new();
    source
        .put(automerge::ROOT, "title", "seed")
        .expect("failed seeding doc");
    let content_plaintext = source.save();

    let doc_id = DocumentId::new([0x5b; 32]);
    let sed_id =
        sedimentree_core::id::SedimentreeId::new(doc_id.to_bytes32().expect(ERROR_IMPOSSIBLE));
    let content_ref = CommitId::new([0x21; 32]);
    let checkpoint = CausalCheckpoint::new([0x87; 32], BTreeSet::from([content_ref]));
    let checkpoint_ref = causal_checkpoint_id(&checkpoint);
    let checkpoint_plaintext = checkpoint.encode().expect("checkpoint encodes");

    let io = Arc::new(FakeRecoveryDocIo {
        sed_id,
        content_ref,
        checkpoint_ref,
        checkpoint_plaintext,
        content_plaintext,
        checkpoint_key: SymmetricKey::from([0x52; 32]),
        content_key: SymmetricKey::from([0x53; 32]),
        delivered: std::sync::atomic::AtomicBool::new(true),
    });

    let (runtime_cmd_tx, runtime_cmd_rx) =
        async_channel::unbounded::<crate::runtime2::Runtime2Cmd>();
    let hub_ack = spawn_fake_hub_ack(runtime_cmd_rx);
    let (evt_tx, _evt_rx) = async_channel::unbounded::<Runtime2Evt>();
    let (change_manager, _stop) = ChangeListenerManager::boot();

    let worker = DocWorker2 {
        doc_id,
        sed_id,
        generation: 0,
        state: DocState::Unloaded,
        interests: crate::interest::SyncInterests::new(Arc::new(
            subduction_ephemeral::clock::std_clock::StdClock,
        )),
        usable: tokio::sync::watch::channel(false).0,
        caller_handles: Vec::new(),
        partially_decrypted: false,
        causal_epoch: None,
        cgka_ops_count: 0,
        recovered_keys: std::sync::Mutex::new(HashMap::new()),
        blocked_refs: HashSet::new(),
        causal_checkpoints: HashMap::new(),
        io: std::sync::Arc::<FakeRecoveryDocIo>::clone(&io),
        change_manager,
        runtime_cmd_tx,
        evt_tx,
        pending_fragment_requests: BTreeSet::new(),
        quiescence_waiters: Vec::new(),
    };
    (worker, hub_ack)
}

async fn acquire_ready(
    worker: &mut DocWorker2<Sendable>,
) -> eyre::Result<crate::runtime2::types::LiveDocHandle> {
    let (resp, rx) = futures::channel::oneshot::channel();
    worker
        .acquire_handle(resp, crate::runtime2::DocLeaseKind::Caller)
        .await?;
    rx.await
        .map_err(|_| ferr!(ERROR_CHANNEL))??
        .into_ready(worker.doc_id.clone())
        .map_err(|err| ferr!("{err:?}"))
}

#[tokio::test]
async fn sync_interest_does_not_create_a_handle_and_internal_handles_do_not_demand() -> Res<()> {
    let (mut worker, hub_ack) = ready_recovery_worker();
    let demand = worker.interests.subscribe();
    let (resp, rx) = futures::channel::oneshot::channel();
    worker
        .acquire_handle(resp, crate::runtime2::DocLeaseKind::Internal)
        .await?;
    let internal = rx.await??.into_ready(worker.doc_id.clone())?;
    assert!(demand.documents().is_empty());
    drop(internal);
    worker.state = DocState::Unloaded;
    let (resp, rx) = futures::channel::oneshot::channel();
    worker.acquire_interest(resp).await?;
    let interest = rx.await??;
    assert!(*interest.usable.borrow());
    assert!(matches!(worker.state, DocState::Transient(_)));
    assert_eq!(*demand.documents(), HashSet::from([worker.doc_id.clone()]));
    let handle = acquire_ready(&mut worker).await?;
    let clone = handle.clone();
    drop(interest);
    drop(handle);
    assert!(worker.has_caller_handle());
    drop(clone);
    assert!(!worker.has_caller_handle());
    assert_eq!(*demand.documents(), HashSet::from([worker.doc_id.clone()]));
    hub_ack.abort();
    Ok(())
}

#[tokio::test]
async fn sync_interest_materializes_persisted_content_before_reporting_ready() -> Res<()> {
    use automerge::ReadDoc;

    for state in [
        DocState::Unloaded,
        DocState::Transient(Box::new(automerge::Automerge::new())),
    ] {
        let (mut worker, hub_ack) = ready_recovery_worker();
        let (resp, rx) = futures::channel::oneshot::channel();
        worker.acquire_interest(resp).await?;
        let interest = rx.await??;
        worker.state = state;
        worker.publish_usable();
        let (reply, receipt) = futures::channel::oneshot::channel();
        let has_caller = worker.has_caller_handle();
        worker
            .report_sync_outcome(PeerKey::random(), has_caller, Some(reply))
            .await?;
        worker.publish_usable();
        assert!(matches!(
            receipt.await??.outcome,
            crate::runtime2::types::SyncDocOutcome::Ready
        ));
        assert!(*interest.usable.borrow());
        let handle = acquire_ready(&mut worker).await?;
        surelock::key::lock_scope(|key| {
            let (doc, _key) = key.lock(&handle.bundle.doc);
            assert_eq!(
                doc.get(automerge::ROOT, "title")?
                    .and_then(|(value, _)| value.to_str().map(str::to_owned)),
                Some("seed".to_owned())
            );
            eyre::Ok(())
        })?;
        drop(handle);
        drop(interest);
        hub_ack.abort();
    }
    Ok(())
}

#[tokio::test]
async fn live_bundle_is_reused_after_caller_drop() -> Res<()> {
    let (mut worker, hub_ack) = ready_recovery_worker();

    // First acquisition materializes and hands out a live handle.
    let first = acquire_ready(&mut worker).await?;
    let first_bundle_id = first.bundle.id();
    drop(first);

    // The worker retains the bundle strongly: a second acquisition after
    // the caller dropped must return the SAME bundle instance, not
    // re-materialize (the pre-fix weak-ref design re-walked every time).
    let second = acquire_ready(&mut worker).await?;
    assert_eq!(
        second.bundle.id(),
        first_bundle_id,
        "re-acquisition after caller drop must reuse the retained bundle"
    );
    assert!(matches!(worker.state, DocState::Live(_)));
    drop(second);
    hub_ack.abort();
    Ok(())
}

#[tokio::test]
async fn broken_bundle_is_reloaded_on_reacquire() -> Res<()> {
    let (mut worker, hub_ack) = ready_recovery_worker();

    let first = acquire_ready(&mut worker).await?;
    let first_bundle_id = first.bundle.id();
    // Exactly what the write-access gate does when it refuses a commit.
    first
        .bundle
        .mark_broken(super::BrokenReason::CommitRejectedNoWriteAccess);
    drop(first);

    // A broken bundle must be reloaded into a fresh instance on the next
    // acquisition, so a rejected commit cannot poison later handles.
    let second = acquire_ready(&mut worker).await?;
    assert_ne!(
        second.bundle.id(),
        first_bundle_id,
        "re-acquisition after a broken bundle must reload a fresh bundle"
    );
    assert!(!second.bundle.is_broken());
    drop(second);
    hub_ack.abort();
    Ok(())
}

/// Teardown is the only thing that can invalidate a bundle with no commit
/// behind it, and it is reachable while a caller still holds a handle. The
/// handle keeps the bundle alive after the worker is gone, so it observes the
/// recorded reason rather than a bare `true`.
#[tokio::test]
async fn dropping_a_live_doc_worker_marks_its_bundle_broken() -> Res<()> {
    let (mut worker, hub_ack) = ready_recovery_worker();

    let handle = acquire_ready(&mut worker).await?;
    let bundle = std::sync::Arc::clone(&handle.bundle);
    assert!(!bundle.is_broken());
    assert_eq!(bundle.broken_reason(), None);

    drop(worker);

    assert!(bundle.is_broken());
    assert_eq!(
        bundle.broken_reason(),
        Some(super::BrokenReason::WorkerDropped),
        "teardown must be recorded as teardown, not as a rejected commit"
    );
    // The caller's `Arc` keeps the bundle readable after its worker is gone;
    // the recorded reason is the only thing the dead worker could still tell
    // it.
    assert_eq!(bundle.doc_id, DocumentId::new([0x5b; 32]));
    drop(handle);
    hub_ack.abort();
    Ok(())
}

/// Every other writer of the broken flag is inside `commit_delta`, so
/// nothing that happens before a commit may latch it. This pins the set of
/// writers: the only route to a broken bundle with no commit is teardown. A
/// fourth "be safe, mark broken on the way out" site is how a refusal became
/// unattributable in the first place.
#[tokio::test]
async fn no_commit_attempted_means_only_drop_can_break_a_bundle() -> Res<()> {
    let (mut worker, hub_ack) = ready_recovery_worker();

    // Acquisition materializes the document (a decrypt walk plus a bundle
    // build) and must leave the flag alone.
    let handle = acquire_ready(&mut worker).await?;
    let bundle = std::sync::Arc::clone(&handle.bundle);
    assert!(!bundle.is_broken());

    // A second acquisition after the caller drops reuses the same bundle;
    // still untouched.
    drop(handle);
    let reused = acquire_ready(&mut worker).await?;
    assert_eq!(reused.bundle.id(), bundle.id());
    assert!(!bundle.is_broken());
    assert_eq!(bundle.broken_reason(), None);
    drop(reused);

    // Only teardown breaks it, and it records why.
    drop(worker);
    assert_eq!(
        bundle.broken_reason(),
        Some(super::BrokenReason::WorkerDropped)
    );
    hub_ack.abort();
    Ok(())
}
