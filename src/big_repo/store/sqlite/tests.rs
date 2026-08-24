use super::*;

use big_sync::{HostPartStoreContractHarness, host_part_store_contract};
use sedimentree_core::blob::BlobMeta;
use subduction_crypto::signer::memory::MemorySigner;

struct SqliteBigRepoHarness {
    store: SqliteBigRepoStore,
}

impl HostPartStoreContractHarness for SqliteBigRepoHarness {
    fn store(&self) -> &dyn HostPartStore {
        &self.store
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_big_repo_host_part_store_contract() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-host-contract", BuckId::MAX_LEVEL).await?;
    host_part_store_contract::assert_host_part_store_contract(&SqliteBigRepoHarness { store }).await
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_big_repo_local_subscription_bypasses_remote_policy_and_hidden_parts() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let part = PartId(Byte32Id::new([221; 32]));
    let obj = ObjId(Byte32Id::new([222; 32]));
    let store = SqliteBigRepoStore::new_with_config(
        sql,
        "big-repo-sqlite-local-subscription",
        BuckId::MAX_LEVEL,
        big_sync::HostPartStoreConfig {
            hidden_parts: HashSet::from([part]),
            ..Default::default()
        },
    )
    .await?;
    HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 1})).await?;
    HostPartStore::ensure_part(&store, part).await?;
    HostPartStore::add_obj_to_parts(&store, obj, vec![part]).await?;

    let rx = HostPartStore::subscribe_local(
        &store,
        SubPartsRequest {
            targets: HashSet::from([SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            }]),
        },
    )
    .await??;
    let mut saw_added = false;
    loop {
        match rx.recv().await? {
            SubEvent::Added(event) if event.obj_id == obj && event.part_id == part => {
                saw_added = true;
            }
            SubEvent::ReplayComplete => break,
            _ => {}
        }
    }
    assert!(saw_added);

    HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 2})).await?;
    assert!(matches!(rx.recv().await?, SubEvent::Changed(event) if event.obj_id == obj));
    Ok(())
}

#[tokio::test]
async fn remote_subscription_delivers_removal_after_policy_revocation() -> Res<()> {
    let store = SqliteBigRepoStore::new(
        SqlCtx::memory().await?,
        "big-repo-sqlite-policy-removal",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let part = PartId(Byte32Id::new([224; 32]));
    let obj = ObjId(Byte32Id::new([225; 32]));
    let peer = PeerId(Byte32Id::new([226; 32]));
    HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 1})).await?;
    store.ensure_part(part).await?;
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::from([part]),
                desired_global: false,
            }],
            1,
            true,
        )
        .await?;

    let subscription_request = |cursor| SubPartsRequest {
        targets: HashSet::from([SubscriptionTarget::Part {
            part_id: part,
            cursor,
        }]),
    };
    let rx = HostPartStore::subscribe(&store, subscription_request(0), peer).await??;
    let added_cursor = match rx.recv().await? {
        SubEvent::Added(added) => {
            assert_eq!(added.obj_id, obj);
            assert_eq!(added.part_id, part);
            added.cursor
        }
        other => panic!("expected initial visible addition, got {other:?}"),
    };
    assert!(matches!(rx.recv().await?, SubEvent::ReplayComplete));

    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::new(),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::new(),
                desired_global: false,
            }],
            2,
            true,
        )
        .await?;
    assert!(matches!(
        rx.recv().await?,
        SubEvent::Removed(removed) if removed.obj_id == obj && removed.part_id == part
    ));

    let replay =
        HostPartStore::subscribe(&store, subscription_request(added_cursor), peer).await??;
    assert!(matches!(
        replay.recv().await?,
        SubEvent::Removed(removed) if removed.obj_id == obj && removed.part_id == part
    ));
    assert!(matches!(replay.recv().await?, SubEvent::ReplayComplete));
    Ok(())
}

#[tokio::test]
async fn grant_resurrects_denied_added_on_live_subscription() -> Res<()> {
    let store = SqliteBigRepoStore::new(
        SqlCtx::memory().await?,
        "big-repo-sqlite-grant-resurrect",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let part = PartId(Byte32Id::new([227; 32]));
    let obj = ObjId(Byte32Id::new([228; 32]));
    let peer = PeerId(Byte32Id::new([229; 32]));
    let other = PeerId(Byte32Id::new([230; 32]));
    HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 1})).await?;
    store.ensure_part(part).await?;
    // Make the doc live in the part without granting `peer`: its Added
    // event must be denied for `peer` at delivery time.
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::from([(other, keyhive_core::access::Access::Read)]),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::from([part]),
                desired_global: false,
            }],
            1,
            true,
        )
        .await?;

    let rx = HostPartStore::subscribe(
        &store,
        SubPartsRequest {
            targets: HashSet::from([SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            }]),
        },
        peer,
    )
    .await??;
    // Replay must deliver nothing but the marker: the Added is denied
    // while `peer` has no syncable row.
    assert!(matches!(rx.recv().await?, SubEvent::ReplayComplete));

    // Granting the row must resurrect visibility on the existing
    // subscription via a fresh Changed event.
    HostPartStore::add_obj_member(&store, obj, peer, keyhive_core::access::Access::Read).await?;
    assert!(matches!(
        rx.recv().await?,
        SubEvent::Changed(changed) if changed.obj_id == obj
    ));

    // A fresh replay from cursor 0 now delivers the previously buried
    // Added, since delivery-time permission passes.
    let replay = HostPartStore::subscribe(
        &store,
        SubPartsRequest {
            targets: HashSet::from([SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            }]),
        },
        peer,
    )
    .await??;
    assert!(matches!(
        replay.recv().await?,
        SubEvent::Added(added) if added.obj_id == obj && added.part_id == part
    ));
    // The grant's resurrection Changed is part of the log as well.
    assert!(matches!(
        replay.recv().await?,
        SubEvent::Changed(changed) if changed.obj_id == obj
    ));
    assert!(matches!(replay.recv().await?, SubEvent::ReplayComplete));
    Ok(())
}

#[tokio::test]
async fn reconcile_grant_reemits_event_for_already_live_doc() -> Res<()> {
    let store = SqliteBigRepoStore::new(
        SqlCtx::memory().await?,
        "big-repo-sqlite-reconcile-grant",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let part = PartId(Byte32Id::new([231; 32]));
    let obj = ObjId(Byte32Id::new([232; 32]));
    let peer = PeerId(Byte32Id::new([233; 32]));
    HostPartStore::set_obj_payload(&store, obj, serde_json::json!({"value": 1})).await?;
    store.ensure_part(part).await?;
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::new(),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::from([part]),
                desired_global: false,
            }],
            1,
            true,
        )
        .await?;

    let rx = HostPartStore::subscribe(
        &store,
        SubPartsRequest {
            targets: HashSet::from([SubscriptionTarget::Part {
                part_id: part,
                cursor: 0,
            }]),
        },
        peer,
    )
    .await??;
    assert!(matches!(rx.recv().await?, SubEvent::ReplayComplete));

    // Granting through the group-part reconciliation path (absent →
    // present principal) re-emits an event for the already-live doc so a
    // subscriber whose earlier Added was denied learns it exists.
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::from([part]),
                desired_global: false,
            }],
            2,
            true,
        )
        .await?;
    assert!(matches!(
        rx.recv().await?,
        SubEvent::Added(added) if added.obj_id == obj && added.part_id == part
    ));

    // Reconciling again with unchanged agents grants nobody and must not
    // emit anything further.
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::from([part]),
                desired_global: false,
            }],
            3,
            true,
        )
        .await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn keyhive_membership_is_not_advertised_until_payload_is_available() -> Res<()> {
    let store = SqliteBigRepoStore::new(
        SqlCtx::memory().await?,
        "big-repo-sqlite-latent-membership",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let obj = ObjId(Byte32Id::new([223; 32]));
    HostPartStore::ensure_part(&store, crate::GLOBAL_PART_ID).await?;
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc: obj,
                agents: HashMap::new(),
                managed_group_parts: HashSet::new(),
                desired_group_parts: HashSet::new(),
                desired_global: true,
            }],
            1,
            true,
        )
        .await?;

    assert_eq!(
        HostPartStore::obj_parts(&store, obj).await?,
        vec![crate::GLOBAL_PART_ID]
    );
    assert_eq!(
        HostPartStore::member_count(&store, crate::GLOBAL_PART_ID).await?,
        0
    );
    assert!(
        HostPartStore::list_events(&store, HashSet::from([crate::GLOBAL_PART_ID]), 0, 8,)
            .await??
            .get(&crate::GLOBAL_PART_ID)
            .expect(ERROR_IMPOSSIBLE)
            .events
            .is_empty(),
    );

    let rx = HostPartStore::subscribe_local(
        &store,
        SubPartsRequest {
            targets: HashSet::from([SubscriptionTarget::Part {
                part_id: crate::GLOBAL_PART_ID,
                cursor: 0,
            }]),
        },
    )
    .await??;
    assert!(matches!(rx.recv().await?, SubEvent::ReplayComplete));

    let payload = serde_json::json!({"heads": ["available"]});
    HostPartStore::set_obj_payload(&store, obj, payload.clone()).await?;
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await??;
    let SubEvent::Added(added) = event else {
        panic!("payload promotion must first advertise Added, got {event:?}");
    };
    assert_eq!(added.obj_id, obj);
    assert_eq!(added.part_id, crate::GLOBAL_PART_ID);
    assert_eq!(added.payload, payload);
    assert_eq!(
        HostPartStore::member_count(&store, crate::GLOBAL_PART_ID).await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn latent_membership_resurrects_removed_member_when_payload_returns() -> Res<()> {
    let store = SqliteBigRepoStore::new(
        SqlCtx::memory().await?,
        "big-repo-sqlite-latent-regrant",
        BuckId::MAX_LEVEL,
    )
    .await?;
    let part = PartId(Byte32Id::new([224; 32]));
    let obj = ObjId(Byte32Id::new([225; 32]));
    HostPartStore::ensure_part(&store, part).await?;

    HostPartStore::add_obj_to_parts(&store, obj, vec![part]).await?;
    HostPartStore::set_obj_payload(&store, obj, serde_json::json!("first")).await?;
    HostPartStore::remove_obj_from_part(&store, obj, part).await?;

    HostPartStore::add_obj_to_parts(&store, obj, vec![part]).await?;
    HostPartStore::set_obj_payload(&store, obj, serde_json::json!("second")).await?;

    assert_eq!(HostPartStore::obj_parts(&store, obj).await?, vec![part]);
    assert_eq!(
        HostPartStore::obj_payload(&store, obj).await?,
        Some(serde_json::json!("second"))
    );
    Ok(())
}

async fn make_commit(
    signer: &MemorySigner,
    tree: SedimentreeId,
    head_byte: u8,
) -> VerifiedMeta<LooseCommit> {
    let blob = Blob::new(vec![head_byte; 3]);
    let mut head = [0; 32];
    head[0] = head_byte;
    let payload = LooseCommit::new(
        tree,
        CommitId::new(head),
        std::collections::BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    let signed = Signed::seal::<Sendable, _>(signer, payload).await;
    VerifiedMeta::new(
        signed.into_signed().try_verify().expect("fresh signature"),
        blob,
    )
    .expect("fresh blob metadata")
}

async fn make_commit_with_parents(
    signer: &MemorySigner,
    tree: SedimentreeId,
    head_byte: u8,
    parents: std::collections::BTreeSet<CommitId>,
) -> VerifiedMeta<LooseCommit> {
    let blob = Blob::new(vec![head_byte; 3]);
    let mut head = [0; 32];
    head[0] = head_byte;
    let payload = LooseCommit::new(tree, CommitId::new(head), parents, BlobMeta::new(&blob));
    let signed = Signed::seal::<Sendable, _>(signer, payload).await;
    VerifiedMeta::new(
        signed.into_signed().try_verify().expect("fresh signature"),
        blob,
    )
    .expect("fresh blob metadata")
}

async fn make_fragment(
    signer: &MemorySigner,
    tree: SedimentreeId,
    head_byte: u8,
    boundary: std::collections::BTreeSet<CommitId>,
) -> VerifiedMeta<Fragment> {
    let blob = Blob::new(vec![head_byte; 3]);
    let mut head = [0; 32];
    head[0] = head_byte;
    let payload = Fragment::new(
        tree,
        CommitId::new(head),
        boundary,
        &[],
        BlobMeta::new(&blob),
    );
    let signed = Signed::seal::<Sendable, _>(signer, payload).await;
    VerifiedMeta::new(
        signed.into_signed().try_verify().expect("fresh signature"),
        blob,
    )
    .expect("fresh blob metadata")
}

fn commit_id(head_byte: u8) -> CommitId {
    let mut head = [0; 32];
    head[0] = head_byte;
    CommitId::new(head)
}

/// The persisted BigSync payload heads for a tree, sorted.
async fn payload_heads(store: &SqliteBigRepoStore, tree: SedimentreeId) -> Res<Vec<CommitId>> {
    let obj_id = SqliteBigRepoStore::obj_id(tree);
    let Some(payload) = HostPartStore::obj_payload(store, obj_id).await? else {
        return Ok(Vec::new());
    };
    let mut heads = Vec::new();
    if let Some(list) = payload.get("heads").and_then(|h| h.as_array()) {
        for head in list {
            let s = head
                .as_str()
                .ok_or_else(|| eyre::eyre!("non-string head in payload"))?;
            let bytes = utils_rs::hash::decode_base58_multibase(s)?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| eyre::eyre!("head not 32 bytes"))?;
            heads.push(CommitId::new(arr));
        }
    }
    heads.sort_unstable();
    Ok(heads)
}

/// Heads of a fresh tree hydrated from the raw durable rows — the
/// reference implementation every persisted payload must match.
async fn fresh_tree_heads(store: &SqliteBigRepoStore, tree: SedimentreeId) -> Res<Vec<CommitId>> {
    let commits = store.load_loose_commit_metas(tree).await?;
    let fragments = store.load_fragment_metas(tree).await?;
    if commits.is_empty() && fragments.is_empty() {
        return Ok(Vec::new());
    }
    let mut tree = MinimizedSedimentree::new(Sedimentree::new(fragments, commits));
    let mut heads = tree.heads(&CountLeadingZeroBytes);
    heads.sort_unstable();
    Ok(heads)
}

/// A parent inserted before its child produces only the child as the
/// final durable head — including the mid-arrival case where the child
/// lands first and the parent covers it later.
#[tokio::test(flavor = "multi_thread")]
async fn parent_before_child_produces_single_durable_head() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-parent-child", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[12; 32]);
    let tree = SedimentreeId::new([13; 32]);

    // A (root), then B (parent A) — B covers A.
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(2)]);
    assert_eq!(
        store.durable_sedimentree_heads(tree).await?,
        vec![commit_id(2)]
    );

    // Mid-arrival: G (parent F) lands before F. G is transiently a head;
    // once F lands, G covers it and only G remains.
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 7, BTreeSet::from([commit_id(6)])).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 6, BTreeSet::from([commit_id(2)])).await,
    )
    .await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(7)]);
    assert_eq!(
        store.durable_sedimentree_heads(tree).await?,
        vec![commit_id(7)]
    );
    Ok(())
}

/// A complete batch produces one final BigSync projection with no
/// intermediate frontier.
#[tokio::test(flavor = "multi_thread")]
async fn batch_produces_single_final_projection() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-batch", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[14; 32]);
    let tree = SedimentreeId::new([15; 32]);

    let commits = vec![
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
        make_commit_with_parents(&signer, tree, 3, BTreeSet::from([commit_id(2)])).await,
    ];
    Storage::<Sendable>::save_batch(&store, tree, commits, Vec::new()).await?;

    // Only the final head is persisted — no intermediate frontier leaked.
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(3)]);
    assert_eq!(
        store.durable_sedimentree_heads(tree).await?,
        vec![commit_id(3)]
    );
    Ok(())
}

/// Multiple concurrent save calls converge to the same heads as
/// constructing a Sedimentree from all durable rows.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_saves_converge_to_full_recompute() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-concurrent", BuckId::MAX_LEVEL).await?;
    let tree = SedimentreeId::new([16; 32]);

    let mut handles = Vec::new();
    for byte in 1..=8u8 {
        let store = store.clone();
        let signer = MemorySigner::from_bytes(&[byte; 32]);
        handles.push(tokio::spawn(async move {
            let commit = make_commit(&signer, tree, byte).await;
            Storage::<Sendable>::save_loose_commit(&store, tree, commit).await
        }));
    }
    for handle in handles {
        handle.await??;
    }

    let expected = fresh_tree_heads(&store, tree).await?;
    assert_eq!(payload_heads(&store, tree).await?, expected);
    assert_eq!(store.durable_sedimentree_heads(tree).await?, expected);
    Ok(())
}

/// Fragment insertion changes heads identically to sedimentree_core.
#[tokio::test(flavor = "multi_thread")]
async fn fragment_insertion_matches_sedimentree_core() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-frag-insert", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[17; 32]);
    let tree = SedimentreeId::new([18; 32]);

    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    // Root fragment (head 0B, empty boundary) — a fresh frontier commit.
    Storage::<Sendable>::save_fragment(
        &store,
        tree,
        make_fragment(&signer, tree, 11, BTreeSet::new()).await,
    )
    .await?;
    // Non-root fragment (head 02, boundary {01}) — covers 01; 02 is
    // already a loose head, so the frontier is unchanged.
    Storage::<Sendable>::save_fragment(
        &store,
        tree,
        make_fragment(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;

    let expected = fresh_tree_heads(&store, tree).await?;
    assert_eq!(payload_heads(&store, tree).await?, expected);
    assert_eq!(store.durable_sedimentree_heads(tree).await?, expected);
    Ok(())
}

/// A fragment and the loose history recoverable from its blob must never
/// coexist durably after the write transaction commits.
#[tokio::test(flavor = "multi_thread")]
async fn fragment_write_durably_prunes_covered_loose_history() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-durable-prune", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[41; 32]);
    let tree = SedimentreeId::new([42; 32]);

    let commits = vec![
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
        make_commit_with_parents(&signer, tree, 0, BTreeSet::from([commit_id(2)])).await,
    ];
    let fragments = vec![make_fragment(&signer, tree, 0, BTreeSet::new()).await];
    Storage::<Sendable>::save_batch(&store, tree, commits, fragments).await?;

    assert!(store.load_loose_commit_metas(tree).await?.is_empty());
    assert_eq!(store.load_fragment_metas(tree).await?.len(), 1);
    let loose_index_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM big_repo_causal_ciphertext_index
            WHERE scope_id = ?1 AND sedimentree_id = ?2 AND kind = 0",
    )
    .bind(store.scope_id)
    .bind(SqliteBigRepoStore::tree_blob(tree))
    .fetch_one(&store.sql.read_pool)
    .await?;
    assert_eq!(loose_index_rows, 0);
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(0)]);
    Ok(())
}

/// Once fragment-covered history is durably forgotten, a loose residue
/// learned from another schedule remains present across cache loss.
#[tokio::test(flavor = "multi_thread")]
async fn post_fragment_residue_survives_cache_eviction() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-pruned-residue", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[43; 32]);
    let tree = SedimentreeId::new([44; 32]);

    Storage::<Sendable>::save_batch(
        &store,
        tree,
        vec![
            make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
            make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
            make_commit_with_parents(&signer, tree, 0, BTreeSet::from([commit_id(2)])).await,
        ],
        vec![make_fragment(&signer, tree, 0, BTreeSet::new()).await],
    )
    .await?;

    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    assert_eq!(store.load_loose_commit_metas(tree).await?.len(), 1);

    store.tree_cache.lock().expect(ERROR_MUTEX).remove(&tree);
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 4, BTreeSet::from([commit_id(0)])).await,
    )
    .await?;

    let commits: Set<_> = store
        .load_loose_commit_metas(tree)
        .await?
        .into_iter()
        .map(|commit| commit.head())
        .collect();
    assert_eq!(commits, Set::from([commit_id(1), commit_id(4)]));
    assert_eq!(
        payload_heads(&store, tree).await?,
        vec![commit_id(1), commit_id(4)]
    );
    Ok(())
}

/// Pruning is part of the same SQLite transaction as the fragment write;
/// rollback restores both the covered loose rows and the absent fragment.
#[tokio::test(flavor = "multi_thread")]
async fn rollback_restores_rows_speculatively_pruned_by_fragment() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-prune-rollback", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[45; 32]);
    let tree = SedimentreeId::new([46; 32]);

    Storage::<Sendable>::save_batch(
        &store,
        tree,
        vec![
            make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
            make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
            make_commit_with_parents(&signer, tree, 0, BTreeSet::from([commit_id(2)])).await,
        ],
        Vec::new(),
    )
    .await?;

    let mut tx = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
    let (_, guard) = store
        .mutate_tree_in_tx(
            &mut tx,
            tree,
            TreeStorageMutation::InsertFragment(
                make_fragment(&signer, tree, 0, BTreeSet::new()).await,
            ),
        )
        .await?;
    drop(tx);
    drop(guard);

    assert_eq!(store.load_loose_commit_metas(tree).await?.len(), 3);
    assert!(store.load_fragment_metas(tree).await?.is_empty());
    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));
    Ok(())
}

/// Fragment deletion rebuilds the projection from the durable rows that
/// remain after canonical pruning.
#[tokio::test(flavor = "multi_thread")]
async fn fragment_deletion_rebuilds_and_reveals_covered_commits() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-frag-del", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[19; 32]);
    let tree = SedimentreeId::new([20; 32]);

    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    Storage::<Sendable>::save_fragment(
        &store,
        tree,
        make_fragment(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(2)]);

    // Deleting the fragment rebuilds from the remaining rows; the loose
    // DAG still yields the same frontier.
    Storage::<Sendable>::delete_fragment(&store, tree, commit_id(2)).await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(2)]);
    assert_eq!(
        store.durable_sedimentree_heads(tree).await?,
        vec![commit_id(2)]
    );

    // Deleting the covering commit reveals the previously covered one.
    Storage::<Sendable>::delete_loose_commit(&store, tree, commit_id(2)).await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(1)]);
    assert_eq!(
        store.durable_sedimentree_heads(tree).await?,
        vec![commit_id(1)]
    );
    Ok(())
}

/// Loose commit deletion produces canonical heads.
#[tokio::test(flavor = "multi_thread")]
async fn loose_commit_deletion_produces_canonical_heads() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-commit-del", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[21; 32]);
    let tree = SedimentreeId::new([22; 32]);

    // A → B → C. Deleting B leaves A and C as unrelated roots (C's parent
    // is absent) — the canonical sedimentree_core result.
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 3, BTreeSet::from([commit_id(2)])).await,
    )
    .await?;
    Storage::<Sendable>::delete_loose_commit(&store, tree, commit_id(2)).await?;

    let expected = fresh_tree_heads(&store, tree).await?;
    assert_eq!(expected, vec![commit_id(1), commit_id(3)]);
    assert_eq!(payload_heads(&store, tree).await?, expected);
    assert_eq!(store.durable_sedimentree_heads(tree).await?, expected);
    Ok(())
}

/// Whole-tree removal (the `remove_sedimentree` sequence) removes the
/// cache entry and the tree registration. The BigSync payload is managed
/// by the mutation paths, so the full sequence leaves `{"heads": []}` —
/// `delete_sedimentree_id` itself does not touch the payload row.
#[tokio::test(flavor = "multi_thread")]
async fn whole_tree_deletion_removes_cache_entry_and_registration() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-tree-del", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[23; 32]);
    let tree = SedimentreeId::new([24; 32]);

    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    assert!(store.tree_cache.lock().unwrap().entries.contains_key(&tree));

    // The full removal sequence as `remove_sedimentree` performs it.
    Storage::<Sendable>::delete_loose_commits(&store, tree).await?;
    Storage::<Sendable>::delete_fragments(&store, tree).await?;
    Storage::<Sendable>::delete_sedimentree_id(&store, tree).await?;

    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));
    assert!(store.durable_sedimentree_heads(tree).await?.is_empty());
    // The payload reflects the last mutation (empty frontier), not the
    // deleted tree's old heads.
    let obj_id = SqliteBigRepoStore::obj_id(tree);
    assert_eq!(
        HostPartStore::obj_payload(&store, obj_id).await?,
        Some(serde_json::json!({ "heads": [] }))
    );
    Ok(())
}

/// Cache eviction followed by another write rehydrates correctly.
#[tokio::test(flavor = "multi_thread")]
async fn cache_eviction_rehydrates_on_next_write() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let mut store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-evict", BuckId::MAX_LEVEL).await?;
    // Tiny cache: 3 metadata items ≈ one single-commit tree (cost 2) plus
    // room for one more before eviction.
    store.tree_cache = Arc::new(std::sync::Mutex::new(TreeCache::new(3)));
    let signer = MemorySigner::from_bytes(&[25; 32]);
    let tree_a = SedimentreeId::new([26; 32]);
    let tree_b = SedimentreeId::new([27; 32]);

    Storage::<Sendable>::save_loose_commit(&store, tree_a, make_commit(&signer, tree_a, 1).await)
        .await?;
    Storage::<Sendable>::save_loose_commit(&store, tree_b, make_commit(&signer, tree_b, 2).await)
        .await?;
    // tree_a (cost 2) was evicted to fit tree_b (cost 2) in capacity 3.
    assert!(
        !store
            .tree_cache
            .lock()
            .unwrap()
            .entries
            .contains_key(&tree_a)
    );

    // A write to the evicted tree rehydrates from durable rows.
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree_a,
        make_commit_with_parents(&signer, tree_a, 3, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    assert_eq!(payload_heads(&store, tree_a).await?, vec![commit_id(3)]);
    assert_eq!(
        store.durable_sedimentree_heads(tree_a).await?,
        vec![commit_id(3)]
    );
    Ok(())
}

/// A tree or batch whose metadata weight exceeds the cache capacity must
/// be used transiently for the transaction and then left uncached — it
/// must never evict itself before heads are captured and panic.
#[tokio::test(flavor = "multi_thread")]
async fn oversized_tree_does_not_panic_and_is_left_uncached() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let mut store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-oversized", BuckId::MAX_LEVEL).await?;
    // Capacity smaller than a single tree: a 3-commit batch has metadata
    // weight 4 > 2.
    store.tree_cache = Arc::new(std::sync::Mutex::new(TreeCache::new(2)));
    let signer = MemorySigner::from_bytes(&[37; 32]);
    let tree = SedimentreeId::new([38; 32]);

    let commits = vec![
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
        make_commit_with_parents(&signer, tree, 3, BTreeSet::from([commit_id(2)])).await,
    ];
    Storage::<Sendable>::save_batch(&store, tree, commits, Vec::new()).await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(3)]);
    // The oversized tree evicted itself after heads were captured.
    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));

    // A subsequent write rehydrates and works.
    Storage::<Sendable>::save_loose_commit(
        &store,
        tree,
        make_commit_with_parents(&signer, tree, 4, BTreeSet::from([commit_id(3)])).await,
    )
    .await?;
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(4)]);
    Ok(())
}

/// Whole-tree deletion must acquire the SQLite writer slot before evicting
/// the cache entry: a concurrent writer that installs its entry after the
/// eviction but before the durable delete would otherwise leave a stale
/// entry for a deleted tree.
#[tokio::test(flavor = "multi_thread")]
async fn whole_tree_deletion_waits_for_writer_and_evicts() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-del-race", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[39; 32]);
    let tree = SedimentreeId::new([40; 32]);

    // Writer A: acquires the tx, then pauses BEFORE installing its cache
    // entry. The deleter's eviction (old code: before BEGIN IMMEDIATE)
    // would race this install.
    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        let mut tx = writer_store
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        writer_store.save_tree(&mut tx, tree).await?;
        if paused_tx.send(()).is_err() {
            warn_loc!(
                ERROR_CALLER,
                "deletion-race test pause signal receiver dropped"
            );
        }
        resume_rx.await.map_err(eyre::Report::from)?;
        let (_, guard) = writer_store
            .mutate_tree_in_tx(
                &mut tx,
                tree,
                TreeStorageMutation::InsertCommit(make_commit(&signer, tree, 1).await),
            )
            .await?;
        tx.commit().await?;
        guard.disarm();
        Ok::<(), SqliteBigRepoStoreError>(())
    });
    paused_rx.await.map_err(eyre::Report::from)?;

    // The deleter is queued behind the writer's BEGIN IMMEDIATE.
    let deleter_store = store.clone();
    let deleter = tokio::spawn(async move {
        Storage::<Sendable>::delete_sedimentree_id(&deleter_store, tree).await
    });

    // Let the writer install its entry and commit; then the deleter
    // proceeds and must evict the committed entry.
    if resume_tx.send(()).is_err() {
        warn_loc!(
            ERROR_CALLER,
            "deletion-race test resume signal receiver dropped"
        );
    }
    writer.await??;
    deleter.await??;

    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));
    assert!(store.durable_sedimentree_heads(tree).await?.is_empty());
    Ok(())
}

/// A forced SQL failure invalidates the speculative cache entry.
#[tokio::test(flavor = "multi_thread")]
async fn forced_sql_failure_invalidates_speculative_entry() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-sql-fail", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[28; 32]);
    let tree = SedimentreeId::new([29; 32]);

    // No save_tree: the commit insert violates the FK to
    // big_repo_subduction_trees, failing the transaction mid-mutation.
    let mut tx = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
    let result = store
        .mutate_tree_in_tx(
            &mut tx,
            tree,
            TreeStorageMutation::InsertCommit(make_commit(&signer, tree, 1).await),
        )
        .await;
    assert!(result.is_err());
    drop(tx); // rollback

    // The speculative entry must not survive the failed transaction.
    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));

    // A subsequent write (with the tree registered) rehydrates cleanly.
    let mut tx = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
    store.save_tree(&mut tx, tree).await?;
    let (_, guard) = store
        .mutate_tree_in_tx(
            &mut tx,
            tree,
            TreeStorageMutation::InsertCommit(make_commit(&signer, tree, 1).await),
        )
        .await?;
    tx.commit().await?;
    guard.disarm();
    assert_eq!(payload_heads(&store, tree).await?, vec![commit_id(1)]);
    Ok(())
}

/// A forced commit failure (rollback) invalidates the entry.
#[tokio::test(flavor = "multi_thread")]
async fn forced_commit_failure_invalidates_entry() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-commit-fail", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[30; 32]);
    let tree = SedimentreeId::new([31; 32]);

    let mut tx = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
    store.save_tree(&mut tx, tree).await?;
    let (_, guard) = store
        .mutate_tree_in_tx(
            &mut tx,
            tree,
            TreeStorageMutation::InsertCommit(make_commit(&signer, tree, 1).await),
        )
        .await?;
    // Simulate a commit failure: roll back and drop the guard without
    // disarming.
    drop(tx);
    drop(guard);
    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));
    Ok(())
}

/// Cancellation during a write cannot leave speculative cache state.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_cannot_leave_speculative_state() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-cancel", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[32; 32]);
    let tree = SedimentreeId::new([33; 32]);

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let task_store = store.clone();
    let handle = tokio::spawn(async move {
        let mut tx = task_store
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        task_store.save_tree(&mut tx, tree).await?;
        let (_, _guard) = task_store
            .mutate_tree_in_tx(
                &mut tx,
                tree,
                TreeStorageMutation::InsertCommit(make_commit(&signer, tree, 1).await),
            )
            .await?;
        if done_tx.send(()).is_err() {
            // The test's receiver was dropped before the signal landed —
            // transient work cancellation (the test aborted), not an
            // invariant break.
            warn_loc!(ERROR_CALLER, "cancellation test signal receiver dropped");
        }
        // Park forever; the task is aborted below, dropping tx + guard
        // mid-flight exactly like a cancelled future.
        std::future::pending::<()>().await;
        Ok::<(), SqliteBigRepoStoreError>(())
    });
    done_rx.await?;
    handle.abort();
    // Aborting always yields a cancelled JoinError — assert it rather
    // than swallowing it.
    assert!(
        handle.await.is_err(),
        "aborted task must not complete normally"
    );

    assert!(!store.tree_cache.lock().unwrap().entries.contains_key(&tree));
    Ok(())
}

/// Reopening the store with an empty cache produces the same heads.
#[tokio::test(flavor = "multi_thread")]
async fn reopen_with_empty_cache_produces_same_heads() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let signer = MemorySigner::from_bytes(&[34; 32]);
    let tree = SedimentreeId::new([35; 32]);

    let store1 =
        SqliteBigRepoStore::new(sql.clone(), "big-repo-sqlite-reopen", BuckId::MAX_LEVEL).await?;
    Storage::<Sendable>::save_loose_commit(
        &store1,
        tree,
        make_commit_with_parents(&signer, tree, 1, BTreeSet::new()).await,
    )
    .await?;
    Storage::<Sendable>::save_loose_commit(
        &store1,
        tree,
        make_commit_with_parents(&signer, tree, 2, BTreeSet::from([commit_id(1)])).await,
    )
    .await?;
    let expected = store1.durable_sedimentree_heads(tree).await?;
    drop(store1);

    // A fresh store on the same database starts with an empty cache.
    let store2 = SqliteBigRepoStore::new(sql, "big-repo-sqlite-reopen", BuckId::MAX_LEVEL).await?;
    assert!(store2.tree_cache.lock().unwrap().entries.is_empty());
    assert_eq!(store2.durable_sedimentree_heads(tree).await?, expected);
    assert_eq!(payload_heads(&store2, tree).await?, expected);
    Ok(())
}

/// Deterministic xorshift64 — no external RNG dependency in tests.
struct XorShift64(u64);
impl XorShift64 {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Randomized operation sequences: the persisted BigSync heads must equal
/// the heads of a fresh tree hydrated from the raw rows after every
/// insert, batch, fragment, and delete.
#[tokio::test(flavor = "multi_thread")]
async fn randomized_operation_sequences_match_sedimentree_core() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-random", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[36; 32]);
    let tree = SedimentreeId::new([37; 32]);
    let mut rng = XorShift64(0xDEAD_BEEF_CAFE_F00D);

    // Random DAG: commit i references each earlier commit with 20%
    // probability — a mix of chains, siblings, and merges.
    const N_COMMITS: u8 = 40;
    let commit_bytes: Vec<u8> = (1..=N_COMMITS).collect();
    let mut parents_of: Vec<BTreeSet<CommitId>> = Vec::new();
    for (i, _byte) in commit_bytes.iter().enumerate() {
        let mut parents = BTreeSet::new();
        for earlier in commit_bytes.iter().take(i) {
            if rng.below(5) == 0 {
                parents.insert(commit_id(*earlier));
            }
        }
        parents_of.push(parents);
    }

    // Random insertion order — exercises child-before-parent arrival.
    let mut order: Vec<u8> = commit_bytes.clone();
    for i in (1..order.len()).rev() {
        let j = rng.below((i + 1) as u64) as usize;
        order.swap(i, j);
    }

    // Insert commits one at a time, checking the payload after each.
    for byte in &order {
        Storage::<Sendable>::save_loose_commit(
            &store,
            tree,
            make_commit_with_parents(&signer, tree, *byte, parents_of[*byte as usize - 1].clone())
                .await,
        )
        .await?;
        let expected = fresh_tree_heads(&store, tree).await?;
        assert_eq!(
            payload_heads(&store, tree).await?,
            expected,
            "payload diverged after commit {byte}"
        );
    }

    // Random fragments: heads drawn from present commits, random
    // boundaries (including members that may not be present yet — the
    // projection must tolerate that, matching sedimentree_core).
    for _ in 0..16 {
        let head_byte = 1 + rng.below(N_COMMITS as u64) as u8;
        let mut boundary = BTreeSet::new();
        for earlier in commit_bytes.iter() {
            if rng.below(4) == 0 {
                boundary.insert(commit_id(*earlier));
            }
        }
        Storage::<Sendable>::save_fragment(
            &store,
            tree,
            make_fragment(&signer, tree, head_byte, boundary).await,
        )
        .await?;
        let expected = fresh_tree_heads(&store, tree).await?;
        assert_eq!(
            payload_heads(&store, tree).await?,
            expected,
            "payload diverged after fragment {head_byte}"
        );
    }

    // Random deletes (commits and fragments), checking after each.
    for _ in 0..12 {
        if rng.below(2) == 0 {
            let byte = 1 + rng.below(N_COMMITS as u64) as u8;
            Storage::<Sendable>::delete_loose_commit(&store, tree, commit_id(byte)).await?;
        } else {
            let byte = 1 + rng.below(N_COMMITS as u64) as u8;
            Storage::<Sendable>::delete_fragment(&store, tree, commit_id(byte)).await?;
        }
        let expected = fresh_tree_heads(&store, tree).await?;
        assert_eq!(
            payload_heads(&store, tree).await?,
            expected,
            "payload diverged after delete"
        );
    }

    // A batch write must also match.
    let batch_commits = vec![
        make_commit_with_parents(&signer, tree, 90, BTreeSet::new()).await,
        make_commit_with_parents(&signer, tree, 91, BTreeSet::from([commit_id(90)])).await,
    ];
    Storage::<Sendable>::save_batch(&store, tree, batch_commits, Vec::new()).await?;
    let expected = fresh_tree_heads(&store, tree).await?;
    assert_eq!(
        payload_heads(&store, tree).await?,
        expected,
        "payload diverged after batch"
    );
    assert_eq!(store.durable_sedimentree_heads(tree).await?, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_big_repo_subduction_roundtrip() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-subduction", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[9; 32]);
    let tree = SedimentreeId::new([4; 32]);
    let verified = make_commit(&signer, tree, 1).await;

    Storage::<Sendable>::save_loose_commit(&store, tree, verified.clone()).await?;
    let loaded = Storage::<Sendable>::load_loose_commits(&store, tree).await?;
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].signed().as_bytes(), verified.signed().as_bytes());
    assert_eq!(loaded[0].blob().as_slice(), verified.blob().as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_big_repo_commit_updates_payload_atomically() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "big-repo-sqlite-atomic", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[10; 32]);
    let tree = SedimentreeId::new([11; 32]);
    let obj_id = SqliteBigRepoStore::obj_id(tree);
    let part_id = PartId(Byte32Id::new([12; 32]));
    HostPartStore::set_obj_payload(&store, obj_id, serde_json::json!({"old": true})).await?;
    HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;

    let commit = make_commit(&signer, tree, 1).await;
    Storage::<Sendable>::save_loose_commit(&store, tree, commit).await?;

    let mut head = [0; 32];
    head[0] = 1;
    let heads: Arc<[automerge::ChangeHash]> = Arc::from(vec![automerge::ChangeHash(head)]);
    let expected = serde_json::json!({
        "heads": am_utils_rs::serialize_commit_heads(&heads),
    });
    assert_eq!(
        HostPartStore::obj_payload(&store, obj_id).await?,
        Some(expected)
    );
    assert_eq!(HostPartStore::member_count(&store, part_id).await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_big_repo_commit_rolls_back_when_payload_update_fails() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "big-repo-sqlite-atomic-failure", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::from_bytes(&[13; 32]);
    let tree = SedimentreeId::new([14; 32]);
    let obj_id = SqliteBigRepoStore::obj_id(tree);
    let part_id = PartId(Byte32Id::new([15; 32]));
    let old_payload = serde_json::json!({"old": true});
    HostPartStore::set_obj_payload(&store, obj_id, old_payload.clone()).await?;
    HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;

    sqlx::query(
        "CREATE TRIGGER fail_big_repo_payload_update
            BEFORE UPDATE OF payload_json ON big_sync_objs
            WHEN hex(NEW.obj_id) = '0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E'
            BEGIN SELECT RAISE(ABORT, 'injected payload failure'); END",
    )
    .execute(&store.sql.write_pool)
    .await?;

    let commit = make_commit(&signer, tree, 1).await;
    assert!(
        Storage::<Sendable>::save_loose_commit(&store, tree, commit)
            .await
            .is_err()
    );
    assert!(
        Storage::<Sendable>::load_loose_commits(&store, tree)
            .await?
            .is_empty()
    );
    assert_eq!(
        HostPartStore::obj_payload(&store, obj_id).await?,
        Some(old_payload)
    );
    assert_eq!(HostPartStore::member_count(&store, part_id).await?, 1);
    Ok(())
}
#[tokio::test]
async fn sqlite_big_repo_keyhive_events_are_ordered_and_deduplicated() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-event-order", BuckId::MAX_LEVEL).await?;
    let first = subduction_keyhive::storage::StorageHash::new([1; 32]);
    let second = subduction_keyhive::storage::StorageHash::new([2; 32]);
    store
        .save_keyhive_event(first, b"first".to_vec(), None)
        .await?;
    store
        .save_keyhive_event(second, b"second".to_vec(), None)
        .await?;
    store
        .save_keyhive_event(first, b"replacement".to_vec(), None)
        .await?;

    assert_eq!(
        store.load_keyhive_events().await?,
        vec![(first, b"first".to_vec()), (second, b"second".to_vec())]
    );
    assert_eq!(store.keyhive_event_log_cursor().await?, 2);
    Ok(())
}

#[tokio::test]
async fn sqlite_big_repo_keyhive_event_log_records_source() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-event-source", BuckId::MAX_LEVEL).await?;
    let hash = subduction_keyhive::storage::StorageHash::new([7; 32]);
    let source = subduction_keyhive::KeyhivePeerId::from_bytes([9; 32]);
    store
        .save_keyhive_event(hash, b"event".to_vec(), Some(source))
        .await?;
    store
        .save_keyhive_event(hash, b"dup".to_vec(), None)
        .await?;

    let rows = sqlx::query(
        "SELECT event_hash, source_id
            FROM big_repo_keyhive_event_log
            WHERE scope_id = ?1",
    )
    .bind(store.scope_id)
    .fetch_all(&store.sql.read_pool)
    .await?;
    assert_eq!(rows.len(), 1, "duplicate hash must not add a row");
    let source_id: Option<Vec<u8>> = rows[0].try_get("source_id")?;
    assert_eq!(source_id, Some(vec![9; 32]), "source must be recorded");
    Ok(())
}

#[tokio::test]
async fn sqlite_big_repo_keyhive_event_log_retains_everything() -> Res<()> {
    // The arrival log is keyhive's recovery source: `ingest_from_storage`
    // replays it wholesale and no snapshot-boundary marker exists, so no
    // row may ever be pruned. Workers consume the admission log instead.
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-event-retention", BuckId::MAX_LEVEL).await?;
    for n in 0..5u8 {
        store
            .save_keyhive_event(
                subduction_keyhive::storage::StorageHash::new([n; 32]),
                vec![n],
                None,
            )
            .await?;
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = ?1")
            .bind(store.scope_id)
            .fetch_one(&store.sql.read_pool)
            .await?;
    assert_eq!(count, 5, "arrival log must never prune rows");
    assert_eq!(store.keyhive_event_log_cursor().await?, 5);
    Ok(())
}

#[tokio::test]
async fn sqlite_big_repo_admission_log_appends_dedups_and_replays() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-admission", BuckId::MAX_LEVEL).await?;
    let first = subduction_keyhive::storage::StorageHash::new([1; 32]);
    let second = subduction_keyhive::storage::StorageHash::new([2; 32]);
    store
        .save_keyhive_event(first, b"first".to_vec(), None)
        .await?;
    store
        .save_keyhive_event(second, b"second".to_vec(), None)
        .await?;

    store
        .append_admitted_events(vec![second, first], None)
        .await
        .expect("admission of known hashes must succeed");
    // Re-reporting an admitted hash is a no-op.
    store
        .append_admitted_events(vec![first], None)
        .await
        .expect("re-admission must be a no-op, not an error");

    let rows = store.admission_events_after(0, 100).await?;
    assert_eq!(rows.len(), 2, "duplicate admission must not add a row");
    // Intra-batch order follows the reporter's hash iteration, which is
    // deliberately unspecified — only cross-batch monotonicity holds.
    let mut admitted_bytes: Vec<Vec<u8>> = rows.iter().map(|row| row.bytes.clone()).collect();
    admitted_bytes.sort();
    assert_eq!(
        admitted_bytes,
        vec![b"first".to_vec(), b"second".to_vec()],
        "both reported hashes must be admitted exactly once"
    );
    assert!(rows[0].seq < rows[1].seq, "seqs must be monotonic");
    let tail = store.admission_events_after(rows[0].seq, 100).await?;
    assert_eq!(tail.len(), 1, "cursor replay resumes after cursor");
    assert_eq!(tail[0].seq, rows[1].seq);
    assert_eq!(tail[0].bytes, rows[1].bytes);
    Ok(())
}

#[tokio::test]
async fn sqlite_big_repo_admission_log_fails_loud_on_unknown_hash() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "keyhive-admission-missing", BuckId::MAX_LEVEL).await?;
    let unknown = subduction_keyhive::storage::StorageHash::new([42; 32]);
    let result = store.append_admitted_events(vec![unknown], None).await;
    assert!(
        result.is_err(),
        "admitting an unarrived hash must fail loudly"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_big_repo_keyhive_event_tail_deletion_keeps_history() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-event-tail", BuckId::MAX_LEVEL).await?;
    let hash = subduction_keyhive::storage::StorageHash::new([3; 32]);
    store
        .save_keyhive_event(hash, b"event".to_vec(), None)
        .await?;
    store.delete_keyhive_event(hash).await?;

    assert!(store.load_keyhive_events().await?.is_empty());
    let immutable_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM big_repo_keyhive_event_log WHERE scope_id = ?1")
            .bind(store.scope_id)
            .fetch_one(&store.sql.read_pool)
            .await?;
    assert_eq!(immutable_count, 1);
    Ok(())
}

#[tokio::test]
async fn sqlite_big_repo_keyhive_events_are_scope_isolated() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let first_store =
        SqliteBigRepoStore::new(sql.clone(), "keyhive-scope-a", BuckId::MAX_LEVEL).await?;
    let second_store = SqliteBigRepoStore::new(sql, "keyhive-scope-b", BuckId::MAX_LEVEL).await?;
    let hash = subduction_keyhive::storage::StorageHash::new([4; 32]);
    first_store
        .save_keyhive_event(hash, b"a".to_vec(), None)
        .await?;
    second_store
        .save_keyhive_event(hash, b"b".to_vec(), None)
        .await?;

    assert_eq!(
        first_store.load_keyhive_events().await?,
        vec![(hash, b"a".to_vec())]
    );
    assert_eq!(
        second_store.load_keyhive_events().await?,
        vec![(hash, b"b".to_vec())]
    );
    Ok(())
}
#[tokio::test]
async fn sqlite_big_repo_keyhive_events_survive_restart() -> Res<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("events.sqlite");
    let url = format!("sqlite://{}", db_path.display());
    let first = SqlCtx::url(&url).await?;
    let store = SqliteBigRepoStore::new(first, "keyhive-restart", BuckId::MAX_LEVEL).await?;
    let hash = subduction_keyhive::storage::StorageHash::new([5; 32]);
    store
        .save_keyhive_event(hash, b"persistent".to_vec(), None)
        .await?;
    drop(store);

    let reopened = SqlCtx::url(&url).await?;
    let store = SqliteBigRepoStore::new(reopened, "keyhive-restart", BuckId::MAX_LEVEL).await?;
    assert_eq!(
        store.load_keyhive_events().await?,
        vec![(hash, b"persistent".to_vec())]
    );
    Ok(())
}

#[tokio::test]
async fn causal_checkpoint_cursor_is_monotonic_and_survives_restart() -> Res<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("causal-checkpoint-cursor.sqlite");
    let url = format!("sqlite://{}", db_path.display());
    let scope = "causal-checkpoint-cursor";
    let store = SqliteBigRepoStore::new(SqlCtx::url(&url).await?, scope, BuckId::MAX_LEVEL).await?;

    assert_eq!(store.causal_checkpoint_cursor().await?, 0);
    store.advance_causal_checkpoint_cursor(7).await?;
    store.advance_causal_checkpoint_cursor(3).await?;
    assert_eq!(store.causal_checkpoint_cursor().await?, 7);
    drop(store);

    let reopened =
        SqliteBigRepoStore::new(SqlCtx::url(&url).await?, scope, BuckId::MAX_LEVEL).await?;
    assert_eq!(reopened.causal_checkpoint_cursor().await?, 7);
    Ok(())
}
#[tokio::test]
async fn sqlite_big_repo_keyhive_duplicate_saves_are_safe_concurrently() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-concurrent", BuckId::MAX_LEVEL).await?;
    let hash = subduction_keyhive::storage::StorageHash::new([6; 32]);
    let left = store.clone();
    let right = store.clone();
    let (left, right) = tokio::join!(
        left.save_keyhive_event(hash, b"left".to_vec(), None),
        right.save_keyhive_event(hash, b"right".to_vec(), None),
    );
    left?;
    right?;
    let events = store.load_keyhive_events().await?;
    assert_eq!(events.len(), 1);
    assert!(events[0].1 == b"left" || events[0].1 == b"right");
    Ok(())
}
#[tokio::test]
async fn sqlite_big_repo_keyhive_archive_changes_do_not_prune_event_log() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "keyhive-archive", BuckId::MAX_LEVEL).await?;
    let archive_dir = tempfile::tempdir()?;
    let storage = crate::keyhive_storage::BigRepoKeyhiveStorage::fs(
        store.clone(),
        archive_dir.path().to_path_buf(),
    )?;
    let event_hash = subduction_keyhive::storage::StorageHash::new([7; 32]);
    let archive_hash = subduction_keyhive::storage::StorageHash::new([8; 32]);
    subduction_keyhive::storage::KeyhiveStorage::<Sendable>::save_event(
        &storage,
        event_hash,
        b"event".to_vec(),
    )
    .await?;
    subduction_keyhive::storage::KeyhiveStorage::<Sendable>::save_archive(
        &storage,
        archive_hash,
        b"archive".to_vec(),
    )
    .await?;
    subduction_keyhive::storage::KeyhiveStorage::<Sendable>::delete_archive(&storage, archive_hash)
        .await?;

    assert!(
        subduction_keyhive::storage::KeyhiveStorage::<Sendable>::load_archives(&storage,)
            .await?
            .is_empty()
    );
    assert_eq!(
        subduction_keyhive::storage::KeyhiveStorage::<Sendable>::load_events(&storage).await?,
        vec![(event_hash, b"event".to_vec())]
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_adds_managed_parts() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-add", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([1; 32]));
    let group_part = PartId(Byte32Id::new([2; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(group_part).await?;
    store.ensure_part(crate::GLOBAL_PART_ID).await?;

    let mutations = vec![GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([group_part]),
        desired_group_parts: HashSet::from([group_part]),
        desired_global: true,
    }];
    store
        .reconcile_group_part_batch(&mutations, 42, true)
        .await?;

    let parts = HostPartStore::obj_parts(&store, doc).await?;
    assert!(
        parts.contains(&group_part),
        "doc should be in the managed group part"
    );
    assert!(
        parts.contains(&crate::GLOBAL_PART_ID),
        "doc should be in the global part when desired_global=true"
    );
    assert_eq!(store.keyhive_group_part_cursor().await?, 42);
    Ok(())
}

#[tokio::test]
async fn reconcile_batch_assigns_unique_paginateable_part_cursors() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "reconcile-cursor-siblings", BuckId::MAX_LEVEL).await?;
    let part = PartId(Byte32Id::new([3; 32]));
    let docs = [ObjId(Byte32Id::new([4; 32])), ObjId(Byte32Id::new([5; 32]))];
    store.ensure_part(part).await?;
    for doc in docs {
        HostPartStore::set_obj_payload(&store, doc, serde_json::json!(doc.to_string())).await?;
    }
    let mutations = docs.map(|doc| GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    });
    store
        .reconcile_group_part_batch(&mutations, 7, true)
        .await?;

    let first = HostPartStore::list_events(&store, HashSet::from([part]), 0, 1)
        .await??
        .remove(&part)
        .expect("requested part page");
    assert_eq!(first.events.len(), 1);
    let cursor = match &first.events[0] {
        PartEvent::Added(event) => event.cursor,
        other => panic!("expected first sibling addition, got {other:?}"),
    };
    let second = HostPartStore::list_events(&store, HashSet::from([part]), cursor, 1)
        .await??
        .remove(&part)
        .expect("requested continuation page");
    assert_eq!(second.events.len(), 1, "continuation must retain sibling");
    let second_cursor = match &second.events[0] {
        PartEvent::Added(event) => event.cursor,
        other => panic!("expected second sibling addition, got {other:?}"),
    };
    assert!(
        second_cursor > cursor,
        "sibling events need distinct cursors"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_removes_stale_managed_membership() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-stale", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([10; 32]));
    let part_a = PartId(Byte32Id::new([11; 32]));
    let part_b = PartId(Byte32Id::new([12; 32]));
    let part_c = PartId(Byte32Id::new([13; 32]));
    let peer = PeerId(Byte32Id::new([14; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part_a).await?;
    store.ensure_part(part_b).await?;
    store.ensure_part(part_c).await?;
    HostPartStore::add_obj_to_parts(&store, doc, vec![part_a, part_b, part_c]).await?;

    let parts_before = HostPartStore::obj_parts(&store, doc).await?;
    assert_eq!(parts_before.len(), 3);

    let mutations = vec![GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part_a, part_b]),
        desired_group_parts: HashSet::from([part_a]),
        desired_global: false,
    }];
    store
        .reconcile_group_part_batch(&mutations, 100, true)
        .await?;

    let parts = HostPartStore::obj_parts(&store, doc).await?;
    assert!(
        parts.contains(&part_a),
        "desired managed part should remain"
    );
    assert!(
        !parts.contains(&part_b),
        "stale managed part should be removed"
    );
    assert!(
        parts.contains(&part_c),
        "unrelated (non-managed) part should be preserved"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_cursor_advances() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-cursor", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([20; 32]));
    let part = PartId(Byte32Id::new([21; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;

    let m = |_cursor| GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    };
    store
        .reconcile_group_part_batch(&[m(0)], 200, false)
        .await?;
    assert_eq!(store.keyhive_group_part_cursor().await?, 0);

    store.reconcile_group_part_batch(&[m(0)], 300, true).await?;
    assert_eq!(store.keyhive_group_part_cursor().await?, 300);
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_rolls_back_on_cursor_update_failure() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-rollback", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([30; 32]));
    let part = PartId(Byte32Id::new([31; 32]));
    let peer = PeerId(Byte32Id::new([32; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;

    sqlx::query(
        "CREATE TRIGGER fail_cursor_update
            BEFORE UPDATE OF cursor ON big_repo_group_part_cursor
            BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END",
    )
    .execute(&store.sql.write_pool)
    .await?;

    let mutations = vec![GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    }];
    assert!(
        store
            .reconcile_group_part_batch(&mutations, 42, true)
            .await
            .is_err()
    );

    assert!(
        HostPartStore::obj_parts(&store, doc).await?.is_empty(),
        "no part membership should survive a failed transaction"
    );
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        0,
        "group-part cursor should remain at the initial value after rollback"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_removes_global_when_desired_global_drops() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-global-drop", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([40; 32]));
    let group_part = PartId(Byte32Id::new([41; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(group_part).await?;
    store.ensure_part(crate::GLOBAL_PART_ID).await?;
    // Local decision puts the doc into both the managed group part AND the
    // global part. (Gossip must never populate the global part — see
    // add_obj_to_parts — so seeding goes through reconciliation.)
    let seed: &[GroupPartReconciliation] = &[GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([group_part]),
        desired_group_parts: HashSet::from([group_part]),
        desired_global: true,
    }];
    store.reconcile_group_part_batch(seed, 100, true).await?;

    let parts_before = HostPartStore::obj_parts(&store, doc).await?;
    assert!(parts_before.contains(&crate::GLOBAL_PART_ID));
    assert!(parts_before.contains(&group_part));

    // Reconcile: same managed part desired, but desired_global dropped to false.
    // This exercises the code path at lines ~2012-2015 where GLOBAL_PART_ID is
    // added to stale outside of the managed_group_parts intersection.
    let mutations = vec![GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([group_part]),
        desired_group_parts: HashSet::from([group_part]),
        desired_global: false,
    }];
    store
        .reconcile_group_part_batch(&mutations, 110, true)
        .await?;

    let parts = HostPartStore::obj_parts(&store, doc).await?;
    assert!(parts.contains(&group_part), "managed part should remain");
    assert!(
        !parts.contains(&crate::GLOBAL_PART_ID),
        "global part should be removed when desired_global drops to false"
    );
    assert_eq!(store.keyhive_group_part_cursor().await?, 110);
    Ok(())
}

#[tokio::test]
async fn remote_gossip_never_records_global_part_membership() -> Res<()> {
    // add_obj_to_parts is the big-sync gossip path (peer membership events).
    // The global partition records only local readability decisions from
    // group-part reconciliation, so gossip must drop it while other parts
    // still record normally.
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "gossip-global-filter", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([7; 32]));
    let group_part = PartId(Byte32Id::new([8; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(group_part).await?;
    store.ensure_part(crate::GLOBAL_PART_ID).await?;

    HostPartStore::add_obj_to_parts(&store, doc, vec![group_part, crate::GLOBAL_PART_ID]).await?;

    let parts = HostPartStore::obj_parts(&store, doc).await?;
    assert_eq!(
        parts,
        vec![group_part],
        "gossip records non-global parts only"
    );
    assert_eq!(
        HostPartStore::member_count(&store, crate::GLOBAL_PART_ID).await?,
        0,
        "global part must stay empty under gossip"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_noop_still_advances_cursor() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-noop", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([50; 32]));
    let part = PartId(Byte32Id::new([51; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;
    // Put the doc into the part first so the reconciliation is a no-op.
    HostPartStore::add_obj_to_parts(&store, doc, vec![part]).await?;

    let m = GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    };
    store.reconcile_group_part_batch(&[m], 500, true).await?;
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        500,
        "cursor should advance even when no transitions occur"
    );

    // Verify state is unchanged: doc is still in the part, and no spurious
    // part removals occurred.
    let parts = HostPartStore::obj_parts(&store, doc).await?;
    assert!(parts.contains(&part), "doc should still be in the part");
    assert_eq!(parts.len(), 1, "no extra parts should appear");
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_empty_mutations_advances_cursor() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-empty", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([60; 32]));
    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;

    // Empty mutations slice: no documents affected by events.
    // This mirrors the case where GroupPartWorker sees PrekeysExpanded
    // or PrekeyRotated events that produce zero affected_documents.
    store.reconcile_group_part_batch(&[], 600, true).await?;

    // Cursor should advance even with zero mutations.
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        600,
        "cursor must advance when no documents are affected"
    );

    // No state should be modified: no part memberships created.
    assert!(
        HostPartStore::obj_parts(&store, doc).await?.is_empty(),
        "empty mutations must not create part memberships"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_idempotent_duplicate_delivery() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-idempotent", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([70; 32]));
    let part = PartId(Byte32Id::new([71; 32]));
    let peer = PeerId(Byte32Id::new([72; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;

    let m = GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    };

    // First delivery: reconcile once.
    store
        .reconcile_group_part_batch(std::slice::from_ref(&m), 700, true)
        .await?;
    let parts_after_first = HostPartStore::obj_parts(&store, doc).await?;
    assert!(
        parts_after_first.contains(&part),
        "doc should be in the part after first reconciliation"
    );
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        700,
        "cursor should advance after first delivery"
    );

    // Second delivery: same reconciliation, same event cursor.
    // This mirrors replaying a Keyhive event whose reconciliation
    // is identical to the already-applied state.
    store.reconcile_group_part_batch(&[m], 700, true).await?;
    let parts_after_second = HostPartStore::obj_parts(&store, doc).await?;
    assert_eq!(
        parts_after_first, parts_after_second,
        "duplicate reconciliation must produce identical membership"
    );
    assert!(
        parts_after_second.contains(&part),
        "doc should remain in the part after duplicate delivery"
    );
    assert_eq!(
        parts_after_second.len(),
        1,
        "no extra parts should appear after duplicate delivery"
    );
    // Cursor must advance monotonically (higher wins).
    store
        .reconcile_group_part_batch(
            &[GroupPartReconciliation {
                doc,
                agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
                managed_group_parts: HashSet::from([part]),
                desired_group_parts: HashSet::from([part]),
                desired_global: false,
            }],
            800,
            true,
        )
        .await?;
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        800,
        "cursor must advance monotonically across repeated reconciliations"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_cursor_survives_store_restart() -> Res<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("reconcile-restart.sqlite");
    let url = format!("sqlite://{}", db_path.display());

    let first = SqlCtx::url(&url).await?;
    let store = SqliteBigRepoStore::new(first, "reconcile-restart", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([80; 32]));
    let part = PartId(Byte32Id::new([81; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;

    let m = GroupPartReconciliation {
        doc,
        agents: HashMap::new(),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    };
    store.reconcile_group_part_batch(&[m], 900, true).await?;
    assert_eq!(store.keyhive_group_part_cursor().await?, 900);
    assert!(
        HostPartStore::obj_parts(&store, doc).await?.contains(&part),
        "doc should be in part before restart"
    );
    drop(store);

    // Reopen the same database.
    let reopened = SqlCtx::url(&url).await?;
    let store = SqliteBigRepoStore::new(reopened, "reconcile-restart", BuckId::MAX_LEVEL).await?;

    // Cursor must survive restart.
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        900,
        "cursor must persist across store restart"
    );
    // Part membership must survive restart.
    assert!(
        HostPartStore::obj_parts(&store, doc).await?.contains(&part),
        "part membership must persist across store restart"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_rolls_back_on_syncable_write_failure() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-syncable-fail", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([90; 32]));
    let part = PartId(Byte32Id::new([91; 32]));
    let peer = PeerId(Byte32Id::new([92; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;
    // Put doc in part so a later removal has stale parts to process.
    HostPartStore::add_obj_to_parts(&store, doc, vec![part]).await?;
    store
        .set_obj_members(
            doc,
            HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        )
        .await?;

    // Inject failure on the syncable DELETE (which runs before member UPDATE).
    sqlx::query(
        "CREATE TRIGGER fail_syncable_delete
            BEFORE DELETE ON big_sync_syncable
            BEGIN SELECT RAISE(ABORT, 'injected syncable failure'); END",
    )
    .execute(&store.sql.write_pool)
    .await?;

    let mutations = vec![GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::new(),
        desired_global: false,
    }];
    assert!(
        store
            .reconcile_group_part_batch(&mutations, 42, true)
            .await
            .is_err(),
        "reconciliation must fail when syncable DELETE fails"
    );

    // Verify no state leaked: cursor unchanged.
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        0,
        "cursor must remain at initial value after syncable-write rollback"
    );
    // Doc should still be in the part (no removal applied).
    assert!(
        HostPartStore::obj_parts(&store, doc).await?.contains(&part),
        "part membership must survive syncable-write rollback"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_rolls_back_on_member_insert_failure() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store =
        SqliteBigRepoStore::new(sql, "reconcile-member-insert-fail", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([100; 32]));
    let part = PartId(Byte32Id::new([101; 32]));
    let peer = PeerId(Byte32Id::new([102; 32]));

    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;

    // Inject failure on member INSERT (runs during Live transition).
    sqlx::query(
        "CREATE TRIGGER fail_member_insert
            BEFORE INSERT ON big_sync_members
            BEGIN SELECT RAISE(ABORT, 'injected member insert failure'); END",
    )
    .execute(&store.sql.write_pool)
    .await?;

    let mutations = vec![GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    }];
    assert!(
        store
            .reconcile_group_part_batch(&mutations, 42, true)
            .await
            .is_err(),
        "reconciliation must fail when member INSERT fails"
    );

    // Verify no state leaked: cursor unchanged.
    assert_eq!(
        store.keyhive_group_part_cursor().await?,
        0,
        "cursor must remain at initial value after member-insert rollback"
    );
    // No membership should be created (INSERT was aborted).
    assert!(
        HostPartStore::obj_parts(&store, doc).await?.is_empty(),
        "no part membership should survive member-insert rollback"
    );
    // The syncable write rolled back too — no agents persisted for this doc.
    let agents_in_syncable: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2",
    )
    .bind(store.scope_id)
    .bind(SqliteBigRepoStore::obj_blob(doc))
    .fetch_one(&store.sql.read_pool)
    .await?;
    assert_eq!(
        agents_in_syncable, 0,
        "syncable write must be rolled back when member INSERT fails"
    );
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_rolls_back_on_bucket_write_failure() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-bucket-fail", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([110; 32]));
    let part = PartId(Byte32Id::new([111; 32]));
    let peer = PeerId(Byte32Id::new([112; 32]));
    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;
    sqlx::query(
        "CREATE TRIGGER fail_bucket_insert
            BEFORE INSERT ON big_sync_buckets
            BEGIN SELECT RAISE(ABORT, 'injected bucket failure'); END",
    )
    .execute(&store.sql.write_pool)
    .await?;
    let mutation = GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    };
    assert!(
        store
            .reconcile_group_part_batch(&[mutation], 42, true)
            .await
            .is_err()
    );
    assert_eq!(store.keyhive_group_part_cursor().await?, 0);
    assert!(HostPartStore::obj_parts(&store, doc).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn reconcile_group_part_batch_rolls_back_on_part_cursor_write_failure() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "reconcile-part-fail", BuckId::MAX_LEVEL).await?;
    let doc = ObjId(Byte32Id::new([120; 32]));
    let part = PartId(Byte32Id::new([121; 32]));
    let peer = PeerId(Byte32Id::new([122; 32]));
    HostPartStore::set_obj_payload(&store, doc, serde_json::json!("live")).await?;
    store.ensure_part(part).await?;
    sqlx::query(
        "CREATE TRIGGER fail_part_cursor_update
            BEFORE UPDATE OF latest_cursor ON big_sync_parts
            BEGIN SELECT RAISE(ABORT, 'injected part cursor failure'); END",
    )
    .execute(&store.sql.write_pool)
    .await?;
    let mutation = GroupPartReconciliation {
        doc,
        agents: HashMap::from([(peer, keyhive_core::access::Access::Read)]),
        managed_group_parts: HashSet::from([part]),
        desired_group_parts: HashSet::from([part]),
        desired_global: false,
    };
    assert!(
        store
            .reconcile_group_part_batch(&[mutation], 42, true)
            .await
            .is_err()
    );
    assert_eq!(store.keyhive_group_part_cursor().await?, 0);
    assert!(HostPartStore::obj_parts(&store, doc).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn tree_cache_guard_stale_drop_does_not_evict_newer_transaction_cache_entry() -> Res<()> {
    let sql = SqlCtx::memory().await?;
    let store = SqliteBigRepoStore::new(sql, "guard-stale-drop", BuckId::MAX_LEVEL).await?;
    let signer = MemorySigner::generate();
    let tree_id = SedimentreeId::new([42; 32]);
    let commit_a = make_commit(&signer, tree_id, 1).await;
    let commit_b = make_commit(&signer, tree_id, 2).await;
    let commit_c = make_commit(&signer, tree_id, 3).await;

    // Transaction 1: speculatively mutates the tree but is abandoned (simulating tx.commit failure or cancellation).
    let guard1 = {
        let mut tx1 = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        store.save_tree(&mut tx1, tree_id).await?;
        let (_events1, guard1) = store
            .mutate_tree_in_tx(
                &mut tx1,
                tree_id,
                TreeStorageMutation::InsertCommit(commit_a),
            )
            .await?;
        // Transaction 1 releases the SQLite writer lock without disarming guard1.
        drop(tx1);
        guard1
    };

    // Transaction 2: starts immediately on another thread, mutates the same tree, and commits.
    {
        let mut tx2 = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        store.save_tree(&mut tx2, tree_id).await?;
        let (_events2, guard2) = store
            .mutate_tree_in_tx(
                &mut tx2,
                tree_id,
                TreeStorageMutation::InsertCommit(commit_b),
            )
            .await?;
        tx2.commit().await?;
        guard2.disarm();
    }

    // Verify the tree entry is present in cache for transaction 2.
    assert!(
        store.tree_cache.lock().unwrap().get(&tree_id).is_some(),
        "tree cache should contain committed entry from transaction 2"
    );

    // Drop the stale guard from transaction 1.
    drop(guard1);

    // Epoch-awareness must ensure the newer entry installed by transaction 2 was NOT evicted by guard1's drop.
    assert!(
        store.tree_cache.lock().unwrap().get(&tree_id).is_some(),
        "stale guard drop must not evict newer cache entry installed by transaction 2"
    );

    // Subsequent mutation should succeed cleanly against the active cached entry.
    {
        let mut tx3 = store.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        store.save_tree(&mut tx3, tree_id).await?;
        let (_events3, guard3) = store
            .mutate_tree_in_tx(
                &mut tx3,
                tree_id,
                TreeStorageMutation::InsertCommit(commit_c),
            )
            .await?;
        tx3.commit().await?;
        guard3.disarm();
    }

    Ok(())
}
