use super::*;

use crate::encrypted_blob::decode_encrypted_blob;
use am_utils_rs::codecs::ThroughJson;
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use autosurgeon::Prop;
use big_sync::backend::contract::{
    self, SyncBackendHarness, SyncBackendOutcome, SyncBackendScenario,
};
use big_sync::stress_support;
use big_sync::{HostPartStore, SyncBackend};
use big_sync_core::{ByteKey, PartKey, PeerKey, SyncCompletionDeets};

/// The band these harnesses run: the one the embedder ships, so big_repo's tests exercise
/// bucket-diff. The offline-reopen stall this used to opt out for is covered by
/// `bucket_band_reconciles_after_offline_reopen`.
const HARNESS_SYNC_MODE: Option<big_sync::SyncMode> = Some(big_sync::SyncMode::Bucket);
use futures::lock::Mutex;
use nonempty::NonEmpty;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use subduction_keyhive::KeyhivePeerId;
use tempfile::tempdir;
use tokio::{sync::Notify, time::timeout};

pub async fn boot_repo() -> Res<(
    Arc<BigRepo>,
    Arc<big_sync::Ctx>,
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
)> {
    utils_rs::testing::setup_tracing_once();
    let (repo, stop) = BigRepo::boot(Config {
        node_identity_seed: [7_u8; 32],
        storage: StorageConfig::Memory,
        scope_key: Arc::from("big-repo-test"),
        hidden_parts: HashSet::new(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
        keyhive_change_notifs: true,
    })
    .await?;
    let shared_store = repo.shared_part_store();
    let (worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&shared_store),
        HashMap::new(),
        "big-repo-boot-repo",
        None,
        HARNESS_SYNC_MODE,
        Arc::from("big-repo-test"),
    )?;
    let big_sync_host = Arc::new(big_sync::Ctx {
        store: shared_store,
        worker,
    });
    Ok((
        repo,
        big_sync_host,
        Box::new(move || {
            async move {
                stop.stop().await?;
                big_sync_stop.stop().await?;
                eyre::Ok(())
            }
            .boxed()
        }),
    ))
}

pub async fn _boot_disk_repo(
    path: PathBuf,
) -> Res<(
    Arc<BigRepo>,
    Arc<big_sync::Ctx>,
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
)> {
    std::fs::create_dir_all(&path)
        .wrap_err_with(|| format!("failed creating disk repo path: {}", path.display()))?;
    let (repo, stop) = BigRepo::boot(Config {
        node_identity_seed: [7_u8; 32],
        storage: StorageConfig::Disk { path },
        scope_key: Arc::from("big-repo-test"),
        hidden_parts: HashSet::new(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
        keyhive_change_notifs: true,
    })
    .await?;
    let shared_store = repo.shared_part_store();
    let (worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&shared_store),
        HashMap::new(),
        "big-repo-boot-disk",
        None,
        HARNESS_SYNC_MODE,
        Arc::from("big-repo-test"),
    )?;
    let big_sync_host = Arc::new(big_sync::Ctx {
        store: shared_store,
        worker,
    });
    Ok((
        repo,
        big_sync_host,
        Box::new(move || {
            async move {
                stop.stop().await?;
                big_sync_stop.stop().await?;
                eyre::Ok(())
            }
            .boxed()
        }),
    ))
}

fn get_int_at_root(doc: &automerge::Automerge, key: &str) -> i64 {
    let value = doc
        .get(automerge::ROOT, key)
        .expect("failed reading document")
        .expect("missing key");
    let automerge::Value::Scalar(scalar) = value.0 else {
        panic!("expected scalar value at root");
    };
    match scalar.as_ref() {
        ScalarValue::Int(value) => *value,
        _ => panic!("expected int scalar"),
    }
}

fn get_str_at_root(doc: &automerge::Automerge, key: &str) -> String {
    let value = doc
        .get(automerge::ROOT, key)
        .expect("failed reading document")
        .expect("missing key");
    let automerge::Value::Scalar(scalar) = value.0 else {
        panic!("expected scalar value at root");
    };
    match scalar.as_ref() {
        ScalarValue::Str(value) => value.to_string(),
        _ => panic!("expected string scalar"),
    }
}

fn try_get_str_at_root(doc: &automerge::Automerge, key: &str) -> Option<String> {
    let (value, _) = doc
        .get(automerge::ROOT, key)
        .expect("failed reading document")?;
    let automerge::Value::Scalar(scalar) = value else {
        panic!("expected scalar value at root");
    };
    match scalar.as_ref() {
        ScalarValue::Str(value) => Some(value.to_string()),
        _ => panic!("expected string scalar"),
    }
}

async fn recv_change_batch(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
) -> Vec<BigRepoChangeNotification> {
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timed out waiting for change batch")
        .expect("change listener closed unexpectedly")
}

async fn recv_head_batch(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<super::changes::BigRepoHeadNotification>>,
) -> Vec<super::changes::BigRepoHeadNotification> {
    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timed out waiting for head batch")
        .expect("head listener closed unexpectedly")
}

async fn get_keyhive_agent(repo: &Arc<BigRepo>, peer_id: PeerKey) -> Res<Option<BigKeyhiveAgent>> {
    let kh_peer_id = KeyhivePeerId::from_bytes(peer_id.to_bytes32());
    repo.keyhive().get_agent_by_peer_id(&kh_peer_id).await
}

fn keyhive_document_id_for_big_repo_doc(
    doc_id: DocumentId,
) -> keyhive_core::principal::document::id::DocumentId {
    let doc_id_bytes = doc_id.to_bytes32();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
        .expect("doc id should be a valid keyhive document id");
    keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    )
}

async fn wait_for_document_access_notification(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<crate::changes::BigRepoDomainNotification>>,
    doc_id: DocumentId,
    member_id: PeerKey,
    expected_access: crate::changes::BigRepoAccess,
) -> Res<()> {
    timeout(utils_rs::scale_timeout(Duration::from_secs(10)), async {
        loop {
            let notifications = rx.recv().await.expect("domain listener must remain open");
            if notifications.iter().any(|notification| {
                matches!(
                    notification,
                    crate::changes::BigRepoDomainNotification::DocumentAccessChanged {
                        doc_id: candidate_doc,
                        member_id: candidate_member,
                        access,
                    } if *candidate_doc == doc_id
                        && *candidate_member == member_id
                        && *access == expected_access
                )
            }) {
                return;
            }
        }
    })
    .await
    .expect("timed out waiting for document access notification");
    Ok(())
}

#[tokio::test]
async fn put_doc_get_doc_and_export_roundtrip() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let fetched = repo.get_doc(&doc_id).await?.into_ready(doc_id.clone())?;
    assert_eq!(fetched.document_id(), doc_id);
    assert_eq!(
        fetched
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await,
        "seed"
    );
    let handle = repo.get_doc(&doc_id).await?.into_ready(doc_id.clone())?;
    assert!(!handle.export().await.is_empty());

    let stored_blobs = repo.inspect_stored_doc_blobs(doc_id).await?;
    assert!(
        !stored_blobs.is_empty(),
        "creating a doc should write encrypted blobs to subduction storage"
    );
    for raw in &stored_blobs {
        let encrypted = decode_encrypted_blob(raw.as_slice())?;
        assert_eq!(encrypted.content_ref.len(), 32);
        assert!(
            !raw.windows(b"seed".len()).any(|window| window == b"seed"),
            "plaintext staging bytes leaked into stored ciphertext"
        );
    }
    drop(handle);
    Ok(())
}

#[tokio::test]
async fn causal_coverage_deduplicates_per_epoch_and_rotates_at_unchanged_frontier() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "stable-frontier"))
        .expect("failed seeding doc");
    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let initial_count = repo.inspect_stored_doc_blobs(doc_id.clone()).await?.len();

    assert!(repo.runtime.ensure_causal_coverage(doc_id.clone()).await?);
    assert_eq!(
        repo.inspect_stored_doc_blobs(doc_id.clone()).await?.len(),
        initial_count,
        "the initial ordinary write already covers its current epoch"
    );

    let kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id.clone());
    let keyhive = repo.keyhive().clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .expect("created document must be present in Keyhive");
    let (update, local_secret) = keyhive.force_pcs_update(kh_doc).await?;
    crate::runtime2::support::persist_cgka_updates_durably(
        &repo.keyhive_protocol,
        &repo.keyhive_storage,
        vec![update],
        vec![local_secret],
    )
    .await?;

    assert!(repo.runtime.ensure_causal_coverage(doc_id.clone()).await?);
    let rotated_count = repo.inspect_stored_doc_blobs(doc_id.clone()).await?.len();
    assert_eq!(
        rotated_count,
        initial_count + 1,
        "a new epoch at the same Automerge frontier needs one checkpoint"
    );
    for _ in 0..3 {
        assert!(repo.runtime.ensure_causal_coverage(doc_id.clone()).await?);
    }
    assert_eq!(
        repo.inspect_stored_doc_blobs(doc_id).await?.len(),
        rotated_count,
        "replayed attempts in one epoch must not mint duplicate checkpoints"
    );
    Ok(())
}

#[tokio::test]
async fn startup_audit_repairs_update_persisted_without_checkpoint() -> Res<()> {
    let temp_root = tempdir()?;
    let repo_path = temp_root.path().join("checkpoint-crash-window");
    let (repo, _part_store, stop) = _boot_disk_repo(repo_path.clone()).await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "crash-window"))
        .expect("failed seeding doc");
    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let kh_doc = repo
        .keyhive()
        .clone_keyhive()
        .get_document(keyhive_document_id_for_big_repo_doc(doc_id.clone()))
        .await
        .expect("created document must be present in Keyhive");
    let (update, local_secret) = repo
        .keyhive()
        .clone_keyhive()
        .force_pcs_update(kh_doc)
        .await?;
    crate::runtime2::support::persist_cgka_updates_durably(
        &repo.keyhive_protocol,
        &repo.keyhive_storage,
        vec![update],
        vec![local_secret],
    )
    .await?;

    drop(handle);
    stop().await?;
    drop(repo);

    let (reopened, _part_store, reopened_stop) = _boot_disk_repo(repo_path).await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let repaired = reopened
                .inspect_stored_doc_blobs(doc_id.clone())
                .await?
                .into_iter()
                .filter_map(|raw| decode_encrypted_blob(&raw).ok())
                .filter_map(|encrypted| encrypted.content_ref.try_into().ok())
                .map(sedimentree_core::loose_commit::id::CommitId::new)
                .any(crate::runtime2::support::is_causal_checkpoint_id);
            if repaired {
                return Ok::<_, crate::eyre::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("startup audit did not repair missing checkpoint")?;
    reopened_stop().await?;
    Ok(())
}

#[tokio::test]
async fn local_boundary_commit_stores_fragment_and_prunes_covered_loose_history() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let mut stored_blob_count = repo.inspect_stored_doc_blobs(doc_id.clone()).await?.len();

    for attempt in 0..2_000_u32 {
        let heads = handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "boundary_probe", attempt))
                    .expect("failed writing boundary probe");
                doc.get_heads()
            })
            .await?;
        assert_eq!(heads.len(), 1, "boundary probe commits should stay linear");
        let head = heads[0];
        let next_stored_blob_count = repo.inspect_stored_doc_blobs(doc_id.clone()).await?.len();

        if head.0[0] == 0 {
            repo.wait_for_quiescence(None).await?;

            let sed_id = sedimentree_core::id::SedimentreeId::new(doc_id.to_bytes32());
            let head_id = sedimentree_core::loose_commit::id::CommitId::new(head.0);
            let fragments = <SqliteBigRepoStore as subduction_core::storage::traits::Storage<
                future_form::Sendable,
            >>::load_fragment_metas(&repo.sqlite_store, sed_id)
            .await?;
            assert!(
                fragments.iter().any(|fragment| fragment.head() == head_id),
                "boundary commit should persist its requested fragment"
            );
            assert!(
                <SqliteBigRepoStore as subduction_core::storage::traits::Storage<
                    future_form::Sendable,
                >>::load_loose_commit(&repo.sqlite_store, sed_id, head_id)
                .await?
                .is_none(),
                "the persisted fragment should durably absorb its covered loose head"
            );
            for raw in repo.inspect_stored_doc_blobs(doc_id).await? {
                decode_encrypted_blob(raw.as_slice())?;
            }
            return Ok(());
        }

        assert_eq!(
            next_stored_blob_count,
            stored_blob_count + 1,
            "non-boundary local commit should only add its loose commit"
        );
        stored_blob_count = next_stored_blob_count;
    }

    panic!("failed to find a boundary Automerge commit after 2000 attempts");
}

#[tokio::test]
async fn create_doc_records_initial_frontier_for_after_content() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let initial_head = initial_content_heads(&doc)?.head.to_vec();

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let doc_id_bytes = doc_id.to_bytes32();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
        .expect("doc id should be a valid keyhive document id");
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );

    let keyhive = repo.keyhive().clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .expect("document should exist in keyhive after create_doc");
    let public_individual = keyhive_core::principal::public::Public.individual();
    let public_agent = keyhive_core::principal::agent::Agent::Individual(
        public_individual.id(),
        Arc::new(Mutex::new(public_individual)),
    );
    let update = keyhive
        .add_member_with_manual_content(
            public_agent,
            &keyhive_core::principal::membered::Membered::Document(kh_doc_id, kh_doc),
            keyhive_core::access::Access::Read,
            std::collections::BTreeMap::from([(kh_doc_id, vec![initial_head.clone()])]),
        )
        .await
        .expect("granting read access should succeed");

    let after_content = update.delegation.payload().after().content[&kh_doc_id].as_slice();
    assert_eq!(after_content, &[initial_head]);

    drop(handle);
    Ok(())
}

#[tokio::test]
async fn write_records_latest_frontier_for_after_content() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let initial_head = initial_content_heads(&doc)?.head.to_vec();

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    handle
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(automerge::ROOT, "body", "updated")
                    .expect("failed writing doc");
                eyre::Ok(())
            })
            .expect("failed writing doc")
        })
        .await?;
    let latest_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;
    assert_ne!(
        latest_head, initial_head,
        "real write should advance the automerge head"
    );

    let doc_id_bytes = doc_id.to_bytes32();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
        .expect("doc id should be a valid keyhive document id");
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );

    let keyhive = repo.keyhive().clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .expect("document should exist in keyhive after create_doc");
    let public_individual = keyhive_core::principal::public::Public.individual();
    let public_agent = keyhive_core::principal::agent::Agent::Individual(
        public_individual.id(),
        Arc::new(Mutex::new(public_individual)),
    );
    let update = keyhive
        .add_member_with_manual_content(
            public_agent,
            &keyhive_core::principal::membered::Membered::Document(kh_doc_id, kh_doc),
            keyhive_core::access::Access::Read,
            std::collections::BTreeMap::from([(kh_doc_id, vec![latest_head.clone()])]),
        )
        .await
        .expect("granting read access should succeed");

    let after_content = update.delegation.payload().after().content[&kh_doc_id].as_slice();
    assert_eq!(after_content, &[latest_head]);

    drop(handle);
    Ok(())
}

#[tokio::test]
async fn create_doc_with_group_parent_uses_public_group_api() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 91, true).await?;
    let client = SyncRepoNode::boot(client_path, 92, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");

    let group = owner.repo.create_group_with_parents(vec![]).await?;
    owner
        .repo
        .add_member_to_group(
            client_agent.clone(),
            &group,
            keyhive_core::access::Access::Read,
        )
        .await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let handle = owner
        .repo
        .create_doc_with_parents(doc, vec![group.clone().into()])
        .await?;
    let doc_id = handle.document_id();

    owner_conn.sync_keyhive_with_peer().await?;
    client_conn.sync_keyhive_with_peer().await?;

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;
    let client_doc = wait_for_doc_handle(&client.repo, doc_id).await;
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "seed");

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

/// ADR 012 decision 6: big_repo's harnesses run the band its embedder ships, and the reason
/// this embedder was opted out was the offline-reopen path — the machine starts
/// post-reopen and did not complete, blocking `wait_for_full_sync`. Nothing covered it, so
/// this is the coverage: a pair reconciles on the bucket band, the client goes offline, the
/// owner writes while it is gone, and the reopened client must reconcile again.
#[tokio::test]
async fn bucket_band_reconciles_after_offline_reopen() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 211, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 212, false).await?;

    let group = owner.repo.create_group_with_parents(vec![]).await?;
    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;
    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");
    owner
        .repo
        .add_member_to_group(client_agent, &group, keyhive_core::access::Access::Read)
        .await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "before-reopen"))
        .expect("failed seeding doc");
    let handle = owner
        .repo
        .create_doc_with_parents(doc, vec![group.clone().into()])
        .await?;
    let first_doc = handle.document_id();
    // The doc's content lives in the group part it was created in; `o:{doc}` is only the
    // subscription view of it, materialized lazily by whoever subscribes (decision 3). The
    // full-sync barrier can only name a part the worker registers, and the harness registers the
    // synthetic one on both nodes -- a real part is advertised by its holder alone, which the
    // puller is about to receive -- so the doc is given that membership too, as the other
    // big_repo sync tests do. That makes the band fence below real work rather than an empty
    // part, and the explicit doc sync is what a doc's payload travels by; a completed part sync
    // does not imply its members' payloads were fetched (see the payload-can-trail-membership
    // contract).
    owner
        .big_sync_store
        .add_obj_to_parts(first_doc.clone(), stress_support::test_parts())
        .await?;
    drop(handle);

    owner_conn.sync_keyhive_with_peer().await?;
    client_conn.sync_keyhive_with_peer().await?;
    // The delegations reach each node's local Keyhive asynchronously through its hub, and the
    // group part is registered from that membership view, so fence the reconciliation before
    // asking the worker which parts the peer advertises (the fence the access-matrix tests use).
    owner.repo.wait_for_keyhive_reconciliation().await?;
    client.repo.wait_for_keyhive_reconciliation().await?;
    wait_for_pair_full_sync(&owner, &client).await?;
    client_conn.sync_doc_with_peer(first_doc.clone()).await?;
    let client_doc = wait_for_doc_handle(&client.repo, first_doc).await;
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(
        title, "before-reopen",
        "the bucket band must move data, not merely complete"
    );

    // Take the client offline and let the owner write while it is gone.
    drop(owner_conn);
    drop(client_conn);
    client.shutdown().await?;

    let mut second = automerge::Automerge::new();
    second
        .transact(|tx| tx.put(automerge::ROOT, "title", "created-offline"))
        .expect("failed seeding doc");
    let handle = owner
        .repo
        .create_doc_with_parents(second, vec![group.clone().into()])
        .await?;
    let second_doc = handle.document_id();
    owner
        .big_sync_store
        .add_obj_to_parts(second_doc.clone(), stress_support::test_parts())
        .await?;
    drop(handle);

    // Reopen the client and reconcile again: this is the path that used to stall.
    let client = SyncRepoNode::boot(client_path, 212, false).await?;
    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;
    owner_conn.sync_keyhive_with_peer().await?;
    // A reopened node starts with an empty membership view, so it must pull and reconcile
    // before the pair can complete a full sync.
    owner.repo.wait_for_keyhive_reconciliation().await?;
    client.repo.wait_for_keyhive_reconciliation().await?;
    wait_for_pair_full_sync(&owner, &client).await?;

    client_conn.sync_doc_with_peer(second_doc.clone()).await?;
    let reopened_doc = wait_for_doc_handle(&client.repo, second_doc).await;
    let title = reopened_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "created-offline");

    drop(owner_conn);
    drop(client_conn);
    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn allocate_and_finalize_pending_document_lifecycle() -> Res<()> {
    let temp_root = tempdir()?;
    let owner = SyncRepoNode::boot(temp_root.path().join("owner"), 191, true).await?;
    let pending = owner.repo.create_group_with_parents(vec![]).await?;
    let intended = owner.repo.create_group_with_parents(vec![]).await?;
    let doc_id = owner
        .repo
        .allocate_doc(vec![pending.clone().into(), intended.clone().into()])
        .await?;

    assert!(!owner.repo.contains_sedimentree_id(doc_id.clone()).await?);
    assert!(
        !owner
            .repo
            .keyhive()
            .document_has_content(doc_id.clone())
            .await?
    );
    // A reservation is not yet a Keyhive authority: no group contains it.
    assert!(
        !owner
            .repo
            .keyhive()
            .group_document_ids(&pending)
            .await
            .contains(&doc_id)
    );
    assert!(
        !owner
            .repo
            .keyhive()
            .group_document_ids(&intended)
            .await
            .contains(&doc_id)
    );

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "id", doc_id.to_string()))
        .expect("failed creating initial document commit");
    let retry_initial = initial.clone();
    owner
        .repo
        .finalize_allocated_doc(doc_id.clone(), initial, pending.clone())
        .await?;

    owner
        .repo
        .finalize_allocated_doc(doc_id.clone(), retry_initial, pending.clone())
        .await?;

    assert!(owner.repo.contains_sedimentree_id(doc_id.clone()).await?);
    assert!(
        owner
            .repo
            .keyhive()
            .document_has_content(doc_id.clone())
            .await?
    );
    assert!(
        !owner
            .repo
            .keyhive()
            .group_document_ids(&pending)
            .await
            .contains(&doc_id)
    );
    assert!(
        owner
            .repo
            .keyhive()
            .group_document_ids(&intended)
            .await
            .contains(&doc_id)
    );

    let mut mismatch = automerge::Automerge::new();
    mismatch
        .transact(|tx| tx.put(automerge::ROOT, "different", true))
        .expect("failed creating mismatched document content");
    let mismatch_error = owner
        .repo
        .finalize_allocated_doc(doc_id, mismatch, pending)
        .await
        .expect_err("different persisted content must be rejected");
    assert!(
        mismatch_error
            .to_string()
            .contains("initial content mismatch")
    );
    owner.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staged_document_reservation_recovers_after_repository_reopen() -> Res<()> {
    let temp_root = tempdir()?;
    let repo_path = temp_root.path().join("reserved-document-restart");
    let (repo, _part_store, stop) = _boot_disk_repo(repo_path.clone()).await?;
    let pending = repo.create_group_with_parents(vec![]).await?;
    let intended = repo.create_group_with_parents(vec![]).await?;
    let pending_id = pending.id().to_bytes();
    let intended_id = intended.id().to_bytes();
    let doc_id = repo
        .allocate_doc(vec![pending.clone().into(), intended.clone().into()])
        .await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "id", doc_id.to_string()))
        .expect("failed creating staged initial document");
    repo.keyhive
        .stage_reserved_doc(
            doc_id.clone(),
            initial.save(),
            Vec::new(),
            &repo.keyhive_storage,
        )
        .await?;
    stop().await?;
    drop(repo);

    let (reopened, _part_store, reopened_stop) = _boot_disk_repo(repo_path).await?;
    let reopened_pending = reopened
        .get_group_by_id(pending_id)
        .await
        .expect("pending group must survive restart");
    let reopened_intended = reopened
        .get_group_by_id(intended_id)
        .await
        .expect("intended group must survive restart");
    assert!(
        reopened
            .recover_allocated_doc(doc_id.clone(), reopened_pending.clone())
            .await?
    );
    assert!(reopened.contains_sedimentree_id(doc_id.clone()).await?);
    assert!(
        reopened
            .keyhive()
            .document_has_content(doc_id.clone())
            .await?
    );
    assert!(!reopened.reserved_doc_ids().await?.contains(&doc_id));
    assert!(
        !reopened
            .keyhive()
            .group_document_ids(&reopened_pending)
            .await
            .contains(&doc_id)
    );
    assert!(
        reopened
            .keyhive()
            .group_document_ids(&reopened_intended)
            .await
            .contains(&doc_id)
    );
    reopened_stop().await?;
    Ok(())
}

#[tokio::test]
async fn reserved_document_crash_windows_are_recoverable() -> Res<()> {
    let temp_root = tempdir()?;
    let owner = SyncRepoNode::boot(temp_root.path().join("owner"), 192, true).await?;
    let pending = owner.repo.create_group_with_parents(vec![]).await?;
    let intended = owner.repo.create_group_with_parents(vec![]).await?;

    // Crash window 1: reservation durable, no sedimentree, no Keyhive doc.
    let doc_id = owner
        .repo
        .allocate_doc(vec![pending.clone().into(), intended.clone().into()])
        .await?;
    assert!(
        owner.repo.reserved_doc_ids().await?.contains(&doc_id),
        "reservation must be durable immediately after allocation"
    );
    assert!(!owner.repo.contains_sedimentree_id(doc_id.clone()).await?);
    assert!(
        !owner
            .repo
            .keyhive()
            .document_has_content(doc_id.clone())
            .await?
    );

    // Finalizing an id that was never allocated must fail: no reservation and
    // no matching document.
    let never_allocated = DocumentId::new([0xEE; 32]);
    let mut phantom = automerge::Automerge::new();
    phantom
        .transact(|tx| tx.put(automerge::ROOT, "id", never_allocated.to_string()))
        .expect("failed creating phantom document");
    let phantom_error = owner
        .repo
        .finalize_allocated_doc(never_allocated, phantom, pending.clone())
        .await
        .expect_err("finalizing an unallocated id must fail");
    assert!(
        phantom_error.to_string().contains("no reservation"),
        "unexpected error: {phantom_error}"
    );

    // Crash window 2: finalize (Keyhive doc + sedimentree), then re-finalize
    // (idempotent recovery after reservation cleanup).
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "id", doc_id.to_string()))
        .expect("failed creating initial document commit");
    let retry_initial = initial.clone();
    owner
        .repo
        .finalize_allocated_doc(doc_id.clone(), initial, pending.clone())
        .await?;
    owner
        .repo
        .finalize_allocated_doc(doc_id.clone(), retry_initial, pending.clone())
        .await?;

    // Crash window 3: everything durable, reservation cleaned up.
    assert!(owner.repo.contains_sedimentree_id(doc_id.clone()).await?);
    assert!(
        owner
            .repo
            .keyhive()
            .document_has_content(doc_id.clone())
            .await?
    );
    assert!(
        !owner.repo.reserved_doc_ids().await?.contains(&doc_id),
        "reservation must be deleted after finalization"
    );
    assert!(
        !owner
            .repo
            .keyhive()
            .group_document_ids(&pending)
            .await
            .contains(&doc_id)
    );
    assert!(
        owner
            .repo
            .keyhive()
            .group_document_ids(&intended)
            .await
            .contains(&doc_id)
    );

    owner.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn ephemeral_roundtrip_between_two_nodes() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 95, true).await?;
    let client = SyncRepoNode::boot(client_path, 96, false).await?;

    let topic = BigEphemeralTopic::new([0xAB; 32]);
    let owner_eph_peer_id = subduction_core::peer::id::PeerId::new(owner.peer_id().to_bytes32());
    let mut subscription = client
        .repo
        .ephemeral()
        .subscribe(BigEphemeralFilter::new(topic).with_sender(owner_eph_peer_id))
        .await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let _owner_conn = owner.take_latest_accepted_connection().await;
    let _client_conn = client.connection_to(&owner).await;

    owner
        .repo
        .ephemeral()
        .publish(topic, b"hello-ephemeral".to_vec())
        .await?;

    let event = timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("timed out waiting for ephemeral event")
        .expect("subscription closed unexpectedly");
    assert_eq!(event.topic, topic);
    assert_eq!(event.sender, owner_eph_peer_id);
    assert_eq!(event.payload, b"hello-ephemeral".to_vec());

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn keyhive_contact_card_bootstrap_happens_on_connect_without_manual_sync() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 93, true).await?;
    let client = SyncRepoNode::boot(client_path, 94, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;
    assert!(
        get_keyhive_agent(&owner.repo, client.peer_id())
            .await?
            .is_some(),
        "owner should resolve the client as a keyhive agent after connect"
    );
    assert!(
        get_keyhive_agent(&client.repo, owner.peer_id())
            .await?
            .is_some(),
        "client should resolve the owner as a keyhive agent after connect"
    );

    drop(owner_conn);
    drop(client_conn);
    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn concurrent_bidirectional_keyhive_sync_is_safe() -> Res<()> {
    let temp_root = tempdir()?;
    let owner = SyncRepoNode::boot(temp_root.path().join("owner"), 97, true).await?;
    let client = SyncRepoNode::boot(temp_root.path().join("client"), 98, false).await?;
    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;
    let (owner_sync, client_sync) = tokio::join!(
        owner_conn.sync_keyhive_with_peer(),
        client_conn.sync_keyhive_with_peer(),
    );
    owner_sync?;
    client_sync?;
    assert!(
        get_keyhive_agent(&owner.repo, client.peer_id())
            .await?
            .is_some()
    );
    assert!(
        get_keyhive_agent(&client.repo, owner.peer_id())
            .await?
            .is_some()
    );
    drop(owner_conn);
    drop(client_conn);
    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn authorized_peer_reads_encrypted_doc_after_keyhive_change_notification_without_reboot()
-> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 95, true).await?;
    let client = SyncRepoNode::boot(client_path, 96, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;
    let client_agent = get_keyhive_agent(&owner.repo, client.peer_id())
        .await?
        .expect("client agent should be known after connection bootstrap");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    let (_access_registration, mut access_events) = client
        .repo
        .subscribe_domain_listener(BigRepoDomainFilter)
        .await?;
    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Read,
        )
        .await?;

    wait_for_document_access_notification(
        &mut access_events,
        doc_id.clone(),
        client.peer_id(),
        BigRepoAccess::Read,
    )
    .await?;

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    let client_doc = wait_for_doc_handle(&client.repo, doc_id.clone()).await;
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "seed");
    assert!(
        client
            .repo
            .doc_payload_heads(doc_id.clone())
            .await?
            .is_some(),
        "authorized client should have payload heads after RPC-triggered keyhive sync and doc sync"
    );

    let handle = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    assert!(!handle.export().await.is_empty());

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn grant_doc_access_writes_checkpoint_ancestor_for_pregrant_head() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 105, true).await?;
    let client = SyncRepoNode::boot(client_path, 106, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;

    owner_conn.sync_keyhive_with_peer().await?;
    let client_agent = get_keyhive_agent(&owner.repo, client.peer_id())
        .await?
        .expect("client agent should be known after connection bootstrap");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let pregrant_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;
    let pregrant_blobs = owner.repo.inspect_stored_doc_blobs(doc_id.clone()).await?;

    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Read,
        )
        .await?;

    let postgrant_automerge_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;
    assert_eq!(
        postgrant_automerge_head, pregrant_head,
        "a key-only checkpoint must not alter the Automerge frontier"
    );

    let postgrant_blobs = owner.repo.inspect_stored_doc_blobs(doc_id.clone()).await?;
    assert!(
        postgrant_blobs.len() > pregrant_blobs.len(),
        "reader grant should add a stored checkpoint blob"
    );

    let checkpoint_blob = postgrant_blobs
        .iter()
        .find_map(|raw| {
            let encrypted = decode_encrypted_blob(raw).ok()?;
            (encrypted.content_ref != pregrant_head).then_some(encrypted)
        })
        .expect("reader grant should add a key-only checkpoint after the pregrant head");

    let kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id);
    let keyhive = owner.repo.keyhive().clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .expect("owner keyhive doc should exist");
    let checkpoint_raw = {
        let mut locked = kh_doc.lock().await;
        let (raw, _checkpoint_key) = locked
            .try_decrypt_content_keyed(&checkpoint_blob)
            .expect("owner should decrypt post-grant checkpoint blob");
        raw
    };
    let checkpoint_envelope: keyhive_core::crypto::envelope::Envelope<Vec<u8>, Vec<u8>> =
        bincode::deserialize(&checkpoint_raw)
            .map_err(|e| ferr!("bincode decode checkpoint envelope: {e}"))?;
    assert!(
        checkpoint_envelope.ancestors.contains_key(&pregrant_head),
        "post-grant checkpoint should carry the pregrant head in its ancestors map"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn client_keyhive_decrypts_postwrite_blob_after_edit_grant_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 109, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 110, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;

    owner_conn.sync_keyhive_with_peer().await?;
    let client_agent = get_keyhive_agent(&owner.repo, client.peer_id())
        .await?
        .expect("client agent should be known after connection bootstrap");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let pregrant_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;

    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Edit,
        )
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    handle
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(automerge::ROOT, "body", "updated")
                    .expect("failed writing post-grant body");
                eyre::Ok(())
            })
            .expect("failed writing post-grant body");
        })
        .await?;
    let postwrite_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;
    assert_ne!(
        postwrite_head, pregrant_head,
        "edit grant should allow a real automerge write to advance the owner head"
    );

    owner_conn.sync_keyhive_with_peer().await?;

    let stored_blobs = owner.repo.inspect_stored_doc_blobs(doc_id.clone()).await?;
    let postwrite_blob = stored_blobs
        .iter()
        .find_map(|raw| {
            let encrypted = decode_encrypted_blob(raw).ok()?;
            (encrypted.content_ref == postwrite_head).then_some(encrypted)
        })
        .expect("post-write blob should be stored under the new owner head");

    let client_kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id);
    let client_keyhive = client.repo.keyhive().clone_keyhive();
    let client_kh_doc = client_keyhive
        .get_document(client_kh_doc_id)
        .await
        .expect("client keyhive doc should exist after explicit sync");
    {
        let mut locked = client_kh_doc.lock().await;
        locked
            .try_decrypt_content_keyed(&postwrite_blob)
            .expect("client should decrypt the post-write blob after edit-grant sync");
    }

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn client_keyhive_decrypts_postgrant_checkpoint_after_explicit_keyhive_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 107, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 108, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;

    owner_conn.sync_keyhive_with_peer().await?;
    let client_agent = get_keyhive_agent(&owner.repo, client.peer_id())
        .await?
        .expect("client agent should be known after connection bootstrap");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let pregrant_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;

    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Read,
        )
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    let postgrant_blobs = owner.repo.inspect_stored_doc_blobs(doc_id.clone()).await?;
    let mut checkpoint_blob = None;
    for raw in postgrant_blobs {
        let encrypted = decode_encrypted_blob(&raw)?;
        let owner_kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id.clone());
        let owner_kh_doc = owner
            .repo
            .keyhive()
            .clone_keyhive()
            .get_document(owner_kh_doc_id)
            .await
            .expect("owner keyhive doc should exist");
        let plaintext = owner_kh_doc
            .lock()
            .await
            .try_decrypt_content_keyed(&encrypted)
            .ok()
            .map(|(plaintext, _)| plaintext);
        let Some(plaintext) = plaintext else { continue };
        let envelope: keyhive_core::crypto::envelope::Envelope<Vec<u8>, Vec<u8>> =
            bincode::deserialize(&plaintext)?;
        if crate::runtime2::support::CausalCheckpoint::decode(&envelope.plaintext)?.is_some() {
            checkpoint_blob = Some(encrypted);
            break;
        }
    }
    let checkpoint_blob = checkpoint_blob.expect("post-grant causal checkpoint must be stored");

    let client_kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id);
    let client_keyhive = client.repo.keyhive().clone_keyhive();
    let client_kh_doc = client_keyhive
        .get_document(client_kh_doc_id)
        .await
        .expect("client keyhive doc should exist after explicit sync");
    let checkpoint_raw = {
        let mut locked = client_kh_doc.lock().await;
        let (raw, _checkpoint_key) = locked
            .try_decrypt_content_keyed(&checkpoint_blob)
            .expect("client should decrypt post-grant checkpoint blob after keyhive sync");
        raw
    };
    let checkpoint_envelope: keyhive_core::crypto::envelope::Envelope<Vec<u8>, Vec<u8>> =
        bincode::deserialize(&checkpoint_raw)
            .map_err(|e| ferr!("bincode decode checkpoint envelope: {e}"))?;
    assert!(
        checkpoint_envelope.ancestors.contains_key(&pregrant_head),
        "post-grant checkpoint should include the pregrant head in its ancestor map"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn disk_repo_round_trip_preserves_encrypted_doc_and_heads() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let repo_path = temp_root.path().join("repo");
    let (repo, _part_store, stop) = _boot_disk_repo(repo_path.clone()).await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "persisted"))
        .expect("failed seeding doc");
    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let export_before = repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?
        .export()
        .await;
    let heads_before = handle.with_document_read(|doc| doc.get_heads()).await;
    let title_before = handle
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title_before, "persisted");

    stop().await?;

    let (repo, _part_store, stop) = _boot_disk_repo(repo_path).await?;
    let fetched = repo.get_doc(&doc_id).await?.into_ready(doc_id.clone())?;
    let title_after = fetched
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title_after, "persisted");

    let export_after = repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?
        .export()
        .await;
    let heads_after = fetched.with_document_read(|doc| doc.get_heads()).await;

    assert_eq!(export_after, export_before);
    assert_eq!(heads_after, heads_before);

    stop().await?;
    Ok(())
}

#[tokio::test]
async fn closed_keyhive_connection_errors_cleanly_then_reconnects() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 97, true).await?;
    let client = SyncRepoNode::boot(client_path, 98, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;

    owner_conn.sync_keyhive_with_peer().await?;

    let closed_conn = owner_conn.clone();
    owner_conn.stop().await?;
    let err = closed_conn
        .sync_keyhive_with_peer()
        .await
        .expect_err("closed connection should fail keyhive sync");
    assert!(
        err.to_string().contains("connection is closed"),
        "closed connection should fail cleanly, got {err:?}"
    );

    let second_owner_path = temp_root.path().join("owner2");
    let second_client_path = temp_root.path().join("client2");
    let second_owner = SyncRepoNode::boot(second_owner_path, 99, true).await?;
    let second_client = SyncRepoNode::boot(second_client_path, 100, false).await?;

    second_client.connect_to(&second_owner).await?;
    second_owner.wait_for_accepts(1).await;
    let second_owner_conn = second_owner.take_latest_accepted_connection().await;

    timeout(Duration::from_secs(5), async {
        second_owner_conn.sync_keyhive_with_peer().await?;
        eyre::Ok(())
    })
    .await
    .expect("timed out waiting for keyhive sync on fresh peer pair")?;

    let second_client_kh_peer_id = KeyhivePeerId::from_bytes(second_client.peer_id().to_bytes32());
    assert!(
        second_owner
            .repo
            .keyhive()
            .get_agent_by_peer_id(&second_client_kh_peer_id)
            .await?
            .is_some(),
        "keyhive sync should still work on a fresh peer pair"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    second_owner.shutdown().await?;
    second_client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn minimal_doc_sync_loads_and_exports_after_keyhive_grant() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 103, true).await?;
    let client = SyncRepoNode::boot(client_path, 104, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| {
        tx.put(automerge::ROOT, "_", "")
            .expect("failed seeding minimal doc");
        eyre::Ok(())
    })
    .expect("failed creating minimal doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Read,
        )
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    let client_doc = wait_for_doc_handle(&client.repo, doc_id.clone()).await;
    let value = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "_"))
        .await;
    assert_eq!(value, "");
    assert!(
        client
            .repo
            .doc_payload_heads(doc_id.clone())
            .await?
            .is_some(),
        "client should have payload heads after minimal doc sync"
    );
    assert!(
        matches!(client.repo.get_doc(&doc_id).await?, DocLookup::Ready(_)),
        "client should export minimal doc plaintext after sync"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn group_member_reads_doc_while_non_member_stays_unauthorized() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let member_path = temp_root.path().join("member");
    let outsider_path = temp_root.path().join("outsider");
    let owner = SyncRepoNode::boot(owner_path, 105, true).await?;
    let member = SyncRepoNode::boot(member_path, 106, false).await?;
    let outsider = SyncRepoNode::boot(outsider_path, 107, false).await?;

    member.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_member_conn = owner.take_latest_accepted_connection().await;
    let member_conn = member.connection_to(&owner).await;
    outsider.connect_to(&owner).await?;
    owner.wait_for_accepts(2).await;
    let owner_outsider_conn = owner.take_latest_accepted_connection().await;
    let outsider_conn = outsider.connection_to(&owner).await;

    owner_member_conn.sync_keyhive_with_peer().await?;
    owner_outsider_conn.sync_keyhive_with_peer().await?;

    let member_kh_peer_id = KeyhivePeerId::from_bytes(member.peer_id().to_bytes32());
    let member_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&member_kh_peer_id)
        .await?
        .expect("member agent should be known after keyhive sync");

    let group = owner.repo.create_group_with_parents(vec![]).await?;
    owner
        .repo
        .add_member_to_group(
            member_agent.clone(),
            &group,
            keyhive_core::access::Access::Read,
        )
        .await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| {
        tx.put(automerge::ROOT, "title", "grouped")
            .expect("failed seeding grouped doc");
        eyre::Ok(())
    })
    .expect("failed creating grouped doc");
    let handle = owner
        .repo
        .create_doc_with_parents(doc, vec![group.clone().into()])
        .await?;
    let doc_id = handle.document_id();

    owner_member_conn.sync_keyhive_with_peer().await?;
    member_conn.sync_keyhive_with_peer().await?;

    member_conn.sync_doc_with_peer(doc_id.clone()).await?;
    let member_doc = wait_for_doc_handle(&member.repo, doc_id.clone()).await;
    assert_eq!(
        member_doc
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await,
        "grouped"
    );
    assert!(
        matches!(member.repo.get_doc(&doc_id).await?, DocLookup::Ready(_)),
        "group member should export plaintext after sync"
    );

    let outsider_sync = outsider_conn.sync_doc_with_peer(doc_id.clone()).await;
    match outsider_sync {
        Ok(()) => match outsider.repo.get_doc(&doc_id).await? {
            DocLookup::PendingMaterialization | DocLookup::Missing => {}
            DocLookup::Ready(_) => panic!("outsider should not materialize plaintext"),
        },
        Err(err) => {
            assert!(
                matches!(err, SyncDocError::Unauthorized | SyncDocError::Policy(_)),
                "outsider doc sync should return Unauthorized or Policy rejection, got {err:?}"
            );
        }
    }

    owner.shutdown().await?;
    member.shutdown().await?;
    outsider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_writers_with_edit_access_converge_after_bidirectional_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 108, true).await?;
    let client = SyncRepoNode::boot(client_path, 109, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| {
        tx.put(automerge::ROOT, "title", "base")
            .expect("failed seeding doc");
        eyre::Ok(())
    })
    .expect("failed creating doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Edit,
        )
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    let owner_doc = owner
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    let client_doc = client
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    set_doc_actor(&owner_doc, automerge::ActorId::from([108_u8; 16])).await?;
    set_doc_actor(&client_doc, automerge::ActorId::from([109_u8; 16])).await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(automerge::ROOT, "owner_note", "one")
                    .expect("failed owner mutation");
                eyre::Ok(())
            })
            .expect("failed owner mutation");
        })
        .await?;
    client_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(automerge::ROOT, "client_note", "two")
                    .expect("failed client mutation");
                eyre::Ok(())
            })
            .expect("failed client mutation");
        })
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    let (owner_sync, client_sync) = tokio::join!(
        owner_conn.sync_doc_with_peer(doc_id.clone()),
        client_conn.sync_doc_with_peer(doc_id.clone()),
    );
    owner_sync.expect("owner doc sync failed");
    client_sync.expect("client doc sync failed");

    let owner_doc = owner
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    assert_eq!(
        owner_doc
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await,
        "base"
    );
    assert_eq!(
        client_doc
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await,
        "base"
    );
    assert_eq!(
        owner_doc
            .with_document_read(|doc| get_str_at_root(doc, "owner_note"))
            .await,
        "one"
    );
    assert_eq!(
        client_doc
            .with_document_read(|doc| get_str_at_root(doc, "owner_note"))
            .await,
        "one"
    );
    assert_eq!(
        owner_doc
            .with_document_read(|doc| get_str_at_root(doc, "client_note"))
            .await,
        "two"
    );
    assert_eq!(
        client_doc
            .with_document_read(|doc| get_str_at_root(doc, "client_note"))
            .await,
        "two"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unauthorized_peer_does_not_materialize_plaintext_without_grant() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 95, true).await?;
    let client = SyncRepoNode::boot(client_path, 96, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "hidden"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    let sync_result = client_conn.sync_doc_with_peer(doc_id.clone()).await;
    match sync_result {
        Ok(()) => {
            assert!(
                client
                    .repo
                    .doc_payload_heads(doc_id.clone())
                    .await?
                    .is_some(),
                "client should at least have doc payload heads if sync completed"
            );
            match client.repo.get_doc(&doc_id).await? {
                DocLookup::PendingMaterialization | DocLookup::Missing => {}
                DocLookup::Ready(_) => {
                    panic!("unauthorized peer should not materialize plaintext")
                }
            }
            match client.repo.get_doc(&doc_id).await? {
                DocLookup::PendingMaterialization | DocLookup::Missing => {}
                DocLookup::Ready(_) => panic!("unauthorized peer should not export plaintext"),
            }
        }
        Err(err) => {
            assert!(
                matches!(err, SyncDocError::Unauthorized | SyncDocError::Policy(_)),
                "unauthorized doc sync should return Unauthorized or Policy rejection, got {err:?}"
            );
        }
    }

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn granted_doc_requires_manual_sync_after_keyhive_notification() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 101, true).await?;
    let client = SyncRepoNode::boot(client_path, 102, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after initial keyhive sync");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "pending"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let missing_doc_id = DocumentId::new([0x42; 32]);

    let (_access_registration, mut access_events) = client
        .repo
        .subscribe_domain_listener(BigRepoDomainFilter)
        .await?;
    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Read,
        )
        .await?;

    wait_for_document_access_notification(
        &mut access_events,
        doc_id.clone(),
        client.peer_id(),
        BigRepoAccess::Read,
    )
    .await?;

    // The grant is observable independently of payload discovery.
    assert!(
        client
            .repo
            .doc_payload_heads(doc_id.clone())
            .await?
            .is_none(),
        "doc should NOT have payload heads yet — no auto-sync has occurred"
    );

    // Since no automatic doc sync happens, the doc should be missing
    // (or pending materialization if the worker saw it but hasn't synced).
    match client.repo.get_doc(&doc_id).await? {
        DocLookup::Missing | DocLookup::PendingMaterialization => {}
        DocLookup::Ready(_) => {
            panic!("doc should not be materialized without explicit sync_doc_with_peer")
        }
    }

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    assert!(
        matches!(
            client.repo.get_doc(&missing_doc_id).await?,
            DocLookup::Missing
        ),
        "missing docs should still be reported as missing"
    );
    // The sync receipt fences the document transfer, not the Keyhive epoch
    // that makes the transferred ciphertext readable: a grant can mint a
    // fresh CGKA epoch (and the checkpoint that bridges the pre-grant head)
    // that reaches this node on an independent Keyhive round. Materialization
    // is therefore eventually consistent after the explicit sync — wait for
    // the documented outcome rather than assuming the receipt orders both.
    let doc = wait_for_doc_handle(&client.repo, doc_id).await;
    let title = doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "pending");
    assert!(
        !doc.export().await.is_empty(),
        "granted client should export after doc sync"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn synced_doc_auto_propagates_subsequent_edits() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 111, true).await?;
    let client = SyncRepoNode::boot(client_path, 112, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;
    let client_agent = get_keyhive_agent(&owner.repo, client.peer_id())
        .await?
        .expect("client agent should be known after bootstrap");

    // Owner creates doc + grants client read access.
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "first"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    let (_access_registration, mut access_events) = client
        .repo
        .subscribe_domain_listener(BigRepoDomainFilter)
        .await?;
    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Read,
        )
        .await?;

    wait_for_document_access_notification(
        &mut access_events,
        doc_id.clone(),
        client.peer_id(),
        BigRepoAccess::Read,
    )
    .await?;

    // Initial pull.
    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    let client_doc = wait_for_doc_handle(&client.repo, doc_id.clone()).await;
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "first", "initial content should match");

    // Owner edits the doc.
    let owner_doc = owner
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "second"))
                .expect("failed editing doc");
        })
        .await?;

    // Sync keyhive (gossip delivers the edit's CGKA ops) then re-pull.
    client_conn.sync_keyhive_with_peer().await?;
    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    let updated_title = loop {
        match client.repo.get_doc(&doc_id).await? {
            DocLookup::Ready(handle) => {
                let t = handle
                    .with_document_read(|doc| get_str_at_root(doc, "title"))
                    .await;
                if t == "second" {
                    break t;
                }
            }
            DocLookup::PendingMaterialization | DocLookup::Missing => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(updated_title, "second");

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn three_node_key_rotation_propagates_to_existing_reader() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let a_path = temp_root.path().join("a");
    let b_path = temp_root.path().join("b");
    let c_path = temp_root.path().join("c");
    let a = SyncRepoNode::boot(a_path, 201, true).await?;
    let b = SyncRepoNode::boot(b_path, 202, false).await?;
    let c = SyncRepoNode::boot(c_path, 203, false).await?;

    // Connect A↔B and A↔C.
    b.connect_to(&a).await?;
    a.wait_for_accepts(1).await;
    let a_b_conn = a.take_latest_accepted_connection().await;
    let b_a_conn = b.connection_to(&a).await;

    c.connect_to(&a).await?;
    a.wait_for_accepts(1).await;
    let a_c_conn = a.take_latest_accepted_connection().await;
    let c_a_conn = c.connection_to(&a).await;

    // Bootstrap keyhive for B and C with A.
    tracing::info!("THREE_NODE: before A↔B keyhive sync");
    a_b_conn.sync_keyhive_with_peer().await?;
    tracing::info!("THREE_NODE: A↔B sync done, before A↔C sync");
    a_c_conn.sync_keyhive_with_peer().await?;
    tracing::info!("THREE_NODE: both syncs done, looking up agents");

    let b_agent = get_keyhive_agent(&a.repo, b.peer_id())
        .await?
        .expect("B agent should be known after keyhive sync");
    let c_agent = get_keyhive_agent(&a.repo, c.peer_id())
        .await?
        .expect("C agent should be known after keyhive sync");

    // A creates a doc and grants B read access.
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "alpha"))
        .expect("failed seeding doc");
    let handle = a.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    let (_access_registration, mut access_events) = b
        .repo
        .subscribe_domain_listener(BigRepoDomainFilter)
        .await?;
    tracing::info!("THREE_NODE: granting B read access");
    a.repo
        .grant_doc_access(doc_id.clone(), b_agent, keyhive_core::access::Access::Read)
        .await?;

    tracing::info!("THREE_NODE: waiting for B's Keyhive access");
    wait_for_document_access_notification(
        &mut access_events,
        doc_id.clone(),
        b.peer_id(),
        BigRepoAccess::Read,
    )
    .await?;
    tracing::info!("THREE_NODE: B received Keyhive access, syncing doc");

    // B pulls the doc.
    b_a_conn.sync_doc_with_peer(doc_id.clone()).await?;
    tracing::info!("THREE_NODE: sync_doc_with_peer done, waiting for handle");
    let b_doc = wait_for_doc_handle(&b.repo, doc_id.clone()).await;
    tracing::info!("THREE_NODE: B got doc handle, reading title");
    let b_title = b_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    tracing::info!(?b_title, "THREE_NODE: B's doc title");
    assert_eq!(b_title, "alpha");

    // Now A grants C edit access — this rotates keys (CGKA op) and allows
    // C's later write to pass the storage access policy.
    a.repo
        .grant_doc_access(doc_id.clone(), c_agent, keyhive_core::access::Access::Edit)
        .await?;

    // Sync keyhive so C learns about the grant and B learns about the key
    // rotation (gossip propagates through the ephemeral notification path).
    a_c_conn.sync_keyhive_with_peer().await?;
    a_b_conn.sync_keyhive_with_peer().await?;

    // C pulls the doc.
    c_a_conn.sync_doc_with_peer(doc_id.clone()).await?;
    let c_title = loop {
        match c.repo.get_doc(&doc_id).await? {
            DocLookup::Ready(handle) => {
                let title = handle
                    .with_document_read(|doc| try_get_str_at_root(doc, "title"))
                    .await;
                if title.as_deref() == Some("alpha") {
                    break "alpha".to_owned();
                }
            }
            DocLookup::PendingMaterialization | DocLookup::Missing => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(c_title, "alpha");

    // A makes an edit.
    let a_handle = a.repo.get_doc(&doc_id).await?.into_ready(doc_id.clone())?;
    a_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "beta"))
                .expect("failed A edit");
        })
        .await?;

    // C makes an edit.
    let c_handle = c.repo.get_doc(&doc_id).await?.into_ready(doc_id.clone())?;
    c_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "author", "carol"))
                .expect("failed C edit");
        })
        .await?;

    // A must pull C's local edit before B can obtain it from A.
    a_c_conn.sync_keyhive_with_peer().await?;
    a_c_conn.sync_doc_with_peer(doc_id.clone()).await?;
    loop {
        match a.repo.get_doc(&doc_id).await? {
            DocLookup::Ready(handle) => {
                let author = handle
                    .with_document_read(|doc| try_get_str_at_root(doc, "author"))
                    .await;
                if author.as_deref() == Some("carol") {
                    break;
                }
            }
            DocLookup::PendingMaterialization | DocLookup::Missing => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // B re-syncs keyhive and re-pulls — the key rotation from adding C means
    // B needs the new CGKA ops to decrypt A and C's edits. This proves the
    // gossip (ephemeral notification → keyhive sync) delivered the keys.
    b_a_conn.sync_keyhive_with_peer().await?;
    b_a_conn.sync_doc_with_peer(doc_id.clone()).await?;

    // B can now decrypt both edits.
    timeout(Duration::from_secs(10), async {
        loop {
            match b.repo.get_doc(&doc_id).await? {
                DocLookup::Ready(handle) => {
                    let title = handle
                        .with_document_read(|doc| try_get_str_at_root(doc, "title"))
                        .await;
                    let author = handle
                        .with_document_read(|doc| try_get_str_at_root(doc, "author"))
                        .await;
                    if title.as_deref() == Some("beta") && author.as_deref() == Some("carol") {
                        return Ok::<_, eyre::Report>(());
                    }
                }
                DocLookup::PendingMaterialization | DocLookup::Missing => {}
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for B to decrypt edits from A and C after key rotation")?;

    a.shutdown().await?;
    b.shutdown().await?;
    c.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn grant_doc_access_checkpoint_becomes_visible_after_reopen_and_keyhive_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 131, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 132, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");

    let group = owner.repo.create_group_with_parents(vec![]).await?;
    owner
        .repo
        .add_member_to_group(
            client_agent.clone(),
            &group,
            keyhive_core::access::Access::Read,
        )
        .await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "pregrant"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            group.clone(),
            keyhive_core::access::Access::Read,
        )
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;

    let client_kh_before_shutdown = client.repo.keyhive().clone_keyhive();
    let client_kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
                .expect("doc id should be keyhive verifying key"),
        ),
    );
    let client_ops_before_shutdown = client_kh_before_shutdown
        .cgka_ops_for_doc(&client_kh_doc_id)
        .await
        .expect("client cgka ops lookup should not fail");
    assert!(
        client_ops_before_shutdown
            .as_ref()
            .is_some_and(|ops| !ops.is_empty()),
        "client should have synced CGKA ops before shutdown"
    );

    client.shutdown().await?;

    let client = SyncRepoNode::boot(client_path.clone(), 132, false).await?;
    client.connect_to(&owner).await?;
    owner.wait_for_accepts(2).await;
    let owner_conn = owner.take_latest_accepted_connection().await;

    owner_conn.sync_keyhive_with_peer().await?;

    assert!(
        matches!(client.repo.get_doc(&doc_id).await?, DocLookup::Ready(_)),
        "reopened client should be able to export the doc after keyhive sync alone"
    );

    client.shutdown().await?;
    owner.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn grant_doc_access_checkpoint_survives_reopen_and_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 121, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 122, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer().await?;

    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");

    let group = owner.repo.create_group_with_parents(vec![]).await?;
    owner
        .repo
        .add_member_to_group(
            client_agent.clone(),
            &group,
            keyhive_core::access::Access::Read,
        )
        .await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "pregrant"))
        .expect("failed seeding doc");
    let pregrant_head = initial_content_heads(&doc)?.head.to_vec();
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    owner
        .repo
        .grant_doc_access(
            doc_id.clone(),
            group.clone(),
            keyhive_core::access::Access::Read,
        )
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;

    client_conn.sync_doc_with_peer(doc_id.clone()).await?;
    let client_doc = wait_for_doc_handle(&client.repo, doc_id.clone()).await;
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "pregrant");

    let pregrant_historic_title = client_doc
        .with_document_read(|doc| {
            doc.get_at(automerge::ROOT, "title", &[change_hash(&pregrant_head)])
                .expect("failed reading pregrant title at head")
                .map(|(value, _)| match value {
                    automerge::Value::Scalar(scalar) => match scalar.as_ref() {
                        ScalarValue::Str(value) => value.to_string(),
                        _ => panic!("expected string scalar at pregrant head"),
                    },
                    _ => panic!("expected scalar value at pregrant head"),
                })
        })
        .await;
    assert_eq!(
        pregrant_historic_title.as_deref(),
        Some("pregrant"),
        "checkpoint grant should preserve pregrant content across sync"
    );

    let client_keyhive_storage = crate::keyhive_storage::BigRepoKeyhiveStorage::fs(
        client.repo.sqlite_store(),
        client_path.join(crate::keyhive_storage::KEYHIVE_SUBDIR),
    )
    .await?;
    let stored_events = subduction_keyhive::load_events::<Vec<u8>, _, future_form::Sendable>(
        &client_keyhive_storage,
    )
    .await?;
    let stored_cgka_ops = stored_events
        .iter()
        .filter(|(_, event)| {
            matches!(
                event,
                keyhive_core::event::static_event::StaticEvent::CgkaOperation(_)
            )
        })
        .count();
    assert!(
        stored_cgka_ops > 0,
        "client keyhive storage should contain synced CGKA ops before shutdown"
    );

    client.shutdown().await?;

    let client = SyncRepoNode::boot(client_path.clone(), 122, false).await?;
    client.connect_to(&owner).await?;
    owner.wait_for_accepts(2).await;
    let owner_conn = owner.take_latest_accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;
    let owner_kh_peer_id = KeyhivePeerId::from_bytes(owner.peer_id().to_bytes32());
    let grantee_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    owner_conn.sync_keyhive_with_peer().await?;
    let reopened_kh = client.repo.keyhive().clone_keyhive();
    let doc_id_bytes = doc_id.to_bytes32();
    let reopened_kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
                .expect("doc id should remain a valid keyhive doc id"),
        ),
    );
    assert!(
        reopened_kh.get_document(reopened_kh_doc_id).await.is_some(),
        "reopened client should still know the granted keyhive document"
    );
    assert!(
        client
            .repo
            .keyhive()
            .get_agent_by_peer_id(&owner_kh_peer_id)
            .await?
            .is_some(),
        "reopened client should still know the owner agent after keyhive sync"
    );
    assert!(
        client
            .repo
            .keyhive()
            .get_agent_by_peer_id(&grantee_kh_peer_id)
            .await?
            .is_some(),
        "reopened client should still know its own agent after keyhive sync"
    );
    assert!(
        matches!(client.repo.get_doc(&doc_id).await?, DocLookup::Ready(_)),
        "reopened client should still be able to export the doc from storage before sync_doc"
    );
    // Reopened clients need their local big-sync membership restored explicitly;
    // that state is not persisted with the doc body itself.
    client
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    client_conn.sync_doc_with_peer(doc_id.clone()).await?;
    assert!(
        !client
            .big_sync_store
            .obj_parts(doc_id.clone())
            .await?
            .is_empty(),
        "reopened client should retain big_sync part registration for the doc"
    );
    assert!(
        client
            .repo
            .doc_payload_heads(doc_id.clone())
            .await?
            .is_some(),
        "sync_doc_with_peer should populate doc payload heads before materialization"
    );
    assert!(
        matches!(client.repo.get_doc(&doc_id).await?, DocLookup::Ready(_)),
        "reopened client should still be able to export the doc after sync_doc"
    );
    let reopened_doc = wait_for_doc_handle(&client.repo, doc_id).await;
    let reopened_title = reopened_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(reopened_title, "pregrant");

    let reopened_historic_title = reopened_doc
        .with_document_read(|doc| {
            doc.get_at(automerge::ROOT, "title", &[change_hash(&pregrant_head)])
                .expect("failed reading pregrant title at head after reopen")
                .map(|(value, _)| match value {
                    automerge::Value::Scalar(scalar) => match scalar.as_ref() {
                        ScalarValue::Str(value) => value.to_string(),
                        _ => panic!("expected string scalar at pregrant head"),
                    },
                    _ => panic!("expected scalar value at pregrant head"),
                })
        })
        .await;
    assert_eq!(
        reopened_historic_title.as_deref(),
        Some("pregrant"),
        "checkpoint grant should survive reopen/sync"
    );

    client.shutdown().await?;
    owner.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn with_document_roundtrip_rehydrates_from_storage() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "before"))
        .expect("failed initializing title");

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "after"))
                .expect("failed mutating doc");
        })
        .await?;
    drop(handle);

    let reloaded = repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    let title = reloaded
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "after");
    Ok(())
}

#[tokio::test]
async fn change_listener_doc_id_filter_only_receives_target_doc() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let first_handle = repo
        .create_doc({
            let mut d = automerge::Automerge::new();
            d.transact(|tx| {
                tx.put(automerge::ROOT, "_", "").unwrap();
                Ok::<_, automerge::AutomergeError>(())
            })
            .unwrap();
            d
        })
        .await?;
    let first_doc_id = first_handle.document_id();
    let second_handle = repo
        .create_doc({
            let mut d = automerge::Automerge::new();
            d.transact(|tx| {
                tx.put(automerge::ROOT, "_", "").unwrap();
                Ok::<_, automerge::AutomergeError>(())
            })
            .unwrap();
            d
        })
        .await?;

    let (_registration, mut rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(first_doc_id.clone())),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    first_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "first"))
                .expect("failed mutating first doc");
        })
        .await?;
    second_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "second"))
                .expect("failed mutating second doc");
        })
        .await?;

    let batch = recv_change_batch(&mut rx).await;
    assert!(!batch.is_empty());
    assert!(batch.iter().all(|item| match item {
        BigRepoChangeNotification::DocCreated { doc_id, .. }
        | BigRepoChangeNotification::DocImported { doc_id, .. }
        | BigRepoChangeNotification::DocChanged { doc_id, .. } => *doc_id == first_doc_id,
    }));
    Ok(())
}

#[tokio::test]
async fn change_listener_path_filter_matches_only_prefix() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let handle = repo
        .create_doc({
            let mut d = automerge::Automerge::new();
            d.transact(|tx| {
                tx.put(automerge::ROOT, "_", "").unwrap();
                Ok::<_, automerge::AutomergeError>(())
            })
            .unwrap();
            d
        })
        .await?;
    let doc_id = handle.document_id();

    handle
        .with_document(|doc| {
            doc.transact(|tx| {
                let profile = tx
                    .put_object(automerge::ROOT, "profile", automerge::ObjType::Map)
                    .expect("failed creating profile object");
                tx.put(&profile, "title", "seed")
                    .expect("failed seeding profile title");
                eyre::Ok(())
            })
            .expect("failed seeding nested profile");
        })
        .await?;

    let profile_obj = handle
        .with_document_read(|doc| {
            let Some((automerge::Value::Object(_), profile_obj)) = doc
                .get(automerge::ROOT, "profile")
                .expect("failed reading profile")
            else {
                panic!("expected profile object");
            };
            profile_obj
        })
        .await;

    let (_registration, mut rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
            origin: None,
            path: vec![Prop::Key("profile".into())],
        })
        .await?;

    handle
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(&profile_obj, "title", "one")
                    .expect("failed mutating profile title");
                eyre::Ok(())
            })
            .expect("failed mutating nested profile");
        })
        .await?;
    handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "body", "two"))
                .expect("failed mutating body");
        })
        .await?;

    let batch = recv_change_batch(&mut rx).await;
    assert_eq!(batch.len(), 1);
    let BigRepoChangeNotification::DocChanged {
        doc_id: seen_doc_id,
        patch,
        ..
    } = &batch[0]
    else {
        panic!("expected doc changed notification");
    };
    assert_eq!(*seen_doc_id, doc_id);
    assert!(big_repo_path_prefix_matches(
        &[Prop::Key("profile".into())],
        &patch.path[..]
    ));
    Ok(())
}

#[tokio::test]
async fn change_listener_origin_filter_works_for_local_events() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let (_registration, mut rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: None,
            origin: Some(BigRepoOriginFilter::Local),
            path: Vec::new(),
        })
        .await?;

    let handle = repo
        .create_doc({
            let mut d = automerge::Automerge::new();
            d.transact(|tx| {
                tx.put(automerge::ROOT, "_", "").unwrap();
                Ok::<_, automerge::AutomergeError>(())
            })
            .unwrap();
            d
        })
        .await?;
    let doc_id = handle.document_id();

    let batch = recv_change_batch(&mut rx).await;
    assert!(batch.iter().any(|item| matches!(
        item,
        BigRepoChangeNotification::DocCreated {
            doc_id: seen_doc_id,
            ..
        } | BigRepoChangeNotification::DocImported {
            doc_id: seen_doc_id,
            ..
        } if *seen_doc_id == doc_id
    )));
    Ok(())
}

#[tokio::test]
async fn change_and_head_listeners_ignore_noop_mutation() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let handle = repo
        .create_doc({
            let mut d = automerge::Automerge::new();
            d.transact(|tx| {
                tx.put(automerge::ROOT, "_", "").unwrap();
                Ok::<_, automerge::AutomergeError>(())
            })
            .unwrap();
            d
        })
        .await?;
    let doc_id = handle.document_id();

    let (_change_registration, mut change_rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
            origin: Some(BigRepoOriginFilter::Local),
            path: Vec::new(),
        })
        .await?;
    let (_head_registration, mut head_rx) = repo
        .change_manager
        .subscribe_head_listener(super::changes::HeadFilter {
            doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
        })
        .await?;

    handle
        .with_document(|_| {
            // No-op on purpose.
        })
        .await?;

    assert!(
        timeout(Duration::from_millis(250), change_rx.recv())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(250), head_rx.recv())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn remote_change_and_head_notifications_survive_handle_reopen() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding title");

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    drop(handle);
    let handle = repo.get_doc(&doc_id).await?.into_ready(doc_id.clone())?;

    let (_change_registration, mut change_rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
            origin: Some(BigRepoOriginFilter::Remote),
            path: Vec::new(),
        })
        .await?;
    let (_head_registration, mut head_rx) = repo
        .change_manager
        .subscribe_head_listener(super::changes::HeadFilter {
            doc_id: Some(super::changes::DocIdFilter::new(doc_id.clone())),
        })
        .await?;

    handle
        .with_document_with_origin(
            |doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "remote-after"))
                    .expect("failed mutating remote doc");
            },
            BigRepoChangeOrigin::Remote {
                peer_id: PeerKey::new([9_u8; 32]),
            },
        )
        .await?;

    let change_batch = recv_change_batch(&mut change_rx).await;
    assert!(matches!(
        change_batch.as_slice(),
        [BigRepoChangeNotification::DocChanged {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Remote { .. },
            ..
        }] if *seen_doc_id == doc_id
    ));

    let head_batch: Vec<super::changes::BigRepoHeadNotification> =
        recv_head_batch(&mut head_rx).await;
    assert!(matches!(
        head_batch.as_slice(),
        [super::changes::BigRepoHeadNotification::SedimentreeHeadsChanged {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Remote { .. },
            ..
        }] if *seen_doc_id == doc_id
    ));

    let title = repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "remote-after");
    Ok(())
}

#[tokio::test]
async fn with_document_handles_concurrent_writers() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let handle = repo
        .create_doc({
            let mut d = automerge::Automerge::new();
            d.transact(|tx| {
                tx.put(automerge::ROOT, "_", "").unwrap();
                Ok::<_, automerge::AutomergeError>(())
            })
            .unwrap();
            d
        })
        .await?;
    let doc_id = handle.document_id();
    handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "count", 0))
                .expect("failed initializing count");
        })
        .await?;

    let writer_count = 8_u64;
    let increments_per_writer = 25_u64;
    let mut joins = Vec::new();
    for _ in 0..writer_count {
        let repo = Arc::clone(&repo);
        let doc_id = doc_id.clone();
        joins.push(tokio::spawn(async move {
            let handle = match repo.get_doc(&doc_id).await {
                Ok(DocLookup::Ready(handle)) => handle,
                Ok(DocLookup::PendingMaterialization) => {
                    panic!("doc should be ready for concurrent writers")
                }
                Ok(DocLookup::Missing) => panic!("doc should exist for concurrent writers"),
                Err(err) => panic!("failed finding doc: {err}"),
            };
            for _ in 0..increments_per_writer {
                handle
                    .with_document(|doc| {
                        doc.transact(|tx| {
                            let current = tx
                                .get(automerge::ROOT, "count")
                                .expect("failed reading count")
                                .map(|(value, _)| match value {
                                    automerge::Value::Scalar(scalar) => match scalar.as_ref() {
                                        ScalarValue::Int(value) => *value,
                                        _ => panic!("unexpected scalar for count"),
                                    },
                                    _ => panic!("unexpected value type for count"),
                                })
                                .unwrap_or(0);
                            tx.put(automerge::ROOT, "count", current + 1)
                        })
                        .expect("failed incrementing count");
                    })
                    .await
                    .expect("with_document failed");
            }
        }));
    }
    for join in joins {
        join.await.expect("writer task panicked");
    }

    let final_count = repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?
        .with_document_read(|doc| get_int_at_root(doc, "count"))
        .await;
    assert_eq!(final_count, (writer_count * increments_per_writer) as i64);
    Ok(())
}

const SYNC_DOC_ITEMS: usize = 32;
const SYNC_DOC_PAYLOAD_LEN: usize = 384;
const SYNC_LARGE_DOC_ITEMS: usize = 1000;
const SYNC_LARGE_DOC_PAYLOAD_LEN: usize = 1024;
const SYNC_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(10);
const SYNC_CASE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
struct SyncMutation {
    item_idx: usize,
    note_key: &'static str,
    side_label: &'static str,
}

fn make_sync_doc_value(title: &str, item_count: usize, payload_len: usize) -> serde_json::Value {
    let payload = "v".repeat(payload_len.max(1));
    make_sync_doc_value_with_payload(title, item_count, &payload)
}

fn make_sync_doc_value_with_payload(
    title: &str,
    item_count: usize,
    payload: &str,
) -> serde_json::Value {
    serde_json::json!({
        "title": title,
        "items": (0..item_count)
            .map(|idx| serde_json::json!({
                "value": format!("{title}-{idx}-{payload}"),
                "note": ""
            }))
            .collect::<Vec<_>>(),
    })
}

fn apply_sync_mutation(doc: &mut serde_json::Value, mutation: SyncMutation, payload_len: usize) {
    let items = doc
        .get_mut("items")
        .and_then(serde_json::Value::as_array_mut)
        .expect("sync doc should contain an items array");
    let item = items
        .get_mut(mutation.item_idx)
        .and_then(serde_json::Value::as_object_mut)
        .expect("sync mutation item index should exist");
    item.insert(
        "note".into(),
        serde_json::Value::String(format!(
            "{}:{}:{}",
            mutation.side_label,
            mutation.note_key,
            "n".repeat(payload_len.max(1))
        )),
    );
}

fn sync_item_note(doc: &serde_json::Value, item_idx: usize) -> &str {
    doc.get("items")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.get(item_idx))
        .and_then(serde_json::Value::as_object)
        .and_then(|item| item.get("note"))
        .and_then(serde_json::Value::as_str)
        .expect("sync doc item note should exist")
}

fn sync_note_snapshot(doc: &serde_json::Value, item_indices: &[usize]) -> Vec<(usize, String)> {
    item_indices
        .iter()
        .copied()
        .map(|item_idx| (item_idx, sync_item_note(doc, item_idx).to_string()))
        .collect()
}

fn apply_sync_mutation_in_place(
    doc: &mut automerge::Automerge,
    mutation: SyncMutation,
    payload_len: usize,
) {
    let note = format!(
        "{}:{}:{}",
        mutation.side_label,
        mutation.note_key,
        "n".repeat(payload_len.max(1))
    );
    let items_obj = doc
        .get(automerge::ROOT, "items")
        .expect("failed reading sync items list")
        .expect("sync doc should contain an items list")
        .1;
    let item_obj = doc
        .get(&items_obj, mutation.item_idx)
        .expect("failed reading sync item")
        .expect("sync mutation item index should exist")
        .1;
    doc.transact(|tx| {
        tx.put(&item_obj, "note", note.as_str())
            .expect("failed writing sync item note");
        eyre::Ok(())
    })
    .expect("failed applying sync mutation in place");
}

fn write_sync_doc_value(doc: &mut automerge::Automerge, value: &serde_json::Value) {
    let title = value
        .get("title")
        .and_then(serde_json::Value::as_str)
        .expect("sync doc should contain a title");
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .expect("sync doc should contain an items array");
    let has_placeholder = doc
        .get(automerge::ROOT, "_")
        .expect("failed reading sync placeholder")
        .is_some();
    doc.transact(|tx| {
        if has_placeholder {
            tx.delete(automerge::ROOT, "_")
                .expect("failed deleting sync placeholder");
        }
        tx.put(automerge::ROOT, "title", title)
            .expect("failed writing sync title");
        let items_obj = tx
            .put_object(automerge::ROOT, "items", automerge::ObjType::List)
            .expect("failed creating sync items list");
        for item in items.iter().rev() {
            let item_obj = tx
                .insert_object(&items_obj, 0, automerge::ObjType::Map)
                .expect("failed inserting sync item");
            let item_value = item
                .get("value")
                .and_then(serde_json::Value::as_str)
                .expect("sync item should contain a string value");
            let item_note = item
                .get("note")
                .and_then(serde_json::Value::as_str)
                .expect("sync item should contain a string note");
            tx.put(&item_obj, "value", item_value)
                .expect("failed writing sync item value");
            tx.put(&item_obj, "note", item_note)
                .expect("failed writing sync item note");
        }
        eyre::Ok(())
    })
    .expect("failed writing sync doc");
}

fn initial_content_heads(doc: &automerge::Automerge) -> Res<NonEmpty<[u8; 32]>> {
    NonEmpty::from_vec(doc.get_heads().into_iter().map(|head| head.0).collect())
        .ok_or_else(|| ferr!("automerge doc has no heads"))
}

fn change_hash(bytes: &[u8]) -> automerge::ChangeHash {
    automerge::ChangeHash(bytes.try_into().expect("expected 32-byte change hash"))
}

fn new_sync_doc(actor: automerge::ActorId, value: &serde_json::Value) -> automerge::Automerge {
    let mut doc = automerge::Automerge::new();
    doc.set_actor(actor);
    write_sync_doc_value(&mut doc, value);
    doc
}

fn sync_test_part() -> PartKey {
    PartKey(ByteKey::new([
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
    ]))
}

fn sync_test_parts() -> Vec<PartKey> {
    vec![sync_test_part()]
}

fn sync_test_parts_multi() -> Vec<PartKey> {
    vec![sync_test_part(), PartKey(ByteKey::new([7; 32]))]
}

struct BigRepoSyncBackendContractHarness {
    backend: Arc<dyn SyncBackend>,
    store: Arc<dyn HostPartStore>,
}

#[async_trait::async_trait]
impl SyncBackendHarness for BigRepoSyncBackendContractHarness {
    fn backend(&self) -> &dyn SyncBackend {
        self.backend.as_ref()
    }

    fn store(&self) -> &dyn HostPartStore {
        self.store.as_ref()
    }
}

#[tracing::instrument(skip_all, fields(doc_id = %handle.document_id()))]
async fn read_json_doc(handle: &BigDocHandle) -> serde_json::Value {
    handle
        .with_document(|doc| {
            autosurgeon::hydrate::<_, ThroughJson<serde_json::Value>>(doc)
                .expect("failed hydrating sync doc")
                .0
        })
        .await
        .expect("sync doc should always hydrate as json")
}

#[tracing::instrument(skip_all, fields(doc_id = %handle.document_id(), timeout_ms = timeout_dur.as_millis() as u64))]
async fn wait_for_json_doc(
    handle: &BigDocHandle,
    expected: &serde_json::Value,
    timeout_dur: Duration,
) {
    let mut last_actual = None;
    let res = timeout(timeout_dur, async {
        loop {
            let actual = read_json_doc(handle).await;
            if actual == *expected {
                break;
            }
            last_actual = Some(actual);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if res.is_err() {
        panic!(
            "timed out waiting for JSON document to converge\nexpected: {}\nactual: {}",
            serde_json::to_string_pretty(expected).expect("json serializes"),
            serde_json::to_string_pretty(&last_actual).expect("json serializes"),
        );
    }
}

async fn wait_for_doc_handle(repo: &Arc<BigRepo>, doc_id: DocumentId) -> BigDocHandle {
    match timeout(SYNC_CASE_TIMEOUT, async {
        loop {
            match repo.get_doc(&doc_id).await? {
                DocLookup::Ready(handle) => return Ok::<BigDocHandle, eyre::Report>(handle),
                DocLookup::PendingMaterialization | DocLookup::Missing => {}
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    {
        Ok(result) => result.expect("doc lookup failed"),
        Err(err) => {
            let export_doc = repo.get_doc(&doc_id).await.unwrap_or(DocLookup::Missing);
            let payload_heads = repo.doc_payload_heads(doc_id.clone()).await.unwrap_or(None);
            let parts = repo
                .big_sync_store
                .obj_parts(doc_id)
                .await
                .unwrap_or_default();
            panic!(
                "timed out waiting for doc to materialize: {err:?}; export_doc_present={}; payload_heads_present={}; parts_len={}",
                matches!(export_doc, DocLookup::Ready(_)),
                payload_heads.is_some(),
                parts.len(),
            );
        }
    }
}

async fn create_shared_sync_doc(
    owner: &SyncRepoNode,
    grantee: &SyncRepoNode,
    owner_conn: &BigRepoConnection,
    grantee_conn: &BigRepoConnection,
    value: &serde_json::Value,
    owner_actor: automerge::ActorId,
) -> Res<BigDocHandle> {
    owner_conn.sync_keyhive_with_peer().await?;

    let doc = new_sync_doc(owner_actor, value);
    let grantee_kh_peer_id = KeyhivePeerId::from_bytes(grantee.peer_id().to_bytes32());
    let grantee_agent = owner
        .repo
        .keyhive()
        .get_agent_by_peer_id(&grantee_kh_peer_id)
        .await?
        .expect("grantee agent should be known after keyhive sync");
    let handle = owner
        .repo
        .create_doc_with_parents(doc, vec![grantee_agent.into()])
        .await?;

    owner_conn.sync_keyhive_with_peer().await?;
    grantee_conn.sync_keyhive_with_peer().await?;

    // Bootstrap the doc on the grantee so the fetch gate
    // (has_doc_worker || contains_sedimentree) passes for
    // subsequent sync scenarios.
    let doc_id = handle.document_id();
    // The runtime listener registers the doc in global_part_id() on the
    // grantee when the delegation arrives via ephemeral notification.
    // If the grantee restarted and the listener isn't active, the caller
    // is responsible for restoring partition membership.
    // The delegations above reach each node's local Keyhive asynchronously
    // through its hub. Both the doc sync below and callers that write to the
    // document right after this helper would otherwise race their own
    // membership view: the sync is rejected as content whose author's edit
    // access is not visible yet, and the write as read-only. Fence the
    // reconciliation before doing either.
    owner.repo.wait_for_keyhive_reconciliation().await?;
    grantee.repo.wait_for_keyhive_reconciliation().await?;
    grantee_conn.sync_doc_with_peer(doc_id.clone()).await?;
    // The grant is announced to the grantee over an ephemeral Keyhive
    // notification, which is not admitted work: the reconciliation fence above
    // cannot see it. Wait until the grantee actually observes access, so
    // callers get a document they can both sync and write.
    let grantee_local = keyhive_core::principal::identifier::Identifier::from(
        ed25519_dalek::VerifyingKey::from_bytes(&grantee.peer_id().to_bytes32())
            .map_err(|_| crate::ferr!("grantee peer id is not a verifying key"))?,
    );
    let doc_ident = keyhive_core::principal::identifier::Identifier::from(
        ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
            .map_err(|_| crate::ferr!("doc id is not a verifying key"))?,
    );
    // Bounded only for a fast, attributed failure: the grant is normally
    // observable within milliseconds of the sync round.
    let access_deadline =
        tokio::time::Instant::now() + utils_rs::scale_timeout(std::time::Duration::from_secs(5));
    while grantee
        .repo
        .keyhive()
        .agent_access_on(&grantee_local, doc_ident)
        .await
        .is_none()
    {
        if tokio::time::Instant::now() >= access_deadline {
            // Name what the grantee's Keyhive actually knows: whether the
            // document ever reached it at all, and what the owner believed it
            // granted. Without this the only signal is the missing access, and
            // the interesting split (document absent vs. delegation not
            // applied) is lost.
            let known_docs = grantee.repo.keyhive().document_ids().await;
            let doc_known = known_docs.contains(&big_sync_core::ObjKey::new(doc_id.as_bytes()));
            let grantee_docs = grantee.repo.keyhive().docs_for_agent(&grantee_local).await;
            let owner_local = keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(&owner.peer_id().to_bytes32())
                    .map_err(|_| crate::ferr!("owner peer id is not a verifying key"))?,
            );
            let owner_access = owner
                .repo
                .keyhive()
                .agent_access_on(&owner_local, doc_ident)
                .await;
            // The ledgers answer the next question: an empty unapplied remainder
            // on the grantee means the granting events never reached it, while a
            // non-empty one names the peer whose events Keyhive never applied.
            let grantee_ledger = crate::test2::describe_ledger(&grantee.repo).await?;
            let owner_ledger = crate::test2::describe_ledger(&owner.repo).await?;
            let grantee_membered = grantee
                .repo
                .keyhive()
                .membered_for_agent(&grantee_local)
                .await;
            return Err(crate::ferr!(
                "grantee never observed access to the document it was granted: \
                 doc_known_to_grantee={doc_known} grantee_doc_count={} \
                 grantee_docs_for_agent={} grantee_membered={} owner_access={owner_access:?} \
                 grantee_ledger={grantee_ledger} owner_ledger={owner_ledger}",
                known_docs.len(),
                grantee_docs.len(),
                grantee_membered.len()
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    Ok(handle)
}

#[derive(Clone, Debug)]
struct SubductionProtocolHandler {
    repo: Arc<BigRepo>,
    endpoint: iroh::Endpoint,
    track_accepts: bool,
    accept_count: Arc<AtomicUsize>,
    accept_notify: Arc<Notify>,
    accepted_connection: Arc<tokio::sync::Mutex<Option<BigRepoConnection>>>,
}

impl iroh::protocol::ProtocolHandler for SubductionProtocolHandler {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(conn, self.endpoint.clone(), None)
            .await
            .map_err(|err| iroh::protocol::AcceptError::from_boxed(err.into()))?;
        if self.track_accepts {
            *self.accepted_connection.lock().await = Some(connection.clone());
            self.accept_count.fetch_add(1, Ordering::SeqCst);
            self.accept_notify.notify_waiters();
        }
        Ok(())
    }
}

pub(crate) type StressReplaySubscriptions = HashMap<
    (
        big_sync_core::rpc::ReplaySessionId,
        big_sync_core::rpc::ReplaySubscriptionId,
    ),
    (
        u64,
        HashMap<big_sync_core::rpc::ReplayTargetId, big_sync_core::rpc::ReplaySubscriptionTarget>,
    ),
>;

pub(crate) struct StressBigSyncRpcClient {
    pub(crate) target_part_store: SharedPartStore,
    pub(crate) subscriber: PeerKey,
    pub(crate) replay_subscriptions: Arc<Mutex<StressReplaySubscriptions>>,
}

#[async_trait::async_trait]
impl big_sync::rpc::WireBigSyncRpcClient for StressBigSyncRpcClient {
    async fn peer_summary(
        &self,
        req: big_sync::rpc::ScopedRequest<big_sync_core::rpc::PeerSummaryRequest>,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<big_sync_core::rpc::PeerSummaryResult, big_sync_core::rpc::ListPartsError>,
        >,
    > {
        let summarized = self
            .target_part_store
            .summarize_parts(req.inner.parts)
            .await?;
        // A part-level failure is a response error, not a transport one.
        let summarized = match summarized {
            Ok(parts) => parts,
            Err(err) => return Ok(Ok(Err(err))),
        };
        let asker = Some(self.subscriber.clone());
        let mut summaries = HashMap::new();
        for (part_id, summary) in summarized {
            let since = req
                .inner
                .asker_part_cursors
                .get(&part_id)
                .copied()
                .unwrap_or(0);
            let dirty = self
                .target_part_store
                .part_dirty_count(part_id.clone(), asker.clone(), since)
                .await?;
            summaries.insert(part_id, summary.into_strat_summaries(dirty));
        }
        Ok(Ok(Ok(big_sync_core::rpc::PeerSummaryResult {
            parts: summaries,
        })))
    }

    async fn replay_page(
        &self,
        req: big_sync::rpc::ScopedRequest<big_sync_core::rpc::ReplayPageRequest>,
    ) -> Res<big_sync_core::rpc::BigSyncRpcResult<big_sync_core::rpc::ReplayPage>> {
        // The double caps the caller's hold so a caught-up page answers promptly
        // rather than parking on the long production poll.
        let hold =
            Duration::from_millis(u64::from(req.inner.hold_ms)).min(Duration::from_millis(50));
        Ok(Ok(self
            .target_part_store
            .replay_page_round(
                req.inner,
                self.subscriber.clone(),
                hold,
                tokio_util::sync::CancellationToken::new(),
            )
            .await?))
    }

    async fn replay_subscription(
        &self,
        req: big_sync::rpc::ScopedRequest<big_sync_core::rpc::ReplaySubscriptionRequest>,
    ) -> Res<big_sync_core::rpc::BigSyncRpcResult<big_sync_core::rpc::ReplaySubscriptionResponse>>
    {
        use big_sync_core::rpc::{
            ReplayPage, ReplayPageRequest, ReplaySubscriptionPage, ReplaySubscriptionRequest,
            ReplaySubscriptionResponse, RpcError,
        };
        let hold =
            |hold_ms| Duration::from_millis(u64::from(hold_ms)).min(Duration::from_millis(50));
        match req.inner {
            ReplaySubscriptionRequest::Open {
                session_id,
                subscription_id,
                generation,
                targets,
            } => {
                let mut target_map = HashMap::new();
                for entry in targets {
                    if target_map.insert(entry.id, entry.target).is_some() {
                        return Ok(Err(RpcError::InvalidRequest(
                            "duplicate replay target id".into(),
                        )));
                    }
                }
                self.replay_subscriptions
                    .lock()
                    .await
                    .insert((session_id, subscription_id), (generation, target_map));
                Ok(Ok(ReplaySubscriptionResponse::Opened { generation }))
            }
            ReplaySubscriptionRequest::Update {
                session_id,
                subscription_id,
                generation,
                additions,
                removals,
            } => {
                let mut subscriptions = self.replay_subscriptions.lock().await;
                let Some((current_generation, target_map)) =
                    subscriptions.get_mut(&(session_id, subscription_id))
                else {
                    return Ok(Err(RpcError::UnknownSubscription));
                };
                if generation == *current_generation {
                    return Ok(Ok(ReplaySubscriptionResponse::Updated { generation }));
                }
                if generation != current_generation.saturating_add(1) {
                    return Ok(Err(RpcError::StaleSubscriptionGeneration));
                }
                for target_id in removals {
                    target_map.remove(&target_id);
                }
                for entry in additions {
                    if target_map.insert(entry.id, entry.target).is_some() {
                        return Ok(Err(RpcError::InvalidRequest(
                            "duplicate replay target id".into(),
                        )));
                    }
                }
                *current_generation = generation;
                Ok(Ok(ReplaySubscriptionResponse::Updated { generation }))
            }
            ReplaySubscriptionRequest::Close {
                session_id,
                subscription_id,
            } => {
                let removed = self
                    .replay_subscriptions
                    .lock()
                    .await
                    .remove(&(session_id, subscription_id));
                if removed.is_none() {
                    return Ok(Err(RpcError::UnknownSubscription));
                }
                Ok(Ok(ReplaySubscriptionResponse::Closed))
            }
            ReplaySubscriptionRequest::Next {
                session_id,
                subscription_id,
                request_id,
                supersede,
                targets,
                limit,
                hold_ms,
            } => {
                let subscriptions = self.replay_subscriptions.lock().await;
                let Some((_, target_map)) = subscriptions.get(&(session_id, subscription_id))
                else {
                    return Ok(Err(RpcError::UnknownSubscription));
                };
                let requested = targets
                    .iter()
                    .map(|(target_id, cursor)| {
                        target_map
                            .get(target_id)
                            .cloned()
                            .map(|target| target.with_cursor(*cursor))
                            .ok_or(RpcError::InvalidRequest("unknown replay target id".into()))
                    })
                    .collect::<Result<Vec<_>, _>>();
                let requested = match requested {
                    Ok(requested) => requested,
                    Err(error) => return Ok(Err(error)),
                };
                let target_ids = targets
                    .iter()
                    .map(|(target_id, _)| {
                        (
                            target_map
                                .get(target_id)
                                .expect("requested target was validated above")
                                .clone(),
                            *target_id,
                        )
                    })
                    .collect::<HashMap<_, _>>();
                drop(subscriptions);
                let page = self
                    .target_part_store
                    .replay_page_round(
                        ReplayPageRequest {
                            session_id,
                            request_id,
                            supersede,
                            targets: requested,
                            limit,
                            hold_ms,
                        },
                        self.subscriber.clone(),
                        hold(hold_ms),
                        tokio_util::sync::CancellationToken::new(),
                    )
                    .await?;
                let target_verdicts = page
                    .targets
                    .iter()
                    .filter_map(|(target, verdict)| {
                        target_ids
                            .get(&big_sync_core::rpc::ReplaySubscriptionTarget::from(target))
                            .map(|target_id| (*target_id, verdict.clone()))
                    })
                    .collect();
                Ok(Ok(ReplaySubscriptionResponse::Page(
                    ReplaySubscriptionPage {
                        page: ReplayPage {
                            events: page.events,
                            targets: Vec::new(),
                        },
                        targets: target_verdicts,
                    },
                )))
            }
        }
    }

    async fn get_changed_buckets(
        &self,
        req: big_sync::rpc::ScopedRequest<big_sync_core::rpc::GetChangedBucketsRequest>,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<Vec<big_sync_core::rpc::BucketSummary>, big_sync_core::rpc::ListPartsError>,
        >,
    > {
        Ok(Ok(self
            .target_part_store
            .get_changed_buckets(req.inner, self.subscriber.clone())
            .await?))
    }

    async fn leaf_buckets(
        &self,
        req: big_sync::rpc::ScopedRequest<big_sync_core::rpc::LeafBucketsRequest>,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<big_sync_core::rpc::LeafBucketResult, big_sync_core::rpc::LeafBucketsError>,
        >,
    > {
        Ok(Ok(self
            .target_part_store
            .leaf_buckets(req.inner, self.subscriber.clone())
            .await?))
    }
}

struct SyncRepoNode {
    #[expect(dead_code)] // kept alive by boot(); used for teardown diagnostics
    path: PathBuf,
    repo: Arc<BigRepo>,
    big_sync_store: SharedPartStore,
    big_sync_worker: big_sync::BigSyncWorkerHandle,
    connections: Arc<tokio::sync::Mutex<HashMap<PeerKey, BigRepoConnection>>>,
    stop_token: BigRepoStopToken,
    endpoint: iroh::Endpoint,
    router: iroh::protocol::Router,
    repo_rpc_stop: crate::rpc::BigRepoRpcStopToken,
    accept_count: Arc<AtomicUsize>,
    accept_notify: Arc<Notify>,
    accepted_connection: Arc<tokio::sync::Mutex<Option<BigRepoConnection>>>,
    big_sync_stop: big_sync::StopToken,
    sync_backend: Arc<BigRepoSyncBackend>,
}

impl SyncRepoNode {
    #[tracing::instrument(skip(path), fields(seed, accept_incoming))]
    async fn boot(path: PathBuf, seed: u8, accept_incoming: bool) -> Res<Self> {
        tracing::info!(path = %path.display(), "booting sync repo node");
        tokio::fs::create_dir_all(&path)
            .await
            .wrap_err_with(|| format!("failed creating sync repo path: {}", path.display()))?;
        let node_identity_seed = [seed; 32];
        let (repo, stop_token) = BigRepo::boot(Config {
            node_identity_seed,
            storage: StorageConfig::Disk { path: path.clone() },
            scope_key: Arc::from("big-repo-sync-test"),
            hidden_parts: HashSet::new(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
            keyhive_change_notifs: true,
        })
        .await?;
        let shared_store = repo.shared_part_store();
        let (initial_worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
            Arc::clone(&shared_store),
            HashMap::new(),
            "big-repo-sync-test",
            None,
            HARNESS_SYNC_MODE,
            Arc::from("big-repo-test"),
        )?;
        let big_sync_host = Arc::new(big_sync::Ctx {
            store: shared_store,
            worker: initial_worker,
        });
        // The stress test part must exist before membership is written to it. It
        // must not be seeded with a synthetic object payload: every object in the
        // document scope has to be a document, which the frontier worker asserts
        // in test builds.
        big_sync_host
            .store
            .ensure_part(stress_support::test_part())
            .await?;
        big_sync_stop.stop().await?;

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .wrap_err("failed binding iroh endpoint")?;

        let sync_backend = Arc::new(
            BigRepoSyncBackend::boot(Arc::downgrade(&repo))
                .await
                .wrap_err("failed booting big repo sync backend")?,
        );
        let mut sync_backends = HashMap::new();
        sync_backends.insert(BigRepo::BACKEND_ID.into(), Arc::clone(&sync_backend) as _);
        let (big_sync_worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
            Arc::clone(&big_sync_host.store),
            sync_backends,
            "big-repo-sync-test-main",
            None,
            HARNESS_SYNC_MODE,
            Arc::from("big-repo-test"),
        )?;

        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_notify = Arc::new(Notify::new());
        let accepted_connection = Arc::new(tokio::sync::Mutex::new(None));
        let (repo_rpc, repo_rpc_stop) = crate::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
        let connections = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                SubductionProtocolHandler {
                    repo: Arc::clone(&repo),
                    endpoint: endpoint.clone(),
                    track_accepts: accept_incoming,
                    accept_count: Arc::clone(&accept_count),
                    accept_notify: Arc::clone(&accept_notify),
                    accepted_connection: Arc::clone(&accepted_connection),
                },
            )
            .accept(crate::rpc::REPO_SYNC_ALPN, repo_rpc.protocol_handler())
            .spawn();

        tracing::info!(
            repo_peer_id = %repo.local_peer_id(),
            endpoint_id = %router.endpoint().addr().id,
            accept_incoming,
            "booted sync repo node"
        );

        Ok(Self {
            path,
            repo,
            big_sync_store: Arc::clone(&big_sync_host.store),
            big_sync_worker,
            connections,
            stop_token,
            big_sync_stop,
            endpoint,
            router,
            repo_rpc_stop,
            accept_count,
            accept_notify,
            accepted_connection,
            sync_backend,
        })
    }

    fn peer_id(&self) -> PeerKey {
        self.repo.local_peer_id()
    }

    #[tracing::instrument(skip(self), fields(expected))]
    async fn wait_for_accepts(&self, expected: usize) {
        timeout(SYNC_PROPAGATION_TIMEOUT, async {
            loop {
                if self.accept_count.load(Ordering::SeqCst) >= expected {
                    break;
                }
                self.accept_notify.notified().await;
            }
        })
        .await
        .expect("timed out waiting for iroh accept loop");
    }

    /// Wait for an accepted connection on this node and take it.
    ///
    /// The peer's accept callback stores the connection asynchronously, so a
    /// caller that dials and then reads the slot immediately races it: the
    /// connection is not recorded yet and an `expect` here would panic. Wait
    /// for the callback instead of assuming the slot is already filled.
    async fn take_latest_accepted_connection(&self) -> BigRepoConnection {
        timeout(SYNC_PROPAGATION_TIMEOUT, async {
            loop {
                let notified = self.accept_notify.notified();
                tokio::pin!(notified);
                // Register interest before checking the slot, so a store
                // landing between the check and the wait cannot be lost.
                notified.as_mut().enable();
                if let Some(connection) = self.accepted_connection.lock().await.take() {
                    return connection;
                }
                notified.await;
            }
        })
        .await
        .expect("timed out waiting for the iroh accept loop to record a connection")
    }

    async fn connect_to(&self, remote: &SyncRepoNode) -> Res<()> {
        {
            let mut connections = self.connections.lock().await;
            if connections
                .get(&remote.peer_id())
                .is_some_and(|conn| !conn.is_closed())
            {
                return Ok(());
            }
            connections.remove(&remote.peer_id());
        }
        let conn = self
            .repo
            .open_connection_iroh(
                self.endpoint.clone(),
                remote.endpoint.addr(),
                remote.peer_id(),
                None,
            )
            .await?;
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part_id| (part_id, BigRepo::BACKEND_ID.into()))
            .collect();
        self.big_sync_worker
            .set_peer(
                remote.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&remote.big_sync_store),
                    subscriber: self.peer_id(),
                    replay_subscriptions: Arc::new(Mutex::new(HashMap::new())),
                }),
                parts,
                HashMap::new(),
            )
            .await?;
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part_id| (part_id, BigRepo::BACKEND_ID.into()))
            .collect();
        remote
            .big_sync_worker
            .set_peer(
                self.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&self.big_sync_store),
                    subscriber: remote.peer_id(),
                    replay_subscriptions: Arc::new(Mutex::new(HashMap::new())),
                }),
                parts,
                HashMap::new(),
            )
            .await?;
        self.connections.lock().await.insert(remote.peer_id(), conn);
        Ok(())
    }

    async fn disconnect_from(&self, remote: &SyncRepoNode) -> Res<()> {
        if let Some(conn) = self.connections.lock().await.remove(&remote.peer_id()) {
            conn.stop().await?;
        }
        self.big_sync_worker.remove_peer(remote.peer_id()).await?;
        remote.big_sync_worker.remove_peer(self.peer_id()).await?;
        Ok(())
    }

    async fn stop_big_sync_with(&self, remote: &SyncRepoNode) -> Res<()> {
        self.big_sync_worker.remove_peer(remote.peer_id()).await?;
        remote.big_sync_worker.remove_peer(self.peer_id()).await?;
        Ok(())
    }

    async fn connection_to(&self, remote: &SyncRepoNode) -> BigRepoConnection {
        self.connections
            .lock()
            .await
            .get(&remote.peer_id())
            .cloned()
            .expect("connection should exist")
    }

    #[tracing::instrument(skip(self))]
    async fn shutdown(self) -> Res<()> {
        tracing::info!(
            repo_peer_id = %self.repo.local_peer_id(),
            "shutting down sync repo node"
        );
        self.endpoint.close().await;
        self.stop_token.stop().await?;
        self.big_sync_stop.stop().await?;
        self.repo_rpc_stop.stop().await?;
        drop(self.router);
        Ok(())
    }
}

#[tracing::instrument(skip_all, fields(item_count, payload_len, ?local_mutation, ?remote_mutation))]
async fn run_sync_case(
    item_count: usize,
    payload_len: usize,
    local_mutation: Option<SyncMutation>,
    remote_mutation: Option<SyncMutation>,
    exit_after_put: bool,
) -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    tracing::info!("starting sync case");
    let temp_root = tempdir()?;
    let server_path = temp_root.path().join("server");
    let client_path = temp_root.path().join("client");

    let mut expected_doc = make_sync_doc_value("base", item_count, payload_len);
    let mut client_expected_doc = expected_doc.clone();
    let mut server_expected_doc = expected_doc.clone();
    let mut base_doc = automerge::Automerge::new();
    write_sync_doc_value(&mut base_doc, &expected_doc);

    tracing::info!("booting server and client repos");
    let server = SyncRepoNode::boot(server_path, 51, true).await?;
    let client = SyncRepoNode::boot(client_path, 61, false).await?;

    tracing::info!("server creating minimal doc (content is added after grant)");
    let server_doc = {
        let mut d = automerge::Automerge::new();
        d.transact(|tx| {
            tx.put(automerge::ROOT, "_", "").unwrap();
            Ok::<_, automerge::AutomergeError>(())
        })
        .unwrap();
        server.repo.create_doc(d).await?
    };
    let doc_id = server_doc.document_id();

    if exit_after_put {
        tracing::info!("exiting sync case immediately after doc creation");
        server.shutdown().await?;
        client.shutdown().await?;
        return Ok(());
    }

    tracing::info!("connecting client to server");
    let client_conn = client
        .repo
        .open_connection_iroh(
            client.endpoint.clone(),
            server.endpoint.addr(),
            server.peer_id(),
            None,
        )
        .await?;
    server.wait_for_accepts(1).await;

    // Keyhive setup: contact cards + grant access
    let server_conn = server.take_latest_accepted_connection().await;
    client_conn.sync_keyhive_with_peer().await?;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
    let client_agent = server
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");
    server
        .repo
        .grant_doc_access(
            doc_id.clone(),
            client_agent,
            keyhive_core::access::Access::Edit,
        )
        .await?;

    // Write actual content AFTER grant so the grant path only needs to
    // preserve future content keys.
    set_doc_actor(&server_doc, automerge::ActorId::from([51_u8; 16])).await?;
    server_doc
        .with_document(|doc| {
            write_sync_doc_value(doc, &make_sync_doc_value("base", item_count, payload_len));
        })
        .await?;

    if let Some(mutation) = remote_mutation {
        tracing::info!(?mutation, "applying remote mutation");
        server_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, payload_len);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, mutation, payload_len);
        apply_sync_mutation(&mut server_expected_doc, mutation, payload_len);
    }

    // Sync the grant delegation + CGKA events before doc sync
    server_conn.sync_keyhive_with_peer().await?;

    // Client syncs doc from server (from empty tree)
    tracing::info!("client pulling doc from server");
    client_conn.sync_doc_with_peer(doc_id.clone()).await?;
    let client_doc = client
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    set_doc_actor(&client_doc, automerge::ActorId::from([61_u8; 16])).await?;

    if let Some(mutation) = local_mutation {
        tracing::info!(?mutation, "applying local mutation");
        client_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, payload_len);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, mutation, payload_len);
        apply_sync_mutation(&mut client_expected_doc, mutation, payload_len);
    }

    if local_mutation.is_some() && remote_mutation.is_some() {
        tracing::info!(
            client_peer_id = %client_conn.peer_id(),
            server_peer_id = %server_conn.peer_id(),
            "running concurrent sync_doc_with_peer"
        );
        let (client_result, server_result) = tokio::join!(
            client_conn.sync_doc_with_peer(doc_id.clone()),
            server_conn.sync_doc_with_peer(doc_id.clone()),
        );
        let () = client_result?;
        let () = server_result?;

        drop(client_doc);
        drop(server_doc);

        let client_doc = client
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        let server_doc = server.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        let client_state = read_json_doc(&client_doc).await;
        let server_state = read_json_doc(&server_doc).await;
        tracing::info!(
            client_state = ?client_state,
            server_state = ?server_state,
            "post-sync diverged-head state"
        );
        tracing::info!(
            client_expected_notes = ?sync_note_snapshot(&client_expected_doc, &[5, 17]),
            server_expected_notes = ?sync_note_snapshot(&server_expected_doc, &[5, 17]),
            expected_notes = ?sync_note_snapshot(&expected_doc, &[5, 17]),
            client_state_notes = ?sync_note_snapshot(&client_state, &[5, 17]),
            server_state_notes = ?sync_note_snapshot(&server_state, &[5, 17]),
            "post-sync diverged-head note snapshot"
        );
        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
    } else {
        if local_mutation.is_some() {
            client_conn.sync_keyhive_with_peer().await?;
            let () = server_conn.sync_doc_with_peer(doc_id).await?;
        }
        tracing::info!(
            peer_id = %client_conn.peer_id(),
            "verifying doc convergence"
        );
        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
    }

    tracing::info!("closing client connection and shutting down repos");
    client_conn.stop().await?;
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(item_count, payload_len, ?first_remote_mutation, ?second_local_mutation)
)]
async fn run_restart_reconnect_case(
    item_count: usize,
    payload_len: usize,
    first_remote_mutation: Option<SyncMutation>,
    second_local_mutation: Option<SyncMutation>,
) -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    tracing::info!("starting reconnect case");
    let temp_root = tempdir()?;
    let server_path = temp_root.path().join("server");
    let client_path = temp_root.path().join("client");

    let mut expected_doc = make_sync_doc_value("base", item_count, payload_len);
    let server = SyncRepoNode::boot(server_path.clone(), 71, true).await?;
    let client = SyncRepoNode::boot(client_path, 81, false).await?;
    client.connect_to(&server).await?;
    let client_conn = client.connection_to(&server).await;
    let server_conn = server.take_latest_accepted_connection().await;
    let server_doc = create_shared_sync_doc(
        &server,
        &client,
        &server_conn,
        &client_conn,
        &expected_doc,
        automerge::ActorId::from([71_u8; 16]),
    )
    .await?;
    let doc_id = server_doc.document_id();

    // Pre-sync so client has the doc under the same ID
    server
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    wait_for_pair_full_sync(&server, &client).await?;
    let client_doc = client
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    set_doc_actor(&client_doc, automerge::ActorId::from([81_u8; 16])).await?;
    client.disconnect_from(&server).await?;

    if let Some(mutation) = first_remote_mutation {
        tracing::info!(?mutation, "applying first remote mutation");
        server_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, payload_len);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, mutation, payload_len);
    }

    tracing::info!("connecting client to server");
    let client_conn = client
        .repo
        .open_connection_iroh(
            client.endpoint.clone(),
            server.endpoint.addr(),
            server.peer_id(),
            None,
        )
        .await?;
    server.wait_for_accepts(1).await;

    tracing::info!("running initial sync before server shutdown");
    client_conn.sync_doc_with_peer(doc_id.clone()).await?;
    wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
    wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

    tracing::info!("shutting down server while connection is still live");
    server.shutdown().await?;
    client_conn.stop().await?;

    tracing::info!("rebooting server from the same disk path");
    let server = SyncRepoNode::boot(server_path, 71, true).await?;
    let server_doc = server
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

    if let Some(mutation) = second_local_mutation {
        tracing::info!(?mutation, "applying second local mutation after restart");
        client_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, payload_len);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, mutation, payload_len);
    }

    tracing::info!("reconnecting after server restart");
    let client_conn = client
        .repo
        .open_connection_iroh(
            client.endpoint.clone(),
            server.endpoint.addr(),
            server.peer_id(),
            None,
        )
        .await?;
    server.wait_for_accepts(1).await;

    tracing::info!("running sync after restart");
    client_conn.sync_doc_with_peer(doc_id).await?;
    wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
    wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

    client_conn.stop().await?;
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(item_count, payload_len, ?remote_mutation))]
async fn run_remote_change_listener_without_live_handle_case(
    item_count: usize,
    payload_len: usize,
    remote_mutation: SyncMutation,
) -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    tracing::info!("starting remote listener without live handle case");
    let temp_root = tempdir()?;
    let server_path = temp_root.path().join("server");
    let client_path = temp_root.path().join("client");

    let mut expected_doc = make_sync_doc_value("base", item_count, payload_len);

    let server = SyncRepoNode::boot(server_path, 91, true).await?;
    let client = SyncRepoNode::boot(client_path, 92, false).await?;
    client.connect_to(&server).await?;
    let client_conn = client.connection_to(&server).await;
    let server_conn = server.take_latest_accepted_connection().await;
    let server_doc = create_shared_sync_doc(
        &server,
        &client,
        &server_conn,
        &client_conn,
        &expected_doc,
        automerge::ActorId::from([91_u8; 16]),
    )
    .await?;
    let doc_id = server_doc.document_id();

    // Pre-sync so client has the doc under the same ID
    server
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    wait_for_pair_full_sync(&server, &client).await?;
    let client_doc = client
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    set_doc_actor(&client_doc, automerge::ActorId::from([92_u8; 16])).await?;
    client.disconnect_from(&server).await?;

    let (_change_registration, mut change_rx) = server
        .repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
            origin: Some(BigRepoOriginFilter::Remote),
            path: Vec::new(),
        })
        .await?;
    let (_head_registration, mut head_rx) = server
        .repo
        .change_manager
        .subscribe_head_listener(super::changes::HeadFilter {
            doc_id: Some(super::changes::DocIdFilter::new(doc_id.clone())),
        })
        .await?;

    drop(server_doc);
    tracing::info!("dropped the server doc handle before remote sync");

    client_doc
        .with_document(|doc| {
            apply_sync_mutation_in_place(doc, remote_mutation, payload_len);
        })
        .await?;
    apply_sync_mutation(&mut expected_doc, remote_mutation, payload_len);

    let client_conn = connect_sync_pair(&client, &server).await?;
    server.wait_for_accepts(1).await;
    let server_conn = server.take_latest_accepted_connection().await;

    server_conn.sync_keyhive_with_peer().await?;
    server_conn.sync_doc_with_peer(doc_id.clone()).await?;

    assert!(
        timeout(Duration::from_millis(250), change_rx.recv())
            .await
            .is_err(),
        "a document without a live handle must not emit materialized change notifications"
    );
    let head_batch: Vec<super::changes::BigRepoHeadNotification> =
        recv_head_batch(&mut head_rx).await;
    assert!(matches!(
        head_batch.as_slice(),
        [super::changes::BigRepoHeadNotification::ColdSedimentreeHeadsUpdated {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Remote { .. },
        }] if *seen_doc_id == doc_id
    ));

    let reopened = server.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    wait_for_json_doc(&reopened, &expected_doc, SYNC_CASE_TIMEOUT).await;

    client_conn.stop().await?;
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(doc_id = %handle.document_id()))]
async fn set_doc_actor(handle: &BigDocHandle, actor: automerge::ActorId) -> Res<()> {
    handle
        .with_document(|doc| {
            doc.set_actor(actor);
        })
        .await?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(doc_id = %doc_id, ?mutation))]
async fn apply_local_sync_mutation_and_assert_notifications(
    repo: &Arc<BigRepo>,
    stale_peer_conn: &BigRepoConnection,
    handle: &BigDocHandle,
    doc_id: DocumentId,
    mutation: SyncMutation,
    payload_len: usize,
) -> Res<()> {
    let (_change_registration, mut change_rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
            origin: Some(BigRepoOriginFilter::Local),
            path: Vec::new(),
        })
        .await?;
    let (_head_registration, mut head_rx) = repo
        .change_manager
        .subscribe_head_listener(super::changes::HeadFilter {
            doc_id: Some(super::changes::DocIdFilter::new(doc_id.clone())),
        })
        .await?;

    handle
        .with_document(|doc| {
            apply_sync_mutation_in_place(doc, mutation, payload_len);
        })
        .await?;

    let change_batch = recv_change_batch(&mut change_rx).await;
    assert!(matches!(
        change_batch.as_slice(),
        [BigRepoChangeNotification::DocChanged {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Local,
            ..
        }] if *seen_doc_id == doc_id
    ));

    let head_batch: Vec<super::changes::BigRepoHeadNotification> =
        recv_head_batch(&mut head_rx).await;
    assert!(matches!(
        head_batch.as_slice(),
        [super::changes::BigRepoHeadNotification::SedimentreeHeadsChanged {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Local,
            ..
        }] if *seen_doc_id == doc_id
    ));

    stale_peer_conn.sync_keyhive_with_peer().await?;
    stale_peer_conn.sync_doc_with_peer(doc_id).await?;
    Ok(())
}

async fn connect_sync_pair(client: &SyncRepoNode, server: &SyncRepoNode) -> Res<BigRepoConnection> {
    // client.connect_to(server).await?;
    // Ok(client
    //     .connections
    //     .lock()
    //     .await
    //     .get(&server.peer_id())
    //     .cloned()
    //     .expect(ERROR_IMPOSSIBLE))
    let conn = client
        .repo
        .open_connection_iroh(
            client.endpoint.clone(),
            server.endpoint.addr(),
            server.peer_id(),
            None,
        )
        .await?;
    Ok(conn)
}

#[tracing::instrument(
    skip_all,
    fields(?local_mutation, ?remote_mutation, ?expected_deets, expect_client_doc)
)]
async fn run_sync_backend_case(
    local_mutation: Option<SyncMutation>,
    remote_mutation: Option<SyncMutation>,
    expected_deets: SyncCompletionDeets,
    expect_client_doc: bool,
    sync_part_hints: Vec<PartKey>,
    remote_payload_missing: bool,
) -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    tracing::info!("starting sync backend case");
    let temp_root = tempdir()?;
    let server_path = temp_root.path().join("server");
    let client_path = temp_root.path().join("client");

    let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);
    let mut base_doc = automerge::Automerge::new();
    write_sync_doc_value(&mut base_doc, &expected_doc);

    let server = SyncRepoNode::boot(server_path, 131, true).await?;
    let client = SyncRepoNode::boot(client_path, 132, false).await?;

    // Connect and exchange contact cards first
    client.connect_to(&server).await?;
    let client_conn = client
        .connections
        .lock()
        .await
        .get(&server.peer_id())
        .cloned()
        .expect("connection should exist after connect_to");
    client_conn.sync_keyhive_with_peer().await?;

    // Create a minimal doc to get a keyhive document ID. The content written
    // after the grant exercises the post-grant encryption path.
    let server_doc = {
        let mut d = automerge::Automerge::new();
        d.transact(|tx| {
            tx.put(automerge::ROOT, "_", "").unwrap();
            Ok::<_, automerge::AutomergeError>(())
        })
        .unwrap();
        server.repo.create_doc(d).await?
    };
    let doc_id = server_doc.document_id();
    set_doc_actor(&server_doc, automerge::ActorId::from([131_u8; 16])).await?;

    // Grant client access and sync keyhive to propagate the new
    // CGKA tree state. Any content written after this will use a
    // PCS key that includes the client's leaf.
    {
        let client_kh_peer_id = KeyhivePeerId::from_bytes(client.peer_id().to_bytes32());
        let client_agent = server
            .repo
            .keyhive()
            .get_agent_by_peer_id(&client_kh_peer_id)
            .await?
            .expect("client agent should be known after keyhive sync");
        server
            .repo
            .grant_doc_access(
                doc_id.clone(),
                client_agent,
                keyhive_core::access::Access::Edit,
            )
            .await?;
    }
    client_conn.sync_keyhive_with_peer().await?;
    // Stop background big-sync in *both* directions before the document is
    // published, so the case's premise ("the client does not have this
    // object") cannot be invalidated by a notification-driven part or
    // document sync racing the explicit backend call below. The transport
    // connection stays up: the backend fetch under test is a runtime document
    // sync, which does not use the background object-sync routes.
    client.stop_big_sync_with(&server).await?;
    client.repo.wait_for_quiescence(None).await?;
    server.repo.wait_for_quiescence(None).await?;

    // Write the real content now that the client is a member.
    // The encrypt here produces a PCS key the client can derive.
    server_doc
        .with_document(|doc| {
            write_sync_doc_value(
                doc,
                &make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN),
            );
        })
        .await?;
    client_conn.sync_keyhive_with_peer().await?;

    // Register for sync
    server
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;

    let client_doc = if expect_client_doc {
        // Bootstrap cases that exercise updates to an existing client document.
        // The added-member case intentionally leaves the document absent.
        client
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        client_conn.sync_doc_with_peer(doc_id.clone()).await?;
        let doc = client
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        set_doc_actor(&doc, automerge::ActorId::from([132_u8; 16])).await?;
        Some(doc)
    } else {
        None
    };

    if let Some(mutation) = local_mutation {
        tracing::info!(?mutation, "applying local mutation");
        client_doc
            .as_ref()
            .expect("client doc should exist for local mutation")
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, SYNC_DOC_PAYLOAD_LEN);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, mutation, SYNC_DOC_PAYLOAD_LEN);
    }
    if let Some(mutation) = remote_mutation {
        tracing::info!(?mutation, "applying remote mutation");
        server_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(doc, mutation, SYNC_DOC_PAYLOAD_LEN);
            })
            .await?;
        apply_sync_mutation(&mut expected_doc, mutation, SYNC_DOC_PAYLOAD_LEN);
    }

    // Sync keyhive again after any encrypt-generating mutations
    client_conn.sync_keyhive_with_peer().await?;

    let backend = Arc::clone(&client.sync_backend);
    let local_payload = client.big_sync_store.obj_payload(doc_id.clone()).await?;
    let remote_payload = server.big_sync_store.obj_payload(doc_id.clone()).await?;
    // A prior subscription may deliver the remote mutation — or the whole
    // object — before this backend invocation: a subscribed part plus eager
    // replication means the client can fetch the document on its own. The
    // completion describes work performed by *this* invocation, so an object
    // that is already present (or already matches the peer) is correctly a
    // no-op.
    //
    // When the case models a missing peer payload *and* expects the object to
    // be absent locally, no payload comparison is possible; if eager
    // replication already delivered it, the backend's document-sync fallback
    // can only confirm convergence. Report that explicitly so a premise that
    // was won by the background route is visible instead of looking like a
    // wrong completion.
    let converged_before_call = local_payload == remote_payload
        || (remote_payload_missing && !expect_client_doc && local_payload.is_some());
    let expected_deets =
        if expected_deets == SyncCompletionDeets::ChangedObject && converged_before_call {
            if remote_payload_missing && !expect_client_doc && local_payload.is_some() {
                tracing::warn!(
                    doc_id = %doc_id,
                    ?local_payload,
                    ?remote_payload,
                    "object already present before the explicit fetch: expecting a no-op completion"
                );
            }
            SyncCompletionDeets::Noop
        } else {
            expected_deets
        };
    let expected_parts = {
        let base = if sync_part_hints.is_empty() {
            client.big_sync_store.obj_parts(doc_id.clone()).await?
        } else {
            sync_part_hints.clone()
        };
        // The runtime auto-adds docs to the global partition on read access
        // (marker model). Include it in expectations.
        let mut parts = base;
        if !parts.contains(&crate::global_part_id()) {
            parts.push(crate::global_part_id());
        }
        parts
    };
    let scenario = SyncBackendScenario {
        name: "big_repo_sync_backend_case",
        peer_id: server.peer_id(),
        obj_id: doc_id.clone(),
        initial_payload: local_payload.clone(),
        initial_parts: sync_part_hints.clone(),
        remote_payload: if remote_payload_missing {
            None
        } else {
            remote_payload.clone()
        },
        expected_outcome: SyncBackendOutcome::Completion(expected_deets.clone()),
        expected_parts,
    };
    let harness = BigRepoSyncBackendContractHarness {
        backend,
        store: Arc::clone(&client.big_sync_store),
    };
    contract::assert_sync_backend_case(&harness, &scenario).await?;

    if let Some(client_doc) = &client_doc {
        wait_for_json_doc(
            client_doc,
            &expected_doc,
            utils_rs::scale_timeout(SYNC_CASE_TIMEOUT),
        )
        .await;
    } else {
        let imported_client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        wait_for_json_doc(
            &imported_client_doc,
            &expected_doc,
            utils_rs::scale_timeout(SYNC_CASE_TIMEOUT),
        )
        .await;
    }
    wait_for_json_doc(
        &server_doc,
        &expected_doc,
        utils_rs::scale_timeout(SYNC_CASE_TIMEOUT),
    )
    .await;

    client.disconnect_from(&server).await?;
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

async fn run_sync_backend_changed_object_case(remote_mutation: Option<SyncMutation>) -> Res<()> {
    run_sync_backend_case(
        None,
        remote_mutation,
        SyncCompletionDeets::ChangedObject,
        false,
        sync_test_parts(),
        false,
    )
    .await
}

async fn run_sync_backend_remote_payload_missing_noop_case() -> Res<()> {
    run_sync_backend_case(
        None,
        None,
        SyncCompletionDeets::Noop,
        true,
        sync_test_parts(),
        true,
    )
    .await
}

async fn run_sync_backend_missing_local_and_remote_payload_case() -> Res<()> {
    run_sync_backend_case(
        None,
        None,
        SyncCompletionDeets::ChangedObject,
        false,
        sync_test_parts(),
        true,
    )
    .await
}

async fn run_sync_backend_remote_payload_missing_changed_case(
    sync_part_hints: Vec<PartKey>,
) -> Res<()> {
    run_sync_backend_case(
        None,
        Some(SyncMutation {
            item_idx: 29,
            note_key: "remote_missing",
            side_label: "remote",
        }),
        SyncCompletionDeets::ChangedObject,
        true,
        sync_part_hints,
        true,
    )
    .await
}

async fn run_sync_backend_put_doc_conflict_case() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    tracing::info!("starting sync backend put-doc-conflict case");
    let temp_root = tempdir()?;
    let server_path = temp_root.path().join("server");
    let client_path = temp_root.path().join("client");

    let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);

    let server = SyncRepoNode::boot(server_path, 131, true).await?;
    let client = SyncRepoNode::boot(client_path, 132, false).await?;
    client.connect_to(&server).await?;
    let client_conn = client.connection_to(&server).await;
    let server_conn = server.take_latest_accepted_connection().await;
    let server_doc = create_shared_sync_doc(
        &server,
        &client,
        &server_conn,
        &client_conn,
        &expected_doc,
        automerge::ActorId::from([131_u8; 16]),
    )
    .await?;
    let doc_id = server_doc.document_id();

    // Pre-sync so client has the doc under the same ID
    server
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
        .await?;
    wait_for_pair_full_sync(&server, &client).await?;
    let client_doc = client
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id.clone())?;
    set_doc_actor(&client_doc, automerge::ActorId::from([132_u8; 16])).await?;
    // Isolate the backend call under test before publishing the remote mutation.
    // Otherwise BigSync can legitimately win the race and make the explicit call a no-op.
    client.stop_big_sync_with(&server).await?;

    let remote_mutation = SyncMutation {
        item_idx: 21,
        note_key: "remote_conflict",
        side_label: "remote",
    };
    server_doc
        .with_document(|doc| {
            apply_sync_mutation_in_place(doc, remote_mutation, SYNC_DOC_PAYLOAD_LEN);
        })
        .await?;
    apply_sync_mutation(&mut expected_doc, remote_mutation, SYNC_DOC_PAYLOAD_LEN);

    client_conn.sync_keyhive_with_peer().await?;

    client
        .big_sync_store
        .remove_obj_from_part(doc_id.clone(), sync_test_part())
        .await?;

    let remote_payload = server.big_sync_store.obj_payload(doc_id.clone()).await?;
    let outcome = client
        .sync_backend
        .sync_obj(
            client_conn.peer_id(),
            doc_id.clone(),
            Vec::new(),
            remote_payload.clone(),
        )
        .await?;
    assert!(
        matches!(
            outcome,
            big_sync::SyncTaskRunOutcome::Completion(big_sync_core::SyncTaskCompletion {
                deets: SyncCompletionDeets::ChangedObject,
                ..
            })
        ),
        "unexpected sync outcome for put_doc_conflict_retries_sync_and_materializes_heads: {outcome:?}"
    );
    assert_eq!(
        client.big_sync_store.obj_payload(doc_id).await?,
        remote_payload,
        "unexpected payload after put_doc_conflict_retries_sync_and_materializes_heads"
    );

    wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
    wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

    client_conn.stop().await?;
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

async fn wait_for_pair_full_sync(left: &SyncRepoNode, right: &SyncRepoNode) -> Res<()> {
    let left_wait = timeout(
        SYNC_CASE_TIMEOUT,
        left.big_sync_worker
            .wait_for_full_sync([right.peer_id()], stress_support::test_parts()),
    );
    let right_wait = timeout(
        SYNC_CASE_TIMEOUT,
        right
            .big_sync_worker
            .wait_for_full_sync([left.peer_id()], stress_support::test_parts()),
    );
    left_wait
        .await
        .expect("timed out waiting for left node full sync")?;
    right_wait
        .await
        .expect("timed out waiting for right node full sync")?;
    Ok(())
}

async fn assert_pair_sync_alignment(
    left: &SyncRepoNode,
    right: &SyncRepoNode,
    doc_id: ObjKey,
) -> Res<()> {
    let left_heads = left.repo.doc_payload_heads(doc_id.clone()).await?;
    let right_heads = right.repo.doc_payload_heads(doc_id.clone()).await?;
    assert_eq!(
        left_heads, right_heads,
        "payload heads diverged for doc {doc_id:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_returns_noop_when_heads_match() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_case(
            None,
            None,
            SyncCompletionDeets::Noop,
            true,
            sync_test_parts(),
            false,
        ),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_applies_remote_update() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_case(
            None,
            Some(SyncMutation {
                item_idx: 17,
                note_key: "remote_backend",
                side_label: "remote",
            }),
            SyncCompletionDeets::ChangedObject,
            true,
            sync_test_parts(),
            false,
        ),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_applies_remote_update_with_empty_part_hints() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_case(
            None,
            Some(SyncMutation {
                item_idx: 18,
                note_key: "remote_backend_empty",
                side_label: "remote",
            }),
            SyncCompletionDeets::ChangedObject,
            true,
            vec![],
            false,
        ),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_applies_remote_update_with_multiple_part_hints() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_case(
            None,
            Some(SyncMutation {
                item_idx: 19,
                note_key: "remote_backend_multi",
                side_label: "remote",
            }),
            SyncCompletionDeets::ChangedObject,
            true,
            sync_test_parts_multi(),
            false,
        ),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_returns_noop_when_remote_payload_is_missing() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_remote_payload_missing_noop_case(),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_fetches_missing_doc_when_remote_payload_is_missing() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_missing_local_and_remote_payload_case(),
    )
    .await
    .expect("sync backend missing-document test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_applies_remote_update_when_remote_payload_is_missing() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_remote_payload_missing_changed_case(sync_test_parts()),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_adds_missing_doc() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_backend_changed_object_case(Some(SyncMutation {
            item_idx: 23,
            note_key: "added_member",
            side_label: "remote",
        })),
    )
    .await
    .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_backend_recovers_from_put_doc_conflict() -> Res<()> {
    timeout(SYNC_CASE_TIMEOUT, run_sync_backend_put_doc_conflict_case())
        .await
        .expect("sync backend test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_payload_first_membership_late_reconnects_cleanly() -> Res<()> {
    timeout(SYNC_CASE_TIMEOUT, async {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting payload-first membership-late reconnect regression");
        let temp_root = tempdir()?;
        let left_path = temp_root.path().join("left");
        let right_path = temp_root.path().join("right");
        let left = SyncRepoNode::boot(left_path, 141, true).await?;
        let right = SyncRepoNode::boot(right_path, 142, false).await?;
        let expected_doc = make_sync_doc_value("payload-first-reconnect", 8, 48);
        right.connect_to(&left).await?;
        left.wait_for_accepts(1).await;
        let right_conn = right.connection_to(&left).await;
        let left_conn = left.take_latest_accepted_connection().await;
        let left_doc = create_shared_sync_doc(
            &left,
            &right,
            &left_conn,
            &right_conn,
            &expected_doc,
            automerge::ActorId::from([141_u8; 16]),
        )
        .await?;
        let doc_id = left_doc.document_id();
        left.big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        right
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;

        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        assert_pair_sync_alignment(&left, &right, doc_id.clone()).await?;

        right.disconnect_from(&left).await?;
        right.connect_to(&left).await?;
        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        assert_pair_sync_alignment(&left, &right, doc_id).await?;

        right.disconnect_from(&left).await?;
        left.shutdown().await?;
        right.shutdown().await?;
        eyre::Ok(())
    })
    .await
    .expect("payload-first reconnect regression timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_membership_first_payload_late_reconnects_cleanly() -> Res<()> {
    timeout(SYNC_CASE_TIMEOUT, async {
        utils_rs::testing::setup_tracing_once();
        tracing::info!("starting membership-first payload-late reconnect regression");
        let temp_root = tempdir()?;
        let left_path = temp_root.path().join("left");
        let right_path = temp_root.path().join("right");
        let left = SyncRepoNode::boot(left_path, 143, true).await?;
        let right = SyncRepoNode::boot(right_path, 144, false).await?;
        let expected_doc = make_sync_doc_value("membership-first-reconnect", 8, 48);
        right.connect_to(&left).await?;
        left.wait_for_accepts(1).await;
        let right_conn = right.connection_to(&left).await;
        let left_conn = left.take_latest_accepted_connection().await;
        let left_doc = create_shared_sync_doc(
            &left,
            &right,
            &left_conn,
            &right_conn,
            &expected_doc,
            automerge::ActorId::from([143_u8; 16]),
        )
        .await?;
        let doc_id = left_doc.document_id();
        left.big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        right
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;

        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        assert_pair_sync_alignment(&left, &right, doc_id.clone()).await?;

        right.disconnect_from(&left).await?;
        right.connect_to(&left).await?;
        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        assert_pair_sync_alignment(&left, &right, doc_id).await?;

        right.disconnect_from(&left).await?;
        left.shutdown().await?;
        right.shutdown().await?;
        eyre::Ok(())
    })
    .await
    .expect("membership-first reconnect regression timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_uses_remote_changes_when_only_remote_diverged() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_case(
            SYNC_DOC_ITEMS,
            SYNC_DOC_PAYLOAD_LEN,
            None,
            Some(SyncMutation {
                item_idx: 7,
                note_key: "remote_note",
                side_label: "remote",
            }),
            false,
        ),
    )
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_uses_local_changes_when_only_local_diverged() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_case(
            SYNC_DOC_ITEMS,
            SYNC_DOC_PAYLOAD_LEN,
            Some(SyncMutation {
                item_idx: 11,
                note_key: "local_note",
                side_label: "local",
            }),
            None,
            false,
        ),
    )
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_both_diverged_loses_remote_change() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_case(
            SYNC_DOC_ITEMS,
            SYNC_DOC_PAYLOAD_LEN,
            Some(SyncMutation {
                item_idx: 5,
                note_key: "local_note",
                side_label: "local",
            }),
            Some(SyncMutation {
                item_idx: 17,
                note_key: "remote_note",
                side_label: "remote",
            }),
            false,
        ),
    )
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn sync_with_peer_handles_large_fragmented_remote_docs() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_sync_case(
            SYNC_LARGE_DOC_ITEMS,
            SYNC_LARGE_DOC_PAYLOAD_LEN,
            None,
            Some(SyncMutation {
                item_idx: 777,
                note_key: "remote_note",
                side_label: "remote",
            }),
            true,
        ),
    )
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_survives_repo_restart_with_live_connection() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT * 2,
        run_restart_reconnect_case(
            SYNC_DOC_ITEMS,
            SYNC_DOC_PAYLOAD_LEN,
            Some(SyncMutation {
                item_idx: 7,
                note_key: "remote_note",
                side_label: "remote",
            }),
            Some(SyncMutation {
                item_idx: 3,
                note_key: "local_after_restart",
                side_label: "local",
            }),
        ),
    )
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_local_write_emits_notifications_while_connected() -> Res<()> {
    timeout(SYNC_CASE_TIMEOUT, async {
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);

        let server = SyncRepoNode::boot(server_path, 101, true).await?;
        let client = SyncRepoNode::boot(client_path, 102, false).await?;
        client.connect_to(&server).await?;
        let client_conn = client.connection_to(&server).await;
        let server_conn = server.take_latest_accepted_connection().await;
        let server_doc = create_shared_sync_doc(
            &server,
            &client,
            &server_conn,
            &client_conn,
            &expected_doc,
            automerge::ActorId::from([101_u8; 16]),
        )
        .await?;
        let doc_id = server_doc.document_id();

        // Pre-sync so client has the doc under the same ID
        server
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        client
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        wait_for_pair_full_sync(&server, &client).await?;
        let client_doc = client
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        set_doc_actor(&client_doc, automerge::ActorId::from([102_u8; 16])).await?;

        apply_local_sync_mutation_and_assert_notifications(
            &client.repo,
            &server_conn,
            &client_doc,
            doc_id,
            SyncMutation {
                item_idx: 4,
                note_key: "local_connected",
                side_label: "local",
            },
            SYNC_DOC_PAYLOAD_LEN,
        )
        .await?;
        apply_sync_mutation(
            &mut expected_doc,
            SyncMutation {
                item_idx: 4,
                note_key: "local_connected",
                side_label: "local",
            },
            SYNC_DOC_PAYLOAD_LEN,
        );

        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        eyre::Ok(())
    })
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_remote_change_notifies_without_live_handle() -> Res<()> {
    timeout(
        SYNC_CASE_TIMEOUT,
        run_remote_change_listener_without_live_handle_case(
            SYNC_DOC_ITEMS,
            SYNC_DOC_PAYLOAD_LEN,
            SyncMutation {
                item_idx: 13,
                note_key: "remote_no_handle",
                side_label: "remote",
            },
        ),
    )
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_remote_change_notifies_with_live_handle_and_listeners() -> Res<()> {
    timeout(SYNC_CASE_TIMEOUT, async {
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);

        let server = SyncRepoNode::boot(server_path, 111, true).await?;
        let client = SyncRepoNode::boot(client_path, 112, false).await?;
        client.connect_to(&server).await?;
        let client_conn = client.connection_to(&server).await;
        let server_conn = server.take_latest_accepted_connection().await;
        let server_doc = create_shared_sync_doc(
            &server,
            &client,
            &server_conn,
            &client_conn,
            &expected_doc,
            automerge::ActorId::from([111_u8; 16]),
        )
        .await?;
        let doc_id = server_doc.document_id();

        // Pre-sync so client has the doc under the same ID
        server
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        client
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        wait_for_pair_full_sync(&server, &client).await?;
        let client_doc = client
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        set_doc_actor(&client_doc, automerge::ActorId::from([112_u8; 16])).await?;

        let (_change_registration, mut change_rx) = server
            .repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id.clone())),
                origin: Some(BigRepoOriginFilter::Remote),
                path: Vec::new(),
            })
            .await?;
        let (_head_registration, mut head_rx) = server
            .repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id.clone())),
            })
            .await?;

        client_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(
                    doc,
                    SyncMutation {
                        item_idx: 7,
                        note_key: "remote_with_handle",
                        side_label: "remote",
                    },
                    SYNC_DOC_PAYLOAD_LEN,
                );
            })
            .await?;
        apply_sync_mutation(
            &mut expected_doc,
            SyncMutation {
                item_idx: 7,
                note_key: "remote_with_handle",
                side_label: "remote",
            },
            SYNC_DOC_PAYLOAD_LEN,
        );

        server_conn.sync_keyhive_with_peer().await?;
        server_conn.sync_doc_with_peer(doc_id.clone()).await?;

        let change_batch = recv_change_batch(&mut change_rx).await;
        assert!(matches!(
            change_batch.as_slice(),
            [BigRepoChangeNotification::DocChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        let head_batch: Vec<super::changes::BigRepoHeadNotification> =
            recv_head_batch(&mut head_rx).await;
        assert!(matches!(
            head_batch.as_slice(),
            [super::changes::BigRepoHeadNotification::SedimentreeHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }] if *seen_doc_id == doc_id
        ));

        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        eyre::Ok(())
    })
    .await
    .expect("sync test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_with_peer_local_change_without_change_listener_only_emits_heads() -> Res<()> {
    timeout(SYNC_CASE_TIMEOUT, async {
        let temp_root = tempdir()?;
        let server_path = temp_root.path().join("server");
        let client_path = temp_root.path().join("client");

        let mut expected_doc = make_sync_doc_value("base", SYNC_DOC_ITEMS, SYNC_DOC_PAYLOAD_LEN);

        let server = SyncRepoNode::boot(server_path, 121, true).await?;
        let client = SyncRepoNode::boot(client_path, 122, false).await?;
        client.connect_to(&server).await?;
        let client_conn = client.connection_to(&server).await;
        let server_conn = server.take_latest_accepted_connection().await;
        let server_doc = create_shared_sync_doc(
            &server,
            &client,
            &server_conn,
            &client_conn,
            &expected_doc,
            automerge::ActorId::from([121_u8; 16]),
        )
        .await?;
        let doc_id = server_doc.document_id();

        // Pre-sync so client has the doc under the same ID
        server
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        client
            .big_sync_store
            .add_obj_to_parts(doc_id.clone(), stress_support::test_parts())
            .await?;
        wait_for_pair_full_sync(&server, &client).await?;
        let client_doc = client
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id.clone())?;
        set_doc_actor(&client_doc, automerge::ActorId::from([122_u8; 16])).await?;

        let (_head_registration, mut head_rx) = client
            .repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id.clone())),
            })
            .await?;
        assert!(
            !client
                .repo
                .change_manager
                .has_change_listener_interest(doc_id.clone(), &BigRepoChangeOrigin::Local),
            "no change listeners should be interested before mutation"
        );

        client_doc
            .with_document(|doc| {
                apply_sync_mutation_in_place(
                    doc,
                    SyncMutation {
                        item_idx: 2,
                        note_key: "heads_only",
                        side_label: "local",
                    },
                    SYNC_DOC_PAYLOAD_LEN,
                );
            })
            .await?;
        apply_sync_mutation(
            &mut expected_doc,
            SyncMutation {
                item_idx: 2,
                note_key: "heads_only",
                side_label: "local",
            },
            SYNC_DOC_PAYLOAD_LEN,
        );

        let head_batch: Vec<super::changes::BigRepoHeadNotification> =
            recv_head_batch(&mut head_rx).await;
        assert!(matches!(
            head_batch.as_slice(),
            [super::changes::BigRepoHeadNotification::SedimentreeHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Local,
                ..
            }] if *seen_doc_id == doc_id
        ));

        server_conn.sync_keyhive_with_peer().await?;
        server_conn.sync_doc_with_peer(doc_id).await?;

        wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

        client_conn.stop().await?;
        server.shutdown().await?;
        client.shutdown().await?;
        eyre::Ok(())
    })
    .await
    .expect("sync test timed out")?;
    eyre::Ok(())
}

// --- Keyhive public API smoke tests ---

/// `agents_for_membered` compiles and returns empty for unknown ids.
#[tokio::test]
async fn api_agents_for_membered_empty_for_unknown() -> Res<()> {
    let (repo, _ctx, stop) = boot_repo().await?;
    let fake_id: keyhive_core::principal::identifier::Identifier =
        keyhive_core::principal::identifier::Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&[1u8; 32])?,
        );
    let agents = repo.keyhive.agents_for_membered(fake_id).await;
    assert!(agents.is_empty(), "unknown id should return empty");
    stop().await?;
    Ok(())
}

/// `agent_access_on` compiles and returns None for strangers.
#[tokio::test]
async fn api_agent_access_on_none_for_stranger() -> Res<()> {
    let (repo, _ctx, stop) = boot_repo().await?;
    let stranger: keyhive_core::principal::identifier::Identifier =
        keyhive_core::principal::identifier::Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&[9u8; 32])?,
        );
    let access = repo.keyhive.agent_access_on(&stranger, stranger).await;
    assert!(
        access.is_none(),
        "stranger has no access to unknown membered"
    );
    stop().await?;
    Ok(())
}

/// `docs_for_agent` compiles and returns empty for fresh boot.
#[tokio::test]
async fn api_docs_for_agent_empty_on_fresh_boot() -> Res<()> {
    let (repo, _ctx, stop) = boot_repo().await?;
    let our_id = repo.keyhive.clone_keyhive().id();
    let our_ident: keyhive_core::principal::identifier::Identifier = our_id.into();
    let docs = repo.keyhive.docs_for_agent(&our_ident).await;
    assert!(docs.is_empty(), "fresh repo should have no docs reachable");
    stop().await?;
    Ok(())
}
