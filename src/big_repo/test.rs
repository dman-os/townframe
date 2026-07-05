use super::*;

use crate::encrypted_blob::decode_encrypted_blob;
use am_utils_rs::codecs::ThroughJson;
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use autosurgeon::Prop;
use big_sync::backend::contract::{
    self, SyncBackendHarness, SyncBackendOutcome, SyncBackendScenario,
};
use big_sync::stress_support::{self, StressFixture};
use big_sync::{HostPartStore, SyncBackend};
use big_sync_core::{Byte32Id, PartId, SyncCompletionDeets};
use futures::lock::Mutex;
use nonempty::NonEmpty;
use rand::rngs::StdRng;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::{sync::Notify, time::timeout};

pub async fn boot_part_store(sqlite_url: &str) -> Res<(Arc<big_sync::Ctx>, big_sync::StopToken)> {
    let sql = sqlx_utils_rs::SqlCtx::url(sqlite_url).await?;

    let store = Arc::new(
        big_sync::SqlitePartStore::new(
            sql,
            sqlite_url.to_owned(),
            big_sync_core::BuckId::MAX_LEVEL,
        )
        .await?,
    );
    let store_for_worker: Arc<dyn big_sync::HostPartStore> = Arc::clone(&store) as _;
    let (worker, stop) =
        big_sync::spawn_big_sync_worker(Arc::clone(&store_for_worker), HashMap::new())?;
    Ok((
        Arc::new(big_sync::Ctx {
            store: store_for_worker,
            worker,
        }),
        stop,
    ))
}
pub async fn boot_repo() -> Res<(
    Arc<BigRepo>,
    Arc<big_sync::Ctx>,
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
)> {
    let (big_sync_host, big_sync_stop) = boot_part_store("sqlite::memory:").await?;
    let (repo, stop) = BigRepo::boot(
        Config {
            node_identity_seed: [7_u8; 32],
            storage: StorageConfig::Memory,
        },
        Arc::clone(&big_sync_host.store),
    )
    .await?;
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
    let sqlite_url = format!("sqlite://{}", path.join("part_store.db").display());
    let (big_sync_host, big_sync_stop) = boot_part_store(&sqlite_url).await?;
    let (repo, stop) = BigRepo::boot(
        Config {
            node_identity_seed: [7_u8; 32],
            storage: StorageConfig::Disk { path },
        },
        Arc::clone(&big_sync_host.store),
    )
    .await?;
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

async fn wait_for_keyhive_agent(
    repo: &Arc<BigRepo>,
    peer_id: PeerId,
) -> Res<Option<BigKeyhiveAgent>> {
    use subduction_keyhive::KeyhivePeerId;

    let kh_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(agent) = repo.keyhive().get_agent_by_peer_id(&kh_peer_id).await? {
                return eyre::Ok(Some(agent));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for keyhive agent")
}

fn keyhive_document_id_for_big_repo_doc(
    doc_id: DocumentId,
) -> keyhive_core::principal::document::id::DocumentId {
    let doc_id_bytes = doc_id.into_bytes();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id_bytes)
        .expect("doc id should be a valid keyhive document id");
    keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    )
}

fn keyhive_identifier_for_peer_id(
    peer_id: PeerId,
) -> keyhive_core::principal::identifier::Identifier {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(peer_id.as_bytes())
        .expect("peer id should be a valid keyhive identifier");
    keyhive_core::principal::identifier::Identifier::from(vk)
}

async fn wait_for_keyhive_document(repo: &Arc<BigRepo>, doc_id: DocumentId) -> Res<()> {
    let kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id);
    timeout(Duration::from_secs(5), async {
        loop {
            if repo
                .keyhive()
                .clone_keyhive()
                .get_document(kh_doc_id)
                .await
                .is_some()
            {
                return eyre::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for keyhive document to materialize")
}

async fn wait_for_keyhive_document_access(
    repo: &Arc<BigRepo>,
    doc_id: DocumentId,
    peer_id: PeerId,
    minimum_access: keyhive_core::access::Access,
) -> Res<()> {
    let kh_doc_id = keyhive_document_id_for_big_repo_doc(doc_id);
    let identifier = keyhive_identifier_for_peer_id(peer_id);
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(doc) = repo.keyhive().clone_keyhive().get_document(kh_doc_id).await {
                let members = doc.lock().await.transitive_members().await;
                if members
                    .get(&identifier)
                    .is_some_and(|(_, access)| *access >= minimum_access)
                {
                    return eyre::Ok(());
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for keyhive document access to materialize")
}

#[tokio::test]
async fn put_doc_get_doc_and_export_roundtrip() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let fetched = repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    assert_eq!(fetched.document_id(), doc_id);
    assert_eq!(
        fetched
            .with_document_read(|doc| get_str_at_root(doc, "title"))
            .await,
        "seed"
    );
    let handle = repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
async fn local_boundary_commit_stores_requested_encrypted_fragment() -> Res<()> {
    let (repo, _part_store, _stop_token) = boot_repo().await?;
    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");

    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let mut stored_blob_count = repo.inspect_stored_doc_blobs(doc_id).await?.len();

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
        let next_stored_blob_count = repo.inspect_stored_doc_blobs(doc_id).await?.len();

        if head.0[0] == 0 {
            assert!(
                next_stored_blob_count >= stored_blob_count + 2,
                "boundary commit should store both the loose commit and requested fragment"
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
    let doc_id_bytes = doc_id.into_bytes();
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
        .add_member(
            public_agent,
            &keyhive_core::principal::membered::Membered::Document(kh_doc_id, kh_doc),
            keyhive_core::access::Access::Read,
            &[],
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

    let doc_id_bytes = doc_id.into_bytes();
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
        .add_member(
            public_agent,
            &keyhive_core::principal::membered::Membered::Document(kh_doc_id, kh_doc),
            keyhive_core::access::Access::Read,
            &[],
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
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
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

    owner_conn.sync_keyhive_with_peer(None).await?;

    client_conn
        .sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT))
        .await?;
    let client_doc = wait_for_doc_handle(&client.repo, doc_id).await;
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "seed");

    owner.shutdown().await?;
    client.shutdown().await?;
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
    let owner_eph_peer_id = subduction_core::peer::id::PeerId::new(*owner.peer_id().as_bytes());
    let mut subscription = client
        .repo
        .ephemeral()
        .subscribe(BigEphemeralFilter::new(topic).with_sender(owner_eph_peer_id))
        .await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let _owner_conn = owner.accepted_connection().await;
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
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    assert!(
        wait_for_keyhive_agent(&owner.repo, client.peer_id())
            .await?
            .is_some(),
        "owner should resolve the client as a keyhive agent after connect"
    );
    assert!(
        wait_for_keyhive_agent(&client.repo, owner.peer_id())
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
async fn authorized_peer_reads_encrypted_doc_after_keyhive_change_notification_without_reboot(
) -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 95, true).await?;
    let client = SyncRepoNode::boot(client_path, 96, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let _owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    let client_agent = wait_for_keyhive_agent(&owner.repo, client.peer_id())
        .await?
        .expect("client agent should be known after connection bootstrap");

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    owner
        .repo
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Read)
        .await?;

    wait_for_keyhive_document(&client.repo, doc_id).await?;

    timeout(
        Duration::from_secs(5),
        client_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for authorized doc sync after ephemeral-triggered keyhive sync")?;

    let client_doc = timeout(
        Duration::from_secs(10),
        wait_for_doc_handle(&client.repo, doc_id),
    )
    .await
    .expect("timed out waiting for authorized doc materialization");
    let title = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title, "seed");
    assert!(
        client.repo.doc_payload_heads(doc_id).await?.is_some(),
        "authorized client should have payload heads after ephemeral-triggered keyhive sync and doc sync"
    );

    let handle = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    assert!(!handle.export().await.is_empty());

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn grant_doc_access_writes_checkpoint_ancestor_for_pregrant_head() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 105, true).await?;
    let client = SyncRepoNode::boot(client_path, 106, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let _owner_conn = owner.accepted_connection().await;
    let _client_conn = client.connection_to(&owner).await;

    let client_agent = wait_for_keyhive_agent(&owner.repo, client.peer_id())
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
    let pregrant_blobs = owner.repo.inspect_stored_doc_blobs(doc_id).await?;

    owner
        .repo
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Read)
        .await?;

    let checkpoint_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;
    assert_ne!(
        checkpoint_head, pregrant_head,
        "reader grant should write a checkpoint commit after the pregrant head"
    );

    let postgrant_blobs = owner.repo.inspect_stored_doc_blobs(doc_id).await?;
    assert!(
        postgrant_blobs.len() > pregrant_blobs.len(),
        "reader grant should add a stored checkpoint blob"
    );

    let checkpoint_blob = postgrant_blobs
        .iter()
        .find_map(|raw| {
            let encrypted = decode_encrypted_blob(raw).ok()?;
            (encrypted.content_ref == checkpoint_head).then_some(encrypted)
        })
        .expect("post-grant checkpoint blob should be stored under the new head");

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
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 109, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 110, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    let client_agent = wait_for_keyhive_agent(&owner.repo, client.peer_id())
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
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Edit)
        .await?;

    owner_conn.sync_keyhive_with_peer(None).await?;
    wait_for_keyhive_document(&client.repo, doc_id).await?;

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

    owner_conn.sync_keyhive_with_peer(None).await?;

    let stored_blobs = owner.repo.inspect_stored_doc_blobs(doc_id).await?;
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
        let _ = locked
            .try_decrypt_content_keyed(&postwrite_blob)
            .expect("client should decrypt the post-write blob after edit-grant sync");
    }

    let client_keyhive_storage = crate::keyhive_storage::BigRepoKeyhiveStorage::fs(
        client_path.join(crate::keyhive_storage::KEYHIVE_SUBDIR),
    )?;
    let stored_events = subduction_keyhive::load_events::<Vec<u8>, _, future_form::Sendable>(
        &client_keyhive_storage,
    )
    .await?;
    assert!(
        stored_events.iter().any(|(_, event)| {
            matches!(
                event,
                keyhive_core::event::static_event::StaticEvent::CgkaOperation(signed)
                    if keyhive_crypto::digest::Digest::hash(signed.as_ref())
                        == postwrite_blob.pcs_update_op_hash
            )
        }),
        "client keyhive storage should contain the CGKA op that matches the post-write blob's pcs_update_op_hash"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn client_keyhive_decrypts_postgrant_checkpoint_after_explicit_keyhive_sync() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 107, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 108, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    let client_agent = wait_for_keyhive_agent(&owner.repo, client.peer_id())
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
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Read)
        .await?;

    let checkpoint_head = handle
        .with_document_read(|doc| initial_content_heads(doc).map(|heads| heads.head.to_vec()))
        .await?;
    assert_ne!(
        checkpoint_head, pregrant_head,
        "reader grant should advance the owner head with a checkpoint commit"
    );

    owner_conn.sync_keyhive_with_peer(None).await?;

    wait_for_keyhive_document(&client.repo, doc_id).await?;

    let postgrant_blobs = owner.repo.inspect_stored_doc_blobs(doc_id).await?;
    let checkpoint_blob = postgrant_blobs
        .iter()
        .find_map(|raw| {
            let encrypted = decode_encrypted_blob(raw).ok()?;
            (encrypted.content_ref == checkpoint_head).then_some(encrypted)
        })
        .expect("post-grant checkpoint blob should be stored under the new head");

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

    let client_keyhive_storage = crate::keyhive_storage::BigRepoKeyhiveStorage::fs(
        client_path.join(crate::keyhive_storage::KEYHIVE_SUBDIR),
    )?;
    let stored_events = subduction_keyhive::load_events::<Vec<u8>, _, future_form::Sendable>(
        &client_keyhive_storage,
    )
    .await?;
    assert!(
        stored_events.iter().any(|(_, event)| {
            matches!(
                event,
                keyhive_core::event::static_event::StaticEvent::CgkaOperation(signed)
                    if keyhive_crypto::digest::Digest::hash(signed.as_ref())
                        == checkpoint_blob.pcs_update_op_hash
            )
        }),
        "client keyhive storage should contain the CGKA op that matches the checkpoint's pcs_update_op_hash"
    );

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn disk_repo_round_trip_preserves_encrypted_doc_and_heads() -> Res<()> {
    let temp_root = tempdir()?;
    let repo_path = temp_root.path().join("repo");
    let (repo, _part_store, stop) = _boot_disk_repo(repo_path.clone()).await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "persisted"))
        .expect("failed seeding doc");
    let handle = repo.create_doc(doc).await?;
    let doc_id = handle.document_id();
    let export_before = repo.get_doc(&doc_id).await?.into_ready(doc_id)?.export().await;
    let heads_before = repo
        .doc_payload_heads(doc_id)
        .await?
        .expect("heads should exist before reboot");
    let title_before = handle
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title_before, "persisted");

    stop().await?;

    let (repo, _part_store, stop) = _boot_disk_repo(repo_path).await?;
    let fetched = repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    let title_after = fetched
        .with_document_read(|doc| get_str_at_root(doc, "title"))
        .await;
    assert_eq!(title_after, "persisted");

    let export_after = repo.get_doc(&doc_id).await?.into_ready(doc_id)?.export().await;
    let heads_after = repo
        .doc_payload_heads(doc_id)
        .await?
        .expect("heads should exist after reboot");

    assert_eq!(export_after, export_before);
    assert_eq!(heads_after, heads_before);

    stop().await?;
    Ok(())
}

#[tokio::test]
async fn closed_keyhive_connection_errors_cleanly_then_reconnects() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 97, true).await?;
    let client = SyncRepoNode::boot(client_path, 98, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    let closed_conn = owner_conn.clone();
    owner_conn.stop().await?;
    let err = closed_conn
        .sync_keyhive_with_peer(None)
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
    let second_owner_conn = second_owner.accepted_connection().await;
    let second_client_conn = second_client.connection_to(&second_owner).await;

    timeout(Duration::from_secs(5), async {
        second_owner_conn.sync_keyhive_with_peer(None).await?;
        eyre::Ok(())
    })
    .await
    .expect("timed out waiting for keyhive sync on fresh peer pair")?;

    use subduction_keyhive::KeyhivePeerId;
    let second_client_kh_peer_id = KeyhivePeerId::from_bytes(*second_client.peer_id().as_bytes());
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
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 103, true).await?;
    let client = SyncRepoNode::boot(client_path, 104, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
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
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Read)
        .await?;

    owner_conn.sync_keyhive_with_peer(None).await?;

    timeout(
        Duration::from_secs(5),
        client_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for minimal doc sync")?;

    let client_doc = timeout(
        Duration::from_secs(5),
        wait_for_doc_handle(&client.repo, doc_id),
    )
    .await
    .expect("timed out waiting for minimal doc materialization");
    let value = client_doc
        .with_document_read(|doc| get_str_at_root(doc, "_"))
        .await;
    assert_eq!(value, "");
    assert!(
        client.repo.doc_payload_heads(doc_id).await?.is_some(),
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
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let member_path = temp_root.path().join("member");
    let outsider_path = temp_root.path().join("outsider");
    let owner = SyncRepoNode::boot(owner_path, 105, true).await?;
    let member = SyncRepoNode::boot(member_path, 106, false).await?;
    let outsider = SyncRepoNode::boot(outsider_path, 107, false).await?;

    member.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_member_conn = owner.accepted_connection().await;
    let member_conn = member.connection_to(&owner).await;
    outsider.connect_to(&owner).await?;
    owner.wait_for_accepts(2).await;
    let owner_outsider_conn = owner.accepted_connection().await;
    let outsider_conn = outsider.connection_to(&owner).await;

    owner_member_conn.sync_keyhive_with_peer(None).await?;
    owner_outsider_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let member_kh_peer_id = KeyhivePeerId::from_bytes(*member.peer_id().as_bytes());
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

    owner_member_conn.sync_keyhive_with_peer(None).await?;

    timeout(
        Duration::from_secs(5),
        member_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for member doc sync")?;
    let member_doc = timeout(
        Duration::from_secs(5),
        wait_for_doc_handle(&member.repo, doc_id),
    )
    .await
    .expect("timed out waiting for member doc materialization");
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

    let outsider_sync = timeout(
        Duration::from_secs(5),
        outsider_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for outsider doc sync");
    match outsider_sync {
        Ok(()) => match outsider.repo.get_doc(&doc_id).await? {
            DocLookup::PendingMaterialization | DocLookup::Missing => {}
            DocLookup::Ready(_) => panic!("outsider should not materialize plaintext"),
        },
        Err(err) => {
            assert!(
                matches!(
                    err,
                    SyncDocError::NotFound
                ),
                "outsider sync should fail cleanly, got {err:?}"
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
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 108, true).await?;
    let client = SyncRepoNode::boot(client_path, 109, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
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
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Edit)
        .await?;

    owner_conn.sync_keyhive_with_peer(None).await?;

    owner
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;

    timeout(
        Duration::from_secs(5),
        client_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for initial writer sync")?;

    let owner_doc = owner.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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

    owner_conn.sync_keyhive_with_peer(None).await?;

    let (owner_sync, client_sync) = tokio::join!(
        timeout(
            Duration::from_secs(5),
            owner_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
        ),
        timeout(
            Duration::from_secs(5),
            client_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
        ),
    );
    owner_sync
        .expect("timed out waiting for owner doc sync")
        .expect("owner doc sync failed");
    client_sync
        .expect("timed out waiting for client doc sync")
        .expect("client doc sync failed");

    let owner_doc = owner.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 95, true).await?;
    let client = SyncRepoNode::boot(client_path, 96, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    let mut doc = automerge::Automerge::new();
    doc.transact(|tx| tx.put(automerge::ROOT, "title", "hidden"))
        .expect("failed seeding doc");
    let handle = owner.repo.create_doc(doc).await?;
    let doc_id = handle.document_id();

    owner
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;

    let sync_result = timeout(
        Duration::from_secs(5),
        client_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for unauthorized doc sync");
    match sync_result {
        Ok(()) => {
            assert!(
                client.repo.doc_payload_heads(doc_id).await?.is_some(),
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
                matches!(
                    err,
                    SyncDocError::NotFound
                ),
                "unauthorized doc sync should fail cleanly, got {err:?}"
            );
        }
    }

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn granted_doc_sync_materializes_without_manual_keyhive_sync() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 101, true).await?;
    let client = SyncRepoNode::boot(client_path, 102, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
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

    owner
        .repo
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Read)
        .await?;

    owner
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;

    timeout(
        Duration::from_secs(5),
        client_conn.sync_doc_with_peer(doc_id, Some(Duration::from_secs(2))),
    )
    .await
    .expect("timed out waiting for pre-grant doc sync")?;

    assert!(
        matches!(
            client.repo.get_doc(&missing_doc_id).await?,
            DocLookup::Missing
        ),
        "missing docs should still be reported as missing"
    );
    match client.repo.get_doc(&doc_id).await? {
        DocLookup::Ready(doc) => {
            let title = doc
                .with_document_read(|doc| get_str_at_root(doc, "title"))
                .await;
            assert_eq!(title, "pending");
        }
        DocLookup::PendingMaterialization => {
            panic!("granted client should materialize after doc sync catches keyhive up")
        }
        DocLookup::Missing => panic!("granted client should have synced document bytes"),
    }
    match client.repo.get_doc(&doc_id).await? {
        DocLookup::Ready(handle) => assert!(!handle.export().await.is_empty()),
        DocLookup::PendingMaterialization => {
            panic!("granted client should export after doc sync catches keyhive up")
        }
        DocLookup::Missing => panic!("granted client should have synced document bytes"),
    }

    owner.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn grant_doc_access_checkpoint_becomes_visible_after_reopen_and_keyhive_sync() -> Res<()> {
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 131, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 132, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
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
        .grant_doc_access(doc_id, group.clone(), keyhive_core::access::Access::Read)
        .await?;

    owner_conn.sync_keyhive_with_peer(None).await?;

    client_conn
        .sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT))
        .await?;

    let client_kh_before_shutdown = client.repo.keyhive().clone_keyhive();
    let client_kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
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
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

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
    let temp_root = tempdir()?;
    let owner_path = temp_root.path().join("owner");
    let client_path = temp_root.path().join("client");
    let owner = SyncRepoNode::boot(owner_path, 121, true).await?;
    let client = SyncRepoNode::boot(client_path.clone(), 122, false).await?;

    client.connect_to(&owner).await?;
    owner.wait_for_accepts(1).await;
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;

    owner_conn.sync_keyhive_with_peer(None).await?;

    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
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
        .grant_doc_access(doc_id, group.clone(), keyhive_core::access::Access::Read)
        .await?;

    owner_conn.sync_keyhive_with_peer(None).await?;

    client_conn
        .sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT))
        .await?;
    let client_doc = wait_for_doc_handle(&client.repo, doc_id).await;
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
        client_path.join(crate::keyhive_storage::KEYHIVE_SUBDIR),
    )?;
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
    let owner_conn = owner.accepted_connection().await;
    let client_conn = client.connection_to(&owner).await;
    let owner_kh_peer_id = KeyhivePeerId::from_bytes(*owner.peer_id().as_bytes());
    let grantee_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
    owner_conn.sync_keyhive_with_peer(None).await?;
    let reopened_kh = client.repo.keyhive().clone_keyhive();
    let doc_id_bytes = doc_id.into_bytes();
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
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client_conn
        .sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT))
        .await?;
    assert!(
        !client.big_sync_store.obj_parts(doc_id).await?.is_empty(),
        "reopened client should retain big_sync part registration for the doc"
    );
    assert!(
        client.repo.doc_payload_heads(doc_id).await?.is_some(),
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
            doc_id: Some(BigRepoDocIdFilter::new(first_doc_id)),
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
            doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
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
            doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
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

    assert!(timeout(Duration::from_millis(250), change_rx.recv())
        .await
        .is_err());
    assert!(timeout(Duration::from_millis(250), head_rx.recv())
        .await
        .is_err());
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
    let handle = repo.get_doc(&doc_id).await?.into_ready(doc_id)?;

    let (_change_registration, mut change_rx) = repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
            origin: Some(BigRepoOriginFilter::Remote),
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
        .with_document_with_origin(
            |doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "remote-after"))
                    .expect("failed mutating remote doc");
            },
            BigRepoChangeOrigin::Remote {
                peer_id: PeerId::new([9_u8; 32]),
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
        [super::changes::BigRepoHeadNotification::DocHeadsChanged {
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

fn sync_test_part() -> PartId {
    PartId(Byte32Id::new([
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
        32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
    ]))
}

fn sync_test_parts() -> Vec<PartId> {
    vec![sync_test_part()]
}

fn sync_test_parts_multi() -> Vec<PartId> {
    vec![sync_test_part(), PartId(Byte32Id::new([7; 32]))]
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
            let payload_heads = repo.doc_payload_heads(doc_id).await.unwrap_or(None);
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
    owner_conn.sync_keyhive_with_peer(None).await?;

    let doc = new_sync_doc(owner_actor, value);
    use subduction_keyhive::KeyhivePeerId;
    let grantee_kh_peer_id = KeyhivePeerId::from_bytes(*grantee.peer_id().as_bytes());
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

    owner_conn.sync_keyhive_with_peer(None).await?;

    Ok(handle)
}

#[derive(Clone, Debug)]
struct SubductionProtocolHandler {
    repo: Arc<BigRepo>,
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
            .accept_connection_iroh(conn, None)
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


struct StressBigSyncRpcClient {
    target_part_store: SharedPartStore,
}

#[async_trait::async_trait]
impl big_sync::rpc::HostBigRpcClient for StressBigSyncRpcClient {
    async fn peer_summary(
        &self,
        req: big_sync_core::rpc::PeerSummaryRequest,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<big_sync_core::rpc::PeerSummaryResult, big_sync_core::rpc::ListPartsError>,
        >,
    > {
        let parts = self.target_part_store.summarize_parts(req.parts).await??;
        Ok(Ok(Ok(big_sync_core::rpc::PeerSummaryResult {
            parts,
            deepest_bucket_level: big_sync_core::BuckId::MAX_LEVEL,
        })))
    }

    async fn sub_parts(
        &self,
        req: big_sync_core::rpc::SubPartsRequest,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<
                big_sync_core::mpsc::Receiver<big_sync_core::rpc::SubEvent>,
                big_sync_core::rpc::ListPartsError,
            >,
        >,
    > {
        Ok(Ok(self.target_part_store.subscribe(req).await?))
    }

    async fn get_changed_buckets(
        &self,
        req: big_sync_core::rpc::GetChangedBucketsRequest,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<Vec<big_sync_core::rpc::BucketSummary>, big_sync_core::rpc::ListPartsError>,
        >,
    > {
        Ok(Ok(self.target_part_store.get_changed_buckets(req).await?))
    }

    async fn leaf_buckets(
        &self,
        req: big_sync_core::rpc::LeafBucketsRequest,
    ) -> Res<
        big_sync_core::rpc::BigSyncRpcResult<
            Result<big_sync_core::rpc::LeafBucketResult, big_sync_core::rpc::LeafBucketsError>,
        >,
    > {
        Ok(Ok(self.target_part_store.leaf_buckets(req).await?))
    }
}

async fn endpoint_addr_from_remote_info(
    endpoint: &iroh::Endpoint,
    endpoint_id: iroh::PublicKey,
) -> Res<iroh::EndpointAddr> {
    let remote_info = endpoint
        .remote_info(endpoint_id)
        .await
        .ok_or_eyre("unable to get remote endpoint info")?;
    Ok(iroh::EndpointAddr::from_parts(
        remote_info.id(),
        remote_info.into_addrs().map(|addr| addr.into_addr()),
    ))
}

struct SyncRepoNode {
    path: PathBuf,
    repo: Arc<BigRepo>,
    big_sync_store: SharedPartStore,
    big_sync_worker: big_sync::BigSyncWorkerHandle,
    docs: Arc<tokio::sync::Mutex<HashMap<ObjId, Arc<BigDocHandle>>>>,
    /// Maps stress framework ObjIds to keyhive-generated DocumentIds.
    obj_doc_ids: Arc<tokio::sync::Mutex<HashMap<ObjId, DocumentId>>>,
    connections: Arc<tokio::sync::Mutex<HashMap<PeerId, BigRepoConnection>>>,
    stop_token: BigRepoStopToken,
    endpoint: iroh::Endpoint,
    router: iroh::protocol::Router,
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
        std::fs::create_dir_all(&path)
            .wrap_err_with(|| format!("failed creating sync repo path: {}", path.display()))?;
        let sqlite_url = format!("sqlite://{}", path.join("part_store.db").display());
        let (big_sync_host, big_sync_stop) = boot_part_store(&sqlite_url).await?;
        let part_init_obj = ObjId(big_sync_core::Byte32Id::new(
            [255_u8.wrapping_sub(seed); 32],
        ));
        big_sync_host
            .store
            .set_obj_payload(
                part_init_obj,
                serde_json::json!({ "heads": Vec::<String>::new() }),
            )
            .await?;
        big_sync_host
            .store
            .remove_obj_from_part(part_init_obj, stress_support::test_part())
            .await?;
        let node_identity_seed = [seed; 32];
        let (repo, stop_token) = BigRepo::boot(
            Config {
                node_identity_seed,
                storage: StorageConfig::Disk { path: path.clone() },
            },
            Arc::clone(&big_sync_host.store),
        )
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
            BigRepoSyncBackend::boot(Arc::downgrade(&repo), endpoint.clone())
                .await
                .wrap_err("failed booting big repo sync backend")?,
        );
        let mut sync_backends = HashMap::new();
        sync_backends.insert(BigRepo::BACKEND_ID.into(), Arc::clone(&sync_backend) as _);
        let (big_sync_worker, big_sync_stop) =
            big_sync::spawn_big_sync_worker(Arc::clone(&big_sync_host.store), sync_backends)?;

        let accept_count = Arc::new(AtomicUsize::new(0));
        let accept_notify = Arc::new(Notify::new());
        let accepted_connection = Arc::new(tokio::sync::Mutex::new(None));
        let docs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let obj_doc_ids = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let connections = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                SubductionProtocolHandler {
                    repo: Arc::clone(&repo),
                    track_accepts: accept_incoming,
                    accept_count: Arc::clone(&accept_count),
                    accept_notify: Arc::clone(&accept_notify),
                    accepted_connection: Arc::clone(&accepted_connection),
                },
            )
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
            docs,
            obj_doc_ids,
            connections,
            stop_token,
            big_sync_stop,
            endpoint,
            router,
            accept_count,
            accept_notify,
            accepted_connection,
            sync_backend,
        })
    }

    fn peer_id(&self) -> PeerId {
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

    async fn accepted_connection(&self) -> BigRepoConnection {
        self.accepted_connection
            .lock()
            .await
            .clone()
            .expect("expected accepted connection to be available")
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
        let remote_addr = endpoint_addr_from_remote_info(&self.endpoint, remote.endpoint.id())
            .await
            .unwrap_or_else(|_| remote.endpoint.addr());
        self.sync_backend
            .register_remote_peer(remote.peer_id(), remote_addr);
        let self_addr = endpoint_addr_from_remote_info(&remote.endpoint, self.endpoint.id())
            .await
            .unwrap_or_else(|_| self.endpoint.addr());
        remote
            .sync_backend
            .register_remote_peer(self.peer_id(), self_addr);
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part_id| (part_id, BigRepo::BACKEND_ID.into()))
            .collect();
        self.big_sync_worker
            .set_peer(
                remote.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&remote.big_sync_store),
                }),
                parts,
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
                }),
                parts,
            )
            .await?;
        self.connections.lock().await.insert(remote.peer_id(), conn);
        Ok(())
    }

    async fn disconnect_from(&self, remote: &SyncRepoNode) -> Res<()> {
        if let Some(conn) = self.connections.lock().await.remove(&remote.peer_id()) {
            conn.stop().await?;
        }
        self.sync_backend.unregister_remote_peer(remote.peer_id());
        remote.sync_backend.unregister_remote_peer(self.peer_id());
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

    async fn update_payload(&self, doc_id: DocumentId, payload: serde_json::Value) -> Res<()> {
        let handle = {
            let mut docs = self.docs.lock().await;
            if let Some(handle) = docs.get(&doc_id) {
                Arc::clone(handle)
            } else {
                let handle = match self.repo.get_doc(&doc_id).await? {
                    DocLookup::Ready(handle) => handle,
                    DocLookup::PendingMaterialization => {
                        return Err(ferr!(
                            "stress doc is present but pending materialization: {doc_id}"
                        ));
                    }
                    DocLookup::Missing => {
                        return Err(ferr!("stress doc is not available locally: {doc_id}"));
                    }
                };
                let handle = Arc::new(handle);
                docs.insert(doc_id, Arc::clone(&handle));
                handle
            }
        };

        handle
            .with_document(|doc| {
                doc.transact(|tx| {
                    autosurgeon::reconcile(tx, ThroughJson(payload.clone()))
                        .expect("failed updating big repo stress doc");
                    eyre::Ok(())
                })
                .expect("failed updating big repo stress doc");
            })
            .await?;
        self.repo
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        Ok(())
    }

    async fn snapshot_docs(&self, all_docs: &[ObjId]) -> Res<BigRepoStressObservation> {
        let worker = self.big_sync_worker.snapshot().await?;
        let mut sync_store = BTreeMap::new();
        let mut memberships = BTreeMap::new();
        let mapping = self.obj_doc_ids.lock().await;
        for obj_id in all_docs {
            let doc_id = mapping.get(obj_id).copied().unwrap_or(*obj_id);
            let heads = self.repo.big_sync_store.obj_payload(doc_id).await?;
            let obj_parts = self.repo.big_sync_store.obj_parts(doc_id).await?;
            sync_store.insert(*obj_id, heads);
            memberships.insert(*obj_id, obj_parts);
        }
        let connected_peers = self.connections.lock().await.keys().copied().collect();
        Ok(BigRepoStressObservation {
            connected_peers,
            worker,
            sync_store,
            parts: memberships,
        })
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
        drop(self.router);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct BigRepoStressObservation {
    connected_peers: BTreeSet<PeerId>,
    worker: big_sync::WorkerSnapshot,
    sync_store: BTreeMap<ObjId, Option<serde_json::Value>>,
    parts: BTreeMap<ObjId, Vec<PartId>>,
}

impl PartialEq for BigRepoStressObservation {
    fn eq(&self, other: &Self) -> bool {
        // self.connected_peers == other.connected_peers
        //     && self.worker == other.worker
        self.sync_store == other.sync_store && self.parts == other.parts
    }
}

#[derive(Default, Clone)]
struct BigRepoStressFixture {
    all_docs: Arc<tokio::sync::Mutex<BTreeSet<ObjId>>>,
    doc_ids: Arc<tokio::sync::Mutex<BTreeMap<ObjId, DocumentId>>>,
}

impl BigRepoStressFixture {
    async fn track_doc(&self, obj_id: ObjId) {
        self.all_docs.lock().await.insert(obj_id);
    }

    async fn tracked_doc_ids(&self) -> Vec<ObjId> {
        self.all_docs.lock().await.iter().copied().collect()
    }

    async fn mapped_doc_id(&self, obj_id: ObjId) -> Res<DocumentId> {
        self.doc_ids
            .lock()
            .await
            .get(&obj_id)
            .copied()
            .ok_or_else(|| ferr!("stress object has no mapped document id: {obj_id}"))
    }
}

fn log_slow_fixture_op(label: &str, started_at: std::time::Instant, details: impl std::fmt::Debug) {
    let elapsed = started_at.elapsed();
    if elapsed >= stress_support::SLOW_OP_LOG_THRESHOLD {
        tracing::warn!(
            %label,
            ?elapsed,
            ?details,
            "stress fixture operation took longer than expected"
        );
    }
}

#[async_trait::async_trait]
impl StressFixture for BigRepoStressFixture {
    type World = ();
    type Node = SyncRepoNode;
    type StressObj = ObjId;
    type Observation = BigRepoStressObservation;

    fn label(&self) -> &'static str {
        "big_repo"
    }

    fn make_stress_obj(&self, rng: &mut StdRng) -> Self::StressObj {
        stress_support::stress_obj(rng)
    }

    fn make_doc_content(
        &self,
        phase: &str,
        step: usize,
        node_idx: usize,
        obj: &Self::StressObj,
        nonce: u64,
        _written_at: u64,
        _writer_id: PeerId,
    ) -> serde_json::Value {
        serde_json::json!({
            "phase": phase,
            "step": step,
            "node": node_idx,
            "obj": format!("{obj:?}"),
            "nonce": nonce,
        })
    }

    async fn boot_node(&self, _world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node> {
        let path = tempfile::tempdir()?.keep();
        SyncRepoNode::boot(path, peer_seed, true).await
    }

    async fn stop_node(&self, node: Self::Node) -> Res<()> {
        node.shutdown().await
    }

    async fn restart_node(
        &self,
        _world: Arc<Self::World>,
        peer_seed: u8,
        node: Self::Node,
    ) -> Res<Self::Node> {
        let path = node.path.clone();
        node.shutdown().await?;
        SyncRepoNode::boot(path, peer_seed, true).await
    }

    async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        let started_at = std::time::Instant::now();
        let res = if left.peer_id() <= right.peer_id() {
            left.connect_to(right).await
        } else {
            right.connect_to(left).await
        };
        log_slow_fixture_op(
            "connect_pair",
            started_at,
            (left.peer_id(), right.peer_id()),
        );
        res
    }

    async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        let started_at = std::time::Instant::now();
        let res = if left.peer_id() <= right.peer_id() {
            left.disconnect_from(right).await
        } else {
            right.disconnect_from(left).await
        };
        log_slow_fixture_op(
            "disconnect_pair",
            started_at,
            (left.peer_id(), right.peer_id()),
        );
        res
    }

    async fn seed_new_obj(
        &self,
        node: &Self::Node,
        nodes: &[Option<Self::Node>],
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        let started_at = std::time::Instant::now();
        let creator_peer_id = node.peer_id();
        let peers: Vec<&Self::Node> = nodes
            .iter()
            .flatten()
            .filter(|peer| peer.peer_id() != creator_peer_id)
            .collect();

        for peer in &peers {
            let connect_started_at = std::time::Instant::now();
            peer.connect_to(node).await?;
            log_slow_fixture_op(
                "seed_new_obj:pre_create_connect_to_creator",
                connect_started_at,
                (creator_peer_id, peer.peer_id(), obj),
            );
        }

        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| {
            autosurgeon::reconcile(tx, ThroughJson(payload.clone()))
                .expect("failed seeding big repo stress doc");
            eyre::Ok(())
        })
        .expect("failed seeding big repo stress doc");

        let mut agents = Vec::new();
        for peer in &peers {
            let wait_agent_started_at = std::time::Instant::now();
            let grantee_agent = wait_for_keyhive_agent(&node.repo, peer.peer_id())
                .await?
                .expect("stress grantee agent should be known after connection bootstrap");
            log_slow_fixture_op(
                "seed_new_obj:pre_create_wait_for_agent",
                wait_agent_started_at,
                (creator_peer_id, peer.peer_id(), obj),
            );
            agents.push(grantee_agent);
        }

        let create_doc_started_at = std::time::Instant::now();
        let handle = node.repo.create_doc(doc).await?;
        let doc_id = handle.document_id();
        log_slow_fixture_op(
            "seed_new_obj:create_doc",
            create_doc_started_at,
            (creator_peer_id, obj, doc_id),
        );

        for agent in agents {
            let grant_started_at = std::time::Instant::now();
            node.repo
                .grant_doc_access(doc_id, agent, keyhive_core::access::Access::Edit)
                .await?;
            log_slow_fixture_op(
                "seed_new_obj:grant_peer_edit_access",
                grant_started_at,
                (creator_peer_id, obj, doc_id),
            );
        }

        for peer in &peers {
            let connect_started_at = std::time::Instant::now();
            node.connect_to(peer).await?;
            log_slow_fixture_op(
                "seed_new_obj:post_create_connect_to_peer",
                connect_started_at,
                (creator_peer_id, peer.peer_id(), obj, doc_id),
            );

            let keyhive_sync_started_at = std::time::Instant::now();
            let creator_conn = node.connection_to(peer).await;
            let peer_conn = peer.connection_to(node).await;
            creator_conn.sync_keyhive_with_peer(None).await?;
            log_slow_fixture_op(
                "seed_new_obj:post_create_sync_keyhive",
                keyhive_sync_started_at,
                (creator_peer_id, peer.peer_id(), obj, doc_id),
            );

            let wait_keyhive_access_started_at = std::time::Instant::now();
            wait_for_keyhive_document_access(
                &peer.repo,
                doc_id,
                peer.peer_id(),
                keyhive_core::access::Access::Edit,
            )
            .await?;
            log_slow_fixture_op(
                "seed_new_obj:post_create_wait_for_keyhive_edit_access",
                wait_keyhive_access_started_at,
                (creator_peer_id, peer.peer_id(), obj, doc_id),
            );
        }

        let creator_part_registration_started_at = std::time::Instant::now();
        node.big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        log_slow_fixture_op(
            "seed_new_obj:part_registration_creator",
            creator_part_registration_started_at,
            (creator_peer_id, obj, doc_id),
        );

        for peer in &peers {
            let peer_part_registration_started_at = std::time::Instant::now();
            peer.big_sync_store
                .add_obj_to_parts(doc_id, stress_support::test_parts())
                .await?;
            log_slow_fixture_op(
                "seed_new_obj:part_registration_peer",
                peer_part_registration_started_at,
                (creator_peer_id, peer.peer_id(), obj, doc_id),
            );
        }

        for peer in &peers {
            let sync_doc_started_at = std::time::Instant::now();
            let conn = peer.connection_to(node).await;
            conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT))
                .await?;
            log_slow_fixture_op(
                "seed_new_obj:sync_doc_with_peer",
                sync_doc_started_at,
                (creator_peer_id, peer.peer_id(), obj, doc_id),
            );
        }

        self.doc_ids.lock().await.insert(*obj, doc_id);
        self.track_doc(doc_id).await;
        log_slow_fixture_op("seed_new_obj", started_at, (node.peer_id(), obj));
        Ok(())
    }

    async fn seed_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        let started_at = std::time::Instant::now();
        let doc_id = self.mapped_doc_id(*obj).await?;
        self.track_doc(doc_id).await;
        let res = node.update_payload(doc_id, payload).await;
        log_slow_fixture_op("seed_obj", started_at, (node.peer_id(), obj));
        res
    }

    async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation> {
        let started_at = std::time::Instant::now();
        let all_docs = self.tracked_doc_ids().await;
        let res = node.snapshot_docs(&all_docs).await;
        log_slow_fixture_op(
            "observed_state",
            started_at,
            (node.peer_id(), all_docs.len()),
        );
        res
    }

    fn peer_id(&self, node: &Self::Node) -> PeerId {
        node.peer_id()
    }

    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
        let started_at = std::time::Instant::now();
        let peer_ids: Vec<PeerId> = nodes.iter().map(|node| node.peer_id()).collect();
        let part_ids = stress_support::test_parts();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        let full_sync_timeout = Duration::from_secs(20);
        let mut last_snapshots: Option<Vec<(PeerId, BigRepoStressObservation)>> = None;
        let mut stable_rounds = 0usize;

        for node in nodes {
            let node_peer_id = node.peer_id();
            let peers = peer_ids
                .iter()
                .copied()
                .filter(|peer_id| *peer_id != node_peer_id)
                .collect::<Vec<_>>();
            let parts = part_ids.clone();
            let wait = node
                .big_sync_worker
                .wait_for_full_sync(peers.iter().copied(), parts.iter().copied());
            if tokio::time::timeout(full_sync_timeout, wait).await.is_err() {
                let worker = node.big_sync_worker.snapshot().await?;
                let observed = self.observed_state(node).await?;
                let mut out = String::new();
                let _ = writeln!(
                    out,
                    "timed out waiting for full sync on peer {node_peer_id:?} after {full_sync_timeout:?}"
                );
                let _ = writeln!(out, "requested peers={peers:?} parts={parts:?}");
                let _ = writeln!(out, "worker snapshot={worker:#?}");
                let _ = writeln!(out, "observed state={observed:#?}");
                eyre::bail!("{out}");
            }
        }

        loop {
            let mut snapshots = Vec::with_capacity(nodes.len());
            for node in nodes {
                snapshots.push((node.peer_id(), self.observed_state(node).await?));
            }

            let aligned = snapshots.windows(2).all(|pair| pair[0].1 == pair[1].1);
            if aligned
                && last_snapshots
                    .as_ref()
                    .is_some_and(|prev| prev == &snapshots)
            {
                stable_rounds += 1;
                if stable_rounds >= 5 {
                    log_slow_fixture_op(
                        "assert_cluster_alignment",
                        started_at,
                        nodes.iter().map(|node| node.peer_id()).collect::<Vec<_>>(),
                    );
                    return Ok(());
                }
            } else {
                stable_rounds = 0;
            }
            last_snapshots = Some(snapshots.clone());

            if std::time::Instant::now() >= deadline {
                let mut out = String::new();
                let _ = writeln!(
                    out,
                    "timed out waiting for big repo cluster alignment; last snapshots:"
                );
                if let Some((baseline_peer, baseline)) = snapshots.first() {
                    for (peer_id, snapshot) in snapshots.iter().skip(1) {
                        let _ = writeln!(out, "peer {peer_id:?} vs baseline {baseline_peer:?}:");
                        let _ = writeln!(
                            out,
                            "  baseline vs snapshot sync_store {}",
                            pretty_assertions::Comparison::new(
                                &baseline.sync_store,
                                &snapshot.sync_store
                            )
                        );
                        let _ = writeln!(
                            out,
                            "  baseline vs snapshot parts {}",
                            pretty_assertions::Comparison::new(&baseline.parts, &snapshot.parts)
                        );
                        let differing_sync_store = baseline
                            .sync_store
                            .iter()
                            .filter_map(|(obj_id, left_payload)| {
                                let right_payload = snapshot.sync_store.get(obj_id)?;
                                if left_payload == right_payload {
                                    None
                                } else {
                                    Some((*obj_id, left_payload, right_payload))
                                }
                            })
                            .take(12)
                            .collect::<Vec<_>>();
                        let differing_parts = baseline
                            .parts
                            .iter()
                            .filter_map(|(obj_id, left_parts)| {
                                let right_parts = snapshot.parts.get(obj_id)?;
                                if left_parts == right_parts {
                                    None
                                } else {
                                    Some((*obj_id, left_parts, right_parts))
                                }
                            })
                            .take(12)
                            .collect::<Vec<_>>();
                        let _ = writeln!(
                            out,
                            "  differing sync_store entries={differing_sync_store:?}"
                        );
                        let _ = writeln!(out, "  differing parts entries={differing_parts:?}");
                        let missing_sync_store = baseline
                            .sync_store
                            .keys()
                            .filter(|obj_id| !snapshot.sync_store.contains_key(obj_id))
                            .take(12)
                            .collect::<Vec<_>>();

                        let extra_sync_store = snapshot
                            .sync_store
                            .keys()
                            .filter(|obj_id| !baseline.sync_store.contains_key(obj_id))
                            .take(12)
                            .collect::<Vec<_>>();

                        let _ = writeln!(out, "  missing sync_store keys={missing_sync_store:?}");
                        let _ = writeln!(out, "  extra sync_store keys={extra_sync_store:?}");

                        let missing_parts = baseline
                            .parts
                            .keys()
                            .filter(|obj_id| !snapshot.parts.contains_key(obj_id))
                            .take(12)
                            .collect::<Vec<_>>();

                        let extra_parts = snapshot
                            .parts
                            .keys()
                            .filter(|obj_id| !baseline.parts.contains_key(obj_id))
                            .take(12)
                            .collect::<Vec<_>>();

                        let _ = writeln!(out, "  missing parts={missing_parts:?}");
                        let _ = writeln!(out, "  extra parts={extra_parts:?}");

                        writeln!(
                            out,
                            "sync_store eq={}",
                            baseline.sync_store == snapshot.sync_store
                        )?;
                        writeln!(
                            out,
                            "sync_store eq={}",
                            baseline.sync_store == snapshot.sync_store
                        )?;
                        writeln!(out, "parts eq={}", baseline.parts == snapshot.parts)?;
                        let left = format!("{:#?}", baseline.sync_store);
                        let right = format!("{:#?}", snapshot.sync_store);
                        writeln!(out, "sync_store debug_eq={}", left == right)?;
                        writeln!(out, "snapshot eq={}", baseline == snapshot)?;
                        let _ = writeln!(
                            out,
                            "  field equality: connected_peers={} worker={} sync_store={} parts={}",
                            baseline.connected_peers == snapshot.connected_peers,
                            baseline.worker == snapshot.worker,
                            baseline.sync_store == snapshot.sync_store,
                            baseline.parts == snapshot.parts,
                        );
                    }
                }
                for node in nodes {
                    let worker = node.big_sync_worker.snapshot().await?;
                    let _ = writeln!(
                        out,
                        "worker peer={:?} task_counts={:?} active_machine_tasks={} active_sync_tasks={} zombie_tasks={} full_sync_waiters={:?}",
                        node.peer_id(),
                        worker.task_counts,
                        worker.active_machine_tasks,
                        worker.active_sync_tasks,
                        worker.zombie_tasks,
                        worker.full_sync_waiters,
                    );
                }
                log_slow_fixture_op(
                    "assert_cluster_alignment",
                    started_at,
                    nodes.iter().map(|node| node.peer_id()).collect::<Vec<_>>(),
                );
                eyre::bail!("{out}");
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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
    let server_conn = server.accepted_connection().await;
    client_conn.sync_keyhive_with_peer(None).await?;
    use subduction_keyhive::KeyhivePeerId;
    let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
    let client_agent = server
        .repo
        .keyhive()
        .get_agent_by_peer_id(&client_kh_peer_id)
        .await?
        .expect("client agent should be known after keyhive sync");
    server
        .repo
        .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Edit)
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
    server_conn.sync_keyhive_with_peer(None).await?;

    // Client syncs doc from server (from empty tree)
    tracing::info!("client pulling doc from server");
    client_conn
        .sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT))
        .await?;
    let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
            timeout(
                SYNC_CASE_TIMEOUT,
                client_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT),),
            ),
            timeout(
                SYNC_CASE_TIMEOUT,
                server_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT),),
            ),
        );
        let () = client_result.expect("timed out waiting for sync_doc_with_peer")?;
        let () = server_result.expect("timed out waiting for reverse sync_doc_with_peer")?;

        drop(client_doc);
        drop(server_doc);

        let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
            client_conn.sync_keyhive_with_peer(None).await?;
            let () = timeout(
                SYNC_CASE_TIMEOUT,
                server_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
            )
            .await
            .expect("timed out waiting for reverse sync_doc_with_peer")?;
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
    let server_conn = server.accepted_connection().await;
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
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    wait_for_pair_full_sync(&server, &client).await?;
    let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
    let () = timeout(
        SYNC_CASE_TIMEOUT,
        client_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
    )
    .await
    .expect("timed out waiting for initial sync_doc_with_peer")?;
    wait_for_json_doc(&client_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
    wait_for_json_doc(&server_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;

    tracing::info!("shutting down server while connection is still live");
    server.shutdown().await?;
    client_conn.stop().await?;

    tracing::info!("rebooting server from the same disk path");
    let server = SyncRepoNode::boot(server_path, 71, true).await?;
    let server_doc = server.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
    let () = timeout(
        SYNC_CASE_TIMEOUT,
        client_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
    )
    .await
    .expect("timed out waiting for reconnect sync_doc_with_peer")?;
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
    let server_conn = server.accepted_connection().await;
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
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    wait_for_pair_full_sync(&server, &client).await?;
    let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    set_doc_actor(&client_doc, automerge::ActorId::from([92_u8; 16])).await?;
    client.disconnect_from(&server).await?;

    let (_change_registration, mut change_rx) = server
        .repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
            origin: Some(BigRepoOriginFilter::Remote),
            path: Vec::new(),
        })
        .await?;
    let (_head_registration, mut head_rx) = server
        .repo
        .change_manager
        .subscribe_head_listener(super::changes::HeadFilter {
            doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
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
    let server_conn = server.accepted_connection().await;

    server_conn.sync_keyhive_with_peer(None).await?;
    let () = timeout(
        SYNC_CASE_TIMEOUT,
        server_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
    )
    .await
    .expect("timed out waiting for remote sync_doc_with_peer")?;

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
        [super::changes::BigRepoHeadNotification::DocHeadsChanged {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Remote { .. },
            ..
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
            doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
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
        [super::changes::BigRepoHeadNotification::DocHeadsChanged {
            doc_id: seen_doc_id,
            origin: BigRepoChangeOrigin::Local,
            ..
        }] if *seen_doc_id == doc_id
    ));

    stale_peer_conn.sync_keyhive_with_peer(None).await?;
    let () = timeout(
        SYNC_CASE_TIMEOUT,
        stale_peer_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
    )
    .await
    .expect("timed out waiting for local sync_doc_with_peer")?;
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
    client
        .sync_backend
        .register_remote_peer(server.peer_id(), server.endpoint.addr());
    server
        .sync_backend
        .register_remote_peer(client.peer_id(), client.endpoint.addr());
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
    sync_part_hints: Vec<PartId>,
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
    let server_conn = server.accepted_connection().await;
    client_conn.sync_keyhive_with_peer(None).await?;

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
        use subduction_keyhive::KeyhivePeerId;
        let client_kh_peer_id = KeyhivePeerId::from_bytes(*client.peer_id().as_bytes());
        let client_agent = server
            .repo
            .keyhive()
            .get_agent_by_peer_id(&client_kh_peer_id)
            .await?
            .expect("client agent should be known after keyhive sync");
        server
            .repo
            .grant_doc_access(doc_id, client_agent, keyhive_core::access::Access::Edit)
            .await?;
    }
    client_conn.sync_keyhive_with_peer(None).await?;

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
    client_conn.sync_keyhive_with_peer(None).await?;

    // Register for sync
    server
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;

    let client_doc = if expect_client_doc {
        client
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        client_conn
            .sync_doc_with_peer(
                doc_id,
                Some(utils_rs::scale_timeout(SYNC_PROPAGATION_TIMEOUT)),
            )
            .await?;
        let doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        set_doc_actor(&doc, automerge::ActorId::from([132_u8; 16])).await?;
        Some(doc)
    } else {
        None
    };

    client.stop_big_sync_with(&server).await?;

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
    client_conn.sync_keyhive_with_peer(None).await?;

    let backend = Arc::clone(&client.sync_backend);
    let local_payload = client.big_sync_store.obj_payload(doc_id).await?;
    let remote_payload = server.big_sync_store.obj_payload(doc_id).await?;
    let expected_parts = if sync_part_hints.is_empty() {
        client.big_sync_store.obj_parts(doc_id).await?
    } else {
        sync_part_hints.clone()
    };
    let scenario = SyncBackendScenario {
        name: "big_repo_sync_backend_case",
        peer_id: server.peer_id(),
        obj_id: doc_id,
        initial_payload: local_payload.clone(),
        initial_parts: sync_part_hints.clone(),
        remote_payload: if remote_payload_missing {
            None
        } else {
            remote_payload.clone()
        },
        expected_outcome: SyncBackendOutcome::Completion(expected_deets.clone()),
        expected_payload: match &expected_deets {
            SyncCompletionDeets::Noop => local_payload.clone(),
            SyncCompletionDeets::ChangedObject | SyncCompletionDeets::AddedMember => {
                remote_payload.clone()
            }
            SyncCompletionDeets::RemovedMember => {
                unreachable!("big repo sync backend should not report RemovedMember")
            }
        },
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

async fn run_sync_backend_remote_payload_missing_changed_case(
    sync_part_hints: Vec<PartId>,
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
    let server_conn = server.accepted_connection().await;
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
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    client
        .big_sync_store
        .add_obj_to_parts(doc_id, stress_support::test_parts())
        .await?;
    wait_for_pair_full_sync(&server, &client).await?;
    let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    set_doc_actor(&client_doc, automerge::ActorId::from([132_u8; 16])).await?;

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

    client_conn.sync_keyhive_with_peer(None).await?;

    client
        .big_sync_store
        .remove_obj_from_part(doc_id, sync_test_part())
        .await?;

    let remote_payload = server.big_sync_store.obj_payload(doc_id).await?;
    // Capture client heads before sync_obj: the big_sync worker may have already
    // delivered the server's mutation, in which case backend.rs's early-Noop
    // (local_heads == remote_heads, pre-sync) is correct.
    let local_heads_pre: Option<Arc<[automerge::ChangeHash]>> =
        super::partition_doc_heads_payload(&client.big_sync_store, doc_id).await?;
    let remote_heads = remote_payload
        .as_ref()
        .map(super::doc_heads_from_payload);
    client.stop_big_sync_with(&server).await?;
    let outcome = client
        .sync_backend
        .sync_obj(client_conn.peer_id(), doc_id, remote_payload.clone())
        .await?;
    let changed_object_ok = matches!(
        outcome,
        big_sync::SyncTaskRunOutcome::Completion(big_sync_core::SyncTaskCompletion {
            deets: SyncCompletionDeets::ChangedObject,
            ..
        })
    );
    let noop_ok = matches!(
        outcome,
        big_sync::SyncTaskRunOutcome::Completion(big_sync_core::SyncTaskCompletion {
            deets: SyncCompletionDeets::Noop,
            ..
        })
    ) && local_heads_pre.as_ref() == remote_heads.as_ref();
    assert!(
        changed_object_ok || noop_ok,
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
    doc_id: ObjId,
) -> Res<()> {
    let left_heads = left.repo.doc_payload_heads(doc_id).await?;
    let right_heads = right.repo.doc_payload_heads(doc_id).await?;
    assert_eq!(
        left_heads, right_heads,
        "payload heads diverged for doc {doc_id:?}"
    );

    let left_parts = left.big_sync_store.obj_parts(doc_id).await?;
    let right_parts = right.big_sync_store.obj_parts(doc_id).await?;
    assert_eq!(
        left_parts, right_parts,
        "part membership diverged for doc {doc_id:?}"
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
        let left_conn = left.accepted_connection().await;
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
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        right
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;

        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        assert_pair_sync_alignment(&left, &right, doc_id).await?;

        right.disconnect_from(&left).await?;
        right.connect_to(&left).await?;
        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
        let left_conn = left.accepted_connection().await;
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
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        right
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;

        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        wait_for_json_doc(&right_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        assert_pair_sync_alignment(&left, &right, doc_id).await?;

        right.disconnect_from(&left).await?;
        right.connect_to(&left).await?;
        wait_for_pair_full_sync(&left, &right).await?;

        wait_for_json_doc(&left_doc, &expected_doc, SYNC_CASE_TIMEOUT).await;
        let right_doc = right.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
        let server_conn = server.accepted_connection().await;
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
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        client
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        wait_for_pair_full_sync(&server, &client).await?;
        let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
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
        let server_conn = server.accepted_connection().await;
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
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        client
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        wait_for_pair_full_sync(&server, &client).await?;
        let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        set_doc_actor(&client_doc, automerge::ActorId::from([112_u8; 16])).await?;

        let (_change_registration, mut change_rx) = server
            .repo
            .subscribe_change_listener(BigRepoChangeFilter {
                doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
                origin: Some(BigRepoOriginFilter::Remote),
                path: Vec::new(),
            })
            .await?;
        let (_head_registration, mut head_rx) = server
            .repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
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

        server_conn.sync_keyhive_with_peer(None).await?;
        timeout(
            SYNC_CASE_TIMEOUT,
            server_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
        )
        .await
        .expect("timed out waiting for remote sync_doc_with_peer")?;

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
            [super::changes::BigRepoHeadNotification::DocHeadsChanged {
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
        let server_conn = server.accepted_connection().await;
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
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        client
            .big_sync_store
            .add_obj_to_parts(doc_id, stress_support::test_parts())
            .await?;
        wait_for_pair_full_sync(&server, &client).await?;
        let client_doc = client.repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
        set_doc_actor(&client_doc, automerge::ActorId::from([122_u8; 16])).await?;

        let (_head_registration, mut head_rx) = client
            .repo
            .change_manager
            .subscribe_head_listener(super::changes::HeadFilter {
                doc_id: Some(super::changes::DocIdFilter::new(doc_id)),
            })
            .await?;
        assert!(
            !client
                .repo
                .change_manager
                .has_change_listener_interest(doc_id, &BigRepoChangeOrigin::Local),
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
            [super::changes::BigRepoHeadNotification::DocHeadsChanged {
                doc_id: seen_doc_id,
                origin: BigRepoChangeOrigin::Local,
                ..
            }] if *seen_doc_id == doc_id
        ));

        server_conn.sync_keyhive_with_peer(None).await?;
        timeout(
            SYNC_CASE_TIMEOUT,
            server_conn.sync_doc_with_peer(doc_id, Some(SYNC_PROPAGATION_TIMEOUT)),
        )
        .await
        .expect("timed out waiting for local sync_doc_with_peer")?;

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

#[tokio::test(flavor = "multi_thread")]
async fn big_repo_sync_randomized_four_node_stress_converges() -> Res<()> {
    stress_support::run_randomized_four_node_stress_with_settle_timeout(
        BigRepoStressFixture::default(),
        Arc::new(()),
        stress_support::PHASE1_MUTATIONS,
        stress_support::PHASE2_MUTATIONS,
        stress_support::PHASE3_MUTATIONS,
        Duration::from_secs(20),
    )
    .await
}
