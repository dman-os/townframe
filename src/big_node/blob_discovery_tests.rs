use super::blob_discovery_net::Network;
use super::*;
use big_blobs::Coverage;
use big_blobs::InventorySelection;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::*;
use utils_rs::prelude::tracing;

async fn collection(node: &BigNode, name: &str) -> DocId {
    node.drawer_access
        .grants()
        .grant(
            &RequestContext::root(),
            node.drawers_actor.actor_id,
            CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let cap = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DrawerScope {
                    create: true,
                    ..Default::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.drawers
        .client
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: cap,
                command: name.to_owned().try_into().unwrap(),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .unwrap()
        .drawer_id
}

async fn child(node: &BigNode, parent: &DocId, child: &DocId) {
    let cap = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DrawerScope {
                    create: false,
                    drawers: BTreeMap::from([
                        (parent.clone(), Access::Admin),
                        (child.clone(), Access::Admin),
                    ]),
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.drawers
        .client
        .call(
            RequestContext::root(),
            AddChild {
                capability: cap,
                command: format!("link-{child}").try_into().unwrap(),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
}

async fn populated(node: &BigNode, drawer: &DocId, name: &str, root: MetaRoot) -> (DocId, BlobId) {
    tracing::info!(%drawer, %root, name, "populate inventory begin");
    let inventory = blobs::inventory(node, drawer, root, name).await;
    tracing::info!(%inventory, "inventory reference indexed");
    let bytes = format!("independently owned bytes for {drawer}/{inventory}").into_bytes();
    let imported = node
        .blobs
        .service
        .import_stream(futures::stream::once(async move { Ok(bytes.into()) }))
        .await
        .unwrap();
    let placement = format!("drawer {drawer}");
    let document = node.docs.client.call(RequestContext::root(), CreateDoc {
        capability: capability(node, DocScope { create: BTreeSet::from([placement.clone()]), ..Default::default() }).await,
        command: format!("content-{inventory}").try_into().unwrap(), placement,
        datums: BTreeMap::from([(
            DatumKey::from_text(Blob::ID, "main").unwrap(),
            json!({"mime":"application/octet-stream", "digest": imported.info.blob_id.digest_str(), "lengthOctets": imported.info.length_octets}),
        )]),
    }).await.unwrap();
    tracing::info!(document = %document.doc_id, %inventory, "content created; awaiting inventory publication");
    let mut datums = node.indexes.datum_set.open_all(0).await.unwrap();
    let mut changes = node
        .blob_workers
        .parts
        .open_revision_reader_all(0)
        .await
        .unwrap()
        .unwrap();
    let digest = loop {
        let content = blobs::all_datums(node, &document.doc_id).await;
        let digest = if root == MetaRoot::Sensitive {
            Some(imported.info.blob_id)
        } else {
            content.datums.iter().find_map(|(key, datum)| {
                if key.tag().as_str() != big_blobs_core::CipherBlob::ID {
                    return None;
                }
                let DatumRead::Accepted { value, .. } = datum else {
                    return None;
                };
                let cipher: big_blobs_core::CipherBlob =
                    serde_json::from_value(value.clone()).unwrap();
                Some(cipher.representation.digest.parse().unwrap())
            })
        };
        if let Some(digest) = digest
            && node
                .blob_workers
                .parts
                .obj_parts(digest.into())
                .await
                .unwrap()
                .contains(&big_blobs::workers::inventory_part(&inventory))
        {
            break digest;
        }
        tokio::select! {
            read = changes.next(RevisionReadLimits::default()) => { read.unwrap(); }
            read = datums.next(RevisionReadLimits::default()) => { read.unwrap(); }
        }
    };
    tracing::info!(%inventory, %digest, "inventory publication complete");
    node.blobs.service.release_import(&imported).await.unwrap();
    (inventory, digest)
}

async fn replace_slot(node: &BigNode, drawer: &DocId, document: &DocId) {
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: capability(
                    node,
                    DocScope {
                        write: BTreeSet::from([drawer.clone()]),
                        ..Default::default()
                    },
                )
                .await,
                command: None,
                doc_id: drawer.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(
                    DatumKey::from_text(
                        big_cabinet_core::drawer::DrawerMetaDoc::ID,
                        "blobInventory/ciphertext",
                    )
                    .unwrap(),
                    json!({"docId": document.to_string()}),
                )]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
}

async fn wait_bytes(node: &BigNode, inventory: &DocId, digest: BlobId) {
    let mut changes = node
        .blob_workers
        .parts
        .open_revision_reader_all(0)
        .await
        .unwrap()
        .unwrap();
    loop {
        if node
            .blob_workers
            .parts
            .obj_parts(digest.into())
            .await
            .unwrap()
            .contains(&big_blobs::workers::inventory_part(inventory))
        {
            return;
        }
        changes.next(RevisionReadLimits::default()).await.unwrap();
    }
}

async fn self_doc(node: &BigNode, parent: &DocId, inventory: &DocId) -> BlobId {
    let placement = format!("drawer {parent}");
    let document = node
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability: capability(
                    node,
                    DocScope {
                        create: BTreeSet::from([placement.clone()]),
                        ..Default::default()
                    },
                )
                .await,
                command: "self-child-content".to_owned().try_into().unwrap(),
                placement,
                datums: BTreeMap::from([(
                    key(),
                    json!({"$type": NOTE, "value":"a self-doc child"}),
                )]),
            },
        )
        .await
        .unwrap()
        .doc_id;
    let cap = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DrawerScope {
                    create: true,
                    drawers: BTreeMap::from([(parent.clone(), Access::Admin)]),
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.drawers
        .client
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: cap,
                command: "self-child-declaration".to_owned().try_into().unwrap(),
                layout: CreateLayout::SelfDoc {
                    doc_id: document.clone(),
                },
            },
        )
        .await
        .unwrap();
    node.drawers
        .client
        .call(
            RequestContext::root(),
            RemoveDoc {
                capability: blobs::drawer_cap(node, parent).await,
                command: "self-child-remove-direct".to_owned().try_into().unwrap(),
                drawer_id: parent.clone(),
                doc_id: document.clone(),
            },
        )
        .await
        .unwrap();
    child(node, parent, &document).await;
    let imported = node
        .blobs
        .service
        .import_stream(futures::stream::once(async {
            Ok(b"self-doc real network bytes".to_vec().into())
        }))
        .await
        .unwrap();
    node.docs.client.call(RequestContext::root(), WriteDatums {
        capability: capability(node, DocScope { write: BTreeSet::from([document.clone()]), ..Default::default() }).await,
        command: None, doc_id: document.clone(), mode: WriteMode::AtCurrent,
        set: BTreeMap::from([(DatumKey::from_text(Blob::ID, "self").unwrap(), json!({
            "mime":"application/octet-stream", "digest":imported.info.blob_id.digest_str(), "lengthOctets":imported.info.length_octets,
        }))]), remove: BTreeSet::new(),
    }).await.unwrap();
    let mut datums = node.indexes.datum_set.open_all(0).await.unwrap();
    let mut changes = node
        .blob_workers
        .parts
        .open_revision_reader_all(0)
        .await
        .unwrap()
        .unwrap();
    let digest = loop {
        let data = blobs::all_datums(node, &document).await;
        if let Some(digest) = data.datums.iter().find_map(|(key, datum)| {
            if key.tag().as_str() != big_blobs_core::CipherBlob::ID {
                return None;
            }
            let DatumRead::Accepted { value, .. } = datum else {
                return None;
            };
            let cipher: big_blobs_core::CipherBlob = serde_json::from_value(value.clone()).unwrap();
            Some(cipher.representation.digest.parse::<BlobId>().unwrap())
        }) && node
            .blob_workers
            .parts
            .obj_parts(digest.into())
            .await
            .unwrap()
            .contains(&big_blobs::workers::inventory_part(inventory))
        {
            break digest;
        }
        tokio::select! {
            read = changes.next(RevisionReadLimits::default()) => { read.unwrap(); }
            read = datums.next(RevisionReadLimits::default()) => { read.unwrap(); }
        }
    };
    node.blobs.service.release_import(&imported).await.unwrap();
    digest
}

async fn complete(node: &BigNode, root: &DocId) -> Coverage {
    let mut changes = node.blob_workers.mirrors.subscribe_changes();
    loop {
        changes.borrow_and_update();
        let coverage = node.blob_workers.mirrors.coverage(root).await.unwrap();
        tracing::info!(%root, ?coverage, "mirror coverage observed");
        if coverage.is_complete() {
            return coverage;
        }
        changes.changed().await.unwrap();
    }
}

async fn verified(node: &BigNode, inventory: &DocId, digest: BlobId) {
    let bytes = node
        .blobs
        .service
        .store()
        .blobs()
        .get_bytes(iroh_blobs::Hash::from(digest.to_bytes32()))
        .await
        .unwrap();
    assert_eq!(
        iroh_blobs::Hash::new(&bytes),
        iroh_blobs::Hash::from(digest.to_bytes32())
    );
    assert!(
        node.blob_workers
            .parts
            .obj_parts(digest.into())
            .await
            .unwrap()
            .contains(&big_blobs::workers::inventory_part(inventory))
    );
}

async fn invite(owner: &BigNode, reader: &BigNode, root: &DocId, access: Access) {
    owner
        .repo
        .receive_keyhive_contact_card(&reader.repo.local_keyhive_contact_card())
        .await
        .unwrap();
    owner
        .drawers
        .client
        .call(
            RequestContext::root(),
            InvitePrincipal {
                capability: blobs::drawer_cap(owner, root).await,
                drawer_id: root.clone(),
                principal: Principal::Peer {
                    peer_id: GroupKey(reader.repo.local_peer_id().to_bytes32().unwrap()),
                },
                access,
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn selecting_only_a_discovers_deep_inventories_and_reopens_without_reselection() {
    utils_rs::testing::setup_tracing_once();
    let owner_secrets = secrets().await;
    let reader_secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let mut owner_cfg = config(
        &owner_secrets,
        &runtime,
        runtime.ephemeral(&[]).unwrap(),
        StorageConfig::Memory,
    );
    owner_cfg.repo.node_identity_seed = [141; 32];
    let (owner, owner_stop) = BigNode::boot(owner_cfg).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let reader_sql = runtime.database(
        dir.path().join("reader.sqlite"),
        &[],
        "_sqlite_rs_migrations",
    );
    let reader_config = || {
        let mut cfg = config(
            &reader_secrets,
            &runtime,
            reader_sql.clone(),
            StorageConfig::Disk {
                path: dir.path().join("repo"),
            },
        );
        cfg.repo.node_identity_seed = [142; 32];
        cfg.blob_storage = big_blobs::Storage::Filesystem(dir.path().join("blobs"));
        cfg
    };
    let (reader, reader_stop) = BigNode::boot(reader_config()).await.unwrap();
    let a = collection(&owner, "A").await;
    let b = collection(&owner, "B").await;
    let c = collection(&owner, "C").await;
    let unrelated = collection(&owner, "unrelated").await;
    child(&owner, &a, &b).await;
    child(&owner, &b, &c).await;
    let mut expected = Vec::new();
    for drawer in [&a, &b, &c] {
        expected.push(populated(&owner, drawer, "ciphertext", MetaRoot::Public).await);
    }
    let self_digest = self_doc(&owner, &a, &expected[0].0).await;
    expected.push((expected[0].0.clone(), self_digest));
    let unused = blobs::inventory(&owner, &b, MetaRoot::Public, "not-selected").await;
    let hidden = populated(&owner, &unrelated, "ciphertext", MetaRoot::Public).await;
    let owner_net = Network::boot(&owner, 141).await;
    let mut reader_net = Network::boot(&reader, 142).await;
    let (connection, accepted) =
        Network::link((&owner, &owner_net), (&reader, &mut reader_net)).await;
    invite(&owner, &reader, &a, Access::Read).await;
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    reader.repo.wait_for_keyhive_reconciliation().await.unwrap();
    reader
        .blob_workers
        .mirrors
        .select_drawer(
            a.clone(),
            Some(InventorySelection {
                root: MetaRoot::Public,
                name: "ciphertext".into(),
            }),
        )
        .await
        .unwrap();
    complete(&reader, &a).await;
    for (inventory, digest) in &expected {
        verified(&reader, inventory, *digest).await;
    }
    assert!(
        reader
            .blobs
            .service
            .stat(&hidden.1)
            .await
            .unwrap()
            .is_none()
    );
    let d = collection(&owner, "D").await;
    expected.push(populated(&owner, &d, "ciphertext", MetaRoot::Public).await);
    child(&owner, &c, &d).await;
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    let (inventory, digest) = expected.last().unwrap();
    wait_bytes(&reader, inventory, *digest).await;
    complete(&reader, &a).await;
    for (inventory, digest) in &expected {
        verified(&reader, inventory, *digest).await;
    }
    assert_eq!(
        reader
            .blob_workers
            .parts
            .member_count(big_blobs::workers::inventory_part(&unused))
            .await
            .unwrap(),
        0
    );
    let replacement = populated(&owner, &c, "replacement", MetaRoot::Public).await;
    replace_slot(&owner, &c, &replacement.0).await;
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    wait_bytes(&reader, &replacement.0, replacement.1).await;
    let old = std::mem::replace(&mut expected[2], replacement);
    complete(&reader, &a).await;
    let mut changes = reader.blob_workers.mirrors.subscribe_changes();
    while reader
        .blob_workers
        .parts
        .member_count(big_blobs::workers::inventory_part(&old.0))
        .await
        .unwrap()
        != 0
    {
        changes.changed().await.unwrap();
    }
    drop(changes);
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    reader_net.stop().await;
    shutdown(reader, reader_stop).await;
    let (reader, reader_stop) = BigNode::boot(reader_config()).await.unwrap();
    let mut reader_net = Network::boot(&reader, 142).await;
    let (connection, accepted) =
        Network::link((&owner, &owner_net), (&reader, &mut reader_net)).await;
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    complete(&reader, &a).await;
    for (inventory, digest) in &expected {
        verified(&reader, inventory, *digest).await;
    }
    assert!(
        reader
            .blobs
            .service
            .stat(&hidden.1)
            .await
            .unwrap()
            .is_none()
    );
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    reader_net.stop().await;
    owner_net.stop().await;
    shutdown(reader, reader_stop).await;
    shutdown(owner, owner_stop).await;
}

async fn transitive_matrix(access: Access, root: MetaRoot, name: &str) {
    utils_rs::testing::setup_tracing_once();
    let owner_secrets = secrets().await;
    let reader_secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let mut cfg = config(
        &owner_secrets,
        &runtime,
        runtime.ephemeral(&[]).unwrap(),
        StorageConfig::Memory,
    );
    cfg.repo.node_identity_seed = [146; 32];
    let (owner, owner_stop) = BigNode::boot(cfg).await.unwrap();
    let mut cfg = config(
        &reader_secrets,
        &runtime,
        runtime.ephemeral(&[]).unwrap(),
        StorageConfig::Memory,
    );
    cfg.repo.node_identity_seed = [147; 32];
    let (reader, reader_stop) = BigNode::boot(cfg).await.unwrap();
    let a = collection(&owner, "matrix-A").await;
    let b = collection(&owner, "matrix-B").await;
    let c = collection(&owner, "matrix-C").await;
    child(&owner, &a, &b).await;
    child(&owner, &b, &c).await;
    let mut expected = Vec::new();
    for drawer in [&a, &b, &c] {
        expected.push(populated(&owner, drawer, name, root).await);
    }
    let owner_net = Network::boot(&owner, 146).await;
    let mut reader_net = Network::boot(&reader, 147).await;
    let (connection, accepted) =
        Network::link((&owner, &owner_net), (&reader, &mut reader_net)).await;
    invite(&owner, &reader, &a, access).await;
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    reader.repo.wait_for_keyhive_reconciliation().await.unwrap();
    reader
        .blob_workers
        .mirrors
        .select_drawer(
            a.clone(),
            Some(InventorySelection {
                root,
                name: name.into(),
            }),
        )
        .await
        .unwrap();
    complete(&reader, &a).await;
    for (inventory, digest) in expected {
        verified(&reader, &inventory, digest).await;
    }
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    reader_net.stop().await;
    owner_net.stop().await;
    shutdown(reader, reader_stop).await;
    shutdown(owner, owner_stop).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relay_root_only_grant_fetches_child_and_grandchild_ciphertext() {
    transitive_matrix(Access::Relay, MetaRoot::Public, "ciphertext").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn read_root_only_grant_fetches_child_and_grandchild_plaintext() {
    transitive_matrix(Access::Read, MetaRoot::Sensitive, "plaintext").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relay_discovers_only_public_inventory_and_revocation_preserves_retained_bytes() {
    utils_rs::testing::setup_tracing_once();
    let owner_secrets = secrets().await;
    let relay_secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let mut cfg = config(
        &owner_secrets,
        &runtime,
        runtime.ephemeral(&[]).unwrap(),
        StorageConfig::Memory,
    );
    cfg.repo.node_identity_seed = [143; 32];
    let (owner, owner_stop) = BigNode::boot(cfg).await.unwrap();
    let mut cfg = config(
        &relay_secrets,
        &runtime,
        runtime.ephemeral(&[]).unwrap(),
        StorageConfig::Memory,
    );
    cfg.repo.node_identity_seed = [144; 32];
    let (relay, relay_stop) = BigNode::boot(cfg).await.unwrap();
    let root = collection(&owner, "relay-root").await;
    let public = populated(&owner, &root, "ciphertext", MetaRoot::Public).await;
    let sensitive = populated(&owner, &root, "plaintext", MetaRoot::Sensitive).await;
    let owner_net = Network::boot(&owner, 143).await;
    let mut relay_net = Network::boot(&relay, 144).await;
    let (connection, accepted) =
        Network::link((&owner, &owner_net), (&relay, &mut relay_net)).await;
    invite(&owner, &relay, &root, Access::Relay).await;
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    relay.repo.wait_for_keyhive_reconciliation().await.unwrap();
    relay
        .blob_workers
        .mirrors
        .select_drawer(
            root.clone(),
            Some(InventorySelection {
                root: MetaRoot::Public,
                name: "ciphertext".into(),
            }),
        )
        .await
        .unwrap();
    complete(&relay, &root).await;
    verified(&relay, &public.0, public.1).await;
    let observation = relay
        .drawers
        .observe_metadata(&RequestContext::root(), &root)
        .await
        .unwrap();
    assert_eq!(
        observation.roots[&MetaRoot::Sensitive],
        big_cabinet::drawer::MetadataRootStatus::Denied
    );
    assert!(
        !observation
            .meta_docs
            .values()
            .any(|slots| slots.values().any(|doc| doc == &sensitive.0))
    );
    assert!(
        relay
            .blobs
            .service
            .stat(&sensitive.1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        relay
            .blob_workers
            .parts
            .member_count(big_blobs::workers::inventory_part(&sensitive.0))
            .await
            .unwrap(),
        0
    );
    let read_secrets = secrets().await;
    let mut cfg = config(
        &read_secrets,
        &runtime,
        runtime.ephemeral(&[]).unwrap(),
        StorageConfig::Memory,
    );
    cfg.repo.node_identity_seed = [145; 32];
    let (read, read_stop) = BigNode::boot(cfg).await.unwrap();
    let mut read_net = Network::boot(&read, 145).await;
    let (read_connection, read_accepted) =
        Network::link((&owner, &owner_net), (&read, &mut read_net)).await;
    invite(&owner, &read, &root, Access::Read).await;
    read_connection.sync_keyhive_with_peer().await.unwrap();
    read_accepted.sync_keyhive_with_peer().await.unwrap();
    read.repo.wait_for_keyhive_reconciliation().await.unwrap();
    read.blob_workers
        .mirrors
        .select_drawer(
            root.clone(),
            Some(InventorySelection {
                root: MetaRoot::Sensitive,
                name: "plaintext".into(),
            }),
        )
        .await
        .unwrap();
    complete(&read, &root).await;
    verified(&read, &sensitive.0, sensitive.1).await;
    owner
        .drawers
        .client
        .call(
            RequestContext::root(),
            RevokePrincipal {
                capability: blobs::drawer_cap(&owner, &root).await,
                drawer_id: root.clone(),
                principal: Principal::Peer {
                    peer_id: GroupKey(relay.repo.local_peer_id().to_bytes32().unwrap()),
                },
            },
        )
        .await
        .unwrap();
    connection.sync_keyhive_with_peer().await.unwrap();
    accepted.sync_keyhive_with_peer().await.unwrap();
    relay.repo.wait_for_keyhive_reconciliation().await.unwrap();
    let mut changes = relay.blob_workers.mirrors.subscribe_changes();
    loop {
        changes.borrow_and_update();
        if !relay
            .blob_workers
            .mirrors
            .coverage(&root)
            .await
            .unwrap()
            .is_complete()
            && relay
                .blob_workers
                .parts
                .member_count(big_blobs::workers::inventory_part(&public.0))
                .await
                .unwrap()
                == 0
        {
            break;
        }
        changes.changed().await.unwrap();
    }
    assert!(relay.blobs.service.stat(&public.1).await.unwrap().is_some());
    drop(changes);
    read_connection.stop().await.unwrap();
    read_accepted.stop().await.unwrap();
    read_net.stop().await;
    connection.stop().await.unwrap();
    accepted.stop().await.unwrap();
    relay_net.stop().await;
    owner_net.stop().await;
    shutdown(read, read_stop).await;
    shutdown(relay, relay_stop).await;
    shutdown(owner, owner_stop).await;
}
