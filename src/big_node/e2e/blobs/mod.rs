use super::*;
use big_blobs::BlobService;
use big_blobs_core::BlobInfo;
use big_blobs_core::BlobsError;
use big_blobs_core::PutBlob;
use big_blobs_core::ReadBlob;
use big_blobs_core::StatBlob;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use service_utils_rs::ClientError;

mod nodule_artifacts;

pub(super) async fn drawer_cap(node: &BigNode, drawer: &DocId) -> CapId {
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    create: false,
                    drawers: BTreeMap::from([(drawer.clone(), DrawerAccess::Admin)]),
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id
}

pub(super) async fn inventory(node: &BigNode, drawer: &DocId, root: MetaRoot, name: &str) -> DocId {
    let receipt = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            CreateMetaDoc {
                capability: drawer_cap(node, drawer).await,
                command: format!("inventory-{drawer}-{root}-{name}")
                    .try_into()
                    .unwrap(),
                drawer_id: drawer.clone(),
                root,
                name: name.into(),
                kind: big_blobs::workers::INVENTORY_KIND.into(),
            },
        )
        .await
        .unwrap();
    let root_doc = match root {
        MetaRoot::Public => drawer.clone(),
        MetaRoot::Sensitive => {
            let record = node
                .drawers
                .client
                .call(
                    RequestContext::root(),
                    big_cabinet_core::drawer::protocol::GetDrawer {
                        capability: drawer_cap(node, drawer).await,
                        drawer_id: drawer.clone(),
                    },
                )
                .await
                .unwrap();
            match record.descriptor.layout {
                big_cabinet_core::drawer::DrawerLayout::SingleGroup(layout) => {
                    layout.sensitive_root
                }
                _ => panic!("a grouped drawer has a sensitive root"),
            }
        }
    };
    let datum_key = DatumKey::from_text(
        big_cabinet_core::drawer::DrawerMetaDoc::ID,
        format!("blobInventory/{name}"),
    )
    .unwrap();
    let mut reader = node
        .indexes
        .datum_set
        .open(datum_key.tag().clone(), 0)
        .await
        .unwrap();
    loop {
        let RevisionRead::Entries { entries, .. } =
            reader.next(RevisionReadLimits::default()).await.unwrap()
        else {
            continue;
        };
        if entries.iter().any(|entry| {
            entry.key.doc_id == root_doc
                && entry.key.datum_key == datum_key
                && matches!(
                    &entry.value,
                    Some(DatumMembership::Present { doc_heads, .. }) if *doc_heads == receipt.heads
                )
        }) {
            return receipt.doc_id;
        }
    }
}

pub(super) async fn all_datums(node: &BigNode, document: &DocId) -> DatumsRead {
    node.docs
        .client
        .call(
            RequestContext::root(),
            ReadDatums {
                capability: capability(
                    node,
                    DocScope {
                        read: BTreeSet::from([document.clone()]),
                        ..Default::default()
                    },
                )
                .await,
                doc_id: document.clone(),
                at: At::Current,
                datums: None,
                optimistic: false,
            },
        )
        .await
        .unwrap()
}

pub(super) async fn blob_cap(node: &BigNode, object: String, dependencies: Vec<CapId>) -> CapId {
    node.authority
        .client
        .call(
            RequestContext::root(),
            MintCapability {
                issuer: node.blobs_namespace.capability,
                parent: None,
                dependencies,
                object,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id
}

#[tokio::test]
async fn named_inventory_distribution_is_root_scoped_and_survives_restart() {
    let directory = tempfile::tempdir().unwrap();
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.database(
        directory.path().join("cabinet.sqlite"),
        &[],
        "_sqlite_rs_migrations",
    );
    let mut cfg = config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk {
            path: directory.path().join("repo"),
        },
    );
    cfg.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    let drawer = node.drawers.local().originated_id.clone();
    let mut inventories = Vec::new();
    for (root, name) in [
        (MetaRoot::Public, "ciphertext"),
        (MetaRoot::Public, "backup"),
        (MetaRoot::Sensitive, "plaintext"),
        (MetaRoot::Sensitive, "backup"),
    ] {
        inventories.push((root, inventory(&node, &drawer, root, name).await));
    }
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: blob_cap(&node, BlobService::put_object().into(), Vec::new()).await,
                bytes: b"root-scoped inventory bytes".to_vec(),
            },
        )
        .await
        .unwrap();
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let blob_key = DatumKey::from_text(big_blobs_core::Blob::ID, "main").unwrap();
    let blob_value = json!({
        "mime": "application/octet-stream",
        "digest": info.blob_id.digest_str(),
        "lengthOctets": info.length_octets,
    });
    let doc = node
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability: capability(
                    &node,
                    DocScope {
                        create: BTreeSet::from([placement.clone()]),
                        ..Default::default()
                    },
                )
                .await,
                command: "inventory-content".to_owned().try_into().unwrap(),
                placement,
                datums: BTreeMap::from([(blob_key, blob_value)]),
            },
        )
        .await
        .unwrap();
    let mut changes = node
        .blob_workers
        .parts
        .open_revision_reader_all(0)
        .await
        .unwrap()
        .unwrap();
    loop {
        let mut ready = true;
        for (_, inventory) in &inventories {
            ready &= node
                .blob_workers
                .parts
                .member_count(big_blobs::workers::inventory_part(inventory))
                .await
                .unwrap()
                > 0;
        }
        if ready {
            break;
        }
        changes.next(RevisionReadLimits::default()).await.unwrap();
    }
    let content = all_datums(&node, &doc.doc_id).await;
    let cipher = content
        .datums
        .iter()
        .find_map(|(key, value)| {
            if key.tag().as_str() != big_blobs_core::CipherBlob::ID {
                return None;
            }
            let DatumRead::Accepted { value, .. } = value else {
                return None;
            };
            Some(serde_json::from_value::<big_blobs_core::CipherBlob>(value.clone()).unwrap())
        })
        .unwrap();
    let cipher_id: big_blobs_core::BlobId = cipher.representation.digest.parse().unwrap();
    assert_ne!(cipher_id, info.blob_id);
    for (root, inventory) in &inventories {
        let pins = all_datums(&node, inventory).await;
        let expected = match root {
            MetaRoot::Public => cipher_id,
            MetaRoot::Sensitive => info.blob_id,
        };
        let digests: BTreeSet<_> = pins
            .datums
            .iter()
            .filter(|(key, _)| key.tag().as_str() == big_blobs_core::BlobPin::ID)
            .map(|(key, datum)| {
                let DatumRead::Accepted { value, .. } = datum else {
                    panic!("inventory pins must be accepted: {datum:?}");
                };
                let pin: big_blobs_core::BlobPin = serde_json::from_value(value.clone()).unwrap();
                let expected_length = match root {
                    MetaRoot::Public => cipher.representation.length_octets,
                    MetaRoot::Sensitive => info.length_octets,
                };
                assert_eq!(pin.length_octets, expected_length);
                std::str::from_utf8(key.slot_bytes())
                    .unwrap()
                    .parse::<big_blobs_core::BlobId>()
                    .unwrap()
            })
            .collect();
        assert_eq!(digests, BTreeSet::from([expected]));
        assert_eq!(
            node.blob_workers
                .parts
                .member_count(big_blobs::workers::inventory_part(inventory))
                .await
                .unwrap(),
            1
        );
        assert!(
            node.blob_workers
                .parts
                .obj_parts(expected.into())
                .await
                .unwrap()
                .contains(&big_blobs::workers::inventory_part(inventory))
        );
    }
    drop(changes);
    shutdown(node, stop).await;
    let mut cfg = config(
        &secrets,
        &runtime,
        sql,
        StorageConfig::Disk {
            path: directory.path().join("repo"),
        },
    );
    cfg.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    for (root, inventory) in &inventories {
        let expected = match root {
            MetaRoot::Public => cipher_id,
            MetaRoot::Sensitive => info.blob_id,
        };
        assert!(
            node.blob_workers
                .parts
                .obj_parts(expected.into())
                .await
                .unwrap()
                .contains(&big_blobs::workers::inventory_part(inventory))
        );
    }
    shutdown(node, stop).await;
}

#[tokio::test]
async fn self_doc_child_uses_containing_drawer_inventories() {
    use big_cabinet_core::drawer::protocol::AddChild;
    use big_cabinet_core::drawer::protocol::CreateDrawer;
    use big_cabinet_core::drawer::protocol::CreateLayout;
    use big_cabinet_core::drawer::protocol::RemoveDoc;
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(&secrets, &runtime, sql, StorageConfig::Memory))
        .await
        .unwrap();
    let drawer = node.drawers.local().originated_id.clone();
    let public = inventory(&node, &drawer, MetaRoot::Public, "ciphertext").await;
    let sensitive = inventory(&node, &drawer, MetaRoot::Sensitive, "plaintext").await;
    let child = create(
        &node,
        "self-child",
        json!({"$type": NOTE, "value": "self child"}),
    )
    .await
    .doc_id;
    node.drawer_access
        .grants()
        .grant(
            &RequestContext::root(),
            node.drawers_actor.actor_id,
            big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let create_cap = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    create: true,
                    drawers: BTreeMap::from([(drawer.clone(), DrawerAccess::Admin)]),
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
                capability: create_cap,
                command: "declare-self-child".to_owned().try_into().unwrap(),
                layout: CreateLayout::SelfDoc {
                    doc_id: child.clone(),
                },
            },
        )
        .await
        .unwrap();
    let cap = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    create: false,
                    drawers: BTreeMap::from([
                        (drawer.clone(), DrawerAccess::Admin),
                        (child.clone(), DrawerAccess::Admin),
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
            RemoveDoc {
                capability: cap,
                command: "remove-direct-child".to_owned().try_into().unwrap(),
                drawer_id: drawer.clone(),
                doc_id: child.clone(),
            },
        )
        .await
        .unwrap();
    node.drawers
        .client
        .call(
            RequestContext::root(),
            AddChild {
                capability: cap,
                command: "link-self-child".to_owned().try_into().unwrap(),
                parent_id: drawer.clone(),
                child_id: child.clone(),
                access: DrawerAccess::Admin,
            },
        )
        .await
        .unwrap();
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: blob_cap(&node, BlobService::put_object().into(), Vec::new()).await,
                bytes: b"self-child inventory bytes".to_vec(),
            },
        )
        .await
        .unwrap();
    let blob_key = DatumKey::from_text(big_blobs_core::Blob::ID, "main").unwrap();
    let blob_value = json!({
        "mime": "application/octet-stream",
        "digest": info.blob_id.digest_str(),
        "lengthOctets": info.length_octets,
    });
    node.docs
        .client
        .call(
            RequestContext::root(),
            big_cabinet_core::doc::WriteDatums {
                capability: capability(
                    &node,
                    DocScope {
                        write: BTreeSet::from([child.clone()]),
                        ..Default::default()
                    },
                )
                .await,
                command: Some("self-child-blob".to_owned().try_into().unwrap()),
                doc_id: child.clone(),
                mode: big_cabinet_core::doc::WriteMode::AtCurrent,
                set: BTreeMap::from([(blob_key, blob_value)]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
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
            .member_count(big_blobs::workers::inventory_part(&public))
            .await
            .unwrap()
            == 1
            && node
                .blob_workers
                .parts
                .member_count(big_blobs::workers::inventory_part(&sensitive))
                .await
                .unwrap()
                == 1
        {
            break;
        }
        changes.next(RevisionReadLimits::default()).await.unwrap();
    }
    let pins = all_datums(&node, &sensitive).await;
    assert!(pins.datums.contains_key(
        &DatumKey::from_text(big_blobs_core::BlobPin::ID, info.blob_id.digest_str()).unwrap()
    ));
    let view = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            big_cabinet_core::drawer::protocol::GetDrawer {
                capability: cap,
                drawer_id: drawer,
            },
        )
        .await
        .unwrap();
    assert!(!view.docs.contains_key(&child));
    assert_eq!(view.children.get(&child), Some(&DrawerAccess::Admin));
    drop(changes);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn put_blob_keeps_independent_durable_standalone_holds() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Memory,
    ))
    .await
    .unwrap();
    let request = PutBlob {
        capability: blob_cap(&node, BlobService::put_object().into(), Vec::new()).await,
        bytes: b"independent protocol uploads".to_vec(),
    };
    let first: BlobInfo = node
        .blobs
        .client
        .call(RequestContext::root(), request.clone())
        .await
        .unwrap();
    let second: BlobInfo = node
        .blobs
        .client
        .call(RequestContext::root(), request)
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        sql.query(
            "SELECT COUNT(*) FROM big_blobs_upload_receipts WHERE released = 0 AND digest = ?",
            (first.blob_id.to_string(),)
        )
        .await
        .unwrap()
        .all::<i64>()
        .unwrap(),
        vec![2]
    );
    assert_eq!(
        sql.query(
            "SELECT COUNT(*) FROM big_blobs_byte_owners WHERE digest = ? AND owner GLOB 'upload:*'",
            (first.blob_id.to_string(),)
        )
        .await
        .unwrap()
        .all::<i64>()
        .unwrap(),
        vec![2]
    );
    shutdown(node, stop).await;
    assert_eq!(
        sql.query(
            "SELECT COUNT(*) FROM big_blobs_upload_receipts WHERE released = 0 AND digest = ?",
            (first.blob_id.to_string(),)
        )
        .await
        .unwrap()
        .all::<i64>()
        .unwrap(),
        vec![2]
    );
}

#[tokio::test]
async fn blob_capabilities_bound_ranges_and_revocation() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(&secrets, &runtime, sql, StorageConfig::Memory))
        .await
        .unwrap();
    let put = blob_cap(&node, BlobService::put_object().into(), Vec::new()).await;
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: b"bounded blob bytes".to_vec(),
            },
        )
        .await
        .unwrap();
    let read = blob_cap(&node, BlobService::read_object(&info.blob_id), vec![put]).await;
    let denied = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            StatBlob {
                capability: put,
                blob_id: info.blob_id,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, ClientError::Operation(BlobsError::Denied)));
    let range = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            ReadBlob {
                capability: read,
                blob_id: info.blob_id,
                offset: 8,
                length: 4,
            },
        )
        .await
        .unwrap();
    assert_eq!(range.bytes, b"blob");
    assert_eq!(range.length_octets, 18);
    let invalid = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            ReadBlob {
                capability: read,
                blob_id: info.blob_id,
                offset: 0,
                length: big_blobs_core::MAX_READ_BYTES + 1,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid,
        ClientError::Operation(BlobsError::InvalidInput { .. })
    ));
    node.authority
        .client
        .call(
            RequestContext::root(),
            RetractCapability {
                via: node.blobs_namespace.capability,
                capability: put,
            },
        )
        .await
        .unwrap();
    let denied = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            StatBlob {
                capability: read,
                blob_id: info.blob_id,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, ClientError::Operation(BlobsError::Denied)));
    shutdown(node, stop).await;
}

#[tokio::test]
async fn filesystem_store_has_one_owner_and_reopens_with_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.database(
        directory.path().join("cabinet.sqlite"),
        &[],
        "_sqlite_rs_migrations",
    );
    let mut cfg = config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk {
            path: directory.path().join("repo"),
        },
    );
    cfg.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    let put = blob_cap(&node, BlobService::put_object().into(), Vec::new()).await;
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: b"persistent bytes".to_vec(),
            },
        )
        .await
        .unwrap();
    let duplicate = big_blobs::boot(big_blobs::Config {
        storage: big_blobs::Storage::Filesystem(directory.path().join("blobs")),
        sql: sql.clone(),
        authority: node.authority.client.clone(),
        namespace: node.blobs_namespace,
    })
    .await;
    assert!(
        duplicate
            .err()
            .unwrap()
            .to_string()
            .contains("already has an owner")
    );
    shutdown(node, stop).await;
    let mut cfg = config(
        &secrets,
        &runtime,
        sql,
        StorageConfig::Disk {
            path: directory.path().join("repo"),
        },
    );
    cfg.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    assert_eq!(
        node.blobs.service.stat(&info.blob_id).await.unwrap(),
        Some(info)
    );
    shutdown(node, stop).await;
}
