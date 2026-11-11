use super::*;
use big_blobs_core::BlobId;
use big_blobs_core::BlobInfo;
use big_blobs_core::BlobsError;
use big_blobs_core::BlobsProtocol;
use big_blobs_core::BlobsScope;
use big_blobs_core::MAX_PUT_BYTES;
use big_blobs_core::MAX_READ_BYTES;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_blobs_core::ReadBlob;
use big_blobs_core::StatBlob;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_client_native_rs::XrpcClient;
use service_utils_rs::ClientError;

mod nodule_artifacts;

use super::cartridges::operator;
use super::listeners::post;

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
                subject: None,
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

/// A blobs handle the node operator mints over the wire, from its identity credential alone.
async fn blob_cap(node: &BigNode, scope: BlobsScope) -> CapId {
    node.blobs
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            MintBlobsCapability {
                scope,
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id
}

pub(super) async fn put_cap(node: &BigNode) -> CapId {
    blob_cap(
        node,
        BlobsScope {
            put: true,
            ..Default::default()
        },
    )
    .await
}

pub(super) async fn read_cap(node: &BigNode, blob: &BlobId) -> CapId {
    blob_cap(
        node,
        BlobsScope {
            read: BTreeSet::from([*blob]),
            ..Default::default()
        },
    )
    .await
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
        Arc::clone(&secrets),
        sql.clone(),
        big_repo::Storage::in_directory(&runtime, &directory.path().join("repo"), None).unwrap(),
    );
    cfg.blob_storage = big_blobs::Storage::filesystem(directory.path().join("blobs"))
        .await
        .unwrap();
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
                capability: put_cap(&node).await,
                bytes: b"root-scoped inventory bytes".to_vec().into(),
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
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::in_directory(&runtime, &directory.path().join("repo"), None).unwrap(),
    );
    cfg.blob_storage = big_blobs::Storage::filesystem(directory.path().join("blobs"))
        .await
        .unwrap();
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
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
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
            node.drawers_actor.actor.actor_id,
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
                subject: None,
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
                subject: None,
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
                capability: put_cap(&node).await,
                bytes: b"self-child inventory bytes".to_vec().into(),
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

/// The number of unreleased upload holds on `digest`, and of byte owners of each kind.
async fn holds(sql: &Database, digest: &BlobId) -> (i64, i64, i64) {
    let count = async |query: &str| {
        sql.query(query, (digest.to_string(),))
            .await
            .unwrap()
            .all::<i64>()
            .unwrap()[0]
    };
    (
        count("SELECT COUNT(*) FROM big_blobs_upload_receipts WHERE released = 0 AND digest = ?")
            .await,
        count("SELECT COUNT(*) FROM big_blobs_byte_owners WHERE digest = ? AND owner GLOB 'upload:*'")
            .await,
        count(
            "SELECT COUNT(*) FROM big_blobs_byte_owners WHERE digest = ? AND owner GLOB 'document:retention:*'",
        )
        .await,
    )
}

#[tokio::test]
async fn put_blob_keeps_independent_durable_standalone_holds() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        sql.clone(),
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let request = PutBlob {
        capability: put_cap(&node).await,
        bytes: b"independent protocol uploads".to_vec().into(),
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
    // Two puts of the same bytes are two holds, neither released by the other.
    assert_eq!(holds(&sql, &first.blob_id).await, (2, 2, 0));

    // A put nothing references keeps its holds, durably. (Expiring an abandoned upload is a
    // separate, unbuilt concern.)
    shutdown(node, stop).await;
    assert_eq!(holds(&sql, &first.blob_id).await, (2, 2, 0));
}

#[tokio::test]
async fn a_document_retaining_a_put_blobs_bytes_releases_its_upload_holds() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        sql.clone(),
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let request = PutBlob {
        capability: put_cap(&node).await,
        bytes: b"independent protocol uploads".to_vec().into(),
    };
    let first: BlobInfo = node
        .blobs
        .client
        .call(RequestContext::root(), request.clone())
        .await
        .unwrap();
    node.blobs
        .client
        .call(RequestContext::root(), request)
        .await
        .unwrap();
    assert_eq!(holds(&sql, &first.blob_id).await, (2, 2, 0));
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let document = node
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
                command: "reference-the-upload".to_owned().try_into().unwrap(),
                placement,
                datums: BTreeMap::from([(
                    DatumKey::from_text(big_blobs_core::Blob::ID, "main").unwrap(),
                    json!({
                        "mime": "application/octet-stream",
                        "digest": first.blob_id.digest_str(),
                        "lengthOctets": first.length_octets,
                    }),
                )]),
            },
        )
        .await
        .unwrap();
    // The document worker retains the bytes under the document's claim, and in that same step
    // releases every upload hold on the digest: the holds only kept the bytes until a document
    // referenced them.
    while holds(&sql, &first.blob_id).await.0 != 0 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(holds(&sql, &first.blob_id).await, (0, 0, 1));
    assert_eq!(
        sql.query(
            "SELECT owner FROM big_blobs_byte_owners WHERE digest = ? AND owner GLOB 'document:retention:*'",
            (first.blob_id.to_string(),)
        )
        .await
        .unwrap()
        .all::<String>()
        .unwrap(),
        vec![format!("document:retention:{}", document.doc_id)]
    );
    assert_eq!(
        node.blobs.service.stat(&first.blob_id).await.unwrap(),
        Some(first)
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn blob_capabilities_bound_ranges_and_revocation() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let put = put_cap(&node).await;
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: b"bounded blob bytes".to_vec().into(),
            },
        )
        .await
        .unwrap();
    let read = read_cap(&node, &info.blob_id).await;
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
    assert!(matches!(denied, ClientError::Method(BlobsError::Denied)));
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
    assert_eq!(range.bytes.as_slice(), b"blob");
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
        ClientError::Method(BlobsError::InvalidInput { .. })
    ));
    node.authority
        .client
        .call(
            RequestContext::root(),
            RetractCapability {
                via: node.blobs_namespace.capability,
                capability: read,
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
    assert!(matches!(denied, ClientError::Method(BlobsError::Denied)));
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
        Arc::clone(&secrets),
        sql.clone(),
        big_repo::Storage::in_directory(&runtime, &directory.path().join("repo"), None).unwrap(),
    );
    cfg.blob_storage = big_blobs::Storage::filesystem(directory.path().join("blobs"))
        .await
        .unwrap();
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    let put = put_cap(&node).await;
    let info = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: b"persistent bytes".to_vec().into(),
            },
        )
        .await
        .unwrap();
    // The owner lock is taken when the store is opened, so a second open fails before any
    // service is booted on it.
    let duplicate = big_blobs::Storage::filesystem(directory.path().join("blobs")).await;
    assert!(
        duplicate
            .err()
            .unwrap()
            .to_string()
            .contains("already has an owner")
    );
    shutdown(node, stop).await;
    let mut cfg = config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::in_directory(&runtime, &directory.path().join("repo"), None).unwrap(),
    );
    cfg.blob_storage = big_blobs::Storage::filesystem(directory.path().join("blobs"))
        .await
        .unwrap();
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    assert_eq!(
        node.blobs.service.stat(&info.blob_id).await.unwrap(),
        Some(info)
    );
    shutdown(node, stop).await;
}

/// A host client stores a file over the node's socket: the operator reaches the blobs service as
/// itself (no token), mints its own put and read handles from the `Authorization` credential, and
/// the bytes travel as the datum model's `$bytes` object. The largest put the service accepts is
/// `MAX_PUT_BYTES` octets, which is about 10.7 MiB of base64 on the wire and fits the 16 MiB body
/// the listener takes; a JSON array of numbers would not.
#[tokio::test]
async fn the_operator_stores_and_reads_a_file_over_the_socket_as_a_bytes_object() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let mut cfg = config(
        Arc::clone(&secrets),
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(&runtime).unwrap(),
    );
    cfg.listeners = super::listeners::unix(&socket);
    let (node, stop) = BigNode::boot(cfg).await.unwrap();
    // A 1 MiB range is about 1.4 MiB of base64, over the default 1 MiB response bound.
    let client = XrpcClient::unix(&socket, 2 << 20).unwrap();
    let blobs = Client::new(Transport::<BlobsProtocol>::xrpc(client.clone()));
    let mint = async |scope: BlobsScope| {
        blobs
            .call_with(
                RequestContext::root(),
                operator(&node).metadata(),
                MintBlobsCapability {
                    scope,
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .unwrap()
            .capability_id
    };
    let put = mint(BlobsScope {
        put: true,
        ..Default::default()
    })
    .await;

    let file: Vec<u8> = (0..MAX_PUT_BYTES).map(|at| (at * 31 % 251) as u8).collect();
    let info = blobs
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: file.clone().into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(info.length_octets, file.len() as u64);

    let read = mint(BlobsScope {
        read: BTreeSet::from([info.blob_id]),
        ..Default::default()
    })
    .await;
    let mut returned = Vec::with_capacity(file.len());
    while returned.len() < file.len() {
        let range = blobs
            .call(
                RequestContext::root(),
                ReadBlob {
                    capability: read,
                    blob_id: info.blob_id,
                    offset: returned.len() as u64,
                    length: MAX_READ_BYTES,
                },
            )
            .await
            .unwrap();
        assert_eq!(range.length_octets, file.len() as u64);
        returned.extend_from_slice(range.bytes.as_slice());
    }
    assert!(
        returned == file,
        "the file read back differs from the file put"
    );

    // One more octet than the service accepts is refused by name.
    let oversized = blobs
        .call(
            RequestContext::root(),
            PutBlob {
                capability: put,
                bytes: vec![0; MAX_PUT_BYTES + 1].into(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        oversized,
        ClientError::Method(BlobsError::InvalidInput { .. })
    ));

    // On the wire the payload is the `$bytes` object. The array of numbers the protocol used to
    // carry is no longer a value of the type.
    let nsid = "org.example.daybook.blobs.put";
    let (status, body) = post(
        &client,
        nsid,
        None,
        json!({"capability": put, "bytes": {"$bytes": "aGk="}}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["lengthOctets"], 2, "{body}");
    let (status, body) = post(
        &client,
        nsid,
        None,
        json!({"capability": put, "bytes": [104, 105]}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    drop(blobs);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn blobs_handles_are_minted_by_the_operator_and_a_handle_only_narrows() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let mint = |authorization: Authorization, scope: BlobsScope, rights: Rights| {
        let node = &node;
        async move {
            node.blobs
                .client
                .call_with(
                    RequestContext::root(),
                    authorization.metadata(),
                    MintBlobsCapability {
                        scope,
                        rights,
                        subject: None,
                    },
                )
                .await
                .map(|capability| Authorization {
                    authority: capability.capability_id,
                    signer: authorization.signer,
                })
        }
    };
    let denied = |result: Result<Authorization, ClientError<BlobsError>>| {
        assert!(
            matches!(result, Err(ClientError::Method(BlobsError::Denied))),
            "{result:?}"
        );
    };
    let put_only = BlobsScope {
        put: true,
        ..Default::default()
    };
    let reading = |blobs: &[BlobId]| BlobsScope {
        read: blobs.iter().copied().collect(),
        ..Default::default()
    };
    let operator = operator(&node);

    // The operator puts through a handle it minted, and reads the result through another.
    let put = mint(operator, put_only.clone(), Rights::USE).await.unwrap();
    let put_bytes = async |bytes: &[u8]| {
        node.blobs
            .client
            .call(
                RequestContext::root(),
                PutBlob {
                    capability: put.authority,
                    bytes: bytes.to_vec().into(),
                },
            )
            .await
            .unwrap()
            .blob_id
    };
    let (a, b) = (put_bytes(b"a").await, put_bytes(b"b").await);
    let c = BlobId::new([9; 32]);
    let stat = async |handle: Authorization, blob: BlobId| {
        node.blobs
            .client
            .call(
                RequestContext::root(),
                StatBlob {
                    capability: handle.authority,
                    blob_id: blob,
                },
            )
            .await
    };
    let read_a = mint(operator, reading(&[a]), Rights::USE | Rights::NARROW)
        .await
        .unwrap();
    assert_eq!(
        stat(read_a, a).await.unwrap().info.unwrap().length_octets,
        1
    );

    // A handle's scope bounds what it mints. A read handle cannot mint a put handle or a read of
    // another blob, and a right it lacks is not something it can hand on.
    denied(mint(read_a, put_only.clone(), Rights::USE).await);
    denied(mint(read_a, reading(&[b]), Rights::USE).await);
    denied(mint(read_a, reading(&[a]), Rights::USE | Rights::DELEGATE).await);
    // A handle without the NARROW right mints nothing, however narrow the request.
    denied(mint(put, put_only.clone(), Rights::USE).await);

    // Narrowing succeeds, and the narrowed handle reaches exactly its own scope.
    let wide = mint(
        operator,
        BlobsScope {
            put: true,
            read: BTreeSet::from([a, b]),
        },
        Rights::USE | Rights::NARROW,
    )
    .await
    .unwrap();
    let narrowed = mint(wide, reading(&[a]), Rights::USE).await.unwrap();
    assert!(stat(narrowed, a).await.unwrap().info.is_some());
    assert!(matches!(
        stat(narrowed, b).await,
        Err(ClientError::Method(BlobsError::Denied))
    ));
    denied(
        mint(
            wide,
            BlobsScope {
                put: true,
                read: BTreeSet::from([a, b, c]),
            },
            Rights::USE,
        )
        .await,
    );

    // Only the operator's identity mints from nothing: another actor's identity is refused, and
    // a request with no credential is unauthenticated.
    denied(
        mint(
            Authorization::identity(node.drawers_actor.capability),
            put_only.clone(),
            Rights::USE,
        )
        .await,
    );
    let anonymous = node
        .blobs
        .client
        .call(
            RequestContext::root(),
            MintBlobsCapability {
                scope: put_only,
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            anonymous,
            ClientError::Method(BlobsError::Context(
                method_core::ContextError::Unauthenticated
            ))
        ),
        "{anonymous:?}"
    );
    shutdown(node, stop).await;
}
