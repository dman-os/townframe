use super::*;
use big_blobs_core::BlobId;
use big_blobs_core::BlobInfo;
use big_blobs_core::BlobsError;
use big_blobs_core::BlobsProtocol;
use big_blobs_core::BlobsScope;
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
use method_core::ByteBody;
use method_core::ByteRange;
use method_core::Method;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_client_native_rs::XrpcClient;
use service_utils_rs::ClientError;

mod nodule_artifacts;

use super::cartridges::operator;

/// Puts `bytes` through the node's blobs service with a fresh put handle: the node's answer is the
/// authoritative [`BlobInfo`].
pub(super) async fn put_bytes(node: &BigNode, bytes: &[u8]) -> BlobInfo {
    node.blobs
        .put(
            RequestContext::root(),
            put_cap(node).await,
            ByteBody::from_bytes(bytes.to_vec()),
        )
        .await
        .unwrap()
}

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
        .put(
            RequestContext::root(),
            put_cap(&node).await,
            ByteBody::from_bytes(b"root-scoped inventory bytes".to_vec()),
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
        .put(
            RequestContext::root(),
            put_cap(&node).await,
            ByteBody::from_bytes(b"self-child inventory bytes".to_vec()),
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
                        write: std::collections::BTreeMap::from([(
                            child.clone(),
                            big_cabinet_core::doc::DatumKinds::All,
                        )]),
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
    let first: BlobInfo = put_bytes(&node, b"independent protocol uploads").await;
    let second: BlobInfo = put_bytes(&node, b"independent protocol uploads").await;
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
    let first: BlobInfo = put_bytes(&node, b"independent protocol uploads").await;
    put_bytes(&node, b"independent protocol uploads").await;
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
        .put(
            RequestContext::root(),
            put,
            ByteBody::from_bytes(b"bounded blob bytes".to_vec()),
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
    // A range is part of the request, not of the call's parameters: it selects the octets the
    // stream yields and says where they sit in the whole.
    let part = node
        .blobs
        .read(
            RequestContext::root(),
            read,
            info.blob_id,
            Some(ByteRange::Between { start: 8, end: 11 }),
        )
        .await
        .unwrap();
    assert_eq!(
        part.span,
        Some(method_core::ByteSpan {
            start: 8,
            total: 18
        })
    );
    assert_eq!(part.collect(4).await.unwrap().as_ref(), b"blob");
    let whole = node
        .blobs
        .read(RequestContext::root(), read, info.blob_id, None)
        .await
        .unwrap();
    assert_eq!(whole.length, Some(18));
    assert_eq!(
        whole.collect(18).await.unwrap().as_ref(),
        b"bounded blob bytes"
    );
    let unsatisfiable = node
        .blobs
        .read(
            RequestContext::root(),
            read,
            info.blob_id,
            Some(ByteRange::From(18)),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        unsatisfiable,
        ClientError::Method(BlobsError::RangeNotSatisfiable { total_octets: 18 })
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
        .put(
            RequestContext::root(),
            put,
            ByteBody::from_bytes(b"persistent bytes".to_vec()),
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
/// the bytes travel as the raw body of the request and of the response (ADR 042), with the
/// capability and the blob id as query parameters. A file well past the 8 MiB a JSON value forced
/// is put whole; a `Range` reads part of it back as `206 Partial Content`; and what the node
/// returns is authoritative: the id it hashed, the same one every time.
#[tokio::test]
async fn the_operator_stores_and_reads_a_file_over_the_socket_as_raw_bytes() {
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
    let client = XrpcClient::unix(&socket, 2 << 20).unwrap();
    let blobs = Client::new(Transport::<BlobsProtocol>::xrpc(client.clone()));
    // A capability id is a query parameter's text.
    let mint = async |scope: BlobsScope| {
        let id = blobs
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
            .capability_id;
        serde_json::to_value(id)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    };
    let put = mint(BlobsScope {
        put: true,
        ..Default::default()
    })
    .await;
    let url = |nsid: &str, query: &[(&str, &str)]| {
        let mut url = client.base_url().join(&format!("xrpc/{nsid}")).unwrap();
        url.query_pairs_mut().extend_pairs(query);
        url
    };

    // 24 MiB: three times the old cap, and over the 16 MiB a JSON body may be.
    let file: Vec<u8> = (0..24 * 1024 * 1024)
        .map(|at| (at * 31 % 251) as u8)
        .collect();
    let put_file = async || {
        let response = client
            .http_client()
            .post(url(PutBlob::XRPC.nsid, &[("capability", &put)]))
            .header("content-type", "application/octet-stream")
            .body(file.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        response.json::<BlobInfo>().await.unwrap()
    };
    let info = put_file().await;
    assert_eq!(info.length_octets, file.len() as u64);
    // Putting what the node holds returns the same answer, with nothing asked first.
    assert_eq!(put_file().await, info);

    let read = mint(BlobsScope {
        read: BTreeSet::from([info.blob_id]),
        ..Default::default()
    })
    .await;
    let blob_id = info.blob_id.to_string();
    let read_url = url(
        ReadBlob::XRPC.nsid,
        &[("capability", &read), ("blobId", &blob_id)],
    );
    let whole = client
        .http_client()
        .get(read_url.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(whole.status().as_u16(), 200);
    assert_eq!(whole.headers()["accept-ranges"], "bytes");
    assert_eq!(whole.headers()["content-length"], file.len().to_string());
    assert!(
        whole.bytes().await.unwrap() == file,
        "the file read back differs from the file put"
    );
    let part = client
        .http_client()
        .get(read_url.clone())
        .header("range", "bytes=1000-1999")
        .send()
        .await
        .unwrap();
    assert_eq!(part.status().as_u16(), 206);
    assert_eq!(
        part.headers()["content-range"],
        format!("bytes 1000-1999/{}", file.len())
    );
    assert_eq!(part.bytes().await.unwrap().as_ref(), &file[1000..2000]);

    // The capability still rules: a read handle cannot put, and the put handle cannot read.
    let (status, body) = {
        let response = client
            .http_client()
            .post(url(PutBlob::XRPC.nsid, &[("capability", &read)]))
            .body(vec![1, 2, 3])
            .send()
            .await
            .unwrap();
        (
            response.status().as_u16(),
            response.json::<Value>().await.unwrap(),
        )
    };
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"], "Denied", "{body}");
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_blobs_handle_presented_by_an_actor_it_is_not_bound_to_cannot_mint() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    // A handle that may mint, bound to the operator that minted it.
    let held = node
        .blobs
        .client
        .call_with(
            RequestContext::root(),
            operator(&node).metadata(),
            MintBlobsCapability {
                scope: BlobsScope {
                    put: true,
                    ..Default::default()
                },
                rights: Rights::USE | Rights::NARROW,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let mint_as = async |signer: CapId| {
        node.blobs
            .client
            .call_with(
                RequestContext::root(),
                Authorization {
                    authority: held,
                    signer,
                }
                .metadata(),
                MintBlobsCapability {
                    scope: BlobsScope {
                        put: true,
                        ..Default::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
    };
    mint_as(operator(&node).signer).await.unwrap();
    // Another actor presenting the same id under its own signer is refused.
    let stolen = super::cartridges::bystander(&node).await.signer;
    assert!(matches!(
        mint_as(stolen).await,
        Err(ClientError::Method(BlobsError::Denied))
    ));
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
            .put(
                RequestContext::root(),
                put.authority,
                ByteBody::from_bytes(bytes.to_vec()),
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
