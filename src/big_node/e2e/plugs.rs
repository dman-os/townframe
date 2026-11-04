//! The plug registry end to end: the service over the real drawer, docs and blob stack.

use super::blobs::blob_cap;
use super::*;
use big_blobs::BlobService;
use big_blobs_core::BlobInfo;
use big_blobs_core::PutBlob;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_plugs_core::Artifact;
use big_plugs_core::Digest;
use big_plugs_core::Plug;
use big_plugs_core::example_plug;
use big_plugs_core::protocol::GetPlug;
use big_plugs_core::protocol::GetPlugVersion;
use big_plugs_core::protocol::GetPublisher;
use big_plugs_core::protocol::ImportPlugDocument;
use big_plugs_core::protocol::ListPlugs;
use big_plugs_core::protocol::MintPlugsCapability;
use big_plugs_core::protocol::PlugChanges;
use big_plugs_core::protocol::PlugsCursor;
use big_plugs_core::protocol::PlugsError;
use big_plugs_core::protocol::PlugsScope;
use big_plugs_core::protocol::PublishPlugVersion;
use big_plugs_core::protocol::PublishedPlugVersion;
use big_plugs_core::protocol::RemovePlug;
use big_plugs_core::protocol::RemovePlugVersion;
use big_plugs_core::protocol::WatchPlugs;
use big_plugs_core::registry::identity_key;
use big_plugs_core::registry::version_key;
use big_plugs_core::signature::SignatureError;
use big_plugs_core::test_support::plug_id;
use big_plugs_core::test_support::sign_as_example;
use big_plugs_core::test_support::sign_with_seed;
use service_utils_rs::ClientError;
use std::time::Duration;

const PHOTOS: &str = example_plug!("photos");

fn operator(node: &BigNode) -> Authorization {
    Authorization::identity(node.authority.root)
}

fn operation<E>(error: ClientError<E>) -> E {
    match error {
        ClientError::Operation(error) => error,
        ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
    }
}

async fn put(node: &BigNode, bytes: &[u8]) -> BlobInfo {
    node.blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: blob_cap(node, BlobService::put_object().into(), Vec::new()).await,
                bytes: bytes.to_vec(),
            },
        )
        .await
        .unwrap()
}

/// `id` at `version` with one artifact, `handler`, holding `info`'s bytes, not yet signed.
fn unsigned(id: &str, version: &str, info: &BlobInfo) -> Plug {
    let mut plug = Plug::new(id.parse().unwrap(), version.parse().unwrap());
    plug.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest(info.blob_id.to_bytes32()),
        size: Some(info.length_octets),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    plug
}

/// `photos` under the example namespace, signed by its owner as an external publisher would.
fn photos(version: &str, info: &BlobInfo) -> Plug {
    sign_as_example(unsigned(PHOTOS, version, info))
}

/// A write to one version slot of a registry document, as a synced change from another node
/// would land: straight on the document, past the registry service.
async fn write_version_datum(node: &BigNode, doc_id: &DocId, envelope: &Plug) {
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: capability(
                    node,
                    DocScope {
                        write: BTreeSet::from([doc_id.clone()]),
                        ..DocScope::default()
                    },
                )
                .await,
                command: None,
                doc_id: doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(
                    version_key(&envelope.version),
                    serde_json::to_value(envelope).unwrap(),
                )]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
}

async fn get_version(
    node: &BigNode,
    id: &str,
    version: &str,
) -> Result<big_plugs_core::protocol::PlugVersionRecord, PlugsError> {
    node.plugs
        .client
        .call(
            RequestContext::root(),
            GetPlugVersion {
                authorization: operator(node),
                id: id.parse().unwrap(),
                version: version.parse().unwrap(),
            },
        )
        .await
        .map_err(operation)
}

async fn publish(
    node: &BigNode,
    authorization: Authorization,
    envelope: Plug,
) -> Result<PublishedPlugVersion, PlugsError> {
    node.plugs
        .client
        .call(
            RequestContext::root(),
            PublishPlugVersion {
                authorization,
                envelope,
            },
        )
        .await
        .map_err(operation)
}

async fn mint(node: &BigNode, scope: PlugsScope) -> Authorization {
    let capability = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            MintPlugsCapability {
                authorization: operator(node),
                scope,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    Authorization {
        authority: capability.capability_id,
        signer: operator(node).signer,
    }
}

async fn watch(
    node: &BigNode,
    after: Option<PlugsCursor>,
    hold_ms: u32,
) -> Result<PlugChanges, PlugsError> {
    node.plugs
        .client
        .call(
            RequestContext::root(),
            WatchPlugs {
                authorization: operator(node),
                after,
                limit: 0,
                hold_ms,
            },
        )
        .await
        .map_err(operation)
}

async fn list(node: &BigNode) -> Vec<big_plugs_core::protocol::PlugRecord> {
    node.plugs
        .client
        .call(
            RequestContext::root(),
            ListPlugs {
                authorization: operator(node),
            },
        )
        .await
        .unwrap()
}

async fn retained(sql: &Database, document: &DocId) -> BTreeSet<String> {
    sql.query(
        "SELECT digest FROM big_blobs_retention WHERE doc_id = ?1",
        (document.to_string(),),
    )
    .await
    .unwrap()
    .all::<(String,)>()
    .unwrap()
    .into_iter()
    .map(|(digest,)| digest)
    .collect()
}

async fn boot_memory(secrets: &SecretStore) -> (BigNode, StopToken, Database) {
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Memory,
    ))
    .await
    .unwrap();
    (node, stop, sql)
}

#[tokio::test]
async fn publish_list_and_get_serve_what_was_published_to_the_right_callers() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"photos handler v1").await;
    let published = publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    assert!(!published.unchanged);
    assert_eq!(published.record.envelope, photos("1.0.0", &info));
    publish(&node, operator, photos("1.1.0", &info))
        .await
        .unwrap();

    let list = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            ListPlugs {
                authorization: operator,
            },
        )
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, plug_id("photos"));
    assert_eq!(
        list[0]
            .versions
            .iter()
            .map(|summary| summary.version.to_string())
            .collect::<Vec<_>>(),
        ["1.0.0", "1.1.0"]
    );
    let record = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();
    assert_eq!(record, list[0]);
    let version = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlugVersion {
                authorization: operator,
                id: plug_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(version.envelope, photos("1.0.0", &info));
    assert_eq!(version.revision, photos("1.0.0", &info).revision());
    assert_eq!(version.conflict, None);

    // A read handle lists and gets but does not publish or remove; a publish handle does; an
    // actor presenting only its own identity is refused outright.
    let reader = mint(
        &node,
        PlugsScope {
            publish: false,
            ..Default::default()
        },
    )
    .await;
    let publisher = mint(
        &node,
        PlugsScope {
            publish: true,
            ..Default::default()
        },
    )
    .await;
    node.plugs
        .client
        .call(
            RequestContext::root(),
            ListPlugs {
                authorization: reader,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        publish(&node, reader, photos("2.0.0", &info)).await,
        Err(PlugsError::Denied)
    );
    assert_eq!(
        node.plugs
            .client
            .call(
                RequestContext::root(),
                RemovePlug {
                    authorization: reader,
                    id: plug_id("photos")
                }
            )
            .await
            .map_err(operation),
        Err(PlugsError::Denied)
    );
    publish(&node, publisher, photos("2.0.0", &info))
        .await
        .unwrap();
    assert_eq!(
        node.plugs
            .client
            .call(
                RequestContext::root(),
                ListPlugs {
                    authorization: Authorization::identity(node.docs_actor.capability)
                }
            )
            .await
            .map_err(operation),
        Err(PlugsError::Denied)
    );
    assert_eq!(
        node.plugs
            .client
            .call(
                RequestContext::root(),
                MintPlugsCapability {
                    authorization: reader,
                    scope: PlugsScope {
                        publish: true,
                        ..Default::default()
                    },
                    rights: Rights::USE,
                }
            )
            .await
            .map_err(operation),
        Err(PlugsError::Denied)
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_published_version_never_changes_content() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"photos handler").await;
    let first = publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    let again = publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    assert!(again.unchanged);
    assert_eq!(again.record, first.record);

    let mut changed = photos("1.0.0", &info);
    changed.title = Some("Photos, renamed".into());
    let changed = sign_as_example(changed);
    assert_eq!(
        publish(&node, operator, changed.clone()).await,
        Err(PlugsError::VersionExists {
            id: plug_id("photos"),
            version: Box::new("1.0.0".parse().unwrap()),
            existing: first.record.revision,
            incoming: changed.revision(),
        })
    );
    // The refusal left the stored version as it was.
    let kept = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlugVersion {
                authorization: operator,
                id: plug_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(kept.envelope.title, None);

    // Removal leaves a tombstone: the label is never reused for other content, though the
    // identical envelope may be published again.
    node.plugs
        .client
        .call(
            RequestContext::root(),
            RemovePlugVersion {
                authorization: operator,
                id: plug_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        publish(&node, operator, changed).await,
        Err(PlugsError::VersionExists { .. })
    ));
    let restored = publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    assert_eq!(restored.record.revision, first.record.revision);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn publishing_refuses_artifacts_whose_bytes_are_not_local() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let absent = BlobInfo {
        blob_id: big_blobs_core::BlobId::new(Digest::of(b"never stored").0),
        length_octets: 12,
    };
    assert_eq!(
        publish(&node, operator(&node), photos("1.0.0", &absent)).await,
        Err(PlugsError::ArtifactUnavailable {
            id: plug_id("photos"),
            artifact: "handler".into(),
        })
    );
    let listed = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            ListPlugs {
                authorization: operator(&node),
            },
        )
        .await
        .unwrap();
    assert!(listed.is_empty());
    shutdown(node, stop).await;
}

#[tokio::test]
async fn artifacts_are_retained_while_published_and_released_after_removal() {
    let secrets = secrets().await;
    let (node, stop, sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"retained handler bytes").await;
    publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    let doc = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap()
        .doc_id;
    // The blob workers pick the registry document up through its `Blob` datum.
    while retained(&sql, &doc).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.plugs
        .client
        .call(
            RequestContext::root(),
            RemovePlugVersion {
                authorization: operator,
                id: plug_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    while !retained(&sql, &doc).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The plug itself stays, with no versions, until it is removed.
    let record = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();
    assert!(record.versions.is_empty());
    node.plugs
        .client
        .call(
            RequestContext::root(),
            RemovePlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.plugs
            .client
            .call(
                RequestContext::root(),
                GetPlug {
                    authorization: operator,
                    id: plug_id("photos"),
                },
            )
            .await
            .map_err(operation),
        Err(PlugsError::UnknownPlug(plug_id("photos")))
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn the_registry_is_rebuilt_from_its_drawer_after_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let secrets = secrets().await;
    let sql_path = directory.path().join("cabinet.sqlite");
    let boot = |runtime: Arc<SqliteRuntime>| {
        let mut config = config(
            &secrets,
            &runtime,
            runtime.database(&sql_path, &[], "_sqlite_rs_migrations"),
            StorageConfig::Disk {
                path: directory.path().join("repo"),
            },
        );
        config.blob_storage = big_blobs::Storage::Filesystem(directory.path().join("blobs"));
        async move { BigNode::boot(config).await.unwrap() }
    };
    let (node, stop) = boot(Arc::new(SqliteRuntime::new(Default::default()))).await;
    let operator = operator(&node);
    let info = put(&node, b"persistent handler").await;
    let published = publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    let drawer = node.registry_drawer.clone();
    let before = list(&node).await;
    shutdown(node, stop).await;

    let (node, stop) = boot(Arc::new(SqliteRuntime::new(Default::default()))).await;
    assert_eq!(node.registry_drawer, drawer);
    // The scan reads the datum-set index, which may still be catching up with the documents, so
    // the registry converges on what was published rather than having it at the first look.
    while list(&node).await != before {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The node still remembers what it first saw for the label, so the label stays immutable.
    let mut changed = photos("1.0.0", &info);
    changed.description = Some("different".into());
    let changed = sign_as_example(changed);
    assert!(matches!(
        publish(&node, Authorization::identity(node.authority.root), changed).await,
        Err(PlugsError::VersionExists { existing, .. }) if existing == published.record.revision
    ));
    shutdown(node, stop).await;
}

#[tokio::test]
async fn the_watch_observes_publish_and_remove_from_a_cursor() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    // With no cursor the answer is the current state, flagged as a reset; an empty registry says
    // so at once rather than holding.
    let start = watch(&node, None, 10_000).await.unwrap();
    assert!(start.reset && start.caught_up && start.changes.is_empty());

    let info = put(&node, b"watched handler").await;
    let held = {
        let node = &node;
        let cursor = start.through;
        async move { watch(node, Some(cursor), 10_000).await.unwrap() }
    };
    let publishing = async {
        // The watch is parked before the publish lands.
        tokio::time::sleep(Duration::from_millis(100)).await;
        publish(&node, operator, photos("1.0.0", &info))
            .await
            .unwrap();
    };
    let (published, ()) = tokio::join!(held, publishing);
    assert!(!published.reset && published.caught_up);
    assert_eq!(published.changes.len(), 1);
    assert_eq!(published.changes[0].plug_id, plug_id("photos"));
    assert!(!published.changes[0].removed);
    assert_eq!(published.through.seq, published.changes[0].seq);

    // Nothing has changed since, so a short hold expires as an empty page.
    let quiet = watch(&node, Some(published.through), 50).await.unwrap();
    assert!(quiet.changes.is_empty() && quiet.caught_up);
    assert_eq!(quiet.through, published.through);

    node.plugs
        .client
        .call(
            RequestContext::root(),
            RemovePlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();
    let removed = watch(&node, Some(published.through), 10_000).await.unwrap();
    assert_eq!(removed.changes.len(), 1);
    assert!(removed.changes[0].removed);
    assert!(removed.changes[0].seq > published.through.seq);

    // A cursor from another incarnation is answered with the full state.
    let stale = watch(
        &node,
        Some(PlugsCursor {
            epoch: big_cabinet_core::dmeta::Uuid7::new(),
            seq: 99,
        }),
        0,
    )
    .await
    .unwrap();
    assert!(stale.reset);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_version_rewritten_by_a_synced_write_is_surfaced_and_the_first_seen_content_is_served() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"conflicted handler").await;
    let original = photos("1.0.0", &info);
    publish(&node, operator, original.clone()).await.unwrap();
    let record = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();

    // Another node's write arrives as an ordinary datum write on the document: the test writes
    // it directly, as the synced change would land.
    let mut rewritten = original.clone();
    rewritten.title = Some("Rewritten elsewhere".into());
    write_version_datum(&node, &record.doc_id, &sign_as_example(rewritten)).await;

    let version = loop {
        let version = node
            .plugs
            .client
            .call(
                RequestContext::root(),
                GetPlugVersion {
                    authorization: operator,
                    id: plug_id("photos"),
                    version: "1.0.0".parse().unwrap(),
                },
            )
            .await
            .unwrap();
        if version.conflict.is_some() {
            break version;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        version.envelope, original,
        "the first-seen content is served"
    );
    assert_eq!(version.revision, original.revision());

    // Publishing the first-seen content again is the explicit resolution: it rewrites the datum
    // and the conflict clears.
    let republished = publish(&node, operator, original.clone()).await.unwrap();
    assert!(!republished.unchanged);
    assert_eq!(republished.record.conflict, None);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn an_envelope_must_carry_its_namespace_owners_signature() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"signed handler").await;

    // Unsigned under a namespace that is not the node's: the node will not sign for it.
    assert_eq!(
        publish(&node, operator, unsigned(PHOTOS, "1.0.0", &info)).await,
        Err(PlugsError::UnsignedForeignNamespace {
            namespace: big_plugs_core::example_namespace!().into(),
        })
    );
    // Signed with another key than the namespace's.
    assert!(matches!(
        publish(
            &node,
            operator,
            sign_with_seed(unsigned(PHOTOS, "1.0.0", &info), &[9; 32])
        )
        .await,
        Err(PlugsError::Signature(SignatureError::Invalid(_)))
    ));
    // A signature that no longer matches the envelope it is attached to.
    let mut tampered = photos("1.0.0", &info);
    tampered.title = Some("edited after signing".into());
    assert!(matches!(
        publish(&node, operator, tampered).await,
        Err(PlugsError::Signature(SignatureError::Invalid(_)))
    ));
    // A DID whose method cannot be verified without resolution fails closed, signed or not.
    let web = sign_as_example(unsigned("did:web:example.com/photos", "1.0.0", &info));
    assert_eq!(
        publish(&node, operator, web).await,
        Err(PlugsError::Signature(SignatureError::UnsupportedMethod(
            "web".into()
        )))
    );
    assert!(list(&node).await.is_empty());

    // The owner's signature is accepted, and survives the round trip through the drawer.
    let published = publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    published.record.envelope.verify_signature().unwrap();
    shutdown(node, stop).await;
}

#[tokio::test]
async fn the_node_signs_unsigned_envelopes_under_its_own_publisher_did() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"node published handler").await;
    let publisher = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPublisher {
                authorization: operator,
            },
        )
        .await
        .unwrap();
    assert_eq!(publisher, node.plugs.publisher);
    // The DID is the did:key of the authority's node key, the one that issues attribution
    // certificates.
    assert_eq!(
        publisher,
        big_plugs_core::signature::did_key(&node.authority.service.node_public_key())
    );

    let own = format!("{publisher}/mine");
    let published = publish(&node, operator, unsigned(&own, "1.0.0", &info))
        .await
        .unwrap();
    assert!(published.record.envelope.signature.is_some());
    published.record.envelope.verify_signature().unwrap();
    assert_eq!(published.record.id.to_string(), own);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_new_label_must_order_after_every_label_the_plug_has_or_had() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"ordered handler").await;
    publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    publish(&node, operator, photos("1.1.0", &info))
        .await
        .unwrap();

    // A validly signed older version is not an upgrade, even though nothing is wrong with it.
    let downgrade = |version: &str, newest: &str| PlugsError::Downgrade {
        id: plug_id("photos"),
        version: Box::new(version.parse().unwrap()),
        newest: Box::new(newest.parse().unwrap()),
    };
    assert_eq!(
        publish(&node, operator, photos("0.9.0", &info)).await,
        Err(downgrade("0.9.0", "1.1.0"))
    );
    assert_eq!(
        publish(&node, operator, photos("1.0.5", &info)).await,
        Err(downgrade("1.0.5", "1.1.0"))
    );
    assert_eq!(
        publish(&node, operator, photos("1.1.0-rc.1", &info)).await,
        Err(downgrade("1.1.0-rc.1", "1.1.0"))
    );
    // Republishing an existing label's identical content is not a new label.
    assert!(
        publish(&node, operator, photos("1.0.0", &info))
            .await
            .unwrap()
            .unchanged
    );

    // A removed version still counts: the newest label stays the bar after it is gone, and the
    // older one's identical content may still be published again.
    publish(&node, operator, photos("1.2.0", &info))
        .await
        .unwrap();
    for version in ["1.2.0", "1.0.0"] {
        node.plugs
            .client
            .call(
                RequestContext::root(),
                RemovePlugVersion {
                    authorization: operator,
                    id: plug_id("photos"),
                    version: version.parse().unwrap(),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        publish(&node, operator, photos("1.1.5", &info)).await,
        Err(downgrade("1.1.5", "1.2.0"))
    );
    publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    publish(&node, operator, photos("1.2.1", &info))
        .await
        .unwrap();
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_synced_version_with_a_bad_signature_is_surfaced_and_never_served() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"forged handler").await;
    publish(&node, operator, photos("1.0.0", &info))
        .await
        .unwrap();
    let record = node
        .plugs
        .client
        .call(
            RequestContext::root(),
            GetPlug {
                authorization: operator,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();

    // Another node's write of a version signed by the wrong key lands on the document.
    let forged = sign_with_seed(unsigned(PHOTOS, "1.1.0", &info), &[9; 32]);
    write_version_datum(&node, &record.doc_id, &forged).await;
    let listed = loop {
        let record = node
            .plugs
            .client
            .call(
                RequestContext::root(),
                GetPlug {
                    authorization: operator,
                    id: plug_id("photos"),
                },
            )
            .await
            .unwrap();
        if record.versions.len() == 2 {
            break record;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let summary = &listed.versions[1];
    assert_eq!(summary.version.to_string(), "1.1.0");
    assert!(
        summary
            .unverified
            .as_deref()
            .is_some_and(|reason| reason.contains("does not verify")),
        "{summary:?}"
    );
    assert_eq!(listed.versions[0].unverified, None);
    assert!(matches!(
        get_version(&node, PHOTOS, "1.1.0").await,
        Err(PlugsError::Unverified { .. })
    ));
    // The genuine versions are untouched.
    assert_eq!(
        get_version(&node, PHOTOS, "1.0.0").await.unwrap().envelope,
        photos("1.0.0", &info)
    );

    // The bad copy never reserved the label: the owner publishes the genuine 1.1.0 over it.
    let genuine = publish(&node, operator, photos("1.1.0", &info))
        .await
        .unwrap();
    assert_eq!(genuine.record.envelope, photos("1.1.0", &info));
    assert_eq!(
        get_version(&node, PHOTOS, "1.1.0").await.unwrap().envelope,
        photos("1.1.0", &info)
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_handle_scoped_to_namespaces_publishes_and_removes_only_under_them() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let info = put(&node, b"scoped handler").await;
    let own = format!("{}/mine", node.plugs.publisher);
    let scoped = |namespaces: Option<&[&str]>| PlugsScope {
        publish: true,
        namespaces: namespaces.map(|namespaces| {
            namespaces
                .iter()
                .map(|namespace| (*namespace).to_owned())
                .collect()
        }),
    };
    // A handle can mint narrower handles only if it carries the NARROW right.
    let mint_as = |authorization: Authorization, scope: PlugsScope, rights: Rights| {
        let node = &node;
        async move {
            node.plugs
                .client
                .call(
                    RequestContext::root(),
                    MintPlugsCapability {
                        authorization,
                        scope,
                        rights,
                    },
                )
                .await
                .map_err(operation)
                .map(|capability| Authorization {
                    authority: capability.capability_id,
                    signer: authorization.signer,
                })
        }
    };
    let handle = mint_as(
        operator(&node),
        scoped(Some(&[big_plugs_core::example_namespace!()])),
        Rights::USE | Rights::NARROW,
    )
    .await
    .unwrap();

    publish(&node, handle, photos("1.0.0", &info))
        .await
        .unwrap();
    assert_eq!(
        publish(&node, handle, unsigned(&own, "1.0.0", &info)).await,
        Err(PlugsError::NamespaceNotPermitted {
            namespace: node.plugs.publisher.clone()
        })
    );
    // The node's convenience signing is not a way around the scope, and removal is scoped too.
    publish(&node, operator(&node), unsigned(&own, "1.0.0", &info))
        .await
        .unwrap();
    assert_eq!(
        node.plugs
            .client
            .call(
                RequestContext::root(),
                RemovePlug {
                    authorization: handle,
                    id: own.parse().unwrap(),
                }
            )
            .await
            .map_err(operation),
        Err(PlugsError::NamespaceNotPermitted {
            namespace: node.plugs.publisher.clone()
        })
    );
    node.plugs
        .client
        .call(
            RequestContext::root(),
            RemovePlug {
                authorization: handle,
                id: plug_id("photos"),
            },
        )
        .await
        .unwrap();

    // Minting narrows: the same or fewer namespaces, never any or more.
    let narrower = mint_as(
        handle,
        scoped(Some(&[big_plugs_core::example_namespace!()])),
        Rights::USE,
    )
    .await
    .unwrap();
    publish(&node, narrower, photos("2.0.0", &info))
        .await
        .unwrap();
    assert_eq!(
        mint_as(handle, scoped(None), Rights::USE).await,
        Err(PlugsError::Denied)
    );
    assert_eq!(
        mint_as(
            handle,
            scoped(Some(&[big_plugs_core::example_namespace!(), "did:web:x"])),
            Rights::USE
        )
        .await,
        Err(PlugsError::Denied)
    );
    shutdown(node, stop).await;
}

const CLAY_MANIFEST: &str = r#"{
    "schema": 1,
    "id": "counter",
    "package_version": 2,
    "authority": "org.example.counter",
    "entrypoints": {
        "initialize": "townframe:dayframe/micro-lifecycle@0.4.0",
        "presentation": "townframe:dayframe/presentation-events@0.4.0",
        "installer": null
    },
    "publications": []
}"#;

/// The smallest thing with a WebAssembly header; the importer does not instantiate it.
const COMPONENT: &[u8] = b"\0asm\x0d\0\x01\0 counter";

async fn micro_pair(
    node: &BigNode,
    component: &[u8],
    manifest: &str,
) -> Result<PublishedPlugVersion, big_plugs::import::ImportError> {
    big_plugs::import::micro_pair(
        &node.plugs.client,
        &node.blobs.service,
        operator(node),
        component.to_vec(),
        manifest.as_bytes(),
    )
    .await
}

#[tokio::test]
async fn clays_package_pair_is_published_under_the_nodes_did_and_signed_by_it() {
    use big_plugs::import::ImportError;
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let published = micro_pair(&node, COMPONENT, CLAY_MANIFEST).await.unwrap();
    let envelope = &published.record.envelope;
    assert_eq!(
        envelope.id.to_string(),
        format!("{}/counter", node.plugs.publisher)
    );
    assert_eq!(envelope.version.to_string(), "2.0.0");
    envelope.verify_signature().unwrap();
    let artifact = envelope.artifact("component").unwrap();
    assert_eq!(artifact.digest, Digest::of(COMPONENT));
    assert_eq!(artifact.size, Some(COMPONENT.len() as u64));
    assert_eq!(artifact.media_type.as_deref(), Some("application/wasm"));
    let micro = envelope.micro().unwrap();
    assert_eq!(micro.schema, big_plugs::import::CLAY_MICRO_MANIFEST_SCHEMA);
    // The description is the manifest minus the members the envelope states itself.
    assert!(!micro.description.contains_key("id"));
    assert!(!micro.description.contains_key("package_version"));
    assert!(!micro.description.contains_key("authority"));
    assert_eq!(envelope.authority.as_deref(), Some("org.example.counter"));
    assert_eq!(micro.description["schema"], json!(1));
    assert_eq!(
        micro.description["entrypoints"]["initialize"],
        "townframe:dayframe/micro-lifecycle@0.4.0"
    );
    // The bytes are in the blob store, as the registry requires of every artifact.
    assert!(
        node.blobs
            .service
            .stat(&big_blobs_core::BlobId::new(artifact.digest.0))
            .await
            .unwrap()
            .is_some()
    );

    // Publishing it again is idempotent, and an older package version is a downgrade.
    assert!(
        micro_pair(&node, COMPONENT, CLAY_MANIFEST)
            .await
            .unwrap()
            .unchanged
    );
    let older = CLAY_MANIFEST.replace("\"package_version\": 2", "\"package_version\": 1");
    assert!(matches!(
        micro_pair(&node, COMPONENT, &older).await,
        Err(ImportError::Registry(PlugsError::Downgrade { .. }))
    ));

    // Bad input is refused before anything is published.
    assert!(matches!(
        micro_pair(&node, b"not wasm", CLAY_MANIFEST).await,
        Err(ImportError::Component(_))
    ));
    assert!(matches!(
        micro_pair(&node, COMPONENT, "[]").await,
        Err(ImportError::Manifest(_))
    ));
    // Clay's real ids are dotted, and a dotted id is its own plug name.
    let dotted = CLAY_MANIFEST.replace("\"counter\"", "\"dayframe.counter\"");
    let published = micro_pair(&node, COMPONENT, &dotted).await.unwrap();
    assert_eq!(
        published.record.id.to_string(),
        format!("{}/dayframe.counter", node.plugs.publisher)
    );
    // The NSID is the format identity, so a manifest without the old integer `schema` imports.
    let unversioned = CLAY_MANIFEST
        .replace("\"schema\": 1,", "")
        .replace("\"counter\"", "\"counter-next\"");
    assert!(!unversioned.contains("\"schema\""));
    micro_pair(&node, COMPONENT, &unversioned).await.unwrap();
    let invalid = CLAY_MANIFEST.replace("\"counter\"", "\"Counter\"");
    assert!(matches!(
        micro_pair(&node, COMPONENT, &invalid).await,
        Err(ImportError::Manifest(reason)) if reason.contains("plug name")
    ));
    assert_eq!(list(&node).await.len(), 3);
    shutdown(node, stop).await;
}

/// A document an author made outside the registry drawer, in the node's local drawer, holding
/// the plug identity and the given versions as they would be written by the author's tooling.
async fn author_document(node: &BigNode, command: &str, id: &str, versions: &[Plug]) -> DocId {
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let create = capability(
        node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let mut datums = BTreeMap::from([(identity_key(), json!({ "id": id }))]);
    for version in versions {
        datums.insert(
            version_key(&version.version),
            serde_json::to_value(version).unwrap(),
        );
    }
    node.docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability: create,
                command: CommandId::try_from(command.to_owned()).unwrap(),
                placement,
                datums,
            },
        )
        .await
        .unwrap()
        .doc_id
}

/// Lets the registry's own actor reach documents in the node's local drawer, which is what an
/// import of a document made there requires.
async fn make_reachable_to_the_registry(node: &BigNode) {
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::protocol::DrawerScope;
    use big_cabinet_core::drawer::protocol::GrantDrawer;
    use big_cabinet_core::drawer::protocol::MintDrawerCapability;
    let drawer = node.drawers.local().present_id;
    let cap = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DrawerScope {
                    drawers: BTreeMap::from([(drawer.clone(), DrawerAccess::Admin)]),
                    create: false,
                },
                rights: Rights::USE | Rights::DELEGATE,
            },
        )
        .await
        .unwrap();
    node.drawers
        .client
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: cap.capability_id,
                drawer_id: drawer,
                subject_id: node.plugs_actor.actor_id,
                access: DrawerAccess::Write,
            },
        )
        .await
        .unwrap();
}

async fn import_document(
    node: &BigNode,
    authorization: Authorization,
    doc_id: &DocId,
) -> Result<big_plugs_core::protocol::PlugRecord, PlugsError> {
    let source_drawer = node.drawers.local().present_id;
    node.plugs
        .client
        .call(
            RequestContext::root(),
            ImportPlugDocument {
                authorization,
                doc_id: doc_id.clone(),
                source_drawer,
            },
        )
        .await
        .map_err(operation)
}

#[tokio::test]
async fn an_author_document_is_imported_and_each_version_is_verified_as_it_is_folded_in() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    make_reachable_to_the_registry(&node).await;
    let info = put(&node, b"authored handler").await;

    // Authored and signed by the namespace owner outside the registry.
    let genuine = photos("1.0.0", &info);
    let doc = author_document(
        &node,
        "author photos",
        PHOTOS,
        std::slice::from_ref(&genuine),
    )
    .await;
    let record = import_document(&node, operator, &doc).await.unwrap();
    assert_eq!(record.id, plug_id("photos"));
    assert_eq!(record.doc_id, doc);
    assert_eq!(record.versions.len(), 1);
    assert_eq!(record.versions[0].unverified, None);
    assert_eq!(listed_ids(&node).await, [plug_id("photos")]);
    assert_eq!(
        get_version(&node, PHOTOS, "1.0.0").await.unwrap().envelope,
        genuine
    );
    // Importing it again is a no-op.
    assert_eq!(
        import_document(&node, operator, &doc).await.unwrap(),
        record
    );

    // A tampered document: signed by another key, and one version edited after signing.
    let other = "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z/other";
    let forged = sign_with_seed(unsigned(other, "1.0.0", &info), &[9; 32]);
    let mut edited = sign_as_example(unsigned(other, "1.1.0", &info));
    edited.title = Some("edited after signing".into());
    let tampered = author_document(&node, "author other", other, &[forged, edited]).await;
    let record = import_document(&node, operator, &tampered).await.unwrap();
    assert_eq!(record.versions.len(), 2);
    for summary in &record.versions {
        assert!(summary.unverified.is_some(), "{summary:?}");
    }
    assert!(matches!(
        get_version(&node, other, "1.0.0").await,
        Err(PlugsError::Unverified { .. })
    ));

    // A document that is not a plug document is refused, and a handle's namespace scope applies
    // to the id the document names.
    let no_identity = {
        let doc = author_document(&node, "author anonymous", PHOTOS, &[]).await;
        // Drop the identity: a plain document in the same drawer.
        node.docs
            .client
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: capability(
                        &node,
                        DocScope {
                            write: BTreeSet::from([doc.clone()]),
                            ..DocScope::default()
                        },
                    )
                    .await,
                    command: None,
                    doc_id: doc.clone(),
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::new(),
                    remove: BTreeSet::from([identity_key()]),
                },
            )
            .await
            .unwrap();
        doc
    };
    assert!(matches!(
        import_document(&node, operator, &no_identity).await,
        Err(PlugsError::Rejected(_))
    ));
    let scoped = mint(
        &node,
        PlugsScope {
            publish: true,
            namespaces: Some(BTreeSet::from(["did:key:z6MkOther".to_owned()])),
        },
    )
    .await;
    let outside = author_document(&node, "author scoped", PHOTOS, &[genuine]).await;
    assert!(matches!(
        import_document(&node, scoped, &outside).await,
        Err(PlugsError::NamespaceNotPermitted { .. })
    ));
    let reader = mint(
        &node,
        PlugsScope {
            publish: false,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        import_document(&node, reader, &outside).await,
        Err(PlugsError::Denied)
    );
    shutdown(node, stop).await;
}

async fn listed_ids(node: &BigNode) -> Vec<big_plugs_core::PlugId> {
    list(node)
        .await
        .into_iter()
        .map(|record| record.id)
        .collect()
}
