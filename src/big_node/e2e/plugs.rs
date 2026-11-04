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
use big_plugs_core::registry::version_key;
use big_plugs_core::test_support::plug_id;
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

/// `photos` at `version` with one artifact, `handler`, holding `info`'s bytes.
fn photos(version: &str, info: &BlobInfo) -> Plug {
    let mut plug = Plug::new(PHOTOS.parse().unwrap(), version.parse().unwrap());
    plug.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest(info.blob_id.to_bytes32()),
        size: Some(info.length_octets),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    plug
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
    let reader = mint(&node, PlugsScope { publish: false }).await;
    let publisher = mint(&node, PlugsScope { publish: true }).await;
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
                    scope: PlugsScope { publish: true },
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
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: capability(
                    &node,
                    DocScope {
                        write: BTreeSet::from([record.doc_id.clone()]),
                        ..DocScope::default()
                    },
                )
                .await,
                command: None,
                doc_id: record.doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(
                    version_key(&"1.0.0".parse().unwrap()),
                    serde_json::to_value(&rewritten).unwrap(),
                )]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();

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
