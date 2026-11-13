//! The cartridge registry end to end: the service over the real drawer, docs and blob stack.

use super::blobs::put_cap;
use super::*;
use big_blobs_core::BlobInfo;
use big_cabinet_core::DatumPin;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;

use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component as ManifestComponent;
use big_cartridges_core::Digest;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::protocol::CartridgeChanges;
use big_cartridges_core::protocol::CartridgesCursor;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::CartridgesScope;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::GetCartridgeVersion;
use big_cartridges_core::protocol::GetPublisher;
use big_cartridges_core::protocol::ImportCartridgeDocument;
use big_cartridges_core::protocol::ListCartridges;
use big_cartridges_core::protocol::MintCartridgesCapability;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::PublishedCartridgeVersion;
use big_cartridges_core::protocol::RemoveCartridge;
use big_cartridges_core::protocol::RemoveCartridgeVersion;
use big_cartridges_core::protocol::ResolveCartridgeVersion;
use big_cartridges_core::protocol::WatchCartridges;
use big_cartridges_core::registry::identity_key;
use big_cartridges_core::registry::version_key;
use big_cartridges_core::signature::SignatureError;
use big_cartridges_core::test_support::FixtureCartridge;
use big_cartridges_core::test_support::cartridge_id;
use big_cartridges_core::test_support::sign_as_example;
use big_cartridges_core::test_support::sign_with_seed;
use method_core::ByteBody;
use service_utils_rs::ClientError;
use std::time::Duration;

const PHOTOS: &str = example_cartridge!("photos");

pub(super) fn operator(node: &BigNode) -> Authorization {
    Authorization::identity(node.authority.root)
}

/// An actor under the operator presenting its own identity: another actor than the one a handle
/// minted by the operator is bound to.
pub(super) async fn bystander(node: &BigNode) -> Authorization {
    let actor = node
        .authority
        .client
        .call(
            RequestContext::root(),
            big_cabinet_core::authority::OpenActor {
                parent: node.authority.root,
                path: "bystander".into(),
                rights: Rights::all(),
            },
        )
        .await
        .unwrap();
    Authorization::identity(actor.capability)
}

fn operation<E>(error: ClientError<E>) -> E {
    match error {
        ClientError::Method(error) => error,
        ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
    }
}

async fn put(node: &BigNode, bytes: &[u8]) -> BlobInfo {
    node.blobs
        .put(
            RequestContext::root(),
            put_cap(node).await,
            ByteBody::from_bytes(bytes.to_vec()),
        )
        .await
        .unwrap()
}

/// `id` at `version` with one component, `handler`, holding `info`'s bytes.
fn fixture(id: &str, version: &str, info: &BlobInfo) -> FixtureCartridge {
    let mut fixture = FixtureCartridge::new(Cartridge::new(
        id.parse().unwrap(),
        version.parse().unwrap(),
    ));
    fixture.with_wasm_digest(
        "handler",
        Digest(info.blob_id.to_bytes32()),
        info.length_octets,
    );
    fixture
}

/// [`fixture`] as the envelope the registry's staging of its payloads hands back; `info`'s bytes must be in the
/// blob store: the envelope the registry's staging of those payloads hands back, not yet signed.
/// A version already published is not staged again; its own pins are reused, so the same call
/// yields the same envelope before and after it is published.
async fn unsigned(node: &BigNode, id: &str, version: &str, info: &BlobInfo) -> Cartridge {
    let fixture = fixture(id, version, info);
    if let Ok(published) = get_version(node, id, version).await {
        return fixture.staged(published.envelope);
    }
    let staged = stage(node, operator(node), &fixture).await.unwrap();
    fixture.staged(staged)
}

async fn stage(
    node: &BigNode,
    authorization: Authorization,
    fixture: &FixtureCartridge,
) -> Result<Cartridge, CartridgesError> {
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            fixture.stage_request(),
        )
        .await
        .map_err(operation)
}

/// `photos` under the example namespace, signed by its owner as an external publisher would.
async fn photos(node: &BigNode, version: &str, info: &BlobInfo) -> Cartridge {
    sign_as_example(unsigned(node, PHOTOS, version, info).await)
}

/// A write to one version slot of a registry document, as a synced change from another node
/// would land: straight on the document, past the registry service.
async fn write_version_datum(node: &BigNode, doc_id: &DocId, envelope: &Cartridge) {
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: capability(
                    node,
                    DocScope {
                        write: std::collections::BTreeMap::from([(
                            doc_id.clone(),
                            big_cabinet_core::doc::DatumKinds::All,
                        )]),
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
) -> Result<big_cartridges_core::protocol::CartridgeVersionRecord, CartridgesError> {
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            GetCartridgeVersion {
                id: id.parse().unwrap(),
                version: version.parse().unwrap(),
            },
        )
        .await
        .map_err(operation)
}

pub(super) async fn publish(
    node: &BigNode,
    authorization: Authorization,
    envelope: Cartridge,
) -> Result<PublishedCartridgeVersion, CartridgesError> {
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            PublishCartridgeVersion { envelope },
        )
        .await
        .map_err(operation)
}

async fn mint(node: &BigNode, scope: CartridgesScope) -> Authorization {
    let capability = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            MintCartridgesCapability {
                scope,
                rights: Rights::USE,
                subject: None,
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
    after: Option<CartridgesCursor>,
    hold_ms: u32,
) -> Result<CartridgeChanges, CartridgesError> {
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            WatchCartridges {
                after,
                limit: 0,
                hold_ms,
            },
        )
        .await
        .map_err(operation)
}

async fn list(node: &BigNode) -> Vec<big_cartridges_core::protocol::CartridgeRecord> {
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            ListCartridges {},
        )
        .await
        .unwrap()
        .cartridges
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

async fn boot_memory(secrets: &Arc<SecretStore>) -> (BigNode, StopToken, Database) {
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        Arc::clone(secrets),
        sql.clone(),
        big_repo::Storage::memory(&runtime).unwrap(),
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
    let published = publish(&node, operator, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();
    assert!(!published.unchanged);
    assert_eq!(
        published.record.envelope,
        photos(&node, "1.0.0", &info).await
    );
    publish(&node, operator, photos(&node, "1.1.0", &info).await)
        .await
        .unwrap();

    let list = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            ListCartridges {},
        )
        .await
        .unwrap()
        .cartridges;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, cartridge_id("photos"));
    assert_eq!(
        list[0]
            .versions
            .iter()
            .map(|summary| summary.version.to_string())
            .collect::<Vec<_>>(),
        ["1.0.0", "1.1.0"]
    );
    let record = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridge {
                id: cartridge_id("photos"),
            },
        )
        .await
        .unwrap();
    assert_eq!(record, list[0]);
    let version = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridgeVersion {
                id: cartridge_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(version.envelope, photos(&node, "1.0.0", &info).await);
    assert_eq!(
        version.revision,
        photos(&node, "1.0.0", &info).await.revision()
    );
    assert_eq!(version.conflict, None);

    // A read handle lists and gets but does not publish or remove; a publish handle does; an
    // actor presenting only its own identity is refused outright.
    let reader = mint(
        &node,
        CartridgesScope {
            publish: false,
            ..Default::default()
        },
    )
    .await;
    let publisher = mint(
        &node,
        CartridgesScope {
            publish: true,
            ..Default::default()
        },
    )
    .await;
    node.cartridges
        .client
        .call_with(RequestContext::root(), reader.metadata(), ListCartridges {})
        .await
        .unwrap();
    assert_eq!(
        publish(&node, reader, photos(&node, "2.0.0", &info).await).await,
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                reader.metadata(),
                RemoveCartridge {
                    id: cartridge_id("photos")
                }
            )
            .await
            .map_err(operation),
        Err(CartridgesError::Denied)
    );
    publish(&node, publisher, photos(&node, "2.0.0", &info).await)
        .await
        .unwrap();
    assert_eq!(
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                Authorization::identity(node.docs_actor.capability).metadata(),
                ListCartridges {}
            )
            .await
            .map_err(operation),
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                reader.metadata(),
                MintCartridgesCapability {
                    scope: CartridgesScope {
                        publish: true,
                        ..Default::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                }
            )
            .await
            .map_err(operation),
        Err(CartridgesError::Denied)
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_published_version_never_changes_content() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"photos handler").await;
    let first = publish(&node, operator, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();
    let again = publish(&node, operator, first.record.envelope.clone())
        .await
        .unwrap();
    assert!(again.unchanged);
    assert_eq!(again.record, first.record);

    let mut changed = photos(&node, "1.0.0", &info).await;
    changed.title = Some("Photos, renamed".into());
    let changed = sign_as_example(changed);
    assert_eq!(
        publish(&node, operator, changed.clone()).await,
        Err(CartridgesError::VersionExists {
            id: cartridge_id("photos"),
            version: Box::new("1.0.0".parse().unwrap()),
            existing: first.record.revision,
            incoming: changed.revision(),
        })
    );
    // The refusal left the stored version as it was.
    let kept = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridgeVersion {
                id: cartridge_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(kept.envelope.title, None);

    // Removal leaves a tombstone: the label is never reused for other content, though the
    // identical envelope may be published again.
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            RemoveCartridgeVersion {
                id: cartridge_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        publish(&node, operator, changed).await,
        Err(CartridgesError::VersionExists { .. })
    ));
    let restored = publish(&node, operator, first.record.envelope.clone())
        .await
        .unwrap();
    assert_eq!(restored.record.revision, first.record.revision);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn staging_refuses_components_whose_bytes_are_not_local() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let absent = BlobInfo {
        blob_id: big_blobs_core::BlobId::new(Digest::of(b"never stored").0),
        length_octets: 12,
    };
    let mut fixture = FixtureCartridge::new(Cartridge::new(
        PHOTOS.parse().unwrap(),
        "1.0.0".parse().unwrap(),
    ));
    fixture.with_wasm_digest(
        "handler",
        Digest(absent.blob_id.to_bytes32()),
        absent.length_octets,
    );
    assert_eq!(
        stage(&node, operator(&node), &fixture).await,
        Err(CartridgesError::ArtifactUnavailable {
            id: cartridge_id("photos"),
            artifact: "handler".into(),
        })
    );
    let listed = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(&node).metadata(),
            ListCartridges {},
        )
        .await
        .unwrap()
        .cartridges;
    assert!(listed.is_empty());
    shutdown(node, stop).await;
}

#[tokio::test]
async fn artifacts_are_retained_while_published_and_released_after_removal() {
    let secrets = secrets().await;
    let (node, stop, sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"retained handler bytes").await;
    publish(&node, operator, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();
    let doc = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridge {
                id: cartridge_id("photos"),
            },
        )
        .await
        .unwrap()
        .doc_id;
    // The blob workers pick the registry document up through its `Blob` datum.
    while retained(&sql, &doc).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            RemoveCartridgeVersion {
                id: cartridge_id("photos"),
                version: "1.0.0".parse().unwrap(),
            },
        )
        .await
        .unwrap();
    while !retained(&sql, &doc).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The cartridge itself stays, with no versions, until it is removed.
    let record = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridge {
                id: cartridge_id("photos"),
            },
        )
        .await
        .unwrap();
    assert!(record.versions.is_empty());
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            RemoveCartridge {
                id: cartridge_id("photos"),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridge {
                    id: cartridge_id("photos"),
                },
            )
            .await
            .map_err(operation),
        Err(CartridgesError::UnknownCartridge(cartridge_id("photos")))
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn the_registry_is_rebuilt_from_its_drawer_after_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let secrets = secrets().await;
    let sql_path = directory.path().join("cabinet.sqlite");
    let boot = async |runtime: Arc<SqliteRuntime>| {
        let mut config = config(
            Arc::clone(&secrets),
            runtime.database(&sql_path, &[], "_sqlite_rs_migrations"),
            big_repo::Storage::in_directory(&runtime, &directory.path().join("repo"), None)
                .unwrap(),
        );
        config.blob_storage = big_blobs::Storage::filesystem(directory.path().join("blobs"))
            .await
            .unwrap();
        BigNode::boot(config).await.unwrap()
    };
    let (node, stop) = boot(Arc::new(SqliteRuntime::new(Default::default()))).await;
    let operator = operator(&node);
    let info = put(&node, b"persistent handler").await;
    let published = publish(&node, operator, photos(&node, "1.0.0", &info).await)
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
    let mut changed = photos(&node, "1.0.0", &info).await;
    changed.description = Some("different".into());
    let changed = sign_as_example(changed);
    assert!(matches!(
        publish(&node, Authorization::identity(node.authority.root), changed).await,
        Err(CartridgesError::VersionExists { existing, .. }) if existing == published.record.revision
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
    // Staging creates the cartridge's document, which the watch reports as a change of its own,
    // so the cursor is taken once that has settled and the publish is the only change after it.
    let watched = photos(&node, "1.0.0", &info).await;
    let start = watch(&node, None, 10_000).await.unwrap();
    assert!(start.reset && start.caught_up);
    let held = {
        let node = &node;
        let cursor = start.through;
        async move { watch(node, Some(cursor), 10_000).await.unwrap() }
    };
    let publishing = async {
        // The watch is parked before the publish lands.
        tokio::time::sleep(Duration::from_millis(100)).await;
        publish(&node, operator, watched).await.unwrap();
    };
    let (published, ()) = tokio::join!(held, publishing);
    assert!(!published.reset && published.caught_up);
    assert_eq!(published.changes.len(), 1);
    assert_eq!(published.changes[0].cartridge_id, cartridge_id("photos"));
    assert!(!published.changes[0].removed);
    assert_eq!(published.through.seq, published.changes[0].seq);

    // Nothing has changed since, so a short hold expires as an empty page.
    let quiet = watch(&node, Some(published.through), 50).await.unwrap();
    assert!(quiet.changes.is_empty() && quiet.caught_up);
    assert_eq!(quiet.through, published.through);

    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            RemoveCartridge {
                id: cartridge_id("photos"),
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
        Some(CartridgesCursor {
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
    let original = photos(&node, "1.0.0", &info).await;
    publish(&node, operator, original.clone()).await.unwrap();
    let record = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridge {
                id: cartridge_id("photos"),
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
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridgeVersion {
                    id: cartridge_id("photos"),
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
        publish(
            &node,
            operator,
            unsigned(&node, PHOTOS, "1.0.0", &info).await
        )
        .await,
        Err(CartridgesError::UnsignedForeignNamespace {
            namespace: big_cartridges_core::example_namespace!().into(),
        })
    );
    // Signed with another key than the namespace's.
    assert!(matches!(
        publish(
            &node,
            operator,
            sign_with_seed(unsigned(&node, PHOTOS, "1.0.0", &info).await, &[9; 32])
        )
        .await,
        Err(CartridgesError::Signature(SignatureError::Invalid(_)))
    ));
    // A signature that no longer matches the envelope it is attached to.
    let mut tampered = photos(&node, "1.0.0", &info).await;
    tampered.title = Some("edited after signing".into());
    assert!(matches!(
        publish(&node, operator, tampered).await,
        Err(CartridgesError::Signature(SignatureError::Invalid(_)))
    ));
    // A DID whose method cannot be verified without resolution fails closed, signed or not.
    let web = sign_as_example(unsigned(&node, "did:web:example.com/photos", "1.0.0", &info).await);
    assert_eq!(
        publish(&node, operator, web).await,
        Err(CartridgesError::Signature(
            SignatureError::UnsupportedMethod("web".into())
        ))
    );
    // Staging made the cartridge's document, but no version was published.
    assert!(
        list(&node)
            .await
            .iter()
            .all(|cartridge| cartridge.versions.is_empty())
    );

    // The owner's signature is accepted, and survives the round trip through the drawer.
    let published = publish(&node, operator, photos(&node, "1.0.0", &info).await)
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
        .cartridges
        .client
        .call_with(RequestContext::root(), operator.metadata(), GetPublisher {})
        .await
        .unwrap()
        .publisher;
    assert_eq!(publisher, node.cartridges.publisher);
    // The DID is the did:key of the authority's node key, the one that issues attribution
    // certificates.
    assert_eq!(
        publisher,
        big_cartridges_core::signature::did_key(&node.authority.service.node_public_key())
    );

    let own = format!("{publisher}/mine");
    let published = publish(&node, operator, unsigned(&node, &own, "1.0.0", &info).await)
        .await
        .unwrap();
    assert!(published.record.envelope.signature.is_some());
    published.record.envelope.verify_signature().unwrap();
    assert_eq!(published.record.id.to_string(), own);
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_new_label_must_order_after_every_label_the_cartridge_has_or_had() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let info = put(&node, b"ordered handler").await;
    publish(&node, operator, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();
    publish(&node, operator, photos(&node, "1.1.0", &info).await)
        .await
        .unwrap();

    // A validly signed older version is not an upgrade, even though nothing is wrong with it.
    let downgrade = |version: &str, newest: &str| CartridgesError::Downgrade {
        id: cartridge_id("photos"),
        version: Box::new(version.parse().unwrap()),
        newest: Box::new(newest.parse().unwrap()),
    };
    assert_eq!(
        publish(&node, operator, photos(&node, "0.9.0", &info).await).await,
        Err(downgrade("0.9.0", "1.1.0"))
    );
    assert_eq!(
        publish(&node, operator, photos(&node, "1.0.5", &info).await).await,
        Err(downgrade("1.0.5", "1.1.0"))
    );
    assert_eq!(
        publish(&node, operator, photos(&node, "1.1.0-rc.1", &info).await).await,
        Err(downgrade("1.1.0-rc.1", "1.1.0"))
    );
    // Republishing an existing label's identical content is not a new label.
    let v100 = photos(&node, "1.0.0", &info).await;
    assert!(
        publish(&node, operator, v100.clone())
            .await
            .unwrap()
            .unchanged
    );

    // A removed version still counts: the newest label stays the bar after it is gone, and the
    // older one's identical content may still be published again.
    publish(&node, operator, photos(&node, "1.2.0", &info).await)
        .await
        .unwrap();
    for version in ["1.2.0", "1.0.0"] {
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                RemoveCartridgeVersion {
                    id: cartridge_id("photos"),
                    version: version.parse().unwrap(),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        publish(&node, operator, photos(&node, "1.1.5", &info).await).await,
        Err(downgrade("1.1.5", "1.2.0"))
    );
    publish(&node, operator, v100).await.unwrap();
    publish(&node, operator, photos(&node, "1.2.1", &info).await)
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
    publish(&node, operator, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();
    let record = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridge {
                id: cartridge_id("photos"),
            },
        )
        .await
        .unwrap();

    // Another node's write of a version signed by the wrong key lands on the document. The
    // genuine 1.1.0 is staged first: once the forged one is there the label can no longer be.
    let genuine_110 = photos(&node, "1.1.0", &info).await;
    let forged = sign_with_seed(unsigned(&node, PHOTOS, "1.1.0", &info).await, &[9; 32]);
    write_version_datum(&node, &record.doc_id, &forged).await;
    let listed = loop {
        let record = node
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridge {
                    id: cartridge_id("photos"),
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
        Err(CartridgesError::Unverified { .. })
    ));
    // The genuine versions are untouched.
    assert_eq!(
        get_version(&node, PHOTOS, "1.0.0").await.unwrap().envelope,
        photos(&node, "1.0.0", &info).await
    );

    // The bad copy never reserved the label: the owner publishes the genuine 1.1.0 over it.
    let genuine = publish(&node, operator, genuine_110.clone()).await.unwrap();
    assert_eq!(genuine.record.envelope, genuine_110);
    assert_eq!(
        get_version(&node, PHOTOS, "1.1.0").await.unwrap().envelope,
        genuine_110
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_handle_presented_by_an_actor_it_is_not_bound_to_is_refused() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let info = put(&node, b"held handle").await;
    let held = mint(
        &node,
        CartridgesScope {
            publish: true,
            namespaces: None,
        },
    )
    .await;
    publish(&node, held, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();

    // The handle is bound to the operator: another actor presenting its id under its own signer
    // is refused for a write and a read alike.
    let stolen = Authorization {
        authority: held.authority,
        signer: bystander(&node).await.signer,
    };
    assert_eq!(
        publish(&node, stolen, photos(&node, "1.1.0", &info).await).await,
        Err(CartridgesError::Denied)
    );
    let list_as = async |authorization: Authorization| {
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                authorization.metadata(),
                ListCartridges {},
            )
            .await
            .map_err(operation)
    };
    assert_eq!(list_as(stolen).await, Err(CartridgesError::Denied));
    assert!(list_as(held).await.is_ok());
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_handle_scoped_to_namespaces_publishes_and_removes_only_under_them() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let info = put(&node, b"scoped handler").await;
    let own = format!("{}/mine", node.cartridges.publisher);
    let scoped = |namespaces: Option<&[&str]>| CartridgesScope {
        publish: true,
        namespaces: namespaces.map(|namespaces| {
            namespaces
                .iter()
                .map(|namespace| (*namespace).to_owned())
                .collect()
        }),
    };
    // A handle can mint narrower handles only if it carries the NARROW right.
    let mint_as = |authorization: Authorization, scope: CartridgesScope, rights: Rights| {
        let node = &node;
        async move {
            node.cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    authorization.metadata(),
                    MintCartridgesCapability {
                        scope,
                        rights,
                        subject: None,
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
        scoped(Some(&[big_cartridges_core::example_namespace!()])),
        Rights::USE | Rights::NARROW,
    )
    .await
    .unwrap();

    publish(&node, handle, photos(&node, "1.0.0", &info).await)
        .await
        .unwrap();
    assert_eq!(
        publish(&node, handle, unsigned(&node, &own, "1.0.0", &info).await).await,
        Err(CartridgesError::NamespaceNotPermitted {
            namespace: node.cartridges.publisher.clone()
        })
    );
    // The node's convenience signing is not a way around the scope, and removal is scoped too.
    publish(
        &node,
        operator(&node),
        unsigned(&node, &own, "1.0.0", &info).await,
    )
    .await
    .unwrap();
    assert_eq!(
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                handle.metadata(),
                RemoveCartridge {
                    id: own.parse().unwrap(),
                }
            )
            .await
            .map_err(operation),
        Err(CartridgesError::NamespaceNotPermitted {
            namespace: node.cartridges.publisher.clone()
        })
    );
    node.cartridges
        .client
        .call_with(
            RequestContext::root(),
            handle.metadata(),
            RemoveCartridge {
                id: cartridge_id("photos"),
            },
        )
        .await
        .unwrap();

    // Minting narrows: the same or fewer namespaces, never any or more.
    let narrower = mint_as(
        handle,
        scoped(Some(&[big_cartridges_core::example_namespace!()])),
        Rights::USE,
    )
    .await
    .unwrap();
    publish(&node, narrower, photos(&node, "2.0.0", &info).await)
        .await
        .unwrap();
    assert_eq!(
        mint_as(handle, scoped(None), Rights::USE).await,
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        mint_as(
            handle,
            scoped(Some(&[
                big_cartridges_core::example_namespace!(),
                "did:web:x"
            ])),
            Rights::USE
        )
        .await,
        Err(CartridgesError::Denied)
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
) -> Result<PublishedCartridgeVersion, big_cartridges::import::ImportError> {
    big_cartridges::import::micro_pair(
        &node.cartridges.client,
        &node.blobs.service,
        operator(node),
        component.to_vec(),
        manifest.as_bytes(),
    )
    .await
}

#[tokio::test]
async fn clays_package_pair_is_published_under_the_nodes_did_and_signed_by_it() {
    use big_cartridges::import::ImportError;
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let published = micro_pair(&node, COMPONENT, CLAY_MANIFEST).await.unwrap();
    let envelope = &published.record.envelope;
    assert_eq!(
        envelope.id.to_string(),
        format!("{}/counter", node.cartridges.publisher)
    );
    assert_eq!(envelope.version.to_string(), "2.0.0");
    envelope.verify_signature().unwrap();
    let resolved = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(&node).metadata(),
            ResolveCartridgeVersion {
                id: envelope.id.clone(),
                version: envelope.version.clone(),
            },
        )
        .await
        .unwrap();
    let ManifestComponent::Wasm(component) = &resolved.components["component"] else {
        panic!("micro component is not wasm")
    };
    assert_eq!(component.digest, Digest::of(COMPONENT));
    assert_eq!(component.size, COMPONENT.len() as u64);
    assert_eq!(component.media_type, "application/wasm");
    let micro = envelope.micro().unwrap();
    assert_eq!(
        micro.schema,
        big_cartridges::import::CLAY_MICRO_MANIFEST_SCHEMA
    );
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
            .stat(&big_blobs_core::BlobId::new(component.digest.0))
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
        Err(ImportError::Registry(CartridgesError::Downgrade { .. }))
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
    // Clay's real ids are dotted, and a dotted id is its own cartridge name.
    let dotted = CLAY_MANIFEST.replace("\"counter\"", "\"dayframe.counter\"");
    let published = micro_pair(&node, COMPONENT, &dotted).await.unwrap();
    assert_eq!(
        published.record.id.to_string(),
        format!("{}/dayframe.counter", node.cartridges.publisher)
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
        Err(ImportError::Manifest(reason)) if reason.contains("cartridge name")
    ));
    assert_eq!(list(&node).await.len(), 3);
    shutdown(node, stop).await;
}

/// A document an author made outside the registry drawer, in the node's local drawer, holding
/// the cartridge identity and the given versions as they would be written by the author's tooling:
/// the payload datums, the typed components pinning their blobs, and each version's envelope
/// pinning both, signed by `sign` (given the version's index). Returns the document and the
/// envelopes as written.
async fn author_document(
    node: &BigNode,
    command: &str,
    id: &str,
    versions: &[FixtureCartridge],
    sign: impl Fn(usize, Cartridge) -> Cartridge,
) -> (DocId, Vec<Cartridge>) {
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let (doc, envelopes) = write_registry_doc(node, placement, command, id, versions, sign).await;
    (doc, envelopes)
}

/// Creates a registry-shaped document in `placement` and writes `versions` into it in the order
/// the registry's staging does (see [`author_document`]), without going through the registry
/// service: the way a version reaches a node that did not publish it, or whose component bytes are
/// not in its blob store.
pub(super) async fn write_registry_doc(
    node: &BigNode,
    placement: String,
    command: &str,
    id: &str,
    versions: &[FixtureCartridge],
    sign: impl Fn(usize, Cartridge) -> Cartridge,
) -> (DocId, Vec<Cartridge>) {
    use big_blobs_core::Blob;
    use big_cabinet_core::DatumKey;
    use big_cabinet_core::doc::ChangeHash;
    use big_cabinet_core::reference::DatumReference;
    use big_cabinet_core::reference::Location;
    use big_cabinet_core::reference::ReferenceDocId;
    use big_cartridges_core::registry::artifact_key;
    use big_cartridges_core::registry::component_key;
    use big_cartridges_core::registry::lexicon_key;
    use big_cartridges_core::registry::sidecar_key;

    let pin_for = |doc: &DocId, key: DatumKey, heads: &[ChangeHash]| {
        DatumPin::new(
            Location::new(
                DatumReference::new(ReferenceDocId::Concrete(doc.clone()), key)
                    .pinned(heads.iter().copied()),
                None,
            )
            .unwrap(),
        )
    };
    let mut payload = BTreeMap::from([(
        identity_key(),
        serde_json::to_value(big_cartridges_core::registry::CartridgeIdentity {
            id: id.parse().unwrap(),
            extra: Default::default(),
        })
        .unwrap(),
    )]);
    for fixture in versions {
        let version = &fixture.version;
        for (nsid, file) in &fixture.resolved.lexicons {
            payload.insert(
                lexicon_key(version, nsid),
                serde_json::to_value(file).unwrap(),
            );
        }
        for (nsid, document) in &fixture.resolved.sidecars {
            payload.insert(
                sidecar_key(version, nsid),
                serde_json::to_value(document).unwrap(),
            );
        }
        for (key, component) in &fixture.resolved.components {
            let ManifestComponent::Wasm(wasm) = component else {
                panic!("a fixture component is wasm")
            };
            let blob = Blob {
                mime: wasm.media_type.clone(),
                length_octets: wasm.size,
                digest: wasm.digest.to_string(),
                inline: None,
                urls: None,
                extra: Default::default(),
            };
            let mut raw = serde_json::to_value(&blob).unwrap();
            raw.as_object_mut()
                .unwrap()
                .retain(|_, value| !value.is_null());
            payload.insert(artifact_key(version, key), raw);
        }
    }
    let create = capability(
        node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let created = node
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability: create,
                command: CommandId::try_from(command.to_owned()).unwrap(),
                placement,
                datums: payload,
            },
        )
        .await
        .unwrap();
    let doc = created.doc_id.clone();
    let write_capability = capability(
        node,
        DocScope {
            write: BTreeMap::from([(doc.clone(), big_cabinet_core::doc::DatumKinds::All)]),
            ..DocScope::default()
        },
    )
    .await;
    let write = async |set: BTreeMap<DatumKey, serde_json::Value>| {
        node.docs
            .client
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: write_capability,
                    command: None,
                    doc_id: doc.clone(),
                    mode: WriteMode::AtCurrent,
                    set,
                    remove: BTreeSet::new(),
                },
            )
            .await
            .unwrap()
            .heads
    };
    let mut typed = BTreeMap::new();
    for fixture in versions {
        for (key, component) in &fixture.resolved.components {
            let ManifestComponent::Wasm(wasm) = component else {
                panic!("a fixture component is wasm")
            };
            let mut wasm = wasm.clone();
            wasm.bytes = pin_for(&doc, artifact_key(&fixture.version, key), &created.heads);
            typed.insert(
                component_key(&fixture.version, key),
                serde_json::to_value(ManifestComponent::Wasm(wasm)).unwrap(),
            );
        }
    }
    let heads = if typed.is_empty() {
        created.heads.clone()
    } else {
        write(typed).await
    };
    let mut envelopes = Vec::new();
    for (index, fixture) in versions.iter().enumerate() {
        let version = &fixture.version;
        let mut template = Cartridge::new(fixture.id.clone(), version.clone());
        template.lexicons = fixture
            .resolved
            .lexicons
            .keys()
            .map(|nsid| {
                (
                    nsid.clone(),
                    pin_for(&doc, lexicon_key(version, nsid), &heads),
                )
            })
            .collect();
        template.sidecars = fixture
            .resolved
            .sidecars
            .keys()
            .map(|nsid| {
                (
                    nsid.clone(),
                    pin_for(&doc, sidecar_key(version, nsid), &heads),
                )
            })
            .collect();
        template.components = fixture
            .resolved
            .components
            .keys()
            .map(|key| {
                (
                    key.clone(),
                    pin_for(&doc, component_key(version, key), &heads),
                )
            })
            .collect();
        envelopes.push(sign(index, fixture.staged(template)));
    }
    if !envelopes.is_empty() {
        write(
            envelopes
                .iter()
                .map(|envelope| {
                    (
                        version_key(&envelope.version),
                        serde_json::to_value(envelope).unwrap(),
                    )
                })
                .collect(),
        )
        .await;
    }
    (doc, envelopes)
}

/// Installs the cartridge document `doc_id`, found in `drawer`, into the registry as `authorization`
/// asks. The operator lends the registry the two handles the import needs, one to read the
/// document and one reaching it through `drawer`, and retracts them once the call returns: the
/// registry keeps no standing access to `drawer`. The operator reaches every drawer without a
/// grant, including one that synced in from a peer.
pub(super) async fn import_document(
    node: &BigNode,
    authorization: Authorization,
    drawer: &DocId,
    doc_id: &DocId,
) -> Result<big_cartridges_core::protocol::CartridgeRecord, CartridgesError> {
    use big_cabinet_core::authority::RetractCapability;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::protocol::DrawerScope;
    use big_cabinet_core::drawer::protocol::MintDrawerCapability;
    let read = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            MintDocCapability {
                scope: DocScope {
                    read: BTreeSet::from([doc_id.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let reach = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::from([(drawer.clone(), DrawerAccess::Read)]),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let imported = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            ImportCartridgeDocument {
                doc_id: doc_id.clone(),
                read,
                reach,
            },
        )
        .await
        .map_err(operation);
    for capability in [read, reach] {
        node.authority
            .client
            .call(
                RequestContext::root(),
                RetractCapability {
                    via: capability,
                    capability,
                },
            )
            .await
            .unwrap();
    }
    imported
}

#[tokio::test]
async fn an_author_document_is_imported_and_each_version_is_verified_as_it_is_folded_in() {
    let secrets = secrets().await;
    let (node, stop, _sql) = boot_memory(&secrets).await;
    let operator = operator(&node);
    let present = node.drawers.local().present_id;
    let info = put(&node, b"authored handler").await;

    // Authored and signed by the namespace owner outside the registry.
    let (doc, mut authored) = author_document(
        &node,
        "author photos",
        PHOTOS,
        &[fixture(PHOTOS, "1.0.0", &info)],
        |_, envelope| sign_as_example(envelope),
    )
    .await;
    let genuine = authored.remove(0);
    let record = import_document(&node, operator, &present, &doc)
        .await
        .unwrap();
    assert_eq!(record.id, cartridge_id("photos"));
    assert_eq!(record.doc_id, doc);
    assert_eq!(record.versions.len(), 1);
    assert_eq!(record.versions[0].unverified, None);
    assert_eq!(listed_ids(&node).await, [cartridge_id("photos")]);
    assert_eq!(
        get_version(&node, PHOTOS, "1.0.0").await.unwrap().envelope,
        genuine
    );
    // Importing it again is a no-op.
    assert_eq!(
        import_document(&node, operator, &present, &doc)
            .await
            .unwrap(),
        record
    );
    // The document is the author's, entered at Read: the registry serves it and never writes it,
    // so neither a new version nor a removal lands in it.
    assert_eq!(
        stage(&node, operator, &fixture(PHOTOS, "1.1.0", &info)).await,
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        node.cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                RemoveCartridgeVersion {
                    id: cartridge_id("photos"),
                    version: "1.0.0".parse().unwrap(),
                },
            )
            .await
            .map_err(operation),
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        get_version(&node, PHOTOS, "1.0.0").await.unwrap().envelope,
        genuine
    );

    // A tampered document: signed by another key, and one version edited after signing.
    let other = "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z/other";
    let (tampered, _) = author_document(
        &node,
        "author other",
        other,
        &[
            fixture(other, "1.0.0", &info),
            fixture(other, "1.1.0", &info),
        ],
        |index, envelope| match index {
            0 => sign_with_seed(envelope, &[9; 32]),
            _ => {
                let mut edited = sign_as_example(envelope);
                edited.title = Some("edited after signing".into());
                edited
            }
        },
    )
    .await;
    let record = import_document(&node, operator, &present, &tampered)
        .await
        .unwrap();
    assert_eq!(record.versions.len(), 2);
    for summary in &record.versions {
        assert!(summary.unverified.is_some(), "{summary:?}");
    }
    assert!(matches!(
        get_version(&node, other, "1.0.0").await,
        Err(CartridgesError::Unverified { .. })
    ));

    // A document that is not a cartridge document is refused, and a handle's namespace scope applies
    // to the id the document names.
    let no_identity = {
        let (doc, _) = author_document(&node, "author anonymous", PHOTOS, &[], |_, envelope| {
            envelope
        })
        .await;
        // Drop the identity: a plain document in the same drawer.
        node.docs
            .client
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: capability(
                        &node,
                        DocScope {
                            write: std::collections::BTreeMap::from([(
                                doc.clone(),
                                big_cabinet_core::doc::DatumKinds::All,
                            )]),
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
        import_document(&node, operator, &present, &no_identity).await,
        Err(CartridgesError::Rejected(_))
    ));
    let scoped = mint(
        &node,
        CartridgesScope {
            publish: true,
            namespaces: Some(BTreeSet::from(["did:key:z6MkOther".to_owned()])),
        },
    )
    .await;
    let (outside, _) = author_document(
        &node,
        "author scoped",
        PHOTOS,
        &[fixture(PHOTOS, "1.0.0", &info)],
        |_, envelope| sign_as_example(envelope),
    )
    .await;
    assert!(matches!(
        import_document(&node, scoped, &present, &outside).await,
        Err(CartridgesError::NamespaceNotPermitted { .. })
    ));
    let reader = mint(
        &node,
        CartridgesScope {
            publish: false,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        import_document(&node, reader, &present, &outside).await,
        Err(CartridgesError::Denied)
    );
    shutdown(node, stop).await;
}

async fn listed_ids(node: &BigNode) -> Vec<big_cartridges_core::CartridgeId> {
    list(node)
        .await
        .into_iter()
        .map(|record| record.id)
        .collect()
}

#[tokio::test]
async fn an_installed_cartridge_is_ready_after_a_restart_with_its_settings_intact() {
    use big_cartridges_core::config::Component;
    use big_cartridges_core::protocol::ListCartridgeInstalls;
    use big_cartridges_core::protocol::SetCartridgeEnabled;
    use big_dispatch_core::Readiness;
    use big_nodule_core::protocol::GetNodule;

    // A nodule with no handler hosts nothing, so it is ready as soon as it is enabled and the
    // test needs no component bytes; its one declared settings datum is what is kept.
    const SETTINGS: &str = "org.example.settingsprobe.settings";
    let settings_file = serde_json::from_value(json!({
        "lexicon": 1,
        "id": SETTINGS,
        "defs": { "main": {
            "type": "object",
            "properties": { "thumbnails": { "type": "boolean" } }
        } }
    }))
    .unwrap();
    let mut cartridge = FixtureCartridge::new(Cartridge::new(
        example_cartridge!("settingsprobe").parse().unwrap(),
        "1.0.0".parse().unwrap(),
    ));
    cartridge.authority = Some("org.example.settingsprobe".into());
    cartridge.nodule = Some(Default::default());
    cartridge.with_lexicon(settings_file);
    cartridge.settings.insert(SETTINGS.parse().unwrap());
    let cartridge_id: CartridgeId = example_cartridge!("settingsprobe").parse().unwrap();

    let directory = tempfile::tempdir().unwrap();
    let secrets = secrets().await;
    let sql_path = directory.path().join("cabinet.sqlite");
    let boot = async || {
        let runtime = Arc::new(SqliteRuntime::new(Default::default()));
        let config = config(
            Arc::clone(&secrets),
            runtime.database(&sql_path, &[], "_sqlite_rs_migrations"),
            big_repo::Storage::in_directory(&runtime, &directory.path().join("repo"), None)
                .unwrap(),
        );
        BigNode::boot(config).await.unwrap()
    };
    let operator = |node: &BigNode| Authorization::identity(node.authority.root);
    let record = async |node: &BigNode| {
        node.nodules
            .client
            .call_with(
                RequestContext::root(),
                operator(node).metadata(),
                GetNodule {
                    cartridge_id: cartridge_id.clone(),
                },
            )
            .await
            .unwrap()
    };
    let config_doc = async |node: &BigNode| {
        node.nodules
            .client
            .call_with(
                RequestContext::root(),
                operator(node).metadata(),
                ListCartridgeInstalls {},
            )
            .await
            .unwrap()
            .installs
            .into_iter()
            .find(|row| row.cartridge_id == cartridge_id)
            .expect("the cartridge is installed")
            .install
            .config_doc
    };

    let (node, stop) = boot().await;
    super::nodule_wasm::admit(&node, cartridge).await;
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator(&node).metadata(),
            SetCartridgeEnabled {
                cartridge_id: cartridge_id.clone(),
                component: Component::Nodule,
                enabled: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(record(&node).await.readiness, Readiness::Ready);
    let doc = config_doc(&node).await;
    let settings_key = DatumKey::from_text(SETTINGS, "main").unwrap();
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: capability(
                    &node,
                    DocScope {
                        write: std::collections::BTreeMap::from([(
                            doc.clone(),
                            big_cabinet_core::doc::DatumKinds::All,
                        )]),
                        ..DocScope::default()
                    },
                )
                .await,
                command: None,
                doc_id: doc.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(
                    settings_key.clone(),
                    json!({ "$type": SETTINGS, "thumbnails": false }),
                )]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    shutdown(node, stop).await;

    // The same directory boots the same node: the install, its enablement and the settings are
    // in its documents, and the service reconciles them to Ready without anyone installing again.
    let (node, stop) = boot().await;
    assert_eq!(config_doc(&node).await, doc);
    let record = record(&node).await;
    assert_eq!(record.readiness, Readiness::Ready, "{record:?}");
    assert_eq!(record.error, None);
    let DatumRead::Accepted { value, .. } =
        &super::blobs::all_datums(&node, &doc).await.datums[&settings_key]
    else {
        panic!("the settings survived the restart");
    };
    assert_eq!(value["thumbnails"], json!(false));
    shutdown(node, stop).await;
}
