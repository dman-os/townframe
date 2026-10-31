use super::*;
use crate::encrypt::EncodingParams;
use crate::encrypt::MasterKey;
use crate::encrypt::add_encrypted;
use crate::encrypt::drop_pair_tags;
use crate::encrypt::encrypt_bytes;
use crate::encrypt::ensure_stored;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use big_sync::HostPartStore;
use big_sync::MemoryPartStore;
use big_sync::backend::contract;
use big_sync::backend::contract::SyncBackendHarness;
use big_sync::backend::contract::SyncBackendOutcome;
use big_sync::backend::contract::SyncBackendScenario;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use service_utils_rs::Client;
use service_utils_rs::InlineRoute;
use service_utils_rs::StopToken;
use sqlite_rs::SqliteRuntime;

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _message: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("internal blob sync must not call the client authority route") })
    }
}

struct Harness {
    backend: BlobSyncBackend,
    stop: StopToken,
}

impl Harness {
    async fn new() -> Res<Self> {
        let runtime = SqliteRuntime::new(Default::default());
        let (blobs, stop) = crate::boot(crate::Config {
            storage: crate::Storage::Memory,
            sql: runtime.ephemeral(&[])?,
            authority: Client::direct(Arc::new(UnusedAuthority)),
            namespace: Namespace {
                namespace_id: Uuid7::new(),
                capability: CapId::from_bytes([1; 16]),
            },
        })
        .await?;
        let lookup = iroh::address_lookup::MemoryLookup::default();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(lookup.clone())
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        Ok(Self {
            backend: BlobSyncBackend::new(
                blobs.service,
                Arc::new(MemoryPartStore::new()),
                endpoint,
                lookup,
            ),
            stop,
        })
    }

    async fn stored(&self, bytes: &[u8]) -> Res<(BlobId, TempTag)> {
        let ciphertext = encrypt_bytes(&MasterKey::random(), bytes);
        let (guard, hash) = ensure_stored(
            self.backend.service.store(),
            futures::stream::iter([Ok(Bytes::from(ciphertext))]),
        )
        .await?;
        Ok((BlobId::new(*hash.as_bytes()), guard))
    }

    async fn assert_announced(&self, expected: &[BlobId]) -> Res<()> {
        let mut reader = self.backend.service.open_availability(0).await?;
        let mut actual = Vec::new();
        while let RevisionRead::Entries { entries, .. } =
            reader.next(RevisionReadLimits::default()).await?
        {
            actual.extend(entries.into_iter().map(|entry| entry.key));
        }
        actual.sort();
        let mut expected = expected.to_vec();
        expected.sort();
        assert_eq!(actual, expected);
        Ok(())
    }

    async fn shutdown(self) {
        self.backend.endpoint.close().await;
        self.stop.stop().await;
    }
}

#[async_trait]
impl SyncBackendHarness for Harness {
    fn backend(&self) -> &dyn SyncBackend {
        &self.backend
    }

    fn store(&self) -> &dyn HostPartStore {
        self.backend.part_store.as_ref()
    }

    async fn assert_case(&self, case: &SyncBackendScenario) -> Res<()> {
        assert_eq!(
            self.store().obj_payload(case.obj_id.clone()).await?,
            Some(
                case.remote_payload
                    .clone()
                    .or_else(|| case.initial_payload.clone())
                    .unwrap_or_else(|| serde_json::json!({}))
            )
        );
        let id = BlobId::try_from(&case.obj_id)?;
        let hash = Hash::from(id.to_bytes32());
        assert_eq!(
            self.backend
                .service
                .store()
                .tags()
                .get(format!("held:{hash}"))
                .await?
                .unwrap()
                .hash,
            hash
        );
        Ok(())
    }
}

#[tokio::test]
async fn blob_sync_backend_contract() -> Res<()> {
    let harness = Harness::new().await?;
    let payload = serde_json::json!({"lengthOctets": 65557});
    let old = serde_json::json!({"lengthOctets": 1});
    let part = PartKey::new([2; 32]);
    let mut cases = Vec::new();
    let mut guards = Vec::new();
    let mut ids = Vec::new();
    for (name, initial_payload, remote_payload, parts, deets) in [
        (
            "no_payloads",
            None,
            None,
            vec![part.clone()],
            SyncCompletionDeets::Noop,
        ),
        (
            "matching",
            Some(payload.clone()),
            Some(payload.clone()),
            vec![part.clone()],
            SyncCompletionDeets::Noop,
        ),
        (
            "missing_remote",
            Some(payload.clone()),
            None,
            vec![part.clone()],
            SyncCompletionDeets::Noop,
        ),
        (
            "changed",
            Some(old.clone()),
            Some(payload.clone()),
            vec![part.clone()],
            SyncCompletionDeets::ChangedObject,
        ),
        (
            "empty_hints",
            Some(old.clone()),
            Some(payload.clone()),
            vec![],
            SyncCompletionDeets::ChangedObject,
        ),
        (
            "multiple_hints",
            Some(old),
            Some(payload.clone()),
            vec![part.clone(), PartKey::new([3; 32])],
            SyncCompletionDeets::ChangedObject,
        ),
        (
            "added",
            None,
            Some(payload),
            vec![part],
            SyncCompletionDeets::AddedMember,
        ),
    ] {
        let (id, guard) = harness.stored(name.as_bytes()).await?;
        guards.push(guard);
        ids.push(id);
        cases.push(SyncBackendScenario {
            name,
            peer_id: PeerKey::new([4; 32]),
            obj_id: id.into(),
            initial_payload,
            initial_parts: parts.clone(),
            remote_payload,
            expected_outcome: SyncBackendOutcome::Completion(deets),
            expected_parts: parts,
        });
    }
    contract::assert_sync_backend_scenarios(&harness, &cases).await?;
    drop(guards);
    harness.assert_announced(&ids).await?;
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ensure_local_blob_durably_roots_stored_bytes_and_announces_once() -> Res<()> {
    let harness = Harness::new().await?;
    let (id, guard) = harness.stored(b"stored cipher representation").await?;
    let hash = Hash::from(id.to_bytes32());
    let tags = harness.backend.service.store().tags();
    assert!(tags.get(format!("held:{hash}")).await?.is_none());
    harness.assert_announced(&[]).await?;
    harness
        .backend
        .ensure_local_blob(PeerKey::new([4; 32]), id)
        .await?;
    let retained = tags.get(format!("held:{hash}")).await?.unwrap();
    assert_eq!(retained.hash, hash);
    assert_eq!(retained.format, BlobFormat::Raw);
    drop(guard);
    harness
        .backend
        .ensure_local_blob(PeerKey::new([7; 32]), id)
        .await?;
    assert!(harness.store().obj_payload(id.into()).await?.is_none());
    assert!(harness.store().obj_parts(id.into()).await?.is_empty());
    harness.assert_announced(&[id]).await?;
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn matching_payload_still_adopts_parts_and_announces() -> Res<()> {
    let harness = Harness::new().await?;
    let (id, guard) = harness.stored(b"matching payload, new membership").await?;
    let obj = ObjKey::from(id);
    let payload = serde_json::json!({"lengthOctets": 65557});
    harness
        .store()
        .set_obj_payload(obj.clone(), payload.clone())
        .await?;
    let part = PartKey::new([5; 32]);
    let outcome = harness
        .backend
        .sync_obj(
            PeerKey::new([4; 32]),
            obj.clone(),
            vec![part.clone()],
            Some(payload.clone()),
        )
        .await?;
    contract::assert_sync_backend_completion_deets(&outcome, SyncCompletionDeets::Noop);
    assert_eq!(
        harness.store().obj_parts(obj.clone()).await?,
        vec![part.clone()]
    );
    harness.assert_announced(&[id]).await?;
    harness
        .backend
        .remove_obj_from_parts(obj.clone(), vec![part.clone()])
        .await?;
    harness
        .backend
        .remove_obj_from_parts(obj.clone(), vec![part])
        .await?;
    assert!(harness.store().obj_parts(obj.clone()).await?.is_empty());
    assert_eq!(harness.store().obj_payload(obj).await?, Some(payload));
    drop(guard);
    assert!(harness.backend.service.stat(&id).await?.is_some());
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn membership_removal_preserves_upload_and_document_pin_roots() -> Res<()> {
    let harness = Harness::new().await?;
    let ciphertext = encrypt_bytes(&MasterKey::random(), b"independently retained upload");
    let info = harness
        .backend
        .service
        .import_stream(futures::stream::iter([Ok(Bytes::from(ciphertext))]))
        .await?;
    let id = info.blob_id;
    let hash = Hash::from(id.to_bytes32());
    let tags = harness.backend.service.store().tags();
    tags.set(format!("pin:{hash}"), HashAndFormat::raw(hash))
        .await?;
    let first = PartKey::new([5; 32]);
    let second = PartKey::new([6; 32]);
    harness
        .backend
        .sync_obj(
            PeerKey::new([4; 32]),
            id.into(),
            vec![first.clone(), second.clone()],
            Some(serde_json::json!({"lengthOctets": info.length_octets})),
        )
        .await?;
    harness
        .backend
        .remove_obj_from_parts(id.into(), vec![first])
        .await?;
    assert_eq!(
        harness.store().obj_parts(id.into()).await?,
        vec![second.clone()]
    );
    harness
        .backend
        .remove_obj_from_parts(id.into(), vec![second.clone(), second])
        .await?;
    assert!(harness.store().obj_parts(id.into()).await?.is_empty());
    for name in [format!("held:{hash}"), format!("pin:{hash}")] {
        let retained = tags.get(name).await?.unwrap();
        assert_eq!(retained.hash, hash);
        assert_eq!(retained.format, BlobFormat::Raw);
    }
    harness.assert_announced(&[id]).await?;
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rooted_virtual_representation_is_terminal_without_export() -> Res<()> {
    let harness = Harness::new().await?;
    let service = &harness.backend.service;
    let (ciphertext, plaintext) = add_encrypted(
        service.store(),
        &service.provider,
        &service.roots,
        &MasterKey::random(),
        EncodingParams::default(),
        b"virtual representation",
    )
    .await?;
    let id = BlobId::new(*ciphertext.as_bytes());
    harness
        .backend
        .ensure_local_blob(PeerKey::new([4; 32]), id)
        .await?;
    assert!(
        service
            .store()
            .tags()
            .get(format!("held:{ciphertext}"))
            .await?
            .is_none()
    );
    let part = PartKey::new([5; 32]);
    let outcome = harness
        .backend
        .sync_obj(
            PeerKey::new([4; 32]),
            id.into(),
            vec![part.clone()],
            Some(serde_json::json!({"lengthOctets": 65557})),
        )
        .await?;
    contract::assert_sync_backend_completion_deets(&outcome, SyncCompletionDeets::AddedMember);
    assert_eq!(
        harness.store().obj_parts(id.into()).await?,
        vec![part.clone()]
    );
    harness
        .backend
        .remove_obj_from_parts(id.into(), vec![part])
        .await?;
    assert!(harness.store().obj_parts(id.into()).await?.is_empty());
    for (prefix, expected) in [(TAG_CT_PREFIX, ciphertext), (TAG_PT_PREFIX, plaintext)] {
        let retained = service
            .store()
            .tags()
            .get(format!("{prefix}{ciphertext}"))
            .await?
            .unwrap();
        assert_eq!(retained.hash, expected);
        assert_eq!(retained.format, BlobFormat::Raw);
    }
    assert!(
        service
            .store()
            .blobs()
            .sync_reader(ciphertext)
            .await?
            .is_none()
    );
    harness.assert_announced(&[id]).await?;
    let guards = harness
        .backend
        .retain_and_announce(PeerKey::new([4; 32]), id)
        .await?;
    drop_pair_tags(service.store(), ciphertext).await?;
    let retained = service
        .store()
        .tags()
        .list_temp_tags()
        .await?
        .collect::<Vec<_>>()
        .await;
    assert!(retained.contains(&HashAndFormat::raw(ciphertext)));
    assert!(retained.contains(&HashAndFormat::raw(plaintext)));
    drop(guards);
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn incomplete_virtual_pair_is_not_announced_or_adopted() -> Res<()> {
    for defect in [
        "missing_plaintext_root",
        "missing_ciphertext_root",
        "wrong_ciphertext_root",
        "wrong_plaintext_format",
        "missing_plaintext_bytes",
        "missing_provider",
    ] {
        let harness = Harness::new().await?;
        let service = &harness.backend.service;
        let (ciphertext, plaintext) = add_encrypted(
            service.store(),
            &service.provider,
            &service.roots,
            &MasterKey::random(),
            EncodingParams::default(),
            defect.as_bytes(),
        )
        .await?;
        let tags = service.store().tags();
        match defect {
            "missing_plaintext_root" => {
                tags.delete(format!("{TAG_PT_PREFIX}{ciphertext}")).await?;
            }
            "missing_ciphertext_root" => {
                tags.delete(format!("{TAG_CT_PREFIX}{ciphertext}")).await?;
            }
            "wrong_ciphertext_root" => {
                tags.set(
                    format!("{TAG_CT_PREFIX}{ciphertext}"),
                    HashAndFormat::raw(plaintext),
                )
                .await?;
            }
            "wrong_plaintext_format" => {
                tags.set(
                    format!("{TAG_PT_PREFIX}{ciphertext}"),
                    HashAndFormat {
                        hash: plaintext,
                        format: BlobFormat::HashSeq,
                    },
                )
                .await?;
            }
            "missing_plaintext_bytes" => {
                tags.set(
                    format!("{TAG_PT_PREFIX}{ciphertext}"),
                    HashAndFormat::raw(Hash::new(b"absent plaintext")),
                )
                .await?;
            }
            "missing_provider" => service.provider.remove(&ciphertext),
            _ => unreachable!(),
        }
        let id = BlobId::new(*ciphertext.as_bytes());
        assert!(
            harness
                .backend
                .sync_obj(
                    PeerKey::new([4; 32]),
                    id.into(),
                    vec![PartKey::new([5; 32])],
                    Some(serde_json::json!({})),
                )
                .await
                .is_err(),
            "accepted virtual representation with {defect}"
        );
        assert!(harness.store().obj_payload(id.into()).await?.is_none());
        assert!(harness.store().obj_parts(id.into()).await?.is_empty());
        assert!(tags.get(format!("held:{ciphertext}")).await?.is_none());
        harness.assert_announced(&[]).await?;
        harness.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn download_verifies_ciphertext_before_announcing_and_adopting() -> Res<()> {
    use bao_tree::ChunkRanges;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use iroh::protocol::AcceptError;
    use iroh::protocol::ProtocolHandler;
    use iroh::protocol::Router;
    use iroh_blobs::api::Store;
    use iroh_blobs::protocol::GetRequest;
    use iroh_blobs::protocol::Request;
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct CiphertextResponse {
        hash: Hash,
        bytes: Bytes,
        prefix_sent: Arc<Notify>,
        finish_response: Arc<Notify>,
    }

    impl ProtocolHandler for CiphertextResponse {
        async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let (request, _) = Request::read_async(&mut recv).await?;
            assert_eq!(request, Request::Get(GetRequest::blob(self.hash)));
            let last = self.bytes.len() - 1;
            tokio::io::AsyncWriteExt::write_all(&mut send, &self.bytes[..last]).await?;
            self.prefix_sent.notify_one();
            self.finish_response.notified().await;
            tokio::io::AsyncWriteExt::write_all(&mut send, &self.bytes[last..]).await?;
            send.finish()?;
            connection.closed().await;
            Ok(())
        }
    }

    for corrupt in [false, true] {
        let harness = Harness::new().await?;
        let plaintext = vec![7; 200_000];
        let ciphertext = encrypt_bytes(&MasterKey::random(), &plaintext);
        let outboard = PreOrderMemOutboard::create(&ciphertext, iroh_blobs::store::IROH_BLOCK_SIZE);
        let hash = Hash::from(outboard.root);
        let id = BlobId::new(*hash.as_bytes());
        let mut response = (ciphertext.len() as u64).to_le_bytes().to_vec();
        bao_tree::io::sync::encode_ranges_validated(
            ciphertext.as_slice(),
            &outboard,
            &ChunkRanges::all(),
            &mut response,
        )?;
        if corrupt {
            *response.last_mut().unwrap() ^= 1;
        }
        let prefix_sent = Arc::new(Notify::new());
        let finish_response = Arc::new(Notify::new());
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let router = Router::builder(endpoint)
            .accept(
                iroh_blobs::ALPN,
                CiphertextResponse {
                    hash,
                    bytes: Bytes::from(response),
                    prefix_sent: Arc::clone(&prefix_sent),
                    finish_response: Arc::clone(&finish_response),
                },
            )
            .spawn();
        let peer = PeerKey::new([4; 32]);
        harness
            .backend
            .register_peer_addr(peer.clone(), router.endpoint().addr());
        let part = PartKey::new([5; 32]);
        let payload = serde_json::json!({"lengthOctets": ciphertext.len()});
        harness
            .store()
            .set_obj_payload(id.into(), payload.clone())
            .await?;
        let transfer = tokio::spawn({
            let backend = harness.backend.clone();
            let peer = peer.clone();
            let part = part.clone();
            let payload = payload.clone();
            async move {
                backend
                    .sync_obj(peer, id.into(), vec![part], Some(payload))
                    .await
            }
        });
        prefix_sent.notified().await;
        assert!(!transfer.is_finished());
        assert!(harness.backend.service.stat(&id).await?.is_none());
        assert!(harness.store().obj_parts(id.into()).await?.is_empty());
        assert_eq!(
            harness.store().obj_payload(id.into()).await?,
            Some(payload.clone())
        );
        assert!(
            harness
                .backend
                .service
                .store()
                .tags()
                .get(format!("held:{hash}"))
                .await?
                .is_none()
        );
        harness.assert_announced(&[]).await?;
        finish_response.notify_one();
        let result = transfer.await?;
        router.shutdown().await?;
        if corrupt {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("failed downloading"),
                "{error:?}"
            );
            assert!(harness.backend.service.stat(&id).await?.is_none());
            assert!(harness.store().obj_parts(id.into()).await?.is_empty());
            assert_eq!(
                harness.store().obj_payload(id.into()).await?,
                Some(payload.clone())
            );
            assert!(
                harness
                    .backend
                    .service
                    .store()
                    .tags()
                    .get(format!("held:{hash}"))
                    .await?
                    .is_none()
            );
            harness.assert_announced(&[]).await?;

            let store = Store::from(iroh_blobs::store::mem::MemStore::new());
            let guard = store
                .blobs()
                .add_bytes(ciphertext.clone())
                .temp_tag()
                .await?;
            assert_eq!(guard.hash(), hash);
            let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .clear_ip_transports()
                .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
                .relay_mode(iroh::RelayMode::Disabled)
                .bind()
                .await?;
            let router = Router::builder(endpoint)
                .accept(
                    iroh_blobs::ALPN,
                    iroh_blobs::BlobsProtocol::new(&store, None),
                )
                .spawn();
            harness
                .backend
                .register_peer_addr(peer.clone(), router.endpoint().addr());
            let outcome = harness
                .backend
                .sync_obj(peer, id.into(), vec![part.clone()], Some(payload))
                .await?;
            contract::assert_sync_backend_completion_deets(&outcome, SyncCompletionDeets::Noop);
            // Release the temp tag while the store actor is still alive, then let
            // `router.shutdown()` be the single teardown: it runs `BlobsProtocol`'s
            // `ProtocolHandler::shutdown`, which already shuts down the backing store.
            // A second `store.shutdown()` here would send to the exited actor and
            // fail with a closed receiver.
            drop(guard);
            router.shutdown().await?;
        } else {
            contract::assert_sync_backend_completion_deets(&result?, SyncCompletionDeets::Noop);
        }
        let store = harness.backend.service.store();
        assert_eq!(store.blobs().get_bytes(hash).await?.as_ref(), ciphertext);
        assert!(
            store
                .blobs()
                .sync_reader(Hash::new(&plaintext))
                .await?
                .is_none()
        );
        let retained = store.tags().get(format!("held:{hash}")).await?.unwrap();
        assert_eq!(retained.hash, hash);
        assert_eq!(retained.format, BlobFormat::Raw);
        assert_eq!(harness.store().obj_parts(id.into()).await?, vec![part]);
        harness.assert_announced(&[id]).await?;
        harness.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn missing_representation_cannot_noop_on_matching_payload() -> Res<()> {
    let harness = Harness::new().await?;
    let id = BlobId::new(*Hash::new(b"absent representation").as_bytes());
    let payload = serde_json::json!({"lengthOctets": 123});
    harness
        .store()
        .set_obj_payload(id.into(), payload.clone())
        .await?;
    let error = harness
        .backend
        .sync_obj(
            PeerKey::new([4; 32]),
            id.into(),
            vec![PartKey::new([5; 32])],
            Some(payload.clone()),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no longer registered"));
    assert_eq!(harness.store().obj_payload(id.into()).await?, Some(payload));
    assert!(harness.store().obj_parts(id.into()).await?.is_empty());
    harness.assert_announced(&[]).await?;
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn peer_registration_is_shared_and_revocable() -> Res<()> {
    let harness = Harness::new().await?;
    let peer = PeerKey::new([4; 32]);
    let backend = harness.backend.clone();
    backend.register_peer_addr(peer.clone(), iroh::EndpointAddr::new(backend.endpoint.id()));
    assert_eq!(harness.backend.active_peer_ids(), vec![peer.clone()]);
    harness.backend.unregister_peer_addr(peer.clone());
    harness.backend.unregister_peer_addr(peer.clone());
    assert!(backend.active_peer_ids().is_empty());
    let error = backend
        .ensure_local_blob(peer, BlobId::new([6; 32]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no longer registered"));
    harness.assert_announced(&[]).await?;
    harness.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rejects_non_digest_object_keys_without_side_effects() -> Res<()> {
    let harness = Harness::new().await?;
    let obj = ObjKey::new(b"/outside/blob/root");
    let error = harness
        .backend
        .sync_obj(
            PeerKey::new([4; 32]),
            obj.clone(),
            vec![PartKey::new([5; 32])],
            Some(serde_json::json!({})),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains(&obj.to_string()));
    assert!(
        harness
            .backend
            .remove_obj_from_parts(obj.clone(), vec![PartKey::new([5; 32])])
            .await
            .is_err()
    );
    assert!(harness.store().obj_payload(obj.clone()).await?.is_none());
    assert!(harness.store().obj_parts(obj).await?.is_empty());
    harness.assert_announced(&[]).await?;
    harness.shutdown().await;
    Ok(())
}
