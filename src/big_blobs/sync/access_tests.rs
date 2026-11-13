use super::*;
use automerge::transaction::Transactable;
use bao_tree::ChunkRanges;
use bao_tree::io::outboard::PreOrderMemOutboard;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use big_repo::BigRepo;
use big_repo::BigRepoConnection;
use big_repo::SharedBigRepo;
use big_repo::keyhive_core::access::Access;
use big_repo::keyhive_core::principal::identifier::Identifier;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use iroh::protocol::Router;
use iroh_blobs::Hash;
use iroh_blobs::protocol::GetRequest;
use iroh_blobs::protocol::Request;
use service_client_native_rs::Client;
use service_client_native_rs::InlineRoute;
use service_client_native_rs::Transport;
use service_utils_rs::StopToken;
use sqlite_rs::SqliteRuntime;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tokio::sync::mpsc;

#[derive(Debug)]
struct Response {
    hash: Hash,
    bytes: Bytes,
    started: Arc<Notify>,
    finish: Arc<Notify>,
    requests: Arc<AtomicUsize>,
}

impl ProtocolHandler for Response {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let (request, _) = Request::read_async(&mut recv).await?;
        self.requests.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request, Request::Get(GetRequest::blob(self.hash)));
        let last = self.bytes.len() - 1;
        tokio::io::AsyncWriteExt::write_all(&mut send, &self.bytes[..last]).await?;
        self.started.notify_one();
        self.finish.notified().await;
        tokio::io::AsyncWriteExt::write_all(&mut send, &self.bytes[last..]).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

#[derive(Debug)]
struct AcceptRepo {
    repo: SharedBigRepo,
    endpoint: iroh::Endpoint,
    accepted: mpsc::UnboundedSender<BigRepoConnection>,
}

impl ProtocolHandler for AcceptRepo {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(connection, self.endpoint.clone(), None)
            .await
            .map_err(|error| AcceptError::from_boxed(error.into()))?;
        self.accepted.send(connection).unwrap();
        Ok(())
    }
}

struct Peer {
    repo: SharedBigRepo,
    router: Router,
    lookup: iroh::address_lookup::MemoryLookup,
    accepted: mpsc::UnboundedReceiver<BigRepoConnection>,
    rpc: big_repo::rpc::BigRepoRpcHandle,
    rpc_stop: big_repo::rpc::BigRepoRpcStopToken,
    repo_stop: big_repo::BigRepoStopToken,
}

impl Peer {
    async fn boot(seed: u8, response: Option<Response>) -> Res<Self> {
        let runtime = Arc::new(SqliteRuntime::new(Default::default()));
        let (repo, repo_stop) = BigRepo::boot(big_repo::Config {
            node_identity_seed: [seed; 32],
            storage: big_repo::Storage::memory(&runtime).unwrap(),
            scope_key: "inventory-access-transfer".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await?;
        let lookup = iroh::address_lookup::MemoryLookup::default();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .address_lookup(lookup.clone())
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let (accepted, accepted_rx) = mpsc::unbounded_channel();
        let (rpc, rpc_stop) = big_repo::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
        let router = Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                AcceptRepo {
                    repo: Arc::clone(&repo),
                    endpoint,
                    accepted,
                },
            )
            .accept(big_repo::rpc::REPO_SYNC_ALPN, rpc.protocol_handler());
        let router = match response {
            Some(response) => router.accept(iroh_blobs::ALPN, response),
            None => router,
        }
        .spawn();
        Ok(Self {
            repo,
            router,
            lookup,
            accepted: accepted_rx,
            rpc,
            rpc_stop,
            repo_stop,
        })
    }

    async fn shutdown(self) -> Res<()> {
        let router = self.router.shutdown().await;
        let rpc = self.rpc_stop.stop().await;
        let repo = self.repo_stop.stop().await;
        router?;
        rpc?;
        repo
    }
}

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("internal blob admission does not use client authority") })
    }
}

struct Fixture {
    owner: Peer,
    receiver: Peer,
    receiver_connection: BigRepoConnection,
    owner_connection: BigRepoConnection,
    mirrors: InventoryMirrors,
    backend: BlobSyncBackend,
    stop: StopToken,
    started: Arc<Notify>,
    finish: Arc<Notify>,
    requests: Arc<AtomicUsize>,
}

impl Fixture {
    async fn boot(bytes: &[u8]) -> Res<Self> {
        let outboard = PreOrderMemOutboard::create(bytes, iroh_blobs::store::IROH_BLOCK_SIZE);
        let mut encoded = (bytes.len() as u64).to_le_bytes().to_vec();
        bao_tree::io::sync::encode_ranges_validated(
            bytes,
            &outboard,
            &ChunkRanges::all(),
            &mut encoded,
        )?;
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let requests = Arc::new(AtomicUsize::new(0));
        let mut owner = Peer::boot(
            111,
            Some(Response {
                hash: Hash::from(outboard.root),
                bytes: Bytes::from(encoded),
                started: Arc::clone(&started),
                finish: Arc::clone(&finish),
                requests: Arc::clone(&requests),
            }),
        )
        .await?;
        let receiver = Peer::boot(112, None).await?;
        owner.rpc.register_peer(
            receiver.router.endpoint().id(),
            receiver.repo.local_peer_id(),
        );
        receiver
            .rpc
            .register_peer(owner.router.endpoint().id(), owner.repo.local_peer_id());
        let receiver_connection = receiver
            .repo
            .open_connection_iroh(
                receiver.router.endpoint().clone(),
                owner.router.endpoint().addr(),
                owner.repo.local_peer_id(),
                None,
            )
            .await?;
        let owner_connection = owner.accepted.recv().await.unwrap();
        let sql = SqliteRuntime::new(Default::default()).ephemeral(&[])?;
        let parts: big_repo::SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
        let (blobs, blob_stop) = crate::boot(crate::Config {
            storage: crate::Storage::memory().unwrap(),
            sql: sql.clone(),
            authority: Client::new(Transport::direct(Arc::new(UnusedAuthority))),
            operator: Uuid7::new(),
            namespace: Namespace {
                namespace_id: Uuid7::new(),
                capability: CapId::from_bytes([113; 16]),
            },
        })
        .await?;
        crate::retention::Retention::migrate(&sql).await?;
        let queue = crate::work_queue::WorkQueue::new(sql.clone()).await?;
        let (mirrors, mirror_stop) =
            InventoryMirrors::boot(sql, blobs.service, parts, Arc::clone(&receiver.repo), queue)
                .await?;
        let backend = BlobSyncBackend::new(
            mirrors.clone(),
            receiver.router.endpoint().clone(),
            receiver.lookup.clone(),
        );
        backend.register_peer_addr(owner.repo.local_peer_id(), owner.router.endpoint().addr());
        Ok(Self {
            owner,
            receiver,
            receiver_connection,
            owner_connection,
            mirrors,
            backend,
            stop: mirror_stop.then(blob_stop),
            started,
            finish,
            requests,
        })
    }

    async fn reconcile_keyhive(&self) -> Res<()> {
        self.owner_connection.sync_keyhive_with_peer().await?;
        self.receiver_connection.sync_keyhive_with_peer().await?;
        self.owner.repo.wait_for_keyhive_reconciliation().await?;
        self.receiver.repo.wait_for_keyhive_reconciliation().await
    }

    async fn receiver_access(&self, inventory: &DocId) -> Res<Option<Access>> {
        let receiver = ed25519_dalek::VerifyingKey::from_bytes(
            &self.receiver.repo.local_peer_id().to_bytes32()?,
        )?;
        let inventory = ed25519_dalek::VerifyingKey::from_bytes(&inventory.to_bytes32()?)?;
        Ok(self
            .receiver
            .repo
            .keyhive()
            .agent_access_on(&Identifier::from(receiver), Identifier::from(inventory))
            .await)
    }

    async fn observe(&self, inventory: &DocId, digest: BlobId, _revision: u64) -> Res<()> {
        self.backend
            .sync_obj(
                self.owner.repo.local_peer_id(),
                digest.into(),
                vec![crate::workers::inventory_part(inventory)],
                None,
            )
            .await?;
        Ok(())
    }

    async fn assert_revoked(&self, inventory: &DocId, digest: BlobId) -> Res<()> {
        let state = self
            .mirrors
            .admission
            .state(inventory, digest)
            .await?
            .unwrap();
        assert!(state.selected && state.pinned && state.observed);
        assert!(!state.writer);
        assert!(!state.readable);
        assert!(!state.fetching);
        assert!(!state.fetch_eligible());
        assert!(!state.advertisable());
        assert!(
            self.mirrors
                .projection
                .parts
                .obj_parts(digest.into())
                .await?
                .is_empty(),
            "revoked inventory must not advertise late or newly observed bytes"
        );
        assert!(
            self.mirrors
                .projection
                .parts
                .obj_payload(digest.into())
                .await?
                .is_none(),
            "revoked inventory must not publish an object payload"
        );
        Ok(())
    }

    async fn shutdown(self) -> Res<()> {
        self.stop.stop().await;
        let owner_connection = self.owner_connection.stop().await;
        let receiver_connection = self.receiver_connection.stop().await;
        let receiver = self.receiver.shutdown().await;
        let owner = self.owner.shutdown().await;
        owner_connection?;
        receiver_connection?;
        receiver?;
        owner
    }
}

#[tokio::test]
async fn in_flight_keyhive_revocation_rejects_late_completion_and_new_admission() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let bytes = vec![21; 200_000];
    let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
    let fixture = Fixture::boot(&bytes).await?;
    let result = std::panic::AssertUnwindSafe(async {
        assert_ne!(
            fixture.owner.repo.local_peer_id(),
            fixture.receiver.repo.local_peer_id()
        );
        assert_ne!(
            fixture.owner.router.endpoint().id(),
            fixture.receiver.router.endpoint().id()
        );
        let mut document = automerge::Automerge::new();
        let mut tx = document.transaction();
        tx.put(automerge::ROOT, "fixture", "owner inventory")?;
        tx.commit();
        let owner_doc = fixture.owner.repo.create_doc(document).await?;
        let inventory = owner_doc.document_id();
        assert_eq!(fixture.receiver_access(&inventory).await?, None);
        let receiver = fixture
            .owner
            .repo
            .receive_keyhive_contact_card(&fixture.receiver.repo.local_keyhive_contact_card())
            .await?;
        fixture
            .owner
            .repo
            .grant_doc_access(inventory.clone(), receiver.clone(), Access::Read)
            .await?;
        fixture.reconcile_keyhive().await?;
        assert_eq!(
            fixture.receiver_access(&inventory).await?,
            Some(Access::Read)
        );
        let interest = fixture.receiver.repo.interest(inventory.clone()).await?;
        fixture
            .receiver_connection
            .sync_doc_with_peer_receipt(inventory.clone())
            .await?;
        let receiver_doc = interest.ready().await?;
        assert_eq!(receiver_doc.document_id(), inventory);
        drop(receiver_doc);
        drop(interest);
        fixture
            .mirrors
            .select_inventory(inventory.clone(), "revocation-root".into())
            .await?;
        fixture
            .mirrors
            .reconcile(
                &inventory,
                1,
                &BTreeMap::from([(digest, bytes.len() as u64)]),
                true,
            )
            .await?;
        let mut completed = fixture.mirrors.completions.subscribe();
        assert_eq!(*completed.borrow_and_update(), 0);
        fixture.observe(&inventory, digest, 10).await?;
        fixture.started.notified().await;
        let before = fixture
            .mirrors
            .admission
            .state(&inventory, digest)
            .await?
            .unwrap();
        assert!(before.selected && before.pinned && before.observed && before.readable);
        assert!(before.fetch_eligible() && before.fetching);
        assert!(!before.writer && !before.bytes && !before.advertisable());
        assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .mirrors
                .projection
                .parts
                .obj_parts(digest.into())
                .await?
                .is_empty()
        );
        assert!(
            fixture
                .mirrors
                .projection
                .service
                .stat(&digest)
                .await?
                .is_none()
        );
        fixture
            .owner
            .repo
            .revoke_doc_access(inventory.clone(), receiver)
            .await?;
        fixture.reconcile_keyhive().await?;
        assert_eq!(fixture.receiver_access(&inventory).await?, None);
        assert!(!fixture.mirrors.projection.readable(&inventory).await?);
        assert!(
            fixture
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .fetching
        );
        assert!(!completed.has_changed()?);
        assert_eq!(*completed.borrow_and_update(), 0);
        fixture.finish.notify_one();
        completed.changed().await?;
        assert_eq!(*completed.borrow_and_update(), 1);
        fixture.assert_revoked(&inventory, digest).await?;
        let state = fixture
            .mirrors
            .admission
            .state(&inventory, digest)
            .await?
            .unwrap();
        assert!(state.authorization > before.authorization);
        assert!(state.bytes);
        assert_eq!(
            fixture
                .mirrors
                .projection
                .service
                .store()
                .blobs()
                .get_bytes(Hash::from(digest.to_bytes32()))
                .await?
                .as_ref(),
            bytes
        );
        assert_eq!(
            fixture
                .mirrors
                .projection
                .service
                .ownership
                .contains(&digest)
                .await?,
            state.retains_bytes()
        );
        let candidate = b"post-revocation candidate";
        let next = BlobId::new(*Hash::new(candidate).as_bytes());
        let mut progress = fixture.mirrors.progress.subscribe();
        fixture
            .mirrors
            .reconcile(
                &inventory,
                2,
                &BTreeMap::from([(digest, bytes.len() as u64), (next, candidate.len() as u64)]),
                true,
            )
            .await?;
        fixture.observe(&inventory, next, 20).await?;
        loop {
            progress.borrow_and_update();
            if fixture.mirrors.admission.dirty().await?.is_empty() {
                break;
            }
            progress.changed().await?;
        }
        fixture.assert_revoked(&inventory, digest).await?;
        fixture.assert_revoked(&inventory, next).await?;
        assert!(
            !fixture
                .mirrors
                .admission
                .state(&inventory, next)
                .await?
                .unwrap()
                .bytes
        );
        assert!(
            !fixture
                .mirrors
                .projection
                .service
                .ownership
                .contains(&next)
                .await?
        );
        assert!(
            fixture
                .mirrors
                .projection
                .service
                .stat(&next)
                .await?
                .is_none()
        );
        assert!(!completed.has_changed()?);
        assert_eq!(*completed.borrow(), 1);
        assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
        drop(owner_doc);
        eyre::Ok(())
    })
    .catch_unwind()
    .await;
    let shutdown = fixture.shutdown().await;
    match result {
        Ok(result) => {
            result?;
            shutdown
        }
        Err(panic) => {
            shutdown.unwrap();
            std::panic::resume_unwind(panic)
        }
    }
}
