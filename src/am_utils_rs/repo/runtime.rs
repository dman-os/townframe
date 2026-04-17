use crate::interlude::*;
use crate::partition::PartitionStore;
use crate::repo::{BigRepoChangeOrigin, DocumentId, PeerId};

use futures::future::BoxFuture;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDocOutcome {
    Success,
    NotFoundOrUnauthorized,
    TransportError,
    IoError,
}

enum RuntimeMsg {
    IngestFull {
        doc_id: DocumentId,
        doc_save: Vec<u8>,
        done: oneshot::Sender<Res<()>>,
    },
    CreateDoc {
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
    },
    ImportDoc {
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
    },
    FindDocHandle {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Arc<crate::repo::LiveDocBundle>>>>,
    },
    LocalContainsDocument {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<bool>>,
    },
    ExportDocSave {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Vec<u8>>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    },
    EnsurePeerConnection {
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        done: oneshot::Sender<Res<()>>,
    },
    AcceptIncomingConnection {
        quic_conn: iroh::endpoint::Connection,
        done: oneshot::Sender<Res<()>>,
    },
    RemovePeerConnection {
        peer_id: PeerId,
        done: oneshot::Sender<Res<()>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
    },
    RemoteHeadsObserved {
        doc_id: DocumentId,
        peer_id: PeerId,
    },
    LiveDocRefreshLoaded {
        doc_id: DocumentId,
        peer_id: PeerId,
        snapshot: Res<Option<automerge::Automerge>>,
    },
    ConnectionEstablished {
        peer_id: PeerId,
        stop_token: RuntimePeerConnectionStopToken,
    },
    ConnectionLost {
        peer_id: PeerId,
    },
}

#[derive(Clone, Debug)]
pub(super) struct BigRepoRuntimeHandle {
    msg_tx: mpsc::UnboundedSender<RuntimeMsg>,
}

pub(super) struct BigRepoRuntimeStopToken {
    cancel_token: CancellationToken,
    done_rx: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
}

#[derive(Clone, Debug)]
pub(super) struct RuntimeDocLease {
    runtime: BigRepoRuntimeHandle,
    doc_id: DocumentId,
}

impl Drop for RuntimeDocLease {
    fn drop(&mut self) {
        self.runtime.release_doc_lease(self.doc_id);
    }
}

#[derive(Clone)]
pub(super) struct RuntimePeerConnectionStopToken {
    cancel_token: CancellationToken,
}

impl RuntimePeerConnectionStopToken {
    fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
        }
    }

    fn cancel(&self) {
        self.cancel_token.cancel();
    }

    fn child_token(&self) -> CancellationToken {
        self.cancel_token.child_token()
    }

    fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}

type RuntimeIrohTransport = subduction_core::transport::message::MessageTransport<
    subduction_iroh::transport::IrohTransport,
>;
type RuntimeSyncHandler<S> = subduction_core::handler::sync::SyncHandler<
    future_form::Sendable,
    S,
    RuntimeIrohTransport,
    subduction_core::policy::open::OpenPolicy,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
    RuntimeRemoteHeadsBridge,
>;
type RuntimeSubduction<S> = subduction_core::subduction::Subduction<
    'static,
    future_form::Sendable,
    S,
    RuntimeIrohTransport,
    RuntimeSyncHandler<S>,
    subduction_core::policy::open::OpenPolicy,
    subduction_crypto::signer::memory::MemorySigner,
    subduction_websocket::tokio::TimeoutTokio,
    sedimentree_core::depth::CountLeadingZeroBytes,
    256,
>;

#[derive(Clone)]
struct RuntimeRemoteHeadsBridge {
    msg_tx: mpsc::UnboundedSender<RuntimeMsg>,
}

impl subduction_core::remote_heads::RemoteHeadsObserver for RuntimeRemoteHeadsBridge {
    fn on_remote_heads(
        &self,
        id: SedimentreeId,
        peer: subduction_core::peer::id::PeerId,
        _heads: subduction_core::remote_heads::RemoteHeads,
    ) {
        let _ = self.msg_tx.send(RuntimeMsg::RemoteHeadsObserved {
            doc_id: id.into(),
            peer_id: peer.into(),
        });
    }
}

impl BigRepoRuntimeHandle {
    pub(super) async fn ingest_full(&self, doc_id: DocumentId, doc_save: Vec<u8>) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::IngestFull {
                doc_id,
                doc_save,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<crate::repo::LiveDocBundle>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::CreateDoc {
                initial_content,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn import_doc(
        &self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<crate::repo::LiveDocBundle>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::ImportDoc {
                doc_id,
                initial_content,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn find_doc_handle(
        &self,
        doc_id: DocumentId,
    ) -> Res<Option<Arc<crate::repo::LiveDocBundle>>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::FindDocHandle {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn local_contains_document(&self, doc_id: DocumentId) -> Res<bool> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::LocalContainsDocument {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn export_doc_save(&self, doc_id: DocumentId) -> Res<Option<Vec<u8>>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::ExportDocSave {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn ensure_peer_connection(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
    ) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::EnsurePeerConnection {
                endpoint,
                endpoint_addr,
                peer_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn remove_peer_connection(&self, peer_id: PeerId) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::RemovePeerConnection {
                peer_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn accept_incoming_connection(
        &self,
        quic_conn: iroh::endpoint::Connection,
    ) -> Res<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::AcceptIncomingConnection {
                quic_conn,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub(super) async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Res<SyncDocOutcome> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::SyncDocWithPeer {
                doc_id,
                peer_id,
                subscribe,
                timeout,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    fn release_doc_lease(&self, doc_id: DocumentId) {
        let _ = self.msg_tx.send(RuntimeMsg::ReleaseDocLease { doc_id });
    }
}

pub(super) fn spawn_big_repo_runtime<S>(
    join_set: Arc<utils_rs::AbortableJoinSet>,
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<(BigRepoRuntimeHandle, BigRepoRuntimeStopToken)>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error:
        std::fmt::Display + Send + Sync + 'static,
{
    use subduction_core::{policy::open::OpenPolicy, subduction::Subduction};
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let policy = Arc::new(OpenPolicy);
    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());
    let nonce_cache = Arc::new(subduction_core::nonce_cache::NonceCache::new(
        Duration::from_secs(60),
    ));
    let storage_for_reads = storage.clone();
    let runtime_stop = CancellationToken::new();
    let (done_tx, done_rx) = oneshot::channel();
    let runtime_tasks = Arc::new(utils_rs::AbortableJoinSet::new());

    let sedimentrees = Arc::new(subduction_core::sharded_map::ShardedMap::new());
    let connections = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let subscriptions = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let pending_blob_requests = Arc::new(async_lock::Mutex::new(
        subduction_core::subduction::pending_blob_requests::PendingBlobRequests::new(256),
    ));

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<RuntimeMsg>();
    let observer = RuntimeRemoteHeadsBridge {
        msg_tx: msg_tx.clone(),
    };
    let storage_powerbox = subduction_core::storage::powerbox::StoragePowerbox::new(
        storage.clone(),
        Arc::clone(&policy),
    );
    let sync_handler = subduction_core::handler::sync::SyncHandler::with_remote_heads_observer(
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox.clone(),
        Arc::clone(&pending_blob_requests),
        sedimentree_core::depth::CountLeadingZeroBytes,
        observer,
    );
    let send_counter = sync_handler.send_counter().clone();
    let handler = Arc::new(sync_handler);
    let (subduction, listener, manager) = Subduction::new(
        handler,
        None,
        signer,
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
        Arc::clone(&pending_blob_requests),
        send_counter,
        subduction_core::nonce_cache::NonceCache::new(Duration::from_secs(60)),
        TimeoutTokio,
        Duration::from_secs(30),
        sedimentree_core::depth::CountLeadingZeroBytes,
        TokioSpawn,
    );
    let subduction_handle: Arc<RuntimeSubduction<S>> = Arc::clone(&subduction);
    let runtime_worker = BigRepoRuntimeWorker {
        subduction: subduction_handle,
        storage_for_reads,
        live_bundles: HashMap::new(),
        partition_store,
        change_manager,
        runtime_tasks: Arc::clone(&runtime_tasks),
        connect_signer,
        local_peer_id,
        nonce_cache,
        runtime_stop: runtime_stop.clone(),
        msg_tx: msg_tx.clone(),
        connected_peers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        doc_lease_counts: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        refresh_in_flight: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
    };

    runtime_tasks
        .spawn({
            // FIXME: use method on CancellationToken for running
            // until cancellation
            let stop = runtime_stop.child_token();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        listener.await.unwrap();
                    })
                    .await;
            }
        })
        .expect("failed spawning subduction listener");
    runtime_tasks
        .spawn({
            let stop = runtime_stop.child_token();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        manager.await.unwrap();
                    })
                    .await;
            }
        })
        .expect("failed spawning subduction manager");

    join_set
        .spawn({
            let runtime_stop = runtime_stop.clone();
            let mut runtime_worker = runtime_worker;
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = runtime_stop.cancelled() => break,
                        msg = msg_rx.recv() => {
                            let Some(msg) = msg else { break; };
                            runtime_worker.handle_msg(msg).await;
                        }
                    }
                }

                let _ = runtime_worker
                    .runtime_tasks
                    .stop(Duration::from_secs(5))
                    .await;
                let _ = done_tx.send(());
            }
        })
        .expect("failed spawning big repo runtime task");

    Ok((
        BigRepoRuntimeHandle { msg_tx },
        BigRepoRuntimeStopToken {
            cancel_token: runtime_stop,
            done_rx: tokio::sync::Mutex::new(Some(done_rx)),
        },
    ))
}

#[derive(Clone)]
struct BigRepoRuntimeWorker<S>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error:
        std::fmt::Display + Send + Sync + 'static,
{
    subduction: Arc<RuntimeSubduction<S>>,
    storage_for_reads: S,
    live_bundles: HashMap<DocumentId, std::sync::Weak<crate::repo::LiveDocBundle>>,
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    connect_signer: subduction_crypto::signer::memory::MemorySigner,
    local_peer_id: subduction_core::peer::id::PeerId,
    nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    runtime_stop: CancellationToken,
    msg_tx: mpsc::UnboundedSender<RuntimeMsg>,
    connected_peers: Arc<tokio::sync::Mutex<HashMap<PeerId, RuntimePeerConnectionStopToken>>>,
    doc_lease_counts: Arc<tokio::sync::Mutex<HashMap<DocumentId, usize>>>,
    refresh_in_flight: Arc<tokio::sync::Mutex<HashSet<DocumentId>>>,
}

impl<S> BigRepoRuntimeWorker<S>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error:
        std::fmt::Display + Send + Sync + 'static,
{
    async fn handle_msg(&mut self, msg: RuntimeMsg) {
        match msg {
            RuntimeMsg::IngestFull {
                doc_id,
                doc_save,
                done,
            } => {
                self.handle_ingest_full(doc_id, doc_save, done).await;
            }
            RuntimeMsg::CreateDoc {
                initial_content,
                done,
            } => {
                self.handle_create_doc(initial_content, done).await;
            }
            RuntimeMsg::ImportDoc {
                doc_id,
                initial_content,
                done,
            } => {
                self.handle_import_doc(doc_id, initial_content, done).await;
            }
            RuntimeMsg::FindDocHandle { doc_id, done } => {
                self.handle_find_doc_handle(doc_id, done).await;
            }
            RuntimeMsg::LocalContainsDocument { doc_id, done } => {
                self.handle_local_contains_document(doc_id, done).await;
            }
            RuntimeMsg::ExportDocSave { doc_id, done } => {
                self.handle_export_doc_save(doc_id, done).await;
            }
            RuntimeMsg::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                done,
            } => {
                self.handle_commit_delta(doc_id, commits, heads, patches, origin, done)
                    .await;
            }
            RuntimeMsg::EnsurePeerConnection {
                endpoint,
                endpoint_addr,
                peer_id,
                done,
            } => {
                self.handle_ensure_peer_connection(endpoint, endpoint_addr, peer_id, done)
                    .await;
            }
            RuntimeMsg::AcceptIncomingConnection { quic_conn, done } => {
                self.handle_accept_incoming_connection(quic_conn, done)
                    .await;
            }
            RuntimeMsg::RemovePeerConnection { peer_id, done } => {
                self.handle_remove_peer_connection(peer_id, done).await;
            }
            RuntimeMsg::SyncDocWithPeer {
                doc_id,
                peer_id,
                subscribe,
                timeout,
                done,
            } => {
                self.handle_sync_doc_with_peer(doc_id, peer_id, subscribe, timeout, done)
                    .await;
            }
            RuntimeMsg::ReleaseDocLease { doc_id } => {
                self.handle_release_doc_lease(doc_id).await;
            }
            RuntimeMsg::RemoteHeadsObserved { doc_id, peer_id } => {
                self.handle_remote_heads_observed(doc_id, peer_id).await;
            }
            RuntimeMsg::LiveDocRefreshLoaded {
                doc_id,
                peer_id,
                snapshot,
            } => {
                self.handle_live_doc_refresh_loaded(doc_id, peer_id, snapshot)
                    .await;
            }
            RuntimeMsg::ConnectionEstablished {
                peer_id,
                stop_token,
            } => {
                self.handle_connection_established(peer_id, stop_token)
                    .await;
            }
            RuntimeMsg::ConnectionLost { peer_id } => {
                self.handle_connection_lost(peer_id).await;
            }
        }
    }

    fn spawn_background<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let stop = self.runtime_stop.child_token();
        self.runtime_tasks
            .spawn(async move {
                let _ = stop.run_until_cancelled(fut).await;
            })
            .expect("failed spawning runtime background task");
    }

    async fn handle_ingest_full(
        &self,
        doc_id: DocumentId,
        doc_save: Vec<u8>,
        done: oneshot::Sender<Res<()>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        self.spawn_background(async move {
            let out = (async {
                let doc = automerge::Automerge::load(&doc_save)
                    .map_err(|err| ferr!("failed loading automerge save for ingest: {err}"))?;
                let sedimentree_id: SedimentreeId = doc_id.into();
                let ingested =
                    automerge_sedimentree::ingest::ingest_automerge(&doc, sedimentree_id)
                        .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
                subduction
                    .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
                    .await
                    .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
                eyre::Ok(())
            })
            .await;
            done.send(out).expect(ERROR_CALLER);
        });
    }

    async fn handle_create_doc(
        &mut self,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
    ) {
        let result = (async {
            let mut doc_id = DocumentId::random();
            while self.local_contains_document_now(doc_id).await? {
                doc_id = DocumentId::random();
            }
            self.create_doc_with_id_now(doc_id, initial_content).await
        })
        .await;
        done.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();
    }

    async fn handle_import_doc(
        &mut self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
        done: oneshot::Sender<Res<Arc<crate::repo::LiveDocBundle>>>,
    ) {
        let result = self.import_doc_now(doc_id, initial_content).await;
        done.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();;
    }

    async fn handle_find_doc_handle(
        &mut self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Arc<crate::repo::LiveDocBundle>>>>,
    ) {
        let result = self.find_doc_handle_now(doc_id).await;
        done.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();;
    }

    async fn handle_local_contains_document(
        &self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<bool>>,
    ) {
        let result = self.local_contains_document_now(doc_id).await;
        done.send(result).inspect_err(|_| warn!(ERROR_CALLER)).ok();;
    }

    async fn handle_export_doc_save(
        &self,
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<Vec<u8>>>>,
    ) {
        let live_bundle = self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade());
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let storage_for_reads = self.storage_for_reads.clone();
        self.spawn_background(async move {
            let snapshot =
                export_doc_save_snapshot(live_bundle, &subduction, &storage_for_reads, doc_id)
                    .await;
            done.send(snapshot).expect(ERROR_CALLER);
        });
    }

    async fn local_contains_document_now(&self, doc_id: DocumentId) -> Res<bool> {
        if self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
            .is_some()
        {
            return Ok(true);
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM big_repo_docs WHERE doc_id = ?")
            .bind(doc_id.to_string())
            .fetch_one(self.partition_store.state_pool())
            .await
            .wrap_err("failed checking big_repo_docs")?;
        Ok(count > 0)
    }

    async fn import_doc_now(
        &mut self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<crate::repo::LiveDocBundle>> {
        let lease = self.acquire_doc_lease_now(doc_id).await?;
        let bundle = Arc::new(crate::repo::LiveDocBundle::new(
            doc_id,
            initial_content,
            lease,
        ));
        let result = async {
            self.persist_bundle_now(&bundle).await?;
            self.upsert_known_doc_now(bundle.doc_id).await?;
            self.register_live_bundle(Arc::clone(&bundle));
            self.record_live_bundle_imported(Arc::clone(&bundle))
                .await?;
            eyre::Ok(bundle)
        }
        .await;
        result
    }

    async fn create_doc_with_id_now(
        &mut self,
        doc_id: DocumentId,
        initial_content: automerge::Automerge,
    ) -> Res<Arc<crate::repo::LiveDocBundle>> {
        let lease = self.acquire_doc_lease_now(doc_id).await?;
        let bundle = Arc::new(crate::repo::LiveDocBundle::new(
            doc_id,
            initial_content,
            lease,
        ));
        let result = async {
            self.persist_bundle_now(&bundle).await?;
            self.upsert_known_doc_now(bundle.doc_id).await?;
            self.register_live_bundle(Arc::clone(&bundle));
            self.record_live_bundle_created(Arc::clone(&bundle)).await?;
            eyre::Ok(bundle)
        }
        .await;
        result
    }

    async fn find_doc_handle_now(
        &mut self,
        doc_id: DocumentId,
    ) -> Res<Option<Arc<crate::repo::LiveDocBundle>>> {
        if let Some(bundle) = self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
        {
            return Ok(Some(bundle));
        }

        let Some(doc) =
            load_doc_snapshot(&self.subduction, &self.storage_for_reads, doc_id).await?
        else {
            return Ok(None);
        };
        let lease = self.acquire_doc_lease_now(doc_id).await?;
        let bundle = Arc::new(crate::repo::LiveDocBundle::new(doc_id, doc, lease));
        let result = async {
            self.upsert_known_doc_now(doc_id).await?;
            self.register_live_bundle(Arc::clone(&bundle));
            eyre::Ok(Some(bundle))
        }
        .await;
        result
    }

    fn register_live_bundle(&mut self, bundle: Arc<crate::repo::LiveDocBundle>) {
        self.live_bundles
            .insert(bundle.doc_id, Arc::downgrade(&bundle));
    }

    async fn persist_bundle_now(&self, bundle: &crate::repo::LiveDocBundle) -> Res<()> {
        let doc = bundle.doc.lock().await;
        self.ingest_save_now(bundle.doc_id, doc.save()).await
    }

    async fn ingest_save_now(&self, doc_id: DocumentId, doc_save: Vec<u8>) -> Res<()> {
        let doc = automerge::Automerge::load(&doc_save)
            .map_err(|err| ferr!("failed loading automerge save for ingest: {err}"))?;
        let sedimentree_id: SedimentreeId = doc_id.into();
        let ingested = automerge_sedimentree::ingest::ingest_automerge(&doc, sedimentree_id)
            .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
        self.subduction
            .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
            .await
            .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
        Ok(())
    }

    async fn upsert_known_doc_now(&self, doc_id: DocumentId) -> Res<()> {
        sqlx::query("INSERT INTO big_repo_docs(doc_id) VALUES(?) ON CONFLICT(doc_id) DO NOTHING")
            .bind(doc_id.to_string())
            .execute(self.partition_store.state_pool())
            .await
            .wrap_err("failed upserting big_repo_docs")?;
        Ok(())
    }

    async fn record_live_bundle_created(&self, bundle: Arc<crate::repo::LiveDocBundle>) -> Res<()> {
        let doc = bundle.doc.lock().await;
        let heads = Arc::<[automerge::ChangeHash]>::from(doc.get_heads());
        let change_count_hint = doc.get_changes(&[]).len().max(1) as u64;
        let item_payload = serde_json::json!({
            "heads": crate::serialize_commit_heads(&heads),
            "change_count_hint": change_count_hint.max(1),
        });
        self.partition_store
            .record_member_item_change(&bundle.doc_id.to_string(), &item_payload)
            .await?;
        self.change_manager
            .notify_doc_created(bundle.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(bundle.doc_id, heads)?;
        Ok(())
    }

    async fn record_live_bundle_imported(
        &self,
        bundle: Arc<crate::repo::LiveDocBundle>,
    ) -> Res<()> {
        let doc = bundle.doc.lock().await;
        let heads = Arc::<[automerge::ChangeHash]>::from(doc.get_heads());
        let change_count_hint = doc.get_changes(&[]).len().max(1) as u64;
        let item_payload = serde_json::json!({
            "heads": crate::serialize_commit_heads(&heads),
            "change_count_hint": change_count_hint.max(1),
        });
        self.partition_store
            .record_member_item_change(&bundle.doc_id.to_string(), &item_payload)
            .await?;
        self.change_manager
            .notify_doc_imported(bundle.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_imported(bundle.doc_id, heads)?;
        Ok(())
    }

    async fn acquire_doc_lease_now(&self, doc_id: DocumentId) -> Res<RuntimeDocLease> {
        let mut counts = self.doc_lease_counts.lock().await;
        let count = counts.entry(doc_id).or_insert(0);
        let first = *count == 0;
        *count += 1;
        drop(counts);

        let result = async {
            if first {
                let sedimentree_id: SedimentreeId = doc_id.into();
                let missing_in_runtime = self
                    .subduction
                    .get_blobs(sedimentree_id)
                    .await
                    .map_err(|err| ferr!("failed reading blobs from subduction: {err}"))?
                    .is_none();
                if missing_in_runtime {
                    if let Some(blobs) =
                        load_blobs_from_storage(&self.storage_for_reads, sedimentree_id)
                            .await
                            .map_err(|err| ferr!("failed reading blobs from storage: {err}"))?
                    {
                        let loaded_doc = reconstruct_automerge_from_blobs(blobs.clone())?;
                        let ingested = automerge_sedimentree::ingest::ingest_automerge(
                            &loaded_doc,
                            sedimentree_id,
                        )
                        .map_err(|err| {
                            ferr!("failed ingesting automerge doc for lease hydrate: {err}")
                        })?;
                        self.subduction
                            .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
                            .await
                            .map_err(|err| {
                                ferr!("failed add_sedimentree for lease hydrate: {err}")
                            })?;
                    }
                }
            }
            eyre::Ok(())
        }
        .await;
        if result.is_err() {
            let mut counts = self.doc_lease_counts.lock().await;
            if let Some(count) = counts.get_mut(&doc_id) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    counts.remove(&doc_id);
                }
            }
        }
        result?;
        Ok(RuntimeDocLease {
            runtime: BigRepoRuntimeHandle {
                msg_tx: self.msg_tx.clone(),
            },
            doc_id,
        })
    }

    async fn release_doc_lease_now(&self, doc_id: DocumentId) {
        let mut counts = self.doc_lease_counts.lock().await;
        if let Some(count) = counts.get_mut(&doc_id) {
            if *count > 1 {
                *count -= 1;
            } else {
                counts.remove(&doc_id);
            }
        }
    }

    async fn handle_commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        done: oneshot::Sender<Res<()>>,
    ) {
        let partition_store = Arc::clone(&self.partition_store);
        let change_manager = Arc::clone(&self.change_manager);
        self.spawn_background(async move {
            let out = apply_commit_delta(
                &partition_store,
                &change_manager,
                doc_id,
                commits.len().max(1) as u64,
                heads,
                patches,
                origin,
            )
            .await;
            done.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        });
    }

    async fn handle_ensure_peer_connection(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        done: oneshot::Sender<Res<()>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let msg_tx = self.msg_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        self.spawn_background(async move {
            let out = async {
                if connected_peers.lock().await.contains_key(&peer_id) {
                    return Ok(());
                }
                let connect = connect_outgoing(endpoint, endpoint_addr, &connect_signer)
                    .await
                    .map_err(|err| ferr!("failed subduction iroh connect: {err}"))?;
                let stop_token = RuntimePeerConnectionStopToken::new();
                let listener_stop = stop_token.child_token();
                let sender_stop = stop_token.child_token();
                let peer_id_for_listener = peer_id;
                let peer_id_for_sender = peer_id;
                let msg_tx_for_listener = msg_tx.clone();
                let msg_tx_for_sender = msg_tx.clone();
                runtime_tasks
                    .spawn({
                        let fut = connect.listener_task;
                        async move {
                            tokio::select! {
                                biased;
                                _ = listener_stop.cancelled() => {}
                                res = fut => {
                                    match res {
                                        Ok(()) => {
                                            let _ = msg_tx_for_listener.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_listener });
                                        }
                                        Err(err) => {
                                            warn!(?err, %peer_id_for_listener, "connection listener task exited");
                                            let _ = msg_tx_for_listener.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_listener });
                                        }
                                    }
                                }
                            }
                        }
                    })
                    .expect("failed spawning connection listener");
                runtime_tasks
                    .spawn({
                        let fut = connect.sender_task;
                        async move {
                            tokio::select! {
                                biased;
                                _ = sender_stop.cancelled() => {}
                                res = fut => {
                                    match res {
                                        Ok(()) => {
                                            let _ = msg_tx_for_sender.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_sender });
                                        }
                                        Err(err) => {
                                            warn!(?err, %peer_id_for_sender, "connection sender task exited");
                                            let _ = msg_tx_for_sender.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_sender });
                                        }
                                    }
                                }
                            }
                        }
                    })
                    .expect("failed spawning connection sender");
                subduction
                    .add_connection(connect.authenticated)
                    .await
                    .map_err(|err| ferr!("failed subduction add_connection: {err}"))?;
                let _ = msg_tx.send(RuntimeMsg::ConnectionEstablished { peer_id, stop_token });
                Ok(())
            }
            .await;
            done.send(out).expect(ERROR_CALLER);
        });
    }

    async fn handle_accept_incoming_connection(
        &self,
        quic_conn: iroh::endpoint::Connection,
        done: oneshot::Sender<Res<()>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let nonce_cache = Arc::clone(&self.nonce_cache);
        let local_peer_id = self.local_peer_id;
        let msg_tx = self.msg_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        self.spawn_background(async move {
            let out = async {
                let accepted = accept_incoming(
                    quic_conn,
                    &connect_signer,
                    nonce_cache.as_ref(),
                    local_peer_id,
                )
                .await
                .map_err(|err| ferr!("failed subduction iroh accept: {err}"))?;
                let peer_id = PeerId::from(accepted.authenticated.peer_id());
                if connected_peers.lock().await.contains_key(&peer_id) {
                    return Ok(());
                }
                let stop_token = RuntimePeerConnectionStopToken::new();
                let listener_stop = stop_token.child_token();
                let sender_stop = stop_token.child_token();
                let peer_id_for_listener = peer_id;
                let peer_id_for_sender = peer_id;
                let msg_tx_for_listener = msg_tx.clone();
                let msg_tx_for_sender = msg_tx.clone();
                runtime_tasks
                    .spawn({
                        let fut = accepted.listener_task;
                        async move {
                            tokio::select! {
                                biased;
                                _ = listener_stop.cancelled() => {}
                                res = fut => {
                                    match res {
                                        Ok(()) => {
                                            let _ = msg_tx_for_listener.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_listener });
                                        }
                                        Err(err) => {
                                            warn!(?err, %peer_id_for_listener, "incoming connection listener task exited");
                                            let _ = msg_tx_for_listener.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_listener });
                                        }
                                    }
                                }
                            }
                        }
                    })
                    .expect("failed spawning incoming connection listener");
                runtime_tasks
                    .spawn({
                        let fut = accepted.sender_task;
                        async move {
                            tokio::select! {
                                biased;
                                _ = sender_stop.cancelled() => {}
                                res = fut => {
                                    match res {
                                        Ok(()) => {
                                            let _ = msg_tx_for_sender.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_sender });
                                        }
                                        Err(err) => {
                                            warn!(?err, %peer_id_for_sender, "incoming connection sender task exited");
                                            let _ = msg_tx_for_sender.send(RuntimeMsg::ConnectionLost { peer_id: peer_id_for_sender });
                                        }
                                    }
                                }
                            }
                        }
                    })
                    .expect("failed spawning incoming connection sender");
                subduction
                    .add_connection(accepted.authenticated)
                    .await
                    .map_err(|err| ferr!("failed subduction add_connection: {err}"))?;
                let _ = msg_tx.send(RuntimeMsg::ConnectionEstablished { peer_id, stop_token });
                Ok(())
            }
            .await;
            done.send(out).expect(ERROR_CALLER);
        });
    }

    async fn handle_remove_peer_connection(&self, peer_id: PeerId, done: oneshot::Sender<Res<()>>) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let connected_peers = Arc::clone(&self.connected_peers);
        self.spawn_background(async move {
            let out = async {
                let stop_token = connected_peers.lock().await.remove(&peer_id);
                if let Some(stop_token) = stop_token {
                    stop_token.cancel();
                    let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
                    subduction
                        .disconnect_from_peer(&remote_peer_id)
                        .await
                        .map_err(|err| ferr!("failed subduction disconnect_from_peer: {err}"))?;
                }
                Ok(())
            }
            .await;
            done.send(out).expect(ERROR_CALLER);
        });
    }

    async fn handle_sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
        done: oneshot::Sender<Res<SyncDocOutcome>>,
    ) {
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let storage_for_reads = self.storage_for_reads.clone();
        let worker = self.clone();
        self.spawn_background(async move {
            let out = async {
                let sedimentree_id: SedimentreeId = doc_id.into();
                let before_doc =
                    load_doc_snapshot(&subduction, &storage_for_reads, sedimentree_id).await?;
                let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
                let result = subduction
                    .sync_with_peer(&remote_peer_id, sedimentree_id, subscribe, timeout)
                    .await;
                let outcome = match result {
                    Ok((had_success, _stats, conn_errs)) => {
                        if had_success {
                            SyncDocOutcome::Success
                        } else if conn_errs.is_empty() {
                            SyncDocOutcome::NotFoundOrUnauthorized
                        } else {
                            SyncDocOutcome::TransportError
                        }
                    }
                    Err(err) => {
                        warn!(?err, %doc_id, %peer_id, "subduction sync_with_peer io error");
                        SyncDocOutcome::IoError
                    }
                };
                if matches!(outcome, SyncDocOutcome::Success) {
                    // FIXME: are we loadign the full sedimentree here?
                    let after_doc =
                        load_doc_snapshot(&subduction, &storage_for_reads, sedimentree_id).await?;
                    if let Some(after_doc) = after_doc {
                        let before_heads = before_doc
                            .as_ref()
                            .map_or_else(Vec::new, |doc| doc.get_heads());
                        let after_heads = after_doc.get_heads();
                        if before_heads != after_heads {
                            let patches = after_doc.diff(&before_heads, &after_heads);
                            let change_count =
                                after_doc.get_changes(&before_heads).len().max(1) as u64;
                            apply_commit_delta(
                                &worker.partition_store,
                                &worker.change_manager,
                                doc_id,
                                change_count,
                                after_heads,
                                patches,
                                BigRepoChangeOrigin::Remote { peer_id },
                            )
                            .await?;
                        }
                    }
                }
                Ok(outcome)
            }
            .await;
            done.send(out).expect(ERROR_CALLER);
        });
    }

    async fn handle_release_doc_lease(&self, doc_id: DocumentId) {
        self.release_doc_lease_now(doc_id).await;
    }

    async fn handle_remote_heads_observed(&mut self, doc_id: DocumentId, peer_id: PeerId) {
        if self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
            .is_none()
        {
            return;
        }
        let mut in_flight = self.refresh_in_flight.lock().await;
        if !in_flight.insert(doc_id) {
            return;
        }
        drop(in_flight);
        let subduction: Arc<RuntimeSubduction<S>> = Arc::clone(&self.subduction);
        let storage_for_reads = self.storage_for_reads.clone();
        let msg_tx = self.msg_tx.clone();
        self.spawn_background(async move {
            let snapshot = load_doc_snapshot(&subduction, &storage_for_reads, doc_id).await;
            let _ = msg_tx.send(RuntimeMsg::LiveDocRefreshLoaded {
                doc_id,
                peer_id,
                snapshot,
            });
        });
    }

    async fn handle_live_doc_refresh_loaded(
        &mut self,
        doc_id: DocumentId,
        peer_id: PeerId,
        snapshot: Res<Option<automerge::Automerge>>,
    ) {
        self.refresh_in_flight.lock().await.remove(&doc_id);
        let Ok(Some(after_doc)) = snapshot else {
            return;
        };
        let Some(bundle) = self
            .live_bundles
            .get(&doc_id)
            .and_then(|entry| entry.upgrade())
        else {
            return;
        };
        self.apply_live_doc_refresh(bundle, after_doc, Some(peer_id))
            .await;
    }

    async fn handle_connection_lost(&self, peer_id: PeerId) {
        let maybe_stop = self.connected_peers.lock().await.remove(&peer_id);
        if let Some(connection) = maybe_stop {
            connection.cancel();
            let remote_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
            let _ = self.subduction.disconnect_from_peer(&remote_peer_id).await;
        }
    }

    async fn handle_connection_established(
        &self,
        peer_id: PeerId,
        stop_token: RuntimePeerConnectionStopToken,
    ) {
        if stop_token.is_cancelled() {
            return;
        }
        let mut connected_peers = self.connected_peers.lock().await;
        if let Some(previous) = connected_peers.insert(peer_id, stop_token) {
            previous.cancel();
        }
    }

    async fn apply_live_doc_refresh(
        &self,
        bundle: Arc<crate::repo::LiveDocBundle>,
        mut after_doc: automerge::Automerge,
        peer_id: Option<PeerId>,
    ) {
        let doc_id = bundle.doc_id;
        let before_heads = {
            let doc = bundle.doc.lock().await;
            doc.get_heads()
        };
        let maybe_delta = {
            let mut doc = bundle.doc.lock().await;
            if let Err(err) = doc.merge(&mut after_doc) {
                warn!(?err, %doc_id, "failed merging refreshed doc into live bundle");
                return;
            }
            let after_heads = doc.get_heads();
            if before_heads == after_heads {
                None
            } else {
                let patches = doc.diff(&before_heads, &after_heads);
                let change_count = doc.get_changes(&before_heads).len().max(1) as u64;
                Some((after_heads, patches, change_count))
            }
        };
        let Some((after_heads, patches, change_count)) = maybe_delta else {
            return;
        };
        let origin = peer_id
            .map(|peer_id| BigRepoChangeOrigin::Remote { peer_id })
            .unwrap_or(BigRepoChangeOrigin::Bootstrap);
        if let Err(err) = apply_commit_delta(
            &self.partition_store,
            &self.change_manager,
            doc_id,
            change_count,
            after_heads,
            patches,
            origin,
        )
        .await
        {
            warn!(?err, %doc_id, "failed to record refreshed live doc delta");
        }
    }
}

impl BigRepoRuntimeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        let done_rx = self
            .done_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| eyre::eyre!("runtime stop token already consumed"))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;
        Ok(())
    }
}

async fn apply_commit_delta(
    partition_store: &Arc<PartitionStore>,
    change_manager: &Arc<changes::ChangeListenerManager>,
    doc_id: DocumentId,
    change_count_hint: u64,
    heads: Vec<automerge::ChangeHash>,
    patches: Vec<automerge::Patch>,
    origin: BigRepoChangeOrigin,
) -> Res<()> {
    let item_payload = serde_json::json!({
        "heads": crate::serialize_commit_heads(&heads),
        "change_count_hint": change_count_hint.max(1),
    });
    partition_store
        .record_member_item_change(&doc_id.to_string(), &item_payload)
        .await?;

    let heads_arc = Arc::<[automerge::ChangeHash]>::from(heads);
    change_manager.notify_doc_heads_changed(doc_id, Arc::clone(&heads_arc), origin.clone())?;
    if matches!(origin, BigRepoChangeOrigin::Local) {
        change_manager.notify_local_doc_heads_updated(doc_id, Arc::clone(&heads_arc))?;
    }
    for patch in patches {
        change_manager.notify_doc_changed(
            doc_id,
            Arc::new(patch),
            Arc::clone(&heads_arc),
            origin.clone(),
        )?;
    }

    Ok(())
}

async fn load_doc_snapshot<S, D>(
    subduction: &Arc<RuntimeSubduction<S>>,
    storage_for_reads: &S,
    doc_id: D,
) -> Res<Option<automerge::Automerge>>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error:
        std::fmt::Display + Send + Sync + 'static,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    let sedimentree_id: SedimentreeId = doc_id.into();
    let blobs = match subduction
        .get_blobs(sedimentree_id)
        .await
        .map_err(|err| ferr!("failed reading blobs from subduction: {err}"))?
    {
        Some(blobs) => blobs.into_iter().collect::<Vec<_>>(),
        None => {
            let Some(blobs) = load_blobs_from_storage(storage_for_reads, sedimentree_id)
                .await
                .map_err(|err| ferr!("failed reading blobs from storage: {err}"))?
            else {
                return Ok(None);
            };
            let loaded_doc = reconstruct_automerge_from_blobs(blobs.clone())?;
            let ingested =
                automerge_sedimentree::ingest::ingest_automerge(&loaded_doc, sedimentree_id)
                    .map_err(|err| {
                        ferr!("failed ingesting automerge doc from storage fallback: {err}")
                    })?;
            subduction
                .add_sedimentree(sedimentree_id, ingested.sedimentree, ingested.blobs)
                .await
                .map_err(|err| ferr!("failed add_sedimentree from storage fallback: {err}"))?;
            blobs
        }
    };
    let doc = reconstruct_automerge_from_blobs(blobs)?;
    Ok(Some(doc))
}

async fn export_doc_save_snapshot<S, D>(
    live_bundle: Option<Arc<crate::repo::LiveDocBundle>>,
    subduction: &Arc<RuntimeSubduction<S>>,
    storage_for_reads: &S,
    doc_id: D,
) -> Res<Option<Vec<u8>>>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>
        + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error:
        std::fmt::Display + Send + Sync + 'static,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    if let Some(bundle) = live_bundle {
        let doc = bundle.doc.lock().await;
        return Ok(Some(doc.save()));
    }
    Ok(load_doc_snapshot(subduction, storage_for_reads, doc_id)
        .await?
        .map(|doc| doc.save()))
}

struct IrohConnectResult {
    authenticated: subduction_core::authenticated::Authenticated<
        subduction_core::transport::message::MessageTransport<
            subduction_iroh::transport::IrohTransport,
        >,
        future_form::Sendable,
    >,
    listener_task: BoxFuture<'static, Result<(), subduction_iroh::error::RunError>>,
    sender_task: BoxFuture<'static, Result<(), subduction_iroh::error::RunError>>,
}

async fn connect_outgoing(
    endpoint: iroh::Endpoint,
    endpoint_addr: iroh::EndpointAddr,
    signer: &subduction_crypto::signer::memory::MemorySigner,
) -> Res<IrohConnectResult> {
    let connected = subduction_iroh::client::connect(
        &endpoint,
        endpoint_addr,
        signer,
        subduction_core::handshake::audience::Audience::discover(b"townframe-subduction"),
    )
    .await
    .map_err(|err| ferr!("subduction iroh connect failed: {err}"))?;
    Ok(IrohConnectResult {
        authenticated: connected
            .authenticated
            .map(subduction_core::transport::message::MessageTransport::new),
        listener_task: connected.listener_task,
        sender_task: connected.sender_task,
    })
}

async fn accept_incoming(
    quic_conn: iroh::endpoint::Connection,
    signer: &subduction_crypto::signer::memory::MemorySigner,
    nonce_cache: &subduction_core::nonce_cache::NonceCache,
    local_peer_id: subduction_core::peer::id::PeerId,
) -> Res<IrohConnectResult> {
    let (send, recv) = quic_conn
        .accept_bi()
        .await
        .map_err(|err| ferr!("failed accepting subduction bidi stream: {err}"))?;
    let now = subduction_core::timestamp::TimestampSeconds::now();
    let handshake = subduction_iroh::handshake::IrohHandshake::new(send, recv);
    let quic_conn_clone = quic_conn.clone();
    let (authenticated, (listener_task, sender_task)) = subduction_core::handshake::respond(
        handshake,
        move |handshake, peer_id| {
            let (send, recv) = handshake.into_parts();
            let (transport, outbound_rx) =
                subduction_iroh::transport::IrohTransport::new(peer_id, quic_conn_clone);
            let listener_transport = transport.clone();
            let listener_task = Box::pin(subduction_iroh::tasks::listener_task(
                listener_transport,
                recv,
            ));
            let sender_task = Box::pin(subduction_iroh::tasks::sender_task(send, outbound_rx));
            (transport, (listener_task, sender_task))
        },
        signer,
        nonce_cache,
        local_peer_id,
        Some(subduction_core::handshake::audience::Audience::discover(
            b"townframe-subduction",
        )),
        now,
        Duration::from_secs(600),
    )
    .await
    .map_err(|err| ferr!("subduction handshake respond failed: {err}"))?;
    Ok(IrohConnectResult {
        authenticated: authenticated
            .map(subduction_core::transport::message::MessageTransport::new),
        listener_task,
        sender_task,
    })
}

fn reconstruct_automerge_from_blobs<I>(blobs: I) -> Res<automerge::Automerge>
where
    I: IntoIterator<Item = Blob>,
{
    let mut doc = automerge::Automerge::new();
    let mut pending = blobs
        .into_iter()
        .map(|blob| blob.as_slice().to_vec())
        .collect::<Vec<_>>();
    while !pending.is_empty() {
        let mut next = Vec::new();
        let mut progressed = false;
        for blob in pending {
            match doc.load_incremental(&blob) {
                Ok(_) => progressed = true,
                Err(_) => next.push(blob),
            }
        }
        if !progressed {
            eyre::bail!("failed reconstructing automerge doc from subduction blobs");
        }
        pending = next;
    }
    Ok(doc)
}

async fn load_blobs_from_storage<S>(
    storage: &S,
    sedimentree_id: SedimentreeId,
) -> Result<
    Option<Vec<Blob>>,
    <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::Error,
>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable>,
{
    let mut blobs = Vec::new();
    for verified in
        <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_loose_commits(
            storage,
            sedimentree_id,
        )
        .await?
    {
        blobs.push(verified.blob().clone());
    }
    for verified in
        <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_fragments(
            storage,
            sedimentree_id,
        )
        .await?
    {
        blobs.push(verified.blob().clone());
    }
    if blobs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(blobs))
    }
}
