use crate::interlude::*;
use crate::partition::PartitionStore;
use crate::repo::{BigRepoChangeOrigin, DocumentId, PeerId};

use futures::future::BoxFuture;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

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
    LoadDoc {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<Option<automerge::Automerge>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        // FIXME: pre making the patches is not cheap, we should
        // only bother if there's a listener (and there isn't any on content docs yet)
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
    AcquireDocLease {
        doc_id: DocumentId,
        done: oneshot::Sender<Res<()>>,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
    },
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub(super) struct BigRepoRuntimeHandle {
    msg_tx: mpsc::UnboundedSender<RuntimeMsg>,
}

#[derive(Clone)]
pub(super) struct RuntimeDocLease {
    runtime: BigRepoRuntimeHandle,
    doc_id: DocumentId,
}

impl Drop for RuntimeDocLease {
    fn drop(&mut self) {
        self.runtime.release_doc_lease(self.doc_id);
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

    pub(super) async fn load_doc(&self, doc_id: DocumentId) -> Res<Option<automerge::Automerge>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::LoadDoc {
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

    pub(super) async fn shutdown(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self.msg_tx.send(RuntimeMsg::Shutdown { done: done_tx });
        let _ = done_rx.await;
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

    pub(super) async fn acquire_doc_lease(&self, doc_id: DocumentId) -> Res<RuntimeDocLease> {
        let (done_tx, done_rx) = oneshot::channel();
        self.msg_tx
            .send(RuntimeMsg::AcquireDocLease {
                doc_id,
                done: done_tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        done_rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))??;
        Ok(RuntimeDocLease {
            runtime: self.clone(),
            doc_id,
        })
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
) -> Res<BigRepoRuntimeHandle>
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
    use subduction_core::{
        policy::open::OpenPolicy, subduction::builder::SubductionBuilder,
        transport::message::MessageTransport,
    };
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let policy = Arc::new(OpenPolicy);
    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());
    let nonce_cache = Arc::new(subduction_core::nonce_cache::NonceCache::new(
        Duration::from_secs(60),
    ));
    let storage_for_reads = storage.clone();
    let (subduction, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(signer)
        .storage(storage, policy)
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .build::<
            future_form::Sendable,
            MessageTransport<subduction_iroh::transport::IrohTransport>,
        >();

    join_set
        .spawn(async move {
            listener.await.unwrap();
        })
        .expect("failed spawning subduction listener");
    join_set
        .spawn(async move {
            manager.await.unwrap();
        })
        .expect("failed spawning subduction manager");

    let worker = BigRepoRuntimeWorker {
        partition_store,
        change_manager,
    };
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<RuntimeMsg>();
    let mut connected_peers: HashSet<PeerId> = HashSet::new();
    let mut doc_lease_counts: HashMap<DocumentId, usize> = HashMap::new();
    join_set
        .spawn(async move {
            while let Some(msg) = msg_rx.recv().await {
                match msg {
                    RuntimeMsg::IngestFull {
                        doc_id,
                        doc_save,
                        done,
                    } => {
                        let out = async {
                            let doc = automerge::Automerge::load(&doc_save).map_err(|err| {
                                ferr!("failed loading automerge save for ingest: {err}")
                            })?;
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            let ingested = automerge_sedimentree::ingest::ingest_automerge(
                                &doc,
                                sedimentree_id,
                            )
                            .map_err(|err| ferr!("failed ingesting automerge doc: {err}"))?;
                            subduction
                                .add_sedimentree(
                                    sedimentree_id,
                                    ingested.sedimentree,
                                    ingested.blobs,
                                )
                                .await
                                .map_err(|err| ferr!("failed add_sedimentree: {err}"))?;
                            eyre::Ok(())
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::LoadDoc { doc_id, done } => {
                        let out = async {
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            let blobs = match subduction
                                .get_blobs(sedimentree_id)
                                .await
                                .map_err(|err| ferr!("failed reading blobs from subduction: {err}"))?
                            {
                                Some(blobs) => blobs.into_iter().collect::<Vec<_>>(),
                                None => {
                                    let Some(blobs) =
                                        load_blobs_from_storage(&storage_for_reads, sedimentree_id)
                                            .await
                                            .map_err(|err| {
                                                ferr!("failed reading blobs from storage: {err}")
                                            })?
                                    else {
                                        return Ok(None);
                                    };
                                    let loaded_doc = reconstruct_automerge_from_blobs(blobs.clone())?;
                                    let ingested = automerge_sedimentree::ingest::ingest_automerge(
                                        &loaded_doc,
                                        sedimentree_id,
                                    )
                                    .map_err(|err| {
                                        ferr!("failed ingesting automerge doc from storage fallback: {err}")
                                    })?;
                                    subduction
                                        .add_sedimentree(
                                            sedimentree_id,
                                            ingested.sedimentree,
                                            ingested.blobs,
                                        )
                                        .await
                                        .map_err(|err| {
                                            ferr!("failed add_sedimentree from storage fallback: {err}")
                                        })?;
                                    blobs
                                }
                            };
                            let doc = reconstruct_automerge_from_blobs(blobs)?;
                            eyre::Ok(Some(doc))
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::CommitDelta {
                        doc_id,
                        commits,
                        heads,
                        patches,
                        origin,
                        done,
                    } => {
                        let out = async {
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            for (head, parents, bytes) in commits {
                                subduction
                                    .add_commit(sedimentree_id, head, parents, Blob::new(bytes))
                                    .await
                                    .map_err(|err| ferr!("failed add_commit: {err}"))?;
                            }
                            worker
                                .handle_commit_delta(doc_id, heads, patches, origin)
                                .await
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::EnsurePeerConnection {
                        endpoint,
                        endpoint_addr,
                        peer_id,
                        done,
                    } => {
                        let out = async {
                            if connected_peers.contains(&peer_id) {
                                return Ok(());
                            }
                            let connect = connect_outgoing(
                                endpoint,
                                endpoint_addr.clone(),
                                &connect_signer,
                            )
                            .await
                            .map_err(|err| ferr!("failed subduction iroh connect: {err}"))?;
                            tokio::spawn(async move {
                                connect.listener_task.await.unwrap();
                            });
                            tokio::spawn(async move {
                                connect.sender_task.await.unwrap();
                            });
                            subduction
                                .add_connection(connect.authenticated)
                                .await
                                .map_err(|err| ferr!("failed subduction add_connection: {err}"))?;
                            connected_peers.insert(peer_id);
                            Ok(())
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::AcceptIncomingConnection { quic_conn, done } => {
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
                            tokio::spawn(async move {
                                accepted.listener_task.await.unwrap();
                            });
                            tokio::spawn(async move {
                                accepted.sender_task.await.unwrap();
                            });
                            subduction
                                .add_connection(accepted.authenticated)
                                .await
                                .map_err(|err| ferr!("failed subduction add_connection: {err}"))?;
                            connected_peers.insert(peer_id);
                            Ok(())
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::RemovePeerConnection { peer_id, done } => {
                        let out = async {
                            let removed = connected_peers.remove(&peer_id);
                            if removed {
                                let remote_peer_id: subduction_core::peer::id::PeerId =
                                    peer_id.into();
                                subduction
                                    .disconnect_from_peer(&remote_peer_id)
                                    .await
                                    .map_err(|err| {
                                        ferr!("failed subduction disconnect_from_peer: {err}")
                                    })?;
                            }
                            Ok(())
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::SyncDocWithPeer {
                        doc_id,
                        peer_id,
                        subscribe,
                        timeout,
                        done,
                    } => {
                        let out = async {
                            let sedimentree_id: SedimentreeId = doc_id.into();
                            let before_doc = match subduction
                                .get_blobs(sedimentree_id)
                                .await
                                .map_err(|err| ferr!("failed reading blobs from subduction: {err}"))?
                            {
                                Some(blobs) => {
                                    Some(reconstruct_automerge_from_blobs(blobs.into_iter())?)
                                }
                                None => {
                                    match load_blobs_from_storage(&storage_for_reads, sedimentree_id)
                                        .await
                                        .map_err(|err| {
                                            ferr!("failed reading blobs from storage: {err}")
                                        })? {
                                        Some(blobs) => {
                                            Some(reconstruct_automerge_from_blobs(blobs)?)
                                        }
                                        None => None,
                                    }
                                }
                            };
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
                                let after_doc = match subduction
                                    .get_blobs(sedimentree_id)
                                    .await
                                    .map_err(|err| {
                                        ferr!("failed reading blobs from subduction: {err}")
                                    })? {
                                    Some(blobs) => {
                                        Some(reconstruct_automerge_from_blobs(blobs.into_iter())?)
                                    }
                                    None => {
                                        match load_blobs_from_storage(
                                            &storage_for_reads,
                                            sedimentree_id,
                                        )
                                        .await
                                        .map_err(|err| {
                                            ferr!("failed reading blobs from storage: {err}")
                                        })? {
                                            Some(blobs) => {
                                                Some(reconstruct_automerge_from_blobs(blobs)?)
                                            }
                                            None => None,
                                        }
                                    }
                                };
                                if let Some(after_doc) = after_doc {
                                    let before_heads =
                                        before_doc.as_ref().map_or_else(Vec::new, |doc| doc.get_heads());
                                    let after_heads = after_doc.get_heads();
                                    if before_heads != after_heads {
                                        let patches = after_doc.diff(&before_heads, &after_heads);
                                        worker
                                            .handle_commit_delta(
                                                doc_id,
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
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::AcquireDocLease { doc_id, done } => {
                        let out = async {
                            let count = doc_lease_counts.entry(doc_id).or_insert(0);
                            if *count == 0 {
                                let sedimentree_id: SedimentreeId = doc_id.into();
                                let missing_in_runtime = subduction
                                    .get_blobs(sedimentree_id)
                                    .await
                                    .map_err(|err| ferr!("failed reading blobs from subduction: {err}"))?
                                    .is_none();
                                if missing_in_runtime {
                                    if let Some(blobs) =
                                        load_blobs_from_storage(&storage_for_reads, sedimentree_id)
                                            .await
                                            .map_err(|err| {
                                                ferr!("failed reading blobs from storage: {err}")
                                            })?
                                    {
                                        let loaded_doc =
                                            reconstruct_automerge_from_blobs(blobs.clone())?;
                                        let ingested =
                                            automerge_sedimentree::ingest::ingest_automerge(
                                                &loaded_doc,
                                                sedimentree_id,
                                            )
                                            .map_err(|err| {
                                                ferr!("failed ingesting automerge doc for lease hydrate: {err}")
                                            })?;
                                        subduction
                                            .add_sedimentree(
                                                sedimentree_id,
                                                ingested.sedimentree,
                                                ingested.blobs,
                                            )
                                            .await
                                            .map_err(|err| {
                                                ferr!("failed add_sedimentree for lease hydrate: {err}")
                                            })?;
                                    }
                                }
                            }
                            *count += 1;
                            Ok(())
                        }
                        .await;
                        done.send(out).unwrap();
                    }
                    RuntimeMsg::ReleaseDocLease { doc_id } => {
                        if let Some(count) = doc_lease_counts.get_mut(&doc_id) {
                            if *count > 1 {
                                *count -= 1;
                            } else {
                                doc_lease_counts.remove(&doc_id);
                            }
                        }
                    }
                    RuntimeMsg::Shutdown { done } => {
                        done.send(()).unwrap();
                        break;
                    }
                }
            }
        })
        .expect("failed spawning big repo runtime task");

    Ok(BigRepoRuntimeHandle { msg_tx })
}

struct BigRepoRuntimeWorker {
    partition_store: Arc<PartitionStore>,
    change_manager: Arc<changes::ChangeListenerManager>,
}

impl BigRepoRuntimeWorker {
    async fn handle_commit_delta(
        &self,
        doc_id: DocumentId,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let item_payload = serde_json::json!({
            "heads": crate::serialize_commit_heads(&heads),
            // FIXME: this should be incrementing per change
            "change_count_hint": 1_u64,
        });
        self.partition_store
            .record_member_item_change(&doc_id.to_string(), &item_payload)
            .await?;

        let heads_arc = Arc::<[automerge::ChangeHash]>::from(heads);
        self.change_manager.notify_doc_heads_changed(
            doc_id,
            Arc::clone(&heads_arc),
            origin.clone(),
        )?;
        if matches!(origin, BigRepoChangeOrigin::Local) {
            self.change_manager
                .notify_local_doc_heads_updated(doc_id, Arc::clone(&heads_arc))?;
        }
        for patch in patches {
            self.change_manager.notify_doc_changed(
                doc_id,
                Arc::new(patch),
                Arc::clone(&heads_arc),
                origin.clone(),
            )?;
        }

        Ok(())
    }
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
