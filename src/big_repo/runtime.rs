//! This is just a crappy reimpl of samod to get subduction working

use crate::interlude::*;

use crate::{
    handler::{boot_keyhive, BigRepoComposedHandler, BigRepoKeyhiveProtocol, BigRepoSubduction},
    keyhive_conn::BigRepoKeyhiveConnAdapter,
    keyhive_storage::BigRepoKeyhiveStorage,
    BigKeyhiveAuthority, BigKeyhiveHandle, BigRepoChangeOrigin, ConnFinishSignal, DocumentId,
    PeerId,
};
use keyhive_core::event::static_event::StaticEvent;
use subduction_keyhive::{KeyhiveConnection, KeyhivePeerId};

use future_form::FutureForm;
use futures::future::BoxFuture;
use keyhive_core::store::ciphertext::CiphertextStore;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::sedimentree::{Sedimentree, SedimentreeItem};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;
use subduction_core::subduction::request::FragmentRequested;

const DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);
type SharedPartitionStore = Arc<dyn big_sync::HostPartStore>;
type PendingKeyhiveSyncWaiters = HashMap<PeerId, Vec<(u64, oneshot::Sender<Res<()>>)>>;
type PendingDocSyncWaiters = HashMap<PeerId, Vec<(u64, oneshot::Sender<Result<(), SyncDocError>>)>>;

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum SyncDocError {
    /// Document not found
    NotFound,
    /// Document exists but peer lacks access
    Unauthorized,
    /// TransportError
    TransportError,
    /// IoError
    IoError(eyre::Report),
    /// Unexpected {0}
    Other(#[from] eyre::Report),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocLookup<T> {
    Missing,
    PendingMaterialization,
    Ready(T),
}

impl<T> DocLookup<T> {
    pub(crate) fn map_ready<U>(self, map: impl FnOnce(T) -> U) -> DocLookup<U> {
        match self {
            Self::Missing => DocLookup::Missing,
            Self::PendingMaterialization => DocLookup::PendingMaterialization,
            Self::Ready(value) => DocLookup::Ready(map(value)),
        }
    }

    pub fn is_some(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    pub fn expect(self, msg: &str) -> T {
        match self {
            Self::Ready(value) => value,
            Self::Missing | Self::PendingMaterialization => panic!("{msg}"),
        }
    }

    pub fn ok_or_else<E>(self, err: impl FnOnce() -> E) -> Result<T, E> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Missing | Self::PendingMaterialization => Err(err()),
        }
    }

    pub fn into_option(self) -> Option<T> {
        match self {
            Self::Ready(value) => Some(value),
            Self::Missing | Self::PendingMaterialization => None,
        }
    }

    pub fn is_pending_materialization(&self) -> bool {
        matches!(self, Self::PendingMaterialization)
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
pub struct LiveDocBundle {
    pub doc_id: DocumentId,
    #[educe(Debug(ignore))]
    pub doc: tokio::sync::Mutex<automerge::Automerge>,
    #[educe(Debug(ignore))]
    _lease: RuntimeDocLease,
}

impl LiveDocBundle {
    fn new(doc_id: DocumentId, doc: automerge::Automerge, lease: RuntimeDocLease) -> Self {
        Self {
            doc_id,
            doc: tokio::sync::Mutex::new(doc),
            _lease: lease,
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeDocLease {
    runtime: BigRepoRuntimeHandle,
    doc_id: DocumentId,
}

impl Drop for RuntimeDocLease {
    fn drop(&mut self) {
        self.runtime.release_doc_lease(self.doc_id);
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum CreateDocError {
    /// keyhive doc creation failed: {0}
    Keyhive(#[from] eyre::Report),
    /// storage put failed: {0}
    Put(#[from] PutDocError),
}

#[derive(thiserror::Error, displaydoc::Display, Debug)]
pub enum PutDocError {
    /// IdOccpuied {id}
    IdOccpuied { id: DocumentId },
    /// {0:}
    Other(#[from] eyre::Report),
}

enum RuntimeCmd {
    PutDoc {
        doc_id: DocumentId,
        initial_content: Box<automerge::Automerge>,
        resp: oneshot::Sender<Result<Arc<LiveDocBundle>, PutDocError>>,
    },
    GetDocHandle {
        doc_id: DocumentId,
        resp: oneshot::Sender<Res<DocLookup<Arc<LiveDocBundle>>>>,
    },
    ExportDocSave {
        doc_id: DocumentId,
        resp: oneshot::Sender<Res<DocLookup<Vec<u8>>>>,
    },
    CommitDelta {
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        resp: oneshot::Sender<Res<()>>,
    },
    OpenConnIroh {
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        resp: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    },
    AcceptConnIroh {
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        resp: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    },
    CloseConnIroh {
        peer_id: PeerId,
        resp: Option<oneshot::Sender<Res<()>>>,
    },
    SyncDocWithPeer {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<Duration>,
        resp: oneshot::Sender<Result<(), SyncDocError>>,
    },
    CancelDocSyncWaiter {
        doc_id: DocumentId,
        peer_id: PeerId,
        waiter_id: u64,
    },
    SyncKeyhiveWithPeer {
        peer_id: PeerId,
        waiter_id: u64,
        resp: oneshot::Sender<Res<()>>,
    },
    CancelKeyhiveSyncWaiter {
        peer_id: PeerId,
        waiter_id: u64,
    },
    ReleaseDocLease {
        doc_id: DocumentId,
    },
}

enum ConnTask {
    Sender,
    Listener,
}

enum RuntimeEvt {
    SyncSessionObserved {
        session: subduction_core::sync_session::SyncSession,
    },
    ConnEstablishedIroh {
        peer_id: PeerId,
        deets: RuntimePeerConnDeets,
    },
    ConnLostIroh {
        peer_id: PeerId,
        src_task: ConnTask,
        error: Option<subduction_iroh::error::RunError>,
    },
    KeyhiveSyncDone {
        peer_id: PeerId,
    },
    DocWorkerTransientFinished {
        doc_id: DocumentId,
    },
    DocWorkerHandleAcquired {
        bundle: Arc<LiveDocBundle>,
    },
    DocWorkerStopped {
        doc_id: DocumentId,
    },
    FatalWorkerError {
        doc_id: Option<DocumentId>,
        context: &'static str,
        error: String,
    },
}

struct RuntimePeerConnDeets {
    cancel_token: CancellationToken,
    end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    closed: Arc<AtomicBool>,
}

#[derive(Clone, educe::Educe)]
#[educe(Debug)]
pub struct BigRepoRuntimeHandle {
    #[educe(Debug(ignore))]
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
    #[educe(Debug(ignore))]
    keyhive: BigKeyhiveHandle,
    #[educe(Debug(ignore))]
    keyhive_storage: BigRepoKeyhiveStorage,
    #[educe(Debug(ignore))]
    keyhive_protocol: BigRepoKeyhiveProtocol,
    #[educe(Debug(ignore))]
    doc_sync_waiter_ids: Arc<AtomicU64>,
    #[educe(Debug(ignore))]
    keyhive_sync_waiter_ids: Arc<AtomicU64>,
}

impl BigRepoRuntimeHandle {
    pub async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
    ) -> Result<Arc<LiveDocBundle>, CreateDocError> {
        self.create_doc_with_parents(initial_content, Vec::new())
            .await
    }

    pub async fn create_doc_with_parents(
        &self,
        initial_content: automerge::Automerge,
        parents: Vec<BigKeyhiveAuthority>,
    ) -> Result<Arc<LiveDocBundle>, CreateDocError> {
        use nonempty::NonEmpty;
        let heads = initial_content.get_heads();
        // FIXME: do we use heads from subduction or from automerge???
        let content_heads = NonEmpty::from_vec(heads.iter().map(|h| h.0).collect())
            .ok_or_else(|| eyre::eyre!("automerge doc has no heads"))?;
        let doc_id = if parents.is_empty() {
            self.keyhive
                .create_doc(content_heads, &self.keyhive_storage)
                .await?
        } else {
            self.keyhive
                .create_doc_with_parents(parents, content_heads, &self.keyhive_storage)
                .await?
        };
        self.note_local_keyhive_changed().await?;
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::PutDoc {
                doc_id,
                initial_content: initial_content.into(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await
            .map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
            .map_err(CreateDocError::from)
    }

    pub async fn get_doc_handle(&self, doc_id: DocumentId) -> Res<Option<Arc<LiveDocBundle>>> {
        let out = self.get_doc_lookup(doc_id).await?;
        Ok(out.into_option())
    }

    pub async fn get_doc_lookup(&self, doc_id: DocumentId) -> Res<DocLookup<Arc<LiveDocBundle>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::GetDocHandle { doc_id, resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let out = rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;
        out
    }

    pub async fn export_doc_save(&self, doc_id: DocumentId) -> Res<DocLookup<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::ExportDocSave { doc_id, resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    pub async fn note_local_keyhive_changed(&self) -> Res<()> {
        self.keyhive_protocol
            .note_local_keyhive_changed()
            .await
            .wrap_err("keyhive local-change refresh failed")
            .map(|_| ())
    }

    pub async fn commit_delta(
        &self,
        doc_id: DocumentId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    pub async fn open_connection_iroh(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<(PeerId, Arc<AtomicBool>)> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::OpenConnIroh {
                endpoint,
                endpoint_addr,
                peer_id,
                end_signal_tx,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(skip_all)]
    pub async fn accept_connection_iroh(
        &self,
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
    ) -> Res<(PeerId, Arc<AtomicBool>)> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::AcceptConnIroh {
                conn,
                end_signal_tx,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    pub async fn close_peer_connection(&self, peer_id: PeerId) -> Res<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::CloseConnIroh {
                peer_id,
                resp: Some(tx),
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
    }

    #[tracing::instrument(
        skip_all,
        fields(%doc_id, %peer_id, timeout = ?timeout)
    )]
    pub async fn sync_doc_with_peer(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
        timeout: Option<Duration>,
    ) -> Result<(), SyncDocError> {
        let waiter_id = self.doc_sync_waiter_ids.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let Some(timeout) = timeout else {
            return rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(out) => out.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?,
            Err(_) => {
                self.cmd_tx
                    .send(RuntimeCmd::CancelDocSyncWaiter {
                        doc_id,
                        peer_id,
                        waiter_id,
                    })
                    .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
                Err(SyncDocError::IoError(ferr!("doc sync timed out")))
            }
        }
    }

    fn release_doc_lease(&self, doc_id: DocumentId) {
        if self
            .cmd_tx
            .send(RuntimeCmd::ReleaseDocLease { doc_id })
            .is_err()
        {
            //let bt = std::backtrace::Backtrace::capture();
            warn!(%doc_id, "runtime stopped before releasing doc lease");
        }
    }

    /// Initiate a keyhive protocol sync with the given peer.
    ///
    /// The call waits until BigRepo observes the completed sync round or the
    /// peer is disconnected/closed.
    pub async fn sync_keyhive_with_peer(
        &self,
        peer_id: PeerId,
        timeout: Option<std::time::Duration>,
    ) -> Res<()> {
        let waiter_id = self.keyhive_sync_waiter_ids.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(RuntimeCmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp: tx,
            })
            .is_err()
        {
            return Err(ferr!("runtime channel closed"));
        }
        let timeout = timeout.unwrap_or_else(|| utils_rs::scale_timeout(Duration::from_secs(5)));
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(res)) => res.wrap_err("keyhive sync request failed")?,
            Ok(Err(_)) => return Err(ferr!("sync keyhive response channel closed")),
            Err(_) => {
                self.cmd_tx
                    .send(RuntimeCmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id })
                    .map_err(|_| ferr!("runtime channel closed"))?;
                return Err(ferr!("keyhive sync timed out waiting for completion"));
            }
        }
        self.keyhive_protocol.ingest_from_storage().await?;
        self.keyhive_protocol.note_local_keyhive_changed().await?;
        Ok(())
    }

    pub async fn refresh_keyhive_cache(&self) -> Res<()> {
        self.keyhive_protocol
            .refresh_cache()
            .await
            .wrap_err("keyhive cache refresh failed")
    }
}

pub struct BigRepoRuntimeStopToken {
    cancel_token: CancellationToken,
    machine_loop_handle: tokio::task::JoinHandle<()>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl BigRepoRuntimeStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        match self.runtime_tasks.stop(Duration::from_secs(5)).await {
            Ok(()) => {}
            Err(utils_rs::AbortableJoinSetStopError::Timeout(_))
            | Err(utils_rs::AbortableJoinSetStopError::Aborted) => {}
            Err(err) => return Err(err.into()),
        }
        utils_rs::wait_on_handle_with_timeout(self.machine_loop_handle, Duration::from_secs(5))
            .await?;
        Ok(())
    }
}

type SubductionSedimentrees = Arc<
    subduction_core::collections::bounded_sharded_map::BoundedShardedMap<
        SedimentreeId,
        sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
        256,
    >,
>;
pub(crate) type BigRepoIrohTransport = subduction_core::transport::message::MessageTransport<
    subduction_iroh::transport::IrohTransport,
>;
pub(crate) type BigRepoPolicy = subduction_keyhive::policy::SubductionKeyhive<
    future_form::Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    keyhive_core::listener::no_listener::NoListener,
    rand_08::rngs::OsRng,
>;
pub trait BigRepoSubductionStorage:
    subduction_core::storage::traits::Storage<
        future_form::Sendable,
        Error: std::fmt::Display + Send + Sync + 'static,
    > + Clone
    + Send
    + Sync
    + std::fmt::Debug
    + 'static
{
}
impl<T> BigRepoSubductionStorage for T where
    T: subduction_core::storage::traits::Storage<
            future_form::Sendable,
            Error: std::fmt::Display + Send + Sync + 'static,
        > + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static
{
}

#[derive(Clone)]
struct BigRepoSyncSessionBridge {
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

impl subduction_core::sync_session::SyncSessionObserver for BigRepoSyncSessionBridge {
    fn on_sync_session(&self, session: subduction_core::sync_session::SyncSession) {
        self.evt_tx
            .send(RuntimeEvt::SyncSessionObserved { session })
            .expect(ERROR_CHANNEL);
    }
}
pub async fn spawn_big_repo_runtime<S>(
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    policy: Arc<BigRepoPolicy>,
    keyhive: BigKeyhiveHandle,
    keyhive_storage: BigRepoKeyhiveStorage,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<(BigRepoRuntimeHandle, BigRepoRuntimeStopToken)>
where
    S: BigRepoSubductionStorage,
{
    use subduction_core::subduction::Subduction;
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());
    let nonce_cache = Arc::new(subduction_core::nonce_cache::NonceCache::new(
        Duration::from_secs(60),
    ));
    let runtime_stop = CancellationToken::new();
    let runtime_tasks = Arc::new(utils_rs::AbortableJoinSet::new());

    let sedimentrees =
        Arc::new(subduction_core::collections::bounded_sharded_map::BoundedShardedMap::new());
    let connections = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let subscriptions = Arc::new(async_lock::Mutex::new(
        sedimentree_core::collections::Map::new(),
    ));
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<RuntimeCmd>();
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel::<RuntimeEvt>();
    let doc_sync_waiter_ids = Arc::new(AtomicU64::new(0));
    let keyhive_sync_waiter_ids = Arc::new(AtomicU64::new(0));
    let storage_for_reads = storage.clone();
    let sync_session_observer: Arc<
        dyn subduction_core::sync_session::SyncSessionObserver + Send + Sync,
    > = Arc::new(BigRepoSyncSessionBridge {
        evt_tx: evt_tx.clone(),
    });
    let storage_powerbox = subduction_core::storage::powerbox::StoragePowerbox::new(
        storage.clone(),
        Arc::clone(&policy),
    );
    let sync_handler = subduction_core::handler::sync::SyncHandler::new(
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox.clone(),
        sedimentree_core::depth::CountLeadingZeroBytes,
        TokioSpawn,
    );
    sync_handler.set_sync_session_observer(Arc::clone(&sync_session_observer));
    let send_counter = sync_handler.send_counter().clone();
    let sync_handler = Arc::new(sync_handler);

    let keyhive_sync_done_observer: Arc<dyn Fn(KeyhivePeerId) + Send + Sync> = {
        let evt_tx = evt_tx.clone();
        Arc::new(move |peer_id| {
            let peer_id = PeerId::new(*peer_id.verifying_key());
            evt_tx
                .send(RuntimeEvt::KeyhiveSyncDone { peer_id })
                .expect(ERROR_CHANNEL);
        })
    };
    // Boot keyhive protocol and handler
    let (keyhive_protocol, keyhive_handler) = boot_keyhive(
        &keyhive,
        keyhive_storage.clone(),
        Some(keyhive_sync_done_observer),
    )
    .await?;
    let composed_handler = Arc::new(BigRepoComposedHandler::new(sync_handler, keyhive_handler));

    let (subduction, listener, manager) = Subduction::new(
        composed_handler,
        None,
        signer,
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
        send_counter,
        subduction_core::nonce_cache::NonceCache::new(Duration::from_secs(60)),
        TimeoutTokio,
        // FIXME: parametrize this
        Duration::from_secs(30),
        sedimentree_core::depth::CountLeadingZeroBytes,
        TokioSpawn,
    );
    subduction.set_sync_session_observer(sync_session_observer);
    let subduction_handle: Arc<BigRepoSubduction<S>> = Arc::clone(&subduction);
    let big_sync_store_for_worker = Arc::clone(&big_sync_store);
    let keyhive_archive_id =
        subduction_keyhive::storage::StorageHash::new(*local_peer_id.as_bytes());
    let runtime_worker = BigRepoRuntimeWorker {
        keyhive: keyhive.clone(),
        keyhive_protocol: Arc::clone(&keyhive_protocol),
        keyhive_storage: keyhive_storage.clone(),
        subduction: subduction_handle,
        sedimentrees: Arc::clone(&sedimentrees),
        storage_for_reads,
        big_sync_store: big_sync_store_for_worker,
        change_manager,
        runtime_tasks: Arc::clone(&runtime_tasks),
        connect_signer,
        local_peer_id,
        nonce_cache,
        runtime_stop: runtime_stop.clone(),
        cmd_tx: cmd_tx.clone(),
        evt_tx: evt_tx.clone(),
        connected_peers: default(),
        doc_sync_waiter_ids: Arc::clone(&doc_sync_waiter_ids),
        keyhive_sync_waiter_ids: Arc::clone(&keyhive_sync_waiter_ids),
        pending_keyhive_syncs: default(),
        doc_workers: default(),
    };

    let peer_id = runtime_worker.local_peer_id;

    runtime_tasks
        .spawn({
            let stop = runtime_stop.clone();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        listener.await.unwrap();
                    })
                    .await;
            }
            .instrument(
                tracing::info_span!("BigRepoRuntime subduction listener", peer_id = %peer_id),
            )
        })
        .expect(ERROR_TOKIO);
    runtime_tasks
        .spawn({
            let stop = runtime_stop.clone();
            async move {
                let _ = stop
                    .run_until_cancelled(async move {
                        // NOTE: manager only returns abort signal on Subduction drop
                        manager.await.ok();
                    })
                    .await;
            }
            .instrument(
                tracing::info_span!("BigRepoRuntime subduction manager", peer_id = %peer_id),
            )
        })
        .expect(ERROR_TOKIO);
    runtime_tasks
        .spawn({
            let stop = runtime_stop.clone();
            let keyhive_protocol = Arc::clone(&keyhive_protocol);
            async move {
                let mut refresh_tick = tokio::time::interval(Duration::from_secs(2));
                let mut compact_tick = tokio::time::interval(Duration::from_secs(300));
                loop {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => break,
                        _ = refresh_tick.tick() => {
                            // Best-effort maintenance only: correctness and
                            // scheduling do not depend on this loop.
                            if let Err(e) = keyhive_protocol.refresh_cache().await {
                                tracing::warn!(error = %e, "keyhive cache refresh failed");
                            }
                        },
                        _ = compact_tick.tick() => {
                            // Best-effort maintenance only; do not make stop
                            // or forward progress depend on compaction.
                            if let Err(e) = keyhive_protocol.compact(keyhive_archive_id).await {
                                tracing::warn!(error = %e, "keyhive archive compact failed");
                            }
                        }
                    }
                }
            }
            .instrument(
                tracing::info_span!("BigRepoRuntime keyhive maintenance", peer_id = %peer_id),
            )
        })
        .expect(ERROR_TOKIO);

    let fut = {
        let runtime_stop = runtime_stop.clone();
        let mut runtime_worker = runtime_worker;
        async move {
            let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    biased;
                    _ = runtime_stop.cancelled() => break,
                    _ = janitor_tick.tick() => {
                        runtime_worker.handle_doc_worker_janitor_tick();
                    },
                    cmd = cmd_rx.recv() => {
                        let Some(cmd) = cmd else { break; };
                        runtime_worker.handle_cmd(cmd).await?;
                    },
                    evt = evt_rx.recv() => {
                        let Some(evt) = evt else { break; };
                        runtime_worker.handle_evt(evt).await?;
                    }
                }
            }
            eyre::Ok(())
        }
        .instrument(tracing::info_span!("BigRepoRuntime machine_loop", peer_id = %peer_id))
    };
    // the main loop is not on the abortable join set
    // to allow the children to die first before main does
    let machine_loop_handle = tokio::spawn(async move {
        fut.await.unwrap();
    });

    Ok((
        BigRepoRuntimeHandle {
            cmd_tx,
            keyhive: keyhive.clone(),
            keyhive_storage,
            keyhive_protocol: Arc::clone(&keyhive_protocol),
            doc_sync_waiter_ids: Arc::clone(&doc_sync_waiter_ids),
            keyhive_sync_waiter_ids: Arc::clone(&keyhive_sync_waiter_ids),
        },
        BigRepoRuntimeStopToken {
            cancel_token: runtime_stop,
            machine_loop_handle,
            runtime_tasks,
        },
    ))
}

struct BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    keyhive: BigKeyhiveHandle,
    keyhive_protocol: BigRepoKeyhiveProtocol,
    keyhive_storage: BigRepoKeyhiveStorage,
    subduction: Arc<BigRepoSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    connect_signer: subduction_crypto::signer::memory::MemorySigner,
    local_peer_id: subduction_core::peer::id::PeerId,
    nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    runtime_stop: CancellationToken,
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    connected_peers: Arc<tokio::sync::Mutex<HashMap<PeerId, RuntimePeerConnDeets>>>,
    doc_sync_waiter_ids: Arc<AtomicU64>,
    keyhive_sync_waiter_ids: Arc<AtomicU64>,
    pending_keyhive_syncs: PendingKeyhiveSyncWaiters,
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
}

impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    async fn handle_cmd(&mut self, cmd: RuntimeCmd) -> Res<()> {
        match cmd {
            RuntimeCmd::PutDoc {
                doc_id,
                initial_content,
                resp,
            } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                worker
                    .send(DocWorkerMsg::PutDoc {
                        initial_content,
                        resp,
                    })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::GetDocHandle { doc_id, resp } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                worker
                    .send(DocWorkerMsg::AcquireHandle { resp })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::ExportDocSave { doc_id, resp } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.transient_work += 1;
                }
                worker
                    .send(DocWorkerMsg::ExportDocSave { resp })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::CommitDelta {
                doc_id,
                commits,
                heads,
                patches,
                origin,
                resp,
            } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.transient_work += 1;
                }
                worker
                    .send(DocWorkerMsg::CommitDelta {
                        commits,
                        heads,
                        patches,
                        origin,
                        resp,
                    })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::OpenConnIroh {
                endpoint,
                endpoint_addr,
                peer_id,
                end_signal_tx,
                resp: done,
            } => {
                self.handle_open_conn_iroh(endpoint, endpoint_addr, peer_id, end_signal_tx, done)
                    .await
            }
            RuntimeCmd::AcceptConnIroh {
                conn,
                end_signal_tx,
                resp: done,
            } => {
                self.handle_accept_conn_iroh(conn, end_signal_tx, done)
                    .await;
            }
            RuntimeCmd::CloseConnIroh {
                peer_id,
                resp: done,
            } => {
                self.handle_close_iroh(peer_id, done).await;
            }
            RuntimeCmd::SyncDocWithPeer {
                doc_id,
                peer_id,
                waiter_id,
                timeout,
                resp: done,
            } => {
                let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
                if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                    entry.transient_work += 1;
                }
                worker
                    .send(DocWorkerMsg::SyncWithPeer {
                        peer_id,
                        waiter_id,
                        timeout,
                        done,
                    })
                    .expect(ERROR_ACTOR);
            }
            RuntimeCmd::CancelDocSyncWaiter {
                doc_id,
                peer_id,
                waiter_id,
            } => {
                if let Some(entry) = self.doc_workers.get(&doc_id) {
                    entry
                        .handle
                        .send(DocWorkerMsg::CancelSyncWithPeer {
                            peer_id,
                            waiter_id: Some(waiter_id),
                            reason: "doc sync timed out",
                        })
                        .expect(ERROR_ACTOR);
                }
            }
            RuntimeCmd::SyncKeyhiveWithPeer {
                peer_id,
                waiter_id,
                resp,
            } => {
                let kh_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
                tracing::debug!("SyncKeyhiveWithPeer: initiating sync");
                self.pending_keyhive_syncs
                    .entry(peer_id)
                    .or_default()
                    .push((waiter_id, resp));
                if let Err(err) = self.keyhive_protocol.ingest_from_storage().await {
                    if let Some(resp) = self.cancel_pending_keyhive_sync(peer_id, waiter_id) {
                        if let Err(send_err) = resp.send(Err(ferr!(
                            "keyhive ingest_from_storage before sync failed: {err}"
                        ))) {
                            warn!(error = ?send_err, "failed sending keyhive sync response");
                        }
                    }
                    return Ok(());
                }
                if let Err(err) = self.keyhive_protocol.note_local_keyhive_changed().await {
                    if let Some(resp) = self.cancel_pending_keyhive_sync(peer_id, waiter_id) {
                        if let Err(send_err) = resp.send(Err(ferr!(
                            "keyhive local-change refresh before sync failed: {err}"
                        ))) {
                            warn!(error = ?send_err, "failed sending keyhive sync response");
                        }
                    }
                    return Ok(());
                }
                match self
                    .keyhive_protocol
                    .initiate_sync_with_peer(&kh_peer_id)
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        if let Some(resp) = self.cancel_pending_keyhive_sync(peer_id, waiter_id) {
                            if let Err(send_err) = resp
                                .send(Err(ferr!("keyhive initiate_sync_with_peer failed: {err}")))
                            {
                                warn!(error = ?send_err, "failed sending keyhive sync response");
                            }
                        }
                    }
                }
            }
            RuntimeCmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id } => {
                let _ = self.cancel_pending_keyhive_sync(peer_id, waiter_id);
            }
            RuntimeCmd::ReleaseDocLease { doc_id } => self.handle_release_doc_lease(doc_id).await,
        }

        Ok(())
    }

    async fn handle_evt(&mut self, evt: RuntimeEvt) -> Res<()> {
        match evt {
            RuntimeEvt::SyncSessionObserved { session } => {
                self.handle_sync_session_observed(session).await;
            }
            RuntimeEvt::ConnEstablishedIroh {
                peer_id,
                deets: cancel_token,
            } => {
                self.handle_connection_established(peer_id, cancel_token)
                    .await;
            }
            RuntimeEvt::ConnLostIroh {
                peer_id,
                error,
                src_task,
            } => {
                self.handle_connection_lost(peer_id, error, src_task)
                    .await?;
            }
            RuntimeEvt::KeyhiveSyncDone { peer_id } => {
                self.handle_keyhive_sync_done(peer_id).await?;
            }
            RuntimeEvt::DocWorkerTransientFinished { doc_id } => {
                self.handle_doc_worker_transient_finished(doc_id);
            }
            RuntimeEvt::DocWorkerHandleAcquired { bundle } => {
                self.handle_doc_worker_handle_acquired(bundle);
            }
            RuntimeEvt::DocWorkerStopped { doc_id } => {
                self.handle_doc_worker_stopped(doc_id);
            }
            RuntimeEvt::FatalWorkerError {
                doc_id,
                context,
                error,
            } => {
                panic!("fatal runtime worker error doc={doc_id:?} context={context}: {error}");
            }
        }
        Ok(())
    }

    fn spawn_background<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let stop = self.runtime_stop.child_token();
        self.runtime_tasks
            .spawn(
                async move {
                    let _ = stop.run_until_cancelled(fut).await;
                }
                .instrument(tracing::info_span!("spawn_background")),
            )
            .expect(ERROR_TOKIO);
    }

    fn clear_doc_worker_eviction(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.eviction_deadline = None;
        }
    }

    fn schedule_doc_worker_eviction_if_idle(&mut self, doc_id: DocumentId) {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return;
        };
        if entry.local_handles > 0 || entry.transient_work > 0 {
            entry.eviction_deadline = None;
            return;
        }
        entry.eviction_deadline = Some(Instant::now() + DOC_WORKER_IDLE_TTL);
    }

    fn handle_doc_worker_janitor_tick(&mut self) {
        let now = Instant::now();
        for entry in self.doc_workers.values() {
            if entry
                .eviction_deadline
                .is_some_and(|deadline| deadline <= now)
            {
                entry.stop.cancel();
            }
        }
    }

    fn handle_doc_worker_transient_finished(&mut self, doc_id: DocumentId) {
        let entry = self.doc_workers.get_mut(&doc_id).unwrap_or_else(|| {
            panic!("transient work finished for unknown doc worker: {doc_id:?}")
        });
        assert!(
            entry.transient_work > 0,
            "transient work underflow for doc worker: {doc_id:?}"
        );
        entry.transient_work -= 1;
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    fn handle_doc_worker_handle_acquired(&mut self, bundle: Arc<LiveDocBundle>) {
        let doc_id = bundle.doc_id;
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.local_handles += 1;
            entry.eviction_deadline = None;
        }
    }

    fn handle_doc_worker_stopped(&mut self, doc_id: DocumentId) {
        self.doc_workers.remove(&doc_id);
    }

    fn complete_pending_keyhive_syncs(&mut self, peer_id: PeerId) {
        if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
            for (_, waiter) in waiters {
                waiter
                    .send(Ok(()))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
    }

    fn cancel_pending_keyhive_syncs(&mut self, peer_id: PeerId, reason: &'static str) {
        if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
            for (_, waiter) in waiters {
                waiter
                    .send(Err(ferr!("{reason}")))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
    }

    async fn cancel_pending_doc_syncs(&mut self, peer_id: PeerId, reason: &'static str) -> Res<()> {
        let workers = self
            .doc_workers
            .iter()
            .filter_map(|(doc_id, entry)| {
                (entry.transient_work > 0).then_some((*doc_id, entry.handle.clone()))
            })
            .collect::<Vec<_>>();
        for (_, worker) in workers {
            worker
                .send(DocWorkerMsg::CancelSyncWithPeer {
                    peer_id,
                    waiter_id: None,
                    reason,
                })
                .wrap_err(ERROR_ACTOR)?;
        }
        Ok(())
    }

    fn cancel_pending_keyhive_sync(
        &mut self,
        peer_id: PeerId,
        waiter_id: u64,
    ) -> Option<oneshot::Sender<Res<()>>> {
        let mut remove_peer = false;
        let waiter = {
            let waiters = self.pending_keyhive_syncs.get_mut(&peer_id)?;
            let pos = waiters
                .iter()
                .position(|(pending_id, _)| *pending_id == waiter_id)?;
            let (_, waiter) = waiters.remove(pos);
            if waiters.is_empty() {
                remove_peer = true;
            }
            waiter
        };
        if remove_peer {
            self.pending_keyhive_syncs.remove(&peer_id);
        }
        Some(waiter)
    }

    #[tracing::instrument(skip_all,fields(%doc_id))]
    fn spawn_doc_worker(&mut self, doc_id: DocumentId) -> Res<()> {
        if self.doc_workers.contains_key(&doc_id) {
            self.clear_doc_worker_eviction(doc_id);
            return Ok(());
        }
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
        let cancel_token = self.runtime_stop.child_token();
        let worker_cancel = cancel_token.clone();
        let runtime_evt_tx = self.evt_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let doc_id_subduction = SedimentreeId::new(doc_id.into_bytes());
        let mut worker = DocWorker {
            doc_id_subduction,
            doc_id,
            state: DocWorkerDocState::Unloaded,
            pending_fragment_requests: BTreeSet::new(),
            pending_sync_jobs: HashMap::new(),
            applied_sync_generations: HashMap::new(),
            subduction: Arc::clone(&self.subduction),
            sedimentrees: Arc::clone(&self.sedimentrees),
            storage_for_reads: self.storage_for_reads.clone(),
            keyhive_storage: self.keyhive_storage.clone(),
            big_sync_store: Arc::clone(&self.big_sync_store),
            change_manager: Arc::clone(&self.change_manager),
            runtime_handle: BigRepoRuntimeHandle {
                cmd_tx: self.cmd_tx.clone(),
                keyhive: self.keyhive.clone(),
                keyhive_storage: self.keyhive_storage.clone(),
                keyhive_protocol: Arc::clone(&self.keyhive_protocol),
                doc_sync_waiter_ids: Arc::clone(&self.doc_sync_waiter_ids),
                keyhive_sync_waiter_ids: Arc::clone(&self.keyhive_sync_waiter_ids),
            },
            runtime_evt_tx,
            // shutdown: worker_cancel.clone(),
        };
        runtime_tasks
            .spawn(
                async move {
                    let res = worker_cancel
                        .run_until_cancelled(async {
                            loop {
                                let Some(msg) = msg_rx.recv().await else {
                                    break;
                                };
                                worker.handle_msg(msg).await?;
                            }
                            eyre::Ok(())
                        })
                        .await;
                    // FIXME: why two errors instead of stopped with error variant
                    if let Some(Err(err)) = res {
                        worker
                            .runtime_evt_tx
                            .send(RuntimeEvt::FatalWorkerError {
                                doc_id: Some(doc_id),
                                context: "doc worker",
                                error: format!("{err:?}"),
                            })
                            .inspect_err(|_| warn!(ERROR_CHANNEL))
                            .ok();
                    }
                    worker
                        .runtime_evt_tx
                        .send(RuntimeEvt::DocWorkerStopped { doc_id })
                        .inspect_err(|_| warn!(ERROR_CHANNEL))
                        .ok();
                }
                .in_current_span(),
            )
            .expect(ERROR_TOKIO);
        self.doc_workers.insert(
            doc_id,
            DocWorkerEntry {
                handle: DocWorkerHandle { msg_tx },
                stop: DocWorkerStopToken { cancel_token },
                local_handles: 0,
                transient_work: 0,
                eviction_deadline: Some(Instant::now() + DOC_WORKER_IDLE_TTL),
            },
        );
        Ok(())
    }

    fn doc_worker_handle(&mut self, doc_id: DocumentId) -> Res<DocWorkerHandle> {
        self.spawn_doc_worker(doc_id)?;
        let entry = self
            .doc_workers
            .get_mut(&doc_id)
            .ok_or_eyre("doc worker missing after spawn")?;
        entry.eviction_deadline = None;
        Ok(entry.handle.clone())
    }

    async fn handle_release_doc_lease(&mut self, doc_id: DocumentId) {
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            assert!(
                entry.local_handles > 0,
                "doc lease underflow for doc worker: {doc_id:?}"
            );
            entry.local_handles -= 1;
            entry
                .handle
                .send(DocWorkerMsg::ReleaseHandleLease)
                .expect(ERROR_ACTOR);
        }
        self.schedule_doc_worker_eviction_if_idle(doc_id);
    }

    #[tracing::instrument(skip_all)]
    async fn handle_sync_session_observed(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) {
        if session.received_commit_ids.is_empty() && session.received_fragment_ids.is_empty() {
            return;
        }
        let doc_id = DocumentId::new(*session.sedimentree_id.as_bytes());
        debug!(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
            sent_commit_ids = session.sent_commit_ids.len(),
            sent_fragment_ids = session.sent_fragment_ids.len(),
            "observed sync session"
        );
        let worker = self.doc_worker_handle(doc_id).expect(ERROR_ACTOR);
        if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
            entry.transient_work += 1;
        }
        worker
            .send(DocWorkerMsg::ApplySyncSession { session })
            .expect(ERROR_ACTOR);
    }

    async fn handle_keyhive_sync_done(&mut self, peer_id: PeerId) -> Res<()> {
        if let Err(err) = self
            .keyhive
            .save_storage_archive(&self.keyhive_storage)
            .await
        {
            if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
                for (_, waiter) in waiters {
                    waiter
                        .send(Err(ferr!("keyhive archive save after sync failed: {err}")))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                }
            }
            return Err(err);
        }
        self.complete_pending_keyhive_syncs(peer_id);
        Ok(())
    }
}

// connections support
impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    #[tracing::instrument(skip_all)]
    async fn handle_open_conn_iroh(
        &self,
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        peer_id: PeerId,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        done: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    ) {
        let subduction: Arc<BigRepoSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let evt_tx = self.evt_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let connected_peers = Arc::clone(&self.connected_peers);
        let cancel_token = self.runtime_stop.child_token();
        let kh_proto = Arc::clone(&self.keyhive_protocol);
        let fut = async move {
            if let Some(deets) = connected_peers.lock().await.get(&peer_id) {
                return Ok((peer_id, Arc::clone(&deets.closed)));
            }
            let connect = connect_outgoing(endpoint, endpoint_addr, &connect_signer).await?;
            let peer_id = PeerId::new(*connect.authenticated.peer_id().as_bytes());
            for (fut, src_task) in [
                (connect.listener_task, ConnTask::Listener),
                (connect.sender_task, ConnTask::Sender),
            ] {
                let fut_wrapped = {
                    let cancel_token = cancel_token.clone();
                    let evt_tx = evt_tx.clone();
                    async move {
                        let Some(res) = cancel_token.run_until_cancelled(fut).await else {
                            // cancellation token lit
                            return;
                        };
                        match res {
                            Ok(()) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: None,
                                        src_task,
                                    })
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                            Err(err) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: Some(err),
                                        src_task,
                                    })
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                        }
                    }
                };
                runtime_tasks.spawn(fut_wrapped).expect(ERROR_TOKIO);
            }
            let auth = connect.authenticated.clone();
            subduction
                .add_connection(connect.authenticated)
                .await
                .map_err(|err| {
                    cancel_token.cancel();
                    ferr!("failed subduction add_connection: {err}")
                })?;
            let adapter = BigRepoKeyhiveConnAdapter::new(auth);
            kh_proto.add_peer(adapter.peer_id(), adapter).await;
            let closed = Arc::new(AtomicBool::new(false));
            evt_tx
                .send(RuntimeEvt::ConnEstablishedIroh {
                    peer_id,
                    deets: RuntimePeerConnDeets {
                        cancel_token,
                        end_signal_tx,
                        closed: Arc::clone(&closed),
                    },
                })
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            Ok((peer_id, closed))
        };
        self.spawn_background(async move {
            done.send(fut.await)
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
        });
    }

    #[tracing::instrument(skip_all)]
    async fn handle_accept_conn_iroh(
        &self,
        conn: iroh::endpoint::Connection,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<ConnFinishSignal>>,
        done: oneshot::Sender<Res<(PeerId, Arc<AtomicBool>)>>,
    ) {
        let subduction: Arc<BigRepoSubduction<S>> = Arc::clone(&self.subduction);
        let connect_signer = self.connect_signer.clone();
        let nonce_cache = Arc::clone(&self.nonce_cache);
        let local_peer_id = self.local_peer_id;
        let evt_tx = self.evt_tx.clone();
        let cancel_token = self.runtime_stop.child_token();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);
        let kh_proto = Arc::clone(&self.keyhive_protocol);
        // let connected_peers = Arc::clone(&self.connected_peers);
        let fut = async move {
            // do handshake
            let accepted =
                accept_incoming(conn, &connect_signer, nonce_cache.as_ref(), local_peer_id).await?;
            let peer_id = PeerId::new(*accepted.authenticated.peer_id().as_bytes());

            // WARN: should we block incoming when there's already a connection?
            // if let Some(deets) = connected_peers.lock().await.get(&peer_id) {
            //     return Ok((peer_id, deets.closed.clone()));
            // }

            for (fut, src_task) in [
                (accepted.listener_task, ConnTask::Listener),
                (accepted.sender_task, ConnTask::Sender),
            ] {
                let fut_wrapped = {
                    let cancel_token = cancel_token.clone();
                    let evt_tx = evt_tx.clone();
                    async move {
                        let Some(res) = cancel_token.run_until_cancelled(fut).await else {
                            // cancellation token lit
                            return;
                        };
                        match res {
                            Ok(()) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: None,
                                        src_task,
                                    })
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                            Err(err) => {
                                evt_tx
                                    .send(RuntimeEvt::ConnLostIroh {
                                        peer_id,
                                        error: Some(err),
                                        src_task,
                                    })
                                    .inspect_err(|_| warn!(ERROR_CALLER))
                                    .ok();
                            }
                        }
                    }
                };
                runtime_tasks.spawn(fut_wrapped).expect(ERROR_TOKIO);
            }
            let auth = accepted.authenticated.clone();
            subduction
                .add_connection(accepted.authenticated)
                .await
                .inspect_err(|_| {
                    cancel_token.cancel();
                })
                .wrap_err("failed subduction add_connection")?;
            let adapter = BigRepoKeyhiveConnAdapter::new(auth);
            kh_proto.add_peer(adapter.peer_id(), adapter).await;
            let closed = Arc::new(AtomicBool::new(false));
            evt_tx
                .send(RuntimeEvt::ConnEstablishedIroh {
                    peer_id,
                    deets: RuntimePeerConnDeets {
                        cancel_token,
                        end_signal_tx,
                        closed: Arc::clone(&closed),
                    },
                })
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            Ok((peer_id, closed))
        };
        self.spawn_background(async move {
            done.send(fut.await)
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
        });
    }

    #[tracing::instrument(skip_all)]
    async fn handle_close_iroh(&mut self, peer_id: PeerId, done: Option<oneshot::Sender<Res<()>>>) {
        let subduction: Arc<BigRepoSubduction<S>> = Arc::clone(&self.subduction);
        let connected_peers = Arc::clone(&self.connected_peers);
        self.keyhive_protocol
            .remove_peer(&KeyhivePeerId::from_bytes(*peer_id.as_bytes()))
            .await;
        self.cancel_pending_keyhive_syncs(peer_id, "keyhive peer closed");
        self.cancel_pending_doc_syncs(peer_id, "doc sync peer closed")
            .await
            .expect(ERROR_ACTOR);
        self.spawn_background(async move {
            let out = async {
                let deets = connected_peers.lock().await.remove(&peer_id);
                if let Some(deets) = deets {
                    deets.closed.store(true, Ordering::SeqCst);
                    deets.cancel_token.cancel();
                    let remote_peer_id =
                        subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
                    subduction
                        .disconnect_from_peer(&remote_peer_id)
                        .await
                        .map_err(|err| ferr!("failed subduction disconnect_from_peer: {err}"))?;
                }
                Ok(())
            }
            .await;
            if let Some(done) = done {
                done.send(out).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
        });
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    async fn handle_connection_lost(
        &mut self,
        peer_id: PeerId,
        err: Option<subduction_iroh::error::RunError>,
        _task: ConnTask,
    ) -> Res<()> {
        let deets = self.connected_peers.lock().await.remove(&peer_id);
        let keyhive_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
        self.keyhive_protocol.remove_peer(&keyhive_peer_id).await;
        self.cancel_pending_keyhive_syncs(peer_id, "keyhive connection lost");
        self.cancel_pending_doc_syncs(peer_id, "doc sync connection lost")
            .await?;
        if let Some(deets) = deets {
            deets.closed.store(true, Ordering::SeqCst);
            deets.cancel_token.cancel();
            let remote_peer_id = subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
            self.subduction
                .disconnect_from_peer(&remote_peer_id)
                .await
                .wrap_err("error on disconnect")?;
            if let Some(tx) = deets.end_signal_tx {
                tx.send(ConnFinishSignal {
                    peer_id,
                    err: err.map(|err| ferr!("connection task error: {err}")),
                })
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    async fn handle_connection_established(&self, peer_id: PeerId, deets: RuntimePeerConnDeets) {
        self.keyhive_protocol
            .clear_syncpoint_for_peer(&KeyhivePeerId::from_bytes(*peer_id.as_bytes()))
            .await;
        let mut connected_peers = self.connected_peers.lock().await;
        if let Some(previous) = connected_peers.insert(peer_id, deets) {
            previous.closed.store(true, Ordering::SeqCst);
            previous.cancel_token.cancel();
            if let Some(tx) = previous.end_signal_tx {
                tx.send(ConnFinishSignal {
                    peer_id,
                    err: Some(ferr!(
                        "connection replaced by a newer connection for the same peer"
                    )),
                })
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
            }
        }
    }
}

#[derive(Clone)]
struct DocWorkerHandle {
    msg_tx: mpsc::UnboundedSender<DocWorkerMsg>,
}

impl DocWorkerHandle {
    fn send(&self, msg: DocWorkerMsg) -> Res<()> {
        self.msg_tx.send(msg).map_err(|_| eyre::eyre!(ERROR_ACTOR))
    }
}

struct DocWorkerStopToken {
    cancel_token: CancellationToken,
}

impl DocWorkerStopToken {
    fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

struct DocWorkerEntry {
    handle: DocWorkerHandle,
    stop: DocWorkerStopToken,
    local_handles: usize,
    transient_work: usize,
    eviction_deadline: Option<Instant>,
}

enum DocWorkerMsg {
    PutDoc {
        initial_content: Box<automerge::Automerge>,
        resp: oneshot::Sender<Result<Arc<LiveDocBundle>, PutDocError>>,
    },
    AcquireHandle {
        resp: oneshot::Sender<Res<DocLookup<Arc<LiveDocBundle>>>>,
    },
    ExportDocSave {
        resp: oneshot::Sender<Res<DocLookup<Vec<u8>>>>,
    },
    CommitDelta {
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
        resp: oneshot::Sender<Res<()>>,
    },
    ApplySyncSession {
        session: subduction_core::sync_session::SyncSession,
    },
    SyncWithPeer {
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<Duration>,
        done: oneshot::Sender<Result<(), SyncDocError>>,
    },
    CancelSyncWithPeer {
        peer_id: PeerId,
        waiter_id: Option<u64>,
        reason: &'static str,
    },
    ReleaseHandleLease,
}

struct DocWorker<S>
where
    S: BigRepoSubductionStorage,
{
    doc_id: DocumentId,
    doc_id_subduction: SedimentreeId,
    state: DocWorkerDocState,
    pending_fragment_requests: BTreeSet<FragmentRequested>,
    pending_sync_jobs: PendingDocSyncWaiters,
    applied_sync_generations: HashMap<PeerId, u64>,
    subduction: Arc<BigRepoSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    keyhive_storage: BigRepoKeyhiveStorage,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_handle: BigRepoRuntimeHandle,
    runtime_evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

enum DocWorkerDocState {
    Unloaded,
    Transient(Box<automerge::Automerge>),
    Live(std::sync::Weak<LiveDocBundle>),
    PendingMaterialization,
}

impl<S> DocWorker<S>
where
    S: BigRepoSubductionStorage,
{
    async fn handle_msg(&mut self, msg: DocWorkerMsg) -> Res<()> {
        match msg {
            DocWorkerMsg::PutDoc {
                initial_content,
                resp: done,
            } => {
                let res = self.handle_put_doc(initial_content).await;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::AcquireHandle { resp: done } => {
                let res = self.handle_acquire_handle().await;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::ExportDocSave { resp: done } => {
                let res = self.handle_export_doc_save().await;
                self.finish_transient_work()?;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::CommitDelta {
                commits,
                heads,
                patches,
                origin,
                resp: done,
            } => {
                let res = self
                    .handle_commit_delta(commits, heads, patches, origin)
                    .await;
                self.finish_transient_work()?;
                done.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            DocWorkerMsg::ApplySyncSession { session } => {
                let peer_id = PeerId::new(*session.peer_id.as_bytes());
                self.handle_apply_sync_session(session).await?;
                *self.applied_sync_generations.entry(peer_id).or_default() += 1;
                self.finish_transient_work()?;
                if let Some(waiters) = self.pending_sync_jobs.remove(&peer_id) {
                    // Each pending sync waiter accounts for the matching
                    // `RuntimeCmd::SyncDocWithPeer` increment.
                    for (_, waiter) in waiters {
                        self.finish_transient_work()?;
                        waiter
                            .send(Ok(()))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                    }
                }
            }
            DocWorkerMsg::SyncWithPeer {
                peer_id,
                waiter_id,
                timeout,
                done,
            } => {
                self.handle_sync_with_peer(peer_id, waiter_id, timeout, done)
                    .await?;
            }
            DocWorkerMsg::CancelSyncWithPeer {
                peer_id,
                waiter_id,
                reason,
            } => {
                self.handle_cancel_sync_with_peer(peer_id, waiter_id, reason)
                    .await?;
            }
            DocWorkerMsg::ReleaseHandleLease => {}
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_put_doc(
        &mut self,
        doc: Box<automerge::Automerge>,
    ) -> Result<Arc<LiveDocBundle>, PutDocError> {
        if !matches!(self.state, DocWorkerDocState::Unloaded) {
            return Err(PutDocError::IdOccpuied { id: self.doc_id });
        }
        // FIXME: thsi is costly just for occupancy check
        // maybe subduction has better APIs for now?
        // storage.contains_sedimentree_id??
        match load_doc_snapshot(
            &self.sedimentrees,
            &self.storage_for_reads,
            &self.runtime_handle.keyhive,
            self.doc_id,
        )
        .await?
        {
            DocLookup::Ready(_) | DocLookup::PendingMaterialization => {
                return Err(PutDocError::IdOccpuied { id: self.doc_id });
            }
            DocLookup::Missing => {}
        }
        let sedimentree_id: SedimentreeId = self.doc_id_subduction;
        let ingested = ingest_automerge(&doc, sedimentree_id);

        // Encrypt blobs via keyhive before storage
        let (sedimentree, blobs, update_ops) =
            encrypt_ingested_blobs(&ingested, &self.runtime_handle.keyhive, sedimentree_id)
                .await
                .map_err(|e| PutDocError::Other(ferr!("encryption failed: {e}")))?;

        for update_op in update_ops {
            persist_cgka_update_op(&self.keyhive_storage, update_op).await?;
        }
        self.subduction
            .store_sedimentree(sedimentree_id, sedimentree, blobs)
            .await
            .map_err(|err| ferr!("failed store_sedimentree: {err}"))?;
        for entry in &ingested.fragment_entries {
            record_keyhive_content_frontier(
                &self.runtime_handle.keyhive,
                sedimentree_id,
                entry.head.as_bytes().to_vec(),
                entry
                    .boundary
                    .iter()
                    .map(|content_ref| content_ref.as_bytes().to_vec())
                    .collect(),
            )
            .await?;
        }
        for entry in &ingested.loose_entries {
            record_keyhive_content_frontier(
                &self.runtime_handle.keyhive,
                sedimentree_id,
                entry.head.as_bytes().to_vec(),
                entry
                    .parents
                    .iter()
                    .map(|content_ref| content_ref.as_bytes().to_vec())
                    .collect(),
            )
            .await?;
        }
        let heads = sedimentree_heads_payload(&ingested.sedimentree);
        let item_payload = serde_json::json!({
            "heads": am_utils_rs::serialize_commit_heads(&heads),
        });
        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            *doc,
            RuntimeDocLease {
                runtime: self.runtime_handle.clone(),
                doc_id: self.doc_id,
            },
        ));
        self.big_sync_store
            .set_obj_payload(self.doc_id, item_payload)
            .await?;
        self.change_manager
            .notify_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.change_manager
            .notify_local_doc_created(self.doc_id, Arc::clone(&heads))?;
        self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
        self.runtime_evt_tx
            .send(RuntimeEvt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            })
            .expect(ERROR_CHANNEL);
        self.runtime_handle.note_local_keyhive_changed().await?;
        Ok(bundle)
    }

    async fn handle_acquire_handle(&mut self) -> Res<DocLookup<Arc<LiveDocBundle>>> {
        if let DocWorkerDocState::Live(bundle) = &self.state {
            if let Some(bundle) = bundle.upgrade() {
                self.runtime_evt_tx
                    .send(RuntimeEvt::DocWorkerHandleAcquired {
                        bundle: Arc::clone(&bundle),
                    })
                    .expect(ERROR_CHANNEL);
                return Ok(DocLookup::Ready(Arc::clone(&bundle)));
            }
            self.state = DocWorkerDocState::Unloaded;
        }
        let Some(doc) = self.take_or_load_transient_doc().await?.into_option() else {
            return Ok(match self.state {
                DocWorkerDocState::PendingMaterialization => DocLookup::PendingMaterialization,
                _ => DocLookup::Missing,
            });
        };
        let bundle = Arc::new(LiveDocBundle::new(
            self.doc_id,
            doc,
            RuntimeDocLease {
                runtime: self.runtime_handle.clone(),
                doc_id: self.doc_id,
            },
        ));
        self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
        self.runtime_evt_tx
            .send(RuntimeEvt::DocWorkerHandleAcquired {
                bundle: Arc::clone(&bundle),
            })
            .expect(ERROR_CHANNEL);
        Ok(DocLookup::Ready(bundle))
    }

    #[tracing::instrument(skip_all)]
    async fn handle_export_doc_save(&mut self) -> Res<DocLookup<Vec<u8>>> {
        match &self.state {
            DocWorkerDocState::Live(bundle) => match bundle.upgrade() {
                Some(bundle) => {
                    let doc = bundle.doc.lock().await;
                    Ok(DocLookup::Ready(doc.save()))
                }
                None => {
                    let was_pending =
                        matches!(self.state, DocWorkerDocState::PendingMaterialization);
                    match load_doc_snapshot(
                        &self.sedimentrees,
                        &self.storage_for_reads,
                        &self.runtime_handle.keyhive,
                        self.doc_id,
                    )
                    .await?
                    {
                        DocLookup::Ready(doc) => {
                            self.mark_materialization_ready(
                                was_pending,
                                Arc::from(doc.get_heads()),
                            )?;
                            let save = doc.save();
                            self.state = DocWorkerDocState::Transient(doc.into());
                            Ok(DocLookup::Ready(save))
                        }
                        DocLookup::PendingMaterialization => {
                            self.mark_materialization_pending(was_pending)?;
                            Ok(DocLookup::PendingMaterialization)
                        }
                        DocLookup::Missing => Ok(DocLookup::Missing),
                    }
                }
            },
            DocWorkerDocState::Transient(doc) => Ok(DocLookup::Ready(doc.save())),
            DocWorkerDocState::PendingMaterialization => {
                let was_pending = true;
                match load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    &self.runtime_handle.keyhive,
                    self.doc_id,
                )
                .await?
                {
                    DocLookup::Ready(doc) => {
                        self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                        let save = doc.save();
                        self.state = DocWorkerDocState::Transient(doc.into());
                        Ok(DocLookup::Ready(save))
                    }
                    DocLookup::PendingMaterialization => {
                        self.mark_materialization_pending(true)?;
                        Ok(DocLookup::PendingMaterialization)
                    }
                    DocLookup::Missing => Ok(DocLookup::Missing),
                }
            }
            DocWorkerDocState::Unloaded => {
                let was_pending = false;
                match load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    &self.runtime_handle.keyhive,
                    self.doc_id,
                )
                .await?
                {
                    DocLookup::Ready(doc) => {
                        self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                        let save = doc.save();
                        self.state = DocWorkerDocState::Transient(doc.into());
                        Ok(DocLookup::Ready(save))
                    }
                    DocLookup::PendingMaterialization => {
                        self.mark_materialization_pending(false)?;
                        Ok(DocLookup::PendingMaterialization)
                    }
                    DocLookup::Missing => Ok(DocLookup::Missing),
                }
            }
        }
    }

    async fn handle_commit_delta(
        &mut self,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Vec<u8>)>,
        heads: Vec<automerge::ChangeHash>,
        patches: Vec<automerge::Patch>,
        origin: BigRepoChangeOrigin,
    ) -> Res<()> {
        let sedimentree_id: SedimentreeId = self.doc_id_subduction;
        // Track content_ref -> SymmetricKey within this batch for the
        // application-level predecessor key chain during encrypt.
        let mut batch_keys: std::collections::HashMap<
            CommitId,
            keyhive_crypto::symmetric_key::SymmetricKey,
        > = std::collections::HashMap::new();
        for (head, parents, blob) in commits {
            let (encrypted_blob, app_key, update_op) = encrypt_loose_commit_with_update_op(
                &self.runtime_handle.keyhive,
                sedimentree_id,
                head,
                &parents,
                &blob,
                &batch_keys,
            )
            .await
            .map_err(|e| PutDocError::Other(ferr!("encryption failed: {e}")))?;
            if let Some(update_op) = update_op {
                persist_cgka_update_op(&self.keyhive_storage, update_op).await?;
            }
            batch_keys.insert(head, app_key);
            let pred_refs: Vec<Vec<u8>> = parents
                .iter()
                .map(|content_ref| content_ref.as_bytes().to_vec())
                .collect();
            let maybe_request = self
                .subduction
                .store_commit(sedimentree_id, head, parents, encrypted_blob)
                .await
                .map_err(|err| ferr!("failed store_commit: {err}"))?;
            record_keyhive_content_frontier(
                &self.runtime_handle.keyhive,
                sedimentree_id,
                head.as_bytes().to_vec(),
                pred_refs,
            )
            .await?;
            if let Some(request) = maybe_request {
                self.pending_fragment_requests.insert(request);
            }
        }
        commit_delta_bookkeep(
            &self.big_sync_store,
            &self.change_manager,
            self.doc_id,
            heads,
            patches,
            origin,
        )
        .await?;
        self.process_pending_fragment_requests().await?;
        self.runtime_handle.note_local_keyhive_changed().await?;
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %session.peer_id,
            kind = ?session.kind,
            received_commit_ids = session.received_commit_ids.len(),
            received_fragment_ids = session.received_fragment_ids.len(),
            is_unloaded = matches!(self.state, DocWorkerDocState::Unloaded),
            is_transient = matches!(self.state, DocWorkerDocState::Transient(_)),
            is_live = matches!(self.state, DocWorkerDocState::Live(_)),
        )
    )]
    // FIXME: decryption logic here seems hacky
    async fn handle_apply_sync_session(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) -> Res<()> {
        let _peer_id = PeerId::new(*session.peer_id.as_bytes());
        let was_pending = matches!(self.state, DocWorkerDocState::PendingMaterialization);
        // FIXME: enabling this quick exit breaks things
        // if session.received_commit_ids.is_empty() && session.received_fragment_ids.is_empty() {
        //     return Ok(());
        // }
        use keyhive_core::crypto::envelope::Envelope;

        let keyhive = self.runtime_handle.keyhive.clone_keyhive();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(self.doc_id_subduction.as_bytes())
            .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
        let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
            keyhive_core::principal::identifier::Identifier::from(vk),
        );
        let kh_doc = keyhive.get_document(kh_doc_id).await;

        let tree = match self.sedimentrees.get_cloned(&session.sedimentree_id).await {
            Some(tree) => tree,
            None => {
                let loose_commits = <S as subduction_core::storage::traits::Storage<
                    future_form::Sendable,
                >>::load_loose_commits(
                    &self.storage_for_reads, session.sedimentree_id
                )
                .await?
                .into_iter()
                .collect::<Vec<_>>();
                let fragments = <S as subduction_core::storage::traits::Storage<
                    future_form::Sendable,
                >>::load_fragments(
                    &self.storage_for_reads, session.sedimentree_id
                )
                .await?
                .into_iter()
                .collect::<Vec<_>>();
                sedimentree_core::sedimentree::minimized::MinimizedSedimentree::new(
                    Sedimentree::new(
                        fragments
                            .iter()
                            .map(|verified| verified.payload().clone())
                            .collect(),
                        loose_commits
                            .iter()
                            .map(|verified| verified.payload().clone())
                            .collect(),
                    ),
                )
            }
        };
        let order = tree
            .topsorted_blob_order()
            .map_err(|err| ferr!("failed ordering sync session blobs: {err}"))?;
        let tree_fragments: Vec<_> = tree.fragments().collect();
        let tree_loose: Vec<_> = tree.loose_commits().collect();
        let received_refs = session
            .received_commit_ids
            .iter()
            .map(|commit_id| commit_id.as_bytes().to_vec())
            .chain(
                session
                    .received_fragment_ids
                    .iter()
                    .map(|fragment_id| fragment_id.as_bytes().to_vec()),
            )
            .collect::<std::collections::HashSet<_>>();

        let mut materialization_pending = false;
        let blobs = {
            match kh_doc {
                Some(kh_doc) => {
                    let ciphertext_store = {
                        let store = BigRepoCiphertextStore::new(self.storage_for_reads.clone());
                        for item in &order {
                            match item {
                                SedimentreeItem::Fragment(ii) => {
                                    store
                                        .record_locator(
                                            tree_fragments[*ii].head().as_bytes().to_vec(),
                                            session.sedimentree_id,
                                            BigRepoCiphertextKind::Fragment,
                                        )
                                        .await?;
                                }
                                SedimentreeItem::LooseCommit(ii) => {
                                    store
                                        .record_locator(
                                            tree_loose[*ii].head().as_bytes().to_vec(),
                                            session.sedimentree_id,
                                            BigRepoCiphertextKind::LooseCommit,
                                        )
                                        .await?;
                                }
                            }
                        }
                        store
                    };

                    let received_order = order
                        .iter()
                        .filter_map(|item| {
                            let content_ref = match item {
                                SedimentreeItem::Fragment(ii) => {
                                    tree_fragments[*ii].head().as_bytes().to_vec()
                                }
                                SedimentreeItem::LooseCommit(ii) => {
                                    tree_loose[*ii].head().as_bytes().to_vec()
                                }
                            };
                            received_refs
                                .contains(&content_ref)
                                .then_some((item, content_ref))
                        })
                        .collect::<Vec<_>>();
                    let mut plaintext_by_ref: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
                    let mut plaintext_by_index: Vec<Option<Vec<u8>>> =
                        std::iter::repeat_with(|| None)
                            .take(received_order.len())
                            .collect();
                    let expected_received =
                        session.received_commit_ids.len() + session.received_fragment_ids.len();
                    if received_order.len() != expected_received {
                        return Err(ferr!(
                            "sync session received blobs are missing from sedimentree order: expected={} found={}",
                            expected_received,
                            received_order.len(),
                        ));
                    }

                    for (idx, (item, content_ref)) in received_order.iter().enumerate().rev() {
                        if let Some(plaintext) = plaintext_by_ref.get(content_ref).cloned() {
                            plaintext_by_index[idx] = Some(plaintext);
                            continue;
                        }

                        let locator = match item {
                            SedimentreeItem::Fragment(ii) => BigRepoCiphertextLocator {
                                kind: BigRepoCiphertextKind::Fragment,
                                sedimentree_id: session.sedimentree_id,
                                commit_id: CommitId::new(*tree_fragments[*ii].head().as_bytes()),
                            },
                            SedimentreeItem::LooseCommit(ii) => BigRepoCiphertextLocator {
                                kind: BigRepoCiphertextKind::LooseCommit,
                                sedimentree_id: session.sedimentree_id,
                                commit_id: CommitId::new(*tree_loose[*ii].head().as_bytes()),
                            },
                        };
                        let raw = ciphertext_store
                            .load_raw_for_locator(&locator)
                            .await
                            .map_err(|e| ferr!("failed loading exact sync session blob: {e}"))?
                            .ok_or_else(|| {
                                ferr!(
                                    "missing exact sync session blob: sedimentree_id={:?} content_ref={:?} kind={:?}",
                                    session.sedimentree_id,
                                    content_ref,
                                    locator.kind
                                )
                            })?;

                        if raw.first() != Some(&0x02) {
                            let first_byte = raw.first().copied();
                            let is_received_commit = session
                                .received_commit_ids
                                .iter()
                                .any(|commit_id| commit_id.as_bytes() == content_ref.as_slice());
                            let is_received_fragment =
                                session.received_fragment_ids.iter().any(|fragment_id| {
                                    fragment_id.as_bytes() == content_ref.as_slice()
                                });
                            return Err(ferr!(
                                "sync session blob is not encrypted: sedimentree_id={:?} content_ref={:?} len={} first_byte={:?} received_commit={} received_fragment={}",
                                session.sedimentree_id,
                                content_ref,
                                raw.len(),
                                first_byte,
                                is_received_commit,
                                is_received_fragment,
                            ));
                        }

                        let encrypted = decode_encrypted_blob(raw.as_slice())?;

                        let (entrypoint_raw, _entrypoint_key) = {
                            let mut doc = kh_doc.lock().await;
                            match doc.try_decrypt_content_keyed(&encrypted) {
                                Ok(result) => result,
                                Err(
                                    keyhive_core::principal::document::DecryptError::KeyNotFound,
                                ) => {
                                    materialization_pending = true;
                                    break;
                                }
                                Err(err) => {
                                    return Err(ferr!(
                                        "failed decrypting sync session entrypoint: {err}"
                                    ));
                                }
                            }
                        };
                        let entrypoint_envelope: Envelope<Vec<u8>, Vec<u8>> =
                            bincode::deserialize(&entrypoint_raw).map_err(|e| {
                                ferr!("bincode decode sync session entrypoint envelope: {e}")
                            })?;
                        let exact_plaintext = entrypoint_envelope.plaintext;
                        plaintext_by_index[idx] = Some(exact_plaintext.clone());
                        plaintext_by_ref.insert(content_ref.clone(), exact_plaintext);

                        let state = {
                            let mut doc = kh_doc.lock().await;
                            doc.try_causal_decrypt_content(&encrypted, ciphertext_store.clone())
                                .await
                                .map_err(|e| {
                                    ferr!("failed causal decrypting sync session blob: {e}")
                                })?
                        };
                        for (ancestor_ref, plaintext) in state.complete {
                            plaintext_by_ref.insert(ancestor_ref, plaintext);
                        }
                    }

                    if materialization_pending {
                        Vec::new()
                    } else {
                        plaintext_by_index
                            .into_iter()
                            .enumerate()
                            .map(|(i, plaintext)| {
                                plaintext.ok_or_else(|| {
                                    ferr!("missing plaintext for sync session blob {i}")
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    }
                }
                None => {
                    materialization_pending = true;
                    Vec::new()
                }
            }
        };
        if blobs.is_empty() {
            store_doc_heads_payload(
                &self.big_sync_store,
                self.doc_id,
                sedimentree_heads_payload(&tree),
            )
            .await?;
            if materialization_pending {
                self.mark_materialization_pending(was_pending)?;
            }
            return Ok(());
        }
        if materialization_pending {
            let heads = sedimentree_heads_payload(&tree);
            store_doc_heads_payload(&self.big_sync_store, self.doc_id, heads).await?;
            self.mark_materialization_pending(was_pending)?;
            return Ok(());
        }
        let maybe_delta = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Unloaded | DocWorkerDocState::PendingMaterialization => {
                // since the doc in storage will have the latest blobs,
                // we must load the before_heads from the partition payloads
                let doc = load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    &self.runtime_handle.keyhive,
                    self.doc_id,
                )
                .await?;
                let mut doc = match doc {
                    DocLookup::Ready(doc) => {
                        self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                        doc
                    }
                    DocLookup::PendingMaterialization => {
                        self.mark_materialization_pending(was_pending)?;
                        return Ok(());
                    }
                    DocLookup::Missing => automerge::Automerge::new(),
                };
                let cached_before_heads =
                    super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id).await?;
                let before_heads = cached_before_heads
                    .clone()
                    .unwrap_or_else(|| doc.get_heads().into());
                let had_cached_before_heads = cached_before_heads.is_some();
                let loaded_heads = doc.get_heads();
                let out = if before_heads[..] == loaded_heads[..] {
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    if before_heads[..] == after_heads[..] {
                        if had_cached_before_heads {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = doc.diff(&before_heads, &after_heads);
                        Some((after_heads, patches))
                    }
                } else {
                    let patches = doc.diff(&before_heads, &loaded_heads);
                    Some((loaded_heads, patches))
                };
                self.state = DocWorkerDocState::Transient(doc.into());
                out
            }
            DocWorkerDocState::Transient(mut doc) => {
                // A sync session can be a no-op for content and still be the
                // first chance to persist heads for a reopened doc.
                let cached_before_heads =
                    super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id).await?;
                let out = {
                    let before_heads = doc.get_heads();
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    if before_heads == after_heads {
                        if cached_before_heads.is_some() {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = doc.diff(&before_heads, &after_heads);
                        Some((after_heads, patches))
                    }
                };
                self.state = DocWorkerDocState::Transient(doc);
                out
            }
            DocWorkerDocState::Live(bundle) => match bundle.upgrade() {
                Some(bundle) => {
                    let mut doc = bundle.doc.lock().await;
                    let cached_before_heads =
                        super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id)
                            .await?;
                    let before_heads = doc.get_heads();
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    let out = if before_heads == after_heads {
                        if cached_before_heads.is_some() {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches = doc.diff(&before_heads, &after_heads);
                        Some((after_heads, patches))
                    };
                    drop(doc);
                    self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
                    out
                }
                None => {
                    let doc = load_doc_snapshot(
                        &self.sedimentrees,
                        &self.storage_for_reads,
                        &self.runtime_handle.keyhive,
                        self.doc_id,
                    )
                    .await?;
                    let mut doc = match doc {
                        DocLookup::Ready(doc) => {
                            self.mark_materialization_ready(
                                was_pending,
                                Arc::from(doc.get_heads()),
                            )?;
                            doc
                        }
                        DocLookup::PendingMaterialization => {
                            self.mark_materialization_pending(was_pending)?;
                            return Ok(());
                        }
                        DocLookup::Missing => automerge::Automerge::new(),
                    };
                    let loaded_heads = doc.get_heads();
                    let cached_before_heads =
                        super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id)
                            .await?;
                    let before_heads = cached_before_heads
                        .clone()
                        .unwrap_or_else(|| loaded_heads.clone().into());
                    let had_cached_before_heads = cached_before_heads.is_some();
                    let out = if before_heads[..] == loaded_heads[..] {
                        for blob in blobs {
                            doc.load_incremental(&blob).map_err(|err| {
                                eyre::eyre!("failed applying sync session blob: {err}")
                            })?;
                        }
                        let after_heads = doc.get_heads();
                        if before_heads[..] == after_heads[..] {
                            if had_cached_before_heads {
                                None
                            } else {
                                Some((after_heads, Vec::new()))
                            }
                        } else {
                            let patches = doc.diff(&before_heads, &after_heads);
                            Some((after_heads, patches))
                        }
                    } else {
                        let patches = doc.diff(&before_heads, &loaded_heads);
                        Some((loaded_heads, patches))
                    };
                    self.state = DocWorkerDocState::Transient(doc.into());
                    out
                }
            },
        };
        let Some((after_heads, patches)) = maybe_delta else {
            return Ok(());
        };
        commit_delta_bookkeep(
            &self.big_sync_store,
            &self.change_manager,
            self.doc_id,
            after_heads,
            patches,
            BigRepoChangeOrigin::Remote {
                peer_id: PeerId::new(*session.peer_id.as_bytes()),
            },
        )
        .await?;
        Ok(())
    }

    async fn handle_sync_with_peer(
        &mut self,
        peer_id: PeerId,
        _waiter_id: u64,
        timeout: Option<Duration>,
        sender: oneshot::Sender<Result<(), SyncDocError>>,
    ) -> Res<()> {
        let sedimentree_id = self.doc_id_subduction;
        let applied_generation_before = self
            .applied_sync_generations
            .get(&peer_id)
            .copied()
            .unwrap_or_default();
        let remote_peer_id = subduction_core::peer::id::PeerId::new(peer_id.clone().into_bytes());
        let timeout = timeout
            .map(|duration| {
                subduction_core::timeout::call::CallTimeout::TimeoutMillis(
                    duration
                        .as_millis()
                        .try_into()
                        .expect("timeout fits in u64"),
                )
            })
            .unwrap_or(subduction_core::timeout::call::CallTimeout::Default);
        let result = self
            .subduction
            .sync_with_peer(&remote_peer_id, sedimentree_id, false, timeout)
            .await;
        let res = match result {
            Ok((had_success, stats, conn_errs)) => {
                if had_success {
                    if stats.commits_received > 0 || stats.fragments_received > 0 {
                        let applied_generation_after = self
                            .applied_sync_generations
                            .get(&peer_id)
                            .copied()
                            .unwrap_or_default();
                        if applied_generation_after == applied_generation_before {
                            self.apply_current_storage_after_sync(peer_id, stats.remote_heads)
                                .await?;
                            *self.applied_sync_generations.entry(peer_id).or_default() += 1;
                        }
                        Ok(())
                    } else {
                        if let Some(tree) = self.sedimentrees.get_cloned(&sedimentree_id).await {
                            store_doc_heads_payload(
                                &self.big_sync_store,
                                self.doc_id,
                                sedimentree_heads_payload(&tree),
                            )
                            .await?;
                        }
                        Ok(())
                    }
                } else if conn_errs.is_empty() {
                    Err(SyncDocError::NotFound)
                } else {
                    Err(SyncDocError::TransportError)
                }
            }
            Err(err) => Err(SyncDocError::IoError(ferr!("{err}"))),
        };
        self.finish_transient_work()?;
        sender.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
        Ok(())
    }

    async fn apply_current_storage_after_sync(
        &mut self,
        peer_id: PeerId,
        remote_heads: subduction_core::remote_heads::RemoteHeads,
    ) -> Res<()> {
        let tree = match self.sedimentrees.get_cloned(&self.doc_id_subduction).await {
            Some(tree) => tree,
            None => {
                let loose_commits = <S as subduction_core::storage::traits::Storage<
                    future_form::Sendable,
                >>::load_loose_commits(
                    &self.storage_for_reads, self.doc_id_subduction
                )
                .await?
                .into_iter()
                .collect::<Vec<_>>();
                let fragments = <S as subduction_core::storage::traits::Storage<
                    future_form::Sendable,
                >>::load_fragments(
                    &self.storage_for_reads, self.doc_id_subduction
                )
                .await?
                .into_iter()
                .collect::<Vec<_>>();
                sedimentree_core::sedimentree::minimized::MinimizedSedimentree::new(
                    Sedimentree::new(
                        fragments
                            .iter()
                            .map(|verified| verified.payload().clone())
                            .collect(),
                        loose_commits
                            .iter()
                            .map(|verified| verified.payload().clone())
                            .collect(),
                    ),
                )
            }
        };
        let mut session = subduction_core::sync_session::SyncSession::new(
            self.doc_id_subduction,
            subduction_core::peer::id::PeerId::new(peer_id.into_bytes()),
            subduction_core::sync_session::SyncSessionKind::OutboundBatch,
        );
        session.remote_heads = Some(remote_heads);
        session.received_commit_ids = tree
            .loose_commits()
            .map(|commit| CommitId::new(*commit.head().as_bytes()))
            .collect();
        session.received_fragment_ids = tree
            .fragments()
            .map(|fragment| CommitId::new(*fragment.head().as_bytes()))
            .collect();
        self.handle_apply_sync_session(session).await
    }

    async fn handle_cancel_sync_with_peer(
        &mut self,
        peer_id: PeerId,
        waiter_id: Option<u64>,
        reason: &'static str,
    ) -> Res<()> {
        let waiters = match waiter_id {
            Some(waiter_id) => {
                let Some(waiters) = self.pending_sync_jobs.get_mut(&peer_id) else {
                    return Ok(());
                };
                let Some(pos) = waiters
                    .iter()
                    .position(|(pending_id, _)| *pending_id == waiter_id)
                else {
                    return Ok(());
                };
                let waiter = waiters.remove(pos);
                if waiters.is_empty() {
                    self.pending_sync_jobs.remove(&peer_id);
                }
                vec![waiter]
            }
            None => self.pending_sync_jobs.remove(&peer_id).unwrap_or_default(),
        };
        for (_, waiter) in waiters {
            self.finish_transient_work()?;
            waiter
                .send(Err(SyncDocError::IoError(ferr!("{reason}"))))
                .inspect_err(|_| warn!(ERROR_CALLER))
                .ok();
        }
        Ok(())
    }

    fn finish_transient_work(&self) -> Res<()> {
        self.runtime_evt_tx
            .send(RuntimeEvt::DocWorkerTransientFinished {
                doc_id: self.doc_id,
            })
            .wrap_err(ERROR_CHANNEL)
    }

    fn mark_materialization_pending(&mut self, was_pending: bool) -> Res<()> {
        self.state = DocWorkerDocState::PendingMaterialization;
        if !was_pending {
            self.change_manager
                .notify_local_doc_materialization_pending(self.doc_id)?;
        }
        Ok(())
    }

    fn mark_materialization_ready(
        &mut self,
        was_pending: bool,
        heads: Arc<[automerge::ChangeHash]>,
    ) -> Res<()> {
        if was_pending {
            self.change_manager
                .notify_local_doc_materialization_ready(self.doc_id, heads)?;
        }
        Ok(())
    }

    // FIXME: this only does fragment processing if there's
    // pending requests, is this wise? seems efficient but
    // confirm that we can defer fragment processing like this
    async fn process_pending_fragment_requests(&mut self) -> Res<()> {
        let DocWorkerDocState::Live(bundle) = &self.state else {
            return Ok(());
        };
        let Some(bundle) = bundle.upgrade() else {
            return Ok(());
        };
        if self.pending_fragment_requests.is_empty() {
            return Ok(());
        }
        let requests = std::mem::take(&mut self.pending_fragment_requests);
        process_fragment_requests(
            Arc::clone(&bundle),
            requests,
            Arc::clone(&self.subduction),
            self.storage_for_reads.clone(),
            self.runtime_handle.keyhive.clone(),
        )
        .await
    }

    #[tracing::instrument(skip_all)]
    async fn take_or_load_transient_doc(&mut self) -> Res<DocLookup<automerge::Automerge>> {
        let was_pending = matches!(self.state, DocWorkerDocState::PendingMaterialization);
        let out = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Transient(doc) => Ok(DocLookup::Ready(*doc)),
            DocWorkerDocState::Unloaded => {
                let loaded = load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    &self.runtime_handle.keyhive,
                    self.doc_id,
                )
                .await?;
                match loaded {
                    DocLookup::Ready(doc) => {
                        self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                        Ok(DocLookup::Ready(doc))
                    }
                    DocLookup::PendingMaterialization => {
                        self.mark_materialization_pending(was_pending)?;
                        Ok(DocLookup::PendingMaterialization)
                    }
                    DocLookup::Missing => Ok(DocLookup::Missing),
                }
            }
            DocWorkerDocState::Live(bundle) => {
                self.state = DocWorkerDocState::Live(bundle);
                eyre::bail!("document already live")
            }
            DocWorkerDocState::PendingMaterialization => {
                let loaded = load_doc_snapshot(
                    &self.sedimentrees,
                    &self.storage_for_reads,
                    &self.runtime_handle.keyhive,
                    self.doc_id,
                )
                .await?;
                match loaded {
                    DocLookup::Ready(doc) => {
                        self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                        Ok(DocLookup::Ready(doc))
                    }
                    DocLookup::PendingMaterialization => {
                        self.mark_materialization_pending(was_pending)?;
                        Ok(DocLookup::PendingMaterialization)
                    }
                    DocLookup::Missing => Ok(DocLookup::Missing),
                }
            }
        };
        out
    }
}

async fn commit_delta_bookkeep(
    big_sync_store: &SharedPartitionStore,
    change_manager: &Arc<changes::ChangeListenerManager>,
    doc_id: DocumentId,
    heads: Vec<automerge::ChangeHash>,
    patches: Vec<automerge::Patch>,
    origin: BigRepoChangeOrigin,
) -> Res<()> {
    let item_payload = serde_json::json!({
        "heads": am_utils_rs::serialize_commit_heads(&heads),
    });
    big_sync_store.set_obj_payload(doc_id, item_payload).await?;

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

fn doc_heads_from_commit_ids(
    heads: impl IntoIterator<Item = CommitId>,
) -> Arc<[automerge::ChangeHash]> {
    Arc::<[automerge::ChangeHash]>::from(
        heads
            .into_iter()
            .map(|head| automerge::ChangeHash(*head.as_bytes()))
            .collect::<Vec<_>>(),
    )
}

async fn store_doc_heads_payload(
    big_sync_store: &SharedPartitionStore,
    doc_id: DocumentId,
    heads: Arc<[automerge::ChangeHash]>,
) -> Res<()> {
    let item_payload = serde_json::json!({
        "heads": am_utils_rs::serialize_commit_heads(&heads),
    });
    big_sync_store.set_obj_payload(doc_id, item_payload).await?;
    Ok(())
}

fn sedimentree_heads_payload(tree: &Sedimentree) -> Arc<[automerge::ChangeHash]> {
    doc_heads_from_commit_ids(tree.heads(&CountLeadingZeroBytes))
}

async fn process_fragment_requests<S>(
    bundle: Arc<LiveDocBundle>,
    requests: BTreeSet<FragmentRequested>,
    subduction: Arc<BigRepoSubduction<S>>,
    storage_for_reads: S,
    keyhive_handle: BigKeyhiveHandle,
) -> Res<()>
where
    S: BigRepoSubductionStorage,
{
    if requests.is_empty() {
        return Ok(());
    }
    let doc_id = bundle.doc_id;
    let sed_id = SedimentreeId::new(doc_id.into_bytes());
    let mut work = Vec::with_capacity(requests.len());
    {
        let doc = bundle.doc.lock().await;
        for req in requests {
            let fragment = doc
                .get_fragment(automerge::ChangeHash(*req.head().as_bytes()))
                .expect("error resolving requested fragment from automerge::Automerge");
            debug_assert_eq!(req.depth().0, fragment.level as u32);
            let raw = doc
                .bundle(fragment.members.iter().cloned())
                .wrap_err("unable to resolve bundle for fragment")?
                .bytes()
                .to_vec();
            work.push((
                req.head(),
                fragment
                    .boundary
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect::<BTreeSet<_>>(),
                fragment
                    .checkpoints
                    .iter()
                    .map(|head| CommitId::new(head.0))
                    .collect::<Vec<_>>(),
                raw,
            ));
        }
    }
    for (commit_id, boundary, checkpoints, raw) in work {
        let blob = encrypt_fragment_blob(
            &keyhive_handle,
            &storage_for_reads,
            sed_id,
            commit_id,
            &boundary,
            &raw,
        )
        .await?;
        subduction
            .add_fragment(sed_id, commit_id, boundary, &checkpoints, blob)
            .await
            .map_err(|err| ferr!("failed add_fragment: {err}"))?;
    }
    Ok(())
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
    conn: iroh::endpoint::Connection,
    signer: &subduction_crypto::signer::memory::MemorySigner,
    nonce_cache: &subduction_core::nonce_cache::NonceCache,
    local_peer_id: subduction_core::peer::id::PeerId,
) -> Res<IrohConnectResult> {
    let (send, recv) = conn
        .accept_bi()
        .await
        .map_err(|err| ferr!("failed accepting subduction bidi stream: {err}"))?;
    let now = subduction_core::timestamp::TimestampSeconds::now();
    let handshake = subduction_iroh::handshake::IrohHandshake::new(send, recv);
    let (authenticated, (listener_task, sender_task)) = subduction_core::handshake::respond(
        handshake,
        move |handshake, peer_id| {
            let (send, recv) = handshake.into_parts();
            let (transport, outbound_rx) =
                subduction_iroh::transport::IrohTransport::new(peer_id, conn);
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

// FIXME: again this seems convoluted.
// one thing I'm confused by is, we provide the heads to the
// keyhive at create_doc but we do'nt seem to be providing the
// heads aftewrards? what's up with that?
// https://www.inkandswitch.com/keyhive/notebook/ specifies
// pointers between keyhive and docs so maybe it can help us here
async fn load_doc_snapshot<S, D>(
    sedimentrees: &SubductionSedimentrees,
    storage_for_reads: &S,
    keyhive: &BigKeyhiveHandle,
    doc_id: D,
) -> Res<DocLookup<automerge::Automerge>>
where
    S: BigRepoSubductionStorage,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    let sedimentree_id = SedimentreeId::new(doc_id.into_bytes());

    let loose_commits =
        <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_loose_commits(
            storage_for_reads,
            sedimentree_id,
        );
    let fragments =
        <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_fragments(
            storage_for_reads,
            sedimentree_id,
        );
    let (loose_commits, fragments) = futures::future::try_join(loose_commits, fragments)
        .await
        .wrap_err("failed reading blobs from storage")?;
    if loose_commits.is_empty() && fragments.is_empty() {
        return Ok(DocLookup::Missing);
    }
    // FIXME: consider subduciton.get_or_hydrate here
    let (tree, fresh) = match sedimentrees.get_cloned(&sedimentree_id).await {
        Some(tree) => (tree, false),
        None => {
            let tree = sedimentree_core::sedimentree::minimized::MinimizedSedimentree::new(
                Sedimentree::new(
                    fragments
                        .iter()
                        .map(|verified| verified.payload().clone())
                        .collect(),
                    loose_commits
                        .iter()
                        .map(|verified| verified.payload().clone())
                        .collect(),
                ),
            );
            (tree, true)
        }
    };
    let order = tree
        .topsorted_blob_order()
        .map_err(|err| ferr!("failed ordering sedimentree blobs: {err}"))?;
    let fragments: Vec<_> = tree.fragments().collect();
    let loose: Vec<_> = tree.loose_commits().collect();

    let keyhive = keyhive.clone_keyhive();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let Some(kh_doc) = keyhive.get_document(kh_doc_id).await else {
        return Ok(DocLookup::PendingMaterialization);
    };

    let ciphertext_store = {
        let store = BigRepoCiphertextStore::new(storage_for_reads.clone());
        for item in &order {
            match item {
                SedimentreeItem::Fragment(ii) => {
                    store
                        .record_locator(
                            fragments[*ii].head().as_bytes().to_vec(),
                            sedimentree_id,
                            BigRepoCiphertextKind::Fragment,
                        )
                        .await?;
                }
                SedimentreeItem::LooseCommit(ii) => {
                    store
                        .record_locator(
                            loose[*ii].head().as_bytes().to_vec(),
                            sedimentree_id,
                            BigRepoCiphertextKind::LooseCommit,
                        )
                        .await?;
                }
            }
        }
        store
    };

    let mut plaintext_by_ref: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut plaintext_by_index: Vec<Option<Vec<u8>>> =
        std::iter::repeat_with(|| None).take(order.len()).collect();
    let mut items_by_ref: HashMap<Vec<u8>, Vec<(usize, SedimentreeItem)>> = HashMap::new();
    for (idx, item) in order.iter().enumerate() {
        let content_ref = match item {
            SedimentreeItem::Fragment(ii) => fragments[*ii].head().as_bytes().to_vec(),
            SedimentreeItem::LooseCommit(ii) => loose[*ii].head().as_bytes().to_vec(),
        };
        items_by_ref
            .entry(content_ref)
            .or_default()
            .push((idx, *item));
    }
    let mut grouped_order: Vec<(usize, Vec<u8>)> = items_by_ref
        .iter()
        .map(|(content_ref, items)| {
            (
                items.iter().map(|(idx, _)| *idx).max().unwrap_or(0),
                content_ref.clone(),
            )
        })
        .collect();
    grouped_order.sort_by_key(|(idx, _)| std::cmp::Reverse(*idx));

    for (_, content_ref) in grouped_order {
        let items = items_by_ref
            .get(&content_ref)
            .expect("missing snapshot group after grouping");
        if let Some(plaintext) = plaintext_by_ref.get(&content_ref).cloned() {
            for (idx, _) in items {
                plaintext_by_index[*idx] = Some(plaintext.clone());
            }
            continue;
        }

        let mut candidates = items.clone();
        candidates.sort_by_key(|(_, item)| match item {
            SedimentreeItem::Fragment(_) => 0usize,
            SedimentreeItem::LooseCommit(_) => 1usize,
        });

        let mut decrypted = None;
        for (_idx, item) in candidates {
            let locator = match item {
                SedimentreeItem::Fragment(ii) => BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::Fragment,
                    sedimentree_id,
                    commit_id: CommitId::new(*fragments[ii].head().as_bytes()),
                },
                SedimentreeItem::LooseCommit(ii) => BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::LooseCommit,
                    sedimentree_id,
                    commit_id: CommitId::new(*loose[ii].head().as_bytes()),
                },
            };
            let Some(raw) = ciphertext_store
                .load_raw_for_locator(&locator)
                .await
                .map_err(|e| ferr!("failed loading exact snapshot blob: {e}"))?
            else {
                continue;
            };
            if raw.first() != Some(&0x02) {
                return Err(ferr!(
                    "snapshot blob is not encrypted: sedimentree_id={sedimentree_id:?} content_ref={content_ref:?} kind={:?}",
                    locator.kind
                ));
            }
            let encrypted = decode_encrypted_blob(raw.as_slice())?;
            let decrypt_result = {
                let mut doc = kh_doc.lock().await;
                doc.try_decrypt_content_keyed(&encrypted)
            };
            let (entrypoint_raw, _entrypoint_key) = match decrypt_result {
                Ok(result) => result,
                Err(keyhive_core::principal::document::DecryptError::KeyNotFound) => {
                    tracing::warn!(
                        sedimentree_id = ?sedimentree_id,
                        content_ref = ?content_ref,
                        kind = ?locator.kind,
                        pcs_update_op_hash = ?encrypted.pcs_update_op_hash,
                        "snapshot decrypt key not found"
                    );
                    continue;
                }
                Err(err) => {
                    return Err(ferr!(
                                "failed decrypting snapshot entrypoint: sedimentree_id={sedimentree_id:?} content_ref={content_ref:?} kind={:?} error={err}",
                        locator.kind
                    ));
                }
            };
            let entrypoint_envelope: keyhive_core::crypto::envelope::Envelope<Vec<u8>, Vec<u8>> =
                bincode::deserialize(&entrypoint_raw)
                    .map_err(|e| ferr!("bincode decode snapshot entrypoint envelope: {e}"))?;
            let exact_plaintext = entrypoint_envelope.plaintext;
            for (idx, _) in items {
                plaintext_by_index[*idx] = Some(exact_plaintext.clone());
            }
            plaintext_by_ref.insert(content_ref.clone(), exact_plaintext);

            let state = {
                let mut doc = kh_doc.lock().await;
                doc.try_causal_decrypt_content(&encrypted, ciphertext_store.clone())
                    .await
                    .map_err(|e| ferr!("failed causal decrypting snapshot blob: {e}"))?
            };
            for (ancestor_ref, plaintext) in state.complete {
                plaintext_by_ref.insert(ancestor_ref, plaintext);
            }
            decrypted = Some(());
            break;
        }

        if decrypted.is_none() {
            continue;
        }
    }

    for (content_ref, items) in &items_by_ref {
        if let Some(plaintext) = plaintext_by_ref.get(content_ref).cloned() {
            for (idx, _) in items {
                plaintext_by_index[*idx] = Some(plaintext.clone());
            }
        }
    }

    let mut plaintexts: Vec<Vec<u8>> = Vec::with_capacity(order.len());
    for plaintext in plaintext_by_index {
        let Some(plaintext) = plaintext else {
            return Ok(DocLookup::PendingMaterialization);
        };
        plaintexts.push(plaintext);
    }

    let mut buf = Vec::new();
    for plaintext in &plaintexts {
        buf.extend_from_slice(plaintext);
    }

    let mut doc = automerge::Automerge::new();
    doc.load_incremental(&buf)
        .map_err(|err| ferr!("failed reconstructing automerge doc from ordered blobs: {err}"))?;

    if fresh {
        sedimentrees
            .get_or_insert_with(sedimentree_id, || tree)
            .await;
    }
    Ok(DocLookup::Ready(doc))
}

// FIXME: the whole encryption/decryption scheme still needs cleanup.

/// Metadata for a single fragment from ingest, used to rebuild after encryption.
struct FragmentEntry {
    head: CommitId,
    boundary: BTreeSet<CommitId>,
    checkpoints: Vec<CommitId>,
}

/// Metadata for a single loose commit from ingest, used to rebuild after encryption.
struct LooseEntry {
    head: CommitId,
    parents: BTreeSet<CommitId>,
}

/// Result of ingesting an Automerge document.
struct IngestResult {
    sedimentree: Sedimentree,
    blobs: Vec<Blob>,
    fragment_entries: Vec<FragmentEntry>,
    loose_entries: Vec<LooseEntry>,
    _change_count: usize,
    _covered_count: usize,
    _loose_count: usize,
    _fragment_count: usize,
}

/// Ingest an Automerge document into a [`Sedimentree`].
///
/// Thin adapter over [`Automerge::fragments`] and
/// [`Automerge::bundle_fragments`]: maps each level-1+ `automerge::Fragment`
/// into a sedimentree [`Fragment`] and each level-0 fragment into a
/// [`LooseCommit`].
fn ingest_automerge(doc: &automerge::Automerge, sedimentree_id: SedimentreeId) -> IngestResult {
    use sedimentree_core::{blob::BlobMeta, fragment::Fragment, loose_commit::LooseCommit};

    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    let loose_bytes = doc.bundle_fragments(loose.iter().cloned());

    let mut fragments = Vec::with_capacity(cached.len());
    let mut blobs = Vec::with_capacity(cached.len() + loose.len());
    let mut fragment_entries = Vec::with_capacity(cached.len());
    let mut loose_entries = Vec::with_capacity(loose.len());
    let mut covered: sedimentree_core::collections::Set<CommitId> = default();
    for (fragment, raw) in cached.iter().zip(cached_bytes) {
        for member in &fragment.members {
            covered.insert(CommitId::new(member.0));
        }
        let head = CommitId::new(fragment.head.0);
        let boundary: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        let checkpoints: Vec<CommitId> = fragment
            .checkpoints
            .iter()
            .map(|head| CommitId::new(head.0))
            .collect();
        let blob = Blob::new(raw);
        let meta = BlobMeta::new(&blob);
        fragments.push(Fragment::new(
            sedimentree_id,
            head,
            boundary.clone(),
            &checkpoints,
            meta,
        ));
        blobs.push(blob);
        fragment_entries.push(FragmentEntry {
            head,
            boundary,
            checkpoints,
        });
    }

    let mut loose_commits = Vec::with_capacity(loose.len());
    for (fragment, raw) in loose.iter().zip(loose_bytes) {
        let head = CommitId::new(fragment.head.0);
        let parents: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|pp| CommitId::new(pp.0))
            .collect();
        let blob = Blob::new(raw);
        let meta = BlobMeta::new(&blob);
        loose_commits.push(LooseCommit::new(
            sedimentree_id,
            head,
            parents.clone(),
            meta,
        ));
        blobs.push(blob);
        loose_entries.push(LooseEntry { head, parents });
    }

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();
    let covered_count = covered.len();

    IngestResult {
        sedimentree: Sedimentree::new(fragments, loose_commits),
        blobs,
        fragment_entries,
        loose_entries,
         _change_count: doc.get_changes_meta(&[]).len(),
        _covered_count: covered_count,
        _loose_count: loose_count,
        _fragment_count: fragment_count,
    }
}

/// DRISL envelope for an encrypted blob.
///
/// Flattens `EncryptedContent` fields inline. The predecessor key chain
/// lives inside the plaintext via keyhive_core's `Envelope<C, T>` wrapper
/// (bincode-serialized before encryption).
///
/// Format: `[0x02][DRISL(EncryptedBlobEnvelope)]`
#[derive(serde::Serialize, serde::Deserialize)]
struct EncryptedBlobEnvelope {
    #[serde(flatten)]
    encrypted: beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum BigRepoCiphertextKind {
    Fragment,
    LooseCommit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BigRepoCiphertextLocator {
    kind: BigRepoCiphertextKind,
    sedimentree_id: SedimentreeId,
    commit_id: CommitId,
}

#[derive(Clone, Debug)]
struct BigRepoCiphertextStore<S> {
    storage_for_reads: S,
    locators: Arc<
        tokio::sync::Mutex<HashMap<Vec<u8>, std::collections::BTreeSet<BigRepoCiphertextLocator>>>,
    >,
    decrypted: Arc<tokio::sync::Mutex<std::collections::HashSet<Vec<u8>>>>,
    pcs_updates: Arc<
        tokio::sync::Mutex<
            HashMap<
                keyhive_crypto::digest::Digest<
                    keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
                >,
                std::collections::HashSet<Vec<u8>>,
            >,
        >,
    >,
}

impl<S> BigRepoCiphertextStore<S> {
    fn new(storage_for_reads: S) -> Self {
        Self {
            storage_for_reads,
            locators: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            decrypted: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
            pcs_updates: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn record_locator(
        &self,
        content_ref: Vec<u8>,
        sedimentree_id: SedimentreeId,
        kind: BigRepoCiphertextKind,
    ) -> Res<()>
    where
        S: BigRepoSubductionStorage,
    {
        let commit_id_bytes: [u8; 32] = content_ref
            .as_slice()
            .try_into()
            .map_err(|_| ferr!("content ref must be 32 bytes"))?;
        let locator = BigRepoCiphertextLocator {
            kind,
            sedimentree_id,
            commit_id: CommitId::new(commit_id_bytes),
        };
        let raw = self
            .load_raw_for_locator(&locator)
            .await?
            .ok_or_else(|| {
                ferr!(
                    "missing ciphertext blob while recording locator: sedimentree_id={sedimentree_id:?} content_ref={content_ref:?} kind={kind:?}"
                )
            })?;
        let encrypted = decode_encrypted_blob(raw.as_slice())?;
        self.locators
            .lock()
            .await
            .entry(content_ref.clone())
            .or_default()
            .insert(locator);
        self.pcs_updates
            .lock()
            .await
            .entry(encrypted.pcs_update_op_hash)
            .or_default()
            .insert(content_ref);
        Ok(())
    }

    async fn load_raw_for_locator(&self, locator: &BigRepoCiphertextLocator) -> Res<Option<Vec<u8>>>
    where
        S: BigRepoSubductionStorage,
    {
        let maybe_raw = match locator.kind {
            BigRepoCiphertextKind::LooseCommit => {
                <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_loose_commit(
                    &self.storage_for_reads,
                    locator.sedimentree_id,
                    locator.commit_id,
                )
                .await
                .map_err(|e| ferr!("failed loading loose commit for ciphertext: {e}"))?
                .map(|verified| verified.blob().clone().into_contents())
            }
            BigRepoCiphertextKind::Fragment => {
                <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_fragment(
                    &self.storage_for_reads,
                    locator.sedimentree_id,
                    locator.commit_id,
                )
                .await
                .map_err(|e| ferr!("failed loading fragment for ciphertext: {e}"))?
                .map(|verified| verified.blob().clone().into_contents())
            }
        };
        Ok(maybe_raw)
    }
}

fn decode_encrypted_blob(
    raw_bytes: &[u8],
) -> Res<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>> {
    if raw_bytes.first() != Some(&0x02) {
        return Err(ferr!("blob is not encrypted (missing 0x02 discriminator)"));
    }
    let envelope: EncryptedBlobEnvelope = atproto_dasl::drisl::from_slice(&raw_bytes[1..])
        .map_err(|e| ferr!("DRISL decode encrypted blob envelope: {e}"))?;
    Ok(Arc::new(envelope.encrypted))
}

impl<S: BigRepoSubductionStorage> CiphertextStore<future_form::Sendable, Vec<u8>, Vec<u8>>
    for BigRepoCiphertextStore<S>
{
    type GetCiphertextError = eyre::Report;
    type MarkDecryptedError = eyre::Report;

    fn get_ciphertext<'a>(
        &'a self,
        content_ref: &'a Vec<u8>,
    ) -> <future_form::Sendable as FutureForm>::Future<
        'a,
        Result<
            Option<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        future_form::Sendable::from_future(async move {
            if self.decrypted.lock().await.contains(content_ref) {
                return Ok(None);
            }
            let Some(locators) = self.locators.lock().await.get(content_ref).cloned() else {
                return Ok(None);
            };
            for locator in locators {
                let maybe_raw =
                    match locator.kind {
                        BigRepoCiphertextKind::LooseCommit => {
                            <S as subduction_core::storage::traits::Storage<
                                future_form::Sendable,
                            >>::load_loose_commit(
                                &self.storage_for_reads,
                                locator.sedimentree_id,
                                locator.commit_id,
                            )
                            .await
                            .map_err(|e| ferr!("failed loading loose commit for ciphertext: {e}"))?
                            .map(|verified| verified.blob().clone().into_contents())
                        }
                        BigRepoCiphertextKind::Fragment => {
                            <S as subduction_core::storage::traits::Storage<
                                future_form::Sendable,
                            >>::load_fragment(
                                &self.storage_for_reads,
                                locator.sedimentree_id,
                                locator.commit_id,
                            )
                            .await
                            .map_err(|e| ferr!("failed loading fragment for ciphertext: {e}"))?
                            .map(|verified| verified.blob().clone().into_contents())
                        }
                    };
                let Some(raw) = maybe_raw else {
                    continue;
                };
                let encrypted = decode_encrypted_blob(raw.as_slice())?;
                return Ok(Some(encrypted));
            }
            Ok(None)
        })
    }

    fn get_ciphertext_by_pcs_update<'a>(
        &'a self,
        pcs_update: &'a keyhive_crypto::digest::Digest<
            keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
        >,
    ) -> <future_form::Sendable as FutureForm>::Future<
        'a,
        Result<
            Vec<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        future_form::Sendable::from_future(async move {
            let content_refs = self
                .pcs_updates
                .lock()
                .await
                .get(pcs_update)
                .cloned()
                .unwrap_or_default();
            let mut out = Vec::new();
            for content_ref in content_refs {
                if let Some(encrypted) = self.get_ciphertext(&content_ref).await? {
                    out.push(encrypted);
                }
            }
            Ok(out)
        })
    }

    fn mark_decrypted<'a>(
        &'a self,
        content_ref: &'a Vec<u8>,
    ) -> <future_form::Sendable as FutureForm>::Future<'a, Result<(), Self::MarkDecryptedError>>
    {
        future_form::Sendable::from_future(async move {
            self.decrypted.lock().await.insert(content_ref.clone());
            self.locators.lock().await.remove(content_ref);
            let mut pcs_updates = self.pcs_updates.lock().await;
            pcs_updates.retain(|_, refs| {
                refs.remove(content_ref);
                !refs.is_empty()
            });
            Ok(())
        })
    }
}

/// Encrypt ingested blobs via the keyhive, rebuilding the sedimentree with
/// updated BlobMeta matching the encrypted bytes.
///
/// If the sedimentree ID does not correspond to a valid keyhive document
/// (e.g. random IDs used in tests), the ingested data is returned unchanged.
/// Encrypt ingested blobs via the keyhive, serializing each encrypted blob as
/// DRISL (deterministic CBOR) prefixed with a 1-byte discriminator:
/// - `0x02` = encrypted: [0x02][DRISL(EncryptedBlobEnvelope)]
///
/// TODO: Upstream (keyhive/subduction_keyhive) hasn't specified the canonical
/// EncryptedContent storage format yet. Revisit when upstream publishes a spec.
async fn encrypt_ingested_blobs(
    ingested: &IngestResult,
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
) -> Res<(
    Sedimentree,
    Vec<Blob>,
    Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;
    use sedimentree_core::{blob::BlobMeta, fragment::Fragment, loose_commit::LooseCommit};

    let keyhive = keyhive_handle.clone_keyhive();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive.get_document(kh_doc_id).await.ok_or_else(|| {
        ferr!("keyhive doc not found in local keyhive; only the doc owner can call put_doc")
    })?;

    let mut encrypted_blobs: Vec<Blob> = Vec::with_capacity(ingested.blobs.len());
    let mut new_fragments: Vec<Fragment> = Vec::with_capacity(ingested.fragment_entries.len());
    let mut new_loose_commits: Vec<LooseCommit> = Vec::with_capacity(ingested.loose_entries.len());
    let mut update_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>> =
        Vec::with_capacity(ingested.fragment_entries.len() + ingested.loose_entries.len());

    // Track content_ref -> SymmetricKey for building ancestor maps
    let mut key_index: std::collections::HashMap<Vec<u8>, SymmetricKey> =
        std::collections::HashMap::new();

    // Encrypt fragment blobs
    for (entry, blob) in ingested
        .fragment_entries
        .iter()
        .zip(ingested.blobs.iter().take(ingested.fragment_entries.len()))
    {
        let content_ref: Vec<u8> = entry.head.as_bytes().to_vec();
        let pred_refs: Vec<Vec<u8>> = entry
            .boundary
            .iter()
            .map(|c| c.as_bytes().to_vec())
            .collect();
        // Build ancestors map from all known predecessor keys
        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|k| (pred.clone(), *k)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes =
            bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(
                Arc::clone(&kh_doc),
                &content_ref,
                &pred_refs,
                &envelope_bytes,
            )
            .await
            .map_err(|e| ferr!("encrypt fragment failed: {e}"))?;
        if let Some(update_op) = encrypted.update_op().cloned() {
            update_ops.push(update_op);
        }

        let env = EncryptedBlobEnvelope {
            encrypted: encrypted.encrypted_content().clone(),
        };
        let drisl_env = atproto_dasl::drisl::to_vec(&env)
            .map_err(|e| ferr!("DRISL encode encrypted blob envelope: {e}"))?;
        let mut encrypted_bytes = vec![0x02u8];
        encrypted_bytes.extend(drisl_env);

        key_index.insert(content_ref.clone(), app_key);

        let encrypted_blob = Blob::new(encrypted_bytes);
        let meta = BlobMeta::new(&encrypted_blob);
        new_fragments.push(Fragment::new(
            sedimentree_id,
            entry.head,
            entry.boundary.clone(),
            &entry.checkpoints,
            meta,
        ));
        encrypted_blobs.push(encrypted_blob);
    }

    // Encrypt loose commit blobs
    for (entry, blob) in ingested
        .loose_entries
        .iter()
        .zip(ingested.blobs.iter().skip(ingested.fragment_entries.len()))
    {
        let content_ref: Vec<u8> = entry.head.as_bytes().to_vec();
        let pred_refs: Vec<Vec<u8>> = entry
            .parents
            .iter()
            .map(|c| c.as_bytes().to_vec())
            .collect();

        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = pred_refs
            .iter()
            .filter_map(|pred| key_index.get(pred).map(|k| (pred.clone(), *k)))
            .collect();
        let envelope = Envelope {
            plaintext: blob.as_slice().to_vec(),
            ancestors,
        };
        let envelope_bytes =
            bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

        let (encrypted, app_key) = keyhive
            .try_encrypt_content_keyed(
                Arc::clone(&kh_doc),
                &content_ref,
                &pred_refs,
                &envelope_bytes,
            )
            .await
            .map_err(|e| ferr!("encrypt loose commit failed: {e}"))?;
        if let Some(update_op) = encrypted.update_op().cloned() {
            update_ops.push(update_op);
        }

        let env = EncryptedBlobEnvelope {
            encrypted: encrypted.encrypted_content().clone(),
        };
        let drisl_env = atproto_dasl::drisl::to_vec(&env)
            .map_err(|e| ferr!("DRISL encode encrypted blob envelope: {e}"))?;
        let mut encrypted_bytes = vec![0x02u8];
        encrypted_bytes.extend(drisl_env);

        key_index.insert(content_ref.clone(), app_key);

        let encrypted_blob = Blob::new(encrypted_bytes);
        let meta = BlobMeta::new(&encrypted_blob);
        new_loose_commits.push(LooseCommit::new(
            sedimentree_id,
            entry.head,
            entry.parents.clone(),
            meta,
        ));
        encrypted_blobs.push(encrypted_blob);
    }

    Ok((
        Sedimentree::new(new_fragments, new_loose_commits),
        encrypted_blobs,
        update_ops,
    ))
}

/// Encrypt a single loose commit blob via keyhive.
///
/// The caller provides `batch_keys` (CommitId → SymmetricKey) so we can build
/// the `Envelope.ancestors` map from all known parent keys.
async fn encrypt_loose_commit(
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    head: CommitId,
    parents: &BTreeSet<CommitId>,
    blob: &[u8],
    batch_keys: &std::collections::HashMap<CommitId, keyhive_crypto::symmetric_key::SymmetricKey>,
) -> Res<(Blob, keyhive_crypto::symmetric_key::SymmetricKey)> {
    let (blob, app_key, _) = encrypt_loose_commit_with_update_op(
        keyhive_handle,
        sedimentree_id,
        head,
        parents,
        blob,
        batch_keys,
    )
    .await?;
    Ok((blob, app_key))
}

async fn encrypt_loose_commit_with_update_op(
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    head: CommitId,
    parents: &BTreeSet<CommitId>,
    blob: &[u8],
    batch_keys: &std::collections::HashMap<CommitId, keyhive_crypto::symmetric_key::SymmetricKey>,
) -> Res<(
    Blob,
    keyhive_crypto::symmetric_key::SymmetricKey,
    Option<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>>,
)> {
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::symmetric_key::SymmetricKey;
    let keyhive = keyhive_handle.clone_keyhive();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for commit encryption"))?;
    let content_ref: Vec<u8> = head.as_bytes().to_vec();
    let pred_refs: Vec<Vec<u8>> = parents.iter().map(|c| c.as_bytes().to_vec()).collect();

    let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = {
        // Try batch_keys first, then fall back to known_decryption_keys
        // (populated by prior encrypts on the same Document)
        let doc_keys = kh_doc.lock().await.known_decryption_keys().clone();
        let ancestors: std::collections::HashMap<Vec<u8>, SymmetricKey> = parents
            .iter()
            .filter_map(|p| {
                let pref: Vec<u8> = p.as_bytes().to_vec();
                batch_keys
                    .get(p)
                    .map(|k| (pref.clone(), *k))
                    .or_else(|| doc_keys.get(&pref).map(|k| (pref, *k)))
            })
            .collect();
        ancestors
    };
    let envelope = Envelope {
        plaintext: blob.to_vec(),
        ancestors,
    };
    let envelope_bytes =
        bincode::serialize(&envelope).map_err(|e| ferr!("bincode encode envelope: {e}"))?;

    let (encrypted, app_key) = keyhive
        .try_encrypt_content_keyed(
            Arc::clone(&kh_doc),
            &content_ref,
            &pred_refs,
            &envelope_bytes,
        )
        .await
        .map_err(|e| ferr!("encrypt commit failed: {e}"))?;
    let update_op = encrypted.update_op().cloned();

    let env = EncryptedBlobEnvelope {
        encrypted: encrypted.encrypted_content().clone(),
    };
    let drisl_env = atproto_dasl::drisl::to_vec(&env)
        .map_err(|e| ferr!("DRISL encode encrypted blob envelope: {e}"))?;
    let mut encrypted_bytes = vec![0x02u8];
    encrypted_bytes.extend(drisl_env);

    Ok((Blob::new(encrypted_bytes), app_key, update_op))
}

async fn encrypt_fragment_blob<S>(
    keyhive_handle: &BigKeyhiveHandle,
    storage_for_reads: &S,
    sedimentree_id: SedimentreeId,
    head: CommitId,
    boundary: &BTreeSet<CommitId>,
    fragment_bytes: &[u8],
) -> Res<Blob>
where
    S: BigRepoSubductionStorage,
{
    use keyhive_core::crypto::envelope::Envelope;
    use keyhive_crypto::{siv::Siv, symmetric_key::SymmetricKey};

    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let keyhive = keyhive_handle.clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for fragment encryption"))?;

    let mut known_decryption_keys = kh_doc.lock().await.known_decryption_keys().clone();

    let head_ref: Vec<u8> = head.as_bytes().to_vec();
    let head_verified = <S as subduction_core::storage::traits::Storage<future_form::Sendable>>::load_loose_commit(
        storage_for_reads,
        sedimentree_id,
        head,
    )
    .await
    .map_err(|e| ferr!("failed loading loose commit for fragment encryption: {e}"))?
    .ok_or_else(|| {
        ferr!(
            "fragment head missing loose commit in storage: sedimentree_id={sedimentree_id:?} head={head:?}"
        )
    })?;
    let head_encrypted = decode_encrypted_blob(head_verified.blob().as_slice())?;
    let head_key: SymmetricKey = if let Some(key) = known_decryption_keys.get(&head_ref).copied() {
        key
    } else {
        let (_, key) = kh_doc
            .lock()
            .await
            .try_decrypt_content_keyed(&head_encrypted)
            .map_err(|e| ferr!("failed recovering fragment head key: {e}"))?;
        known_decryption_keys.insert(head_ref.clone(), key);
        key
    };

    let mut ancestors = std::collections::HashMap::with_capacity(boundary.len());
    for pred in boundary {
        let pred_ref: Vec<u8> = pred.as_bytes().to_vec();
        let pred_key = if let Some(key) = known_decryption_keys.get(&pred_ref).copied() {
            key
        } else {
            let pred_verified = <S as subduction_core::storage::traits::Storage<
                future_form::Sendable,
            >>::load_loose_commit(storage_for_reads, sedimentree_id, *pred)
                .await
                .map_err(|e| {
                    ferr!("failed loading fragment boundary loose commit for encryption: {e}")
                })?
                .ok_or_else(|| {
                    ferr!(
                        "fragment boundary missing loose commit in storage: sedimentree_id={sedimentree_id:?} head={pred:?}"
                    )
                })?;
            let pred_encrypted = decode_encrypted_blob(pred_verified.blob().as_slice())?;
            let (_, key) = kh_doc
                .lock()
                .await
                .try_decrypt_content_keyed(&pred_encrypted)
                .map_err(|e| ferr!("failed recovering fragment boundary key: {e}"))?;
            known_decryption_keys.insert(pred_ref.clone(), key);
            key
        };
        ancestors.insert(pred_ref, pred_key);
    }

    let envelope = Envelope {
        plaintext: fragment_bytes.to_vec(),
        ancestors,
    };
    let envelope_bytes = bincode::serialize(&envelope)
        .map_err(|e| ferr!("bincode encode fragment envelope: {e}"))?;
    let nonce_context = fragment_nonce_context(sedimentree_id, head, boundary);
    let nonce = Siv::new(&head_key, &envelope_bytes, &nonce_context);
    let mut ciphertext = envelope_bytes;
    head_key
        .try_encrypt(nonce, &mut ciphertext)
        .map_err(|e| ferr!("encrypt fragment payload failed: {e}"))?;

    let encrypted = beekem::encrypted::EncryptedContent::new(
        nonce,
        ciphertext,
        head_encrypted.pcs_key_hash,
        head_encrypted.pcs_update_op_hash,
        head_encrypted.content_ref.clone(),
        head_encrypted.pred_refs,
    );
    let env = EncryptedBlobEnvelope { encrypted };
    let drisl_env = atproto_dasl::drisl::to_vec(&env)
        .map_err(|e| ferr!("DRISL encode encrypted fragment blob envelope: {e}"))?;
    let mut encrypted_bytes = vec![0x02u8];
    encrypted_bytes.extend(drisl_env);

    Ok(Blob::new(encrypted_bytes))
}

fn fragment_nonce_context(
    sedimentree_id: SedimentreeId,
    head: CommitId,
    boundary: &BTreeSet<CommitId>,
) -> Vec<u8> {
    let mut context = b"big_repo.fragment.v1".to_vec();
    context.extend_from_slice(sedimentree_id.as_bytes());
    context.extend_from_slice(head.as_bytes());
    for boundary_head in boundary {
        context.extend_from_slice(boundary_head.as_bytes());
    }
    context
}

/// Advance the Keyhive document's local content frontier after durable storage.
///
/// This only updates the local document state used for later `after_content`
/// construction. It does not emit a Keyhive event.
async fn record_keyhive_content_frontier(
    keyhive_handle: &BigKeyhiveHandle,
    sedimentree_id: SedimentreeId,
    content_ref: Vec<u8>,
    pred_refs: Vec<Vec<u8>>,
) -> Res<()> {
    let keyhive = keyhive_handle.clone_keyhive();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
        .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| ferr!("keyhive doc not found for frontier recording"))?;
    keyhive
        .record_content_frontier(kh_doc, &content_ref, &pred_refs)
        .await;
    Ok(())
}

async fn persist_cgka_update_op(
    keyhive_storage: &BigRepoKeyhiveStorage,
    update_op: keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
) -> Res<()> {
    let event = StaticEvent::CgkaOperation(Box::new(update_op));
    subduction_keyhive::save_event::<Vec<u8>, _, future_form::Sendable>(keyhive_storage, &event)
        .await
        .map_err(|e| ferr!("failed to save keyhive cgka update op: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use keyhive_core::crypto::envelope::Envelope;
    use nonempty::nonempty;
    use sedimentree_core::blob::verified::VerifiedBlobMeta;
    use std::collections::BTreeSet;
    use subduction_core::storage::{memory::MemoryStorage, traits::Storage};
    use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};

    #[tokio::test]
    async fn ciphertext_store_loads_and_tombstones_stored_commit() -> Res<()> {
        let keyhive = BigKeyhiveHandle::boot_memory_from_seed([9; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let _doc_id = keyhive
            .create_doc(nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*_doc_id.as_bytes());
        let commit_id = CommitId::new([7; 32]);
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[42u8; 32]);
        let parents = BTreeSet::new();

        let (encrypted_blob, _app_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            commit_id,
            &parents,
            b"payload-bytes",
            &HashMap::new(),
        )
        .await?;

        let verified = VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<
            future_form::Sendable,
            _,
        >(
            &signer,
            (sedimentree_id, commit_id, parents),
            VerifiedBlobMeta::new(encrypted_blob.clone()),
        )
        .await;
        Storage::<future_form::Sendable>::save_loose_commit(&storage, sedimentree_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let adapter = BigRepoCiphertextStore::new(storage.clone());
        adapter
            .record_locator(
                commit_id.as_bytes().to_vec(),
                sedimentree_id,
                BigRepoCiphertextKind::LooseCommit,
            )
            .await?;

        let content_ref = commit_id.as_bytes().to_vec();
        let encrypted = adapter
            .get_ciphertext(&content_ref)
            .await
            .map_err(|e| ferr!("get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");
        assert_eq!(encrypted.content_ref, content_ref);

        adapter
            .mark_decrypted(&commit_id.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("mark_decrypted failed: {e}"))?;
        assert!(adapter
            .get_ciphertext(&commit_id.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("get_ciphertext after mark_decrypted failed: {e}"))?
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn ciphertext_store_prefers_fragment_over_loose_commit_for_same_ref() -> Res<()> {
        use sedimentree_core::fragment::Fragment;

        let keyhive = BigKeyhiveHandle::boot_memory_from_seed([11; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let _doc_id = keyhive
            .create_doc(nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*_doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[44u8; 32]);

        let h1 = CommitId::new([7; 32]);
        let h2 = CommitId::new([8; 32]);
        let h1_parents = BTreeSet::new();
        let h2_parents = BTreeSet::from([h1]);
        let h1_blob_bytes = b"loose-commit-payload";
        let fragment_bytes = b"fragment-payload";

        let (h1_blob, h1_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            h1,
            &h1_parents,
            h1_blob_bytes,
            &HashMap::new(),
        )
        .await?;
        let h1_verified = VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<
            future_form::Sendable,
            _,
        >(
            &signer,
            (sedimentree_id, h1, h1_parents.clone()),
            VerifiedBlobMeta::new(h1_blob),
        )
        .await;
        Storage::<future_form::Sendable>::save_loose_commit(&storage, sedimentree_id, h1_verified)
            .await
            .map_err(|e| ferr!("save_loose_commit for h1 failed: {e}"))?;

        let (h2_blob, h2_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            h2,
            &h2_parents,
            b"head-bytes",
            &HashMap::from([(h1, h1_key)]),
        )
        .await?;
        let h2_verified = VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<
            future_form::Sendable,
            _,
        >(
            &signer,
            (sedimentree_id, h2, h2_parents.clone()),
            VerifiedBlobMeta::new(h2_blob),
        )
        .await;
        Storage::<future_form::Sendable>::save_loose_commit(&storage, sedimentree_id, h2_verified)
            .await
            .map_err(|e| ferr!("save_loose_commit for h2 failed: {e}"))?;

        let fragment_blob = encrypt_fragment_blob(
            &keyhive,
            &storage,
            sedimentree_id,
            h2,
            &h2_parents,
            fragment_bytes,
        )
        .await?;
        let fragment_verified = VerifiedMeta::<Fragment>::seal::<future_form::Sendable, _>(
            &signer,
            (sedimentree_id, h2, h2_parents.clone(), vec![]),
            VerifiedBlobMeta::new(fragment_blob),
        )
        .await;
        Storage::<future_form::Sendable>::save_fragment(
            &storage,
            sedimentree_id,
            fragment_verified,
        )
        .await
        .map_err(|e| ferr!("save_fragment failed: {e}"))?;

        let adapter = BigRepoCiphertextStore::new(storage.clone());
        adapter
            .record_locator(
                h2.as_bytes().to_vec(),
                sedimentree_id,
                BigRepoCiphertextKind::LooseCommit,
            )
            .await?;
        adapter
            .record_locator(
                h2.as_bytes().to_vec(),
                sedimentree_id,
                BigRepoCiphertextKind::Fragment,
            )
            .await?;

        let loose_locator = BigRepoCiphertextLocator {
            kind: BigRepoCiphertextKind::LooseCommit,
            sedimentree_id,
            commit_id: h2,
        };
        let loose_raw = adapter
            .load_raw_for_locator(&loose_locator)
            .await
            .map_err(|e| ferr!("load_raw_for_locator for loose commit failed: {e}"))?
            .expect("loose commit raw should exist");
        let loose_encrypted = decode_encrypted_blob(loose_raw.as_slice())?;
        let loose_decrypted = loose_encrypted
            .try_decrypt(h2_key)
            .map_err(|e| ferr!("decrypting loose locator failed: {e}"))?;
        let loose_envelope: Envelope<Vec<u8>, Vec<u8>> = bincode::deserialize(&loose_decrypted)
            .map_err(|e| ferr!("decode loose locator envelope: {e}"))?;
        assert_eq!(loose_envelope.plaintext, b"head-bytes");

        let fragment_locator = BigRepoCiphertextLocator {
            kind: BigRepoCiphertextKind::Fragment,
            sedimentree_id,
            commit_id: h2,
        };
        let fragment_raw = adapter
            .load_raw_for_locator(&fragment_locator)
            .await
            .map_err(|e| ferr!("load_raw_for_locator for fragment failed: {e}"))?
            .expect("fragment raw should exist");
        let fragment_encrypted = decode_encrypted_blob(fragment_raw.as_slice())?;
        let fragment_decrypted = fragment_encrypted
            .try_decrypt(h2_key)
            .map_err(|e| ferr!("decrypting fragment locator failed: {e}"))?;
        let fragment_envelope: Envelope<Vec<u8>, Vec<u8>> =
            bincode::deserialize(&fragment_decrypted)
                .map_err(|e| ferr!("decode fragment locator envelope: {e}"))?;
        assert_eq!(fragment_envelope.plaintext, fragment_bytes);

        let encrypted = adapter
            .get_ciphertext(&h2.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");
        let decrypted = encrypted
            .try_decrypt(h2_key)
            .map_err(|e| ferr!("decrypting preferred fragment failed: {e}"))?;
        let envelope: Envelope<Vec<u8>, Vec<u8>> = bincode::deserialize(&decrypted)
            .map_err(|e| ferr!("decode preferred fragment envelope: {e}"))?;
        assert_eq!(envelope.plaintext, fragment_bytes);
        assert_eq!(envelope.ancestors.get(&h1.as_bytes()[..]), Some(&h1_key));
        Ok(())
    }

    #[tokio::test]
    async fn fragment_encryption_reuses_existing_content_key() -> Res<()> {
        let keyhive = BigKeyhiveHandle::boot_memory_from_seed([10; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let doc_id = keyhive
            .create_doc(nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[43u8; 32]);

        let h1 = CommitId::new([7; 32]);
        let h2 = CommitId::new([8; 32]);
        let h1_parents = BTreeSet::new();
        let h2_parents = BTreeSet::from([h1]);

        let (h1_blob, h1_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            h1,
            &h1_parents,
            b"parent-bytes",
            &std::collections::HashMap::new(),
        )
        .await?;
        let h1_verified = VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<
            future_form::Sendable,
            _,
        >(
            &signer,
            (sedimentree_id, h1, h1_parents.clone()),
            VerifiedBlobMeta::new(h1_blob),
        )
        .await;
        Storage::<future_form::Sendable>::save_loose_commit(&storage, sedimentree_id, h1_verified)
            .await
            .map_err(|e| ferr!("save_loose_commit for h1 failed: {e}"))?;

        let (h2_blob, h2_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            h2,
            &h2_parents,
            b"head-bytes",
            &std::collections::HashMap::from([(h1, h1_key)]),
        )
        .await?;
        let h2_verified = VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<
            future_form::Sendable,
            _,
        >(
            &signer,
            (sedimentree_id, h2, h2_parents.clone()),
            VerifiedBlobMeta::new(h2_blob),
        )
        .await;
        Storage::<future_form::Sendable>::save_loose_commit(&storage, sedimentree_id, h2_verified)
            .await
            .map_err(|e| ferr!("save_loose_commit for h2 failed: {e}"))?;

        let fragment_plaintext = b"fragment-bundle-bytes";
        let encrypted_blob = encrypt_fragment_blob(
            &keyhive,
            &storage,
            sedimentree_id,
            h2,
            &h2_parents,
            fragment_plaintext,
        )
        .await?;
        assert_eq!(encrypted_blob.as_slice().first(), Some(&0x02));

        let encrypted = decode_encrypted_blob(encrypted_blob.as_slice())?;
        assert_eq!(encrypted.content_ref, h2.as_bytes().to_vec());
        let decrypted = encrypted
            .try_decrypt(h2_key)
            .map_err(|e| ferr!("decrypting fragment blob failed: {e}"))?;
        let envelope: Envelope<Vec<u8>, Vec<u8>> =
            bincode::deserialize(&decrypted).map_err(|e| ferr!("decode fragment envelope: {e}"))?;
        assert_eq!(envelope.plaintext, fragment_plaintext);
        assert_eq!(envelope.ancestors.get(&h1.as_bytes()[..]), Some(&h1_key));
        Ok(())
    }

    #[tokio::test]
    async fn load_doc_snapshot_rejects_plaintext_blob() -> Res<()> {
        let keyhive = BigKeyhiveHandle::boot_memory_from_seed([12; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let doc_id = keyhive
            .create_doc(nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[45u8; 32]);
        let commit_id = CommitId::new([9; 32]);
        let parents = BTreeSet::new();
        let plaintext_blob = Blob::new(b"plain commit bytes".to_vec());
        let verified = VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<
            future_form::Sendable,
            _,
        >(
            &signer,
            (sedimentree_id, commit_id, parents),
            VerifiedBlobMeta::new(plaintext_blob),
        )
        .await;
        Storage::<future_form::Sendable>::save_loose_commit(&storage, sedimentree_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let sedimentrees =
            Arc::new(subduction_core::collections::bounded_sharded_map::BoundedShardedMap::new());
        let err = load_doc_snapshot(&sedimentrees, &storage, &keyhive, doc_id)
            .await
            .expect_err("plaintext blob should be rejected");
        assert!(err.to_string().contains("blob is not encrypted"));
        Ok(())
    }
}
