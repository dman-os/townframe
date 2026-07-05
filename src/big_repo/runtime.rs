//! This is just a crappy reimpl of samod to get subduction working

use crate::interlude::*;

use crate::{
    encrypted_blob::{decode_encrypted_blob, encode_encrypted_blob},
    ephemeral::{
        BigEphemeral, BigEphemeralBackend, BigEphemeralFilter, BigEphemeralSwitchboard,
        BigEphemeralTopic, BigRepoEphemeralBackend,
    },
    handler::{
        BigRepoComposedHandler, BigRepoEphemeralHandler, BigRepoKeyhiveHandler,
        BigRepoKeyhiveProtocol,
    },
    keyhive_conn::BigRepoKeyhiveConnAdapter,
    keyhive_storage::BigRepoKeyhiveStorage,
    BigKeyhiveAuthority, BigKeyhiveHandle, BigRepoChangeOrigin, ConnFinishSignal, DocumentId,
    PeerId,
};
use keyhive_core::event::static_event::StaticEvent;
use subduction_keyhive::{KeyhiveConnection, KeyhivePeerId};
use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

use future_form::{FutureForm, Sendable};
use futures::future::BoxFuture;
use keyhive_core::store::ciphertext::CiphertextStore;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::sedimentree::{Sedimentree, SedimentreeItem};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use subduction_ephemeral::{
    clock::std_clock::StdClock, config::EphemeralConfig, handler::EphemeralHandler,
    policy::OpenEphemeralPolicy,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::changes;
use subduction_core::{authenticated::Authenticated, subduction::request::FragmentRequested};

const DEFAULT_DOC_WORKER_IDLE_TTL: Duration = Duration::from_secs(3);
const DEFAULT_DOC_SYNC_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SUBDUCTION_NONCE_TTL: Duration = Duration::from_secs(60);
const DEFAULT_SUBDUCTION_DEFAULT_ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BigRepoSyncPolicy {
    pub(crate) doc_worker_idle_ttl: Duration,
    pub(crate) doc_sync_timeout: Duration,
    pub(crate) subduction_nonce_ttl: Duration,
    pub(crate) subduction_default_roundtrip_timeout: Duration,
}

impl Default for BigRepoSyncPolicy {
    fn default() -> Self {
        Self {
            doc_worker_idle_ttl: DEFAULT_DOC_WORKER_IDLE_TTL,
            doc_sync_timeout: DEFAULT_DOC_SYNC_TIMEOUT,
            subduction_nonce_ttl: DEFAULT_SUBDUCTION_NONCE_TTL,
            subduction_default_roundtrip_timeout: DEFAULT_SUBDUCTION_DEFAULT_ROUNDTRIP_TIMEOUT,
        }
    }
}
type SharedPartitionStore = Arc<dyn big_sync::HostPartStore>;
type PendingKeyhiveSyncWaiters = HashMap<PeerId, Vec<(u64, oneshot::Sender<Res<()>>)>>;
type PendingDocSyncWaiters = HashMap<PeerId, Vec<(u64, oneshot::Sender<Result<(), SyncDocError>>)>>;

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum SyncDocError {
    /// Document not found
    NotFound,
    /// TransportError
    TransportError,
    /// IoError
    IoError(eyre::Report),
    /// Unexpected {0}
    Other(#[from] eyre::Report),
}

#[derive(Debug, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
pub enum GetDocError {
    /// document {0} is not found
    NotFound(DocumentId),
    /// document {0} is pending materialization
    PendingMaterialization(DocumentId),
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

    pub fn into_ready(self, doc_id: DocumentId) -> Result<T, GetDocError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Missing => Err(GetDocError::NotFound(doc_id)),
            Self::PendingMaterialization => Err(GetDocError::PendingMaterialization(doc_id)),
        }
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
    #[cfg(test)]
    InspectStoredDocBlobs {
        doc_id: DocumentId,
        resp: oneshot::Sender<Res<Vec<Vec<u8>>>>,
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
    SyncKeyhiveWithPeerInternal {
        peer_id: PeerId,
    },
    NoteLocalKeyhiveChanged {
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
    KeyhiveSyncRequested {
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
    DocWorkerMaterializationPending {
        doc_id: DocumentId,
    },
    DocWorkerMaterializationReady {
        doc_id: DocumentId,
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
    sync_policy: BigRepoSyncPolicy,
    #[educe(Debug(ignore))]
    doc_sync_waiter_ids: Arc<AtomicU64>,
    #[educe(Debug(ignore))]
    keyhive_sync_waiter_ids: Arc<AtomicU64>,
}

impl BigRepoRuntimeHandle {
    pub async fn create_doc(
        &self,
        initial_content: automerge::Automerge,
        parents: Vec<BigKeyhiveAuthority>,
    ) -> Result<Arc<LiveDocBundle>, CreateDocError> {
        use nonempty::NonEmpty;
        let heads = initial_content.get_heads();
        let content_heads = NonEmpty::from_vec(heads.iter().map(|h| h.0).collect())
            .ok_or_else(|| eyre::eyre!("automerge doc has no heads"))?;
        let doc_id = self
            .keyhive
            .create_doc(parents, content_heads, &self.keyhive_storage)
            .await?;
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

    pub async fn get_doc_handle(&self, doc_id: DocumentId) -> Res<DocLookup<Arc<LiveDocBundle>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::GetDocHandle { doc_id, resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        let out = rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?;
        out
    }

    pub async fn note_local_keyhive_changed(&self) -> Res<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::NoteLocalKeyhiveChanged { resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
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
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn inspect_stored_doc_blobs(&self, doc_id: DocumentId) -> Res<Vec<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RuntimeCmd::InspectStoredDocBlobs { doc_id, resp: tx })
            .map_err(|_| eyre::eyre!(ERROR_ACTOR))?;
        rx.await.map_err(|_| eyre::eyre!(ERROR_CHANNEL))?
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
    Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    Vec<u8>,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<Vec<u8>, Vec<u8>>,
    keyhive_core::listener::no_listener::NoListener,
    rand_08::rngs::OsRng,
>;
pub trait BigRepoSubductionStorage:
    subduction_core::storage::traits::Storage<
        Sendable,
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
            Sendable,
            Error: std::fmt::Display + Send + Sync + 'static,
        > + Clone
        + Send
        + Sync
        + std::fmt::Debug
        + 'static
{
}

/// The concrete Subduction type for BigRepo, using the composed handler.
type BigRepoSubduction<S> = subduction_core::subduction::Subduction<
    'static,
    Sendable,
    S,
    BigRepoIrohTransport,
    BigRepoComposedHandler<S>,
    crate::runtime::BigRepoPolicy,
    subduction_crypto::signer::memory::MemorySigner,
    TimeoutTokio,
    TokioSpawn,
    CountLeadingZeroBytes,
    256,
>;

#[derive(Clone)]
struct BigRepoSyncSessionBridge {
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
}

impl subduction_core::sync_session::SyncSessionObserver for BigRepoSyncSessionBridge {
    fn on_sync_session(&self, session: subduction_core::sync_session::SyncSession) {
        if self
            .evt_tx
            .send(RuntimeEvt::SyncSessionObserved { session })
            .is_err()
        {
            warn!("runtime shutting down; dropping observed sync session");
        }
    }
}
pub async fn spawn_big_repo_runtime<S>(
    signer: subduction_crypto::signer::memory::MemorySigner,
    storage: S,
    policy: Arc<BigRepoPolicy>,
    sync_policy: BigRepoSyncPolicy,
    keyhive: BigKeyhiveHandle,
    keyhive_storage: BigRepoKeyhiveStorage,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
) -> Res<(BigRepoRuntimeHandle, BigEphemeral, BigRepoRuntimeStopToken)>
where
    S: BigRepoSubductionStorage,
{
    use subduction_core::subduction::Subduction;
    use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

    let connect_signer = signer.clone();
    let local_peer_id =
        subduction_core::peer::id::PeerId::new(*connect_signer.verifying_key().as_bytes());
    let nonce_cache = Arc::new(subduction_core::nonce_cache::NonceCache::new(
        sync_policy.subduction_nonce_ttl,
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
    let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
        Arc::clone(&connections),
        OpenEphemeralPolicy,
        EphemeralConfig::default(),
        StdClock,
    );
    let ephemeral_handler: BigRepoEphemeralHandler = Arc::new(ephemeral_handler);
    let ephemeral_backend: Arc<dyn BigEphemeralBackend> = Arc::new(BigRepoEphemeralBackend::new(
        signer.clone(),
        Arc::clone(&ephemeral_handler),
    ));
    let ephemeral_switchboard = BigEphemeralSwitchboard::spawn(
        Arc::clone(&ephemeral_backend),
        ephemeral_rx,
        runtime_stop.clone(),
        Arc::clone(&runtime_tasks),
    );
    let ephemeral = BigEphemeral::new(Arc::clone(&ephemeral_backend), ephemeral_switchboard);

    // Boot keyhive protocol and handler
    let (keyhive_protocol, keyhive_handler) = {
        let contact_card = keyhive.contact_card().clone();
        let kh_peer_id = keyhive.keyhive_peer_id();
        let keyhive_protocol: BigRepoKeyhiveProtocol = Arc::new(
            subduction_keyhive::KeyhiveProtocol::new(
                Arc::clone(&keyhive.clone_keyhive()),
                keyhive_storage.clone(),
                kh_peer_id,
                contact_card,
            )
            .with_storage_recovery(),
        );

        let mut keyhive_handler = BigRepoKeyhiveHandler::new(
            Arc::clone(&keyhive_protocol),
            BigRepoKeyhiveConnAdapter::new
                as fn(Authenticated<BigRepoIrohTransport, Sendable>) -> BigRepoKeyhiveConnAdapter,
        );
        keyhive_handler = keyhive_handler.with_sync_done_observer({
            let evt_tx = evt_tx.clone();
            Arc::new(move |peer_id| {
                let peer_id = PeerId::new(*peer_id.verifying_key());
                if evt_tx
                    .send(RuntimeEvt::KeyhiveSyncDone { peer_id })
                    .is_err()
                {
                    tracing::debug!(%peer_id, "runtime stopped before keyhive sync-done event");
                }
            })
        });
        (keyhive_protocol, keyhive_handler)
    };

    let mut keyhive_change_subscription = ephemeral
        .subscribe(BigEphemeralFilter::new(BigEphemeralTopic::keyhive_changed()))
        .await?;
    runtime_tasks
        .spawn({
            let stop = runtime_stop.child_token();
            let cmd_tx = cmd_tx.clone();
            async move {
                loop {
                    let Some(event) = stop
                        .run_until_cancelled(keyhive_change_subscription.recv())
                        .await
                    else {
                        break;
                    };
                    let Some(event) = event else {
                        break;
                    };
                    if cmd_tx
                        .send(RuntimeCmd::SyncKeyhiveWithPeerInternal {
                            peer_id: PeerId::new(*event.sender.as_bytes()),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(tracing::info_span!("BigRepo keyhive change listener"))
        })
        .expect(ERROR_TOKIO);
    let composed_handler = Arc::new(BigRepoComposedHandler::new(
        sync_handler,
        Arc::clone(&ephemeral_handler),
        keyhive_handler,
    ));

    let (subduction, listener, manager) = Subduction::new(
        composed_handler,
        None,
        signer,
        Arc::clone(&sedimentrees),
        Arc::clone(&connections),
        Arc::clone(&subscriptions),
        storage_powerbox,
        send_counter,
        subduction_core::nonce_cache::NonceCache::new(sync_policy.subduction_nonce_ttl),
        TimeoutTokio,
        sync_policy.subduction_default_roundtrip_timeout,
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
        ephemeral_backend: Arc::clone(&ephemeral_backend),
        ephemeral: ephemeral.clone(),
        sedimentrees: Arc::clone(&sedimentrees),
        storage_for_reads,
        big_sync_store: big_sync_store_for_worker,
        change_manager,
        runtime_tasks: Arc::clone(&runtime_tasks),
        connect_signer,
        local_peer_id,
        nonce_cache,
        sync_policy,
        runtime_stop: runtime_stop.clone(),
        cmd_tx: cmd_tx.clone(),
        evt_tx: evt_tx.clone(),
        connected_peers: Arc::new(surelock::mutex::Mutex::new(HashMap::new())),
        doc_sync_waiter_ids: Arc::clone(&doc_sync_waiter_ids),
        keyhive_sync_waiter_ids: Arc::clone(&keyhive_sync_waiter_ids),
        pending_keyhive_syncs: default(),
        active_keyhive_syncs: default(),
        keyhive_dirty: default(),
        pending_materialization: default(),
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
                compact_tick.tick().await;
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
                        runtime_worker.handle_cmd(cmd)?;
                    },
                    evt = evt_rx.recv() => {
                        let Some(evt) = evt else { break; };
                        runtime_worker.handle_evt(evt)?;
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
            sync_policy,
            doc_sync_waiter_ids: Arc::clone(&doc_sync_waiter_ids),
            keyhive_sync_waiter_ids: Arc::clone(&keyhive_sync_waiter_ids),
        },
        ephemeral,
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
    ephemeral_backend: Arc<dyn BigEphemeralBackend>,
    ephemeral: BigEphemeral,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    connect_signer: subduction_crypto::signer::memory::MemorySigner,
    local_peer_id: subduction_core::peer::id::PeerId,
    nonce_cache: Arc<subduction_core::nonce_cache::NonceCache>,
    sync_policy: BigRepoSyncPolicy,
    runtime_stop: CancellationToken,
    cmd_tx: mpsc::UnboundedSender<RuntimeCmd>,
    evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    connected_peers: Arc<surelock::mutex::Mutex<HashMap<PeerId, RuntimePeerConnDeets>>>,
    doc_sync_waiter_ids: Arc<AtomicU64>,
    keyhive_sync_waiter_ids: Arc<AtomicU64>,
    pending_keyhive_syncs: PendingKeyhiveSyncWaiters,
    active_keyhive_syncs: BTreeSet<PeerId>,
    keyhive_dirty: BTreeSet<PeerId>,
    pending_materialization: HashSet<DocumentId>,
    doc_workers: HashMap<DocumentId, DocWorkerEntry>,
}

impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    fn handle_cmd(&mut self, cmd: RuntimeCmd) -> Res<()> {
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
                self.handle_open_conn_iroh(endpoint, endpoint_addr, peer_id, end_signal_tx, done);
            }
            RuntimeCmd::AcceptConnIroh {
                conn,
                end_signal_tx,
                resp: done,
            } => {
                self.handle_accept_conn_iroh(conn, end_signal_tx, done);
            }
            RuntimeCmd::CloseConnIroh {
                peer_id,
                resp: done,
            } => {
                self.handle_close_iroh(peer_id, done);
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
                let msg = DocWorkerMsg::SyncWithPeer {
                    peer_id,
                    waiter_id,
                    timeout,
                    done,
                };
                if let Err(err) = worker.send_msg(msg) {
                    self.handle_doc_worker_transient_finished(doc_id);
                    let DocWorkerMsg::SyncWithPeer { done, .. } = err.0 else {
                        unreachable!("send_msg returned a different doc worker message");
                    };
                    done.send(Err(SyncDocError::IoError(ferr!(
                        "doc worker stopped before sync request"
                    ))))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
                }
            }
            #[cfg(test)]
            RuntimeCmd::InspectStoredDocBlobs { doc_id, resp } => {
                let storage = self.storage_for_reads.clone();
                self.spawn_background(async move {
                    let sedimentree_id = SedimentreeId::new(doc_id.into_bytes());
                    let res = async {
                        let loose_commits = <S as subduction_core::storage::traits::Storage<
                            Sendable,
                        >>::load_loose_commits(&storage, sedimentree_id)
                        .await
                        .wrap_err("failed loading loose commits for inspection")?;
                        let fragments = <S as subduction_core::storage::traits::Storage<
                            Sendable,
                        >>::load_fragments(&storage, sedimentree_id)
                        .await
                        .wrap_err("failed loading fragments for inspection")?;
                        let mut out = Vec::with_capacity(loose_commits.len() + fragments.len());
                        out.extend(
                            loose_commits
                                .into_iter()
                                .map(|verified| verified.blob().clone().into_contents()),
                        );
                        out.extend(
                            fragments
                                .into_iter()
                                .map(|verified| verified.blob().clone().into_contents()),
                        );
                        Res::Ok(out)
                    }
                    .await;
                    resp.send(res).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                });
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
                tracing::debug!("SyncKeyhiveWithPeer: scheduling sync");
                self.pending_keyhive_syncs
                    .entry(peer_id)
                    .or_default()
                    .push((waiter_id, resp));
                if !self.active_keyhive_syncs.contains(&peer_id) {
                    let _ = self.start_keyhive_sync(peer_id);
                }
            }
            RuntimeCmd::SyncKeyhiveWithPeerInternal { peer_id } => {
                self.schedule_internal_keyhive_sync(peer_id);
            }
            RuntimeCmd::NoteLocalKeyhiveChanged { resp } => {
                let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
                let ephemeral = self.ephemeral.clone();
                self.spawn_background(async move {
                    let out = async {
                        keyhive_protocol
                            .note_local_keyhive_changed()
                            .await
                            .wrap_err("keyhive local-change refresh failed")?;
                        ephemeral
                            .publish(BigEphemeralTopic::keyhive_changed(), Vec::new())
                            .await
                            .wrap_err("keyhive change notification publish failed")?;
                        Res::Ok(())
                    }
                    .await;
                    if resp.send(out).is_err() {
                        warn!("failed sending keyhive local-change response");
                    }
                });
            }
            RuntimeCmd::CancelKeyhiveSyncWaiter { peer_id, waiter_id } => {
                let _ = self.cancel_pending_keyhive_sync(peer_id, waiter_id);
            }
            RuntimeCmd::ReleaseDocLease { doc_id } => self.handle_release_doc_lease(doc_id),
        }

        Ok(())
    }

    fn handle_evt(&mut self, evt: RuntimeEvt) -> Res<()> {
        match evt {
            RuntimeEvt::SyncSessionObserved { session } => {
                self.handle_sync_session_observed(session);
            }
            RuntimeEvt::ConnEstablishedIroh {
                peer_id,
                deets: cancel_token,
            } => {
                self.handle_connection_established(peer_id, cancel_token);
            }
            RuntimeEvt::ConnLostIroh {
                peer_id,
                error,
                src_task,
            } => {
                self.handle_connection_lost(peer_id, error, src_task)?;
            }
            RuntimeEvt::KeyhiveSyncDone { peer_id } => {
                self.finish_keyhive_sync(peer_id)?;
            }
            RuntimeEvt::KeyhiveSyncRequested { peer_id } => {
                self.schedule_internal_keyhive_sync(peer_id);
            }
            RuntimeEvt::DocWorkerTransientFinished { doc_id } => {
                self.handle_doc_worker_transient_finished(doc_id);
            }
            RuntimeEvt::DocWorkerHandleAcquired { bundle } => {
                self.handle_doc_worker_handle_acquired(bundle);
            }
            RuntimeEvt::DocWorkerStopped { doc_id } => {
                self.doc_workers.remove(&doc_id);
            }
            RuntimeEvt::FatalWorkerError {
                doc_id,
                context,
                error,
            } => {
                panic!("fatal runtime worker error doc={doc_id:?} context={context}: {error}");
            }
            RuntimeEvt::DocWorkerMaterializationPending { doc_id } => {
                self.pending_materialization.insert(doc_id);
            }
            RuntimeEvt::DocWorkerMaterializationReady { doc_id } => {
                self.pending_materialization.remove(&doc_id);
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

    fn schedule_doc_worker_eviction_if_idle(&mut self, doc_id: DocumentId) {
        let Some(entry) = self.doc_workers.get_mut(&doc_id) else {
            return;
        };
        if entry.local_handles > 0
            || entry.transient_work > 0
            || self.pending_materialization.contains(&doc_id)
        {
            entry.eviction_deadline = None;
            return;
        }
        entry.eviction_deadline = Some(Instant::now() + self.sync_policy.doc_worker_idle_ttl);
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

    fn cancel_pending_doc_syncs(&mut self, peer_id: PeerId, reason: &'static str) -> Res<()> {
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
        if let Some(waiters) = self.pending_keyhive_syncs.get_mut(&peer_id) {
            let pos = waiters.iter().position(|(pending_id, _)| *pending_id == waiter_id);
            if let Some(pos) = pos {
                let (_, waiter) = waiters.remove(pos);
                if waiters.is_empty() {
                    self.pending_keyhive_syncs.remove(&peer_id);
                }
                return Some(waiter);
            }
        }
        None
    }



    #[tracing::instrument(skip_all,fields(%doc_id))]
    fn spawn_doc_worker(&mut self, doc_id: DocumentId) -> Res<()> {
        if self
            .doc_workers
            .get(&doc_id)
            .is_some_and(|entry| !entry.handle.is_closed())
        {
            if let Some(entry) = self.doc_workers.get_mut(&doc_id) {
                entry.eviction_deadline = None;
            }
            return Ok(());
        }
        self.doc_workers.remove(&doc_id);
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
        let worker_msg_tx = msg_tx.clone();
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
            subduction: Arc::clone(&self.subduction),
            sedimentrees: Arc::clone(&self.sedimentrees),
            storage_for_reads: self.storage_for_reads.clone(),
            ciphertext_store: BigRepoCiphertextStore::new(
                self.storage_for_reads.clone(),
                doc_id_subduction,
            ),
            keyhive_storage: self.keyhive_storage.clone(),
            big_sync_store: Arc::clone(&self.big_sync_store),
            change_manager: Arc::clone(&self.change_manager),
            runtime_handle: BigRepoRuntimeHandle {
                cmd_tx: self.cmd_tx.clone(),
                keyhive: self.keyhive.clone(),
                keyhive_storage: self.keyhive_storage.clone(),
                sync_policy: self.sync_policy,
                doc_sync_waiter_ids: Arc::clone(&self.doc_sync_waiter_ids),
                keyhive_sync_waiter_ids: Arc::clone(&self.keyhive_sync_waiter_ids),
            },
            msg_tx: worker_msg_tx,
            runtime_tasks: Arc::clone(&runtime_tasks),
            runtime_evt_tx,
            last_notified_heads: None,
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
                eviction_deadline: Some(Instant::now() + self.sync_policy.doc_worker_idle_ttl),
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

    fn handle_release_doc_lease(&mut self, doc_id: DocumentId) {
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
    fn handle_sync_session_observed(
        &mut self,
        session: subduction_core::sync_session::SyncSession,
    ) {
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
}

// keyhive support
impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    fn cancel_pending_keyhive_syncs(&mut self, peer_id: PeerId, reason: &'static str) {
        self.active_keyhive_syncs.remove(&peer_id);
        self.keyhive_dirty.remove(&peer_id);
        if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
            for (_, waiter) in waiters {
                waiter
                    .send(Err(ferr!("{reason}")))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
    }

    fn finish_keyhive_sync(&mut self, peer_id: PeerId) -> Res<()> {
        if !self.active_keyhive_syncs.remove(&peer_id) {
            debug!(%peer_id, "ignoring untracked keyhive sync completion");
            return Ok(());
        }
        if let Some(waiters) = self.pending_keyhive_syncs.remove(&peer_id) {
            for (_, waiter) in waiters {
                waiter
                    .send(Ok(()))
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        self.spawn_background(async move {
            let _ = keyhive_protocol.refresh_cache().await;
        });
        if self.keyhive_dirty.remove(&peer_id) {
            self.start_keyhive_sync(peer_id)?;
        }
        for doc_id in self.pending_materialization.clone() {
            match self.doc_worker_handle(doc_id) {
                Ok(worker) => {
                    worker
                        .send(DocWorkerMsg::ReattemptMaterialization)
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                }
                Err(e) => {
                    tracing::warn!(
                        %doc_id, error=%e,
                        "failed to get doc worker for reattempt on keyhive sync done"
                    );
                }
            }
        }
        Ok(())
    }

    fn start_keyhive_sync(&mut self, peer_id: PeerId) -> Res<()> {
        let was_idle = self.active_keyhive_syncs.insert(peer_id);
        if !was_idle {
            return Ok(());
        }
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        let kh_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
        self.spawn_background(async move {
            if let Err(err) = keyhive_protocol.initiate_sync_with_peer(&kh_peer_id).await {
                tracing::warn!(%peer_id, error=%err, "keyhive initiate_sync_with_peer failed");
            }
        });
        Ok(())
    }

    fn schedule_internal_keyhive_sync(&mut self, peer_id: PeerId) {
        let connected = surelock::key::lock_scope(|key| {
            let (m, _) = key.lock(&self.connected_peers);
            m.contains_key(&peer_id)
        });
        if !connected {
            debug!(%peer_id, "dropping internal keyhive sync for disconnected peer");
            return;
        }
        if self.active_keyhive_syncs.contains(&peer_id) {
            self.keyhive_dirty.insert(peer_id);
        } else {
            let _ = self.start_keyhive_sync(peer_id);
        }
    }
}

// connections support
impl<S> BigRepoRuntimeWorker<S>
where
    S: BigRepoSubductionStorage,
{
    #[tracing::instrument(skip_all)]
    fn handle_open_conn_iroh(
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
        let ephemeral_backend = Arc::clone(&self.ephemeral_backend);
        let kh_proto = Arc::clone(&self.keyhive_protocol);
        let fut = async move {
            let existing = surelock::key::lock_scope(|key| {
                let (guard, _key) = key.lock(&connected_peers);
                guard.get(&peer_id).map(|deets| Arc::clone(&deets.closed))
            });
            if let Some(closed) = existing {
                return Ok((peer_id, closed));
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
            ephemeral_backend
                .subscribe_peer(subduction_core::peer::id::PeerId::new(*peer_id.as_bytes()))
                .await;
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
            evt_tx
                .send(RuntimeEvt::KeyhiveSyncRequested { peer_id })
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
    fn handle_accept_conn_iroh(
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
        let ephemeral_backend = Arc::clone(&self.ephemeral_backend);
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
            ephemeral_backend
                .subscribe_peer(subduction_core::peer::id::PeerId::new(*peer_id.as_bytes()))
                .await;
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
            evt_tx
                .send(RuntimeEvt::KeyhiveSyncRequested { peer_id })
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
    fn handle_close_iroh(&mut self, peer_id: PeerId, done: Option<oneshot::Sender<Res<()>>>) {
        self.cancel_pending_keyhive_syncs(peer_id, "keyhive peer closed");
        self.cancel_pending_doc_syncs(peer_id, "doc sync peer closed")
            .expect(ERROR_ACTOR);
        let deets = surelock::key::lock_scope(|key| {
            let (mut g, _) = key.lock(&self.connected_peers);
            g.remove(&peer_id)
        });
        if let Some(ref deets) = deets {
            deets.closed.store(true, Ordering::SeqCst);
            deets.cancel_token.cancel();
        }
        let subduction = Arc::clone(&self.subduction);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        self.spawn_background(async move {
            let out: Res<()> = async {
                keyhive_protocol
                    .remove_peer(&KeyhivePeerId::from_bytes(*peer_id.as_bytes()))
                    .await;
                if deets.is_some() {
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
    fn handle_connection_lost(
        &mut self,
        peer_id: PeerId,
        err: Option<subduction_iroh::error::RunError>,
        _task: ConnTask,
    ) -> Res<()> {
        self.cancel_pending_keyhive_syncs(peer_id, "keyhive connection lost");
        self.cancel_pending_doc_syncs(peer_id, "doc sync connection lost")?;
        let deets = surelock::key::lock_scope(|key| {
            let (mut g, _) = key.lock(&self.connected_peers);
            g.remove(&peer_id)
        });
        if let Some(ref deets) = deets {
            deets.closed.store(true, Ordering::SeqCst);
            deets.cancel_token.cancel();
        }
        let subduction = Arc::clone(&self.subduction);
        let keyhive_protocol = Arc::clone(&self.keyhive_protocol);
        self.spawn_background(async move {
            let out: Res<()> = async {
                keyhive_protocol
                    .remove_peer(&KeyhivePeerId::from_bytes(*peer_id.as_bytes()))
                    .await;
                if let Some(deets) = deets {
                    let remote_peer_id =
                        subduction_core::peer::id::PeerId::new(peer_id.into_bytes());
                    subduction
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
                Res::Ok(())
            }
            .await;
            if let Err(e) = out {
                tracing::warn!(%peer_id, error=%e, "connection lost teardown failed");
            }
        });
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(%peer_id))]
    fn handle_connection_established(&self, peer_id: PeerId, deets: RuntimePeerConnDeets) {
        let kh_proto = Arc::clone(&self.keyhive_protocol);
        self.spawn_background(async move {
            kh_proto
                .clear_syncpoint_for_peer(&KeyhivePeerId::from_bytes(*peer_id.as_bytes()))
                .await;
        });
        let old = surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.connected_peers);
            guard.insert(peer_id, deets)
        });
        if let Some(previous) = old {
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

    fn send_msg(&self, msg: DocWorkerMsg) -> Result<(), mpsc::error::SendError<DocWorkerMsg>> {
        self.msg_tx.send(msg)
    }

    fn is_closed(&self) -> bool {
        self.msg_tx.is_closed()
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
    SyncWithPeerResult {
        peer_id: PeerId,
        waiter_id: u64,
        result: Result<(), SyncDocError>,
    },
    ReleaseHandleLease,
    ReattemptMaterialization,
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
    subduction: Arc<BigRepoSubduction<S>>,
    sedimentrees: SubductionSedimentrees,
    storage_for_reads: S,
    ciphertext_store: BigRepoCiphertextStore<S>,
    keyhive_storage: BigRepoKeyhiveStorage,
    big_sync_store: SharedPartitionStore,
    change_manager: Arc<changes::ChangeListenerManager>,
    runtime_handle: BigRepoRuntimeHandle,
    msg_tx: mpsc::UnboundedSender<DocWorkerMsg>,
    runtime_tasks: Arc<utils_rs::AbortableJoinSet>,
    runtime_evt_tx: mpsc::UnboundedSender<RuntimeEvt>,
    last_notified_heads: Option<Arc<[automerge::ChangeHash]>>,
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
                let completes_sync_waiters = matches!(
                    session.kind,
                    subduction_core::sync_session::SyncSessionKind::OutboundBatch
                );
                let _materialization_pending = self.handle_apply_sync_session(session).await?;
                if completes_sync_waiters {
                    let waiter = self
                        .pending_sync_jobs
                        .get_mut(&peer_id)
                        .and_then(|waiters| {
                            // One successful `sync_with_peer` call emits one
                            // outbound sync session, so one observed outbound
                            // session completes one caller waiter.
                            (!waiters.is_empty()).then(|| waiters.remove(0).1)
                        });
                    if self
                        .pending_sync_jobs
                        .get(&peer_id)
                        .is_some_and(Vec::is_empty)
                    {
                        self.pending_sync_jobs.remove(&peer_id);
                    }
                    if let Some(waiter) = waiter {
                        waiter
                            .send(Ok(()))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                    }
                }
                self.finish_transient_work()?;
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
            DocWorkerMsg::SyncWithPeerResult {
                peer_id,
                waiter_id,
                result,
            } => {
                if let Err(err) = result {
                    let waiters = self.pending_sync_jobs.get_mut(&peer_id);
                    if let Some(waiters) = waiters {
                        if let Some(pos) = waiters.iter().position(|(id, _)| *id == waiter_id) {
                            let (_id, sender) = waiters.remove(pos);
                            if waiters.is_empty() {
                                self.pending_sync_jobs.remove(&peer_id);
                            }
                            sender.send(Err(err)).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                            self.finish_transient_work()?;
                        }
                    }
                }
            }
            DocWorkerMsg::ReleaseHandleLease => {}
            DocWorkerMsg::ReattemptMaterialization => {
                let _ = self.retry_pending_materialization().await?;
            }
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
        let sedimentree_id: SedimentreeId = self.doc_id_subduction;
        let resident = self
            .sedimentrees
            .get_cloned(&sedimentree_id)
            .await
            .is_some();
        let stored = self
            .storage_for_reads
            .contains_sedimentree_id(sedimentree_id)
            .await
            .map_err(|err| PutDocError::Other(ferr!("failed checking doc occupancy: {err}")))?;
        if resident || stored {
            return Err(PutDocError::IdOccpuied { id: self.doc_id });
        }
        let staged_ingest = stage_automerge_ingest(&doc);

        // Encrypt blobs via keyhive before storage
        let (sedimentree, blobs, update_ops) = encrypt_staged_automerge_ingest(
            &staged_ingest,
            &self.runtime_handle.keyhive,
            sedimentree_id,
        )
        .await
        .map_err(|e| PutDocError::Other(ferr!("encryption failed: {e}")))?;

        for update_op in update_ops {
            persist_cgka_update_op(&self.keyhive_storage, update_op).await?;
        }
        let heads = sedimentree_heads_payload(&sedimentree);
        self.subduction
            .store_sedimentree(sedimentree_id, sedimentree, blobs)
            .await
            .map_err(|err| ferr!("failed store_sedimentree: {err}"))?;
        for entry in &staged_ingest.fragment_entries {
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
        for entry in &staged_ingest.loose_entries {
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
        let doc = match self.take_or_load_transient_doc().await? {
            DocLookup::Ready(doc) => doc,
            DocLookup::PendingMaterialization => {
                return Ok(DocLookup::PendingMaterialization);
            }
            DocLookup::Missing => return Ok(DocLookup::Missing),
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
        let mut keyhive_changed = false;
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
                keyhive_changed = true;
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
        let last_heads = heads.clone();
        commit_delta_bookkeep(
            &self.big_sync_store,
            &self.change_manager,
            self.doc_id,
            heads,
            patches,
            origin,
        )
        .await?;
        self.last_notified_heads = Some(Arc::<[automerge::ChangeHash]>::from(last_heads));
        self.process_pending_fragment_requests().await?;
        if keyhive_changed {
            self.runtime_handle.note_local_keyhive_changed().await?;
        }
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
    ) -> Res<bool> {
        let peer_id = PeerId::new(*session.peer_id.as_bytes());
        let was_pending = matches!(self.state, DocWorkerDocState::PendingMaterialization);
        use keyhive_core::crypto::envelope::Envelope;
        let wants_patches = self
            .change_manager
            .has_change_listener_interest(self.doc_id, &BigRepoChangeOrigin::Remote { peer_id });
        let make_patches = |doc: &automerge::Automerge,
                            before_heads: &[automerge::ChangeHash],
                            after_heads: &[automerge::ChangeHash]| {
            if wants_patches {
                doc.diff(before_heads, after_heads)
            } else {
                Vec::new()
            }
        };

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

        if received_refs.is_empty() {
            if was_pending {
                return self.retry_pending_materialization().await;
            }
            return Ok(false);
        }

        let hydrated = Self::get_or_hydrate_minimized_tree_from_storage(
            &self.sedimentrees,
            &self.storage_for_reads,
            session.sedimentree_id,
        )
        .await?;
        let fresh = hydrated.fresh;
        let tree = hydrated.tree;
        let order = tree
            .topsorted_blob_order()
            .map_err(|err| ferr!("failed ordering sync session blobs: {err}"))?;
        let tree_fragments: Vec<_> = tree.fragments().collect();
        let tree_loose: Vec<_> = tree.loose_commits().collect();
        let received_all_ordered_blobs = received_refs.len() == order.len();

        if !wants_patches
            && matches!(
                self.state,
                DocWorkerDocState::Unloaded | DocWorkerDocState::PendingMaterialization
            )
        {
            let heads = sedimentree_heads_payload(&tree);
            if fresh {
                self.sedimentrees
                    .get_or_insert_with(session.sedimentree_id, || tree)
                    .await;
            }
            store_doc_heads_payload(&self.big_sync_store, self.doc_id, heads.clone()).await?;
            self.change_manager.notify_doc_pending_heads_changed(self.doc_id, heads, BigRepoChangeOrigin::Remote { peer_id })?;
            return Ok(false);
        }

        let keyhive = self.runtime_handle.keyhive.clone_keyhive();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(self.doc_id_subduction.as_bytes())
            .map_err(|_| ferr!("not a valid Keyhive DocumentId"))?;
        let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
            keyhive_core::principal::identifier::Identifier::from(vk),
        );
        let kh_doc = keyhive.get_document(kh_doc_id).await;

        let mut materialization_pending = false;
        let blobs = {
            match kh_doc {
                Some(kh_doc) => {
                    let ciphertext_store = &self.ciphertext_store;

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
                    let mut encrypted_by_ref: HashMap<
                        Vec<u8>,
                        Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>,
                    > = HashMap::new();
                    let expected_received =
                        session.received_commit_ids.len() + session.received_fragment_ids.len();
                    if received_order.len() != expected_received {
                        return Err(ferr!(
                            "sync session received blobs are missing from sedimentree order: expected={} found={}",
                            expected_received,
                            received_order.len(),
                        ));
                    }

                    loop {
                        let mut made_progress = false;
                        for (idx, (item, content_ref)) in received_order.iter().enumerate().rev() {
                            if plaintext_by_index[idx].is_some() {
                                continue;
                            }
                            if let Some(plaintext) = plaintext_by_ref.get(content_ref).cloned() {
                                plaintext_by_index[idx] = Some(plaintext);
                                made_progress = true;
                                continue;
                            }

                            let encrypted = if let Some(encrypted) =
                                encrypted_by_ref.get(content_ref).cloned()
                            {
                                encrypted
                            } else {
                                let locator = match item {
                                    SedimentreeItem::Fragment(ii) => BigRepoCiphertextLocator {
                                        kind: BigRepoCiphertextKind::Fragment,
                                        sedimentree_id: session.sedimentree_id,
                                        commit_id: CommitId::new(
                                            *tree_fragments[*ii].head().as_bytes(),
                                        ),
                                    },
                                    SedimentreeItem::LooseCommit(ii) => BigRepoCiphertextLocator {
                                        kind: BigRepoCiphertextKind::LooseCommit,
                                        sedimentree_id: session.sedimentree_id,
                                        commit_id: CommitId::new(
                                            *tree_loose[*ii].head().as_bytes(),
                                        ),
                                    },
                                };
                                let raw = ciphertext_store
                                    .load_raw_for_locator(&locator)
                                    .await
                                    .map_err(|e| {
                                        ferr!("failed loading exact sync session blob: {e}")
                                    })?
                                    .ok_or_else(|| {
                                        ferr!(
                                            "missing exact sync session blob: sedimentree_id={:?} content_ref={:?} kind={:?}",
                                            session.sedimentree_id,
                                            content_ref,
                                            locator.kind
                                        )
                                    })?;

                                let encrypted = ciphertext_store
                                    .index_loaded_raw(content_ref.clone(), locator, raw.as_slice())
                                    .await?;
                                encrypted_by_ref.insert(content_ref.clone(), encrypted.clone());
                                encrypted
                            };

                            let entrypoint_raw = {
                                let mut doc = kh_doc.lock().await;
                                match doc.try_decrypt_content_keyed(&encrypted) {
                                    Ok((raw, _entrypoint_key)) => raw,
                                    Err(
                                        keyhive_core::principal::document::DecryptError::KeyNotFound,
                                    ) => {
                                        continue;
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
                            made_progress = true;

                            let state = {
                                let mut doc = kh_doc.lock().await;
                                doc.try_causal_decrypt_content(
                                    &encrypted,
                                    (*ciphertext_store).clone(),
                                )
                                .await
                                .map_err(|e| {
                                    ferr!("failed causal decrypting sync session blob: {e}")
                                })?
                            };
                            for (ancestor_ref, plaintext) in state.complete {
                                if plaintext_by_ref.insert(ancestor_ref, plaintext).is_none() {
                                    made_progress = true;
                                }
                            }
                        }

                        if plaintext_by_index.iter().all(Option::is_some) {
                            break;
                        }
                        if !made_progress {
                            materialization_pending = true;
                            break;
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
            let heads = sedimentree_heads_payload(&tree);
            store_doc_heads_payload(
                &self.big_sync_store,
                self.doc_id,
                heads.clone(),
            )
            .await?;
            self.change_manager.notify_doc_pending_heads_changed(self.doc_id, heads, BigRepoChangeOrigin::Remote { peer_id })?;
            if materialization_pending {
                self.mark_materialization_pending(was_pending)?;
            }
            return Ok(materialization_pending);
        }
        if materialization_pending {
            let heads = sedimentree_heads_payload(&tree);
            store_doc_heads_payload(&self.big_sync_store, self.doc_id, heads.clone()).await?;
            self.change_manager.notify_doc_pending_heads_changed(self.doc_id, heads, BigRepoChangeOrigin::Remote { peer_id })?;
            self.mark_materialization_pending(was_pending)?;
            return Ok(true);
        }
        let maybe_delta = match std::mem::replace(&mut self.state, DocWorkerDocState::Unloaded) {
            DocWorkerDocState::Unloaded | DocWorkerDocState::PendingMaterialization => {
                let cached_before_heads =
                    super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id).await?;
                if received_all_ordered_blobs {
                    let before_heads = cached_before_heads
                        .as_deref()
                        .map(<[_]>::to_vec)
                        .unwrap_or_default();
                    let had_cached_before_heads = cached_before_heads.is_some();
                    let mut doc = automerge::Automerge::new();
                    for blob in blobs {
                        doc.load_incremental(&blob).map_err(|err| {
                            eyre::eyre!("failed applying full sync session blob: {err}")
                        })?;
                    }
                    let after_heads = doc.get_heads();
                    let out = if before_heads == after_heads {
                        if had_cached_before_heads {
                            None
                        } else {
                            Some((after_heads, Vec::new()))
                        }
                    } else {
                        let patches =
                            make_patches(&doc, before_heads.as_ref(), after_heads.as_ref());
                        Some((after_heads, patches))
                    };
                    self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                    self.state = DocWorkerDocState::Transient(doc.into());
                    out
                } else {
                    // since the doc in storage will have the latest blobs,
                    // we must load the before_heads from the partition payloads
                    let doc = load_doc_snapshot(
                        &self.sedimentrees,
                        &self.storage_for_reads,
                        &self.ciphertext_store,
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
                            return Ok(true);
                        }
                        DocLookup::Missing => automerge::Automerge::new(),
                    };
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
                            let patches =
                                make_patches(&doc, before_heads.as_ref(), after_heads.as_ref());
                            Some((after_heads, patches))
                        }
                    } else {
                        let patches =
                            make_patches(&doc, before_heads.as_ref(), loaded_heads.as_ref());
                        Some((loaded_heads, patches))
                    };
                    self.state = DocWorkerDocState::Transient(doc.into());
                    out
                }
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
                        let patches =
                            make_patches(&doc, before_heads.as_ref(), after_heads.as_ref());
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
                        let patches =
                            make_patches(&doc, before_heads.as_ref(), after_heads.as_ref());
                        Some((after_heads, patches))
                    };
                    drop(doc);
                    self.state = DocWorkerDocState::Live(Arc::downgrade(&bundle));
                    out
                }
                None => {
                    let cached_before_heads =
                        super::partition_doc_heads_payload(&self.big_sync_store, self.doc_id)
                            .await?;
                    if received_all_ordered_blobs {
                        let before_heads = cached_before_heads
                            .as_deref()
                            .map(<[_]>::to_vec)
                            .unwrap_or_default();
                        let had_cached_before_heads = cached_before_heads.is_some();
                        let mut doc = automerge::Automerge::new();
                        for blob in blobs {
                            doc.load_incremental(&blob).map_err(|err| {
                                eyre::eyre!("failed applying full sync session blob: {err}")
                            })?;
                        }
                        let after_heads = doc.get_heads();
                        let out = if before_heads == after_heads {
                            if had_cached_before_heads {
                                None
                            } else {
                                Some((after_heads, Vec::new()))
                            }
                        } else {
                            let patches =
                                make_patches(&doc, before_heads.as_ref(), after_heads.as_ref());
                            Some((after_heads, patches))
                        };
                        self.mark_materialization_ready(was_pending, Arc::from(doc.get_heads()))?;
                        self.state = DocWorkerDocState::Transient(doc.into());
                        out
                    } else {
                        let doc = load_doc_snapshot(
                            &self.sedimentrees,
                            &self.storage_for_reads,
                            &self.ciphertext_store,
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
                                return Ok(true);
                            }
                            DocLookup::Missing => automerge::Automerge::new(),
                        };
                        let loaded_heads = doc.get_heads();
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
                                let patches =
                                    make_patches(&doc, before_heads.as_ref(), after_heads.as_ref());
                                Some((after_heads, patches))
                            }
                        } else {
                            let patches =
                                make_patches(&doc, before_heads.as_ref(), loaded_heads.as_ref());
                            Some((loaded_heads, patches))
                        };
                        self.state = DocWorkerDocState::Transient(doc.into());
                        out
                    }
                }
            },
        };
        let Some((after_heads, patches)) = maybe_delta else {
            return Ok(false);
        };
        let last_heads = after_heads.clone();
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
        self.last_notified_heads = Some(Arc::<[automerge::ChangeHash]>::from(last_heads));
        Ok(false)
    }

    async fn retry_pending_materialization(&mut self) -> Res<bool> {
        let was_pending = matches!(self.state, DocWorkerDocState::PendingMaterialization);
        match load_doc_snapshot(
            &self.sedimentrees,
            &self.storage_for_reads,
            &self.ciphertext_store,
            &self.runtime_handle.keyhive,
            self.doc_id,
        )
        .await?
        {
            DocLookup::Ready(doc) => {
                let after_heads = doc.get_heads();
                self.mark_materialization_ready(was_pending, Arc::from(after_heads.clone()))?;
                if was_pending {
                    let before: &[automerge::ChangeHash] = self
                        .last_notified_heads
                        .as_deref()
                        .unwrap_or(&[]);
                    let patches = doc.diff(before, &after_heads);
                    commit_delta_bookkeep(
                        &self.big_sync_store,
                        &self.change_manager,
                        self.doc_id,
                        after_heads.clone(),
                        patches,
                        BigRepoChangeOrigin::Bootstrap,
                    )
                    .await?;
                    self.last_notified_heads =
                        Some(Arc::<[automerge::ChangeHash]>::from(after_heads));
                }
                self.state = DocWorkerDocState::Transient(doc.into());
                Ok(false)
            }
            DocLookup::PendingMaterialization => {
                self.mark_materialization_pending(was_pending)?;
                Ok(true)
            }
            DocLookup::Missing => Ok(false),
        }
    }

    async fn get_or_hydrate_minimized_tree_from_storage(
        sedimentrees: &SubductionSedimentrees,
        storage_for_reads: &S,
        sedimentree_id: SedimentreeId,
    ) -> Res<HydratedMinimizedTree>
    where
        S: BigRepoSubductionStorage,
    {
        if let Some(tree) = sedimentrees.get_cloned(&sedimentree_id).await {
            let has_blobs =
                tree.fragments().next().is_some() || tree.loose_commits().next().is_some();
            return Ok(HydratedMinimizedTree {
                tree,
                fresh: false,
                has_blobs,
            });
        }

        let loose_commits =
            <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commits(
                storage_for_reads,
                sedimentree_id,
            );
        let fragments = <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragments(
            storage_for_reads,
            sedimentree_id,
        );
        let (loose_commits, fragments) = futures::future::try_join(loose_commits, fragments)
            .await
            .wrap_err("failed reading blobs from storage")?;
        let has_blobs = !(loose_commits.is_empty() && fragments.is_empty());
        let tree =
            sedimentree_core::sedimentree::minimized::MinimizedSedimentree::new(Sedimentree::new(
                fragments
                    .iter()
                    .map(|verified| verified.payload().clone())
                    .collect(),
                loose_commits
                    .iter()
                    .map(|verified| verified.payload().clone())
                    .collect(),
            ));
        Ok(HydratedMinimizedTree {
            tree,
            fresh: true,
            has_blobs,
        })
    }

    async fn handle_sync_with_peer(
        &mut self,
        peer_id: PeerId,
        waiter_id: u64,
        timeout: Option<Duration>,
        sender: oneshot::Sender<Result<(), SyncDocError>>,
    ) -> Res<()> {
        let sedimentree_id = self.doc_id_subduction;
        let remote_peer_id = subduction_core::peer::id::PeerId::new(peer_id.clone().into_bytes());

        // Push the waiter immediately so it is visible to both
        // ApplySyncSession (on OutboundBatch) and SyncWithPeerResult
        // (on error) — whichever fires first resolves it.
        self.pending_sync_jobs
            .entry(peer_id)
            .or_default()
            .push((waiter_id, sender));
        let subduction_timeout = timeout
            .map(|duration| {
                subduction_core::timeout::call::CallTimeout::TimeoutMillis(
                    duration
                        .as_millis()
                        .try_into()
                        .expect("timeout fits in u64"),
                )
            })
            .unwrap_or(subduction_core::timeout::call::CallTimeout::Default);
        let subduction = Arc::clone(&self.subduction);
        let msg_tx = self.msg_tx.clone();
        let runtime_tasks = Arc::clone(&self.runtime_tasks);

        runtime_tasks
            .spawn(async move {
                let result = subduction
                    .sync_with_peer(&remote_peer_id, sedimentree_id, false, subduction_timeout)
                    .await;
                let result = match result {
                    Ok((had_success, _stats, conn_errs)) => {
                        if had_success {
                            Ok(())
                        } else if conn_errs.is_empty() {
                            Err(SyncDocError::NotFound)
                        } else {
                            Err(SyncDocError::TransportError)
                        }
                    }
                    Err(err) => Err(SyncDocError::IoError(ferr!("{err}"))),
                };
                let _ = msg_tx.send(DocWorkerMsg::SyncWithPeerResult {
                    peer_id,
                    waiter_id,
                    result,
                });
            })
            .expect(ERROR_TOKIO);

        Ok(())
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
            self.runtime_evt_tx
                .send(RuntimeEvt::DocWorkerMaterializationPending {
                    doc_id: self.doc_id,
                })
                .inspect_err(|_| warn!(ERROR_CHANNEL))
                .ok();
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
            self.runtime_evt_tx
                .send(RuntimeEvt::DocWorkerMaterializationReady {
                    doc_id: self.doc_id,
                })
                .inspect_err(|_| warn!(ERROR_CHANNEL))
                .ok();
        }
        Ok(())
    }

    // Subduction returns fragment requests only from local commit insertion
    // paths. Process the accumulated requests while the live Automerge doc is
    // available; remote sync receives existing fragments through normal sync.
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
                    &self.ciphertext_store,
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
                    &self.ciphertext_store,
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
        Sendable,
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
    ciphertext_store: &BigRepoCiphertextStore<S>,
    keyhive: &BigKeyhiveHandle,
    doc_id: D,
) -> Res<DocLookup<automerge::Automerge>>
where
    S: BigRepoSubductionStorage,
    D: Into<DocumentId>,
{
    let doc_id: DocumentId = doc_id.into();
    let sedimentree_id = SedimentreeId::new(doc_id.into_bytes());

    let hydrated = DocWorker::<S>::get_or_hydrate_minimized_tree_from_storage(
        sedimentrees,
        storage_for_reads,
        sedimentree_id,
    )
    .await?;
    if !hydrated.has_blobs {
        return Ok(DocLookup::Missing);
    }
    let tree = hydrated.tree;
    let fresh = hydrated.fresh;
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

        let Some(encrypted) = ciphertext_store
            .load_entrypoint_ciphertext(&content_ref)
            .await?
        else {
            continue;
        };
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
                    pcs_update_op_hash = ?encrypted.pcs_update_op_hash,
                    "snapshot decrypt key not found"
                );
                continue;
            }
            Err(err) => {
                return Err(ferr!(
                    "failed decrypting snapshot entrypoint: sedimentree_id={sedimentree_id:?} content_ref={content_ref:?} error={err}"
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
            doc.try_causal_decrypt_content(&encrypted, (*ciphertext_store).clone())
                .await
                .map_err(|e| ferr!("failed causal decrypting snapshot blob: {e}"))?
        };
        for (ancestor_ref, plaintext) in state.complete {
            plaintext_by_ref.insert(ancestor_ref, plaintext);
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

/// Metadata for a staged fragment emitted by Automerge bundling.
///
/// These are transient in-memory plaintext bytes and must be encrypted before
/// they reach Subduction storage.
struct FragmentEntry {
    head: CommitId,
    boundary: BTreeSet<CommitId>,
    checkpoints: Vec<CommitId>,
}

/// Metadata for a staged loose commit emitted by Automerge bundling.
///
/// These are transient in-memory plaintext bytes and must be encrypted before
/// they reach Subduction storage.
struct LooseEntry {
    head: CommitId,
    parents: BTreeSet<CommitId>,
}

/// Plaintext Automerge staging output.
///
/// These bytes are transient in-memory data and must be encrypted before they
/// are persisted through Subduction storage.
struct StagedAutomergeIngest {
    blobs: Vec<Blob>,
    fragment_entries: Vec<FragmentEntry>,
    loose_entries: Vec<LooseEntry>,
    _change_count: usize,
    _covered_count: usize,
    _loose_count: usize,
    _fragment_count: usize,
}

struct HydratedMinimizedTree {
    tree: sedimentree_core::sedimentree::minimized::MinimizedSedimentree,
    fresh: bool,
    has_blobs: bool,
}

/// Stage transient plaintext Automerge bundle bytes and provisional
/// sedimentree metadata.
///
/// Thin adapter over [`Automerge::fragments`] and
/// [`Automerge::bundle_fragments`]: maps each level-1+ `automerge::Fragment`
/// into a staged sedimentree [`Fragment`] and each level-0 fragment into a
/// staged [`LooseCommit`]. The staged plaintext must be encrypted before
/// it reaches Subduction storage.
fn stage_automerge_ingest(
    doc: &automerge::Automerge,
) -> StagedAutomergeIngest {
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    let loose_bytes = doc.bundle_fragments(loose.iter().cloned());

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
        blobs.push(Blob::new(raw));
        fragment_entries.push(FragmentEntry {
            head,
            boundary,
            checkpoints,
        });
    }

    for (fragment, raw) in loose.iter().zip(loose_bytes) {
        let head = CommitId::new(fragment.head.0);
        let parents: BTreeSet<CommitId> = fragment
            .boundary
            .iter()
            .map(|pp| CommitId::new(pp.0))
            .collect();
        blobs.push(Blob::new(raw));
        loose_entries.push(LooseEntry { head, parents });
    }

    let covered_count = covered.len();
    let fragment_count = fragment_entries.len();
    let loose_count = loose_entries.len();

    StagedAutomergeIngest {
        blobs,
        fragment_entries,
        loose_entries,
        _change_count: doc.get_changes_meta(&[]).len(),
        _covered_count: covered_count,
        _loose_count: loose_count,
        _fragment_count: fragment_count,
    }
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
    sedimentree_id: SedimentreeId,
    locators: Arc<
        tokio::sync::Mutex<HashMap<Vec<u8>, std::collections::BTreeSet<BigRepoCiphertextLocator>>>,
    >,
    ciphertexts: Arc<
        tokio::sync::Mutex<
            HashMap<Vec<u8>, Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
        >,
    >,
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
    fn new(storage_for_reads: S, sedimentree_id: SedimentreeId) -> Self {
        Self {
            storage_for_reads,
            sedimentree_id,
            locators: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            ciphertexts: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            pcs_updates: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn remember_locator(&self, content_ref: Vec<u8>, locator: BigRepoCiphertextLocator) {
        self.locators
            .lock()
            .await
            .entry(content_ref)
            .or_default()
            .insert(locator);
    }

    async fn index_loaded_raw(
        &self,
        content_ref: Vec<u8>,
        locator: BigRepoCiphertextLocator,
        raw: &[u8],
    ) -> Res<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>> {
        self.remember_locator(content_ref.clone(), locator).await;
        let encrypted = decode_encrypted_blob(raw)?;
        let encrypted = Arc::new(encrypted);
        self.ciphertexts
            .lock()
            .await
            .insert(content_ref.clone(), Arc::clone(&encrypted));
        self.pcs_updates
            .lock()
            .await
            .entry(encrypted.pcs_update_op_hash)
            .or_default()
            .insert(content_ref);
        Ok(encrypted)
    }

    #[cfg(test)]
    async fn ensure_ciphertext_indexed(
        &self,
        content_ref: Vec<u8>,
        locator: BigRepoCiphertextLocator,
    ) -> Res<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>
    where
        S: BigRepoSubductionStorage,
    {
        if let Some(encrypted) = self.ciphertexts.lock().await.get(&content_ref).cloned() {
            return Ok(encrypted);
        }
        let raw = self
            .load_raw_for_locator(&locator)
            .await?
            .ok_or_else(|| {
                ferr!(
                    "missing ciphertext blob while indexing locator: sedimentree_id={:?} content_ref={content_ref:?} kind={:?}",
                    locator.sedimentree_id,
                    locator.kind
                )
            })?;
        self.index_loaded_raw(content_ref, locator, raw.as_slice())
            .await
    }

    fn candidate_locators(&self, content_ref: &[u8]) -> Res<Vec<BigRepoCiphertextLocator>> {
        let commit_id_bytes: [u8; 32] = content_ref
            .try_into()
            .map_err(|_| ferr!("content ref must be 32 bytes"))?;
        let commit_id = CommitId::new(commit_id_bytes);
        Ok(vec![
            BigRepoCiphertextLocator {
                kind: BigRepoCiphertextKind::Fragment,
                sedimentree_id: self.sedimentree_id,
                commit_id,
            },
            BigRepoCiphertextLocator {
                kind: BigRepoCiphertextKind::LooseCommit,
                sedimentree_id: self.sedimentree_id,
                commit_id,
            },
        ])
    }

    async fn load_raw_for_locator(&self, locator: &BigRepoCiphertextLocator) -> Res<Option<Vec<u8>>>
    where
        S: BigRepoSubductionStorage,
    {
        let maybe_raw = match locator.kind {
            BigRepoCiphertextKind::LooseCommit => {
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
                    &self.storage_for_reads,
                    locator.sedimentree_id,
                    locator.commit_id,
                )
                .await
                .map_err(|e| ferr!("failed loading loose commit for ciphertext: {e}"))?
                .map(|verified| verified.blob().clone().into_contents())
            }
            BigRepoCiphertextKind::Fragment => {
                <S as subduction_core::storage::traits::Storage<Sendable>>::load_fragment(
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

    async fn load_entrypoint_ciphertext(
        &self,
        content_ref: &[u8],
    ) -> Res<Option<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>>
    where
        S: BigRepoSubductionStorage,
    {
        let content_ref_vec = content_ref.to_vec();

        if let Some(encrypted) = self.ciphertexts.lock().await.get(content_ref).cloned() {
            return Ok(Some(encrypted));
        }

        let mut locators = self
            .locators
            .lock()
            .await
            .get(content_ref)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        for candidate in self.candidate_locators(content_ref)? {
            if !locators.contains(&candidate) {
                locators.push(candidate);
            }
        }

        for locator in locators {
            let Some(raw) = self.load_raw_for_locator(&locator).await? else {
                continue;
            };
            let encrypted = self
                .index_loaded_raw(content_ref_vec.clone(), locator, raw.as_slice())
                .await?;
            return Ok(Some(encrypted));
        }

        Ok(None)
    }

}

impl<S: BigRepoSubductionStorage> CiphertextStore<Sendable, Vec<u8>, Vec<u8>>
    for BigRepoCiphertextStore<S>
{
    type GetCiphertextError = eyre::Report;
    type MarkDecryptedError = eyre::Report;

    fn get_ciphertext<'a>(
        &'a self,
        content_ref: &'a Vec<u8>,
    ) -> <Sendable as FutureForm>::Future<
        'a,
        Result<
            Option<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        Sendable::from_future(async move { self.load_entrypoint_ciphertext(content_ref.as_slice()).await })
    }

    fn get_ciphertext_by_pcs_update<'a>(
        &'a self,
        pcs_update: &'a keyhive_crypto::digest::Digest<
            keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>,
        >,
    ) -> <Sendable as FutureForm>::Future<
        'a,
        Result<
            Vec<Arc<beekem::encrypted::EncryptedContent<Vec<u8>, Vec<u8>>>>,
            Self::GetCiphertextError,
        >,
    > {
        Sendable::from_future(async move {
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
        _content_ref: &'a Vec<u8>,
    ) -> <Sendable as FutureForm>::Future<'a, Result<(), Self::MarkDecryptedError>> {
        Sendable::from_future(async move { Ok(()) })
    }
}

/// Encrypt staged plaintext blobs via the keyhive, rebuilding the sedimentree
/// with updated BlobMeta matching the encrypted bytes.
///
/// BigRepo requires the sedimentree ID to map to a local Keyhive document.
/// Invalid IDs are rejected instead of falling back to plaintext storage.
/// Encrypted blobs use a provisional format with a dedicated discriminator
/// followed by a DRISL envelope.
///
/// TODO: Upstream (keyhive/subduction_keyhive) hasn't specified the canonical
/// EncryptedContent storage format yet. Revisit when upstream publishes a spec.
async fn encrypt_staged_automerge_ingest(
    staged_ingest: &StagedAutomergeIngest,
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

    let mut encrypted_blobs: Vec<Blob> = Vec::with_capacity(staged_ingest.blobs.len());
    let mut new_fragments: Vec<Fragment> = Vec::with_capacity(staged_ingest.fragment_entries.len());
    let mut new_loose_commits: Vec<LooseCommit> =
        Vec::with_capacity(staged_ingest.loose_entries.len());
    let mut update_ops: Vec<keyhive_crypto::signed::Signed<beekem::operation::CgkaOperation>> =
        Vec::with_capacity(
            staged_ingest.fragment_entries.len() + staged_ingest.loose_entries.len(),
        );

    // Track content_ref -> SymmetricKey for building ancestor maps
    let mut key_index: std::collections::HashMap<Vec<u8>, SymmetricKey> =
        std::collections::HashMap::new();

    // Encrypt fragment blobs
    for (entry, blob) in staged_ingest.fragment_entries.iter().zip(
        staged_ingest
            .blobs
            .iter()
            .take(staged_ingest.fragment_entries.len()),
    ) {
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

        let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;

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
    for (entry, blob) in staged_ingest.loose_entries.iter().zip(
        staged_ingest
            .blobs
            .iter()
            .skip(staged_ingest.fragment_entries.len()),
    ) {
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

        let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;

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
#[cfg(test)]
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

    let encrypted_bytes = encode_encrypted_blob(encrypted.encrypted_content())?;

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
    let head_verified = <S as subduction_core::storage::traits::Storage<Sendable>>::load_loose_commit(
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
                Sendable,
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
    let encrypted_bytes = encode_encrypted_blob(&encrypted)?;

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
    subduction_keyhive::save_event::<Vec<u8>, _, Sendable>(keyhive_storage, &event)
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
    async fn ciphertext_store_keeps_stored_commit_loadable_after_mark_decrypted() -> Res<()> {
        let keyhive = BigKeyhiveHandle::new([9; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let _doc_id = keyhive
            .create_doc(default(), nonempty![[1; 32]], &keyhive_storage)
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

        let verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, commit_id, parents),
                VerifiedBlobMeta::new(encrypted_blob.clone()),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let adapter = BigRepoCiphertextStore::new(storage.clone(), sedimentree_id);
        let content_ref = commit_id.as_bytes().to_vec();
        let indexed = adapter
            .ensure_ciphertext_indexed(
                content_ref.clone(),
                BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::LooseCommit,
                    sedimentree_id,
                    commit_id,
                },
            )
            .await
            .map_err(|e| ferr!("ensure_ciphertext_indexed failed: {e}"))?;
        assert_eq!(indexed.content_ref, content_ref);

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
        let encrypted_after_mark = adapter
            .get_ciphertext(&commit_id.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("get_ciphertext after mark_decrypted failed: {e}"))?
            .expect("durable ciphertext should remain loadable after mark_decrypted");
        assert_eq!(encrypted_after_mark.content_ref, content_ref);
        Ok(())
    }

    #[tokio::test]
    async fn ciphertext_store_reuses_indexed_entry_for_repeat_lookup() -> Res<()> {
        let keyhive = BigKeyhiveHandle::new([14; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let doc_id = keyhive
            .create_doc(default(), nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*doc_id.as_bytes());
        let commit_id = CommitId::new([17; 32]);
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[47u8; 32]);
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

        let verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, commit_id, parents),
                VerifiedBlobMeta::new(encrypted_blob.clone()),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let adapter = BigRepoCiphertextStore::new(storage.clone(), sedimentree_id);
        let content_ref = commit_id.as_bytes().to_vec();
        let first = adapter
            .ensure_ciphertext_indexed(
                content_ref.clone(),
                BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::LooseCommit,
                    sedimentree_id,
                    commit_id,
                },
            )
            .await
            .map_err(|e| ferr!("ensure_ciphertext_indexed failed: {e}"))?;
        let second = adapter
            .get_ciphertext(&content_ref)
            .await
            .map_err(|e| ferr!("first get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");
        let third = adapter
            .get_ciphertext(&content_ref)
            .await
            .map_err(|e| ferr!("second get_ciphertext failed: {e}"))?
            .expect("ciphertext should be found");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &third));
        Ok(())
    }

    #[tokio::test]
    async fn ciphertext_store_prefers_fragment_over_loose_commit_for_same_ref() -> Res<()> {
        use sedimentree_core::fragment::Fragment;

        let keyhive = BigKeyhiveHandle::new([11; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let _doc_id = keyhive
            .create_doc(default(), nonempty![[1; 32]], &keyhive_storage)
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
        let h1_verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, h1, h1_parents.clone()),
                VerifiedBlobMeta::new(h1_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, h1_verified)
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
        let h2_verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, h2, h2_parents.clone()),
                VerifiedBlobMeta::new(h2_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, h2_verified)
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
        let fragment_verified = VerifiedMeta::<Fragment>::seal::<Sendable, _>(
            &signer,
            (sedimentree_id, h2, h2_parents.clone(), vec![]),
            VerifiedBlobMeta::new(fragment_blob),
        )
        .await;
        Storage::<Sendable>::save_fragment(&storage, sedimentree_id, fragment_verified)
            .await
            .map_err(|e| ferr!("save_fragment failed: {e}"))?;

        let adapter = BigRepoCiphertextStore::new(storage.clone(), sedimentree_id);
        adapter
            .remember_locator(
                h2.as_bytes().to_vec(),
                BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::LooseCommit,
                    sedimentree_id,
                    commit_id: h2,
                },
            )
            .await;
        adapter
            .remember_locator(
                h2.as_bytes().to_vec(),
                BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::Fragment,
                    sedimentree_id,
                    commit_id: h2,
                },
            )
            .await;

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

    #[test]
    fn doc_lookup_distinguishes_missing_and_pending_materialization() {
        let doc_id = DocumentId::new([23; 32]);
        let missing = DocLookup::<()>::Missing
            .into_ready(doc_id)
            .expect_err("missing doc should fail");
        assert!(matches!(missing, GetDocError::NotFound(id) if id == doc_id));

        let pending = DocLookup::<()>::PendingMaterialization
            .into_ready(doc_id)
            .expect_err("pending doc should fail");
        assert!(matches!(
            pending,
            GetDocError::PendingMaterialization(id) if id == doc_id
        ));
    }

    #[tokio::test]
    async fn ciphertext_store_lazy_ancestor_lookup_uses_indexed_child_only() -> Res<()> {
        let keyhive = BigKeyhiveHandle::new([13; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let doc_id = keyhive
            .create_doc(default(), nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[46u8; 32]);

        let h1 = CommitId::new([9; 32]);
        let h2 = CommitId::new([10; 32]);
        let h1_parents = BTreeSet::new();
        let h2_parents = BTreeSet::from([h1]);

        let (h1_blob, h1_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            h1,
            &h1_parents,
            b"ancestor-bytes",
            &std::collections::HashMap::new(),
        )
        .await?;
        let h1_verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, h1, h1_parents.clone()),
                VerifiedBlobMeta::new(h1_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, h1_verified)
            .await
            .map_err(|e| ferr!("save_loose_commit for h1 failed: {e}"))?;

        let (h2_blob, h2_key) = encrypt_loose_commit(
            &keyhive,
            sedimentree_id,
            h2,
            &h2_parents,
            b"child-bytes",
            &std::collections::HashMap::from([(h1, h1_key)]),
        )
        .await?;
        let h2_verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, h2, h2_parents.clone()),
                VerifiedBlobMeta::new(h2_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, h2_verified)
            .await
            .map_err(|e| ferr!("save_loose_commit for h2 failed: {e}"))?;

        let adapter = BigRepoCiphertextStore::new(storage.clone(), sedimentree_id);
        let child_encrypted = adapter
            .ensure_ciphertext_indexed(
                h2.as_bytes().to_vec(),
                BigRepoCiphertextLocator {
                    kind: BigRepoCiphertextKind::LooseCommit,
                    sedimentree_id,
                    commit_id: h2,
                },
            )
            .await
            .map_err(|e| ferr!("ensure_ciphertext_indexed for h2 failed: {e}"))?;
        let child_decrypted = child_encrypted
            .try_decrypt(h2_key)
            .map_err(|e| ferr!("decrypting child failed: {e}"))?;
        let child_envelope: Envelope<Vec<u8>, Vec<u8>> = bincode::deserialize(&child_decrypted)
            .map_err(|e| ferr!("decode child envelope failed: {e}"))?;
        assert_eq!(child_envelope.plaintext, b"child-bytes");

        let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
            keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(sedimentree_id.as_bytes())
                    .map_err(|_| ferr!("invalid doc id for test"))?,
            ),
        );
        let kh_doc = keyhive
            .clone_keyhive()
            .get_document(kh_doc_id)
            .await
            .ok_or_else(|| ferr!("missing keyhive doc"))?;
        let state = {
            let mut doc = kh_doc.lock().await;
            doc.try_causal_decrypt_content(&child_encrypted, adapter.clone())
                .await
                .map_err(|e| ferr!("causal decrypt with lazy ancestor lookup failed: {e}"))?
        };
        assert!(state
            .complete
            .iter()
            .any(
                |(content_ref, plaintext)| content_ref == &h1.as_bytes().to_vec()
                    && plaintext.as_slice() == b"ancestor-bytes"
            ));

        adapter
            .mark_decrypted(&h2.as_bytes().to_vec())
            .await
            .map_err(|e| ferr!("mark_decrypted for h2 failed: {e}"))?;
        let direct_loaded = adapter
            .load_entrypoint_ciphertext(h2.as_bytes())
            .await?
            .expect("direct ciphertext load should remain available after mark_decrypted");
        assert_eq!(direct_loaded.content_ref, h2.as_bytes().to_vec());
        let loaded_after_mark = adapter
            .get_ciphertext(&h2.as_bytes().to_vec())
            .await?
            .expect("durable ciphertext lookup should remain available after mark_decrypted");
        assert_eq!(loaded_after_mark.content_ref, h2.as_bytes().to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn fragment_encryption_reuses_existing_content_key() -> Res<()> {
        let keyhive = BigKeyhiveHandle::new([10; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let doc_id = keyhive
            .create_doc(default(), nonempty![[1; 32]], &keyhive_storage)
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
        let h1_verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, h1, h1_parents.clone()),
                VerifiedBlobMeta::new(h1_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, h1_verified)
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
        let h2_verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, h2, h2_parents.clone()),
                VerifiedBlobMeta::new(h2_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, h2_verified)
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
        let keyhive = BigKeyhiveHandle::new([12; 32]).await?;
        let keyhive_storage = BigRepoKeyhiveStorage::memory();
        let doc_id = keyhive
            .create_doc(default(), nonempty![[1; 32]], &keyhive_storage)
            .await?;
        let sedimentree_id = SedimentreeId::new(*doc_id.as_bytes());
        let storage = MemoryStorage::new();
        let signer = MemorySigner::from_bytes(&[45u8; 32]);
        let commit_id = CommitId::new([9; 32]);
        let parents = BTreeSet::new();
        let plaintext_blob = Blob::new(b"plain commit bytes".to_vec());
        let verified =
            VerifiedMeta::<sedimentree_core::loose_commit::LooseCommit>::seal::<Sendable, _>(
                &signer,
                (sedimentree_id, commit_id, parents),
                VerifiedBlobMeta::new(plaintext_blob),
            )
            .await;
        Storage::<Sendable>::save_loose_commit(&storage, sedimentree_id, verified)
            .await
            .map_err(|e| ferr!("save_loose_commit failed: {e}"))?;

        let sedimentrees =
            Arc::new(subduction_core::collections::bounded_sharded_map::BoundedShardedMap::new());
        let ciphertext_store = BigRepoCiphertextStore::new(storage.clone(), sedimentree_id);
        let err = load_doc_snapshot(&sedimentrees, &storage, &ciphertext_store, &keyhive, doc_id)
            .await
            .expect_err("plaintext blob should be rejected");
        assert!(err.to_string().contains("blob is not encrypted"));
        Ok(())
    }
}
