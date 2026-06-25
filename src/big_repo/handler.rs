use std::sync::Arc;

use crate::keyhive_conn::BigRepoKeyhiveConnAdapter;
use crate::keyhive_storage::BigRepoKeyhiveStorage;
use crate::runtime::BigRepoIrohTransport;
use crate::wire::BigRepoWireMessage;

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::depth::CountLeadingZeroBytes;
use subduction_core::{
    authenticated::Authenticated,
    connection::message::SyncMessage,
    handler::{sync::SyncHandler, Handler},
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    subduction::error::{IoError, ListenError},
};
use subduction_keyhive::handler::{SendableKeyhiveHandler, SendableRuntimeProtocol};
use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn};

use crate::keyhive::BigKeyhiveHandle;

/// The concrete keyhive protocol type for BigRepo.
pub(crate) type BigRepoKeyhiveProtocol =
    Arc<SendableRuntimeProtocol<BigRepoKeyhiveConnAdapter, BigRepoKeyhiveStorage>>;

/// The concrete keyhive handler type for BigRepo.
pub(crate) type BigRepoKeyhiveHandler = SendableKeyhiveHandler<
    BigRepoKeyhiveConnAdapter,
    BigRepoKeyhiveStorage,
    BigRepoIrohTransport,
    fn(Authenticated<BigRepoIrohTransport, Sendable>) -> BigRepoKeyhiveConnAdapter,
>;

/// Concrete ListenError for the composed handler.
type SynchronousListenError<S> = ListenError<Sendable, S, BigRepoIrohTransport, BigRepoWireMessage>;

/// The concrete sync handler type.
type BigRepoSyncHandler<S> = Arc<
    SyncHandler<
        Sendable,
        S,
        BigRepoIrohTransport,
        crate::runtime::BigRepoPolicy,
        CountLeadingZeroBytes,
        TokioSpawn,
    >,
>;

/// Composed handler that dispatches to sync and keyhive sub-handlers.
pub(crate) struct BigRepoComposedHandler<
    S: subduction_core::storage::traits::Storage<future_form::Sendable>,
> {
    sync: BigRepoSyncHandler<S>,
    keyhive: BigRepoKeyhiveHandler,
}

impl<S: subduction_core::storage::traits::Storage<future_form::Sendable>>
    BigRepoComposedHandler<S>
{
    pub(crate) fn new(sync: BigRepoSyncHandler<S>, keyhive: BigRepoKeyhiveHandler) -> Self {
        Self { sync, keyhive }
    }
}

impl<S: subduction_core::storage::traits::Storage<future_form::Sendable>> core::fmt::Debug
    for BigRepoComposedHandler<S>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BigRepoComposedHandler")
            .finish_non_exhaustive()
    }
}

impl<S: subduction_core::storage::traits::Storage<future_form::Sendable>> RemoteHeadsNotifier
    for BigRepoComposedHandler<S>
{
    fn notify_remote_heads(
        &self,
        id: sedimentree_core::id::SedimentreeId,
        peer: PeerId,
        heads: RemoteHeads,
    ) {
        RemoteHeadsNotifier::notify_remote_heads(self.sync.as_ref(), id, peer, heads);
    }
}

impl<S: subduction_core::storage::traits::Storage<future_form::Sendable>>
    Handler<Sendable, BigRepoIrohTransport> for BigRepoComposedHandler<S>
where
    S: crate::runtime::BigRepoSubductionStorage,
    <S as subduction_core::storage::traits::Storage<Sendable>>::Error: 'static,
{
    type Message = BigRepoWireMessage;
    type HandlerError = SynchronousListenError<S>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<BigRepoIrohTransport, Sendable>,
        message: BigRepoWireMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                BigRepoWireMessage::Sync(sync_msg) => {
                    Handler::<Sendable, BigRepoIrohTransport>::handle(
                        self.sync.as_ref(),
                        conn,
                        *sync_msg,
                    )
                    .await
                    .map_err(convert_sync_listen_error)
                }
                BigRepoWireMessage::Keyhive(keyhive_msg) => {
                    if let Err(err) = Handler::<Sendable, BigRepoIrohTransport>::handle(
                        &self.keyhive,
                        conn,
                        keyhive_msg,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "keyhive handler failed");
                    }
                    Ok(())
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            Handler::<Sendable, BigRepoIrohTransport>::on_peer_disconnect(self.sync.as_ref(), peer)
                .await;
            Handler::<Sendable, BigRepoIrohTransport>::on_peer_disconnect(&self.keyhive, peer)
                .await;
        })
    }
}

/// Convert a sync `ListenError<..., SyncMessage>` into `SynchronousListenError<S>`.
fn convert_sync_listen_error<S: subduction_core::storage::traits::Storage<Sendable>>(
    err: ListenError<Sendable, S, BigRepoIrohTransport, SyncMessage>,
) -> SynchronousListenError<S> {
    match err {
        ListenError::IoError(io_err) => ListenError::IoError(match io_err {
            IoError::Storage(e) => IoError::Storage(e),
            IoError::ConnSend(e) => IoError::ConnSend(e),
            IoError::ConnRecv(e) => IoError::ConnRecv(e),
            IoError::ConnCall(e) => IoError::ConnCall(e),
            IoError::BlobMismatch(e) => IoError::BlobMismatch(e),
        }),
        ListenError::TrySendError => ListenError::TrySendError,
    }
}

/// The concrete Subduction type for BigRepo, using the composed handler.
pub(crate) type BigRepoSubduction<S> = subduction_core::subduction::Subduction<
    'static,
    future_form::Sendable,
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

/// Bootstrap the keyhive protocol and handler. Returns the protocol and handler.
pub(crate) async fn boot_keyhive(
    keyhive: &BigKeyhiveHandle,
    keyhive_storage: BigRepoKeyhiveStorage,
) -> crate::Res<(BigRepoKeyhiveProtocol, BigRepoKeyhiveHandler)> {
    let shared_keyhive = keyhive.shared_keyhive();
    let contact_card = keyhive.contact_card().clone();
    let kh_peer_id = keyhive.keyhive_peer_id();
    let keyhive_protocol: BigRepoKeyhiveProtocol =
        Arc::new(subduction_keyhive::KeyhiveProtocol::new(
            Arc::clone(&shared_keyhive),
            keyhive_storage,
            kh_peer_id,
            contact_card,
        ));

    if let Err(e) = keyhive_protocol.ingest_from_storage().await {
        tracing::warn!(error = %e, "keyhive ingest_from_storage failed");
    }

    let keyhive_for_handler = Arc::clone(&keyhive_protocol);
    let keyhive_handler = BigRepoKeyhiveHandler::new(
        keyhive_for_handler,
        BigRepoKeyhiveConnAdapter::new
            as fn(Authenticated<BigRepoIrohTransport, Sendable>) -> BigRepoKeyhiveConnAdapter,
    );

    Ok((keyhive_protocol, keyhive_handler))
}
