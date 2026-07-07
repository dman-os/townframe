//! Adapted from `subduction_cli/src/handler.rs`.
//! Original license: Apache-2.0/MIT. (c) 2024 Ink & Switch

use crate::interlude::*;

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
use subduction_ephemeral::{
    clock::std_clock::StdClock, handler::EphemeralHandler, policy::OpenEphemeralPolicy,
};
use subduction_keyhive::handler::{
    HandleError as KeyhiveHandleError, SendableKeyhiveHandler, SendableRuntimeProtocol,
};
use subduction_websocket::tokio::{TokioSpawn};

/// The concrete keyhive protocol type for BigRepo.
pub(crate) type BigRepoKeyhiveProtocol =
    Arc<SendableRuntimeProtocol<crate::keyhive_listener::BigRepoKeyhiveListener, BigRepoKeyhiveConnAdapter, BigRepoKeyhiveStorage>>;

/// The concrete keyhive handler type for BigRepo.
pub(crate) type BigRepoKeyhiveHandler = SendableKeyhiveHandler<
    crate::keyhive_listener::BigRepoKeyhiveListener,
    BigRepoKeyhiveConnAdapter,
    BigRepoKeyhiveStorage,
    BigRepoIrohTransport,
    fn(Authenticated<BigRepoIrohTransport, Sendable>) -> BigRepoKeyhiveConnAdapter,
>;

/// The concrete ephemeral handler type for BigRepo.
pub(crate) type BigRepoEphemeralHandler =
    Arc<EphemeralHandler<Sendable, BigRepoIrohTransport, OpenEphemeralPolicy, StdClock>>;

type BigRepoListenError<S> = ListenError<Sendable, S, BigRepoIrohTransport, BigRepoWireMessage>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BigRepoComposedHandlerError<S>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable> + core::fmt::Debug,
{
    /// Sync handler error.
    #[error(transparent)]
    Sync(#[from] ListenError<Sendable, S, BigRepoIrohTransport, SyncMessage>),
    /// Keyhive handler error.
    #[error("keyhive: {0}")]
    Keyhive(#[from] KeyhiveHandleError),
}

impl<S> From<BigRepoComposedHandlerError<S>> for BigRepoListenError<S>
where
    S: subduction_core::storage::traits::Storage<future_form::Sendable> + core::fmt::Debug,
{
    fn from(value: BigRepoComposedHandlerError<S>) -> Self {
        match value {
            BigRepoComposedHandlerError::Sync(err) => match err {
                ListenError::IoError(io_err) => ListenError::IoError(match io_err {
                    IoError::Storage(err) => IoError::Storage(err),
                    IoError::ConnSend(err) => IoError::ConnSend(err),
                    IoError::ConnRecv(err) => IoError::ConnRecv(err),
                    IoError::ConnCall(err) => IoError::ConnCall(err),
                    IoError::BlobMismatch(err) => IoError::BlobMismatch(err),
                }),
                ListenError::TrySendError => ListenError::TrySendError,
            }
            BigRepoComposedHandlerError::Keyhive(err) => {
                panic!("keyhive handler error reached listen conversion: {err}")
            }
        }
    }
}

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

/// Composed handler that dispatches to sync, ephemeral, and keyhive sub-handlers.
///
/// Sync dispatch remains fatal because Subduction uses `ListenError` to control
/// connection teardown. Ephemeral dispatch is logged and treated as
/// non-fatal. Keyhive failures are preserved as an explicit handler error and
/// must not be collapsed into channel backpressure.
pub(crate) struct BigRepoComposedHandler<
    S: subduction_core::storage::traits::Storage<future_form::Sendable>,
> {
    sync: BigRepoSyncHandler<S>,
    ephemeral: BigRepoEphemeralHandler,
    keyhive: BigRepoKeyhiveHandler,
}

impl<S: subduction_core::storage::traits::Storage<future_form::Sendable>>
    BigRepoComposedHandler<S>
{
    pub(crate) fn new(
        sync: BigRepoSyncHandler<S>,
        ephemeral: BigRepoEphemeralHandler,
        keyhive: BigRepoKeyhiveHandler,
    ) -> Self {
        Self {
            sync,
            ephemeral,
            keyhive,
        }
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
    type HandlerError = BigRepoComposedHandlerError<S>;

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
                    .map_err(BigRepoComposedHandlerError::Sync)
                }
                BigRepoWireMessage::Ephemeral(ephemeral_msg) => {
                    if let Err(err) = Handler::<Sendable, BigRepoIrohTransport>::handle(
                        self.ephemeral.as_ref(),
                        conn,
                        ephemeral_msg,
                    ).await {
                        tracing::error!(%err, "ephemeral handler error");
                    }
                    Ok(())
                }
                BigRepoWireMessage::Keyhive(keyhive_msg) => {
                    Handler::<Sendable, BigRepoIrohTransport>::handle(
                        &self.keyhive,
                        conn,
                        keyhive_msg,
                    )
                    .await
                    .map_err(BigRepoComposedHandlerError::Keyhive)
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            Handler::<Sendable, BigRepoIrohTransport>::on_peer_disconnect(self.sync.as_ref(), peer)
                .await;
            Handler::<Sendable, BigRepoIrohTransport>::on_peer_disconnect(
                self.ephemeral.as_ref(),
                peer,
            )
            .await;
            Handler::<Sendable, BigRepoIrohTransport>::on_peer_disconnect(&self.keyhive, peer)
                .await;
        })
    }
}
