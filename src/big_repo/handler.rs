use std::sync::Arc;

use crate::keyhive_conn::BigRepoKeyhiveConnAdapter;
use crate::keyhive_storage::BigRepoKeyhiveStorage;
use crate::runtime::BigRepoIrohTransport;
use crate::wire::BigRepoWireMessage;
use async_lock::Mutex;

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
            BigRepoComposedHandlerError::Sync(sync_err) => convert_sync_listen_error(sync_err),
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
                    let result = Handler::<Sendable, BigRepoIrohTransport>::handle(
                        self.ephemeral.as_ref(),
                        conn,
                        ephemeral_msg,
                    )
                    .await;
                    log_nonfatal_handler_result(result, "ephemeral").await;
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

/// Convert a sync `ListenError<..., SyncMessage>` into `BigRepoListenError<S>`.
fn convert_sync_listen_error<S: subduction_core::storage::traits::Storage<Sendable>>(
    err: ListenError<Sendable, S, BigRepoIrohTransport, SyncMessage>,
) -> BigRepoListenError<S> {
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

async fn log_nonfatal_handler_result<E: core::fmt::Display>(
    result: Result<(), E>,
    label: &'static str,
) {
    if let Err(err) = result {
        tracing::error!(error = %err, %label, "handler error (non-fatal)");
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
    sync_done_observer: Option<Arc<dyn Fn(subduction_keyhive::KeyhivePeerId) + Send + Sync>>,
) -> crate::Res<(BigRepoKeyhiveProtocol, BigRepoKeyhiveHandler)> {
    let shared_keyhive = Arc::new(Mutex::new(keyhive.clone_keyhive()));
    let contact_card = keyhive.contact_card().clone();
    let kh_peer_id = keyhive.keyhive_peer_id();
    let keyhive_protocol: BigRepoKeyhiveProtocol = Arc::new(
        subduction_keyhive::KeyhiveProtocol::new(
            Arc::clone(&shared_keyhive),
            keyhive_storage,
            kh_peer_id,
            contact_card,
        )
        .with_storage_recovery(),
    );

    keyhive_protocol
        .ingest_from_storage()
        .await
        .map_err(|err| crate::ferr!("keyhive ingest_from_storage failed: {err}"))?;

    let keyhive_for_handler = Arc::clone(&keyhive_protocol);
    let mut keyhive_handler = BigRepoKeyhiveHandler::new(
        keyhive_for_handler,
        BigRepoKeyhiveConnAdapter::new
            as fn(Authenticated<BigRepoIrohTransport, Sendable>) -> BigRepoKeyhiveConnAdapter,
    );
    if let Some(observer) = sync_done_observer {
        keyhive_handler = keyhive_handler.with_sync_done_observer(observer);
    }

    Ok((keyhive_protocol, keyhive_handler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use subduction_core::storage::memory::MemoryStorage;

    #[derive(Debug)]
    struct TestError;

    impl core::fmt::Display for TestError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("test error")
        }
    }

    impl std::error::Error for TestError {}

    #[tokio::test]
    async fn nonfatal_dispatch_swallows_handler_errors() {
        log_nonfatal_handler_result::<TestError>(Err(TestError), "keyhive").await;
    }

    #[test]
    fn sync_try_send_error_remains_try_send_error() {
        let err: BigRepoListenError<MemoryStorage> =
            convert_sync_listen_error(ListenError::TrySendError);
        assert!(matches!(err, ListenError::TrySendError));
    }

    #[test]
    fn keyhive_error_conversion_panics_instead_of_becoming_try_send_error() {
        let result = std::panic::catch_unwind(|| {
            let _: BigRepoListenError<MemoryStorage> =
                BigRepoComposedHandlerError::Keyhive(KeyhiveHandleError::ActorGone).into();
        });
        assert!(result.is_err(), "keyhive errors should not be laundered");
    }
}
