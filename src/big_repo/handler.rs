//! Adapted from `subduction_cli/src/handler.rs`.
//! Original license: Apache-2.0/MIT. (c) 2024 Ink & Switch
//!
//! Composed handler dispatching to sync, ephemeral, and keyhive sub-handlers.
//!
//! Two explicit [`Handler`] impls (one for [`Sendable`], one for [`Local`])
//! so `BoxFuture` / `LocalBoxFuture` can be returned with the correct Send
//! bound — the generic `FutureForm::from_future` approach can't express this
//! at the call site without two separate `impl` blocks.

use crate::interlude::*;

use crate::keyhive_conn::BigRepoKeyhiveConnAdapter;
use crate::keyhive_storage::BigRepoKeyhiveStorage;
// Retained for concrete aliases below; will be removed when aliases move out.
use crate::runtime::BigRepoIrohTransport;
use crate::wire::BigRepoWireMessage;
use future_form::{Local, Sendable};
use futures::future::{BoxFuture, LocalBoxFuture};
use subduction_core::{
    authenticated::Authenticated,
    connection::message::SyncMessage,
    handler::Handler,
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    subduction::error::{IoError, ListenError},
};
use subduction_ephemeral::{
    clock::std_clock::StdClock, handler::EphemeralHandler, message::EphemeralMessage,
    policy::OpenEphemeralPolicy,
};
use subduction_keyhive::{
    handler::{HandleError as KeyhiveHandleError, SendableKeyhiveHandler, SendableRuntimeProtocol},
    KeyhiveMessage,
};

// ─── Concrete type aliases (for old-runtime compatibility) ─────────────────
// These are concrete to `Sendable` / `BigRepoIrohTransport`. They do not
// import `subduction_websocket::tokio` and will be parameterised or moved
// in a follow-up.

/// The concrete keyhive protocol type for BigRepo.
pub(crate) type BigRepoKeyhiveProtocol = Arc<
    SendableRuntimeProtocol<
        crate::keyhive_listener::BigRepoKeyhiveListener,
        BigRepoKeyhiveConnAdapter,
        BigRepoKeyhiveStorage,
    >,
>;

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
    EphemeralHandler<Sendable, BigRepoIrohTransport, OpenEphemeralPolicy, StdClock>;

// ─── Generic composed error ────────────────────────────────────────────────

/// Error type for [`BigRepoComposedHandler`]. Wraps sync and keyhive
/// sub-handler errors. Ephemeral errors are logged and not propagated.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BigRepoComposedHandlerError<SyncErr, KeyhiveErr>
where
    SyncErr: std::error::Error + 'static,
    KeyhiveErr: std::error::Error + 'static,
{
    /// Sync handler error (fatal — tears down the connection).
    #[error(transparent)]
    Sync(SyncErr),

    /// Keyhive handler error (fatal — propagated explicitly).
    #[error("keyhive: {0}")]
    Keyhive(KeyhiveErr),
}

impl<S>
    From<
        BigRepoComposedHandlerError<
            ListenError<Sendable, S, BigRepoIrohTransport, SyncMessage>,
            KeyhiveHandleError,
        >,
    > for ListenError<Sendable, S, BigRepoIrohTransport, BigRepoWireMessage>
where
    S: subduction_core::storage::traits::Storage<Sendable> + core::fmt::Debug,
{
    fn from(
        value: BigRepoComposedHandlerError<
            ListenError<Sendable, S, BigRepoIrohTransport, SyncMessage>,
            KeyhiveHandleError,
        >,
    ) -> Self {
        match value {
            BigRepoComposedHandlerError::Sync(error) => match error {
                ListenError::IoError(error) => ListenError::IoError(match error {
                    IoError::Storage(error) => IoError::Storage(error),
                    IoError::ConnSend(error) => IoError::ConnSend(error),
                    IoError::ConnRecv(error) => IoError::ConnRecv(error),
                    IoError::ConnCall(error) => IoError::ConnCall(error),
                    IoError::BlobMismatch(error) => IoError::BlobMismatch(error),
                }),
                ListenError::TrySendError => ListenError::TrySendError,
            },
            BigRepoComposedHandlerError::Keyhive(error) => {
                panic!("keyhive handler error reached listen conversion: {error}")
            }
        }
    }
}

// ─── Composed handler struct ──────────────────────────────────────────────

/// Composed handler that dispatches to sync, ephemeral, and keyhive
/// sub-handlers. The struct itself carries no `FutureForm` generic; the
/// two [`Handler`] impls below specialise for [`Sendable`] and [`Local`].
///
/// Dispatch semantics (identical to the old concrete version):
/// - **Sync errors** are fatal (returned as `Err`).
/// - **Ephemeral errors** are logged and treated as non-fatal.
/// - **Keyhive errors** are fatal (returned as `Err`).
///
/// The ephemeral sub-handler is optional (`Option<Arc<EH>>`), so relay-only
/// runtime2 configurations can omit it entirely.
pub(crate) struct BigRepoComposedHandler<SH, EH, KH> {
    pub(crate) sync: Arc<SH>,
    pub(crate) ephemeral: Option<Arc<EH>>,
    pub(crate) keyhive: KH,
}

// ─── Debug ────────────────────────────────────────────────────────────────

impl<SH, EH, KH> core::fmt::Debug for BigRepoComposedHandler<SH, EH, KH> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BigRepoComposedHandler")
            .finish_non_exhaustive()
    }
}

// ─── Constructor ──────────────────────────────────────────────────────────

impl<SH, EH, KH> BigRepoComposedHandler<SH, EH, KH> {
    /// Construct a new composed handler.
    ///
    /// `ephemeral` is optional — pass `None` to omit ephemeral dispatch
    /// (e.g. in relay-only runtime2 configurations).
    pub(crate) fn new(sync: Arc<SH>, ephemeral: Option<Arc<EH>>, keyhive: KH) -> Self {
        Self { sync, ephemeral, keyhive }
    }
}

// ─── RemoteHeadsNotifier ───────────────────────────────────────────────────

impl<SH, EH, KH> RemoteHeadsNotifier for BigRepoComposedHandler<SH, EH, KH>
where
    SH: RemoteHeadsNotifier,
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

// ═══════════════════════════════════════════════════════════════════════════
// Handler impl — Sendable (multi-threaded runtimes)
// ═══════════════════════════════════════════════════════════════════════════

impl<C, SH, EH, KH> Handler<Sendable, C> for BigRepoComposedHandler<SH, EH, KH>
where
    C: Clone + Send + Sync + 'static,
    SH: Handler<Sendable, C, Message = SyncMessage> + Send + Sync,
    SH::HandlerError: 'static,
    EH: Handler<Sendable, C, Message = EphemeralMessage> + Send + Sync,
    EH::HandlerError: 'static,
    KH: Handler<Sendable, C, Message = KeyhiveMessage> + Send + Sync,
    KH::HandlerError: 'static,
{
    type Message = BigRepoWireMessage;
    type HandlerError = BigRepoComposedHandlerError<SH::HandlerError, KH::HandlerError>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, Sendable>,
        message: BigRepoWireMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                BigRepoWireMessage::Sync(sync_msg) => {
                    Handler::<Sendable, C>::handle(self.sync.as_ref(), conn, *sync_msg)
                        .await
                        .map_err(BigRepoComposedHandlerError::Sync)
                }
                BigRepoWireMessage::Ephemeral(ephemeral_msg) => {
                    if let Some(ref eph) = self.ephemeral {
                        if let Err(err) =
                            Handler::<Sendable, C>::handle(eph.as_ref(), conn, ephemeral_msg).await
                        {
                            tracing::error!(%err, "ephemeral handler error");
                        }
                    }
                    Ok(())
                }
                BigRepoWireMessage::Keyhive(keyhive_msg) => {
                    Handler::<Sendable, C>::handle(&self.keyhive, conn, keyhive_msg)
                        .await
                        .map_err(BigRepoComposedHandlerError::Keyhive)
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            Handler::<Sendable, C>::on_peer_disconnect(self.sync.as_ref(), peer).await;
            if let Some(ref eph) = self.ephemeral {
                Handler::<Sendable, C>::on_peer_disconnect(eph.as_ref(), peer).await;
            }
            Handler::<Sendable, C>::on_peer_disconnect(&self.keyhive, peer).await;
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Handler impl — Local (single-threaded runtimes, e.g. Wasm)
// ═══════════════════════════════════════════════════════════════════════════

impl<C, SH, EH, KH> Handler<Local, C> for BigRepoComposedHandler<SH, EH, KH>
where
    C: Clone + 'static,
    SH: Handler<Local, C, Message = SyncMessage>,
    SH::HandlerError: 'static,
    EH: Handler<Local, C, Message = EphemeralMessage>,
    EH::HandlerError: 'static,
    KH: Handler<Local, C, Message = KeyhiveMessage>,
    KH::HandlerError: 'static,
{
    type Message = BigRepoWireMessage;
    type HandlerError = BigRepoComposedHandlerError<SH::HandlerError, KH::HandlerError>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, Local>,
        message: BigRepoWireMessage,
    ) -> LocalBoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                BigRepoWireMessage::Sync(sync_msg) => {
                    Handler::<Local, C>::handle(self.sync.as_ref(), conn, *sync_msg)
                        .await
                        .map_err(BigRepoComposedHandlerError::Sync)
                }
                BigRepoWireMessage::Ephemeral(ephemeral_msg) => {
                    if let Some(ref eph) = self.ephemeral {
                        if let Err(err) =
                            Handler::<Local, C>::handle(eph.as_ref(), conn, ephemeral_msg).await
                        {
                            tracing::error!(%err, "ephemeral handler error");
                        }
                    }
                    Ok(())
                }
                BigRepoWireMessage::Keyhive(keyhive_msg) => {
                    Handler::<Local, C>::handle(&self.keyhive, conn, keyhive_msg)
                        .await
                        .map_err(BigRepoComposedHandlerError::Keyhive)
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> LocalBoxFuture<'_, ()> {
        Box::pin(async move {
            Handler::<Local, C>::on_peer_disconnect(self.sync.as_ref(), peer).await;
            if let Some(ref eph) = self.ephemeral {
                Handler::<Local, C>::on_peer_disconnect(eph.as_ref(), peer).await;
            }
            Handler::<Local, C>::on_peer_disconnect(&self.keyhive, peer).await;
        })
    }
}
