//! Adapted from `subduction_cli/src/keyhive.rs`.
//! Original license: Apache-2.0/MIT. (c) 2024 Ink & Switch

use crate::runtime::BigRepoIrohTransport;
use crate::wire::BigRepoWireMessage;

use core::convert::Infallible;
use future_form::Sendable;
use futures::future::BoxFuture;
use futures::FutureExt;
use subduction_core::{authenticated::Authenticated, connection::Connection};
use subduction_keyhive::{
    connection::KeyhiveConnection,
    peer_id::KeyhivePeerId,
    signed_message::{CborError, SignedMessage},
    KeyhiveMessage,
};

/// Errors from [`BigRepoKeyhiveConnAdapter::send`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum BigRepoKeyhiveSendError<E: std::error::Error + 'static> {
    /// Serializing the [`SignedMessage`] to CBOR failed.
    #[error("serialize signed message: {0}")]
    Serialize(#[from] CborError),
    /// The underlying subduction transport failed to send.
    #[error("send via big repo conn: {0}")]
    Transport(E),
}

/// Wraps an [`Authenticated`] connection as a [`KeyhiveConnection`],
/// framing outbound keyhive messages as [`BigRepoWireMessage::Keyhive`].
#[derive(Debug)]
pub(crate) struct BigRepoKeyhiveConnAdapter<C = BigRepoIrohTransport>
where
    C: Clone,
{
    auth: Authenticated<C, Sendable>,
}

impl<C> Clone for BigRepoKeyhiveConnAdapter<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            auth: self.auth.clone(),
        }
    }
}

impl<C> BigRepoKeyhiveConnAdapter<C>
where
    C: Clone,
{
    pub(crate) const fn new(auth: Authenticated<C, Sendable>) -> Self {
        Self { auth }
    }
}

impl<C> KeyhiveConnection<Sendable> for BigRepoKeyhiveConnAdapter<C>
where
    C: Connection<Sendable, BigRepoWireMessage> + Clone + Send + Sync + 'static,
    C::SendError: std::error::Error + 'static,
{
    type SendError = BigRepoKeyhiveSendError<C::SendError>;
    type RecvError = Infallible;
    type DisconnectError = Infallible;

    fn peer_id(&self) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes(*self.auth.peer_id().as_bytes())
    }

    fn send(&self, message: SignedMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            let msg = BigRepoWireMessage::Keyhive(KeyhiveMessage::from_signed(&message)?);
            C::send(self.auth.inner(), &msg)
                .await
                .map_err(BigRepoKeyhiveSendError::Transport)?;
            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<SignedMessage, Self::RecvError>> {
        futures::future::pending().boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectError>> {
        futures::future::ready(Ok(())).boxed()
    }
}
