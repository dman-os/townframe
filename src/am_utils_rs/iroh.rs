//! modified from https://github.com/n0-computer/iroh-examples/tree/8b40bb5557bacbfd817a4b66a931aec6af655b51/iroh-automerge-repo
//! unkown license??
//! Combines [`iroh`] with automerge's [`samod`] library, a library to create "automerge repositories"
//! in rust that speak the automerge repo protocol.

use crate::interlude::*;

use codec::Codec;
use samod::ConnDirection;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use super::AmCtx;

mod codec;

impl AmCtx {
    pub const SYNC_ALPN: &[u8] = b"townframe/automerge-repo/1";

    #[tracing::instrument(skip(self, endpoint))]
    pub async fn spawn_connection_iroh(
        &self,
        endpoint: &iroh::Endpoint,
        addr: iroh::EndpointAddr,
        // rx_from_peer: iroh::endpoint::RecvStream,
        // tx_to_peer: iroh::endpoint::SendStream,
        // direction: samod::ConnDirection,
    ) -> Res<super::RepoConnection> {
        let endpoint_id = addr.id;
        let conn = endpoint.connect(addr, Self::SYNC_ALPN).await?;
        let (tx, rx) = conn.open_bi().await?;

        let repo = self.repo.clone();
        let conn = tokio::task::block_in_place(|| {
            repo.connect(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
                ConnDirection::Outgoing,
            )
        })
        .wrap_err("failed to establish connection")?;
        let peer_info = conn
            .handshake_complete()
            .await
            .map_err(|err| ferr!("failed on handshake: {err:?}"))?;
        let cancel_token = CancellationToken::new();
        let join_handle = tokio::spawn({
            let cancel_token = cancel_token.clone();
            async move {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("iroh connection cancelled, dropping");
                    }
                    fin_reason = conn.finished() => {
                        info!(?fin_reason, "iroh connection finished");
                    }
                }
            }
            .instrument(tracing::info_span!(
                "iroh connector task",
                peer = ?peer_info
            ))
        });

        Ok(super::RepoConnection {
            peer_info,
            join_handle: Some(join_handle),
            cancel_token,
        })
    }
}

#[derive(Clone, Debug)]
pub struct IrohRepoProtocol {
    pub acx: AmCtx,
    pub conn_tx: tokio::sync::mpsc::UnboundedSender<crate::RepoConnection>,
}

impl iroh::protocol::ProtocolHandler for IrohRepoProtocol {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let endpoint_id = connection.remote_id();

        let (tx, rx) = connection.accept_bi().await?;

        let repo = self.acx.repo.clone();
        let conn = tokio::task::block_in_place(|| {
            repo.connect(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
                ConnDirection::Incoming,
            )
        })
        .map_err(iroh::protocol::AcceptError::from_err)?;

        let peer_info = conn
            .handshake_complete()
            .await
            .map_err(|err| Box::from(format!("failed on handshake: {err:?}")))
            .map_err(iroh::protocol::AcceptError::from_boxed)?;

        let span = tracing::info_span!(
            "iroh incoming connection task",
            peer = ?peer_info
        );

        let cancel_token = CancellationToken::new();
        self.conn_tx
            .send(crate::RepoConnection {
                join_handle: None,
                peer_info,
                cancel_token: cancel_token.clone(),
            })
            .expect(ERROR_CHANNEL);

        let _guard = span.enter();
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("connection cancelled, dropping");
            }
            fin_reason = conn.finished() => {
                info!(?fin_reason, "incoming connection finished");
                info!(?fin_reason, "sync server connector task finished");
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        // The AmCtx stop token owns repo shutdown; protocol shutdown only tears down routing.
    }
}
