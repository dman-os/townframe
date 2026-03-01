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
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<super::ConnFinishSignal>>,
        // direction: samod::ConnDirection,
        // rx_from_peer: iroh::endpoint::RecvStream,
        // tx_to_peer: iroh::endpoint::SendStream,
    ) -> Res<super::RepoConnection> {
        let endpoint_id = addr.id;
        let conn = endpoint.connect(addr, Self::SYNC_ALPN).await?;
        let (tx, rx) = conn.open_bi().await?;

        let repo = self.repo.clone();
        let conn = tokio::task::block_in_place(|| {
            repo.connect(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
                samod::ConnDirection::Outgoing,
            )
        })
        .wrap_err("failed to establish connection")?;

        let conn_id = conn.id();
        let peer_info = conn
            .handshake_complete()
            .await
            .map_err(|err| ferr!("failed on handshake: {err:?}"))?;
        let peer_id: Arc<str> = peer_info.peer_id.as_str().into();

        let cancel_token = CancellationToken::new();
        let join_handle = tokio::spawn({
            let cancel_token = cancel_token.clone();
            let peer_id = peer_id.clone();
            async move {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("cancel token lit");
                    }
                    fin_reason = conn.finished() => {
                        info!(?fin_reason, "iroh connection finished");
                        if let Some(tx) = end_signal_tx {
                            tx.send(super::ConnFinishSignal{ peer_id, reason: fin_reason })
                                .inspect_err(|_| warn!("connection owner closed before finish"))
                                .ok();
                        }
                    }
                }
            }
            .instrument(tracing::info_span!(
                "iroh connector task",
                peer = ?peer_info
            ))
        });

        Ok(super::RepoConnection {
            id: conn_id,
            peer_id,
            peer_info,
            join_handle: Some(join_handle),
            cancel_token,
        })
    }
}

#[derive(Clone, Debug)]
pub struct IrohRepoProtocol {
    pub acx: AmCtx,
    pub cancel_token: CancellationToken,
    pub conn_tx: tokio::sync::mpsc::UnboundedSender<crate::RepoConnection>,
    pub end_signal_tx: tokio::sync::mpsc::UnboundedSender<super::ConnFinishSignal>,
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

        let conn_id = conn.id();
        let peer_info = conn
            .handshake_complete()
            .await
            .map_err(|err| Box::from(format!("failed on handshake: {err:?}")))
            .map_err(iroh::protocol::AcceptError::from_boxed)?;
        let peer_id: Arc<str> = peer_info.peer_id.as_str().into();

        let span = tracing::info_span!(
            "iroh incoming connection task",
            peer = ?peer_info
        );

        let cancel_token = self.cancel_token.child_token();
        self.conn_tx
            .send(crate::RepoConnection {
                id: conn_id,
                peer_id: peer_id.clone(),
                join_handle: None,
                peer_info,
                cancel_token: cancel_token.clone(),
            })
            .expect(ERROR_CHANNEL);

        let _guard = span.enter();
        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("cancel token lit");
            }
            fin_reason = conn.finished() => {
                info!(?fin_reason, "incoming connection finished");
                self.end_signal_tx.send(super::ConnFinishSignal{ peer_id, reason: fin_reason })
                    .inspect_err(|_| warn!("connection owner closed before finish"))
                    .ok();
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        self.cancel_token.cancel();
        // The AmCtx stop token owns repo shutdown; protocol shutdown only tears down routing.
    }
}
