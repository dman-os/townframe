//! modified from https://github.com/n0-computer/iroh-examples/tree/8b40bb5557bacbfd817a4b66a931aec6af655b51/iroh-automerge-repo
//! MIT/Apache 2.0
//! Combines [`iroh`] with automerge's [`samod`] library, a library to create "automerge repositories"
//! in rust that speak the automerge repo protocol.

use crate::interlude::*;

use codec::Codec;
use samod::{AcceptorEvent, Dialer, DialerEvent, Transport};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use crate::repo::{BigRepo, SharedBigRepo};

mod codec;

const CONN_URL_SCHEME: &str = "db+iroh+samod";

impl BigRepo {
    pub const SYNC_ALPN: &[u8] = b"townframe/automerge-repo/0";

    #[tracing::instrument(skip(self, endpoint, end_signal_tx))]
    pub async fn spawn_connection_iroh(
        &self,
        endpoint: &iroh::Endpoint,
        to_addr: iroh::EndpointAddr,
        end_signal_tx: Option<tokio::sync::mpsc::UnboundedSender<super::ConnFinishSignal>>,
        // direction: samod::ConnDirection,
        // rx_from_peer: iroh::endpoint::RecvStream,
        // tx_to_peer: iroh::endpoint::SendStream,
    ) -> Res<super::RepoConnection> {
        let endpoint_id = to_addr.id;
        let repo = self.samod_repo().clone();
        let dialer = IrohDialer {
            url: Url::parse(&format!(
                "{CONN_URL_SCHEME}:{}",
                utils_rs::hash::encode_base58_multibase(endpoint_id)
            ))
            .expect(ERROR_IMPOSSIBLE),
            endpoint: endpoint.clone(),
            endpoint_id,
            to_addr,
        };
        let handle = repo
            .dial(samod::BackoffConfig::default(), Arc::new(dialer))
            .wrap_err("error setting up dialer")?;

        let peer_info = handle
            .established()
            .await
            .wrap_err("error during handshake")?;
        let peer_id: Arc<str> = peer_info.peer_id.as_str().into();
        let conn_id = handle.connection_id().expect(ERROR_IMPOSSIBLE);

        let cancel_token = CancellationToken::new();
        let fut = {
            let cancel_token = cancel_token.clone();
            let peer_id = Arc::<str>::clone(&peer_id);
            async move {
                let mut events = handle.events();
                let mut last_reason = None;
                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            debug!("cancel token lit");
                            break;
                        }
                        evt = events.next() => {
                            let Some(evt) = evt else {
                                eyre::bail!(
                                    "connection stream ended befor disconnect"
                                );
                            };
                            match evt {
                                DialerEvent::Connected { peer_info } => {
                                    if peer_info.peer_id.as_str() != &peer_id[..] {
                                        eyre::bail!(
                                            "reconnection changed peer_id {peer_id} != {:?}",
                                            peer_info.peer_id
                                        );
                                    }
                                    info!("connection established");
                                }
                                DialerEvent::Disconnected { reason } => {
                                    last_reason = Some(reason);
                                    info!("connection lost");
                                },
                                DialerEvent::Reconnecting { attempt } => {
                                    info!(?attempt, "trying to reconnect to peer");
                                }
                                DialerEvent::MaxRetriesReached => {
                                    info!("max retries reached, aborting");
                                    if let Some(tx) = end_signal_tx {
                                        tx.send(super::ConnFinishSignal {
                                            conn_id,
                                            peer_id,
                                            // FIXME: find better reason type
                                            reason: last_reason.unwrap_or_else(|| "max re-connention atttempts reached".into()),
                                        })
                                        .inspect_err(|_| warn!("connection owner closed before finish"))
                                        .ok();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                eyre::Ok(())
            }
            .instrument(tracing::info_span!(
                "iroh connector task",
                peer = ?peer_info,
                endpoint_id = ?endpoint_id,
            ))
        };
        let join_handle = tokio::spawn(async {
            fut.await.unwrap();
        });

        Ok(super::RepoConnection {
            id: conn_id,
            peer_id,
            peer_info,
            endpoint_id: Some(endpoint_id),
            join_handle: Some(join_handle),
            cancel_token,
        })
    }
}

struct IrohDialer {
    url: Url,
    endpoint: iroh::Endpoint,
    endpoint_id: iroh::PublicKey,
    to_addr: iroh::EndpointAddr,
}

impl Dialer for IrohDialer {
    fn url(&self) -> Url {
        self.url.clone()
    }

    fn connect(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Transport, Box<dyn std::error::Error + Send + Sync>>,
                > + Send,
        >,
    > {
        let endpoint = self.endpoint.clone();
        let addr = self.to_addr.clone();
        let endpoint_id = self.endpoint_id;
        Box::pin(async move {
            let conn = endpoint.connect(addr, BigRepo::SYNC_ALPN).await?;
            let (tx, rx) = conn.open_bi().await?;
            // establish your transport here, then wrap it:
            Ok(Transport::new(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct IrohRepoProtocol {
    pub big_repo: SharedBigRepo,
    pub cancel_token: CancellationToken,
    pub conn_tx: tokio::sync::mpsc::UnboundedSender<crate::RepoConnection>,
    pub end_signal_tx: tokio::sync::mpsc::UnboundedSender<super::ConnFinishSignal>,
}

impl iroh::protocol::ProtocolHandler for IrohRepoProtocol {
    #[tracing::instrument(skip(connection))]
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let endpoint_id = connection.remote_id();
        tracing::record_all!(
            tracing::Span::current(),
            endpoint_id = ?endpoint_id,
        );

        let (tx, rx) = connection.accept_bi().await?;

        let repo = self.big_repo.samod_repo().clone();
        let acceptor = repo
            .make_acceptor(
                Url::parse(&format!(
                    "{CONN_URL_SCHEME}:{}",
                    utils_rs::hash::encode_base58_multibase(endpoint_id)
                ))
                .expect(ERROR_IMPOSSIBLE),
            )
            .expect("error making acceptor");
        acceptor
            .accept(Transport::new(
                FramedRead::new(rx, Codec::new(endpoint_id)),
                FramedWrite::new(tx, Codec::new(endpoint_id)),
            ))
            .map_err(|err| {
                Box::from(format!(
                    "failed making samod acceptor for {endpoint_id}: {err:?}"
                ))
            })
            .map_err(iroh::protocol::AcceptError::from_boxed)?;

        let mut events = acceptor.events();
        let event = events.next().await;
        let Some(event) = event else {
            return Err(iroh::protocol::AcceptError::from_boxed(Box::from(format!(
                "connection stream ended befor disconnect with {endpoint_id}"
            ))));
        };
        let (conn_id, peer_info) = match event {
            AcceptorEvent::ClientConnected {
                connection_id,
                peer_info,
            } => (connection_id, peer_info),
            AcceptorEvent::ClientDisconnected { reason, .. } => {
                return Err(iroh::protocol::AcceptError::from_boxed(Box::from(format!(
                    "failed on handshake with {endpoint_id}: {reason:?}"
                ))));
            }
        };

        let peer_id: Arc<str> = peer_info.peer_id.as_str().into();

        tracing::record_all!(
            tracing::Span::current(),
            peer = ?peer_info,
        );

        let cancel_token = self.cancel_token.child_token();
        self.conn_tx
            .send(crate::RepoConnection {
                id: conn_id,
                peer_id: Arc::<str>::clone(&peer_id),
                join_handle: None,
                peer_info,
                endpoint_id: Some(endpoint_id),
                cancel_token: cancel_token.clone(),
            })
            .expect(ERROR_CHANNEL);

        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("cancel token lit");
            }
            evt = events.next() => {
                let Some(evt) = evt else {
                    return Err(iroh::protocol::AcceptError::from_boxed(Box::from(format!(
                        "connection stream ended befor disconnect with {endpoint_id}"
                    ))));
                };
                match evt {
                    AcceptorEvent::ClientDisconnected {
                        reason,
                        ..
                    } => {
                        info!(?reason, "incoming connection finished");
                        self.end_signal_tx.send(super::ConnFinishSignal {
                            conn_id,
                            peer_id,
                            reason: format!("{reason}"),
                        })
                        .inspect_err(|_| warn!("connection owner closed before finish"))
                        .ok();
                    },
                    AcceptorEvent::ClientConnected {..} => {
                        unreachable!()
                    }
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        self.cancel_token.cancel();
        // BigRepo shutdown owns samod repo shutdown; protocol shutdown only tears down routing.
    }
}
