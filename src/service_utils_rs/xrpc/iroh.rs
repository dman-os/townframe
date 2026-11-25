use super::expected_peer_error;
use async_lock::Semaphore;
use axum::Router;
use n0_future::task::JoinError;
use n0_future::task::JoinSet;
use service_client_iroh_rs::IrohHttpStream;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct IrohXrpcProtocolHandler {
    router: Router,
    connection_admission: Arc<Semaphore>,
    max_connections: usize,
    stream_admission: Arc<Semaphore>,
    max_streams: usize,
    shutdown: watch::Sender<bool>,
    shutdown_state: watch::Receiver<bool>,
}

impl std::fmt::Debug for IrohXrpcProtocolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IrohXrpcProtocolHandler")
            .field("max_connections", &self.max_connections)
            .field("max_streams", &self.max_streams)
            .finish_non_exhaustive()
    }
}

/// Records how a stream task ended. `true` means the stream failed in a way that ends the
/// connection: its error is kept in `first_error` and the caller aborts the others.
fn stream_failed(
    joined: Result<Result<(), hyper::Error>, JoinError>,
    first_error: &mut Option<String>,
) -> bool {
    match joined {
        Ok(Ok(())) => false,
        Ok(Err(error)) if expected_peer_error(&error) => false,
        Ok(Err(error)) => {
            first_error.get_or_insert(error.to_string());
            true
        }
        Err(error) if !error.is_cancelled() => {
            first_error.get_or_insert(error.to_string());
            true
        }
        Err(_) => false,
    }
}

async fn serve_iroh_stream(
    router: Router,
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
) -> Result<(), hyper::Error> {
    // Hyper may be waiting on an entered handler and therefore not poll the
    // transport for a peer reset.  Keep the send-side stop notification beside
    // the owned Hyper future so a client stream reset cancels that handler.
    let stopped = send.stopped();
    let io = IrohHttpStream::new(send, recv);
    tokio::select! {
        result = hyper::server::conn::http1::Builder::new()
            // HTTP/1 is deliberately one exchange per QUIC bi-stream.  The
            // QUIC connection itself remains reusable and is owned by the client.
            .keep_alive(false).half_close(true)
            // Hyper's header-read timeout needs a `hyper::rt::Timer`, whose `now` is a std
            // `Instant`, and std's `Instant::now` panics on wasm32-unknown-unknown, so no
            // n0-future timer can serve a browser. Hyper skips its 30s default without a timer
            // (and warns on every connection); turning it off states that. A peer that stalls
            // mid-header is bounded by the connection and stream admission limits and by
            // QUIC's idle timeout.
            .header_read_timeout(None)
            // Hyper stamps `Date` from std's `SystemTime::now`, which panics on
            // wasm32-unknown-unknown. The only client of this wire is `IrohHttp`, which
            // reads no `Date`, so no target sends one. This alone does not make hyper's
            // server run in a browser: its dispatcher refreshes the cached date on every
            // connection whether or not the header is on (`Http1Transaction::update_date`),
            // so a browser needs a hyper that skips that refresh when the header is off.
            .auto_date_header(false)
            .serve_connection(io, hyper_util::service::TowerToHyperService::new(router)).with_upgrades() => result,
        _ = stopped => Ok(()),
    }
}

impl iroh::protocol::ProtocolHandler for IrohXrpcProtocolHandler {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let _connection_permit = self.connection_admission.try_acquire_arc().ok_or_else(|| {
            iroh::protocol::AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "Iroh connection admission is full",
            ))
        })?;
        let router = self
            .router
            .clone()
            .layer(axum::Extension(crate::TransportContext::Iroh {
                endpoint: service_core_rs::EndpointId::from_bytes(
                    *connection.remote_id().as_bytes(),
                ),
            }));
        let mut shutdown = self.shutdown_state.clone();
        let mut streams = JoinSet::new();
        let mut first_error = None;
        'accept: loop {
            tokio::select! {
                biased;
                joined = streams.join_next(), if !streams.is_empty() => {
                    let joined = joined.expect("guarded JoinSet branch cannot be empty");
                    if stream_failed(joined, &mut first_error) { streams.abort_all(); break; }
                }
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { streams.abort_all(); break; } }
                closed = connection.closed() => { drop(closed); streams.abort_all(); break; }
                accepted = connection.accept_bi() => {
                    let (send, recv) = match accepted { Ok(stream) => stream, Err(_) => { streams.abort_all(); break; } };
                    // Waiting for stream capacity must not stop the connection from noticing
                    // finished streams (which free capacity), shutdown or a closed connection.
                    let permit = loop {
                        tokio::select! {
                            biased;
                            joined = streams.join_next(), if !streams.is_empty() => {
                                let joined = joined.expect("guarded JoinSet branch cannot be empty");
                                if stream_failed(joined, &mut first_error) { streams.abort_all(); break 'accept; }
                            }
                            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { streams.abort_all(); break 'accept; } }
                            closed = connection.closed() => { drop(closed); streams.abort_all(); break 'accept; }
                            permit = self.stream_admission.acquire_arc() => break permit,
                        }
                    };
                    let router = router.clone();
                    streams.spawn(async move { let result = serve_iroh_stream(router, send, recv).await; drop(permit); result });
                }
            }
        }
        while let Some(joined) = streams.join_next().await {
            stream_failed(joined, &mut first_error);
        }
        first_error.map_or(Ok(()), |error| {
            Err(iroh::protocol::AcceptError::from_err(
                std::io::Error::other(error),
            ))
        })
    }
    async fn shutdown(&self) {
        self.shutdown
            .send(true)
            .expect("Iroh protocol shutdown receiver must remain open");
    }
}

pub fn iroh_protocol_handler(
    router: Router,
    max_connections: usize,
    max_streams: usize,
) -> IrohXrpcProtocolHandler {
    assert!(
        max_connections > 0,
        "Iroh connection limit must be positive"
    );
    assert!(max_streams > 0, "Iroh stream limit must be positive");
    let (shutdown, shutdown_state) = watch::channel(false);
    IrohXrpcProtocolHandler {
        router,
        connection_admission: Arc::new(Semaphore::new(max_connections)),
        max_connections,
        stream_admission: Arc::new(Semaphore::new(max_streams)),
        max_streams,
        shutdown,
        shutdown_state,
    }
}
