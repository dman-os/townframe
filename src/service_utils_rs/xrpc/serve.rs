//! Bounded HTTP/1 serving over TCP: a connection limit, per-connection tasks,
//! and a shutdown that aborts and joins rather than leaking.

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinSet,
};

pub(super) type AcceptFuture<'a> = Pin<
    Box<
        dyn Future<Output = std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>
            + Send
            + 'a,
    >,
>;

/// A production accept driver. Tests can provide a deterministic driver to
/// exercise listener failures without replacing the connection lifecycle.
pub(super) trait ConnectionAcceptor: Send {
    fn accept(&mut self) -> AcceptFuture<'_>;
}

struct TcpConnectionAcceptor {
    listener: tokio::net::TcpListener,
}

impl ConnectionAcceptor for TcpConnectionAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        Box::pin(self.listener.accept())
    }
}

/// Starts a loopback-friendly HTTP server and returns its owned task and
/// shutdown sender. Production callers can instead attach `into_router()` to
/// an Iroh HTTP listener.
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ),
    std::io::Error,
> {
    serve_with_connection_limit(listener, router, 1024).await
}

/// Starts the unary server with an independent bound on accepted/idle
/// connections. Handler admission and connection admission are separate: an
/// idle peer cannot consume a handler permit, and excess peers wait at the
/// listener boundary instead of being accepted and immediately closed.
pub async fn serve_with_connection_limit(
    listener: tokio::net::TcpListener,
    router: Router,
    max_connections: usize,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ),
    std::io::Error,
> {
    assert!(
        max_connections > 0,
        "XRPC connection limit must be positive"
    );
    serve_with_acceptor(TcpConnectionAcceptor { listener }, router, max_connections).await
}

pub(super) async fn serve_with_acceptor<A: ConnectionAcceptor + 'static>(
    mut acceptor: A,
    router: Router,
    max_connections: usize,
) -> Result<
    (
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
        oneshot::Sender<()>,
    ),
    std::io::Error,
> {
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let connection_admission = Arc::new(Semaphore::new(max_connections));
        let mut connections = JoinSet::new();
        let mut shutdown_receiver = shutdown_receiver;
        loop {
            let accept = accept_with_permit(&mut acceptor, Arc::clone(&connection_admission));
            tokio::pin!(accept);
            if connections.is_empty() {
                tokio::select! {
                    accepted = &mut accept => {
                        match accepted {
                            (permit, Ok((stream, _))) => spawn_connection(&mut connections, router.clone(), permit, stream),
                            (permit, Err(error)) => {
                                drop(permit);
                                return abort_and_reap(&mut connections, error).await;
                            }
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        return reap_shutdown_connections(&mut connections).await;
                    }
                }
            } else {
                tokio::select! {
                    accepted = &mut accept => {
                        match accepted {
                            (permit, Ok((stream, _))) => spawn_connection(&mut connections, router.clone(), permit, stream),
                            (permit, Err(error)) => {
                                drop(permit);
                                return abort_and_reap(&mut connections, error).await;
                            }
                        }
                    }
                    joined = connections.join_next() => {
                        if let Some(joined) = joined
                            && let Some(error) = observe_connection(joined)
                        {
                            connections.abort_all();
                            if let Err(reap_error) = reap_shutdown_connections(&mut connections).await {
                                panic!("XRPC connection reaping failed after task error: {reap_error}");
                            }
                            return Err(error);
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        return reap_shutdown_connections(&mut connections).await;
                    }
                }
            }
        }
    });
    Ok((task, shutdown_sender))
}

async fn accept_with_permit<A: ConnectionAcceptor>(
    acceptor: &mut A,
    connection_admission: Arc<Semaphore>,
) -> (
    OwnedSemaphorePermit,
    std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>,
) {
    let permit = connection_admission
        .acquire_owned()
        .await
        .expect("connection admission semaphore cannot close");
    (permit, acceptor.accept().await)
}

fn spawn_connection(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
    router: Router,
    connection_permit: OwnedSemaphorePermit,
    stream: tokio::net::TcpStream,
) {
    connections.spawn(async move {
        let _connection_permit = connection_permit;
        let service = TowerToHyperService::new(router);
        http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
    });
}

async fn abort_and_reap(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
    listener_error: std::io::Error,
) -> Result<(), std::io::Error> {
    if let Err(reap_error) = reap_shutdown_connections(connections).await {
        panic!("XRPC connection reaping failed after listener error: {reap_error}");
    }
    Err(listener_error)
}

fn observe_connection(
    joined: Result<Result<(), hyper::Error>, tokio::task::JoinError>,
) -> Option<std::io::Error> {
    match joined {
        Ok(Ok(())) => None,
        Ok(Err(error)) if expected_peer_error(&error) => {
            tracing::debug!(%error, "XRPC peer connection closed");
            None
        }
        Ok(Err(error)) => Some(std::io::Error::other(error.to_string())),
        Err(error) if error.is_cancelled() => None,
        Err(error) => panic!("XRPC connection task failed: {error}"),
    }
}

pub(super) fn expected_peer_error(error: &hyper::Error) -> bool {
    error.is_closed()
        || error.is_canceled()
        || error.is_incomplete_message()
        || error.is_body_write_aborted()
        || error.is_shutdown()
}

async fn reap_shutdown_connections(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
) -> Result<(), std::io::Error> {
    connections.abort_all();
    let mut first_error = None;
    while let Some(joined) = connections.join_next().await {
        if let Some(error) = observe_connection(joined) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}
