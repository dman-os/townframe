//! A caller that goes away cancels its handler on every transport, and a WebSocket subscription
//! lives no longer than its socket and its host. Each test parks a call in a handler that never
//! finishes and waits for what the handler holds to be dropped; a regression hangs the test until
//! the runner's timeout.

use super::wait_for_activity;
use crate::Admission;
use crate::CallOrigin;
use crate::Envelope;
use crate::Handler;
use crate::Host;
use crate::IrpcAdapter;
use crate::RequestContext;
use crate::ResponseEnvelope;
use crate::Route;
use crate::StopToken;
use crate::XrpcRouter;
use method_core::ContextError;
use method_core::EventStream;
use method_core::Method;
use method_core::Subscription;
use method_core::XrpcMethod;
use serde::Deserialize;
use serde::Serialize;
use service_client_native_rs::Client;
use service_client_native_rs::Transport;
use service_client_native_rs::xrpc::ws::WsIo;
use service_client_native_rs::xrpc::ws::WsMessage;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::oneshot;

/// A long poll that never answers.
#[derive(Debug, Serialize, Deserialize)]
struct Hold {}

impl Method for Hold {
    type Output = ();
    type Error = ContextError;
    const XRPC: XrpcMethod = XrpcMethod::query("org.example.hold.get").long_poll();
}

/// A subscription that never sends an event.
#[derive(Debug, Serialize, Deserialize)]
struct Watch {}

impl Method for Watch {
    type Output = EventStream<u64, ContextError>;
    type Error = ContextError;
    const XRPC: XrpcMethod = XrpcMethod::subscription("org.example.hold.watch");
}

impl Subscription for Watch {
    type Event = u64;
}

crate::service_protocol! {
    enum HoldProtocol => HoldMessage {
        unary { Hold(Hold) }
        stream { Watch(Watch) }
    }
}

/// Notifies `entered` when a call reaches it, and `dropped` when what the call holds goes away:
/// the pending handler of a `Hold`, the idle event stream of a `Watch`.
#[derive(Default)]
struct Holder {
    entered: Arc<Notify>,
    dropped: Arc<Notify>,
}

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl Handler<Hold> for Holder {
    async fn call(&self, _: Envelope<Hold>) -> Result<ResponseEnvelope<()>, ContextError> {
        let _held = NotifyOnDrop(Arc::clone(&self.dropped));
        self.entered.notify_one();
        std::future::pending().await
    }
}

impl Handler<Watch> for Holder {
    async fn call(
        &self,
        _: Envelope<Watch>,
    ) -> Result<ResponseEnvelope<EventStream<u64, ContextError>>, ContextError> {
        let held = NotifyOnDrop(Arc::clone(&self.dropped));
        self.entered.notify_one();
        let idle = futures::stream::poll_fn(move |_| {
            let _held = &held;
            Poll::Pending
        });
        Ok(ResponseEnvelope::from(Box::pin(idle) as EventStream<_, _>))
    }
}

type HoldHost = Arc<Host<Arc<Holder>>>;

fn boot() -> (Arc<Holder>, HoldHost, StopToken) {
    let holder = Arc::new(Holder::default());
    let (host, stop) = Host::new(Arc::clone(&holder));
    (holder, host, stop)
}

/// A router over `host` with one ordinary and one long-poll admission slot, so a slot that is
/// not given back refuses the next call.
fn router(host: &HoldHost) -> XrpcRouter {
    let mut router = XrpcRouter::with_admission_limits(1024, 1, 1);
    HoldProtocol::mount(&mut router, Arc::clone(host)).expect("mount hold protocol");
    router
}

async fn serve_tcp(
    router: XrpcRouter,
) -> (
    SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
    oneshot::Sender<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = crate::xrpc::serve(listener, router.into_router())
        .await
        .expect("start HTTP server");
    (address, task, shutdown)
}

/// Sends a request head and leaves the connection open.
async fn send_head(address: SocketAddr, head: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    stream
        .write_all(format!("{head}Host: {address}\r\n\r\n").as_bytes())
        .await
        .expect("write request head");
    stream
}

/// Opens a `Watch` socket the way the XRPC client does, and returns it after the 101.
async fn open_watch(address: SocketAddr) -> TcpStream {
    let mut stream = send_head(
        address,
        &format!(
            "GET /xrpc/{} HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n",
            Watch::XRPC.nsid
        ),
    )
    .await;
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await.expect("read response head"));
    }
    let head = String::from_utf8(head).expect("response head is text");
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    stream
}

#[tokio::test]
async fn dropping_an_irpc_caller_cancels_its_admitted_handler() {
    let (holder, host, stop) = boot();
    let admission = Admission::new(1, 1);
    let (irpc, irpc_stop) =
        IrpcAdapter::local::<_, HoldProtocol>(Arc::clone(&host), Arc::clone(&admission), 1);
    let client = Client::new(Transport::irpc(irpc, CallOrigin::LocalNode));
    for _ in 0..2 {
        let call = tokio::spawn({
            let client = client.clone();
            async move { client.call(RequestContext::root(), Hold {}).await }
        });
        holder.entered.notified().await;
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        holder.dropped.notified().await;
        // The single handler slot is free again, so the second round is admitted at all.
        wait_for_activity(&admission, |activity| activity.active_count() == 0).await;
    }
    irpc_stop.then(stop).stop().await;
}

/// Hyper notices the peer's close while the handler is entered and drops the exchange, which is
/// what cancels an XRPC handler over TCP.
#[tokio::test]
async fn a_tcp_disconnect_drops_the_entered_handler_and_its_long_poll_slot() {
    let (holder, host, stop) = boot();
    let (address, task, shutdown) = serve_tcp(router(&host)).await;
    for _ in 0..2 {
        let peer = send_head(
            address,
            &format!("GET /xrpc/{} HTTP/1.1\r\n", Hold::XRPC.nsid),
        )
        .await;
        // On the second round, a slot that was not given back answers 429 and never enters.
        holder.entered.notified().await;
        drop(peer);
        holder.dropped.notified().await;
    }
    shutdown.send(()).expect("HTTP server is serving");
    task.await
        .expect("HTTP server join")
        .expect("HTTP server exit");
    stop.stop().await;
}

#[tokio::test]
async fn closing_an_idle_websocket_ends_its_pump_and_frees_its_slot() {
    let (holder, host, stop) = boot();
    let (address, task, shutdown) = serve_tcp(router(&host)).await;
    for _ in 0..2 {
        // On the second round, a slot that was not given back refuses the upgrade with a 429.
        let socket = open_watch(address).await;
        holder.entered.notified().await;
        drop(socket);
        holder.dropped.notified().await;
    }
    assert_eq!(host.spawner().live_tasks(), 0);
    shutdown.send(()).expect("HTTP server is serving");
    task.await
        .expect("HTTP server join")
        .expect("HTTP server exit");
    stop.stop().await;
}

/// The pump belongs to the host, so stopping the host ends an idle subscription, tells the client
/// why, and joins the pump; nothing is left running after the server is gone.
#[tokio::test]
async fn stopping_the_host_ends_an_idle_websocket_and_joins_its_pump() {
    let (holder, host, stop) = boot();
    let (address, task, shutdown) = serve_tcp(router(&host)).await;
    let mut socket = WsIo::new(open_watch(address).await, true);
    holder.entered.notified().await;
    let Some(WsMessage::Text(open)) = socket.read_message().await.expect("open frame") else {
        panic!("the subscription opens with its metadata");
    };
    assert!(open.starts_with(r#"{"open":"#), "{open}");
    shutdown.send(()).expect("HTTP server is serving");
    task.await
        .expect("HTTP server join")
        .expect("HTTP server exit");
    stop.stop().await;
    holder.dropped.notified().await;
    let Some(WsMessage::Text(error)) = socket.read_message().await.expect("error frame") else {
        panic!("a stopped subscription ends with its error");
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&error).expect("error frame is JSON"),
        serde_json::json!({ "error": "Cancelled" })
    );
    assert!(matches!(
        socket.read_message().await.expect("close frame"),
        Some(WsMessage::Close) | None
    ));
}

/// A client cancels an iroh exchange by dropping its streams (STOP_SENDING), which drops the
/// entered handler and gives back the long-poll slot it held.
#[cfg(feature = "iroh-http")]
#[tokio::test]
async fn an_iroh_stream_cancel_releases_the_long_poll_slot() {
    async fn endpoint() -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("loopback bind address")
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .expect("bind endpoint")
    }
    let (holder, host, stop) = boot();
    let server = endpoint().await;
    let server_router = iroh::protocol::Router::builder(server.clone())
        .accept(
            service_client_iroh_rs::IROH_XRPC_HTTP_ALPN,
            crate::xrpc::iroh_protocol_handler(router(&host).into_router(), 1, 2),
        )
        .spawn();
    let client = endpoint().await;
    let connection = client
        .connect(server.addr(), service_client_iroh_rs::IROH_XRPC_HTTP_ALPN)
        .await
        .expect("connect");
    for _ in 0..2 {
        let (mut send, recv) = connection.open_bi().await.expect("open stream");
        send.write_all(
            format!(
                "GET /xrpc/{} HTTP/1.1\r\nHost: peer\r\n\r\n",
                Hold::XRPC.nsid
            )
            .as_bytes(),
        )
        .await
        .expect("write request head");
        // On the second round, a slot that was not given back answers 429 and never enters.
        holder.entered.notified().await;
        drop((send, recv));
        holder.dropped.notified().await;
    }
    assert!(connection.close_reason().is_none());
    server_router.shutdown().await.expect("router shutdown");
    client.close().await;
    server.close().await;
    stop.stop().await;
}
