//! The client against a hand-written HTTP/1 peer, so this crate tests without the service host.
//! The round trips against the real server live beside it, in `service_utils_rs`'s iroh wire
//! tests.

use super::*;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use iroh::protocol::Router;
use serde::Deserialize;
use serde::Serialize;
use service_client_core_rs::Client;
use service_core_rs::RequestContext;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Echo {
    value: String,
}

service_core_rs::service_method!(Echo => Echo, ContextError, procedure("org.example.test.echo"));

const RESPONSE_LIMIT: usize = 1024;

/// Answers every exchange with its own JSON request body, except a `pending` one, which it never
/// answers: it reports instead how the client ended that stream. Counts the QUIC connections
/// it admits.
#[derive(Debug)]
struct EchoHandler {
    admitted: Arc<AtomicUsize>,
    pending_ended: mpsc::UnboundedSender<Result<Option<iroh::endpoint::VarInt>, String>>,
}

impl ProtocolHandler for EchoHandler {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        self.admitted.fetch_add(1, Ordering::AcqRel);
        // The loop ends when the client closes the connection.
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let pending_ended = self.pending_ended.clone();
            tokio::spawn(async move {
                let body = read_request_body(&mut recv).await;
                let echo: Echo = serde_json::from_slice(&body).expect("echo request body");
                if echo.value == "pending" {
                    let stopped = send.stopped().await.map_err(|error| error.to_string());
                    pending_ended
                        .send(stopped)
                        .expect("the test awaits the pending exchange's end");
                    return;
                }
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let written = async {
                    tokio::io::AsyncWriteExt::write_all(&mut send, head.as_bytes()).await?;
                    tokio::io::AsyncWriteExt::write_all(&mut send, &body).await?;
                    send.finish()?;
                    std::io::Result::Ok(())
                }
                .await;
                // Only a client over its response limit stops the stream mid-response.
                if let Err(error) = written {
                    assert!(body.len() > RESPONSE_LIMIT, "response write: {error}");
                }
            });
        }
        Ok(())
    }
}

async fn read_request_body(recv: &mut iroh::endpoint::RecvStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        if let Some(head_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = std::str::from_utf8(&request[..head_end]).expect("ASCII request head");
            let length: usize = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().expect("numeric content-length"))
                })
                .expect("JSON body request carries content-length");
            let body = head_end + 4;
            if request.len() >= body + length {
                return request[body..body + length].to_vec();
            }
        }
        let read = tokio::io::AsyncReadExt::read(recv, &mut chunk)
            .await
            .expect("request read");
        assert_ne!(read, 0, "request stream ended before its body");
        request.extend_from_slice(&chunk[..read]);
    }
}

async fn test_endpoint() -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("loopback bind address")
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await
        .expect("Iroh endpoint")
}

struct Fixture {
    server: Router,
    server_endpoint: iroh::Endpoint,
    client_endpoint: iroh::Endpoint,
    client: Client<IrohHttp>,
    admitted: Arc<AtomicUsize>,
    pending_ended: mpsc::UnboundedReceiver<Result<Option<iroh::endpoint::VarInt>, String>>,
}

impl Fixture {
    async fn new() -> Self {
        let server_endpoint = test_endpoint().await;
        let admitted = Arc::new(AtomicUsize::new(0));
        let (pending_tx, pending_ended) = mpsc::unbounded_channel();
        let server = Router::builder(server_endpoint.clone())
            .accept(
                IROH_XRPC_HTTP_ALPN,
                EchoHandler {
                    admitted: Arc::clone(&admitted),
                    pending_ended: pending_tx,
                },
            )
            .spawn();
        let client_endpoint = test_endpoint().await;
        let client = Client::new(IrohHttp::new(
            client_endpoint.clone(),
            server_endpoint.addr(),
            RESPONSE_LIMIT,
        ));
        Self {
            server,
            server_endpoint,
            client_endpoint,
            client,
            admitted,
            pending_ended,
        }
    }

    async fn echo(&self, value: &str) -> Echo {
        self.client
            .call(
                RequestContext::root(),
                Echo {
                    value: value.to_owned(),
                },
            )
            .await
            .expect("echo exchange")
    }

    async fn shutdown(self) {
        self.server.shutdown().await.expect("router shutdown");
        self.client_endpoint.close().await;
        self.server_endpoint.close().await;
    }
}

#[tokio::test]
async fn retained_connection_is_shared_and_replaced_once_closed() {
    let fixture = Fixture::new().await;
    let transport = fixture.client.transport();
    assert!(transport.active_connection().await.is_none());

    // Both exchanges reach the connect before either has connected; the first connects while
    // the second waits on it and then reuses its connection.
    let (first, second) = tokio::join!(fixture.echo("first"), fixture.echo("second"));
    assert_eq!(first.value, "first");
    assert_eq!(second.value, "second");
    assert_eq!(fixture.echo("third").await.value, "third");
    assert_eq!(fixture.admitted.load(Ordering::Acquire), 1);

    let retained = transport
        .active_connection()
        .await
        .expect("retained connection");
    retained.close(0u32.into(), b"test closes the retained connection");
    assert_eq!(fixture.echo("fourth").await.value, "fourth");
    assert_eq!(fixture.admitted.load(Ordering::Acquire), 2);
    let replacement = transport
        .active_connection()
        .await
        .expect("replacement connection");
    assert_ne!(replacement.stable_id(), retained.stable_id());
    assert!(replacement.close_reason().is_none());

    fixture.shutdown().await;
}

/// A spent budget answers `DeadlineExceeded` and drops the exchange: its stream is stopped, which
/// is what the server cancels its handler on, and the connection stays up for the next call.
#[tokio::test]
async fn deadline_drops_the_exchange_and_stops_its_stream() {
    let mut fixture = Fixture::new().await;
    let result = fixture
        .client
        .call(
            RequestContext::root().with_time_budget(Duration::from_millis(100)),
            Echo {
                value: "pending".to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            result,
            Err(ClientError::Method(ContextError::DeadlineExceeded))
        ),
        "{result:?}"
    );
    let ended = fixture
        .pending_ended
        .recv()
        .await
        .expect("the server reports the pending exchange's end");
    assert_eq!(ended, Ok(Some(0u32.into())), "the client stops the stream");
    let connection = fixture
        .client
        .transport()
        .active_connection()
        .await
        .expect("retained connection");
    assert!(connection.close_reason().is_none());
    assert_eq!(fixture.echo("after").await.value, "after");
    assert_eq!(fixture.admitted.load(Ordering::Acquire), 1);
    fixture.shutdown().await;
}

/// A response over the limit fails the call as a transport failure whose outcome is unknown,
/// since a procedure may have run.
#[tokio::test]
async fn oversized_response_fails_the_call() {
    let fixture = Fixture::new().await;
    let result = fixture
        .client
        .call(
            RequestContext::root(),
            Echo {
                value: "x".repeat(2 * RESPONSE_LIMIT),
            },
        )
        .await;
    assert!(
        matches!(
            result,
            Err(ClientError::Transport {
                outcome_unknown: true,
                ..
            })
        ),
        "{result:?}"
    );
    fixture.shutdown().await;
}

/// A native host holds the typed client in `Send` futures (spawned tasks, boxed handlers).
#[test]
fn native_calls_are_send() {
    fn assert_send<T: Send>(_: &T) {}
    async fn call(client: Client<IrohHttp>) {
        drop(
            client
                .call(
                    RequestContext::root(),
                    Echo {
                        value: String::new(),
                    },
                )
                .await,
        );
    }
    let endpoint_addr = iroh::EndpointAddr::new(iroh::SecretKey::from_bytes(&[7; 32]).public());
    let future = async move {
        let endpoint = test_endpoint().await;
        call(Client::new(IrohHttp::new(endpoint, endpoint_addr, 1))).await;
    };
    assert_send(&future);
}
