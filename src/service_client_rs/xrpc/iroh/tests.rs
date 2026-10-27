use super::*;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use iroh::protocol::Router;
use serde_json::json;
use service_core_rs::HttpInput;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

const ECHO_OPERATION: XrpcOperation = XrpcOperation {
    nsid: "org.example.test.echo",
    kind: XrpcKind::Procedure,
    method: HttpMethod::Post,
    input: HttpInput::JsonBody,
    output_schema: "EchoOutput",
    error_schema: "TestError",
};

/// Answers every HTTP/1 exchange with its own JSON request body and counts
/// the QUIC connections it admits.
#[derive(Debug)]
struct EchoHandler(Arc<AtomicUsize>);

impl ProtocolHandler for EchoHandler {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        self.0.fetch_add(1, Ordering::AcqRel);
        // The loop ends when the client closes the connection.
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let body = read_request_body(&mut recv).await;
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            tokio::io::AsyncWriteExt::write_all(&mut send, head.as_bytes()).await?;
            tokio::io::AsyncWriteExt::write_all(&mut send, &body).await?;
            send.finish()?;
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

#[tokio::test]
async fn retained_connection_is_shared_and_replaced_once_closed() {
    let server_endpoint = test_endpoint().await;
    let admitted = Arc::new(AtomicUsize::new(0));
    let server = Router::builder(server_endpoint.clone())
        .accept(IROH_XRPC_HTTP_ALPN, EchoHandler(Arc::clone(&admitted)))
        .spawn();
    let client_endpoint = test_endpoint().await;
    let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
        .expect("Iroh client");
    assert!(client.active_connection().await.is_none());
    let echo = |value: &'static str| {
        let client = client.clone();
        async move {
            client
                .execute::<Value, Value, Value>(
                    ECHO_OPERATION,
                    RequestContext::root(),
                    &HttpMetadata::default(),
                    &json!({ "value": value }),
                )
                .await
                .expect("echo exchange")
        }
    };

    // Both exchanges acquire before either has connected; the actor connects
    // once and hands the second requester the same connection.
    let (first, second) = tokio::join!(echo("first"), echo("second"));
    assert_eq!(first, json!({ "value": "first" }));
    assert_eq!(second, json!({ "value": "second" }));
    assert_eq!(admitted.load(Ordering::Acquire), 1);

    let retained = client
        .active_connection()
        .await
        .expect("retained connection");
    retained.close(0u32.into(), b"test closes the retained connection");
    assert_eq!(echo("third").await, json!({ "value": "third" }));
    assert_eq!(admitted.load(Ordering::Acquire), 2);
    let replacement = client
        .active_connection()
        .await
        .expect("replacement connection");
    assert_ne!(replacement.stable_id(), retained.stable_id());
    assert!(replacement.close_reason().is_none());

    server.shutdown().await.expect("router shutdown");
    client_endpoint.close().await;
    server_endpoint.close().await;
}
