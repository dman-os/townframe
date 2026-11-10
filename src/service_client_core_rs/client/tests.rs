use super::*;
use crate::xrpc::HttpTransport;
use crate::xrpc::HttpTransportError;
use crate::xrpc::Xrpc;
use crate::xrpc::XrpcRequest;
use crate::xrpc::XrpcResponse;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::ResponseMetadata;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SampleEvent {
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SubscribeEvents {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Echo {
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    value: String,
}

service_core_rs::service_method!(SubscribeEvents => stream SampleEvent, ContextError, "org.example.client.subscribeEvents");
service_core_rs::service_method!(Echo => String, ContextError, query("org.example.client.echo"));
service_core_rs::service_method!(Record => String, ContextError, procedure("org.example.client.record"));

fn subscribed(
    frames: Vec<Result<EventFrame<SampleEvent>, ClientError<ContextError>>>,
) -> Subscribed<SubscribeEvents> {
    Subscribed {
        frames: futures::stream::iter(frames).boxed(),
        scope: None,
    }
}

fn open() -> Result<EventFrame<SampleEvent>, ClientError<ContextError>> {
    Ok(EventFrame::Open(ResponseMetadata::default()))
}

fn event(value: &str) -> Result<EventFrame<SampleEvent>, ClientError<ContextError>> {
    Ok(EventFrame::Event(SampleEvent {
        value: value.into(),
    }))
}

use futures::StreamExt;

/// The smallest transport: it answers in memory, the way a guest transport answers from its host
/// import. It is also what the wasm builds of this crate compile its tests against.
struct Memory;

impl UnaryTransport<Echo> for Memory {
    async fn call(
        &self,
        request: Envelope<Echo>,
    ) -> Result<ResponseEnvelope<String>, ClientError<ContextError>> {
        Ok(ResponseEnvelope::from(request.params.value))
    }
}

impl SubscriptionTransport<SubscribeEvents> for Memory {
    async fn subscribe(
        &self,
        _request: Envelope<SubscribeEvents>,
        _buffer: usize,
    ) -> Result<Subscribed<SubscribeEvents>, ClientError<ContextError>> {
        Ok(subscribed(vec![open(), event("one"), Ok(EventFrame::End)]))
    }
}

#[tokio::test]
async fn client_calls_and_subscribes_through_a_transport() {
    let client = Client::new(Memory);
    assert_eq!(
        client
            .call(
                RequestContext::root(),
                Echo {
                    value: "typed".into()
                }
            )
            .await
            .unwrap(),
        "typed"
    );
    let mut events = client
        .subscribe(RequestContext::root(), SubscribeEvents {})
        .await
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap(),
        Some(SampleEvent {
            value: "one".into()
        })
    );
    assert_eq!(events.recv().await.unwrap(), None);
}

#[tokio::test]
async fn cancelled_context_fails_the_call_before_the_transport_answers() {
    let context = RequestContext::root();
    context.cancel();
    assert_eq!(
        Client::new(Memory)
            .call(
                context,
                Echo {
                    value: "late".into()
                }
            )
            .await,
        Err(ClientError::Operation(ContextError::Cancelled))
    );
}

/// Answers over the XRPC envelope like the host router: it records the request it was handed and
/// replies with a fixed JSON body.
struct Recorded {
    requests: Mutex<Vec<XrpcRequest>>,
    status: StatusCode,
    body: &'static str,
}

impl HttpTransport for &Recorded {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        self.requests.lock().unwrap().push(request);
        Ok(XrpcResponse {
            status: self.status,
            headers: HeaderMap::new(),
            body: self.body.as_bytes().to_vec(),
        })
    }
}

#[tokio::test]
async fn xrpc_transport_encodes_the_envelope_and_decodes_the_response() {
    let http = Recorded {
        requests: Mutex::new(Vec::new()),
        status: StatusCode::OK,
        body: "\"echoed\"",
    };
    let client = Client::new(Xrpc(&http));
    let metadata = HttpMetadata {
        authorization: Some("Bearer token".into()),
        idempotency_key: Some("key-1".into()),
    };
    assert_eq!(
        client
            .call_with(
                RequestContext::root(),
                metadata.clone(),
                Echo {
                    value: "a b".into()
                }
            )
            .await
            .unwrap(),
        "echoed"
    );
    assert_eq!(
        client
            .call(RequestContext::root(), Record { value: "x".into() })
            .await
            .unwrap(),
        "echoed"
    );
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(requests[0].path, "xrpc/org.example.client.echo?value=a+b");
    assert!(requests[0].body.is_empty());
    assert_eq!(requests[0].headers["authorization"], "Bearer token");
    assert_eq!(requests[0].headers["idempotency-key"], "key-1");
    assert_eq!(requests[1].method, Method::POST);
    assert_eq!(requests[1].path, "xrpc/org.example.client.record");
    assert_eq!(requests[1].body, br#"{"value":"x"}"#);
    assert_eq!(requests[1].headers["content-type"], "application/json");
}

#[tokio::test]
async fn xrpc_transport_decodes_domain_errors_and_unparsable_bodies() {
    for (status, body, expected) in [
        (
            StatusCode::BAD_REQUEST,
            r#"{"error":"x","message":"y","operationError":"Cancelled"}"#,
            true,
        ),
        (StatusCode::OK, "not json", false),
    ] {
        let http = Recorded {
            requests: Mutex::new(Vec::new()),
            status,
            body,
        };
        let result = Client::new(Xrpc(&http))
            .call(RequestContext::root(), Record { value: "x".into() })
            .await;
        match (result, expected) {
            (Err(ClientError::Operation(_)), true) => {}
            (
                Err(ClientError::Transport {
                    message,
                    outcome_unknown,
                }),
                false,
            ) => {
                assert!(message.contains("invalid JSON XRPC response"), "{message}");
                assert!(
                    outcome_unknown,
                    "a procedure's outcome is unknown after a bad reply"
                );
            }
            (other, _) => panic!("unexpected result {other:?}"),
        }
    }
}

#[tokio::test]
async fn end_marker_distinguishes_completion_from_truncation() {
    for ended in [true, false] {
        let mut frames = vec![open(), event("one")];
        if ended {
            frames.push(Ok(EventFrame::End));
        }
        let mut stream = ClientStream::open(subscribed(frames), RequestContext::root())
            .await
            .unwrap()
            .output;
        assert_eq!(
            stream.recv().await.unwrap(),
            Some(SampleEvent {
                value: "one".into()
            })
        );
        if !ended {
            assert_eq!(
                stream.recv().await,
                Err(ClientError::transport(
                    "subscription closed without an end marker",
                    false
                ))
            );
        }
        assert_eq!(stream.recv().await.unwrap(), None);
    }
}

#[tokio::test]
async fn malformed_establishment_and_duplicate_metadata_are_terminal() {
    let opened = ClientStream::open(
        subscribed(vec![Ok(EventFrame::End), open()]),
        RequestContext::root(),
    )
    .await;
    assert_eq!(
        opened.unwrap_err(),
        ClientError::transport("subscription event preceded its response metadata", false)
    );
    let mut stream = ClientStream::open(subscribed(vec![open(), open()]), RequestContext::root())
        .await
        .unwrap()
        .output;
    assert_eq!(
        stream.recv().await,
        Err(ClientError::transport(
            "subscription sent response metadata twice",
            false
        ))
    );
    assert_eq!(stream.recv().await.unwrap(), None);
}

#[tokio::test]
async fn cancellation_preempts_buffered_events_and_is_terminal() {
    let context = RequestContext::root();
    let mut stream =
        ClientStream::open(subscribed(vec![open(), event("buffered")]), context.clone())
            .await
            .unwrap()
            .output;
    context.cancel();
    assert_eq!(
        stream.recv().await,
        Err(ClientError::Operation(ContextError::Cancelled))
    );
    assert_eq!(stream.recv().await.unwrap(), None);
}

/// The wasm32-wasip3 build links getrandom's `custom` backend, which a guest binary normally
/// provides from its host. Request ids need entropy but not secrecy, so a counter serves the
/// unit-test binary.
#[cfg(target_os = "wasi")]
#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> Result<(), getrandom03::Error> {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let destination = unsafe { std::slice::from_raw_parts_mut(dest, len) };
    for byte in destination {
        *byte = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u8;
    }
    Ok(())
}
