//! Transport tests for the HTTP router, client, and TCP server. Shared
//! fixtures live at the top; the Iroh wire reuses them from `iroh_wire`.

use super::{
    serve::{AcceptFuture, ConnectionAcceptor, serve_with_acceptor},
    *,
};
use crate::{
    context::{HttpMetadata, RequestContext},
    operation::{HttpInput, HttpMethod, XrpcKind, XrpcOperation},
};
use axum::http::{HeaderMap, HeaderValue, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use url::Url;

#[cfg(feature = "iroh-http")]
mod iroh_wire;

const QUERY_OPERATION: XrpcOperation = XrpcOperation {
    nsid: "org.example.test.echo",
    kind: XrpcKind::Query,
    method: HttpMethod::Get,
    input: HttpInput::Query,
    output_schema: "EchoOutput",
    error_schema: "TestError",
};
const PROCEDURE_OPERATION: XrpcOperation = XrpcOperation {
    nsid: "org.example.test.write",
    kind: XrpcKind::Procedure,
    method: HttpMethod::Post,
    input: HttpInput::JsonBody,
    output_schema: "WriteOutput",
    error_schema: "TestError",
};
const NUMERIC_QUERY_OPERATION: XrpcOperation = XrpcOperation {
    nsid: "org.example.test.numeric",
    kind: XrpcKind::Query,
    method: HttpMethod::Get,
    input: HttpInput::Query,
    output_schema: "NumericOutput",
    error_schema: "TestError",
};

#[derive(Debug, Deserialize, Serialize)]
struct EchoParams {
    value: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct NumericParams {
    count: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct NumericOutput {
    doubled: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Output {
    value: String,
    request_id: uuid::Uuid,
    authorization: Option<String>,
    idempotency_key: Option<String>,
    timeout_millis: Option<u64>,
    traceparent: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct TestError {
    error: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct OptionalTestError {
    error: String,
    message: Option<String>,
    field: String,
}

struct ScriptedAcceptor {
    listener: TcpListener,
    calls: Arc<AtomicUsize>,
    first_accepted: Option<oneshot::Sender<()>>,
    failure: Option<oneshot::Receiver<()>>,
}

impl ConnectionAcceptor for ScriptedAcceptor {
    fn accept(&mut self) -> AcceptFuture<'_> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        if call == 0 {
            let listener = &mut self.listener;
            let first_accepted = self.first_accepted.take();
            Box::pin(async move {
                let accepted = listener.accept().await?;
                if let Some(sender) = first_accepted {
                    sender.send(()).expect("first accept receiver");
                }
                Ok(accepted)
            })
        } else if let Some(failure) = self.failure.take() {
            Box::pin(async move {
                failure.await.map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Interrupted, "accept script cancelled")
                })?;
                Err(std::io::Error::other("scripted listener failure"))
            })
        } else {
            Box::pin(async { Err(std::io::Error::other("unexpected scripted accept")) })
        }
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct StreamDrop(Option<oneshot::Sender<()>>);

impl Drop for StreamDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            sender.send(()).expect("stream drop receiver");
        }
    }
}

async fn test_server() -> (
    XrpcClient,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
    oneshot::Sender<()>,
) {
    let mut router = XrpcRouter::new(1024);
    router
        .register(XrpcOperationHandler::typed(
            QUERY_OPERATION,
            |context, metadata, params: EchoParams| async move {
                Ok(Output {
                    value: params.value,
                    request_id: context.request_id,
                    authorization: metadata.authorization,
                    idempotency_key: metadata.idempotency_key,
                    timeout_millis: metadata.timeout.map(|timeout| timeout.as_millis() as u64),
                    traceparent: metadata.traceparent,
                })
            },
        ))
        .expect("query registration");
    router
        .register(XrpcOperationHandler::typed(
            PROCEDURE_OPERATION,
            |context, metadata, params: EchoParams| async move {
                if params.value == "pending" {
                    return std::future::pending::<Result<Output, XrpcHandlerError>>().await;
                } else if params.value == "bad" {
                    return Err(XrpcHandlerError::domain(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "InvalidValue",
                        "bad value",
                    ));
                }
                Ok(Output {
                    value: params.value,
                    request_id: context.request_id,
                    authorization: metadata.authorization,
                    idempotency_key: metadata.idempotency_key,
                    timeout_millis: metadata.timeout.map(|timeout| timeout.as_millis() as u64),
                    traceparent: metadata.traceparent,
                })
            },
        ))
        .expect("procedure registration");
    router
        .register(XrpcOperationHandler::typed(
            NUMERIC_QUERY_OPERATION,
            |_context, _metadata, params: NumericParams| async move {
                Ok(NumericOutput {
                    doubled: params.count * 2,
                })
            },
        ))
        .expect("numeric query registration");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = serve(listener, router.into_router())
        .await
        .expect("start HTTP server");
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    (client, task, shutdown)
}

#[tokio::test]
async fn loopback_query_and_procedure_map_http_and_context() {
    let (client, task, shutdown) = test_server().await;
    assert_eq!(QUERY_OPERATION.kind, XrpcKind::Query);
    assert_eq!(QUERY_OPERATION.input, HttpInput::Query);
    assert_eq!(PROCEDURE_OPERATION.method, HttpMethod::Post);
    let context = RequestContext::root();
    let metadata = HttpMetadata {
        authorization: Some("Capability test".to_owned()),
        idempotency_key: Some("command-1".to_owned()),
        timeout: Some(Duration::from_millis(25)),
        traceparent: Some("00-trace-span-01".to_owned()),
        ..HttpMetadata::default()
    };
    let query: Output = client
        .execute::<EchoParams, Output, TestError>(
            QUERY_OPERATION,
            context.clone(),
            &metadata,
            &EchoParams {
                value: "hello world".to_owned(),
            },
        )
        .await
        .expect("query response");
    assert_eq!(query.value, "hello world");
    assert_eq!(query.request_id, context.request_id);
    assert_eq!(query.authorization, metadata.authorization);
    assert_eq!(query.idempotency_key, metadata.idempotency_key);
    assert_eq!(query.timeout_millis, Some(25));
    assert_eq!(query.traceparent, metadata.traceparent);
    let numeric: NumericOutput = client
        .execute::<NumericParams, NumericOutput, TestError>(
            NUMERIC_QUERY_OPERATION,
            context.clone(),
            &HttpMetadata::default(),
            &NumericParams { count: 17 },
        )
        .await
        .expect("numeric query response");
    assert_eq!(numeric, NumericOutput { doubled: 34 });
    let procedure: Output = client
        .execute::<EchoParams, Output, TestError>(
            PROCEDURE_OPERATION,
            context.clone(),
            &metadata,
            &EchoParams {
                value: "written".to_owned(),
            },
        )
        .await
        .expect("procedure response");
    assert_eq!(procedure.value, "written");
    assert_eq!(procedure.request_id, context.request_id);
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn optional_error_message_decodes_on_public_client_path() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (body, expected_message, malformed) in [
        (r#"{"error":"NotReady","field":"queued"}"#, None, false),
        (
            r#"{"error":"NotReady","message":"try later","field":"queued"}"#,
            Some("try later"),
            false,
        ),
        (
            r#"{"error":"NotReady","message":7,"field":"queued"}"#,
            None,
            true,
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let body = body.to_owned();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            let response = format!(
                "HTTP/1.1 409 Conflict\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client");
        let result: Result<Value, XrpcClientError<OptionalTestError>> = client
            .execute(
                QUERY_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "query".to_owned(),
                },
            )
            .await;
        if malformed {
            assert!(matches!(
                result,
                Err(XrpcClientError::Protocol {
                    outcome_unknown: false,
                    ..
                })
            ));
        } else {
            match result {
                Err(XrpcClientError::Domain {
                    error,
                    outcome_unknown: false,
                    ..
                }) => {
                    assert_eq!(error.message.as_deref(), expected_message);
                    assert_eq!(error.error, "NotReady");
                    assert_eq!(error.field, "queued");
                }
                other => panic!("unexpected optional-message result: {other:?}"),
            }
        }
        task.await
            .expect("connection task join")
            .expect("connection task");
    }
}

#[tokio::test]
async fn domain_errors_are_typed_and_malformed_query_is_rejected() {
    let (client, task, shutdown) = test_server().await;
    let error: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "bad".to_owned(),
            },
        )
        .await;
    assert!(matches!(
        error,
        Err(XrpcClientError::Domain {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            error: TestError { message, .. },
            ..
        }) if message == "bad value"
    ));
    let malformed = client
        .client
        .get(
            client
                .base_url
                .join("xrpc/org.example.test.echo")
                .expect("URL"),
        )
        .send()
        .await
        .expect("malformed query response");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn response_body_limit_is_enforced() {
    let (client, task, shutdown) = test_server().await;
    let limited = XrpcClient::with_client(client.client.clone(), client.base_url.clone(), 2)
        .expect("limited client");
    let result: Result<Output, XrpcClientError<TestError>> = limited
        .execute(
            QUERY_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "large".to_owned(),
            },
        )
        .await;
    assert!(matches!(result, Err(XrpcClientError::Protocol { .. })));
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn loopback_connection_loss_marks_only_procedures_ambiguous() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        stream.shutdown().await.expect("close response connection");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let query: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            QUERY_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "query".to_owned(),
            },
        )
        .await;
    let procedure: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "procedure".to_owned(),
            },
        )
        .await;
    assert!(matches!(
        query,
        Err(XrpcClientError::Transport {
            outcome_unknown: false,
            ..
        })
    ));
    assert!(matches!(
        procedure,
        Err(XrpcClientError::Transport {
            outcome_unknown: true,
            ..
        })
    ));
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn malformed_procedure_error_body_is_ambiguous_protocol_failure() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        stream
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 9\r\nContent-Type: application/json\r\n\r\nnot-json",
                )
                .await
                .expect("write malformed response");
        stream.shutdown().await.expect("close response connection");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let result: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "procedure".to_owned(),
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(XrpcClientError::Protocol {
            outcome_unknown: true,
            ..
        })
    ));
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn method_body_and_request_body_limits_are_explicit() {
    let (client, task, shutdown) = test_server().await;
    let wrong_method = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.echo")
                .expect("URL"),
        )
        .body("{}")
        .send()
        .await
        .expect("method response");
    assert_eq!(wrong_method.status(), StatusCode::METHOD_NOT_ALLOWED);
    let missing_media_type = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .body("{}")
        .send()
        .await
        .expect("media type response");
    assert_eq!(
        missing_media_type.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let parameterized_body = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(header::CONTENT_TYPE, "Application/JSON; charset=utf-8")
        .body(r#"{"value":"parameterized"}"#)
        .send()
        .await
        .expect("parameterized media type response");
    assert_eq!(parameterized_body.status(), StatusCode::OK);
    let suffix_body = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(header::CONTENT_TYPE, "application/jsonp")
        .body("{}")
        .send()
        .await
        .expect("suffix media type response");
    assert_eq!(suffix_body.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let mut duplicate_headers = HeaderMap::new();
    duplicate_headers.append(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    duplicate_headers.append(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let duplicate_parameter = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8; charset=utf-8",
        )
        .body("{}")
        .send()
        .await
        .expect("duplicate parameter response");
    assert_eq!(
        duplicate_parameter.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let duplicate_body = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .headers(duplicate_headers)
        .body("{}")
        .send()
        .await
        .expect("duplicate media type response");
    assert_eq!(duplicate_body.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let malformed_media_type = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(header::CONTENT_TYPE, "application/json; charset")
        .body("{}")
        .send()
        .await
        .expect("malformed media type response");
    assert_eq!(
        malformed_media_type.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let malformed_body = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body("not json")
        .send()
        .await
        .expect("body response");
    assert_eq!(malformed_body.status(), StatusCode::BAD_REQUEST);
    let oversized_body = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body("x".repeat(2048))
        .send()
        .await
        .expect("oversized body response");
    assert_eq!(oversized_body.status(), StatusCode::PAYLOAD_TOO_LARGE);
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn server_timeout_stops_handler_before_it_can_complete() {
    let (client, task, shutdown) = test_server().await;
    let response = client
        .client
        .post(
            client
                .base_url
                .join("xrpc/org.example.test.write")
                .expect("URL"),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .header(TIMEOUT_HEADER, "0")
        .json(&EchoParams {
            value: "pending".to_owned(),
        })
        .send()
        .await
        .expect("deadline response");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let envelope: XrpcErrorEnvelope = response.json().await.expect("error envelope");
    assert_eq!(
        envelope,
        XrpcErrorEnvelope {
            error: "DeadlineExceeded".to_owned(),
            message: Some("XRPC operation exceeded its time budget".to_owned()),
            details: BTreeMap::new(),
        }
    );
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn chunked_request_and_response_limits_are_exact() {
    use futures::stream;
    let (client, task, shutdown) = test_server().await;
    let endpoint = client
        .base_url
        .join("xrpc/org.example.test.write")
        .expect("URL");
    let exact_body = "x".repeat(1024);
    let exact_response = client
        .client
        .post(endpoint.clone())
        .header(header::CONTENT_TYPE, "application/json")
        .body(reqwest::Body::wrap_stream(stream::iter(vec![Ok::<
            _,
            std::io::Error,
        >(
            exact_body
        )])))
        .send()
        .await
        .expect("exact chunked request response");
    assert_ne!(exact_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let oversized_body = "x".repeat(1025);
    let oversized_response = client
        .client
        .post(endpoint)
        .header(header::CONTENT_TYPE, "application/json")
        .body(reqwest::Body::wrap_stream(stream::iter(vec![Ok::<
            _,
            std::io::Error,
        >(
            oversized_body,
        )])))
        .send()
        .await
        .expect("oversized chunked request response");
    assert_eq!(oversized_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");

    for (chunks, expected) in [
        (vec!["{".to_owned(), "}".to_owned()], Ok(())),
        (vec!["{".to_owned(), "} ".to_owned()], Err(())),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let context = RequestContext::root();
        let request_id = context.request_id;
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept client");
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut request = [0_u8; 4096];
            let _bytes_read = stream.read(&mut request).await.expect("read request");
            let headers = format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nx-request-id: {request_id}\r\n\r\n"
            );
            stream
                .write_all(headers.as_bytes())
                .await
                .expect("write headers");
            for chunk in chunks {
                let frame = format!("{:x}\r\n{chunk}\r\n", chunk.len());
                stream
                    .write_all(frame.as_bytes())
                    .await
                    .expect("write chunk");
            }
            stream.write_all(b"0\r\n\r\n").await.expect("write end");
            Ok::<(), std::io::Error>(())
        });
        let client = XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            2,
        )
        .expect("client");
        let result: Result<Value, XrpcClientError<TestError>> = client
            .execute(
                QUERY_OPERATION,
                context,
                &HttpMetadata::default(),
                &EchoParams {
                    value: "chunked".to_owned(),
                },
            )
            .await;
        match expected {
            Ok(()) => assert_eq!(result.expect("exact response"), json!({})),
            Err(()) => assert!(matches!(result, Err(XrpcClientError::Protocol { .. }))),
        }
        task.await
            .expect("connection task join")
            .expect("connection task");
    }
}

#[tokio::test]
async fn fragmented_request_exact_boundary_and_overflow_are_bounded() {
    use futures::stream;
    use tokio::sync::mpsc;
    let (client, task, shutdown) = test_server().await;
    let endpoint = client
        .base_url
        .join("xrpc/org.example.test.write")
        .expect("URL");
    for (chunks, oversized) in [
        (
            vec!["x".repeat(300), "x".repeat(300), "x".repeat(424)],
            false,
        ),
        (
            vec!["x".repeat(300), "x".repeat(300), "x".repeat(425)],
            true,
        ),
    ] {
        let (chunk_sender, chunk_receiver) = mpsc::channel::<Result<String, std::io::Error>>(1);
        let (first_polled_sender, first_polled_receiver) = oneshot::channel();
        let (resume_sender, resume_receiver) = oneshot::channel();
        let body_stream = stream::unfold(
            (
                chunk_receiver,
                Some(first_polled_sender),
                Some(resume_receiver),
            ),
            |(mut chunk_receiver, first_polled, mut resume)| async move {
                if first_polled.is_none()
                    && let Some(resume_receiver) = resume.take()
                {
                    resume_receiver.await.expect("body resume sender");
                }
                chunk_receiver.recv().await.map(|chunk| {
                    let next_resume = if first_polled.is_some() { resume } else { None };
                    if let Some(sender) = first_polled {
                        sender.send(()).expect("first body chunk receiver");
                    }
                    (chunk, (chunk_receiver, None, next_resume))
                })
            },
        );
        let request = client
            .client
            .post(endpoint.clone())
            .header(header::CONTENT_TYPE, "application/json")
            .body(reqwest::Body::wrap_stream(body_stream));
        let request_task = tokio::spawn(request.send());
        let mut chunks = chunks.into_iter();
        chunk_sender
            .send(Ok(chunks.next().expect("first body chunk")))
            .await
            .expect("body consumer");
        first_polled_receiver
            .await
            .expect("first body chunk polled");
        resume_sender.send(()).expect("body resume receiver");
        for chunk in chunks {
            chunk_sender.send(Ok(chunk)).await.expect("body consumer");
        }
        drop(chunk_sender);
        let response = request_task
            .await
            .expect("request join")
            .expect("request response");
        if oversized {
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        } else {
            assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        }
    }
    shutdown.send(()).expect("shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn fragmented_response_pauses_between_wire_chunks() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let context = RequestContext::root();
    let request_id = context.request_id;
    let (first_sender, first_receiver) = oneshot::channel();
    let (continue_sender, continue_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        let headers = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nx-request-id: {request_id}\r\n\r\n"
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .expect("write headers");
        stream
            .write_all(b"1\r\n{\r\n")
            .await
            .expect("write first chunk");
        first_sender.send(()).expect("first chunk receiver");
        continue_receiver.await.expect("continue receiver");
        stream
            .write_all(b"1\r\n}\r\n0\r\n\r\n")
            .await
            .expect("write final chunk");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        2,
    )
    .expect("client");
    let request = tokio::spawn(async move {
        client
            .execute::<EchoParams, Value, TestError>(
                QUERY_OPERATION,
                context,
                &HttpMetadata::default(),
                &EchoParams {
                    value: "fragmented".to_owned(),
                },
            )
            .await
    });
    first_receiver.await.expect("first chunk sent");
    continue_sender.send(()).expect("continue sender");
    assert_eq!(
        request.await.expect("request join").expect("response"),
        json!({})
    );
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn blocked_fragmented_request_drops_on_shutdown_before_handler() {
    use futures::stream;
    use tokio::sync::mpsc;
    let invoked = Arc::new(AtomicBool::new(false));
    let body_dropped = Arc::new(AtomicBool::new(false));
    let mut router = XrpcRouter::new(1024);
    router.observe_body_drop(Arc::clone(&body_dropped));
    let handler_invoked = Arc::clone(&invoked);
    router
        .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
            handler_invoked.store(true, Ordering::Release);
            async { Ok(json!({"value":"late"})) }
        }))
        .expect("procedure registration");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = serve(listener, router.into_router())
        .await
        .expect("start server");
    let (chunk_sender, chunk_receiver) = mpsc::channel::<Result<String, std::io::Error>>(1);
    let (polled_sender, polled_receiver) = oneshot::channel();
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let body_stream = stream::unfold(
        (
            chunk_receiver,
            Some(polled_sender),
            StreamDrop(Some(dropped_sender)),
        ),
        move |(mut chunk_receiver, polled, drop_guard)| async move {
            chunk_receiver.recv().await.map(|chunk| {
                if let Some(sender) = polled {
                    sender.send(()).expect("body polled receiver");
                }
                (chunk, (chunk_receiver, None, drop_guard))
            })
        },
    );
    let request = tokio::spawn(
        reqwest::Client::new()
            .post(format!(
                "http://{address}/xrpc/{}",
                PROCEDURE_OPERATION.nsid
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .body(reqwest::Body::wrap_stream(body_stream))
            .send(),
    );
    chunk_sender
        .send(Ok(r#"{"value":"#.to_owned()))
        .await
        .expect("body consumer");
    polled_receiver.await.expect("body was polled");
    shutdown.send(()).expect("shutdown receiver");
    task.await.expect("server task join").expect("server task");
    assert!(body_dropped.load(Ordering::Acquire));
    drop(chunk_sender);
    dropped_receiver.await.expect("body stream dropped");
    assert!(!invoked.load(Ordering::Acquire));
    assert!(request.await.expect("request join").is_err());
}

#[tokio::test]
async fn peer_disconnect_drops_blocked_body_and_recovers_capacity() {
    use tokio::io::AsyncWriteExt;
    let invoked = Arc::new(AtomicBool::new(false));
    let body_dropped = Arc::new(tokio::sync::Notify::new());
    let mut router = XrpcRouter::with_limits(1024, 1);
    router.observe_body_drop(Arc::new(AtomicBool::new(false)));
    router.observe_body_drop_notify(Arc::clone(&body_dropped));
    let handler_invoked = Arc::clone(&invoked);
    router
        .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
            handler_invoked.store(true, Ordering::Release);
            async { Ok(json!({"value":"complete"})) }
        }))
        .expect("procedure registration");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = serve(listener, router.into_router())
        .await
        .expect("start server");

    let mut peer = TcpStream::connect(address).await.expect("connect peer");
    let server_drop = body_dropped.notified();
    peer.write_all(
            format!(
                "POST /xrpc/{} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nA\r\n{{\"value\":\"\r\n",
                PROCEDURE_OPERATION.nsid
            )
            .as_bytes(),
        )
        .await
        .expect("write incomplete chunk");
    peer.shutdown().await.expect("disconnect peer");
    server_drop.await;
    assert!(!invoked.load(Ordering::Acquire));
    let response = reqwest::Client::new()
        .post(format!(
            "http://{address}/xrpc/{}",
            PROCEDURE_OPERATION.nsid
        ))
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"value":"recovery"}"#)
        .send()
        .await
        .expect("recovery response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(invoked.load(Ordering::Acquire));
    shutdown.send(()).expect("shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn handler_timeout_drops_guard_before_receipt() {
    let completed = Arc::new(AtomicBool::new(false));
    let started = Arc::new(Mutex::new(None));
    let (started_sender, started_receiver) = oneshot::channel();
    *started.lock().expect("started lock") = Some(started_sender);
    let handler_completed = Arc::clone(&completed);
    let handler_started = Arc::clone(&started);
    let mut router = XrpcRouter::new(1024);
    router
        .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
            let completed = Arc::clone(&handler_completed);
            let started = Arc::clone(&handler_started);
            async move {
                if let Some(sender) = started.lock().expect("started lock").take() {
                    sender.send(()).expect("started receiver");
                }
                let _drop_flag = DropFlag(completed);
                std::future::pending::<Result<Value, XrpcHandlerError>>().await
            }
        }))
        .expect("procedure registration");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = serve(listener, router.into_router())
        .await
        .expect("start server");
    let response = reqwest::Client::new()
        .post(format!(
            "http://{address}/xrpc/{}",
            PROCEDURE_OPERATION.nsid
        ))
        .header(header::CONTENT_TYPE, "application/json")
        .header(TIMEOUT_HEADER, "0")
        .body(r#"{"value":"pending"}"#)
        .send()
        .await
        .expect("timeout response");
    started_receiver.await.expect("handler started");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(!completed.load(Ordering::Acquire));
    shutdown.send(()).expect("shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn procedure_timeout_domain_error_remains_ambiguous() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let context = RequestContext::root();
    let response_request_id = context.request_id;
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        let response = format!(
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 50\r\nContent-Type: application/json\r\nx-request-id: {response_request_id}\r\n\r\n{{\"error\":\"DeadlineExceeded\",\"message\":\"timed out\"}}"
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let result: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            context,
            &HttpMetadata::default(),
            &EchoParams {
                value: "procedure".to_owned(),
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(XrpcClientError::Domain {
            status: StatusCode::GATEWAY_TIMEOUT,
            outcome_unknown: true,
            error: TestError { error, .. },
        }) if error == "DeadlineExceeded"
    ));
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn procedure_server_error_after_effect_remains_ambiguous() {
    use std::sync::atomic::AtomicBool;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let effect = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let context = RequestContext::root();
    let request_id = context.request_id;
    let handler_effect = Arc::clone(&effect);
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        handler_effect.store(true, Ordering::Release);
        let response = format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 52\r\nContent-Type: application/json\r\nx-request-id: {request_id}\r\n\r\n{{\"error\":\"InternalError\",\"message\":\"effect applied\"}}"
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let result: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            context,
            &HttpMetadata::default(),
            &EchoParams {
                value: "effect".to_owned(),
            },
        )
        .await;
    assert!(effect.load(Ordering::Acquire));
    assert!(matches!(
        result,
        Err(XrpcClientError::Domain {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            outcome_unknown: true,
            error: TestError { message, .. },
        }) if message == "effect applied"
    ));
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn shutdown_cancels_and_joins_pending_connection() {
    let started = Arc::new(Mutex::new(None));
    let (started_sender, started_receiver) = oneshot::channel();
    *started.lock().expect("started lock") = Some(started_sender);
    let completed = Arc::new(AtomicBool::new(false));
    let mut router = XrpcRouter::with_limits(1024, 1);
    let handler_completed = Arc::clone(&completed);
    let handler_started = Arc::clone(&started);
    router
        .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
            if let Some(sender) = handler_started.lock().expect("started lock").take() {
                sender.send(()).expect("started receiver");
            }
            let completed = Arc::clone(&handler_completed);
            async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                completed.store(true, Ordering::Release);
                Ok(json!({"value":"completed"}))
            }
        }))
        .expect("procedure registration");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = serve(listener, router.into_router())
        .await
        .expect("start server");
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let request = tokio::spawn(async move {
        client
            .execute::<EchoParams, Value, TestError>(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "pending".to_owned(),
                },
            )
            .await
    });
    started_receiver.await.expect("handler started");
    let rejected = reqwest::Client::new()
        .post(format!(
            "http://{address}/xrpc/{}",
            PROCEDURE_OPERATION.nsid
        ))
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            REQUEST_ID_HEADER,
            RequestContext::root().request_id.to_string(),
        )
        .body(r#"{"value":"rejected"}"#)
        .send()
        .await
        .expect("admission response");
    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
    shutdown.send(()).expect("server shutdown receiver");
    task.await.expect("server task join").expect("server task");
    assert!(!completed.load(Ordering::Acquire));
    assert!(request.await.expect("request join").is_err());
}

#[tokio::test]
async fn response_request_id_must_correlate() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: application/json\r\nx-request-id: 00000000-0000-0000-0000-000000000000\r\n\r\n{}")
                .await
                .expect("write response");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let result: Result<Value, XrpcClientError<TestError>> = client
        .execute(
            QUERY_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "query".to_owned(),
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(XrpcClientError::Protocol {
            outcome_unknown: false,
            ..
        })
    ));
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn response_without_request_id_remains_compatible() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream.read(&mut request).await.expect("read request");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: application/json\r\n\r\n{}",
            )
            .await
            .expect("write response");
        Ok::<(), std::io::Error>(())
    });
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let result: Result<Value, XrpcClientError<TestError>> = client
        .execute(
            QUERY_OPERATION,
            RequestContext::root(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "query".to_owned(),
            },
        )
        .await;
    assert_eq!(
        result.expect("response without optional request ID"),
        json!({})
    );
    task.await
        .expect("connection task join")
        .expect("connection task");
}

#[tokio::test]
async fn domain_error_details_remain_in_standard_envelope() {
    let mut details = BTreeMap::new();
    details.insert("retry_after_ms".to_owned(), json!(250));
    let error = XrpcHandlerError::domain_with_details(
        StatusCode::TOO_MANY_REQUESTS,
        "RateLimited",
        "try again later",
        details.clone(),
    )
    .expect("non-reserved domain details");
    let XrpcHandlerError::Domain { body, .. } = error else {
        panic!("expected domain error")
    };
    assert_eq!(body.error, "RateLimited");
    assert_eq!(body.details, details);
    assert_eq!(
        serde_json::to_value(body).expect("serialize envelope")["message"],
        "try again later"
    );
    let mut reserved = BTreeMap::new();
    reserved.insert("message".to_owned(), json!("collision"));
    assert!(matches!(
        XrpcHandlerError::domain_with_details(
            StatusCode::BAD_REQUEST,
            "Invalid",
            "message",
            reserved,
        ),
        Err(XrpcDomainDetailsError::ReservedField("message")),
    ));
    assert!(serde_json::from_str::<XrpcErrorEnvelope>("null").is_err());
    assert!(
        serde_json::from_str::<XrpcErrorEnvelope>(r#"{"error":12,"message":"invalid"}"#).is_err()
    );
}

#[tokio::test]
async fn listener_failure_aborts_and_joins_active_connection() {
    let accepted_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = accepted_listener.local_addr().expect("listener address");
    let calls = Arc::new(AtomicUsize::new(0));
    let (accepted_sender, accepted_receiver) = oneshot::channel();
    let (failure_sender, failure_receiver) = oneshot::channel();
    let acceptor = ScriptedAcceptor {
        listener: accepted_listener,
        calls: Arc::clone(&calls),
        first_accepted: Some(accepted_sender),
        failure: Some(failure_receiver),
    };
    let started = Arc::new(Mutex::new(None));
    let (started_sender, started_receiver) = oneshot::channel();
    *started.lock().expect("started lock") = Some(started_sender);
    let active = Arc::new(AtomicBool::new(false));
    let handler_active = Arc::clone(&active);
    let handler_started = Arc::clone(&started);
    let mut router = XrpcRouter::new(1024);
    router
        .register(XrpcOperationHandler::new(PROCEDURE_OPERATION, move |_| {
            if let Some(sender) = handler_started.lock().expect("started lock").take() {
                sender.send(()).expect("started receiver");
            }
            handler_active.store(true, Ordering::Release);
            let active = Arc::clone(&handler_active);
            async move {
                let _drop_flag = DropFlag(active);
                std::future::pending::<Result<Value, XrpcHandlerError>>().await
            }
        }))
        .expect("procedure registration");
    let router = router.into_router();
    let (task, _shutdown) = serve_with_acceptor(acceptor, router, 2)
        .await
        .expect("start server");
    let client = XrpcClient::new(
        Url::parse(&format!("http://{address}/")).expect("base URL"),
        1024,
    )
    .expect("client");
    let request = tokio::spawn(async move {
        client
            .execute::<EchoParams, Value, TestError>(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "pending".to_owned(),
                },
            )
            .await
    });
    accepted_receiver.await.expect("first connection accepted");
    started_receiver.await.expect("handler started");
    failure_sender.send(()).expect("failure receiver");
    let result = task.await.expect("server task join");
    assert!(result.is_err());
    assert!(!active.load(Ordering::Acquire));
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert!(request.await.expect("request join").is_err());
}

#[tokio::test]
async fn connection_capacity_waits_before_accepting_more_peers() {
    let accepted_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = accepted_listener.local_addr().expect("listener address");
    let calls = Arc::new(AtomicUsize::new(0));
    let (accepted_sender, accepted_receiver) = oneshot::channel();
    let acceptor = ScriptedAcceptor {
        listener: accepted_listener,
        calls: Arc::clone(&calls),
        first_accepted: Some(accepted_sender),
        failure: None,
    };
    let router = XrpcRouter::new(1024).into_router();
    let (task, shutdown) = serve_with_acceptor(acceptor, router, 1)
        .await
        .expect("start server");
    let first = tokio::net::TcpStream::connect(address)
        .await
        .expect("first peer");
    accepted_receiver.await.expect("first connection accepted");
    let second = tokio::net::TcpStream::connect(address)
        .await
        .expect("second peer queued by listener");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    shutdown.send(()).expect("shutdown receiver");
    task.await.expect("server task join").expect("server task");
    drop(second);
    drop(first);
}

#[tokio::test]
async fn repeated_normal_connections_complete_under_connection_bound() {
    let mut router = XrpcRouter::new(1024);
    router
            .register(XrpcOperationHandler::typed(
                QUERY_OPERATION,
                |_context, _metadata, params: EchoParams| async move {
                    Ok(json!({"value": params.value}))
                },
            ))
            .expect("query registration");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (task, shutdown) = serve_with_connection_limit(listener, router.into_router(), 2)
        .await
        .expect("start server");
    let client = reqwest::Client::new();
    for value in ["one", "two", "three", "four", "five"] {
        let response = client
            .get(format!(
                "http://{address}/xrpc/{}?value={value}",
                QUERY_OPERATION.nsid
            ))
            .header("connection", "close")
            .send()
            .await
            .expect("normal response");
        assert_eq!(response.status(), StatusCode::OK);
    }
    shutdown.send(()).expect("shutdown receiver");
    task.await.expect("server task join").expect("server task");
}

#[tokio::test]
async fn unsupported_subscription_is_explicit() {
    let subscription = XrpcOperation {
        nsid: "org.example.test.events",
        kind: XrpcKind::Subscription,
        method: HttpMethod::Get,
        input: HttpInput::Query,
        output_schema: "Event",
        error_schema: "TestError",
    };
    let mut router = XrpcRouter::new(1024);
    assert_eq!(
        router
            .register(XrpcOperationHandler::new(subscription, |_| async {
                Ok(json!({}))
            }))
            .expect_err("subscription must be rejected"),
        XrpcRegistrationError::SubscriptionUnsupported
    );
}
