//! HTTP/1 over Iroh: the same wire, driven through Iroh endpoints.

use super::*;

const COEXISTING_ALPN: &[u8] = b"townframe/test-coexisting/1";

#[derive(Debug)]
struct CoexistingProtocolHandler(Arc<AtomicUsize>);

impl ::iroh::protocol::ProtocolHandler for CoexistingProtocolHandler {
    async fn accept(
        &self,
        connection: ::iroh::endpoint::Connection,
    ) -> Result<(), ::iroh::protocol::AcceptError> {
        self.0.fetch_add(1, Ordering::AcqRel);
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

#[tokio::test]
async fn iroh_http1_registration_coexists_on_embedder_router() {
    let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
    let xrpc_router = XrpcRouter::new(1024).into_router();
    let other_accepts = Arc::new(AtomicUsize::new(0));
    let server_router = ::iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(
            COEXISTING_ALPN,
            CoexistingProtocolHandler(Arc::clone(&other_accepts)),
        )
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(xrpc_router, 1, 2),
        )
        .spawn();

    let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
    let connection = client_endpoint
        .connect(server_endpoint.addr(), COEXISTING_ALPN)
        .await
        .expect("coexisting protocol connection");
    let (mut send, mut recv) = connection.open_bi().await.expect("coexisting stream");
    send.write_all(b"coexisting")
        .await
        .expect("coexisting write");
    send.finish().expect("coexisting finish");
    let response = recv.read_to_end(1024).await.expect("coexisting response");
    assert_eq!(response, b"coexisting");
    assert_eq!(other_accepts.load(Ordering::Acquire), 1);

    server_router.shutdown().await.expect("router shutdown");
    client_endpoint.close().await;
    server_endpoint.close().await;
}

async fn iroh_test_endpoint() -> Result<::iroh::Endpoint, Box<dyn std::error::Error + Send + Sync>>
{
    Ok(
        ::iroh::Endpoint::builder(::iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(::iroh::RelayMode::Disabled)
            .bind()
            .await?,
    )
}

#[tokio::test]
async fn iroh_http1_query_procedure_and_domain_error_use_shared_wire() {
    let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
    let mut router = XrpcRouter::new(1024);
    router
        .register(XrpcOperationHandler::typed(
            QUERY_OPERATION,
            |_context, metadata, params: EchoParams| async move {
                Ok(Output {
                    value: params.value,
                    request_id: RequestContext::root().request_id,
                    authorization: metadata.authorization,
                    idempotency_key: metadata.idempotency_key,
                })
            },
        ))
        .expect("query registration");
    let deadline_started = Arc::new(tokio::sync::Notify::new());
    let handler_deadline_started = Arc::clone(&deadline_started);
    router
        .register(XrpcOperationHandler::typed(
            PROCEDURE_OPERATION,
            move |_context, _metadata, params: EchoParams| {
                let handler_deadline_started = Arc::clone(&handler_deadline_started);
                async move {
                    if params.value == "bad" {
                        return Err(XrpcHandlerError::domain(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            "InvalidValue",
                            "bad value",
                        ));
                    }
                    if params.value == "pending" {
                        handler_deadline_started.notify_one();
                        drop(std::future::pending::<Result<Output, XrpcHandlerError>>().await);
                    }
                    Ok(Output {
                        value: if params.value == "large" {
                            "x".repeat(8192)
                        } else {
                            params.value
                        },
                        request_id: RequestContext::root().request_id,
                        authorization: None,
                        idempotency_key: None,
                    })
                }
            },
        ))
        .expect("procedure registration");
    let server_router = ::iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(router.into_router(), 1, 8),
        )
        .spawn();
    let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
    let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
        .expect("Iroh client");
    let context = RequestContext::root();
    // Both exchanges must share the single admitted QUIC connection.  The
    // server allows only one connection, so a per-RPC connect path would
    // reject one of these concurrent streams.
    let concurrent_metadata = HttpMetadata::default();
    let concurrent_query_params = EchoParams {
        value: "same-connection-query".to_owned(),
    };
    let concurrent_procedure_params = EchoParams {
        value: "same-connection-procedure".to_owned(),
    };
    let concurrent_query_client = client.clone();
    let concurrent_procedure_client = client.clone();
    let concurrent_query = concurrent_query_client.execute::<EchoParams, Output, TestError>(
        QUERY_OPERATION,
        context.clone(),
        &concurrent_metadata,
        &concurrent_query_params,
    );
    let concurrent_procedure = concurrent_procedure_client
        .execute::<EchoParams, Output, TestError>(
            PROCEDURE_OPERATION,
            context.clone(),
            &concurrent_metadata,
            &concurrent_procedure_params,
        );
    let (concurrent_query, concurrent_procedure) =
        tokio::join!(concurrent_query, concurrent_procedure);
    assert_eq!(
        concurrent_query.expect("concurrent query").value,
        "same-connection-query"
    );
    assert_eq!(
        concurrent_procedure.expect("concurrent procedure").value,
        "same-connection-procedure"
    );
    let query: Output = client
        .execute::<EchoParams, Output, TestError>(
            QUERY_OPERATION,
            context.clone(),
            &HttpMetadata {
                authorization: Some("Capability iroh".to_owned()),
                ..HttpMetadata::default()
            },
            &EchoParams {
                value: "iroh-query".to_owned(),
            },
        )
        .await
        .expect("Iroh query");
    assert_eq!(query.value, "iroh-query");
    assert_eq!(query.authorization.as_deref(), Some("Capability iroh"));
    let deadline_context = context.clone().with_time_budget(Duration::from_millis(25));
    let deadline_connection = client
        .active_connection()
        .await
        .expect("retained connection");
    let deadline_request = tokio::spawn(async move {
        let (mut send, mut recv) = deadline_connection
            .open_bi()
            .await
            .expect("deadline stream");
        let body = serde_json::to_string(&EchoParams {
            value: "pending".to_owned(),
        })
        .unwrap();
        let request = format!(
            "POST /xrpc/{} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nx-request-id: {}\r\nx-timeout-ms: {}\r\nconnection: close\r\n\r\n{body}",
            PROCEDURE_OPERATION.nsid,
            body.len(),
            deadline_context.request_id,
            deadline_context.remaining_budget().unwrap().as_millis(),
        );
        tokio::io::AsyncWriteExt::write_all(&mut send, request.as_bytes())
            .await
            .expect("deadline request");
        send.finish().expect("deadline request finished");
        recv.read_to_end(4096).await.expect("deadline response")
    });
    deadline_started.notified().await;
    let deadline = deadline_request.await.expect("deadline request join");
    let deadline = String::from_utf8(deadline).expect("HTTP response");
    assert!(deadline.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"));
    let (_, body) = deadline.split_once("\r\n\r\n").expect("HTTP body");
    let error: TestError = serde_json::from_str(body).expect("deadline error");
    assert_eq!(error.error, "DeadlineExceeded");
    let limited: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            context.clone(),
            &HttpMetadata::default(),
            &EchoParams {
                value: "large".to_owned(),
            },
        )
        .await;
    assert!(matches!(
        limited,
        Err(XrpcClientError::Protocol {
            outcome_unknown: true,
            ..
        })
    ));
    let error: Result<Output, XrpcClientError<TestError>> = client
        .execute(
            PROCEDURE_OPERATION,
            context,
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
            outcome_unknown: false,
            error: TestError { message, .. },
        }) if message == "bad value"
    ));
    server_router
        .shutdown()
        .await
        .expect("Iroh router shutdown");
    client_endpoint.close().await;
    server_endpoint.close().await;
}

#[tokio::test]
async fn iroh_http1_chunked_request_limit_is_cumulative_across_wire_frames() {
    let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
    let mut xrpc_router = XrpcRouter::new(8);
    xrpc_router
        .register(XrpcOperationHandler::typed(
            PROCEDURE_OPERATION,
            |_context, _metadata, params: EchoParams| async move {
                Ok(Output {
                    value: params.value,
                    request_id: RequestContext::root().request_id,
                    authorization: None,
                    idempotency_key: None,
                })
            },
        ))
        .expect("procedure registration");
    let server_router = ::iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(xrpc_router.into_router(), 1, 2),
        )
        .spawn();
    let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
    let connection = client_endpoint
        .connect(server_endpoint.addr(), IROH_XRPC_HTTP_ALPN)
        .await
        .expect("Iroh connection");
    let (mut send, mut recv) = connection.open_bi().await.expect("Iroh stream");
    let request = format!(
        "POST /xrpc/{} HTTP/1.1\r\nHost: iroh\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nX-Request-ID: {}\r\n\r\n6\r\n{{\"valu\r\n7\r\ne\":\"x\"}}\r\n0\r\n\r\n",
        PROCEDURE_OPERATION.nsid,
        uuid::Uuid::new_v4(),
    );
    send.write_all(request.as_bytes())
        .await
        .expect("chunked request write");
    send.finish().expect("chunked request finish");
    let response = recv.read_to_end(4096).await.expect("chunked response");
    let response = String::from_utf8(response).expect("HTTP response");
    assert!(response.starts_with("HTTP/1.1 413"), "response: {response}");
    server_router.shutdown().await.expect("router shutdown");
    client_endpoint.close().await;
    server_endpoint.close().await;
}

#[tokio::test]
async fn iroh_http1_client_disconnect_drops_entered_handler() {
    let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
    let started = Arc::new(tokio::sync::Notify::new());
    let dropped_sender = Arc::new(Mutex::new(None));
    let (dropped_tx, dropped_rx) = oneshot::channel();
    *dropped_sender.lock().expect("drop sender lock") = Some(dropped_tx);
    let handler_started = Arc::clone(&started);
    let handler_dropped_sender = Arc::clone(&dropped_sender);
    let mut xrpc_router = XrpcRouter::new(1024);
    xrpc_router
        .register(XrpcOperationHandler::typed(
            PROCEDURE_OPERATION,
            move |_context, _metadata, params: EchoParams| {
                let handler_started = Arc::clone(&handler_started);
                let handler_dropped_sender = Arc::clone(&handler_dropped_sender);
                async move {
                    if params.value == "pending" {
                        handler_started.notify_one();
                        let _drop_guard = StreamDrop(
                            handler_dropped_sender
                                .lock()
                                .expect("drop sender lock")
                                .take(),
                        );
                        std::future::pending::<Result<Output, XrpcHandlerError>>().await
                    } else {
                        Ok(Output {
                            value: params.value,
                            request_id: RequestContext::root().request_id,
                            authorization: None,
                            idempotency_key: None,
                        })
                    }
                }
            },
        ))
        .expect("procedure registration");
    let server_router = ::iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(xrpc_router.into_router(), 1, 2),
        )
        .spawn();
    let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
    let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
        .expect("Iroh client");
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        request_client
            .execute::<EchoParams, Output, TestError>(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "pending".to_owned(),
                },
            )
            .await
    });
    started.notified().await;
    let connection = client
        .active_connection()
        .await
        .expect("retained client connection");
    connection.close(0u32.into(), b"client disconnect");
    let result = request.await.expect("disconnected request join");
    assert!(matches!(
        result,
        Err(XrpcClientError::Transport {
            outcome_unknown: true,
            ..
        }) | Err(XrpcClientError::Protocol {
            outcome_unknown: true,
            ..
        })
    ));
    dropped_rx
        .await
        .expect("handler drop after client disconnect");
    server_router.shutdown().await.expect("router shutdown");
    client_endpoint.close().await;
    server_endpoint.close().await;
}

#[tokio::test]
async fn iroh_http1_client_stream_cancel_drops_only_entered_handler() {
    let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
    let started = Arc::new(tokio::sync::Notify::new());
    let survivor_started = Arc::new(tokio::sync::Notify::new());
    let survivor_release = Arc::new(tokio::sync::Notify::new());
    let dropped_sender = Arc::new(Mutex::new(None));
    let (dropped_tx, dropped_rx) = oneshot::channel();
    *dropped_sender.lock().expect("drop sender lock") = Some(dropped_tx);
    let handler_started = Arc::clone(&started);
    let handler_survivor_started = Arc::clone(&survivor_started);
    let handler_survivor_release = Arc::clone(&survivor_release);
    let handler_dropped_sender = Arc::clone(&dropped_sender);
    let mut router = XrpcRouter::new(1024);
    router
        .register(XrpcOperationHandler::typed(
            PROCEDURE_OPERATION,
            move |_context, _metadata, params: EchoParams| {
                let handler_started = Arc::clone(&handler_started);
                let handler_survivor_started = Arc::clone(&handler_survivor_started);
                let handler_survivor_release = Arc::clone(&handler_survivor_release);
                let handler_dropped_sender = Arc::clone(&handler_dropped_sender);
                async move {
                    if params.value == "pending" {
                        handler_started.notify_one();
                        let _drop_guard = StreamDrop(
                            handler_dropped_sender
                                .lock()
                                .expect("drop sender lock")
                                .take(),
                        );
                        std::future::pending::<Result<Output, XrpcHandlerError>>().await
                    } else if params.value == "survivor" {
                        handler_survivor_started.notify_one();
                        handler_survivor_release.notified().await;
                        Ok(Output {
                            value: params.value,
                            request_id: RequestContext::root().request_id,
                            authorization: None,
                            idempotency_key: None,
                        })
                    } else {
                        Ok(Output {
                            value: params.value,
                            request_id: RequestContext::root().request_id,
                            authorization: None,
                            idempotency_key: None,
                        })
                    }
                }
            },
        ))
        .expect("procedure registration");
    let server_router = ::iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(router.into_router(), 1, 2),
        )
        .spawn();
    let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
    let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
        .expect("Iroh client");

    let request_client = client.clone();
    let request = tokio::spawn(async move {
        request_client
            .execute::<EchoParams, Output, TestError>(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "pending".to_owned(),
                },
            )
            .await
    });
    started.notified().await;
    let retained_connection = client
        .active_connection()
        .await
        .expect("retained client connection");

    let survivor_started_wait = survivor_started.notified();
    let survivor_client = client.clone();
    let survivor_request = tokio::spawn(async move {
        survivor_client
            .execute::<EchoParams, Output, TestError>(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "survivor".to_owned(),
                },
            )
            .await
    });
    survivor_started_wait.await;

    // Cancel only the first HTTP exchange while the unrelated second
    // exchange is already inside its handler on the retained connection.
    request.abort();
    assert!(
        request
            .await
            .expect_err("cancelled request must not complete")
            .is_cancelled()
    );
    dropped_rx
        .await
        .expect("entered handler drop after stream cancellation");
    assert!(retained_connection.close_reason().is_none());

    survivor_release.notify_one();
    let completed: Output = survivor_request
        .await
        .expect("surviving request join")
        .expect("unrelated same-connection request");
    assert_eq!(completed.value, "survivor");

    server_router.shutdown().await.expect("router shutdown");
    client_endpoint.close().await;
    server_endpoint.close().await;
}

#[tokio::test]
async fn iroh_http1_shutdown_reaps_pending_exchange() {
    let server_endpoint = iroh_test_endpoint().await.expect("server endpoint");
    let started = Arc::new(tokio::sync::Notify::new());
    let handler_started = Arc::clone(&started);
    let mut router = XrpcRouter::new(1024);
    router
        .register(XrpcOperationHandler::typed(
            PROCEDURE_OPERATION,
            move |_context, _metadata, params: EchoParams| {
                let handler_started = Arc::clone(&handler_started);
                async move {
                    if params.value == "pending" {
                        handler_started.notify_one();
                        std::future::pending::<Result<Output, XrpcHandlerError>>().await
                    } else {
                        Ok(Output {
                            value: params.value,
                            request_id: RequestContext::root().request_id,
                            authorization: None,
                            idempotency_key: None,
                        })
                    }
                }
            },
        ))
        .expect("procedure registration");
    let server_router = ::iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(router.into_router(), 1, 8),
        )
        .spawn();
    let client_endpoint = iroh_test_endpoint().await.expect("client endpoint");
    let client = IrohXrpcClient::new(client_endpoint.clone(), server_endpoint.addr(), 1024)
        .expect("Iroh client");
    let started_wait = started.notified();
    let request = tokio::spawn(async move {
        client
            .execute::<EchoParams, Output, TestError>(
                PROCEDURE_OPERATION,
                RequestContext::root(),
                &HttpMetadata::default(),
                &EchoParams {
                    value: "pending".to_owned(),
                },
            )
            .await
    });
    started_wait.await;
    server_router
        .shutdown()
        .await
        .expect("Iroh router shutdown");
    let result = request.await.expect("pending request join");
    assert!(matches!(
        result,
        Err(XrpcClientError::Transport {
            outcome_unknown: true,
            ..
        }) | Err(XrpcClientError::Protocol {
            outcome_unknown: true,
            ..
        })
    ));
    client_endpoint.close().await;
    server_endpoint.close().await;
}
