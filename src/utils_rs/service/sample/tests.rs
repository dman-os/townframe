use super::*;
use crate::service::HttpMetadata;

#[tokio::test]
async fn direct_and_irpc_use_the_same_inline_middleware_without_direct_spawn() {
    let service = SampleService::new("initial");
    let direct = SampleClient::direct(Arc::clone(&service));
    let root = RequestContext::root();
    let child = root.child();
    let direct_result = TEST_TASK_MARKER
        .scope(41, direct.get_value(child.clone(), GetValueParams))
        .await
        .expect("direct query");
    assert_eq!(direct_result.observed_context, child);
    assert_eq!(
        *service.last_handler_task.lock().expect("task mutex"),
        Some(41)
    );
    let direct_log = service.middleware_log().await;
    assert_eq!(
        direct_log,
        vec!["getValue:context", "getValue:validate", "getValue:complete"]
    );

    let (irpc, adapter) = spawn_sample_irpc(Arc::clone(&service), 4, 1, 1);
    let irpc_result = irpc
        .get_value(root.child(), GetValueParams)
        .await
        .expect("irpc query");
    assert_eq!(irpc_result.snapshot.source_revision, 0);
    assert_eq!(adapter.peak_handlers(), 1);
    assert_eq!(*service.last_handler_task.lock().expect("task mutex"), None);
    assert_eq!(
        &service.middleware_log().await[3..],
        &["getValue:context", "getValue:validate", "getValue:complete"]
    );
}

#[tokio::test]
async fn irpc_adapter_shutdown_joins_owned_child_tasks() {
    let service = SampleService::new("initial");
    let (client, mut adapter) = spawn_sample_irpc(Arc::clone(&service), 4, 1, 1);
    let result = client
        .get_value(RequestContext::root(), GetValueParams)
        .await
        .expect("IRPC query");
    assert_eq!(result.snapshot.value, "initial");
    adapter.shutdown().await;
    assert_eq!(adapter.active_handlers(), 0);
    assert_eq!(adapter.stream_pumps(), 0);
}

#[tokio::test]
async fn local_and_external_source_changes_share_projector_and_coalesce() {
    let service = SampleService::new("initial");
    let gate = service.block_projector();
    let client = SampleClient::direct(Arc::clone(&service));
    let events = service.subscribe_events();
    let _a = service.inject_source("A");
    let _b = service.inject_source("B");
    let _c = service.inject_source("C");
    gate.add_permits(1);
    tokio::task::yield_now().await;
    let mut events = events;
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
        .await
        .expect("event")
        .expect("broadcast");
    assert_eq!(event.value, "C");
    let state = client
        .read_state(
            RequestContext::root(),
            ReadStateParams {
                after_source_revision: 0,
            },
        )
        .await
        .expect("state");
    assert_eq!(state.snapshot.value, "C");
    assert_eq!(state.snapshot.source_revision, 3);
}

#[tokio::test]
async fn mutation_waits_for_projector_and_external_changes_are_not_mutation_events() {
    let service = SampleService::new("initial");
    let client = SampleClient::direct(Arc::clone(&service));
    let written = client
        .put_value(
            RequestContext::root(),
            PutValueParams {
                value: "local".into(),
            },
        )
        .await
        .expect("write");
    assert_eq!(written.snapshot.value, "local");

    let mut events = service.subscribe_events();
    let external_revision = service.inject_source("remote");
    let external_event = events.recv().await.expect("external projection event");
    assert_eq!(external_event.source_revision, external_revision);
    assert_eq!(external_event.value, "remote");
    let page = client
        .read_state(
            RequestContext::root(),
            ReadStateParams {
                after_source_revision: external_revision,
            },
        )
        .await
        .expect("read");
    assert_eq!(page.snapshot.source_revision, external_revision);
    assert_eq!(page.snapshot.value, "remote");
}

#[tokio::test]
async fn concurrent_source_writers_get_distinct_monotonic_revisions() {
    let service = SampleService::new("initial");
    let client = SampleClient::direct(Arc::clone(&service));
    let (left, right) = tokio::join!(
        client.put_value(
            RequestContext::root(),
            PutValueParams {
                value: "left".into()
            }
        ),
        client.put_value(
            RequestContext::root(),
            PutValueParams {
                value: "right".into()
            }
        ),
    );
    left.expect("left write");
    right.expect("right write");
    let state = client
        .read_state(
            RequestContext::root(),
            ReadStateParams {
                after_source_revision: 0,
            },
        )
        .await
        .expect("final state");
    assert_eq!(state.snapshot.source_revision, 2);
}

#[tokio::test]
async fn xrpc_query_and_procedure_transport_failures_have_different_outcomes() {
    struct FailingExecutor;
    #[async_trait::async_trait]
    impl XrpcExecutor for FailingExecutor {
        async fn execute(&self, _request: HttpRequest) -> Result<HttpResponse, XrpcTransportError> {
            Err(XrpcTransportError {
                message: "connection lost".into(),
            })
        }
    }
    let client = SampleClient::xrpc(Arc::new(FailingExecutor));
    let query = client
        .get_value(RequestContext::root(), GetValueParams)
        .await;
    let procedure = client
        .put_value(RequestContext::root(), PutValueParams { value: "x".into() })
        .await;
    assert!(matches!(
        query,
        Err(ClientError::Transport {
            outcome_unknown: false,
            ..
        })
    ));
    assert!(matches!(
        procedure,
        Err(ClientError::Transport {
            outcome_unknown: true,
            ..
        })
    ));
}

#[tokio::test]
async fn subscription_context_validation_is_shared_across_transports() {
    let service = SampleService::new("initial");
    let mut invalid_context = RequestContext::root();
    invalid_context.request_id = uuid::Uuid::nil();
    invalid_context.trace_id = uuid::Uuid::nil();
    let params = SubscribeEventsParams {
        after_source_revision: 0,
    };

    let direct = SampleClient::direct(Arc::clone(&service));
    assert!(matches!(
        direct
            .subscribe_events(invalid_context.clone(), params.clone())
            .await,
        Err(ClientError::Operation(SampleError::Context(
            ContextError::MissingRequestId
        )))
    ));

    let (irpc, _adapter) = spawn_sample_irpc(Arc::clone(&service), 4, 1, 1);
    let mut irpc_subscription = irpc
        .subscribe_events(invalid_context.clone(), params.clone())
        .await
        .expect("IRPC admission returns a typed stream");
    assert!(matches!(
        irpc_subscription.recv().await,
        Err(ClientError::Operation(SampleError::Context(
            ContextError::MissingRequestId
        )))
    ));

    let xrpc = SampleClient::xrpc(LocalXrpcExecutor::new(service));
    assert!(matches!(
        xrpc.subscribe_events(invalid_context, params).await,
        Err(ClientError::Operation(SampleError::Context(
            ContextError::MissingRequestId
        )))
    ));
}

#[tokio::test]
async fn irpc_procedure_response_drop_is_outcome_unknown() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = SampleClient {
        transport: SampleTransport::Irpc(irpc::Client::<SampleIrpc>::local(sender)),
    };
    let drop_response = tokio::spawn(async move {
        match receiver.recv().await {
            Some(SampleIrpcMessage::PutValue(request)) => drop(request),
            other => panic!("unexpected IRPC request: {other:?}"),
        }
    });

    let result = client
        .put_value(
            RequestContext::root(),
            PutValueParams {
                value: "write".into(),
            },
        )
        .await;
    drop_response.await.expect("response-drop task");
    assert!(matches!(
        result,
        Err(ClientError::Transport {
            outcome_unknown: true,
            ..
        })
    ));
}

#[tokio::test]
async fn canonical_xrpc_query_and_procedure_use_json_http_mapping() {
    let service = SampleService::new("initial");
    let executor = LocalXrpcExecutor::new(Arc::clone(&service));
    let xrpc_executor: Arc<dyn XrpcExecutor> =
        std::sync::Arc::<LocalXrpcExecutor>::clone(&executor);
    let xrpc = SampleClient::xrpc(xrpc_executor);
    assert_eq!(XRPC_OPERATIONS[0].nsid, "org.example.sample.getValue");
    assert_eq!(XRPC_OPERATIONS[1].input, HttpInput::JsonBody);
    let query_context = RequestContext::root();
    let read = xrpc
        .get_value(query_context.clone(), GetValueParams)
        .await
        .expect("xrpc query");
    assert_eq!(read.snapshot.value, "initial");
    assert_eq!(read.observed_context, query_context);
    let write = xrpc
        .put_value(
            RequestContext::root(),
            PutValueParams {
                value: "next".into(),
            },
        )
        .await
        .expect("xrpc procedure");
    assert_eq!(write.snapshot.value, "next");

    let mut subscription = xrpc
        .subscribe_events(
            RequestContext::root(),
            SubscribeEventsParams {
                after_source_revision: write.snapshot.source_revision,
            },
        )
        .await
        .expect("xrpc subscription");
    let revision = service.inject_source("streamed");
    let event = subscription
        .recv()
        .await
        .expect("xrpc event")
        .expect("xrpc stream remains open");
    assert_eq!(event.source_revision, revision);
    assert_eq!(event.value, "streamed");
    executor.shutdown().await;
}

#[tokio::test]
async fn generated_metadata_request_api_preserves_transport_metadata() {
    struct CapturingExecutor {
        seen: Arc<StdMutex<Option<HttpMetadata>>>,
    }

    #[async_trait::async_trait]
    impl XrpcExecutor for CapturingExecutor {
        async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, XrpcTransportError> {
            *self.seen.lock().expect("metadata mutex") = Some(request.metadata);
            Ok(HttpResponse {
                status: 200,
                body: Some(
                    serde_json::to_value(GetValueOutput {
                        snapshot: ValueSnapshot {
                            value: "initial".into(),
                            source_revision: 0,
                            service_revision: 0,
                        },
                        observed_context: request.context,
                    })
                    .expect("sample output serializes"),
                ),
                stream: None,
            })
        }
    }

    let seen = Arc::new(StdMutex::new(None));
    let client = SampleClient::xrpc(Arc::new(CapturingExecutor {
        seen: Arc::clone(&seen),
    }));
    let metadata = HttpMetadata {
        authorization: Some("capability".into()),
        idempotency_key: Some("command-1".into()),
        ..HttpMetadata::default()
    };
    client
        .get_value_with_metadata(Request::with_metadata(
            RequestContext::root(),
            metadata.clone(),
            GetValueParams,
        ))
        .await
        .expect("metadata XRPC query");
    assert_eq!(*seen.lock().expect("metadata mutex"), Some(metadata));
}

#[tokio::test]
async fn stream_capacity_is_independent_from_unary_capacity() {
    let service = SampleService::new("initial");
    let (client, adapter) = spawn_sample_irpc(Arc::clone(&service), 8, 1, 1);
    let subscription = client
        .subscribe_events(
            RequestContext::root(),
            SubscribeEventsParams {
                after_source_revision: 0,
            },
        )
        .await
        .expect("sub");
    adapter.wait_for_stream_peak(1).await;
    let query = client
        .get_value(RequestContext::root(), GetValueParams)
        .await
        .expect("query remains available");
    assert_eq!(query.snapshot.value, "initial");
    assert_eq!(adapter.stream_pumps(), 1);
    let second = client
        .subscribe_events(
            RequestContext::root(),
            SubscribeEventsParams {
                after_source_revision: 0,
            },
        )
        .await
        .expect("second sub request");
    let mut second = second;
    assert!(matches!(
        second.recv().await,
        Err(ClientError::Operation(
            SampleError::SubscriptionCapacityExceeded { limit: 1 }
        ))
    ));
    drop(subscription);
}

#[tokio::test]
async fn service_shutdown_joins_owned_projector() {
    let service = SampleService::new("initial");
    service.shutdown().await;
    assert_eq!(service.middleware_log().await, Vec::<String>::new());
}
