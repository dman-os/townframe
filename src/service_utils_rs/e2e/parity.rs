//! The same request through direct, IRPC, and HTTP must produce the same
//! typed result: outputs, domain errors, host-side context rejection, and the
//! metadata the handler observes.

use super::Stack;
use crate::{
    ClientError, ContextError, Envelope, HttpMetadata, RequestContext,
    sample::{GetValue, PutValue, SampleError, SubscribeEvents},
};
use std::time::Duration;

#[tokio::test]
async fn query_and_procedure_agree_across_transports() {
    let stack = Stack::boot(4, 4).await;
    for (index, (name, client)) in stack.transports().into_iter().enumerate() {
        let value = format!("written-by-{name}");
        let written = client
            .call(
                RequestContext::root(),
                PutValue {
                    value: value.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{name} procedure: {error:?}"));
        assert_eq!(written.snapshot.value, value, "{name}");
        assert_eq!(written.snapshot.source_revision, index as u64 + 1, "{name}");

        let context = RequestContext::root();
        let read = client
            .call(context.clone(), GetValue {})
            .await
            .unwrap_or_else(|error| panic!("{name} query: {error:?}"));
        assert_eq!(read.snapshot, written.snapshot, "{name}");
        let observed = &read.observed_context;
        assert_eq!(
            observed.request_id, context.request_id,
            "{name}: request id"
        );
        assert_eq!(observed.trace, context.trace, "{name}: trace context");
        if name != "xrpc" {
            assert_eq!(
                read.observed_context, context,
                "{name}: native hop keeps the whole context"
            );
        }
    }
    stack.shutdown().await;
}

#[tokio::test]
async fn metadata_reaches_the_handler_on_every_transport() {
    let stack = Stack::boot(4, 4).await;
    let metadata = HttpMetadata {
        authorization: Some("Bearer capability".into()),
        idempotency_key: Some("command-1".into()),
    };
    for (name, client) in stack.transports() {
        let read = client
            .call_with_metadata(Envelope::with_metadata(
                RequestContext::root(),
                metadata.clone(),
                GetValue {},
            ))
            .await
            .unwrap_or_else(|error| panic!("{name} query: {error:?}"));
        assert_eq!(read.output.observed_metadata, metadata, "{name}");
    }
    stack.shutdown().await;
}

#[tokio::test]
async fn domain_errors_are_typed_on_every_transport() {
    let stack = Stack::boot(4, 4).await;
    for (name, client) in stack.transports() {
        let rejected = client
            .call(
                RequestContext::root(),
                PutValue {
                    value: String::new(),
                },
            )
            .await;
        assert!(
            matches!(
                rejected,
                Err(ClientError::Operation(SampleError::InvalidValue { ref message }))
                    if message == "value cannot be empty"
            ),
            "{name}: {rejected:?}"
        );
    }
    stack.shutdown().await;
}

#[tokio::test]
async fn invalid_and_expired_contexts_are_rejected_before_the_handler_on_every_transport() {
    let stack = Stack::boot(4, 4).await;
    let mut missing_request_id = RequestContext::root();
    missing_request_id.request_id = uuid::Uuid::nil();
    let expired = RequestContext::root().with_time_budget(Duration::ZERO);
    let cancelled = RequestContext::root();
    cancelled.cancel();
    let cases = [
        (
            "missing request id",
            missing_request_id,
            ContextError::MissingRequestId,
        ),
        ("expired", expired, ContextError::DeadlineExceeded),
        ("cancelled", cancelled, ContextError::Cancelled),
    ];

    for (name, client) in stack.transports() {
        for (case, context, expected) in &cases {
            let result = client
                .call(
                    context.clone(),
                    PutValue {
                        value: "never written".into(),
                    },
                )
                .await;
            assert!(
                matches!(
                    result,
                    Err(ClientError::Operation(SampleError::Context(ref error))) if error == expected
                ),
                "{name}/{case}: {result:?}"
            );
        }
    }

    let page = stack
        .direct
        .call(RequestContext::root(), GetValue {})
        .await
        .expect("state after rejected writes");
    assert_eq!(page.snapshot.source_revision, 0);
    assert!(
        stack
            .sample
            .calls
            .entries()
            .iter()
            .all(|entry| entry.starts_with("org.example.sample.getValue")),
        "rejected requests never reach the layers: {:?}",
        stack.sample.calls.entries()
    );
    stack.shutdown().await;
}

#[tokio::test]
async fn an_in_flight_call_fails_at_its_deadline_on_every_transport() {
    let stack = Stack::boot(4, 4).await;
    let gate = stack.sample.service.block_projector();
    for (name, client) in stack.transports() {
        let budgeted = RequestContext::root().with_time_budget(Duration::from_millis(20));
        let result = client
            .call(
                budgeted,
                PutValue {
                    value: "too slow".into(),
                },
            )
            .await;
        assert_eq!(
            result,
            Err(ClientError::Operation(SampleError::Context(
                ContextError::DeadlineExceeded
            ))),
            "{name}"
        );
    }
    drop(gate);
    stack.shutdown().await;
}

#[tokio::test]
async fn subscription_establishment_errors_are_returned_before_the_stream() {
    let stack = Stack::boot(4, 4).await;
    let mut invalid = RequestContext::root();
    invalid.request_id = uuid::Uuid::nil();
    let params = SubscribeEvents {
        after_source_revision: 0,
    };

    for (name, client) in [("direct", &stack.direct), ("irpc", &stack.irpc)] {
        let result = client.subscribe(invalid.clone(), params.clone()).await;
        assert!(
            matches!(
                result,
                Err(ClientError::Operation(SampleError::Context(
                    ContextError::MissingRequestId
                )))
            ),
            "{name}"
        );
    }

    // XRPC subscriptions run over WebSockets: establishment errors are
    // returned before the stream opens, exactly as on the native transports,
    // and a valid context opens a stream.
    let result = stack.xrpc.subscribe(invalid, params.clone()).await;
    assert!(
        matches!(
            result,
            Err(ClientError::Operation(SampleError::Context(
                ContextError::MissingRequestId
            )))
        ),
        "xrpc: {result:?}"
    );
    let stream = stack.xrpc.subscribe(RequestContext::root(), params).await;
    assert!(stream.is_ok(), "xrpc websocket subscription: {stream:?}");
    drop(stream);
    stack.shutdown().await;
}
