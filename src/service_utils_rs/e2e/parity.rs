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
            (
                observed.request_id,
                observed.trace_id,
                observed.span_id,
                observed.parent_span_id
            ),
            (
                context.request_id,
                context.trace_id,
                context.span_id,
                context.parent_span_id
            ),
            "{name}: request identity survives the hop"
        );
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
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
        tracestate: Some("vendor=value".into()),
        timeout: Some(Duration::from_secs(30)),
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
        assert_eq!(read.observed_metadata, metadata, "{name}");
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
async fn subscription_establishment_errors_arrive_as_the_first_stream_item() {
    let stack = Stack::boot(4, 4).await;
    let mut invalid = RequestContext::root();
    invalid.request_id = uuid::Uuid::nil();
    let params = SubscribeEvents {
        after_source_revision: 0,
    };

    for (name, client) in [("direct", &stack.direct), ("irpc", &stack.irpc)] {
        let mut stream = client
            .subscribe(invalid.clone(), params.clone())
            .await
            .unwrap_or_else(|error| panic!("{name} subscribe sends: {error:?}"));
        assert!(
            matches!(
                stream.recv().await,
                Err(ClientError::Operation(SampleError::Context(
                    ContextError::MissingRequestId
                )))
            ),
            "{name}"
        );
        assert!(matches!(stream.recv().await, Ok(None)), "{name} ends");
    }

    let unsupported = stack.xrpc.subscribe(RequestContext::root(), params).await;
    assert!(matches!(
        unsupported,
        Err(ClientError::Transport {
            outcome_unknown: false,
            ..
        })
    ));
    stack.shutdown().await;
}
