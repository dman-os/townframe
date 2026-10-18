//! Behaviour of the sample itself, driven through the direct transport. The
//! transport-independent contracts (parity, admission, shutdown) live in the
//! crate's `e2e` module and use the sample as their subject.

use super::{
    GetValue, PutValue, ReadState, SampleError, SampleProtocol, SubscribeEvents, boot,
    service::TEST_TASK_MARKER,
};
use crate::{Client, ClientError, ContextError, RequestContext};
use std::sync::Arc;

fn nsid_log(nsid: &str) -> Vec<String> {
    vec![format!("{nsid}:start"), format!("{nsid}:complete")]
}

#[tokio::test]
async fn direct_calls_run_inline_on_the_caller_task_through_the_layers() {
    let (sample, stop) = boot("initial");
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));
    let root = RequestContext::root();
    let child = root.child();

    let output = TEST_TASK_MARKER
        .scope(41, client.call(child.clone(), GetValue {}))
        .await
        .expect("direct query");

    assert_eq!(output.observed_context, child);
    assert_eq!(output.snapshot.value, "initial");
    assert_eq!(sample.service.last_handler_task(), Some(41));
    assert_eq!(
        sample.calls.entries(),
        nsid_log("org.example.sample.getValue")
    );
    stop.stop().await;
}

#[tokio::test]
async fn local_and_external_source_changes_share_projector_and_coalesce() {
    let (sample, stop) = boot("initial");
    let gate = sample.service.block_projector();
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));
    let mut events = sample.service.subscribe_events();

    sample.service.inject_source("A");
    sample.service.inject_source("B");
    let last = sample.service.inject_source("C");
    gate.add_permits(1);

    let event = events.recv().await.expect("coalesced projection event");
    assert_eq!(event.value, "C");
    assert_eq!(event.source_revision, last);
    assert_eq!(event.service_revision, 1);

    let page = client
        .call(
            RequestContext::root(),
            ReadState {
                after_source_revision: 0,
            },
        )
        .await
        .expect("state");
    assert_eq!(page.snapshot.value, "C");
    assert_eq!(page.snapshot.source_revision, 3);
    assert!(!page.reset_required);
    stop.stop().await;
}

#[tokio::test]
async fn mutation_waits_for_projector_and_external_changes_are_not_mutation_events() {
    let (sample, stop) = boot("initial");
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));

    let written = client
        .call(
            RequestContext::root(),
            PutValue {
                value: "local".into(),
            },
        )
        .await
        .expect("write");
    assert_eq!(written.snapshot.value, "local");
    assert_eq!(written.snapshot.source_revision, 1);

    let mut events = sample.service.subscribe_events();
    let external_revision = sample.service.inject_source("remote");
    let external_event = events.recv().await.expect("external projection event");
    assert_eq!(external_event.source_revision, external_revision);
    assert_eq!(external_event.value, "remote");

    let page = client
        .call(
            RequestContext::root(),
            ReadState {
                after_source_revision: external_revision,
            },
        )
        .await
        .expect("read");
    assert_eq!(page.snapshot.source_revision, external_revision);
    assert_eq!(page.snapshot.value, "remote");
    assert!(!page.reset_required);

    let ahead = client
        .call(
            RequestContext::root(),
            ReadState {
                after_source_revision: external_revision + 1,
            },
        )
        .await
        .expect("read ahead of the source");
    assert!(ahead.reset_required);
    stop.stop().await;
}

#[tokio::test]
async fn concurrent_source_writers_get_distinct_monotonic_revisions() {
    let (sample, stop) = boot("initial");
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));
    let (left, right) = tokio::join!(
        client.call(
            RequestContext::root(),
            PutValue {
                value: "left".into()
            }
        ),
        client.call(
            RequestContext::root(),
            PutValue {
                value: "right".into()
            }
        ),
    );
    left.expect("left write");
    right.expect("right write");
    let page = client
        .call(
            RequestContext::root(),
            ReadState {
                after_source_revision: 0,
            },
        )
        .await
        .expect("final state");
    assert_eq!(page.snapshot.source_revision, 2);
    stop.stop().await;
}

#[tokio::test]
async fn empty_value_is_rejected_before_the_source_is_touched() {
    let (sample, stop) = boot("initial");
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));
    let rejected = client
        .call(
            RequestContext::root(),
            PutValue {
                value: String::new(),
            },
        )
        .await;
    assert!(matches!(
        rejected,
        Err(ClientError::Operation(SampleError::InvalidValue { .. }))
    ));
    let page = client
        .call(
            RequestContext::root(),
            ReadState {
                after_source_revision: 0,
            },
        )
        .await
        .expect("state");
    assert_eq!(page.snapshot.source_revision, 0);
    assert_eq!(
        sample.calls.entries(),
        [
            nsid_log("org.example.sample.putValue"),
            nsid_log("org.example.sample.readState"),
        ]
        .concat()
    );
    stop.stop().await;
}

#[tokio::test]
async fn subscription_replays_current_state_then_live_events_and_skips_stale_ones() {
    let (sample, stop) = boot("initial");
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));
    let first = sample.service.inject_source("first");
    client
        .call(
            RequestContext::root(),
            PutValue {
                value: "second".into(),
            },
        )
        .await
        .expect("write establishes a projected revision");

    let mut behind = client
        .subscribe(
            RequestContext::root(),
            SubscribeEvents {
                after_source_revision: first,
            },
        )
        .await
        .expect("subscription");
    let replayed = behind.recv().await.expect("replay").expect("open");
    assert_eq!(replayed.value, "second");
    assert_eq!(replayed.source_revision, first + 1);

    let mut current = client
        .subscribe(
            RequestContext::root(),
            SubscribeEvents {
                after_source_revision: first + 1,
            },
        )
        .await
        .expect("subscription at the head");
    let live = sample.service.inject_source("third");
    let behind_live = behind.recv().await.expect("live").expect("open");
    let current_live = current.recv().await.expect("live").expect("open");
    assert_eq!(behind_live.source_revision, live);
    assert_eq!(current_live, behind_live);
    stop.stop().await;
}

#[tokio::test]
async fn lagged_subscriber_gets_a_terminal_error_and_the_stream_ends() {
    let (sample, stop) = boot("initial");
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host)).with_event_buffer(1);
    let mut lagging = client
        .subscribe(
            RequestContext::root(),
            SubscribeEvents {
                after_source_revision: 0,
            },
        )
        .await
        .expect("subscription");
    let mut raw = sample.service.subscribe_events();
    for index in 0..80 {
        sample.service.inject_source(format!("value-{index}"));
        raw.recv().await.expect("each projection lands separately");
    }

    let mut delivered = 0;
    let terminal = loop {
        match lagging.recv().await {
            Ok(Some(_)) => delivered += 1,
            other => break other,
        }
    };
    assert!(delivered <= 2, "buffered events only: {delivered}");
    assert!(matches!(
        terminal,
        Err(ClientError::Operation(SampleError::SubscriptionLagged { skipped })) if skipped > 0
    ));
    assert!(matches!(lagging.recv().await, Ok(None)));
    stop.stop().await;
}

#[tokio::test]
async fn stop_fails_a_write_waiting_on_the_projector_and_joins_everything() {
    let (sample, stop) = boot("initial");
    let gate = sample.service.block_projector();
    let client = Client::<SampleProtocol>::direct(Arc::clone(&sample.host));
    let mut subscription = client
        .subscribe(
            RequestContext::root(),
            SubscribeEvents {
                after_source_revision: 0,
            },
        )
        .await
        .expect("subscription");

    let blocked = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .call(
                    RequestContext::root(),
                    PutValue {
                        value: "never projected".into(),
                    },
                )
                .await
        }
    });
    tokio::task::yield_now().await;

    stop.stop().await;
    assert!(matches!(
        blocked.await.expect("writer task"),
        Err(ClientError::Operation(SampleError::Context(
            ContextError::Cancelled
        )))
    ));
    assert!(matches!(
        subscription.recv().await,
        Err(ClientError::Operation(SampleError::Context(
            ContextError::Cancelled
        )))
    ));
    drop(gate);
}
