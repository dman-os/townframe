//! The IRPC adapter's admission and lifecycle guarantees: bounded concurrency,
//! independent unary and stream capacity, cooperative cancellation, and a
//! stop that joins every admitted task.

use super::{Stack, wait_for_activity};
use crate::{
    Client, ClientError, ContextError, RequestContext,
    sample::{GetValue, PutValue, SampleError, SampleMessage, SampleProtocol, SubscribeEvents},
};
use futures::future::join_all;

#[tokio::test]
async fn unary_admission_is_bounded_by_permits_not_by_queue_depth() {
    let stack = Stack::boot(1, 1).await;
    let gate = stack.sample.service.block_projector();

    let writes = join_all((0..3).map(|index| {
        let client = stack.irpc.clone();
        async move {
            client
                .call(
                    RequestContext::root(),
                    PutValue {
                        value: format!("write-{index}"),
                    },
                )
                .await
        }
    }));
    let writes = tokio::spawn(writes);

    wait_for_activity(&stack.admission, |activity| activity.active_count() == 1).await;
    assert_eq!(stack.admission.activity().peak_count(), 1);
    gate.add_permits(3);
    for result in writes.await.expect("writers") {
        result.expect("write completes once admitted");
    }
    assert_eq!(stack.admission.activity().peak_count(), 1);
    wait_for_activity(&stack.admission, |activity| activity.active_count() == 0).await;
    stack.shutdown().await;
}

#[tokio::test]
async fn stream_capacity_is_independent_from_unary_capacity_and_is_released() {
    let stack = Stack::boot(1, 1).await;
    let params = SubscribeEvents {
        after_source_revision: 0,
    };

    let first = stack
        .irpc
        .subscribe(RequestContext::root(), params.clone())
        .await
        .expect("first subscription");
    wait_for_activity(&stack.admission, |activity| {
        activity.stream_pump_count() == 1
    })
    .await;

    let query = stack
        .irpc
        .call(RequestContext::root(), GetValue {})
        .await
        .expect("unary capacity is untouched by the stream pump");
    assert_eq!(query.snapshot.value, "initial");

    let mut second = stack
        .irpc
        .subscribe(RequestContext::root(), params.clone())
        .await
        .expect("second subscription request is sent");
    assert!(matches!(
        second.recv().await,
        Err(ClientError::Operation(SampleError::Context(
            ContextError::StreamCapacityExceeded { limit: 1 }
        )))
    ));

    drop(first);
    wait_for_activity(&stack.admission, |activity| {
        activity.stream_pump_count() == 0
    })
    .await;
    let mut third = stack
        .irpc
        .subscribe(RequestContext::root(), params)
        .await
        .expect("third subscription");
    let revision = stack.sample.service.inject_source("after release");
    let event = third.recv().await.expect("event").expect("open");
    assert_eq!(event.source_revision, revision);
    stack.shutdown().await;
}

#[tokio::test]
async fn cancelling_the_context_ends_an_admitted_call() {
    let stack = Stack::boot(1, 1).await;
    let _gate = stack.sample.service.block_projector();
    let context = RequestContext::root();
    let blocked = tokio::spawn({
        let client = stack.irpc.clone();
        let context = context.clone();
        async move {
            client
                .call(
                    context,
                    PutValue {
                        value: "cancelled mid-flight".into(),
                    },
                )
                .await
        }
    });
    wait_for_activity(&stack.admission, |activity| activity.active_count() == 1).await;

    context.cancel();
    assert!(matches!(
        blocked.await.expect("writer task"),
        Err(ClientError::Operation(SampleError::Context(
            ContextError::Cancelled
        )))
    ));
    wait_for_activity(&stack.admission, |activity| activity.active_count() == 0).await;
    stack.shutdown().await;
}

#[tokio::test]
async fn adapter_stop_cancels_in_flight_work_and_joins_it() {
    let stack = Stack::boot(2, 1).await;
    let _gate = stack.sample.service.block_projector();
    let blocked = tokio::spawn({
        let client = stack.irpc.clone();
        async move {
            client
                .call(
                    RequestContext::root(),
                    PutValue {
                        value: "outlives nothing".into(),
                    },
                )
                .await
        }
    });
    let mut subscription = stack
        .irpc
        .subscribe(
            RequestContext::root(),
            SubscribeEvents {
                after_source_revision: 0,
            },
        )
        .await
        .expect("subscription");
    wait_for_activity(&stack.admission, |activity| {
        activity.active_count() == 1 && activity.stream_pump_count() == 1
    })
    .await;

    let Stack {
        sample,
        admission,
        irpc_stop,
        stop,
        http_task,
        http_shutdown,
        ..
    } = stack;
    irpc_stop.stop().await;

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
    assert_eq!(admission.activity().active_count(), 0);
    assert_eq!(admission.activity().stream_pump_count(), 0);
    assert_eq!(admission.activity().admitted_count(), 0);

    http_shutdown
        .send(())
        .expect("HTTP server is still serving");
    http_task
        .await
        .expect("HTTP server task join")
        .expect("HTTP server exit");
    stop.stop().await;
    drop(sample);
}

#[tokio::test]
async fn a_lost_reply_is_outcome_unknown_only_for_procedures() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let client = Client::<SampleProtocol>::irpc(irpc::Client::local(sender));
    let dropper = tokio::spawn(async move {
        for _ in 0..2 {
            match receiver.recv().await {
                Some(SampleMessage::PutValue(message)) => drop(message),
                Some(SampleMessage::GetValue(message)) => drop(message),
                other => panic!("unexpected IRPC message: {other:?}"),
            }
        }
    });

    let procedure = client
        .call(
            RequestContext::root(),
            PutValue {
                value: "write".into(),
            },
        )
        .await;
    let query = client.call(RequestContext::root(), GetValue {}).await;
    dropper.await.expect("reply-drop task");

    assert!(matches!(
        procedure,
        Err(ClientError::Transport {
            outcome_unknown: true,
            ..
        })
    ));
    assert!(matches!(
        query,
        Err(ClientError::Transport {
            outcome_unknown: false,
            ..
        })
    ));
}
