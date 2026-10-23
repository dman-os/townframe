use super::*;
use crate::RequestContext;

#[tokio::test]
async fn terminal_lease_rejects_delayed_serialized_admission_after_cancel_and_drop() {
    let (transport, receiver) = cancellation_channel(4);
    let producer = RequestContext::root().with_cancellation_transport(transport);
    let guard = producer
        .cancellation
        .begin_transport_request()
        .unwrap()
        .unwrap();
    let wire = serde_json::to_vec(&producer).unwrap();
    producer.cancel();
    drop(guard);
    let delayed: RequestContext = serde_json::from_slice(&wire).unwrap();
    assert!(
        receiver
            .register_cancellation(delayed.cancellation.clone())
            .is_err()
    );
    assert!(delayed.cancellation.is_cancelled());
    let replay: RequestContext = serde_json::from_slice(&wire).unwrap();
    assert!(receiver.register_cancellation(replay.cancellation).is_err());
}

#[tokio::test]
async fn alternating_delayed_admissions_remain_bounded() {
    let (transport, receiver) = cancellation_channel(2);
    let oldest = RequestContext::root().with_cancellation_transport(transport.clone());
    let _oldest_guard = oldest
        .cancellation
        .begin_transport_request()
        .unwrap()
        .unwrap();
    for _ in 0..128 {
        let producer = RequestContext::root().with_cancellation_transport(transport.clone());
        let guard = producer
            .cancellation
            .begin_transport_request()
            .unwrap()
            .unwrap();
        let wire = serde_json::to_vec(&producer).unwrap();
        drop(guard);
        let delayed: RequestContext = serde_json::from_slice(&wire).unwrap();
        assert!(
            receiver
                .register_cancellation(delayed.cancellation)
                .is_err()
        );
        assert_eq!(receiver.admissions.lock().unwrap().live.len(), 1);
    }
}

#[tokio::test]
async fn three_queued_cancellation_scopes_do_not_panic() {
    let (transport, receiver) = cancellation_channel(4);
    let mut queued = Vec::new();
    for _ in 0..3 {
        let producer = RequestContext::root().with_cancellation_transport(transport.clone());
        let guard = producer
            .cancellation
            .begin_transport_request()
            .unwrap()
            .unwrap();
        producer.cancel();
        let wire = serde_json::to_vec(&producer).unwrap();
        queued.push((wire, guard));
    }
    for (wire, guard) in queued {
        let delayed: RequestContext = serde_json::from_slice(&wire).unwrap();
        let registration = receiver
            .register_cancellation(delayed.cancellation.clone())
            .unwrap();
        assert!(delayed.cancellation.is_cancelled());
        drop(registration);
        drop(guard);
    }
    let (transport, _receiver) = cancellation_channel(1);
    let first = CancellationSignal::with_transport(transport.clone());
    let first_guard = first.begin_transport_request().unwrap().unwrap();
    let second = CancellationSignal::with_transport(transport);
    assert!(matches!(
        second.begin_transport_request(),
        Err(ContextError::CancellationCapacityExceeded { limit: 1 })
    ));
    drop(first_guard);
}

#[test]
fn dropped_scopes_do_not_make_later_cancel_panic() {
    let (transport, _receiver) = cancellation_channel(1);
    for _ in 0..64 {
        let signal = CancellationSignal::with_transport(transport.clone());
        let guard = signal.begin_transport_request().unwrap().unwrap();
        drop(guard);
    }
    let signal = CancellationSignal::with_transport(transport);
    let guard = signal.begin_transport_request().unwrap().unwrap();
    signal.cancel();
    drop(guard);
}

#[tokio::test]
async fn lease_validation_fails_closed_but_nontransport_contexts_remain_valid() {
    let (transport, receiver) = cancellation_channel(4);
    let producer = RequestContext::root().with_cancellation_transport(transport);
    let _guard = producer
        .cancellation
        .begin_transport_request()
        .unwrap()
        .unwrap();
    let wire = serde_json::to_value(&producer).unwrap();
    for mutation in [
        ("epoch", serde_json::json!(Uuid::now_v7().to_string())),
        ("ordinal", serde_json::json!(u64::MAX)),
    ] {
        let mut value = wire.clone();
        value["cancellation"]["admission"][mutation.0] = mutation.1;
        let malformed: RequestContext = serde_json::from_value(value).unwrap();
        assert!(
            receiver
                .register_cancellation(malformed.cancellation)
                .is_err()
        );
    }
    let mut missing = wire.clone();
    missing["cancellation"]["admission"] = serde_json::Value::Null;
    let missing: RequestContext = serde_json::from_value(missing).unwrap();
    assert!(
        receiver
            .register_cancellation(missing.cancellation)
            .is_err()
    );
    let ordinary = RequestContext::root();
    let ordinary_registration = receiver
        .register_cancellation(ordinary.cancellation)
        .unwrap();
    drop(ordinary_registration);
}

#[tokio::test]
async fn same_state_and_serde_detached_admissions_share_canonical_cancellation() {
    let (transport, receiver) = cancellation_channel(4);
    let producer = RequestContext::root().with_cancellation_transport(transport.clone());
    let guard = producer
        .cancellation
        .begin_transport_request()
        .unwrap()
        .unwrap();
    let same_state = receiver
        .register_cancellation(producer.cancellation.clone())
        .unwrap();
    producer.cancel();
    assert!(producer.cancellation.is_cancelled());
    drop(same_state);
    drop(guard);
    let producer = RequestContext::root().with_cancellation_transport(transport);
    let guard = producer
        .cancellation
        .begin_transport_request()
        .unwrap()
        .unwrap();
    let wire = serde_json::to_vec(&producer).unwrap();
    let detached: RequestContext = serde_json::from_slice(&wire).unwrap();
    let registration = receiver
        .register_cancellation(detached.cancellation.clone())
        .unwrap();
    producer.cancel();
    assert!(detached.cancellation.is_cancelled());
    drop(registration);
    drop(guard);
}

#[tokio::test]
async fn producer_drop_does_not_cancel_admitted_work() {
    let (transport, receiver) = cancellation_channel(2);
    let producer = RequestContext::root().with_cancellation_transport(transport);
    let guard = producer
        .cancellation
        .begin_transport_request()
        .unwrap()
        .unwrap();
    let wire = serde_json::to_vec(&producer).unwrap();
    let detached: RequestContext = serde_json::from_slice(&wire).unwrap();
    let registration = receiver
        .register_cancellation(detached.cancellation.clone())
        .unwrap();
    let producer_state = Arc::downgrade(&producer.cancellation.state);
    drop(guard);
    drop(producer);
    assert!(producer_state.upgrade().is_some());
    assert!(!detached.cancellation.is_cancelled());
    drop(registration);
    drop(detached);
    assert!(producer_state.upgrade().is_none());
}

#[test]
fn ordinary_duplicate_registration_is_atomic() {
    let receiver = Arc::new(cancellation_channel(3).1);
    let context = RequestContext::root();
    let wire = serde_json::to_vec(&context).unwrap();
    let first = serde_json::from_slice::<RequestContext>(&wire)
        .unwrap()
        .cancellation;
    let second = serde_json::from_slice::<RequestContext>(&wire)
        .unwrap()
        .cancellation;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    receiver.set_ordinary_admission_hook(Arc::new(OrdinaryAdmissionHook {
        first: AtomicBool::new(false),
        ready: std::sync::Mutex::new(Some(ready_tx)),
        release: std::sync::Mutex::new(Some(release_rx)),
    }));
    let first_receiver = Arc::clone(&receiver);
    let first_task = std::thread::spawn(move || first_receiver.register_cancellation(first));
    ready_rx.recv().unwrap();
    let (contender_started_tx, contender_started_rx) = std::sync::mpsc::channel();
    let second_receiver = Arc::clone(&receiver);
    let second_task = std::thread::spawn(move || {
        contender_started_tx.send(()).unwrap();
        second_receiver.register_cancellation(second)
    });
    contender_started_rx.recv().unwrap();
    assert!(receiver.cancellations.try_lock().is_err());
    release_tx.send(()).unwrap();
    let first_result = first_task.join().unwrap();
    let second_result = second_task.join().unwrap();
    assert!(first_result.is_ok());
    assert!(matches!(
        second_result,
        Err(ContextError::CancellationAlreadyAdmitted)
    ));
}

#[tokio::test]
async fn cancellation_waiter_registers_before_final_flag_check() {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let hook = Arc::new(CancellationWaitHook {
        point: WaitHookPoint::AfterFlagCheck,
        gates: std::sync::Mutex::new(std::collections::VecDeque::from([WaitGate {
            ready: ready_tx,
            release: Arc::clone(&release),
        }])),
    });
    let signal = CancellationSignal::default();
    signal.set_wait_hook(hook);
    let mut waiter = Box::pin(signal.cancelled());
    let waker = futures::task::noop_waker_ref();
    let mut context = std::task::Context::from_waker(waker);
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Pending
    ));
    ready_rx.await.unwrap();
    signal.cancel();
    release.notify_one();
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Ready(())
    ));

    let (transport, receiver) = cancellation_channel(2);
    let producer = CancellationSignal::with_transport(transport);
    let producer_guard = producer.begin_transport_request().unwrap().unwrap();
    let wire = serde_json::to_vec(&producer).unwrap();
    let detached: CancellationSignal = serde_json::from_slice(&wire).unwrap();
    let registration = receiver.register_cancellation(detached.clone()).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    detached.set_wait_hook(Arc::new(CancellationWaitHook {
        point: WaitHookPoint::AfterFlagCheck,
        gates: std::sync::Mutex::new(std::collections::VecDeque::from([WaitGate {
            ready: ready_tx,
            release: Arc::clone(&release),
        }])),
    }));
    let mut waiter = Box::pin(detached.cancelled());
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Pending
    ));
    ready_rx.await.unwrap();
    producer.cancel();
    release.notify_one();
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Ready(())
    ));
    drop(registration);
    drop(producer_guard);
}

#[tokio::test]
async fn binding_wakes_a_detached_waiter_already_waiting_locally() {
    let (transport, receiver) = cancellation_channel(2);
    let producer = CancellationSignal::with_transport(transport);
    let producer_guard = producer.begin_transport_request().unwrap().unwrap();
    let wire = serde_json::to_vec(&producer).unwrap();
    let detached: CancellationSignal = serde_json::from_slice(&wire).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    detached.set_wait_hook(Arc::new(CancellationWaitHook {
        point: WaitHookPoint::BeforeWait,
        gates: std::sync::Mutex::new(std::collections::VecDeque::from([WaitGate {
            ready: ready_tx,
            release: Arc::clone(&release),
        }])),
    }));
    let waiter_signal = detached.clone();
    let mut waiter = Box::pin(waiter_signal.cancelled());
    let waker = futures::task::noop_waker_ref();
    let mut context = std::task::Context::from_waker(waker);
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Pending
    ));
    ready_rx.await.unwrap();
    release.notify_one();
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Pending
    ));
    let registration = receiver.register_cancellation(detached).unwrap();
    assert!(!producer.is_cancelled());
    producer.cancel();
    assert!(matches!(
        std::future::Future::poll(waiter.as_mut(), &mut context),
        std::task::Poll::Ready(())
    ));
    drop(registration);
    drop(producer_guard);
}

#[tokio::test]
async fn cancellation_before_admission_is_consumed_by_matching_registration() {
    let (transport, receiver) = cancellation_channel(4);
    let signal = CancellationSignal::with_transport(transport);
    let _request = signal.begin_transport_request().unwrap().unwrap();
    signal.cancel();
    let registration = receiver.register_cancellation(signal.clone()).unwrap();
    assert!(signal.is_cancelled());
    drop(registration);
}

#[tokio::test]
async fn completed_transport_scope_ignores_late_cancellation() {
    let (transport, _receiver) = cancellation_channel(1);
    let signal = CancellationSignal::with_transport(transport);
    let request = signal.begin_transport_request().unwrap().unwrap();
    drop(request);
    signal.cancel();
    assert!(signal.is_cancelled());
}

#[tokio::test]
async fn unmatched_cancellation_does_not_exhaust_registry_capacity() {
    let (transport, receiver) = cancellation_channel(1);
    for _ in 0..4 {
        let signal = CancellationSignal::with_transport(transport.clone());
        let guard = signal.begin_transport_request().unwrap().unwrap();
        signal.cancel();
        let registration = receiver.register_cancellation(signal).unwrap();
        drop(registration);
        drop(guard);
    }
}
