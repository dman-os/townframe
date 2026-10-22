use super::*;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;

#[test]
fn instance_values_do_not_retain_host_or_owned_services() {
    let host = Host::new();
    let state = Arc::downgrade(&host.inner);
    let instance = host.start("retained");
    let retained = instance.clone();
    let service = Arc::new(());
    let service_lifetime = Arc::downgrade(&service);
    host.own(&instance, Box::new(move || drop(service)))
        .unwrap();
    assert_eq!(Arc::strong_count(&host.inner), 1);
    drop(host);
    assert!(state.upgrade().is_none());
    assert!(service_lifetime.upgrade().is_none());
    assert_eq!(instance, retained);
    assert_eq!(retained.name, "retained");
}

#[test]
fn foreign_instance_with_colliding_id_is_denied() {
    let host = Host::new();
    let foreign_host = Host::new();
    let local = host.start("same-name");
    let foreign = foreign_host.start("same-name");
    assert_eq!(local.id, foreign.id);
    assert_ne!(local, foreign);
    assert_eq!(host.check(&foreign), Err(AccessError::Denied));
    assert_eq!(host.close_instance(&foreign), Err(AccessError::Denied));
    assert!(matches!(
        host.observe_instance(&foreign),
        Err(AccessError::Denied)
    ));
    assert_eq!(host.check(&local), Ok(()));
    assert_eq!(foreign_host.check(&foreign), Ok(()));
    host.close();
    assert_eq!(host.close_instance(&local), Ok(()));
    assert_eq!(host.close_instance(&foreign), Err(AccessError::Denied));
    foreign_host.close();
}

#[tokio::test]
#[should_panic(expected = "instance belongs to another host")]
async fn awaiting_a_foreign_instance_is_an_invariant_failure() {
    let host = Host::new();
    let foreign_host = Host::new();
    let instance = foreign_host.start("foreign");
    foreign_host.close();
    host.await_close(&instance).await;
}

#[tokio::test]
async fn stale_identity_cannot_revoke_replacement_and_terminal_storage_is_drained() {
    let host = Host::new();
    let stale = host.start("same-name");
    let terminal = host.observe_instance(&stale).unwrap();
    host.close_instance(&stale).unwrap();
    assert_eq!(*terminal.borrow(), Some(CloseReason::Requested));
    assert!(matches!(
        host.observe_instance(&stale),
        Err(AccessError::Revoked)
    ));
    for _ in 0..128 {
        let replacement = host.start("same-name");
        let ended = host.observe_instance(&replacement).unwrap();
        assert_ne!(stale, replacement);
        host.close_instance(&stale).unwrap();
        host.await_close(&stale).await;
        assert_eq!(host.check(&stale), Err(AccessError::Revoked));
        assert_eq!(host.check(&replacement), Ok(()));
        assert_eq!(*ended.borrow(), None);
        host.close_instance(&replacement).unwrap();
        assert_eq!(*ended.borrow(), Some(CloseReason::Requested));
        let state = host.inner.lock().unwrap();
        assert!(state.instances.is_empty());
        assert!(state.ending.is_empty());
        assert!(state.closing.is_empty());
        assert!(state.dependencies.is_empty());
        assert!(state.recovery.pending.is_empty());
    }
    host.close();
    assert_eq!(host.try_start("closed"), Err(AccessError::Revoked));
}

#[tokio::test]
async fn early_and_late_waiters_wait_for_terminal_cleanup() {
    use std::future::Future;
    use std::task::Poll;

    let host = Host::new();
    let instance = host.start("owned");
    let mut terminal = host.observe_instance(&instance).unwrap();
    let mut early = Box::pin(host.await_close(&instance));
    std::future::poll_fn(|context| {
        assert!(early.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    host.own(
        &instance,
        Box::new(move || {
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        }),
    )
    .unwrap();
    let closing_host = host.clone();
    let closing_instance = instance.clone();
    let closing = std::thread::spawn(move || {
        closing_host.close_instance(&closing_instance).unwrap();
    });
    entered_rx.await.unwrap();
    assert_eq!(host.check(&instance), Err(AccessError::Revoked));
    assert!(matches!(
        host.observe_instance(&instance),
        Err(AccessError::Revoked)
    ));
    assert_eq!(*terminal.borrow(), None);
    let mut late = Box::pin(host.await_close(&instance));
    std::future::poll_fn(|context| {
        assert!(early.as_mut().poll(context).is_pending());
        assert!(late.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    early.await;
    late.await;
    terminal.changed().await.unwrap();
    assert_eq!(*terminal.borrow(), Some(CloseReason::Requested));
    closing.join().unwrap();
    host.await_close(&instance).await;
    assert!(host.inner.lock().unwrap().ending.is_empty());
    host.close();
}

#[test]
fn collection_writer_binding_rejects_foreign_and_closed_authority() {
    let host = Host::new();
    let provider = host.start("provider");
    let contributor = host.start("contributor");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let foreign_host = Host::new();
    let foreign = foreign_host.start("foreign");
    assert!(matches!(
        collection.writer(&foreign),
        Err(AccessError::Denied)
    ));
    let writer = collection.writer(&contributor).unwrap();
    let entry = writer.insert("live".into()).unwrap();
    let closed = host.start("closed");
    host.close_instance(&closed).unwrap();
    assert!(matches!(
        collection.writer(&closed),
        Err(AccessError::Revoked)
    ));
    host.close_instance(&provider).unwrap();
    assert_eq!(host.check(&contributor), Err(AccessError::Revoked));
    assert_eq!(entry.update("stale".into()), Err(AccessError::Revoked));
    let replacement = host.start("replacement");
    assert!(matches!(
        collection.writer(&replacement),
        Err(AccessError::Revoked)
    ));
    assert_eq!(host.check(&replacement), Ok(()));
    assert!(host.inner.lock().unwrap().dependencies.is_empty());
    host.close();
    foreign_host.close();
}

#[test]
fn collection_writer_binding_rejects_queued_closes() {
    let host = Host::new();
    let blocker = host.start("blocker");
    let provider = host.start("provider");
    let contributor = host.start("contributor");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    host.own(
        &blocker,
        Box::new(move || {
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        }),
    )
    .unwrap();
    let closing_host = host.clone();
    let closing = std::thread::spawn(move || {
        closing_host.close_instance(&blocker).unwrap();
    });
    entered_rx.recv().unwrap();
    host.close_instance(&contributor).unwrap();
    let contributor_rejected = matches!(collection.writer(&contributor), Err(AccessError::Revoked));
    host.close_instance(&provider).unwrap();
    let live = host.start("live");
    let provider_rejected = matches!(collection.writer(&live), Err(AccessError::Revoked));
    release.send(()).unwrap();
    closing.join().unwrap();
    assert!(contributor_rejected);
    assert!(provider_rejected);
    assert_eq!(host.check(&live), Ok(()));
    assert!(host.inner.lock().unwrap().dependencies.is_empty());
    host.close();
}

#[test]
fn collection_entries_and_readers_follow_their_owned_lifetimes() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let first_contributor = host.start("first-contributor");
    let second_contributor = host.start("second-contributor");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let reader = collection.reader(&consumer);
    let first = collection
        .writer(&first_contributor)
        .unwrap()
        .insert("first".into())
        .unwrap();
    let second = collection
        .writer(&second_contributor)
        .unwrap()
        .insert("second".into())
        .unwrap();

    assert_eq!(
        reader
            .snapshot()
            .unwrap()
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    host.close_instance(&first_contributor).unwrap();
    assert_eq!(
        reader
            .snapshot()
            .unwrap()
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["second"]
    );
    assert_eq!(
        first.update("not retained".into()),
        Err(AccessError::Revoked)
    );
    assert!(second.update("updated".into()).is_ok());
    assert_eq!(
        reader
            .snapshot()
            .unwrap()
            .get(&second.id)
            .map(String::as_str),
        Some("updated")
    );

    host.close_instance(&provider).unwrap();
    assert_eq!(reader.snapshot(), Err(AccessError::Revoked));
    assert_eq!(
        second.update("not retained".into()),
        Err(AccessError::Revoked)
    );
    host.close();
}

#[test]
fn withdrawing_an_entry_is_idempotent_and_a_stale_lease_cannot_remove_a_replacement() {
    let host = Host::new();
    let provider = host.start("provider");
    let contributor = host.start("contributor");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let reader = collection.reader(&contributor);
    let writer = collection.writer(&contributor).unwrap();
    let entry = writer.insert("live".into()).unwrap();
    entry.release();
    // Releasing again — and a stale handle to the same id — must not error and must not
    // withdraw the replacement entry the slot is reused for.
    entry.release();
    writer.insert("replacement".into()).unwrap();
    assert_eq!(
        reader
            .snapshot()
            .unwrap()
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["replacement"]
    );
    // Ids are not reused — each insert mints a fresh one — so the stale handle names an
    // empty slot, and its generation matches no live entry either, so it cannot withdraw
    // the replacement.
    entry.release();
    assert_eq!(
        reader
            .snapshot()
            .unwrap()
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["replacement"]
    );
    assert_eq!(entry.update("stale".into()), Err(AccessError::Revoked));
    host.close();
}
#[tokio::test]
async fn cancelling_async_activation_withdraws_owned_effects() {
    use std::future::Future;
    use std::task::Poll;

    let host = Host::new();
    let root = host.start("root");
    let host_for_activation = host.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let mut activation = Box::pin(host.activate_async("async", move |instance| async move {
        let component = host_for_activation.publish_component(&instance)?;
        if ready.send((instance.clone(), component)).is_err() {
            panic!("activation waiter dropped before publication");
        }
        std::future::pending::<Result<(), AccessError>>().await
    }));
    std::future::poll_fn(|context| {
        assert!(activation.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    let (instance, component) = match started.await {
        Ok(value) => value,
        Err(_) => panic!("activation exited before publishing its component"),
    };
    drop(activation);
    assert_eq!(host.check(&instance), Err(AccessError::Revoked));
    assert!(matches!(
        host.mount(&root, component),
        Err(AccessError::Revoked)
    ));
    host.close();
}

#[test]
fn closure_notifications_follow_the_entire_dependent_recovery() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    host.bind_dependency(&consumer, &provider).unwrap();
    let provider_ended = host.observe_instance(&provider).unwrap();
    let consumer_ended = host.observe_instance(&consumer).unwrap();
    let mut changes = host.changes();
    let sibling = host.start("sibling");
    host.bind_dependency(&sibling, &provider).unwrap();
    let sibling_ended = host.observe_instance(&sibling).unwrap();
    changes.borrow_and_update();
    for instance in [&provider, &consumer, &sibling] {
        let observer = changes.clone();
        let terminals = [
            provider_ended.clone(),
            consumer_ended.clone(),
            sibling_ended.clone(),
        ];
        host.own(
            instance,
            Box::new(move || {
                assert!(
                    !observer.has_changed().unwrap(),
                    "closure notification overtook recovery"
                );
                for terminal in terminals {
                    assert_eq!(*terminal.borrow(), None);
                }
            }),
        )
        .unwrap();
    }
    host.close_instance(&provider).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    assert_eq!(*provider_ended.borrow(), Some(CloseReason::Requested));
    assert_eq!(
        *consumer_ended.borrow(),
        Some(CloseReason::DependencyWithdrawn)
    );
    host.close_instance(&provider).unwrap();
    assert!(
        !changes.has_changed().unwrap(),
        "duplicate close emitted activity"
    );
    host.close();
    assert_eq!(*changes.borrow(), HostStatus::Closed);
}

#[test]
fn host_shutdown_is_not_reported_complete_before_recovery() {
    let host = Host::new();
    let instance = host.start("owned");
    let ended = host.observe_instance(&instance).unwrap();
    let changes = host.changes();
    let during_cleanup = changes.clone();
    host.own(
        &instance,
        Box::new(move || {
            assert_eq!(*during_cleanup.borrow(), HostStatus::Closing);
        }),
    )
    .unwrap();
    host.close();
    assert_eq!(*changes.borrow(), HostStatus::Closed);
    assert_eq!(*ended.borrow(), Some(CloseReason::HostShutdown));
}

#[test]
fn a_close_started_inside_cleanup_recovers_dependents_before_their_provider() {
    // A host shutdown re-entered from an instance's own cleanup must still close the
    // consumer first, keep the provider live for the consumer's cleanup, and report
    // Closed only after the whole cascade finished — all without OS-thread blocking.
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    host.bind_dependency(&consumer, &provider).unwrap();
    let provider_ended = host.observe_instance(&provider).unwrap();
    let consumer_ended = host.observe_instance(&consumer).unwrap();
    let changes = host.changes();
    let provider_for_cleanup = provider.clone();
    let host_in_cleanup = host.clone();
    let recovered = Arc::new(Mutex::new(Vec::new()));
    let consumer_recovered = recovered.clone();
    host.own(
        &consumer,
        Box::new(move || {
            host_in_cleanup
                .check(&provider_for_cleanup)
                .expect("provider must outlive consumer cleanup");
            host_in_cleanup.close();
            consumer_recovered.lock().unwrap().push("consumer");
        }),
    )
    .unwrap();
    let provider_recovered = recovered.clone();
    let consumer_terminal = consumer_ended.clone();
    host.own(
        &provider,
        Box::new(move || {
            assert_eq!(*consumer_terminal.borrow(), None);
            provider_recovered.lock().unwrap().push("provider");
        }),
    )
    .unwrap();
    host.close_instance(&consumer).unwrap();
    assert_eq!(*recovered.lock().unwrap(), ["consumer", "provider"]);
    assert_eq!(*changes.borrow(), HostStatus::Closed);
    assert_eq!(*consumer_ended.borrow(), Some(CloseReason::Requested));
    assert_eq!(*provider_ended.borrow(), Some(CloseReason::HostShutdown));
}

#[test]
fn reentrant_shutdown_does_not_publish_closed_before_current_cleanup() {
    let host = Host::new();
    let instance = host.start("owned");
    let ended = host.observe_instance(&instance).unwrap();
    let changes = host.changes();
    let observer = changes.clone();
    let reentrant = host.clone();
    host.own(
        &instance,
        Box::new(move || {
            reentrant.close();
            assert_eq!(*observer.borrow(), HostStatus::Closing);
        }),
    )
    .unwrap();
    host.close_instance(&instance).unwrap();
    assert_eq!(*changes.borrow(), HostStatus::Closed);
    assert_eq!(*ended.borrow(), Some(CloseReason::Requested));
}

#[tokio::test]
async fn closing_an_instance_cancels_its_pending_activation() {
    use std::future::Future;
    use std::task::Poll;

    let host = Host::new();
    let (ready, started) = tokio::sync::oneshot::channel();
    let mut activation = Box::pin(host.activate_async("pending", |instance| async move {
        assert!(ready.send(instance).is_ok());
        std::future::pending::<Result<(), AccessError>>().await
    }));
    std::future::poll_fn(|context| {
        assert!(activation.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    host.close_instance(&started.await.unwrap()).unwrap();
    assert_eq!(activation.await, Err(AccessError::Revoked));
    host.close();
}
#[test]
fn collection_subscription_is_self_synchronizing_and_carries_each_mutation() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let reader = collection.reader(&consumer);
    let writer = collection.writer(&provider).unwrap();

    let mut early = reader.subscribe().unwrap();
    assert_eq!(
        early.try_recv(),
        Ok(Batch {
            incarnation: 1,
            base: Vec::new(),
            overlays: Vec::new(),
            settled: Vec::new(),
            covered: Vec::new(),
        })
    );

    let entry = writer.insert("first".into()).unwrap();
    let mut expected = Batch::new(1);
    expected.base = vec![BaseDelta::Upsert {
        key: entry.id,
        version: Version::new(1, 1),
        value: "first".into(),
    }];
    assert_eq!(early.try_recv(), Ok(expected));

    entry.update("updated".into()).unwrap();
    let mut expected = Batch::new(1);
    expected.base = vec![BaseDelta::Upsert {
        key: entry.id,
        version: Version::new(1, 2),
        value: "updated".into(),
    }];
    assert_eq!(early.try_recv(), Ok(expected));

    // A later subscription opens on the state as of now, not as of the first one.
    let mut late = reader.subscribe().unwrap();
    assert_eq!(
        late.try_recv(),
        Ok(Batch {
            incarnation: 1,
            base: vec![BaseDelta::Upsert {
                key: entry.id,
                version: Version::new(1, 2),
                value: "updated".into(),
            }],
            overlays: Vec::new(),
            settled: Vec::new(),
            covered: Vec::new(),
        })
    );

    let id = entry.id;
    entry.remove();
    let mut expected = Batch::new(1);
    expected.base = vec![BaseDelta::Remove {
        key: id,
        version: Version::new(1, 3),
    }];
    assert_eq!(early.try_recv(), Ok(expected.clone()));
    assert_eq!(late.try_recv(), Ok(expected));
    assert!(matches!(
        early.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    host.close();
}

#[test]
fn closing_the_consumer_clears_only_its_own_streams() {
    let host = Host::new();
    let provider = host.start("provider");
    let departing = host.start("departing");
    let staying = host.start("staying");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let departing_reader = collection.reader(&departing);
    let staying_reader = collection.reader(&staying);
    let writer = collection.writer(&provider).unwrap();

    let mut departing_stream = departing_reader.subscribe().unwrap();
    let mut staying_stream = staying_reader.subscribe().unwrap();
    assert!(matches!(
        departing_stream.try_recv(),
        Ok(batch) if batch.covered.is_empty() && batch.incarnation == 1
    ));
    assert!(matches!(
        staying_stream.try_recv(),
        Ok(batch) if batch.covered.is_empty() && batch.incarnation == 1
    ));

    writer.insert("row".into()).unwrap();
    assert!(matches!(
        departing_stream.try_recv(),
        Ok(batch) if batch.base.len() == 1
    ));
    assert!(matches!(
        staying_stream.try_recv(),
        Ok(batch) if batch.base.len() == 1
    ));

    host.close_instance(&departing).unwrap();
    assert!(matches!(
        departing_stream.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));

    // The surviving reader keeps its stream and keeps receiving mutations.
    writer.insert("another".into()).unwrap();
    assert!(matches!(
        staying_stream.try_recv(),
        Ok(batch) if batch.base.len() == 1
    ));
    assert!(!matches!(
        staying_stream.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    host.close();
}

#[test]
fn closing_the_provider_clears_every_stream() {
    let host = Host::new();
    let provider = host.start("provider");
    let first_consumer = host.start("first-consumer");
    let second_consumer = host.start("second-consumer");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let mut first = collection.reader(&first_consumer).subscribe().unwrap();
    let mut second = collection.reader(&second_consumer).subscribe().unwrap();
    for stream in [&mut first, &mut second] {
        assert!(matches!(
            stream.try_recv(),
            Ok(batch) if batch.covered.is_empty() && batch.incarnation == 1
        ));
    }

    host.close_instance(&provider).unwrap();
    for stream in [&mut first, &mut second] {
        assert!(matches!(
            stream.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
    host.close();
}

#[test]
fn a_dropped_subscription_is_pruned_without_disturbing_the_others() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let reader = collection.reader(&consumer);
    let writer = collection.writer(&provider).unwrap();

    let abandoned = reader.subscribe().unwrap();
    let mut kept = reader.subscribe().unwrap();
    assert!(matches!(
        kept.try_recv(),
        Ok(batch) if batch.covered.is_empty() && batch.incarnation == 1
    ));
    drop(abandoned);

    writer.insert("row".into()).unwrap();
    assert!(matches!(
        kept.try_recv(),
        Ok(batch) if batch.base.len() == 1
    ));
    host.close();
}

#[test]
fn reader_view_tracks_updates_and_single_batch_contributor_withdrawal() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let contributor = host.start("contributor");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let writer = collection.writer(&contributor).unwrap();
    let first = writer.insert("one".into()).unwrap();
    let second = writer.insert("two".into()).unwrap();

    let reader = collection.reader(&consumer);
    let mut binding = reader.view().unwrap();
    let view = binding.view().clone();
    assert_eq!(
        view.query(&clay_collection::Query::new()),
        vec![(first.id, "one".into()), (second.id, "two".into()),]
    );
    let demand = view.demand(DemandToken(json!({ "kind": "all" })));
    assert!(!view.is_covered(demand));
    assert_eq!(binding.pump(), 1);
    assert!(view.is_covered(demand));

    first.update("uno".into()).unwrap();
    assert_eq!(binding.pump(), 1);
    assert_eq!(view.visible(&first.id), Some("uno".into()));

    let events = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = events.clone();
    let listener = view.subscribe(move |event| sink.borrow_mut().push(event.clone()));
    host.close_instance(&contributor).unwrap();
    assert_eq!(binding.pump(), 1);
    assert_eq!(events.borrow().len(), 1);
    assert_eq!(
        events.borrow()[0].changes,
        vec![
            clay_collection::RowChange {
                key: first.id,
                before: Some("uno".into()),
                after: None
            },
            clay_collection::RowChange {
                key: second.id,
                before: Some("two".into()),
                after: None
            },
        ]
    );
    assert_eq!(view.base_len(), 0);
    drop(listener);
    host.close();
}

#[test]
fn demand_coverage_resolves_only_the_subscription_that_issued_it() {
    let host = Host::new();
    let provider = host.start("provider");
    let asking = host.start("asking");
    let quiet = host.start("quiet");
    let collection = HostedCollection::<String>::new(&host, &provider);
    collection
        .writer(&provider)
        .unwrap()
        .insert("row".into())
        .unwrap();

    let reader = collection.reader(&asking);
    let mut first = reader.view().unwrap();
    let mut same_reader = reader.clone().view().unwrap();
    let mut other_reader = collection.reader(&quiet).view().unwrap();
    let demand = first.view().demand(DemandToken(json!({ "kind": "all" })));

    assert_eq!(first.pump(), 1);
    assert!(first.view().is_covered(demand));
    for binding in [&mut same_reader, &mut other_reader] {
        assert_eq!(binding.pump(), 0);
        assert!(!binding.view().is_covered(demand));
        assert_eq!(binding.view().base_len(), 1);
    }
    let released = same_reader.view().demand(DemandToken(json!("other")));
    same_reader.view().release(released);
    assert_eq!(same_reader.pump(), 1);
    assert!(!same_reader.view().is_covered(released));
    host.close();
}

#[test]
fn revocation_discards_queued_rows_and_clears_views_without_forging_an_epoch() {
    for close_provider in [false, true] {
        let host = Host::new();
        let provider = host.start("provider");
        let consumer = host.start("consumer");
        let sibling = host.start("sibling");
        let collection = HostedCollection::<String>::new(&host, &provider);
        let entry = collection
            .writer(&provider)
            .unwrap()
            .insert("initial".into())
            .unwrap();
        let reader = collection.reader(&consumer);
        let mut binding = reader.view().unwrap();
        let mut surviving = collection.reader(&sibling).view().unwrap();
        let view = binding.view().clone();
        let epoch = view.incarnation();
        let demand = view.demand(DemandToken(json!(null)));
        assert_eq!(binding.pump(), 1);
        entry.update("queued".into()).unwrap();
        let mut queued = reader.subscribe().unwrap();
        host.close_instance(if close_provider { &provider } else { &consumer })
            .unwrap();
        assert_eq!(
            queued.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        );
        assert_eq!(binding.pump(), 0);
        assert!(view.is_closed());
        assert_eq!(view.incarnation(), epoch);
        assert_eq!(view.base_len(), 0);
        assert!(!view.is_covered(demand));
        assert_eq!(binding.pump(), 0);
        assert!(matches!(reader.subscribe(), Err(AccessError::Revoked)));
        surviving.pump();
        if close_provider {
            assert!(surviving.view().is_closed());
        } else {
            assert_eq!(surviving.view().visible(&entry.id()), Some("queued".into()));
            assert_eq!(
                collection.state.lock().unwrap().replica.incarnation(),
                epoch
            );
        }
        host.close();
    }
}

#[tokio::test]
async fn asynchronous_pump_clears_on_disconnect_and_drop_prunes_immediately() {
    use std::future::Future;
    use std::task::Poll;
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let entry = collection
        .writer(&provider)
        .unwrap()
        .insert("row".into())
        .unwrap();
    let reader = collection.reader(&consumer);
    let mut binding = reader.view().unwrap();
    let view = binding.view().clone();
    let mut advance = Box::pin(binding.advance());
    std::future::poll_fn(|cx| {
        assert!(advance.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    host.close_instance(&consumer).unwrap();
    assert!(!advance.await);
    assert!(view.is_closed());
    assert_eq!(view.visible(&entry.id()), None);

    let another = host.start("another");
    let reader = collection.reader(&another);
    let binding = reader.view().unwrap();
    let retained = binding.view().clone();
    drop(binding);
    assert!(retained.is_closed());
    assert!(collection.state.lock().unwrap().subscriptions[&reader.reader_id].is_empty());
    let mut replacement = reader.view().unwrap();
    entry.update("updated".into()).unwrap();
    assert!(replacement.advance().await);
    assert_eq!(
        replacement.view().visible(&entry.id()),
        Some("updated".into())
    );
    host.close();
}

// ── Lifecycle model property: random op sequences against an in-memory model ─

/// One slot in the model. `Instance` handles stay valid as values after closure, so the model
/// retains them alongside the terminal receiver captured while the incarnation was live.
struct SlotModel {
    instance: Instance,
    ended: watch::Receiver<Option<CloseReason>>,
    reason: Option<CloseReason>,
    effects: Vec<u64>,
    providers: BTreeSet<u8>,
    components: Vec<ComponentRef>,
}

impl SlotModel {
    fn live(&self) -> bool {
        self.reason.is_none()
    }
}

#[derive(Debug, Clone, Copy)]
enum HostOp {
    Start(u8),
    /// Attach an effect recording (slot, sequence) into the shared cleanup log.
    Own(u8),
    Bind {
        consumer: u8,
        provider: u8,
    },
    Close(u8),
    Publish(u8),
    /// Assert a second host denies every authority over this slot's instance.
    ForeignCheck(u8),
    CloseHost,
}

fn host_op() -> impl proptest::strategy::Strategy<Value = HostOp> {
    use proptest::prelude::*;
    prop_oneof![
        3 => (0u8..6).prop_map(HostOp::Start),
        3 => (0u8..6).prop_map(HostOp::Own),
        2 => (0u8..6, 0u8..6).prop_map(|(consumer, provider)| HostOp::Bind { consumer, provider }),
        4 => (0u8..6).prop_map(HostOp::Close),
        2 => (0u8..6).prop_map(HostOp::Publish),
        1 => (0u8..6).prop_map(HostOp::ForeignCheck),
        1 => Just(HostOp::CloseHost),
    ]
}

/// True if `provider` can already reach `consumer` through provider edges, so binding
/// consumer→provider would close a cycle (an invariant failure the model must not request).
fn would_cycle(slots: &BTreeMap<u8, SlotModel>, consumer: u8, provider: u8) -> bool {
    let mut frontier = vec![provider];
    let mut seen = BTreeSet::new();
    while let Some(id) = frontier.pop() {
        if id == consumer {
            return true;
        }
        if seen.insert(id)
            && let Some(slot) = slots.get(&id)
        {
            frontier.extend(slot.providers.iter().copied());
        }
    }
    false
}

/// Slots closed by closing `root`, in dependency order: the requested slot first with
/// `requested`, every transitively dependent live slot with `dependent`.
fn cascade(
    slots: &BTreeMap<u8, SlotModel>,
    root: u8,
    requested: CloseReason,
    dependent: CloseReason,
) -> Vec<(u8, CloseReason)> {
    let mut closed = vec![(root, requested)];
    let mut frontier = vec![root];
    while let Some(provider) = frontier.pop() {
        for (&id, slot) in slots {
            if slot.live()
                && !closed.iter().any(|(closed, _)| *closed == id)
                && slot.providers.contains(&provider)
            {
                closed.push((id, dependent));
                frontier.push(id);
            }
        }
    }
    closed
}

/// The whole-model invariant check, run after every op:
/// - liveness in the host matches the model exactly,
/// - terminal receivers observe exactly the modelled reason (and live slots observe nothing),
/// - component references are live exactly while their supplier is,
/// - a dependent's cleanups always precede its provider's in the shared log,
/// - each incarnation's own effects ran in reverse acquisition order.
fn verify_model(
    host: &Host,
    slots: &BTreeMap<u8, SlotModel>,
    edges: &[(u64, u64)],
    log: &[(u64, u64)],
) {
    for slot in slots.values() {
        let expected = if slot.live() {
            Ok(())
        } else {
            Err(AccessError::Revoked)
        };
        assert_eq!(host.check(&slot.instance), expected);
        assert_eq!(*slot.ended.borrow(), slot.reason);
        for component in &slot.components {
            assert_eq!(host.check_component(*component), expected);
        }
        let ran: Vec<u64> = log
            .iter()
            .filter(|(owner, _)| *owner == slot.instance.id)
            .map(|(_, sequence)| *sequence)
            .collect();
        if slot.live() {
            assert!(ran.is_empty(), "live instance ran cleanup effects");
        } else {
            let mut reversed = slot.effects.clone();
            reversed.reverse();
            assert_eq!(ran, reversed, "effects must run LIFO per incarnation");
        }
    }
    for &(consumer, provider) in edges {
        let consumer_last = log.iter().rposition(|(owner, _)| *owner == consumer);
        let provider_first = log.iter().position(|(owner, _)| *owner == provider);
        if let (Some(consumer_last), Some(provider_first)) = (consumer_last, provider_first) {
            assert!(
                consumer_last < provider_first,
                "dependent cleanup overtook its provider"
            );
        }
    }
}

fn run_host_script(ops: Vec<HostOp>) {
    let host = Host::new();
    let foreign = Host::new();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut slots: BTreeMap<u8, SlotModel> = BTreeMap::new();
    let mut edges: Vec<(u64, u64)> = Vec::new();
    let mut sequence = 0u64;
    let mut host_closed = false;
    for op in ops {
        match op {
            HostOp::Start(slot) => {
                if host_closed {
                    assert_eq!(host.try_start("late"), Err(AccessError::Revoked));
                } else if let std::collections::btree_map::Entry::Vacant(entry) = slots.entry(slot)
                {
                    let instance = host.start(format!("slot-{slot}"));
                    let ended = host.observe_instance(&instance).unwrap();
                    entry.insert(SlotModel {
                        instance,
                        ended,
                        reason: None,
                        effects: Vec::new(),
                        providers: BTreeSet::new(),
                        components: Vec::new(),
                    });
                }
            }
            HostOp::Own(slot) => {
                if let Some(model) = slots.get(&slot) {
                    if model.live() {
                        sequence += 1;
                        let recorded = log.clone();
                        let owner = model.instance.id;
                        host.own(
                            &model.instance,
                            Box::new(move || {
                                recorded.lock().unwrap().push((owner, sequence));
                            }),
                        )
                        .unwrap();
                        slots.get_mut(&slot).unwrap().effects.push(sequence);
                    } else {
                        assert_eq!(
                            host.own(&model.instance, Box::new(|| {})),
                            Err(AccessError::Revoked)
                        );
                    }
                }
            }
            HostOp::Bind { consumer, provider } => {
                let (Some(consumer_model), Some(provider_model)) =
                    (slots.get(&consumer), slots.get(&provider))
                else {
                    continue;
                };
                if !consumer_model.live() || !provider_model.live() {
                    assert_eq!(
                        host.bind_dependency(&consumer_model.instance, &provider_model.instance),
                        Err(AccessError::Revoked)
                    );
                } else if consumer == provider {
                    // A self-dependency is a no-op, not an edge.
                    host.bind_dependency(&consumer_model.instance, &provider_model.instance)
                        .unwrap();
                } else if !would_cycle(&slots, consumer, provider) {
                    host.bind_dependency(&consumer_model.instance, &provider_model.instance)
                        .unwrap();
                    edges.push((consumer_model.instance.id, provider_model.instance.id));
                    slots.get_mut(&consumer).unwrap().providers.insert(provider);
                }
            }
            HostOp::Close(slot) => {
                if let Some(model) = slots.get(&slot) {
                    // Closing is once-only in effect but idempotent in result.
                    host.close_instance(&model.instance).unwrap();
                    if model.live() {
                        for (id, reason) in cascade(
                            &slots,
                            slot,
                            CloseReason::Requested,
                            CloseReason::DependencyWithdrawn,
                        ) {
                            slots.get_mut(&id).unwrap().reason = Some(reason);
                        }
                    }
                }
            }
            HostOp::Publish(slot) => {
                if let Some(model) = slots.get(&slot) {
                    if model.live() {
                        let component = host.publish_component(&model.instance).unwrap();
                        assert_eq!(host.check_component(component), Ok(()));
                        slots.get_mut(&slot).unwrap().components.push(component);
                    } else {
                        assert_eq!(
                            host.publish_component(&model.instance),
                            Err(AccessError::Revoked)
                        );
                    }
                }
            }
            HostOp::ForeignCheck(slot) => {
                if let Some(model) = slots.get(&slot) {
                    assert_eq!(foreign.check(&model.instance), Err(AccessError::Denied));
                    assert_eq!(
                        foreign.close_instance(&model.instance),
                        Err(AccessError::Denied)
                    );
                }
            }
            HostOp::CloseHost => {
                if !host_closed {
                    host.close();
                    host_closed = true;
                    for slot in slots.values_mut() {
                        if slot.live() {
                            slot.reason = Some(CloseReason::HostShutdown);
                        }
                    }
                }
            }
        }
        verify_model(&host, &slots, &edges, &log.lock().unwrap().clone());
    }
    if !host_closed {
        host.close();
        for slot in slots.values_mut() {
            if slot.live() {
                slot.reason = Some(CloseReason::HostShutdown);
            }
        }
    }
    verify_model(&host, &slots, &edges, &log.lock().unwrap().clone());
    // Quiescence drains every piece of lifecycle bookkeeping.
    let state = host.inner.lock().unwrap();
    assert!(state.instances.is_empty());
    assert!(state.ending.is_empty());
    assert!(state.closing.is_empty());
    assert!(state.dependencies.is_empty());
    assert!(state.components.is_empty());
    assert!(state.mounts.is_empty());
    drop(state);
    foreign.close();
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 64,
        ..Default::default()
    })]
    #[test]
    fn lifecycle_cascades_match_the_dependency_model(
        ops in proptest::collection::vec(host_op(), 1..80)
    ) {
        run_host_script(ops);
    }
}

// ── Collection contribution property: streams and snapshots track the model ──

#[derive(Debug, Clone, Copy)]
enum CollectionOp {
    Insert {
        contributor: u8,
        value: u8,
    },
    /// Operate on a previously inserted entry by index into the entry table (live or stale).
    Update {
        entry: u8,
        value: u8,
    },
    Release {
        entry: u8,
    },
    CloseContributor(u8),
    CloseConsumer(u8),
    CloseProvider,
    Snapshot {
        consumer: u8,
    },
}

fn collection_op() -> impl proptest::strategy::Strategy<Value = CollectionOp> {
    use proptest::prelude::*;
    prop_oneof![
        4 => (0u8..3, any::<u8>()).prop_map(|(contributor, value)| CollectionOp::Insert { contributor, value }),
        3 => (any::<u8>(), any::<u8>()).prop_map(|(entry, value)| CollectionOp::Update { entry, value }),
        2 => (any::<u8>()).prop_map(|entry| CollectionOp::Release { entry }),
        1 => (0u8..3).prop_map(CollectionOp::CloseContributor),
        1 => (0u8..2).prop_map(CollectionOp::CloseConsumer),
        1 => Just(CollectionOp::CloseProvider),
        2 => (0u8..2).prop_map(|consumer| CollectionOp::Snapshot { consumer }),
    ]
}

/// A consumer's local fold of its subscription stream: the oracle for "the stream carries every
/// mutation exactly once". An incarnation bump resets the fold, mirroring the authority's
/// wholesale reset on provider closure.
struct StreamFold {
    subscription: CollectionSubscription<String>,
    incarnation: u64,
    rows: BTreeMap<u64, String>,
    closed: bool,
}

impl StreamFold {
    fn drain(&mut self) {
        loop {
            match self.subscription.try_recv() {
                Ok(batch) => {
                    if batch.incarnation != self.incarnation {
                        self.incarnation = batch.incarnation;
                        self.rows.clear();
                    }
                    for delta in batch.base {
                        match delta {
                            BaseDelta::Upsert { key, value, .. } => {
                                self.rows.insert(key, value);
                            }
                            BaseDelta::Remove { key, .. } => {
                                self.rows.remove(&key);
                            }
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => return,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.closed = true;
                    return;
                }
            }
        }
    }
}

fn run_collection_script(ops: Vec<CollectionOp>) {
    let host = Host::new();
    let provider = host.start("provider");
    let contributors: Vec<Instance> = (0..3)
        .map(|i| host.start(format!("contributor-{i}")))
        .collect();
    let consumers: Vec<Instance> = (0..2)
        .map(|i| host.start(format!("consumer-{i}")))
        .collect();
    let collection = HostedCollection::<String>::new(&host, &provider);
    let readers: Vec<CollectionReader<String>> = consumers
        .iter()
        .map(|consumer| collection.reader(consumer))
        .collect();
    let mut folds: Vec<Option<StreamFold>> = readers
        .iter()
        .map(|reader| {
            let mut fold = StreamFold {
                subscription: reader.subscribe().unwrap(),
                incarnation: 0,
                rows: BTreeMap::new(),
                closed: false,
            };
            fold.drain();
            Some(fold)
        })
        .collect();
    let mut writers: BTreeMap<u8, CollectionWriter<String>> = BTreeMap::new();
    let mut entries: Vec<Entry<String>> = Vec::new();
    // Model: entry id → value, plus per-entry liveness (contributor live, not released,
    // provider live) for predicting update outcomes.
    let mut model: BTreeMap<u64, String> = BTreeMap::new();
    let mut live_entries: BTreeMap<u64, (u8, u64)> = BTreeMap::new(); // id → (contributor, generation)
    let mut contributors_live = [true; 3];
    let mut consumers_live = [true; 2];
    let mut provider_live = true;
    let mut generation = 0u64;

    let verify = |model: &BTreeMap<u64, String>,
                  folds: &mut [Option<StreamFold>],
                  readers: &[CollectionReader<String>],
                  consumers_live: &[bool; 2],
                  provider_live: bool| {
        for (index, fold) in folds.iter_mut().enumerate() {
            let Some(fold) = fold else { continue };
            fold.drain();
            let stream_live = consumers_live[index] && provider_live;
            assert_eq!(
                fold.closed, !stream_live,
                "stream liveness diverged from model"
            );
            if stream_live {
                assert_eq!(fold.rows, *model, "stream fold diverged from model");
                assert_eq!(readers[index].snapshot().unwrap(), *model);
            } else {
                assert_eq!(readers[index].snapshot(), Err(AccessError::Revoked));
            }
        }
    };

    for op in ops {
        match op {
            CollectionOp::Insert { contributor, value } => {
                if !contributors_live[contributor as usize] || !provider_live {
                    continue;
                }
                let writer = writers.entry(contributor).or_insert_with(|| {
                    collection
                        .writer(&contributors[contributor as usize])
                        .unwrap()
                });
                let entry = writer.insert(format!("v{value}")).unwrap();
                model.insert(entry.id(), format!("v{value}"));
                generation += 1;
                live_entries.insert(entry.id(), (contributor, generation));
                entries.push(entry);
            }
            CollectionOp::Update { entry, value } => {
                let Some(handle) = entries.get(entry as usize % entries.len().max(1)) else {
                    continue;
                };
                let current = live_entries.get(&handle.id());
                match current {
                    Some((contributor, _))
                        if contributors_live[*contributor as usize] && provider_live =>
                    {
                        handle.update(format!("v{value}")).unwrap();
                        model.insert(handle.id(), format!("v{value}"));
                    }
                    _ => {
                        assert_eq!(
                            handle.update(format!("v{value}")),
                            Err(AccessError::Revoked)
                        );
                    }
                }
            }
            CollectionOp::Release { entry } => {
                let Some(handle) = entries.get(entry as usize % entries.len().max(1)) else {
                    continue;
                };
                // Withdrawal is idempotent and needs no live authority: always allowed.
                handle.release();
                if live_entries.remove(&handle.id()).is_some() {
                    model.remove(&handle.id());
                }
            }
            CollectionOp::CloseContributor(contributor) => {
                if !contributors_live[contributor as usize] {
                    continue;
                }
                contributors_live[contributor as usize] = false;
                host.close_instance(&contributors[contributor as usize])
                    .unwrap();
                // Contributor closure withdraws every live entry it owns in one batch.
                live_entries.retain(|id, (owner, _)| {
                    if *owner == contributor {
                        model.remove(id);
                        false
                    } else {
                        true
                    }
                });
            }
            CollectionOp::CloseConsumer(consumer) => {
                if !consumers_live[consumer as usize] {
                    continue;
                }
                consumers_live[consumer as usize] = false;
                host.close_instance(&consumers[consumer as usize]).unwrap();
            }
            CollectionOp::CloseProvider => {
                if !provider_live {
                    continue;
                }
                provider_live = false;
                host.close_instance(&provider).unwrap();
                // Provider closure cascades to every dependent (contributors via writer
                // bindings, consumers via reader bindings) and resets the collection wholesale.
                contributors_live = [false; 3];
                consumers_live = [false; 2];
                model.clear();
                live_entries.clear();
            }
            CollectionOp::Snapshot { consumer } => {
                let consumer = consumer as usize;
                match readers[consumer].snapshot() {
                    Ok(snapshot) => {
                        assert!(consumers_live[consumer] && provider_live);
                        assert_eq!(snapshot, model);
                    }
                    Err(AccessError::Revoked) => {
                        assert!(!consumers_live[consumer] || !provider_live);
                    }
                    other => panic!("unexpected snapshot outcome: {other:?}"),
                }
            }
        }
        verify(&model, &mut folds, &readers, &consumers_live, provider_live);
    }
    verify(&model, &mut folds, &readers, &consumers_live, provider_live);
    host.close();
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 48,
        ..Default::default()
    })]
    #[test]
    fn contributions_and_streams_track_the_model_under_random_lifecycle_ops(
        ops in proptest::collection::vec(collection_op(), 1..80)
    ) {
        run_collection_script(ops);
    }
}

// ── Mount authority: a mount is only as live as both of its endpoints ────────

#[test]
fn mounting_requires_a_live_owner_and_a_native_live_component() {
    let host = Host::new();
    let foreign_host = Host::new();
    let owner = host.start("owner");
    let supplier = host.start("supplier");
    let component = host.publish_component(&supplier).unwrap();

    let foreign_component = foreign_host
        .publish_component(&foreign_host.start("foreign"))
        .unwrap();
    // A ComponentRef minted by another host is denied outright, never treated as withdrawn.
    assert!(matches!(
        host.mount(&owner, foreign_component),
        Err(AccessError::Denied)
    ));
    assert_eq!(
        host.check_component(foreign_component),
        Err(AccessError::Denied)
    );

    let mount = host.mount(&owner, component).unwrap();
    assert!(mount.is_live());

    host.close_instance(&owner).unwrap();
    assert!(matches!(
        host.mount(&owner, component),
        Err(AccessError::Revoked)
    ));
    // Owner teardown ends its mounts; the supplier's component stays published.
    assert!(!mount.is_live());
    assert_eq!(host.check_component(component), Ok(()));

    host.close_instance(&supplier).unwrap();
    assert_eq!(host.check_component(component), Err(AccessError::Revoked));
    let consumer = host.start("consumer");
    assert!(matches!(
        host.mount(&consumer, component),
        Err(AccessError::Revoked)
    ));
    foreign_host.close();
    host.close();
}

#[test]
fn mount_liveness_follows_supplier_teardown_and_mount_drop() {
    let host = Host::new();
    let owner = host.start("owner");
    let supplier = host.start("supplier");
    let component = host.publish_component(&supplier).unwrap();
    let mount = host.mount(&owner, component).unwrap();
    let mut liveness = mount.subscribe();
    assert!(*liveness.borrow_and_update());

    host.close_instance(&supplier).unwrap();
    assert!(!*liveness.borrow_and_update());
    assert!(!mount.is_live());

    // Dropping the mount record unmounts: a later supplier teardown has no subscriber left to
    // flip, so liveness must already report dead before the teardown runs.
    let second_supplier = host.start("second-supplier");
    let second = host.publish_component(&second_supplier).unwrap();
    let second_mount = host.mount(&host.start("second-owner"), second).unwrap();
    let mut second_liveness = second_mount.subscribe();
    assert!(*second_liveness.borrow_and_update());
    drop(second_mount);
    assert!(!*second_liveness.borrow_and_update());
    host.close_instance(&second_supplier).unwrap();
    host.close();
}

#[test]
fn component_lifetimes_track_publication_and_supplier_withdrawal() {
    let host = Host::new();
    let supplier = host.start("supplier");
    let other = host.start("other");
    let mut lifetimes = host.component_lifetimes();
    assert!(lifetimes.borrow_and_update().is_empty());

    let first = host.publish_component(&supplier).unwrap();
    let second = host.publish_component(&supplier).unwrap();
    let unrelated = host.publish_component(&other).unwrap();
    assert_eq!(
        *lifetimes.borrow_and_update(),
        BTreeSet::from([first, second, unrelated])
    );

    // Supplier teardown withdraws exactly its own components, no more.
    host.close_instance(&supplier).unwrap();
    assert_eq!(*lifetimes.borrow_and_update(), BTreeSet::from([unrelated]));
    host.close();
    assert!(lifetimes.borrow_and_update().is_empty());
}
