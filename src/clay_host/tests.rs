use super::*;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
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
        assert!(state.tearing_down.is_empty());
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

#[tokio::test]
async fn cancelling_async_activation_withdraws_owned_effects() {
    use std::future::Future;
    use std::task::Poll;

    let host = Host::new();
    let root = host.start("root");
    let host_for_activation = host.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let mut activation = Box::pin(host.initialize("async", move |instance| async move {
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
    let _consumer_binding = host.bind(&consumer, &provider).unwrap();
    let provider_ended = host.observe_instance(&provider).unwrap();
    let consumer_ended = host.observe_instance(&consumer).unwrap();
    let mut changes = host.changes();
    let sibling = host.start("sibling");
    let _sibling_binding = host.bind(&sibling, &provider).unwrap();
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
    let _binding = host.bind(&consumer, &provider).unwrap();
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
    let mut activation = Box::pin(host.initialize("pending", |instance| async move {
        assert!(ready.send(instance).is_ok());
        std::future::pending::<Result<(), AccessError>>().await
    }));
    std::future::poll_fn(|context| {
        assert!(activation.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    host.close_instance(&started.await.unwrap()).unwrap();
    assert_eq!(activation.await.map(drop), Err(AccessError::Revoked));
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
    let mut bindings = Vec::new();
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
                if consumer == provider {
                    // Binding an incarnation to itself is an invariant failure, never requested.
                } else if !consumer_model.live() || !provider_model.live() {
                    assert!(matches!(
                        host.bind(&consumer_model.instance, &provider_model.instance),
                        Err(AccessError::Revoked)
                    ));
                } else if !would_cycle(&slots, consumer, provider) {
                    bindings.push(
                        host.bind(&consumer_model.instance, &provider_model.instance)
                            .unwrap(),
                    );
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
    assert!(state.tearing_down.is_empty());
    assert!(state.dependencies.is_empty());
    assert!(state.components.is_empty());
    assert!(state.mounts.is_empty());
    drop(state);
    // Teardown already removed every edge; dropping the stale bindings must not resurrect one.
    drop(bindings);
    assert!(host.inner.lock().unwrap().dependencies.is_empty());
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

// ── Staged lifecycle: initialization, outward hooks, and strong bindings ─────

type Events = Arc<Mutex<Vec<Lifecycle>>>;

fn record_outward(host: &Host, instance: &Instance) -> (Lifecycle, Events) {
    let events = Events::default();
    let recorded = events.clone();
    let current = host
        .on_outward(instance, move |lifecycle| {
            recorded.lock().unwrap().push(lifecycle)
        })
        .unwrap();
    (current, events)
}

#[tokio::test]
async fn committed_initialization_exposes_then_withdraws_outward_effects() {
    let host = Host::new();
    let mut hooked = None;
    let (instance, ()) = host
        .initialize("staged", |instance| {
            let (current, events) = record_outward(&host, &instance);
            assert_eq!(current, Lifecycle::Initializing);
            assert_eq!(host.lifecycle(&instance), Lifecycle::Initializing);
            hooked = Some(events);
            async { Ok::<_, AccessError>(()) }
        })
        .await
        .unwrap();
    let events = hooked.unwrap();
    assert_eq!(host.lifecycle(&instance), Lifecycle::Active);
    assert_eq!(*events.lock().unwrap(), [Lifecycle::Active]);
    let mut lifecycle = host.observe_lifecycle(&instance).unwrap();
    assert_eq!(*lifecycle.borrow_and_update(), Lifecycle::Active);

    assert!(host.begin_close(&instance).unwrap());
    assert_eq!(
        *events.lock().unwrap(),
        [Lifecycle::Active, Lifecycle::Closing]
    );
    assert_eq!(*lifecycle.borrow_and_update(), Lifecycle::Closing);
    // Exactly one caller drives each close.
    assert!(!host.begin_close(&instance).unwrap());
    // A closing incarnation keeps its own authority but acquires nothing outward.
    assert_eq!(host.check(&instance), Ok(()));
    host.own(&instance, Box::new(|| {})).unwrap();
    assert_eq!(host.publish_component(&instance), Err(AccessError::Revoked));
    let peer = host.start("peer");
    assert!(matches!(
        host.bind(&peer, &instance),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        host.bind(&instance, &peer),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        host.on_outward(&instance, |_| {}),
        Err(AccessError::Revoked)
    ));

    host.close_instance(&instance).unwrap();
    assert_eq!(*lifecycle.borrow_and_update(), Lifecycle::Closed);
    assert_eq!(host.lifecycle(&instance), Lifecycle::Closed);
    assert_eq!(
        *events.lock().unwrap(),
        [Lifecycle::Active, Lifecycle::Closing]
    );
    host.close();
}

#[tokio::test]
async fn failed_initialization_never_reports_active_and_unwinds_owned_effects() {
    let host = Host::new();
    let unwound = Arc::new(Mutex::new(false));
    let mut observed = None;
    let result = host
        .initialize("failing", |instance| {
            let (_, events) = record_outward(&host, &instance);
            let unwound = unwound.clone();
            host.own(&instance, Box::new(move || *unwound.lock().unwrap() = true))
                .unwrap();
            observed = Some((instance, events));
            async { Err::<(), _>(AccessError::Denied) }
        })
        .await;
    assert_eq!(result.map(drop), Err(AccessError::Denied));
    let (instance, events) = observed.unwrap();
    assert_eq!(*events.lock().unwrap(), [Lifecycle::Closing]);
    assert!(*unwound.lock().unwrap());
    assert_eq!(host.lifecycle(&instance), Lifecycle::Closed);
    assert!(host.inner.lock().unwrap().instances.is_empty());
    host.close();
}

#[test]
fn initialization_cannot_commit_once_closing() {
    let host = Host::new();
    let instance = host.create("late").unwrap();
    assert_eq!(host.lifecycle(&instance), Lifecycle::Created);
    host.begin_initialization(&instance).unwrap();
    let (_, events) = record_outward(&host, &instance);
    assert!(host.begin_close(&instance).unwrap());
    assert_eq!(
        host.commit_initialization(&instance),
        Err(AccessError::Revoked)
    );
    assert_eq!(*events.lock().unwrap(), [Lifecycle::Closing]);
    host.close_instance(&instance).unwrap();
    assert_eq!(
        host.commit_initialization(&instance),
        Err(AccessError::Revoked)
    );
    host.close();
}

#[test]
#[should_panic(expected = "initialization committed twice")]
fn committing_initialization_twice_is_an_invariant_failure() {
    let host = Host::new();
    let instance = host.create("twice").unwrap();
    host.begin_initialization(&instance).unwrap();
    host.commit_initialization(&instance).unwrap();
    let _ = host.commit_initialization(&instance);
}

#[test]
fn a_hook_registered_after_commit_is_told_only_of_withdrawal() {
    let host = Host::new();
    let instance = host.start("active");
    let (current, events) = record_outward(&host, &instance);
    assert_eq!(current, Lifecycle::Active);
    assert!(events.lock().unwrap().is_empty());
    host.close_instance(&instance).unwrap();
    assert_eq!(*events.lock().unwrap(), [Lifecycle::Closing]);
    host.close();
}

#[test]
fn bindings_retain_their_provider_until_the_last_one_drops() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let first = host.bind(&consumer, &provider).unwrap();
    let second = host.bind(&consumer, &provider).unwrap();
    assert!(host.is_bound(&provider));
    assert!(!host.is_bound(&consumer));
    drop(first);
    assert!(host.is_bound(&provider));
    drop(second);
    assert!(!host.is_bound(&provider));
    assert!(host.inner.lock().unwrap().dependencies.is_empty());
    // Without an edge, closing the provider no longer reaches the consumer.
    host.close_instance(&provider).unwrap();
    assert_eq!(host.check(&consumer), Ok(()));
    host.close();
}

#[test]
fn a_stale_binding_cannot_release_its_replacements_edge() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let consumer_ended = host.observe_instance(&consumer).unwrap();
    let stale = host.bind(&consumer, &provider).unwrap();
    // Closing a bound provider closes its consumer first and removes the edge.
    host.close_instance(&provider).unwrap();
    assert_eq!(
        *consumer_ended.borrow(),
        Some(CloseReason::DependencyWithdrawn)
    );
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let live = host.bind(&consumer, &provider).unwrap();
    drop(stale);
    assert!(host.is_bound(&provider));
    drop(live);
    assert!(!host.is_bound(&provider));
    host.close();
}

#[test]
#[should_panic(expected = "a micro cannot bind itself")]
fn binding_an_incarnation_to_itself_is_an_invariant_failure() {
    let host = Host::new();
    let instance = host.start("self");
    let _binding = host.bind(&instance, &instance);
}

#[test]
#[should_panic(expected = "cyclic micro dependency")]
fn closing_a_binding_cycle_is_an_invariant_failure() {
    let host = Host::new();
    let first = host.start("first");
    let second = host.start("second");
    let _forward = host.bind(&first, &second).unwrap();
    let _backward = host.bind(&second, &first);
}
