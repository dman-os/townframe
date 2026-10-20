use super::*;
use clay_compose::micros::composition::{Enablement, MockConfiguration, StaticConfiguration};
use clay_compose::micros::{ActivationError, MicroFactory, MicroId, MicroProvision, Provision};
use clay_host::{AccessError, CloseReason};
use std::sync::{Arc, Mutex};
use std::{future::Future, pin::Pin, task::Poll};

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn configured(provider: &str) -> ConfiguredMicro {
    ConfiguredMicro {
        provider: MicroId::new(provider),
        enablement: Enablement::Enabled,
        bindings: BTreeMap::new(),
    }
}

struct TrackedFuture {
    counts: Rc<Cell<(usize, usize)>>,
    future: Pin<Box<dyn Future<Output = ()>>>,
}

impl Future for TrackedFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> Poll<()> {
        let (polled, dropped) = self.counts.get();
        self.counts.set((polled + 1, dropped));
        self.future.as_mut().poll(context)
    }
}

impl Drop for TrackedFuture {
    fn drop(&mut self) {
        let (polled, dropped) = self.counts.get();
        self.counts.set((polled, dropped + 1));
    }
}

#[tokio::test]
async fn owned_local_watch_is_event_driven_without_a_surface_and_survives_driver_handoff() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut runtime = ApplicationRuntime::new();
    let counts = Rc::new(Cell::new((0, 0)));
    let seen = Rc::new(Cell::new(0));
    let retained = Rc::new(RefCell::new(None));
    let (sender, updates) = tokio::sync::watch::channel(0);
    let saved = retained.clone();
    let counted = counts.clone();
    let observed = seen.clone();
    runtime
        .register_provider(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("watch"),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                let mut updates = updates.clone();
                let observed = observed.clone();
                activation.own_future(TrackedFuture {
                    counts: counted.clone(),
                    future: Box::pin(async move {
                        loop {
                            updates.changed().await.unwrap();
                            observed.set(*updates.borrow_and_update());
                        }
                    }),
                })?;
                *saved.borrow_mut() = Some(activation);
                Ok(())
            },
        ))
        .unwrap();
    runtime
        .configure(&BTreeMap::from([("watch".into(), configured("watch"))]))
        .await
        .unwrap();
    assert_eq!(counts.get(), (0, 0));
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut observer = Box::pin(runtime.observe());
    assert!(observer.as_mut().poll(&mut context).is_pending());
    let before = wakes.0.load(Ordering::Relaxed);
    sender.send_replace(1);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert!(observer.as_mut().poll(&mut context).is_pending());
    assert_eq!(seen.get(), 1);
    drop(observer);
    assert_eq!(counts.get().1, 0);
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    sender.send_replace(2);
    assert_pending(observer.as_mut()).await;
    assert_eq!(seen.get(), 2);
    std::thread::spawn({
        let host = runtime.host();
        move || host.close()
    })
    .join()
    .unwrap();
    observer.await;
    let polled = counts.get().0;
    assert_eq!(counts.get(), (polled, 1));
    let rejected = Rc::new(Cell::new((0, 0)));
    assert_eq!(
        retained
            .borrow()
            .as_ref()
            .unwrap()
            .own_future(TrackedFuture {
                counts: rejected.clone(),
                future: Box::pin(std::future::pending()),
            }),
        Err(AccessError::Revoked)
    );
    assert_eq!(rejected.get(), (0, 1));
}

#[tokio::test]
async fn owned_work_can_release_activation_and_disable_drops_only_its_incarnation() {
    let mut runtime = ApplicationRuntime::new();
    let counts = Rc::new(RefCell::new(BTreeMap::new()));
    let counted = counts.clone();
    runtime
        .register_provider(MicroFactory::new_async(
            MicroDescriptor {
                id: MicroId::new("worker"),
                version: 1,
                requirements: vec![],
                provisions: vec![declaration()],
            },
            move |activation| {
                let counted = counted.clone();
                async move {
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    activation.own_future(async move { sender.send(()).unwrap() })?;
                    receiver.await.unwrap();
                    let counts = Rc::new(Cell::new((0, 0)));
                    assert!(
                        counted
                            .borrow_mut()
                            .insert(activation.instance().name.clone(), counts.clone())
                            .is_none()
                    );
                    activation.own_future(TrackedFuture {
                        counts,
                        future: Box::pin(std::future::pending()),
                    })?;
                    activation.publish(Facility(activation.instance()));
                    Ok(())
                }
            },
        ))
        .unwrap();
    let source = MockConfiguration::new(BTreeMap::from([
        ("first".into(), configured("worker")),
        ("second".into(), configured("worker")),
    ]));
    runtime
        .set_configuration_source(source.clone())
        .await
        .unwrap();
    let first = runtime.output::<Facility>("first").unwrap().0;
    let second = runtime.output::<Facility>("second").unwrap().0;
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    assert_eq!(counts.borrow()["first"].get().1, 0);
    assert_eq!(counts.borrow()["second"].get().1, 0);
    source
        .set_enablement("first", Enablement::Disabled)
        .unwrap();
    assert_pending(observer.as_mut()).await;
    assert_eq!(runtime.host().check(&first), Err(AccessError::Revoked));
    assert_eq!(runtime.host().check(&second), Ok(()));
    assert_eq!(counts.borrow()["first"].get().1, 1);
    assert_eq!(counts.borrow()["second"].get().1, 0);
    runtime.host().close();
    observer.await;
    assert_eq!(counts.borrow()["second"].get().1, 1);
}

#[tokio::test]
async fn failed_activation_drops_registered_work_before_its_first_poll() {
    let mut runtime = ApplicationRuntime::new();
    let counts = Rc::new(Cell::new((0, 0)));
    let counted = counts.clone();
    runtime
        .register_provider(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("failure"),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                activation.own_future(TrackedFuture {
                    counts: counted.clone(),
                    future: Box::pin(std::future::pending()),
                })?;
                Err(AccessError::Denied)
            },
        ))
        .unwrap();
    assert_eq!(
        runtime
            .configure(&BTreeMap::from([("failure".into(), configured("failure"))]))
            .await,
        Ok(BTreeMap::from([(
            "failure".into(),
            InstanceState::Failed(MicroRegistryError::Access(AccessError::Denied))
        )]))
    );
    assert_eq!(counts.get(), (0, 1));
    runtime.host().close();
}

#[tokio::test]
async fn cancelling_pending_activation_drops_its_registered_local_work() {
    let mut runtime = ApplicationRuntime::new();
    let counts = Rc::new(Cell::new((0, 0)));
    let counted = counts.clone();
    runtime
        .register_provider(MicroFactory::new_async(
            MicroDescriptor {
                id: MicroId::new("pending-work"),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                let counted = counted.clone();
                async move {
                    activation.own_future(TrackedFuture {
                        counts: counted,
                        future: Box::pin(std::future::pending()),
                    })?;
                    std::future::pending().await
                }
            },
        ))
        .unwrap();
    let desired = BTreeMap::from([("pending".into(), configured("pending-work"))]);
    let mut configure = Box::pin(runtime.configure(&desired));
    assert_pending(configure.as_mut()).await;
    assert_pending(configure.as_mut()).await;
    assert!(counts.get().0 > 0);
    drop(configure);
    assert_eq!(counts.get().1, 1);
    runtime.host().close();
}

#[tokio::test]
async fn owned_work_can_register_followup_work_while_being_polled() {
    let mut runtime = ApplicationRuntime::new();
    let seen = Rc::new(Cell::new(0));
    let observed = seen.clone();
    runtime
        .register_provider(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("followup"),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                let child = activation.clone();
                let observed = observed.clone();
                activation.own_future(async move {
                    child
                        .own_future(async move { observed.set(observed.get() + 1) })
                        .unwrap();
                })?;
                Ok(())
            },
        ))
        .unwrap();
    runtime
        .configure(&BTreeMap::from([(
            "followup".into(),
            configured("followup"),
        )]))
        .await
        .unwrap();
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    assert_pending(observer.as_mut()).await;
    assert_eq!(seen.get(), 1);
    assert_pending(observer.as_mut()).await;
    assert_eq!(seen.get(), 1);
    runtime.host().close();
    observer.await;
}

#[derive(Clone, Default)]
struct Gate {
    instances: Rc<RefCell<Vec<Instance>>>,
    finish: Rc<RefCell<Vec<tokio::sync::oneshot::Sender<()>>>>,
    dropped: Rc<Cell<usize>>,
}

impl Gate {
    fn register(&self, runtime: &mut ApplicationRuntime) {
        struct Guard(Rc<Cell<usize>>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }
        let gate = self.clone();
        runtime
            .register_provider(MicroFactory::new_async(
                MicroDescriptor {
                    id: MicroId::new("gated"),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![declaration()],
                },
                move |activation| {
                    let gate = gate.clone();
                    async move {
                        let _guard = Guard(gate.dropped.clone());
                        let instance = activation.instance();
                        gate.instances.borrow_mut().push(instance.clone());
                        activation.publish(Facility(instance));
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        gate.finish.borrow_mut().push(sender);
                        receiver.await.unwrap();
                        Ok(())
                    }
                },
            ))
            .unwrap();
    }
}

fn ready_provider(runtime: &mut ApplicationRuntime, count: Rc<Cell<usize>>) {
    runtime
        .register_provider(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("ready"),
                version: 1,
                requirements: vec![],
                provisions: vec![declaration()],
            },
            move |activation| {
                count.set(count.get() + 1);
                activation.publish(Facility(activation.instance()));
                Ok(())
            },
        ))
        .unwrap();
}

#[tokio::test]
async fn independent_ready_micro_starts_and_disable_cancels_pending_activation() {
    let mut runtime = ApplicationRuntime::new();
    let gate = Gate::default();
    gate.register(&mut runtime);
    let count = Rc::new(Cell::new(0));
    ready_provider(&mut runtime, count.clone());
    let source = MockConfiguration::new(BTreeMap::from([
        ("a-pending".into(), configured("gated")),
        ("z-ready".into(), configured("ready")),
    ]));
    runtime.install_configuration_source(source.clone());
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    assert_eq!(count.get(), 1);
    assert_eq!(gate.instances.borrow().len(), 1);
    assert!(matches!(
        runtime.output::<Facility>("a-pending"),
        Err(AccessError::Revoked)
    ));
    assert_eq!(
        runtime.reports().borrow().clone(),
        Ok(BTreeMap::from([
            ("a-pending".into(), InstanceState::Activating),
            ("z-ready".into(), InstanceState::Active),
        ]))
    );
    source
        .set_enablement("a-pending", Enablement::Disabled)
        .unwrap();
    assert_pending(observer.as_mut()).await;
    assert_eq!(gate.dropped.get(), 1);
    assert_eq!(
        runtime.host().check(&gate.instances.borrow()[0]),
        Err(AccessError::Revoked)
    );
    assert_eq!(
        runtime.reports().borrow().clone(),
        Ok(BTreeMap::from([
            ("a-pending".into(), InstanceState::Disabled),
            ("z-ready".into(), InstanceState::Active),
        ]))
    );
    assert_eq!(count.get(), 1);
    runtime.host().close();
    observer.await;
}

#[tokio::test]
async fn host_close_cancels_provider_without_releasing_its_gate() {
    let mut runtime = ApplicationRuntime::new();
    let gate = Gate::default();
    gate.register(&mut runtime);
    let desired = BTreeMap::from([("pending".into(), configured("gated"))]);
    let mut configure = Box::pin(runtime.configure(&desired));
    assert_pending(configure.as_mut()).await;
    runtime.host().close();
    assert_eq!(
        configure.await,
        Ok(BTreeMap::from([("pending".into(), InstanceState::Stopped)]))
    );
    assert_eq!(gate.dropped.get(), 1);
    assert!(gate.finish.borrow_mut().pop().unwrap().send(()).is_err());
    runtime.observe().await;
}

#[tokio::test]
async fn dropping_reconciliation_preserves_committed_instances_and_cancels_pending_work() {
    let mut runtime = ApplicationRuntime::new();
    let count = Rc::new(Cell::new(0));
    ready_provider(&mut runtime, count.clone());
    let gate = Gate::default();
    gate.register(&mut runtime);
    let desired = BTreeMap::from([
        ("a-ready".into(), configured("ready")),
        ("z-pending".into(), configured("gated")),
    ]);
    let mut configure = Box::pin(runtime.configure(&desired));
    assert_pending(configure.as_mut()).await;
    let committed = runtime.output::<Facility>("a-ready").unwrap().0;
    drop(configure);
    assert_eq!(gate.dropped.get(), 1);
    assert_eq!(runtime.host().check(&committed), Ok(()));
    assert_eq!(
        runtime.configure(&BTreeMap::new()).await,
        Ok(BTreeMap::new())
    );
    assert_eq!(runtime.host().check(&committed), Err(AccessError::Revoked));
    assert_eq!(count.get(), 1);
    runtime.host().close();
}

#[tokio::test]
async fn cancelling_after_withdrawal_leaves_no_stale_order_entries() {
    let mut runtime = ApplicationRuntime::new();
    ready_provider(&mut runtime, Rc::new(Cell::new(0)));
    runtime
        .configure(&BTreeMap::from([("removed".into(), configured("ready"))]))
        .await
        .unwrap();
    let removed = runtime.output::<Facility>("removed").unwrap().0;
    let gate = Gate::default();
    gate.register(&mut runtime);
    let next = BTreeMap::from([("pending".into(), configured("gated"))]);
    let mut configure = Box::pin(runtime.configure(&next));
    assert_pending(configure.as_mut()).await;
    assert_eq!(runtime.host().check(&removed), Err(AccessError::Revoked));
    drop(configure);
    assert_eq!(
        runtime.configure(&BTreeMap::new()).await,
        Ok(BTreeMap::new())
    );
    runtime.host().close();
}

#[tokio::test]
async fn observer_waiting_for_owner_never_applies_an_old_snapshot() {
    let mut runtime = ApplicationRuntime::new();
    let gate = Gate::default();
    gate.register(&mut runtime);
    let desired = BTreeMap::from([("pending".into(), configured("gated"))]);
    let mut configure = Box::pin(runtime.configure(&desired));
    assert_pending(configure.as_mut()).await;
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    gate.finish.borrow_mut().pop().unwrap().send(()).unwrap();
    assert_eq!(
        configure.await,
        Ok(BTreeMap::from([("pending".into(), InstanceState::Active)]))
    );
    let instance = runtime.output::<Facility>("pending").unwrap().0;
    assert_pending(observer.as_mut()).await;
    assert_eq!(gate.instances.borrow().len(), 1);
    assert!(runtime.output::<Facility>("pending").unwrap().0 == instance);
    runtime.host().close();
    observer.await;
}

#[tokio::test]
async fn newer_request_supersedes_waiter_without_restarting_unchanged_micro() {
    let mut runtime = ApplicationRuntime::new();
    let count = Rc::new(Cell::new(0));
    ready_provider(&mut runtime, count.clone());
    let gate = Gate::default();
    gate.register(&mut runtime);
    let initial = BTreeMap::from([
        ("a-ready".into(), configured("ready")),
        ("z-pending".into(), configured("gated")),
    ]);
    let mut first = Box::pin(runtime.configure(&initial));
    assert_pending(first.as_mut()).await;
    let instance = runtime.output::<Facility>("a-ready").unwrap().0;
    let next = BTreeMap::from([("a-ready".into(), configured("ready"))]);
    let mut second = Box::pin(runtime.configure(&next));
    assert_pending(second.as_mut()).await;
    assert_eq!(first.await, Err(ConfigurationError::Superseded));
    assert_eq!(
        second.await,
        Ok(BTreeMap::from([("a-ready".into(), InstanceState::Active)]))
    );
    assert!(runtime.output::<Facility>("a-ready").unwrap().0 == instance);
    assert_eq!(count.get(), 1);
    assert_eq!(gate.dropped.get(), 1);
    runtime.host().close();
}

#[tokio::test]
async fn reentrant_configuration_change_cannot_commit_obsolete_activation() {
    let mut runtime = ApplicationRuntime::new();
    let changes = runtime.clone();
    runtime
        .register_provider(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("obsolete"),
                version: 1,
                requirements: vec![],
                provisions: vec![declaration()],
            },
            move |activation| {
                activation.publish(Facility(activation.instance()));
                changes.install_configuration_source(StaticConfiguration::default());
                Ok(())
            },
        ))
        .unwrap();
    assert_eq!(
        runtime
            .configure(&BTreeMap::from([(
                "obsolete".into(),
                configured("obsolete")
            )]))
            .await,
        Err(ConfigurationError::Superseded)
    );
    assert!(matches!(
        runtime.output::<Facility>("obsolete"),
        Err(AccessError::Revoked)
    ));
    assert_eq!(runtime.reconcile_configuration().await, Ok(BTreeMap::new()));
    runtime.host().close();
}

#[tokio::test]
async fn reports_recheck_earlier_incarnation_after_later_await() {
    let mut runtime = ApplicationRuntime::new();
    ready_provider(&mut runtime, Rc::new(Cell::new(0)));
    let gate = Gate::default();
    gate.register(&mut runtime);
    let desired = BTreeMap::from([
        ("a-ready".into(), configured("ready")),
        ("z-pending".into(), configured("gated")),
    ]);
    let mut configure = Box::pin(runtime.configure(&desired));
    assert_pending(configure.as_mut()).await;
    runtime
        .host()
        .close_instance(&runtime.output::<Facility>("a-ready").unwrap().0)
        .unwrap();
    gate.finish.borrow_mut().pop().unwrap().send(()).unwrap();
    assert_eq!(
        configure.await,
        Ok(BTreeMap::from([
            ("a-ready".into(), InstanceState::Stopped),
            ("z-pending".into(), InstanceState::Active),
        ]))
    );
    runtime.host().close();
}

#[tokio::test]
async fn privileged_configure_cannot_remove_or_rebind_fixed_entries() {
    let mut runtime = ApplicationRuntime::new();
    ready_provider(&mut runtime, Rc::new(Cell::new(0)));
    let fixed = BTreeMap::from([("fixed".into(), configured("ready"))]);
    runtime
        .set_configuration_source(StaticConfiguration::new(fixed.clone()))
        .await
        .unwrap();
    let instance = runtime.output::<Facility>("fixed").unwrap().0;
    assert_eq!(
        runtime.configure(&BTreeMap::new()).await,
        Err(ConfigurationError::LockedInstance("fixed".into()))
    );
    let mut changed = fixed;
    changed
        .get_mut("fixed")
        .unwrap()
        .bindings
        .insert("other".into(), "other".into());
    assert_eq!(
        runtime.configure(&changed).await,
        Err(ConfigurationError::LockedInstance("fixed".into()))
    );
    assert_eq!(runtime.host().check(&instance), Ok(()));
    runtime.host().close();
}

#[tokio::test]
async fn initialization_error_remains_inspectable_and_does_not_retry() {
    let mut runtime = ApplicationRuntime::new();
    let count = Rc::new(Cell::new(0));
    let counted = count.clone();
    runtime
        .register_provider(MicroFactory::new_async(
            MicroDescriptor {
                id: MicroId::new("failure"),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |_| {
                counted.set(counted.get() + 1);
                async {
                    Err(ActivationError::Initialization(
                        "component could not be loaded".into(),
                    ))
                }
            },
        ))
        .unwrap();
    let desired = BTreeMap::from([("failure".into(), configured("failure"))]);
    let expected = Ok(BTreeMap::from([(
        "failure".into(),
        InstanceState::Failed(MicroRegistryError::Initialization(
            "component could not be loaded".into(),
        )),
    )]));
    assert_eq!(runtime.configure(&desired).await, expected);
    assert_eq!(runtime.configure(&desired).await, expected);
    assert_eq!(count.get(), 1);
    runtime.host().close();
}

#[derive(Clone, Copy)]
enum SourceFault {
    Subscribe,
    Current,
    Disconnected,
}

struct FaultSource(SourceFault);

impl ConfigurationSource for FaultSource {
    fn current(&self) -> Result<ConfigurationSnapshot, AccessError> {
        match self.0 {
            SourceFault::Current => Err(AccessError::Revoked),
            SourceFault::Subscribe | SourceFault::Disconnected => {
                Ok(ConfigurationSnapshot::Ready(BTreeMap::new()))
            }
        }
    }

    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        match self.0 {
            SourceFault::Subscribe => Err(AccessError::Denied),
            SourceFault::Current => Ok(ConfigurationWatch::from_static(BTreeMap::new())),
            SourceFault::Disconnected => {
                let (sender, receiver) = tokio::sync::watch::channel(BTreeMap::new());
                drop(sender);
                Ok(ConfigurationWatch::from_watch(receiver))
            }
        }
    }
}

#[tokio::test]
async fn source_read_and_subscribe_failures_keep_graph_and_recover_on_replacement() {
    for (fault, error) in [
        (SourceFault::Subscribe, AccessError::Denied),
        (SourceFault::Current, AccessError::Revoked),
        (SourceFault::Disconnected, AccessError::Revoked),
    ] {
        let mut runtime = ApplicationRuntime::new();
        let count = Rc::new(Cell::new(0));
        ready_provider(&mut runtime, count.clone());
        let desired = BTreeMap::from([("ready".into(), configured("ready"))]);
        runtime.configure(&desired).await.unwrap();
        let instance = runtime.output::<Facility>("ready").unwrap().0;
        let failure = Err(ConfigurationError::Access(error));
        assert_eq!(
            runtime.set_configuration_source(FaultSource(fault)).await,
            failure
        );
        assert_eq!(runtime.reports().borrow().clone(), failure);
        assert_eq!(runtime.configure(&BTreeMap::new()).await, failure);
        assert_eq!(runtime.reconcile_configuration().await, failure);
        assert_eq!(runtime.host().check(&instance), Ok(()));
        assert_eq!(
            runtime
                .set_configuration_source(StaticConfiguration::new(desired))
                .await,
            Ok(BTreeMap::from([("ready".into(), InstanceState::Active)]))
        );
        assert!(runtime.output::<Facility>("ready").unwrap().0 == instance);
        assert_eq!(count.get(), 1);
        runtime.host().close();
    }
}

struct WatchedSource {
    initial: BTreeMap<String, ConfiguredMicro>,
    updates: tokio::sync::watch::Receiver<BTreeMap<String, ConfiguredMicro>>,
}

impl ConfigurationSource for WatchedSource {
    fn current(&self) -> Result<ConfigurationSnapshot, AccessError> {
        Ok(ConfigurationSnapshot::Ready(self.initial.clone()))
    }
    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        Ok(ConfigurationWatch::from_watch(self.updates.clone()))
    }
}

#[tokio::test]
async fn disconnected_source_watch_reports_revocation_without_tearing_down_graph() {
    let mut runtime = ApplicationRuntime::new();
    ready_provider(&mut runtime, Rc::new(Cell::new(0)));
    let desired = BTreeMap::from([("ready".into(), configured("ready"))]);
    let (sender, updates) = tokio::sync::watch::channel(desired.clone());
    runtime
        .set_configuration_source(WatchedSource {
            initial: desired.clone(),
            updates,
        })
        .await
        .unwrap();
    let instance = runtime.output::<Facility>("ready").unwrap().0;
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    drop(sender);
    assert_pending(observer.as_mut()).await;
    assert_eq!(
        runtime.reports().borrow().clone(),
        Err(ConfigurationError::Access(AccessError::Revoked))
    );
    assert_eq!(runtime.host().check(&instance), Ok(()));
    runtime.install_configuration_source(StaticConfiguration::new(desired));
    assert_pending(observer.as_mut()).await;
    assert_eq!(
        runtime.reports().borrow().clone(),
        Ok(BTreeMap::from([("ready".into(), InstanceState::Active)]))
    );
    runtime.host().close();
    observer.await;
}

fn configuration_facet(
    runtime: &ApplicationRuntime,
    owner: &Instance,
) -> (
    clay_data::Facet<BTreeMap<String, ConfiguredMicro>>,
    clay_data::FacetRoute,
) {
    use clay_data::{DocumentGrant, FacetRoute};
    use daybook_types::manifest::RoutineFacetAccess;
    let route = FacetRoute {
        document: "lifecycle-config".into(),
        branch: "main".into(),
        tag: "composition".into(),
        key_id: "main".into(),
    };
    let access = runtime
        .documents()
        .grant_documents(
            &runtime.host(),
            owner,
            vec![DocumentGrant {
                document: route.document.clone(),
                branch: route.branch.clone(),
                facets: vec![RoutineFacetAccess {
                    tag: route.tag.clone().into(),
                    key_id: Some(route.key_id.clone()),
                    read: true,
                    write: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let facet = access
        .document(route.document.clone(), route.branch.clone())
        .unwrap()
        .facet(route.tag.clone(), route.key_id.clone())
        .unwrap();
    (facet, route)
}

#[tokio::test]
async fn facet_configuration_writes_preserve_observable_operations() {
    use clay_compose::micros::composition::FacetConfiguration;
    use clay_data::{OperationError, OperationReceipt, OperationStatus};

    let runtime = ApplicationRuntime::new();
    let owner = runtime.host().start("configuration-authority");
    let (facet, route) = configuration_facet(&runtime, &owner);
    let mock = runtime.mock_documents();
    let mut desired = BTreeMap::from([("ready".into(), configured("ready"))]);
    mock.external_change(route, Some(serde_json::to_value(&desired).unwrap()));
    let source = FacetConfiguration::new(facet.clone(), BTreeMap::new());
    let enablement = source
        .set_enablement("ready", Enablement::Disabled)
        .unwrap();
    assert_eq!(
        enablement.current(),
        OperationReceipt {
            id: enablement.id(),
            revision: 0,
            status: OperationStatus::LocallyApplied,
        }
    );
    desired.get_mut("ready").unwrap().enablement = Enablement::Disabled;
    assert_eq!(
        source.current(),
        Ok(ConfigurationSnapshot::Ready(desired.clone()))
    );
    mock.complete(enablement.id(), Ok(()));
    assert_eq!(
        enablement.next(0).await.unwrap(),
        Some(OperationReceipt {
            id: enablement.id(),
            revision: 1,
            status: OperationStatus::Accepted,
        })
    );

    let bindings = BTreeMap::from([("facility".into(), "supplier".into())]);
    let rebind = source.rebind("ready", bindings.clone()).unwrap();
    let mut optimistic = desired.clone();
    optimistic.get_mut("ready").unwrap().bindings = bindings;
    assert_eq!(
        source.current(),
        Ok(ConfigurationSnapshot::Ready(optimistic))
    );
    let failure = OperationError::Transport {
        message: "response lost".into(),
        outcome_unknown: true,
    };
    mock.complete(rebind.id(), Err(failure.clone()));
    assert_eq!(
        rebind.next(0).await.unwrap(),
        Some(OperationReceipt {
            id: rebind.id(),
            revision: 1,
            status: OperationStatus::RolledBack(failure),
        })
    );
    assert_eq!(source.current(), Ok(ConfigurationSnapshot::Ready(desired)));
    assert!(facet.watch.current().unwrap().pending.is_empty());

    let pending = source.set_enablement("ready", Enablement::Enabled).unwrap();
    runtime.host().close_instance(&owner).unwrap();
    mock.complete(pending.id(), Ok(()));
    assert_eq!(
        pending.next(0).await.unwrap(),
        Some(OperationReceipt {
            id: pending.id(),
            revision: 1,
            status: OperationStatus::Accepted,
        })
    );
    assert_eq!(source.current(), Err(AccessError::Revoked));
    runtime.host().close();
}

#[tokio::test]
async fn cold_configuration_keeps_graph_until_ready_and_absent_remains_empty() {
    use clay_compose::micros::composition::FacetConfiguration;
    let mut runtime = ApplicationRuntime::new();
    let count = Rc::new(Cell::new(0));
    ready_provider(&mut runtime, count.clone());
    let desired = BTreeMap::from([("ready".into(), configured("ready"))]);
    runtime.configure(&desired).await.unwrap();
    let instance = runtime.output::<Facility>("ready").unwrap().0;
    runtime.mock_documents().hold_refreshes(true);
    let (facet, route) = configuration_facet(&runtime, &runtime.root());
    runtime
        .mock_documents()
        .seed(route.clone(), serde_json::to_value(&desired).unwrap());
    let source = FacetConfiguration::new(facet, BTreeMap::new());
    assert_eq!(source.current(), Ok(ConfigurationSnapshot::Loading));
    assert_eq!(
        source
            .set_enablement("ready", Enablement::Disabled)
            .unwrap_err(),
        ConfigurationError::Loading
    );
    let mut install = Box::pin(runtime.set_configuration_source(source));
    assert_pending(install.as_mut()).await;
    assert_eq!(runtime.host().check(&instance), Ok(()));
    assert_eq!(
        runtime.reports().borrow().clone(),
        Err(ConfigurationError::Loading)
    );
    runtime.mock_documents().complete_refreshes();
    assert_eq!(
        install.await,
        Ok(BTreeMap::from([("ready".into(), InstanceState::Active)]))
    );
    assert!(runtime.output::<Facility>("ready").unwrap().0 == instance);
    assert_eq!(count.get(), 1);
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    runtime.mock_documents().external_change(route, None);
    assert_pending(observer.as_mut()).await;
    assert_eq!(runtime.reports().borrow().clone(), Ok(BTreeMap::new()));
    assert_eq!(runtime.host().check(&instance), Err(AccessError::Revoked));
    runtime.host().close();
    observer.await;
}

#[tokio::test]
async fn revoked_configuration_snapshot_is_reported_without_replacing_last_graph() {
    use clay_compose::micros::composition::FacetConfiguration;
    let mut runtime = ApplicationRuntime::new();
    ready_provider(&mut runtime, Rc::new(Cell::new(0)));
    let owner = runtime.host().start("configuration-authority");
    let (facet, route) = configuration_facet(&runtime, &owner);
    let desired = BTreeMap::from([("ready".to_owned(), configured("ready"))]);
    runtime
        .mock_documents()
        .external_change(route, Some(serde_json::to_value(&desired).unwrap()));
    runtime
        .set_configuration_source(FacetConfiguration::new(facet, BTreeMap::new()))
        .await
        .unwrap();
    let instance = runtime.output::<Facility>("ready").unwrap().0;
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    runtime.host().close_instance(&owner).unwrap();
    assert_pending(observer.as_mut()).await;
    assert_eq!(
        runtime.reports().borrow().clone(),
        Err(ConfigurationError::Access(AccessError::Revoked))
    );
    assert_eq!(runtime.host().check(&instance), Ok(()));
    runtime.host().close();
    observer.await;
}

#[tokio::test]
async fn observer_cancellation_drops_pending_activation_and_can_resume() {
    let mut runtime = ApplicationRuntime::new();
    let gate = Gate::default();
    gate.register(&mut runtime);
    let source = MockConfiguration::new(BTreeMap::from([("pending".into(), configured("gated"))]));
    runtime.install_configuration_source(source.clone());
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    drop(observer);
    assert_eq!(gate.dropped.get(), 1);
    assert_eq!(
        runtime.host().check(&gate.instances.borrow()[0]),
        Err(AccessError::Revoked)
    );
    source
        .set_enablement("pending", Enablement::Disabled)
        .unwrap();
    let mut observer = Box::pin(runtime.observe());
    assert_pending(observer.as_mut()).await;
    assert_eq!(
        runtime.reports().borrow().clone(),
        Ok(BTreeMap::from([(
            "pending".into(),
            InstanceState::Disabled
        )]))
    );
    runtime.host().close();
    observer.await;
}

#[derive(Clone)]
struct Facility(Instance);
impl Provision for Facility {
    const ID: &'static str = "test.facility";
}

struct Fixture {
    runtime: ApplicationRuntime,
    desired: BTreeMap<String, ConfiguredMicro>,
    reports:
        tokio::sync::watch::Receiver<Result<BTreeMap<String, InstanceState>, ConfigurationError>>,
    activations: Arc<Mutex<Vec<String>>>,
}

impl Fixture {
    async fn new() -> Self {
        let mut runtime = ApplicationRuntime::new();
        let activations = Arc::new(Mutex::new(Vec::new()));
        let counted = activations.clone();
        runtime
            .register_provider(MicroFactory::new(
                MicroDescriptor {
                    id: MicroId::new("consumer"),
                    version: 1,
                    requirements: vec![declaration()],
                    provisions: vec![declaration()],
                },
                move |activation| {
                    activation.require::<Facility>()?;
                    counted.lock().unwrap().push("consumer".into());
                    activation.publish(Facility(activation.instance()));
                    Ok(())
                },
            ))
            .unwrap();
        let desired = BTreeMap::from([
            (
                "consumer".into(),
                ConfiguredMicro {
                    provider: MicroId::new("consumer"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
                },
            ),
            (
                "supplier".into(),
                ConfiguredMicro {
                    provider: MicroId::new("supplier"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            ),
        ]);
        runtime.configure(&desired).await.unwrap();
        let reports = runtime.reports();
        Self {
            runtime,
            desired,
            reports,
            activations,
        }
    }

    fn register_supplier(&mut self) {
        let counted = self.activations.clone();
        self.runtime
            .register_provider(MicroFactory::new(
                MicroDescriptor {
                    id: MicroId::new("supplier"),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![declaration()],
                },
                move |activation| {
                    counted.lock().unwrap().push("supplier".into());
                    activation.publish(Facility(activation.instance()));
                    Ok(())
                },
            ))
            .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.runtime.host().close();
    }
}

fn declaration() -> MicroProvision {
    MicroProvision {
        id: Facility::ID.into(),
        version: 1,
    }
}

fn active() -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
    Ok(BTreeMap::from([
        ("consumer".into(), InstanceState::Active),
        ("supplier".into(), InstanceState::Active),
    ]))
}

#[tokio::test]
async fn late_registration_activates_blocked_instances_without_manual_reconciliation() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new().await;
            fixture.reports.borrow_and_update();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture.reports.changed().await.unwrap();
            assert_eq!(
                fixture.reports.borrow_and_update().clone(),
                Ok(BTreeMap::from([
                    (
                        "consumer".into(),
                        InstanceState::Blocked {
                            requirements: vec![Facility::ID.into()]
                        }
                    ),
                    (
                        "supplier".into(),
                        InstanceState::UnavailableProvider(MicroId::new("supplier"))
                    ),
                ]))
            );
            // There is deliberately no configure/reconcile call after registration.
            fixture.register_supplier();
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer"]
            );
            assert_eq!(
                fixture
                    .runtime
                    .host()
                    .check(&fixture.runtime.output::<Facility>("consumer").unwrap().0),
                Ok(())
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
            assert_eq!(
                fixture.reports.borrow().clone(),
                Ok(BTreeMap::from([
                    ("consumer".into(), InstanceState::Stopped),
                    ("supplier".into(), InstanceState::Stopped),
                ]))
            );
        })
        .await;
}

#[tokio::test]
async fn registration_before_observer_first_poll_is_not_lost() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new().await;
            fixture.register_supplier();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer"]
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn independent_stop_withdraws_dependents_but_does_not_restart_the_provider() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new().await;
            fixture.register_supplier();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            let supplier = fixture.runtime.output::<Facility>("supplier").unwrap().0;
            let consumer = fixture.runtime.output::<Facility>("consumer").unwrap().0;
            let host = fixture.runtime.host();
            let supplier_closed = host.observe_instance(&supplier).unwrap();
            let consumer_closed = host.observe_instance(&consumer).unwrap();
            host.close_instance(&supplier).unwrap();
            let stopped = Ok(BTreeMap::from([
                (
                    "consumer".into(),
                    InstanceState::Blocked {
                        requirements: vec![Facility::ID.into()],
                    },
                ),
                ("supplier".into(), InstanceState::Stopped),
            ]));
            fixture
                .reports
                .wait_for(|report| report == &stopped)
                .await
                .unwrap();
            assert_eq!(*supplier_closed.borrow(), Some(CloseReason::Requested));
            assert_eq!(
                *consumer_closed.borrow(),
                Some(CloseReason::DependencyWithdrawn)
            );
            assert_eq!(host.check(&consumer), Err(AccessError::Revoked));
            // Force another observation pass without changing the stopped instance's inputs.
            fixture.reports.borrow_and_update();
            fixture
                .runtime
                .register_provider(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new("unrelated"),
                        version: 1,
                        requirements: vec![],
                        provisions: vec![],
                    },
                    |_| Ok(()),
                ))
                .unwrap();
            fixture.reports.changed().await.unwrap();
            assert_eq!(fixture.reports.borrow_and_update().clone(), stopped);
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer"]
            );
            fixture.desired.get_mut("supplier").unwrap().enablement = Enablement::Disabled;
            fixture.runtime.configure(&fixture.desired).await.unwrap();
            fixture.desired.get_mut("supplier").unwrap().enablement = Enablement::Enabled;
            fixture.runtime.configure(&fixture.desired).await.unwrap();
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert!(fixture.runtime.output::<Facility>("supplier").unwrap().0 != supplier);
            assert!(fixture.runtime.output::<Facility>("consumer").unwrap().0 != consumer);
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer", "supplier", "consumer"]
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn newly_available_but_incompatible_provider_reports_error_and_observation_can_recover() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new().await;
            fixture.reports.borrow_and_update();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture.reports.changed().await.unwrap();
            let mut incompatible = declaration();
            incompatible.version = 2;
            fixture
                .runtime
                .register_provider(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new("supplier"),
                        version: 1,
                        requirements: vec![],
                        provisions: vec![incompatible],
                    },
                    |_| panic!("incompatible graph must not activate"),
                ))
                .unwrap();
            let error = Err(ConfigurationError::IncompatibleBinding {
                instance: "consumer".into(),
                contract: Facility::ID.into(),
            });
            fixture
                .reports
                .wait_for(|report| report == &error)
                .await
                .unwrap();
            let counted = fixture.activations.clone();
            fixture
                .runtime
                .register_provider(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new("replacement"),
                        version: 1,
                        requirements: vec![],
                        provisions: vec![declaration()],
                    },
                    move |activation| {
                        counted.lock().unwrap().push("replacement".into());
                        activation.publish(Facility(activation.instance()));
                        Ok(())
                    },
                ))
                .unwrap();
            fixture.desired.get_mut("supplier").unwrap().provider = MicroId::new("replacement");
            fixture.runtime.configure(&fixture.desired).await.unwrap();
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["replacement", "consumer"]
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn cancelling_an_observer_releases_ownership_for_a_replacement() {
    use std::{future::Future, task::Poll};
    let fixture = Fixture::new().await;
    let mut observer = Box::pin(fixture.runtime.observe());
    std::future::poll_fn(|context| {
        assert!(observer.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(observer);
    fixture.runtime.host().close();
    fixture.runtime.observe().await;
}

#[tokio::test]
async fn facet_and_static_configuration_sources_enforce_locked_entries_and_drive_observation() {
    use clay_compose::micros::composition::{
        FacetConfiguration, MockConfiguration, StaticConfiguration,
    };
    use clay_data::{DocumentGrant, FacetRoute};
    use daybook_types::manifest::RoutineFacetAccess;

    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new().await;
            fixture.register_supplier();

            let locked = BTreeMap::from([(
                "supplier".into(),
                ConfiguredMicro {
                    provider: MicroId::new("supplier"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            )]);
            let dynamic = BTreeMap::from([(
                "consumer".into(),
                ConfiguredMicro {
                    provider: MicroId::new("consumer"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
                },
            )]);

            let static_source = StaticConfiguration::new(locked.clone());
            assert_eq!(
                fixture
                    .runtime
                    .set_configuration_source(static_source)
                    .await
                    .unwrap(),
                BTreeMap::from([("supplier".into(), InstanceState::Active)])
            );
            assert!(fixture.runtime.is_locked("supplier"));
            assert!(!fixture.runtime.is_locked("consumer"));

            let mock_source = MockConfiguration::with_locked(locked.clone(), dynamic.clone());
            assert_eq!(
                mock_source.set_enablement("supplier", Enablement::Disabled),
                Err(ConfigurationError::LockedInstance("supplier".into()))
            );

            let route = FacetRoute {
                document: "composition-doc".into(),
                branch: "main".into(),
                tag: "org.dayframe.composition".into(),
                key_id: "main".into(),
            };
            fixture
                .runtime
                .mock_documents()
                .seed(route.clone(), serde_json::to_value(&dynamic).unwrap());
            let access = fixture
                .runtime
                .documents()
                .grant_documents(
                    &fixture.runtime.host(),
                    &fixture.runtime.root(),
                    vec![DocumentGrant {
                        document: route.document.clone(),
                        branch: route.branch.clone(),
                        facets: vec![RoutineFacetAccess {
                            tag: route.tag.clone().into(),
                            key_id: Some(route.key_id.clone()),
                            read: true,
                            write: true,
                            ..Default::default()
                        }],
                    }],
                )
                .unwrap();
            let doc = access
                .document(route.document.clone(), route.branch.clone())
                .unwrap();
            let facet = doc
                .facet::<BTreeMap<String, ConfiguredMicro>>(route.tag.clone(), route.key_id.clone())
                .unwrap();
            let facet_source = FacetConfiguration::new(facet, locked);

            assert_eq!(
                facet_source
                    .set_enablement("supplier", Enablement::Disabled)
                    .unwrap_err(),
                ConfigurationError::LockedInstance("supplier".into())
            );
            assert_eq!(
                fixture
                    .runtime
                    .set_configuration_source(facet_source.clone())
                    .await
                    .unwrap(),
                BTreeMap::from([
                    ("consumer".into(), InstanceState::Active),
                    ("supplier".into(), InstanceState::Active),
                ])
            );
            assert!(fixture.runtime.is_locked("supplier"));
            assert!(!fixture.runtime.is_locked("consumer"));

            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();

            facet_source
                .set_enablement("consumer", Enablement::Disabled)
                .unwrap();
            let disabled = Ok(BTreeMap::from([
                ("consumer".into(), InstanceState::Disabled),
                ("supplier".into(), InstanceState::Active),
            ]));
            fixture
                .reports
                .wait_for(|report| report == &disabled)
                .await
                .unwrap();

            fixture.runtime.mock_documents().settle(false);
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();

            let mut tampered = dynamic;
            tampered.insert(
                "supplier".into(),
                ConfiguredMicro {
                    provider: MicroId::new("supplier"),
                    enablement: Enablement::Disabled,
                    bindings: BTreeMap::new(),
                },
            );
            fixture
                .runtime
                .mock_documents()
                .external_change(route, Some(serde_json::to_value(tampered).unwrap()));
            assert_eq!(
                fixture.runtime.reconcile_configuration().await.unwrap(),
                BTreeMap::from([
                    ("consumer".into(), InstanceState::Active),
                    ("supplier".into(), InstanceState::Active),
                ])
            );

            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}
