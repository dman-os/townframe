//! Activation supervisor (ADR 024 §§1–6, ADR 025 §§8–9): configured singleton micros, strong
//! requirement bindings, external leases, staged initialization, and ordered closing.
//!
//! The supervisor is engine-neutral. An engine (or a test) supplies a [`Factory`] that runs one
//! incarnation's initialization and hands back its [`Root`] and named publications; the
//! supervisor decides when an incarnation exists. A configured micro is started because it is
//! an eager root, because a micro that requires it is being started, or because something
//! [`Supervisor::acquire`]s one of its publications. Each resolved requirement becomes a
//! [`Binding`] owned by the consumer incarnation and each acquisition a [`Lease`]; the provider
//! stays live exactly while some binding or lease retains it, and a provider whose last
//! retention drops is closed unless it is itself an eager root.
//!
//! Closing is ordered: dependents close before their providers, `Root::close` runs while the
//! closing incarnation's own bindings still hold its providers live, its publications are then
//! dropped (revoked) before the incarnation is torn down (which removes its registration
//! scopes), and the teardown drops those bindings last, in reverse acquisition order.
//!
//! Single-threaded by construction (`Rc`/`RefCell`): it lives on one local executor beside the
//! engines it drives. No `RefCell` borrow is held across an `.await`. Engine store state must
//! be `Send` and so cannot hold the supervisor; it holds an [`Activator`], a `Send + Sync`
//! handle whose requests [`Supervisor::drive`] serves on the supervisor's executor.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::instance::AccessError;
use crate::instance::Host;
use crate::instance::Instance;
use crate::instance::Lifecycle;

pub type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// The configured name of a singleton micro: its package, its configured instance, and the
/// at-most-one live incarnation (ADR 025 §5).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MicroId(String);

impl MicroId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MicroId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Runs one incarnation's initialization. `requirements` maps each local requirement binding
/// to the publication its provider serves. `activator` is the supervisor's demand handle,
/// which the incarnation keeps for the lazy references it mints from stored endpoints.
/// Everything the incarnation publishes before the returned future resolves stays staged until
/// the supervisor commits it.
pub trait Factory<P> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, P>,
        activator: Activator<P>,
    ) -> LocalFuture<'static, Result<Initialized<P>, String>>;
}

/// What a successful initialization hands the supervisor (ADR 025 §8): the incarnation's
/// lifecycle proof and the publications it serves, by the names its manifest declares. They
/// become reachable together, only once `initialize` has returned.
pub struct Initialized<P> {
    pub root: Box<dyn Root>,
    pub publications: BTreeMap<String, P>,
}

/// Proof of a successful initialization, held by the supervisor for the incarnation.
pub trait Root {
    /// Called exactly once, before teardown, while the incarnation's dependencies are live.
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>>;
}

pub struct ConfiguredMicro<P> {
    pub identity: MicroId,
    pub eager: bool,
    /// Whether the micro starts enabled. A disabled micro starts in [`MicroState::Disabled`],
    /// so an eager reconcile never starts it and no boot-time race with applying the persisted
    /// enablement exists; [`Supervisor::enable`] and [`Supervisor::disable`] move it later.
    pub enabled: bool,
    /// Local requirement binding -> what satisfies it.
    pub requirements: BTreeMap<String, Requirement>,
    pub factory: Rc<dyn Factory<P>>,
}

/// A strong requirement: the configured `provider`'s named `publication` (ADR 025 §5).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Requirement {
    pub provider: MicroId,
    pub publication: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartError {
    #[error("no configured micro {0}")]
    Unknown(MicroId),
    #[error("{0} is disabled")]
    Disabled(MicroId),
    #[error("requirement cycle through {0}")]
    Cycle(MicroId),
    #[error("requirement {binding} unavailable: {cause}")]
    Unavailable {
        binding: String,
        cause: Box<StartError>,
    },
    /// The provider's live incarnation does not serve this publication. It stays up for its
    /// other users.
    #[error("{provider} does not publish {publication}")]
    Unpublished {
        provider: MicroId,
        publication: String,
    },
    #[error("initialization failed: {0}")]
    Faulted(String),
    #[error(transparent)]
    Access(#[from] AccessError),
}

/// Close failures surfaced by an orderly close, by configured identity. Teardown still
/// happened for every listed micro; it is recorded `Faulted` until retried.
#[derive(Clone, Debug, Default, Eq, PartialEq, thiserror::Error)]
#[error("close failed: {0:?}")]
pub struct CloseError(pub BTreeMap<MicroId, String>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MicroState {
    Disabled,
    Idle,
    Blocked(Vec<String>),
    Initializing,
    Active,
    Closing,
    Faulted(String),
}

/// An external strong edge to one publication of one incarnation (ADR 024 §2, ADR 025 §9).
/// While any lease on an incarnation lives, it is retained. Dropping the last one makes a
/// non-eager incarnation without bindings eligible for idle close, which
/// [`Supervisor::drive`] performs. The lease dereferences to the publication it was acquired
/// for; after its incarnation closes, the publication is revoked and the lease is inert.
pub struct Lease<P> {
    publication: P,
    micro: MicroId,
    /// The leases on the incarnation this one was acquired from.
    retention: Arc<AtomicUsize>,
    idle: Arc<Idle>,
}

/// Micros whose last lease dropped. `Lease::drop` is synchronous and cannot close anything,
/// so it enqueues here and wakes `Supervisor::drive`.
#[derive(Default)]
struct Idle {
    queue: Mutex<VecDeque<MicroId>>,
    wake: tokio::sync::Notify,
}

/// A `Send + Sync` handle to the supervisor's demand and status surface, for engine state that
/// cannot hold the `Rc` supervisor. Requests are served by [`Supervisor::drive`], so
/// `acquire` makes progress only while a driver runs. The supervisor outlives every
/// incarnation (shutdown closes them first), so a closed channel here is an invariant break.
pub struct Activator<P> {
    requests: mpsc::UnboundedSender<Request<P>>,
    mirror: Arc<Mutex<BTreeMap<MicroId, MicroState>>>,
}

/// The request queue while a driver owns it. Dropping the driver puts it back, so a later
/// driver (or an `Activator` call made in between) still finds an open channel.
struct ServedRequests<'a, P> {
    receiver: Option<mpsc::UnboundedReceiver<Request<P>>>,
    home: &'a RefCell<Option<mpsc::UnboundedReceiver<Request<P>>>>,
}

impl<P> Drop for ServedRequests<'_, P> {
    /// Hand the channel back for a later driver, after discarding what is queued. A request
    /// still queued when its driver goes away has nobody left to answer it, and parking it
    /// for a later driver would hang its requester if none ever runs. Dropping its reply
    /// sender instead makes [`Activator::acquire`] observe a closed channel and panic: the
    /// supervisor outlives every incarnation, so losing the driver under demand is an
    /// invariant break, never a hang.
    fn drop(&mut self) {
        let mut receiver = self.receiver.take().unwrap();
        while receiver.try_recv().is_ok() {}
        *self.home.borrow_mut() = Some(receiver);
    }
}

struct Request<P> {
    micro: MicroId,
    publication: String,
    reply: oneshot::Sender<Result<Lease<P>, StartError>>,
}

pub struct Supervisor<P> {
    host: Host,
    inner: RefCell<Inner<P>>,
    idle: Arc<Idle>,
    requests: mpsc::UnboundedSender<Request<P>>,
    /// Taken by the one running [`Self::drive`] and handed back when it is dropped.
    served: RefCell<Option<mpsc::UnboundedReceiver<Request<P>>>>,
    /// Every node's state as last [`Self::set`], for [`Activator::status`].
    mirror: Arc<Mutex<BTreeMap<MicroId, MicroState>>>,
    /// Ticks on every recorded state change, for shells that render [`Self::state`].
    changes: watch::Sender<u64>,
}

struct Inner<P> {
    nodes: BTreeMap<MicroId, Node<P>>,
}

struct Node<P> {
    config: ConfiguredMicro<P>,
    state: NodeState<P>,
}

enum NodeState<P> {
    Disabled,
    Idle,
    Blocked(Vec<String>),
    /// Starting its providers; no incarnation exists yet. Concurrent starts wait on it.
    Starting(Rc<tokio::sync::Notify>),
    /// Its incarnation is initializing. Waiters are woken when the outcome is recorded.
    Initializing(Instance, Rc<tokio::sync::Notify>),
    Active {
        instance: Instance,
        root: Box<dyn Root>,
        publications: BTreeMap<String, P>,
        providers: Vec<MicroId>,
        leases: Arc<AtomicUsize>,
    },
    Closing(Instance),
    Faulted(String),
}

/// What `start` found when it looked at a node.
enum Next {
    Done,
    Start,
    AwaitOutcome(Rc<tokio::sync::Notify>),
    AwaitClosed(Instance),
}

impl<P: Clone + 'static> Supervisor<P> {
    /// Configuration is trusted: a duplicate micro or a requirement on an unconfigured
    /// provider is a programming error.
    pub fn new(host: Host, micros: impl IntoIterator<Item = ConfiguredMicro<P>>) -> Self {
        let mut nodes = BTreeMap::new();
        for config in micros {
            let identity = config.identity.clone();
            let node = Node {
                state: if config.enabled {
                    NodeState::Idle
                } else {
                    NodeState::Disabled
                },
                config,
            };
            assert!(
                nodes.insert(identity.clone(), node).is_none(),
                "configured micro {identity} declared twice"
            );
        }
        for (identity, node) in &nodes {
            for (binding, requirement) in &node.config.requirements {
                assert!(
                    nodes.contains_key(&requirement.provider),
                    "{identity} requires {binding} from unconfigured micro {}",
                    requirement.provider
                );
            }
        }
        let mirror = nodes
            .iter()
            .map(|(identity, node)| {
                let state = if node.config.enabled {
                    MicroState::Idle
                } else {
                    MicroState::Disabled
                };
                (identity.clone(), state)
            })
            .collect();
        let (requests, served) = mpsc::unbounded_channel();
        Self {
            host,
            inner: RefCell::new(Inner { nodes }),
            idle: Arc::default(),
            requests,
            served: RefCell::new(Some(served)),
            mirror: Arc::new(Mutex::new(mirror)),
            changes: watch::channel(0).0,
        }
    }

    /// A receiver that ticks whenever any micro's recorded state changes.
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub fn activator(&self) -> Activator<P> {
        Activator {
            requests: self.requests.clone(),
            mirror: Arc::clone(&self.mirror),
        }
    }

    /// Start every enabled eager root that is not live, returning the ones that failed.
    pub async fn reconcile(&self) -> BTreeMap<MicroId, StartError> {
        let roots = self
            .inner
            .borrow()
            .nodes
            .iter()
            // A disabled root stays down; that is its recorded state, not a start failure.
            .filter(|(_, node)| node.config.eager && !matches!(node.state, NodeState::Disabled))
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        let mut failures = BTreeMap::new();
        for identity in roots {
            if let Err(error) = self.start(&identity).await {
                failures.insert(identity, error);
            }
        }
        failures
    }

    /// Start or join the one activation of `micro` and lease its `publication`. Resolves only
    /// once initialization has returned, so a publication is never observed staged. A faulted
    /// micro is not retried by demand; see [`Self::retry`]. A publication the live incarnation
    /// does not serve is [`StartError::Unpublished`]; the incarnation stays up.
    pub async fn acquire(
        &self,
        micro: &MicroId,
        publication: &str,
    ) -> Result<Lease<P>, StartError> {
        self.start(micro).await?;
        let leased = self.lease(micro, publication);
        if leased.is_err() {
            // The demand may have been what started it, and no lease exists whose drop would
            // queue the idle check.
            self.idle.queue.lock().unwrap().push_back(micro.clone());
            self.idle.wake.notify_one();
        }
        leased
    }

    /// Serve the supervisor's deferred work for its whole life: [`Activator`] requests, and
    /// idle closes whenever a micro's last [`Lease`] drops while nothing else retains it and it
    /// is not an eager root. There is no grace period. Work runs concurrently, so one slow
    /// activation does not delay an idle close. A failed close is recorded as the micro's
    /// `Faulted` state, which is where it is observed, so one micro's failure does not stop
    /// the others. The host spawns this on its local executor and drops it at shutdown.
    /// Running two drivers at once is a programming error.
    pub async fn drive(&self) -> ! {
        let mut served = ServedRequests {
            receiver: Some(
                self.served
                    .borrow_mut()
                    .take()
                    .expect("Supervisor::drive is already running"),
            ),
            home: &self.served,
        };
        let mut work: FuturesUnordered<LocalFuture<'_, ()>> = FuturesUnordered::new();
        loop {
            tokio::select! {
                request = served.receiver.as_mut().unwrap().recv() => {
                    let Request { micro, publication, reply } =
                        request.expect("the supervisor holds a sender");
                    work.push(Box::pin(async move {
                        let leased = self.acquire(&micro, &publication).await;
                        // The requester may have been cancelled meanwhile; dropping the lease
                        // then enqueues the micro for an idle check like any other release.
                        drop(reply.send(leased));
                    }));
                }
                () = self.idle.wake.notified() => {
                    let queued = std::mem::take(&mut *self.idle.queue.lock().unwrap());
                    for micro in queued {
                        work.push(Box::pin(self.close_if_idle(micro)));
                    }
                }
                Some(()) = work.next() => {}
            }
        }
    }

    async fn close_if_idle(&self, micro: MicroId) {
        let idle = self.inner.borrow().nodes[&micro].is_idle(&self.host);
        if idle && let Err(CloseError(_)) = self.close(micro).await {
            // `close` already recorded each failure as `Faulted` on the micro it concerns,
            // which is where it is observed; nothing is lost by carrying on.
        }
    }

    /// Bring `identity` live, starting the providers its requirements resolve to first.
    /// A faulted micro is not retried by demand; see [`Self::retry`].
    async fn start(&self, identity: &MicroId) -> Result<(), StartError> {
        self.start_visiting(identity.clone(), BTreeSet::new()).await
    }

    fn start_visiting(
        &self,
        identity: MicroId,
        mut visiting: BTreeSet<MicroId>,
    ) -> LocalFuture<'_, Result<(), StartError>> {
        Box::pin(async move {
            if visiting.contains(&identity) {
                return Err(StartError::Cycle(identity));
            }
            loop {
                match self.next(&identity)? {
                    Next::Done => return Ok(()),
                    Next::Start => break,
                    Next::AwaitOutcome(outcome) => outcome.notified().await,
                    Next::AwaitClosed(instance) => self.host.await_close(&instance).await,
                }
            }
            visiting.insert(identity.clone());
            let (requirements, factory) = {
                let inner = self.inner.borrow();
                let config = &inner.nodes[&identity].config;
                let requirements = config.requirements.clone();
                (requirements, Rc::clone(&config.factory))
            };
            self.set(&identity, NodeState::Starting(Rc::default()));
            let mut providers = Vec::new();
            for (binding, requirement) in &requirements {
                if let Err(cause) = self
                    .start_visiting(requirement.provider.clone(), visiting.clone())
                    .await
                {
                    if matches!(cause, StartError::Cycle(_)) {
                        self.set(&identity, NodeState::Idle);
                        self.release(providers).await;
                        return Err(cause);
                    }
                    self.set(&identity, NodeState::Blocked(vec![binding.clone()]));
                    self.release(providers).await;
                    return Err(StartError::Unavailable {
                        binding: binding.clone(),
                        cause: Box::new(cause),
                    });
                }
                providers.push(requirement.provider.clone());
            }
            let mut resolved = BTreeMap::new();
            let mut provider_instances = Vec::new();
            let mut unprovided = None;
            for (binding, requirement) in &requirements {
                let inner = self.inner.borrow();
                // A provider started earlier in this loop may have been closed while a later
                // one was starting; nothing binds it yet, so that is an ordinary unavailability.
                let NodeState::Active {
                    instance,
                    publications,
                    ..
                } = &inner.nodes[&requirement.provider].state
                else {
                    let cause = format!(
                        "{} closed while {identity} was starting",
                        requirement.provider
                    );
                    unprovided = Some((binding.clone(), StartError::Faulted(cause)));
                    break;
                };
                match publications.get(&requirement.publication) {
                    Some(publication) => {
                        resolved.insert(binding.clone(), publication.clone());
                        provider_instances.push(instance.clone());
                    }
                    None => {
                        let cause = StartError::Unpublished {
                            provider: requirement.provider.clone(),
                            publication: requirement.publication.clone(),
                        };
                        unprovided = Some((binding.clone(), cause));
                        break;
                    }
                }
            }
            if let Some((binding, cause)) = unprovided {
                self.set(&identity, NodeState::Blocked(vec![binding.clone()]));
                self.release(providers).await;
                return Err(StartError::Unavailable {
                    binding,
                    cause: Box::new(cause),
                });
            }
            let host = self.host.clone();
            let initialized = host
                .initialize(identity.as_str(), |instance| {
                    let host = host.clone();
                    let identity = identity.clone();
                    async move {
                        // Bindings are the consumer's first owned effects, so its teardown
                        // releases them last: its providers outlive every other cleanup.
                        for provider in &provider_instances {
                            let binding = host.bind(&instance, provider)?;
                            host.own(&instance, Box::new(move || drop(binding)))?;
                        }
                        let outcome = match &self.inner.borrow().nodes[&identity].state {
                            NodeState::Starting(outcome) => Rc::clone(outcome),
                            // Disabled while its providers were starting.
                            _ => return Err(StartError::Disabled(identity)),
                        };
                        self.set(
                            &identity,
                            NodeState::Initializing(instance.clone(), outcome),
                        );
                        factory
                            .initialize(host, instance, resolved, self.activator())
                            .await
                            .map_err(StartError::Faulted)
                    }
                })
                .await;
            match initialized {
                Ok((instance, Initialized { root, publications })) => {
                    let state = NodeState::Active {
                        instance,
                        root,
                        publications,
                        providers,
                        leases: Arc::default(),
                    };
                    self.set(&identity, state);
                    Ok(())
                }
                Err(error) => {
                    let state = match &error {
                        StartError::Faulted(message) => NodeState::Faulted(message.clone()),
                        _ => NodeState::Idle,
                    };
                    // A disable that raced the pending initialization already recorded it.
                    if !matches!(self.state(&identity), MicroState::Disabled) {
                        self.set(&identity, state);
                    }
                    self.release(providers).await;
                    Err(error)
                }
            }
        })
    }

    /// Close `identity` and keep it closed until [`Self::enable`]. Dependents close first; the
    /// providers it alone held are released afterwards.
    pub async fn disable(&self, identity: &MicroId) -> Result<(), CloseError> {
        let result = self.close(identity.clone()).await;
        if !matches!(self.state(identity), MicroState::Faulted(_)) {
            self.set(identity, NodeState::Disabled);
        }
        result
    }

    pub fn enable(&self, identity: &MicroId) {
        if self.is_in(identity, |state| matches!(state, NodeState::Disabled)) {
            self.set(identity, NodeState::Idle);
        }
    }

    /// Clear a recorded fault so demand may start the micro again.
    pub fn retry(&self, identity: &MicroId) {
        if self.is_in(identity, |state| matches!(state, NodeState::Faulted(_))) {
            self.set(identity, NodeState::Idle);
        }
    }

    fn is_in(&self, identity: &MicroId, test: impl FnOnce(&NodeState<P>) -> bool) -> bool {
        let inner = self.inner.borrow();
        test(&inner.nodes.get(identity).expect("unknown micro").state)
    }

    /// Close every live micro, dependents before providers.
    pub async fn shutdown(&self) -> Result<(), CloseError> {
        let live = self
            .inner
            .borrow()
            .nodes
            .iter()
            .filter(|(_, node)| node.live().is_some())
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        let mut failures = CloseError::default();
        for identity in live {
            if let Err(error) = self.close(identity).await {
                failures.0.extend(error.0);
            }
        }
        failures.into_result()
    }

    pub fn state(&self, identity: &MicroId) -> MicroState {
        let inner = self.inner.borrow();
        let node = inner.nodes.get(identity).expect("unknown micro");
        if let NodeState::Active { instance, .. } = &node.state
            && self.host.lifecycle(instance) == Lifecycle::Closed
        {
            return MicroState::Faulted("incarnation ended outside supervision".into());
        }
        node.state.micro_state()
    }

    pub fn instance(&self, identity: &MicroId) -> Option<Instance> {
        self.inner.borrow().nodes.get(identity)?.live().cloned()
    }

    /// A lease on `publication` of the live incarnation of `micro`. `start` returns `Ok` only
    /// in the poll that observed or established `Active`, with no `.await` after it, so no
    /// other task can have closed the incarnation by the time this runs.
    fn lease(&self, micro: &MicroId, publication: &str) -> Result<Lease<P>, StartError> {
        let inner = self.inner.borrow();
        let NodeState::Active {
            publications,
            leases,
            ..
        } = &inner.nodes[micro].state
        else {
            panic!("{micro} is not active although its start returned Ok in the same poll");
        };
        let publication = publications
            .get(publication)
            .ok_or_else(|| StartError::Unpublished {
                provider: micro.clone(),
                publication: publication.to_owned(),
            })?
            .clone();
        leases.fetch_add(1, Ordering::AcqRel);
        Ok(Lease {
            publication,
            micro: micro.clone(),
            retention: Arc::clone(leases),
            idle: Arc::clone(&self.idle),
        })
    }

    fn next(&self, identity: &MicroId) -> Result<Next, StartError> {
        if let MicroState::Faulted(fault) = self.state(identity) {
            // Pin an out-of-band ending the host observed as the node's fault.
            self.set(identity, NodeState::Faulted(fault.clone()));
            return Err(StartError::Faulted(fault));
        }
        let inner = self.inner.borrow();
        let node = inner
            .nodes
            .get(identity)
            .ok_or_else(|| StartError::Unknown(identity.clone()))?;
        Ok(match &node.state {
            NodeState::Disabled => return Err(StartError::Disabled(identity.clone())),
            NodeState::Faulted(_) => unreachable!("handled above"),
            NodeState::Idle | NodeState::Blocked(_) => Next::Start,
            NodeState::Active { .. } => Next::Done,
            NodeState::Starting(outcome) | NodeState::Initializing(_, outcome) => {
                Next::AwaitOutcome(Rc::clone(outcome))
            }
            NodeState::Closing(instance) => Next::AwaitClosed(instance.clone()),
        })
    }

    fn close(&self, identity: MicroId) -> LocalFuture<'_, Result<(), CloseError>> {
        Box::pin(async move {
            let state = {
                let mut inner = self.inner.borrow_mut();
                let node = inner
                    .nodes
                    .get_mut(&identity)
                    .unwrap_or_else(|| panic!("unknown micro {identity}"));
                match &node.state {
                    NodeState::Active { instance, .. } => {
                        let closing = NodeState::Closing(instance.clone());
                        std::mem::replace(&mut node.state, closing)
                    }
                    NodeState::Closing(instance) => NodeState::Closing(instance.clone()),
                    NodeState::Initializing(instance, outcome) => {
                        NodeState::Initializing(instance.clone(), Rc::clone(outcome))
                    }
                    _ => return Ok(()),
                }
            };
            let (instance, root, publications, providers) = match state {
                NodeState::Active {
                    instance,
                    root,
                    publications,
                    providers,
                    ..
                } => (instance, root, publications, providers),
                // Another close drives this incarnation; wait for its teardown.
                NodeState::Closing(instance) => {
                    self.host.await_close(&instance).await;
                    return Ok(());
                }
                // Closing a pending incarnation cancels its initialization, which unwinds it
                // and records the outcome in the starting task.
                NodeState::Initializing(instance, _) => {
                    self.host.close_instance(&instance).unwrap();
                    return Ok(());
                }
                _ => unreachable!(),
            };
            let mut failures = CloseError::default();
            let dependents = self
                .inner
                .borrow()
                .nodes
                .iter()
                .filter(|(_, node)| {
                    matches!(&node.state, NodeState::Active { providers, .. } if providers.contains(&identity))
                })
                .map(|(dependent, _)| dependent.clone())
                .collect::<Vec<_>>();
            for dependent in dependents {
                if let Err(error) = self.close(dependent).await {
                    failures.0.extend(error.0);
                }
            }
            let closed = if self.host.begin_close(&instance).unwrap() {
                root.close().await
            } else {
                drop(root);
                Ok(())
            };
            // Revoke the publications while the incarnation's registration scopes still exist,
            // so nothing is reachable through a publication whose backing scope is gone
            // (ADR 017 §6, ADR 025 §9). Teardown removes those scopes next.
            drop(publications);
            self.host.close_instance(&instance).unwrap();
            let state = match closed {
                Ok(()) => NodeState::Idle,
                Err(message) => {
                    failures.0.insert(identity.clone(), message.clone());
                    NodeState::Faulted(message)
                }
            };
            self.set(&identity, state);
            failures.0.extend(self.release(providers).await.0);
            failures.into_result()
        })
    }

    /// Close each of `providers` that is live, not an eager root, and no longer retained by a
    /// binding or a lease. Close failures are also recorded as each provider's fault.
    async fn release(&self, providers: Vec<MicroId>) -> CloseError {
        let mut failures = CloseError::default();
        for provider in providers {
            let unneeded = self.inner.borrow().nodes[&provider].is_idle(&self.host);
            if unneeded && let Err(error) = self.close(provider).await {
                failures.0.extend(error.0);
            }
        }
        failures
    }

    fn set(&self, identity: &MicroId, state: NodeState<P>) {
        self.mirror
            .lock()
            .unwrap()
            .insert(identity.clone(), state.micro_state());
        self.changes.send_modify(|ticks| *ticks += 1);
        let previous = std::mem::replace(
            &mut self
                .inner
                .borrow_mut()
                .nodes
                .get_mut(identity)
                .unwrap()
                .state,
            state,
        );
        if let NodeState::Starting(outcome) | NodeState::Initializing(_, outcome) = &previous {
            outcome.notify_waiters();
        }
        // A replaced root is dropped outside the borrow: its drop may re-enter the engine.
        drop(previous);
    }
}

impl<P> Node<P> {
    fn live(&self) -> Option<&Instance> {
        match &self.state {
            NodeState::Initializing(instance, _)
            | NodeState::Active { instance, .. }
            | NodeState::Closing(instance) => Some(instance),
            _ => None,
        }
    }

    /// Active, not an eager root, and retained by neither a consumer binding nor a lease.
    fn is_idle(&self, host: &Host) -> bool {
        match &self.state {
            NodeState::Active {
                instance, leases, ..
            } => {
                !self.config.eager
                    && leases.load(Ordering::Acquire) == 0
                    && !host.is_bound(instance)
            }
            _ => false,
        }
    }
}

impl<P> NodeState<P> {
    fn micro_state(&self) -> MicroState {
        match self {
            NodeState::Disabled => MicroState::Disabled,
            NodeState::Idle => MicroState::Idle,
            NodeState::Blocked(missing) => MicroState::Blocked(missing.clone()),
            NodeState::Starting(_) | NodeState::Initializing(..) => MicroState::Initializing,
            NodeState::Active { .. } => MicroState::Active,
            NodeState::Closing(_) => MicroState::Closing,
            NodeState::Faulted(message) => MicroState::Faulted(message.clone()),
        }
    }
}

impl<P> Clone for Activator<P> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            mirror: Arc::clone(&self.mirror),
        }
    }
}

impl<P> Activator<P> {
    /// [`Supervisor::acquire`], served by the supervisor's driver.
    pub async fn acquire(
        &self,
        micro: &MicroId,
        publication: &str,
    ) -> Result<Lease<P>, StartError> {
        let (reply, response) = oneshot::channel();
        let request = Request {
            micro: micro.clone(),
            publication: publication.to_owned(),
            reply,
        };
        self.requests
            .send(request)
            .unwrap_or_else(|_| panic!("the supervisor outlives every incarnation"));
        response
            .await
            .expect("the supervisor's driver ended while a request was outstanding")
    }

    /// The micro's state as last recorded by the supervisor, without activating anything.
    /// `None` for a micro that is not configured. An incarnation that ended outside
    /// supervision shows as `Active` until the supervisor next observes it.
    pub fn status(&self, micro: &MicroId) -> Option<MicroState> {
        self.mirror.lock().unwrap().get(micro).cloned()
    }
}

impl<P> Deref for Lease<P> {
    type Target = P;

    fn deref(&self) -> &P {
        &self.publication
    }
}

impl<P> Drop for Lease<P> {
    fn drop(&mut self) {
        if self.retention.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle
                .queue
                .lock()
                .unwrap()
                .push_back(self.micro.clone());
            self.idle.wake.notify_one();
        }
    }
}

impl CloseError {
    fn into_result(self) -> Result<(), CloseError> {
        if self.0.is_empty() { Ok(()) } else { Err(self) }
    }
}

#[cfg(test)]
mod tests;
