//! Activation supervisor (ADR 030 §§1–6, ADR 031 §§8–9): configured singleton micros, strong
//! requirement bindings, external leases, staged initialization, and ordered closing.
//!
//! The supervisor is engine-neutral. An engine (or a test) supplies a [`Factory`] that runs one
//! incarnation's initialization and hands back its [`Root`] and named publications; the
//! supervisor decides when an incarnation exists. A configured micro is started because it is
//! an eager root, because a micro that requires it is being started, or because something
//! [`Supervisor::acquire`]s one of its publications. Each resolved requirement becomes a
//! [`Binding`] owned by the consumer incarnation and each acquisition a [`Lease`]. Every lease
//! has a holder: an external root (a shell, a test, anything holding the supervisor's own
//! [`Supervisor::activator`]) or the incarnation whose [`Factory::initialize`] was handed the
//! activator the lease was acquired through. Bindings and incarnation-held leases are the
//! edges `holder -> provider` of a retention graph; a provider stays live exactly while it is
//! reachable in that graph from a root (an eager micro, an external lease, or an incarnation
//! that is still starting or closing). Reference counting alone would let a disconnected
//! cycle of incarnations that retain each other live forever, so when an edge drops the
//! supervisor searches backwards from the provider for a root, and a provider with none closes
//! together with the rest of its strongly connected group (ADR 030 §§2, 5).
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
/// at-most-one live incarnation (ADR 031 §5).
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

/// What a successful initialization hands the supervisor (ADR 031 §8): the incarnation's
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
    /// Required collection bindings that configuration left unresolved. While any remain the
    /// micro is not startable and shows as [`MicroState::Unconfigured`]: that is a
    /// configuration state, not a fault, so neither an eager reconcile nor demand starts it
    /// (ADR 031 §11). [`Supervisor::reconfigure`] changes it later.
    pub unresolved_bindings: Vec<String>,
    pub factory: Rc<dyn Factory<P>>,
}

/// A strong requirement: the configured `provider`'s named `publication` (ADR 031 §5).
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
    #[error("{0} needs configuration of {1:?}")]
    Unconfigured(MicroId, Vec<String>),
    /// Either a configured requirement cycle, or a demand that would wait on an activation which
    /// is itself (transitively) waiting on the demander's own staged outcome.
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
    /// Enabled but not startable until the named required collection bindings are configured.
    Unconfigured(Vec<String>),
    Initializing,
    Active,
    Closing,
    Faulted(String),
}

/// A strong edge `holder -> provider` to one publication of one incarnation (ADR 030 §2,
/// ADR 031 §9). The holder is recorded when the lease is acquired: an external root for a lease
/// taken through [`Supervisor::acquire`] or [`Supervisor::activator`], the acquiring incarnation
/// for one taken through the activator its factory was handed. While the holder is reachable
/// from a root the provider is retained; dropping the edge makes the provider a candidate for
/// an idle close, which [`Supervisor::drive`] decides by reachability. The lease dereferences
/// to the publication it was acquired for; after its incarnation closes, the publication is
/// revoked and the lease is inert.
pub struct Lease<P> {
    publication: P,
    micro: MicroId,
    holder: Holder,
    /// The leases on the incarnation this one was acquired from.
    retention: Arc<Leases>,
    idle: Arc<Idle>,
}

/// Who holds a lease. An incarnation is named by its scope id, which is never reused, so a
/// lease that outlives its holder cannot be mistaken for one of the replacement's.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Holder {
    /// A root outside the supervisor's graph: a shell mount, a test, an in-flight call from one.
    External,
    Incarnation(u64),
}

/// The live leases on one incarnation, counted per holder. `Lease::drop` is synchronous and
/// may run on any thread, hence the mutex.
#[derive(Default)]
struct Leases(Mutex<BTreeMap<Holder, usize>>);

/// Micros one of whose holders dropped its last lease on them. `Lease::drop` is synchronous and
/// cannot close anything, so it enqueues here and wakes `Supervisor::drive`, which decides by
/// reachability. A drop is the only event that can make a retained micro unreachable besides a
/// close (which re-checks its configured providers itself), so nothing recomputes on reads.
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
    /// Who holds the leases acquired through this handle.
    holder: Holder,
    requests: mpsc::UnboundedSender<Request<P>>,
    mirror: Arc<Mutex<BTreeMap<MicroId, MicroState>>>,
    /// The supervisor's change counter, so [`Self::changes`] can wake whoever acts on a
    /// status without polling the mirror.
    changes: Arc<watch::Sender<u64>>,
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
    holder: Holder,
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
    changes: Arc<watch::Sender<u64>>,
}

struct Inner<P> {
    nodes: BTreeMap<MicroId, Node<P>>,
    /// Pending activations blocked on another micro's activation: `waiter -> targets`, one
    /// entry per outstanding wait (see [`Waiting`]). A demand that would close a loop in this
    /// graph would wait on its own staged outcome forever, so it fails with
    /// [`StartError::Cycle`] instead.
    waits: BTreeMap<MicroId, Vec<MicroId>>,
}

/// One outstanding wait recorded in [`Inner::waits`], removed when dropped, so a cancelled
/// activation cannot leave a stale edge behind.
struct Waiting<'a, P> {
    inner: &'a RefCell<Inner<P>>,
    waiter: MicroId,
    target: MicroId,
}

impl<P> Drop for Waiting<'_, P> {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        let targets = inner.waits.get_mut(&self.waiter).unwrap();
        let at = targets
            .iter()
            .position(|target| *target == self.target)
            .unwrap();
        targets.swap_remove(at);
        if targets.is_empty() {
            inner.waits.remove(&self.waiter);
        }
    }
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
        leases: Arc<Leases>,
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
            .map(|(identity, node)| (identity.clone(), node.micro_state()))
            .collect();
        let (requests, served) = mpsc::unbounded_channel();
        Self {
            host,
            inner: RefCell::new(Inner {
                nodes,
                waits: BTreeMap::new(),
            }),
            idle: Arc::default(),
            requests,
            served: RefCell::new(Some(served)),
            mirror: Arc::new(Mutex::new(mirror)),
            changes: Arc::new(watch::channel(0).0),
        }
    }

    /// A receiver that ticks whenever any micro's recorded state changes.
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// A handle for an external root: leases acquired through it are held by that root.
    pub fn activator(&self) -> Activator<P> {
        self.activator_for(Holder::External)
    }

    fn activator_for(&self, holder: Holder) -> Activator<P> {
        Activator {
            holder,
            requests: self.requests.clone(),
            mirror: Arc::clone(&self.mirror),
            changes: Arc::clone(&self.changes),
        }
    }

    /// Start every enabled eager root that is not live, returning the ones that failed.
    pub async fn reconcile(&self) -> BTreeMap<MicroId, StartError> {
        let roots = self
            .inner
            .borrow()
            .nodes
            .iter()
            // A disabled or unconfigured root stays down; that is its recorded state, not a
            // start failure.
            .filter(|(_, node)| {
                node.config.eager
                    && !matches!(node.state, NodeState::Disabled)
                    && node.config.unresolved_bindings.is_empty()
            })
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
        self.acquire_as(Holder::External, micro, publication).await
    }

    /// [`Self::acquire`] on behalf of `holder`. A holder that is itself still initializing is
    /// blocked on this demand, which is recorded as a wait so a demand cycle is found.
    async fn acquire_as(
        &self,
        holder: Holder,
        micro: &MicroId,
        publication: &str,
    ) -> Result<Lease<P>, StartError> {
        let waiter = match holder {
            Holder::External => None,
            Holder::Incarnation(id) => self.initializing_micro(id),
        };
        let _waiting = waiter.as_ref().map(|waiter| self.waiting(waiter, micro));
        self.start_visiting(micro.clone(), BTreeSet::new(), waiter.clone())
            .await?;
        let leased = self.lease(holder, micro, publication);
        if leased.is_err() {
            // The demand may have been what started it, and no lease exists whose drop would
            // queue the idle check.
            self.idle.queue.lock().unwrap().push_back(micro.clone());
            self.idle.wake.notify_one();
        }
        leased
    }

    /// Serve the supervisor's deferred work for its whole life: [`Activator`] requests, and
    /// idle closes whenever a [`Lease`] edge drops and its provider is no longer reachable from
    /// a root, along with the unreachable cycle it belongs to. There is no grace period. Work runs concurrently, so one slow
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
                    let Request { holder, micro, publication, reply } =
                        request.expect("the supervisor holds a sender");
                    work.push(Box::pin(async move {
                        let leased = self.acquire_as(holder, &micro, &publication).await;
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
        // `close` already recorded each failure as `Faulted` on the micro it concerns, which is
        // where it is observed; nothing is lost by carrying on.
        drop(self.close_unreachable(&micro).await);
    }

    /// The micro of the incarnation `id` while that incarnation is initializing.
    fn initializing_micro(&self, id: u64) -> Option<MicroId> {
        self.inner
            .borrow()
            .nodes
            .iter()
            .find(|(_, node)| {
                matches!(&node.state, NodeState::Initializing(instance, _) if instance.scope_id().0 == id)
            })
            .map(|(micro, _)| micro.clone())
    }

    /// Record that `waiter`'s activation is blocked on `target`'s until the guard drops.
    fn waiting(&self, waiter: &MicroId, target: &MicroId) -> Waiting<'_, P> {
        self.inner
            .borrow_mut()
            .waits
            .entry(waiter.clone())
            .or_default()
            .push(target.clone());
        Waiting {
            inner: &self.inner,
            waiter: waiter.clone(),
            target: target.clone(),
        }
    }

    /// Whether `waiter` waiting on `target`'s activation would wait on itself: `target` is
    /// `waiter`, or is pending and (transitively, through pending activations only) waits on
    /// it. An activation that already finished waits on nothing, whatever a detached task of
    /// it still has outstanding.
    fn would_wait_on_itself(&self, waiter: &MicroId, target: &MicroId) -> bool {
        let inner = self.inner.borrow();
        let mut frontier = vec![target];
        let mut seen = BTreeSet::new();
        while let Some(micro) = frontier.pop() {
            if micro == waiter {
                return true;
            }
            if seen.insert(micro)
                && matches!(
                    inner.nodes[micro].state,
                    NodeState::Starting(_) | NodeState::Initializing(..)
                )
            {
                frontier.extend(inner.waits.get(micro).into_iter().flatten());
            }
        }
        false
    }

    /// Bring `identity` live, starting the providers its requirements resolve to first.
    /// A faulted micro is not retried by demand; see [`Self::retry`].
    async fn start(&self, identity: &MicroId) -> Result<(), StartError> {
        self.start_visiting(identity.clone(), BTreeSet::new(), None)
            .await
    }

    /// `waiter` is the pending activation, if any, whose progress is blocked on this one:
    /// the consumer starting its provider, or the initializing incarnation that demanded it.
    fn start_visiting(
        &self,
        identity: MicroId,
        mut visiting: BTreeSet<MicroId>,
        waiter: Option<MicroId>,
    ) -> LocalFuture<'_, Result<(), StartError>> {
        Box::pin(async move {
            if visiting.contains(&identity) {
                return Err(StartError::Cycle(identity));
            }
            loop {
                match self.next(&identity)? {
                    Next::Done => return Ok(()),
                    Next::Start => break,
                    Next::AwaitOutcome(outcome) => {
                        // Waiting on a pending activation that is itself waiting on ours
                        // would never resolve. The wait edges are recorded before any await,
                        // so of two demands racing into each other the second sees the first.
                        if let Some(waiter) = &waiter
                            && self.would_wait_on_itself(waiter, &identity)
                        {
                            return Err(StartError::Cycle(identity));
                        }
                        outcome.notified().await;
                    }
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
                let waiting = self.waiting(&identity, &requirement.provider);
                let started = self
                    .start_visiting(
                        requirement.provider.clone(),
                        visiting.clone(),
                        Some(identity.clone()),
                    )
                    .await;
                drop(waiting);
                if let Err(cause) = started {
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
                    // Leases the incarnation takes through this handle are held by it.
                    let activator = self.activator_for(Holder::Incarnation(instance.scope_id().0));
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
                            .initialize(host, instance, resolved, activator)
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

    /// Replace the required collection bindings `identity` is missing and close it, so its next
    /// start sees the new configuration (there is no hot rebinding). Dependents close first,
    /// as for any close. Nothing is started here: an eager root is started again by
    /// [`Self::reconcile`], any other micro by demand.
    pub async fn reconfigure(
        &self,
        identity: &MicroId,
        unresolved: Vec<String>,
    ) -> Result<(), CloseError> {
        let result = self.close(identity.clone()).await;
        self.inner
            .borrow_mut()
            .nodes
            .get_mut(identity)
            .expect("unknown micro")
            .config
            .unresolved_bindings = unresolved;
        self.refresh(identity);
        result
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
            return MicroState::Faulted(
                self.host
                    .fault(instance)
                    .unwrap_or_else(|| "incarnation ended outside supervision".into()),
            );
        }
        node.micro_state()
    }

    pub fn instance(&self, identity: &MicroId) -> Option<Instance> {
        self.inner.borrow().nodes.get(identity)?.live().cloned()
    }

    /// A lease on `publication` of the live incarnation of `micro`. `start` returns `Ok` only
    /// in the poll that observed or established `Active`, with no `.await` after it, so no
    /// other task can have closed the incarnation by the time this runs.
    fn lease(
        &self,
        holder: Holder,
        micro: &MicroId,
        publication: &str,
    ) -> Result<Lease<P>, StartError> {
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
        *leases.0.lock().unwrap().entry(holder).or_default() += 1;
        Ok(Lease {
            publication,
            micro: micro.clone(),
            holder,
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
        if matches!(node.state, NodeState::Idle | NodeState::Blocked(_))
            && !node.config.unresolved_bindings.is_empty()
        {
            return Err(StartError::Unconfigured(
                identity.clone(),
                node.config.unresolved_bindings.clone(),
            ));
        }
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
                } => {
                    // Record the transition like every other (mirror, then tick): a waiter on
                    // `changes` sees the close begin, and an activator's status stops saying
                    // the provider is live while its guest closes.
                    self.refresh(&identity);
                    (instance, root, publications, providers)
                }
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
            // (ADR 017 §6, ADR 031 §9). Teardown removes those scopes next.
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

    /// Close each of `providers` that is live and no longer reachable from a root, with the
    /// unreachable cycle it belongs to. Close failures are also recorded as each provider's
    /// fault.
    async fn release(&self, providers: Vec<MicroId>) -> CloseError {
        let mut failures = CloseError::default();
        for provider in providers {
            failures.0.extend(self.close_unreachable(&provider).await.0);
        }
        failures
    }

    /// Close `micro` if nothing reachable from a root retains it, together with every micro in
    /// its strongly connected group (ADR 030 §5): members that only retain each other. They
    /// begin closing together, so each member's guest close runs once with its peers possibly
    /// already revoked, and each close's own teardown drops its leases, which queue the next
    /// reachability check (step 6: recomputed after each close).
    async fn close_unreachable(&self, micro: &MicroId) -> CloseError {
        let group = self.unreachable_group(micro);
        let mut failures = CloseError::default();
        for closed in
            futures::future::join_all(group.into_iter().map(|member| self.close(member))).await
        {
            if let Err(error) = closed {
                failures.0.extend(error.0);
            }
        }
        failures
    }

    /// Empty while `start` is retained from a root; otherwise `start` and the micros that are
    /// both reachable from it and retain it, which are exactly its strongly connected group.
    ///
    /// The search runs backwards from `start` over the retainers of each micro (the holders of
    /// its leases and the micros that configure a strong requirement on it) and stops at the
    /// first root: an eager micro, an external lease, or an incarnation that is still
    /// initializing or already closing (or no longer supervised), whose leases are therefore
    /// not going away on their own. Its cost is the size of `start`'s ancestry, and it runs
    /// only when an edge was dropped, never on a read. Every micro it visits without finding a
    /// root is unreachable, and the group is the part of them `start` reaches forward.
    fn unreachable_group(&self, start: &MicroId) -> Vec<MicroId> {
        let inner = self.inner.borrow();
        let mut retainers: BTreeMap<&MicroId, Vec<&MicroId>> = BTreeMap::new();
        let mut frontier = vec![start];
        while let Some(micro) = frontier.pop() {
            if retainers.contains_key(micro) {
                continue;
            }
            let node = &inner.nodes[micro];
            // Only `start` can be something other than active: retainers are pushed below
            // only when they are.
            let NodeState::Active { leases, .. } = &node.state else {
                return Vec::new();
            };
            if node.config.eager {
                return Vec::new();
            }
            let mut found = Vec::new();
            for holder in leases.0.lock().unwrap().keys() {
                let Holder::Incarnation(id) = holder else {
                    return Vec::new();
                };
                match inner
                    .nodes
                    .iter()
                    .find(|(_, node)| node.live().is_some_and(|live| live.scope_id().0 == *id))
                {
                    Some((holder, node)) if matches!(node.state, NodeState::Active { .. }) => {
                        found.push(holder);
                    }
                    _ => return Vec::new(),
                }
            }
            for (consumer, node) in &inner.nodes {
                if !node
                    .config
                    .requirements
                    .values()
                    .any(|requirement| requirement.provider == *micro)
                {
                    continue;
                }
                match node.state {
                    NodeState::Active { .. } => found.push(consumer),
                    // Starting, initializing and closing consumers hold their bindings.
                    NodeState::Starting(_)
                    | NodeState::Initializing(..)
                    | NodeState::Closing(_) => {
                        return Vec::new();
                    }
                    _ => {}
                }
            }
            frontier.extend(found.iter().copied());
            retainers.insert(micro, found);
        }
        let mut group = BTreeSet::new();
        let mut frontier = vec![start];
        while let Some(micro) = frontier.pop() {
            if group.insert(micro) {
                frontier.extend(
                    retainers
                        .iter()
                        .filter(|(_, held)| held.contains(&micro))
                        .map(|(retained, _)| *retained),
                );
            }
        }
        group.into_iter().cloned().collect()
    }

    /// Re-record `identity`'s state in the mirror and tick the change counter.
    fn refresh(&self, identity: &MicroId) {
        let state = self.inner.borrow().nodes[identity].micro_state();
        self.mirror.lock().unwrap().insert(identity.clone(), state);
        self.changes.send_modify(|ticks| *ticks += 1);
    }

    fn set(&self, identity: &MicroId, state: NodeState<P>) {
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
        self.refresh(identity);
        if let NodeState::Starting(outcome) | NodeState::Initializing(_, outcome) = &previous {
            outcome.notify_waiters();
        }
        // A replaced root is dropped outside the borrow: its drop may re-enter the engine.
        drop(previous);
    }
}

impl<P> Node<P> {
    /// The state as shells see it: an enabled, idle micro with required bindings still
    /// unresolved is `Unconfigured`, not `Idle`.
    fn micro_state(&self) -> MicroState {
        match &self.state {
            NodeState::Idle if !self.config.unresolved_bindings.is_empty() => {
                MicroState::Unconfigured(self.config.unresolved_bindings.clone())
            }
            state => state.micro_state(),
        }
    }

    fn live(&self) -> Option<&Instance> {
        match &self.state {
            NodeState::Initializing(instance, _)
            | NodeState::Active { instance, .. }
            | NodeState::Closing(instance) => Some(instance),
            _ => None,
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
            holder: self.holder,
            requests: self.requests.clone(),
            mirror: Arc::clone(&self.mirror),
            changes: Arc::clone(&self.changes),
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
            holder: self.holder,
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

    /// [`Supervisor::changes`]: ticks whenever any micro's recorded state changes, after the
    /// mirror [`Self::status`] reads was updated. A waiter re-reads the status it cares about
    /// on each tick; a tick does not say which micro moved.
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
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
        let mut holders = self.retention.0.lock().unwrap();
        let count = holders.get_mut(&self.holder).unwrap();
        *count -= 1;
        if *count == 0 {
            // This holder's edge is gone; whether the provider is still reachable some other
            // way is the driver's question.
            holders.remove(&self.holder);
            drop(holders);
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
