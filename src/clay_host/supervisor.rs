//! Activation supervisor (ADR 024 §§1–6): configured singleton micros, authorized activation
//! registrations, strong requirement bindings, staged initialization, and ordered closing.
//!
//! The supervisor is engine-neutral. An engine (or a test) supplies a [`Factory`] that runs one
//! incarnation's initialization and hands back its [`Root`]; the supervisor decides when an
//! incarnation exists. A configured micro is started either because it is an eager root or
//! because a micro that requires it is being started. Each resolved requirement becomes a
//! [`Binding`] owned by the consumer incarnation, so the provider stays live exactly while some
//! consumer incarnation holds it, and a provider whose last binding drops is closed unless it
//! is itself an eager root.
//!
//! Closing is ordered: dependents close before their providers, `Root::close` runs while the
//! closing incarnation's own bindings still hold its providers live, and only then is the
//! incarnation torn down — which drops those bindings last, in reverse acquisition order.
//!
//! Single-threaded by construction (`Rc`/`RefCell`): it lives on one local executor beside the
//! engines it drives. No `RefCell` borrow is held across an `.await`.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use crate::instance::AccessError;
use crate::instance::Host;
use crate::instance::Instance;
use crate::instance::Lifecycle;

pub type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Runs one incarnation's initialization. `requirements` maps each local requirement binding
/// to the provision its provider published. Everything the incarnation publishes before the
/// returned future resolves stays staged until the supervisor commits it.
pub trait Factory<P> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, P>,
    ) -> LocalFuture<'static, Result<Box<dyn Root<P>>, String>>;
}

/// Proof of a successful initialization, held by the supervisor for the incarnation.
pub trait Root<P> {
    /// The provision this incarnation publishes for `contract`, if it provides it.
    fn provision(&self, contract: &str) -> Option<P>;
    /// Called exactly once, before teardown, while the incarnation's dependencies are live.
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>>;
}

pub struct ConfiguredMicro<P> {
    pub identity: String,
    pub eager: bool,
    /// Local requirement binding -> what satisfies it.
    pub requirements: BTreeMap<String, Requirement>,
    /// Extension contracts this micro accepts activation registrations for, as a target.
    pub accepts: BTreeSet<String>,
    /// Which (target, contract) registrations configuration authorizes for this subject.
    pub registration_grants: Vec<RegistrationGrant>,
    /// The subject's contribution target bindings, routed to configured target identities.
    pub routes: BTreeMap<String, String>,
    pub factory: Rc<dyn Factory<P>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Requirement {
    pub contract: String,
    /// A configured supplier. `None` leaves the requirement to accepted registrations.
    pub provider: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrationGrant {
    pub target: String,
    pub contract: String,
}

/// An installable activation default, read from a package manifest without instantiating it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrationTemplate {
    pub id: String,
    pub target_binding: String,
    pub contract: String,
    pub declaration_id: String,
}

/// An installed registration: data, not a live resource. It produces a strong binding only
/// when its target starts, accepts the contract, and requires it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivationRegistration {
    pub id: String,
    pub subject: String,
    pub target: String,
    pub contract: String,
    pub declaration_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RegistrationError {
    #[error("no configured micro {0}")]
    UnknownSubject(String),
    #[error("configuration routes no target for binding {0}")]
    Unrouted(String),
    #[error("{subject} is not authorized to register {contract} on {target}")]
    Unauthorized {
        subject: String,
        target: String,
        contract: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartError {
    #[error("no configured micro {0}")]
    Unknown(String),
    #[error("{0} is disabled")]
    Disabled(String),
    #[error("unresolved requirements {0:?}")]
    Blocked(Vec<String>),
    #[error("requirement cycle through {0}")]
    Cycle(String),
    #[error("requirement {binding} unavailable: {cause}")]
    Unavailable {
        binding: String,
        cause: Box<StartError>,
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
pub struct CloseError(pub BTreeMap<String, String>);

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

pub struct Supervisor<P> {
    host: Host,
    inner: RefCell<Inner<P>>,
}

struct Inner<P> {
    nodes: BTreeMap<String, Node<P>>,
    registrations: BTreeMap<String, ActivationRegistration>,
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
        root: Box<dyn Root<P>>,
        providers: Vec<String>,
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
    pub fn new(host: Host, micros: impl IntoIterator<Item = ConfiguredMicro<P>>) -> Self {
        let mut nodes = BTreeMap::new();
        for config in micros {
            let identity = config.identity.clone();
            let node = Node {
                config,
                state: NodeState::Idle,
            };
            assert!(
                nodes.insert(identity.clone(), node).is_none(),
                "configured micro {identity} declared twice"
            );
        }
        Self {
            host,
            inner: RefCell::new(Inner {
                nodes,
                registrations: BTreeMap::new(),
            }),
        }
    }

    /// Resolve `template` for `subject` against configuration and record it. Only routing and
    /// authority are checked here; whether the target accepts it is decided when it starts.
    pub fn install(
        &self,
        subject: &str,
        template: RegistrationTemplate,
    ) -> Result<ActivationRegistration, RegistrationError> {
        let mut inner = self.inner.borrow_mut();
        let config = &inner
            .nodes
            .get(subject)
            .ok_or_else(|| RegistrationError::UnknownSubject(subject.to_owned()))?
            .config;
        let target = config
            .routes
            .get(&template.target_binding)
            .ok_or_else(|| RegistrationError::Unrouted(template.target_binding.clone()))?
            .clone();
        let authorized = config
            .registration_grants
            .iter()
            .any(|grant| grant.target == target && grant.contract == template.contract);
        if !authorized {
            return Err(RegistrationError::Unauthorized {
                subject: subject.to_owned(),
                target,
                contract: template.contract,
            });
        }
        let registration = ActivationRegistration {
            id: template.id,
            subject: subject.to_owned(),
            target,
            contract: template.contract,
            declaration_id: template.declaration_id,
        };
        inner
            .registrations
            .insert(registration.id.clone(), registration.clone());
        Ok(registration)
    }

    pub fn uninstall(&self, registration: &str) -> Option<ActivationRegistration> {
        self.inner.borrow_mut().registrations.remove(registration)
    }

    /// Start every enabled eager root that is not live, returning the ones that failed.
    pub async fn reconcile(&self) -> BTreeMap<String, StartError> {
        let roots = self
            .inner
            .borrow()
            .nodes
            .iter()
            .filter(|(_, node)| node.config.eager)
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

    /// Bring `identity` live, starting the providers its requirements resolve to first.
    /// A faulted micro is not retried by demand; see [`Self::retry`].
    pub async fn start(&self, identity: &str) -> Result<(), StartError> {
        self.start_visiting(identity.to_owned(), BTreeSet::new())
            .await
    }

    fn start_visiting(
        &self,
        identity: String,
        mut visiting: BTreeSet<String>,
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
            let (resolved, factory) = self.resolve(&identity)?;
            self.set(&identity, NodeState::Starting(Rc::default()));
            let mut providers = Vec::new();
            for (binding, (provider, _)) in &resolved {
                if let Err(cause) = self
                    .start_visiting(provider.clone(), visiting.clone())
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
                providers.push(provider.clone());
            }
            let mut requirements = BTreeMap::new();
            let mut provider_instances = Vec::new();
            let mut unprovided = None;
            for (binding, (provider, contract)) in &resolved {
                let inner = self.inner.borrow();
                // A provider started earlier in this loop may have been closed while a later
                // one was starting; nothing binds it yet, so that is an ordinary unavailability.
                let NodeState::Active { instance, root, .. } = &inner.nodes[provider].state else {
                    let cause = format!("{provider} closed while {identity} was starting");
                    unprovided = Some((binding.clone(), cause));
                    break;
                };
                match root.provision(contract) {
                    Some(provision) => {
                        requirements.insert(binding.clone(), provision);
                        provider_instances.push(instance.clone());
                    }
                    None => {
                        let cause = format!("{provider} does not provide {contract}");
                        unprovided = Some((binding.clone(), cause));
                        break;
                    }
                }
            }
            if let Some((binding, cause)) = unprovided {
                self.set(&identity, NodeState::Blocked(vec![binding.clone()]));
                self.release(providers).await;
                let cause = Box::new(StartError::Faulted(cause));
                return Err(StartError::Unavailable { binding, cause });
            }
            let host = self.host.clone();
            let initialized = host
                .initialize(identity.clone(), |instance| {
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
                            .initialize(host, instance, requirements)
                            .await
                            .map_err(StartError::Faulted)
                    }
                })
                .await;
            match initialized {
                Ok((instance, root)) => {
                    let state = NodeState::Active {
                        instance,
                        root,
                        providers,
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
    pub async fn disable(&self, identity: &str) -> Result<(), CloseError> {
        let result = self.close(identity.to_owned()).await;
        if !matches!(self.state(identity), MicroState::Faulted(_)) {
            self.set(identity, NodeState::Disabled);
        }
        result
    }

    pub fn enable(&self, identity: &str) {
        let mut inner = self.inner.borrow_mut();
        let node = inner.nodes.get_mut(identity).expect("unknown micro");
        if matches!(node.state, NodeState::Disabled) {
            node.state = NodeState::Idle;
        }
    }

    /// Clear a recorded fault so demand may start the micro again.
    pub fn retry(&self, identity: &str) {
        let mut inner = self.inner.borrow_mut();
        let node = inner.nodes.get_mut(identity).expect("unknown micro");
        if matches!(node.state, NodeState::Faulted(_)) {
            node.state = NodeState::Idle;
        }
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

    pub fn state(&self, identity: &str) -> MicroState {
        let inner = self.inner.borrow();
        let node = inner.nodes.get(identity).expect("unknown micro");
        if let NodeState::Active { instance, .. } = &node.state
            && self.host.lifecycle(instance) == Lifecycle::Closed
        {
            return MicroState::Faulted("incarnation ended outside supervision".into());
        }
        match &node.state {
            NodeState::Disabled => MicroState::Disabled,
            NodeState::Idle => MicroState::Idle,
            NodeState::Blocked(missing) => MicroState::Blocked(missing.clone()),
            NodeState::Starting(_) | NodeState::Initializing(..) => MicroState::Initializing,
            NodeState::Active { .. } => MicroState::Active,
            NodeState::Closing(_) => MicroState::Closing,
            NodeState::Faulted(message) => MicroState::Faulted(message.clone()),
        }
    }

    pub fn instance(&self, identity: &str) -> Option<Instance> {
        self.inner.borrow().nodes.get(identity)?.live().cloned()
    }

    pub fn provision(&self, identity: &str, contract: &str) -> Option<P> {
        match &self.inner.borrow().nodes.get(identity)?.state {
            NodeState::Active { root, .. } => root.provision(contract),
            _ => None,
        }
    }

    fn next(&self, identity: &str) -> Result<Next, StartError> {
        if let MicroState::Faulted(fault) = self.state(identity) {
            // Pin an out-of-band ending the host observed as the node's fault.
            self.set(identity, NodeState::Faulted(fault.clone()));
            return Err(StartError::Faulted(fault));
        }
        let inner = self.inner.borrow();
        let node = inner
            .nodes
            .get(identity)
            .ok_or_else(|| StartError::Unknown(identity.to_owned()))?;
        Ok(match &node.state {
            NodeState::Disabled => return Err(StartError::Disabled(identity.to_owned())),
            NodeState::Faulted(_) => unreachable!("handled above"),
            NodeState::Idle | NodeState::Blocked(_) => Next::Start,
            NodeState::Active { .. } => Next::Done,
            NodeState::Starting(outcome) | NodeState::Initializing(_, outcome) => {
                Next::AwaitOutcome(Rc::clone(outcome))
            }
            NodeState::Closing(instance) => Next::AwaitClosed(instance.clone()),
        })
    }

    /// Requirement binding -> (provider identity, contract), plus the factory.
    #[allow(clippy::type_complexity)]
    fn resolve(
        &self,
        identity: &str,
    ) -> Result<(BTreeMap<String, (String, String)>, Rc<dyn Factory<P>>), StartError> {
        let mut inner = self.inner.borrow_mut();
        let node = &inner.nodes[identity];
        let mut resolved = BTreeMap::new();
        let mut missing = Vec::new();
        for (binding, requirement) in &node.config.requirements {
            let provider = requirement.provider.clone().or_else(|| {
                node.config
                    .accepts
                    .contains(&requirement.contract)
                    .then(|| {
                        inner.registrations.values().find(|registration| {
                            registration.target == identity
                                && registration.contract == requirement.contract
                                && inner.nodes.contains_key(&registration.subject)
                        })
                    })
                    .flatten()
                    .map(|registration| registration.subject.clone())
            });
            match provider {
                Some(provider) => {
                    if !inner.nodes.contains_key(&provider) {
                        return Err(StartError::Unknown(provider));
                    }
                    resolved.insert(binding.clone(), (provider, requirement.contract.clone()));
                }
                None => missing.push(binding.clone()),
            }
        }
        let factory = Rc::clone(&node.config.factory);
        if !missing.is_empty() {
            let node = inner.nodes.get_mut(identity).unwrap();
            node.state = NodeState::Blocked(missing.clone());
            return Err(StartError::Blocked(missing));
        }
        Ok((resolved, factory))
    }

    fn close(&self, identity: String) -> LocalFuture<'_, Result<(), CloseError>> {
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
            let (instance, root, providers) = match state {
                NodeState::Active {
                    instance,
                    root,
                    providers,
                } => (instance, root, providers),
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

    /// Close each of `providers` that is live, not an eager root, and no longer bound. Close
    /// failures are also recorded as each provider's fault.
    async fn release(&self, providers: Vec<String>) -> CloseError {
        let mut failures = CloseError::default();
        for provider in providers {
            let unneeded = {
                let inner = self.inner.borrow();
                let node = &inner.nodes[&provider];
                matches!(node.state, NodeState::Active { .. })
                    && !node.config.eager
                    && !self.host.is_bound(node.live().unwrap())
            };
            if unneeded && let Err(error) = self.close(provider).await {
                failures.0.extend(error.0);
            }
        }
        failures
    }

    fn set(&self, identity: &str, state: NodeState<P>) {
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
}

impl CloseError {
    fn into_result(self) -> Result<(), CloseError> {
        if self.0.is_empty() { Ok(()) } else { Err(self) }
    }
}

#[cfg(test)]
mod tests;
