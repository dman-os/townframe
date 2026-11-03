//! Engine conformance for the ADR 024 activation boundary, run against the prepared
//! `lifecycle` fixture component (`src/clay_mdk/lifecycle_fixture.rs`).
//!
//! Each engine crate implements [`Incarnations`] over its runtime and runs these scenarios from
//! its own tests, so every engine is held to one chain: manifest requirement → authorized
//! registration → target acceptance → strong binding → provider initialization → publication
//! visible only once committed → async `instance-root.close` with dependencies live →
//! reverse-order teardown that leaves the guest holding nothing.
//!
//! Scenarios must run inside a [`tokio::task::LocalSet`]: incarnation drivers are spawned with
//! [`tokio::task::spawn_local`] and joined (panics propagate) once the host closes.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;

use crate::collections::BytesRegistrations;
use crate::collections::CollectionSession;
use crate::collections::bytes_registrations;
use clay_collection::Change;
use clay_collection::ReadLimits;
use clay_host::ActivationRegistration;
use clay_host::ConfiguredMicro;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Instance;
use clay_host::Lifecycle;
use clay_host::LocalFuture;
use clay_host::MicroState;
use clay_host::RegistrationGrant;
use clay_host::RegistrationTemplate;
use clay_host::Requirement;
use clay_host::Root;
use clay_host::StartError;
use clay_host::Supervisor;

use crate::InitialContext;
use crate::capabilities::CapabilityFailure;
use crate::capabilities::CapabilityLimits;
use crate::capabilities::CapabilityPublication;
use crate::capabilities::CapabilityRef;
use crate::capabilities::CapabilitySession;

/// The contract the fixture provides and requires (`lifecycle_fixture.manifest.json`).
pub const PRESENTATION: &str = "dayframe.lifecycle.presentation";

/// What an incarnation's store still holds once its guest half closed. Anything but
/// [`GuestResidue::NONE`] is a resource the close path leaked to the store's drop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestResidue {
    pub table_empty: bool,
    pub publications: usize,
    pub references: usize,
    pub proxies: usize,
    pub readers: usize,
    pub writers: usize,
}

impl GuestResidue {
    pub const NONE: Self = Self {
        table_empty: true,
        publications: 0,
        references: 0,
        proxies: 0,
        readers: 0,
        writers: 0,
    };

    pub fn of(
        table_empty: bool,
        capabilities: &CapabilitySession,
        collections: &CollectionSession,
    ) -> Self {
        Self {
            table_empty,
            publications: capabilities.publication_count(),
            references: capabilities.reference_count(),
            proxies: capabilities.proxy_count(),
            readers: collections.reader_count(),
            writers: collections.writer_count(),
        }
    }
}

/// One engine's incarnations of the prepared lifecycle fixture.
pub trait Incarnations: 'static {
    type Runtime: 'static;

    /// Instantiate the fixture for `instance` and run its `initialize` with `context`,
    /// returning the runtime and the driver future the caller must poll until the
    /// incarnation ends. A failure is the engine's [`std::fmt::Display`] of it.
    #[expect(
        clippy::type_complexity,
        reason = "the nested Result of (runtime, driver) is the trait's whole contract; an alias would only rename it"
    )]
    fn initialize(
        self: Rc<Self>,
        host: Host,
        instance: Instance,
        context: InitialContext,
    ) -> LocalFuture<'static, Result<(Self::Runtime, LocalFuture<'static, ()>), String>>;

    fn provision(runtime: &Self::Runtime, contract: &str) -> Option<CapabilityPublication>;

    fn inspect(runtime: &Self::Runtime) -> GuestResidue;

    /// Close the guest half and report what the store still held afterwards, before it drops.
    fn close(runtime: Self::Runtime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)>;
}

/// The supplier publishes `provider`'s presentation through an installed registration that
/// the consumer accepts; the consumer redeems it while initializing and again while closing.
/// Disabling the provider must close the consumer first, with the provider still live.
pub async fn provider_consumer_graph<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor("provider");
    let registration = supervisor
        .install("provider", serve_host())
        .expect("configuration authorizes the provider's registration on the consumer");
    assert_eq!(
        registration,
        ActivationRegistration {
            id: "serve-host".to_owned(),
            subject: "provider".to_owned(),
            target: "consumer".to_owned(),
            contract: PRESENTATION.to_owned(),
            declaration_id: "serve-host".to_owned(),
        }
    );
    assert!(harness.log().is_empty(), "installing instantiates nothing");

    assert_eq!(supervisor.reconcile().await, BTreeMap::new());
    assert_eq!(
        harness.log(),
        ["initialize provider", "initialize consumer"]
    );
    let provider = supervisor.instance("provider").unwrap();
    let consumer = supervisor.instance("consumer").unwrap();
    assert!(
        harness.host.is_bound(&provider),
        "the consumer holds its provider"
    );
    assert_eq!(supervisor.provision("consumer", PRESENTATION), None);
    let publication = supervisor
        .provision("provider", PRESENTATION)
        .expect("the committed provider provides its contract");
    let observed = harness.observe(&publication);
    assert!(
        observed.as_presentation_component().is_ok(),
        "a committed publication is visible outside its incarnation"
    );

    // Closing the provider closes its dependent first; the consumer's guest `close`
    // redeems its proxy and fails unless the provider is still live at that point.
    supervisor.disable("provider").await.unwrap();
    assert_eq!(
        harness.log(),
        [
            "initialize provider",
            "initialize consumer",
            "close consumer",
            "close provider"
        ]
    );
    assert_eq!(supervisor.state("provider"), MicroState::Disabled);
    assert_eq!(supervisor.state("consumer"), MicroState::Idle);
    assert_eq!(harness.host.lifecycle(&consumer), Lifecycle::Closed);
    assert_eq!(harness.host.lifecycle(&provider), Lifecycle::Closed);
    assert!(publication.is_revoked());
    assert_eq!(
        observed.as_presentation_component().err(),
        Some(CapabilityFailure::Revoked)
    );
    drop(observed);
    harness.finish(supervisor).await;
}

/// A provider that publishes and then rejects its initialization publishes nothing: the
/// consumer is never initialized and records the unavailable requirement.
pub async fn failing_provider_publishes_nothing<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor("failing-provider");
    supervisor.install("provider", serve_host()).unwrap();
    let refusal = "guest refused initialization: rejected: failing provider rejects its \
                   initialization";
    assert_eq!(
        supervisor.reconcile().await,
        BTreeMap::from([(
            "consumer".to_owned(),
            StartError::Unavailable {
                binding: "presentation".to_owned(),
                cause: Box::new(StartError::Faulted(refusal.to_owned())),
            }
        )])
    );
    assert_eq!(harness.log(), ["initialize provider"]);
    assert_eq!(
        supervisor.state("provider"),
        MicroState::Faulted(refusal.to_owned())
    );
    assert_eq!(
        supervisor.state("consumer"),
        MicroState::Blocked(vec!["presentation".to_owned()])
    );
    assert_eq!(supervisor.instance("provider"), None);
    assert_eq!(supervisor.instance("consumer"), None);
    harness.finish(supervisor).await;
}

pub async fn registration_scope_ownership<E: Incarnations>(engine: E) {
    let engine = Rc::new(engine);
    for initial_state in [Lifecycle::Initializing, Lifecycle::Active] {
        let host = Host::new();
        let registrations = bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), [81, 82]);
        let mut observer = registrations
            .projection()
            .acquire(Vec::new(), None)
            .unwrap();
        let bootstrap = observer.read(None, ReadLimits::default()).await.unwrap();
        assert!(bootstrap.bootstrap_complete);
        assert!(bootstrap.transactions.is_empty());
        let mut through = bootstrap.through;
        let mut incarnations = Vec::new();
        let mut owned_rows = Vec::new();
        for name in ["first", "other"] {
            let instance = host.create(name).unwrap();
            host.begin_initialization(&instance).unwrap();
            if initial_state == Lifecycle::Active {
                host.commit_initialization(&instance).unwrap();
            }
            let before = registrations.snapshot();
            let (runtime, driver) = Rc::clone(&engine)
                .initialize(
                    host.clone(),
                    instance.clone(),
                    registration_context("registrations", &registrations),
                )
                .await
                .unwrap();
            assert_eq!(E::inspect(&runtime), GuestResidue::NONE);
            if initial_state == Lifecycle::Initializing {
                assert_eq!(registrations.snapshot(), before);
                let staged = observer
                    .read(Some(through.clone()), ReadLimits::default())
                    .complete_timeout()
                    .unwrap();
                assert!(staged.transactions.is_empty());
                assert_eq!(staged.through, through);
                host.commit_initialization(&instance).unwrap();
            }
            let rows = registrations
                .snapshot()
                .into_iter()
                .filter(|(key, _)| !before.contains_key(key))
                .collect::<BTreeMap<_, _>>();
            let mut values = rows
                .values()
                .map(|entry| {
                    assert!(entry.value.capabilities.is_empty());
                    ciborium::from_reader::<String, _>(entry.value.body.as_slice()).unwrap()
                })
                .collect::<Vec<_>>();
            values.sort();
            assert_eq!(values, ["a-x", "a-y", "b-x"]);
            let published = observer
                .read(Some(through), ReadLimits::default())
                .await
                .unwrap();
            if initial_state == Lifecycle::Initializing {
                assert_eq!(published.transactions.len(), 1);
                assert_eq!(published.transactions[0].changes.len(), 3);
                assert!(published.transactions[0].changes.iter().all(|change| {
                    matches!(change, Change::Upsert { key, value, row_version, .. }
                        if rows.get(key).is_some_and(|entry| entry.value == *value && entry.version == *row_version))
                }));
            } else {
                assert_eq!(
                    published
                        .transactions
                        .iter()
                        .map(|transaction| transaction.changes.len())
                        .collect::<Vec<_>>(),
                    [1, 1, 1, 1, 1, 2, 1, 1]
                );
                let cleared = &published.transactions[5].changes;
                let cleared_keys = cleared
                    .iter()
                    .map(|change| {
                        assert!(matches!(change, Change::Remove { .. }));
                        change.key().clone()
                    })
                    .collect::<BTreeSet<_>>();
                let first_scope = rows
                    .iter()
                    .filter_map(|(key, entry)| {
                        let value: String =
                            ciborium::from_reader(entry.value.body.as_slice()).unwrap();
                        value.starts_with("a-").then_some(key.clone())
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(cleared_keys, first_scope);
            }
            through = published.through;
            incarnations.push((instance, runtime, driver));
            owned_rows.push(rows);
        }
        let [
            (first, runtime, driver),
            (other, other_runtime, other_driver),
        ]: [_; 2] = incarnations.try_into().ok().unwrap();
        let [first_rows, other_rows]: [_; 2] = owned_rows.try_into().unwrap();
        assert_eq!(registrations.snapshot().len(), 6);
        assert!(host.begin_close(&first).unwrap());
        assert_eq!(registrations.snapshot().len(), 6);
        let closing = observer
            .read(Some(through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(closing.transactions.is_empty());
        let (closed, residue) = E::close(runtime).await;
        closed.unwrap();
        assert_eq!(residue, GuestResidue::NONE);
        drop(driver);
        assert_eq!(registrations.snapshot().len(), 6);
        host.close_instance(&first).unwrap();
        let removed = observer
            .read(
                Some(through),
                ReadLimits {
                    max_transactions: Some(1),
                    max_changes: Some(1),
                },
            )
            .await
            .unwrap();
        assert_eq!(removed.transactions.len(), 1);
        assert_eq!(removed.transactions[0].changes.len(), first_rows.len());
        let removed_keys = removed.transactions[0]
            .changes
            .iter()
            .map(|change| {
                let Change::Remove { key, row_version } = change else {
                    panic!("incarnation closure published an upsert");
                };
                assert!(*row_version > first_rows[key].version);
                key.clone()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(removed_keys, first_rows.into_keys().collect());
        assert_eq!(registrations.snapshot(), other_rows);
        let quiet = observer
            .read(Some(removed.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(quiet.transactions.is_empty());
        drop((other_runtime, other_driver));
        assert_eq!(registrations.snapshot(), other_rows);
        host.close_instance(&other).unwrap();
        let removed = observer
            .read(Some(removed.through), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(removed.transactions.len(), 1);
        assert_eq!(removed.transactions[0].changes.len(), other_rows.len());
        assert!(registrations.snapshot().is_empty());
        host.close();
    }
}

pub async fn failed_registration_initialization_publishes_nothing<E: Incarnations>(engine: E) {
    let host = Host::new();
    let registrations = bytes_registrations(std::num::NonZeroUsize::new(16).unwrap(), [83, 84]);
    let mut observer = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = observer.read(None, ReadLimits::default()).await.unwrap();
    let instance = host.create("failing-registrations").unwrap();
    host.begin_initialization(&instance).unwrap();
    let result = Rc::new(engine)
        .initialize(
            host.clone(),
            instance.clone(),
            registration_context("failing-registrations", &registrations),
        )
        .await;
    assert_eq!(
        result.err().unwrap(),
        "guest refused initialization: rejected: registration fixture rejects its initialization"
    );
    host.close_instance(&instance).unwrap();
    assert!(registrations.snapshot().is_empty());
    let unseen = observer
        .read(Some(bootstrap.through), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    assert!(unseen.transactions.is_empty());
    host.close();
}

fn registration_context(role: &str, registrations: &BytesRegistrations) -> InitialContext {
    let mut arguments = Vec::new();
    ciborium::into_writer(role, &mut arguments).unwrap();
    InitialContext {
        arguments,
        registrations: BTreeMap::from([
            ("a".to_owned(), registrations.clone()),
            ("b".to_owned(), registrations.clone()),
        ]),
        ..InitialContext::default()
    }
}

/// The fixture's `serve-host` activation default, as installation reads it from the manifest.
fn serve_host() -> RegistrationTemplate {
    RegistrationTemplate {
        id: "serve-host".to_owned(),
        target_binding: "host".to_owned(),
        contract: PRESENTATION.to_owned(),
        declaration_id: "serve-host".to_owned(),
    }
}

struct Harness<E> {
    engine: Rc<E>,
    host: Host,
    shared: Rc<Shared>,
}

/// State every fixture factory and root of one scenario writes to.
struct Shared {
    log: RefCell<Vec<String>>,
    drivers: RefCell<Vec<tokio::task::JoinHandle<()>>>,
    /// A live incarnation outside the graph, standing in for every other recipient.
    observer: CapabilitySession,
    observer_instance: Instance,
}

impl<E: Incarnations> Harness<E> {
    fn new(engine: E) -> Self {
        let host = Host::new();
        let observer_instance = host.start("observer");
        let observer = CapabilitySession::new(
            host.clone(),
            observer_instance.clone(),
            CapabilityLimits::default(),
        );
        Self {
            engine: Rc::new(engine),
            host,
            shared: Rc::new(Shared {
                log: RefCell::default(),
                drivers: RefCell::default(),
                observer,
                observer_instance,
            }),
        }
    }

    /// The configured graph: an eager `consumer` target that accepts presentation
    /// registrations through its `presentation` requirement, and a lazy `provider` subject
    /// playing `provider_role`, whose `host` binding is routed to and authorized on it.
    fn supervisor(&self, provider_role: &'static str) -> Supervisor<CapabilityPublication> {
        let provider = ConfiguredMicro {
            identity: "provider".to_owned(),
            eager: false,
            requirements: BTreeMap::new(),
            accepts: BTreeSet::new(),
            registration_grants: vec![RegistrationGrant {
                target: "consumer".to_owned(),
                contract: PRESENTATION.to_owned(),
            }],
            routes: BTreeMap::from([("host".to_owned(), "consumer".to_owned())]),
            factory: self.factory("provider", provider_role),
        };
        let consumer = ConfiguredMicro {
            identity: "consumer".to_owned(),
            eager: true,
            requirements: BTreeMap::from([(
                "presentation".to_owned(),
                Requirement {
                    contract: PRESENTATION.to_owned(),
                    provider: None,
                },
            )]),
            accepts: BTreeSet::from([PRESENTATION.to_owned()]),
            registration_grants: Vec::new(),
            routes: BTreeMap::new(),
            factory: self.factory("consumer", "consumer"),
        };
        Supervisor::new(self.host.clone(), [provider, consumer])
    }

    fn factory(
        &self,
        identity: &'static str,
        role: &'static str,
    ) -> Rc<dyn Factory<CapabilityPublication>> {
        Rc::new(Fixture {
            engine: Rc::clone(&self.engine),
            shared: Rc::clone(&self.shared),
            identity,
            role,
        })
    }

    fn log(&self) -> Vec<String> {
        self.shared.log.borrow().clone()
    }

    fn observe(&self, publication: &CapabilityPublication) -> CapabilityRef {
        self.shared.observer.mint(publication).unwrap()
    }

    /// Shut the graph down, close the host, and join every incarnation driver.
    async fn finish(self, supervisor: Supervisor<CapabilityPublication>) {
        supervisor.shutdown().await.unwrap();
        drop(supervisor);
        self.host
            .close_instance(&self.shared.observer_instance)
            .unwrap();
        self.host.close();
        let drivers = std::mem::take(&mut *self.shared.drivers.borrow_mut());
        for driver in drivers {
            driver.await.unwrap();
        }
    }
}

struct Fixture<E> {
    engine: Rc<E>,
    shared: Rc<Shared>,
    identity: &'static str,
    role: &'static str,
}

impl<E: Incarnations> Factory<CapabilityPublication> for Fixture<E> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Box<dyn Root<CapabilityPublication>>, String>> {
        let engine = Rc::clone(&self.engine);
        let shared = Rc::clone(&self.shared);
        let identity = self.identity;
        let role = self.role;
        Box::pin(async move {
            shared
                .log
                .borrow_mut()
                .push(format!("initialize {identity}"));
            let mut arguments = Vec::new();
            ciborium::into_writer(role, &mut arguments).unwrap();
            let context = InitialContext {
                arguments,
                requirements,
                ..InitialContext::default()
            };
            let (runtime, driver) = engine
                .initialize(host.clone(), instance.clone(), context)
                .await?;
            shared
                .drivers
                .borrow_mut()
                .push(tokio::task::spawn_local(driver));
            assert_eq!(host.lifecycle(&instance), Lifecycle::Initializing);
            if let Some(publication) = E::provision(&runtime, PRESENTATION) {
                // Staged until the supervisor commits this initialization (ADR 024 §5).
                let staged = shared.observer.mint(&publication).unwrap();
                assert_eq!(
                    staged.as_presentation_component().err(),
                    Some(CapabilityFailure::Revoked),
                    "{identity}'s provision is visible before its initialization committed"
                );
            }
            let root: Box<dyn Root<CapabilityPublication>> = Box::new(FixtureRoot::<E> {
                runtime,
                shared,
                identity,
            });
            Ok(root)
        })
    }
}

struct FixtureRoot<E: Incarnations> {
    runtime: E::Runtime,
    shared: Rc<Shared>,
    identity: &'static str,
}

impl<E: Incarnations> Root<CapabilityPublication> for FixtureRoot<E> {
    fn provision(&self, contract: &str) -> Option<CapabilityPublication> {
        E::provision(&self.runtime, contract)
    }

    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        let Self {
            runtime,
            shared,
            identity,
        } = *self;
        Box::pin(async move {
            let (closed, residue) = E::close(runtime).await;
            assert_eq!(
                residue,
                GuestResidue::NONE,
                "{identity} still held guest resources after its close"
            );
            shared.log.borrow_mut().push(format!("close {identity}"));
            closed
        })
    }
}
