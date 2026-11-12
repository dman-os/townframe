//! The `context` fixture's scenarios (`src/clay_mdk/context_fixture.rs`): the authoring
//! `Context`'s collections, calls, slots and retained calls, against a host-side provider.
use super::*;

/// The shared authoring `Context` over this engine, run against the prepared `context` fixture
/// (`src/clay_mdk/context_fixture.rs`): the guest reads the collection bound to `feed` (a row
/// the host wrote before it started) and registers a row recording what it read, so the host
/// sees a guest collection read and a guest registration write; its `rows` call then answers
/// through its own mirror, which holds the host's row and its own once the host commits it.
/// Closing leaves the guest holding nothing and retracts its row.
///
/// It runs twice, over a volatile collection and over one backed by a durable store: both cross
/// the same projection WIT, so the guest cannot tell them apart (ADR 031 validation 2).
pub async fn context_collections_and_calls<E: Incarnations>(engine: E) {
    let engine = Rc::new(engine);
    let replay = std::num::NonZeroUsize::new(64).unwrap();
    collections_and_calls_over(Rc::clone(&engine), bytes_registrations(replay, [93, 94])).await;
    let durable = crate::collections::durable_bytes_registrations(
        Arc::new(clay_store::MemoryStore::new()),
        b"context.feed".to_vec(),
        replay,
        [93, 94],
    )
    .await
    .expect("a memory store does not fail");
    collections_and_calls_over(engine, durable).await;
}

async fn collections_and_calls_over<E: Incarnations>(
    engine: Rc<E>,
    registrations: BytesRegistrations,
) {
    let host = Host::new();
    let cbor = |text: &str| {
        let mut bytes = Vec::new();
        ciborium::into_writer(text, &mut bytes).unwrap();
        bytes
    };
    let seeder = host.start("seeder");
    CollectionSession::new(
        host.clone(),
        seeder,
        crate::collections::CollectionLimits::default(),
    )
    .grant_writer(clay_host::Grant {
        collection: registrations.clone(),
        micro: MicroId::new("seeder"),
        writer: writer_of("seeder"),
    })
    .unwrap()
    .put(
        cbor("seed"),
        clay_collection::WriteBasis::Concurrent,
        cbor("one"),
    )
    .unwrap();
    let instance = host.create("context").unwrap();
    host.begin_initialization(&instance).unwrap();
    // The guest's mirror mints a reference for each attachment of the rows it sees, its own
    // late row's endpoint included, and a lazy reference needs the supervisor's demand handle.
    // No micro is configured, so nothing can be activated through it.
    let supervisor = clay_host::Supervisor::<CapabilityPublication>::new(host.clone(), []);
    let (runtime, driver) = engine
        .initialize(
            host.clone(),
            instance.clone(),
            ContextWorld::granted("context", &registrations, supervisor.activator()),
        )
        .await
        .unwrap();
    let driver = tokio::task::spawn_local(driver);
    // The guest read the seed through its mirror and registered what it saw.
    host.commit_initialization(&instance).unwrap();
    let written = registrations
        .snapshot()
        .into_iter()
        .map(|(key, entry)| {
            let value = ciborium::from_reader::<String, _>(entry.value.unwrap().body.as_slice());
            (
                ciborium::from_reader::<String, _>(key.as_slice()).unwrap(),
                value.unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        written,
        BTreeMap::from([
            ("guest".to_owned(), "seed=one".to_owned()),
            ("seed".to_owned(), "one".to_owned()),
        ])
    );

    // The call is served from the guest's mirror, which also holds its own committed row.
    let recipient_instance = host.start("recipient");
    let recipient = CapabilitySession::new(
        host.clone(),
        recipient_instance.clone(),
        CapabilityLimits::default(),
    );
    let rows = E::publications(&runtime)
        .remove("rows")
        .expect("the guest publishes `rows`");
    let mut request = Vec::new();
    ciborium::into_writer(&(), &mut request).unwrap();
    let response = recipient.mint(&rows).unwrap().call(request).await.unwrap();
    assert_eq!(
        ciborium::from_reader::<Vec<(String, String)>, _>(response.as_slice()).unwrap(),
        // Rows come in key-byte order, which is CBOR's: shorter text keys first.
        [
            ("seed".to_owned(), "one".to_owned()),
            ("guest".to_owned(), "seed=one".to_owned()),
        ]
    );

    let attach = E::publications(&runtime)
        .remove("attach")
        .expect("the guest publishes `attach`");
    attach_own_publication(&recipient.mint(&attach).unwrap(), &registrations).await;

    assert!(host.begin_close(&instance).unwrap());
    let (closed, residue) = E::close(runtime).await;
    closed.unwrap();
    assert_eq!(residue, GuestResidue::NONE);
    driver.await.unwrap();
    host.close_instance(&instance).unwrap();
    assert_eq!(
        registrations.snapshot().len(),
        1,
        "only the seed outlives the guest"
    );
    host.close_instance(&recipient_instance).unwrap();
    host.close();
}

/// The host's writer of a `feed` collection, as the micro instance `name`.
pub(super) fn seeder(
    host: &Host,
    name: &str,
    registrations: &BytesRegistrations,
) -> crate::collections::RegistrationWriter {
    CollectionSession::new(
        host.clone(),
        host.start(name),
        crate::collections::CollectionLimits::default(),
    )
    .grant_writer(clay_host::Grant {
        collection: registrations.clone(),
        micro: MicroId::new(name),
        writer: writer_of(name),
    })
    .unwrap()
}

pub(super) fn cbor_text(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(text, &mut bytes).unwrap();
    bytes
}

/// A running `context` fixture beside a lazy host-side provider micro. The fixture's `feed`
/// collection starts with one row, `key`, attaching the provider's publication `publication` (of
/// `contract`) as a lazy reference, and the supervisor serves its demand; [`Self::seed`] adds
/// others. `recipient` is the host's handle for calling the fixture's publications.
struct ContextRig<E: Incarnations> {
    host: Host,
    registrations: BytesRegistrations,
    /// The host's writer of `feed`, for rows seeded while the guest runs.
    seeder: crate::collections::RegistrationWriter,
    world: Rc<ContextWorld>,
    provider: MicroId,
    supervisor: Rc<Supervisor<CapabilityPublication>>,
    serving: tokio::task::AbortHandle,
    instance: Instance,
    runtime: E::Runtime,
    driver: JoinHandle<()>,
    recipient_instance: Instance,
    recipient: CapabilitySession,
}

impl<E: Incarnations> ContextRig<E> {
    async fn boot(engine: E, prefix: [u64; 2]) -> Self {
        let host = Host::new();
        let registrations = bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), prefix);
        let provider = MicroId::new("provider");
        let seeder = seeder(&host, "seeder", &registrations);
        let world = Rc::new(ContextWorld::default());
        let supervisor = Rc::new(Supervisor::<CapabilityPublication>::new(
            host.clone(),
            [ConfiguredMicro {
                identity: provider.clone(),
                eager: false,
                enabled: true,
                requirements: BTreeMap::new(),
                unresolved_bindings: Vec::new(),
                factory: Rc::clone(&world) as _,
            }],
        ));
        // Redeeming a lazy reference is served by the supervisor's driver.
        let serving = tokio::task::spawn_local({
            let supervisor = Rc::clone(&supervisor);
            async move { supervisor.drive().await }
        })
        .abort_handle();
        let instance = host.create("context").unwrap();
        host.begin_initialization(&instance).unwrap();
        let (runtime, driver) = Rc::new(engine)
            .initialize(
                host.clone(),
                instance.clone(),
                ContextWorld::granted("context", &registrations, supervisor.activator()),
            )
            .await
            .unwrap();
        let driver = tokio::task::spawn_local(driver);
        host.commit_initialization(&instance).unwrap();
        let recipient_instance = host.start("recipient");
        let recipient = CapabilitySession::new(
            host.clone(),
            recipient_instance.clone(),
            CapabilityLimits::default(),
        );
        Self {
            host,
            registrations,
            seeder,
            world,
            provider,
            supervisor,
            serving,
            instance,
            runtime,
            driver,
            recipient_instance,
            recipient,
        }
    }

    /// Write the `feed` row `key`, attaching the provider's `publication` (of `contract`) as a
    /// lazy reference.
    fn seed_endpoint(&self, key: &str, publication: &str, contract: Nsid) {
        self.seed(
            key,
            RowAttachment::Endpoint(PublicationEndpoint {
                micro: self.provider.clone(),
                publication: publication.to_owned(),
                contract,
            }),
        );
    }

    /// Write the `feed` row `key`, carrying `attachment`, as the host.
    fn seed(&self, key: &str, attachment: RowAttachment) {
        self.seeder
            .put_value(
                cbor_text(key),
                clay_collection::WriteBasis::Concurrent,
                crate::collections::CollectionValue {
                    body: cbor_text(key),
                    attachments: vec![attachment],
                },
            )
            .unwrap();
    }

    /// Close the fixture in order: guest, then the provider and host.
    async fn finish(self) {
        assert!(self.host.begin_close(&self.instance).unwrap());
        let (closed, residue) = E::close(self.runtime).await;
        closed.unwrap();
        assert_eq!(residue, GuestResidue::NONE);
        self.driver.await.unwrap();
        self.host.close_instance(&self.instance).unwrap();
        self.host.close_instance(&self.recipient_instance).unwrap();
        self.serving.abort();
        self.supervisor.shutdown().await.unwrap();
        self.host.close();
    }
}

/// The host side of the `context` fixture's scenarios (`src/clay_mdk/context_fixture.rs`): what
/// it grants the fixture, and a provider factory whose presentation `view` the fixture follows
/// and whose call `echo` (answering its request) it retains.
#[derive(Default)]
pub(super) struct ContextWorld {
    /// How many incarnations of the provider have started.
    activations: Rc<std::cell::Cell<usize>>,
}

/// The provider's `echo` call.
struct Echoes;

impl CallTarget for Echoes {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, CallError>> {
        async move { Ok(request) }.boxed()
    }
}

/// The contract of the provider's `echo` call.
pub(super) const ECHO_CONTRACT: &str = "org.example.clay.test.context.echo";
/// The contract of the micro's own `relay` call.
pub(super) const RELAY_CONTRACT: &str = "org.example.clay.test.context.relay";

impl ContextWorld {
    /// The publications the `context` micro's manifest declares, by name.
    pub(super) fn declared() -> BTreeMap<String, DeclaredPublication> {
        let call = |contract: &str| DeclaredPublication {
            kind: PublicationKind::Call,
            contract: Nsid::literal(contract),
        };
        let presentation = || DeclaredPublication {
            kind: PublicationKind::Presentation,
            contract: presentation_component(),
        };
        BTreeMap::from([
            (
                "rows".to_owned(),
                call("org.example.clay.test.context.rows"),
            ),
            (
                "attach".to_owned(),
                call("org.example.clay.test.context.attach"),
            ),
            (
                "follow".to_owned(),
                call("org.example.clay.test.context.follow"),
            ),
            (
                "hold".to_owned(),
                call("org.example.clay.test.context.hold"),
            ),
            (
                "release".to_owned(),
                call("org.example.clay.test.context.release"),
            ),
            (
                "probe".to_owned(),
                call("org.example.clay.test.context.probe"),
            ),
            ("echo".to_owned(), call(ECHO_CONTRACT)),
            ("relay".to_owned(), call(RELAY_CONTRACT)),
            ("detail".to_owned(), presentation()),
            ("slot".to_owned(), presentation()),
        ])
    }

    /// The `feed` registration collection and the publications the fixture's manifest declares,
    /// with `supervisor` behind the lazy references its mirror mints.
    pub(super) fn granted(
        micro: &str,
        registrations: &BytesRegistrations,
        activator: Activator<CapabilityPublication>,
    ) -> InitialContext {
        InitialContext {
            collections: BTreeMap::from([("feed".to_owned(), registrations.projection())]),
            registrations: BTreeMap::from([(
                "feed".to_owned(),
                clay_host::Grant {
                    collection: registrations.clone(),
                    micro: MicroId::new(micro),
                    writer: writer_of(micro),
                },
            )]),
            publications: Self::declared(),
            micro: Some(MicroId::new(micro)),
            activator: Some(activator),
            ..InitialContext::default()
        }
    }
}

struct PresenterRoot {
    _published: Vec<PublishedCapability>,
}

impl Factory<CapabilityPublication> for ContextWorld {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        _requirements: BTreeMap<String, CapabilityPublication>,
        _activator: Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        self.activations.set(self.activations.get() + 1);
        Box::pin(async move {
            let bank = CapabilitySession::new(host, instance, CapabilityLimits::default());
            let view = bank.publish_presentation("view").unwrap();
            let echo = bank
                .publish_call("echo", Nsid::literal(ECHO_CONTRACT), Arc::new(Echoes))
                .unwrap();
            let publications = BTreeMap::from([
                ("view".to_owned(), view.publication()),
                ("echo".to_owned(), echo.publication()),
            ]);
            Ok(Initialized {
                root: Box::new(PresenterRoot {
                    _published: vec![view, echo],
                }),
                publications,
            })
        })
    }
}

impl Root for PresenterRoot {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

/// The two waits a guest slot follows its provider with, against a real supervisor. The guest
/// (`context` fixture) reads a row attaching a lazy reference to a host-side provider's
/// presentation, and its `follow` call redeems it, recording each step as a `progress/<n>` row:
///
/// - while the provider is disabled the redemption fails and the guest parks in
///   `capability-ref.status-changed`; enabling the provider wakes it, and it redeems again,
///   which starts the provider (`mounted`);
/// - disabling the provider ends the incarnation the proxy was redeemed from, which completes
///   `presentation-component.revoked` in the guest (`revoked`).
///
/// A wake the host failed to deliver would leave the call parked, so the scenario would hang.
pub async fn context_follows_a_provider<E: Incarnations>(engine: E) {
    let rig = ContextRig::boot(engine, [97, 98]).await;
    rig.seed_endpoint("view", "view", presentation_component());
    let (provider, supervisor) = (&rig.provider, &rig.supervisor);
    let registrations = &rig.registrations;
    supervisor.disable(provider).await.unwrap();
    let cbor = cbor_text;
    let follow = rig
        .recipient
        .mint(&E::publications(&rig.runtime).remove("follow").unwrap())
        .unwrap();
    let following = tokio::task::spawn_local(async move { follow.call(cbor("view")).await });

    // The guest records each step in a row of its own; waiting on the collection's change
    // stream is how the host learns the guest reached it.
    let mut changes = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let mut through = None;
    let mut reached = async |step: usize, expected: &str| {
        let key = cbor(&format!("progress/{step}"));
        loop {
            let recorded = registrations
                .snapshot()
                .get(&key)
                .and_then(|entry| entry.value.as_ref())
                .map(|value| ciborium::from_reader::<String, _>(value.body.as_slice()).unwrap());
            if let Some(recorded) = recorded {
                assert_eq!(recorded, expected);
                return;
            }
            let page = changes
                .read(through.take(), ReadLimits::default())
                .await
                .unwrap();
            through = Some(page.through);
        }
    };
    reached(0, "unavailable").await;
    supervisor.enable(provider);
    reached(1, "mounted").await;
    assert_eq!(supervisor.state(provider), MicroState::Active);
    supervisor.disable(provider).await.unwrap();
    let steps = following.await.unwrap().unwrap();
    assert_eq!(
        ciborium::from_reader::<Vec<String>, _>(steps.as_slice()).unwrap(),
        ["unavailable", "mounted", "revoked"]
    );
    rig.finish().await;
}

/// `Capability::retain_call` through a guest (`hold` and `release` of the `context` fixture,
/// against a real supervisor): a row attaches a lazy reference to a host-side provider's `echo`
/// call; `hold` retains it and calls it twice, so the provider, which a plain call would start
/// and release per call, is started once and still live after `hold` returned; `release` drops
/// the binding, which the provider observes as going idle.
pub async fn context_retains_a_call<E: Incarnations>(engine: E) {
    let rig = ContextRig::boot(engine, [99, 100]).await;
    rig.seed_endpoint("echo", "echo", Nsid::literal(ECHO_CONTRACT));
    let (provider, supervisor) = (&rig.provider, &rig.supervisor);
    let call = |name: &str, request: Vec<u8>| {
        let reference = rig
            .recipient
            .mint(&E::publications(&rig.runtime).remove(name).unwrap())
            .unwrap();
        async move { reference.call(request).await.unwrap() }
    };
    assert_eq!(supervisor.state(provider), MicroState::Idle);
    assert_eq!(rig.world.activations.get(), 0);

    let replies = call("hold", cbor_text("echo")).await;
    assert_eq!(
        ciborium::from_reader::<Vec<String>, _>(replies.as_slice()).unwrap(),
        ["one", "two"]
    );
    assert_eq!(
        rig.world.activations.get(),
        1,
        "two calls through the binding are one incarnation"
    );
    assert_eq!(
        supervisor.state(provider),
        MicroState::Active,
        "the retained binding outlives the call that took it"
    );

    let mut unit = Vec::new();
    ciborium::into_writer(&(), &mut unit).unwrap();
    call("release", unit).await;
    let mut changes = supervisor.changes();
    changes
        .wait_for(|_| supervisor.state(provider) == MicroState::Idle)
        .await
        .unwrap();
    assert_eq!(rig.world.activations.get(), 1);
    rig.finish().await;
}

/// ADR 031 validation 9 through a guest: a bound (non-lazy) reference the guest holds, a row
/// attaching a host-side provider's published `echo`, reads `Live` while the provider's
/// incarnation runs and answers a call and a `retain_call`; once the provider idle-closed,
/// `status()` reads `Revoked` through the WIT and a call and a `retain_call` through it fail as
/// `Revoked`, and the provider is still idle, one activation in all: a reference to a closed
/// incarnation never starts a new one.
pub async fn context_sees_a_closed_provider_revoked<E: Incarnations>(engine: E) {
    let rig = ContextRig::boot(engine, [101, 102]).await;
    let (provider, supervisor) = (&rig.provider, &rig.supervisor);
    let lease = supervisor
        .acquire(provider, "echo")
        .await
        .expect("the provider publishes `echo`");
    rig.seed("bound", RowAttachment::Bound((*lease).clone()));
    let probe = rig
        .recipient
        .mint(&E::publications(&rig.runtime).remove("probe").unwrap())
        .unwrap();
    let probed = async || {
        let response = probe.call(cbor_text("bound")).await.unwrap();
        ciborium::from_reader::<Vec<String>, _>(response.as_slice()).unwrap()
    };
    assert_eq!(probed().await, ["Live", "Ok(())", "Ok(())"]);

    drop(lease);
    supervisor
        .changes()
        .wait_for(|_| supervisor.state(provider) == MicroState::Idle)
        .await
        .unwrap();
    assert_eq!(
        probed().await,
        ["Revoked", "Err(Revoked)", "Err(Revoked)"],
        "the guest's reference to the closed incarnation is revoked"
    );
    assert_eq!(supervisor.state(provider), MicroState::Idle);
    assert_eq!(rig.world.activations.get(), 1);
    drop(probe);
    rig.finish().await;
}

/// Two supervised `context` micros, `a` and `b`, each with a `feed` of its own holding one row,
/// `key`, that attaches the other's `echo` call as a lazy reference. The supervisor serves their
/// demands, and the factory hands each incarnation its own activator, so the leases a micro takes
/// on its peer are held by that incarnation. `log` records `initialize <micro>` and `close
/// <micro>` as the supervisor runs them.
struct PeerRig {
    host: Host,
    supervisor: Rc<Supervisor<CapabilityPublication>>,
    serving: tokio::task::AbortHandle,
    log: Rc<RefCell<Vec<String>>>,
    recipient_instance: Instance,
    recipient: CapabilitySession,
    _seeders: Vec<crate::collections::RegistrationWriter>,
}

/// One peer's factory: an incarnation of the `context` fixture on `E`.
struct Peer<E: Incarnations> {
    engine: Rc<E>,
    identity: &'static str,
    registrations: BytesRegistrations,
    log: Rc<RefCell<Vec<String>>>,
}

/// A running peer: its guest half closes with the root, and must leave nothing behind.
struct PeerRoot<E: Incarnations> {
    runtime: E::Runtime,
    driver: JoinHandle<()>,
    identity: &'static str,
    log: Rc<RefCell<Vec<String>>>,
}

impl<E: Incarnations> Factory<CapabilityPublication> for Peer<E> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        _requirements: BTreeMap<String, CapabilityPublication>,
        activator: Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        let engine = Rc::clone(&self.engine);
        let (identity, log) = (self.identity, Rc::clone(&self.log));
        let context = ContextWorld::granted(identity, &self.registrations, activator);
        Box::pin(async move {
            log.borrow_mut().push(format!("initialize {identity}"));
            let (runtime, driver) = engine.initialize(host, instance, context).await?;
            let publications = E::publications(&runtime);
            let root = Box::new(PeerRoot::<E> {
                runtime,
                driver: tokio::task::spawn_local(driver),
                identity,
                log,
            });
            Ok(Initialized { root, publications })
        })
    }
}

impl<E: Incarnations> Root for PeerRoot<E> {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let Self {
                runtime,
                driver,
                identity,
                log,
            } = *self;
            let (closed, residue) = E::close(runtime).await;
            assert_eq!(
                residue,
                GuestResidue::NONE,
                "{identity} still held resources after its close"
            );
            driver.await.unwrap();
            log.borrow_mut().push(format!("close {identity}"));
            closed
        })
    }
}

impl PeerRig {
    async fn boot<E: Incarnations>(engine: E, key: &str) -> Self {
        let host = Host::new();
        let engine = Rc::new(engine);
        let log = Rc::<RefCell<Vec<String>>>::default();
        let mut seeders = Vec::new();
        let micros =
            [("a", "b", [103, 104]), ("b", "a", [105, 106])].map(|(identity, peer, prefix)| {
                let registrations =
                    bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), prefix);
                let writer = seeder(&host, &format!("seeder-{identity}"), &registrations);
                writer
                    .put_value(
                        cbor_text(key),
                        clay_collection::WriteBasis::Concurrent,
                        crate::collections::CollectionValue {
                            body: cbor_text(key),
                            attachments: vec![RowAttachment::Endpoint(PublicationEndpoint {
                                micro: MicroId::new(peer),
                                publication: "echo".to_owned(),
                                contract: Nsid::literal("org.example.clay.test.context.echo"),
                            })],
                        },
                    )
                    .unwrap();
                seeders.push(writer);
                ConfiguredMicro {
                    identity: MicroId::new(identity),
                    eager: false,
                    enabled: true,
                    requirements: BTreeMap::new(),
                    unresolved_bindings: Vec::new(),
                    factory: Rc::new(Peer::<E> {
                        engine: Rc::clone(&engine),
                        identity,
                        registrations,
                        log: Rc::clone(&log),
                    }) as _,
                }
            });
        let supervisor = Rc::new(Supervisor::<CapabilityPublication>::new(
            host.clone(),
            micros,
        ));
        let serving = tokio::task::spawn_local({
            let supervisor = Rc::clone(&supervisor);
            async move { supervisor.drive().await }
        })
        .abort_handle();
        let recipient_instance = host.start("recipient");
        let recipient = CapabilitySession::new(
            host.clone(),
            recipient_instance.clone(),
            CapabilityLimits::default(),
        );
        Self {
            host,
            supervisor,
            serving,
            log,
            recipient_instance,
            recipient,
            _seeders: seeders,
        }
    }

    async fn finish(self) {
        self.serving.abort();
        self.supervisor.shutdown().await.unwrap();
        self.host.close_instance(&self.recipient_instance).unwrap();
        self.host.close();
    }
}

/// ADR 030 validation 8 through guests. Two `context` micros each retain the other's `echo` call
/// (`hold`, through a lazy reference minted by the incarnation, so the lease is held by it).
/// While an outside lease reaches either, both stay up, and a lease on only one of them keeps the
/// other, which it retains, up too; once the last outside lease drops, the pair is a disconnected
/// cycle and closes together, one initialization and one close each.
pub async fn context_peers_retaining_each_other_close_together<E: Incarnations>(engine: E) {
    let rig = PeerRig::boot(engine, "peer").await;
    let supervisor = &rig.supervisor;
    let (a, b) = (MicroId::new("a"), MicroId::new("b"));
    let hold = async |micro: &MicroId| {
        let lease = supervisor.acquire(micro, "hold").await.unwrap();
        let replies = rig
            .recipient
            .mint(&lease)
            .unwrap()
            .call(cbor_text("peer"))
            .await
            .unwrap();
        assert_eq!(
            ciborium::from_reader::<Vec<String>, _>(replies.as_slice()).unwrap(),
            ["one", "two"]
        );
        lease
    };
    let lease_a = hold(&a).await;
    assert_eq!(
        supervisor.state(&b),
        MicroState::Active,
        "a retains b's call, which started b"
    );
    let lease_b = hold(&b).await;

    // Either outside lease reaches both: a's reaches b through a's binding, and b's reaches a
    // through b's.
    drop(lease_a);
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(supervisor.state(&a), MicroState::Active);
    assert_eq!(supervisor.state(&b), MicroState::Active);
    assert_eq!(*rig.log.borrow(), ["initialize a", "initialize b"]);

    drop(lease_b);
    supervisor
        .changes()
        .wait_for(|_| {
            [&a, &b]
                .into_iter()
                .all(|micro| supervisor.state(micro) == MicroState::Idle)
        })
        .await
        .unwrap();
    let log = rig.log.borrow().clone();
    assert_eq!(log[..2], ["initialize a", "initialize b"]);
    let mut closes = log[2..].to_vec();
    closes.sort();
    assert_eq!(closes, ["close a", "close b"], "one close each: {log:?}");
    rig.finish().await;
}

/// ADR 030 validation 8, the activation half, through guests: each `context` micro's `activate`
/// calls the other's `echo` before it returns (the `init` row), so the first activation demands
/// the second, which demands the first. The demand that would wait on its own staged outcome
/// fails as `Unavailable` in the guest, which refuses its activation, and the other's demand of
/// it fails the same way: both end faulted, nothing stays pending or live.
pub async fn context_peers_demanding_each_other_fail_as_unavailable<E: Incarnations>(engine: E) {
    let rig = PeerRig::boot(engine, "init").await;
    let supervisor = &rig.supervisor;
    let (a, b) = (MicroId::new("a"), MicroId::new("b"));
    let refused = supervisor
        .acquire(&a, "echo")
        .await
        .err()
        .expect("an activation cycle cannot complete");
    assert!(matches!(refused, StartError::Faulted(_)), "{refused:?}");
    for micro in [&a, &b] {
        let MicroState::Faulted(message) = supervisor.state(micro) else {
            panic!("{micro:?} is {:?}", supervisor.state(micro));
        };
        assert!(
            message.to_lowercase().contains("unavailable"),
            "{micro:?} refused with {message:?}"
        );
        assert_eq!(supervisor.instance(micro), None);
    }
    assert_eq!(*rig.log.borrow(), ["initialize a", "initialize b"]);
    rig.finish().await;
}

/// A row written by a call, after `initialize` returned its publication handles to the host,
/// still attaches the guest's own declared presentation: the host stores its endpoint. A name
/// the manifest does not declare is an invalid attachment.
async fn attach_own_publication(attach: &CapabilityRef, registrations: &BytesRegistrations) {
    let attach_call = |name: &str| {
        let mut request = Vec::new();
        ciborium::into_writer(name, &mut request).unwrap();
        attach.call(request)
    };
    attach_call("detail").await.unwrap();
    let late = registrations
        .snapshot()
        .into_iter()
        .find(|(key, _)| ciborium::from_reader::<String, _>(key.as_slice()).unwrap() == "late")
        .expect("the guest wrote its late row")
        .1
        .value
        .unwrap();
    assert_eq!(
        late.attachments,
        [RowAttachment::Endpoint(PublicationEndpoint {
            micro: MicroId::new("context"),
            publication: "detail".into(),
            contract: presentation_component(),
        })]
    );
    assert!(matches!(
        attach_call("nope").await,
        Err(CallError::Provider(message)) if message.contains("attachment")
    ));
}
