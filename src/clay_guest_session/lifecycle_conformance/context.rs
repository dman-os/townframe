//! The `context` fixture's scenarios (`src/clay_mdk/context_fixture.rs`): the authoring
//! `Context`'s collections, calls, slots and retained calls, against a host-side provider.
use super::*;

/// The fixture's `registrations` roles take `[role, key-prefix]` arguments and bind `a` and
/// `b` to `micro`'s grant; the prefix namespaces the keys the incarnation writes.
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
            ContextWorld::granted(&registrations, &supervisor),
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

fn cbor_text(text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(text, &mut bytes).unwrap();
    bytes
}

/// A running `context` fixture beside a lazy host-side provider micro: the fixture's `feed`
/// collection holds one row, `key`, attaching the provider's publication `publication` (of
/// `contract`) as a lazy reference, and the supervisor serves its demand. `recipient` is the
/// host's handle for calling the fixture's publications.
struct ContextRig<E: Incarnations> {
    host: Host,
    registrations: BytesRegistrations,
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
    async fn boot(
        engine: E,
        prefix: [u64; 2],
        key: &str,
        publication: &str,
        contract: Nsid,
    ) -> Self {
        let host = Host::new();
        let registrations = bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), prefix);
        let provider = MicroId::new("provider");
        CollectionSession::new(
            host.clone(),
            host.start("seeder"),
            crate::collections::CollectionLimits::default(),
        )
        .grant_writer(clay_host::Grant {
            collection: registrations.clone(),
            micro: MicroId::new("seeder"),
            writer: writer_of("seeder"),
        })
        .unwrap()
        .put_value(
            cbor_text(key),
            clay_collection::WriteBasis::Concurrent,
            crate::collections::CollectionValue {
                body: cbor_text(key),
                attachments: vec![RowAttachment::Endpoint(PublicationEndpoint {
                    micro: provider.clone(),
                    publication: publication.to_owned(),
                    contract,
                })],
            },
        )
        .unwrap();
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
                ContextWorld::granted(&registrations, &supervisor),
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
struct ContextWorld {
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
const ECHO_CONTRACT: &str = "org.example.clay.test.context.echo";

impl ContextWorld {
    /// The `feed` registration collection and the publications the fixture's manifest declares,
    /// with `supervisor` behind the lazy references its mirror mints.
    fn granted(
        registrations: &BytesRegistrations,
        supervisor: &Supervisor<CapabilityPublication>,
    ) -> InitialContext {
        let call = |contract: &str| DeclaredPublication {
            kind: PublicationKind::Call,
            contract: Nsid::literal(contract),
        };
        let presentation = || DeclaredPublication {
            kind: PublicationKind::Presentation,
            contract: presentation_component(),
        };
        InitialContext {
            collections: BTreeMap::from([("feed".to_owned(), registrations.projection())]),
            registrations: BTreeMap::from([(
                "feed".to_owned(),
                clay_host::Grant {
                    collection: registrations.clone(),
                    micro: MicroId::new("context"),
                    writer: writer_of("context"),
                },
            )]),
            publications: BTreeMap::from([
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
                ("detail".to_owned(), presentation()),
                ("slot".to_owned(), presentation()),
            ]),
            micro: Some(MicroId::new("context")),
            activator: Some(supervisor.activator()),
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
    let rig = ContextRig::boot(engine, [97, 98], "view", "view", presentation_component()).await;
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
    let rig = ContextRig::boot(
        engine,
        [99, 100],
        "echo",
        "echo",
        Nsid::literal(ECHO_CONTRACT),
    )
    .await;
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
