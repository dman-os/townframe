use super::*;
use clay_collection::Stamped;
use clay_dioxus::NativeComponents;
use clay_guest_session::capabilities::RowAttachment;
use clay_host::StartError;
use clay_mdk::host::factory;
use clay_mdk::Capability;
use clay_mdk::Error;
use micro_settings_manifest::Section;
use micro_settings_manifest::SECTIONS;
use std::cell::RefCell;
use std::future::Future;

const COUNTER: &str = micro_counter_manifest::ID;

fn sections() -> Nsid {
    Nsid::literal(SECTIONS)
}

/// The id a built-in micro named `micro` is installed as.
fn id(micro: &str) -> MicroId {
    MicroId::local(micro)
}

/// The supervisor's name for the same micro.
fn supervised(micro: &str) -> SupervisorId {
    SupervisorId::from(&id(micro))
}

fn cbor_key(key: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(key, &mut bytes).unwrap();
    bytes
}

/// Settings (eager, locked) and Sample (eager) as the desktop links them, plus a counter whose
/// guest is `counter`: a catalog entry like any other.
fn catalog(counter: InstalledMicro) -> Vec<InstalledMicro> {
    let settings = InstalledMicro {
        eager: true,
        locked: true,
        ..InstalledMicro::new(
            id(micro_settings_manifest::ID),
            micro_settings_manifest::sidecar(),
            |id, resources, bindings| {
                factory(
                    resources,
                    id,
                    micro_settings_manifest::manifest(),
                    bindings,
                    micro_settings::activate,
                )
            },
        )
    };
    let sample = InstalledMicro {
        eager: true,
        enabled_on_admission: true,
        ..InstalledMicro::new(
            id(micro_sample::ID),
            micro_sample::sidecar(),
            |id, resources, bindings| {
                factory(
                    resources,
                    id,
                    micro_sample::manifest(),
                    bindings,
                    micro_sample::activate,
                )
            },
        )
    };
    vec![settings, sample, counter]
}

/// A counter that publishes its declared view without any engine.
fn stub_counter() -> InstalledMicro {
    InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(
            id(micro_counter_manifest::ID),
            micro_counter_manifest::sidecar(),
            |id, resources, bindings| {
                factory(
                    resources,
                    id,
                    micro_counter_manifest::manifest(),
                    bindings,
                    |context| async move {
                        context.publish_component(micro_counter_manifest::VIEW, || {
                            rsx! { p { "stub counter" } }
                        })?;
                        Ok(())
                    },
                )
            },
        )
    }
}

/// A counter whose guest cannot be produced (fetch, compile, missing artifact).
#[derive(Default)]
struct Unfetchable {
    /// How many times the supervisor tried to start it.
    attempts: Rc<std::cell::Cell<usize>>,
}

impl Factory<CapabilityPublication> for Unfetchable {
    fn initialize(
        &self,
        _host: Host,
        _instance: Instance,
        _requirements: BTreeMap<String, CapabilityPublication>,
        _activator: clay_host::Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<clay_host::Initialized<CapabilityPublication>, String>> {
        self.attempts.set(self.attempts.get() + 1);
        Box::pin(async { Err("the package could not be fetched".to_owned()) })
    }
}

fn unfetchable_counter(attempts: Rc<std::cell::Cell<usize>>) -> InstalledMicro {
    InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(
            id(micro_counter_manifest::ID),
            micro_counter_manifest::sidecar(),
            move |_id, _resources, _bindings| {
                Rc::new(Unfetchable {
                    attempts: Rc::clone(&attempts),
                })
            },
        )
    }
}

/// The catalog over a fresh in-memory store.
async fn memory_runtime(counter: InstalledMicro) -> CompositionRuntime {
    CompositionRuntime::new(Arc::new(clay_store::MemoryStore::new()), catalog(counter))
        .await
        .expect("a memory store does not fail")
}

/// Wait until `done` holds, woken by the supervisor's change ticks: every state `done` reads
/// is a micro's recorded state, which ticks after the mirror it reads was updated. The receiver
/// is taken before the first check, so a change between the two cannot be missed.
async fn settle(runtime: &CompositionRuntime, done: impl Fn() -> bool) {
    runtime
        .supervisor
        .changes()
        .wait_for(|_| done())
        .await
        .unwrap();
}

/// Serve `runtime` beside `scenario`, then shut it down in order. The scenario runs in its own
/// `LocalSet`, so the mirrors it spawns are not part of the composition's.
async fn served<T>(runtime: &CompositionRuntime, scenario: impl Future<Output = T>) -> T {
    tokio::task::LocalSet::new()
        .run_until(async {
            let serving = runtime.serve();
            tokio::pin!(serving);
            let out = tokio::select! {
                () = &mut serving => panic!("the composition stopped before shutdown"),
                out = scenario => out,
            };
            runtime.request_shutdown();
            serving.await;
            out
        })
        .await
}

fn section_refs(
    runtime: &CompositionRuntime,
) -> (
    Collection<String, Section>,
    impl Future<Output = ()> + 'static,
) {
    Collection::observe(
        &runtime.host,
        runtime.shell(),
        runtime.supervisor.activator(),
        &runtime.collection(&sections()).unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn manifest_rows_are_installed_before_anything_runs() {
    let runtime = memory_runtime(stub_counter()).await;
    assert!(runtime.installation_errors.is_empty());
    for micro in runtime.micros() {
        assert_eq!(runtime.state(micro), MicroState::Idle, "{micro}");
    }
    let Stamped { value, .. } = runtime
        .collection(&sections())
        .unwrap()
        .get(&cbor_key(COUNTER))
        .unwrap();
    let row = value.unwrap();
    let section: Section = ciborium::from_reader(row.body.as_slice()).unwrap();
    assert_eq!(section.title, "Counter (Wasm)");
    assert!(matches!(
        row.attachments.as_slice(),
        [RowAttachment::Endpoint(endpoint)] if endpoint.micro == supervised(COUNTER) && endpoint.publication == "view"
    ));
    // The sample writes its row at runtime, so nothing exists for it yet.
    assert!(runtime
        .collection(&sections())
        .unwrap()
        .get(&cbor_key(micro_sample::ID))
        .is_none());
    runtime.host.close();
}

#[tokio::test(flavor = "current_thread")]
async fn eager_roots_start_and_a_section_starts_its_micro_only_while_mounted() {
    let runtime = memory_runtime(stub_counter()).await;
    served(&runtime, async {
        settle(&runtime, || {
            runtime.state(&id("dayframe.settings")) == MicroState::Active
                && runtime.state(&id(micro_sample::ID)) == MicroState::Active
        })
        .await;
        // The shell mounts the root through its manifest-declared endpoint.
        let mut roots = runtime.roots.clone();
        while runtime.roots.rows().unwrap().is_empty() {
            roots.changed().await.unwrap();
        }
        let root = &runtime.roots.rows().unwrap()[0];
        assert_eq!(root.key, "dayframe.settings");
        assert_eq!(root.value.title, "Settings");
        let view = root.attachments[root.value.content.index()]
            .redeem_presentation()
            .await
            .unwrap();
        assert!(view.component().is_ok());

        let (sections, feed) = section_refs(&runtime);
        tokio::task::spawn_local(feed);
        let mut changes = sections.clone();
        while sections.rows().unwrap().len() < 2 {
            changes.changed().await.unwrap();
        }
        let rows = sections.rows().unwrap();
        let counter = rows.iter().find(|row| row.key == COUNTER).unwrap();
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Idle);
        let detail = counter.attachments[counter.value.detail.index()]
            .redeem_presentation()
            .await
            .unwrap();
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Active);
        drop(detail);
        settle(&runtime, || runtime.state(&id(COUNTER)) == MicroState::Idle).await;
        // The runtime-written sample row is bound, so it needs no activation to mount.
        let sample = rows.iter().find(|row| row.key == micro_sample::ID).unwrap();
        assert!(sample.attachments[0].redeem_presentation().await.is_ok());
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_disabled_micro_keeps_its_row_but_cannot_be_redeemed_until_enabled() {
    let runtime = memory_runtime(stub_counter()).await;
    served(&runtime, async {
        let (sections, feed) = section_refs(&runtime);
        tokio::task::spawn_local(feed);
        let mut changes = sections.clone();
        while sections.rows().unwrap().is_empty() {
            changes.changed().await.unwrap();
        }
        let supervisor = &runtime.supervisor;
        supervisor.disable(&supervised(COUNTER)).await.unwrap();
        let rows = sections.rows().unwrap();
        let counter = rows.iter().find(|row| row.key == COUNTER).unwrap();
        let reference = &counter.attachments[0];
        assert_eq!(
            reference.redeem_presentation().await.err(),
            Some(Error::Unavailable)
        );
        supervisor.enable(&supervised(COUNTER));
        assert!(reference.redeem_presentation().await.is_ok());
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Active);
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_unfetchable_guest_faults_alone_until_retried() {
    let runtime = memory_runtime(unfetchable_counter(Rc::default())).await;
    served(&runtime, async {
        let (sections, feed) = section_refs(&runtime);
        tokio::task::spawn_local(feed);
        let mut changes = sections.clone();
        while sections.rows().unwrap().len() < 2 {
            changes.changed().await.unwrap();
        }
        let rows = sections.rows().unwrap();
        let counter = rows.iter().find(|row| row.key == COUNTER).unwrap();
        assert_eq!(
            counter.attachments[0].redeem_presentation().await.err(),
            Some(Error::Unavailable)
        );
        assert!(matches!(
            runtime.state(&id(COUNTER)),
            MicroState::Faulted(message) if message.contains("fetched")
        ));
        assert_eq!(runtime.state(&id("dayframe.settings")), MicroState::Active);
        assert_eq!(runtime.state(&id(micro_sample::ID)), MicroState::Active);
        // Demand does not retry a fault; the retry control does, and it fails again.
        let supervisor = &runtime.supervisor;
        assert!(matches!(
            supervisor.acquire(&supervised(COUNTER), "view").await.err(),
            Some(StartError::Faulted(_))
        ));
        supervisor.retry(&supervised(COUNTER));
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Idle);
        assert!(counter.attachments[0].redeem_presentation().await.is_err());
        assert!(matches!(
            runtime.state(&id(COUNTER)),
            MicroState::Faulted(_)
        ));
    })
    .await;
}

/// A package the admission gate refuses is dropped whole with one installation error naming
/// the defect: nothing it declares reaches the registry and nothing of it is configured.
async fn refused(mutate: impl FnOnce(&mut clay_mdk::Sidecar)) -> (CompositionRuntime, String) {
    let mut sidecar = micro_counter_manifest::sidecar();
    mutate(&mut sidecar);
    let counter = InstalledMicro::new(id(&sidecar.id), sidecar, |_id, _resources, _bindings| {
        Rc::new(Unfetchable::default())
    });
    let runtime = memory_runtime(counter).await;
    assert_eq!(runtime.installation_errors.len(), 1);
    let error = runtime.installation_errors[0].clone();
    assert!(!runtime.micros().contains(&id(COUNTER)));
    (runtime, error)
}

#[tokio::test(flavor = "current_thread")]
async fn a_contribution_naming_an_unknown_binding_is_an_installation_error() {
    let (runtime, error) = refused(|sidecar| {
        sidecar.manifest.contributions[0].binding = "nobody.declared.this".into()
    })
    .await;
    assert!(error.contains("undeclared collection binding"), "{error}");
    runtime.host.close();
}

#[tokio::test(flavor = "current_thread")]
async fn a_contribution_through_a_read_only_binding_is_an_installation_error() {
    let (runtime, error) = refused(|sidecar| {
        sidecar.manifest.collection_requests[0].access = Some(clay_mdk::Access::Read)
    })
    .await;
    assert!(error.contains("does not request write access"), "{error}");
    runtime.host.close();
}

#[tokio::test(flavor = "current_thread")]
async fn a_declaration_outside_the_packages_authority_is_an_installation_error() {
    let (runtime, error) = refused(|sidecar| {
        let hijack = |name: &str| Nsid::literal(&format!("org.example.dayframe.settings.{name}"));
        sidecar
            .manifest
            .collection_contracts
            .push(clay_mdk::CollectionContract {
                id: hijack("hijack"),
                selector_schema: hijack("hijackSelector"),
                key_schema: hijack("hijackKey"),
                value_schema: hijack("hijackValue"),
                write_context_schema: None,
                offered: vec![clay_mdk::CollectionCapability::Read],
            })
    })
    .await;
    assert!(error.contains("outside the package's authority"), "{error}");
    runtime.host.close();
}

// Manifest rows persist in the store, stamps included. Boot 2 changes the row, so its lamport
// moves above boot 1's only if boot 1's state was loaded; boot 3 reinstalls the same row and
// must keep that lamport instead of recreating the row at lamport 1.
#[tokio::test(flavor = "current_thread")]
async fn a_reboot_over_the_same_sqlite_file_keeps_manifest_row_stamps() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.sqlite");
    // Boots over the file and returns the counter section row's stamp. The sqlite runtime
    // owns the workers and lives exactly as long as the composition; the host closes before
    // either is dropped (ordered shutdown).
    let boot = |title: &'static str| {
        let path = path.clone();
        async move {
            let sqlite = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default());
            let store = clay_store::SqliteStore::open(sqlite.database(
                &path,
                &[],
                "_dayframe_composition_test_unused",
            ))
            .await
            .unwrap();
            let mut sidecar = micro_counter_manifest::sidecar();
            sidecar.manifest.contributions[0].payload["title"] = title.into();
            let runtime = CompositionRuntime::new(
                Arc::new(store),
                catalog(InstalledMicro::new(
                    id(&sidecar.id),
                    sidecar,
                    |_id, _resources, _bindings| Rc::new(Unfetchable::default()),
                )),
            )
            .await
            .unwrap();
            assert!(runtime.installation_errors.is_empty());
            let row = runtime
                .collection(&sections())
                .unwrap()
                .get(&cbor_key(COUNTER))
                .expect("the manifest row is installed at boot");
            runtime.host.close();
            row.stamp
        }
    };

    let first = boot("Counter").await;
    let second = boot("Counter, renamed").await;
    let third = boot("Counter, renamed").await;
    assert_eq!(first.lamport, 1);
    assert_eq!(second.lamport, 2);
    assert_eq!(third, second);
}

// The replica identity is generated when a store is first opened and persisted in it, so every
// boot over that store has the same one (a changed replica would restamp unchanged rows), while
// another store is another replica. Durable rows are stamped with (replica, micro id).
#[tokio::test(flavor = "current_thread")]
async fn the_replica_id_is_stable_across_boots_and_stamps_rows_with_the_micro_id() {
    let boot = |store: &Arc<clay_store::MemoryStore>| {
        let store = Arc::clone(store);
        async move {
            let runtime = CompositionRuntime::new(store, catalog(stub_counter()))
                .await
                .unwrap();
            let replica = *runtime.replica();
            let stamp = runtime
                .collection(&sections())
                .unwrap()
                .get(&cbor_key(COUNTER))
                .expect("the manifest row is installed at boot")
                .stamp;
            runtime.host.close();
            (replica, stamp)
        }
    };
    let store = Arc::new(clay_store::MemoryStore::new());
    let (first, first_stamp) = boot(&store).await;
    let (second, second_stamp) = boot(&store).await;
    let (other, _) = boot(&Arc::new(clay_store::MemoryStore::new())).await;
    assert_eq!(first, second);
    assert_ne!(first, other);
    assert_eq!(first_stamp, second_stamp, "unchanged rows keep their stamp");
    assert_eq!(
        first_stamp.writer.as_bytes(),
        [first.as_bytes().as_slice(), id(COUNTER).as_str().as_bytes()].concat(),
        "a writer is the replica's bytes followed by the micro id's"
    );
}

// Documents live in the same store as collections but in their own subspace. Boot 1 changes a
// document and runs the composition until the persister has written it; boot 2 over the same
// store sees the value, and its seed (which a fresh install would apply) does not overwrite it.
#[tokio::test(flavor = "current_thread")]
async fn a_document_written_in_one_boot_is_read_in_the_next() {
    use clay_docs::DocumentGrant;
    use clay_docs::FacetRoute;
    use clay_docs::FacetSelection;
    use clay_docs::FacetValue;
    use clay_docs::WatchSelection;
    let store = Arc::new(clay_store::MemoryStore::new());
    let route = FacetRoute {
        document: "notes".into(),
        branch: "main".into(),
        tag: "org.example.note".into(),
        key_id: "main".into(),
    };
    let boot = || async {
        CompositionRuntime::new(
            Arc::clone(&store),
            catalog(InstalledMicro::new(
                id(micro_counter_manifest::ID),
                micro_counter_manifest::sidecar(),
                |_, _, _| Rc::new(Unfetchable::default()),
            )),
        )
        .await
        .unwrap()
    };

    let first = boot().await;
    first
        .documents()
        .mock_documents()
        .external_change(route.clone(), Some(serde_json::json!("from boot one")));
    // Orderly shutdown drains the persister, so the write is in the store once `serve` returns.
    first.request_shutdown();
    first.serve().await;

    let second = boot().await;
    second
        .documents()
        .mock_documents()
        .seed(route.clone(), serde_json::json!("seed"));
    let instance = second.host.start("observer");
    let access = second
        .documents()
        .grant_documents(
            &second.host,
            &instance,
            vec![DocumentGrant {
                document: "notes".into(),
                branch: "main".into(),
                facets: vec![daybook_types::manifest::RoutineFacetAccess {
                    tag: "org.example.note".into(),
                    read: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let mirror = access
        .watch(WatchSelection {
            documents: ["notes".into()].into(),
            branch: "main".into(),
            facets: FacetSelection::Tags(["org.example.note".into()].into()),
        })
        .unwrap();
    assert_eq!(
        mirror.current().unwrap().facets.get(&route),
        Some(&FacetValue::Present(serde_json::json!("from boot one")))
    );
    second.host.close();
}

fn install_datum_of(runtime: &CompositionRuntime, micro: &str) -> Option<serde_json::Value> {
    runtime
        .documents()
        .mock_documents()
        .get(&install_route(&id(micro)))
}

// A micro nobody enabled is admitted disabled (an incoming package), a built-in the launcher
// links is admitted enabled, and the locked root has no datum at all.
#[tokio::test(flavor = "current_thread")]
async fn admission_records_a_package_disabled_and_a_built_in_enabled() {
    let package = InstalledMicro::new(
        id(micro_counter_manifest::ID),
        micro_counter_manifest::sidecar(),
        |_id, _resources, _bindings| Rc::new(Unfetchable::default()),
    );
    let runtime = memory_runtime(package).await;
    let version = micro_counter_manifest::PACKAGE_VERSION;
    assert_eq!(
        install_datum_of(&runtime, COUNTER),
        Some(serde_json::json!({ "enabled": false, "package_version": version }))
    );
    assert_eq!(
        install_datum_of(&runtime, micro_sample::ID).unwrap()["enabled"],
        true
    );
    assert_eq!(install_datum_of(&runtime, "dayframe.settings"), None);
    assert_eq!(runtime.state(&id(COUNTER)), MicroState::Disabled);
    assert_eq!(runtime.state(&id(micro_sample::ID)), MicroState::Idle);
    assert_eq!(runtime.state(&id("dayframe.settings")), MicroState::Idle);
    runtime.host.close();
}

// Writing the datum is the only control: the reconciler applies it to the supervisor, and an
// eager micro that was admitted disabled is never started by the boot reconcile.
#[tokio::test(flavor = "current_thread")]
async fn the_install_datum_drives_the_supervisor() {
    let runtime = memory_runtime(InstalledMicro::new(
        id(micro_counter_manifest::ID),
        micro_counter_manifest::sidecar(),
        |_id, _resources, _bindings| Rc::new(Unfetchable::default()),
    ))
    .await;
    served(&runtime, async {
        let sample = id(micro_sample::ID);
        settle(&runtime, || runtime.state(&sample) == MicroState::Active).await;
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Disabled);
        runtime.set_enabled(&sample, false).await;
        settle(&runtime, || runtime.state(&sample) == MicroState::Disabled).await;
        runtime.set_enabled(&sample, true).await;
        settle(&runtime, || runtime.state(&sample) == MicroState::Idle).await;
        runtime.set_enabled(&id(COUNTER), true).await;
        settle(&runtime, || runtime.state(&id(COUNTER)) == MicroState::Idle).await;
        assert_eq!(
            install_datum_of(&runtime, COUNTER).unwrap()["enabled"],
            true
        );
    })
    .await;
}

// The flag outlives the boot: an admitted-enabled built-in that was disabled stays disabled
// after a restart over the same store, because the stored datum beats the admission default.
#[tokio::test(flavor = "current_thread")]
async fn a_disabled_micro_stays_disabled_across_a_reboot() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let boot = || async {
        CompositionRuntime::new(Arc::clone(&store), catalog(stub_counter()))
            .await
            .unwrap()
    };
    let first = boot().await;
    let sample = id(micro_sample::ID);
    served(&first, async {
        settle(&first, || first.state(&sample) == MicroState::Active).await;
        first.set_enabled(&sample, false).await;
        settle(&first, || first.state(&sample) == MicroState::Disabled).await;
    })
    .await;

    let second = boot().await;
    assert_eq!(second.state(&sample), MicroState::Disabled);
    assert_eq!(
        install_datum_of(&second, micro_sample::ID).unwrap()["enabled"],
        false
    );
    assert_eq!(install_datum_of(&second, "dayframe.settings"), None);
    assert_eq!(second.state(&id("dayframe.settings")), MicroState::Idle);
    second.host.close();
}

// ── collection binding resolution (ADR 031 §11) ──────────────────────────────────────────────

const ROWS: &str = "org.example.test.rows";
const MISSING: &str = "org.example.test.missing";
const BOOK: &str = "book";
const PROBE: &str = "tests.probe";

/// A test package holding the `org.example.test` authority, which every contract the probes
/// declare lies under.
fn probe_sidecar(
    micro: &str,
    contracts: &[&str],
    requests: Vec<clay_mdk::Request>,
) -> clay_mdk::Sidecar {
    clay_mdk::Sidecar::new(
        micro,
        1,
        clay_mdk::MicroManifest {
            entrypoints: clay_mdk::Entrypoints::micro_client(),
            publications: vec![],
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: contracts
                .iter()
                .map(|contract| clay_mdk::CollectionContract {
                    id: Nsid::literal(contract),
                    selector_schema: Nsid::literal("org.example.test.selector"),
                    key_schema: Nsid::literal("org.example.test.key"),
                    value_schema: Nsid::literal("org.example.test.value"),
                    write_context_schema: None,
                    offered: vec![clay_mdk::CollectionCapability::Read],
                })
                .collect(),
            collection_requests: requests,
            contributions: vec![],
            extra: BTreeMap::new(),
        },
    )
    .with_authority("org.example.test")
}

/// Declares the `tests.rows` collection and nothing else.
fn rows_owner() -> InstalledMicro {
    let sidecar = probe_sidecar("tests.owner", &[ROWS], vec![]);
    let manifest = sidecar.manifest.clone();
    InstalledMicro::new(id(&sidecar.id), sidecar, |id, resources, bindings| {
        factory(resources, id, manifest, bindings, |_| async { Ok(()) })
    })
}

/// An eager micro requesting read access to `BOOK` of `contract`; each activation reports
/// whether the binding was bound to it.
fn probe(
    contract: &str,
    necessity: clay_mdk::Necessity,
) -> (InstalledMicro, watch::Receiver<Vec<bool>>) {
    probe_asking(contract, necessity, clay_mdk::Access::Read)
}

/// [`probe`] asking for `access`.
fn probe_asking(
    contract: &str,
    necessity: clay_mdk::Necessity,
    access: clay_mdk::Access,
) -> (InstalledMicro, watch::Receiver<Vec<bool>>) {
    let sidecar = probe_sidecar(
        PROBE,
        &[],
        vec![clay_mdk::Request {
            binding: BOOK.into(),
            contract: Nsid::literal(contract),
            access: Some(access),
            necessity,
            resolution: clay_mdk::BindingResolution::Configured,
            min_revision: None,
        }],
    );
    let manifest = sidecar.manifest.clone();
    let (activations, seen) = watch::channel(Vec::new());
    let micro = InstalledMicro {
        eager: true,
        enabled_on_admission: true,
        ..InstalledMicro::new(id(&sidecar.id), sidecar, move |id, resources, bindings| {
            factory(resources, id, manifest, bindings, move |context| {
                let bound = context.collection::<String, String>(BOOK).is_some();
                activations.send_modify(|seen| seen.push(bound));
                async { Ok(()) }
            })
        })
    };
    (micro, seen)
}

async fn probe_runtime(
    store: &Arc<clay_store::MemoryStore>,
    probe: InstalledMicro,
) -> CompositionRuntime {
    let mut micros = catalog(stub_counter());
    micros.push(rows_owner());
    micros.push(probe);
    CompositionRuntime::new(Arc::clone(store), micros)
        .await
        .unwrap()
}

/// Wait for the supervisor to record `state` for `micro`, woken by its change ticks.
async fn reaches(runtime: &CompositionRuntime, micro: &MicroId, state: MicroState) {
    settle(runtime, || runtime.state(micro) == state).await;
}

async fn activations(seen: &mut watch::Receiver<Vec<bool>>, count: usize) -> Vec<bool> {
    seen.wait_for(|seen| seen.len() >= count)
        .await
        .unwrap()
        .clone()
}

#[tokio::test(flavor = "current_thread")]
async fn a_contract_with_one_declared_collection_binds_by_default() {
    let (probe, mut seen) = probe(ROWS, clay_mdk::Necessity::Required);
    let runtime = probe_runtime(&Arc::new(clay_store::MemoryStore::new()), probe).await;
    served(&runtime, async {
        assert_eq!(activations(&mut seen, 1).await, [true]);
        // The built-ins resolved their sections binding the same way.
        reaches(&runtime, &id(micro_sample::ID), MicroState::Active).await;
        reaches(&runtime, &id("dayframe.settings"), MicroState::Active).await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_required_binding_that_resolves_to_nothing_is_never_started() {
    let (probe, seen) = probe(MISSING, clay_mdk::Necessity::Required);
    let runtime = probe_runtime(&Arc::new(clay_store::MemoryStore::new()), probe).await;
    let micro = id(PROBE);
    let unconfigured = MicroState::Unconfigured(vec![BOOK.into()]);
    assert_eq!(runtime.state(&micro), unconfigured);
    served(&runtime, async {
        // Eager reconcile has run once the other eager roots are up.
        reaches(&runtime, &id(micro_sample::ID), MicroState::Active).await;
        assert_eq!(runtime.state(&micro), unconfigured);
        // Redeeming one of its publications is refused, not a fault.
        let refused = runtime
            .supervisor
            .acquire(&SupervisorId::from(&micro), "none")
            .await
            .err()
            .unwrap();
        assert_eq!(
            refused,
            StartError::Unconfigured(SupervisorId::from(&micro), vec![BOOK.into()])
        );
        assert_eq!(runtime.state(&micro), unconfigured);
    })
    .await;
    assert!(seen.borrow().is_empty(), "the micro was started");
}

#[tokio::test(flavor = "current_thread")]
async fn an_optional_binding_that_resolves_to_nothing_starts_with_the_binding_absent() {
    let (probe, mut seen) = probe(MISSING, clay_mdk::Necessity::Optional);
    let runtime = probe_runtime(&Arc::new(clay_store::MemoryStore::new()), probe).await;
    served(&runtime, async {
        assert_eq!(activations(&mut seen, 1).await, [false]);
        reaches(&runtime, &id(PROBE), MicroState::Active).await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn changing_the_binding_datum_restarts_the_micro_with_the_new_binding() {
    let (probe, mut seen) = probe(ROWS, clay_mdk::Necessity::Optional);
    let runtime = probe_runtime(&Arc::new(clay_store::MemoryStore::new()), probe).await;
    let micro = id(PROBE);
    served(&runtime, async {
        assert_eq!(activations(&mut seen, 1).await, [true]);
        // An explicit value is final even where the default would bind.
        runtime.set_binding(&micro, BOOK, None).await;
        assert_eq!(activations(&mut seen, 2).await, [true, false]);
        runtime.clear_binding(&micro, BOOK).await;
        assert_eq!(activations(&mut seen, 3).await, [true, false, true]);
        reaches(&runtime, &micro, MicroState::Active).await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn configuring_a_required_binding_starts_a_waiting_micro_and_unconfiguring_stops_it() {
    let (probe, mut seen) = probe(ROWS, clay_mdk::Necessity::Required);
    let runtime = probe_runtime(&Arc::new(clay_store::MemoryStore::new()), probe).await;
    let micro = id(PROBE);
    let unconfigured = MicroState::Unconfigured(vec![BOOK.into()]);
    served(&runtime, async {
        assert_eq!(activations(&mut seen, 1).await, [true]);
        runtime.set_binding(&micro, BOOK, None).await;
        reaches(&runtime, &micro, unconfigured.clone()).await;
        runtime.clear_binding(&micro, BOOK).await;
        assert_eq!(activations(&mut seen, 2).await, [true, true]);
        reaches(&runtime, &micro, MicroState::Active).await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_binding_datum_survives_a_reboot() {
    let store = Arc::new(clay_store::MemoryStore::new());
    let (first_probe, mut seen) = probe(ROWS, clay_mdk::Necessity::Required);
    let first = probe_runtime(&store, first_probe).await;
    let micro = id(PROBE);
    served(&first, async {
        assert_eq!(activations(&mut seen, 1).await, [true]);
        first.set_binding(&micro, BOOK, None).await;
        reaches(&first, &micro, MicroState::Unconfigured(vec![BOOK.into()])).await;
    })
    .await;

    let (second_probe, seen) = probe(ROWS, clay_mdk::Necessity::Required);
    let second = probe_runtime(&store, second_probe).await;
    assert_eq!(
        second.state(&micro),
        MicroState::Unconfigured(vec![BOOK.into()])
    );
    served(&second, async {
        reaches(&second, &id(micro_sample::ID), MicroState::Active).await;
    })
    .await;
    assert!(seen.borrow().is_empty(), "the micro was started");
}

// A configured binding records the grant it was made under. A package update that asks for more
// under the same binding name is unresolved until configured again, never inheriting the old
// consent; one that asks for the same or less keeps it. Enablement is the install datum's and is
// never touched by any of this.
#[tokio::test(flavor = "current_thread")]
async fn a_widened_request_is_unresolved_until_configured_again_and_enablement_is_untouched() {
    use clay_mdk::Access;
    let store = Arc::new(clay_store::MemoryStore::new());
    let micro = id(PROBE);
    let unconfigured = MicroState::Unconfigured(vec![BOOK.into()]);
    // Boots with a probe asking for `access`; `configure` grants the binding under that request.
    let boot = |access: Access, configure: bool| {
        let store = Arc::clone(&store);
        let micro = micro.clone();
        async move {
            let (probe, seen) = probe_asking(ROWS, clay_mdk::Necessity::Required, access);
            let runtime = probe_runtime(&store, probe).await;
            if configure {
                served(&runtime, async {
                    runtime
                        .set_binding(&micro, BOOK, Some(&Nsid::literal(ROWS)))
                        .await;
                })
                .await;
            }
            (runtime, seen)
        }
    };

    // Granted at write: the same request and a narrowed (read) one keep the binding.
    let (runtime, _) = boot(Access::Write, true).await;
    assert_eq!(runtime.state(&micro), MicroState::Idle);
    let (runtime, seen) = boot(Access::Read, false).await;
    assert_eq!(runtime.state(&micro), MicroState::Idle);
    assert!(seen.borrow().is_empty());

    // Re-granted at read; a package asking for write is now a widening.
    let (runtime, _) = boot(Access::Read, true).await;
    assert_eq!(runtime.state(&micro), MicroState::Idle);
    let (runtime, _) = boot(Access::Write, false).await;
    assert_eq!(runtime.state(&micro), unconfigured);
    assert!(runtime.is_enabled(&micro), "enablement is not the grant's");
    assert_eq!(
        install_datum_of(&runtime, PROBE).unwrap()["enabled"],
        true,
        "the install datum is untouched"
    );

    // Configuring again grants the widened request, and the waiting micro starts.
    let (probe, mut seen) = probe_asking(ROWS, clay_mdk::Necessity::Required, Access::Write);
    let runtime = probe_runtime(&store, probe).await;
    assert_eq!(runtime.state(&micro), unconfigured);
    served(&runtime, async {
        runtime
            .set_binding(&micro, BOOK, Some(&Nsid::literal(ROWS)))
            .await;
        assert_eq!(activations(&mut seen, 1).await, [true]);
    })
    .await;
}

#[derive(Clone, PartialEq, Props)]
struct SlotApp {
    components: NativeComponents,
    reference: Capability,
}

fn slot_app(props: SlotApp) -> Element {
    use_context_provider(|| props.components.clone());
    rsx! { PresentationSlot { reference: props.reference } }
}

/// A slot over the counter's section detail, rendered into a surface tree the way a shell
/// mounts it.
async fn counter_slot(runtime: &CompositionRuntime) -> (VirtualDom, clay_surface::SurfaceTree) {
    let (sections, feed) = section_refs(runtime);
    tokio::task::spawn_local(feed);
    let mut changes = sections.clone();
    while sections.rows().unwrap().is_empty() {
        changes.changed().await.unwrap();
    }
    let rows = sections.rows().unwrap();
    let counter = rows.iter().find(|row| row.key == COUNTER).unwrap();
    let mut dom = VirtualDom::new_with_props(
        slot_app,
        SlotApp {
            components: runtime.components.clone(),
            reference: counter.attachments[counter.value.detail.index()].clone(),
        },
    );
    let mut tree = clay_surface::SurfaceTree::new();
    dom.rebuild(&mut tree);
    (dom, tree)
}

/// Run the slot's tasks until its rendering contains `text`; each wake is a task or signal of
/// the slot, so a slot that never gets there hangs until the test runner's timeout.
async fn slot_shows(dom: &mut VirtualDom, tree: &mut clay_surface::SurfaceTree, text: &str) {
    loop {
        dom.render_immediate(tree);
        if tree.text_content().contains(text) {
            return;
        }
        dom.wait_for_work().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_slot_opened_on_a_disabled_micro_mounts_once_it_is_enabled() {
    let runtime = memory_runtime(stub_counter()).await;
    served(&runtime, async {
        runtime.set_enabled(&id(COUNTER), false).await;
        reaches(&runtime, &id(COUNTER), MicroState::Disabled).await;
        let (mut dom, mut tree) = counter_slot(&runtime).await;
        slot_shows(&mut dom, &mut tree, "Section unavailable").await;
        // The same slot, not a remounted one, picks the micro up.
        runtime.set_enabled(&id(COUNTER), true).await;
        slot_shows(&mut dom, &mut tree, "stub counter").await;
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Active);
        drop(dom);
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_mounted_slot_follows_its_micro_through_disable_enable_and_reconfigure() {
    let runtime = memory_runtime(stub_counter()).await;
    served(&runtime, async {
        let counter = id(COUNTER);
        let (mut dom, mut tree) = counter_slot(&runtime).await;
        slot_shows(&mut dom, &mut tree, "stub counter").await;
        runtime.set_enabled(&counter, false).await;
        slot_shows(&mut dom, &mut tree, "Section unavailable").await;
        assert_eq!(runtime.state(&counter), MicroState::Disabled);
        runtime.set_enabled(&counter, true).await;
        slot_shows(&mut dom, &mut tree, "stub counter").await;
        // A reconfiguration closes the incarnation; the slot re-redeems the next one, which
        // demand starts again without anyone remounting the slot.
        let first = runtime
            .supervisor
            .instance(&SupervisorId::from(&counter))
            .unwrap();
        runtime
            .supervisor
            .reconfigure(&SupervisorId::from(&counter), vec![])
            .await
            .unwrap();
        loop {
            dom.render_immediate(&mut tree);
            match runtime.supervisor.instance(&SupervisorId::from(&counter)) {
                Some(next) if next.scope_id() != first.scope_id() => break,
                _ => dom.wait_for_work().await,
            }
        }
        slot_shows(&mut dom, &mut tree, "stub counter").await;
        drop(dom);
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_slot_does_not_redeem_a_faulted_micro_again_until_it_is_retried() {
    let attempts = Rc::<std::cell::Cell<usize>>::default();
    let runtime = memory_runtime(unfetchable_counter(Rc::clone(&attempts))).await;
    served(&runtime, async {
        let counter = id(COUNTER);
        let (mut dom, mut tree) = counter_slot(&runtime).await;
        slot_shows(&mut dom, &mut tree, "Section unavailable").await;
        assert_eq!(attempts.get(), 1);
        // Further status ticks of other micros and renders do not redeem it again.
        for _ in 0..32 {
            tokio::task::yield_now().await;
            dom.render_immediate(&mut tree);
        }
        assert_eq!(attempts.get(), 1);
        runtime.supervisor.retry(&SupervisorId::from(&counter));
        while attempts.get() < 2 {
            dom.wait_for_work().await;
        }
        reaches(
            &runtime,
            &counter,
            MicroState::Faulted("the package could not be fetched".into()),
        )
        .await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
            dom.render_immediate(&mut tree);
        }
        assert_eq!(attempts.get(), 2);
        drop(dom);
    })
    .await;
}

/// The catalog the toolbar scenarios run: Settings, Sample and the Toolbar as the desktop links
/// them, and the real counter (its native `ACTIVATE` entry, the same source the Wasm guest is
/// built from), counting how many times it is activated.
fn toolbar_catalog(activations: Rc<std::cell::Cell<usize>>) -> Vec<InstalledMicro> {
    let counter = InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(
            id(micro_counter_manifest::ID),
            micro_counter_manifest::sidecar(),
            move |id, resources, bindings| {
                factory(
                    resources,
                    id,
                    micro_counter_manifest::manifest(),
                    bindings,
                    move |context| {
                        activations.set(activations.get() + 1);
                        micro_counter::ACTIVATE(context)
                    },
                )
            },
        )
    };
    let toolbar = InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(
            id(micro_toolbar_manifest::ID),
            micro_toolbar_manifest::sidecar(),
            |id, resources, bindings| {
                factory(
                    resources,
                    id,
                    micro_toolbar_manifest::manifest(),
                    bindings,
                    micro_toolbar::activate,
                )
            },
        )
    };
    let mut catalog = catalog(counter);
    catalog.push(toolbar);
    catalog
}

/// One call of `micro`'s call publication `name`, made by a fresh recipient that holds the
/// provider only for the length of the call.
async fn call_publication<Req: Serialize, Resp: serde::de::DeserializeOwned>(
    runtime: &CompositionRuntime,
    micro: &str,
    name: &str,
    request: &Req,
) -> Resp {
    let lease = runtime
        .supervisor
        .acquire(&supervised(micro), name)
        .await
        .unwrap();
    let recipient = runtime.host.start("caller");
    let reference = clay_guest_session::capabilities::CapabilitySession::new(
        runtime.host.clone(),
        recipient.clone(),
        clay_guest_session::capabilities::CapabilityLimits::default(),
    )
    .mint(&lease)
    .unwrap();
    let mut bytes = Vec::new();
    ciborium::into_writer(request, &mut bytes).unwrap();
    let response = reference.call(bytes).await.unwrap();
    drop((reference, lease));
    runtime.host.close_instance(&recipient).unwrap();
    ciborium::from_reader(response.as_slice()).unwrap()
}

// The counter's Increment button is a manifest-declared row whose action is a lazy endpoint:
// the toolbar view renders it, and `status()` reads it, without the counter ever starting. A
// press calls the attachment, which starts the counter once for the length of the call; the
// increment lands and the counter idles again.
#[tokio::test(flavor = "current_thread")]
async fn the_toolbar_renders_the_counters_button_while_it_is_stopped_and_a_press_starts_it_once() {
    let activations = Rc::<std::cell::Cell<usize>>::default();
    let runtime = CompositionRuntime::new(
        Arc::new(clay_store::MemoryStore::new()),
        toolbar_catalog(Rc::clone(&activations)),
    )
    .await
    .expect("a memory store does not fail");
    assert!(runtime.installation_errors.is_empty());
    runtime.documents().mock_documents().seed(
        clay_docs::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: micro_counter_manifest::CONFIG_TAG.into(),
            key_id: micro_counter_manifest::CONFIG_KEY.into(),
        },
        serde_json::json!(0),
    );
    served(&runtime, async {
        // The shell mounts the toolbar through its pinned root row.
        let mut roots = runtime.roots.clone();
        while runtime.roots.rows().unwrap().len() < 2 {
            roots.changed().await.unwrap();
        }
        let rows = runtime.roots.rows().unwrap();
        let root = rows
            .iter()
            .find(|row| row.key == micro_toolbar_manifest::ID)
            .unwrap();
        assert!(root.value.pinned);
        let toolbar = root.attachments[root.value.content.index()].clone();
        let mut dom = VirtualDom::new_with_props(
            slot_app,
            SlotApp {
                components: runtime.components.clone(),
                reference: toolbar,
            },
        );
        let mut tree = clay_surface::SurfaceTree::new();
        dom.rebuild(&mut tree);
        slot_shows(&mut dom, &mut tree, "Increment").await;

        // The button is rendered, and its lazy reference read, with the counter never started.
        let (buttons, feed) = Collection::<String, micro_toolbar_manifest::Button>::observe(
            &runtime.host,
            runtime.shell(),
            runtime.supervisor.activator(),
            &runtime
                .collection(&Nsid::literal(micro_toolbar_manifest::BUTTONS))
                .unwrap(),
        )
        .unwrap();
        tokio::task::spawn_local(feed);
        let mut changes = buttons.clone();
        while buttons.rows().unwrap().is_empty() {
            changes.changed().await.unwrap();
        }
        let rows = buttons.rows().unwrap();
        let [button] = &*rows else {
            panic!("only the counter contributes a button")
        };
        assert_eq!(button.key, micro_counter_manifest::INCREMENT);
        assert_eq!(button.value.label, "Increment");
        assert_eq!(button.value.icon, "plus");
        let action = &button.attachments[button.value.action.index()];
        assert_eq!(action.status(), clay_mdk::RefStatus::Dormant);
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Idle);
        assert_eq!(activations.get(), 0);

        // What the toolbar's press performs: one call of the attachment.
        action.call::<(), ()>(&()).await.unwrap();
        assert_eq!(activations.get(), 1);
        // Nothing holds the counter once the call returned, so idle shutdown follows, and the
        // increment outlives the incarnation that made it.
        settle(&runtime, || runtime.state(&id(COUNTER)) == MicroState::Idle).await;
        assert_eq!(
            call_publication::<(), u64>(&runtime, COUNTER, micro_counter_manifest::COUNT, &())
                .await,
            1
        );
        assert_eq!(
            activations.get(),
            2,
            "the count read is the second activation"
        );
        drop(dom);
    })
    .await;
}

// ── completion providers (ADR 031 §13), as a validation of the contribution pattern ──────────

const SOURCES: &str = "org.example.test.editor.sources";

/// One completion-source row: when the typed text starts with `trigger`, the provider the
/// `provider` attachment names is asked for candidates.
#[derive(Serialize, Deserialize)]
struct Source {
    trigger: String,
    provider: clay_mdk::Attachment,
}

fn editor_sidecar() -> clay_mdk::Sidecar {
    let mut sidecar = probe_sidecar(
        "tests.editor",
        &[SOURCES],
        vec![clay_mdk::Request {
            binding: "sources".into(),
            contract: Nsid::literal(SOURCES),
            access: Some(clay_mdk::Access::Read),
            necessity: clay_mdk::Necessity::Required,
            resolution: clay_mdk::BindingResolution::Configured,
            min_revision: None,
        }],
    );
    sidecar.manifest.publications = ["complete", "focus", "blur"]
        .into_iter()
        .map(|name| clay_mdk::PublicationDeclaration {
            name: name.into(),
            kind: clay_mdk::PublicationKind::Call,
            contract: Some(Nsid::literal(&format!("org.example.test.editor.{name}"))),
        })
        .collect();
    sidecar
}

/// The editor: it reads the completion-source rows, filters them locally on the typed text and
/// calls only the providers whose trigger matches, collecting their candidates. While focused
/// (`focus`) it holds a retained call per source row whose trigger the text matches, and a
/// completion goes through the retained call of a row when there is one; `blur` drops them.
fn editor() -> InstalledMicro {
    let sidecar = editor_sidecar();
    let manifest = sidecar.manifest.clone();
    InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(id(&sidecar.id), sidecar, |id, resources, bindings| {
            factory(resources, id, manifest, bindings, |context| async move {
                let sources = context
                    .collection::<String, Source>("sources")
                    .expect("configuration binds the sources collection");
                let retained = Rc::new(RefCell::new(
                    BTreeMap::<String, Rc<clay_mdk::CallBinding>>::new(),
                ));
                context.publish_call("focus", {
                    let sources = sources.clone();
                    let retained = Rc::clone(&retained);
                    move |typed: String| {
                        let sources = sources.clone();
                        let retained = Rc::clone(&retained);
                        async move {
                            sources.bootstrapped().await;
                            for row in sources.rows()?.iter() {
                                if typed.starts_with(&row.value.trigger) {
                                    let provider = &row.attachments[row.value.provider.index()];
                                    let binding = provider.retain_call().await?;
                                    retained
                                        .borrow_mut()
                                        .insert(row.key.clone(), Rc::new(binding));
                                }
                            }
                            Ok(())
                        }
                    }
                })?;
                context.publish_call("blur", {
                    let retained = Rc::clone(&retained);
                    move |(): ()| {
                        retained.borrow_mut().clear();
                        async { Ok(()) }
                    }
                })?;
                context.publish_call("complete", move |typed: String| {
                    let sources = sources.clone();
                    let retained = Rc::clone(&retained);
                    async move {
                        sources.bootstrapped().await;
                        let mut candidates = Vec::<String>::new();
                        for row in sources.rows()?.iter() {
                            if typed.starts_with(&row.value.trigger) {
                                // Clone the handle out: the map borrow must not span the await.
                                let binding = retained.borrow().get(&row.key).cloned();
                                candidates.extend(match binding {
                                    Some(binding) => {
                                        binding.call::<String, Vec<String>>(&typed).await?
                                    }
                                    None => {
                                        let provider = &row.attachments[row.value.provider.index()];
                                        provider.call::<String, Vec<String>>(&typed).await?
                                    }
                                });
                            }
                        }
                        Ok(candidates)
                    }
                })?;
                Ok(())
            })
        })
    }
}

/// What a provider records: how many incarnations started and how many calls it served.
#[derive(Clone, Default)]
struct Served {
    activations: Rc<std::cell::Cell<usize>>,
    calls: Rc<std::cell::Cell<usize>>,
}

/// A provider named `name` triggered by `trigger`: it contributes one completion-source row, a
/// lazy reference to its `complete` call, and answers `name:<typed>`.
fn provider(name: &'static str, trigger: &'static str) -> (InstalledMicro, Served) {
    let micro = format!("tests.{name}");
    let authority = format!("org.example.test.{name}");
    let mut sidecar = clay_mdk::Sidecar::new(
        micro.clone(),
        1,
        clay_mdk::MicroManifest {
            entrypoints: clay_mdk::Entrypoints::micro_client(),
            publications: vec![clay_mdk::PublicationDeclaration {
                name: "complete".into(),
                kind: clay_mdk::PublicationKind::Call,
                contract: Some(Nsid::literal(&format!("{authority}.complete"))),
            }],
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: vec![],
            collection_requests: vec![clay_mdk::Request {
                binding: "sources".into(),
                contract: Nsid::literal(SOURCES),
                access: Some(clay_mdk::Access::Write),
                necessity: clay_mdk::Necessity::Optional,
                resolution: clay_mdk::BindingResolution::Configured,
                min_revision: None,
            }],
            contributions: vec![clay_mdk::ContributionDeclaration {
                id: "source".into(),
                binding: "sources".into(),
                key: name.into(),
                payload: serde_json::json!({
                    "trigger": trigger,
                    "provider": { "$attachment": 0 },
                }),
                attachments: vec![clay_mdk::AttachmentDeclaration {
                    index: 0,
                    publication: "complete".into(),
                }],
            }],
            extra: BTreeMap::new(),
        },
    );
    sidecar.authority = Some(authority);
    let manifest = sidecar.manifest.clone();
    let served = Served::default();
    let record = served.clone();
    let installed = InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(id(&micro), sidecar, move |id, resources, bindings| {
            factory(resources, id, manifest, bindings, move |context| {
                record.activations.set(record.activations.get() + 1);
                let record = record.clone();
                async move {
                    context.publish_call("complete", move |typed: String| {
                        record.calls.set(record.calls.get() + 1);
                        async move { Ok(vec![format!("{name}:{typed}")]) }
                    })?;
                    Ok(())
                }
            })
        })
    };
    (installed, served)
}

// The editor provides a durable completion-source collection and two providers each contribute
// one row with a lazy reference to their call. Completing a text filters the rows locally and
// calls only the matching provider, which is started by the call and not before; a text that
// matches nothing starts nobody. While the editor is focused it retains the matching provider's
// call: several completions are then one activation of the provider, and blurring (dropping the
// binding) lets it go idle.
#[tokio::test(flavor = "current_thread")]
async fn the_editor_calls_only_the_providers_whose_trigger_matches() {
    let (alpha, alpha_served) = provider("alpha", "a.");
    let (beta, beta_served) = provider("beta", "b.");
    let mut micros = catalog(stub_counter());
    micros.extend([editor(), alpha, beta]);
    let runtime = CompositionRuntime::new(Arc::new(clay_store::MemoryStore::new()), micros)
        .await
        .unwrap();
    assert!(
        runtime.installation_errors.is_empty(),
        "{:?}",
        runtime.installation_errors
    );
    served(&runtime, async {
        for provider in ["tests.alpha", "tests.beta"] {
            assert_eq!(runtime.state(&id(provider)), MicroState::Idle);
        }
        let runtime = &runtime;
        async fn complete(runtime: &CompositionRuntime, typed: &str) -> Vec<String> {
            call_publication(runtime, "tests.editor", "complete", &typed.to_owned()).await
        }

        assert_eq!(complete(runtime, "a.x").await, ["alpha:a.x"]);
        assert_eq!(
            (alpha_served.activations.get(), alpha_served.calls.get()),
            (1, 1)
        );
        assert_eq!(
            (beta_served.activations.get(), beta_served.calls.get()),
            (0, 0)
        );
        assert_eq!(runtime.state(&id("tests.beta")), MicroState::Idle);

        assert_eq!(complete(runtime, "b.y").await, ["beta:b.y"]);
        assert_eq!(
            (beta_served.activations.get(), beta_served.calls.get()),
            (1, 1)
        );
        assert_eq!(alpha_served.calls.get(), 1);

        assert!(complete(runtime, "zzz").await.is_empty());
        assert_eq!(
            (
                alpha_served.activations.get(),
                beta_served.activations.get()
            ),
            (1, 1)
        );

        // Both providers' calls ended, so both go idle before the focus scenario starts.
        for provider in ["tests.alpha", "tests.beta"] {
            settle(runtime, || runtime.state(&id(provider)) == MicroState::Idle).await;
        }

        // Focus: the editor retains the matching provider's call, which starts it once and
        // keeps it up between completions. The editor itself is held live by the focused
        // surface (a lease), since its retained bindings live and die with its incarnation.
        let focused = runtime
            .supervisor
            .acquire(&supervised("tests.editor"), "complete")
            .await
            .unwrap();
        call_publication::<_, ()>(runtime, "tests.editor", "focus", &"a.".to_owned()).await;
        assert_eq!(runtime.state(&id("tests.alpha")), MicroState::Active);
        assert_eq!(alpha_served.activations.get(), 2);
        for typed in ["a.b", "a.bc", "a.bcd"] {
            assert_eq!(complete(runtime, typed).await, [format!("alpha:{typed}")]);
        }
        assert_eq!(
            (alpha_served.activations.get(), alpha_served.calls.get()),
            (2, 4),
            "three completions while focused are one activation"
        );
        assert_eq!(runtime.state(&id("tests.alpha")), MicroState::Active);
        // A provider whose trigger the focus did not match is still called once per completion.
        assert_eq!(complete(runtime, "b.z").await, ["beta:b.z"]);
        assert_eq!(beta_served.activations.get(), 2);

        // Blur drops the bindings, so the provider goes idle and the next completion starts it
        // again.
        call_publication::<_, ()>(runtime, "tests.editor", "blur", &()).await;
        settle(runtime, || {
            runtime.state(&id("tests.alpha")) == MicroState::Idle
        })
        .await;
        assert_eq!(complete(runtime, "a.again").await, ["alpha:a.again"]);
        assert_eq!(
            (alpha_served.activations.get(), alpha_served.calls.get()),
            (3, 5)
        );
        drop(focused);
    })
    .await;
}
