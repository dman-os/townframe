use super::*;
use clay_collection::Stamped;
use clay_host::StartError;
use clay_mdk::Error;
use clay_mdk::RowAttachment;
use clay_mdk::host::factory;
use micro_settings_manifest::SECTIONS;
use micro_settings_manifest::Section;
use std::future::Future;

const COUNTER: &str = micro_counter_manifest::ID;

fn id(micro: &str) -> MicroId {
    MicroId::new(micro)
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
            micro_settings_manifest::manifest(),
            |resources, _components, bindings| {
                factory(
                    resources,
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
            micro_sample::manifest(),
            |resources, _components, bindings| {
                factory(
                    resources,
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
            micro_counter_manifest::manifest(),
            |resources, _components, bindings| {
                factory(
                    resources,
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
struct Unfetchable;

impl Factory<CapabilityPublication> for Unfetchable {
    fn initialize(
        &self,
        _host: Host,
        _instance: Instance,
        _requirements: BTreeMap<String, CapabilityPublication>,
        _activator: clay_host::Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<clay_host::Initialized<CapabilityPublication>, String>> {
        Box::pin(async { Err("the package could not be fetched".to_owned()) })
    }
}

fn unfetchable_counter() -> InstalledMicro {
    InstalledMicro {
        enabled_on_admission: true,
        ..InstalledMicro::new(
            micro_counter_manifest::manifest(),
            |_resources, _components, _bindings| Rc::new(Unfetchable),
        )
    }
}

/// The catalog over a fresh in-memory store.
async fn memory_runtime(counter: InstalledMicro) -> CompositionRuntime {
    CompositionRuntime::new(Arc::new(clay_store::MemoryStore::new()), catalog(counter))
        .await
        .expect("a memory store does not fail")
}

async fn settle(done: impl Fn() -> bool) {
    while !done() {
        tokio::task::yield_now().await;
    }
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
        &runtime.shell,
        runtime.supervisor.activator(),
        &runtime.collection(SECTIONS).unwrap(),
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
        .collection(SECTIONS)
        .unwrap()
        .get(&cbor_key(COUNTER))
        .unwrap();
    let row = value.unwrap();
    let section: Section = ciborium::from_reader(row.body.as_slice()).unwrap();
    assert_eq!(section.title, "Counter (Wasm)");
    assert!(matches!(
        row.attachments.as_slice(),
        [RowAttachment::Endpoint(endpoint)] if endpoint.micro == id(COUNTER) && endpoint.publication == "view"
    ));
    // The sample writes its row at runtime, so nothing exists for it yet.
    assert!(
        runtime
            .collection(SECTIONS)
            .unwrap()
            .get(&cbor_key(micro_sample::ID))
            .is_none()
    );
    runtime.host.close();
}

#[tokio::test(flavor = "current_thread")]
async fn eager_roots_start_and_a_section_starts_its_micro_only_while_mounted() {
    let runtime = memory_runtime(stub_counter()).await;
    served(&runtime, async {
        settle(|| {
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
        settle(|| runtime.state(&id(COUNTER)) == MicroState::Idle).await;
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
        supervisor.disable(&id(COUNTER)).await.unwrap();
        let rows = sections.rows().unwrap();
        let counter = rows.iter().find(|row| row.key == COUNTER).unwrap();
        let reference = &counter.attachments[0];
        assert_eq!(
            reference.redeem_presentation().await.err(),
            Some(Error::Unavailable)
        );
        supervisor.enable(&id(COUNTER));
        assert!(reference.redeem_presentation().await.is_ok());
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Active);
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_unfetchable_guest_faults_alone_until_retried() {
    let runtime = memory_runtime(unfetchable_counter()).await;
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
            supervisor.acquire(&id(COUNTER), "view").await.err(),
            Some(StartError::Faulted(_))
        ));
        supervisor.retry(&id(COUNTER));
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Idle);
        assert!(counter.attachments[0].redeem_presentation().await.is_err());
        assert!(matches!(
            runtime.state(&id(COUNTER)),
            MicroState::Faulted(_)
        ));
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_manifest_contributing_to_an_undeclared_collection_is_an_installation_error() {
    let mut manifest = micro_counter_manifest::manifest();
    manifest.contributions[0].collection = "nobody.declared.this".into();
    let counter = InstalledMicro::new(manifest, |_resources, _components, _bindings| {
        Rc::new(Unfetchable)
    });
    let runtime = memory_runtime(counter).await;
    assert_eq!(runtime.installation_errors.len(), 1);
    assert!(runtime.installation_errors[0].contains("nobody.declared.this"));
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
            let mut manifest = micro_counter_manifest::manifest();
            manifest.contributions[0].payload["title"] = title.into();
            let runtime = CompositionRuntime::new(
                Arc::new(store),
                catalog(InstalledMicro::new(
                    manifest,
                    |_resources, _components, _bindings| Rc::new(Unfetchable),
                )),
            )
            .await
            .unwrap();
            assert!(runtime.installation_errors.is_empty());
            let row = runtime
                .collection(SECTIONS)
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
                micro_counter_manifest::manifest(),
                |_, _, _| Rc::new(Unfetchable),
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
        micro_counter_manifest::manifest(),
        |_resources, _components, _bindings| Rc::new(Unfetchable),
    );
    let runtime = memory_runtime(package).await;
    let version = micro_counter_manifest::manifest().package_version;
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
        micro_counter_manifest::manifest(),
        |_resources, _components, _bindings| Rc::new(Unfetchable),
    ))
    .await;
    served(&runtime, async {
        let sample = id(micro_sample::ID);
        settle(|| runtime.state(&sample) == MicroState::Active).await;
        assert_eq!(runtime.state(&id(COUNTER)), MicroState::Disabled);
        runtime.set_enabled(&sample, false).await;
        settle(|| runtime.state(&sample) == MicroState::Disabled).await;
        runtime.set_enabled(&sample, true).await;
        settle(|| runtime.state(&sample) == MicroState::Idle).await;
        runtime.set_enabled(&id(COUNTER), true).await;
        settle(|| runtime.state(&id(COUNTER)) == MicroState::Idle).await;
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
        settle(|| first.state(&sample) == MicroState::Active).await;
        first.set_enabled(&sample, false).await;
        settle(|| first.state(&sample) == MicroState::Disabled).await;
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

// ── collection binding resolution (ADR 025 §11) ──────────────────────────────────────────────

const ROWS: &str = "tests.rows";
const MISSING: &str = "tests.missing";
const BOOK: &str = "book";
const PROBE: &str = "tests.probe";

fn probe_manifest(
    micro: &str,
    contracts: &[&str],
    requests: Vec<clay_mdk::CollectionRequest>,
) -> clay_mdk::MicroManifest {
    clay_mdk::MicroManifest {
        schema: clay_mdk::MANIFEST_SCHEMA,
        id: clay_mdk::MicroId::new(micro),
        package_version: 1,
        entrypoints: clay_mdk::Entrypoints::micro_client(),
        publications: vec![],
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: contracts
            .iter()
            .map(|contract| clay_mdk::CollectionContract {
                id: (*contract).into(),
                selector_schema: "dayframe.collection.whole".into(),
                key_schema: "text".into(),
                value_schema: "text".into(),
                write_context_schema: None,
                offered: vec![clay_mdk::CollectionCapability::Read],
            })
            .collect(),
        collection_requests: requests,
        contributions: vec![],
        extra: BTreeMap::new(),
    }
}

/// Declares the `tests.rows` collection and nothing else.
fn rows_owner() -> InstalledMicro {
    InstalledMicro::new(
        probe_manifest("tests.owner", &[ROWS], vec![]),
        |resources, _components, bindings| {
            factory(
                resources,
                probe_manifest("tests.owner", &[ROWS], vec![]),
                bindings,
                |_| async { Ok(()) },
            )
        },
    )
}

/// An eager micro requesting `BOOK` of `contract`; each activation reports whether the binding
/// was bound to it.
fn probe(
    contract: &str,
    necessity: clay_mdk::Necessity,
) -> (InstalledMicro, watch::Receiver<Vec<bool>>) {
    let manifest = probe_manifest(
        PROBE,
        &[],
        vec![clay_mdk::CollectionRequest {
            binding: BOOK.into(),
            contract: contract.into(),
            access: clay_mdk::CollectionAccess::Read,
            necessity,
            resolution: clay_mdk::BindingResolution::Configured,
        }],
    );
    let (activations, seen) = watch::channel(Vec::new());
    let micro = InstalledMicro {
        eager: true,
        enabled_on_admission: true,
        ..InstalledMicro::new(manifest.clone(), move |resources, _components, bindings| {
            factory(resources, manifest, bindings, move |context| {
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
    let mut changes = runtime.supervisor.changes();
    while runtime.state(micro) != state {
        changes.changed().await.unwrap();
    }
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
            .acquire(&micro, "none")
            .await
            .err()
            .unwrap();
        assert_eq!(
            refused,
            StartError::Unconfigured(micro.clone(), vec![BOOK.into()])
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
