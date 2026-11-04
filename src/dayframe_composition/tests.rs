use super::*;
use clay_collection::Stamped;
use clay_host::StartError;
use clay_mdk::native::factory;
use clay_mdk::native::CapabilityFailure;
use clay_mdk::native::CollectionBindings;
use clay_mdk::native::RowAttachment;
use micro_settings_manifest::Section;
use micro_settings_manifest::SECTIONS;
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
        ..InstalledMicro::new(micro_settings_manifest::manifest(), |resources| {
            factory(
                resources,
                micro_settings_manifest::manifest(),
                CollectionBindings {
                    read: vec![SECTIONS.into()],
                    write: vec![],
                },
                micro_settings::activate,
            )
        })
    };
    let sample = InstalledMicro {
        eager: true,
        ..InstalledMicro::new(micro_sample::manifest(), |resources| {
            factory(
                resources,
                micro_sample::manifest(),
                CollectionBindings {
                    read: vec![],
                    write: vec![SECTIONS.into()],
                },
                micro_sample::activate,
            )
        })
    };
    vec![settings, sample, counter]
}

/// A counter that publishes its declared view without any engine.
fn stub_counter() -> InstalledMicro {
    InstalledMicro::new(micro_counter_manifest::manifest(), |resources| {
        factory(
            resources,
            micro_counter_manifest::manifest(),
            CollectionBindings::default(),
            |context| {
                context.publish_component(micro_counter_manifest::VIEW, || {
                    rsx! { p { "stub counter" } }
                })?;
                Ok(())
            },
        )
    })
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
    InstalledMicro::new(micro_counter_manifest::manifest(), |_resources| {
        Rc::new(Unfetchable)
    })
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
    CollectionWatch<String, Section>,
    impl Future<Output = ()> + 'static,
) {
    CollectionWatch::observe(
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
    assert!(runtime
        .collection(SECTIONS)
        .unwrap()
        .get(&cbor_key(micro_sample::ID))
        .is_none());
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
        let mut roots = runtime.roots.revisions();
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
        let mut changes = sections.revisions();
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
        let mut changes = sections.revisions();
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
            Some(CapabilityFailure::Unavailable)
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
        let mut changes = sections.revisions();
        while sections.rows().unwrap().len() < 2 {
            changes.changed().await.unwrap();
        }
        let rows = sections.rows().unwrap();
        let counter = rows.iter().find(|row| row.key == COUNTER).unwrap();
        assert_eq!(
            counter.attachments[0].redeem_presentation().await.err(),
            Some(CapabilityFailure::Unavailable)
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
    let counter = InstalledMicro::new(manifest, |_resources| Rc::new(Unfetchable));
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
                catalog(InstalledMicro::new(manifest, |_resources| {
                    Rc::new(Unfetchable)
                })),
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
