use super::*;
use clay_host::MicroId;
use clay_host::MicroState;
use clay_mdk::native::CollectionWatch;
use clay_mdk::native::RowAttachment;
use micro_settings_manifest::Section;

// The package directory must install the Wasmtime counter beside the shell's native
// built-ins, and its manifest-declared section must be there before the guest ever runs.
#[tokio::test(flavor = "current_thread")]
async fn desktop_runtime_installs_the_counter_section_from_its_package_manifest() {
    let runtime = desktop_runtime(std::sync::Arc::new(clay_store::MemoryStore::new()))
        .await
        .unwrap();
    assert!(
        runtime.installation_errors.is_empty(),
        "{:?}",
        runtime.installation_errors
    );
    let micros: Vec<&str> = runtime.micros().iter().map(MicroId::as_str).collect();
    assert_eq!(
        micros,
        [
            micro_settings_manifest::ID,
            micro_sample::ID,
            micro_counter_manifest::ID
        ]
    );
    let mut key = Vec::new();
    ciborium::into_writer(micro_counter_manifest::ID, &mut key).unwrap();
    let row = runtime
        .collection(micro_settings_manifest::SECTIONS)
        .unwrap()
        .get(&key)
        .unwrap()
        .value
        .unwrap();
    assert!(matches!(
        row.attachments.as_slice(),
        [RowAttachment::Endpoint(endpoint)]
            if endpoint.micro.as_str() == micro_counter_manifest::ID
    ));
    // A discovered package is admitted disabled; nothing auto-enables it.
    assert_eq!(
        runtime.state(&MicroId::new(micro_counter_manifest::ID)),
        MicroState::Disabled
    );
    runtime.host().close();
}

// Redeeming the counter's section starts the Wasmtime guest, and releasing it lets the
// supervisor close the guest again.
#[tokio::test(flavor = "current_thread")]
async fn redeeming_the_counter_section_runs_the_guest_only_while_held() {
    let runtime = desktop_runtime(std::sync::Arc::new(clay_store::MemoryStore::new()))
        .await
        .unwrap();
    let counter = MicroId::new(micro_counter_manifest::ID);
    // Packages are admitted disabled; enable through the install datum, which the reconciler
    // applies once the composition serves.
    runtime.set_enabled(&counter, true);
    tokio::task::LocalSet::new()
        .run_until(async {
            let serving = runtime.serve();
            tokio::pin!(serving);
            let scenario = async {
                let shell = runtime.host().start("test-observer");
                let (sections, feed) = CollectionWatch::<String, Section>::observe(
                    runtime.host(),
                    &shell,
                    runtime.supervisor().activator(),
                    &runtime
                        .collection(micro_settings_manifest::SECTIONS)
                        .unwrap(),
                )
                .unwrap();
                tokio::task::spawn_local(feed);
                let mut changes = sections.revisions();
                while sections.rows().unwrap().len() < 2 {
                    changes.changed().await.unwrap();
                }
                let rows = sections.rows().unwrap();
                let row = rows
                    .iter()
                    .find(|row| row.key == micro_counter_manifest::ID)
                    .unwrap();
                let detail = row.attachments[row.value.detail.index()]
                    .redeem_presentation()
                    .await
                    .unwrap();
                assert_eq!(runtime.state(&counter), MicroState::Active);
                assert!(detail.component().is_ok());
                drop(detail);
                while runtime.state(&counter) != MicroState::Idle {
                    tokio::task::yield_now().await;
                }
            };
            tokio::select! {
                () = &mut serving => panic!("the composition stopped before shutdown"),
                () = scenario => {}
            }
            runtime.request_shutdown();
            serving.await;
        })
        .await;
}
