//! C1 step 0 SPIKE: the shell as a Wasm guest root on Wasmtime, in place of the native shell.
//! `SPIKE_SHELL_WASM` names the `micro_shell` wasm32-wasip3 component.
use super::composed;
use super::HeadlessDom;
use super::Launch;
use crate::composition::native_micros;
use crate::composition::CompositionRuntime;
use clay_host::MicroState;
use clay_mdk::MicroId;
use dayframe_composition::InstalledMicro;

async fn guest_shell_runtime() -> CompositionRuntime {
    let bytes = std::fs::read(std::env::var("SPIKE_SHELL_WASM").expect("SPIKE_SHELL_WASM"))
        .expect("the guest shell component is built");
    let sidecar = micro_shell_manifest::sidecar();
    let id = MicroId::local(micro_shell_manifest::ID);
    let mut installed: Vec<InstalledMicro> = native_micros()
        .into_iter()
        .filter(|micro| micro.id != id)
        .collect();
    installed.push(InstalledMicro {
        eager: true,
        locked: true,
        ..InstalledMicro::new(id, sidecar.clone(), move |id, resources, bindings| {
            let bytes = bytes.clone();
            clay_mdk::host::wasm_factory(resources, id, sidecar.manifest, bindings, move || {
                let bytes = bytes.clone();
                async move { clay_mdk::host::WasmPackage::from_bytes(bytes).await }
            })
        })
    });
    CompositionRuntime::new(std::sync::Arc::new(clay_store::MemoryStore::new()), installed)
        .await
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn a_guest_shell_renders_home_and_opens_settings_and_a_section() {
    let runtime = guest_shell_runtime().await;
    let shell = MicroId::local(micro_shell_manifest::ID);
    let launch = Launch::new(runtime.clone(), super::WIDE_PX);
    let mut ui = HeadlessDom::new(composed, launch);
    ui.wait_until("home lists Settings", |ui| {
        ui.find(super::HOME).is_some() && ui.find(super::OPEN_SETTINGS).is_some()
    })
    .await;
    assert_eq!(runtime.state(&shell), MicroState::Active);
    ui.click(super::OPEN_SETTINGS);
    ui.wait_until("Settings sections listed", |ui| {
        ui.find(super::SAMPLE).is_some()
    })
    .await;
    ui.click(super::SAMPLE);
    ui.wait_until("the sample section is the details", |ui| {
        ui.find(super::BACK).is_some()
    })
    .await;
    ui.click(super::BACK);
    ui.wait_until("back from the section", |ui| ui.find(super::BACK).is_none())
        .await;
}
