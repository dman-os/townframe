//! Wasm counter micro contributing a live Settings section over the WIT `presentation-client`
//! boundary.
use dayframe_app::{
    host::{ComponentRef, Instance},
    micros::{MicroDescriptor, MicroFactory, MicroId, MicroProvision, Provision},
    native::NativeComponents,
    wasm::{WasmMicro, WasmPackage},
};
use dayframe_settings_contract::{SettingsSection, SettingsSections};
use std::{
    path::PathBuf,
    process::Command,
    sync::LazyLock,
};

pub const ID: &str = "dayframe.counter";

#[derive(Clone)]
pub struct CounterMicro {
    pub instance: Instance,
    pub wasm: WasmMicro,
}

impl CounterMicro {
    pub fn detail(&self) -> ComponentRef {
        self.wasm.surface()
    }
}

impl Provision for CounterMicro {
    const ID: &'static str = "counter.controller";
}

pub fn package() -> WasmPackage {
    static BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let output = Command::new("cargo")
            .current_dir(&directory)
            .args([
                "build",
                "-p",
                "dayframe_guest",
                "--example",
                "counter",
                "--target",
                "wasm32-unknown-unknown",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "guest build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let metadata = Command::new("cargo")
            .current_dir(&directory)
            .args(["metadata", "--no-deps", "--format-version", "1"])
            .output()
            .unwrap();
        assert!(
            metadata.status.success(),
            "{}",
            String::from_utf8_lossy(&metadata.stderr)
        );
        let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout).unwrap();
        let core = PathBuf::from(metadata["target_directory"].as_str().unwrap())
            .join("wasm32-unknown-unknown/debug/examples/counter.wasm");
        let encoded = Command::new("wasm-tools")
            .args(["component", "new"])
            .arg(core)
            .output()
            .unwrap();
        assert!(
            encoded.status.success(),
            "component encoding failed: {}",
            String::from_utf8_lossy(&encoded.stderr)
        );
        encoded.stdout
    });
    WasmPackage::from_bytes(&*BYTES).expect("counter Wasm component is valid")
}

pub fn provider(native: NativeComponents) -> MicroFactory {
    provider_with_package(package(), native)
}

pub fn provider_with_package(package: WasmPackage, native: NativeComponents) -> MicroFactory {
    MicroFactory::new(
        MicroDescriptor {
            id: MicroId::new(ID),
            version: 1,
            requirements: vec![MicroProvision {
                id: SettingsSections::ID.into(),
                version: 1,
                        }],
            provisions: vec![MicroProvision {
                id: CounterMicro::ID.into(),
                version: 1,
                        }],
        },
        move |activation| {
            let instance = activation.instance();
            let sections = activation.require::<SettingsSections>()?;
            let settings = sections.bind(&instance)?;
            let wasm = WasmMicro::activate(instance.clone(), &package, &native)?;
            settings.insert(SettingsSection {
                title: "Counter (Wasm)".into(),
                detail: wasm.surface(),
            })?;
            activation.publish(CounterMicro { instance, wasm });
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dayframe_app::{
        application::ApplicationRuntime,
        host::AccessError,
        micros::composition::{ConfiguredMicro, Enablement, InstanceState},
        native::ComponentMount,
        presentation::SurfaceTree,
    };
    use dayframe_settings::SettingsMicro;
    use dioxus::prelude::*;
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedMouseData};
    use std::collections::BTreeMap;

    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[derive(Clone, PartialEq)]
    struct MountProps {
        components: NativeComponents,
        owner: Instance,
        reference: ComponentRef,
    }

    fn mount_host_app(props: MountProps) -> Element {
        rsx! {
            div {
                ComponentMount {
                    components: props.components,
                    owner: props.owner,
                    reference: props.reference,
                }
            }
        }
    }

    #[test]
    fn counter_micro_contributes_live_settings_section_and_withdraws_on_trap_or_disablement() {
        let mut app = ApplicationRuntime::new();
        app.register_provider(dayframe_settings::provider(
            app.native(),
            app.root_mounts(),
        ))
        .unwrap();
        app.register_provider(provider(app.native())).unwrap();

        let mut config = BTreeMap::from([
            (
                "settings".into(),
                ConfiguredMicro {
                    provider: MicroId::new("dayframe.settings"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            ),
            (
                "counter".into(),
                ConfiguredMicro {
                    provider: MicroId::new(ID),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([(SettingsSections::ID.into(), "settings".into())]),
                },
            ),
        ]);
        let states = block_on(app.configure(&config)).unwrap();
        assert_eq!(states["settings"], InstanceState::Active);
        assert_eq!(states["counter"], InstanceState::Active);

        let settings_micro = app.output::<SettingsMicro>("settings").unwrap();
        let first_counter = app.output::<CounterMicro>("counter").unwrap();
        let sections = settings_micro.sections.snapshot().unwrap();
        assert_eq!(sections.len(), 1);
        let section = sections.values().next().unwrap().clone();
        assert_eq!(section.title, "Counter (Wasm)");

        let mut host_dom = VirtualDom::new_with_props(
            mount_host_app,
            MountProps {
                components: app.native(),
                owner: settings_micro.instance.clone(),
                reference: section.detail,
            },
        );
        let mut host_tree = SurfaceTree::new();
        host_dom.rebuild(&mut host_tree);
        assert_eq!(host_tree.text_content(), "0");

        let click = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        host_dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            click,
            dioxus_core::ElementId::from_raw(3),
        );
        host_dom.render_immediate(&mut host_tree);
        assert_eq!(host_tree.text_content(), "1");

        first_counter.wasm.revoke_surfaces();
        let click2 = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        host_dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            click2,
            dioxus_core::ElementId::from_raw(3),
        );
        assert_eq!(first_counter.instance.check(), Err(AccessError::Revoked));
        assert!(settings_micro.sections.snapshot().unwrap().is_empty());

        let states = block_on(app.configure(&config)).unwrap();
        assert_eq!(states["counter"], InstanceState::Stopped);

        config.get_mut("counter").unwrap().enablement = Enablement::Disabled;
        assert_eq!(
            block_on(app.configure(&config)).unwrap()["counter"],
            InstanceState::Disabled
        );

        config.get_mut("counter").unwrap().enablement = Enablement::Enabled;
        assert_eq!(
            block_on(app.configure(&config)).unwrap()["counter"],
            InstanceState::Active
        );
        let second_counter = app.output::<CounterMicro>("counter").unwrap();
        assert!(second_counter.instance != first_counter.instance);
        assert_eq!(settings_micro.sections.snapshot().unwrap().len(), 1);

        app.host().close();
    }
}
