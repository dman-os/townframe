//! Wasm counter micro contributing a live Settings section over the WIT `micro-client` world.
//! Its count lives in the counter's owner-config facet: activation grants exactly that facet's
//! read/write rights and passes the binding to the guest, which watches it and writes through it.
//! `CONFIG_TAG`/`CONFIG_KEY` must match the constants compiled into the guest component.
use dayframe_app::{
    documents::FacetRoute,
    host::{ComponentRef, Host, Instance},
    micros::{AsyncMicroFactory, MicroDescriptor, MicroId, MicroProvision, Provision},
    native::NativeComponents,
};
use dayframe_engine_wasmtime::{WasmMicro, WasmPackage};
use dayframe_settings_contract::{SettingsSection, SettingsSections};
use std::sync::LazyLock;

#[path = "../../../counter_fixture.rs"]
mod counter_fixture;

pub const ID: &str = "dayframe.counter";
pub const CONFIG_TAG: &str = "org.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";

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
    static PACKAGE: LazyLock<WasmPackage> = LazyLock::new(|| {
        WasmPackage::from_bytes(counter_fixture::counter_component())
            .expect("DAYFRAME_COUNTER_COMPONENT must contain a valid counter Wasm component")
    });
    PACKAGE.clone()
}

pub fn bind_mock_defaults(host: &Host) {
    let documents = host.mock_documents();
    documents.bind_config("counter".into(), "counter-config".into(), "main".into());
    documents.seed(
        FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: CONFIG_TAG.into(),
            key_id: CONFIG_KEY.into(),
        },
        serde_json::json!(0),
    );
}

pub fn provider(package: WasmPackage, native: NativeComponents) -> AsyncMicroFactory {
    AsyncMicroFactory::new(
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
            let package = package.clone();
            let native = native.clone();
            async move {
                let instance = activation.instance();
                let sections = activation.require::<SettingsSections>()?;
                let settings = sections.bind(&instance)?;
                let configs = activation.host().grant_config(
                    &instance,
                    "counter",
                    vec![daybook_types::manifest::RoutineFacetAccess {
                        tag: CONFIG_TAG.into(),
                        key_id: Some(CONFIG_KEY.into()),
                        read: true,
                        write: true,
                        ..Default::default()
                    }],
                )?;
                let wasm = WasmMicro::activate(&activation, &package, &native, configs)?;
                settings.insert(SettingsSection {
                    title: "Counter (Wasm)".into(),
                    detail: wasm.surface(),
                })?;
                activation.publish(CounterMicro { instance, wasm });
                Ok(())
            }
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
    };
    use dayframe_presentation::SurfaceTree;
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
        bind_mock_defaults(&app.host());
        app.register_provider(dayframe_settings::provider(app.native(), app.root_mounts()))
            .unwrap();
        app.register_provider(provider(package(), app.native()))
            .unwrap();

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
        assert_eq!(
            host_tree.text_content(),
            "0Pending counter writes: 0Refresh counter"
        );

        let click = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        host_dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            click,
            host_tree
                .listener_id("button", "click")
                .expect("host tree rendered the counter button"),
        );
        host_dom.render_immediate(&mut host_tree);
        assert_eq!(
            host_tree.text_content(),
            "1Pending counter writes: 1Refresh counter"
        );

        first_counter.wasm.exhaust_fuel();
        let click2 = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        host_dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            click2,
            host_tree
                .listener_id("button", "click")
                .expect("host tree rendered the counter button"),
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
