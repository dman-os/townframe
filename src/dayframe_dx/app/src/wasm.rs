//! Wasmtime host adapter for WIT `presentation-client` micro components.
//!
//! Each activated micro [`Instance`] owns an isolated `wasmtime::Store` so a guest trap never
//! taints sibling micros or a later re-enabled incarnation. If a guest traps during `mount` or
//! `dispatch`, the adapter immediately withdraws the surface and closes the micro's [`Instance`],
//! which revokes its [`ComponentRef`] and any host collections bound to that instance.

use crate::{
    host::{AccessError, ComponentRef, Instance},
    micros::{MicroDescriptor, MicroFactory, MicroId, MicroProvision, Provision, ProvisionKind},
    native::{NativeComponents, SurfaceSession, SurfaceWriter},
    presentation::{AttributeValue, Event, EventResponse, Mutation, PresentationError},
};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use wasmtime::{
    Engine, Store,
    component::{Component, HasSelf, Linker, Resource, ResourceTable},
};

mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "presentation-client",
        imports: { default: trappable },
        with: {
            "townframe:dayframe/presentation.surface": super::WasmSurface,
            "townframe:dayframe/presentation.mount-point": super::WasmPlacement,
        },
    });
}

use bindings::{
    PresentationClient,
    exports::townframe::dayframe::presentation_events,
    townframe::dayframe::presentation::{self as wit_presentation},
};

pub struct WasmPlacement {
    writer: SurfaceWriter,
    revoked: Arc<AtomicBool>,
}

pub struct WasmSurface {
    writer: SurfaceWriter,
    revoked: Arc<AtomicBool>,
}

struct WasmStoreState {
    table: ResourceTable,
}

impl WasmStoreState {
    fn new() -> Self {
        Self {
            table: ResourceTable::new(),
        }
    }
}

fn convert_attribute_value(value: wit_presentation::AttributeValue) -> AttributeValue {
    match value {
        wit_presentation::AttributeValue::Text(text) => AttributeValue::Text(text),
        wit_presentation::AttributeValue::Float(float) => AttributeValue::Float(float),
        wit_presentation::AttributeValue::Int(int) => AttributeValue::Int(int),
        wit_presentation::AttributeValue::Boolean(bool) => AttributeValue::Boolean(bool),
        wit_presentation::AttributeValue::None => AttributeValue::None,
    }
}

fn convert_mutation(mutation: wit_presentation::Mutation) -> Mutation {
    match mutation {
        wit_presentation::Mutation::PushId(id) => Mutation::PushId(id),
        wit_presentation::Mutation::SetId(id) => Mutation::SetId(id),
        wit_presentation::Mutation::Child(index) => Mutation::Child(index),
        wit_presentation::Mutation::Pop => Mutation::Pop,
        wit_presentation::Mutation::CreateElement((tag, ns)) => Mutation::CreateElement((tag, ns)),
        wit_presentation::Mutation::CreateText(value) => Mutation::CreateText(value),
        wit_presentation::Mutation::CloneNode => Mutation::CloneNode,
        wit_presentation::Mutation::AppendChildren(m) => Mutation::AppendChildren(m),
        wit_presentation::Mutation::ReplaceWith(m) => Mutation::ReplaceWith(m),
        wit_presentation::Mutation::InsertAfter(m) => Mutation::InsertAfter(m),
        wit_presentation::Mutation::InsertBefore(m) => Mutation::InsertBefore(m),
        wit_presentation::Mutation::SetAttribute((name, ns, value)) => {
            Mutation::SetAttribute((name, ns, convert_attribute_value(value)))
        }
        wit_presentation::Mutation::SetText(value) => Mutation::SetText(value),
        wit_presentation::Mutation::AddEventListener(name) => Mutation::AddEventListener(name),
        wit_presentation::Mutation::RemoveEventListener(name) => {
            Mutation::RemoveEventListener(name)
        }
        wit_presentation::Mutation::Remove => Mutation::Remove,
    }
}

fn convert_error(error: PresentationError) -> wit_presentation::PresentationError {
    match error {
        PresentationError::Revoked => wit_presentation::PresentationError::Revoked,
        PresentationError::Unsupported(msg) => {
            wit_presentation::PresentationError::Unsupported(msg)
        }
        PresentationError::InvalidBatch(msg) => {
            wit_presentation::PresentationError::InvalidBatch(msg)
        }
    }
}

impl wit_presentation::Host for WasmStoreState {}

impl wit_presentation::HostMountPoint for WasmStoreState {
    fn open(
        &mut self,
        placement: Resource<WasmPlacement>,
    ) -> wasmtime::Result<Result<Resource<WasmSurface>, wit_presentation::PresentationError>> {
        let granted = self.table.get(&placement)?;
        if granted.revoked.load(Ordering::Acquire) || !granted.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let surface = WasmSurface {
            writer: granted.writer.clone(),
            revoked: granted.revoked.clone(),
        };
        let resource = self.table.push(surface)?;
        Ok(Ok(resource))
    }

    fn drop(&mut self, placement: Resource<WasmPlacement>) -> wasmtime::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}

impl wit_presentation::HostSurface for WasmStoreState {
    fn apply(
        &mut self,
        surface: Resource<WasmSurface>,
        edits: Vec<wit_presentation::Mutation>,
    ) -> wasmtime::Result<Result<u64, wit_presentation::PresentationError>> {
        let surface = self.table.get_mut(&surface)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let converted: Vec<Mutation> = edits.into_iter().map(convert_mutation).collect();
        Ok(surface.writer.apply(&converted).map_err(convert_error))
    }

    fn drop(&mut self, surface: Resource<WasmSurface>) -> wasmtime::Result<()> {
        if let Ok(entry) = self.table.delete(surface) {
            entry.writer.withdraw();
        }
        Ok(())
    }
}

/// Compiled Wasm component and pre-configured linker reusable across incarnations.
#[derive(Clone)]
pub struct WasmPackage {
    engine: Engine,
    component: Component,
    linker: Arc<Linker<WasmStoreState>>,
}

impl WasmPackage {
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, String> {
        let engine = Engine::default();
        let component = Component::new(&engine, bytes).map_err(|err| err.to_string())?;
        let mut linker = Linker::new(&engine);
        wit_presentation::add_to_linker::<_, HasSelf<WasmStoreState>>(&mut linker, |state| state)
            .map_err(|err| err.to_string())?;
        Ok(Self {
            engine,
            component,
            linker: Arc::new(linker),
        })
    }

    fn instantiate(&self) -> Result<WasmIncarnation, AccessError> {
        let mut store = Store::new(&self.engine, WasmStoreState::new());
        let client = PresentationClient::instantiate(&mut store, &self.component, &self.linker)
            .map_err(|_| AccessError::Denied)?;
        Ok(WasmIncarnation { store, client })
    }
}

struct WasmIncarnation {
    store: Store<WasmStoreState>,
    client: PresentationClient,
}

#[derive(Clone)]
pub struct WasmMicro {
    pub instance: Instance,
    pub detail: ComponentRef,
    revoked: Arc<AtomicBool>,
}

impl PartialEq for WasmMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl Provision for WasmMicro {
    const ID: &'static str = "wasm.controller";
}

impl WasmMicro {
    pub fn activate(
        instance: Instance,
        package: &WasmPackage,
        native: &NativeComponents,
    ) -> Result<Self, AccessError> {
        let incarnation = Arc::new(Mutex::new(Some(package.instantiate()?)));
        let cleanup_incarnation = incarnation.clone();
        let revoked = Arc::new(AtomicBool::new(false));
        let cleanup_revoked = revoked.clone();
        instance.host().own(
            &instance,
            Box::new(move || {
                cleanup_revoked.store(true, Ordering::Release);
                cleanup_incarnation.lock().unwrap().take();
            }),
        )?;

        let instance_for_surface = instance.clone();
        let incarnation_for_surface = incarnation.clone();
        let revoked_for_surface = revoked.clone();
        let detail = native.publish_surface(&instance, move |_mount, writer| {
            let placement_rep = {
                let mut guard = incarnation_for_surface.lock().unwrap();
                let Some(active) = guard.as_mut() else {
                    return Err(PresentationError::Revoked);
                };
                let placement = active
                    .store
                    .data_mut()
                    .table
                    .push(WasmPlacement {
                        writer: writer.clone(),
                        revoked: revoked_for_surface.clone(),
                    })
                    .map_err(|_| PresentationError::Revoked)?;
                let placement_rep = placement.rep();
                let mount_result = active
                    .client
                    .townframe_dayframe_presentation_events()
                    .call_mount(&mut active.store, Resource::new_borrow(placement_rep));
                let _ = active
                    .store
                    .data_mut()
                    .table
                    .delete(Resource::<WasmPlacement>::new_own(placement_rep));
                match mount_result {
                    Ok(Ok(surface)) => surface.rep(),
                    Ok(Err(err)) => {
                        writer.withdraw();
                        return Err(match err {
                            wit_presentation::PresentationError::Revoked => {
                                PresentationError::Revoked
                            }
                            wit_presentation::PresentationError::Unsupported(msg) => {
                                PresentationError::Unsupported(msg)
                            }
                            wit_presentation::PresentationError::InvalidBatch(msg) => {
                                PresentationError::InvalidBatch(msg)
                            }
                        });
                    }
                    Err(_trap) => {
                        guard.take();
                        drop(guard);
                        writer.withdraw();
                        instance_for_surface.close();
                        return Err(PresentationError::Revoked);
                    }
                }
            };

            let dispatch_incarnation = incarnation_for_surface.clone();
            let dispatch_instance = instance_for_surface.clone();
            let dispatch_writer = writer.clone();
            let drop_incarnation = incarnation_for_surface.clone();
            let drop_writer = writer;

            Ok(SurfaceSession::with_drop(
                move |event: Event| {
                    let wit_event = presentation_events::Event {
                        revision: event.revision,
                        element: event.element,
                        name: event.name,
                        bubbles: event.bubbles,
                        data_json: event.data_json,
                    };
                    let result = {
                        let mut guard = dispatch_incarnation.lock().unwrap();
                        let Some(active) = guard.as_mut() else {
                            return EventResponse::default();
                        };
                        let outcome = active
                            .client
                            .townframe_dayframe_presentation_events()
                            .call_dispatch(
                                &mut active.store,
                                Resource::new_borrow(placement_rep),
                                &wit_event,
                            );
                        if outcome.is_err() {
                            guard.take();
                        }
                        outcome
                    };
                    match result {
                        Ok(response) => EventResponse {
                            prevent_default: response.prevent_default,
                            stop_propagation: response.stop_propagation,
                        },
                        Err(_trap) => {
                            dispatch_writer.withdraw();
                            dispatch_instance.close();
                            EventResponse::default()
                        }
                    }
                },
                move || {
                    drop_writer.withdraw();
                    let mut guard = drop_incarnation.lock().unwrap();
                    if let Some(active) = guard.as_mut() {
                        let _ = active
                            .store
                            .data_mut()
                            .table
                            .delete(Resource::<WasmSurface>::new_own(placement_rep));
                    }
                },
            ))
        })?;

        Ok(Self {
            instance,
            detail,
            revoked,
        })
    }

    pub fn surface(&self) -> ComponentRef {
        self.detail
    }

    /// Revokes this incarnation's surface capability handles without closing the instance first,
    /// allowing tests to verify that a subsequent guest `surface.apply` sees `Revoked`, traps, and
    /// triggers automatic instance closure and collection withdrawal.
    pub fn revoke_surfaces(&self) {
        self.revoked.store(true, Ordering::Release);
    }

    pub fn close(&self) {
        self.instance.close();
    }
}

pub fn provider(id: MicroId, package: WasmPackage, native: NativeComponents) -> MicroFactory {
    MicroFactory::new(
        MicroDescriptor {
            id,
            version: 1,
            requirements: vec![],
            provisions: vec![MicroProvision {
                id: WasmMicro::ID.into(),
                version: 1,
                kind: ProvisionKind::Context,
            }],
        },
        move |activation| {
            let instance = activation.instance();
            let micro = WasmMicro::activate(instance, &package, &native)?;
            activation.publish(micro);
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::ApplicationRuntime,
        host::{HostedCollection, Host},
        micros::{
            AsyncMicroFactory, MicroRegistry,
            composition::{ConfiguredMicro, Enablement, InstanceState},
        },
        native::ComponentMount,
        presentation::SurfaceTree,
    };
    use dioxus::prelude::*;
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedMouseData};
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        process::Command,
        sync::LazyLock,
    };

    fn counter_package() -> WasmPackage {
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
        WasmPackage::from_bytes(&*BYTES).expect("counter component is valid")
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
    fn wasm_provider_mounts_surface_and_updates_on_click() {
        let mut app = ApplicationRuntime::new();
        app.register_provider(provider(
            MicroId::new("dayframe.wasm.counter"),
            counter_package(),
            app.native(),
        ))
        .unwrap();

        let config = BTreeMap::from([(
            "counter".into(),
            ConfiguredMicro {
                provider: MicroId::new("dayframe.wasm.counter"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        )]);
        let states = app.configure(&config).unwrap();
        assert_eq!(states["counter"], InstanceState::Active);

        let wasm_micro = app.output::<WasmMicro>("counter").unwrap();

        let mut host_dom = VirtualDom::new_with_props(
            mount_host_app,
            MountProps {
                components: app.native(),
                owner: app.root(),
                reference: wasm_micro.surface(),
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

        app.host().close();
    }

    #[test]
    fn wasm_guest_trap_or_disablement_closes_instance_and_withdraws_owned_entries() {
        let mut app = ApplicationRuntime::new();
        let host = app.host();
        let root = app.root();
        let collection = HostedCollection::<ComponentRef>::new(&host, &root);
        let collection_for_provider = collection.clone();
        let package = counter_package();
        let native = app.native();

        app.register_provider(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("dayframe.wasm.counter"),
                version: 1,
                requirements: vec![],
                provisions: vec![MicroProvision {
                    id: WasmMicro::ID.into(),
                    version: 1,
                    kind: ProvisionKind::Context,
                }],
            },
            move |activation| {
                let instance = activation.instance();
                let micro = WasmMicro::activate(instance.clone(), &package, &native)?;
                collection_for_provider
                    .writer(&instance)
                    .insert(micro.surface())?;
                activation.publish(micro);
                Ok(())
            },
        ))
        .unwrap();

        let mut config = BTreeMap::from([(
            "counter".into(),
            ConfiguredMicro {
                provider: MicroId::new("dayframe.wasm.counter"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        )]);
        app.configure(&config).unwrap();

        let reader = collection.reader(&root);
        let first_wasm = app.output::<WasmMicro>("counter").unwrap();
        assert_eq!(reader.snapshot().unwrap().len(), 1);

        let mut host_dom = VirtualDom::new_with_props(
            mount_host_app,
            MountProps {
                components: app.native(),
                owner: root.clone(),
                reference: first_wasm.surface(),
            },
        );
        let mut host_tree = SurfaceTree::new();
        host_dom.rebuild(&mut host_tree);
        assert_eq!(host_tree.text_content(), "0");

        first_wasm.revoke_surfaces();
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
        assert_eq!(first_wasm.instance.check(), Err(AccessError::Revoked));
        assert!(reader.snapshot().unwrap().is_empty());

        let states = app.configure(&config).unwrap();
        assert_eq!(states["counter"], InstanceState::Stopped);

        config.get_mut("counter").unwrap().enablement = Enablement::Disabled;
        let states = app.configure(&config).unwrap();
        assert_eq!(states["counter"], InstanceState::Disabled);

        config.get_mut("counter").unwrap().enablement = Enablement::Enabled;
        let states = app.configure(&config).unwrap();
        assert_eq!(states["counter"], InstanceState::Active);
        let second_wasm = app.output::<WasmMicro>("counter").unwrap();
        assert!(second_wasm.instance != first_wasm.instance);
        assert_eq!(reader.snapshot().unwrap().len(), 1);

        app.host().close();
    }

    #[tokio::test]
    async fn async_micro_factory_activates_and_rolls_back_on_error() {
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let ok_id = MicroId::new("dayframe.async.ok");
        let fail_id = MicroId::new("dayframe.async.fail");
        let leaked = Arc::new(Mutex::new(None));
        let leaked_capture = leaked.clone();

        registry
            .register(AsyncMicroFactory::new(
                MicroDescriptor {
                    id: ok_id.clone(),
                    version: 1,
                    provisions: Vec::new(),
                    requirements: Vec::new(),
                },
                |context| async move {
                    tokio::task::yield_now().await;
                    context.instance().check()?;
                    Ok(())
                },
            ))
            .unwrap();

        registry
            .register(AsyncMicroFactory::new(
                MicroDescriptor {
                    id: fail_id.clone(),
                    version: 1,
                    provisions: Vec::new(),
                    requirements: Vec::new(),
                },
                move |context| {
                    let leaked_capture = leaked_capture.clone();
                    async move {
                        tokio::task::yield_now().await;
                        let comp = context.host().publish_component(&context.instance())?;
                        *leaked_capture.lock().unwrap() = Some((context.instance(), comp));
                        Err(AccessError::Denied)
                    }
                },
            ))
            .unwrap();

        let instance = registry
            .activate_async(&host, &ok_id, "async-ok", BTreeMap::new())
            .await
            .unwrap();
        assert!(instance.check().is_ok());

        assert!(matches!(
            registry
                .activate_async(&host, &fail_id, "async-fail", BTreeMap::new())
                .await,
            Err(crate::micros::MicroRegistryError::Access(AccessError::Denied))
        ));
        let (failed_instance, _) = leaked.lock().unwrap().take().unwrap();
        assert_eq!(failed_instance.check(), Err(AccessError::Revoked));
        host.close();
    }
}
