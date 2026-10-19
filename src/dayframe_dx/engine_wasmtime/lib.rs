//! Wasmtime host adapter for WIT `presentation-client` micro components.
//!
//! Each activated micro [`Instance`] owns an isolated `wasmtime::Store` so a guest trap never
//! taints sibling micros or a later re-enabled incarnation. If a guest traps during `mount` or
//! `dispatch`, the adapter immediately withdraws the surface and closes the micro's [`Instance`],
//! which revokes its [`ComponentRef`] and any host collections bound to that instance.

mod documents;

use dayframe_app::{
    documents::DocumentRef,
    host::{ComponentRef, Instance},
    micros::{
        ActivationError, AsyncMicroFactory, MicroActivation, MicroDescriptor, MicroId,
        MicroProvision, Provision,
    },
    native::{NativeComponents, SurfaceSession, SurfaceWriter},
};
use dayframe_presentation::{
    AttributeValue, Event, EventResponse, Mutation, PresentationError, SurfaceTree,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;
use wasmtime::{
    Config, Engine, Store, StoreLimits, StoreLimitsBuilder,
    component::{Component, HasSelf, Linker, Resource, ResourceTable},
};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../presentation/wit",
        world: "micro-client",
        imports: { default: trappable },
        with: {
            "townframe:dayframe/presentation.surface": super::WasmSurface,
            "townframe:dayframe/presentation.mount-point": super::WasmPlacement,
            "townframe:dayframe/documents.document-access": super::documents::WasmDocumentAccess,
            "townframe:dayframe/documents.document-watch": super::documents::WasmDocumentWatch,
            "townframe:dayframe/documents.operation": super::documents::WasmOperation,
        },
    });
}

use bindings::{
    MicroClient,
    exports::townframe::dayframe::presentation_events,
    townframe::dayframe::presentation::{self as wit_presentation},
};

pub struct WasmPlacement {
    writer: SurfaceWriter,
    revoked: Arc<AtomicBool>,
    surface_identity: Option<u64>,
}

#[derive(Clone)]
pub struct WasmSurface {
    writer: SurfaceWriter,
    revoked: Arc<AtomicBool>,
    identity: u64,
    ownership: Arc<()>,
}

impl Drop for WasmSurface {
    fn drop(&mut self) {
        if Arc::strong_count(&self.ownership) == 1 {
            self.writer.withdraw();
        }
    }
}

struct WasmStoreState {
    table: ResourceTable,
    instance: Instance,
    work: mpsc::Sender<documents::Work>,
    next_watch: u64,
    next_surface: u64,
    document_handles: usize,
    surface_handles: usize,
    limits: StoreLimits,
}

impl WasmStoreState {
    const MAX_SURFACE_HANDLES: usize = 1024;
    const MAX_DOCUMENT_HANDLES: usize = 1024;
    const MAX_PENDING_DELIVERIES: usize = 1024;

    fn new(instance: Instance, work: mpsc::Sender<documents::Work>) -> Self {
        Self {
            table: ResourceTable::new(),
            instance,
            work,
            next_watch: 0,
            next_surface: 0,
            document_handles: 0,
            surface_handles: 0,
            limits: StoreLimitsBuilder::new()
                .memory_size(64 * 1024 * 1024)
                .table_elements(8192)
                .build(),
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

fn ingress_budget(edits: &[wit_presentation::Mutation]) -> Result<(), &'static str> {
    if edits.len() > SurfaceTree::MAX_BATCH_MUTATIONS {
        return Err("batch mutation limit");
    }
    let mut bytes = 0_usize;
    for mutation in edits {
        let strings: [Option<&str>; 3] = match mutation {
            wit_presentation::Mutation::CreateElement((tag, ns)) => {
                [Some(tag), ns.as_deref(), None]
            }
            wit_presentation::Mutation::CreateText(text)
            | wit_presentation::Mutation::SetText(text)
            | wit_presentation::Mutation::AddEventListener(text)
            | wit_presentation::Mutation::RemoveEventListener(text) => [Some(text), None, None],
            wit_presentation::Mutation::SetAttribute((name, ns, value)) => [
                Some(name),
                ns.as_deref(),
                match value {
                    wit_presentation::AttributeValue::Text(text) => Some(text),
                    _ => None,
                },
            ],
            wit_presentation::Mutation::PushId(_)
            | wit_presentation::Mutation::SetId(_)
            | wit_presentation::Mutation::Child(_)
            | wit_presentation::Mutation::Pop
            | wit_presentation::Mutation::CloneNode
            | wit_presentation::Mutation::AppendChildren(_)
            | wit_presentation::Mutation::ReplaceWith(_)
            | wit_presentation::Mutation::InsertAfter(_)
            | wit_presentation::Mutation::InsertBefore(_)
            | wit_presentation::Mutation::Remove => [None, None, None],
        };
        bytes += std::mem::size_of::<Mutation>();
        for value in strings.into_iter().flatten() {
            if value.len() > SurfaceTree::MAX_STRING_BYTES {
                return Err("string byte limit");
            }
            bytes += value.len();
        }
        if bytes > SurfaceTree::MAX_BATCH_BYTES {
            return Err("batch byte limit");
        }
    }
    Ok(())
}

impl wit_presentation::Host for WasmStoreState {}

impl wit_presentation::HostMountPoint for WasmStoreState {
    fn open(
        &mut self,
        placement: Resource<WasmPlacement>,
    ) -> wasmtime::Result<Result<Resource<WasmSurface>, wit_presentation::PresentationError>> {
        let granted = self.table.get_mut(&placement)?;
        if granted.revoked.load(Ordering::Acquire) || !granted.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        if granted.surface_identity.is_some() {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "placement already opened".into(),
            )));
        }
        if self.surface_handles >= Self::MAX_SURFACE_HANDLES {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "surface handle limit".into(),
            )));
        }
        self.next_surface = self
            .next_surface
            .checked_add(1)
            .expect("surface identity overflow");
        granted.surface_identity = Some(self.next_surface);
        let surface = WasmSurface {
            writer: granted.writer.clone(),
            revoked: granted.revoked.clone(),
            identity: self.next_surface,
            ownership: Arc::new(()),
        };
        let resource = self.table.push(surface)?;
        self.surface_handles += 1;
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
        if let Err(reason) = ingress_budget(&edits) {
            surface.writer.withdraw();
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                reason.into(),
            )));
        }
        let converted: Vec<Mutation> = edits.into_iter().map(convert_mutation).collect();
        Ok(surface.writer.apply(&converted).map_err(convert_error))
    }

    fn retain(
        &mut self,
        surface: Resource<WasmSurface>,
    ) -> wasmtime::Result<Result<Resource<WasmSurface>, wit_presentation::PresentationError>> {
        let surface = self.table.get(&surface)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        if self.surface_handles >= Self::MAX_SURFACE_HANDLES {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "surface handle limit".into(),
            )));
        }
        let retained = self.table.push(surface.clone())?;
        self.surface_handles += 1;
        Ok(Ok(retained))
    }

    fn identity(&mut self, surface: Resource<WasmSurface>) -> wasmtime::Result<u64> {
        Ok(self.table.get(&surface)?.identity)
    }

    fn drop(&mut self, surface: Resource<WasmSurface>) -> wasmtime::Result<()> {
        self.table.delete(surface)?;
        self.surface_handles -= 1;
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
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(|error| error.to_string())?;
        let component = Component::new(&engine, bytes).map_err(|err| err.to_string())?;
        let mut linker = Linker::new(&engine);
        MicroClient::add_to_linker::<_, HasSelf<WasmStoreState>>(&mut linker, |state| state)
            .map_err(|err| err.to_string())?;
        Ok(Self {
            engine,
            component,
            linker: Arc::new(linker),
        })
    }

    fn instantiate(
        &self,
        instance: Instance,
    ) -> Result<(WasmIncarnation, mpsc::Receiver<documents::Work>), ActivationError> {
        let (work, incoming) = mpsc::channel(WasmStoreState::MAX_PENDING_DELIVERIES);
        let mut store = Store::new(&self.engine, WasmStoreState::new(instance, work));
        store.limiter(|state| &mut state.limits);
        store.set_fuel(WasmIncarnation::FUEL_PER_CALL).unwrap();
        let client = MicroClient::instantiate(&mut store, &self.component, &self.linker).map_err(
            |error| {
                ActivationError::Initialization(format!("instantiating Wasm component: {error:#}"))
            },
        )?;
        Ok((
            WasmIncarnation {
                store,
                client,
                fuel_per_call: WasmIncarnation::FUEL_PER_CALL,
            },
            incoming,
        ))
    }
}

struct WasmIncarnation {
    store: Store<WasmStoreState>,
    client: MicroClient,
    fuel_per_call: u64,
}

impl WasmIncarnation {
    const FUEL_PER_CALL: u64 = 50_000_000;

    /// Every host-initiated guest entry gets a fresh, bounded instruction budget, so a guest that
    /// loops forever inside one synchronous call traps with `OutOfFuel` instead of hanging the UI
    /// or document driver that called it.
    fn refuel(&mut self) {
        self.store.set_fuel(self.fuel_per_call).unwrap();
    }
}

#[derive(Clone)]
pub struct WasmMicro {
    pub instance: Instance,
    pub detail: ComponentRef,
    incarnation: Arc<Mutex<Option<WasmIncarnation>>>,
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
        activation: &MicroActivation,
        package: &WasmPackage,
        native: &NativeComponents,
        configs: BTreeMap<String, DocumentRef>,
    ) -> Result<Self, ActivationError> {
        let instance = activation.instance();
        let (mut active, incoming) = package.instantiate(instance.clone())?;
        documents::activate(&mut active, configs)?;
        let incarnation = Arc::new(Mutex::new(Some(active)));
        activation.own_future(documents::drive(
            instance.clone(),
            incarnation.clone(),
            incoming,
        ))?;
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
            let surface_rep = {
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
                        surface_identity: None,
                    })
                    .map_err(|_| PresentationError::Revoked)?;
                let placement_rep = placement.rep();
                active.refuel();
                let mount_result = active
                    .client
                    .townframe_dayframe_presentation_events()
                    .call_mount(&mut active.store, Resource::new_borrow(placement_rep));
                let placement = active
                    .store
                    .data_mut()
                    .table
                    .delete(Resource::<WasmPlacement>::new_own(placement_rep))
                    .expect("borrowed placement must remain in the host resource table");
                match mount_result {
                    Ok(Ok(surface)) => {
                        let granted = active
                            .store
                            .data()
                            .table
                            .get(&surface)
                            .expect("returned surface must be in the host resource table");
                        if placement.surface_identity != Some(granted.identity) {
                            wit_presentation::HostSurface::drop(active.store.data_mut(), surface)
                                .expect("returned surface must remain in the host resource table");
                            writer.withdraw();
                            return Err(PresentationError::InvalidBatch(
                                "guest returned a surface from another placement".into(),
                            ));
                        }
                        surface.rep()
                    }
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
            let drop_instance = instance_for_surface.clone();
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
                        active.refuel();
                        let outcome = active
                            .client
                            .townframe_dayframe_presentation_events()
                            .call_dispatch(
                                &mut active.store,
                                Resource::new_borrow(surface_rep),
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
                    let mut guard = drop_incarnation.lock().unwrap();
                    let trapped = if let Some(active) = guard.as_mut() {
                        active.refuel();
                        let outcome = active
                            .client
                            .townframe_dayframe_presentation_events()
                            .call_unmount(&mut active.store, Resource::new_borrow(surface_rep));
                        if outcome.is_err() {
                            guard.take();
                            true
                        } else {
                            wit_presentation::HostSurface::drop(
                                active.store.data_mut(),
                                Resource::new_own(surface_rep),
                            )
                            .expect("mounted surface must remain in the host resource table");
                            false
                        }
                    } else {
                        false
                    };
                    drop(guard);
                    drop_writer.withdraw();
                    if trapped {
                        drop_instance.close();
                    }
                },
            ))
        })?;

        Ok(Self {
            instance,
            detail,
            incarnation,
        })
    }

    pub fn surface(&self) -> ComponentRef {
        self.detail
    }

    /// Withdraws this incarnation's execution budget, so the next guest entry (event dispatch,
    /// mount, or document callback) traps with `OutOfFuel`. This drives the same trap recovery a
    /// runaway guest takes: the store is discarded, the instance closes, and its contributions are
    /// withdrawn while sibling micros keep running.
    pub fn exhaust_fuel(&self) {
        if let Some(active) = self.incarnation.lock().unwrap().as_mut() {
            active.fuel_per_call = 0;
        }
    }

    pub fn close(&self) {
        self.instance.close();
    }
}

pub fn provider(id: MicroId, package: WasmPackage, native: NativeComponents) -> AsyncMicroFactory {
    AsyncMicroFactory::new(
        MicroDescriptor {
            id,
            version: 1,
            requirements: vec![],
            provisions: vec![MicroProvision {
                id: WasmMicro::ID.into(),
                version: 1,
            }],
        },
        move |activation| {
            let package = package.clone();
            let native = native.clone();
            async move {
                let micro = WasmMicro::activate(&activation, &package, &native, BTreeMap::new())?;
                activation.publish(micro);
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
        host::{AccessError, Host, HostedCollection},
        micros::{
            AsyncMicroFactory, MicroRegistry,
            composition::{ConfiguredMicro, Enablement, InstanceState},
        },
        native::ComponentMount,
    };
    use dioxus::prelude::*;
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedMouseData};
    use std::collections::BTreeMap;
    use wasmtime::component::Resource;

    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn counter_package() -> WasmPackage {
        let path = std::env::var("DAYFRAME_COUNTER_COMPONENT")
            .expect("prepare the counter fixture with x/check-dayframe.ts --prepare-only and set DAYFRAME_COUNTER_COMPONENT");
        WasmPackage::from_bytes(
            std::fs::read(path).expect("prepared counter component is readable"),
        )
        .expect("counter component is valid")
    }

    #[test]
    fn ingress_budget_rejects_oversized_batches_before_conversion() {
        use wit_presentation::Mutation as Wire;
        assert_eq!(
            ingress_budget(&[Wire::Pop, Wire::CreateText("ok".into())]),
            Ok(())
        );
        assert_eq!(
            ingress_budget(&vec![Wire::Pop; SurfaceTree::MAX_BATCH_MUTATIONS + 1]),
            Err("batch mutation limit")
        );
        assert_eq!(
            ingress_budget(&[Wire::SetAttribute((
                "title".into(),
                None,
                wit_presentation::AttributeValue::Text(
                    "x".repeat(SurfaceTree::MAX_STRING_BYTES + 1)
                ),
            ))]),
            Err("string byte limit")
        );
        let text = "x".repeat(SurfaceTree::MAX_STRING_BYTES);
        let batch = vec![
            Wire::CreateText(text);
            SurfaceTree::MAX_BATCH_BYTES / SurfaceTree::MAX_STRING_BYTES + 1
        ];
        assert_eq!(ingress_budget(&batch), Err("batch byte limit"));
    }

    #[test]
    fn execution_and_linear_memory_are_bounded_per_incarnation() {
        let host = Host::new();
        let package = counter_package();
        let (mut active, _incoming) = package.instantiate(host.start("bounded")).unwrap();
        let module = wasmtime::Module::new(
            &package.engine,
            r#"(module
            (memory (export "memory") 1)
            (func (export "loop") (loop br 0))
        )"#,
        )
        .unwrap();
        let instance = wasmtime::Instance::new(&mut active.store, &module, &[]).unwrap();
        let memory = instance.get_memory(&mut active.store, "memory").unwrap();
        assert!(memory.grow(&mut active.store, 1024).is_err());
        let looping = instance
            .get_typed_func::<(), ()>(&mut active.store, "loop")
            .unwrap();
        active.store.set_fuel(16).unwrap();
        let trap = looping.call(&mut active.store, ()).unwrap_err();
        assert_eq!(
            trap.downcast_ref::<wasmtime::Trap>(),
            Some(&wasmtime::Trap::OutOfFuel)
        );
        active.refuel();
        assert_eq!(active.store.get_fuel().unwrap(), 50_000_000);
        host.close();
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
    fn retained_surface_handles_are_bounded_and_release_capacity() {
        use wit_presentation::{HostMountPoint, HostSurface};

        let host = Host::new();
        let owner = host.start("surface-budget");
        let native = NativeComponents::new(&host);
        let (work, _incoming) = mpsc::channel(1);
        let state = Arc::new(Mutex::new(WasmStoreState::new(owner.clone(), work)));
        let capture = state.clone();
        let reference = native
            .publish_surface(&owner, move |_mount, writer| {
                let mut state = capture.lock().unwrap();
                let placement = state
                    .table
                    .push(WasmPlacement {
                        writer: writer.clone(),
                        revoked: Arc::new(AtomicBool::new(false)),
                        surface_identity: None,
                    })
                    .unwrap();
                let surface =
                    HostMountPoint::open(&mut *state, Resource::new_borrow(placement.rep()))
                        .unwrap()
                        .unwrap();
                let identity =
                    HostSurface::identity(&mut *state, Resource::new_borrow(surface.rep()))
                        .unwrap();
                let mut retained = Vec::new();
                for _ in 1..WasmStoreState::MAX_SURFACE_HANDLES {
                    retained.push(
                        HostSurface::retain(&mut *state, Resource::new_borrow(surface.rep()))
                            .unwrap()
                            .unwrap(),
                    );
                }
                assert!(matches!(
                    HostSurface::retain(&mut *state, Resource::new_borrow(surface.rep())).unwrap(),
                    Err(wit_presentation::PresentationError::Unsupported(_))
                ));
                assert!(writer.is_live());
                HostSurface::drop(&mut *state, retained.pop().unwrap()).unwrap();
                let replacement =
                    HostSurface::retain(&mut *state, Resource::new_borrow(surface.rep()))
                        .unwrap()
                        .unwrap();
                assert_eq!(
                    HostSurface::identity(&mut *state, Resource::new_borrow(replacement.rep()))
                        .unwrap(),
                    identity
                );
                HostSurface::drop(&mut *state, surface).unwrap();
                for resource in retained {
                    HostSurface::drop(&mut *state, resource).unwrap();
                }
                assert!(writer.is_live());
                HostSurface::drop(&mut *state, replacement).unwrap();
                assert!(!writer.is_live());
                assert_eq!(state.surface_handles, 0);
                HostMountPoint::drop(&mut *state, placement).unwrap();
                Ok(SurfaceSession::new(|_| EventResponse::default()))
            })
            .unwrap();
        let mut dom = VirtualDom::new_with_props(
            mount_host_app,
            MountProps {
                components: native,
                owner,
                reference,
            },
        );
        dom.rebuild(&mut SurfaceTree::new());
        host.close();
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
        let states = block_on(app.configure(&config)).unwrap();
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
            host_tree
                .listener_id("button", "click")
                .expect("host tree rendered the counter button"),
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

        app.register_provider(AsyncMicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("dayframe.wasm.counter"),
                version: 1,
                requirements: vec![],
                provisions: vec![MicroProvision {
                    id: WasmMicro::ID.into(),
                    version: 1,
                }],
            },
            move |activation| {
                let package = package.clone();
                let native = native.clone();
                let collection = collection_for_provider.clone();
                async move {
                    let instance = activation.instance();
                    let micro =
                        WasmMicro::activate(&activation, &package, &native, BTreeMap::new())?;
                    collection.writer(&instance).insert(micro.surface())?;
                    activation.publish(micro);
                    Ok(())
                }
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
        block_on(app.configure(&config)).unwrap();

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

        first_wasm.exhaust_fuel();
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
        assert_eq!(first_wasm.instance.check(), Err(AccessError::Revoked));
        assert!(reader.snapshot().unwrap().is_empty());

        let states = block_on(app.configure(&config)).unwrap();
        assert_eq!(states["counter"], InstanceState::Stopped);

        config.get_mut("counter").unwrap().enablement = Enablement::Disabled;
        let states = block_on(app.configure(&config)).unwrap();
        assert_eq!(states["counter"], InstanceState::Disabled);

        config.get_mut("counter").unwrap().enablement = Enablement::Enabled;
        let states = block_on(app.configure(&config)).unwrap();
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
                        Err(AccessError::Denied.into())
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
            Err(dayframe_app::micros::MicroRegistryError::Access(
                AccessError::Denied
            ))
        ));
        let (failed_instance, _) = leaked.lock().unwrap().take().unwrap();
        assert_eq!(failed_instance.check(), Err(AccessError::Revoked));
        host.close();
    }
}
