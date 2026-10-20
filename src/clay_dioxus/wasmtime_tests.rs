use super::*;
use crate::{ComponentMount, application::ApplicationRuntime};
use clay_compose::micros::{
    ConfigManifest, MicroDescriptor, MicroId, MicroProvision,
    composition::{ConfiguredMicro, Enablement, InstanceState},
};
use clay_host::{Host, HostedCollection};
use clay_surface::SurfaceTree;
use dioxus::prelude::*;
use dioxus_core::{RenderTargetId, VirtualDom};
use dioxus_html::{EventData, SerializedMouseData};
use std::cell::{Cell, RefCell};

fn counter_package() -> WasmPackage {
    let path = std::env::var("DAYFRAME_COUNTER_COMPONENT")
        .expect("prepare the counter with x/check-dayframe.ts --prepare-only");
    WasmPackage::from_bytes(std::fs::read(path).unwrap()).unwrap()
}

fn manifest() -> MicroManifest<u32> {
    MicroManifest {
        descriptor: MicroDescriptor {
            id: MicroId::new("test.wasm"),
            version: 1,
            requirements: vec![],
            provisions: vec![MicroProvision {
                id: WasmMicro::ID.into(),
                version: 1,
            }],
        },
        config: None,
        contributions: vec![],
    }
}

fn configuration() -> BTreeMap<String, ConfiguredMicro> {
    BTreeMap::from([(
        "counter".into(),
        ConfiguredMicro {
            provider: MicroId::new("test.wasm"),
            enablement: Enablement::Enabled,
            bindings: BTreeMap::new(),
        },
    )])
}

#[derive(Clone, PartialEq)]
struct MountProps {
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
}

fn mount_host_app(props: MountProps) -> Element {
    rsx! { div {
        ComponentMount { components: props.components, owner: props.owner, reference: props.reference }
    } }
}

fn mounted(app: &ApplicationRuntime, micro: &WasmMicro) -> (VirtualDom, SurfaceTree) {
    let mut dom = VirtualDom::new_with_props(
        mount_host_app,
        MountProps {
            components: app.native(),
            owner: app.root(),
            reference: micro.surface(),
        },
    );
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    (dom, tree)
}

fn click(dom: &mut VirtualDom, tree: &mut SurfaceTree) {
    let event = dioxus_core::Event::new(
        EventData::Mouse(SerializedMouseData::default()).into_any(),
        true,
    );
    dom.runtime().handle_event_for_target(
        RenderTargetId::ROOT,
        "click",
        event,
        tree.listener_id("button", "click").unwrap(),
    );
    dom.render_immediate(tree);
}

#[tokio::test]
async fn wasm_layout_slots_compose_interactive_wasm_children_and_isolate_withdrawal() {
    let host = Host::new();
    let native = NativeComponents::new(&host);
    let viewer = host.start("viewer");
    let child = host.start("counter child");
    let layout = host.start("wasm layout");
    let (counter, _counter_driver) =
        WasmRuntime::activate(&host, child.clone(), &counter_package(), BTreeMap::new()).unwrap();
    let child_reference = native
        .publish_surface(&child, move |_, writer| {
            counter.mount(Arc::new(writer), &[])
        })
        .unwrap();
    let path = std::env::var("DAYFRAME_SLOTS_COMPONENT")
        .expect("prepare the slots fixture with x/check-dayframe.ts --prepare-only");
    let package = WasmPackage::from_bytes(std::fs::read(path).unwrap()).unwrap();
    let (runtime, _layout_driver) =
        WasmRuntime::activate(&host, layout.clone(), &package, BTreeMap::new()).unwrap();
    let foreign = Host::new();
    let foreign_owner = foreign.start("foreign");
    let foreign_reference = foreign.publish_component(&foreign_owner).unwrap();
    let placement = host.publish_component(&viewer).unwrap();
    let probe = crate::SurfaceWriter::new(Arc::new(host.mount(&viewer, placement).unwrap()));
    assert!(matches!(
        runtime.mount(Arc::new(probe.clone()), &[foreign_reference]),
        Err(clay_surface::PresentationError::InvalidBatch(_))
    ));
    assert!(probe.is_live());
    let layout_runtime = runtime.clone();
    let reference = native
        .publish_surface(&layout, move |_, writer| {
            layout_runtime.mount(Arc::new(writer), &[child_reference])
        })
        .unwrap();
    let mut dom = VirtualDom::new_with_props(
        mount_host_app,
        MountProps {
            components: native,
            owner: viewer,
            reference,
        },
    );
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert_eq!(tree.text_content(), "before0after");
    click(&mut dom, &mut tree);
    assert_eq!(tree.text_content(), "before1after");
    child.close();
    assert!(matches!(
        runtime.mount(Arc::new(probe), &[child_reference]),
        Err(clay_surface::PresentationError::Revoked)
    ));
    dom.wait_for_work().await;
    dom.render_immediate(&mut tree);
    assert_eq!(tree.text_content(), "beforeComponent withdrawn.after");
    assert_eq!(layout.check(), Ok(()));
    drop(dom);
    host.close();
    foreign.close();
}

#[tokio::test]
async fn wasm_provider_mounts_surface_and_updates_on_click() {
    let mut app = ApplicationRuntime::new();
    app.register_provider(provider(
        manifest(),
        counter_package(),
        app.native(),
        |_, _, _| Ok(()),
    ))
    .unwrap();
    let states = app.configure(&configuration()).await.unwrap();
    assert_eq!(states["counter"], InstanceState::Active);
    let micro = app.output::<WasmMicro>("counter").unwrap();
    let (mut dom, mut tree) = mounted(&app, &micro);
    assert_eq!(tree.text_content(), "0");
    click(&mut dom, &mut tree);
    assert_eq!(tree.text_content(), "1");
    app.host().close();
}

#[tokio::test]
async fn wasm_guest_trap_or_disablement_closes_instance_and_withdraws_owned_entries() {
    let mut app = ApplicationRuntime::new();
    let collection = HostedCollection::<ComponentRef>::new(&app.host(), &app.root());
    let entries = collection.clone();
    let mut declared = manifest();
    declared.contributions = vec![0];
    app.register_provider(provider(
        declared,
        counter_package(),
        app.native(),
        move |activation, _, detail| {
            entries.writer(&activation.instance()).insert(detail)?;
            Ok(())
        },
    ))
    .unwrap();
    let mut config = configuration();
    app.configure(&config).await.unwrap();
    let reader = collection.reader(&app.root());
    let first = app.output::<WasmMicro>("counter").unwrap();
    assert_eq!(
        reader.snapshot().unwrap().into_values().collect::<Vec<_>>(),
        vec![first.surface()]
    );
    let (mut dom, mut tree) = mounted(&app, &first);
    assert_eq!(tree.text_content(), "0");
    first.exhaust_fuel();
    click(&mut dom, &mut tree);
    assert_eq!(first.instance.check(), Err(AccessError::Revoked));
    assert!(reader.snapshot().unwrap().is_empty());
    assert_eq!(
        app.configure(&config).await.unwrap()["counter"],
        InstanceState::Stopped
    );
    config.get_mut("counter").unwrap().enablement = Enablement::Disabled;
    assert_eq!(
        app.configure(&config).await.unwrap()["counter"],
        InstanceState::Disabled
    );
    config.get_mut("counter").unwrap().enablement = Enablement::Enabled;
    assert_eq!(
        app.configure(&config).await.unwrap()["counter"],
        InstanceState::Active
    );
    let second = app.output::<WasmMicro>("counter").unwrap();
    assert!(second.instance != first.instance);
    assert_eq!(
        reader.snapshot().unwrap().into_values().collect::<Vec<_>>(),
        vec![second.surface()]
    );
    app.host().close();
}

#[tokio::test]
async fn failed_contribution_rolls_back_surfaces_entries_and_publications() {
    let mut app = ApplicationRuntime::new();
    let collection = HostedCollection::<ComponentRef>::new(&app.host(), &app.root());
    let entries = collection.clone();
    let leaked = Rc::new(RefCell::new(None));
    let capture = leaked.clone();
    let mut declared = manifest();
    declared.contributions = vec![1, 2];
    app.register_provider(provider(
        declared,
        counter_package(),
        app.native(),
        move |activation, value, detail| {
            if *value == 2 {
                return Err(AccessError::Denied);
            }
            let instance = activation.instance();
            entries.writer(&instance).insert(detail)?;
            *capture.borrow_mut() = Some((instance, detail));
            Ok(())
        },
    ))
    .unwrap();
    let states = app.configure(&configuration()).await.unwrap();
    assert!(matches!(states["counter"], InstanceState::Failed(_)));
    let (instance, detail) = leaked.borrow_mut().take().unwrap();
    assert_eq!(instance.check(), Err(AccessError::Revoked));
    assert!(
        collection
            .reader(&app.root())
            .snapshot()
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        app.host().mount(&app.root(), detail),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        app.output::<WasmMicro>("counter"),
        Err(AccessError::Revoked)
    ));
    app.host().close();
}

fn config_manifest(read: bool, write: bool) -> ConfigManifest {
    ConfigManifest {
        name: "counter".into(),
        facets: vec![daybook_types::manifest::RoutineFacetAccess {
            tag: "org.dayframe.counter.config".into(),
            key_id: Some("count".into()),
            read,
            write,
            ..Default::default()
        }],
    }
}

fn seed(app: &ApplicationRuntime) {
    let documents = app.mock_documents();
    documents.bind_config("counter".into(), "counter-config".into(), "main".into());
    documents.seed(
        clay_data::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: "org.dayframe.counter.config".into(),
            key_id: "count".into(),
        },
        serde_json::json!(0),
    );
}

#[tokio::test]
async fn manifest_read_only_grant_cannot_be_escalated_by_guest_mutation() {
    let mut app = ApplicationRuntime::new();
    seed(&app);
    let mut declared = manifest();
    declared.config = Some(config_manifest(true, false));
    app.register_provider(provider(
        declared,
        counter_package(),
        app.native(),
        |_, _, _| Ok(()),
    ))
    .unwrap();
    assert_eq!(
        app.configure(&configuration()).await.unwrap()["counter"],
        InstanceState::Active
    );
    let micro = app.output::<WasmMicro>("counter").unwrap();
    let (mut dom, mut tree) = mounted(&app, &micro);
    assert_eq!(
        tree.text_content(),
        "0Pending counter writes: 0Refresh counter"
    );
    click(&mut dom, &mut tree);
    assert_eq!(
        tree.text_content(),
        "0Pending counter writes: 0Write failed: DocumentError::DeniedRefresh counter"
    );
    assert!(micro.instance.check().is_ok());
    app.host().close();
}

#[tokio::test]
async fn guest_activation_failure_does_not_run_contributions_or_publish_a_controller() {
    let mut app = ApplicationRuntime::new();
    seed(&app);
    let mut declared = manifest();
    declared.config = Some(config_manifest(false, true));
    declared.contributions = vec![1];
    let called = Rc::new(Cell::new(false));
    let capture = called.clone();
    app.register_provider(provider(
        declared,
        counter_package(),
        app.native(),
        move |_, _, _| {
            capture.set(true);
            Ok(())
        },
    ))
    .unwrap();
    let states = app.configure(&configuration()).await.unwrap();
    assert_eq!(
        states["counter"],
        InstanceState::Failed(clay_compose::micros::MicroRegistryError::Initialization(
            "counter configuration access failed: DocumentError::Denied".into(),
        ),)
    );
    assert!(!called.get());
    assert!(matches!(
        app.output::<WasmMicro>("counter"),
        Err(AccessError::Revoked)
    ));
    app.host().close();
}
