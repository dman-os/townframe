use super::*;
use crate::ConfigGrant;
use crate::Contribution;
use crate::FacetAccess;
use crate::MicroId;
use crate::Provision;
use crate::ProvisionRef;
use crate::native::Collection;
use crate::native::Contributable;
use crate::native::output;
use clay_dioxus::ComponentMount;
use clay_dioxus::SurfaceWriter;
use clay_host::AccessError;
use clay_host::Host;
use clay_micro_core::micros::MicroId as ComposeMicroId;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::InstanceState;
use clay_surface::SurfaceTree;
use dioxus::prelude::*;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_html::EventData;
use dioxus_html::SerializedMouseData;
use std::sync::Mutex;

/// The composition runtime with the Dioxus registry installed: what these tests compose into and
/// what [`provider`] assembles for.
type ApplicationRuntime = clay_micro_core::application::ApplicationRuntime<NativeComponents>;

fn counter_package() -> WasmPackage {
    super::component_package("counter")
}

/// A native contract that accepts declared contributions: the composition-side half every
/// wasm contribution needs. It shelves each contributor's surface in a host collection (so
/// withdrawal is observable) and records who contributed what (so routing is observable).
#[derive(Clone)]
struct Shelf {
    entries: Collection<ComponentRef>,
    seen: Arc<Mutex<Vec<(Instance, ComponentRef)>>>,
}

impl Provision for Shelf {
    const ID: &'static str = "test.shelf";
}

impl Contributable for Shelf {
    fn contribute(
        &self,
        contributor: &Instance,
        payload: &serde_json::Value,
        surface: ComponentRef,
    ) -> Result<(), AccessError> {
        if payload["reject"].as_bool().unwrap() {
            return Err(AccessError::Denied);
        }
        self.entries.writer(contributor)?.insert(surface)?;
        self.seen
            .lock()
            .unwrap()
            .push((contributor.clone(), surface));
        Ok(())
    }
}

fn shelf_provider(app: &ApplicationRuntime) -> MicroFactory {
    crate::native::provider(
        app,
        MicroManifest {
            id: MicroId::new("test.shelf"),
            version: 1,
            requirements: vec![],
            provisions: vec![ProvisionRef::of::<Shelf>()],
            config: None,
            contributions: vec![],
        },
        |context| {
            context.publish_contributable(Shelf {
                entries: context.collection()?,
                seen: Arc::default(),
            });
            Ok(())
        },
    )
}

fn contribution(reject: bool) -> Contribution {
    Contribution {
        provision: Shelf::ID.into(),
        payload: serde_json::json!({ "reject": reject }),
    }
}

fn manifest() -> MicroManifest {
    MicroManifest {
        id: MicroId::new("test.wasm"),
        version: 1,
        requirements: vec![ProvisionRef::of::<Shelf>()],
        provisions: vec![ProvisionRef {
            id: WasmMicro::ID.into(),
            version: 1,
        }],
        config: None,
        contributions: vec![],
    }
}

/// A runtime with the shelf registered and the counter provider for `declared` beside it.
fn composed(declared: MicroManifest) -> ApplicationRuntime {
    let mut app = ApplicationRuntime::new();
    app.register_provider(shelf_provider(&app)).unwrap();
    let counter = provider(&app, declared, counter_package());
    app.register_provider(counter).unwrap();
    app
}

fn configuration() -> BTreeMap<String, ConfiguredMicro> {
    BTreeMap::from([
        (
            "shelf".into(),
            ConfiguredMicro {
                provider: ComposeMicroId::new("test.shelf"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        ),
        (
            "counter".into(),
            ConfiguredMicro {
                provider: ComposeMicroId::new("test.wasm"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::from([(Shelf::ID.into(), "shelf".into())]),
            },
        ),
    ])
}

fn shelved(app: &ApplicationRuntime) -> (Vec<ComponentRef>, Vec<(Instance, ComponentRef)>) {
    let shelf = output::<Shelf, _>(app, "shelf").unwrap();
    let entries = shelf
        .entries
        .reader(&app.root())
        .snapshot()
        .unwrap()
        .into_values()
        .collect();
    let seen = shelf.seen.lock().unwrap().clone();
    (entries, seen)
}

#[derive(Clone, PartialEq)]
struct MountProps {
    components: NativeComponents,
    owner: Instance,
    reference: clay_host::ComponentRef,
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
            components: app.renderer(),
            owner: app.root(),
            reference: micro.surface().host(),
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
    let package = super::component_package("slots");
    let (runtime, _layout_driver) =
        WasmRuntime::activate(&host, layout.clone(), &package, BTreeMap::new()).unwrap();
    let foreign = Host::new();
    let foreign_owner = foreign.start("foreign");
    let foreign_reference = foreign.publish_component(&foreign_owner).unwrap();
    let placement = host.publish_component(&viewer).unwrap();
    let probe = SurfaceWriter::new(Arc::new(host.mount(&viewer, placement).unwrap()));
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
    host.close_instance(&child).unwrap();
    assert!(matches!(
        runtime.mount(Arc::new(probe), &[child_reference]),
        Err(clay_surface::PresentationError::Revoked)
    ));
    dom.wait_for_work().await;
    dom.render_immediate(&mut tree);
    assert_eq!(tree.text_content(), "beforeComponent withdrawn.after");
    assert_eq!(host.check(&layout), Ok(()));
    drop(dom);
    host.close();
    foreign.close();
}

#[tokio::test]
async fn wasm_provider_mounts_surface_and_updates_on_click() {
    let app = composed(manifest());
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
    let mut declared = manifest();
    declared.contributions = vec![contribution(false)];
    let app = composed(declared);
    let mut config = configuration();
    app.configure(&config).await.unwrap();
    let first = app.output::<WasmMicro>("counter").unwrap();
    assert_eq!(shelved(&app).0, vec![first.surface()]);
    let (mut dom, mut tree) = mounted(&app, &first);
    assert_eq!(tree.text_content(), "0");
    first.exhaust_fuel();
    click(&mut dom, &mut tree);
    assert_eq!(app.host().check(&first.instance), Err(AccessError::Revoked));
    assert!(shelved(&app).0.is_empty());
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
    assert_eq!(shelved(&app).0, vec![second.surface()]);
    app.host().close();
}

#[tokio::test]
async fn failed_contribution_rolls_back_surfaces_entries_and_publications() {
    let mut declared = manifest();
    declared.contributions = vec![contribution(false), contribution(true)];
    let app = composed(declared);
    let states = app.configure(&configuration()).await.unwrap();
    assert!(matches!(states["counter"], InstanceState::Failed(_)));
    let (entries, seen) = shelved(&app);
    let [(instance, detail)] = seen.as_slice() else {
        panic!("exactly the accepted contribution reached the shelf: {seen:?}");
    };
    let detail = *detail;
    assert_eq!(app.host().check(instance), Err(AccessError::Revoked));
    assert!(entries.is_empty());
    assert!(matches!(
        app.host().mount(&app.root(), detail.host()),
        Err(AccessError::Revoked)
    ));
    assert!(matches!(
        app.output::<WasmMicro>("counter"),
        Err(AccessError::Revoked)
    ));
    app.host().close();
}

fn config_grant(read: bool, write: bool) -> ConfigGrant {
    ConfigGrant {
        name: "counter".into(),
        facets: vec![FacetAccess {
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
        clay_docs::FacetRoute {
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
    let mut declared = manifest();
    declared.config = Some(config_grant(true, false));
    let app = composed(declared);
    seed(&app);
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
    assert!(app.host().check(&micro.instance).is_ok());
    app.host().close();
}

#[tokio::test]
async fn guest_activation_failure_does_not_run_contributions_or_publish_a_controller() {
    let mut declared = manifest();
    declared.config = Some(config_grant(false, true));
    declared.contributions = vec![contribution(false)];
    let app = composed(declared);
    seed(&app);
    let states = app.configure(&configuration()).await.unwrap();
    assert_eq!(
        states["counter"],
        InstanceState::Failed(clay_micro_core::micros::MicroRegistryError::Initialization(
            "DocumentError::Denied".into(),
        ),)
    );
    assert!(shelved(&app).1.is_empty());
    assert!(matches!(
        app.output::<WasmMicro>("counter"),
        Err(AccessError::Revoked)
    ));
    app.host().close();
}
