use super::*;
use crate::ConfigurationRequest;
use crate::Entrypoints;
use crate::FacetAccess;
use crate::MANIFEST_SCHEMA;
use crate::MicroId;
use crate::PublicationDeclaration;
use crate::PublicationKind;
use clay_dioxus::ComponentMount;
use clay_dioxus::SurfaceWriter;
use clay_docs::DocumentService;
use clay_guest_session::InitialContext;
use clay_guest_session::capabilities::ContractIdentity;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_host::AccessError;
use clay_host::CollectionRegistry;
use clay_host::ConfiguredMicro;
use clay_host::Lease;
use clay_host::MicroState;
use clay_host::StartError;
use clay_host::Supervisor;
use clay_rt_wasmtime::WasmRuntime;
use clay_surface::SurfaceTree;
use dioxus::prelude::*;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_html::EventData;
use dioxus_html::SerializedMouseData;

async fn counter_package() -> WasmPackage {
    super::component_package("counter")
        .await
        .expect("prepared counter component is valid")
}

/// The counter's manifest, publishing its presentation as `view`, optionally requesting its
/// configuration document with the given `(read, write)` access.
/// A context whose manifest declares the presentation publication `view`.
fn declaring_view() -> InitialContext {
    InitialContext {
        publications: BTreeMap::from([(
            "view".to_owned(),
            DeclaredPublication {
                kind: clay_guest_session::capabilities::PublicationKind::Presentation,
                contract: ContractIdentity::presentation_component(),
            },
        )]),
        ..InitialContext::default()
    }
}

fn manifest(view: &str, config: Option<(bool, bool)>) -> MicroManifest {
    MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: MicroId::new("test.wasm"),
        package_version: 1,
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: view.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
        configuration_requests: config
            .map(|(read, write)| ConfigurationRequest {
                binding: "counter".into(),
                facets: vec![FacetAccess {
                    tag: "org.dayframe.counter.config".into(),
                    key_id: Some("count".into()),
                    read,
                    write,
                    ..Default::default()
                }],
            })
            .into_iter()
            .collect(),
        collection_contracts: vec![],
        collection_requests: vec![],
        contributions: vec![],
        extra: BTreeMap::new(),
    }
}

struct Rig {
    host: Host,
    resources: Resources<NativeComponents>,
    supervisor: Supervisor<clay_guest_session::capabilities::CapabilityPublication>,
    id: clay_host::MicroId,
}

impl Rig {
    async fn start(declared: MicroManifest) -> Self {
        let host = Host::new();
        let resources = Resources {
            registry: NativeComponents::new(&host),
            documents: DocumentService::new(16),
            collections: CollectionRegistry::default(),
        };
        let package = counter_package().await;
        let id = clay_host::MicroId::new("test.wasm");
        let micro = ConfiguredMicro {
            identity: id.clone(),
            eager: false,
            enabled: true,
            requirements: BTreeMap::new(),
            unresolved_bindings: Vec::new(),
            factory: factory(
                resources.clone(),
                declared,
                CollectionBindings::default(),
                move || {
                    let package = package.clone();
                    async move { Ok(package) }
                },
            ),
        };
        let supervisor = Supervisor::new(host.clone(), [micro]);
        Self {
            host,
            resources,
            supervisor,
            id,
        }
    }

    async fn view(
        &self,
    ) -> Result<Lease<clay_guest_session::capabilities::CapabilityPublication>, StartError> {
        self.supervisor.acquire(&self.id, "view").await
    }

    /// Mount the leased presentation the way a shell does, through the renderer registry.
    fn mounted(
        &self,
        view: &Lease<clay_guest_session::capabilities::CapabilityPublication>,
    ) -> (VirtualDom, SurfaceTree) {
        let mut dom = VirtualDom::new_with_props(
            mount_host_app,
            MountProps {
                components: self.resources.registry.clone(),
                owner: self.host.start("viewer"),
                reference: view.component().expect("a presentation has a component"),
            },
        );
        let mut tree = SurfaceTree::new();
        dom.rebuild(&mut tree);
        (dom, tree)
    }

    fn seed(&self) {
        let documents = self.resources.documents.mock_documents();
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

async fn in_local_set<T>(future: impl Future<Output = T>) -> T {
    tokio::task::LocalSet::new().run_until(future).await
}

#[tokio::test]
async fn wasm_layout_slots_compose_interactive_wasm_children_and_isolate_withdrawal() {
    let host = Host::new();
    let native = NativeComponents::new(&host);
    let viewer = host.start("viewer");
    let child = host.start("counter child");
    let layout = host.start("wasm layout");
    let (counter, _counter_driver) = WasmRuntime::initialize(
        &host,
        child.clone(),
        &counter_package().await,
        declaring_view(),
    )
    .await
    .unwrap();
    let child_reference = native
        .publish_surface(&child, move |_, writer| {
            counter.mount("view", Arc::new(writer), &[])
        })
        .unwrap();
    let package = super::component_package("slots")
        .await
        .expect("prepared slots component is valid");
    let (runtime, _layout_driver) =
        WasmRuntime::initialize(&host, layout.clone(), &package, InitialContext::default())
            .await
            .unwrap();
    let foreign = Host::new();
    let foreign_owner = foreign.start("foreign");
    let foreign_reference = foreign.publish_component(&foreign_owner).unwrap();
    let placement = host.publish_component(&viewer).unwrap();
    let probe = SurfaceWriter::new(Arc::new(host.mount(&viewer, placement).unwrap()));
    assert!(matches!(
        runtime.mount("layout", Arc::new(probe.clone()), &[foreign_reference]),
        Err(clay_surface::PresentationError::InvalidBatch(_))
    ));
    assert!(probe.is_live());
    let layout_runtime = runtime.clone();
    let reference = native
        .publish_surface(&layout, move |_, writer| {
            layout_runtime.mount("layout", Arc::new(writer), &[child_reference])
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
        runtime.mount("layout", Arc::new(probe), &[child_reference]),
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
async fn wasm_provider_mounts_its_presentation_and_updates_on_click() {
    in_local_set(async {
        let rig = Rig::start(manifest("view", None)).await;
        let view = rig.view().await.unwrap();
        let (mut dom, mut tree) = rig.mounted(&view);
        assert_eq!(tree.text_content(), "0");
        click(&mut dom, &mut tree);
        assert_eq!(tree.text_content(), "1");
        drop(dom);
        drop(view);
        rig.supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test]
async fn disabling_revokes_the_presentation_and_a_fresh_incarnation_replaces_it() {
    in_local_set(async {
        let rig = Rig::start(manifest("view", None)).await;
        let first = rig.view().await.unwrap();
        let first_instance = rig.supervisor.instance(&rig.id).unwrap();
        let first_component = first.component().unwrap();
        assert_eq!(rig.supervisor.state(&rig.id), MicroState::Active);
        rig.supervisor.disable(&rig.id).await.unwrap();
        assert!(first.is_revoked());
        assert_eq!(rig.host.check(&first_instance), Err(AccessError::Revoked));
        assert!(matches!(
            rig.host.mount(&rig.host.start("late"), first_component),
            Err(AccessError::Revoked)
        ));
        assert!(matches!(
            rig.view().await.err(),
            Some(StartError::Disabled(_))
        ));
        rig.supervisor.enable(&rig.id);
        let second = rig.view().await.unwrap();
        assert_ne!(rig.supervisor.instance(&rig.id).unwrap(), first_instance);
        let (mut dom, mut tree) = rig.mounted(&second);
        assert_eq!(tree.text_content(), "0");
        click(&mut dom, &mut tree);
        assert_eq!(tree.text_content(), "1");
        drop(dom);
        drop((first, second));
        rig.supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test]
async fn a_guest_publication_its_manifest_does_not_declare_fails_initialization() {
    in_local_set(async {
        let rig = Rig::start(manifest("other", None)).await;
        let error = rig.view().await.err().unwrap();
        assert!(
            matches!(&error, StartError::Faulted(message)
                if message.contains("guest published \"view\", which its manifest does not declare")),
            "{error:?}"
        );
        rig.host.close();
    })
    .await;
}

#[tokio::test]
async fn manifest_read_only_grant_cannot_be_escalated_by_guest_mutation() {
    in_local_set(async {
        let rig = Rig::start(manifest("view", Some((true, false)))).await;
        rig.seed();
        let view = rig.view().await.unwrap();
        let (mut dom, mut tree) = rig.mounted(&view);
        assert_eq!(
            tree.text_content(),
            "0Pending counter writes: 0Refresh counter"
        );
        click(&mut dom, &mut tree);
        assert_eq!(
            tree.text_content(),
            "0Pending counter writes: 0Write failed: DocumentError::DeniedRefresh counter"
        );
        assert_eq!(rig.supervisor.state(&rig.id), MicroState::Active);
        drop(dom);
        drop(view);
        rig.supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test]
async fn guest_activation_failure_faults_the_micro_and_publishes_nothing() {
    in_local_set(async {
        let rig = Rig::start(manifest("view", Some((false, true)))).await;
        rig.seed();
        let error = rig.view().await.err().unwrap();
        assert!(
            matches!(&error, StartError::Faulted(message) if message.contains("DocumentError::Denied")),
            "{error:?}"
        );
        assert!(matches!(rig.supervisor.state(&rig.id), MicroState::Faulted(_)));
        // Demand does not retry a fault.
        assert!(matches!(rig.view().await.err(), Some(StartError::Faulted(_))));
        rig.host.close();
    })
    .await;
}
