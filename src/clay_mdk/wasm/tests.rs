use super::*;
use crate::ConfigurationRequest;
use crate::Entrypoints;
use crate::FacetAccess;
use crate::MicroId;
use crate::PublicationDeclaration;
use crate::PublicationKind;
use clay_collection::Nsid;
use clay_collection::ReplicaId;
use clay_dioxus::ComponentMount;
use clay_dioxus::NativeComponents;
use clay_docs::DocumentService;
use clay_guest_session::capabilities::presentation_component;
use clay_host::AccessError;
use clay_host::CollectionRegistry;
use clay_host::ConfiguredMicro;
use clay_host::Lease;
use clay_host::MicroState;
use clay_host::StartError;
use clay_host::Supervisor;
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
fn manifest(view: &str, config: Option<(bool, bool)>) -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        // The counter also publishes a badge presentation and the `count` and `increment` calls.
        publications: vec![
            PublicationDeclaration {
                name: view.into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
            PublicationDeclaration {
                name: "badge".into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
            PublicationDeclaration {
                name: "count".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.dayframe.counter.count")),
            },
            PublicationDeclaration {
                name: "increment".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.dayframe.toolbar.action")),
            },
        ],
        requires: vec![],
        configuration_requests: config
            .map(|(read, write)| ConfigurationRequest {
                binding: "counter".into(),
                facets: vec![FacetAccess {
                    tag: Nsid::literal("org.example.dayframe.counter.config"),
                    key_id: Some("count".into()),
                    read,
                    write,
                    create: false,
                    delete: false,
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
    resources: Resources,
    components: NativeComponents,
    supervisor: Supervisor<clay_guest_session::capabilities::CapabilityPublication>,
    id: clay_host::MicroId,
}

impl Rig {
    async fn start(declared: MicroManifest) -> Self {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let resources = Resources::new(
            components.clone(),
            DocumentService::new(16),
            CollectionRegistry::new(ReplicaId::from_bytes([1; ReplicaId::LEN])),
        );
        let package = counter_package().await;
        let id = clay_host::MicroId::new(MicroId::local("test.wasm").as_str());
        let micro = ConfiguredMicro {
            identity: id.clone(),
            eager: false,
            enabled: true,
            requirements: BTreeMap::new(),
            unresolved_bindings: Vec::new(),
            factory: factory(
                resources.clone(),
                MicroId::local("test.wasm"),
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
            components,
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
                components: self.components.clone(),
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
                tag: "org.example.dayframe.counter.config".into(),
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
            "0Pending counter writes: 0Write failed: capability deniedRefresh counter"
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
            matches!(&error, StartError::Faulted(message) if message.contains("capability denied")),
            "{error:?}"
        );
        assert!(matches!(
            rig.supervisor.state(&rig.id),
            MicroState::Faulted(_)
        ));
        // Demand does not retry a fault.
        assert!(matches!(
            rig.view().await.err(),
            Some(StartError::Faulted(_))
        ));
        rig.host.close();
    })
    .await;
}

/// A guest micro (the `context` fixture) hosts a `PresentationSlot` over another guest's
/// presentation (the counter's `view`): the slot redeems the row's lazy reference, the guest
/// emits `mount-component` at its position, and the host renders the counter inside it.
/// Disabling the provider ends its incarnation, which the slot shows as unavailable; enabling it
/// mounts the next incarnation in the same slot, without remounting the consumer.
#[tokio::test]
async fn a_guest_slot_mounts_another_guests_presentation_and_follows_it_through_disable_and_enable()
{
    use clay_guest_session::capabilities::PublicationEndpoint;
    use clay_guest_session::capabilities::RowAttachment;
    use clay_guest_session::collections::CollectionLimits;
    use clay_guest_session::collections::CollectionSession;
    use clay_guest_session::collections::CollectionValue;
    use clay_guest_session::collections::bytes_registrations;
    use clay_host::CollectionContract;

    in_local_set(async {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let resources = Resources::new(
            components.clone(),
            DocumentService::new(16),
            CollectionRegistry::new(ReplicaId::from_bytes([1; ReplicaId::LEN])),
        );
        let feed = Nsid::literal("org.example.clay.test.context.feed");
        resources
            .collections
            .declare(
                CollectionContract {
                    id: feed.clone(),
                    key_schema: Nsid::literal("org.example.clay.test.rowKey"),
                    value_schema: Nsid::literal("org.example.clay.test.rowValue"),
                },
                bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), [0, 0]),
            )
            .unwrap();
        let counter_id = clay_host::MicroId::new(MicroId::local("slot.counter").as_str());
        let consumer_id = clay_host::MicroId::new(MicroId::local("slot.consumer").as_str());
        // The declaration of the guest component this test loads (`context_micro.rs`).
        let consumer_manifest = crate::context_micro::manifest();
        let counter_package = counter_package().await;
        let consumer_package = super::component_package("context")
            .await
            .expect("prepared context component is valid");
        let micro = |identity: &clay_host::MicroId,
                     name: &str,
                     manifest: MicroManifest,
                     bindings: CollectionBindings,
                     package: WasmPackage| {
            ConfiguredMicro {
                identity: identity.clone(),
                eager: false,
                enabled: true,
                requirements: BTreeMap::new(),
                unresolved_bindings: Vec::new(),
                factory: factory(
                    resources.clone(),
                    MicroId::local(name),
                    manifest,
                    bindings,
                    move || {
                        let package = package.clone();
                        async move { Ok(package) }
                    },
                ),
            }
        };
        let supervisor = Rc::new(Supervisor::new(
            host.clone(),
            [
                micro(
                    &counter_id,
                    "slot.counter",
                    manifest("view", None),
                    CollectionBindings::default(),
                    counter_package,
                ),
                micro(
                    &consumer_id,
                    "slot.consumer",
                    consumer_manifest,
                    CollectionBindings::new([(
                        "feed".to_owned(),
                        crate::native::BoundCollection::write(feed.clone()),
                    )]),
                    consumer_package,
                ),
            ],
        ));
        // Redeeming a lazy reference is served by the supervisor's driver.
        let serving = tokio::task::spawn_local({
            let supervisor = Rc::clone(&supervisor);
            async move { supervisor.drive().await }
        });
        // The row the consumer's `slot` shows: its attachment is the counter's `view`.
        let cbor = |text: &str| {
            let mut bytes = Vec::new();
            ciborium::into_writer(text, &mut bytes).unwrap();
            bytes
        };
        CollectionSession::new(
            host.clone(),
            host.start("seeder"),
            CollectionLimits::default(),
        )
        .grant_writer(
            resources
                .collections
                .grant(&feed, &clay_host::MicroId::new("seeder"))
                .unwrap(),
        )
        .unwrap()
        .put_value(
            cbor("view"),
            clay_collection::WriteBasis::Concurrent,
            CollectionValue {
                body: cbor("view"),
                attachments: vec![RowAttachment::Endpoint(PublicationEndpoint {
                    micro: counter_id.clone(),
                    publication: "view".to_owned(),
                    contract: presentation_component(),
                })],
            },
        )
        .unwrap();

        let slot = supervisor.acquire(&consumer_id, "slot").await.unwrap();
        let mut dom = VirtualDom::new_with_props(
            mount_host_app,
            MountProps {
                components,
                owner: host.start("viewer"),
                reference: slot.component().expect("a presentation has a component"),
            },
        );
        let mut tree = SurfaceTree::new();
        dom.rebuild(&mut tree);
        // Each wake is a task or signal of the slot, so a slot that never gets there hangs
        // until the test runner's timeout.
        async fn shows(dom: &mut VirtualDom, tree: &mut SurfaceTree, text: &str) {
            loop {
                dom.render_immediate(tree);
                if tree.text_content().contains(text) {
                    return;
                }
                dom.wait_for_work().await;
            }
        }
        shows(&mut dom, &mut tree, "0").await;
        // The counter inside the guest slot is interactive.
        click(&mut dom, &mut tree);
        assert_eq!(tree.text_content(), "1");
        assert_eq!(supervisor.state(&counter_id), MicroState::Active);

        supervisor.disable(&counter_id).await.unwrap();
        shows(&mut dom, &mut tree, "Section unavailable").await;
        supervisor.enable(&counter_id);
        shows(&mut dom, &mut tree, "0").await;
        assert_eq!(supervisor.state(&counter_id), MicroState::Active);

        drop(dom);
        drop(slot);
        serving.abort();
        supervisor.shutdown().await.unwrap();
        host.close();
    })
    .await;
}
