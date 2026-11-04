use super::*;
use crate::Access;
use crate::BindingResolution;
use crate::Entrypoints;
use crate::Necessity;
use crate::PublicationDeclaration;
use crate::PublicationKind;
use crate::Registration;
use crate::Request;
use crate::Sidecar;
use clay_collection::ReplicaId;
use clay_guest_session::capabilities::PublicationEndpoint;
use clay_guest_session::capabilities::presentation_component;
use clay_guest_session::collections::bytes_registrations;
use clay_host::CollectionContract;
use clay_host::ConfiguredMicro;
use clay_host::MicroState;
use clay_host::Requirement;
use clay_host::StartError;
use clay_host::Supervisor;
use dioxus_core::Element;
use dioxus_core::VNode;
use std::cell::Cell;
use std::num::NonZeroUsize;

const ROWS: &str = "org.example.clay.test.rows";
const INCREMENT: &str = "org.example.clay.test.calls.increment";

fn rows() -> Nsid {
    Nsid::literal(ROWS)
}

fn blank() -> Element {
    VNode::empty()
}

/// One test micro's manifest-side declaration; `Rig::micro` pairs it with an `activate`.
/// The supervisor-side id of the built-in micro named `name`, the one its factory is given.
fn host_id(name: &str) -> clay_host::MicroId {
    clay_host::MicroId::new(crate::MicroId::local(name).as_str())
}

#[derive(Default)]
struct Spec {
    publications: Vec<PublicationDeclaration>,
    requests: Vec<Request>,
    requires: Vec<Request>,
    bindings: CollectionBindings,
    eager: bool,
    requirements: BTreeMap<String, Requirement>,
}

struct Rig {
    host: Host,
    resources: Resources,
}

impl Rig {
    fn new() -> Self {
        let host = Host::new();
        let resources = Resources::new(
            NativeComponents::new(&host),
            DocumentService::new(16),
            CollectionRegistry::new(ReplicaId::from_bytes([1; ReplicaId::LEN])),
        );
        resources
            .collections
            .declare(
                CollectionContract {
                    id: rows(),
                    key_schema: Nsid::literal("org.example.clay.test.rowKey"),
                    value_schema: Nsid::literal("org.example.clay.test.rowValue"),
                },
                bytes_registrations(NonZeroUsize::new(64).unwrap(), [0, 0]),
            )
            .unwrap();
        Self { host, resources }
    }

    fn micro<Fut: Future<Output = Result<(), Error>> + 'static>(
        &self,
        id: &str,
        spec: Spec,
        activate: impl Fn(Context) -> Fut + 'static,
    ) -> ConfiguredMicro<CapabilityPublication> {
        let manifest = MicroManifest {
            entrypoints: Entrypoints::micro_client(),
            publications: spec.publications,
            requires: spec.requires,
            configuration_requests: vec![],
            collection_contracts: vec![],
            collection_requests: spec.requests,
            contributions: vec![],
            extra: BTreeMap::new(),
        };
        manifest.validate().unwrap();
        ConfiguredMicro {
            identity: host_id(id),
            eager: spec.eager,
            enabled: true,
            requirements: spec.requirements,
            unresolved_bindings: Vec::new(),
            factory: factory(
                self.resources.clone(),
                crate::MicroId::local(id),
                manifest,
                spec.bindings,
                activate,
            ),
        }
    }
}

fn presentation(name: &str) -> PublicationDeclaration {
    PublicationDeclaration {
        name: name.into(),
        kind: PublicationKind::Presentation,
        contract: None,
    }
}

fn call(name: &str) -> PublicationDeclaration {
    PublicationDeclaration {
        name: name.into(),
        kind: PublicationKind::Call,
        contract: Some(Nsid::literal(INCREMENT)),
    }
}

/// The binding name the test micros use for the `ROWS` collection.
const ROWS_BINDING: &str = "rows";

fn rows_request(access: Access, necessity: Necessity) -> Request {
    Request {
        binding: ROWS_BINDING.into(),
        contract: rows(),
        access: Some(access),
        necessity,
        resolution: BindingResolution::Configured,
        min_revision: None,
    }
}

fn writes_rows() -> Spec {
    Spec {
        eager: true,
        requests: vec![rows_request(Access::Write, Necessity::Required)],
        bindings: CollectionBindings::new([(
            ROWS_BINDING.to_owned(),
            BoundCollection::write(rows()),
        )]),
        ..Spec::default()
    }
}

fn reads_rows() -> Spec {
    Spec {
        requests: vec![rows_request(Access::Read, Necessity::Required)],
        bindings: CollectionBindings::new([(
            ROWS_BINDING.to_owned(),
            BoundCollection::read(rows()),
        )]),
        ..Spec::default()
    }
}

async fn in_local_set<T>(future: impl Future<Output = T>) -> T {
    tokio::task::LocalSet::new().run_until(future).await
}

fn titles(collection: &Collection<String, String>) -> Vec<(String, String)> {
    collection
        .rows()
        .unwrap()
        .iter()
        .map(|row| (row.key.clone(), row.value.clone()))
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn declared_presentations_are_published_by_name_and_leased() {
    in_local_set(async {
        let rig = Rig::new();
        let micro = rig.micro(
            "native.shelf",
            Spec {
                publications: vec![presentation("view"), presentation("detail")],
                ..Spec::default()
            },
            |context| async move {
                context.publish_component("view", blank)?;
                context.publish_component("detail", blank)?;
                Ok(())
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        let id = host_id("native.shelf");
        let view = supervisor.acquire(&id, "view").await.unwrap();
        let detail = supervisor.acquire(&id, "detail").await.unwrap();
        assert_eq!(view.name(), "view");
        assert_eq!(detail.name(), "detail");
        assert_eq!(
            view.kind(),
            clay_guest_session::capabilities::PublicationKind::Presentation
        );
        assert!(matches!(
            supervisor.acquire(&id, "other").await.err(),
            Some(StartError::Unpublished { .. })
        ));
        drop((view, detail));
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_an_undeclared_name_fails_initialization() {
    in_local_set(async {
        let rig = Rig::new();
        let ran = Rc::new(Cell::new(false));
        let micro = rig.micro(
            "native.rogue",
            Spec {
                publications: vec![presentation("view")],
                ..reads_rows()
            },
            {
                let ran = Rc::clone(&ran);
                move |context| {
                    let ran = Rc::clone(&ran);
                    async move {
                        // A mirror task is registered before the rejected publication.
                        context.collection::<String, String>(ROWS_BINDING).unwrap();
                        context.publish_component("undeclared", blank)?;
                        ran.set(true);
                        Ok(())
                    }
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        let id = host_id("native.rogue");
        let error = supervisor.acquire(&id, "view").await.err().unwrap();
        assert!(
            matches!(&error, StartError::Faulted(message) if message.contains("undeclared")),
            "{error:?}"
        );
        assert!(ran.get());
        assert!(matches!(supervisor.state(&id), MicroState::Faulted(_)));
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn registration_rows_reach_a_reader_and_are_retracted_when_the_writer_closes() {
    in_local_set(async {
        let rig = Rig::new();
        let watched = Rc::new(RefCell::new(None::<Collection<String, String>>));
        let reader = rig.micro(
            "native.reader",
            Spec {
                eager: true,
                ..reads_rows()
            },
            {
                let watched = Rc::clone(&watched);
                move |context| {
                    let watched = Rc::clone(&watched);
                    async move {
                        *watched.borrow_mut() = context.collection(ROWS_BINDING);
                        Ok(())
                    }
                }
            },
        );
        let writer = rig.micro("native.writer", writes_rows(), |context| async move {
            let registration = context
                .registration::<String, String>(ROWS_BINDING)
                .unwrap();
            registration
                .put(&"counter".to_owned(), &"Counter".to_owned(), vec![])
                .await?;
            Ok(())
        });
        let supervisor = Supervisor::new(rig.host.clone(), [reader, writer]);
        assert!(supervisor.reconcile().await.is_empty());
        let mut watch = watched.borrow().clone().unwrap();
        while titles(&watch) != [("counter".to_owned(), "Counter".to_owned())] {
            watch.changed().await.unwrap();
        }
        supervisor.disable(&host_id("native.writer")).await.unwrap();
        while !titles(&watch).is_empty() {
            watch.changed().await.unwrap();
        }
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn rows_are_decoded_once_per_revision_and_rewritten_over_what_the_mirror_saw() {
    in_local_set(async {
        let rig = Rig::new();
        let written = Rc::new(RefCell::new(None::<Registration<String, String>>));
        let writer = rig.micro("native.writer", writes_rows(), {
            let written = Rc::clone(&written);
            move |context| {
                let written = Rc::clone(&written);
                async move {
                    *written.borrow_mut() = context.registration(ROWS_BINDING);
                    Ok(())
                }
            }
        });
        let supervisor = Supervisor::new(rig.host.clone(), [writer]);
        assert!(supervisor.reconcile().await.is_empty());
        let registration = written.borrow().clone().unwrap();
        let mut watch = registration.collection().clone();
        let key = "k".to_owned();
        registration
            .put(&key, &"one".to_owned(), vec![])
            .await
            .unwrap();
        while titles(&watch) != [(key.clone(), "one".to_owned())] {
            watch.changed().await.unwrap();
        }
        let first = watch.rows().unwrap();
        assert!(Rc::ptr_eq(&first, &watch.rows().unwrap()));
        // The default basis is the mirror's stamp for the key, so this supersedes "one".
        registration
            .put(&key, &"two".to_owned(), vec![])
            .await
            .unwrap();
        while titles(&watch) != [(key.clone(), "two".to_owned())] {
            watch.changed().await.unwrap();
        }
        assert!(!Rc::ptr_eq(&first, &watch.rows().unwrap()));
        registration.delete(&key).await.unwrap();
        while !titles(&watch).is_empty() {
            watch.changed().await.unwrap();
        }
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn optional_requests_configuration_left_unbound_are_none() {
    in_local_set(async {
        let rig = Rig::new();
        let unbound = Rc::new(Cell::new(false));
        let micro = rig.micro(
            "native.unbound",
            Spec {
                eager: true,
                requests: vec![rows_request(Access::Write, Necessity::Optional)],
                ..Spec::default()
            },
            {
                let unbound = Rc::clone(&unbound);
                move |context| {
                    let unbound = Rc::clone(&unbound);
                    async move {
                        unbound.set(
                            context.collection::<String, String>(ROWS_BINDING).is_none()
                                && context
                                    .registration::<String, String>(ROWS_BINDING)
                                    .is_none(),
                        );
                        Ok(())
                    }
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        assert!(supervisor.reconcile().await.is_empty());
        assert!(unbound.get());
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn each_incarnation_sees_the_binding_resolution_current_when_it_started() {
    in_local_set(async {
        let rig = Rig::new();
        let spec = Spec {
            requests: vec![rows_request(Access::Read, Necessity::Optional)],
            ..reads_rows()
        };
        let bindings = spec.bindings.clone();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let micro = rig.micro("native.rebound", spec, {
            let seen = Rc::clone(&seen);
            move |context| {
                let seen = Rc::clone(&seen);
                async move {
                    seen.borrow_mut()
                        .push(context.collection::<String, String>(ROWS_BINDING).is_some());
                    Ok(())
                }
            }
        });
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        let id = host_id("native.rebound");
        supervisor.reconfigure(&id, vec![]).await.unwrap();
        supervisor.acquire(&id, "none").await.err().unwrap();
        // The binding changes while the incarnation runs: it keeps what it started with,
        // and only the restart reads the new resolution.
        bindings.replace(BTreeMap::new());
        assert_eq!(*seen.borrow(), [true]);
        supervisor.reconfigure(&id, vec![]).await.unwrap();
        supervisor.acquire(&id, "none").await.err().unwrap();
        assert_eq!(*seen.borrow(), [true, false]);
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_requirement_resolves_to_a_live_reference_to_its_provider() {
    in_local_set(async {
        let rig = Rig::new();
        let provider = rig.micro(
            "native.provider",
            Spec {
                publications: vec![presentation("view")],
                ..Spec::default()
            },
            |context| async move {
                context.publish_component("view", blank)?;
                Ok(())
            },
        );
        let status = Rc::new(RefCell::new(None));
        let consumer = rig.micro(
            "native.consumer",
            Spec {
                eager: true,
                requires: vec![
                    required("host", Necessity::Required),
                    required("absent", Necessity::Optional),
                ],
                requirements: BTreeMap::from([(
                    "host".to_owned(),
                    provides("native.provider", "view"),
                )]),
                ..Spec::default()
            },
            {
                let status = Rc::clone(&status);
                move |context| {
                    let status = Rc::clone(&status);
                    async move {
                        *status.borrow_mut() =
                            Some(context.require("host").map(Capability::status));
                        assert!(context.require("absent").is_none());
                        Ok(())
                    }
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [provider, consumer]);
        assert!(supervisor.reconcile().await.is_empty());
        assert_eq!(*status.borrow(), Some(Some(crate::RefStatus::Live)));
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

fn required(binding: &str, necessity: Necessity) -> Request {
    Request {
        binding: binding.into(),
        contract: Nsid::literal(INCREMENT),
        access: None,
        necessity,
        resolution: BindingResolution::Configured,
        min_revision: None,
    }
}

fn provides(provider: &str, publication: &str) -> Requirement {
    Requirement {
        provider: host_id(provider),
        publication: publication.into(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_published_call_serves_typed_requests_and_reports_handler_rejections() {
    in_local_set(async {
        let rig = Rig::new();
        let provider = rig.micro(
            "native.provider",
            Spec {
                publications: vec![call("increment")],
                ..Spec::default()
            },
            |context| async move {
                context.publish_call("increment", |value: u32| async move {
                    // The await point proves handlers run as tasks, not inline in the queue.
                    tokio::task::yield_now().await;
                    if value == 0 {
                        return Err(Error::Rejected("zero".into()));
                    }
                    Ok(value + 1)
                })?;
                Ok(())
            },
        );
        let capability = Rc::new(RefCell::new(None::<Capability>));
        let consumer = rig.micro(
            "native.consumer",
            Spec {
                eager: true,
                requires: vec![required("increment", Necessity::Required)],
                requirements: BTreeMap::from([(
                    "increment".to_owned(),
                    provides("native.provider", "increment"),
                )]),
                ..Spec::default()
            },
            {
                let capability = Rc::clone(&capability);
                move |context| {
                    let capability = Rc::clone(&capability);
                    async move {
                        *capability.borrow_mut() = context.require("increment").cloned();
                        Ok(())
                    }
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [provider, consumer]);
        assert!(supervisor.reconcile().await.is_empty());
        let increment = capability.borrow().clone().unwrap();
        assert_eq!(increment.call::<u32, u32>(&41).await, Ok(42));
        assert_eq!(
            increment.call::<u32, u32>(&0).await,
            Err(Error::Rejected("rejected: zero".into()))
        );
        // A request the handler cannot decode is the provider's rejection, not a crash.
        assert!(matches!(
            increment.call::<String, u32>(&"nope".to_owned()).await,
            Err(Error::Rejected(message)) if message.contains("does not decode")
        ));
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

/// A row may attach one of its writer's own declared publications at any time: the host stores
/// the endpoint, so no handle from `activate` is needed and a reader redeems the row's
/// reference like a manifest row's. A name the manifest does not declare fails the write.
#[tokio::test(flavor = "current_thread")]
async fn a_row_written_after_activate_attaches_the_writers_own_declared_publication() {
    in_local_set(async {
        let rig = Rig::new();
        let writer = rig.micro(
            "native.late",
            Spec {
                publications: vec![presentation("detail"), call("attach")],
                ..writes_rows()
            },
            |context| async move {
                let rows = context
                    .registration::<String, String>(ROWS_BINDING)
                    .unwrap();
                context.publish_component("detail", blank)?;
                // The handler runs long after `activate` returned and its staged handles moved.
                context.publish_call("attach", move |publication: String| {
                    let rows = rows.clone();
                    async move {
                        rows.put(
                            &"late".to_owned(),
                            &"Late".to_owned(),
                            vec![crate::RowAttachment::own(publication)],
                        )
                        .await?;
                        Ok(())
                    }
                })?;
                Ok(())
            },
        );
        let watched = Rc::new(RefCell::new(None::<Collection<String, String>>));
        let attach = Rc::new(RefCell::new(None::<Capability>));
        let reader = rig.micro(
            "native.reader",
            Spec {
                eager: true,
                requires: vec![required("attach", Necessity::Required)],
                requirements: BTreeMap::from([(
                    "attach".to_owned(),
                    provides("native.late", "attach"),
                )]),
                ..reads_rows()
            },
            {
                let watched = Rc::clone(&watched);
                let attach = Rc::clone(&attach);
                move |context| {
                    let watched = Rc::clone(&watched);
                    let attach = Rc::clone(&attach);
                    async move {
                        *watched.borrow_mut() = context.collection(ROWS_BINDING);
                        *attach.borrow_mut() = context.require("attach").cloned();
                        Ok(())
                    }
                }
            },
        );
        let supervisor = Rc::new(Supervisor::new(rig.host.clone(), [writer, reader]));
        // Redeeming a reference is served by the supervisor's driver.
        let driver = tokio::task::spawn_local({
            let supervisor = Rc::clone(&supervisor);
            async move { supervisor.drive().await }
        });
        assert!(supervisor.reconcile().await.is_empty());
        let attach = attach.borrow().clone().unwrap();
        assert_eq!(
            attach.call::<String, ()>(&"detail".to_owned()).await,
            Ok(())
        );
        let mut watch = watched.borrow().clone().unwrap();
        while watch.rows().unwrap().is_empty() {
            watch.changed().await.unwrap();
        }
        let rows = watch.rows().unwrap();
        let detail = rows[0].attachments[0].redeem_presentation().await.unwrap();
        assert!(detail.component().is_ok());
        drop(detail);
        // Neither an undeclared name nor a declared call attaches as a presentation row.
        assert!(matches!(
            attach.call::<String, ()>(&"nope".to_owned()).await,
            Err(Error::Rejected(message)) if message.contains("attachment")
        ));
        drop((attach, rows, watch));
        driver.abort();
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[test]
fn declared_publications_keep_the_manifest_contract_and_skip_unknown_kinds() {
    let manifest = MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![
            presentation("view"),
            PublicationDeclaration {
                name: "echo".into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal("org.example.clay.test.lifecycle.echoV2")),
            },
            PublicationDeclaration {
                name: "future".into(),
                kind: PublicationKind::Other("stream".into()),
                contract: None,
            },
        ],
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![],
        contributions: vec![],
        extra: BTreeMap::new(),
    };
    let declared = declared_publications(&manifest);
    assert_eq!(declared.len(), 2, "an unknown kind is not served");
    assert_eq!(
        declared["echo"].contract,
        Nsid::literal("org.example.clay.test.lifecycle.echoV2")
    );
    assert_eq!(declared["view"].contract, presentation_component());
}

#[test]
fn declared_rows_store_endpoints_and_tag_attachment_slots() {
    use crate::AttachmentDeclaration;
    use crate::ContributionDeclaration;

    let mut sidecar = Sidecar::new(
        "native.declares",
        1,
        MicroManifest {
            entrypoints: Entrypoints::micro_client(),
            publications: vec![presentation("view")],
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: vec![],
            collection_requests: vec![Request {
                binding: "roots".into(),
                contract: Nsid::literal(SHELL_ROOTS),
                access: Some(Access::Write),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            }],
            contributions: vec![ContributionDeclaration {
                id: "root".into(),
                binding: "roots".into(),
                key: "native.declares".into(),
                payload: serde_json::json!({"title": "Declared", "content": {"$attachment": 0}}),
                attachments: vec![AttachmentDeclaration {
                    index: 0,
                    publication: "view".into(),
                }],
            }],
            extra: BTreeMap::new(),
        },
    );
    let micro = crate::MicroId::local("native.declares");
    let rows = declared_rows(&micro, &sidecar).unwrap();
    let mut key = Vec::new();
    ciborium::into_writer("native.declares", &mut key).unwrap();
    let value = rows["roots"][&key].clone().unwrap();
    let row: RootRow = ciborium::from_reader(value.body.as_slice()).unwrap();
    assert_eq!(
        row,
        RootRow {
            title: "Declared".into(),
            content: Attachment::at(0),
            pinned: false,
        }
    );
    assert_eq!(
        value.attachments,
        [clay_guest_session::capabilities::RowAttachment::Endpoint(
            PublicationEndpoint {
                micro: host_id(micro.name()),
                publication: "view".into(),
                contract: presentation_component(),
            }
        )]
    );
    assert_ne!(
        manifest_scope(&host_id(micro.name())),
        runtime_scope(&host_id(micro.name()))
    );

    // An attachment index that the contribution does not declare is refused.
    sidecar.manifest.contributions[0].attachments[0].index = 3;
    assert!(
        declared_rows(&micro, &sidecar)
            .unwrap_err()
            .contains("index 3")
    );
}
