use super::*;
use crate::Entrypoints;
use crate::MANIFEST_SCHEMA;
use crate::MicroId as ManifestId;
use crate::PublicationDeclaration;
use clay_guest_session::collections::bytes_registrations;
use clay_host::CollectionContract;
use clay_host::ConfiguredMicro;
use clay_host::MicroState;
use clay_host::Requirement;
use clay_host::StartError;
use clay_host::Supervisor;
use std::cell::Cell;
use std::num::NonZeroUsize;

const ROWS: &str = "test.rows";

/// Publishes through the host so revocation stays authoritative, but renders nothing.
#[derive(Clone)]
struct StubRegistry {
    host: Host,
}

impl RendererRegistry for StubRegistry {
    type Render = ();

    fn for_host(host: &Host) -> Self {
        Self { host: host.clone() }
    }

    fn publish<F>(
        &self,
        owner: &Instance,
        _render: F,
    ) -> Result<clay_host::ComponentRef, AccessError>
    where
        F: Fn() -> Self::Render + 'static,
    {
        self.host.publish_component(owner)
    }
}

struct Rig {
    host: Host,
    resources: Resources<StubRegistry>,
}

impl Rig {
    fn new() -> Self {
        let host = Host::new();
        let resources = Resources {
            registry: StubRegistry::for_host(&host),
            documents: DocumentService::new(16),
            collections: CollectionRegistry::default(),
        };
        resources
            .collections
            .declare(
                CollectionContract {
                    id: ROWS.into(),
                    key_schema: "text".into(),
                    value_schema: "text".into(),
                },
                bytes_registrations(NonZeroUsize::new(64).unwrap(), [0, 0]),
            )
            .unwrap();
        Self { host, resources }
    }

    fn micro(
        &self,
        id: &str,
        publications: Vec<PublicationDeclaration>,
        bindings: CollectionBindings,
        eager: bool,
        requirements: BTreeMap<String, Requirement>,
        activate: impl Fn(&Context<StubRegistry>) -> Result<(), AccessError> + 'static,
    ) -> ConfiguredMicro<CapabilityPublication> {
        let manifest = MicroManifest {
            schema: MANIFEST_SCHEMA,
            id: ManifestId::new(id),
            package_version: 1,
            entrypoints: Entrypoints::micro_client(),
            publications,
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: vec![],
            contributions: vec![],
            extra: BTreeMap::new(),
        };
        manifest.validate().unwrap();
        ConfiguredMicro {
            identity: clay_host::MicroId::new(id),
            eager,
            requirements,
            factory: factory(self.resources.clone(), manifest, bindings, activate),
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

fn writes_rows() -> CollectionBindings {
    CollectionBindings {
        read: vec![],
        write: vec![ROWS.into()],
    }
}

fn reads_rows() -> CollectionBindings {
    CollectionBindings {
        read: vec![ROWS.into()],
        write: vec![],
    }
}

async fn in_local_set<T>(future: impl Future<Output = T>) -> T {
    tokio::task::LocalSet::new().run_until(future).await
}

#[tokio::test(flavor = "current_thread")]
async fn a_declared_presentation_is_published_by_name_and_leased() {
    in_local_set(async {
        let rig = Rig::new();
        let micro = rig.micro(
            "native.shelf",
            vec![presentation("view")],
            CollectionBindings::default(),
            false,
            BTreeMap::new(),
            |context| {
                context.publish_component("view", || ())?;
                Ok(())
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        let id = clay_host::MicroId::new("native.shelf");
        let lease = supervisor.acquire(&id, "view").await.unwrap();
        assert_eq!(lease.name(), "view");
        assert_eq!(
            lease.kind(),
            clay_guest_session::capabilities::PublicationKind::Presentation
        );
        assert!(matches!(
            supervisor.acquire(&id, "other").await.err(),
            Some(StartError::Unpublished { .. })
        ));
        drop(lease);
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
            vec![presentation("view")],
            reads_rows(),
            false,
            BTreeMap::new(),
            {
                let ran = Rc::clone(&ran);
                move |context| {
                    // A mirror task is registered before the rejected publication.
                    context.collection::<String, String>(ROWS)?;
                    context.publish_component("undeclared", || ())?;
                    ran.set(true);
                    Ok(())
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        let id = clay_host::MicroId::new("native.rogue");
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
        let watched = Rc::new(RefCell::new(None::<CollectionWatch<String, String>>));
        let reader = rig.micro(
            "native.reader",
            vec![],
            reads_rows(),
            true,
            BTreeMap::new(),
            {
                let watched = Rc::clone(&watched);
                move |context| {
                    *watched.borrow_mut() = Some(context.collection(ROWS)?);
                    Ok(())
                }
            },
        );
        let writer = rig.micro(
            "native.writer",
            vec![],
            writes_rows(),
            true,
            BTreeMap::new(),
            |context| {
                let registration = context.registration::<String, String>(ROWS)?;
                registration
                    .put(&"counter".to_owned(), &"Counter".to_owned(), vec![])
                    .unwrap();
                Ok(())
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [reader, writer]);
        assert!(supervisor.reconcile().await.is_empty());
        let watch = watched.borrow().clone().unwrap();
        let mut revisions = watch.revisions();
        let titles = |watch: &CollectionWatch<String, String>| {
            watch
                .rows()
                .unwrap()
                .into_iter()
                .map(|row| (row.key, row.value))
                .collect::<Vec<_>>()
        };
        while titles(&watch) != [("counter".to_owned(), "Counter".to_owned())] {
            revisions.changed().await.unwrap();
        }
        supervisor
            .disable(&clay_host::MicroId::new("native.writer"))
            .await
            .unwrap();
        while !titles(&watch).is_empty() {
            revisions.changed().await.unwrap();
        }
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn collections_outside_the_configured_bindings_are_denied() {
    in_local_set(async {
        let rig = Rig::new();
        let denied = Rc::new(Cell::new(0));
        let micro = rig.micro(
            "native.unbound",
            vec![],
            reads_rows(),
            true,
            BTreeMap::new(),
            {
                let denied = Rc::clone(&denied);
                move |context| {
                    // Read-only binding: no write authority; an unbound id: no read.
                    denied.set(
                        usize::from(matches!(
                            context.registration::<String, String>(ROWS).err(),
                            Some(AccessError::Denied)
                        )) + usize::from(matches!(
                            context.collection::<String, String>("other").err(),
                            Some(AccessError::Denied)
                        )),
                    );
                    Ok(())
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [micro]);
        assert!(supervisor.reconcile().await.is_empty());
        assert_eq!(denied.get(), 2);
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
            vec![presentation("view")],
            CollectionBindings::default(),
            false,
            BTreeMap::new(),
            |context| {
                context.publish_component("view", || ())?;
                Ok(())
            },
        );
        let status = Rc::new(RefCell::new(None));
        let consumer = rig.micro(
            "native.consumer",
            vec![],
            CollectionBindings::default(),
            true,
            BTreeMap::from([(
                "host".to_owned(),
                Requirement {
                    provider: clay_host::MicroId::new("native.provider"),
                    publication: "view".into(),
                },
            )]),
            {
                let status = Rc::clone(&status);
                move |context| {
                    *status.borrow_mut() = Some(context.require("host").map(CapabilityRef::status));
                    assert!(context.require("absent").is_none());
                    Ok(())
                }
            },
        );
        let supervisor = Supervisor::new(rig.host.clone(), [provider, consumer]);
        assert!(supervisor.reconcile().await.is_empty());
        assert_eq!(
            *status.borrow(),
            Some(Some(clay_guest_session::capabilities::RefStatus::Live))
        );
        supervisor.shutdown().await.unwrap();
        rig.host.close();
    })
    .await;
}

#[test]
fn manifest_contracts_map_to_session_contracts() {
    let manifest = MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: ManifestId::new("native.contracts"),
        package_version: 1,
        entrypoints: Entrypoints::micro_client(),
        publications: vec![
            presentation("view"),
            PublicationDeclaration {
                name: "echo".into(),
                kind: PublicationKind::Call,
                contract: Some(ContractRef {
                    id: "dayframe.lifecycle.echo".into(),
                    version: 2,
                }),
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
        contributions: vec![],
        extra: BTreeMap::new(),
    };
    let declared = declared_publications(&manifest);
    assert_eq!(declared.len(), 2, "an unknown kind is not served");
    assert_eq!(
        declared["echo"].contract,
        ContractIdentity::new("dayframe.lifecycle", "echo", (2, 0, 0))
    );
    assert_eq!(
        declared["view"].contract,
        ContractIdentity::presentation_component()
    );
}
