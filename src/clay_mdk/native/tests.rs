use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use clay_host::RendererRegistry;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroDescriptor;
use clay_micro_core::micros::MicroId as ComposeMicroId;
use clay_micro_core::micros::MicroProvision;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::InstanceState;
use serde_json::Value;

use super::Collection;
use super::Context;
use super::Contributable;
use super::compose_descriptor;
use super::output;
use super::provider;
use super::routine_facets;
use crate::ComponentRef;
use crate::Contribution;
use crate::MicroId;
use crate::MicroManifest;
use crate::Provision;
use crate::ProvisionRef;
use crate::manifest::FacetAccess;

// ── Fixtures ──

// The stub registry publishes through the host so revocation stays authoritative, but produces
// no renderable element: contribution routing never looks at the element.
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

type Runtime = ApplicationRuntime<StubRegistry>;

// A native contract that accepts declared contributions — the composition-side half every
// declared contribution needs (native twin of the wasm suite's shelf). It shelves each
// contributor's surface in a host collection, so withdrawal is observable, and records who
// contributed what, so routing and order are observable.
#[derive(Clone)]
struct Shelf {
    entries: Collection<ComponentRef>,
    seen: Arc<Mutex<Vec<(Instance, Value, ComponentRef)>>>,
}

impl Provision for Shelf {
    const ID: &'static str = "test.shelf";
}

impl Contributable for Shelf {
    fn contribute(
        &self,
        contributor: &Instance,
        payload: &Value,
        surface: ComponentRef,
    ) -> Result<(), AccessError> {
        if payload["reject"].as_bool().unwrap() {
            return Err(AccessError::Denied);
        }
        self.entries.writer(contributor)?.insert(surface)?;
        self.seen
            .lock()
            .unwrap()
            .push((contributor.clone(), payload.clone(), surface));
        Ok(())
    }
}

fn contribution(slot: &str, reject: bool) -> Contribution {
    Contribution {
        provision: Shelf::ID.into(),
        payload: serde_json::json!({ "slot": slot, "reject": reject }),
    }
}

fn shelf_provider(app: &Runtime, contributable: bool) -> clay_micro_core::micros::MicroFactory {
    provider(
        app,
        MicroManifest {
            id: MicroId::new("test.shelf"),
            version: 1,
            requirements: vec![],
            provisions: vec![ProvisionRef::of::<Shelf>()],
            config: None,
            contributions: vec![],
        },
        move |context| {
            let shelf = Shelf {
                entries: context.collection()?,
                seen: Arc::default(),
            };
            if contributable {
                context.publish_contributable(shelf);
            } else {
                context.publish(shelf);
            }
            Ok(())
        },
    )
}

// A native micro that lands its declared contributions exactly the way the wasm backend does
// (wasm.rs): activate, publish a surface, then contribute the manifest's declared entries.
fn contributor_provider(
    app: &Runtime,
    contributions: Vec<Contribution>,
    requirements: Vec<ProvisionRef>,
) -> clay_micro_core::micros::MicroFactory {
    provider(
        app,
        MicroManifest {
            id: MicroId::new("test.contributor"),
            version: 1,
            requirements,
            provisions: vec![],
            config: None,
            contributions: contributions.clone(),
        },
        move |context| {
            let surface = context.publish_component(|| ())?;
            context.contribute(&contributions, surface)?;
            Ok(())
        },
    )
}

fn composed(
    contributions: Vec<Contribution>,
    requirements: Vec<ProvisionRef>,
    contributable: bool,
) -> Runtime {
    let mut app = Runtime::new();
    app.register_provider(shelf_provider(&app, contributable))
        .unwrap();
    app.register_provider(contributor_provider(&app, contributions, requirements))
        .unwrap();
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
            "contributor".into(),
            ConfiguredMicro {
                provider: ComposeMicroId::new("test.contributor"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::from([(Shelf::ID.into(), "shelf".into())]),
            },
        ),
    ])
}

fn shelved(app: &Runtime) -> (Vec<ComponentRef>, Vec<(Instance, Value, ComponentRef)>) {
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

fn declared() -> Vec<ProvisionRef> {
    vec![ProvisionRef::of::<Shelf>()]
}

// ── Contribution routing ──

#[tokio::test]
async fn contributions_route_in_manifest_order_and_withdraw_on_close() {
    let app = composed(
        vec![
            contribution("toolbar", false),
            contribution("footer", false),
        ],
        declared(),
        true,
    );
    let states = app.configure(&configuration()).await.unwrap();
    assert_eq!(states["contributor"], InstanceState::Active);

    let (entries, seen) = shelved(&app);
    let [
        (instance, first_payload, surface),
        (second_instance, second_payload, second_surface),
    ] = seen.as_slice()
    else {
        panic!("both contributions reached the shelf: {seen:?}");
    };
    assert_eq!(instance, second_instance);
    assert_eq!(surface, second_surface);
    // Routing follows the manifest's declared order, and both land on the one published surface.
    assert_eq!(
        (first_payload, second_payload),
        (
            &serde_json::json!({ "slot": "toolbar", "reject": false }),
            &serde_json::json!({ "slot": "footer", "reject": false })
        )
    );
    assert_eq!(entries, vec![*surface, *surface]);

    app.host().close_instance(instance).unwrap();
    assert_eq!(shelved(&app).0, vec![]);
    app.host().close();
}

#[tokio::test]
#[should_panic(expected = "manifest contributes to a contract it does not require")]
async fn contributing_an_undeclared_contract_panics_the_guest() {
    let app = composed(vec![contribution("toolbar", false)], vec![], true);
    // The contributor declares no requirements, so there is nothing to bind.
    let mut configuration = configuration();
    configuration
        .get_mut("contributor")
        .unwrap()
        .bindings
        .clear();
    let _ = app.configure(&configuration).await;
}

#[tokio::test]
#[should_panic(expected = "accepts no contributions")]
async fn contributing_to_a_non_contributable_target_panics_the_guest() {
    let app = composed(vec![contribution("toolbar", false)], declared(), false);
    let _ = app.configure(&configuration()).await;
}

#[tokio::test]
async fn a_rejected_contribution_rolls_back_surfaces_entries_and_publications() {
    let app = composed(
        vec![contribution("toolbar", false), contribution("footer", true)],
        declared(),
        true,
    );
    let states = app.configure(&configuration()).await.unwrap();
    assert!(matches!(states["contributor"], InstanceState::Failed(_)));

    // Exactly the accepted contribution reached the acceptor; activation rollback withdrew its
    // collection entry and closed the contributor.
    let (entries, seen) = shelved(&app);
    let [(instance, _, surface)] = seen.as_slice() else {
        panic!("exactly the accepted contribution reached the shelf: {seen:?}");
    };
    assert!(entries.is_empty());
    assert_eq!(app.host().check(instance), Err(AccessError::Revoked));
    assert!(matches!(
        app.host().mount(&app.root(), surface.host()),
        Err(AccessError::Revoked)
    ));
    app.host().close();
}

// ── Typed requirement resolution ──

#[tokio::test]
async fn require_returns_the_published_value_for_a_declared_requirement() {
    let mut app = Runtime::new();
    app.register_provider(shelf_provider(&app, true)).unwrap();
    let observed = Arc::new(Mutex::new(None));
    let captured = observed.clone();
    app.register_provider(provider(
        &app,
        MicroManifest {
            id: MicroId::new("test.consumer"),
            version: 1,
            requirements: declared(),
            provisions: vec![],
            config: None,
            contributions: vec![],
        },
        move |context: &Context<StubRegistry>| {
            *captured.lock().unwrap() = Some(context.require::<Shelf>().unwrap());
            Ok(())
        },
    ))
    .unwrap();
    let states = app
        .configure(&BTreeMap::from([
            (
                "shelf".into(),
                ConfiguredMicro {
                    provider: ComposeMicroId::new("test.shelf"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            ),
            (
                "consumer".into(),
                ConfiguredMicro {
                    provider: ComposeMicroId::new("test.consumer"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([(Shelf::ID.into(), "shelf".into())]),
                },
            ),
        ]))
        .await
        .unwrap();
    assert_eq!(states["consumer"], InstanceState::Active);

    let required = observed.lock().unwrap().clone().unwrap();
    let published = output::<Shelf, _>(&app, "shelf").unwrap();
    assert!(Arc::ptr_eq(&required.seen, &published.seen));
    app.host().close();
}

#[tokio::test]
#[should_panic(expected = "contract identity mapped to two native types")]
async fn output_at_the_wrong_native_type_for_a_contract_id_is_an_invariant_failure() {
    #[derive(Clone)]
    struct Impostor;
    impl Provision for Impostor {
        const ID: &'static str = Shelf::ID;
    }
    let mut app = Runtime::new();
    app.register_provider(shelf_provider(&app, true)).unwrap();
    app.configure(&configuration()).await.unwrap();
    let _ = output::<Impostor, _>(&app, "shelf");
}

// ── Wire lowering ──

#[test]
fn compose_descriptor_lowers_identity_and_contracts_verbatim() {
    let manifest = MicroManifest {
        id: MicroId::new("test.counter"),
        version: 2,
        requirements: vec![
            ProvisionRef {
                id: "a".into(),
                version: 1,
            },
            ProvisionRef {
                id: "b".into(),
                version: 3,
            },
        ],
        provisions: vec![ProvisionRef {
            id: "c".into(),
            version: 1,
        }],
        config: None,
        contributions: vec![],
    };

    assert_eq!(
        compose_descriptor(&manifest),
        MicroDescriptor {
            id: ComposeMicroId::new("test.counter"),
            version: 2,
            requirements: vec![
                MicroProvision {
                    id: "a".into(),
                    version: 1,
                },
                MicroProvision {
                    id: "b".into(),
                    version: 3,
                },
            ],
            provisions: vec![MicroProvision {
                id: "c".into(),
                version: 1,
            }],
        }
    );
}

#[test]
fn routine_facets_carries_acl_fields_and_wraps_tags_verbatim() {
    let facets = vec![
        FacetAccess {
            owner_plug_id: None,
            tag: "org.dayframe.counter.config".into(),
            key_id: Some("count".into()),
            read: true,
            write: true,
            create: false,
            delete: false,
        },
        FacetAccess {
            owner_plug_id: Some("dayframe.organizer".into()),
            tag: "org.dayframe.slots".into(),
            key_id: None,
            read: true,
            write: false,
            create: true,
            delete: true,
        },
    ];

    let lowered = routine_facets(&facets);
    assert_eq!(lowered.len(), facets.len());
    // RoutineFacetAccess carries no PartialEq; pin the lowering field by field.
    for (lowered, facet) in lowered.iter().zip(&facets) {
        assert_eq!(lowered.owner_plug_id, facet.owner_plug_id);
        assert_eq!(lowered.tag.to_string(), facet.tag);
        assert_eq!(lowered.key_id, facet.key_id);
        assert_eq!(lowered.read, facet.read);
        assert_eq!(lowered.write, facet.write);
        assert_eq!(lowered.create, facet.create);
        assert_eq!(lowered.delete, facet.delete);
    }
}
