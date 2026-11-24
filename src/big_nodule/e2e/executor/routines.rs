//! Each routine holds its own capability (ADR 047 §6), seen through the fake executor (the
//! capabilities its `ensure` is handed and the ones a call is delivered with) and through the
//! real docs, dispatch and authority services those handles are enforced by.

use super::FakeExecutor;
use crate::e2e::Node;
use crate::e2e::dispatch::Echo;
use crate::e2e::id;
use crate::e2e::init::Probe;
use crate::e2e::init::init_cartridge;
use crate::e2e::init::necessity;
use crate::e2e::init::with_requests;
use crate::e2e::lexicon;
use crate::e2e::nodule_mut;
use crate::executor::NoduleExecutor;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Initializer;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindDatum;
use big_cartridges_core::PermbindDrawers;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::RunMode;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::test_support::FixtureCartridge;
use big_dispatch::Operation;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use big_nodule_core::protocol::NoduleGrantStatus;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

const ALPHA: &str = example_cartridge!("alpha");
const PING: &str = "org.example.alpha.ping";
const SETUP: &str = "org.example.alpha.setup";
const REMOTE: &str = "org.example.remote.sync";
const OTHER: &str = "org.example.remote.other";
const NOTES: &str = "org.example.alpha.notes";
const PREFS: &str = "org.example.alpha.prefs";

/// Alpha serving `routines` (each with no `uses`), requesting `methods` optionally and
/// declaring each of `settings` as a settings datum that a `#datum` permbind of the same name
/// binds.
fn alpha(routines: &[&str], methods: &[&str], settings: &[&str]) -> FixtureCartridge {
    let mut cartridge = with_requests(
        init_cartridge("alpha", "1.0.0", routines, &[], &[]),
        methods
            .iter()
            .map(|method| necessity(method, Necessity::Optional))
            .collect(),
    );
    for kind in settings {
        with_setting(&mut cartridge, kind);
    }
    cartridge
}

/// Declares `kind` as a settings datum of `cartridge`, bound by a `#datum` permbind of the same
/// name that a routine lists in its `uses` to reach it.
fn with_setting(cartridge: &mut FixtureCartridge, kind: &str) {
    cartridge.with_lexicon(lexicon(json!({
        "lexicon": 1,
        "id": kind,
        "defs": { "main": { "type": "object", "properties": {} } }
    })));
    cartridge.settings.insert(kind.parse().unwrap());
    cartridge.permbinds.insert(
        kind.to_owned(),
        Permbind::Datum(PermbindDatum {
            nsid: kind.parse().unwrap(),
            description: kind.to_owned(),
            read: Some(Necessity::Optional),
            write: Some(Necessity::Optional),
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );
}

/// `routine` lists `binding` in its `uses` with `usage`.
fn uses(cartridge: &mut FixtureCartridge, routine: &str, binding: &str, usage: RoutineUse) {
    nodule_mut(cartridge)
        .routines
        .get_mut(routine)
        .expect("a fixture routine")
        .uses
        .insert(binding.to_owned(), usage);
}

fn writes() -> RoutineUse {
    RoutineUse {
        write: true,
        ..Default::default()
    }
}

/// A node hosting alpha through a fake executor, with `remote` serving the operations of the
/// remote cartridge, enabled and granted every remote operation.
async fn hosted(
    cartridge: FixtureCartridge,
    grant: &[&str],
) -> (Node, Arc<FakeExecutor>, Arc<Probe>) {
    let executor = FakeExecutor::hosting(Arc::new(Echo::default()), true);
    let node = crate::e2e::node_with(Some(Arc::clone(&executor) as Arc<dyn NoduleExecutor>)).await;
    let remote = Probe::new();
    node.dispatch
        .router
        .register(
            "remote",
            [REMOTE, OTHER].map(|operation| {
                (
                    operation.to_owned(),
                    Operation::unchecked(Some(OperationKind::Query)),
                )
            }),
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::clone(&remote) as _)),
        )
        .unwrap();
    node.admit(node.operator(), cartridge).await.unwrap();
    node.grant_operations(ALPHA, grant).await;
    node.set_enabled(ALPHA, true).await;
    (node, executor, remote)
}

fn dispatch_of(executor: &FakeExecutor, routine: &str) -> CapId {
    executor.routine_capabilities()[routine]
        .dispatch
        .unwrap_or_else(|| panic!("`{routine}` holds no dispatch capability"))
}

impl Node {
    /// Invokes `operation` as the nodule's actor under `capability`.
    async fn invoke_as_nodule(
        &self,
        capability: CapId,
        operation: &str,
    ) -> Result<serde_json::Value, DispatchError> {
        let actor = self.actor_at(&id(ALPHA)).await;
        self.invoke(
            Authorization {
                authority: capability,
                signer: actor.capability,
            },
            operation,
            json!({}),
        )
        .await
    }

    /// Writes one datum of `kind` into the nodule's config document under `capability`.
    async fn write_kind(&self, capability: CapId, kind: &str) -> bool {
        let actor: OpenedActor = self.actor_at(&id(ALPHA)).await;
        let doc_id: DocId = self.install_of(ALPHA).await.config_doc;
        self.docs
            .client
            .call_with(
                RequestContext::root(),
                Authorization {
                    authority: capability,
                    signer: actor.capability,
                }
                .metadata(),
                WriteDatums {
                    capability,
                    command: None,
                    doc_id,
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::from([(
                        DatumKey::from_text(kind, "main").unwrap(),
                        json!({ "$type": kind }),
                    )]),
                    remove: BTreeSet::new(),
                },
            )
            .await
            .is_ok()
    }

    async fn is_live(&self, capability: CapId) -> bool {
        self.authority
            .client
            .call(RequestContext::root(), InspectCapability { capability })
            .await
            .is_ok()
    }
}

#[tokio::test]
async fn a_write_is_refused_for_the_routine_that_did_not_declare_it() {
    let mut cartridge = alpha(&[PING, SETUP], &[], &[NOTES, PREFS]);
    uses(&mut cartridge, PING, NOTES, writes());
    uses(&mut cartridge, SETUP, PREFS, writes());
    let (node, executor, _) = hosted(cartridge, &[]).await;
    let ping = executor.routine_capabilities()[PING]
        .docs
        .expect("ping writes notes");
    let setup = executor.routine_capabilities()[SETUP]
        .docs
        .expect("setup writes prefs");

    assert!(
        node.write_kind(ping, NOTES).await,
        "the declaring routine writes its kind"
    );
    assert!(
        !node.write_kind(setup, NOTES).await,
        "the sibling's write of that kind is refused"
    );
    assert!(
        !node.write_kind(ping, PREFS).await,
        "and so is the other way round"
    );
    assert!(node.write_kind(setup, PREFS).await);
    node.shutdown().await;
}

#[tokio::test]
async fn a_routine_cannot_call_an_operation_only_its_sibling_binds() {
    let mut cartridge = alpha(&[PING, SETUP], &[REMOTE, OTHER], &[]);
    uses(&mut cartridge, PING, OTHER, RoutineUse::default());
    uses(&mut cartridge, SETUP, REMOTE, RoutineUse::default());
    let (node, executor, _) = hosted(cartridge, &["org.example.remote"]).await;
    let ping = dispatch_of(&executor, PING);
    let setup = dispatch_of(&executor, SETUP);

    node.invoke_as_nodule(setup, REMOTE)
        .await
        .expect("the binding routine calls it");
    node.invoke_as_nodule(ping, OTHER)
        .await
        .expect("so does the other for its own");
    assert!(
        matches!(
            node.invoke_as_nodule(ping, REMOTE).await,
            Err(DispatchError::Denied)
        ),
        "the sibling's operation is refused at dispatch"
    );
    assert!(matches!(
        node.invoke_as_nodule(setup, OTHER).await,
        Err(DispatchError::Denied)
    ));
    node.shutdown().await;
}

#[tokio::test]
async fn a_narrowed_grant_retracts_the_old_handle_and_calls_wait_for_the_new_one() {
    let mut cartridge = alpha(&[PING], &[REMOTE, OTHER], &[]);
    uses(&mut cartridge, PING, REMOTE, RoutineUse::default());
    uses(&mut cartridge, PING, OTHER, RoutineUse::default());
    let (node, executor, _) = hosted(cartridge, &["org.example.remote"]).await;
    let old = dispatch_of(&executor, PING);
    node.invoke_as_nodule(old, OTHER)
        .await
        .expect("both are granted");

    // Narrow the grant while the reconcile is between applying capabilities and serving.
    let hold = executor.hold_next_ensure();
    let narrowed = node.set_methods(
        ALPHA,
        DispatchScope {
            operations: [REMOTE.to_owned()].into(),
            ..Default::default()
        },
    );
    let gap = async {
        hold.entered.notified().await;
        assert!(
            matches!(
                node.invoke(node.operator(), PING, json!({})).await,
                Err(DispatchError::NotReady { .. })
            ),
            "a call in the gap is not served"
        );
        assert!(
            node.invoke_as_nodule(old, REMOTE).await.is_err(),
            "the old handle is already retracted"
        );
        hold.release.notify_one();
    };
    tokio::join!(narrowed, gap);

    let new = dispatch_of(&executor, PING);
    assert_ne!(new, old);
    assert!(!node.is_live(old).await);
    node.invoke_as_nodule(new, REMOTE)
        .await
        .expect("the kept grant works");
    assert!(
        matches!(
            node.invoke_as_nodule(new, OTHER).await,
            Err(DispatchError::Denied)
        ),
        "the revoked grant does not"
    );

    // Revoking the rest leaves the routine only the right to ask for access (ADR 049 §4.2), on
    // a handle minted afresh: it reaches no operation.
    node.grant_operations(ALPHA, &[]).await;
    let asking = dispatch_of(&executor, PING);
    assert_ne!(asking, new);
    assert!(!node.is_live(new).await);
    assert!(
        matches!(
            node.invoke_as_nodule(asking, REMOTE).await,
            Err(DispatchError::Denied)
        ),
        "the routine can ask, and call nothing"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_reconcile_that_changes_nothing_mints_and_retracts_nothing() {
    let mut cartridge = alpha(&[PING, SETUP], &[REMOTE, OTHER], &[NOTES]);
    uses(&mut cartridge, PING, REMOTE, RoutineUse::default());
    uses(&mut cartridge, PING, NOTES, writes());
    uses(&mut cartridge, SETUP, OTHER, RoutineUse::default());
    // Granted as operations, so narrowing keeps the grant of the one that stays untouched.
    let (node, executor, _) = hosted(cartridge, &[]).await;
    node.set_methods(
        ALPHA,
        DispatchScope {
            operations: [REMOTE.to_owned(), OTHER.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    let handles = |executor: &FakeExecutor| {
        executor
            .routine_capabilities()
            .into_iter()
            .map(|(routine, held)| (routine, [held.dispatch, held.docs, held.drawers]))
            .collect::<BTreeMap<_, _>>()
    };
    let before = handles(&executor);
    node.touch(ALPHA).await;
    assert_eq!(
        handles(&executor),
        before,
        "the same handles, none re-minted"
    );
    for capability in before.values().flatten().flatten() {
        assert!(node.is_live(*capability).await, "and none retracted");
    }

    // A change that reaches one routine alone leaves the other's handles as they were.
    node.set_methods(
        ALPHA,
        DispatchScope {
            operations: [OTHER.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    let after = handles(&executor);
    assert_eq!(
        after[SETUP], before[SETUP],
        "the unaffected routine keeps its handles"
    );
    assert_eq!(
        after[PING][1], before[PING][1],
        "and so does the config handle of the one that changed"
    );
    // Its dispatch handle lost REMOTE, so it was re-minted narrower; the docs operation its
    // config write needs remains.
    assert_ne!(after[PING][0], before[PING][0]);
    assert!(!node.is_live(before[PING][0].unwrap()).await);
    node.shutdown().await;
}

#[tokio::test]
async fn a_local_initializer_target_is_served_with_its_implementing_routines_capabilities() {
    let mut cartridge = alpha(&[PING, SETUP], &[REMOTE, OTHER], &[]);
    uses(&mut cartridge, PING, REMOTE, RoutineUse::default());
    uses(&mut cartridge, SETUP, OTHER, RoutineUse::default());
    nodule_mut(&mut cartridge).initialization.push(Initializer {
        key: "setup".to_owned(),
        target: SETUP.parse().unwrap(),
        mode: RunMode::Once,
        extra: Default::default(),
    });
    let (node, executor, _) = hosted(cartridge, &["org.example.remote"]).await;
    node.until(ALPHA, Readiness::Ready).await;

    let delivered = executor.delivered();
    let [(operation, capabilities)] = delivered.as_slice() else {
        panic!("the initializer is the one call: {}", delivered.len())
    };
    assert_eq!(operation, SETUP);
    assert_eq!(
        capabilities.dispatch,
        executor.routine_capabilities()[SETUP].dispatch
    );
    assert_ne!(
        capabilities.dispatch,
        executor.routine_capabilities()[PING].dispatch
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_empty_use_of_a_method_permbind_grants_that_method() {
    let mut cartridge = alpha(&[PING], &[REMOTE], &[]);
    uses(&mut cartridge, PING, REMOTE, RoutineUse::default());
    let (node, executor, _) = hosted(cartridge, &["org.example.remote"]).await;
    node.invoke_as_nodule(dispatch_of(&executor, PING), REMOTE)
        .await
        .expect("listing the binding is the use");
    node.shutdown().await;
}

/// A routine whose `#method` permbind names an operation that needs a service handle is minted
/// that handle with the operation's flags (ADR 049 §6.2); its sibling, which does not bind the
/// operation, holds none.
#[tokio::test]
async fn only_the_routine_binding_blobs_put_is_minted_a_blobs_handle() {
    const BLOBS_PUT: &str = "org.example.daybook.blobs.put";
    let mut cartridge = alpha(&[PING, SETUP], &[BLOBS_PUT], &[]);
    uses(&mut cartridge, PING, BLOBS_PUT, RoutineUse::default());
    let (node, executor, _) = hosted(cartridge, &[BLOBS_PUT]).await;
    let capabilities = executor.routine_capabilities();
    let blobs = capabilities[PING]
        .blobs
        .expect("the routine binding blobs.put holds a blobs handle");
    assert!(node.is_live(blobs).await);
    assert_eq!(
        capabilities[SETUP].blobs, None,
        "its sibling binds no blobs operation"
    );
    node.shutdown().await;
}

/// A routine whose `#drawers` use is `create` alone can make drawers but holds no access to
/// the ones that exist: only a routine that uses `write` can write the granted drawer. Create
/// sets the scope's create flag and nothing else (ADR 028: grants never widen silently).
#[tokio::test]
async fn a_create_only_drawers_use_makes_drawers_but_writes_no_existing_one() {
    let mut cartridge = alpha(&[PING, SETUP], &[], &[]);
    cartridge.permbinds.insert(
        "drawers".into(),
        Permbind::Drawers(PermbindDrawers {
            description: "drawers".into(),
            resolution: Default::default(),
            read: None,
            write: Some(Necessity::Optional),
            create: Some(Necessity::Optional),
            delete: None,
            extra: Default::default(),
        }),
    );
    uses(
        &mut cartridge,
        PING,
        "drawers",
        RoutineUse {
            create: true,
            ..Default::default()
        },
    );
    uses(&mut cartridge, SETUP, "drawers", writes());
    let (node, executor, _) = hosted(cartridge, &[]).await;
    let present = node.drawers.local().present_id.clone();
    node.set_drawers(
        ALPHA,
        BTreeMap::from([(present.clone(), DrawerAccess::Write)]),
    )
    .await;
    let ping = executor.routine_capabilities()[PING]
        .drawers
        .expect("ping creates drawers");
    let setup = executor.routine_capabilities()[SETUP]
        .drawers
        .expect("setup writes the granted drawer");
    let actor = node.actor_at(&id(ALPHA)).await;
    let command = |name: &str| CommandId::try_from(name.to_owned()).unwrap();
    let authorization = |capability| {
        Authorization {
            authority: capability,
            signer: actor.capability,
        }
        .metadata()
    };

    let created = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            authorization(ping),
            CreateDrawer {
                capability: ping,
                command: command("ping-creates"),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .expect("a create use mints the right to create drawers")
        .drawer_id;
    // Records who owns what a routine creates through its handle: the owner is the actor the
    // handle chain originates from, which is the service, not the nodule actor it is bound to.
    assert!(reaches(&node, node.nodules_actor.capability, &created).await);
    assert!(!reaches(&node, actor.capability, &created).await);

    let create_meta_doc = |capability, name: &str| CreateMetaDoc {
        capability,
        command: command(name),
        drawer_id: present.clone(),
        kind: "org.example.alpha.meta".into(),
        name: "main".into(),
        root: MetaRoot::Public,
    };
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            authorization(setup),
            create_meta_doc(setup, "setup-writes"),
        )
        .await
        .expect("a write use reaches the granted drawer");
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            authorization(ping),
            create_meta_doc(ping, "ping-writes"),
        )
        .await
        .expect_err("a create use confers no write on an existing drawer");
    node.shutdown().await;
}

/// Whether `holder`, presenting its own identity, can mint a handle with Admin on `drawer`.
async fn reaches(node: &Node, holder: CapId, drawer: &DrawerId) -> bool {
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(holder).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::from([(drawer.clone(), DrawerAccess::Admin)]),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .is_ok()
}

/// Whether the `SETUP` routine's current drawers handle reads `drawer`.
async fn routine_reaches(
    node: &Node,
    executor: &FakeExecutor,
    actor: &OpenedActor,
    drawer: &DrawerId,
) -> bool {
    let handle = executor.routine_capabilities()[SETUP]
        .drawers
        .expect("a drawers handle");
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization {
                authority: handle,
                signer: actor.capability,
            }
            .metadata(),
            big_cabinet_core::drawer::protocol::GetDrawer {
                capability: handle,
                drawer_id: drawer.clone(),
            },
        )
        .await
        .is_ok()
}

/// A drawer the operator made, which the service's actor holds none of, is granted to a cartridge
/// through `setCartridgeGrant` alone: the call grants the drawer to the service's actor on the
/// operator's authority, the service hands it down, and revoking the grant takes the service's
/// hold back.
#[tokio::test]
async fn an_operator_made_drawer_granted_through_set_cartridge_grant_reaches_the_routine() {
    let mut cartridge = alpha(&[PING], &[], &[]);
    cartridge.permbinds.insert(
        "drawers".into(),
        Permbind::Drawers(PermbindDrawers {
            description: "drawers".into(),
            resolution: Default::default(),
            read: None,
            write: Some(Necessity::Optional),
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );
    uses(&mut cartridge, PING, "drawers", writes());
    let (mut node, executor, _) = hosted(cartridge, &[]).await;
    let operator = node.operator();
    let minted = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::new(),
                    create: true,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .expect("the operator mints the right to create drawers");
    let made = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization {
                authority: minted.capability_id,
                signer: operator.signer,
            }
            .metadata(),
            CreateDrawer {
                capability: minted.capability_id,
                command: CommandId::try_from("operator-makes".to_owned()).unwrap(),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .expect("the operator creates a drawer")
        .drawer_id;
    assert!(
        !reaches(&node, node.nodules_actor.capability, &made).await,
        "the service's actor holds none of the operator's drawers"
    );

    let record = node
        .set_drawers(ALPHA, BTreeMap::from([(made.clone(), DrawerAccess::Write)]))
        .await;
    assert_eq!(record.error, None);
    assert!(record.drawers[&made].granted);
    assert!(
        executor.routine_capabilities()[PING].drawers.is_some(),
        "the routine's drawers handle reaches the drawer with no staging by the test"
    );
    assert!(
        reaches(&node, node.nodules_actor.capability, &made).await,
        "the call granted the service"
    );
    // The hold is a standing grant, not one resting on a capability: the capability that made it
    // was retracted at once, and the routine still reaches the drawer after the service restarts.
    node.restart_nodules().await.unwrap();
    assert!(
        reaches(&node, node.nodules_actor.capability, &made).await,
        "the service's hold outlives a restart"
    );
    assert!(executor.routine_capabilities()[PING].drawers.is_some());

    node.set_grant(operator, ALPHA, GrantKey::Drawer(made.clone()), None)
        .await
        .unwrap();
    assert!(
        !reaches(&node, node.nodules_actor.capability, &made).await,
        "revoking the only grant ends the service's hold"
    );
    assert_eq!(executor.routine_capabilities()[PING].drawers, None);
    // Giving the hold back is durable too: nothing brings it back at the next start.
    node.restart_nodules().await.unwrap();
    assert!(!reaches(&node, node.nodules_actor.capability, &made).await);
    node.shutdown().await;
}

/// The native provider of `createDrawer` the egress reaches through dispatch: it runs the call
/// against the drawers service as the routine's actor presenting the handle in the body, as the
/// node's own provider does.
struct CreatesDrawers {
    drawers: service_client_native_rs::Client<big_cabinet_core::drawer::protocol::DrawersProtocol>,
}

#[async_trait::async_trait]
impl big_dispatch::Provider for CreatesDrawers {
    async fn call(
        &self,
        caller: &big_dispatch::Caller,
        _operation: &str,
        request: &RequestContext,
        _context: &big_dispatch_core::InvocationContext,
        input: serde_json::Value,
        _body: Option<big_dispatch_core::Body>,
    ) -> Result<big_dispatch_core::Body, big_dispatch::ProviderError> {
        let params: CreateDrawer = serde_json::from_value(input).expect("a createDrawer body");
        let created = self
            .drawers
            .call_with(
                request.child(),
                Authorization {
                    authority: params.capability,
                    signer: caller.authorization.signer,
                }
                .metadata(),
                params,
            )
            .await
            .expect("the routine's handle may create drawers");
        Ok(big_dispatch_core::Body::Json(
            serde_json::to_value(created).expect("a created drawer serializes"),
        ))
    }
}

const CREATE_DRAWER: &str = "org.example.daybook.drawers.createDrawer";

/// A drawer a routine creates through the host's egress is owned by the service's actor, which
/// the runtime then grants down: the cartridge gets a grant datum for it at the access the
/// routine's `#drawers` permbind declares (Write here, not the Admin the service owns it at),
/// the routine's re-minted handle reaches it, and none of that is the guest's doing. The
/// service's ownership is its own, so neither revoking that grant nor a restart takes it back.
#[tokio::test]
async fn a_drawer_a_routine_creates_is_owned_by_the_service_and_granted_to_the_cartridge() {
    let mut cartridge = alpha(&[SETUP], &[CREATE_DRAWER], &[]);
    cartridge.permbinds.insert(
        "makes".into(),
        Permbind::Drawers(PermbindDrawers {
            description: "makes".into(),
            resolution: Default::default(),
            read: None,
            write: Some(Necessity::Optional),
            create: Some(Necessity::Optional),
            delete: None,
            extra: Default::default(),
        }),
    );
    uses(
        &mut cartridge,
        SETUP,
        "makes",
        RoutineUse {
            write: true,
            create: true,
            ..Default::default()
        },
    );
    uses(&mut cartridge, SETUP, CREATE_DRAWER, RoutineUse::default());
    let (mut node, executor, _) = hosted(cartridge, &[CREATE_DRAWER]).await;
    node.dispatch
        .router
        .register(
            "drawers",
            [(
                CREATE_DRAWER.to_owned(),
                Operation::unchecked(Some(OperationKind::Procedure)),
            )],
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(CreatesDrawers {
                drawers: node.drawers.client.clone(),
            }))),
        )
        .unwrap();
    let actor = node.actor_at(&id(ALPHA)).await;

    let create = |node: &Node, command: &str| {
        let caller = crate::egress::WorkloadCaller {
            signer: actor.capability,
            missing_authority: executor.missing_authority(),
        };
        let handles = executor.routine_capabilities()[SETUP].clone();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            service_core_rs::xrpc::bearer_header_value(&Authorization::id_text(
                handles.dispatch.expect("a dispatch handle"),
            ))
            .parse()
            .unwrap(),
        );
        let body = serde_json::to_vec(&CreateDrawer {
            capability: handles.drawers.expect("a drawers handle"),
            command: CommandId::try_from(command.to_owned()).unwrap(),
            layout: CreateLayout::SingleGroup,
        })
        .unwrap();
        let client = node.dispatch.client.clone();
        async move {
            let (status, body) = crate::egress::call(
                &client,
                Some(&caller),
                &http::Method::POST,
                &format!("/xrpc/{CREATE_DRAWER}"),
                &headers,
                crate::XrpcBody::json(body),
            )
            .await;
            let crate::XrpcBody::Json(body) = body else {
                panic!("a created drawer is JSON")
            };
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            )
        }
    };
    let (status, body) = create(&node, "setup-creates").await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    let created: DrawerId = serde_json::from_value(body["drawerId"].clone()).unwrap();

    // Owned by the service's actor, not by the nodule's.
    assert!(reaches(&node, node.nodules_actor.capability, &created).await);
    assert!(!reaches(&node, actor.capability, &created).await);

    // The runtime wrote the grant datum, so it shows in the cartridge's consent and audit view.
    let grants: Vec<GrantKey> = node
        .cartridge_doc(ALPHA)
        .await
        .datums
        .keys()
        .filter_map(GrantKey::from_datum_key)
        .filter(|grant| matches!(grant, GrantKey::Drawer(_)))
        .collect();
    assert_eq!(grants, vec![GrantKey::Drawer(created.clone())]);
    let record = node.get(node.operator(), ALPHA).await.unwrap();
    assert_eq!(
        record.drawers[&created],
        NoduleGrantStatus {
            desired: DrawerAccess::Write,
            granted: true,
            error: None,
        }
    );

    // The routine's handle, re-minted by the reconcile, reaches the drawer.
    assert!(routine_reaches(&node, &executor, &actor, &created).await);

    // Survives a restart, and the cartridge's grant going does not take the owner's hold.
    node.restart_nodules().await.unwrap();
    assert!(routine_reaches(&node, &executor, &actor, &created).await);
    node.set_grant(
        node.operator(),
        ALPHA,
        GrantKey::Drawer(created.clone()),
        None,
    )
    .await
    .unwrap();
    assert!(reaches(&node, node.nodules_actor.capability, &created).await);
    node.restart_nodules().await.unwrap();
    assert!(reaches(&node, node.nodules_actor.capability, &created).await);
    node.shutdown().await;
}

/// A caller whose authority does not reach the drawer cannot grant it: the service's actor is
/// not granted it, and no grant datum is written.
#[tokio::test]
async fn a_caller_without_authority_over_the_drawer_cannot_grant_it() {
    let mut cartridge = alpha(&[PING], &[], &[]);
    cartridge.permbinds.insert(
        "drawers".into(),
        Permbind::Drawers(PermbindDrawers {
            description: "drawers".into(),
            resolution: Default::default(),
            read: None,
            write: Some(Necessity::Optional),
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );
    uses(&mut cartridge, PING, "drawers", writes());
    let (node, executor, _) = hosted(cartridge, &[]).await;
    let present = node.drawers.local().present_id.clone();
    // Manages alpha, and holds a handle on no drawer.
    let manager = node
        .mint(
            node.operator(),
            crate::e2e::scope(false, &[ALPHA]),
            Rights::USE,
        )
        .await
        .unwrap();
    node.set_grant(
        manager,
        ALPHA,
        GrantKey::Drawer(present.clone()),
        Some(GrantBody::access(DrawerAccess::Write.into())),
    )
    .await
    .expect_err("a manager with no authority over the drawer cannot grant it");
    assert!(
        node.cartridge_doc(ALPHA)
            .await
            .datums
            .keys()
            .filter_map(GrantKey::from_datum_key)
            .all(|grant| !matches!(grant, GrantKey::Drawer(_))),
        "no grant datum was written"
    );
    assert_eq!(executor.routine_capabilities()[PING].drawers, None);
    node.shutdown().await;
}

/// A grant is the cartridge's, not a permbind's (ADR 049 §4.1): granting a drawer once reaches
/// every routine whose `uses` take a `#drawers` permbind that needs it, a routine whose `uses`
/// do not take one does not reach the drawer however it is granted, and revoking the grant
/// retracts the handles of every routine that held it.
#[tokio::test]
async fn one_drawer_grant_reaches_every_routine_that_uses_it_and_revoking_retracts_them_all() {
    const PEEK: &str = "org.example.alpha.peek";
    let mut cartridge = alpha(&[PING, SETUP, PEEK], &[], &[]);
    for (binding, read, write) in [
        ("readable", Some(Necessity::Optional), None),
        ("writable", None, Some(Necessity::Optional)),
    ] {
        cartridge.permbinds.insert(
            binding.into(),
            Permbind::Drawers(PermbindDrawers {
                description: binding.into(),
                resolution: Default::default(),
                read,
                write,
                create: None,
                delete: None,
                extra: Default::default(),
            }),
        );
    }
    // Two routines hold different permbinds over drawers, and PEEK holds none: its `uses` do
    // not take the drawers, so the grant does not reach it.
    uses(
        &mut cartridge,
        PING,
        "readable",
        RoutineUse {
            read: true,
            ..Default::default()
        },
    );
    uses(&mut cartridge, SETUP, "writable", writes());
    let (node, executor, _) = hosted(cartridge, &[]).await;
    let present = node.drawers.local().present_id.clone();
    assert!(
        executor
            .routine_capabilities()
            .values()
            .all(|held| held.drawers.is_none()),
        "no grant, no drawers"
    );

    // One grant of the drawer at Write satisfies both permbinds, each at what it declares.
    node.set_drawers(
        ALPHA,
        BTreeMap::from([(present.clone(), DrawerAccess::Write)]),
    )
    .await;
    let held = executor.routine_capabilities();
    let ping = held[PING].drawers.expect("ping reads the granted drawer");
    let setup = held[SETUP]
        .drawers
        .expect("setup writes the granted drawer");
    assert_ne!(ping, setup, "each routine holds a handle of its own");
    assert!(
        held[PEEK].drawers.is_none(),
        "a routine whose `uses` do not take the drawers does not reach a granted one"
    );
    assert!(node.is_live(ping).await && node.is_live(setup).await);

    // Revoking the one grant retracts both routines' handles.
    node.set_drawers(ALPHA, BTreeMap::new()).await;
    assert!(!node.is_live(ping).await, "ping's handle is retracted");
    assert!(!node.is_live(setup).await, "setup's handle is retracted");
    assert!(
        executor
            .routine_capabilities()
            .values()
            .all(|held| held.drawers.is_none()),
        "no routine holds drawers once the grant is revoked"
    );
    node.shutdown().await;
}

/// A routine whose `uses` take a `#drawers` permbind and `#datum` kinds reaches the documents of
/// each granted drawer for those kinds and no others (ADR 049 §2): a reader of one kind cannot
/// read another or write the one it reads, a writer of that kind cannot write another, and a
/// routine that names no `#datum` kind reaches no document in any drawer.
#[tokio::test]
async fn a_routine_reaches_granted_drawers_only_for_its_datum_kinds() {
    const LOG: &str = "org.example.alpha.log";
    const SECRET: &str = "org.example.alpha.secret";
    const PEEK: &str = "org.example.alpha.peek";
    const KEEP: &str = "org.example.alpha.keep";
    const SPLIT: &str = "org.example.alpha.split";
    let mut cartridge = alpha(&[PING, SETUP, PEEK, KEEP, SPLIT], &[], &[]);
    for kind in [LOG, SECRET] {
        cartridge.with_lexicon(lexicon(json!({
            "lexicon": 1,
            "id": kind,
            "defs": { "main": { "type": "object", "properties": {} } }
        })));
    }
    for (binding, kind) in [("log", LOG), ("secret", SECRET)] {
        cartridge.permbinds.insert(
            binding.into(),
            Permbind::Datum(PermbindDatum {
                nsid: kind.parse().unwrap(),
                description: binding.into(),
                read: Some(Necessity::Optional),
                write: Some(Necessity::Optional),
                create: None,
                delete: None,
                extra: Default::default(),
            }),
        );
    }
    cartridge.permbinds.insert(
        "drawers".into(),
        Permbind::Drawers(PermbindDrawers {
            description: "drawers".into(),
            resolution: Default::default(),
            read: Some(Necessity::Optional),
            write: Some(Necessity::Optional),
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );
    let reads = || RoutineUse {
        read: true,
        ..Default::default()
    };
    // PING reads log from the drawers, KEEP writes it, SETUP reads the drawers and names no
    // datum kind, and PEEK reads log without any drawers permbind.
    uses(&mut cartridge, PING, "drawers", reads());
    uses(&mut cartridge, PING, "log", reads());
    uses(&mut cartridge, KEEP, "drawers", writes());
    uses(&mut cartridge, KEEP, "log", writes());
    uses(&mut cartridge, SETUP, "drawers", reads());
    uses(&mut cartridge, PEEK, "log", reads());
    // SPLIT reads log and writes secret: the kinds are held per direction.
    uses(&mut cartridge, SPLIT, "drawers", writes());
    uses(&mut cartridge, SPLIT, "log", reads());
    uses(&mut cartridge, SPLIT, "secret", writes());
    let (node, executor, _) = hosted(cartridge, &[]).await;
    let present = node.drawers.local().present_id.clone();
    node.set_drawers(
        ALPHA,
        BTreeMap::from([(present.clone(), DrawerAccess::Write)]),
    )
    .await;
    let held = executor.routine_capabilities();
    assert!(
        held[SETUP].docs.is_none(),
        "a drawers use without a datum kind reaches no document"
    );
    assert!(
        held[PEEK].docs.is_none(),
        "a datum kind that is not a settings kind reaches nothing without a drawers use"
    );
    let reader = held[PING].docs.expect("ping reads log from the drawers");
    let keeper = held[KEEP].docs.expect("keep writes log into the drawers");
    let split = held[SPLIT].docs.expect("split reads log and writes secret");

    // The nodule's actor puts one document of each kind into the granted drawer.
    let actor = node.actor_at(&id(ALPHA)).await;
    let placement = format!("drawer {present}");
    let create = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    create: BTreeSet::from([placement.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap();
    let log_key = DatumKey::from_text(LOG, "main").unwrap();
    let secret_key = DatumKey::from_text(SECRET, "main").unwrap();
    let doc_id = node
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability: create.capability_id,
                command: CommandId::try_from("drawer-doc".to_owned()).unwrap(),
                placement,
                datums: BTreeMap::from([
                    (log_key.clone(), json!({ "$type": LOG })),
                    (secret_key.clone(), json!({ "$type": SECRET })),
                ]),
            },
        )
        .await
        .unwrap()
        .doc_id;
    let authorization = |capability| {
        Authorization {
            authority: capability,
            signer: actor.capability,
        }
        .metadata()
    };
    let read = async |capability, datums: Option<BTreeSet<DatumKey>>| {
        node.docs
            .client
            .call_with(
                RequestContext::root(),
                authorization(capability),
                ReadDatums {
                    capability,
                    doc_id: doc_id.clone(),
                    at: At::Current,
                    datums,
                    optimistic: false,
                },
            )
            .await
    };
    let write = async |capability, key: &DatumKey, kind: &str| {
        node.docs
            .client
            .call_with(
                RequestContext::root(),
                authorization(capability),
                WriteDatums {
                    capability,
                    command: None,
                    doc_id: doc_id.clone(),
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::from([(key.clone(), json!({ "$type": kind }))]),
                    remove: BTreeSet::new(),
                },
            )
            .await
    };
    let denied = ClientError::Method(big_cabinet_core::doc::DocsError::Denied);

    // The reader sees the log kind of the drawer's document and not the secret kind.
    let seen = read(reader, None).await.unwrap();
    assert!(seen.datums.contains_key(&log_key));
    assert!(!seen.datums.contains_key(&secret_key));
    assert_eq!(
        read(reader, Some(BTreeSet::from([secret_key.clone()])))
            .await
            .unwrap_err(),
        denied
    );
    // Its drawers use only reads, so the log kind is not writable either.
    assert_eq!(write(reader, &log_key, LOG).await.unwrap_err(), denied);
    // The keeper writes the log kind and no other.
    write(keeper, &log_key, LOG).await.unwrap();
    assert_eq!(
        write(keeper, &secret_key, SECRET).await.unwrap_err(),
        denied
    );
    // The split routine reads log, writes secret, and is refused writing log (which it only
    // reads) and reading secret (which it only writes).
    let seen = read(split, None).await.unwrap();
    assert!(seen.datums.contains_key(&log_key));
    assert!(!seen.datums.contains_key(&secret_key));
    write(split, &secret_key, SECRET).await.unwrap();
    assert_eq!(write(split, &log_key, LOG).await.unwrap_err(), denied);
    node.shutdown().await;
}

/// Runtime grant requests (ADR 049 §4.2): a routine files a request through the cartridges
/// service presenting its dispatch handle, a person answers, and the routine's handles follow.
mod grant_requests {
    use super::*;
    use big_cabinet_core::drawer::protocol::DrawerId;
    use big_cartridges_core::config::GrantBody;
    use big_cartridges_core::config::GrantKey;
    use big_cartridges_core::protocol::AWAIT_MAX_HOLD_MS;
    use big_cartridges_core::protocol::AnswerCartridgeGrantRequest;
    use big_cartridges_core::protocol::AwaitCartridgeGrantRequest;
    use big_cartridges_core::protocol::AwaitCartridgeGrants;
    use big_cartridges_core::protocol::CartridgesError;
    use big_cartridges_core::protocol::FiledGrantRequest;
    use big_cartridges_core::protocol::GrantAnswer;
    use big_cartridges_core::protocol::GrantRequestState;
    use big_cartridges_core::protocol::ListCartridgeGrantRequests;
    use big_cartridges_core::protocol::ListCartridgeGrants;
    use big_cartridges_core::protocol::RequestCartridgeGrant;
    use big_cartridges_core::protocol::RequestedAccess;
    use big_cartridges_core::protocol::SetCartridgeGrant;
    use big_dispatch_core::DispatchError;
    use big_dispatch_core::DispatchScope;

    const BETA: &str = example_cartridge!("beta");

    /// Alpha's `ping` routine uses a drawers permbind (`drawers`, which writes) and the remote
    /// method. The cartridge also declares a datum permbind and a drawers permbind that only
    /// creates, neither of which has an answer of its own.
    fn requesting() -> FixtureCartridge {
        let mut cartridge = alpha(&[PING], &[REMOTE], &[NOTES]);
        for (binding, write, create) in [("drawers", true, false), ("makes", false, true)] {
            cartridge.permbinds.insert(
                binding.into(),
                Permbind::Drawers(PermbindDrawers {
                    description: format!("{binding} description"),
                    resolution: Default::default(),
                    read: None,
                    write: write.then_some(Necessity::Optional),
                    create: create.then_some(Necessity::Optional),
                    delete: None,
                    extra: Default::default(),
                }),
            );
        }
        uses(&mut cartridge, PING, "drawers", writes());
        uses(&mut cartridge, PING, REMOTE, RoutineUse::default());
        cartridge
    }

    /// Alpha hosted with no grants at all, and the drawer a person would pick.
    async fn asking() -> (Node, Arc<FakeExecutor>, DrawerId) {
        let (node, executor, _) = hosted(requesting(), &[]).await;
        let drawer = node.drawers.local().present_id.clone();
        (node, executor, drawer)
    }

    impl Node {
        /// What a call from alpha's `ping` routine presents: the dispatch handle minted for it,
        /// signed by the nodule's actor, as dispatch forwards it to a provider. Read at the call
        /// because a restart mints it afresh.
        async fn as_ping(&self, executor: &FakeExecutor) -> Authorization {
            Authorization {
                authority: dispatch_of(executor, PING),
                signer: self.actor_at(&id(ALPHA)).await.capability,
            }
        }

        async fn file(
            &self,
            from: Authorization,
            permbind: &str,
            drawer: Option<DrawerId>,
        ) -> Result<FiledGrantRequest, CartridgesError> {
            self.call(
                from,
                RequestCartridgeGrant {
                    permbind: permbind.into(),
                    drawer,
                },
            )
            .await
        }

        async fn answer(
            &self,
            request: big_cabinet_core::dmeta::Uuid7,
            answer: GrantAnswer,
            drawer: Option<DrawerId>,
        ) -> Result<GrantRequestState, CartridgesError> {
            self.call(
                self.operator(),
                AnswerCartridgeGrantRequest {
                    request,
                    answer,
                    drawer,
                },
            )
            .await
            .map(|answered| answered.state)
        }

        async fn pending(&self) -> Vec<big_cartridges_core::protocol::GrantRequestRecord> {
            self.call(self.operator(), ListCartridgeGrantRequests {})
                .await
                .unwrap()
                .requests
        }

        async fn grant_datum_present(&self, grant: GrantKey) -> bool {
            self.cartridge_doc(ALPHA)
                .await
                .datums
                .contains_key(&grant.datum_key())
        }

        /// Whether alpha's `ping` routine can call the remote method now.
        async fn ping_reaches_remote(&self, executor: &FakeExecutor) -> bool {
            match self
                .invoke_as_nodule(dispatch_of(executor, PING), REMOTE)
                .await
            {
                Ok(_) => true,
                Err(DispatchError::Denied) => false,
                Err(other) => panic!("unexpected dispatch failure: {other:?}"),
            }
        }
    }

    impl Node {
        /// What alpha's `ping` routine sees when it calls the query `operation` through the host's
        /// egress with its dispatch handle: the HTTP status and JSON body its guest is served.
        async fn ping_calls(
            &self,
            executor: &FakeExecutor,
            operation: &str,
        ) -> (http::StatusCode, serde_json::Value) {
            let caller = crate::egress::WorkloadCaller {
                signer: self.actor_at(&id(ALPHA)).await.capability,
                missing_authority: executor.missing_authority(),
            };
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::AUTHORIZATION,
                service_core_rs::xrpc::bearer_header_value(&Authorization::id_text(dispatch_of(
                    executor, PING,
                )))
                .parse()
                .unwrap(),
            );
            let (status, body) = crate::egress::call(
                &self.dispatch.client,
                Some(&caller),
                &http::Method::GET,
                &format!("/xrpc/{operation}"),
                &headers,
                crate::XrpcBody::json(Vec::new()),
            )
            .await;
            let crate::XrpcBody::Json(body) = body else {
                panic!("an XRPC error is JSON")
            };
            (status, serde_json::from_slice(&body).unwrap())
        }
    }

    /// The error a guest decodes, as every method's error wraps `ContextError` (ADR 049 §4.3).
    fn missing_authority(
        cartridge: &str,
        permbind: &str,
        request: Option<String>,
    ) -> serde_json::Value {
        json!({
            "Context": {
                "MissingAuthority": {
                    "cartridge": cartridge,
                    "permbind": permbind,
                    "request": request,
                }
            }
        })
    }

    #[tokio::test]
    async fn a_declared_but_ungranted_call_is_missing_authority_and_an_undeclared_one_is_denied() {
        let (node, executor, _) = asking().await;
        let alpha_id = id(ALPHA).to_string();

        // Declared (`uses` takes the permbind that names it) but not granted: nobody has said
        // yes yet, which no request has been filed for, since nothing files one implicitly.
        let (status, body) = node.ping_calls(&executor, REMOTE).await;
        assert_eq!(status, http::StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "MissingAuthority");
        assert_eq!(
            body["operationError"],
            missing_authority(&alpha_id, REMOTE, None)
        );
        assert!(
            node.pending().await.is_empty(),
            "the failed call filed nothing"
        );

        // Never declared by this routine: the rules forbid it, so it stays a denial.
        let (status, body) = node.ping_calls(&executor, OTHER).await;
        assert_eq!(status, http::StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "Denied");
        assert!(body.get("operationError").is_none());

        // Once the routine has filed a request, the same call names it.
        let ping = node.as_ping(&executor).await;
        let request = node
            .file(ping, REMOTE, None)
            .await
            .unwrap()
            .request
            .unwrap();
        let (_, body) = node.ping_calls(&executor, REMOTE).await;
        assert_eq!(
            body["operationError"],
            missing_authority(&alpha_id, REMOTE, Some(request.to_string()))
        );

        // Declined, the call is still the cartridge's missing authority, with no request left.
        node.answer(request, GrantAnswer::Decline, None)
            .await
            .unwrap();
        let (_, body) = node.ping_calls(&executor, REMOTE).await;
        assert_eq!(
            body["operationError"],
            missing_authority(&alpha_id, REMOTE, None)
        );

        // Allowed, the routine's handle is re-minted and the call goes through.
        node.set_methods(
            ALPHA,
            DispatchScope {
                operations: [REMOTE.to_owned()].into(),
                ..Default::default()
            },
        )
        .await;
        let (status, _) = node.ping_calls(&executor, REMOTE).await;
        assert_eq!(status, http::StatusCode::OK);
        node.shutdown().await;
    }

    /// Cartridge X's routine calls an operation another cartridge Y serves, and Y's own call
    /// failed for want of a grant. Y's handler answered with Y's `MissingAuthority` (a domain
    /// error, as a guest's method error is served); it reaches X as it was, naming Y.
    #[tokio::test]
    async fn another_cartridges_missing_authority_reaches_the_caller_unchanged() {
        let (node, executor, remote) = hosted(requesting(), &["org.example.remote"]).await;
        let beta = id(BETA).to_string();
        let served = serde_json::to_value(service_core_rs::xrpc::domain_error_envelope(
            &DispatchError::Context(method_core::ContextError::MissingAuthority {
                cartridge: beta.clone(),
                permbind: "its-binding".to_owned(),
                request: Some("018f0000-0000-7000-8000-000000000001".to_owned()),
            }),
        ))
        .unwrap();
        remote.refuse_with(REMOTE, 400, served.clone());

        let (status, body) = node.ping_calls(&executor, REMOTE).await;
        assert_eq!(status, http::StatusCode::BAD_REQUEST);
        assert_eq!(body, served, "the serving path rewrote Y's error");
        assert_eq!(
            body["operationError"],
            missing_authority(
                &beta,
                "its-binding",
                Some("018f0000-0000-7000-8000-000000000001".to_owned())
            ),
            "it names Y, not the caller"
        );
        node.shutdown().await;
    }

    #[tokio::test]
    async fn filing_is_refused_for_an_unrequestable_permbind_and_for_anyone_but_the_cartridge() {
        let (node, executor, drawer) = asking().await;
        let ping = node.as_ping(&executor).await;

        // The cartridge may file only for a permbind its manifest declares, and only a method
        // or a drawers permbind that grants something has an answer of its own.
        for permbind in ["undeclared", NOTES, "makes"] {
            assert!(
                matches!(
                    node.file(ping, permbind, None).await,
                    Err(CartridgesError::Rejected(_))
                ),
                "{permbind}"
            );
        }
        assert!(
            matches!(
                node.file(ping, REMOTE, Some(drawer.clone())).await,
                Err(CartridgesError::Rejected(_))
            ),
            "a method request names no drawer"
        );

        // The cartridge is the caller's, never a field: the operator and a stranger run for
        // none, and another cartridge's actor files only for what that cartridge declares.
        node.admit(
            node.operator(),
            init_cartridge("beta", "1.0.0", &[], &[], &[]),
        )
        .await
        .unwrap();
        let beta = Authorization::identity(node.actor_at(&id(BETA)).await.capability);
        let stranger = node.bystander().await;
        let handle_under_another_signer = Authorization {
            signer: beta.signer,
            ..ping
        };
        for (presenter, expected) in [
            (node.operator(), CartridgesError::Denied),
            (stranger, CartridgesError::Denied),
            (handle_under_another_signer, CartridgesError::Denied),
        ] {
            assert_eq!(
                node.file(presenter, "drawers", Some(drawer.clone()))
                    .await
                    .unwrap_err(),
                expected
            );
        }
        assert!(
            matches!(
                node.file(beta, "drawers", Some(drawer)).await,
                Err(CartridgesError::Rejected(_))
            ),
            "beta declares no `drawers`; it cannot file alpha's"
        );
        assert!(node.pending().await.is_empty());
        node.shutdown().await;
    }

    #[tokio::test]
    async fn listing_answering_and_listing_grants_are_the_operators_alone() {
        let (node, executor, _) = asking().await;
        let ping = node.as_ping(&executor).await;
        let filed = node.file(ping, REMOTE, None).await.unwrap();
        let request = filed.request.expect("a request was filed");
        for presenter in [ping, node.bystander().await] {
            assert_eq!(
                node.call(presenter, ListCartridgeGrantRequests {})
                    .await
                    .unwrap_err(),
                CartridgesError::Denied
            );
            assert_eq!(
                node.call(
                    presenter,
                    AnswerCartridgeGrantRequest {
                        request,
                        answer: GrantAnswer::Allow,
                        drawer: None,
                    }
                )
                .await
                .unwrap_err(),
                CartridgesError::Denied
            );
            assert_eq!(
                node.call(presenter, ListCartridgeGrants::default())
                    .await
                    .unwrap_err(),
                CartridgesError::Denied
            );
        }
        assert_eq!(node.pending().await.len(), 1, "nothing was answered");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn allowing_a_drawer_request_writes_the_grant_and_the_routines_handle_appears() {
        let (node, executor, drawer) = asking().await;
        let ping = node.as_ping(&executor).await;
        assert_eq!(executor.routine_capabilities()[PING].drawers, None);

        let filed = node
            .file(ping, "drawers", Some(drawer.clone()))
            .await
            .unwrap();
        let request = filed.request.expect("a request was filed");
        assert_eq!(filed.state, GrantRequestState::Pending);
        assert_eq!(
            node.file(ping, "drawers", None).await.unwrap(),
            filed,
            "filing the same permbind again returns the pending request"
        );
        let [waiting] = node.pending().await.try_into().unwrap();
        assert_eq!(waiting.id, request);
        assert_eq!(waiting.cartridge_id, id(ALPHA));
        assert_eq!(
            waiting.access,
            RequestedAccess::Drawers {
                access: DrawerAccess::Write,
                drawer: Some(drawer.clone()),
            }
        );
        assert_eq!(waiting.description, "drawers description");

        // The await is held while pending and resolves when the answer lands: the awaiter is
        // never answered before the handles are re-minted.
        let held = node
            .call(
                ping,
                AwaitCartridgeGrantRequest {
                    request,
                    hold_ms: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(held.state, GrantRequestState::Pending);
        let (awaited, answered) = tokio::join!(
            node.call(
                ping,
                AwaitCartridgeGrantRequest {
                    request,
                    hold_ms: 10_000,
                },
            ),
            node.answer(request, GrantAnswer::Allow, None),
        );
        assert_eq!(answered.unwrap(), GrantRequestState::Allowed);
        assert_eq!(awaited.unwrap().state, GrantRequestState::Allowed);

        assert!(
            executor.routine_capabilities()[PING].drawers.is_some(),
            "the routine's drawers handle was minted from the grant"
        );
        assert!(
            node.grant_datum_present(GrantKey::Drawer(drawer.clone()))
                .await,
            "an allow writes the grant datum"
        );
        assert!(node.pending().await.is_empty());
        let granted = node
            .call(
                node.operator(),
                ListCartridgeGrants {
                    cartridge_id: Some(id(ALPHA)),
                },
            )
            .await
            .unwrap()
            .grants;
        let [record] = granted.try_into().unwrap();
        assert_eq!(record.grant, GrantKey::Drawer(drawer.clone()));
        assert!(!record.this_boot);
        assert!(
            matches!(
                node.answer(request, GrantAnswer::Decline, None).await,
                Err(CartridgesError::Rejected(reason)) if reason.contains("already answered")
            ),
            "a request is answered once"
        );

        // Only the cartridge that filed it may ask after it.
        node.admit(
            node.operator(),
            init_cartridge("beta", "1.0.0", &[], &[], &[]),
        )
        .await
        .unwrap();
        let beta = Authorization::identity(node.actor_at(&id(BETA)).await.capability);
        assert!(matches!(
            node.call(
                beta,
                AwaitCartridgeGrantRequest {
                    request,
                    hold_ms: 0
                }
            )
            .await,
            Err(CartridgesError::Rejected(_))
        ));
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_drawers_request_naming_no_drawer_is_answered_with_the_drawer_the_person_picks() {
        let (node, executor, drawer) = asking().await;
        let ping = node.as_ping(&executor).await;
        let request = node
            .file(ping, "drawers", None)
            .await
            .unwrap()
            .request
            .unwrap();
        assert!(matches!(
            node.answer(request, GrantAnswer::Allow, None).await,
            Err(CartridgesError::Rejected(_))
        ));
        assert_eq!(
            node.answer(request, GrantAnswer::Allow, Some(drawer.clone()))
                .await
                .unwrap(),
            GrantRequestState::Allowed
        );
        assert!(node.grant_datum_present(GrantKey::Drawer(drawer)).await);
        assert!(executor.routine_capabilities()[PING].drawers.is_some());
        node.shutdown().await;
    }

    /// Allow writes a datum that outlives the boot; only-this-boot grants the same handle and
    /// writes nothing, so it is gone after a restart and a routine refiles.
    #[tokio::test]
    async fn only_this_boot_grants_without_a_datum_and_is_gone_after_a_restart() {
        for (answer, written) in [
            (GrantAnswer::Allow, true),
            (GrantAnswer::OnlyThisBoot, false),
        ] {
            let (mut node, executor, _) = asking().await;
            let ping = node.as_ping(&executor).await;
            assert!(!node.ping_reaches_remote(&executor).await);
            let request = node
                .file(ping, REMOTE, None)
                .await
                .unwrap()
                .request
                .unwrap();
            node.answer(request, answer, None).await.unwrap();

            assert!(
                node.ping_reaches_remote(&executor).await,
                "{answer:?}: the handle is minted from the answer"
            );
            assert_eq!(
                node.grant_datum_present(GrantKey::Method(REMOTE.into()))
                    .await,
                written,
                "{answer:?}"
            );
            let listed = node
                .call(
                    node.operator(),
                    ListCartridgeGrants {
                        cartridge_id: Some(id(ALPHA)),
                    },
                )
                .await
                .unwrap()
                .grants;
            let [record] = listed.try_into().unwrap();
            assert_eq!(record.this_boot, !written, "{answer:?}");

            node.restart_nodules().await.unwrap();
            assert_eq!(
                node.ping_reaches_remote(&executor).await,
                written,
                "{answer:?}: only the datum survives a restart"
            );
            assert!(node.pending().await.is_empty(), "the queue is in memory");
            if !written {
                let ping = node.as_ping(&executor).await;
                let refiled = node.file(ping, REMOTE, None).await.unwrap();
                assert_eq!(refiled.state, GrantRequestState::Pending);
                assert_ne!(refiled.request, Some(request), "a refile is a new request");
            }
            node.shutdown().await;
        }
    }

    /// The grants feed answers at once without a revision, holds on the current one until an
    /// answer moves it, and moves again when a revoke takes the grant back.
    #[tokio::test]
    async fn the_grants_feed_holds_until_an_answer_or_a_revoke_moves_it() {
        let (node, executor, _) = asking().await;
        let feed = |since: Option<&str>, hold_ms| {
            node.call(
                node.operator(),
                AwaitCartridgeGrants {
                    cartridge_id: id(ALPHA),
                    since: since.map(str::to_owned),
                    hold_ms,
                },
            )
        };
        let first = feed(None, 0).await.unwrap();
        // An expired hold answers the unchanged grants at the same revision.
        assert_eq!(feed(Some(&first.revision), 50).await.unwrap(), first);

        let ping = node.as_ping(&executor).await;
        let request = node
            .file(ping, REMOTE, None)
            .await
            .unwrap()
            .request
            .unwrap();
        let (moved, answered) = tokio::join!(
            feed(Some(&first.revision), AWAIT_MAX_HOLD_MS),
            node.answer(request, GrantAnswer::OnlyThisBoot, None)
        );
        answered.unwrap();
        let moved = moved.unwrap();
        assert_ne!(moved.revision, first.revision);
        let added: Vec<_> = moved
            .grants
            .iter()
            .filter(|record| !first.grants.contains(record))
            .collect();
        let [record] = added.try_into().unwrap();
        assert_eq!(record.grant, GrantKey::Method(REMOTE.into()));
        assert!(record.this_boot);

        node.call(
            node.operator(),
            SetCartridgeGrant {
                cartridge_id: id(ALPHA),
                grant: GrantKey::Method(REMOTE.into()),
                value: None,
            },
        )
        .await
        .unwrap();
        let revoked = feed(Some(&moved.revision), 0).await.unwrap();
        assert_eq!(revoked.grants, first.grants);
        assert_ne!(revoked.revision, moved.revision);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn revoking_takes_back_an_only_this_boot_grant_too() {
        let (node, executor, _) = asking().await;
        let ping = node.as_ping(&executor).await;
        let request = node
            .file(ping, REMOTE, None)
            .await
            .unwrap()
            .request
            .unwrap();
        node.answer(request, GrantAnswer::OnlyThisBoot, None)
            .await
            .unwrap();
        assert!(node.ping_reaches_remote(&executor).await);
        node.call(
            node.operator(),
            SetCartridgeGrant {
                cartridge_id: id(ALPHA),
                grant: GrantKey::Method(REMOTE.into()),
                value: None,
            },
        )
        .await
        .unwrap();
        assert!(!node.ping_reaches_remote(&executor).await);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_decline_suppresses_a_refile_for_the_boot_and_ends_with_it() {
        let (mut node, executor, drawer) = asking().await;
        let ping = node.as_ping(&executor).await;
        let request = node
            .file(ping, REMOTE, None)
            .await
            .unwrap()
            .request
            .unwrap();
        assert_eq!(
            node.answer(request, GrantAnswer::Decline, None)
                .await
                .unwrap(),
            GrantRequestState::Declined
        );
        assert!(!node.ping_reaches_remote(&executor).await);
        assert!(
            !node
                .grant_datum_present(GrantKey::Method(REMOTE.into()))
                .await
        );

        // Filing again is answered at once, with no new pending request.
        assert_eq!(
            node.file(ping, REMOTE, None).await.unwrap(),
            FiledGrantRequest {
                request: None,
                state: GrantRequestState::Declined,
            }
        );
        assert!(node.pending().await.is_empty());
        // What awaited the declined request is told so, and the decline is the permbind's alone.
        let awaited = node
            .call(
                ping,
                AwaitCartridgeGrantRequest {
                    request,
                    hold_ms: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(awaited.state, GrantRequestState::Declined);
        assert_eq!(
            node.file(ping, "drawers", Some(drawer))
                .await
                .unwrap()
                .state,
            GrantRequestState::Pending
        );

        node.restart_nodules().await.unwrap();
        let ping = node.as_ping(&executor).await;
        assert_eq!(
            node.file(ping, REMOTE, None).await.unwrap().state,
            GrantRequestState::Pending,
            "a decline lasts until the node next starts"
        );
        node.shutdown().await;
    }

    /// A grant already in force settles a request at once, so a routine that files on every
    /// start is not prompted again for what the person already allowed.
    #[tokio::test]
    async fn filing_for_what_is_already_granted_files_nothing() {
        let (node, executor, _) = asking().await;
        node.set_methods(
            ALPHA,
            DispatchScope {
                operations: [REMOTE.to_owned()].into(),
                ..Default::default()
            },
        )
        .await;
        let ping = node.as_ping(&executor).await;
        assert_eq!(
            node.file(ping, REMOTE, None).await.unwrap(),
            FiledGrantRequest {
                request: None,
                state: GrantRequestState::Allowed,
            }
        );
        assert!(node.pending().await.is_empty());
        node.shutdown().await;
    }

    /// Alpha's `ping` routine takes two `#net` permbinds: GET and HEAD to `api.example.org:8443`,
    /// and any verb to `www.example.org` on any port (a permbind without a port, as in Deno's
    /// `--allow-net`).
    fn networked() -> FixtureCartridge {
        let mut cartridge = requesting();
        for (binding, host, port, methods) in [
            (
                "api",
                "api.example.org",
                Some(8443),
                Some(["GET", "HEAD"].map(str::to_owned).into()),
            ),
            ("www", "www.example.org", None, None),
        ] {
            cartridge.permbinds.insert(
                binding.into(),
                Permbind::Net(big_cartridges_core::PermbindNet {
                    host: host.into(),
                    port,
                    methods,
                    description: format!("{binding} description"),
                    extra: Default::default(),
                }),
            );
            uses(&mut cartridge, PING, binding, RoutineUse::default());
        }
        cartridge
    }

    impl Node {
        /// What the egress answers alpha's `ping` routine for a `method` request to `url`:
        /// `Ok` when it may go out, else the refusal its guest is served.
        async fn ping_fetches(
            &self,
            executor: &FakeExecutor,
            method: http::Method,
            url: &str,
        ) -> Result<(), (http::StatusCode, serde_json::Value)> {
            let caller = crate::egress::WorkloadCaller {
                signer: self.actor_at(&id(ALPHA)).await.capability,
                missing_authority: executor.missing_authority(),
            };
            let mut headers = http::HeaderMap::new();
            headers.insert(
                service_core_rs::xrpc::NODULE_CAPABILITY_HEADER,
                Authorization::id_text(dispatch_of(executor, PING))
                    .parse()
                    .unwrap(),
            );
            crate::egress::authorize_net(Some(&caller), &method, &url.parse().unwrap(), &headers)
                .map_err(|(status, body)| {
                    let crate::XrpcBody::Json(body) = body else {
                        panic!("an XRPC error is JSON")
                    };
                    (status, serde_json::from_slice(&body).unwrap())
                })
        }
    }

    const API: &str = "https://api.example.org:8443/v1/things";

    /// Declared but ungranted is the cartridge's missing authority (naming the request once one
    /// is filed), and granting it through a runtime request lets the same request out at once,
    /// with no restart.
    #[tokio::test]
    async fn a_declared_net_request_waits_for_its_grant_and_a_runtime_request_gives_it() {
        let (node, executor, _) = hosted(networked(), &[]).await;
        let alpha_id = id(ALPHA).to_string();

        let (status, body) = node
            .ping_fetches(&executor, http::Method::GET, API)
            .await
            .unwrap_err();
        assert_eq!(status, http::StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "MissingAuthority");
        assert_eq!(
            body["operationError"],
            missing_authority(&alpha_id, "api", None)
        );

        // The routine asks, and the refusal now names its request.
        let ping = node.as_ping(&executor).await;
        let filed = node.file(ping, "api", None).await.unwrap();
        let request = filed.request.unwrap();
        let (_, body) = node
            .ping_fetches(&executor, http::Method::GET, API)
            .await
            .unwrap_err();
        assert_eq!(
            body["operationError"],
            missing_authority(&alpha_id, "api", Some(request.to_string()))
        );
        assert_eq!(
            node.pending().await[0].access,
            RequestedAccess::Net {
                host: "api.example.org".into(),
                port: Some(8443),
            }
        );

        // Allow writes `grant/net/api.example.org:8443`, and the very next request goes out. The
        // verb the permbind lists goes too, with the host in any case.
        node.answer(request, GrantAnswer::Allow, None)
            .await
            .unwrap();
        assert!(
            node.grant_datum_present(GrantKey::Net("api.example.org:8443".parse().unwrap()))
                .await
        );
        for (method, url) in [
            (http::Method::GET, API),
            (http::Method::HEAD, API),
            (http::Method::GET, "https://API.Example.ORG:8443/"),
        ] {
            node.ping_fetches(&executor, method, url).await.unwrap();
        }

        // What is granted is the grant's alone: the other permbind still waits.
        let (_, body) = node
            .ping_fetches(&executor, http::Method::GET, "https://www.example.org/")
            .await
            .unwrap_err();
        assert_eq!(body["error"], "MissingAuthority");
        node.shutdown().await;
    }

    /// With everything granted, a request that is not exactly what a permbind declares is
    /// refused as undeclared: a different host, a port other than the one a permbind names (a
    /// permbind with no port names every port), or verb, and a URL that is not a network one.
    #[tokio::test]
    async fn a_net_request_is_allowed_only_exactly_as_declared() {
        let (node, executor, _) = hosted(networked(), &[]).await;
        for host in ["api.example.org:8443", "www.example.org"] {
            node.set_grant(
                node.operator(),
                ALPHA,
                GrantKey::Net(host.parse().unwrap()),
                Some(GrantBody::present()),
            )
            .await
            .unwrap();
        }
        for (method, url) in [
            (http::Method::GET, API),
            (http::Method::GET, "https://www.example.org/anything"),
            (http::Method::POST, "https://www.example.org/anything"),
            // No port is every port, and the scheme only chooses the port a URL leaves out.
            (http::Method::GET, "http://www.example.org/"),
            (http::Method::GET, "https://www.example.org:8443/"),
            (http::Method::GET, "http://www.example.org:8080/"),
        ] {
            node.ping_fetches(&executor, method, url).await.unwrap();
        }
        for (method, url, why) in [
            (
                http::Method::GET,
                "https://other.example.org:8443/",
                "another host",
            ),
            (
                http::Method::GET,
                "https://api.example.org/",
                "the default port, not the declared one",
            ),
            (
                http::Method::GET,
                "https://api.example.org:9443/",
                "another port",
            ),
            (http::Method::POST, API, "a verb the permbind does not list"),
            (
                http::Method::GET,
                "https://sub.www.example.org/",
                "a subdomain, since there are no wildcards",
            ),
        ] {
            let (status, body) = node
                .ping_fetches(&executor, method.clone(), url)
                .await
                .expect_err(why);
            assert_eq!(
                (status, &body["error"]),
                (http::StatusCode::FORBIDDEN, &json!("Denied")),
                "{method} {url}: {why}"
            );
        }
        let (status, _) = node
            .ping_fetches(&executor, http::Method::GET, "ftp://www.example.org/")
            .await
            .unwrap_err();
        assert_eq!(status, http::StatusCode::BAD_REQUEST);
        node.shutdown().await;
    }

    /// A routine that does not take a `#net` permbind reaches no host, whatever the cartridge was
    /// granted, and a request that presents no routine's capability is refused the same way.
    #[tokio::test]
    async fn a_routine_without_a_net_use_reaches_nothing() {
        let mut cartridge = networked();
        nodule_mut(&mut cartridge)
            .routines
            .get_mut(PING)
            .unwrap()
            .uses
            .remove("api");
        let (node, executor, _) = hosted(cartridge, &[]).await;
        node.set_grant(
            node.operator(),
            ALPHA,
            GrantKey::Net("api.example.org:8443".parse().unwrap()),
            Some(GrantBody::present()),
        )
        .await
        .unwrap();
        let (_, body) = node
            .ping_fetches(&executor, http::Method::GET, API)
            .await
            .unwrap_err();
        assert_eq!(body["error"], "Denied");

        let caller = crate::egress::WorkloadCaller {
            signer: node.actor_at(&id(ALPHA)).await.capability,
            missing_authority: executor.missing_authority(),
        };
        let (status, body) = crate::egress::authorize_net(
            Some(&caller),
            &http::Method::GET,
            &API.parse().unwrap(),
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        let crate::XrpcBody::Json(body) = body else {
            panic!("an XRPC error is JSON")
        };
        assert_eq!(status, http::StatusCode::FORBIDDEN);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
            "Denied"
        );
        node.shutdown().await;
    }

    /// A grant states its own port reach, separately from the permbind's: `host` is every port
    /// and `host:port` that port only. A narrower grant than the permbind leaves the request
    /// waiting for authority, a wider one covers it, and an IPv6 literal is written in brackets.
    #[tokio::test]
    async fn a_grant_covers_a_permbind_by_host_and_port_as_deno_allow_net_does() {
        let mut cartridge = networked();
        cartridge.permbinds.insert(
            "lit".into(),
            Permbind::Net(big_cartridges_core::PermbindNet {
                host: "[2001:db8::1]".into(),
                port: Some(8000),
                methods: None,
                description: "lit description".into(),
                extra: Default::default(),
            }),
        );
        uses(&mut cartridge, PING, "lit", RoutineUse::default());
        let (node, executor, _) = hosted(cartridge, &[]).await;
        // Host and port are joined here so no literal reads as a fixed socket address to bind.
        let v6 = |host: &str, port: u16| format!("[{host}]:{port}");
        let lit = format!("http://{}/", v6("2001:db8::1", 8000));
        let lit = lit.as_str();

        // `www.example.org:443` is narrower than the `www.example.org` permbind (every port), so
        // it does not cover it; `api.example.org` (every port) covers the pinned 8443.
        for grant in [
            "www.example.org:443",
            "api.example.org",
            &v6("2001:db8::1", 8001),
        ] {
            node.set_grant(
                node.operator(),
                ALPHA,
                GrantKey::Net(grant.parse().unwrap()),
                Some(GrantBody::present()),
            )
            .await
            .unwrap();
        }
        node.ping_fetches(&executor, http::Method::GET, API)
            .await
            .unwrap();
        for url in ["https://www.example.org/", lit] {
            let (_, body) = node
                .ping_fetches(&executor, http::Method::GET, url)
                .await
                .unwrap_err();
            assert_eq!(body["error"], "MissingAuthority", "{url}");
        }

        for grant in ["www.example.org", &v6("2001:DB8:0::1", 8000)] {
            node.set_grant(
                node.operator(),
                ALPHA,
                GrantKey::Net(grant.parse().unwrap()),
                Some(GrantBody::present()),
            )
            .await
            .unwrap();
        }
        for url in ["https://www.example.org/", lit] {
            node.ping_fetches(&executor, http::Method::GET, url)
                .await
                .unwrap();
        }
        // The permbind's own port is all the literal may use.
        let (_, body) = node
            .ping_fetches(
                &executor,
                http::Method::GET,
                &format!("http://{}/", v6("2001:db8::1", 8001)),
            )
            .await
            .unwrap_err();
        assert_eq!(body["error"], "Denied");
        node.shutdown().await;
    }
}

/// Every routine is delivered its nodule's own actor identity capability (the one the egress
/// signs its calls with), the same for all of them: it is the actor's, not minted per routine,
/// so a reconcile that re-mints a routine's handles leaves it as it is. Dispatch is not
/// granted anything here, which shows it does not depend on a handle.
#[tokio::test]
async fn every_routine_is_delivered_its_nodules_actor_capability() {
    let (node, executor, _) = hosted(alpha(&[PING, SETUP], &[], &[]), &[]).await;
    let actor_of = |routine: &str| {
        executor.routine_capabilities()[routine]
            .actor
            .expect("a routine is delivered its actor capability")
    };
    let actor = actor_of(PING);
    assert_eq!(actor_of(SETUP), actor);
    let held = node
        .authority
        .client
        .call(
            RequestContext::root(),
            InspectCapability { capability: actor },
        )
        .await
        .unwrap();
    assert_eq!(
        held.subject,
        Some(node.actor_at(&id(ALPHA)).await.actor.actor_id),
        "it is bound to the nodule's own actor"
    );
    // A change of grants re-mints handles and keeps the actor's capability.
    node.set_methods(
        ALPHA,
        DispatchScope {
            authorities: ["org.example.remote".to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(actor_of(PING), actor);
    node.shutdown().await;
}

/// The node's core cartridge as the actor of ADR 052 sees it: a nodule acting as the `micros`
/// subsystem actor, whose routine uses the methods it opens actors and reads other cartridges
/// with, and which the node (not a person) grants them as `installs::core_grants` does. The
/// fixture's methods are optional requests because the nodule service's own methods are
/// registered after it boots (see `BigNode::register_late_built_ins`).
mod core_like {
    use super::*;
    use big_cabinet_core::authority::AttenuateCapability;
    use big_cabinet_core::authority::OpenActor;
    use big_cartridges_core::protocol::AwaitCartridgeGrants;
    use big_cartridges_core::protocol::CartridgesError;
    use big_cartridges_core::protocol::ListCartridgeGrants;
    use big_cartridges_core::protocol::ListCartridgeInstalls;
    use big_dispatch::ServiceProvider;
    use method_core::Method;

    const CORE: &str = example_cartridge!("core");
    const TICK: &str = "org.example.core.tick";
    const CORE_NOTES: &str = "org.example.core.notes";

    fn open_actor() -> &'static str {
        OpenActor::XRPC.nsid
    }

    fn attenuate() -> &'static str {
        AttenuateCapability::XRPC.nsid
    }

    fn list_grants() -> &'static str {
        ListCartridgeGrants::XRPC.nsid
    }

    fn await_grants() -> &'static str {
        AwaitCartridgeGrants::XRPC.nsid
    }

    fn list_installs() -> &'static str {
        ListCartridgeInstalls::XRPC.nsid
    }

    /// A cartridge whose one routine `routine` uses each of `methods`.
    fn using(
        name: &str,
        version: &str,
        routine: &str,
        methods: &[&'static str],
    ) -> FixtureCartridge {
        let mut cartridge = with_requests(
            init_cartridge(name, version, &[routine], &[], &[]),
            methods
                .iter()
                .map(|method| necessity(method, Necessity::Optional))
                .collect(),
        );
        for method in methods {
            uses(&mut cartridge, routine, method, RoutineUse::default());
        }
        cartridge
    }

    /// A node with the authority and nodule services served through dispatch as `big_node`
    /// serves them, and the core-like cartridge installed. Returns the executor's view of
    /// the core routine's handles (its last `ensure` is the core nodule's, until another is
    /// admitted).
    async fn core_node() -> (
        Node,
        Arc<FakeExecutor>,
        crate::executor::RoutineCapabilities,
    ) {
        let executor = FakeExecutor::hosting(Arc::new(Echo::default()), true);
        let node =
            crate::e2e::node_with(Some(Arc::clone(&executor) as Arc<dyn NoduleExecutor>)).await;
        ServiceProvider::default()
            .serve(&node.authority.client)
            .register(&node.dispatch.router, "builtin:authority");
        ServiceProvider::default()
            .serve(&node.nodules.client)
            .register(&node.dispatch.router, "builtin:nodules");
        let mut core = using(
            "core",
            crate::e2e::installs::CORE_VERSION,
            TICK,
            &[
                open_actor(),
                attenuate(),
                list_grants(),
                await_grants(),
                list_installs(),
            ],
        );
        // The routine reads a settings kind of its own, which is what gives it a docs handle.
        with_setting(&mut core, CORE_NOTES);
        uses(
            &mut core,
            TICK,
            CORE_NOTES,
            RoutineUse {
                read: true,
                ..Default::default()
            },
        );
        node.admit(node.operator(), core).await.unwrap();
        let held = executor.routine_capabilities_of(&id(CORE))[TICK].clone();
        (node, executor, held)
    }

    /// `alpha`, a plain nodule granted `methods` as any cartridge is, with the handles it holds.
    async fn plain_nodule(
        node: &Node,
        executor: &FakeExecutor,
        methods: &[&'static str],
    ) -> crate::executor::RoutineCapabilities {
        node.admit(node.operator(), using("alpha", "1.0.0", PING, methods))
            .await
            .unwrap();
        node.set_methods(
            ALPHA,
            DispatchScope {
                operations: methods.iter().map(|method| (*method).to_owned()).collect(),
                ..Default::default()
            },
        )
        .await;
        node.set_enabled(ALPHA, true).await;
        executor.routine_capabilities_of(&id(ALPHA))[PING].clone()
    }

    fn as_routine(held: &crate::executor::RoutineCapabilities) -> Authorization {
        Authorization {
            authority: held.dispatch.expect("the routine holds a dispatch handle"),
            signer: held
                .actor
                .expect("the routine is told its actor capability"),
        }
    }

    async fn shut_down(node: Node) {
        node.dispatch.router.unbind("builtin:authority");
        node.dispatch.router.unbind("builtin:nodules");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn a_nodule_is_told_its_actor_capability_which_opens_children_only_with_the_right_to() {
        let (node, executor, core) = core_node().await;
        let core_auth = as_routine(&core);
        let actor = core.actor.unwrap();
        let inspected = node
            .authority
            .client
            .call(
                RequestContext::root(),
                InspectCapability { capability: actor },
            )
            .await
            .unwrap();
        // It is the identity capability of the nodule's own actor, the `micros` actor, with the
        // rights it was opened with.
        assert_eq!(
            inspected.subject,
            Some(node.micros_actor_id().await),
            "the capability is the actor's own"
        );
        assert_eq!(inspected.rights, Rights::all());

        // Presented as `parent` through dispatch, signed as the same actor (ADR 037's holder
        // binding), it opens a child; and the nodule narrows its docs handle onto it with
        // `attenuateCapability { subject }`, which the child then reads the config document with.
        let child = node
            .invoke(
                core_auth,
                open_actor(),
                json!({ "parent": actor, "path": "micros/child", "rights": ["use"] }),
            )
            .await
            .unwrap();
        let child_id = child["actorId"].clone();
        assert!(child_id.is_string(), "{child}");
        let docs = core.docs.expect("the routine reads a settings kind");
        let narrowed = node
            .invoke(
                core_auth,
                attenuate(),
                json!({ "capability": docs, "rights": ["use"], "subject": child_id }),
            )
            .await
            .unwrap();
        assert_eq!(narrowed["subject"], child_id, "{narrowed}");
        assert_eq!(narrowed["rights"], json!(["use"]));
        let narrowed_cap: CapId = serde_json::from_value(narrowed["capabilityId"].clone()).unwrap();
        let child_cap: CapId = serde_json::from_value(child["capability"].clone()).unwrap();
        let config_doc = node.install_of(CORE).await.config_doc;
        node.docs
            .client
            .call_with(
                RequestContext::root(),
                Authorization {
                    authority: narrowed_cap,
                    signer: child_cap,
                }
                .metadata(),
                ReadDatums {
                    capability: narrowed_cap,
                    doc_id: config_doc,
                    at: At::Current,
                    datums: Some(BTreeSet::new()),
                    optimistic: false,
                },
            )
            .await
            .expect("the child reads with the handle narrowed onto it");
        // The handle is bound to the child alone (ADR 037 §2): the parent, whose own actor
        // capability signs the call, presents the child's narrowed handle and is refused. The
        // refusal is the holder check on the body capability, not a missing right.
        let parent_refused = node
            .docs
            .client
            .call_with(
                RequestContext::root(),
                Authorization {
                    authority: narrowed_cap,
                    signer: actor,
                }
                .metadata(),
                ReadDatums {
                    capability: narrowed_cap,
                    doc_id: node.install_of(CORE).await.config_doc,
                    at: At::Current,
                    datums: Some(BTreeSet::new()),
                    optimistic: false,
                },
            )
            .await
            .unwrap_err();
        assert!(
            format!("{parent_refused:?}").contains("Unauthorized"),
            "{parent_refused:?}"
        );
        // A path that is not one segment under the presenting actor is refused: a sibling, the
        // `nodules` subtree, an ancestor.
        for path in ["sibling", "nodules/alpha", "", "micros/child/grandchild"] {
            let refused = node
                .invoke(
                    core_auth,
                    open_actor(),
                    json!({ "parent": actor, "path": path, "rights": ["use"] }),
                )
                .await
                .unwrap_err();
            assert!(
                format!("{refused:?}").contains("InvalidActorPath"),
                "`{path}`: {refused:?}"
            );
        }

        // Every nodule is told its actor capability, but it carries the rights its actor was
        // opened with: a plain nodule's is `use` alone, which opens nothing.
        let plain = plain_nodule(&node, &executor, &[open_actor()]).await;
        assert_ne!(plain.actor, core.actor);
        let refused = node
            .invoke(
                as_routine(&plain),
                open_actor(),
                json!({ "parent": plain.actor, "path": "nodules/alpha/child", "rights": ["use"] }),
            )
            .await
            .unwrap_err();
        assert!(
            format!("{refused:?}").contains("Unauthorized"),
            "{refused:?}"
        );
        shut_down(node).await;
    }

    #[tokio::test]
    async fn a_nodule_reads_other_cartridges_through_dispatch_only_on_a_privileged_grant_of_that_method()
     {
        let (node, executor, core) = core_node().await;
        let core_auth = as_routine(&core);
        let plain = plain_nodule(&node, &executor, &[list_grants(), list_installs()]).await;
        let plain_auth = as_routine(&plain);

        // The core-like routine reads another cartridge's grants, its installs, and waits on
        // its grants, all as a nodule through dispatch.
        let grants = node
            .invoke(core_auth, list_grants(), json!({ "cartridgeId": ALPHA }))
            .await
            .unwrap();
        let listed = grants["grants"].as_array().unwrap();
        assert!(
            !listed.is_empty()
                && listed
                    .iter()
                    .all(|grant| grant["cartridgeId"] == json!(ALPHA)),
            "{grants}"
        );
        let installs = node
            .invoke(core_auth, list_installs(), json!({}))
            .await
            .unwrap();
        let installed: Vec<&str> = installs["installs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|install| install["cartridgeId"].as_str().unwrap())
            .collect();
        assert!(
            installed.contains(&ALPHA) && installed.contains(&CORE),
            "{installs}"
        );
        let awaited = node
            .invoke(
                core_auth,
                await_grants(),
                json!({ "cartridgeId": ALPHA, "holdMs": 0 }),
            )
            .await
            .unwrap();
        assert!(awaited["revision"].is_string(), "{awaited}");

        // A plain nodule granted the very same methods is refused: the grant states no
        // privilege, so its handle does not reach a privileged method, and dispatch says so.
        assert_eq!(
            node.invoke(plain_auth, list_grants(), json!({ "cartridgeId": ALPHA }))
                .await,
            Err(DispatchError::Denied)
        );
        // The installs read is no privileged method, so dispatch lets the call through and the
        // service refuses it.
        let refused = node
            .invoke(plain_auth, list_installs(), json!({}))
            .await
            .unwrap_err();
        assert!(format!("{refused:?}").contains("Denied"), "{refused:?}");
        // Dispatch is not the only guard: the service itself refuses a nodule whose cartridge
        // holds no privileged grant of the method, handed the handle directly.
        assert!(matches!(
            node.call(
                plain_auth,
                ListCartridgeGrants {
                    cartridge_id: Some(id(ALPHA)),
                },
            )
            .await,
            Err(CartridgesError::Denied)
        ));
        assert!(matches!(
            node.call(plain_auth, ListCartridgeInstalls {}).await,
            Err(CartridgesError::Denied)
        ));
        shut_down(node).await;
    }

    /// The rule is "a privileged method trusts a privileged grant for exactly that method", not
    /// "the core cartridge may read": with the grant of one read gone from what the node makes
    /// its core cartridge at boot, the core routine's handle still reads grants, and the
    /// service refuses it the installs whatever else it holds.
    #[tokio::test]
    async fn a_privileged_grant_trusts_exactly_its_method() {
        let (mut node, executor, _) = core_node().await;
        node.core_grants
            .remove(&GrantKey::Method(list_installs().to_owned()));
        node.dispatch.router.unbind("builtin:nodules");
        node.restart_nodules().await.unwrap();
        ServiceProvider::default()
            .serve(&node.nodules.client)
            .register(&node.dispatch.router, "builtin:nodules");
        let core_auth = as_routine(&executor.routine_capabilities_of(&id(CORE))[TICK]);

        node.call(
            core_auth,
            ListCartridgeGrants {
                cartridge_id: Some(id(CORE)),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            node.call(core_auth, ListCartridgeInstalls {}).await,
            Err(CartridgesError::Denied)
        ));
        shut_down(node).await;
    }
}
