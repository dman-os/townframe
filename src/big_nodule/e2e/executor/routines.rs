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
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::CreateMetaDoc;
use big_cartridges_core::Initializer;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindDatum;
use big_cartridges_core::PermbindDrawers;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::RunMode;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::test_support::FixtureCartridge;
use big_dispatch::Operation;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use serde_json::json;
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
        cartridge.with_lexicon(lexicon(json!({
            "lexicon": 1,
            "id": kind,
            "defs": { "main": { "type": "object", "properties": {} } }
        })));
        cartridge.settings.insert(kind.parse().unwrap());
        cartridge.permbinds.insert(
            (*kind).to_owned(),
            Permbind::Datum(PermbindDatum {
                nsid: kind.parse().unwrap(),
                description: (*kind).to_owned(),
                read: Some(Necessity::Optional),
                write: Some(Necessity::Optional),
                create: None,
                delete: None,
                extra: Default::default(),
            }),
        );
    }
    cartridge
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
        .config
        .expect("ping writes notes");
    let setup = executor.routine_capabilities()[SETUP]
        .config
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

    // Revoking the rest leaves the routine with no handle at all.
    node.grant_operations(ALPHA, &[]).await;
    assert!(executor.routine_capabilities()[PING].dispatch.is_none());
    assert!(!node.is_live(new).await);
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
            .map(|(routine, held)| (routine, [held.dispatch, held.config, held.drawers]))
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
    // The service revokes its right to create drawers once it has made the shared config
    // drawer, so a create use can only be minted while the right is held again.
    node.drawer_access
        .grants()
        .grant(
            &RequestContext::root(),
            node.nodules_actor.actor.actor_id,
            CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let present = node.drawers.local().present_id.clone();
    crate::e2e::grant_drawer(
        &node.drawers,
        &node.drawers_actor,
        &present,
        node.nodules_actor.actor.actor_id,
        DrawerAccess::Admin,
    )
    .await;
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

    node.drawers
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
        .expect("a create use mints the right to create drawers");

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
