//! The micros nodule of the core cartridge (ADR 052), embedded in the node and driven through
//! dispatch the way the GUI drives it: `micros.handles` and `micros.awaitHandles`, as the operator,
//! for a cartridge that ships a micro whose `uses` reach a granted drawer.

use super::nodule_wasm::boot_node;
use super::nodule_wasm::invoke;
use super::*;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cartridges_core::Cartridge;
use big_cartridges_core::MicroContribution;
use big_cartridges_core::Necessity;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindDatum;
use big_cartridges_core::PermbindDrawers;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::test_support::FixtureCartridge;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Invoke;
use big_dispatch_core::MintDispatchCapability;
use method_core::Method;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;

const GALLERY: &str = example_cartridge!("gallery");

/// A cartridge with a micro whose `uses` read `NOTE` datums in the drawers it is granted.
fn gallery() -> FixtureCartridge {
    let mut cartridge = FixtureCartridge::new(Cartridge::new(
        GALLERY.parse().unwrap(),
        "1.0.0".parse().unwrap(),
    ));
    cartridge.with_wasm("micro", b"micro component");
    cartridge.permbinds.insert(
        "drawers".into(),
        Permbind::Drawers(PermbindDrawers {
            description: "the drawers the gallery shows".into(),
            resolution: Default::default(),
            read: Some(Necessity::Required),
            write: None,
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );
    cartridge.permbinds.insert(
        "notes".into(),
        Permbind::Datum(PermbindDatum {
            nsid: NOTE.parse().unwrap(),
            description: "notes".into(),
            read: Some(Necessity::Required),
            write: None,
            create: None,
            delete: None,
            extra: Default::default(),
        }),
    );
    cartridge.micro = Some(MicroContribution {
        component: "micro".into(),
        schema: "org.example.clay.micro.manifest".into(),
        description: Default::default(),
        uses: ["drawers", "notes"]
            .into_iter()
            .map(|key| {
                (
                    key.to_owned(),
                    RoutineUse {
                        read: true,
                        ..Default::default()
                    },
                )
            })
            .collect(),
        extra: Default::default(),
    });
    cartridge
}

fn operator(node: &BigNode) -> Authorization {
    Authorization::identity(node.authority.root)
}

async fn install_gallery(node: &BigNode) {
    let pin = node.publish_fixture(&gallery()).await;
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            InstallCartridge { cartridge: pin },
        )
        .await
        .unwrap();
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            SetCartridgeEnabled {
                cartridge_id: GALLERY.parse().unwrap(),
                component: Component::Micro,
                enabled: true,
            },
        )
        .await
        .unwrap();
}

async fn grant_drawer(node: &BigNode, access: Option<DrawerAccess>) {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            SetCartridgeGrant {
                cartridge_id: GALLERY.parse().unwrap(),
                grant: GrantKey::Drawer(node.drawers.local().present_id.clone()),
                value: access.map(|access| GrantBody::access(access.into())),
            },
        )
        .await
        .unwrap();
}

fn handles_nsid() -> &'static str {
    <micros_nodule::Handles as Method>::XRPC.nsid
}

fn await_nsid() -> &'static str {
    <micros_nodule::AwaitHandles as Method>::XRPC.nsid
}

async fn handles(node: &BigNode) -> Result<Value, DispatchError> {
    invoke(
        node,
        handles_nsid(),
        json!({ "cartridgeId": GALLERY }),
        Default::default(),
    )
    .await
}

fn cap(value: &Value) -> CapId {
    serde_json::from_value(value.clone()).expect("a capability id")
}

async fn subject_of(node: &BigNode, capability: CapId) -> Option<big_cabinet_core::dmeta::Uuid7> {
    node.authority
        .client
        .call(RequestContext::root(), InspectCapability { capability })
        .await
        .map(|inspected| inspected.subject)
        .unwrap()
}

#[tokio::test]
async fn the_micros_nodule_mints_a_micro_its_handles_and_reminted_ones_when_a_grant_moves() {
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    install_gallery(&node).await;
    grant_drawer(&node, Some(DrawerAccess::Read)).await;

    // The micro's actor lives at `micros/<cartridge>/<micro>`, its handles are bound to it, and
    // its config carries the document capability.
    let first = handles(&node)
        .await
        .expect("the operator reads the handles");
    let micros = first["micros"].as_array().unwrap();
    assert_eq!(micros.len(), 1, "{first}");
    let micro = &micros[0];
    assert_eq!(micro["micro"], format!("{GALLERY}/micro/main"));
    let actor = subject_of(&node, cap(&micro["actor"])).await.unwrap();
    let resolved = node
        .authority
        .client
        .call(
            RequestContext::root(),
            big_cabinet_core::authority::ResolveActor { actor_id: actor },
        )
        .await
        .unwrap();
    assert!(
        resolved.url.contains("micros/") && resolved.url.matches('/').count() >= 2,
        "the micro's actor is two segments below `micros`: {}",
        resolved.url
    );
    let docs = cap(&micro["handles"]["docs"]);
    for handle in [cap(&micro["handles"]["dispatch"]), docs] {
        assert_eq!(subject_of(&node, handle).await, Some(actor));
    }
    assert_eq!(micro["config"]["docCap"], micro["handles"]["docs"]);

    // Asking again returns the same live handles and mints nothing.
    assert_eq!(handles(&node).await.unwrap(), first);

    // Only the operator may ask.
    let bystander = super::cartridges::bystander(&node).await;
    let bystander_actor = subject_of(&node, bystander.authority).await.unwrap();
    let nsids = [handles_nsid(), await_nsid()];
    let granted = node
        .dispatch
        .client
        .call_with(
            RequestContext::root(),
            operator(&node).metadata(),
            MintDispatchCapability {
                capability: None,
                scope: DispatchScope {
                    operations: nsids.iter().map(|nsid| (*nsid).to_owned()).collect(),
                    ..Default::default()
                },
                rights: Rights::USE,
                subject: Some(bystander_actor),
            },
        )
        .await
        .unwrap()
        .capability_id;
    let refused = node
        .dispatch
        .client
        .call_with(
            RequestContext::root(),
            Authorization {
                authority: granted,
                signer: bystander.signer,
            }
            .metadata(),
            Invoke::json(
                handles_nsid(),
                json!({ "cartridgeId": GALLERY }),
                Default::default(),
            ),
        )
        .await
        .unwrap_err();
    assert!(format!("{refused:?}").contains("Denied"), "{refused:?}");

    // A handle presented under another actor is refused, which is what binds it to the micro.
    let doc_id = DocId::from(node.drawers.local().present_id.clone());
    let read = |capability| ReadDatums {
        capability,
        doc_id: doc_id.clone(),
        at: At::Current,
        datums: Some(BTreeSet::new()),
        optimistic: false,
    };
    let under_other = node
        .docs
        .client
        .call_with(RequestContext::root(), bystander.metadata(), read(docs))
        .await;
    assert!(
        matches!(
            under_other,
            Err(ClientError::Method(
                big_cabinet_core::doc::DocsError::Denied
            ))
        ),
        "{under_other:?}"
    );

    // A grant change moves the revision: `awaitHandles` holds on the grant feed and answers
    // re-minted handles, and the old ones stop working. Revoking the drawer takes the micro's
    // docs handle away (a change of access level alone moves no scope, as the micro only reads,
    // so its handles are kept).
    let revision = first["revision"].as_str().unwrap().to_owned();
    let awaiting = invoke(
        &node,
        await_nsid(),
        json!({ "since": { GALLERY: revision }, "holdMs": 10_000 }),
        Default::default(),
    );
    let changing = async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        grant_drawer(&node, None).await;
    };
    let (moved, ()) = tokio::join!(awaiting, changing);
    let moved = moved.expect("the operator awaits the handles");
    let remade = &moved["cartridges"][GALLERY];
    assert_ne!(remade["revision"], first["revision"], "{moved}");
    let micro = &remade["micros"][0];
    assert!(micro["handles"].get("docs").is_none(), "{moved}");
    assert_ne!(
        micro["handles"]["dispatch"],
        first["micros"][0]["handles"]["dispatch"]
    );
    for old in [docs, cap(&first["micros"][0]["handles"]["dispatch"])] {
        assert!(
            node.authority
                .client
                .call(
                    RequestContext::root(),
                    InspectCapability { capability: old },
                )
                .await
                .is_err(),
            "an old handle is retracted"
        );
    }
    assert_eq!(
        subject_of(&node, cap(&micro["handles"]["dispatch"])).await,
        Some(actor)
    );
    // What the host reads next is the new revision's handles.
    assert_eq!(handles(&node).await.unwrap(), *remade);

    shutdown(node, stop).await;
}
