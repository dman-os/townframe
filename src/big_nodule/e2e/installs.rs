//! Installs: the config document an install creates and a reinstall reuses, upgrades that fail
//! admission, and the node's core cartridge, whose install only the node makes (ADR 053).

use super::*;

/// The cartridge these nodes treat as their core cartridge: a fixture id, so a test can publish
/// versions of it as the example namespace. (A real node's is under its own publisher DID and
/// is installed by `big_node` at boot.)
pub(super) fn core_cartridge_id() -> CartridgeId {
    big_cartridges_core::test_support::cartridge_id("core")
}

/// The version these nodes' builds carry of [`core_cartridge_id`].
pub(super) const CORE_VERSION: &str = "1.0.0";

/// The core cartridge's install is the node's (ADR 053 §3 item 5): only the version the node's
/// build carries installs, so a person cannot move it to another, and once installed it cannot be
/// uninstalled, disabled, granted or cleared, by the operator either.
#[tokio::test]
async fn the_core_cartridge_installs_only_at_its_build_version_and_its_install_cannot_change() {
    let mut node = node().await;
    let operator = node.operator();
    let core = core_cartridge_id();
    let version = |label: &str| {
        let mut cartridge =
            FixtureCartridge::new(Cartridge::new(core.clone(), label.parse().unwrap()));
        cartridge.nodule = Some(Default::default());
        cartridge
    };
    let refused = CartridgesError::CoreCartridge(core.clone());

    let installed = node
        .install_cartridge(operator, version(CORE_VERSION))
        .await
        .unwrap();
    // Nobody can enable the core cartridge, so its install enables its nodule.
    assert!(
        installed
            .install
            .nodule
            .is_some_and(|nodule| nodule.enabled)
    );
    // Its nodule acts as the micros subsystem actor, not as `nodules/<cartridge segment>`, and
    // still does when the service recovers its persisted actors after a restart.
    let micros = Some(node.micros_actor_id().await);
    for restarted in [false, true] {
        if restarted {
            node.restart_nodules().await.unwrap();
        }
        assert_eq!(
            node.get(operator, &core.to_string())
                .await
                .unwrap()
                .actor
                .map(|actor| actor.actor_id),
            micros
        );
    }
    // Installing the build's version again is the node's idempotent boot step.
    node.install_cartridge(operator, version(CORE_VERSION))
        .await
        .unwrap();
    assert_eq!(
        node.install_cartridge(operator, version("1.0.1"))
            .await
            .map(|_| ()),
        Err(refused.clone())
    );
    assert_eq!(
        node.call(
            operator,
            SetCartridgeEnabled {
                cartridge_id: core.clone(),
                component: Component::Nodule,
                enabled: false,
            },
        )
        .await
        .map(|_| ()),
        Err(refused.clone())
    );
    assert_eq!(
        node.set_grant(
            operator,
            &core.to_string(),
            GrantKey::Drawer(node.drawers.local().present_id.clone()),
            None
        )
        .await,
        Err(refused.clone())
    );
    assert_eq!(
        node.call(
            operator,
            ClearCartridgeConfig {
                cartridge_id: core.clone(),
            }
        )
        .await,
        Err(refused.clone())
    );
    assert_eq!(
        node.call(
            operator,
            UninstallCartridge {
                cartridge_id: core.clone(),
            }
        )
        .await,
        Err(refused)
    );
    let installs = node
        .call(operator, ListCartridgeInstalls {})
        .await
        .unwrap()
        .installs;
    assert_eq!(
        installs
            .iter()
            .map(|record| &record.cartridge_id)
            .collect::<Vec<_>>(),
        vec![&core]
    );
    node.shutdown().await;
}

#[tokio::test]
async fn install_creates_the_cartridge_config_document_and_reinstall_reuses_it_with_its_settings() {
    let mut node = node().await;
    let operator = node.operator();
    let photos = example_cartridge!("photos");
    node.admit(operator, cartridge(photos, "1.0.0", true))
        .await
        .unwrap();
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();

    // The install names a document in the shared config drawer that is the cartridge's own: it
    // says whose it is, and another cartridge has another.
    let installed = node.install_of(photos).await;
    let schema_doc = node
        .install_of(example_cartridge!("schema"))
        .await
        .config_doc;
    assert_ne!(installed.config_doc, schema_doc);
    let doc = node.cartridge_doc(photos).await;
    let DatumRead::Accepted { value, .. } =
        &doc.datums[&big_cartridges_core::config::config_identity_key()]
    else {
        panic!("the document names its cartridge");
    };
    assert_eq!(value["cartridge"], json!(photos));
    assert!(
        installed.nodule.is_some() && installed.micro.is_none(),
        "{installed:?}"
    );

    // Settings are ordinary datums in the document, written once the nodule's Lexicon is
    // registered (when it is enabled), and a grant is a datum of its own beside them.
    node.call(
        operator,
        SetCartridgeEnabled {
            cartridge_id: id(photos),
            component: Component::Nodule,
            enabled: true,
        },
    )
    .await
    .unwrap();
    node.write_cartridge_datums(
        photos,
        BTreeMap::from([(photos_settings_key(), photos_settings(false))]),
    )
    .await;
    let grant = GrantKey::Method("org.example.remote.ping".to_owned());
    node.set_grant(operator, photos, grant.clone(), Some(GrantBody::present()))
        .await
        .unwrap();

    // Uninstalling keeps the document, so a reinstall (this node's, or a sister's) finds the
    // settings and the grant where they were left, in the same document.
    node.call(
        operator,
        UninstallCartridge {
            cartridge_id: id(photos),
        },
    )
    .await
    .unwrap();
    assert!(
        node.list(operator)
            .await
            .unwrap()
            .contains(&id(example_cartridge!("schema")))
    );
    let left = node.read_doc(installed.config_doc.clone()).await;
    assert!(left.datums.contains_key(&photos_settings_key()));
    assert!(left.datums.contains_key(&grant.datum_key()));

    node.restart_nodules().await.unwrap();
    node.admit(operator, cartridge(photos, "1.0.0", true))
        .await
        .unwrap();
    let reinstalled = node.install_of(photos).await;
    assert_eq!(
        reinstalled.config_doc, installed.config_doc,
        "the document is reused"
    );
    let doc = node.cartridge_doc(photos).await;
    let DatumRead::Accepted { value, .. } = &doc.datums[&photos_settings_key()] else {
        panic!("the settings survived the uninstall");
    };
    assert_eq!(value["thumbnails"], json!(false));
    assert!(doc.datums.contains_key(&grant.datum_key()));
    // The reinstall starts disabled, as any install does: enablement is per node and per install.
    assert!(!node.get(operator, photos).await.unwrap().enabled);
    node.shutdown().await;
}

#[tokio::test]
async fn an_upgrade_that_fails_admission_is_rejected_and_a_good_one_keeps_actor_and_grants() {
    let node = node().await;
    let operator = node.operator();
    let present = node.drawers.local().present_id.clone();
    let installed = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "1.0.0", true),
        )
        .await
        .unwrap();
    let v1 = node
        .set_drawers(
            example_cartridge!("photos"),
            BTreeMap::from([(present.clone(), DrawerAccess::Write)]),
        )
        .await;
    assert_eq!(v1.error, None);
    assert!(v1.drawers[&present].granted);
    assert_eq!(v1.actor, installed.actor);

    // A version admission refuses is never pinned: the install keeps running 1.0.0.
    assert!(matches!(
        node.admit(
            operator,
            cartridge_with_undeclared_settings(example_cartridge!("photos"), "2.0.0"),
        )
        .await,
        Err(CartridgesError::Rejected(reason)) if reason.contains("org.example.photos.missing")
    ));
    assert_eq!(
        node.get(operator, example_cartridge!("photos"))
            .await
            .unwrap(),
        v1
    );

    // A good upgrade keeps the durable actor and the grants, which live in the config document.
    let upgraded = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "2.1.0", true),
        )
        .await
        .unwrap();
    assert_eq!(upgraded.version, Some("2.1.0".parse().unwrap()));
    assert_eq!(upgraded.actor, v1.actor, "the durable actor is kept");
    assert_eq!(upgraded.error, None);
    assert!(upgraded.drawers[&present].granted);
    node.shutdown().await;
}
