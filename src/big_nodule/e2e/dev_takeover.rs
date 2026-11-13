//! The dev takeover hook (ADR 045 §5): `fork` is run *as* `beta`, so beta's slot keeps its actor,
//! grants, lockfile and dispatch registrations while the fork's code and manifest run in it. It
//! is explicit and gated by `dev.allow_slot_takeover`. Against the fake executor, with no sleeps.

use super::Node;
use super::executor::FakeExecutor;
use super::id;
use super::init::PING;
use super::init::Probe;
use super::init::init_cartridge;
use super::init::necessity;
use super::init::with_requests;
use big_cabinet_core::datum::operation::OperationKind;
use big_cartridges_core::Digest;
use big_cartridges_core::config::Component;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::test_support::FixtureCartridge;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use big_nodule_core::inventory::FilledBy;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::protocol::SetNoduleDevTakeover;
use big_nodule_core::providers::LockedProvider;
use big_nodule_core::providers::RequestStatus;
use std::sync::Arc;

const ALPHA: &str = example_cartridge!("alpha");
const BETA: &str = example_cartridge!("beta");
const FORK: &str = example_cartridge!("fork");
const GAMMA: &str = example_cartridge!("gamma");
const BETA_PING: &str = "org.example.beta.ping";
/// Operations beta requests (and a native provider serves): the one it and the fork both ask for,
/// and one only the fork asks for.
const SYNC: &str = "org.example.remote.sync";
const MORE: &str = "org.example.remote.more";

fn beta(version: &str) -> FixtureCartridge {
    with_requests(
        init_cartridge("beta", version, &[BETA_PING], &[], &[]),
        vec![necessity(SYNC, Necessity::Required)],
    )
}

/// A fork of beta: its own cartridge id but beta's authority, serving beta's operation, requesting
/// `requests` (as required).
fn fork(version: &str, requests: &[&str]) -> FixtureCartridge {
    let mut cartridge = with_requests(
        init_cartridge("fork", version, &[BETA_PING], &[], &[]),
        requests
            .iter()
            .map(|nsid| necessity(nsid, Necessity::Required))
            .collect(),
    );
    cartridge.authority = Some("org.example.beta".into());
    cartridge
}

fn host(cartridge_id: &str, bytes: &str) -> String {
    format!("host {cartridge_id} {}", Digest::of(bytes.as_bytes()))
}

impl Node {
    async fn take_over(&self, slot: &str, by: Option<&str>) -> Result<NoduleRecord, NodulesError> {
        self.call(
            self.operator(),
            SetNoduleDevTakeover {
                cartridge_id: id(slot),
                by: by.map(id),
            },
        )
        .await
    }
}

/// A node hosting every handler, with native providers of the operations beta and the fork
/// request, beta enabled and granted everything under `org.example.remote`, and the fork
/// installed beside it, disabled.
async fn node_with_beta_and(fork: FixtureCartridge) -> (Node, Arc<FakeExecutor>) {
    let executor = FakeExecutor::hosting(Probe::new(), true);
    let node = super::node_with_takeover(Some(Arc::clone(&executor) as _), true).await;
    node.dispatch
        .router
        .register(
            "native-remote",
            [SYNC, MORE].map(|nsid| {
                (
                    nsid.to_owned(),
                    Operation::unchecked(Some(OperationKind::Query)),
                )
            }),
            ProviderState::Ready(ProviderKind::BuiltIn(Probe::new() as Arc<dyn Provider>)),
        )
        .unwrap();
    node.admit(node.operator(), beta("1.0.0")).await.unwrap();
    node.grant_operations(BETA, &["org.example.remote"]).await;
    node.admit(node.operator(), fork).await.unwrap();
    let record = node.set_enabled_record(BETA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    (node, executor)
}

#[tokio::test]
async fn the_slot_runs_the_dev_builds_code_and_reports_who_fills_it() {
    let (node, executor) = node_with_beta_and(fork("1.5.0", &[SYNC])).await;
    let before = node.record(BETA).await;

    let record = node.take_over(BETA, Some(FORK)).await.unwrap();
    assert_eq!(record.cartridge_id, id(BETA));
    assert_eq!(
        record.filled_by,
        Some(FilledBy {
            cartridge_id: id(FORK),
            version: "1.5.0".parse().unwrap()
        })
    );
    assert_eq!(record.version, Some("1.5.0".parse().unwrap()));
    assert_ne!(record.revision, before.revision);
    // Enablement and the slot's durable actor survive; dispatch routes the slot, not the fork.
    assert!(record.enabled);
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.actor, before.actor);
    assert_eq!(
        node.dispatch.router.resolve(BETA_PING).unwrap().provider_id,
        BETA
    );
    // The executor hosted beta's own handler, then retired it and hosted the fork's in its slot.
    assert_eq!(
        executor.log(),
        [
            host(BETA, "beta-1.0.0"),
            format!("retire {BETA}"),
            host(BETA, "fork-1.5.0")
        ]
    );
    // The fork's own slot is just an installed cartridge.
    let own = node.record(FORK).await;
    assert_eq!((own.filled_by, own.readiness), (None, Readiness::Disabled));
    node.shutdown().await;
}

#[tokio::test]
async fn dependents_of_the_slot_stay_satisfied_with_the_dev_builds_version() {
    let (node, _executor) = node_with_beta_and(fork("1.5.0", &[SYNC])).await;
    node.admit(
        node.operator(),
        with_requests(
            init_cartridge("alpha", "1.0.0", &[PING], &[], &[]),
            vec![necessity(BETA_PING, Necessity::Required)],
        ),
    )
    .await
    .unwrap();
    node.grant_operations(ALPHA, &["org.example.beta"]).await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);

    // Dependents name the slot, which the takeover still offers under its own id with the dev
    // build's version: no provider change to confirm, only the version moves, as for an upgrade.
    node.take_over(BETA, Some(FORK)).await.unwrap();
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        record.requests[BETA_PING],
        RequestStatus {
            contract: BETA_PING.to_owned(),
            locked: Some(LockedProvider {
                contract: BETA_PING.to_owned(),
                provider: BETA.to_owned(),
                version: Some("1.5.0".parse().unwrap()),
                lexicon_revision: None,
            }),
            unresolved: None,
        }
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_identical_request_keeps_its_grant_and_a_new_one_is_ungranted_until_configured() {
    // The fork asks for what beta asked for and one thing more, both covered by beta's
    // `org.example.remote` prefix grant: the prefix must not silently cover the addition.
    let (node, _executor) = node_with_beta_and(fork("1.5.0", &[SYNC, MORE])).await;
    let record = node.take_over(BETA, Some(FORK)).await.unwrap();
    assert_eq!(record.readiness, Readiness::NeedsConfiguration);
    let operations = record.methods.unwrap();
    assert_eq!(operations.desired.operations, [SYNC.to_owned()].into());
    assert!(record.enabled);

    // A prefix grant is consent given for beta's own code: granting it again still does not
    // stretch to the fork's addition.
    let record = node.grant_operations(BETA, &["org.example.remote"]).await;
    assert_eq!(record.readiness, Readiness::NeedsConfiguration);
    // Naming the added operation is the consent, and makes the slot ready.
    let record = node
        .set_methods(
            BETA,
            DispatchScope {
                operations: [MORE.to_owned()].into(),
                authorities: ["org.example.remote".to_owned()].into(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        record.methods.unwrap().desired.operations,
        [SYNC.to_owned(), MORE.to_owned()].into()
    );

    // A fork that asks for no more than beta did simply carries the grant over.
    let (node2, _executor2) = node_with_beta_and(fork("1.5.0", &[SYNC])).await;
    let record = node2.take_over(BETA, Some(FORK)).await.unwrap();
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        record.methods.unwrap().desired.operations,
        [SYNC.to_owned()].into()
    );
    node2.shutdown().await;
    node.shutdown().await;
}

#[tokio::test]
async fn clearing_the_takeover_reinstalls_the_slots_own_cartridge() {
    let (node, executor) = node_with_beta_and(fork("1.5.0", &[SYNC])).await;
    let before = node.record(BETA).await;
    node.take_over(BETA, Some(FORK)).await.unwrap();

    let record = node.take_over(BETA, None).await.unwrap();
    // The stored grants were never rewritten, so beta gets back exactly what it was granted.
    assert_eq!(record.methods, before.methods);
    assert_eq!(record.filled_by, None);
    assert_eq!(record.version, before.version);
    assert_eq!(record.revision, before.revision);
    assert_eq!(record.actor, before.actor);
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        executor.log().last(),
        Some(&host(BETA, "beta-1.0.0")),
        "beta's own handler runs again"
    );
    // Clearing what is not set is a no-op.
    node.take_over(BETA, None).await.unwrap();
    node.shutdown().await;
}

#[tokio::test]
async fn a_dev_build_enabled_on_its_own_cannot_take_over_a_slot_and_the_reverse() {
    let (node, _executor) = node_with_beta_and(fork("1.5.0", &[SYNC])).await;
    let operator = node.operator();
    let enable = |cartridge_id: &str| {
        node.call(
            operator,
            SetCartridgeEnabled {
                cartridge_id: id(cartridge_id),
                component: Component::Nodule,
                enabled: true,
            },
        )
    };

    // Beta holds the prefix, so the fork cannot be enabled beside it at all.
    let refused = enable(FORK).await.unwrap_err().to_string();
    assert!(refused.contains("overlaps"), "{refused}");

    // With beta disabled the fork runs on its own; it then cannot also fill beta's slot.
    node.set_enabled(BETA, false).await;
    enable(FORK).await.unwrap();
    let refused = node
        .take_over(BETA, Some(FORK))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("enabled on its own"), "{refused}");

    // And the other way round: once it fills the slot it cannot be enabled on its own.
    node.set_enabled(FORK, false).await;
    node.take_over(BETA, Some(FORK)).await.unwrap();
    let refused = enable(FORK).await.unwrap_err().to_string();
    assert!(refused.contains("cannot also be enabled"), "{refused}");
    node.shutdown().await;
}

#[tokio::test]
async fn a_dev_build_holding_another_authority_is_refused() {
    let (node, _executor) = node_with_beta_and(fork("1.5.0", &[SYNC])).await;
    // Gamma holds `org.example.gamma`: a different prefix is a different slot, not a drop-in.
    node.admit(
        node.operator(),
        init_cartridge("gamma", "1.0.0", &[BETA_PING], &[], &[]),
    )
    .await
    .unwrap();
    let refused = node
        .take_over(BETA, Some(GAMMA))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("holds authority"), "{refused}");
    let refused = node
        .take_over(BETA, Some(example_cartridge!("absent")))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("not installed"), "{refused}");
    assert_eq!(node.record(BETA).await.filled_by, None);
    node.shutdown().await;
}

#[tokio::test]
async fn a_node_that_did_not_opt_in_refuses_a_takeover_and_does_not_honor_a_stored_one() {
    let mut node = node_with_beta_and(fork("1.5.0", &[SYNC])).await.0;
    node.take_over(BETA, Some(FORK)).await.unwrap();
    node.restart_with_takeover(false).await;

    // The datum is still in the config document but the node did not opt in: the slot is
    // refused rather than run as the dev build, and it says why.
    let record = node.record(BETA).await;
    assert_eq!(record.readiness, Readiness::Refused);
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("takeover is off")),
        "{record:?}"
    );

    // Setting one is refused outright, naming the switch; clearing is how the slot recovers.
    let refused = node
        .take_over(BETA, Some(FORK))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("dev.allow_slot_takeover"), "{refused}");
    let record = node.take_over(BETA, None).await.unwrap();
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.filled_by, None);
    node.shutdown().await;
}
