//! Resolving operation requests to providers and the lockfile (ADR 028 §3–§4, plan item P5):
//! a required request without exactly one satisfying provider holds the nodule at
//! `ProviderMissing`, the resolution is recorded node-locally, and a different cartridge providing the
//! contract is re-consented. Against the fake executor, with no sleeps.

use super::Node;
use super::dispatch::not_ready;
use super::dispatch::serving_cartridge;
use super::id;
use super::init::PING;
use super::init::Probe;
use super::init::init_cartridge;
use super::init::necessity;
use super::init::node_hosting;
use super::init::with_requests;
use big_cartridges_core::Cartridge;
use big_cartridges_core::Contribution;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::request::Request;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_nodule_core::protocol::ConfirmNoduleResolution;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::providers::LockedProvider;
use big_nodule_core::providers::RequestStatus;
use big_nodule_core::providers::Unresolved;
use serde_json::json;

const ALPHA: &str = example_cartridge!("alpha");
const BETA: &str = example_cartridge!("beta");
const GAMMA: &str = example_cartridge!("gamma");
/// The operation alpha requests: beta serves it from its own Lexicons, gamma as a substitute.
const BETA_PING: &str = "org.example.beta.ping";

/// A node hosting every handler, with `requests` made by an installed but disabled alpha that
/// may call anything under `org.example.beta`.
async fn node_with_alpha(requests: Vec<Request>) -> Node {
    let node = node_hosting(&Probe::new()).await;
    node.admit(
        node.operator(),
        with_requests(
            init_cartridge("alpha", "1.0.0", &[PING], &[], &[]),
            requests,
        ),
    )
    .await
    .unwrap();
    node.grant_operations(ALPHA, &["org.example.beta"]).await;
    node
}

fn beta(version: &str) -> Cartridge {
    init_cartridge("beta", version, &[BETA_PING], &[], &[])
}

/// `cartridge` whose Lexicon for `nsid` declares `revision`.
fn with_revision(mut cartridge: Cartridge, nsid: &str, revision: u32) -> Cartridge {
    let Some(Contribution::Nodule(nodule)) = cartridge.contributions.first_mut() else {
        panic!("init cartridges carry a nodule");
    };
    let lexicons = nodule.description["lexicons"].as_array_mut().unwrap();
    let file = lexicons.iter_mut().find(|file| file["id"] == nsid).unwrap();
    file["revision"] = json!(revision);
    cartridge
}

fn locked(provider: &str, version: &str, lexicon_revision: Option<u32>) -> LockedProvider {
    LockedProvider {
        contract: BETA_PING.to_owned(),
        provider: provider.to_owned(),
        version: Some(version.parse().unwrap()),
        lexicon_revision,
    }
}

fn resolved(provider: &str, version: &str, revision: Option<u32>) -> RequestStatus {
    RequestStatus {
        contract: BETA_PING.to_owned(),
        locked: Some(locked(provider, version, revision)),
        unresolved: None,
    }
}

fn unresolved(locked: Option<LockedProvider>, reason: Unresolved) -> RequestStatus {
    RequestStatus {
        contract: BETA_PING.to_owned(),
        locked,
        unresolved: Some(reason),
    }
}

impl Node {
    pub(super) async fn record(&self, cartridge_id: &str) -> NoduleRecord {
        self.get(self.operator(), cartridge_id).await.unwrap()
    }

    async fn confirm(
        &self,
        cartridge_id: &str,
        binding: &str,
    ) -> Result<NoduleRecord, NodulesError> {
        self.call(
            self.operator(),
            ConfirmNoduleResolution {
                cartridge_id: id(cartridge_id),
                binding: binding.to_owned(),
            },
        )
        .await
    }

    /// Admits `cartridge` and enables it.
    pub(super) async fn install(&self, cartridge: Cartridge) {
        let cartridge_id = cartridge.id.to_string();
        self.admit(self.operator(), cartridge).await.unwrap();
        self.set_enabled(&cartridge_id, true).await;
    }
}

#[tokio::test]
async fn a_required_request_waits_for_its_provider_and_becomes_ready_when_one_is_enabled() {
    let node = node_with_alpha(vec![necessity(BETA_PING, Necessity::Required)]).await;
    let operator = node.operator();
    node.admit(operator, beta("1.0.0")).await.unwrap();

    // Enabling is accepted and the nodule waits: the retryable not-ready, no fault.
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(record.error, None);
    assert_eq!(
        record.requests[BETA_PING],
        unresolved(None, Unresolved::NoProvider)
    );
    assert_eq!(
        node.invoke(operator, PING, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::ProviderMissing
        )))
    );

    // Beta (which sorts after alpha) is the only change: alpha is ready in the same reconcile.
    node.set_enabled(BETA, true).await;
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.requests[BETA_PING], resolved(BETA, "1.0.0", None));
    node.invoke(operator, PING, json!({})).await.unwrap();
    node.shutdown().await;
}

#[tokio::test]
async fn an_optional_request_with_no_provider_leaves_the_nodule_ready() {
    let node = node_with_alpha(vec![necessity(BETA_PING, Necessity::Optional)]).await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    // It is still reported, so an operator sees what it would reach.
    assert_eq!(
        record.requests[BETA_PING],
        unresolved(None, Unresolved::NoProvider)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_provider_older_than_min_revision_is_unresolved_until_it_is_upgraded() {
    let mut request = necessity(BETA_PING, Necessity::Required);
    request.min_revision = Some(2);
    let node = node_with_alpha(vec![request]).await;
    node.install(with_revision(beta("1.0.0"), BETA_PING, 1))
        .await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[BETA_PING],
        unresolved(
            None,
            Unresolved::RevisionTooOld {
                provider: BETA.to_owned(),
                required: 2,
                offered: 1
            }
        )
    );

    // The same cartridge at a sufficient revision resolves and is pinned at its new version, with
    // no confirmation: same publisher, an ordinary upgrade.
    node.admit(node.operator(), with_revision(beta("1.1.0"), BETA_PING, 2))
        .await
        .unwrap();
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.requests[BETA_PING], resolved(BETA, "1.1.0", Some(2)));

    // A later upgrade that drops back below the floor holds the nodule again, and keeps the pin.
    node.admit(node.operator(), with_revision(beta("1.2.0"), BETA_PING, 1))
        .await
        .unwrap();
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[BETA_PING].locked,
        Some(locked(BETA, "1.1.0", Some(2)))
    );
    node.shutdown().await;
}

#[tokio::test]
async fn the_lockfile_is_on_the_record_durable_and_begins_afresh_when_the_nodule_is_enabled() {
    let mut node = node_with_alpha(vec![necessity(BETA_PING, Necessity::Required)]).await;
    node.install(beta("1.0.0")).await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.requests[BETA_PING], resolved(BETA, "1.0.0", None));

    // It is host-local state, not in memory only: after a restart a different cartridge providing
    // the contract is still a change from what was consented to.
    node.restart_nodules().await.unwrap();
    assert_eq!(
        node.record(ALPHA).await.requests[BETA_PING],
        resolved(BETA, "1.0.0", None)
    );
    node.set_enabled(BETA, false).await;
    node.install(serving_cartridge(GAMMA, BETA_PING)).await;
    assert_eq!(
        node.record(ALPHA).await.requests[BETA_PING].unresolved,
        Some(Unresolved::ProviderChanged {
            from: BETA.to_owned(),
            to: GAMMA.to_owned()
        })
    );

    // Disabling the nodule drops its lock: enabling it again is the consent, to gamma now.
    node.set_enabled(ALPHA, false).await;
    assert!(node.record(ALPHA).await.requests.is_empty());
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.requests[BETA_PING], resolved(GAMMA, "1.0.0", None));
    node.shutdown().await;
}

#[tokio::test]
async fn a_different_cartridge_providing_the_contract_is_reconsented() {
    let node = node_with_alpha(vec![necessity(BETA_PING, Necessity::Required)]).await;
    node.install(beta("1.0.0")).await;
    node.admit(node.operator(), serving_cartridge(GAMMA, BETA_PING))
        .await
        .unwrap();
    assert_eq!(
        node.set_enabled_record(ALPHA).await.readiness,
        Readiness::Ready
    );

    // Nothing to confirm while the pinned cartridge still provides it.
    assert!(matches!(
        node.confirm(ALPHA, BETA_PING).await,
        Err(NodulesError::Rejected(_))
    ));

    // Beta goes away and gamma provides the contract instead: held, not silently re-pinned.
    node.set_enabled(BETA, false).await;
    assert_eq!(
        node.record(ALPHA).await.requests[BETA_PING].unresolved,
        Some(Unresolved::NoProvider)
    );
    node.set_enabled(GAMMA, true).await;
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[BETA_PING],
        unresolved(
            Some(locked(BETA, "1.0.0", None)),
            Unresolved::ProviderChanged {
                from: BETA.to_owned(),
                to: GAMMA.to_owned()
            }
        )
    );

    // Confirming names a binding that is held at a change, and only that.
    assert!(matches!(
        node.confirm(ALPHA, "no-such-binding").await,
        Err(NodulesError::Rejected(_))
    ));
    assert!(matches!(
        node.confirm(example_cartridge!("absent"), BETA_PING).await,
        Err(NodulesError::UnknownCartridge(_))
    ));
    let record = node.confirm(ALPHA, BETA_PING).await.unwrap();
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(
        record.requests[BETA_PING].locked.as_ref().unwrap().provider,
        GAMMA
    );
    assert!(matches!(
        node.confirm(ALPHA, BETA_PING).await,
        Err(NodulesError::Rejected(_))
    ));
    node.shutdown().await;
}

#[tokio::test]
async fn two_providers_are_ambiguous_and_resolve_again_when_one_is_disabled() {
    let node = node_with_alpha(vec![necessity(BETA_PING, Necessity::Required)]).await;
    node.install(beta("1.0.0")).await;
    assert_eq!(
        node.set_enabled_record(ALPHA).await.readiness,
        Readiness::Ready
    );

    node.install(serving_cartridge(GAMMA, BETA_PING)).await;
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[BETA_PING],
        unresolved(
            Some(locked(BETA, "1.0.0", None)),
            Unresolved::Ambiguous {
                providers: vec![BETA.to_owned(), GAMMA.to_owned()]
            }
        )
    );

    // Dispatch agrees the operation is ambiguous, and disabling the newcomer needs no
    // confirmation: the pinned cartridge provides it again.
    node.set_enabled(GAMMA, false).await;
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(record.requests[BETA_PING], resolved(BETA, "1.0.0", None));
    node.shutdown().await;
}
