//! Method routes (ADR 045): the operator's "this method, or everything under this prefix, is
//! served by that provider", over nodules and built-ins alike. A route settles what would be
//! ambiguous, is never a fallback, and is the consent of every dependent: their resolution locks
//! move to the routed provider with no confirmation. Against the fake executor, with no sleeps.

use super::Node;
use super::dispatch::not_ready;
use super::dispatch::serving_cartridge;
use super::init::PING;
use super::init::Probe;
use super::init::init_cartridge;
use super::init::necessity;
use super::init::node_hosting;
use super::init::with_requests;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::operation::OperationKind;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::request::Necessity;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::DispatchError;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_dispatch_core::Route;
use big_nodule_core::config::MethodRoute;
use big_nodule_core::config::route_key;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::protocol::SetMethodRoute;
use big_nodule_core::providers::Unresolved;
use std::sync::Arc;

const ALPHA: &str = example_cartridge!("alpha");
const BETA: &str = example_cartridge!("beta");
const GAMMA: &str = example_cartridge!("gamma");
/// The operation alpha requests; beta and gamma (and, when registered, a built-in) all serve it.
const BETA_PING: &str = "org.example.beta.ping";
const NATIVE: &str = "native-ping";

impl Node {
    async fn route(&self, pattern: &str, provider: Option<&str>) -> Result<(), NodulesError> {
        self.call(
            self.operator(),
            SetMethodRoute {
                pattern: pattern.to_owned(),
                provider: provider.map(str::to_owned),
            },
        )
        .await
    }

    fn served_by(&self, nsid: &str) -> String {
        self.dispatch
            .router
            .resolve(nsid)
            .unwrap()
            .provider_id
            .clone()
    }

    fn locked_provider(record: &big_nodule_core::protocol::NoduleRecord) -> Option<&str> {
        record.requests[BETA_PING]
            .locked
            .as_ref()
            .map(|locked| locked.provider.as_str())
    }

    fn register_native(&self) {
        self.dispatch
            .router
            .register(
                NATIVE,
                [(
                    BETA_PING.to_owned(),
                    Operation::unchecked(Some(OperationKind::Query)),
                )],
                ProviderState::Ready(ProviderKind::BuiltIn(Probe::new() as Arc<dyn Provider>)),
            )
            .unwrap();
    }
}

/// A node with alpha, which requires `BETA_PING`, enabled and pinned to beta, then gamma enabled
/// too: both serve the operation, so it is ambiguous until a route says otherwise.
async fn ambiguous_node() -> Node {
    let node = node_hosting(&Probe::new()).await;
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
    node.install(init_cartridge("beta", "1.0.0", &[BETA_PING], &[], &[]))
        .await;
    assert_eq!(
        node.set_enabled_record(ALPHA).await.readiness,
        Readiness::Ready
    );
    node.install(serving_cartridge(GAMMA, BETA_PING)).await;
    let record = node.record(ALPHA).await;
    assert_eq!(
        record.requests[BETA_PING].unresolved,
        Some(Unresolved::Ambiguous {
            providers: vec![BETA.to_owned(), GAMMA.to_owned()]
        })
    );
    node
}

#[tokio::test]
async fn a_route_settles_an_ambiguity_and_moves_the_dependents_lock_without_a_confirmation() {
    let node = ambiguous_node().await;
    assert_eq!(
        node.invoke(node.operator(), BETA_PING, serde_json::json!({}))
            .await,
        Err(not_ready(NotReadyReason::Ambiguous))
    );

    node.route(BETA_PING, Some(GAMMA)).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), GAMMA);
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(Node::locked_provider(&record), Some(GAMMA));
    assert_eq!(record.requests[BETA_PING].unresolved, None);

    // The lock was pinned to beta before, and a plain provider change would have been held at
    // `ProviderChanged`: re-routing moves it again, still unconfirmed.
    node.route(BETA_PING, Some(BETA)).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), BETA);
    assert_eq!(Node::locked_provider(&node.record(ALPHA).await), Some(BETA));

    // Clearing the route is today's rule again: both claim it, so it is ambiguous.
    node.route(BETA_PING, None).await.unwrap();
    assert!(node.dispatch.router.routes().is_empty());
    assert_eq!(
        node.invoke(node.operator(), BETA_PING, serde_json::json!({}))
            .await,
        Err(not_ready(NotReadyReason::Ambiguous))
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_built_in_is_routed_over_a_nodule_and_the_reverse() {
    let node = ambiguous_node().await;
    node.register_native();
    node.route(BETA_PING, Some(NATIVE)).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), NATIVE);
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert_eq!(Node::locked_provider(&record), Some(NATIVE));
    assert_eq!(
        record.requests[BETA_PING].locked.as_ref().unwrap().version,
        None
    );

    node.route(BETA_PING, Some(GAMMA)).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), GAMMA);
    assert_eq!(
        Node::locked_provider(&node.record(ALPHA).await),
        Some(GAMMA)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_routed_provider_that_is_not_serving_is_not_replaced_by_another_claimant() {
    let node = ambiguous_node().await;
    node.route(BETA_PING, Some(GAMMA)).await.unwrap();
    node.set_enabled(GAMMA, false).await;

    // A disabled nodule claims nothing. Beta still claims the operation, but the route names
    // gamma: callers wait for it (retryable) and dependents are not sent to beta.
    assert_eq!(
        node.invoke(node.operator(), BETA_PING, serde_json::json!({}))
            .await,
        Err(DispatchError::NotReady {
            reason: NotReadyReason::RouteTargetMissing {
                provider: GAMMA.to_owned()
            }
        })
    );
    let record = node.record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert_eq!(
        record.requests[BETA_PING].unresolved,
        Some(Unresolved::NoProvider)
    );

    node.set_enabled(GAMMA, true).await;
    assert_eq!(node.record(ALPHA).await.readiness, Readiness::Ready);
    node.shutdown().await;
}

#[tokio::test]
async fn a_route_to_a_provider_that_does_not_claim_the_operation_is_retryable() {
    let node = ambiguous_node().await;
    node.route(BETA_PING, Some(NATIVE)).await.unwrap();
    assert_eq!(
        node.invoke(node.operator(), BETA_PING, serde_json::json!({}))
            .await,
        Err(DispatchError::NotReady {
            reason: NotReadyReason::RouteTargetMissing {
                provider: NATIVE.to_owned()
            }
        })
    );
    assert_eq!(
        node.record(ALPHA).await.requests[BETA_PING].unresolved,
        Some(Unresolved::NoProvider)
    );

    // The target registers: the same call now reaches it, and nothing else had to change.
    node.register_native();
    assert_eq!(node.served_by(BETA_PING), NATIVE);
    node.shutdown().await;
}

#[tokio::test]
async fn an_exact_route_beats_the_prefix_over_it_and_prefixes_match_whole_segments() {
    let node = ambiguous_node().await;
    node.route("org.example.beta", Some(GAMMA)).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), GAMMA);
    node.route(BETA_PING, Some(BETA)).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), BETA);
    node.route(BETA_PING, None).await.unwrap();
    assert_eq!(node.served_by(BETA_PING), GAMMA);

    // `org.example.bet` is not a segment prefix of `org.example.beta.ping`.
    node.route("org.example.beta", None).await.unwrap();
    node.route("org.example.bet", Some(GAMMA)).await.unwrap();
    assert_eq!(
        node.invoke(node.operator(), BETA_PING, serde_json::json!({}))
            .await,
        Err(not_ready(NotReadyReason::Ambiguous))
    );
    node.shutdown().await;
}

#[tokio::test]
async fn routes_persist_across_a_restart_and_a_malformed_pattern_is_refused() {
    let mut node = ambiguous_node().await;
    node.route(BETA_PING, Some(GAMMA)).await.unwrap();
    node.restart_nodules().await.unwrap();
    assert_eq!(
        node.dispatch.router.routes().get(BETA_PING),
        Some(&Route::Provider(GAMMA.to_owned()))
    );
    assert_eq!(node.served_by(BETA_PING), GAMMA);

    for pattern in ["", "a..b", "a b", "*", ".a"] {
        assert!(
            matches!(
                node.route(pattern, Some(GAMMA)).await,
                Err(NodulesError::Rejected(_))
            ),
            "`{pattern}` is refused"
        );
    }
    assert!(matches!(
        node.route(BETA_PING, Some("")).await,
        Err(NodulesError::Rejected(_))
    ));
    node.shutdown().await;
}

#[tokio::test]
async fn a_damaged_route_datum_refuses_only_what_its_pattern_covers() {
    let node = ambiguous_node().await;
    let delta = example_cartridge!("delta");
    node.install(serving_cartridge(delta, "org.example.delta.ping"))
        .await;
    assert_eq!(node.record(delta).await.readiness, Readiness::Ready);

    // Any client holding a capability on the config document can write a datum the service did
    // not validate: here a route naming an empty provider, which the datum's schema admits.
    let pattern = "org.example.beta";
    let receipt = node
        .write_config_datums(
            [(
                route_key(pattern),
                serde_json::json!({ "$type": MethodRoute::ID, "provider": "" }),
            )]
            .into(),
        )
        .await;

    // The request the pattern covers is refused with the reason, and calls to what it covers
    // are refused rather than sent to whichever provider claims them.
    let record = node.reconciled_at(&receipt, ALPHA).await;
    assert_eq!(record.readiness, Readiness::ProviderMissing);
    assert!(
        matches!(
            &record.requests[BETA_PING].unresolved,
            Some(Unresolved::RouteInvalid { reason }) if reason.contains(pattern) && reason.contains("empty")
        ),
        "{record:?}"
    );
    assert!(matches!(
        node.invoke(node.operator(), BETA_PING, serde_json::json!({}))
            .await,
        Err(DispatchError::NotReady {
            reason: NotReadyReason::RouteInvalid { .. }
        })
    ));

    // Everything else keeps reconciling.
    let unrelated = node.reconciled_at(&receipt, delta).await;
    assert_eq!(unrelated.readiness, Readiness::Ready);
    assert_eq!(unrelated.error, None);

    // Clearing the datum through the service repairs it.
    node.route(pattern, None).await.unwrap();
    assert!(matches!(
        node.record(ALPHA).await.requests[BETA_PING].unresolved,
        Some(Unresolved::Ambiguous { .. })
    ));
    node.shutdown().await;
}
