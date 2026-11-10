//! `test_nodule_handlers`' echo and relay served natively, against the assertions of the wasm
//! e2e (`src/big_node/e2e/nodule_wasm.rs`): the same contract, observed through the same real
//! node, with the native executor in place of the wasm one.

use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::Artifact;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Contribution;
use big_cartridges_core::Digest;
use big_cartridges_core::NoduleContribution;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::request::Request;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Readiness;
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use service_utils_rs::TraceContext;
use test_nodule_handlers::Fixture;

use super::TestNode;

const OUTER_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// One nodule running the fixture under `authority`.
struct Spec {
    cartridge: CartridgeId,
    authority: &'static str,
}

impl Spec {
    fn echo(&self) -> String {
        format!("{}.echo", self.authority)
    }

    fn relay(&self) -> String {
        format!("{}.relay", self.authority)
    }
}

fn caller() -> Spec {
    Spec {
        cartridge: big_cartridges_core::example_cartridge!("native-test")
            .parse()
            .unwrap(),
        authority: "org.example.test",
    }
}

fn callee() -> Spec {
    Spec {
        cartridge: big_cartridges_core::example_cartridge!("native-callee")
            .parse()
            .unwrap(),
        authority: "org.example.callee",
    }
}

fn procedure(nsid: &str) -> Value {
    json!({
        "lexicon": 1,
        "id": nsid,
        "defs": { "main": {
            "type": "procedure",
            "input": {
                "encoding": "application/json",
                "schema": { "type": "object", "properties": {} }
            },
            "output": {
                "encoding": "application/json",
                "schema": { "type": "object", "properties": {} }
            }
        }}
    })
}

/// The cartridge declaring `spec`'s echo and relay, with a handler artifact whose digest is a
/// placeholder (its bytes are never put), requesting each of `calls`.
fn cartridge(spec: &Spec, calls: &[String]) -> Cartridge {
    let mut nodule = Nodule {
        requires: calls
            .iter()
            .map(|nsid| {
                let mut request = Request::new(nsid, nsid);
                request.necessity = Necessity::Optional;
                request
            })
            .collect(),
        ..Default::default()
    };
    let operations = [spec.echo(), spec.relay()];
    for nsid in &operations {
        nodule
            .lexicons
            .push(serde_json::from_value(procedure(nsid)).unwrap());
    }
    nodule.handler = Some(Handler {
        artifact: "handler".into(),
        extra: Default::default(),
    });
    nodule.implements = operations.into();
    let mut cartridge = Cartridge::new(spec.cartridge.clone(), "1.0.0".parse().unwrap());
    cartridge.authority = Some(spec.authority.to_owned());
    cartridge.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest::of(b"placeholder: the native executor never reads the handler"),
        size: Some(1),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    cartridge
        .contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: serde_json::from_value(serde_json::to_value(nodule).unwrap()).unwrap(),
            extra: Default::default(),
        }));
    cartridge
}

/// The error of [`CallRelay`], shaped as the fixture's own (serde matches by variant name).
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum RelayError {
    Context(method_core::ContextError),
    InvalidRequest(String),
    UpstreamFailed(String),
}

impl std::fmt::Display for RelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(error) => error.fmt(f),
            Self::InvalidRequest(reason) => write!(f, "invalid request: {reason}"),
            Self::UpstreamFailed(reason) => write!(f, "upstream failed: {reason}"),
        }
    }
}

impl From<method_core::ContextError> for RelayError {
    fn from(error: method_core::ContextError) -> Self {
        Self::Context(error)
    }
}

/// The fixture's relay, declared as a typed method under the caller's authority.
#[derive(serde::Serialize, serde::Deserialize)]
struct CallRelay {
    operation: String,
    authority: Option<String>,
    input: Option<Value>,
}

method_core::methods! {
    error = RelayError, nsid_prefix = "org.example.test.";
    CallRelay => Value, procedure, "relay";
}

/// A traceparent in the outer call's trace (same trace id) under a span of its own.
fn assert_continues_trace(traceparent: &str) {
    let (outer, seen) = (
        OUTER_TRACEPARENT.split('-').collect::<Vec<_>>(),
        traceparent.split('-').collect::<Vec<_>>(),
    );
    assert_eq!(seen[1], outer[1], "the trace id continues");
    assert_ne!(seen[2], outer[2], "each hop is a new span");
}

async fn invoke(
    node: &TestNode,
    operation: &str,
    input: Value,
    context: InvocationContext,
) -> Result<Value, DispatchError> {
    node.invoke(operation, input, context, RequestContext::root())
        .await
}

/// The capability id a nodule is told in `x-nodule-capability`, as the fixture echoes it.
async fn own_capability(node: &TestNode, spec: &Spec) -> Option<String> {
    invoke(node, &spec.echo(), json!({}), Default::default())
        .await
        .unwrap()["noduleCapability"]
        .as_str()
        .map(str::to_owned)
}

/// What the caller's `relay` reports having received from calling `operation` through
/// `dispatch.local`. `authority` overrides the capability the nodule presents, which is
/// otherwise the one it was told.
async fn relay(
    node: &TestNode,
    from: &Spec,
    operation: &str,
    authority: Option<&str>,
    context: InvocationContext,
) -> Value {
    let mut input = json!({ "operation": operation, "input": { "n": 1 } });
    if let Some(authority) = authority {
        input["authority"] = json!(authority);
    }
    invoke(node, &from.relay(), input, context).await.unwrap()
}

#[tokio::test]
async fn admit_accepts_a_placeholder_handler_digest_whose_bytes_were_never_put() {
    let spec = caller();
    let node = super::node()
        .with_nodule(cartridge(&spec, &[]), Fixture)
        .boot()
        .await;
    // Admitted at boot with a digest no blob has: the native executor never reads it, so the
    // nodule is ready the moment it is enabled.
    let enabled = node.enable(&spec.cartridge).await;
    assert_eq!(enabled.readiness, Readiness::Ready);
    assert!(node.big_node().executor.hosts(&spec.cartridge));
    node.stop().await;
}

#[tokio::test]
async fn a_native_handler_is_served_the_production_delivery_contract() {
    let spec = caller();
    let node = super::node()
        .with_nodule(cartridge(&spec, &[]), Fixture)
        .boot()
        .await;
    node.enable(&spec.cartridge).await;
    let echo = spec.echo();

    // The operator's capability as a bare `Bearer` token (no signer), the idempotency key, a
    // fresh request id and the outer trace under a new span. No operations are granted, so the
    // nodule is told no capability of its own.
    let operator_cap = serde_json::to_value(node.big_node().authority.root).unwrap();
    let context = InvocationContext {
        idempotency_key: Some("key-1".into()),
    };
    let mut outer = RequestContext::root();
    outer.trace = TraceContext::from_headers(Some(OUTER_TRACEPARENT), None);
    let echoed = node
        .invoke(&echo, json!({ "n": 1 }), context, outer.clone())
        .await
        .unwrap();
    assert_eq!(echoed["input"], json!({ "n": 1 }));
    assert_eq!(
        echoed["authorization"],
        format!("Bearer {}", operator_cap.as_str().unwrap())
    );
    assert_eq!(echoed["idempotencyKey"], "key-1");
    assert_eq!(echoed["timeoutMs"], Value::Null);
    assert_ne!(echoed["requestId"], outer.request_id.to_string());
    assert_eq!(echoed["requestId"].as_str().unwrap().len(), 36, "a uuid");
    assert_continues_trace(echoed["traceparent"].as_str().unwrap());
    assert_eq!(echoed["noduleCapability"], Value::Null);

    // A budget travels as the relative `x-timeout-ms`, re-derived from what is left.
    let budgeted = RequestContext::root().with_time_budget(std::time::Duration::from_secs(60));
    let echoed = node
        .invoke(&echo, json!({}), Default::default(), budgeted)
        .await
        .unwrap();
    let timeout_ms: u64 = echoed["timeoutMs"].as_str().unwrap().parse().unwrap();
    assert!(
        (1..=60_000).contains(&timeout_ms),
        "the handler sees what is left of 60s, got {timeout_ms}ms"
    );

    // Disabling unbinds the operations and stops the thread; enabling starts it again.
    node.set_enabled(&spec.cartridge, false).await;
    assert_eq!(
        invoke(&node, &echo, json!({}), Default::default()).await,
        Err(DispatchError::UnknownOperation)
    );
    assert!(!node.big_node().executor.hosts(&spec.cartridge));
    node.enable(&spec.cartridge).await;
    assert_eq!(
        invoke(&node, &echo, json!({ "n": 2 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 2 })
    );
    node.stop().await;
}

#[tokio::test]
async fn a_native_nodule_calls_another_through_dispatch_only_with_its_own_live_grant() {
    let (a, b) = (caller(), callee());
    let node = super::node()
        .with_nodule(cartridge(&a, &[b.echo(), b.relay()]), Fixture)
        .with_nodule(cartridge(&b, &[a.echo()]), Fixture)
        .boot()
        .await;
    for spec in [&a, &b] {
        assert_eq!(
            node.enable(&spec.cartridge).await.readiness,
            Readiness::Ready
        );
    }
    let target = b.echo();
    let context = InvocationContext {
        idempotency_key: Some("nested-key".into()),
    };

    // Granted a different operation, A holds a capability that does not permit the callee's
    // echo: dispatch denies the nested call and the handler sees a 403, which is how a missing
    // grant is observed.
    node.grant(
        &a.cartridge,
        DispatchScope {
            operations: [b.relay()].into(),
            ..Default::default()
        },
    )
    .await;
    let unrelated = own_capability(&node, &a)
        .await
        .expect("a granted nodule is told its capability");
    let denied = relay(&node, &a, &target, None, context.clone()).await;
    assert_eq!(denied["status"], 403, "{denied}");

    // Granted the callee's authority, the same relay succeeds, and B sees the nodule's own
    // capability as the caller's `authorization`.
    node.grant(
        &a.cartridge,
        DispatchScope {
            authorities: [b.authority.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    let granted = own_capability(&node, &a).await.unwrap();
    assert_ne!(granted, unrelated, "a changed grant is a new capability");
    let relayed = relay(&node, &a, &target, None, context.clone()).await;
    assert_eq!(relayed["status"], 200, "{relayed}");
    assert_eq!(relayed["body"]["input"], json!({ "n": 1 }));
    assert_eq!(
        relayed["body"]["authorization"],
        json!(format!("Bearer {granted}"))
    );
    assert_eq!(relayed["body"]["idempotencyKey"], "nested-key");
    assert_eq!(relayed["body"]["noduleCapability"], Value::Null);

    // The nested call is a hop of its own: a new request id, the outer trace under a new span
    // and a budget within the outer one.
    let mut outer = RequestContext::root().with_time_budget(std::time::Duration::from_secs(60));
    outer.trace = TraceContext::from_headers(Some(OUTER_TRACEPARENT), None);
    let nested = node
        .invoke(
            &a.relay(),
            json!({ "operation": target, "input": {} }),
            context.clone(),
            outer.clone(),
        )
        .await
        .unwrap();
    assert_eq!(nested["status"], 200, "{nested}");
    assert_ne!(nested["body"]["requestId"], outer.request_id.to_string());
    assert_continues_trace(nested["body"]["traceparent"].as_str().unwrap());
    let nested_ms: u64 = nested["body"]["timeoutMs"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        (1..=60_000).contains(&nested_ms),
        "the nested call carries a budget within the outer 60s, got {nested_ms}ms"
    );

    // A handler forging another nodule's capability id is denied: B's capability is bound to B.
    // It permits A's echo (which B requested), so only its holder binding can refuse A's use of
    // it.
    node.grant(
        &b.cartridge,
        DispatchScope {
            authorities: [a.authority.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    let theirs = own_capability(&node, &b).await.unwrap();
    let forged = relay(&node, &a, &a.echo(), Some(&theirs), context.clone()).await;
    assert_eq!(forged["status"], 403, "{forged}");
    // The same capability from its own nodule is honored, which is what the forgery lacked.
    let own = relay(&node, &b, &a.echo(), None, context.clone()).await;
    assert_eq!(own["status"], 200, "{own}");

    // Revoking takes effect: the handler is told no capability and the one it remembered is dead.
    node.grant(&a.cartridge, DispatchScope::default()).await;
    assert_eq!(own_capability(&node, &a).await, None);
    let stale = relay(&node, &a, &target, Some(&granted), context).await;
    assert_eq!(stale["status"], 403, "{stale}");
    node.stop().await;
}

#[tokio::test]
async fn a_handlers_domain_error_reaches_the_caller_typed() {
    let spec = caller();
    let node = super::node()
        .with_nodule(cartridge(&spec, &[]), Fixture)
        .boot()
        .await;
    node.enable(&spec.cartridge).await;
    // No authority and no capability of its own (nothing granted): the fixture's own
    // `InvalidRequest` domain error, which must not be flattened into a dispatch input refusal.
    let refused = node
        .call(CallRelay {
            operation: "org.example.callee.echo".into(),
            authority: None,
            input: None,
        })
        .await;
    assert_eq!(
        refused,
        Err(crate::CallError::Method(RelayError::InvalidRequest(
            "no authority and no nodule capability".into()
        )))
    );
    node.stop().await;
}
