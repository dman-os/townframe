//! `test_nodule_handlers`' echo and relay served natively, against the assertions of the wasm
//! e2e (`src/big_node/e2e/nodule_wasm.rs`): the same contract, observed through the same real
//! node, with the native executor in place of the wasm one.

use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::ComponentExport;
use big_cartridges_core::Nodule;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindMethod;
use big_cartridges_core::Routine;
use big_cartridges_core::RoutineImpl;
use big_cartridges_core::test_support::FixtureCartridge;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Readiness;
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

    fn ask(&self) -> String {
        format!("{}.ask", self.authority)
    }

    /// The query [`Asker`] serves, which answers the parameters it was called with.
    fn peek(&self) -> String {
        format!("{}.peek", self.authority)
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

fn procedure(nsid: &str, input: Value) -> Value {
    json!({
        "lexicon": 1,
        "id": nsid,
        "defs": { "main": {
            "type": "procedure",
            "input": {
                "encoding": "application/json",
                "schema": input
            },
            "output": {
                "encoding": "application/json",
                "schema": { "type": "object", "properties": {} }
            }
        }}
    })
}

/// The cartridge declaring `spec`'s echo and relay, with a handler component of placeholder bytes
/// (the native executor never reads them), requesting each of `calls`.
fn cartridge(spec: &Spec, calls: &[String]) -> FixtureCartridge {
    cartridge_with_echo_input(spec, calls, json!({ "type": "object", "properties": {} }))
}

/// [`cartridge`] with `echo_input` as the input schema its `echo` declares.
fn cartridge_with_echo_input(spec: &Spec, calls: &[String], echo_input: Value) -> FixtureCartridge {
    let operations = [spec.echo(), spec.relay(), spec.ask()];
    let mut nodule = Nodule::default();
    let mut lexicons = Vec::new();
    for nsid in &operations {
        let input = if *nsid == spec.echo() {
            echo_input.clone()
        } else {
            json!({ "type": "object", "properties": {} })
        };
        lexicons.push(serde_json::from_value(procedure(nsid, input)).unwrap());
        nodule.routines.insert(
            nsid.clone(),
            Routine {
                r#impl: RoutineImpl::ComponentExport(ComponentExport {
                    component: "handler".into(),
                    export: "handle".into(),
                    extra: Default::default(),
                }),
                implements: nsid.parse().unwrap(),
                uses: Default::default(),
                extra: Default::default(),
            },
        );
    }
    let peek = spec.peek();
    let peek_file = serde_json::from_value(json!({
        "lexicon": 1,
        "id": peek,
        "defs": { "main": {
            "type": "query",
            "parameters": {
                "type": "params",
                "required": ["n", "label"],
                "properties": {
                    "n": { "type": "integer" },
                    "label": { "type": "string" }
                }
            },
            "output": {
                "encoding": "application/json",
                "schema": { "type": "object", "properties": {} }
            }
        }}
    }))
    .unwrap();
    lexicons.push(peek_file);
    nodule.routines.insert(
        peek.clone(),
        Routine {
            r#impl: RoutineImpl::ComponentExport(ComponentExport {
                component: "handler".into(),
                export: "handle".into(),
                extra: Default::default(),
            }),
            implements: peek.parse().unwrap(),
            uses: Default::default(),
            extra: Default::default(),
        },
    );

    let mut cartridge = FixtureCartridge::new(Cartridge::new(
        spec.cartridge.clone(),
        "1.0.0".parse().unwrap(),
    ));
    cartridge.authority = Some(spec.authority.to_owned());
    cartridge.nodule = Some(nodule);
    for file in lexicons {
        cartridge.with_lexicon(file);
    }
    cartridge.with_wasm(
        "handler",
        b"placeholder: the native executor never reads the handler",
    );
    cartridge.permbinds = calls
        .iter()
        .map(|nsid| {
            (
                nsid.clone(),
                Permbind::Method(PermbindMethod {
                    nsid: nsid.parse().unwrap(),
                    description: nsid.clone(),
                    necessity: big_cartridges_core::Necessity::Optional,
                    min_revision: None,
                    extra: Default::default(),
                }),
            )
        })
        .collect();
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

/// Asks the nodule to call the callee's `peek` through the typed client, with these parameters.
#[derive(serde::Serialize, serde::Deserialize)]
struct Ask {
    n: u32,
    label: String,
}

/// The callee's `peek` as the typed client of a caller names it: a query, so a `GET`.
#[derive(serde::Serialize, serde::Deserialize)]
struct CallPeek {
    n: u32,
    label: String,
}

method_core::methods! {
    error = RelayError, nsid_prefix = "org.example.test.";
    CallRelay => Value, procedure, "relay";
    Ask => Value, procedure, "ask";
}

mod callee_query {
    use super::CallPeek;
    use super::RelayError;
    use serde_json::Value;

    method_core::methods! {
        error = RelayError, nsid_prefix = "org.example.callee.";
        CallPeek => Value, query, "peek";
    }
}

/// The fixture plus two operations that exercise queries. `ask` calls the callee's `peek`
/// through the typed client, so the call leaves as `GET /xrpc/<nsid>?<params>` to
/// `dispatch.local`. `peek` is a query a `big_ndk` nodule cannot declare a typed handler for
/// (the kit serves procedures only), so it answers by hand: dispatch hands a provider a query's
/// input as the JSON its Lexicon `parameters` coerce the query string to, which it echoes.
#[derive(Default, Clone)]
struct Asker;

impl big_ndk::Nodule for Asker {
    async fn route(&self, nsid: &str, cx: &big_ndk::Cx, body: &[u8]) -> Option<big_ndk::Reply> {
        match nsid.rsplit('.').next() {
            Some("ask") => Some(big_ndk::invoke::<Self, Ask>(self, cx, body).await),
            Some("peek") => Some(big_ndk::Reply {
                status: http::StatusCode::OK,
                body: serde_json::from_slice(body)
                    .expect("dispatch hands a query its input as JSON"),
            }),
            _ => Fixture.route(nsid, cx, body).await,
        }
    }
}

impl big_ndk::Handler<Ask> for Asker {
    async fn call(&self, cx: &big_ndk::Cx, Ask { n, label }: Ask) -> Result<Value, RelayError> {
        cx.call(CallPeek { n, label })
            .await
            .map_err(|error| RelayError::UpstreamFailed(format!("{error:?}")))
    }
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
async fn admit_accepts_a_placeholder_handler_whose_bytes_are_never_read() {
    let spec = caller();
    let node = super::node()
        .with_nodule(cartridge(&spec, &[]), Fixture)
        .boot()
        .await;
    // Admitted at boot with placeholder bytes: the native executor never reads them, so the
    // nodule is ready the moment it is enabled.
    let enabled = node.enable(&spec.cartridge).await;
    assert_eq!(enabled.readiness, Readiness::Ready);
    assert!(node.big_node().executor.hosts(&spec.cartridge));
    node.stop().await;
}

/// A nodule method's declared input is enforced by dispatch before the handler runs, as a
/// built-in's is: the fixture would echo anything, so a refusal is dispatch's.
#[tokio::test]
async fn dispatch_refuses_a_nodule_methods_malformed_input_before_the_handler() {
    let spec = caller();
    let strict = json!({
        "type": "object",
        "required": ["n"],
        "properties": { "n": { "type": "integer", "minimum": 0 } }
    });
    let node = super::node()
        .with_nodule(cartridge_with_echo_input(&spec, &[], strict), Fixture)
        .boot()
        .await;
    node.enable(&spec.cartridge).await;
    let echo = spec.echo();
    assert_eq!(
        invoke(&node, &echo, json!({ "n": 1 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 1 })
    );
    for broken in [json!({}), json!({ "n": "one" }), json!({ "n": -1 })] {
        let Err(DispatchError::InvalidInput(message)) =
            invoke(&node, &echo, broken.clone(), Default::default()).await
        else {
            panic!("{broken} does not match the declared input and must be refused");
        };
        assert!(message.contains("Lexicon"), "{message}");
    }
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
        ..Default::default()
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
    // No operations are granted, but the nodule's settings capability (docs read and write on its
    // config document) is a dispatch capability of its own, so it is told one.
    assert!(echoed["noduleCapability"].is_string());

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
        ..Default::default()
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
    // B has its own settings capability, but it was not handed A's dispatch capability.
    assert!(relayed["body"]["noduleCapability"].is_string());
    assert_ne!(relayed["body"]["noduleCapability"], json!(granted));

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
    node.revoke(
        &a.cartridge,
        DispatchScope {
            operations: [b.relay()].into(),
            authorities: [b.authority.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    assert!(own_capability(&node, &a).await.is_some());
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
    // No operation to relay: the fixture's own `InvalidRequest` domain error, which must not be
    // flattened into a dispatch input refusal.
    let refused = node
        .call(CallRelay {
            operation: String::new(),
            authority: None,
            input: None,
        })
        .await;
    assert_eq!(
        refused,
        Err(crate::CallError::Method(RelayError::InvalidRequest(
            "no operation to relay".into()
        )))
    );
    node.stop().await;
}

/// `dispatch.local` serves a query as XRPC does, `GET /xrpc/<nsid>?<params>`, decoded by the
/// same Lexicon `parameters` as the node's listener, and refuses a verb that is not the method's
/// kind as the listener does.
#[tokio::test]
async fn dispatch_local_serves_queries_over_get_and_refuses_a_verb_mismatch() {
    let (a, b) = (caller(), callee());
    let node = super::node()
        .with_nodule(cartridge(&a, &[b.peek(), b.echo()]), Asker)
        .with_nodule(cartridge(&b, &[]), Asker)
        .boot()
        .await;
    for spec in [&a, &b] {
        node.enable(&spec.cartridge).await;
    }
    node.grant(
        &a.cartridge,
        DispatchScope {
            authorities: [b.authority.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;

    // The typed client's GET: the parameters travel percent-encoded in the query string and
    // reach the callee as the typed object its Lexicon `parameters` coerce them to (`n` an
    // integer, the label decoded), which `peek` answers.
    let answered = node
        .call(Ask {
            n: 3,
            label: "a&b c=d".to_owned(),
        })
        .await;
    assert_eq!(answered, Ok(json!({ "n": 3, "label": "a&b c=d" })));

    // A query string off the callee's `parameters` is refused by dispatch as the listener
    // refuses it, and the handler sees the refusal as a 400.
    let relay_operation = a.relay();
    let through = |operation: &str, query: Option<&str>| {
        let mut input = json!({ "operation": operation, "input": { "n": 1 } });
        if let Some(query) = query {
            input["query"] = json!(query);
        }
        invoke(&node, &relay_operation, input, Default::default())
    };
    let off_parameters = through(&b.peek(), Some("n=three&label=x")).await.unwrap();
    assert_eq!(off_parameters["status"], 400, "{off_parameters}");
    assert!(
        off_parameters["body"]["message"]
            .as_str()
            .unwrap()
            .contains('n'),
        "{off_parameters}"
    );

    // The wrong verb for the method's kind is dispatch's `InvalidRequest`, 400, whichever way
    // it is wrong.
    let post_to_query = through(&b.peek(), None).await.unwrap();
    assert_eq!(post_to_query["status"], 400, "{post_to_query}");
    assert!(
        post_to_query["body"]["message"]
            .as_str()
            .unwrap()
            .contains("expected GET"),
        "{post_to_query}"
    );
    let get_to_procedure = through(&b.echo(), Some("n=1")).await.unwrap();
    assert_eq!(get_to_procedure["status"], 400, "{get_to_procedure}");
    assert!(
        get_to_procedure["body"]["message"]
            .as_str()
            .unwrap()
            .contains("expected POST"),
        "{get_to_procedure}"
    );
    node.stop().await;
}
