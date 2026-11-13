//! A real Wasm handler (`test_nodule_guest`) hosted by the node's `big_nodule_wasm` executor,
//! driven through dispatch the way an operator would: the handler bytes go into the blob store,
//! the nodule is admitted and enabled, and `org.example.test.*` operations are invoked.
//!
//! Needs the guest component, built with
//! `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p test_nodule_guest --target wasm32-wasip3 --release`.

use super::*;
use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::Artifact;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Contribution;
use big_cartridges_core::Digest;
use big_cartridges_core::NoduleContribution;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::protocol::ClearCartridgeConfig;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::registry::version_pin;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::request::Request;
use big_cartridges_core::test_support::sign_as_example;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::NoduleRecord;
use method_core::ByteBody;
use method_core::Method;
use service_utils_rs::ClientError;
use service_utils_rs::TraceContext;
use std::time::Duration;

/// One nodule running the test guest: the guest serves `echo` and `relay` under whatever
/// authority the nodule owns, so the same component stands in for several nodules.
pub(super) struct Spec {
    cartridge: CartridgeId,
    pub(super) authority: &'static str,
}

impl Spec {
    pub(super) fn echo(&self) -> String {
        format!("{}.echo", self.authority)
    }

    pub(super) fn config(&self) -> String {
        format!("{}.config", self.authority)
    }

    fn relay(&self) -> String {
        format!("{}.relay", self.authority)
    }

    /// Implemented by the nodule but not served by the guest, which answers 404 for it.
    fn absent(&self) -> String {
        format!("{}.absent", self.authority)
    }
}

pub(super) fn caller() -> Spec {
    Spec {
        cartridge: example_cartridge!("wasm-test").parse().unwrap(),
        authority: "org.example.test",
    }
}

fn callee() -> Spec {
    Spec {
        cartridge: example_cartridge!("wasm-callee").parse().unwrap(),
        authority: "org.example.callee",
    }
}

pub(super) fn guest_bytes() -> Vec<u8> {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src/wasm_utils_rs/target")
        });
    let path = target_dir.join("wasm32-wasip3/release/test_nodule_guest.wasm");
    assert!(
        path.is_file(),
        "{} not found; build with `cargo build --locked --manifest-path src/wasm_utils_rs/Cargo.toml --release --target wasm32-wasip3 -p test_nodule_guest`",
        path.display()
    );
    std::fs::read(path).unwrap()
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
fn setting(nsid: &str) -> Value {
    json!({
        "lexicon": 1,
        "id": nsid,
        "defs": {
            "main": {
                "type": "object",
                "properties": {}
            }
        }
    })
}

/// The cartridge whose nodule declares `spec.authority`'s three operations, implements them through
/// the handler artifact `bytes`, and requests each of `calls` (operations of another nodule it
/// relays to). The requests are optional: the nodule runs before its grant, and the tests
/// exercise what the grant then permits.
pub(super) fn cartridge(spec: &Spec, bytes: &[u8], calls: &[String]) -> Cartridge {
    cartridge_requiring(
        spec,
        bytes,
        calls
            .iter()
            .map(|nsid| {
                let mut request = Request::new(nsid, nsid);
                request.necessity = Necessity::Optional;
                request
            })
            .collect(),
    )
}

/// [`cartridge`] with the nodule's `requires` given as they are, so a test can make a request
/// required.
fn cartridge_requiring(spec: &Spec, bytes: &[u8], requires: Vec<Request>) -> Cartridge {
    let mut nodule = Nodule {
        requires,
        ..Default::default()
    };
    let operations = [spec.echo(), spec.relay(), spec.absent()];
    let config = spec.config();
    for nsid in &operations {
        nodule
            .lexicons
            .push(serde_json::from_value(procedure(nsid)).unwrap());
    }
    nodule
        .lexicons
        .push(serde_json::from_value(procedure(&config)).unwrap());
    nodule
        .lexicons
        .push(serde_json::from_value(setting(&format!("{}.settings", spec.authority))).unwrap());
    nodule.settings = vec![format!("{}.settings", spec.authority)];
    nodule.handler = Some(Handler {
        artifact: "handler".into(),
        extra: Default::default(),
    });
    nodule.implements = operations.into();
    nodule.implements.push(config);
    let mut cartridge = Cartridge::new(spec.cartridge.clone(), "1.0.0".parse().unwrap());
    cartridge.authority = Some(spec.authority.to_owned());
    cartridge.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest::of(bytes),
        size: Some(bytes.len() as u64),
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

pub(super) async fn put(node: &BigNode, bytes: &[u8]) {
    node.blobs
        .put(
            RequestContext::root(),
            blobs::put_cap(node).await,
            ByteBody::from_bytes(bytes.to_vec()),
        )
        .await
        .unwrap();
}

/// Publishes `cartridge` through the registry (signed as the example namespace when it is
/// unsigned; the handler bytes must be in the blob store already) and installs the pin of the
/// version just published.
pub(super) async fn admit(node: &BigNode, cartridge: Cartridge) -> NoduleRecord {
    let cartridge = if cartridge.signature.is_some() {
        cartridge
    } else {
        sign_as_example(cartridge)
    };
    let (id, version) = (cartridge.id.clone(), cartridge.version.clone());
    let operator = super::cartridges::operator(node);
    super::cartridges::publish(node, operator, cartridge)
        .await
        .unwrap();
    let doc_id = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            GetCartridge { id: id.clone() },
        )
        .await
        .unwrap()
        .doc_id;
    let heads = blobs::all_datums(node, &doc_id).await.heads;
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            InstallCartridge {
                cartridge: version_pin(doc_id, &version, heads),
            },
        )
        .await
        .unwrap();
    record_of(node, &id).await
}

/// Installs `cartridge` whose handler bytes are not in the blob store yet. The registry service
/// refuses a version whose artifacts it cannot find, so this writes the version datum of a registry
/// document itself, as the operator, and installs the pin of it: the nodule then waits for its
/// bytes (`ArtifactsUnavailable`) instead of being refused.
pub(super) async fn admit_before_its_bytes(node: &BigNode, cartridge: Cartridge) -> NoduleRecord {
    use big_cabinet_core::authority::Rights;
    use big_cabinet_core::doc::CommandId;
    use big_cabinet_core::doc::DocScope;
    use big_cartridges_core::registry::version_key;
    let cartridge = sign_as_example(cartridge);
    let operator = Authorization::identity(node.authority.root);
    let placement = format!("drawer {}", node.registry_drawer);
    let capability = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            big_cabinet_core::doc::MintDocCapability {
                scope: DocScope {
                    create: [placement.clone()].into(),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let receipt = node
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: CommandId::try_from("version before its bytes".to_owned()).unwrap(),
                placement,
                datums: BTreeMap::from([(
                    version_key(&cartridge.version),
                    serde_json::to_value(&cartridge).unwrap(),
                )]),
            },
        )
        .await
        .unwrap();
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            InstallCartridge {
                cartridge: version_pin(receipt.doc_id, &cartridge.version, receipt.heads),
            },
        )
        .await
        .unwrap();
    record_of(node, &cartridge.id).await
}

pub(super) async fn set_enabled(node: &BigNode, spec: &Spec, enabled: bool) -> NoduleRecord {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            SetCartridgeEnabled {
                cartridge_id: spec.cartridge.clone(),
                component: Component::Nodule,
                enabled,
            },
        )
        .await
        .unwrap();
    record(node, spec).await
}

/// Makes `methods` the whole of the nodule's method grants: the cartridge's config is cleared
/// (these nodules have no settings), then each operation and authority is granted as a datum of
/// its own.
pub(super) async fn set_operations(
    node: &BigNode,
    spec: &Spec,
    methods: DispatchScope,
) -> NoduleRecord {
    let operator = Authorization::identity(node.authority.root);
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            ClearCartridgeConfig {
                cartridge_id: spec.cartridge.clone(),
            },
        )
        .await
        .unwrap();
    let grants = methods
        .operations
        .into_iter()
        .map(GrantKey::Method)
        .chain(methods.authorities.into_iter().map(GrantKey::Authority));
    for grant in grants {
        node.nodules
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                SetCartridgeGrant {
                    cartridge_id: spec.cartridge.clone(),
                    component: Component::Nodule,
                    grant,
                    value: Some(GrantBody::method(methods.privileged)),
                },
            )
            .await
            .unwrap();
    }
    record(node, spec).await
}

async fn record(node: &BigNode, spec: &Spec) -> NoduleRecord {
    record_of(node, &spec.cartridge).await
}

async fn record_of(node: &BigNode, cartridge_id: &CartridgeId) -> NoduleRecord {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            GetNodule {
                cartridge_id: cartridge_id.clone(),
            },
        )
        .await
        .unwrap()
}

pub(super) async fn invoke(
    node: &BigNode,
    operation: &str,
    input: Value,
    context: InvocationContext,
) -> Result<Value, DispatchError> {
    invoke_within(node, operation, input, context, RequestContext::root()).await
}

/// [`invoke`] under the caller's own request context: its budget, request id and trace.
pub(super) async fn invoke_within(
    node: &BigNode,
    operation: &str,
    input: Value,
    context: InvocationContext,
    request: RequestContext,
) -> Result<Value, DispatchError> {
    node.dispatch
        .client
        .call_with(
            request,
            Authorization::identity(node.authority.root).metadata(),
            Invoke::json(operation, input, context),
        )
        .await
        .map(|output| output.body.into_json())
        .map_err(|error| match error {
            ClientError::Method(error) => error,
            ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
        })
}

pub(super) async fn boot_node(secrets: &Arc<SecretStore>) -> (BigNode, StopToken) {
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    BigNode::boot(config(
        Arc::clone(secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap()
}

const OUTER_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// A traceparent in the outer call's trace (same trace id) under a span of its own.
fn assert_continues_trace(traceparent: &str) {
    let (outer, seen) = (
        OUTER_TRACEPARENT.split('-').collect::<Vec<_>>(),
        traceparent.split('-').collect::<Vec<_>>(),
    );
    assert_eq!(seen[1], outer[1], "the trace id continues");
    assert_ne!(seen[2], outer[2], "each hop is a new span");
}

#[tokio::test]
async fn a_guest_can_write_only_its_declared_settings() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let spec = caller();
    put(&node, &bytes).await;
    admit(&node, cartridge(&spec, &bytes, &[])).await;
    set_enabled(&node, &spec, true).await;

    let initial = invoke(
        &node,
        &spec.config(),
        json!({"action": "read"}),
        Default::default(),
    )
    .await
    .expect("the config document is readable before writing");
    assert!(initial.get("org.example.test.settings/main").is_none());

    let setting = json!({"enabled": true});
    invoke(
        &node,
        &spec.config(),
        json!({
            "action": "write",
            "tag": "org.example.test.settings",
            "slot": "main",
            "value": setting,
        }),
        Default::default(),
    )
    .await
    .expect("the declared setting is writable");
    let read = invoke(
        &node,
        &spec.config(),
        json!({"action": "read"}),
        Default::default(),
    )
    .await
    .expect("the config document is readable");
    assert!(read.get("org.example.test.settings/main").is_some());

    fn assert_config_denied(result: Result<Value, DispatchError>) {
        match result {
            Err(DispatchError::Provider { status: 400, body }) => {
                assert_eq!(body["error"], "Config");
                let message = body["message"].as_str().unwrap_or_default();
                assert!(
                    message.contains("Denied")
                        && message
                            .contains("the presented capability does not permit this operation"),
                    "unexpected config denial: {body}"
                );
            }
            other => panic!("expected the docs Denied error through the guest, got {other:?}"),
        }
    }

    assert_config_denied(
        invoke(
            &node,
            &spec.config(),
            json!({
                "action": "write",
                "tag": "org.example.daybook.nodule.grant",
                "slot": "method:org.example.other.ping",
                "value": {"privileged": false},
            }),
            Default::default(),
        )
        .await,
    );
    assert_config_denied(
        invoke(
            &node,
            &spec.config(),
            json!({
                "action": "write",
                "tag": "org.example.test.other",
                "slot": "main",
                "value": {"enabled": false},
            }),
            Default::default(),
        )
        .await,
    );

    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_wasm_handler_serves_dispatched_operations_until_disabled() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let spec = caller();
    let (echo, absent) = (spec.echo(), spec.absent());
    put(&node, &bytes).await;
    admit(&node, cartridge(&spec, &bytes, &[])).await;
    let enabled = set_enabled(&node, &spec, true).await;
    assert_eq!(enabled.readiness, Readiness::Ready);
    assert!(node.executor.hosts(&spec.cartridge));

    let old_config = invoke(
        &node,
        &spec.config(),
        json!({"action": "capability"}),
        Default::default(),
    )
    .await
    .expect("the initial settings capability is delivered");
    let old_doc_id = old_config["docId"].as_str().unwrap().to_owned();
    let old_capability = old_config["capability"].as_str().unwrap().to_owned();
    // The echo returns the input, and the headers the host built for the guest: the operator's
    // capability as a bare `Bearer` token (no signer), the idempotency key, the request id and
    // no budget. The declared settings also grant docs operations, so a nodule capability is present.
    let operator_cap = serde_json::to_value(node.authority.root).unwrap();
    let context = InvocationContext {
        idempotency_key: Some("key-1".into()),
        ..Default::default()
    };
    let mut outer = RequestContext::root();
    outer.trace = TraceContext::from_headers(Some(OUTER_TRACEPARENT), None);
    let echoed = invoke_within(&node, &echo, json!({ "n": 1 }), context, outer.clone())
        .await
        .unwrap();
    assert_eq!(echoed["input"], json!({ "n": 1 }));
    assert_eq!(
        echoed["authorization"],
        format!("Bearer {}", operator_cap.as_str().unwrap())
    );
    assert_eq!(echoed["idempotencyKey"], "key-1");
    assert_eq!(echoed["timeoutMs"], Value::Null);
    // The guest's request id is the delivery hop's, not the caller's; the trace continues
    // under a new span.
    assert_ne!(echoed["requestId"], outer.request_id.to_string());
    assert_eq!(echoed["requestId"].as_str().unwrap().len(), 36, "a uuid");
    assert_continues_trace(echoed["traceparent"].as_str().unwrap());
    assert!(echoed["noduleCapability"].is_string());

    // A budget travels as the relative `x-timeout-ms`, re-derived from what is left.
    let budgeted = RequestContext::root().with_time_budget(std::time::Duration::from_secs(60));
    let echoed = invoke_within(&node, &echo, json!({}), Default::default(), budgeted)
        .await
        .unwrap();
    let timeout_ms: u64 = echoed["timeoutMs"].as_str().unwrap().parse().unwrap();
    assert!(
        (1..=60_000).contains(&timeout_ms),
        "the guest sees what is left of 60s, got {timeout_ms}ms"
    );
    assert_eq!(echoed["idempotencyKey"], Value::Null);

    // A budget already spent fails before the handler is entered.
    let elapsed = RequestContext::root().with_time_budget(std::time::Duration::ZERO);
    assert_eq!(
        invoke_within(&node, &echo, json!({}), Default::default(), elapsed).await,
        Err(DispatchError::Context(
            method_core::ContextError::DeadlineExceeded
        ))
    );

    // The guest's own refusal of an operation it does not serve is the provider's error.
    assert_eq!(
        invoke(&node, &absent, json!({}), Default::default()).await,
        Err(DispatchError::Provider {
            status: 404,
            body: json!({ "error": "UnknownOperation" }),
        })
    );

    // Disabling unbinds the operations and stops the workload; enabling starts it again.
    set_enabled(&node, &spec, false).await;
    assert_eq!(
        invoke(&node, &echo, json!({}), Default::default()).await,
        Err(DispatchError::UnknownOperation)
    );
    assert!(!node.executor.hosts(&spec.cartridge));
    let revoked = invoke(
        &node,
        "org.example.daybook.docs.writeDatums",
        json!({
            "capability": old_capability,
            "docId": old_doc_id,
            "mode": {"mode": "atCurrent"},
            "set": {"org.example.test.settings/main": {"enabled": false}},
            "remove": [],
        }),
        Default::default(),
    )
    .await;
    match revoked {
        Err(DispatchError::Provider { status: 400, body }) => {
            assert_eq!(body["error"], "Denied");
            assert!(
                body["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("presented capability")),
                "unexpected revoked-capability response: {body}"
            );
        }
        other => panic!("expected the retracted settings capability to be denied, got {other:?}"),
    }
    set_enabled(&node, &spec, true).await;
    assert!(node.executor.hosts(&spec.cartridge));
    let new_config = invoke(
        &node,
        &spec.config(),
        json!({"action": "capability"}),
        Default::default(),
    )
    .await
    .expect("re-enabling mints settings capability");
    assert_ne!(new_config["capability"].as_str().unwrap(), old_capability);
    invoke(
        &node,
        &spec.config(),
        json!({
            "action": "write",
            "tag": "org.example.test.settings",
            "slot": "main",
            "value": {"enabled": true},
        }),
        Default::default(),
    )
    .await
    .expect("the re-enabled settings capability is usable");
    assert_eq!(
        invoke(&node, &echo, json!({ "n": 2 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 2 })
    );
    shutdown(node, stop).await;
}

/// The capability id a nodule is told in `x-nodule-capability`, as the guest echoes it.
async fn own_capability(node: &BigNode, spec: &Spec) -> Option<String> {
    invoke(node, &spec.echo(), json!({}), Default::default())
        .await
        .unwrap()["noduleCapability"]
        .as_str()
        .map(str::to_owned)
}

/// What the caller's `relay` reports having received from calling `operation` through
/// `dispatch.local`: the upstream status and body. `authority` overrides the capability the
/// guest presents, which is otherwise the one it was told.
pub(super) async fn relay(
    node: &BigNode,
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
async fn a_nodule_calls_another_through_dispatch_only_with_its_own_live_grant() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let (a, b) = (caller(), callee());
    put(&node, &bytes).await;
    // A relays to B's operations, and B to A's echo: each requests what it calls, which is all
    // its grants can ever confer.
    let requested = [(&a, vec![b.echo(), b.absent()]), (&b, vec![a.echo()])];
    for (spec, calls) in &requested {
        admit(&node, cartridge(spec, &bytes, calls)).await;
        assert_eq!(
            set_enabled(&node, spec, true).await.readiness,
            Readiness::Ready
        );
    }
    let target = b.echo();
    let context = InvocationContext {
        idempotency_key: Some("nested-key".into()),
        ..Default::default()
    };

    // Granted a different operation, A holds a capability that does not permit the callee's:
    // dispatch denies the nested call and the handler sees a 403.
    let first = set_operations(
        &node,
        &a,
        DispatchScope {
            operations: [b.absent()].into(),
            ..Default::default()
        },
    )
    .await;
    assert!(first.methods.as_ref().unwrap().granted);
    let unrelated = own_capability(&node, &a)
        .await
        .expect("a granted nodule is told its capability");
    let denied = relay(&node, &a, &target, None, context.clone()).await;
    assert_eq!(denied["status"], 403, "{denied}");

    // Granted the callee's authority, the same relay succeeds. The grant changed in place, so
    // the already-running handler is told the new capability, and B sees it as the caller's
    // `authorization` along with the context headers.
    set_operations(
        &node,
        &a,
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
    // B has its own settings grant, but it was not handed A's dispatch capability.
    assert!(relayed["body"]["noduleCapability"].is_string());
    assert_ne!(relayed["body"]["noduleCapability"], json!(granted));

    // The nested call is a hop of its own: a new request id (the id is the admission key and
    // the outer call is still in flight), the outer trace under a new span, and a budget
    // within the outer one. The guest forwards the trace and budget it was delivered and the
    // egress parses them rather than starting a fresh context.
    let mut outer = RequestContext::root().with_time_budget(std::time::Duration::from_secs(60));
    outer.trace = TraceContext::from_headers(Some(OUTER_TRACEPARENT), None);
    let nested = invoke_within(
        &node,
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

    // A guest forging another nodule's capability id is denied: B's capability is bound to B.
    // It permits A's echo (which B requested), so only its holder binding can refuse A's use of
    // it.
    set_operations(
        &node,
        &b,
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

    // Revoking the dispatch grant takes effect while the separate settings capability remains;
    // the capability A remembered for dispatch is dead.
    let revoked = set_operations(&node, &a, DispatchScope::default()).await;
    assert!(revoked.methods.is_none());
    assert!(own_capability(&node, &a).await.is_some());
    let stale = relay(&node, &a, &target, Some(&granted), context.clone()).await;
    assert_eq!(stale["status"], 403, "{stale}");

    // Granting again mints a fresh capability that works.
    set_operations(
        &node,
        &a,
        DispatchScope {
            operations: [target.clone()].into(),
            ..Default::default()
        },
    )
    .await;
    let regranted = own_capability(&node, &a).await.unwrap();
    assert_ne!(regranted, granted);
    let again = relay(&node, &a, &target, None, context).await;
    assert_eq!(again["status"], 200, "{again}");

    // Disabling revokes too.
    set_enabled(&node, &a, false).await;
    assert_eq!(
        record(&node, &a).await.methods.map(|status| status.granted),
        Some(false)
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_handler_whose_bytes_arrive_late_becomes_ready_when_they_do() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let spec = caller();
    let echo = spec.echo();
    admit_before_its_bytes(&node, cartridge(&spec, &bytes, &[])).await;
    let waiting = set_enabled(&node, &spec, true).await;
    assert_eq!(waiting.readiness, Readiness::ArtifactsUnavailable);
    assert!(!node.executor.hosts(&spec.cartridge));
    assert_eq!(
        invoke(&node, &echo, json!({}), Default::default()).await,
        Err(DispatchError::NotReady {
            reason: NotReadyReason::Provider(Readiness::ArtifactsUnavailable),
        })
    );

    // The blob service's availability feed wakes the executor, which asks the state actor to
    // ensure again: no reconcile trigger from the test. (The test itself can only wait for
    // the state to change by looking; there is no signal for "reconciled" to subscribe to.)
    put(&node, &bytes).await;
    while record(&node, &spec).await.readiness != Readiness::Ready {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        invoke(&node, &echo, json!({ "n": 3 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 3 })
    );
    shutdown(node, stop).await;
}

/// A cartridge published on one node is installed and invoked on another (ADR 033 section 9). Alice
/// publishes the nodule's cartridge in her registry and invites Bob's node to her registry drawer at
/// `access`; the cartridge document and, through Bob's mirror of the drawer's blob inventory, the
/// handler bytes sync to Bob. Bob's operator installs the document into Bob's registry, lending the
/// registry a handle on Alice's drawer it holds without any grant (the operator reaches every
/// drawer), then admits the nodule from what Bob's registry serves and invokes it through his
/// dispatch once the synced bytes make it ready.
///
/// Installing enters Alice's document in Bob's registry drawer at Read, which Bob's node signs as
/// a Keyhive delegation on a document it holds only through Alice's drawer: both invite levels
/// are exercised, since Read is the lowest that can carry it.
async fn install_a_cartridge_published_on_another_node(
    access: big_cabinet_core::drawer::DrawerAccess,
) {
    use super::cartridges::import_document;
    use super::cartridges::operator;
    use super::cartridges::publish;
    use super::harness::Cluster;
    use super::harness::Topology;
    use big_cartridges_core::protocol::GetCartridge;
    use big_cartridges_core::protocol::GetCartridgeVersion;
    use big_cartridges_core::test_support::sign_as_example;

    let bytes = guest_bytes();
    let spec = caller();
    let mut cluster = Cluster::boot(&["alice", "bob"]).await;
    cluster.connect(Topology::Pair).await;
    let (alice, bob) = (0, 1);

    let publisher = cluster.node(alice);
    put(publisher, &bytes).await;
    let envelope = sign_as_example(cartridge(&spec, &bytes, &[]));
    publish(publisher, operator(publisher), envelope.clone())
        .await
        .unwrap();
    let document = publisher
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(publisher).metadata(),
            GetCartridge {
                id: spec.cartridge.clone(),
            },
        )
        .await
        .unwrap()
        .doc_id;
    let registry = publisher.registry_drawer.clone();
    cluster.admit(alice, bob, &registry, access).await;
    cluster.mirror_blobs(bob, &registry).await;
    cluster
        .until(async |cluster| cluster.read_note(bob, &document).await.is_some())
        .await;

    let node = cluster.node(bob);
    let installed = import_document(node, operator(node), &registry, &document)
        .await
        .unwrap();
    assert_eq!(installed.id, spec.cartridge);
    assert_eq!(installed.doc_id, document);
    let served = node
        .cartridges
        .client
        .call_with(
            RequestContext::root(),
            operator(node).metadata(),
            GetCartridgeVersion {
                id: spec.cartridge.clone(),
                version: envelope.version.clone(),
            },
        )
        .await
        .unwrap()
        .envelope;
    assert_eq!(served, envelope);

    admit(node, served).await;
    set_enabled(node, &spec, true).await;
    // There is no signal for "reconciled" to subscribe to (see the late-bytes test above): the
    // record is looked at until the synced handler bytes have made the nodule ready.
    while record(node, &spec).await.readiness != Readiness::Ready {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(node.executor.hosts(&spec.cartridge));
    assert_eq!(
        invoke(
            node,
            &spec.echo(),
            json!({ "from": "bob" }),
            Default::default()
        )
        .await
        .unwrap()["input"],
        json!({ "from": "bob" })
    );
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cartridge_published_on_one_node_is_installed_and_invoked_on_another() {
    install_a_cartridge_published_on_another_node(big_cabinet_core::drawer::DrawerAccess::Read)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cartridge_published_on_one_node_is_installed_by_an_admin_member_of_its_drawer() {
    install_a_cartridge_published_on_another_node(big_cabinet_core::drawer::DrawerAccess::Admin)
        .await;
}

/// A nodule whose required requests are built-in docs and drawers methods is `Ready` again after
/// a restart, and serves. The Boot reconcile of the nodule service resolves requests against the
/// router's claims, so the node's own providers must be registered before it runs.
#[tokio::test]
async fn a_nodule_requiring_built_in_methods_is_ready_after_a_restart() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let boot = async || {
        let runtime = Arc::new(SqliteRuntime::new(Default::default()));
        let bindings = Bindings::in_directory(&runtime, directory.path(), None)
            .await
            .unwrap();
        let mut cfg = config(Arc::clone(&secrets), bindings.sql, bindings.repo);
        cfg.blob_storage = bindings.blobs;
        BigNode::boot(cfg).await.unwrap()
    };
    let spec = caller();
    let required = [
        CreateDoc::XRPC.nsid,
        big_cabinet_core::drawer::protocol::CreateDrawer::XRPC.nsid,
    ];

    let (node, stop) = boot().await;
    put(&node, &bytes).await;
    admit(
        &node,
        cartridge_requiring(
            &spec,
            &bytes,
            required
                .iter()
                .map(|nsid| Request::new(*nsid, *nsid))
                .collect(),
        ),
    )
    .await;
    set_operations(
        &node,
        &spec,
        DispatchScope {
            operations: required.iter().map(|nsid| (*nsid).to_owned()).collect(),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        set_enabled(&node, &spec, true).await.readiness,
        Readiness::Ready
    );
    shutdown(node, stop).await;

    let (node, stop) = boot().await;
    assert_eq!(record(&node, &spec).await.readiness, Readiness::Ready);
    assert_eq!(
        invoke(&node, &spec.echo(), json!({ "n": 1 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 1 })
    );
    shutdown(node, stop).await;
}
