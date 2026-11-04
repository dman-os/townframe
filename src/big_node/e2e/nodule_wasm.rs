//! A real Wasm handler (`test_nodule_guest`) hosted by the node's `big_nodule_wasm` executor,
//! driven through dispatch the way an operator would: the handler bytes go into the blob store,
//! the nodule is admitted and enabled, and `org.example.test.*` operations are invoked.
//!
//! Needs the guest component, built with
//! `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p test_nodule_guest --target wasm32-wasip3 --release`.

use super::*;
use big_blobs::BlobService;
use big_blobs_core::PutBlob;
use big_cabinet_core::datum::DatumMember;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::SetNoduleOperationGrants;
use big_plugs_core::Artifact;
use big_plugs_core::Contribution;
use big_plugs_core::Digest;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use big_plugs_core::PlugId;
use big_plugs_core::example_plug;
use big_plugs_core::request::Necessity;
use big_plugs_core::request::Request;
use service_utils_rs::ClientError;
use std::time::Duration;

/// One nodule running the test guest: the guest serves `echo` and `relay` under whatever
/// authority the nodule owns, so the same component stands in for several nodules.
pub(super) struct Spec {
    plug: PlugId,
    pub(super) authority: &'static str,
}

impl Spec {
    pub(super) fn echo(&self) -> String {
        format!("{}.echo", self.authority)
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
        plug: example_plug!("wasm-test").parse().unwrap(),
        authority: "org.example.test",
    }
}

fn callee() -> Spec {
    Spec {
        plug: example_plug!("wasm-callee").parse().unwrap(),
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

/// The plug whose nodule declares `spec.authority`'s three operations, implements them through
/// the handler artifact `bytes`, and requests each of `calls` (operations of another nodule it
/// relays to). The requests are optional: the nodule runs before its grant, and the tests
/// exercise what the grant then permits.
pub(super) fn plug(spec: &Spec, bytes: &[u8], calls: &[String]) -> Plug {
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
    let operations = [spec.echo(), spec.relay(), spec.absent()];
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
    let mut plug = Plug::new(spec.plug.clone(), "1.0.0".parse().unwrap());
    plug.authority = Some(spec.authority.to_owned());
    plug.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest::of(bytes),
        size: Some(bytes.len() as u64),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    plug.contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: serde_json::from_value(serde_json::to_value(nodule).unwrap()).unwrap(),
            extra: Default::default(),
        }));
    plug
}

pub(super) async fn put(node: &BigNode, bytes: &[u8]) {
    node.blobs
        .client
        .call(
            RequestContext::root(),
            PutBlob {
                capability: blobs::blob_cap(node, BlobService::put_object().into(), Vec::new())
                    .await,
                bytes: bytes.to_vec(),
            },
        )
        .await
        .unwrap();
}

pub(super) async fn admit(node: &BigNode, plug: Plug) -> NoduleRecord {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            AdmitNodule {
                plug,
                drawers: None,
            },
        )
        .await
        .unwrap()
}

pub(super) async fn set_enabled(node: &BigNode, spec: &Spec, enabled: bool) -> NoduleRecord {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            SetNoduleEnabled {
                plug_id: spec.plug.clone(),
                enabled,
            },
        )
        .await
        .unwrap()
}

async fn set_operations(node: &BigNode, spec: &Spec, operations: DispatchScope) -> NoduleRecord {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            SetNoduleOperationGrants {
                plug_id: spec.plug.clone(),
                operations,
            },
        )
        .await
        .unwrap()
}

async fn record(node: &BigNode, spec: &Spec) -> NoduleRecord {
    node.nodules
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            GetNodule {
                plug_id: spec.plug.clone(),
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
    node.dispatch
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            Invoke {
                operation: operation.to_owned(),
                input,
                context,
            },
        )
        .await
        .map(|output| output.output)
        .map_err(|error| match error {
            ClientError::Operation(error) => error,
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

#[tokio::test]
async fn a_wasm_handler_serves_dispatched_operations_until_disabled() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let spec = caller();
    let (echo, absent) = (spec.echo(), spec.absent());
    put(&node, &bytes).await;
    admit(&node, plug(&spec, &bytes, &[])).await;
    let enabled = set_enabled(&node, &spec, true).await;
    assert_eq!(enabled.readiness, Readiness::Ready);
    assert!(node.wasm.hosts(&spec.plug));

    // The echo returns the input, and the headers the host built for the guest: the operator's
    // capability as the authority, and the idempotency key. No operations are granted, so the
    // nodule is told no capability of its own.
    let operator_cap = serde_json::to_value(node.authority.root).unwrap();
    let context = InvocationContext {
        idempotency_key: Some("key-1".into()),
        ..Default::default()
    };
    assert_eq!(
        invoke(&node, &echo, json!({ "n": 1 }), context).await,
        Ok(json!({
            "input": { "n": 1 },
            "authorization": format!("Capability {}", operator_cap.as_str().unwrap()),
            "idempotencyKey": "key-1",
            "deadline": null,
            "noduleCapability": null,
        }))
    );
    // A deadline in the future travels as a header.
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let context = InvocationContext {
        deadline_unix_ms: Some(deadline),
        ..Default::default()
    };
    let echoed = invoke(&node, &echo, json!({}), context).await.unwrap();
    assert_eq!(echoed["deadline"], json!(deadline.to_string()));
    assert_eq!(echoed["idempotencyKey"], Value::Null);

    // A deadline already behind us fails in dispatch before the handler is entered.
    let elapsed = InvocationContext {
        deadline_unix_ms: Some(1),
        ..Default::default()
    };
    assert_eq!(
        invoke(&node, &echo, json!({}), elapsed).await,
        Err(DispatchError::DeadlineExceeded)
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
    assert!(!node.wasm.hosts(&spec.plug));
    set_enabled(&node, &spec, true).await;
    assert!(node.wasm.hosts(&spec.plug));
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
async fn relay(
    node: &BigNode,
    from: &Spec,
    operation: &str,
    authority: Option<&str>,
    context: InvocationContext,
) -> Value {
    let mut input = json!({ "operation": operation, "input": { "n": 1 } });
    if let Some(authority) = authority {
        input["authority"] = json!(authority.strip_prefix("Capability ").unwrap_or(authority));
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
        admit(&node, plug(spec, &bytes, calls)).await;
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
    assert!(first.operations.as_ref().unwrap().granted);
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
    assert_eq!(relayed["body"]["authorization"], json!(granted));
    assert_eq!(relayed["body"]["idempotencyKey"], "nested-key");
    // B has no grants of its own, so it was handed no capability to confuse with A's.
    assert_eq!(relayed["body"]["noduleCapability"], Value::Null);

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

    // Revoking by a config change takes effect: the handler is told no capability, and the one
    // it remembered is dead.
    let revoked = set_operations(&node, &a, DispatchScope::default()).await;
    assert!(revoked.operations.is_none());
    assert_eq!(own_capability(&node, &a).await, None);
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
        record(&node, &a)
            .await
            .operations
            .map(|status| status.granted),
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
    admit(&node, plug(&spec, &bytes, &[])).await;
    let waiting = set_enabled(&node, &spec, true).await;
    assert_eq!(waiting.readiness, Readiness::ArtifactsUnavailable);
    assert!(!node.wasm.hosts(&spec.plug));
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
