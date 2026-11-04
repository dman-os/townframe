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
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use big_nodule_core::dispatch::DispatchError;
use big_nodule_core::dispatch::InvocationContext;
use big_nodule_core::dispatch::Invoke;
use big_nodule_core::dispatch::NotReadyReason;
use big_nodule_core::inventory::Readiness;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_plugs_core::Artifact;
use big_plugs_core::Contribution;
use big_plugs_core::Digest;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use big_plugs_core::PlugId;
use service_utils_rs::ClientError;
use std::time::Duration;

const PLUG: &str = "@example/wasm-test";
const ECHO: &str = "org.example.test.echo";
const RELAY: &str = "org.example.test.relay";
/// Implemented by the nodule but not served by the guest, which answers 404 for it.
const ABSENT: &str = "org.example.test.absent";

fn plug_id() -> PlugId {
    PLUG.parse().unwrap()
}

fn guest_bytes() -> Vec<u8> {
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

/// The plug whose nodule owns `org.example.test` and implements all three operations through
/// the handler artifact `bytes`.
fn plug(bytes: &[u8]) -> Plug {
    let mut nodule = Nodule::new("org.example.test");
    for nsid in [ECHO, RELAY, ABSENT] {
        nodule
            .lexicons
            .push(serde_json::from_value(procedure(nsid)).unwrap());
    }
    nodule.handler = Some(Handler {
        artifact: "handler".into(),
        extra: Default::default(),
    });
    nodule.implements = vec![ECHO.into(), RELAY.into(), ABSENT.into()];
    let mut plug = Plug::new(plug_id(), "1.0.0".parse().unwrap());
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

async fn put(node: &BigNode, bytes: &[u8]) {
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

async fn admit(node: &BigNode, plug: Plug) -> NoduleRecord {
    node.nodules
        .client
        .call(
            RequestContext::root(),
            AdmitNodule {
                authorization: Authorization::identity(node.authority.root),
                plug,
                drawers: None,
            },
        )
        .await
        .unwrap()
}

async fn set_enabled(node: &BigNode, enabled: bool) -> NoduleRecord {
    node.nodules
        .client
        .call(
            RequestContext::root(),
            SetNoduleEnabled {
                authorization: Authorization::identity(node.authority.root),
                plug_id: plug_id(),
                enabled,
            },
        )
        .await
        .unwrap()
}

async fn record(node: &BigNode) -> NoduleRecord {
    node.nodules
        .client
        .call(
            RequestContext::root(),
            GetNodule {
                authorization: Authorization::identity(node.authority.root),
                plug_id: plug_id(),
            },
        )
        .await
        .unwrap()
}

async fn invoke(
    node: &BigNode,
    operation: &str,
    input: Value,
    context: InvocationContext,
) -> Result<Value, DispatchError> {
    node.dispatch
        .client
        .call(
            RequestContext::root(),
            Invoke {
                authorization: Authorization::identity(node.authority.root),
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

async fn boot_node(secrets: &SecretStore) -> (BigNode, StopToken) {
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    BigNode::boot(config(secrets, &runtime, sql, StorageConfig::Memory))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_wasm_handler_serves_dispatched_operations_until_disabled() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    put(&node, &bytes).await;
    admit(&node, plug(&bytes)).await;
    let enabled = set_enabled(&node, true).await;
    assert_eq!(enabled.readiness, Readiness::Ready);
    assert!(node.wasm.hosts(&plug_id()));

    // The echo returns the input, and the headers the host built for the guest: the operator's
    // capability as the authority, and the idempotency key.
    let operator_cap = serde_json::to_value(node.authority.root).unwrap();
    let context = InvocationContext {
        idempotency_key: Some("key-1".into()),
        ..Default::default()
    };
    assert_eq!(
        invoke(&node, ECHO, json!({ "n": 1 }), context).await,
        Ok(json!({
            "input": { "n": 1 },
            "authorization": format!("Capability {}", operator_cap.as_str().unwrap()),
            "idempotencyKey": "key-1",
            "deadline": null,
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
    let echoed = invoke(&node, ECHO, json!({}), context).await.unwrap();
    assert_eq!(echoed["deadline"], json!(deadline.to_string()));
    assert_eq!(echoed["idempotencyKey"], Value::Null);

    // Not asserted here: that an already-elapsed deadline yields `DeadlineExceeded`. Dispatch
    // polls the provider once before its timer (see the dispatch e2e), and tokio's timer only
    // fires on the driver's next millisecond tick, so a guest that answers within that tick
    // wins the race. The deadline path is covered with a gated provider in `big_nodule`.

    // The guest's own refusal of an operation it does not serve is the provider's error.
    assert_eq!(
        invoke(&node, ABSENT, json!({}), Default::default()).await,
        Err(DispatchError::Provider {
            status: 404,
            body: json!({ "error": "UnknownOperation" }),
        })
    );

    // Handlers have no egress in this slice: the relay's `dispatch.local` call is refused and
    // the guest reports its upstream failure. Slice 2c replaces this assertion.
    let relayed = invoke(
        &node,
        RELAY,
        json!({ "operation": ECHO, "authority": operator_cap, "input": {} }),
        Default::default(),
    )
    .await;
    let Err(DispatchError::Provider { status: 502, body }) = relayed else {
        panic!("the relay's egress must be refused: {relayed:?}");
    };
    assert_eq!(body["error"], "UpstreamFailed");

    // Disabling unbinds the operations and stops the workload; enabling starts it again.
    set_enabled(&node, false).await;
    assert_eq!(
        invoke(&node, ECHO, json!({}), Default::default()).await,
        Err(DispatchError::UnknownOperation)
    );
    assert!(!node.wasm.hosts(&plug_id()));
    set_enabled(&node, true).await;
    assert!(node.wasm.hosts(&plug_id()));
    assert_eq!(
        invoke(&node, ECHO, json!({ "n": 2 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 2 })
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn a_handler_whose_bytes_arrive_late_becomes_ready_when_they_do() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    admit(&node, plug(&bytes)).await;
    let waiting = set_enabled(&node, true).await;
    assert_eq!(waiting.readiness, Readiness::ArtifactsUnavailable);
    assert!(!node.wasm.hosts(&plug_id()));
    assert_eq!(
        invoke(&node, ECHO, json!({}), Default::default()).await,
        Err(DispatchError::NotReady {
            reason: NotReadyReason::Provider(Readiness::ArtifactsUnavailable),
        })
    );

    // The blob service's availability feed wakes the executor, which asks the state actor to
    // ensure again: no reconcile trigger from the test. (The test itself can only wait for
    // the state to change by looking; there is no signal for "reconciled" to subscribe to.)
    put(&node, &bytes).await;
    while record(&node).await.readiness != Readiness::Ready {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        invoke(&node, ECHO, json!({ "n": 3 }), Default::default())
            .await
            .unwrap()["input"],
        json!({ "n": 3 })
    );
    shutdown(node, stop).await;
}
