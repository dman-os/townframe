//! One router per node (ADR 032 §4): a built-in operation (docs) and a nodule operation (the
//! Wasm test guest) are invoked through the same `Client<DispatchProtocol>`, under the same
//! admission, and the built-in service keeps checking the capability in its own request.

use super::nodule_wasm::admit;
use super::nodule_wasm::boot_node;
use super::nodule_wasm::caller;
use super::nodule_wasm::guest_bytes;
use super::nodule_wasm::invoke;
use super::nodule_wasm::plug;
use super::nodule_wasm::put;
use super::nodule_wasm::set_enabled;
use super::*;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::WriteReceipt;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::MintDispatchCapability;
use big_nodule_core::protocol::NoduleRecord;
use service_utils_rs::ClientError;
use service_utils_rs::Operation;

/// The operator's identity narrowed to a dispatch handle that covers `scope` only.
pub(super) async fn narrowed(node: &BigNode, scope: DispatchScope) -> Authorization {
    let operator = Authorization::identity(node.authority.root);
    let capability = node
        .dispatch
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            MintDispatchCapability {
                scope,
                rights: Rights::USE | Rights::NARROW,
            },
        )
        .await
        .unwrap();
    Authorization {
        authority: capability.capability_id,
        signer: operator.signer,
    }
}

pub(super) fn operations(nsids: &[&str]) -> DispatchScope {
    DispatchScope {
        operations: nsids.iter().map(|nsid| (*nsid).to_owned()).collect(),
        ..Default::default()
    }
}

async fn call(
    node: &BigNode,
    authorization: Authorization,
    operation: &str,
    input: Value,
) -> Result<Value, DispatchError> {
    node.dispatch
        .client
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            big_dispatch_core::Invoke {
                operation: operation.to_owned(),
                input,
                context: Default::default(),
            },
        )
        .await
        .map(|output| output.output)
        .map_err(|error| match error {
            ClientError::Operation(error) => error,
            ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
        })
}

#[tokio::test]
async fn the_built_in_services_claim_their_declared_operations_without_collisions() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let router = &node.dispatch.router;
    for (provider_id, expected) in [
        ("builtin:docs", CreateDoc::XRPC.nsid),
        ("builtin:docs", ReadDatums::XRPC.nsid),
        (
            "builtin:drawers",
            big_cabinet_core::drawer::protocol::CreateDrawer::XRPC.nsid,
        ),
        ("builtin:blobs", big_blobs_core::PutBlob::XRPC.nsid),
        (
            "builtin:authority",
            big_cabinet_core::authority::OpenActor::XRPC.nsid,
        ),
        (
            "builtin:plugs",
            big_plugs_core::protocol::PublishPlugVersion::XRPC.nsid,
        ),
        (
            "builtin:nodules",
            big_nodule_core::protocol::AdmitNodule::XRPC.nsid,
        ),
    ] {
        let claimed = router.operations_of(provider_id);
        assert!(
            claimed.iter().any(|nsid| nsid == expected),
            "{provider_id} does not claim {expected}: {claimed:?}"
        );
        // A claim shared with another provider would make the NSID ambiguous: unroutable.
        for nsid in claimed {
            assert_eq!(router.claimants(&nsid), [provider_id], "{nsid}");
        }
    }
    shutdown(node, stop).await;
}

#[tokio::test]
async fn built_in_and_nodule_operations_share_one_router_and_one_admission() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let spec = caller();
    let echo = spec.echo();
    put(&node, &bytes).await;
    admit(&node, plug(&spec, &bytes, &[])).await;
    let record: NoduleRecord = set_enabled(&node, &spec, true).await;
    assert_eq!(record.readiness, big_dispatch_core::Readiness::Ready);

    let operator = Authorization::identity(node.authority.root);
    let (create_op, read_op) = (CreateDoc::XRPC.nsid, ReadDatums::XRPC.nsid);

    // The operator is admitted everywhere. The docs call is the docs service's own: the
    // capability inside the request is minted by the docs service and checked by it.
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let creating = capability(
        &node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let value = json!({"$type": NOTE, "value": "through dispatch"});
    let receipt: WriteReceipt = serde_json::from_value(
        call(
            &node,
            operator,
            create_op,
            serde_json::to_value(CreateDoc {
                capability: creating,
                command: CommandId::try_from("dispatched-note".to_owned()).unwrap(),
                placement,
                datums: BTreeMap::from([(key(), value.clone())]),
            })
            .unwrap(),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    let stored = read(&node, &receipt.doc_id).await;
    assert!(
        matches!(&stored.datums[&key()], DatumRead::Accepted { value: accepted, .. } if *accepted == value)
    );
    assert!(
        invoke(&node, &echo, json!({ "n": 1 }), Default::default())
            .await
            .is_ok()
    );

    // A handle scoped to the docs read admits that operation and nothing else, built-in or
    // nodule alike.
    let reading = capability(
        &node,
        DocScope {
            read: BTreeSet::from([receipt.doc_id.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let read_input = serde_json::to_value(ReadDatums {
        capability: reading,
        doc_id: receipt.doc_id.clone(),
        at: At::Current,
        datums: Some(BTreeSet::from([key()])),
        optimistic: false,
    })
    .unwrap();
    let docs_reader = narrowed(&node, operations(&[read_op])).await;
    assert_eq!(
        call(&node, docs_reader, read_op, read_input.clone())
            .await
            .unwrap(),
        serde_json::to_value(&stored).unwrap()
    );
    assert_eq!(
        call(&node, docs_reader, &echo, json!({})).await,
        Err(DispatchError::Denied)
    );
    assert_eq!(
        call(&node, docs_reader, create_op, json!({})).await,
        Err(DispatchError::Denied)
    );

    // A handle scoped to the nodule's authority admits the nodule and denies the built-in.
    let nodule_caller = narrowed(
        &node,
        DispatchScope {
            authorities: [spec.authority.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    assert!(call(&node, nodule_caller, &echo, json!({})).await.is_ok());
    assert_eq!(
        call(&node, nodule_caller, read_op, read_input.clone()).await,
        Err(DispatchError::Denied)
    );

    // Admission is not authorization: the handle admits `readDatums`, but the docs service
    // still refuses a request whose own capability does not cover the document, and says so
    // in the XRPC domain envelope.
    let other = create(&node, "other-note", value.clone()).await;
    let unrelated = capability(
        &node,
        DocScope {
            read: BTreeSet::from([other.doc_id]),
            ..DocScope::default()
        },
    )
    .await;
    let mut forged = read_input.clone();
    forged["capability"] = serde_json::to_value(unrelated).unwrap();
    let Err(DispatchError::Provider { status, body }) =
        call(&node, docs_reader, read_op, forged).await
    else {
        panic!("the docs service must refuse a capability that does not cover the document");
    };
    assert_eq!(status, 400);
    assert_eq!(body["operationError"], json!("Denied"));

    // Disabling the nodule unroutes only its operations; the built-ins stay.
    set_enabled(&node, &spec, false).await;
    assert_eq!(
        call(&node, operator, &echo, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    assert!(call(&node, docs_reader, read_op, read_input).await.is_ok());
    shutdown(node, stop).await;
}
