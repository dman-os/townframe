//! One router per node (ADR 033 §4): a built-in operation (docs) and a nodule operation (the
//! Wasm test guest) are invoked through the same `Client<DispatchProtocol>`, under the same
//! admission, and the built-in service keeps checking the capability in its own request.

use super::nodule_wasm::admit;
use super::nodule_wasm::boot_node;
use super::nodule_wasm::caller;
use super::nodule_wasm::cartridge;
use super::nodule_wasm::guest_bytes;
use super::nodule_wasm::invoke;
use super::nodule_wasm::put;
use super::nodule_wasm::set_enabled;
use super::*;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::WriteReceipt;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::MintDispatchCapability;
use big_nodule_core::protocol::NoduleRecord;
use method_core::ByteBody;
use method_core::Method;
use service_utils_rs::ClientError;

/// The operator's root capability narrowed to a dispatch handle that covers `scope` only.
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
                subject: None,
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
            big_dispatch_core::Invoke::json(operation, input, Default::default()),
        )
        .await
        .map(|output| output.body.into_json())
        .map_err(|error| match error {
            ClientError::Method(error) => error,
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
            "builtin:cartridges",
            big_cartridges_core::protocol::PublishCartridgeVersion::XRPC.nsid,
        ),
        (
            "builtin:nodules",
            big_cartridges_core::protocol::InstallCartridge::XRPC.nsid,
        ),
        ("builtin:node", crate::GET_NODE_INFO),
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
    // The methods that introduce authority or manage nodules carry the privileged declaration into
    // their routes, and those that only narrow what the caller presents do not: a nodule's scope,
    // which never holds privilege, reaches exactly the second kind.
    {
        use big_cabinet_core::authority as authority_methods;
        use big_cartridges_core::protocol as cartridge_methods;
        use big_nodule_core::protocol as nodule_methods;
        for (nsid, privileged) in [
            (authority_methods::OpenActor::XRPC.nsid, false),
            (authority_methods::MintCapability::XRPC.nsid, true),
            (authority_methods::AttenuateCapability::XRPC.nsid, false),
            (authority_methods::RetractCapability::XRPC.nsid, false),
            (cartridge_methods::InstallCartridge::XRPC.nsid, true),
            (cartridge_methods::SetCartridgeEnabled::XRPC.nsid, true),
            (cartridge_methods::UninstallCartridge::XRPC.nsid, true),
            (cartridge_methods::SetCartridgeGrant::XRPC.nsid, true),
            (cartridge_methods::ClearCartridgeConfig::XRPC.nsid, true),
            (cartridge_methods::ListCartridgeInstalls::XRPC.nsid, false),
            // Filing a request and awaiting its answer are a cartridge's own and reachable by its
            // routines; answering and listing the answers are the operator's.
            (cartridge_methods::RequestCartridgeGrant::XRPC.nsid, false),
            (
                cartridge_methods::AwaitCartridgeGrantRequest::XRPC.nsid,
                false,
            ),
            (
                cartridge_methods::ListCartridgeGrantRequests::XRPC.nsid,
                true,
            ),
            (
                cartridge_methods::AnswerCartridgeGrantRequest::XRPC.nsid,
                true,
            ),
            (cartridge_methods::ListCartridgeGrants::XRPC.nsid, true),
            (cartridge_methods::AwaitCartridgeGrants::XRPC.nsid, true),
            (nodule_methods::SetNoduleDevTakeover::XRPC.nsid, true),
            (nodule_methods::ListNodules::XRPC.nsid, false),
            (nodule_methods::MintNoduleCapability::XRPC.nsid, false),
            (big_blobs_core::PutBlob::XRPC.nsid, false),
        ] {
            assert_eq!(
                router
                    .resolve(nsid)
                    .unwrap_or_else(|error| panic!("{nsid}: {error:?}"))
                    .privileged,
                privileged,
                "{nsid}"
            );
        }
    }
    shutdown(node, stop).await;
}

#[tokio::test]
async fn get_node_info_reports_the_drawers_the_node_booted_with() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let output = node
        .dispatch
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            big_dispatch_core::Invoke::json(crate::GET_NODE_INFO, json!({}), Default::default()),
        )
        .await
        .unwrap();
    let info: crate::NodeInfo = serde_json::from_value(output.body.into_json()).unwrap();

    let local = node.drawers.local();
    assert_eq!(info.drawers.present, local.present_id);
    assert_eq!(info.drawers.originated, local.originated_id);
    let reported: BTreeMap<_, _> = info
        .drawers
        .config
        .iter()
        .map(|drawer| (drawer.role.as_str(), drawer.drawer_id.clone()))
        .collect();
    assert_eq!(
        reported,
        BTreeMap::from([
            ("known drawers", local.known_drawers_id),
            ("node config", local.config_id),
            ("cartridge registry", node.registry_drawer.clone()),
            (
                "cartridge config",
                node.nodules.shared_config_drawer.clone()
            ),
        ])
    );
    stop.stop().await.unwrap();
}

#[tokio::test]
async fn built_in_and_nodule_operations_share_one_router_and_one_admission() {
    let bytes = guest_bytes();
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let spec = caller();
    let echo = spec.echo();
    put(&node, &bytes).await;
    admit(&node, cartridge(&spec, &bytes, &[])).await;
    let record: NoduleRecord = set_enabled(&node, &spec, true).await;
    assert_eq!(record.readiness, big_dispatch_core::Readiness::Ready);

    let operator = Authorization::identity(node.authority.root);
    let (create_op, read_op) = (CreateDoc::XRPC.nsid, ReadDatums::XRPC.nsid);

    // The operator is admitted everywhere. The docs call is the docs service's own: the
    // capability inside the request is minted by the docs service and checked by it.
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let creating = operator_capability(
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
    let reading = operator_capability(
        &node,
        DocScope {
            read: DocScope::read_all([receipt.doc_id.clone()]),
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
    let unrelated = operator_capability(
        &node,
        DocScope {
            read: DocScope::read_all([other.doc_id]),
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

/// A second actor under the operator, with a dispatch handle that admits `operations`: the
/// handle is bound to it and it presents it under its own signer, so dispatch admission passes.
async fn second_caller(
    node: &BigNode,
    operations: &[&str],
) -> (big_cabinet_core::dmeta::Uuid7, Authorization) {
    let opened = node
        .authority
        .client
        .call(
            RequestContext::root(),
            big_cabinet_core::authority::OpenActor {
                parent: node.authority.root,
                path: "second".into(),
                rights: Rights::all(),
            },
        )
        .await
        .unwrap();
    let operator = Authorization::identity(node.authority.root);
    let handle = node
        .dispatch
        .client
        .call_with(
            RequestContext::root(),
            operator.metadata(),
            MintDispatchCapability {
                scope: self::operations(operations),
                rights: Rights::USE,
                subject: Some(opened.actor.actor_id),
            },
        )
        .await
        .unwrap()
        .capability_id;
    (
        opened.actor.actor_id,
        Authorization {
            authority: handle,
            signer: opened.capability,
        },
    )
}

/// One dispatch call whose answer may be bytes. `parameters` are the call's JSON, or the Lexicon
/// parameters of a method whose body is `bytes`.
async fn raw(
    node: &BigNode,
    authorization: Authorization,
    operation: &str,
    parameters: Value,
    bytes: Option<ByteBody>,
) -> Result<big_dispatch_core::Body, DispatchError> {
    let invoke = match bytes {
        Some(bytes) => big_dispatch_core::Invoke {
            operation: operation.to_owned(),
            parameters,
            body: big_dispatch_core::Body::Bytes(bytes),
            context: Default::default(),
        },
        None => big_dispatch_core::Invoke::json(operation, parameters, Default::default()),
    };
    node.dispatch
        .client
        .call_with(RequestContext::root(), authorization.metadata(), invoke)
        .await
        .map(|output| output.body)
        .map_err(|error| match error {
            ClientError::Method(error) => error,
            ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
        })
}

/// ADR 037 §2: a handle is bound to its holder in every domain, and a leaked id is a dead number
/// under another actor's signer. Dispatch admits the second actor on its own handle, but the
/// resource capability in the request body is another actor's, and the domain must refuse it.
#[tokio::test]
async fn a_body_capability_presented_by_an_actor_it_is_not_bound_to_is_refused() {
    let secrets = secrets().await;
    let (node, stop) = boot_node(&secrets).await;
    let (read_op, write_op) = (
        ReadDatums::XRPC.nsid,
        big_cabinet_core::doc::WriteDatums::XRPC.nsid,
    );
    let (read_blob, put_blob) = (
        big_blobs_core::ReadBlob::XRPC.nsid,
        big_blobs_core::PutBlob::XRPC.nsid,
    );

    let stored = create(&node, "held-note", json!({"$type": NOTE, "value": "a's"})).await;
    let info = super::blobs::put_bytes(&node, b"a's blob").await;
    let (second, caller) = second_caller(&node, &[read_op, write_op, read_blob, put_blob]).await;

    let refused =
        |outcome: Result<big_dispatch_core::Body, DispatchError>, what: &str| match outcome {
            Err(DispatchError::Provider { body, .. }) => {
                assert!(
                    matches!(
                        body["operationError"].as_str(),
                        Some("Denied" | "Unauthorized")
                    ),
                    "{what}: refused, but as {body}"
                );
            }
            other => panic!("{what}: another actor's handle must be refused, got {other:?}"),
        };

    // Docs: the capability is bound to the drawers actor, the presenter is the second actor.
    let doc_read = operator_capability(
        &node,
        DocScope {
            read: DocScope::read_all([stored.doc_id.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let read_input = |capability| {
        serde_json::to_value(ReadDatums {
            capability,
            doc_id: stored.doc_id.clone(),
            at: At::Current,
            datums: Some(BTreeSet::from([key()])),
            optimistic: false,
        })
        .unwrap()
    };
    refused(
        raw(&node, caller, read_op, read_input(doc_read), None).await,
        "docs read",
    );

    // A write signs as the handle's actor, so presenting another actor's write handle would
    // forge that actor's signature.
    let doc_write = operator_capability(
        &node,
        DocScope {
            write: BTreeMap::from([(
                stored.doc_id.clone(),
                big_cabinet_core::doc::DatumKinds::All,
            )]),
            ..DocScope::default()
        },
    )
    .await;
    refused(
        raw(
            &node,
            caller,
            write_op,
            serde_json::to_value(big_cabinet_core::doc::WriteDatums {
                capability: doc_write,
                command: None,
                doc_id: stored.doc_id.clone(),
                mode: big_cabinet_core::doc::WriteMode::AtCurrent,
                set: BTreeMap::from([(key(), json!({"$type": NOTE, "value": "forged"}))]),
                remove: BTreeSet::new(),
            })
            .unwrap(),
            None,
        )
        .await,
        "docs write",
    );

    // Blobs: the capabilities are bound to the operator's actor.
    let blob_read = super::blobs::read_cap(&node, &info.blob_id).await;
    refused(
        raw(
            &node,
            caller,
            read_blob,
            json!({"capability": blob_read, "blobId": info.blob_id}),
            None,
        )
        .await,
        "blobs read",
    );
    let blob_put = super::blobs::put_cap(&node).await;
    refused(
        raw(
            &node,
            caller,
            put_blob,
            json!({"capability": blob_put}),
            Some(ByteBody::from_bytes(b"b's".to_vec())),
        )
        .await,
        "blobs put",
    );

    // Controls: the same operations succeed under handles bound to the second actor, so the
    // refusals above are the holder check and nothing else.
    let own_doc_read = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            MintDocCapability {
 capability: None,
                scope: DocScope {
                    read: DocScope::read_all([stored.doc_id.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: Some(second),
            },
        )
        .await
        .unwrap()
        .capability_id;
    raw(&node, caller, read_op, read_input(own_doc_read), None)
        .await
        .unwrap();
    let own_read = super::blobs::read_cap_for(&node, &info.blob_id, Some(second)).await;
    raw(
        &node,
        caller,
        read_blob,
        json!({"capability": own_read, "blobId": info.blob_id}),
        None,
    )
    .await
    .unwrap();
    shutdown(node, stop).await;
}
