use crate::DocId;
use crate::authority::{
    Actor, AuthorityProtocol, CapId, Capability, MintCapability, Namespace, ResolveAuthority,
    ResolvedAuthority, Rights,
};
use crate::dmeta::{Bytes, MultibaseKey, Uuid7};
use crate::doc::{At, DocsProtocol, ObserveFacets, ObserveScope, ReadFacets};
use crate::drawer::protocol::{DrawersProtocol, GetDrawer};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use service_core_rs::{
    Envelope, EventReply, HttpInput, HttpMethod, Operation, Reply, RequestContext, Subscription,
    Unary, XrpcKind,
};

fn wire<T: Serialize + DeserializeOwned>(value: &T, expected: Value) {
    let encoded = serde_json::to_value(value).unwrap();
    assert_eq!(encoded, expected);
    let decoded: T = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
}

#[test]
fn core_protocol_metadata_wire() {
    for (operations, namespace, error_schema) in [
        (
            AuthorityProtocol::OPERATIONS,
            "org.example.daybook.authority.",
            "AuthorityServiceError",
        ),
        (
            DocsProtocol::OPERATIONS,
            "org.example.daybook.docs.",
            "DocsError",
        ),
        (
            DrawersProtocol::OPERATIONS,
            "org.example.daybook.drawers.",
            "DrawersError",
        ),
    ] {
        assert!(!operations.unary.is_empty());
        let mut nsids = std::collections::BTreeSet::new();
        for operation in operations.unary.iter().chain(operations.stream) {
            assert!(operation.nsid.starts_with(namespace));
            assert!(nsids.insert(operation.nsid));
            assert!(!operation.output_schema.is_empty());
            assert_eq!(operation.error_schema, error_schema);
            let (method, input) = match operation.kind {
                XrpcKind::Query | XrpcKind::Subscription => (HttpMethod::Get, HttpInput::Query),
                XrpcKind::Procedure => (HttpMethod::Post, HttpInput::JsonBody),
            };
            assert_eq!((operation.method, operation.input), (method, input));
        }
        assert!(
            operations
                .unary
                .iter()
                .all(|operation| operation.kind != XrpcKind::Subscription)
        );
        assert!(
            operations
                .stream
                .iter()
                .all(|operation| operation.kind == XrpcKind::Subscription)
        );
    }
    assert!(
        AuthorityProtocol::OPERATIONS
            .unary
            .contains(&MintCapability::XRPC)
    );
    assert!(DocsProtocol::OPERATIONS.unary.contains(&ReadFacets::XRPC));
    assert!(
        DocsProtocol::OPERATIONS
            .stream
            .contains(&ObserveFacets::XRPC)
    );
    assert!(DrawersProtocol::OPERATIONS.unary.contains(&GetDrawer::XRPC));
    assert_eq!(ObserveFacets::XRPC.output_schema, "FacetObserved");
    assert!(
        AuthorityProtocol::OPERATIONS
            .unary
            .contains(&ResolveAuthority::XRPC)
    );
    assert!(AuthorityProtocol::OPERATIONS.unary.iter().all(|operation| {
        !["grantAccess", "revokeAccess", "checkGrant", "checkGrants"]
            .iter()
            .any(|name| operation.nsid.ends_with(name))
    }));
    assert_eq!(
        ResolveAuthority::XRPC.nsid,
        "org.example.daybook.authority.resolveAuthority"
    );
    assert_eq!(ResolveAuthority::XRPC.kind, XrpcKind::Query);
    assert_eq!(ResolveAuthority::XRPC.output_schema, "ResolvedAuthority");
    assert_eq!(
        MintCapability::XRPC.nsid,
        "org.example.daybook.authority.mintCapability"
    );
    assert_eq!(MintCapability::XRPC.kind, XrpcKind::Procedure);
    assert_eq!(MintCapability::XRPC.output_schema, "Capability");
}

#[test]
fn core_protocol_channels_wire() {
    fn unary<P, Proto>()
    where
        P: Unary,
        Proto: irpc::Service,
        Envelope<P>: irpc::Channels<Proto, Tx = Reply<P>>,
    {
    }
    fn stream<P, Proto>()
    where
        P: Subscription,
        Proto: irpc::Service,
        Envelope<P>: irpc::Channels<Proto, Tx = EventReply<P>>,
    {
    }
    unary::<MintCapability, AuthorityProtocol>();
    unary::<ResolveAuthority, AuthorityProtocol>();
    unary::<ReadFacets, DocsProtocol>();
    unary::<GetDrawer, DrawersProtocol>();
    stream::<ObserveFacets, DocsProtocol>();
}

#[test]
fn authority_identifiers_wire() {
    let capability_id = CapId::from_bytes([1; 16]);
    let actor_id =
        Uuid7::try_from(uuid::Uuid::parse_str("01940000-0000-7000-8000-000000000001").unwrap())
            .unwrap();
    wire(
        &Capability {
            capability_id,
            object: "read document".into(),
            rights: Rights::USE | Rights::NARROW,
            issuer_id: actor_id,
        },
        json!({
            "capabilityId": "01010101-0101-0101-0101-010101010101",
            "object": "read document",
            "rights": ["use", "narrow"],
            "issuerId": actor_id,
        }),
    );
    let public_key = MultibaseKey::try_from(&[2u8; 32][..]).unwrap();
    let certificate = Bytes::from(vec![3, 4]);
    wire(
        &Actor {
            actor_id,
            url: "db+actor://node/child".into(),
            public_key: public_key.clone(),
            certificate: certificate.clone(),
            parent_actor_id: Some(actor_id),
            capability: capability_id,
        },
        json!({
            "actorId": actor_id,
            "url": "db+actor://node/child",
            "publicKey": public_key,
            "certificate": certificate,
            "parentActorId": actor_id,
            "capability": capability_id,
        }),
    );
    wire(
        &Namespace {
            namespace_id: actor_id,
            capability: capability_id,
        },
        json!({"namespaceId": actor_id, "capability": capability_id}),
    );
    assert_eq!(format!("{capability_id:?}"), "CapId([redacted])");
    assert_eq!(CapId::from_bytes(*capability_id.as_bytes()), capability_id);
}

#[test]
fn authority_resolution_and_mint_wire() {
    let capability_id = CapId::from_bytes([1; 16]);
    let actor_id =
        Uuid7::try_from(uuid::Uuid::parse_str("01940000-0000-7000-8000-000000000001").unwrap())
            .unwrap();
    wire(
        &ResolveAuthority {
            capability: capability_id,
        },
        json!({"capability": capability_id}),
    );
    let capability = Capability {
        capability_id,
        object: "opaque application object".into(),
        rights: Rights::USE,
        issuer_id: actor_id,
    };
    let actor = Actor {
        actor_id,
        url: "db+actor://node/child".into(),
        public_key: MultibaseKey::try_from(&[2u8; 32][..]).unwrap(),
        certificate: Bytes::from(vec![3, 4]),
        parent_actor_id: None,
        capability: capability_id,
    };
    for actor in [None, Some(actor)] {
        let response: <ResolveAuthority as Operation>::Output = ResolvedAuthority {
            capability: capability.clone(),
            actor: actor.clone(),
        };
        wire(&response, json!({"capability": capability, "actor": actor}));
    }
    let source = CapId::from_bytes([2; 16]);
    let signer = CapId::from_bytes([3; 16]);
    wire(
        &MintCapability {
            issuer: capability_id,
            parent: Some(source),
            dependencies: vec![signer],
            object: "opaque domain data".into(),
            rights: Rights::USE,
        },
        json!({
            "issuer": capability_id,
            "parent": source,
            "dependencies": [signer],
            "object": "opaque domain data",
            "rights": ["use"],
        }),
    );
}

#[test]
fn rights_wire_rejects_unknown_vocabulary() {
    assert!(serde_json::from_value::<Rights>(json!(["futureRight"])).is_err());
    for (bits, expected) in [
        (0, json!([])),
        (1, json!(["use"])),
        (3, json!(["use", "narrow"])),
        (15, json!(["use", "narrow", "mintChild", "delegate"])),
    ] {
        let rights = Rights::from_bits(bits).unwrap();
        assert_eq!(rights.bits_i64(), bits);
        wire(&rights, expected);
    }
    for bits in [-1, 16, i64::MAX] {
        assert_eq!(Rights::from_bits(bits), None);
    }
}

#[test]
fn core_protocol_envelopes_wire() {
    let capability = CapId::from_bytes([1; 16]);
    let context = RequestContext::root();
    let expected = |variant: &str, params: Value| {
        json!({(variant): {
            "context": context,
            "metadata": {"authorization": null, "idempotency_key": null},
            "params": params,
        }})
    };
    let authority = AuthorityProtocol::from(Envelope::new(
        context.clone(),
        MintCapability {
            issuer: capability,
            parent: None,
            dependencies: Vec::new(),
            object: "read document".into(),
            rights: Rights::USE,
        },
    ));
    wire(
        &authority,
        expected(
            "MintCapability",
            json!({
                "issuer": capability, "parent": null, "dependencies": [],
                "object": "read document", "rights": ["use"],
            }),
        ),
    );
    let resolved = AuthorityProtocol::from(Envelope::new(
        context.clone(),
        ResolveAuthority { capability },
    ));
    wire(
        &resolved,
        expected("ResolveAuthority", json!({"capability": capability})),
    );
    let doc_id = DocId::new([2; 32]);
    let docs = DocsProtocol::from(Envelope::new(
        context.clone(),
        ReadFacets {
            capability,
            doc_id: doc_id.clone(),
            at: At::Current,
            facets: None,
            optimistic: false,
        },
    ));
    wire(
        &docs,
        expected(
            "ReadFacets",
            json!({
                "capability": capability,
                "docId": doc_id, "at": {"at": "current"}, "optimistic": false,
            }),
        ),
    );
    let observed = DocsProtocol::from(Envelope::new(
        context.clone(),
        ObserveFacets {
            capability,
            scope: ObserveScope::Doc {
                doc_id: doc_id.clone(),
            },
            after: 0,
        },
    ));
    wire(
        &observed,
        expected(
            "ObserveFacets",
            json!({
                "capability": capability,
                "scope": {"scope": "doc", "docId": doc_id}, "after": 0,
            }),
        ),
    );
    let drawers = DrawersProtocol::from(Envelope::new(
        context.clone(),
        GetDrawer {
            capability,
            drawer_id: doc_id.clone(),
        },
    ));
    wire(
        &drawers,
        expected(
            "GetDrawer",
            json!({
                "capability": capability,
                "drawerId": doc_id,
            }),
        ),
    );
}
