//! Fixtures for tests that boot a [`crate::NodeComposition`]: a schema repo for the notes they
//! create, drawers and notes made as the node's drawers actor does, and a `readDatums` call as a
//! micro makes it. Shared with the hosts' tests behind the `test-support` feature.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use bytes::Bytes;
use clay_guest_session::egress::DispatchEgress;
use http_body_util::BodyExt;
use http_body_util::Full;
use method_core::Method as _;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

/// A vocabulary of no datum kinds.
pub fn no_schemas() -> Arc<big_cabinet::SchemaRepo> {
    Arc::new(big_cabinet::SchemaRepo::open(std::iter::empty()).unwrap())
}

/// A vocabulary of one datum kind, [`NOTE`].
pub fn note_schemas() -> Arc<big_cabinet::SchemaRepo> {
    Arc::new(
        big_cabinet::SchemaRepo::open([big_cabinet::SchemaRegistrationGroup::new(
            None,
            [big_cabinet::SchemaRegistration::new(
                NOTE,
                json!({
                    "lexicon": 1,
                    "id": NOTE,
                    "defs": {"main": {"type": "record", "key": "tid", "record": {
                        "type": "object",
                        "required": ["value"],
                        "properties": {"value": {"type": "string"}}
                    }}}
                }),
            )],
            Vec::new(),
        )])
        .unwrap(),
    )
}

pub const NOTE: &str = "org.example.note";

/// Creates a drawer of its own as the node's drawers actor does. (The node's two required local
/// drawers hold every document, so a document in one is in the other.)
pub async fn create_drawer(node: &big_node::BigNode, name: &str) -> DrawerId {
    node.drawer_access
        .grants()
        .grant(
            &RequestContext::root(),
            node.drawers_actor.actor.actor_id,
            big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let capability = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                capability: None,
                scope: DrawerScope {
                    create: true,
                    ..DrawerScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.drawers
        .client
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability,
                command: CommandId::try_from(format!("create-drawer-{name}")).unwrap(),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .unwrap()
        .drawer_id
}

/// Creates a note in `drawer` as the node's drawers actor does, and returns its id.
pub async fn create_note(node: &big_node::BigNode, drawer: &DrawerId, text: &str) -> DocId {
    let placement = format!("drawer {drawer}");
    let capability = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDocCapability {
                capability: None,
                scope: DocScope {
                    create: BTreeSet::from([placement.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: CommandId::try_from(format!("create-{text}")).unwrap(),
                placement,
                datums: BTreeMap::from([(
                    format!("{NOTE}/main").parse::<DatumKey>().unwrap(),
                    json!({"$type": NOTE, "value": text}),
                )]),
            },
        )
        .await
        .unwrap()
        .doc_id
}

/// What a guest's `readDatums` is on the wire: a JSON `POST` to `dispatch.local`, answered by the
/// micro's egress. Returns the HTTP status and the JSON body.
pub async fn read_datums(
    egress: &Arc<dyn DispatchEgress>,
    capability: CapId,
    doc_id: &DocId,
) -> (u16, Value) {
    let body = serde_json::to_vec(&ReadDatums {
        capability,
        doc_id: doc_id.clone(),
        at: At::Current,
        datums: None,
        optimistic: false,
    })
    .unwrap();
    let request = http::Request::post(format!(
        "http://dispatch.local/xrpc/{}",
        ReadDatums::XRPC.nsid
    ))
    .header(http::header::CONTENT_TYPE, "application/json")
    .body(
        Full::new(Bytes::from(body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    )
    .unwrap();
    let response = egress.send(request).await;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}
