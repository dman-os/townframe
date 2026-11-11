//! intxt as a cartridge (ledger D38, D41): what admits intxt's vocabularies (`intxt_core`) to a
//! node.
//!
//! The cartridge holds every intxt record under its authority, `org.example.intxt`. Each record
//! ships its Lexicon, which states what a value is, and its sidecar, which states only how the
//! node stores it (ADR 028): `note.content` as collaborative text and `body.order` as a list, so
//! concurrent edits to a file and concurrent appends to an intxt's parts merge instead of the last
//! writer replacing the value. Without the sidecar every position would keep the plain JSON kind
//! its Lexicon states.
//!
//! It also serves operations through a handler ([`Intxt`]): `createIntxt` today, recording capture
//! observations later. The handler's Lexicons are projected from the Rust types of its methods.

// Booting a node in the tests (`big_ndk_testing`) makes rustc evaluate deep auto-trait chains
// through its services' futures, which the default limit of 128 cuts off with a warning.
#![recursion_limit = "256"]

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Artifact;
use big_cartridges_core::Cartridge;
use big_cartridges_core::Contribution;
use big_cartridges_core::Digest;
use big_cartridges_core::NoduleContribution;
use big_cartridges_core::request::Request;
use big_nodule_core::Nodule;
use indexmap::IndexMap;
use intxt_core::generic;
use method_core::ContextError;
use method_core::Method;
use method_core::Unary;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_client_core_rs::Client;
use service_client_core_rs::xrpc::Xrpc;
use service_core_rs::HttpMetadata;
use service_core_rs::xrpc::bearer_header_value;

/// The authority every intxt record is under. A cartridge registers only NSIDs it owns, so the
/// generic datums intxt holds until they migrate out before the beta (ledger D39) are under it too.
pub const AUTHORITY: &str = "org.example.intxt";

/// The key of the handler artifact in the cartridge.
const HANDLER_ARTIFACT: &str = "handler";

/// Creates an intxt (ADR 036 §2): a drawer, marked as an intxt, with a title and an empty body.
/// The drawer is owned by the caller, whose authority the handler presents.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIntxt {
    pub title: String,
    /// Markdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The new intxt: its drawer, whose id is its descriptor document's id.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntxtCreated {
    pub drawer_id: DrawerId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum IntxtError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    /// A call the handler made was refused or failed; `reason` is what the callee answered.
    #[error("{operation} failed: {reason}")]
    Upstream { operation: String, reason: String },
}

method_core::methods! {
    error = IntxtError, nsid_prefix = "org.example.intxt.";
    CreateIntxt => IntxtCreated, procedure, "createIntxt";
}

/// The operations of other authorities the handler calls, which the cartridge requests.
const REQUIRED_OPERATIONS: [&str; 4] = [
    MintDrawerCapability::XRPC.nsid,
    CreateDrawer::XRPC.nsid,
    MintDocCapability::XRPC.nsid,
    WriteDatums::XRPC.nsid,
];

/// The intxt nodule's handler. A fresh value serves each request (`big_ndk::export_nodule!`
/// cannot be used here because the guest wrapper, not this crate, owns the export), so it holds
/// no state.
#[derive(Debug, Default, Clone)]
pub struct Intxt;

impl big_ndk::Nodule for Intxt {
    async fn route(&self, nsid: &str, cx: &big_ndk::Cx, body: &[u8]) -> Option<big_ndk::Reply> {
        if nsid == CreateIntxt::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, CreateIntxt>(self, cx, body).await);
        }
        None
    }
}

/// Calls `input` presenting `caller`, the capability the caller of this nodule presented, as the
/// authority of the call rather than the nodule's own: the drawer and its descriptor belong to
/// whoever asked for the intxt, and the nodule is not a confused deputy.
async fn as_caller<M: Unary>(
    cx: &big_ndk::Cx,
    caller: &str,
    input: M,
) -> Result<M::Output, IntxtError> {
    let metadata = HttpMetadata {
        authorization: Some(bearer_header_value(caller)),
        idempotency_key: None,
    };
    Client::new(Xrpc(cx.transport()))
        .call_with(cx.next_hop(), metadata, input)
        .await
        .map_err(|error| IntxtError::Upstream {
            operation: M::XRPC.nsid.to_owned(),
            reason: error.to_string(),
        })
}

/// `value` as the `main` slot of record `tag`; the key names the type, so the value carries no
/// tag of its own.
fn datum(value: Value, tag: &str) -> (DatumKey, Value) {
    (
        DatumKey::new(tag, b"main").expect("`main` is a valid slot"),
        value,
    )
}

impl big_ndk::Handler<CreateIntxt> for Intxt {
    /// Four calls: a capability to create drawers, the drawer, a capability to write its
    /// descriptor document (the drawer's id is the descriptor's `DocId`), and the three
    /// descriptor datums in one write. A failure between the calls leaves a plain drawer, which a
    /// reader does not show as an intxt.
    async fn call(&self, cx: &big_ndk::Cx, input: CreateIntxt) -> Result<IntxtCreated, IntxtError> {
        let caller = cx
            .caller_capability()
            .ok_or(ContextError::Unauthenticated)?;
        let command = |kind: &str| {
            CommandId::try_from(format!("{kind}-{}", cx.request().request_id))
                .expect("a kind and a request id make a command id")
        };
        let drawers: CapId = as_caller(
            cx,
            caller,
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: Default::default(),
                    create: true,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await?
        .capability_id;
        let drawer_id: DocId = as_caller(
            cx,
            caller,
            CreateDrawer {
                capability: drawers,
                command: command("intxt-new"),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await?
        .drawer_id;
        let descriptor = as_caller(
            cx,
            caller,
            MintDocCapability {
                scope: DocScope {
                    write: [drawer_id.clone()].into(),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await?
        .capability_id;
        let [intxt, title, body] = [
            serde_json::to_value(generic::Intxt::default()),
            serde_json::to_value(generic::Title {
                title: input.title,
                description: input.description,
                extra: Default::default(),
            }),
            serde_json::to_value(generic::Body {
                order: Vec::new(),
                extra: Default::default(),
            }),
        ]
        .map(|value| value.expect("a record serializes"));
        as_caller(
            cx,
            caller,
            WriteDatums {
                capability: descriptor,
                command: Some(command("intxt-descriptor")),
                doc_id: drawer_id.clone(),
                mode: WriteMode::AtCurrent,
                set: [
                    datum(intxt, generic::Intxt::ID),
                    datum(title, generic::Title::ID),
                    datum(body, generic::Body::ID),
                ]
                .into(),
                remove: Default::default(),
            },
        )
        .await?;
        Ok(IntxtCreated { drawer_id })
    }
}

pub fn cartridge() -> Cartridge {
    let bundle = Bundle::new(&intxt_core::UNIVERSE, Vec::new());
    let lexicons = bundle
        .lexicons()
        .expect("intxt's declarations publish lexicons");
    let sidecar = intxt_core::UNIVERSE
        .datums
        .iter()
        .filter_map(|datum| {
            let document = bundle
                .document(&datum.name)
                .expect("every declared datum renders its document");
            let document = Value::from(document);
            declares_storage(&document).then(|| (datum.id.clone(), document))
        })
        .collect();
    let nodule = Nodule {
        lexicons: lexicons
            .into_iter()
            .map(|lexicon| as_map(lexicon.file))
            .chain([as_map(
                method_lexicon::lexicon::<CreateIntxt>()
                    .expect("createIntxt's types project a Lexicon"),
            )])
            .collect(),
        sidecar,
        requires: REQUIRED_OPERATIONS
            .into_iter()
            .map(|nsid| Request::new(nsid, nsid))
            .collect(),
        handler: Some(big_nodule_core::Handler {
            artifact: HANDLER_ARTIFACT.into(),
            extra: Default::default(),
        }),
        implements: vec![CreateIntxt::XRPC.nsid.to_owned()],
        ..Default::default()
    };
    let mut cartridge = Cartridge::new(
        big_cartridges_core::example_cartridge!("intxt")
            .parse()
            .expect("intxt's cartridge id is valid"),
        "0.1.0".parse().expect("intxt's version is valid"),
    );
    cartridge.authority = Some(AUTHORITY.to_owned());
    // A placeholder: the handler component is not built or packaged yet, and the native test
    // harness never reads the artifact's bytes. Packaging replaces the digest with the
    // component's.
    cartridge.artifacts.push(Artifact {
        key: HANDLER_ARTIFACT.into(),
        digest: Digest::of(b"placeholder: intxt's handler component is not packaged yet"),
        size: Some(1),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    cartridge
        .contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: as_map(
                serde_json::to_value(nodule).expect("a nodule description serializes"),
            ),
            extra: Default::default(),
        }));
    cartridge
}

/// Whether `value` declares a storage form anywhere: a record that declares none needs no
/// sidecar, since without one every position keeps the plain JSON kind its Lexicon states.
fn declares_storage(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.contains_key("x-automerge") || map.values().any(declares_storage),
        Value::Array(items) => items.iter().any(declares_storage),
        _ => false,
    }
}

fn as_map(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).expect("a JSON object")
}

#[cfg(test)]
mod e2e;

#[cfg(test)]
mod tests {
    use super::*;
    use intxt_core::generic;

    /// The cartridge holds every record intxt declares, so none is left for another holder, and
    /// ships the sidecar of each record that declares storage forms.
    #[test]
    fn the_cartridge_holds_every_intxt_record() {
        let cartridge = cartridge();
        let Contribution::Nodule(contribution) = &cartridge.contributions[0] else {
            panic!("intxt contributes a nodule");
        };
        let nodule: Nodule = serde_json::from_value(Value::Object(
            contribution.description.clone().into_iter().collect(),
        ))
        .unwrap();
        let held: Vec<&str> = nodule
            .registrable_datums(cartridge.authority.as_deref())
            .held
            .iter()
            .map(|(id, _, _)| *id)
            .collect();
        for record in [generic::Note::ID, generic::Body::ID] {
            assert!(
                nodule.sidecar.contains_key(record),
                "{record} has no sidecar"
            );
        }
        let declared: Vec<&str> = intxt_core::UNIVERSE
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        let unheld: Vec<_> = nodule
            .datum_lexicons()
            .filter(|file| !held.contains(&file["id"].as_str().unwrap()))
            .map(|file| serde_json::to_string(&file["defs"]["main"]).unwrap())
            .collect();
        assert_eq!(held, declared, "not held: {unheld:?}");
    }
}
