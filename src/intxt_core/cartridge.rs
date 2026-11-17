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
//! It also serves operations through a handler (`intxt_nodule::Intxt`): `createIntxt` and
//! `addToIntxt`. The handler's Lexicons are projected from the Rust types of its methods.

use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::Artifact;
use big_cartridges_core::Cartridge;
use big_cartridges_core::Contribution;
use big_cartridges_core::Digest;
use big_cartridges_core::NoduleContribution;
use big_cartridges_core::request::Request;
use big_nodule_core::Nodule;
use indexmap::IndexMap;
use method_core::Method;
use serde_json::Value;

use crate::methods::AddToIntxt;
use crate::methods::CreateIntxt;
use crate::methods::REQUIRED_OPERATIONS;

/// The authority every intxt record is under. A cartridge registers only NSIDs it owns, so the
/// generic datums intxt holds until they migrate out before the beta (ledger D39) are under it too.
pub const AUTHORITY: &str = "org.example.intxt";

/// The key of the handler artifact in the cartridge.
pub const HANDLER_ARTIFACT: &str = "handler";

/// The envelope of the handler component `wasm`: the cartridge's artifact for it. The digest is
/// the component's BLAKE3, which is also the id of the blob a node stores it under, so putting
/// the bytes in the node's blob store is what makes the artifact available.
pub fn handler_artifact(wasm: &[u8]) -> Artifact {
    Artifact {
        key: HANDLER_ARTIFACT.into(),
        digest: Digest::of(wasm),
        size: Some(wasm.len() as u64),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    }
}

/// intxt's cartridge, whose handler is `handler` (see [`handler_artifact`]). It requests
/// [`REQUIRED_OPERATIONS`], which an operator must grant the nodule.
pub fn cartridge(handler: Artifact) -> Cartridge {
    assert_eq!(
        handler.key, HANDLER_ARTIFACT,
        "the nodule's handler names the artifact `{HANDLER_ARTIFACT}`"
    );
    let bundle = Bundle::new(&crate::UNIVERSE, Vec::new());
    let lexicons = bundle
        .lexicons()
        .expect("intxt's declarations publish lexicons");
    let sidecar = crate::UNIVERSE
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
            .chain([
                as_map(
                    method_lexicon::lexicon::<CreateIntxt>()
                        .expect("createIntxt's types project a Lexicon"),
                ),
                as_map(
                    method_lexicon::lexicon::<AddToIntxt>()
                        .expect("addToIntxt's types project a Lexicon"),
                ),
            ])
            .collect(),
        sidecar,
        requires: REQUIRED_OPERATIONS
            .iter()
            .map(|nsid| Request::new(*nsid, *nsid))
            .collect(),
        handler: Some(big_nodule_core::Handler {
            artifact: HANDLER_ARTIFACT.into(),
            extra: Default::default(),
        }),
        implements: vec![
            CreateIntxt::XRPC.nsid.to_owned(),
            AddToIntxt::XRPC.nsid.to_owned(),
        ],
        ..Default::default()
    };
    // intxt has no namespace of its own yet, so its id sits under the fixture namespace
    // (`test-support`, enabled by the `cartridge` feature for this alone).
    let mut cartridge = Cartridge::new(
        big_cartridges_core::example_cartridge!("intxt")
            .parse()
            .expect("intxt's cartridge id is valid"),
        "0.1.0".parse().expect("intxt's version is valid"),
    );
    cartridge.authority = Some(AUTHORITY.to_owned());
    cartridge.artifacts.push(handler);
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
mod tests {
    use super::*;
    use crate::generic;

    /// The cartridge holds every record intxt declares, so none is left for another holder, and
    /// ships the sidecar of each record that declares storage forms.
    #[test]
    fn the_cartridge_holds_every_intxt_record() {
        let cartridge = cartridge(handler_artifact(b"a handler"));
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
        let declared: Vec<&str> = crate::UNIVERSE
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
