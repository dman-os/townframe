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
//! The nodule has no handler yet: it holds records and serves no operation. Its operations
//! (`createIntxt`, recording capture observations) land with the handler.

use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::Cartridge;
use big_cartridges_core::Contribution;
use big_cartridges_core::NoduleContribution;
use big_nodule_core::Nodule;
use indexmap::IndexMap;
use serde_json::Value;

/// The authority every intxt record is under. A cartridge registers only NSIDs it owns, so the
/// generic datums intxt holds until they migrate out before the beta (ledger D39) are under it too.
pub const AUTHORITY: &str = "org.example.intxt";

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
            let document = reached_only(Value::from(document), &datum.name);
            declares_storage(&document).then(|| (datum.id.clone(), document))
        })
        .collect();
    let nodule = Nodule {
        lexicons: lexicons
            .into_iter()
            .map(|lexicon| as_map(as_record(lexicon.file)))
            .collect(),
        sidecar,
        ..Default::default()
    };
    let mut cartridge = Cartridge::new(
        big_cartridges_core::example_cartridge!("intxt")
            .parse()
            .expect("intxt's cartridge id is valid"),
        "0.1.0".parse().expect("intxt's version is valid"),
    );
    cartridge.authority = Some(AUTHORITY.to_owned());
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

/// `document` keeping only the `$defs` its datum `name` reaches. `Bundle::document` carries every
/// definition in scope, so one document per record grows with the square of the vocabulary:
/// intxt's 25 came to 22,701 JSON nodes, past the 16,384 a stored datum may hold.
fn reached_only(mut document: Value, name: &str) -> Value {
    let defs = document["$defs"].take();
    let mut reached = serde_json::Map::new();
    let mut pending = vec![name.to_owned()];
    while let Some(name) = pending.pop() {
        if reached.contains_key(&name) {
            continue;
        }
        let def = defs[&name].clone();
        refs(&def, &mut pending);
        reached.insert(name, def);
    }
    document["$defs"] = Value::Object(reached);
    document
}

/// Pushes the name of every `#/$defs/<name>` reference in `value` onto `names`.
fn refs(value: &Value, names: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(name) = map
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|target| target.strip_prefix("#/$defs/"))
            {
                names.push(name.to_owned());
            }
            map.values().for_each(|value| refs(value, names));
        }
        Value::Array(items) => items.iter().for_each(|value| refs(value, names)),
        _ => {}
    }
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

/// `file` with its `main` definition stated as a record. `Bundle::lexicons` publishes a datum's
/// `main` as the bare object the node registers its built-in datums with, but a nodule holds only
/// the Lexicons whose `main` is a record (ADR 028 §4). The key is `any`: intxt keys its slots by
/// UUIDv7, `main`, or a content-derived key such as a commit sha (ADR 036 §1).
fn as_record(mut file: Value) -> Value {
    let main = file["defs"]["main"].take();
    file["defs"]["main"] = serde_json::json!({"type": "record", "key": "any", "record": main});
    file
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
