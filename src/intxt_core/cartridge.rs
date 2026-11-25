//! intxt as a cartridge (ledger D38, D41): what admits intxt's vocabularies (`intxt_core`) to a
//! node, as an ADR 047 manifest.
//!
//! The cartridge holds every intxt record under its authority, `org.example.intxt`. Each record
//! ships its Lexicon, which states what a value is, and its sidecar, which states only how the
//! node stores it (ADR 028): `note.content` as collaborative text and `body.order` as a list, so
//! concurrent edits to a file and concurrent appends to an intxt's parts merge instead of the last
//! writer replacing the value. Without the sidecar every position would keep the plain JSON kind
//! its Lexicon states.
//!
//! It also serves two operations, `createIntxt` and `addToIntxt`, as routines of its one nodule
//! (ADR 047 §3): each is an export of the handler component (`intxt_nodule::Intxt`) and `uses`
//! the operations of other authorities its handler calls, which are the cartridge's method
//! permbinds (§5) and what an operator grants. The operations' Lexicons are projected from the
//! Rust types of its methods.

use big_cabinet_core::datum::Bundle;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::ListDocDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Cartridge;
use big_cartridges_core::ComponentExport;
use big_cartridges_core::Necessity;
use big_cartridges_core::Nodule;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindMethod;
use big_cartridges_core::Routine;
use big_cartridges_core::RoutineImpl;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::test_support::FixtureCartridge;
use indexmap::IndexMap;
use method_core::Method;
use serde_json::Value;

use crate::methods::AddToIntxt;
use crate::methods::CreateIntxt;
use crate::methods::REQUIRED_OPERATIONS;

/// The authority every intxt record is under. A cartridge registers only NSIDs it owns, so the
/// generic datums intxt holds until they migrate out before the beta (ledger D39) are under it too.
pub const AUTHORITY: &str = "org.example.intxt";

/// The key of the handler component in the cartridge.
pub const HANDLER_COMPONENT: &str = "handler";

/// The export of the handler component that serves every routine: the component dispatches on
/// the operation's NSID itself (`big_ndk::Nodule::route`).
const HANDLER_EXPORT: &str = "handle";

/// The operations of other authorities each routine's handler calls, a subset of
/// [`REQUIRED_OPERATIONS`]: a routine reaches nothing it does not list (ADR 047 §3).
const CREATE_INTXT_USES: [&str; 4] = [
    MintDrawerCapability::XRPC.nsid,
    CreateDrawer::XRPC.nsid,
    MintDocCapability::XRPC.nsid,
    WriteDatums::XRPC.nsid,
];
const ADD_TO_INTXT_USES: [&str; 6] = [
    ReadDatums::XRPC.nsid,
    WriteDatums::XRPC.nsid,
    MintDocCapability::XRPC.nsid,
    MintDrawerCapability::XRPC.nsid,
    ListDocDrawers::XRPC.nsid,
    AddDoc::XRPC.nsid,
];

/// The permbind key of the operation `nsid`: its last segment, which is unique among
/// [`REQUIRED_OPERATIONS`].
fn permbind_key(nsid: &str) -> &str {
    nsid.rsplit('.')
        .next()
        .expect("a split always yields a segment")
}

/// intxt's cartridge version, with `wasm` as its handler component: the envelope, the payloads
/// its pins name, and the component's bytes, as a publisher stages and publishes them. Its
/// method permbinds request [`REQUIRED_OPERATIONS`], which an operator must grant the nodule.
pub fn cartridge(wasm: &[u8]) -> FixtureCartridge {
    let bundle = Bundle::new(&crate::UNIVERSE, Vec::new());
    let mut cartridge = FixtureCartridge::new(Cartridge::new(
        // intxt has no namespace of its own yet, so its id sits under the fixture namespace
        // (`test-support`, enabled by the `cartridge` feature for this alone).
        big_cartridges_core::example_cartridge!("intxt")
            .parse()
            .expect("intxt's cartridge id is valid"),
        "0.1.0".parse().expect("intxt's version is valid"),
    ));
    cartridge.authority = Some(AUTHORITY.to_owned());
    cartridge.name = Some("intxt".to_owned());
    for lexicon in bundle
        .lexicons()
        .expect("intxt's declarations publish lexicons")
    {
        cartridge.with_lexicon(as_map(lexicon.file));
    }
    for datum in &crate::UNIVERSE.datums {
        let document = Value::from(
            bundle
                .document(&datum.name)
                .expect("every declared datum renders its document"),
        );
        if declares_storage(&document) {
            cartridge.with_sidecar(&datum.id, as_map(document));
        }
    }
    cartridge
        .with_lexicon(as_map(
            method_lexicon::lexicon::<CreateIntxt>()
                .expect("createIntxt's types project a Lexicon"),
        ))
        .with_lexicon(as_map(
            method_lexicon::lexicon::<AddToIntxt>().expect("addToIntxt's types project a Lexicon"),
        ))
        .with_wasm(HANDLER_COMPONENT, wasm);

    cartridge.permbinds = REQUIRED_OPERATIONS
        .iter()
        .map(|nsid| {
            (
                permbind_key(nsid).to_owned(),
                Permbind::Method(PermbindMethod {
                    nsid: nsid.parse().expect("a required operation is an NSID"),
                    description: format!("intxt calls {nsid} on its caller's behalf"),
                    necessity: Necessity::Required,
                    min_revision: None,
                    extra: Default::default(),
                }),
            )
        })
        .collect();
    let routine = |operation: &str, uses: &[&str]| {
        (
            permbind_key(operation).to_owned(),
            Routine {
                r#impl: RoutineImpl::ComponentExport(ComponentExport {
                    component: HANDLER_COMPONENT.to_owned(),
                    export: HANDLER_EXPORT.to_owned(),
                    extra: Default::default(),
                }),
                implements: operation.parse().expect("an operation is an NSID"),
                uses: uses
                    .iter()
                    .map(|nsid| (permbind_key(nsid).to_owned(), RoutineUse::default()))
                    .collect(),
                extra: Default::default(),
            },
        )
    };
    cartridge.nodule = Some(Nodule {
        routines: [
            routine(CreateIntxt::XRPC.nsid, &CREATE_INTXT_USES),
            routine(AddToIntxt::XRPC.nsid, &ADD_TO_INTXT_USES),
        ]
        .into(),
        ..Default::default()
    });
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
    use big_cabinet_core::datum::DatumMember;
    use big_cartridges_core::test_support::cartridge_id;
    use big_nodule_core::NoduleAdmission;

    /// The cartridge is admitted as a manifest, holds every record intxt declares so none is left
    /// for another holder, ships the sidecar of each record that declares storage forms, and
    /// serves both operations.
    #[test]
    fn the_cartridge_holds_every_intxt_record() {
        let fixture = cartridge(b"a handler");
        assert_eq!(fixture.id, cartridge_id("intxt"));
        let admitted = fixture
            .resolved
            .admit(fixture.authority.as_deref())
            .expect("intxt's manifest is admitted");
        assert_eq!(
            admitted
                .operations
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [AddToIntxt::XRPC.nsid, CreateIntxt::XRPC.nsid],
        );
        // Every operation the cartridge requests is one some routine uses, so nothing is
        // requested (and granted) that no handler calls.
        let nodule = fixture.nodule.as_ref().unwrap();
        for permbind in fixture.permbinds.keys() {
            assert!(
                nodule
                    .routines
                    .values()
                    .any(|routine| routine.uses.contains_key(permbind)),
                "no routine uses {permbind}"
            );
        }
        // The manifest's Lexicons are a map, so they come back in NSID order, not declaration order.
        let mut held: Vec<&str> = fixture
            .resolved
            .registrable_datums(fixture.authority.as_deref())
            .held
            .iter()
            .map(|(id, _, _)| *id)
            .collect();
        held.sort_unstable();
        for record in [generic::Note::ID, generic::Body::ID] {
            assert!(
                fixture
                    .resolved
                    .sidecars
                    .keys()
                    .any(|nsid| nsid.as_str() == record),
                "{record} has no sidecar"
            );
        }
        let mut declared: Vec<&str> = crate::UNIVERSE
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect();
        declared.sort_unstable();
        let unheld: Vec<_> = fixture
            .resolved
            .datum_lexicons()
            .filter(|file| !held.contains(&file["id"].as_str().unwrap()))
            .map(|file| serde_json::to_string(&file["defs"]["main"]).unwrap())
            .collect();
        assert_eq!(held, declared, "not held: {unheld:?}");
    }
}
