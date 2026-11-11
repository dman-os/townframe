//! intxt's vocabularies written and read through a real node that admitted intxt's cartridge,
//! so they are stored as its sidecar says.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_dispatch_core::DispatchScope;
use big_ndk_testing::TestNode;
use intxt_core::generic;
use intxt_core::work;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// A node that admitted intxt's cartridge, serving its handler natively, with the four
/// operations the handler calls granted to it, and the nodule enabled.
async fn boot() -> TestNode {
    let cartridge = crate::cartridge();
    let id = cartridge.id.clone();
    // The ceiling of what a nodule can be granted covers the registered cartridge's own
    // authority only, so the foreign operations it calls must be added to it.
    let operations = DispatchScope {
        operations: crate::REQUIRED_OPERATIONS.map(str::to_owned).into(),
        ..DispatchScope::default()
    };
    let node = big_ndk_testing::node()
        .with_operation_ceiling(operations.clone())
        .with_nodule(cartridge, crate::Intxt)
        .boot()
        .await;
    node.grant(&id, operations).await;
    node.enable(&id).await;
    node
}

fn key<T: DatumMember>(slot: &str) -> DatumKey {
    format!("{}/{slot}", T::ID).parse().unwrap()
}

/// A `T` datum at `slot` holding `value`, tagged with its record's `$type` as every record value
/// is.
fn datum<T: DatumMember>(slot: &str, mut value: Value) -> (DatumKey, Value) {
    value["$type"] = T::ID.into();
    (key::<T>(slot), value)
}

/// A capability over `scope` for the node's operator.
async fn capability(node: &TestNode, scope: DocScope) -> big_cabinet_core::authority::CapId {
    let node = node.big_node();
    node.docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id
}

/// Creates a document in the node's local drawer holding `datums`.
async fn create(node: &TestNode, datums: BTreeMap<DatumKey, Value>) -> WriteReceipt {
    let placement = format!("drawer {}", node.big_node().drawers.local().present_id);
    let capability = capability(
        node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    node.big_node()
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: CommandId::try_from("create".to_owned()).unwrap(),
                placement,
                datums,
            },
        )
        .await
        .unwrap()
}

async fn write(node: &TestNode, document: &DocId, mode: WriteMode, set: BTreeMap<DatumKey, Value>) {
    let capability = capability(
        node,
        DocScope {
            write: BTreeSet::from([document.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    node.big_node()
        .docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability,
                command: None,
                doc_id: document.clone(),
                mode,
                set,
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
}

/// The accepted value of each of `datums` at the document's current heads.
async fn read(node: &TestNode, document: &DocId, datums: &[DatumKey]) -> BTreeMap<DatumKey, Value> {
    let capability = capability(
        node,
        DocScope {
            read: BTreeSet::from([document.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let read = node
        .big_node()
        .docs
        .client
        .call(
            RequestContext::root(),
            ReadDatums {
                capability,
                doc_id: document.clone(),
                at: At::Current,
                datums: Some(datums.iter().cloned().collect()),
                optimistic: false,
            },
        )
        .await
        .unwrap();
    read.datums
        .into_iter()
        .map(|(key, read)| match read {
            DatumRead::Accepted { value, .. } => (key, value),
            other => panic!("{key} was not accepted: {other:?}"),
        })
        .collect()
}

/// A file document (ADR 040 §4) as capture writes it: its path, its text, and the worktree it is
/// in. Each datum reads back as written and decodes into its type.
#[tokio::test]
async fn a_file_document_reads_back_as_written() {
    let node = boot().await;
    let datums = BTreeMap::from([
        datum::<generic::PathName>("main", json!({"path": "src/big_cabinet_core/reference.rs"})),
        datum::<generic::Note>(
            "main",
            json!({"mime": "text/x-rust", "content": "fn parse() {}\n"}),
        ),
        datum::<work::File>(
            "main",
            json!({"worktree": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.worktree/main"}),
        ),
    ]);
    let receipt = create(&node, datums.clone()).await;
    let keys: Vec<DatumKey> = datums.keys().cloned().collect();
    let read = read(&node, &receipt.doc_id, &keys).await;
    assert_eq!(read, datums);
    serde_json::from_value::<generic::PathName>(read[&key::<generic::PathName>("main")].clone())
        .unwrap();
    serde_json::from_value::<generic::Note>(read[&key::<generic::Note>("main")].clone()).unwrap();
    serde_json::from_value::<work::File>(read[&key::<work::File>("main")].clone()).unwrap();
    node.stop().await;
}

/// `note.content` is collaborative text through the node's write path, not a string that the last
/// writer replaces: two writers editing different lines from the same base both land, which is
/// what lets several agents and people edit one file document (ADR 040 §4).
#[tokio::test]
async fn concurrent_line_edits_to_a_note_both_land() {
    let node = boot().await;
    let content = |lines: [&str; 3]| {
        datum::<generic::Note>(
            "main",
            json!({"mime": "text/plain", "content": lines.join("\n")}),
        )
    };
    let (note, _) = content(["one", "two", "three"]);
    let receipt = create(&node, BTreeMap::from([content(["one", "two", "three"])])).await;
    for edited in [["ONE", "two", "three"], ["one", "two", "THREE"]] {
        write(
            &node,
            &receipt.doc_id,
            WriteMode::AtHeads {
                heads: receipt.heads.clone(),
            },
            BTreeMap::from([content(edited)]),
        )
        .await;
    }
    let read = read(&node, &receipt.doc_id, std::slice::from_ref(&note)).await;
    assert_eq!(read[&note], content(["ONE", "two", "THREE"]).1);
    node.stop().await;
}

/// `createIntxt`, called as the operator, creates the drawer and writes the three descriptor
/// datums of ADR 036 §2 on its descriptor document, each tagged with its `$type`.
#[tokio::test]
async fn create_intxt_writes_the_descriptor_datums() {
    let node = boot().await;
    let created = node
        .call(crate::CreateIntxt {
            title: "Port the sync".to_owned(),
            description: Some("Notes *in markdown*".to_owned()),
        })
        .await
        .unwrap();
    let keys = [
        key::<generic::Intxt>("main"),
        key::<generic::Title>("main"),
        key::<generic::Body>("main"),
    ];
    assert_eq!(
        read(&node, &created.drawer_id, &keys).await,
        BTreeMap::from([
            datum::<generic::Intxt>("main", json!({"empty": true})),
            datum::<generic::Title>(
                "main",
                json!({"title": "Port the sync", "description": "Notes *in markdown*"}),
            ),
            datum::<generic::Body>("main", json!({"order": []})),
        ])
    );
    node.stop().await;
}
