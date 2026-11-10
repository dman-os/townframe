//! intxt's vocabularies written and read through a real node.
//!
//! The node registers them at boot, the way it registers its built-in datums, because a cartridge
//! cannot yet ship the storage annotations (`x-automerge: text` on `note.content`) a cartridge's
//! Lexicons would need: admitted as a cartridge, the note would be stored as a plain string. When
//! cartridges carry them, these tests admit intxt's cartridge instead.

use crate::*;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::Bundle;
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
use big_ndk_testing::TestNode;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

fn schema_group() -> SchemaRegistrationGroup<'static> {
    let bundle = Bundle::new(&UNIVERSE, Vec::new());
    let registrations = bundle
        .lexicons()
        .expect("intxt's declarations publish lexicons")
        .into_iter()
        .map(|lexicon| SchemaRegistration::new(lexicon.id, lexicon.file));
    SchemaRegistrationGroup::new(Some(bundle), registrations, Vec::new())
}

async fn boot() -> TestNode {
    big_ndk_testing::node()
        .with_schemas(schema_group())
        .boot()
        .await
}

fn key<T: DatumMember>(slot: &str) -> DatumKey {
    format!("{}/{slot}", T::ID).parse().unwrap()
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
        (
            key::<generic::PathName>("main"),
            json!({"path": "src/big_cabinet_core/reference.rs"}),
        ),
        (
            key::<generic::Note>("main"),
            json!({"mime": "text/x-rust", "content": "fn parse() {}\n"}),
        ),
        (
            key::<work::File>("main"),
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
    let note = key::<generic::Note>("main");
    let content = |lines: [&str; 3]| json!({"mime": "text/plain", "content": lines.join("\n")});
    let receipt = create(
        &node,
        BTreeMap::from([(note.clone(), content(["one", "two", "three"]))]),
    )
    .await;
    for edited in [["ONE", "two", "three"], ["one", "two", "THREE"]] {
        write(
            &node,
            &receipt.doc_id,
            WriteMode::AtHeads {
                heads: receipt.heads.clone(),
            },
            BTreeMap::from([(note.clone(), content(edited))]),
        )
        .await;
    }
    let read = read(&node, &receipt.doc_id, std::slice::from_ref(&note)).await;
    assert_eq!(read[&note], content(["ONE", "two", "THREE"]));
    node.stop().await;
}
