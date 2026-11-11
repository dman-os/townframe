//! intxt's vocabularies written and read through a real node that admitted intxt's cartridge,
//! so they are stored as its sidecar says.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::ResourceLimit;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::ListDocs;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_ndk_testing::TestNode;
use intxt_core::generic;
use intxt_core::work;
use intxt_handlers::AddToIntxt;
use intxt_handlers::AddedToIntxt;
use method_core::Method;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

mod capture;

/// Each document creation is a command of its own; the node refuses a reused command id.
static NEXT_COMMAND: AtomicU64 = AtomicU64::new(0);

/// A node that admitted intxt's cartridge, serving its handler natively, with the operations the
/// handler calls granted to it, and the nodule enabled.
async fn boot() -> TestNode {
    let cartridge = crate::cartridge();
    let id = cartridge.id.clone();
    // The ceiling of what a nodule can be granted covers the registered cartridge's own
    // authority only, so the foreign operations it calls must be added to it.
    let operations = DispatchScope {
        operations: intxt_handlers::REQUIRED_OPERATIONS
            .map(str::to_owned)
            .into(),
        ..DispatchScope::default()
    };
    let node = big_ndk_testing::node()
        .with_operation_ceiling(operations.clone())
        .with_nodule(cartridge, intxt_handlers::Intxt)
        .boot()
        .await;
    node.grant(&id, operations).await;
    node.enable(&id).await;
    node
}

fn key<T: DatumMember>(slot: &str) -> DatumKey {
    format!("{}/{slot}", T::ID).parse().unwrap()
}

/// A `T` datum at `slot` holding `value`: the key names the type, so the value carries no tag.
fn datum<T: DatumMember>(slot: &str, value: Value) -> (DatumKey, Value) {
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

/// Creates a document in `drawer` holding `datums`.
async fn create_in(
    node: &TestNode,
    drawer: &DocId,
    datums: BTreeMap<DatumKey, Value>,
) -> WriteReceipt {
    let placement = format!("drawer {drawer}");
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
                command: CommandId::try_from(format!(
                    "create-{}",
                    NEXT_COMMAND.fetch_add(1, Ordering::Relaxed)
                ))
                .unwrap(),
                placement,
                datums,
            },
        )
        .await
        .unwrap()
}

/// Creates a document in the node's local drawer holding `datums`.
async fn create(node: &TestNode, datums: BTreeMap<DatumKey, Value>) -> WriteReceipt {
    create_in(node, &node.big_node().drawers.local().present_id, datums).await
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
/// datums of ADR 036 §2 on its descriptor document.
#[tokio::test]
async fn create_intxt_writes_the_descriptor_datums() {
    let node = boot().await;
    let created = node
        .call(intxt_handlers::CreateIntxt {
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

/// `addToIntxt` includes parts once: each part is an entry in the intxt and a place in its body's
/// order, adding one again changes neither, and the parts' documents are in the intxt's drawer.
#[tokio::test]
async fn add_to_intxt_includes_parts_once_and_shares_their_documents() {
    let node = boot().await;
    let intxt = async |title: &str| {
        node.call(intxt_handlers::CreateIntxt {
            title: title.to_owned(),
            description: None,
        })
        .await
        .unwrap()
        .drawer_id
    };
    let captures = intxt("captures").await;
    let review = intxt("review").await;
    let capture = async |datums: BTreeMap<DatumKey, Value>| {
        let tag = datums.keys().next().unwrap().tag().to_string();
        let receipt = create_in(&node, &captures, datums).await;
        format!("dtxt:datum:{}/{tag}/main", receipt.doc_id)
            .parse::<DatumReference>()
            .unwrap()
    };
    let worktree = capture(BTreeMap::from([datum::<work::Worktree>(
        "main",
        json!({
            "clone": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitClone/0193c",
            "id": "0193a",
            "path": "/work/townframe",
            "files": {},
            "opLogs": []
        }),
    )]))
    .await;
    let session = capture(BTreeMap::from([datum::<work::AgentSession>(
        "main",
        json!({"harness": "claude-code", "sessionId": "eb26ecaa", "cwd": "/work/townframe"}),
    )]))
    .await;

    let mut added = Vec::new();
    for part in [&worktree, &session, &worktree] {
        let AddedToIntxt { entry } = node
            .call(AddToIntxt {
                intxt: review.clone(),
                part: part.clone(),
            })
            .await
            .unwrap();
        added.push(entry.to_string());
    }
    assert_eq!(
        added,
        [
            format!("dtxt:datum:{review}/org.example.intxt.work.worktreeEntry/0193a"),
            format!("dtxt:datum:{review}/org.example.intxt.work.sessionEntry/eb26ecaa"),
            format!("dtxt:datum:{review}/org.example.intxt.work.worktreeEntry/0193a"),
        ]
    );
    let entries = [
        key::<work::WorktreeEntry>("0193a"),
        key::<work::SessionEntry>("eb26ecaa"),
        key::<generic::Body>("main"),
    ];
    assert_eq!(
        read(&node, &review, &entries).await,
        BTreeMap::from([
            datum::<work::WorktreeEntry>("0193a", json!({"worktree": worktree.to_string()})),
            datum::<work::SessionEntry>("eb26ecaa", json!({"session": session.to_string()})),
            datum::<generic::Body>(
                "main",
                json!({"order": [
                    "dtxt:datum:self/org.example.intxt.work.worktreeEntry/0193a",
                    "dtxt:datum:self/org.example.intxt.work.sessionEntry/eb26ecaa",
                ]}),
            ),
        ])
    );

    let node_drawers = &node.big_node().drawers;
    let listing = node_drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.big_node().authority.root).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(review.clone(), DrawerAccess::Read)].into(),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let listed = node_drawers
        .client
        .call(
            RequestContext::root(),
            ListDocs {
                capability: listing,
                drawer_id: review.clone(),
                offset: 0,
                limit: None,
            },
        )
        .await
        .unwrap();
    for part in [&worktree, &session] {
        let ReferenceDocId::Concrete(doc) = part.document() else {
            unreachable!("the parts were named by document")
        };
        assert!(
            listed.docs.iter().any(|listed| &listed.doc_id == doc),
            "{doc} is not in the intxt's drawer: {listed:?}"
        );
    }
    node.stop().await;
}

/// Retrying `createIntxt` under one idempotency key reuses its commands, so it answers the same
/// drawer instead of creating a second one; another key creates another.
#[tokio::test]
async fn create_intxt_under_one_idempotency_key_creates_one_drawer() {
    let node = boot().await;
    let create = async |key: &str| {
        let output = node
            .invoke(
                intxt_handlers::CreateIntxt::XRPC.nsid,
                json!({"title": "Retried"}),
                InvocationContext {
                    idempotency_key: Some(key.to_owned()),
                    http: None,
                },
                RequestContext::root(),
            )
            .await
            .unwrap();
        serde_json::from_value::<intxt_handlers::IntxtCreated>(output)
            .unwrap()
            .drawer_id
    };
    let first = create("retry-1").await;
    assert_eq!(create("retry-1").await, first);
    assert_ne!(create("retry-2").await, first);
    node.stop().await;
}

/// A file document far past the old per-value node cap reads back as written, and an edit of one
/// line from the heads it was created at lands, as it does for a small file.
#[tokio::test]
async fn long_test_a_large_note_reads_back_and_takes_a_line_edit() {
    let node = boot().await;
    let lines = |edited: Option<usize>| -> String {
        (0..4_000)
            .map(|line| match edited {
                Some(at) if at == line => format!("line {line:05} EDITED\n"),
                _ => format!("line {line:05} of a generated file, long enough to add up\n"),
            })
            .collect()
    };
    let note = |content: String| {
        datum::<generic::Note>("main", json!({"mime": "text/plain", "content": content}))
    };
    let original = lines(None);
    assert!(original.len() > 200 * 1024, "{} bytes", original.len());
    let (key, value) = note(original);
    let receipt = create(&node, BTreeMap::from([(key.clone(), value.clone())])).await;
    assert_eq!(
        read(&node, &receipt.doc_id, std::slice::from_ref(&key)).await[&key],
        value
    );
    let (_, edited) = note(lines(Some(2_000)));
    write(
        &node,
        &receipt.doc_id,
        WriteMode::AtHeads {
            heads: receipt.heads.clone(),
        },
        BTreeMap::from([(key.clone(), edited.clone())]),
    )
    .await;
    assert_eq!(
        read(&node, &receipt.doc_id, std::slice::from_ref(&key)).await[&key],
        edited
    );
    node.stop().await;
}

/// A value past the per-value budget is refused by name, through the docs service client the
/// helpers here use (an in-process call, so no request-body limit is in the way).
#[tokio::test]
async fn long_af_test_a_note_past_the_value_budget_is_refused_by_name() {
    let node = boot().await;
    let (key, small) =
        datum::<generic::Note>("main", json!({"mime": "text/plain", "content": "x"}));
    let receipt = create(&node, BTreeMap::from([(key.clone(), small)])).await;
    let capability = capability(
        &node,
        DocScope {
            write: BTreeSet::from([receipt.doc_id.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let (_, oversized) = datum::<generic::Note>(
        "main",
        json!({"mime": "text/plain", "content": "x".repeat(17 * 1024 * 1024)}),
    );
    let error = node
        .big_node()
        .docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability,
                command: None,
                doc_id: receipt.doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key.clone(), oversized)]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap_err();
    let ClientError::Method(DocsError::ValueTooLarge {
        datum_key, limit, ..
    }) = error
    else {
        panic!("expected ValueTooLarge, got {error:?}");
    };
    assert_eq!((datum_key, limit), (key, ResourceLimit::Bytes));
    node.stop().await;
}
