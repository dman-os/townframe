//! The command ledger: replays, reuse, and what the boot pass settles after a crash.

use super::super::commands::{Commands, Detail, Kind, State};
use super::*;
use automerge::transaction::{CommitOptions, Transactable};

fn command(text: &str) -> CommandId {
    CommandId::try_from(text.to_owned()).unwrap()
}

#[tokio::test]
async fn a_replayed_command_returns_its_receipt_without_acting_again() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    assert_eq!(
        node.create(&alice, "create", [(key("main"), note("x"))])
            .await
            .unwrap(),
        created
    );
    let capability = node
        .capability(
            &alice,
            DocScope {
                write: BTreeSet::from([created.doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    let write = || WriteFacets {
        capability,
        command: Some(command("write")),
        doc_id: created.doc_id.clone(),
        mode: WriteMode::AtCurrent,
        set: facet_map(&[(&key("main"), note("y"))]),
        remove: BTreeSet::new(),
    };
    let first = op(node.client().call(RequestContext::root(), write()).await).unwrap();
    let again = op(node.client().call(RequestContext::root(), write()).await).unwrap();
    assert_eq!(again, first);
    let read = node
        .read(&alice, &created.doc_id, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, first.heads, "the replay committed nothing");
    node.stop().await;
}

#[tokio::test]
async fn a_command_reused_for_another_request_is_refused() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    node.create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    assert_eq!(
        node.create(&alice, "create", [(key("main"), note("other"))])
            .await
            .unwrap_err(),
        DocsError::CommandReused {
            command: command("create")
        }
    );
    node.stop().await;
}

#[tokio::test]
async fn a_refused_write_releases_its_command() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let capability = node
        .capability(
            &alice,
            DocScope {
                write: BTreeSet::from([created.doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    let write = |heads: Vec<ChangeHash>| WriteFacets {
        capability,
        command: Some(command("write")),
        doc_id: created.doc_id.clone(),
        mode: WriteMode::IfDocUnchanged { heads },
        set: facet_map(&[(&key("main"), note("y"))]),
        remove: BTreeSet::new(),
    };
    let stale = vec![ChangeHash([3; 32])];
    assert!(
        op(node
            .client()
            .call(RequestContext::root(), write(stale))
            .await)
        .is_err()
    );
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    assert!(ledger.get(&command("write")).await.unwrap().is_none());
    node.stop().await;
}

#[tokio::test]
async fn the_boot_pass_settles_interrupted_writes_from_their_commit_message() {
    let mut node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    for name in ["committed", "lost"] {
        ledger
            .begin(
                &command(name),
                Kind::Write,
                [0; 32],
                alice.actor_id,
                Some(document),
                None,
                None,
            )
            .await
            .unwrap();
    }
    let heads = node
        .sabotage(document, |doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "touched", true).unwrap();
            tx.commit_with(CommitOptions::default().with_message("committed"));
        })
        .await;
    node.restart().await.unwrap();
    let settled = ledger.get(&command("committed")).await.unwrap().unwrap();
    assert_eq!((settled.state, settled.heads), (State::Done, heads));
    assert!(ledger.get(&command("lost")).await.unwrap().is_none());
    node.stop().await;
}

#[tokio::test]
async fn the_boot_pass_fails_on_a_repository_error_and_leaves_the_row_for_the_next_pass() {
    let mut node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("x"))])
        .await
        .unwrap();
    let document = &created.doc_id;
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    ledger
        .begin(
            &command("committed"),
            Kind::Write,
            [0; 32],
            alice.actor_id,
            Some(document),
            None,
            None,
        )
        .await
        .unwrap();
    let heads = node
        .sabotage(document, |doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "touched", true).unwrap();
            tx.commit_with(CommitOptions::default().with_message("committed"));
        })
        .await;
    // A lookup the boot pass cannot classify (neither missing nor still materializing) is a
    // repository failure: boot refuses rather than reporting a recovery it did not perform.
    node.repo.fail_next_doc_lookup(document.clone());
    let error = node.restart().await.unwrap_err();
    assert!(
        matches!(&error, DocsError::Repository(message) if message.contains("injected document lookup failure")),
        "{error:?}"
    );
    let row = ledger.get(&command("committed")).await.unwrap().unwrap();
    assert_eq!(row.state, State::Pending);
    // The next pass, with the repository answering, settles it from the commit message.
    node.start().await.unwrap();
    let settled = ledger.get(&command("committed")).await.unwrap().unwrap();
    assert_eq!((settled.state, settled.heads), (State::Done, heads));
    node.stop().await;
}

#[tokio::test]
async fn the_boot_pass_places_a_committed_create_and_abandons_an_uncommitted_one() {
    let mut node = Node::boot().await;
    let alice = node.writer("alice").await;
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    let committed = node.repo.allocate_id().await.unwrap();
    let uncommitted = node.repo.allocate_id().await.unwrap();
    for (name, document) in [("committed", &committed), ("uncommitted", &uncommitted)] {
        ledger
            .begin(
                &command(name),
                Kind::Create,
                [0; 32],
                alice.actor_id,
                None,
                Some(PLACEMENT),
                Some(&Detail::Create {
                    owner_id: Some(alice.actor_id),
                }),
            )
            .await
            .unwrap();
        ledger.set_document(&command(name), document).await.unwrap();
    }
    let mut initial = Automerge::new();
    let mut tx = initial.transaction();
    tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)
        .unwrap();
    tx.commit();
    node.repo
        .commit_id(committed.clone(), initial, Vec::new(), &[])
        .await
        .unwrap();
    node.restart().await.unwrap();
    let settled = ledger.get(&command("committed")).await.unwrap().unwrap();
    assert_eq!(settled.state, State::Done);
    assert!(ledger.get(&command("uncommitted")).await.unwrap().is_none());
    let reserved = node.repo.reserved_doc_ids().await.unwrap();
    assert!(!reserved.contains(&uncommitted));
    // Placement granted the creator access, so the document is readable now.
    let read = node
        .read(&alice, &committed, At::Current, false)
        .await
        .unwrap();
    assert_eq!(read.heads, settled.heads);
    node.stop().await;
}
