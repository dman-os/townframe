//! `PollDatums` doc-scoped reads over the ungated `datum_set` projection: replay current state,
//! then hold for membership and version changes (ADR 009 §6, ADR 025).

use super::*;
use crate::test_poll::Poller;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;

#[tokio::test]
async fn doc_poll_replays_then_reports_versions_and_removal() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("hello"))])
        .await
        .unwrap();
    let document = created.doc_id.clone();
    let capability = node
        .capability(
            &alice,
            DocScope {
                read: BTreeSet::from([document.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    let mut poll = Poller::doc(node.client(), capability, document.clone());
    let (doc_id, datum_key, datum) = poll.next_datum().await;
    assert_eq!(doc_id, document);
    assert_eq!(datum_key, key("main"));
    let ObservedDatum::Present { version, .. } = &datum else {
        panic!("expected a present replay entry, got {datum:?}");
    };
    assert!(version.clock > 0);
    let first_clock = version.clock;

    node.write(
        &alice,
        &document,
        WriteMode::AtCurrent,
        [(key("main"), note("changed"))],
        [],
    )
    .await
    .unwrap();
    let mut clock = first_clock;
    while clock <= first_clock {
        let (_, _, datum) = poll.next_datum().await;
        if let ObservedDatum::Present { version, .. } = &datum {
            clock = version.clock;
        }
    }

    node.write(&alice, &document, WriteMode::AtCurrent, [], [key("main")])
        .await
        .unwrap();
    loop {
        let (_, _, datum) = poll.next_datum().await;
        if matches!(datum, ObservedDatum::Removed { .. }) {
            break;
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn a_scoped_caller_needs_an_anchor_that_reaches_the_document() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let mallory = node.writer("mallory").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("secret"))])
        .await
        .unwrap();

    let own = node
        .create(&mallory, "mallory-create", [(key("main"), note("mine"))])
        .await
        .unwrap();
    let capability = node
        .capability(
            &mallory,
            DocScope {
                read: BTreeSet::from([own.doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();

    // An `IsDoc` anchor re-authorizes the named document, so a doc the caller cannot read is
    // refused before any page is read.
    let refused = Poller::doc(node.client(), capability, created.doc_id.clone())
        .page(0)
        .await;
    assert!(
        matches!(refused, Err(ClientError::Method(DocsError::Denied))),
        "{refused:?}"
    );

    // A tag predicate names no resource, so a scoped caller cannot use it: it would enumerate
    // documents the capability does not bound.
    let refused = Poller::tag(
        node.client(),
        capability,
        big_cabinet_core::DatumTag::new(NOTE).unwrap(),
    )
    .page(0)
    .await;
    assert!(
        matches!(refused, Err(ClientError::Method(DocsError::Denied))),
        "{refused:?}"
    );

    // The document the caller does read replays its own state and nothing of Alice's.
    let mut own_poll = Poller::doc(node.client(), capability, own.doc_id.clone());
    let (doc_id, _, datum) = own_poll.next_datum().await;
    assert_eq!(doc_id, own.doc_id);
    assert!(matches!(datum, ObservedDatum::Present { .. }), "{datum:?}");
    node.stop().await;
}

#[tokio::test]
async fn a_sole_doc_poll_is_not_woken_by_writes_to_other_documents() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let watched = node
        .create(&alice, "watched", [(key("main"), note("one"))])
        .await
        .unwrap()
        .doc_id;
    let other = node
        .create(&alice, "other", [(key("main"), note("elsewhere"))])
        .await
        .unwrap()
        .doc_id;
    // The capability reads both documents, so any leak is the predicate's filtering and not
    // the capability's reach.
    let capability = node
        .capability(
            &alice,
            DocScope {
                read: BTreeSet::from([watched.clone(), other.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    // `IsDoc` alone would hide a leak, because `matching_docs` rejects the other document
    // anyway. Adding a tag clause makes every note write a predicate fact, which without the
    // `sole_doc` filter marks the other document as moved and answers it with a `leave`.
    let mut poll = Poller::new(
        node.client(),
        PollDatums {
            capability,
            predicate: Predicate::And {
                clauses: vec![
                    Predicate::IsDoc {
                        doc_id: watched.clone(),
                    },
                    Predicate::HasTag {
                        tag: big_cabinet_core::DatumTag::new(NOTE).unwrap(),
                    },
                ],
            },
            datums: DatumSelection::All,
            after: 0,
            limit: 0,
            hold_ms: 0,
        },
    );
    let replay = poll.page(0).await.unwrap();
    assert!(replay.caught_up);
    assert!(
        replay
            .events
            .iter()
            .all(|event| event_doc(event) == &watched),
        "{:?}",
        replay.events
    );

    node.write(
        &alice,
        &other,
        WriteMode::AtCurrent,
        [(key("main"), note("changed elsewhere"))],
        [],
    )
    .await
    .unwrap();
    // A held poll over the other document's write returns empty once the hold expires, with
    // the cursor moved past the unrelated entry.
    let quiet = poll.page(500).await.unwrap();
    assert!(quiet.events.is_empty(), "{:?}", quiet.events);

    node.write(
        &alice,
        &watched,
        WriteMode::AtCurrent,
        [(key("main"), note("two"))],
        [],
    )
    .await
    .unwrap();
    let (doc_id, _, datum) = poll.next_datum().await;
    assert_eq!(doc_id, watched);
    assert!(matches!(datum, ObservedDatum::Present { .. }), "{datum:?}");
    node.stop().await;
}

fn event_doc(event: &DatumEvent) -> &DocId {
    match event {
        DatumEvent::Datum { doc_id, .. } | DatumEvent::Leave { doc_id, .. } => doc_id,
    }
}
