//! `ObserveFacets`: doc-scoped observation over the ungated `facet_set`
//! projection, filtered by the same read access as gated reads (ADR 009 §6).

use super::*;
use futures::StreamExt;

/// The next frontier entry on an observation stream, skipping the
/// replay-complete marker.
async fn next_entry(
    events: &mut (impl futures::Stream<Item = Result<FacetObserved, ClientError<DocsError>>> + Unpin),
) -> (DocId, FacetKey, ObservedFacet) {
    loop {
        match events.next().await.unwrap().unwrap() {
            FacetObserved::Entry {
                doc_id,
                facet_key,
                facet,
                ..
            } => return (doc_id, facet_key, facet),
            FacetObserved::ReplayComplete { .. } => continue,
        }
    }
}

#[tokio::test]
async fn doc_observation_replays_then_streams_membership_and_versions() {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let created = node
        .create(&alice, "create", [(key("main"), note("hello"))])
        .await
        .unwrap();
    let document = created.doc_id.clone();
    let mut events = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability: node
                    .capability(
                        &alice,
                        DocScope {
                            read: BTreeSet::from([document.clone()]),
                            ..DocScope::default()
                        },
                    )
                    .await
                    .unwrap(),
                scope: ObserveScope::Doc {
                    doc_id: document.clone(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    let (doc_id, facet_key, facet) = next_entry(&mut events).await;
    assert_eq!(doc_id, document);
    assert_eq!(facet_key, key("main"));
    let ObservedFacet::Present { version, .. } = &facet else {
        panic!("expected a present replay entry, got {facet:?}");
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
        let (_, _, facet) = next_entry(&mut events).await;
        if let ObservedFacet::Present { version, .. } = &facet {
            clock = version.clock;
        }
    }

    node.write(&alice, &document, WriteMode::AtCurrent, [], [key("main")])
        .await
        .unwrap();
    loop {
        let (_, _, facet) = next_entry(&mut events).await;
        if matches!(facet, ObservedFacet::Removed { .. }) {
            break;
        }
    }
    drop(events);
    node.stop().await;
}

#[tokio::test]
async fn tag_observation_hides_documents_the_observer_cannot_read() {
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

    // Doc scope refuses at establishment, like a gated read.
    let refused = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability,
                scope: ObserveScope::Doc {
                    doc_id: created.doc_id.clone(),
                },
                after: 0,
            },
        )
        .await;
    assert!(
        matches!(refused, Err(ClientError::Operation(DocsError::Denied))),
        "{refused:?}"
    );

    let mut events = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability,
                scope: ObserveScope::Tag {
                    tag: big_cabinet_core::FacetTag::new(NOTE).unwrap(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    // Alice's document was never delivered to Mallory, so it is silently
    // omitted rather than named by an `AccessLost` entry: the first entry is
    // Mallory's own document.
    let (doc_id, _, facet) = next_entry(&mut events).await;
    assert_eq!(doc_id, own.doc_id);
    assert!(matches!(facet, ObservedFacet::Present { .. }), "{facet:?}");
    drop(events);
    node.stop().await;
}
