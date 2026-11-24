//! Keyhive ingest from a peer with no standing (ADR 005 section 2).
//!
//! A connected peer can put keyhive events in front of a node whether or not any drawer, group or
//! document grants it anything: `SubductionKeyhive::authorize_connect` is permissive and
//! `KeyhiveProtocol::handle_message` checks only the message signature before `handle_sync_ops`
//! ingests whatever `ops` the message carries (it does not check that the ops were asked for).
//! These tests deliver a hostile `SyncOps` to the protocol handler, which is exactly what the
//! transport's keyhive handler does with a received message, and assert the safe behaviour:
//! a peer with no standing makes the receiver store nothing about documents it is not a member of.
//!
//! | Test | Invariant |
//! |------|-----------|
//! | `stranger_cannot_found_a_document_on_a_peer` | An unsolicited `SyncOps` carrying a peer's own document history (founding delegation, CGKA ops, prekey ops) leaves the receiver without that document and without its events in the keyhive event log. |
//! | `stranger_cannot_park_pending_events_on_a_peer` | An unsolicited `SyncOps` of CGKA ops for a document the receiver does not hold is not persisted as pending. |

use super::harness::Pair;
use crate::Res;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::BTreeSet;
use subduction_keyhive::KeyhivePeerId;
use subduction_keyhive::KeyhiveStorage;
use subduction_keyhive::SignedMessage;
use subduction_keyhive::message::Message;
use subduction_keyhive::message::RequestId;

/// The hashes of every event `repo`'s keyhive event log holds, admitted or still pending.
async fn logged_events(repo: &crate::BigRepo) -> Res<BTreeSet<[u8; 32]>> {
    Ok(repo
        .keyhive_storage
        .load_events()
        .await
        .map_err(|error| crate::ferr!("loading the keyhive event log: {error}"))?
        .into_iter()
        .map(|(hash, _)| *hash.as_bytes())
        .collect())
}

/// Sign `ops` as a `SyncOps` from the node booted with `sender_seed` to `target` and hand it to
/// `target_repo`'s protocol handler, the way the keyhive handler does for a received message.
async fn deliver_sync_ops(
    sender_seed: [u8; 32],
    target_repo: &crate::BigRepo,
    sender: &KeyhivePeerId,
    ops: Vec<std::sync::Arc<[u8]>>,
) -> Res<()> {
    let message = Message::SyncOps {
        sender_id: sender.clone(),
        target_id: target_repo.keyhive_protocol.peer_id(),
        request_id: RequestId {
            requestor: sender.clone(),
            nonce: 0,
        },
        ops,
        sync_responder_total: 0,
        sync_requester_total: 0,
    };
    let mut payload = Vec::new();
    ciborium::into_writer(&message, &mut payload)?;
    let signer = keyhive_crypto::signer::memory::MemorySigner::from(
        ed25519_dalek::SigningKey::from_bytes(&sender_seed),
    );
    let signed = signer
        .try_sign_sync(payload)
        .map_err(|error| crate::ferr!("signing the hostile message: {error}"))?;
    let signed = SignedMessage::new(bincode::serialize(&signed)?);
    target_repo
        .keyhive_protocol
        .handle_message(sender, signed, None)
        .await
        .map_err(|error| crate::ferr!("the receiver's handler failed: {error}"))?;
    Ok(())
}

/// Boots a connected pair, has the left node create a private document nobody is granted, and
/// returns the pair, the document's id and the keyhive events that creation appended to the
/// left node's log.
async fn pair_with_private_doc(
    seeds: (u8, u8),
) -> Res<(Pair, crate::DocumentId, Vec<Vec<u8>>)> {
    let pair = Pair::boot(seeds.0, seeds.1, "Stranger", "Relay").await?;
    let before = logged_events(&pair.left().repo).await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| automerge::transaction::Transactable::put(tx, automerge::ROOT, "title", "private"))
        .map_err(|error| crate::ferr!("{error:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = doc.document_id();
    let events = pair
        .left()
        .repo
        .keyhive_storage
        .load_events()
        .await
        .map_err(|error| crate::ferr!("loading the keyhive event log: {error}"))?
        .into_iter()
        .filter(|(hash, _)| !before.contains(hash.as_bytes()))
        .map(|(_, bytes)| bytes)
        .collect::<Vec<_>>();
    assert!(
        !events.is_empty(),
        "creating a document must append keyhive events"
    );
    assert!(
        !pair.right().repo.has_keyhive_document(&doc_id).await?,
        "precondition: the receiver must not hold the document before the hostile message"
    );
    Ok((pair, doc_id, events))
}

#[tokio::test(flavor = "multi_thread")]
async fn stranger_cannot_found_a_document_on_a_peer() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (pair, doc_id, events) = pair_with_private_doc((170, 171)).await?;
    let relay_log_before = logged_events(&pair.right().repo).await?;

    let sender = pair.left().repo.keyhive_protocol.peer_id();
    let ops = events.into_iter().map(Into::into).collect::<Vec<_>>();
    deliver_sync_ops(pair.left().identity_seed, &pair.right().repo, &sender, ops).await?;

    assert!(
        !pair.right().repo.has_keyhive_document(&doc_id).await?,
        "a peer with no standing founded a document in the receiver's keyhive graph"
    );
    let relay_log_after = logged_events(&pair.right().repo).await?;
    assert_eq!(
        relay_log_before,
        relay_log_after,
        "the receiver stored {} keyhive events from a peer with no standing",
        relay_log_after.difference(&relay_log_before).count()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stranger_cannot_park_pending_events_on_a_peer() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let (pair, _doc_id, events) = pair_with_private_doc((172, 173)).await?;
    let relay_log_before = logged_events(&pair.right().repo).await?;

    // Only the CGKA ops: the receiver holds no such document, so they can never resolve and
    // sit in keyhive's pending set (replayed on every later ingest) and in the event log.
    let ops = events
        .into_iter()
        .filter(
            |bytes| match bincode::deserialize::<StaticEvent<Vec<u8>>>(bytes) {
                Ok(event) => matches!(event, StaticEvent::CgkaOperation(_)),
                Err(error) => panic!("the creator's own log held an unreadable event: {error}"),
            },
        )
        .map(Into::into)
        .collect::<Vec<_>>();
    assert!(!ops.is_empty(), "document creation must emit CGKA ops");
    let sender = pair.left().repo.keyhive_protocol.peer_id();
    deliver_sync_ops(pair.left().identity_seed, &pair.right().repo, &sender, ops).await?;

    let relay_log_after = logged_events(&pair.right().repo).await?;
    assert_eq!(
        relay_log_before,
        relay_log_after,
        "the receiver parked {} unresolvable events from a peer with no standing",
        relay_log_after.difference(&relay_log_before).count()
    );
    Ok(())
}
