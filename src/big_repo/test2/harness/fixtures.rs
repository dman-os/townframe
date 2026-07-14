//! Test fixtures: contact-card exchange, access grants, and sync helpers.
//!
//! Every helper here is **synchronous-by-design**: one `sync_*` call is the
//! barrier. There are no retry loops. If an expected post-condition is not met
//! after a single `sync_keyhive_with_peer` / `sync_doc_with_peer`, the helper
//! returns `Err` with a diagnostic — surfacing the runtime2 ordering bug
//! rather than papering over it (see `play.big_repo.test2.md`).

use super::log_nickname;
use super::topo::{Node, Pair};
use crate::{BigKeyhiveAgent, DocumentId, Res};
use keyhive_core::access::Access;
use std::sync::Arc;
use subduction_keyhive::KeyhivePeerId;

/// Look up `peer`'s agent in `repo`'s keyhive — a single call.
///
/// Valid after [`Pair::boot`] has run the contact-card exchange. A `None` here
/// means the keyhive sync did not actually deliver the agent — a bug.
pub async fn agent_of(repo: &crate::BigRepo, peer: &Node) -> Res<BigKeyhiveAgent> {
    let kh_peer_id = KeyhivePeerId::from_bytes(*peer.peer_id().as_bytes());
    repo.keyhive()
        .get_agent_by_peer_id(&kh_peer_id)
        .await?
        .ok_or_else(|| {
            crate::ferr!(
                "agent for {} not present in {}'s keyhive after contact-card exchange",
                log_nickname::nickname(&peer.peer_id()),
                log_nickname::nickname(&repo.local_peer_id()),
            )
        })
}

/// Grant `access` on `doc_id` to `grantee` (resolved on the owner) and
/// propagate the membership change to the reader via a single bidirectional
/// keyhive sync.
///
/// Post-condition (asserted, not polled): the reader sees its access on the
/// document.
pub async fn grant_and_propagate(
    pair: &Pair,
    doc_id: DocumentId,
    grantee: &BigKeyhiveAgent,
    access: Access,
) -> Res<()> {
    pair.left()
        .repo
        .grant_doc_access(doc_id, grantee.clone(), access)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    assert_reader_has_access(&pair.right().repo, doc_id).await?;
    super::keyhive::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    Ok(())
}

/// Assert the reader's keyhive reflects access on `doc_id` — single lookup.
pub async fn assert_reader_has_access(
    repo: &crate::BigRepo,
    doc_id: DocumentId,
) -> Res<()> {
    let peer = repo.local_peer_id();
    let agent_key = ed25519_dalek::VerifyingKey::from_bytes(peer.as_bytes())
        .expect("peer id must be a verifying key");
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("document id must be a verifying key");
    let agent = keyhive_core::principal::identifier::Identifier::from(agent_key);
    let document = keyhive_core::principal::identifier::Identifier::from(doc_key);
    if repo
        .keyhive()
        .agent_access_on(&agent, document)
        .await
        .is_some()
    {
        Ok(())
    } else {
        Err(crate::ferr!(
            "{} has no access on {} after grant + keyhive sync",
            log_nickname::nickname(&peer),
            log_nickname::nickname(&peer), // doc_id has no nickname; peer stands in
        ))
    }
}

/// Sync a document and expect it to be fully materialized (Ready) on `repo`.
///
/// One `sync_doc_with_peer` call is the barrier. If the doc is not `Ready`
/// (or still `PendingMaterialization`) on return, the sync's synchronous
/// ack/apply guarantee is broken — returned as `Err`.
pub async fn sync_doc_expect_ready(
    conn: &crate::BigRepoConnection,
    repo: &Arc<crate::BigRepo>,
    doc_id: DocumentId,
) -> Res<crate::BigDocHandle> {
    conn.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    repo.wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;
    match repo.get_doc(&doc_id).await? {
        crate::DocLookup::Ready(handle) => Ok(handle),
        crate::DocLookup::PendingMaterialization => Err(crate::ferr!(
            "{}: doc still PendingMaterialization after a single sync_doc_with_peer — \
             sync barrier did not await materialization",
            log_nickname::nickname(&repo.local_peer_id()),
        )),
        crate::DocLookup::Missing => Err(crate::ferr!(
            "{}: doc Missing after sync_doc_with_peer",
            log_nickname::nickname(&repo.local_peer_id()),
        )),
    }
}
