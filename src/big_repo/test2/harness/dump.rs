//! Rich per-node diagnostics, printed on assertion failure.
//!
//! Per `play.big_repo.test2.md`: on parity failure, dump per-node head-set /
//! payload / parts / access / lookup-state, then run a single re-sync probe
//! to distinguish a *timing* flake (resolves on re-sync) from a *correctness*
//! bug (persists). Today this module renders the per-node snapshot; the
//! re-sync probe is wired by the ladder rungs calling `tier0_invariants`
//! again after an explicit re-sync.

use super::heads::state_summary;
use super::log_nickname;
use super::topo::Pair;
use crate::{DocumentId, Res};
use std::sync::Arc;

/// Dump a per-node diagnostic snapshot of `doc_id` across the pair.
///
/// Intended to be called (or injected into an assertion message) when a
/// Tier-0 check fails, so the failure output names each node and shows its
/// sedimentree/materialized head counts + materialization state.
pub async fn diagnostics(pair: &Pair, doc_id: DocumentId) -> Res<String> {
    let left = pair.left().repo.doc_head_state(doc_id).await?;
    let right = pair.right().repo.doc_head_state(doc_id).await?;
    let left_lookup = lookup_summary(&pair.left().repo, doc_id).await;
    let right_lookup = lookup_summary(&pair.right().repo, doc_id).await;
    Ok(format!(
        "\n[diagnostics doc={doc_id}]\n  {}\n    {}\n    {}\n  {}\n    {}\n    {}\n",
        pair.left().label,
        state_summary(pair.left().label, &left),
        left_lookup,
        pair.right().label,
        state_summary(pair.right().label, &right),
        right_lookup,
    ))
}

async fn lookup_summary(repo: &Arc<crate::BigRepo>, doc_id: DocumentId) -> String {
    let nick = log_nickname::nickname(&repo.local_peer_id());
    match repo.get_doc(&doc_id).await {
        Ok(crate::DocLookup::Ready(_)) => format!("{nick}: lookup=Ready"),
        Ok(crate::DocLookup::PendingMaterialization) => format!("{nick}: lookup=PendingMaterialization"),
        Ok(crate::DocLookup::Missing) => format!("{nick}: lookup=Missing"),
        Err(e) => format!("{nick}: lookup=error({e})"),
    }
}
