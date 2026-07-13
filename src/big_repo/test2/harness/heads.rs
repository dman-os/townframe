//! Tier-0 invariant assertions — run after *every* scenario.
//!
//! These are the flake detectors. Per `play.big_repo.test2.md` Tier 0:
//! - sedimentree-heads parity across all nodes holding the doc;
//! - materialized-heads parity across readers with read access;
//! - (security) no plaintext materializes without access — Tier 8;
//! - (encryption) stored blobs encrypted — Tier 8.
//!
//! The security/encryption checks live with Tier 8 (they need raw blob
//! access); Tier 0 here covers the head-parity invariants that the
//! head-divergence flake violated.

use super::log_nickname;
use super::topo::Pair;
use crate::{DocumentId, Res};

/// Sort heads into a canonical order for order-independent comparison.
fn sorted(heads: &mut Vec<automerge::ChangeHash>) {
    heads.sort_by_key(|h| h.0);
}

/// Assert sedimentree-heads parity between the two nodes of a [`Pair`].
pub async fn assert_sedimentree_parity(pair: &Pair, doc_id: DocumentId) -> Res<()> {
    let left = pair.left().repo.doc_head_state(doc_id).await?;
    let right = pair.right().repo.doc_head_state(doc_id).await?;
    let (mut l, mut r) = (
        left.sedimentree_heads.to_vec(),
        right.sedimentree_heads.to_vec(),
    );
    sorted(&mut l);
    sorted(&mut r);
    if l != r {
        return Err(crate::ferr!(
            "sedimentree-heads parity violated: {} = {:?}, {} = {:?}",
            pair.left().label,
            l,
            pair.right().label,
            r,
        ));
    }
    Ok(())
}

/// Assert materialized-heads parity between the two nodes of a [`Pair`],
/// for nodes that have a materialized (live) document.
///
/// `None` (not yet materialized) on a side that should have the doc is a
/// separate failure caught by the scenario itself; here we only compare when
/// both sides report materialized heads.
pub async fn assert_materialized_parity(pair: &Pair, doc_id: DocumentId) -> Res<()> {
    let left = pair.left().repo.doc_head_state(doc_id).await?;
    let right = pair.right().repo.doc_head_state(doc_id).await?;
    let (mut l, mut r) = (
        left.materialized_heads.map(|h| h.to_vec()),
        right.materialized_heads.map(|h| h.to_vec()),
    );
    if let (Some(ref mut l), Some(ref mut r)) = (&mut l, &mut r) {
        sorted(l);
        sorted(r);
        if l != r {
            return Err(crate::ferr!(
                "materialized-heads parity violated: {} = {:?}, {} = {:?}",
                pair.left().label,
                l,
                pair.right().label,
                r,
            ));
        }
    }
    Ok(())
}

/// Run the full Tier-0 invariant set after a scenario.
pub async fn tier0_invariants(pair: &Pair, doc_id: DocumentId) -> Res<()> {
    assert_sedimentree_parity(pair, doc_id).await?;
    assert_materialized_parity(pair, doc_id).await?;
    Ok(())
}

/// Render a one-line state summary for diagnostics.
#[allow(dead_code)]
pub fn state_summary(label: &str, state: &crate::runtime2::DocHeadState) -> String {
    let mat = state
        .materialized_heads
        .as_ref()
        .map(|h| format!("{} head(s)", h.len()))
        .unwrap_or_else(|| "unmaterialized".to_string());
    format!(
        "{label}: sedimentree={} materialized={} state={:?}",
        state.sedimentree_heads.len(),
        mat,
        state.state,
    )
}

#[allow(dead_code)]
fn _nickname_used(_p: &crate::PeerId) -> String {
    log_nickname::nickname(_p)
}
