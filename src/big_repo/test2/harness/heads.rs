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

use super::topo::Pair;
use crate::{DocumentId, Res};
use utils_rs::prelude::*;
/// Sort heads into a canonical order for order-independent comparison.
fn sorted(heads: &mut [automerge::ChangeHash]) {
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

/// Assert sedimentree-heads parity, polling until `deadline` for the frontier
/// to converge.
///
/// The doc-sync and part pipelines can race: a worker may be fetching a
/// newly published fragment while both runtime hubs are idle, so a one-shot
/// parity check right after the network-rest fence can observe an
/// intermediate Sedimentree frontier. Poll so a transiently-divergent
/// frontier converges instead of failing the scenario.
pub async fn assert_sedimentree_parity_with_deadline(
    pair: &Pair,
    doc_id: DocumentId,
    deadline: tokio::time::Instant,
) -> Res<()> {
    loop {
        match assert_sedimentree_parity(pair, doc_id).await {
            Ok(()) => return Ok(()),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Assert materialized-heads parity for the live handles held by the
/// scenario. We deliberately do not reacquire handles through `get_doc`: the
/// lookup/reload path is a separate runtime2 surface under investigation, and
/// must not turn a stale lookup bundle into a false parity failure.
pub async fn assert_materialized_parity_handles(
    left_label: &str,
    left: &crate::BigDocHandle,
    right_label: &str,
    right: &crate::BigDocHandle,
) -> Res<()> {
    let (mut l, mut r) = (
        left.with_document_read(|doc| doc.get_heads()).await,
        right.with_document_read(|doc| doc.get_heads()).await,
    );
    sorted(&mut l);
    sorted(&mut r);
    if l != r {
        return Err(crate::ferr!(
            "materialized-heads parity violated: {} = {:?}, {} = {:?}",
            left_label,
            l,
            right_label,
            r,
        ));
    }
    Ok(())
}

/// Run the full Tier-0 invariant set after a scenario.
pub async fn tier0_invariants(
    pair: &Pair,
    doc_id: DocumentId,
    left: &crate::BigDocHandle,
    right: &crate::BigDocHandle,
) -> Res<()> {
    // Runtime quiescence alone does not fence BigSync's external workers. A
    // worker may already be fetching a newly published fragment while both
    // runtime hubs are idle, which exposes an intermediate Sedimentree
    // frontier here. Fence the complete network + runtime fixed point.
    super::fixtures::wait_for_network_rest(
        &[pair.left(), pair.right()],
        utils_rs::scale_timeout(std::time::Duration::from_secs(30)),
    )
    .await?;

    // Poll parity past the rest fence: the fence waits on part cursors, but a
    // fragment fetch already in flight can still advance a node's
    // Sedimentree frontier just after rest is declared. Give the frontier a
    // scaled window to converge before failing the scenario.
    let parity_deadline =
        tokio::time::Instant::now() + utils_rs::scale_timeout(std::time::Duration::from_secs(30));
    if let Err(error) = assert_sedimentree_parity_with_deadline(pair, doc_id, parity_deadline).await
    {
        let diagnostics = super::dump::diagnostics(pair, doc_id).await?;
        return Err(crate::ferr!("{error}\n{diagnostics}"));
    }
    if let Err(error) =
        assert_materialized_parity_handles(pair.left().label, left, pair.right().label, right).await
    {
        let diagnostics = super::dump::diagnostics(pair, doc_id).await?;
        return Err(crate::ferr!("{error}\n{diagnostics}"));
    }
    Ok(())
}

/// Log both nodes' head states — for before/after-fence comparison when a
/// parity failure fires. The log line is emitted at debug level (enabled by
/// the stress harness), and the same state shows up verbatim in the failure
/// diagnostics via [`state_summary`].
pub async fn log_head_state(pair: &Pair, doc_id: DocumentId) -> Res<()> {
    let left = pair.left().repo.doc_head_state(doc_id).await?;
    let right = pair.right().repo.doc_head_state(doc_id).await?;
    debug!(
        %doc_id,
        "tier2 head state: {} | {}",
        state_summary(pair.left().label, &left),
        state_summary(pair.right().label, &right),
    );
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
    let sed_heads = format_heads(&state.sedimentree_heads);
    let mat_heads = state
        .materialized_heads
        .as_ref()
        .map(|h| format_heads(h))
        .unwrap_or_else(|| "-".to_string());
    format!(
        "{label}: sedimentree={} [{sed_heads}] materialized={mat} [{mat_heads}] state={:?}",
        state.sedimentree_heads.len(),
        state.state,
    )
}

/// Format a head list, flagging causal-checkpoint commits (`TFCASL01` prefix)
/// so a parity failure shows at a glance whether a node's frontier differs by
/// key-only coverage markers vs real content commits.
fn format_heads(heads: &[automerge::ChangeHash]) -> String {
    heads
        .iter()
        .map(|h| {
            let short: String = h.0.iter().take(8).map(|b| format!("{b:02x}")).collect();
            let id = sedimentree_core::loose_commit::id::CommitId::new(h.0);
            if crate::runtime2::support::is_causal_checkpoint_id(id) {
                format!("{short}(checkpoint)")
            } else {
                short
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}
