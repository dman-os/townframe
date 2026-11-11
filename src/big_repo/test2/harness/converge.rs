//! Fixed-point convergence for multi-node scenarios.
//!
//! The product mints follow-up facts in reaction to what it admits: a reader
//! that materializes mints a CGKA op and a causal checkpoint, the owner that
//! admits those mints its own seating op and a checkpoint for the new epoch,
//! and each of those must travel back to the others, which can in turn react.
//! A hand-ordered sync script covers a fixed number of those rounds and races
//! the next one, so a test that asserts parity right after its script flakes.
//! [`converge`] instead repeats full sync rounds over every given link until a
//! whole round changes no node's fingerprint.
//!
//! The scenario under test stays scripted by the test; call this only before
//! the final parity / divergence assertions.

use crate::BigRepoConnection;
use crate::DocumentId;
use crate::Res;
use crate::test2::harness::Node;

/// Rounds after which non-convergence is a product finding, not a flake.
const MAX_ROUNDS: usize = 8;

/// One node's observable state: the keyhive ledger plus, per doc, the
/// sedimentree and materialized heads. This is what the parity and divergence
/// checks read.
async fn fingerprint(node: &Node, docs: &[DocumentId]) -> Res<String> {
    let ledger = node.store.keyhive_event_ledger().await?;
    let mut out = format!(
        "{}[logged={} admitted={} head={}",
        node.label, ledger.logged, ledger.admitted, ledger.admission_head
    );
    for doc_id in docs {
        let state = node.repo.doc_head_state(doc_id.clone()).await?;
        out.push_str(&format!(
            " {doc_id}:sed={:?} mat={:?} {:?}",
            state.sedimentree_heads, state.materialized_heads, state.state
        ));
    }
    out.push(']');
    Ok(out)
}

/// Sync keyhive and `docs` over every directed link in `links` (pass both
/// directions of each edge), quiesce every node, and repeat until a full round
/// leaves every fingerprint unchanged. Panics with the per-round fingerprints
/// after [`MAX_ROUNDS`].
pub(crate) async fn converge(
    nodes: &[&Node],
    links: &[&BigRepoConnection],
    docs: &[DocumentId],
) -> Res<()> {
    let mut history = Vec::new();
    let mut previous = Vec::new();
    for node in nodes {
        previous.push(fingerprint(node, docs).await?);
    }
    history.push(previous.clone());
    for round in 1..=MAX_ROUNDS {
        for link in links {
            link.sync_keyhive_with_peer().await?;
            for doc_id in docs {
                link.sync_doc_with_peer(doc_id.clone()).await?;
            }
        }
        for node in nodes {
            node.repo.wait_for_quiescence(None).await?;
        }
        let mut current = Vec::new();
        for node in nodes {
            current.push(fingerprint(node, docs).await?);
        }
        let settled = current == previous;
        history.push(current.clone());
        if settled {
            tracing::warn!(rounds = round, "INSTR converge rounds"); // INSTR
            return Ok(());
        }
        previous = current;
    }
    panic!(
        "converge: no fixed point after {MAX_ROUNDS} rounds; per-round fingerprints (round 0 is the start): {history:#?}"
    );
}
