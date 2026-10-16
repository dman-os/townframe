//! History-backed document read evidence.
//!
//! This module is deliberately below the ordinary application read facade.  It reconstructs the
//! logical claim for a dmeta signature from the Automerge change that introduced the signature;
//! callers still have to perform actor attribution and node authorization separately.

mod claim_history;

use automerge::{Automerge, ChangeHash, ExId};
use big_cabinet_core::{DocumentId, FacetKey};

pub(crate) use claim_history::{
    ClaimHistoryError, ClaimHistoryResult, HistoryBudget, ReconstructedClaim,
    SignatureIntroduction, reconstruct_claim, reconstruct_claim_for_marker,
    reconstruct_current_claims,
};

/// The production DocumentsRepo evidence seam.  It intentionally accepts a loaded Automerge
/// document and an explicit frontier: storage loading, node-authority admission, schema checks,
/// and actor-reference resolution happen at the repository boundaries that call this seam.
///
/// A signature marker is mapped to its introducing change with Automerge's native `hash_for_opid`
/// index; no serialized claim object or duplicate claim fields are created here.
pub(crate) fn reconstruct_claim_evidence(
    doc: &Automerge,
    origin_document: &DocumentId,
    facet_key: &FacetKey,
    marker: &ExId,
    budget: HistoryBudget,
) -> ClaimHistoryResult {
    reconstruct_claim_for_marker(doc, origin_document, facet_key, marker, budget)
}

/// Reconstruct all claims in the current competing signature set at an explicit read frontier.
/// This is evidence hydration only; it is not current capability or node authorization.
pub(crate) fn reconstruct_current_claim_evidence(
    doc: &Automerge,
    origin_document: &DocumentId,
    facet_key: &FacetKey,
    heads: &[ChangeHash],
    budget: HistoryBudget,
) -> Result<Vec<ReconstructedClaim>, ClaimHistoryError> {
    reconstruct_current_claims(doc, origin_document, facet_key, heads, budget)
}
