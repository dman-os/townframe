//! DocumentsRepo: documents as maps of signed facets, read and written as whole JSON values at
//! explicit heads.
//!
//! - [`protocol`]: the wire surface.
//! - `service`: the hosted handler, the write pipeline, and boot.
//! - `gate`: the authenticity check every read and every write base passes through.
//! - [`access`]: the policy seam deciding who may read, write, and create.
//! - `commands`: the idempotency and crash-recovery ledger.
//! - `claim_history`: reconstruction of a signed claim from the change that introduced it.
//!
//! TODO(big-repo-delete): DocumentsRepo exposes no Delete operation, and a partially created
//! branch cannot be rolled back, because BigRepo has no physical document-deletion primitive —
//! `abandon_allocation` deletes only the reservation row, nothing from the document's events,
//! sedimentree, or bytes. Branch documents are therefore committed with no coparents/groups, so
//! an unregistered branch never leaves this node, and branch recovery rolls forward instead of
//! back. Add document deletion to BigRepo before exposing Delete or branch rollback.

pub mod access;
mod claim_history;
mod commands;
mod gate;
pub mod protocol;
mod service;

#[cfg(test)]
mod e2e;
#[cfg(test)]
mod tests;

pub use access::{AccessError, AccessFuture, AccessRequest, DocumentAccess, LedgerAccess};
pub use protocol::{
    At, BranchDocument, CommandId, CreateDocument, DocumentsError, DocumentsProtocol,
    EvidenceScope, FacetClaimEvidence, FacetClaimsInspection, FacetListing, FacetRead, FacetsRead,
    InspectFacetClaims, ListFacets, MergeDocument, ReadFacets, SignatureEvidence, Unaccepted,
    UnsafeFacetInspection, UnsafeInspectFacet, VerifiedSignatureEvidence, WriteFacets, WriteMode,
    WriteReceipt,
};
pub use service::{DocumentsConfig, DocumentsHandler, DocumentsRepo, DocumentsService, boot};

pub use claim_history::{
    ClaimHistoryEntry, ClaimHistoryError, ClaimHistoryResult, HistoryBudget, ReconstructedClaim,
    SignatureIntroduction, predecessor_ids, reconstruct_claim, reconstruct_claim_for_marker,
    reconstruct_current_claims,
};
