//! History-backed document read evidence.
//!
//! This module is deliberately below the ordinary application read facade.  It reconstructs the
//! logical claim for a dmeta signature from the Automerge change that introduced the signature;
//! callers still have to perform actor attribution and node authorization separately.

mod claim_history;

pub use claim_history::{
    ClaimHistoryError, ClaimHistoryResult, HistoryBudget, ReconstructedClaim,
    SignatureIntroduction, reconstruct_claim, reconstruct_claim_for_marker,
    reconstruct_current_claims,
};
