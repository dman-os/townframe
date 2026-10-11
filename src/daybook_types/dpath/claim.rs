//! Dpath claims as they cross the CRDT boundary (FDR 001 §2/§4).

use crate::interlude::*;

use crate::doc::DocId;

use super::facet::DpathFacet;
use super::label::{Dpath, DpathError};

/// A dpath claim exactly as it arrives from the CRDT: the facet key-id is a raw
/// string, because a replica may hold a malformed one (FDR 001 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawClaim {
    pub doc_id: DocId,
    /// The dpath as it appears in the facet key-id, unvalidated.
    pub dpath: String,
    pub facet: DpathFacet,
    /// The dpath facet's change metadata (`dmeta.createdAt`), FDR 001 §3
    /// tiebreaker 2. Unknown timestamps sort last.
    pub assigned_at: Option<Timestamp>,
}

impl RawClaim {
    /// Validate the raw dpath into a typed claim.
    pub fn validate(&self) -> Result<DpathClaim, DpathError> {
        Ok(DpathClaim {
            doc_id: self.doc_id.clone(),
            dpath: Dpath::parse(&self.dpath)?,
            facet: self.facet.clone(),
            assigned_at: self.assigned_at,
        })
    }
}

/// A validated dpath claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpathClaim {
    pub doc_id: DocId,
    pub dpath: Dpath,
    pub facet: DpathFacet,
    pub assigned_at: Option<Timestamp>,
}
