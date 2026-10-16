mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod authority;
pub mod document;
pub mod index;
pub mod schema;

use automerge::{Automerge, ChangeHash, ExId};
use big_cabinet_core::{DocumentId, FacetKey};
use big_repo::SharedBigRepo;
use crate::document::{ClaimHistoryResult, HistoryBudget, ReconstructedClaim};
use crate::interlude::Res;
use std::sync::Arc;

pub use authority::{
    ActorClaim, ActorPublicKey, AuthorityError, AuthorityRepo, CapabilityToken, DrawerId, Grant,
    Signature, TokenId,
};
pub use index::facet_set::{
    Availability, FacetMembership, FacetRecord, FacetRoute, FacetSetDelta, FacetSetError,
    FacetSetQuery, FacetSetStore, ProjectionCursor, ProjectionOutcome,
};
pub use schema::{
    SchemaHandle, SchemaRegistration, SchemaRegistrationGroup, SchemaRepo, SchemaRepoError,
};

pub struct DocumentRepo {
    #[expect(
        dead_code,
        reason = "DocumentRepo wiring is reserved for the later document layer"
    )]
    big_repo: SharedBigRepo,
    #[expect(
        dead_code,
        reason = "DocumentRepo wiring is reserved for the later document layer"
    )]
    schema_repo: Arc<SchemaRepo>,
}

impl DocumentRepo {
    pub async fn boot(big_repo: SharedBigRepo, schema_repo: Arc<SchemaRepo>) -> Res<Self> {
        Ok(Self {
            big_repo,
            schema_repo,
        })
    }

    /// Hydrate one ordinary signature claim from a loaded document. This is the evidence stage
    /// of a DocumentsRepo read: storage loading, node-authority admission, schema validation, and
    /// actor-reference resolution remain explicit caller stages rather than being inferred here.
    pub(crate) fn claim_history_evidence(
        &self,
        doc: &Automerge,
        origin_document: &DocumentId,
        facet_key: &FacetKey,
        marker: &ExId,
        budget: HistoryBudget,
    ) -> ClaimHistoryResult {
        document::reconstruct_claim_evidence(doc, origin_document, facet_key, marker, budget)
    }

    /// Hydrate the current competing signature set at an explicit document frontier. The result
    /// is not an authorization decision and does not expose an arbitrary merged future value.
    pub(crate) fn current_claim_history_evidence(
        &self,
        doc: &Automerge,
        origin_document: &DocumentId,
        facet_key: &FacetKey,
        heads: &[ChangeHash],
        budget: HistoryBudget,
    ) -> Result<Vec<ReconstructedClaim>, document::ClaimHistoryError> {
        document::reconstruct_current_claim_evidence(doc, origin_document, facet_key, heads, budget)
    }
}
