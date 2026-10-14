mod interlude {
    pub use utils_rs::prelude::*;
}

mod document;

use automerge::{Automerge, ChangeHash, ExId};
use big_cabinet_core::{DocumentId, FacetKey};
use big_repo::SharedBigRepo;
use document::{ClaimHistoryResult, HistoryBudget, ReconstructedClaim};
use std::sync::Arc;
use utils_rs::prelude::Res;

pub struct AuthorityRepo {}
impl AuthorityRepo {
    // methods
    // get_actor_capability
}

pub struct SchemaRepo {}

pub struct DocumentRepo {
    big_repo: SharedBigRepo,
    schema_repo: Arc<SchemaRepo>,
}

impl DocumentRepo {
    pub async fn boot(big_repo: SharedBigRepo, schema_repo: Arc<SchemaRepo>) -> Res<Self> {
        Ok(Self {
            big_repo,
            schema_repo,
        })
    }

    /// Hydrate one ordinary signature claim from a loaded document.  This is the evidence stage
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

    /// Hydrate the current competing signature set at an explicit document frontier.  The result
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

    // mutations
    // create_doc(facets, actor_capability)
    // write_doc(doc_id, facets, actor_revision, actor_capability)
}
