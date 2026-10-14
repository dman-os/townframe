#![expect(dead_code)]

mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod index;

use crate::interlude::*;
use std::sync::Arc;

pub use index::facet_set::{
    Availability, FacetMembership, FacetRecord, FacetRoute, FacetSetDelta, FacetSetError,
    FacetSetQuery, FacetSetStore, ProjectionCursor, ProjectionOutcome,
};

use big_repo::SharedBigRepo;

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
    // mutations
    // create_doc(facets, actor_capability)
    // write_doc(doc_id, facets, actor_revision, actor_capability)
}
