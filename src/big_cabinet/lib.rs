mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod authority;
pub mod document;
pub mod index;
pub mod schema;

use crate::interlude::Res;
use big_repo::SharedBigRepo;
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
}
