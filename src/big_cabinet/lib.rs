pub mod authority;
pub mod schema;

pub use authority::{
    ActorClaim, ActorPublicKey, AuthorityError, AuthorityRepo, CapabilityToken, DrawerId, Grant,
    Signature, TokenId,
};
pub use schema::{
    SchemaHandle, SchemaRegistration, SchemaRegistrationGroup, SchemaRepo, SchemaRepoError,
};

mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::Res;
use big_repo::SharedBigRepo;
use std::sync::Arc;

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
    // mutations
    // create_doc(facets, actor_capability)
    // write_doc(doc_id, facets, actor_revision, actor_capability)
}
