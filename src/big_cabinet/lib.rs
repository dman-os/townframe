pub mod authority;

pub use authority::{
    ActorClaim, ActorPublicKey, AuthorityError, AuthorityRepo, CapabilityToken, DrawerId, Grant,
    Signature, TokenId,
};

use big_repo::SharedBigRepo;
use std::sync::Arc;
use utils_rs::prelude::Res;

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
}
