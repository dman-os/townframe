mod interlude {
    pub use utils_rs::prelude::*;
}

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
