mod interlude {
    pub use api_utils_rs::{api, prelude::*};
}
use futures::{stream::BoxStream, StreamExt};

use crate::{interlude::*, plugin::bindings_metadata_store::townframe::wflow::metadata_store};

mod log;
mod partition;
mod plugin;

struct Ctx {
    metadata: Box<dyn MetdataStore>,
}
type SharedCtx = Arc<Ctx>;

// Contains information about what wflows exist
#[async_trait::async_trait]
trait MetdataStore: Send + Sync {
    async fn get_wflow(&self, key: Arc<str>) -> Option<metadata_store::WflowMeta>;
    async fn get_partitions(&self) -> metadata_store::PartitionsMeta;
}
