mod interlude {
    pub use api_utils_rs::{api, prelude::*};
}

use crate::interlude::*;

mod log;
mod metastore;
mod partition;
mod plugin;

struct Ctx {
    metadata: Arc<dyn metastore::MetdataStore>,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    fn new(metadata: Arc<dyn metastore::MetdataStore>) -> Arc<Self> {
        Arc::new(Self { metadata })
    }
}

#[async_trait]
trait KvStore {
    async fn count(&self) -> Res<u64>;
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>>;
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>>;
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>>;
}

#[async_trait]
impl KvStore for DHashMap<Arc<[u8]>, Arc<[u8]>> {
    async fn count(&self) -> Res<u64> {
        Ok(self.len() as u64)
    }
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.get(key).map(|v| v.value().clone()))
    }
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        Ok(self.insert(key, value))
    }
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.remove(key).map(|(_, val)| val))
    }
}
