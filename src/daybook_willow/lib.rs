mod interlude {
    pub use big_sync_core::PeerId;
    pub use utils_rs::prelude::*;
}

mod store;
use store::WillowStore;

use crate::interlude::*;
use willow25::prelude::*;

#[derive(Debug, Clone)]
pub struct Config {
    pub peer_id: PeerId,
    pub secret_key_bytes: [u8; 32],
    pub storage: StorageConfig,
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    Disk { path: PathBuf },
    Memory,
}

pub struct BigWillow {
    store: WillowStore,
}

impl BigWillow {
    pub async fn boot(config: Config) -> Res<Arc<Self>> {
        let store = match config.storage {
            StorageConfig::Disk { path } => WillowStore::Persisted(
                willow25::storage::PersistentStore::new(path)
                    .await
                    .wrap_err("error opening PersistentStore fs store")?,
            ),
            StorageConfig::Memory => {
                WillowStore::Memory(willow25::storage::MemoryStore::new()) as _
            }
        };
        Ok(Self { store }.into())
    }
}
