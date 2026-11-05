mod interlude {
    pub use utils_rs::prelude::*;
}

pub use wash_plugin_wflow;
pub use wflow_core;
pub use wflow_tokio;
pub use wflow_tokio::authority_runtime::AuthorityRuntime;
pub use wflow_tokio::authority_runtime::CancelError;
pub use wflow_tokio::authority_runtime::Submission;
pub use wflow_tokio::authority_runtime::SubmitError;
pub use wflow_wdk;

mod kvstore {
    pub mod atomic;
}

use crate::interlude::*;

use sqlite_rs as sqlite_rs_alias;
use sqlite_rs::Database;
use wflow_core::r#gen::types::PartitionId;

use crate::kvstore::atomic::SqliteFactory;

// The initial authority storage generation is deliberately separate from the
// journal authority epoch, which advances on every runtime open. A future
// destructive namespace migration must bump this incarnation.
const AUTHORITY_PARTITION_INCARNATION: u64 = 0;

// pub struct Config {}

#[derive(Clone)]
pub struct Ctx {
    authority_factory: Arc<SqliteFactory>,
}

impl Ctx {
    pub async fn init(db: Database) -> Res<Self> {
        let authority_factory = Arc::new(SqliteFactory::boot(db).await?);
        Ok(Self { authority_factory })
    }

    /// Start the production authority for one partition.
    ///
    /// The atomic journal is keyed by the stable `wflow_authority_{partition_id}`
    /// namespace, so reopening the same partition recovers its durable state
    /// instead of creating a new journal. The partition incarnation remains 0
    /// for this initial storage generation; it is distinct from the journal's
    /// per-open authority epoch and must only change for a destructive namespace
    /// migration.
    pub async fn start_authority_runtime(
        &self,
        partition_id: PartitionId,
        host: Arc<dyn wflow_tokio::authority_runtime::session::Host>,
    ) -> Res<AuthorityRuntime> {
        let namespace = format!("wflow_authority_{partition_id}");
        let authority_factory = &self.authority_factory;
        let writer = authority_factory.writer(&namespace).await?;
        AuthorityRuntime::start(writer, AUTHORITY_PARTITION_INCARNATION, host).await
    }
}

#[cfg(test)]
mod authority_facade_tests {
    use super::*;
    use sqlite_rs::RuntimeConfig;
    use sqlite_rs::SqliteRuntime;
    use wflow_tokio::local_native_host::LocalNativeHost;

    #[tokio::test]
    async fn authority_runtime_reopens_partition_namespace() -> Res<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("wflow.sqlite");
        let database = SqliteRuntime::new(RuntimeConfig::default()).database(&path, &[], "_unused");
        let context = Ctx::init(database).await?;

        let runtime = context
            .start_authority_runtime(0, Arc::new(LocalNativeHost::new()))
            .await?;
        runtime.stop().await;

        let runtime = context
            .start_authority_runtime(0, Arc::new(LocalNativeHost::new()))
            .await?;
        runtime.stop().await;
        Ok(())
    }
}
