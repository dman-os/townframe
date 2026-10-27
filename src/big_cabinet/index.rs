pub mod facet_ref;
pub mod facet_set;

pub use facet_ref::FacetRefEdge;
pub use facet_ref::FacetRefQuery;
pub use facet_ref::FacetRefStore;
pub use facet_set::FacetMembership;
pub use facet_set::FacetRecord;
pub use facet_set::FacetRoute;
pub use facet_set::FacetSetQuery;
pub use facet_set::FacetSetStore;

use crate::schema::SchemaRepo;
use big_repo::SharedBigRepo;
use futures::FutureExt;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken as ComponentStopToken;
use sqlite_rs::Database;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use utils_rs::prelude::Res;

pub struct Indexes {
    pub facet_set: Arc<FacetSetStore>,
    pub facet_ref: Arc<FacetRefStore>,
    set_tasks: Spawner,
    ref_tasks: Spawner,
}

impl Indexes {
    pub async fn boot(
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        sql: Database,
        concurrency: NonZeroUsize,
    ) -> Res<(Arc<Self>, StopToken)> {
        let facet_set = Arc::new(FacetSetStore::new(sql.clone()).await?);
        let facet_ref = Arc::new(FacetRefStore::new(sql).await?);
        let (set_stop, set_tasks) = ComponentStopToken::new();
        let (ready, started) = tokio::sync::oneshot::channel();
        let store = Arc::clone(&facet_set);
        let set_repo = Arc::clone(&repo);
        let owner = set_tasks.clone();
        set_tasks
            .spawn(async move {
                let mut ready = Some(ready);
                let result = store
                    .run_until(set_repo, concurrency, owner.cancelled(), &mut ready)
                    .await;
                if let Some(ready) = ready {
                    ready.send(result).expect("index boot waits for readiness");
                } else {
                    result.unwrap();
                }
            })
            .expect("index worker starts before shutdown");
        if let Err(error) = started
            .await
            .expect("index initialization reports its result")
        {
            set_stop.stop().await;
            return Err(error);
        }

        let (ref_stop, ref_tasks) = ComponentStopToken::new();
        let (ready, started) = tokio::sync::oneshot::channel();
        let store = Arc::clone(&facet_ref);
        let set = (*facet_set).clone();
        let owner = ref_tasks.clone();
        ref_tasks
            .spawn(async move {
                let mut ready = Some(ready);
                let result = store
                    .run_until(repo, schemas, set, owner.cancelled(), &mut ready)
                    .await;
                if let Some(ready) = ready {
                    ready.send(result).expect("index boot waits for readiness");
                } else {
                    result.unwrap();
                }
            })
            .expect("index worker starts before shutdown");
        let stop = StopToken { ref_stop, set_stop };
        if let Err(error) = started
            .await
            .expect("index initialization reports its result")
        {
            stop.stop().await;
            return Err(error);
        }
        Ok((
            Arc::new(Self {
                facet_set,
                facet_ref,
                set_tasks,
                ref_tasks,
            }),
            stop,
        ))
    }

    pub fn live_tasks(&self) -> usize {
        self.set_tasks.live_tasks() + self.ref_tasks.live_tasks()
    }
}

#[must_use]
pub struct StopToken {
    ref_stop: ComponentStopToken,
    set_stop: ComponentStopToken,
}

impl StopToken {
    pub async fn stop(self) {
        let reference = AssertUnwindSafe(self.ref_stop.stop()).catch_unwind().await;
        let facets = AssertUnwindSafe(self.set_stop.stop()).catch_unwind().await;
        for result in [reference, facets] {
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

#[cfg(test)]
mod test_support {
    use super::*;

    pub(super) async fn reopen_document(
        document: automerge::Automerge,
    ) -> (
        tempfile::TempDir,
        SharedBigRepo,
        big_repo::BigRepoStopToken,
        big_cabinet_core::DocId,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let config = big_repo::Config {
            node_identity_seed: [59; 32],
            storage: big_repo::StorageConfig::Disk {
                path: directory.path().join("repo"),
            },
            scope_key: "projection-demand-test".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        };
        let runtime = Arc::new(sqlite_rs::SqliteRuntime::new(Default::default()));
        let (repo, stop) = big_repo::BigRepo::boot(Arc::clone(&runtime), config.clone())
            .await
            .unwrap();
        let handle = repo.create_doc(document).await.unwrap();
        let doc_id = handle.document_id();
        drop(handle);
        drop(repo);
        stop.stop().await.unwrap();
        let (repo, stop) = big_repo::BigRepo::boot(runtime, config).await.unwrap();
        assert!(repo.sync_demand().documents().is_empty());
        (directory, repo, stop, doc_id)
    }
}
