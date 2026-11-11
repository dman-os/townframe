pub mod datum_ref;
pub mod datum_set;

pub use datum_ref::DatumRefEdge;
pub use datum_ref::DatumRefQuery;
pub use datum_ref::DatumRefStore;
pub use datum_set::DatumMembership;
pub use datum_set::DatumRecord;
pub use datum_set::DatumRoute;
pub use datum_set::DatumSetQuery;
pub use datum_set::DatumSetStore;

use crate::schema::SchemaRepo;
use big_repo::SharedBigRepo;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken as ComponentStopToken;
use sqlite_rs::Database;
use std::num::NonZeroUsize;
use std::sync::Arc;
use utils_rs::prelude::Res;

/// The change-stream-fed indexes of one cabinet.
///
/// `datum_set` is the only worker: it projects each document once and, from that read, writes both
/// the predicate entries and the declared-reference edges (ADR 024). `datum_ref` is left here as the
/// read surface over the edge table those projections fill.
pub struct Indexes {
    pub datum_set: Arc<DatumSetStore>,
    pub datum_ref: Arc<DatumRefStore>,
    set_tasks: Spawner,
}

impl Indexes {
    pub async fn boot(
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        sql: Database,
        concurrency: NonZeroUsize,
    ) -> Res<(Arc<Self>, StopToken)> {
        let datum_set = Arc::new(DatumSetStore::new(sql.clone(), schemas).await?);
        let datum_ref = Arc::new(DatumRefStore::new(sql).await?);
        let (set_stop, set_tasks) = ComponentStopToken::new();
        let (ready, started) = tokio::sync::oneshot::channel();
        let store = Arc::clone(&datum_set);
        let owner = set_tasks.clone();
        set_tasks
            .spawn(async move {
                let mut ready = Some(ready);
                let result = store
                    .run_until(repo, concurrency, owner.cancelled(), &mut ready)
                    .await;
                if let Some(ready) = ready {
                    ready.send(result).expect("index boot waits for readiness");
                } else {
                    result.unwrap();
                }
            })
            .expect("index worker starts before shutdown");
        let stop = StopToken { set_stop };
        if let Err(error) = started
            .await
            .expect("index initialization reports its result")
        {
            stop.stop().await;
            return Err(error);
        }
        Ok((
            Arc::new(Self {
                datum_set,
                datum_ref,
                set_tasks,
            }),
            stop,
        ))
    }

    pub fn live_tasks(&self) -> usize {
        self.set_tasks.live_tasks()
    }
}

#[must_use]
pub struct StopToken {
    set_stop: ComponentStopToken,
}

impl StopToken {
    pub async fn stop(self) {
        self.set_stop.stop().await;
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
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        // Each boot opens the directory afresh, as a restarted process would.
        let config = || big_repo::Config {
            node_identity_seed: [59; 32],
            storage: big_repo::Storage::in_directory(
                &runtime,
                &directory.path().join("repo"),
                None,
            )
            .unwrap(),
            scope_key: "projection-demand-test".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        };
        let (repo, stop) = big_repo::BigRepo::boot(config()).await.unwrap();
        let handle = repo.create_doc(document).await.unwrap();
        let doc_id = handle.document_id();
        drop(handle);
        drop(repo);
        stop.stop().await.unwrap();
        let (repo, stop) = big_repo::BigRepo::boot(config()).await.unwrap();
        assert!(repo.sync_demand().documents().is_empty());
        (directory, repo, stop, doc_id)
    }
}
