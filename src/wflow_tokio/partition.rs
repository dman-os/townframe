use crate::interlude::*;

use tokio_util::sync::CancellationToken;

use wflow_core::{
    log, metastore,
    partition::{effects, log::PartitionLogEntry, service},
    r#gen::types::PartitionId,
};

use wflow_core::snapstore::SnapStore;

mod effect_worker;
pub mod reducer;
pub mod state;

#[derive(Clone)]
pub struct PartitionCtx {
    pub id: PartitionId,
    pub metadata: Arc<dyn metastore::MetdataStore>,
    pub processed_entries_offset: u64,
    pub log: Arc<dyn wflow_core::log::LogStore>,
    pub local_wasmcloud_host: Arc<
        dyn service::WflowServiceHost<ExtraArgs = metastore::WasmcloudWflowServiceMeta>
            + Sync
            + Send,
    >,
    pub local_native_host: Arc<dyn service::WflowServiceHost<ExtraArgs = ()> + Sync + Send>,
}

impl PartitionCtx {
    pub fn new(
        id: PartitionId,
        metadata: Arc<dyn metastore::MetdataStore>,
        log: Arc<dyn log::LogStore>,
        processed_entries_offset: u64,
        local_wasmcloud_host: Arc<
            dyn service::WflowServiceHost<ExtraArgs = metastore::WasmcloudWflowServiceMeta>
                + Sync
                + Send,
        >,
        local_native_host: Arc<dyn service::WflowServiceHost<ExtraArgs = ()> + Sync + Send>,
    ) -> Self {
        Self {
            id,
            metadata,
            processed_entries_offset,
            log,
            local_wasmcloud_host,
            local_native_host,
        }
    }

    pub fn log_ref(&self) -> PartitionLogRef {
        PartitionLogRef::new(Arc::clone(&self.log))
    }
}

pub struct PartitionLogRef {
    buffer: Vec<u8>,
    log: Arc<dyn wflow_core::log::LogStore>,
}

impl Clone for PartitionLogRef {
    fn clone(&self) -> Self {
        Self {
            buffer: default(),
            log: Arc::clone(&self.log),
        }
    }
}
impl PartitionLogRef {
    pub fn new(log: Arc<dyn wflow_core::log::LogStore>) -> Self {
        Self {
            buffer: vec![],
            log,
        }
    }
    #[tracing::instrument(skip(self))]
    pub async fn append(&mut self, entry: &PartitionLogEntry) -> Res<u64> {
        self.buffer.clear();
        debug!("appending");
        serde_json::to_writer(&mut self.buffer, entry).expect(ERROR_JSON);
        self.log.append(&self.buffer).await
    }

    pub fn tail(
        &'_ self,
        offset: u64,
    ) -> futures::stream::BoxStream<'_, Res<(u64, Option<PartitionLogEntry>)>> {
        use futures::stream::*;
        self.log
            .tail(offset)
            .map(|entry| {
                entry.map(|entry| {
                    (
                        entry.idx,
                        entry
                            .val
                            .map(|bytes| serde_json::from_slice(&bytes).expect(ERROR_JSON)),
                    )
                })
            })
            .boxed()
    }
}

pub struct TokioPartitionWorkerHandle {
    part_reducer: Option<reducer::TokioPartitionReducerHandle>,
    effect_workers: Option<Vec<effect_worker::TokioEffectWorkerHandle>>,
    cancel_token: CancellationToken,
}

impl TokioPartitionWorkerHandle {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        // Close all effect workers first
        for worker in self.effect_workers.take().unwrap() {
            worker.stop().await?;
        }
        // Then close the event worker
        self.part_reducer.take().unwrap().stop().await?;
        // Drop will cancel again, which is safe (idempotent)
        Ok(())
    }
}

impl Drop for TokioPartitionWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

pub async fn start_tokio_worker(
    pcx: PartitionCtx,
    working_state: Arc<state::PartitionWorkingState>,
    snap_store: Arc<dyn SnapStore<Snapshot = Arc<[u8]>>>,
) -> TokioPartitionWorkerHandle {
    let cancel_token = CancellationToken::new();
    let mut effect_workers = vec![];
    // Shared channel for effect scheduling
    let (effect_tx, effect_rx) = async_channel::unbounded::<effects::EffectId>();
    for ii in 0..8 {
        effect_workers.push(effect_worker::start_tokio_effect_worker(
            ii,
            pcx.clone(),
            Arc::clone(&working_state),
            effect_rx.clone(),
            cancel_token.child_token(),
        ));
    }
    let part_reducer = reducer::start_tokio_partition_reducer(
        pcx.clone(),
        Arc::clone(&working_state),
        effect_tx,
        cancel_token.child_token(),
        snap_store,
    );

    TokioPartitionWorkerHandle {
        part_reducer: Some(part_reducer),
        effect_workers: Some(effect_workers),
        cancel_token,
    }
}
