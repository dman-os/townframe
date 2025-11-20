use crate::interlude::*;

use tokio_util::sync::CancellationToken;

use wflow_core::{
    log, metastore,
    partition::{effects, service},
    r#gen::types::PartitionId,
};

use crate::snapstore::SnapStore;

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
    ) -> Self {
        Self {
            id,
            metadata,
            processed_entries_offset,
            log,
            local_wasmcloud_host,
        }
    }

    pub fn log_ref(&self) -> PartitionLogRef {
        PartitionLogRef::new(self.log.clone())
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
            log: self.log.clone(),
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
    pub async fn append(
        &mut self,
        evt: &wflow_core::partition::log::PartitionLogEntry,
    ) -> Res<u64> {
        self.buffer.clear();
        serde_json::to_writer(&mut self.buffer, evt).expect(ERROR_JSON);
        self.log.append(&self.buffer).await
    }
}

pub struct TokioPartitionWorkerHandle {
    part_reducer: Option<reducer::TokioPartitionReducerHandle>,
    effect_workers: Option<Vec<effect_worker::TokioEffectWorkerHandle>>,
    cancel_token: CancellationToken,
}

impl TokioPartitionWorkerHandle {
    pub async fn close(mut self) -> Res<()> {
        self.cancel_token.cancel();
        // Close all effect workers first
        if let Some(effect_workers) = self.effect_workers.take() {
            for worker in effect_workers {
                worker.close().await?;
            }
        }
        // Then close the event worker
        if let Some(reducer) = self.part_reducer.take() {
            reducer.close().await?;
        }
        // Drop will cancel again, which is safe (idempotent)
        Ok(())
    }
}

impl Drop for TokioPartitionWorkerHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        // Cancel all effect workers
        if let Some(ref effect_workers) = self.effect_workers {
            for worker in effect_workers {
                worker.cancel();
            }
        }
        // Cancel the event worker
        if let Some(ref event_worker) = self.part_reducer {
            event_worker.cancel();
        }
    }
}

pub async fn start_tokio_worker(
    pcx: PartitionCtx,
    working_state: Arc<state::PartitionWorkingState>,
    snap_store: Option<Arc<dyn SnapStore>>,
) -> TokioPartitionWorkerHandle {
    let cancel_token = CancellationToken::new();
    let mut effect_workers = vec![];
    // Shared channel for effect scheduling
    let (effect_tx, effect_rx) = async_channel::unbounded::<effects::EffectId>();
    for _ii in 0..8 {
        effect_workers.push(effect_worker::start_tokio_effect_worker(
            pcx.clone(),
            working_state.clone(),
            effect_rx.clone(),
            cancel_token.child_token(),
        ));
    }
    let part_reducer = reducer::start_tokio_partition_reducer(
        pcx.clone(),
        working_state.clone(),
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
