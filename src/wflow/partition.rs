use crate::interlude::*;

use futures::StreamExt;

use crate::log::LogStore;
use crate::plugin::bindings_partition_host::townframe::wflow::partition_host;

mod core;

#[derive(Clone)]
struct PartitionCtx {
    cx: crate::SharedCtx,
    id: partition_host::PartitionId,
    log: Arc<dyn crate::log::LogStore>,
}

struct PartitionHandle {
    evt_tx: tokio::sync::mpsc::Sender<PartitionEvent>,
    join_handle: tokio::task::JoinHandle<Res<()>>,
}

#[derive(Serialize)]
struct PartitionEffectLogEntry<'a> {
    source_event_entry_id: u64,
    effects: &'a Vec<PartitionEffect>,
}

async fn start_tokio_worker(pcx: PartitionCtx) -> PartitionHandle {
    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::channel(16);
    let (effect_tx, mut effect_rx) = tokio::sync::mpsc::channel(16);
    let reducer_fut = {
        let pcx = pcx.clone();
        async move {
            let mut effects = vec![];
            let mut json_buffer = vec![];
            // TODO: recover last offest
            let stream = pcx.log.tail(0).await;
            while let Some(Ok((entry_id, entry))) = stream.next().await {
                let evt: core::JobEvent = serde_json::from_slice(&entry).expect(ERROR_JSON);

                effects.clear();
                core::reduce(evt, &mut effects);

                for effect in effects {
                    match effect {
                        PartitionEffect::LookupWflowForInvocation(new_invocation_event) => todo!(),
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { reducer_fut.await });

    PartitionHandle {
        evt_tx,
        join_handle,
    }
}

struct PartitionLogRef {
    buffer: Vec<u8>,
    log: Arc<dyn crate::log::LogStore>,
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
    fn new(cx: &PartitionCtx) -> Self {
        Self {
            buffer: vec![],
            log: cx.log.clone(),
        }
    }
    async fn append(&mut self, evt: &PartitionEvent) -> Res<u64> {
        self.buffer.clear();
        serde_json::to_writer(&mut self.buffer, evt).expect(ERROR_JSON);
        self.log.append(&self.buffer).await
    }
}
