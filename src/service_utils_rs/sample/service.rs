//! The sample implementation: a source revision, an owned projector that
//! interprets it, and the four handlers that read, write, and observe it.

use super::protocol::{
    GetValue, GetValueOutput, PutValue, PutValueOutput, ReadState, SampleError, SampleEvent,
    StatePage, SubscribeEvents, ValueSnapshot,
};
use crate::{
    ContextError, Envelope, EventStream, Handler, Host, Layer, Operation, ServiceBuilder, Spawner,
    StopToken, Trace, Traced,
};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{broadcast, watch};

/// Events a slow subscriber may fall behind by before it is told to reread.
const EVENT_REPLAY_WINDOW: usize = 64;

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_TASK_MARKER: u64;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceSnapshot {
    value: String,
    revision: u64,
}

/// The implementation is separate from source persistence and from its
/// clients. A projector task observes both local and externally injected
/// source changes; handlers only ever read the interpreted state.
pub struct SampleService {
    readable: RwLock<ValueSnapshot>,
    source: watch::Sender<SourceSnapshot>,
    projection_watermark: watch::Sender<u64>,
    events: broadcast::Sender<SampleEvent>,
    spawner: Spawner,
    #[cfg(test)]
    last_handler_task: Mutex<Option<u64>>,
    #[cfg(test)]
    projector_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
}

impl SampleService {
    /// Starts the service and its projector. Stopping the token joins the
    /// projector; a write still waiting on it then fails as cancelled.
    pub fn boot(initial_value: impl Into<String>) -> (Arc<Self>, StopToken) {
        let (stop, spawner) = StopToken::new();
        let value = initial_value.into();
        let (source, source_rx) = watch::channel(SourceSnapshot {
            value: value.clone(),
            revision: 0,
        });
        let (projection_watermark, _) = watch::channel(0);
        let (events, _) = broadcast::channel(EVENT_REPLAY_WINDOW);
        let service = Arc::new(Self {
            readable: RwLock::new(ValueSnapshot {
                value,
                source_revision: 0,
                service_revision: 0,
            }),
            source,
            projection_watermark,
            events,
            spawner: spawner.clone(),
            #[cfg(test)]
            last_handler_task: Mutex::new(None),
            #[cfg(test)]
            projector_gate: Mutex::new(None),
        });
        spawner.spawn(Arc::clone(&service).projector(source_rx));
        (service, stop)
    }

    async fn projector(self: Arc<Self>, mut source_rx: watch::Receiver<SourceSnapshot>) {
        loop {
            tokio::select! {
                changed = source_rx.changed() => {
                    changed.expect("the source sender lives as long as the service");
                }
                _ = self.spawner.cancelled() => return,
            }
            #[cfg(test)]
            {
                let gate = self.projector_gate.lock().expect("gate mutex").clone();
                if let Some(gate) = gate {
                    tokio::select! {
                        permit = gate.acquire() => drop(permit.expect("gate lives")),
                        _ = self.spawner.cancelled() => return,
                    }
                }
            }
            let source = source_rx.borrow_and_update().clone();
            let event = {
                let mut readable = self.readable.write().expect("readable state lock");
                if source.revision <= readable.source_revision {
                    continue;
                }
                *readable = ValueSnapshot {
                    value: source.value,
                    source_revision: source.revision,
                    service_revision: readable.service_revision + 1,
                };
                SampleEvent {
                    source_revision: readable.source_revision,
                    service_revision: readable.service_revision,
                    value: readable.value.clone(),
                }
            };
            // State and the interpreted watermark precede broadcast: a writer
            // released by the watermark reads the state the event describes.
            // Broadcast with no subscribers is not an error.
            self.projection_watermark
                .send_replace(event.source_revision);
            drop(self.events.send(event));
        }
    }

    /// One source boundary allocates revisions for every writer, local or
    /// external. The projector never allocates revisions, so coalescing
    /// cannot duplicate or reorder source identities.
    fn write_source(&self, value: String) -> u64 {
        let mut revision = 0;
        self.source.send_modify(|source| {
            source.revision += 1;
            source.value = value;
            revision = source.revision;
        });
        revision
    }

    async fn wait_projected(&self, source_revision: u64) -> Result<(), SampleError> {
        let mut watermark = self.projection_watermark.subscribe();
        loop {
            if *watermark.borrow_and_update() >= source_revision {
                return Ok(());
            }
            tokio::select! {
                changed = watermark.changed() => {
                    changed.expect("the watermark sender lives as long as the service");
                }
                _ = self.spawner.cancelled() => return Err(ContextError::Cancelled.into()),
            }
        }
    }

    fn snapshot(&self) -> ValueSnapshot {
        self.readable.read().expect("readable state lock").clone()
    }

    /// External synchronization uses exactly the same source/projector path as
    /// a local procedure; it does not publish directly.
    pub fn inject_source(&self, value: impl Into<String>) -> u64 {
        self.write_source(value.into())
    }

    /// Raw interpreted-change feed, for in-process observers that do not go
    /// through a request.
    pub fn subscribe_events(&self) -> broadcast::Receiver<SampleEvent> {
        self.events.subscribe()
    }

    #[cfg(test)]
    fn record_handler_task(&self) {
        *self.last_handler_task.lock().expect("task mutex") =
            TEST_TASK_MARKER.try_with(|marker| *marker).ok();
    }

    #[cfg(test)]
    pub(crate) fn last_handler_task(&self) -> Option<u64> {
        *self.last_handler_task.lock().expect("task mutex")
    }

    /// Holds the projector before each interpretation until the returned
    /// semaphore grants a permit, so tests can pile up source changes.
    #[cfg(test)]
    pub(crate) fn block_projector(&self) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *self.projector_gate.lock().expect("gate mutex") = Some(Arc::clone(&gate));
        gate
    }
}

impl Handler<GetValue> for SampleService {
    async fn call(&self, request: Envelope<GetValue>) -> Result<GetValueOutput, SampleError> {
        #[cfg(test)]
        self.record_handler_task();
        Ok(GetValueOutput {
            snapshot: self.snapshot(),
            observed_context: request.context,
            observed_metadata: request.metadata,
        })
    }
}

impl Handler<PutValue> for SampleService {
    async fn call(&self, request: Envelope<PutValue>) -> Result<PutValueOutput, SampleError> {
        #[cfg(test)]
        self.record_handler_task();
        if request.params.value.is_empty() {
            return Err(SampleError::InvalidValue {
                message: "value cannot be empty".into(),
            });
        }
        // This writes only the source. The independent projector observes it;
        // waiting here is a completion contract, not a direct projector call.
        let revision = self.write_source(request.params.value);
        self.wait_projected(revision).await?;
        Ok(PutValueOutput {
            snapshot: self.snapshot(),
        })
    }
}

impl Handler<ReadState> for SampleService {
    async fn call(&self, request: Envelope<ReadState>) -> Result<StatePage, SampleError> {
        #[cfg(test)]
        self.record_handler_task();
        let snapshot = self.snapshot();
        Ok(StatePage {
            reset_required: request.params.after_source_revision > snapshot.source_revision,
            snapshot,
        })
    }
}

impl Handler<SubscribeEvents> for SampleService {
    async fn call(
        &self,
        request: Envelope<SubscribeEvents>,
    ) -> Result<EventStream<SampleEvent, SampleError>, SampleError> {
        #[cfg(test)]
        self.record_handler_task();
        // Subscribe before reading so no interpretation lands between the
        // snapshot and the first live event.
        let receiver = self.events.subscribe();
        let current = self.snapshot();
        let after = request.params.after_source_revision;
        let pending = (current.source_revision > after).then_some(SampleEvent {
            source_revision: current.source_revision,
            service_revision: current.service_revision,
            value: current.value,
        });
        struct Cursor {
            pending: Option<SampleEvent>,
            receiver: broadcast::Receiver<SampleEvent>,
            last_source_revision: u64,
        }
        let cursor = Cursor {
            pending,
            receiver,
            last_source_revision: after,
        };
        let stream = futures::stream::unfold(cursor, |mut cursor| async move {
            loop {
                let event = match cursor.pending.take() {
                    Some(event) => event,
                    None => match cursor.receiver.recv().await {
                        Ok(event) => event,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            return Some((
                                Err(SampleError::SubscriptionLagged { skipped }),
                                cursor,
                            ));
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    },
                };
                if event.source_revision <= cursor.last_source_revision {
                    continue;
                }
                cursor.last_source_revision = event.source_revision;
                return Some((Ok(event), cursor));
            }
        });
        Ok(Box::pin(stream))
    }
}

/// A shared record of handler entry and exit, written by the [`Log`] layer.
#[derive(Debug, Clone, Default)]
pub struct CallLog(Arc<Mutex<Vec<String>>>);

impl CallLog {
    pub fn entries(&self) -> Vec<String> {
        self.0.lock().expect("call log mutex").clone()
    }

    fn push(&self, entry: String) {
        self.0.lock().expect("call log mutex").push(entry);
    }
}

/// The smallest useful [`Layer`]: it demonstrates that one generic layer
/// applies to every operation without naming any of them, and that a layer
/// sees the whole envelope, not just the params.
#[derive(Debug, Clone)]
pub struct Log {
    log: CallLog,
}

impl Log {
    pub fn new(log: CallLog) -> Self {
        Self { log }
    }
}

impl<H> Layer<H> for Log {
    type Handler = Logged<H>;
    fn layer(&self, inner: H) -> Logged<H> {
        Logged {
            inner,
            log: self.log.clone(),
        }
    }
}

pub struct Logged<H> {
    inner: H,
    log: CallLog,
}

impl<P: Operation, H: Handler<P>> Handler<P> for Logged<H> {
    async fn call(&self, request: Envelope<P>) -> Result<P::Output, P::Error> {
        let nsid = P::XRPC.nsid;
        self.log.push(format!("{nsid}:start"));
        let result = self.inner.call(request).await;
        self.log.push(format!("{nsid}:complete"));
        result
    }
}

/// The layered handler [`boot`] hosts. Naming it lets consumers write
/// `Arc<Host<SampleHandler>>` and `Client::<SampleProtocol>::direct(host)`.
pub type SampleHandler = Logged<Traced<Arc<SampleService>>>;

/// A booted sample: the implementation, the host that fronts it, and the log
/// its layer writes.
pub struct Sample {
    pub service: Arc<SampleService>,
    pub host: Arc<Host<SampleHandler>>,
    pub calls: CallLog,
}

/// Boots the service and hosts it behind `Trace` and `Log`. The returned
/// token stops the host (joining in-flight calls and direct subscription
/// pumps) before the service (joining the projector).
pub fn boot(initial_value: impl Into<String>) -> (Sample, StopToken) {
    let (service, service_stop) = SampleService::boot(initial_value);
    let calls = CallLog::default();
    let handler = ServiceBuilder::new()
        .layer(Trace::new("sample"))
        .layer(Log::new(calls.clone()))
        .service(Arc::clone(&service));
    let (host, host_stop) = Host::new(handler);
    let sample = Sample {
        service,
        host,
        calls,
    };
    (sample, host_stop.then(service_stop))
}
