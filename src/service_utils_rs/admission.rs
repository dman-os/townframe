use crate::context::ContextError;
pub use service_core_rs::cancellation::CancellationKey;
pub use service_core_rs::cancellation::CancellationReceiver;
pub use service_core_rs::cancellation::CancellationRegistration;
pub use service_core_rs::cancellation::CancellationRequestGuard;
pub use service_core_rs::cancellation::CancellationSignal;
pub use service_core_rs::cancellation::CancellationTransport;
pub use service_core_rs::cancellation::cancellation_channel;
use std::sync::Arc;

#[derive(Default)]
pub struct TaskActivity {
    admitted: std::sync::atomic::AtomicUsize,
    peak_admitted: std::sync::atomic::AtomicUsize,
    spawned: std::sync::atomic::AtomicUsize,
    peak_spawned: std::sync::atomic::AtomicUsize,
    active: std::sync::atomic::AtomicUsize,
    peak_active: std::sync::atomic::AtomicUsize,
    stream_pumps: std::sync::atomic::AtomicUsize,
    peak_stream_pumps: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

pub struct TaskGuard {
    activity: Arc<TaskActivity>,
    counter: TaskCounter,
}
#[derive(Clone, Copy)]
enum TaskCounter {
    Admitted,
    Spawned,
    Active,
    StreamPumps,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let counter = match self.counter {
            TaskCounter::Admitted => &self.activity.admitted,
            TaskCounter::Spawned => &self.activity.spawned,
            TaskCounter::Active => &self.activity.active,
            TaskCounter::StreamPumps => &self.activity.stream_pumps,
        };
        counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.activity.changed.notify_one();
    }
}

impl TaskActivity {
    fn guard(activity: Arc<Self>, kind: TaskCounter) -> TaskGuard {
        let (counter, peak) = match kind {
            TaskCounter::Admitted => (&activity.admitted, &activity.peak_admitted),
            TaskCounter::Spawned => (&activity.spawned, &activity.peak_spawned),
            TaskCounter::Active => (&activity.active, &activity.peak_active),
            TaskCounter::StreamPumps => (&activity.stream_pumps, &activity.peak_stream_pumps),
        };
        let count = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        peak.fetch_max(count, std::sync::atomic::Ordering::SeqCst);
        activity.changed.notify_one();
        TaskGuard {
            activity,
            counter: kind,
        }
    }
    pub fn admitted(self: &Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::Admitted)
    }
    pub fn spawned(self: &Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::Spawned)
    }
    pub fn active(self: &Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::Active)
    }
    pub fn stream_pump(self: &Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::StreamPumps)
    }
    pub fn admitted_count(&self) -> usize {
        self.admitted.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn spawned_count(&self) -> usize {
        self.spawned.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn active_count(&self) -> usize {
        self.active.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn peak_count(&self) -> usize {
        self.peak_active.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn stream_pump_count(&self) -> usize {
        self.stream_pumps.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn peak_stream_pump_count(&self) -> usize {
        self.peak_stream_pumps
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub async fn wait_for_change(&self) {
        self.changed.notified().await;
    }
}

pub struct Admission {
    unary_permits: Arc<tokio::sync::Semaphore>,
    stream_permits: Arc<tokio::sync::Semaphore>,
    activity: Arc<TaskActivity>,
    cancellations: CancellationReceiver,
    max_handlers: usize,
    max_stream_pumps: usize,
}

impl Admission {
    pub fn new(max_handlers: usize, max_stream_pumps: usize) -> Arc<Self> {
        let scope_capacity = max_handlers.saturating_add(max_stream_pumps).max(1);
        Self::with_cancellation(
            max_handlers,
            max_stream_pumps,
            cancellation_channel(scope_capacity).1,
        )
    }

    pub fn with_cancellation(
        max_handlers: usize,
        max_stream_pumps: usize,
        receiver: CancellationReceiver,
    ) -> Arc<Self> {
        assert!(max_handlers > 0 && max_stream_pumps > 0);
        Arc::new(Self {
            unary_permits: Arc::new(tokio::sync::Semaphore::new(max_handlers)),
            stream_permits: Arc::new(tokio::sync::Semaphore::new(max_stream_pumps)),
            activity: Arc::new(TaskActivity::default()),
            cancellations: receiver,
            max_handlers,
            max_stream_pumps,
        })
    }

    pub fn register_cancellation(
        self: &Arc<Self>,
        signal: CancellationSignal,
    ) -> Result<CancellationRegistration, ContextError> {
        self.cancellations.register_cancellation(signal)
    }

    pub async fn acquire_unary(&self) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(&self.unary_permits)
            .acquire_owned()
            .await
            .expect("owned IRPC unary semaphore lives")
    }
    pub fn try_acquire_stream(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.stream_permits).try_acquire_owned().ok()
    }
    pub fn activity(&self) -> Arc<TaskActivity> {
        Arc::clone(&self.activity)
    }
    pub fn max_handlers(&self) -> usize {
        self.max_handlers
    }
    pub fn max_stream_pumps(&self) -> usize {
        self.max_stream_pumps
    }
}
