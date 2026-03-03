//! FIXME: fuck me, we have a name clash with the core::repo module

use crate::interlude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct RepoStopToken {
    pub cancel_token: CancellationToken,
    pub worker_handle: Option<JoinHandle<()>>,
    pub broker_stop_tokens: Vec<Arc<am_utils_rs::changes::DocChangeBrokerStopToken>>,
}

impl RepoStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle {
            handle.await?;
        }
        for token in self.broker_stop_tokens {
            if let Ok(token) = Arc::try_unwrap(token) {
                token.stop().await?;
            }
        }
        Ok(())
    }
}

pub trait Repo {
    type Event: Send + Sync + 'static;

    fn registry(&self) -> &Arc<ListenersRegistry>;
    fn cancel_token(&self) -> &CancellationToken;

    fn subscribe(&self, opts: SubscribeOpts) -> ListenerHandle<Self::Event> {
        self.registry().subscribe::<Self::Event>(opts)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SubscribeOpts {
    pub capacity: usize,
}

impl SubscribeOpts {
    pub fn new(capacity: usize) -> Self {
        Self { capacity }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    Closed,
    Dropped { dropped_count: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Closed,
    Dropped { dropped_count: u64 },
}

struct QueueState<E> {
    queue: std::sync::Mutex<VecDeque<Arc<E>>>,
    condvar: std::sync::Condvar,
    notify: tokio::sync::Notify,
    capacity: usize,
    closed: AtomicBool,
    dropped_count: AtomicU64,
}

impl<E> QueueState<E> {
    fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 0, "subscribe capacity must be > 0");
        Arc::new(Self {
            queue: std::sync::Mutex::new(VecDeque::with_capacity(capacity)),
            condvar: std::sync::Condvar::new(),
            notify: tokio::sync::Notify::new(),
            capacity,
            closed: AtomicBool::new(false),
            dropped_count: AtomicU64::new(0),
        })
    }

    fn push(&self, event: Arc<E>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }

        let mut lock = self.queue.lock().expect(ERROR_MUTEX);
        if lock.len() >= self.capacity {
            lock.pop_front();
            let prev = self.dropped_count.fetch_add(1, Ordering::AcqRel);
            if prev == 0 {
                warn!("listener queue dropped events due to full capacity");
            }
        }
        lock.push_back(event);
        drop(lock);

        self.condvar.notify_one();
        self.notify.notify_waiters();
    }

    fn close(&self) {
        let was_closed = self.closed.swap(true, Ordering::AcqRel);
        if !was_closed {
            self.condvar.notify_all();
            self.notify.notify_waiters();
        }
    }

    fn pop_now(&self) -> Option<Arc<E>> {
        self.queue.lock().expect(ERROR_MUTEX).pop_front()
    }
}

type ErasedEvent = Arc<dyn std::any::Any + Send + Sync + 'static>;

#[derive(Clone)]
struct ErasedListener {
    on_event_cb: Arc<dyn Fn(ErasedEvent) + Send + Sync>,
}

impl ErasedListener {
    fn new<Ev>(on_event_cb: impl Fn(Arc<Ev>) + Send + Sync + 'static) -> Self
    where
        Ev: Send + Sync + 'static,
    {
        Self {
            on_event_cb: Arc::new(move |ev| {
                let ev: Arc<Ev> = Arc::downcast(ev)
                    .expect("error downcasting event, wrong event sent to listener");
                on_event_cb(ev)
            }),
        }
    }

    fn on_event(&self, ev: ErasedEvent) {
        (self.on_event_cb)(ev)
    }
}

pub struct ListenersRegistry {
    list: std::sync::Mutex<Vec<(Uuid, ErasedListener)>>,
}

impl ListenersRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { list: default() })
    }

    pub fn subscribe<E>(self: &Arc<Self>, opts: SubscribeOpts) -> ListenerHandle<E>
    where
        E: Send + Sync + 'static,
    {
        let state = QueueState::<E>::new(opts.capacity);
        let state_for_cb = Arc::clone(&state);

        let registration = Arc::new(ListenerRegistration::new(
            Arc::downgrade(self),
            Some({
                let state = Arc::clone(&state);
                Arc::new(move || state.close())
            }),
        ));

        {
            let mut lock = self.list.lock().expect(ERROR_MUTEX);
            lock.push((
                registration.id,
                ErasedListener::new(move |event: Arc<E>| {
                    state_for_cb.push(event);
                }),
            ));
        }

        ListenerHandle::new(state, registration)
    }

    pub fn notify<E, I>(&self, events: I)
    where
        E: std::any::Any + Send + Sync + 'static,
        I: IntoIterator<Item = E>,
    {
        let listeners = {
            let lock = self.list.lock().expect(ERROR_MUTEX);
            lock.iter()
                .map(|(_id, listener)| listener.clone())
                .collect::<Vec<_>>()
        };

        for event in events {
            let event: ErasedEvent = Arc::new(event);
            for listener in listeners.iter() {
                listener.on_event(Arc::clone(&event));
            }
        }
    }

    pub fn dump(&self) {
        let list = self.list.lock().expect(ERROR_MUTEX);
        info!("ListenersRegistry: ================================================");
        for (id, _listener) in list.iter() {
            info!("- {id}");
        }
        info!("================================================");
    }
}

#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct ListenerRegistration {
    pub registry: std::sync::Weak<ListenersRegistry>,
    pub id: Uuid,
    pub on_unregister: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    on_unregister_extra: std::sync::Mutex<Vec<Arc<dyn Fn() + Send + Sync + 'static>>>,
    is_unregistered: AtomicBool,
}

impl ListenerRegistration {
    fn new(
        registry: std::sync::Weak<ListenersRegistry>,
        on_unregister: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            registry,
            id: Uuid::new_v4(),
            on_unregister,
            on_unregister_extra: std::sync::Mutex::new(Vec::new()),
            is_unregistered: AtomicBool::new(false),
        }
    }

    pub fn add_on_unregister(&self, cb: Arc<dyn Fn() + Send + Sync + 'static>) {
        let mut callbacks = self.on_unregister_extra.lock().expect(ERROR_MUTEX);
        if self.is_unregistered.load(Ordering::Acquire) {
            drop(callbacks);
            cb();
            return;
        }
        callbacks.push(cb);
    }

    fn unregister_impl(&self) {
        let was = self.is_unregistered.swap(true, Ordering::AcqRel);
        if was {
            return;
        }

        if let Some(registry) = self.registry.upgrade() {
            let mut lock = registry.list.lock().expect(ERROR_MUTEX);
            lock.retain(|(lid, _)| *lid != self.id);
        }

        if let Some(on_unregister) = &self.on_unregister {
            on_unregister();
        }

        let extra_callbacks = self.on_unregister_extra.lock().expect(ERROR_MUTEX).clone();
        for callback in extra_callbacks {
            callback();
        }
    }
}

#[cfg_attr(feature = "uniffi", uniffi::export)]
impl ListenerRegistration {
    fn unregister(&self) {
        self.unregister_impl();
    }
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        self.unregister_impl();
    }
}

pub struct ListenerHandle<E>
where
    E: Send + Sync + 'static,
{
    state: Arc<QueueState<E>>,
    registration: Arc<ListenerRegistration>,
    seen_dropped_count: AtomicU64,
}

impl<E> ListenerHandle<E>
where
    E: Send + Sync + 'static,
{
    fn new(state: Arc<QueueState<E>>, registration: Arc<ListenerRegistration>) -> Self {
        Self {
            state,
            registration,
            seen_dropped_count: AtomicU64::new(0),
        }
    }

    pub fn registration(&self) -> Arc<ListenerRegistration> {
        Arc::clone(&self.registration)
    }

    fn take_drop_error(&self) -> Option<u64> {
        let now = self.state.dropped_count.load(Ordering::Acquire);
        let seen = self.seen_dropped_count.load(Ordering::Acquire);
        if now > seen {
            self.seen_dropped_count.store(now, Ordering::Release);
            Some(now - seen)
        } else {
            None
        }
    }

    pub fn has_dropped(&self) -> bool {
        self.state.dropped_count.load(Ordering::Acquire) > 0
    }

    pub fn dropped_count(&self) -> u64 {
        self.state.dropped_count.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.registration.unregister_impl();
    }

    pub fn try_recv(&self) -> Result<Arc<E>, TryRecvError> {
        if let Some(dropped_count) = self.take_drop_error() {
            return Err(TryRecvError::Dropped { dropped_count });
        }

        if let Some(ev) = self.state.pop_now() {
            return Ok(ev);
        }

        if self.state.closed.load(Ordering::Acquire) {
            return Err(TryRecvError::Closed);
        }

        Err(TryRecvError::Empty)
    }

    pub fn try_recv_lossy(&self) -> Result<Arc<E>, TryRecvError> {
        if let Some(ev) = self.state.pop_now() {
            return Ok(ev);
        }

        if self.state.closed.load(Ordering::Acquire) {
            return Err(TryRecvError::Closed);
        }

        Err(TryRecvError::Empty)
    }

    pub fn recv_blocking(&self) -> Result<Arc<E>, RecvError> {
        loop {
            if let Some(dropped_count) = self.take_drop_error() {
                return Err(RecvError::Dropped { dropped_count });
            }

            if let Some(ev) = self.state.pop_now() {
                return Ok(ev);
            }

            if self.state.closed.load(Ordering::Acquire) {
                return Err(RecvError::Closed);
            }

            let mut lock = self.state.queue.lock().expect(ERROR_MUTEX);
            while lock.is_empty() && !self.state.closed.load(Ordering::Acquire) {
                lock = self.state.condvar.wait(lock).expect(ERROR_MUTEX);
            }
            drop(lock);
        }
    }

    pub fn recv_lossy_blocking(&self) -> Result<Arc<E>, RecvError> {
        loop {
            if let Some(ev) = self.state.pop_now() {
                return Ok(ev);
            }

            if self.state.closed.load(Ordering::Acquire) {
                return Err(RecvError::Closed);
            }

            let mut lock = self.state.queue.lock().expect(ERROR_MUTEX);
            while lock.is_empty() && !self.state.closed.load(Ordering::Acquire) {
                lock = self.state.condvar.wait(lock).expect(ERROR_MUTEX);
            }
            drop(lock);
        }
    }

    pub async fn recv_async(&self) -> Result<Arc<E>, RecvError> {
        loop {
            let notified = self.state.notify.notified();

            if let Some(dropped_count) = self.take_drop_error() {
                return Err(RecvError::Dropped { dropped_count });
            }

            if let Some(ev) = self.state.pop_now() {
                return Ok(ev);
            }

            if self.state.closed.load(Ordering::Acquire) {
                return Err(RecvError::Closed);
            }

            notified.await;
        }
    }

    pub async fn recv_lossy_async(&self) -> Result<Arc<E>, RecvError> {
        loop {
            let notified = self.state.notify.notified();

            if let Some(ev) = self.state.pop_now() {
                return Ok(ev);
            }

            if self.state.closed.load(Ordering::Acquire) {
                return Err(RecvError::Closed);
            }

            notified.await;
        }
    }

    pub fn into_iter_result(self) -> ResultIter<E> {
        ResultIter { handle: self }
    }

    pub fn into_iter_lossy(self) -> LossyIter<E> {
        LossyIter { handle: self }
    }

    pub fn into_stream_result(
        self,
    ) -> impl futures::stream::Stream<Item = Result<Arc<E>, RecvError>> + Send {
        futures::stream::unfold(self, |handle| async move {
            match handle.recv_async().await {
                Ok(ev) => Some((Ok(ev), handle)),
                Err(RecvError::Dropped { dropped_count }) => {
                    Some((Err(RecvError::Dropped { dropped_count }), handle))
                }
                Err(RecvError::Closed) => None,
            }
        })
    }

    pub fn into_stream_lossy(self) -> impl futures::stream::Stream<Item = Arc<E>> + Send {
        futures::stream::unfold(self, |handle| async move {
            match handle.recv_lossy_async().await {
                Ok(ev) => Some((ev, handle)),
                Err(RecvError::Closed) => None,
                Err(RecvError::Dropped { .. }) => unreachable!("lossy stream never reports drops"),
            }
        })
    }
}

impl<E> Drop for ListenerHandle<E>
where
    E: Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.registration.unregister_impl();
    }
}

pub struct ResultIter<E>
where
    E: Send + Sync + 'static,
{
    handle: ListenerHandle<E>,
}

impl<E> Iterator for ResultIter<E>
where
    E: Send + Sync + 'static,
{
    type Item = Result<Arc<E>, RecvError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.handle.recv_blocking() {
            Ok(ev) => Some(Ok(ev)),
            Err(RecvError::Dropped { dropped_count }) => {
                Some(Err(RecvError::Dropped { dropped_count }))
            }
            Err(RecvError::Closed) => None,
        }
    }
}

pub struct LossyIter<E>
where
    E: Send + Sync + 'static,
{
    handle: ListenerHandle<E>,
}

impl<E> Iterator for LossyIter<E>
where
    E: Send + Sync + 'static,
{
    type Item = Arc<E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.handle.recv_lossy_blocking().ok()
    }
}

pub struct ErasedListenerAdapter<E>
where
    E: Send + Sync + 'static,
{
    on_event: Arc<dyn Fn(Arc<E>) + Send + Sync>,
}

impl<E> ErasedListenerAdapter<E>
where
    E: Send + Sync + 'static,
{
    pub fn new(on_event: impl Fn(Arc<E>) + Send + Sync + 'static) -> Self {
        Self {
            on_event: Arc::new(on_event),
        }
    }

    pub fn on_event(&self, event: Arc<E>) {
        (self.on_event)(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Weak;

    fn mk_handle(capacity: usize) -> ListenerHandle<u64> {
        let state = QueueState::<u64>::new(capacity);
        let reg = Arc::new(ListenerRegistration {
            registry: Weak::new(),
            id: Uuid::new_v4(),
            on_unregister: Some({
                let state = Arc::clone(&state);
                Arc::new(move || state.close())
            }),
            on_unregister_extra: std::sync::Mutex::new(Vec::new()),
            is_unregistered: AtomicBool::new(false),
        });
        ListenerHandle::new(state, reg)
    }

    #[test]
    fn bounded_queue_drops_oldest_and_surfaces_drop_error() {
        let handle = mk_handle(2);
        handle.state.push(Arc::new(1));
        handle.state.push(Arc::new(2));
        handle.state.push(Arc::new(3));

        assert_eq!(handle.dropped_count(), 1);
        assert!(handle.has_dropped());

        assert_eq!(
            handle.try_recv(),
            Err(TryRecvError::Dropped { dropped_count: 1 })
        );
        assert_eq!(*handle.try_recv().expect("value expected"), 2);
        assert_eq!(*handle.try_recv().expect("value expected"), 3);
        assert_eq!(handle.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn close_returns_closed() {
        let handle = mk_handle(2);
        handle.close();

        assert_eq!(handle.try_recv(), Err(TryRecvError::Closed));
        assert_eq!(handle.try_recv_lossy(), Err(TryRecvError::Closed));
        assert_eq!(handle.recv_blocking(), Err(RecvError::Closed));
        assert_eq!(handle.recv_lossy_blocking(), Err(RecvError::Closed));
    }

    #[test]
    fn lossy_try_recv_skips_drop_error() {
        let handle = mk_handle(1);
        handle.state.push(Arc::new(10));
        handle.state.push(Arc::new(20));

        assert_eq!(handle.dropped_count(), 1);
        assert_eq!(*handle.try_recv_lossy().expect("value expected"), 20);
        assert_eq!(handle.try_recv_lossy(), Err(TryRecvError::Empty));
    }
}

#[cfg(test)]
mod loom_tests {
    use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;
    use std::collections::VecDeque;

    struct LoomQueue {
        queue: Mutex<VecDeque<u64>>,
        capacity: usize,
        closed: AtomicBool,
        dropped_count: AtomicU64,
    }

    impl LoomQueue {
        fn new(capacity: usize) -> Arc<Self> {
            Arc::new(Self {
                queue: Mutex::new(VecDeque::with_capacity(capacity)),
                capacity,
                closed: AtomicBool::new(false),
                dropped_count: AtomicU64::new(0),
            })
        }

        fn push(&self, value: u64) {
            if self.closed.load(Ordering::Acquire) {
                return;
            }

            let mut lock = self.queue.lock().expect("loom mutex poisoned");
            if lock.len() >= self.capacity {
                lock.pop_front();
                self.dropped_count.fetch_add(1, Ordering::AcqRel);
            }
            lock.push_back(value);
        }

        fn pop_now(&self) -> Option<u64> {
            self.queue.lock().expect("loom mutex poisoned").pop_front()
        }

        fn close(&self) {
            self.closed.store(true, Ordering::Release);
        }
    }

    #[test]
    fn loom_drop_oldest_on_overflow() {
        loom::model(|| {
            let q = LoomQueue::new(1);
            q.push(1);
            q.push(2);
            assert_eq!(q.pop_now(), Some(2));
            assert_eq!(q.pop_now(), None);
            assert_eq!(q.dropped_count.load(Ordering::Acquire), 1);
        });
    }

    #[test]
    fn loom_close_blocks_future_pushes() {
        loom::model(|| {
            let q = LoomQueue::new(4);
            let q1 = Arc::clone(&q);
            let q2 = Arc::clone(&q);

            let t1 = thread::spawn(move || {
                q1.push(1);
                q1.close();
            });
            let t2 = thread::spawn(move || {
                q2.push(2);
            });

            t1.join().expect("join failed");
            t2.join().expect("join failed");

            let mut seen = vec![];
            while let Some(v) = q.pop_now() {
                seen.push(v);
            }

            assert!(seen.len() <= 2);
            assert!(seen.iter().all(|v| *v == 1 || *v == 2));
        });
    }
}
