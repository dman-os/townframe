//! FIXME: fuck me, we have a name clash with the core::repo module

use crate::interlude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct RepoStopToken {
    pub cancel_token: CancellationToken,
    pub worker_handle: Option<JoinHandle<()>>,
}

impl RepoStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle {
            handle.await?;
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

/// Returns true when a live notification patch should be ignored by repo listeners.
///
/// This is intentionally only for live notification handling. Historical replay paths
/// (for example `diff_events`) should pass `live_origin = None` and must not be skipped.
pub fn should_skip_live_patch(
    live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
    exclude_peer_id: Option<&am_utils_rs::repo::PeerId>,
) -> bool {
    match live_origin {
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Local) => true,
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Remote { peer_id, .. }) => {
            exclude_peer_id.is_some_and(|exclude| peer_id == exclude)
        }
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Bootstrap) | None => false,
    }
}

/// Resolve event origin for versioned map updates where a `vtag` actor id is present.
///
/// `live_origin` is authoritative for live notifications. Historical replay (`None`)
/// cannot reliably recover source transport semantics, so it falls back to non-local/unknown.
pub fn resolve_origin_from_vtag_actor(
    local_actor_id: &automerge::ActorId,
    vtag_actor_id: &automerge::ActorId,
    live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
) -> crate::event_origin::SwitchEventOrigin {
    match live_origin {
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Bootstrap) => {
            crate::event_origin::SwitchEventOrigin::Bootstrap
        }
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Remote { peer_id, .. }) => {
            crate::event_origin::SwitchEventOrigin::Remote {
                peer_id: peer_id.to_string(),
            }
        }
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Local) => {
            if vtag_actor_id == local_actor_id {
                crate::event_origin::SwitchEventOrigin::Local {
                    actor_id: vtag_actor_id.to_string(),
                }
            } else {
                crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: "unknown".to_string(),
                }
            }
        }
        None => crate::event_origin::SwitchEventOrigin::Remote {
            peer_id: "unknown".to_string(),
        },
    }
}

/// Resolve event origin for delete patches where no `vtag` exists on the deleted entry.
///
/// If `live_origin` is unavailable (historical replay), optional tombstone actor can be used
/// for local-vs-nonlocal inference; otherwise this falls back to non-local/unknown.
pub fn resolve_origin_for_delete(
    local_actor_id: &automerge::ActorId,
    live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
    tombstone_actor_id: Option<&automerge::ActorId>,
) -> crate::event_origin::SwitchEventOrigin {
    match live_origin {
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Local) => {
            crate::event_origin::SwitchEventOrigin::Local {
                actor_id: local_actor_id.to_string(),
            }
        }
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Remote { peer_id, .. }) => {
            crate::event_origin::SwitchEventOrigin::Remote {
                peer_id: peer_id.to_string(),
            }
        }
        Some(am_utils_rs::repo::BigRepoChangeOrigin::Bootstrap) => {
            crate::event_origin::SwitchEventOrigin::Bootstrap
        }
        None => {
            if let Some(actor_id) = tombstone_actor_id {
                if actor_id == local_actor_id {
                    crate::event_origin::SwitchEventOrigin::Local {
                        actor_id: actor_id.to_string(),
                    }
                } else {
                    crate::event_origin::SwitchEventOrigin::Remote {
                        peer_id: "unknown".to_string(),
                    }
                }
            } else {
                crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: "unknown".to_string(),
                }
            }
        }
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
        let (sender, receiver) = async_channel::bounded::<Arc<E>>(opts.capacity);
        let dropped_count = Arc::new(AtomicU64::new(0));
        let dropped_count_for_cb = Arc::clone(&dropped_count);

        let registration = Arc::new(ListenerRegistration::new(
            Arc::downgrade(self),
            None,
        ));

        {
            let mut lock = self.list.lock().expect(ERROR_MUTEX);
            lock.push((
                registration.id,
                ErasedListener::new(move |event: Arc<E>| {
                    match sender.force_send(Arc::clone(&event)) {
                        Ok(Some(_)) => {
                            dropped_count_for_cb.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }),
            ));
        }

        ListenerHandle::new(receiver, registration, dropped_count)
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
    receiver: async_channel::Receiver<Arc<E>>,
    registration: Arc<ListenerRegistration>,
    dropped_count: Arc<AtomicU64>,
    seen_dropped_count: AtomicU64,
}

impl<E> ListenerHandle<E>
where
    E: Send + Sync + 'static,
{
    fn new(
        receiver: async_channel::Receiver<Arc<E>>,
        registration: Arc<ListenerRegistration>,
        dropped_count: Arc<AtomicU64>,
    ) -> Self {
        Self {
            receiver,
            registration,
            dropped_count,
            seen_dropped_count: AtomicU64::new(0),
        }
    }

    pub fn registration(&self) -> Arc<ListenerRegistration> {
        Arc::clone(&self.registration)
    }

    fn take_drop_error(&self) -> Option<u64> {
        let now = self.dropped_count.load(Ordering::Acquire);
        let seen = self.seen_dropped_count.load(Ordering::Acquire);
        if now > seen {
            self.seen_dropped_count.store(now, Ordering::Release);
            Some(now - seen)
        } else {
            None
        }
    }

    pub fn has_dropped(&self) -> bool {
        self.dropped_count.load(Ordering::Acquire) > 0
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.receiver.close();
        self.registration.unregister_impl();
    }

    pub fn try_recv(&self) -> Result<Arc<E>, TryRecvError> {
        if let Some(dropped_count) = self.take_drop_error() {
            return Err(TryRecvError::Dropped { dropped_count });
        }

        match self.receiver.try_recv() {
            Ok(ev) => Ok(ev),
            Err(async_channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(async_channel::TryRecvError::Closed) => Err(TryRecvError::Closed),
        }
    }

    pub fn try_recv_lossy(&self) -> Result<Arc<E>, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(ev) => Ok(ev),
            Err(async_channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(async_channel::TryRecvError::Closed) => Err(TryRecvError::Closed),
        }
    }

    pub fn recv_blocking(&self) -> Result<Arc<E>, RecvError> {
        if let Some(dropped_count) = self.take_drop_error() {
            return Err(RecvError::Dropped { dropped_count });
        }

        match self.receiver.recv_blocking() {
            Ok(ev) => Ok(ev),
            Err(async_channel::RecvError) => Err(RecvError::Closed),
        }
    }

    pub fn recv_lossy_blocking(&self) -> Result<Arc<E>, RecvError> {
        match self.receiver.recv_blocking() {
            Ok(ev) => Ok(ev),
            Err(async_channel::RecvError) => Err(RecvError::Closed),
        }
    }

    pub async fn recv_async(&self) -> Result<Arc<E>, RecvError> {
        if let Some(dropped_count) = self.take_drop_error() {
            return Err(RecvError::Dropped { dropped_count });
        }

        match self.receiver.recv().await {
            Ok(ev) => Ok(ev),
            Err(async_channel::RecvError) => Err(RecvError::Closed),
        }
    }

    pub async fn recv_lossy_async(&self) -> Result<Arc<E>, RecvError> {
        match self.receiver.recv().await {
            Ok(ev) => Ok(ev),
            Err(async_channel::RecvError) => Err(RecvError::Closed),
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
            if let Some(dropped_count) = handle.take_drop_error() {
                return Some((Err(RecvError::Dropped { dropped_count }), handle));
            }
            match handle.receiver.recv().await {
                Ok(ev) => Some((Ok(ev), handle)),
                Err(async_channel::RecvError) => None,
            }
        })
    }

    pub fn into_stream_lossy(self) -> impl futures::stream::Stream<Item = Arc<E>> + Send {
        futures::stream::unfold(self, |handle| async move {
            match handle.receiver.recv().await {
                Ok(ev) => Some((ev, handle)),
                Err(async_channel::RecvError) => None,
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

    struct TestSender {
        inner: async_channel::Sender<Arc<u64>>,
        dropped_count: Arc<AtomicU64>,
    }

    impl TestSender {
        fn try_send(&self, v: u64) {
            self.inner.try_send(Arc::new(v)).unwrap();
        }

        fn force_send(&self, v: u64) {
            if let Ok(Some(_)) = self.inner.force_send(Arc::new(v)) {
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn mk_handle(capacity: usize) -> (TestSender, ListenerHandle<u64>) {
        let (sender, receiver) = async_channel::bounded(capacity);
        let dropped_count = Arc::new(AtomicU64::new(0));
        let reg = Arc::new(ListenerRegistration {
            registry: Weak::new(),
            id: Uuid::new_v4(),
            on_unregister: None,
            on_unregister_extra: std::sync::Mutex::new(Vec::new()),
            is_unregistered: AtomicBool::new(false),
        });
        let handle = ListenerHandle::new(receiver, reg, Arc::clone(&dropped_count));
        let sender = TestSender { inner: sender, dropped_count };
        (sender, handle)
    }

    #[test]
    fn bounded_queue_drops_oldest_and_surfaces_drop_error() {
        let (sender, handle) = mk_handle(2);
        sender.try_send(1);
        sender.try_send(2);
        sender.force_send(3);

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
        let (_sender, handle) = mk_handle(2);
        handle.close();

        assert_eq!(handle.try_recv(), Err(TryRecvError::Closed));
        assert_eq!(handle.try_recv_lossy(), Err(TryRecvError::Closed));
        assert_eq!(handle.recv_blocking(), Err(RecvError::Closed));
        assert_eq!(handle.recv_lossy_blocking(), Err(RecvError::Closed));
    }

    #[test]
    fn lossy_try_recv_skips_drop_error() {
        let (sender, handle) = mk_handle(1);
        sender.try_send(10);
        sender.force_send(20);

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

#[cfg(test)]
mod origin_tests {
    use super::*;
    use am_utils_rs::repo::BigRepoChangeOrigin;

    #[test]
    fn should_skip_live_patch_skips_local_and_keeps_unrelated_remote() {
        let exclude_peer = crate::peer_id_from_label("peer-exclude");
        assert!(should_skip_live_patch(
            Some(&BigRepoChangeOrigin::Local),
            Some(&exclude_peer)
        ));

        let other_peer = crate::peer_id_from_label("peer-other");
        assert!(!should_skip_live_patch(
            Some(&BigRepoChangeOrigin::Remote {
                peer_id: other_peer
            }),
            Some(&exclude_peer)
        ));
    }

    #[test]
    fn should_skip_live_patch_skips_matching_remote_peer() {
        let exclude_peer = crate::peer_id_from_label("peer-a");
        assert!(should_skip_live_patch(
            Some(&BigRepoChangeOrigin::Remote {
                peer_id: exclude_peer
            }),
            Some(&exclude_peer)
        ));
    }

    #[test]
    fn resolve_origin_from_vtag_actor_bootstrap_takes_precedence_over_local_actor_match() {
        let actor = automerge::ActorId::from([1_u8; 16]);
        let origin =
            resolve_origin_from_vtag_actor(&actor, &actor, Some(&BigRepoChangeOrigin::Bootstrap));
        assert!(matches!(
            origin,
            crate::event_origin::SwitchEventOrigin::Bootstrap
        ));
    }

    #[test]
    fn resolve_origin_from_vtag_actor_replay_none_does_not_mark_local() {
        let actor = automerge::ActorId::from([1_u8; 16]);
        let origin = resolve_origin_from_vtag_actor(&actor, &actor, None);
        assert!(matches!(
            origin,
            crate::event_origin::SwitchEventOrigin::Remote { peer_id } if peer_id == "unknown"
        ));
    }

    #[test]
    fn resolve_origin_from_vtag_actor_live_local_stays_local_for_matching_actor() {
        let actor = automerge::ActorId::from([1_u8; 16]);
        let origin =
            resolve_origin_from_vtag_actor(&actor, &actor, Some(&BigRepoChangeOrigin::Local));
        assert!(matches!(
            origin,
            crate::event_origin::SwitchEventOrigin::Local { actor_id } if actor_id == actor.to_string()
        ));
    }

    #[test]
    fn resolve_origin_from_vtag_actor_live_remote_maps_peer_id() {
        let local_actor = automerge::ActorId::from([1_u8; 16]);
        let vtag_actor = automerge::ActorId::from([2_u8; 16]);
        let remote_peer_id = crate::peer_id_from_label("peer-123");
        let origin = resolve_origin_from_vtag_actor(
            &local_actor,
            &vtag_actor,
            Some(&BigRepoChangeOrigin::Remote {
                peer_id: remote_peer_id,
            }),
        );
        assert!(matches!(
            origin,
            crate::event_origin::SwitchEventOrigin::Remote { peer_id } if peer_id == remote_peer_id.to_string()
        ));
    }

    #[test]
    fn resolve_origin_from_vtag_actor_live_local_mismatch_maps_unknown_remote() {
        let local_actor = automerge::ActorId::from([1_u8; 16]);
        let vtag_actor = automerge::ActorId::from([2_u8; 16]);
        let origin = resolve_origin_from_vtag_actor(
            &local_actor,
            &vtag_actor,
            Some(&BigRepoChangeOrigin::Local),
        );
        assert!(matches!(
            origin,
            crate::event_origin::SwitchEventOrigin::Remote { peer_id } if peer_id == "unknown"
        ));
    }
}
