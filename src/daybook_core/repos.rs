use crate::interlude::*;

type ErasedEvent = Arc<dyn std::any::Any + Send + Sync + 'static>;

pub struct ErasedListener {
    on_event_cb: Arc<dyn Fn(ErasedEvent) + Send + Sync>,
}

impl ErasedListener {
    pub fn new<Ev>(on_event_cb: impl Fn(Arc<Ev>) + Send + Sync + 'static) -> Self
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
    // Maintain weak references to listeners to avoid leaks.
    pub list: parking_lot::Mutex<Vec<(Uuid, ErasedListener)>>,
}

impl ListenersRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { list: default() })
    }
    pub fn notify(&self, event: impl std::any::Any + Send + Sync + 'static) {
        let event = Arc::new(event);
        // Iterate listeners, upgrading Weak refs and pruning dead ones.
        let lock = self.list.lock();
        for (_id, listener) in lock.iter() {
            let ev = event.clone();
            // Call synchronously; foreign side should hop to main thread as needed.
            listener.on_event(ev);
        }
    }
}

// A registration handle that unregisters on drop.
#[derive(uniffi::Object)]
pub struct ListenerRegistration {
    pub registry: std::sync::Weak<ListenersRegistry>,
    pub id: Uuid,
}

#[uniffi::export]
impl ListenerRegistration {
    fn unregister(&self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut lock = registry.list.lock();
            lock.retain(|(lid, _)| *lid != self.id);
        }
    }
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            // Best-effort cleanup
            let mut lock = registry.list.lock();
            lock.retain(|(lid, _)| *lid != self.id);
        }
    }
}
