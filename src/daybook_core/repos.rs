use crate::interlude::*;

pub trait Repo {
    type Event: Send + Sync + 'static;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry>;

    /// Add a listener to the repository.
    ///
    /// Dropping the registration handle will unregister the listener.
    fn register_listener<F>(&self, listener: F) -> crate::repos::ListenerRegistration
    where
        F: Fn(Arc<Self::Event>) + Send + Sync + 'static,
    {
        let id = Uuid::new_v4();
        {
            let mut lock = self.registry().list.lock();
            lock.push((id, ErasedListener::new::<Self::Event>(listener)));
        }
        ListenerRegistration {
            // we only keep Weak to avoid leaks.
            registry: Arc::downgrade(self.registry()),
            id,
        }
    }
}

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
    // WARN: sync mutex, take care
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

    pub fn dump(&self) {
        let list = self.list.lock();
        info!("ListenersRegistry: ================================================");
        for (id, _listener) in list.iter() {
            info!("- {id}");
        }
        info!("================================================");
    }
}

// A registration handle that unregisters on drop.
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct ListenerRegistration {
    pub registry: std::sync::Weak<ListenersRegistry>,
    pub id: Uuid,
}

#[cfg_attr(feature = "uniffi", uniffi::export)]
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
