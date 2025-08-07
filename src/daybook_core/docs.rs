use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use std::collections::HashMap;

#[derive(Debug, Clone, autosurgeon::Reconcile, autosurgeon::Hydrate, uniffi::Record)]
pub struct Doc {
    #[key]
    pub id: Uuid,
    #[autosurgeon(with = "crate::am::autosurgeon_date")]
    pub timestamp: OffsetDateTime,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone, uniffi::Enum)]
pub enum DocsEvent {
    ListChanged,
}

// Define a foreign trait that Kotlin will implement.
#[uniffi::export(with_foreign)]
pub trait DocsListener: Send + Sync + 'static {
    fn on_docs_event(&self, event: DocsEvent);
}

#[derive(autosurgeon::Reconcile, autosurgeon::Hydrate, Default)]
pub struct DocsAm {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    map: HashMap<Uuid, Doc>,
}
impl DocsAm {
    // const PATH: &[&str] = &["docs"];
    pub const PROP: &str = "docs";

    async fn load(cx: &Ctx) -> Res<Self> {
        cx.acx
            .hydrate_path::<Self>(automerge::ROOT, vec![Self::PROP.into()])
            .await?
            .ok_or_eyre("unable to find obj in am")
    }

    async fn flush(&self, cx: &Ctx) -> Res<()> {
        cx.acx
            .reconcile_prop(automerge::ROOT, Self::PROP, self)
            .await
    }
}

#[derive(uniffi::Object)]
struct DocsRepo {
    fcx: SharedFfiCtx,
    am: Arc<tokio::sync::RwLock<DocsAm>>,
    // Maintain weak references to listeners to avoid leaks.
    listeners: Arc<parking_lot::Mutex<Vec<(Uuid, Arc<dyn DocsListener>)>>>,
}

impl DocsRepo {
    async fn load(fcx: SharedFfiCtx) -> Res<Arc<Self>> {
        let am = DocsAm::load(&fcx.cx).await?;
        let am = Arc::new(tokio::sync::RwLock::new(am));
        Ok(Arc::new(Self {
            fcx,
            am,
            listeners: default(),
        }))
    }

    async fn get(&self, id: Uuid) -> Res<Option<Doc>> {
        let am = self.am.read().await;
        Ok(am.map.get(&id).cloned())
    }

    async fn set(&self, id: Uuid, val: Doc) -> Res<Option<Doc>> {
        let mut am = self.am.clone().write_owned().await;
        let ret = am.map.insert(id, val);
        am.flush(&self.fcx.cx).await?;
        // Notify listeners that the list changed
        self.notify(DocsEvent::ListChanged);
        Ok(ret)
    }

    async fn list(&self) -> Res<Vec<Doc>> {
        let am = self.am.read().await;
        Ok(am.map.values().cloned().collect())
    }

    fn notify(&self, event: DocsEvent) {
        // Iterate listeners, upgrading Weak refs and pruning dead ones.
        let mut lock = self.listeners.lock();
        for (_id, listener) in lock.iter() {
            let ev = event.clone();
            // Call synchronously; foreign side should hop to main thread as needed.
            listener.on_docs_event(ev);
        }
    }
}

// A registration handle that unregisters on drop.
#[derive(uniffi::Object)]
struct ListenerRegistration {
    repo: std::sync::Weak<DocsRepo>,
    id: Uuid,
}

#[uniffi::export]
impl ListenerRegistration {
    fn unregister(&self) {
        if let Some(repo) = self.repo.upgrade() {
            let mut lock = repo.listeners.lock();
            lock.retain(|(lid, _)| *lid != self.id);
        }
    }
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.upgrade() {
            // Best-effort cleanup
            let mut lock = repo.listeners.lock();
            lock.retain(|(lid, _)| *lid != self.id);
        }
    }
}

#[uniffi::export]
impl DocsRepo {
    #[uniffi::constructor]
    async fn for_ffi(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let cx = fcx.clone();
        let this = fcx.do_on_rt(Self::load(cx)).await?;
        Ok(this)
    }

    async fn ffi_get(self: Arc<Self>, id: Uuid) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .clone()
            .do_on_rt(async move { this.get(id).await })
            .await?;
        Ok(out)
    }

    async fn ffi_set(self: Arc<Self>, id: Uuid, doc: Doc) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .clone()
            .do_on_rt(async move { this.set(id, doc).await })
            .await?;
        Ok(out)
    }

    async fn ffi_list(self: Arc<Self>) -> Result<Vec<Doc>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .clone()
            .do_on_rt(async move { this.list().await })
            .await?;
        Ok(out)
    }

    // Register a listener; returns a handle that unregisters on drop.
    //
    // UniFFI expects callback parameters to be plain trait objects (Box<dyn Trait>) rather than Arc<dyn Trait>.
    async fn ffi_register_listener(
        self: Arc<Self>,
        listener: Arc<dyn DocsListener>,
    ) -> Result<Arc<ListenerRegistration>, FfiError> {
        let id = Uuid::new_v4();
        {
            let mut lock = self.listeners.lock();
            lock.push((id, listener));
            // strong is dropped here; we only keep Weak to avoid leaks.
        }
        Ok(Arc::new(ListenerRegistration {
            repo: Arc::downgrade(&self),
            id,
        }))
    }
}
