//! Host-side Dioxus adapter: the native renderer registry, component mounts, and surface
//! projection (ADR 015 §3). This crate is a backend: it depends inward on `clay_host` and
//! `clay_surface` only, so it names no composition registry, document model, or engine. Provider
//! assembly that needs those lives in `clay_mdk`, which implements against
//! [`RendererRegistry`] and [`NativeComponents::publish_surface`].
use clay_host::AccessError;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::Instance;
use clay_host::Mount;
use clay_host::RendererRegistry;
use clay_surface::EventResponse;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::SurfaceEventHandler;
use clay_surface::SurfaceTree;
use dioxus::prelude::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::watch;

type Renderer = Rc<dyn Fn(&Arc<Mount>) -> Element>;

/// Handle through which a mounted surface commits wire batches into its retained [`SurfaceTree`].
///
/// Because [`SurfaceTree`] excludes process-local `Any` attributes (D4), both the tree and its
/// [`Mount`] lifetime handle are `Send + Sync`, allowing engine resource tables (such as Wasmtime's
/// `ResourceTable`) to hold a [`SurfaceWriter`] directly without mixing `!Send` UI state into the
/// engine store.
#[derive(Clone)]
pub struct SurfaceWriter {
    mount: Arc<Mount>,
    tree: Arc<Mutex<SurfaceTree<ComponentRef>>>,
    ancestors: Vec<Arc<Mount>>,
    ancestor_surfaces: Vec<Arc<AtomicBool>>,
    revoked: Arc<AtomicBool>,
    revisions: watch::Sender<u64>,
}

impl SurfaceWriter {
    /// A writer bound to an existing [`Mount`] lease. The adapter builds one per mounted surface;
    /// an engine adapter that drives its own mount (see [`NativeComponents::publish_surface`])
    /// builds one directly to feed a sink it holds.
    pub fn new(mount: Arc<Mount>) -> Self {
        let (revisions, _) = watch::channel(0);
        Self {
            mount,
            tree: Arc::new(Mutex::new(SurfaceTree::new())),
            ancestors: Vec::new(),
            ancestor_surfaces: Vec::new(),
            revoked: Arc::new(AtomicBool::new(false)),
            revisions,
        }
    }

    pub fn is_live(&self) -> bool {
        !self.revoked.load(Ordering::Acquire)
            && self.mount.is_live()
            && self.ancestors.iter().all(|mount| mount.is_live())
            && self
                .ancestor_surfaces
                .iter()
                .all(|revoked| !revoked.load(Ordering::Acquire))
    }

    pub fn revision(&self) -> u64 {
        self.tree.lock().unwrap().revision()
    }

    pub fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        let mut tree = self.tree.lock().unwrap();
        if !self.is_live() {
            if !self.revoked.swap(true, Ordering::AcqRel) {
                tree.clear();
                self.revisions.send_replace(tree.revision());
            }
            return Err(PresentationError::Revoked);
        }
        let result = tree.apply(edits);
        if result.is_err() {
            self.revoked.store(true, Ordering::Release);
        }
        self.revisions.send_replace(tree.revision());
        result
    }

    pub fn withdraw(&self) {
        let mut tree = self.tree.lock().unwrap();
        if !self.revoked.swap(true, Ordering::AcqRel) {
            tree.clear();
            self.revisions.send_replace(tree.revision());
        }
    }
}

pub use clay_surface::SurfaceSession;

impl clay_surface::SurfaceSink<ComponentRef> for SurfaceWriter {
    fn is_live(&self) -> bool {
        self.is_live()
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        self.apply(edits)
    }

    fn withdraw(&self) {
        self.withdraw();
    }
}

type SurfaceMountFn =
    Rc<dyn Fn(Arc<Mount>, SurfaceWriter) -> Result<SurfaceSession, PresentationError>>;

#[derive(Clone)]
struct SurfaceFactory(SurfaceMountFn);

impl PartialEq for SurfaceFactory {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
struct SharedMount(Arc<Mount>);

impl PartialEq for SharedMount {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub struct NativeComponents {
    host: Host,
    renderers: Rc<RefCell<BTreeMap<ComponentRef, Renderer>>>,
}
impl PartialEq for NativeComponents {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.renderers, &other.renderers)
    }
}
impl NativeComponents {
    pub fn new(host: &Host) -> Self {
        Self {
            host: host.clone(),
            renderers: Rc::default(),
        }
    }
    /// The host this registry serves. Every publication and mount below is checked against it,
    /// so a component reference issued here is only mountable while its owner instance lives.
    pub fn host(&self) -> &Host {
        &self.host
    }
    /// Publish an engine-driven surface: the factory receives the retained [`Mount`] lifetime
    /// handle and a [`SurfaceWriter`] it commits wire batches into. Unlike
    /// [`RendererRegistry::publish`] the factory produces no element itself — the mounted
    /// surface renders the writer's retained tree — so this stays an inherent method.
    pub fn publish_surface(
        &self,
        owner: &Instance,
        mount_surface: impl Fn(Arc<Mount>, SurfaceWriter) -> Result<SurfaceSession, PresentationError>
        + 'static,
    ) -> Result<ComponentRef, AccessError> {
        self.recover_withdrawn();
        let reference = self.host.publish_component(owner)?;
        let factory = SurfaceFactory(Rc::new(mount_surface));
        let owner = owner.clone();
        assert!(
            self.renderers
                .borrow_mut()
                .insert(
                    reference,
                    Rc::new(move |mount| {
                        rsx! {
                            MountedSurface {
                                mount: SharedMount(mount.clone()),
                                owner: owner.clone(),
                                factory: factory.clone(),
                            }
                        }
                    }),
                )
                .is_none()
        );
        Ok(reference)
    }
    fn recover_withdrawn(&self) {
        let allowed = self.host.component_lifetimes().borrow().clone();
        self.renderers
            .borrow_mut()
            .retain(|reference, _| allowed.contains(reference));
    }
}

impl RendererRegistry for NativeComponents {
    type Render = Element;

    fn for_host(host: &Host) -> Self {
        NativeComponents::new(host)
    }

    fn publish<F>(&self, owner: &Instance, render: F) -> Result<ComponentRef, AccessError>
    where
        F: Fn() -> Element + 'static,
    {
        self.recover_withdrawn();
        let reference = self.host.publish_component(owner)?;
        let render = Rc::new(render);
        assert!(
            self.renderers
                .borrow_mut()
                .insert(reference, Rc::new(move |_mount| render()))
                .is_none()
        );
        Ok(reference)
    }
}

/// The native adapter drops local factories when the host withdraws their portable references.
/// This task belongs to the application render scope; no micro-specific cleanup lives in startup.
pub fn use_component_recovery(components: NativeComponents) {
    use_future(move || {
        let mut lifetimes = components.host.component_lifetimes();
        let registry = Rc::downgrade(&components.renderers);
        async move {
            loop {
                let allowed = lifetimes.borrow_and_update().clone();
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                registry
                    .borrow_mut()
                    .retain(|reference, _| allowed.contains(reference));
                if lifetimes.changed().await.is_err() {
                    break;
                }
            }
        }
    });
}

#[derive(Clone, Default)]
struct MountPath {
    references: Vec<ComponentRef>,
    mounts: Vec<Arc<Mount>>,
    surfaces: Vec<Arc<AtomicBool>>,
}

impl MountPath {
    const MAX_DEPTH: usize = 64;

    fn is_live(&self) -> bool {
        self.mounts.iter().all(|mount| mount.is_live())
            && self
                .surfaces
                .iter()
                .all(|revoked| !revoked.load(Ordering::Acquire))
    }
}

#[component]
pub fn ComponentMount(
    owner: Instance,
    reference: ComponentRef,
    #[props(default)] components: Option<NativeComponents>,
) -> Element {
    let components = components.unwrap_or_else(use_context::<NativeComponents>);
    // A changed reference is a new mount incarnation, even when its collection entry stays put.
    rsx! { MountedComponent { key: "{reference:?}", components, owner, reference } }
}

#[component]
fn MountedComponent(
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
) -> Element {
    let path = use_hook(|| try_consume_context::<MountPath>().unwrap_or_default());
    let rejected =
        path.references.contains(&reference) || path.references.len() >= MountPath::MAX_DEPTH;
    use_context_provider(|| components.clone());
    let mount = use_hook(|| {
        if rejected || !path.is_live() {
            return None;
        }
        match components.host.mount(&owner, reference) {
            Ok(mount) => Some(Arc::new(mount)),
            Err(AccessError::Revoked) => None,
            Err(AccessError::Denied) => panic!("native component mount crossed its granted host"),
        }
    });
    use_context_provider(|| {
        let mut path = path.clone();
        path.references.push(reference);
        if let Some(mount) = &mount {
            path.mounts.push(mount.clone());
        }
        path
    });
    let mut live = use_signal(|| mount.as_ref().is_some_and(|mount| mount.is_live()));
    let subscription = mount.clone();
    use_future(move || {
        let mount = subscription.clone();
        async move {
            if let Some(mount) = mount {
                let mut changes = mount.subscribe();
                loop {
                    live.set(*changes.borrow_and_update());
                    if changes.changed().await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    if rejected {
        return rsx! { p { "Component unavailable: cyclic or excessively deep mount." } };
    }
    let Some(mount) = mount.filter(|mount| live() && mount.is_live() && path.is_live()) else {
        return rsx! { p { "Component withdrawn." } };
    };
    let render = components.renderers.borrow().get(&reference).cloned();
    match render {
        Some(render) => render(&mount),
        None => panic!("live component has no native renderer"),
    }
}

#[component]
fn MountedSurface(mount: SharedMount, owner: Instance, factory: SurfaceFactory) -> Element {
    let path = use_hook(|| try_consume_context::<MountPath>().unwrap_or_default());
    let writer = use_hook(|| {
        let mut writer = SurfaceWriter::new(mount.0.clone());
        writer.ancestors = path.mounts.clone();
        writer.ancestor_surfaces = path.surfaces.clone();
        writer
    });
    use_context_provider(|| {
        let mut path = path.clone();
        path.surfaces.push(writer.revoked.clone());
        path
    });
    let session = use_hook(|| match (factory.0)(mount.0.clone(), writer.clone()) {
        Ok(session) => Ok(Rc::new(session)),
        Err(error) => {
            writer.withdraw();
            Err(error)
        }
    });
    let mut revision = use_signal(|| writer.revision());
    let writer_for_watch = writer.clone();
    use_future(move || {
        let mut updates = writer_for_watch.revisions.subscribe();
        async move {
            loop {
                revision.set(*updates.borrow_and_update());
                if updates.changed().await.is_err() {
                    break;
                }
            }
        }
    });
    let writer_for_drop = writer.clone();
    let session_for_drop = session.clone();
    use_drop(move || {
        writer_for_drop.withdraw();
        drop(session_for_drop);
    });

    let _current_revision = revision();
    let session = match session {
        Err(error) => return rsx! { p { "Component unavailable: {error}" } },
        Ok(session) if writer.is_live() => session,
        Ok(_) => return rsx! { p { "Component withdrawn." } },
    };
    let dispatch = session.handler();
    let writer_for_event = writer.clone();
    let handler: SurfaceEventHandler = Rc::new(move |event| {
        if !writer_for_event.is_live() {
            return EventResponse::default();
        }
        let current_revision = writer_for_event.revision();
        if event.revision != current_revision {
            let mut rev = revision;
            rev.set(current_revision);
            return EventResponse::default();
        }
        let response = dispatch(event);
        let mut rev = revision;
        rev.set(writer_for_event.revision());
        response
    });
    let tree = writer.tree.lock().unwrap();
    tree.render(&handler, &|reference| {
        rsx! {
            ComponentMount { owner: owner.clone(), reference: *reference }
        }
    })
}

#[cfg(test)]
mod tests;
