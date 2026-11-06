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
use std::rc::Weak;
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
    ) -> Result<(ComponentRef, RenderGuard), AccessError> {
        let reference = self.host.publish_component(owner)?;
        let guard = self.attach_surface(owner, reference, mount_surface);
        Ok((reference, guard))
    }

    /// Bind an engine-driven surface to a component reference the host already issued to
    /// `owner`, such as the one a presentation publication minted when its incarnation
    /// published it (ADR 031 §5). The reference was issued by this registry's host to `owner`;
    /// a second factory for one reference is a programming error. The factory lives exactly as
    /// long as the returned guard: whoever attaches it owns it and drops it when the
    /// incarnation closes.
    pub fn attach_surface(
        &self,
        owner: &Instance,
        reference: ComponentRef,
        mount_surface: impl Fn(Arc<Mount>, SurfaceWriter) -> Result<SurfaceSession, PresentationError>
        + 'static,
    ) -> RenderGuard {
        let factory = SurfaceFactory(Rc::new(mount_surface));
        let owner = owner.clone();
        self.retain(
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
    }

    fn retain(&self, reference: ComponentRef, renderer: Renderer) -> RenderGuard {
        assert!(
            self.renderers
                .borrow_mut()
                .insert(reference, renderer)
                .is_none(),
            "a renderer is already attached to this component reference"
        );
        RenderGuard {
            renderers: Rc::downgrade(&self.renderers),
            reference,
        }
    }
}

/// Owns one component's renderer in a [`NativeComponents`] registry: dropping the guard removes
/// it, so the render closure (and every value it captured) is dropped at the moment its owner
/// lets go, not whenever the registry next notices the host withdrew the reference. The
/// incarnation that published the component holds the guard and drops it as the views step of
/// its close (ADR 030 §6), while its dependencies are still retained. The host owns the
/// reference's lifetime (withdrawn when the supplier is torn down); the guard owns only the
/// renderer, and a mount that still shows a component whose renderer is gone renders the
/// withdrawn placeholder.
#[must_use = "dropping the guard removes the renderer immediately"]
pub struct RenderGuard {
    renderers: Weak<RefCell<BTreeMap<ComponentRef, Renderer>>>,
    reference: ComponentRef,
}

impl Drop for RenderGuard {
    fn drop(&mut self) {
        // The registry may be gone first (application teardown), taking the renderer with it.
        let Some(renderers) = self.renderers.upgrade() else {
            return;
        };
        // Bound to a local so the map's borrow ends before the closure's captures drop: a
        // captured value's `Drop` may reach back into this registry.
        let removed = renderers.borrow_mut().remove(&self.reference);
        drop(removed);
    }
}

impl RendererRegistry for NativeComponents {
    type Render = Element;

    fn for_host(host: &Host) -> Self {
        NativeComponents::new(host)
    }

    type Guard = RenderGuard;

    fn publish<F>(
        &self,
        owner: &Instance,
        render: F,
    ) -> Result<(ComponentRef, RenderGuard), AccessError>
    where
        F: Fn() -> Element + 'static,
    {
        let reference = self.host.publish_component(owner)?;
        let guard = self.retain(reference, Rc::new(move |_mount| render()));
        Ok((reference, guard))
    }
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
    // The owner drops its renderer as its close begins and the host withdraws the reference
    // only after the close returns, so a render in that window finds the mount live and the
    // renderer gone: it shows what a withdrawn component shows.
    match render {
        Some(render) => rsx! { RenderedMount { render, mount } },
        None => rsx! { p { "Component withdrawn." } },
    }
}

/// What one mount's render scope is made of. Equal while it is the same renderer and the same
/// mount, so a `MountedComponent` re-render (its liveness flipping, say) does not re-run the
/// micro's render.
#[derive(Props, Clone)]
struct RenderedMountProps {
    render: Renderer,
    mount: Arc<Mount>,
}

impl PartialEq for RenderedMountProps {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.render, &other.render) && Arc::ptr_eq(&self.mount, &other.mount)
    }
}

/// The scope a micro's render closure runs in, one per mount, like one instance of a custom
/// element: the hooks the closure calls are this mount's own, and nothing shares them with
/// another mount of the same publication. Being its own component, those hooks also run in a
/// fixed order, unlike in `MountedComponent`, which returns early while the mount is withdrawn.
#[expect(non_snake_case, reason = "Dioxus component naming")]
fn RenderedMount(props: RenderedMountProps) -> Element {
    (props.render)(&props.mount)
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
