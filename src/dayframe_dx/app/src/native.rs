//! Native renderer adapter. Closures stay here; the host boundary carries component references.
use crate::{
    host::{AccessError, ComponentRef, Host, Instance, Mount},
    presentation::{
        Event, EventResponse, Mutation, PresentationError, SurfaceEventHandler, SurfaceTree,
    },
};
use dioxus::prelude::*;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex},
};
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
    tree: Arc<Mutex<SurfaceTree>>,
    revisions: watch::Sender<u64>,
}

impl SurfaceWriter {
    fn new(mount: Arc<Mount>) -> Self {
        let (revisions, _) = watch::channel(0);
        Self {
            mount,
            tree: Arc::new(Mutex::new(SurfaceTree::new())),
            revisions,
        }
    }

    pub fn is_live(&self) -> bool {
        self.mount.is_live()
    }

    pub fn revision(&self) -> u64 {
        self.tree.lock().unwrap().revision()
    }

    pub fn apply(&self, edits: &[Mutation]) -> Result<u64, PresentationError> {
        if !self.mount.is_live() {
            return Err(PresentationError::Revoked);
        }
        let revision = self.tree.lock().unwrap().apply(edits);
        self.revisions.send_replace(revision);
        Ok(revision)
    }

    pub fn withdraw(&self) {
        let revision = {
            let mut tree = self.tree.lock().unwrap();
            tree.clear();
            tree.revision()
        };
        self.revisions.send_replace(revision);
    }
}

/// Active surface binding returned when a surface component mounts into a [`Mount`].
pub struct SurfaceSession {
    dispatch: SurfaceEventHandler,
    on_drop: Option<Box<dyn FnOnce()>>,
}

impl SurfaceSession {
    pub fn new(dispatch: impl Fn(Event) -> EventResponse + 'static) -> Self {
        Self {
            dispatch: Rc::new(dispatch),
            on_drop: None,
        }
    }

    pub fn with_drop(
        dispatch: impl Fn(Event) -> EventResponse + 'static,
        on_drop: impl FnOnce() + 'static,
    ) -> Self {
        Self {
            dispatch: Rc::new(dispatch),
            on_drop: Some(Box::new(on_drop)),
        }
    }
}

impl Drop for SurfaceSession {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
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
    /// A renderer may capture this factory, never the registry itself: otherwise publishing
    /// a layout that mounts other content would create a registry → renderer → registry cycle.
    pub fn weak_context(&self) -> impl Fn() -> Self + 'static {
        let registry = Rc::downgrade(&self.renderers);
        let host = self.host.clone();
        move || Self {
            host: host.clone(),
            renderers: registry
                .upgrade()
                .expect("renderer invoked after native adapter destruction"),
        }
    }
    pub fn new(host: &Host) -> Self {
        Self {
            host: host.clone(),
            renderers: Rc::default(),
        }
    }
    pub fn publish(
        &self,
        owner: &Instance,
        render: impl Fn() -> Element + 'static,
    ) -> Result<ComponentRef, AccessError> {
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
    pub fn publish_surface(
        &self,
        owner: &Instance,
        mount_surface: impl Fn(Arc<Mount>, SurfaceWriter) -> Result<SurfaceSession, PresentationError>
        + 'static,
    ) -> Result<ComponentRef, AccessError> {
        self.recover_withdrawn();
        let reference = self.host.publish_component(owner)?;
        let factory = SurfaceFactory(Rc::new(mount_surface));
        assert!(
            self.renderers
                .borrow_mut()
                .insert(
                    reference,
                    Rc::new(move |mount| {
                        rsx! {
                            MountedSurface {
                                mount: SharedMount(mount.clone()),
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

#[component]
pub fn ComponentMount(
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
) -> Element {
    // A changed reference is a new mount incarnation, even when its collection entry stays put.
    rsx! { MountedComponent { key: "{reference:?}", components, owner, reference } }
}

#[component]
fn MountedComponent(
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
) -> Element {
    let mount = use_hook(|| match components.host.mount(&owner, reference) {
        Ok(mount) => Some(Arc::new(mount)),
        Err(AccessError::Revoked) => None,
        Err(AccessError::Denied) => panic!("native component mount crossed its granted host"),
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
    let Some(mount) = mount.filter(|mount| live() && mount.is_live()) else {
        return rsx! { p { "Component withdrawn." } };
    };
    let render = components.renderers.borrow().get(&reference).cloned();
    match render {
        Some(render) => render(&mount),
        None => panic!("live component has no native renderer"),
    }
}

#[component]
fn MountedSurface(mount: SharedMount, factory: SurfaceFactory) -> Element {
    let writer = use_hook(|| SurfaceWriter::new(mount.0.clone()));
    let session = use_hook(|| (factory.0)(mount.0.clone(), writer.clone()).ok().map(Rc::new));
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
    let Some(session) = session.filter(|_| writer.is_live()) else {
        return rsx! { p { "Component withdrawn." } };
    };
    let dispatch = session.dispatch.clone();
    let writer_for_event = writer.clone();
    let handler: SurfaceEventHandler = Rc::new(move |event| {
        let response = dispatch(event);
        let mut rev = revision;
        rev.set(writer_for_event.revision());
        response
    });
    let tree = writer.tree.lock().unwrap();
    tree.render(&handler)
}
