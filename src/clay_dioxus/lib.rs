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
use clay_surface::ELEMENT_REPLY;
use clay_surface::ElementCommand;
use clay_surface::ElementHandles;
use clay_surface::ElementRead;
use clay_surface::EventResponse;
use clay_surface::Mutation;
use clay_surface::NodeId;
use clay_surface::PresentationError;
use clay_surface::ScrollBehavior;
use clay_surface::SurfaceEventHandler;
use clay_surface::SurfaceTree;
use dioxus::prelude::*;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::watch;

mod frames;
mod timing;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod direct;
/// Native hosts have no direct receiver (X4 spike): nothing ever requests one.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod direct {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Kind {}
    pub(crate) fn requested(_owner: &str) -> Option<Kind> {
        None
    }
    pub(crate) fn create(kind: Kind) -> u32 {
        match kind {}
    }
    pub(crate) fn container_id(_id: u32) -> String {
        unreachable!("no direct receiver off the browser")
    }
    pub(crate) fn bind(_id: u32, _handler: clay_surface::SurfaceEventHandler) {
        unreachable!()
    }
    pub(crate) fn attach(_id: u32) {
        unreachable!()
    }
    pub(crate) fn apply(
        _id: u32,
        _edits: &[clay_surface::Mutation<clay_host::ComponentRef>],
    ) -> Result<u64, clay_surface::PresentationError> {
        unreachable!()
    }
    pub(crate) fn command(_id: u32, _element: u32, _command: clay_surface::ElementCommand) {
        unreachable!()
    }
    pub(crate) fn withdraw(_id: u32) {
        unreachable!()
    }
}

type Renderer = Rc<dyn Fn(&Arc<Mount>) -> Element>;
type CommandReceiver = mpsc::UnboundedReceiver<(NodeId, ElementCommand)>;

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
    /// Element commands the guest sent, drained by the mounted surface on the renderer's
    /// thread (the handles they act on are not `Send`).
    commands: mpsc::UnboundedSender<(NodeId, ElementCommand)>,
    command_receiver: Arc<Mutex<Option<CommandReceiver>>>,
    /// Guest element ids whose `mounted` event the renderer has delivered: the only elements a
    /// command may address.
    mounted: Arc<Mutex<BTreeSet<NodeId>>>,
    /// X4 spike: the direct receiver this surface's batches go to instead of the retained tree.
    /// Chosen when the writer is built (`MountedSurface`), before the guest can commit.
    direct: Option<u32>,
}

impl SurfaceWriter {
    /// A writer bound to an existing [`Mount`] lease. The adapter builds one per mounted surface;
    /// an engine adapter that drives its own mount (see [`NativeComponents::publish_surface`])
    /// builds one directly to feed a sink it holds.
    pub fn new(mount: Arc<Mount>) -> Self {
        let (revisions, _) = watch::channel(0);
        let (commands, command_receiver) = mpsc::unbounded_channel();
        Self {
            mount,
            tree: Arc::new(Mutex::new(SurfaceTree::new())),
            ancestors: Vec::new(),
            ancestor_surfaces: Vec::new(),
            revoked: Arc::new(AtomicBool::new(false)),
            revisions,
            commands,
            command_receiver: Arc::new(Mutex::new(Some(command_receiver))),
            mounted: Arc::default(),
            direct: None,
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
        if let Some(direct) = self.direct {
            if !self.is_live() {
                self.revoked.store(true, Ordering::Release);
                return Err(PresentationError::Revoked);
            }
            let started = timing::now();
            let result = direct::apply(direct, edits);
            timing::record("apply", started, edits.len() as f64, 0.0);
            return result;
        }
        let mut tree = self.tree.lock().unwrap();
        if !self.is_live() {
            if !self.revoked.swap(true, Ordering::AcqRel) {
                tree.clear();
                self.revisions.send_replace(tree.revision());
            }
            return Err(PresentationError::Revoked);
        }
        let started = timing::now();
        let result = tree.apply(edits);
        timing::record(
            "apply",
            started,
            edits.len() as f64,
            tree.node_count() as f64,
        );
        if result.is_err() {
            self.revoked.store(true, Ordering::Release);
        }
        self.revisions.send_replace(tree.revision());
        result
    }

    pub fn withdraw(&self) {
        if let Some(direct) = self.direct {
            if !self.revoked.swap(true, Ordering::AcqRel) {
                direct::withdraw(direct);
            }
            return;
        }
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

    fn element_command(
        &self,
        element: NodeId,
        command: ElementCommand,
    ) -> Result<(), PresentationError> {
        if !self.is_live() {
            return Err(PresentationError::Revoked);
        }
        if !self.mounted.lock().unwrap().contains(&element) {
            return Err(PresentationError::InvalidBatch(format!(
                "element {element} has not announced mounted"
            )));
        }
        if let Some(direct) = self.direct {
            direct::command(direct, element, command);
            return Ok(());
        }
        self.commands
            .send((element, command))
            .map_err(|_| PresentationError::Revoked)
    }
}

/// Run one element command against the element's host handle. A read answers with the JSON
/// value of the `element-reply` event; a failure answers with its message.
async fn run_element_command(
    mounted: &dioxus_html::MountedData,
    command: ElementCommand,
) -> Result<serde_json::Value, String> {
    use dioxus_html::geometry::PixelsVector2D;
    let behavior = |behavior| match behavior {
        ScrollBehavior::Instant => dioxus_html::ScrollBehavior::Instant,
        ScrollBehavior::Smooth => dioxus_html::ScrollBehavior::Smooth,
    };
    let failed = |error: dioxus_html::MountedError| error.to_string();
    match command {
        ElementCommand::Scroll(to) => mounted
            .scroll(PixelsVector2D::new(to.left, to.top), behavior(to.behavior))
            .await
            .map(|()| serde_json::Value::Null)
            .map_err(failed),
        ElementCommand::ScrollBy(by) => {
            let current = mounted.get_scroll_offset().await.map_err(failed)?;
            mounted
                .scroll(
                    PixelsVector2D::new(current.x + by.left, current.y + by.top),
                    behavior(by.behavior),
                )
                .await
                .map(|()| serde_json::Value::Null)
                .map_err(failed)
        }
        ElementCommand::SetFocus(focus) => mounted
            .set_focus(focus)
            .await
            .map(|()| serde_json::Value::Null)
            .map_err(failed),
        ElementCommand::Read { what, .. } => Ok(match what {
            ElementRead::ClientRect => {
                let rect = mounted.get_client_rect().await.map_err(failed)?;
                serde_json::json!({
                    "x": rect.origin.x, "y": rect.origin.y,
                    "width": rect.size.width, "height": rect.size.height,
                })
            }
            ElementRead::ScrollOffset => {
                let offset = mounted.get_scroll_offset().await.map_err(failed)?;
                serde_json::json!({ "x": offset.x, "y": offset.y })
            }
            ElementRead::ScrollSize => {
                let size = mounted.get_scroll_size().await.map_err(failed)?;
                serde_json::json!({ "width": size.width, "height": size.height })
            }
        }),
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
        Some(render) => render(&mount),
        None => rsx! { p { "Component withdrawn." } },
    }
}

#[component]
fn MountedSurface(mount: SharedMount, owner: Instance, factory: SurfaceFactory) -> Element {
    let path = use_hook(|| try_consume_context::<MountPath>().unwrap_or_default());
    let writer = use_hook(|| {
        let mut writer = SurfaceWriter::new(mount.0.clone());
        writer.direct = direct::requested(&owner.name).map(direct::create);
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

    // Element commands the guest sent run here, on the renderer's thread, against the host
    // handles `mounted` events collected; a read's answer re-enters the guest as an
    // `element-reply` event. The raw session handler is used: a reply is not an input event
    // and must not be dropped as stale.
    let handles = use_hook(ElementHandles::default);
    // Hooks stay above the early returns below.
    let held = use_hook(|| Rc::new(RefCell::new(BTreeMap::<NodeId, clay_surface::Event>::new())));
    let received = use_hook(|| Rc::new(Cell::new(0_u64)));
    let command_session = session.as_ref().ok().cloned();
    let command_writer = writer.clone();
    let command_handles = handles.clone();
    use_future(move || {
        let session = command_session.clone();
        let writer = command_writer.clone();
        let handles = command_handles.clone();
        async move {
            let Some(session) = session else { return };
            let mut commands = writer
                .command_receiver
                .lock()
                .unwrap()
                .take()
                .expect("one command drain per surface writer");
            let dispatch = session.handler();
            while let Some((element, command)) = commands.recv().await {
                let mounted = handles
                    .get(element)
                    .expect("a command is only queued for an element that announced mounted");
                let request = match command {
                    ElementCommand::Read { request, .. } => request,
                    _ => 0,
                };
                let cmd_started = timing::now();
                let outcome = run_element_command(&mounted, command).await;
                timing::record("cmd", cmd_started, request as f64, 0.0);
                // A failed fire-and-forget command and every read answer the guest, so a
                // failure is never silent; a successful fire-and-forget command needs none.
                if request == 0 && outcome.is_ok() {
                    continue;
                }
                let data_json = match outcome {
                    Ok(value) => serde_json::json!({ "request": request, "value": value }),
                    Err(error) => serde_json::json!({ "request": request, "error": error }),
                }
                .to_string();
                dispatch(clay_surface::Event {
                    revision: writer.revision(),
                    element,
                    name: ELEMENT_REPLY.to_owned(),
                    bubbles: false,
                    data_json,
                });
                let mut rev = revision;
                rev.set(writer.revision());
            }
        }
    });

    // The handler and the component renderer are built once, so the incremental projection can
    // memoize on their identity. Built here, above the early returns, because it is a hook.
    let handler_session = session.as_ref().ok().cloned();
    let handler_writer = writer.clone();
    let handler_owner = owner.clone();
    let direct = writer.direct;
    use_effect(move || {
        if let Some(id) = direct {
            direct::attach(id);
        }
    });
    // X4 spike: the host's own cost of a guest batch, measured rather than derived. A render
    // begins when this body runs (the revision signal woke it) and its effects run once the
    // `VirtualDom` has diffed every dirty scope below it, so the span covers the surface
    // projection, the host diff, and the mutation writes made before the effect fires.
    use_hook(|| {
        if timing::ENABLED {
            clay_surface::probe::install(|label| timing::record(label, timing::now(), 0.0, 0.0));
        }
    });
    let render_started = use_hook(|| Rc::new(Cell::new(0.0_f64)));
    if timing::ENABLED {
        render_started.set(timing::now());
    }
    let render_started_for_effect = render_started.clone();
    use_effect(move || {
        let _ = revision();
        if timing::ENABLED {
            timing::record("host-render", render_started_for_effect.get(), 0.0, 0.0);
        }
    });
    let projection = use_hook(move || {
        let session = handler_session?;
        let dispatch = session.handler();
        let writer = handler_writer;
        if let Some(id) = writer.direct {
            // A direct receiver feeds DOM events straight to the guest: no retained tree to
            // check staleness against and no host re-render to schedule.
            let mounted = writer.mounted.clone();
            direct::bind(
                id,
                Rc::new(move |event| {
                    if event.name == "mounted" {
                        mounted.lock().unwrap().insert(event.element);
                    }
                    dispatch(event)
                }),
            );
            return None;
        }
        let writer_for_event = writer.clone();
        let deliver: SurfaceEventHandler = Rc::new(move |event| {
            if !writer_for_event.is_live() {
                return EventResponse::default();
            }
            if event.name == "mounted" {
                writer_for_event
                    .mounted
                    .lock()
                    .unwrap()
                    .insert(event.element);
            }
            // A listener carries its node's generation: the event is stale only if that node
            // was removed or its element id reassigned, not whenever any batch landed.
            if !writer_for_event
                .tree
                .lock()
                .unwrap()
                .event_is_current(event.element, event.revision)
            {
                timing::record(&format!("stale:{}", event.name), timing::now(), 0.0, 0.0);
                let mut rev = revision;
                rev.set(writer_for_event.revision());
                return EventResponse::default();
            }
            let started = timing::now();
            let kind = timing::ENABLED.then(|| format!("dispatch:{}", event.name));
            let response = dispatch(event);
            if let Some(kind) = kind {
                timing::record(&kind, started, 0.0, 0.0);
            }
            let mut rev = revision;
            rev.set(writer_for_event.revision());
            response
        });
        // A scroll event is a snapshot of where the element is, so the last one per animation
        // frame is the only one the guest needs; holding the rest until the frame spares it a
        // synchronous render per event.
        let handler: SurfaceEventHandler = {
            let deliver = deliver.clone();
            Rc::new(move |event| {
                if event.name != "scroll" || !frames::coalescing() {
                    return deliver(event);
                }
                received.set(received.get() + 1);
                let first = {
                    let mut held = held.borrow_mut();
                    let first = held.is_empty();
                    held.insert(event.element, event);
                    first
                };
                if first {
                    let (held, received, deliver) =
                        (held.clone(), received.clone(), deliver.clone());
                    frames::next_frame(move || {
                        let started = timing::now();
                        let events = std::mem::take(&mut *held.borrow_mut());
                        let delivered = events.len() as f64;
                        for (_, event) in events {
                            deliver(event);
                        }
                        timing::record("coalesce", started, received.replace(0) as f64, delivered);
                    });
                }
                EventResponse::default()
            })
        };
        let component: Rc<dyn Fn(&ComponentRef) -> Element> = Rc::new(move |reference| {
            rsx! {
                ComponentMount { owner: handler_owner.clone(), reference: *reference }
            }
        });
        Some(clay_surface::SurfaceProjection {
            tree: writer.tree.clone(),
            dispatch: handler,
            component,
            handles: handles.clone(),
        })
    });

    let _current_revision = revision();
    if !matches!(session, Ok(_)) || !writer.is_live() {
        return match session {
            Err(error) => rsx! { p { "Component unavailable: {error}" } },
            Ok(_) => rsx! { p { "Component withdrawn." } },
        };
    }
    if let Some(id) = direct {
        // The host owns only this container; the receiver fills it, so this component never
        // re-renders for the guest's batches.
        return rsx! { div { id: "{direct::container_id(id)}", style: "display:contents" } };
    }
    let projection = projection.expect("a live session has a projection");
    if frames::incremental() {
        let stamp = {
            let tree = writer.tree.lock().unwrap();
            let started = timing::now();
            let stamp = tree.root_stamp();
            timing::record(
                "render",
                started,
                tree.node_count() as f64,
                tree.revision() as f64,
            );
            stamp
        };
        return rsx! { clay_surface::SurfaceRoot::<ComponentRef> { projection, stamp } };
    }
    let tree = writer.tree.lock().unwrap();
    let started = timing::now();
    let projected = tree.render(
        &projection.dispatch,
        &*projection.component,
        &projection.handles,
    );
    timing::record(
        "render",
        started,
        tree.node_count() as f64,
        tree.revision() as f64,
    );
    projected
}

#[cfg(test)]
mod tests;
