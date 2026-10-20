//! Host-side Dioxus adapter: native renderer registry, component mounts, and the assembled
//! application runtime driver (ADR 015 §3).
pub mod application;
#[cfg(feature = "wasmtime")]
pub mod wasmtime;

use clay_host::{AccessError, ComponentRef, Host, Instance, Mount};
use clay_surface::{EventResponse, Mutation, PresentationError, SurfaceEventHandler, SurfaceTree};
use dioxus::prelude::*;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    tree: Arc<Mutex<SurfaceTree<ComponentRef>>>,
    ancestors: Vec<Arc<Mount>>,
    ancestor_surfaces: Vec<Arc<AtomicBool>>,
    revoked: Arc<AtomicBool>,
    revisions: watch::Sender<u64>,
}

impl SurfaceWriter {
    fn new(mount: Arc<Mount>) -> Self {
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
mod tests {
    use super::*;
    type SurfaceTree = clay_surface::SurfaceTree<ComponentRef>;
    type Mutation = clay_surface::Mutation<ComponentRef>;
    use clay_surface::AttributeValue;
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedMouseData};
    use std::cell::Cell;

    #[tokio::test]
    async fn wire_slots_mount_through_context_preserve_identity_and_revoke_nested_surfaces() {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let viewer = host.start("viewer");
        let layout = host.start("layout");
        let child = host.start("child");
        let mounts = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let child_writers = Rc::new(RefCell::new(Vec::new()));
        let reference = components
            .publish_surface(&child, {
                let mounts = mounts.clone();
                let drops = drops.clone();
                let writers = child_writers.clone();
                move |_, writer| {
                    mounts.set(mounts.get() + 1);
                    writer.apply(&counter_initial_batch())?;
                    writers.borrow_mut().push(writer.clone());
                    let drops = drops.clone();
                    Ok(SurfaceSession::with_drop(
                        |_| EventResponse::default(),
                        move || drops.set(drops.get() + 1),
                    ))
                }
            })
            .unwrap();
        let parent_writer = Rc::new(RefCell::new(None));
        let parent = components
            .publish_surface(&layout, {
                let parent_writer = parent_writer.clone();
                move |_, writer| {
                    writer.apply(&[
                        Mutation::MountComponent(reference),
                        Mutation::SetId(1),
                        Mutation::AppendChildren(1),
                    ])?;
                    *parent_writer.borrow_mut() = Some(writer);
                    Ok(SurfaceSession::new(|_| EventResponse::default()))
                }
            })
            .unwrap();
        fn app(props: (NativeComponents, Instance, ComponentRef)) -> Element {
            use_context_provider(|| props.0.clone());
            rsx! { ComponentMount { owner: props.1, reference: props.2 } }
        }
        let mut dom = VirtualDom::new_with_props(app, (components, viewer, parent));
        let mut tree = SurfaceTree::new();
        dom.rebuild(&mut tree);
        assert_eq!(tree.text_content(), "0");
        assert_eq!(mounts.get(), 1);
        let parent_writer = parent_writer.borrow().clone().unwrap();
        parent_writer.apply(&[]).unwrap();
        dom.wait_for_work().await;
        dom.render_immediate(&mut tree);
        assert_eq!((mounts.get(), drops.get()), (1, 0));
        parent_writer
            .apply(&[
                Mutation::PushId(1),
                Mutation::MountComponent(reference),
                Mutation::SetId(1),
                Mutation::ReplaceWith(1),
            ])
            .unwrap();
        dom.wait_for_work().await;
        dom.render_immediate(&mut tree);
        assert_eq!((mounts.get(), drops.get()), (2, 1));
        assert!(!child_writers.borrow()[0].is_live());
        assert!(child_writers.borrow()[1].is_live());
        parent_writer.withdraw();
        assert!(!child_writers.borrow()[1].is_live());
        assert_eq!(
            child_writers.borrow()[1].apply(&[]),
            Err(PresentationError::Revoked)
        );
        assert_eq!(host.check(&child), Ok(()));
        dom.wait_for_work().await;
        dom.render_immediate(&mut tree);
        assert_eq!(drops.get(), 2);
        drop(dom);
        host.close();
    }

    #[test]
    fn recursive_component_slots_are_rejected_without_recursing_into_the_factory() {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let owner = host.start("recursive layout");
        let self_reference = Rc::new(Cell::new(None));
        let mounts = Rc::new(Cell::new(0));
        let reference = components
            .publish_surface(&owner, {
                let self_reference = self_reference.clone();
                let mounts = mounts.clone();
                move |_, writer| {
                    mounts.set(mounts.get() + 1);
                    writer.apply(&[
                        Mutation::MountComponent(self_reference.get().unwrap()),
                        Mutation::AppendChildren(1),
                    ])?;
                    Ok(SurfaceSession::new(|_| EventResponse::default()))
                }
            })
            .unwrap();
        self_reference.set(Some(reference));
        fn app(props: (NativeComponents, Instance, ComponentRef)) -> Element {
            rsx! { ComponentMount { components: props.0, owner: props.1, reference: props.2 } }
        }
        let mut dom = VirtualDom::new_with_props(app, (components, owner, reference));
        let mut tree = SurfaceTree::new();
        dom.rebuild(&mut tree);
        assert_eq!(mounts.get(), 1);
        assert_eq!(
            tree.text_content(),
            "Component unavailable: cyclic or excessively deep mount."
        );
        host.close();
    }

    fn counter_initial_batch() -> Vec<Mutation> {
        vec![
            Mutation::PushId(0),
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetAttribute(("class".into(), None, AttributeValue::Text("btn".into()))),
            Mutation::CreateText("0".into()),
            Mutation::AppendChildren(1),
            Mutation::SetId(1),
            Mutation::AddEventListener("click".into()),
            Mutation::PushId(1),
            Mutation::Child(0),
            Mutation::SetId(2),
            Mutation::Pop,
            Mutation::AppendChildren(1),
            Mutation::Pop,
        ]
    }

    fn counter_text_update_batch(value: &str) -> Vec<Mutation> {
        vec![
            Mutation::PushId(2),
            Mutation::SetText(value.into()),
            Mutation::Pop,
        ]
    }

    fn mounted_writer() -> (Host, Instance, Arc<Mount>, SurfaceWriter) {
        let host = Host::new();
        let owner = host.start("surface owner");
        let reference = host.publish_component(&owner).unwrap();
        let mount = Arc::new(host.mount(&owner, reference).unwrap());
        let writer = SurfaceWriter::new(mount.clone());
        (host, owner, mount, writer)
    }

    #[test]
    fn withdraw_revokes_all_clones_without_closing_the_mount_or_sibling_surface() {
        let (_host, _owner, mount, writer) = mounted_writer();
        let clone = writer.clone();
        let sibling = SurfaceWriter::new(mount.clone());
        writer.apply(&counter_initial_batch()).unwrap();
        sibling.apply(&counter_initial_batch()).unwrap();
        let updates = writer.revisions.subscribe();
        writer.withdraw();
        assert!(mount.is_live());
        assert!(!clone.is_live());
        assert_eq!(
            clone.apply(&counter_initial_batch()),
            Err(PresentationError::Revoked)
        );
        assert!(writer.tree.lock().unwrap().is_empty());
        assert_eq!(*updates.borrow(), writer.revision());
        let revision = writer.revision();
        clone.withdraw();
        assert_eq!(writer.revision(), revision);
        sibling
            .apply(&counter_text_update_batch("sibling"))
            .unwrap();
        assert_eq!(sibling.tree.lock().unwrap().text_content(), "sibling");
    }

    #[test]
    fn withdrawal_before_first_apply_permanently_revokes_escaped_clones() {
        let (_host, _owner, mount, writer) = mounted_writer();
        let clone = writer.clone();
        let mut updates = writer.revisions.subscribe();
        writer.withdraw();
        assert!(mount.is_live());
        assert!(!writer.is_live());
        assert!(!clone.is_live());
        assert_eq!(clone.apply(&[]), Err(PresentationError::Revoked));
        assert_eq!(
            clone.apply(&counter_initial_batch()),
            Err(PresentationError::Revoked)
        );
        assert_eq!(*updates.borrow_and_update(), writer.revision());
        let revision = writer.revision();
        clone.withdraw();
        assert_eq!(writer.revision(), revision);
        assert!(!updates.has_changed().unwrap());
        assert!(!writer.tree.is_poisoned());
    }

    #[test]
    fn invalid_batch_publishes_only_terminal_clear_and_preserves_sibling_revision() {
        let (_host, _owner, mount, writer) = mounted_writer();
        let sibling = SurfaceWriter::new(mount.clone());
        writer.apply(&counter_initial_batch()).unwrap();
        sibling.apply(&counter_initial_batch()).unwrap();
        let updates = writer.revisions.subscribe();
        let mut sibling_updates = sibling.revisions.subscribe();
        let sibling_revision = *sibling_updates.borrow_and_update();
        let revision = writer.revision();
        let mut edits = counter_text_update_batch("must not be published");
        edits.push(Mutation::Pop);
        let error = writer.apply(&edits).unwrap_err();
        assert!(matches!(error, PresentationError::InvalidBatch(_)));
        assert_eq!(*updates.borrow(), revision + 1);
        assert_eq!(writer.revision(), revision + 1);
        let tree = writer.tree.lock().unwrap();
        assert!(tree.is_empty());
        assert_eq!(tree.error(), Some(&error));
        drop(tree);
        assert!(mount.is_live());
        assert!(!writer.tree.is_poisoned());
        assert_eq!(sibling.revision(), sibling_revision);
        assert!(!sibling_updates.has_changed().unwrap());
        assert_eq!(sibling.tree.lock().unwrap().text_content(), "0");
        sibling
            .apply(&counter_text_update_batch("still live"))
            .unwrap();
        assert_eq!(sibling.tree.lock().unwrap().text_content(), "still live");
    }

    #[test]
    fn malformed_guest_batches_do_not_panic_or_poison_the_shared_surface_mutex() {
        for edits in [
            vec![Mutation::Pop],
            vec![Mutation::PushId(u32::MAX)],
            vec![Mutation::PushId(1), Mutation::Child(u32::MAX)],
            vec![
                Mutation::PushId(1),
                Mutation::PushId(1),
                Mutation::AppendChildren(1),
            ],
            vec![
                Mutation::PushId(1),
                Mutation::AddEventListener("unsupported".into()),
            ],
            vec![Mutation::CreateText(
                "x".repeat(SurfaceTree::MAX_STRING_BYTES + 1),
            )],
        ] {
            let (_host, _owner, mount, writer) = mounted_writer();
            let clone = writer.clone();
            let sibling = SurfaceWriter::new(mount.clone());
            writer.apply(&counter_initial_batch()).unwrap();
            sibling.apply(&counter_initial_batch()).unwrap();
            let updates = writer.revisions.subscribe();
            let error = writer.apply(&edits).unwrap_err();
            assert!(matches!(
                error,
                PresentationError::InvalidBatch(_) | PresentationError::Unsupported(_)
            ));
            assert!(!writer.tree.is_poisoned());
            let tree = writer.tree.lock().unwrap();
            assert_eq!(tree.error(), Some(&error));
            assert!(tree.is_empty());
            assert_eq!(*updates.borrow(), tree.revision());
            drop(tree);
            assert!(mount.is_live());
            assert!(!clone.is_live());
            assert_eq!(
                clone.apply(&counter_initial_batch()),
                Err(PresentationError::Revoked)
            );
            clone.withdraw();
            sibling.apply(&counter_text_update_batch("alive")).unwrap();
            assert_eq!(sibling.tree.lock().unwrap().text_content(), "alive");
        }
    }

    #[test]
    fn concurrent_apply_and_withdraw_cannot_resurrect_or_reorder_surface_publication() {
        for _ in 0..16 {
            let (_host, _owner, mount, writer) = mounted_writer();
            writer.apply(&counter_initial_batch()).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let apply_writer = writer.clone();
            let apply_barrier = barrier.clone();
            let apply = std::thread::spawn(move || {
                apply_barrier.wait();
                apply_writer.apply(&counter_text_update_batch("racing"))
            });
            let withdraw_writer = writer.clone();
            let withdraw_barrier = barrier.clone();
            let withdraw = std::thread::spawn(move || {
                withdraw_barrier.wait();
                withdraw_writer.withdraw();
            });
            barrier.wait();
            assert!(matches!(
                apply.join().unwrap(),
                Ok(_) | Err(PresentationError::Revoked)
            ));
            withdraw.join().unwrap();
            assert!(mount.is_live());
            assert!(!writer.is_live());
            assert_eq!(
                writer.apply(&counter_initial_batch()),
                Err(PresentationError::Revoked)
            );
            assert!(writer.tree.lock().unwrap().is_empty());
            assert_eq!(*writer.revisions.borrow(), writer.revision());
        }
    }

    #[test]
    fn failed_factory_reports_the_error_and_revokes_escaped_writers() {
        let (_host, owner, mount, _writer) = mounted_writer();
        let slot = Rc::new(RefCell::new(None));
        let capture = slot.clone();
        let factory = SurfaceFactory(Rc::new(move |_mount, writer| {
            writer.apply(&counter_initial_batch())?;
            *capture.borrow_mut() = Some(writer);
            Err(PresentationError::Unsupported("factory failed".into()))
        }));
        #[derive(Clone, PartialEq)]
        struct Props {
            mount: SharedMount,
            owner: Instance,
            factory: SurfaceFactory,
        }
        fn app(props: Props) -> Element {
            rsx! { MountedSurface { mount: props.mount, owner: props.owner, factory: props.factory } }
        }
        let mut dom = VirtualDom::new_with_props(
            app,
            Props {
                mount: SharedMount(mount.clone()),
                owner,
                factory,
            },
        );
        let mut tree = SurfaceTree::new();
        dom.rebuild(&mut tree);
        assert!(tree.text_content().contains("factory failed"));
        let writer = slot.borrow().clone().unwrap();
        assert!(mount.is_live());
        assert!(!writer.is_live());
        assert_eq!(
            writer.apply(&counter_initial_batch()),
            Err(PresentationError::Revoked)
        );
        assert!(writer.tree.lock().unwrap().is_empty());
    }

    #[test]
    fn stale_host_listener_is_suppressed_after_external_apply_then_current_listener_dispatches() {
        let (_host, owner, mount, _writer) = mounted_writer();
        let slot = Rc::new(RefCell::new(None));
        let capture = slot.clone();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_capture = received.clone();
        let factory = SurfaceFactory(Rc::new(move |_mount, writer| {
            writer.apply(&counter_initial_batch())?;
            *capture.borrow_mut() = Some(writer.clone());
            let received = received_capture.clone();
            Ok(SurfaceSession::new(move |event| {
                assert_eq!(event.name, "click");
                received.borrow_mut().push((event.revision, event.element));
                writer
                    .apply(&counter_text_update_batch(
                        &received.borrow().len().to_string(),
                    ))
                    .unwrap();
                EventResponse::default()
            }))
        }));
        #[derive(Clone, PartialEq)]
        struct Props {
            mount: SharedMount,
            owner: Instance,
            factory: SurfaceFactory,
        }
        fn app(props: Props) -> Element {
            rsx! { MountedSurface { mount: props.mount, owner: props.owner, factory: props.factory } }
        }
        fn click(dom: &VirtualDom, id: dioxus_core::ElementId) {
            dom.runtime().handle_event_for_target(
                RenderTargetId::ROOT,
                "click",
                dioxus_core::Event::new(
                    EventData::Mouse(SerializedMouseData::default()).into_any(),
                    true,
                ),
                id,
            );
        }
        let mut dom = VirtualDom::new_with_props(
            app,
            Props {
                mount: SharedMount(mount),
                owner,
                factory,
            },
        );
        let mut host_tree = SurfaceTree::new();
        dom.rebuild(&mut host_tree);
        let stale_host_id = host_tree.listener_id("button", "click").unwrap();
        let writer = slot.borrow().clone().unwrap();
        let previous_revision = writer.revision();
        let mut replacement = vec![Mutation::PushId(1), Mutation::Remove];
        replacement.extend(counter_initial_batch());
        replacement.extend(counter_text_update_batch("replacement"));
        let current_revision = writer.apply(&replacement).unwrap();
        assert_eq!(current_revision, previous_revision + 1);
        assert_eq!(host_tree.text_content(), "0");
        click(&dom, stale_host_id);
        assert!(received.borrow().is_empty());
        assert_eq!(writer.revision(), current_revision);
        assert_eq!(writer.tree.lock().unwrap().text_content(), "replacement");
        dom.render_immediate(&mut host_tree);
        assert_eq!(host_tree.text_content(), "replacement");
        let current_host_id = host_tree.listener_id("button", "click").unwrap();
        click(&dom, current_host_id);
        assert_eq!(&*received.borrow(), &[(current_revision, 1)]);
        dom.render_immediate(&mut host_tree);
        assert_eq!(host_tree.text_content(), "1");
        let current_host_id = host_tree.listener_id("button", "click").unwrap();
        click(&dom, current_host_id);
        assert_eq!(
            &*received.borrow(),
            &[(current_revision, 1), (current_revision + 1, 1)]
        );
        dom.render_immediate(&mut host_tree);
        assert_eq!(host_tree.text_content(), "2");
    }

    #[test]
    fn surface_mount_renders_in_host_vdom_dispatches_reentrantly_and_revokes_on_instance_close() {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let viewer = host.start("viewer");
        let micro = host.start("micro");

        let dropped = Rc::new(Cell::new(false));
        let dropped_flag = dropped.clone();
        let writer_slot: Rc<RefCell<Option<SurfaceWriter>>> = Rc::default();
        let writer_slot_capture = writer_slot.clone();

        let reference = components
            .publish_surface(&micro, move |_mount, writer| {
                writer.apply(&counter_initial_batch())?;
                *writer_slot_capture.borrow_mut() = Some(writer.clone());
                let clicks = Cell::new(0_u64);
                let flag = dropped_flag.clone();
                Ok(SurfaceSession::with_drop(
                    move |event| {
                        assert_eq!(event.name, "click");
                        assert_eq!(event.element, 1);
                        let next = clicks.get() + 1;
                        clicks.set(next);
                        // Re-entrant apply inside event dispatch must not conflict with SurfaceTree borrow.
                        writer
                            .apply(&counter_text_update_batch(&next.to_string()))
                            .unwrap();
                        EventResponse::default()
                    },
                    move || flag.set(true),
                ))
            })
            .unwrap();

        #[derive(Clone, PartialEq)]
        struct HostProps {
            components: NativeComponents,
            viewer: Instance,
            reference: ComponentRef,
        }

        fn host_app(props: HostProps) -> Element {
            rsx! {
                section {
                    ComponentMount {
                        components: props.components,
                        owner: props.viewer,
                        reference: props.reference,
                    }
                }
            }
        }

        let mut host_dom = VirtualDom::new_with_props(
            host_app,
            HostProps {
                components,
                viewer,
                reference,
            },
        );
        let mut host_tree = SurfaceTree::new();
        host_dom.rebuild(&mut host_tree);
        assert_eq!(host_tree.text_content(), "0");

        // Find the host VirtualDom's ElementId for the mounted `<button>` (carrying the click listener,
        // not the detached template prototype).
        let host_button_id = host_tree
            .listener_id("button", "click")
            .expect("host tree rendered the surface button");

        let click = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        host_dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            click,
            host_button_id,
        );
        host_dom.render_immediate(&mut host_tree);
        assert_eq!(host_tree.text_content(), "1");

        // Closing the micro's Instance revokes the surface writer immediately.
        host.close_instance(&micro).unwrap();
        let writer = writer_slot.borrow().clone().unwrap();
        assert_eq!(
            writer.apply(&counter_text_update_batch("2")),
            Err(PresentationError::Revoked)
        );
        drop(host_dom);
        assert!(dropped.get());
    }
}
