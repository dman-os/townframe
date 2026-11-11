// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/Virtualizer.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the class is `Virtualizer<Host>` and every DOM read, write and
// observer registration goes through the `VirtualizerHost` trait (geometry,
// scroll writes, observers and listeners from the platform; the structural
// reads (first child, the anchor line queries, which search the file element's shadow root when
// it has one) are the port's `Document`'s); the control flow,
// dirty flags, anchor selection and scroll fixing are upstream's, statement for
// statement. Everything a DOM does to the class from outside becomes a public
// method the host calls: the `handleElementScroll`, `handleWindowScroll`,
// `handleWindowResize`, `handleContainerResize` and `handleIntersectionChange`
// arrow functions are `handle_*` methods (upstream registers them itself as
// listeners and observer callbacks; here `setup` asks the host to).
// Upstream mutates the instance through `this` from re-entrant callbacks
// (`onRender` calls back `instanceChanged`, `requestHeightReconcile`,
// `connect`), so the state is `Cell`/`RefCell` fields behind `&self` and no
// borrow is held across an instance callback; `Map`/`Set` iteration that
// tolerates mutation uses `InsertionMap` cursors (JavaScript's live iterator).
// The class is built as an `Rc` with a weak self reference because
// `computeRenderRangeAndEmit` is a bound arrow function whose identity keys the
// render queue. `queueRender`/`dequeueRender` are methods of the injected
// `UniversalRenderingManager` rather than module functions. Instances are
// `Rc<dyn SubscribedInstance>` compared by allocation, as JavaScript compares
// the objects. `connect` panics when the container is already connected (it
// throws upstream). The `instanceof` checks (`target instanceof HTMLElement`,
// `contentContainer instanceof HTMLElement`, `line instanceof HTMLElement`) are
// the host's: the host hands over elements only, and `contentContainer ??=
// root.firstElementChild` is `first_element_child`. The shadow-root query for
// anchor lines is lazy (`anchor_lines`) so the walk stops at the first line
// below the viewport top, as the `break` does upstream. Not ported: the
// development hooks (`Virtualizer.__STOP`, `__lastScrollPosition`,
// `window.__INSTANCE`, `window.__TOGGLE`; upstream marks them "Remove me before
// release"). The element branch of `scrollFix`'s write is the host method
// `element_scroll_by` (default: upstream's `scrollTo(scrollTop + offset)`), the
// one divergence of the class's control flow: a host whose `scrollTop` is only
// readable asynchronously reads and scrolls in one task, so the offset is applied
// to the live position rather than to a cached one (parity/divergences.psv). The module-level `lastSize` is a thread-local. `ScrollBehavior`
// and the `IntersectionObserver` options are plain values.

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::document::Document;
use crate::document::NodeKey;
use crate::js_collections::InsertionMap;
use crate::managers::universal_rendering_manager::AnimationFrames;
use crate::managers::universal_rendering_manager::RenderCallback;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::layout::VirtualWindowSpecs;
use crate::utils::create_window_from_scroll_position::WindowFromScrollPositionProps;
use crate::utils::create_window_from_scroll_position::create_window_from_scroll_position;
use crate::utils::predicates::are_virtual_window_specs_equal::are_virtual_window_specs_equal;

/// `SubscribedInstance`: a file or diff the virtualizer drives.
pub trait SubscribedInstance {
    fn on_render(&self, dirty: bool) -> bool;
    fn reconcile_heights(&self) -> bool;
    fn set_visibility(&self, visible: bool);
}

pub type InstanceRef = Rc<dyn SubscribedInstance>;

fn instance_key(instance: &InstanceRef) -> usize {
    Rc::as_ptr(instance).cast::<()>() as usize
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileTypeOffset {
    Top,
    Bottom,
}

#[derive(Debug, Clone, PartialEq)]
struct ScrollAnchor<Element> {
    file_element: Element,
    file_type_offset: FileTypeOffset,
    file_offset: f64,
    line_index: Option<String>,
    line_offset: Option<f64>,
}

// 800 seems like the healthy overscan required to
// keep safari from blanking... if we catch it tho, maybe 900
pub const DEFAULT_OVERSCROLL_SIZE: f64 = 1000.0;
pub const INTERSECTION_OBSERVER_MARGIN: f64 = DEFAULT_OVERSCROLL_SIZE * 4.0;
pub const INTERSECTION_OBSERVER_THRESHOLD: [f64; 4] = [0.0, 0.000001, 0.99999, 1.0];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VirtualizerConfig {
    /// Extra pixels rendered above and below the viewport to reduce blanking during fast scrolls.
    pub overscroll_size: f64,
    /// Margin used by IntersectionObserver to decide when items should be considered visible.
    pub intersection_observer_margin: f64,
    /// Enables noisy resize logs to help tune metrics and investigate scroll jitter.
    pub resize_debugging: bool,
}

/// `DEFAULT_VIRTUALIZER_CONFIG`; build a partial config with struct update
/// syntax (`VirtualizerConfig { overscroll_size: 200.0, ..Default::default() }`).
impl Default for VirtualizerConfig {
    fn default() -> Self {
        Self {
            overscroll_size: DEFAULT_OVERSCROLL_SIZE,
            intersection_observer_margin: INTERSECTION_OBSERVER_MARGIN,
            resize_debugging: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScrollBehavior {
    #[default]
    Auto,
    Instant,
    Smooth,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VirtualizerScrollOptions {
    pub top: f64,
    pub behavior: ScrollBehavior,
}

/// `HTMLElement | Document`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Root<Element> {
    Document,
    Element(Element),
}

/// The options `setup` hands the `IntersectionObserver` constructor; the
/// `rootMargin` is `{root_margin}px 0px {root_margin}px 0px`.
#[derive(Debug, Clone, PartialEq)]
pub struct IntersectionObserverInit<Element> {
    pub root: Root<Element>,
    pub threshold: [f64; 4],
    pub root_margin: f64,
}

/// A `ResizeObserverEntry`: the observed target and its
/// `borderBoxSize[0].blockSize`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResizeEntry<Element> {
    pub target: Element,
    pub block_size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntersectionEntry<Element> {
    pub target: Element,
    pub is_intersecting: bool,
}

/// A line the anchor search walks: its `dataset.lineIndex` and element.
#[derive(Debug, Clone, PartialEq)]
pub struct AnchorLine<Element> {
    pub line_index: String,
    pub element: Element,
}

/// Everything the class asks of the DOM. All methods are reads or
/// fire-and-forget registrations; events and observer deliveries come back as
/// the `Virtualizer::handle_*` methods.
pub trait VirtualizerHost: AnimationFrames + Sized + 'static {
    /// The rendered document: structure (children, attributes, the anchor
    /// line query) is read from it; the platform supplies the rest.
    fn document(&self) -> &Document;

    // `window.scrollY`, `globalThis.innerHeight`,
    // `document.documentElement.scrollHeight`.
    fn window_scroll_y(&self) -> f64;
    fn window_inner_height(&self) -> f64;
    fn document_scroll_height(&self) -> f64;
    // `element.scrollTop`, `.scrollHeight`, `.getBoundingClientRect().height`
    // / `.top`, `.offsetHeight`, `.firstElementChild`.
    fn element_scroll_top(&self, element: &NodeKey) -> f64;
    fn element_scroll_height(&self, element: &NodeKey) -> f64;
    fn element_bounding_height(&self, element: &NodeKey) -> f64;
    fn element_bounding_top(&self, element: &NodeKey) -> f64;
    fn element_offset_height(&self, element: &NodeKey) -> f64;
    // `window.scrollTo` / `element.scrollTo` with `{ top, behavior }`.
    fn window_scroll_to(&self, top: f64, behavior: ScrollBehavior);
    fn element_scroll_to(&self, element: &NodeKey, top: f64, behavior: ScrollBehavior);
    /// `scrollFix`'s write: `element.scrollTo({ top: element.scrollTop + scroll_offset,
    /// behavior: 'instant' })`. A host that cannot read `scrollTop` synchronously overrides it to
    /// read and write as one step; this default is upstream's read-then-write.
    fn element_scroll_by(&self, element: &NodeKey, scroll_offset: f64) {
        self.element_scroll_to(
            element,
            self.element_scroll_top(element) + scroll_offset,
            ScrollBehavior::Instant,
        );
    }
    // Observer and listener registration (`new ResizeObserver`,
    // `new IntersectionObserver`, `addEventListener`, ...).
    fn create_observers(&self, intersection: IntersectionObserverInit<NodeKey>);
    fn disconnect_observers(&self);
    fn observe_resize(&self, element: &NodeKey);
    /// `resizeObserver.observe(document.documentElement)`.
    fn observe_document_resize(&self);
    fn observe_intersection(&self, element: &NodeKey);
    fn unobserve_intersection(&self, element: &NodeKey);
    /// `window.addEventListener('scroll' | 'resize', ..., { passive: true })`.
    fn add_window_listeners(&self);
    /// `root.addEventListener('scroll', ..., { passive: true })`.
    fn add_element_scroll_listener(&self, root: &NodeKey);
    /// Removes the element scroll listener (if any) and the two window ones.
    fn remove_listeners(&self, root: Option<&Root<NodeKey>>);
    /// `console.log(message, this.__id, { sizeChange, newSize })` of the
    /// `resizeDebugging` branches.
    fn console_log(&self, message: &str, virtualizer_id: &str, size_change: f64, new_size: f64);
}

static INSTANCE_COUNTER: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    // Module-level `let lastSize = 0` upstream: shared by every virtualizer and
    // only read by the `resizeDebugging` logs.
    static LAST_SIZE: Cell<f64> = const { Cell::new(0.0) };
}

pub struct Virtualizer<Host: VirtualizerHost> {
    pub __id: String,
    pub config: VirtualizerConfig,
    host: Rc<Host>,
    render_queue: UniversalRenderingManager<Host>,
    compute_render_range_and_emit: RenderCallback,
    observing: Cell<bool>,
    scroll_top: Cell<f64>,
    height: Cell<f64>,
    scroll_height: Cell<f64>,
    window_specs: Cell<VirtualWindowSpecs>,
    root: RefCell<Option<Root<NodeKey>>>,
    content_container: RefCell<Option<NodeKey>>,
    observers: RefCell<InsertionMap<NodeKey, InstanceRef>>,
    visible_instances: RefCell<InsertionMap<NodeKey, InstanceRef>>,
    visible_instances_dirty: Cell<bool>,
    instances_changed: RefCell<InsertionMap<usize, InstanceRef>>,
    // Instances whose content was (re)painted outside a virtualizer-driven
    // onRender — host/React render calls, async highlight completions — and
    // still need a measured-height reconciliation pass. See
    // requestHeightReconcile.
    reconcile_queue: RefCell<InsertionMap<usize, InstanceRef>>,
    scroll_dirty: Cell<bool>,
    height_dirty: Cell<bool>,
    scroll_height_dirty: Cell<bool>,
    rendered_observers: Cell<usize>,
    connect_queue: RefCell<InsertionMap<NodeKey, InstanceRef>>,
}

impl<Host: VirtualizerHost> Virtualizer<Host> {
    pub fn new(
        host: Rc<Host>,
        render_queue: UniversalRenderingManager<Host>,
        config: VirtualizerConfig,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak: &Weak<Self>| {
            let weak = Weak::clone(weak);
            let compute_render_range_and_emit: RenderCallback = Rc::new(move |_time| {
                if let Some(virtualizer) = weak.upgrade() {
                    virtualizer.compute_render_range_and_emit();
                }
            });
            Self {
                __id: format!(
                    "virtualizer-{}",
                    INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed)
                ),
                config,
                host,
                render_queue,
                compute_render_range_and_emit,
                observing: Cell::new(false),
                scroll_top: Cell::new(0.0),
                height: Cell::new(0.0),
                scroll_height: Cell::new(0.0),
                window_specs: Cell::new(VirtualWindowSpecs {
                    top: 0.0,
                    bottom: 0.0,
                }),
                root: RefCell::new(None),
                content_container: RefCell::new(None),
                observers: RefCell::default(),
                visible_instances: RefCell::default(),
                visible_instances_dirty: Cell::new(false),
                instances_changed: RefCell::default(),
                reconcile_queue: RefCell::default(),
                scroll_dirty: Cell::new(true),
                height_dirty: Cell::new(true),
                scroll_height_dirty: Cell::new(true),
                rendered_observers: Cell::new(0),
                connect_queue: RefCell::default(),
            }
        })
    }

    fn queue_render(&self) {
        self.render_queue
            .queue_render(&self.compute_render_range_and_emit);
    }

    pub fn setup(&self, root: Root<NodeKey>, content_container: Option<NodeKey>) {
        if self.root.borrow().is_some() {
            return;
        }
        *self.root.borrow_mut() = Some(root.clone());
        self.host.create_observers(IntersectionObserverInit {
            root: root.clone(),
            threshold: INTERSECTION_OBSERVER_THRESHOLD,
            root_margin: self.config.intersection_observer_margin,
            // FIXME(amadeus): Figure out the other settings we'll want in here, or
            // if we should make them configurable...
        });
        self.observing.set(true);
        match &root {
            Root::Document => self.setup_window(),
            Root::Element(_) => self.setup_element(content_container),
        }

        let queued = self.connect_queue.borrow().entries();
        for (container, instance) in queued {
            // Upstream calls `connect` and drops the returned disposer.
            self.connect_instance(container, instance);
        }
        self.connect_queue.borrow_mut().clear();
        self.mark_dom_dirty();
        self.queue_render();
    }

    pub fn instance_changed(&self, instance: &InstanceRef, dom_dirty: bool) {
        self.instances_changed
            .borrow_mut()
            .insert(instance_key(instance), Rc::clone(instance));
        if dom_dirty {
            self.mark_dom_dirty();
        }
        self.queue_render();
    }

    // The render loop only reconciles heights for instances it repainted
    // itself, but content can also land outside a virtualizer pass (a
    // host/React-driven render call, an async highlight completion). Those
    // renders queue themselves here so their measured line deltas — wrapped
    // lines, annotation heights — are re-captured; otherwise a layout reset
    // (e.g. an edit-session exit recompute) leaves the instance stuck on its
    // baseline estimate and its placeholder renders the wrong height.
    pub fn request_height_reconcile(&self, instance: &InstanceRef) {
        self.reconcile_queue
            .borrow_mut()
            .insert(instance_key(instance), Rc::clone(instance));
        self.queue_render();
    }

    pub fn get_window_specs(&self) -> VirtualWindowSpecs {
        let window_specs = self.window_specs.get();
        if window_specs.top == 0.0 && window_specs.bottom == 0.0 {
            self.window_specs.set(self.window_from_scroll_position());
        }
        self.window_specs.get()
    }

    pub fn get_root(&self) -> Option<Root<NodeKey>> {
        self.root.borrow().clone()
    }

    pub fn is_instance_visible(&self, element_top: f64, element_height: f64) -> bool {
        let scroll_top = self.get_scroll_top();
        let height = self.get_height();
        let margin = self.config.intersection_observer_margin;
        let top = scroll_top - margin;
        let bottom = scroll_top + height + margin;
        !(element_top < top - element_height || element_top > bottom)
    }

    /// `handleContainerResize`: the `ResizeObserver` callback.
    pub fn handle_container_resize(&self, entries: &[ResizeEntry<NodeKey>]) {
        let Some(root) = self.get_root() else {
            return;
        };
        let mut should_queue_update = false;
        for entry in entries {
            let block_size = entry.block_size;
            match &root {
                Root::Document => {
                    if block_size != self.scroll_height.get() {
                        self.scroll_height_dirty.set(true);
                        should_queue_update = true;
                        if self.config.resize_debugging {
                            self.host.console_log(
                                "Virtualizer: content size change",
                                &self.__id,
                                block_size - LAST_SIZE.get(),
                                block_size,
                            );
                            LAST_SIZE.set(block_size);
                        }
                    }
                }
                Root::Element(root_element) => {
                    if &entry.target == root_element {
                        if block_size != self.height.get() {
                            self.height_dirty.set(true);
                            should_queue_update = true;
                        }
                    } else if self.content_container.borrow().as_ref() == Some(&entry.target) {
                        self.scroll_height_dirty.set(true);
                        should_queue_update = true;
                        if self.config.resize_debugging {
                            self.host.console_log(
                                "Virtualizer: scroller size change",
                                &self.__id,
                                block_size - LAST_SIZE.get(),
                                block_size,
                            );
                            LAST_SIZE.set(block_size);
                        }
                    }
                }
            }
        }

        if should_queue_update {
            self.queue_render();
        }
    }

    fn setup_window(&self) {
        assert!(
            matches!(*self.root.borrow(), Some(Root::Document)),
            "Virtualizer.setupWindow: Invalid setup method"
        );
        self.host.add_window_listeners();
        self.host.observe_document_resize();
    }

    fn setup_element(&self, content_container: Option<NodeKey>) {
        let root = match &*self.root.borrow() {
            Some(Root::Element(root)) => *root,
            _ => panic!("Virtualizer.setupElement: Invalid setup method"),
        };
        self.host.add_element_scroll_listener(&root);
        self.host.observe_resize(&root);
        // `contentContainer ??= root.firstElementChild ?? undefined`
        let content_container =
            content_container.or_else(|| self.host.document().children(root).first().copied());
        if let Some(content_container) = content_container {
            *self.content_container.borrow_mut() = Some(content_container);
            self.host.observe_resize(&content_container);
        }
    }

    pub fn clean_up(&self) {
        self.render_queue
            .dequeue_render(&self.compute_render_range_and_emit);
        // `this.resizeObserver?.disconnect()` and the intersection one.
        if self.observing.replace(false) {
            self.host.disconnect_observers();
        }
        let root = self.root.borrow_mut().take();
        self.host.remove_listeners(root.as_ref());
        *self.content_container.borrow_mut() = None;
        self.observers.borrow_mut().clear();
        self.visible_instances.borrow_mut().clear();
        self.instances_changed.borrow_mut().clear();
        self.reconcile_queue.borrow_mut().clear();
        self.connect_queue.borrow_mut().clear();
        self.visible_instances_dirty.set(false);
        self.window_specs.set(VirtualWindowSpecs {
            top: 0.0,
            bottom: 0.0,
        });
        self.scroll_top.set(0.0);
        self.height.set(0.0);
        self.scroll_height.set(0.0);
        self.scroll_dirty.set(true);
        self.height_dirty.set(true);
        self.scroll_height_dirty.set(true);
    }

    pub fn get_offset_in_scroll_container(&self, element: &NodeKey) -> f64 {
        self.get_scroll_top()
            + self.get_relative_bounding_top(element, self.get_scroll_container_element().as_ref())
    }

    /// Returns the disposer upstream returns (`() => this.disconnect(container)`).
    pub fn connect(self: &Rc<Self>, container: NodeKey, instance: InstanceRef) -> Box<dyn Fn()> {
        self.connect_instance(container, instance);
        let virtualizer = Rc::downgrade(self);
        Box::new(move || {
            if let Some(virtualizer) = virtualizer.upgrade() {
                virtualizer.disconnect(&container);
            }
        })
    }

    // The body of `connect` without the disposer: `setup` replays the connect
    // queue through it and has no `Rc` to hand out a disposer from.
    fn connect_instance(&self, container: NodeKey, instance: InstanceRef) {
        assert!(
            !self.observers.borrow().contains_key(&container),
            "Virtualizer.connect: instance is already connected..."
        );
        // If we are racing against the intersectionObserver, then we should just
        // queue up the connection for when the observer does get set up
        if !self.observing.get() {
            self.connect_queue.borrow_mut().insert(container, instance);
        } else {
            // FIXME(amadeus): Go through the connection phase a bit more closely...
            self.host.observe_intersection(&container);
            self.observers
                .borrow_mut()
                .insert(container, Rc::clone(&instance));
            self.instances_changed
                .borrow_mut()
                .insert(instance_key(&instance), instance);
            self.mark_dom_dirty();
            self.queue_render();
        }
    }

    pub fn disconnect(&self, container: &NodeKey) {
        let instance = self.observers.borrow().get(container).cloned();
        self.connect_queue.borrow_mut().remove(container);
        let Some(instance) = instance else {
            return;
        };
        if self.observing.get() {
            self.host.unobserve_intersection(container);
        }
        self.observers.borrow_mut().remove(container);
        self.instances_changed
            .borrow_mut()
            .remove(&instance_key(&instance));
        self.reconcile_queue
            .borrow_mut()
            .remove(&instance_key(&instance));
        if self.visible_instances.borrow_mut().remove(container) {
            self.visible_instances_dirty.set(true);
        }
        self.mark_dom_dirty();
        self.queue_render();
    }

    pub fn handle_window_resize(&self) {
        if self.host.window_inner_height() == self.height.get() {
            return;
        }
        self.height_dirty.set(true);
        self.queue_render();
    }

    pub fn handle_window_scroll(&self) {
        if !matches!(*self.root.borrow(), Some(Root::Document)) {
            return;
        }
        self.scroll_dirty.set(true);
        self.queue_render();
    }

    pub fn handle_element_scroll(&self) {
        if !matches!(*self.root.borrow(), Some(Root::Element(_))) {
            return;
        }
        self.scroll_dirty.set(true);
        self.queue_render();
    }

    fn window_from_scroll_position(&self) -> VirtualWindowSpecs {
        create_window_from_scroll_position(WindowFromScrollPositionProps::new(
            self.get_scroll_top(),
            self.get_height(),
            self.get_scroll_height(),
            self.config.overscroll_size,
        ))
    }

    /// `computeRenderRangeAndEmit`, the frame callback.
    fn compute_render_range_and_emit(&self) {
        // Tombstones of the maps the pass is about to walk.
        self.observers.borrow_mut().compact();
        self.visible_instances.borrow_mut().compact();
        let wrapper_dirty = self.height_dirty.get() || self.scroll_height_dirty.get();
        let mut instances_have_changed = !self.instances_changed.borrow().is_empty();

        // If we got an emitted update from a bunch of instances, we should skip
        // the window check first and attempt to render with existing logic first
        // and then queue up a corrected render after
        if self.instances_changed.borrow().is_empty() {
            let window_specs = self.window_from_scroll_position();
            if !wrapper_dirty
                && are_virtual_window_specs_equal(
                    Some(&self.window_specs.get()),
                    Some(&window_specs),
                )
                && self.rendered_observers.get() == self.observers.borrow().len()
                && !self.visible_instances_dirty.get()
                && self.reconcile_queue.borrow().is_empty()
            {
                return;
            }
            self.window_specs.set(window_specs);
        }
        self.visible_instances_dirty.set(false);
        self.rendered_observers.set(self.observers.borrow().len());
        let anchor = self.get_scroll_anchor(self.height.get());
        let mut updated_instances: InsertionMap<usize, InstanceRef> = InsertionMap::default();
        // NOTE(amadeus): If the wrapper is dirty, we need to force every component
        // to re-render
        let mut cursor = 0;
        loop {
            let next = if wrapper_dirty {
                self.observers.borrow().next_entry(&mut cursor)
            } else {
                self.visible_instances.borrow().next_entry(&mut cursor)
            };
            let Some((_, instance)) = next else {
                break;
            };
            if instance.on_render(wrapper_dirty) {
                updated_instances.insert(instance_key(&instance), instance);
            }
        }
        let mut cursor = 0;
        loop {
            let next = self.instances_changed.borrow().next_entry(&mut cursor);
            let Some((key, instance)) = next else {
                break;
            };
            if updated_instances.contains_key(&key) {
                continue;
            }
            if instance.on_render(wrapper_dirty) {
                updated_instances.insert(key, instance);
            }
        }

        self.scroll_fix(anchor);

        for (key, instance) in updated_instances.entries() {
            self.reconcile_queue.borrow_mut().remove(&key);
            instance.reconcile_heights();
        }
        // Reconcile externally-rendered instances that may have new heights
        let mut cursor = 0;
        loop {
            let next = self.reconcile_queue.borrow().next_entry(&mut cursor);
            let Some((_, instance)) = next else {
                break;
            };
            if instance.reconcile_heights() {
                instances_have_changed = true;
            }
        }
        self.reconcile_queue.borrow_mut().clear();
        instances_have_changed |= !self.instances_changed.borrow().is_empty();

        // Reconciliation reads virtualized offsets and can consume dirty geometry
        // flags, so mark after it when an instance update needs a corrected pass.
        if instances_have_changed {
            self.mark_dom_dirty();
        }

        if instances_have_changed || wrapper_dirty {
            self.queue_render();
        }
        updated_instances.clear();
        self.instances_changed.borrow_mut().clear();
    }

    fn scroll_fix(&self, anchor: Option<ScrollAnchor<NodeKey>>) {
        let Some(anchor) = anchor else {
            return;
        };
        let scroll_container = self.get_scroll_container_element();
        let ScrollAnchor {
            line_index,
            line_offset,
            file_element,
            file_offset,
            file_type_offset,
        } = anchor;
        if let (Some(line_index), Some(line_offset)) = (&line_index, line_offset)
            && let Some(element) = self.find_anchor_line(file_element, line_index)
        {
            let top = self.get_relative_bounding_top(&element, scroll_container.as_ref());
            if top != line_offset {
                let scroll_offset = top - line_offset;
                self.apply_scroll_fix(scroll_offset);
            }
            return;
        }
        let top = self.get_relative_bounding_top(&file_element, scroll_container.as_ref());
        if file_type_offset == FileTypeOffset::Top {
            if top != file_offset {
                self.apply_scroll_fix(top - file_offset);
            }
        } else {
            let bottom = top + self.host.element_bounding_height(&file_element);
            if bottom != file_offset {
                self.apply_scroll_fix(bottom - file_offset);
            }
        }
    }

    fn apply_scroll_fix(&self, scroll_offset: f64) {
        let root = self.root.borrow().clone();
        match root {
            None | Some(Root::Document) => {
                self.host.window_scroll_to(
                    self.host.window_scroll_y() + scroll_offset,
                    ScrollBehavior::Instant,
                );
            }
            Some(Root::Element(root)) => self.host.element_scroll_by(&root, scroll_offset),
        }
        // Because we fixed our scroll positions, it means something resized or
        // moved around, so we should mark everything as dirty so the
        // reconciliation call will get the latest data when figuring calling
        // .getOffsetInScrollContainer
        self.mark_dom_dirty();
    }

    /// Where the lines are: `fileElement.shadowRoot`. A file element with no shadow root is
    /// searched itself, which is what the scripted worlds of the oracles declare.
    fn line_scope(document: &Document, file_element: NodeKey) -> NodeKey {
        document.shadow_root(file_element).unwrap_or(file_element)
    }

    /// `fileElement.shadowRoot?.querySelectorAll('[data-line][data-line-index]')`
    /// in document order, skipping lines without a `dataset.lineIndex`.
    fn anchor_lines(&self, file_element: NodeKey) -> impl Iterator<Item = AnchorLine<NodeKey>> {
        let document = self.host.document();
        document
            .query_by_attribute(
                Self::line_scope(document, file_element),
                "data-line-index",
                None,
            )
            .into_iter()
            .filter(|element| document.get_attribute(*element, "data-line").is_some())
            .filter_map(|element| {
                Some(AnchorLine {
                    line_index: document.get_attribute(element, "data-line-index")?,
                    element,
                })
            })
    }

    /// `fileElement.shadowRoot?.querySelector('[data-line][data-line-index="{line_index}"]')`.
    fn find_anchor_line(&self, file_element: NodeKey, line_index: &str) -> Option<NodeKey> {
        let document = self.host.document();
        document
            .query_by_attribute(
                Self::line_scope(document, file_element),
                "data-line-index",
                Some(line_index),
            )
            .into_iter()
            .find(|element| document.get_attribute(*element, "data-line").is_some())
    }

    // This function tries to figure out the closest file or line to the viewport
    // top that's visible to use as a relative marker for how to fix scroll
    // position after issuing dom updates
    fn get_scroll_anchor(&self, viewport_height: f64) -> Option<ScrollAnchor<NodeKey>> {
        let scroll_container = self.get_scroll_container_element();
        let mut best_anchor: Option<ScrollAnchor<NodeKey>> = None;

        let file_elements: Vec<NodeKey> = self
            .visible_instances
            .borrow()
            .entries()
            .into_iter()
            .map(|(file_element, _)| file_element)
            .collect();
        for file_element in file_elements {
            let file_top = self.get_relative_bounding_top(&file_element, scroll_container.as_ref());
            let file_bottom = file_top + self.host.element_offset_height(&file_element);

            // Determine file offset and type based on position
            // Only use bottom anchor when entire file is above viewport
            let (file_offset, file_type_offset) = if file_bottom <= 0.0 {
                // Entire file is above viewport - use bottom as anchor
                (file_bottom, FileTypeOffset::Bottom)
            } else {
                // File is at least partially visible or below - use top
                (file_top, FileTypeOffset::Top)
            };

            // Find the best line (first fully visible) within this file
            let mut best_line_index: Option<String> = None;
            let mut best_line_offset: Option<f64> = None;

            // Only search for lines if file potentially intersects viewport
            if file_bottom > 0.0 && file_top < viewport_height {
                for line in self.anchor_lines(file_element) {
                    let line_offset =
                        self.get_relative_bounding_top(&line.element, scroll_container.as_ref());

                    // Ignore lines with negative offsets (above viewport top)
                    if line_offset < 0.0 {
                        continue;
                    }

                    // First visible line in DOM order is the best one because
                    // querySelectorAll will grab lines in order as they appear in the
                    // DOM
                    best_line_index = Some(line.line_index);
                    best_line_offset = Some(line_offset);
                    break;
                }
            }

            // If we already have an anchor with a visible line, skip files without one
            if best_anchor
                .as_ref()
                .is_some_and(|anchor| anchor.line_offset.is_some())
                && best_line_offset.is_none()
            {
                continue;
            }

            // Decide if this file should become the new best anchor
            let mut should_replace = false;
            match &best_anchor {
                // If we don't already have an anchor we should set one
                None => should_replace = true,
                Some(anchor) => {
                    // If we found a better line anchor, we should replace the old one
                    if let Some(line_offset) = best_line_offset
                        && anchor.line_offset.is_none_or(|best| line_offset < best)
                    {
                        should_replace = true;
                    }
                    // Otherwise we need to compare file only anchors
                    else if best_line_offset.is_none() && anchor.line_offset.is_none() {
                        // Favor files with their tops in view, or the closest
                        // file (two branches with one body upstream).
                        let favor_top_in_view = file_offset >= 0.0
                            && (anchor.file_offset < 0.0 || file_offset < anchor.file_offset);
                        let closest_file = file_offset < 0.0
                            && anchor.file_offset < 0.0
                            && file_offset > anchor.file_offset;
                        should_replace = favor_top_in_view || closest_file;
                    }
                }
            }

            if should_replace {
                best_anchor = Some(ScrollAnchor {
                    file_element,
                    file_type_offset,
                    file_offset,
                    line_index: best_line_index,
                    line_offset: best_line_offset,
                });
            }
        }

        best_anchor
    }

    /// `handleIntersectionChange`: the `IntersectionObserver` callback.
    pub fn handle_intersection_change(&self, entries: &[IntersectionEntry<NodeKey>]) {
        self.scroll_dirty.set(true);
        for IntersectionEntry {
            target,
            is_intersecting,
        } in entries
        {
            let instance = self.observers.borrow().get(target).cloned();
            // IntersectionObserver delivers entries asynchronously, so an entry can
            // arrive after the target was unobserved via disconnect() or releaseElement().
            let Some(instance) = instance else {
                continue;
            };
            let is_visible = self.visible_instances.borrow().contains_key(target);
            if *is_intersecting && !is_visible {
                instance.set_visibility(true);
                self.visible_instances
                    .borrow_mut()
                    .insert(*target, instance);
                self.visible_instances_dirty.set(true);
            } else if !is_intersecting && is_visible {
                instance.set_visibility(false);
                self.visible_instances.borrow_mut().remove(target);
                self.visible_instances_dirty.set(true);
            }
        }

        if self.visible_instances_dirty.get() {
            // Since this call is already debounced, should we just call
            // computeRenderRangeAndEmit directly?
            self.queue_render();
        }
    }

    /// Return the logical vertical position for the scroll container
    pub fn get_scroll_top(&self) -> f64 {
        if !self.scroll_dirty.get() {
            return self.scroll_top.get();
        }
        self.scroll_dirty.set(false);
        let scroll_top = match &*self.root.borrow() {
            None => 0.0,
            Some(Root::Document) => self.host.window_scroll_y(),
            Some(Root::Element(root)) => self.host.element_scroll_top(root),
        };

        // Lets always make sure to clamp scroll position cases of
        // over/bounce scroll
        let scroll_top = self.clamp_scroll_top(scroll_top);
        self.scroll_top.set(scroll_top);
        scroll_top
    }

    /// Scroll to a logical vertical position and queue the normal render pass.
    pub fn scroll_to(&self, options: VirtualizerScrollOptions) {
        let VirtualizerScrollOptions { top, behavior } = options;
        let Some(root) = self.get_root() else {
            return;
        };

        let scroll_top = self.clamp_scroll_top(top);
        match root {
            Root::Document => self.host.window_scroll_to(scroll_top, behavior),
            Root::Element(root) => self.host.element_scroll_to(&root, scroll_top, behavior),
        }
        self.scroll_dirty.set(true);
        self.queue_render();
    }

    fn clamp_scroll_top(&self, scroll_top: f64) -> f64 {
        let max = self.get_scroll_height() - self.get_height();
        0.0_f64.max(scroll_top.min(max))
    }

    fn get_scroll_height(&self) -> f64 {
        if !self.scroll_height_dirty.get() {
            return self.scroll_height.get();
        }
        self.scroll_height_dirty.set(false);
        let scroll_height = match &*self.root.borrow() {
            None => 0.0,
            Some(Root::Document) => self.host.document_scroll_height(),
            Some(Root::Element(root)) => self.host.element_scroll_height(root),
        };
        self.scroll_height.set(scroll_height);
        scroll_height
    }

    fn get_height(&self) -> f64 {
        if !self.height_dirty.get() {
            return self.height.get();
        }
        self.height_dirty.set(false);
        let height = match &*self.root.borrow() {
            None => 0.0,
            Some(Root::Document) => self.host.window_inner_height(),
            Some(Root::Element(root)) => self.host.element_bounding_height(root),
        };
        self.height.set(height);
        height
    }

    /// Test-only: the observers and visible instances held and the window.
    #[cfg(test)]
    pub fn observed_state(&self) -> (usize, usize, f64, f64) {
        let window = self.window_specs.get();
        (
            self.observers.borrow().len(),
            self.visible_instances.borrow().len(),
            window.top,
            window.bottom,
        )
    }

    /// Test-only: whether the geometry flags `mark_dom_dirty` sets are all set.
    #[cfg(test)]
    pub fn is_dom_dirty(&self) -> bool {
        self.scroll_dirty.get() && self.height_dirty.get() && self.scroll_height_dirty.get()
    }

    pub fn mark_dom_dirty(&self) {
        self.scroll_dirty.set(true);
        self.scroll_height_dirty.set(true);
        self.height_dirty.set(true);
    }

    fn get_scroll_container_element(&self) -> Option<NodeKey> {
        match &*self.root.borrow() {
            Some(Root::Element(root)) => Some(*root),
            None | Some(Root::Document) => None,
        }
    }

    // This function is like a generalized getBoundingClientRect for it's relative
    // scroll container
    fn get_relative_bounding_top(
        &self,
        element: &NodeKey,
        scroll_container: Option<&NodeKey>,
    ) -> f64 {
        let top = self.host.element_bounding_top(element);
        let scroll_container_top =
            scroll_container.map_or(0.0, |container| self.host.element_bounding_top(container));
        top - scroll_container_top
    }
}
