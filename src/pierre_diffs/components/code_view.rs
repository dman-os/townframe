// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C2: the item model; C3: the scroll model, in `code_view/scroll.rs`; see `parity/closure_notes.md`, "CodeView").
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `CodeView<LAnnotation, Caret>` is `CodeView<Host, Rendered, LAnnotation>`
//   (no `Caret`: edit mode is DROP). It is built behind an `Rc`
//   (`Rc::new_cyclic`): its items' instances call back into it through
//   `Weak<dyn AdvancedVirtualizer>` (`advanced.rs`).
// * The class's mutable fields are one `RefCell<State>`. No borrow is held
//   across a call into an instance or a user callback, both of which can
//   re-enter the viewer (`instanceChanged`, the selection wrappers).
// * Collaborators the constructor does not create (`FileDiff` needs a host, a
//   resize-observer hub, a render queue and a highlighter) are constructor
//   arguments, shared with every item instance.
// * `workerManager`, the highlighter-readiness gate (`isReady`), the React slot
//   coordinator and edit mode are DROP (dropped.psv): the highlights backend is
//   synchronous behind `DiffsHighlighter`, the Dioxus view is not
//   component-managed (Q5), and nothing renders into React slots.
//   `console.error`/`console.warn` on unknown ids return `false` without a log.
// * The three divs the constructor makes are created here, with their scaffolding
//   styles; `setup` and the rest of the DOM shell are `code_view/shell.rs`, the
//   element pool `code_view/pool.rs` (C4). `syncLayout`'s three style writes
//   are in C2 because `setOptions` runs without `setup`.
// * The render pass (`render`, `computeRenderRangeAndEmit`, `scrollTo`, C5) is
//   `code_view/render.rs`; `render` queues the callback on the host's frames as
//   upstream does.

mod item_options;
mod items;
mod layout;
pub mod options;
mod pool;
mod render;
mod scroll;
mod shell;
pub mod state;

pub use shell::SlabNodes;

#[cfg(test)]
mod element_pooling_upstream_tests;
#[cfg(test)]
mod harness_tests;
#[cfg(test)]
mod header_footer_upstream_tests;
#[cfg(test)]
mod item_cases_upstream_tests;
#[cfg(test)]
mod items_upstream_tests;
#[cfg(test)]
mod oracle_tests;
#[cfg(test)]
mod scroll_anchoring_upstream_tests;
#[cfg(test)]
mod scroll_oracle_tests;
#[cfg(test)]
mod scroll_upstream_tests;
#[cfg(test)]
mod shell_oracle_tests;
#[cfg(test)]
mod shell_upstream_tests;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::rc::Weak;

use self::options::CodeViewOptions;
use self::options::compute_item_metrics;
use self::scroll::DEFAULT_SCROLL_REBASE;
use self::scroll::FIREFOX_SCROLL_REBASE;
use self::scroll::ScrollRebaseConfig;
use self::state::CodeViewLineSelection;
use self::state::EMPTY_WINDOW_SPECS;
use self::state::HeaderFooterHost;
use self::state::ItemRef;
use self::state::PendingScrollTarget;
use self::state::RenderState;
use self::state::ScrollAnchor;
use self::state::ScrollToAnimation;
use crate::components::file_diff::RenderedContent;
use crate::components::file_diff::VirtualizedFileDiffHost;
use crate::components::virtualized_file_diff::advanced::AdvancedVirtualizer;
use crate::components::virtualizer::InstanceRef;
use crate::components::virtualizer::VirtualizerConfig;
use crate::components::virtualizer::VirtualizerHost;
use crate::document::NodeKey;
use crate::highlighter_types::DiffsHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::scroll_sync_manager::TimeoutId;
use crate::managers::universal_rendering_manager::RenderCallback;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::layout::CodeViewLayout;
use crate::types::layout::PendingCodeViewLayoutReset;
use crate::types::layout::VirtualFileMetrics;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::view::CodeViewItem;
use crate::utils::dom::platform::PlatformInfo;
use crate::utils::dom::platform::is_firefox;

/// What `CodeView` reads from the platform beyond a virtualized diff's own
/// needs. C2 has the two reads of `roundToDevicePixel` and
/// `prefersReducedMotion`; C4 and C5 add the rest of the host table.
pub trait CodeViewHost: VirtualizedFileDiffHost {
    /// `window.devicePixelRatio ?? 1`, read fresh on every call so a monitor
    /// switch or zoom is seen.
    fn device_pixel_ratio(&self) -> f64;
    /// `matchMedia('(prefers-reduced-motion: reduce)').matches`; false when the
    /// platform cannot answer.
    fn prefers_reduced_motion(&self) -> bool;
    /// `navigator.platform`, `navigator.userAgent` and the rest of what
    /// `utils/platform.ts` reads, as `new` needs it to choose the scroll
    /// rebase config (`isFirefox()`).
    fn platform_info(&self) -> PlatformInfo;
    /// `Math.random()`, for the sticky slab's jitter (Q9). A host a test
    /// scripts replays its sequence.
    fn random(&self) -> f64;
    /// `performance.now()`: the clock the frame timestamps are on, for
    /// `computeRenderRangeAndEmit`'s default timestamp and the start of a
    /// smooth scroll's animation.
    fn now(&self) -> f64;
    /// `new ResizeObserver(this.handleResize)`: the viewer's own observer, as
    /// distinct from the one `ResizeManager` shares. Its entries come back
    /// through `CodeView::handle_resize`.
    fn create_shell_observer(&self);
    /// `resizeObserver.observe(element)`
    fn shell_observe(&self, element: &NodeKey);
    /// `resizeObserver.unobserve(element)`
    fn shell_unobserve(&self, element: &NodeKey);
    /// `resizeObserver.disconnect()`
    fn shell_disconnect(&self);
    /// `root.addEventListener` of `scroll` (`CodeView::handle_scroll`) and of
    /// `wheel`, `touchstart`, `pointerdown` and `keydown`
    /// (`CodeView::clear_pending_scroll`), all passive.
    fn add_shell_listeners(&self, root: &NodeKey);
    /// The matching `removeEventListener`s.
    fn remove_shell_listeners(&self, root: &NodeKey);
    /// `document.activeElement`
    fn active_element(&self) -> Option<NodeKey>;
    /// `element.shadowRoot?.activeElement`
    fn shadow_active_element(&self, element: &NodeKey) -> Option<NodeKey>;
    /// `root.focus({ preventScroll: true })`
    fn focus_root(&self, root: &NodeKey);

    // The sticky slab's writes, on their own narrow path: they happen on every
    // frame the slab moves, so a host that renders the slab separately from the
    // rest of the diff overrides these three. The defaults are the `Document`
    // writes upstream makes.
    /// `stickyOffset.style.height = `${height}px``; `None` is `= ''`.
    fn write_slab_spacer(&self, spacer: NodeKey, height: Option<f64>) {
        let document = VirtualizerHost::document(self);
        match height {
            Some(height) => document.set_style_property(spacer, "height", &css_px(height)),
            None => document.remove_style_property(spacer, "height"),
        }
    }
    /// `stickyContainer.style.top`/`.bottom`
    fn write_slab_sticky(&self, slab: NodeKey, top: f64, bottom: f64) {
        let document = VirtualizerHost::document(self);
        document.set_style_property(slab, "top", &css_px(top));
        document.set_style_property(slab, "bottom", &css_px(bottom));
    }
    /// `container.style.height = `${height}px``
    fn write_container_height(&self, container: NodeKey, height: f64) {
        VirtualizerHost::document(self).set_style_property(container, "height", &css_px(height));
    }
}

/// `MOBILE_SAFARI`: a heuristic to detect mobile Safari only.
fn is_mobile_safari(info: &PlatformInfo) -> bool {
    let user_agent = info.user_agent.as_str();
    let is_ios = ["iPhone", "iPad", "iPod"]
        .iter()
        .any(|device| user_agent.contains(device));
    let is_ipad_os = info.platform.as_deref() == Some("MacIntel") && info.max_touch_points > 1;
    (is_ios || is_ipad_os)
        && user_agent.contains("AppleWebKit")
        && user_agent.contains("Safari")
        && !["CriOS", "FxiOS", "EdgiOS", "OPiOS"]
            .iter()
            .any(|browser| user_agent.contains(browser))
}

/// `${value}px`, with JavaScript's rendering of negative zero.
pub(crate) fn css_px(value: f64) -> String {
    format!("{}px", if value == 0.0 { 0.0 } else { value })
}

/// `CodeViewScrollListener`: called with the logical scroll top and the viewer.
pub type ScrollListener<Host, Rendered, LAnnotation> =
    Rc<dyn Fn(f64, &CodeView<Host, Rendered, LAnnotation>)>;

/// The mutable fields of the class (see the header).
pub(crate) struct State<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub(crate) items: Vec<ItemRef<Host, Rendered, LAnnotation>>,
    pub(crate) id_to_item: HashMap<String, ItemRef<Host, Rendered, LAnnotation>>,
    pub(crate) selected_lines: Option<CodeViewLineSelection>,
    /// Keyed by the instance's allocation (`instance_key`).
    pub(crate) instance_to_item: HashMap<usize, ItemRef<Host, Rendered, LAnnotation>>,
    pub(crate) layout_dirty_index: Option<usize>,
    pub(crate) pending_layout_reset: Option<PendingCodeViewLayoutReset>,
    pub(crate) render_options_revision: i64,
    pub(crate) scroll_height: f64,
    /// The cached logical scroll position, valid while `scroll_dirty` is false.
    pub(crate) scroll_top: f64,
    /// How far the paged scroll window sits into the logical scroll range.
    pub(crate) scroll_page_offset: f64,
    pub(crate) scroll_rebase_config: ScrollRebaseConfig,
    pub(crate) scroll_dirty: bool,
    /// The cached viewport height, valid while `height_dirty` is false.
    pub(crate) height: f64,
    pub(crate) height_dirty: bool,
    pub(crate) window_specs: VirtualWindowSpecs,
    pub(crate) render_state: RenderState,
    pub(crate) item_metrics_cache: VirtualFileMetrics,
    pub(crate) pending_scroll_target: Option<PendingScrollTarget>,
    pub(crate) pending_layout_anchor: Option<ScrollAnchor>,
    pub(crate) scroll_animation: Option<ScrollToAnimation>,
    pub(crate) header: HeaderFooterHost<Rendered>,
    pub(crate) footer: HeaderFooterHost<Rendered>,
    pub(crate) options: CodeViewOptions<Rendered, LAnnotation>,
    pub(crate) root: Option<NodeKey>,
    /// `container: HTMLDivElement | undefined`: undefined after `cleanUp`.
    pub(crate) container: Option<NodeKey>,
    /// The height last written to `container`, `-1` before the first write.
    pub(crate) container_height: f64,
    pub(crate) scroll_interaction_fix_timer: Option<TimeoutId>,
    pub(crate) pointer_events_disabled: bool,
    pub(crate) code_overflow_fix: bool,
    pub(crate) should_fix_container_focus: bool,
    /// `resizeObserver !== undefined`
    pub(crate) resize_observing: bool,
    pub(crate) element_pool: Vec<NodeKey>,
    pub(crate) element_pool_version: u64,
    /// `elementPoolTracker`: the pool version each live shell was made under.
    pub(crate) element_pool_tracker: HashMap<NodeKey, u64>,
    pub(crate) scroll_listeners: Vec<(u64, ScrollListener<Host, Rendered, LAnnotation>)>,
    pub(crate) next_scroll_listener_id: u64,
}

pub struct CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub(crate) weak_self: Weak<Self>,
    pub(crate) host: Rc<Host>,
    pub(crate) resize_observer_hub: Rc<ResizeObserverHub<Host>>,
    pub(crate) render_queue: UniversalRenderingManager<Host>,
    pub(crate) highlighter: Rc<dyn DiffsHighlighter>,
    /// `computeRenderRangeAndEmit` as the render queue holds it (identity is
    /// the `Rc`).
    pub(crate) compute_render_range_and_emit: RenderCallback,
    /// `config: VirtualizerConfig`, the one the instances read through the
    /// virtualizer union (`overscrollSize: 200`).
    pub config: VirtualizerConfig,
    pub(crate) sticky_container: NodeKey,
    pub(crate) sticky_offset: NodeKey,
    /// `MOBILE_SAFARI`, from the host's `PlatformInfo`.
    pub(crate) mobile_safari: bool,
    pub(crate) state: RefCell<State<Host, Rendered, LAnnotation>>,
    /// Test-only: the `immediate` argument of every `render` call, in order.
    #[cfg(test)]
    pub(crate) render_calls: RefCell<Vec<bool>>,
}

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `new CodeView(options, undefined, false)`
    pub fn new(
        host: Rc<Host>,
        resize_observer_hub: Rc<ResizeObserverHub<Host>>,
        render_queue: UniversalRenderingManager<Host>,
        highlighter: Rc<dyn DiffsHighlighter>,
        options: CodeViewOptions<Rendered, LAnnotation>,
    ) -> Rc<Self> {
        options.validate();
        let item_metrics_cache = compute_item_metrics(options.item_metrics);
        let scroll_rebase_config = if is_firefox(&host.platform_info()) {
            FIREFOX_SCROLL_REBASE
        } else {
            DEFAULT_SCROLL_REBASE
        };
        let document = VirtualizerHost::document(&*host);
        let (container, sticky_container, sticky_offset) = (
            document.create_element("div"),
            document.create_element("div"),
            document.create_element("div"),
        );
        document.set_style_property(sticky_offset, "contain", "layout size");
        for (name, value) in [
            ("position", "sticky"),
            ("width", "100%"),
            ("contain", "layout style inline-size"),
            ("isolation", "isolate"),
            ("display", "flex"),
            ("flex-direction", "column"),
        ] {
            document.set_style_property(sticky_container, name, value);
        }
        let mobile_safari = is_mobile_safari(&host.platform_info());
        Rc::new_cyclic(|weak: &Weak<Self>| {
            let compute_weak = Weak::clone(weak);
            let compute_render_range_and_emit: RenderCallback = Rc::new(move |timestamp| {
                compute_weak
                    .upgrade()
                    .expect("the CodeView outlives its queued render")
                    .compute_render_range_and_emit(Some(timestamp));
            });
            Self {
                weak_self: Weak::clone(weak),
                host,
                resize_observer_hub,
                render_queue,
                highlighter,
                compute_render_range_and_emit,
                config: VirtualizerConfig {
                    overscroll_size: 200.0,
                    intersection_observer_margin: 0.0,
                    resize_debugging: false,
                },
                sticky_container,
                sticky_offset,
                mobile_safari,
                state: RefCell::new(State {
                    items: Vec::new(),
                    id_to_item: HashMap::new(),
                    selected_lines: None,
                    instance_to_item: HashMap::new(),
                    layout_dirty_index: None,
                    pending_layout_reset: None,
                    render_options_revision: 0,
                    scroll_height: 0.0,
                    scroll_top: 0.0,
                    scroll_page_offset: 0.0,
                    scroll_rebase_config,
                    scroll_dirty: true,
                    height: 0.0,
                    height_dirty: true,
                    window_specs: EMPTY_WINDOW_SPECS,
                    render_state: RenderState::RESET,
                    item_metrics_cache,
                    pending_scroll_target: None,
                    pending_layout_anchor: None,
                    scroll_animation: None,
                    header: HeaderFooterHost::default(),
                    footer: HeaderFooterHost::default(),
                    options,
                    root: None,
                    container: Some(container),
                    container_height: -1.0,
                    scroll_interaction_fix_timer: None,
                    pointer_events_disabled: false,
                    code_overflow_fix: false,
                    should_fix_container_focus: false,
                    resize_observing: false,
                    element_pool: Vec::new(),
                    element_pool_version: 0,
                    element_pool_tracker: HashMap::new(),
                    scroll_listeners: Vec::new(),
                    next_scroll_listener_id: 0,
                }),
                #[cfg(test)]
                render_calls: RefCell::default(),
            }
        })
    }

    /// `getLayout()`
    pub fn get_layout(&self) -> CodeViewLayout {
        self.state.borrow().options.layout_or_default()
    }

    /// Absolute offset (in scroll pixels) from the top of the scroll content to
    /// the first virtualized item: the container's top margin plus the header
    /// host's height. `getItemTopOffset()`
    pub fn get_item_top_offset(&self) -> f64 {
        self.get_layout().padding_top + self.state.borrow().header.height
    }

    /// `getScrollHeight()`
    pub fn get_scroll_height(&self) -> f64 {
        self.state.borrow().scroll_height
    }

    /// `getItem(itemId)`: the latest snapshot of the item.
    pub fn get_item(&self, item_id: &str) -> Option<CodeViewItem<LAnnotation>> {
        self.state
            .borrow()
            .id_to_item
            .get(item_id)
            .map(|item| item.borrow().snapshot())
    }

    /// `getTopForItem(id)`
    pub fn get_top_for_item(&self, id: &str) -> Option<f64> {
        let top = self.state.borrow().id_to_item.get(id)?.borrow().top;
        Some(top + self.get_item_top_offset())
    }

    /// `getSelectedLines()`
    pub fn get_selected_lines(&self) -> Option<CodeViewLineSelection> {
        self.state.borrow().selected_lines.clone()
    }

    /// `syncLayout()`: the gap on the sticky container and the paddings as the
    /// container's margins.
    pub(crate) fn sync_layout(&self) {
        let CodeViewLayout {
            gap,
            padding_bottom,
            padding_top,
        } = self.get_layout();
        let document = VirtualizerHost::document(&*self.host);
        document.set_style_property(self.sticky_container, "gap", &format!("{gap}px"));
        let container = self.state.borrow().container;
        if let Some(container) = container {
            document.set_style_property(container, "margin-top", &format!("{padding_top}px"));
            document.set_style_property(container, "margin-bottom", &format!("{padding_bottom}px"));
        }
    }

    /// `this` as an instance's virtualizer names it.
    pub(crate) fn instance_key(instance: &InstanceRef) -> usize {
        crate::components::virtualizer::instance_key(instance)
    }
}

impl<Host, Rendered, LAnnotation> AdvancedVirtualizer for CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `instanceChanged`. Upstream's `NOTE(amadeus)`: "technically broken"; it
    /// should collect changed instances and recompute every top. Inherited.
    fn instance_changed(&self, instance: &InstanceRef, layout_dirty: bool) {
        let item = self
            .state
            .borrow()
            .instance_to_item
            .get(&Self::instance_key(instance))
            .cloned()
            .expect("CodeView.instanceChanged: An instance has changed that is not registered");
        if layout_dirty {
            self.mark_item_layout_dirty(&item);
        }
        self.render(false);
    }

    /// `getWindowSpecs()`
    fn get_window_specs(&self) -> VirtualWindowSpecs {
        self.state.borrow().window_specs
    }

    /// `getLocalTopForInstance(instance)`
    fn get_local_top_for_instance(&self, instance: &InstanceRef) -> f64 {
        self.state
            .borrow()
            .instance_to_item
            .get(&Self::instance_key(instance))
            .expect("CodeView.getLocalTopForInstance: unknown virtualized instance")
            .borrow()
            .top
    }
}
