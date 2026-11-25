//! Differential test for C4, the DOM shell of `CodeView`
//! (`oracle/s12e_code_view_shell.mjs`, golden
//! `oracle/golden/s12e_code_view_shell.json.gz`): the published class over
//! jsdom, with a fake clock, scripted `Math.random`, scripted geometry and
//! focus, and `render` observed (and forwarded to the real render pass), ran scenarios of `setup`, the
//! root handlers, scroll interaction suspension, the header and footer hosts,
//! the sticky slab and the element pool. Each step records its value, the host
//! trace, the observable state and the trees of the root and the pool. The port
//! replays them over `FakeDom` (`render_calls` is the observer). Test-only,
//! original to this crate.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::CodeView;
use super::options::CodeViewOptions;
use super::options::RenderCodeViewSlotCallback;
use super::scroll_oracle_tests::anchor_json;
use super::scroll_oracle_tests::animation_from_json;
use super::scroll_oracle_tests::pending_json;
use super::state::ItemRef;
use crate::components::file_diff_render_oracle_tests::snapshot_outline;
use crate::components::virtualized_file_diff::layout_oracle_tests::MetricsSpec;
use crate::components::virtualized_file_diff::layout_oracle_tests::Spec;
use crate::components::virtualized_file_diff::layout_oracle_tests::options_from_spec;
use crate::components::virtualized_file_diff::layout_oracle_tests::panic_message;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::js_collections::PtrEq;
use crate::managers::resize_manager::ResizeObserverEntry;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_close;
use crate::oracle_testing::load_golden;
use crate::types::layout::CodeViewLayout;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::options::LineDiffTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewScrollTarget;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

type View = CodeView<FakeDom, u8, u8>;

#[derive(Deserialize)]
struct Golden {
    diffs: HashMap<String, FileDiffMetadata>,
    metrics: HashMap<String, MetricsSpec>,
    layouts: HashMap<String, CodeViewLayout>,
    slots: HashMap<String, Option<u8>>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    steps: Vec<Recorded>,
}

#[derive(Deserialize)]
struct Recorded {
    #[serde(flatten)]
    result: Map<String, Value>,
    trace: Vec<Value>,
    state: Value,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    #[serde(default)]
    env: Env,
    options: OptionsSpec,
    steps: Vec<Step>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Env {
    user_agent: Option<String>,
    platform: Option<String>,
    max_touch_points: Option<u32>,
    dpr: Option<f64>,
    /// The geometry rules of the render scenarios (`FakeDom`'s `rules`).
    #[serde(default)]
    rules: bool,
    /// A root that clamps its scroll position to the container's extent.
    #[serde(default)]
    clamp: bool,
    /// A root that does not clamp it at all.
    #[serde(default)]
    unclamped: bool,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct OptionsSpec {
    #[serde(default)]
    diff: Spec,
    pointer_events_on_scroll: Option<bool>,
    item_metrics: Option<String>,
    layout: Option<String>,
    sticky_headers: Option<bool>,
    /// `undefined` is absent; `null` (no callback) is `Some(None)`.
    #[serde(default, deserialize_with = "present")]
    header: Option<Option<String>>,
    #[serde(default, deserialize_with = "present")]
    footer: Option<Option<String>>,
}

fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct ItemSpec {
    id: String,
    diff: String,
    version: Option<usize>,
    collapsed: Option<bool>,
}

#[derive(Deserialize)]
struct Entry {
    target: String,
    #[serde(rename = "blockSize")]
    block_size: f64,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Step {
    Setup {
        tabindex: Option<i32>,
        #[serde(rename = "overflowAnchor")]
        overflow_anchor: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    World {
        scroll_top: Option<f64>,
        scroll_height: Option<f64>,
        viewport_height: Option<f64>,
    },
    SetHeight {
        target: String,
        height: f64,
    },
    SetItems {
        items: Vec<ItemSpec>,
    },
    AddItems {
        items: Vec<ItemSpec>,
    },
    UpdateItem {
        item: ItemSpec,
    },
    RemoveItem {
        id: String,
    },
    UpdateItemId {
        from: String,
        to: String,
    },
    ScrollTo {
        target: CodeViewScrollTarget,
    },
    Render {
        #[serde(default)]
        immediate: bool,
    },
    Frames {
        #[serde(default = "one")]
        rounds: usize,
    },
    Viewport {
        height: f64,
    },
    Recompute,
    SetOptions {
        options: OptionsSpec,
    },
    SetState {
        state: Map<String, Value>,
    },
    Random {
        values: Vec<f64>,
    },
    Scroll {
        top: f64,
    },
    ClearPendingScroll,
    Resize {
        entries: Vec<Entry>,
    },
    Advance {
        ms: f64,
    },
    Mount {
        index: usize,
        after: Option<usize>,
    },
    Release {
        index: usize,
    },
    Decorate {
        index: usize,
        #[serde(default)]
        shadow: Vec<String>,
        #[serde(default)]
        light: usize,
        focus: Option<String>,
    },
    Unfocus,
    Call {
        #[serde(rename = "fn")]
        function: String,
        window: Option<Window>,
        bounds: Option<Bounds>,
        target: Option<f64>,
        synced: Option<f64>,
        updated: Option<Vec<usize>>,
    },
    Reset,
    CleanUp,
}

fn one() -> usize {
    1
}

#[derive(Deserialize, Clone, Copy)]
struct Window {
    top: f64,
    bottom: f64,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
struct Bounds {
    sticky_top: f64,
    sticky_bottom: f64,
}

impl From<Window> for VirtualWindowSpecs {
    fn from(window: Window) -> Self {
        Self {
            top: window.top,
            bottom: window.bottom,
        }
    }
}

struct World {
    dom: Rc<FakeDom>,
    view: Rc<View>,
    golden: Rc<Golden>,
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
    slots: HashMap<String, RenderCodeViewSlotCallback<u8>>,
    root: std::cell::Cell<Option<NodeKey>>,
    env_rules: bool,
    env_clamp: bool,
    env_unclamped: bool,
}

/// The shell vocabulary of the trace; document mutations are compared through
/// the trees.
const TRACED: [&str; 12] = [
    "raf.request",
    "raf.cancel",
    "element.scrollTo",
    "setTimeout",
    "clearTimeout",
    "observer.create",
    "observe",
    "unobserve",
    "observer.disconnect",
    "listener.add",
    "listener.remove",
    "focus",
];

impl World {
    fn options(&self, spec: &OptionsSpec) -> CodeViewOptions<u8, u8> {
        let mut options = CodeViewOptions {
            diff: options_from_spec(&spec.diff, &mut HashMap::new()),
            ..CodeViewOptions::default()
        };
        // The port's word diff is `similar`'s, not jsdiff's (PARITY.md,
        // diffWordsWithSpace), so the renders compared here carry no intra-line
        // diff spans.
        options
            .diff
            .base
            .line_diff_type
            .get_or_insert(LineDiffTypes::None);
        options.pointer_events_on_scroll = spec.pointer_events_on_scroll;
        options.sticky_headers = spec.sticky_headers;
        options.item_metrics = spec
            .item_metrics
            .as_ref()
            .and_then(|name| self.golden.metrics.get(name))
            .map(|metrics| PartialVirtualFileMetrics::from(*metrics));
        options.layout = spec.layout.as_ref().map(|name| self.golden.layouts[name]);
        let slot = |name: &Option<Option<String>>| {
            name.as_ref()
                .and_then(Option::as_ref)
                .map(|name| self.slots[name].clone())
        };
        options.render_code_view_header = slot(&spec.header);
        options.render_code_view_footer = slot(&spec.footer);
        options
    }

    /// The node a script names; `None` for a host that is not mounted (the
    /// script's `undefined`).
    fn node(&self, name: &str) -> Option<NodeKey> {
        let state = self.view.state.borrow();
        match name {
            "root" => self.root.get(),
            "sticky" => Some(self.view.sticky_container),
            "offset" => Some(self.view.sticky_offset),
            "container" => state.container,
            "header" => state.header.element,
            "footer" => state.footer.element,
            other => panic!("unknown target {other}"),
        }
    }

    fn item(&self, spec: &ItemSpec) -> CodeViewItem<u8> {
        CodeViewItem::Diff(CodeViewDiffItem {
            id: spec.id.clone(),
            file_diff: Rc::clone(&self.diffs[&spec.diff]),
            annotations: None,
            version: spec.version,
            collapsed: spec.collapsed,
            edit: None,
        })
    }

    fn items(&self, specs: &[ItemSpec]) -> Vec<CodeViewItem<u8>> {
        specs.iter().map(|spec| self.item(spec)).collect()
    }

    fn item_at(&self, index: usize) -> Option<ItemRef<FakeDom, u8, u8>> {
        self.view.state.borrow().items.get(index).cloned()
    }

    fn apply(&self, step: &Step) -> Value {
        let (dom, view) = (&self.dom, &self.view);
        match step {
            Step::Setup {
                tabindex,
                overflow_anchor,
            } => {
                let root = dom.doc.create_element("div");
                dom.shared
                    .labels
                    .borrow_mut()
                    .insert(root, "root".to_owned());
                dom.by_name.borrow_mut().insert("root".to_owned(), root);
                self.root.set(Some(root));
                let viewport = dom.world.borrow().viewport_height;
                dom.heights.borrow_mut().insert(root, viewport);
                {
                    let mut world = dom.world.borrow_mut();
                    world.rules = self.env_rules;
                    if self.env_clamp {
                        world.clamp_root = Some(root);
                    } else if self.env_unclamped {
                        world.scroll_height = f64::INFINITY;
                    }
                }
                if let Some(tabindex) = tabindex {
                    dom.doc
                        .set_attribute(root, "tabindex", &tabindex.to_string());
                }
                if let Some(value) = overflow_anchor {
                    dom.doc.set_style_property(root, "overflow-anchor", value);
                }
                view.setup(root);
                Value::Null
            }
            Step::World {
                scroll_top,
                scroll_height,
                viewport_height,
            } => {
                let mut world = dom.world.borrow_mut();
                if let Some(value) = scroll_top {
                    world.scroll_top = *value;
                }
                if let Some(value) = scroll_height {
                    world.scroll_height = *value;
                }
                if let Some(value) = viewport_height {
                    world.viewport_height = *value;
                }
                drop(world);
                if let (Some(value), Some(root)) = (viewport_height, self.root.get()) {
                    dom.heights.borrow_mut().insert(root, *value);
                }
                Value::Null
            }
            Step::SetHeight { target, height } => {
                if let Some(node) = self.node(target) {
                    dom.heights.borrow_mut().insert(node, *height);
                }
                Value::Null
            }
            Step::SetItems { items } => {
                view.set_items(&self.items(items));
                Value::Null
            }
            Step::AddItems { items } => {
                view.add_items(&self.items(items));
                Value::Null
            }
            Step::UpdateItem { item } => json!(view.update_item(&self.item(item))),
            Step::RemoveItem { id } => json!(view.remove_item(id)),
            Step::UpdateItemId { from, to } => json!(view.update_item_id(from, to)),
            Step::ScrollTo { target } => {
                view.scroll_to(target);
                Value::Null
            }
            Step::Render { immediate } => {
                view.render(*immediate);
                Value::Null
            }
            Step::Frames { rounds } => {
                for _ in 0..*rounds {
                    if !dom.frames.run_round(dom.clock.get()) {
                        break;
                    }
                }
                Value::Null
            }
            Step::Viewport { height } => {
                dom.world.borrow_mut().viewport_height = *height;
                let root = self.root.get().expect("the viewport follows a setup");
                dom.heights.borrow_mut().insert(root, *height);
                Value::Null
            }
            Step::Recompute => {
                let pending = {
                    let mut state = view.state.borrow_mut();
                    let reset = state.pending_layout_reset.take();
                    state.layout_dirty_index.take().map(|index| (index, reset))
                };
                if let Some((index, reset)) = pending {
                    view.recompute_layout(index, reset);
                }
                Value::Null
            }
            Step::SetOptions { options } => {
                view.set_options(Some(self.options(options)));
                Value::Null
            }
            Step::SetState { state } => {
                let mut view_state = view.state.borrow_mut();
                let number = |key: &str| state.get(key).and_then(Value::as_f64);
                let flag = |key: &str| state.get(key).and_then(Value::as_bool);
                if let Some(value) = number("scrollTop") {
                    view_state.scroll_top = value;
                }
                if let Some(value) = number("scrollPageOffset") {
                    view_state.scroll_page_offset = value;
                }
                if let Some(value) = flag("scrollDirty") {
                    view_state.scroll_dirty = value;
                }
                if let Some(value) = number("height") {
                    view_state.height = value;
                }
                if let Some(value) = flag("heightDirty") {
                    view_state.height_dirty = value;
                }
                if let Some(value) = number("scrollHeight") {
                    view_state.scroll_height = value;
                }
                if let Some(render_state) = state.get("renderState") {
                    let render = &mut view_state.render_state;
                    let field = |key: &str| render_state.get(key).and_then(Value::as_f64);
                    if let Some(value) = field("firstIndex") {
                        render.first_index = value as i64;
                    }
                    if let Some(value) = field("lastIndex") {
                        render.last_index = value as i64;
                    }
                    if let Some(value) = field("stickyHeight") {
                        render.sticky_height = value;
                    }
                    if let Some(value) = field("stickyTop") {
                        render.sticky_top = value;
                    }
                    if let Some(value) = field("stickyBottom") {
                        render.sticky_bottom = value;
                    }
                }
                if let Some(animation) = state.get("scrollAnimation") {
                    view_state.scroll_animation =
                        (!animation.is_null()).then(|| animation_from_json(animation));
                }
                Value::Null
            }
            Step::Random { values } => {
                dom.random_values
                    .borrow_mut()
                    .extend(values.iter().copied());
                Value::Null
            }
            Step::Scroll { top } => {
                dom.world.borrow_mut().scroll_top = *top;
                view.handle_scroll();
                Value::Null
            }
            Step::ClearPendingScroll => {
                view.clear_pending_scroll();
                Value::Null
            }
            Step::Resize { entries } => {
                let entries: Vec<_> = entries
                    .iter()
                    .filter_map(|entry| {
                        Some(ResizeObserverEntry {
                            target: self.node(&entry.target)?,
                            border_box_block_size: entry.block_size,
                            content_box_inline_size: 0.0,
                        })
                    })
                    .collect();
                view.handle_resize(&entries);
                Value::Null
            }
            Step::Advance { ms } => {
                dom.advance_clock(*ms);
                Value::Null
            }
            Step::Mount { index, after } => {
                let Some(item) = self.item_at(*index) else {
                    return Value::Null;
                };
                if item.borrow().element.is_some() {
                    return Value::Null;
                }
                let element = view.acquire_element();
                item.borrow_mut().element = Some(element);
                let previous = after
                    .and_then(|after| self.item_at(after))
                    .and_then(|item| item.borrow().element);
                view.sync_rendered_item_order(element, previous);
                let instance = Rc::clone(item.borrow().instance());
                instance.file_diff().virtualized().virtualized_setup();
                let mut state = view.state.borrow_mut();
                let index = *index as i64;
                if state.render_state.first_index == -1 || index < state.render_state.first_index {
                    state.render_state.first_index = index;
                }
                if index > state.render_state.last_index {
                    state.render_state.last_index = index;
                }
                Value::Null
            }
            Step::Release { index } => {
                if let Some(item) = self.item_at(*index) {
                    view.release_rendered_item(&item);
                }
                Value::Null
            }
            Step::Decorate {
                index,
                shadow,
                light,
                focus,
            } => {
                let Some(element) = self.item_at(*index).and_then(|item| item.borrow().element)
                else {
                    return Value::Null;
                };
                let document = &dom.doc;
                let shadow_root = document
                    .shadow_root(element)
                    .unwrap_or_else(|| document.attach_shadow(element));
                for kind in shadow {
                    let (tag, attribute) = match kind.as_str() {
                        "sprite" => ("svg", "data-shell-sprite"),
                        "theme" => ("style", "data-theme-css"),
                        "unsafe" => ("style", "data-unsafe-css"),
                        "core" => ("style", "data-core-css"),
                        "junkStyle" => ("style", "data-junk"),
                        "junkDiv" => ("div", "data-junk"),
                        "header" => ("div", "data-diffs-header"),
                        other => panic!("unknown shadow kind {other}"),
                    };
                    let child = document.create_element(tag);
                    document.set_attribute(child, attribute, "");
                    document.append_child(shadow_root, child);
                }
                for at in 0..*light {
                    let child = document.create_element("span");
                    document.set_attribute(child, "slot", &format!("s{at}"));
                    document.append_child(element, child);
                }
                match focus.as_deref() {
                    None => {}
                    Some("light") => dom
                        .active_element
                        .set(document.children(element).last().copied()),
                    Some("self") => dom.active_element.set(Some(element)),
                    Some("shadow") => {
                        dom.active_element.set(None);
                        if let Some(last) = document.children(shadow_root).last() {
                            dom.shadow_active_elements
                                .borrow_mut()
                                .insert(element, *last);
                        }
                    }
                    Some(other) => panic!("unknown focus {other}"),
                }
                Value::Null
            }
            Step::Unfocus => {
                dom.active_element.set(None);
                Value::Null
            }
            Step::Call {
                function,
                window,
                bounds,
                target,
                synced,
                updated,
            } => {
                let window = window.map(VirtualWindowSpecs::from);
                match function.as_str() {
                    "getStickyBounds" => view.get_sticky_bounds(window).map_or(Value::Null, |bounds| {
                        json!({ "stickyTop": bounds.sticky_top, "stickyBottom": bounds.sticky_bottom })
                    }),
                    "applyStickyPositioning" => {
                        let bounds = bounds.expect("bounds");
                        view.apply_sticky_positioning(super::shell::StickyBounds {
                            sticky_top: bounds.sticky_top,
                            sticky_bottom: bounds.sticky_bottom,
                        });
                        Value::Null
                    }
                    "syncPagedScrollScaffolding" => {
                        view.sync_paged_scroll_scaffolding(window.expect("a window"));
                        Value::Null
                    }
                    "applyScrollFix" => {
                        view.apply_scroll_fix(
                            target.expect("a target"),
                            synced.expect("a synced position"),
                            window.expect("a window"),
                        );
                        Value::Null
                    }
                    "reconcileRenderedItems" => {
                        let updated = updated.as_ref().map(|indexes| {
                            indexes
                                .iter()
                                .filter_map(|index| self.item_at(*index))
                                .map(|item| super::shell::item_key(&item))
                                .collect::<std::collections::HashSet<_>>()
                        });
                        view.reconcile_rendered_items(updated.as_ref());
                        Value::Null
                    }
                    "acquireAndDrop" => {
                        let element = view.acquire_element();
                        dom.shared
                            .labels
                            .borrow_mut()
                            .insert(element, "acquired".to_owned());
                        Value::Null
                    }
                    "getElementPoolLimit" => json!(view.get_element_pool_limit()),
                    "reconcileHeaderFooterHosts" => json!(view.reconcile_header_footer_hosts()),
                    "measureMountedHosts" => {
                        view.measure_mounted_hosts();
                        Value::Null
                    }
                    "syncContainerHeight" => {
                        view.sync_container_height();
                        Value::Null
                    }
                    "updateStickyPositioning" => {
                        view.update_sticky_positioning();
                        Value::Null
                    }
                    "suspendScrollInteractions" => {
                        view.suspend_scroll_interactions();
                        Value::Null
                    }
                    "restoreScrollInteractions" => {
                        view.restore_scroll_interactions();
                        Value::Null
                    }
                    "fixContainerFocus" => {
                        view.fix_container_focus();
                        Value::Null
                    }
                    other => panic!("unknown call {other}"),
                }
            }
            Step::Reset => {
                view.reset();
                Value::Null
            }
            Step::CleanUp => {
                view.clean_up();
                Value::Null
            }
        }
    }

    fn snapshot(&self) -> Value {
        let state = self.view.state.borrow();
        let tree = |node: NodeKey| snapshot_outline(&self.dom, node);
        let host = |host: &super::state::HeaderFooterHost<u8>| {
            json!({
                "height": host.height,
                "element": host.element.is_some(),
                "render": host.render.is_some(),
            })
        };
        json!({
            "scrollTop": state.scroll_top,
            "scrollPageOffset": state.scroll_page_offset,
            "scrollDirty": state.scroll_dirty,
            "height": state.height,
            "heightDirty": state.height_dirty,
            "scrollHeight": state.scroll_height,
            "containerHeight": state.container_height,
            "pointerEventsDisabled": state.pointer_events_disabled,
            "codeOverflowFix": state.code_overflow_fix,
            "shouldFixContainerFocus": state.should_fix_container_focus,
            "timer": state.scroll_interaction_fix_timer,
            "header": host(&state.header),
            "footer": host(&state.footer),
            "poolSize": state.element_pool.len(),
            "poolVersion": state.element_pool_version,
            "renderState": {
                "scrollTop": state.render_state.scroll_top,
                "firstIndex": state.render_state.first_index,
                "lastIndex": state.render_state.last_index,
                "stickyHeight": state.render_state.sticky_height,
                "stickyTop": state.render_state.sticky_top,
                "stickyBottom": state.render_state.sticky_bottom,
            },
            "windowSpecs": { "top": state.window_specs.top, "bottom": state.window_specs.bottom },
            "pendingScrollTarget": state.pending_scroll_target.as_ref().map_or(Value::Null, pending_json),
            "scrollAnimation": state.scroll_animation.map_or(Value::Null, |animation| json!({
                "position": animation.position,
                "velocity": animation.velocity,
                "lastTimestamp": animation.last_timestamp,
            })),
            "pendingLayoutAnchor": state.pending_layout_anchor.as_ref().map_or(Value::Null, anchor_json),
            "items": state.items.iter().map(|item| {
                let item = item.borrow();
                json!([item.id(), item.top, item.height, item.element.is_some()])
            }).collect::<Vec<_>>(),
            "root": self.root.get().map_or(Value::Null, tree),
            "pool": state.element_pool.iter().map(|element| tree(*element)).collect::<Vec<_>>(),
            "hasContainer": state.container.is_some(),
            "renders": self.view.render_calls.borrow().clone(),
            "clock": self.dom.clock.get(),
            "domScrollTop": self.dom.world.borrow().scroll_top,
            "pendingFrames": self.dom.frames.frame_ids().len(),
        })
    }
}

#[test]
fn code_view_shell_matches_the_published_class() {
    let steps = replay("s12e_code_view_shell.json", 100);
    assert!(steps > 1_000, "{steps} steps");
}

#[test]
fn code_view_render_pass_matches_the_published_class() {
    let steps = replay("s12f_code_view_render.json", 60);
    assert!(steps > 1_000, "{steps} steps");
}

/// Replays every scenario of a golden over `FakeDom`, comparing each step's
/// value, trace and state; returns the steps replayed.
fn replay(golden_file: &str, min_scenarios: usize) -> usize {
    let golden: Rc<Golden> = Rc::new(load_golden(golden_file));
    assert!(
        golden.cases.len() > min_scenarios,
        "{} scenarios",
        golden.cases.len()
    );
    let mut steps = 0;
    for case in &golden.cases {
        let scenario = &case.scenario;
        let dom = Rc::new(FakeDom::default());
        dom.recording().set(true);
        dom.shared.path_names.set(true);
        dom.device_pixel_ratio.set(scenario.env.dpr.unwrap_or(1.0));
        {
            let mut info = dom.platform_info.borrow_mut();
            info.user_agent = scenario.env.user_agent.clone().unwrap_or_default();
            info.platform = Some(
                scenario
                    .env
                    .platform
                    .clone()
                    .unwrap_or_else(|| "Linux x86_64".to_owned()),
            );
            info.max_touch_points = scenario.env.max_touch_points.unwrap_or(0);
        }
        let slots = golden
            .slots
            .iter()
            .map(|(name, value)| {
                let value = *value;
                let callback: Rc<dyn Fn() -> Option<u8>> = Rc::new(move || value);
                (name.clone(), PtrEq::new(callback))
            })
            .collect();
        let diffs = golden
            .diffs
            .iter()
            .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
            .collect();
        let placeholder = World {
            dom: Rc::clone(&dom),
            view: CodeView::new(
                Rc::clone(&dom),
                ResizeObserverHub::new(Rc::clone(&dom)),
                UniversalRenderingManager::new(Rc::clone(&dom)),
                Rc::new(PlainTextHighlighter),
                CodeViewOptions::default(),
            ),
            golden: Rc::clone(&golden),
            diffs,
            slots,
            root: std::cell::Cell::new(None),
            env_rules: scenario.env.rules,
            env_clamp: scenario.env.clamp,
            env_unclamped: scenario.env.unclamped,
        };
        let options = placeholder.options(&scenario.options);
        let view = CodeView::new(
            Rc::clone(&dom),
            ResizeObserverHub::new(Rc::clone(&dom)),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            Rc::new(PlainTextHighlighter),
            options,
        );
        let world = World {
            view,
            ..placeholder
        };
        // The constructor's writes are not part of any step.
        dom.shared.trace.borrow_mut().clear();
        for (index, step) in scenario.steps.iter().enumerate() {
            let context = format!("{} step {}", scenario.name, index + 1);
            let recorded = &case.steps[index];
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| world.apply(step)));
            let result = match result {
                Ok(value) => json!({ "value": value }),
                Err(payload) => json!({ "throws": panic_message(&*payload) }),
            };
            assert_json_close(
                &format!("{context} result"),
                &Value::Object(recorded.result.clone()),
                &result,
                1e-9,
            );
            let trace: Vec<Value> = dom
                .shared
                .trace
                .borrow_mut()
                .drain(..)
                .filter(|entry| TRACED.contains(&entry[0].as_str().unwrap_or_default()))
                .collect();
            assert_json_close(&context, &recorded.state, &world.snapshot(), 1e-9);
            assert_json_close(
                &format!("{context} trace"),
                &Value::Array(recorded.trace.clone()),
                &Value::Array(trace),
                1e-9,
            );
            steps += 1;
        }
    }
    steps
}
