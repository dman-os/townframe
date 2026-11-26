//! Sections (`ct-sections`): a horizontal run of panes separated by draggable handles.
//!
//! Parts: `pane` (state `data-state`: `open`, or `collapsed` while a drag would shut it), `handle`
//! (state `data-state`: `idle`|`dragging`; the strip that reopens a folded first pane is also a
//! handle and carries `data-edge`) with a `grip` in it, and `gesture`, the overlay that takes a
//! live drag's pointer events. A pane carries its geometry as the custom properties
//! `--ct-sections-weight`, `--ct-sections-min` and, when it has a limit, `--ct-sections-max`
//! (px); the stylesheet turns them into the layout, the handles' 24px hit area and the layers.
//!
//! The layout is controlled. The parent owns the [`Split`] (weights, the host-measured extent and
//! the sizing rules) and hears of a new set of weights through `on_resize`; during a drag the
//! component previews the weights it would report, and reports them once, on release
//! (`KIT-021`).
//!
//! # Renderer limits
//!
//! * No DOM measuring (`KIT-040`): converting pointer travel into weights needs the pixel
//!   extent, so the host passes it in [`Split::extent`] instead of the component asking.
//! * No pointer capture on Blitz: a drag that leaves the handle stops delivering events. While a
//!   drag is live the component therefore mounts a window-sized overlay, which every later move
//!   lands on however far the pointer travels. Where [`Renderer::captures_pointer`] is false
//!   (Blitz) the overlay also ends the gesture when the pointer leaves the window, so a release
//!   outside the app cannot leave it stuck. A browser engine keeps delivering a pressed
//!   pointer's events after it leaves the window, so there the gesture ends only on
//!   `pointerup`/`pointercancel`. On touch the browser captures the pointer implicitly to the
//!   handle, so the handle takes the same move/release handlers as the overlay.
//! * A Wry webview does not deliver `pointerup` when the button is released outside the window,
//!   so the drag would still be live when the pointer comes back. Where
//!   [`Renderer::captures_pointer`] is true a `pointermove` whose held buttons no longer include
//!   the primary one therefore ends the gesture exactly as `pointerup` does, with the weights
//!   last shown. Blitz does not report held buttons on moves, so it keeps the leave rule above.
//! * Positions come from `page_coordinates()`: the event target changes from handle to overlay
//!   mid-gesture, and element-relative coordinates would jump with it.
//! * No measuring also means the edge strip of a folded first pane reads the pointer's page x as
//!   the width to open at: the strip lies on the host's left edge, so page x is the distance from
//!   it.
//! * `tabindex` panics Blitz, so keyboard resizing is gated on [`Renderer::takes_focus`], which the host
//!   sets through [`Split::renderer`]: browser engines turn it on, Blitz leaves it off.
use dioxus::html::input_data::MouseButton;
use dioxus::prelude::*;
use std::rc::Rc;

/// One pane's size limits, in logical px along the split axis.
///
/// A pane whose size would fall below [`Snap::below`] while dragging jumps to [`Snap::to`]
/// instead: the old Kotlin `DockableRegion` sidebar used this to snap between an 80dp rail and
/// full width without ever showing an in-between sliver.
///
/// A pane with [`SizeRule::collapse_past`] can also be dragged shut: once the drag has taken it
/// that many px below `min` it previews at 0 and the release reports a collapse (`on_collapse`)
/// instead of weights, so the weights, and with them the width it reopens at, are untouched.
/// A collapse is checked before a snap.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SizeRule {
    pub min: f64,
    pub max: f64,
    pub snap: Option<Snap>,
    /// How far below `min`, in px, a drag must take the pane for it to collapse; `None` never
    /// collapses.
    pub collapse_past: Option<f64>,
}

/// See [`SizeRule`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Snap {
    pub below: f64,
    pub to: f64,
}

impl Default for SizeRule {
    fn default() -> Self {
        Self {
            min: 0.0,
            max: f64::INFINITY,
            snap: None,
            collapse_past: None,
        }
    }
}

/// The engine a host renders with, so the kit can work around what that engine lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Renderer {
    /// A browser engine (Wry webview, the web build).
    Browser,
    /// The native Blitz renderer (desktop native, Android).
    Blitz,
}

impl Renderer {
    /// Whether elements take keyboard focus; Blitz panics on `tabindex`.
    pub fn takes_focus(self) -> bool {
        matches!(self, Self::Browser)
    }

    /// Whether a pressed pointer keeps delivering events outside the window; Blitz has no pointer capture.
    pub fn captures_pointer(self) -> bool {
        matches!(self, Self::Browser)
    }
}

/// The controlled layout state of a run of panes.
#[derive(Clone, Debug, PartialEq)]
pub struct Split {
    /// Relative sizes, one per pane. Only the ratios matter.
    pub weights: Vec<f64>,
    /// The px length of the split axis, from the host that knows its viewport.
    pub extent: f64,
    /// Limits per pane.
    pub rules: Vec<SizeRule>,
    /// The renderer the split runs in; see [`Renderer`] and the module docs.
    pub renderer: Renderer,
}

impl Split {
    /// How far one arrow-key press moves a handle, in px.
    pub const KEYBOARD_STEP_PX: f64 = 16.0;

    /// The px an arrow key moves a handle: left is negative, right positive.
    fn keyboard_step(key: &Key) -> Option<f64> {
        match key {
            Key::Named(NamedKey::ArrowLeft) => Some(-Self::KEYBOARD_STEP_PX),
            Key::Named(NamedKey::ArrowRight) => Some(Self::KEYBOARD_STEP_PX),
            _ => None,
        }
    }

    /// Weights over `extent` px, with no limits.
    pub fn new(weights: Vec<f64>, extent: f64) -> Self {
        assert!(
            !weights.is_empty() && weights.iter().all(|w| w.is_finite() && *w > 0.0),
            "split weights must be positive and finite, got {weights:?}"
        );
        assert!(
            extent.is_finite() && extent > 0.0,
            "split extent must be positive and finite, got {extent}"
        );
        let rules = vec![SizeRule::default(); weights.len()];
        Self {
            weights,
            extent,
            rules,
            renderer: Renderer::Blitz,
        }
    }

    /// Limit pane `index`.
    pub fn rule(mut self, index: usize, rule: SizeRule) -> Self {
        self.rules[index] = rule;
        self
    }

    /// Name the renderer so the split can use what it supports; see the module docs.
    pub fn renderer(mut self, renderer: Renderer) -> Self {
        self.renderer = renderer;
        self
    }

    /// Each pane's size in px.
    pub fn sizes(&self) -> Vec<f64> {
        let total: f64 = self.weights.iter().sum();
        self.weights
            .iter()
            .map(|weight| weight / total * self.extent)
            .collect()
    }

    /// The pane that moving `handle` by `delta` px would collapse (see [`SizeRule`]): the first
    /// when the drag takes it more than its `collapse_past` below its minimum, the second when it
    /// takes the second pane that far.
    pub fn collapses(&self, handle: usize, delta: f64) -> Option<usize> {
        assert!(delta.is_finite(), "drag delta must be finite, got {delta}");
        let sizes = self.sizes();
        let (a, b) = (&self.rules[handle], &self.rules[handle + 1]);
        let pair = sizes[handle] + sizes[handle + 1];
        let first = sizes[handle] + delta;
        let past = |rule: &SizeRule, size: f64| {
            rule.collapse_past
                .is_some_and(|distance| size < rule.min - distance)
        };
        if past(a, first) {
            Some(handle)
        } else if past(b, pair - first) {
            Some(handle + 1)
        } else {
            None
        }
    }

    /// What a drag shows: the weights [`Self::dragged`] would report, or, while the drag
    /// [collapses](Self::collapses) a pane, that pane at weight 0 with its neighbour in the pair
    /// taking its share.
    pub fn preview(&self, handle: usize, delta: f64) -> Vec<f64> {
        let Some(shut) = self.collapses(handle, delta) else {
            return self.dragged(handle, delta);
        };
        let other = if shut == handle { handle + 1 } else { handle };
        let mut weights = self.weights.clone();
        weights[other] += weights[shut];
        weights[shut] = 0.0;
        weights
    }

    /// The weights after moving `handle` (the one between panes `handle` and `handle + 1`) by
    /// `delta` px. The pair keeps its combined size; both panes honour their rules, a snap
    /// applies, and the other panes are untouched. A pair whose rules cannot both hold in the
    /// space it has (a window narrower than the minimums) does not move.
    pub fn dragged(&self, handle: usize, delta: f64) -> Vec<f64> {
        assert!(delta.is_finite(), "drag delta must be finite, got {delta}");
        let sizes = self.sizes();
        let (a, b) = (&self.rules[handle], &self.rules[handle + 1]);
        let pair = sizes[handle] + sizes[handle + 1];
        let lowest = a.min.max(pair - b.max);
        let highest = a.max.min(pair - b.min);
        if lowest > highest {
            return self.weights.clone();
        }
        let mut first = (sizes[handle] + delta).clamp(lowest, highest);
        if let Some(snap) = a.snap.filter(|snap| first < snap.below) {
            first = snap.to.clamp(lowest, highest);
        } else if let Some(snap) = b.snap.filter(|snap| pair - first < snap.below) {
            first = (pair - snap.to).clamp(lowest, highest);
        }
        let total: f64 = self.weights.iter().sum();
        let mut weights = self.weights.clone();
        weights[handle] = first / self.extent * total;
        weights[handle + 1] = (pair - first) / self.extent * total;
        weights
    }
}

/// One pane of [`Sections`].
#[derive(Clone, Debug, PartialEq)]
pub struct SectionPane {
    /// The pane's accessible name, also used to name the handles beside it.
    pub name: String,
    pub content: Element,
}

/// An in-flight drag: where it began and how far the pointer has travelled since. A drag from the
/// edge strip of a folded first pane is a drag of handle 0 whose pane has no size yet: the pointer's
/// x is the width it would open at.
#[derive(Clone, Copy)]
struct Drag {
    handle: usize,
    origin: f64,
    delta: f64,
    from_edge: bool,
}

/// What releasing a drag asks of the parent.
enum Release {
    Resize(Vec<f64>),
    Collapse(usize),
    /// New weights, then the pane opens.
    Expand(Vec<f64>),
    Nothing,
}

impl Drag {
    /// A drag of `handle` that began with the pointer at page `x`.
    fn press(handle: usize, x: f64, from_edge: bool) -> Self {
        Self {
            handle,
            origin: x,
            delta: 0.0,
            from_edge,
        }
    }

    fn release(self, split: &Split) -> Release {
        let travel = self.travel(split);
        match (split.collapses(self.handle, travel), self.from_edge) {
            (Some(pane), false) => Release::Collapse(pane),
            (None, false) => Release::Resize(split.dragged(self.handle, travel)),
            (None, true) => Release::Expand(split.dragged(self.handle, travel)),
            (Some(_), true) => Release::Nothing,
        }
    }

    /// The travel `split` measures the drag by: the pointer's travel, or, from the edge, how far
    /// the pointer's x is from the pane's stored width.
    fn travel(self, split: &Split) -> f64 {
        if self.from_edge {
            self.origin + self.delta - split.sizes()[0]
        } else {
            self.delta
        }
    }
}

/// What a handle announces about the pair of panes it divides.
struct HandleView {
    label: String,
    now: f64,
    min: f64,
    max: f64,
}

impl HandleView {
    /// The views of the handles between `panes`, which `sizes` (px) lays out in `split`.
    fn between(panes: &[SectionPane], split: &Split, sizes: &[f64]) -> Vec<Self> {
        let percent = |px: f64| (px / split.extent * 100.0).round().min(100.0);
        (0..panes.len() - 1)
            .map(|index| {
                let (a, b) = (&split.rules[index], &split.rules[index + 1]);
                let pair = sizes[index] + sizes[index + 1];
                Self {
                    label: format!("Resize {} and {}", panes[index].name, panes[index + 1].name),
                    now: percent(sizes[index]),
                    min: percent(a.min),
                    max: percent(a.max.min(pair - b.min)),
                }
            })
            .collect()
    }
}

/// What a render lays out: the drag's preview of the weights, and which panes show.
struct Layout {
    weights: Vec<f64>,
    /// `weights` in px.
    sizes: Vec<f64>,
    /// The pane the drag in flight collapses.
    shut: Option<usize>,
    /// A folded first pane shows only while the edge strip's drag would open it.
    first_shown: bool,
}

impl Layout {
    fn of(split: &Split, drag: Option<Drag>, folded: bool) -> Self {
        let travel = drag.map(|drag| (drag, drag.travel(split)));
        let shut = travel.and_then(|(drag, travel)| split.collapses(drag.handle, travel));
        // While dragging, lay out what the drag would report on release.
        let weights = match travel {
            Some((drag, travel)) => split.preview(drag.handle, travel),
            None => split.weights.clone(),
        };
        let sizes = Split {
            weights: weights.clone(),
            ..split.clone()
        }
        .sizes();
        Self {
            weights,
            sizes,
            shut,
            first_shown: !folded || drag.is_some_and(|drag| drag.from_edge && shut.is_none()),
        }
    }
}

/// `folded` folds the first pane away: it is not rendered, its handle goes, and an edge strip on
/// the leading edge takes their place. Dragging the strip previews the pane opening at the
/// pointer's x (within its rule); the release past the rule's collapse distance reports the new
/// weights through `on_resize` and then `on_expand`, and short of it nothing. Enter or Right
/// Arrow on a focusable strip calls `on_expand` alone, so the pane reopens at its stored width.
/// Folding is the parent's state: it folds in answer to `on_collapse` and unfolds in answer to
/// `on_expand`.
#[component]
pub fn Sections(
    label: String,
    panes: Vec<SectionPane>,
    split: Split,
    on_resize: EventHandler<Vec<f64>>,
    /// Called with the pane a drag released while it collapsed; see [`SizeRule::collapse_past`].
    #[props(default)]
    on_collapse: EventHandler<usize>,
    #[props(default)] on_expand: EventHandler<()>,
    #[props(default)] folded: bool,
) -> Element {
    assert_eq!(
        panes.len(),
        split.weights.len(),
        "Sections needs one weight per pane"
    );
    let mut drag = use_signal(|| None::<Drag>);
    let split = Rc::new(split);
    let captures_pointer = split.renderer.captures_pointer();
    let Layout {
        weights,
        sizes,
        shut,
        first_shown,
    } = Layout::of(&split, drag(), folded);
    let handles = HandleView::between(&panes, &split, &sizes);

    let release = use_callback({
        let split = Rc::clone(&split);
        move |()| {
            let Some(drag) = drag.write().take() else {
                return;
            };
            drag.release(&split)
                .report(on_resize, on_collapse, on_expand);
        }
    });
    let abort = use_callback(move |()| {
        drag.write().take();
    });
    let track = use_callback(move |event: PointerEvent| {
        // A release outside a Wry window never arrives; the pointer coming back with no button
        // held is how it shows (see the module docs).
        if captures_pointer && !event.held_buttons().contains(MouseButton::Primary) {
            release.call(());
            return;
        }
        let x = event.page_coordinates().x;
        if let Some(drag) = drag.write().as_mut() {
            drag.delta = x - drag.origin;
        }
    });
    let tabindex = split.renderer.takes_focus().then_some("0");
    let dragging = drag();
    let dragging_handle = dragging
        .filter(|drag| !drag.from_edge)
        .map(|drag| drag.handle);
    let edge_dragging = dragging.is_some_and(|drag| drag.from_edge);

    rsx! {
        div {
            class: "ct-sections",
            role: "group",
            "aria-label": "{label}",
            for (index, pane) in panes.iter().enumerate() {
                if index > 0 || first_shown {
                    PaneView {
                        key: "pane-{index}",
                        pane: pane.clone(),
                        weight: weights[index],
                        rule: split.rules[index],
                        collapsed: shut == Some(index),
                    }
                }
                if let Some(view) = handles.get(index).filter(|_| index > 0 || first_shown) {
                    div {
                        key: "handle-{index}",
                        "part": "sections-handle",
                        role: "separator",
                        "aria-orientation": "vertical",
                        "aria-label": "{view.label}",
                        "aria-valuenow": "{view.now}",
                        "aria-valuemin": "{view.min}",
                        "aria-valuemax": "{view.max}",
                        tabindex,
                        "data-state": if dragging_handle == Some(index) { "dragging" } else { "idle" },
                        onpointerdown: move |event| {
                            event.prevent_default();
                            drag.set(Some(Drag::press(index, event.page_coordinates().x, false)));
                        },
                        onpointermove: move |event| track.call(event),
                        onpointerup: move |_| release.call(()),
                        onpointercancel: move |_| abort.call(()),
                        onkeydown: {
                            let split = Rc::clone(&split);
                            move |event| {
                                let Some(step) = Split::keyboard_step(&event.key()) else {
                                    return;
                                };
                                event.prevent_default();
                                on_resize.call(split.dragged(index, step));
                            }
                        },
                        div { "part": "sections-grip" }
                    }
                }
            }
            if folded {
                div {
                    "part": "sections-handle",
                    "data-edge": "",
                    role: "separator",
                    "aria-orientation": "vertical",
                    "aria-label": "Open {panes[0].name}",
                    "aria-valuenow": "0",
                    "aria-valuemin": "0",
                    "aria-valuemax": "{handles[0].max}",
                    tabindex,
                    "data-state": if edge_dragging { "dragging" } else { "idle" },
                    onpointerdown: move |event| {
                        event.prevent_default();
                        drag.set(Some(Drag::press(0, event.page_coordinates().x, true)));
                    },
                    onpointermove: move |event| track.call(event),
                    onpointerup: move |_| release.call(()),
                    onpointercancel: move |_| abort.call(()),
                    onkeydown: move |event| {
                        if matches!(
                            event.key(),
                            Key::Named(NamedKey::Enter | NamedKey::ArrowRight)
                        ) {
                            event.prevent_default();
                            on_expand.call(());
                        }
                    },
                    div { "part": "sections-grip" }
                }
            }
            if drag().is_some() {
                div {
                    "part": "sections-gesture",
                    role: "presentation",
                    onpointermove: move |event| track.call(event),
                    onpointerup: move |_| release.call(()),
                    onpointerleave: move |_| {
                        if !captures_pointer {
                            release.call(());
                        }
                    },
                    onpointercancel: move |_| abort.call(()),
                }
            }
        }
    }
}

/// One pane: its content, and its geometry as the custom properties the stylesheet reads.
#[component]
fn PaneView(pane: SectionPane, weight: f64, rule: SizeRule, collapsed: bool) -> Element {
    // A collapsed pane lays out at 0 whatever its minimum says.
    let min = if collapsed { 0.0 } else { rule.min };
    let max = if rule.max.is_finite() {
        format!("; --ct-sections-max: {}px", rule.max)
    } else {
        String::new()
    };
    rsx! {
        div {
            "part": "sections-pane",
            "data-state": if collapsed { "collapsed" } else { "open" },
            role: "region",
            "aria-label": "{pane.name}",
            style: "--ct-sections-weight: {weight}; --ct-sections-min: {min}px{max}",
            {pane.content}
        }
    }
}

impl Release {
    /// Tells the parent what the gesture amounted to.
    fn report(
        self,
        on_resize: EventHandler<Vec<f64>>,
        on_collapse: EventHandler<usize>,
        on_expand: EventHandler<()>,
    ) {
        match self {
            Release::Resize(weights) => on_resize.call(weights),
            Release::Collapse(pane) => on_collapse.call(pane),
            Release::Expand(weights) => {
                on_resize.call(weights);
                on_expand.call(());
            }
            Release::Nothing => {}
        }
    }
}

#[cfg(test)]
mod tests;

/// Dragging a pane shut and reopening it from the edge strip.
#[cfg(test)]
mod collapse_tests {
    use super::*;
    use crate::testing::Page;
    use std::cell::RefCell;

    /// The sidebar rule of a shell: 160 to 480, collapsing 48 px below the minimum (so at 112).
    const SIDEBAR: SizeRule = SizeRule {
        min: 160.0,
        max: 480.0,
        snap: None,
        collapse_past: Some(48.0),
    };

    #[derive(Debug, PartialEq)]
    enum Heard {
        Resized(Vec<f64>),
        Collapsed(usize),
        Expanded,
    }

    #[derive(Clone)]
    struct Probe {
        renderer: Renderer,
        folded: bool,
        heard: Rc<RefCell<Vec<Heard>>>,
    }

    /// The parent a shell is: it folds on a collapse and unfolds on an expand.
    fn app(probe: Probe) -> Element {
        let heard = probe.heard;
        let (resized, collapsed, expanded) =
            (Rc::clone(&heard), Rc::clone(&heard), Rc::clone(&heard));
        let mut weights = use_signal(|| vec![1.0, 3.0]);
        let mut folded = use_signal(|| probe.folded);
        rsx! {
            Sections {
                label: "Shell",
                panes: vec![
                    SectionPane { name: "Sidebar".into(), content: rsx! { "side" } },
                    SectionPane { name: "Content".into(), content: rsx! { "main" } },
                ],
                split: Split::new(weights(), 800.0).rule(0, SIDEBAR).renderer(probe.renderer),
                folded: folded(),
                on_resize: move |next: Vec<f64>| {
                    resized.borrow_mut().push(Heard::Resized(next.clone()));
                    weights.set(next);
                },
                on_collapse: move |pane| {
                    collapsed.borrow_mut().push(Heard::Collapsed(pane));
                    folded.set(true);
                },
                on_expand: move |()| {
                    expanded.borrow_mut().push(Heard::Expanded);
                    folded.set(false);
                },
            }
        }
    }

    fn render(renderer: Renderer, folded: bool) -> (Page, Rc<RefCell<Vec<Heard>>>) {
        let heard = Rc::new(RefCell::new(vec![]));
        let probe = Probe {
            renderer,
            folded,
            heard: Rc::clone(&heard),
        };
        (Page::with_props(app, probe), heard)
    }

    /// The sidebar's width in px as the reported weights lay it out over 800 px.
    fn sidebar_px(weights: &[f64]) -> f64 {
        weights[0] / (weights[0] + weights[1]) * 800.0
    }

    fn resized_sidebar(heard: &RefCell<Vec<Heard>>) -> f64 {
        match &heard.borrow()[0] {
            Heard::Resized(weights) => sidebar_px(weights),
            other => panic!("expected a resize first, heard {other:?}"),
        }
    }

    #[test]
    fn a_drag_collapses_a_pane_only_past_its_collapse_distance() {
        let split = Split::new(vec![1.0, 3.0], 800.0).rule(0, SIDEBAR);
        // 200 px now: -80 leaves 120, above the 112 threshold; -100 leaves 100, below it.
        assert_eq!(split.collapses(0, -80.0), None);
        assert_eq!(sidebar_px(&split.preview(0, -80.0)), 160.0);
        assert_eq!(split.collapses(0, -100.0), Some(0));
        assert_eq!(split.preview(0, -100.0), [0.0, 4.0]);
        assert_eq!(split.dragged(0, -100.0), split.dragged(0, -80.0));
        // A rule without the distance never collapses.
        let fixed = Split::new(vec![1.0, 3.0], 800.0).rule(
            0,
            SizeRule {
                collapse_past: None,
                ..SIDEBAR
            },
        );
        assert_eq!(fixed.collapses(0, -190.0), None);
    }

    #[test]
    fn dragging_past_the_distance_previews_the_pane_shut_and_reports_a_collapse_not_weights() {
        let (mut page, heard) = render(Renderer::Blitz, false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 100.0);
        let sidebar = page.by_role("region", "Sidebar");
        assert_eq!(
            page.custom_property(sidebar, "--ct-sections-weight")
                .as_deref(),
            Some("0")
        );
        assert_eq!(
            page.custom_property(sidebar, "--ct-sections-min")
                .as_deref(),
            Some("0px")
        );
        // Coming back above the threshold un-snaps it to the minimum.
        page.pointer(overlay, "pointermove", 130.0);
        assert_eq!(
            page.custom_property(sidebar, "--ct-sections-min")
                .as_deref(),
            Some("160px")
        );
        page.pointer(overlay, "pointermove", 20.0);
        page.pointer(overlay, "pointerup", 20.0);
        assert_eq!(*heard.borrow(), [Heard::Collapsed(0)]);
        assert!(!page.has_role("region", "Sidebar"), "the parent folded it");
        // Nothing was written, so the previous width survives for reopening.
        page.pointer(
            page.by_role("separator", "Open Sidebar"),
            "pointerdown",
            1.0,
        );
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 200.0);
        page.pointer(overlay, "pointerup", 200.0);
        assert_eq!(
            heard.borrow()[1..],
            [Heard::Resized(vec![1.0, 3.0]), Heard::Expanded]
        );
    }

    #[test]
    fn a_drag_that_stays_above_the_distance_resizes_at_the_minimum() {
        let (mut page, heard) = render(Renderer::Blitz, false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 120.0);
        page.pointer(overlay, "pointerup", 120.0);
        assert_eq!(resized_sidebar(&heard), 160.0);
        assert_eq!(heard.borrow().len(), 1);
    }

    #[test]
    fn a_folded_pane_is_an_edge_strip_that_opens_it_at_the_pointer_within_its_rule() {
        let (mut page, heard) = render(Renderer::Blitz, true);
        assert!(!page.has_role("region", "Sidebar"));
        assert!(!page.has_role("separator", "Resize Sidebar and Content"));
        let strip = page.by_role("separator", "Open Sidebar");
        assert_eq!(page.attr(strip, "data-edge").as_deref(), Some(""));
        assert_eq!(page.attr(strip, "part").as_deref(), Some("sections-handle"));
        page.pointer(strip, "pointerdown", 2.0);
        assert_eq!(page.attr(strip, "data-state").as_deref(), Some("dragging"));
        let overlay = page.by_role("presentation", "");
        // Short of the threshold the pane stays folded; past it the pane previews at the pointer.
        page.pointer(overlay, "pointermove", 100.0);
        assert!(!page.has_role("region", "Sidebar"));
        page.pointer(overlay, "pointermove", 300.0);
        let sidebar = page.by_role("region", "Sidebar");
        assert_eq!(
            page.custom_property(sidebar, "--ct-sections-weight")
                .as_deref(),
            Some("1.5")
        );
        assert!(heard.borrow().is_empty(), "nothing is reported mid-drag");
        // Between the threshold and the minimum it opens at the minimum; beyond the maximum, at it.
        page.pointer(overlay, "pointermove", 130.0);
        let sidebar = page.by_role("region", "Sidebar");
        assert_eq!(
            page.custom_property(sidebar, "--ct-sections-min")
                .as_deref(),
            Some("160px")
        );
        page.pointer(overlay, "pointermove", 700.0);
        page.pointer(overlay, "pointerup", 700.0);
        assert_eq!(resized_sidebar(&heard), 480.0);
        assert_eq!(heard.borrow()[1], Heard::Expanded);
        assert!(page.has_role("region", "Sidebar"));
        assert!(!page.has_role("separator", "Open Sidebar"));
    }

    #[test]
    fn releasing_the_edge_strip_short_of_the_threshold_leaves_the_pane_folded() {
        let (mut page, heard) = render(Renderer::Blitz, true);
        let strip = page.by_role("separator", "Open Sidebar");
        page.pointer(strip, "pointerdown", 2.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 100.0);
        page.pointer(overlay, "pointerup", 100.0);
        assert!(heard.borrow().is_empty());
        assert!(!page.has_role("presentation", ""));
        assert!(!page.has_role("region", "Sidebar"));
        assert!(page.has_role("separator", "Open Sidebar"));
    }

    #[test]
    fn the_edge_strip_takes_focus_and_reopens_from_the_keyboard_only_in_a_browser() {
        let (page, _) = render(Renderer::Blitz, true);
        let strip = page.by_role("separator", "Open Sidebar");
        assert_eq!(page.attr(strip, "tabindex"), None);
        for key in [
            Key::Named(NamedKey::Enter),
            Key::Named(NamedKey::ArrowRight),
        ] {
            let (mut page, heard) = render(Renderer::Browser, true);
            let strip = page.by_role("separator", "Open Sidebar");
            assert_eq!(page.attr(strip, "tabindex").as_deref(), Some("0"));
            page.key(strip, Key::Named(NamedKey::Tab));
            page.key(strip, key);
            // The stored width, with no new weights.
            assert_eq!(*heard.borrow(), [Heard::Expanded]);
            assert!(page.has_role("region", "Sidebar"));
        }
    }

    #[test]
    fn in_a_browser_the_edge_strip_drag_ends_when_the_pointer_returns_unpressed() {
        let (mut page, heard) = render(Renderer::Browser, true);
        let strip = page.by_role("separator", "Open Sidebar");
        page.pointer(strip, "pointerdown", 2.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 300.0);
        page.pointer_without_buttons(overlay, 500.0);
        assert_eq!(resized_sidebar(&heard), 300.0);
        assert_eq!(heard.borrow()[1], Heard::Expanded);
    }
}
