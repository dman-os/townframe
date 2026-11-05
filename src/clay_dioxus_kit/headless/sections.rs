//! Sections: a horizontal run of panes separated by draggable handles.
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
//!   lands on however far the pointer travels. The overlay also ends the gesture when the
//!   pointer leaves the window, so a release outside the app cannot leave it stuck. On touch the
//!   browser captures the pointer implicitly to the handle, so the handle takes the same
//!   move/release handlers as the overlay.
//! * Positions come from `page_coordinates()`: the event target changes from handle to overlay
//!   mid-gesture, and element-relative coordinates would jump with it.
//! * `tabindex` panics Blitz, so keyboard resizing is opt-in through [`Split::keyboard`]: the
//!   web build turns it on, the native build leaves it off.
use crate::Layer;
use dioxus::prelude::*;
use std::rc::Rc;

/// One pane's size limits, in logical px along the split axis.
///
/// A pane whose size would fall below [`Snap::below`] while dragging jumps to [`Snap::to`]
/// instead: the old Kotlin `DockableRegion` sidebar used this to snap between an 80dp rail and
/// full width without ever showing an in-between sliver.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SizeRule {
    pub min: f64,
    pub max: f64,
    pub snap: Option<Snap>,
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
        }
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
    /// Whether handles take keyboard focus and arrow-key resizing.
    pub keyboard: bool,
}

impl Split {
    /// How far one arrow-key press moves a handle, in px.
    pub const KEYBOARD_STEP_PX: f64 = 16.0;

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
            keyboard: false,
        }
    }

    /// Limit pane `index`.
    pub fn rule(mut self, index: usize, rule: SizeRule) -> Self {
        self.rules[index] = rule;
        self
    }

    /// Enable keyboard resizing; see the module docs.
    pub fn keyboard(mut self, keyboard: bool) -> Self {
        self.keyboard = keyboard;
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

/// The classes a skin gives each part of [`Sections`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SectionsSkin {
    pub root: &'static str,
    pub pane: &'static str,
    /// The handle's hit area; it carries `data-state="idle"` or `"dragging"`.
    pub handle: &'static str,
    /// The visible line inside the hit area.
    pub grip: &'static str,
}

/// An in-flight drag: where it began and how far the pointer has travelled since.
#[derive(Clone, Copy)]
struct Drag {
    handle: usize,
    origin: f64,
    delta: f64,
}

/// Width of a handle's hit area, in px. It overlaps the neighbouring panes (negative margins), so
/// it takes no layout space. It is the one pressable narrower than the 48 px touch target
/// (`KIT-106`): a pane's own edge, with its scrollbar and first controls, lies under half of it.
/// This is the WCAG 2.5.8 minimum.
const HANDLE_HIT_PX: f64 = 24.0;

/// What a handle announces about the pair of panes it divides.
struct HandleView {
    label: String,
    now: f64,
    min: f64,
    max: f64,
}

#[component]
pub fn Sections(
    label: String,
    panes: Vec<SectionPane>,
    split: Split,
    on_resize: EventHandler<Vec<f64>>,
    #[props(default)] skin: SectionsSkin,
) -> Element {
    assert_eq!(
        panes.len(),
        split.weights.len(),
        "Sections needs one weight per pane"
    );
    let mut drag = use_signal(|| None::<Drag>);
    let split = Rc::new(split);
    // While dragging, lay out the weights the drag would report on release.
    let weights = match drag() {
        Some(drag) => split.dragged(drag.handle, drag.delta),
        None => split.weights.clone(),
    };
    let sizes = Split {
        weights: weights.clone(),
        ..(*split).clone()
    }
    .sizes();
    let percent = |px: f64| (px / split.extent * 100.0).round().min(100.0);
    let handles: Vec<HandleView> = (0..panes.len() - 1)
        .map(|index| {
            let (a, b) = (&split.rules[index], &split.rules[index + 1]);
            let pair = sizes[index] + sizes[index + 1];
            HandleView {
                label: format!("Resize {} and {}", panes[index].name, panes[index + 1].name),
                now: percent(sizes[index]),
                min: percent(a.min),
                max: percent(a.max.min(pair - b.min)),
            }
        })
        .collect();

    let track = use_callback(move |event: PointerEvent| {
        let x = event.page_coordinates().x;
        if let Some(drag) = drag.write().as_mut() {
            drag.delta = x - drag.origin;
        }
    });
    let release = use_callback({
        let split = Rc::clone(&split);
        move |()| {
            if let Some(drag) = drag.write().take() {
                on_resize.call(split.dragged(drag.handle, drag.delta));
            }
        }
    });
    let abort = use_callback(move |()| {
        drag.write().take();
    });
    let handle_style = format!(
        "position:relative;z-index:{};flex:none;touch-action:none;cursor:col-resize;width:{HANDLE_HIT_PX}px;margin:0 -{}px",
        Layer::Raised.css(),
        HANDLE_HIT_PX / 2.0
    );
    let gesture_style = format!(
        "position:fixed;top:0;right:0;bottom:0;left:0;z-index:{};cursor:col-resize;touch-action:none",
        Layer::Gesture.css()
    );
    let tabindex = split.keyboard.then_some("0");
    let dragging_handle = drag().map(|drag| drag.handle);

    rsx! {
        div {
            class: skin.root,
            role: "group",
            "aria-label": "{label}",
            style: "display:flex;flex-direction:row;width:100%;height:100%;min-width:0;min-height:0",
            for (index, pane) in panes.iter().enumerate() {
                div {
                    key: "pane-{index}",
                    class: skin.pane,
                    role: "region",
                    "aria-label": "{pane.name}",
                    style: pane_style(weights[index], &split.rules[index]),
                    {pane.content.clone()}
                }
                if let Some(view) = handles.get(index) {
                    div {
                        key: "handle-{index}",
                        class: skin.handle,
                        role: "separator",
                        "aria-orientation": "vertical",
                        "aria-label": "{view.label}",
                        "aria-valuenow": "{view.now}",
                        "aria-valuemin": "{view.min}",
                        "aria-valuemax": "{view.max}",
                        tabindex,
                        "data-state": if dragging_handle == Some(index) { "dragging" } else { "idle" },
                        style: "{handle_style}",
                        onpointerdown: move |event| {
                            event.prevent_default();
                            drag.set(Some(Drag {
                                handle: index,
                                origin: event.page_coordinates().x,
                                delta: 0.0,
                            }));
                        },
                        onpointermove: move |event| track.call(event),
                        onpointerup: move |_| release.call(()),
                        onpointercancel: move |_| abort.call(()),
                        onkeydown: {
                            let split = Rc::clone(&split);
                            move |event| {
                                let step = match event.key() {
                                    Key::Named(NamedKey::ArrowLeft) => -Split::KEYBOARD_STEP_PX,
                                    Key::Named(NamedKey::ArrowRight) => Split::KEYBOARD_STEP_PX,
                                    _ => return,
                                };
                                event.prevent_default();
                                on_resize.call(split.dragged(index, step));
                            }
                        },
                        div { class: skin.grip }
                    }
                }
            }
            if drag().is_some() {
                div {
                    role: "presentation",
                    style: "{gesture_style}",
                    onpointermove: move |event| track.call(event),
                    onpointerup: move |_| release.call(()),
                    onpointerleave: move |_| release.call(()),
                    onpointercancel: move |_| abort.call(()),
                }
            }
        }
    }
}

fn pane_style(weight: f64, rule: &SizeRule) -> String {
    let max = if rule.max.is_finite() {
        format!(";max-width:{}px", rule.max)
    } else {
        String::new()
    };
    format!(
        "flex:{weight} 1 0px;min-width:{}px;min-height:0;position:relative;z-index:{}{max}",
        rule.min,
        Layer::Content.css()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;
    use std::cell::RefCell;

    fn near(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }

    fn sizes_of(split: &Split, weights: Vec<f64>) -> Vec<f64> {
        Split {
            weights,
            ..split.clone()
        }
        .sizes()
    }

    #[test]
    fn sizes_split_the_extent_by_weight() {
        let split = Split::new(vec![1.0, 3.0], 800.0);
        assert_eq!(split.sizes(), [200.0, 600.0]);
    }

    #[test]
    fn a_drag_moves_the_boundary_and_keeps_the_pair_and_the_total() {
        let split = Split::new(vec![1.0, 1.0, 2.0], 800.0);
        let after = split.dragged(0, 50.0);
        assert_eq!(sizes_of(&split, after.clone()), [250.0, 150.0, 400.0]);
        near(after.iter().sum::<f64>(), 4.0);
        assert_eq!(after[2], 2.0, "panes outside the pair are untouched");
    }

    #[test]
    fn minimums_and_maximums_clamp_the_drag() {
        let split = Split::new(vec![1.0, 1.0], 400.0)
            .rule(
                0,
                SizeRule {
                    min: 120.0,
                    max: 260.0,
                    snap: None,
                },
            )
            .rule(
                1,
                SizeRule {
                    min: 100.0,
                    ..SizeRule::default()
                },
            );
        assert_eq!(sizes_of(&split, split.dragged(0, -500.0)), [120.0, 280.0]);
        assert_eq!(sizes_of(&split, split.dragged(0, 500.0)), [260.0, 140.0]);
        // The neighbour's minimum bounds the other direction.
        let tight = split.clone().rule(0, SizeRule::default());
        assert_eq!(sizes_of(&tight, tight.dragged(0, 500.0)), [300.0, 100.0]);
    }

    #[test]
    fn a_pane_below_its_snap_threshold_jumps_to_the_snap_size() {
        // The Kotlin sidebar: an 80px rail, or at least 240px, never in between.
        let rail = SizeRule {
            min: 80.0,
            max: f64::INFINITY,
            snap: Some(Snap {
                below: 160.0,
                to: 80.0,
            }),
        };
        let split = Split::new(vec![1.0, 3.0], 800.0).rule(0, rail);
        assert_eq!(
            sizes_of(&split, split.dragged(0, -60.0)),
            [140.0 - 60.0, 800.0 - 80.0]
        );
        assert_eq!(sizes_of(&split, split.dragged(0, 100.0))[0], 300.0);
        // Dragging the neighbour's edge the other way snaps the same pane when it is second.
        let flipped = Split::new(vec![3.0, 1.0], 800.0).rule(
            1,
            SizeRule {
                min: 80.0,
                max: f64::INFINITY,
                snap: Some(Snap {
                    below: 160.0,
                    to: 80.0,
                }),
            },
        );
        assert_eq!(sizes_of(&flipped, flipped.dragged(0, 50.0)), [720.0, 80.0]);
    }

    #[test]
    fn rules_that_cannot_both_hold_leave_the_weights_alone() {
        let split = Split::new(vec![1.0, 1.0], 100.0)
            .rule(
                0,
                SizeRule {
                    min: 80.0,
                    ..SizeRule::default()
                },
            )
            .rule(
                1,
                SizeRule {
                    min: 80.0,
                    ..SizeRule::default()
                },
            );
        assert_eq!(split.dragged(0, 10.0), split.weights);
    }

    #[test]
    #[should_panic(expected = "positive and finite")]
    fn zero_weights_are_a_programming_error() {
        Split::new(vec![1.0, 0.0], 100.0);
    }

    #[derive(Clone)]
    struct Probe {
        keyboard: bool,
        reports: Rc<RefCell<Vec<Vec<f64>>>>,
    }

    fn app(probe: Probe) -> Element {
        let reports = probe.reports;
        let mut weights = use_signal(|| vec![1.0, 3.0]);
        let split = Split::new(weights(), 800.0)
            .rule(
                0,
                SizeRule {
                    min: 100.0,
                    ..SizeRule::default()
                },
            )
            .keyboard(probe.keyboard);
        rsx! {
            Sections {
                label: "Shell",
                panes: vec![
                    SectionPane { name: "Sidebar".into(), content: rsx! { "side" } },
                    SectionPane { name: "Content".into(), content: rsx! { "main" } },
                ],
                split,
                on_resize: move |next: Vec<f64>| {
                    reports.borrow_mut().push(next.clone());
                    weights.set(next);
                },
            }
        }
    }

    fn render(keyboard: bool) -> (Page, Rc<RefCell<Vec<Vec<f64>>>>) {
        let reports = Rc::new(RefCell::new(vec![]));
        let probe = Probe {
            keyboard,
            reports: Rc::clone(&reports),
        };
        (Page::with_props(app, probe), reports)
    }

    #[test]
    fn panes_are_named_regions_and_the_handle_a_named_separator() {
        let (page, _) = render(false);
        page.by_role("group", "Shell");
        page.by_role("region", "Sidebar");
        page.by_role("region", "Content");
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        assert_eq!(page.attr(handle, "aria-valuenow").as_deref(), Some("25"));
        assert_eq!(page.attr(handle, "aria-valuemin").as_deref(), Some("13"));
        assert_eq!(page.attr(handle, "aria-valuemax").as_deref(), Some("100"));
        assert_eq!(page.attr(handle, "data-state").as_deref(), Some("idle"));
        let sidebar = page.by_role("region", "Sidebar");
        assert_eq!(page.style(sidebar, "flex").as_deref(), Some("1 1 0px"));
    }

    #[test]
    fn panes_handles_and_the_gesture_overlay_stack_on_the_layer_scale() {
        let (mut page, _) = render(false);
        let layer = |page: &Page, node| page.style(node, "z-index");
        // A pane is a stacking context of its own, so what an app bar inside it does cannot
        // climb over the handle that overlaps its edge.
        let pane = page.by_role("region", "Sidebar");
        assert_eq!(layer(&page, pane).as_deref(), Some(Layer::Content.css()));
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        assert_eq!(layer(&page, handle).as_deref(), Some(Layer::Raised.css()));
        // 24 px wide, centred on the seam: the one pressable under 48 px (`KIT-106`).
        assert_eq!(page.style(handle, "width").as_deref(), Some("24px"));
        assert_eq!(page.style(handle, "margin").as_deref(), Some("0 -12px"));
        page.pointer(handle, "pointerdown", 200.0);
        let overlay = page.by_role("presentation", "");
        assert_eq!(layer(&page, overlay).as_deref(), Some(Layer::Gesture.css()));
    }

    #[test]
    fn a_drag_previews_live_and_reports_once_on_release() {
        let (mut page, reports) = render(false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        assert_eq!(page.attr(handle, "data-state").as_deref(), Some("dragging"));
        // The overlay now takes the moves; it is the only presentation element.
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 260.0);
        let sidebar = page.by_role("region", "Sidebar");
        assert_eq!(page.style(sidebar, "flex").as_deref(), Some("1.3 1 0px"));
        assert!(reports.borrow().is_empty(), "nothing is reported mid-drag");
        page.pointer(overlay, "pointerup", 260.0);
        let reported = reports.borrow();
        assert_eq!(reported.len(), 1);
        assert_eq!(
            sizes_of(&Split::new(vec![1.0, 3.0], 800.0), reported[0].clone()),
            [260.0, 540.0]
        );
        drop(reported);
        assert!(!page.has_role("presentation", ""), "the overlay goes away");
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        assert_eq!(page.attr(handle, "data-state").as_deref(), Some("idle"));
    }

    #[test]
    fn a_drag_honours_the_pane_minimum() {
        let (mut page, reports) = render(false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", -500.0);
        page.pointer(overlay, "pointerup", -500.0);
        let reported = reports.borrow();
        assert_eq!(
            sizes_of(&Split::new(vec![1.0, 3.0], 800.0), reported[0].clone())[0],
            100.0
        );
    }

    #[test]
    fn leaving_the_window_ends_the_gesture_with_what_was_shown() {
        let (mut page, reports) = render(false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 230.0);
        page.pointer(overlay, "pointerleave", 230.0);
        assert_eq!(reports.borrow().len(), 1);
        assert!(!page.has_role("presentation", ""));
    }

    #[test]
    fn a_cancelled_gesture_reports_nothing() {
        let (mut page, reports) = render(false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        let overlay = page.by_role("presentation", "");
        page.pointer(overlay, "pointermove", 300.0);
        page.pointer(overlay, "pointercancel", 300.0);
        assert!(reports.borrow().is_empty());
        assert!(!page.has_role("presentation", ""));
        let sidebar = page.by_role("region", "Sidebar");
        assert_eq!(page.style(sidebar, "flex").as_deref(), Some("1 1 0px"));
    }

    #[test]
    fn touch_moves_that_stay_captured_by_the_handle_still_drag() {
        let (mut page, reports) = render(false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.pointer(handle, "pointerdown", 200.0);
        page.pointer(handle, "pointermove", 240.0);
        page.pointer(handle, "pointerup", 240.0);
        assert_eq!(reports.borrow().len(), 1);
    }

    #[test]
    fn handles_take_keyboard_focus_only_when_the_host_says_the_renderer_can() {
        let (page, _) = render(false);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        assert_eq!(page.attr(handle, "tabindex"), None);
        let (page, _) = render(true);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        assert_eq!(page.attr(handle, "tabindex").as_deref(), Some("0"));
    }

    #[test]
    fn arrow_keys_resize_by_a_step() {
        let (mut page, reports) = render(true);
        let handle = page.by_role("separator", "Resize Sidebar and Content");
        page.key(handle, Key::Named(NamedKey::ArrowRight));
        page.key(handle, Key::Named(NamedKey::Tab));
        let reported = reports.borrow();
        assert_eq!(reported.len(), 1, "only arrows resize");
        assert_eq!(
            sizes_of(&Split::new(vec![1.0, 3.0], 800.0), reported[0].clone()),
            [216.0, 584.0]
        );
    }
}
