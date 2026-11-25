use super::*;
use crate::testing::Page;
use dioxus_storm::Storm;
use std::cell::RefCell;

/// A rule with a minimum and nothing else.
fn at_least(min: f64) -> SizeRule {
    SizeRule {
        min,
        ..SizeRule::default()
    }
}

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
                max: 260.0,
                ..at_least(120.0)
            },
        )
        .rule(1, at_least(100.0));
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
        collapse_past: None,
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
            collapse_past: None,
        },
    );
    assert_eq!(sizes_of(&flipped, flipped.dragged(0, 50.0)), [720.0, 80.0]);
}

#[test]
fn rules_that_cannot_both_hold_leave_the_weights_alone() {
    let split = Split::new(vec![1.0, 1.0], 100.0)
        .rule(0, at_least(80.0))
        .rule(1, at_least(80.0));
    assert_eq!(split.dragged(0, 10.0), split.weights);
}

#[test]
#[should_panic(expected = "positive and finite")]
fn zero_weights_are_a_programming_error() {
    Split::new(vec![1.0, 0.0], 100.0);
}

/// The one report a gesture made, as pane sizes.
fn reported_sizes(reports: &RefCell<Vec<Vec<f64>>>) -> Vec<f64> {
    let reports = reports.borrow();
    assert_eq!(reports.len(), 1);
    sizes_of(&Split::new(vec![1.0, 3.0], 800.0), reports[0].clone())
}

#[derive(Clone)]
struct Probe {
    renderer: Renderer,
    reports: Rc<RefCell<Vec<Vec<f64>>>>,
}

fn app(probe: Probe) -> Element {
    let reports = probe.reports;
    let mut weights = use_signal(|| vec![1.0, 3.0]);
    let split = Split::new(weights(), 800.0)
        .rule(0, at_least(100.0))
        .renderer(probe.renderer);
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

fn render(renderer: Renderer) -> (Page, Rc<RefCell<Vec<Vec<f64>>>>) {
    let reports = Rc::new(RefCell::new(vec![]));
    let probe = Probe {
        renderer,
        reports: Rc::clone(&reports),
    };
    (Page::with_props(app, probe), reports)
}

#[test]
fn panes_are_named_regions_and_the_handle_a_named_separator() {
    let (page, _) = render(Renderer::Blitz);
    page.by_role("group", "Shell");
    page.by_role("region", "Sidebar");
    page.by_role("region", "Content");
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    assert_eq!(page.attr(handle, "aria-valuenow").as_deref(), Some("25"));
    assert_eq!(page.attr(handle, "aria-valuemin").as_deref(), Some("13"));
    assert_eq!(page.attr(handle, "aria-valuemax").as_deref(), Some("100"));
    assert_eq!(page.attr(handle, "data-state").as_deref(), Some("idle"));
    let sidebar = page.by_role("region", "Sidebar");
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-weight")
            .as_deref(),
        Some("1")
    );
}

#[test]
fn the_sections_name_their_parts_and_hand_their_geometry_to_the_stylesheet() {
    let (mut page, _) = render(Renderer::Blitz);
    let root = page.by_class("ct-sections");
    // A pane, then the handle that divides it from the next: the seam is a part of the run.
    assert_eq!(page.parts(root), ["sections-pane", "sections-handle", "sections-pane"]);
    let sidebar = page.by_role("region", "Sidebar");
    assert_eq!(page.attr(sidebar, "data-state").as_deref(), Some("open"));
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-weight")
            .as_deref(),
        Some("1")
    );
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-min")
            .as_deref(),
        Some("100px")
    );
    // No limit, no custom property: the stylesheet's own default is "none".
    assert_eq!(page.custom_property(sidebar, "--ct-sections-max"), None);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    assert_eq!(page.parts(handle), ["sections-grip"]);
    assert_eq!(page.attr(handle, "data-edge"), None);
    page.pointer(handle, "pointerdown", 200.0);
    let overlay = page.by_role("presentation", "");
    assert_eq!(page.attr(overlay, "part").as_deref(), Some("sections-gesture"));
    assert_eq!(page.parts(root), ["sections-pane", "sections-handle", "sections-pane", "sections-gesture"]);
}

#[test]
fn a_limited_pane_hands_over_its_maximum_in_px() {
    fn limited() -> Element {
        rsx! {
            Sections {
                label: "Shell",
                panes: vec![
                    SectionPane { name: "Sidebar".into(), content: rsx! { "side" } },
                    SectionPane { name: "Content".into(), content: rsx! { "main" } },
                ],
                split: Split::new(vec![1.0, 3.0], 800.0).rule(
                    0,
                    SizeRule { min: 160.0, max: 480.0, ..SizeRule::default() },
                ),
                on_resize: |_| {},
            }
        }
    }
    let page = Page::with_props(|()| limited(), ());
    let sidebar = page.by_role("region", "Sidebar");
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-min")
            .as_deref(),
        Some("160px")
    );
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-max")
            .as_deref(),
        Some("480px")
    );
}

#[test]
fn a_drag_previews_live_and_reports_once_on_release() {
    let (mut page, reports) = render(Renderer::Blitz);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    page.pointer(handle, "pointerdown", 200.0);
    assert_eq!(page.attr(handle, "data-state").as_deref(), Some("dragging"));
    // The overlay now takes the moves; it is the only presentation element.
    let overlay = page.by_role("presentation", "");
    page.pointer(overlay, "pointermove", 260.0);
    let sidebar = page.by_role("region", "Sidebar");
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-weight")
            .as_deref(),
        Some("1.3")
    );
    assert!(reports.borrow().is_empty(), "nothing is reported mid-drag");
    page.pointer(overlay, "pointerup", 260.0);
    assert_eq!(reported_sizes(&reports), [260.0, 540.0]);
    assert!(!page.has_role("presentation", ""), "the overlay goes away");
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    assert_eq!(page.attr(handle, "data-state").as_deref(), Some("idle"));
}

#[test]
fn a_drag_honours_the_pane_minimum() {
    let (mut page, reports) = render(Renderer::Blitz);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    page.pointer(handle, "pointerdown", 200.0);
    let overlay = page.by_role("presentation", "");
    page.pointer(overlay, "pointermove", -500.0);
    page.pointer(overlay, "pointerup", -500.0);
    assert_eq!(reported_sizes(&reports)[0], 100.0);
}

/// Press the handle, move to 230 and leave the window.
fn drag_and_leave(renderer: Renderer) -> (Page, Rc<RefCell<Vec<Vec<f64>>>>) {
    let (mut page, reports) = render(renderer);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    page.pointer(handle, "pointerdown", 200.0);
    let overlay = page.by_role("presentation", "");
    page.pointer(overlay, "pointermove", 230.0);
    page.pointer(overlay, "pointerleave", 230.0);
    (page, reports)
}

#[test]
fn on_blitz_leaving_the_window_ends_the_gesture_with_what_was_shown() {
    let (page, reports) = drag_and_leave(Renderer::Blitz);
    near(reported_sizes(&reports)[0], 230.0);
    assert!(!page.has_role("presentation", ""));
}

#[test]
fn in_a_browser_leaving_the_window_keeps_the_gesture_until_release() {
    let (mut page, reports) = drag_and_leave(Renderer::Browser);
    assert!(reports.borrow().is_empty(), "leaving does not end the drag");
    let overlay = page.by_role("presentation", "");
    page.pointer(overlay, "pointermove", 260.0);
    page.pointer(overlay, "pointerup", 260.0);
    assert_eq!(reported_sizes(&reports), [260.0, 540.0]);
}

#[test]
fn in_a_browser_a_move_with_no_button_held_ends_the_gesture_with_what_was_shown() {
    // A Wry release outside the window never arrives; the pointer comes back unpressed.
    let (mut page, reports) = drag_and_leave(Renderer::Browser);
    let overlay = page.by_role("presentation", "");
    page.pointer_without_buttons(overlay, 400.0);
    near(reported_sizes(&reports)[0], 230.0);
    assert!(!page.has_role("presentation", ""));
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    assert_eq!(page.attr(handle, "data-state").as_deref(), Some("idle"));
}

#[test]
fn a_cancelled_gesture_reports_nothing() {
    let (mut page, reports) = render(Renderer::Blitz);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    page.pointer(handle, "pointerdown", 200.0);
    let overlay = page.by_role("presentation", "");
    page.pointer(overlay, "pointermove", 300.0);
    page.pointer(overlay, "pointercancel", 300.0);
    assert!(reports.borrow().is_empty());
    assert!(!page.has_role("presentation", ""));
    let sidebar = page.by_role("region", "Sidebar");
    assert_eq!(
        page.custom_property(sidebar, "--ct-sections-weight")
            .as_deref(),
        Some("1")
    );
}

#[test]
fn touch_moves_that_stay_captured_by_the_handle_still_drag() {
    let (mut page, reports) = render(Renderer::Blitz);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    page.pointer(handle, "pointerdown", 200.0);
    page.pointer(handle, "pointermove", 240.0);
    page.pointer(handle, "pointerup", 240.0);
    assert_eq!(reports.borrow().len(), 1);
}

#[test]
fn handles_take_keyboard_focus_only_when_the_host_says_the_renderer_can() {
    let (page, _) = render(Renderer::Blitz);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    assert_eq!(page.attr(handle, "tabindex"), None);
    let (page, _) = render(Renderer::Browser);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    assert_eq!(page.attr(handle, "tabindex").as_deref(), Some("0"));
}

#[test]
fn arrow_keys_resize_by_a_step() {
    let (mut page, reports) = render(Renderer::Browser);
    let handle = page.by_role("separator", "Resize Sidebar and Content");
    page.key(handle, Key::Named(NamedKey::ArrowRight));
    page.key(handle, Key::Named(NamedKey::Tab));
    assert_eq!(reported_sizes(&reports), [216.0, 584.0]);
}

/// A controlled owner, as a shell writes one: it keeps the weights and the fold, and answers
/// every callback the component makes.
fn owner(renderer: Renderer) -> Element {
    let mut weights = use_signal(|| vec![1.0, 2.0, 1.0]);
    let mut folded = use_signal(|| false);
    let rail = SizeRule {
        min: 80.0,
        snap: Some(Snap {
            below: 160.0,
            to: 80.0,
        }),
        collapse_past: Some(40.0),
        ..SizeRule::default()
    };
    let split = Split::new(weights(), 900.0)
        .rule(0, rail)
        .rule(
            1,
            SizeRule {
                min: 120.0,
                ..SizeRule::default()
            },
        )
        .rule(
            2,
            SizeRule {
                min: 120.0,
                max: 500.0,
                ..SizeRule::default()
            },
        )
        .renderer(renderer);
    let panes = ["Rail", "Main", "Aside"]
        .map(|name| SectionPane {
            name: name.into(),
            content: rsx! { button { "{name}" } },
        })
        .to_vec();
    rsx! {
        Sections {
            label: "Workspace",
            panes,
            split,
            on_resize: move |next| weights.set(next),
            on_collapse: move |_| folded.set(true),
            on_expand: move |()| folded.set(false),
            folded: folded(),
        }
    }
}

#[test]
fn sections_survive_any_order_of_events_in_both_renderers() {
    for renderer in [Renderer::Browser, Renderer::Blitz] {
        Storm::with_props(owner, renderer)
            .viewport(900.0, 600.0)
            .steps(400)
            .run_seeds(0..64);
    }
}
