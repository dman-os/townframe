use super::*;

const HEIGHT: f64 = 860.0;
const WIDTH: f64 = 1076.0;

/// The distance a label has to retreat into before it runs out of window.
fn room() -> f64 {
    WIDTH - REST_INSET - spacing(HEIGHT, breakpoints().len()) / 2.0
}

fn placed(engaged: bool) -> Vec<Placed> {
    let items = breakpoints();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let limit = push_limit(300.0, room());
    let floor = push_floor(limit);
    place(&items, 6.0, spacing, floor, limit, engaged)
}

/// The whole point of deriving spacing from the window: at rest every
/// breakpoint is on screen, rather than the strip running off the bottom
/// after J.
#[test]
fn resting_index_fits_every_breakpoint() {
    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let span = (count - 1) as f64 * spacing;
    let inset = top_inset(spacing, count, HEIGHT);

    assert!(span <= HEIGHT, "index spans {span} of {HEIGHT}");
    assert!(inset > 0.0, "index starts off screen at {inset}");
}

/// At rest the strip is an ordinary equally spaced list with no sideways
/// displacement at all.
#[test]
fn resting_strip_is_flat_and_uniform() {
    let placed = placed(false);

    assert!(
        placed.iter().all(|p| p.push == 0.0),
        "idle list must not bow"
    );

    let steps: Vec<f64> = placed.windows(2).map(|w| w[1].dy - w[0].dy).collect();
    assert!(
        steps.windows(2).all(|w| (w[0] - w[1]).abs() < 1e-9),
        "uneven: {steps:?}"
    );
}

/// The label under the thumb moves furthest, and the push decays away from
/// it on both sides.
#[test]
fn the_push_decays_with_distance_from_the_thumb() {
    let limit = push_limit(0.0, 900.0);
    let floor = push_floor(limit);
    let reach = reach_for(limit);
    assert_eq!(push_at(0.0, floor, limit, reach), limit);

    // Strictly decaying up to the reach, where the dent ends.
    let mut previous = f64::INFINITY;
    for fraction in [0.0, 0.25, 0.5, 0.99] {
        let push = push_at(fraction * reach, floor, limit, reach);
        assert!(push < previous, "push did not decay at {fraction}");
        previous = push;
    }

    // And flat beyond it: a label out of reach is not dragged along.
    for fraction in [1.0, 3.0, 100.0] {
        let distance = fraction * reach;
        assert_eq!(
            push_at(distance, floor, limit, reach),
            floor,
            "moved at {distance}"
        );
    }
}

/// Nothing is ever pushed past the far edge of the window, however hard the
/// thumb pulls, and nothing is ever pushed back inside the floor.
#[test]
fn the_push_stays_between_its_floor_and_the_window_edge() {
    let room = room();

    for thumb_inboard in [0.0, 100.0, 300.0, 5000.0] {
        let limit = push_limit(thumb_inboard, room);
        let floor = push_floor(limit);
        let reach = reach_for(limit);
        assert!(limit <= room, "limit {limit} left the window");

        for distance in [0.0, 1.0, 50.0, 190.0, 10_000.0] {
            let push = push_at(distance, floor, limit, reach);
            assert!(push <= limit, "push {push} exceeded its limit {limit}");
            assert!(push >= floor.min(limit), "push {push} fell below its floor");
        }
    }
}

/// The strip follows the thumb across the window rather than stopping part
/// way: a thumb on the edge barely moves anything, and a thumb dragged
/// inboard takes the labels with it until they run out of window.
#[test]
fn the_strip_follows_the_thumb_until_the_window_runs_out() {
    let room = room();

    // A thumb sitting right on the resting column clears it by the margin.
    assert_eq!(push_limit(0.0, room), PUSH_CLEARANCE);

    // Dragged inboard, the labels keep pace with the thumb.
    for inboard in [50.0, 150.0, 400.0] {
        assert_eq!(push_limit(inboard, room), inboard + PUSH_CLEARANCE);
    }

    // And it stops at the far edge of the window, not before.
    assert_eq!(push_limit(room * 4.0, room), room);

    // Which for a window this wide is far more than the old fixed cap.
    assert!(room > 900.0, "room was only {room}");
}

/// The regression this strip once had: its geometry took a window width that did not match the
/// pointer's coordinates, so in a window wider than the number it was given the thumb read as
/// "on the resting column" and the picked label never travelled past the clearance. Measured
/// from the box itself, a thumb at the far left of a wide box takes the label to the far edge,
/// with the labels out of reach trailing it by exactly the cap.
#[test]
fn a_thumb_at_the_far_left_of_a_wide_box_pushes_to_the_far_edge() {
    let (width, height) = (1920.0, HEIGHT);
    let items = breakpoints();
    let state = StripState {
        thumb_x: Some(12.0),
        ..StripState::default()
    };
    let geometry = Geometry::new((width, height), &items, &state);
    let room = width - REST_INSET - spacing(height, items.len()) * ACTIVE_SPACING_MULTIPLIER / 2.0;
    assert!(
        (geometry.limit - room).abs() < 1e-9,
        "the label stopped short of the far edge: {} of {room}",
        geometry.limit
    );
    assert!(
        geometry.limit > 1700.0,
        "pushed only {} of a {width} px box",
        geometry.limit
    );
    let pushes: Vec<f64> = geometry.placed.iter().map(|placed| placed.push).collect();
    let furthest = pushes.iter().copied().fold(f64::MIN, f64::max);
    let least = pushes.iter().copied().fold(f64::MAX, f64::min);
    assert_eq!(furthest, geometry.limit);
    // Out of reach the labels trail by the cap, never more: the floor is `limit - MAX_DIFF`
    // and nothing sits below it.
    assert_eq!(push_floor(geometry.limit), geometry.limit - MAX_DIFF);
    assert!(
        least >= geometry.limit - MAX_DIFF - 1e-6,
        "a label trails by {}, past the {MAX_DIFF} cap",
        furthest - least
    );

    // The same thumb in a narrower box reaches a nearer edge: the size comes from the box.
    let narrow = Geometry::new((720.0, height), &items, &state);
    assert!(narrow.limit < geometry.limit);
}

/// The floor is where the breakpoints out of reach sit. It has to stay on
/// the resting column for a short drag and then come along on a long one,
/// with the gap to the thumb never widening past the cap.
#[test]
fn the_floor_stays_on_the_resting_column_until_the_cap() {
    // A drag short enough that the gap is under the cap leaves the far end
    // completely alone: the strip is dented, not shifted.
    for limit in [PUSH_CLEARANCE, 120.0, MAX_DIFF] {
        assert_eq!(
            push_floor(limit),
            0.0,
            "far end moved at a limit of {limit}"
        );
    }

    // Past the cap it comes along, and the gap stops growing.
    for limit in [MAX_DIFF + 1.0, MAX_DIFF + 150.0, 5000.0] {
        let floor = push_floor(limit);
        assert_eq!(floor, limit - MAX_DIFF, "far end did not follow at {limit}");
        assert_eq!(limit - floor, MAX_DIFF, "gap was not capped at {limit}");
        assert!(
            floor < limit,
            "the picked breakpoint must still move furthest"
        );
    }
}

/// The dent reaches further along the strip the harder the drag, so pulling
/// the thumb right across lifts far more breakpoints than a short pull —
/// rather than only deepening the same small dent.
#[test]
fn the_dent_reaches_further_as_the_drag_goes() {
    let mut previous = 0.0;
    for limit in [PUSH_CLEARANCE, 150.0, 300.0, 600.0] {
        let reach = reach_for(limit);
        assert!(reach > previous, "reach did not grow at a limit of {limit}");
        previous = reach;
    }
    assert!(reach_for(PUSH_CLEARANCE) >= REACH_BASE);

    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let lifted = |limit: f64| {
        let (floor, reach) = (push_floor(limit), reach_for(limit));
        (0..count)
            .filter(|i| push_at(*i as f64 * spacing, floor, limit, reach) > floor + 1e-9)
            .count()
    };

    assert!(
        lifted(600.0) > lifted(150.0),
        "a longer drag should reach more breakpoints: {} vs {}",
        lifted(600.0),
        lifted(150.0)
    );
}

/// The breakpoint the pointer selects is the one sitting under it, so the
/// selection never drifts away from the finger.
#[test]
fn the_selected_breakpoint_is_the_one_under_the_pointer() {
    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let top = top_inset(spacing, count, HEIGHT);

    for index in 0..count {
        let centre = top + index as f64 * spacing;
        assert!(
            (focus_at(centre, top, spacing, count) - index as f64).abs() < 1e-9,
            "centre of {index} selected something else"
        );

        // Anywhere within half a spacing still belongs to that breakpoint.
        for offset in [-0.4, 0.4] {
            let selected = focus_at(centre + offset * spacing, top, spacing, count);
            assert!(
                (selected - index as f64).abs() < 0.5,
                "pointer {offset} away from {index} selected {selected}"
            );
        }
    }
}

/// Pressing a breakpoint must pick up *that* one, and must not move the list.
/// Anchoring the previous focus instead snapped the strip to wherever the
/// last gesture ended, so the letter under the finger was never the letter
/// you got.
#[test]
fn pressing_picks_the_breakpoint_under_the_finger() {
    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let top = top_inset(spacing, count, HEIGHT);

    // Pressing `L` must focus `L`, not whatever was focused before.
    let l = breakpoints().iter().position(|l| *l == "L").unwrap() as f64;
    let press_y = top + l * spacing;
    assert_eq!(focus_at(press_y, top, spacing, count), l);

    // And the press moves nothing: the anchor is exactly where the pointer
    // already was, so nothing shifts under the finger.
    let anchor = anchor_y(
        l,
        spacing,
        top,
        pull_for(press_y, top, spacing, count, HEIGHT),
    );
    assert!(
        (anchor - press_y).abs() < 1e-9,
        "press moved the list to {anchor}"
    );

    // Presses outside the index clamp rather than running off the ends.
    assert_eq!(focus_at(-500.0, top, spacing, count), 0.0);
    assert_eq!(
        focus_at(HEIGHT + 500.0, top, spacing, count),
        (count - 1) as f64
    );
}

/// The regression this model exists to fix: inside the index the list must
/// not move at all. Otherwise dragging up from the middle shifts the strip,
/// A leaves the top of the window, and it can never be reached again.
#[test]
fn the_list_only_travels_once_the_drag_passes_an_end() {
    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let top = top_inset(spacing, count, HEIGHT);

    // Anywhere within the index: no movement, so the whole list stays put.
    for index in [0.0, 6.0, 13.0, (count - 1) as f64] {
        let y = top + index * spacing;
        let pull = pull_for(y, top, spacing, count, HEIGHT);
        assert!(pull.abs() < 1e-9, "list moved {pull} at {index}");
    }

    // Only past the ends does it pull, and in the right direction.
    assert!(pull_for(top - 3.0 * spacing, top, spacing, count, HEIGHT) < 0.0);
    let below = top + (count as f64 + 2.0) * spacing;
    assert!(pull_for(below, top, spacing, count, HEIGHT) > 0.0);

    // One px of drag past the end is one px of pull, so it tracks the finger
    // rather than the list's spacing.
    let pulled = pull_for(below, top, spacing, count, HEIGHT)
        - pull_for(below - 1.0, top, spacing, count, HEIGHT);
    assert!((pulled - 1.0).abs() < 1e-9, "pull stepped by {pulled}");
}

/// The invariant this model is built on: wherever the thumb is, the picked
/// breakpoint is exactly under it — inside the index and out past either end
/// alike. The pull resolves from the same absolute pointer as the selection,
/// so the two can never disagree and the picked breakpoint cannot slide out
/// from under the finger.
#[test]
fn the_picked_breakpoint_stays_under_the_thumb() {
    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let top = top_inset(spacing, count, HEIGHT);

    for step in 0..=860 {
        let y = step as f64;
        let focus = focus_at(y, top, spacing, count);
        let anchor = anchor_y(
            focus,
            spacing,
            top,
            pull_for(y, top, spacing, count, HEIGHT),
        );
        assert!(
            (anchor - y).abs() < 1e-9,
            "picked breakpoint sat at {anchor} for a thumb at {y}"
        );
    }
}

/// And the index that the thumb drags stays in the window while it follows.
/// This holds because the resting index is centred: the slack at each end is
/// exactly the furthest the thumb can overshoot past it.
#[test]
fn the_index_stays_inside_the_window() {
    let count = breakpoints().len();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let top = top_inset(spacing, count, HEIGHT);
    let span = (count - 1) as f64 * spacing;

    // Including pointers that have left the window entirely, which is the
    // one case the slack alone does not cover.
    for step in -200..=1060 {
        let y = step as f64;
        let focus = focus_at(y, top, spacing, count);
        let anchor = anchor_y(
            focus,
            spacing,
            top,
            pull_for(y, top, spacing, count, HEIGHT),
        );

        let first = anchor - focus * spacing;
        let last = first + span;
        assert!(
            first >= -1e-9,
            "top left the window with the thumb at {y}: {first}"
        );
        assert!(
            last <= HEIGHT + 1e-9,
            "bottom left the window with the thumb at {y}"
        );
    }
}

/// Nothing is ever culled, so a run of neighbours can never drop one.
#[test]
fn neighbours_are_never_skipped() {
    let labels: Vec<&str> = placed(true).iter().map(|p| p.label).collect();
    assert_eq!(labels.len(), breakpoints().len());

    let start = labels.iter().position(|l| *l == "A").expect("A is visible");
    let end = labels.iter().position(|l| *l == "F").expect("F is visible");
    assert_eq!(&labels[start..=end], ["A", "B", "C", "D", "E", "F"]);
}

/// Any list length lays out inside the window while a finger is down: the pull's slack
/// (`height - top - span`) used to go negative past ~40 breakpoints, which made
/// `f64::clamp` panic on the first press.
#[test]
fn long_lists_fit_and_pulling_never_panics() {
    for count in [1, 2, REFERENCE_COUNT, REFERENCE_COUNT + 1, 60, 500] {
        // While engaged the strip spreads by `ACTIVE_SPACING_MULTIPLIER`.
        let spacing = spacing(HEIGHT, count) * ACTIVE_SPACING_MULTIPLIER;
        let top = top_inset(spacing, count, HEIGHT);
        let span = (count - 1) as f64 * spacing;
        assert!(
            top >= 0.0 && span <= HEIGHT,
            "{count}: top {top} span {span}"
        );
        for step in -200..=1060 {
            let y = f64::from(step);
            let pull = pull_for(y, top, spacing, count, HEIGHT);
            let focus = focus_at(y, top, spacing, count);
            let first = anchor_y(focus, spacing, top, pull) - focus * spacing;
            assert!(first >= -1e-9, "{count}: top left the window at {y}");
            assert!(first + span <= HEIGHT + 1e-9, "{count}: bottom left at {y}");
        }
    }
}

/// The resting strip of a list longer than the reference still spans the same share of the window.
#[test]
fn spacing_shrinks_so_a_long_list_spans_what_the_reference_one_does() {
    let reference = (REFERENCE_COUNT - 1) as f64 * spacing(HEIGHT, REFERENCE_COUNT);
    let long = 59.0 * spacing(HEIGHT, 60);
    assert!((reference - long).abs() < 1e-9, "{reference} vs {long}");
    // At or under the reference count the spacing is the window's alone.
    assert_eq!(spacing(HEIGHT, 3), spacing(HEIGHT, REFERENCE_COUNT));
}

mod transitions {
    //! [`StripState::next`] on its own: no `VirtualDom`, no signals, no timers.
    use super::*;

    const VIEW: (f64, f64) = (WIDTH, HEIGHT);
    const COUNT: usize = 28;

    /// The state after the events, and every breakpoint they reported, in order.
    fn run(state: &StripState, events: &[StripEvent]) -> (StripState, Vec<usize>) {
        let mut state = state.clone();
        let mut all = vec![];
        for event in events {
            let (next, selected) = state.next(VIEW, COUNT, *event);
            state = next;
            all.extend(selected);
        }
        (state, all)
    }

    /// The y of breakpoint `index` in the resting layout.
    fn y_of(index: usize) -> f64 {
        let spacing = spacing(HEIGHT, COUNT);
        top_inset(spacing, COUNT, HEIGHT) + index as f64 * spacing
    }

    fn pressed_at(index: usize) -> StripState {
        run(
            &StripState::default(),
            &[StripEvent::Press {
                x: 1050.0,
                y: y_of(index),
            }],
        )
        .0
    }

    #[test]
    fn a_press_engages_the_strip_on_the_breakpoint_under_it_and_reports_it_once() {
        let (state, effects) = run(
            &StripState::default(),
            &[StripEvent::Press {
                x: 1050.0,
                y: y_of(5),
            }],
        );
        assert!(state.engaged());
        assert_eq!(state.thumb_x, Some(1050.0));
        assert_eq!(state.focus.round() as usize, 5);
        assert_eq!(effects, [5]);
    }

    #[test]
    fn a_release_without_a_press_changes_nothing() {
        let rest = StripState::default();
        let (state, effects) = run(&rest, &[StripEvent::Release, StripEvent::Release]);
        assert_eq!(state, rest);
        assert!(effects.is_empty());
    }

    #[test]
    fn a_move_without_a_press_changes_nothing() {
        let rest = StripState::default();
        let (state, effects) = run(
            &rest,
            &[StripEvent::Drag {
                x: 1000.0,
                y: y_of(9),
            }],
        );
        assert_eq!(state, rest);
        assert!(effects.is_empty());
    }

    #[test]
    fn a_release_after_a_press_returns_the_strip_to_rest_at_once() {
        // Even from a pull past an end: the stylesheet's transition does the unbending.
        let pulled = run(&pressed_at(0), &[StripEvent::Drag { x: 900.0, y: -50.0 }]).0;
        assert!(pulled.pull < 0.0);
        let (state, selected) = run(&pulled, &[StripEvent::Release]);
        assert_eq!((state.thumb_x, state.pull), (None, 0.0));
        assert!(!state.engaged());
        // Releasing keeps the selection and reports nothing new.
        assert_eq!(state.focus, pulled.focus);
        assert!(selected.is_empty());
    }

    #[test]
    fn a_second_release_changes_nothing() {
        let released = run(&pressed_at(3), &[StripEvent::Release]).0;
        let (again, selected) = run(&released, &[StripEvent::Release]);
        assert_eq!(again, released);
        assert!(selected.is_empty());
    }

    #[test]
    fn a_press_clears_the_hover_label() {
        let hovering = run(&StripState::default(), &[StripEvent::Hover { y: y_of(7) }]).0;
        assert_eq!(hovering.hovered, Some(7));
        let (pressed, _) = run(
            &hovering,
            &[StripEvent::Press {
                x: 1050.0,
                y: y_of(7),
            }],
        );
        assert_eq!(pressed.hovered, None);
    }

    #[test]
    fn dragging_past_an_end_pulls_and_inside_the_index_does_not() {
        // A drag is laid out with the engaged (wider) spacing.
        let Metrics { spacing, top, .. } = Metrics::of(VIEW, COUNT, true);
        let inside = run(
            &pressed_at(4),
            &[StripEvent::Drag {
                x: 1000.0,
                y: top + 10.0 * spacing,
            }],
        )
        .0;
        assert_eq!(inside.pull, 0.0);
        let past = run(
            &pressed_at(4),
            &[StripEvent::Drag {
                x: 1000.0,
                y: top - 30.0,
            }],
        )
        .0;
        assert!(past.pull < 0.0, "{}", past.pull);
        assert_eq!(past.focus, 0.0);
    }

    #[test]
    fn nudges_and_wheels_clamp_to_the_list_and_report_each_new_breakpoint_once() {
        let (state, effects) = run(
            &StripState::default(),
            &[
                StripEvent::Nudge(1.0),
                StripEvent::Nudge(1.0),
                StripEvent::Nudge(-1000.0),
                StripEvent::Nudge(1000.0),
                StripEvent::Nudge(1000.0),
            ],
        );
        assert_eq!(state.focus, (COUNT - 1) as f64);
        assert_eq!(effects, [1, 2, 0, COUNT - 1]);
        let spacing = spacing(HEIGHT, COUNT);
        let (wheeled, _) = run(
            &StripState::default(),
            &[StripEvent::Wheel {
                travel: 3.0 * spacing,
            }],
        );
        assert_eq!(wheeled.focus.round(), 3.0);
    }

    #[test]
    fn hovering_labels_the_breakpoint_only_while_not_pressed() {
        let (hovered, _) = run(&StripState::default(), &[StripEvent::Hover { y: y_of(7) }]);
        assert_eq!(hovered.hovered, Some(7));
        let (left, _) = run(&hovered, &[StripEvent::HoverEnd]);
        assert_eq!(left.hovered, None);
        let (held, _) = run(&pressed_at(2), &[StripEvent::Hover { y: y_of(7) }]);
        assert_eq!(held.hovered, None);
    }

    #[test]
    fn a_strip_at_rest_follows_the_owner_without_announcing() {
        let (state, effects) = run(&StripState::default(), &[StripEvent::Follow(9)]);
        assert_eq!(state.focus, 9.0);
        assert!(effects.is_empty());
        assert_eq!(state.announced, None);
        // The list may be shorter than the index the owner reports.
        let (clamped, _) = run(&StripState::default(), &[StripEvent::Follow(COUNT + 5)]);
        assert_eq!(clamped.focus, (COUNT - 1) as f64);
    }

    #[test]
    fn a_strip_under_a_thumb_keeps_where_the_owner_is_for_when_it_rests() {
        let held = pressed_at(2);
        let (state, effects) = run(&held, &[StripEvent::Follow(9)]);
        assert_eq!(state.focus.round(), 2.0, "the thumb owns the focus");
        assert!(effects.is_empty());
        assert_eq!(state.follow, Some(9));
        let (rested, _) = run(&state, &[StripEvent::Release]);
        assert_eq!(rested.thumb_x, None);
        assert_eq!(rested.focus, 9.0);
        assert_eq!(rested.follow, None);
    }

    #[test]
    fn a_focus_left_past_a_shrunken_list_is_clamped_by_the_next_event() {
        let far = StripState {
            focus: 20.0,
            ..StripState::default()
        };
        let (state, _) = far.next(VIEW, 3, StripEvent::Nudge(1.0));
        assert_eq!(state.focus, 2.0);
    }
}

mod rendered {
    //! The component rendered into the harness's retained DOM, as the lab drives it: one
    //! `ScrubStrip` whose items the parent swaps when the tab changes, and the vocabulary a
    //! stylesheet keys on.
    use super::*;
    use crate::testing::Page;
    use dioxus::html::geometry::PixelsRect;
    use dioxus::html::geometry::euclid::Point2D;
    use dioxus::html::geometry::euclid::Size2D;
    use std::cell::RefCell;
    use std::rc::Rc;

    const TABS: [&[&str]; 3] = [
        &["Component kit"],
        &["Typography", "Spacing", "Shape", "Surfaces"],
        &["Colors", "Extra"],
    ];

    #[derive(Clone)]
    struct Probe {
        renderer: Renderer,
        selected: Rc<RefCell<Vec<usize>>>,
    }

    fn app(probe: Probe) -> Element {
        let mut tab = use_signal(|| 0usize);
        let selected = probe.selected;
        rsx! {
            for index in 0..TABS.len() {
                button { key: "{index}", onclick: move |_| tab.set(index), "tab {index}" }
            }
            ScrubStrip {
                items: TABS[tab()].to_vec(),
                renderer: probe.renderer,
                on_select: move |index| selected.borrow_mut().push(index),
            }
        }
    }

    fn render(renderer: Renderer) -> (Page, Rc<RefCell<Vec<usize>>>) {
        let selected = Rc::new(RefCell::new(Vec::new()));
        let probe = Probe {
            renderer,
            selected: Rc::clone(&selected),
        };
        (Page::with_props(app, probe), selected)
    }

    /// The overlay's mount, reporting `rect` as its box, as a browser would.
    fn measure(page: &mut Page, origin: (f64, f64), size: (f64, f64)) {
        let root = page.by_class("ct-scrub-strip");
        page.mount_measured(
            root,
            PixelsRect::new(
                Point2D::new(origin.0, origin.1),
                Size2D::new(size.0, size.1),
            ),
        );
    }

    fn markers(page: &Page) -> Vec<String> {
        let root = page.by_class("ct-scrub-strip");
        page.children(root)
            .into_iter()
            .filter(|part| page.attr(*part, "data-part").as_deref() == Some("marker"))
            .map(|marker| page.text(marker))
            .collect()
    }

    #[test]
    fn the_strip_is_named_parts_and_vocabulary_only() {
        let (page, _) = render(Renderer::Blitz);
        let root = page.by_class("ct-scrub-strip");
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("idle"));
        // Every child is a part (`parts` fails on one that is not), in paint order.
        assert_eq!(
            page.parts(root),
            ["scrim", "marker", "bubble", "grab-band", "status"]
        );
        for property in [
            "--ct-strip-cell",
            "--ct-strip-inset",
            "--ct-strip-anchor",
            "--ct-strip-limit",
        ] {
            assert!(page.custom_property(root, property).is_some(), "{property}");
        }
        // At rest nothing is pushed and nothing reaches out.
        assert_eq!(
            page.custom_property(root, "--ct-strip-limit").as_deref(),
            Some("0px")
        );
        let marker = page.children(root)[1];
        assert_eq!(
            page.custom_property(marker, "--ct-strip-push").as_deref(),
            Some("0px")
        );
        assert_eq!(page.parts(marker), ["tick", "label"]);
    }

    #[test]
    fn the_strip_is_one_slider_whose_value_is_the_breakpoint() {
        let (page, _) = render(Renderer::Blitz);
        let slider = page.by_role("slider", "Index");
        assert_eq!(
            page.attr(slider, "aria-orientation").as_deref(),
            Some("vertical")
        );
        assert_eq!(page.attr(slider, "aria-valuemin").as_deref(), Some("1"));
        assert_eq!(page.attr(slider, "aria-valuemax").as_deref(), Some("1"));
        assert_eq!(page.attr(slider, "aria-valuenow").as_deref(), Some("1"));
        assert_eq!(
            page.attr(slider, "aria-valuetext").as_deref(),
            Some("Component kit")
        );
        // The labels are decorative: the slider and the live region carry the meaning.
        let root = page.by_class("ct-scrub-strip");
        let marker = page.children(root)[1];
        assert_eq!(page.attr(marker, "aria-hidden").as_deref(), Some("true"));
    }

    #[test]
    fn the_slider_is_a_tab_stop_only_where_the_renderer_takes_focus() {
        let (page, _) = render(Renderer::Browser);
        assert_eq!(
            page.attr(page.by_role("slider", "Index"), "tabindex")
                .as_deref(),
            Some("0")
        );
        let (page, _) = render(Renderer::Blitz);
        assert_eq!(page.attr(page.by_role("slider", "Index"), "tabindex"), None);
    }

    #[test]
    fn the_strip_follows_the_items_the_parent_passes() {
        let (mut page, _) = render(Renderer::Blitz);
        assert_eq!(markers(&page).len(), 1);
        let second = page.by_role("button", "tab 1");
        page.click(second);
        let labels = markers(&page);
        assert_eq!(labels.len(), 4, "{labels:?}");
        // A marker holds the tick and the label, so its text is the label.
        assert_eq!(labels[0], "Typography");
        let slider = page.by_role("slider", "Index");
        assert_eq!(page.attr(slider, "aria-valuemax").as_deref(), Some("4"));
        let third = page.by_role("button", "tab 2");
        page.click(third);
        let slider = page.by_role("slider", "Index");
        assert_eq!(page.attr(slider, "aria-valuemax").as_deref(), Some("2"));
    }

    fn on_tab(page: &mut Page, tab: usize) {
        let button = page.by_role("button", &format!("tab {tab}"));
        page.click(button);
    }

    #[test]
    fn the_keys_scrub_report_each_breakpoint_once_and_move_the_slider() {
        let (mut page, selected) = render(Renderer::Browser);
        on_tab(&mut page, 1);
        let slider = page.by_role("slider", "Index");
        page.key(slider, Key::Named(NamedKey::ArrowDown));
        page.key(slider, Key::Named(NamedKey::PageDown));
        page.key(slider, Key::Named(NamedKey::ArrowRight));
        let slider = page.by_role("slider", "Index");
        assert_eq!(page.attr(slider, "aria-valuenow").as_deref(), Some("4"));
        assert_eq!(
            page.attr(slider, "aria-valuetext").as_deref(),
            Some("Surfaces")
        );
        // The end clamps and is not reported again.
        page.key(slider, Key::Named(NamedKey::ArrowDown));
        page.key(slider, Key::Named(NamedKey::Home));
        page.key(slider, Key::Named(NamedKey::ArrowUp));
        page.key(slider, Key::Named(NamedKey::End));
        assert_eq!(*selected.borrow(), [1, 2, 3, 0, 3]);
        let slider = page.by_role("slider", "Index");
        assert_eq!(page.attr(slider, "aria-valuenow").as_deref(), Some("4"));
        // The live region speaks the last breakpoint.
        let root = page.by_class("ct-scrub-strip");
        let status = *page.children(root).last().expect("a status part");
        assert_eq!(page.text(status), "Surfaces");
    }

    #[test]
    fn a_key_that_is_not_a_step_is_left_alone() {
        let (mut page, selected) = render(Renderer::Browser);
        on_tab(&mut page, 1);
        let slider = page.by_role("slider", "Index");
        page.key(slider, Key::Character("x".into()));
        page.key(slider, Key::Named(NamedKey::Enter));
        assert!(selected.borrow().is_empty());
    }

    #[test]
    fn pressing_engages_and_releasing_returns_to_rest_with_the_labels_at_the_edge() {
        let (mut page, selected) = render(Renderer::Browser);
        measure(&mut page, (0.0, 0.0), (1076.0, 860.0));
        on_tab(&mut page, 1);
        let root = page.by_class("ct-scrub-strip");
        let band = page.by_role("slider", "Index");
        page.pointer_at(band, "pointerdown", (1050.0, 430.0));
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("engaged"));
        assert!(!selected.borrow().is_empty());
        // The thumb is 26 px inboard of the resting column, so something is pushed, and the
        // active label gets a bubble.
        assert_ne!(
            page.custom_property(root, "--ct-strip-limit").as_deref(),
            Some("0px")
        );
        // The bubble is always mounted; the stylesheet shows it while engaged.
        let parts = page.parts(root);
        assert!(parts.contains(&"bubble".to_string()), "{parts:?}");
        page.pointer_at(root, "pointerup", (1050.0, 430.0));
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("idle"));
        assert_eq!(
            page.custom_property(root, "--ct-strip-limit").as_deref(),
            Some("0px")
        );
        // The bubble stays for the stylesheet to slide it back to the column; it is pushed no
        // further.
        let bubble = page
            .children(root)
            .into_iter()
            .find(|part| page.attr(*part, "data-part").as_deref() == Some("bubble"))
            .expect("the bubble stays mounted at rest");
        assert_eq!(
            page.custom_property(bubble, "--ct-strip-push").as_deref(),
            Some("0px")
        );
        for marker in page
            .children(root)
            .into_iter()
            .filter(|part| page.attr(*part, "data-part").as_deref() == Some("marker"))
        {
            assert_eq!(
                page.custom_property(marker, "--ct-strip-push").as_deref(),
                Some("0px")
            );
        }
    }

    /// The overlay need not start at the client origin (a micro's mount may be offset): the
    /// pointer is read relative to the measured box, so a thumb 26 px inboard of the box's right
    /// edge reads the same wherever the box sits.
    #[test]
    fn pointers_are_read_relative_to_the_measured_box() {
        let limit_after_press = |origin_x: f64| {
            let (mut page, _) = render(Renderer::Browser);
            measure(&mut page, (origin_x, 0.0), (1076.0, 860.0));
            let root = page.by_class("ct-scrub-strip");
            let band = page.by_role("slider", "Index");
            page.pointer_at(band, "pointerdown", (origin_x + 1050.0, 430.0));
            page.custom_property(root, "--ct-strip-limit")
        };
        assert_eq!(limit_after_press(0.0), limit_after_press(300.0));
    }

    /// A strip that was never measured (Blitz, which has no layout to read) still engages and
    /// picks; its labels just have no window to travel into.
    #[test]
    fn an_unmeasured_strip_still_picks() {
        let (mut page, selected) = render(Renderer::Blitz);
        let band = page.by_role("slider", "Index");
        page.pointer_at(band, "pointerdown", (1050.0, 430.0));
        assert!(!selected.borrow().is_empty());
    }

    /// A release with no press of the strip's own (a scrollbar drag let go over the grab band)
    /// leaves the strip at rest: it never turns its `pointer-events` on.
    #[test]
    fn a_release_without_a_press_leaves_the_strip_at_rest() {
        let (mut page, selected) = render(Renderer::Blitz);
        let root = page.by_class("ct-scrub-strip");
        let band = page.by_role("slider", "Index");
        for target in [root, band] {
            page.pointer_at(target, "pointerup", (1050.0, 400.0));
            page.pointer_at(target, "pointercancel", (1050.0, 400.0));
        }
        let root = page.by_class("ct-scrub-strip");
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("idle"));
        assert!(selected.borrow().is_empty());
    }

    #[test]
    fn hovering_the_band_reveals_the_labels_and_marks_the_one_under_the_pointer() {
        let (mut page, _) = render(Renderer::Blitz);
        on_tab(&mut page, 1);
        let band = page.by_role("slider", "Index");
        page.pointer_at(band, "pointermove", (1050.0, 430.0));
        let root = page.by_class("ct-scrub-strip");
        // No press started a gesture, so the move over the band is a hover, not a drag.
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("hover"));
        let hovered = page
            .children(root)
            .into_iter()
            .filter(|part| page.attr(*part, "data-hover").is_some())
            .count();
        assert_eq!(hovered, 1);
        page.pointer_at(band, "pointerleave", (1050.0, 430.0));
        let root = page.by_class("ct-scrub-strip");
        assert_eq!(page.attr(root, "data-state").as_deref(), Some("idle"));
    }

    /// A wheel over the strip steps it without panicking: the handler reads the focus before
    /// committing the next one, which writes it.
    #[test]
    fn a_wheel_over_the_strip_steps_without_a_double_borrow() {
        let (mut page, selected) = render(Renderer::Blitz);
        on_tab(&mut page, 1);
        let root = page.by_class("ct-scrub-strip");
        page.wheel(root, 120.0);
        assert!(!selected.borrow().is_empty());
    }

    /// The lab's parent: the tabs swap the items under a live strip, so a storm also changes
    /// the props mid-gesture.
    fn tabbed() -> Element {
        let mut tab = use_signal(|| 0usize);
        rsx! {
            for index in 0..TABS.len() {
                button { key: "{index}", onclick: move |_| tab.set(index), "tab {index}" }
            }
            ScrubStrip {
                items: TABS[tab()].to_vec(),
                renderer: Renderer::Browser,
                on_select: move |_| {},
            }
        }
    }

    /// Every listener of the strip, fed every event kind it takes in seeded random order, with
    /// the items swapped under it.
    #[test]
    fn a_storm_of_events_in_any_order_never_panics() {
        let dispatched = dioxus_storm::Storm::of(tabbed)
            .viewport(1076.0, 860.0)
            .run_seeds(0..64);
        for kind in [
            "pointerdown",
            "pointermove",
            "pointerup",
            "pointerleave",
            "wheel",
            "keydown",
            "click",
        ] {
            assert!(
                dispatched.contains_key(kind),
                "{kind} was never dispatched: {dispatched:?}"
            );
        }
    }
}
