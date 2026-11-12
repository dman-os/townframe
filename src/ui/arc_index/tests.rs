use super::*;

const HEIGHT: f64 = 860.0;
const WIDTH: f64 = 1076.0;

/// The distance a label has to retreat into before it runs out of window.
fn room() -> f64 {
    WIDTH - REST_INSET - spacing(HEIGHT, breakpoints().len()) / 2.0
}

fn placed(settle: f64) -> Vec<Placed> {
    let items = breakpoints();
    let spacing = spacing(HEIGHT, breakpoints().len());
    let limit = push_limit(300.0, room());
    let floor = push_floor(limit);
    place(&items, 6.0, spacing, floor, limit, settle)
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
    let placed = placed(0.0);

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
    let labels: Vec<&str> = placed(1.0).iter().map(|p| p.label).collect();
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
    //! [`ArcState::next`] on its own: no `VirtualDom`, no signals, no timers.
    use super::*;

    const VIEW: (f64, f64) = (WIDTH, HEIGHT);
    const COUNT: usize = 28;

    fn run(state: &ArcState, events: &[ArcEvent]) -> (ArcState, Vec<Effect>) {
        let mut state = state.clone();
        let mut all = vec![];
        for event in events {
            let (next, effects) = state.next(VIEW, COUNT, *event);
            state = next;
            all.extend(effects);
        }
        (state, all)
    }

    /// The y of breakpoint `index` in the resting layout.
    fn y_of(index: usize) -> f64 {
        let spacing = spacing(HEIGHT, COUNT);
        top_inset(spacing, COUNT, HEIGHT) + index as f64 * spacing
    }

    fn pressed_at(index: usize) -> ArcState {
        run(
            &ArcState::default(),
            &[ArcEvent::Press {
                x: 1050.0,
                y: y_of(index),
            }],
        )
        .0
    }

    #[test]
    fn a_press_engages_the_strip_on_the_breakpoint_under_it_and_reports_it_once() {
        let (state, effects) = run(
            &ArcState::default(),
            &[
                ArcEvent::Press {
                    x: 1050.0,
                    y: y_of(5),
                },
                ArcEvent::Drag {
                    x: 1040.0,
                    y: y_of(5),
                },
            ],
        );
        assert_eq!(state.settle, 1.0);
        assert_eq!(state.thumb_x, Some(1040.0));
        assert_eq!(state.generation, 1);
        assert_eq!(state.focus.round() as usize, 5);
        assert_eq!(effects, [Effect::Select(5)]);
    }

    #[test]
    fn a_release_without_a_press_changes_nothing() {
        let rest = ArcState::default();
        let (state, effects) = run(&rest, &[ArcEvent::Release, ArcEvent::Release]);
        assert_eq!(state, rest);
        assert!(effects.is_empty());
    }

    #[test]
    fn a_move_without_a_press_changes_nothing() {
        let rest = ArcState::default();
        let (state, effects) = run(
            &rest,
            &[ArcEvent::Drag {
                x: 1000.0,
                y: y_of(9),
            }],
        );
        assert_eq!(state, rest);
        assert!(effects.is_empty());
    }

    #[test]
    fn a_release_after_a_press_starts_one_settle_of_a_new_generation() {
        let held = pressed_at(3);
        let (state, effects) = run(&held, &[ArcEvent::Release]);
        assert_eq!(state.generation, held.generation + 1);
        assert_eq!(effects, [Effect::Settle(state.generation)]);
        // Still engaged until the animation lands.
        assert_eq!(state.settle, 1.0);
        assert!(state.thumb_x.is_some());
    }

    #[test]
    fn the_settle_eases_out_and_lands_at_rest() {
        let released = run(&pressed_at(3), &[ArcEvent::Release]).0;
        let generation = released.generation;
        let mut previous = 1.0;
        let mut state = released;
        for step in 1..SETTLE_STEPS {
            state = state
                .next(VIEW, COUNT, ArcEvent::Tick { generation, step })
                .0;
            assert!(state.settle < previous, "step {step} did not ease");
            assert!(state.thumb_x.is_some(), "landed early at {step}");
            previous = state.settle;
        }
        state = state
            .next(
                VIEW,
                COUNT,
                ArcEvent::Tick {
                    generation,
                    step: SETTLE_STEPS,
                },
            )
            .0;
        assert_eq!((state.settle, state.pull, state.thumb_x), (0.0, 0.0, None));
    }

    #[test]
    fn a_frame_of_a_superseded_settle_is_ignored() {
        let released = run(&pressed_at(3), &[ArcEvent::Release]).0;
        let stale = released.generation;
        // A new press supersedes the animation in flight.
        let (pressed, _) = released.next(
            VIEW,
            COUNT,
            ArcEvent::Press {
                x: 1050.0,
                y: y_of(8),
            },
        );
        let (after, effects) = pressed.next(
            VIEW,
            COUNT,
            ArcEvent::Tick {
                generation: stale,
                step: SETTLE_STEPS,
            },
        );
        assert_eq!(after, pressed);
        assert!(effects.is_empty());
    }

    #[test]
    fn dragging_past_an_end_pulls_and_inside_the_index_does_not() {
        let spacing = spacing(HEIGHT, COUNT);
        let top = top_inset(spacing, COUNT, HEIGHT);
        let inside = run(
            &pressed_at(4),
            &[ArcEvent::Drag {
                x: 1000.0,
                y: y_of(10),
            }],
        )
        .0;
        assert_eq!(inside.pull, 0.0);
        let past = run(
            &pressed_at(4),
            &[ArcEvent::Drag {
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
            &ArcState::default(),
            &[
                ArcEvent::Nudge(1.0),
                ArcEvent::Nudge(1.0),
                ArcEvent::Nudge(-1000.0),
                ArcEvent::Nudge(1000.0),
                ArcEvent::Nudge(1000.0),
            ],
        );
        assert_eq!(state.focus, (COUNT - 1) as f64);
        assert_eq!(
            effects,
            [
                Effect::Select(1),
                Effect::Select(2),
                Effect::Select(0),
                Effect::Select(COUNT - 1)
            ]
        );
        let spacing = spacing(HEIGHT, COUNT);
        let (wheeled, _) = run(
            &ArcState::default(),
            &[ArcEvent::Wheel {
                travel: 3.0 * spacing,
            }],
        );
        assert_eq!(wheeled.focus.round(), 3.0);
    }

    #[test]
    fn hovering_labels_the_breakpoint_only_while_not_pressed() {
        let (hovered, _) = run(&ArcState::default(), &[ArcEvent::Hover { y: y_of(7) }]);
        assert_eq!(hovered.hovered, Some(7));
        let (left, _) = run(&hovered, &[ArcEvent::HoverEnd]);
        assert_eq!(left.hovered, None);
        let (held, _) = run(&pressed_at(2), &[ArcEvent::Hover { y: y_of(7) }]);
        assert_eq!(held.hovered, None);
    }

    #[test]
    fn a_focus_left_past_a_shrunken_list_is_clamped_by_the_next_event() {
        let far = ArcState {
            focus: 20.0,
            ..ArcState::default()
        };
        let (state, _) = far.next(VIEW, 3, ArcEvent::Nudge(1.0));
        assert_eq!(state.focus, 2.0);
    }
}

mod rendered {
    //! The component under a `VirtualDom`, as the lab drives it: one `ArcIndex` whose items the
    //! parent swaps when the tab changes.
    use super::*;

    static TAB: GlobalSignal<usize> = Signal::global(|| 0);
    const TABS: [&[&str]; 3] = [
        &["Component kit"],
        &["Typography", "Spacing", "Shape", "Surfaces"],
        &["Colors", "Extra"],
    ];

    fn app() -> Element {
        rsx! {
            ArcIndex { viewport: (1076.0, 860.0), items: TABS[TAB()].to_vec(), on_select: |_| {} }
        }
    }

    fn switch_to(dom: &mut VirtualDom, tab: usize) -> String {
        dom.in_runtime(|| *TAB.write() = tab);
        format!("{:?}", dom.render_immediate_to_vec())
    }

    #[test]
    fn the_strip_follows_the_items_the_parent_passes() {
        let mut dom = VirtualDom::new(app);
        let first = format!("{:?}", dom.rebuild_to_vec());
        assert!(
            first.contains("aria-valuemax") && first.contains("Int(1)"),
            "{first}"
        );

        let second = switch_to(&mut dom, 1);
        assert!(
            second.contains("Int(4)"),
            "valuemax did not follow the items: {second}"
        );
        assert!(
            second.contains("Typography"),
            "labels did not follow the items: {second}"
        );

        let third = switch_to(&mut dom, 2);
        assert!(third.contains("Int(2)"), "{third}");
    }

    /// A release with no press of the strip's own (a scrollbar drag let go over the grab
    /// band) leaves the strip at rest: no settle animation starts, so the overlay never
    /// turns on `pointer-events`.
    #[test]
    fn a_release_without_a_press_leaves_the_strip_at_rest() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        // Handlers read their event data through the converter a renderer installs.
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        runtime.block_on(async {
            let mut dom = VirtualDom::new(app);
            let first = dom.rebuild_to_vec();
            assert!(
                format!("{first:?}").contains("pointer-events:none"),
                "{first:?}"
            );
            // Every element the renderer registered: a stray release lands wherever the
            // cursor is, and none of them may engage the strip.
            let targets: Vec<_> = first
                .edits
                .iter()
                .filter_map(|edit| match edit {
                    dioxus_core::Mutation::SetId { id } => Some(*id),
                    _ => None,
                })
                .collect();
            assert!(!targets.is_empty(), "{first:?}");
            let release: dioxus_html::SerializedPointerData =
                serde_json::from_value(serde_json::json!({
                    "pointer_id": 1, "width": 1.0, "height": 1.0, "pressure": 0.0,
                    "tangential_pressure": 0.0, "tilt_x": 0, "tilt_y": 0, "twist": 0,
                    "pointer_type": "mouse", "is_primary": true,
                    "alt_key": false, "button": 0, "buttons": 0,
                    "client_x": 1050.0, "client_y": 400.0, "ctrl_key": false,
                    "meta_key": false, "offset_x": 0.0, "offset_y": 0.0,
                    "page_x": 1050.0, "page_y": 400.0, "screen_x": 1050.0,
                    "screen_y": 400.0, "shift_key": false,
                }))
                .expect("pointer data");
            for target in targets {
                dom.runtime().handle_event_for_target(
                    dioxus_core::RenderTargetId::ROOT,
                    "pointerup",
                    dioxus_core::Event::new(
                        dioxus_html::EventData::Pointer(release.clone()).into_any(),
                        true,
                    ),
                    target,
                );
            }
            // Long enough for several settle ticks had a release started.
            let _ =
                tokio::time::timeout(Duration::from_millis(SETTLE_MS), dom.wait_for_work()).await;
            let after = format!("{:?}", dom.render_immediate_to_vec());
            assert!(
                !after.contains("pointer-events:auto"),
                "a stray release engaged the strip: {after}"
            );
        });
    }

    /// A wheel over the index steps the strip without panicking: the handler reads the focus
    /// before committing the next one, which writes it.
    #[test]
    fn a_wheel_over_the_index_steps_without_a_double_borrow() {
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let mut dom = VirtualDom::new(app);
        let first = dom.rebuild_to_vec();
        let targets: Vec<_> = first
            .edits
            .iter()
            .filter_map(|edit| match edit {
                dioxus_core::Mutation::SetId { id } => Some(*id),
                _ => None,
            })
            .collect();
        assert!(!targets.is_empty(), "{first:?}");
        let wheel: dioxus_html::SerializedWheelData = serde_json::from_value(serde_json::json!({
            "delta_mode": 0, "delta_x": 0.0, "delta_y": 120.0, "delta_z": 0.0,
            "alt_key": false, "button": 0, "buttons": 0,
            "client_x": 1050.0, "client_y": 400.0, "ctrl_key": false,
            "meta_key": false, "offset_x": 0.0, "offset_y": 0.0,
            "page_x": 1050.0, "page_y": 400.0, "screen_x": 1050.0,
            "screen_y": 400.0, "shift_key": false,
        }))
        .expect("wheel data");
        for target in targets {
            dom.runtime().handle_event_for_target(
                dioxus_core::RenderTargetId::ROOT,
                "wheel",
                dioxus_core::Event::new(
                    dioxus_html::EventData::Wheel(wheel.clone()).into_any(),
                    true,
                ),
                target,
            );
        }
        dom.render_immediate_to_vec();
    }

    /// The lab's parent: the tabs swap the items under a live strip, so a storm also changes
    /// the props mid-gesture.
    fn tabbed() -> Element {
        let mut tab = use_signal(|| 0usize);
        rsx! {
            for index in 0..TABS.len() {
                button { key: "{index}", onclick: move |_| tab.set(index), "tab {index}" }
            }
            ArcIndex { viewport: (1076.0, 860.0), items: TABS[tab()].to_vec(), on_select: move |_| {} }
        }
    }

    /// Every listener of the strip, fed every event kind it takes in seeded random order, with
    /// settle animations running between steps and the items swapped under it.
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
