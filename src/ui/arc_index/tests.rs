use super::*;

const HEIGHT: f64 = 860.0;
const WIDTH: f64 = 1076.0;

/// The distance a label has to retreat into before it runs out of window.
fn room() -> f64 {
    WIDTH - REST_INSET - spacing(HEIGHT) / 2.0
}

fn placed(settle: f64) -> Vec<Placed> {
    let items = breakpoints();
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
    let spacing = spacing(HEIGHT);
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
