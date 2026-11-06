use super::super::Point;
use super::super::use_drawer_snap_points::active_snap_point_offset;
use super::super::use_drawer_snap_points::resolve_active_snap_point;
use super::super::use_drawer_snap_points::resolve_snap_points;
use super::super::use_swipe_dismiss::ElementMetrics;
use super::super::use_swipe_dismiss::SwipeDismiss;
use super::super::use_swipe_dismiss::SwipeEnd;
use super::super::use_swipe_dismiss::SwipeThreshold;
use super::super::use_swipe_dismiss::Transform;
use super::*;

const POPUP: Size = Size {
    width: 320.0,
    height: 300.0,
};

fn release(direction: Option<SwipeDirection>, delta: (f64, f64), velocity: (f64, f64)) -> Release {
    Release {
        direction,
        delta_x: delta.0,
        delta_y: delta.1,
        velocity_x: velocity.0,
        velocity_y: velocity.1,
        release_velocity_x: velocity.0,
        release_velocity_y: velocity.1,
    }
}

/// The popup (300px) and viewport (400px) heights `DrawerViewport.test.tsx`'s snap point tests
/// set, with `texts` as the drawer's `snapPoints` and `active` as its snap point.
struct Drawer {
    snap_points: Vec<SnapPoint>,
    resolved: Vec<ResolvedSnapPoint>,
    active_offset: Option<f64>,
    sequential: bool,
}

impl Drawer {
    fn new(texts: &[&str], active: &str) -> Self {
        let snap_points: Vec<SnapPoint> = texts
            .iter()
            .map(|text| SnapPoint::Text((*text).to_owned()))
            .collect();
        let resolved = resolve_snap_points(&snap_points, 400.0, 16.0, 300.0);
        let active = SnapPoint::Text(active.to_owned());
        let active_offset = active_snap_point_offset(resolve_active_snap_point(
            Some(&active),
            &resolved,
            300.0,
            400.0,
            16.0,
        ));
        Self {
            snap_points,
            resolved,
            active_offset,
            sequential: false,
        }
    }

    fn snap(&self) -> DrawerSnap<'_> {
        DrawerSnap {
            snap_points: &self.snap_points,
            resolved: &self.resolved,
            active_offset: self.active_offset,
            popup_height: 300.0,
            snap_to_sequential_points: self.sequential,
        }
    }

    /// What the drawer settles on, by snap point text.
    fn outcome(&self, decision: DrawerRelease) -> String {
        match decision {
            DrawerRelease::SettleOnSnapPoint(index) => match &self.resolved[index].value {
                SnapPoint::Text(text) => text.clone(),
                SnapPoint::Number(number) => number.to_string(),
            },
            other => format!("{other:?}"),
        }
    }

    /// A `Drawer.Root swipeDirection="down"` drag from `(from)` through `moves` and a release
    /// at `up`, each `(x, y, timeStamp)`, as `firePointer.down/move/up` on the viewport.
    fn drag(&self, down: (f64, f64, f64), moves: &[(f64, f64, f64)], up: f64) -> DrawerRelease {
        let mut swipe = SwipeDismiss::new(
            swipe_directions(true, SwipeDirection::Down),
            Some(SwipeThreshold::PerDirection(get_base_swipe_threshold)),
            true,
        );
        swipe.start_swipe_at_position(
            Point {
                x: down.0,
                y: down.1,
            },
            down.2,
            Some(ElementMetrics {
                size: POPUP,
                transform: Transform::default(),
            }),
        );
        for (x, y, time_stamp) in moves {
            swipe.handle_move(Point { x: *x, y: *y }, *time_stamp);
        }
        let mut decision = None;
        let (end, _) = swipe.handle_end(up, |details| {
            decision = Some(on_release(
                POPUP,
                SwipeDirection::Down,
                details,
                &self.snap(),
            ));
            None
        });
        let decision = decision.expect("the swipe was active");
        // `Default` is the hook's own decision: dismiss when past its threshold.
        match (decision, end) {
            (DrawerRelease::Default, SwipeEnd::Dismissed(direction)) => {
                DrawerRelease::Close { direction }
            }
            (DrawerRelease::Default, _) => DrawerRelease::Stay,
            (decision, _) => decision,
        }
    }
}

// Ports of `DrawerViewport.test.tsx` cases whose outcome is the release decision. Upstream
// reads the outcome off `onSnapPointChange`/`onOpenChange`; here it is the `DrawerRelease`.
// The viewport is 400px and the popup 300px, as in `setHeight(.., 400)` and `(.., 300)`.
mod drawer_viewport {
    use super::*;

    #[test]
    fn does_not_dismiss_from_a_fast_swipe_that_was_never_attributed_to_the_snap_point_axis() {
        // The taller point is declared first so the expected settle target is not simply the
        // first resolved snap point.
        let drawer = Drawer::new(&["200px", "100px"], "200px");
        // A mostly horizontal flick: cumulative |deltaX| stays above |deltaY| on every move
        // so no swipe direction is ever attributed, while the final samples carry fast
        // downward velocity from the finger arcing down at lift.
        let decision = drawer.drag(
            (100.0, 10.0, 1000.0),
            &[
                (120.0, 12.0, 1050.0),
                (200.0, 20.0, 1100.0),
                (240.0, 55.0, 1120.0),
            ],
            1130.0,
        );
        // The release settles on the nearest snap point rather than dismissing.
        assert_eq!(drawer.outcome(decision), "100px");
    }

    #[test]
    fn navigates_snap_points_from_a_drag_that_was_never_attributed_to_the_swipe_axis() {
        let drawer = Drawer::new(&["100px", "200px"], "100px");
        // A sustained diagonal drag: cumulative |deltaX| stays above |deltaY| on every move
        // so no direction is attributed, while the vertical component drags the sheet 150px
        // upward toward the taller snap point. (Upstream's events carry the test runner's
        // clock; these are 8ms apart.)
        let decision = drawer.drag(
            (300.0, 300.0, 1000.0),
            &[
                (280.0, 290.0, 1008.0),
                (200.0, 220.0, 1016.0),
                (140.0, 150.0, 1024.0),
            ],
            1032.0,
        );
        assert_eq!(drawer.outcome(decision), "200px");
    }

    #[test]
    fn settles_on_the_nearest_snap_point_when_an_unattributed_drag_ends_past_the_last_snap_point() {
        let drawer = Drawer::new(&["200px", "100px"], "100px");
        // A slow diagonal drag that never attributes a direction and ends nearer the closed
        // position than to any snap point, so the release resolves through the close branch.
        let decision = drawer.drag(
            (100.0, 100.0, 1000.0),
            &[
                (200.0, 120.0, 1100.0),
                (300.0, 170.0, 1400.0),
                (400.0, 190.0, 1900.0),
            ],
            1950.0,
        );
        assert_eq!(drawer.outcome(decision), "100px");
    }
}

#[test]
fn an_attributed_drag_past_the_last_snap_point_dismisses_in_the_swipe_direction() {
    let drawer = Drawer::new(&["200px", "100px"], "100px");
    // The same slow drag, straight down: the direction is attributed, so the close branch
    // dismisses instead of settling.
    let decision = drawer.drag(
        (100.0, 100.0, 1000.0),
        &[
            (100.0, 120.0, 1100.0),
            (100.0, 190.0, 1400.0),
            (100.0, 250.0, 1900.0),
        ],
        1950.0,
    );
    assert_eq!(
        decision,
        DrawerRelease::Close {
            direction: SwipeDirection::Down
        }
    );
}

#[test]
fn get_base_swipe_threshold_is_half_the_popup_but_at_least_10_px() {
    let table = [
        (SwipeDirection::Down, 150.0),
        (SwipeDirection::Up, 150.0),
        (SwipeDirection::Left, 160.0),
        (SwipeDirection::Right, 160.0),
    ];
    for (direction, expected) in table {
        assert_eq!(
            get_base_swipe_threshold(POPUP, direction),
            expected,
            "{direction:?}"
        );
    }
    assert_eq!(
        get_base_swipe_threshold(
            Size {
                width: 8.0,
                height: 8.0
            },
            SwipeDirection::Down
        ),
        10.0
    );
    assert_eq!(
        get_base_swipe_threshold(Size::default(), SwipeDirection::Left),
        10.0
    );
}

#[test]
fn release_without_snap_points_dismisses_on_velocity_or_half_the_popup() {
    let none = Drawer {
        snap_points: vec![],
        resolved: vec![],
        active_offset: None,
        sequential: false,
    };
    // (direction, delta (x, y), velocity (x, y), expected)
    let down = Some(SwipeDirection::Down);
    let close_down = DrawerRelease::Close {
        direction: SwipeDirection::Down,
    };
    let table = [
        // No attributed direction: the hook decides.
        (None, (0.0, 200.0), (0.0, 1.0), DrawerRelease::Default),
        // Not moved the dismiss way at all.
        (down, (0.0, 0.0), (0.0, 2.0), DrawerRelease::Stay),
        (down, (0.0, -40.0), (0.0, 2.0), DrawerRelease::Stay),
        // A fast flick dismisses however short.
        (down, (0.0, 12.0), (0.0, 0.5), close_down),
        (down, (0.0, 12.0), (0.0, 0.49), DrawerRelease::Stay),
        // Past half the popup (150) dismisses, at or below it does not.
        (down, (0.0, 151.0), (0.0, 0.0), close_down),
        (down, (0.0, 150.0), (0.0, 0.0), DrawerRelease::Stay),
        // Horizontal drawers measure the width (160).
        (
            Some(SwipeDirection::Right),
            (161.0, 0.0),
            (0.0, 0.0),
            DrawerRelease::Close {
                direction: SwipeDirection::Right,
            },
        ),
        (
            Some(SwipeDirection::Right),
            (160.0, 0.0),
            (0.0, 0.0),
            DrawerRelease::Stay,
        ),
        (
            Some(SwipeDirection::Left),
            (-161.0, 0.0),
            (0.0, 0.0),
            DrawerRelease::Close {
                direction: SwipeDirection::Left,
            },
        ),
        (
            Some(SwipeDirection::Up),
            (0.0, -151.0),
            (0.0, 0.0),
            DrawerRelease::Close {
                direction: SwipeDirection::Up,
            },
        ),
    ];
    for (direction, delta, velocity, expected) in table {
        let details = release(direction, delta, velocity);
        assert_eq!(
            on_release(POPUP, SwipeDirection::Down, &details, &none.snap()),
            expected,
            "{direction:?} {delta:?} {velocity:?}"
        );
    }
}

#[test]
fn release_with_snap_points_projects_momentum_onto_the_nearest_one() {
    // Snap points at 100 (offset 200) and 200 (offset 100) px on a 300px popup; resting on
    // the lower one, so the drawer sits at offset 200.
    let drawer = Drawer::new(&["100px", "200px"], "100px");
    let down = Some(SwipeDirection::Down);
    // (delta y, release velocity y, expected)
    let table = [
        // Dragged up (negative dy) 60px, no momentum: offset 140 is nearest 100 (the taller).
        (-60.0, 0.0, "200px"),
        // 20px up only: 180 is nearest 200 (the shorter, where it was).
        (-20.0, 0.0, "100px"),
        // The same 20px with a fast upward flick: -1.0 px/ms * 300 = 300px of momentum
        // clamps at the open edge, nearest the taller point.
        (-20.0, -1.0, "200px"),
        // Below the 0.5 threshold, momentum is ignored.
        (-20.0, -0.49, "100px"),
        // Dragged down 40px: offset 240 is 40 from the point it rests on (200) and 60 from
        // closed (300), so it settles back.
        (40.0, 0.0, "100px"),
        // Dragged down 70px: offset 270 is 30 from closed, 70 from the nearest point.
        (70.0, 0.0, "Close { direction: Down }"),
        // A fast flick down closes.
        (10.0, 0.5, "Close { direction: Down }"),
    ];
    for (delta_y, velocity_y, expected) in table {
        let details = release(down, (0.0, delta_y), (0.0, velocity_y));
        let decision = on_release(POPUP, SwipeDirection::Down, &details, &drawer.snap());
        assert_eq!(drawer.outcome(decision), expected, "{delta_y} {velocity_y}");
    }
}

#[test]
fn release_with_snap_points_ignores_a_touch_reversal_at_lift() {
    // `DrawerViewport.tsx:523-529`: the finger travelled 60px up but its last samples curl
    // back down. The reversal is dropped in favour of the whole-gesture velocity.
    let drawer = Drawer::new(&["100px", "200px"], "100px");
    let mut details = release(Some(SwipeDirection::Down), (0.0, -60.0), (0.0, -0.2));
    details.release_velocity_y = 1.0;
    let decision = on_release(POPUP, SwipeDirection::Down, &details, &drawer.snap());
    assert_eq!(drawer.outcome(decision), "200px");
}

#[test]
fn release_with_sequential_snap_points_advances_at_most_one_point() {
    // Three points; resting on the lowest (offset 250).
    let mut drawer = Drawer::new(&["50px", "150px", "250px"], "50px");
    drawer.sequential = true;
    assert_eq!(drawer.active_offset, Some(250.0));
    let down = Some(SwipeDirection::Down);
    // (delta y, release velocity y, expected)
    let table = [
        // Without momentum a long drag still lands where the finger is: offset 20 -> top.
        (-230.0, 0.0, "250px"),
        // A fast flick only forces the adjacent point when the finger stopped short of it
        // (`:598-605`): this one went past it, so the finger's point stands...
        (-230.0, -2.0, "250px"),
        // ...and momentum is not added to the target: 150 is where the finger is.
        (-100.0, -2.0, "150px"),
        // A flick that is too short to reach the adjacent point advances to it anyway.
        (-30.0, -1.0, "150px"),
        // Slow and short: stays.
        (-30.0, -0.1, "50px"),
        // Flicking down from the lowest point closes it (unattributed would settle).
        (30.0, 1.0, "Close { direction: Down }"),
    ];
    for (delta_y, velocity_y, expected) in table {
        let details = release(down, (0.0, delta_y), (0.0, velocity_y));
        let decision = on_release(POPUP, SwipeDirection::Down, &details, &drawer.snap());
        assert_eq!(drawer.outcome(decision), expected, "{delta_y} {velocity_y}");
    }
}

#[test]
fn an_unattributed_release_never_closes_with_sequential_snap_points() {
    // `DrawerViewport.test.tsx:4073`'s intent: the same flick down from the lowest point
    // settles instead of closing when no direction was attributed.
    let mut drawer = Drawer::new(&["50px", "150px", "250px"], "50px");
    drawer.sequential = true;
    let details = release(None, (0.0, 30.0), (0.0, 1.0));
    let decision = on_release(POPUP, SwipeDirection::Down, &details, &drawer.snap());
    assert_eq!(drawer.outcome(decision), "50px");
}

#[test]
fn release_defers_to_the_hook_for_unusable_snap_state() {
    let details = release(Some(SwipeDirection::Down), (0.0, 100.0), (0.0, 0.0));
    let drawer = Drawer::new(&["100px", "200px"], "100px");
    // Horizontal drawers ignore snap points (`DrawerViewport.tsx:502-505`).
    assert_eq!(
        on_release(POPUP, SwipeDirection::Left, &details, &drawer.snap()),
        DrawerRelease::Default
    );
    // No measured popup: settle where it was (`:507-510`).
    let mut unmeasured = drawer.snap();
    unmeasured.popup_height = 0.0;
    assert_eq!(
        on_release(POPUP, SwipeDirection::Down, &details, &unmeasured),
        DrawerRelease::Stay
    );
    // Every snap point invalid: regular dismissal (`:512-515`).
    let invalid = Drawer::new(&["nonsense"], "nonsense");
    assert_eq!(
        on_release(POPUP, SwipeDirection::Down, &details, &invalid.snap()),
        DrawerRelease::Default
    );
}

#[test]
fn an_up_drawer_mirrors_the_down_drawer() {
    let drawer = Drawer::new(&["100px", "200px"], "100px");
    let up = Some(SwipeDirection::Up);
    // Dragging an `up` drawer's sheet toward closed is downward travel: dy > 0 is the
    // taller point for a `down` drawer and dy < 0 is for an `up` one.
    let details = release(up, (0.0, 60.0), (0.0, 0.0));
    let decision = on_release(POPUP, SwipeDirection::Up, &details, &drawer.snap());
    assert_eq!(drawer.outcome(decision), "200px");
    let details = release(up, (0.0, -270.0), (0.0, 0.0));
    let decision = on_release(POPUP, SwipeDirection::Up, &details, &drawer.snap());
    assert_eq!(
        decision,
        DrawerRelease::Close {
            direction: SwipeDirection::Up
        }
    );
}

#[test]
fn resolve_swipe_release_scales_the_exit_by_how_hard_the_drawer_was_flung() {
    let none = Drawer {
        snap_points: vec![],
        resolved: vec![],
        active_offset: None,
        sequential: false,
    };
    let down = SwipeDirection::Down;
    // (delta y, velocity y, expected scalar)
    let table = [
        // 300 - 100 = 200px to go at the 4px/ms cap: 50ms, floored to 80ms: scalar 0.1.
        (100.0, 5.0, Some(0.1)),
        // 200px at 0.5px/ms = 400ms, capped at 360ms: scalar 1.
        (100.0, 0.5, Some(1.0)),
        // 200px at 1px/ms = 200ms: 0.1 + (120 / 280) * 0.9.
        (100.0, 1.0, Some(0.1 + 120.0 / 280.0 * 0.9)),
        // Too slow to be a fling.
        (100.0, 0.2, None),
        (100.0, -3.0, None),
        // Already all the way out.
        (300.0, 3.0, None),
    ];
    for (delta_y, velocity_y, expected) in table {
        let details = release(Some(down), (0.0, delta_y), (0.0, velocity_y));
        let actual = resolve_swipe_release(POPUP, down, &details, &none.snap());
        match (actual, expected) {
            (Some(actual), Some(expected)) => {
                assert!((actual - expected).abs() < 1e-12, "{delta_y} {velocity_y}");
            }
            (actual, expected) => assert_eq!(actual, expected, "{delta_y} {velocity_y}"),
        }
    }
    // The whole-gesture velocity stands in when the release velocity is zero.
    let mut details = release(Some(down), (0.0, 100.0), (0.0, 1.0));
    details.release_velocity_y = 0.0;
    assert!(resolve_swipe_release(POPUP, down, &details, &none.snap()).is_some());
    // An unmeasured popup has nothing to fling.
    assert_eq!(
        resolve_swipe_release(Size::default(), down, &details, &none.snap()),
        None
    );
    // A drawer resting on a snap point (offset 200) starts its exit that much further along:
    // 50px of travel leaves 300 - 250 = 50px to go (a hard 80ms fling), where the same 50px
    // from open leaves 250px at 1px/ms (250ms).
    let drawer = Drawer::new(&["100px", "200px"], "100px");
    let details = release(Some(down), (0.0, 50.0), (0.0, 1.0));
    let with_snap = resolve_swipe_release(POPUP, down, &details, &drawer.snap()).unwrap();
    let without = resolve_swipe_release(POPUP, down, &details, &none.snap()).unwrap();
    assert!((with_snap - 0.1).abs() < 1e-12);
    assert!((without - (0.1 + 170.0 / 280.0 * 0.9)).abs() < 1e-12);
    // With nothing left to travel there is no fling at all.
    let details = release(Some(down), (0.0, 100.0), (0.0, 1.0));
    assert_eq!(
        resolve_swipe_release(POPUP, down, &details, &drawer.snap()),
        None
    );
}

#[test]
fn snap_point_range_runs_between_the_two_tallest_points() {
    let drawer = Drawer::new(&["100px", "200px", "250px"], "100px");
    assert_eq!(
        snap_point_range(&drawer.snap(), SwipeDirection::Down),
        Some(SnapPointRange {
            min_offset: 50.0,
            range: 50.0
        })
    );
    assert_eq!(snap_point_range(&drawer.snap(), SwipeDirection::Left), None);
    let single = Drawer::new(&["100px"], "100px");
    assert_eq!(snap_point_range(&single.snap(), SwipeDirection::Down), None);
}

#[test]
fn on_progress_runs_between_the_first_two_snap_points() {
    let drawer = Drawer::new(&["100px", "200px"], "100px");
    let snap = drawer.snap();
    let range = snap_point_range(&snap, SwipeDirection::Down);
    // At rest on the lower point the backdrop is at its far end...
    assert_eq!(snap_point_progress(range, snap.active_offset), Some(1.0));
    let at_rest = on_progress(0.0, false, None, &snap, SwipeDirection::Down);
    assert_eq!(at_rest, 1.0);
    // ...and dragging up 50px (offset 200 -> 150) is half way between the two.
    let details = ProgressDetails {
        delta_x: 0.0,
        delta_y: -50.0,
        direction: Some(SwipeDirection::Up),
    };
    assert_eq!(
        on_progress(0.0, true, Some(&details), &snap, SwipeDirection::Down),
        0.5
    );
    // A press that never became a swipe does not move it.
    assert_eq!(
        on_progress(0.0, false, Some(&details), &snap, SwipeDirection::Down),
        1.0
    );
    // Without two snap points the hook's progress stands.
    let single = Drawer::new(&["100px"], "100px");
    assert_eq!(
        on_progress(
            0.3,
            true,
            Some(&details),
            &single.snap(),
            SwipeDirection::Down
        ),
        0.3
    );
}

#[test]
fn apply_swipe_progress_tracks_a_nested_drawer_for_its_parent_only() {
    let table = [
        // open, nested, tracked: (own progress, progress for the parent, shows)
        (true, false, true, (0.4, 0.4, true)),
        (true, true, true, (0.0, 0.4, false)),
        (true, false, false, (0.0, 0.0, false)),
        (false, false, true, (0.0, 0.0, false)),
    ];
    for (open, nested, tracked, (own, parent, shows)) in table {
        assert_eq!(
            apply_swipe_progress(open, nested, 0.4, tracked),
            SwipeProgress {
                swipe_progress: own,
                nested_swipe_progress: parent,
                show_progress: shows
            },
            "{open} {nested} {tracked}"
        );
    }
    assert!(!apply_swipe_progress(true, false, 0.0, true).show_progress);
}

#[test]
fn update_nested_swipe_active_waits_for_10_px_of_travel() {
    let details = |delta_y: f64, direction: Option<SwipeDirection>| ProgressDetails {
        delta_x: 0.0,
        delta_y,
        direction,
    };
    let down = SwipeDirection::Down;
    assert!(!update_nested_swipe_active(false, None, down));
    assert!(!update_nested_swipe_active(
        false,
        Some(&details(9.9, Some(down))),
        down
    ));
    assert!(update_nested_swipe_active(
        false,
        Some(&details(10.0, Some(down))),
        down
    ));
    // Either way counts (`Math.abs`), and the root's direction stands in when none was
    // attributed.
    assert!(update_nested_swipe_active(
        false,
        Some(&details(-12.0, Some(down))),
        down
    ));
    assert!(update_nested_swipe_active(
        false,
        Some(&details(12.0, None)),
        down
    ));
    // It stays active once active.
    assert!(update_nested_swipe_active(true, None, down));
}

#[test]
fn swipe_directions_add_the_opposite_way_for_snap_points() {
    use SwipeDirection::*;
    assert_eq!(swipe_directions(true, Down), &[Down, Up]);
    assert_eq!(swipe_directions(true, Up), &[Up, Down]);
    assert_eq!(swipe_directions(true, Left), &[Left]);
    assert_eq!(swipe_directions(false, Down), &[Down]);
    assert_eq!(swipe_directions(false, Right), &[Right]);
}
