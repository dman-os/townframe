use super::*;

/// Drives a [`SwipeDismiss`] the way `useSwipeDismiss.test.tsx`'s `fireEvent.pointerDown`,
/// `pointerMove` and `pointerUp` drive the hook: a press, moves and a release, each at a
/// position and a `timeStamp`, collecting what `onProgress` and `onRelease` would have seen.
struct Gesture {
    swipe: SwipeDismiss,
    element: Option<ElementMetrics>,
    progress: Vec<Progress>,
    releases: Vec<Release>,
}

impl Gesture {
    fn new(directions: &[SwipeDirection]) -> Self {
        Self::with(directions, None, 0.0, 0.0)
    }

    fn with(
        directions: &[SwipeDirection],
        threshold: Option<SwipeThreshold>,
        width: f64,
        height: f64,
    ) -> Self {
        Self {
            swipe: SwipeDismiss::new(directions, threshold, true),
            element: Some(ElementMetrics {
                size: Size { width, height },
                transform: Transform::default(),
            }),
            progress: vec![],
            releases: vec![],
        }
    }

    fn down(&mut self, x: f64, y: f64, time_stamp: f64) {
        let progress = self
            .swipe
            .start_swipe_at_position(Point { x, y }, time_stamp, self.element);
        self.progress.extend(progress);
    }

    fn move_to(&mut self, x: f64, y: f64, time_stamp: f64) {
        let moved = self.swipe.handle_move(Point { x, y }, time_stamp);
        self.progress.extend(moved.progress);
    }

    fn up(&mut self, time_stamp: f64, decision: Option<bool>) -> SwipeEnd {
        let releases = &mut self.releases;
        let (end, progress) = self.swipe.handle_end(time_stamp, |release| {
            releases.push(*release);
            decision
        });
        self.progress.extend(progress);
        end
    }

    fn release(&self) -> Release {
        *self.releases.first().expect("a release")
    }
}

fn close(actual: f64, expected: f64, digits: i32) {
    let tolerance = 10f64.powi(-digits) / 2.0;
    assert!(
        (actual - expected).abs() < tolerance,
        "{actual} is not close to {expected} ({digits} digits)"
    );
}

#[test]
fn fires_on_progress_relative_to_the_element_size() {
    let mut gesture = Gesture::with(&[SwipeDirection::Right], None, 200.0, 0.0);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(50.0, 0.0, 1020.0);
    close(gesture.progress.last().unwrap().progress, 0.25, 2);
}

#[test]
fn continues_firing_on_progress_when_swipe_progress_is_clamped() {
    let mut gesture = Gesture::with(&[SwipeDirection::Right], None, 200.0, 0.0);
    gesture.down(0.0, 0.0, 1000.0);
    // Baseline move.
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(50.0, 0.0, 1020.0);
    let calls_after_forward = gesture.progress.len();

    // Move past the starting point in the opposite direction; progress is clamped to 0.
    gesture.move_to(-10.0, 0.0, 1030.0);
    let calls_after_reverse = gesture.progress.len();
    assert!(calls_after_reverse > calls_after_forward);

    gesture.move_to(-20.0, 0.0, 1040.0);
    assert!(gesture.progress.len() > calls_after_reverse);
    assert_eq!(gesture.progress.last().unwrap().progress, 0.0);
}

#[test]
fn applies_exponential_damping_for_opposite_direction_movement() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 100.0, 1000.0);
    gesture.move_to(0.0, 100.0, 1010.0);
    gesture.move_to(0.0, 50.0, 1020.0);
    // `--y` is not `0px`: the 50px the wrong way moves the element by sqrt(50).
    close(gesture.swipe.drag_offset().y, -(50f64.sqrt()), 9);
}

#[test]
fn respects_custom_swipe_threshold() {
    let mut gesture = Gesture::with(
        &[SwipeDirection::Down],
        Some(SwipeThreshold::Fixed(10.0)),
        0.0,
        0.0,
    );
    gesture.down(0.0, 0.0, 1000.0);
    // Baseline move.
    gesture.move_to(0.0, 0.0, 1010.0);
    // Move beyond the custom 10px threshold.
    gesture.move_to(0.0, 20.0, 1020.0);
    assert_eq!(
        gesture.up(1030.0, None),
        SwipeEnd::Dismissed(SwipeDirection::Down)
    );
}

#[test]
fn the_default_swipe_threshold_is_40_px() {
    for (travel, expected) in [
        (40.0, SwipeEnd::SnappedBack),
        (41.0, SwipeEnd::Dismissed(SwipeDirection::Down)),
    ] {
        let mut gesture = Gesture::new(&[SwipeDirection::Down]);
        gesture.down(0.0, 0.0, 1000.0);
        gesture.move_to(0.0, 0.0, 1010.0);
        gesture.move_to(0.0, travel, 1020.0);
        assert_eq!(gesture.up(1030.0, None), expected, "{travel}");
    }
}

#[test]
fn a_per_direction_swipe_threshold_sees_the_element_and_the_attributed_direction() {
    fn half(element: Size, direction: SwipeDirection) -> f64 {
        if direction.is_horizontal() {
            element.width / 2.0
        } else {
            element.height / 2.0
        }
    }
    for (directions, to, expected) in [
        // A 400px-tall element dismisses past 200px...
        (
            &[SwipeDirection::Down][..],
            (0.0, 150.0),
            SwipeEnd::SnappedBack,
        ),
        (
            &[SwipeDirection::Down][..],
            (0.0, 201.0),
            SwipeEnd::Dismissed(SwipeDirection::Down),
        ),
        // ...a 300px-wide one past 150px, and the threshold is resolved once the direction
        // is attributed when the hook has several directions.
        (
            &[SwipeDirection::Left, SwipeDirection::Right][..],
            (-151.0, 0.0),
            SwipeEnd::Dismissed(SwipeDirection::Left),
        ),
        (
            &[SwipeDirection::Left, SwipeDirection::Right][..],
            (149.0, 0.0),
            SwipeEnd::SnappedBack,
        ),
    ] {
        let mut gesture = Gesture::with(
            directions,
            Some(SwipeThreshold::PerDirection(half)),
            300.0,
            400.0,
        );
        gesture.down(0.0, 0.0, 1000.0);
        gesture.move_to(0.0, 0.0, 1010.0);
        gesture.move_to(to.0, to.1, 1020.0);
        assert_eq!(gesture.up(1030.0, None), expected, "{directions:?} {to:?}");
    }
}

#[test]
fn fires_on_swiping_change_on_start_and_end() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    assert!(!gesture.swipe.is_swiping());
    gesture.down(0.0, 0.0, 1000.0);
    assert!(gesture.swipe.is_swiping());
    gesture.up(1010.0, None);
    assert!(!gesture.swipe.is_swiping());
}

#[test]
fn allows_on_release_to_override_dismissal() {
    let mut gesture = Gesture::with(
        &[SwipeDirection::Down],
        Some(SwipeThreshold::Fixed(10.0)),
        0.0,
        0.0,
    );
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(0.0, 20.0, 1020.0);
    assert_eq!(gesture.up(1030.0, Some(false)), SwipeEnd::SnappedBack);
    assert_eq!(gesture.releases.len(), 1);
    assert!(!gesture.swipe.drag_dismissed());
}

#[test]
fn on_release_true_dismisses_in_the_attributed_direction_even_below_the_threshold() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(0.0, 5.0, 1020.0);
    assert_eq!(
        gesture.up(1030.0, Some(true)),
        SwipeEnd::Dismissed(SwipeDirection::Down)
    );
    assert!(gesture.swipe.drag_dismissed());
}

#[test]
fn a_reversal_after_crossing_the_threshold_cancels_the_dismissal() {
    // `useSwipeDismiss.ts:655-662`: 30px out, then 15px back. The release is still 15px past
    // the 10px threshold, but the change of mind (>= 10px off the peak) wins.
    let mut gesture = Gesture::with(
        &[SwipeDirection::Down],
        Some(SwipeThreshold::Fixed(10.0)),
        0.0,
        0.0,
    );
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(0.0, 30.0, 1020.0);
    gesture.move_to(0.0, 15.0, 1030.0);
    assert_eq!(gesture.up(1040.0, None), SwipeEnd::SnappedBack);
}

#[test]
fn two_opposite_directions_never_cancel_by_reversal() {
    let mut gesture = Gesture::with(
        &[SwipeDirection::Down, SwipeDirection::Up],
        Some(SwipeThreshold::Fixed(10.0)),
        0.0,
        0.0,
    );
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(0.0, 30.0, 1020.0);
    gesture.move_to(0.0, 15.0, 1030.0);
    assert_eq!(
        gesture.up(1040.0, None),
        SwipeEnd::Dismissed(SwipeDirection::Down)
    );
}

#[test]
fn a_release_without_a_swipe_is_not_a_swipe() {
    let mut swipe = SwipeDismiss::new(&[SwipeDirection::Down], None, true);
    assert_eq!(
        swipe.handle_end(1000.0, |_| unreachable!()).0,
        SwipeEnd::NotSwiping
    );
}

#[test]
fn provides_swipe_velocity_on_release() {
    let mut gesture = Gesture::new(&[SwipeDirection::Right]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1100.0);
    gesture.move_to(50.0, 0.0, 1200.0);
    gesture.up(1300.0, None);
    // 50px over the 200ms from the first move (1100) to the release (1300).
    close(gesture.release().velocity_x, 0.25, 2);
    close(gesture.release().velocity_y, 0.0, 2);
}

#[test]
fn provides_release_velocity_from_the_latest_swipe_movement() {
    let mut gesture = Gesture::new(&[SwipeDirection::Right]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1100.0);
    gesture.move_to(50.0, 0.0, 1200.0);
    gesture.move_to(70.0, 0.0, 1216.0);
    gesture.up(1224.0, None);
    close(gesture.release().release_velocity_x, 1.25, 2);
    close(gesture.release().release_velocity_y, 0.0, 2);
}

#[test]
fn handles_a_stationary_move_at_trailing_time_and_release_at_release_time() {
    for (trailing_time, release_time, expected_velocity) in [
        (1040.0, 1048.0, 2.5),
        (1040.0, 1113.0, 0.0),
        (1113.0, 1121.0, 0.0),
    ] {
        let mut gesture = Gesture::new(&[SwipeDirection::Down]);
        gesture.down(0.0, 0.0, 1000.0);
        gesture.move_to(0.0, 20.0, 1008.0);
        gesture.move_to(0.0, 40.0, 1016.0);
        gesture.move_to(0.0, 60.0, 1024.0);
        gesture.move_to(0.0, 80.0, 1032.0);
        // Chrome on Android emits an effectively stationary sample right before `pointerup`.
        gesture.move_to(0.0, 80.5, trailing_time);
        gesture.up(release_time, None);
        // Keep the last moving sample (20px / 8ms), unless it is older than 80ms.
        close(gesture.release().release_velocity_y, expected_velocity, 4);
        close(gesture.release().release_velocity_x, 0.0, 2);
    }
}

#[test]
fn measures_release_velocity_from_the_latest_movement() {
    type Case = (&'static str, &'static [(f64, f64)], (f64, f64), f64);
    let cases: [Case; 5] = [
        (
            "60 Hz input",
            &[
                (1016.0, 14.4),
                (1032.0, 28.8),
                (1048.0, 43.2),
                (1064.0, 57.6),
            ],
            (1080.0, 57.6),
            0.9,
        ),
        (
            "120 Hz input",
            &[
                (1008.0, 7.2),
                (1016.0, 14.4),
                (1024.0, 21.6),
                (1032.0, 28.8),
            ],
            (1040.0, 28.8),
            0.9,
        ),
        (
            "a slower final step",
            &[
                (1008.0, 10.0),
                (1016.0, 20.0),
                (1024.0, 30.0),
                (1032.0, 31.0),
            ],
            (1040.0, 31.0),
            0.125,
        ),
        (
            "a flick after a pause",
            &[
                (1008.0, 10.0),
                (1016.0, 20.0),
                (1216.0, 30.0),
                (1224.0, 40.0),
            ],
            (1232.0, 40.0),
            1.25,
        ),
        // 8px over 2ms is floored to 4ms.
        (
            "near-simultaneous samples",
            &[(1008.0, 10.0), (1010.0, 18.0)],
            (1018.0, 18.0),
            2.0,
        ),
    ];
    for (name, moves, release, expected) in cases {
        let mut gesture = Gesture::new(&[SwipeDirection::Down]);
        gesture.down(0.0, 0.0, 1000.0);
        for (time_stamp, client_y) in moves {
            gesture.move_to(0.0, *client_y, *time_stamp);
        }
        gesture.up(release.0, None);
        let actual = gesture.release().release_velocity_y;
        assert!(
            (actual - expected).abs() < 5e-5,
            "{name}: {actual} is not {expected}"
        );
    }
}

#[test]
fn keeps_release_velocity_when_moves_arrive_sparsely() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 20.0, 1060.0);
    gesture.move_to(0.0, 200.0, 1120.0);
    gesture.move_to(0.0, 200.5, 1128.0);
    gesture.up(1136.0, None);
    // Keep the last moving sample: 180px over 60ms.
    close(gesture.release().release_velocity_y, 3.0, 4);
}

#[test]
fn measures_release_velocity_from_the_first_move_rather_than_the_press() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 1000.0);
    // The first move arrives 200ms after the press and re-anchors the drag origin.
    gesture.move_to(0.0, 5.0, 1200.0);
    gesture.move_to(0.0, 15.0, 1208.0);
    gesture.up(1216.0, None);
    // 10px over 8ms. Measuring from the press would report 0.05.
    close(gesture.release().release_velocity_y, 1.25, 4);
}

#[test]
fn decays_release_velocity_while_the_pointer_stays_pinned() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 20.0, 1016.0);
    // An off-screen mouse keeps emitting moves at the pinned coordinates.
    for time_stamp in [1032.0, 1048.0, 1064.0] {
        gesture.move_to(0.0, 20.0, time_stamp);
    }
    gesture.up(1072.0, None);
    // Repeated stationary samples clear the velocity from the last movement.
    assert_eq!(gesture.release().release_velocity_y, 0.0);
}

#[test]
fn does_not_report_a_previous_gesture_velocity_for_a_release_without_movement() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 40.0, 1008.0);
    gesture.move_to(0.0, 120.0, 1016.0);
    gesture.up(1024.0, None);
    assert!(gesture.releases[0].release_velocity_y > 1.0);

    gesture.down(0.0, 0.0, 5000.0);
    gesture.up(5016.0, None);
    assert_eq!(gesture.releases[1].release_velocity_y, 0.0);
}

#[test]
fn clamps_short_swipe_durations_when_computing_velocity() {
    let mut gesture = Gesture::new(&[SwipeDirection::Right]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1005.0);
    gesture.move_to(30.0, 0.0, 1010.0);
    gesture.up(1015.0, None);
    // 30px over 10ms (1005 to 1015) floors to the 50ms minimum.
    close(gesture.release().velocity_x, 0.6, 2);
    close(gesture.release().velocity_y, 0.0, 2);
}

#[test]
fn unusable_timestamps_yield_no_velocity() {
    // Synthetic events report `0`; `getValidTimestamp` rejects it (`:56-58`).
    assert_eq!(get_valid_timestamp(0.0), None);
    assert_eq!(get_valid_timestamp(f64::NAN), None);
    assert_eq!(get_valid_timestamp(-5.0), None);
    assert_eq!(get_valid_timestamp(f64::INFINITY), None);
    assert_eq!(get_valid_timestamp(1.0), Some(1.0));
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 0.0);
    gesture.move_to(0.0, 0.0, 0.0);
    gesture.move_to(0.0, 60.0, 0.0);
    gesture.up(0.0, None);
    assert_eq!(gesture.release().velocity_y, 0.0);
    assert_eq!(gesture.release().release_velocity_y, 0.0);
}

#[test]
fn get_displacement_is_positive_towards_the_direction() {
    let table = [
        (SwipeDirection::Up, 3.0, -7.0, 7.0),
        (SwipeDirection::Down, 3.0, -7.0, -7.0),
        (SwipeDirection::Left, 3.0, -7.0, -3.0),
        (SwipeDirection::Right, 3.0, -7.0, 3.0),
    ];
    for (direction, delta_x, delta_y, expected) in table {
        assert_eq!(
            get_displacement(direction, delta_x, delta_y),
            expected,
            "{direction:?}"
        );
    }
}

#[test]
fn an_off_axis_drag_is_square_root_damped_and_an_allowed_one_is_not() {
    let swipe = SwipeDismiss::new(&[SwipeDirection::Down], None, true);
    // Down only: up is damped, down is raw, and the whole horizontal axis is damped.
    assert_eq!(
        swipe.apply_directional_damping(0.0, 50.0),
        Point { x: 0.0, y: 50.0 }
    );
    assert_eq!(
        swipe.apply_directional_damping(16.0, -25.0),
        Point { x: 4.0, y: -5.0 }
    );
    assert_eq!(
        swipe.apply_directional_damping(-16.0, 0.0),
        Point { x: -4.0, y: 0.0 }
    );
    let both = SwipeDismiss::new(&[SwipeDirection::Down, SwipeDirection::Up], None, true);
    assert_eq!(
        both.apply_directional_damping(0.0, -25.0),
        Point { x: 0.0, y: -25.0 }
    );
}

#[test]
fn can_swipe_from_scroll_edge_on_pending_move_follows_the_scroll_position() {
    let down = SwipeDismiss::new(&[SwipeDirection::Down], None, true);
    let metrics = |scroll_top: f64| ScrollMetrics {
        scroll_top,
        scroll_height: 1000.0,
        client_height: 400.0,
        ..ScrollMetrics::default()
    };
    // Pulling down is a swipe only from the top of the content.
    assert_eq!(
        down.can_swipe_from_scroll_edge_on_pending_move(metrics(0.0), 0.0, 12.0),
        Some(true)
    );
    assert_eq!(
        down.can_swipe_from_scroll_edge_on_pending_move(metrics(30.0), 0.0, 12.0),
        Some(false)
    );
    // Pushing up (the wrong way for a down drawer) is not, even at the end.
    assert_eq!(
        down.can_swipe_from_scroll_edge_on_pending_move(metrics(600.0), 0.0, -12.0),
        Some(false)
    );
    // No movement on the drawer's axis leaves it undecided.
    assert_eq!(
        down.can_swipe_from_scroll_edge_on_pending_move(metrics(0.0), 9.0, 0.0),
        None
    );
    let up = SwipeDismiss::new(&[SwipeDirection::Up], None, true);
    assert_eq!(
        up.can_swipe_from_scroll_edge_on_pending_move(metrics(600.0), 0.0, -12.0),
        Some(true)
    );
}

#[test]
fn a_trackless_hook_keeps_the_press_position_as_its_origin() {
    // `useSwipeDismiss.ts:589-601`: the swipe area passes `trackDrag: false` so a flick whose
    // whole travel lands in the first move still registers.
    let mut swipe = SwipeDismiss::new(&[SwipeDirection::Up], None, false);
    swipe.start_swipe_at_position(Point { x: 0.0, y: 300.0 }, 1000.0, None);
    let moved = swipe.handle_move(Point { x: 0.0, y: 200.0 }, 1010.0);
    assert_eq!(
        moved.progress.unwrap().details.unwrap().direction,
        Some(SwipeDirection::Up)
    );
    assert_eq!(moved.progress.unwrap().details.unwrap().delta_y, -100.0);

    let mut tracking = SwipeDismiss::new(&[SwipeDirection::Up], None, true);
    tracking.start_swipe_at_position(Point { x: 0.0, y: 300.0 }, 1000.0, None);
    let moved = tracking.handle_move(Point { x: 0.0, y: 200.0 }, 1010.0);
    assert_eq!(moved.progress.unwrap().details.unwrap().delta_y, 0.0);
}

#[test]
fn a_swipe_continues_from_the_elements_resting_transform() {
    // A drawer resting on a snap point is translated by its offset; the swipe starts there.
    let mut gesture = Gesture::with(&[SwipeDirection::Down], None, 300.0, 400.0);
    gesture.element = Some(ElementMetrics {
        size: Size {
            width: 300.0,
            height: 400.0,
        },
        transform: Transform {
            x: 0.0,
            y: 120.0,
            scale: 1.0,
        },
    });
    gesture.down(0.0, 500.0, 1000.0);
    assert_eq!(gesture.swipe.drag_offset(), Point { x: 0.0, y: 120.0 });
    gesture.move_to(0.0, 500.0, 1010.0);
    gesture.move_to(0.0, 540.0, 1020.0);
    assert_eq!(gesture.swipe.drag_offset(), Point { x: 0.0, y: 160.0 });
    // Progress is the travel over the element's size: 40 / 400.
    close(gesture.progress.last().unwrap().progress, 0.1, 6);
    // Cancelling returns to the resting transform, not to zero.
    gesture.swipe.cancel_swipe_interaction();
    assert_eq!(gesture.swipe.drag_offset(), Point { x: 0.0, y: 120.0 });
    assert!(!gesture.swipe.is_swiping());
}

#[test]
fn reset_forgets_the_gesture() {
    let mut gesture = Gesture::new(&[SwipeDirection::Down]);
    gesture.down(0.0, 0.0, 1000.0);
    gesture.move_to(0.0, 0.0, 1010.0);
    gesture.move_to(0.0, 60.0, 1020.0);
    assert_eq!(gesture.swipe.swipe_direction(), Some(SwipeDirection::Down));
    gesture.swipe.reset();
    assert!(!gesture.swipe.is_swiping());
    assert_eq!(gesture.swipe.swipe_direction(), None);
    assert_eq!(gesture.swipe.drag_offset(), Point::default());
    assert_eq!(gesture.swipe.initial_transform(), Transform::default());
}
