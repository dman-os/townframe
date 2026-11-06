//! Port of the swipe arithmetic in `packages/react/src/drawer/swipe-area/DrawerSwipeArea.tsx`
//! (Base UI v1.9.0, MIT, Copyright (c) 2019 Material-UI SAS; the licence text is in `swipe.rs`):
//! how far a drawer follows a finger that is opening it, with overshoot damping, and whether the
//! release opens it. Outside-press guards, trigger registration and style writes are DOM and are
//! not ported.

use super::Point;
use super::Size;
use super::clamp;
use super::use_swipe_dismiss::SwipeDirection;
use super::use_swipe_dismiss::Transform;
use super::use_swipe_dismiss::get_displacement;

/// `DrawerSwipeArea.tsx:27`.
pub const DEFAULT_SWIPE_OPEN_RATIO: f64 = 0.5;
/// `DrawerSwipeArea.tsx:28`.
pub const MIN_SWIPE_START_DISTANCE: f64 = 1.0;
/// `DrawerSwipeArea.tsx:29`.
pub const VELOCITY_THRESHOLD: f64 = 0.1;
/// `DrawerSwipeArea.tsx:30`.
pub const FALLBACK_SWIPE_OPEN_THRESHOLD: f64 = 40.0;

/// `DrawerSwipeArea.tsx:63-68` (`oppositeSwipeDirection`).
pub fn opposite_swipe_direction(direction: SwipeDirection) -> SwipeDirection {
    match direction {
        SwipeDirection::Up => SwipeDirection::Down,
        SwipeDirection::Down => SwipeDirection::Up,
        SwipeDirection::Left => SwipeDirection::Right,
        SwipeDirection::Right => SwipeDirection::Left,
    }
}

/// `DrawerSwipeArea.tsx:70-72`: the CSS `touch-action` that leaves the cross axis to the browser.
pub fn resolve_touch_action(direction: SwipeDirection) -> &'static str {
    if matches!(direction, SwipeDirection::Left | SwipeDirection::Right) {
        "pan-y"
    } else {
        "pan-x"
    }
}

fn is_horizontal(direction: SwipeDirection) -> bool {
    matches!(direction, SwipeDirection::Left | SwipeDirection::Right)
}

/// `getPopupSize` (`DrawerSwipeArea.tsx:171-179`): the popup's extent along the dismiss axis,
/// `None` when it has none.
pub fn get_popup_size(popup: Size, dismiss_direction: SwipeDirection) -> Option<f64> {
    let size = if is_horizontal(dismiss_direction) {
        popup.width
    } else {
        popup.height
    };
    (size > 0.0).then_some(size)
}

/// `DrawerSwipeArea.tsx:186-200`: how far the closed popup sits from its open position. Its
/// rendered transform wins over its size when the popup is part-way (a closed popup that does not
/// slide its full height), capped by the size.
pub fn resolve_closed_offset(
    popup: Size,
    transform: Transform,
    dismiss_direction: SwipeDirection,
) -> Option<f64> {
    let offset = get_popup_size(popup, dismiss_direction)?;
    let transform_offset = if is_horizontal(dismiss_direction) {
        transform.x
    } else {
        transform.y
    };
    if transform_offset.is_finite() && transform_offset.abs() > 0.5 {
        return Some(offset.min(transform_offset.abs()));
    }
    Some(offset)
}

/// `DrawerSwipeArea.tsx:202-209`: how far the drag must go to open the drawer on release.
pub fn resolve_swipe_open_threshold(popup: Option<Size>, dismiss_direction: SwipeDirection) -> f64 {
    match popup.and_then(|popup| get_popup_size(popup, dismiss_direction)) {
        None => FALLBACK_SWIPE_OPEN_THRESHOLD,
        Some(popup_size) => popup_size * DEFAULT_SWIPE_OPEN_RATIO,
    }
}

/// What `applySwipeMovement` writes (`DrawerSwipeArea.tsx:241-259`, `:267-270`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SwipeMovement {
    /// `--drawer-swipe-movement-x/y`, in px.
    pub movement: Point,
    pub open_progress: f64,
    /// `--drawer-swipe-progress` on the backdrop.
    pub backdrop_progress: f64,
}

/// `applySwipeMovement` (`DrawerSwipeArea.tsx:230-244`): the popup follows the finger from its
/// closed offset towards open; past fully open the extra travel is square-root damped
/// (rubber-banding).
pub fn apply_swipe_movement(
    closed_offset: f64,
    drag_delta: Point,
    resolved_swipe_direction: SwipeDirection,
) -> SwipeMovement {
    let dismiss_direction = opposite_swipe_direction(resolved_swipe_direction);
    let displacement = get_displacement(resolved_swipe_direction, drag_delta.x, drag_delta.y);
    let clamped_displacement = displacement.max(0.0);
    let damped_displacement = if clamped_displacement > closed_offset {
        closed_offset + (clamped_displacement - closed_offset).sqrt()
    } else {
        clamped_displacement
    };
    let remaining = closed_offset - damped_displacement;
    let direction_sign = if matches!(dismiss_direction, SwipeDirection::Left | SwipeDirection::Up) {
        -1.0
    } else {
        1.0
    };
    let movement = remaining * direction_sign;
    let open_progress = clamp(clamped_displacement / closed_offset, 0.0, 1.0);
    let backdrop_progress = clamp(1.0 - open_progress, 0.0, 1.0);
    SwipeMovement {
        movement: if is_horizontal(dismiss_direction) {
            Point {
                x: movement,
                y: 0.0,
            }
        } else {
            Point {
                x: 0.0,
                y: movement,
            }
        },
        open_progress,
        backdrop_progress,
    }
}

/// `onRelease` (`DrawerSwipeArea.tsx:368-381`): whether the release opens the drawer. `direction`
/// is the direction the hook attributed the gesture to; `popup` is the popup's size when mounted.
pub fn on_release(
    direction: Option<SwipeDirection>,
    delta: Point,
    release_velocity: Point,
    resolved_swipe_direction: SwipeDirection,
    popup: Option<Size>,
    disabled: bool,
) -> bool {
    let displacement = get_displacement(resolved_swipe_direction, delta.x, delta.y);
    let release_velocity = get_displacement(
        resolved_swipe_direction,
        release_velocity.x,
        release_velocity.y,
    );
    let threshold =
        resolve_swipe_open_threshold(popup, opposite_swipe_direction(resolved_swipe_direction));
    let has_enough_distance = displacement >= threshold;
    let has_enough_velocity = release_velocity >= VELOCITY_THRESHOLD;
    direction == Some(resolved_swipe_direction)
        && (has_enough_distance || has_enough_velocity)
        && !disabled
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIRECTIONS: [SwipeDirection; 4] = [
        SwipeDirection::Up,
        SwipeDirection::Down,
        SwipeDirection::Left,
        SwipeDirection::Right,
    ];

    #[test]
    fn opposite_swipe_direction_is_an_involution() {
        for direction in DIRECTIONS {
            assert_ne!(opposite_swipe_direction(direction), direction);
            assert_eq!(
                opposite_swipe_direction(opposite_swipe_direction(direction)),
                direction
            );
        }
    }

    #[test]
    fn touch_action_leaves_the_cross_axis_to_the_browser() {
        assert_eq!(resolve_touch_action(SwipeDirection::Left), "pan-y");
        assert_eq!(resolve_touch_action(SwipeDirection::Right), "pan-y");
        assert_eq!(resolve_touch_action(SwipeDirection::Up), "pan-x");
        assert_eq!(resolve_touch_action(SwipeDirection::Down), "pan-x");
    }

    #[test]
    fn closed_offset_prefers_the_rendered_transform_but_never_exceeds_the_size() {
        let popup = Size {
            width: 300.0,
            height: 400.0,
        };
        let moved = |x: f64, y: f64| Transform { x, y, scale: 1.0 };
        let table = [
            // A bottom sheet (dismiss direction down): the vertical transform counts.
            (SwipeDirection::Down, moved(0.0, 250.0), Some(250.0)),
            (SwipeDirection::Down, moved(0.0, 900.0), Some(400.0)),
            (SwipeDirection::Down, moved(0.0, 0.4), Some(400.0)),
            // A left drawer (dismiss direction left): the transform is negative.
            (SwipeDirection::Left, moved(-120.0, 0.0), Some(120.0)),
            (SwipeDirection::Left, moved(0.0, 77.0), Some(300.0)),
        ];
        for (dismiss, transform, expected) in table {
            assert_eq!(
                resolve_closed_offset(popup, transform, dismiss),
                expected,
                "{dismiss:?} {transform:?}"
            );
        }
        assert_eq!(
            resolve_closed_offset(Size::default(), Transform::default(), SwipeDirection::Down),
            None
        );
    }

    #[test]
    fn open_threshold_is_half_the_popup_or_the_fallback() {
        let popup = Size {
            width: 300.0,
            height: 400.0,
        };
        assert_eq!(
            resolve_swipe_open_threshold(Some(popup), SwipeDirection::Down),
            200.0
        );
        assert_eq!(
            resolve_swipe_open_threshold(Some(popup), SwipeDirection::Left),
            150.0
        );
        assert_eq!(
            resolve_swipe_open_threshold(None, SwipeDirection::Down),
            40.0
        );
        assert_eq!(
            resolve_swipe_open_threshold(Some(Size::default()), SwipeDirection::Down),
            40.0
        );
    }

    #[test]
    fn swipe_movement_follows_the_finger_then_damps_past_fully_open() {
        // An up swipe opens a bottom drawer (dismiss direction down) with a 400px closed offset.
        let up = |travel: f64| Point { x: 0.0, y: -travel };
        let halfway = apply_swipe_movement(400.0, up(100.0), SwipeDirection::Up);
        assert_eq!(halfway.movement, Point { x: 0.0, y: 300.0 });
        assert_eq!(halfway.open_progress, 0.25);
        assert_eq!(halfway.backdrop_progress, 0.75);

        let open = apply_swipe_movement(400.0, up(400.0), SwipeDirection::Up);
        assert_eq!(open.movement, Point { x: 0.0, y: 0.0 });
        assert_eq!(open.open_progress, 1.0);

        // 100px past open moves the popup sqrt(100) = 10px beyond, against the dismiss direction.
        let overshoot = apply_swipe_movement(400.0, up(500.0), SwipeDirection::Up);
        assert_eq!(overshoot.movement, Point { x: 0.0, y: -10.0 });
        assert_eq!(overshoot.open_progress, 1.0);
        assert_eq!(overshoot.backdrop_progress, 0.0);

        // The wrong way never moves the popup past closed.
        let backwards = apply_swipe_movement(400.0, Point { x: 0.0, y: 80.0 }, SwipeDirection::Up);
        assert_eq!(backwards.movement, Point { x: 0.0, y: 400.0 });
        assert_eq!(backwards.open_progress, 0.0);
    }

    #[test]
    fn swipe_movement_is_horizontal_for_left_and_right_drawers() {
        // A right swipe opens a drawer that dismisses to the left: it rests at -300.
        let moved = apply_swipe_movement(300.0, Point { x: 75.0, y: 3.0 }, SwipeDirection::Right);
        assert_eq!(moved.movement, Point { x: -225.0, y: 0.0 });
        let moved = apply_swipe_movement(300.0, Point { x: -75.0, y: 3.0 }, SwipeDirection::Left);
        assert_eq!(moved.movement, Point { x: 225.0, y: 0.0 });
    }

    #[test]
    fn release_opens_on_distance_or_velocity_in_the_open_direction_only() {
        let popup = Some(Size {
            width: 300.0,
            height: 400.0,
        });
        let up = SwipeDirection::Up;
        let still = Point::default();
        // (direction, travel up, release velocity up, disabled, expected)
        let table = [
            (Some(up), 200.0, 0.0, false, true),
            (Some(up), 199.0, 0.0, false, false),
            (Some(up), 10.0, 0.1, false, true),
            (Some(up), 10.0, 0.09, false, false),
            (Some(SwipeDirection::Down), 300.0, 0.5, false, false),
            (None, 300.0, 0.5, false, false),
            (Some(up), 300.0, 0.5, true, false),
        ];
        for (direction, travel, velocity, disabled, expected) in table {
            assert_eq!(
                on_release(
                    direction,
                    Point { x: 0.0, y: -travel },
                    Point {
                        x: 0.0,
                        y: -velocity
                    },
                    up,
                    popup,
                    disabled
                ),
                expected,
                "{direction:?} {travel} {velocity} {disabled}"
            );
        }
        // Without a measured popup, 40px is enough.
        assert!(on_release(
            Some(up),
            Point { x: 0.0, y: -40.0 },
            still,
            up,
            None,
            false
        ));
        assert!(!on_release(
            Some(up),
            Point { x: 0.0, y: -39.0 },
            still,
            up,
            None,
            false
        ));
    }
}
